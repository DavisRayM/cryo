use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{self, Mutex},
};

use log::{info, trace, warn};

use crate::{FlushGuard, read_be};

const MAGIC: &str = "PD";
const MAGIC_SIZE: usize = MAGIC.len();

const RECORD_FORMAT_VERSION: u8 = 1;
const RECORD_FORMAT_SIZE: usize = size_of::<u8>();

const FLAGS_SIZE: usize = size_of::<u8>();
const LSN_SIZE: usize = size_of::<u64>();
const CHECKSUM_SIZE: usize = size_of::<u32>();
const PAYLOAD_LEN_SIZE: usize = size_of::<u32>();

const HEADER_SIZE: usize = MAGIC_SIZE
    + RECORD_FORMAT_SIZE
    + FLAGS_SIZE
    + LSN_SIZE
    + CHECKSUM_SIZE
    + PAYLOAD_LEN_SIZE;

/// The file extension used for WAL generation segment files.
const WAL_EXTENSION: &str = "wal";

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct RecordFlags: u8 {}
}

fn read_exact_or_eof(
    reader: &mut impl Read,
    buf: &mut [u8],
) -> io::Result<bool> {
    let mut read = 0;

    while read < buf.len() {
        match reader.read(&mut buf[read..])? {
            0 if read == 0 => return Ok(false),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "partial WAL frame",
                ));
            }
            n => read += n,
        }
    }

    Ok(true)
}

/// Scans a single `generation`'s `reader` for all valid [`Record`] entries
/// starting from byte `offset`.
///
/// Returns the entries found along with the next [`Lsn`] within this generation
/// (i.e. the byte offset immediately past the last valid frame). A trailing
/// partial/corrupt frame is treated as the end of the valid log and the reader
/// is rewound to the last known-good position.
fn scan_records_from(
    reader: &mut (impl Read + Seek),
    generation: u32,
    offset: u32,
) -> io::Result<(Lsn, Vec<RecordEntry>)> {
    let mut offset = offset;
    let mut records = Vec::new();
    reader.seek(SeekFrom::Start(offset as u64))?;

    loop {
        match Record::read(reader) {
            Ok(Some((stored_lsn, record))) => {
                let lsn = Lsn::new(generation, offset);
                if u64::from(lsn) != stored_lsn {
                    warn!(
                        "WAL LSN does not match offset!! expected={lsn} \
                         stored={}",
                        Lsn::from(stored_lsn)
                    );
                }
                offset = reader.stream_position()? as u32;
                records.push(RecordEntry { lsn, record });
            }
            Ok(None) => break,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
                ) =>
            {
                reader.seek(SeekFrom::Start(offset as u64))?;
                break;
            }
            Err(e) => return Err(e),
        }
    }

    Ok((Lsn::new(generation, offset), records))
}

/// Returns the path of the `<generation>.wal` segment inside `dir`.
fn generation_path(dir: &Path, generation: u32) -> PathBuf {
    dir.join(format!("{generation}.{WAL_EXTENSION}"))
}

/// Discovers all WAL generation numbers present in `dir`.
///
/// A generation file is any file named `<N>.wal` where `N` parses as a `u32`.
/// The returned generations are sorted ascending.
fn discover_generations(dir: &Path) -> io::Result<Vec<u32>> {
    let mut generations = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path
            .extension()
            .and_then(|ext| ext.to_str())
            != Some(WAL_EXTENSION)
        {
            continue;
        }

        if let Some(generation) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u32>().ok())
        {
            generations.push(generation);
        }
    }

    generations.sort_unstable();
    Ok(generations)
}

/// [`Lsn`] is a physical log address.
///
/// It is encoded as a `u64` where the high 32 bits are the WAL `generation`
/// and the low 32 bits are the byte `offset` within that generation's file.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn {
    generation: u32,
    offset: u32,
}

impl Lsn {
    /// Construct an [`Lsn`] from its `generation` and byte `offset`.
    pub const fn new(generation: u32, offset: u32) -> Self {
        Self { generation, offset }
    }

    /// The WAL generation this address points into.
    pub const fn generation(self) -> u32 {
        self.generation
    }

    /// The byte offset within the generation file.
    pub const fn offset(self) -> u32 {
        self.offset
    }

    /// The [`Lsn`] of the record that would follow a record of `len` bytes
    /// written at `self`, staying within the same generation.
    ///
    /// Returns `None` when the addition would overflow the 32-bit offset field,
    /// i.e. the generation has grown beyond 4 GiB. The caller must rotate to a
    /// new generation before appending further.
    fn advanced_by(self, len: u32) -> Option<Lsn> {
        let offset = self.offset.checked_add(len)?;
        Some(Lsn {
            generation: self.generation,
            offset,
        })
    }

    /// The first [`Lsn`] of the generation following `self`.
    fn next_generation(self) -> Lsn {
        Lsn {
            generation: self.generation + 1,
            offset: 0,
        }
    }
}

impl From<Lsn> for u64 {
    fn from(value: Lsn) -> Self {
        ((value.generation as u64) << 32) | (value.offset as u64)
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Lsn {
        let generation = (value >> 32) as u32;
        let offset = (value & 0xffff_ffff) as u32;
        Lsn { generation, offset }
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.generation, self.offset)
    }
}

#[derive(Debug, Clone)]
pub struct RecordEntry {
    lsn: Lsn,
    record: Record,
}

impl PartialOrd for RecordEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.lsn
            .partial_cmp(&other.lsn)
    }
}

impl PartialEq for RecordEntry {
    fn eq(&self, other: &Self) -> bool {
        self.lsn.eq(&other.lsn)
    }
}

impl RecordEntry {
    /// The physical [`Lsn`] at which this record is stored.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// The [`Record`] payload.
    pub fn record(&self) -> &Record {
        &self.record
    }
}

impl From<(Lsn, Record)> for RecordEntry {
    fn from((lsn, record): (Lsn, Record)) -> Self {
        RecordEntry { lsn, record }
    }
}

/// [`Record`] is an entry in the Write-Ahead Log.
///
/// The [`Record`] keeps track of actions taken against pages in memory so they
/// can be redone during recovery or undone when a transaction aborts.
///
/// Record Layout:
///
/// `[`0..2`]`       bytes`[`2`]`     magic
/// `[`2..3`]`       u8           format
/// `[`3..4`]`       u8           flags
/// `[`4..12`]`      u64          lsn
/// `[`12..16`]`     u32          crc
/// `[`16..20`]`     u32          payload_len
/// `[`20..`]`       bytes        payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Record {
    /// Marks the beginning of a transaction.
    Begin { txn_id: u64, prev_lsn: Option<u64> },

    /// Describes a byte-range update made to a page.
    ///
    /// The `before` bytes are used to undo the update and the `after` bytes are
    /// used to redo it.
    Update {
        txn_id: u64,
        page_id: u64,
        offset: u16,
        before: Vec<u8>,
        after: Vec<u8>,
        prev_lsn: Option<u64>,
    },

    /// Marks a transaction as committed.
    Commit { txn_id: u64, prev_lsn: Option<u64> },

    /// Marks a transaction as aborted.
    Abort { txn_id: u64, prev_lsn: Option<u64> },

    /// Describes an undo action that has already been applied.
    ///
    /// Compensation records are redo-only and point at the next log record that
    /// should be undone for the transaction.
    Compensation {
        txn_id: u64,
        page_id: u64,
        offset: u16,
        after: Vec<u8>,
        undo_next_lsn: Option<u64>,
        prev_lsn: Option<u64>,
    },

    /// Marks the end of a transaction's log records.
    End { txn_id: u64, prev_lsn: Option<u64> },

    /// Marks the beginning of a checkpoint.
    BeginCheckpoint,

    /// Marks the end of a checkpoint.
    EndCheckpoint,
}

impl Record {
    /// Reads a single [`Record`] from `reader`. Returning the stored `lsn`
    /// and [`Record`] payload.
    ///
    /// The record is read from the reader's current position and validated against
    /// the expected on-disk record format.
    ///
    /// ## Errors
    ///
    /// This function returns an error when bytes cannot be read from `reader` or
    /// when the bytes read do not describe a valid [`Record`].
    pub fn read(reader: &mut impl Read) -> io::Result<Option<(u64, Record)>> {
        let mut magic = [0; MAGIC_SIZE];
        if !read_exact_or_eof(reader, &mut magic)? {
            return Ok(None);
        }
        if magic != MAGIC.as_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid WAL record magic",
            ));
        }

        let version = read_be!(reader, u8);
        if version != RECORD_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported WAL record version",
            ));
        }

        let flags = read_be!(reader, u8);
        let _flags = RecordFlags::from_bits(flags).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted WAL record flags",
            )
        })?;

        let lsn = read_be!(reader, u64);
        let crc = read_be!(reader, u32);
        let payload_len = read_be!(reader, u32);

        if payload_len > u16::MAX as u32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL payload too large",
            ));
        }

        let mut payload = vec![0; payload_len as usize];
        reader.read_exact(&mut payload)?;

        let actual_crc = crate::CRC32C.checksum(&payload);
        if actual_crc != crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL record checksum mismatch",
            ));
        }

        let record: Record = postcard::from_bytes(&payload[..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        record.validate(None)?;

        Ok(Some((lsn, record)))
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        HEADER_SIZE
            + self
                .as_bytes()
                .expect("should be able to marshal record")
                .len()
    }

    pub fn txn_id(&self) -> Option<u64> {
        match self {
            Self::Begin { txn_id, .. }
            | Self::Update { txn_id, .. }
            | Self::Commit { txn_id, .. }
            | Self::Abort { txn_id, .. }
            | Self::Compensation { txn_id, .. }
            | Self::End { txn_id, .. } => Some(*txn_id),
            Self::BeginCheckpoint | Self::EndCheckpoint => None,
        }
    }

    pub fn page_id(&self) -> Option<u64> {
        match self {
            Self::Update { page_id, .. }
            | Self::Compensation { page_id, .. } => Some(*page_id),
            _ => None,
        }
    }

    pub fn prev_lsn(&self) -> Option<u64> {
        match self {
            Self::Begin { prev_lsn, .. }
            | Self::Update { prev_lsn, .. }
            | Self::Commit { prev_lsn, .. }
            | Self::Abort { prev_lsn, .. }
            | Self::Compensation { prev_lsn, .. }
            | Self::End { prev_lsn, .. } => *prev_lsn,
            Self::BeginCheckpoint | Self::EndCheckpoint => None,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Begin { .. } => "begin",
            Self::Update { .. } => "update",
            Self::Commit { .. } => "commit",
            Self::Abort { .. } => "abort",
            Self::Compensation { .. } => "clr",
            Self::End { .. } => "end",
            Self::BeginCheckpoint => "begin_checkpoint",
            Self::EndCheckpoint => "end_checkpoint",
        }
    }

    pub fn as_bytes(&self) -> io::Result<Box<[u8]>> {
        let payload = postcard::to_allocvec(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if payload.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL record too large",
            ));
        }

        Ok(payload.into())
    }

    /// Validate that this record is structurally sound before it is applied to
    /// or read from the log.
    ///
    /// These checks reject records that recovery could never apply correctly:
    ///
    /// - `Update.before` and `Update.after` must have equal length so an undo
    ///   can restore exactly the bytes a redo replaced.
    /// - `Update`/`Compensation` payloads must be non-empty (a zero-length
    ///   change carries no redo/undo information).
    /// - `Compensation` records are redo-only and must carry an
    ///   `undo_next_lsn` so undo can continue past the compensated action.
    ///
    /// A `page_size` hint, when provided, additionally requires the changed
    /// byte range (`offset + len`) to fit within a single page.
    pub fn validate(&self, page_size: Option<u16>) -> io::Result<()> {
        let invalid = |msg: &str| {
            io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
        };

        match self {
            Self::Update {
                offset,
                before,
                after,
                ..
            } => {
                if before.len() != after.len() {
                    return Err(invalid(
                        "WAL Update before/after length mismatch",
                    ));
                }
                if after.is_empty() {
                    return Err(invalid("WAL Update carries no bytes"));
                }
                Self::check_range(*offset, after.len(), page_size)?;
            }
            Self::Compensation {
                offset,
                after,
                undo_next_lsn,
                ..
            } => {
                if after.is_empty() {
                    return Err(invalid("WAL Compensation carries no bytes"));
                }
                if undo_next_lsn.is_none() {
                    return Err(invalid(
                        "WAL Compensation missing undo_next_lsn",
                    ));
                }
                Self::check_range(*offset, after.len(), page_size)?;
            }
            _ => {}
        }

        Ok(())
    }

    /// Ensure a changed byte range `[offset, offset + len)` fits within a page.
    fn check_range(
        offset: u16,
        len: usize,
        page_size: Option<u16>,
    ) -> io::Result<()> {
        let Some(page_size) = page_size else {
            return Ok(());
        };

        let end = offset as usize + len;
        if end > page_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL record byte range {offset}+{len} exceeds page size \
                     {page_size}"
                ),
            ));
        }

        Ok(())
    }
}

/// Mutable state protected by the [`Logger`]'s lock.
struct Inner {
    /// Directory containing the `<N>.wal` generation files.
    dir: PathBuf,
    /// Append handle for the current (highest) generation.
    writer: File,
    /// The generation currently being appended to.
    current_generation: u32,
    /// Records appended but not yet flushed to disk.
    buffer: VecDeque<RecordEntry>,
    /// The [`Lsn`] the next appended record will occupy.
    next_lsn: Lsn,
    /// The [`Lsn`] of the last record durably written to disk, if any.
    flushed_lsn: Option<Lsn>,
}

/// A directory-backed Write-Ahead Log.
///
/// The log is stored as a sequence of append-only generation files named
/// `<N>.wal`. Physical [`Lsn`]s encode `(generation, offset)`, so lookups can
/// seek directly to the owning generation file and byte offset. New appends
/// always go to the highest generation; [`Logger::rotate`] starts a new one.
///
/// All state is held behind a [`Mutex`] so the public API only requires shared
/// (`&self`) access and the logger can be shared across threads.
pub struct Logger {
    inner: Mutex<Inner>,
}

impl Logger {
    /// Open (or create) a WAL [`Logger`] backed by the directory at `path`.
    ///
    /// The directory is scanned for `<N>.wal` generation files. The highest
    /// generation is opened for appending and `next_lsn`/`flushed_lsn` are
    /// resumed from its valid prefix. When the directory contains no generation
    /// files, generation `0` is created.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let generations = discover_generations(&dir)?;
        let current_generation = generations
            .last()
            .copied()
            .unwrap_or(0);

        let mut writer = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(generation_path(&dir, current_generation))?;

        let (next_lsn, records) =
            scan_records_from(&mut writer, current_generation, 0)?;
        let flushed_lsn = records
            .last()
            .map(|entry| entry.lsn);

        // Position the append handle at the end of the valid prefix so a
        // trailing partial frame is overwritten by the next append.
        writer.seek(SeekFrom::Start(next_lsn.offset() as u64))?;

        info!(
            "wal open: dir={} current_generation={current_generation} \
             next_lsn={next_lsn} flushed_lsn={flushed_lsn:?}",
            dir.display()
        );

        Ok(Self {
            inner: Mutex::new(Inner {
                dir,
                writer,
                current_generation,
                buffer: VecDeque::new(),
                next_lsn,
                flushed_lsn,
            }),
        })
    }

    fn lock(&self) -> io::Result<sync::MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("wal: lock poisoned"))
    }

    /// The generation currently being appended to.
    pub fn current_generation(&self) -> io::Result<u32> {
        Ok(self
            .lock()?
            .current_generation)
    }

    /// The [`Lsn`] of the last record durably written to disk, if any.
    pub fn flushed_lsn(&self) -> io::Result<Option<Lsn>> {
        Ok(self.lock()?.flushed_lsn)
    }

    /// The [`Lsn`] the next appended record will occupy.
    pub fn next_lsn(&self) -> io::Result<Lsn> {
        Ok(self.lock()?.next_lsn)
    }

    /// Retrieve the [`Record`] stored at `lsn`.
    ///
    /// The record is served from the in-memory buffer when it has not yet been
    /// flushed, otherwise it is read from the generation file addressed by
    /// `lsn.generation()` at byte offset `lsn.offset()`.
    pub fn get(&self, lsn: Lsn) -> io::Result<Option<RecordEntry>> {
        let inner = self.lock()?;

        if let Ok(pos) = inner
            .buffer
            .binary_search_by(|r| r.lsn.cmp(&lsn))
        {
            return Ok(Some(inner.buffer[pos].clone()));
        }

        if lsn >= inner.next_lsn {
            return Ok(None);
        }

        // Anything at or after the first buffered record but not found in the
        // buffer does not correspond to a real record boundary.
        if let Some(first_buffered) = inner
            .buffer
            .front()
            .map(|r| r.lsn)
            && lsn >= first_buffered
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "wal: attempt to retrieve record outside of known range",
            ));
        }

        let mut reader = inner.open_generation_reader(lsn.generation())?;
        reader.seek(SeekFrom::Start(lsn.offset() as u64))?;

        let Some((stored_lsn, record)) = Record::read(&mut reader)? else {
            return Ok(None);
        };
        if stored_lsn != u64::from(lsn) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "wal: unexpected LSN. expected={lsn}, got={}",
                    Lsn::from(stored_lsn)
                ),
            ));
        }

        Ok(Some((lsn, record).into()))
    }

    /// Retrieve every record from `lsn` onwards, in order.
    ///
    /// Traversal follows the physical layout: it reads the generation
    /// containing `lsn` from `lsn.offset()`, then continues through each
    /// subsequent on-disk generation, and finally appends any buffered records
    /// that have not yet been flushed.
    pub fn records_from(&self, lsn: Lsn) -> io::Result<Vec<RecordEntry>> {
        let inner = self.lock()?;
        let mut records = Vec::new();

        // Read flushed records across generations, starting from `lsn`.
        let mut generation = lsn.generation();
        let mut offset = lsn.offset();
        while generation <= inner.current_generation {
            let mut reader = inner.open_generation_reader(generation)?;
            let (_next, mut found) =
                scan_records_from(&mut reader, generation, offset)?;
            records.append(&mut found);

            generation += 1;
            offset = 0;
        }

        // Append buffered (not-yet-flushed) records at or after `lsn`.
        records.extend(
            inner
                .buffer
                .iter()
                .filter(|entry| entry.lsn >= lsn)
                .cloned(),
        );

        Ok(records)
    }

    /// Append `record` to the WAL, returning its assigned [`Lsn`].
    ///
    /// The append is buffered in memory and only reaches disk on a call to
    /// [`Logger::flush_through`].
    pub fn append(&self, record: Record) -> io::Result<Lsn> {
        record.validate(None)?;

        let mut inner = self.lock()?;
        let lsn = inner.next_lsn;
        inner.next_lsn = lsn
            .advanced_by(record.len() as u32)
            .ok_or_else(|| {
                io::Error::other(
                    "WAL generation offset overflow: rotate to a new generation before appending",
                )
            })?;

        info!(
            "wal append: lsn={lsn} txn={:?} page={:?} kind={}",
            record.txn_id(),
            record.page_id(),
            record.kind(),
        );

        inner
            .buffer
            .push_back((lsn, record).into());

        Ok(lsn)
    }

    /// Read every flushed [`Record`] in the current generation from its start.
    pub fn read_all(&self) -> io::Result<Vec<RecordEntry>> {
        let inner = self.lock()?;
        let (_next, records) = {
            let mut reader =
                inner.open_generation_reader(inner.current_generation)?;
            scan_records_from(&mut reader, inner.current_generation, 0)?
        };
        Ok(records)
    }

    /// Flush all buffered records up to and including `target_lsn` to disk.
    ///
    /// Records after `target_lsn` remain buffered. This writes to the OS file
    /// but does not guarantee durability; use [`Logger::sync_all`] for that.
    pub fn flush_through(&self, target_lsn: Lsn) -> io::Result<()> {
        let mut inner = self.lock()?;
        inner.flush_through(target_lsn)
    }

    /// Start a new generation, directing subsequent appends to it.
    ///
    /// Buffered records are flushed and synced into the current generation
    /// first so the previous generation is complete and durable before the
    /// boundary. The new generation's addresses start at offset `0`.
    pub fn rotate(&self) -> io::Result<u32> {
        let mut inner = self.lock()?;

        let pending = inner
            .buffer
            .back()
            .map(|entry| entry.lsn);
        if let Some(last) = pending {
            inner.flush_through(last)?;
        }
        inner.writer.sync_all()?;

        let next_generation = inner.current_generation + 1;
        let writer = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(generation_path(&inner.dir, next_generation))?;

        let next_lsn = inner
            .next_lsn
            .next_generation();
        inner.writer = writer;
        inner.current_generation = next_generation;
        inner.next_lsn = next_lsn;

        info!("wal rotate: new generation={next_generation}");
        Ok(next_generation)
    }

    /// Durably persist all flushed WAL bytes (equivalent to `fsync`).
    pub fn sync_all(&self) -> io::Result<()> {
        let inner = self.lock()?;
        inner.writer.sync_all()
    }
}

impl Inner {
    /// Open a fresh read handle for `generation`'s segment file.
    fn open_generation_reader(
        &self,
        generation: u32,
    ) -> io::Result<BufReader<File>> {
        let file = OpenOptions::new()
            .read(true)
            .open(generation_path(&self.dir, generation))?;
        Ok(BufReader::new(file))
    }

    /// Flush buffered records up to and including `target_lsn`.
    fn flush_through(&mut self, target_lsn: Lsn) -> io::Result<()> {
        if let Some(flushed_lsn) = self.flushed_lsn
            && target_lsn <= flushed_lsn
        {
            trace!(
                "wal flush skipped: target_lsn={target_lsn} \
                     flushed_lsn={flushed_lsn}"
            );
            return Ok(());
        }

        info!(
            "wal flush start: target_lsn={target_lsn} \
             current_flushed={:?}",
            self.flushed_lsn
        );

        let mut flushed_until = self.flushed_lsn;

        while let Some(entry) = self.buffer.pop_front() {
            if entry.lsn > target_lsn {
                self.buffer.push_front(entry);
                break;
            }

            self.write(entry.lsn, &entry.record)?;
            flushed_until = Some(entry.lsn);
        }

        self.writer.flush()?;
        self.flushed_lsn = flushed_until;

        info!("wal flush complete: flushed_lsn={:?}", self.flushed_lsn);
        Ok(())
    }

    /// Encode and write a single record frame at `lsn.offset()` in the current
    /// generation file.
    fn write(&mut self, lsn: Lsn, record: &Record) -> io::Result<()> {
        debug_assert_eq!(lsn.generation(), self.current_generation);

        let payload = record.as_bytes()?;
        let crc = crate::CRC32C.checksum(&payload);

        self.writer
            .seek(SeekFrom::Start(lsn.offset() as u64))?;
        self.writer
            .write_all(MAGIC.as_bytes())?;
        self.writer
            .write_all(&[RECORD_FORMAT_VERSION])?;
        self.writer
            .write_all(&[RecordFlags::empty().bits()])?;
        self.writer.write_all(
            u64::from(lsn)
                .to_be_bytes()
                .as_ref(),
        )?;
        self.writer
            .write_all(crc.to_be_bytes().as_ref())?;
        self.writer.write_all(
            (payload.len() as u32)
                .to_be_bytes()
                .as_ref(),
        )?;
        self.writer
            .write_all(&payload)?;

        Ok(())
    }
}

/// A [`FlushGuard`] that enforces the write-ahead rule: a page may only be
/// flushed once the WAL is durable through the page's `pageLSN`.
pub struct WalFlushGuard {
    wal: sync::Arc<Logger>,
}

impl WalFlushGuard {
    /// Create a guard that flushes `wal` before dependent pages are written.
    pub fn new(wal: sync::Arc<Logger>) -> Self {
        Self { wal }
    }
}

impl FlushGuard for WalFlushGuard {
    fn before_flush(
        &self,
        _page_id: u64,
        page: &crate::Page,
    ) -> io::Result<()> {
        let lsn = Lsn::from(page.latest_lsn());
        self.wal.flush_through(lsn)?;
        self.wal.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Page;
    use tempfile::TempDir;

    /// Open a [`Logger`] backed by a fresh temporary directory, returning the
    /// guard so the directory lives for the duration of the test.
    fn temp_logger() -> (TempDir, Logger) {
        let dir = TempDir::new().expect("temp dir can be created");
        let logger = Logger::open(dir.path()).expect("logger can be created");
        (dir, logger)
    }

    fn update_record(prev_lsn: Option<u64>) -> Record {
        Record::Update {
            txn_id: 10,
            page_id: 7,
            offset: 42,
            before: vec![b'a', b'b', b'c'],
            after: vec![b'x', b'y', b'z'],
            prev_lsn,
        }
    }

    #[test]
    fn record_metadata_helpers_return_expected_values() {
        let update = update_record(Some(3));
        assert_eq!(update.txn_id(), Some(10));
        assert_eq!(update.page_id(), Some(7));
        assert_eq!(update.prev_lsn(), Some(3));
        assert_eq!(update.kind(), "update");

        let clr = Record::Compensation {
            txn_id: 11,
            page_id: 8,
            offset: 9,
            after: vec![1, 2, 3],
            undo_next_lsn: Some(4),
            prev_lsn: Some(5),
        };
        assert_eq!(clr.txn_id(), Some(11));
        assert_eq!(clr.page_id(), Some(8));
        assert_eq!(clr.prev_lsn(), Some(5));
        assert_eq!(clr.kind(), "clr");

        let checkpoint = Record::BeginCheckpoint;
        assert_eq!(checkpoint.txn_id(), None);
        assert_eq!(checkpoint.page_id(), None);
        assert_eq!(checkpoint.prev_lsn(), None);
        assert_eq!(checkpoint.kind(), "begin_checkpoint");
    }

    #[test]
    fn append_buffers_records_until_flush_through() {
        let (_dir, logger) = temp_logger();

        let begin = Record::Begin {
            txn_id: 1,
            prev_lsn: None,
        };
        let expected_begin_lsn = Lsn::new(0, 0);
        let expected_update_lsn = expected_begin_lsn
            .advanced_by(begin.len() as u32)
            .unwrap();
        let begin_lsn = logger
            .append(begin)
            .expect("begin can be appended");
        let update_lsn = logger
            .append(update_record(Some(begin_lsn.into())))
            .expect("update can be appended");

        assert_eq!(begin_lsn, expected_begin_lsn);
        assert_eq!(update_lsn, expected_update_lsn);
        assert_eq!(logger.flushed_lsn().unwrap(), None);
        assert_eq!(
            logger
                .lock()
                .unwrap()
                .buffer
                .len(),
            2
        );

        logger
            .flush_through(begin_lsn)
            .expect("flush through first record succeeds");

        assert_eq!(logger.flushed_lsn().unwrap(), Some(begin_lsn));
        assert_eq!(
            logger
                .lock()
                .unwrap()
                .buffer
                .len(),
            1
        );

        let records = logger
            .read_all()
            .expect("flushed WAL records can be read");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lsn, begin_lsn);
        assert_eq!(records[0].record.kind(), "begin");

        logger
            .flush_through(update_lsn)
            .expect("flush through second record succeeds");

        assert_eq!(logger.flushed_lsn().unwrap(), Some(update_lsn));
        assert!(
            logger
                .lock()
                .unwrap()
                .buffer
                .is_empty()
        );

        let records = logger
            .read_all()
            .expect("all flushed WAL records can be read");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].lsn, begin_lsn);
        assert_eq!(records[1].lsn, update_lsn);
        assert_eq!(records[1].record.kind(), "update");
        assert_eq!(records[1].record.prev_lsn(), Some(begin_lsn.into()));
    }

    #[test]
    fn logger_new_scans_existing_records_and_resumes_lsn_numbering() {
        let dir = TempDir::new().expect("temp dir can be created");

        let begin_lsn;
        let commit_lsn;
        let commit_len;
        {
            let logger =
                Logger::open(dir.path()).expect("logger can be created");
            begin_lsn = logger
                .append(Record::Begin {
                    txn_id: 1,
                    prev_lsn: None,
                })
                .expect("begin can be appended");
            let commit = Record::Commit {
                txn_id: 1,
                prev_lsn: Some(begin_lsn.into()),
            };
            commit_len = commit.len() as u32;
            commit_lsn = logger
                .append(commit)
                .expect("commit can be appended");
            logger
                .flush_through(commit_lsn)
                .expect("records can be flushed");
            logger
                .sync_all()
                .expect("records can be synced");
        }

        let reopened =
            Logger::open(dir.path()).expect("logger can scan existing WAL");

        assert_eq!(
            reopened
                .flushed_lsn()
                .unwrap(),
            Some(commit_lsn)
        );
        assert_eq!(
            reopened.next_lsn().unwrap(),
            commit_lsn
                .advanced_by(commit_len)
                .unwrap()
        );
        assert!(
            reopened
                .lock()
                .unwrap()
                .buffer
                .is_empty()
        );

        let end_lsn = reopened
            .append(Record::End {
                txn_id: 1,
                prev_lsn: Some(commit_lsn.into()),
            })
            .expect("end can be appended after reopening");
        assert_eq!(
            end_lsn,
            commit_lsn
                .advanced_by(commit_len)
                .unwrap()
        );
    }

    #[test]
    fn record_read_rejects_invalid_magic_version_and_checksum() {
        use std::io::Cursor;

        let mut invalid_magic = Cursor::new(vec![b'X', b'X']);
        let err = Record::read(&mut invalid_magic)
            .expect_err("invalid magic should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let mut invalid_version =
            Cursor::new(vec![b'P', b'D', RECORD_FORMAT_VERSION + 1]);
        let err = Record::read(&mut invalid_version)
            .expect_err("invalid version should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let dir = TempDir::new().expect("temp dir can be created");
        let path = generation_path(dir.path(), 0);
        {
            let logger =
                Logger::open(dir.path()).expect("logger can be created");
            let lsn = logger
                .append(Record::Begin {
                    txn_id: 1,
                    prev_lsn: None,
                })
                .expect("record can be appended");
            logger
                .flush_through(lsn)
                .expect("record can be flushed");
            logger
                .sync_all()
                .expect("record can be synced");
        }

        let mut bytes = std::fs::read(&path).expect("wal file can be read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;

        let mut corrupted = Cursor::new(bytes);
        let err = Record::read(&mut corrupted)
            .expect_err("checksum mismatch should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn scan_existing_stops_at_trailing_partial_frame() {
        use std::io::Cursor;

        let dir = TempDir::new().expect("temp dir can be created");
        let path = generation_path(dir.path(), 0);

        let begin_lsn;
        {
            let logger =
                Logger::open(dir.path()).expect("logger can be created");
            begin_lsn = logger
                .append(Record::Begin {
                    txn_id: 1,
                    prev_lsn: None,
                })
                .expect("begin can be appended");
            logger
                .flush_through(begin_lsn)
                .expect("record can be flushed");
            logger
                .sync_all()
                .expect("record can be synced");
        }

        let mut bytes = std::fs::read(&path).expect("wal file can be read");
        bytes.extend_from_slice(MAGIC.as_bytes());

        let mut cursor = Cursor::new(bytes);
        let (_next_lsn, lsns) = scan_records_from(&mut cursor, 0, 0)
            .expect("trailing partial frame is treated as end of valid WAL");
        let last_lsn = lsns.last().unwrap().lsn;

        assert_eq!(last_lsn, begin_lsn);
    }

    #[test]
    fn get_retrieves_flushed_and_buffered_records_by_lsn() {
        let (_dir, logger) = temp_logger();

        let begin_lsn = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");
        let update_lsn = logger
            .append(update_record(Some(begin_lsn.into())))
            .expect("update can be appended");
        let commit_lsn = logger
            .append(Record::Commit {
                txn_id: 1,
                prev_lsn: Some(update_lsn.into()),
            })
            .expect("commit can be appended");

        logger
            .flush_through(update_lsn)
            .expect("first two records can be flushed");

        let begin = logger
            .get(begin_lsn)
            .expect("flushed begin lookup succeeds")
            .expect("flushed begin is found");
        assert_eq!(begin.lsn, begin_lsn);
        assert_eq!(begin.record.kind(), "begin");

        let update = logger
            .get(update_lsn)
            .expect("flushed update lookup succeeds")
            .expect("flushed update is found");
        assert_eq!(update.lsn, update_lsn);
        assert_eq!(update.record.kind(), "update");
        assert_eq!(update.record.prev_lsn(), Some(begin_lsn.into()));

        let commit = logger
            .get(commit_lsn)
            .expect("buffered commit lookup succeeds")
            .expect("buffered commit is found");
        assert_eq!(commit.lsn, commit_lsn);
        assert_eq!(commit.record.kind(), "commit");
        assert_eq!(commit.record.prev_lsn(), Some(update_lsn.into()));

        let eof = logger.next_lsn().unwrap();
        assert!(
            logger
                .get(eof)
                .expect("lookup at EOF succeeds")
                .is_none()
        );
    }

    #[test]
    fn records_from_reads_both_flushed_and_buffered() {
        let (_dir, logger) = temp_logger();

        let begin_lsn = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");
        let update_lsn = logger
            .append(update_record(Some(begin_lsn.into())))
            .expect("update can be appended");
        let commit_lsn = logger
            .append(Record::Commit {
                txn_id: 1,
                prev_lsn: Some(update_lsn.into()),
            })
            .expect("commit can be appended");
        let end_lsn = logger
            .append(Record::End {
                txn_id: 1,
                prev_lsn: Some(commit_lsn.into()),
            })
            .expect("end can be appended");

        logger
            .flush_through(update_lsn)
            .expect("first two records can be flushed");

        let records = logger
            .records_from(begin_lsn)
            .expect("flushed and buffered suffix can be read");
        assert_eq!(
            records
                .iter()
                .map(|entry| entry.lsn)
                .collect::<Vec<_>>(),
            vec![begin_lsn, update_lsn, commit_lsn, end_lsn]
        );
    }

    #[test]
    fn records_from_traverses_multiple_generations() {
        let (_dir, logger) = temp_logger();

        // Generation 0.
        let g0_begin = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");
        let g0_commit = logger
            .append(Record::Commit {
                txn_id: 1,
                prev_lsn: Some(g0_begin.into()),
            })
            .expect("commit can be appended");
        logger
            .flush_through(g0_commit)
            .expect("generation 0 can be flushed");

        // Roll to generation 1.
        let new_gen = logger
            .rotate()
            .expect("wal can rotate");
        assert_eq!(new_gen, 1);

        let g1_begin = logger
            .append(Record::Begin {
                txn_id: 2,
                prev_lsn: None,
            })
            .expect("begin can be appended in new generation");
        assert_eq!(g1_begin, Lsn::new(1, 0));

        // Buffered record still in generation 1.
        let g1_commit = logger
            .append(Record::Commit {
                txn_id: 2,
                prev_lsn: Some(g1_begin.into()),
            })
            .expect("commit can be appended in new generation");
        logger
            .flush_through(g1_begin)
            .expect("only the first generation-1 record is flushed");

        // Traverse from the very first LSN across both generations, including
        // the still-buffered final record.
        let records = logger
            .records_from(g0_begin)
            .expect("records can be traversed across generations");
        assert_eq!(
            records
                .iter()
                .map(|entry| entry.lsn)
                .collect::<Vec<_>>(),
            vec![g0_begin, g0_commit, g1_begin, g1_commit]
        );

        // A generation-crossing lookup by LSN resolves to the right file.
        let fetched = logger
            .get(g1_begin)
            .expect("cross-generation get succeeds")
            .expect("record is found");
        assert_eq!(fetched.lsn, g1_begin);
        assert_eq!(fetched.record.txn_id(), Some(2));
    }

    #[test]
    fn wal_flush_guard_flushes_through_page_lsn() {
        let dir = TempDir::new().expect("temp dir can be created");
        let wal = sync::Arc::new(
            Logger::open(dir.path()).expect("logger can be created"),
        );

        let lsn = wal
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");

        let guard = WalFlushGuard::new(wal.clone());
        let mut page = Page::build(vec![0; 4096]);
        page.set_lsn(lsn.into());

        guard
            .before_flush(1, &page)
            .expect("guard can flush WAL through page LSN");

        assert_eq!(wal.flushed_lsn().unwrap(), Some(lsn));
        assert!(
            wal.lock()
                .unwrap()
                .buffer
                .is_empty()
        );
    }

    /// A commit is only "durable" once its record has been flushed through
    /// and synced.
    #[test]
    fn commit_is_durable_before_being_reported() {
        let dir = TempDir::new().expect("temp dir can be created");

        let commit_lsn;
        {
            let logger =
                Logger::open(dir.path()).expect("logger can be created");
            let begin = logger
                .append(Record::Begin {
                    txn_id: 1,
                    prev_lsn: None,
                })
                .expect("begin can be appended");
            commit_lsn = logger
                .append(Record::Commit {
                    txn_id: 1,
                    prev_lsn: Some(begin.into()),
                })
                .expect("commit can be appended");

            // Commit protocol: force WAL through the Commit record, then sync.
            logger
                .flush_through(commit_lsn)
                .expect("commit record can be flushed");
            logger
                .sync_all()
                .expect("commit record can be synced");

            assert!(
                logger
                    .flushed_lsn()
                    .unwrap()
                    .unwrap()
                    >= commit_lsn,
                "commit must be flushed before success is reported"
            );
        }

        // Reopen: the committed record survived without an explicit End.
        let reopened =
            Logger::open(dir.path()).expect("logger can reopen after commit");
        let commit = reopened
            .get(commit_lsn)
            .expect("commit lookup succeeds")
            .expect("commit is durable");
        assert_eq!(commit.record.kind(), "commit");
    }

    #[test]
    fn append_rejects_update_with_mismatched_before_after() {
        let (_dir, logger) = temp_logger();

        let bad = Record::Update {
            txn_id: 1,
            page_id: 1,
            offset: 0,
            before: vec![1, 2, 3],
            after: vec![9, 9],
            prev_lsn: None,
        };

        let err = logger
            .append(bad)
            .expect_err("mismatched before/after must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn append_rejects_compensation_without_undo_next() {
        let (_dir, logger) = temp_logger();

        let bad = Record::Compensation {
            txn_id: 1,
            page_id: 1,
            offset: 0,
            after: vec![1, 2, 3],
            undo_next_lsn: None,
            prev_lsn: None,
        };

        let err = logger
            .append(bad)
            .expect_err("redo-only CLR without undo_next must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn validate_rejects_byte_range_beyond_page_size() {
        let update = Record::Update {
            txn_id: 1,
            page_id: 1,
            offset: 4094,
            before: vec![0, 0, 0, 0],
            after: vec![1, 1, 1, 1],
            prev_lsn: None,
        };

        assert!(update.validate(None).is_ok());
        let err = update
            .validate(Some(4096))
            .expect_err("range past page size must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn compensation_undo_next_lsn_drives_undo_traversal() {
        let (_dir, logger) = temp_logger();

        let begin = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");
        let update = logger
            .append(update_record(Some(begin.into())))
            .expect("update can be appended");

        // Compensate the update: its undo_next_lsn points before the update,
        // i.e. at the Begin record.
        let clr = logger
            .append(Record::Compensation {
                txn_id: 1,
                page_id: 7,
                offset: 42,
                after: vec![b'a', b'b', b'c'],
                undo_next_lsn: Some(begin.into()),
                prev_lsn: Some(update.into()),
            })
            .expect("clr can be appended");

        logger
            .flush_through(clr)
            .expect("records can be flushed");

        let clr_entry = logger
            .get(clr)
            .expect("clr lookup succeeds")
            .expect("clr is found");
        let Record::Compensation { undo_next_lsn, .. } = clr_entry.record()
        else {
            panic!("expected compensation record");
        };

        // Undo resumes at undo_next_lsn, which resolves to the Begin record.
        let resume = Lsn::from(undo_next_lsn.expect("clr carries undo_next"));
        assert_eq!(resume, begin);
        let resumed = logger
            .get(resume)
            .expect("resume lookup succeeds")
            .expect("resume record is found");
        assert_eq!(resumed.record().kind(), "begin");
    }

    #[test]
    fn flush_through_preserves_records_after_target() {
        let (_dir, logger) = temp_logger();

        let begin = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");
        let update = logger
            .append(update_record(Some(begin.into())))
            .expect("update can be appended");
        let commit = logger
            .append(Record::Commit {
                txn_id: 1,
                prev_lsn: Some(update.into()),
            })
            .expect("commit can be appended");

        logger
            .flush_through(update)
            .expect("flush through the update only");

        assert_eq!(logger.flushed_lsn().unwrap(), Some(update));
        // The commit remains buffered and is still retrievable by LSN.
        assert_eq!(
            logger
                .lock()
                .unwrap()
                .buffer
                .len(),
            1
        );
        let buffered = logger
            .get(commit)
            .expect("buffered commit lookup succeeds")
            .expect("commit still buffered");
        assert_eq!(buffered.record().kind(), "commit");

        // The on-disk prefix stops at the flushed target.
        let on_disk = logger
            .read_all()
            .expect("flushed prefix can be read");
        assert_eq!(
            on_disk
                .iter()
                .map(|e| e.lsn())
                .collect::<Vec<_>>(),
            vec![begin, update]
        );
    }
}
