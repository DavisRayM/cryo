use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    sync,
};

use log::{info, trace};

use crate::pager::FlushGuard;
use crate::read_be;

const MAGIC: &str = "PD";
const MAGIC_SIZE: usize = MAGIC.len();
const RECORD_FORMAT_VERSION: u8 = 1;

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

/// [`Record`] is an entry in the Write-Ahead Log.
///
/// The [`Record`] keeps track of actions taken against pages in memory so they
/// can be redone during recovery or undone when a transaction aborts.
///
/// Record Layout:
///
/// [0..2]       bytes[2]     magic
/// [2..3]       u8           format
/// [3..4]       u8           flags
/// [4..12]      u64          lsn
/// [12..16]     u32          crc
/// [16..20]     u32          payload_len
/// [20..]       bytes        payload
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
    pub fn scan_existing(reader: &mut (impl Read + Seek)) -> io::Result<u64> {
        let mut last_lsn = 0;
        let mut last_offset = 0;

        reader.seek(std::io::SeekFrom::Start(last_offset))?;

        loop {
            match Self::read(reader) {
                Ok(Some((lsn, _record))) => {
                    last_lsn = lsn;
                }
                Ok(None) => break,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // recover to last know good position
                    reader.seek(SeekFrom::Start(last_offset))?;
                    break;
                }
                Err(e) => return Err(e),
            }

            last_offset = reader.stream_position()?;
        }

        Ok(last_lsn)
    }

    pub fn read(reader: &mut impl Read) -> io::Result<Option<(u64, Self)>> {
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

        Ok(Some((lsn, record)))
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
}

pub struct Logger<W> {
    buffer: VecDeque<(u64, Record)>,
    inner: W,
    next_lsn: u64,
    pub current_generation: u64,
    pub flushed_lsn: u64,
}

impl<W> Logger<W>
where
    W: Read + Write + Seek,
{
    /// Open a WAL [`Logger`].
    ///
    /// [`Logger`] will load latest state from the backing record log
    /// and continue tracking from the latest known `lsn`.
    pub fn open(mut inner: W) -> io::Result<Self> {
        let last_lsn = Record::scan_existing(&mut inner)?;

        Ok(Self {
            buffer: VecDeque::new(),
            inner,
            next_lsn: last_lsn + 1,
            current_generation: 0,
            flushed_lsn: last_lsn,
        })
    }

    /// Append [`Record`] into WAL Log.
    ///
    /// The append operation is buffered in memory and is flushed on
    /// call to [Self::flush_through].
    pub fn append(&mut self, record: Record) -> io::Result<u64> {
        // TODO: Compact the log. The Logger keeps track of
        //       generation but never truly does anything with it.
        let lsn = self.next_lsn;
        self.next_lsn += 1;

        info!(
            "wal append: lsn={lsn} txn={:?} page={:?} kind={}",
            record.txn_id(),
            record.page_id(),
            record.kind(),
        );

        self.buffer
            .push_back((lsn, record));

        Ok(lsn)
    }

    /// Read all [`Record`] currently in the WAL
    pub fn read_all(&mut self) -> io::Result<Vec<(u64, Record)>> {
        self.inner
            .seek(SeekFrom::Start(0))?;
        let mut out = Vec::new();
        while let Some(r) = Record::read(&mut self.inner)? {
            out.push(r);
        }
        Ok(out)
    }

    /// Flushes all changes up to `target_lsn`
    pub fn flush_through(&mut self, target_lsn: u64) -> io::Result<()> {
        if target_lsn <= self.flushed_lsn {
            trace!(
                "wal flush skipped: target_lsn={target_lsn} flushed_lsn={}",
                self.flushed_lsn
            );
            return Ok(());
        }

        info!(
            "wal flush start: target_lsn={target_lsn} current_flushed={}",
            self.flushed_lsn
        );
        let mut flushed_until = self.flushed_lsn;

        while let Some((lsn, record)) = self.buffer.pop_front() {
            if lsn > target_lsn {
                self.buffer
                    .push_front((lsn, record));
                break;
            }

            self.write(lsn, &record)?;
            flushed_until = lsn;
        }

        self.inner.flush()?;
        self.flushed_lsn = flushed_until;

        info!("wal flush complete: flushed len={}", self.flushed_lsn);
        Ok(())
    }

    fn write(&mut self, lsn: u64, record: &Record) -> io::Result<()> {
        let payload = postcard::to_allocvec(record)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if payload.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL record too large",
            ));
        }

        let crc = crate::CRC32C.checksum(&payload);
        self.inner
            .write_all(MAGIC.as_bytes())?;
        self.inner
            .write_all(&[RECORD_FORMAT_VERSION])?;
        self.inner
            .write_all(&[RecordFlags::empty().bits()])?;
        self.inner
            .write_all(lsn.to_be_bytes().as_ref())?;
        self.inner
            .write_all(crc.to_be_bytes().as_ref())?;
        self.inner.write_all(
            (payload.len() as u32)
                .to_be_bytes()
                .as_ref(),
        )?;
        self.inner
            .write_all(&payload)?;

        Ok(())
    }
}

impl Logger<File> {
    /// Ensure the underlying file is actually synced
    ///
    /// This is the equivalent of an `fsync` syscall
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.inner.sync_all()?;
        Ok(())
    }
}

pub struct WalFlushGuard<W> {
    wal: sync::Arc<sync::Mutex<Logger<W>>>,
}

impl FlushGuard for WalFlushGuard<std::io::Cursor<Vec<u8>>> {
    fn before_flush(&self, page_id: u64, page: &crate::Page) -> io::Result<()> {
        let lsn = page.latest_lsn();

        let mut wal = self.wal.lock().map_err(|_| {
            io::Error::other("failed to lock Write-Ahead Log before page flush")
        })?;

        if wal.flushed_lsn < lsn {
            info!(
                "[WAL][BEFORE][FLUSH] Page: id={page_id} lsn={lsn} last_wal_flushed={}",
                wal.flushed_lsn
            );
            wal.flush_through(lsn)?;
        } else {
            trace!(
                "[WAL][BEFORE][FLUSHED] Page: {page_id} lsn={lsn} last_wal_flushed={}",
                wal.flushed_lsn
            );
        }
        Ok(())
    }
}

impl FlushGuard for WalFlushGuard<File> {
    fn before_flush(&self, page_id: u64, page: &crate::Page) -> io::Result<()> {
        let lsn = page.latest_lsn();

        let mut wal = self.wal.lock().map_err(|_| {
            io::Error::other("failed to lock Write-Ahead Log before page flush")
        })?;

        if wal.flushed_lsn < lsn {
            info!(
                "[WAL][BEFORE][FLUSH] Page: id={page_id} lsn={lsn} last_wal_flushed={}",
                wal.flushed_lsn
            );
            wal.flush_through(lsn)?;
            wal.sync_all()
        } else {
            trace!(
                "[WAL][BEFORE][FLUSHED] Page: {page_id} lsn={lsn} last_wal_flushed={}",
                wal.flushed_lsn
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Page;
    use std::io::Cursor;

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
        let inner = Cursor::new(Vec::new());
        let mut logger = Logger::open(inner).expect("logger can be created");

        let begin_lsn = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");
        let update_lsn = logger
            .append(update_record(Some(begin_lsn)))
            .expect("update can be appended");

        assert_eq!(begin_lsn, 1);
        assert_eq!(update_lsn, 2);
        assert_eq!(logger.flushed_lsn, 0);
        assert_eq!(logger.buffer.len(), 2);
        assert!(
            logger
                .inner
                .get_ref()
                .is_empty()
        );

        logger
            .flush_through(begin_lsn)
            .expect("flush through first record succeeds");

        assert_eq!(logger.flushed_lsn, begin_lsn);
        assert_eq!(logger.buffer.len(), 1);

        let records = logger
            .read_all()
            .expect("flushed WAL records can be read");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, begin_lsn);
        assert_eq!(records[0].1.kind(), "begin");

        logger
            .flush_through(update_lsn)
            .expect("flush through second record succeeds");

        assert_eq!(logger.flushed_lsn, update_lsn);
        assert!(logger.buffer.is_empty());

        let records = logger
            .read_all()
            .expect("all flushed WAL records can be read");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, begin_lsn);
        assert_eq!(records[1].0, update_lsn);
        assert_eq!(records[1].1.kind(), "update");
        assert_eq!(records[1].1.prev_lsn(), Some(begin_lsn));
    }

    #[test]
    fn logger_new_scans_existing_records_and_resumes_lsn_numbering() {
        let inner = Cursor::new(Vec::new());
        let mut logger = Logger::open(inner).expect("logger can be created");

        let begin_lsn = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");
        let commit_lsn = logger
            .append(Record::Commit {
                txn_id: 1,
                prev_lsn: Some(begin_lsn),
            })
            .expect("commit can be appended");
        logger
            .flush_through(commit_lsn)
            .expect("records can be flushed");

        let inner = logger.inner;
        let mut reopened =
            Logger::open(inner).expect("logger can scan existing WAL");

        assert_eq!(reopened.flushed_lsn, commit_lsn);
        assert_eq!(reopened.next_lsn, commit_lsn + 1);
        assert!(reopened.buffer.is_empty());

        let end_lsn = reopened
            .append(Record::End {
                txn_id: 1,
                prev_lsn: Some(commit_lsn),
            })
            .expect("end can be appended after reopening");
        assert_eq!(end_lsn, commit_lsn + 1);
    }

    #[test]
    fn record_read_rejects_invalid_magic_version_and_checksum() {
        let mut invalid_magic = Cursor::new(vec![b'X', b'X']);
        let err = Record::read(&mut invalid_magic)
            .expect_err("invalid magic should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let mut invalid_version =
            Cursor::new(vec![b'P', b'D', RECORD_FORMAT_VERSION + 1]);
        let err = Record::read(&mut invalid_version)
            .expect_err("invalid version should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let inner = Cursor::new(Vec::new());
        let mut logger = Logger::open(inner).expect("logger can be created");
        let lsn = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("record can be appended");
        logger
            .flush_through(lsn)
            .expect("record can be flushed");

        let mut bytes = logger.inner.into_inner();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;

        let mut corrupted = Cursor::new(bytes);
        let err = Record::read(&mut corrupted)
            .expect_err("checksum mismatch should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn scan_existing_stops_at_trailing_partial_frame() {
        let inner = Cursor::new(Vec::new());
        let mut logger = Logger::open(inner).expect("logger can be created");

        let begin_lsn = logger
            .append(Record::Begin {
                txn_id: 1,
                prev_lsn: None,
            })
            .expect("begin can be appended");
        logger
            .flush_through(begin_lsn)
            .expect("record can be flushed");

        let mut bytes = logger.inner.into_inner();
        bytes.extend_from_slice(MAGIC.as_bytes());

        let mut cursor = Cursor::new(bytes);
        let last_lsn = Record::scan_existing(&mut cursor)
            .expect("trailing partial frame is treated as end of valid WAL");

        assert_eq!(last_lsn, begin_lsn);
    }

    #[test]
    fn wal_flush_guard_flushes_through_page_lsn() {
        let inner = Cursor::new(Vec::new());
        let wal = sync::Arc::new(sync::Mutex::new(
            Logger::open(inner).expect("logger can be created"),
        ));

        let lsn = {
            let mut logger = wal
                .lock()
                .expect("wal lock is available");
            logger
                .append(Record::Begin {
                    txn_id: 1,
                    prev_lsn: None,
                })
                .expect("begin can be appended")
        };

        let guard = WalFlushGuard { wal: wal.clone() };
        let mut page = Page::build(vec![0; 4096]);
        page.set_lsn(lsn);

        guard
            .before_flush(1, &page)
            .expect("guard can flush WAL through page LSN");

        let logger = wal
            .lock()
            .expect("wal lock is available");
        assert_eq!(logger.flushed_lsn, lsn);
        assert!(logger.buffer.is_empty());
    }
}
