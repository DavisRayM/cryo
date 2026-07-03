use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::{self, Read, Seek, Write},
    sync,
};

use log::{info, trace};

use crate::pager::FlushGuard;

const MAGIC: &str = "PD";
const MAGIC_SIZE: usize = MAGIC.len();
const MAGIC_OFFSET: usize = 0;

const RECORD_FORMAT_VERSION: u8 = 1;
const RECORD_FORMAT_SIZE: usize = size_of::<u8>();
const RECORD_FORMAT_OFFSET: usize = MAGIC_OFFSET + MAGIC_SIZE;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct RecordFlags: u8 {}
}
const RECORD_FLAGS_SIZE: usize = size_of::<u8>();
const RECORD_FLAGS_OFFSET: usize = RECORD_FORMAT_OFFSET + RECORD_FORMAT_SIZE;

const LSN_SIZE: usize = size_of::<u32>();
const LSN_OFFSET: usize = RECORD_FLAGS_OFFSET + RECORD_FLAGS_SIZE;

const CHECKSUM_SIZE: usize = size_of::<u32>();
const CHECKSUM_OFFSET: usize = LSN_OFFSET + LSN_SIZE;

const PAYLOAD_LEN_SIZE: usize = size_of::<u32>();
const PAYLOAD_LEN_OFFSET: usize = CHECKSUM_OFFSET + CHECKSUM_SIZE;

/// [`Record`] is an entry in the Write-Ahead Log.
///
/// The [`Record`] keeps track of actions taken against pages in memory so they
/// can be redone during recovery or undone when a transaction aborts.
///
/// Record Layout:
///
/// [0..2]      u16     magic
/// [2..3]      u8      format
/// [3..4]      u8      flags
/// [4..8]      u32     lsn
/// [8..12]     u32     crc
/// [12..16]    u32     payload_len
/// [16..]      bytes   payload
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

fn read_u32_be(reader: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64_be(reader: &mut impl Read) -> io::Result<u64> {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

impl Record {
    pub fn scan_existing(reader: &mut (impl Read + Seek)) -> io::Result<u64> {
        reader.seek(std::io::SeekFrom::Start(0))?;

        let mut last_lsn = 0;

        loop {
            match Self::read(reader) {
                Ok(Some((lsn, _record))) => {
                    last_lsn = lsn;
                }
                Ok(None) => break,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(last_lsn)
    }

    pub fn read(reader: &mut impl Read) -> io::Result<Option<(u64, Self)>> {
        let mut magic = [0; MAGIC_SIZE];

        if !read_exact_or_eof(reader, &mut magic)? {
            return Ok(None);
        }

        if &magic != MAGIC.as_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid WAL record magic",
            ));
        }

        let mut version = [0; RECORD_FORMAT_SIZE];
        reader.read_exact(&mut version)?;

        if version[0] != RECORD_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported WAL record version",
            ));
        }

        let mut flags = [0; 1];
        reader.read_exact(&mut flags)?;
        let _flags = RecordFlags::from_bits(flags[0]);

        let lsn = read_u64_be(reader)?;
        let crc = read_u32_be(reader)?;
        let payload_len = read_u32_be(reader)?;

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
    pub fn new(mut inner: W) -> io::Result<Self> {
        let last_lsn = Record::scan_existing(&mut inner)?;

        inner.seek(std::io::SeekFrom::End(0))?;

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

        while let Some((lsn, record)) = self.buffer.front().cloned() {
            if lsn > target_lsn {
                break;
            }

            self.write(lsn, &record)?;
            self.buffer.pop_front();
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
        if payload.len() > u32::MAX as usize {
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

pub struct WalFlushGuard<W> {
    wal: sync::Arc<sync::Mutex<Logger<W>>>,
}

impl<W> FlushGuard for WalFlushGuard<W>
where
    W: Read + Write + Seek + Send,
{
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Page;
    use std::io::{Cursor, SeekFrom};

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

    fn read_all_records(
        cursor: &mut Cursor<Vec<u8>>,
    ) -> io::Result<Vec<(u64, Record)>> {
        cursor.seek(SeekFrom::Start(0))?;

        let mut out = Vec::new();
        while let Some((lsn, record)) = Record::read(cursor)? {
            out.push((lsn, record));
        }

        Ok(out)
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
        let mut logger = Logger::new(inner).expect("logger can be created");

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

        let records = read_all_records(&mut logger.inner)
            .expect("flushed WAL records can be read");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, begin_lsn);
        assert_eq!(records[0].1.kind(), "begin");

        logger
            .flush_through(update_lsn)
            .expect("flush through second record succeeds");

        assert_eq!(logger.flushed_lsn, update_lsn);
        assert!(logger.buffer.is_empty());

        let records = read_all_records(&mut logger.inner)
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
        let mut logger = Logger::new(inner).expect("logger can be created");

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
            Logger::new(inner).expect("logger can scan existing WAL");

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
        let mut logger = Logger::new(inner).expect("logger can be created");
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
        let mut logger = Logger::new(inner).expect("logger can be created");

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
            Logger::new(inner).expect("logger can be created"),
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
