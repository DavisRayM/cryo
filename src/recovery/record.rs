use super::read_exact_or_eof;
use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use std::io::{self, Read};

use crate::{read_be, storage::page::Mutation};

pub const MAGIC: &str = "PO";
pub const MAGIC_SIZE: usize = MAGIC.len();

pub const RECORD_FORMAT_VERSION: u8 = 1;
pub const RECORD_FORMAT_SIZE: usize = size_of::<u8>();

pub const FLAGS_SIZE: usize = size_of::<u8>();
pub const LSN_SIZE: usize = size_of::<u64>();
pub const CHECKSUM_SIZE: usize = size_of::<u32>();
pub const PAYLOAD_LEN_SIZE: usize = size_of::<u32>();

pub const HEADER_SIZE: usize = MAGIC_SIZE
    + RECORD_FORMAT_SIZE
    + FLAGS_SIZE
    + LSN_SIZE
    + CHECKSUM_SIZE
    + PAYLOAD_LEN_SIZE;

#[derive(Debug, Clone)]
pub struct RecordEntry {
    pub lsn: Lsn,
    pub record: Record,
}

impl RecordEntry {
    pub fn as_bytes(&self) -> std::io::Result<Vec<u8>> {
        let mut out = vec![];

        let value = self.record.as_bytes()?;
        let crc = crate::CRC32C.checksum(&value);

        out.extend(MAGIC.as_bytes());
        out.push(RECORD_FORMAT_VERSION);
        out.push(RecordFlags::empty().bits());
        out.extend(u64::from(self.lsn).to_be_bytes());
        out.extend(crc.to_be_bytes().as_ref());
        out.extend((value.len() as u32).to_be_bytes());
        out.extend(value);

        Ok(out)
    }
}

/// [`Lsn`] is a physical log address.
///
/// It is encoded as a `u64` where the high 32 bits are the WAL `generation`
/// and the low 32 bits are the byte `offset` within that generation's file.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn {
    pub generation: u32,
    pub offset: u32,
}

impl Lsn {
    /// Construct an [`Lsn`] from its `generation` and byte `offset`.
    pub const fn new(generation: u32, offset: u32) -> Self {
        Self { generation, offset }
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct RecordFlags: u8 {}
}

/// [`Record`] is an entry in the Write-Ahead Log.
///
/// The [`Record`] keeps track of actions taken against pages in memory so they
/// can be redone during recovery or undone when a transaction aborts.
///
/// Record Layout:
///
/// `[`0..2`]`       bytes`[`2`]`     magic
/// `[`2..3`]`       u8               format
/// `[`3..4`]`       u8               flags
/// `[`4..12`]`      u64              lsn
/// `[`12..16`]`     u32              crc
/// `[`16..20`]`     u32              payload_len
/// `[`20..`]`       bytes            payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Record {
    /// Marks the beginning of log.
    StartSentinel,

    /// Marks the beginning of a transaction.
    Begin { txn_id: u64, prev_lsn: Option<u64> },

    /// Describes a byte-range update made to a page.
    ///
    /// The `before` bytes are used to undo the update and the `after` bytes are
    /// used to redo it.
    Update {
        txn_id: u64,
        page_id: u64,
        mutations: Vec<Mutation>,
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
            Self::BeginCheckpoint
            | Self::EndCheckpoint
            | Self::StartSentinel => None,
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
            Self::BeginCheckpoint
            | Self::EndCheckpoint
            | Self::StartSentinel => None,
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
            Self::StartSentinel => "log_start",
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
            Self::Update { mutations, .. } => {
                for mutation in mutations.iter() {
                    if mutation.before.len() != mutation.after.len() {
                        return Err(invalid(
                            "WAL Update before/after length mismatch",
                        ));
                    }

                    if mutation.after.is_empty() {
                        return Err(invalid("WAL Update carries no bytes"));
                    }

                    Self::check_range(
                        mutation.offset.start as u16,
                        mutation.after.len(),
                        page_size,
                    )?;
                }
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

impl Lsn {
    /// The [`Lsn`] of the record that would follow a record of `len` bytes
    /// written at `self`, staying within the same generation.
    ///
    /// Returns `None` when the addition would overflow the 32-bit offset field,
    /// i.e. the generation has grown beyond 4 GiB. The caller must rotate to a
    /// new generation before appending further.
    pub fn advanced_by(self, len: u32) -> Option<Lsn> {
        let offset = self.offset.checked_add(len)?;
        Some(Lsn {
            generation: self.generation,
            offset,
        })
    }

    /// The first [`Lsn`] of the generation following `self`.
    pub fn next_generation(self) -> Lsn {
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

impl From<(Lsn, Record)> for RecordEntry {
    fn from((lsn, record): (Lsn, Record)) -> Self {
        RecordEntry { lsn, record }
    }
}

#[cfg(test)]
mod test {
    use crate::storage::page::MutationOffset;

    use super::*;

    fn update_record(prev_lsn: Option<u64>) -> Record {
        Record::Update {
            txn_id: 10,
            page_id: 7,
            mutations: vec![Mutation {
                offset: MutationOffset { start: 42, end: 45 },
                before: vec![0; 3].into_boxed_slice(),
                after: vec![b'x', b'y', b'z'].into_boxed_slice(),
            }],
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
    fn validate_rejects_byte_range_beyond_page_size() {
        let update = Record::Update {
            txn_id: 1,
            page_id: 1,
            mutations: vec![Mutation {
                offset: MutationOffset {
                    start: 4094,
                    end: 4098,
                },
                before: vec![0; 4].into_boxed_slice(),
                after: vec![1, 1, 1, 1].into_boxed_slice(),
            }],
            prev_lsn: None,
        };

        assert!(update.validate(None).is_ok());
        let err = update
            .validate(Some(4096))
            .expect_err("range past page size must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
