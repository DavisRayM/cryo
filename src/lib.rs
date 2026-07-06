use std::io;

use crc::Crc;

pub mod btree;
pub mod recovery;
pub mod storage;
pub mod wal;

pub use storage::{AccessContext, FlushGuard, Page, PageFlags, Pager};
pub use wal::{Logger, Lsn, Record, RecordEntry, RecordFlags, WalFlushGuard};

/// https://reveng.sourceforge.io/crc-catalogue/all.htm
pub(crate) const CRC32C: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);

/// Read from `reader` N bytes that would construct `ty`pe.
///
/// This Macro needs to be run in a `io::Result<R>` context.
#[macro_export]
macro_rules! read_be {
    ($reader:expr, $ty:ty) => {{
        let mut buf = [0; size_of::<$ty>()];
        ::std::io::Read::read_exact($reader, &mut buf)?;

        <$ty>::from_be_bytes(buf)
    }};
}

pub type Key = u32;

pub const KEYCELL_KEY_OFFSET: usize = 0;
pub const KEYCELL_KEY_SIZE: usize = size_of::<u32>();

pub const KEYCELL_OFFSET_OFFSET: usize = KEYCELL_KEY_OFFSET + KEYCELL_KEY_SIZE;
pub const KEYCELL_OFFSET_SIZE: usize = size_of::<u32>();

pub const KEYCELL_SIZE: usize = KEYCELL_KEY_SIZE + KEYCELL_OFFSET_SIZE;

pub const VALUECELL_KEY_OFFSET: usize = KEYCELL_KEY_OFFSET;
pub const VALUECELL_KEY_SIZE: usize = KEYCELL_KEY_SIZE;

pub const VALUECELL_VALUE_LEN_OFFSET: usize =
    VALUECELL_KEY_OFFSET + VALUECELL_KEY_SIZE;
pub const VALUECELL_VALUE_LEN_SIZE: usize = size_of::<u16>();

pub const VALUECELL_HEADER_SIZE: usize =
    VALUECELL_KEY_SIZE + VALUECELL_VALUE_LEN_SIZE;

#[derive(Debug, Clone)]
pub(crate) struct KeyCell {
    pub key: Key,
    pub offset: u32,
}

impl KeyCell {
    pub(crate) fn with_key(key: &Key) -> Self {
        Self {
            key: *key,
            offset: 0,
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != KEYCELL_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bytes is not valid KeyCell bytes",
            ));
        }
        let key = u32::from_be_bytes(
            bytes[KEYCELL_KEY_OFFSET..KEYCELL_OFFSET_OFFSET]
                .try_into()
                .expect("is key sized bytes"),
        );
        let offset = u32::from_be_bytes(
            bytes[KEYCELL_OFFSET_OFFSET..KEYCELL_SIZE]
                .try_into()
                .expect("is offset sized bytes"),
        );

        Ok(Self { key, offset })
    }
}

impl From<&KeyCell> for [u8; KEYCELL_SIZE] {
    fn from(val: &KeyCell) -> Self {
        let mut out = [0; KEYCELL_SIZE];
        out[KEYCELL_KEY_OFFSET..KEYCELL_OFFSET_OFFSET]
            .copy_from_slice(&val.key.to_be_bytes());
        out[KEYCELL_OFFSET_OFFSET..KEYCELL_SIZE]
            .copy_from_slice(&val.offset.to_be_bytes());

        out
    }
}

impl PartialEq for KeyCell {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl Eq for KeyCell {}

impl PartialOrd for KeyCell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyCell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueCell {
    key: u32,
    value: Box<[u8]>,
}

impl ValueCell {
    pub fn key(bytes: &[u8]) -> io::Result<Key> {
        if bytes.len() <= VALUECELL_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bytes is not valid KeyCell bytes",
            ));
        }

        Ok(u32::from_be_bytes(
            bytes[VALUECELL_KEY_OFFSET..VALUECELL_VALUE_LEN_OFFSET]
                .try_into()
                .expect("is key sized bytes"),
        ))
    }

    pub fn value(bytes: &[u8]) -> io::Result<Box<[u8]>> {
        if bytes.len() <= VALUECELL_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bytes is not valid KeyCell bytes",
            ));
        }

        let len = u32::from_be_bytes(
            bytes[VALUECELL_VALUE_LEN_OFFSET..VALUECELL_HEADER_SIZE]
                .try_into()
                .expect("is len sized bytes"),
        );

        let mut value = vec![0; len as usize];
        value.clone_from_slice(
            &bytes[VALUECELL_HEADER_SIZE..VALUECELL_HEADER_SIZE + len as usize],
        );
        Ok(value.into())
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        VALUECELL_HEADER_SIZE + self.value.len()
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() <= VALUECELL_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bytes is not valid KeyCell bytes",
            ));
        }
        let key = u32::from_be_bytes(
            bytes[VALUECELL_KEY_OFFSET..VALUECELL_VALUE_LEN_OFFSET]
                .try_into()
                .expect("is key sized bytes"),
        );
        let len = u16::from_be_bytes(
            bytes[VALUECELL_VALUE_LEN_OFFSET..VALUECELL_HEADER_SIZE]
                .try_into()
                .expect("is len sized bytes"),
        );

        let mut value = vec![0; len as usize];
        value.clone_from_slice(
            &bytes[VALUECELL_HEADER_SIZE..VALUECELL_HEADER_SIZE + len as usize],
        );

        Ok(Self {
            key,
            value: value.into(),
        })
    }
}

impl From<&ValueCell> for Box<[u8]> {
    fn from(val: &ValueCell) -> Self {
        let mut out = vec![0; val.len()];
        out[VALUECELL_KEY_OFFSET..VALUECELL_VALUE_LEN_OFFSET]
            .copy_from_slice(&val.key.to_be_bytes());

        out[VALUECELL_VALUE_LEN_OFFSET..VALUECELL_HEADER_SIZE]
            .copy_from_slice(&(val.value.len() as u16).to_be_bytes());
        out[VALUECELL_HEADER_SIZE..].copy_from_slice(&val.value);

        out.into()
    }
}

/// StorageInterface defines an interface for interacting with
/// "Storages" which handle organizing information on disk.
pub trait StorageInterface {
    /// Retrieve request key from underlying storage.
    ///
    /// Returns `Ok(None)` if key is not present.
    fn get(&self, key: Key) -> io::Result<Option<ValueCell>>;

    /// Set the value of `key` in the underlying storage, returning the
    /// previously set value if any.
    fn set(
        &mut self,
        key: Key,
        value: Box<[u8]>,
    ) -> io::Result<Option<ValueCell>>;

    /// Remove the value of the `key` in the underlying storage, returning
    /// the previously set value if any.
    fn remove(&mut self, key: Key) -> io::Result<Option<ValueCell>>;
}
