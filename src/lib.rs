use crc::Crc;

pub mod page;
pub mod pager;
pub mod recovery;
pub mod wal;

pub use page::{Page, PageFlags};
pub use pager::{AccessContext, Pager};
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
