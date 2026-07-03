use crc::Crc;

pub mod page;
pub mod pager;

pub use page::{Page, PageFlags};

/// https://reveng.sourceforge.io/crc-catalogue/all.htm
pub(crate) const CRC32C: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
