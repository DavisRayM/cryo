use crc::Crc;

pub mod file;
pub mod page;

/// https://reveng.sourceforge.io/crc-catalogue/all.htm
pub(crate) const CRC32C: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
