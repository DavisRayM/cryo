use std::{error::Error, io};

pub const DEFAULT_PAGE_SIZE: u16 = 4096;
pub const MAGIC: &str = "CRYOGENIC";
pub const FORMAT_VERSION: u8 = 1;
const fn magic_size() -> usize {
    MAGIC.len()
}

pub const SIZE: usize = 100;

pub const PAGE_SIZE_OFFSET: usize = magic_size();
pub const PAGE_SIZE_SIZE: usize = size_of::<u16>();

pub const FORMAT_VERSION_OFFSET: usize = PAGE_SIZE_OFFSET + PAGE_SIZE_SIZE;
pub const FORMAT_VERSION_SIZE: usize = size_of::<u8>();

/// Placeholder header section for a database file (reserves the first 100 bytes of the file)
#[derive(Debug, PartialEq, Eq)]
pub struct FileHeader {
    page_size: u16,
    format_version: u8,
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            format_version: FORMAT_VERSION,
        }
    }
}

impl TryFrom<&[u8]> for FileHeader {
    type Error = Box<dyn Error>;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < SIZE {
            return Err(
                io::Error::other("file is not a valid cryo database").into()
            );
        }

        let magic = &value[..magic_size()];
        if magic != MAGIC.as_bytes() {
            return Err(
                io::Error::other("file is not a valid cryo database").into()
            );
        }

        let page_size = u16::from_be_bytes(
            value[PAGE_SIZE_OFFSET..FORMAT_VERSION_OFFSET]
                .try_into()
                .expect("is u16 sized slice"),
        );

        let format_version_end = FORMAT_VERSION_OFFSET + FORMAT_VERSION_SIZE;
        let format_version = u8::from_be_bytes(
            value[FORMAT_VERSION_OFFSET..format_version_end]
                .try_into()
                .expect("is a u8 slice"),
        );

        Ok(Self {
            page_size,
            format_version,
        })
    }
}

impl From<FileHeader> for [u8; SIZE] {
    fn from(value: FileHeader) -> Self {
        let mut out = [0; SIZE];

        out[..magic_size()].copy_from_slice(MAGIC.as_bytes());

        out[PAGE_SIZE_OFFSET..FORMAT_VERSION_OFFSET].copy_from_slice(
            value
                .page_size
                .to_be_bytes()
                .as_ref(),
        );

        out[FORMAT_VERSION_OFFSET..FORMAT_VERSION_OFFSET + FORMAT_VERSION_SIZE]
            .copy_from_slice(
                value
                    .format_version
                    .to_be_bytes()
                    .as_ref(),
            );

        out
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn file_header_to_bytes() {
        let mut expected = MAGIC.as_bytes().to_vec();
        expected.extend(DEFAULT_PAGE_SIZE.to_be_bytes());
        expected.push(FORMAT_VERSION);
        expected.resize(100, 0);
        let actual: [u8; 100] = FileHeader::default().into();

        assert_eq!(actual[..], expected[..])
    }

    #[test]
    fn file_header_from_bytes() {
        let mut expected = MAGIC.as_bytes().to_vec();
        expected.extend(DEFAULT_PAGE_SIZE.to_be_bytes());
        expected.push(FORMAT_VERSION);
        expected.resize(100, 0);
        let expected: FileHeader = expected[..]
            .try_into()
            .unwrap();
        let actual = FileHeader::default();

        assert_eq!(actual, expected)
    }

    #[test]
    #[should_panic(expected = "file is not a valid cryo database")]
    fn incorrect_magic() {
        let input: [u8; 100] = [0; 100];
        let _: FileHeader = input[..].try_into().unwrap();
    }
}
