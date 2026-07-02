use std::ops;

use bitflags::bitflags;

const CHECKSUM_OFFSET: usize = 0;
const CHECKSUM_SIZE: usize = size_of::<u32>();

const FLAGS_OFFSET: usize = CHECKSUM_OFFSET + CHECKSUM_SIZE;
const FLAGS_SIZE: usize = size_of::<u8>();

const FREESPACE_START_OFFSET: usize = FLAGS_OFFSET + FLAGS_SIZE;
const FREESPACE_START_SIZE: usize = size_of::<u64>();

const FREESPACE_END_OFFSET: usize =
    FREESPACE_START_OFFSET + FREESPACE_START_SIZE;
const FREESPACE_END_SIZE: usize = size_of::<u64>();

const FREESPACE_OFFSET: usize = FREESPACE_END_OFFSET + FREESPACE_END_SIZE;
const FREESPACE_SIZE: usize = size_of::<u16>();

const NUM_KEY_OFFSET: usize = FREESPACE_OFFSET + FREESPACE_SIZE;
const NUM_KEY_SIZE: usize = size_of::<u16>();

const HEADER_SIZE: usize = CHECKSUM_SIZE
    + FLAGS_SIZE
    + FREESPACE_START_SIZE
    + FREESPACE_END_SIZE
    + FREESPACE_SIZE
    + NUM_KEY_SIZE;
const CONTENT_START: usize = HEADER_SIZE;

macro_rules! read_be {
    ($page:expr, $ty:ty, $start:expr, $end: expr) => {
        <$ty>::from_be_bytes(
            $page
                .cell($start, $end)
                .try_into()
                .expect("incorrect number of bytes"),
        )
    };
}
macro_rules! write_be {
    ($page:expr, $start:expr, $end: expr, $slice: expr) => {
        $page
            .mut_cell($start, $end)
            .copy_from_slice($slice.to_be_bytes().as_ref())
    };
}
macro_rules! field {
    ($getter:ident, $setter:ident, $ty:ty, $start:expr, $end:expr) => {
        pub fn $getter(&self) -> $ty {
            read_be!(self, $ty, $start, $end)
        }

        pub fn $setter(&mut self, value: $ty) {
            write_be!(self, $start, $end, value)
        }
    };
}

/// Basic operational unit within the index-organized table.
///
/// Page Layout:
/// [0..4]     u32   checksum
/// [4..5]     u8    flags (is_leaf,is_root,has_overflow,...)
/// [5..13]    u64   free_space_start
/// [13..21]   u64   free_space_end
/// [21..23]   u16   free_space
/// [23..25]   u16   number_of_keys
/// [25..] Content Start
///
#[derive(Debug, Clone)]
pub struct Page {
    inner: Box<[u8]>,
}

bitflags! {
    /// [`PageFlags`] is a set of all possible flags to a page
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct PageFlags: u8 {
        const IsLeaf = 0x01;
        const IsRoot = 0x02;
        const HasOverflow = 0x04;
    }
}

impl Page {
    /// Create a new [`Page`]
    ///
    /// ## Note
    ///
    /// Suggest utilizing a Vec<u8>, Box<[u8]> or a [u8; N] to avoid
    /// any copying that might be done.
    pub fn build<T>(bytes: T) -> Self
    where
        T: Into<Box<[u8]>>,
    {
        let inner: Box<[u8]> = bytes.into();
        assert!(
            inner.len() >= HEADER_SIZE,
            "supplied bytes can not be a page."
        );
        Self { inner }
    }

    /// Immutable view into the held [`Page`]
    pub fn cell(&self, start: usize, end: usize) -> &[u8] {
        &self[start..end]
    }

    /// Mutable view into the held [`Page`]
    pub fn mut_cell(&mut self, start: usize, end: usize) -> &mut [u8] {
        &mut self[start..end]
    }

    field!(checksum, set_checksum, u32, CHECKSUM_OFFSET, FLAGS_OFFSET);
    field!(flags, set_flags, u8, FLAGS_OFFSET, FREESPACE_START_OFFSET);
    field!(
        free_space_start,
        set_free_space_start,
        u64,
        FREESPACE_START_OFFSET,
        FREESPACE_END_OFFSET
    );
    field!(
        free_space_end,
        set_free_space_end,
        u64,
        FREESPACE_END_OFFSET,
        FREESPACE_OFFSET
    );
    field!(
        free_space,
        set_free_space,
        u16,
        FREESPACE_OFFSET,
        NUM_KEY_OFFSET
    );
    field!(num_keys, set_num_keys, u16, NUM_KEY_OFFSET, CONTENT_START);
}

impl ops::DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl ops::Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn page_field_access() {
        let mut page = Page::build(vec![0; 4096]);

        page.set_free_space(4096);
        page.set_flags(64);
        assert_eq!(page.flags(), 64);
        assert_eq!(page.free_space(), 4096);
        assert_ne!(page.inner[..], vec![0; 4096][..])
    }
}
