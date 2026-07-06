use std::{fmt, ops};

use bitflags::bitflags;

use crate::CRC32C;

use super::constants::page::*;

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
    ($getter:ident, $setter:ident, $ty:ty, $start:expr) => {
        pub fn $getter(&self) -> $ty {
            read_be!(self, $ty, $start, $start + size_of::<$ty>())
        }

        pub fn $setter(&mut self, value: $ty) {
            write_be!(self, $start, $start + size_of::<$ty>(), value)
        }
    };
}

/// Basic operational unit within the index-organized table.
///
/// ## Page Header
///
/// ### Common
/// ```text
/// [0..8]      bytes[8]        magic
/// [8..12]     u32             checksum
/// [12..13]    u8              flags (is_meta, is_leaf, is_root, has_overflow, ...)
/// [13..15]    u16             free_space_start
/// [15..17]    u16             free_space_end
/// [17..19]    u16             free_space
/// [..]
/// [21..29]    u64             latest_lsn
/// [..]
/// [49..53]    u32             overflow_offset (only valid if has_overflow bit is set)
/// [..]                        reserved
/// [64..] body start
/// ```
///
/// ### Meta Page:
/// ```text
/// [..]
/// [19..21]    u16             page_size
/// [..]
/// [29..30]    u8              format_version
/// [30..34]    u32             tree_root
/// [34..36]    u16             next_page
/// [..]
/// [64..] body start
/// ```
///
/// ### Table Page
/// ``` text
/// [..]
/// [19..21]    u16             number_of_keys
/// [..]
/// [29..33]    u32             left_sibling_offset
/// [33..37]    u32             right_sibling_offset
/// [37..41]    u32             right_most_pointer  (only valid if internal page)
/// [41..49]    u64             node_high_key       (only valid if internal page)
/// [..]
/// [64..] body start
/// ```
///
///
#[derive(Clone)]
pub struct Page {
    inner: Box<[u8]>,
}

bitflags! {
    /// [`PageFlags`] is a set of all possible flags to a page
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct PageFlags: u8 {
        const IsLeaf = 1;
        const IsRoot = 1 << 1;
        const HasOverflow = 1 << 2;
        const IsMeta = 1 << 3;
        const IsInternal = 1 << 4;
        const IsOverflow = 1 << 5;
        const IsFree = 1 << 6;
    }
}

impl Page {
    /// Build a page from `bytes`
    ///
    /// ## Note
    ///
    /// Suggest utilizing a `Vec<u8>`, `Box<[u8]>` or a `[u8; N]` to avoid
    /// any copying that might be done.
    pub fn build<T>(bytes: T) -> Self
    where
        T: Into<Box<[u8]>>,
    {
        let inner: Box<[u8]> = bytes.into();
        assert!(
            inner.len() >= 512 && inner.len() <= u16::MAX as usize,
            "bytes is not page size len"
        );
        Self { inner }
    }

    /// Create a new [`Page`] of `size` size.
    pub fn new(size: u16, flags: PageFlags) -> Self {
        let inner = vec![0; size as usize];
        let mut out = Self::build(inner);
        out.reset(size, flags);
        out
    }

    pub fn reset(&mut self, size: u16, flags: PageFlags) {
        self.mut_cell(0, size as usize)
            .copy_from_slice(vec![0; size as usize].as_ref());

        self.set_flags(flags.bits());
        self.set_free_space_start(HEADER_SIZE as u16);
        self.set_free_space_end(size);
        self.set_free_space(size - HEADER_SIZE as u16);
        self.set_magic();
        self.set_checksum(self.compute_checksum());

        if flags.contains(PageFlags::IsMeta) {
            self.set_page_size(size);
            self.set_format_version(FORMAT_VERSION);
        }
    }

    /// Immutable view into the held [`Page`]
    pub fn cell(&self, start: usize, end: usize) -> &[u8] {
        &self[start..end]
    }

    /// Immutable view into the held [`Page`]
    pub fn cell_from(&self, start: usize) -> &[u8] {
        &self[start..]
    }

    /// Mutable view into the held [`Page`]
    pub fn mut_cell(&mut self, start: usize, end: usize) -> &mut [u8] {
        &mut self[start..end]
    }

    /// Computes the current checksum of the package
    pub fn compute_checksum(&self) -> u32 {
        CRC32C.checksum(&self[FLAGS_OFFSET..])
    }

    pub fn magic(&self) -> &[u8] {
        self.cell(MAGIC_OFFSET, CHECKSUM_OFFSET)
    }

    pub fn set_magic(&mut self) {
        self.mut_cell(MAGIC_OFFSET, CHECKSUM_OFFSET)
            .copy_from_slice(&MAGIC);
    }

    /// Returns whether the [`Page`] is valid.
    ///
    /// A page is valid if it contains valid `magic` bytes and
    /// stored checksum matches computed checksum
    pub fn valid(&self) -> (bool, Option<&str>) {
        if self.magic() != MAGIC {
            return (false, Some("unexpected magic bytes, not reading a page"));
        }

        if self.checksum() != self.compute_checksum() {
            return (false, Some("corrupted data, checksum is not valid"));
        }

        (true, None)
    }

    field!(checksum, set_checksum, u32, CHECKSUM_OFFSET);
    field!(flags, set_flags, u8, FLAGS_OFFSET);
    field!(
        free_space_start,
        set_free_space_start,
        u16,
        FREESPACE_START_OFFSET
    );
    field!(
        free_space_end,
        set_free_space_end,
        u16,
        FREESPACE_END_OFFSET
    );
    field!(free_space, set_free_space, u16, FREESPACE_OFFSET);
    field!(num_keys, set_num_keys, u16, NUM_KEY_OFFSET);
    field!(page_size, set_page_size, u16, PAGE_SIZE_OFFSET);
    field!(latest_lsn, set_lsn, u64, LSN_OFFSET);
    field!(left_sibling, set_left_sibling, u32, LEFT_SIBLING_OFFSET);
    field!(right_sibling, set_right_sibling, u32, RIGHT_SIBLING_OFFSET);
    field!(high_key, set_high_key, u64, NODE_HIGH_KEY_OFFSET);
    field!(
        right_pointer,
        set_right_pointer,
        u32,
        RIGHT_MOST_POINTER_OFFSET
    );
    field!(overflow, set_overflow, u32, OVERFLOW_OFFSET_OFFSET);
    field!(
        format_version,
        set_format_version,
        u8,
        FORMAT_VERSION_OFFSET
    );
    field!(tree_root, set_tree_root, u32, BTREE_ROOT_OFFSET);
    field!(next_page, set_next_page, u16, NEXT_PAGE_OFFSET);
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

impl fmt::Display for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "keys={}, free_space(start={}, end={}, size={})",
            self.num_keys(),
            self.free_space_start(),
            self.free_space_end(),
            self.free_space(),
        )
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
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
