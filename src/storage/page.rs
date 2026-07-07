use std::{fmt, ops};

use bitflags::bitflags;
use log::trace;

use crate::CRC32C;

use super::constants::page::*;

trait Field: Sized {
    fn read(bytes: &[u8]) -> Self;
    fn write(&self, out: &mut [u8]);
}

macro_rules! impl_field_int {
    ($($t:ty),*) => {$(
        impl Field for $t {
            fn read(bytes: &[u8]) -> Self {
                <$t>::from_be_bytes(bytes.try_into().expect("wrong byte count"))
            }
            fn write(&self, out: &mut [u8]) {
                out.copy_from_slice(&self.to_be_bytes());
            }
        }
    )*};
}
impl_field_int!(u8, u16, u32, u64);

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

impl Field for PageFlags {
    fn read(bytes: &[u8]) -> Self {
        PageFlags::from_bits(u8::read(bytes)).expect("invalid page flags")
    }
    fn write(&self, out: &mut [u8]) {
        self.bits().write(out);
    }
}

/// Basic operational unit within the index-organized table.
///
/// ## Page Header
/// ```text
/// [0..8]      bytes[8]        magic
/// [8..12]     u32             checksum
/// [12..13]    u8              flags (is_meta, is_leaf, is_root, has_overflow, ...)
/// [13..15]    u16             free_space_start
/// [15..17]    u16             free_space_end
/// [17..19]    u16             free_space
/// [..]                        unused
/// [21..29]    u64             latest_lsn
/// [..]                        unused
/// [49..53]    u32             overflow_offset (only valid if has_overflow bit is set)
/// [..]                        reserved
/// [64..]                      BODY
/// ```
#[derive(Clone)]
pub struct Page {
    bytes: Box<[u8]>,
}

impl Page {
    /// Build a page from `bytes`
    ///
    /// ## Panics
    ///
    /// This function panics if the length of `bytes` is not a power of two
    /// greater than or equal to 512.
    pub fn build<T>(bytes: T) -> Self
    where
        T: Into<Box<[u8]>>,
    {
        let bytes: Box<[u8]> = bytes.into();
        assert!(
            Self::is_valid_size(bytes.len()),
            "size is not a power of two greater than or equal to 512"
        );
        Self { bytes }
    }

    /// Create a new [`Page`] of `size` size.
    ///
    /// ## Panics
    ///
    /// This function panics if `size` is not a power of two greater than or equal to 512.
    pub fn new(size: u16, flags: PageFlags) -> Self {
        let mut out = Self::build(vec![0; size as usize]);
        out.format(size, flags);
        out
    }

    /// Formats the page, zeroing all page bytes, resetting `flags` and page
    /// header fields.
    pub fn format(&mut self, size: u16, flags: PageFlags) {
        self.mut_cell(0, size as usize)
            .copy_from_slice(vec![0; size as usize].as_ref());

        self.set_flags(flags);
        self.set_free_space_start(HEADER_SIZE as u16);
        self.set_free_space_end(size);
        self.set_free_space(size - HEADER_SIZE as u16);
        self.set_magic();
        self.set_checksum(self.compute_checksum());
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

    /// Returns an immutable slice into the page bytes from `[start...end)`.
    pub fn cell(&self, start: usize, end: usize) -> &[u8] {
        &self[start..end]
    }

    /// Returns an immutable slice into the page bytes from `[start...PAGE_END]`.
    pub fn cell_from(&self, start: usize) -> &[u8] {
        &self[start..]
    }

    /// Returns a mutable slice into the page bytes from `[start...end)`.
    pub fn mut_cell(&mut self, start: usize, end: usize) -> &mut [u8] {
        &mut self[start..end]
    }

    fn is_valid_size(size: impl Into<usize>) -> bool {
        let size: usize = size.into();
        size >= 512 && size.is_power_of_two()
    }
}

pub enum AnyPage<'a> {
    Meta(MetaPage<&'a Page>),
    Table(TablePage<&'a Page>),
}

pub enum AnyPageMut<'a> {
    Meta(MetaPage<&'a mut Page>),
    Table(TablePage<&'a mut Page>),
}

impl Page {
    /// Convert the basic [`Page`] into a specific variant
    /// based on its `flags` header.
    pub fn as_variant(&self) -> AnyPage<'_> {
        if self
            .flags()
            .contains(PageFlags::IsMeta)
        {
            AnyPage::Meta(MetaPage { page: self })
        } else {
            AnyPage::Table(TablePage { page: self })
        }
    }

    /// Convert the basic [`Page`] into a mutable specific variant
    /// based on its `flags` header.
    pub fn as_variant_mut(&mut self) -> AnyPageMut<'_> {
        if self
            .flags()
            .contains(PageFlags::IsMeta)
        {
            AnyPageMut::Meta(MetaPage { page: self })
        } else {
            AnyPageMut::Table(TablePage { page: self })
        }
    }
}

macro_rules! getter {
    (
        $(#[$doc:meta])*
        $getter:ident,
        $ty:ty,
        $start:expr
    ) => {
        $(#[$doc])*
        pub fn $getter(&self) -> $ty {
            <$ty as Field>::read(
                self.cell($start, $start + size_of::<$ty>())
            )
        }
    };
}

macro_rules! setter {
    (
        $(#[$doc:meta])*
        $setter:ident,
        $ty:ty,
        $start:expr
    ) => {
        $(#[$doc])*
        pub fn $setter(&mut self, value: $ty) {
            value.write(
                self.mut_cell($start, $start + size_of::<$ty>())
            )
        }
    };
}

macro_rules! field {
    (
        $(#[$getter_doc:meta])*
        $getter:ident,
        $(#[$setter_doc:meta])*
        $setter:ident,
        $ty:ty, $start:expr
    ) => {
        $(#[$getter_doc])*
        pub fn $getter(&self) -> $ty {
            <$ty as Field>::read(self.cell($start, $start + size_of::<$ty>()))
        }

        $(#[$setter_doc])*
        pub fn $setter(&mut self, value: $ty) {
            value.write(self.mut_cell($start, $start + size_of::<$ty>()))
        }
    };
}

impl Page {
    /// Retrieve the page header `magic` bytes.
    pub fn magic(&self) -> &[u8] {
        self.cell(MAGIC_OFFSET, CHECKSUM_OFFSET)
    }

    pub fn set_magic(&mut self) {
        self.mut_cell(MAGIC_OFFSET, CHECKSUM_OFFSET)
            .copy_from_slice(&MAGIC);
    }

    /// Computes the current checksum of the page
    pub fn compute_checksum(&self) -> u32 {
        let mut digest = CRC32C.digest();
        digest.update(&self[..CHECKSUM_OFFSET]);
        digest.update(&self[FLAGS_OFFSET..]);
        digest.finalize()
    }

    /// Returns the pages `overflow_offset` if any.
    pub fn overflow_offset(&self) -> Option<u32> {
        self.flags()
            .contains(PageFlags::HasOverflow)
            .then(|| self.overflow_offset_raw())
    }

    field!(
        /// The stored CRC(Cyclic Redundancy Check) value.
        checksum,
        /// Set page `crc` to `value`. See: [Self::compute_checksum]
        set_checksum, u32, CHECKSUM_OFFSET);

    field!(
        /// The [`PageFlags`] of the page.
        flags,
        set_flags, PageFlags, FLAGS_OFFSET);

    field!(
        /// Free space starting offset.
        free_space_start,
        set_free_space_start,
        u16,
        FREESPACE_START_OFFSET
    );
    field!(
        /// Free space end offset.
        free_space_end,
        set_free_space_end,
        u16,
        FREESPACE_END_OFFSET
    );
    field!(
        /// Amount of free space available in `page`.
        free_space,
        set_free_space, u16, FREESPACE_OFFSET);
    field!(
        /// Latest log sequence number(lsn) that modified the page.
        latest_lsn,
        set_lsn, u64, LSN_OFFSET);
    field!(
        /// Overflow offset of page. This value only has meaning when
        /// [`PageFlags::HasOverflow`] is set.
        overflow_offset_raw,
        set_overflow_offset,
        u32,
        OVERFLOW_OFFSET_OFFSET
    );
}

pub trait PageView: ops::Deref<Target = Page> {}
impl<T> PageView for T where T: ops::Deref<Target = Page> {}

pub trait PageViewMut: PageView + ops::DerefMut<Target = Page> {}
impl<T> PageViewMut for T where T: ops::DerefMut<Target = Page> {}

pub fn build() {}

/// A metadata page.
///
/// ```rust
/// use cryo::{Page, PageFlags};
/// use cryo::storage::page::AnyPage;
///
/// let page = Page::new(4096, PageFlags::IsMeta);
/// let AnyPage::Meta(meta) = page.as_variant() else {
///     panic!("big problem")
/// };
///
/// println!(
///     "page size: {}, format version: {}, tree root: {}, next page: {}",
///     meta.page_size(),
///     meta.format_version(),
///     meta.tree_root(),
///     meta.next_page()
/// );
/// ```
///
/// ## Header
/// ```text
/// [..]                        -- standard page headers --
/// [19..21]    u16             page_size
/// [..]                        -- standard page headers --
/// [29..30]    u8              format_version
/// [30..34]    u32             tree_root
/// [34..36]    u16             next_page
/// [..]                        -- standard page headers --
/// [64..]                      BODY
/// ```
pub struct MetaPage<P> {
    pub(crate) page: P,
}

impl<P: PageView> MetaPage<P> {
    getter!(
        /// The configured page size for the storage file.
        page_size,
         u16, PAGE_SIZE_OFFSET);
    getter!(
        /// The current format version of the storage file.
        format_version,
         u8, FORMAT_VERSION_OFFSET
    );
    getter!(
        /// The offset of the B-Tree root node
        tree_root,
         u32, BTREE_ROOT_OFFSET
    );
    getter!(
        /// The next logical page ID
        next_page,
         u16, NEXT_PAGE_OFFSET
    );
}

impl<P: PageViewMut> MetaPage<P> {
    setter!(set_page_size, u16, PAGE_SIZE_OFFSET);
    setter!(set_format_version, u8, FORMAT_VERSION_OFFSET);
    setter!(set_tree_root, u32, BTREE_ROOT_OFFSET);
    setter!(set_next_page, u16, NEXT_PAGE_OFFSET);
}

/// A B-Tree node/page.
///
/// ```rust
/// use cryo::{Page, PageFlags};
/// use cryo::storage::page::AnyPage;
///
/// let page = Page::new(4096, PageFlags::IsInternal);
/// let AnyPage::Table(table) = page.as_variant() else {
///     panic!("big problem")
/// };
///
/// println!(
///     "no. of keys: {}, sibling: (l: {}, r: {}), right pointer: {:?}, high key: {}",
///     table.num_keys(),
///     table.left_sibling_offset(),
///     table.right_sibling_offset(),
///     table.right_pointer(),
///     table.high_key(),
/// );
/// ```
///
/// ## Header
/// ```text
/// [..]                        -- standard page headers --
/// [19..21]    u16             number_of_keys
/// [..]                        -- standard page headers --
/// [29..33]    u32             left_sibling_offset
/// [33..37]    u32             right_sibling_offset
/// [37..41]    u32             right_most_pointer    (only valid if internal page)
/// [41..49]    u64             node_high_key
/// [..]                        -- standard page headers --
/// [64..]                      BODY
/// ```
pub struct TablePage<P> {
    page: P,
}

impl<P: PageView> TablePage<P> {
    getter!(
        /// The number of keys stored in the page.
        num_keys,
         u16, NUM_KEY_OFFSET);
    getter!(
        /// The offset of the left sibling
        left_sibling_offset,
        u32,
        LEFT_SIBLING_OFFSET
    );
    getter!(
        /// The offset of the right sibling
        right_sibling_offset,
        u32,
        RIGHT_SIBLING_OFFSET
    );
    getter!(
        /// The right most pointer; may not be valid.
        right_pointer_raw,
        u32,
        RIGHT_MOST_POINTER_OFFSET
    );
    getter!(
        /// The highest key the current page can store
        high_key, u64, NODE_HIGH_KEY_OFFSET);

    /// Returns the pages right most pointer if called on an internal page.
    pub fn right_pointer(&self) -> Option<u32> {
        self.flags()
            .contains(PageFlags::IsInternal)
            .then(|| self.right_pointer_raw())
    }
}

impl<P: PageViewMut> TablePage<P> {
    setter!(set_num_keys, u16, NUM_KEY_OFFSET);
    setter!(set_left_sibling_offset, u32, LEFT_SIBLING_OFFSET);
    setter!(set_right_sibling_offset, u32, RIGHT_SIBLING_OFFSET);
    setter!(set_right_pointer, u32, RIGHT_MOST_POINTER_OFFSET);
    setter!(set_high_key, u64, NODE_HIGH_KEY_OFFSET);
}

/// MutationScope is used to track the mutations made to an initial page and
/// report back the changed byte offsets as well as before and after byte
/// snapshots
#[derive(Debug, Clone)]
pub struct MutationScope {
    page: Page,
    initial: Box<[u8]>,
    diff: Option<Vec<Mutation>>,
}

/// Mutation provides information on a change that has occurred in a [`Page`].
/// It tracks the offset, starting bytes and after bytes of the mutation.
#[derive(Debug, Clone)]
pub struct Mutation {
    /// Start and end offset range of the mutation
    pub offset: MutationOffset,
    /// Bytes stored at offset before change
    pub before: Box<[u8]>,
    /// Bytes stored at offset after change
    pub after: Box<[u8]>,
}

#[derive(Debug, Clone)]
pub struct MutationOffset {
    pub start: usize,
    pub end: usize,
}

impl Mutation {
    /// Create a new [`Mutation`]
    pub fn new(
        start: usize,
        end: usize,
        before: impl Into<Box<[u8]>>,
        after: impl Into<Box<[u8]>>,
    ) -> Self {
        Self {
            offset: MutationOffset { start, end },
            before: before.into(),
            after: after.into(),
        }
    }

    /// Replay the mutation on the [`Page`]
    pub fn replay(&self, page: &mut impl PageViewMut) {
        page.mut_cell(self.offset.start, self.offset.end)
            .copy_from_slice(&self.after);
    }
}

impl MutationScope {
    /// Initialize a new mutation scope
    pub fn init(page: &Page) -> Self {
        Self {
            page: page.clone(),
            initial: page.bytes.clone(),
            diff: None,
        }
    }

    /// Replays the actions taken within the [`MutationScope`] onto the `page`.
    /// This function requires [`MutationScope::diff`] be called before it
    ///
    pub fn replay(&self, page: &mut impl PageViewMut) {
        if let Some(d) = &self.diff {
            d.iter()
                .for_each(|m| m.replay(page));
        }
    }

    /// Calculates the difference between the initial [`Page`] contents and the
    /// current [`Page`] contents.
    pub fn diff(&mut self, max_gap: Option<usize>) -> Vec<Mutation> {
        if let Some(d) = &self.diff {
            return d.clone();
        }

        let after_bytes = &self.page.bytes;
        let initial_bytes = &self.initial;
        let mut out = Vec::new();
        let max_gap = max_gap.unwrap_or(DEFAULT_MERGE_MUTATION_GAP);

        let mut start = 0;
        let mut end = 0;
        let mut spotted = false;
        trace!("page diff start");

        for (i, (after, initial)) in after_bytes
            .iter()
            .zip(initial_bytes.iter())
            .enumerate()
        {
            if initial != after {
                end = i;

                if !spotted {
                    spotted = true;
                    start = i;
                }

                continue;
            }

            if spotted {
                end += 1;
                trace!("page diff spotted: start={start} end={end}");
                out.push(Mutation::new(
                    start,
                    end,
                    self.initial[start..end].to_vec(),
                    self.cell(start, end).to_vec(),
                ));
            }
            spotted = false;
        }

        if spotted {
            end += 1;
            trace!("page diff spotted: start={start} end={end}");
            out.push(Mutation::new(
                start,
                end,
                self.initial[start..end].to_vec(),
                self.cell(start, end).to_vec(),
            ));
        }
        trace!("page diff end: changes={}", out.len());

        let out = self.coalesce(out, max_gap);
        self.diff = Some(out.clone());
        out
    }

    /// Merges adjacent mutations whose unchanged gap is `<= max_gap` bytes.
    /// `mutations` must be sorted by offset (as produced by [`MutationScope::diff`]).
    fn coalesce(
        &self,
        mut mutations: Vec<Mutation>,
        max_gap: usize,
    ) -> Vec<Mutation> {
        let mut out: Vec<Mutation> = Vec::with_capacity(mutations.len());
        let initial_count = mutations.len();

        for m in mutations.drain(..) {
            let should_merge = match out.last() {
                Some(last) => {
                    m.offset
                        .start
                        .saturating_sub(last.offset.end)
                        <= max_gap
                }
                None => false,
            };

            if should_merge {
                let prev = out.pop().unwrap();
                out.push(self.merge(prev, m))
            } else {
                out.push(m);
            }
        }

        trace!(
            "merge mutations: initial={initial_count} after_merge={}",
            out.len()
        );

        out
    }

    /// Merges two [`Mutation`] together into a single mutation, filling the gap
    /// between their offsets with current page contents.
    fn merge(&self, first: Mutation, second: Mutation) -> Mutation {
        let (left, right) = {
            if first.offset.start > second.offset.start {
                (second, first)
            } else {
                (first, second)
            }
        };

        let mut after = left.after.to_vec();
        let mut before = left.before.to_vec();

        let fill_bytes = self.cell(left.offset.end, right.offset.start);
        after.extend(fill_bytes.iter());
        before.extend(fill_bytes.iter());

        after.extend(right.after.iter());
        before.extend(right.before.iter());

        trace!(
            "mutation merge: left=(start={} end={}) right=(start={} end={})",
            left.offset.start,
            left.offset.end,
            right.offset.start,
            right.offset.end
        );

        Mutation::new(left.offset.start, right.offset.end, before, after)
    }
}

impl MutationScope {
    /// Convert the basic [`Page`] into a specific variant
    /// based on its `flags` header.
    pub fn as_variant(&self) -> AnyPage<'_> {
        if self
            .flags()
            .contains(PageFlags::IsMeta)
        {
            AnyPage::Meta(MetaPage { page: &self.page })
        } else {
            AnyPage::Table(TablePage { page: &self.page })
        }
    }

    /// Convert the basic [`Page`] into a mutable specific variant
    /// based on its `flags` header.
    pub fn as_variant_mut(&mut self) -> AnyPageMut<'_> {
        if self
            .flags()
            .contains(PageFlags::IsMeta)
        {
            AnyPageMut::Meta(MetaPage {
                page: &mut self.page,
            })
        } else {
            AnyPageMut::Table(TablePage {
                page: &mut self.page,
            })
        }
    }
}

impl ops::DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

impl ops::Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl fmt::Display for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "free_space(start={}, end={}, size={}), latest_lsn={}",
            self.free_space_start(),
            self.free_space_end(),
            self.free_space(),
            self.latest_lsn(),
        )
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl<P: PageView> ops::Deref for MetaPage<P> {
    type Target = Page;

    fn deref(&self) -> &Page {
        &self.page
    }
}

impl<P: PageView> ops::Deref for TablePage<P> {
    type Target = Page;

    fn deref(&self) -> &Page {
        &self.page
    }
}

impl<P: PageViewMut> ops::DerefMut for MetaPage<P> {
    fn deref_mut(&mut self) -> &mut Page {
        &mut self.page
    }
}

impl<P: PageViewMut> ops::DerefMut for TablePage<P> {
    fn deref_mut(&mut self) -> &mut Page {
        &mut self.page
    }
}

impl ops::Deref for MutationScope {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl ops::DerefMut for MutationScope {
    fn deref_mut(&mut self) -> &mut Page {
        &mut self.page
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn page_field_access() {
        let mut page = Page::build(vec![0; 4096]);

        page.set_free_space(4096);
        page.set_flags(PageFlags::empty());
        assert_eq!(page.flags(), PageFlags::empty());
        assert_eq!(page.free_space(), 4096);
        assert_ne!(page.bytes[..], vec![0; 4096][..])
    }

    #[test]
    fn mutation_scope_diff_actual_changes() {
        let page = Page::new(512, PageFlags::IsLeaf);
        let mut scope = MutationScope::init(&page);

        let changes: Vec<(usize, [u8; 3])> =
            vec![(90, [23, 34, 29]), (120, [12, 10, 0])];
        let expected: Vec<(usize, Vec<u8>)> =
            vec![(90, vec![23, 34, 29]), (120, vec![12, 10])];

        match scope.as_variant_mut() {
            AnyPageMut::Table(mut p) => {
                for (start, bytes) in changes {
                    p.mut_cell(start, start + bytes.len())
                        .copy_from_slice(&bytes);
                }
            }
            _ => assert!(false, "expected page to be leaf variant"),
        }

        let diff = scope.diff(None);
        let actual = diff
            .iter()
            .map(|m| (m.offset.start, m.after.to_vec()))
            .collect::<Vec<_>>();

        assert_eq!(diff.len(), 2);
        assert_eq!(actual, expected);
    }

    #[test]
    fn mutation_scope_diff_coalesce_gaps() {
        let page = Page::new(512, PageFlags::IsLeaf);
        let mut scope = MutationScope::init(&page);

        let changes: Vec<(usize, [u8; 3])> =
            vec![(90, [23, 34, 29]), (96, [12, 10, 10]), (120, [8, 0, 10])];
        let expected: Vec<(usize, Vec<u8>)> = vec![
            (90, vec![23, 34, 29, 0, 0, 0, 12, 10, 10]),
            (120, vec![8, 0, 10]),
        ];

        match scope.as_variant_mut() {
            AnyPageMut::Table(mut p) => {
                for (start, bytes) in changes {
                    p.mut_cell(start, start + bytes.len())
                        .copy_from_slice(&bytes);
                }
            }
            _ => assert!(false, "expected page to be leaf variant"),
        }

        let diff = scope.diff(None);
        let actual = diff
            .iter()
            .map(|m| (m.offset.start, m.after.to_vec()))
            .collect::<Vec<_>>();

        assert_eq!(diff.len(), 2);
        assert_eq!(actual, expected);
    }
}
