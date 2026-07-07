pub mod page {
    //! Constant values utilized in page layout.
    //!

    /// Page identifier reserved for the root page.
    ///
    /// Page identifiers are one-based; page id `0` is invalid.
    pub const META_PAGE_ID: usize = 1;

    /// The default maximum allowed gap during page diff.
    pub const DEFAULT_MERGE_MUTATION_GAP: usize = 4;

    pub const MAGIC: [u8; MAGIC_SIZE] = [25, 3, 20, 26, 7, 4, 8, 0];
    pub const MAGIC_SIZE: usize = 8;
    pub const MAGIC_OFFSET: usize = 0;

    pub const CHECKSUM_OFFSET: usize = MAGIC_OFFSET + MAGIC_SIZE;
    pub const CHECKSUM_SIZE: usize = size_of::<u32>();

    pub const FLAGS_OFFSET: usize = CHECKSUM_OFFSET + CHECKSUM_SIZE;
    pub const FLAGS_SIZE: usize = size_of::<u8>();

    pub const FREESPACE_START_OFFSET: usize = FLAGS_OFFSET + FLAGS_SIZE;
    pub const FREESPACE_START_SIZE: usize = size_of::<u16>();

    pub const FREESPACE_END_OFFSET: usize =
        FREESPACE_START_OFFSET + FREESPACE_START_SIZE;
    pub const FREESPACE_END_SIZE: usize = size_of::<u16>();

    pub const FREESPACE_OFFSET: usize =
        FREESPACE_END_OFFSET + FREESPACE_END_SIZE;
    pub const FREESPACE_SIZE: usize = size_of::<u16>();

    pub const NUM_KEY_OFFSET: usize = FREESPACE_OFFSET + FREESPACE_SIZE;
    pub const NUM_KEY_SIZE: usize = size_of::<u16>();

    pub const PAGE_SIZE_OFFSET: usize = NUM_KEY_OFFSET;
    pub const PAGE_SIZE_SIZE: usize = NUM_KEY_SIZE;

    pub const LSN_OFFSET: usize = NUM_KEY_OFFSET + NUM_KEY_SIZE;
    pub const LSN_SIZE: usize = size_of::<u64>();

    pub const LEFT_SIBLING_OFFSET: usize = LSN_OFFSET + LSN_SIZE;
    pub const LEFT_SIBLING_SIZE: usize = size_of::<u32>();

    pub const FORMAT_VERSION_OFFSET: usize = LSN_OFFSET + LSN_SIZE;
    pub const FORMAT_VERSION: u8 = 1;
    pub const FORMAT_VERSION_SIZE: usize = size_of::<u8>();

    pub const BTREE_ROOT_OFFSET: usize =
        FORMAT_VERSION_OFFSET + FORMAT_VERSION_SIZE;
    pub const BTREE_ROOT_SIZE: usize = size_of::<u32>();

    pub const NEXT_PAGE_OFFSET: usize = BTREE_ROOT_OFFSET + BTREE_ROOT_SIZE;

    pub const RIGHT_SIBLING_OFFSET: usize =
        LEFT_SIBLING_OFFSET + LEFT_SIBLING_SIZE;
    pub const RIGHT_SIBLING_SIZE: usize = size_of::<u32>();

    pub const RIGHT_MOST_POINTER_OFFSET: usize =
        RIGHT_SIBLING_OFFSET + RIGHT_SIBLING_SIZE;
    pub const RIGHT_MOST_POINTER_SIZE: usize = size_of::<u32>();

    pub const NODE_HIGH_KEY_OFFSET: usize =
        RIGHT_MOST_POINTER_OFFSET + RIGHT_MOST_POINTER_SIZE;
    pub const NODE_HIGH_KEY_SIZE: usize = size_of::<u64>();

    pub const OVERFLOW_OFFSET_OFFSET: usize =
        NODE_HIGH_KEY_OFFSET + NODE_HIGH_KEY_SIZE;
    pub const OVERFLOW_OFFSET_SIZE: usize = size_of::<u32>();

    pub const HEADER_SIZE: usize = 64;
}
