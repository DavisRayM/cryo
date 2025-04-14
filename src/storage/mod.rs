use crate::Command;

pub mod btree;
pub mod page;
pub mod row;

pub use btree::BTreeStorage;

pub mod header {
    pub mod row {
        use crate::statement::{EMAIL_MAX_LENGTH, USERNAME_MAX_LENGTH};

        pub(crate) const ROW_ID_SIZE: usize = size_of::<usize>();
        pub(crate) const ROW_OFFSET_SIZE: usize = size_of::<usize>();
        pub(crate) const ROW_USERNAME_SIZE: usize = size_of::<char>() * USERNAME_MAX_LENGTH;
        pub(crate) const ROW_EMAIL_SIZE: usize = size_of::<char>() * EMAIL_MAX_LENGTH;
        pub(crate) const ROW_BODY_SIZE: usize = ROW_ID_SIZE + ROW_USERNAME_SIZE + ROW_EMAIL_SIZE;
        pub(crate) const LEAF_ROW_SIZE: usize = ROW_BODY_SIZE;
        pub(crate) const INTERNAL_ROW_SIZE: usize = ROW_ID_SIZE + ROW_OFFSET_SIZE + ROW_OFFSET_SIZE;

        pub(crate) const ROW_ID: usize = 0;
        pub(crate) const ROW_OFFSET: usize = ROW_ID + ROW_ID_SIZE;
        pub(crate) const ROW_LEFT_OFFSET: usize = ROW_OFFSET;
        pub(crate) const ROW_RIGHT_OFFSET: usize = ROW_LEFT_OFFSET + ROW_OFFSET_SIZE;
        pub(crate) const ROW_USERNAME: usize = ROW_ID + ROW_ID_SIZE;
        pub(crate) const ROW_EMAIL: usize = ROW_USERNAME + ROW_USERNAME_SIZE;
    }

    pub mod page {
        use super::row::{INTERNAL_ROW_SIZE, LEAF_ROW_SIZE};

        pub(crate) const PAGE_SIZE: usize = 4096;
        pub(crate) const PAGE_ID_SIZE: usize = size_of::<usize>();
        pub(crate) const PAGE_CELLS_SIZE: usize = size_of::<usize>();
        pub(crate) const PAGE_PARENT_SIZE: usize = size_of::<usize>();
        pub(crate) const PAGE_KIND_SIZE: usize = size_of::<u8>();
        pub(crate) const PAGE_HEADER_SIZE: usize =
            PAGE_ID_SIZE + PAGE_CELLS_SIZE + PAGE_PARENT_SIZE + PAGE_KIND_SIZE;

        pub(crate) const PAGE_ID: usize = 0;
        pub(crate) const PAGE_CELLS: usize = PAGE_ID + PAGE_ID_SIZE;
        pub(crate) const PAGE_PARENT: usize = PAGE_CELLS + PAGE_CELLS_SIZE;
        pub(crate) const PAGE_KIND: usize = PAGE_PARENT + PAGE_PARENT_SIZE;
        pub(crate) const HEADER_END: usize = PAGE_HEADER_SIZE;

        pub(crate) const CELLS_PER_LEAF: usize = (PAGE_SIZE - PAGE_HEADER_SIZE) / LEAF_ROW_SIZE;
        pub(crate) const CELLS_PER_INTERNAL: usize =
            (PAGE_SIZE - PAGE_HEADER_SIZE) / INTERNAL_ROW_SIZE;

        pub(crate) const PAGE_INTERNAL: u8 = 0x1;
        pub(crate) const PAGE_LEAF: u8 = 0x0;

        pub(crate) const LEAF_SPLITAT: usize = (CELLS_PER_LEAF / 2) + 1;
    }
}

pub mod error {
    use std::{error::Error, io};

    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum PageAction {
        #[error("PageRead")]
        Read,
        #[error("PageInsert")]
        Insert,
        #[error("PageSelect")]
        Select,
        #[error("PageWrite")]
        Write,
    }

    #[derive(Debug, Error)]
    pub enum PageErrorCause {
        #[error("page is full")]
        Full,
        #[error("duplicate record")]
        Duplicate,
        #[error("unknown state")]
        Unknown,
        #[error("failed to wrangle data")]
        DataWrangling,
        #[error("page currently in use")]
        InUse,
    }

    #[derive(Debug, Error)]
    pub enum StorageAction {
        #[error("Page")]
        Page,
        #[error("PageOut")]
        PageOut,
        #[error("CreatePage")]
        PageCreate,
        #[error("Insert")]
        Insert,
        #[error("SplitLeaf")]
        SplitLeaf,
        #[error("Query")]
        Query,
        #[error("Search")]
        Search,
    }

    #[derive(Debug, Error)]
    pub enum StorageErrorCause {
        #[error("out of bounds access")]
        OutOfBounds,
        #[error("{0}")]
        Error(Box<dyn Error>),
        #[error("unknown issue")]
        Unknown,
        #[error("failed to locate cache item")]
        CacheMiss,
        #[error("accessed in use storage")]
        PageInUse,
    }

    #[derive(Debug, Error)]
    pub enum StorageError {
        #[error("[row error][{action}]: {error}")]
        Row { action: String, error: String },

        #[error("[utility][{name}] error. reason: {cause:?}")]
        Utility { name: String, cause: Option<String> },

        #[error("[page error][{action}] {cause}")]
        Page {
            action: PageAction,
            cause: PageErrorCause,
        },

        #[error("[IO error] {0}")]
        Io(#[from] io::Error),

        #[error("[storage][{action}]{cause}")]
        Storage {
            action: StorageAction,
            cause: StorageErrorCause,
        },
    }
}

/// Trait implemented by storage backends
pub trait StorageBackend {
    type Error;

    fn query(&mut self, cmd: Command) -> Result<Option<String>, Self::Error>;
}
