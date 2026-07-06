use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("accessed invalid page: {0}")]
    InvalidPage(String),

    #[error("storage data corrupted: {0}")]
    CorruptedData(String),

    #[error("outdated or unsupported table format version")]
    FormatVersion,

    #[error("cache miss: page {0} is not currently tracked")]
    CacheMiss(usize),

    #[error("page is currently in use")]
    PagePinned,

    #[error("all pages in use")]
    AllInUse,

    #[error("action not allowed: {0}")]
    NotAllowed(&'static str),

    #[error("internal lock poisoned")]
    LockPoisoned,

    #[error("recursion detected: {0}")]
    RecursionDetected(&'static str),

    #[error("action would split current tree")]
    WouldSplit,

    #[error("page requires defragmentation")]
    FragmentedPage,
}

pub type Result<T> = std::result::Result<T, StorageError>;
