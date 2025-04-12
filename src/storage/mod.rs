mod btree;
mod row;

use thiserror::Error;

use crate::{Statement, cli::Command};
pub use btree::BTreeStorage;

pub trait StorageBackend {
    type Error;
    type Output;

    fn query(&mut self, cmd: Command) -> Result<Option<Self::Output>, Self::Error>;
    fn insert(&mut self, statement: Statement) -> Result<(), Self::Error>;
    fn select(&self, statement: Statement) -> Result<Self::Output, Self::Error>;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("corrupted data: {0}")]
    Data(String),

    #[error("operation failed: duplicate key")]
    DuplicateKey,
}
