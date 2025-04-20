//! Core abstractions and implementation for data storage.
//!
//! This module defines the core traits and types for managing data storage within Cryo,
//! including the [`StorageEngine`] trait, which outlines how storage backends interact with
//! the rest of the system.
//!
//! It also includes concrete storage implementations, such as a BTree backend, and
//! structures for representing persistent data (e.g., pages and rows) both in memory and on disk.
//!
//! # Overview
//!
//! Cryo’s storage layer is designed to be modular and extensible. At the heart of this
//! module is the [`StorageEngine`] trait, which defines a consistent interface for
//! executing DSL-level operations like inserts, reads, and deletes, regardless of the
//! underlying storage format.
//!
//! # Key Components
//!
//! - [`StorageEngine`]: A trait that defines high-level operations (`select`, `insert`, `delete`) on a storage engine.
//! - [`Row`]: A record stored inside a page; has an in-memory and binary representation.
//!
//! # In-Memory vs On-Disk Representation
//!
//! - **In-memory**: Data structures like `Row` and `Page` are represented using rich types (`Vec<u8>`, structs) for ergonomic and efficient access.
//! - **On-disk**: Pages are serialized into a compact binary format, including metadata like offsets and lengths. `Row` structures are encoded into byte sequences stored inside pages.
//!
//! # See Also
//! - [`statement`](crate::statement): Frontend statements that are evaluated by a storage engine.
pub mod btree;
pub mod page;
pub mod pager;
pub mod row;

use std::{error::Error, io};

use thiserror::Error;

use crate::{Command, Statement};
pub use row::Row;

/// List of possible errors that can be thrown by the Storage module
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("page error: {cause}")]
    Page { cause: PageError },

    #[error("paging error: {cause}")]
    Pager { cause: PagerError },

    #[error("engine error during {action}: {cause}")]
    Engine {
        action: EngineAction,
        cause: Box<dyn Error>,
    },
}

#[derive(Debug, Error)]
pub enum PageError {
    #[error("out of space")]
    Full,
    #[error("key does not exist")]
    MissingKey,
    #[error("duplicate row")]
    Duplicate,
}

#[derive(Debug, Error)]
pub enum PagerError {
    #[error("io error; {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum EngineAction {
    #[error("insert")]
    Insert,
    #[error("split")]
    Split,
}

/// `StorageEngine` defines the interface through which higher-level components
/// (such as DSL statements or CLI commands) interact with the storage layer.
/// It abstracts over various storage implementations (in-memory, file-backed, etc.)
/// and provides a consistent set of operations for manipulating data.
///
/// # Implementors
/// - (Future) File-backed storage engines based on B-Tree and paging.
///
/// # See Also
/// - [`Statement`]: DSL-level abstraction that uses this trait to perform operations.
/// - [`Row`]: Represents the atomic unit of data managed by a storage engine.
pub trait StorageEngine {
    fn execute(&mut self, command: Command) -> Result<(), StorageError>;
    fn evaluate_statement(&mut self, statement: Statement) -> Result<Option<String>, StorageError>;
}
