//! DSL Statement representation
//!
//! This module defines the [`Statement`] enum, which represents the supported domain-specific
//! language (DSL) statements that can be executed against a storage backend.
//!
//! A `Statement` is an abstract representation of an operation like inserting, retrieving,
//! or deleting data. Unlike [`Command`](crate::Command), which is meant for user-facing
//! CLI interactions, `Statement` focuses on the underlying intent and is typically generated
//! by a converting a [`Command::Statement`](crate::Command).
//!
//! # Overview
//!
//! The DSL is designed to be simple and expressive, enabling the construction of
//! higher-level interfaces such as CLI or network protocols.
//!
//! Supported statements include:
//! - `Select`: Select all or a specific row from the storage
//! - `Insert`: Store a row entry in the storage
//! - `Update`: Update a row entry present in the storage
//! - `Delete`: Deletes a row entry present in the storage
//!
//! # Example
//! ```rust
//! use cryo::{Statement, Command};
//!
//! let cmd: Command = "select".try_into().unwrap();
//! assert_eq!(cmd, Command::Statement(Statement::Select));
//! ```
//!
//! # See Also
//! - [`Command`](crate::Command): Higher-level command abstraction used in the CLI layer.
use crate::utilities::*;

/// An action that affects a row to be performed by the Storage engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// Insert a new row
    Insert {
        id: usize,
        username: [char; USERNAME_MAX_LENGTH],
        email: Box<[char; EMAIL_MAX_LENGTH]>,
    },
    /// Update an existing row
    Update {
        id: usize,
        username: [char; USERNAME_MAX_LENGTH],
        email: Box<[char; EMAIL_MAX_LENGTH]>,
    },
    /// Select row from storage
    Select,
    /// Delete row from storage
    Delete { id: usize },
}
