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
use crate::{
    storage::row::{Row, RowType},
    utilities::*,
};

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

impl From<Statement> for Row {
    fn from(value: Statement) -> Self {
        match value {
            Statement::Insert {
                id,
                username,
                email,
            }
            | Statement::Update {
                id,
                username,
                email,
            } => {
                let mut row = Row::new(id, RowType::Leaf);
                let username = char_to_byte(&username);
                let email = char_to_byte(email.as_ref());

                row.set_username(
                    username[..]
                        .try_into()
                        .expect("statement <-> row username size discrepancy"),
                );
                row.set_email(
                    email[..]
                        .try_into()
                        .expect("statement <-> row email size discrepancy"),
                );

                row
            }
            Statement::Select => Row::new(0, RowType::Leaf),
            Statement::Delete { id } => Row::new(id, RowType::Leaf),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_statement_to_row() {
        let username = extend_char_array::<USERNAME_MAX_LENGTH>(vec!['a'], '\0').unwrap();
        let email = extend_char_array::<EMAIL_MAX_LENGTH>(vec!['b'], '\0').unwrap();
        let stmt = Statement::Insert {
            id: 1,
            username,
            email: Box::new(email),
        };

        let row: Row = stmt.into();
        assert_eq!(row.id(), 1);
        assert_eq!(row.username(), char_to_byte(&username));
        assert_eq!(row.email(), char_to_byte(&email));
    }

    #[test]
    fn update_statement_to_row() {
        let username = extend_char_array::<USERNAME_MAX_LENGTH>(vec!['a'], '\0').unwrap();
        let email = extend_char_array::<EMAIL_MAX_LENGTH>(vec!['b'], '\0').unwrap();
        let stmt = Statement::Update {
            id: 1,
            username,
            email: Box::new(email),
        };

        let row: Row = stmt.into();
        assert_eq!(row.id(), 1);
        assert_eq!(row.username(), char_to_byte(&username));
        assert_eq!(row.email(), char_to_byte(&email));
    }

    #[test]
    fn select_statement_to_row() {
        let stmt = Statement::Select;
        let row: Row = stmt.into();
        assert_eq!(row.id(), 0);
    }

    #[test]
    fn delete_statement_to_row() {
        let stmt = Statement::Delete { id: 1 };
        let row: Row = stmt.into();
        assert_eq!(row.id(), 1)
    }
}
