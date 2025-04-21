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
        username: Vec<char>,
        email: Vec<char>,
    },
    /// Update an existing row
    Update {
        id: usize,
        username: Vec<char>,
        email: Vec<char>,
    },
    /// Select row from storage
    Select,
    /// Delete row from storage
    Delete { id: usize },
}

pub fn print_row(row: &Row) -> String {
    let value = byte_to_char(row.value().as_ref()).expect("failed to convert bytes to characters");
    let mut parts = value.split(|c| *c == '\0');
    let username = parts
        .next()
        .map(|v| v.iter().copied().collect::<String>())
        .unwrap_or(String::default());
    let email = parts
        .next()
        .map(|v| v.iter().copied().collect::<String>())
        .unwrap_or(String::default());

    format!("{},{username},{email}", row.id())
}

impl From<Statement> for Row {
    fn from(value: Statement) -> Self {
        match value {
            Statement::Insert {
                id,
                mut username,
                email,
            }
            | Statement::Update {
                id,
                mut username,
                email,
            } => {
                let mut row = Row::new(id, RowType::Leaf);
                username.push('\0');
                let mut value = char_to_byte(&username);
                value.extend_from_slice(char_to_byte(&email).as_ref());
                row.set_value(value.as_ref());
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
        let username = vec!['a'];
        let email = vec!['b'];
        let stmt = Statement::Insert {
            id: 1,
            username: username.clone(),
            email: email.clone(),
        };

        let row: Row = stmt.into();
        let value = row.value();
        let characters = byte_to_char(value.as_ref()).unwrap();
        let mut value = characters.split(|c| *c == '\0');
        let returned_username = value
            .next()
            .unwrap()
            .iter()
            .map(|c| *c)
            .collect::<Vec<char>>();
        let returned_email = value
            .next()
            .unwrap()
            .iter()
            .map(|c| *c)
            .collect::<Vec<char>>();
        assert_eq!(row.id(), 1);
        assert_eq!(returned_username, username);
        assert_eq!(returned_email, email);
    }

    #[test]
    fn update_statement_to_row() {
        let username = vec!['a'];
        let email = vec!['b'];
        let stmt = Statement::Update {
            id: 1,
            username: username.clone(),
            email: email.clone(),
        };

        let row: Row = stmt.into();
        let value = row.value();
        let characters = byte_to_char(value.as_ref()).unwrap();
        let mut value = characters.split(|c| *c == '\0');
        let returned_username = value
            .next()
            .unwrap()
            .iter()
            .map(|c| *c)
            .collect::<Vec<char>>();
        let returned_email = value
            .next()
            .unwrap()
            .iter()
            .map(|c| *c)
            .collect::<Vec<char>>();
        assert_eq!(row.id(), 1);
        assert_eq!(returned_username, username);
        assert_eq!(returned_email, email);
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
