//! High-level command module for communication.
//!
//! This module defines the [`Command`] struct, which encapsulates high-level user commands
//! and their associated arguments. These commands are used to interact with the underlying
//! storage engine (e.g., inserting, retrieving, or deleting records).
//!
//! It serves as an abstraction between the CLI parser and the storage backend, providing
//! a consistent interface for executing various storage operations.
//!
//! # Overview
//! The `Command` enum represents supported operations such as:
//!
//! - `Exit`: Close the current session.
//! - `Statement(String)`: Pass possible statement to underlying storage for execution.
//! - `Populate(usize)`: Populate the storage with test data for debugging.
//! - `Structure`: Print out the storage backends current structure.
//!
//! These commands are executed through a [`StorageEngine`](crate::storage) `query` call. And,
//! provides the ability to try parsing a command from a user-inputted string
//!
//! # Example
//! ```rust
//! use cryo::{ Command, Statement };
//!
//! let cmd: Command = "select".try_into().unwrap();
//! assert_eq!(cmd, Command::Statement(Statement::Select));
//! ```
//!
//! # See Also
//! - [`StorageEngine`](crate::storage): Trait that defines the storage engine interface.
use std::path::PathBuf;

use thiserror::Error;

use crate::{
    Statement,
    utilities::{EMAIL_MAX_LENGTH, USERNAME_MAX_LENGTH},
};

/// List of possible error that a command can throw.
#[derive(Debug, Error, Clone)]
pub enum CommandError {
    #[error("unrecognized command '{0}'")]
    UnrecognizedCommand(String),

    #[error("invalid '{command}' command, {reason}")]
    InvalidCommandArguments { command: String, reason: String },

    #[error("invalid statement, {reason}")]
    InvalidStatement { reason: String },

    #[error("unsupported statement '{0}'")]
    UnrecognizedStatement(String),

    #[error("no command provided")]
    Empty,
}

/// High-level user supplied commands to execute on a StorageEngine
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Possible statement to provide to the storage engine
    Statement(Statement),
    /// Requests the storage engine to close connection and
    /// terminate process
    Exit,
    /// Requests the storage engine to populate the database
    /// with test data.
    Populate(usize),
    /// Requests the storage engine to write out a representation
    /// of it's storage structure.
    Structure(Option<PathBuf>),
    /// Checks if the connect is still open.
    Ping,
}

impl TryInto<Command> for &str {
    type Error = CommandError;

    fn try_into(self) -> Result<Command, Self::Error> {
        match self.trim() {
            ".exit" => Ok(Command::Exit),
            ".ping" => Ok(Command::Ping),
            s if s.starts_with(".structure") => {
                let parts = s.split(' ').collect::<Vec<&str>>();

                let path = if parts.len() == 2 {
                    Some(PathBuf::from(parts[1]))
                } else {
                    None
                };
                Ok(Command::Structure(path))
            }
            s if s.starts_with(".populate") => {
                let parts = s.split(' ').collect::<Vec<&str>>();
                if parts.len() < 2 {
                    return Err(CommandError::InvalidCommandArguments {
                        command: parts[1].to_string(),
                        reason:
                            "requires integer argument for number of records. Example: .populate 10"
                                .to_string(),
                    });
                }

                let records = parts[1].parse::<usize>().map_err(|_| {
                    CommandError::InvalidCommandArguments {
                        command: ".populate".to_string(),
                        reason:
                            "invalid integer argument; argument should be a non-negative number."
                                .to_string(),
                    }
                })?;
                Ok(Command::Populate(records))
            }
            s if s.to_lowercase().starts_with("select") => {
                Ok(Command::Statement(Statement::Select))
            }
            s if s.to_lowercase().starts_with("delete") => {
                let parts = s.split(' ').skip(1).collect::<Vec<&str>>();

                if parts.is_empty() {
                    return Err(CommandError::InvalidStatement {
                        reason: "delete statement requires an id. Example: delete 1".to_string(),
                    });
                }

                let id = parts[0]
                    .parse::<usize>()
                    .map_err(|_| CommandError::InvalidStatement {
                        reason: "delete statement requires a valid non-negative integer."
                            .to_string(),
                    })?;

                Ok(Command::Statement(Statement::Delete { id }))
            }
            s if s.to_lowercase().starts_with("insert")
                || s.to_lowercase().starts_with("update") =>
            {
                let parts = s.split(' ').collect::<Vec<&str>>();

                if parts.len() < 4 {
                    return Err(CommandError::InvalidStatement {
                        reason: format!(
                            "{0} statement requires id, username, email fields. Example: {0} 1 test test@example.com",
                            parts[0]
                        ),
                    });
                }

                let id = parts[1]
                    .parse::<usize>()
                    .map_err(|_| CommandError::InvalidStatement {
                        reason: format!(
                            "{} statement requires a valid non-negative integer.",
                            parts[0]
                        ),
                    })?;

                let username: Vec<char> = parts[2].chars().collect();
                if username.len() > USERNAME_MAX_LENGTH {
                    return Err(CommandError::InvalidStatement {
                        reason: format!(
                            "username should be less than or equal to {USERNAME_MAX_LENGTH}."
                        ),
                    });
                }
                let email: Vec<char> = parts[3].chars().collect();
                if email.len() > EMAIL_MAX_LENGTH {
                    return Err(CommandError::InvalidStatement {
                        reason: format!(
                            "email should be less than or equal to {EMAIL_MAX_LENGTH}."
                        ),
                    });
                }

                let stmt = if parts[0] == "insert" {
                    Statement::Insert {
                        id,
                        username,
                        email,
                    }
                } else {
                    Statement::Update {
                        id,
                        username,
                        email,
                    }
                };

                Ok(Command::Statement(stmt))
            }
            s => Err(CommandError::UnrecognizedCommand(s.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_from_string() {
        let inputs = vec![
            (".exit", Command::Exit),
            (".structure", Command::Structure(None)),
            (".populate 10", Command::Populate(10)),
            ("select", Command::Statement(Statement::Select)),
        ];

        for (cmd, expected) in inputs {
            let command: Command = cmd.try_into().unwrap();
            assert_eq!(command, expected);
        }
    }
}
