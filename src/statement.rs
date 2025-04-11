use thiserror::Error;

use crate::cli::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Insert,
    Select,
}

#[derive(Error, Debug)]
pub enum StatementError {
    #[error("empty statement")]
    EmptyStatement,

    #[error("statement not supported: {0}")]
    Unsupported(String),

    #[error("failed to convert into statement")]
    Conversion,
}

impl TryFrom<Command> for Statement {
    type Error = StatementError;

    fn try_from(value: Command) -> Result<Self, Self::Error> {
        match value {
            Command::Statement(s) => {
                let parts = s.split(' ').collect::<Vec<&str>>();

                match parts[0].to_lowercase().as_str() {
                    "insert" => Ok(Statement::Insert),
                    "select" => Ok(Statement::Select),
                    kind => {
                        if kind.is_empty() {
                            Err(StatementError::EmptyStatement)
                        } else {
                            Err(StatementError::Unsupported(s))
                        }
                    }
                }
            }
            _ => Err(StatementError::Conversion),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "Unsupported")]
    fn command_to_unsupported_statement() {
        let command = Command::Statement(String::from("get from somewhere"));
        let _: Statement = command.try_into().unwrap();
    }

    #[test]
    #[should_panic(expected = "EmptyStatement")]
    fn command_to_empty_statement() {
        let command = Command::Statement(String::default());

        let _: Statement = command.try_into().unwrap();
    }

    #[test]
    fn command_to_statement_case_insensitive() {
        let command = Command::Statement(String::from("SELECT"));

        assert_eq!(Statement::Select, command.try_into().unwrap());
    }

    #[test]
    fn command_to_insert_statement() {
        let command = Command::Statement(String::from("insert"));

        assert_eq!(Statement::Insert, command.try_into().unwrap());
    }

    #[test]
    fn command_to_select_statement() {
        let command = Command::Statement(String::from("select"));

        assert_eq!(Statement::Select, command.try_into().unwrap());
    }
}
