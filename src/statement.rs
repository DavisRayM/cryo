use thiserror::Error;

use crate::cli::Command;

pub const USERNAME_MAX_LENGTH: usize = 32;
pub const EMAIL_MAX_LENGTH: usize = 255;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Insert {
        id: usize,
        username: [char; USERNAME_MAX_LENGTH],
        email: Box<[char; EMAIL_MAX_LENGTH]>,
    },
    Select,
    Update {
        id: usize,
        username: [char; USERNAME_MAX_LENGTH],
        email: Box<[char; EMAIL_MAX_LENGTH]>,
    },
    Delete {
        id: usize,
    },
}

#[derive(Error, Debug)]
pub enum StatementError {
    #[error("empty statement")]
    EmptyStatement,

    #[error("statement not supported: {0}")]
    Unsupported(String),

    #[error("invalid statement: {0}")]
    InvalidStatement(String),

    #[error("failed to convert into statement")]
    Conversion,
}

impl TryFrom<Command> for Statement {
    type Error = StatementError;

    fn try_from(value: Command) -> Result<Self, Self::Error> {
        match value {
            Command::Statement(s) => {
                let mut parts = s.split(' ');

                match parts.next().unwrap_or("").to_lowercase().as_str() {
                    "delete" => {
                        let content = parts.collect::<Vec<&str>>();
                        if content.is_empty() {
                            return Err(StatementError::InvalidStatement(
                                "delete requires id.".into(),
                            ));
                        }

                        let id = content[0].parse::<usize>().map_err(|_| {
                            StatementError::InvalidStatement(format!(
                                "insert 'id' field should be an integer, got '{}'",
                                content[0]
                            ))
                        })?;

                        Ok(Statement::Delete { id })
                    }
                    p if p == "insert" || p == "update" => {
                        let content = parts.collect::<Vec<&str>>();
                        if content.len() < 3 {
                            return Err(StatementError::InvalidStatement(format!(
                                "{p} requires id, username, email fields."
                            )));
                        }

                        let id = content[0].parse::<usize>().map_err(|_| {
                            StatementError::InvalidStatement(format!(
                                "insert 'id' field should be an integer, got '{}'",
                                content[0]
                            ))
                        })?;

                        let username = convert_to_char_array::<USERNAME_MAX_LENGTH>(
                            content[1].chars().collect(),
                            '\0',
                        )
                        .map_err(|_| {
                            StatementError::InvalidStatement(format!(
                                "username should be less than {USERNAME_MAX_LENGTH} characters"
                            ))
                        })?;
                        let email = convert_to_char_array::<EMAIL_MAX_LENGTH>(
                            content[2].chars().collect(),
                            '\0',
                        )
                        .map_err(|_| {
                            StatementError::InvalidStatement(format!(
                                "email should be less than {EMAIL_MAX_LENGTH} characters"
                            ))
                        })?;

                        if p == "insert" {
                            Ok(Statement::Insert {
                                id,
                                username,
                                email: Box::new(email),
                            })
                        } else {
                            Ok(Statement::Update {
                                id,
                                username,
                                email: Box::new(email),
                            })
                        }
                    }
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

pub(crate) fn convert_to_char_array<const N: usize>(
    mut src: Vec<char>,
    fill: char,
) -> Result<[char; N], String> {
    if src.len() > N {
        return Err("source character array is larger than size".into());
    }

    src.resize(N, fill);
    Ok(src.try_into().expect("should be correct length"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "email should be less than")]
    fn insert_statement_long_email() {
        let long_email = (0..EMAIL_MAX_LENGTH + 1).map(|_| "a").collect::<String>();
        let command = Command::Statement(format!("insert 1 dave {long_email}"));

        let _: Statement = command.try_into().unwrap();
    }

    #[test]
    #[should_panic(expected = "username should be less than")]
    fn insert_statement_long_username() {
        let long_username = (0..USERNAME_MAX_LENGTH + 1)
            .map(|_| "a")
            .collect::<String>();
        let command = Command::Statement(format!("insert 1 {long_username} dave@example.com"));

        let _: Statement = command.try_into().unwrap();
    }

    #[test]
    fn convert_to_char_arr() {
        const EXPECTED_SIZE: usize = 5;
        let initial = vec!['a', 'b', 'c'];
        let expected: [char; EXPECTED_SIZE] = ['a', 'b', 'c', '_', '_'];

        assert_eq!(
            expected,
            convert_to_char_array::<EXPECTED_SIZE>(initial, '_').unwrap()
        )
    }

    #[test]
    #[should_panic(expected = "larger than size")]
    fn convert_to_char_greater_length() {
        const EXPECTED_SIZE: usize = 1;
        let initial = vec!['a', 'b', 'c'];

        convert_to_char_array::<EXPECTED_SIZE>(initial, '_').unwrap();
    }

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
        let id = 1;
        let username: String = "davis".into();
        let email: String = "git@davisraym.com".into();
        let command = Command::Statement(format!("insert {id} {username} {email}"));

        assert_eq!(
            Statement::Insert {
                id,
                username: convert_to_char_array::<USERNAME_MAX_LENGTH>(
                    username.chars().collect(),
                    '\0'
                )
                .unwrap(),
                email: Box::new(
                    convert_to_char_array::<EMAIL_MAX_LENGTH>(email.chars().collect(), '\0')
                        .unwrap()
                ),
            },
            command.try_into().unwrap()
        );
    }

    #[test]
    fn command_to_select_statement() {
        let command = Command::Statement(String::from("select"));

        assert_eq!(Statement::Select, command.try_into().unwrap());
    }
}
