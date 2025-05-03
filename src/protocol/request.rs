use bincode::{Decode, Encode};

use crate::{Command, storage::Row};

#[derive(Debug, Encode, Decode, PartialEq, Eq)]
pub enum QueryKind {
    Select,
    Insert,
    Delete,
    Update,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq)]
pub enum Request {
    Query { kind: QueryKind, row: Vec<u8> },
    CloseConnection,
    Populate(usize),
    PrintStructure,
    Ping,
}

impl From<Command> for Request {
    fn from(value: Command) -> Self {
        match value {
            Command::Statement(statement) => {
                let kind = match statement {
                    crate::Statement::Insert { .. } => QueryKind::Insert,
                    crate::Statement::Update { .. } => QueryKind::Update,
                    crate::Statement::Select => QueryKind::Select,
                    crate::Statement::Delete { .. } => QueryKind::Delete,
                };

                let row: Row = statement.into();
                Request::Query {
                    kind,
                    row: row.as_bytes(),
                }
            }
            Command::Exit => Request::CloseConnection,
            Command::Populate(i) => Request::Populate(i),
            Command::Structure(_) => Request::PrintStructure,
            Command::Ping => Request::Ping,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Statement;

    use super::*;

    #[test]
    fn query_statement_command() {
        let statement = Statement::Insert {
            id: 0,
            username: vec![],
            email: vec![],
        };
        let command = Command::Statement(statement.clone());
        let row: Row = statement.into();
        let request: Request = command.into();

        assert_eq!(
            request,
            Request::Query {
                kind: QueryKind::Insert,
                row: row.as_bytes()
            }
        )
    }

    #[test]
    fn request_exit_command() {
        let command = Command::Exit;
        let request: Request = command.into();

        assert_eq!(request, Request::CloseConnection)
    }

    #[test]
    fn request_populate_command() {
        let command = Command::Populate(1);
        let request: Request = command.into();

        assert_eq!(request, Request::Populate(1))
    }

    #[test]
    fn request_structure_command() {
        let command = Command::Structure(None);
        let request: Request = command.into();

        assert_eq!(request, Request::PrintStructure)
    }
}
