use super::{Command, Statement, StorageBackend, StorageError, row::Row};

#[derive(Debug)]
pub struct Page {
    id: usize,
    rows: Vec<Row>,
}

impl Page {
    pub fn new(id: usize) -> Self {
        Self {
            id,
            rows: Vec::new(),
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        match self.rows.binary_search(&row) {
            Ok(_) => Err(StorageError::DuplicateKey),
            Err(pos) => {
                self.rows.insert(pos, row);
                Ok(())
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct BTreeStorage {
    pages: Vec<Page>,
    current_page: usize,
}

impl BTreeStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn btree_insert(&mut self, row: Row) -> Result<(), StorageError> {
        let page = match self.pages.get_mut(self.current_page) {
            Some(page) => page,
            None => {
                self.pages
                    .insert(self.current_page, Page::new(self.current_page));
                &mut self.pages[self.current_page]
            }
        };
        page.insert(row)
    }

    fn btree_select(&self) -> Result<Vec<Row>, StorageError> {
        let page = match self.pages.get(self.current_page) {
            Some(page) => page,
            None => {
                return Ok(Vec::new());
            }
        };

        Ok(page.rows.clone())
    }
}

impl StorageBackend for BTreeStorage {
    type Error = StorageError;
    type Output = String;

    fn query(&mut self, cmd: Command) -> Result<Option<Self::Output>, Self::Error> {
        let stmt: Statement = cmd.try_into().unwrap();

        println!("query received: {:?}", stmt);

        match stmt {
            Statement::Select => Ok(Some(self.select(stmt)?)),
            Statement::Insert { .. } => {
                self.insert(stmt)?;
                Ok(None)
            }
        }
    }

    fn insert(&mut self, statement: Statement) -> Result<(), Self::Error> {
        let row: Row = statement
            .try_into()
            .map_err(|e| StorageError::Data(format!("invalid insert statement: {:?}", e)))?;
        println!("insert: {}", row);
        self.btree_insert(row)
    }

    fn select(&self, _: Statement) -> Result<Self::Output, Self::Error> {
        println!("select");
        let rows = self.btree_select()?;
        let mut out = String::default();

        for row in rows {
            let repr = format!("{}\n", row);
            out.push_str(&repr);
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::{EMAIL_MAX_LENGTH, USERNAME_MAX_LENGTH, convert_to_char_array};

    use super::*;

    fn select<T>(backend: &mut T) -> Result<T::Output, T::Error>
    where
        T: StorageBackend,
    {
        let stmt = Statement::Select;
        backend.select(stmt)
    }

    fn insert<T>(backend: &mut T, id: usize, username: &str, email: &str) -> Result<(), T::Error>
    where
        T: StorageBackend,
    {
        let stmt = Statement::Insert {
            id,
            username: convert_to_char_array::<USERNAME_MAX_LENGTH>(
                username.chars().collect(),
                '\0',
            )
            .unwrap(),
            email: Box::new(
                convert_to_char_array::<EMAIL_MAX_LENGTH>(email.chars().collect(), '\0').unwrap(),
            ),
        };
        backend.insert(stmt)
    }

    #[test]
    #[should_panic(expected = "DuplicateKey")]
    fn insert_duplicate_record() {
        let id = 1;
        let username = "davis";
        let email = "git@davisraym.com";

        let mut backend = BTreeStorage::new();
        insert(&mut backend, id, username, email).unwrap();
        let username = "some_thing_else";
        let email = "git@davisraym.com";
        insert(&mut backend, id, username, email).unwrap();
    }

    #[test]
    fn insert_record() {
        let id = 1;
        let username = "davis";
        let email = "git@davisraym.com";

        let mut backend = BTreeStorage::new();
        insert(&mut backend, id, username, email).unwrap();
    }

    #[test]
    fn select_record() {
        let mut backend = BTreeStorage::new();

        let id = 1;
        let username = "davis";
        let email = "git@davisraym.com";
        insert(&mut backend, id, username, email).unwrap();

        let actual = select(&mut backend).unwrap();
        let expected = format!("{} {} {}\n", id, username, email);
        assert_eq!(expected, actual)
    }

    #[test]
    fn query_backend() {
        let cmd = Command::Statement("insert 1 davis git@davisraym".into());
        let mut backend = BTreeStorage::new();
        backend.query(cmd).unwrap();
    }
}
