//! Fixed-size B-Tree node representation for persistent storage.
//!
//! This module defines the [`Page`] struct, which models a single node (or "page") in the
//! B-Tree used by Cryo's storage engine. Each page is a fixed-size binary blob that holds
//! multiple [`Row`] entries and associated metadata for indexing,
//! navigation, and serialization.
//!
//! Pages are the core unit of I/O and caching. They are stored contiguously on disk and loaded
//! into memory on demand. Pages may be either **internal** nodes (holding pointers to child pages)
//! or **leaf** nodes (holding actual data rows).
//!
//! # Layout
//!
//! A page is represented on disk as a fixed-size buffer (typically 4KB), and contains:
//! - A page header: stores metadata (e.g. page type, number of rows, offsets)
//! - A row directory: points to where each row is located in the page
//! - Raw row data: serialized bytes for each [`Row`]
//!
//! This design allows for compact layout and binary search within a page, while maintaining
//! fast insert/delete operations and B-Tree navigation.
//!
//! # Page Types
//! - [`PageType::Leaf`]: Stores actual record rows.
//! - [`PageType::Internal`]: Stores keys and child page pointers.
//!
//! # See Also
//! - [`Row`]: Encapsulates record stored within a page.
//! - [`StorageEngine`](crate::storage::StorageEngine): Issues operations that ultimately read/write to pages.

use super::{
    PageError, Row, StorageError,
    row::{INTERNAL_ROW_SIZE, LEAF_ROW_SIZE},
};

/// Standard page size; 4KB
pub const PAGE_SIZE: usize = 4096;

pub const PAGE_KIND_SIZE: usize = size_of::<u8>();
pub const PAGE_HAS_PARENT_SIZE: usize = size_of::<u8>();
pub const PAGE_PARENT_SIZE: usize = size_of::<usize>();
pub const PAGE_ROWS_SIZE: usize = size_of::<usize>();
pub const PAGE_HEADER_SIZE: usize =
    PAGE_KIND_SIZE + PAGE_HAS_PARENT_SIZE + PAGE_PARENT_SIZE + PAGE_ROWS_SIZE;

pub const PAGE_TYPE: usize = 0;
pub const PAGE_HAS_PARENT: usize = PAGE_TYPE + PAGE_KIND_SIZE;
pub const PAGE_PARENT: usize = PAGE_HAS_PARENT + PAGE_HAS_PARENT_SIZE;
pub const PAGE_ROWS: usize = PAGE_PARENT + PAGE_PARENT_SIZE;

/// Flags to denote whether a page has a parent or not
pub const HAS_PARENT: u8 = 0x3;
pub const IS_ROOT: u8 = 0x4;

/// List of support page types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Internal,
    Leaf,
}

/// Representation of Page
///
/// Pages are the core unit of IO operation in the database.
#[derive(Debug, Clone)]
pub struct Page {
    pub _type: PageType,
    pub offset: usize,
    pub parent: Option<usize>,
    pub size: usize,
    rows: Vec<Row>,
}

impl Page {
    /// Create new page instance
    pub fn new(_type: PageType, parent: Option<usize>, rows: Vec<Row>, size: usize) -> Self {
        Self {
            _type,
            offset: 0,
            parent,
            rows,
            size,
        }
    }

    /// Select all rows present in the page.
    pub fn select(&mut self) -> Vec<Row> {
        self.rows.clone()
    }

    /// Inserts a new row into the page.
    ///
    /// # Errors
    ///
    /// If an existing row contains the same ID as the `row` a [`StorageError`] will be raised.
    pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        let insert =
            |items: &mut Vec<Row>, row: Row| -> Result<(usize, Option<Row>), StorageError> {
                match items.binary_search(&row) {
                    Ok(_) => Err(StorageError::Page {
                        cause: PageError::Duplicate,
                    }),
                    Err(pos) => {
                        items.insert(pos, row);
                        Ok((pos, None))
                    }
                }
            };

        self.row_task(row, insert)?;
        self.size += if self._type == PageType::Leaf {
            LEAF_ROW_SIZE
        } else {
            INTERNAL_ROW_SIZE
        };

        Ok(())
    }

    /// Update an existing row in the page.
    ///
    /// # Errors
    ///
    /// If no existing row contains the same ID as the `row` a [`StorageError`] will be raised.
    pub fn update(&mut self, row: Row) -> Result<Row, StorageError> {
        let update =
            |items: &mut Vec<Row>, row: Row| -> Result<(usize, Option<Row>), StorageError> {
                match items.binary_search(&row) {
                    Ok(pos) => {
                        let out = items.remove(pos);
                        items.insert(pos, row);
                        Ok((pos, Some(out)))
                    }
                    Err(_) => Err(StorageError::Page {
                        cause: PageError::MissingKey,
                    }),
                }
            };

        Ok(self.row_task(row, update)?.expect("row should be returned"))
    }

    /// Delete a row in the page
    ///
    /// # Errors
    ///
    /// If no existing row contains the same ID as the `row` a [`StorageError`] will be raised.
    pub fn delete(&mut self, row: Row) -> Result<(), StorageError> {
        let delete =
            |items: &mut Vec<Row>, row: Row| -> Result<(usize, Option<Row>), StorageError> {
                match items.binary_search(&row) {
                    Ok(pos) => {
                        items.remove(pos);
                        Ok((pos, None))
                    }
                    Err(_) => Err(StorageError::Page {
                        cause: PageError::MissingKey,
                    }),
                }
            };

        self.row_task(row, delete)?;
        self.size -= if self._type == PageType::Leaf {
            LEAF_ROW_SIZE
        } else {
            INTERNAL_ROW_SIZE
        };

        Ok(())
    }

    /// Returns the byte representation of the page.
    pub fn as_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0; PAGE_SIZE];

        buf[PAGE_TYPE] = self._type.into();
        if self.parent.is_some() {
            buf[PAGE_HAS_PARENT] = HAS_PARENT;
            buf[PAGE_PARENT..PAGE_ROWS].clone_from_slice(
                self.parent
                    .expect("checked in if condition")
                    .to_ne_bytes()
                    .as_ref(),
            );
        } else {
            buf[PAGE_HAS_PARENT] = IS_ROOT;
        }

        buf[PAGE_ROWS..PAGE_HEADER_SIZE].clone_from_slice(self.rows.len().to_ne_bytes().as_ref());

        let mut offset = PAGE_HEADER_SIZE;
        for row in &self.rows {
            let bytes = row.as_bytes();
            buf[offset..offset + bytes.len()].clone_from_slice(&bytes);
            offset += bytes.len();
        }

        buf
    }

    fn row_task(
        &mut self,
        row: Row,
        task: impl Fn(&mut Vec<Row>, Row) -> Result<(usize, Option<Row>), StorageError>,
    ) -> Result<Option<Row>, StorageError> {
        if self.size + row.as_bytes().len() >= PAGE_SIZE {
            return Err(StorageError::Page {
                cause: PageError::Full,
            });
        }

        Ok(match self._type {
            PageType::Internal => {
                let (pos, row) = task(&mut self.rows, row)?;
                // Ensure links are up to data after action
                if pos < self.rows.len() {
                    if pos > 0 {
                        let left = self.rows[pos].left_offset();
                        self.rows[pos - 1].set_right_offset(left);
                    }

                    if pos + 1 < self.rows.len() {
                        let right = self.rows[pos].right_offset();
                        self.rows[pos + 1].set_left_offset(right);
                    }
                }
                row
            }
            PageType::Leaf => {
                let (_, row) = task(&mut self.rows, row)?;
                row
            }
        })
    }
}

impl From<PageType> for u8 {
    fn from(value: PageType) -> Self {
        match value {
            PageType::Leaf => 0x0,
            PageType::Internal => 0x1,
        }
    }
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        match value {
            0x0 => PageType::Leaf,
            0x1 => PageType::Internal,
            _ => panic!("unknown page type"),
        }
    }
}

impl TryFrom<[u8; PAGE_SIZE]> for Page {
    type Error = StorageError;

    fn try_from(value: [u8; PAGE_SIZE]) -> Result<Self, Self::Error> {
        let _type: PageType = value[PAGE_TYPE].into();
        let is_root = value[PAGE_HAS_PARENT] == IS_ROOT;
        let parent = if is_root {
            None
        } else {
            Some(usize::from_ne_bytes(
                value[PAGE_PARENT..PAGE_ROWS]
                    .try_into()
                    .expect("should be expected byte size"),
            ))
        };
        let num_rows = usize::from_ne_bytes(
            value[PAGE_ROWS..PAGE_HEADER_SIZE]
                .try_into()
                .expect("should be expected byte size"),
        );
        let mut rows = Vec::with_capacity(num_rows);
        let mut offset = PAGE_HEADER_SIZE;

        for _ in 0..num_rows {
            let row: Row = (&value[offset..]).try_into()?;
            offset += row.as_bytes().len();
            rows.push(row);
        }

        Ok(Page::new(_type, parent, rows, offset - PAGE_HEADER_SIZE))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        storage::row::{Row, RowType},
        utilities::{USERNAME_MAX_LENGTH, char_to_byte, extend_char_array},
    };

    use super::*;

    #[test]
    fn leaf_to_bytes() {
        let page = Page::new(PageType::Leaf, None, vec![], 0);
        let bytes: [u8; PAGE_SIZE] = page.as_bytes();

        let page: Page = bytes.try_into().unwrap();
        assert_eq!(page.offset, 0);
        assert_eq!(page.size, 0);
        assert_eq!(page.parent, None);
        assert_eq!(page._type, PageType::Leaf);
        assert_eq!(page.rows, vec![])
    }

    #[test]
    fn internal_to_bytes() {
        let page = Page::new(PageType::Internal, None, vec![], 0);
        let bytes: [u8; PAGE_SIZE] = page.as_bytes();

        let page: Page = bytes.try_into().unwrap();
        assert_eq!(page.offset, 0);
        assert_eq!(page.size, 0);
        assert_eq!(page.parent, None);
        assert_eq!(page._type, PageType::Internal);
        assert_eq!(page.rows, vec![])
    }

    #[test]
    fn leaf_insert_row() {
        let mut page = Page::new(PageType::Leaf, None, vec![], 0);
        let row = Row::new(1, RowType::Leaf);
        page.insert(row.clone()).unwrap();
        assert_eq!(page.size, LEAF_ROW_SIZE);
        assert_eq!(page.rows, vec![row])
    }

    #[test]
    fn leaf_update_row() {
        let mut page = Page::new(PageType::Leaf, None, vec![], 0);
        let mut row = Row::new(1, RowType::Leaf);
        let initial = vec!['t', 'e', 's', 't'];
        row.set_username(
            char_to_byte(
                extend_char_array::<USERNAME_MAX_LENGTH>(initial, '\0')
                    .unwrap()
                    .as_ref(),
            )
            .as_slice()
            .try_into()
            .unwrap(),
        );
        page.insert(row).unwrap();

        let mut row = Row::new(1, RowType::Leaf);
        let expected = vec!['c', 'h', 'a', 'n', 'g', 'e', 'd'];
        let expected = char_to_byte(
            extend_char_array::<USERNAME_MAX_LENGTH>(expected, '\0')
                .unwrap()
                .as_ref(),
        );

        row.set_username(expected.as_slice().try_into().unwrap());
        page.update(row.clone()).unwrap();
        assert_eq!(page.rows, vec![row]);
    }

    #[test]
    fn leaf_delete_row() {
        let mut page = Page::new(PageType::Leaf, None, vec![], 0);
        let mut row = Row::new(1, RowType::Leaf);
        let initial = vec!['t', 'e', 's', 't'];
        row.set_username(
            char_to_byte(
                extend_char_array::<USERNAME_MAX_LENGTH>(initial, '\0')
                    .unwrap()
                    .as_ref(),
            )
            .as_slice()
            .try_into()
            .unwrap(),
        );
        page.insert(row).unwrap();

        let row = Row::new(1, RowType::Leaf);
        page.delete(row).unwrap();

        assert_eq!(page.rows, vec![]);
    }

    #[test]
    fn internal_insert_cell() {
        let mut page = Page::new(PageType::Internal, None, vec![], 0);
        let row = Row::new(1, RowType::Internal);
        page.insert(row).unwrap();
        assert_eq!(page.size, INTERNAL_ROW_SIZE);
    }

    #[test]
    fn leaf_select() {
        let mut page = Page::new(PageType::Leaf, None, vec![], 0);
        let row = Row::new(1, RowType::Leaf);
        page.insert(row.clone()).unwrap();
        assert_eq!(page.select(), vec![row]);
    }

    #[test]
    #[should_panic(expected = "Duplicate")]
    fn insert_duplicate() {
        let mut page = Page::new(PageType::Leaf, None, vec![], 0);
        let row = Row::new(1, RowType::Leaf);
        page.insert(row.clone()).unwrap();
        page.insert(row).unwrap();
    }

    #[test]
    #[should_panic(expected = "MissingKey")]
    fn delete_non_existent() {
        let mut page = Page::new(PageType::Leaf, None, vec![], 0);
        let row = Row::new(1, RowType::Leaf);
        page.delete(row).unwrap();
    }

    #[test]
    #[should_panic(expected = "MissingKey")]
    fn update_non_existent() {
        let mut page = Page::new(PageType::Leaf, None, vec![], 0);
        let row = Row::new(1, RowType::Leaf);
        page.update(row).unwrap();
    }
}
