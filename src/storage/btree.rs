use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::storage::{
    error::{StorageAction, StorageErrorCause},
    header::page::PAGE_SIZE,
};

use super::{
    error::{PageErrorCause, StorageError},
    page::{Page, PageKind},
    row::Row,
};

const PAGE_IN_MEMORY: usize = 5;

#[derive(Debug, Default)]
pub struct BTreeStorage {
    pub pages: usize,
    pub root: usize,
    pub current: usize,
    cached: VecDeque<Arc<RefCell<Page>>>,
    path: Option<PathBuf>,
}

impl BTreeStorage {
    /// Create a new BTreeStorage backend and configures persistence to the directory
    pub fn new(dir: PathBuf) -> Result<Self, StorageError> {
        let path = dir.join("btree.db");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(&path)?;
        let size = f.metadata()?.len() as usize;

        let pages = size / PAGE_SIZE;
        let root = 0;

        let mut storage = Self {
            pages,
            root,
            current: root,
            cached: VecDeque::with_capacity(PAGE_IN_MEMORY),
            path: Some(path),
        };

        if pages == 0 {
            storage.create(PageKind::Leaf { rows: vec![] }, 0, 0)?;
        } else {
            let mut pos = 0;
            let mut parent = storage.page(pos)?.borrow().parent;
            while pos != parent {
                pos = parent;
                parent = storage.page(pos)?.borrow().parent;
            }
        }
        Ok(storage)
    }

    /// Walks the BTree and prints all the nodes
    pub(crate) fn walk(&mut self, width: Option<usize>) -> Result<String, StorageError> {
        let page_offset = self.current;
        let page = self.page(page_offset)?;
        let mut out = String::default();
        let page = page.borrow().clone();
        let width = width.unwrap_or(0);

        if page.leaf() {
            out += format!(
                "{:width$}leaf {} {}\n",
                "",
                page.id,
                page.cells,
                width = width
            )
            .as_ref();
        } else {
            out += format!(
                "{:width$}internal {} {}\n",
                "",
                page.id,
                page.cells,
                width = width
            )
            .as_ref();
            let node = page.select()?;
            for child in node {
                self.current = child.offset()?;
                out += self.walk(Some(width + 2))?.as_ref();
            }
        }

        Ok(out)
    }

    /// Flushes pager cache to disk
    pub fn close(mut self) -> Result<(), StorageError> {
        while !self.cached.is_empty() {
            self.free()?;
        }
        Ok(())
    }

    /// Creates a new page and returns the offset to the page
    fn create(
        &mut self,
        kind: PageKind,
        cells: usize,
        parent: usize,
    ) -> Result<usize, StorageError> {
        let offset = self.pages * PAGE_SIZE;
        let page = Page::new(offset, self.pages, kind, cells, parent);

        if let Some(path) = self.path.take() {
            let f = OpenOptions::new().write(true).open(&path)?;
            let mut writer = BufWriter::new(f);

            writer.seek(SeekFrom::Start(offset as u64))?;
            let buf: [u8; PAGE_SIZE] = page.into();
            writer.write_all(&buf)?;
            writer.flush()?;
            self.path = Some(path);
            self.pages += 1;
            self.page(self.pages - 1)?;
        } else {
            self.cached.push_back(Arc::new(RefCell::new(page)));
            self.pages += 1;
        };

        Ok(offset)
    }

    /// Inserts a new Row into the BTree storage
    fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        let page_offset = self.current;
        let page = self.page(page_offset)?;

        if page.borrow().leaf() {
            let mut page = page.borrow().clone();
            match page.insert(row.clone()) {
                Ok(_) => {
                    self.page(page_offset)?.borrow_mut().cells = page.cells;
                    self.page(page_offset)?.borrow_mut().kind = page.kind;
                    Ok(())
                }
                Err(StorageError::Page {
                    cause: PageErrorCause::Full,
                    ..
                }) => {
                    self.split_leaf(page_offset == self.root)?;
                    self.insert(row)
                }
                Err(e) => Err(StorageError::Storage {
                    action: StorageAction::Insert,
                    cause: StorageErrorCause::Error(Box::new(e)),
                }),
            }
        } else {
            let children = page.borrow().clone().select()?;
            let pos = match children.binary_search(&row) {
                Ok(pos) => children[pos].offset()?,
                Err(pos) => {
                    if pos == 0 {
                        self.current = self.root;
                        eprintln!("{}", self.walk(None).unwrap());
                    }
                    children[pos - 1].offset()?
                }
            };
            self.current = pos;
            self.insert(row)
        }
    }

    /// Splits an Internal node at the current position
    fn split_internal(&mut self, root: bool) -> Result<(), StorageError> {
        if root {
            let parent_offset =
                self.create(PageKind::Internal { offsets: vec![] }, 0, self.root)?;
            let child_offset = self.current;
            let right_offset =
                self.create(PageKind::Internal { offsets: vec![] }, 0, parent_offset)?;

            // Link child to parent
            self.page(child_offset)?.borrow_mut().parent = parent_offset;
            self.page(parent_offset)?.borrow_mut().parent = parent_offset;

            // Add children
            for child in vec![child_offset, right_offset] {
                let mut row = Row::new();
                row.set_offset(self.page(child)?.borrow().offset);
                row.set_id(self.page(child)?.borrow().id);
                self.page(parent_offset)?.borrow_mut().insert(row)?;
            }

            // Ensure root parent ID is itself.
            self.root = parent_offset;
            self.current = right_offset;
            Ok(())
        } else {
            let parent = self.page(self.current)?.borrow().parent;
            let offset = self.create(PageKind::Internal { offsets: vec![] }, 0, parent)?;
            self.current = parent;
            self.split_insert(offset)
        }
    }

    /// Splits a Leaf node at the current position
    ///
    /// TODO: Handle median/max keys
    fn split_leaf(&mut self, root: bool) -> Result<(), StorageError> {
        if root {
            let parent_offset =
                self.create(PageKind::Internal { offsets: vec![] }, 0, self.root)?;
            let child_offset = self.current;
            let right_offset = self.create(PageKind::Leaf { rows: vec![] }, 0, parent_offset)?;

            // Link child to parent
            self.page(child_offset)?.borrow_mut().parent = parent_offset;
            self.page(parent_offset)?.borrow_mut().parent = parent_offset;

            // Add children
            for child in vec![child_offset, right_offset] {
                let mut row = Row::new();
                row.set_offset(self.page(child)?.borrow().offset);
                row.set_id(self.page(child)?.borrow().id);
                self.page(parent_offset)?.borrow_mut().insert(row)?;
            }

            // Ensure root parent ID is itself.
            self.root = parent_offset;
            self.current = self.root;
            Ok(())
        } else {
            let parent = self.page(self.current)?.borrow().parent;
            let offset = self.create(PageKind::Leaf { rows: vec![] }, 0, parent)?;
            self.current = parent;
            self.split_insert(offset)
        }
    }

    /// Splits the current node and inserts a new child to it
    fn split_insert(&mut self, child: usize) -> Result<(), StorageError> {
        let mut row = Row::new();
        let offset = self.page(child)?.borrow().offset;
        let id = self.page(child)?.borrow().id;

        row.set_id(id);
        row.set_offset(offset);

        let res = self.page(self.current)?.borrow_mut().insert(row);
        if let Ok(_) = res {
            return Ok(());
        }

        if let Err(StorageError::Page {
            cause: PageErrorCause::Full,
            ..
        }) = res
        {
            let root = self.current == self.root;
            self.split_internal(root)?;

            let mut row = Row::new();
            row.set_id(id);
            row.set_offset(offset);

            self.page(self.current)?.borrow_mut().insert(row)?;
            self.page(child)?.borrow_mut().parent = self.current;
            return Ok(());
        }

        res
    }

    // Clear a page from cache and write it to disk
    //
    // # Panics
    // If no path has been configured for the storage
    fn free(&mut self) -> Result<(), StorageError> {
        if let Some(path) = self.path.take() {
            let page = self.cached.pop_front().ok_or(StorageError::Storage {
                action: StorageAction::PageOut,
                cause: StorageErrorCause::Unknown,
            })?;
            let page = page.borrow().clone();
            let offset = page.offset;
            let bytes: [u8; PAGE_SIZE] = page.into();

            let f = OpenOptions::new().write(true).open(&path)?;
            let mut writer = BufWriter::new(f);

            writer.seek(SeekFrom::Start(offset as u64))?;
            writer.write_all(&bytes)?;
            self.path = Some(path);
            Ok(())
        } else {
            Err(StorageError::Storage {
                action: StorageAction::PageOut,
                cause: StorageErrorCause::Unknown,
            })
        }
    }

    /// Retrieves page from cache if present of loads it from disk.
    ///
    /// # Errors
    /// - If `offset` is OutOfBounds
    /// - If IO error occurs
    /// - If failed to load page from disk
    fn page(&mut self, offset: usize) -> Result<Arc<RefCell<Page>>, StorageError> {
        if offset >= self.pages * PAGE_SIZE {
            return Err(StorageError::Storage {
                action: StorageAction::Page,
                cause: StorageErrorCause::OutOfBounds,
            });
        }

        let cached = self.cached.len();

        for page in self.cached.iter() {
            if page.borrow().offset == offset {
                return Ok(Arc::clone(&page));
            }
        }

        if cached >= PAGE_IN_MEMORY {
            self.free()?;
        }

        if let Some(path) = self.path.take() {
            let offset = offset;
            let f = OpenOptions::new().read(true).open(&path)?;
            let mut reader = BufReader::new(f);

            reader.seek(SeekFrom::Start(offset as u64))?;
            let mut buf = [0; PAGE_SIZE];
            reader.read_exact(&mut buf)?;

            let mut page: Page = buf.try_into().map_err(|e| StorageError::Storage {
                action: StorageAction::Page,
                cause: StorageErrorCause::Error(Box::new(e)),
            })?;
            page.offset = offset;
            self.cached.push_front(Arc::new(RefCell::new(page)));
            self.path = Some(path);
            self.page(offset)
        } else {
            Err(StorageError::Storage {
                action: StorageAction::Page,
                cause: StorageErrorCause::Unknown,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::header::page::{CELLS_PER_INTERNAL, CELLS_PER_LEAF};

    use super::*;
    use tempdir::TempDir;

    #[test]
    fn storage_create_page() {
        let mut storage = BTreeStorage::default();
        storage
            .create(PageKind::Leaf { rows: vec![] }, 0, 0)
            .unwrap();
        assert_eq!(storage.pages, 1);
    }

    #[test]
    fn storage_create_page_disk() {
        let dir = TempDir::new("CreatePage").unwrap();
        let mut storage = BTreeStorage::new(dir.into_path()).unwrap();

        let page = storage.page(0).unwrap();
        let page = page.borrow();
        assert_eq!(page.offset, 0);
        assert_eq!(page.id, 0);
    }

    #[test]
    fn storage_persistence() {
        let dir = TempDir::new("CreatePage").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        storage.close().unwrap();

        let mut storage = BTreeStorage::new(path).unwrap();
        let page = storage.page(0).unwrap();
        let page = page.borrow();
        assert_eq!(page.offset, 0);
        assert_eq!(page.id, 0);
    }

    #[test]
    fn storage_insert_leaf() {
        let dir = TempDir::new("InsertLeaf").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();

        let row = Row::new();
        storage.insert(row).unwrap();
        storage.close().unwrap();

        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let page = storage.page(storage.root).unwrap().borrow_mut().clone();
        assert_eq!(1, page.cells);
    }

    #[test]
    fn storage_split_leaf() {
        let dir = TempDir::new("InsertLeaf").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let leafs = CELLS_PER_INTERNAL;
        let mut tree = format!("internal 1 {leafs}\n");
        let mut leaf = 0;
        tree += format!("  leaf {leaf} {}\n", CELLS_PER_LEAF).as_str();
        leaf += 2;
        while leaf <= leafs {
            tree += format!("  leaf {leaf} {}\n", CELLS_PER_LEAF).as_str();
            leaf += 1;
        }

        for i in 0..CELLS_PER_LEAF * leafs {
            let mut row = Row::new();
            row.set_id(i);
            storage.insert(row).unwrap();
        }

        storage.current = storage.root;
        assert_eq!(storage.walk(None).unwrap().trim(), tree.to_string().trim());
    }

    #[test]
    fn storage_split_internal() {
        let dir = TempDir::new("InsertInternal").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let mut tree = format!("internal {} 2\n", CELLS_PER_INTERNAL + 2);
        tree += format!("  internal {} {}\n", 1, CELLS_PER_INTERNAL).as_str();

        let mut leaf = 0;
        tree += format!("    leaf {leaf} {}\n", CELLS_PER_LEAF).as_str();
        leaf += 2;
        while leaf <= CELLS_PER_INTERNAL {
            tree += format!("    leaf {leaf} {}\n", CELLS_PER_LEAF).as_str();
            leaf += 1;
        }

        tree += format!("  internal {} {}\n", CELLS_PER_INTERNAL + 3, 1).as_str();
        tree += format!("    leaf {} {}\n", CELLS_PER_INTERNAL + 1, CELLS_PER_LEAF).as_str();

        for i in 0..CELLS_PER_LEAF * (CELLS_PER_INTERNAL + 1) {
            let mut row = Row::new();
            row.set_id(i);
            storage.insert(row).unwrap();
        }

        storage.current = storage.root;
        assert_eq!(storage.walk(None).unwrap().trim(), tree.to_string().trim());
    }

    #[test]
    fn storage_split_internal_multi() {
        let dir = TempDir::new("InsertInternalMulti").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let mut tree = "";

        for i in 0..CELLS_PER_LEAF * (CELLS_PER_INTERNAL * 2) + 1 {
            let mut row = Row::new();
            row.set_id(i);
            storage.insert(row).unwrap();
        }

        storage.current = storage.root;
        assert_eq!(storage.walk(None).unwrap().trim(), tree.to_string().trim());
    }
}
