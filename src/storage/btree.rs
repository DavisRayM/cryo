use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

use log::{debug, info, trace};

use crate::Statement;
use crate::storage::{
    Command,
    error::{PageAction, StorageAction, StorageErrorCause},
    header::page::PAGE_SIZE,
};

use super::{
    StorageBackend,
    error::{PageErrorCause, StorageError},
    page::{Page, PageKind},
    row::Row,
};

const PAGE_IN_MEMORY: usize = 5;

#[derive(Debug)]
pub struct BTreeStorage {
    pub pages: usize,
    pub root: usize,
    pub current: usize,
    cached: VecDeque<Arc<RefCell<Page>>>,
    path: Option<PathBuf>,
}

impl StorageBackend for BTreeStorage {
    type Error = StorageError;

    fn query(&mut self, cmd: Command) -> Result<Option<String>, Self::Error> {
        Ok(match cmd {
            Command::Exit => {
                trace!("received exit command; flushing database cache");
                self.close()?;
                Some("connection closed.".into())
            }
            cmd => {
                trace!("storage received command: {cmd:?}");
                let stmt: Statement = cmd.try_into().map_err(|e| StorageError::Storage {
                    action: StorageAction::Query,
                    cause: StorageErrorCause::Error(Box::new(e)),
                })?;
                debug!("received statement: {stmt:?}");

                match stmt {
                    Statement::Insert {
                        id,
                        username,
                        email,
                    } => {
                        debug!(
                            "creating row: {} {} {}",
                            id,
                            username.iter().collect::<String>(),
                            email.iter().collect::<String>()
                        );

                        let mut row = Row::new();
                        row.set_id(id);
                        row.set_email(email.as_ref());
                        row.set_username(&username);

                        self.insert(row)?;
                        None
                    }
                    Statement::Select => {
                        debug!("executing select statement");
                        todo!()
                    }
                }
            }
        })
    }
}

impl BTreeStorage {
    /// Create a new BTreeStorage backend and configures persistence to the directory
    pub fn new(dir: PathBuf) -> Result<Self, StorageError> {
        let path = dir.join("btree.db");
        trace!("opening btree storage at: {:?}", path);

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
            trace!("no pages detected; creating starting leaf node.");
            storage.create(PageKind::Leaf { rows: vec![] }, 0, 0)?;
        } else {
            trace!("locating root node");
            let page = storage.page(0)?;
            let mut pos = page.borrow().offset;
            let mut parent = page.borrow().parent;

            while pos != parent {
                trace!("traversing {} to parent {}", pos, parent);
                pos = parent;
                parent = storage.page(parent)?.borrow().parent;
            }
            storage.root = parent;
            trace!("root located at: {pos}");
        }
        Ok(storage)
    }

    /// Walks the BTree and prints all the nodes
    pub(crate) fn walk(&mut self, width: Option<usize>) -> Result<String, StorageError> {
        let mut out = String::default();
        let page = self.page(self.current)?;
        let width = width.unwrap_or(0);

        if page.borrow().leaf() {
            out += format!(
                "{:width$}leaf {} {}\n",
                "",
                page.borrow().id,
                page.borrow().cells,
                width = width
            )
            .as_ref();
        } else {
            out += format!(
                "{:width$}internal {} {}\n",
                "",
                page.borrow().id,
                page.borrow().cells,
                width = width
            )
            .as_ref();

            let node = page.borrow_mut().select()?;
            for child in node {
                self.current = child.offset()?;
                out += self.walk(Some(width + 2))?.as_ref();
            }
        }

        Ok(out)
    }

    /// Flushes pager cache to disk
    pub fn close(&mut self) -> Result<(), StorageError> {
        debug!("closing database; emptying cache");
        while !self.cached.is_empty() {
            self.free()?;
        }
        Ok(())
    }

    /// Inserts a new Row into the BTree storage
    fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        self.current = self.root;
        self.insert_row(row)
    }

    /// Creates a new page and returns the offset to the page
    fn create(
        &mut self,
        kind: PageKind,
        cells: usize,
        parent: usize,
    ) -> Result<usize, StorageError> {
        let offset = self.pages * PAGE_SIZE;
        debug!(
            "creating page\noffset: {}\ntype: {:?}\ncells: {}\nparent: {}",
            offset, kind, cells, parent
        );

        let page = Page::new(offset, self.pages, kind, cells, parent);
        self.write_to_disk(page)?;
        self.pages += 1;
        Ok(offset)
    }

    fn insert_row(&mut self, row: Row) -> Result<(), StorageError> {
        loop {
            let page = self.page(self.current)?;
            debug!("attempt to insert record at {}", self.current);
            if !page.borrow().leaf() {
                debug!("page {} is internal, searching for leaf", page.borrow().id);
                self.search_internal(&row)?;
                continue;
            }

            debug!("page {} is a leaf; inserting value", page.borrow().id);
            break match page.borrow_mut().insert(row) {
                Ok(_) => {
                    debug!("row successfully inserted");
                    Ok(())
                }
                Err(StorageError::Page {
                    cause: PageErrorCause::Full,
                    ..
                }) => {
                    debug!("current leaf node is full; splitting nodes");
                    todo!("implement splitting again")
                }
                Err(e) => {
                    debug!("unexpected error during insert: {e:?}");
                    Err(StorageError::Storage {
                        action: StorageAction::Insert,
                        cause: StorageErrorCause::Error(Box::new(e)),
                    })
                }
            };
        }
    }

    /// Searches an internal node for the position of `row`
    ///
    /// # Panics
    ///
    /// This function panics if called by a leaf node.
    fn search_internal(&mut self, row: &Row) -> Result<(), StorageError> {
        let page = self.page(self.current)?;
        if page.borrow().leaf() {
            panic!("tried to search a leaf node.");
        }

        debug!("searching internal node {} for {}", self.current, row.id()?);
        let pointers = page.borrow_mut().select()?;
        debug!("candidates: {:?}", pointers);

        let pos = match pointers.binary_search(&row) {
            Ok(pos) => {
                debug!("found candidate at location: {pos}, picking next pointer");
                pos + 1
            }
            Err(pos) => {
                debug!("possible candidate at {pos}");
                pos
            }
        };

        if pos >= pointers.len() {
            debug!(
                "position({}) out of bounds, pointers {} current {}",
                pos,
                pointers.len(),
                self.current
            );
            trace!("current page: {:?}", page);
            Err(StorageError::Storage {
                action: StorageAction::Search,
                cause: StorageErrorCause::OutOfBounds,
            })
        } else {
            let offset = pointers[pos].offset()?;
            debug!("traversing to child at {offset}");
            self.current = offset;
            Ok(())
        }
    }

    /// Retrieves page from cache if present of loads it from disk.
    ///
    /// # Errors
    /// - If `offset` is OutOfBounds
    /// - If IO error occurs
    /// - If failed to load page from disk
    fn page(&mut self, offset: usize) -> Result<Arc<RefCell<Page>>, StorageError> {
        trace!("paging in {offset} page");
        if offset >= self.pages * PAGE_SIZE {
            debug!(
                "offset {offset} is out of bounds; current pages {1} maximum {0}",
                self.pages * PAGE_SIZE,
                self.pages
            );
            return Err(StorageError::Storage {
                action: StorageAction::Page,
                cause: StorageErrorCause::OutOfBounds,
            });
        }

        if let Ok(Some(page)) = self.cached_page(offset) {
            return Ok(page);
        }

        let page = self.read_from_disk(offset)?;
        self.cache(page)
    }

    // Clear a page from cache and write it to disk
    //
    // # Panics
    // If no path has been configured for the storage
    fn free(&mut self) -> Result<(), StorageError> {
        let page = self.cached.pop_front().ok_or(StorageError::Storage {
            action: StorageAction::PageOut,
            cause: StorageErrorCause::CacheMiss,
        })?;
        match Arc::try_unwrap(page) {
            Ok(rc) => {
                let inner = rc.into_inner();
                self.write_to_disk(inner)?;
                Ok(())
            }
            Err(_) => {
                info!("failed to free page; currently in use.");
                Err(StorageError::Page {
                    action: PageAction::Write,
                    cause: PageErrorCause::InUse,
                })
            }
        }
    }

    /// Adds a new page into the cache.
    fn cache(&mut self, page: Page) -> Result<Arc<RefCell<Page>>, StorageError> {
        trace!("caching page {}", page.id);
        let page = Arc::new(RefCell::new(page));
        let clone = Arc::clone(&page);
        self.cached.push_front(page);
        Ok(clone)
    }

    /// Removes a page from the cache.
    fn uncache(&mut self, offset: usize) -> Result<Option<Page>, StorageError> {
        debug!("attempt to remove page {} from cache", offset);
        let pos = match self
            .cached
            .iter()
            .position(|page| page.borrow().offset == offset)
        {
            Some(pos) => pos,
            None => return Ok(None),
        };
        debug!("page located at {pos} in cache; attempting removal");

        let page = self.cached.remove(pos).expect("page should be at position");
        match Arc::try_unwrap(page) {
            Ok(rc) => {
                let page = rc.into_inner();
                debug!("page {} successfully uncached.", page.id);
                Ok(Some(page))
            }
            Err(_) => Err(StorageError::Storage {
                action: StorageAction::PageOut,
                cause: StorageErrorCause::PageInUse,
            }),
        }
    }

    /// Reads page at specified offset.
    fn read_from_disk(&mut self, offset: usize) -> Result<Page, StorageError> {
        trace!("reading from disk, offset {offset}");
        if let Some(path) = self.path.take() {
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
            debug!("read page {} at {offset}", page.id);
            self.path = Some(path);
            Ok(page)
        } else {
            debug!("storage does not have access to disk path.");
            Err(StorageError::Storage {
                action: StorageAction::Page,
                cause: StorageErrorCause::Unknown,
            })
        }
    }

    /// Writes a page out to the disk location.
    fn write_to_disk(&mut self, page: Page) -> Result<(), StorageError> {
        trace!("writing to disk, offset {}", page.offset);
        if let Some(path) = self.path.take() {
            let offset = page.offset;
            let bytes: [u8; PAGE_SIZE] = page.into();

            let f = OpenOptions::new().write(true).open(&path)?;
            let mut writer = BufWriter::new(f);

            writer.seek(SeekFrom::Start(offset as u64))?;
            writer.write_all(&bytes)?;
            self.path = Some(path);
            Ok(())
        } else {
            debug!("storage does not have access to disk path.");
            Err(StorageError::Storage {
                action: StorageAction::PageOut,
                cause: StorageErrorCause::Unknown,
            })
        }
    }

    /// Retrieves page from cache if any
    fn cached_page(&mut self, offset: usize) -> Result<Option<Arc<RefCell<Page>>>, StorageError> {
        trace!("checking cache for page: {offset}");
        while self.cached.len() >= PAGE_IN_MEMORY {
            trace!(
                "cache over capacity({}) {}, clearing page",
                PAGE_IN_MEMORY,
                self.cached.len()
            );
            self.free()?;
        }

        let page = self
            .cached
            .iter()
            .filter(|p| p.borrow().offset == offset)
            .collect::<Vec<&Arc<RefCell<Page>>>>();
        if page.is_empty() {
            debug!("page {offset} is not cached");
            Ok(None)
        } else {
            debug!("page {offset} cached (hits: {})", page.len());
            Ok(Some(Arc::clone(page[0])))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::header::page::{CELLS_PER_INTERNAL, CELLS_PER_LEAF};

    use super::*;
    use tempdir::TempDir;

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
            storage.current = storage.root;
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
            storage.current = storage.root;
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
            storage.current = storage.root;
            storage.insert(row).unwrap();
        }

        storage.current = storage.root;
        assert_eq!(storage.walk(None).unwrap().trim(), tree.to_string().trim());
    }
}
