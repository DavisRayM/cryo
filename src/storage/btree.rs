use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    rc::Rc,
};

use log::{debug, info, trace};

use crate::storage::{
    Command,
    error::{PageAction, StorageAction, StorageErrorCause},
    header::page::{INTERNAL_SPLITAT, PAGE_SIZE},
};
use crate::{Statement, storage::header::page::LEAF_SPLITAT};

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
    cached: VecDeque<Rc<RefCell<Page>>>,
    path: Option<PathBuf>,
}

impl StorageBackend for BTreeStorage {
    type Error = StorageError;

    fn query(&mut self, cmd: Command) -> Result<Option<String>, Self::Error> {
        Ok(match cmd {
            Command::Exit => {
                trace!("received exit command; flushing database cache");
                self.close()?;
                Some("connection closed\n".into())
            }
            Command::Structure => Some(self.structure()?),
            Command::Populate(records) => {
                for i in 1..=records {
                    let cmd = Command::Statement(format!("insert {} test test@populate.com", i));
                    self.query(cmd)?;
                }
                Some(format!("populated database with {records} records"))
            }
            cmd => {
                let stmt: Statement = cmd.try_into().map_err(|e| StorageError::Storage {
                    action: StorageAction::Query,
                    cause: StorageErrorCause::Error(Box::new(e)),
                })?;

                match stmt {
                    Statement::Insert {
                        id,
                        username,
                        email,
                    } => {
                        let mut row = Row::new();
                        row.set_id(id);
                        row.set_email(email.as_ref());
                        row.set_username(&username);

                        self.insert(row)?;
                        None
                    }
                    Statement::Select => Some(
                        self.select()?
                            .iter()
                            .map(|r| {
                                format!(
                                    "{} {} {}",
                                    r.id().unwrap(),
                                    r.username().unwrap(),
                                    r.email().unwrap()
                                )
                            })
                            .collect::<Vec<String>>()
                            .join("\n"),
                    ),
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
        let mut visited = Vec::new();
        self.walk_tree(width.unwrap_or(0), &mut visited)
    }

    fn walk_tree(
        &mut self,
        width: usize,
        visited: &mut Vec<usize>,
    ) -> Result<String, StorageError> {
        let mut out = String::default();
        let page = self.page(self.current)?;

        let id = page.borrow().id;
        if visited.contains(&id) {
            return Ok(out);
        }
        visited.push(page.borrow().id);

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
            for pointer in node {
                self.current = pointer.left()?;
                out += self.walk_tree(width + 2, visited)?.as_ref();
                self.current = pointer.right()?;
                out += self.walk_tree(width + 2, visited)?.as_ref();
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
    pub(crate) fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        self.current = self.root;
        self.insert_row(row)
    }

    /// Selects all leaf cells
    pub(crate) fn select(&mut self) -> Result<Vec<Row>, StorageError> {
        self.current = self.root;
        let mut visited = Vec::new();
        self.select_traverse(&mut visited)
    }

    /// Prints out the current structure of the BTree
    pub(crate) fn structure(&mut self) -> Result<String, StorageError> {
        self.current = self.root;
        self.walk(None)
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

    /// Recursively traverses and selects all leaf cells in the entire tree
    fn select_traverse(&mut self, visited: &mut Vec<usize>) -> Result<Vec<Row>, StorageError> {
        let page = self.page(self.current)?;
        let mut out = Vec::new();

        if visited.contains(&page.borrow().id) {
            return Ok(out);
        }
        visited.push(page.borrow().id);

        if page.borrow().leaf() {
            return page.borrow_mut().select();
        }

        let pointers = page.borrow_mut().select()?;
        for pointer in pointers {
            self.current = pointer.left()?;
            out.extend_from_slice(self.select_traverse(visited)?.as_slice());
            self.current = pointer.right()?;
            out.extend_from_slice(self.select_traverse(visited)?.as_slice());
        }

        Ok(out)
    }

    /// Inserts a new row into storage
    fn insert_row(&mut self, row: Row) -> Result<(), StorageError> {
        loop {
            let page = self.page(self.current)?;
            debug!("attempt to insert record at {}", self.current);
            if !page.borrow().leaf() {
                debug!("page {} is internal, searching for leaf", page.borrow().id);
                self.search_internal(&row)?;
                continue;
            }

            drop(page);
            let mut page = match self.uncache(self.current)? {
                Some(page) => page,
                None => self.read_from_disk(self.current)?,
            };
            debug!("page {} is a leaf; inserting value", page.id);
            trace!("record: {} {}", row.id()?, row.offset()?);
            break match page.insert(row.clone()) {
                Ok(_) => {
                    self.write_to_disk(page)?;
                    debug!("row successfully inserted");
                    Ok(())
                }
                Err(StorageError::Page {
                    cause: PageErrorCause::Full,
                    ..
                }) => {
                    debug!("current leaf node is full; splitting nodes");
                    self.split(page, row)
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

    /// Splits node to accommodate the new row.
    fn split(&mut self, mut target: Page, row: Row) -> Result<(), StorageError> {
        debug!(
            "splitting node at {}; leaf: {}",
            self.current,
            target.leaf()
        );

        if target.offset == self.root {
            let parent_offset =
                self.create(PageKind::Internal { offsets: vec![] }, 0, self.root)?;
            let mut parent = self.read_from_disk(parent_offset)?;
            parent.parent = parent.offset;

            trace!(
                "linking page {} at {} to new parent {} from {}",
                target.id, target.offset, parent.offset, target.parent
            );
            // Update target to track new node as parent
            target.parent = parent.offset;
            self.root = parent.offset;

            let left = target.offset;
            let mut separator = Row::new();
            separator.set_left(left);

            let (right, key) = self.split_child(target, row)?;
            separator.set_id(key);
            separator.set_right(right);

            trace!("insert new separator key {key}, left: {left}, right: {right}");
            parent.insert(separator)?;
            self.write_to_disk(parent)?;
            Ok(())
        } else {
            let parent_offset = target.parent;
            let left = target.offset;

            let (right, key) = self.split_child(target, row)?;

            self.current = parent_offset;
            let mut parent = self.read_from_disk(parent_offset)?;

            let mut pointer = Row::new();
            pointer.set_id(key);
            pointer.set_left(left);
            pointer.set_right(right);

            let pointers = parent.select()?;

            trace!(
                "Updating index pointers: {:?}",
                pointers
                    .iter()
                    .map(|p| p.id().unwrap())
                    .collect::<Vec<usize>>()
            );

            match pointers.binary_search(&pointer) {
                Ok(_) => {
                    // TODO: No idea what might cause this at the moment...
                    todo!("handle updating existing keys")
                }
                Err(pos) => {
                    trace!("inserting index {key} pointer at: {pos}");
                    let mut kind = parent.kind.take();
                    if let Some(PageKind::Internal { offsets }) = &mut kind {
                        if pos > 0 {
                            let left_pointer = &mut offsets[pos - 1];
                            trace!(
                                "updating right pointer for {} to {} from {}",
                                left_pointer.id()?,
                                left,
                                left_pointer.left()?
                            );
                            left_pointer.set_right(left);
                        }

                        if pos < offsets.len() {
                            let right_pointer = &mut offsets[pos];
                            trace!(
                                "updating left pointer for {} to {} from {}",
                                right_pointer.id()?,
                                right,
                                right_pointer.left()?
                            );
                            right_pointer.set_left(right);
                        }
                    } else {
                        panic!("leaf cell as parent of another");
                    }
                    parent.kind = kind;
                }
            }

            match parent.insert(pointer.clone()) {
                Ok(()) => {
                    self.write_to_disk(parent)?;
                    Ok(())
                }
                Err(StorageError::Page {
                    cause: PageErrorCause::Full,
                    ..
                }) => {
                    self.current = parent.offset;
                    self.split(parent, pointer)
                }
                e => e,
            }
        }
    }

    fn split_child(&mut self, mut target: Page, row: Row) -> Result<(usize, usize), StorageError> {
        let parent_offset = target.parent;
        let parent = match self.uncache(parent_offset)? {
            Some(parent) => parent,
            None => self.read_from_disk(parent_offset)?,
        };

        let mut left_candidates = target.select()?;
        let splitat = if target.leaf() {
            LEAF_SPLITAT
        } else {
            INTERNAL_SPLITAT
        };
        let right_candidates = left_candidates.split_off(splitat);
        let left_cells = left_candidates.len();
        let right_cells = right_candidates.len();
        let key = right_candidates[0].id()?;
        trace!("selected split key: {key}");

        let mut right = if target.leaf() {
            let right_offset = self.create(
                PageKind::Leaf {
                    rows: right_candidates,
                },
                right_cells,
                parent_offset,
            )?;
            trace!("updating left child, assigning {left_cells} cells");
            target.kind = Some(PageKind::Leaf {
                rows: left_candidates,
            });
            target.cells = left_cells;
            self.read_from_disk(right_offset)?
        } else {
            let right_offset = self.create(
                PageKind::Internal {
                    offsets: right_candidates,
                },
                right_cells,
                parent_offset,
            )?;
            trace!("updating left child, assigning {left_cells} cells");
            target.kind = Some(PageKind::Internal {
                offsets: left_candidates,
            });
            target.cells = left_cells;
            self.read_from_disk(right_offset)?
        };

        debug!(
            "creating new index key: {key}\nleft: {}\nright: {}",
            target.offset, right.offset
        );

        let insert = row.id()?;
        if insert >= key {
            right.insert(row)?;
        } else {
            target.insert(row)?;
        }

        let right_offset = right.offset;
        self.write_to_disk(parent)?;
        self.write_to_disk(target)?;
        self.write_to_disk(right)?;

        Ok((right_offset, key))
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
        trace!("candidates: {:?}", pointers);

        let pos = match pointers.binary_search(row) {
            Ok(pos) => pointers[pos].right()?,
            Err(pos) => {
                let pointer = if pos == pointers.len() {
                    &pointers[pos - 1]
                } else {
                    &pointers[pos]
                };
                trace!("candidate {} row {}", pointer.id()?, row.id()?);
                if pointer.id()? >= row.id()? {
                    pointer.left()?
                } else {
                    pointer.right()?
                }
            }
        };
        debug!("possible candidate at {pos}");

        let offset = pos;
        debug!("traversing to child at {offset}");
        self.current = offset;
        Ok(())
    }

    /// Retrieves page from cache if present of loads it from disk.
    ///
    /// # Errors
    /// - If `offset` is OutOfBounds
    /// - If IO error occurs
    /// - If failed to load page from disk
    fn page(&mut self, offset: usize) -> Result<Rc<RefCell<Page>>, StorageError> {
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
        match Rc::try_unwrap(page) {
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
    fn cache(&mut self, page: Page) -> Result<Rc<RefCell<Page>>, StorageError> {
        let page = Rc::new(RefCell::new(page));
        let clone = Rc::clone(&page);
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
        match Rc::try_unwrap(page) {
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
    fn cached_page(&mut self, offset: usize) -> Result<Option<Rc<RefCell<Page>>>, StorageError> {
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
            .collect::<Vec<&Rc<RefCell<Page>>>>();
        if page.is_empty() {
            debug!("page {offset} is not cached");
            Ok(None)
        } else {
            debug!("retrieved from cache page {offset}(hits: {})", page.len());
            Ok(Some(Rc::clone(page[0])))
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

        for i in 0..=CELLS_PER_LEAF * 2 {
            let mut row = Row::new();
            row.set_id(i);
            storage.insert(row).unwrap();
        }

        assert_eq!(storage.pages, 4);
        let rows = storage.select().unwrap();
        assert_eq!(rows.len(), (CELLS_PER_LEAF * 2) + 1);
    }

    #[test]
    fn storage_split_internal() {
        let dir = TempDir::new("InsertLeaf").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();

        for i in 0..=CELLS_PER_LEAF * CELLS_PER_INTERNAL {
            let mut row = Row::new();
            row.set_id(i);
            storage.insert(row).unwrap();
        }

        let rows = storage.select().unwrap();
        eprintln!("{}", storage.structure().unwrap());
        assert_eq!(rows.len(), (CELLS_PER_LEAF * CELLS_PER_INTERNAL) + 1);
    }

    #[test]
    fn storage_query_exit() {
        let dir = TempDir::new("InsertInternalMulti").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let cmd = Command::Exit;

        assert_eq!(
            storage.query(cmd).unwrap(),
            Some("connection closed\n".into())
        );
    }

    #[test]
    fn storage_query_structure() {
        let dir = TempDir::new("InsertInternalMulti").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let cmd = Command::Structure;

        assert_eq!(storage.query(cmd).unwrap(), Some("leaf 0 0\n".into()));
    }

    #[test]
    fn storage_query_statement() {
        let dir = TempDir::new("InsertInternalMulti").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let cmd = Command::Statement("insert 1 dave dave".into());

        assert_eq!(storage.query(cmd).unwrap(), None);
    }
}
