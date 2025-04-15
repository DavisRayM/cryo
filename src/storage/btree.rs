use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    rc::Rc,
};

use log::{debug, info, trace};

use crate::storage::{
    Command,
    error::{PageAction, StorageAction, StorageErrorCause},
    header::page::{
        CELLS_PER_LEAF, INTERNAL_SPLITAT, PAGE_SIZE, RECLAIM_COUNT_SIZE, RECLAIM_OFFSET_SIZE,
    },
};
use crate::{Statement, storage::header::page::LEAF_SPLITAT};

use super::{
    StorageBackend,
    error::{PageErrorCause, StorageError},
    header::page::{
        CELLS_PER_INTERNAL, RECLAIM_COUNT, STORAGE_HEADER, STORAGE_ROOT, STORAGE_ROOT_SIZE,
    },
    page::{Page, PageKind},
    row::Row,
};

const PAGE_IN_MEMORY: usize = 10;
// Stop printing leaves in structure after pages surpass cutoff
const LEAF_PRINT_CUTOFF: usize = 100;

#[derive(Debug)]
pub struct BTreeStorage {
    cached: VecDeque<Rc<RefCell<Page>>>,
    pub current: usize,
    pub pages: usize,
    pub root: usize,
    reader: BufReader<File>,
    writer: BufWriter<File>,
    breadcrumbs: Vec<(usize, usize)>,
    reclaim: Vec<usize>,
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
                    Statement::Delete { id } => {
                        let mut row = Row::new();
                        row.set_id(id);

                        self.delete(row)?;
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
                    Statement::Update {
                        id,
                        username,
                        email,
                    } => {
                        let mut row = Row::new();
                        row.set_id(id);
                        row.set_email(email.as_ref());
                        row.set_username(&username);
                        let r = self.update(row)?;
                        Some(format!(
                            "{} {} {}",
                            r.id().unwrap(),
                            r.username().unwrap(),
                            r.email().unwrap()
                        ))
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

        let writer = BufWriter::new(f.try_clone()?);
        let reader = BufReader::new(f.try_clone()?);

        let mut storage = Self {
            breadcrumbs: Vec::new(),
            cached: VecDeque::with_capacity(PAGE_IN_MEMORY),
            current: root,
            pages,
            reader,
            reclaim: Vec::new(),
            root,
            writer,
        };

        if pages == 0 {
            trace!("no pages detected; creating starting leaf node.");
            storage.write_header()?;
            storage.root = storage.create(PageKind::Leaf { rows: vec![] }, 0, 0)?;
            storage.write_header()?;
        } else {
            storage.read_header()?;
        }
        Ok(storage)
    }

    pub(crate) fn read_header(&mut self) -> Result<(), StorageError> {
        self.reader.seek(SeekFrom::Start(0))?;

        let mut buf = [0; STORAGE_ROOT_SIZE];
        self.reader.read_exact(&mut buf)?;
        self.root = usize::from_ne_bytes(buf);

        let mut buf = [0; RECLAIM_COUNT_SIZE];
        self.reader.read_exact(&mut buf)?;
        let reclaim_keys = usize::from_ne_bytes(buf);

        eprintln!("keys: {}", reclaim_keys);
        self.reclaim = Vec::new();

        for _ in 0..reclaim_keys {
            let mut buf = [0; RECLAIM_OFFSET_SIZE];
            self.reader.read_exact(&mut buf)?;
            let offset = usize::from_be_bytes(buf);
            self.reclaim.push(offset);
        }
        Ok(())
    }

    pub(crate) fn write_header(&mut self) -> Result<(), StorageError> {
        let mut buf = [0; PAGE_SIZE];

        buf[STORAGE_ROOT..RECLAIM_COUNT].clone_from_slice(self.root.to_ne_bytes().as_ref());
        buf[RECLAIM_COUNT..STORAGE_HEADER]
            .clone_from_slice(self.reclaim.len().to_ne_bytes().as_ref());
        let mut offset = STORAGE_HEADER;

        for key in self.reclaim.iter() {
            buf[offset..offset + RECLAIM_OFFSET_SIZE].clone_from_slice(key.to_ne_bytes().as_ref());
            offset += STORAGE_HEADER;
        }

        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_all(&buf)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Walks the BTree and prints all the nodes
    pub(crate) fn walk(&mut self, width: Option<usize>) -> Result<String, StorageError> {
        let mut visited = Vec::new();
        Ok(format!(
            "digraph {{\n{}}}",
            self.walk_tree(width.unwrap_or(0), &mut visited)?
        ))
    }

    fn walk_tree(
        &mut self,
        width: usize,
        visited: &mut Vec<usize>,
    ) -> Result<String, StorageError> {
        let mut out = String::default();
        let page = self.page(self.current)?;
        let id = page.borrow().offset;

        if visited.contains(&id) {
            return Ok(out);
        }
        visited.push(id);

        if !page.borrow().leaf() {
            let node = page.borrow_mut().select()?;
            for pointer in node {
                out += format!("  {id} -> {};\n", pointer.id()?).as_str();

                self.current = pointer.left()?;
                out += format!("  {} -> {}[color=green];\n", pointer.id()?, self.current).as_str();
                out += self.walk_tree(width + 2, visited)?.as_ref();

                self.current = pointer.right()?;
                out += format!("  {} -> {}[color=blue];\n", pointer.id()?, self.current).as_str();
                out += self.walk_tree(width + 2, visited)?.as_ref();
            }
        } else if self.pages <= LEAF_PRINT_CUTOFF {
            let parent = self.page(page.borrow().parent)?.borrow().id;
            out += format!("  {} -> {}[color=red]", id, parent).as_str();
        }

        Ok(out)
    }

    /// Flushes pager cache to disk
    pub fn close(&mut self) -> Result<(), StorageError> {
        debug!("closing database; emptying cache");
        self.write_header()?;

        while !self.cached.is_empty() {
            self.free()?;
        }
        self.writer.flush()?;
        Ok(())
    }

    /// Inserts a new Row into the BTree storage
    pub(crate) fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        self.current = self.root;
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

    pub(crate) fn delete(&mut self, row: Row) -> Result<(), StorageError> {
        self.current = self.root;
        loop {
            let page = self.page(self.current)?;
            debug!("attempt to delete record at {}", self.current);
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
            debug!("page {} is a leaf; deleting value", page.id);
            trace!("record: {} {}", row.id()?, row.offset()?);
            page.delete(row)?;

            if self.root == page.offset {
                trace!("page is root; deleting entry");
                break self.write_to_disk(page);
            }

            // Merge nodes
            break if let Some((parent, key)) = self.breadcrumbs.pop() {
                trace!("checking {parent} pointer {key} for opportunity to merge");
                let pointers = self.page(parent)?.borrow_mut().select()?;
                let sibling = if pointers[key].right()? == page.offset {
                    pointers[key].left()?
                } else {
                    pointers[key].right()?
                };
                trace!("sibling pointer: {sibling}");

                let sibling_cells = self.page(sibling)?.borrow().cells;
                debug!(
                    "attempt merge: {} < {CELLS_PER_LEAF}?",
                    page.cells + sibling_cells
                );

                if page.cells + sibling_cells < CELLS_PER_LEAF {
                    let sibling = match self.uncache(sibling)? {
                        Some(page) => page,
                        None => self.read_from_disk(sibling)?,
                    };
                    self.merge(page, sibling, key)
                } else {
                    self.write_to_disk(page)
                }
            } else {
                Err(StorageError::Storage {
                    action: StorageAction::Delete,
                    cause: StorageErrorCause::Unknown,
                })
            };
        }
    }

    fn merge(&mut self, target: Page, other: Page, pointer_pos: usize) -> Result<(), StorageError> {
        debug!("merging {} & {}", target.offset, other.offset);
        let mut parent = match self.uncache(target.parent)? {
            Some(page) => page,
            None => self.read_from_disk(target.parent)?,
        };
        let pointers = parent.select()?;

        let (mut successor, mut ancestor) = if pointers[pointer_pos].left()? == target.offset {
            (target, other)
        } else {
            (other, target)
        };
        trace!(
            "merge successor {} <- {}",
            successor.offset, ancestor.offset
        );

        let mut rows = ancestor.select()?;
        while let Some(row) = rows.pop() {
            successor.insert(row.clone())?;
            ancestor.delete(row)?;
        }

        parent.delete(pointers[pointer_pos].clone())?;
        if pointer_pos > 0 {
            let mut left_pointer = Row::new();
            left_pointer.set_id(pointers[pointer_pos - 1].id()?);
            left_pointer.set_left(pointers[pointer_pos - 1].left()?);
            left_pointer.set_right(successor.offset);
            debug!(
                "updating left pointer {} to {}",
                left_pointer.id()?,
                left_pointer.right()?
            );
            parent.update(left_pointer)?;
        }

        if pointer_pos + 1 < pointers.len() {
            let mut right_pointer = Row::new();
            right_pointer.set_id(pointers[pointer_pos + 1].id()?);
            right_pointer.set_right(pointers[pointer_pos + 1].right()?);
            right_pointer.set_left(successor.offset);
            debug!(
                "updating right pointer {} to {}",
                right_pointer.id()?,
                right_pointer.right()?
            );
            parent.update(right_pointer)?;
        }

        let pointers = parent.select()?;
        if pointers.is_empty() {
            if parent.offset == self.root {
                debug!("parent is root and empty; child succeeding as parent");
                self.root = successor.offset;
                successor.parent = successor.offset;
                self.breadcrumbs.clear();
            } else if let Some((parent_offset, key)) = self.breadcrumbs.pop() {
                let mut row = Row::new();
                let max_key = successor
                    .select()?
                    .iter()
                    .max()
                    .ok_or(StorageError::Storage {
                        action: StorageAction::Delete,
                        cause: StorageErrorCause::Unknown,
                    })?
                    .id()?
                    + 1;
                row.set_id(max_key);
                row.set_left(successor.offset);
                row.set_right(ancestor.offset);

                parent.insert(row)?;
                debug!(
                    "triger merge for parent {} on {parent_offset} key {key}",
                    parent.offset
                );

                self.write_to_disk(successor)?;
                self.write_to_disk(ancestor)?;

                let target = parent;
                let pointers = self.page(parent_offset)?.borrow_mut().select()?;
                let other = if pointers[key].left()? == target.offset {
                    pointers[key].right()?
                } else {
                    pointers[key].left()?
                };
                let other = self.uncache(other)?.unwrap_or(self.read_from_disk(other)?);
                return self.merge(target, other, key);
            }
        }

        self.write_to_disk(parent)?;
        self.write_to_disk(successor)?;
        self.write_to_disk(ancestor)?;

        if let Some((parent_offset, key)) = self.breadcrumbs.pop() {
            debug!("checking if parent requires merge");
            let mut parent = self
                .uncache(parent_offset)?
                .unwrap_or(self.read_from_disk(parent_offset)?);
            let pointers = parent.select()?;

            if parent.offset == self.root {
                debug!("parent is root; merge complete");
                return Ok(());
            }

            let other_offset = if pointers[key].left()? == parent.offset {
                pointers[key].right()?
            } else {
                pointers[key].left()?
            };

            let other = self.page(other_offset)?;
            if parent.cells + other.borrow().cells < CELLS_PER_INTERNAL {
                debug!("parent requires merge; requesting merge of parent");
                drop(other);
                let other = self
                    .uncache(other_offset)?
                    .unwrap_or(self.read_from_disk(other_offset)?);
                return self.merge(parent, other, key);
            }
        }

        Ok(())
    }

    pub(crate) fn update(&mut self, row: Row) -> Result<Row, StorageError> {
        self.current = self.root;
        loop {
            let page = self.page(self.current)?;
            debug!("attempt to update record at {}", self.current);
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
            debug!("page {} is a leaf; updating value", page.id);
            trace!("record: {} {}", row.id()?, row.offset()?);
            let out = page.update(row);
            self.write_to_disk(page)?;
            break out;
        }
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
        let offset = (self.pages * PAGE_SIZE) + PAGE_SIZE;
        debug!(
            "creating page\noffset: {}\ncells: {}\nparent: {}",
            offset, cells, parent
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

            let separator = self.split_child(target, row)?;

            trace!(
                "insert new separator key {}, left: {}, right: {}",
                separator.id()?,
                separator.left()?,
                separator.right()?
            );
            parent.insert(separator)?;
            self.write_to_disk(parent)?;
            Ok(())
        } else {
            let parent_offset = target.parent;
            let pointer = self.split_child(target, row)?;
            self.current = parent_offset;
            let mut parent = self.read_from_disk(parent_offset)?;

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

    fn split_child(&mut self, mut target: Page, row: Row) -> Result<Row, StorageError> {
        let parent_offset = target.parent;
        let parent = match self.uncache(parent_offset)? {
            Some(parent) => parent,
            None => self.read_from_disk(parent_offset)?,
        };
        let mut pointer = Row::new();

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

        pointer.set_id(key);
        pointer.set_left(target.offset);

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
            // Update links to this child
            let child_pages = right_candidates
                .iter()
                .map(|r| r.offset().unwrap())
                .collect::<Vec<usize>>();

            let right_offset = self.create(
                PageKind::Internal {
                    offsets: right_candidates,
                },
                right_cells,
                parent_offset,
            )?;

            for child in child_pages {
                let mut page = self.uncache(child)?.unwrap_or(self.read_from_disk(child)?);
                page.parent = right_offset;
                self.write_to_disk(page)?;
            }

            trace!("updating left child, assigning {left_cells} cells");
            target.kind = Some(PageKind::Internal {
                offsets: left_candidates,
            });
            target.cells = left_cells;
            self.read_from_disk(right_offset)?
        };
        pointer.set_right(right.offset);

        let insert = row.id()?;
        if insert >= key {
            if !target.leaf() {
                // Update the links target page to point to the correct parent
                let mut page = self
                    .uncache(row.left()?)?
                    .unwrap_or(self.read_from_disk(row.left()?)?);
                page.parent = right.offset;
                self.write_to_disk(page)?;

                let mut page = self
                    .uncache(row.right()?)?
                    .unwrap_or(self.read_from_disk(row.right()?)?);
                page.parent = right.offset;
                self.write_to_disk(page)?;
            }
            right.insert(row)?;
        } else {
            target.insert(row)?;
        }

        self.write_to_disk(parent)?;
        self.write_to_disk(target)?;
        self.write_to_disk(right)?;

        Ok(pointer)
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
            Ok(pos) => {
                self.breadcrumbs.push((self.current, pos));
                pointers[pos].right()?
            }
            Err(pos) => {
                let pointer = if pos == pointers.len() {
                    self.breadcrumbs.push((self.current, pos - 1));
                    &pointers[pos - 1]
                } else {
                    self.breadcrumbs.push((self.current, pos));
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
        if offset >= (self.pages * PAGE_SIZE) + PAGE_SIZE {
            debug!(
                "offset {offset} is out of bounds; current pages {1} maximum {0}",
                (self.pages * PAGE_SIZE) + PAGE_SIZE,
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
        self.writer.flush()?;

        self.reader.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = [0; PAGE_SIZE];
        self.reader.read_exact(&mut buf)?;

        let mut page: Page = buf.try_into().map_err(|e| StorageError::Storage {
            action: StorageAction::Page,
            cause: StorageErrorCause::Error(Box::new(e)),
        })?;
        page.offset = offset;
        debug!("read page {} at {offset}", page.id);
        Ok(page)
    }

    /// Writes a page out to the disk location.
    fn write_to_disk(&mut self, page: Page) -> Result<(), StorageError> {
        trace!("writing to disk, offset {}", page.offset);
        let offset = page.offset;
        let bytes: [u8; PAGE_SIZE] = page.into();

        self.writer.seek(SeekFrom::Start(offset as u64))?;
        self.writer.write_all(&bytes)?;

        Ok(())
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
        assert_eq!(page.id, 4096);
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
        assert_eq!(page.id, 4096);
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
        let cells = (CELLS_PER_LEAF * 2) + 1;

        storage.query(Command::Populate(cells)).unwrap();

        assert_eq!(storage.pages, 4);
        let rows = storage.select().unwrap();
        assert_eq!(rows.len(), cells);
    }

    #[test]
    fn storage_split_internal() {
        let dir = TempDir::new("InsertLeaf").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let cells = (CELLS_PER_INTERNAL * 2) + 3;

        storage.query(Command::Populate(cells)).unwrap();

        let rows = storage.select().unwrap();
        eprintln!("{}", storage.structure().unwrap());
        assert_eq!(rows.len(), cells);
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

        assert_eq!(
            storage.query(cmd).unwrap(),
            Some("digraph {\n  4096 -> 4096[color=red]}".into())
        );
    }

    #[test]
    fn storage_header() {
        let dir = TempDir::new("InsertInternalMulti").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();

        storage.root = 99999;
        storage.close().unwrap();

        let storage = BTreeStorage::new(path.clone()).unwrap();
        assert_eq!(storage.root, 99999);
    }

    #[test]
    fn storage_query_statement() {
        let dir = TempDir::new("InsertInternalMulti").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path.clone()).unwrap();
        let cmd = Command::Statement("insert 1 dave dave".into());

        assert_eq!(storage.query(cmd).unwrap(), None);
    }

    #[test]
    fn storage_update() {
        let dir = TempDir::new("StorageUpdate").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path).unwrap();
        let cmd = Command::Statement("insert 1 dave dave".into());
        storage.query(cmd).unwrap();

        let cmd = Command::Statement("update 1 sam sam".into());
        storage.query(cmd).unwrap();

        assert_eq!(
            "1 sam sam",
            storage
                .query(Command::Statement("select".into()))
                .unwrap()
                .unwrap()
                .replace("\0", "")
        )
    }

    #[test]
    fn storage_delete_multi() {
        let dir = TempDir::new("StorageDeleteMulti").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path).unwrap();
        let cmd = Command::Populate(100);
        storage.query(cmd).unwrap();

        for i in 1..=100 {
            let cmd = Command::Statement(format!("delete {}", i));
            storage.query(cmd).unwrap();

            assert_eq!(100 - i, storage.select().unwrap().len())
        }
    }

    #[test]
    fn storage_delete() {
        let dir = TempDir::new("StorageDelete").unwrap();
        let path = dir.into_path();
        let mut storage = BTreeStorage::new(path).unwrap();
        let cmd = Command::Statement("insert 1 dave dave".into());
        storage.query(cmd).unwrap();

        let cmd = Command::Statement("delete 1".into());
        storage.query(cmd).unwrap();

        assert_eq!(
            Some("".into()),
            storage.query(Command::Statement("select".into())).unwrap()
        )
    }
}
