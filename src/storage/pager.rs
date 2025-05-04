//! Disk-backed page management layer.
//!
//! The `pager` module defines the [`Pager`] struct, which is responsible for loading,
//! caching, and writing [`Page`] structures to and from persistent storage.
//!
//! It abstracts the low-level mechanics of file I/O and page indexing, allowing the
//! storage engine to interact with logical page IDs rather than raw byte offsets.
//!
//! # Responsibilities
//!
//! - Allocating new pages and assigning unique page IDs & offsets.
//! - Reading and writing fixed-size pages from/to disk
//! - Maintaining an in-memory cache of active pages
//!
//! # Page Identifiers
//!
//! Each page is referenced by a `PageId` (`usize`) that maps to its offset within the
//! underlying file. The pager ensures that reads and writes always deal with full,
//! aligned pages.
//!
//! # Example
//! ```rust
//! use cryo::storage::pager::Pager;
//! use cryo::storage::page::{Page, PageType};
//!
//! let mut pager = Pager::open("cryo.db".into()).unwrap();
//!
//! let mut page = Page::new(PageType::Leaf, None, vec![], 0);
//! let page_id = pager.allocate().unwrap();
//!
//! pager.write(page_id, &mut page).unwrap();
//!
//! let loaded_page = pager.read(page_id).unwrap();
//!
//! assert_eq!(loaded_page._type, PageType::Leaf);
//! assert_eq!(loaded_page.offset, page.offset);
//! ```
//!
//! # Design Notes
//!
//! - Pages are written in fixed-size blocks, with metadata stored at the beginning of the file.
//! - In the future, this module may include LRU or clock-style caching for performance.
//!
//! # See Also
//! - [`Page`]: The fixed-size unit of storage.
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use log::trace;

use super::{
    PagerError, StorageError,
    page::{PAGE_SIZE, Page, PageType},
};

const ROOT_PAGE_SIZE: usize = size_of::<usize>();
const NUM_PAGES_SIZE: usize = size_of::<usize>();
const FREE_PAGES_LEN_SIZE: usize = size_of::<usize>();
const METADATA_HEADER_SIZE: usize = ROOT_PAGE_SIZE + NUM_PAGES_SIZE + FREE_PAGES_LEN_SIZE;
const FREE_PAGE_SIZE: usize = size_of::<usize>();

const ROOT_PAGE: usize = 0;
const FREE_PAGES_LEN: usize = ROOT_PAGE + ROOT_PAGE_SIZE;
const NUM_PAGES: usize = FREE_PAGES_LEN + FREE_PAGES_LEN_SIZE;

const METADATA_PAGE_ID: usize = 0;

#[derive(Debug, Default)]
pub struct PagerMetadata {
    pub free_pages: Vec<usize>,
    pub pages: usize,
    pub root: usize,
}

impl PagerMetadata {
    pub fn from_u8(value: &[u8]) -> Self {
        let root = usize::from_ne_bytes(
            value[ROOT_PAGE..ROOT_PAGE + ROOT_PAGE_SIZE]
                .try_into()
                .expect("should be expected size"),
        );
        let free_pages_len = usize::from_ne_bytes(
            value[FREE_PAGES_LEN..FREE_PAGES_LEN + FREE_PAGES_LEN_SIZE]
                .try_into()
                .expect("should be expected size"),
        );
        let pages = usize::from_ne_bytes(
            value[NUM_PAGES..NUM_PAGES + NUM_PAGES_SIZE]
                .try_into()
                .expect("should be expected size"),
        );

        let mut free_pages = Vec::with_capacity(free_pages_len);
        let mut offset = METADATA_HEADER_SIZE;

        for _ in 0..free_pages_len {
            let page_id = usize::from_ne_bytes(
                value[offset..offset + FREE_PAGE_SIZE]
                    .try_into()
                    .expect("should be expected size"),
            );
            offset += FREE_PAGE_SIZE;
            free_pages.push(page_id);
        }

        Self {
            root,
            pages,
            free_pages,
        }
    }
}

#[derive(Debug)]
pub struct Pager {
    cache: Option<BTreeMap<usize, Arc<Mutex<Vec<u8>>>>>,
    /// Whether to automatically commit changes
    /// to the page. When set to false all state changes
    /// are temporary.
    commit: bool,
    metadata: PagerMetadata,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl Pager {
    /// Loads a new pager instance for an on-disk file.
    pub fn open(path: PathBuf) -> Result<Self, StorageError> {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;

        let metadata = f.metadata().map_err(|e| StorageError::Pager {
            cause: PagerError::Io(e),
        })?;

        let mut pager = Self {
            cache: Some(BTreeMap::new()),
            commit: true,
            metadata: PagerMetadata::default(),
            reader: BufReader::new(f.try_clone().map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?),
            writer: BufWriter::new(f),
        };

        if metadata.len() > 0 {
            pager.read_metadata()?;
        } else {
            pager.write_metadata()?;
        }

        Ok(pager)
    }

    /// Set commit state of the pager
    pub fn commit(&mut self, commit: bool) {
        self.commit = commit;
    }

    /// Returns the root page
    pub fn root(&self) -> usize {
        self.metadata.root
    }

    /// Set the root node of the BTree
    pub fn set_root(&mut self, root: usize) -> Result<(), StorageError> {
        self.metadata.root = root;
        self.write_metadata()
    }

    /// Allocates space for a new page on disk. Returns the page id
    /// for the allocated page.
    ///
    pub fn allocate(&mut self) -> Result<usize, StorageError> {
        let id = if let Some(id) = self.metadata.free_pages.pop() {
            id
        } else {
            let id = self.metadata.pages;
            self.metadata.pages += 1;
            id
        };

        self.write_metadata()?;
        self.stored_bytes(id, Some(vec![0; PAGE_SIZE]))?;

        Ok(id)
    }

    /// Discards the page; Stops tracking the page and
    /// adds it to the list of pages that should be reclaimed.
    ///
    /// NOTE: Once a page is freed there is no gurantee that the
    /// state will not be modified.
    pub fn free(&mut self, id: usize) -> Result<(), StorageError> {
        self.is_valid(id)?;
        if let Some(cache) = &mut self.cache {
            cache.remove(&id);
        }

        self.metadata.free_pages.push(id);
        self.write_metadata()
    }

    /// Reads a page tracked by the pager.
    ///
    /// # Error
    /// This function errors out when accessing invalid IDs or if
    /// the stored bytes are corrupted.
    pub fn read(&mut self, id: usize) -> Result<Page, StorageError> {
        self.is_valid(id)?;

        let bytes = self.stored_bytes(id, None)?;
        let handle = bytes.as_ref().lock().map_err(|_| StorageError::Pager {
            cause: PagerError::PoisonedState,
        })?;
        let page: Page = handle.as_slice().try_into()?;
        Ok(page)
    }

    /// Updates the tracked state of the page.
    ///
    /// # Error
    /// This function errors if the `id` is out of bounds.
    pub fn write(&mut self, id: usize, page: &mut Page) -> Result<(), StorageError> {
        self.is_valid(id)?;

        let bytes = page.as_bytes();
        self.stored_bytes(id, Some(bytes.to_vec()))?;
        Ok(())
    }

    /// Writes the entire cache to the on-disk file.
    ///
    /// # Panics
    /// This function panics if it fails to write the entire cache on disk.
    pub fn flush(&mut self) {
        if !self.commit {
            return;
        }

        if let Some(cache) = self.cache.take() {
            for (id, bytes) in cache {
                let offset = id * PAGE_SIZE;
                let inner = Arc::try_unwrap(bytes).expect("page still in use");
                let bytes = inner
                    .into_inner()
                    .expect("failed to retrieve bytes from mutex");
                self.write_bytes(
                    offset,
                    bytes[..]
                        .try_into()
                        .expect("failed to convert to page sized bytes"),
                )
                .unwrap();
            }
        }
        self.cache = Some(BTreeMap::new());
    }

    /// Updates the in-memory representation of the Pager metadata
    /// structure.
    ///
    /// # Panics
    /// This function panics if the byte representation of the
    /// metadata structure is corrupted.
    fn read_metadata(&mut self) -> Result<(), StorageError> {
        let bytes = self.stored_bytes(METADATA_PAGE_ID, None)?;
        let handle = bytes.as_ref().lock().map_err(|_| StorageError::Pager {
            cause: PagerError::PoisonedState,
        })?;
        self.metadata = PagerMetadata::from_u8(handle.as_slice());

        trace!("pager metadata: {:?}", self.metadata);

        Ok(())
    }

    /// Updates the in-memory representation of the Pager
    /// metadata structure.
    fn write_metadata(&mut self) -> Result<(), StorageError> {
        if self.metadata.pages == 0 {
            self.allocate()?;
            self.metadata.root = self.allocate()?;
            let mut page = Page::new(PageType::Leaf, None, vec![], 0);
            self.write(self.metadata.root, &mut page)?;
        }

        let buf: [u8; PAGE_SIZE] = (&self.metadata).into();
        self.stored_bytes(METADATA_PAGE_ID, Some(buf.to_vec()))?;
        Ok(())
    }

    fn read_bytes(&mut self, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
        self.reader
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;

        self.reader
            .read_exact(buf)
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;

        Ok(())
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8; PAGE_SIZE]) -> Result<(), StorageError> {
        self.writer
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;
        self.writer
            .write_all(bytes)
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;
        self.writer.flush().map_err(|e| StorageError::Pager {
            cause: PagerError::Io(e),
        })?;

        Ok(())
    }

    /// Ensures the accessed ID is a valid allocated
    /// page tracked by the Pager.
    ///
    /// # Errors
    /// This function returns a [PagerError::OutOfBounds] if the id
    /// is not currently being tracked.
    fn is_valid(&mut self, id: usize) -> Result<(), StorageError> {
        if id >= self.metadata.pages && !self.metadata.free_pages.contains(&id) {
            Err(StorageError::Pager {
                cause: PagerError::OutOfBounds,
            })
        } else {
            Ok(())
        }
    }

    /// Retrieves the cached bytes for a particular page. If
    /// the page is not currently stored in memory it'll be paged
    /// in.
    ///
    /// NOTE: Memory is permanently stored in memory until flush
    ///       is called.
    fn stored_bytes(
        &mut self,
        id: usize,
        update: Option<Vec<u8>>,
    ) -> Result<Arc<Mutex<Vec<u8>>>, StorageError> {
        if let Some(update) = update {
            if let Some(cache) = &mut self.cache {
                cache
                    .entry(id)
                    .and_modify(|b| *b = Arc::new(Mutex::new(update.clone())))
                    .or_insert(Arc::new(Mutex::new(update)));
            }
            self.stored_bytes(id, None)
        } else {
            let entry = if let Some(cache) = &mut self.cache {
                cache.get(&id).map(Arc::clone)
            } else {
                return Err(StorageError::Pager {
                    cause: PagerError::PoisonedState,
                });
            };

            if let Some(entry) = entry {
                Ok(entry)
            } else {
                let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
                self.read_bytes(id * PAGE_SIZE, &mut buf)?;
                self.stored_bytes(id, Some(buf.to_vec()))
            }
        }
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        self.flush();
    }
}

impl From<&PagerMetadata> for [u8; PAGE_SIZE] {
    fn from(value: &PagerMetadata) -> Self {
        let mut out = [0; PAGE_SIZE];

        out[ROOT_PAGE..ROOT_PAGE + ROOT_PAGE_SIZE]
            .clone_from_slice(value.root.to_ne_bytes().as_ref());
        out[FREE_PAGES_LEN..FREE_PAGES_LEN + FREE_PAGES_LEN_SIZE]
            .clone_from_slice(value.free_pages.len().to_ne_bytes().as_ref());
        out[NUM_PAGES..NUM_PAGES + NUM_PAGES_SIZE]
            .clone_from_slice(value.pages.to_ne_bytes().as_ref());

        let mut offset = METADATA_HEADER_SIZE;
        for free_page in value.free_pages.iter() {
            out[offset..offset + FREE_PAGE_SIZE].clone_from_slice(free_page.to_ne_bytes().as_ref());
            offset += FREE_PAGE_SIZE;
        }

        out
    }
}

impl From<[u8; PAGE_SIZE]> for PagerMetadata {
    fn from(value: [u8; PAGE_SIZE]) -> Self {
        Self::from_u8(&value)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::storage::page::PageType;

    use super::*;

    #[test]
    fn pager_allocate() {
        let temp = TempDir::new("allocate").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();

        let page_id = pager.allocate().unwrap();
        assert_eq!(page_id, 2);
        assert_eq!(pager.metadata.pages, 3);
    }

    #[test]
    fn pager_flush() {
        let temp = TempDir::new("allocate").unwrap();
        let mut pager = Pager::open(temp.path().join("cryo.db")).unwrap();

        pager.allocate().unwrap();
        let pages = pager.metadata.pages;
        let root = pager.metadata.root;
        pager.flush();

        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        assert_eq!(pager.metadata.pages, pages);
        assert_eq!(pager.metadata.root, root);
    }

    #[test]
    fn pager_write() {
        let temp = TempDir::new("allocate").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();

        let _ = pager.allocate().unwrap();
        let page_id = pager.allocate().unwrap();
        let mut page = Page::new(PageType::Internal, None, vec![], 0);

        pager.write(page_id, &mut page).unwrap();
        assert_eq!(pager.read(page_id).unwrap(), page);
    }

    #[test]
    fn pager_read() {
        let temp = TempDir::new("allocate").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();

        let page_id = pager.allocate().unwrap();
        let mut page = Page::new(PageType::Internal, None, vec![], 0);

        pager.write(page_id, &mut page).unwrap();

        let returned = pager.read(page_id).unwrap();

        assert_eq!(returned.offset, page.offset);
        assert_eq!(returned._type, page._type);
        assert_eq!(returned.parent, page.parent);
    }

    #[test]
    fn pager_free() {
        let temp = TempDir::new("allocate").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();

        let _ = pager.allocate().unwrap();
        let free = pager.allocate().unwrap();
        let _ = pager.allocate().unwrap();

        pager.free(free).unwrap();
        assert_eq!(free, pager.allocate().unwrap());
    }

    #[test]
    fn pager_commit_false() {
        let temp = TempDir::new("persistance").unwrap();
        let mut pager = Pager::open(temp.path().join("cryo.db")).unwrap();

        let pages = pager.metadata.pages;
        let root = pager.metadata.root;

        pager.commit(false);
        pager.allocate().unwrap();
        let new_root = pager.allocate().unwrap();
        pager.allocate().unwrap();
        pager.set_root(new_root).unwrap();

        assert!(pager.metadata.pages > pages);
        assert_ne!(pager.metadata.root, root);
        drop(pager);

        let pager = Pager::open(temp.path().join("cryo.db")).unwrap();
        assert_eq!(pages, pager.metadata.pages);
        assert_eq!(root, pager.metadata.root);
    }
}
