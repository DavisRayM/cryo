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
//! let handle = pager.read(page_id).unwrap();
//! let loaded_page = handle.as_ref().lock().unwrap();
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
    collections::VecDeque,
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

const CACHED_PAGES: usize = 20;

#[derive(Debug, Default)]
pub struct PagerMetadata {
    pub free_pages: Vec<usize>,
    pub pages: usize,
    pub root: usize,
}

#[derive(Debug)]
pub struct Pager {
    cache: VecDeque<(usize, Arc<Mutex<Page>>)>,
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
            .open(path)
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;

        let metadata = f.metadata().map_err(|e| StorageError::Pager {
            cause: PagerError::Io(e),
        })?;
        let reader = BufReader::new(f.try_clone().map_err(|e| StorageError::Pager {
            cause: PagerError::Io(e),
        })?);
        let writer = BufWriter::new(f);

        let mut pager = Self {
            cache: VecDeque::with_capacity(20),
            metadata: PagerMetadata::default(),
            reader,
            writer,
        };

        if metadata.len() > 0 {
            pager.read_metadata()?;
        } else {
            pager.write_metadata()?;
        }

        Ok(pager)
    }

    /// Returns the root page
    pub fn root(&self) -> usize {
        self.metadata.root
    }

    pub fn set_root(&mut self, root: usize) -> Result<(), StorageError> {
        self.metadata.root = root;
        self.write_metadata()
    }

    /// Allocates space for a new page on disk. Returns the page id
    /// for the allocated page.
    ///
    pub fn allocate(&mut self) -> Result<usize, StorageError> {
        if let Some(id) = self.metadata.free_pages.pop() {
            self.cached_page(id, Some([0_u8; PAGE_SIZE].try_into()?))?;
            return Ok(id);
        }

        self.cached_page(self.metadata.pages, Some([0_u8; PAGE_SIZE].try_into()?))?;
        let id = self.metadata.pages;
        self.metadata.pages += 1;
        self.write_metadata()?;

        Ok(id)
    }

    /// Frees the space utilized by a page; Returning the used
    /// space back to the allocation pool.
    ///
    pub fn free(&mut self, id: usize) -> Result<(), StorageError> {
        if id >= self.metadata.pages {
            panic!("out of bounds");
        }

        let offset = id * PAGE_SIZE;
        let buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        self.metadata.free_pages.push(id);
        self.write_bytes(offset, &buf)
    }

    /// Read a page present in the configured file.
    ///
    /// # Errors
    ///
    /// This function panics if `id` is greater than or equal to `self.pages`.
    pub fn read(&mut self, id: usize) -> Result<Arc<Mutex<Page>>, StorageError> {
        if id >= self.metadata.pages {
            panic!("out of bounds");
        }

        if let Ok(Some(page)) = self.cached_page(id, None) {
            Ok(page)
        } else {
            panic!("page is unreadable")
        }
    }

    /// Writes a page to the on-memory cache. Modifies the `page` offset setting
    /// the correct offset for the page on-disk.
    ///
    /// # Errors
    ///
    /// This function errors if the `page` value is greater than or equal to `self.pages`
    pub fn write(&mut self, id: usize, page: &mut Page) -> Result<(), StorageError> {
        if id >= self.metadata.pages {
            panic!("out of bounds");
        }

        page.offset = id * PAGE_SIZE;
        self.cached_page(id, Some(page.clone()))?;

        Ok(())
    }

    /// Attempts to flush the pagers cache buffer
    ///
    /// # Panics
    ///
    /// If it fails to flush a page to the on disk structure this function panics.
    pub fn flush(&mut self) {
        while let Some((id, handle)) = self.cache.pop_back() {
            eprintln!("flushing page {id} to disk");
            let inner = Arc::try_unwrap(handle).unwrap().into_inner().unwrap();
            self.write_page(inner).unwrap();
        }
        self.write_metadata().unwrap();
    }

    fn write_page(&mut self, page: Page) -> Result<(), StorageError> {
        let buf: [u8; PAGE_SIZE] = page.as_bytes();
        self.write_bytes(page.offset, &buf)
    }

    fn read_page(&mut self, id: usize) -> Result<Page, StorageError> {
        let offset = id * PAGE_SIZE;
        let mut buf = [0; PAGE_SIZE];
        self.read_bytes(offset, &mut buf)?;
        let mut page: Page = buf.try_into()?;
        page.offset = offset;
        Ok(page)
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

    fn read_metadata(&mut self) -> Result<(), StorageError> {
        let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        self.read_bytes(METADATA_PAGE_ID * PAGE_SIZE, &mut buf)?;
        self.metadata = buf.into();
        trace!("pager metadata: {:?}", self.metadata);
        Ok(())
    }

    fn write_metadata(&mut self) -> Result<(), StorageError> {
        if self.metadata.pages == 0 {
            self.allocate()?;
            self.metadata.root = self.allocate()?;
            let mut page = Page::new(PageType::Leaf, None, vec![], 0);
            self.write(self.metadata.root, &mut page)?;
        }

        let buf: [u8; PAGE_SIZE] = (&self.metadata).into();
        self.write_bytes(METADATA_PAGE_ID * PAGE_SIZE, &buf)?;
        Ok(())
    }

    fn cached_page(
        &mut self,
        id: usize,
        page: Option<Page>,
    ) -> Result<Option<Arc<Mutex<Page>>>, StorageError> {
        if let Some(pos) = self.cache.iter().position(|(page_id, _)| *page_id == id) {
            if let Some(page) = page {
                self.cache[pos] = (id, Arc::new(Mutex::new(page)))
            }
            Ok(Some(Arc::clone(&(self.cache[pos].1))))
        } else {
            if self.cache.len() >= CACHED_PAGES {
                self.flush();
            }

            if let Some(page) = page {
                self.cache.push_front((id, Arc::new(Mutex::new(page))));
            } else {
                let page = self.read_page(id)?;
                self.cache.push_front((id, Arc::new(Mutex::new(page))));
            }

            self.cached_page(id, None)
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
        assert_eq!(page.offset, page_id * PAGE_SIZE);
    }

    #[test]
    fn pager_read() {
        let temp = TempDir::new("allocate").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();

        let page_id = pager.allocate().unwrap();
        let mut page = Page::new(PageType::Internal, None, vec![], 0);

        pager.write(page_id, &mut page).unwrap();

        let returned = pager.read(page_id).unwrap();
        let returned = returned.as_ref().lock().unwrap();

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
}
