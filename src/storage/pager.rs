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
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use super::{
    PagerError, StorageError,
    page::{PAGE_SIZE, Page},
};

#[derive(Debug)]
pub struct Pager {
    free_pages: Vec<usize>,
    pub pages: usize,
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
        let pages = metadata.len() as usize / PAGE_SIZE;

        Ok(Self {
            free_pages: vec![],
            pages,
            reader,
            writer,
        })
    }

    /// Allocates space for a new page on disk. Returns the page id
    /// for the allocated page.
    ///
    pub fn allocate(&mut self) -> Result<usize, StorageError> {
        if let Some(id) = self.free_pages.pop() {
            return Ok(id);
        }

        let id = self.pages;
        let offset = id * PAGE_SIZE;

        let buf = [0; PAGE_SIZE];

        self.writer
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;
        self.writer
            .write_all(&buf)
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;
        self.writer.flush().map_err(|e| StorageError::Pager {
            cause: PagerError::Io(e),
        })?;
        self.pages += 1;

        Ok(id)
    }

    /// Frees the space utilized by a page; Returning the used
    /// space back to the allocation pool.
    ///
    pub fn free(&mut self, id: usize) -> Result<(), StorageError> {
        if id >= self.pages {
            panic!("out of bounds");
        }

        let offset = id * PAGE_SIZE;
        let buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        self.free_pages.push(id);
        self.write_bytes(offset, &buf)
    }

    /// Read a page present in the configured file.
    ///
    /// # Errors
    ///
    /// This function panics if `id` is greater than or equal to `self.pages`.
    pub fn read(&mut self, id: usize) -> Result<Page, StorageError> {
        if id >= self.pages {
            panic!("out of bounds");
        }

        let offset = id * PAGE_SIZE;
        let mut buf = [0; PAGE_SIZE];
        self.reader
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;
        self.reader
            .read_exact(&mut buf)
            .map_err(|e| StorageError::Pager {
                cause: PagerError::Io(e),
            })?;

        let mut page: Page = buf.try_into()?;
        page.offset = offset;
        Ok(page)
    }

    /// Writes a page to the on-disk storage file. Modifies the `page` offset setting
    /// the correct offset for the page on-disk.
    ///
    /// # Errors
    ///
    /// This function errors if the `page` value is greater than or equal to `self.pages`
    pub fn write(&mut self, id: usize, page: &mut Page) -> Result<(), StorageError> {
        if id >= self.pages {
            panic!("out of bounds");
        }

        let offset = id * PAGE_SIZE;
        page.offset = offset;
        let buf: [u8; PAGE_SIZE] = page.as_bytes();
        self.write_bytes(offset, &buf)
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
        assert_eq!(page_id, 0);
        assert_eq!(pager.pages, 1);
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
