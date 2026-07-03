#![allow(unused_variables)]
#![allow(dead_code)]
use std::{
    collections::HashMap,
    fmt,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
};

use crate::{
    Page, PageFlags,
    page::{MAGIC, MAGIC_SIZE, TABLE_HEADER_SIZE},
};
use log::{debug, info};

const O_DIRECT: i32 = 0o40000;
pub const DEFAULT_PAGE_SIZE: u16 = 4096;
pub const FORMAT_VERSION: u8 = 1;
pub const ROOT_PAGE_ID: usize = 1;

/// Loads a [`Page`] of `size` bytes from `reader`.
///
/// A [`Page`] is valid when the `MAGIC` bytes are present in its
/// trailer and the stored checksum matches the checksum computed
/// when the page is loaded.
fn load_page(
    page_id: usize,
    size: usize,
    reader: &mut (impl Read + Seek),
) -> io::Result<Page> {
    info!("loading page {page_id} (size: {size})");
    if page_id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "page id can not be zero",
        ));
    }

    let offset = (page_id - 1) * size;
    reader.seek(SeekFrom::Start(offset as u64))?;

    let mut buf = vec![0; size];
    reader.read_exact(&mut buf)?;

    let page = Page::build(buf);
    if page.cell(size - MAGIC_SIZE, size) != MAGIC.as_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "corrupted data; bytes are not a valid page",
        ));
    }
    if page.checksum() != page.compute_checksum() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "corrupted data; crc check failed",
        ));
    }

    Ok(page)
}

/// Writes a [`Page`] to `writer`.
///
/// Before writing, this updates the [`Page`]'s trailing magic bytes and recalculates
/// its checksum so the persisted [`Page`] can be validated during durability checks.
fn write_page(
    page_id: usize,
    size: usize,
    writer: &mut (impl Write + Seek),
    mut page: Page,
) -> io::Result<()> {
    info!("writing page {page_id} (size: {size})");
    if page_id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "page id can not be zero",
        ));
    }

    let offset = (page_id - 1) * size;

    page.mut_cell(size - MAGIC_SIZE, size)
        .copy_from_slice(MAGIC.as_bytes());
    page.set_checksum(page.compute_checksum());
    writer.seek(SeekFrom::Start(offset as u64))?;
    writer.write_all(&page[..])?;

    Ok(())
}

/// Create a new [`Page`].
///
/// The created page is constructed with the necessary maintenance
/// headers for the [`Pager`] struct to track and store information
/// in.
fn create_page(
    flags: PageFlags,
    size: u16,
    free_space_start: u64,
    root: bool,
) -> Page {
    info!("creating page of size {size} with {flags:?}");
    let mut page = Page::build(vec![0; size as usize]);

    if root {
        page.set_page_size(size);
        page.set_format_version(FORMAT_VERSION);
    }

    page.set_flags(flags.bits());
    page.set_free_space_start(free_space_start);
    page.set_free_space_end((size - MAGIC_SIZE as u16) as u64);
    page.set_free_space(size - free_space_start as u16 - MAGIC_SIZE as u16);
    page.mut_cell(size as usize - MAGIC_SIZE, size as usize)
        .copy_from_slice(MAGIC.as_bytes());
    page.set_checksum(page.compute_checksum());

    page
}

/// A [`CachedPage`] is a [`Page`] that has been loaded into memory.
pub struct CachedPage {
    page: Page,
    dirty: bool,
}

impl fmt::Display for CachedPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[CACHED")?;
        if self.dirty {
            write!(f, "|DIRTY")?;
        }
        write!(f, "]{}", self.page)
    }
}

impl fmt::Debug for CachedPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug)]
pub struct Pager<F>
where
    F: Read + Write + Seek,
{
    capacity: usize,
    inner: F,
    pages: HashMap<usize, CachedPage>,
    page_size: u16,
}

impl Pager<File> {
    pub fn open(path: impl Into<PathBuf>, capacity: usize) -> io::Result<Self> {
        let mut inner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(path.into())?;
        let len = inner.metadata()?.len();
        let pages = HashMap::with_capacity(capacity);

        let page_size: u16;
        let root: Page;
        let created: bool;

        if len < DEFAULT_PAGE_SIZE as u64 {
            root = create_page(
                PageFlags::IsRoot | PageFlags::IsLeaf,
                DEFAULT_PAGE_SIZE,
                TABLE_HEADER_SIZE as u64,
                true,
            );
            created = true;
            page_size = DEFAULT_PAGE_SIZE;
        } else {
            root =
                load_page(ROOT_PAGE_ID, DEFAULT_PAGE_SIZE as usize, &mut inner)
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("corrupted root information: {e}"),
                        )
                    })?;
            created = false;
            page_size = root.page_size();
        }

        let mut out = Self {
            capacity,
            inner,
            pages,
            page_size,
        };
        info!("initializing pager with root page:\n\t{root}");
        out.track(ROOT_PAGE_ID, root, created)?;

        Ok(out)
    }

    /// Caches a [`Page`] in memory to optimize direct I/O reads from on-disk storage.
    /// If the pager is at maximum capacity, this may evict an existing cached page.
    ///
    /// Page eviction is determined by the Clock replacement algorithm:
    /// <https://en.wikipedia.org/wiki/Page_replacement_algorithm#Clock>.
    fn track(&mut self, id: usize, page: Page, dirty: bool) -> io::Result<()> {
        // TODO: Actually implement the Clock cache ;-;
        self.pages
            .insert(id, CachedPage { page, dirty });
        Ok(())
    }
}

impl<F: Read + Write + Seek> Drop for Pager<F> {
    fn drop(&mut self) {
        debug!("cleaning up pager resources");
        for (id, page) in self.pages.drain() {
            if page.dirty {
                info!("flushing page {id}: {}", page.page);
                write_page(
                    id,
                    self.page_size as usize,
                    &mut self.inner,
                    page.page,
                )
                .expect("store should be writeable");
            }
        }
    }
}
