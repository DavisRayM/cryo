//! Pager and page-cache support for on-disk pages.
//!
use crate::{
    Page, PageFlags,
    page::{HEADER_SIZE, MAGIC},
};
use log::{debug, info, trace, warn};
use std::{
    collections::HashMap,
    fmt,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{
        self,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::ThreadId,
};

const O_DIRECT: i32 = 0o40000;

/// Default size, in bytes, used when creating a new database file.
pub const DEFAULT_PAGE_SIZE: u16 = 4096;

/// On-disk format version written into the root page of newly created files.
pub const FORMAT_VERSION: u8 = 1;

/// Page identifier reserved for the root page.
///
/// Page identifiers are one-based; page id `0` is invalid.
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
    if page.magic() != MAGIC.as_bytes() {
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
    page: &mut Page,
) -> io::Result<()> {
    info!("writing page {page_id} (size: {size})");
    if page_id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "page id can not be zero",
        ));
    }

    let offset = (page_id - 1) * size;

    page.set_magic(None);
    page.set_checksum(page.compute_checksum());
    writer.seek(SeekFrom::Start(offset as u64))?;
    writer.write_all(&page[..])?;

    Ok(())
}

/// Create a new [`Page`].
///
/// The created page is initialized with page flags, free-space metadata,
/// trailing magic bytes, and a checksum. When `root` is true, the root-only
/// metadata fields for page size and format version are also written.
fn create_page(
    flags: PageFlags,
    size: u16,
    free_space_start: u16,
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
    page.set_free_space_end(size);
    page.set_free_space(size - free_space_start);
    page.set_magic(None);
    page.set_checksum(page.compute_checksum());

    page
}

/// Describes how a thread is currently accessing a cached page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    /// The page is being read.
    Read,
    /// The page is being mutated and will be marked dirty.
    Write,
}

/// Describes the context by the which the thread is accessing a cached page
#[derive(Debug, Clone, Copy)]
pub struct AccessContext {
    pub txn_id: Option<u64>,
    pub lsn: Option<u64>,
    pub reason: Option<&'static str>,
}

impl AccessContext {
    /// No idea... Probably left-over code... FIXME
    pub const fn anonymous() -> Self {
        Self {
            txn_id: None,
            lsn: None,
            reason: None,
        }
    }

    /// Access [`Page`] as part of a user-initiated transaction.
    pub const fn txn(
        txn_id: u64,
        lsn: Option<u64>,
        reason: &'static str,
    ) -> Self {
        Self {
            txn_id: Some(txn_id),
            lsn,
            reason: Some(reason),
        }
    }

    /// Access [`Page`] as part of a maintenance process.
    pub const fn maintenance(reason: &'static str) -> Self {
        Self {
            txn_id: None,
            lsn: None,
            reason: Some(reason),
        }
    }
}

/// Records one active access to a cached page.
///
/// Handles are used for cache diagnostics only; pin counts are the source of truth
/// for eviction safety.
#[derive(Clone)]
pub struct PageHandle {
    pub lsn: Option<u64>,
    pub mode: AccessMode,
    pub page_id: usize,
    pub reason: Option<&'static str>,
    pub thread_id: ThreadId,
    pub txn_id: Option<u64>,
}

impl PageHandle {
    /// Adds this handle to a cached page's diagnostic handle list.
    pub fn add(&self, page: &CachedPage) -> io::Result<()> {
        page.handles
            .lock()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to acquire lock on cached page handles list",
                )
            })?
            .push(self.clone());
        Ok(())
    }

    /// Removes this handle from a cached page's diagnostic handle list.
    pub fn remove(&self, page: &CachedPage) -> io::Result<()> {
        let mut handles = page
            .handles
            .lock()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to acquire lock on cached page handles list",
                )
            })?;

        if let Some(pos) = handles
            .iter()
            .position(|h| h.thread_id == self.thread_id && h.mode == self.mode)
        {
            handles.swap_remove(pos);
        }
        Ok(())
    }
}

impl fmt::Display for PageHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}(page={}, txn={:?}, lsn={:?}, thread={:?}",
            self.mode, self.page_id, self.txn_id, self.lsn, self.thread_id
        )?;

        if let Some(reason) = self.reason {
            write!(f, ", reason={reason}")?;
        }
        write!(f, ")")
    }
}

impl fmt::Debug for PageHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// A [`CachedPage`] is a [`Page`] that has been loaded into memory.
///
/// It stores the page contents together with cache bookkeeping used by the pager:
/// dirty tracking for write-back, an accessed bit for Clock replacement, a pin
/// count to prevent eviction while in use, and diagnostic access handles.
pub struct CachedPage {
    page_id: usize,

    page: sync::RwLock<Page>,
    /// Whether the page has been accessed since the clock last checked.
    accessed: AtomicBool,
    /// Whether the page needs to be flushed to the backing store.
    dirty: AtomicBool,
    /// Number of active users.
    pin_count: AtomicUsize,

    handles: sync::Mutex<Vec<PageHandle>>,
}

impl CachedPage {
    /// Creates a new [`CachedPage`] that tracks a [`Page`] in memory.
    pub fn new(page_id: usize, page: Page, dirty: bool) -> Self {
        Self {
            page_id,
            page: sync::RwLock::new(page),
            dirty: AtomicBool::new(dirty),
            accessed: AtomicBool::new(true),
            pin_count: AtomicUsize::new(0),
            handles: sync::Mutex::new(Vec::new()),
        }
    }

    /// Pins the [`Page`] in memory, ensuring it cannot be evicted while in use.
    pub fn pin(&self) {
        self.pin_count
            .fetch_add(1, Ordering::AcqRel);
        self.accessed
            .store(true, Ordering::Release);
    }

    /// Unpins the [`Page`], indicating that it is no longer in use.
    pub fn unpin(&self) {
        let old = self
            .pin_count
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "unpin without pin");
    }

    /// Returns whether the [`CachedPage`] is currently pinned.
    pub fn is_pinned(&self) -> bool {
        self.pin_count
            .load(Ordering::Acquire)
            > 0
    }
}

impl fmt::Display for CachedPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[CACHED")?;
        if self
            .dirty
            .load(Ordering::Acquire)
        {
            write!(f, "|DIRTY")?;
        }
        write!(
            f,
            "]{}",
            self.page
                .read()
                .expect("failed to retrieve read lock")
        )
    }
}

impl fmt::Debug for CachedPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// A fixed-size page manager backed by a readable, writable, seekable store.
///
/// The pager lazily loads pages from the backing store, caches them in memory,
/// exposes closure-based read and write access, and flushes dirty pages during
/// eviction or drop.
#[derive(Debug)]
pub struct Pager<F>
where
    F: Read + Write + Seek,
{
    capacity: usize,
    inner: sync::Mutex<F>,
    page_size: u16,

    clock: sync::Mutex<ClockState>,
    pages: sync::RwLock<HashMap<usize, sync::Arc<CachedPage>>>,
}

/// State used by the Clock cache replacement algorithm.
#[derive(Debug)]
struct ClockState {
    hand: usize,
    ring: Vec<usize>,
}

/// Snapshot of cache metadata for one cached page.
#[derive(Clone)]
pub struct CacheInfo {
    pub page_id: usize,
    pub dirty: bool,
    pub accessed: bool,
    pub pin_count: usize,
    pub handles: Vec<PageHandle>,
}

impl fmt::Display for CacheInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "page {}: dirty={} accessed={} pins={} handles=[",
            self.page_id, self.dirty, self.accessed, self.pin_count,
        )?;
        for h in self.handles.iter() {
            write!(f, "\n\t{}", h)?;
        }
        write!(f, "]")
    }
}

impl fmt::Debug for CacheInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl<F> Pager<F>
where
    F: Read + Write + Seek,
{
    /// Access a [`Page`] with read access.
    ///
    /// The page is loaded into the cache if needed, pinned for the duration of
    /// the closure, and then unpinned before this function returns.
    pub fn page<R>(
        &self,
        page_id: usize,
        ctx: AccessContext,
        f: impl FnOnce(&Page) -> R,
    ) -> io::Result<R> {
        trace!(
            "page {page_id} access start: mode={:?} txn={:?} lsn={:?} reason={:?}",
            AccessMode::Write,
            ctx.txn_id,
            ctx.lsn,
            ctx.reason,
        );
        let page = self.get_or_load(page_id)?;
        page.pin();

        let handle = PageHandle {
            lsn: ctx.lsn,
            mode: AccessMode::Read,
            page_id,
            reason: ctx.reason,
            thread_id: std::thread::current().id(),
            txn_id: ctx.txn_id,
        };
        handle.add(&page)?;

        let out = {
            let page = page
                .page
                .read()
                .map_err(|_e| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "failed to acquire read lock on page",
                    )
                })?;
            f(&page)
        };
        handle.remove(&page)?;
        page.unpin();
        trace!(
            "page {page_id} access end: mode={:?} txn={:?}",
            AccessMode::Write,
            ctx.txn_id,
        );

        Ok(out)
    }

    /// Access [`Page`] with `page_id` with write access.
    ///
    /// The page is loaded into the cache if needed, pinned for the duration of
    /// the closure, and marked dirty after the closure runs.
    pub fn mut_page<R>(
        &self,
        page_id: usize,
        ctx: AccessContext,
        f: impl FnOnce(&mut Page) -> R,
    ) -> io::Result<R> {
        if ctx.txn_id.is_none() && ctx.reason.is_none() {
            warn!(
                "mutating page {page_id} without transaction or maintenance context. You forgot something dingus!"
            );
        }

        trace!(
            "page {page_id} access start: mode={:?} txn={:?} lsn={:?} reason={:?}",
            AccessMode::Write,
            ctx.txn_id,
            ctx.lsn,
            ctx.reason,
        );
        let cached = self.get_or_load(page_id)?;
        cached.pin();

        let handle = PageHandle {
            lsn: ctx.lsn,
            mode: AccessMode::Write,
            page_id,
            reason: ctx.reason,
            thread_id: std::thread::current().id(),
            txn_id: ctx.txn_id,
        };
        handle.add(&cached)?;

        let out = {
            let mut page = cached
                .page
                .write()
                .map_err(|_e| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "failed to acquire write lock on page",
                    )
                })?;
            let out = f(&mut page);
            cached
                .dirty
                .store(true, Ordering::Release);

            out
        };
        handle.remove(&cached)?;
        cached.unpin();
        trace!(
            "page {page_id} access end: mode={:?} txn={:?}",
            AccessMode::Write,
            ctx.txn_id,
        );

        Ok(out)
    }

    /// Returns a snapshot of metadata for all currently cached pages.
    pub fn info(&self) -> Vec<CacheInfo> {
        let pages = self
            .pages
            .read()
            .expect("failed to acquire read lock on pages map");

        pages
            .values()
            .map(|p| CacheInfo {
                page_id: p.page_id,
                dirty: p
                    .dirty
                    .load(Ordering::Acquire),
                accessed: p
                    .accessed
                    .load(Ordering::Acquire),
                pin_count: p
                    .pin_count
                    .load(Ordering::Acquire),
                handles: p
                    .handles
                    .lock()
                    .expect("can lock cached page handles")
                    .clone(),
            })
            .collect()
    }

    /// Retrieves a [`Page`] from the cache or loads it from the backing store.
    ///
    /// If loading a new page would exceed the configured cache capacity, this
    /// attempts to evict one unpinned page first.
    fn get_or_load(&self, page_id: usize) -> io::Result<sync::Arc<CachedPage>> {
        if let Some(cached_page) = self
            .pages
            .read()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to request read lock on pager state",
                )
            })?
            .get(&page_id)
            .cloned()
        {
            trace!("page {page_id} retrieved from cache");
            cached_page
                .accessed
                .store(true, Ordering::Release);

            return Ok(cached_page);
        }

        let mut pages = self
            .pages
            .write()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to request write lock on pager state",
                )
            })?;

        // Check if another thread loaded the page before lock was
        // acquired
        if let Some(cached_page) = pages.get(&page_id).cloned() {
            cached_page
                .accessed
                .store(true, Ordering::Release);
            return Ok(cached_page);
        }

        if pages.len() >= self.capacity {
            self.evict_one(&mut pages)?;
        }

        let page = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("failed to lock pager state: {e}"),
                    )
                })?;
            load_page(page_id, self.page_size as usize, &mut *inner)?
        };
        info!("loaded page {page_id}: {page}");

        let page = sync::Arc::new(CachedPage::new(page_id, page, false));
        pages.insert(page_id, page.clone());
        self.clock
            .lock()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("failed to lock pager state: {e}"),
                )
            })?
            .ring
            .push(page_id);
        Ok(page)
    }

    /// Evicts a single page from the page cache using a variant of
    /// the Clock Page Replacement algorithm:
    ///   <https://en.wikipedia.org/wiki/Page_replacement_algorithm#Clock>.
    fn evict_one(
        &self,
        pages: &mut HashMap<usize, sync::Arc<CachedPage>>,
    ) -> io::Result<()> {
        info!("attempting to evict one cached page:\n\t{pages:?}");
        let mut clock = self
            .clock
            .lock()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to acquire clock state lock",
                )
            })?;

        if clock.ring.is_empty() {
            return Err(io::Error::other("can not evict from empty cache"));
        }

        // Traverse through the circular buffer at least twice
        // before giving up on finding a slot.
        let max_attempts = clock.ring.len() * 2;
        let mut attempts = 0;

        while attempts < max_attempts {
            attempts += 1;

            if clock.hand >= clock.ring.len() {
                clock.hand = 0;
            }

            let hand = clock.hand;
            let page_id = clock.ring[hand];
            let Some(cached_page) = pages.get(&page_id) else {
                clock.ring.swap_remove(hand);
                continue;
            };

            if cached_page.is_pinned() {
                clock.hand += 1;
                continue;
            }

            if cached_page
                .accessed
                .swap(false, Ordering::AcqRel)
            {
                clock.hand += 1;
                continue;
            }

            // Eviction candidate found; not pinned and not accessed recently
            let mut page = cached_page
                .page
                .write()
                .map_err(|_e| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "failed to acquire write lock on cached page contents",
                    )
                })?;
            info!("eviction candidate found (page id: {page_id})",);

            // Re-check; lock might have been acquired after pin
            if cached_page.is_pinned() {
                clock.hand += 1;
                continue;
            }

            // Flush if dirty
            if cached_page
                .dirty
                .load(Ordering::Acquire)
            {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_e| {
                        io::Error::other(
                            "failed to acquire lock on pager state",
                        )
                    })?;
                write_page(
                    cached_page.page_id,
                    self.page_size as usize,
                    &mut *inner,
                    &mut page,
                )?;
                cached_page
                    .dirty
                    .store(false, Ordering::Release);
            }

            drop(page);
            pages.remove(&page_id);
            info!("page {page_id} evicted");
            clock.ring.swap_remove(hand);

            if clock.hand >= clock.ring.len() && !clock.ring.is_empty() {
                clock.hand = 0;
            }

            return Ok(());
        }

        debug!("[CACHE][FULL] all cached pages pinned");
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "all cached pages are pinned",
        ))
    }
}

impl Pager<File> {
    /// Opens an existing pager file or creates a new one.
    ///
    /// New files are initialized with a root leaf page using
    /// [`DEFAULT_PAGE_SIZE`]. Existing files read the root page at the default
    /// size first so the stored page size can be discovered.
    pub fn open(path: impl Into<PathBuf>, capacity: usize) -> io::Result<Self> {
        let mut inner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(path.into())?;
        let len = inner.metadata()?.len();

        let page_size: u16;
        let root: Page;
        let created: bool;

        if len < DEFAULT_PAGE_SIZE as u64 {
            root = create_page(
                PageFlags::IsRoot | PageFlags::IsLeaf,
                DEFAULT_PAGE_SIZE,
                HEADER_SIZE as u16,
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
            inner: sync::Mutex::new(inner),
            pages: sync::RwLock::new(HashMap::with_capacity(capacity)),
            page_size,
            clock: sync::Mutex::new(ClockState {
                hand: 0,
                ring: vec![],
            }),
        };
        info!("initializing pager with root page:\n\t{root}");
        out.track(ROOT_PAGE_ID, root, created)?;

        Ok(out)
    }

    /// Caches a [`Page`] in memory.
    ///
    /// This is used during pager initialization to register the root page.
    fn track(&mut self, id: usize, page: Page, dirty: bool) -> io::Result<()> {
        self.pages
            .write()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to retrieve write lock",
                )
            })?
            .insert(id, sync::Arc::new(CachedPage::new(id, page, dirty)));
        Ok(())
    }
}

impl<F> Drop for Pager<F>
where
    F: Read + Write + Seek,
{
    fn drop(&mut self) {
        debug!("cleaning up pager resources");

        let mut inner = self
            .inner
            .lock()
            .expect("mutex poisoned");
        for (id, page) in self
            .pages
            .write()
            .expect("failed to retrieve write lock during drop")
            .drain()
        {
            if page
                .dirty
                .load(Ordering::Acquire)
            {
                let mut page = page
                    .page
                    .write()
                    .expect("page is not held by another thread");
                info!("flushing page {id}: {}", page);
                write_page(id, self.page_size as usize, &mut *inner, &mut page)
                    .expect("store should be writeable");
            }
        }
    }
}

impl<F> fmt::Display for Pager<F>
where
    F: Read + Write + Seek,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "pager contents:")?;
        for i in self.info().iter() {
            write!(f, "\t{i}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn pager_with_pages(
        pages: impl IntoIterator<Item = (usize, Page)>,
    ) -> Pager<Cursor<Vec<u8>>> {
        let mut inner = Cursor::new(Vec::new());

        for (id, mut page) in pages {
            write_page(id, DEFAULT_PAGE_SIZE as usize, &mut inner, &mut page)
                .expect("test page can be written");
        }

        inner.set_position(0);

        Pager {
            capacity: 8,
            inner: sync::Mutex::new(inner),
            page_size: DEFAULT_PAGE_SIZE,
            clock: sync::Mutex::new(ClockState {
                hand: 0,
                ring: vec![],
            }),
            pages: sync::RwLock::new(HashMap::with_capacity(8)),
        }
    }

    fn test_page(num_keys: u16, marker: u8) -> Page {
        let mut page = create_page(
            PageFlags::IsLeaf,
            DEFAULT_PAGE_SIZE,
            HEADER_SIZE as u16,
            false,
        );
        page.set_num_keys(num_keys);
        page.mut_cell(HEADER_SIZE, HEADER_SIZE + 1)[0] = marker;
        page.set_checksum(page.compute_checksum());
        page
    }

    #[test]
    fn loads_multiple_pages_from_backing_store() {
        let pager = pager_with_pages([
            (1, test_page(10, b'a')),
            (2, test_page(20, b'b')),
            (3, test_page(30, b'c')),
        ]);

        for (id, expected_keys, expected_marker) in
            [(1, 10, b'a'), (2, 20, b'b'), (3, 30, b'c')]
        {
            let (num_keys, marker) = pager
                .page(id, AccessContext::anonymous(), |page| {
                    (
                        page.num_keys(),
                        page.cell(HEADER_SIZE, HEADER_SIZE + 1)[0],
                    )
                })
                .expect("page exists in backing store");

            assert_eq!(num_keys, expected_keys);
            assert_eq!(marker, expected_marker);
        }

        let mut cached_ids = pager
            .info()
            .into_iter()
            .map(|info| info.page_id)
            .collect::<Vec<_>>();
        cached_ids.sort_unstable();
        assert_eq!(cached_ids, vec![1, 2, 3]);
    }

    #[test]
    fn accessing_page_not_in_backing_store_returns_unexpected_eof() {
        let pager = pager_with_pages([(1, test_page(1, b'a'))]);

        let err = pager
            .page(2, AccessContext::anonymous(), |_| ())
            .expect_err("missing page should not load");

        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(pager.info().is_empty());
    }

    #[test]
    fn accessing_page_zero_is_invalid() {
        let pager = pager_with_pages([(1, test_page(1, b'a'))]);

        let err = pager
            .page(0, AccessContext::anonymous(), |_| ())
            .expect_err("page id zero is invalid");

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
