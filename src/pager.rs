//! Pager and page-cache support for on-disk pages.
//!
use crate::{Page, PageFlags, page::FORMAT_VERSION};
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

/// Page identifier reserved for the root page.
///
/// Page identifiers are one-based; page id `0` is invalid.
pub const META_PAGE_ID: usize = 1;

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
    if let (_, Some(reason)) = page.valid() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, reason));
    }

    Ok(page)
}

/// Durably persists a [`Page`] to the given `writer` file, guaranteeing that
/// the written bytes are safely flushed and stored on disk.
///
/// Prior to writing, this refreshes the [`Page`]'s magic bytes and recomputes
/// its checksum, ensuring the persisted [`Page`] can be verified during
/// durability checks.
fn write_page(
    page_id: usize,
    size: usize,
    writer: &mut File,
    page: &mut Page,
) -> io::Result<()> {
    info!("writing page {page_id} (size: {size})");
    if page_id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "page id can not be zero",
        ));
    }

    page.set_magic();
    page.set_checksum(page.compute_checksum());

    let offset = (page_id - 1) * size;
    let size = writer.metadata()?.len();

    if offset > size as usize {
        // If offset is past the written size, resize the file till offset
        // and write page.
        writer.set_len(offset as u64)?;
    }

    writer.seek(SeekFrom::Start(offset as u64))?;
    writer.write_all(&page[..])?;
    writer.sync_all()?;

    Ok(())
}

/// [`FlushGuard`] defines a guarded function that should be run
/// before a page is committed/flushed to disk. A page is only
/// allowed to flush to disk if `before_flush` is successful.
pub trait FlushGuard: Send + Sync {
    fn before_flush(&self, page_id: u64, page: &Page) -> io::Result<()>;
}

pub struct NoopFlushGuard;

impl FlushGuard for NoopFlushGuard {
    fn before_flush(&self, _page_id: u64, _page: &Page) -> io::Result<()> {
        Ok(())
    }
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
    /// No specific access context
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
/// eviction or flush requests.
///
/// # Lock ordering
///
/// Several locks guard the pager's state. To stay deadlock-free every code
/// path that holds more than one at a time acquires them in this order and
/// releases them in the reverse order:
///
/// ```text
/// clock  >  pages  >  CachedPage::page  >  inner
/// ```
///
/// [`CachedPage::handles`] is only ever taken on its own (never while another
/// pager lock is held), so it sits outside this hierarchy. A lock later in the
/// chain must never be held while acquiring one earlier in the chain.
pub struct Pager {
    capacity: usize,
    inner: sync::Mutex<File>,
    page_size: u16,
    flush_guard: sync::Arc<dyn FlushGuard>,

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
    pub latest_lsn: u64,
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

impl Pager {
    /// Opens an existing pager file or creates a new one.
    ///
    /// New files are initialized with a metadata(meta) page using
    /// [`DEFAULT_PAGE_SIZE`]. Existing files read the meta page at the default
    /// size first so the stored page size can be discovered.
    pub fn open(path: impl Into<PathBuf>, capacity: usize) -> io::Result<Self> {
        let inner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(path.into())?;
        let len = inner.metadata()?.len();

        let mut out = Self {
            capacity,
            clock: sync::Mutex::new(ClockState {
                hand: 0,
                ring: vec![],
            }),
            flush_guard: sync::Arc::new(NoopFlushGuard),
            inner: sync::Mutex::new(inner),
            page_size: DEFAULT_PAGE_SIZE,
            pages: sync::RwLock::new(HashMap::with_capacity(capacity)),
        };

        if len >= DEFAULT_PAGE_SIZE as u64 {
            out.page_size = out.page(
                META_PAGE_ID,
                AccessContext::maintenance("startup"),
                |p| -> io::Result<u16> {
                    if !PageFlags::from_bits(p.flags())
                        .expect("is valid flag bits")
                        .contains(PageFlags::IsMeta)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unable to locate database meta page",
                        ));
                    }

                    if p.format_version() != FORMAT_VERSION {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unsupported format version",
                        ));
                    }

                    Ok(p.page_size())
                },
            )??;
        } else {
            let mut meta = Page::new(out.page_size, PageFlags::IsMeta);
            meta.set_next_page((META_PAGE_ID + 1) as u16);
            out.flush(META_PAGE_ID, &mut meta)?;
        }
        Ok(out)
    }

    /// Allocates `Self::page_size` in the storage file. Returning the
    /// new page_id.
    pub fn allocate_page(
        &self,
        ctx: AccessContext,
        flags: PageFlags,
    ) -> io::Result<usize> {
        let page_id = self.mut_page(META_PAGE_ID, ctx, |p| {
            let page_id = p.next_page();
            p.set_next_page(page_id + 1);
            if let Some(lsn) = ctx.lsn {
                p.set_lsn(lsn);
            }
            page_id
        })? as usize;

        let page_size = self.page_size as usize;
        let offset = page_id * page_size;

        if !offset.is_multiple_of(page_size) {
            warn!(
                "page storage file size ({offset} bytes) is not a multiple of page size ({} bytes); file structure may be corrupted",
                self.page_size
            )
        }

        let mut page = Page::new(self.page_size, flags);
        if let Some(lsn) = ctx.lsn {
            page.set_lsn(lsn);
        }
        self.track(page_id, page, true)?;

        Ok(page_id)
    }

    /// Set the [`FlushGuard`] for the [`Pager`]. Ensuring the set
    /// guards [`FlushGuard::before_flush`] is called before any data is synced
    /// to disk.
    pub fn set_guard(&mut self, guard: sync::Arc<dyn FlushGuard>) {
        self.flush_guard = guard;
    }

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
            AccessMode::Read,
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
            AccessMode::Read,
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
                "mutating page {page_id} without transaction or maintenance context."
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
            if let Some(lsn) = ctx.lsn {
                page.set_lsn(lsn)
            }

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
                latest_lsn: p
                    .page
                    .read()
                    .expect("able to acquire read lock")
                    .latest_lsn(),
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

    /// Attempts to flush every page currently tracked by the cache.
    ///
    /// If `evict` is `false`, each dirty page is written to the backing store and
    /// remains cached. Clean pages are left unchanged.
    ///
    /// If `evict` is `true`, each page is removed from the cache after it is known
    /// to be clean. Dirty pages are first flushed successfully, then evicted; pages
    /// that are already clean are evicted immediately.
    ///
    /// The set of page identifiers to flush is snapshotted before flushing begins.
    /// Pages loaded after that snapshot are not included in this call.
    ///
    /// ## Errors
    ///
    /// Callers should be prepared to handle these error cases:
    ///
    /// - [`io::ErrorKind::PermissionDenied`] when the page cache lock cannot be
    ///   acquired while collecting tracked pages or while flushing an individual
    ///   page.
    /// - [`io::ErrorKind::Other`] when a page that was present in the initial
    ///   snapshot is no longer tracked by the cache by the time it is flushed.
    /// - [`io::ErrorKind::ResourceBusy`] when a dirty page is pinned by an active
    ///   reader or writer. The caller may retry after the page is unpinned.
    /// - [`io::ErrorKind::ResourceBusy`] when a dirty page has been accessed
    ///   recently and receives a Clock-replacement second chance. The accessed bit
    ///   is cleared, and the caller may retry the flush later.
    /// - [`io::ErrorKind::PermissionDenied`] when the cached page contents cannot be
    ///   write-locked for flushing.
    /// - Any error returned by the configured [`FlushGuard`] before the page is
    ///   written.
    /// - Any error returned by the underlying readable, writable, seekable backing
    ///   store while writing page bytes.
    ///
    /// This function stops at the first error. Pages flushed before the error remain
    /// flushed, and pages evicted before the error remain evicted.
    pub fn flush_all(&self, evict: bool) -> io::Result<()> {
        let mut pages = self
            .pages
            .read()
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to acquire read lock on page cache",
                )
            })?
            .keys()
            .cloned()
            .collect::<Vec<usize>>();
        for page in pages.drain(..) {
            // TODO: Instead of failing immediately collect the pages
            //       that failed to flush and provide the information to the caller.
            //       This allows them to retry flushing on the specific cases ?
            //
            // NOTE: For now `Self::info()` is enough to debug
            self.flush_page(page, evict)?;
        }
        Ok(())
    }

    /// Flush a [`CachedPage`] to the underlying memory.
    ///
    /// If `evict` is `true`, the page is removed from the cache after it has been
    /// successfully flushed, or immediately if it is already clean.
    ///
    /// ## Errors
    ///
    /// This function returns an error in the following cases:
    ///
    /// - [`io::ErrorKind::PermissionDenied`] if the page cache write lock cannot be
    ///   acquired.
    /// - [`io::ErrorKind::Other`] if `page_id` does not refer to a page currently
    ///   tracked by the cache.
    /// - [`io::ErrorKind::ResourceBusy`] if the page is dirty and is currently
    ///   pinned.
    /// - [`io::ErrorKind::ResourceBusy`] if the page is dirty and has been accessed
    ///   recently. In this case, the accessed flag is cleared and the caller may
    ///   retry the flush later.
    /// - [`io::ErrorKind::PermissionDenied`] if the cached page contents write lock
    ///   cannot be acquired.
    /// - Any error returned by the underlying flush operation.
    pub fn flush_page(&self, page_id: usize, evict: bool) -> io::Result<()> {
        let evicted = self.flush_page_maps_only(page_id, evict)?;
        if evicted {
            self.remove_from_ring(page_id);
        }
        Ok(())
    }

    /// Flush and optionally evict `page_id` from the `pages` map only.
    ///
    /// Returns `true` when the page was removed from the map so the caller can
    /// follow up with [`Self::remove_from_ring`] once the `pages` lock has been
    /// dropped.
    fn flush_page_maps_only(
        &self,
        page_id: usize,
        evict: bool,
    ) -> io::Result<bool> {
        info!("page flush attempt: page={page_id}, evict={evict}");
        let mut pages = self
            .pages
            .write()
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to acquire write lock on page cache",
                )
            })?;
        let Some(cached_page) = pages.get(&page_id) else {
            info!(
                "page flush attempt fail: page={page_id}, evict={evict} UNCACHED"
            );
            return Err(io::Error::other("untracked page"));
        };

        if cached_page.is_pinned() {
            trace!(
                "page flush attempt fail: page={page_id}, evict={evict}, pin=true, accessed=?"
            );
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "page is in use",
            ));
        }

        if cached_page
            .dirty
            .load(Ordering::Acquire)
        {
            if cached_page
                .accessed
                .swap(false, Ordering::AcqRel)
            {
                trace!(
                    "page flush attempt fail: page={page_id}, evict={evict}, pin=false, accessed=true"
                );
                return Err(io::Error::new(
                    io::ErrorKind::ResourceBusy,
                    "page has been accessed recently",
                ));
            }

            let mut page = cached_page
                .page
                .write()
                .map_err(|_e| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "failed to acquire write lock on cached page contents",
                    )
                })?;

            // Re-check; lock might have been acquired after pin
            if cached_page.is_pinned() {
                trace!(
                    "page flush attempt fail: page={page_id}, evict={evict}, pin=true, accessed=false"
                );
                return Err(io::Error::new(
                    io::ErrorKind::ResourceBusy,
                    "page is in use",
                ));
            }

            self.flush(page_id, &mut page)?;
            cached_page
                .dirty
                .store(false, Ordering::Release);
        }

        if evict {
            info!("page flush: page {page_id} has been evicted");
            pages.remove(&page_id);
            return Ok(true);
        }

        Ok(false)
    }

    /// Remove `page_id` from the Clock ring.
    ///
    /// Must only be called **after** the `pages` lock has been released so the
    /// clock-before-pages lock ordering is respected.
    fn remove_from_ring(&self, page_id: usize) {
        let Ok(mut clock) = self.clock.lock() else {
            return;
        };
        if let Some(pos) = clock
            .ring
            .iter()
            .position(|&id| id == page_id)
        {
            clock.ring.swap_remove(pos);
            if clock.hand >= clock.ring.len() && !clock.ring.is_empty() {
                clock.hand = 0;
            }
        }
    }

    /// Write [`Page`] to the underlying disk storage.
    fn flush(&self, page_id: usize, page: &mut Page) -> io::Result<()> {
        let page_lsn = page.latest_lsn();

        info!("page flush requested: page_id={page_id} page_lsn={page_lsn}");
        self.flush_guard
            .before_flush(page_id as u64, page)?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to acquire lock on pager state",
                )
            })?;
        write_page(page_id, self.page_size as usize, &mut inner, page)?;
        let flags =
            PageFlags::from_bits(page.flags()).expect("flags is parseable");
        info!(
            "page flushed: page_id={page_id} page_lsn={page_lsn} meta={} leaf={} internal={}",
            flags.contains(PageFlags::IsMeta),
            flags.contains(PageFlags::IsLeaf),
            flags.contains(PageFlags::IsInternal)
        );
        Ok(())
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
            cached_page
                .accessed
                .store(true, Ordering::Release);

            return Ok(cached_page);
        }

        // Snapshot the current cache occupancy so we can decide whether an
        // eviction is required before loading. Another thread may load the same
        // page after this read lock is dropped; `track` performs a final
        // double-check under lock before inserting.
        let cache_count = {
            let pages = self
                .pages
                .read()
                .map_err(|_e| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "failed to request read lock on pager state",
                    )
                })?;

            if let Some(cached_page) = pages.get(&page_id).cloned() {
                cached_page
                    .accessed
                    .store(true, Ordering::Release);
                return Ok(cached_page);
            }

            pages.len()
        };

        if cache_count >= self.capacity {
            // Eviction locks `clock` then `pages`; we hold neither here.
            self.evict_one()?;
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

        self.track(page_id, page, false)
    }

    /// Caches a [`Page`] in memory, returning the tracked [`CachedPage`].
    ///
    /// This performs an atomic check-and-insert: if another thread already
    /// tracked `id` in the window since the caller last checked, the existing
    /// entry is returned and `page` is discarded. The Clock ring is only
    /// extended when a genuinely new entry is inserted, keeping it in sync with
    /// the cache map.
    ///
    /// The `clock` lock is acquired before `pages` to respect the pager's lock
    /// ordering (see [`Pager`]).
    fn track(
        &self,
        id: usize,
        page: Page,
        dirty: bool,
    ) -> io::Result<sync::Arc<CachedPage>> {
        trace!("page start tracking: page={id}, dirty={dirty}");

        let mut clock = self
            .clock
            .lock()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to acquire clock state lock",
                )
            })?;
        let mut pages = self
            .pages
            .write()
            .map_err(|_e| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "failed to retrieve write lock",
                )
            })?;

        // Another thread may have loaded and tracked this page while we were
        // reading it from the backing store. Prefer the existing entry so all
        // callers share a single `CachedPage` per page id.
        if let Some(existing) = pages.get(&id).cloned() {
            existing
                .accessed
                .store(true, Ordering::Release);
            return Ok(existing);
        }

        let cached = sync::Arc::new(CachedPage::new(id, page, dirty));
        pages.insert(id, cached.clone());
        clock.ring.push(id);
        Ok(cached)
    }

    /// Evicts a single page from the page cache using a variant of
    /// the Clock Page Replacement algorithm:
    ///   <https://en.wikipedia.org/wiki/Page_replacement_algorithm#Clock>.
    fn evict_one(&self) -> io::Result<()> {
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
        info!("page evict: candidate=\n\t{:?}", clock.ring);

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

            info!("page evict: candidate={page_id}");
            // Use flush_page_maps_only: we already hold the clock lock so we
            // must not call flush_page (which would call remove_from_ring and
            // try to re-acquire the clock lock, causing a deadlock). Ring
            // cleanup is our responsibility here.
            match self.flush_page_maps_only(page_id, true) {
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ResourceBusy
                            | io::ErrorKind::PermissionDenied
                    ) =>
                {
                    clock.hand += 1;
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::Other => {
                    clock.ring.swap_remove(hand);
                    if clock.hand >= clock.ring.len() && !clock.ring.is_empty()
                    {
                        clock.hand = 0;
                    }
                    continue;
                }
                Err(e) => return Err(e),
                Ok(_) => {
                    info!("page evict success: page={page_id}");
                    clock.ring.swap_remove(hand);

                    if clock.hand >= clock.ring.len() && !clock.ring.is_empty()
                    {
                        clock.hand = 0;
                    }

                    return Ok(());
                }
            }
        }

        debug!("page evict fail: all pages are currently pinned");
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "all cached pages are pinned",
        ))
    }
}

impl fmt::Display for Pager {
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
    use tempfile::TempDir;

    fn temp_pager(capacity: usize) -> (TempDir, Pager) {
        let dir = TempDir::new().expect("temp dir can be created");
        let path = dir.path().join("cryo.db");
        let pager = Pager::open(path, capacity).expect("pager can be created");
        (dir, pager)
    }

    fn pager_with_pages(
        pages: impl IntoIterator<Item = (u16, u8)>,
    ) -> (TempDir, Pager, Vec<usize>) {
        let (dir, pager) = temp_pager(8);
        let mut ids = Vec::new();

        for (num_keys, marker) in pages {
            let page_id = pager
                .allocate_page(
                    AccessContext::maintenance("pager with pages"),
                    PageFlags::IsLeaf,
                )
                .expect("test page can be allocated");
            pager
                .mut_page(
                    page_id,
                    AccessContext::maintenance("test setup"),
                    |page| {
                        let start = page.free_space_start() as usize;
                        page.set_num_keys(num_keys);
                        page.mut_cell(start, start + 1)[0] = marker;
                    },
                )
                .expect("test page can be initialized");
            pager
                .flush_page(page_id, false)
                .expect_err("first flush clears accessed bit");
            pager
                .flush_page(page_id, true)
                .expect("test page can be persisted and evicted");
            pager
                .flush_page(META_PAGE_ID, false)
                .expect_err("first flush clears accessed bit");
            pager
                .flush_page(META_PAGE_ID, true)
                .expect("test page can be persisted and evicted");
            ids.push(page_id);
        }

        (dir, pager, ids)
    }

    fn persisted_page(pager: &Pager, page_id: usize) -> io::Result<Page> {
        let mut inner = pager
            .inner
            .lock()
            .expect("test can lock pager backing store");
        load_page(page_id, DEFAULT_PAGE_SIZE as usize, &mut *inner)
    }

    struct FailingFlushGuard;

    impl FlushGuard for FailingFlushGuard {
        fn before_flush(&self, _page_id: u64, _page: &Page) -> io::Result<()> {
            Err(io::Error::other("blocked by test guard"))
        }
    }

    #[test]
    fn loads_multiple_pages_from_backing_store() {
        let (_dir, pager, ids) =
            pager_with_pages([(10, b'a'), (20, b'b'), (30, b'c')]);

        for (id, expected_keys, expected_marker) in
            [(ids[0], 10, b'a'), (ids[1], 20, b'b'), (ids[2], 30, b'c')]
        {
            let (num_keys, marker) = pager
                .page(id, AccessContext::anonymous(), |page| {
                    (
                        page.num_keys(),
                        page.cell(
                            page.free_space_start() as usize,
                            page.free_space_start() as usize + 1,
                        )[0],
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
        assert_eq!(cached_ids, ids);
    }

    #[test]
    fn accessing_page_not_in_backing_store_returns_unexpected_eof() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a')]);
        let missing = ids[0] + 1;

        let err = pager
            .page(missing, AccessContext::anonymous(), |_| ())
            .expect_err("missing page should not load");

        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(pager.info().is_empty());
    }

    #[test]
    fn flushing_untracked_page_returns_other() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a')]);

        let err = pager
            .flush_page(ids[0], false)
            .expect_err("uncached page should not flush");

        assert_eq!(err.kind(), io::ErrorKind::Other);
    }

    #[test]
    fn flushing_clean_page_with_evict_removes_it_from_cache() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a')]);

        pager
            .page(ids[0], AccessContext::anonymous(), |_| ())
            .expect("page can be loaded into cache");

        assert_eq!(pager.info().len(), 1);

        pager
            .flush_page(ids[0], true)
            .expect("clean page can be evicted");

        assert!(pager.info().is_empty());
    }

    #[test]
    fn dirty_page_gets_second_chance_before_flush() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a')]);

        pager
            .mut_page(
                ids[0],
                AccessContext::maintenance("test mutation"),
                |page| {
                    page.set_num_keys(42);
                    page.mut_cell(
                        page.free_space_start() as usize,
                        page.free_space_start() as usize + 1,
                    )[0] = b'z';
                },
            )
            .expect("page can be mutated");

        let err = pager
            .flush_page(ids[0], false)
            .expect_err(
                "recently accessed dirty page should get a second chance",
            );
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);

        let info = pager.info();
        assert_eq!(info.len(), 1);
        assert!(info[0].dirty);
        assert!(!info[0].accessed);

        pager
            .flush_page(ids[0], false)
            .expect("dirty page can flush after second chance is cleared");

        let info = pager.info();
        assert_eq!(info.len(), 1);
        assert!(!info[0].dirty);

        let persisted =
            persisted_page(&pager, ids[0]).expect("flushed page is valid");
        assert_eq!(persisted.num_keys(), 42);
        assert_eq!(
            persisted.cell(
                persisted.free_space_start() as usize,
                persisted.free_space_start() as usize + 1
            )[0],
            b'z'
        );
    }

    #[test]
    fn dirty_page_can_be_flushed_and_evicted() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a')]);

        pager
            .mut_page(
                ids[0],
                AccessContext::maintenance("test mutation"),
                |page| {
                    page.set_num_keys(7);
                    page.mut_cell(
                        page.free_space_start() as usize,
                        page.free_space_start() as usize + 1,
                    )[0] = b'x';
                },
            )
            .expect("page can be mutated");

        pager
            .flush_page(ids[0], false)
            .expect_err("first flush clears accessed bit");
        pager
            .flush_page(ids[0], true)
            .expect("second flush writes and evicts dirty page");

        assert!(pager.info().is_empty());

        let persisted =
            persisted_page(&pager, ids[0]).expect("flushed page is valid");
        assert_eq!(persisted.num_keys(), 7);
        assert_eq!(
            persisted.cell(
                persisted.free_space_start() as usize,
                persisted.free_space_start() as usize + 1
            )[0],
            b'x'
        );
    }

    #[test]
    fn pinned_dirty_page_is_resource_busy() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a')]);
        let cached = pager
            .get_or_load(ids[0])
            .expect("page can be loaded into cache");
        cached.pin();
        cached
            .dirty
            .store(true, Ordering::Release);
        cached
            .accessed
            .store(false, Ordering::Release);

        let err = pager
            .flush_page(ids[0], false)
            .expect_err("pinned dirty page should not flush");

        cached.unpin();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        assert!(pager.info()[0].dirty);
    }

    #[test]
    fn flush_guard_error_prevents_write_and_keeps_page_dirty() {
        let (_dir, mut pager, ids) = pager_with_pages([(1, b'a')]);
        pager.set_guard(sync::Arc::new(FailingFlushGuard));

        pager
            .mut_page(
                ids[0],
                AccessContext::maintenance("test mutation"),
                |page| {
                    page.set_num_keys(99);
                    page.mut_cell(
                        page.free_space_start() as usize,
                        page.free_space_start() as usize + 1,
                    )[0] = b'q';
                },
            )
            .expect("page can be mutated");

        let cached = pager
            .get_or_load(ids[0])
            .expect("page remains cached after mutation");
        cached
            .accessed
            .store(false, Ordering::Release);

        let err = pager
            .flush_page(ids[0], false)
            .expect_err("failing guard should block flush");

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(pager.info()[0].dirty);

        let persisted =
            persisted_page(&pager, ids[0]).expect("original page is valid");
        assert_eq!(persisted.num_keys(), 1);
        assert_eq!(
            persisted.cell(
                persisted.free_space_start() as usize,
                persisted.free_space_start() as usize + 1
            )[0],
            b'a'
        );
    }

    #[test]
    fn flush_all_flushes_dirty_pages_and_keeps_them_cached() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a'), (2, b'b')]);

        pager
            .mut_page(
                ids[0],
                AccessContext::maintenance("test mutation"),
                |page| {
                    page.set_num_keys(11);
                    page.mut_cell(
                        page.free_space_start() as usize,
                        page.free_space_start() as usize + 1,
                    )[0] = b'x';
                },
            )
            .expect("page 1 can be mutated");
        pager
            .mut_page(
                ids[1],
                AccessContext::maintenance("test mutation"),
                |page| {
                    page.set_num_keys(22);
                    page.mut_cell(
                        page.free_space_start() as usize,
                        page.free_space_start() as usize + 1,
                    )[0] = b'y';
                },
            )
            .expect("page 2 can be mutated");

        for page_id in &ids {
            pager
                .get_or_load(*page_id)
                .expect("page remains cached")
                .accessed
                .store(false, Ordering::Release);
        }

        pager
            .flush_all(false)
            .expect("dirty pages can be flushed");

        let mut info = pager.info();
        info.sort_by_key(|info| info.page_id);
        assert_eq!(info.len(), 2);
        assert_eq!(info[0].page_id, ids[0]);
        assert!(!info[0].dirty);
        assert_eq!(info[1].page_id, ids[1]);
        assert!(!info[1].dirty);

        let persisted =
            persisted_page(&pager, ids[0]).expect("page 1 was flushed");
        assert_eq!(persisted.num_keys(), 11);
        assert_eq!(
            persisted.cell(
                persisted.free_space_start() as usize,
                persisted.free_space_start() as usize + 1
            )[0],
            b'x'
        );

        let persisted =
            persisted_page(&pager, ids[1]).expect("page 2 was flushed");
        assert_eq!(persisted.num_keys(), 22);
        assert_eq!(
            persisted.cell(
                persisted.free_space_start() as usize,
                persisted.free_space_start() as usize + 1
            )[0],
            b'y'
        );
    }

    #[test]
    fn flush_all_evicts_clean_pages() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a'), (2, b'b')]);

        pager
            .page(ids[0], AccessContext::anonymous(), |_| ())
            .expect("page 1 can be loaded");
        pager
            .page(ids[1], AccessContext::anonymous(), |_| ())
            .expect("page 2 can be loaded");

        assert_eq!(pager.info().len(), 2);

        pager
            .flush_all(true)
            .expect("clean pages can be evicted");

        assert!(pager.info().is_empty());
    }

    #[test]
    fn flush_all_evicts_dirty_pages_after_flush() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a'), (2, b'b')]);
        eprintln!("pager: {pager} ids: {ids:?}");

        pager
            .mut_page(
                ids[0],
                AccessContext::maintenance("test mutation"),
                |page| {
                    page.set_num_keys(31);
                    page.mut_cell(
                        page.free_space_start() as usize,
                        page.free_space_start() as usize + 1,
                    )[0] = b'm';
                },
            )
            .expect("page 1 can be mutated");
        pager
            .mut_page(
                ids[1],
                AccessContext::maintenance("test mutation"),
                |page| {
                    page.set_num_keys(32);
                    page.mut_cell(
                        page.free_space_start() as usize,
                        page.free_space_start() as usize + 1,
                    )[0] = b'n';
                },
            )
            .expect("page 2 can be mutated");

        for page_id in &ids {
            pager
                .get_or_load(*page_id)
                .expect("page remains cached")
                .accessed
                .store(false, Ordering::Release);
        }

        pager
            .flush_all(true)
            .expect("dirty pages can be flushed and evicted");

        assert!(pager.info().is_empty());

        let persisted =
            persisted_page(&pager, ids[0]).expect("page 1 was flushed");
        assert_eq!(persisted.num_keys(), 31);
        assert_eq!(
            persisted.cell(
                persisted.free_space_start() as usize,
                persisted.free_space_start() as usize + 1
            )[0],
            b'm'
        );

        let persisted =
            persisted_page(&pager, ids[1]).expect("page 2 was flushed");
        assert_eq!(persisted.num_keys(), 32);
        assert_eq!(
            persisted.cell(
                persisted.free_space_start() as usize,
                persisted.free_space_start() as usize + 1
            )[0],
            b'n'
        );
    }

    #[test]
    fn flush_all_returns_resource_busy_for_recently_accessed_dirty_page() {
        let (_dir, pager, ids) = pager_with_pages([(1, b'a')]);

        pager
            .mut_page(
                ids[0],
                AccessContext::maintenance("test mutation"),
                |page| {
                    page.set_num_keys(44);
                    page.mut_cell(
                        page.free_space_start() as usize,
                        page.free_space_start() as usize + 1,
                    )[0] = b'r';
                },
            )
            .expect("page can be mutated");

        let err = pager
            .flush_all(false)
            .expect_err("recently accessed dirty page should not flush");
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);

        let info = pager.info();
        assert_eq!(info.len(), 1);
        assert!(info[0].dirty);
        assert!(!info[0].accessed);

        pager
            .flush_all(false)
            .expect("dirty page can flush after second chance is cleared");

        assert!(!pager.info()[0].dirty);

        let persisted =
            persisted_page(&pager, ids[0]).expect("flushed page is valid");
        assert_eq!(persisted.num_keys(), 44);
        assert_eq!(
            persisted.cell(
                persisted.free_space_start() as usize,
                persisted.free_space_start() as usize + 1
            )[0],
            b'r'
        );
    }

    #[test]
    fn accessing_page_zero_is_invalid() {
        let (_dir, pager, _ids) = pager_with_pages([(1, b'a')]);

        let err = pager
            .page(0, AccessContext::anonymous(), |_| ())
            .expect_err("page id zero is invalid");

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
