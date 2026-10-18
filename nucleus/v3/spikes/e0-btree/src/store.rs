//! Page ids, the error type and the two page stores of the E0 spike
//! (card X-0 Work 2; PLAN-v3 §3.3 pages and buffer manager).
//!
//! Invariants owned here:
//! - [`PageStore`] is the binding I/O interface for X-5 (E4): everything
//!   above it (pool, tree, tests, benchmarks) reaches storage only through
//!   these five methods, so X-5 can swap in a page map underneath.
//! - [`StoreError::Corrupt`] is the only answer bad on-disk bytes may
//!   produce (PLAN-v3 §6.1: corruption never panics and never is UB).
//! - [`MemStore`] mirrors exactly `Mutex<Vec<Box<[u8]>>>`; [`FileStore`] is
//!   Unix `pread`/`pwrite` via [`std::os::unix::fs::FileExt`]. The spike is
//!   Linux-only by design (card X-0 Work 2).

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Identifier of one page of a [`PageStore`].
///
/// Ids are allocated densely from 0 and are never reused in this spike: the
/// tree never frees a page (no merges), so a stale cursor can always safely
/// re-fetch a page it recorded (PLAN-v3 §3.3 "no leaf sibling links").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u64);

/// Error type of the whole spike: one `thiserror` enum per crate
/// (PLAN-v3 §6.1).
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// An I/O error from the underlying store or file.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// A page failed its structural checks. Never a panic, never UB
    /// (PLAN-v3 §6.1); `XX001` territory in the future engine.
    #[error("corrupt page: {0}")]
    Corrupt(String),
    /// A value exceeded the spike limit (card X-0 Work 5: no overflow pages).
    #[error("value of {0} bytes exceeds the spike limit of 2048")]
    ValueTooLarge(usize),
    /// A key exceeded the spike limit (kept small enough that any split can
    /// always leave both halves room for one more cell; see `page::MAX_KEY`).
    #[error("key of {0} bytes exceeds the spike limit of 2000")]
    KeyTooLarge(usize),
    /// A page id was never allocated in this store.
    #[error("page {0:?} was never allocated")]
    OutOfRange(PageId),
    /// Every frame stayed pinned across two full clock sweeps: the pool is
    /// smaller than threads × (tree height + 2), below the minimum of
    /// PLAN-v3 §3.3.
    #[error("buffer pool starved: no unpinned frame after two clock sweeps")]
    PoolStarved,
    /// A pool or store was constructed with unusable parameters.
    #[error("invalid configuration: {0}")]
    Invalid(String),
    /// A mutex needed for correctness was poisoned by a panic elsewhere.
    #[error("poisoned lock: {0}")]
    Poisoned(String),
}

/// The storage surface of the spike; binding for X-5, which implements it
/// over a logical-page-id page map (PLAN-v3 §3.3, N1).
///
/// `read`/`write` move whole pages; `buf` is always exactly
/// [`Self::page_size`] bytes.
pub trait PageStore: Send + Sync {
    /// Page size of this store; fixed for the store's lifetime.
    fn page_size(&self) -> usize;
    /// Read page `id` into `buf`.
    fn read(&self, id: PageId, buf: &mut [u8]) -> Result<(), StoreError>;
    /// Write `buf` into page `id`, creating it if needed.
    fn write(&self, id: PageId, buf: &[u8]) -> Result<(), StoreError>;
    /// Allocate a fresh page id.
    fn allocate(&self) -> Result<PageId, StoreError>;
    /// Free a page. The spike's tree never calls this (no merges); both
    /// stores accept and ignore it, documented per implementation.
    fn free(&self, id: PageId) -> Result<(), StoreError>;
}

/// In-memory [`PageStore`]: exactly a `Mutex<Vec<Box<[u8]>>>` of zeroed
/// pages, allocated densely from id 0.
#[derive(Debug)]
pub struct MemStore {
    pages: Mutex<Vec<Box<[u8]>>>,
    page_size: usize,
}

impl MemStore {
    /// A store of `page_size`-byte pages over the heap.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `page_size` is zero.
    pub fn new(page_size: usize) -> Result<Self, StoreError> {
        if page_size == 0 {
            return Err(StoreError::Invalid("page_size must be nonzero".into()));
        }
        Ok(Self {
            pages: Mutex::new(Vec::new()),
            page_size,
        })
    }

    /// Number of allocated pages (used by the benchmark to size pools).
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the page mutex was poisoned.
    pub fn page_count(&self) -> Result<usize, StoreError> {
        self.pages
            .lock()
            .map(|p| p.len())
            .map_err(|e| StoreError::Poisoned(e.to_string()))
    }
}

impl PageStore for MemStore {
    fn page_size(&self) -> usize {
        self.page_size
    }

    fn read(&self, id: PageId, buf: &mut [u8]) -> Result<(), StoreError> {
        let pages = self
            .pages
            .lock()
            .map_err(|e| StoreError::Poisoned(e.to_string()))?;
        let page = pages
            .get(id.0 as usize)
            .ok_or(StoreError::OutOfRange(id))?;
        let src = page
            .get(..)
            .ok_or_else(|| StoreError::Corrupt("short page in MemStore".into()))?;
        buf.get_mut(..)
            .ok_or_else(|| StoreError::Invalid("read buffer smaller than page".into()))?
            .copy_from_slice(src);
        Ok(())
    }

    fn write(&self, id: PageId, buf: &[u8]) -> Result<(), StoreError> {
        let mut pages = self
            .pages
            .lock()
            .map_err(|e| StoreError::Poisoned(e.to_string()))?;
        let page = pages
            .get_mut(id.0 as usize)
            .ok_or(StoreError::OutOfRange(id))?
            .get_mut(..)
            .ok_or_else(|| StoreError::Corrupt("short page in MemStore".into()))?;
        page.copy_from_slice(buf);
        Ok(())
    }

    fn allocate(&self) -> Result<PageId, StoreError> {
        let mut pages = self
            .pages
            .lock()
            .map_err(|e| StoreError::Poisoned(e.to_string()))?;
        let id = PageId(pages.len() as u64);
        pages.push(vec![0u8; self.page_size].into_boxed_slice());
        Ok(id)
    }

    fn free(&self, _id: PageId) -> Result<(), StoreError> {
        // The tree never frees (no merges in the spike). Accept and leak:
        // ids must stay dense because PageId indexes the Vec, and page
        // reclamation is E4's free-set work, not this spike's.
        Ok(())
    }
}

/// File-backed [`PageStore`] using Unix `pread`/`pwrite`
/// (`std::os::unix::fs::FileExt`); the spike is Linux-only (card X-0).
///
/// Pages are stored at offset `id * page_size`; the file grows on demand
/// and the next id is derived from the file length, so a file can be
/// reopened after `flush_all()` and read through a fresh pool.
#[derive(Debug)]
pub struct FileStore {
    file: File,
    page_size: usize,
    next: AtomicU64,
}

impl FileStore {
    /// Open (creating if absent) the page file at `path`.
    ///
    /// # Errors
    /// [`StoreError::Io`] from the open or metadata call;
    /// [`StoreError::Corrupt`] if the length is not a whole number of pages.
    pub fn open(path: &Path, page_size: usize) -> Result<Self, StoreError> {
        if page_size == 0 {
            return Err(StoreError::Invalid("page_size must be nonzero".into()));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let len = file.metadata()?.len();
        if len % page_size as u64 != 0 {
            return Err(StoreError::Corrupt(format!(
                "file length {len} is not a multiple of page size {page_size}"
            )));
        }
        Ok(Self {
            file,
            page_size,
            next: AtomicU64::new(len / page_size as u64),
        })
    }

    /// Number of pages the file holds (used by the benchmark to size pools).
    pub fn page_count(&self) -> usize {
        self.next.load(Ordering::Acquire) as usize
    }
}

impl PageStore for FileStore {
    fn page_size(&self) -> usize {
        self.page_size
    }

    fn read(&self, id: PageId, buf: &mut [u8]) -> Result<(), StoreError> {
        use std::os::unix::fs::FileExt;
        if id.0 >= self.next.load(Ordering::Acquire) {
            return Err(StoreError::OutOfRange(id));
        }
        let n = self.file.read_at(buf, id.0 * self.page_size as u64)?;
        if n != buf.len() {
            return Err(StoreError::Corrupt(format!(
                "short read of {n}/{} bytes on page {id:?}",
                buf.len()
            )));
        }
        Ok(())
    }

    fn write(&self, id: PageId, buf: &[u8]) -> Result<(), StoreError> {
        use std::os::unix::fs::FileExt;
        self.file
            .write_at(buf, id.0 * self.page_size as u64)?;
        Ok(())
    }

    fn allocate(&self) -> Result<PageId, StoreError> {
        Ok(PageId(self.next.fetch_add(1, Ordering::AcqRel)))
    }

    fn free(&self, _id: PageId) -> Result<(), StoreError> {
        // Not a truncation: ids index file offsets, so freed pages are
        // simply never reused in this spike (E4 owns the free set).
        Ok(())
    }
}
