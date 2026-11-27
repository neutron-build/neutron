// Provenance (card A-0): nucleus/v3/spikes/e0-btree/src/store.rs at 7ccd1cff8ef926f95a240b15d4b9c6a346251efe lines 1-166.
// Edits: the module doc drops its FileStore sentence; the three imports only FileStore used are deleted; FileStore itself (source lines 167-251) is not copied, so I/O reaches disk only through nucleus-env (card A-1a).
//! Page ids, the error type, the page-store trait and its in-memory store, from the E0 spike
//! (card X-0 Work 2; PLAN-v3 §3.3 pages and buffer manager).
//!
//! Invariants owned here:
//! - [`PageStore`] is the seed's only page I/O surface: the pool and the
//!   tree reach storage only through its five methods. A-3a and A-3c
//!   replace it with frames read through the page map (S-STORE §4.1, §5).
//! - [`StoreError::Corrupt`] is the only answer bad on-disk bytes may
//!   produce (PLAN-v3 §6.1: corruption never panics and never is UB).
//! - [`MemStore`] mirrors exactly `Mutex<Vec<Box<[u8]>>>`.

use std::io;
use std::sync::Mutex;

/// Identifier of one page of a [`PageStore`].
///
/// Ids are allocated densely from 0 and are never reused in this spike: the
/// tree never frees a page (no merges), so a stale cursor can always safely
/// re-fetch a page it recorded (PLAN-v3 §3.3 "no leaf sibling links").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u64);

/// The crate's error type: one `thiserror` enum per crate (PLAN-v3 §6.1).
/// Its SQLSTATE mapping is A-3c's and A-4c's (lib.rs, Seed status).
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// An I/O error from the underlying store or file.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// A page failed its structural checks. Never a panic, never UB
    /// (PLAN-v3 §6.1); `XX001` territory in the future engine.
    #[error("corrupt page: {0}")]
    Corrupt(String),
    /// A value longer than `V_INLINE` (S-STORE §3.2); overflow chains are card A-5's.
    #[error("value of {0} bytes exceeds V_INLINE (S-STORE §3.2)")]
    ValueTooLarge(usize),
    /// A key longer than `K_MAX` (S-STORE §3.2, I-KEY-SIZE): SQLSTATE 54000.
    #[error("key of {0} bytes exceeds K_MAX (S-STORE §3.2)")]
    KeyTooLarge(usize),
    /// A page id was never allocated in this store.
    #[error("page {0:?} was never allocated")]
    OutOfRange(PageId),
    /// Every frame stayed pinned across two full clock sweeps: the pool is
    /// smaller than threads × (tree height + 2), below the minimum of
    /// PLAN-v3 §3.3.
    /// Card A-3b-0: `buf::Pool`'s frame acquisition returns it when no frame is
    /// free and every unpinned frame is dirty (A-3c writes dirty victims).
    #[error("buffer pool starved: no unpinned frame after two clock sweeps")]
    PoolStarved,
    /// A pool or store was constructed with unusable parameters.
    #[error("invalid configuration: {0}")]
    Invalid(String),
    /// A mutex needed for correctness was poisoned by a panic elsewhere.
    #[error("poisoned lock: {0}")]
    Poisoned(String),
    /// A `nucleus-env` call failed (card A-3a: a page read through
    /// `buf::Pool`). Its SQLSTATE is the `EnvError`'s (S-STORE's error
    /// table): 58030, or 53100 for `NoSpace`.
    #[error("env: {0}")]
    Env(#[from] nucleus_env::EnvError),
    /// An allocation the store makes once, at construction, failed (card
    /// A-3a: `buf::Pool`'s frames and page table): SQLSTATE 53200 (N16).
    #[error("out of memory: {0}")]
    OutOfMemory(String),
    /// The pool is below S-STORE §5.4's minimum with R-1-memory's values
    /// (card A-3b-0): SQLSTATE 53200. The open is refused and writes nothing.
    #[error(
        "buffer pool of {frames} frames is below its minimum of {minimum} frames (S-STORE §5.4)"
    )]
    BelowMinimum {
        /// The frame count asked for.
        frames: u32,
        /// `threads × 3 × (H_min + 2) + R_EPOCH + R_MAP` (R-1-memory §4.2).
        minimum: u64,
    },
    /// The operation must release what it holds and restart (S-STORE §5.2,
    /// §5.3): no frame could be obtained without blocking where the caller
    /// may not block. Never returned to a client (card A-3c).
    #[error("restart: {0:?}")]
    Restart(crate::buf::Restart),
}

/// The seed's page storage surface (card A-0). E4 implemented it over a
/// page map; the engine replaces it with the page map and frames (A-3a,
/// A-3c).
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
    /// Free a page. The spike's tree never calls this (no merges);
    /// `MemStore` accepts and ignores it, documented per implementation.
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
        let page = pages.get(id.0 as usize).ok_or(StoreError::OutOfRange(id))?;
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
