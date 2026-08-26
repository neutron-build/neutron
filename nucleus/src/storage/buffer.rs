//! Buffer pool manager — the page cache.
//!
//! All page access goes through the buffer pool. Pages are pinned while in use
//! and evicted via LRU-K(2) when memory pressure requires it.

use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::disk::DiskManager;
use super::page::{self, INVALID_PAGE_ID, PAGE_SIZE, PageBuf};

/// Default buffer pool: 2048 frames x 16 KB = 32 MB.
pub const DEFAULT_POOL_SIZE: usize = 2048;

// ============================================================================
// Buffer pool statistics
// ============================================================================

/// Thread-safe statistics for buffer pool monitoring and observability.
#[derive(Debug)]
pub struct BufferPoolStats {
    /// Number of page fetches that found the page already in the pool.
    pub hits: AtomicU64,
    /// Number of page fetches that required reading from disk.
    pub misses: AtomicU64,
    /// Number of frames evicted to make room for new pages.
    pub evictions: AtomicU64,
    /// Current number of dirty pages in the pool.
    pub dirty_pages: AtomicU64,
}

impl BufferPoolStats {
    fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            dirty_pages: AtomicU64::new(0),
        }
    }

    /// Get the hit ratio (0.0 to 1.0). Returns 0.0 if no accesses yet.
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get a snapshot of all stats as a tuple: (hits, misses, evictions, dirty_pages).
    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
            self.dirty_pages.load(Ordering::Relaxed),
        )
    }
}

// ============================================================================
// Aligned page frame
// ============================================================================

/// 16 KB page buffer aligned to 4096 bytes for Direct I/O readiness.
#[repr(C, align(4096))]
pub struct AlignedPage {
    pub data: PageBuf,
}

impl AlignedPage {
    fn new() -> Self {
        Self {
            data: [0u8; PAGE_SIZE],
        }
    }
}

// ============================================================================
// Frame descriptor
// ============================================================================

/// Metadata for a single buffer pool frame.
pub struct FrameDescriptor {
    /// Page ID loaded in this frame (INVALID_PAGE_ID = empty).
    pub page_id: AtomicU32,
    /// Number of active pins. Cannot evict while > 0.
    pub pin_count: AtomicU32,
    /// Dirty flag.
    pub is_dirty: AtomicBool,
    /// Read-write latch protecting the page CONTENT — the 16 KB of bytes in the
    /// frame, as opposed to `pin_count`, which protects the frame's *identity*
    /// (which page lives here, and whether it may be evicted). A pin alone does
    /// not make a page's bytes yours: two sessions inserting into the same page
    /// both pin it, and without this latch both run `page::insert_tuple` over
    /// the same slot array and free-space pointer, each overwriting the other's
    /// bookkeeping. That is a byte-level data race, and it reproduces as
    /// duplicate primary keys, lost rows, and out-of-bounds slot offsets read
    /// back later as panics.
    ///
    /// # THE LOCK ORDER
    ///
    /// Every lock in the paged storage stack (`buffer.rs`, `disk_engine.rs`,
    /// `btree.rs`) is assigned a level. A thread acquires locks in strictly
    /// increasing level order, and never the reverse. Deadlock freedom follows
    /// from the levels being a total order plus rule (B) below.
    ///
    /// ```text
    ///   L0  DiskEngine::dir_save_lock
    ///   L1  DiskEngine::tables
    ///   L2  DiskEngine::txn_state
    ///   L3  DiskEngine::indexes        (covers ALL B-tree page mutation)
    ///   L4  DiskEngine::free_list_head, then free_page_count
    ///   L5  BufferPool admission: eviction_lock, free_list, page_table
    ///       partitions, replacer shards  -- i.e. everything fetch_page /
    ///       new_page / unpin touch
    ///   L6  FrameDescriptor::latch      <-- THIS LOCK
    ///   L7  BufferPool bookkeeping: dirty_set, wal_pending
    ///   L8  WalBackend / DiskManager internals
    /// ```
    ///
    /// `tables` sits above `txn_state` because `alloc_data_page` links a new
    /// page into the chain under `tables` and calls `record_dirty_page`
    /// (`txn_state`) inside that section. The reverse never happens: both
    /// `abort_txn` and `rollback_open_txn_in_memory` `take()` the txn state and
    /// drop its guard before restoring `tables`.
    ///
    /// Two rules make L6 safe to retrofit onto code that never had it:
    ///
    /// **(A) A frame latch is acquired after every engine-level lock and before
    /// nothing except pool bookkeeping and I/O.** In particular a thread
    /// holding a latch must NOT call `fetch_page`, `new_page`, or `unpin` (L5),
    /// and must not reach for `tables`, `indexes`, or the free list (L2-L4).
    /// The concrete shape this forbids is the tempting one: latch a data page,
    /// then do index maintenance, then allocate an overflow page. Sites that
    /// want that sequence release the latch first and re-take it after, which
    /// is why `DiskEngine::delete_at` / `update_at` collect their index work
    /// into a list and drain it outside the latched region.
    ///
    /// **(B) At most ONE frame latch is held at a time.** No site anywhere
    /// holds two. Splitting a B-tree node reads the source page into a local
    /// `Vec`, drops the latch, and then writes the destination pages one latch
    /// at a time. Because no thread ever waits for a latch while holding a
    /// latch, latches cannot form a cycle among themselves regardless of the
    /// order pages are visited in, so no page-ordering discipline is needed.
    ///
    /// Holding L6 across an `.await` would be both a correctness and a
    /// liveness hazard. `parking_lot`'s guards are `!Send`, and every storage
    /// future in this crate must be `Send`, so the compiler rejects it — which
    /// is why [`PageReadGuard`] / [`PageWriteGuard`] deliberately wrap a
    /// `parking_lot` guard instead of an owning handle.
    pub latch: RwLock<()>,
}

impl FrameDescriptor {
    fn new() -> Self {
        Self {
            page_id: AtomicU32::new(INVALID_PAGE_ID),
            pin_count: AtomicU32::new(0),
            is_dirty: AtomicBool::new(false),
            latch: RwLock::new(()),
        }
    }
}

// ============================================================================
// Page guards
// ============================================================================

/// A pinned, read-latched page. Derefs to the page bytes.
///
/// Releasing the guard drops the latch and then the pin, in that order — the
/// frame cannot be evicted out from under a latch holder.
///
/// See [`FrameDescriptor::latch`] for the lock order this participates in.
/// The short version: hold at most one of these, and acquire no other lock
/// while you do.
pub struct PageReadGuard<'a> {
    pool: &'a BufferPool,
    frame_id: u32,
    latch: Option<RwLockReadGuard<'a, ()>>,
}

impl PageReadGuard<'_> {
    /// The frame this page is resident in.
    pub fn frame_id(&self) -> u32 {
        self.frame_id
    }
}

impl Deref for PageReadGuard<'_> {
    type Target = PageBuf;
    fn deref(&self) -> &PageBuf {
        self.pool.frame_data(self.frame_id)
    }
}

impl Drop for PageReadGuard<'_> {
    fn drop(&mut self) {
        self.latch = None;
        self.pool.unpin(self.frame_id);
    }
}

/// A pinned, write-latched page. Derefs mutably to the page bytes.
///
/// Call [`PageWriteGuard::set_dirty`] after modifying the page; the buffer
/// pool is told at release, after the latch is dropped but before the pin is,
/// so a flusher can never observe a half-written page and can never miss the
/// dirty mark.
pub struct PageWriteGuard<'a> {
    pool: &'a BufferPool,
    frame_id: u32,
    latch: Option<RwLockWriteGuard<'a, ()>>,
    dirty: bool,
}

impl PageWriteGuard<'_> {
    /// The frame this page is resident in.
    pub fn frame_id(&self) -> u32 {
        self.frame_id
    }

    /// Record that the page was modified. Marked dirty when the guard drops.
    pub fn set_dirty(&mut self) {
        self.dirty = true;
    }
}

impl Deref for PageWriteGuard<'_> {
    type Target = PageBuf;
    fn deref(&self) -> &PageBuf {
        self.pool.frame_data(self.frame_id)
    }
}

impl DerefMut for PageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut PageBuf {
        self.pool.frame_data_mut(self.frame_id)
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        self.latch = None;
        if self.dirty {
            self.pool.mark_dirty(self.frame_id);
        }
        self.pool.unpin(self.frame_id);
    }
}

// ============================================================================
// Page table (partitioned hash map)
// ============================================================================

const NUM_PARTITIONS: usize = 64;

struct PageTable {
    partitions: [Mutex<HashMap<u32, u32>>; NUM_PARTITIONS],
}

impl PageTable {
    fn new() -> Self {
        Self {
            partitions: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    fn partition_for(&self, page_id: u32) -> usize {
        (page_id as usize) % NUM_PARTITIONS
    }

    fn lookup(&self, page_id: u32) -> Option<u32> {
        let idx = self.partition_for(page_id);
        self.partitions[idx].lock().get(&page_id).copied()
    }

    fn insert(&self, page_id: u32, frame_id: u32) {
        let idx = self.partition_for(page_id);
        self.partitions[idx].lock().insert(page_id, frame_id);
    }

    fn remove(&self, page_id: u32) {
        let idx = self.partition_for(page_id);
        self.partitions[idx].lock().remove(&page_id);
    }
}

// ============================================================================
// LRU-K replacer (K=2)
// ============================================================================

struct FrameHistory {
    access_history: VecDeque<u64>,
    is_evictable: bool,
}

const REPLACER_SHARDS: usize = 16;

struct LruKReplacer {
    k: usize,
    current_ts: AtomicU64,
    shards: [Mutex<HashMap<u32, FrameHistory>>; REPLACER_SHARDS],
}

impl LruKReplacer {
    fn new(k: usize) -> Self {
        Self {
            k,
            current_ts: AtomicU64::new(0),
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    fn shard_for(&self, frame_id: u32) -> usize {
        (frame_id as usize) % REPLACER_SHARDS
    }

    fn record_access(&self, frame_id: u32) {
        let ts = self.current_ts.fetch_add(1, Ordering::Relaxed);
        let mut shard = self.shards[self.shard_for(frame_id)].lock();
        let entry = shard.entry(frame_id).or_insert_with(|| FrameHistory {
            access_history: VecDeque::with_capacity(self.k),
            is_evictable: false,
        });
        if entry.access_history.len() >= self.k {
            entry.access_history.pop_front();
        }
        entry.access_history.push_back(ts);
    }

    fn set_evictable(&self, frame_id: u32, evictable: bool) {
        let mut shard = self.shards[self.shard_for(frame_id)].lock();
        if let Some(entry) = shard.get_mut(&frame_id) {
            entry.is_evictable = evictable;
        }
    }

    /// Find the best eviction candidate across all shards.
    /// Locks one shard at a time (never holds two shard locks simultaneously).
    ///
    /// The scan-then-remove sequence below is racy by construction: two
    /// concurrent callers (e.g. `BufferPool::prefetch_pages`'s parallel rayon
    /// fetches) can each independently scan the shards and converge on the
    /// same `best_frame`, since nothing is locked for the full scan. The
    /// `remove()` call is what arbitrates the race — only one caller's
    /// `remove()` actually finds-and-deletes the entry; the other's is a
    /// silent no-op on an already-missing key. We must check that outcome:
    /// a caller whose removal lost the race must NOT return that frame_id as
    /// if it owned it, or two callers end up believing they exclusively own
    /// the same frame and concurrently overwrite its buffer with two
    /// different pages' disk content — corrupting whichever page's bytes
    /// happen to land second (this was reproduced empirically: it silently
    /// corrupted an on-disk page-chain "next" pointer into a self-loop,
    /// hanging any table scan holding the buffer pool under eviction
    /// pressure). Retry against a fresh scan when the race is lost.
    fn evict_where<F: Fn(u32) -> bool>(&self, claimable: F) -> Option<u32> {
        loop {
            let current_ts = self.current_ts.load(Ordering::Relaxed);

            let mut best_frame: Option<u32> = None;
            let mut best_k_dist: u64 = 0;
            let mut best_earliest: u64 = u64::MAX;
            let mut best_has_k = true;
            let mut best_shard: usize = 0;

            for (si, shard_lock) in self.shards.iter().enumerate() {
                let shard = shard_lock.lock();
                for (&frame_id, history) in shard.iter() {
                    if !history.is_evictable || !claimable(frame_id) {
                        continue;
                    }

                    let has_k = history.access_history.len() >= self.k;
                    let k_dist = if has_k {
                        current_ts.saturating_sub(history.access_history[0])
                    } else {
                        u64::MAX
                    };
                    let earliest = history.access_history.front().copied().unwrap_or(0);

                    let is_better = if best_frame.is_none() {
                        true
                    } else {
                        match (best_has_k, has_k) {
                            (true, false) => true,
                            (false, true) => false,
                            (false, false) => earliest < best_earliest,
                            (true, true) => k_dist > best_k_dist,
                        }
                    };

                    if is_better {
                        best_frame = Some(frame_id);
                        best_k_dist = k_dist;
                        best_earliest = earliest;
                        best_has_k = has_k;
                        best_shard = si;
                    }
                }
                // Drop the shard lock before moving to the next shard.
            }

            let frame_id = best_frame?;

            // Re-lock only the winning shard and re-validate before removing.
            // Two things can have happened since the scan observed this
            // frame as evictable: (a) a concurrent evict() already claimed
            // and removed it — `get` finds nothing, or (b) a concurrent
            // `fetch_page` cache hit just re-pinned it via
            // `set_evictable(frame_id, false)` — the entry is still present
            // but no longer evictable. `set_evictable` mutates the entry in
            // place rather than removing it, so a plain `remove()` here
            // would blindly hand out a frame that's actively in use again,
            // racing its content against whatever the new pinner reads.
            // Only claim it if it's present, still evictable, AND the
            // caller's predicate approves. The predicate is how the buffer
            // pool vetoes frames whose pin_count is nonzero: `unpin` sets
            // evictable=true outside the eviction lock, so a stale
            // set_evictable(true) can land AFTER a concurrent pinner
            // re-pinned the frame — the flag alone cannot be trusted.
            let mut shard = self.shards[best_shard].lock();
            let should_claim = matches!(shard.get(&frame_id), Some(entry) if entry.is_evictable)
                && claimable(frame_id);
            if should_claim {
                shard.remove(&frame_id);
                return Some(frame_id);
            }
            drop(shard);
            // Lost the race for this candidate — rescan. The scan itself
            // also applies the predicate, so a still-pinned frame won't be
            // re-selected; if every flagged frame is vetoed the scan
            // returns None (PoolFull), which is the correct semantics.
        }
    }

    fn remove(&self, frame_id: u32) {
        self.shards[self.shard_for(frame_id)]
            .lock()
            .remove(&frame_id);
    }
}

// ============================================================================
// Buffer pool
// ============================================================================

/// The buffer pool manager. Central point for all page access.
/// See `BufferPool::applying_session`.
const SESSION_UNKNOWN: u64 = u64::MAX;

pub struct BufferPool {
    // SAFETY: UnsafeCell allows interior mutability for page frames.
    // Callers coordinate access via pin_count and frame latches (RwLock).
    frames: Vec<UnsafeCell<AlignedPage>>,
    descriptors: Vec<FrameDescriptor>,
    page_table: PageTable,
    replacer: LruKReplacer,
    free_list: Mutex<Vec<u32>>,
    disk: DiskManager,
    /// Optional WAL backend — if set, every page flush writes a WAL record first.
    /// Accepts either a single-file `Wal` or a `SegmentedWal`.
    wal: Option<Box<dyn super::wal::WalBackend>>,
    next_page_id: AtomicU32,
    pool_size: usize,
    /// Performance statistics for monitoring and observability.
    stats: BufferPoolStats,
    /// Tracked set of dirty frame indices for efficient batch flushing.
    dirty_set: Mutex<HashSet<u32>>,
    /// Page IDs dirtied since the last commit-time WAL force
    /// (`wal_force_pending`). Tracks page IDs, not frame IDs, so an entry
    /// stays meaningful across eviction (an evicted page was WAL-logged and
    /// group-synced by the flush path, so the force skips it as clean).
    wal_pending: Mutex<HashSet<u32>>,
    /// Serializes "pin an existing resident page" against "evict a frame to
    /// make room". These are two different operations over two different
    /// data structures (`page_table` and the replacer's per-frame
    /// `is_evictable` state) that both need to agree on which frame a given
    /// page_id currently owns. Without a lock spanning both, a pinner's
    /// `page_table` lookup and an evictor's replacer-side claim can
    /// interleave: the pinner sees page_id still mapped to frame F (correct,
    /// at that instant) and increments F's pin count, while — in the same
    /// window — an evictor (which doesn't consult `page_table` at all, only
    /// `is_evictable`) claims F as free, flushes it, repurposes it for a
    /// different page_id, and removes page_id's `page_table` entry. Both
    /// operations individually "succeed"; the pinner ends up holding a
    /// pinned reference to a frame that's already been overwritten with a
    /// different page's disk content. This was reproduced empirically as a
    /// production hang: a table's on-disk page-chain "next" pointer got
    /// corrupted into pointing at itself, spinning a query at ~100% CPU
    /// forever. See `pin_if_present` and `get_free_frame`.
    eviction_lock: Mutex<()>,
    /// Sentinel for "the applying transaction did not name a session", so
    /// every page dirtied in its window is attributed to it.
    ///
    /// `u64::MAX` rather than 0: 0 is a real session id (the embedded/default
    /// one), and using it here would make an embedded write indistinguishable
    /// from an unknown owner.
    applying_session: AtomicU64,
    /// The transaction currently applying its buffered writes, or 0 for none.
    ///
    /// Commit application is serialized (see `BufferedDiskEngine::commit_txn`),
    /// so at most one transaction is in this window at a time and a page
    /// dirtied while it is set belongs to that transaction. That is what makes
    /// full-page undo sound: nobody else is mutating those pages, so a
    /// before-image restores exactly the pre-transaction state and cannot
    /// revert someone else's committed work.
    applying_txn: AtomicU64,
    /// Pages dirtied by the transaction named in `applying_txn`. Cleared when
    /// that transaction ends. A page NOT in here is committed state, and is
    /// still logged at txn 0 — recovery redoes those unconditionally.
    txn_dirty: Mutex<HashSet<u32>>,
    /// Pages whose before-image has already been logged for the current
    /// transaction. One undo record per page per transaction is enough: the
    /// first one captured the pre-transaction image, and every later flush of
    /// the same page is still that same transaction's uncommitted work.
    undo_logged: Mutex<HashSet<u32>>,
}

#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("buffer pool full — all frames are pinned")]
    PoolFull,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("page checksum mismatch for page {0}")]
    ChecksumMismatch(u32),
}

impl BufferPool {
    /// Create a new buffer pool with the given number of frames.
    pub fn new(
        disk: DiskManager,
        wal: Option<Box<dyn super::wal::WalBackend>>,
        pool_size: usize,
        initial_pages: u32,
    ) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut descriptors = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            frames.push(UnsafeCell::new(AlignedPage::new()));
            descriptors.push(FrameDescriptor::new());
            free_list.push(i as u32);
        }

        Self {
            frames,
            descriptors,
            page_table: PageTable::new(),
            replacer: LruKReplacer::new(2),
            free_list: Mutex::new(free_list),
            disk,
            wal,
            next_page_id: AtomicU32::new(initial_pages),
            pool_size,
            stats: BufferPoolStats::new(),
            dirty_set: Mutex::new(HashSet::new()),
            wal_pending: Mutex::new(HashSet::new()),
            eviction_lock: Mutex::new(()),
            applying_txn: AtomicU64::new(0),
            applying_session: AtomicU64::new(SESSION_UNKNOWN),
            txn_dirty: Mutex::new(HashSet::new()),
            undo_logged: Mutex::new(HashSet::new()),
        }
    }

    /// Open the commit-application window for `txn_id`, owned by `session`.
    ///
    /// Pages dirtied by that session until [`BufferPool::end_page_txn`] are
    /// attributed to this transaction, and any flush of one of them logs a
    /// before-image first. `txn_id` must be non-zero: 0 is the encoding for
    /// "committed state, redo unconditionally".
    ///
    /// The session matters because the apply lock does not cover everything.
    /// It serializes commits against each other, but an AUTOCOMMIT statement
    /// takes `!is_in_txn()` in `buffered_engine` and goes straight to the inner
    /// engine, so another connection can dirty a page inside this window.
    /// Attributing that page here would hand another session's acknowledged
    /// write to this transaction, and a crash before this transaction
    /// committed would then undo it.
    pub fn begin_page_txn(&self, txn_id: u64, session: Option<u64>) {
        debug_assert!(txn_id != 0, "txn 0 means 'no transaction' to recovery");
        self.applying_session
            .store(session.unwrap_or(SESSION_UNKNOWN), Ordering::Release);
        self.applying_txn.store(txn_id, Ordering::Release);
        self.txn_dirty.lock().clear();
        self.undo_logged.lock().clear();
    }

    /// Close the commit-application window, logging COMMIT or ABORT.
    ///
    /// The caller must sync the WAL after this before acknowledging the
    /// commit: a COMMIT record that is not durable is one recovery will not
    /// see, and it will undo the transaction the client was told succeeded.
    /// `commit_body` is the optional S63 enlistment payload carried inside the
    /// COMMIT record (`None` for plain SQL transactions). Returns the LSN of
    /// the control record, or 0 when no WAL is configured.
    pub fn end_page_txn(
        &self,
        txn_id: u64,
        committed: bool,
        commit_body: Option<&[u8]>,
    ) -> Result<u64, BufferError> {
        let result = match self.wal {
            Some(ref wal) if committed => wal.log_commit(txn_id, commit_body),
            Some(ref wal) => wal.log_abort(txn_id),
            None => Ok(0),
        };
        self.applying_txn.store(0, Ordering::Release);
        self.txn_dirty.lock().clear();
        self.undo_logged.lock().clear();
        result.map_err(BufferError::Io)
    }

    /// Close the window without logging anything, for a transaction that
    /// dirtied no page. There is nothing for recovery to redo or undo, so a
    /// record would be pure log noise on the read-only path.
    pub fn close_page_txn_silently(&self) {
        self.applying_txn.store(0, Ordering::Release);
        self.txn_dirty.lock().clear();
        self.undo_logged.lock().clear();
    }

    /// Whether the applying transaction has dirtied any page.
    ///
    /// Distinct from `wal_force_needed`: a page this transaction dirtied and
    /// the pool then STOLE is no longer pending (eviction logged it itself),
    /// but it is still uncommitted work on disk that needs a durable COMMIT
    /// record, or recovery will undo a transaction the client was told
    /// succeeded.
    pub fn page_txn_touched(&self) -> bool {
        !self.txn_dirty.lock().is_empty()
    }

    /// Note that `page_id` was dirtied, attributing it to the applying
    /// transaction unless it provably belongs to a different session.
    ///
    /// Fail-safe by construction: attribution is skipped ONLY when both this
    /// write and the open window name a session and the two differ. An unknown
    /// session attributes, because the cost of guessing wrong in that
    /// direction is an unnecessary undo record, while guessing wrong the other
    /// way is an uncommitted page with no way back — the bug this whole path
    /// exists to remove.
    fn note_dirty_page(&self, page_id: u32) {
        if self.applying_txn.load(Ordering::Acquire) == 0 {
            return;
        }
        let owner = self.applying_session.load(Ordering::Acquire);
        if owner != SESSION_UNKNOWN
            && let Some(writer) = super::current_storage_session()
            && writer != owner
        {
            return;
        }
        self.txn_dirty.lock().insert(page_id);
    }

    /// WAL-log a page image that is about to be written to disk (or forced),
    /// attributing it to the transaction that dirtied it and, when that
    /// transaction is still uncommitted, logging the page's before-image first.
    ///
    /// This is the single place the page WAL learns about transactions. Every
    /// flush site funnels through it; before it existed, all six logged at
    /// txn 0, so no page image was attributable and recovery had no choice but
    /// to replay uncommitted work.
    fn log_flush(
        &self,
        wal: &dyn super::wal::WalBackend,
        page_id: u32,
        data: &PageBuf,
    ) -> std::io::Result<u64> {
        let txn = self.applying_txn.load(Ordering::Acquire);
        let uncommitted = txn != 0 && self.txn_dirty.lock().contains(&page_id);

        if uncommitted {
            // One before-image per page per transaction — and taken from the
            // DATA FILE, not from the frame, because the frame already holds
            // the uncommitted bytes. `make_durable` WAL-logs every dirty page
            // at commit, so the file lags the WAL but never leads it: the disk
            // image is either the last committed state, or older than it and
            // superseded by a committed record that redo applies afterwards.
            let needs_undo = self.undo_logged.lock().insert(page_id);
            if needs_undo {
                let mut before = Box::new([0u8; PAGE_SIZE]);
                match self.disk.read_page(page_id, before.as_mut()) {
                    Ok(()) => {
                        wal.log_page_undo(txn, page_id, before.as_ref())?;
                    }
                    Err(_) => {
                        // The page does not exist in the file yet, so this
                        // transaction created it. There is no before-image to
                        // restore; recovery drops the page instead, which is
                        // recorded by an undo record with a zeroed image.
                        let blank = Box::new([0u8; PAGE_SIZE]);
                        wal.log_page_undo(txn, page_id, blank.as_ref())?;
                    }
                }
            }
        }

        wal.log_page_write(txn, page_id, data)
    }

    /// If `page_id` is currently resident, pin it and return its frame.
    /// Returns `None` if it isn't resident (a genuine cache miss).
    ///
    /// Holds `eviction_lock` for the lookup-then-pin sequence so this can't
    /// interleave with `get_free_frame`'s evict-then-repurpose sequence for
    /// the same frame — see the comment on `eviction_lock` for why that gap
    /// matters.
    fn pin_if_present(&self, page_id: u32) -> Option<u32> {
        let _guard = self.eviction_lock.lock();
        let frame_id = self.page_table.lookup(page_id)?;
        let desc = &self.descriptors[frame_id as usize];
        desc.pin_count.fetch_add(1, Ordering::AcqRel);
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);
        Some(frame_id)
    }

    /// Fetch a page into the buffer pool and pin it. Returns the frame ID.
    /// The caller must call `unpin` when done.
    ///
    /// Loops rather than recursing on the (rare) lost-race retry paths — see
    /// the comment above the final race check below.
    pub fn fetch_page(&self, page_id: u32) -> Result<u32, BufferError> {
        loop {
            // Check if already in pool
            if let Some(frame_id) = self.pin_if_present(page_id) {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(frame_id);
            }

            // Cache miss — must load from disk
            self.stats.misses.fetch_add(1, Ordering::Relaxed);

            // Get a free frame
            let frame_id = self.get_free_frame()?;

            // Double-check: another thread may have loaded this page while we
            // were allocating a frame.
            if let Some(existing_frame) = self.pin_if_present(page_id) {
                self.free_list.lock().push(frame_id);
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(existing_frame);
            }

            // Read from disk. The frame is exclusively ours — it is not in the
            // page table and not in the replacer, so nobody can pin it — but a
            // frame-index scan (`flush_all`, `flush_dirty_batch`) walks frames
            // without consulting the page table, so hold the write latch while
            // the bytes are in flux.
            {
                let _latch = self.frame_latch(frame_id).write();
                let buf = self.frame_data_mut(frame_id);
                self.disk.read_page(page_id, buf)?;

                // Verify checksum (skip for freshly allocated pages with all zeros)
                if (page::get_page_type(buf) != page::PAGE_TYPE_FREE
                    || page::read_u32(buf, page::HEADER_CHECKSUM) != 0)
                    && !page::verify_checksum(buf)
                {
                    return Err(BufferError::ChecksumMismatch(page_id));
                }
            }

            // Second race check: the earlier double-check only guarded the gap
            // before this disk read. Two threads can both miss on the same
            // page_id, both pass that check (neither has registered yet), and
            // both independently read the same page from disk into two
            // different frames. Without re-checking here, both would then
            // insert into page_table and whichever insert lands last would
            // silently win — orphaning the other thread's frame: it would sit
            // pinned forever (never reachable to unpin via a page_table lookup,
            // since callers look pages up by page_id) while still consuming a
            // pool slot, permanently shrinking the effective pool size by one
            // frame per lost race. This check doesn't need `eviction_lock` (our
            // own `frame_id` is exclusively ours and not yet visible to any
            // evictor — it has no page_table or replacer entry until we
            // register it below) — just the plain partition lock `insert()`
            // uses, so only the winner registers.
            // Set up the descriptor BEFORE publishing the page_table mapping
            // (same order new_page uses). The moment the insert below lands,
            // a concurrent pin_if_present can find this frame and fetch_add
            // its pin_count — a store(1) issued after that would erase that
            // caller's pin, letting the frame be evicted and repurposed while
            // they are still reading it.
            let desc = &self.descriptors[frame_id as usize];
            desc.page_id.store(page_id, Ordering::Release);
            desc.pin_count.store(1, Ordering::Release);
            desc.is_dirty.store(false, Ordering::Release);

            let part_idx = self.page_table.partition_for(page_id);
            let mut part = self.page_table.partitions[part_idx].lock();
            if part.get(&page_id).is_some() {
                // Lost the second race — someone else's load already won.
                // The frame was never published, so the descriptor is still
                // exclusively ours: reset it before freeing.
                drop(part);
                desc.pin_count.store(0, Ordering::Release);
                desc.page_id.store(INVALID_PAGE_ID, Ordering::Release);
                self.free_list.lock().push(frame_id);
                // Re-resolve via pin_if_present rather than trusting the
                // frame_id observed above — in the (astronomically rare)
                // case that it was evicted again in between, retry the
                // whole fetch from the top rather than fall back to a
                // frame_id we never actually pinned.
                match self.pin_if_present(page_id) {
                    Some(existing_frame) => {
                        self.stats.hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(existing_frame);
                    }
                    None => continue,
                }
            }
            part.insert(page_id, frame_id);
            drop(part);

            // Track in replacer
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);

            return Ok(frame_id);
        }
    }

    /// Prefetch pages into the buffer pool for sequential scan read-ahead.
    ///
    /// Issues all disk reads in parallel using Rayon worker threads, then
    /// immediately unpins each page so it stays in the LRU cache for the
    /// imminent sequential read without blocking eviction.
    ///
    /// Pages already in the pool are skipped (cache hit — no I/O needed).
    /// Individual page errors are silently ignored (best-effort prefetch).
    pub fn prefetch_pages(&self, page_ids: &[u32]) {
        // Filter to only uncached pages before spawning threads.
        let uncached: Vec<u32> = page_ids
            .iter()
            .copied()
            .filter(|&pid| self.page_table.lookup(pid).is_none())
            .collect();
        if uncached.is_empty() {
            return;
        }
        // BufferPool: Sync (unsafe impl above) — safe to share &self across
        // rayon::scope threads. scope() blocks until all spawned tasks finish,
        // so self is guaranteed to be alive for the entire parallel section.
        rayon::scope(|s| {
            for page_id in uncached {
                s.spawn(move |_| {
                    // Re-check: another thread may have loaded this page while
                    // we were building the uncached list.
                    if self.page_table.lookup(page_id).is_some() {
                        return;
                    }
                    if let Ok(frame_id) = self.fetch_page(page_id) {
                        self.unpin(frame_id);
                    }
                });
            }
        });
    }

    /// Allocate a new page on disk and fetch it into the pool.
    pub fn new_page(&self) -> Result<(u32, u32), BufferError> {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        self.disk.extend_to_page(page_id)?;

        let frame_id = self.get_free_frame()?;

        // Initialize blank page. Latched for the same reason as the disk read
        // in `fetch_page`: frame-index scans do not consult the page table.
        {
            let _latch = self.frame_latch(frame_id).write();
            self.frame_data_mut(frame_id).fill(0);
        }

        let desc = &self.descriptors[frame_id as usize];
        desc.page_id.store(page_id, Ordering::Release);
        desc.pin_count.store(1, Ordering::Release);
        desc.is_dirty.store(true, Ordering::Release);
        self.dirty_set.lock().insert(frame_id);
        self.note_dirty_page(page_id);
        if self.wal.is_some() {
            self.wal_pending.lock().insert(page_id);
        }

        self.page_table.insert(page_id, frame_id);
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        Ok((page_id, frame_id))
    }

    /// Get a read reference to the page data in a frame.
    ///
    /// SAFETY CONTRACT: the caller must hold the frame's read or write latch.
    /// Prefer [`BufferPool::read_guard`], which pins and latches together; this
    /// raw accessor exists for the guards themselves, for the pool's own
    /// internals, and for tests.
    pub fn frame_data(&self, frame_id: u32) -> &PageBuf {
        // SAFETY: Read access is safe because callers coordinate via pin_count
        // and frame latches (RwLock). The UnsafeCell provides interior mutability;
        // concurrent reads are valid when no writer holds the latch.
        unsafe { &(*self.frames[frame_id as usize].get()).data }
    }

    /// Get a mutable reference to the page data in a frame.
    ///
    /// SAFETY CONTRACT: the caller must hold the frame's WRITE latch. Being
    /// the sole pinner is not sufficient and never was — the flusher and the
    /// evictor read frames they have not pinned. Prefer
    /// [`BufferPool::write_guard`].
    #[allow(clippy::mut_from_ref)]
    pub fn frame_data_mut(&self, frame_id: u32) -> &mut PageBuf {
        // SAFETY: Mutable access is safe because callers coordinate via pin_count
        // and frame latches (RwLock). Only one writer can hold the write latch at
        // a time, and readers must hold the read latch. The UnsafeCell allows
        // obtaining a mutable reference through a shared reference.
        unsafe { &mut (*self.frames[frame_id as usize].get()).data }
    }

    /// Get the read-write latch for a frame.
    pub fn frame_latch(&self, frame_id: u32) -> &RwLock<()> {
        &self.descriptors[frame_id as usize].latch
    }

    /// Pin `page_id` and take its read latch. The page bytes cannot change
    /// while the returned guard lives.
    ///
    /// Read [`FrameDescriptor::latch`] before adding a call site: no other
    /// lock may be acquired, and no other page fetched, while the guard is
    /// alive.
    pub fn read_guard(&self, page_id: u32) -> Result<PageReadGuard<'_>, BufferError> {
        let frame_id = self.fetch_page(page_id)?;
        Ok(self.read_guard_for_frame(frame_id))
    }

    /// Pin `page_id` and take its write latch. Exclusive against every other
    /// reader and writer of these bytes, including the flusher and the evictor.
    pub fn write_guard(&self, page_id: u32) -> Result<PageWriteGuard<'_>, BufferError> {
        let frame_id = self.fetch_page(page_id)?;
        Ok(self.write_guard_for_frame(frame_id))
    }

    /// Take the read latch on a frame the caller has ALREADY pinned. The guard
    /// takes over that pin and releases it on drop.
    pub fn read_guard_for_frame(&self, frame_id: u32) -> PageReadGuard<'_> {
        let latch = self.frame_latch(frame_id).read();
        PageReadGuard {
            pool: self,
            frame_id,
            latch: Some(latch),
        }
    }

    /// Take the write latch on a frame the caller has ALREADY pinned. The guard
    /// takes over that pin and releases it on drop.
    pub fn write_guard_for_frame(&self, frame_id: u32) -> PageWriteGuard<'_> {
        let latch = self.frame_latch(frame_id).write();
        PageWriteGuard {
            pool: self,
            frame_id,
            latch: Some(latch),
            dirty: false,
        }
    }

    /// Allocate a new page and return it write-latched. Already marked dirty
    /// by `new_page`, so the guard does not need `set_dirty`.
    pub fn new_page_guard(&self) -> Result<(u32, PageWriteGuard<'_>), BufferError> {
        let (page_id, frame_id) = self.new_page()?;
        Ok((page_id, self.write_guard_for_frame(frame_id)))
    }

    /// Allocate a new page, leaving it unlatched and unpinned. For callers that
    /// need the page ID now and will write the page later under its own latch
    /// (so they never hold a latch across an allocation — rule (A)).
    pub fn new_page_id(&self) -> Result<u32, BufferError> {
        let (page_id, frame_id) = self.new_page()?;
        self.unpin(frame_id);
        Ok(page_id)
    }

    /// Mark a frame as dirty (modified).
    pub fn mark_dirty(&self, frame_id: u32) {
        let desc = &self.descriptors[frame_id as usize];
        let was_dirty = desc.is_dirty.swap(true, Ordering::AcqRel);
        if !was_dirty {
            self.stats.dirty_pages.fetch_add(1, Ordering::Relaxed);
        }
        self.dirty_set.lock().insert(frame_id);
        let page_id = desc.page_id.load(Ordering::Acquire);
        if page_id != INVALID_PAGE_ID {
            // Attribution has to happen whether or not a WAL is configured:
            // `wal_pending` is a WAL concern, but "which transaction dirtied
            // this page" is not, and gating it on the WAL would silently
            // disable undo the moment someone ran without one.
            self.note_dirty_page(page_id);
            if self.wal.is_some() {
                self.wal_pending.lock().insert(page_id);
            }
        }
    }

    /// Commit-time WAL force: log a page image for every page dirtied since
    /// the last force, then group-sync the WAL up to the highest LSN written.
    ///
    /// This is the durability point for committed work — the data pages
    /// themselves still flush lazily (background flusher / checkpoint), but
    /// once this returns, crash recovery replays every logged image, so the
    /// loss window for acked commits is zero rather than the checkpoint
    /// interval.
    ///
    /// Pages that were flushed or evicted since being dirtied are skipped:
    /// both paths WAL-log the image themselves, and the flush path group-syncs
    /// (evictions) or writes + syncs the data page (batch flusher).
    ///
    /// No-op when no WAL is configured. On error the drained page set is
    /// merged back so a retry (or the next commit) re-covers these pages.
    pub fn wal_force_pending(&self) -> Result<(), BufferError> {
        let max_lsn = self.wal_log_pending()?;
        if max_lsn > 0
            && let Some(ref wal) = self.wal
            && let Err(e) = wal.sync_up_to(max_lsn)
        {
            return Err(BufferError::Io(e));
        }
        Ok(())
    }

    /// Log every pending page image WITHOUT syncing, returning the highest LSN
    /// written (0 if nothing was pending).
    ///
    /// Split out of `wal_force_pending` for the commit path, which has to get
    /// the COMMIT record appended after these page images and then cover both
    /// with ONE sync. Syncing here and again for the COMMIT record would put a
    /// second fsync on every commit — on macOS that is a real 4 ms, doubling
    /// commit latency to buy nothing.
    pub fn wal_log_pending(&self) -> Result<u64, BufferError> {
        let Some(ref wal) = self.wal else {
            return Ok(0);
        };
        let pending: Vec<u32> = {
            let mut set = self.wal_pending.lock();
            if set.is_empty() {
                return Ok(0);
            }
            set.drain().collect()
        };

        let mut max_lsn = 0u64;
        let restore_on_err = |pool: &Self, remaining: &[u32]| {
            let mut set = pool.wal_pending.lock();
            for &p in remaining {
                set.insert(p);
            }
        };

        for (i, &page_id) in pending.iter().enumerate() {
            // Pin via the eviction-safe path so the frame can't be repurposed
            // for a different page between lookup and image copy.
            let Some(frame_id) = self.pin_if_present(page_id) else {
                // Not resident: it was evicted, and eviction WAL-logs +
                // group-syncs the image first. Nothing left to force.
                continue;
            };
            let desc = &self.descriptors[frame_id as usize];
            if desc.is_dirty.load(Ordering::Acquire) {
                // Write-latch the frame: this both stops a concurrent writer
                // tearing the image as it streams into the WAL record AND
                // covers `set_page_lsn` below, which mutates the page header.
                // A read latch would have let two forces stamp the LSN
                // concurrently.
                let _latch = self.frame_latch(frame_id).write();
                let data = self.frame_data_mut(frame_id);
                match self.log_flush(wal.as_ref(), page_id, data) {
                    Ok(lsn) => {
                        page::set_page_lsn(data, lsn);
                        max_lsn = lsn;
                    }
                    Err(e) => {
                        drop(_latch);
                        self.unpin(frame_id);
                        restore_on_err(self, &pending[i..]);
                        return Err(BufferError::Io(e));
                    }
                }
            }
            self.unpin(frame_id);
        }

        // Deliberately NOT synced here — the caller decides, because the commit
        // path needs the COMMIT record appended first so one sync covers both.
        // `wal_force_pending` syncs immediately for every other caller.
        let _ = &restore_on_err;
        Ok(max_lsn)
    }

    /// Whether any pages are awaiting a commit-time WAL force. Cheap check so
    /// callers can skip `wal_force_pending` entirely on read-only statements.
    pub fn wal_force_needed(&self) -> bool {
        self.wal.is_some() && !self.wal_pending.lock().is_empty()
    }

    /// Unpin a frame (decrement pin count).
    pub fn unpin(&self, frame_id: u32) {
        let desc = &self.descriptors[frame_id as usize];
        let old = desc.pin_count.load(Ordering::Acquire);
        if old == 0 {
            return; // Already unpinned — avoid underflow.
        }
        let prev = desc.pin_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.replacer.set_evictable(frame_id, true);
        }
    }

    /// Flush a specific page to disk (WAL record written first if WAL is enabled).
    ///
    /// Uses group commit for the WAL sync so concurrent flushers can piggyback
    /// on a single fsync. The data page sync is skipped because the WAL already
    /// holds the full page image — crash recovery will replay it if needed.
    pub fn flush_page(&self, page_id: u32) -> Result<(), BufferError> {
        // Pin via the eviction-safe path: a bare page_table lookup can hand
        // back a frame that is repurposed for a different page before the
        // bytes are read, which would write one page's contents to another
        // page's offset.
        let Some(frame_id) = self.pin_if_present(page_id) else {
            return Ok(());
        };
        let result = self.flush_pinned_frame(frame_id, page_id, true);
        self.unpin(frame_id);
        result
    }

    /// Write one already-pinned frame out, WAL record first. The frame is
    /// write-latched for the whole sequence — `set_page_lsn` and
    /// `write_checksum` mutate the page, and the disk write must see the same
    /// bytes the WAL record captured.
    fn flush_pinned_frame(
        &self,
        frame_id: u32,
        page_id: u32,
        group_sync: bool,
    ) -> Result<(), BufferError> {
        let desc = &self.descriptors[frame_id as usize];
        {
            let _latch = self.frame_latch(frame_id).write();
            if !desc.is_dirty.load(Ordering::Acquire) {
                return Ok(());
            }
            let data = self.frame_data_mut(frame_id);
            // WAL protocol: sync WAL before writing data pages
            if let Some(ref wal) = self.wal {
                let lsn = self
                    .log_flush(wal.as_ref(), page_id, data)
                    .map_err(BufferError::Io)?;
                page::set_page_lsn(data, lsn);
                if group_sync {
                    wal.group_sync();
                }
            }
            // Checksum must be computed AFTER LSN is set so on-disk page is valid
            page::write_checksum(data);
            self.disk.write_page(page_id, data)?;
            // Data page sync skipped — WAL guarantees durability.
            // The background flusher's flush_dirty_batch() will do a
            // coalesced disk.sync() covering this and other written pages.
            desc.is_dirty.store(false, Ordering::Release);
            self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
        }
        self.dirty_set.lock().remove(&frame_id);
        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&self) -> Result<(), BufferError> {
        // Flush WAL first (force all WAL records to disk)
        if let Some(ref wal) = self.wal {
            wal.sync().map_err(BufferError::Io)?;
        }
        for i in 0..self.pool_size {
            let desc = &self.descriptors[i];
            let page_id = desc.page_id.load(Ordering::Acquire);
            if page_id != INVALID_PAGE_ID && desc.is_dirty.load(Ordering::Acquire) {
                // Write-latched: the page is mutated (LSN, checksum) and its
                // bytes must not shift under the disk write. Re-checked inside
                // the latch because a concurrent flusher may have won the race.
                let _latch = self.frame_latch(i as u32).write();
                if desc.page_id.load(Ordering::Acquire) != page_id
                    || !desc.is_dirty.load(Ordering::Acquire)
                {
                    continue;
                }
                let data = self.frame_data_mut(i as u32);
                if let Some(ref wal) = self.wal {
                    let lsn = self
                        .log_flush(wal.as_ref(), page_id, data)
                        .map_err(BufferError::Io)?;
                    page::set_page_lsn(data, lsn);
                }
                // Checksum must be computed AFTER LSN is set so on-disk page is valid
                page::write_checksum(data);
                self.disk.write_page(page_id, data)?;
                desc.is_dirty.store(false, Ordering::Release);
                self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
        }
        // Clear the dirty set — all pages have been flushed
        self.dirty_set.lock().clear();
        // Write-ahead: the loop above MINTED a record per page and stamped its
        // LSN onto the page, so the pre-loop sync did not cover them. Without
        // this, `disk.sync()` below makes pages durable at LSNs whose records
        // are still in a process-local BufWriter, and a crash there leaves the
        // data file ahead of the WAL. Recovery then reissues those LSNs (it
        // takes its floor from the WAL) and the NEXT recovery discards the new
        // records as stale — silently losing acknowledged commits.
        if let Some(ref wal) = self.wal {
            wal.sync().map_err(BufferError::Io)?;
        }
        self.disk.sync()?;
        Ok(())
    }

    /// Collect dirty pages for async flushing, handling WAL and checksum inline.
    ///
    /// Returns `Vec<(page_id, page_bytes)>` and marks each collected page clean.
    /// The caller is responsible for writing the returned bytes to disk via
    /// an async I/O backend (e.g. `AsyncDiskOps`).
    ///
    /// Uses the dirty_set to visit only dirty frames (O(dirty) not O(pool_size))
    /// and copies into `Box<PageBuf>` for known-size page data.
    pub fn collect_dirty_for_async_flush(&self) -> Result<Vec<(u32, Box<PageBuf>)>, BufferError> {
        if let Some(ref wal) = self.wal {
            wal.sync().map_err(BufferError::Io)?;
        }
        // Snapshot the dirty set and clear it — only visit dirty frames
        let dirty_frames: Vec<u32> = {
            let mut set = self.dirty_set.lock();
            let frames: Vec<u32> = set.drain().collect();
            frames
        };
        let mut dirty = Vec::with_capacity(dirty_frames.len());
        for frame_id in dirty_frames {
            let idx = frame_id as usize;
            let desc = &self.descriptors[idx];
            let page_id = desc.page_id.load(Ordering::Acquire);
            if page_id != INVALID_PAGE_ID && desc.is_dirty.load(Ordering::Acquire) {
                // Write-latched: stamps the LSN and checksum, then copies the
                // image out. Without the latch the copy tears against a
                // concurrent writer and a torn page reaches the data file.
                let _latch = self.frame_latch(frame_id).write();
                if desc.page_id.load(Ordering::Acquire) != page_id
                    || !desc.is_dirty.load(Ordering::Acquire)
                {
                    continue;
                }
                let data = self.frame_data_mut(frame_id);
                if let Some(ref wal) = self.wal {
                    let lsn = self
                        .log_flush(wal.as_ref(), page_id, data)
                        .map_err(BufferError::Io)?;
                    page::set_page_lsn(data, lsn);
                }
                page::write_checksum(data);
                dirty.push((page_id, Box::new(*data)));
                desc.is_dirty.store(false, Ordering::Release);
                self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
        }
        // Write-ahead: the loop minted a record per collected page. The caller
        // writes these bytes and fsyncs the data file, so the records must be
        // durable before we hand them over. See `flush_all` for what a crash in
        // that window costs.
        if !dirty.is_empty()
            && let Some(ref wal) = self.wal
        {
            wal.sync().map_err(BufferError::Io)?;
        }
        Ok(dirty)
    }

    /// Get the next page ID that would be allocated.
    pub fn next_page_id(&self) -> u32 {
        self.next_page_id.load(Ordering::Acquire)
    }

    /// Get a reference to the buffer pool statistics.
    pub fn stats(&self) -> &BufferPoolStats {
        &self.stats
    }

    /// Get the pool size (number of frames).
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Get the number of dirty pages currently tracked.
    pub fn dirty_page_count(&self) -> usize {
        self.dirty_set.lock().len()
    }

    /// Flush up to `max_pages` dirty pages from the tracked set.
    /// Returns the number actually flushed (WAL-logged and written).
    pub fn flush_dirty_batch(&self, max_pages: usize) -> usize {
        let to_flush: Vec<u32> = {
            let mut set = self.dirty_set.lock();
            let batch: Vec<u32> = set.iter().copied().take(max_pages).collect();
            for &id in &batch {
                set.remove(&id);
            }
            batch
        };

        // WAL protocol: sync WAL before writing data pages to ensure
        // recoverability if we crash mid-flush.
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.sync()
        {
            tracing::error!("WAL sync failed before data page flush: {e}");
            // Nothing was written, so the frames must go back to the tracked
            // set — leaving them dirty-but-untracked hid them from every
            // dirty_set-driven flusher until a full flush_all.
            let mut set = self.dirty_set.lock();
            for &id in &to_flush {
                set.insert(id);
            }
            return 0; // Do NOT write data pages if WAL is not durable
        }

        let mut flushed = 0;
        for frame_id in &to_flush {
            let idx = *frame_id as usize;
            let desc = &self.descriptors[idx];
            let page_id = desc.page_id.load(Ordering::Acquire);
            if page_id != INVALID_PAGE_ID && desc.is_dirty.load(Ordering::Acquire) {
                // Write-latched — see `flush_all` for why.
                let _latch = self.frame_latch(*frame_id).write();
                if desc.page_id.load(Ordering::Acquire) == page_id
                    && desc.is_dirty.load(Ordering::Acquire)
                {
                    let data = self.frame_data_mut(*frame_id);
                    // WAL first, no exceptions — every other flush site in
                    // this file propagates this error. A page whose current
                    // image has no WAL record must not reach the data file:
                    // recovery's wal_lsn > disk_lsn test would accept those
                    // unlogged bytes forever.
                    let logged = match self.wal.as_ref() {
                        Some(wal) => match self.log_flush(wal.as_ref(), page_id, data) {
                            Ok(lsn) => {
                                page::set_page_lsn(data, lsn);
                                true
                            }
                            Err(e) => {
                                tracing::error!(
                                    "WAL append failed for page {page_id}; keeping frame dirty: {e}"
                                );
                                false
                            }
                        },
                        None => true,
                    };
                    if !logged {
                        self.dirty_set.lock().insert(*frame_id);
                        continue;
                    }
                    page::write_checksum(data);
                    match self.disk.write_page(page_id, data) {
                        Ok(()) => {
                            desc.is_dirty.store(false, Ordering::Release);
                            self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
                            flushed += 1;
                        }
                        Err(e) => {
                            tracing::error!(
                                "data page write failed for page {page_id}; keeping frame dirty: {e}"
                            );
                            self.dirty_set.lock().insert(*frame_id);
                        }
                    }
                }
            }
        }

        // Write-ahead: records minted above must be durable before the pages
        // stamped with their LSNs are. See `flush_all`.
        if flushed > 0
            && let Some(ref wal) = self.wal
            && let Err(e) = wal.sync()
        {
            // The pages ARE written — reporting zero would pretend they
            // weren't. Log and keep the count; the next flush_all surfaces
            // the WAL fault.
            tracing::error!("WAL sync failed after flushing {flushed} pages: {e}");
        }

        // Sync data pages to stable storage so they survive power failure.
        if flushed > 0
            && let Err(e) = self.disk.sync()
        {
            tracing::error!("disk sync failed after flushing {flushed} pages: {e}");
        }

        flushed
    }

    /// Get WAL stats: (bytes_written, syncs). Returns (0, 0) if no WAL is configured.
    pub fn wal_stats(&self) -> (u64, u64) {
        self.wal.as_ref().map_or((0, 0), |w| w.wal_stats())
    }

    /// Seal and archive the WAL segment currently being written. Returns
    /// whether a segment was archived — `false` means there was nothing to do
    /// or this backend has no archive, never that the tail is safe.
    pub fn wal_archive_active(&self) -> Result<bool, BufferError> {
        match self.wal.as_ref() {
            Some(wal) => wal.archive_active().map_err(BufferError::Io),
            None => Ok(false),
        }
    }

    /// Log a COMMIT record to the WAL for the given transaction ID. `body` is
    /// the optional S63 enlistment payload; see [`WalRecord::control_body`].
    pub fn wal_log_commit(&self, txn_id: u64, body: Option<&[u8]>) -> Result<u64, BufferError> {
        match self.wal.as_ref() {
            Some(wal) => wal.log_commit(txn_id, body).map_err(BufferError::Io),
            None => Ok(0),
        }
    }

    /// Log an ABORT record to the WAL for the given transaction ID.
    pub fn wal_log_abort(&self, txn_id: u64) -> Result<u64, BufferError> {
        match self.wal.as_ref() {
            Some(wal) => wal.log_abort(txn_id).map_err(BufferError::Io),
            None => Ok(0),
        }
    }

    /// Reload a set of pages from disk, discarding any dirty in-memory modifications.
    ///
    /// Used by transaction abort to undo page-level writes made during the transaction:
    /// since `flush_all()` was called at BEGIN (ensuring pre-txn state is on disk), reloading
    /// a dirty frame from disk restores the pre-txn page content.
    ///
    /// Pages not currently in the buffer pool are silently skipped (they haven't been
    /// modified in memory, so no reload is needed).
    pub fn reload_pages_from_disk(&self, page_ids: &[u32]) -> Result<(), BufferError> {
        for &page_id in page_ids {
            if let Some(frame_id) = self.page_table.lookup(page_id) {
                let desc = &self.descriptors[frame_id as usize];
                // Only reload if currently dirty (unmodified frames are fine as-is).
                if !desc.is_dirty.load(Ordering::Acquire) {
                    continue;
                }
                // Reload the page from disk, discarding the in-memory dirty
                // state. Write-latched: this replaces every byte of the frame.
                {
                    let _latch = self.frame_latch(frame_id).write();
                    let data = self.frame_data_mut(frame_id);
                    self.disk.read_page(page_id, data)?;
                }
                // Clear dirty tracking.
                let was_dirty = desc.is_dirty.swap(false, Ordering::AcqRel);
                if was_dirty {
                    self.dirty_set.lock().remove(&frame_id);
                    self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    /// Write a checkpoint record to the WAL and return the checkpoint LSN.
    pub fn wal_checkpoint(&self) -> Result<u64, BufferError> {
        match self.wal.as_ref() {
            Some(wal) => wal.log_checkpoint().map_err(BufferError::Io),
            None => Ok(0),
        }
    }

    /// The underlying disk manager (physical backup reads slots through it so
    /// the copy honors the file's compression/encryption layout).
    pub fn disk(&self) -> &DiskManager {
        &self.disk
    }

    /// The next LSN the WAL will assign (`0` when no WAL is configured).
    pub fn wal_current_lsn(&self) -> u64 {
        self.wal.as_ref().map_or(0, |w| w.current_lsn())
    }

    /// Force every WAL record up to and including `lsn` to stable storage.
    pub fn wal_sync_up_to(&self, lsn: u64) -> Result<(), BufferError> {
        match self.wal.as_ref() {
            Some(wal) => wal.sync_up_to(lsn).map_err(BufferError::Io),
            None => Ok(()),
        }
    }

    /// Seal the active WAL segment so everything logged so far is in an
    /// inactive, copyable segment.
    pub fn wal_rotate(&self) -> Result<(), BufferError> {
        match self.wal.as_ref() {
            Some(wal) => wal.rotate().map_err(BufferError::Io),
            None => Ok(()),
        }
    }

    /// Hold WAL segments carrying records at or after `lsn` against
    /// checkpoint truncation (online backup). Returns the owner token to pass
    /// to [`BufferPool::wal_unpin_retention`]; `0` when the backend does not
    /// support pinning.
    pub fn wal_pin_retention(&self, lsn: u64) -> u64 {
        self.wal.as_ref().map_or(0, |w| w.pin_retention(lsn))
    }

    /// Release the WAL retention pin owned by `token` (other live pins stay
    /// held).
    pub fn wal_unpin_retention(&self, token: u64) {
        if let Some(wal) = self.wal.as_ref() {
            wal.unpin_retention(token);
        }
    }

    /// The effective WAL retention floor across all live owner pins (`0`
    /// when none). Test/introspection hook.
    pub fn wal_retention_pin(&self) -> u64 {
        self.wal.as_ref().map_or(0, |w| w.retention_pin())
    }

    /// Truncate WAL segments before the given LSN to reclaim disk space.
    pub fn wal_truncate_before(&self, before_lsn: u64) -> Result<usize, BufferError> {
        match self.wal.as_ref() {
            Some(wal) => wal.truncate_before(before_lsn).map_err(BufferError::Io),
            None => Ok(0),
        }
    }

    /// Prefetch a set of pages into the buffer pool.
    ///
    /// Loads pages that are not already cached without pinning them,
    /// useful for sequential scan read-ahead. Errors on individual
    /// pages are silently ignored (best-effort prefetch).
    ///
    /// Delegates to `prefetch_pages` for parallel I/O.
    pub fn prefetch(&self, page_ids: &[u32]) {
        self.prefetch_pages(page_ids);
    }

    // Internal: get a free frame by popping from free list or evicting.
    fn get_free_frame(&self) -> Result<u32, BufferError> {
        // Try free list
        if let Some(frame_id) = self.free_list.lock().pop() {
            return Ok(frame_id);
        }

        // Held for the rest of this function: see the comment on
        // `eviction_lock` for why eviction (which repurposes a resident
        // frame for a different page) must be mutually exclusive with
        // `pin_if_present` (which pins a resident frame in place).
        let _guard = self.eviction_lock.lock();

        // Evict
        // The predicate vetoes frames still pinned: `unpin`'s
        // set_evictable(true) runs outside `eviction_lock`, so a stale flag
        // can mark a re-pinned frame evictable. We hold `eviction_lock` here
        // and every pin path (pin_if_present) also takes it, so a frame
        // observed unpinned in this check cannot gain a pin until we're done.
        let frame_id = self
            .replacer
            .evict_where(|fid| {
                self.descriptors[fid as usize]
                    .pin_count
                    .load(Ordering::Acquire)
                    == 0
            })
            .ok_or(BufferError::PoolFull)?;
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);

        let desc = &self.descriptors[frame_id as usize];
        let old_page_id = desc.page_id.load(Ordering::Acquire);

        // Flush if dirty (WAL record first, then checksum — same order as
        // flush_page). Write-latched, and the latch is released before
        // `page_table.remove` below so this never holds L6 while taking L5.
        if desc.is_dirty.load(Ordering::Acquire) {
            {
                let _latch = self.frame_latch(frame_id).write();
                let data = self.frame_data_mut(frame_id);
                // WAL protocol: log before flush, set LSN first
                if let Some(ref wal) = self.wal {
                    let lsn = self
                        .log_flush(wal.as_ref(), old_page_id, data)
                        .map_err(BufferError::Io)?;
                    page::set_page_lsn(data, lsn);
                }
                // Checksum must be computed AFTER LSN is set so on-disk page is valid
                page::write_checksum(data);
                self.disk.write_page(old_page_id, data)?;
                desc.is_dirty.store(false, Ordering::Release);
                self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
            self.dirty_set.lock().remove(&frame_id);
        }

        // Remove old mapping
        if old_page_id != INVALID_PAGE_ID {
            self.page_table.remove(old_page_id);
        }

        self.replacer.remove(frame_id);
        Ok(frame_id)
    }
}

// SAFETY: BufferPool uses UnsafeCell for frame data, but all access is
// coordinated through pin_count (AtomicU32) and frame latches (RwLock).
// The UnsafeCell is never accessed without proper synchronization.
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

/// Spawn a background task that periodically flushes dirty pages.
///
/// The flusher wakes every `interval_ms` milliseconds and checks whether the
/// number of tracked dirty pages exceeds `threshold_pct` of the pool size. If
/// so, it flushes up to `batch_size` pages via [`BufferPool::flush_dirty_batch`].
#[cfg(feature = "server")]
pub fn spawn_background_flusher(
    pool: std::sync::Arc<BufferPool>,
    interval_ms: u64,
    threshold_pct: f64,
    batch_size: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            let dirty = pool.dirty_page_count();
            let threshold = (pool.pool_size() as f64 * threshold_pct) as usize;
            if dirty > threshold {
                pool.flush_dirty_batch(batch_size);
            }
        }
    })
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("pool_size", &self.pool_size)
            .field("next_page_id", &self.next_page_id.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pool(pool_size: usize) -> (BufferPool, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let disk = DiskManager::open(&db_path).unwrap();
        let pool = BufferPool::new(disk, None, pool_size, 0);
        (pool, dir)
    }

    #[test]
    fn stats_initial_values() {
        let (pool, _dir) = make_pool(8);
        let (hits, misses, evictions, dirty) = pool.stats().snapshot();
        assert_eq!(hits, 0);
        assert_eq!(misses, 0);
        assert_eq!(evictions, 0);
        assert_eq!(dirty, 0);
        assert_eq!(pool.stats().hit_ratio(), 0.0);
    }

    #[test]
    fn stats_track_miss_on_first_fetch() {
        let (pool, _dir) = make_pool(8);
        let (page_id, frame_id) = pool.new_page().unwrap();
        pool.unpin(frame_id);

        // Evict it out of the pool by filling with other pages
        for _ in 0..9 {
            let (_, fid) = pool.new_page().unwrap();
            pool.unpin(fid);
        }

        // Now fetch the original page — should be a miss
        let _ = pool.fetch_page(page_id).unwrap();
        assert!(pool.stats().misses.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn stats_track_hit_on_cached_fetch() {
        let (pool, _dir) = make_pool(8);
        let (page_id, frame_id) = pool.new_page().unwrap();
        pool.unpin(frame_id);

        // Fetch same page again — should be a hit
        let fid = pool.fetch_page(page_id).unwrap();
        pool.unpin(fid);
        assert!(pool.stats().hits.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn stats_track_dirty_pages() {
        let (pool, _dir) = make_pool(8);
        let (page_id, frame_id) = pool.new_page().unwrap();
        pool.unpin(frame_id);

        // Fetch the page fresh and mark it dirty via our tracked API
        let fid = pool.fetch_page(page_id).unwrap();
        // Clear the descriptor dirty flag first (new_page sets it directly)
        pool.descriptors[fid as usize]
            .is_dirty
            .store(false, Ordering::Release);

        pool.mark_dirty(fid);
        assert_eq!(pool.stats().dirty_pages.load(Ordering::Relaxed), 1);

        // Mark dirty again — should not double-count
        pool.mark_dirty(fid);
        assert_eq!(pool.stats().dirty_pages.load(Ordering::Relaxed), 1);

        pool.unpin(fid);
    }

    #[test]
    fn stats_track_evictions() {
        let (pool, _dir) = make_pool(4);
        // Fill pool with 4 pages
        for _ in 0..4 {
            let (_, fid) = pool.new_page().unwrap();
            pool.unpin(fid);
        }
        // Allocate one more — forces eviction
        let (_, fid) = pool.new_page().unwrap();
        pool.unpin(fid);
        assert!(pool.stats().evictions.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn stats_hit_ratio_calculation() {
        let (pool, _dir) = make_pool(16);
        let (page_id, frame_id) = pool.new_page().unwrap();
        pool.unpin(frame_id);

        // 3 hits
        for _ in 0..3 {
            let fid = pool.fetch_page(page_id).unwrap();
            pool.unpin(fid);
        }

        let hits = pool.stats().hits.load(Ordering::Relaxed);
        let _misses = pool.stats().misses.load(Ordering::Relaxed);
        assert!(hits >= 3);
        let ratio = pool.stats().hit_ratio();
        assert!(ratio > 0.5, "hit ratio should be high: {ratio}");
    }

    #[test]
    fn prefetch_loads_pages_into_pool() {
        let (pool, _dir) = make_pool(16);
        // Create several pages
        let mut page_ids = Vec::new();
        for _ in 0..5 {
            let (pid, fid) = pool.new_page().unwrap();
            pool.unpin(fid);
            page_ids.push(pid);
        }

        // Evict them all by creating more pages than pool size
        // (pool is 16 frames, we already used 5, create 12 more)
        for _ in 0..12 {
            let (_, fid) = pool.new_page().unwrap();
            pool.unpin(fid);
        }

        // Prefetch the original pages
        let misses_before = pool.stats().misses.load(Ordering::Relaxed);
        pool.prefetch(&page_ids);
        let misses_after = pool.stats().misses.load(Ordering::Relaxed);

        // Should have loaded some pages (misses increased)
        assert!(
            misses_after > misses_before,
            "prefetch should cause disk reads"
        );

        // Now fetching them should be hits
        let hits_before = pool.stats().hits.load(Ordering::Relaxed);
        for &pid in &page_ids {
            if let Ok(fid) = pool.fetch_page(pid) {
                pool.unpin(fid);
            }
        }
        let hits_after = pool.stats().hits.load(Ordering::Relaxed);
        assert!(hits_after > hits_before, "prefetched pages should be hits");
    }

    #[test]
    fn pool_size_accessor() {
        let (pool, _dir) = make_pool(32);
        assert_eq!(pool.pool_size(), 32);
    }

    /// Regression test for a data race in `LruKReplacer::evict()`: under
    /// concurrent eviction pressure (pool smaller than the working set),
    /// two threads could each independently scan the shards, converge on the
    /// same "best" frame, and both receive it from `evict()` — because the
    /// old code returned the scanned candidate unconditionally instead of
    /// checking whether *this* caller's removal actually won the race. Both
    /// callers would then believe they exclusively owned the frame and
    /// concurrently overwrite its buffer with two different pages' disk
    /// content, corrupting one of them.
    ///
    /// This was found by reproducing a real production hang: a `DELETE`
    /// against a large table drove `BufferPool::prefetch_pages`'s parallel
    /// rayon fetches, which raced on eviction and corrupted a page's
    /// on-disk "next page" pointer into pointing at itself — the table's
    /// page-chain walk then spun at ~100% CPU forever. Disabling eviction
    /// (a pool big enough to hold the whole table) made the corruption
    /// disappear, isolating the bug to eviction under contention.
    ///
    /// Here we stamp every page with its own ID (far from the page header,
    /// so we're not relying on any higher-layer page-format internals) and
    /// hammer the tiny pool from many threads. If a frame is ever handed to
    /// two callers at once, one thread's stamp will clobber another's and a
    /// reader will observe a stamp that doesn't match the page it fetched.
    #[test]
    fn concurrent_eviction_never_hands_out_the_same_frame_twice() {
        use std::sync::Arc;
        use std::thread;

        const STAMP_OFFSET: usize = PAGE_SIZE - 4;

        // Deliberately tiny relative to the working set so essentially every
        // fetch on a miss forces an eviction.
        let (pool, _dir) = make_pool(8);
        let pool = Arc::new(pool);

        let mut page_ids = Vec::new();
        for _ in 0..64 {
            let (page_id, frame_id) = pool.new_page().unwrap();
            let data = pool.frame_data_mut(frame_id);
            data[STAMP_OFFSET..STAMP_OFFSET + 4].copy_from_slice(&page_id.to_le_bytes());
            pool.mark_dirty(frame_id);
            pool.unpin(frame_id);
            page_ids.push(page_id);
        }

        let mut handles = Vec::new();
        for t in 0..8usize {
            let pool = Arc::clone(&pool);
            let page_ids = page_ids.clone();
            handles.push(thread::spawn(move || {
                for i in 0..500usize {
                    let page_id = page_ids[(t * 37 + i) % page_ids.len()];
                    let frame_id = pool.fetch_page(page_id).unwrap();
                    let data = pool.frame_data(frame_id);
                    let stamp = u32::from_le_bytes(
                        data[STAMP_OFFSET..STAMP_OFFSET + 4].try_into().unwrap(),
                    );
                    assert_eq!(
                        stamp, page_id,
                        "buffer pool handed out a frame for page {page_id} whose content is \
                         stamped {stamp} — two callers were given the same frame concurrently"
                    );
                    pool.unpin(frame_id);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}

#[cfg(test)]
mod dirty_tracking_tests {
    use super::*;

    fn make_pool(pool_size: usize) -> (BufferPool, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let disk = DiskManager::open(&db_path).unwrap();
        let pool = BufferPool::new(disk, None, pool_size, 0);
        (pool, dir)
    }

    #[test]
    fn dirty_set_tracks_inserts() {
        let (pool, _dir) = make_pool(16);
        assert_eq!(pool.dirty_page_count(), 0);
        pool.dirty_set.lock().insert(0);
        pool.dirty_set.lock().insert(5);
        pool.dirty_set.lock().insert(10);
        assert_eq!(pool.dirty_page_count(), 3);
    }

    #[test]
    fn dirty_set_no_duplicates() {
        let (pool, _dir) = make_pool(16);
        pool.dirty_set.lock().insert(3);
        pool.dirty_set.lock().insert(3);
        pool.dirty_set.lock().insert(3);
        assert_eq!(pool.dirty_page_count(), 1);
    }

    /// A WAL backend whose `log_page_write` fails on demand. `sync` keeps
    /// succeeding so the failure being exercised is exactly the WAL APPEND
    /// that must gate the data-page write.
    struct FailWal(std::sync::Arc<std::sync::atomic::AtomicBool>);

    impl super::super::wal::WalBackend for FailWal {
        fn log_page_write(
            &self,
            _txn_id: u64,
            _page_id: u32,
            _page_image: &PageBuf,
        ) -> std::io::Result<u64> {
            if self.0.load(std::sync::atomic::Ordering::Acquire) {
                Err(std::io::Error::other("injected WAL append failure"))
            } else {
                Ok(1)
            }
        }
        fn log_page_undo(
            &self,
            _txn_id: u64,
            _page_id: u32,
            _before_image: &PageBuf,
        ) -> std::io::Result<u64> {
            Ok(0)
        }
        fn sync(&self) -> std::io::Result<()> {
            Ok(())
        }
        fn bump_next_lsn(&self, _min_next: u64) {}
    }

    fn dirty_pages(pool: &BufferPool, n: usize) {
        for _ in 0..n {
            let (_, fid) = pool.new_page().unwrap();
            pool.unpin(fid);
        }
    }

    #[test]
    fn flush_dirty_batch_partial() {
        let (pool, _dir) = make_pool(16);
        dirty_pages(&pool, 10);
        assert_eq!(pool.dirty_page_count(), 10);
        let flushed = pool.flush_dirty_batch(3);
        assert_eq!(flushed, 3);
        assert_eq!(pool.dirty_page_count(), 7);
    }

    #[test]
    fn flush_dirty_batch_empty() {
        let (pool, _dir) = make_pool(16);
        let flushed = pool.flush_dirty_batch(100);
        assert_eq!(flushed, 0);
        assert_eq!(pool.dirty_page_count(), 0);
    }

    #[test]
    fn flush_dirty_batch_all() {
        let (pool, _dir) = make_pool(16);
        dirty_pages(&pool, 5);
        let flushed = pool.flush_dirty_batch(100);
        assert_eq!(flushed, 5);
        assert_eq!(pool.dirty_page_count(), 0);
    }

    /// STO-5: a WAL-append failure must keep the page out of the data file
    /// and the frame in the dirty set — writing it with a stale LSN and no
    /// WAL record is how unlogged bytes reached the data file and were then
    /// accepted by recovery forever.
    #[test]
    fn flush_dirty_batch_skips_and_retries_on_wal_append_failure() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let disk = DiskManager::open(&db_path).unwrap();
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let pool = BufferPool::new(disk, Some(Box::new(FailWal(flag.clone()))), 16, 0);
        dirty_pages(&pool, 3);
        assert_eq!(pool.dirty_page_count(), 3);

        flag.store(true, std::sync::atomic::Ordering::Release);
        let flushed = pool.flush_dirty_batch(100);
        assert_eq!(
            flushed, 0,
            "pages whose WAL record could not be appended must not be counted as flushed"
        );
        assert_eq!(
            pool.dirty_page_count(),
            3,
            "frames must stay dirty-tracked when the WAL append fails, or no \
             dirty_set-driven flusher ever retries them"
        );

        flag.store(false, std::sync::atomic::Ordering::Release);
        let flushed = pool.flush_dirty_batch(100);
        assert_eq!(flushed, 3);
        assert_eq!(pool.dirty_page_count(), 0);
    }

    #[test]
    fn dirty_page_count_accurate() {
        let (pool, _dir) = make_pool(32);
        for i in 0..20u32 {
            pool.dirty_set.lock().insert(i);
        }
        assert_eq!(pool.dirty_page_count(), 20);
        pool.dirty_set.lock().remove(&5);
        pool.dirty_set.lock().remove(&10);
        assert_eq!(pool.dirty_page_count(), 18);
    }

    #[test]
    fn flush_all_clears_dirty_set() {
        let (pool, _dir) = make_pool(16);
        // Create actual pages so flush_all has valid state
        let (_, fid) = pool.new_page().unwrap();
        pool.unpin(fid);
        // new_page sets dirty + inserts into dirty_set
        assert!(pool.dirty_page_count() > 0);
        pool.flush_all().unwrap();
        assert_eq!(pool.dirty_page_count(), 0);
    }

    #[test]
    fn mark_dirty_inserts_into_dirty_set() {
        let (pool, _dir) = make_pool(16);
        let (_, frame_id) = pool.new_page().unwrap();
        // new_page already inserts into dirty_set; clear it to test mark_dirty
        pool.dirty_set.lock().clear();
        assert_eq!(pool.dirty_page_count(), 0);
        pool.mark_dirty(frame_id);
        assert_eq!(pool.dirty_page_count(), 1);
        pool.unpin(frame_id);
    }

    #[test]
    fn new_page_inserts_into_dirty_set() {
        let (pool, _dir) = make_pool(16);
        assert_eq!(pool.dirty_page_count(), 0);
        let (_, fid) = pool.new_page().unwrap();
        assert_eq!(pool.dirty_page_count(), 1);
        pool.unpin(fid);
    }

    #[test]
    fn flush_page_removes_from_dirty_set() {
        let (pool, _dir) = make_pool(16);
        let (page_id, fid) = pool.new_page().unwrap();
        pool.unpin(fid);
        assert!(pool.dirty_page_count() >= 1);
        pool.flush_page(page_id).unwrap();
        assert_eq!(pool.dirty_page_count(), 0);
    }

    #[test]
    fn eviction_removes_from_dirty_set() {
        let (pool, _dir) = make_pool(4);
        // Fill pool with 4 dirty pages
        for _ in 0..4 {
            let (_, fid) = pool.new_page().unwrap();
            pool.unpin(fid);
        }
        let dirty_before = pool.dirty_page_count();
        assert_eq!(dirty_before, 4);
        // Allocating one more forces eviction of one dirty page
        let (_, fid) = pool.new_page().unwrap();
        pool.unpin(fid);
        // dirty_set should have 4 (evicted one removed, new one added)
        assert_eq!(pool.dirty_page_count(), 4);
    }

    #[tokio::test]
    async fn background_flusher_clears_dirty() {
        let (pool, _dir) = make_pool(32);
        let pool = std::sync::Arc::new(pool);
        // Directly insert frame indices to simulate dirty pages
        for i in 0..20u32 {
            pool.dirty_set.lock().insert(i);
        }
        // threshold_pct = 0.0 means always flush when any dirty pages exist
        let handle = spawn_background_flusher(pool.clone(), 10, 0.0, 100);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(pool.dirty_page_count(), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn background_flusher_respects_threshold() {
        let (pool, _dir) = make_pool(32);
        let pool = std::sync::Arc::new(pool);
        // Insert only 2 dirty pages
        pool.dirty_set.lock().insert(0);
        pool.dirty_set.lock().insert(1);
        // threshold_pct = 0.5 means need > 16 dirty pages out of 32 to flush
        let handle = spawn_background_flusher(pool.clone(), 10, 0.5, 100);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // Should NOT have flushed — 2 < 16
        assert_eq!(pool.dirty_page_count(), 2);
        handle.abort();
    }
}

#[cfg(test)]
mod eviction_content_integrity_tests {
    use super::*;

    /// Single-threaded control for
    /// `tests::concurrent_eviction_never_hands_out_the_same_frame_twice`:
    /// confirms heavy eviction pressure alone (no concurrency at all) never
    /// corrupts frame content, isolating the earlier bug to the concurrent
    /// path specifically rather than eviction/reuse in general.
    #[test]
    fn sequential_heavy_eviction_preserves_content() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let disk = DiskManager::open(&db_path).unwrap();
        let pool = BufferPool::new(disk, None, 4, 0);

        const STAMP_OFFSET: usize = PAGE_SIZE - 4;
        let mut page_ids = Vec::new();
        for _ in 0..64 {
            let (page_id, frame_id) = pool.new_page().unwrap();
            let data = pool.frame_data_mut(frame_id);
            data[STAMP_OFFSET..STAMP_OFFSET + 4].copy_from_slice(&page_id.to_le_bytes());
            pool.mark_dirty(frame_id);
            pool.unpin(frame_id);
            page_ids.push(page_id);
        }

        let mut bad = Vec::new();
        for &page_id in &page_ids {
            let frame_id = pool.fetch_page(page_id).unwrap();
            let data = pool.frame_data(frame_id);
            let stamp =
                u32::from_le_bytes(data[STAMP_OFFSET..STAMP_OFFSET + 4].try_into().unwrap());
            pool.unpin(frame_id);
            if stamp != page_id {
                bad.push((page_id, stamp));
            }
        }
        assert!(
            bad.is_empty(),
            "sequential (single-threaded) corruption found: {bad:?}"
        );
    }
}
