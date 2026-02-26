//! Buffer pool manager — the page cache.
//!
//! All page access goes through the buffer pool. Pages are pinned while in use
//! and evicted via LRU-K(2) when memory pressure requires it.

use std::cell::UnsafeCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use super::disk::DiskManager;
use super::page::{self, PageBuf, INVALID_PAGE_ID, PAGE_SIZE};

/// Default buffer pool: 1024 frames × 16 KB = 16 MB.
pub const DEFAULT_POOL_SIZE: usize = 1024;

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
    /// Read-write latch protecting the page content.
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

struct LruKReplacer {
    k: usize,
    current_ts: AtomicU64,
    frames: Mutex<HashMap<u32, FrameHistory>>,
}

impl LruKReplacer {
    fn new(k: usize) -> Self {
        Self {
            k,
            current_ts: AtomicU64::new(0),
            frames: Mutex::new(HashMap::new()),
        }
    }

    fn record_access(&self, frame_id: u32) {
        let ts = self.current_ts.fetch_add(1, Ordering::Relaxed);
        let mut frames = self.frames.lock();
        let entry = frames.entry(frame_id).or_insert_with(|| FrameHistory {
            access_history: VecDeque::with_capacity(self.k),
            is_evictable: false,
        });
        if entry.access_history.len() >= self.k {
            entry.access_history.pop_front();
        }
        entry.access_history.push_back(ts);
    }

    fn set_evictable(&self, frame_id: u32, evictable: bool) {
        let mut frames = self.frames.lock();
        if let Some(entry) = frames.get_mut(&frame_id) {
            entry.is_evictable = evictable;
        }
    }

    fn evict(&self) -> Option<u32> {
        let mut frames = self.frames.lock();
        let current_ts = self.current_ts.load(Ordering::Relaxed);

        let mut best_frame: Option<u32> = None;
        let mut best_k_dist: u64 = 0;
        let mut best_earliest: u64 = u64::MAX;
        let mut best_has_k = true; // start true so first +inf candidate wins

        for (&frame_id, history) in frames.iter() {
            if !history.is_evictable {
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
                    (true, false) => true,   // +inf beats finite
                    (false, true) => false,  // finite doesn't beat +inf
                    (false, false) => earliest < best_earliest,
                    (true, true) => k_dist > best_k_dist,
                }
            };

            if is_better {
                best_frame = Some(frame_id);
                best_k_dist = k_dist;
                best_earliest = earliest;
                best_has_k = has_k;
            }
        }

        if let Some(frame_id) = best_frame {
            frames.remove(&frame_id);
        }
        best_frame
    }

    fn remove(&self, frame_id: u32) {
        self.frames.lock().remove(&frame_id);
    }
}

// ============================================================================
// Buffer pool
// ============================================================================

/// The buffer pool manager. Central point for all page access.
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
    pub fn new(disk: DiskManager, wal: Option<Box<dyn super::wal::WalBackend>>, pool_size: usize, initial_pages: u32) -> Self {
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
        }
    }

    /// Fetch a page into the buffer pool and pin it. Returns the frame ID.
    /// The caller must call `unpin` when done.
    pub fn fetch_page(&self, page_id: u32) -> Result<u32, BufferError> {
        // Check if already in pool
        if let Some(frame_id) = self.page_table.lookup(page_id) {
            let desc = &self.descriptors[frame_id as usize];
            desc.pin_count.fetch_add(1, Ordering::AcqRel);
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(frame_id);
        }

        // Cache miss — must load from disk
        self.stats.misses.fetch_add(1, Ordering::Relaxed);

        // Get a free frame
        let frame_id = self.get_free_frame()?;

        // Read from disk
        self.disk
            .read_page(page_id, &mut self.frame_data_mut(frame_id))?;

        // Verify checksum (skip for freshly allocated pages with all zeros)
        let page_data = self.frame_data(frame_id);
        if page::get_page_type(page_data) != page::PAGE_TYPE_FREE || page::read_u32(page_data, page::HEADER_CHECKSUM) != 0 {
            if !page::verify_checksum(page_data) {
                return Err(BufferError::ChecksumMismatch(page_id));
            }
        }

        // Setup descriptor
        let desc = &self.descriptors[frame_id as usize];
        desc.page_id.store(page_id, Ordering::Release);
        desc.pin_count.store(1, Ordering::Release);
        desc.is_dirty.store(false, Ordering::Release);

        // Register in page table
        self.page_table.insert(page_id, frame_id);

        // Track in replacer
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        Ok(frame_id)
    }

    /// Allocate a new page on disk and fetch it into the pool.
    pub fn new_page(&self) -> Result<(u32, u32), BufferError> {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        self.disk.extend_to_page(page_id)?;

        let frame_id = self.get_free_frame()?;

        // Initialize blank page
        let data = self.frame_data_mut(frame_id);
        data.fill(0);

        let desc = &self.descriptors[frame_id as usize];
        desc.page_id.store(page_id, Ordering::Release);
        desc.pin_count.store(1, Ordering::Release);
        desc.is_dirty.store(true, Ordering::Release);

        self.page_table.insert(page_id, frame_id);
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);

        Ok((page_id, frame_id))
    }

    /// Get a read reference to the page data in a frame.
    pub fn frame_data(&self, frame_id: u32) -> &PageBuf {
        // SAFETY: Read access is safe because callers coordinate via pin_count
        // and frame latches (RwLock). The UnsafeCell provides interior mutability;
        // concurrent reads are valid when no writer holds the latch.
        unsafe { &(*self.frames[frame_id as usize].get()).data }
    }

    /// Get a mutable reference to the page data in a frame.
    /// SAFETY: Caller must hold appropriate latch or be the sole pinner.
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

    /// Mark a frame as dirty (modified).
    pub fn mark_dirty(&self, frame_id: u32) {
        let was_dirty = self.descriptors[frame_id as usize]
            .is_dirty
            .swap(true, Ordering::AcqRel);
        if !was_dirty {
            self.stats.dirty_pages.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Unpin a frame (decrement pin count).
    pub fn unpin(&self, frame_id: u32) {
        let desc = &self.descriptors[frame_id as usize];
        let old = desc.pin_count.fetch_sub(1, Ordering::AcqRel);
        if old == 1 {
            self.replacer.set_evictable(frame_id, true);
        }
    }

    /// Flush a specific page to disk (WAL record written first if WAL is enabled).
    pub fn flush_page(&self, page_id: u32) -> Result<(), BufferError> {
        if let Some(frame_id) = self.page_table.lookup(page_id) {
            let desc = &self.descriptors[frame_id as usize];
            if desc.is_dirty.load(Ordering::Acquire) {
                let data = self.frame_data_mut(frame_id);
                // WAL protocol: log before flush
                if let Some(ref wal) = self.wal {
                    let lsn = wal.log_page_write(0, page_id, data)
                        .map_err(|e| BufferError::Io(e))?;
                    page::set_page_lsn(data, lsn);
                }
                // Checksum must be computed AFTER LSN is set so on-disk page is valid
                page::write_checksum(data);
                self.disk.write_page(page_id, data)?;
                desc.is_dirty.store(false, Ordering::Release);
                self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&self) -> Result<(), BufferError> {
        // Flush WAL first (force all WAL records to disk)
        if let Some(ref wal) = self.wal {
            wal.sync().map_err(|e| BufferError::Io(e))?;
        }
        for i in 0..self.pool_size {
            let desc = &self.descriptors[i];
            let page_id = desc.page_id.load(Ordering::Acquire);
            if page_id != INVALID_PAGE_ID && desc.is_dirty.load(Ordering::Acquire) {
                let data = self.frame_data_mut(i as u32);
                if let Some(ref wal) = self.wal {
                    let lsn = wal.log_page_write(0, page_id, data)
                        .map_err(|e| BufferError::Io(e))?;
                    page::set_page_lsn(data, lsn);
                }
                // Checksum must be computed AFTER LSN is set so on-disk page is valid
                page::write_checksum(data);
                self.disk.write_page(page_id, data)?;
                desc.is_dirty.store(false, Ordering::Release);
                self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.disk.sync()?;
        Ok(())
    }

    /// Collect dirty pages for async flushing, handling WAL and checksum inline.
    ///
    /// Returns `Vec<(page_id, page_bytes)>` and marks each collected page clean.
    /// The caller is responsible for writing the returned bytes to disk via
    /// an async I/O backend (e.g. `AsyncDiskOps`).
    pub fn collect_dirty_for_async_flush(&self) -> Result<Vec<(u32, Vec<u8>)>, BufferError> {
        if let Some(ref wal) = self.wal {
            wal.sync().map_err(BufferError::Io)?;
        }
        let mut dirty = Vec::new();
        for i in 0..self.pool_size {
            let desc = &self.descriptors[i];
            let page_id = desc.page_id.load(Ordering::Acquire);
            if page_id != INVALID_PAGE_ID && desc.is_dirty.load(Ordering::Acquire) {
                let data = self.frame_data_mut(i as u32);
                if let Some(ref wal) = self.wal {
                    let lsn = wal.log_page_write(0, page_id, data).map_err(BufferError::Io)?;
                    page::set_page_lsn(data, lsn);
                }
                page::write_checksum(data);
                dirty.push((page_id, data.to_vec()));
                desc.is_dirty.store(false, Ordering::Release);
                self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
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

    /// Get WAL stats: (bytes_written, syncs). Returns (0, 0) if no WAL is configured.
    pub fn wal_stats(&self) -> (u64, u64) {
        self.wal.as_ref().map_or((0, 0), |w| w.wal_stats())
    }

    /// Prefetch a set of pages into the buffer pool.
    ///
    /// Loads pages that are not already cached without pinning them,
    /// useful for sequential scan read-ahead. Errors on individual
    /// pages are silently ignored (best-effort prefetch).
    pub fn prefetch(&self, page_ids: &[u32]) {
        for &page_id in page_ids {
            // Skip if already in pool
            if self.page_table.lookup(page_id).is_some() {
                continue;
            }
            // Try to fetch and immediately unpin
            if let Ok(frame_id) = self.fetch_page(page_id) {
                self.unpin(frame_id);
            }
        }
    }

    // Internal: get a free frame by popping from free list or evicting.
    fn get_free_frame(&self) -> Result<u32, BufferError> {
        // Try free list
        if let Some(frame_id) = self.free_list.lock().pop() {
            return Ok(frame_id);
        }

        // Evict
        let frame_id = self.replacer.evict().ok_or(BufferError::PoolFull)?;
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);

        let desc = &self.descriptors[frame_id as usize];
        let old_page_id = desc.page_id.load(Ordering::Acquire);

        // Flush if dirty (WAL record first, then checksum — same order as flush_page)
        if desc.is_dirty.load(Ordering::Acquire) {
            let data = self.frame_data_mut(frame_id);
            // WAL protocol: log before flush, set LSN first
            if let Some(ref wal) = self.wal {
                let lsn = wal.log_page_write(0, old_page_id, data)
                    .map_err(|e| BufferError::Io(e))?;
                page::set_page_lsn(data, lsn);
            }
            // Checksum must be computed AFTER LSN is set so on-disk page is valid
            page::write_checksum(data);
            self.disk.write_page(old_page_id, data)?;
            desc.is_dirty.store(false, Ordering::Release);
            self.stats.dirty_pages.fetch_sub(1, Ordering::Relaxed);
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
        pool.descriptors[fid as usize].is_dirty.store(false, Ordering::Release);

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
        assert!(misses_after > misses_before, "prefetch should cause disk reads");

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
}
