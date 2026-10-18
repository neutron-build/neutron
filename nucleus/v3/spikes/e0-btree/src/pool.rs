//! The E0 buffer pool: the "simple" N8 buffer manager of PLAN-v3 §3.3.
//!
//! Invariants owned here (card X-0 Work 3):
//! - a page is pinned whenever its latch is (or is about to be) held: the
//!   pin count is raised under the page-table mutex, and the evictor may
//!   only consider frames whose pin count is zero, so a frame cannot be
//!   reused under a live guard;
//! - the page-table mutex is held while a frame's identity (`Frame::id`)
//!   changes, so lookups never see a half-installed page;
//! - a dirty victim is written back to the store **before** its frame is
//!   reused (this is what `tests/model.rs` at 16 frames pins down with the
//!   DIRTY-LOST mutation);
//! - the evictor never blocks on a page latch it cannot justify: victims
//!   have pin count zero, and pin count zero together with the table mutex
//!   held means no other thread holds or waits on that frame's latch, so
//!   plain latch acquisition cannot deadlock (PLAN-v3 §3.3 "the evictor
//!   uses try_latch and never blocks on a victim" — here the same guarantee
//!   follows from the pin discipline rather than try-locking).
//!
//! Deadlock-freedom of the whole pool follows from one lock order: the
//! page-table mutex is a leaf lock (never held while acquiring a frame
//! latch that could be pinned by someone waiting on the mutex), and frame
//! latches are taken top-down by the tree.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::page;
use crate::store::{PageId, PageStore, StoreError};

/// One frame of the pool: the page bytes, their identity, and the dirty
/// bit. Guarded by its own `RwLock` latch (N8).
#[derive(Debug)]
struct Frame {
    id: Option<PageId>,
    data: Box<[u8]>,
    dirty: bool,
}

/// A frame slot: the latched frame plus its pin count.
#[derive(Debug)]
struct Slot {
    page: RwLock<Frame>,
    pins: AtomicU32,
}

/// The simple buffer manager over any [`PageStore`] (card X-0 Work 3).
///
/// `capacity` frames, a page table `Mutex<HashMap<PageId, usize>>`, and a
/// clock hand that skips pinned frames. There is deliberately nothing
/// cleverer here (N8): if E0/E7 shows this costing more than 20% of a point
/// op, the deferred-mechanism trigger of PLAN-v3 §3.12 fires instead.
pub struct BufferPool<S: PageStore> {
    store: S,
    page_size: usize,
    slots: Vec<Slot>,
    table: Mutex<HashMap<PageId, usize>>,
    clock: AtomicUsize,
}

impl<S: PageStore> BufferPool<S> {
    /// Build a pool of `capacity` frames over `store`.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the capacity is zero or the store's page
    /// size is outside what the slotted format can split safely
    /// (`page::MIN_PAGE..=65535`).
    pub fn new(store: S, capacity: usize) -> Result<Self, StoreError> {
        let page_size = store.page_size();
        if capacity == 0 {
            return Err(StoreError::Invalid("pool capacity must be nonzero".into()));
        }
        if page_size < page::MIN_PAGE || page_size > u16::MAX as usize {
            return Err(StoreError::Invalid(format!(
                "page size {page_size} outside {}..=65535",
                page::MIN_PAGE
            )));
        }
        let slots = (0..capacity)
            .map(|_| Slot {
                page: RwLock::new(Frame {
                    id: None,
                    data: vec![0u8; page_size].into_boxed_slice(),
                    dirty: false,
                }),
                pins: AtomicU32::new(0),
            })
            .collect();
        Ok(Self {
            store,
            page_size,
            slots,
            table: Mutex::new(HashMap::new()),
            clock: AtomicUsize::new(0),
        })
    }

    /// The page size of the underlying store.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Consume the pool and hand back the store (used by tests and the
    /// benchmark to inspect page counts).
    pub fn into_store(self) -> S {
        self.store
    }

    /// Latch page `id` shared and pin it; the page is loaded on a miss.
    ///
    /// # Errors
    /// Any [`StoreError`] from the store, or [`StoreError::PoolStarved`]
    /// if every frame stayed pinned across two clock sweeps.
    pub fn fetch_read(&self, id: PageId) -> Result<ReadGuard<'_, S>, StoreError> {
        let frame = self.locate(id)?;
        let inner = self.latch_read(frame)?;
        Ok(ReadGuard {
            pool: self,
            frame,
            inner,
        })
    }

    /// Latch page `id` exclusive and pin it; the page is loaded on a miss.
    ///
    /// # Errors
    /// As [`Self::fetch_read`].
    pub fn fetch_write(&self, id: PageId) -> Result<WriteGuard<'_, S>, StoreError> {
        let frame = self.locate(id)?;
        let inner = self.latch_write(frame)?;
        Ok(WriteGuard {
            pool: self,
            frame,
            inner,
        })
    }

    /// Allocate a fresh page and return it latched exclusive. The frame is
    /// installed dirty and zeroed: the caller writes the page header.
    ///
    /// # Errors
    /// As [`Self::fetch_read`], plus the store's allocation error.
    pub fn new_page(&self) -> Result<(PageId, WriteGuard<'_, S>), StoreError> {
        let id = self.store.allocate()?;
        let mut table = self.lock_table()?;
        if table.contains_key(&id) {
            return Err(StoreError::Corrupt(format!(
                "store handed out page {id:?} twice"
            )));
        }
        let frame = self.victim(&mut table)?;
        let mut inner = self.latch_write(frame)?;
        if let Some(old) = inner.id {
            if inner.dirty {
                self.store.write(old, &inner.data)?;
            }
            table.remove(&old);
        }
        inner.data.fill(0);
        inner.id = Some(id);
        inner.dirty = true;
        if let Some(slot) = self.slots.get(frame) {
            slot.pins.store(1, Ordering::Release);
        }
        table.insert(id, frame);
        drop(table);
        Ok((
            id,
            WriteGuard {
                pool: self,
                frame,
                inner,
            },
        ))
    }

    /// Write every dirty resident page back to the store and clear the
    /// dirty bits. Safe to call with guards held elsewhere: it simply waits
    /// for each frame's latch in turn (tests call it quiescently before
    /// reopening a file store).
    ///
    /// # Errors
    /// Any store error from the write-back.
    pub fn flush_all(&self) -> Result<(), StoreError> {
        for i in 0..self.slots.len() {
            let mut f = self.latch_write(i)?;
            if let Some(id) = f.id
                && f.dirty
            {
                self.store.write(id, &f.data)?;
                f.dirty = false;
            }
        }
        Ok(())
    }

    /// Shared read latch on a frame the caller has pinned.
    fn latch_read(&self, frame: usize) -> Result<RwLockReadGuard<'_, Frame>, StoreError> {
        self.slot(frame)?
            .page
            .read()
            .map_err(|e| StoreError::Poisoned(e.to_string()))
    }

    /// Exclusive latch on a frame the caller has pinned.
    fn latch_write(&self, frame: usize) -> Result<RwLockWriteGuard<'_, Frame>, StoreError> {
        self.slot(frame)?
            .page
            .write()
            .map_err(|e| StoreError::Poisoned(e.to_string()))
    }

    fn slot(&self, frame: usize) -> Result<&Slot, StoreError> {
        self.slots
            .get(frame)
            .ok_or_else(|| StoreError::Corrupt(format!("frame {frame} does not exist")))
    }

    fn lock_table(&self) -> Result<std::sync::MutexGuard<'_, HashMap<PageId, usize>>, StoreError> {
        self.table
            .lock()
            .map_err(|e| StoreError::Poisoned(e.to_string()))
    }

    /// Return the frame index of `id`, pinned. On a miss, evict a victim
    /// and load the page into it, all under the page-table mutex so two
    /// loaders cannot install the same page twice (or lose an eviction).
    fn locate(&self, id: PageId) -> Result<usize, StoreError> {
        let mut table = self.lock_table()?;
        if let Some(&frame) = table.get(&id) {
            self.pin(frame)?;
            return Ok(frame);
        }
        let frame = self.victim(&mut table)?;
        {
            let slot = self.slot(frame)?;
            let mut f = self.latch_write(frame)?;
            if let Some(old) = f.id {
                // Dirty eviction: write the victim back before reuse
                // (card X-0 Work 3; the DIRTY-LOST mutation removes this).
                if f.dirty {
                    self.store.write(old, &f.data)?;
                }
                table.remove(&old);
            }
            self.store.read(id, &mut f.data)?;
            f.id = Some(id);
            f.dirty = false;
            slot.pins.store(1, Ordering::Release);
        }
        table.insert(id, frame);
        Ok(frame)
    }

    /// Advance the clock hand to the next frame with pin count zero.
    /// Two full sweeps without a victim mean the pool is smaller than the
    /// working depth of its users (PLAN-v3 §3.3 minimum pool size).
    fn victim(&self, _table: &mut HashMap<PageId, usize>) -> Result<usize, StoreError> {
        let n = self.slots.len();
        for _ in 0..2 * n {
            let hand = self.clock.fetch_add(1, Ordering::Relaxed) % n;
            let slot = self.slot(hand)?;
            if slot.pins.load(Ordering::Acquire) == 0 {
                return Ok(hand);
            }
        }
        Err(StoreError::PoolStarved)
    }

    /// Raise a frame's pin count. Called with the table mutex held, which
    /// is what makes pin-vs-evict atomic.
    fn pin(&self, frame: usize) -> Result<(), StoreError> {
        let slot = self.slot(frame)?;
        slot.pins.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Drop a frame's pin count; called by the guard destructors.
    fn unpin(&self, frame: usize) {
        if let Some(slot) = self.slots.get(frame) {
            slot.pins.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Shared latch on a pinned frame; dereferences to the page bytes.
/// Dropping it unpins the frame (card X-0 Work 3).
pub struct ReadGuard<'a, S: PageStore> {
    pool: &'a BufferPool<S>,
    frame: usize,
    inner: RwLockReadGuard<'a, Frame>,
}

impl<S: PageStore> std::ops::Deref for ReadGuard<'_, S> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.inner.data
    }
}

impl<S: PageStore> Drop for ReadGuard<'_, S> {
    fn drop(&mut self) {
        self.pool.unpin(self.frame);
    }
}

/// Exclusive latch on a pinned frame; dereferences (mutably) to the page
/// bytes. Dropping it unpins the frame. Mutations must call
/// [`Self::mark_dirty`] or the page will never be written back.
pub struct WriteGuard<'a, S: PageStore> {
    pool: &'a BufferPool<S>,
    frame: usize,
    inner: RwLockWriteGuard<'a, Frame>,
}

impl<S: PageStore> std::ops::Deref for WriteGuard<'_, S> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.inner.data
    }
}

impl<S: PageStore> std::ops::DerefMut for WriteGuard<'_, S> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.inner.data
    }
}

impl<S: PageStore> WriteGuard<'_, S> {
    /// Record that this frame's bytes changed and must be written back
    /// before the frame is reused or at `flush_all`.
    pub fn mark_dirty(&mut self) {
        self.inner.dirty = true;
    }
}

impl<S: PageStore> Drop for WriteGuard<'_, S> {
    fn drop(&mut self) {
        self.pool.unpin(self.frame);
    }
}
