//! The page table and the free list (S-STORE §5.1, §5.7), crate-private.
//! Every function here is called with the pool's table mutex held, and none
//! allocates: the buckets, the per-frame metadata and the free stack are
//! sized once by `Pool::new` and never grow (BUF-FIXED, §5.1 "It never
//! grows", §5.7 "page table | O(F)"; R-1-memory §5.1 "page-table entries
//! (sized for `F` at open, never rehashed larger)").
//!
//! Invariants owned here:
//! - **Chained buckets.** The table is `frames.next_power_of_two()` buckets,
//!   each the head of a chain linked through the frames' `next` fields; a
//!   frame is on at most one chain, the one its key hashes to. A chain is a
//!   simple path: a walk that meets more links than there are frames has met
//!   a cycle, an engine invariant violation (`Invalid`; XX000 under S-STORE
//!   §9.2 at draft 7.20, mapped by card A-3c-B), and stops, rather than loop
//!   or answer "absent" (tests `table_chains`, the model tests; mutants
//!   TABLE-UNLINK-HEAD-ONLY, TABLE-INSERT-OVERWRITES).
//! - **The free list** is a fixed stack of the `F` frame ids, initially
//!   `[F−1, …, 1, 0]`, so pops give frame 0, then 1, and so on, and the frame
//!   pushed last is popped next (LIFO, which tests `load_integrity` (j) and
//!   `born_latched` rely on). A frame is on it exactly when its state is
//!   `Free` ([`Table::release`] refuses a frame that is already `Free`).
//! - **§4.1's pins** (card A-3c, Work 6.1; S-STORE §4.1 "Residency": "a
//!   MapLeaf stays resident while any page it maps is dirty in the pool (it
//!   holds one pin per such page; §5.4)"): a MapLeaf frame's `dirty_pins`
//!   counts the dirty LPID pages that hold it and is part of its `pins`, so
//!   `try_victim` refuses it; an LPID page's `holds_dirty_pin` is true exactly
//!   while it is dirty in a write-back pool. Both change under this mutex,
//!   latched by the page whose transition they belong to.
//! - **The reference bit** (S-STORE §5.2 "Clock"; card A-3b-0, Work 3):
//!   [`FrameMeta::referenced`] is set only where a user pin raises a frame's
//!   count, cleared when the frame becomes `Free` or `Reserved`, and by the
//!   clock's sweep. Test `clock_bit_sites`; mutants of the sweep's table,
//!   "the bit".
//! - **The clock's counter and the generation** (card A-3b-0, Work 4): `hand`
//!   advances once per sweep step, whoever takes it, so its position is
//!   `hand mod F`; `unheld_events` changes only at U1–U4, each under this
//!   mutex. Test `clock_order`; mutant HAND-STAYS. Mutant
//!   UNHELD-NO-BUMP-UNPIN is caught by hang since A6′
//!   (`acquire_vs_acquire`, `unpin_wait_then_served`; re-gate ruling A-3b-0
//!   (a)) and by assertion (`own_pass_rechecks`).

use super::frame::{FrameId, FrameState};
use super::key::PageKey;
use crate::store::StoreError;

/// One frame's entry in the page table's bookkeeping, guarded by the table
/// mutex (card A-3a, Work 6). A frame's state, key and pin count change only
/// under that mutex (BUF-ONE-FRAME).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameMeta {
    /// The frame's state (`FrameState`).
    pub(crate) state: FrameState,
    /// The key the frame holds or loads while `Loading`, `Resident` or
    /// `Orphan` (an Orphan frame keeps the key it was loading, out of the
    /// table); `None` while `Free` or `Reserved`.
    pub(crate) key: Option<PageKey>,
    /// The pin count: the loader's and the waiters' counts on a `Loading`
    /// frame, the `Pin`s on a `Resident` one, the waiters' on an `Orphan`.
    pub(crate) pins: u64,
    /// The frame's freed flag, mirrored here (from the frame's content,
    /// under its latch) so that `create` can read it under the mutex.
    pub(crate) freed: bool,
    /// The clock's reference bit (S-STORE §5.2: a frame used since the hand
    /// last passed it gets a second chance; card A-3b-0, Work 3): set only
    /// where a user pin raises the count, never by `pin_frame`, and cleared
    /// when the frame becomes `Free` or `Reserved` and by the sweep.
    pub(crate) referenced: bool,
    /// On a MapLeaf's frame (card A-3c, Work 6.1): how many LPID pages it
    /// maps hold §4.1's pin on it while they are dirty in the pool (S-STORE
    /// §4.1 "Residency"). A part of `pins`, kept apart so that
    /// `try_victim` refuses the frame and the hooks can tell it from an
    /// operation's pin; 0 on every other frame.
    pub(crate) dirty_pins: u64,
    /// On an LPID page's frame (card A-3c, Work 6.1): whether the page is
    /// dirty and so holds §4.1's pin on its MapLeaf's frame; false on every
    /// other frame and on a clean page.
    pub(crate) holds_dirty_pin: bool,
    /// The next frame on this frame's bucket chain.
    pub(crate) next: Option<FrameId>,
}

impl FrameMeta {
    /// A frame on the free list: no key, no pins, not freed, not referenced,
    /// on no chain.
    pub(crate) const FREE: FrameMeta = FrameMeta {
        state: FrameState::Free,
        key: None,
        pins: 0,
        freed: false,
        referenced: false,
        dirty_pins: 0,
        holds_dirty_pin: false,
        next: None,
    };
}

/// The page table's buckets, the frames' bookkeeping and the free stack
/// (card A-3a, Work 6), all allocated by `Pool::new`.
#[derive(Debug)]
pub(crate) struct Table {
    /// Bucket heads; the length is a power of two, at least the frame count.
    buckets: Box<[Option<FrameId>]>,
    /// `buckets.len() − 1`.
    mask: u64,
    /// One entry per frame, indexed by `FrameId`.
    meta: Box<[FrameMeta]>,
    /// The free stack; its first `free_len` entries are the free frames.
    free: Box<[FrameId]>,
    /// How many entries of `free` are on the stack.
    free_len: u32,
    /// Slot reads issued by loads (card A-3a, `PoolStats::reads`).
    pub(crate) reads: u64,
    /// The clock's counter (card A-3b-0, Work 4): its position is
    /// `hand mod F`, and it advances by one per sweep step, whoever takes it.
    pub(crate) hand: u64,
    /// Clean victims the frame acquisition has taken (card A-3b-0, Work 5,
    /// A2; `PoolStats::evictions`).
    pub(crate) evictions: u64,
    /// Frame acquisitions registered as waiting for an unpin (card A-3b-0,
    /// Work 5, A7; `PoolStats::unpin_waiters`).
    pub(crate) unpin_waiters: u32,
    /// The generation a waiting acquisition re-tests (card A-3b-0, Work 4):
    /// it changes only when a held frame becomes unheld, at U1–U4, each under
    /// this mutex, and wraps.
    pub(crate) unheld_events: u64,
}

/// The bucket `key` lands in, for a table whose bucket count is
/// `mask + 1` (a power of two). Shared by the table and
/// `Pool::bucket_of`, so the two cannot disagree.
pub(crate) fn bucket_index(key: PageKey, mask: u32) -> u32 {
    // The mask has fewer than 32 bits set, so the cast cannot truncate.
    (key.hash64() & u64::from(mask)) as u32
}

impl Table {
    /// A table over buffers `Pool::new` allocated: `buckets` all empty, its
    /// length a power of two of at least `meta.len()`; `meta` all
    /// [`FrameMeta::FREE`]; `free` holding every frame id, `[F−1, …, 0]`.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the lengths do not fit together.
    pub(crate) fn new(
        buckets: Box<[Option<FrameId>]>,
        meta: Box<[FrameMeta]>,
        free: Box<[FrameId]>,
    ) -> Result<Table, StoreError> {
        let n = buckets.len();
        if !n.is_power_of_two() || n < meta.len() || free.len() != meta.len() {
            return Err(StoreError::Invalid(format!(
                "page table of {n} buckets, {} frames and a free stack of {}",
                meta.len(),
                free.len()
            )));
        }
        let mask = u64::try_from(n - 1)
            .map_err(|_| StoreError::Invalid("bucket count past u64".into()))?;
        let free_len = u32::try_from(free.len())
            .map_err(|_| StoreError::Invalid("frame count past u32".into()))?;
        Ok(Table {
            buckets,
            mask,
            meta,
            free,
            free_len,
            reads: 0,
            hand: 0,
            evictions: 0,
            unpin_waiters: 0,
            unheld_events: 0,
        })
    }

    /// The number of frames.
    pub(crate) fn frames(&self) -> usize {
        self.meta.len()
    }

    /// The number of buckets.
    pub(crate) fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// How many frames are on the free list.
    pub(crate) fn free_len(&self) -> u32 {
        self.free_len
    }

    /// Every frame's bookkeeping, for the diagnostics.
    pub(crate) fn metas(&self) -> &[FrameMeta] {
        &self.meta
    }

    /// Frame `f`'s bookkeeping.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range.
    pub(crate) fn meta(&self, f: FrameId) -> Result<&FrameMeta, StoreError> {
        usize::try_from(f.0)
            .ok()
            .and_then(|i| self.meta.get(i))
            .ok_or_else(|| out_of_range(f, self.meta.len()))
    }

    /// Frame `f`'s bookkeeping, to change it.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range.
    pub(crate) fn meta_mut(&mut self, f: FrameId) -> Result<&mut FrameMeta, StoreError> {
        let n = self.meta.len();
        usize::try_from(f.0)
            .ok()
            .and_then(|i| self.meta.get_mut(i))
            .ok_or_else(|| out_of_range(f, n))
    }

    /// The bucket `key` lands in, as an index into `buckets`.
    fn bucket(&self, key: PageKey) -> Result<usize, StoreError> {
        usize::try_from(key.hash64() & self.mask)
            .map_err(|_| StoreError::Invalid("bucket index past usize".into()))
    }

    /// The head of bucket `b`.
    fn head(&self, b: usize) -> Result<Option<FrameId>, StoreError> {
        self.buckets
            .get(b)
            .copied()
            .ok_or_else(|| StoreError::Invalid(format!("bucket {b} out of range")))
    }

    /// Set the head of bucket `b`.
    fn set_head(&mut self, b: usize, f: Option<FrameId>) -> Result<(), StoreError> {
        let head = self
            .buckets
            .get_mut(b)
            .ok_or_else(|| StoreError::Invalid(format!("bucket {b} out of range")))?;
        *head = f;
        Ok(())
    }

    /// The frame the table holds for `key`, if any: a walk of `key`'s bucket
    /// chain comparing whole keys (an LPID and a MapLeaf position never
    /// match each other).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the chain is broken: a link out of range,
    /// or more links than frames (a cycle).
    pub(crate) fn lookup(&self, key: PageKey) -> Result<Option<FrameId>, StoreError> {
        let mut cur = self.head(self.bucket(key)?)?;
        for _ in 0..self.meta.len() {
            let Some(f) = cur else {
                return Ok(None);
            };
            let m = self.meta(f)?;
            if m.key == Some(key) {
                return Ok(Some(f));
            }
            cur = m.next;
        }
        match cur {
            None => Ok(None),
            Some(_) => Err(broken_chain(key)),
        }
    }

    /// Link frame `f` at the head of `key`'s bucket chain, with the old head
    /// behind it. Precondition: `key` is absent (the caller looked it up
    /// under the same mutex hold) and `f` is on no chain; the caller sets
    /// `f`'s key and state.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range.
    pub(crate) fn insert(&mut self, key: PageKey, f: FrameId) -> Result<(), StoreError> {
        let b = self.bucket(key)?;
        let old = self.head(b)?;
        self.meta_mut(f)?.next = old;
        self.set_head(b, Some(f))
    }

    /// Unlink frame `f` from `key`'s bucket chain, wherever it is on it.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is not on that chain, or the chain is
    /// broken; the table is then unchanged.
    pub(crate) fn remove(&mut self, key: PageKey, f: FrameId) -> Result<(), StoreError> {
        let b = self.bucket(key)?;
        let mut prev: Option<FrameId> = None;
        let mut cur = self.head(b)?;
        for _ in 0..self.meta.len() {
            let Some(g) = cur else {
                break;
            };
            let next = self.meta(g)?.next;
            if g == f {
                match prev {
                    None => self.set_head(b, next)?,
                    Some(p) => self.meta_mut(p)?.next = next,
                }
                self.meta_mut(f)?.next = None;
                return Ok(());
            }
            prev = Some(g);
            cur = next;
        }
        Err(StoreError::Invalid(format!(
            "frame {} is not in the page table under {key:?}",
            f.0
        )))
    }

    /// Pop the free stack: the frame pushed last, or frame 0, 1, … from a
    /// fresh table. `None` when no frame is free.
    pub(crate) fn pop_free(&mut self) -> Option<FrameId> {
        let top = self.free_len.checked_sub(1)?;
        let f = *self.free.get(usize::try_from(top).ok()?)?;
        self.free_len = top;
        Some(f)
    }

    /// Take a frame off the free list for the caller (card A-3a's `reserve`;
    /// card A-3b-0's step A1): pop the free stack and make the frame
    /// `Reserved`, its reference bit cleared (card A-3b-0, Work 3). `None`
    /// when no frame is free.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if a frame on the free list is not `Free` (an
    /// engine invariant violation); that frame is pushed back and the table
    /// is otherwise unchanged.
    pub(crate) fn reserve_free(&mut self) -> Result<Option<FrameId>, StoreError> {
        let Some(f) = self.pop_free() else {
            return Ok(None);
        };
        let m = self.meta_mut(f)?;
        if m.state != FrameState::Free {
            let state = m.state;
            self.push_free(f)?;
            return Err(StoreError::Invalid(format!(
                "frame {} is {state:?} on the free list",
                f.0
            )));
        }
        m.state = FrameState::Reserved;
        m.referenced = false;
        Ok(Some(f))
    }

    /// The clock's next frame, `hand mod F`, advancing the counter by one
    /// (card A-3b-0, Work 5, A2).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the table has no frame, or its frame count
    /// or the position does not fit a `u32` (neither can a pool reach).
    pub(crate) fn clock_frame(&mut self) -> Result<FrameId, StoreError> {
        let f = self.clock_at(self.hand)?;
        self.hand = self.hand.wrapping_add(1);
        Ok(f)
    }

    /// The frame the clock's counter `hand` names.
    fn clock_at(&self, hand: u64) -> Result<FrameId, StoreError> {
        // `new` sized the metadata from a `u32` frame count of at least 1.
        self.meta
            .first()
            .and_then(|_| u64::try_from(self.meta.len()).ok())
            .and_then(|n| u32::try_from(hand % n).ok())
            .map(FrameId)
            .ok_or_else(|| StoreError::Invalid("the page table has no frame to sweep".into()))
    }

    /// The clock's position, `hand mod F` (card A-3b-0, Work 7).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] as [`Table::clock_frame`]'s.
    pub(crate) fn clock_hand(&self) -> Result<u32, StoreError> {
        self.clock_at(self.hand).map(|f| f.0)
    }

    /// Whether some frame is `Resident` with no count: the scan that decides
    /// between a retry, `PoolStarved` and a wait for an unpin (card A-3b-0,
    /// Work 5, A6).
    pub(crate) fn has_unpinned_resident(&self) -> bool {
        self.meta
            .iter()
            .any(|m| m.state == FrameState::Resident && m.pins == 0)
    }

    /// Push frame `f` on the free stack.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the stack is full (`f` would be a frame
    /// freed twice).
    pub(crate) fn push_free(&mut self, f: FrameId) -> Result<(), StoreError> {
        let full = || StoreError::Invalid(format!("frame {} pushed on a full free list", f.0));
        let next_len = self.free_len.checked_add(1).ok_or_else(full)?;
        let at = usize::try_from(self.free_len).map_err(|_| full())?;
        let entry = self.free.get_mut(at).ok_or_else(full)?;
        *entry = f;
        self.free_len = next_len;
        Ok(())
    }

    /// Raise frame `f`'s pin count by one (BUF-NO-WRAP).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the count is already `u64::MAX`; the count
    /// is then unchanged.
    pub(crate) fn raise(&mut self, f: FrameId) -> Result<(), StoreError> {
        let m = self.meta_mut(f)?;
        m.pins = m.pins.checked_add(1).ok_or_else(|| {
            StoreError::Invalid(format!(
                "frame {}'s pin count is at u64::MAX and cannot be raised",
                f.0
            ))
        })?;
        Ok(())
    }

    /// Lower frame `f`'s pin count by one and return what is left.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the count is already 0 (an engine
    /// invariant violation); the count is then unchanged.
    pub(crate) fn lower(&mut self, f: FrameId) -> Result<u64, StoreError> {
        let m = self.meta_mut(f)?;
        m.pins = m.pins.checked_sub(1).ok_or_else(|| {
            StoreError::Invalid(format!("frame {}'s pin count is already 0", f.0))
        })?;
        Ok(m.pins)
    }

    /// Return frame `f` to the free list: `Free`, no key, not freed, not
    /// referenced, on no chain, pushed on the free stack (BUF-FIXED's "returns
    /// to it").
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is already `Free`, still counted, or
    /// the stack is full; the table is then unchanged.
    pub(crate) fn release(&mut self, f: FrameId) -> Result<(), StoreError> {
        let m = *self.meta(f)?;
        if m.state == FrameState::Free || m.pins != 0 {
            return Err(StoreError::Invalid(format!(
                "frame {} cannot return to the free list: {:?} with {} pins",
                f.0, m.state, m.pins
            )));
        }
        self.push_free(f)?;
        *self.meta_mut(f)? = FrameMeta::FREE;
        Ok(())
    }
}

/// The error for a frame id past the pool.
fn out_of_range(f: FrameId, frames: usize) -> StoreError {
    StoreError::Invalid(format!(
        "frame {} is out of range for a pool of {frames} frames",
        f.0
    ))
}

/// The error for a bucket chain with more links than frames.
fn broken_chain(key: PageKey) -> StoreError {
    StoreError::Invalid(format!(
        "the page table's chain for {key:?} has more links than frames"
    ))
}
