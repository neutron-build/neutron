//! Frames, their states, their latches and the handles callers hold on
//! them (S-STORE §5.1, §5.3; card A-3a, Work 7).
//!
//! Invariants owned here:
//! - **Guards cannot outlive their pin** (§5.3 "No latch escapes a call"):
//!   a [`SharedGuard`] or [`ExclusiveGuard`] borrows the [`Pin`] it came
//!   from, so the frame stays `Resident` under the pin's key while any guard
//!   on it lives (BUF-PINNED-STAYS).
//! - **A pin is counted** (BUF-PINNED-STAYS): a [`Pin`] exists only for a
//!   count raised under the table mutex, and its `Drop` lowers that count
//!   under the mutex, even a poisoned one, and never panics.
//! - **A frame is held while a pin counts on it** (card A-3b-0, Work 4): a
//!   [`Pin`]'s last count (U1) and a dropped [`FrameReservation`] (U2) are two
//!   of the four transitions that make a held frame unheld; each counts the
//!   event and wakes the acquisitions waiting for an unpin, still under the
//!   table mutex. Test `unpin_wait_then_served`. Mutant
//!   UNHELD-NO-BUMP-UNPIN is caught by hang since A6′
//!   (`acquire_vs_acquire`, `unpin_wait_then_served`; re-gate ruling A-3b-0
//!   (a)) and by assertion (`own_pass_rechecks`).
//! - **Clean only by a write or a free** (BUF-LEAVE-CLEAN): the dirty flag is
//!   cleared only by `mark_written`, whose four checks are §3.3's against
//!   the home its caller wrote, or by `mark_freed`; a freed page is never
//!   marked dirty again (§4.2: "Nothing mutates a freed page").
//! - **§4.1's pin follows the page's dirtiness** (card A-3c, Work 6.2 and
//!   6.3; S-STORE §4.1 "Residency"; R-1-memory §4.4: "§4.1's pins end when a
//!   MapLeaf's last dirty page leaves the pool"): in a write-back pool an
//!   LPID page's `mark_dirty` on a clean page takes the pin from a resident
//!   MapLeaf before the flag is set, and `mark_written` and `mark_freed` end
//!   it; a pool built with `Pool::new` takes none. Tests `mappin_residency`,
//!   `dirty_needs_resident_mapleaf`, `pin_hook_kinds`, `t_pool_floor_scattered`;
//!   mutants MAPPIN-KEPT, WB-MAPPIN-NOT-TAKEN, WB-DIRTY-COLD-IGNORED.
//! - **Every frame-latch request announces itself** (card A-3c, Work 10):
//!   the four `Pin` methods and `try_victim` fire `LatchTaking` first, in a
//!   write-back pool, holding what the caller holds.
//! - **The frame latch is task-fair, and on the store's sync layer**
//!   (§5.3 "Fair latches"; card A-3b, Work 4): a [`FrameLatch`] is card
//!   A-4c0's [`Latch`] with bypass bound [`FRAME_LATCH_BOUND`] and a fixed
//!   waiter queue, over the backend `P`, beside the frame's content in a
//!   [`RwCell`]. Every lock, wait, wake-up and atomic of a frame's latch is
//!   the sync layer's, so the interleaving harness runs the latch's own code
//!   (§15), and `std::sync::RwLock` is gone from `buf`.
//! - **A cell is never waited on** (the sync layer's F7): each of the
//!   latch's four methods takes the content cell only under a latch grant,
//!   which makes the try succeed; a busy cell under a grant is an engine
//!   invariant violation and returns `Invalid`. Each guard declares the
//!   cell's borrow before the latch's guard, so the cell is released first:
//!   released the other way round, a writer granted at the release instant
//!   could find the cell still read-held (mutant CELL-RELEASE-ORDER; test
//!   `harness_cell_handoff`). No store path names `std::sync` here but
//!   `Arc` (card A-3b, Limit 3).
//! - **A reservation returns its frame** (BUF-FIXED): a [`FrameReservation`]
//!   not consumed by the table returns its frame to the free list when it
//!   is dropped (mutant RESERVATION-LEAK).

use core::fmt;
use core::mem::ManuallyDrop;
use core::ops::{Deref, DerefMut};

use nucleus_env::File;

use super::key::PageKey;
use super::pool::{HookPoint, Pool};
use super::sync::StdPrimitives;
use crate::latch::{Latch, LatchGuard, Mode, TryLatch};
use crate::meta::Ref;
use crate::store::StoreError;
use crate::sync::{Primitives, RwCell, RwCellReadGuard, RwCellWriteGuard};

/// The bypass bound `K` of every frame latch (card A-3b; S-STORE §5.3: "A-3b
/// pins `K`").
///
/// A waiter is overtaken by at most this many later requests. It is 1, the
/// smallest bound that lets a running request take a freed latch while the
/// queue's head wakes, which strict FIFO handoff cannot (§5.3: it "convoys
/// on a write-hot root"). It also keeps the fairness bound's bypass term,
/// and so a pending flip's delay (§5.3 "The fairness bound", §5.6), the
/// smallest of any `K ≥ 1`. The measurements of A-5, A-7e and E7 can raise
/// it, since every latch takes it at construction. Tests
/// `frame_latch_pinned` (the pin), `t_root_latch_fair_*`; mutant
/// FRAME-BOUND-ZERO.
pub const FRAME_LATCH_BOUND: u32 = 1;

/// A frame's index in its pool, `0 ≤ id < frames` (S-STORE §5.1: "A fixed
/// array of `F` frames"; card A-3a).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FrameId(pub u32);

/// A frame's state (S-STORE §5.1, §5.4; card A-3a, Work 7). The five states
/// partition the pool's
/// frames (BUF-FIXED), and a frame is in the page table exactly when it is
/// `Loading` or `Resident` (BUF-ONE-FRAME). Deliberately not
/// `#[non_exhaustive]`: the tests match it exhaustively, so a state added
/// later fails to compile there until they name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameState {
    /// On the free list (§5.1), in no reservation and not in the page table.
    Free,
    /// Owned by a [`FrameReservation`]: off the free list and not in the page
    /// table (a pin's miss path holds one between its steps P3 and P4; a
    /// split holds one for each page it will create, §6.4).
    Reserved,
    /// In the page table under its key, being loaded (S-STORE §5.4 "The
    /// load"): its one loader holds a count on it and fills it; pins that
    /// found it hold counts while they wait. A page being loaded is not
    /// resident.
    Loading,
    /// In the page table under its key, its content valid: loaded and
    /// checked (§3.3), or created and initialised under its creator's latch.
    Resident,
    /// Out of the page table: a load that failed, or found the key unmapped,
    /// removed it while waiters still held counts; it keeps the key it was
    /// loading and becomes `Free` when the last waiter drops its count (§5.4:
    /// a pin never takes a frame whose load failed).
    Orphan,
}

/// A frame's content, guarded by its latch (S-STORE §5.1: "Holds the page
/// bytes, … a dirty flag, … and the content epoch").
#[derive(Debug)]
pub(crate) struct FrameContent {
    /// The page bytes, `page_size` long, allocated once by `Pool::new`.
    pub(crate) bytes: Box<[u8]>,
    /// Whether the bytes differ from the page's home (or the page has none).
    pub(crate) dirty: bool,
    /// Whether the page was freed (§4.2): never written again, and never
    /// mutated until a creation of its key remakes it in this frame.
    pub(crate) freed: bool,
    /// The content epoch (§1), low 32 bits, as the header and the map entry
    /// hold it. Card A-3a stores it and compares it with nothing but
    /// `mark_written`'s reference; A-7a's `write_guard` decides the rest.
    pub(crate) content_epoch: u32,
}

/// A frame's latch (S-STORE §5.3 "Fair latches"; card A-3b, Work 4): card
/// A-4c0's task-fair [`Latch`] over the backend `P`, with bypass bound
/// [`FRAME_LATCH_BOUND`] and room for every thread the pool counts, beside
/// the frame's content in a [`RwCell`]. Its four methods are card A-3a's,
/// so no caller changed when the inside did.
///
/// Invariants owned here:
/// - The latch excludes; the cell excludes nothing by itself and is never
///   waited on (F7): a method takes the cell only under the latch's grant.
/// - [`LatchShared`] and [`LatchExclusive`] declare the cell's borrow before
///   the latch's guard, so a guard that drops releases the cell first.
pub(crate) struct FrameLatch<P: Primitives = StdPrimitives> {
    content: RwCell<P, FrameContent>,
    latch: Latch<P>,
}

/// A held shared latch on a frame's content (card A-3b): the cell's shared
/// borrow, then the latch's guard. The fields drop in declaration order, so
/// the cell is released before the latch; [`LatchShared::release`] keeps the
/// same order.
pub(crate) struct LatchShared<'a, P: Primitives = StdPrimitives> {
    content: RwCellReadGuard<'a, P, FrameContent>,
    latch: LatchGuard<'a, P>,
}

/// A held exclusive latch on a frame's content (card A-3b): the cell's
/// exclusive borrow, then the latch's guard, dropped in that order.
pub(crate) struct LatchExclusive<'a, P: Primitives = StdPrimitives> {
    content: RwCellWriteGuard<'a, P, FrameContent>,
    latch: LatchGuard<'a, P>,
}

impl<P: Primitives> Deref for LatchShared<'_, P> {
    type Target = FrameContent;
    fn deref(&self) -> &FrameContent {
        &self.content
    }
}

impl<P: Primitives> Deref for LatchExclusive<'_, P> {
    type Target = FrameContent;
    fn deref(&self) -> &FrameContent {
        &self.content
    }
}

impl<P: Primitives> DerefMut for LatchExclusive<'_, P> {
    fn deref_mut(&mut self) -> &mut FrameContent {
        &mut self.content
    }
}

macro_rules! held_latch {
    ($guard:ident) => {
        impl<P: Primitives> $guard<'_, P> {
            /// The hold's stamps (the latch's W2): its arrival's and its
            /// grant's.
            pub(crate) fn stamps(&self) -> (u64, u64) {
                self.latch.stamps()
            }

            /// Release the hold: the cell's borrow first, then the latch, so
            /// a writer granted at the release instant finds the cell free.
            /// Returns the latch's release stamp.
            ///
            /// # Errors
            ///
            /// [`StoreError::Poisoned`] if the latch's internal mutex is
            /// poisoned, in which case the latch is not released.
            pub(crate) fn release(self) -> Result<u64, StoreError> {
                let Self { content, latch } = self;
                drop(content);
                latch.release()
            }
        }
    };
}

held_latch!(LatchShared);
held_latch!(LatchExclusive);

impl<P: Primitives> fmt::Debug for FrameLatch<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The content is not shown: reading it takes the latch. The latch's
        // snapshot is taken under its own internal mutex.
        match self.latch.info() {
            Ok(info) => f.debug_struct("FrameLatch").field("latch", &info).finish(),
            Err(err) => f
                .debug_struct("FrameLatch")
                .field("error", &err.to_string())
                .finish(),
        }
    }
}

/// The error of a content cell that is busy under a granted latch: an engine
/// invariant violation (S-STORE §9.2: XX000; the sync layer's F7).
fn content_busy() -> StoreError {
    StoreError::Invalid("frame content busy under a granted latch (S-STORE §5.3; F7)".into())
}

impl<P: Primitives> FrameLatch<P> {
    /// A latch over `content`, with bypass bound [`FRAME_LATCH_BOUND`] and a
    /// waiter queue of `threads` entries, the pool's thread count (card
    /// A-3b-0's `PoolSizing::threads`): a thread waits on at most one latch
    /// at a time, so that queue never overflows in a correct build.
    ///
    /// # Errors
    ///
    /// [`StoreError::Invalid`] outside [`Latch::new`]'s domain;
    /// [`StoreError::OutOfMemory`] if the queue's allocation fails. This is
    /// the latch's only allocation (A-4c0's W7).
    pub(crate) fn new(threads: u32, content: FrameContent) -> Result<FrameLatch<P>, StoreError> {
        Ok(FrameLatch {
            content: RwCell::new(content),
            latch: Latch::new(FRAME_LATCH_BOUND, threads)?,
        })
    }

    /// Take the latch shared, blocking (§5.3), then the cell under the grant.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch or the cell was poisoned;
    /// [`StoreError::Invalid`] if the cell is busy under the grant (F7). In
    /// both cases the latch's guard is dropped, so the latch is released.
    pub(crate) fn shared(&self) -> Result<LatchShared<'_, P>, StoreError> {
        let latch = self.latch.latch(Mode::Shared)?;
        match self.content.try_read()? {
            Some(content) => Ok(LatchShared { content, latch }),
            None => Err(content_busy()),
        }
    }

    /// Take the latch exclusively, blocking (§5.3), then the cell under the
    /// grant.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch or the cell was poisoned;
    /// [`StoreError::Invalid`] if the cell is busy under the grant (F7). In
    /// both cases the latch's guard is dropped.
    pub(crate) fn exclusive(&self) -> Result<LatchExclusive<'_, P>, StoreError> {
        let latch = self.latch.latch(Mode::Exclusive)?;
        match self.content.try_write()? {
            Some(content) => Ok(LatchExclusive { content, latch }),
            None => Err(content_busy()),
        }
    }

    /// Take the latch shared if that does not block, then the cell under the
    /// grant; `Ok(None)` if the latch would block.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch or the cell was poisoned;
    /// [`StoreError::Invalid`] if the cell is busy under the grant (F7).
    pub(crate) fn try_shared(&self) -> Result<Option<LatchShared<'_, P>>, StoreError> {
        match self.latch.try_latch(Mode::Shared)? {
            TryLatch::Refused(_) => Ok(None),
            TryLatch::Granted(latch) => match self.content.try_read()? {
                Some(content) => Ok(Some(LatchShared { content, latch })),
                None => Err(content_busy()),
            },
        }
    }

    /// Take the latch exclusively if that does not block, then the cell
    /// under the grant; `Ok(None)` if the latch would block.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch or the cell was poisoned;
    /// [`StoreError::Invalid`] if the cell is busy under the grant (F7).
    pub(crate) fn try_exclusive(&self) -> Result<Option<LatchExclusive<'_, P>>, StoreError> {
        match self.latch.try_latch(Mode::Exclusive)? {
            TryLatch::Refused(_) => Ok(None),
            TryLatch::Granted(latch) => match self.content.try_write()? {
                Some(content) => Ok(Some(LatchExclusive { content, latch })),
                None => Err(content_busy()),
            },
        }
    }

    /// The waiter capacity of the latch's queue, the length it was allocated
    /// with: what [`super::Pool::footprint`] measures (card A-3b, Work 6).
    /// Fixed at construction: no lock.
    pub(crate) fn capacity(&self) -> u32 {
        self.latch.capacity()
    }

    /// A snapshot of the latch under its internal mutex
    /// ([`super::Pool::latch_info`], card A-3b, Work 7).
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch's internal mutex is poisoned.
    pub(crate) fn info(&self) -> Result<crate::latch::LatchInfo, StoreError> {
        self.latch.info()
    }

    /// The length of the frame's page buffer, read under the latch shared
    /// (blocking), as R-1's footprint measures the allocation: call it
    /// holding no latch.
    ///
    /// A failed read, which only a poisoned or busy latch or cell gives,
    /// answers `fallback`, the length the pool built its buffers with: the
    /// store is then failed (§9.2), and the report is no longer R-1's.
    pub(crate) fn page_len(&self, fallback: usize) -> usize {
        match self.shared() {
            Ok(held) => held.bytes.len(),
            Err(_) => fallback,
        }
    }
}

/// A pin on a `Resident` frame (S-STORE §5.3: "Pins guarantee residency, not
/// identity"; card A-3a, Work 7): while it lives the frame stays `Resident`
/// under [`Pin::key`] (BUF-PINNED-STAYS). It takes no latch; its `latch_*`
/// methods do, and their guards borrow it. The backend `P` is the pool's
/// (card A-3b, Work 5).
pub struct Pin<'p, H: File, P: Primitives = StdPrimitives> {
    pool: &'p Pool<H, P>,
    frame: FrameId,
    key: PageKey,
    /// Whether the pin is an operation's own, which fires `Pinned` where it
    /// is taken and `Unpinned` where it drops (§5.3), or one the eviction
    /// takes for the frame it obtains (card A-3c, Work 6.5), which fires
    /// neither: not an operation's pin in §5.4's count.
    hooks: bool,
}

impl<'p, H: File, P: Primitives> Pin<'p, H, P> {
    /// A pin for a count the caller raised on `frame`, a `Resident` frame of
    /// `key`, under the table mutex: an operation's own pin.
    pub(crate) fn counted(pool: &'p Pool<H, P>, frame: FrameId, key: PageKey) -> Pin<'p, H, P> {
        Pin {
            pool,
            frame,
            key,
            hooks: true,
        }
    }

    /// A pin that fires nothing where it is taken or where it drops (card
    /// A-3c, Work 6.5): the eviction's, on a frame the victim's §4.1 pin
    /// already holds or on the frame the acquisition is obtaining.
    pub(crate) fn quiet(pool: &'p Pool<H, P>, frame: FrameId, key: PageKey) -> Pin<'p, H, P> {
        Pin {
            pool,
            frame,
            key,
            hooks: false,
        }
    }

    /// The pinned page's key (§5.1).
    pub fn key(&self) -> PageKey {
        self.key
    }

    /// The pinned frame (§5.3: a pin guarantees residency, not identity).
    pub fn frame(&self) -> FrameId {
        self.frame
    }

    /// Latch the frame shared, blocking (§5.3).
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch was poisoned;
    /// [`StoreError::Invalid`] if the frame's content cell is busy under the
    /// latch's grant, an engine invariant violation (F7).
    pub fn latch_shared(&self) -> Result<SharedGuard<'_, P>, StoreError> {
        self.pool
            .fire_wb(super::pool::HookPoint::LatchTaking, self.key, self.frame);
        let content = self.pool.latch(self.frame)?.shared()?;
        Ok(SharedGuard {
            content,
            key: self.key,
            frame: self.frame,
        })
    }

    /// Latch the frame exclusively, blocking (§5.3).
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch was poisoned;
    /// [`StoreError::Invalid`] if the frame's content cell is busy under the
    /// latch's grant (F7).
    pub fn latch_exclusive(&self) -> Result<ExclusiveGuard<'_, H, P>, StoreError> {
        self.pool
            .fire_wb(super::pool::HookPoint::LatchTaking, self.key, self.frame);
        let content = self.pool.latch(self.frame)?.exclusive()?;
        Ok(ExclusiveGuard {
            content,
            pool: self.pool,
            key: self.key,
            frame: self.frame,
        })
    }

    /// Latch the frame shared if that does not block; `Ok(None)` if it would
    /// (§5.3).
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch was poisoned;
    /// [`StoreError::Invalid`] if the frame's content cell is busy under the
    /// latch's grant (F7).
    pub fn try_latch_shared(&self) -> Result<Option<SharedGuard<'_, P>>, StoreError> {
        self.pool
            .fire_wb(super::pool::HookPoint::LatchTaking, self.key, self.frame);
        Ok(self
            .pool
            .latch(self.frame)?
            .try_shared()?
            .map(|content| SharedGuard {
                content,
                key: self.key,
                frame: self.frame,
            }))
    }

    /// Latch the frame exclusively if that does not block; `Ok(None)` if it
    /// would (§5.3).
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch was poisoned;
    /// [`StoreError::Invalid`] if the frame's content cell is busy under the
    /// latch's grant (F7).
    pub fn try_latch_exclusive(&self) -> Result<Option<ExclusiveGuard<'_, H, P>>, StoreError> {
        self.pool
            .fire_wb(super::pool::HookPoint::LatchTaking, self.key, self.frame);
        Ok(self
            .pool
            .latch(self.frame)?
            .try_exclusive()?
            .map(|content| ExclusiveGuard {
                content,
                pool: self.pool,
                key: self.key,
                frame: self.frame,
            }))
    }
}

impl<H: File, P: Primitives> Drop for Pin<'_, H, P> {
    /// Lower the count under the table mutex; a count already 0, impossible by
    /// construction, stays 0. A `Resident` frame whose last count leaves
    /// becomes unheld here (U1, card A-3b-0, Work 4), still under that
    /// mutex, so an acquisition waiting for an unpin wakes. Then fire
    /// `Unpinned`. Never panics.
    ///
    /// The sync layer has no way into a poisoned mutex (card A-3b, Work 5),
    /// so on a table mutex a panic poisoned this leaves the count where it is
    /// and returns: the frame stays counted, the pool is unusable, and that
    /// is right, since the panic poisoned the store (S-STORE §9.2) and
    /// A-3c's mark fails every later call.
    fn drop(&mut self) {
        {
            let mut t = match self.pool.lock_table() {
                Ok(t) => t,
                Err(_) => return,
            };
            let mut last = false;
            if let Ok(m) = t.meta_mut(self.frame) {
                if m.state == FrameState::Resident && m.pins == 1 {
                    m.pins = 0;
                    last = true;
                } else {
                    m.pins = m.pins.saturating_sub(1);
                }
            }
            if last {
                self.pool.became_unheld(&mut t);
            }
        }
        if self.hooks {
            self.pool.fire(HookPoint::Unpinned, self.key, self.frame);
        }
    }
}

impl<H: File, P: Primitives> fmt::Debug for Pin<'_, H, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pin")
            .field("key", &self.key)
            .field("frame", &self.frame)
            .finish()
    }
}

/// A shared latch on a pinned frame (§5.3), borrowing its [`Pin`]. It reads
/// as the whole page.
pub struct SharedGuard<'a, P: Primitives = StdPrimitives> {
    content: LatchShared<'a, P>,
    key: PageKey,
    frame: FrameId,
}

impl<P: Primitives> SharedGuard<'_, P> {
    /// The page's key (§5.1).
    pub fn key(&self) -> PageKey {
        self.key
    }

    /// The latched frame (§5.3).
    pub fn frame(&self) -> FrameId {
        self.frame
    }

    /// The hold's stamps: the frame latch's arrival and grant stamps, in its
    /// logical clock (card A-3b, Work 4; the latch's W2). They are for
    /// diagnostics and the interleaving harness's monitor (§15
    /// `t_root_latch_fair`), and no store code reads them.
    pub fn stamps(&self) -> (u64, u64) {
        self.content.stamps()
    }

    /// Release the latch and return its release stamp (the latch's W2). A
    /// guard that is dropped instead releases the same way and ignores the
    /// release's error.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch's internal mutex is poisoned,
    /// in which case the latch is not released.
    pub fn release(self) -> Result<u64, StoreError> {
        self.content.release()
    }

    /// The frame's content epoch (§1).
    pub fn content_epoch(&self) -> u32 {
        self.content.content_epoch
    }

    /// Whether the page is dirty (§5.2).
    pub fn is_dirty(&self) -> bool {
        self.content.dirty
    }

    /// Whether the page was freed (§4.2).
    pub fn is_freed(&self) -> bool {
        self.content.freed
    }
}

impl<P: Primitives> Deref for SharedGuard<'_, P> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.content.bytes
    }
}

impl<P: Primitives> fmt::Debug for SharedGuard<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedGuard")
            .field("key", &self.key)
            .field("frame", &self.frame)
            .finish()
    }
}

/// An exclusive latch on a frame (§5.3): from a [`Pin`], which it borrows,
/// or inside a [`CreatedPage`]. It reads and writes as the whole page; a
/// caller mutates the bytes only after [`ExclusiveGuard::mark_dirty`]
/// returns `Ok` (A-7a's `write_guard` calls it).
pub struct ExclusiveGuard<'a, H: File, P: Primitives = StdPrimitives> {
    content: LatchExclusive<'a, P>,
    pool: &'a Pool<H, P>,
    key: PageKey,
    frame: FrameId,
}

impl<'a, H: File, P: Primitives> ExclusiveGuard<'a, H, P> {
    /// A guard over a latch already held on `frame`, a frame of `key`.
    pub(crate) fn held(
        content: LatchExclusive<'a, P>,
        pool: &'a Pool<H, P>,
        key: PageKey,
        frame: FrameId,
    ) -> ExclusiveGuard<'a, H, P> {
        ExclusiveGuard {
            content,
            pool,
            key,
            frame,
        }
    }

    /// The page's key (§5.1).
    pub fn key(&self) -> PageKey {
        self.key
    }

    /// The latched frame (§5.3).
    pub fn frame(&self) -> FrameId {
        self.frame
    }

    /// The hold's stamps: the frame latch's arrival and grant stamps, in its
    /// logical clock (card A-3b, Work 4; the latch's W2). They are for
    /// diagnostics and the interleaving harness's monitor (§15
    /// `t_root_latch_fair`), and no store code reads them.
    pub fn stamps(&self) -> (u64, u64) {
        self.content.stamps()
    }

    /// Release the latch and return its release stamp (the latch's W2). A
    /// guard that is dropped instead releases the same way and ignores the
    /// release's error.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the latch's internal mutex is poisoned,
    /// in which case the latch is not released.
    pub fn release(self) -> Result<u64, StoreError> {
        self.content.release()
    }

    /// The frame's content epoch (§1).
    pub fn content_epoch(&self) -> u32 {
        self.content.content_epoch
    }

    /// Whether the page is dirty (§5.2).
    pub fn is_dirty(&self) -> bool {
        self.content.dirty
    }

    /// Whether the page was freed (§4.2).
    pub fn is_freed(&self) -> bool {
        self.content.freed
    }

    /// Mark the page dirty in content epoch `epoch` (§5.1, §5.6), before the
    /// caller mutates its bytes.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a freed page (§4.2: "Nothing mutates a
    /// freed page"; only a creation of its key remakes it); the page is then
    /// unchanged.
    pub fn mark_dirty(&mut self, epoch: u32) -> Result<(), StoreError> {
        self.pool
            .mark_dirty_frame(&mut self.content, self.key, self.frame, epoch)
    }

    /// Record that the caller wrote these bytes, sealed, to `home.slot`
    /// (§5.2): for an LPID page it set the key's current reference to `home`
    /// first, and for a map page it keeps a pin on the frame until the
    /// parent's reference names `home` (§4.1's two-step write), so the frame
    /// never leaves the page table before its reference names its home
    /// (BUF-LEAVE-CLEAN; I-PIN-CURRENT). The page is then clean.
    ///
    /// # Errors
    /// [`StoreError::Invalid`], with the page still dirty, unless `home.slot`
    /// is a page slot (not 0–2, not reserved, its bytes addressable), the
    /// page is not freed, `home.epoch` is the content epoch, the bytes pass
    /// `page::verify` against `home` and the key's header id (§3.3), and the
    /// key admits the header's kind.
    pub fn mark_written(&mut self, home: Ref) -> Result<(), StoreError> {
        self.pool
            .mark_written(&mut self.content, self.key, self.frame, home)
    }

    /// Mark the page freed (§4.2): clean, never written again, and never
    /// mutated until a creation of its key remakes it in this frame
    /// (BUF-LEAVE-CLEAN). Sets the table's mirror of the flag under the
    /// table mutex, taken under this latch (BUF-TABLE-LEAF's order).
    /// Idempotent.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the table mutex was poisoned.
    pub fn mark_freed(&mut self) -> Result<(), StoreError> {
        self.pool.mark_freed(&mut self.content, self.frame)
    }
}

impl<H: File, P: Primitives> Deref for ExclusiveGuard<'_, H, P> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.content.bytes
    }
}

impl<H: File, P: Primitives> DerefMut for ExclusiveGuard<'_, H, P> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.content.bytes
    }
}

impl<H: File, P: Primitives> fmt::Debug for ExclusiveGuard<'_, H, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExclusiveGuard")
            .field("key", &self.key)
            .field("frame", &self.frame)
            .finish()
    }
}

/// `mark_dirty`'s flag, after [`super::Pool::mark_dirty_frame`] took the
/// page's §4.1 pin where its clean-to-dirty transition needs one (card A-3c,
/// Work 6.2).
pub(crate) fn set_dirty(
    content: &mut FrameContent,
    key: PageKey,
    epoch: u32,
) -> Result<(), StoreError> {
    if content.freed {
        return Err(StoreError::Invalid(format!(
            "{key:?} was freed and is never mutated again (S-STORE §4.2)"
        )));
    }
    content.dirty = true;
    content.content_epoch = epoch;
    Ok(())
}

/// A page just created (S-STORE §5.4, §6.4; card A-3a, `Pool::create`): a
/// [`Pin`] with the frame's exclusive latch held, so no other thread reads
/// the page before its creator releases that latch (BUF-BORN-LATCHED). It
/// has [`ExclusiveGuard`]'s methods, and [`CreatedPage::release`] gives back
/// the pin.
pub struct CreatedPage<'p, H: File, P: Primitives = StdPrimitives> {
    // Declared first, so it drops first: the latch is released before the
    // pin's count is lowered.
    guard: ExclusiveGuard<'p, H, P>,
    pin: Pin<'p, H, P>,
}

impl<'p, H: File, P: Primitives> CreatedPage<'p, H, P> {
    /// A created page over `guard`, the exclusive latch on `pin`'s frame.
    pub(crate) fn new(
        guard: ExclusiveGuard<'p, H, P>,
        pin: Pin<'p, H, P>,
    ) -> CreatedPage<'p, H, P> {
        CreatedPage { guard, pin }
    }

    /// Release the latch and keep the pin: from here other threads may read
    /// the page (§6.8 "Root names").
    pub fn release(self) -> Pin<'p, H, P> {
        let CreatedPage { guard, pin } = self;
        drop(guard);
        pin
    }

    /// The page's key (§5.1).
    pub fn key(&self) -> PageKey {
        self.pin.key()
    }

    /// The page's frame (§5.3).
    pub fn frame(&self) -> FrameId {
        self.pin.frame()
    }

    /// The frame's content epoch (§1).
    pub fn content_epoch(&self) -> u32 {
        self.guard.content_epoch()
    }

    /// Whether the page is dirty (§5.2); a created page is, until written.
    pub fn is_dirty(&self) -> bool {
        self.guard.is_dirty()
    }

    /// Whether the page was freed (§4.2).
    pub fn is_freed(&self) -> bool {
        self.guard.is_freed()
    }

    /// As [`ExclusiveGuard::mark_dirty`] (§5.6).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a freed page.
    pub fn mark_dirty(&mut self, epoch: u32) -> Result<(), StoreError> {
        self.guard.mark_dirty(epoch)
    }

    /// As [`ExclusiveGuard::mark_written`] (§5.2, §3.3).
    ///
    /// # Errors
    /// [`StoreError::Invalid`], the page still dirty, unless every check of
    /// [`ExclusiveGuard::mark_written`] passes.
    pub fn mark_written(&mut self, home: Ref) -> Result<(), StoreError> {
        self.guard.mark_written(home)
    }

    /// As [`ExclusiveGuard::mark_freed`] (§4.2).
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the table mutex was poisoned.
    pub fn mark_freed(&mut self) -> Result<(), StoreError> {
        self.guard.mark_freed()
    }
}

impl<H: File, P: Primitives> Deref for CreatedPage<'_, H, P> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.guard
    }
}

impl<H: File, P: Primitives> DerefMut for CreatedPage<'_, H, P> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.guard
    }
}

impl<H: File, P: Primitives> fmt::Debug for CreatedPage<'_, H, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreatedPage")
            .field("key", &self.pin.key())
            .field("frame", &self.pin.frame())
            .finish()
    }
}

/// A `Reserved` frame owned by the caller (card A-3a, `Pool::reserve`; S-STORE
/// §5.4's "reservation's pages", §6.4): off the free list and not in the page
/// table. Dropped unconsumed, it returns
/// the frame to the free list (BUF-FIXED).
pub struct FrameReservation<'p, H: File, P: Primitives = StdPrimitives> {
    pool: &'p Pool<H, P>,
    frame: FrameId,
}

impl<'p, H: File, P: Primitives> FrameReservation<'p, H, P> {
    /// A reservation of `frame`, which the caller set `Reserved` under the
    /// table mutex.
    pub(crate) fn reserved(pool: &'p Pool<H, P>, frame: FrameId) -> FrameReservation<'p, H, P> {
        FrameReservation { pool, frame }
    }

    /// The reserved frame (§5.4).
    pub fn frame(&self) -> FrameId {
        self.frame
    }

    /// Whether this reservation is of `pool`'s frame.
    pub(crate) fn is_of(&self, pool: &Pool<H, P>) -> bool {
        core::ptr::eq(self.pool, pool)
    }

    /// Hand the frame to the page table: the caller has just inserted it, so
    /// the reservation no longer owns it and its `Drop` does not run.
    pub(crate) fn consume(self) -> FrameId {
        ManuallyDrop::new(self).frame
    }
}

impl<H: File, P: Primitives> Drop for FrameReservation<'_, H, P> {
    /// Return the frame to the free list (`Free`, key and freed flag
    /// cleared), under the table mutex. A frame that returns becomes unheld
    /// here (U2, card A-3b-0, Work 4), still under that mutex. Never panics:
    /// on a poisoned table mutex it returns and the frame stays `Reserved`.
    fn drop(&mut self) {
        // A table mutex a panic poisoned cannot be taken (card A-3b, Work 5):
        // the frame stays `Reserved`, as when the release below fails, and
        // the store is poisoned (S-STORE §9.2).
        let mut t = match self.pool.lock_table() {
            Ok(t) => t,
            Err(_) => return,
        };
        // Only an engine bug fails this, and `drop` cannot report it: the
        // frame then stays `Reserved`, which `Pool::stats` shows.
        if t.release(self.frame).is_ok() {
            self.pool.became_unheld(&mut t);
        }
    }
}

impl<H: File, P: Primitives> fmt::Debug for FrameReservation<'_, H, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameReservation")
            .field("frame", &self.frame)
            .finish()
    }
}

/// A `Resident` frame's exclusive latch, taken with no pin by
/// `Pool::try_victim` (S-STORE §5.2: "The evictor takes victims with
/// `try_latch` and never blocks on a victim"). It reads and writes as the
/// whole page, for sealing; [`VictimGuard::remove_from_table`] takes the
/// frame out of the page table if it is still unpinned and clean.
pub struct VictimGuard<'p, H: File, P: Primitives = StdPrimitives> {
    content: LatchExclusive<'p, P>,
    pool: &'p Pool<H, P>,
    key: PageKey,
    frame: FrameId,
}

impl<'p, H: File, P: Primitives> VictimGuard<'p, H, P> {
    /// A victim guard over `content`, the exclusive latch of `frame`, which
    /// the caller found `Resident` under `key` with no count.
    pub(crate) fn latched(
        content: LatchExclusive<'p, P>,
        pool: &'p Pool<H, P>,
        key: PageKey,
        frame: FrameId,
    ) -> VictimGuard<'p, H, P> {
        VictimGuard {
            content,
            pool,
            key,
            frame,
        }
    }

    /// The page's key (§5.1).
    pub fn key(&self) -> PageKey {
        self.key
    }

    /// The latched frame (§5.3).
    pub fn frame(&self) -> FrameId {
        self.frame
    }

    /// The frame's content epoch (§1).
    pub fn content_epoch(&self) -> u32 {
        self.content.content_epoch
    }

    /// Whether the page is dirty (§5.2).
    pub fn is_dirty(&self) -> bool {
        self.content.dirty
    }

    /// Whether the page was freed (§4.2).
    pub fn is_freed(&self) -> bool {
        self.content.freed
    }

    /// As [`ExclusiveGuard::mark_written`]: the evictor wrote the sealed
    /// bytes to `home` and, for an LPID page, set its entry first (§5.2).
    ///
    /// # Errors
    /// [`StoreError::Invalid`], the page still dirty, unless every check of
    /// [`ExclusiveGuard::mark_written`] passes.
    pub fn mark_written(&mut self, home: Ref) -> Result<(), StoreError> {
        self.pool
            .mark_written(&mut self.content, self.key, self.frame, home)
    }

    /// Pin the frame while its latch is held (§4.1's two-step map-page write:
    /// the pin keeps a second eviction off the frame until the parent's
    /// reference names its new home). Fires `Pinned` holding the latch.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the count is at `u64::MAX` (BUF-NO-WRAP).
    pub fn pin_frame(&self) -> Result<Pin<'p, H, P>, StoreError> {
        self.pool.pin_latched_victim(self.frame, self.key)
    }

    /// Take the frame out of the page table (S-STORE §5.2, §5.4; steps R1 and
    /// R2): fire `RemoveLatched`; then, under the table mutex, if the frame
    /// has no pin and is not dirty, remove its key, make it `Reserved` and
    /// return it as [`Removal::Removed`]; otherwise [`Removal::Kept`]. Either
    /// way the latch is released. It never writes: a dirty page is written
    /// by its caller first and marked with `mark_written`, and a freed page
    /// never (BUF-LEAVE-CLEAN).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the frame is no longer `Resident` under its
    /// key (an engine invariant violation); [`StoreError::Poisoned`] if the
    /// table mutex was poisoned.
    pub fn remove_from_table(self) -> Result<Removal<'p, H, P>, StoreError> {
        let VictimGuard {
            content,
            pool,
            key,
            frame,
        } = self;
        pool.remove_victim(content, key, frame)
    }
}

impl<H: File, P: Primitives> Deref for VictimGuard<'_, H, P> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.content.bytes
    }
}

impl<H: File, P: Primitives> DerefMut for VictimGuard<'_, H, P> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.content.bytes
    }
}

impl<H: File, P: Primitives> fmt::Debug for VictimGuard<'_, H, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VictimGuard")
            .field("key", &self.key)
            .field("frame", &self.frame)
            .finish()
    }
}

/// What [`VictimGuard::remove_from_table`] did (S-STORE §5.2).
#[must_use]
pub enum Removal<'p, H: File, P: Primitives = StdPrimitives> {
    /// The frame left the page table and is `Reserved` for the caller (A-3b's
    /// frame acquisition uses it at once; dropped, it returns to the free
    /// list; §5.2).
    Removed(FrameReservation<'p, H, P>),
    /// The frame stays in the page table: it was pinned or dirty (§5.2, §5.4).
    Kept,
}

impl<H: File, P: Primitives> fmt::Debug for Removal<'_, H, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Removal::Removed(r) => f.debug_tuple("Removed").field(r).finish(),
            Removal::Kept => f.write_str("Kept"),
        }
    }
}
