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
//! - **No std guard escapes** (§5.3; card A-3b): [`FrameLatch`] wraps the
//!   standard library's `RwLock` behind four methods and guard types of its
//!   own, so A-3b replaces its inside with the task-fair latch of §5.3
//!   without touching a caller.
//! - **A reservation returns its frame** (BUF-FIXED): a [`FrameReservation`]
//!   not consumed by the table returns its frame to the free list when it
//!   is dropped (mutant RESERVATION-LEAK).

use core::fmt;
use core::mem::ManuallyDrop;
use core::ops::{Deref, DerefMut};

use nucleus_env::File;

use super::key::PageKey;
use super::pool::{HookPoint, Pool};
use super::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};
use crate::meta::Ref;
use crate::store::StoreError;

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

/// A frame's latch (S-STORE §5.3), card A-3a's form: the standard library's
/// `RwLock` behind four methods. It is not task-fair, a known departure
/// (lib.rs, "Seed status"); card A-3b replaces the inside with §5.3's
/// count-bounded latch behind the same methods.
#[derive(Debug)]
pub(crate) struct FrameLatch {
    lock: RwLock<FrameContent>,
}

/// A held shared latch on a frame's content.
pub(crate) struct LatchShared<'a>(RwLockReadGuard<'a, FrameContent>);

/// A held exclusive latch on a frame's content.
pub(crate) struct LatchExclusive<'a>(RwLockWriteGuard<'a, FrameContent>);

impl Deref for LatchShared<'_> {
    type Target = FrameContent;
    fn deref(&self) -> &FrameContent {
        &self.0
    }
}

impl Deref for LatchExclusive<'_> {
    type Target = FrameContent;
    fn deref(&self) -> &FrameContent {
        &self.0
    }
}

impl DerefMut for LatchExclusive<'_> {
    fn deref_mut(&mut self) -> &mut FrameContent {
        &mut self.0
    }
}

/// The error a poisoned latch gives.
fn poisoned_latch() -> StoreError {
    StoreError::Poisoned("a frame latch was poisoned by a panic".into())
}

impl FrameLatch {
    /// A latch over `content`.
    pub(crate) fn new(content: FrameContent) -> FrameLatch {
        FrameLatch {
            lock: RwLock::new(content),
        }
    }

    /// Take the latch shared, blocking.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if a panic poisoned it.
    pub(crate) fn shared(&self) -> Result<LatchShared<'_>, StoreError> {
        self.lock
            .read()
            .map(LatchShared)
            .map_err(|_| poisoned_latch())
    }

    /// Take the latch exclusively, blocking.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if a panic poisoned it.
    pub(crate) fn exclusive(&self) -> Result<LatchExclusive<'_>, StoreError> {
        self.lock
            .write()
            .map(LatchExclusive)
            .map_err(|_| poisoned_latch())
    }

    /// Take the latch shared if that does not block; `Ok(None)` if it would.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if a panic poisoned it.
    pub(crate) fn try_shared(&self) -> Result<Option<LatchShared<'_>>, StoreError> {
        match self.lock.try_read() {
            Ok(g) => Ok(Some(LatchShared(g))),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(_)) => Err(poisoned_latch()),
        }
    }

    /// Take the latch exclusively if that does not block; `Ok(None)` if it
    /// would.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if a panic poisoned it.
    pub(crate) fn try_exclusive(&self) -> Result<Option<LatchExclusive<'_>>, StoreError> {
        match self.lock.try_write() {
            Ok(g) => Ok(Some(LatchExclusive(g))),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(_)) => Err(poisoned_latch()),
        }
    }

    /// The length of the frame's page buffer, read under the latch shared
    /// (blocking), poisoned or not: R-1's footprint measures the allocation.
    pub(crate) fn page_len(&self) -> usize {
        self.lock
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .bytes
            .len()
    }
}

/// A pin on a `Resident` frame (S-STORE §5.3: "Pins guarantee residency, not
/// identity"; card A-3a, Work 7): while it lives the frame stays `Resident`
/// under [`Pin::key`] (BUF-PINNED-STAYS). It takes no latch; its `latch_*`
/// methods do, and their guards borrow it.
pub struct Pin<'p, H: File> {
    pool: &'p Pool<H>,
    frame: FrameId,
    key: PageKey,
}

impl<'p, H: File> Pin<'p, H> {
    /// A pin for a count the caller raised on `frame`, a `Resident` frame of
    /// `key`, under the table mutex.
    pub(crate) fn counted(pool: &'p Pool<H>, frame: FrameId, key: PageKey) -> Pin<'p, H> {
        Pin { pool, frame, key }
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
    /// [`StoreError::Poisoned`] if the latch was poisoned.
    pub fn latch_shared(&self) -> Result<SharedGuard<'_>, StoreError> {
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
    /// [`StoreError::Poisoned`] if the latch was poisoned.
    pub fn latch_exclusive(&self) -> Result<ExclusiveGuard<'_, H>, StoreError> {
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
    /// [`StoreError::Poisoned`] if the latch was poisoned.
    pub fn try_latch_shared(&self) -> Result<Option<SharedGuard<'_>>, StoreError> {
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
    /// [`StoreError::Poisoned`] if the latch was poisoned.
    pub fn try_latch_exclusive(&self) -> Result<Option<ExclusiveGuard<'_, H>>, StoreError> {
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

impl<H: File> Drop for Pin<'_, H> {
    /// Lower the count under the table mutex, taken even if poisoned; a count
    /// already 0, impossible by construction, stays 0. A `Resident` frame
    /// whose last count leaves becomes unheld here (U1, card A-3b-0, Work 4),
    /// still under that mutex, so an acquisition waiting for an unpin wakes.
    /// Then fire `Unpinned`. Never panics.
    fn drop(&mut self) {
        {
            let mut t = self.pool.lock_table_anyway();
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
        self.pool.fire(HookPoint::Unpinned, self.key, self.frame);
    }
}

impl<H: File> fmt::Debug for Pin<'_, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pin")
            .field("key", &self.key)
            .field("frame", &self.frame)
            .finish()
    }
}

/// A shared latch on a pinned frame (§5.3), borrowing its [`Pin`]. It reads
/// as the whole page.
pub struct SharedGuard<'a> {
    content: LatchShared<'a>,
    key: PageKey,
    frame: FrameId,
}

impl SharedGuard<'_> {
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
}

impl Deref for SharedGuard<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.content.bytes
    }
}

impl fmt::Debug for SharedGuard<'_> {
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
pub struct ExclusiveGuard<'a, H: File> {
    content: LatchExclusive<'a>,
    pool: &'a Pool<H>,
    key: PageKey,
    frame: FrameId,
}

impl<'a, H: File> ExclusiveGuard<'a, H> {
    /// A guard over a latch already held on `frame`, a frame of `key`.
    pub(crate) fn held(
        content: LatchExclusive<'a>,
        pool: &'a Pool<H>,
        key: PageKey,
        frame: FrameId,
    ) -> ExclusiveGuard<'a, H> {
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
        set_dirty(&mut self.content, self.key, epoch)
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
        self.pool.mark_written(&mut self.content, self.key, home)
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

impl<H: File> Deref for ExclusiveGuard<'_, H> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.content.bytes
    }
}

impl<H: File> DerefMut for ExclusiveGuard<'_, H> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.content.bytes
    }
}

impl<H: File> fmt::Debug for ExclusiveGuard<'_, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExclusiveGuard")
            .field("key", &self.key)
            .field("frame", &self.frame)
            .finish()
    }
}

/// `mark_dirty`'s rule, shared by the exclusive guards.
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
pub struct CreatedPage<'p, H: File> {
    // Declared first, so it drops first: the latch is released before the
    // pin's count is lowered.
    guard: ExclusiveGuard<'p, H>,
    pin: Pin<'p, H>,
}

impl<'p, H: File> CreatedPage<'p, H> {
    /// A created page over `guard`, the exclusive latch on `pin`'s frame.
    pub(crate) fn new(guard: ExclusiveGuard<'p, H>, pin: Pin<'p, H>) -> CreatedPage<'p, H> {
        CreatedPage { guard, pin }
    }

    /// Release the latch and keep the pin: from here other threads may read
    /// the page (§6.8 "Root names").
    pub fn release(self) -> Pin<'p, H> {
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

impl<H: File> Deref for CreatedPage<'_, H> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.guard
    }
}

impl<H: File> DerefMut for CreatedPage<'_, H> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.guard
    }
}

impl<H: File> fmt::Debug for CreatedPage<'_, H> {
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
pub struct FrameReservation<'p, H: File> {
    pool: &'p Pool<H>,
    frame: FrameId,
}

impl<'p, H: File> FrameReservation<'p, H> {
    /// A reservation of `frame`, which the caller set `Reserved` under the
    /// table mutex.
    pub(crate) fn reserved(pool: &'p Pool<H>, frame: FrameId) -> FrameReservation<'p, H> {
        FrameReservation { pool, frame }
    }

    /// The reserved frame (§5.4).
    pub fn frame(&self) -> FrameId {
        self.frame
    }

    /// Whether this reservation is of `pool`'s frame.
    pub(crate) fn is_of(&self, pool: &Pool<H>) -> bool {
        core::ptr::eq(self.pool, pool)
    }

    /// Hand the frame to the page table: the caller has just inserted it, so
    /// the reservation no longer owns it and its `Drop` does not run.
    pub(crate) fn consume(self) -> FrameId {
        ManuallyDrop::new(self).frame
    }
}

impl<H: File> Drop for FrameReservation<'_, H> {
    /// Return the frame to the free list (`Free`, key and freed flag
    /// cleared), under the table mutex, taken even if poisoned. A frame that
    /// returns becomes unheld here (U2, card A-3b-0, Work 4), still under
    /// that mutex. Never panics.
    fn drop(&mut self) {
        let mut t = self.pool.lock_table_anyway();
        // Only an engine bug fails this, and `drop` cannot report it: the
        // frame then stays `Reserved`, which `Pool::stats` shows.
        if t.release(self.frame).is_ok() {
            self.pool.became_unheld(&mut t);
        }
    }
}

impl<H: File> fmt::Debug for FrameReservation<'_, H> {
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
pub struct VictimGuard<'p, H: File> {
    content: LatchExclusive<'p>,
    pool: &'p Pool<H>,
    key: PageKey,
    frame: FrameId,
}

impl<'p, H: File> VictimGuard<'p, H> {
    /// A victim guard over `content`, the exclusive latch of `frame`, which
    /// the caller found `Resident` under `key` with no count.
    pub(crate) fn latched(
        content: LatchExclusive<'p>,
        pool: &'p Pool<H>,
        key: PageKey,
        frame: FrameId,
    ) -> VictimGuard<'p, H> {
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
        self.pool.mark_written(&mut self.content, self.key, home)
    }

    /// Pin the frame while its latch is held (§4.1's two-step map-page write:
    /// the pin keeps a second eviction off the frame until the parent's
    /// reference names its new home). Fires `Pinned` holding the latch.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the count is at `u64::MAX` (BUF-NO-WRAP).
    pub fn pin_frame(&self) -> Result<Pin<'p, H>, StoreError> {
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
    pub fn remove_from_table(self) -> Result<Removal<'p, H>, StoreError> {
        let VictimGuard {
            content,
            pool,
            key,
            frame,
        } = self;
        pool.remove_victim(content, key, frame)
    }
}

impl<H: File> Deref for VictimGuard<'_, H> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.content.bytes
    }
}

impl<H: File> DerefMut for VictimGuard<'_, H> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.content.bytes
    }
}

impl<H: File> fmt::Debug for VictimGuard<'_, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VictimGuard")
            .field("key", &self.key)
            .field("frame", &self.frame)
            .finish()
    }
}

/// What [`VictimGuard::remove_from_table`] did (S-STORE §5.2).
#[must_use]
pub enum Removal<'p, H: File> {
    /// The frame left the page table and is `Reserved` for the caller (A-3b's
    /// frame acquisition uses it at once; dropped, it returns to the free
    /// list; §5.2).
    Removed(FrameReservation<'p, H>),
    /// The frame stays in the page table: it was pinned or dirty (§5.2, §5.4).
    Kept,
}

impl<H: File> fmt::Debug for Removal<'_, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Removal::Removed(r) => f.debug_tuple("Removed").field(r).finish(),
            Removal::Kept => f.write_str("Kept"),
        }
    }
}
