//! The buffer pool and its protocols (S-STORE §5.1–§5.4; card A-3a,
//! Work 8): a fixed array of frames, a page table that never grows, pins
//! that look in the page table first, loads through a page's current
//! reference under I-INTEGRITY, creation under the creator's latch, and the
//! step by which a frame leaves the page table.
//!
//! The step labels below (P1–P10, C1–C4, V1–V2, R1–R2) are card A-3a's; their
//! order is what the invariants of `buf` (module doc) and the race tests
//! rest on. The table mutex is a leaf lock (BUF-TABLE-LEAF): no step takes a
//! latch, calls a [`CurrentRef`], makes I/O, fires a hook or waits while it
//! holds it, except the condition-variable wait, which releases it.
//!
//! Card A-3b-0 adds, without moving a step: the reference bit (Work 3), set
//! where a user pin raises a count and cleared where a frame becomes `Free`
//! or `Reserved`; the four transitions U1–U4 that make a held frame unheld
//! (Work 4), two of them in `frame.rs` and two here, each counted by
//! [`Pool::became_unheld`]; and P3's call of the frame acquisition (Work 5),
//! whose own steps A1–A7 are in `acquire.rs`.

use core::fmt;
use core::ops::Range;
use std::collections::TryReserveError;

use nucleus_env::File;

use super::frame::{
    CreatedPage, ExclusiveGuard, FrameContent, FrameId, FrameLatch, FrameReservation, FrameState,
    LatchExclusive, Pin, Removal, VictimGuard,
};
use super::key::PageKey;
#[cfg(feature = "test-hooks")]
use super::sync::TryLockError;
use super::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use super::table::{FrameMeta, Table, bucket_index};
use crate::meta::{self, Ref};
use crate::page::{self, Kind};
use crate::store::StoreError;

use super::sizing::{H_MIN, OWN_PINNING_THREADS, PoolSizing, STEP2_RESERVE_FRAMES};

/// The largest frame count a pool takes: `2^31`, so that every frame id and
/// the bucket count (the next power of two) fit a `u32`.
const MAX_FRAMES: u32 = 1 << 31;

/// The lowest slot that can be a page's home: slots 0–2 are page 0 and the
/// two meta slots (S-STORE §1, §2, §4.4).
const FIRST_PAGE_SLOT: u64 = 3;

/// A pool's size (card A-3a; card A-3b-0, Work 2): `frames` frames
/// (S-STORE §5.1's `F`) of `page_size` bytes for `callers` calling threads.
/// Phase A opens take `F` and `n_c` directly (R-1-memory §4.1, §11); R-1
/// derives them from a budget in Phase B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// `F`, the frame count (§5.1): `1 ≤ frames ≤ 2^31`, and at least the
    /// minimum of §5.4 for `callers` ([`PoolSizing::compute`]).
    pub frames: u32,
    /// The page size: one of `meta::PAGE_SIZES` (§2.1).
    pub page_size: u32,
    /// `n_c`: the threads (or under N10 the tasks that can hold a pin while
    /// they wait) that call into the pool at once (R-1-memory §1, §11). The
    /// minimum counts every pin each can hold (S-STORE §5.4), so a pool that
    /// would be below it is refused; 0 is refused. `Pool::bytes_for` and
    /// `Pool::footprint` do not read it.
    pub callers: u32,
}

/// The source of a page's current reference (S-STORE §4.1, §5.4): the
/// current page map, card A-3c's; card A-3a's tests pass a test double.
pub trait CurrentRef<H: File> {
    /// The key's current reference `(slot, epoch, crc)`: an LPID's current
    /// map entry, or the MapLeaf's ref in its parent; slot 0 when the LPID
    /// is unmapped or the MapLeaf was never made (§4.1).
    ///
    /// A load calls it holding only its count on its `Loading` frame: no
    /// latch, no mutex. It may pin, latch and load map pages through `pool`
    /// (an LPID's MapLeaf, card A-3c), and must not pin `key` itself, which
    /// would wait on its own load. An error ends the load before it is
    /// returned: the frame leaves the page table and nothing is served
    /// (S-STORE §5.4 "The load": a load ends "before its thread restarts or
    /// waits for the flip").
    ///
    /// # Errors
    /// Whatever the map's reads give; the load returns it unchanged.
    fn current_ref(&self, pool: &Pool<H>, key: PageKey) -> Result<Ref, StoreError>;
}

/// What [`Pool::pin`] found (S-STORE §5.4 "What a pin finds").
#[must_use]
pub enum PinOutcome<'p, H: File> {
    /// The page, pinned in its frame (§5.3).
    Pinned(Pin<'p, H>),
    /// No frame held the key and its reference has slot 0: an LPID that is
    /// not mapped, or a MapLeaf never made. The pin holds no frame, and
    /// nothing is left in the page table. Not an error: the caller decides,
    /// as §5.4 and §6.8 say (XX001 for a referenced LPID; a re-read of the
    /// entry on the root-name path).
    Unmapped,
}

/// What [`Pool::create`] did (S-STORE §5.4 "The load", last paragraph).
#[must_use]
pub enum CreateOutcome<'p, H: File> {
    /// The page was made: in the reservation's frame, or in the frame that
    /// held the key freed (§4.2), and its exclusive latch is held.
    Created(CreatedPage<'p, H>),
    /// A frame holds the key live: a pin on it, and the unused reservation.
    /// For a MapLeaf another restart made it first (§5.6 "Restart"); for an
    /// LPID it is damage, the caller's XX001 (§5.4).
    Exists(Pin<'p, H>, FrameReservation<'p, H>),
    /// The key is being loaded: the caller waits with [`Pool::wait_settled`]
    /// and calls `create` again with the reservation (§5.4: "the creation
    /// waits for that load to end and looks again").
    Loading(FrameReservation<'p, H>),
}

/// The pool's frames by state, and its counters (S-STORE §5.1, §5.4; card
/// A-3a; each answer may be stale once returned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// `F` (§5.1).
    pub frames: u32,
    /// Frames on the free list (§5.1).
    pub free: u32,
    /// Frames owned by a reservation (§5.4, §6.4).
    pub reserved: u32,
    /// Frames being loaded (§5.4 "The load").
    pub loading: u32,
    /// Frames holding their page (§5.4).
    pub resident: u32,
    /// Frames of failed or unmapped loads that waiters still count (§5.4).
    pub orphan: u32,
    /// Slot reads issued by loads (§3.3).
    pub reads: u64,
    /// The page table's bucket count, fixed at `F.next_power_of_two()` (§5.7).
    pub buckets: u32,
    /// Clean victims the frame acquisition has taken (§5.2; card A-3b-0).
    pub evictions: u64,
    /// The clock's position, `hand mod F` (§5.2; card A-3b-0).
    pub clock_hand: u32,
    /// Frame acquisitions waiting for an unpin right now (§5.4; card A-3b-0).
    pub unpin_waiters: u32,
}

/// One frame's bookkeeping (S-STORE §5.1; card A-3a and A-3b-0; may be stale
/// once returned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameInfo {
    /// The frame's state (§5.1, §5.4).
    pub state: FrameState,
    /// Its key while `Loading`, `Resident` or `Orphan` (§5.1).
    pub key: Option<PageKey>,
    /// Its pin count (§5.3).
    pub pins: u64,
    /// Whether its page was freed (§4.2).
    pub freed: bool,
    /// The clock's reference bit (§5.2; card A-3b-0, Work 3).
    pub referenced: bool,
}

/// The pool's memory as R-1 accounts it (R-1-memory §4.1 `o_frame`, §5.1,
/// §8; card A-3a reports the frame-header and page-table terms, and card
/// A-3b-0's [`PoolSizing::o_frame`] bounds both per frame, with the
/// image-record list's record that A-3c adds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolFootprint {
    /// The page buffers: `F × page_size` (R-1-memory §5.1).
    pub page_bytes: u64,
    /// The frames' headers: `F × (size_of::<FrameLatch>() +
    /// size_of::<FrameMeta>())`, each latch holding its frame's content
    /// cell (dirty flag, freed flag, content epoch and the buffer's handle;
    /// R-1-memory §4.1).
    pub frame_bytes: u64,
    /// The page table: `F.next_power_of_two()` bucket heads and the free
    /// stack's `F` frame ids (S-STORE §5.7).
    pub table_bytes: u64,
}

/// A named point in the pool's protocols where a test hook fires (card
/// A-3a, Work 8; card A-3b-0, Work 6). Always defined; the hook storage and
/// [`Pool::set_hook`] exist only under the crate's `test-hooks` feature, and
/// without it firing compiles to nothing. A hook fires holding no pool mutex;
/// the latches each point holds are in card A-3a's hook table. S-STORE §15's
/// `t_pin_load_race` names four of them: *missed* is [`HookPoint::MissReserved`], *published*
/// [`HookPoint::LoadPublished`], *resolved* [`HookPoint::LoadResolved`] and
/// *waiting* [`HookPoint::LoadWaiting`].
///
/// A hook with no page fires with key `PageKey::Lpid(0)` (LPID 0 is null,
/// S-STORE §1, so no page has it) and frame `FrameId(u32::MAX)` (no frame has
/// it: `frames ≤ 2^31`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookPoint {
    /// P3 (§5.4 "The load"): a miss holds its reservation (a frame not in the table), before
    /// it looks again and publishes it.
    MissReserved,
    /// P4 (§5.4): the frame is in the table as `Loading`; the loader holds its
    /// count.
    LoadPublished,
    /// P7 (§5.4): the reference read has a non-zero slot; the loader holds its
    /// count.
    LoadResolved,
    /// P8 (§3.3): a whole page was read, before `verify`; the loader holds its count
    /// and the frame's exclusive latch.
    LoadRead,
    /// Where a pin (P2 (b), holding its count on the `Loading` frame) or a
    /// creation ([`Pool::wait_settled`], holding its reservation) is about to
    /// wait for a load (§5.4). No latch.
    LoadWaiting,
    /// P2 (b) (§5.4): a pin's wait for a load ended, before it decides; it holds its
    /// count. No latch.
    WaitWoken,
    /// After any [`Pin`] is made (§5.3); from `create` or `VictimGuard::pin_frame`,
    /// the frame's exclusive latch is held.
    Pinned,
    /// After a [`Pin`]'s count is lowered (§5.3).
    Unpinned,
    /// C3 (§6.8): the reservation's frame is latched exclusively and initialised,
    /// not yet in the table; no pin.
    CreateReady,
    /// R1 (§5.2): the victim's exclusive latch is held; no pin.
    RemoveLatched,
    /// The frame acquisition's step A2 (§5.2; card A-3b-0, Work 5), after a
    /// clean victim left the page table: with the victim's old key and its
    /// frame. The firing thread holds that frame's reservation; no latch, no
    /// mutex.
    Evicted,
    /// Step A3, once per sweep that took no victim: with the key the
    /// acquisition is for (`pin`'s miss) or none. The firing thread holds
    /// nothing: no reservation, no pin, no latch, no mutex.
    SweepEnded,
    /// Step A7, after the acquisition registered as a waiter for an unpin and
    /// before its first wait: with the key it is for or none. The firing
    /// thread holds nothing, and is counted in [`PoolStats::unpin_waiters`].
    UnpinWait,
    /// Step A2, at the start of each sweep step, before the table mutex is
    /// taken (so before the test that ends the sweep): with the key the
    /// acquisition is for (`pin`'s miss) or none, and no frame (card A-3b-0,
    /// Work 6; gate ruling A-3b-0 R1). The firing thread holds nothing: no
    /// reservation, no pin, no latch, no mutex.
    SweepStep,
}

/// A test hook (card A-3a; S-STORE §15's hook-placed interleavings): called
/// with the point, the key and the frame.
pub type Hook = Arc<dyn Fn(HookPoint, PageKey, FrameId) + Send + Sync>;

/// The buffer pool (S-STORE §5.1): `F` frames, the page table and the free
/// list, all allocated by [`Pool::new`] and never grown (BUF-FIXED), over the
/// page file `H` (every page read goes through `nucleus-env`, §2).
pub struct Pool<H: File> {
    /// The page file.
    file: Arc<H>,
    /// The page size.
    page_size: u32,
    /// The page size as a buffer length.
    page_len: usize,
    /// The reserved slots of the replica region (§2.4), never a page home.
    reserved: Range<u64>,
    /// `buckets − 1`, outside the mutex: the bucket count never changes.
    bucket_mask: u32,
    /// One latch per frame, each guarding its frame's content.
    latches: Box<[FrameLatch]>,
    /// The page table, the frames' bookkeeping and the free list.
    table: Mutex<Table>,
    /// Signalled whenever a load ends (P10, cleanup).
    settled: Condvar,
    /// Signalled whenever a held frame becomes unheld (U1–U4, card A-3b-0,
    /// Work 4) while an acquisition waits for it.
    unheld: Condvar,
    /// The numbers the pool was opened with (R-1-memory §8 "The report").
    sizing: PoolSizing,
    /// The test hook (card A-3a, `test-hooks`).
    #[cfg(feature = "test-hooks")]
    hook: Mutex<Option<Hook>>,
}

/// A validated [`PoolConfig`].
#[derive(Debug, Clone, Copy)]
struct Sizes {
    frames: u32,
    page_size: u32,
    buckets: u32,
}

/// Validate `cfg` (card A-3a, `Pool::new`): the page size is allowed and
/// `1 ≤ frames ≤ 2^31`. The minimum of §5.4 is checked after this, by
/// [`PoolSizing::compute`] (card A-3b-0, Work 2), and not here: R-1 charges
/// [`Pool::bytes_for`], which reads only `frames` and `page_size`, before it
/// calls [`Pool::new`].
fn sizes(cfg: PoolConfig) -> Result<Sizes, StoreError> {
    if !meta::is_page_size(cfg.page_size) {
        return Err(StoreError::Invalid(format!(
            "page size {} is not one of {:?}",
            cfg.page_size,
            meta::PAGE_SIZES
        )));
    }
    if cfg.frames < 1 || cfg.frames > MAX_FRAMES {
        return Err(StoreError::Invalid(format!(
            "a pool of {} frames: the frame count must be 1 to 2^31",
            cfg.frames
        )));
    }
    let buckets = cfg
        .frames
        .checked_next_power_of_two()
        .ok_or_else(|| StoreError::Invalid("bucket count past u32".into()))?;
    Ok(Sizes {
        frames: cfg.frames,
        page_size: cfg.page_size,
        buckets,
    })
}

/// The error for a failed allocation at construction (N16: 53200).
fn oom(what: &str, e: TryReserveError) -> StoreError {
    StoreError::OutOfMemory(format!("buffer pool {what}: {e}"))
}

/// A `usize` as a `u64`, saturating (only a footprint sum uses it).
fn wide(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// The error a poisoned table mutex gives.
pub(crate) fn poisoned_table() -> StoreError {
    StoreError::Poisoned("the buffer pool's page table mutex was poisoned by a panic".into())
}

/// The error for a frame found in a state its step does not allow (an
/// engine invariant violation).
fn bad_state(f: FrameId, state: FrameState, where_: &str) -> StoreError {
    StoreError::Invalid(format!("frame {} is {state:?} {where_}", f.0))
}

/// Zero a frame's bytes and write its header (S-STORE §3.1): `kind`, `id`,
/// `epoch`. The page is dirty (it has no home), not freed, in content epoch
/// `epoch`.
fn init_page(c: &mut FrameContent, kind: Kind, id: u64, epoch: u32) -> Result<(), StoreError> {
    c.bytes.fill(0);
    page::write_header(&mut c.bytes, kind, id, epoch)?;
    c.dirty = true;
    c.freed = false;
    c.content_epoch = epoch;
    Ok(())
}

impl<H: File> Pool<H> {
    /// A pool of `cfg.frames` frames over `file` (S-STORE §5.1).
    ///
    /// A-3a's checks first (the page size, `1 ≤ frames ≤ 2^31`), then the
    /// minimum of §5.4 with R-1-memory's values (card A-3b-0, Work 2), then
    /// the allocations. So a pool below its minimum allocates nothing and
    /// does no I/O (§5.4: "refused with 53200 and writes nothing").
    ///
    /// Allocates, each with `try_reserve_exact` and in this order, the frame
    /// metadata, the latches, the buckets, the free stack and the `frames`
    /// zeroed page buffers; nothing in `buf` allocates after this
    /// (BUF-FIXED), but for an error's message.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] unless the page size is allowed and
    /// `1 ≤ frames ≤ 2^31`; [`StoreError::BelowMinimum`] (53200) if `frames`
    /// is below the minimum for `cfg.callers`;
    /// [`StoreError::OutOfMemory`] (53200) if an allocation fails.
    pub fn new(file: Arc<H>, cfg: PoolConfig) -> Result<Pool<H>, StoreError> {
        let s = sizes(cfg)?;
        let reserved = meta::reserved_slots(s.page_size)?;
        let sizing = PoolSizing::compute(
            cfg.frames,
            cfg.callers,
            OWN_PINNING_THREADS,
            STEP2_RESERVE_FRAMES,
            H_MIN,
        )?;
        Self::build(file, s, reserved, sizing)
    }

    /// [`Pool::new`] for a pool below its minimum (card A-3b-0, Work 2;
    /// `test-hooks` only): the same checks but §5.4's, so a pool of a few
    /// frames opens, and [`Pool::sizing`] reports the numbers
    /// [`PoolSizing::below_minimum`] gives.
    ///
    /// It exists so this crate's own tests can exercise the pin protocol and
    /// the clock on pools of a few frames, which the minimum (34 for one
    /// caller) would refuse. No store path calls it, and no workspace crate
    /// other than nucleus-store may reference it (card A-0's reach rule for
    /// `test-hooks`).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] unless the page size is allowed,
    /// `1 ≤ frames ≤ 2^31` and `callers ≥ 1`; [`StoreError::OutOfMemory`]
    /// (53200) if an allocation fails.
    #[cfg(feature = "test-hooks")]
    pub fn new_below_minimum_for_tests(
        file: Arc<H>,
        cfg: PoolConfig,
    ) -> Result<Pool<H>, StoreError> {
        let s = sizes(cfg)?;
        let reserved = meta::reserved_slots(s.page_size)?;
        let sizing = PoolSizing::below_minimum(
            cfg.frames,
            cfg.callers,
            OWN_PINNING_THREADS,
            STEP2_RESERVE_FRAMES,
            H_MIN,
        )?;
        Self::build(file, s, reserved, sizing)
    }

    /// The pool's numbers (R-1-memory §8 "The report": `F`, `R_EPOCH`,
    /// `R_MAP`, `H`, `n_s`, `s` and `o_frame`; card A-3b-0, Work 2). Fixed at
    /// open.
    pub fn sizing(&self) -> PoolSizing {
        self.sizing
    }

    /// Card A-3a's allocations (BUF-FIXED), for a validated configuration
    /// and its sizing. Neither `Pool::new` nor
    /// [`Pool::new_below_minimum_for_tests`] allocates before it is called.
    fn build(
        file: Arc<H>,
        s: Sizes,
        reserved: Range<u64>,
        sizing: PoolSizing,
    ) -> Result<Pool<H>, StoreError> {
        let too_wide = || StoreError::Invalid("pool size past usize".into());
        let frames = usize::try_from(s.frames).map_err(|_| too_wide())?;
        let buckets_len = usize::try_from(s.buckets).map_err(|_| too_wide())?;
        let page_len = usize::try_from(s.page_size).map_err(|_| too_wide())?;

        let mut meta_v: Vec<FrameMeta> = Vec::new();
        meta_v
            .try_reserve_exact(frames)
            .map_err(|e| oom("frame metadata", e))?;
        let mut latches: Vec<FrameLatch> = Vec::new();
        latches
            .try_reserve_exact(frames)
            .map_err(|e| oom("frame latches", e))?;
        let mut buckets: Vec<Option<FrameId>> = Vec::new();
        buckets
            .try_reserve_exact(buckets_len)
            .map_err(|e| oom("page table buckets", e))?;
        let mut free: Vec<FrameId> = Vec::new();
        free.try_reserve_exact(frames)
            .map_err(|e| oom("free list", e))?;
        for _ in 0..frames {
            let mut bytes: Vec<u8> = Vec::new();
            bytes
                .try_reserve_exact(page_len)
                .map_err(|e| oom("page buffer", e))?;
            bytes.resize(page_len, 0);
            latches.push(FrameLatch::new(FrameContent {
                bytes: bytes.into_boxed_slice(),
                dirty: false,
                freed: false,
                content_epoch: 0,
            }));
        }
        meta_v.resize(frames, FrameMeta::FREE);
        buckets.resize(buckets_len, None);
        free.extend((0..s.frames).rev().map(FrameId));
        let table = Table::new(
            buckets.into_boxed_slice(),
            meta_v.into_boxed_slice(),
            free.into_boxed_slice(),
        )?;
        Ok(Pool {
            file,
            page_size: s.page_size,
            page_len,
            reserved,
            bucket_mask: s.buckets - 1,
            latches: latches.into_boxed_slice(),
            table: Mutex::new(table),
            settled: Condvar::new(),
            unheld: Condvar::new(),
            sizing,
            #[cfg(feature = "test-hooks")]
            hook: Mutex::new(None),
        })
    }

    /// `F`, the frame count (§5.1).
    pub fn frames(&self) -> u32 {
        // `new` built one latch per frame of a `u32` count.
        u32::try_from(self.latches.len()).unwrap_or(u32::MAX)
    }

    /// The page size (§2.1).
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// The page file, through which every page read goes (§2).
    pub fn file(&self) -> &Arc<H> {
        &self.file
    }

    /// Lock the table.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if a panic poisoned the mutex.
    pub(crate) fn lock_table(&self) -> Result<MutexGuard<'_, Table>, StoreError> {
        self.table.lock().map_err(|_| poisoned_table())
    }

    /// Lock the table even if poisoned (for `Drop`, which cannot fail).
    pub(crate) fn lock_table_anyway(&self) -> MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wait for a load to end: release the table mutex, sleep on the
    /// condition variable, and lock it again.
    fn wait<'t>(&self, t: MutexGuard<'t, Table>) -> Result<MutexGuard<'t, Table>, StoreError> {
        self.settled.wait(t).map_err(|_| poisoned_table())
    }

    /// Wait for a held frame to become unheld (card A-3b-0, Work 5, A7):
    /// release the table mutex, wait on `unheld`, and lock it again. A
    /// poisoned mutex gives the guard back in `Err`, so the caller, a
    /// registered waiter, can take itself off the count before it returns
    /// the error.
    pub(crate) fn wait_for_unheld<'t>(
        &self,
        t: MutexGuard<'t, Table>,
    ) -> Result<MutexGuard<'t, Table>, MutexGuard<'t, Table>> {
        self.unheld.wait(t).map_err(PoisonError::into_inner)
    }

    /// U1–U4's second half (card A-3b-0, Work 4): a held frame has just
    /// become unheld, under the table mutex, which the caller still holds as
    /// `t`. Counts the event and, while an acquisition waits for an unpin,
    /// wakes every waiter. The four callers are [`Pin::drop`] (U1),
    /// [`FrameReservation::drop`] (U2), the cleanup of a load (U3) and a
    /// pin's Orphan path (U4); no other site changes `unheld_events`, and a
    /// frame that `remove_from_table` makes `Reserved` stays held.
    pub(crate) fn became_unheld(&self, t: &mut Table) {
        t.unheld_events = t.unheld_events.wrapping_add(1);
        if t.unpin_waiters > 0 {
            self.unheld.notify_all();
        }
    }

    /// Frame `f`'s latch.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range.
    pub(crate) fn latch(&self, f: FrameId) -> Result<&FrameLatch, StoreError> {
        usize::try_from(f.0)
            .ok()
            .and_then(|i| self.latches.get(i))
            .ok_or_else(|| {
                StoreError::Invalid(format!(
                    "frame {} is out of range for a pool of {} frames",
                    f.0,
                    self.latches.len()
                ))
            })
    }

    /// The byte offset of `slot` if it can be a page's home (S-STORE §4.4:
    /// "Slots 0–2 and the reserved slots (§2.4) are never page homes"), and
    /// its whole page is addressable in a `u64`; `None` otherwise.
    fn home_offset(&self, slot: u64) -> Option<u64> {
        if slot < FIRST_PAGE_SLOT || self.reserved.contains(&slot) {
            return None;
        }
        let ps = u64::from(self.page_size);
        let off = slot.checked_mul(ps)?;
        off.checked_add(ps)?;
        Some(off)
    }

    /// Fire the test hook at `point`, if one is set: its `Arc` is cloned
    /// under the hook's own mutex, which is released before the call, so a
    /// hook never runs under a pool mutex. Compiles to nothing without the
    /// `test-hooks` feature.
    pub(crate) fn fire(&self, point: HookPoint, key: PageKey, frame: FrameId) {
        #[cfg(feature = "test-hooks")]
        {
            let hook = self
                .hook
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            if let Some(hook) = hook {
                hook(point, key, frame);
            }
        }
        #[cfg(not(feature = "test-hooks"))]
        let _ = (point, key, frame);
    }

    /// Pin `key`'s page, loading it on a miss (S-STORE §5.4 "What a pin
    /// finds" and "The load"; card A-3a, steps P1–P10).
    ///
    /// The page table first: a `Resident` frame is pinned with no reference
    /// read (P2 (a)); a `Loading` one is waited for, holding only a count, and
    /// pinned if its load made it hold the page, or left if the load failed
    /// (P2 (b)). Only when no frame holds the key does the pin take a frame,
    /// publish it in the table as `Loading` if the key is still absent
    /// (P3–P4), and only then read the key's current reference (P5) through
    /// `map`: slot 0 is [`PinOutcome::Unmapped`] (P6), a slot that is never a
    /// page home is XX001 with no read (P7), and the page read (P8) is served
    /// only if it is whole, passes `page::verify` against the reference and
    /// the key's header id (§3.3) and has a kind the key admits (P9). Any
    /// failure takes the frame out of the table and serves nothing.
    ///
    /// Precondition (§5.3, §5.4): `pin` may wait for another thread's load of
    /// `key`, which reads a page and takes the key's map latches shared, and
    /// a pin that misses may wait for an unpin, since its frame is the frame
    /// acquisition's waiting form (§5.4: "every frame is pinned"; card
    /// A-3b-0, Work 5). A caller holding latches calls it only where §5.3
    /// allows a page read and a wait for an unpin, and never while it holds a
    /// map latch; elsewhere it uses [`Pool::pin_resident`], and on `None`
    /// releases its latches and restarts.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] for `Lpid(0)` (before anything is touched) or a
    /// count at `u64::MAX`; [`StoreError::PoolStarved`] when no frame is free
    /// and every unpinned frame is dirty, which only A-3c's dirty eviction
    /// resolves (§5.2); [`StoreError::Corrupt`] (XX001) for
    /// a reference naming slot 1, 2, a reserved slot or a slot past `u64`, a
    /// short read, a §3.3 mismatch or a kind the key does not admit;
    /// [`StoreError::Env`] (58030) if the read fails; whatever `map` returns.
    pub fn pin(
        &self,
        key: PageKey,
        map: &impl CurrentRef<H>,
    ) -> Result<PinOutcome<'_, H>, StoreError> {
        // P1.
        let id = key.header_id()?;
        loop {
            // P2.
            let mut t = self.lock_table()?;
            if let Some(f) = t.lookup(key)? {
                match t.meta(f)?.state {
                    // (a) Resident: no reference is read.
                    FrameState::Resident => {
                        t.raise(f)?;
                        // A user pin sets the clock's bit (card A-3b-0, Work 3).
                        t.meta_mut(f)?.referenced = true;
                        drop(t);
                        let pin = Pin::counted(self, f, key);
                        self.fire(HookPoint::Pinned, key, f);
                        return Ok(PinOutcome::Pinned(pin));
                    }
                    // (b) Loading: wait for that load, holding a count.
                    FrameState::Loading => {
                        t.raise(f)?;
                        drop(t);
                        self.fire(HookPoint::LoadWaiting, key, f);
                        let mut t = self.lock_table()?;
                        while t.meta(f)?.state == FrameState::Loading {
                            t = self.wait(t)?;
                        }
                        drop(t);
                        self.fire(HookPoint::WaitWoken, key, f);
                        let mut t = self.lock_table()?;
                        match t.meta(f)?.state {
                            // A counted frame cannot change key.
                            FrameState::Resident => {
                                // The waiter is served: a user pin sets the
                                // clock's bit (card A-3b-0, Work 3).
                                t.meta_mut(f)?.referenced = true;
                                drop(t);
                                let pin = Pin::counted(self, f, key);
                                self.fire(HookPoint::Pinned, key, f);
                                return Ok(PinOutcome::Pinned(pin));
                            }
                            // The load failed: never take its frame.
                            FrameState::Orphan => {
                                if t.lower(f)? == 0 {
                                    t.release(f)?;
                                    // U4 (card A-3b-0, Work 4): the last count
                                    // left an Orphan frame.
                                    self.became_unheld(&mut t);
                                }
                                drop(t);
                                continue;
                            }
                            other => return Err(bad_state(f, other, "after a wait for its load")),
                        }
                    }
                    other => return Err(bad_state(f, other, "in the page table")),
                }
            }
            // (c) Absent.
            drop(t);

            // P3: the frame acquisition in its waiting form, for this key, so
            // that `pin` may wait for an unpin (card A-3b-0, Work 5). Its
            // `None` cannot occur there.
            let res = self.acquire(true, Some(key))?.ok_or_else(|| {
                StoreError::Invalid("a waiting frame acquisition returned no frame".into())
            })?;
            let f = res.frame();
            self.fire(HookPoint::MissReserved, key, f);

            // P4: publish the frame as Loading only if the key is still
            // absent, under the same mutex hold as the look.
            let mut t = self.lock_table()?;
            if t.lookup(key)?.is_some() {
                drop(t);
                // The reservation is never dropped under the table mutex:
                // its drop takes that mutex.
                drop(res);
                continue;
            }
            t.insert(key, f)?;
            let m = t.meta_mut(f)?;
            m.state = FrameState::Loading;
            m.key = Some(key);
            m.pins = 1;
            m.freed = false;
            drop(t);
            let f = res.consume();
            self.fire(HookPoint::LoadPublished, key, f);

            return self.load(f, key, id, map);
        }
    }

    /// Steps P5–P10 of a pin's load into `f`, which is `Loading` under `key`
    /// with the loader's count.
    fn load(
        &self,
        f: FrameId,
        key: PageKey,
        id: u64,
        map: &impl CurrentRef<H>,
    ) -> Result<PinOutcome<'_, H>, StoreError> {
        // P5: the reference, holding only the count on f.
        let r = match map.current_ref(self, key) {
            Ok(r) => r,
            Err(e) => return self.abandon(f, key, false, e),
        };

        // P6: unmapped. No slot is read.
        if r.slot == 0 {
            self.end_load(f, key, false)?;
            return Ok(PinOutcome::Unmapped);
        }

        // P7: only a page slot is read.
        self.fire(HookPoint::LoadResolved, key, f);
        let Some(off) = self.home_offset(r.slot) else {
            let e = StoreError::Corrupt(format!(
                "the reference names slot {}, which is never a page home (S-STORE §4.4)",
                r.slot
            ));
            return self.abandon(f, key, false, e);
        };

        // P8: the read, under the frame's exclusive latch (only a
        // `try_victim`, which backs off from a Loading frame, can hold it).
        let mut c = match self.latch(f).and_then(FrameLatch::exclusive) {
            Ok(c) => c,
            Err(e) => return self.abandon(f, key, false, e),
        };
        let n = match self.file.read_at(off, &mut c.bytes) {
            Ok(n) => n,
            Err(e) => {
                drop(c);
                return self.abandon(f, key, true, StoreError::Env(e));
            }
        };
        if n < self.page_len {
            drop(c);
            let e = StoreError::Corrupt(format!(
                "slot {} read {n} of {} bytes: the reference names a page past the end of \
                 the file (S-STORE §3.3)",
                r.slot, self.page_len
            ));
            return self.abandon(f, key, true, e);
        }
        self.fire(HookPoint::LoadRead, key, f);

        // P9: I-INTEGRITY, and the kind the key admits. The bytes are never
        // served unless both pass.
        let checked = page::verify(&c.bytes, &r, id).and_then(|h| {
            if key.admits(h.kind) {
                Ok(())
            } else {
                Err(StoreError::Corrupt(format!(
                    "a {:?} page read for {key:?} (S-STORE §3.1)",
                    h.kind
                )))
            }
        });
        if let Err(e) = checked {
            drop(c);
            return self.abandon(f, key, true, e);
        }
        c.dirty = false;
        c.freed = false;
        c.content_epoch = r.epoch;
        drop(c);

        // P10.
        let mut t = self.lock_table()?;
        let m = t.meta_mut(f)?;
        m.state = FrameState::Resident;
        // The loader is served a page: a user pin sets the clock's bit
        // (card A-3b-0, Work 3).
        m.referenced = true;
        t.reads = t.reads.saturating_add(1);
        self.settled.notify_all();
        drop(t);
        let pin = Pin::counted(self, f, key);
        self.fire(HookPoint::Pinned, key, f);
        Ok(PinOutcome::Pinned(pin))
    }

    /// End a failed load into `f` (the cleanup), then return `e`.
    fn abandon<T>(
        &self,
        f: FrameId,
        key: PageKey,
        read: bool,
        e: StoreError,
    ) -> Result<T, StoreError> {
        self.end_load(f, key, read)?;
        Err(e)
    }

    /// The cleanup of a load that serves nothing (latch already dropped):
    /// count the read if one was issued, take `f` out of the table, lower the
    /// loader's count, and make the frame `Free` if no waiter counts it,
    /// `Orphan` (keeping its key) otherwise; then wake every waiter.
    fn end_load(&self, f: FrameId, key: PageKey, read: bool) -> Result<(), StoreError> {
        let mut t = self.lock_table()?;
        if read {
            t.reads = t.reads.saturating_add(1);
        }
        t.remove(key, f)?;
        if t.lower(f)? == 0 {
            t.release(f)?;
            // U3 (card A-3b-0, Work 4): the loader's was the last count.
            self.became_unheld(&mut t);
        } else {
            t.meta_mut(f)?.state = FrameState::Orphan;
        }
        self.settled.notify_all();
        Ok(())
    }

    /// Pin `key`'s page only if a frame holds it `Resident` (S-STORE §5.3,
    /// §5.4): no reference read, no I/O, no wait. A page being loaded is not
    /// resident. The form for a thread under latches where §5.3 allows no
    /// page read; on `None` it releases its latches and restarts.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] for `Lpid(0)` or a count at `u64::MAX`.
    pub fn pin_resident(&self, key: PageKey) -> Result<Option<Pin<'_, H>>, StoreError> {
        key.header_id()?;
        let mut t = self.lock_table()?;
        let Some(f) = t.lookup(key)? else {
            return Ok(None);
        };
        if t.meta(f)?.state != FrameState::Resident {
            return Ok(None);
        }
        t.raise(f)?;
        // A user pin sets the clock's bit (card A-3b-0, Work 3).
        t.meta_mut(f)?.referenced = true;
        drop(t);
        let pin = Pin::counted(self, f, key);
        self.fire(HookPoint::Pinned, key, f);
        Ok(Some(pin))
    }

    /// Pin frame `f` if it is `Resident`, with its key; `None` otherwise. For
    /// callers that scan frames (A-7b's step 2, §13 step 2; §4.1's two-step
    /// map write). Not a use of the page: it sets no reference bit, so the
    /// clock gives the frame no second chance for it (card A-3b-0, Work 3).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range or its count is at
    /// `u64::MAX`.
    pub fn pin_frame(&self, f: FrameId) -> Result<Option<Pin<'_, H>>, StoreError> {
        let mut t = self.lock_table()?;
        let m = *t.meta(f)?;
        if m.state != FrameState::Resident {
            return Ok(None);
        }
        let key = m
            .key
            .ok_or_else(|| StoreError::Invalid(format!("resident frame {} has no key", f.0)))?;
        t.raise(f)?;
        drop(t);
        let pin = Pin::counted(self, f, key);
        self.fire(HookPoint::Pinned, key, f);
        Ok(Some(pin))
    }

    /// Take a frame for the caller (card A-3a; card A-3b-0, Work 5): the
    /// frame a split reserves before its first mutation (§5.4's
    /// "reservation's pages", §6.4), or a pin's miss (P3). The free list
    /// first; then the clock over the clean, unpinned frames, taken with
    /// `try_latch` and never blocked on (§5.2); then, if no frame was found,
    /// it waits for an unpin (§5.4: "A thread that cannot pin a frame because
    /// every frame is pinned waits for an unpin").
    ///
    /// §5.3's rule: call it holding no latch, or from a gated operation past
    /// its first mutation, which cannot restart (§5.6 "Restart"); which a
    /// caller is, A-4a decides. A caller that holds a latch and may still
    /// restart calls [`Pool::try_reserve`] instead.
    ///
    /// # Errors
    /// [`StoreError::PoolStarved`] when no frame is free and every unpinned
    /// frame is dirty: at this card no dirty victim is written (A-3c writes
    /// them, §5.2); [`StoreError::Poisoned`] if the table mutex was poisoned.
    pub fn reserve(&self) -> Result<FrameReservation<'_, H>, StoreError> {
        match self.acquire(true, None)? {
            Some(res) => Ok(res),
            None => Err(StoreError::Invalid(
                "a waiting frame acquisition returned no frame".into(),
            )),
        }
    }

    /// [`Pool::reserve`] in the form that never waits (card A-3b-0, Work 5;
    /// S-STORE §5.3, §5.4): it looks at the free list and runs the clock once,
    /// and neither retries nor waits for an unpin. `Ok(None)` means the caller
    /// releases its latches and restarts (§5.3: "with no evictable frame it
    /// releases its latches and restarts"): at the pool floor a thread that
    /// holds a latch and may still restart never waits for an unpin.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the table mutex or a latch was poisoned.
    pub fn try_reserve(&self) -> Result<Option<FrameReservation<'_, H>>, StoreError> {
        self.acquire(false, None)
    }

    /// Make `key`'s page in the reservation's frame (S-STORE §5.4 "The load",
    /// last paragraph; §6.4, §6.8; card A-3a, steps C1–C4), born latched: the
    /// frame is zeroed, given its header (`kind`, the key's id, `epoch`) and
    /// published `Resident` while its creator holds its exclusive latch, so
    /// no other thread reads it before [`CreatedPage::release`]
    /// (BUF-BORN-LATCHED). A key that a frame already holds gets no second
    /// frame: `Loading` while it is being loaded, the freed page remade in
    /// its own frame (§4.2), or `Exists` for a live one.
    ///
    /// Precondition: the caller allocated `key` (an LPID it took under the
    /// free-set lock, §4.2, §5.6; or a MapLeaf whose parent reference it found
    /// null under its MapInner's latch with no frame for it in the table,
    /// §5.6 "Restart"). `create` reads no reference. A frame that holds the
    /// key freed is claimed under the same mutex hold that finds it freed, so
    /// two creations of one freed key never both remake it: the second gets
    /// `Exists`.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `res` belongs to another pool, the key has
    /// no header id, the key does not admit `kind`, or a count is at
    /// `u64::MAX` (the reservation then returns to the free list by its
    /// drop); [`StoreError::Poisoned`] if a latch or the mutex was poisoned.
    pub fn create<'p>(
        &'p self,
        res: FrameReservation<'p, H>,
        key: PageKey,
        kind: Kind,
        epoch: u32,
    ) -> Result<CreateOutcome<'p, H>, StoreError> {
        // C1.
        if !res.is_of(self) {
            return Err(StoreError::Invalid(
                "the reservation belongs to another pool".into(),
            ));
        }
        let id = key.header_id()?;
        if !key.admits(kind) {
            return Err(StoreError::Invalid(format!(
                "{key:?} cannot be a {kind:?} page (S-STORE §3.1)"
            )));
        }

        // C2: latch and initialise the reserved frame, out of the table
        // (only a `try_victim`, which backs off from a Reserved frame, can
        // hold its latch).
        let r = res.frame();
        let mut c = self.latch(r)?.exclusive()?;
        init_page(&mut c, kind, id, epoch)?;

        // C3.
        self.fire(HookPoint::CreateReady, key, r);

        // C4.
        let mut t = self.lock_table()?;
        let Some(g) = t.lookup(key)? else {
            t.insert(key, r)?;
            let m = t.meta_mut(r)?;
            m.state = FrameState::Resident;
            m.key = Some(key);
            m.pins = 1;
            m.freed = false;
            // A user pin sets the clock's bit (card A-3b-0, Work 3).
            m.referenced = true;
            drop(t);
            let r = res.consume();
            let pin = Pin::counted(self, r, key);
            let guard = ExclusiveGuard::held(c, self, key, r);
            self.fire(HookPoint::Pinned, key, r);
            return Ok(CreateOutcome::Created(CreatedPage::new(guard, pin)));
        };
        let m = *t.meta(g)?;
        match m.state {
            FrameState::Loading => {
                drop(t);
                drop(c);
                Ok(CreateOutcome::Loading(res))
            }
            FrameState::Resident if m.freed => {
                t.raise(g)?;
                // Claimed in the hold that decided: a second creation of the
                // key now finds the frame live (`Exists`) and never remakes
                // the page over this one's.
                let m = t.meta_mut(g)?;
                m.freed = false;
                // A user pin sets the clock's bit (card A-3b-0, Work 3).
                m.referenced = true;
                drop(t);
                let pin = Pin::counted(self, g, key);
                drop(c);
                drop(res);
                // A freed frame's latch holders hold nothing else: stale-name
                // threads (§6.8) and `try_victim`.
                let mut gc = self.latch(g)?.exclusive()?;
                init_page(&mut gc, kind, id, epoch)?;
                let guard = ExclusiveGuard::held(gc, self, key, g);
                self.fire(HookPoint::Pinned, key, g);
                Ok(CreateOutcome::Created(CreatedPage::new(guard, pin)))
            }
            FrameState::Resident => {
                t.raise(g)?;
                // A user pin sets the clock's bit (card A-3b-0, Work 3).
                t.meta_mut(g)?.referenced = true;
                drop(t);
                drop(c);
                let pin = Pin::counted(self, g, key);
                self.fire(HookPoint::Pinned, key, g);
                Ok(CreateOutcome::Exists(pin, res))
            }
            other => Err(bad_state(g, other, "in the page table")),
        }
    }

    /// Wait until `key` is not being loaded (S-STORE §5.4: a creation that
    /// finds its page being loaded "waits for that load to end and looks
    /// again, holding no map latch"). Fires `LoadWaiting` before it waits,
    /// holding no mutex (§15's *waiting*, `t_pin_load_race` (f)).
    ///
    /// Precondition: the caller holds no latch that a load of `key` takes
    /// through its [`CurrentRef`] (for an LPID, its MapLeaf's; for a MapLeaf,
    /// its MapInner path's).
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the table mutex was poisoned.
    pub fn wait_settled(&self, key: PageKey) -> Result<(), StoreError> {
        let t = self.lock_table()?;
        let Some(f) = loading_frame(&t, key)? else {
            return Ok(());
        };
        drop(t);
        self.fire(HookPoint::LoadWaiting, key, f);
        let mut t = self.lock_table()?;
        while loading_frame(&t, key)?.is_some() {
            t = self.wait(t)?;
        }
        Ok(())
    }

    /// Take frame `f` as an eviction victim if it is `Resident` with no pin
    /// (S-STORE §5.2: "The evictor takes victims with `try_latch` and never
    /// blocks on a victim"; steps V1–V2). It never blocks.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range;
    /// [`StoreError::Poisoned`] if its latch or the mutex was poisoned.
    pub fn try_victim(&self, f: FrameId) -> Result<Option<VictimGuard<'_, H>>, StoreError> {
        // V1.
        let Some(c) = self.latch(f)?.try_exclusive()? else {
            return Ok(None);
        };
        // V2.
        let t = self.lock_table()?;
        let m = *t.meta(f)?;
        drop(t);
        match (m.state, m.key) {
            (FrameState::Resident, Some(key)) if m.pins == 0 => {
                Ok(Some(VictimGuard::latched(c, self, key, f)))
            }
            _ => {
                drop(c);
                Ok(None)
            }
        }
    }

    /// `VictimGuard::pin_frame`: raise `f`'s count while its exclusive latch
    /// is held, and fire `Pinned` still holding it.
    pub(crate) fn pin_latched_victim(
        &self,
        f: FrameId,
        key: PageKey,
    ) -> Result<Pin<'_, H>, StoreError> {
        let mut t = self.lock_table()?;
        t.raise(f)?;
        drop(t);
        let pin = Pin::counted(self, f, key);
        self.fire(HookPoint::Pinned, key, f);
        Ok(pin)
    }

    /// `VictimGuard::remove_from_table` (steps R1–R2), with the victim's
    /// exclusive latch `content`.
    pub(crate) fn remove_victim<'p>(
        &'p self,
        content: LatchExclusive<'p>,
        key: PageKey,
        f: FrameId,
    ) -> Result<Removal<'p, H>, StoreError> {
        // R1.
        self.fire(HookPoint::RemoveLatched, key, f);
        // R2: only an unpinned, clean frame leaves the table.
        let mut t = self.lock_table()?;
        let m = *t.meta(f)?;
        if m.state != FrameState::Resident || m.key != Some(key) {
            return Err(bad_state(f, m.state, "under a victim's latch"));
        }
        if m.pins == 0 && !content.dirty {
            t.remove(key, f)?;
            let m = t.meta_mut(f)?;
            m.state = FrameState::Reserved;
            m.key = None;
            m.freed = false;
            // A `Reserved` frame's reference bit is clear (card A-3b-0,
            // Work 3): a load or a creation into it sets the bit afresh.
            m.referenced = false;
            drop(t);
            drop(content);
            return Ok(Removal::Removed(FrameReservation::reserved(self, f)));
        }
        drop(t);
        drop(content);
        Ok(Removal::Kept)
    }

    /// `mark_written`'s checks and effect (BUF-LEAVE-CLEAN): the page is clean
    /// only if the caller's home is a page slot, the page is not freed, the
    /// home's epoch is the content epoch, the bytes pass §3.3 against the
    /// home and the key's header id, and the key admits their kind.
    pub(crate) fn mark_written(
        &self,
        c: &mut FrameContent,
        key: PageKey,
        home: Ref,
    ) -> Result<(), StoreError> {
        if self.home_offset(home.slot).is_none() {
            return Err(StoreError::Invalid(format!(
                "slot {} is never a page home (S-STORE §4.4)",
                home.slot
            )));
        }
        if c.freed {
            return Err(StoreError::Invalid(format!(
                "{key:?} was freed and is never written (S-STORE §4.2)"
            )));
        }
        if home.epoch != c.content_epoch {
            return Err(StoreError::Invalid(format!(
                "the home's epoch {} is not the content epoch {}",
                home.epoch, c.content_epoch
            )));
        }
        let h = page::verify(&c.bytes, &home, key.header_id()?).map_err(|e| {
            StoreError::Invalid(format!(
                "the bytes do not match their home (S-STORE §3.3): {e}"
            ))
        })?;
        if !key.admits(h.kind) {
            return Err(StoreError::Invalid(format!(
                "a {:?} page written for {key:?} (S-STORE §3.1)",
                h.kind
            )));
        }
        c.dirty = false;
        Ok(())
    }

    /// `mark_freed`'s effect: clean and freed, and the table's mirror set,
    /// under the table mutex taken under the frame's latch.
    pub(crate) fn mark_freed(&self, c: &mut FrameContent, f: FrameId) -> Result<(), StoreError> {
        let mut t = self.lock_table()?;
        let m = t.meta_mut(f)?;
        c.dirty = false;
        c.freed = true;
        m.freed = true;
        Ok(())
    }

    /// The frames by state and the counters (S-STORE §5.1, §5.2, §5.4; each
    /// diagnostic takes the table mutex briefly; the answer may be stale once
    /// returned). Card A-3b-0, Work 7 adds the eviction count, the clock's
    /// position and the acquisitions waiting for an unpin: table-only reads.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the free list's length is not the number of
    /// `Free` frames (an engine invariant violation), or the pool has no
    /// frame; [`StoreError::Poisoned`] if the mutex was poisoned.
    pub fn stats(&self) -> Result<PoolStats, StoreError> {
        let t = self.lock_table()?;
        let mut s = PoolStats {
            frames: 0,
            free: 0,
            reserved: 0,
            loading: 0,
            resident: 0,
            orphan: 0,
            reads: t.reads,
            buckets: u32::try_from(t.bucket_count()).unwrap_or(u32::MAX),
            evictions: t.evictions,
            clock_hand: t.clock_hand()?,
            unpin_waiters: t.unpin_waiters,
        };
        for m in t.metas() {
            s.frames = s.frames.saturating_add(1);
            let n = match m.state {
                FrameState::Free => &mut s.free,
                FrameState::Reserved => &mut s.reserved,
                FrameState::Loading => &mut s.loading,
                FrameState::Resident => &mut s.resident,
                FrameState::Orphan => &mut s.orphan,
            };
            *n = n.saturating_add(1);
        }
        if s.free != t.free_len() {
            return Err(StoreError::Invalid(format!(
                "{} frames are Free but the free list holds {}",
                s.free,
                t.free_len()
            )));
        }
        Ok(s)
    }

    /// Frame `f`'s bookkeeping (§5.1, §5.2), a table-only read.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range.
    pub fn frame_info(&self, f: FrameId) -> Result<FrameInfo, StoreError> {
        let t = self.lock_table()?;
        let m = *t.meta(f)?;
        Ok(FrameInfo {
            state: m.state,
            key: m.key,
            pins: m.pins,
            freed: m.freed,
            referenced: m.referenced,
        })
    }

    /// The frame the page table holds for `key`, if any: a lookup, no pin
    /// (§5.1).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the table's chain is broken.
    pub fn frame_of(&self, key: PageKey) -> Result<Option<FrameId>, StoreError> {
        self.lock_table()?.lookup(key)
    }

    /// The page-table bucket `key` lands in: its hash masked by the bucket
    /// count less one (§5.1, §5.7). No lock: the bucket count never changes.
    pub fn bucket_of(&self, key: PageKey) -> u32 {
        bucket_index(key, self.bucket_mask)
    }

    /// The memory a pool of `cfg` holds, computed from `cfg` alone, without
    /// allocating (R-1-memory §2.4 M1: R-1 charges it before it calls
    /// [`Pool::new`]).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] for a configuration [`Pool::new`] refuses, or
    /// a size past `u64`.
    pub fn bytes_for(cfg: PoolConfig) -> Result<PoolFootprint, StoreError> {
        let s = sizes(cfg)?;
        let past = || StoreError::Invalid("pool footprint past u64".into());
        let frames = u64::from(s.frames);
        let per_frame =
            u64::try_from(size_of::<FrameLatch>() + size_of::<FrameMeta>()).map_err(|_| past())?;
        let head = u64::try_from(size_of::<Option<FrameId>>()).map_err(|_| past())?;
        let id = u64::try_from(size_of::<FrameId>()).map_err(|_| past())?;
        let page_bytes = frames
            .checked_mul(u64::from(s.page_size))
            .ok_or_else(past)?;
        let frame_bytes = frames.checked_mul(per_frame).ok_or_else(past)?;
        let table_bytes = u64::from(s.buckets)
            .checked_mul(head)
            .and_then(|b| frames.checked_mul(id).and_then(|f| b.checked_add(f)))
            .ok_or_else(past)?;
        Ok(PoolFootprint {
            page_bytes,
            frame_bytes,
            table_bytes,
        })
    }

    /// The memory this pool holds, measured from its allocations' lengths:
    /// the page buffers' lengths (read under each frame's latch shared, in
    /// turn, so call it holding no latch), the latches and metadata, the
    /// buckets and the free stack. Independent of [`Pool::bytes_for`]'s
    /// arithmetic, which it must equal (R-1-memory §4.1, §8).
    pub fn footprint(&self) -> PoolFootprint {
        let page_bytes = self
            .latches
            .iter()
            .fold(0u64, |sum, l| sum.saturating_add(wide(l.page_len())));
        let latches = wide(self.latches.len());
        let t = self.lock_table_anyway();
        let (metas, buckets, free) = (
            wide(t.frames()),
            wide(t.bucket_count()),
            wide(t.free_capacity()),
        );
        drop(t);
        let times = |n: u64, size: usize| n.saturating_mul(wide(size));
        PoolFootprint {
            page_bytes,
            frame_bytes: times(latches, size_of::<FrameLatch>())
                .saturating_add(times(metas, size_of::<FrameMeta>())),
            table_bytes: times(buckets, size_of::<Option<FrameId>>())
                .saturating_add(times(free, size_of::<FrameId>())),
        }
    }

    /// Install or remove the test hook (`test-hooks` only; S-STORE §15's
    /// hook-placed interleavings).
    #[cfg(feature = "test-hooks")]
    pub fn set_hook(&self, hook: Option<Hook>) {
        *self.hook.lock().unwrap_or_else(PoisonError::into_inner) = hook;
    }

    /// Set a `Resident` frame's pin count to `n`: fault injection for a count
    /// no caller can reach (§5.3's counts; test `pin_count_overflow`;
    /// `test-hooks` only).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range or not `Resident`.
    #[cfg(feature = "test-hooks")]
    pub fn force_pins(&self, f: FrameId, n: u64) -> Result<(), StoreError> {
        let mut t = self.lock_table()?;
        let m = t.meta_mut(f)?;
        if m.state != FrameState::Resident {
            return Err(bad_state(f, m.state, "where force_pins needs Resident"));
        }
        m.pins = n;
        Ok(())
    }

    /// Whether the table mutex is free (`try_lock` would not block): what a
    /// test checks while another thread is paused mid-load (test
    /// `independent_loads`; `test-hooks` only; S-STORE §4.2's leaf-lock rank).
    #[cfg(feature = "test-hooks")]
    pub fn table_free(&self) -> bool {
        match self.table.try_lock() {
            Ok(_) | Err(TryLockError::Poisoned(_)) => true,
            Err(TryLockError::WouldBlock) => false,
        }
    }
}

/// The frame loading `key`, if the table holds it `Loading`.
fn loading_frame(t: &Table, key: PageKey) -> Result<Option<FrameId>, StoreError> {
    match t.lookup(key)? {
        Some(f) if t.meta(f)?.state == FrameState::Loading => Ok(Some(f)),
        _ => Ok(None),
    }
}

impl<H: File> fmt::Debug for PinOutcome<'_, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinOutcome::Pinned(p) => f.debug_tuple("Pinned").field(p).finish(),
            PinOutcome::Unmapped => f.write_str("Unmapped"),
        }
    }
}

impl<H: File> fmt::Debug for CreateOutcome<'_, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CreateOutcome::Created(c) => f.debug_tuple("Created").field(c).finish(),
            CreateOutcome::Exists(p, r) => f.debug_tuple("Exists").field(p).field(r).finish(),
            CreateOutcome::Loading(r) => f.debug_tuple("Loading").field(r).finish(),
        }
    }
}

impl<H: File> fmt::Debug for Pool<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("frames", &self.latches.len())
            .field("page_size", &self.page_size)
            .finish_non_exhaustive()
    }
}
