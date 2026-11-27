//! LPID allocation under the free-set lock (card A-3c, Work 5; S-STORE §4.2,
//! §5.6 "Restart").
//!
//! Invariants owned here, each rule quoting S-STORE:
//! - **The free-set lock** (§4.2): "It guards the LPID bitmap's in-memory
//!   state and `next_lpid` only ... It ranks after the gate and any frame
//!   latches ... may take the page table inside it, never the reverse, so a
//!   thread holding the page table never takes the free-set lock. Under it a
//!   thread only tests and sets LPID bits, advances `next_lpid`, and tests
//!   and pins resident MapLeaf frames (§5.6): no I/O, eviction or frame latch
//!   runs under it, it creates no page, and it waits on nothing but the page
//!   table." Every acquisition goes through [`Pool::lock_freeset`], which
//!   fires `FreesetLocking` before it locks, and whose guard fires
//!   `FreesetUnlocked` after it unlocks (Work 10): the seam of
//!   `t_freeset_lock_order`. Nothing allocates under it: `alloc_lpids`
//!   reserves its buffers before it locks, and `release_lpid`, which may
//!   need a longer bitmap, reserves one with the lock released and adopts
//!   it under the lock by copying words.
//! - **L1, held MapLeafs first** (§5.6 "Restart"): "A restarted allocation
//!   keeps the MapLeaf pins its restart took and, on its retry, takes the
//!   lowest free LPIDs that lie under MapLeafs it holds pinned, before any
//!   other; under a pinned new MapLeaf, the LPIDs from `next_lpid` on count
//!   as free. That is the only exception to §4.2's lowest-first order."
//! - **L2, an emptied held MapLeaf restarts** (§5.6): "A retry that holds
//!   the pin of a MapLeaf left with no free LPID takes no LPID under a
//!   MapLeaf it does not hold pinned: it restarts instead."
//! - **L3, lowest free, then `next_lpid`** (§4.2: "Take the lowest free
//!   LPID; if none is free, take `next_lpid++`"), and (§5.6) "An operation
//!   that allocates several LPIDs ... takes them in one hold of the free-set
//!   lock, and only if every one's MapLeaf is resident, so its allocations
//!   are one mutation": either every LPID is taken, or none.
//! - **An LPID with no resident MapLeaf is not resident** (§5.6: "An LPID
//!   with no MapLeaf yet, one that `next_lpid++` would take past the last
//!   MapLeaf, counts as not resident"): the allocator asks only
//!   `pin_resident`, which answers the same for a cold and a missing
//!   MapLeaf; which it is, the caller finds (`latch_leaf_parent`, Work 3.8),
//!   since no MapInner mutex is taken under the free-set lock.
//! - **LPID 0 is never allocated** (§1: "0 = null").
//!
//! Tests `lpid_table`, `t_freeset_lock_order`, `t_freeset_lock_order_boundary`;
//! mutants ALLOC-RETRY-LOWEST, WB-LPID-SPLIT-HOLD, WB-LPID-EMPTIED-TAKES,
//! FREESET-LOCK-IO, FREESET-LOCK-INVERT.

use core::ops::{Deref, DerefMut};

use nucleus_env::File;

use super::bitmap::Bitmap;
use super::frame::Pin;
use super::gate::GateHold;
use super::key::PageKey;
use super::pool::{FireOnDrop, HookPoint, Pool};
use super::sync::{MutexGuard, Primitives, StdPrimitives};
use super::wb::{NO_FRAME, NO_KEY};
use crate::store::StoreError;

/// The most LPIDs one call takes (card A-3c, Work 5): a split cascading
/// through a tree of `H` levels allocates at most `H + 1`, and 64 bounds the
/// buffers a call reserves before the lock.
pub(crate) const MAX_LPIDS_AT_ONCE: usize = 64;

/// The LPID bitmap and `next_lpid`, under the free-set lock.
#[derive(Debug)]
pub(crate) struct LpidState {
    /// One past the highest LPID ever allocated (§2.2 `next_lpid`).
    pub(crate) next_lpid: u64,
    /// Bit `x` set = LPID `x` free, for `1 ≤ x < next_lpid` (§4.3).
    pub(crate) free: Bitmap,
}

/// The allocator's counters (the diagnostics of Work 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LpidStats {
    /// One past the highest LPID ever allocated.
    pub next_lpid: u64,
    /// How many LPIDs below `next_lpid` are free.
    pub free: u64,
}

/// Why an allocation took nothing (S-STORE §5.6 "Restart"): the caller
/// makes MapLeaf `leaf` resident as the restart does and retries, holding
/// `held` minus `emptied` and the pins it kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocRestart {
    /// The MapLeaf of the lowest LPID the retry will take.
    pub leaf: u64,
    /// The held MapLeafs left with no free LPID (L2), which the restart
    /// stops pinning before it pins another; empty after a failed residency
    /// pin (L3).
    pub emptied: Vec<u64>,
}

/// What [`Pool::alloc_lpids`] did (card A-3c, Work 5).
#[must_use]
pub enum LpidAlloc<'p, H: File, P: Primitives = StdPrimitives> {
    /// The LPIDs, in the order taken, and the pins on the MapLeafs they
    /// needed that the caller did not hold, in the order taken: the caller
    /// keeps them until its operation ends.
    Taken {
        /// The allocated LPIDs.
        lpids: Vec<u64>,
        /// The new MapLeaf pins; none when every MapLeaf was held.
        pins: Vec<Pin<'p, H, P>>,
    },
    /// Nothing was taken: the caller restarts (S-STORE §5.6).
    Restart(AllocRestart),
}

impl<H: File, P: Primitives> core::fmt::Debug for LpidAlloc<'_, H, P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LpidAlloc::Taken { lpids, pins } => f
                .debug_struct("Taken")
                .field("lpids", lpids)
                .field("pins", &pins.len())
                .finish(),
            LpidAlloc::Restart(r) => f.debug_tuple("Restart").field(r).finish(),
        }
    }
}

/// The guard of the free-set lock (Work 10): it fires `FreesetUnlocked`
/// after it unlocks. Its fields drop in order: the mutex first, then the
/// hook.
pub(crate) struct FreesetGuard<'a, H: File, P: Primitives> {
    inner: MutexGuard<'a, P, LpidState>,
    _fire: FireOnDrop<'a, H, P, true>,
}

impl<H: File, P: Primitives> Deref for FreesetGuard<'_, H, P> {
    type Target = LpidState;

    fn deref(&self) -> &LpidState {
        &self.inner
    }
}

impl<H: File, P: Primitives> DerefMut for FreesetGuard<'_, H, P> {
    fn deref_mut(&mut self) -> &mut LpidState {
        &mut self.inner
    }
}

/// What the rules decided, before any residency pin is tried.
enum Verdict {
    /// The LPIDs are chosen in `lpids`; the MapLeafs to pin are in `need`.
    Taken,
    /// Nothing is taken (L2).
    Restart,
}

/// The buffers one call reserves before the free-set lock, so that nothing
/// allocates under it.
struct Scratch {
    /// The candidates under the held MapLeafs (L1), ascending.
    cands: Vec<u64>,
    /// `held`, ascending and without a repeat.
    held_sorted: Vec<u64>,
    /// The MapLeafs of the chosen LPIDs that are not held, in the order
    /// first needed (L3).
    need: Vec<u64>,
    /// The held MapLeafs left with no candidate (L2).
    emptied: Vec<u64>,
}

impl Scratch {
    /// Empty buffers with room for every candidate: `held` MapLeafs of `f`
    /// LPIDs and `n` more.
    fn new(held: usize, f: u64, n: usize) -> Result<Scratch, StoreError> {
        let oom = |what: &str, e: std::collections::TryReserveError| {
            StoreError::OutOfMemory(format!("LPID allocation {what}: {e}"))
        };
        let room = u64::try_from(held)
            .ok()
            .and_then(|h| h.checked_mul(f))
            .and_then(|c| c.checked_add(u64::try_from(n).unwrap_or(u64::MAX)))
            .and_then(|c| usize::try_from(c).ok())
            .ok_or_else(|| {
                StoreError::Invalid("LPID allocation: a buffer size past usize".into())
            })?;
        let mut cands = Vec::new();
        cands
            .try_reserve_exact(room)
            .map_err(|e| oom("candidates", e))?;
        let mut held_sorted = Vec::new();
        held_sorted
            .try_reserve_exact(held)
            .map_err(|e| oom("held list", e))?;
        let mut need = Vec::new();
        need.try_reserve_exact(n).map_err(|e| oom("pin list", e))?;
        let mut emptied = Vec::new();
        emptied
            .try_reserve_exact(held)
            .map_err(|e| oom("emptied list", e))?;
        Ok(Scratch {
            cands,
            held_sorted,
            need,
            emptied,
        })
    }
}

/// The lowest free LPID from `from` on whose MapLeaf is not in
/// `held_sorted`, if any.
fn first_outside_held(st: &LpidState, held_sorted: &[u64], f: u64, from: u64) -> Option<u64> {
    let mut at = st.free.first_set_in(from.max(1), st.next_lpid);
    while let Some(x) = at {
        if held_sorted.binary_search(&(x / f)).is_err() {
            return Some(x);
        }
        at = st.free.first_set_in(x.saturating_add(1), st.next_lpid);
    }
    None
}

/// The first LPID that L3 may take from `next_lpid` on: `next_lpid` itself,
/// or, when L1 already counted the LPIDs from `next_lpid` to the end of a
/// held MapLeaf as free (S-STORE §5.6: "under a pinned new MapLeaf, the
/// LPIDs from `next_lpid` on count as free"), the first LPID past them, so
/// that no LPID is chosen twice in one call. `cands` are L1's candidates.
fn fresh_start(next: u64, cands: &[u64]) -> Result<u64, StoreError> {
    match cands.iter().copied().filter(|&x| x >= next).max() {
        Some(last) => last
            .checked_add(1)
            .ok_or_else(|| StoreError::Invalid("LPID allocation: an LPID passed u64".into())),
        None => Ok(next),
    }
}

/// Note that MapLeaf `leaf` must be pinned, once, keeping the order first
/// needed.
fn need_leaf(need: &mut Vec<u64>, leaf: u64) {
    if !need.contains(&leaf) {
        need.push(leaf);
    }
}

/// L1–L3 on `st`: choose `lpids` and fill `need`, or answer `Restart`. The
/// state is not changed until [`commit`]. One function on `LpidState` and
/// the caller's residency answer, so that `wb_unit.rs`'s exhaustive table
/// drives the same code through the pool (Work 14.1).
fn plan(
    st: &LpidState,
    f: u64,
    n: usize,
    held: &[u64],
    scr: &mut Scratch,
    lpids: &mut Vec<u64>,
) -> Result<Verdict, StoreError> {
    let past = || StoreError::Invalid("LPID allocation: an LPID range passed u64".into());
    let next = st.next_lpid;
    scr.held_sorted.clear();
    scr.held_sorted.extend_from_slice(held);
    scr.held_sorted.sort_unstable();
    scr.held_sorted.dedup();
    scr.cands.clear();
    scr.emptied.clear();
    scr.need.clear();
    lpids.clear();

    // L1: the candidates under the held MapLeafs, and which of them is left
    // with none.
    for &i in &scr.held_sorted {
        let base = i.checked_mul(f).ok_or_else(past)?;
        let end = i
            .checked_add(1)
            .and_then(|j| j.checked_mul(f))
            .ok_or_else(past)?;
        let mut any = false;
        let mut at = st.free.first_set_in(base.max(1), end.min(next));
        while let Some(x) = at {
            scr.cands.push(x);
            any = true;
            at = st.free.first_set_in(x.saturating_add(1), end.min(next));
        }
        if base <= next && next < end {
            scr.cands.extend(next..end);
            any = true;
        }
        if !any {
            scr.emptied.push(i);
        }
    }
    scr.cands.sort_unstable();

    // L1: enough under the held MapLeafs: the n lowest, no new pin.
    if scr.cands.len() >= n {
        let Some(lowest) = scr.cands.get(..n) else {
            return Err(past());
        };
        lpids.extend_from_slice(lowest);
        return Ok(Verdict::Taken);
    }

    // L2: a held MapLeaf is emptied, so nothing is taken under one the
    // caller does not hold; the caller restarts at the MapLeaf of the lowest
    // LPID L3 would choose.
    if !scr.emptied.is_empty() {
        return Ok(Verdict::Restart);
    }

    // L3: every held candidate, then the lowest free LPIDs under MapLeafs
    // not held, then `next_lpid` on.
    lpids.extend_from_slice(&scr.cands);
    let mut at = first_outside_held(st, &scr.held_sorted, f, 1);
    while lpids.len() < n {
        let Some(x) = at else {
            break;
        };
        lpids.push(x);
        need_leaf(&mut scr.need, x / f);
        at = first_outside_held(st, &scr.held_sorted, f, x.saturating_add(1));
    }
    // From `next_lpid` on, past any LPID L1 already counted there. None of
    // these lies under a held MapLeaf: a held MapLeaf that starts past
    // `next_lpid` has no candidate, so L2 has already answered.
    let mut x = fresh_start(next, &scr.cands)?;
    while lpids.len() < n {
        lpids.push(x);
        need_leaf(&mut scr.need, x / f);
        x = x.checked_add(1).ok_or_else(past)?;
    }
    Ok(Verdict::Taken)
}

/// Take the chosen LPIDs: clear their bits and advance `next_lpid` past
/// every one taken from it. The highest of those need not come last: L3
/// takes L1's candidates from `next_lpid` on before the free LPIDs below it.
/// Called with the free-set lock held.
fn commit(st: &mut LpidState, lpids: &[u64]) -> Result<(), StoreError> {
    let old_next = st.next_lpid;
    for &x in lpids {
        if x < old_next {
            st.free.set(x, false).map_err(|_| {
                StoreError::Invalid(format!("LPID {x} is free past the LPID bitmap's length"))
            })?;
        }
    }
    if let Some(highest) = lpids.iter().copied().filter(|&x| x >= old_next).max() {
        st.next_lpid = highest
            .checked_add(1)
            .ok_or_else(|| StoreError::Invalid("next_lpid passed u64".into()))?;
    }
    Ok(())
}

impl<H: File, P: Primitives> Pool<H, P> {
    /// Take the free-set lock (S-STORE §4.2): `FreesetLocking` fires just
    /// before it is taken, holding only the caller's holds, and the guard
    /// fires `FreesetUnlocked` just after it is released.
    pub(crate) fn lock_freeset(&self) -> Result<FreesetGuard<'_, H, P>, StoreError> {
        self.fire(HookPoint::FreesetLocking, NO_KEY, NO_FRAME);
        let inner = self.lpid_state()?.lock().map_err(|_| {
            StoreError::Poisoned("the free-set lock was poisoned by a panic".into())
        })?;
        Ok(FreesetGuard {
            inner,
            _fire: FireOnDrop { pool: self },
        })
    }

    /// Allocate `n` LPIDs (S-STORE §4.2, §5.6 "Restart"), in one hold of the
    /// free-set lock. `held` are the indexes of the MapLeafs whose pins the
    /// caller holds from its restart; `g` the caller's gate hold, which the
    /// lock ranks after.
    ///
    /// L1: the lowest free LPIDs under the held MapLeafs come first, and
    /// under a held MapLeaf that covers `next_lpid` the LPIDs from it on
    /// count as free. L2: if a held MapLeaf is left with no candidate and
    /// they are fewer than `n`, nothing is taken and the caller restarts at
    /// `leaf`, dropping the pins of `emptied`. L3: otherwise every MapLeaf
    /// of the chosen LPIDs that is not held is pinned, under the lock and
    /// with no load; if one is not resident the pins taken are given back,
    /// nothing is taken, and the caller restarts at that MapLeaf.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a pool without write-back, or for `n`
    /// outside `1..=64`; [`StoreError::OutOfMemory`] if a result buffer
    /// cannot be reserved before the lock (nothing is taken);
    /// [`StoreError::Poisoned`] if a mutex was poisoned.
    pub fn alloc_lpids(
        &self,
        n: usize,
        held: &[u64],
        _g: &GateHold<'_, H, P>,
    ) -> Result<LpidAlloc<'_, H, P>, StoreError> {
        let invalid = |what: &str| StoreError::Invalid(format!("LPID allocation: {what}"));
        if self.wb().is_none() {
            return Err(invalid(
                "the pool was not built with write-back (card A-3c)",
            ));
        }
        if n == 0 || n > MAX_LPIDS_AT_ONCE {
            return Err(invalid("one call takes 1 to 64 LPIDs"));
        }
        let f = self.cmap()?.fanout();
        let oom = |what: &str, e: std::collections::TryReserveError| {
            StoreError::OutOfMemory(format!("LPID allocation {what}: {e}"))
        };
        let mut lpids = Vec::new();
        lpids.try_reserve_exact(n).map_err(|e| oom("LPIDs", e))?;
        let mut pins: Vec<Pin<'_, H, P>> = Vec::new();
        pins.try_reserve_exact(n).map_err(|e| oom("pins", e))?;
        let mut scr = Scratch::new(held.len(), f, n)?;

        let mut st = self.lock_freeset()?;
        match plan(&st, f, n, held, &mut scr, &mut lpids)? {
            Verdict::Restart => {
                // `leaf` is the MapLeaf of the lowest LPID L3 would choose
                // under a MapLeaf the caller does not hold: the lowest free
                // one, else the first from `next_lpid` on past L1's.
                let fresh = fresh_start(st.next_lpid, &scr.cands)?;
                let leaf = first_outside_held(&st, &scr.held_sorted, f, 1).unwrap_or(fresh) / f;
                Ok(LpidAlloc::Restart(AllocRestart {
                    leaf,
                    emptied: core::mem::take(&mut scr.emptied),
                }))
            }
            Verdict::Taken => {
                let mut failed = None;
                // The reservation's MapLeafs are the operation's own pins
                // (Work 6.5): `pin_resident`, which fires `Pinned` (and their
                // drop `Unpinned`), hooks Work 10 lets fire under the
                // free-set lock.
                for &leaf in &scr.need {
                    match self.pin_resident(PageKey::MapLeaf(leaf))? {
                        Some(p) => pins.push(p),
                        None => {
                            failed = Some(leaf);
                            break;
                        }
                    }
                }
                if let Some(leaf) = failed {
                    // Under the lock: a `Pin`'s drop takes the page table,
                    // the one order §4.2 allows inside the free-set lock.
                    drop(pins);
                    return Ok(LpidAlloc::Restart(AllocRestart {
                        leaf,
                        emptied: Vec::new(),
                    }));
                }
                commit(&mut st, &lpids)?;
                Ok(LpidAlloc::Taken { lpids, pins })
            }
        }
    }

    /// Free an LPID (S-STORE §4.2 "Reuse is immediate, in memory"): its bit
    /// is set under the free-set lock. A-4b's free calls it after the entry
    /// is unmapped and the frame is freed (A-3a OBL-5).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a pool without write-back, for LPID 0,
    /// for an LPID at or past `next_lpid` or for one already free;
    /// [`StoreError::OutOfMemory`] if the bitmap must grow for it and cannot
    /// (nothing changes).
    ///
    /// The bitmap covers the LPIDs below `next_lpid` at construction, and
    /// `alloc_lpids` advances `next_lpid` without growing it, so a released
    /// LPID may lie past its length. Nothing allocates under the free-set
    /// lock (§4.2: under it "a thread only tests and sets LPID bits"): the
    /// longer bitmap is reserved with the lock released, then adopted under
    /// it by copying words, and the old one is dropped after the unlock.
    /// `next_lpid` only grows, so the second hold always adopts.
    pub fn release_lpid(&self, lpid: u64) -> Result<(), StoreError> {
        let invalid = |what: String| StoreError::Invalid(format!("LPID free: {what}"));
        if self.wb().is_none() {
            return Err(invalid(
                "the pool was not built with write-back (card A-3c)".to_string(),
            ));
        }
        let mut spare: Option<Bitmap> = None;
        loop {
            let mut st = self.lock_freeset()?;
            if lpid == 0 {
                return Err(invalid("LPID 0 is null (S-STORE §1)".to_string()));
            }
            if lpid >= st.next_lpid {
                return Err(invalid(format!(
                    "LPID {lpid} is at or past next_lpid {}",
                    st.next_lpid
                )));
            }
            if st.free.get(lpid) {
                return Err(invalid(format!("LPID {lpid} is already free")));
            }
            let mut old = None;
            if lpid >= st.free.len() {
                match spare.take() {
                    Some(longer) if lpid < longer.len() => {
                        old = Some(st.free.adopt(longer)?);
                    }
                    _ => {
                        // Reserve a bitmap for every LPID below `next_lpid`
                        // with the lock released, and look again.
                        let want = st.next_lpid;
                        drop(st);
                        spare = Some(Bitmap::with_len(want)?);
                        continue;
                    }
                }
            }
            let set = st
                .free
                .set(lpid, true)
                .map_err(|_| invalid("a bit past the bitmap".to_string()));
            drop(st);
            drop(old);
            return set;
        }
    }

    /// The allocator's counters (the diagnostics of Work 5).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a pool without write-back;
    /// [`StoreError::Poisoned`] if the lock was poisoned.
    pub fn lpid_stats(&self) -> Result<LpidStats, StoreError> {
        if self.wb().is_none() {
            return Err(StoreError::Invalid(
                "LPID allocation: the pool was not built with write-back (card A-3c)".into(),
            ));
        }
        let st = self.lock_freeset()?;
        Ok(LpidStats {
            next_lpid: st.next_lpid,
            free: st.free.count_ones(),
        })
    }

    /// Whether LPID `x` is free.
    ///
    /// # Errors
    /// As [`Pool::lpid_stats`].
    pub fn lpid_is_free(&self, x: u64) -> Result<bool, StoreError> {
        if self.wb().is_none() {
            return Err(StoreError::Invalid(
                "LPID allocation: the pool was not built with write-back (card A-3c)".into(),
            ));
        }
        let st = self.lock_freeset()?;
        Ok(x < st.next_lpid && st.free.get(x))
    }
}
