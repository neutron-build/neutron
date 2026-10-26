#![deny(clippy::indexing_slicing)]
//! The store primitives (S-STORE §4, §5; card 3.3–3.5), converted to the
//! S-BATON step discipline (card X-5b Work 11): every primitive is a
//! short lock-work-unlock step ([`Cx::with`]) or a sequence of them, and
//! page bytes travel as owned copies between steps — no world borrow
//! survives a yield (rule 1).
//!
//! Yield points (card 1.2): gate attempts (§5.6), every `store_page` (the
//! `write_guard` yield), pin waits, and the env calls inside the
//! write-out paths.
//!
//! The image map (the CoW delta of §4.1/§13 step 2, the Work 12
//! simplification): while a checkpoint's flip window is open (step 0 →
//! step 5), the **first** post-flip touch of each LPID freezes its
//! image-k state — `At(home)` for a clean page (its bytes are safe at the
//! home slot), `CopyPending` for a dirty one (§5.6's epoch copy preserves
//! them and step 2 or a reserve eviction writes them to a fresh slot),
//! `Free` for a page that did not exist at the flip. After step 5 the
//! overrides are resolved into the new durable base and cleared.
//!
//! Invariants owned here:
//! - PR-EVICT (§5.2): a victim is latched (`Frame::evicting`) from
//!   selection to completion, so no fetch or mutation can observe a frame
//!   mid-write-out; dirty victims go to a fresh slot (I-NO-INPLACE) and a
//!   previous-epoch victim during a flip window is recorded in the image
//!   map (§5.2, the EVICT-SKIP-IMAGE-MAP site).
//! - PR-RESERVE (§5.6): the first `R_EPOCH` frames of the pool are the
//!   epoch-copy reserve; normal pages never occupy them; exhaustion
//!   writes the oldest copy out and reuses its frame.

use std::collections::{BTreeMap, BTreeSet};

use super::page::{self, Ref};
use super::{Cx, Fail, Frame, FrozenImage, Img, Mutant, StoreHost, StoreInner};
use crate::sim::{E4Env, Sched, SimFail, TaskId, WaitKey};

/// Record a world-acquisition failure observed where it cannot be
/// propagated (release helpers, `Drop` guards, best-effort keep-steps):
/// S-BATON rule 4 keeps it loud — the harness folds the flag into the
/// round's `baton` outcome and the round fails (the X-5b gate's minor
/// finding: `.ok()`/`unwrap_or(false)` here swallowed
/// [`SimFail::Baton`] silently).
pub fn note_step_fail<H: StoreHost>(cx: &Cx<'_, H>, site: &'static str, e: Fail) {
    if matches!(e, Fail::Sim(SimFail::Baton(_))) {
        let _ = cx.with("baton_fail", |c| {
            let st = c.store();
            if st.baton_fail.is_none() {
                st.baton_fail = Some(site);
            }
            Ok(())
        });
    }
}

/// The effective image-k entry of `lpid`: the override if frozen, else
/// the current ref. `None` = the image leaves it unmapped.
pub fn img_entry(st: &StoreInner, lpid: u64) -> Option<Ref> {
    match st.map.img.get(&lpid).copied() {
        Some(Img::Same) | None => st.map.cur.get(&lpid).copied(),
        Some(Img::At(r)) => Some(r),
        Some(Img::Free | Img::CopyPending | Img::Lost) => None,
    }
}

/// Every LPID the image being built has an entry for (`cur` ∪ overrides:
/// a page freed post-flip keeps its image entry through its override).
pub fn img_lpids(st: &StoreInner) -> BTreeSet<u64> {
    let mut out: BTreeSet<u64> = st.map.cur.keys().copied().collect();
    out.extend(st.map.img.keys().copied());
    out
}

/// The data-page slots the image being built references (map and free-set
/// pages excluded — the checkpointer adds its own).
pub fn image_slot_set(st: &StoreInner) -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    for l in img_lpids(st) {
        if let Some(r) = img_entry(st, l) {
            out.insert(r.slot);
        }
    }
    out
}

/// Unpin a replaced home slot: it leaves `used_cur` only when the image
/// being built no longer references it (an `Img::At` override, the
/// frozen table's pins, or an `image_refs` entry — the three-reference
/// rule of §4.4). Removing it unconditionally let a live image page be
/// re-allocated and overwritten (prototype deviation fixed in B5).
pub fn unpin_home(st: &mut StoreInner, slot: u64) {
    let img_refs_it = st
        .map
        .img
        .values()
        .any(|i| matches!(i, Img::At(r) if r.slot == slot));
    if img_refs_it || st.slots.image_refs.contains(&slot) {
        return;
    }
    st.slots.used_cur.remove(&slot);
}

/// Record `lpid → r` in the image map (§5.2: a previous-epoch victim or
/// an exhausted reserve copy "is also recorded in the epoch-k image
/// map"). **EVICT-SKIP-IMAGE-MAP's single site.**
fn record_image_at(st: &mut StoreInner, lpid: u64, r: Ref) {
    if matches!(st.cfg.mutant, Some(Mutant::EvictSkipImageMap)) {
        return;
    }
    if st.map.img.get(&lpid) != Some(&Img::At(r)) {
        st.map.img.insert(lpid, Img::At(r));
        st.slots.image_refs.insert(r.slot);
    }
}

/// Take the frozen image table (step 2's map phase, B5): every LPID's
/// image ref and the two size counters, in one atomic step, pinning
/// every referenced slot in `image_refs`. From this step until step 5
/// resolves the image, the radix, the free set and the durable set read
/// only this table — an eviction in flight across the freeze can move
/// the live home (`cur`) but cannot unpin or redirect an embedded slot.
pub fn freeze_image(st: &mut StoreInner) {
    let mut entries = BTreeMap::new();
    let mut pins = BTreeSet::new();
    for lpid in img_lpids(st) {
        if let Some(r) = img_entry(st, lpid) {
            pins.insert(r.slot);
            entries.insert(lpid, Some(r));
        } else {
            entries.insert(lpid, None);
        }
    }
    st.image_frozen = Some(FrozenImage {
        entries,
        next_lpid: st.meta.next_lpid,
        slot_count: st.slots.slot_count,
    });
    st.slots.image_refs.extend(pins);
}

/// The data-page slots the frozen image references (map and free-set
/// pages excluded — the checkpointer adds its own).
pub fn frozen_slot_set(frozen: &FrozenImage) -> BTreeSet<u64> {
    frozen.entries.values().flatten().map(|r| r.slot).collect()
}

impl StoreInner {
    /// The first `R_EPOCH` frames are the epoch-copy reserve (PR-RESERVE).
    pub fn normal_frame_range(&self) -> std::ops::Range<usize> {
        self.cfg.r_epoch..self.pool.frames.len()
    }

    /// Mutably borrow the normal (non-reserve) frames (borrow-order
    /// helper: the range is computed first).
    pub fn normal_frames_mut(&mut self) -> &mut [Frame] {
        let r = self.normal_frame_range();
        self.pool.frames.get_mut(r).unwrap_or(&mut [])
    }

    /// Immutably borrow the normal (non-reserve) frames.
    pub fn normal_frames(&self) -> &[Frame] {
        let r = self.normal_frame_range();
        self.pool.frames.get(r).unwrap_or(&[])
    }

    /// Mutably borrow the reserve frames.
    pub fn reserve_frames_mut(&mut self) -> &mut [Frame] {
        let n = self.cfg.r_epoch.min(self.pool.frames.len());
        self.pool.frames.get_mut(0..n).unwrap_or(&mut [])
    }
}

// The S-BATON gate (§5.6)
//
/// Acquire the gate shared. Blocks behind a pending flip (the flip waits
/// for holders; new acquisitions queue). The acquire attempt is a yield
/// point (card 1.2).
///
/// Wakeup discipline (no lost wakeups): the predicate is checked in a
/// `with` step and the park follows **without an intervening yield** —
/// while this task holds the baton no other task can change the gate or
/// fire the wake, so a `wake(GateShared)` either sees us parked or
/// happens before our check. A yield between check and park loses the
/// wake that freed the gate (the task parks on a predicate that is
/// already true, and nothing wakes it again).
pub fn gate_shared<H: StoreHost>(cx: &Cx<'_, H>) -> Result<(), Fail> {
    loop {
        cx.yield_now("gate_shared")?;
        let wait = cx.with("gate_shared", |c| {
            let st = c.store();
            if st.gate.exclusive || st.gate.flip_pending {
                Ok(true)
            } else {
                st.gate.holders += 1;
                Ok(false)
            }
        })?;
        if !wait {
            return Ok(());
        }
        cx.wait_for(WaitKey::GateShared)?;
    }
}

/// Release one shared hold.
pub fn gate_release<H: StoreHost>(cx: &Cx<'_, H>) {
    let wake_flip = match cx.with("gate_release", |c| {
        let st = c.store();
        st.gate.holders = st.gate.holders.saturating_sub(1);
        Ok(st.gate.flip_pending && st.gate.holders == 0)
    }) {
        Ok(b) => b,
        Err(e) => {
            note_step_fail(cx, "gate_release", e);
            false
        }
    };
    if wake_flip {
        cx.w.wake(WaitKey::GateFlip);
    }
}

/// Take the gate exclusively (step 0's flip): waits for the shared
/// holders to drain while blocking new acquisitions. With the
/// FLIP-NO-GATE mutant this does not wait, so a gated operation can
/// straddle the flip (I-CKPT-AT-T).
pub fn gate_exclusive<H: StoreHost>(cx: &Cx<'_, H>) -> Result<(), Fail> {
    let no_gate = cx.with("gate_exclusive", |c| {
        let st = c.store();
        if st.gate.exclusive {
            return Err(Fail::Invalid("nested flip".to_owned()));
        }
        st.gate.flip_pending = !matches!(st.cfg.mutant, Some(Mutant::FlipNoGate));
        Ok(matches!(st.cfg.mutant, Some(Mutant::FlipNoGate)))
    })?;
    if no_gate {
        cx.with("gate_exclusive:nogate", |c| {
            c.store().gate.exclusive = true;
            Ok(())
        })?;
        return Ok(());
    }
    cx.w.wake(WaitKey::GateShared);
    loop {
        // Yield first, then check while holding the baton, then park
        // without an intervening yield (the wakeup discipline of
        // `gate_shared`; a yield between check and park loses the wake).
        cx.yield_now("gate_exclusive")?;
        let ready = cx.with("gate_exclusive:wait", |c| Ok(c.store().gate.holders == 0))?;
        if ready {
            cx.with("gate_exclusive:take", |c| {
                c.store().gate.exclusive = true;
                Ok(())
            })?;
            return Ok(());
        }
        cx.wait_for(WaitKey::GateFlip)?;
    }
}

/// Release the exclusive hold (end of the flip's critical part).
pub fn gate_flip_release<H: StoreHost>(cx: &Cx<'_, H>) {
    if let Err(e) = cx.with("gate_flip_release", |c| {
        let st = c.store();
        st.gate.exclusive = false;
        st.gate.flip_pending = false;
        Ok(())
    }) {
        note_step_fail(cx, "gate_flip_release", e);
    }
    cx.w.wake(WaitKey::GateShared);
}

// The per-tree sim mutex (card 3.6)
//
/// Acquire tree `t`'s sim mutex, held for one gated operation. The same
/// wakeup discipline as `gate_shared`: check while holding the baton,
/// park without an intervening yield.
pub fn tree_lock<H: StoreHost>(cx: &Cx<'_, H>, t: u64) -> Result<(), Fail> {
    loop {
        cx.yield_now("tree_lock")?;
        let busy = cx.with("tree_lock", |c| {
            let st = c.store();
            let busy = st
                .tree_mu
                .get(&t)
                .copied()
                .map(|h| h != cx.t)
                .unwrap_or(false);
            if !busy {
                st.tree_mu.insert(t, cx.t);
            }
            Ok(busy)
        })?;
        if !busy {
            return Ok(());
        }
        cx.wait_for(WaitKey::Tree(t))?;
    }
}

/// Release tree `t`'s mutex.
pub fn tree_unlock<H: StoreHost>(cx: &Cx<'_, H>, t: u64) {
    if let Err(e) = cx.with("tree_unlock", |c| {
        c.store().tree_mu.remove(&t);
        Ok(())
    }) {
        note_step_fail(cx, "tree_unlock", e);
    }
    cx.w.wake(WaitKey::Tree(t));
}

// LPIDs (§4.2) and slots (§4.4–§4.5)
//
/// Allocate the lowest free LPID, or `next_lpid++` (never 0). One step.
pub fn alloc_lpid<H: StoreHost>(cx: &Cx<'_, H>) -> Result<u64, Fail> {
    cx.with("alloc_lpid", |c| {
        let st = c.store();
        if let Some(l) = st.slots.free_lpids.iter().next().copied() {
            st.slots.free_lpids.remove(&l);
            Ok(l)
        } else {
            let l = st.meta.next_lpid.max(1);
            st.meta.next_lpid = l + 1;
            Ok(l)
        }
    })
}

/// The first-touch freeze of `lpid`'s image state during a flip window
/// (the Work 12 image map). A dirty frame's epoch-k content is preserved
/// by an epoch copy (`CopyPending` → [`record_image_copy`]).
enum FreezeAct {
    /// Nothing to do (no window, already frozen, or nothing to copy).
    None,
    /// The frame's current bytes are the image-k content: copy them.
    Copy(Vec<u8>),
}

fn freeze_first_touch<H: StoreHost>(cx: &Cx<'_, H>, lpid: u64) -> Result<(), Fail> {
    let act = cx.with("img_freeze", |c| {
        let st = c.store();
        if !(st.flip_window && !st.map.img.contains_key(&lpid)) {
            return Ok(FreezeAct::None);
        }
        let frame = st
            .normal_frames()
            .iter()
            .find(|f| f.key == lpid && !f.is_epoch_copy && !f.evicting)
            .map(|f| (f.bytes.clone(), f.dirty));
        match frame {
            Some((old, true)) => {
                if matches!(st.cfg.mutant, Some(Mutant::WriteGuardNoCopy)) {
                    // WRITE-GUARD-NO-COPY: no copy, no freeze — the
                    // mutant's damage marker.
                    st.map.img.insert(lpid, Img::Lost);
                    Ok(FreezeAct::None)
                } else {
                    st.map.img.insert(lpid, Img::CopyPending);
                    Ok(FreezeAct::Copy(old))
                }
            }
            _ => {
                match st.map.cur.get(&lpid).copied() {
                    Some(r) => {
                        st.map.img.insert(lpid, Img::At(r));
                        st.slots.image_refs.insert(r.slot);
                    }
                    None => {
                        st.map.img.insert(lpid, Img::Free);
                    }
                }
                Ok(FreezeAct::None)
            }
        }
    })?;
    if let FreezeAct::Copy(old) = act {
        record_image_copy(cx, lpid, old)?;
    }
    Ok(())
}

/// Free an LPID (immediate reuse in memory, §4.2). During a flip window
/// the image keeps the page's epoch-k content (a dirty frame's bytes are
/// preserved by an epoch copy first), and the frame is dropped: the page
/// is gone for the current epoch.
pub fn free_lpid<H: StoreHost>(cx: &Cx<'_, H>, lpid: u64) -> Result<(), Fail> {
    freeze_first_touch(cx, lpid)?;
    cx.with("free_lpid", |c| {
        let st = c.store();
        for f in st.normal_frames_mut() {
            if f.key == lpid && !f.is_epoch_copy {
                f.key = 0;
                f.bytes = Vec::new();
                f.dirty = false;
                f.pins = 0;
                f.evicting = false;
            }
        }
        st.map.cur.remove(&lpid);
        if lpid + 1 == st.meta.next_lpid && lpid > 0 {
            st.meta.next_lpid -= 1;
        } else {
            st.slots.free_lpids.insert(lpid);
        }
        Ok(())
    })?;
    cx.w.wake(WaitKey::Pin);
    Ok(())
}

/// Allocate the lowest free slot (§4.5), growing the file by
/// `max(16, slot_count/16)` when none is free (the card's test-scale
/// override of §4.5's `max(1 MiB, n/16)`). A slot is allocatable only if
/// referenced by none of: the current map (`used_cur`), the durable image
/// (`used_durable`), the image being built (`image_refs` + `used_cur`,
/// §4.4's three references). The slot is marked used on allocation.
pub fn alloc_slot<H: StoreHost>(cx: &Cx<'_, H>) -> Result<u64, Fail> {
    let mut s = 3u64;
    loop {
        // One scan step: `Allocated(slot)` (marked used), `Next` (slot s
        // occupied, keep scanning) or `Grow` (no free slot below
        // `slot_count`).
        enum Scan {
            Allocated(u64),
            Next,
            Grow,
        }
        let res = cx.with("alloc_slot", |c| {
            let st = c.store();
            if s >= st.slots.slot_count {
                return Ok(Scan::Grow);
            }
            if !st.slots.used_cur.contains(&s)
                && !st.slots.used_durable.contains(&s)
                && !st.slots.image_refs.contains(&s)
            {
                st.slots.used_cur.insert(s);
                return Ok(Scan::Allocated(s));
            }
            Ok(Scan::Next)
        })?;
        match res {
            Scan::Allocated(slot) => return Ok(slot),
            Scan::Next => {
                s += 1;
            }
            Scan::Grow => {
                let newlen = cx.with("alloc_slot:grow", |c| {
                    let st = c.store();
                    let grow = (st.slots.slot_count / 16).max(16);
                    st.slots.slot_count += grow;
                    Ok(st.slots.slot_count * page::PAGE as u64)
                })?;
                let f = cx.with("alloc_slot:pf", |c| Ok(c.store().page_file))?;
                cx.env().set_len(f, newlen).map_err(Fail::Sim)?;
                s = 3;
            }
        }
    }
}

// The pool (§5)
//
/// What `fetch_page` decided in its last step.
enum FetchStep {
    /// Frame hit: bytes and content epoch.
    Hit(Vec<u8>, u32),
    /// The frame is mid-eviction: wait for the pin wake.
    WaitEvict,
    /// No free frame: the caller must evict (or wait).
    NeedFrame,
    /// Load from `Ref`.
    Load(Ref),
}

/// Find or load the frame of `lpid`, pinning it (§5.4). Loads go through
/// the map reference with §3.3's four checks; accepted bytes are reported
/// to the accept hook (card 1.4). Returns the owned page bytes and the
/// frame's content epoch. `inside_op` says whether the caller holds the
/// gate shared (§5.2's eviction rule).
pub fn fetch_page<H: StoreHost>(
    cx: &Cx<'_, H>,
    lpid: u64,
    inside_op: bool,
) -> Result<(Vec<u8>, u32), Fail> {
    let mut loaded: Option<(Ref, Vec<u8>)> = None;
    loop {
        let step = cx.with("fetch_page", |c| {
            let st = c.store();
            st.alive()?;
            let normal = st.normal_frames_mut();
            if let Some(f) = normal
                .iter_mut()
                .find(|f| f.key == lpid && !f.is_epoch_copy && !f.evicting)
            {
                f.pins += 1;
                f.refed = true;
                return Ok(FetchStep::Hit(f.bytes.clone(), f.epoch));
            }
            if normal.iter().any(|f| f.key == lpid && f.evicting) {
                return Ok(FetchStep::WaitEvict);
            }
            if let Some((r, bytes)) = loaded.take() {
                // Install the previously loaded bytes (they passed §3.3
                // against `r` when read).
                if let Some(f) = normal
                    .iter_mut()
                    .find(|f| f.key == 0 && !f.is_epoch_copy && !f.evicting)
                {
                    f.key = lpid;
                    f.bytes = bytes;
                    f.epoch = r.epoch;
                    f.dirty = false;
                    f.pins = 1;
                    f.refed = true;
                    return Ok(FetchStep::Hit(f.bytes.clone(), f.epoch));
                }
                loaded = Some((r, bytes));
                return Ok(FetchStep::NeedFrame);
            }
            if !normal.iter().any(|f| f.key == 0 && !f.is_epoch_copy) {
                return Ok(FetchStep::NeedFrame);
            }
            match st.map.cur.get(&lpid).copied() {
                Some(r) => Ok(FetchStep::Load(r)),
                None => Err(Fail::Corrupt(format!("lpid {lpid} unmapped"))),
            }
        })?;
        match step {
            FetchStep::Hit(b, e) => return Ok((b, e)),
            FetchStep::WaitEvict => cx.wait_for(WaitKey::Pin)?,
            FetchStep::NeedFrame => {
                let evicted = evict_one(cx, inside_op)?;
                if !evicted {
                    cx.wait_for(WaitKey::Pin)?;
                }
            }
            FetchStep::Load(r) => {
                let (f, off) = cx.with("fetch_page:ref", |c| {
                    let st = c.store();
                    Ok((st.page_file, r.slot * page::PAGE as u64))
                })?;
                let bytes = cx
                    .env()
                    .read_at(f, off, page::PAGE as u64)
                    .map_err(Fail::Sim)?;
                let mutant = cx.with("fetch_page:mut", |c| Ok(c.store().cfg.mutant))?;
                page::check(&bytes, &r, lpid, mutant).map_err(|()| {
                    Fail::Corrupt(format!("integrity fail lpid {lpid} slot {}", r.slot))
                })?;
                // Accept hook: a page passing §3.3 (card 1.4).
                cx.env()
                    .accepted(f, off, page::PAGE as u64)
                    .map_err(Fail::Sim)?;
                loaded = Some((r, bytes));
            }
        }
    }
}

/// Unpin every normal frame of `lpid` (one op's interest ends).
pub fn unpin<H: StoreHost>(cx: &Cx<'_, H>, lpid: u64) {
    if let Err(e) = cx.with("unpin", |c| {
        let st = c.store();
        for f in st.normal_frames_mut() {
            if f.key == lpid && !f.is_epoch_copy && f.pins > 0 {
                f.pins -= 1;
            }
        }
        Ok(())
    }) {
        note_step_fail(cx, "unpin", e);
    }
    cx.w.wake(WaitKey::Pin);
}

/// Unpins every LPID in `pins` when dropped (error paths included).
pub struct PinGuard<'a, H: StoreHost> {
    sched: &'a Sched<H>,
    task: TaskId,
    pins: Vec<u64>,
}

impl<'a, H: StoreHost> PinGuard<'a, H> {
    /// An empty guard.
    pub fn new(cx: &'a Cx<'a, H>) -> Self {
        Self {
            sched: cx.w,
            task: cx.t,
            pins: Vec::new(),
        }
    }

    /// Note a pinned LPID.
    pub fn note(&mut self, lpid: u64) {
        if !self.pins.contains(&lpid) {
            self.pins.push(lpid);
        }
    }
}

impl<H: StoreHost> Drop for PinGuard<'_, H> {
    fn drop(&mut self) {
        let cx = Cx {
            w: self.sched,
            t: self.task,
        };
        for l in self.pins.drain(..) {
            unpin(&cx, l);
        }
    }
}

/// `write_guard(frame)` + install: the only way pages mutate (§5.6). A
/// yield point first (the `write_guard` yield); then the first post-flip
/// touch of the LPID freezes its image state (an epoch copy when the
/// frame was dirty); then the frame receives the new bytes with content
/// epoch `e`.
pub fn store_page<H: StoreHost>(cx: &Cx<'_, H>, lpid: u64, bytes: Vec<u8>) -> Result<(), Fail> {
    cx.with("store_page:alive", |c| c.store().alive())?;
    cx.yield_now("write_guard")?;
    freeze_first_touch(cx, lpid)?;
    loop {
        let placed = cx.with("store_page:place", |c| {
            let st = c.store();
            let e = st.e;
            let normal = st.normal_frames_mut();
            if normal.iter().any(|f| f.key == lpid && f.evicting) {
                return Ok(false);
            }
            if let Some(f) = normal
                .iter_mut()
                .find(|f| f.key == lpid && !f.is_epoch_copy && !f.evicting)
            {
                f.bytes = bytes.clone();
                f.epoch = e;
                f.dirty = true;
                f.refed = true;
                return Ok(true);
            }
            if let Some(f) = normal
                .iter_mut()
                .find(|f| f.key == 0 && !f.is_epoch_copy && !f.evicting)
            {
                f.key = lpid;
                f.bytes = bytes.clone();
                f.epoch = e;
                f.dirty = true;
                f.pins = 0;
                f.refed = true;
                return Ok(true);
            }
            Ok(false)
        })?;
        if placed {
            return Ok(());
        }
        let evicted = evict_one(cx, true)?;
        if !evicted {
            cx.wait_for(WaitKey::Pin)?;
        }
    }
}

/// Record an epoch copy of `bytes` for the image being built (§5.6): into
/// a reserve frame, or — when the `R_EPOCH` reserve is exhausted — by
/// writing the oldest copy out to a fresh slot recorded in the image map
/// (the EVICT-SKIP-IMAGE-MAP site) and reusing its frame.
fn record_image_copy<H: StoreHost>(cx: &Cx<'_, H>, lpid: u64, bytes: Vec<u8>) -> Result<(), Fail> {
    // An exhausted reserve writes the oldest copy out (frame order =
    // issue order) and frees its frame, then retries the install.
    // Phase 1: is a reserve frame free (no bytes move yet)?
    let free_idx = cx.with("epoch_copy:pick", |c| {
        let st = c.store();
        st.alive()?;
        Ok(st.reserve_frames_mut().iter().position(|f| f.key == 0))
    })?;
    if let Some(i) = free_idx {
        cx.with("epoch_copy:install", |c| {
            let st = c.store();
            let e = st.e;
            if let Some(f) = st.reserve_frames_mut().get_mut(i) {
                f.key = lpid;
                f.bytes = bytes;
                f.epoch = e.saturating_sub(1);
                f.dirty = true;
                f.is_epoch_copy = true;
                f.pins = 1;
                f.refed = true;
                f.evicting = false;
            }
            Ok(())
        })?;
        return Ok(());
    }
    // Exhausted: dirty-evict the oldest copy, then retry the install.
    let oldest = cx.with("epoch_copy:oldest", |c| {
        let st = c.store();
        st.epoch_exhausted += 1;
        Ok(st
            .reserve_frames_mut()
            .iter()
            .position(|f| f.is_epoch_copy && f.key != 0))
    })?;
    let Some(idx) = oldest else {
        return Err(Fail::Invalid("epoch reserve empty".to_owned()));
    };
    let (key, bytes_out) = cx.with("epoch_copy:take", |c| {
        let st = c.store();
        let f = st
            .reserve_frames_mut()
            .get(idx)
            .ok_or_else(|| Fail::Invalid("frame".to_owned()))?;
        Ok((f.key, f.bytes.clone()))
    })?;
    let r = write_fresh(cx, &bytes_out)?;
    cx.with("epoch_copy:record", |c| {
        let st = c.store();
        record_image_at(st, key, r);
        if let Some(f) = st.reserve_frames_mut().get_mut(idx) {
            f.key = 0;
            f.bytes = Vec::new();
            f.dirty = false;
            f.is_epoch_copy = false;
            f.pins = 0;
            f.evicting = false;
        }
        Ok(())
    })?;
    record_image_copy(cx, lpid, bytes)
}

/// Allocate a fresh slot and write `bytes` there (I-NO-INPLACE). Returns
/// the new ref. Pure placement (no map change) — callers decide adoption
/// and image-map recording. Used by eviction, the reserve exhaustion path
/// and checkpoint step 2.
#[track_caller]
pub fn write_fresh<H: StoreHost>(cx: &Cx<'_, H>, bytes: &[u8]) -> Result<Ref, Fail> {
    let slot = alloc_slot(cx)?;
    let (f, off) = cx.with("write_fresh:pf", |c| {
        let st = c.store();
        Ok((st.page_file, slot * page::PAGE as u64))
    })?;
    cx.env().write_at(f, off, bytes).map_err(Fail::Sim)?;
    Ok(Ref {
        slot,
        epoch: page::header_epoch(bytes),
        crc: page::header_crc(bytes),
    })
}

/// One clock-eviction attempt (§5.2): latch an unpinned, non-reserve,
/// non-evicting victim (the latch stands in for try-latch, card 3.6),
/// write a dirty victim to a fresh slot (I-NO-INPLACE; the INPLACE-EVICT
/// mutant writes its current home instead), and record a previous-epoch
/// victim in the image map when a flip window is open. An eviction
/// outside a gated operation only *tries* the gate: with a flip pending
/// it takes a clean victim, and if there is none it gives up (`false`);
/// the caller retries later.
pub fn evict_one<H: StoreHost>(cx: &Cx<'_, H>, inside_op: bool) -> Result<bool, Fail> {
    let mut took_gate = false;
    // Select and latch a victim (one step). Clock with second chance:
    // a `refed` candidate clears its bit and lives on; the first
    // un-referenced candidate is latched.
    let pick = cx.with("evict:pick", |c| {
        let st = c.store();
        let normal = st.normal_frames();
        if normal.is_empty() {
            return Ok(None);
        }
        let n = normal.len();
        let mut found = None;
        for _ in 0..2 * n {
            let i = st.pool.hand % n;
            st.pool.hand += 1;
            let i = i + st.cfg.r_epoch;
            let eligible = match st.pool.frames.get(i) {
                Some(f) => f.key != 0 && !f.is_epoch_copy && f.pins == 0 && !f.evicting,
                None => false,
            };
            if !eligible {
                continue;
            }
            if let Some(f) = st.pool.frames.get_mut(i) {
                if f.refed {
                    f.refed = false;
                    continue;
                }
                f.evicting = true;
                found = Some((i, f.key, f.dirty, f.bytes.clone(), f.epoch));
                break;
            }
        }
        Ok(found)
    })?;
    let Some((idx, key, dirty, bytes, epoch)) = pick else {
        return Ok(false);
    };
    if !dirty {
        // Clean victim: drop it.
        cx.with("evict:drop", |c| {
            if let Some(f) = c.store().pool.frames.get_mut(idx) {
                f.key = 0;
                f.bytes = Vec::new();
                f.evicting = false;
            }
            Ok(())
        })?;
        cx.w.wake(WaitKey::Pin);
        return Ok(true);
    }
    if !inside_op && !took_gate {
        // §5.2: try the gate shared; a pending flip means clean
        // victims only, and if there is none, give up.
        let got = cx.with("evict:trygate", |c| {
            let st = c.store();
            if st.gate.exclusive || st.gate.flip_pending {
                Ok(false)
            } else {
                st.gate.holders += 1;
                Ok(true)
            }
        })?;
        if !got {
            // Look for any clean victim instead.
            let clean = cx.with("evict:clean", |c| {
                let st = c.store();
                Ok(st
                    .normal_frames()
                    .iter()
                    .position(|f| f.key != 0 && !f.dirty && f.pins == 0 && !f.evicting))
            })?;
            if let Some(rel) = clean {
                cx.with("evict:drop2", |c| {
                    let st = c.store();
                    let idx = rel + st.cfg.r_epoch;
                    if let Some(f) = st.pool.frames.get_mut(idx) {
                        f.key = 0;
                        f.bytes = Vec::new();
                        f.evicting = false;
                    }
                    Ok(())
                })?;
                // Unlatch the dirty candidate we had marked.
                cx.with("evict:unlatch", |c| {
                    if let Some(f) = c.store().pool.frames.get_mut(idx) {
                        f.evicting = false;
                    }
                    Ok(())
                })?;
                cx.w.wake(WaitKey::Pin);
                return Ok(true);
            }
            // No clean victim: release our latch on the dirty candidate
            // and restart (§5.3).
            cx.with("evict:release", |c| {
                if let Some(f) = c.store().pool.frames.get_mut(idx) {
                    f.evicting = false;
                }
                Ok(())
            })?;
            return Ok(false);
        }
        took_gate = true;
    }
    // Write the dirty victim out.
    let r = {
        let inplace = cx.with("evict:inplace", |c| {
            let st = c.store();
            Ok(matches!(st.cfg.mutant, Some(Mutant::InplaceEvict)))
        })?;
        if inplace {
            let home = cx.with("evict:home", |c| {
                let st = c.store();
                Ok(st.map.cur.get(&key).copied())
            })?;
            match home {
                Some(r) => {
                    let (f, off) = cx.with("evict:homeio", |c| {
                        let st = c.store();
                        Ok((st.page_file, r.slot * page::PAGE as u64))
                    })?;
                    cx.env().write_at(f, off, &bytes).map_err(Fail::Sim)?;
                    None
                }
                None => Some(write_fresh(cx, &bytes)?),
            }
        } else {
            Some(write_fresh(cx, &bytes)?)
        }
    };
    // Finish: move the home, record previous-epoch victims in the image
    // map (§5.2), clear the frame.
    let flip = cx.with("evict:finish", |c| {
        let st = c.store();
        if let Some(r) = r {
            let prev_epoch_victim = st.flip_window && epoch < st.e;
            if let Some(old) = st.map.cur.insert(key, r) {
                unpin_home(st, old.slot);
            }
            st.slots.used_cur.insert(r.slot);
            if prev_epoch_victim {
                record_image_at(st, key, r);
            }
        }
        if let Some(f) = st.pool.frames.get_mut(idx) {
            f.key = 0;
            f.bytes = Vec::new();
            f.dirty = false;
            f.evicting = false;
            f.pins = 0;
        }
        Ok(st.flip_window)
    })?;
    let _ = flip;
    if took_gate {
        let wake_flip = match cx.with("evict:ungate", |c| {
            let st = c.store();
            st.gate.holders = st.gate.holders.saturating_sub(1);
            Ok(st.gate.flip_pending && st.gate.holders == 0)
        }) {
            Ok(b) => b,
            Err(e) => {
                note_step_fail(cx, "evict:ungate", e);
                false
            }
        };
        if wake_flip {
            cx.w.wake(WaitKey::GateFlip);
        }
    }
    cx.w.wake(WaitKey::Pin);
    Ok(true)
}
