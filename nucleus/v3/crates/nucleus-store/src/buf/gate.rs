//! The epoch gate (card A-3c, Work 2; S-STORE §5.6 "The epoch gate", §13
//! step 0 item 1; §15 "Stand-ins" S1 for the flip): shared holds for gated
//! operations, a non-blocking `try_shared`, the flip's exclusive hold, the
//! epoch `e`, the image root a flip records, `wait_for_flip`, and the two
//! hooks of its test seam (§5.4's pool-floor variant).
//!
//! Invariants owned here, each rule quoting S-STORE §5.6 or §13:
//! - **G1, shared grant.** "It is held **shared** by every gated operation"
//!   and "The checkpoint's flip takes the gate **exclusively**": a shared
//!   request is granted iff `!exclusive && waiting_exclusive == 0`. A flip
//!   that has asked for the gate therefore blocks new shared holders, which
//!   is what makes §5.2's try fail "because a flip is pending" and lets the
//!   flip drain the current holders.
//! - **G2, exclusive grant.** "Take the epoch gate exclusively. No gated
//!   operation is now in flight" (§13 step 0 item 1) and "At most one
//!   checkpoint runs at a time" (§13): an exclusive request registers at
//!   once and is granted iff `shared == 0 && !exclusive`.
//! - **G3, release.** Each release changes the state under the mutex and
//!   then wakes every waiter.
//! - **G4, waits.** "Every wait releases its mutex and sleeps in one step
//!   ... and re-tests its predicate on every wake" (§9.2 item 3): each wait
//!   is a facade condition-variable loop. A-3c-B adds the poison mark to
//!   these predicates (§9.2 item 3, "gate and pin waiters").
//! - **G5, the epoch.** `epoch` starts at 1 when the pool is built (§1: "a
//!   counter that starts at 1 when the file is created") and changes only
//!   under the exclusive hold, by [`FlipGuard::advance_epoch`]: `e + 1` in
//!   checked arithmetic, `Invalid` at `u32::MAX` (meta's `epoch` is a `u32`,
//!   and a content epoch that wrapped would compare wrongly).
//! - **G6, the image root.** [`FlipGuard::record_image_root`] stores
//!   `ImageRoot { image: e, level }`; S1 and A-7c's step 0 call it before
//!   `advanceEpoch` (§13 step 0 item 2: "record the map's root and its level
//!   as image `k`'s (§4.1), and set `e := k+1`").
//! - **The epoch cannot move under a shared hold** (G2), so the epoch
//!   [`GateHold::epoch`] read at its grant is the gate's for the hold's life.
//!
//! Tests `gate_rules`, `s1_records_root`, `gate_try_table`, `evict_holds_gate`,
//! `t_pin_load_race_k`; mutants WB-GATE-WRITER-STARVE, WB-EPOCH-NOT-ADVANCED,
//! WB-ROOT-NOT-RECORDED, WB-GATE-BLOCKING.

use nucleus_env::File;

use super::frame::FrameId;
use super::pool::{HookPoint, Pool};
use super::sync::{Condvar, Mutex, MutexGuard, Primitives, StdPrimitives};
use super::wb::{NO_FRAME, NO_KEY};
use crate::store::StoreError;

/// The map's root as a flip records it for an image (S-STORE §13 step 0
/// item 2; §4.1: "The root is always a MapInner, at level 1 or above").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageRoot {
    /// The epoch `k` that the recording flip closed: image `k`'s.
    pub image: u32,
    /// The root's level: the root is always the MapInner at index 0 of its
    /// level (§4.1), so its level names it.
    pub level: u8,
}

/// What the gate holds, under its mutex (card A-3c, Work 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GateState {
    /// Shared holders.
    pub(crate) shared: u64,
    /// Whether the flip holds the gate exclusively.
    pub(crate) exclusive: bool,
    /// Exclusive requests registered and not yet granted.
    pub(crate) waiting_exclusive: u32,
    /// `e`: the checkpoint epoch (§1).
    pub(crate) epoch: u32,
    /// The root the last flip recorded for its image (G6).
    pub(crate) image_root: Option<ImageRoot>,
}

/// The gate: its state and the variable every waiter sleeps on.
pub(crate) struct Gate<P: Primitives> {
    /// The state, under one mutex.
    pub(crate) state: Mutex<P, GateState>,
    /// Signalled whenever the state changes (G3).
    pub(crate) changed: Condvar<P>,
}

impl<P: Primitives> Gate<P> {
    /// A gate at `epoch`, open, with nothing recorded.
    pub(crate) fn new(epoch: u32) -> Gate<P> {
        Gate {
            state: Mutex::new(GateState {
                shared: 0,
                exclusive: false,
                waiting_exclusive: 0,
                epoch,
                image_root: None,
            }),
            changed: Condvar::new(),
        }
    }

    /// G1: whether a shared request is granted now.
    pub(crate) fn shared_grantable(s: &GateState) -> bool {
        !s.exclusive && s.waiting_exclusive == 0
    }

    /// G2: whether the registered exclusive request is granted now.
    pub(crate) fn exclusive_grantable(s: &GateState) -> bool {
        s.shared == 0 && !s.exclusive
    }
}

/// A shared hold of the gate (S-STORE §5.6: a gated operation "holds the
/// gate from its first page mutation to its last"), from
/// [`Pool::gate_shared`] or [`Pool::gate_try_shared`]. Dropping it releases
/// it (G3).
pub struct GateHold<'p, H: File, P: Primitives = StdPrimitives> {
    pool: &'p Pool<H, P>,
    epoch: u32,
}

/// The flip's exclusive hold (S-STORE §13 step 0 item 1), from
/// [`Pool::gate_exclusive`]: no gated operation is in flight while it lives.
/// Dropping it is step 0 item 3, "Release the gate".
pub struct FlipGuard<'p, H: File, P: Primitives = StdPrimitives> {
    pool: &'p Pool<H, P>,
}

impl<H: File, P: Primitives> Pool<H, P> {
    /// The gate's mutex, or the error a panic left.
    fn gate_lock(&self) -> Result<MutexGuard<'_, P, GateState>, StoreError> {
        self.gate().state.lock().map_err(|_| {
            StoreError::Poisoned("the epoch gate's mutex was poisoned by a panic".into())
        })
    }

    /// Grant a shared hold of the state `s` (G1), which the caller holds.
    fn grant_shared(
        &self,
        s: &mut MutexGuard<'_, P, GateState>,
    ) -> Result<GateHold<'_, H, P>, StoreError> {
        s.shared = s
            .shared
            .checked_add(1)
            .ok_or_else(|| StoreError::Invalid("the gate's shared count is at u64::MAX".into()))?;
        Ok(GateHold {
            pool: self,
            epoch: s.epoch,
        })
    }

    /// Grant the exclusive hold of the state `s` (G2), which the caller
    /// holds: the registration ends and the flip holds the gate.
    fn grant_exclusive(
        &self,
        s: &mut MutexGuard<'_, P, GateState>,
    ) -> Result<FlipGuard<'_, H, P>, StoreError> {
        s.waiting_exclusive = s.waiting_exclusive.checked_sub(1).ok_or_else(|| {
            StoreError::Invalid("the gate has no exclusive request to grant".into())
        })?;
        if s.exclusive {
            return Err(StoreError::Invalid(
                "the gate is already held exclusively while a second flip is granted".into(),
            ));
        }
        s.exclusive = true;
        Ok(FlipGuard { pool: self })
    }

    /// Take the gate shared, blocking until G1 grants it (S-STORE §5.6
    /// "Acquisition": "An operation acquires the gate before its first
    /// latch, never twice").
    ///
    /// This is the seam of §5.4's pool-floor variant: "its shared acquire
    /// tries the gate first and, when it finds the gate held exclusively,
    /// calls the hook before it blocks". [`HookPoint::GateSharedBlocking`]
    /// fires whenever the acquire is about to block, with frame
    /// `FrameId(0)` when the gate is held exclusively and `FrameId(1)` when
    /// an exclusive request only waits (Work 10's sentinel rule), so a test
    /// can tell the two; it fires holding no part of the gate.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the gate's mutex was poisoned.
    pub fn gate_shared(&self) -> Result<GateHold<'_, H, P>, StoreError> {
        let mut s = self.gate_lock()?;
        if Gate::<P>::shared_grantable(&s) {
            return self.grant_shared(&mut s);
        }
        let frame = if s.exclusive { FrameId(0) } else { FrameId(1) };
        drop(s);
        self.fire(HookPoint::GateSharedBlocking, NO_KEY, frame);
        let mut s = self.gate_lock()?;
        while !Gate::<P>::shared_grantable(&s) {
            s = self.gate().changed.wait(s).map_err(|_| {
                StoreError::Poisoned("the epoch gate's mutex was poisoned by a panic".into())
            })?;
        }
        self.grant_shared(&mut s)
    }

    /// Take the gate shared only if that does not block (S-STORE §5.2: an
    /// eviction outside a gated operation "only *tries* to take the gate
    /// shared"): `Ok(None)` while a flip holds the gate or waits for it. It
    /// never waits.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the gate's mutex was poisoned.
    pub fn gate_try_shared(&self) -> Result<Option<GateHold<'_, H, P>>, StoreError> {
        let mut s = self.gate_lock()?;
        if Gate::<P>::shared_grantable(&s) {
            return self.grant_shared(&mut s).map(Some);
        }
        Ok(None)
    }

    /// Take the gate exclusively, as the checkpoint's flip does (S-STORE
    /// §13 step 0 item 1; §5.6: "The checkpoint's flip takes the gate
    /// **exclusively**"). The request registers at once (G2), so every
    /// later `gate_try_shared` fails until the flip ends; then, if it is not
    /// granted at once, [`HookPoint::GateExclusiveWaiting`] fires holding
    /// the registration and no gate, and it waits per G4.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the gate's mutex was poisoned.
    pub fn gate_exclusive(&self) -> Result<FlipGuard<'_, H, P>, StoreError> {
        let mut s = self.gate_lock()?;
        s.waiting_exclusive = s.waiting_exclusive.checked_add(1).ok_or_else(|| {
            StoreError::Invalid("the gate's exclusive requests passed u32".into())
        })?;
        if Gate::<P>::exclusive_grantable(&s) {
            return self.grant_exclusive(&mut s);
        }
        drop(s);
        self.fire(HookPoint::GateExclusiveWaiting, NO_KEY, NO_FRAME);
        let mut s = self.gate_lock()?;
        while !Gate::<P>::exclusive_grantable(&s) {
            s = self.gate().changed.wait(s).map_err(|_| {
                StoreError::Poisoned("the epoch gate's mutex was poisoned by a panic".into())
            })?;
        }
        self.grant_exclusive(&mut s)
    }

    /// Wait until no flip holds the gate or waits for it: `gate_shared()`,
    /// then drop the hold (S-STORE §5.4 "The load": the thread "loads the
    /// page again only once the flip has run"). It fires
    /// [`HookPoint::GateSharedBlocking`] before it blocks.
    ///
    /// # Errors
    /// As [`Pool::gate_shared`].
    pub fn wait_for_flip(&self) -> Result<(), StoreError> {
        let hold = self.gate_shared()?;
        drop(hold);
        Ok(())
    }

    /// The epoch `e`, read under the gate's mutex (§1).
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the gate's mutex was poisoned.
    pub fn epoch(&self) -> Result<u32, StoreError> {
        Ok(self.gate_lock()?.epoch)
    }

    /// The root the last flip recorded for its image (G6), read under the
    /// gate's mutex.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the gate's mutex was poisoned.
    pub fn image_root(&self) -> Result<Option<ImageRoot>, StoreError> {
        Ok(self.gate_lock()?.image_root)
    }
}

impl<H: File, P: Primitives> GateHold<'_, H, P> {
    /// The epoch at the hold's grant (§1). It cannot change while any shared
    /// hold exists (G2).
    pub fn epoch(&self) -> u32 {
        self.epoch
    }
}

impl<H: File, P: Primitives> Drop for GateHold<'_, H, P> {
    /// G3: lower the shared count under the mutex, then wake every waiter.
    /// On a mutex a panic poisoned nothing can be released: the count stays,
    /// the gate stays blocked and the store is failed (S-STORE §9.2), which
    /// A-3c-B's mark makes every later call say. Never panics.
    fn drop(&mut self) {
        if let Ok(mut s) = self.pool.gate_lock() {
            s.shared = s.shared.saturating_sub(1);
            self.pool.gate().changed.notify_all();
        }
    }
}

impl<H: File, P: Primitives> FlipGuard<'_, H, P> {
    /// The epoch `e` now, read under the gate's mutex: the same as
    /// [`GateHold::epoch`] gives, since no other thread can move it while
    /// the flip holds the gate.
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the gate's mutex was poisoned.
    pub fn epoch(&self) -> Result<u32, StoreError> {
        self.pool.gate_lock().map(|s| s.epoch)
    }

    /// G5: `e := e + 1` (S-STORE §13 step 0 item 2, "set `e := k+1`"), and
    /// the new epoch.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] at `u32::MAX`, with the epoch unchanged (a
    /// flip that would carry the epoch past `2^32 − 1` is an engine
    /// invariant violation; meta's `epoch` is a `u32`);
    /// [`StoreError::Poisoned`] if the gate's mutex was poisoned.
    pub fn advance_epoch(&mut self) -> Result<u32, StoreError> {
        let mut s = self.pool.gate_lock()?;
        s.epoch = s.epoch.checked_add(1).ok_or_else(|| {
            StoreError::Invalid("the epoch is at u32::MAX and cannot advance".into())
        })?;
        Ok(s.epoch)
    }

    /// G6: record the map's root and its level as the closing epoch's image
    /// root (S-STORE §13 step 0 item 2), with `level` the root's
    /// ([`super::MapShape::root_level`]).
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the gate's mutex was poisoned.
    pub fn record_image_root(&mut self, level: u8) -> Result<(), StoreError> {
        let mut s = self.pool.gate_lock()?;
        s.image_root = Some(ImageRoot {
            image: s.epoch,
            level,
        });
        Ok(())
    }
}

impl<H: File, P: Primitives> Drop for FlipGuard<'_, H, P> {
    /// G3, and §13 step 0 item 3 ("Release the gate"): clear `exclusive`
    /// under the mutex, then wake every waiter. As [`GateHold`]'s on a
    /// poisoned mutex. Never panics.
    fn drop(&mut self) {
        if let Ok(mut s) = self.pool.gate_lock() {
            s.exclusive = false;
            self.pool.gate().changed.notify_all();
        }
    }
}

impl<H: File, P: Primitives> core::fmt::Debug for GateHold<'_, H, P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GateHold")
            .field("epoch", &self.epoch)
            .finish()
    }
}

impl<H: File, P: Primitives> core::fmt::Debug for FlipGuard<'_, H, P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FlipGuard").finish()
    }
}
