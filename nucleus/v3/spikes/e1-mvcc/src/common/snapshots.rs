//! The snapshot registry and GC watermark (card X-1a Work 3; C-T0 §3.1,
//! §9.1).
//!
//! Invariants owned here:
//! - C-SNAP-REG: [`Snapshots::register`] reads `visible_ts` from the
//!   sequencer **while holding the registry mutex**, and
//!   [`Snapshots::oldest`] computes the watermark under the same mutex —
//!   so a GC that publishes watermark `W` cannot interleave between a
//!   reader's read of `visible_ts` and its registration. Every registered
//!   snapshot is `>= W` at registration time (C-T0 §9.1).
//! - C-SNAP-REFCOUNT: registrations are **refcounted per ts**. Several
//!   readers legitimately share one snapshot ts (an RC txn committing at
//!   ts c and an RR reader that opened at the same visible_ts); one of
//!   them ending must never unregister the others — the registry counts
//!   guards per ts and the ts leaves the registry only when the last
//!   guard for it drops.
//! - C-RAII: a snapshot stays registered until its guard drops — the end
//!   of the txn for RR, the next statement for RC (C-T0 §3.1). Dropping
//!   late is safe (it only holds the watermark back); dropping early is
//!   what the guard makes impossible.
//!
//! Lock order (module docs, [`crate::common`]): registry → sequencer.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::api::Ts;
use crate::common::sequencer::Sequencer;

/// Registry of the ts values of every active snapshot, refcounted
/// (C-SNAP-REFCOUNT).
#[derive(Clone)]
pub struct Snapshots {
    active: Arc<Mutex<BTreeMap<Ts, u32>>>,
}

/// RAII registration of one snapshot ts ([`Snapshots::register`]).
pub struct SnapshotGuard {
    active: Arc<Mutex<BTreeMap<Ts, u32>>>,
    ts: Ts,
}

impl Snapshots {
    /// An empty registry (no active snapshots).
    pub fn new() -> Self {
        Self {
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Register a fresh snapshot atomically with the sequencer's
    /// `visible_ts` (C-SNAP-REG) and return its RAII guard.
    pub fn register(&self, seq: &Sequencer) -> SnapshotGuard {
        let mut active = crate::common::lock_unpoisoned(&self.active);
        let ts = seq.begin_snapshot();
        let count = active.get(&ts).copied().unwrap_or(0);
        active.insert(ts, count + 1);
        SnapshotGuard {
            active: Arc::clone(&self.active),
            ts,
        }
    }

    /// The GC watermark: `min(active)` or the sequencer's `visible_ts`
    /// when there are no active snapshots (card Work 3; C-T0 §9.1).
    ///
    /// GC that keeps every version above this value plus the newest at or
    /// below it preserves every version any current or future reader can
    /// select (C-GC-SAFE).
    pub fn oldest(&self, seq: &Sequencer) -> Ts {
        let active = crate::common::lock_unpoisoned(&self.active);
        let visible = seq.visible_ts();
        match active.keys().next() {
            Some(min) => (*min).min(visible),
            None => visible,
        }
    }
}

impl Default for Snapshots {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotGuard {
    /// The registered snapshot ts.
    pub fn ts(&self) -> Ts {
        self.ts
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        // A poisoned registry mutex is unreachable (infallible critical
        // sections); if it ever happens, keep the registration (holds the
        // watermark back, never unsound).
        if let Ok(mut active) = self.active.lock() {
            match active.get(&self.ts).copied() {
                Some(1) => {
                    active.remove(&self.ts);
                }
                Some(n) => {
                    active.insert(self.ts, n - 1);
                }
                None => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Sequencer, Snapshots};

    /// C-RAII and C-SNAP-REG: the watermark follows the oldest guard and
    /// falls back to `visible_ts` when none is active. (The GC-TOO-EAGER
    /// mutant of the card breaks the first half.)
    #[test]
    fn oldest_tracks_the_oldest_active_snapshot() {
        let seq = Sequencer::new();
        let snaps = Snapshots::new();
        // Nothing registered: the watermark is visible_ts.
        assert_eq!(snaps.oldest(&seq), 0);
        // Commit once so visible_ts moves.
        let ts = seq.next_commit_ts();
        seq.mark_applied(ts);
        assert_eq!(snaps.oldest(&seq), 1);
        // An old snapshot pins the watermark below visible_ts...
        let old = snaps.register(&seq);
        assert_eq!(old.ts(), 1);
        let ts2 = seq.next_commit_ts();
        seq.mark_applied(ts2);
        assert_eq!(
            snaps.oldest(&seq),
            1,
            "the active snapshot pins the watermark"
        );
        // ...and only its drop releases it.
        drop(old);
        assert_eq!(snaps.oldest(&seq), 2);
    }

    /// C-SNAP-REFCOUNT: two readers at the same ts; one ending must not
    /// unregister the other (the S12 shape: a seed txn and a long reader
    /// sharing the snapshot ts of the same commit frontier).
    #[test]
    fn same_ts_snapshots_are_refcounted() {
        let seq = Sequencer::new();
        let snaps = Snapshots::new();
        let a = snaps.register(&seq);
        let b = snaps.register(&seq);
        assert_eq!(a.ts(), b.ts());
        drop(a);
        let ts = seq.next_commit_ts();
        seq.mark_applied(ts);
        assert_eq!(
            snaps.oldest(&seq),
            b.ts(),
            "b's registration survives a's drop"
        );
        drop(b);
        assert_eq!(snaps.oldest(&seq), 1);
    }
}
