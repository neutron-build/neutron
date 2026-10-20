//! The ts sequencer (card X-1a Work 3; PLAN-v3 §3.4 "advance `visible_ts`
//! in ts order"; C-T0 §3).
//!
//! Invariants owned here:
//! - S-TS: commit ts's are handed out strictly ascending and never
//!   reused; `next_commit_ts` is one critical section (atomic).
//! - S-VISIBLE-PREFIX (C-VISIBLE-PREFIX): `visible_ts` is the largest `t`
//!   such that **every** commit with ts `<= t` has been marked applied —
//!   a commit that applied out of order holds `visible_ts` below itself
//!   until every smaller ts has applied. A reader at a snapshot `S` thus
//!   sees **all** commits `<= S` and **no** commit `> S` (C-T0 §3.1 "why
//!   that is enough").
//! - S-GATE-SCOPE (card X-1a-F3, C-GATE-SCOPE): the test-only apply gate
//!   is **per-sequencer** state — arming it parks only commits whose
//!   [`commit_with`](crate::common::commit_with) receives this
//!   sequencer, never another engine's. (`Sequencer::new` also records
//!   a `Weak` pointer for the free-function compatibility bridge of
//!   [`commit::apply_gate`]; no engine path reads it.)

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::api::Ts;
use crate::common::commit::apply_gate::{self, ApplyGate};

/// Interior state of [`Sequencer`].
struct SeqState {
    /// The next ts to hand out (S-TS); ts's handed out so far are
    /// `1..next_ts`.
    next_ts: Ts,
    /// The published visible ts (S-VISIBLE-PREFIX).
    visible: Ts,
    /// Commits that hold a ts but have not yet applied; the prefix rule
    /// is exactly "advance while `visible + 1` is not in here".
    pending: BTreeSet<Ts>,
}

/// Assigns commit ts's and publishes `visible_ts`.
///
/// Shared by every arm and by the reference engine (card Work 3).
pub struct Sequencer {
    state: Mutex<SeqState>,
    /// This sequencer's test-only apply gate (S-GATE-SCOPE, card
    /// X-1a-F3): every `commit_with` on this sequencer consults it, and
    /// no other engine's commit does. Shared ownership (`Arc`) so the
    /// gate outlives any guard the free-function compatibility bridge
    /// holds (see [`apply_gate`](crate::common::commit::apply_gate));
    /// the engine path only ever reads through
    /// [`Sequencer::apply_gate`].
    gate: Arc<ApplyGate>,
}

impl Sequencer {
    /// A fresh sequencer with nothing committed: `visible_ts = 0`.
    pub fn new() -> Self {
        let gate = Arc::new(ApplyGate::new());
        apply_gate::register_latest(&gate);
        Self {
            state: Mutex::new(SeqState {
                next_ts: 1,
                visible: 0,
                pending: BTreeSet::new(),
            }),
            gate,
        }
    }

    /// This sequencer's test-only apply gate (card X-1a-F3): the
    /// per-engine instance `commit_with` consults, so the suite can
    /// hold this engine's commits without parking any other engine's.
    /// Test observability, not part of the commit semantics.
    #[doc(hidden)]
    pub fn apply_gate(&self) -> &ApplyGate {
        &self.gate
    }

    /// The ts a reader opening a snapshot now must use: the current
    /// `visible_ts` (card Work 3).
    pub fn begin_snapshot(&self) -> Ts {
        self.visible_ts()
    }

    /// The published visible ts (S-VISIBLE-PREFIX).
    pub fn visible_ts(&self) -> Ts {
        crate::common::lock_unpoisoned(&self.state).visible
    }

    /// Atomically assign the next commit ts and mark it pending-apply
    /// (S-TS). Callers must follow with [`Sequencer::mark_applied`] for
    /// that ts.
    pub fn next_commit_ts(&self) -> Ts {
        let mut s = crate::common::lock_unpoisoned(&self.state);
        let ts = s.next_ts;
        s.next_ts += 1;
        s.pending.insert(ts);
        ts
    }

    /// Mark the commit at `ts` applied and advance `visible_ts` over the
    /// contiguous applied prefix (S-VISIBLE-PREFIX). Idempotent per ts;
    /// an unknown ts is ignored (apply failures are fail-stop, so the
    /// spike never marks an unassigned ts).
    pub fn mark_applied(&self, ts: Ts) {
        let mut s = crate::common::lock_unpoisoned(&self.state);
        s.pending.remove(&ts);
        loop {
            let candidate = s.visible + 1;
            // Stop at the next commit that holds a ts but has not
            // applied, or when every assigned ts has applied.
            if candidate >= s.next_ts || s.pending.contains(&candidate) {
                break;
            }
            s.visible = candidate;
        }
    }
}

impl Default for Sequencer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Sequencer;

    /// S-VISIBLE-PREFIX: an out-of-order apply pins visible_ts below
    /// itself until the gap closes (the VISIBLE-JUMPS mutant of the card
    /// breaks exactly this property).
    #[test]
    fn visible_ts_follows_the_applied_prefix() {
        let seq = Sequencer::new();
        let t1 = seq.next_commit_ts();
        let t2 = seq.next_commit_ts();
        let t3 = seq.next_commit_ts();
        assert_eq!(seq.visible_ts(), 0, "nothing applied yet");
        seq.mark_applied(t3);
        assert_eq!(seq.visible_ts(), 0, "t1 and t2 are still pending");
        seq.mark_applied(t1);
        assert_eq!(seq.visible_ts(), 1, "t2 still pins the prefix");
        seq.mark_applied(t2);
        assert_eq!(seq.visible_ts(), 3, "prefix complete");
    }
}
