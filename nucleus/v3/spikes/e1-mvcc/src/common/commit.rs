//! The commit protocol (card X-1a Work 3; PLAN-v3 §3.4 "Commit"; C-T0 §3).
//!
//! Invariants owned here:
//! - C-COMMIT-ORDER: for every commit, in order: (1) assign the ts, (2)
//!   apply the versions, (3) mark the ts applied (advancing
//!   `visible_ts`), (4) **only then** release the txn's locks. A waiter
//!   that acquires a key after its writer's commit therefore always
//!   observes that commit as a *visible* commit (C-T0 §3.2 — the
//!   commit-visibility window closes before any lock is handed on), and
//!   an RC updater's EPQ base already includes it.
//! - C-FAIL-STOP: an error inside apply is fail-stop: the caller returns
//!   [`TxnErr::Engine`], the engine is poisoned (every later call returns
//!   `Engine("poisoned")`), and the txn's locks are **not** released —
//!   the process is considered dead (PLAN-v3 §3.4 "apply cannot fail
//!   logically"; C-T0 §3 "the thread stops and the process aborts").
//! - C-GATE-SCOPE (card X-1a-F3): the test-only apply gate is
//!   **per-sequencer** state — each [`Sequencer`] owns its
//!   [`apply_gate::ApplyGate`] instance, which `commit_with` already
//!   receives — so a gate armed in one engine parks only that engine's
//!   commits, never another test's, and `run_all`s may run concurrently
//!   in one binary.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::api::{Ts, TxnErr, TxnId};
use crate::common::lock_table::LockTable;
use crate::common::sequencer::Sequencer;

/// Run the commit protocol for `txn` (C-COMMIT-ORDER): `apply` writes the
/// txn's versions into the version store under the ts handed out here.
/// See the module docs for the fail-stop rule (C-FAIL-STOP).
///
/// Shared by every arm and by the reference engine; arms must not
/// implement their own ordering (card Work 3).
pub fn commit_with<F>(
    seq: &Sequencer,
    locks: &LockTable,
    poison: &AtomicBool,
    txn: TxnId,
    apply: F,
) -> Result<Ts, TxnErr>
where
    F: FnOnce(Ts) -> Result<(), TxnErr>,
{
    // (1) ts = next_commit_ts
    let ts = seq.next_commit_ts();
    // Test hook (see [`apply_gate`]): parks between ts assignment and
    // apply so a scenario can hold a commit's ts pending — on this
    // sequencer's gate only (C-GATE-SCOPE).
    seq.apply_gate().maybe_park();
    // (2) apply(ts)
    if let Err(e) = apply(ts) {
        poison.store(true, Ordering::Release);
        return Err(TxnErr::Engine(format!(
            "commit apply failed at ts {ts} (fail-stop): {e}"
        )));
    }
    // (3) mark_applied(ts) — advances visible_ts in ts order
    seq.mark_applied(ts);
    // (4) only then release_all
    locks.release_all(txn);
    Ok(ts)
}

/// Test-only inter-commit hook for the card's VISIBLE-JUMPS mutant: it
/// lets a scenario hold one commit's **ts** assigned while delaying its
/// **apply**, so a second, higher commit can apply first and a buggy
/// `mark_applied` can be caught jumping `visible_ts` past the pending
/// lower ts.
///
/// Per-engine since card X-1a-F3 (C-GATE-SCOPE): the gate is an
/// [`ApplyGate`] instance owned by the [`Sequencer`]
/// ([`Sequencer::apply_gate`]), which `commit_with` already receives,
/// so arming a gate in one engine parks only that engine's commits and
/// never another test's. Never used outside tests. The gate is
/// reachable only through [`Sequencer::apply_gate`], so there is no
/// process-global path to it.
pub mod apply_gate {
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    /// Gate state: `armed` selects the next arriving `commit_with`;
    /// `taken` records that it parked; `released` lets it proceed;
    /// `parked` is the observable signal for
    /// [`ApplyGate::wait_parked`].
    struct State {
        armed: bool,
        taken: bool,
        released: bool,
        parked: bool,
    }

    impl State {
        const NEW: State = State {
            armed: false,
            taken: false,
            released: false,
            parked: false,
        };
    }

    /// One engine's apply gate (card X-1a-F3, C-GATE-SCOPE): the
    /// test-only hook `commit_with` consults between ts assignment and
    /// apply. Owned by a [`Sequencer`](crate::common::Sequencer) and
    /// reached through
    /// [`apply_gate`](crate::common::Sequencer::apply_gate), so each
    /// engine's commits park only in its own gate. The API:
    /// [`Self::arm`], [`Self::wait_parked`], [`Self::release`].
    ///
    /// The `armed` field is the fast-path flag of card X-1a-F1 Work 3:
    /// `true` only while this gate is armed. The hook on the commit
    /// path checks it **before** touching the mutex, so an unarmed
    /// (i.e. every production) commit pays one relaxed-fail atomic load
    /// instead of the gate's mutex — the fast path exists because the
    /// process-global predecessor of this gate put a mutex on every
    /// commit of both E1′ arms and pulled their CPU ratio toward 1.
    pub struct ApplyGate {
        armed: AtomicBool,
        state: Mutex<State>,
        cv: Condvar,
    }

    impl ApplyGate {
        /// A gate nobody ever armed — the permanent state outside tests.
        pub(crate) fn new() -> Self {
            Self {
                armed: AtomicBool::new(false),
                state: Mutex::new(State::NEW),
                cv: Condvar::new(),
            }
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, State> {
            match self.state.lock() {
                Ok(g) => g,
                // Unreachable (infallible critical sections); total anyway.
                Err(e) => e.into_inner(),
            }
        }

        /// Arm the gate: the next
        /// [`commit_with`](crate::common::commit_with) on **this gate's
        /// sequencer** parks after its ts assignment and before its
        /// apply. Dropping the guard releases the parked commit and
        /// disarms the gate.
        pub fn arm(&self) -> Arm<'_> {
            let mut s = self.lock();
            s.armed = true;
            s.taken = false;
            s.released = false;
            s.parked = false;
            // The fast-path flag `ARMED` (the `armed` atomic; card
            // X-1a-F1-F3) changes only under the gate mutex, so a
            // holder of the mutex always sees `ARMED == state.armed`:
            // the store sits inside this critical section, after the
            // state it mirrors. That keeps [`Self::maybe_park`]'s fast
            // path correct — an unarmed load returns without locking
            // (the gate was unarmed at that instant, and a commit that
            // already passed the hook is not one a later arm selects);
            // an armed load takes the mutex and re-checks
            // `armed`/`taken`, which the invariant keeps consistent
            // with the flag the load saw (a `release()` that ran in
            // between changed both under the mutex).
            self.armed.store(true, Ordering::Release);
            drop(s);
            Arm {
                gate: self,
                armed: true,
            }
        }

        /// Block until the armed commit has parked, or `timeout` passes.
        /// Returns whether it parked; a `false` return means the engine
        /// never reached commit (a scenario bug, reported as such).
        pub fn wait_parked(&self, timeout: Duration) -> bool {
            let deadline = Instant::now() + timeout;
            let mut s = self.lock();
            while !s.parked {
                let now = Instant::now();
                if now >= deadline {
                    return false;
                }
                let (guard, _) = match self
                    .cv
                    .wait_timeout(s, deadline.saturating_duration_since(now))
                {
                    Ok(w) => w,
                    Err(_) => return false,
                };
                s = guard;
            }
            true
        }

        /// Let the parked commit proceed and disarm the gate.
        pub fn release(&self) {
            let mut s = self.lock();
            s.released = true;
            s.armed = false;
            // The fast-path flag `ARMED` (the `armed` atomic; card
            // X-1a-F1-F3) changes only under the gate mutex, so a
            // holder of the mutex always sees `ARMED == state.armed`:
            // the store sits inside this critical section, before
            // unlocking. Stored after the unlock, it raced a fresh
            // `arm()` on another thread: release cleared the state and
            // unlocked, the arm set the state and stored
            // `ARMED = true`, and only then did release's late
            // `ARMED = false` land — erasing the new arm's flag, whose
            // commit then took maybe_park's fast path out and never
            // parked. Under the mutex the critical sections are strictly
            // ordered, which keeps [`Self::maybe_park`]'s fast path
            // correct: an unarmed load returns without locking; an
            // armed load takes the mutex and re-checks
            // `armed`/`taken`.
            self.armed.store(false, Ordering::Release);
            drop(s);
            self.cv.notify_all();
        }

        /// The hook `commit_with` calls between ts assignment and apply.
        /// Unarmed — the permanent state outside tests — it returns after
        /// one atomic load, without locking.
        pub(super) fn maybe_park(&self) {
            if !self.armed.load(Ordering::Acquire) {
                return;
            }
            let mut s = self.lock();
            if !s.armed || s.taken {
                return;
            }
            s.taken = true;
            s.parked = true;
            self.cv.notify_all();
            while !s.released {
                match self.cv.wait(s) {
                    Ok(g) => s = g,
                    Err(_) => return,
                }
            }
            s.parked = false;
        }
    }

    /// The guard returned by [`ApplyGate::arm`]: dropping it releases
    /// the parked commit and disarms the gate.
    pub struct Arm<'a> {
        gate: &'a ApplyGate,
        armed: bool,
    }

    impl Drop for Arm<'_> {
        fn drop(&mut self) {
            if self.armed {
                self.gate.release();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::thread;

    use super::commit_with;
    use crate::api::{TxnErr, Wait};
    use crate::common::{LockTable, Sequencer};

    /// C-COMMIT-ORDER, end to end: ts, apply, mark, release — and the
    /// visible ts is advanced before the locks are gone.
    #[test]
    fn protocol_marks_visible_before_releasing() {
        let seq = Sequencer::new();
        let locks = LockTable::new();
        let poison = AtomicBool::new(false);
        locks.acquire(b"k", 1, Wait::NoWait).unwrap();
        let ts = commit_with(&seq, &locks, &poison, 1, |ts| {
            assert_eq!(ts, 1);
            Ok(())
        })
        .unwrap();
        assert_eq!(ts, 1);
        assert_eq!(seq.visible_ts(), 1, "mark_applied ran inside commit");
        assert_eq!(locks.entries(), 0, "release_all ran after it");
        assert!(!poison.load(std::sync::atomic::Ordering::Acquire));
    }

    /// C-FAIL-STOP: an apply error returns Engine, sets the poison flag,
    /// and does **not** release the txn's locks or advance visible_ts.
    #[test]
    fn apply_failure_is_fail_stop() {
        let seq = Sequencer::new();
        let locks = LockTable::new();
        let poison = AtomicBool::new(false);
        locks.acquire(b"k", 5, Wait::NoWait).unwrap();
        let r = commit_with(&seq, &locks, &poison, 5, |_| {
            Err(TxnErr::Engine("simulated apply failure".into()))
        });
        assert!(matches!(r, Err(TxnErr::Engine(_))));
        assert!(poison.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            seq.visible_ts(),
            0,
            "the failed commit never became visible"
        );
        assert_eq!(locks.entries(), 1, "fail-stop keeps the locks");
    }

    /// The gate parks exactly one commit between ts and apply, in that
    /// order: while the first commit is parked, a second commit takes the
    /// **next** ts and runs to completion. The gate is per-sequencer
    /// (card X-1a-F3), so this test needs no serialization against the
    /// other tests: its sequencer's gate is its own.
    #[test]
    fn gate_parks_one_commit_after_its_ts() {
        let seq = Arc::new(Sequencer::new());
        let locks = Arc::new(LockTable::new());
        let gate = seq.apply_gate().arm();
        let (seq2, locks2) = (Arc::clone(&seq), Arc::clone(&locks));
        let parked = thread::spawn(move || {
            let poison = AtomicBool::new(false);
            commit_with(&seq2, &locks2, &poison, 1, |_| Ok(()))
        });
        assert!(
            seq.apply_gate()
                .wait_parked(std::time::Duration::from_secs(30))
        );
        // The parked commit already holds ts 1 (parking is after ts
        // assignment), so a second commit gets ts 2 and finishes first.
        let poison = AtomicBool::new(false);
        let ts2 = commit_with(&seq, &locks, &poison, 2, |_| Ok(())).unwrap();
        assert_eq!(ts2, 2);
        assert_eq!(
            seq.visible_ts(),
            0,
            "ts 1 is pending: the prefix cannot move"
        );
        seq.apply_gate().release();
        assert!(parked.join().is_ok_and(|r| r.is_ok()));
        assert_eq!(seq.visible_ts(), 2);
        drop(gate); // already disarmed; Drop is idempotent
    }
}
