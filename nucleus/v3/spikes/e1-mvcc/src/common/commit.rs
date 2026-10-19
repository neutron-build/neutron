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
    // apply so a scenario can hold a commit's ts pending.
    apply_gate::maybe_park();
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
/// Global because the [`crate::api::TxnEngine`] trait (card Work 2)
/// exposes no engine-specific hooks; scenarios run sequentially inside
/// [`crate::suite::run_all`], so a process-global gate is safe there.
/// Never used outside tests.
pub mod apply_gate {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::{Duration, Instant};

    /// Fast-path flag: `true` only while a gate is armed. The hook on
    /// the commit path checks this **before** touching the mutex, so an
    /// unarmed (i.e. every production) commit pays one relaxed-fail
    /// atomic load instead of a process-global mutex — card X-1a-F1
    /// Work 3, which exists because that mutex sat on every commit of
    /// both E1′ arms and pulled their CPU ratio toward 1.
    static ARMED: AtomicBool = AtomicBool::new(false);

    /// Gate state: `armed` selects the next arriving `commit_with`;
    /// `taken` records that it parked; `released` lets it proceed;
    /// `parked` is the observable signal for [`wait_parked`].
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

    static STATE: Mutex<State> = Mutex::new(State::NEW);
    static CV: Condvar = Condvar::new();

    fn lock() -> std::sync::MutexGuard<'static, State> {
        match STATE.lock() {
            Ok(g) => g,
            // Unreachable (infallible critical sections); total anyway.
            Err(e) => e.into_inner(),
        }
    }

    /// Arm the gate: the next [`crate::common::commit_with`] to arrive
    /// parks after its ts assignment and before its apply. Dropping the
    /// guard releases the parked commit and disarms the gate.
    pub struct Arm {
        armed: bool,
    }

    impl Drop for Arm {
        fn drop(&mut self) {
            if self.armed {
                release();
            }
        }
    }

    /// Arm the gate (see [`Arm`]).
    pub fn arm() -> Arm {
        let mut s = lock();
        s.armed = true;
        s.taken = false;
        s.released = false;
        s.parked = false;
        drop(s);
        // Publish the flag only after the state is set: a committer
        // that loads `ARMED == true` and then takes the mutex is
        // guaranteed to see `state.armed == true` (Release store here,
        // Acquire load in [`maybe_park`]). Every test arms the gate
        // **before** spawning the committing thread, so the thread's
        // creation already orders the state before its first commit;
        // the flag keeps the general case honest anyway.
        ARMED.store(true, Ordering::Release);
        Arm { armed: true }
    }

    /// Block until the armed commit has parked, or `timeout` passes.
    /// Returns whether it parked; a `false` return means the engine
    /// never reached commit (a scenario bug, reported as such).
    pub fn wait_parked(timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut s = lock();
        while !s.parked {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _) = match CV.wait_timeout(s, deadline.saturating_duration_since(now)) {
                Ok(w) => w,
                Err(_) => return false,
            };
            s = guard;
        }
        true
    }

    /// Let the parked commit proceed and disarm the gate.
    pub fn release() {
        let mut s = lock();
        s.released = true;
        s.armed = false;
        drop(s);
        CV.notify_all();
        // Clear the flag after the state: a committer that already
        // passed the fast path just before this store takes the mutex
        // and then sees `armed == false` (or `released == true`) and
        // returns without parking.
        ARMED.store(false, Ordering::Release);
    }

    /// The hook [`crate::common::commit_with`] calls between ts
    /// assignment and apply. Unarmed — the permanent state outside
    /// tests — it returns after one atomic load, without locking.
    pub(super) fn maybe_park() {
        if !ARMED.load(Ordering::Acquire) {
            return;
        }
        let mut s = lock();
        if !s.armed || s.taken {
            return;
        }
        s.taken = true;
        s.parked = true;
        CV.notify_all();
        while !s.released {
            match CV.wait(s) {
                Ok(g) => s = g,
                Err(_) => return,
            }
        }
        s.parked = false;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicBool;
    use std::thread;

    use super::{apply_gate, commit_with};
    use crate::api::{TxnErr, Wait};
    use crate::common::{LockTable, Sequencer};

    /// The apply gate is process-global, so the tests that run
    /// `commit_with` (any of which may park in an armed gate) must not
    /// overlap: a parallel test's commit could steal the armed gate from
    /// the test that armed it. The suite is unaffected — `run_all` runs
    /// its scenarios sequentially inside one test.
    fn serial() -> std::sync::MutexGuard<'static, ()> {
        static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
        match SERIAL.get_or_init(|| Mutex::new(())).lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        }
    }

    /// C-COMMIT-ORDER, end to end: ts, apply, mark, release — and the
    /// visible ts is advanced before the locks are gone.
    #[test]
    fn protocol_marks_visible_before_releasing() {
        let _serial = serial();
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
        let _serial = serial();
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
    /// **next** ts and runs to completion.
    #[test]
    fn gate_parks_one_commit_after_its_ts() {
        let _serial = serial();
        let seq = Arc::new(Sequencer::new());
        let locks = Arc::new(LockTable::new());
        let gate = apply_gate::arm();
        let (seq2, locks2) = (Arc::clone(&seq), Arc::clone(&locks));
        let parked = thread::spawn(move || {
            let poison = AtomicBool::new(false);
            commit_with(&seq2, &locks2, &poison, 1, |_| Ok(()))
        });
        assert!(apply_gate::wait_parked(std::time::Duration::from_secs(30)));
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
        apply_gate::release();
        assert!(parked.join().is_ok_and(|r| r.is_ok()));
        assert_eq!(seq.visible_ts(), 2);
        drop(gate); // already disarmed; Drop is idempotent
    }
}
