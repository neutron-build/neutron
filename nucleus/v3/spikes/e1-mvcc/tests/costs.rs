//! Deterministic cost-shape tests of the shared components (card
//! X-1a-F1 Work 6). No timing assertions: every complexity claim is
//! observed through the lock table's `#[doc(hidden)]` counters
//! (`shard_visits`, `escalation_attempts`) or through behaviour alone.
//!
//! These pin the three costs the X-1a verdict's F1 follow-up named: a
//! `release_all` that is O(the txn's own keys) instead of O(every lock
//! in the table), an escalation that defers under a foreign holder at a
//! bounded retry cadence instead of rescanning on every acquire, and a
//! commit path whose apply-gate hook costs one atomic load while
//! unarmed.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;

use e1_mvcc::api::{TxnErr, Wait};
use e1_mvcc::common::commit::apply_gate;
use e1_mvcc::common::{LockTable, Sequencer, commit_with};

/// Work 1: `release_all` of a 1-row txn touches only that row's shard
/// while another txn holds 100k rows, and leaves the other txn intact.
#[test]
fn release_touches_only_own_shards() {
    const A_ROWS: u64 = 100_000;
    let lt = LockTable::new();
    for i in 0..A_ROWS {
        lt.acquire(&i.to_be_bytes(), 1, Wait::NoWait).unwrap();
    }
    lt.acquire(b"b-key", 2, Wait::NoWait).unwrap();
    assert_eq!(lt.entries(), A_ROWS + 1);
    assert_eq!(
        lt.shard_visits(),
        0,
        "acquiring alone must not visit shards through release/escalate"
    );
    lt.release_all(2);
    assert!(
        lt.shard_visits() <= 1,
        "release_all of one row visited {} key shards",
        lt.shard_visits()
    );
    // entries() drops by exactly 1 ...
    assert_eq!(lt.entries(), A_ROWS);
    // ... B's key is acquirable again ...
    assert!(lt.acquire(b"b-key", 3, Wait::NoWait).is_ok());
    // ... and A's rows are all still held.
    assert_eq!(lt.held_by(1), A_ROWS);
    assert_eq!(lt.entries(), A_ROWS + 1);
}

/// Work 1: a waiter blocked on the released txn's key wakes and
/// acquires (L-RELEASE's waking half).
#[test]
fn release_wakes_waiter_on_released_key() {
    let lt = Arc::new(LockTable::new());
    lt.acquire(b"k", 2, Wait::NoWait).unwrap();
    let lt2 = Arc::clone(&lt);
    let waiter = thread::spawn(move || lt2.acquire(b"k", 5, Wait::Block(Duration::from_secs(5))));
    thread::sleep(Duration::from_millis(100)); // let the waiter park
    lt.release_all(2);
    assert!(
        waiter.join().is_ok_and(|r| r.is_ok()),
        "the waiter woke and acquired the released key"
    );
    assert_eq!(lt.held_by(5), 1);
}

/// Work 4 / L-ESCALATE-DEFER: with a foreign row held, a big txn never
/// escalates, retries at most once per retry step (not once per
/// acquire), and cannot steal the foreign row; once the foreign holder
/// releases, the escalation lands within one more retry step.
#[test]
fn escalation_defers_under_foreign_holder() {
    const A_ROWS: u64 = 20_000;
    const ESCALATE_AT: usize = 1000;
    let lt = LockTable::with_escalate_at(ESCALATE_AT);
    let step = lt.escalation_retry_step() as u64;
    assert_eq!(step, (ESCALATE_AT / 8) as u64, "max(escalate_at/8, 1)");
    lt.acquire(b"foreign", 2, Wait::NoWait).unwrap();
    for i in 0..A_ROWS {
        lt.acquire(&i.to_be_bytes(), 1, Wait::NoWait).unwrap();
    }
    assert_eq!(lt.escalations(), 0, "a foreign row defers every attempt");
    let attempts = lt.escalation_attempts();
    assert!(
        attempts > 0,
        "the budget was crossed, so at least one attempt happened"
    );
    assert!(
        attempts <= 2 + 19_000 / step,
        "one attempt per retry step was exceeded: {attempts} attempts"
    );
    // The foreign row is still exclusive: A cannot claim it with NoWait.
    assert_eq!(
        lt.acquire(b"foreign", 1, Wait::NoWait),
        Err(TxnErr::LockNotAvailable)
    );
    // Release the foreign holder: within one more retry step, A
    // escalates and then holds 0 rows.
    lt.release_all(2);
    let mut extra = 0u64;
    while !lt.is_escalated(1) {
        assert!(
            extra < step,
            "escalation needed more than one retry step after the release"
        );
        lt.acquire(&(A_ROWS + extra).to_be_bytes(), 1, Wait::NoWait)
            .unwrap();
        extra += 1;
    }
    assert_eq!(lt.escalations(), 1);
    assert_eq!(lt.held_by(1), 0, "the rows were replaced by the owner");
    assert_eq!(lt.entries(), 1, "only the table-wide owner remains");
}

/// Work 4: after a successful escalation, every other txn is excluded
/// from every key (L-EXCL), including keys the escalant never touched.
#[test]
fn escalation_excludes_foreign_after_success() {
    let lt = LockTable::with_escalate_at(4);
    for i in 0..5u64 {
        lt.acquire(&i.to_be_bytes(), 1, Wait::NoWait).unwrap();
    }
    assert!(lt.is_escalated(1));
    assert_eq!(lt.escalations(), 1);
    assert_eq!(
        lt.acquire(b"never-touched", 7, Wait::NoWait),
        Err(TxnErr::LockNotAvailable)
    );
    assert_eq!(
        lt.acquire(&0u64.to_be_bytes(), 7, Wait::NoWait),
        Err(TxnErr::LockNotAvailable)
    );
}

/// Work 1: releasing an escalated txn clears the table owner and wakes
/// a waiter parked on it.
#[test]
fn release_all_after_escalation_clears_owner() {
    let lt = Arc::new(LockTable::with_escalate_at(2));
    for i in 0..3u64 {
        lt.acquire(&i.to_be_bytes(), 1, Wait::NoWait).unwrap();
    }
    assert!(lt.is_escalated(1));
    assert_eq!(lt.entries(), 1);
    let lt2 = Arc::clone(&lt);
    let waiter = thread::spawn(move || lt2.acquire(b"w", 5, Wait::Block(Duration::from_secs(5))));
    thread::sleep(Duration::from_millis(100)); // let the waiter park on the owner
    lt.release_all(1);
    assert!(
        waiter.join().is_ok_and(|r| r.is_ok()),
        "the waiter on the table owner woke and acquired"
    );
    assert_eq!(lt.held_by(5), 1);
    assert_eq!(lt.entries(), 1);
}

/// Work 3: the apply gate's fast path. Unarmed (the state of every
/// production commit), 10,000 commits park nothing; armed, exactly one
/// commit parks — including when the committing thread was spawned
/// after `arm()`.
#[test]
fn apply_gate_fast_path() {
    let seq = Arc::new(Sequencer::new());
    let locks = Arc::new(LockTable::new());
    // Unarmed: none of 10,000 commits may park. `wait_parked` returns
    // true the moment any commit parks, so this is deterministic: a
    // parked commit is seen, an unparked run merely spends the timeout.
    {
        let (seq2, locks2) = (Arc::clone(&seq), Arc::clone(&locks));
        let commits = thread::spawn(move || {
            let poison = AtomicBool::new(false);
            let mut last = Ok(0);
            for _ in 0..10_000 {
                last = commit_with(&seq2, &locks2, &poison, 1, |_| Ok(()));
            }
            last
        });
        assert!(
            !apply_gate::wait_parked(Duration::from_secs(2)),
            "an unarmed gate parked a commit"
        );
        assert!(
            commits.join().is_ok_and(|r| r.is_ok()),
            "the unarmed commits all completed"
        );
    }
    // Armed: exactly one commit parks — here on a thread spawned after
    // arm() — and a second commit arriving while the gate is taken
    // runs through without parking.
    {
        let _gate = apply_gate::arm();
        let (seq2, locks2) = (Arc::clone(&seq), Arc::clone(&locks));
        let parked = thread::spawn(move || {
            let poison = AtomicBool::new(false);
            commit_with(&seq2, &locks2, &poison, 2, |_| Ok(()))
        });
        assert!(
            apply_gate::wait_parked(Duration::from_secs(10)),
            "the armed commit, spawned after arm(), never parked"
        );
        let poison = AtomicBool::new(false);
        assert!(
            commit_with(&seq, &locks, &poison, 3, |_| Ok(())).is_ok(),
            "the second armed-but-taken commit must not park"
        );
        apply_gate::release();
        assert!(parked.join().is_ok_and(|r| r.is_ok()));
    }
}
