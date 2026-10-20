//! Per-engine test hooks (card X-1a-F3 Work 5). The suite's two hooks
//! — the lock table's parked-waiter count and the apply gate — used to
//! be process-global, which made the suite safe only while `run_all`
//! was the single `#[test]` of its binary. X-2 and X-3 add arm test
//! files, so these tests pin the per-engine reasoning they will rely
//! on:
//! - `waiting_is_per_table`: a waiter parked on one table's lock never
//!   shows in another table's count (catches `WAITING-GLOBAL`);
//! - `apply_gate_is_per_engine`: a gate armed on one engine parks only
//!   that engine's commit, never another engine's (catches
//!   `GATE-GLOBAL`);
//! - `concurrent_run_all_in_one_binary`: two `run_all`s race cleanly
//!   in one binary.

use std::thread;
use std::time::{Duration, Instant};

use e1_mvcc::api::{Isolation, TxnEngine, Wait};
use e1_mvcc::common::LockTable;
use e1_mvcc::reference::RefEngine;
use e1_mvcc::suite;

/// Card X-1a-F3 Work 5: park a waiter on table A (a 30 s block on a
/// held key); once A's count proves the waiter parked, table B's count
/// must still be zero, and A's count must drain when the waiter leaves.
/// Under the card's `WAITING-GLOBAL` mutant (the count a process-global
/// static again) B sees A's waiter and this fails.
#[test]
fn waiting_is_per_table() {
    let a = LockTable::new();
    let b = LockTable::new();
    a.acquire(b"k", 1, Wait::NoWait).unwrap();
    let b_while_parked = thread::scope(|s| {
        let a = &a;
        let waiter = s.spawn(move || a.acquire(b"k", 2, Wait::Block(Duration::from_secs(30))));
        let deadline = Instant::now() + Duration::from_secs(30);
        while a.waiting() == 0 {
            assert!(Instant::now() < deadline, "the waiter never parked on A");
            thread::yield_now();
        }
        let b_waiting = b.waiting();
        a.release_all(1);
        assert!(
            waiter.join().is_ok_and(|r| r.is_ok()),
            "the parked waiter woke and acquired"
        );
        b_waiting
    });
    assert_eq!(
        b_while_parked, 0,
        "table B must not see table A's parked waiter"
    );
    assert_eq!(a.waiting(), 0, "the count drained when the waiter left");
}

/// Card X-1a-F3 Work 5: with engine A's gate armed, a commit on engine
/// B completes promptly (within 5 s, on a thread, joined) and does not
/// park; a commit on A parks, and `wait_parked` observes it. Under the
/// card's `GATE-GLOBAL` mutant (one shared gate instance for every
/// engine) B's commit parks on A's armed gate and this fails.
#[test]
fn apply_gate_is_per_engine() {
    let a = RefEngine::new();
    let b = RefEngine::new();
    for e in [&a, &b] {
        let mut t = e.begin(Isolation::ReadCommitted);
        e.put(&mut t, b"k", b"seed", Wait::NoWait).unwrap();
        e.commit(t).unwrap();
    }
    thread::scope(|s| {
        let (a, b) = (&a, &b);
        let gate = a.sequencer().apply_gate().arm();
        // Engine B's commit must run straight through A's armed gate.
        let (tx, rx) = std::sync::mpsc::channel();
        let tb = s.spawn(move || {
            let mut t = b.begin(Isolation::ReadCommitted);
            b.put(&mut t, b"k", b"wb", Wait::NoWait).unwrap();
            let _ = tx.send(b.commit(t));
        });
        let b_result = rx.recv_timeout(Duration::from_secs(5));
        if b_result.is_err() {
            // Something parked on A's gate: release it so the thread
            // can end and the scope can join it, then report.
            a.sequencer().apply_gate().release();
            let _ = tb.join();
            panic!("engine B's commit parked on engine A's armed gate");
        }
        match b_result {
            Ok(Ok(_)) => {}
            other => panic!("engine B's commit failed: {other:?}"),
        }
        tb.join().unwrap();
        // Engine A's own commit parks in the armed gate.
        let (tx2, rx2) = std::sync::mpsc::channel();
        let ta = s.spawn(move || {
            let mut t = a.begin(Isolation::ReadCommitted);
            a.put(&mut t, b"k", b"wa", Wait::NoWait).unwrap();
            let _ = tx2.send(a.commit(t));
        });
        assert!(
            a.sequencer()
                .apply_gate()
                .wait_parked(Duration::from_secs(5)),
            "engine A's commit must park in its own armed gate"
        );
        a.sequencer().apply_gate().release();
        assert!(
            rx2.recv_timeout(Duration::from_secs(5))
                .is_ok_and(|r| r.is_ok()),
            "A's parked commit never finished after the release"
        );
        ta.join().unwrap();
        drop(gate); // already released; Drop is idempotent
    });
}

/// Card X-1a-F3 Work 5: two `run_all`s in one binary at the same time.
/// With per-engine hooks, neither run's barrier can be satisfied by the
/// other run's parked waiters and neither run's gate can park the other
/// run's commits, so both must return no failures.
#[test]
fn concurrent_run_all_in_one_binary() {
    thread::scope(|s| {
        let r1 = s.spawn(|| suite::run_all(&RefEngine::new));
        let r2 = s.spawn(|| suite::run_all(&RefEngine::new));
        let f1 = r1.join().unwrap();
        let f2 = r2.join().unwrap();
        assert!(f1.is_empty(), "concurrent run_all #1 failures: {f1:#?}");
        assert!(f2.is_empty(), "concurrent run_all #2 failures: {f2:#?}");
    });
}
