//! Deterministic checks of the W6 head-of-line mechanism (card X-1b Work
//! 2 W6): a 1-row txn that arrives while a big txn holds a key — during
//! its fill or during its commit apply — must wait for the big commit's
//! lock release, and that wait is what W6's probe measures.
//!
//! The scenarios use the engines' own test hooks
//! ([`TxnEngine::sequencer`]'s apply gate and
//! [`TxnEngine::lock_table`]'s parked-waiter count — the per-engine
//! hooks of cards X-1a-F3/X-2 Work 8), so the interleaving is forced,
//! not scheduling luck:
//!
//! - `head_of_line_blocks_through_the_big_commit`: the big txn holds one
//!   key; the probe's blocked `put` provably parks (the lock table's
//!   waiter count rises) **while the big commit is parked in the apply
//!   gate** — i.e. during the apply window — and completes only after
//!   the big commit's release, with a latency that covers the held
//!   apply.
//! - `locks_are_held_during_the_big_txns_fill`: during the fill (before
//!   any commit), a `Block` put on a locked key times out (55P03 never
//!   applies; a `Block` expiry is `LockTimeout`), proving the fill
//!   window blocks 1-row txns exactly as the apply window does.
//
// Tests may unwrap (clippy.toml's allow-unwrap-in-tests covers `#[test]`
// bodies; this file's shared driver lives in a helper, so the allow is
// explicit here).
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use e1_arm_b::ArmB;
use e1_arm_c::ArmC;
use e1_mvcc::api::{Isolation, TxnEngine, TxnErr, Wait};

/// A scratch dir for arm (c)'s spill file (the engine removes the file
/// on drop; the dir stays and is the test's own).
fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("e1-bench-w6-hol-{tag}-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&d);
    d
}

/// Drive one arm through the forced interleaving (module docs). The
/// hold time is the gate-parked apply duration; the probe's put must
/// outlast it.
fn forced_head_of_line<E: TxnEngine>(engine: Arc<E>) {
    // Preload one key so updates have a base.
    let mut seed = engine.begin(Isolation::ReadCommitted);
    engine.put(&mut seed, b"k", &[7; 40], Wait::NoWait).unwrap();
    engine.commit(seed).unwrap();

    // The big txn locks the key (fill complete from the key's side).
    let mut big = engine.begin(Isolation::ReadCommitted);
    engine.put(&mut big, b"k", &[9; 40], Wait::NoWait).unwrap();

    // Arm the apply gate: the big commit will park between its ts
    // assignment and its apply — inside W6's "apply" window.
    let gate = engine.sequencer().apply_gate().arm();
    let parked_apply = {
        let e = Arc::clone(&engine);
        thread::spawn(move || {
            let t0 = std::time::Instant::now();
            // The probe's 1-row txn: its put blocks on the big txn's
            // lock until the big commit releases it.
            let mut t = e.begin(Isolation::ReadCommitted);
            let r = e.put(&mut t, b"k", &[1; 40], Wait::Block(Duration::from_secs(60)));
            let commit = e.commit(t);
            (t0.elapsed().as_nanos() as u64, r, commit)
        })
    };

    // Start the big commit; it parks in the gate (apply window open).
    let big_thread = {
        let e = Arc::clone(&engine);
        thread::spawn(move || e.commit(big).unwrap())
    };
    assert!(
        engine
            .sequencer()
            .apply_gate()
            .wait_parked(Duration::from_secs(30)),
        "the big commit never parked in the apply gate"
    );

    // The probe must be parked on the lock table while the apply window
    // is open — the suite's ParkedBarrier observation (card X-1a-F3).
    let mut waited = 0;
    while engine.lock_table().waiting() == 0 && waited < 10_000 {
        thread::sleep(Duration::from_millis(1));
        waited += 1;
    }
    assert!(
        engine.lock_table().waiting() > 0,
        "the probe never parked on the big txn's lock during the apply window"
    );

    // Hold the apply for a measurable 150 ms, then let both finish.
    thread::sleep(Duration::from_millis(150));
    drop(gate); // release + disarm
    let big_ts = big_thread.join().unwrap();
    let (probe_ns, put, commit) = parked_apply.join().unwrap();
    assert!(put.is_ok(), "the probe's put failed: {put:?}");
    assert!(commit.is_ok(), "the probe's commit failed: {commit:?}");
    assert!(
        commit.unwrap() > big_ts,
        "the probe committed after the big txn"
    );

    // The probe's put waited at least the held apply: it parked before
    // the 150 ms hold began (proved above) and completed after the big
    // commit returned.
    assert!(
        probe_ns >= 140_000_000,
        "probe latency {probe_ns} ns did not cover the 150 ms held apply — \
         the head-of-line mechanism did not engage"
    );
}

/// Arm (b): the head-of-line wait covers the big commit's apply.
#[test]
fn arm_b_head_of_line_blocks_through_the_big_commit() {
    forced_head_of_line(Arc::new(ArmB::new(4096, 8192)));
}

/// Arm (c): the same forced interleaving on the undo arm.
#[test]
fn arm_c_head_of_line_blocks_through_the_big_commit() {
    forced_head_of_line(Arc::new(ArmC::new(4096, 8192, 64 << 20, dir("c"))));
}

/// The fill window blocks 1-row txns the same way: while the big txn
/// holds a key and has not committed, a `Block` put on that key can only
/// end in `LockTimeout` (never success, never 55P03).
#[test]
fn locks_are_held_during_the_big_txns_fill() {
    let engine = ArmB::new(4096, 8192);
    let mut seed = engine.begin(Isolation::ReadCommitted);
    engine.put(&mut seed, b"k", &[7; 40], Wait::NoWait).unwrap();
    engine.commit(seed).unwrap();
    let mut big = engine.begin(Isolation::ReadCommitted);
    engine.put(&mut big, b"k", &[9; 40], Wait::NoWait).unwrap();
    let mut probe = engine.begin(Isolation::ReadCommitted);
    assert_eq!(
        engine.put(
            &mut probe,
            b"k",
            &[1; 40],
            Wait::Block(Duration::from_millis(50))
        ),
        Err(TxnErr::LockTimeout),
        "a 1-row txn during the fill must wait on the big txn's lock"
    );
    engine.abort(probe);
    engine.abort(big);
}
