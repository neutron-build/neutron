//! C-T2c-r5: the four ON CONFLICT / FK waits honor the statement's
//! `lock_timeout` (§6, 55P03) with ONE deadline per statement: it is
//! computed before the restart loop and shared by every wait, so a
//! statement that keeps restarting (every wait ending `GenChanged`, each
//! mapping to `Ok` and a restart) still hits 55P03 within the timeout —
//! a restart must not buy a fresh clock.
//!
//! Harness: each test pins one call site — the pre-check restart wait,
//! the arbiter lock wait, the locked-row update wait, the FK child KEY
//! SHARE wait, and the restart loop as a whole. The statement runs on a
//! DETACHED `std::thread::spawn` thread (the rig's `Arc<Core>` and the
//! `Txn` move in as `'static`), and the test waits with
//! `recv_timeout(2 s)`. A dropped deadline therefore FAILS the bound
//! instead of hanging the suite: the panic does not join the stuck
//! thread (the r4 gate's HIGH finding — `thread::scope` joined the still
//! blocked thread after the panic and the binary never exited), the
//! thread is left detached, and the statement's cancel handle is fired
//! first so the leaked thread can drain and return. After each 55P03 the
//! timed-out waiter left no wait edges and no wait slots, I-COUNT is
//! exact, and once the blocker finishes a retry of the statement
//! succeeds.
//!
//! Mutants: a site passing `None` (or recomputing the deadline at each
//! wait, the r4 behaviour) never returns and its test fails at the 2 s
//! bound with the binary still exiting.

mod c_t2c_support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use c_t2c_support::{as_u64, assert_count_exact, ok, u64v, Fixed, Rig};
use nucleus_txn::txn::{CancelHandle, Isolation};
use nucleus_txn::write::{OnConflictAction, OnConflictResult, ProposedRow, RowOp, StmtCtx};
use nucleus_txn::{RowLockMode, TxnError};

fn t_key(pk: &str) -> Vec<u8> {
    format!("/t/r/{pk}").into_bytes()
}

fn u_key(k: &str) -> Vec<u8> {
    format!("/u/a/{k}").into_bytes()
}

fn t_key_of(payload: &[u8]) -> Vec<u8> {
    let mut k = b"/t/r/".to_vec();
    k.extend_from_slice(payload);
    k
}

fn proposed(pk: &str, v: u64, arbiters: &[&str]) -> ProposedRow {
    ProposedRow {
        t_key: t_key(pk),
        value: u64v(v),
        entries: arbiters
            .iter()
            .map(|k| nucleus_txn::write::IndexEntry {
                key: u_key(k),
                value: pk.as_bytes().to_vec(),
                arbiter: true,
            })
            .collect(),
        pk_arbiter: false,
    }
}

fn preload_row(rig: &Rig, pk: &str, v: u64, k: &str) {
    rig.preload(&t_key(pk), &u64v(v));
    rig.preload(&u_key(k), pk.as_bytes());
}

/// Runs `f` on a detached thread and returns its result. If nothing
/// arrives within 2 s — the shape a dropped or endlessly refreshed
/// deadline produces — the statement's cancel handle is fired (the rig
/// has no generic blocker-release lever; cancelling the waiter is what
/// lets the leaked thread drain) and the test PANICS WITHOUT JOINING:
/// the stuck thread stays detached, so a regression fails the bound
/// instead of hanging the binary. `f` gets everything it owns moved in
/// (`Arc`-shared core, the txn) and may move the txn back out through
/// the result, so the caller can keep using the session.
fn bounded<R: Send + 'static>(leash: &CancelHandle, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(r) => r,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            leash.cancel();
            panic!("the statement ignored lock_timeout: still running at the 2 s bound");
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("the statement thread ended without a result");
        }
    }
}

/// The pinned post-55P03 state (card item 4): no wait edges, no wait
/// slots, I-COUNT exact.
fn assert_clean_timeout(
    core: &nucleus_txn::boot::Core<nucleus_kv::MemKv>,
    txn: &nucleus_txn::txn::Txn,
) {
    assert!(
        core.wait_edges().is_empty(),
        "the timed-out waiter left wait edges: {:?}",
        core.wait_edges()
    );
    assert!(
        core.wait_slots().is_empty(),
        "the timed-out waiter left wait slots: {:?}",
        core.wait_slots()
    );
    assert_count_exact(core, txn);
}

/// Site 1 — the pre-check restart wait: a pending writer of `r` blocks DO
/// NOTHING; the 50 ms `lock_timeout` surfaces as 55P03. Mutant: the
/// restart's `wait_on_any` drops the deadline.
#[test]
fn precheck_wait_honors_lock_timeout() {
    let rig = Rig::new();
    preload_row(&rig, "1", 0, "K");
    let mover = rig.txn(Isolation::ReadCommitted);
    let ts = ok(mover.next_seq());
    let op = RowOp::Update {
        value: u64v(1),
        key_cols_changed: false,
    };
    ok(rig.core.row_op(
        &mover,
        &t_key("1"),
        None,
        op.clone(),
        StmtCtx::new(rig.core.visible_ts(), ts, ts),
        &mut Fixed(op),
    ));
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let core = rig.core.clone();
    let leash = w.cancel_handle();
    let start = Instant::now();
    let (out, w) = bounded(&leash, move || {
        let r = core.insert_on_conflict(
            &w,
            StmtCtx::new(s, seq0, seq0).lock_timeout(Duration::from_millis(50)),
            proposed("2", 0, &["K"]),
            &t_key_of,
            &mut |_| {},
            OnConflictAction::DoNothing,
        );
        (r, w)
    });
    assert_eq!(out.map(|o| o.result), Err(TxnError::LockNotAvailable));
    assert!(start.elapsed() < Duration::from_secs(1), "prompt 55P03");
    assert_clean_timeout(&rig.core, &w);
    // The blocker finishes; a retry of the statement succeeds.
    rig.commit(mover);
    rig.resolve();
    let seq0 = ok(w.next_seq());
    let out = ok(rig.core.insert_on_conflict(
        &w,
        StmtCtx::new(rig.core.visible_ts(), seq0, seq0),
        proposed("2", 0, &["K"]),
        &t_key_of,
        &mut |_| {},
        OnConflictAction::DoNothing,
    ));
    assert_eq!(out.result, OnConflictResult::Nothing);
    ok(rig.core.abort(w));
}

/// Site 2 — the arbiter lock wait: a lock-only `NoKeyUpdate` holder of `r`
/// blocks the DO UPDATE arbiter lock; 55P03. Mutant: the lock loop's
/// `wait_on_any` drops the deadline.
#[test]
fn arbiter_lock_wait_honors_lock_timeout() {
    let rig = Rig::new();
    preload_row(&rig, "1", 0, "K");
    let holder = rig.txn(Isolation::ReadCommitted);
    let seq = ok(holder.next_seq());
    let lock = RowOp::Lock(RowLockMode::NoKeyUpdate);
    ok(rig.core.row_op(
        &holder,
        &t_key("1"),
        None,
        lock.clone(),
        StmtCtx::new(rig.core.visible_ts(), seq, seq),
        &mut Fixed(lock),
    ));
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let core = rig.core.clone();
    let leash = w.cancel_handle();
    let start = Instant::now();
    let (out, w) = bounded(&leash, move || {
        let mut upd = |_: &[u8]| Some((u64v(5), false));
        let r = core.insert_on_conflict(
            &w,
            StmtCtx::new(s, seq0, seq0).lock_timeout(Duration::from_millis(50)),
            proposed("1", 0, &["K"]),
            &t_key_of,
            &mut |_| {},
            OnConflictAction::DoUpdate(&mut upd),
        );
        (r, w)
    });
    assert_eq!(out.map(|o| o.result), Err(TxnError::LockNotAvailable));
    assert!(start.elapsed() < Duration::from_secs(1), "prompt 55P03");
    assert_clean_timeout(&rig.core, &w);
    // The blocker finishes; a retry of the statement succeeds.
    rig.commit(holder);
    rig.resolve();
    let seq0 = ok(w.next_seq());
    let out = ok(rig.core.insert_on_conflict(
        &w,
        StmtCtx::new(rig.core.visible_ts(), seq0, seq0),
        proposed("1", 0, &["K"]),
        &t_key_of,
        &mut |_| {},
        OnConflictAction::DoUpdate(&mut |v: &[u8]| Some((u64v(as_u64(v) + 5), false))),
    ));
    assert_eq!(out.result, OnConflictResult::Updated);
    ok(rig.core.abort(w));
}

/// Site 3 — the locked-row update wait: a KEY SHARE holder of `r` conflicts
/// with a key-changing DO UPDATE; 55P03. Mutant: the update loop's
/// `wait_on_any` drops the deadline.
#[test]
fn update_path_wait_honors_lock_timeout() {
    let rig = Rig::new();
    preload_row(&rig, "1", 0, "K");
    let holder = rig.txn(Isolation::ReadCommitted);
    let seq = ok(holder.next_seq());
    let share = RowOp::Lock(RowLockMode::KeyShare);
    ok(rig.core.row_op(
        &holder,
        &t_key("1"),
        None,
        share.clone(),
        StmtCtx::new(rig.core.visible_ts(), seq, seq),
        &mut Fixed(share),
    ));
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let core = rig.core.clone();
    let leash = w.cancel_handle();
    let start = Instant::now();
    let (out, w) = bounded(&leash, move || {
        let mut upd = |_: &[u8]| Some((u64v(5), true));
        let r = core.insert_on_conflict(
            &w,
            StmtCtx::new(s, seq0, seq0).lock_timeout(Duration::from_millis(50)),
            proposed("1", 0, &["K"]),
            &t_key_of,
            &mut |_| {},
            OnConflictAction::DoUpdate(&mut upd),
        );
        (r, w)
    });
    assert_eq!(out.map(|o| o.result), Err(TxnError::LockNotAvailable));
    assert!(start.elapsed() < Duration::from_secs(1), "prompt 55P03");
    assert_clean_timeout(&rig.core, &w);
    // The blocker finishes; a retry of the statement succeeds (the failed
    // statement's arbiter lock stays, the retry is a new statement).
    rig.commit(holder);
    rig.resolve();
    let seq0 = ok(w.next_seq());
    let out = ok(rig.core.insert_on_conflict(
        &w,
        StmtCtx::new(rig.core.visible_ts(), seq0, seq0),
        proposed("1", 0, &["K"]),
        &t_key_of,
        &mut |_| {},
        OnConflictAction::DoUpdate(&mut |v: &[u8]| Some((u64v(as_u64(v) + 5), true))),
    ));
    assert_eq!(out.result, OnConflictResult::Updated);
    ok(rig.core.abort(w));
}

/// Site 4 — the FK child check: an `Update`-strength holder of the parent
/// blocks the child's KEY SHARE; 55P03. Mutant: `fk_check_child`'s
/// `wait_on_any` drops the deadline.
#[test]
fn fk_child_wait_honors_lock_timeout() {
    let rig = Rig::new();
    rig.preload(b"/t/p/1", b"k1");
    let holder = rig.txn(Isolation::ReadCommitted);
    let seq = ok(holder.next_seq());
    // KEY SHARE conflicts only with UPDATE-strength holders (§6); a
    // NoKeyUpdate holder is compatible and would not block the check.
    let lock = RowOp::Lock(RowLockMode::Update);
    ok(rig.core.row_op(
        &holder,
        b"/t/p/1",
        None,
        lock.clone(),
        StmtCtx::new(rig.core.visible_ts(), seq, seq),
        &mut Fixed(lock),
    ));
    let c = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(c.next_seq());
    let s = rig.core.visible_ts();
    let ctx = StmtCtx::new(s, seq0, ok(c.next_seq()))
        .internal()
        .lock_timeout(Duration::from_millis(50));
    let core = rig.core.clone();
    let leash = c.cancel_handle();
    let start = Instant::now();
    let (out, c) = bounded(&leash, move || {
        let r = core.fk_check_child(&c, &ctx, b"/t/p/1", &|v| v == b"k1");
        (r, c)
    });
    assert_eq!(out, Err(TxnError::LockNotAvailable));
    assert!(start.elapsed() < Duration::from_secs(1), "prompt 55P03");
    assert_clean_timeout(&rig.core, &c);
    // The blocker finishes; a retry of the check succeeds.
    rig.commit(holder);
    rig.resolve();
    let seq = ok(c.next_seq());
    let s = rig.core.visible_ts();
    ok(rig
        .core
        .fk_check_child(&c, &StmtCtx::new(s, seq, seq).internal(), b"/t/p/1", &|v| {
            v == b"k1"
        }));
    ok(rig.core.abort(c));
}

/// One deadline per statement, restart-bounded: a statement whose every
/// wait ends `GenChanged` (so every restart returns and waits again) must
/// still hit 55P03 within `lock_timeout`. The blocker is a pending writer
/// of `r` whose wake generation a helper thread bumps in a loop — the
/// single-session stand-in for the two-session ON CONFLICT restart
/// livelock (ESCALATE-1, another card's): each bump ends the waiter's
/// current wait without ever releasing `r`, so the statement restarts for
/// as long as the bumper runs, and only the statement-wide deadline ends
/// it. Under the r4 behaviour (a fresh deadline per wait) 55P03 never
/// arrives and this test fails at the 2 s bound.
#[test]
fn restart_loop_still_hits_lock_timeout() {
    let rig = Rig::new();
    let parkers = rig.parkers();
    preload_row(&rig, "1", 0, "K");
    let blocker = rig.txn(Isolation::ReadCommitted);
    let ts = ok(blocker.next_seq());
    let op = RowOp::Update {
        value: u64v(1),
        key_cols_changed: false,
    };
    ok(rig.core.row_op(
        &blocker,
        &t_key("1"),
        None,
        op.clone(),
        StmtCtx::new(rig.core.visible_ts(), ts, ts),
        &mut Fixed(op),
    ));
    assert_count_exact(&rig.core, &blocker);

    // The bumper: bump the blocker's generation ~every ms until stopped,
    // hard-capped at ~3 s (past the 2 s bound, so a regressed build's
    // leaked statement thread also drains once the bumps end).
    let stop = Arc::new(AtomicBool::new(false));
    let bumper_stop = Arc::clone(&stop);
    let bumper_core = rig.core.clone();
    let blocker_id = blocker.id;
    let bumper = std::thread::spawn(move || {
        for _ in 0..3_000 {
            if bumper_stop.load(Ordering::SeqCst) {
                break;
            }
            ok(bumper_core.bump_and_wake(blocker_id));
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let core = rig.core.clone();
    let leash = w.cancel_handle();
    let attempts = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&attempts);
    let parks_before = parkers.parks();
    let start = Instant::now();
    let (out, w) = bounded(&leash, move || {
        let mut on_attempt = move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
        };
        let r = core.insert_on_conflict(
            &w,
            StmtCtx::new(s, seq0, seq0).lock_timeout(Duration::from_millis(300)),
            proposed("2", 0, &["K"]),
            &t_key_of,
            &mut on_attempt,
            OnConflictAction::DoNothing,
        );
        (r, w)
    });
    let elapsed = start.elapsed();
    stop.store(true, Ordering::SeqCst);
    if let Err(e) = bumper.join() {
        std::panic::resume_unwind(e);
    }
    assert_eq!(
        out.map(|o| o.result),
        Err(TxnError::LockNotAvailable),
        "the restart loop ended in 55P03, not a watchdog or a decision"
    );
    assert!(
        elapsed >= Duration::from_millis(300),
        "55P03 did not cut the wait short of lock_timeout ({} ms)",
        elapsed.as_millis()
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "55P03 arrived {elapsed:?} after statement start: later than lock_timeout + margin"
    );
    let attempts = attempts.load(Ordering::SeqCst);
    assert!(attempts > 1, "the statement restarted: {attempts} attempts");
    assert!(
        parkers.parks() > parks_before + 1,
        "the statement made more than one wait (parks {} -> {})",
        parks_before,
        parkers.parks()
    );
    assert_clean_timeout(&rig.core, &w);
    // The blocker finishes; a retry of the statement succeeds.
    ok(rig.core.abort(blocker));
    rig.resolve();
    let seq0 = ok(w.next_seq());
    let out = ok(rig.core.insert_on_conflict(
        &w,
        StmtCtx::new(rig.core.visible_ts(), seq0, seq0),
        proposed("2", 0, &["K"]),
        &t_key_of,
        &mut |_| {},
        OnConflictAction::DoNothing,
    ));
    assert_eq!(out.result, OnConflictResult::Nothing);
    ok(rig.core.abort(w));
}
