//! C-T2c-r4: the four ON CONFLICT / FK waits honor the statement's
//! `lock_timeout` (§6, 55P03). Each test pins one call site: the pre-check
//! restart wait, the arbiter lock wait, the locked-row update wait, and the
//! FK child KEY SHARE wait. A blocker holds the conflicting lock; the
//! statement runs with `lock_timeout(50 ms)` on a bounded thread, so a
//! dropped deadline fails the bound instead of hanging the suite.
//!
//! Mutant (each test): the site calls `wait_on_any` without the deadline —
//! the call then never returns and the 2 s bound panics.

mod c_t2c_support;

use std::sync::mpsc;
use std::time::{Duration, Instant};

use c_t2c_support::{assert_count_exact, ok, u64v, Fixed, Rig};
use nucleus_txn::txn::Isolation;
use nucleus_txn::write::{OnConflictAction, ProposedRow, RowOp, StmtCtx};
use nucleus_txn::RowLockMode;

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

/// Runs `f` on a scoped thread and returns its result, failing (not
/// hanging) if it takes longer than 2 s — the shape a dropped deadline
/// produces.
fn bounded<R: Send>(f: impl FnOnce() -> R + Send) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(Duration::from_secs(2))
            .expect("the wait ignored lock_timeout and never returned")
    })
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
    let core = &rig.core;
    let start = Instant::now();
    let out = bounded(move || {
        core.insert_on_conflict(
            &w,
            StmtCtx::new(s, seq0, seq0).lock_timeout(Duration::from_millis(50)),
            proposed("2", 0, &["K"]),
            &t_key_of,
            &mut |_| {},
            OnConflictAction::DoNothing,
        )
    });
    assert_eq!(
        out.map(|o| o.result),
        Err(nucleus_txn::TxnError::LockNotAvailable)
    );
    assert!(start.elapsed() < Duration::from_secs(1), "prompt 55P03");
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
    let core = &rig.core;
    let start = Instant::now();
    let out = bounded(move || {
        let mut upd = |_: &[u8]| Some((u64v(5), false));
        let r = core.insert_on_conflict(
            &w,
            StmtCtx::new(s, seq0, seq0).lock_timeout(Duration::from_millis(50)),
            proposed("1", 0, &["K"]),
            &t_key_of,
            &mut |_| {},
            OnConflictAction::DoUpdate(&mut upd),
        );
        r
    });
    assert_eq!(
        out.map(|o| o.result),
        Err(nucleus_txn::TxnError::LockNotAvailable)
    );
    assert!(start.elapsed() < Duration::from_secs(1), "prompt 55P03");
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
    let core = &rig.core;
    let start = Instant::now();
    let out = bounded(move || {
        let mut upd = |_: &[u8]| Some((u64v(5), true));
        let r = core.insert_on_conflict(
            &w,
            StmtCtx::new(s, seq0, seq0).lock_timeout(Duration::from_millis(50)),
            proposed("1", 0, &["K"]),
            &t_key_of,
            &mut |_| {},
            OnConflictAction::DoUpdate(&mut upd),
        );
        r
    });
    assert_eq!(
        out.map(|o| o.result),
        Err(nucleus_txn::TxnError::LockNotAvailable)
    );
    assert!(start.elapsed() < Duration::from_secs(1), "prompt 55P03");
}

/// Site 4 — the FK child check: a `NoKeyUpdate` holder of the parent blocks
/// the child's KEY SHARE; 55P03. Mutant: `fk_check_child`'s `wait_on_any`
/// drops the deadline.
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
    let core = &rig.core;
    let start = Instant::now();
    let out = bounded(move || {
        let r = core.fk_check_child(&c, &ctx, b"/t/p/1", &|v| v == b"k1");
        assert_count_exact(core, &c);
        r
    });
    assert_eq!(out, Err(nucleus_txn::TxnError::LockNotAvailable));
    assert!(start.elapsed() < Duration::from_secs(1), "prompt 55P03");
}
