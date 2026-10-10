//! C-T2b scenario tests over the real shared row-lock table (§6): C-T2's
//! shared-lock seeds rebuilt with [`LockManager::install`] (no
//! `tests/common` helper and no `TestRowLocks` double), the §6 conflict
//! behaviour of KEY SHARE / SHARE / FOR UPDATE against intents and
//! holders, and seed 17's `wait_begin` generation re-check. Interleavings
//! are driven by hand on one thread through the step APIs; each test
//! names the mutant it must kill.

mod locks_support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use locks_support::{hold_latch, lock_op, lock_row, not_within_200ms, ok, upd, Fixed, Rig};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::wait::WaitOutcome;
use nucleus_txn::write::{
    EpqDecision, RowLocks as _, RowOp, RowOpTask, RowOutcome, SkipReason, Step, StmtCtx, UniqueRule,
};
use nucleus_txn::{RowLockMode, TxnError, TxnStatus};

/// Small helper: map a `WaitOutcome` to `Ok` (the driver's retry
/// semantics) in the hand-driven step loops.
fn wait_retry(o: WaitOutcome) -> Result<(), TxnError> {
    assert!(
        !matches!(
            o,
            WaitOutcome::Cancelled | WaitOutcome::Deadlock | WaitOutcome::LockTimeout
        ),
        "unexpected wait outcome {o:?}"
    );
    Ok(())
}

// ---- seed 45: KEY SHARE examines all newer versions ------------------------

/// Seed 45's setup (C-T2's, rebuilt on the real table): preload t0; Ta
/// deletes it and commits; Tb re-inserts over the tombstone and commits.
/// A snapshot below both holds Tb's live write **and** Ta's tombstone in
/// N. The RR KEY SHARE request raises 40001 (the tombstone is examined,
/// not only the newest version); the RC one skips through the EPQ pass for
/// the same reason; and a request at the current snapshot is granted into
/// the real table and released by abort.
/// Mutant (C-T2's): `above_s.iter().take(1)` — only the newest examined
/// makes the RC request grant instead of skip.
#[test]
fn seed45_keyshare_examines_all_newer_real_table() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let s_below = rig.core.visible_ts();
    {
        let ta = rig.txn(Isolation::ReadCommitted);
        let sa = ok(ta.next_seq());
        ok(rig.core.row_op(
            &ta,
            b"/t/1/k",
            None,
            RowOp::Delete,
            StmtCtx::new(rig.core.visible_ts(), sa, sa),
            &mut Fixed(RowOp::Delete),
        ));
        rig.commit(ta);
        ok(Resolver::run_once(&rig.core));
        ok(Resolver::run_once(&rig.core));
    }
    {
        let tb = rig.txn(Isolation::ReadCommitted);
        let sb = ok(tb.next_seq());
        ok(rig.core.insert_key(
            &tb,
            b"/t/1/k",
            None,
            b"v".to_vec(),
            StmtCtx::new(rig.core.visible_ts(), sb, sb),
            UniqueRule::Unique { same_row: None },
        ));
        rig.commit(tb);
        ok(Resolver::run_once(&rig.core));
        ok(Resolver::run_once(&rig.core));
    }
    assert!(rig.core.visible_ts() > s_below);

    // RR with S below both: the tombstone in N fails KEY SHARE.
    let rr = rig.txn(Isolation::RepeatableRead);
    let sq = ok(rr.next_seq());
    let err = rig
        .core
        .row_op(
            &rr,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::KeyShare),
            StmtCtx::new(s_below, sq, sq),
            &mut Fixed(lock_op(RowLockMode::KeyShare)),
        )
        .expect_err("a tombstone inside N blocks KEY SHARE (seed 45)");
    assert_eq!(err, TxnError::SerializationFailure);
    ok(rig.core.abort(rr));

    // RC with S below both (the EPQ pass of a KEY SHARE request examines
    // every version above S): the tombstone below Tb's live write fails
    // it, so the row is skipped even though the newest version is live.
    let rc = rig.txn(Isolation::ReadCommitted);
    let sb = ok(rc.next_seq());
    assert_eq!(
        ok(rig.core.row_op(
            &rc,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::KeyShare),
            StmtCtx::new(s_below, sb, sb),
            &mut Fixed(lock_op(RowLockMode::KeyShare)),
        )),
        RowOutcome::Skipped(SkipReason::EpqFailed),
        "the tombstone above S fails the KEY SHARE EPQ (seed 45, RC)"
    );
    ok(rig.core.abort(rc));

    // At the current snapshot N is empty: the grant lands in the real
    // table, tagged with the statement's seq, and abort releases it.
    let rc = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(rc.next_seq());
    assert_eq!(
        lock_row(&rig.core, &rc, s2, b"/t/1/k", RowLockMode::KeyShare),
        RowOutcome::Applied
    );
    let holders = rig.locks.row_table().holders(b"/t/1/k");
    assert_eq!(holders, vec![(rc.id, RowLockMode::KeyShare, s2)]);
    ok(rig.core.abort(rc));
    assert!(
        rig.locks.row_table().holders(b"/t/1/k").is_empty(),
        "abort released the shared lock through the real table"
    );
}

// ---- seed 52: EPQ once over a non-conflicting intent -----------------------

/// Seed 52 rebuilt on the real table: after one EPQ pass re-bases the
/// task, a pending foreign NO KEY UPDATE intent is passed without a
/// second EPQ, the KEY SHARE is granted into the real table, and abort
/// releases it.
/// Mutant (C-T2's): EPQ repeats on the non-conflicting intent — the
/// second `epq_result` call errors (no remembered version to re-base on).
#[test]
fn seed52_epq_once_real_table() {
    let mut rig = Rig::new();
    let ts1 = rig.preload(b"/t/1/k", &1u64.to_be_bytes());
    {
        let a = rig.txn(Isolation::ReadCommitted);
        let sa = ok(a.next_seq());
        ok(rig.core.row_op(
            &a,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: 2u64.to_be_bytes().to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(ts1, sa, sa),
            &mut Fixed(RowOp::Update {
                value: 2u64.to_be_bytes().to_vec(),
                key_cols_changed: false,
            }),
        ));
        rig.commit(a);
        ok(Resolver::run_once(&rig.core));
        ok(Resolver::run_once(&rig.core));
    }
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    let mut task = RowOpTask::new(
        b"/t/1/k",
        None,
        lock_op(RowLockMode::KeyShare),
        StmtCtx::new(ts1, s1, s1),
    );
    // A second txn holds a pending NO KEY UPDATE intent on k after T1's
    // first EPQ: the KEY SHARE requester must pass it (no conflict, no
    // second EPQ).
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    upd(&rig.core, &t2, s2, b"/t/1/k", b"t2");

    let mut epqs = 0usize;
    let mut steps = 0usize;
    let outcome = loop {
        steps += 1;
        assert!(steps < 20, "too many steps");
        match ok(task.step(&rig.core, &t1)) {
            Step::Epq(_) => {
                epqs += 1;
                ok(task.epq_result(EpqDecision::Apply(lock_op(RowLockMode::KeyShare))));
            }
            Step::Done(o) => break o,
            Step::Again => {}
            Step::Wait(w) => wait_retry(rig.core.wait_on_any(&t1, &w)).expect("retry"),
            other => panic!("unexpected step {other:?}"),
        }
    };
    assert_eq!(outcome, RowOutcome::Applied);
    assert_eq!(epqs, 1, "exactly one EPQ (seed 52)");
    let holders = rig.locks.row_table().holders(b"/t/1/k");
    assert_eq!(holders, vec![(t1.id, RowLockMode::KeyShare, s1)]);

    ok(rig.core.abort(t1));
    ok(rig.core.abort(t2));
    ok(Resolver::run_once(&rig.core));
    assert!(
        rig.locks.row_table().holders(b"/t/1/k").is_empty(),
        "abort released both txns' in-memory state"
    );
}

// ---- seed 46: shared release under the key's latch -------------------------

/// Seed 46 rebuilt on the real table: `ROLLBACK TO` and commit step 5
/// release a SHARE lock only under the key's latch. The latch is held on
/// another thread; the release must not complete within 200 ms and the
/// table must still hold the lock; after the latch is released it
/// completes.
/// Mutant: `release_row_locks` without the latch (the release completes
/// while the latch is held).
#[test]
fn seed46_shared_release_under_latch_real_table() {
    let rig = Rig::new();
    // ROLLBACK TO half.
    {
        let t = rig.txn(Isolation::ReadCommitted);
        let sp = ok(t.savepoint());
        let s = ok(t.next_seq());
        assert_eq!(
            lock_row(&rig.core, &t, s, b"/t/1/k", RowLockMode::Share),
            RowOutcome::Applied
        );
        let holder = hold_latch(&rig.core, b"/t/1/k");
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        {
            let core = Arc::clone(&rig.core);
            let t = Arc::new(t);
            std::thread::spawn(move || {
                let _ = core.rollback_to(&t, sp);
                let _ = tx.send(());
            });
        }
        not_within_200ms(&rx);
        assert!(
            !rig.locks.row_table().holders(b"/t/1/k").is_empty(),
            "the shared lock was released without the key's latch"
        );
        holder.release();
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_ok(),
            "the rollback finished after the release"
        );
        assert!(rig.locks.row_table().holders(b"/t/1/k").is_empty());
    }
    // Commit step 5 half.
    {
        let t2 = rig.txn(Isolation::ReadCommitted);
        let s2 = ok(t2.next_seq());
        assert_eq!(
            lock_row(&rig.core, &t2, s2, b"/t/1/k", RowLockMode::Share),
            RowOutcome::Applied
        );
        // A write on another key keeps t2 off the no-write fast path, so
        // its commit really goes through the pipeline and step 5.
        let s2b = ok(t2.next_seq());
        upd(&rig.core, &t2, s2b, b"/t/1/other", b"w");
        assert!(!rig.locks.row_table().holders(b"/t/1/k").is_empty());
        let ticket = ok(rig.core.commit_submit(t2, SyncCommit::On));
        let holder2 = hold_latch(&rig.core, b"/t/1/k");
        // Step 5 blocks in release_row_locks on the helper thread: the
        // ack (step 4) arrives, the release does not.
        let (tx5, rx5) = std::sync::mpsc::channel::<()>();
        {
            let pipeline = Arc::clone(&rig.pipeline);
            std::thread::spawn(move || {
                let mut p = pipeline.lock().expect("pipeline");
                let group = p.drain_available();
                p.process_group(group);
                drop(p);
                let _ = tx5.send(());
            });
        }
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !rig.locks.row_table().holders(b"/t/1/k").is_empty(),
            "step 5 released the shared lock without the key's latch (seed 46)"
        );
        assert!(
            ticket.try_ack().is_some(),
            "the ack (step 4) legally precedes the blocked step 5"
        );
        not_within_200ms(&rx5);
        holder2.release();
        assert!(
            rx5.recv_timeout(Duration::from_millis(200)).is_ok(),
            "step 5 finished after the release"
        );
        assert!(rig.locks.row_table().holders(b"/t/1/k").is_empty());
        ok(Resolver::run_once(&rig.core));
    }
}

// ---- §6 conflict behaviour over the real table -----------------------------

/// The §6 matrix applied by the §5.1 loop over the real table: KEY SHARE
/// passes a pending NO KEY UPDATE intent; SHARE waits on it; FOR UPDATE
/// waits on both the intent and a KEY SHARE holder; NOWAIT is 55P03 and
/// SKIP LOCKED skips. Aborted and visible-committed holders never block,
/// even while their release is still blocked on a held latch.
/// Mutant: ended holders conflict (the FOR UPDATE over the aborted /
/// visible-committed holders returns Wait instead of Applied).
#[test]
fn keyshare_passes_intent_share_waits_forupdate_waits_on_holder() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    // A pending NO KEY UPDATE intent of t1.
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");

    // KEY SHARE vs the pending NO KEY UPDATE intent: granted, no wait.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let mut ks = RowOpTask::new(
        b"/t/1/k",
        None,
        lock_op(RowLockMode::KeyShare),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
    );
    assert_eq!(ok(ks.step(&rig.core, &t2)), Step::Done(RowOutcome::Applied));
    assert_eq!(
        rig.locks.row_table().holders(b"/t/1/k"),
        vec![(t2.id, RowLockMode::KeyShare, s2)]
    );

    // SHARE vs the pending intent: waits on t1 only (t2's KEY SHARE is
    // compatible with SHARE).
    let t3 = rig.txn(Isolation::ReadCommitted);
    let s3 = ok(t3.next_seq());
    let mut sh = RowOpTask::new(
        b"/t/1/k",
        None,
        lock_op(RowLockMode::Share),
        StmtCtx::new(rig.core.visible_ts(), s3, s3),
    );
    let g1 = rig.core.status.entry(t1.id).map(|e| e.gen).unwrap_or(0);
    assert_eq!(
        ok(sh.step(&rig.core, &t3)),
        Step::Wait(vec![(t1.id, g1)]),
        "SHARE conflicts with the intent's NO KEY UPDATE, not with KEY SHARE"
    );

    // FOR UPDATE with the intent present: §5.1's intent block exits first
    // — the wait is on the owner alone (the holders check runs on the
    // retry, below).
    let t4 = rig.txn(Isolation::ReadCommitted);
    let s4 = ok(t4.next_seq());
    let mut fu = RowOpTask::new(
        b"/t/1/k",
        None,
        lock_op(RowLockMode::Update),
        StmtCtx::new(rig.core.visible_ts(), s4, s4),
    );
    assert_eq!(
        ok(fu.step(&rig.core, &t4)),
        Step::Wait(vec![(t1.id, g1)]),
        "a conflicting foreign intent is waited on before the holders check"
    );

    // NOWAIT: 55P03 instead of waiting; SKIP LOCKED: skipped.
    let s4b = ok(t4.next_seq());
    let err = rig
        .core
        .row_op(
            &t4,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::Update),
            StmtCtx::new(rig.core.visible_ts(), s4b, s4b).nowait(),
            &mut Fixed(lock_op(RowLockMode::Update)),
        )
        .expect_err("NOWAIT over a conflicting intent is 55P03");
    assert_eq!(err, TxnError::LockNotAvailable);
    let s4c = ok(t4.next_seq());
    assert_eq!(
        ok(rig.core.row_op(
            &t4,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::Update),
            StmtCtx::new(rig.core.visible_ts(), s4c, s4c).skip_locked(),
            &mut Fixed(lock_op(RowLockMode::Update)),
        )),
        RowOutcome::Skipped(SkipReason::Locked)
    );

    // The intent's owner aborts: the retry discards its intent (Again) and
    // then waits on the KEY SHARE holder alone.
    ok(rig.core.abort(t1));
    assert!(matches!(ok(fu.step(&rig.core, &t4)), Step::Again));
    let g2 = rig.core.status.entry(t2.id).map(|e| e.gen).unwrap_or(0);
    assert_eq!(
        ok(fu.step(&rig.core, &t4)),
        Step::Wait(vec![(t2.id, g2)]),
        "FOR UPDATE waits on the KEY SHARE holder once the intent is gone"
    );
    // NOWAIT over just the holder is 55P03 too.
    let s4d = ok(t4.next_seq());
    let err = rig
        .core
        .row_op(
            &t4,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::Update),
            StmtCtx::new(rig.core.visible_ts(), s4d, s4d).nowait(),
            &mut Fixed(lock_op(RowLockMode::Update)),
        )
        .expect_err("NOWAIT over a conflicting holder is 55P03");
    assert_eq!(err, TxnError::LockNotAvailable);

    // The holder aborts (its release runs synchronously in abort): the
    // retry applies.
    ok(rig.core.abort(t2));
    assert_eq!(ok(fu.step(&rig.core, &t4)), Step::Done(RowOutcome::Applied));
    ok(rig.core.abort(t4));

    // ---- An Aborted holder whose release has not run never blocks. The
    // holder granted KEY SHARE on kb and kc; the test holds kb's latch so
    // abort (set_aborted first, release_row_locks second) blocks while
    // releasing kb, leaving the kc entry in the table under an Aborted
    // status. FOR UPDATE on kc passes.
    {
        let h = rig.txn(Isolation::ReadCommitted);
        let shh = ok(h.next_seq());
        lock_row(&rig.core, &h, shh, b"/t/1/kb", RowLockMode::KeyShare);
        lock_row(&rig.core, &h, shh, b"/t/1/kc", RowLockMode::KeyShare);
        let h_id = h.id;
        let holder = hold_latch(&rig.core, b"/t/1/kb");
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        {
            let core = Arc::clone(&rig.core);
            std::thread::spawn(move || {
                let _ = core.abort(h);
                let _ = tx.send(());
            });
        }
        // set_aborted ran before the (blocked) release; status is Aborted
        // while the kc entry is still in the table.
        let aborted = AtomicBool::new(false);
        for _ in 0..500 {
            if matches!(
                rig.core.status.entry(h_id),
                Some(nucleus_txn::status::StatusEntry {
                    status: TxnStatus::Aborted,
                    ..
                })
            ) {
                aborted.store(true, Ordering::SeqCst);
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(aborted.load(Ordering::SeqCst), "abort set Aborted");
        assert!(
            !rig.locks.row_table().holders(b"/t/1/kc").is_empty(),
            "the kc entry survives while the release is blocked on kb"
        );
        let u = rig.txn(Isolation::ReadCommitted);
        let su = ok(u.next_seq());
        assert_eq!(
            lock_row(&rig.core, &u, su, b"/t/1/kc", RowLockMode::Update),
            RowOutcome::Applied,
            "an Aborted holder never conflicts, even before its release ran"
        );
        ok(rig.core.abort(u));
        not_within_200ms(&rx);
        holder.release();
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_ok(),
            "abort finished after the release"
        );
        assert!(rig.locks.row_table().holders(b"/t/1/kc").is_empty());
    }

    // ---- A visible-committed holder whose release has not run never
    // blocks: same shape, but the holder commits and step 5 blocks on kb.
    {
        let c = rig.txn(Isolation::ReadCommitted);
        let sc = ok(c.next_seq());
        lock_row(&rig.core, &c, sc, b"/t/1/kb2", RowLockMode::KeyShare);
        lock_row(&rig.core, &c, sc, b"/t/1/kc2", RowLockMode::KeyShare);
        // A write keeps the commit off the no-write fast path.
        let scb = ok(c.next_seq());
        upd(&rig.core, &c, scb, b"/t/1/kd", b"w");
        let c_id = c.id;
        let ticket = ok(rig.core.commit_submit(c, SyncCommit::On));
        let holder = hold_latch(&rig.core, b"/t/1/kb2");
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        {
            let pipeline = Arc::clone(&rig.pipeline);
            std::thread::spawn(move || {
                let mut p = pipeline.lock().expect("pipeline");
                let group = p.drain_available();
                p.process_group(group);
                drop(p);
                let _ = tx.send(());
            });
        }
        // Step 4 (status + visibility + ack) ran; step 5's release is
        // blocked on kb2, so kc2's entry sits under a visible commit.
        let visible = AtomicBool::new(false);
        for _ in 0..500 {
            if ticket.try_ack().is_some() {
                visible.store(true, Ordering::SeqCst);
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(visible.load(Ordering::SeqCst), "step 4 acked");
        assert!(matches!(
            rig.core.status.entry(c_id),
            Some(nucleus_txn::status::StatusEntry {
                status: TxnStatus::Committed(_),
                ..
            })
        ));
        assert!(
            !rig.locks.row_table().holders(b"/t/1/kc2").is_empty(),
            "the kc2 entry survives while step 5 is blocked on kb2"
        );
        let u = rig.txn(Isolation::ReadCommitted);
        let su = ok(u.next_seq());
        assert_eq!(
            lock_row(&rig.core, &u, su, b"/t/1/kc2", RowLockMode::Update),
            RowOutcome::Applied,
            "a visible-committed holder never conflicts, even before its release ran"
        );
        ok(rig.core.abort(u));
        not_within_200ms(&rx);
        holder.release();
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_ok(),
            "step 5 finished after the release"
        );
        assert!(rig.locks.row_table().holders(b"/t/1/kc2").is_empty());
        ok(Resolver::run_once(&rig.core));
    }

    ok(rig.core.abort(t3));
    ok(Resolver::run_once(&rig.core));
}

// ---- seed 17: wait_begin re-checks the generation --------------------------

/// Seed 17: T2's step returned `Wait([(T1, g)])`; T1 rolls back to a
/// savepoint (bumping its generation) before T2 calls `wait_begin`;
/// `wait_begin` returns `Done(GenChanged)` and inserts no edge. With the
/// fresh generation the same wait registers.
/// Mutant: the `gen != g` re-check dropped (the wait registers a stale
/// edge instead of returning Done).
#[test]
fn seed17_begin_rechecks_generation() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let t1 = rig.txn(Isolation::ReadCommitted);
    let sp = ok(t1.savepoint());
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");

    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let mut task = RowOpTask::new(
        b"/t/1/k",
        None,
        lock_op(RowLockMode::Update),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
    );
    let g0 = rig.core.status.entry(t1.id).map(|e| e.gen).unwrap_or(0);
    assert_eq!(ok(task.step(&rig.core, &t2)), Step::Wait(vec![(t1.id, g0)]));

    // T1's ROLLBACK TO drops the layer and bumps the generation (seed 26's
    // wake) before T2 begins its wait.
    ok(rig.core.rollback_to(&t1, sp));
    let g1 = rig.core.status.entry(t1.id).map(|e| e.gen).unwrap_or(0);
    assert_eq!(g1, g0 + 1);

    match rig.core.wait_begin(&t2, &[(t1.id, g0)]) {
        nucleus_txn::wait::WaitBegin::Done(WaitOutcome::GenChanged) => {}
        other => panic!("the stale generation must return Done(GenChanged), got {other:?}"),
    }
    assert!(
        !rig.core.wait_edges().contains(&(t2.id, t1.id)),
        "no edge for an already-satisfied wait (I-LIVE c)"
    );

    // The same wait with the fresh generation registers (and end removes
    // the edge).
    let h = match rig.core.wait_begin(&t2, &[(t1.id, g1)]) {
        nucleus_txn::wait::WaitBegin::Registered(h) => h,
        other => panic!("the fresh generation must register, got {other:?}"),
    };
    assert!(rig.core.wait_edges().contains(&(t2.id, t1.id)));
    rig.core.wait_end(h);
    assert!(!rig.core.wait_edges().contains(&(t2.id, t1.id)));
    ok(rig.core.abort(t2));
    ok(rig.core.abort(t1));
}
