//! C-T2b wait-for-graph tests (§6): the seeds the card names over the
//! real graph — 17 (begin re-checks the generation) lives with the row
//! seeds; here: 26 (rollback wakes a parked waiter), 37 (a missing status
//! is ended), 48 (cancel wakes relation and advisory waits), 59 (the
//! waker removes woken waiters' edges) — plus I-LIVE(b)/(c): no stale
//! edge ever closes a false cycle, `dlk3` elects exactly one victim per
//! cycle over all six check orders, a mixed relation+row two-cycle raises
//! exactly one 40P01, a two-txn row deadlock on real threads, and
//! `lock_timeout`. Threads are used only for real parks (infinite parker,
//! 100 ms bounds). Each test names the mutant it must kill.

mod locks_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use locks_support::{ok, upd, InfiniteParkers, Rig};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::wait::{WaitBegin, WaitOutcome};
use nucleus_txn::write::{LockWait, RowOp, RowOpTask, RowOutcome, Step, StmtCtx};
use nucleus_txn::TxnError;

/// A fixed-update EPQ (no newer versions exist in these setups).
fn fixed_update(value: &[u8]) -> locks_support::Fixed {
    locks_support::Fixed(RowOp::Update {
        value: value.to_vec(),
        key_cols_changed: false,
    })
}

// ---- seed 26 ---------------------------------------------------------------

/// Seed 26: a waiter parked on T1's intent (infinite parker: a lost wake
/// fails the test, never a timeout rescue) is woken by `ROLLBACK TO`
/// within 100 ms, and its retry places.
/// Mutant: `bump_and_wake` wakes before the bump, or not at all.
#[test]
fn seed26_rollback_wakes_parked_waiter() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());
    let t1 = rig.txn(Isolation::ReadCommitted);
    let sp = ok(t1.savepoint());
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1"); // layer >= sp: dropped below

    let t2 = Arc::new(rig.txn(Isolation::ReadCommitted));
    let s2 = ok(t2.next_seq());
    let core = Arc::clone(&rig.core);
    let t2c = Arc::clone(&t2);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(core.row_op(
            &t2c,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(core.visible_ts(), s2, s2),
            &mut fixed_update(b"v2"),
        ));
    });
    parkers.wait_parked();
    let start = Instant::now();
    ok(rig.core.rollback_to(&t1, sp)); // drops T1's layer, bumps + wakes
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(RowOutcome::Applied)),
        Err(_) => panic!("the waiter was not woken within 100 ms (seed 26)"),
    }
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(
        locks_support::intent_owner(&rig.core, b"/t/1/k"),
        Some(t2.id),
        "the retry placed T2's intent"
    );
    // t2 stays pending (its thread owns it); the core drops with the test.
    ok(rig.core.abort(t1));
}

// ---- seed 37 ---------------------------------------------------------------

/// Seed 37: T2 parks on T1's intent; T1 commits and its status is fully
/// truncated; T2's parked wait returns within 100 ms and its retry places
/// over the removed intent. The deterministic arm: `wait_begin` on an
/// already-truncated target returns `Done(Ended)` and registers nothing.
/// Mutant: a missing status treated as Pending (the parked waiter never
/// returns; `wait_begin` registers an edge instead of returning Done).
#[test]
fn seed37_missing_status_is_ended() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v0");
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");
    let t1_id = t1.id;

    let t2 = Arc::new(rig.txn(Isolation::ReadCommitted));
    let s2 = ok(t2.next_seq());
    let core = Arc::clone(&rig.core);
    let t2c = Arc::clone(&t2);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(core.row_op(
            &t2c,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(core.visible_ts(), s2, s2),
            &mut fixed_update(b"v2"),
        ));
    });
    parkers.wait_parked();

    // T1 commits; the resolver resolves and truncates its status entry.
    rig.commit(t1);
    for _ in 0..500 {
        ok(Resolver::run_once(&rig.core));
        if rig.core.status.entry(t1_id).is_none() {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        rig.core.status.entry(t1_id).is_none(),
        "T1's status was truncated while T2 was parked on it"
    );
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(RowOutcome::Applied)),
        Err(_) => panic!("the waiter never returned after the truncation (seed 37)"),
    }
    assert_eq!(
        locks_support::intent_owner(&rig.core, b"/t/1/k"),
        Some(t2.id),
        "the retry placed"
    );

    // The deterministic arm (§4: a missing status for a remembered TxnId
    // means ended and released): a wait begun on the truncated id returns
    // `Ended` at once and inserts no edge — treating it as Pending would
    // park with nobody left to wake it.
    let t3 = rig.txn(Isolation::ReadCommitted);
    match rig.core.wait_begin(&t3, &[(t1_id, 7)]) {
        WaitBegin::Done(WaitOutcome::Ended) => {}
        other => panic!("a truncated target reads as ended, got {other:?}"),
    }
    assert!(rig.core.wait_edges().is_empty());
    // A parked waiter on a target truncated mid-wait returns the same way.
    let ghost = nucleus_txn::TxnId {
        epoch: rig.core.epoch(),
        n: 9_999,
    };
    assert_eq!(
        rig.core.wait_on(&t3, ghost, 0),
        WaitOutcome::Ended,
        "no status entry: ended"
    );
    ok(rig.core.abort(t3));
}

// ---- seed 48 ---------------------------------------------------------------

/// Seed 48: a txn parked on a relation lock, and separately on an advisory
/// lock, returns 57014 within 100 ms of `cancel()` (infinite parker: no
/// unpark means a hang, not a slow test).
/// Mutant: the cancel path does not unpark.
#[test]
fn seed48_cancel_wakes_relation_and_advisory_waits() {
    let rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    let h1 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h1,
        42,
        RelLockMode::Exclusive,
        LockWait::Block,
        None,
    )));
    let h2 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.advisory_xact_lock(
        &rig.core,
        &h2,
        7,
        false,
        LockWait::Block,
        None
    )));

    // A relation waiter and an advisory waiter, both parked.
    let w1 = Arc::new(rig.txn(Isolation::ReadCommitted));
    let c1 = w1.cancel_handle();
    let w2 = Arc::new(rig.txn(Isolation::ReadCommitted));
    let c2 = w2.cancel_handle();
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        let w1 = Arc::clone(&w1);
        std::thread::spawn(move || {
            let _ = tx.send(locks.lock_relation(
                &core,
                &w1,
                42,
                RelLockMode::RowExclusive,
                LockWait::Block,
                None,
            ));
        });
    }
    let (tx2, rx2) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        let w2 = Arc::clone(&w2);
        std::thread::spawn(move || {
            let _ = tx2.send(locks.advisory_xact_lock(&core, &w2, 7, true, LockWait::Block, None));
        });
    }
    parkers.wait_parked_n(2);
    let start = Instant::now();
    c1.cancel();
    c2.cancel();
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(Err(TxnError::QueryCanceled)) => {}
        other => panic!("the relation waiter must return 57014, got {other:?}"),
    }
    match rx2.recv_timeout(Duration::from_millis(100)) {
        Ok(Err(TxnError::QueryCanceled)) => {}
        other => panic!("the advisory waiter must return 57014, got {other:?}"),
    }
    assert!(start.elapsed() < Duration::from_millis(500));
    assert!(rig.core.wait_edges().is_empty(), "both waits unregistered");
}

// ---- seed 59 ---------------------------------------------------------------

/// Seed 59: X registered waiting on T; T commits (`process_group`), aborts
/// or rolls back — each waker removes the woken waiter's edges **before X
/// polls**, so `wait_edges()` no longer contains `(X, T)`.
/// Mutant: wake without edge removal (a stale edge closes a false cycle).
#[test]
fn seed59_waker_removes_edges() {
    // Commit.
    {
        let mut rig = Rig::new();
        rig.preload(b"/t/1/k", b"v");
        let t1 = rig.txn(Isolation::ReadCommitted);
        let s1 = ok(t1.next_seq());
        upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");
        let g = rig.core.status.entry(t1.id).map(|e| e.gen).unwrap_or(0);
        let x = rig.txn(Isolation::ReadCommitted);
        let h = match rig.core.wait_begin(&x, &[(t1.id, g)]) {
            WaitBegin::Registered(h) => h,
            other => panic!("must register, got {other:?}"),
        };
        assert!(rig.core.wait_edges().contains(&(x.id, t1.id)));
        let t1_id = t1.id;
        let ticket = ok(rig
            .core
            .commit_submit(t1, nucleus_txn::commit::SyncCommit::On));
        {
            let mut p = rig.pipeline.lock().expect("pipeline");
            let group = p.drain_available();
            p.process_group(group);
        }
        ok(ticket.wait());
        assert!(
            !rig.core.wait_edges().contains(&(x.id, t1_id)),
            "commit step 5 removed the woken waiter's edge before X polls (seed 59)"
        );
        assert_eq!(
            match rig.core.wait_poll(&h) {
                Some(WaitOutcome::Committed(_)) => "committed",
                Some(WaitOutcome::GenChanged) => "gen",
                other => panic!("X still observes why it was woken, got {other:?}"),
            },
            "committed",
            "X still observes why it was woken"
        );
        rig.core.wait_end(h);
        ok(rig.core.abort(x));
    }
    // Abort.
    {
        let mut rig = Rig::new();
        rig.preload(b"/t/1/k", b"v");
        let t1 = rig.txn(Isolation::ReadCommitted);
        let s1 = ok(t1.next_seq());
        upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");
        let g = rig.core.status.entry(t1.id).map(|e| e.gen).unwrap_or(0);
        let x = rig.txn(Isolation::ReadCommitted);
        let h = match rig.core.wait_begin(&x, &[(t1.id, g)]) {
            WaitBegin::Registered(h) => h,
            other => panic!("must register, got {other:?}"),
        };
        let t1_id = t1.id;
        assert!(rig.core.wait_edges().contains(&(x.id, t1_id)));
        ok(rig.core.abort(t1));
        assert!(
            !rig.core.wait_edges().contains(&(x.id, t1_id)),
            "abort removed the woken waiter's edge (seed 59)"
        );
        rig.core.wait_end(h);
        ok(rig.core.abort(x));
    }
    // ROLLBACK TO.
    {
        let mut rig = Rig::new();
        rig.preload(b"/t/1/k", b"v");
        let t1 = rig.txn(Isolation::ReadCommitted);
        let sp = ok(t1.savepoint());
        let s1 = ok(t1.next_seq());
        upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");
        let g = rig.core.status.entry(t1.id).map(|e| e.gen).unwrap_or(0);
        let x = rig.txn(Isolation::ReadCommitted);
        let h = match rig.core.wait_begin(&x, &[(t1.id, g)]) {
            WaitBegin::Registered(h) => h,
            other => panic!("must register, got {other:?}"),
        };
        assert!(rig.core.wait_edges().contains(&(x.id, t1.id)));
        ok(rig.core.rollback_to(&t1, sp));
        assert!(
            !rig.core.wait_edges().contains(&(x.id, t1.id)),
            "ROLLBACK TO removed the woken waiter's edge (seed 59)"
        );
        rig.core.wait_end(h);
        ok(rig.core.abort(x));
        ok(rig.core.abort(t1));
    }
}

// ---- I-LIVE(c): a stale edge never closes a false cycle --------------------

/// X waits on T's row; T rolls back to a savepoint, releasing it (the
/// waker removes X's edge); X stays registered (it has not polled). T then
/// waits on a row X holds; T's deadlock check returns `false` — the stale
/// `X -> T` edge must not close a false cycle.
/// Mutant: the seed-59 mutant (waker leaves the edge) — T's check finds
/// the two-cycle and returns `true`.
#[test]
fn stale_edge_never_deadlocks() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k1", b"v");
    rig.preload(b"/t/1/k2", b"v");
    let x = rig.txn(Isolation::ReadCommitted);
    let sx = ok(x.next_seq());
    upd(&rig.core, &x, sx, b"/t/1/k2", b"vx"); // X holds k2

    let t = rig.txn(Isolation::ReadCommitted);
    let sp = ok(t.savepoint());
    let st = ok(t.next_seq());
    upd(&rig.core, &t, st, b"/t/1/k1", b"vt"); // T holds k1 (after sp)

    // X waits on T's k1 intent.
    let gt = rig.core.status.entry(t.id).map(|e| e.gen).unwrap_or(0);
    let mut xt = RowOpTask::new(
        b"/t/1/k1",
        None,
        RowOp::Update {
            value: b"vx1".to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(rig.core.visible_ts(), sx + 1, sx + 1),
    );
    assert_eq!(ok(xt.step(&rig.core, &x)), Step::Wait(vec![(t.id, gt)]));
    let xh = match rig.core.wait_begin(&x, &[(t.id, gt)]) {
        WaitBegin::Registered(h) => h,
        other => panic!("must register, got {other:?}"),
    };
    assert!(rig.core.wait_edges().contains(&(x.id, t.id)));

    // T rolls back to before its k1 write: the release's bump-and-wake
    // removes X's edge while X is still registered (it never polls here).
    ok(rig.core.rollback_to(&t, sp));
    assert!(
        !rig.core.wait_edges().contains(&(x.id, t.id)),
        "the waker removed X's edge (seed 59)"
    );

    // T waits on X's k2 intent; its deadlock check must be false.
    let mut tt = RowOpTask::new(
        b"/t/1/k2",
        None,
        RowOp::Update {
            value: b"vt2".to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(rig.core.visible_ts(), st + 1, st + 1),
    );
    let targets = match ok(tt.step(&rig.core, &t)) {
        Step::Wait(targets) => targets,
        other => panic!("T must wait on X's intent, got {other:?}"),
    };
    let h = match rig.core.wait_begin(&t, &targets) {
        WaitBegin::Registered(h) => h,
        other => panic!("T must register its wait, got {other:?}"),
    };
    assert!(rig.core.wait_edges().contains(&(t.id, x.id)));
    assert!(
        !rig.core.wait_deadlock_check(&h),
        "the stale X -> T edge must not close a false cycle (I-LIVE c)"
    );
    rig.core.wait_end(h);
    rig.core.wait_end(xh);
    ok(rig.core.abort(t));
    ok(rig.core.abort(x));
}

// ---- I-LIVE(b): dlk3, exactly one victim -----------------------------------

/// G0's `dlk3` workload: W0 holds t0 and waits t1, W1 holds t1 and waits
/// c0, W2 holds c0 and waits t0. For each of the six orders of running
/// the three deadlock checks, exactly one returns `true`; after the victim
/// aborts, the other two complete (one commits, which lets the last
/// proceed).
/// Mutant: the victim removes its edges after releasing the graph mutex,
/// or not at all — a second checker on the same cycle also returns `true`
/// (two victims).
#[test]
fn dlk3_exactly_one_victim() {
    let orders: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for order in orders {
        let mut rig = Rig::new();
        let keys = [
            b"/t/1/t0".as_slice(),
            b"/t/1/t1".as_slice(),
            b"/t/1/c0".as_slice(),
        ];
        for k in keys {
            rig.preload(k, b"v");
        }
        // W(i) holds keys[i] and waits on keys[(i+1)%3] held by W(i+1).
        let mut txns: Vec<Option<_>> = (0..3)
            .map(|_| Some(rig.txn(Isolation::ReadCommitted)))
            .collect();
        for i in 0..3 {
            let t = txns[i].as_ref().expect("txn");
            let s = ok(t.next_seq());
            upd(&rig.core, t, s, keys[i], b"w");
        }
        let mut tasks = Vec::new();
        for (i, slot) in txns.iter().enumerate() {
            let next = (i + 1) % 3;
            let t = slot.as_ref().expect("txn");
            let s = ok(t.next_seq());
            tasks.push(RowOpTask::new(
                keys[next],
                None,
                RowOp::Update {
                    value: b"w".to_vec(),
                    key_cols_changed: false,
                },
                StmtCtx::new(rig.core.visible_ts(), s, s),
            ));
        }
        let mut handles = Vec::new();
        for i in 0..3 {
            let next = (i + 1) % 3;
            match ok(tasks[i].step(&rig.core, txns[i].as_ref().expect("txn"))) {
                Step::Wait(targets) => {
                    assert_eq!(targets.len(), 1, "one blocker");
                    assert_eq!(targets[0].0, txns[next].as_ref().expect("txn").id);
                    handles.push(
                        match rig
                            .core
                            .wait_begin(txns[i].as_ref().expect("txn"), &targets)
                        {
                            WaitBegin::Registered(h) => h,
                            other => panic!("W{i} must register, got {other:?}"),
                        },
                    );
                }
                other => panic!("W{i} must wait, got {other:?}"),
            }
        }
        // The three edges close the cycle W0 -> W1 -> W2 -> W0.
        let ids: Vec<nucleus_txn::TxnId> =
            (0..3).map(|i| txns[i].as_ref().expect("txn").id).collect();
        for (i, from) in ids.iter().enumerate() {
            assert!(rig.core.wait_edges().contains(&(*from, ids[(i + 1) % 3])));
        }

        // Run the checks in this order: exactly the first checker of the
        // cycle is the victim (its check removed its edges under the
        // graph mutex, so no later checker sees the cycle again).
        let mut victims = Vec::new();
        for &i in &order {
            if rig.core.wait_deadlock_check(&handles[i]) {
                victims.push(i);
            }
        }
        assert_eq!(
            victims,
            vec![order[0]],
            "exactly one victim for order {order:?}"
        );
        let victim = victims[0];
        ok(rig.core.abort(txns[victim].take().expect("victim txn")));

        // The survivor whose blocker was the victim completes first; the
        // other waits for it to commit.
        let mut first = None;
        let mut last = None;
        for i in 0..3 {
            if i == victim {
                continue;
            }
            if (i + 1) % 3 == victim {
                first = Some(i);
            } else {
                last = Some(i);
            }
        }
        let (first, last) = (first.expect("first"), last.expect("last"));

        // `first` retries: the victim's aborted intent is discarded
        // (Again), then its op applies; it commits.
        for i in [first, last] {
            if i == last {
                // `last` waits on `first`'s intent; `first` committed, so
                // the registered wait resolves (its edge was removed by
                // commit step 5's wake).
                match rig.core.wait_poll(&handles[last]) {
                    Some(WaitOutcome::Committed(_)) => {}
                    other => panic!(
                        "the first survivor's commit resolved the last one's wait, got {other:?}"
                    ),
                }
            }
            let mut steps = 0;
            let outcome = loop {
                steps += 1;
                assert!(steps < 10, "too many steps");
                match ok(tasks[i].step(&rig.core, txns[i].as_ref().expect("txn"))) {
                    Step::Done(o) => break o,
                    Step::Again => {}
                    Step::Epq(_) => {
                        // RC: `first`'s commit landed a newer version above
                        // `last`'s snapshot; EPQ passes and applies.
                        ok(tasks[i].epq_result(nucleus_txn::write::EpqDecision::Apply(
                            RowOp::Update {
                                value: b"w".to_vec(),
                                key_cols_changed: false,
                            },
                        )));
                    }
                    Step::Wait(w) => {
                        let out = rig.core.wait_on_any(txns[i].as_ref().expect("txn"), &w);
                        assert!(
                            !matches!(out, WaitOutcome::Cancelled | WaitOutcome::Deadlock),
                            "no second 40P01"
                        );
                    }
                    other => panic!("unexpected step {other:?}"),
                }
            };
            assert_eq!(outcome, RowOutcome::Applied);
            let ticket = ok(rig.core.commit_submit(
                txns[i].take().expect("survivor txn"),
                nucleus_txn::commit::SyncCommit::On,
            ));
            {
                let mut p = rig.pipeline.lock().expect("pipeline");
                let group = p.drain_available();
                p.process_group(group);
            }
            ok(ticket.wait());
        }
        for h in handles {
            rig.core.wait_end(h);
        }
        assert!(rig.core.wait_edges().is_empty(), "order {order:?}");
        ok(Resolver::run_once(&rig.core));
        ok(Resolver::run_once(&rig.core));
    }
}

// ---- I-LIVE(b): a mixed relation+row two-cycle ------------------------------

/// T1 holds relation R `Exclusive` and waits on T2's row intent; T2
/// requests R `RowExclusive`. The one graph covers relation and row waits
/// together: exactly one 40P01 (T1's check finds the two-cycle; T2, whose
/// own check would fire later under the raised `deadlock_timeout`, gets
/// the lock after the victim aborts).
/// Mutant: relation waits outside the graph (T1's check finds no cycle and
/// the test hangs at the 10 s parked relation wait).
#[test]
fn mixed_relation_row_two_cycle_exactly_one_40p01() {
    let mut rig = Rig::new();
    rig.core
        .waits
        .set_deadlock_timeout(Duration::from_secs(3600)); // the test elects the victim itself
    rig.preload(b"/t/1/k", b"v");

    let t1 = rig.txn(Isolation::ReadCommitted);
    let t2 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t1,
        5,
        RelLockMode::Exclusive,
        LockWait::Block,
        None,
    )));
    let s2 = ok(t2.next_seq());
    upd(&rig.core, &t2, s2, b"/t/1/k", b"v2");

    // T1 waits on T2's row intent (step-driven).
    let s1 = ok(t1.next_seq());
    let mut task = RowOpTask::new(
        b"/t/1/k",
        None,
        RowOp::Update {
            value: b"v1".to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
    );
    let g2 = rig.core.status.entry(t2.id).map(|e| e.gen).unwrap_or(0);
    let targets = match ok(task.step(&rig.core, &t1)) {
        Step::Wait(targets) => targets,
        other => panic!("T1 must wait on T2's intent, got {other:?}"),
    };
    assert_eq!(targets, vec![(t2.id, g2)]);
    let h1 = match rig.core.wait_begin(&t1, &targets) {
        WaitBegin::Registered(h) => h,
        other => panic!("T1 must register, got {other:?}"),
    };

    // T2 blocks on T1's relation lock: the edge T2 -> T1 enters the same
    // graph (the blocking call parks on another thread).
    let t2_id = t2.id;
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        let t2 = Arc::new(t2);
        std::thread::spawn(move || {
            let _ = tx.send(locks.lock_relation(
                &core,
                &t2,
                5,
                RelLockMode::RowExclusive,
                LockWait::Block,
                None,
            ));
        });
    }
    // Wait until the relation wait registered its edge (bounded: the edge
    // appears as soon as its wait_begin ran).
    let deadline = Instant::now() + Duration::from_millis(500);
    while !rig.core.wait_edges().contains(&(t2_id, t1.id)) {
        assert!(
            Instant::now() < deadline,
            "the relation wait never registered its edge"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(rig.core.wait_edges().contains(&(t1.id, t2_id)));

    // T1's deadlock check finds the two-cycle: exactly one 40P01.
    assert!(
        rig.core.wait_deadlock_check(&h1),
        "the mixed relation+row cycle must be detected (one graph)"
    );
    // The victim (T1) aborts; T2 acquires the relation lock.
    ok(rig.core.abort(t1));
    match rx.recv_timeout(Duration::from_millis(200)) {
        Ok(r) => assert_eq!(
            r,
            Ok(true),
            "T2 got the relation lock after the victim aborted"
        ),
        Err(_) => panic!("T2's relation wait was not released within 200 ms"),
    }
    assert!(rig.core.wait_edges().is_empty());
}

// ---- I-LIVE(b): a two-txn row deadlock on real threads ----------------------

/// Two txns in opposite order on two keys with `deadlock_timeout` = 50 ms
/// on real threads (a spawned commit thread processes their commits):
/// exactly one driver returns 40P01 within 1 s, its session aborts, and
/// the other's op completes and commits after the victim aborted.
/// Mutant: two victims (both drivers return 40P01), or none (a hang past
/// the 1 s bound).
#[test]
fn threads_two_txn_row_deadlock_one_40p01() {
    let core = Arc::new(ok(nucleus_txn::boot::Core::open(nucleus_kv::MemKv::new())));
    let _locks = nucleus_txn::locks::LockManager::install(&core);
    let handle = ok(nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core)));

    // Preload two rows through the pipeline.
    for k in [b"/t/1/a".as_slice(), b"/t/1/b".as_slice()] {
        let t = core.begin(Isolation::ReadCommitted);
        let s = ok(t.next_seq());
        ok(core.insert_key(
            &t,
            k,
            None,
            b"v".to_vec(),
            StmtCtx::new(core.visible_ts(), s, s),
            nucleus_txn::write::UniqueRule::Unique { same_row: None },
        ));
        ok(core.commit(t, nucleus_txn::commit::SyncCommit::On));
    }

    core.waits.set_deadlock_timeout(Duration::from_millis(50));
    let t1 = core.begin(Isolation::ReadCommitted);
    let t2 = core.begin(Isolation::ReadCommitted);
    let s11 = ok(t1.next_seq());
    upd(&core, &t1, s11, b"/t/1/a", b"1a");
    let s21 = ok(t2.next_seq());
    upd(&core, &t2, s21, b"/t/1/b", b"2b");

    // Each thread owns its whole session: the op; on 40P01 the session
    // aborts (the victim), otherwise it commits (the survivor).
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&core);
        let tx = tx.clone();
        std::thread::spawn(move || {
            let s = ok(t1.next_seq());
            let r = core.row_op(
                &t1,
                b"/t/1/b",
                None,
                RowOp::Update {
                    value: b"1b".to_vec(),
                    key_cols_changed: false,
                },
                StmtCtx::new(core.visible_ts(), s, s),
                &mut fixed_update(b"1b"),
            );
            match r {
                Err(TxnError::Deadlock) => {
                    ok(core.abort(t1));
                    let _ = tx.send(Err(TxnError::Deadlock));
                }
                other => {
                    let _ = tx.send(other);
                    let _ = tx.send(
                        core.commit(t1, nucleus_txn::commit::SyncCommit::On)
                            .map(|_| RowOutcome::Applied),
                    );
                }
            }
        });
    }
    {
        let core = Arc::clone(&core);
        std::thread::spawn(move || {
            let s = ok(t2.next_seq());
            let r = core.row_op(
                &t2,
                b"/t/1/a",
                None,
                RowOp::Update {
                    value: b"2a".to_vec(),
                    key_cols_changed: false,
                },
                StmtCtx::new(core.visible_ts(), s, s),
                &mut fixed_update(b"2a"),
            );
            match r {
                Err(TxnError::Deadlock) => {
                    ok(core.abort(t2));
                    let _ = tx.send(Err(TxnError::Deadlock));
                }
                other => {
                    let _ = tx.send(other);
                    let _ = tx.send(
                        core.commit(t2, nucleus_txn::commit::SyncCommit::On)
                            .map(|_| RowOutcome::Applied),
                    );
                }
            }
        });
    }

    // The first result is the victim's 40P01 (the survivor cannot finish
    // before the victim aborts); the second is its completion, then its
    // commit ack.
    let start = Instant::now();
    let first = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the victim reported within 1 s");
    assert_eq!(
        first,
        Err(TxnError::Deadlock),
        "exactly one 40P01, and it fires first"
    );
    let second = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the survivor completed within 1 s");
    assert_eq!(
        second,
        Ok(RowOutcome::Applied),
        "the survivor completed after the victim aborted"
    );
    let ack = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the survivor committed");
    assert_eq!(ack, Ok(RowOutcome::Applied));
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "the deadlock resolved promptly"
    );
    assert!(core.wait_edges().is_empty());
    ok(handle.shutdown());
}

// ---- lock_timeout -----------------------------------------------------------

/// A row wait with `.lock_timeout(50 ms)` returns 55P03 within 500 ms and
/// leaves no edge.
/// Mutant: the deadline ignored (the wait parks on; the 500 ms bound
/// fails the test).
#[test]
fn lock_timeout_returns_55p03_and_leaves_no_edge() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");

    let t2 = Arc::new(rig.txn(Isolation::ReadCommitted));
    let core = Arc::clone(&rig.core);
    let t2c = Arc::clone(&t2);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let s = ok(t2c.next_seq());
        let _ = tx.send(core.row_op(
            &t2c,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(core.visible_ts(), s, s).lock_timeout(Duration::from_millis(50)),
            &mut fixed_update(b"v2"),
        ));
    });
    let start = Instant::now();
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(Err(TxnError::LockNotAvailable)) => {}
        other => panic!("the lock_timeout must surface as 55P03, got {other:?}"),
    }
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "the timeout fired promptly"
    );
    assert!(
        rig.core.wait_edges().is_empty(),
        "the timed-out wait removed its edges"
    );
    ok(rig.core.abort(t1));
}
