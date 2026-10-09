//! C-T2b relation and advisory lock tests (§6): acquire / conflict /
//! NOWAIT / SKIP LOCKED / `lock_timeout` on relations, re-acquisition
//! keeping the older seq, release at commit and abort (a waiter wakes
//! within 100 ms), advisory exclusivity and re-entrancy, the
//! `pg_try_advisory_xact_lock` form, and the card's named
//! `rollback_to_releases_relation_and_advisory_locks`. Threads only for
//! real parks (infinite parker, 100 ms bounds). Each test names the
//! mutant it must kill.

mod locks_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use locks_support::{ok, InfiniteParkers, Rig};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::txn::Isolation;
use nucleus_txn::write::LockWait;
use nucleus_txn::TxnError;

/// Acquire, NOWAIT (55P03), SKIP LOCKED (`Ok(false)`), a blocking wait
/// released by abort within 100 ms, compatible holders, and `lock_timeout`
/// bounding a relation wait.
/// Mutant: `NoWait` errors nowhere / conflicts ignored — the assertions on
/// each arm fail.
#[test]
fn relation_acquire_nowait_skip_locked_block_and_timeout() {
    let rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    let t1 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t1,
        9,
        RelLockMode::Exclusive,
        LockWait::Block,
        None,
    )));

    // RowShare conflicts with a held Exclusive.
    let t2 = rig.txn(Isolation::ReadCommitted);
    assert_eq!(
        rig.locks.lock_relation(
            &rig.core,
            &t2,
            9,
            RelLockMode::RowShare,
            LockWait::NoWait,
            None,
        ),
        Err(TxnError::LockNotAvailable),
        "NOWAIT over a conflict is 55P03"
    );
    assert!(
        !ok(rig.locks.lock_relation(
            &rig.core,
            &t2,
            9,
            RelLockMode::RowShare,
            LockWait::SkipLocked,
            None,
        )),
        "SKIP LOCKED reports not-acquired"
    );

    // A blocked waiter parks (infinite parker: no rescue) and is released
    // by the holder's abort within 100 ms.
    let (tx, rx) = std::sync::mpsc::channel();
    let t2_id = t2.id;
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        let t2 = Arc::new(t2);
        std::thread::spawn(move || {
            let _ = tx.send(locks.lock_relation(
                &core,
                &t2,
                9,
                RelLockMode::RowShare,
                LockWait::Block,
                None,
            ));
        });
    }
    parkers.wait_parked();
    let start = Instant::now();
    ok(rig.core.abort(t1));
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(true), "the waiter acquired after the abort"),
        Err(_) => panic!("the relation waiter was not released within 100 ms"),
    }
    assert!(start.elapsed() < Duration::from_millis(500));

    // t2 (still parked-thread-owned, now holding RowShare) is compatible
    // with another txn's RowExclusive — t3 acquires at once.
    let t3 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t3,
        9,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let _ = t2_id;
    ok(rig.core.abort(t3));
}

/// `lock_timeout` bounds a relation wait (default parkers, so the park's
/// own timeout can fire): Exclusive conflicts with the held RowShare, and
/// the wait returns 55P03 promptly with no edge left.
/// Mutant: the deadline ignored (the wait parks on; the 500 ms bound
/// fails the test).
#[test]
fn relation_lock_timeout_bounds_the_wait() {
    let rig = Rig::new();
    let t1 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t1,
        9,
        RelLockMode::RowShare,
        LockWait::Block,
        None,
    )));
    let t4 = rig.txn(Isolation::ReadCommitted);
    let start = Instant::now();
    assert_eq!(
        rig.locks.lock_relation(
            &rig.core,
            &t4,
            9,
            RelLockMode::Exclusive,
            LockWait::Block,
            Some(Duration::from_millis(50)),
        ),
        Err(TxnError::LockNotAvailable),
        "an expired lock_timeout is 55P03"
    );
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "the timeout fired promptly"
    );
    assert!(rig.core.wait_edges().is_empty());
    ok(rig.core.abort(t4));
    ok(rig.core.abort(t1));
}

/// Re-acquiring a held mode is a no-op that keeps the older seq, so a
/// later `ROLLBACK TO` (which drops acquisitions with `seq >= s`) does not
/// drop it.
/// Mutant: re-acquisition refreshes the seq (the rollback drops the lock
/// and the probe below acquires).
#[test]
fn relation_reacquire_keeps_older_seq() {
    let rig = Rig::new();
    let t = rig.txn(Isolation::ReadCommitted);
    ok(t.savepoint()); // t.seq() is the savepoint's seq when the grant runs
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t,
        1,
        RelLockMode::AccessShare,
        LockWait::Block,
        None,
    )));
    let s1 = ok(t.next_seq()); // s1 > s0
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t,
        1,
        RelLockMode::AccessShare,
        LockWait::Block,
        None,
    ))); // no-op: the seq-0 grant must survive
    ok(rig.core.rollback_to(&t, s1));

    // The lock survived: a conflicting request still cannot take it.
    let u = rig.txn(Isolation::ReadCommitted);
    assert_eq!(
        rig.locks.lock_relation(
            &rig.core,
            &u,
            1,
            RelLockMode::AccessExclusive,
            LockWait::NoWait,
            None,
        ),
        Err(TxnError::LockNotAvailable),
        "the seq-s0 AccessShare survived ROLLBACK TO s1"
    );
    ok(rig.core.abort(u));
    ok(rig.core.abort(t));
}

/// Relation locks are released at commit step 5 and at abort; a blocked
/// waiter wakes within 100 ms of each.
/// Mutant: `release_all` not called on abort (the second waiter never
/// wakes).
#[test]
fn relation_released_at_commit_and_abort_wakes_waiter() {
    let mut rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    // Commit: t1 (main-owned) holds RowExclusive; u blocks on it.
    let t1 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t1,
        3,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let u = rig.txn(Isolation::ReadCommitted);
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        std::thread::spawn(move || {
            let _ = tx.send(locks.lock_relation(
                &core,
                &u,
                3,
                RelLockMode::AccessExclusive,
                LockWait::Block,
                None,
            ));
        });
    }
    parkers.wait_parked();
    rig.commit(t1); // step 5 releases the relation lock and wakes
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(true), "the waiter acquired after the commit"),
        Err(_) => panic!("the relation waiter was not released by commit step 5"),
    }

    // Abort: h (main-owned) holds AccessExclusive; v blocks on it.
    let h = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        4,
        RelLockMode::AccessExclusive,
        LockWait::Block,
        None,
    )));
    let v = rig.txn(Isolation::ReadCommitted);
    let (tx2, rx2) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        std::thread::spawn(move || {
            let _ = tx2.send(locks.lock_relation(
                &core,
                &v,
                4,
                RelLockMode::RowShare,
                LockWait::Block,
                None,
            ));
        });
    }
    parkers.wait_parked();
    ok(rig.core.abort(h));
    match rx2.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(true), "the waiter acquired after the abort"),
        Err(_) => panic!("the relation waiter was not released by abort"),
    }
}

/// Advisory locks (xact scope): exclusive blocks exclusive and shared;
/// shared/shared is compatible; re-entrant per txn; the try form reports
/// `Ok(false)`; released at txn end (commit and abort), releasing a
/// blocked waiter within 100 ms.
/// Mutant: exclusive/shared compatibility inverted — the assertions name
/// each arm.
#[test]
fn advisory_conflicts_reentrancy_and_release_at_txn_end() {
    let mut rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    let t1 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.advisory_xact_lock(
        &rig.core,
        &t1,
        5,
        false,
        LockWait::Block,
        None
    )));

    // Exclusive vs exclusive and vs shared, in try form.
    let t2 = rig.txn(Isolation::ReadCommitted);
    assert!(
        !ok(rig
            .locks
            .advisory_xact_lock(&rig.core, &t2, 5, false, LockWait::NoWait, None)),
        "the try form reports not-acquired (never an error)"
    );
    assert!(
        !ok(rig
            .locks
            .advisory_xact_lock(&rig.core, &t2, 5, true, LockWait::NoWait, None)),
        "an exclusive holder blocks a shared request"
    );

    // Shared/shared compatible, re-entrant per txn.
    let t3 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.advisory_xact_lock(
        &rig.core,
        &t3,
        6,
        true,
        LockWait::Block,
        None
    )));
    assert!(
        ok(rig
            .locks
            .advisory_xact_lock(&rig.core, &t3, 6, true, LockWait::Block, None)),
        "re-entrant"
    );
    let t4 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.advisory_xact_lock(
        &rig.core,
        &t4,
        6,
        true,
        LockWait::NoWait,
        None
    )));
    // A shared holder blocks a new exclusive.
    let t5 = rig.txn(Isolation::ReadCommitted);
    assert!(
        !ok(rig
            .locks
            .advisory_xact_lock(&rig.core, &t5, 6, false, LockWait::NoWait, None)),
        "shared holders block an exclusive request"
    );

    // Released at commit: t1's exclusive on 5 dies with it.
    rig.commit(t1);
    let t6 = rig.txn(Isolation::ReadCommitted);
    assert!(
        ok(rig
            .locks
            .advisory_xact_lock(&rig.core, &t6, 5, false, LockWait::Block, None)),
        "released at commit"
    );

    // Released at abort, waking a blocked waiter: t6 holds 5; t7 parks on
    // it; t6 aborts. (t7 moves into its thread and stays pending.)
    let t7 = rig.txn(Isolation::ReadCommitted);
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        std::thread::spawn(move || {
            let _ = tx.send(locks.advisory_xact_lock(&core, &t7, 5, false, LockWait::Block, None));
        });
    }
    parkers.wait_parked();
    ok(rig.core.abort(t6));
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(true), "the advisory waiter acquired after the abort"),
        Err(_) => panic!("the advisory waiter was not released by abort"),
    }
    ok(rig.core.abort(t5));
    ok(rig.core.abort(t4));
    ok(rig.core.abort(t3));
    ok(rig.core.abort(t2));
}

/// The card's named test: T holds AccessShare at seq 1 (before the
/// savepoint), takes AccessExclusive and an advisory lock after the
/// savepoint; U blocks on both; `rollback_to(s)` wakes U within 100 ms,
/// U gets the locks, and T still holds AccessShare.
/// Mutants: `release_from` no-op (U never wakes); drops seq < s too (the
/// AccessShare probe below acquires); no wake (rollback_to's bump_and_wake
/// removed — U stays parked).
#[test]
fn rollback_to_releases_relation_and_advisory_locks() {
    let rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    let t = rig.txn(Isolation::ReadCommitted);
    // AccessShare taken before the savepoint (at t.seq() == 0).
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t,
        1,
        RelLockMode::AccessShare,
        LockWait::Block,
        None,
    )));
    let s = ok(t.savepoint());
    // After the savepoint: AccessExclusive on another relation and an
    // exclusive advisory lock (both at seq s >= s).
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &t,
        2,
        RelLockMode::AccessExclusive,
        LockWait::Block,
        None,
    )));
    assert!(ok(rig.locks.advisory_xact_lock(
        &rig.core,
        &t,
        42,
        false,
        LockWait::Block,
        None
    )));

    // U1 blocks on the relation, U2 on the advisory key (each waiter
    // moved by value into its thread).
    let u1 = rig.txn(Isolation::ReadCommitted);
    let (tx1, rx1) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        std::thread::spawn(move || {
            let _ = tx1.send(locks.lock_relation(
                &core,
                &u1,
                2,
                RelLockMode::AccessExclusive,
                LockWait::Block,
                None,
            ));
        });
    }
    let u2 = rig.txn(Isolation::ReadCommitted);
    let (tx2, rx2) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        std::thread::spawn(move || {
            let _ =
                tx2.send(locks.advisory_xact_lock(&core, &u2, 42, false, LockWait::Block, None));
        });
    }
    parkers.wait_parked_n(2);

    let start = Instant::now();
    ok(rig.core.rollback_to(&t, s));
    match rx1.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(true), "U1 got the relation lock"),
        Err(_) => panic!("rollback_to did not wake the relation waiter within 100 ms"),
    }
    match rx2.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(true), "U2 got the advisory lock"),
        Err(_) => panic!("rollback_to did not wake the advisory waiter within 100 ms"),
    }
    assert!(start.elapsed() < Duration::from_millis(500));

    // T still holds the AccessShare taken before the savepoint: a
    // conflicting NOWAIT probe on relation 1 still fails, and after T
    // aborts it succeeds.
    let w = rig.txn(Isolation::ReadCommitted);
    assert_eq!(
        rig.locks.lock_relation(
            &rig.core,
            &w,
            1,
            RelLockMode::AccessExclusive,
            LockWait::NoWait,
            None,
        ),
        Err(TxnError::LockNotAvailable),
        "T still holds its pre-savepoint AccessShare"
    );
    ok(rig.core.abort(t));
    assert!(
        ok(rig.locks.lock_relation(
            &rig.core,
            &w,
            1,
            RelLockMode::AccessExclusive,
            LockWait::NoWait,
            None,
        )),
        "the AccessShare died with the txn"
    );
    ok(rig.core.abort(w));
}
