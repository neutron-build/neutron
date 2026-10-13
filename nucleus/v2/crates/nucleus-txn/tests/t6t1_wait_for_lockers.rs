//! C-T6t1 §6 draft 7.8 `wait_for_lockers` tests (PostgreSQL
//! `WaitForLockers`): waits for a RowExclusive holder present at the call
//! and returns on its commit and on its abort; never waits for a holder
//! that acquired after the call (bounded join); no wait on non-conflicting
//! holders (AccessShare vs `Share`); 55P03 with a zero `lock_timeout`;
//! cancel → 57014; and `no_spin_on_gen_bump`: a holder whose generation a
//! `ROLLBACK TO` bumps is re-waited with its **current** generation (a
//! `WaitHook` counts the waits), the waiter returns only after the commit,
//! and the count stays <= 3. Each test names the mutant it must kill
//! (D4–D6 of the card's mutation list).

mod locks_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use locks_support::{ok, upd, CountingWaitHook, InfiniteParkers, Rig};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::wait::WaitHook;
use nucleus_txn::write::LockWait;
use nucleus_txn::TxnError;

/// Spawns `f` on a thread and returns the receiver for a bounded join.
fn spawn_call<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> std::sync::mpsc::Receiver<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx
}

/// Asserts `rx` stays empty for 200 ms (a call that should block must not
/// finish).
fn not_within_200ms<T>(rx: &std::sync::mpsc::Receiver<T>) {
    match rx.recv_timeout(Duration::from_millis(200)) {
        Ok(_) => panic!("wait_for_lockers returned while its holder was still live"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("the wait_for_lockers thread died while its holder was live")
        }
    }
}

/// Waits for a RowExclusive holder present at the call: the waiter parks
/// (infinite parker, no rescue) and returns `Ok(())` within 100 ms of the
/// holder's commit (a write keeps the commit on the pipeline path, so the
/// waiter's re-read sees a visible commit) and within 100 ms of its abort.
/// Mutant: the wait never returns on one of the two ends — the 100 ms
/// bound fails.
#[test]
fn waits_for_row_exclusive_holder_returns_on_commit_and_abort() {
    let mut rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());
    rig.preload(b"/t/1/wc", b"v");

    // Commit: h holds RowExclusive and has a write.
    let h = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        30,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let sh = ok(h.next_seq());
    upd(&rig.core, &h, sh, b"/t/1/wc", b"v2");
    let w = rig.txn(Isolation::ReadCommitted);
    let rx = {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || locks.wait_for_lockers(&core, &w, 30, RelLockMode::Share, None))
    };
    parkers.wait_parked();
    rig.commit(h);
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert!(r.is_ok(), "returned after the commit: {r:?}"),
        Err(_) => panic!("wait_for_lockers did not return after the holder committed"),
    }
    ok(Resolver::run_once(&rig.core));

    // Abort: h2 holds RowExclusive (no write needed — abort runs the full
    // §7.1 and sets Aborted).
    let h2 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h2,
        31,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let w2 = rig.txn(Isolation::ReadCommitted);
    let rx2 = {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || locks.wait_for_lockers(&core, &w2, 31, RelLockMode::Share, None))
    };
    parkers.wait_parked();
    ok(rig.core.abort(h2));
    match rx2.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert!(r.is_ok(), "returned after the abort: {r:?}"),
        Err(_) => panic!("wait_for_lockers did not return after the holder aborted"),
    }
}

/// A holder that acquired **after** the call is never waited for: w parks
/// on h0 (present at the call); `late` then acquires a compatible
/// RowExclusive; h0 aborts and w returns within the bound although `late`
/// still holds a conflicting lock.
/// Mutant D4 (wait_for_lockers re-scans the table and waits for later
/// acquirers): after h0 ends the call re-scans, finds `late`, and waits —
/// the bounded join fails.
#[test]
fn does_not_wait_for_later_acquirers() {
    let rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    let h0 = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h0,
        40,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let w = rig.txn(Isolation::ReadCommitted);
    let rx = {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || locks.wait_for_lockers(&core, &w, 40, RelLockMode::Share, None))
    };
    parkers.wait_parked();
    // A later acquirer: RowExclusive is compatible with h0's RowExclusive,
    // so it takes the relation at once — but it conflicts with Share.
    let late = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &late,
        40,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    ok(rig.core.abort(h0));
    match rx.recv_timeout(Duration::from_millis(200)) {
        Ok(r) => assert!(r.is_ok(), "w returned although `late` still holds: {r:?}"),
        Err(_) => panic!("wait_for_lockers waited for a holder that acquired after the call"),
    }
    ok(rig.core.abort(late));
}

/// No wait on non-conflicting holders: an AccessShare holder never blocks a
/// `Share` wait (the table's own matrix) — the call returns at once and
/// the wait hook records nothing for the pair.
/// Mutant D5 (waits on non-conflicting holders): the call parks on the
/// live holder — the bounded join fails (and the hook records the wait).
#[test]
fn no_wait_on_non_conflicting_holders() {
    let rig = Rig::new();
    let hook = Arc::new(CountingWaitHook::default());
    rig.core
        .waits
        .set_hook(Arc::clone(&hook) as Arc<dyn WaitHook>);

    let h = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        50,
        RelLockMode::AccessShare,
        LockWait::Block,
        None,
    )));
    let w_id;
    let rx = {
        let w = rig.txn(Isolation::ReadCommitted);
        w_id = w.id;
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || locks.wait_for_lockers(&core, &w, 50, RelLockMode::Share, None))
    };
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(r) => assert!(
            r.is_ok(),
            "a non-conflicting holder is not waited for: {r:?}"
        ),
        Err(_) => panic!("wait_for_lockers parked on a non-conflicting AccessShare holder"),
    }
    assert!(
        !hook
            .starts
            .lock()
            .expect("starts")
            .contains_key(&(w_id, h.id)),
        "no wait began at all for the non-conflicting pair"
    );
    ok(rig.core.abort(h));
}

/// `lock_timeout` bounds the wait: a zero timeout over a live conflicting
/// holder is 55P03, promptly, with no edge left in the graph.
/// Mutant: the deadline ignored (the call parks; the bound fails).
#[test]
fn zero_lock_timeout_is_55p03() {
    let rig = Rig::new();
    let h = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        60,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let w = rig.txn(Isolation::ReadCommitted);
    let start = Instant::now();
    assert_eq!(
        rig.locks
            .wait_for_lockers(&rig.core, &w, 60, RelLockMode::Share, Some(Duration::ZERO)),
        Err(TxnError::LockNotAvailable),
        "an expired lock_timeout is 55P03"
    );
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "the timeout fired promptly"
    );
    assert!(rig.core.wait_edges().is_empty());
    ok(rig.core.abort(w));
    ok(rig.core.abort(h));
}

/// Cancel while parked: the flag (which also unparks, seed 48) makes the
/// call return 57014 within the bound.
/// Mutant: cancel not observed — the bounded join fails.
#[test]
fn cancel_is_57014() {
    let rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    let h = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        70,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let w = rig.txn(Isolation::ReadCommitted);
    let cancel = w.cancel_handle();
    let rx = {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || locks.wait_for_lockers(&core, &w, 70, RelLockMode::Share, None))
    };
    parkers.wait_parked();
    cancel.cancel();
    match rx.recv_timeout(Duration::from_millis(200)) {
        Ok(r) => assert_eq!(r, Err(TxnError::QueryCanceled), "cancel is 57014"),
        Err(_) => panic!("wait_for_lockers did not return after the cancel"),
    }
    ok(rig.core.abort(h));
}

/// A generation bump by a holder that has not ended must not turn the wait
/// into a spin: the holder takes a savepoint, writes a row, `ROLLBACK TO`
/// (which released something, so it bumps and wakes) and stays open; the
/// waiter re-reads the **current** generation and parks again; only the
/// holder's commit ends the wait. A [`CountingWaitHook`] counts
/// `on_wait_start(waiter, holder)`: the correct re-read keeps it at <= 3.
/// Mutant D6 (generations not refreshed): the stale generation returns
/// `GenChanged` without parking, over and over — the count explodes past 3
/// inside the 200 ms window below (or the waiter never returns).
#[test]
fn no_spin_on_gen_bump() {
    let mut rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());
    let hook = Arc::new(CountingWaitHook::default());
    rig.core
        .waits
        .set_hook(Arc::clone(&hook) as Arc<dyn WaitHook>);
    rig.preload(b"/t/1/g", b"v");

    // The relation lock is taken BEFORE the savepoint, so the rollback
    // must not release it (only the write's layer is dropped — enough to
    // bump and wake).
    let h = rig.txn(Isolation::ReadCommitted);
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        80,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let h_id = h.id;
    let w = rig.txn(Isolation::ReadCommitted);
    let w_id = w.id;
    let rx = {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || locks.wait_for_lockers(&core, &w, 80, RelLockMode::Share, None))
    };
    parkers.wait_parked();

    let sp = ok(h.savepoint());
    let sw = ok(h.next_seq());
    upd(&rig.core, &h, sw, b"/t/1/g", b"v2");
    ok(rig.core.rollback_to(&h, sp)); // bump + wake; h stays open
    not_within_200ms(&rx); // the waiter is parked again on the live holder

    // A write keeps the commit on the pipeline path (a no-write commit
    // takes the read-only fast path, which leaves the status Pending).
    let sc = ok(h.next_seq());
    upd(&rig.core, &h, sc, b"/t/1/g", b"v3");
    rig.commit(h);
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(r) => assert!(r.is_ok(), "returned after the commit: {r:?}"),
        Err(_) => panic!("wait_for_lockers did not return after the holder committed"),
    }
    let starts = hook.starts.lock().expect("starts");
    let count = starts.get(&(w_id, h_id)).copied().unwrap_or(0);
    assert!(
        count <= 3,
        "wait_for_lockers re-waited {count} times over one generation bump (spin)"
    );
    assert!(
        count >= 1,
        "the wait ran at least once (the holder was live and conflicting)"
    );
    drop(starts);
    ok(Resolver::run_once(&rig.core));
}
