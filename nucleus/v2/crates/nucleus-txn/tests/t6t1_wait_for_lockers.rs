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
//!
//! C-T6t1fix (both gates' follow-ups): the caller's own acquisitions are
//! excluded from the scan (Opus M-1 / GLM O7); a holder whose lock-mode
//! *term* changed mid-txn (SELECT then UPDATE: AccessShare then
//! RowExclusive) is still waited for (Opus M-3 / mutant X1); a detected
//! deadlock surfaces as 40P01 instead of being swallowed into a re-wait
//! loop (Opus L-5 / mutant X3); and `lock_timeout` is one deadline for the
//! whole call, honoured across a mid-call generation bump (Opus N-2 /
//! mutant X4).

mod locks_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use locks_support::{ok, upd, CountingWaitHook, InfiniteParkers, Rig};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::wait::{WaitBegin, WaitHook};
use nucleus_txn::write::LockWait;
use nucleus_txn::{TxnError, TxnId};

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

/// Polls `wait_edges()` until `(waiter, target)` is present — the wait
/// registered — panicking after 2 s (a call that never waits fails the
/// test instead of hanging it).
fn wait_edge_present(core: &Core<MemKv>, waiter: TxnId, target: TxnId) {
    let deadline = Instant::now() + Duration::from_millis(2000);
    while Instant::now() < deadline {
        if core.wait_edges().contains(&(waiter, target)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("no wait-for edge {waiter:?} -> {target:?} within 2 s");
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

/// C-T6t1fix item 1 (Opus M-1 / GLM O7): the caller's own acquisitions are
/// excluded from the scan — §6 "holders of `rel` **other than the
/// caller**". The online build's shape: the caller holds
/// `ShareUpdateExclusive` on the rel and has written a row (so it is live
/// in the status table; a writeless caller would make the pin vacuous),
/// then calls `wait_for_lockers(rel, Share)` — SUE conflicts with a Share
/// request, so without the self-exclusion the scan would collect the
/// caller itself. The call returns `Ok` at once, leaves no edge, and
/// records no `(w, w)` wait.
/// Mutant O7 (the `a.txn == txn.id` term dropped from the scan): the
/// self-edge makes the call return 40P01 after `deadlock_timeout` — the
/// `Ok` assert fails.
#[test]
fn caller_self_exclusion() {
    let mut rig = Rig::new();
    let hook = Arc::new(CountingWaitHook::default());
    rig.core
        .waits
        .set_hook(Arc::clone(&hook) as Arc<dyn WaitHook>);
    rig.preload(b"/t/1/self", b"v");

    let w = rig.txn(Isolation::ReadCommitted);
    let w_id = w.id;
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &w,
        90,
        RelLockMode::ShareUpdateExclusive,
        LockWait::Block,
        None,
    )));
    let sw = ok(w.next_seq());
    upd(&rig.core, &w, sw, b"/t/1/self", b"v2");

    let start = Instant::now();
    let r = rig
        .locks
        .wait_for_lockers(&rig.core, &w, 90, RelLockMode::Share, None);
    assert!(r.is_ok(), "the caller's own locks never block it: {r:?}");
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "self-exclusion is decided in the scan, not by waiting out a self-edge"
    );
    assert!(
        rig.core.wait_edges().is_empty(),
        "no self-edge was left: {:?}",
        rig.core.wait_edges()
    );
    assert!(
        !hook
            .starts
            .lock()
            .expect("starts")
            .contains_key(&(w_id, w_id)),
        "no (w, w) wait began"
    );
    ok(rig.core.abort(w));
}

/// C-T6t1fix item 4 (Opus M-3 / mutant X1): a holder whose lock-mode term
/// changed mid-txn — it ran a SELECT (AccessShare) and then an UPDATE
/// (RowExclusive) — is still waited for. Its acquisitions appear as
/// `[AccessShare, RowExclusive]` in the relation table, so only a
/// conflicting term may mark the txn seen. `wait_for_lockers(Share)`
/// parks (a real wait edge is recorded) until the holder aborts; in the
/// online build a regression here means a live writer's rows never reach
/// the new index.
/// Mutant M3SWAP (the dedup term before the conflict test): the holder is
/// marked seen on its AccessShare entry and never collected — the call
/// returns while the holder is live and the bounded check fails.
#[test]
fn mode_changed_holder_is_still_waited_for() {
    let mut rig = Rig::new();
    let parkers = InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());
    let hook = Arc::new(CountingWaitHook::default());
    rig.core
        .waits
        .set_hook(Arc::clone(&hook) as Arc<dyn WaitHook>);
    rig.preload(b"/t/1/swap", b"v");

    let h = rig.txn(Isolation::ReadCommitted);
    let h_id = h.id;
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        91,
        RelLockMode::AccessShare, // the SELECT
        LockWait::Block,
        None,
    )));
    let sh = ok(h.next_seq());
    upd(&rig.core, &h, sh, b"/t/1/swap", b"v2");
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        91,
        RelLockMode::RowExclusive, // then the UPDATE
        LockWait::Block,
        None,
    )));

    let w = rig.txn(Isolation::ReadCommitted);
    let w_id = w.id;
    let rx = {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || locks.wait_for_lockers(&core, &w, 91, RelLockMode::Share, None))
    };
    not_within_200ms(&rx);
    wait_edge_present(&rig.core, w_id, h_id);
    assert!(
        *hook
            .starts
            .lock()
            .expect("starts")
            .get(&(w_id, h_id))
            .unwrap_or(&0)
            >= 1,
        "the wait on the mode-changed holder began (the wait-edge was recorded)"
    );
    ok(rig.core.abort(h));
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(r) => assert!(r.is_ok(), "returned after the abort: {r:?}"),
        Err(_) => panic!("wait_for_lockers did not return after the holder aborted"),
    }
}

/// C-T6t1fix item 6 (Opus L-5 / mutant X3): when the wait-for cycle is
/// detected during a `wait_for_lockers`, the 40P01 surfaces — it is never
/// swallowed into a re-wait loop. The build's cycle: w holds SUE on the
/// rel (and has a write), H holds RowExclusive and is parked waiting on w
/// (it requested SUE, which conflicts with w's); w parks on H.
///
/// H's half of the cycle is registered through the public wait-step API
/// ([`Core::wait_begin`]) instead of a second blocking thread: a
/// two-thread cycle has an inherent victim race — either side's deadlock
/// check can fire first, and if H wins, w's cycle vanishes with H's
/// edges and w would legitimately keep waiting (the test would flake). A
/// hand-held registration never parks, never runs a deadlock check of
/// its own and never ends until the test ends it, so w's check — the one
/// this test pins — always sees the full cycle and w is always the
/// victim.
/// Mutant L5SWALLOW (`WaitOutcome::Deadlock` re-looped): the victim
/// re-waits forever on a holder that is still waiting on it — the bounded
/// join fails.
#[test]
fn detected_deadlock_surfaces_as_40p01() {
    let mut rig = Rig::new();
    rig.core
        .waits
        .set_deadlock_timeout(Duration::from_millis(400));
    rig.preload(b"/t/1/dl", b"v");

    let w = rig.txn(Isolation::ReadCommitted);
    let w_id = w.id;
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &w,
        92,
        RelLockMode::ShareUpdateExclusive,
        LockWait::Block,
        None,
    )));
    let sw = ok(w.next_seq());
    upd(&rig.core, &w, sw, b"/t/1/dl", b"v2");
    let h = rig.txn(Isolation::ReadCommitted);
    let h_id = h.id;
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        92,
        RelLockMode::RowExclusive, // compatible with w's SUE
        LockWait::Block,
        None,
    )));

    // H is parked waiting on w (it requested SUE, which conflicts with
    // w's): the edge exists, nothing ends it, and no deadlock check ever
    // runs on H's side — w is always the victim.
    let gen_w = rig.core.status.entry(w_id).map_or(0, |e| e.gen);
    let h_wait = match rig.core.wait_begin(&h, &[(w_id, gen_w)]) {
        WaitBegin::Registered(hw) => hw,
        WaitBegin::Done(o) => panic!("H's wait on w unblocked at once: {o:?}"),
    };
    assert!(
        rig.core.wait_edges().contains(&(h_id, w_id)),
        "H's half of the cycle is registered"
    );

    // w parks on H (RowExclusive conflicts with Share); after
    // deadlock_timeout its check finds w -> H -> w and the 40P01 must
    // surface here.
    let rx_w = {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || locks.wait_for_lockers(&core, &w, 92, RelLockMode::Share, None))
    };
    wait_edge_present(&rig.core, w_id, h_id);
    match rx_w.recv_timeout(Duration::from_millis(2000)) {
        Ok(Err(TxnError::Deadlock)) => {}
        Ok(other) => panic!("the detected deadlock must surface as 40P01, got {other:?}"),
        Err(_) => panic!("wait_for_lockers swallowed the detected deadlock (re-wait loop)"),
    }
    // The victim's own edges are already gone (the check removes them
    // under the graph mutex); H's registration is the test's to end.
    rig.core.wait_end(h_wait);
    assert!(
        rig.core.wait_edges().is_empty(),
        "the ended cycle left no edges: {:?}",
        rig.core.wait_edges()
    );
    ok(rig.core.abort(h));
    // (w stays pending in its thread, like the other thread-owned txns.)
}

/// C-T6t1fix item 3d, `wait_for_lockers` half (Opus N-2 / mutant X4):
/// `lock_timeout` is one deadline for the **whole call**, not one per
/// re-wait. The holder stays open and bumps its wake generation mid-call
/// (a `ROLLBACK TO` that releases its post-savepoint write while keeping
/// the relation lock it took before the savepoint); the re-wait must
/// still expire at the deadline taken at call entry — a per-iteration
/// deadline would let a bump-heavy holder extend `lock_timeout`
/// indefinitely.
#[test]
fn lock_timeout_is_one_deadline_for_the_whole_call() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/n2", b"v");
    // The relation lock predates the savepoint: the rollback releases
    // only the write's layer (enough to bump and wake), never the lock.
    let h = rig.txn(Isolation::ReadCommitted);
    let h_id = h.id;
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &h,
        93,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    )));
    let w = rig.txn(Isolation::ReadCommitted);
    let w_id = w.id;
    let start = Instant::now();
    let rx = {
        let core = Arc::clone(&rig.core);
        let locks = Arc::clone(&rig.locks);
        spawn_call(move || {
            locks.wait_for_lockers(
                &core,
                &w,
                93,
                RelLockMode::Share,
                Some(Duration::from_millis(1000)),
            )
        })
    };
    wait_edge_present(&rig.core, w_id, h_id);
    let sp = ok(h.savepoint());
    let sw = ok(h.next_seq());
    upd(&rig.core, &h, sw, b"/t/1/n2", b"v2");
    // Bump late inside the window: a per-iteration deadline would then
    // allow a full extra second past the whole-call bound below.
    std::thread::sleep(Duration::from_millis(850));
    ok(rig.core.rollback_to(&h, sp)); // bump + wake; h stays open
    let bound = start + Duration::from_millis(1450);
    match rx.recv_timeout(bound.saturating_duration_since(Instant::now())) {
        Ok(Err(TxnError::LockNotAvailable)) => {}
        Ok(other) => panic!("lock_timeout expiry is 55P03, got {other:?}"),
        Err(_) => panic!(
            "the mid-call generation bump extended lock_timeout past the whole-call deadline"
        ),
    }
    assert!(
        start.elapsed() >= Duration::from_millis(800),
        "the call honoured its lock_timeout (it did not fail early)"
    );
    assert!(
        start.elapsed() <= Duration::from_millis(1450),
        "one deadline covered the whole call, not just the last re-wait"
    );
    ok(rig.core.abort(h));
}
