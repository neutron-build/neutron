//! C-OC-LIVE: the ON CONFLICT restart livelock (ESCALATE-1). Two RC
//! sessions each UPDATE their own row and then upsert (`ON CONFLICT (pk)
//! DO UPDATE`) the row the other holds. Every restart's `ROLLBACK TO`
//! released nothing, yet it bumped the abandoning txn's wake generation
//! and deleted its waiters' edges, so the two statements kept waking each
//! other, no wait lasted `deadlock_timeout`, and the deadlock DFS never
//! saw a stable cycle (I-LIVE b). A second form survived the first fix:
//! `arbiter_insert` abandoned on a wait at **any** op — a non-arbiter
//! unique entry and the `/t/` key when `!pk_arbiter` — releasing the
//! layers the attempt had placed and bumping again.
//!
//! The fix under test (§5.5, §5.3.1(2)): `rollback_to` bumps only if it
//! released something, and a non-arbiter key's wait is an ordinary §5.1
//! key-existence wait that waits in place (PostgreSQL's
//! `UNIQUE_CHECK_YES`). So a wait-for cycle of two restarting upserts
//! stays stable and the §6 deadlock check breaks it with exactly one
//! 40P01.
//!
//! Harness (the r5 rule, see `c_t2c_deadline.rs`): every multi-thread
//! test runs its statements on DETACHED `std::thread::spawn` threads —
//! everything the thread needs (`Arc<Core>`, the `Txn`, the sender)
//! moves in as `'static` — and the main thread bounds them with
//! `recv_timeout`. On the bound the test fires both cancel handles and
//! PANICS WITHOUT JOINING: a livelock regression fails the bound instead
//! of hanging the binary (the stuck threads stay detached; their
//! cancelled statements drain and their threads exit). No
//! `std::thread::scope`, no `JoinHandle::join` on a possibly blocked
//! thread, no `process::abort`. The victim thread aborts its own txn
//! after its 40P01; the winner sends its `Txn` back so the main thread
//! commits it (`Rig::commit`).
//!
//! Mutants: M1 (the unconditional bump in `rollback_to`) fails T1, T2, T4
//! and T5(a) at their bounds; M2 (abandon on every wait in
//! `arbiter_insert`) fails T3; M3/M4 (never set `released` for the layer
//! / shared-row-lock loop) fail T5(b)/(c) and the seed 26/46 tests; M5
//! (`LockManager::release_from` always `false`) fails T5(d) and
//! `locks_relation`'s rollback test. C-FIXUPS1 pins the ones those left:
//! M8 (`CatalogRelease::release_from` ignoring the inner hook's bool)
//! fails the catalog-chain wake test; M9 (the advisory branch of
//! `LockManager::release_from` never setting `released`) fails the
//! advisory-only wake test — `locks_relation`'s rollback test releases a
//! relation lock in the same rollback, whose branch sets `released`
//! anyway, masking it; M10 (the non-arbiter wait-in-place passing `None`
//! instead of the statement deadline) fails the `lock_timeout` test.

mod c_t2c_support;

use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use c_t2c_support::{assert_count_exact, ok, u64v, CountingWaits, Fixed, Rig};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::Catalog;
use nucleus_txn::locks::{LockManager, RelLockMode};
use nucleus_txn::ssi::Ssi;
use nucleus_txn::txn::{CancelHandle, Isolation, Txn};
use nucleus_txn::wait::{WaitBegin, WaitOutcome};
use nucleus_txn::write::{
    IndexEntry, LockWait, OnConflictAction, OnConflictOutcome, OnConflictResult, ProposedRow,
    RowOp, StmtCtx, UniqueRule,
};
use nucleus_txn::{RowLockMode, TxnError, TxnId};

/// The tests' `deadlock_timeout` (§6), so a cycle is broken at ~100 ms.
const DEADLOCK_MS: u64 = 100;
/// The per-side bound of the multi-thread tests.
const BOUND: Duration = Duration::from_secs(2);

/// A pending UPDATE of `key` in statement `seq` (the rig's fixed EPQ).
fn upd(core: &Core<MemKv>, txn: &Txn, seq: u32, key: &[u8], v: u64) {
    let op = RowOp::Update {
        value: u64v(v),
        key_cols_changed: false,
    };
    ok(core.row_op(
        txn,
        key,
        None,
        op.clone(),
        StmtCtx::new(core.visible_ts(), seq, seq),
        &mut Fixed(op),
    ));
}

/// A `ProposedRow` for `t_key` with no `/u/` entries and the PK as the
/// arbiter — G2's upsert shape, `ON CONFLICT (pk) DO UPDATE`.
fn pk_row(t_key: &[u8], value: u64) -> ProposedRow {
    ProposedRow {
        t_key: t_key.to_vec(),
        value: u64v(value),
        entries: Vec::new(),
        pk_arbiter: true,
    }
}

/// One side's report: the statement's result and, for the winner, its txn
/// (the victim aborts its own txn on its own thread, so `None`).
type Report = (Result<OnConflictOutcome, TxnError>, Option<Txn>);

/// One detached side: the channel its thread reports through, the cancel
/// handle of its txn, and when its statement started.
struct Side {
    rx: mpsc::Receiver<Report>,
    leash: CancelHandle,
    start: Instant,
}

/// Runs `body(core, txn)` on a detached thread and returns its side. The
/// body owns the txn: on an error it aborts it itself (the victim rule)
/// and reports `None`; on success it reports the txn back.
fn spawn_session(
    core: &Arc<Core<MemKv>>,
    txn: Txn,
    body: impl FnOnce(Arc<Core<MemKv>>, Txn) -> Report + Send + 'static,
) -> Side {
    let leash = txn.cancel_handle();
    let (tx, rx) = mpsc::channel();
    let core = Arc::clone(core);
    let start = Instant::now();
    std::thread::spawn(move || {
        let _ = tx.send(body(core, txn));
    });
    Side { rx, leash, start }
}

/// The DO UPDATE upsert body: `set_to` is the closure's new value (it is
/// only ever called on a locked conflicting row).
fn upsert_body(
    ctx: StmtCtx,
    row: ProposedRow,
    set_to: u64,
) -> impl FnOnce(Arc<Core<MemKv>>, Txn) -> Report + Send + 'static {
    move |core, txn| {
        let mut upd = move |_: &[u8]| Some((u64v(set_to), false));
        match core.insert_on_conflict(
            &txn,
            ctx,
            row,
            &|p: &[u8]| p.to_vec(),
            &mut |_| {},
            OnConflictAction::DoUpdate(&mut upd),
        ) {
            Ok(o) => (Ok(o), Some(txn)),
            Err(e) => {
                let _ = core.abort(txn);
                (Err(e), None)
            }
        }
    }
}

/// Receives one side's report within `bound`. On the bound: fire every
/// leash (this side's first), then PANIC — never joining the detached
/// threads, so a regression fails the bound instead of hanging the
/// binary (the cancelled statements drain and their threads exit).
fn recv_bounded(
    side: &Side,
    leashes: &[CancelHandle],
    bound: Duration,
    what: &str,
) -> (Result<OnConflictOutcome, TxnError>, Option<Txn>, Duration) {
    match side.rx.recv_timeout(bound) {
        Ok((r, txn)) => (r, txn, side.start.elapsed()),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            side.leash.cancel();
            for l in leashes {
                l.cancel();
            }
            panic!("{what} never resolved within {bound:?} (a livelock?)");
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{what}'s thread ended without a report");
        }
    }
}

/// The wake generation of `id` (read like the tests in `wait.rs`).
fn gen_of(core: &Core<MemKv>, id: TxnId) -> u64 {
    core.status.entry(id).map_or(0, |e| e.gen)
}

/// T1 — the ESCALATE-1 repro, no `lock_timeout`: two RC txns each UPDATE
/// their own row, then each upserts the row the other holds (pk arbiter,
/// DO UPDATE), each on its own detached thread. With the fix, no restart
/// bumps (the attempt had placed nothing), the wait-for cycle is stable,
/// and the deadlock check breaks it with exactly one 40P01 at
/// `deadlock_timeout`. Without the fix both statements spin (about 85k
/// waits in 3 s) and the 2 s bound fails with the binary exiting.
#[test]
fn upsert_cross_cycle_one_deadlock_no_lock_timeout() {
    let rig = Rig::new();
    rig.core
        .waits
        .set_deadlock_timeout(Duration::from_millis(DEADLOCK_MS));
    let waits = CountingWaits::new();
    rig.core.waits.set_hook(waits.clone());
    rig.preload(b"/t/r/a", &u64v(1));
    rig.preload(b"/t/r/b", &u64v(2));

    // A updates /t/r/a, B updates /t/r/b — both pending, no lock_timeout.
    let a = rig.txn(Isolation::ReadCommitted);
    let b = rig.txn(Isolation::ReadCommitted);
    let seq_a = ok(a.next_seq());
    upd(&rig.core, &a, seq_a, b"/t/r/a", 11);
    let seq_b = ok(b.next_seq());
    upd(&rig.core, &b, seq_b, b"/t/r/b", 22);
    assert_count_exact(&rig.core, &a);
    assert_count_exact(&rig.core, &b);

    // A upserts /t/r/b (DO UPDATE to 111), B upserts /t/r/a (to 222).
    let sa = ok(a.next_seq());
    let sb = ok(b.next_seq());
    let s = rig.core.visible_ts();
    let side_a = spawn_session(
        &rig.core,
        a,
        upsert_body(StmtCtx::new(s, sa, sa), pk_row(b"/t/r/b", 111), 111),
    );
    let side_b = spawn_session(
        &rig.core,
        b,
        upsert_body(StmtCtx::new(s, sb, sb), pk_row(b"/t/r/a", 222), 222),
    );
    let leashes = [side_a.leash.clone(), side_b.leash.clone()];
    let (ra, ta, ela) = recv_bounded(&side_a, &leashes, BOUND, "A's upsert");
    let (rb, tb, elb) = recv_bounded(&side_b, &leashes, BOUND, "B's upsert");

    // Exactly one 40P01; the other statement completed with Updated.
    let deadlocks = [&ra, &rb]
        .iter()
        .filter(|r| matches!(r, Err(TxnError::Deadlock)))
        .count();
    assert_eq!(deadlocks, 1, "exactly one 40P01: {ra:?} / {rb:?}");
    let (winner_txn, winner_key, winner_val, victim_elapsed) =
        if matches!(&ra, Err(TxnError::Deadlock)) {
            // A is the victim; B's DO UPDATE landed on /t/r/a.
            assert_eq!(
                rb.as_ref().map(|o| o.result),
                Ok(OnConflictResult::Updated),
                "the non-victim completed with Updated"
            );
            (tb, b"/t/r/a".to_vec(), 222u64, ela)
        } else {
            assert_eq!(
                ra.as_ref().map(|o| o.result),
                Ok(OnConflictResult::Updated),
                "the non-victim completed with Updated"
            );
            (ta, b"/t/r/b".to_vec(), 111u64, elb)
        };

    // The victim's 40P01 came at `deadlock_timeout`, not early, not late.
    assert!(
        victim_elapsed >= Duration::from_millis(DEADLOCK_MS),
        "the 40P01 must not predate deadlock_timeout ({victim_elapsed:?})"
    );
    assert!(
        victim_elapsed < Duration::from_secs(1),
        "the 40P01 must be prompt ({victim_elapsed:?})"
    );
    // No spin: the whole scenario made a handful of waits (the livelock
    // made about 85k in 3 s).
    let total = waits.starts.load(Ordering::SeqCst);
    assert!(total <= 8, "the scenario must not spin: {total} waits");

    // The winner: I-COUNT exact before the commit, a clean graph after
    // it, and its DO UPDATE value visible.
    let winner_txn = winner_txn.expect("the winner sends its txn back");
    assert_count_exact(&rig.core, &winner_txn);
    rig.commit(winner_txn);
    rig.resolve();
    assert!(
        rig.core.wait_edges().is_empty(),
        "wait edges left: {:?}",
        rig.core.wait_edges()
    );
    assert!(
        rig.core.wait_slots().is_empty(),
        "wait slots left: {:?}",
        rig.core.wait_slots()
    );
    assert_eq!(rig.read_latest(&winner_key), Some(u64v(winner_val)));
}

/// T2 — the same cycle with `lock_timeout` (2 s) on both statements:
/// 40P01 (at 100 ms) must beat 55P03 (at 2 s). This card's fix — not
/// r5's statement deadline — is what ends the cycle.
#[test]
fn upsert_cross_cycle_deadlock_beats_lock_timeout() {
    let rig = Rig::new();
    rig.core
        .waits
        .set_deadlock_timeout(Duration::from_millis(DEADLOCK_MS));
    rig.preload(b"/t/r/a", &u64v(1));
    rig.preload(b"/t/r/b", &u64v(2));

    let a = rig.txn(Isolation::ReadCommitted);
    let b = rig.txn(Isolation::ReadCommitted);
    let seq_a = ok(a.next_seq());
    upd(&rig.core, &a, seq_a, b"/t/r/a", 11);
    let seq_b = ok(b.next_seq());
    upd(&rig.core, &b, seq_b, b"/t/r/b", 22);

    let sa = ok(a.next_seq());
    let sb = ok(b.next_seq());
    let s = rig.core.visible_ts();
    let side_a = spawn_session(
        &rig.core,
        a,
        upsert_body(
            StmtCtx::new(s, sa, sa).lock_timeout(Duration::from_secs(2)),
            pk_row(b"/t/r/b", 111),
            111,
        ),
    );
    let side_b = spawn_session(
        &rig.core,
        b,
        upsert_body(
            StmtCtx::new(s, sb, sb).lock_timeout(Duration::from_secs(2)),
            pk_row(b"/t/r/a", 222),
            222,
        ),
    );
    let leashes = [side_a.leash.clone(), side_b.leash.clone()];
    let (ra, ta, ela) = recv_bounded(&side_a, &leashes, BOUND, "A's upsert");
    let (rb, tb, elb) = recv_bounded(&side_b, &leashes, BOUND, "B's upsert");

    let deadlocks = [&ra, &rb]
        .iter()
        .filter(|r| matches!(r, Err(TxnError::Deadlock)))
        .count();
    assert_eq!(deadlocks, 1, "exactly one 40P01: {ra:?} / {rb:?}");
    for (what, r) in [("A", &ra), ("B", &rb)] {
        assert!(
            !matches!(r, Err(TxnError::LockNotAvailable)),
            "{what}'s statement must not end in 55P03 (40P01 comes first): {r:?}"
        );
    }
    assert!(ela < Duration::from_secs(1), "A done in < 1 s ({ela:?})");
    assert!(elb < Duration::from_secs(1), "B done in < 1 s ({elb:?})");
    // Clean up whichever side won (the victim already aborted itself).
    for t in [ta, tb].into_iter().flatten() {
        ok(rig.core.abort(t));
    }
    assert!(rig.core.wait_edges().is_empty());
    assert!(rig.core.wait_slots().is_empty());
}

/// T3 — the second form of the livelock: both txns hold pending
/// **non-arbiter** unique values, and each upserts a new row whose
/// non-arbiter entry is the other's pending value. The non-arbiter wait
/// must wait in place (§5.1, PostgreSQL `UNIQUE_CHECK_YES`): the attempt
/// keeps its layers, the cycle is stable, exactly one 40P01. With the
/// abandon-on-any-wait behaviour (M2) the attempts kept releasing their
/// layers, bumping and restarting — no 40P01, and this test fails at its
/// bound.
#[test]
fn nonarbiter_unique_cross_cycle_one_deadlock() {
    let rig = Rig::new();
    rig.core
        .waits
        .set_deadlock_timeout(Duration::from_millis(DEADLOCK_MS));
    let waits = CountingWaits::new();
    rig.core.waits.set_hook(waits.clone());

    // A holds /u/n/N1 pending, B holds /u/n/N2 pending.
    let a = rig.txn(Isolation::ReadCommitted);
    let b = rig.txn(Isolation::ReadCommitted);
    let seq_a = ok(a.next_seq());
    ok(rig.core.insert_key(
        &a,
        b"/u/n/N1",
        None,
        b"a2".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), seq_a, seq_a),
        UniqueRule::Unique { same_row: None },
    ));
    let seq_b = ok(b.next_seq());
    ok(rig.core.insert_key(
        &b,
        b"/u/n/N2",
        None,
        b"b2".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), seq_b, seq_b),
        UniqueRule::Unique { same_row: None },
    ));

    // A upserts /t/r/a2 with [/u/a/a2 arbiter, /u/n/N2 non-arbiter]; B
    // upserts /t/r/b2 with [/u/a/b2 arbiter, /u/n/N1 non-arbiter].
    fn entry(key: &[u8], payload: &[u8], arbiter: bool) -> IndexEntry {
        IndexEntry {
            key: key.to_vec(),
            value: payload.to_vec(),
            arbiter,
        }
    }
    let row_a = ProposedRow {
        t_key: b"/t/r/a2".to_vec(),
        value: u64v(1),
        entries: vec![
            entry(b"/u/a/a2", b"a2", true),
            entry(b"/u/n/N2", b"a2", false),
        ],
        pk_arbiter: false,
    };
    let row_b = ProposedRow {
        t_key: b"/t/r/b2".to_vec(),
        value: u64v(2),
        entries: vec![
            entry(b"/u/a/b2", b"b2", true),
            entry(b"/u/n/N1", b"b2", false),
        ],
        pk_arbiter: false,
    };

    let sa = ok(a.next_seq());
    let sb = ok(b.next_seq());
    let s = rig.core.visible_ts();
    let side_a = spawn_session(&rig.core, a, upsert_body(StmtCtx::new(s, sa, sa), row_a, 3));
    let side_b = spawn_session(&rig.core, b, upsert_body(StmtCtx::new(s, sb, sb), row_b, 4));
    let leashes = [side_a.leash.clone(), side_b.leash.clone()];
    let (ra, ta, _ela) = recv_bounded(&side_a, &leashes, BOUND, "A's upsert");
    let (rb, tb, _elb) = recv_bounded(&side_b, &leashes, BOUND, "B's upsert");

    let deadlocks = [&ra, &rb]
        .iter()
        .filter(|r| matches!(r, Err(TxnError::Deadlock)))
        .count();
    assert_eq!(deadlocks, 1, "exactly one 40P01: {ra:?} / {rb:?}");
    let winner_txn = if matches!(&ra, Err(TxnError::Deadlock)) {
        assert_eq!(
            rb.as_ref().map(|o| o.result),
            Ok(OnConflictResult::Inserted),
            "the non-victim inserted its row"
        );
        tb
    } else {
        assert_eq!(
            ra.as_ref().map(|o| o.result),
            Ok(OnConflictResult::Inserted),
            "the non-victim inserted its row"
        );
        ta
    };
    let total = waits.starts.load(Ordering::SeqCst);
    assert!(total <= 8, "the scenario must not spin: {total} waits");

    let winner_txn = winner_txn.expect("the winner sends its txn back");
    assert_count_exact(&rig.core, &winner_txn);
    rig.commit(winner_txn);
    rig.resolve();
    assert!(
        rig.core.wait_edges().is_empty(),
        "wait edges left: {:?}",
        rig.core.wait_edges()
    );
    assert!(
        rig.core.wait_slots().is_empty(),
        "wait slots left: {:?}",
        rig.core.wait_slots()
    );
}

/// T4 — G2's `g2_escalate_upsert_cycle_livelocks`, ported to this
/// harness: a real `LockManager` installed (so `rollback_to` runs a real
/// `ReleaseHook::release_from` under its restarts), `deadlock_timeout`
/// 5 ms as G2 uses, a `Barrier` between each session's own-row UPDATE
/// and its cross upsert, detached threads with a 10 s `recv_timeout`
/// bound (replacing G2's `process::abort` watchdog), and 20 fresh-core
/// repeats of the scenario. Each repeat ends in exactly one 40P01.
#[test]
fn g2_escalate_upsert_cycle_port() {
    /// One G2 session body: UPDATE the own row, wait at the barrier
    /// (both updates are placed before either upsert starts), then
    /// upsert (pk arbiter, DO UPDATE) the other's row.
    fn g2_body(
        barrier: Arc<Barrier>,
        own: &'static [u8],
        other: &'static [u8],
        set_to: u64,
    ) -> impl FnOnce(Arc<Core<MemKv>>, Txn) -> Report + Send + 'static {
        move |core, txn| {
            let seq = ok(txn.next_seq());
            let op = RowOp::Update {
                value: u64v(set_to),
                key_cols_changed: false,
            };
            if let Err(e) = core.row_op(
                &txn,
                own,
                None,
                op.clone(),
                StmtCtx::new(core.visible_ts(), seq, seq),
                &mut Fixed(op),
            ) {
                let _ = core.abort(txn);
                return (Err(e), None);
            }
            barrier.wait();
            let seq0 = ok(txn.next_seq());
            upsert_body(
                StmtCtx::new(core.visible_ts(), seq0, seq0),
                pk_row(other, set_to),
                set_to + 100,
            )(core, txn)
        }
    }

    const REPEATS: usize = 20;
    let bound = Duration::from_secs(10);
    let mut total_deadlocks = 0usize;
    for _ in 0..REPEATS {
        let rig = Rig::new();
        let _locks = LockManager::install(&rig.core);
        rig.core
            .waits
            .set_deadlock_timeout(Duration::from_millis(5));
        rig.preload(b"/t/r/0", &u64v(0));
        rig.preload(b"/t/r/2", &u64v(2));
        let barrier = Arc::new(Barrier::new(2));

        let a = rig.txn(Isolation::ReadCommitted);
        let b = rig.txn(Isolation::ReadCommitted);
        let side_a = spawn_session(
            &rig.core,
            a,
            g2_body(Arc::clone(&barrier), b"/t/r/0", b"/t/r/2", 10),
        );
        let side_b = spawn_session(&rig.core, b, g2_body(barrier, b"/t/r/2", b"/t/r/0", 20));
        let leashes = [side_a.leash.clone(), side_b.leash.clone()];
        let (ra, ta, _) = recv_bounded(&side_a, &leashes, bound, "A's session");
        let (rb, tb, _) = recv_bounded(&side_b, &leashes, bound, "B's session");

        let deadlocks = [&ra, &rb]
            .iter()
            .filter(|r| matches!(r, Err(TxnError::Deadlock)))
            .count();
        assert_eq!(
            deadlocks, 1,
            "exactly one 40P01 per repeat: {ra:?} / {rb:?}"
        );
        total_deadlocks += deadlocks;
        // The winner returns its txn; the main thread ends it and drains
        // resolution, so each repeat's core is clean before it drops.
        for t in [ta, tb].into_iter().flatten() {
            ok(rig.core.abort(t));
        }
        rig.resolve();
    }
    assert_eq!(total_deadlocks, REPEATS, "every repeat broke its cycle");
}

/// T5 — the bump predicate itself, step-driven on one thread
/// (`wait_begin` / `wait_edges` / `wait_poll`; no parking): a rollback
/// that released nothing must not bump; one that dropped a layer, a
/// shared row lock, or a relation lock must (I-LIVE a).
#[test]
fn rollback_to_bumps_iff_released() {
    // (a) A pending intent from statement 1, nothing after the
    //     savepoint: no bump, the waiter's edge stays, the wait is
    //     still current.
    {
        let rig = Rig::new();
        rig.preload(b"/t/r/x", &u64v(1));
        let t = rig.txn(Isolation::ReadCommitted);
        let s1 = ok(t.next_seq());
        upd(&rig.core, &t, s1, b"/t/r/x", 2); // the pending intent (< sp)
        let w = rig.txn(Isolation::ReadCommitted);
        let g0 = gen_of(&rig.core, t.id);
        let h = match rig.core.wait_begin(&w, &[(t.id, g0)]) {
            WaitBegin::Registered(h) => h,
            other => panic!("the waiter must register, got {other:?}"),
        };
        assert!(
            rig.core.wait_edges().contains(&(w.id, t.id)),
            "the wait registered an edge"
        );
        let sp = ok(t.savepoint());
        ok(rig.core.rollback_to(&t, sp));
        assert_eq!(
            gen_of(&rig.core, t.id),
            g0,
            "(a) a rollback that released nothing must not bump"
        );
        assert!(
            rig.core.wait_edges().contains(&(w.id, t.id)),
            "(a) the waiter's edge must survive a release-free rollback"
        );
        assert_eq!(
            rig.core.wait_poll(&h),
            None,
            "(a) the wait is still current"
        );
        rig.core.wait_end(h);
        ok(rig.core.abort(w));
        ok(rig.core.abort(t));
    }

    // (b) A layer written after the savepoint: bump, the edge goes, the
    //     wait ends GenChanged.
    {
        let rig = Rig::new();
        rig.preload(b"/t/r/x", &u64v(1));
        rig.preload(b"/t/r/y", &u64v(1));
        let t = rig.txn(Isolation::ReadCommitted);
        let sp = ok(t.savepoint());
        let s2 = ok(t.next_seq());
        upd(&rig.core, &t, s2, b"/t/r/y", 2); // layer >= sp
        let w = rig.txn(Isolation::ReadCommitted);
        let g0 = gen_of(&rig.core, t.id);
        let h = match rig.core.wait_begin(&w, &[(t.id, g0)]) {
            WaitBegin::Registered(h) => h,
            other => panic!("the waiter must register, got {other:?}"),
        };
        ok(rig.core.rollback_to(&t, sp));
        assert_eq!(
            gen_of(&rig.core, t.id),
            g0 + 1,
            "(b) dropping a layer bumps the generation"
        );
        assert!(
            !rig.core.wait_edges().contains(&(w.id, t.id)),
            "(b) the bump removes the waiter's edge"
        );
        assert_eq!(
            rig.core.wait_poll(&h),
            Some(WaitOutcome::GenChanged),
            "(b) the woken waiter sees the new generation"
        );
        rig.core.wait_end(h);
        ok(rig.core.abort(w));
        ok(rig.core.abort(t));
    }

    // (c) A shared row lock (KEY SHARE) taken after the savepoint: bump.
    {
        let rig = Rig::new();
        rig.preload(b"/t/r/x", &u64v(1));
        let t = rig.txn(Isolation::ReadCommitted);
        let sp = ok(t.savepoint());
        let s2 = ok(t.next_seq());
        let lock = RowOp::Lock(RowLockMode::KeyShare);
        ok(rig.core.row_op(
            &t,
            b"/t/r/x",
            None,
            lock.clone(),
            StmtCtx::new(rig.core.visible_ts(), s2, s2),
            &mut Fixed(lock),
        ));
        let g0 = gen_of(&rig.core, t.id);
        ok(rig.core.rollback_to(&t, sp));
        assert_eq!(
            gen_of(&rig.core, t.id),
            g0 + 1,
            "(c) releasing the KEY SHARE bumps the generation"
        );
        ok(rig.core.abort(t));
    }

    // (d) A relation lock taken after the savepoint, with the real
    //     LockManager installed: the rollback bumps; the hook itself
    //     reports true for a removal and false when called again.
    {
        use nucleus_txn::commit::ReleaseHook;
        use nucleus_txn::write::LockWait;

        let rig = Rig::new();
        let locks = LockManager::install(&rig.core);
        let t = rig.txn(Isolation::ReadCommitted);
        let sp = ok(t.savepoint());
        assert!(ok(locks.lock_relation(
            &rig.core,
            &t,
            7,
            RelLockMode::AccessExclusive,
            LockWait::Block,
            None,
        )));
        let g0 = gen_of(&rig.core, t.id);
        ok(rig.core.rollback_to(&t, sp));
        assert_eq!(
            gen_of(&rig.core, t.id),
            g0 + 1,
            "(d) releasing the relation lock bumps the generation"
        );

        // The hook's own answer: true when it removed an acquisition,
        // false when there is nothing left to remove.
        let u = rig.txn(Isolation::ReadCommitted);
        let spu = ok(u.savepoint());
        assert!(ok(locks.lock_relation(
            &rig.core,
            &u,
            8,
            RelLockMode::AccessExclusive,
            LockWait::Block,
            None,
        )));
        assert!(
            locks.release_from(u.id, spu),
            "release_from reports the removal"
        );
        assert!(
            !locks.release_from(u.id, spu),
            "release_from reports nothing left"
        );
        ok(rig.core.abort(u));
        ok(rig.core.abort(t));
    }
}

// ------------------------------------------------------------ C-FIXUPS1 ----

/// Runs `f` on a detached thread and returns a receiver of its result
/// (the r5 rule: never join a possibly blocked thread; the caller bounds
/// with `recv_timeout` and a fired cancel handle drains the statement).
fn detached<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx
}

/// The C-FIXUPS1 driver: T takes an **advisory** lock after a savepoint
/// — and nothing else after it, no layer, no row lock, no relation lock —
/// W parks on that advisory lock, and T's `ROLLBACK TO` releases exactly
/// the advisory lock. W must wake promptly: `rollback_to` bumps only if
/// the release hook reported something, and the advisory lock is the
/// only thing that could be reported.
fn advisory_only_rollback_wakes(locks: Arc<LockManager>, rig: &Rig, what: &str) {
    // The deadlock check must not fire inside the bounds (there is no
    // cycle): park the waiter on a long deadlock timeout.
    rig.core.waits.set_deadlock_timeout(Duration::from_secs(30));
    let t = rig.txn(Isolation::ReadCommitted);
    let sp = ok(t.savepoint());
    assert!(
        ok(locks.advisory_xact_lock(&rig.core, &t, 42, false, LockWait::Block, None,)),
        "T takes the advisory lock after the savepoint"
    );
    let w = rig.txn(Isolation::ReadCommitted);
    let wid = w.id;
    let leash = w.cancel_handle();
    let core = Arc::clone(&rig.core);
    let waiter_locks = Arc::clone(&locks);
    let rx = detached(move || {
        let r = waiter_locks.advisory_xact_lock(&core, &w, 42, false, LockWait::Block, None);
        let _ = core.abort(w);
        r
    });
    // W must already be waiting on T when the rollback runs: the edge and
    // the slot land in one graph critical section, so observing the edge
    // means the wait is registered (a rollback before that would let W
    // acquire without ever parking, passing a mutant the same way).
    let t0 = Instant::now();
    while !rig.core.wait_edges().contains(&(wid, t.id)) {
        assert!(
            t0.elapsed() < BOUND,
            "{what}: W never blocked on the advisory lock"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    let start = Instant::now();
    ok(rig.core.rollback_to(&t, sp));
    match rx.recv_timeout(BOUND) {
        Ok(r) => {
            assert_eq!(r, Ok(true), "{what}: the woken waiter acquires");
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "{what}: the wake is prompt ({:?})",
                start.elapsed()
            );
        }
        Err(_) => {
            leash.cancel();
            panic!(
                "{what}: LOST WAKE — W still parked after a ROLLBACK TO \
                 that released only its blocking advisory lock"
            );
        }
    }
    ok(rig.core.abort(t));
    assert!(
        rig.core.wait_edges().is_empty(),
        "wait edges left: {:?}",
        rig.core.wait_edges()
    );
    assert!(
        rig.core.wait_slots().is_empty(),
        "wait slots left: {:?}",
        rig.core.wait_slots()
    );
}

/// A `ROLLBACK TO` that releases **only** an advisory lock wakes the
/// waiter on it. The advisory branch of
/// `LockManager::release_from` must set `released` for exactly this
/// rollback: no layer drop, no shared row lock and no relation lock
/// happens, so the other branches cannot report the release for it.
/// Mutant killed: M9 (that branch never sets `released`) —
/// `rollback_to` bumps nobody and W stays parked; this test fails at its
/// bound. `locks_relation::rollback_to_releases_relation_and_advisory_locks`
/// releases a relation lock in the same rollback, whose branch sets
/// `released` anyway, so it cannot catch M9; this test has no relation
/// lock to mask it.
#[test]
fn rollback_to_releasing_only_an_advisory_lock_wakes_the_waiter() {
    let rig = Rig::new();
    let locks = LockManager::install(&rig.core);
    advisory_only_rollback_wakes(locks, &rig, "advisory-only");
}

/// The same advisory-only release through the production
/// `Catalog::open` hook chain (`CatalogRelease` wrapping the lock
/// manager's hook — the chain `Catalog::open` installs on every booted
/// core): the inner hook's `released` bool must survive the chain, or
/// the journal-less rollback (no DDL ran) reports nothing and wakes
/// nobody.
/// Mutant killed: M8 (`CatalogRelease::release_from` ignoring the inner
/// hook's bool) — the advisory lock is still released, but the bump
/// never runs and W stays parked; this test fails at its bound.
#[test]
fn rollback_to_advisory_only_release_wakes_through_catalog_chain() {
    let rig = Rig::new();
    let ssi = Ssi::install(&rig.core);
    let locks = LockManager::install(&rig.core);
    let _catalog = ok(Catalog::open(&rig.core, &ssi, &locks));
    advisory_only_rollback_wakes(locks, &rig, "catalog-chain");
}

/// A row whose non-arbiter entry `/u/n/N` is the only thing another txn
/// holds pending: the `/t/` insert and the arbiter entry place freely,
/// and the non-arbiter wait-in-place is the statement's only wait.
fn row_with_n() -> ProposedRow {
    fn u_entry(key: &[u8], payload: &[u8], arbiter: bool) -> IndexEntry {
        IndexEntry {
            key: key.to_vec(),
            value: payload.to_vec(),
            arbiter,
        }
    }
    ProposedRow {
        t_key: b"/t/r/new".to_vec(),
        value: u64v(5),
        entries: vec![
            u_entry(b"/u/a/new", b"/t/r/new", true),
            u_entry(b"/u/n/N", b"/t/r/new", false),
        ],
        pk_arbiter: false,
    }
}

/// The non-arbiter wait-in-place runs on the statement's one
/// `lock_timeout` deadline (§5.3.1(2), r5): B's upsert waits in place on
/// A's pending non-arbiter value and must return 55P03 when the deadline
/// passes — not park until A ends — leaving no stale edges or slots.
/// Mutant killed: M10 (that wait passing `None` instead of the statement
/// deadline) — the wait is unbounded, no 55P03 arrives, and this test
/// fails at its bound (the fired cancel handle ends the statement).
#[test]
fn nonarbiter_wait_in_place_honours_lock_timeout() {
    let rig = Rig::new();
    rig.core.waits.set_deadlock_timeout(Duration::from_secs(30));
    // A holds /u/n/N pending (no lock_timeout of its own; it never waits).
    let a = rig.txn(Isolation::ReadCommitted);
    let sa = ok(a.next_seq());
    ok(rig.core.insert_key(
        &a,
        b"/u/n/N",
        None,
        b"/t/r/other".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), sa, sa),
        UniqueRule::Unique { same_row: None },
    ));
    // B upserts a row whose non-arbiter entry is that value, with a
    // 250 ms lock_timeout: the wait-in-place must end in 55P03 at it.
    let b = rig.txn(Isolation::ReadCommitted);
    let sb = ok(b.next_seq());
    let ctx = StmtCtx::new(rig.core.visible_ts(), sb, sb).lock_timeout(Duration::from_millis(250));
    let side = spawn_session(&rig.core, b, upsert_body(ctx, row_with_n(), 77));
    let leashes = [side.leash.clone()];
    let (r, tb, el) = recv_bounded(&side, &leashes, BOUND, "B's upsert");
    assert_eq!(
        r.map(|o| o.result),
        Err(TxnError::LockNotAvailable),
        "the wait-in-place ends in 55P03 at lock_timeout"
    );
    assert!(
        el >= Duration::from_millis(200),
        "55P03 must not predate the deadline ({el:?})"
    );
    assert!(
        el < Duration::from_millis(1500),
        "55P03 must be prompt, not the park slice or the deadlock timeout ({el:?})"
    );
    // The victim rule aborted B on its own thread; nothing of the wait
    // survives, and A's abort leaves a clean graph.
    assert!(tb.is_none(), "the failed statement's txn was aborted");
    ok(rig.core.abort(a));
    assert!(
        rig.core.wait_edges().is_empty(),
        "wait edges left: {:?}",
        rig.core.wait_edges()
    );
    assert!(
        rig.core.wait_slots().is_empty(),
        "wait slots left: {:?}",
        rig.core.wait_slots()
    );
}
