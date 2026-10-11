//! C-OC-TIE: the ON CONFLICT speculative-insertion tie race, the MEDIUM
//! residual the C-OC-LIVE gate found (§5.3.1(2), draft 7.5). Two RC upserts
//! propose different rows whose arbiter entries are X,Y and Y,X (the
//! caller's `entries` order). Both pass the §5.3.1(1) pre-check (nothing is
//! placed yet), each places its `/t/` key and its first arbiter entry, and
//! each then meets the other's freshly placed entry: both attempts abandon
//! in `arbiter_insert`, both `rollback_to`s really release layers and so
//! bump (legitimately, §5.5), each wait ends `GenChanged` at once, and both
//! restart in lockstep. The race never raises 40P01; it ends only when
//! thread scheduling breaks the symmetry (on 988a9bf9: both sides
//! restarted in 1729/2000 iterations, worst 126 restarts and 19 ms).
//!
//! The fix under test: `arbiter_insert` places every op of the attempt —
//! the `/t/` key, arbiter entries and non-arbiter entries — in ascending
//! key order. An attempt blocked at `k` then holds exactly its keys `< k`,
//! so no two attempts (no cycle of attempts) can each be blocked on
//! another's attempt layers: the loser abandons once and waits on a winner
//! that released nothing and so never bumps.
//!
//! Harness (the r5 rule, as `c_oc_live.rs`): every multi-thread test runs
//! its statement on DETACHED `std::thread::spawn` threads — everything the
//! thread needs (`Arc<Core>`, the `Txn`, the sender) moves in as `'static`
//! — and the main thread bounds them with `recv_timeout(5 s)`. On the
//! bound it fires every cancel handle and PANICS WITHOUT JOINING, so a
//! livelock regression fails the bound instead of hanging the binary (the
//! cancelled statements drain and their threads exit). No
//! `std::thread::scope`, no `JoinHandle::join`, no `process::abort`.
//!
//! Mutants: M1 (no sort — caller order) fails T1, T3, T4 and very likely
//! T2 at their bounds; M2 (descending sort) fails only T3 — any one fixed
//! total order prevents the race, and T3 pins the spec's ascending choice
//! while T1/T2 pin the bound; M3 (sort only the arbiter ops, one plausible
//! half-fix) fails only T3.

mod c_t2c_support;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use c_t2c_support::{assert_count_exact, ok, u64v, CountingWaits, Rig};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::txn::{CancelHandle, Isolation, Txn};
use nucleus_txn::write::{
    IndexEntry, OnConflictAction, OnConflictOutcome, OnConflictResult, ProposedRow, StmtCtx,
    UniqueRule,
};
use nucleus_txn::TxnError;

/// The tests' `deadlock_timeout` (§6): a cycle breaks at ~100 ms. The
/// tie race must never form one (T1-T4 assert no `Err` at all). T3 uses
/// 30 s instead: its in-place wait is one-directional, and the poll bound,
/// not the detector, bounds the test.
const DEADLOCK_MS: u64 = 100;
/// The per-side bound of the multi-thread tests.
const BOUND: Duration = Duration::from_secs(5);

/// An index entry whose payload names its row by the row's `t_key`
/// (`t_key_of` is the identity in every test of this card).
fn entry(key: &[u8], t_key: &[u8], arbiter: bool) -> IndexEntry {
    IndexEntry {
        key: key.to_vec(),
        value: t_key.to_vec(),
        arbiter,
    }
}

/// T1/T4's rows: session 0 proposes `/t/r/0` with arbiter entries X,Y;
/// session 1 proposes `/t/r/1` with the same two arbiters in the opposite
/// caller order. Ascending placement is `/t/r/{i} < /u/x/X < /u/y/Y` for
/// both, so both sessions walk X before Y whatever the caller said.
fn tie_row(i: usize) -> ProposedRow {
    let t_key = format!("/t/r/{i}").into_bytes();
    let keys: [&[u8]; 2] = if i == 0 {
        [b"/u/x/X", b"/u/y/Y"]
    } else {
        [b"/u/y/Y", b"/u/x/X"]
    };
    ProposedRow {
        t_key: t_key.clone(),
        value: u64v(i as u64),
        entries: keys.iter().map(|k| entry(k, &t_key, true)).collect(),
        pk_arbiter: false,
    }
}

/// T2's ring row: session `i` proposes `/t/r/{i}` with arbiter entries
/// `/u/k/{i}` and `/u/k/{(i+1)%3}` — a 3-cycle of shared arbiters.
fn ring_row(i: usize) -> ProposedRow {
    let t_key = format!("/t/r/{i}").into_bytes();
    let keys = [
        format!("/u/k/{i}").into_bytes(),
        format!("/u/k/{}", (i + 1) % 3).into_bytes(),
    ];
    ProposedRow {
        t_key: t_key.clone(),
        value: u64v(i as u64),
        entries: keys.iter().map(|k| entry(k, &t_key, true)).collect(),
        pk_arbiter: false,
    }
}

/// One side's report: the statement's outcome, and the txn when the
/// statement succeeded (a side that errored aborts its own txn on its own
/// thread — the victim rule — and reports `None`).
type Report = (Result<OnConflictOutcome, TxnError>, Option<Txn>);

/// One detached side: the channel its thread reports through and the
/// cancel handle of its txn.
struct Side {
    rx: mpsc::Receiver<Report>,
    leash: CancelHandle,
}

/// Runs `body(core, txn)` on a detached thread and returns its side. The
/// body owns the txn: on an error it aborts it itself; on success it
/// reports the txn back (or `None` when it aborted after its statement,
/// as T1/T2 do).
fn spawn_session(
    core: &Arc<Core<MemKv>>,
    txn: Txn,
    body: impl FnOnce(Arc<Core<MemKv>>, Txn) -> Report + Send + 'static,
) -> Side {
    let leash = txn.cancel_handle();
    let (tx, rx) = mpsc::channel();
    let core = Arc::clone(core);
    std::thread::spawn(move || {
        let _ = tx.send(body(core, txn));
    });
    Side { rx, leash }
}

/// Receives one side's report within `bound`. On the bound: fire every
/// leash (this side's first), then PANIC — never joining the detached
/// threads, so a regression fails the bound instead of hanging the binary.
fn recv_bounded(
    side: &Side,
    leashes: &[CancelHandle],
    bound: Duration,
    what: &str,
) -> (Result<OnConflictOutcome, TxnError>, Option<Txn>) {
    match side.rx.recv_timeout(bound) {
        Ok((r, txn)) => (r, txn),
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

/// One T1/T2 race session: take `next_seq`, meet the barrier (both/all
/// sessions pass the pre-check before anything is placed), upsert (DO
/// NOTHING), then abort its own txn — the self-abort is what releases the
/// loser's blocker — and report the outcome.
fn race_session(
    barrier: Arc<Barrier>,
    row: ProposedRow,
) -> impl FnOnce(Arc<Core<MemKv>>, Txn) -> Report + Send + 'static {
    move |core, txn| {
        let seq = ok(txn.next_seq());
        barrier.wait();
        let r = core.insert_on_conflict(
            &txn,
            StmtCtx::new(core.visible_ts(), seq, seq),
            row,
            &|p: &[u8]| p.to_vec(),
            &mut |_| {},
            OnConflictAction::DoNothing,
        );
        let _ = core.abort(txn);
        (r, None)
    }
}

/// One T4 race session: like [`race_session`], but a session that gets
/// `Ok` sends its `Txn` back — the MAIN thread commits it (`Rig::commit`)
/// as soon as it arrives. `set_77` switches DO NOTHING for a DO UPDATE
/// whose closure sets 77.
fn commit_session(
    barrier: Arc<Barrier>,
    row: ProposedRow,
    set_77: bool,
) -> impl FnOnce(Arc<Core<MemKv>>, Txn) -> Report + Send + 'static {
    move |core, txn| {
        let seq = ok(txn.next_seq());
        barrier.wait();
        let mut upd = move |_: &[u8]| Some((u64v(77), false));
        let action = if set_77 {
            OnConflictAction::DoUpdate(&mut upd)
        } else {
            OnConflictAction::DoNothing
        };
        let r = core.insert_on_conflict(
            &txn,
            StmtCtx::new(core.visible_ts(), seq, seq),
            row,
            &|p: &[u8]| p.to_vec(),
            &mut |_| {},
            action,
        );
        match r {
            Ok(o) => (Ok(o), Some(txn)),
            Err(e) => {
                let _ = core.abort(txn);
                (Err(e), None)
            }
        }
    }
}

/// T1 — the gate's shape (the designer's scratch race): two sessions, two
/// rows, arbiter entries X,Y and Y,X in the caller's order. With ascending
/// placement the winner never waits (it restarts zero times, provably: it
/// only ever touches keys the loser cannot hold), the loser abandons once
/// and waits for the winner's self-abort. Without the sort both sides
/// restart in lockstep (86% of iterations on 988a9bf9, worst 126 restarts
/// and 252 waits) and these bounds fail at once.
#[test]
fn spec_race_two_sessions_bounded() {
    const ITERATIONS: usize = 500;
    let mut histogram: BTreeMap<(u32, u32), usize> = BTreeMap::new();
    for _ in 0..ITERATIONS {
        let rig = Rig::new();
        rig.core
            .waits
            .set_deadlock_timeout(Duration::from_millis(DEADLOCK_MS));
        let waits = CountingWaits::new();
        rig.core.waits.set_hook(waits.clone());

        let a = rig.txn(Isolation::ReadCommitted);
        let b = rig.txn(Isolation::ReadCommitted);
        let barrier = Arc::new(Barrier::new(2));
        let side_a = spawn_session(&rig.core, a, race_session(Arc::clone(&barrier), tie_row(0)));
        let side_b = spawn_session(&rig.core, b, race_session(barrier, tie_row(1)));
        let leashes = [side_a.leash.clone(), side_b.leash.clone()];
        let (ra, _) = recv_bounded(&side_a, &leashes, BOUND, "session 0's upsert");
        let (rb, _) = recv_bounded(&side_b, &leashes, BOUND, "session 1's upsert");

        let (oa, ob) = match (&ra, &rb) {
            (Ok(a), Ok(b)) => (*a, *b),
            _ => panic!(
                "both sessions must succeed with no error of any kind (in particular no 40P01): {ra:?} / {rb:?}"
            ),
        };
        assert_eq!(oa.result, OnConflictResult::Inserted, "session 0: {oa:?}");
        assert_eq!(ob.result, OnConflictResult::Inserted, "session 1: {ob:?}");
        assert!(oa.restarts <= 2, "session 0 restarts bounded: {oa:?}");
        assert!(ob.restarts <= 2, "session 1 restarts bounded: {ob:?}");
        assert!(
            !(oa.restarts > 0 && ob.restarts > 0),
            "at most one side may restart (the winner never waits): {oa:?} / {ob:?}"
        );
        let starts = waits.starts.load(Ordering::SeqCst);
        assert!(starts <= 4, "the iteration must not spin: {starts} waits");
        *histogram.entry((oa.restarts, ob.restarts)).or_insert(0) += 1;
    }
    eprintln!(
        "T1 restart histogram (session0, session1) -> iterations over {ITERATIONS}: {histogram:?}"
    );
}

/// T2 — the 3-session ring: session `i` holds `/u/k/{i}` and waits for
/// `/u/k/{(i+1)%3}` (caller order), so every pair of neighbours shares one
/// arbiter and the old caller-order placement could abandon all three on
/// each other. With ascending placement the ring is acyclic; the designer
/// measured max 2 restarts per side and 3 waits with the fix (without it:
/// up to 14 restarts and 42 waits, every session restarting in ~50% of
/// iterations).
#[test]
fn spec_race_three_session_ring_bounded() {
    const ITERATIONS: usize = 300;
    let mut worst: u32 = 0;
    for _ in 0..ITERATIONS {
        let rig = Rig::new();
        rig.core
            .waits
            .set_deadlock_timeout(Duration::from_millis(DEADLOCK_MS));
        let waits = CountingWaits::new();
        rig.core.waits.set_hook(waits.clone());

        let barrier = Arc::new(Barrier::new(3));
        let mut sides = Vec::with_capacity(3);
        let mut leashes = Vec::with_capacity(3);
        for i in 0..3 {
            let t = rig.txn(Isolation::ReadCommitted);
            let side = spawn_session(
                &rig.core,
                t,
                race_session(Arc::clone(&barrier), ring_row(i)),
            );
            leashes.push(side.leash.clone());
            sides.push(side);
        }
        let mut outcomes = Vec::with_capacity(3);
        for (i, side) in sides.iter().enumerate() {
            let (r, _) = recv_bounded(side, &leashes, BOUND, &format!("ring session {i}"));
            let o = match r {
                Ok(o) => o,
                Err(e) => panic!("ring session {i} errored (no 40P01 expected): {e:?}"),
            };
            assert_eq!(o.result, OnConflictResult::Inserted, "ring {i}: {o:?}");
            assert!(o.restarts <= 3, "ring session {i} restarts bounded: {o:?}");
            worst = worst.max(o.restarts);
            outcomes.push(o);
        }
        let sum: u32 = outcomes.iter().map(|o| o.restarts).sum();
        assert!(sum <= 6, "the ring's restart sum is bounded: {outcomes:?}");
        let starts = waits.starts.load(Ordering::SeqCst);
        assert!(starts <= 9, "the iteration must not spin: {starts} waits");
    }
    eprintln!("T2 ring: worst per-side restarts over {ITERATIONS} iterations: {worst}");
}

/// T3 — the placement order itself, deterministic (one race-free
/// sequence): H holds `/u/z/Z` pending; A upserts `/t/r/a` whose caller
/// entry order is `[/u/z/Z non-arbiter, /u/a/A arbiter]`. Ascending order
/// is `/t/r/a < /u/a/A < /u/z/Z`, so A must place `/u/a/A` **before** it
/// waits in place at `/u/z/Z` — while it is parked there it already holds
/// `/u/a/A`'s intent. Caller order (or descending, or the arbiter-only
/// half-fix) waits at `/u/z/Z` first and holds nothing on `/u/a/A`, which
/// this test catches mid-wait, without any race.
#[test]
fn placement_is_ascending_key_order() {
    let rig = Rig::new();
    // 30 s: the wait at /u/z/Z is one-directional (H waits on nobody), so
    // the detector must not touch it; the poll and receive bounds below
    // bound this test instead.
    rig.core.waits.set_deadlock_timeout(Duration::from_secs(30));
    let waits = CountingWaits::new();
    rig.core.waits.set_hook(waits.clone());

    // Holder H inserts /u/z/Z pending (plain key-existence op).
    let h = rig.txn(Isolation::ReadCommitted);
    let h_id = h.id;
    let seq_h = ok(h.next_seq());
    ok(rig.core.insert_key(
        &h,
        b"/u/z/Z",
        None,
        b"holder".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), seq_h, seq_h),
        UniqueRule::Unique { same_row: None },
    ));

    // A upserts /t/r/a with [/u/z/Z non-arbiter, /u/a/A arbiter], in that
    // caller order, on a detached thread (its statement blocks).
    let a = rig.txn(Isolation::ReadCommitted);
    let a_id = a.id;
    let t_key = b"/t/r/a".to_vec();
    let row = ProposedRow {
        t_key: t_key.clone(),
        value: u64v(1),
        entries: vec![
            entry(b"/u/z/Z", &t_key, false),
            entry(b"/u/a/A", &t_key, true),
        ],
        pk_arbiter: false,
    };
    let side = spawn_session(&rig.core, a, {
        move |core, txn| {
            let seq = ok(txn.next_seq());
            match core.insert_on_conflict(
                &txn,
                StmtCtx::new(core.visible_ts(), seq, seq),
                row,
                &|p: &[u8]| p.to_vec(),
                &mut |_| {},
                OnConflictAction::DoNothing,
            ) {
                Ok(o) => (Ok(o), Some(txn)),
                Err(e) => {
                    let _ = core.abort(txn);
                    (Err(e), None)
                }
            }
        }
    });

    // Poll until A waits (bound 2 s, then cancel and panic).
    let poll_until = Instant::now() + Duration::from_secs(2);
    loop {
        if !rig.core.wait_edges().is_empty() {
            break;
        }
        if Instant::now() >= poll_until {
            side.leash.cancel();
            panic!("A never reached its in-place wait at /u/z/Z within 2 s");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    // A waits in place on H, and only on H.
    assert_eq!(rig.core.wait_edges(), vec![(a_id, h_id)], "A waits on H");
    // The load-bearing assert: ascending placement put /u/a/A down before
    // the wait at /u/z/Z, so A already holds it while parked.
    assert_eq!(
        rig.intent(b"/u/a/A").map(|i| i.txn),
        Some(a_id),
        "A must already hold /u/a/A while waiting in place at /u/z/Z"
    );
    let starts = waits.starts.load(Ordering::SeqCst);
    assert!(starts <= 2, "one in-place wait, no spin: {starts} waits");

    // Abort H: A's wait ends and its statement completes within the bound.
    ok(rig.core.abort(h));
    let (r, txn) = recv_bounded(&side, &[], BOUND, "A's upsert");
    let o = match r {
        Ok(o) => o,
        Err(e) => panic!("A's statement must complete Ok after H's abort: {e:?}"),
    };
    assert_eq!(o.result, OnConflictResult::Inserted, "A: {o:?}");
    assert_eq!(o.restarts, 0, "a non-arbiter in-place wait never abandons");

    // After A aborts, the wait graph is clean.
    ok(rig.core.abort(txn.expect("A's txn came back")));
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

/// T4 — the tie race where the winner **commits**: T1's two rows, but a
/// session that gets `Ok` sends its `Txn` back, and the main thread
/// commits each txn (`Rig::commit`) as soon as it arrives (`try_recv`
/// poll under the 5 s bound). The loser stays parked on the winner until
/// that commit; its next attempt then finds the winner's committed arbiter
/// entry in the pre-check, so exactly one side inserts and the other ends
/// `Nothing` (DO NOTHING) or `Updated` (DO UPDATE, whose closure sets 77 —
/// the loser updates the WINNER's row, which `read_latest` shows as 77
/// after both commits).
#[test]
fn spec_race_winner_commits() {
    const ITERATIONS: usize = 200;
    for &(do_update, name) in &[(false, "DO NOTHING"), (true, "DO UPDATE")] {
        let mut histogram: BTreeMap<(u32, u32), usize> = BTreeMap::new();
        for _ in 0..ITERATIONS {
            let rig = Rig::new();
            rig.core
                .waits
                .set_deadlock_timeout(Duration::from_millis(DEADLOCK_MS));
            let waits = CountingWaits::new();
            rig.core.waits.set_hook(waits.clone());

            let a = rig.txn(Isolation::ReadCommitted);
            let b = rig.txn(Isolation::ReadCommitted);
            let barrier = Arc::new(Barrier::new(2));
            let side_a = spawn_session(
                &rig.core,
                a,
                commit_session(Arc::clone(&barrier), tie_row(0), do_update),
            );
            let side_b =
                spawn_session(&rig.core, b, commit_session(barrier, tie_row(1), do_update));
            let leashes = [side_a.leash.clone(), side_b.leash.clone()];

            // Poll both receivers, committing each txn as soon as it
            // arrives (I-COUNT exact before every commit).
            let deadline = Instant::now() + BOUND;
            let mut reports: Vec<Option<OnConflictOutcome>> = vec![None, None];
            let mut done = 0usize;
            while done < 2 {
                if Instant::now() >= deadline {
                    for l in &leashes {
                        l.cancel();
                    }
                    panic!("a {name} session never resolved within {BOUND:?} (a livelock?)");
                }
                let mut progressed = false;
                for (i, side) in [&side_a, &side_b].iter().enumerate() {
                    if reports[i].is_some() {
                        continue;
                    }
                    match side.rx.try_recv() {
                        Ok((r, txn)) => {
                            progressed = true;
                            if let Some(t) = txn {
                                assert_count_exact(&rig.core, &t);
                                rig.commit(t);
                            }
                            match r {
                                Ok(o) => reports[i] = Some(o),
                                Err(e) => panic!("{name} session {i} errored: {e:?}"),
                            }
                            done += 1;
                        }
                        Err(mpsc::TryRecvError::Empty) => {}
                        Err(mpsc::TryRecvError::Disconnected) => {
                            panic!("{name} session {i}'s thread ended without a report");
                        }
                    }
                }
                if !progressed {
                    std::thread::sleep(Duration::from_micros(200));
                }
            }

            let oa = reports[0].expect("session 0 reported");
            let ob = reports[1].expect("session 1 reported");
            assert!(oa.restarts <= 2, "session 0 restarts bounded: {oa:?}");
            assert!(ob.restarts <= 2, "session 1 restarts bounded: {ob:?}");
            assert!(
                !(oa.restarts > 0 && ob.restarts > 0),
                "at most one side may restart: {oa:?} / {ob:?}"
            );
            let inserted = [oa.result, ob.result]
                .iter()
                .filter(|r| **r == OnConflictResult::Inserted)
                .count();
            assert_eq!(inserted, 1, "exactly one Inserted: {oa:?} / {ob:?}");
            let expected_other = if do_update {
                OnConflictResult::Updated
            } else {
                OnConflictResult::Nothing
            };
            let other = [oa.result, ob.result]
                .into_iter()
                .find(|r| *r != OnConflictResult::Inserted)
                .expect("the other side's result");
            assert_eq!(other, expected_other, "{name}: {oa:?} / {ob:?}");
            let starts = waits.starts.load(Ordering::SeqCst);
            assert!(starts <= 4, "the iteration must not spin: {starts} waits");
            *histogram.entry((oa.restarts, ob.restarts)).or_insert(0) += 1;

            rig.resolve();
            if do_update {
                // The loser's DO UPDATE set the WINNER's row to 77.
                let winner_t_key: &[u8] = if oa.result == OnConflictResult::Inserted {
                    b"/t/r/0"
                } else {
                    b"/t/r/1"
                };
                assert_eq!(rig.read_latest(winner_t_key), Some(u64v(77)));
            }
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
        eprintln!(
            "T4 {name}: restart histogram (session0, session1) -> iterations over {ITERATIONS}: {histogram:?}"
        );
    }
}
