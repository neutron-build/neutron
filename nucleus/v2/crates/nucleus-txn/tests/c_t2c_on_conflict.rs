//! C-T2c: the §5.3.1 ON CONFLICT arbiter protocol, driven by hand (the
//! G0 `ocdup` / `ocnothing` shapes rebuilt with steps and callbacks). Rows
//! live at `/t/r/{pk}` with a u64 value; the arbiter entry `/u/a/{k}` names
//! its row by `pk`. Each test names the mutant it kills; I-COUNT is checked
//! after every step.

mod c_t2c_support;

use std::cell::{Cell, RefCell};
use std::sync::Mutex;

use c_t2c_support::{as_u64, assert_count_exact, ok, u64v, Rig};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::write::{
    IndexEntry, OnConflictAction, OnConflictOutcome, OnConflictResult, ProposedRow, StmtCtx,
};
use nucleus_txn::{LayerData, TxnError};

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

/// A proposed row `pk` = `v` with arbiter entries on `arbiters`.
fn proposed(pk: &str, v: u64, arbiters: &[&str]) -> ProposedRow {
    ProposedRow {
        t_key: t_key(pk),
        value: u64v(v),
        entries: arbiters
            .iter()
            .map(|k| IndexEntry {
                key: u_key(k),
                value: pk.as_bytes().to_vec(),
                arbiter: true,
            })
            .collect(),
    }
}

/// `DO UPDATE SET v = v + 1`.
fn incr(v: &[u8]) -> Option<(Vec<u8>, bool)> {
    Some((u64v(as_u64(v) + 1), false))
}

/// Preloads row `pk` = `v` with its arbiter entry `k`.
fn preload_row(rig: &Rig, pk: &str, v: u64, k: &str) {
    rig.preload(&t_key(pk), &u64v(v));
    rig.preload(&u_key(k), pk.as_bytes());
}

fn upsert(
    rig: &Rig,
    txn: &Txn,
    ctx: StmtCtx,
    row: ProposedRow,
    action: OnConflictAction<'_>,
) -> Result<OnConflictOutcome, TxnError> {
    rig.core
        .insert_on_conflict(txn, ctx, row, &t_key_of, &mut |_| {}, action)
}

/// Plain ocnothing / ocdup without races: no conflict inserts; DO NOTHING
/// on a live conflict does nothing; DO UPDATE updates.
#[test]
fn ocnothing_and_ocdup_basics() {
    let rig = Rig::new();
    preload_row(&rig, "0", 0, "K");
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let out = ok(upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq0, seq0),
        proposed("1", 0, &["K"]),
        OnConflictAction::DoNothing,
    ));
    assert_eq!(out.result, OnConflictResult::Nothing);
    assert_eq!(out.restarts, 0);
    assert!(
        rig.intent(&t_key("1")).is_none(),
        "DO NOTHING writes nothing"
    );
    assert_count_exact(&rig.core, &w);

    let out = ok(upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq0, seq0),
        proposed("2", 0, &["J"]),
        OnConflictAction::DoNothing,
    ));
    assert_eq!(out.result, OnConflictResult::Inserted);
    assert_count_exact(&rig.core, &w);

    let seq1 = ok(w.next_seq());
    let mut f = incr;
    let out = ok(upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq1, seq1),
        proposed("3", 0, &["K"]),
        OnConflictAction::DoUpdate(&mut f),
    ));
    assert_eq!(out.result, OnConflictResult::Updated);
    assert_count_exact(&rig.core, &w);

    // A false WHERE keeps the lock and changes no data.
    let seq2 = ok(w.next_seq());
    let mut never = |_: &[u8]| None;
    let out = ok(upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq2, seq2),
        proposed("4", 0, &["J"]),
        OnConflictAction::DoUpdate(&mut never),
    ));
    assert_eq!(out.result, OnConflictResult::WhereFalse);
    assert_count_exact(&rig.core, &w);
    rig.commit(w);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("0")), Some(u64v(1)));
    assert_eq!(rig.read_latest(&t_key("2")), Some(u64v(0)));
    assert_eq!(rig.read_latest(&t_key("3")), None);
    assert_eq!(rig.read_latest(&u_key("J")), Some(b"2".to_vec()));
}

/// Seed 53: the conflicting row is deleted and committed between the
/// pre-check and the lock. The lock must restart the arbiter (which then
/// inserts), never EPQ-and-skip.
/// Mutant killed: run EPQ in the arbiter lock and skip the row (the
/// statement ends `WhereFalse`/`Nothing` with the arbiter key not live).
#[test]
fn seed53_arbiter_lock_restarts_not_skips() {
    let rig = Rig::new();
    preload_row(&rig, "1", 0, "K");
    let w = rig.txn(Isolation::ReadCommitted);
    let deleter = RefCell::new(Some(rig.txn(Isolation::ReadCommitted)));
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let calls = Cell::new(0u32);
    // Between the pre-check (which saw K live, naming row 1) and the
    // lock: T deletes row 1 and its entry and commits (visible, not yet
    // resolved — the lock's latch section finds T's committed intent).
    let race = |payload: &[u8]| -> Vec<u8> {
        calls.set(calls.get() + 1);
        if let Some(t) = deleter.borrow_mut().take() {
            let ts = ok(t.next_seq());
            rig.delete_in(&t, ts, &t_key("1"));
            rig.delete_in(&t, ts, &u_key("K"));
            assert_count_exact(&rig.core, &t);
            rig.commit(t);
            assert_count_exact(&rig.core, &w);
        }
        t_key_of(payload)
    };
    let mut f = incr;
    let out = ok(rig.core.insert_on_conflict(
        &w,
        StmtCtx::new(s, seq0, seq0),
        proposed("2", 7, &["K"]),
        &race,
        &mut |_| {},
        OnConflictAction::DoUpdate(&mut f),
    ));
    assert_eq!(
        out.result,
        OnConflictResult::Inserted,
        "restart, then insert"
    );
    assert_eq!(out.restarts, 1);
    assert_eq!(calls.get(), 1, "the restarted attempt saw no conflict");
    assert!(
        rig.intent(&t_key("1")).is_none_or(|i| i.txn != w.id),
        "no lock remains on the deleted row"
    );
    assert_count_exact(&rig.core, &w);
    rig.commit(w);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("1")), None);
    assert_eq!(rig.read_latest(&t_key("2")), Some(u64v(7)));
    assert_eq!(rig.read_latest(&u_key("K")), Some(b"2".to_vec()));
}

/// Seed 57: `r` was updated above `S` before the pre-check. The lock is
/// measured against `v_r`, so it succeeds at once (zero restarts) and the
/// update is computed from the newest version.
/// Mutant killed: base `S` instead of `v_r.ts` (every attempt restarts;
/// the attempt bound below fails the test instead of spinning).
#[test]
fn seed57_arbiter_lock_base_is_v_r() {
    let rig = Rig::new();
    preload_row(&rig, "1", 0, "K");
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    // Above S, before W's pre-check.
    rig.update(&t_key("1"), &u64v(10));
    assert!(rig.core.visible_ts() > s);
    let mut attempts = 0u32;
    let mut f = incr;
    let out = ok(rig.core.insert_on_conflict(
        &w,
        StmtCtx::new(s, seq0, seq0),
        proposed("2", 0, &["K"]),
        &t_key_of,
        &mut |_| {
            attempts += 1;
            assert!(attempts <= 5, "the arbiter spins (I-PROGRESS)");
        },
        OnConflictAction::DoUpdate(&mut f),
    ));
    assert_eq!(out.result, OnConflictResult::Updated);
    assert_eq!(out.restarts, 0);
    assert_count_exact(&rig.core, &w);
    rig.commit(w);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("1")), Some(u64v(11)));
}

/// What the abandon scenario observed.
struct Abandon {
    /// While W was parked after abandoning attempt 1.
    parked_a_intent: Option<nucleus_txn::Intent>,
    parked_t_intent: Option<nucleus_txn::Intent>,
    parked_events: Vec<(u32, Vec<u8>)>,
    /// The `sa` of every attempt, in order.
    sas: Vec<u32>,
    early: u32,
    seq0: u32,
    outcome: OnConflictOutcome,
    final_events: Vec<(u32, Vec<u8>)>,
    final_t_intent: Option<nucleus_txn::Intent>,
}

/// The abandon scenario (G0 `ocdup` shape): W deleted row 1 in an earlier
/// statement; T holds a lock-only intent on the second arbiter key B. W's
/// attempt passes the pre-check (a lock-only intent does not block it),
/// inserts row 1 and entry A at `sa`, then must wait on B: it abandons
/// (rolls back to `sa`) and parks. T commits; W restarts and inserts.
fn run_abandon_scenario() -> Abandon {
    let rig = Rig::new();
    let parkers = rig.parkers();
    rig.preload(&t_key("1"), &u64v(5));
    let t = rig.txn(Isolation::ReadCommitted);
    let tseq = ok(t.next_seq());
    ok(rig.core.row_op(
        &t,
        &u_key("B"),
        None,
        nucleus_txn::write::RowOp::Lock(nucleus_txn::RowLockMode::NoKeyUpdate),
        StmtCtx::new(rig.core.visible_ts(), tseq, tseq),
        &mut c_t2c_support::Fixed(nucleus_txn::write::RowOp::Lock(
            nucleus_txn::RowLockMode::NoKeyUpdate,
        )),
    ));
    assert_count_exact(&rig.core, &t);

    let w = rig.txn(Isolation::ReadCommitted);
    let early = ok(w.next_seq());
    rig.delete_in(&w, early, &t_key("1"));
    assert_count_exact(&rig.core, &w);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let sas = Mutex::new(Vec::new());
    let core = &rig.core;
    let before = parkers.parks();

    let (outcome, parked) = std::thread::scope(|scope| {
        let h = scope.spawn(|| {
            let mut f = incr;
            core.insert_on_conflict(
                &w,
                StmtCtx::new(s, seq0, seq0),
                proposed("1", 7, &["A", "B"]),
                &t_key_of,
                &mut |sa| {
                    sas.lock().expect("sas").push(sa);
                    // The SQL layer's AFTER event for this attempt.
                    w.queue_event(sa, sa.to_be_bytes().to_vec());
                },
                OnConflictAction::DoUpdate(&mut f),
            )
        });
        parkers.wait_parked_unless(before, &|| h.is_finished());
        // W is parked after the abandon.
        assert_count_exact(core, &w);
        let parked = (
            rig.intent(&u_key("A")),
            rig.intent(&t_key("1")),
            w.take_events(),
        );
        rig.commit(t);
        let outcome = match h.join() {
            Ok(r) => ok(r),
            Err(e) => std::panic::resume_unwind(e),
        };
        (outcome, parked)
    });
    assert_count_exact(&rig.core, &w);
    let final_events = w.take_events();
    let final_t_intent = rig.intent(&t_key("1"));
    rig.commit(w);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("1")), Some(u64v(7)));
    assert_eq!(rig.read_latest(&u_key("A")), Some(b"1".to_vec()));
    assert_eq!(rig.read_latest(&u_key("B")), Some(b"1".to_vec()));
    Abandon {
        parked_a_intent: parked.0,
        parked_t_intent: parked.1,
        parked_events: parked.2,
        sas: sas.into_inner().expect("sas"),
        early,
        seq0,
        outcome,
        final_events,
        final_t_intent,
    }
}

/// Seed 54: the abandon rolls back to `sa` — row 1's intent keeps W's
/// earlier `Delete` layer (seq < sa) and loses only the attempt's layer.
/// Mutant killed: the abandon removes whole intents (W's earlier delete of
/// row 1 is lost; the intent is gone while W is parked).
#[test]
fn seed54_abandon_rolls_back_to_sa() {
    let a = run_abandon_scenario();
    let t = a.parked_t_intent.expect("row 1 keeps W's earlier layer");
    assert_eq!(t.layers.len(), 1, "{t:?}");
    assert_eq!(t.layers[0].seq, a.early);
    assert_eq!(t.layers[0].data, LayerData::Delete { moved: false });
    assert_eq!(a.outcome.result, OnConflictResult::Inserted);
    assert_eq!(a.outcome.restarts, 1);
    let t = a.final_t_intent.expect("row 1 intent");
    assert_eq!(t.layers.len(), 2, "{t:?}");
    assert_eq!(
        t.layers[1].seq, a.sas[1],
        "the final attempt placed at its sa"
    );
    assert_eq!(
        t.layers[1].data_seq, a.seq0,
        "data_seq = seq0 (es_output_cid)"
    );
}

/// Seed 56: the attempt's writes place at `sa`, so the abandon leaves no
/// new arbiter intent on A.
/// Mutant killed: attempt writes placed at `seq0` (A's intent survives
/// `ROLLBACK TO sa`).
#[test]
fn seed56_attempt_writes_at_sa() {
    let a = run_abandon_scenario();
    assert!(
        a.parked_a_intent.is_none(),
        "the abandoned attempt left an arbiter intent: {:?}",
        a.parked_a_intent
    );
    assert_eq!(a.sas.len(), 2);
    assert!(a.sas[0] > a.seq0 && a.sas[1] > a.sas[0]);
}

/// Seed 64: `on_attempt` queues an event tagged with the `sa` it gets; the
/// abandoned attempt's event is gone, the final attempt's survives.
/// Mutants killed: `on_attempt` passed `seq0` (the event survives the
/// abandon); the abandon does not discard events.
#[test]
fn seed64_attempt_events_tagged_sa() {
    let a = run_abandon_scenario();
    assert!(
        a.parked_events.is_empty(),
        "the abandoned attempt's event survived: {:?}",
        a.parked_events
    );
    assert_eq!(
        a.final_events,
        vec![(a.sas[1], a.sas[1].to_be_bytes().to_vec())]
    );
}

/// DO UPDATE on a row this statement inserted → 21000, raised before the
/// lock (no layer added, the closure never runs). A later statement's DO
/// UPDATE on the same row updates it from the own intent's data.
/// Mutant killed: no 21000 check before the arbiter lock (a lock layer is
/// added at `sa` before the update path raises).
#[test]
fn do_update_on_row_inserted_by_this_statement_is_21000() {
    let rig = Rig::new();
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let out = ok(upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq0, seq0),
        proposed("1", 3, &["K"]),
        OnConflictAction::DoNothing,
    ));
    assert_eq!(out.result, OnConflictResult::Inserted);
    assert_count_exact(&rig.core, &w);
    let before = rig.intent(&t_key("1"));
    let mut called = false;
    let mut f = |v: &[u8]| {
        called = true;
        incr(v)
    };
    let r = upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq0, seq0),
        proposed("2", 0, &["K"]),
        OnConflictAction::DoUpdate(&mut f),
    );
    assert_eq!(r, Err(TxnError::CardinalityViolation));
    assert!(!called);
    assert_eq!(
        rig.intent(&t_key("1")),
        before,
        "no layer added before 21000"
    );
    assert_count_exact(&rig.core, &w);

    // Next statement: the own row is updated from its own data (3 → 4).
    let seq1 = ok(w.next_seq());
    let mut f = incr;
    let out = ok(upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq1, seq1),
        proposed("2", 0, &["K"]),
        OnConflictAction::DoUpdate(&mut f),
    ));
    assert_eq!(out.result, OnConflictResult::Updated);
    assert_count_exact(&rig.core, &w);
    rig.commit(w);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("1")), Some(u64v(4)));
    assert_eq!(rig.read_latest(&t_key("2")), None);
}

/// RR/SER: `r`'s newest committed version newer than `S` → 40001, for DO
/// NOTHING too (`ExecCheckTupleVisible`). RC does nothing / updates.
/// Mutant killed: no RR/SER check in §5.3.1(3) (DO NOTHING returns
/// `Nothing`, DO UPDATE locks at `v_r` and updates).
#[test]
fn rr_ser_conflict_with_r_newer_than_s_is_40001() {
    for iso in [Isolation::RepeatableRead, Isolation::Serializable] {
        let rig = Rig::new();
        preload_row(&rig, "1", 0, "K");
        let w = rig.txn(iso);
        let snap = rig.core.registry.take_snapshot();
        let s = snap.ts();
        rig.update(&t_key("1"), &u64v(1));
        let seq0 = ok(w.next_seq());
        let r = upsert(
            &rig,
            &w,
            StmtCtx::new(s, seq0, seq0),
            proposed("2", 0, &["K"]),
            OnConflictAction::DoNothing,
        );
        assert_eq!(r, Err(TxnError::SerializationFailure), "{iso:?} DO NOTHING");
        assert_count_exact(&rig.core, &w);
        let seq1 = ok(w.next_seq());
        let mut f = incr;
        let r = upsert(
            &rig,
            &w,
            StmtCtx::new(s, seq1, seq1),
            proposed("2", 0, &["K"]),
            OnConflictAction::DoUpdate(&mut f),
        );
        assert_eq!(r, Err(TxnError::SerializationFailure), "{iso:?} DO UPDATE");
        assert_count_exact(&rig.core, &w);
        ok(rig.core.abort(w));

        // RC at the same S: DO NOTHING skips.
        let rc = rig.txn(Isolation::ReadCommitted);
        let q = ok(rc.next_seq());
        let out = ok(upsert(
            &rig,
            &rc,
            StmtCtx::new(s, q, q),
            proposed("2", 0, &["K"]),
            OnConflictAction::DoNothing,
        ));
        assert_eq!(out.result, OnConflictResult::Nothing);
        assert_count_exact(&rig.core, &rc);
        ok(rig.core.abort(rc));
    }
}
