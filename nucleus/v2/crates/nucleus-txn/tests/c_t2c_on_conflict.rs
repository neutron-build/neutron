//! C-T2c: the §5.3.1 ON CONFLICT arbiter protocol, driven by hand (the
//! G0 `ocdup` / `ocnothing` shapes rebuilt with steps and callbacks). Rows
//! live at `/t/r/{pk}` with a u64 value; the arbiter entry `/u/a/{k}` names
//! its row by `pk`. Each test names the mutant it kills; I-COUNT is checked
//! after every step.

mod c_t2c_support;

use std::cell::{Cell, RefCell};
use std::sync::Mutex;

use c_t2c_support::{as_u64, assert_count_exact, ok, u64v, Fixed, Rig};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::write::{
    IndexEntry, OnConflictAction, OnConflictOutcome, OnConflictResult, ProposedRow, RowOp,
    RowOutcome, StmtCtx, UniqueRule,
};
use nucleus_txn::{LayerData, RowLockMode, TxnError};

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

// ---- C-T2c rework ---------------------------------------------------------

/// The key-moving writer (rework item 1): moves the arbiter key `K` off row
/// 1 onto row 9 — inserts `/t/r/9`, deletes the old entry, re-creates the
/// entry naming row 9. Row 1's own `/t/` key is untouched, which is exactly
/// what hides the move from the arbiter lock's newer-version check (the lock
/// at `base = v_r.ts` sees `v_r` as still-newest). Runs on W's thread inside
/// the `t_key_of` window, so it only touches `core`.
fn move_key_to_9(core: &nucleus_txn::boot::Core<nucleus_kv::MemKv>, t: &Txn, ts: u32) {
    let s = core.visible_ts();
    ok(core.insert_key(
        t,
        &t_key("9"),
        None,
        u64v(5),
        StmtCtx::new(s, ts, ts),
        UniqueRule::Unique { same_row: None },
    ));
    ok(core.row_op(
        t,
        &u_key("K"),
        None,
        RowOp::Delete,
        StmtCtx::new(s, ts, ts),
        &mut Fixed(RowOp::Delete),
    ));
    ok(core.insert_key(
        t,
        &u_key("K"),
        None,
        b"9".to_vec(),
        StmtCtx::new(s, ts, ts),
        UniqueRule::Unique {
            same_row: Some(b"9".to_vec()),
        },
    ));
    assert_count_exact(core, t);
}

/// Rework item 1 (applied stages): between the pre-check and the DO UPDATE
/// apply, another txn moves the arbiter key onto another row and commits —
/// once committed-but-unresolved (`resolve = false`) and once fully
/// resolved (`resolve = true`). The outcome must touch only the true owner:
/// the update lands on `/t/r/9`, row 1 keeps its value, exactly one restart
/// follows the observed change of ownership.
/// Mutant killed: no ownership re-check on the arbiter entry at the
/// lock/apply point (DO UPDATE applies to `/t/r/1` with `restarts == 0`
/// although the entry names `/t/r/9`).
#[test]
fn rework1_key_move_in_window_updates_true_owner() {
    for resolve in [false, true] {
        let rig = Rig::new();
        preload_row(&rig, "1", 5, "K");
        let w = rig.txn(Isolation::ReadCommitted);
        let seq0 = ok(w.next_seq());
        let s = rig.core.visible_ts();
        let mover = RefCell::new(Some(rig.txn(Isolation::ReadCommitted)));
        let race = |payload: &[u8]| -> Vec<u8> {
            if let Some(t) = mover.borrow_mut().take() {
                let ts = ok(t.next_seq());
                move_key_to_9(&rig.core, &t, ts);
                let c = rig.commit(t);
                if resolve {
                    rig.resolve();
                }
                assert!(c > s, "the mover committed above W's snapshot");
            }
            t_key_of(payload)
        };
        let mut f = incr;
        let out = ok(rig.core.insert_on_conflict(
            &w,
            StmtCtx::new(s, seq0, seq0),
            proposed("2", 0, &["K"]),
            &race,
            &mut |_| {},
            OnConflictAction::DoUpdate(&mut f),
        ));
        assert_eq!(
            out.result,
            OnConflictResult::Updated,
            "resolve={resolve}: the true owner got the update"
        );
        assert_eq!(
            out.restarts, 1,
            "resolve={resolve}: the ownership change restarted the attempt"
        );
        assert_count_exact(&rig.core, &w);
        rig.commit(w);
        rig.resolve();
        assert_eq!(
            rig.read_latest(&t_key("1")),
            Some(u64v(5)),
            "resolve={resolve}: the row that lost the key is untouched"
        );
        assert_eq!(
            rig.read_latest(&t_key("9")),
            Some(u64v(6)),
            "resolve={resolve}: the update landed on the true owner"
        );
        assert_eq!(rig.read_latest(&u_key("K")), Some(b"9".to_vec()));
        assert_eq!(rig.read_latest(&t_key("2")), None);
    }
}

/// Rework item 1 (committed-but-unapplied stage): the key-moving writer is
/// still pending when W reaches the apply point. The pre-check rule must
/// govern the arbiter entry there: W waits on the mover (after abandoning
/// the attempt, so no lock on the stale row survives the park) and the
/// restart updates the true owner.
/// Mutant killed: the lock/apply point ignores a foreign intent on the
/// arbiter entry (W applies to `/t/r/1` immediately and never parks).
#[test]
fn rework1_pending_key_move_waits_then_updates_true_owner() {
    let rig = Rig::new();
    let parkers = rig.parkers();
    preload_row(&rig, "1", 5, "K");
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let mover = Mutex::new(Some(rig.txn(Isolation::ReadCommitted)));
    let handoff = Mutex::new(None::<Txn>);
    let core = &rig.core;
    let before = parkers.parks();

    let out = std::thread::scope(|scope| {
        let h = scope.spawn(|| {
            let race = |payload: &[u8]| -> Vec<u8> {
                if let Some(t) = mover.lock().expect("mover").take() {
                    let ts = ok(t.next_seq());
                    move_key_to_9(core, &t, ts);
                    *handoff.lock().expect("handoff") = Some(t);
                }
                t_key_of(payload)
            };
            let mut f = incr;
            core.insert_on_conflict(
                &w,
                StmtCtx::new(s, seq0, seq0),
                proposed("2", 0, &["K"]),
                &race,
                &mut |_| {},
                OnConflictAction::DoUpdate(&mut f),
            )
        });
        parkers.wait_parked_unless(before, &|| h.is_finished());
        let parked = parkers.parks();
        assert_count_exact(core, &w);
        let stale_lock = rig.intent(&t_key("1"));
        let entry_intent = rig.intent(&u_key("K"));
        let mover = handoff.lock().expect("handoff").take().expect("mover ran");
        assert_eq!(
            entry_intent.map(|i| i.txn),
            Some(mover.id),
            "the mover still owns the entry intent while W waits"
        );
        rig.commit(mover);
        let out = match h.join() {
            Ok(r) => ok(r),
            Err(e) => std::panic::resume_unwind(e),
        };
        (parked, stale_lock, out)
    });
    assert!(out.0 > before, "W waited on the pending key mover");
    assert!(
        out.1.is_none_or(|i| i.txn != w.id),
        "the abandoned attempt left no lock on the stale row"
    );
    assert_eq!(out.2.result, OnConflictResult::Updated);
    assert_eq!(out.2.restarts, 1);
    assert_count_exact(&rig.core, &w);
    rig.commit(w);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("1")), Some(u64v(5)));
    assert_eq!(rig.read_latest(&t_key("9")), Some(u64v(6)));
    assert_eq!(rig.read_latest(&u_key("K")), Some(b"9".to_vec()));
    assert_eq!(rig.read_latest(&t_key("2")), None);
}

/// Commits a writer that updates `key` to `value` above `W`'s snapshot with
/// application stalled: the commit is visible but the resolver has not run,
/// so the intent is still in the KV (§3.2's window from the writer side).
fn commit_stalled_write(rig: &Rig, key: &[u8], value: u64) {
    let t = rig.txn(Isolation::ReadCommitted);
    let ts = ok(t.next_seq());
    let op = RowOp::Update {
        value: u64v(value),
        key_cols_changed: false,
    };
    ok(rig.core.row_op(
        &t,
        key,
        None,
        op.clone(),
        StmtCtx::new(rig.core.visible_ts(), ts, ts),
        &mut Fixed(op),
    ));
    assert_count_exact(&rig.core, &t);
    rig.commit(t);
}

/// Rework item 2: under RR/SER, `r`'s newest committed version newer than
/// `S` is a 40001 for DO NOTHING and DO UPDATE alike, and "committed"
/// includes committed-not-yet-applied. Stalled application (the intent is
/// still in the KV), then drained and resolved: the retry at the same `S` is
/// still a 40001 once the version is visible.
/// Mutant killed: the (3) check reads only applied versions (the stalled
/// writer is invisible to it: DO NOTHING returns `Nothing`).
#[test]
fn rework2_rr_ser_do_nothing_counts_unapplied_writer() {
    for iso in [Isolation::RepeatableRead, Isolation::Serializable] {
        let rig = Rig::new();
        preload_row(&rig, "1", 0, "K");
        let w = rig.txn(iso);
        let snap = rig.core.registry.take_snapshot();
        let s = snap.ts();
        commit_stalled_write(&rig, &t_key("1"), 1);

        let seq0 = ok(w.next_seq());
        let r = upsert(
            &rig,
            &w,
            StmtCtx::new(s, seq0, seq0),
            proposed("2", 0, &["K"]),
            OnConflictAction::DoNothing,
        );
        assert_eq!(r, Err(TxnError::SerializationFailure), "{iso:?} stalled");
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

        rig.resolve();
        let seq2 = ok(w.next_seq());
        let r = upsert(
            &rig,
            &w,
            StmtCtx::new(s, seq2, seq2),
            proposed("2", 0, &["K"]),
            OnConflictAction::DoNothing,
        );
        assert_eq!(
            r,
            Err(TxnError::SerializationFailure),
            "{iso:?} retry after the version is visible"
        );
        assert_count_exact(&rig.core, &w);
        ok(rig.core.abort(w));
    }
}

/// Rework item 2, RC: the same stalled scenario decides on applied state —
/// the conflict point resolves the unapplied writer inline, so the update is
/// computed from the newest version and completes with no restart at all.
/// Mutant killed: no pre-check rule on `r` at the conflict point (the stale
/// `v_r` forces a restart through the arbiter lock: `restarts == 1`).
#[test]
fn rework2_rc_do_update_decides_on_applied_state() {
    let rig = Rig::new();
    preload_row(&rig, "1", 0, "K");
    let w = rig.txn(Isolation::ReadCommitted);
    let snap = rig.core.registry.take_snapshot();
    let s = snap.ts();
    commit_stalled_write(&rig, &t_key("1"), 1);
    let seq0 = ok(w.next_seq());
    let mut f = incr;
    let out = ok(upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq0, seq0),
        proposed("2", 0, &["K"]),
        OnConflictAction::DoUpdate(&mut f),
    ));
    assert_eq!(out.result, OnConflictResult::Updated);
    assert_eq!(
        out.restarts, 0,
        "the pre-check rule on r absorbed the unapplied writer inline"
    );
    assert_count_exact(&rig.core, &w);
    rig.commit(w);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("1")), Some(u64v(2)));
    assert_eq!(rig.read_latest(&t_key("2")), None);
}

/// Rework item 2, RC, pending stage: a pending writer of `r` also forces the
/// wait-and-restart (the pre-check rule governs `r` for the (3) check, DO
/// NOTHING included) — W parks before deciding and restarts exactly once.
/// Mutant killed: the (3) check decides on the stale applied-only `v_r`
/// without waiting (DO NOTHING returns at once, without parking).
#[test]
fn rework2_rc_do_nothing_waits_on_pending_writer_of_r() {
    let rig = Rig::new();
    let parkers = rig.parkers();
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
    assert_count_exact(&rig.core, &mover);

    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let core = &rig.core;
    let before = parkers.parks();
    let out = std::thread::scope(|scope| {
        let h = scope.spawn(|| {
            core.insert_on_conflict(
                &w,
                StmtCtx::new(s, seq0, seq0),
                proposed("2", 0, &["K"]),
                &t_key_of,
                &mut |_| {},
                OnConflictAction::DoNothing,
            )
        });
        parkers.wait_parked_unless(before, &|| h.is_finished());
        let parked = parkers.parks();
        rig.commit(mover);
        let out = match h.join() {
            Ok(r) => ok(r),
            Err(e) => std::panic::resume_unwind(e),
        };
        (parked, out)
    });
    assert!(
        out.0 > before,
        "DO NOTHING waited on the pending writer of r before deciding"
    );
    assert_eq!(out.1.result, OnConflictResult::Nothing);
    assert_eq!(out.1.restarts, 1, "the wait was followed by one restart");
    assert_count_exact(&rig.core, &w);
    rig.resolve();
    ok(rig.core.abort(w));
    assert_eq!(rig.read_latest(&t_key("1")), Some(u64v(1)));
}

/// Rework item 2, RC, DO UPDATE over a pending writer of `r`: W waits, the
/// restart re-decides on the writer's applied state, and the update is
/// computed from the newest version (0 → 1 → 2), not from `S`.
#[test]
fn rework2_rc_do_update_pending_writer_restarts_and_completes() {
    let rig = Rig::new();
    let parkers = rig.parkers();
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
    assert_count_exact(&rig.core, &mover);

    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let core = &rig.core;
    let before = parkers.parks();
    let out = std::thread::scope(|scope| {
        let h = scope.spawn(|| {
            let mut f = incr;
            core.insert_on_conflict(
                &w,
                StmtCtx::new(s, seq0, seq0),
                proposed("2", 0, &["K"]),
                &t_key_of,
                &mut |_| {},
                OnConflictAction::DoUpdate(&mut f),
            )
        });
        parkers.wait_parked_unless(before, &|| h.is_finished());
        let parked = parkers.parks();
        rig.commit(mover);
        let out = match h.join() {
            Ok(r) => ok(r),
            Err(e) => std::panic::resume_unwind(e),
        };
        (parked, out)
    });
    assert!(
        out.0 > before,
        "DO UPDATE waited on the pending writer of r"
    );
    assert_eq!(out.1.result, OnConflictResult::Updated);
    assert_eq!(out.1.restarts, 1);
    assert_count_exact(&rig.core, &w);
    rig.commit(w);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("1")), Some(u64v(2)));
    assert_eq!(rig.read_latest(&t_key("2")), None);
}

/// Item 4: a false `DO UPDATE ... WHERE` leaves the conflict lock on `r`
/// (§5.3.1(3)): the lock-only layer stays on W's intent, and a concurrent
/// writer of `r` parks on it until W ends.
/// Mutant killed: WhereFalse drops the lock (the concurrent writer completes
/// without ever parking).
#[test]
fn where_false_keeps_the_lock() {
    let rig = Rig::new();
    let parkers = rig.parkers();
    preload_row(&rig, "1", 9, "K");
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    let s = rig.core.visible_ts();
    let mut never = |_: &[u8]| None;
    let out = ok(upsert(
        &rig,
        &w,
        StmtCtx::new(s, seq0, seq0),
        proposed("2", 0, &["K"]),
        OnConflictAction::DoUpdate(&mut never),
    ));
    assert_eq!(out.result, OnConflictResult::WhereFalse);
    assert_eq!(out.restarts, 0);
    assert_count_exact(&rig.core, &w);
    let i = rig.intent(&t_key("1")).expect("the conflict lock stays");
    assert_eq!(i.txn, w.id);
    assert_eq!(i.layers.len(), 1);
    assert_eq!(i.layers[0].data, LayerData::Absent);
    assert_eq!(i.layers[0].lock, RowLockMode::NoKeyUpdate);

    let t = rig.txn(Isolation::ReadCommitted);
    let tseq = ok(t.next_seq());
    let op = RowOp::Update {
        value: u64v(10),
        key_cols_changed: false,
    };
    let core = &rig.core;
    let before = parkers.parks();
    let r = std::thread::scope(|scope| {
        let h = scope.spawn(|| {
            core.row_op(
                &t,
                &t_key("1"),
                None,
                op.clone(),
                StmtCtx::new(core.visible_ts(), tseq, tseq),
                &mut Fixed(op),
            )
        });
        parkers.wait_parked_unless(before, &|| h.is_finished());
        let parked = parkers.parks();
        rig.commit(w);
        let r = match h.join() {
            Ok(r) => r,
            Err(e) => std::panic::resume_unwind(e),
        };
        (parked, r)
    });
    assert!(
        r.0 > before,
        "the concurrent writer parked on the WhereFalse lock"
    );
    assert_eq!(r.1, Ok(RowOutcome::Applied));
    assert_count_exact(&rig.core, &t);
    rig.commit(t);
    rig.resolve();
    assert_eq!(rig.read_latest(&t_key("1")), Some(u64v(10)));
    assert_eq!(rig.read_latest(&u_key("K")), Some(b"1".to_vec()));
    assert_eq!(rig.read_latest(&t_key("2")), None);
}
