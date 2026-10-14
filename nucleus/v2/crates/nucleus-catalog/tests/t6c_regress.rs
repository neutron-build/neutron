//! C-T6c1b's deterministic regressions: hand-scheduled tests for the two
//! engine mutants the soak cannot pin deterministically, plus non-vacuity
//! proofs of the new differential checks on synthetic histories.
//!
//! - **E3** — `catalog_alter_rel_def` locking RowExclusive instead of
//!   AccessExclusive (nucleus-txn): the ALTER's AccessExclusive is what
//!   keeps writers out of a relation whose definition is changing. The
//!   test drives `catalog_alter_rel_def` directly (the mutant's site),
//!   parks the DDL thread on a channel handoff **right after the call
//!   returns** (the lock is held to the txn's end), and proves three
//!   things about
//!   a concurrent insert: it *blocks* (a wait-for edge appears while the
//!   insert is unfinished — a deterministic observation, never a
//!   wall-clock one), it proceeds only after the DDL commits, and it
//!   then encodes at the **new** definition (its handle reports the
//!   bumped `def_version`, and the row it wrote decodes with an explicit
//!   NULL — not the ADD COLUMN default — in the new column, which only
//!   a row written after the bump can do).
//! - **E4** — writes encoding at `version - 1` (the row header's
//!   `schema_version`, nucleus-codec): single-threaded ADD COLUMN (with
//!   a default) → INSERT → decode. §3's lazy-upgrade rule writes every
//!   row at the current version with `natts = ncols`, so the stored
//!   row's header must name the post-DDL version and the new column
//!   must read back the written NULL, not the `missing` default an
//!   old-version row decodes through.
//!
//! No verdict reads a clock: the bounded [`spin_until`] helper only
//! turns "the condition never became true" into a failure (a lost wakeup
//! would otherwise hang the test); what it waits *for* — a wait-for
//! edge, a flag, a commit ts — is scheduling state.
//!
//! The synthetic histories at the bottom prove C-T6c1b's oracle is not
//! vacuous: a legal small history checks clean, and each new violation
//! class — a duplicate insert, an affected write with no row, an RR "no
//! row" over a live row, a wrong update effect, a SET NOT NULL success
//! over a NULL, and a write-skew cycle among committed SERIALIZABLE
//! txns — is flagged when hand-built into a history the engine never
//! produced. The follow-up pins refine both directions: the absent-read
//! rw refinement stays legal on a finally-dead key yet flags a phantom
//! write-skew (c1b FU-A), an in-window RR write is flagged while its RC
//! twin stays clean (FU-B), and the RC re-insert carve-out keeps a
//! legal EPQ-skip history clean (FU-D).

mod diff;
// diff/crash.rs (pulled in by diff/mod.rs) drives the t6b crash rig.
mod t6b_support;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use diff::check::check;
use diff::runner::{t_schema, Rig, DDL_SESSION};
use diff::{Fate, History, Level, OpDetail, OpKind, OpOut, OpRec, RowVals, Run, TxnRec};

use nucleus_catalog::table_lo;
use nucleus_catalog::Stmt;
use nucleus_codec::row::{row_header, Datum};
use nucleus_codec::{encode_key, KeyColumn, KeyType, Value};
use nucleus_txn::catalog::Ddl;
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;

/// The pk of the rows the regressions insert (outside the soak's 0..8
/// domain, so the rig's setup rows never collide with them).
const FRESH_PK: i64 = 100;

/// Turns "this condition never became true" into a failure. The bound is
/// a liveness backstop (a hang would otherwise stall the test forever);
/// the verdict itself is the condition, never the elapsed time.
fn spin_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        if Instant::now() > deadline {
            panic!("t6c_regress: timed out waiting for {what}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

// ----- E3: an ALTER's AccessExclusive excludes writers -----------------------

/// `catalog_alter_rel_def` takes AccessExclusive, so a writer arriving
/// while the ALTER's txn is parked right after the call must block until
/// it commits, then encode at the new definition. The E3 mutant (which
/// locks RowExclusive instead) lets the writer straight through and is
/// caught twice: no wait-for edge ever appears, and the insert encodes
/// at the stale definition.
#[test]
fn t6c_regress_e3_alter_rel_def_excludes_writers() {
    let mut rig = Rig::new(5);
    rig.setup();
    let rig = &rig;

    let altered = Arc::new(AtomicBool::new(false));
    let inserted = Arc::new(AtomicBool::new(false));
    let insert_version = Arc::new(AtomicU32::new(0));
    let ddl_ts = Arc::new(Mutex::new(None));

    std::thread::scope(|sc| {
        // The DDL thread parks here, the def rewritten and the lock
        // held; the orchestrating thread releases it. A channel, not a
        // Barrier (c1b FU-E) — and the sender lives **inside** the
        // scope, so a panic anywhere below unwinds it before the scope
        // joins: the recv returns, the DDL thread finishes, and the
        // test fails cleanly instead of wedging the runner on a join
        // that can never happen while the DDL thread waits forever.
        let (release, park) = mpsc::channel::<()>();
        sc.spawn({
            let (altered, park, ddl_ts) = (Arc::clone(&altered), park, Arc::clone(&ddl_ts));
            move || {
                let txn = rig.db.core.begin(Isolation::ReadCommitted);
                let guard = rig.db.core.registry.take_snapshot();
                let st = Stmt::new(&txn, guard.ts()).expect("the DDL statement");
                // ADD COLUMN c1 int8 DEFAULT 77, written straight through
                // the mutant's site. The schema is built from the
                // fixture's v1 definition — the rig is otherwise idle.
                let next = t_schema()
                    .add_column("c1", KeyType::Int8, false, Some(Value::Int8(77)))
                    .expect("the bumped definition");
                rig.db
                    .catalog
                    .catalog_alter_rel_def(
                        &Ddl::new(&rig.db.core, st.txn(), st.ctx()),
                        rig.t_oid,
                        &next.encode().expect("the definition encodes"),
                    )
                    .expect("the alter");
                // The call returned: the AccessExclusive is held to this
                // txn's end. Park until released (a dropped sender on a
                // failure upstream releases too, so this thread always
                // finishes and the scope can join).
                altered.store(true, Ordering::SeqCst);
                let _ = park.recv();
                drop(guard);
                let ts = rig
                    .db
                    .core
                    .commit(txn, SyncCommit::On)
                    .expect("the DDL commit");
                *ddl_ts.lock().expect("the ddl ts slot") = Some(ts.0);
            }
        });
        // Wait until the lock is genuinely held, then start the writer.
        spin_until("the alter to hold its lock", || {
            altered.load(Ordering::SeqCst)
        });
        sc.spawn({
            let (inserted, insert_version) = (Arc::clone(&inserted), Arc::clone(&insert_version));
            move || {
                let txn = rig.db.core.begin(Isolation::ReadCommitted);
                let guard = rig.db.core.registry.take_snapshot();
                let st = Stmt::new(&txn, guard.ts()).expect("the insert statement");
                // RowExclusive conflicts with the held AccessExclusive:
                // this open parks until the DDL commits.
                let h = rig
                    .db
                    .open_table(&st, rig.t_oid, RelLockMode::RowExclusive)
                    .expect("the table opens once the DDL commits");
                insert_version.store(h.def_version, Ordering::SeqCst);
                let mut row: Vec<Option<Datum>> = Vec::new();
                for col in h.schema.live_columns() {
                    row.push(match col.name.as_str() {
                        "pk" => Some(Datum::Inline(Value::Int8(FRESH_PK))),
                        "a" => Some(Datum::Inline(Value::Int8(4242))),
                        "b" => Some(Datum::Inline(Value::Text("e3".into()))),
                        // A column a DDL added: the writer names no value
                        // for it, so it is NULL (§3 lazy upgrade).
                        _ => None,
                    });
                }
                rig.db
                    .insert_with(&st, &h, &row)
                    .expect("the insert at the new definition");
                drop(guard);
                rig.db
                    .core
                    .commit(txn, SyncCommit::On)
                    .expect("the insert commit");
                inserted.store(true, Ordering::SeqCst);
            }
        });

        // Deterministic proof the insert is blocked: it holds a wait-for
        // edge (it is parked in §6, not merely slow) and has not
        // finished — it cannot, the DDL has not committed.
        spin_until("the insert to park on the ALTER", || {
            !rig.db.core.wait_edges().is_empty()
        });
        assert!(
            !inserted.load(Ordering::SeqCst),
            "the insert finished while the ALTER's txn still held its lock"
        );

        // Release the DDL: it commits, and only then may the insert go
        // through.
        release
            .send(())
            .expect("the parked DDL thread holds the receiver");
        spin_until("the DDL commit ts", || {
            ddl_ts.lock().expect("the ddl ts slot").is_some()
        });
        spin_until("the insert to commit", || inserted.load(Ordering::SeqCst));
        assert_eq!(
            insert_version.load(Ordering::SeqCst),
            2,
            "the insert must encode at the post-DDL definition (version 2)"
        );
    });

    // The inserted row decodes with an explicit NULL in c1 — a row
    // written at version 2. Had it encoded at the stale definition, c1
    // would decode through the ADD COLUMN default (77) instead.
    let (c1, def_version) = {
        let txn = rig.db.core.begin(Isolation::ReadCommitted);
        let guard = rig.db.core.registry.take_snapshot();
        let st = Stmt::new(&txn, guard.ts()).expect("the read-back statement");
        let h = rig
            .db
            .open_table(&st, rig.t_oid, RelLockMode::AccessShare)
            .expect("the read-back handle");
        let c1_pos = h
            .schema
            .live_columns()
            .take_while(|c| c.name != "c1")
            .count();
        let row = rig
            .db
            .get(&st, &h, &[Some(Value::Int8(FRESH_PK))])
            .expect("the read-back")
            .expect("the inserted row is visible");
        let out = (row.get(c1_pos).cloned(), h.def_version);
        drop(guard);
        rig.db
            .core
            .commit(txn, SyncCommit::On)
            .expect("the read-back commit");
        out
    };
    assert_eq!(def_version, 2, "the committed definition is version 2");
    assert_eq!(
        c1,
        Some(None),
        "the new column must decode as the written NULL, not the default"
    );
}

// ----- E4: writes encode at the current definition version -------------------

/// Single-threaded: ADD COLUMN c1 int8 DEFAULT 77 (the typed path), then
/// INSERT a row whose c1 is NULL, then decode. §3's lazy upgrade writes
/// every row at the definition the writer's handle reported, so the
/// stored header's `schema_version` is the post-DDL version (2), `natts`
/// counts every attnum ever allocated (4), and the new column reads the
/// written NULL — not the `missing` default a pre-DDL row decodes
/// through (which the setup row, written at version 1, still does).
#[test]
fn t6c_regress_e4_writes_encode_at_the_current_version() {
    let mut rig = Rig::new(5);
    rig.setup();
    let rig = &rig;

    // ADD COLUMN c1 int8 DEFAULT 77, its own committed RC txn.
    {
        let txn = rig.db.core.begin(Isolation::ReadCommitted);
        let guard = rig.db.core.registry.take_snapshot();
        let st = Stmt::new(&txn, guard.ts()).expect("the DDL statement");
        rig.db
            .add_column(
                &st,
                rig.t_oid,
                "c1",
                KeyType::Int8,
                false,
                Some(Value::Int8(77)),
            )
            .expect("ADD COLUMN");
        drop(guard);
        rig.db
            .core
            .commit(txn, SyncCommit::On)
            .expect("the DDL commit");
    }

    // INSERT pk 100 at the new definition (c1 explicit NULL).
    {
        let txn = rig.db.core.begin(Isolation::ReadCommitted);
        let guard = rig.db.core.registry.take_snapshot();
        let st = Stmt::new(&txn, guard.ts()).expect("the insert statement");
        let h = rig
            .db
            .open_table(&st, rig.t_oid, RelLockMode::RowExclusive)
            .expect("the insert handle");
        assert_eq!(h.def_version, 2, "the writer sees the bumped definition");
        let mut row: Vec<Option<Datum>> = Vec::new();
        for col in h.schema.live_columns() {
            row.push(match col.name.as_str() {
                "pk" => Some(Datum::Inline(Value::Int8(FRESH_PK))),
                "a" => Some(Datum::Inline(Value::Int8(4242))),
                "b" => Some(Datum::Inline(Value::Text("e4".into()))),
                _ => None,
            });
        }
        rig.db.insert_with(&st, &h, &row).expect("the insert");
        drop(guard);
        rig.db
            .core
            .commit(txn, SyncCommit::On)
            .expect("the insert commit");
    }

    // Decode both rows through a registered reader: the row written
    // before the ADD COLUMN reads the default; the row written after
    // reads its NULL. And the raw header of the new row names version 2.
    let (sid, ncols, old, new) = {
        let txn = rig.db.core.begin(Isolation::ReadCommitted);
        let guard = rig.db.core.registry.take_snapshot();
        let st = Stmt::new(&txn, guard.ts()).expect("the reader statement");
        let h = rig
            .db
            .open_table(&st, rig.t_oid, RelLockMode::AccessShare)
            .expect("the reader handle");
        let c1_pos = h
            .schema
            .live_columns()
            .take_while(|c| c.name != "c1")
            .count();
        let old = rig
            .db
            .get(&st, &h, &[Some(Value::Int8(0))])
            .expect("the setup row read")
            .expect("the setup row is visible");
        let new = rig
            .db
            .get(&st, &h, &[Some(Value::Int8(FRESH_PK))])
            .expect("the new row read")
            .expect("the new row is visible");
        let out = (
            h.storage_id,
            h.schema.columns.len(),
            old.get(c1_pos).cloned(),
            new.get(c1_pos).cloned(),
        );
        drop(guard);
        rig.db
            .core
            .commit(txn, SyncCommit::On)
            .expect("the reader commit");
        out
    };
    assert_eq!(
        old,
        Some(Some(Datum::Inline(Value::Int8(77)))),
        "a row written before the ADD COLUMN decodes the default (missing)"
    );
    assert_eq!(
        new,
        Some(None),
        "a row written after the ADD COLUMN decodes its written NULL"
    );

    // The raw row bytes: the §4 read of the pk key returns the payload,
    // and its header must name the current definition (§2/§3).
    {
        let txn = rig.db.core.begin(Isolation::ReadCommitted);
        let guard = rig.db.core.registry.take_snapshot();
        let st = Stmt::new(&txn, guard.ts()).expect("the raw statement");
        let cols = [KeyColumn::asc(KeyType::Int8)];
        let mut key = table_lo(sid).as_slice().to_vec();
        encode_key(&cols, &[Some(Value::Int8(FRESH_PK))], &mut key).expect("the pk key");
        let ctx = ReadCtx {
            txn: st.txn().id,
            snapshot: st.snapshot(),
            stmt_seq: st.seq0(),
        };
        let view = rig.db.core.open_view();
        let payload = read_key(&rig.db.core, &view, &key, &ctx, &mut NoSsi)
            .expect("the raw read")
            .expect("the row is committed");
        let header = row_header(&payload).expect("the row header");
        assert_eq!(
            header.schema_version, 2,
            "every write encodes at the current definition version (§3 lazy upgrade)"
        );
        assert_eq!(
            header.natts as usize, ncols,
            "natts counts every attnum ever allocated (§2)"
        );
        drop(view);
        drop(guard);
        rig.db
            .core
            .commit(txn, SyncCommit::On)
            .expect("the raw commit");
    }
}

// ----- the oracle can fail: synthetic histories ------------------------------

/// Builds an op record over `t`'s initial definition (v1, def_version 1,
/// storage 1).
fn op(kind: OpKind, detail: OpDetail, out: OpOut, snap: u64) -> OpRec {
    OpRec {
        kind,
        detail,
        out,
        snap,
        def_version: Some(1),
        sid: Some(1),
        start: 0,
        end: 0,
    }
}

/// One synthetic txn.
fn txn(uid: u32, session: u16, level: Level, snap: u64, fate: Fate, ops: Vec<OpRec>) -> TxnRec {
    TxnRec {
        uid,
        session,
        prog: uid,
        attempt: 0,
        level,
        txn_snap: (level != Level::ReadCommitted).then_some(snap),
        start: 0,
        end: 0,
        fate,
        ops,
    }
}

fn row3(pk: i64, a: i64, b: Option<&str>) -> RowVals {
    vec![
        (1, Some(Value::Int8(pk))),
        (2, Some(Value::Int8(a))),
        (3, b.map(|s| Value::Text(s.to_string()))),
    ]
}

fn run_of(history: History, final_t: Vec<(i64, RowVals)>) -> Run {
    Run {
        history,
        amcheck: Vec::new(),
        final_t: (1, final_t),
        final_u: (1, Vec::new()),
        leaked_intents: 0,
        leak_report: String::new(),
    }
}

/// The setup shape every synthetic history starts from: one committed RC
/// txn that inserted pk 1 (`a = 100`, `b` set) at ts 1, snapshot 0.
fn base_history() -> (History, RowVals) {
    let row = row3(1, 100, Some("x"));
    let history = History {
        txns: vec![txn(
            1,
            0,
            Level::ReadCommitted,
            0,
            Fate::Committed(1),
            vec![op(
                OpKind::InsertT,
                OpDetail::InsertT {
                    pk: 1,
                    row: row.clone(),
                },
                OpOut::Ok,
                0,
            )],
        )],
    };
    (history, row)
}

/// A legal small history checks clean — the guard against an over-eager
/// oracle (the new rules have teeth, so they must not bite a legal
/// history either).
#[test]
fn t6c_regress_synthetic_legal_history_is_clean() {
    let (history, row) = base_history();
    // A committed SER reader of the row at S=1 (after the insert
    // committed), then the final state matching the model.
    let history = History {
        txns: history
            .txns
            .into_iter()
            .chain(std::iter::once(txn(
                2,
                1,
                Level::Serializable,
                1,
                Fate::Committed(2),
                vec![op(
                    OpKind::Get,
                    OpDetail::Get { pk: 1 },
                    OpOut::Row(Some(row.clone())),
                    1,
                )],
            )))
            .collect(),
    };
    let run = run_of(history, vec![(1, row)]);
    let verdict = check(&run);
    assert!(verdict.is_clean(), "{}", verdict.report());
}

/// Insert finding a live pk in the model at its commit position ⇒
/// "engine accepted a duplicate".
#[test]
fn t6c_regress_flags_a_duplicate_insert() {
    let (history, row) = base_history();
    let dup = txn(
        2,
        1,
        Level::ReadCommitted,
        1,
        Fate::Committed(2),
        vec![op(
            OpKind::InsertT,
            OpDetail::InsertT {
                pk: 1,
                row: row3(1, 200, Some("dup")),
            },
            OpOut::Ok,
            1,
        )],
    );
    let history = History {
        txns: history
            .txns
            .into_iter()
            .chain(std::iter::once(dup))
            .collect(),
    };
    let run = run_of(history, vec![(1, row)]);
    let verdict = check(&run);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("engine accepted a duplicate")),
        "{}",
        verdict.report()
    );
}

/// An affected write with no row in the model at its commit position.
#[test]
fn t6c_regress_flags_a_write_over_no_row() {
    let (history, row) = base_history();
    let up = txn(
        2,
        1,
        Level::Serializable,
        1,
        Fate::Committed(2),
        vec![op(
            OpKind::UpdateSet,
            OpDetail::UpdateSet {
                pk: 42,
                attnum: 2,
                value: Some(Value::Int8(9)),
                new_row: Some(row3(42, 9, Some("x"))),
            },
            OpOut::Applied(true),
            1,
        )],
    );
    let history = History {
        txns: history
            .txns
            .into_iter()
            .chain(std::iter::once(up))
            .collect(),
    };
    let run = run_of(history, vec![(1, row)]);
    let verdict = check(&run);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("reported a row written but the model has no pk 42")),
        "{}",
        verdict.report()
    );
}

/// An RR "no row" over a row the model had at S plus own writes.
#[test]
fn t6c_regress_flags_a_missing_row_verdict() {
    let (history, row) = base_history();
    let del = txn(
        2,
        1,
        Level::RepeatableRead,
        1,
        Fate::Committed(2),
        vec![op(
            OpKind::DeleteT,
            OpDetail::DeleteT { pk: 1 },
            OpOut::Applied(false),
            1,
        )],
    );
    let history = History {
        txns: history
            .txns
            .into_iter()
            .chain(std::iter::once(del))
            .collect(),
    };
    let run = run_of(history, vec![(1, row)]);
    let verdict = check(&run);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("reported no row but the model has one at")),
        "{}",
        verdict.report()
    );
}

/// An update effect that differs from the recompute (latest committed
/// row plus own writes).
#[test]
fn t6c_regress_flags_a_wrong_update_effect() {
    let (history, _) = base_history();
    // The engine's read-back claims the base row's `b` was rewritten
    // too; the recompute keeps it.
    let claimed = vec![
        (1, Some(Value::Int8(1))),
        (2, Some(Value::Int8(555))),
        (3, Some(Value::Text("claimed".to_string()))),
    ];
    let up = txn(
        2,
        1,
        Level::ReadCommitted,
        1,
        Fate::Committed(2),
        vec![op(
            OpKind::UpdateSet,
            OpDetail::UpdateSet {
                pk: 1,
                attnum: 2,
                value: Some(Value::Int8(555)),
                new_row: Some(claimed),
            },
            OpOut::Applied(true),
            1,
        )],
    );
    let history = History {
        txns: history
            .txns
            .into_iter()
            .chain(std::iter::once(up))
            .collect(),
    };
    let run = run_of(history, vec![(1, row3(1, 555, Some("claimed")))]);
    let verdict = check(&run);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("differs from the recompute")),
        "{}",
        verdict.report()
    );
}

/// SET NOT NULL succeeding although a NULL sits in the column at its
/// commit position.
#[test]
fn t6c_regress_flags_set_not_null_over_a_null() {
    // pk 2 with a NULL `b` (attnum 3).
    let history = History {
        txns: vec![
            txn(
                1,
                0,
                Level::ReadCommitted,
                0,
                Fate::Committed(1),
                vec![op(
                    OpKind::InsertT,
                    OpDetail::InsertT {
                        pk: 2,
                        row: row3(2, 100, None),
                    },
                    OpOut::Ok,
                    0,
                )],
            ),
            txn(
                2,
                5,
                Level::ReadCommitted,
                1,
                Fate::Committed(2),
                vec![op(
                    OpKind::SetNotNull,
                    OpDetail::SetNotNull { attnum: 3 },
                    OpOut::Ok,
                    1,
                )],
            ),
        ],
    };
    let run = run_of(history, vec![(2, row3(2, 100, None))]);
    let verdict = check(&run);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("SET NOT NULL on attnum 3 succeeded but pk 2 is NULL")),
        "{}",
        verdict.report()
    );
}

/// The SERIALIZABLE oracle: a write-skew history (two committed SER txns,
/// each reading the row the other then overwrote — each read's key ends
/// with the *other* txn as its final writer, so both rw edges are
/// forced) is not serializable, and the checker must say so. The
/// serializable variant (the second txn reads the row the first wrote
/// and rewrites it — a chain, not a cycle) stays clean.
#[test]
fn t6c_regress_flags_a_ser_write_skew_cycle() {
    // setup (RC): pk 3 and pk 7 at ts 1.
    let setup = txn(
        1,
        0,
        Level::ReadCommitted,
        0,
        Fate::Committed(1),
        vec![
            op(
                OpKind::InsertT,
                OpDetail::InsertT {
                    pk: 3,
                    row: row3(3, 300, Some("x")),
                },
                OpOut::Ok,
                0,
            ),
            op(
                OpKind::InsertT,
                OpDetail::InsertT {
                    pk: 7,
                    row: row3(7, 700, Some("y")),
                },
                OpOut::Ok,
                0,
            ),
        ],
    );
    // One SER txn: read `read_pk` (the row the setup committed), then
    // update `write_pk`'s `a` to 9. The update's recorded effect is the
    // recompute of the base row at its commit position.
    let read_write = |uid: u32, read_pk: i64, write_pk: i64, ts: u64| {
        let base = if write_pk == 3 {
            row3(3, 300, Some("x"))
        } else {
            row3(7, 700, Some("y"))
        };
        let mut new_row = base.clone();
        new_row[1].1 = Some(Value::Int8(9));
        txn(
            uid,
            1,
            Level::Serializable,
            2,
            Fate::Committed(ts),
            vec![
                op(
                    OpKind::Get,
                    OpDetail::Get { pk: read_pk },
                    OpOut::Row(Some(base)),
                    2,
                ),
                op(
                    OpKind::UpdateSet,
                    OpDetail::UpdateSet {
                        pk: write_pk,
                        attnum: 2,
                        value: Some(Value::Int8(9)),
                        new_row: Some(new_row),
                    },
                    OpOut::Applied(true),
                    2,
                ),
            ],
        )
    };
    // Write skew: T2 reads pk 3 and overwrites pk 7; T3 reads pk 7 and
    // overwrites pk 3. Each key's final writer is the txn that did not
    // read it: rw edges T2 -> T3 and T3 -> T2.
    let skew = History {
        txns: vec![
            setup.clone(),
            read_write(2, 3, 7, 10),
            read_write(3, 7, 3, 11),
        ],
    };
    let final_rows = vec![(3, row3(3, 9, Some("x"))), (7, row3(7, 9, Some("y")))];
    let verdict = check(&run_of(skew, final_rows.clone()));
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("not serializable")),
        "{}",
        verdict.report()
    );

    // Serializable shape: the second txn reads the row the first wrote
    // and rewrites it — the only rw edge points forward (T2 -> T3).
    let chain = History {
        txns: vec![setup, read_write(2, 3, 7, 10), read_write(3, 7, 7, 11)],
    };
    let verdict = check(&run_of(chain, final_rows));
    assert!(
        !verdict
            .violations
            .iter()
            .any(|v| v.contains("not serializable")),
        "{}",
        verdict.report()
    );
}

// ----- c1b FU-A: the absent-read rw refinement, both directions ---------------

/// The refinement must not bite legal histories (kills M-ser-add-all,
/// the pre-refinement shape that added an rw edge for *every* absent
/// point read). I (RC) inserts pk5 @ts1; D (SER, snap 1) reads pk6
/// absent and deletes pk5 @ts2; R (SER, snap 0 — before the setup even
/// committed) reads pk5 absent and inserts pk6 @ts3. The serial order
/// I, D, R is legal: R's absent read of pk5 is over a **finally-dead**
/// key — D's tombstone can be ordered before the read — so it must add
/// no rw edge. The verdict is clean; the add-all shape instead reports
/// the false cycle T_D -> T_R -> T_D.
#[test]
fn t6c_regress_ser_absent_read_refinement_stays_legal() {
    let history = History {
        txns: vec![
            // I: RC insert pk5 @ts1.
            txn(
                1,
                0,
                Level::ReadCommitted,
                0,
                Fate::Committed(1),
                vec![op(
                    OpKind::InsertT,
                    OpDetail::InsertT {
                        pk: 5,
                        row: row3(5, 500, Some("i")),
                    },
                    OpOut::Ok,
                    0,
                )],
            ),
            // D: SER, snap 1 — Get pk6 absent, DeleteT pk5 applied @ts2.
            txn(
                2,
                1,
                Level::Serializable,
                1,
                Fate::Committed(2),
                vec![
                    op(OpKind::Get, OpDetail::Get { pk: 6 }, OpOut::Row(None), 1),
                    op(
                        OpKind::DeleteT,
                        OpDetail::DeleteT { pk: 5 },
                        OpOut::Applied(true),
                        1,
                    ),
                ],
            ),
            // R: SER, snap 0 — Get pk5 absent, InsertT pk6 @ts3.
            txn(
                3,
                2,
                Level::Serializable,
                0,
                Fate::Committed(3),
                vec![
                    op(OpKind::Get, OpDetail::Get { pk: 5 }, OpOut::Row(None), 0),
                    op(
                        OpKind::InsertT,
                        OpDetail::InsertT {
                            pk: 6,
                            row: row3(6, 600, Some("r")),
                        },
                        OpOut::Ok,
                        0,
                    ),
                ],
            ),
        ],
    };
    let run = run_of(history, vec![(6, row3(6, 600, Some("r")))]);
    let verdict = check(&run);
    assert!(verdict.is_clean(), "{}", verdict.report());
}

/// The refinement must still bite: a phantom write-skew — two committed
/// SER txns at snap 1, each reading absent the key the other then
/// inserts (kills M-ser-add-none, the shape that added no rw edge for
/// any absent point read). Both keys are live at the end, so both final
/// writers are forced after the other's read: the rw cycle is real and
/// the checker must report it.
#[test]
fn t6c_regress_flags_a_ser_phantom_skew() {
    // setup: RC insert pk1 @ts1 (the state snap 1 reads).
    let setup = txn(
        1,
        0,
        Level::ReadCommitted,
        0,
        Fate::Committed(1),
        vec![op(
            OpKind::InsertT,
            OpDetail::InsertT {
                pk: 1,
                row: row3(1, 100, Some("x")),
            },
            OpOut::Ok,
            0,
        )],
    );
    // R1: SER snap 1 — Get pk5 absent, InsertT pk6 @ts5.
    let r1 = txn(
        2,
        1,
        Level::Serializable,
        1,
        Fate::Committed(5),
        vec![
            op(OpKind::Get, OpDetail::Get { pk: 5 }, OpOut::Row(None), 1),
            op(
                OpKind::InsertT,
                OpDetail::InsertT {
                    pk: 6,
                    row: row3(6, 600, Some("r1")),
                },
                OpOut::Ok,
                1,
            ),
        ],
    );
    // R2: SER snap 1 — Get pk6 absent, InsertT pk5 @ts6.
    let r2 = txn(
        3,
        2,
        Level::Serializable,
        1,
        Fate::Committed(6),
        vec![
            op(OpKind::Get, OpDetail::Get { pk: 6 }, OpOut::Row(None), 1),
            op(
                OpKind::InsertT,
                OpDetail::InsertT {
                    pk: 5,
                    row: row3(5, 500, Some("r2")),
                },
                OpOut::Ok,
                1,
            ),
        ],
    );
    let history = History {
        txns: vec![setup, r1, r2],
    };
    let final_rows = vec![
        (1, row3(1, 100, Some("x"))),
        (5, row3(5, 500, Some("r2"))),
        (6, row3(6, 600, Some("r1"))),
    ];
    let verdict = check(&run_of(history, final_rows));
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("not serializable")),
        "{}",
        verdict.report()
    );
}

// ----- c1b FU-B: the write side of the TRUNCATE window ------------------------

/// §4's relation rule obliges 40001 for *any* RR/SER access whose
/// catalog read names a post-`S` storage — a write included. An
/// in-window insert is data-consistent with the model (it commits after
/// the truncate, so the model's clear-then-insert ordering agrees): no
/// data check can flag it, only the window check's write leg can. The
/// same shape at RC is legal (the relation rule is RR/SER-only) and
/// stays clean — the over-eagerness guard.
#[test]
fn t6c_regress_flags_an_in_window_rr_write() {
    /// The setup every variant starts from: pk1 @ts1, then the DDL
    /// session's truncate installs storage 2 @ts2.
    fn setup_and_truncate() -> History {
        History {
            txns: vec![
                txn(
                    1,
                    0,
                    Level::ReadCommitted,
                    0,
                    Fate::Committed(1),
                    vec![op(
                        OpKind::InsertT,
                        OpDetail::InsertT {
                            pk: 1,
                            row: row3(1, 100, Some("x")),
                        },
                        OpOut::Ok,
                        0,
                    )],
                ),
                txn(
                    2,
                    DDL_SESSION,
                    Level::ReadCommitted,
                    1,
                    Fate::Committed(2),
                    vec![op(
                        OpKind::Truncate,
                        OpDetail::Truncate { new_sid: 2 },
                        OpOut::Ok,
                        1,
                    )],
                ),
            ],
        }
    }
    // W: began at snap 1 (before the truncate committed), opens the
    // table after it, and the insert lands in the new storage — the
    // recorded `sid` says so.
    let in_window_insert = |uid: u32, level: Level| {
        let mut write = op(
            OpKind::InsertT,
            OpDetail::InsertT {
                pk: 7,
                row: row3(7, 700, Some("w")),
            },
            OpOut::Ok,
            1,
        );
        write.sid = Some(2);
        txn(uid, 1, level, 1, Fate::Committed(3), vec![write])
    };
    // RR: the engine must have answered 40001 — flagged.
    let mut history = setup_and_truncate();
    history
        .txns
        .push(in_window_insert(3, Level::RepeatableRead));
    let verdict = check(&run_of(history, vec![(7, row3(7, 700, Some("w")))]));
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| { v.contains("a successful It write") && v.contains("40001 was required") }),
        "{}",
        verdict.report()
    );
    // RC: legal — the relation rule is RR/SER-only, and the write's
    // effect is model-consistent.
    let mut history = setup_and_truncate();
    history.txns.push(in_window_insert(3, Level::ReadCommitted));
    let verdict = check(&run_of(history, vec![(7, row3(7, 700, Some("w")))]));
    assert!(verdict.is_clean(), "{}", verdict.report());
}

// ----- c1b FU-D: the RC carve-out ---------------------------------------------

/// An RC "no row" verdict over a row that was live at `S`, deleted
/// inside `(S, commit]`, and re-inserted before the commit position —
/// the §5.2 EPQ skip shape the carve-out exists for. The history is
/// legal and must check clean (kills M-carveout-blind, `carved = false`
/// in `check_row_write`, which would flag it as a vanishing row).
#[test]
fn t6c_regress_rc_reinsert_carveout_stays_clean() {
    // setup: pk1 @ts1.
    let setup = txn(
        1,
        0,
        Level::ReadCommitted,
        0,
        Fate::Committed(1),
        vec![op(
            OpKind::InsertT,
            OpDetail::InsertT {
                pk: 1,
                row: row3(1, 100, Some("x")),
            },
            OpOut::Ok,
            0,
        )],
    );
    // W1: RC, snap 1 — deletes pk1 @ts2.
    let w1 = txn(
        2,
        1,
        Level::ReadCommitted,
        1,
        Fate::Committed(2),
        vec![op(
            OpKind::DeleteT,
            OpDetail::DeleteT { pk: 1 },
            OpOut::Applied(true),
            1,
        )],
    );
    // W2: RC, snap 2 — re-inserts pk1 @ts3.
    let w2 = txn(
        3,
        2,
        Level::ReadCommitted,
        2,
        Fate::Committed(3),
        vec![op(
            OpKind::InsertT,
            OpDetail::InsertT {
                pk: 1,
                row: row3(1, 111, Some("again")),
            },
            OpOut::Ok,
            2,
        )],
    );
    // V: RC, snap 1 — the row it reads at S was deleted and re-inserted
    // inside (S, commit]; the verdict "no row" is the legal EPQ skip.
    let v = txn(
        4,
        3,
        Level::ReadCommitted,
        1,
        Fate::Committed(4),
        vec![op(
            OpKind::UpdateSet,
            OpDetail::UpdateSet {
                pk: 1,
                attnum: 2,
                value: Some(Value::Int8(9)),
                new_row: None,
            },
            OpOut::Applied(false),
            1,
        )],
    );
    let history = History {
        txns: vec![setup, w1, w2, v],
    };
    let run = run_of(history, vec![(1, row3(1, 111, Some("again")))]);
    let verdict = check(&run);
    assert!(verdict.is_clean(), "{}", verdict.report());
    // The carve-out is the path that kept it clean — it fired, and was
    // needed: blind (`carved = false`) the same history is flagged.
    assert!(
        verdict.stats.rc_window_carveouts >= 1,
        "the carve-out did not fire: {:?}",
        verdict.stats
    );
}
