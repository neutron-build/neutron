//! C-T6b3a item 4, ADD COLUMN half: C-T6 §3 without a rewrite — old rows
//! read `missing` (default or NULL), new rows round-trip, an update
//! rewrites the `row_header` to the current version (§3 lazy upgrade; no
//! DDL rewrites rows), ADD COLUMN NOT NULL without a default scans under
//! the AccessExclusive (23502 on a non-empty table, committed rows or own
//! writes — mutant N11; OK on an empty one, where the def then enforces),
//! a non-NULL default is the fast default (no scan), abort and ROLLBACK
//! TO restore the def, an RR txn with `S` before the ADD COLUMN sees the
//! new column, the rewrite refusals name C-T6r, and
//! `add_not_null_vs_concurrent_insert` pins the lock-first rule (mutant
//! N18).

mod t6b_support;

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use t6b_support::{
    decode_def, i32_d, i32_v, ok, pk_schema, row_key_of, set_col, text_d, wait_until, Rig,
};

use nucleus_catalog::{refuse_rewrite, DbError, RewriteKind, Stmt};
use nucleus_codec::row::{row_header, Datum};
use nucleus_codec::{Collation, KeyType, Value};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::TxnId;

/// Everything a thread may wait on is bounded by this; the joins use it
/// too (a hang is a failure, not a pass).
const BOUND: Duration = Duration::from_secs(30);

/// `CREATE TABLE t (id int4 PRIMARY KEY, payload text)`, committed.
fn mk_table(rig: &Rig, name: &[u8]) -> (u64, u64) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = ok(Stmt::new(&t, s));
    let r = ok(rig.db.create_table(&st, name, &pk_schema()));
    ok(rig.commit_rc(t));
    r
}

/// One committed row `(id, payload)` in one statement of its own txn.
fn add_row(rig: &Rig, oid: u64, id: i32, payload: Option<&str>) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig
        .db
        .insert_with(&st, &h, &[i32_d(id), payload.map(text_d).unwrap_or(None)]));
    ok(rig.commit_rc(t));
}

/// A ghost reader's raw bytes of one row at `visible_ts` (committed
/// state only — the oracle for the §2 row header, independent of the
/// typed layer).
fn raw_row(rig: &Rig, key: &[u8]) -> Vec<u8> {
    let view = rig.core().open_view();
    ok(nucleus_txn::read::read_key(
        rig.core(),
        &view,
        key,
        &ReadCtx {
            txn: TxnId {
                epoch: rig.core().epoch(),
                n: 0,
            },
            snapshot: rig.core().visible_ts(),
            stmt_seq: 0,
        },
        &mut nucleus_txn::read::NoSsi,
    ))
    .expect("row bytes")
}

/// §3 ADD COLUMN: old rows read `missing` (a default, or NULL when none
/// was given), new rows round-trip, an update rewrites the row header to
/// the current version and an untouched old row keeps its original one
/// (lazy upgrade: no DDL ever rewrites rows).
#[test]
fn add_column_missing_new_rows_and_lazy_upgrade() {
    let rig = Rig::new();
    let (oid, sid) = mk_table(&rig, b"add");
    add_row(&rig, oid, 1, Some("a"));
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);

    // ADD COLUMN note text NULL, no default → old rows read NULL; the
    // def gains attnum 3 with no `missing`.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig
            .db
            .add_column(&st, oid, "note", KeyType::Text(Collation::C), false, None));
        ok(rig.commit_rc(t));
    }
    let def = decode_def(&rig.rel_row(oid).expect("rel row").def);
    assert_eq!(def.version, 2, "one DDL, one version");
    assert_eq!(def.columns[2].attnum, 3, "attnums are never reused");
    assert_eq!(def.columns[2].missing, None, "no default was given");
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
        assert_eq!(
            ok(rig.db.get(&st, &h, &[i32_v(1)])),
            Some(vec![i32_d(1), text_d("a"), None]),
            "the old row reads NULL for the new column"
        );
        ok(rig.commit_rc(t));
    }
    let old = raw_row(&rig, &key1);
    assert_eq!(ok(row_header(&old)).schema_version, 1);
    assert_eq!(ok(row_header(&old)).natts, 2, "no DDL rewrites rows");

    // ADD COLUMN flag int4 NOT NULL DEFAULT 7 — the fast default: no scan
    // on a non-empty table, old rows read the `missing` value.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig
            .db
            .add_column(&st, oid, "flag", KeyType::Int4, true, Some(Value::Int4(7))));
        ok(rig.commit_rc(t));
    }
    let def = decode_def(&rig.rel_row(oid).expect("rel row").def);
    assert_eq!(def.version, 3);
    assert!(def.columns[3].not_null);
    assert_eq!(def.columns[3].missing, Some(Value::Int4(7)));

    // A new row round-trips through insert and get.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(
            &st,
            &h,
            &[
                i32_d(2),
                text_d("b"),
                Some(Datum::Inline(Value::Text("n".into()))),
                i32_d(9),
            ],
        ));
        ok(rig.commit_rc(t));
    }
    let key2 = row_key_of(sid, &pk_schema(), &[i32_v(2)]);
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
        assert_eq!(
            ok(rig.db.get(&st, &h, &[i32_v(2)])),
            Some(vec![
                i32_d(2),
                text_d("b"),
                Some(Datum::Inline(Value::Text("n".into()))),
                i32_d(9)
            ]),
            "a row written under the current def round-trips"
        );
        // The old row under the newest def: NULL note, missing 7.
        assert_eq!(
            ok(rig.db.get(&st, &h, &[i32_v(1)])),
            Some(vec![
                i32_d(1),
                text_d("a"),
                None,
                Some(Datum::Inline(Value::Int4(7)))
            ]),
            "the old row reads its missing values"
        );
        ok(rig.commit_rc(t));
    }
    let new = raw_row(&rig, &key2);
    assert_eq!(ok(row_header(&new)).schema_version, 3, "written at v3");
    assert_eq!(ok(row_header(&new)).natts, 4, "every allocated attnum");

    // An update of the old row rewrites its header to the current
    // version (§3 lazy upgrade) — and only that row's.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        assert!(ok(rig.db.update(
            &st,
            oid,
            &[i32_v(1)],
            &[set_col(2, text_d("a2"))]
        )));
        ok(rig.commit_rc(t));
    }
    let upgraded = raw_row(&rig, &key1);
    assert_eq!(
        ok(row_header(&upgraded)).schema_version,
        3,
        "the update writes at the current version"
    );
    assert_eq!(ok(row_header(&upgraded)).natts, 4);
    assert_eq!(
        ok(row_header(&new)).natts,
        4,
        "the untouched row keeps its bytes"
    );
    assert_eq!(
        ok(row_header(&raw_row(&rig, &key2))).schema_version,
        3,
        "the other untouched row keeps its bytes"
    );
}

/// §3: NOT NULL without a non-NULL default is allowed only if no row is
/// visible at the latest committed state plus own writes (mutant N11: a
/// skipped scan lets the non-empty case through). On an empty table the
/// def itself then enforces the constraint (a NULL insert is 23502).
#[test]
fn add_column_not_null_without_default_scans() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"nn");
    add_row(&rig, oid, 1, Some("a"));

    // A committed row → 23502, and nothing was written.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let err = rig
            .db
            .add_column(&st, oid, "x", KeyType::Int4, true, None)
            .expect_err("a non-empty table refuses NOT NULL without a default");
        assert!(matches!(err, DbError::NotNullViolation));
        assert_eq!(err.sqlstate(), "23502");
        rig.abort_txn(t);
    }
    assert_eq!(
        decode_def(&rig.rel_row(oid).expect("rel row").def).version,
        1,
        "the refused DDL wrote nothing"
    );

    // The txn's own writes count too: insert a row in an earlier
    // statement, then ADD COLUMN NOT NULL in the same txn.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(2), text_d("b")]));
        let st2 = rig.stmt(&t, s);
        let err = rig
            .db
            .add_column(&st2, oid, "x", KeyType::Int4, true, None)
            .expect_err("the txn's own row is visible to the scan");
        assert_eq!(err.sqlstate(), "23502");
        rig.abort_txn(t);
    }

    // An empty table takes it; the def then refuses a NULL (23502) and
    // accepts a value.
    let (oid2, _sid2) = mk_table(&rig, b"empty");
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.add_column(&st, oid2, "x", KeyType::Int4, true, None));
        ok(rig.commit_rc(t));
    }
    assert_eq!(
        decode_def(&rig.rel_row(oid2).expect("rel row").def).version,
        2
    );
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid2, RelLockMode::RowExclusive));
        let err = rig
            .db
            .insert_with(&st, &h, &[i32_d(1), text_d("a"), None])
            .expect_err("the new NOT NULL column refuses a NULL");
        assert_eq!(err.sqlstate(), "23502");
        rig.abort_txn(t);
    }
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid2, RelLockMode::RowExclusive));
        ok(rig
            .db
            .insert_with(&st, &h, &[i32_d(1), text_d("a"), i32_d(3)]));
        ok(rig.commit_rc(t));
    }
}

/// §5/§5.5: the def rewrite is transactional — an abort and a ROLLBACK TO
/// restore the old def, the DDL txn's own later statements see the new
/// one before that, and the txn continues cleanly after a rollback.
#[test]
fn add_column_abort_and_rollback_restore_the_def() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"ab");

    // Abort.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.add_column(&st, oid, "c", KeyType::Int8, false, None));
        let st2 = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
        assert_eq!(h.def_version, 2, "the txn sees its own ADD COLUMN");
        assert_eq!(h.schema.columns.len(), 3);
        rig.abort_txn(t);
        let restored = decode_def(&rig.rel_row(oid).expect("rel row").def);
        assert_eq!(restored.version, 1, "the abort restores the def");
        assert_eq!(restored.columns.len(), 2);
    }

    // ROLLBACK TO the savepoint taken before the ADD COLUMN; the txn then
    // commits with the old def.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let sp = ok(t.savepoint());
        let st = rig.stmt(&t, s);
        ok(rig.db.add_column(&st, oid, "c", KeyType::Int8, false, None));
        ok(rig.core().rollback_to(&t, sp));
        let st2 = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
        assert_eq!(h.def_version, 1, "the rollback restores the def");
        ok(rig.commit_rc(t));
        assert_eq!(
            decode_def(&rig.rel_row(oid).expect("rel row").def).version,
            1,
            "nothing the txn added survived"
        );
    }
}

/// §4/C-T6 §4: an RR txn with `S` taken before a foreign ADD COLUMN sees
/// the new column — the storage id is unchanged (no 40001), the handle's
/// def is the latest, and the txn's rows at `S` decode under it (the old
/// row reads the `missing` default).
#[test]
fn rr_snapshot_before_add_column_sees_the_new_column() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"rr");
    add_row(&rig, oid, 1, Some("a"));

    // The RR txn's snapshot S is taken first and stays registered.
    let (t, _guard, s) = rig.rr();

    {
        let t2 = rig.rc();
        let s2 = rig.core().visible_ts();
        let st = rig.stmt(&t2, s2);
        ok(rig
            .db
            .add_column(&st, oid, "c", KeyType::Int8, false, Some(Value::Int8(9))));
        ok(rig.commit_rc(t2));
    }

    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    assert_eq!(h.def_version, 2, "the def is read at the latest catalog");
    assert_eq!(h.schema.columns.len(), 3, "the new column is there");
    let rows = ok(rig.db.scan(&st, &h));
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1,
        vec![i32_d(1), text_d("a"), Some(Datum::Inline(Value::Int8(9)))],
        "the row at S decodes under the latest def, missing applied"
    );
    ok(rig.commit_rc(t));
}

/// §9.3/§9.4: every rewrite-requiring ALTER is 0A000 with a message
/// naming C-T6r (the SQL layer calls this before anything else).
#[test]
fn refuse_rewrite_names_c_t6r() {
    for kind in [
        RewriteKind::VolatileDefault,
        RewriteKind::Serial,
        RewriteKind::Identity,
        RewriteKind::AlterColumnType,
        RewriteKind::AddPrimaryKey,
    ] {
        let err = refuse_rewrite(kind);
        assert_eq!(err.sqlstate(), "0A000", "{kind:?}");
        match err {
            DbError::FeatureNotSupported(what) => {
                assert!(what.contains("C-T6r"), "{kind:?} names C-T6r: {what}")
            }
            other => panic!("wrong error for {kind:?}: {other:?}"),
        }
    }
}

/// The input errors: a duplicate live name is 42701 (a dropped column's
/// name may be reused — pinned in the drop-column tests), and a relation
/// the catalog has no row for is 42P01.
#[test]
fn add_column_input_errors() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"err");

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let err = rig
        .db
        .add_column(&st, oid, "payload", KeyType::Int4, false, None)
        .expect_err("a live name may not be reused");
    assert!(matches!(err, DbError::DuplicateColumn));
    assert_eq!(err.sqlstate(), "42701");

    for r in [
        rig.db.add_column(&st, 999, "x", KeyType::Int4, false, None),
        rig.db.drop_column(&st, 999, 2),
        rig.db.set_not_null(&st, 999, 2, true),
    ] {
        let err = r.expect_err("a missing relation is 42P01");
        assert!(matches!(err, DbError::UndefinedTable), "{err:?}");
        assert_eq!(err.sqlstate(), "42P01");
    }
    rig.abort_txn(t);
}

/// Mutant N18 (the scan before the AccessExclusive): thread B inserts a
/// row and parks holding RowExclusive; thread A's ADD COLUMN NOT NULL
/// must park on B's lock **before** its scan, so after B commits the scan
/// sees B's row and the DDL fails 23502. An implementation that scans
/// first sees the uncommitted table empty and wrongly succeeds once the
/// lock is granted.
#[test]
fn add_not_null_vs_concurrent_insert() {
    let rig = Arc::new(Rig::new());
    let (oid, _sid) = mk_table(&rig, b"vs");

    // B: an insert txn that stays open until the test releases it.
    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_b = Arc::clone(&rig);
    let b = thread::spawn(move || {
        let t = rig_b.rc();
        let s = rig_b.core().visible_ts();
        let st = ok(Stmt::new(&t, s));
        let h = ok(rig_b.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig_b.db.insert_with(&st, &h, &[i32_d(1), text_d("b")]));
        held_tx.send(()).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        ok(rig_b.commit_rc(t));
    });
    held_rx
        .recv_timeout(BOUND)
        .expect("B holds RowExclusive with a row in flight");

    // A: ADD COLUMN NOT NULL without a default — the only wait this
    // schedule can produce is A parked on B's RowExclusive.
    let (res_tx, res_rx) = mpsc::channel::<Result<(), String>>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        let st = ok(Stmt::new(&t, s));
        let r = rig_a
            .db
            .add_column(&st, oid, "x", KeyType::Int4, true, None);
        let out = match &r {
            Ok(()) => Ok(()),
            Err(e) => Err(e.sqlstate().to_string()),
        };
        match r {
            Ok(()) => {
                ok(rig_a.commit_rc(t));
            }
            Err(_) => {
                rig_a.abort_txn(t);
            }
        }
        res_tx.send(out).expect("res signal");
    });

    wait_until(BOUND, || !rig.wait_edges().is_empty());
    assert!(
        res_rx.try_recv().is_err(),
        "the ADD COLUMN must not finish under B's RowExclusive"
    );

    // B commits; A wakes with the lock granted and its scan — run under
    // the lock — sees B's row.
    release_tx.send(()).expect("release");
    match res_rx.recv_timeout(BOUND).expect("A finished within bound") {
        Ok(()) => panic!("the ADD COLUMN must fail: B's row is visible under the lock"),
        Err(state) => assert_eq!(state, "23502", "the scan sees B's committed row"),
    }
    b.join().expect("B join");
    a.join().expect("A join");

    // The refused DDL left the def alone.
    assert_eq!(
        decode_def(&rig.rel_row(oid).expect("rel row").def).version,
        1
    );
}
