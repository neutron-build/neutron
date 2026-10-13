//! C-T6b3a item 4, SET/DROP NOT NULL half: C-T6 §3 — SET NOT NULL is a
//! validating scan of the latest committed state plus own writes under
//! the AccessExclusive: a stored NULL or a NULL `missing` (the decode
//! rule yields both as `None` — mutant N13) is 23502; otherwise the def
//! is rewritten and enforces (a NULL insert is 23502). DROP NOT NULL is
//! the def rewrite alone. Aborts restore the def.

mod t6b_support;

use t6b_support::{decode_def, i32_d, i32_v, ok, pk_schema, text_d, Rig};

use nucleus_catalog::{DbError, Stmt};
use nucleus_codec::{KeyType, Value};
use nucleus_txn::locks::RelLockMode;

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

/// §3 SET NOT NULL: the validating scan fails on a NULL row — the txn's
/// own writes count like committed ones — and succeeds once no NULL is
/// visible; the def then enforces the constraint on every later insert,
/// and an abort of the refused DDL changes nothing.
#[test]
fn set_not_null_validates_and_enforces() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"snn");
    add_row(&rig, oid, 1, Some("a"));

    // The txn's own NULL, written by an earlier statement of the same
    // txn: the scan (latest committed plus own writes) sees it.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(2), None]));
        let st2 = rig.stmt(&t, s);
        let err = rig
            .db
            .set_not_null(&st2, oid, 2, true)
            .expect_err("the own NULL row is visible to the scan");
        assert!(matches!(err, DbError::NotNullViolation));
        assert_eq!(err.sqlstate(), "23502");
        rig.abort_txn(t);
    }
    assert_eq!(
        decode_def(&rig.rel_row(oid).expect("rel row").def).version,
        1,
        "the refused DDL wrote nothing"
    );

    // A committed NULL row: refused the same way.
    add_row(&rig, oid, 3, None);
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let err = rig
            .db
            .set_not_null(&st, oid, 2, true)
            .expect_err("a committed NULL row is visible to the scan");
        assert_eq!(err.sqlstate(), "23502");
        rig.abort_txn(t);
    }

    // Delete the NULL row: the scan passes, the def is rewritten, and
    // the constraint enforces from then on.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        assert!(ok(rig.db.delete(&st, oid, &[i32_v(3)])));
        ok(rig.commit_rc(t));
    }
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.set_not_null(&st, oid, 2, true));
        ok(rig.commit_rc(t));
    }
    let def = decode_def(&rig.rel_row(oid).expect("rel row").def);
    assert_eq!(def.version, 2, "one DDL, one version");
    assert!(def.columns[1].not_null);
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        let err = rig
            .db
            .insert_with(&st, &h, &[i32_d(4), None])
            .expect_err("the def now refuses a NULL payload");
        assert_eq!(err.sqlstate(), "23502");
        rig.abort_txn(t);
    }
}

/// §3: a NULL `missing` is a NULL like any other (mutant N13) — SET NOT
/// NULL on a column added without a default fails while rows predate the
/// column; with a non-NULL `missing` the same scan passes (the decode
/// rule reads the default into those rows).
#[test]
fn set_not_null_null_missing_is_a_violation() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"miss");
    add_row(&rig, oid, 1, Some("a"));

    // ADD COLUMN c int4 NULL, no default: old rows read NULL (missing
    // absent). SET NOT NULL on it is 23502.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.add_column(&st, oid, "c", KeyType::Int4, false, None));
        ok(rig.commit_rc(t));
    }
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let err = rig
            .db
            .set_not_null(&st, oid, 3, true)
            .expect_err("a NULL missing is a NULL to the scan (N13)");
        assert!(matches!(err, DbError::NotNullViolation));
        assert_eq!(err.sqlstate(), "23502");
        rig.abort_txn(t);
    }
    assert_eq!(
        decode_def(&rig.rel_row(oid).expect("rel row").def).version,
        2,
        "the refused DDL wrote nothing"
    );

    // The twin: a column added WITH default 7 — the old rows read the
    // missing value, so the same scan passes and the flag lands.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig
            .db
            .add_column(&st, oid, "d", KeyType::Int4, false, Some(Value::Int4(7))));
        ok(rig.commit_rc(t));
    }
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.set_not_null(&st, oid, 4, true));
        ok(rig.commit_rc(t));
    }
    let def = decode_def(&rig.rel_row(oid).expect("rel row").def);
    assert!(
        def.columns[3].not_null,
        "the non-NULL missing satisfied the scan"
    );
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
        let rows = ok(rig.db.scan(&st, &h));
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1,
            vec![
                i32_d(1),
                text_d("a"),
                None,
                Some(nucleus_codec::row::Datum::Inline(Value::Int4(7)))
            ],
            "the old row reads [NULL missing, 7]"
        );
        ok(rig.commit_rc(t));
    }
}

/// §3 DROP NOT NULL: the def rewrite alone (no scan), version +1, and
/// the constraint is gone — a NULL insert succeeds afterwards.
#[test]
fn drop_not_null_is_def_only() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"dnn");
    add_row(&rig, oid, 1, Some("a"));

    // Make the column NOT NULL (v2), then drop it again (v3).
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.set_not_null(&st, oid, 2, true));
        ok(rig.commit_rc(t));
    }
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.set_not_null(&st, oid, 2, false));
        ok(rig.commit_rc(t));
    }
    let def = decode_def(&rig.rel_row(oid).expect("rel row").def);
    assert_eq!(def.version, 3);
    assert!(!def.columns[1].not_null, "DROP NOT NULL cleared the flag");

    // A NULL payload is plain data again.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(2), None]));
        ok(rig.commit_rc(t));
    }

    // And the rollback half: a SET NOT NULL rolled back to a savepoint
    // leaves the flag as it was.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        assert!(ok(rig.db.delete(&st, oid, &[i32_v(2)])));
        ok(rig.commit_rc(t));
    }
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let sp = ok(t.savepoint());
        let st = rig.stmt(&t, s);
        ok(rig.db.set_not_null(&st, oid, 2, true));
        ok(rig.core().rollback_to(&t, sp));
        ok(rig.commit_rc(t));
        assert!(
            !decode_def(&rig.rel_row(oid).expect("rel row").def).columns[1].not_null,
            "the rollback restores the flag"
        );
    }
}
