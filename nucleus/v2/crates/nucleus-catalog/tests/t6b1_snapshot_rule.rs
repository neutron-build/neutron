//! C-T6b1 item 9, the C-T0 §4 relation rule as `open_table` applies it:
//! RR `S` before a foreign TRUNCATE → 40001 (mutant N2: the check
//! skipped); the txn's own TRUNCATE is exempt (mutant N2b: the check run
//! against the own-writes view); absent at `S` → proceed and read empty;
//! and the handle's catalog read is at the **latest committed** state
//! (mutant N1: read at `S`), so a def change committed after `S` is seen
//! and rows decode under it.

mod t6b_support;

use t6b_support::{i32_d, i32_v, ok, pk_schema, text_d, Rig};

use nucleus_catalog::{DbError, Stmt};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::TxnError;

/// Mutant N2: the RR/SER storage-id check skipped — a foreign TRUNCATE
/// committed after the reader's `S` must raise 40001 on the next access.
#[test]
fn rr_before_a_foreign_truncate_is_40001() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (r, _guard, s) = rig.rr();
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    ok(rig.catalog().catalog_truncate_table(&rig.ddl(&t1, s1), oid));
    ok(rig.commit_rc(t1));

    let st = ok(Stmt::new(&r, s));
    let err = rig
        .db
        .open_table(&st, oid, RelLockMode::AccessShare)
        .expect_err("a foreign TRUNCATE after S is a serialization failure");
    assert_eq!(err.sqlstate(), "40001");
    assert!(matches!(err, DbError::Txn(TxnError::SerializationFailure)));
    rig.abort_txn(r);

    // SER behaves the same (§4 covers both levels).
    let ser = rig.ser(false);
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    ok(rig.catalog().catalog_truncate_table(&rig.ddl(&t2, s2), oid));
    ok(rig.commit_rc(t2));
    let st = ok(Stmt::new(&ser.txn, ser.s));
    let err = rig
        .db
        .open_table(&st, oid, RelLockMode::AccessShare)
        .expect_err("SER: a foreign TRUNCATE after S is a serialization failure");
    assert_eq!(err.sqlstate(), "40001");
    rig.abort_ser(ser);
}

/// Mutant N2b: the latest-committed row of the storage check read through
/// the txn's own-writes view — the txn's **own** TRUNCATE would look like
/// a foreign rewrite. §4: "a storage id installed by the txn's own DDL is
/// not an error". After its own TRUNCATE the txn inserts and reads back
/// through the new, empty storage.
#[test]
fn rr_own_truncate_then_insert_then_get_succeeds() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid0) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    let st2 = rig.stmt(&t0, s0);
    let h0 = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st2, &h0, &[i32_d(1), text_d("old")]));
    ok(rig.commit_rc(t0));

    let (r, _guard, s) = rig.rr();
    // The txn's own TRUNCATE (an own-DDL storage id is exempt).
    ok(rig.catalog().catalog_truncate_table(&rig.ddl(&r, s), oid));

    let st = ok(Stmt::new(&r, s));
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    assert_ne!(
        h.storage_id, sid0,
        "the handle names the txn's own new storage"
    );
    ok(rig.db.insert(&st, &h, &[i32_d(5), text_d("new")]));
    let st2 = rig.stmt(&r, s);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(
        ok(rig.db.get(&st2, &h2, &[i32_v(5)])),
        Some(vec![i32_d(5), text_d("new")]),
        "the own insert reads back through the new storage"
    );
    assert_eq!(
        ok(rig.db.get(&st2, &h2, &[i32_v(1)])),
        None,
        "the truncated row is gone"
    );
    let rows = ok(rig.db.scan(&st2, &h2));
    assert_eq!(rows.len(), 1);
    ok(rig.commit_rc(r));

    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st3, &h3)).len(), 1);
    ok(rig.commit_rc(t2));
}

/// §4: "A relation absent at `S` reads as empty" — the table was created
/// after the reader's snapshot; the handle is still built from the latest
/// committed row and the scan sees nothing.
#[test]
fn rr_before_create_table_scans_empty() {
    let rig = Rig::new();
    let (r, _guard, s) = rig.rr();

    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"late", &pk_schema()));
    ok(rig.commit_rc(t0));

    let st = ok(Stmt::new(&r, s));
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    assert_eq!(h.def_version, 1);
    let st2 = rig.stmt(&r, s);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert!(
        ok(rig.db.scan(&st2, &h2)).is_empty(),
        "a relation absent at S reads as empty"
    );
    assert_eq!(ok(rig.db.get(&st2, &h2, &[i32_v(1)])), None);
    rig.abort_txn(r);
}

/// Mutant N1: the handle's catalog read at `st.snapshot` instead of the
/// latest committed state — a def change committed after `S` must be seen
/// (§4: "a newer def.version is not an error: rows decode under the latest
/// def"). The ADD COLUMN has no `missing` value, so the old row decodes
/// with a NULL in the new column (§3 decode rule).
#[test]
fn latest_committed_catalog_read_sees_a_def_change_after_s() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let schema = pk_schema();
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &schema));
    let st2 = rig.stmt(&t0, s0);
    let h0 = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st2, &h0, &[i32_d(1), text_d("x")]));
    ok(rig.commit_rc(t0));

    let (r, _guard, s) = rig.rr();

    // A foreign ADD COLUMN commits after S (§3: version +1, no rewrite).
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let schema2 = ok(schema.add_column("extra", nucleus_codec::KeyType::Int8, false, None));
    assert_eq!(schema2.version, 2);
    ok(rig
        .catalog()
        .catalog_alter_rel_def(&rig.ddl(&t1, s1), oid, &ok(schema2.encode())));
    ok(rig.commit_rc(t1));

    let st = ok(Stmt::new(&r, s));
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    assert_eq!(h.def_version, 2, "the latest def version is seen");
    assert_eq!(h.schema.columns.len(), 3);
    let st2 = rig.stmt(&r, s);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st2, &h2));
    assert_eq!(rows.len(), 1, "the row written under v1 is still there");
    assert_eq!(
        rows[0].1,
        vec![i32_d(1), text_d("x"), None],
        "the old row decodes under the latest def with a NULL missing"
    );
    rig.abort_txn(r);
}

/// The §4 rule is about storage ids only: a def change committed after
/// `S` (same storage) is NOT an error for the RR reader, and its own
/// later statements continue on the same storage id.
#[test]
fn rr_reader_continues_after_a_same_storage_def_change() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (r, _guard, s) = rig.rr();
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let schema2 = ok(pk_schema().set_not_null(2, true));
    ok(rig
        .catalog()
        .catalog_alter_rel_def(&rig.ddl(&t1, s1), oid, &ok(schema2.encode())));
    ok(rig.commit_rc(t1));

    // Reads proceed; writes still land in the same storage.
    let st = ok(Stmt::new(&r, s));
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    assert_eq!(h.storage_id, sid, "ALTER keeps every storage id (§3)");
    ok(rig.db.insert(&st, &h, &[i32_d(2), text_d("rr")]));
    ok(rig.commit_rc(r));

    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st2, &h2)).len(), 1);
    ok(rig.commit_rc(t2));
}
