//! C-T6b1 item 9, the C-T0 §4 relation rule as `open_table` applies it:
//! RR `S` before a foreign TRUNCATE → 40001 (mutant N2: the check
//! skipped); the txn's own TRUNCATE is exempt (mutant N2b: the check run
//! against the own-writes view); absent at `S` → proceed and read empty;
//! and the handle's catalog read is at the **latest committed** state
//! (mutant N1: read at `S`), so a def change committed after `S` is seen
//! and rows decode under it.
//!
//! C-T6b1-fix, the gate's follow-up pins: the **index** rows of the
//! handle's read are at the latest committed state too (mutant G2: read
//! at `S` — the 0A000 guard of `insert` would be bypassable); `insert`
//! re-validates its handle under the RowExclusive it takes (mutant
//! DEL-CHK: an own TRUNCATE would key rows into the retired storage
//! range, a foreign def change would encode under a stale `natts`); and
//! the own-writes bound of the catalog read is `seq < seq0` (mutant G3:
//! `u32::MAX` — a statement would see its own uncommitted DDL).

mod t6b_support;

use std::ops::Bound;

use t6b_support::{i32_d, i32_v, ok, pk_schema, text_d, Rig};

use nucleus_catalog::{DbError, IndexColumn, IndexDef, IndexState, Stmt};
use nucleus_codec::{Direction, Nulls};
use nucleus_kv::{OrderedKv, Snapshot};
use nucleus_txn::catalog::{table_range, Ddl, IdxStorage};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::TxnError;

/// One index definition (§6 bytes): a non-unique DeleteOnly index on
/// attnum 1 — enough for `open_table` to list it and `insert` to refuse.
fn idx_def() -> IndexDef {
    IndexDef {
        columns: vec![IndexColumn {
            attnum: 1,
            dir: Direction::Asc,
            nulls: Nulls::Last,
        }],
        unique: false,
        nulls_not_distinct: false,
        concurrently: false,
        state: IndexState::DeleteOnly,
        built_at: 0,
    }
}

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

/// Mutant G2 (M-1): the index rows read at `st.snapshot` instead of the
/// latest committed catalog — a handle whose `S` predates a committed
/// CREATE INDEX would list no index, so C-T6b2a's maintenance would be
/// silently skippable by the same one-line regression. §4: the
/// statement's catalog read — relation and index rows alike — is at the
/// latest committed state plus the txn's own earlier statements. The
/// insert half of the C-T6b1 placeholder pin ("0A000 until C-T6b2a")
/// became the DeleteOnly no-entry pin when C-T6b2a landed maintenance.
#[test]
fn rr_lists_an_index_committed_after_s_and_insert_writes_no_entry() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (r, _guard, s) = rig.rr();

    // A foreign CREATE INDEX commits after the reader's S.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let isid = ok(rig.catalog().alloc_storage_id(rig.core()));
    let (idx_oid, _ranges) = ok(rig.catalog().catalog_create_index(
        &rig.ddl(&t1, s1),
        oid,
        IdxStorage::Id(isid),
        &ok(idx_def().encode()),
    ));
    ok(rig.commit_rc(t1));

    // The reader's open_table lists the index: the index rows are read at
    // the latest committed catalog, not at the reader's S.
    let st = ok(Stmt::new(&r, s));
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    assert_eq!(
        h.indexes.len(),
        1,
        "the latest committed index row is listed"
    );
    assert_eq!(h.indexes[0].oid, idx_oid);

    // DeleteOnly (§6): the insert maintains nothing — both entry ranges
    // stay empty.
    ok(rig.db.insert(&st, &h, &[i32_d(1), text_d("x")]));
    ok(rig.commit_rc(r));
    assert!(
        rig.raw_entry_versions(isid).is_empty(),
        "a DeleteOnly index gets no entry from an insert"
    );
}

/// Item 2 (M-2), the own-DDL half, mutant DEL-CHK: the handle validation
/// in `insert` deleted — the row would be keyed into the retired storage
/// range, where the drop-cleanup job's DeleteRange destroys the committed
/// row once `W` passes the DDL. Statement 1 opens the handle, statement 2
/// TRUNCATEs, statement 3 inserts through statement 1's handle: the
/// insert must refuse with 55000 and leave no row key inside the retired
/// range.
#[test]
fn insert_through_a_pre_truncate_handle_is_refused() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid0) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // One txn: statement 1 opens the handle, statement 2 TRUNCATEs.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st1 = ok(Stmt::new(&t, s));
    let h_old = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    let sid_new = ok(rig.catalog().catalog_truncate_table(&rig.ddl(&t, s), oid));
    assert_ne!(sid_new, sid0, "the TRUNCATE installed a new storage id");

    // Statement 3 inserts through the pre-TRUNCATE handle: refused.
    let st3 = ok(Stmt::new(&t, s));
    let err = rig
        .db
        .insert(&st3, &h_old, &[i32_d(1), text_d("stale")])
        .expect_err("a pre-TRUNCATE handle must be refused");
    assert_eq!(err.sqlstate(), "55000");
    assert!(matches!(
        err,
        DbError::ObjectNotInPrerequisiteState("stale table handle")
    ));

    // No row key landed inside the retired range `[lo, hi)`: a raw scan
    // of the old storage sees nothing (the TRUNCATE wrote only catalog
    // rows; the range's data deletion is the drop-cleanup job's).
    let (lo, hi) = table_range(sid0);
    let snap = rig.kv.snapshot();
    let mut stale_keys = 0usize;
    for k in snap.scan(
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        false,
    ) {
        k.expect("kv scan");
        stale_keys += 1;
    }
    assert_eq!(stale_keys, 0, "no row key inside the retired storage range");

    // Through a fresh handle of the same statement the insert works and
    // the row is the table's only one.
    let h_new = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    assert_eq!(h_new.storage_id, sid_new);
    ok(rig.db.insert(&st3, &h_new, &[i32_d(1), text_d("fresh")]));
    ok(rig.commit_rc(t));

    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st4 = rig.stmt(&t2, s2);
    let h4 = ok(rig.db.open_table(&st4, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st4, &h4)).len(), 1);
    ok(rig.commit_rc(t2));
}

/// Item 2 (M-2), the foreign-DDL half, mutant DEL-CHK: a def change
/// committed after the handle's opening must refuse `insert` through the
/// stale handle — never encode a row under a stale `natts` (the handle's
/// v1 def knows 2 columns; the committed catalog says 3). The handle
/// comes from an ended txn (a foreign ALTER's AccessExclusive cannot
/// commit while the opening txn holds any relation lock, so the
/// cross-statement staleness is only schedulable through a handle that
/// outlives its opening txn — exactly the long-lived-handle reuse the
/// validation exists to make safe).
#[test]
fn insert_through_a_pre_alter_handle_is_refused() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let schema = pk_schema();
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &schema));
    // The handle, opened at v1 by a txn that then ends.
    let st2 = rig.stmt(&t0, s0);
    let h_old = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    assert_eq!(h_old.def_version, 1);
    ok(rig.commit_rc(t0));

    // A foreign ADD COLUMN commits (v2, same storage id).
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let schema2 = ok(schema.add_column("extra", nucleus_codec::KeyType::Int8, false, None));
    assert_eq!(schema2.version, 2);
    ok(rig
        .catalog()
        .catalog_alter_rel_def(&rig.ddl(&t1, s1), oid, &ok(schema2.encode())));
    ok(rig.commit_rc(t1));

    // Inserting through the v1 handle is refused: the row would encode
    // with a stale `natts` under a catalog that says 3 columns.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st3 = ok(Stmt::new(&t, s));
    let err = rig
        .db
        .insert(&st3, &h_old, &[i32_d(1), text_d("stale")])
        .expect_err("a pre-ALTER handle must be refused");
    assert_eq!(err.sqlstate(), "55000");
    assert!(matches!(
        err,
        DbError::ObjectNotInPrerequisiteState("stale table handle")
    ));

    // A fresh handle of the same statement sees v2 (§4: the latest
    // committed def) and the insert works; the new column is NULL (§3
    // decode rule).
    let h_new = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    assert_eq!(h_new.def_version, 2);
    assert_eq!(h_new.storage_id, sid, "ALTER keeps every storage id (§3)");
    ok(rig.db.insert(&st3, &h_new, &[i32_d(1), text_d("v2"), None]));
    ok(rig.commit_rc(t));

    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st4 = rig.stmt(&t2, s2);
    let h4 = ok(rig.db.open_table(&st4, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st4, &h4));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("v2"), None]);
    ok(rig.commit_rc(t2));
}

/// Mutant G3 (L-1): the own catalog read's `stmt_seq` widened to
/// `u32::MAX` — a statement would see its own uncommitted DDL. §4's read
/// is "the latest committed catalog plus the txn's own **earlier**
/// statements": a `Ddl` built at the statement's own `st.ctx()` writes at
/// `seq == seq0`, invisible to that statement's `open_table`
/// (I-HALLOWEEN) and visible to a later statement of the same txn.
#[test]
fn a_statements_own_uncommitted_ddl_is_invisible_to_its_open_table() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // One statement that ALTERs and then opens: the Ddl runs at the
    // statement's own ctx, so its catalog write is an own intent at
    // `seq == seq0`.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = ok(Stmt::new(&t, s));
    let schema2 = ok(pk_schema().set_not_null(2, true));
    let ddl = Ddl::new(rig.core(), &t, st.ctx());
    ok(rig
        .catalog()
        .catalog_alter_rel_def(&ddl, oid, &ok(schema2.encode())));

    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    assert_eq!(
        h.def_version, 1,
        "the statement's own uncommitted DDL is invisible to it"
    );

    // A later statement of the same txn sees it (an own intent at
    // `seq < seq0` there)…
    let st2 = ok(Stmt::new(&t, s));
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(h2.def_version, 2, "a later statement sees the own DDL");
    ok(rig.commit_rc(t));

    // …and the commit leaves v2 behind.
    let row = rig.rel_row(oid).expect("rel row");
    assert_eq!(t6b_support::decode_def(&row.def).version, 2);
}
