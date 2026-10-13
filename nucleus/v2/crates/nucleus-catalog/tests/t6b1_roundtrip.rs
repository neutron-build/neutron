//! C-T6b1 item 9: create/insert/get/scan round-trips — a pk table, a
//! no-PK table (hidden rowids distinct within a statement, across txns
//! and across a crash + reopen), the §3 2704-byte key limit, insert
//! refused on an indexed table (0A000, C-T6b2a) and `open_table` refused
//! on an explicit-range index. Every test names the mutant it kills.

mod t6b_support;

use std::collections::BTreeSet;

use t6b_support::{i32_d, i32_v, ok, pk_schema, rowid_schema, text_d, text_pk_schema, Rig};

use nucleus_catalog::{DbError, Stmt};
use nucleus_codec::Value;
use nucleus_txn::catalog::IdxStorage;
use nucleus_txn::locks::RelLockMode;

/// Mutants: row keys computed without the table_lo prefix (collisions
/// across storages); the pk key not ASC NULLS LAST; decode under a stale
/// schema. Round-trips the row bytes through the whole stack: encode →
/// insert_key → §4 read → decode.
#[test]
fn pk_table_round_trip() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core().visible_ts();

    let st = ok(Stmt::new(&t, s));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));

    // Two inserts in one statement, then the reads of a LATER statement
    // (I-HALLOWEEN: the same statement never sees its own writes).
    let st2 = ok(Stmt::new(&t, s));
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st2, &h, &[i32_d(1), text_d("one")]));
    ok(rig.db.insert(&st2, &h, &[i32_d(2), text_d("two")]));

    // A read in the SAME statement never sees its own writes
    // (I-HALLOWEEN: the intent's layer sits at seq0).
    assert!(
        ok(rig.db.get(&st2, &h, &[i32_v(1)])).is_none(),
        "a statement never sees its own writes (I-HALLOWEEN)"
    );

    let st3 = rig.stmt(&t, s);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    assert_eq!(
        ok(rig.db.get(&st3, &h3, &[i32_v(1)])),
        Some(vec![i32_d(1), text_d("one")]),
        "a later statement of the txn sees its own writes"
    );
    let rows = ok(rig.db.scan(&st3, &h3));
    assert_eq!(rows.len(), 2, "scan sees both rows");
    assert_eq!(ok(rig.db.get(&st3, &h3, &[i32_v(3)])), None);
    ok(rig.commit_rc(t));

    // A fresh txn: everything committed, one row per key.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st4 = rig.stmt(&t2, s2);
    let h4 = ok(rig.db.open_table(&st4, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st4, &h4)).len(), 2);
    assert_eq!(
        ok(rig.db.get(&st4, &h4, &[i32_v(2)])),
        Some(vec![i32_d(2), text_d("two")])
    );
    ok(rig.commit_rc(t2));

    // The primary key is unique across txns: a duplicate insert is 23505.
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st5 = rig.stmt(&t3, s3);
    let h5 = ok(rig.db.open_table(&st5, oid, RelLockMode::RowExclusive));
    let err = rig
        .db
        .insert(&st5, &h5, &[i32_d(1), text_d("again")])
        .expect_err("duplicate pk");
    assert_eq!(err.sqlstate(), "23505");
    rig.abort_txn(t3);
}

/// Mutant N19: the rowid drawn without the statement's ordinal counter (a
/// constant, or a per-call counter) — two inserts of one statement build
/// the same rowid and the second collides (23505). Also kills rowids that
/// repeat across txns (`txn n` dropped) or across boots (`epoch` dropped).
#[test]
fn rowid_table_round_trip_distinct_within_statement_across_txns_and_crash() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"nopk", &rowid_schema()));
    ok(rig.commit_rc(t0));

    // Three inserts in ONE statement: three distinct rowids.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st, &h, &[i32_d(1)]));
    ok(rig.db.insert(&st, &h, &[i32_d(2)]));
    ok(rig.db.insert(&st, &h, &[i32_d(3)]));
    let st2 = rig.stmt(&t, s);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st2, &h2));
    assert_eq!(rows.len(), 3, "one row per insert");
    let keys: BTreeSet<_> = rows.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys.len(), 3, "rowids are distinct within the statement");
    ok(rig.commit_rc(t));

    // A second txn adds two more; all five coexist.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st3, &h3, &[i32_d(4)]));
    ok(rig.db.insert(&st3, &h3, &[i32_d(5)]));
    ok(rig.commit_rc(t2));

    // Crash + reopen (the committed rows were synced): the five survive,
    // and the new epoch's rowids stay distinct from every old one.
    let rig2 = rig.crash_reopen(0);
    assert!(
        rig2.core().epoch() > rig.core().epoch(),
        "the epoch advanced at boot (§7.2)"
    );
    let t3 = rig2.rc();
    let s3 = rig2.core().visible_ts();
    let st4 = rig2.stmt(&t3, s3);
    let h4 = ok(rig2.db.open_table(&st4, oid, RelLockMode::AccessShare));
    let old: BTreeSet<_> = ok(rig2.db.scan(&st4, &h4))
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(old.len(), 5, "the five committed rows survive the crash");
    let st5 = rig2.stmt(&t3, s3);
    let h5 = ok(rig2.db.open_table(&st5, oid, RelLockMode::RowExclusive));
    ok(rig2.db.insert(&st5, &h5, &[i32_d(6)]));
    ok(rig2.commit_rc(t3));
    let t4 = rig2.rc();
    let s4 = rig2.core().visible_ts();
    let st6 = rig2.stmt(&t4, s4);
    let h6 = ok(rig2.db.open_table(&st6, oid, RelLockMode::AccessShare));
    let now: BTreeSet<_> = ok(rig2.db.scan(&st6, &h6))
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(now.len(), 6, "five old rows plus the new one");
    let fresh: Vec<_> = now.difference(&old).collect();
    assert_eq!(
        fresh.len(),
        1,
        "the reopened epoch's rowid is fresh, not a reuse"
    );
    ok(rig2.commit_rc(t4));
}

/// §3: the encoded primary key is limited to 2704 bytes — over it the
/// insert fails 54000, at the limit it succeeds. (A text value of N
/// non-zero bytes encodes to N bytes plus the column's marker byte.)
#[test]
fn pk_over_the_2704_byte_limit_is_54000() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"textpk", &text_pk_schema()));
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st2 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));

    // marker + 2701 text bytes + the text terminator's 2 bytes = 2704:
    // allowed.
    ok(rig
        .db
        .insert(&st2, &h, &[text_d(&"a".repeat(2701)), i32_d(1)]));
    // One byte more: 2705 > 2704 → 54000.
    let err = rig
        .db
        .insert(&st2, &h, &[text_d(&"a".repeat(2702)), i32_d(2)])
        .expect_err("pk over the limit");
    assert_eq!(err.sqlstate(), "54000");
    assert!(matches!(err, DbError::ProgramLimitExceeded));
    ok(rig.commit_rc(t));

    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st3, &h3));
    assert_eq!(rows.len(), 1, "only the in-limit row was written");
    assert_eq!(rows[0].1, vec![text_d(&"a".repeat(2701)), i32_d(1)]);
    ok(rig.commit_rc(t2));
}

/// §4/C-T6 §6: DML on a table with an index is 0A000 until C-T6b2a —
/// never a silent missing-maintenance write. The index def itself is the
/// §6 bytes of [`nucleus_catalog::IndexDef`].
#[test]
fn insert_on_an_indexed_table_is_refused() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let def = nucleus_catalog::IndexDef {
        columns: vec![nucleus_catalog::IndexColumn {
            attnum: 1,
            dir: nucleus_codec::Direction::Asc,
            nulls: nucleus_codec::Nulls::Last,
        }],
        unique: true,
        nulls_not_distinct: false,
        concurrently: false,
        state: nucleus_catalog::IndexState::DeleteOnly,
        built_at: 0,
    };
    let isid = ok(rig.catalog().alloc_storage_id(rig.core()));
    let (_idx_oid, _ranges) = ok(rig.catalog().catalog_create_index(
        &rig.ddl(&t1, s1),
        oid,
        IdxStorage::Id(isid),
        &ok(def.encode()),
    ));
    ok(rig.commit_rc(t1));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st2 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    assert_eq!(h.indexes.len(), 1, "the handle lists the index");
    let err = rig
        .db
        .insert(&st2, &h, &[i32_d(1), text_d("x")])
        .expect_err("DML on an indexed table");
    assert_eq!(err.sqlstate(), "0A000");
    match err {
        DbError::FeatureNotSupported(what) => assert_eq!(what, "index maintenance: C-T6b2a"),
        other => panic!("wrong error: {other:?}"),
    }
    rig.abort_txn(t);
}

/// An index with an explicit entry range ([`IdxStorage::Range`]) has no
/// storage id the typed layer can name: `open_table` refuses it (0A000),
/// and a missing relation is 42P01.
#[test]
fn open_table_refuses_range_storage_and_missing_tables() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let def = ok(nucleus_catalog::IndexDef {
        columns: Vec::new(),
        unique: false,
        nulls_not_distinct: false,
        concurrently: false,
        state: nucleus_catalog::IndexState::DeleteOnly,
        built_at: 0,
    }
    .encode());
    let (_idx_oid, _) = ok(rig.catalog().catalog_create_index(
        &rig.ddl(&t1, s1),
        oid,
        IdxStorage::Range(b"/i/x/".to_vec(), b"/i/x0".to_vec()),
        &def,
    ));
    ok(rig.commit_rc(t1));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st2 = rig.stmt(&t, s);
    let err = rig
        .db
        .open_table(&st2, oid, RelLockMode::AccessShare)
        .expect_err("range storage");
    assert_eq!(err.sqlstate(), "0A000");
    assert!(matches!(err, DbError::FeatureNotSupported(_)));

    let err = rig
        .db
        .open_table(&st2, 999, RelLockMode::AccessShare)
        .expect_err("missing relation");
    assert!(matches!(err, DbError::UndefinedTable));
    assert_eq!(err.sqlstate(), "42P01");
    rig.abort_txn(t);
}

/// The row-key shape itself: a hidden-rowid key is the table prefix plus a
/// `bytea ASC` encoding of the 20-byte id (be32 epoch ‖ be64 n ‖ be32
/// seq0 ‖ be32 ord) — nothing else — and a pk key is the prefix plus the
/// pk's encoded values, in pk order.
#[test]
fn row_keys_carry_the_storage_prefix() {
    use nucleus_catalog::table_lo;
    use nucleus_codec::{encode_key, KeyColumn};

    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st2 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st2, &h, &[i32_d(7), text_d("seven")]));
    ok(rig.commit_rc(t));

    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    let (key, row) = ok(rig.db.scan(&st3, &h3)).remove(0);
    let lo = table_lo(sid);
    assert!(
        key.starts_with(&lo),
        "the row key lives inside the table's storage range"
    );
    let mut expected = lo;
    ok(encode_key(
        &[KeyColumn::asc(nucleus_codec::KeyType::Int4)],
        &[i32_v(7)],
        &mut expected,
    ));
    assert_eq!(key, expected, "pk ASC NULLS LAST in the key");
    assert_eq!(row, vec![i32_d(7), text_d("seven")]);
    ok(rig.commit_rc(t2));

    // And the rowid table's keys are prefix ‖ bytea-encoded 20-byte ids.
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st4 = ok(Stmt::new(&t3, s3));
    let (oid2, sid2) = ok(rig.db.create_table(&st4, b"nopk", &rowid_schema()));
    let tid = t3.id;
    let st5 = rig.stmt(&t3, s3);
    let h5 = ok(rig.db.open_table(&st5, oid2, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st5, &h5, &[i32_d(1)]));
    let st6 = rig.stmt(&t3, s3);
    let h6 = ok(rig.db.open_table(&st6, oid2, RelLockMode::AccessShare));
    let (key, _) = ok(rig.db.scan(&st6, &h6)).remove(0);
    let lo2 = table_lo(sid2);
    assert!(key.starts_with(&lo2));
    // The suffix decodes back to exactly the 20-byte rowid of §3:
    // be32(epoch) be64(txn n) be32(seq0) be32(ord) of the inserting txn
    // and statement.
    let vals = ok(nucleus_codec::decode_key(
        &[KeyColumn::asc(nucleus_codec::KeyType::Bytea)],
        &key[lo2.len()..],
    ));
    match vals.as_slice() {
        [Some(Value::Bytea(id))] => {
            assert_eq!(id.len(), 20, "the 20-byte rowid");
            assert_eq!(
                &id[..4],
                &tid.epoch.to_be_bytes(),
                "the inserting txn's epoch"
            );
            assert_eq!(&id[4..12], &tid.n.to_be_bytes(), "the inserting txn's n");
            assert_eq!(
                &id[12..16],
                &st5.seq0().to_be_bytes(),
                "the statement's seq0"
            );
            assert_eq!(&id[16..], &0u32.to_be_bytes(), "the statement's first ord");
        }
        other => panic!("the rowid key does not decode: {other:?}"),
    }
    // The raw value written under the key is the §2 row format (magic,
    // version, natts), not a bare datum — read after the commit, with the
    // writer's own id at a snapshot above it.
    ok(rig.commit_rc(t3));
    let view = rig.core().open_view();
    let raw = ok(nucleus_txn::read::read_key(
        rig.core(),
        &view,
        &key,
        &nucleus_txn::visibility::ReadCtx {
            txn: tid,
            snapshot: rig.core().visible_ts(),
            stmt_seq: 0,
        },
        &mut nucleus_txn::read::NoSsi,
    ))
    .expect("the raw row bytes");
    assert_eq!(&raw[..2], &[0x52, 0x01], "the §2 row magic");
    let header = ok(nucleus_codec::row::row_header(&raw));
    assert_eq!(header.schema_version, 1);
    assert_eq!(header.natts, 1);
}
