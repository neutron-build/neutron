//! C-T6b2b items 2 and 3: the verified index scan (C-T6 §4/§6) —
//! `index_scan` equals the filtered `scan` after inserts, updates and
//! deletes (bounds of every `Bound` shape, NULLs included); a `Public`
//! index with `built_at` above the statement's snapshot is refused
//! (mutant N6); a stale raw entry is XX001 whether its row is invisible
//! or its key no longer re-encodes (mutant N21); and §6.1(b)'s own-rows
//! scan runs at `S_e`, never at the statement's older snapshot (mutant
//! N16, the born-public-after-concurrent-delete schedule).

mod t6b_support;

use std::ops::Bound;

use t6b_support::{
    i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, plant_live_version, row_key_of, set_col,
    text_d, text_v, u_entry_key, Rig,
};

use nucleus_catalog::{DbError, IndexState, Stmt};
use nucleus_codec::row::Datum;
use nucleus_codec::{Collation, KeyColumn, KeyType, Value};
use nucleus_kv::Key;
use nucleus_txn::catalog::IdxStorage;
use nucleus_txn::locks::RelLockMode;

fn text_col() -> KeyColumn {
    KeyColumn::asc(KeyType::Text(Collation::C))
}

/// The storage id of a committed index row.
fn isid_of(rig: &Rig, idx_oid: u64) -> u64 {
    let row = rig.idx_row(idx_oid).expect("idx row committed");
    let IdxStorage::Id(isid) = row.storage else {
        panic!("a born-public index has an id storage")
    };
    isid
}

/// The SQL order key of a payload value under text ASC NULLS LAST (the
/// oracle's comparator — byte order for C collation, NULLs last),
/// independent of the source under test's encoding.
fn order_key(v: &Option<Value>) -> (u8, Vec<u8>) {
    match v {
        None => (1, Vec::new()),
        Some(Value::Text(s)) => (0, s.as_bytes().to_vec()),
        other => panic!("unexpected bound value: {other:?}"),
    }
}

fn datum_order_key(v: &Option<Datum>) -> (u8, Vec<u8>) {
    match v {
        None => (1, Vec::new()),
        Some(Datum::Inline(Value::Text(s))) => (0, s.as_bytes().to_vec()),
        other => panic!("unexpected payload datum: {other:?}"),
    }
}

/// The oracle: the table rows (sorted by row key) whose payload lies in
/// `[lo, hi)` — what a correct `index_scan` must return for those bounds.
fn expected(
    rows: &[(Key, Vec<Option<Datum>>)],
    lo: &Bound<Vec<Option<Value>>>,
    hi: &Bound<Vec<Option<Value>>>,
) -> Vec<(Key, Vec<Option<Datum>>)> {
    let bound0 = |b: &Vec<Option<Value>>| order_key(b.first().expect("one bound value"));
    let mut out: Vec<_> = rows
        .iter()
        .filter(|(_, live)| {
            let k = datum_order_key(live.get(1).expect("payload datum"));
            let ge = match lo {
                Bound::Included(b) => k >= bound0(b),
                Bound::Excluded(b) => k > bound0(b),
                Bound::Unbounded => true,
            };
            let le = match hi {
                Bound::Included(b) => k <= bound0(b),
                Bound::Excluded(b) => k < bound0(b),
                Bound::Unbounded => true,
            };
            ge && le
        })
        .cloned()
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// `index_scan` over `[lo, hi)`, sorted by row key (the scan itself
/// returns index order; both orders carry the same rows).
fn got_sorted(
    rig: &Rig,
    idx_oid: u64,
    lo: Bound<Vec<Option<Value>>>,
    hi: Bound<Vec<Option<Value>>>,
) -> Vec<(Key, Vec<Option<Datum>>)> {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let mut rows = ok(rig.db.index_scan(&st, idx_oid, lo, hi));
    ok(rig.commit_rc(t));
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn b_incl(v: &str) -> Bound<Vec<Option<Value>>> {
    Bound::Included(vec![text_v(v.to_string())])
}

fn b_excl(v: &str) -> Bound<Vec<Option<Value>>> {
    Bound::Excluded(vec![text_v(v.to_string())])
}

/// The equality oracle (§6/§4): after inserts, entry-changing and
/// value-identical updates, and deletes, `index_scan` returns exactly the
/// live rows — NULL payloads included, through their `/i/` entries — for
/// the full range and for every bound shape (an excluded lower bound
/// drops the entries equal to it; an included upper bound keeps them; a
/// NULL bound compares NULLS LAST).
#[test]
fn index_scan_equals_the_filtered_scan() {
    let rig = Rig::new();
    // One txn: the table, three own rows, then the born-public index
    // (§6.1 case (a): the own rows are indexed by the creation itself).
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st0, b"eq", &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("b")]));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(2), text_d("a")]));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(3), None]));
    let st2 = rig.stmt(&t0, s0);
    let idx_oid = ok(rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public)));
    ok(rig.commit_rc(t0));

    // Maintenance phases, each in its own txn: two inserts (one NULL), a
    // key-changing update into another row's value, an update into NULL,
    // a delete, one more insert.
    let phase = |rig: &Rig, f: &dyn Fn(&Rig, &Stmt<'_>)| {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        f(rig, &st);
        ok(rig.commit_rc(t));
    };
    phase(&rig, &|rig, st| {
        let h = ok(rig.db.open_table(st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(st, &h, &[i32_d(4), text_d("c")]));
        ok(rig.db.insert_with(st, &h, &[i32_d(5), None]));
    });
    phase(&rig, &|rig, st| {
        assert!(ok(rig.db.update(
            st,
            oid,
            &[i32_v(1)],
            &[set_col(2, text_d("a"))]
        )));
    });
    phase(&rig, &|rig, st| {
        assert!(ok(rig.db.update(st, oid, &[i32_v(2)], &[set_col(2, None)])));
    });
    phase(&rig, &|rig, st| {
        assert!(ok(rig.db.delete(st, oid, &[i32_v(3)])));
    });
    phase(&rig, &|rig, st| {
        let h = ok(rig.db.open_table(st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(st, &h, &[i32_d(6), text_d("b")]));
    });

    // The live table at this point: 1→"a", 2→NULL, 4→"c", 5→NULL, 6→"b".
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    let mut scan_rows = ok(rig.db.scan(&st, &h));
    ok(rig.commit_rc(t));
    scan_rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(scan_rows.len(), 5, "the fixture rows are as intended");
    let rows_ref = &scan_rows;

    // The full range: every live row, in either order.
    assert_eq!(
        got_sorted(&rig, idx_oid, Bound::Unbounded, Bound::Unbounded),
        expected(rows_ref, &Bound::Unbounded, &Bound::Unbounded),
        "the full index scan equals the table scan"
    );
    // ["a", "c"): the "a" and "b" rows.
    assert_eq!(
        got_sorted(&rig, idx_oid, b_incl("a"), b_excl("c")),
        expected(rows_ref, &b_incl("a"), &b_excl("c"))
    );
    // ["b", "c"]: both ends included.
    assert_eq!(
        got_sorted(&rig, idx_oid, b_incl("b"), b_incl("c")),
        expected(rows_ref, &b_incl("b"), &b_incl("c"))
    );
    // ("a", ∞): everything above "a", NULLs last included.
    assert_eq!(
        got_sorted(&rig, idx_oid, b_excl("a"), Bound::Unbounded),
        expected(rows_ref, &b_excl("a"), &Bound::Unbounded)
    );
    // (-∞, NULL): a NULL upper bound excludes the NULL payloads.
    let null_hi = Bound::Excluded(vec![None]);
    assert_eq!(
        got_sorted(&rig, idx_oid, Bound::Unbounded, null_hi.clone()),
        expected(rows_ref, &Bound::Unbounded, &null_hi)
    );
    // ["c", "c"]: exactly the "c" row.
    assert_eq!(
        got_sorted(&rig, idx_oid, b_incl("c"), b_incl("c")),
        expected(rows_ref, &b_incl("c"), &b_incl("c"))
    );
}

/// §4: an index serves a read at `S` iff `Public` and `built_at <= S`
/// (mutant N6: an `index_scan` that ignores `built_at` would serve the
/// index). The `built_at > S` state is produced the way the card names:
/// `catalog_alter_idx_def` rewriting the def.
#[test]
fn index_not_usable_at_a_snapshot_below_built_at() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st0, b"ba", &pk_schema()));
    ok(rig.commit_rc(t0));

    // Born public on the empty pre-existing table (case (b)).
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let idx_oid = ok(rig
        .db
        .create_index(&st1, oid, &idx_def(2, false, IndexState::Public)));
    ok(rig.commit_rc(t1));

    // Rewrite the def to Public with a built_at above every snapshot this
    // test can take (the alter's own commit advances visible_ts by 1).
    let mut future = idx_def(2, false, IndexState::Public);
    future.built_at = rig.core().visible_ts().0 + 100;
    rig.set_index_def(idx_oid, future);

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let err = rig
        .db
        .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded)
        .expect_err("an index above the snapshot is not usable");
    assert!(matches!(
        err,
        DbError::FeatureNotSupported("index not usable at snapshot")
    ));
    assert_eq!(err.sqlstate(), "0A000");
    ok(rig.commit_rc(t));

    // A non-Public index is refused the same way.
    rig.set_index_def(idx_oid, idx_def(2, false, IndexState::WriteOnly));
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let err = rig
        .db
        .index_scan(&st2, idx_oid, Bound::Unbounded, Bound::Unbounded)
        .expect_err("a WriteOnly index serves no read");
    assert!(matches!(
        err,
        DbError::FeatureNotSupported("index not usable at snapshot")
    ));
    ok(rig.commit_rc(t2));
}

/// §6's `index_scan` verification (mutant N21): every fetched row is
/// checked — an entry whose row is not visible at the snapshot, and an
/// entry whose key does not re-encode from its visible row, are both
/// XX001, never a silently wrong result. The stale entries are planted
/// raw (`ingest_sorted`), no maintenance wrote them; each mechanism gets
/// its own index so it is observed alone.
#[test]
fn stale_entry_fetch_is_xx001() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"stale", &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("a")]));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(2), text_d("b")]));
    let st2 = rig.stmt(&t0, s0);
    let ix_a = ok(rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public)));
    let st3 = rig.stmt(&t0, s0);
    let ix_b = ok(rig
        .db
        .create_index(&st3, oid, &idx_def(2, true, IndexState::Public)));
    ok(rig.commit_rc(t0));

    // Delete row 1: both of its entries are tombstoned.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st4 = rig.stmt(&t1, s1);
    assert!(ok(rig.db.delete(&st4, oid, &[i32_v(1)])));
    ok(rig.commit_rc(t1));

    let row1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let row2 = row_key_of(sid, &pk_schema(), &[i32_v(2)]);

    // (a) An entry naming a row that is not visible at the snapshot: a
    // live entry at row 1's old `/i/` slot, planted at the newest ts so
    // it sorts above the delete's tombstone.
    let isid_a = isid_of(&rig, ix_a);
    let e_a = i_entry_key(isid_a, &[text_col()], &[text_v("a".into())], &row1);
    plant_live_version(&rig, &e_a, rig.core().visible_ts(), &[]);
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let err = rig
        .db
        .index_scan(&st, ix_a, Bound::Unbounded, Bound::Unbounded)
        .expect_err("an entry over a deleted row is corrupt");
    assert!(matches!(
        err,
        DbError::Row(nucleus_codec::row::RowError::Corrupt(_))
    ));
    assert_eq!(err.sqlstate(), "XX001");
    ok(rig.commit_rc(t));

    // (b) An entry whose row is visible but whose key does not re-encode
    // from it: a `/u/` entry at "zzz" naming row 2 (payload "b").
    let isid_b = isid_of(&rig, ix_b);
    let e_b = u_entry_key(isid_b, &[text_col()], &[text_v("zzz".into())]);
    plant_live_version(&rig, &e_b, rig.core().visible_ts(), &row2);
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st5 = rig.stmt(&t2, s2);
    let err = rig
        .db
        .index_scan(&st5, ix_b, Bound::Unbounded, Bound::Unbounded)
        .expect_err("an entry that does not re-encode from its row is corrupt");
    assert!(matches!(
        err,
        DbError::Row(nucleus_codec::row::RowError::Corrupt(_))
    ));
    assert_eq!(err.sqlstate(), "XX001");
    ok(rig.commit_rc(t2));
}

/// §6.1 case (b) names the own-rows scan's snapshot exactly: `S_e`,
/// registered after the Share lock — never the statement's (possibly
/// older) snapshot (mutant N16). Schedule: the RC statement's snapshot is
/// taken; a foreign txn deletes the last row `payload = 'k'` and commits;
/// the statement creates a unique index on `payload` → born public over
/// the now-empty-at-`S_e` table, with no `/u/` entry for 'k' (checked by
/// raw read), so a later insert of 'k' succeeds.
#[test]
fn born_public_after_concurrent_delete() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st0, b"del", &pk_schema()));
    ok(rig.commit_rc(t0));

    // The last row, payload 'k'.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("k")]));
    ok(rig.commit_rc(t1));

    // The creating statement exists (its snapshot taken) before the
    // foreign delete commits.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let creator = ok(Stmt::new(&t2, s2));

    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st3 = rig.stmt(&t3, s3);
    assert!(ok(rig.db.delete(&st3, oid, &[i32_v(1)])));
    ok(rig.commit_rc(t3));

    // Born public: the emptiness check at S_e sees the delete, so the
    // build is allowed and indexes nothing.
    let idx_oid = ok(rig
        .db
        .create_index(&creator, oid, &idx_def(2, true, IndexState::Public)));
    ok(rig.commit_rc(t2));

    // Amcheck-free: no /u/ entry for 'k' (no entry at all) by raw read.
    let isid = isid_of(&rig, idx_oid);
    let u_lo = nucleus_txn::catalog::index_ranges(isid).remove(0).0;
    assert!(
        rig.raw_entry_versions(isid)
            .iter()
            .all(|(k, ..)| !k.starts_with(&u_lo)),
        "no /u/ entry survived the concurrent delete"
    );

    // A later insert of 'k' succeeds (no stale unique entry), and the
    // index serves it.
    let t4 = rig.rc();
    let s4 = rig.core().visible_ts();
    let st4 = rig.stmt(&t4, s4);
    let h4 = ok(rig.db.open_table(&st4, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st4, &h4, &[i32_d(2), text_d("k")]));
    ok(rig.commit_rc(t4));

    let t5 = rig.rc();
    let s5 = rig.core().visible_ts();
    let st5 = rig.stmt(&t5, s5);
    let rows = ok(rig
        .db
        .index_scan(&st5, idx_oid, Bound::Unbounded, Bound::Unbounded));
    ok(rig.commit_rc(t5));
    assert_eq!(rows.len(), 1, "the new row is the only entry");
    assert_eq!(rows[0].1, vec![i32_d(2), text_d("k")]);
}
