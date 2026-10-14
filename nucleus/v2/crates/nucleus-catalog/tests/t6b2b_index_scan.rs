//! C-T6b2b items 2 and 3: the verified index scan (C-T6 §4/§6) —
//! `index_scan` equals the filtered `scan` after inserts, updates and
//! deletes (bounds of every `Bound` shape, NULLs included); a `Public`
//! index with `built_at` above the statement's snapshot is refused
//! (mutant N6); a stale raw entry is XX001 whether its row is invisible
//! or its key no longer re-encodes (mutant N21); and §6.1(b)'s own-rows
//! scan runs at `S_e`, never at the statement's older snapshot (mutant
//! N16, the born-public-after-concurrent-delete schedule).
//!
//! C-T6b2bfix follow-ups (gate findings F-1..F-8): the RR snapshot pin
//! (FU-1/M1), the same-txn own-entries pin (FU-2/M13), the §4 RR/SER
//! relation rule in `index_scan` (FU-3), the born-public key limit
//! (FU-4/K-LIM), the DROP-INDEX fence (FU-5) and the `built_at == S`
//! boundary (FU-6/B-EDGE) — plus one ESCALATE-marked reproduction of the
//! fencing gap found while writing FU-5.
//!
//! C-T6b2bfix2 (the gate's binding FU-A): the fencing gap is fixed —
//! `index_scan`'s post-lock catalog reads run at a fresh post-lock
//! `visible_ts()` — and the reproduction is un-ignored, now pinning both
//! during-wait drops: a DROP INDEX is 42704, a DROP TABLE (the ghost
//! pair's `(Some, None)` shape, closed by the fresh post-lock rel read
//! as `open_table` closes it) is 42P01 — neither is ever served.

mod t6b_support;

use std::ops::Bound;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use t6b_support::{
    i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, plant_live_version, raw_entry_trace,
    row_key_of, set_col, text_d, text_v, u_entry_key, wait_until, Rig,
};

use nucleus_catalog::{DbError, IndexDef, IndexState, Stmt, KEY_LIMIT};
use nucleus_codec::row::Datum;
use nucleus_codec::{Collation, KeyColumn, KeyType, Value};
use nucleus_kv::Key;
use nucleus_txn::catalog::{Catalog, IdxStorage};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnError, TxnId};

/// Everything a thread may wait on is bounded by this; the joins use it
/// too (a hang is a failure, not a pass).
const BOUND: Duration = Duration::from_secs(30);

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

// ---- C-T6b2bfix: the gate's follow-ups (FU-1..FU-6) --------------------------

/// `CREATE TABLE t (id int4 PRIMARY KEY, payload text)`, committed.
fn mk_table(rig: &Rig, name: &[u8]) -> (u64, u64) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = ok(Stmt::new(&t, s));
    let r = ok(rig.db.create_table(&st, name, &pk_schema()));
    ok(rig.commit_rc(t));
    r
}

/// An empty pre-existing table that gets one own row and a born-public
/// index in a single committed txn (§6.1 case (b): the row is the txn's
/// own write, invisible to the emptiness scan's ghost reader). Returns
/// `(relation oid, index oid)`.
fn born_index_after_own_row(rig: &Rig, name: &[u8], id: i32, payload: &str) -> (u64, u64) {
    let (oid, _sid) = mk_table(rig, name);
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st, &h, &[i32_d(id), text_d(payload)]));
    let st2 = rig.stmt(&t, s);
    let idx_oid = ok(rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public)));
    ok(rig.commit_rc(t));
    (oid, idx_oid)
}

/// The index rows of relation `oid` at `visible_ts`, through a reader id
/// this process never allocates (the "abort leaves no idx row" oracle —
/// the failing creation never returns its oid to count directly).
fn idx_rows_of(rig: &Rig, oid: u64) -> Vec<nucleus_txn::catalog::IdxRow> {
    let view = rig.core().open_view();
    let ctx = ReadCtx {
        txn: TxnId {
            epoch: rig.core().epoch(),
            n: u64::MAX,
        },
        snapshot: rig.core().visible_ts(),
        stmt_seq: 1,
    };
    ok(Catalog::scan_idxs_of_rel(rig.core(), &view, &ctx, oid))
}

/// FU-1 (F-1's M1): `index_scan`'s read ctx runs at the statement's
/// snapshot, never at `visible_ts` — an RR txn whose snapshot precedes a
/// foreign insert's commit sees exactly its snapshot's rows through the
/// index. Under the mutant the entry scan (and the row fetch) run at
/// `visible_ts` and the post-snapshot row leaks in: two rows, silently.
#[test]
fn index_scan_reads_at_the_rr_snapshot() {
    let rig = Rig::new();
    // One txn: the table, one own row, the born-public index (§6.1 (a)),
    // committed.
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st0, b"rrsnap", &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("a")]));
    let st2 = rig.stmt(&t0, s0);
    let idx_oid = ok(rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public)));
    ok(rig.commit_rc(t0));

    // The RR txn begins and takes its registered snapshot S; its
    // statement exists before the foreign insert commits (§3.1: the
    // caller owns the registration).
    let (rr, guard, s) = rig.rr();
    let st = rig.stmt(&rr, s);

    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st3 = rig.stmt(&t1, s1);
    let h1 = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st3, &h1, &[i32_d(2), text_d("b")]));
    ok(rig.commit_rc(t1));

    // §4: the RR statement reads at S — exactly the one row committed at
    // or below S, never the post-S insert.
    let rows = ok(rig
        .db
        .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(
        rows.len(),
        1,
        "the RR statement sees only its snapshot's row"
    );
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("a")]);
    rig.abort_txn(rr);
    drop(guard);
}

/// FU-2 (F-2's M13): a later statement of the creating txn reads its own
/// born-index entries — the `stmt_seq` half of the own-intent read rule
/// (I-HALLOWEEN's boundary: own writes of *earlier* statements are
/// visible). Under the mutant (`stmt_seq: 0`) the scan misses the txn's
/// own entry: zero rows. Both §6.1 cases, the own-table (a) and the
/// pre-existing (b) twin (insert, savepoint, create, scan).
#[test]
fn index_scan_serves_the_creating_txns_own_entries() {
    let rig = Rig::new();
    // (a) the txn's own relation: CREATE TABLE; INSERT; CREATE INDEX; a
    // later statement of the same txn scans through the index.
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st0, b"ownscan", &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("a")]));
    let st2 = rig.stmt(&t0, s0);
    let idx_oid = ok(rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public)));
    let st3 = rig.stmt(&t0, s0);
    let rows = ok(rig
        .db
        .index_scan(&st3, idx_oid, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(
        rows.len(),
        1,
        "the creating txn's later statement sees its own row"
    );
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("a")]);
    ok(rig.commit_rc(t0));

    // (b) the twin on a pre-existing relation: INSERT; SAVEPOINT; CREATE
    // INDEX; a later statement of the same txn scans (the savepoint
    // between them is the case-(b) shape; nothing rolls back).
    let (oid_b, _sid_b) = mk_table(&rig, b"prescan");
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st = rig.stmt(&t1, s1);
    let hb = ok(rig.db.open_table(&st, oid_b, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st, &hb, &[i32_d(1), text_d("k")]));
    let _sp = ok(t1.savepoint());
    let st2 = rig.stmt(&t1, s1);
    let idx_b = ok(rig
        .db
        .create_index(&st2, oid_b, &idx_def(2, false, IndexState::Public)));
    let st3 = rig.stmt(&t1, s1);
    let rows = ok(rig
        .db
        .index_scan(&st3, idx_b, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows.len(), 1, "the case-(b) twin sees its own row too");
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("k")]);
    ok(rig.commit_rc(t1));
}

/// FU-3 (F-3): `index_scan` runs §4's RR/SER relation rule, as
/// `open_table` runs it for every other relation access — the relation
/// row read at the statement's snapshot and at the latest committed
/// state, a storage-id difference (a foreign TRUNCATE committed after
/// the snapshot) is 40001, never a silent serve of pre-rewrite data.
/// The typed layer has no TRUNCATE yet, so the rewrite is driven through
/// the catalog-level DDL (`catalog_truncate_table`: new table storage
/// id, the Id-storage index re-allocated with it).
#[test]
fn rr_index_scan_refuses_a_relation_rewritten_after_its_snapshot() {
    let rig = Rig::new();
    let (oid, idx_oid) = born_index_after_own_row(&rig, b"rrtrunc", 1, "a");

    // The RR txn's snapshot S precedes the foreign rewrite.
    let (rr, guard, s) = rig.rr();
    let st = rig.stmt(&rr, s);

    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    ok(rig.catalog().catalog_truncate_table(&rig.ddl(&t1, s1), oid));
    ok(rig.commit_rc(t1));

    // §4: the storage id moved after S → 40001 (mutant FU3-DEL deletes
    // the check and the scan serves the rewrite's empty new index
    // instead).
    let err = rig
        .db
        .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded)
        .expect_err("a relation rewritten after the snapshot is refused");
    assert!(matches!(err, DbError::Txn(TxnError::SerializationFailure)));
    assert_eq!(err.sqlstate(), "40001");

    // The same rule every other relation access runs: `open_table` on
    // the same statement agrees.
    let err = rig
        .db
        .open_table(&st, oid, RelLockMode::AccessShare)
        .expect_err("open_table refuses the rewritten relation too");
    assert_eq!(err.sqlstate(), "40001");
    rig.abort_txn(rr);
    drop(guard);
}

/// FU-4 (F-4's K-LIM): §6's born-public key limit at the creation's
/// own-rows call site — an own row whose encoded entry key exceeds 2704
/// bytes fails the creation with 54000 and the abort leaves no idx row
/// and no entry anywhere. An exactly-at-limit key still creates (the
/// limit is a `>` bound).
#[test]
fn born_public_creation_enforces_the_key_limit() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"keylim");

    // A text of KEY_LIMIT - 2 bytes encodes (marker + bytes + the 2-byte
    // terminator) to KEY_LIMIT + 1: over the §6 limit.
    let over = "a".repeat(KEY_LIMIT - 2);
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st, &h, &[i32_d(1), text_d(&over)]));
    let st2 = rig.stmt(&t, s);
    let err = rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public))
        .expect_err("the over-limit own row refuses the creation");
    assert!(matches!(err, DbError::ProgramLimitExceeded));
    assert_eq!(err.sqlstate(), "54000");
    rig.abort_txn(t);
    assert!(
        raw_entry_trace(&rig).is_empty(),
        "no entry of the refused creation survived"
    );
    assert!(
        idx_rows_of(&rig, oid).is_empty(),
        "no idx row of the refused creation survived"
    );

    // Exactly at the limit (KEY_LIMIT - 3 bytes encode to KEY_LIMIT):
    // the creation succeeds and the entry is served.
    let fits = "a".repeat(KEY_LIMIT - 3);
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h2 = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st3, &h2, &[i32_d(2), text_d(&fits)]));
    let st4 = rig.stmt(&t2, s2);
    let idx_oid = ok(rig
        .db
        .create_index(&st4, oid, &idx_def(2, false, IndexState::Public)));
    let st5 = rig.stmt(&t2, s2);
    let rows = ok(rig
        .db
        .index_scan(&st5, idx_oid, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows.len(), 1, "the at-limit entry is created and served");
    ok(rig.commit_rc(t2));
}

/// FU-5 (F-5/F-6): the DROP-INDEX fence, threaded and bounded (the N20
/// pattern). A DROP INDEX holds the owner's AccessExclusive — the scan
/// parks at its AccessShare and completes only after the DDL ends (the
/// lock is what orders reader and DDL; a scan that discarded it would
/// never park, caught by the bounded wait), and an aborted drop leaves a
/// live index the scan then serves. A **committed** DROP INDEX is never
/// served: a later scan reports 42704.
#[test]
fn index_scan_fences_against_a_drop_index() {
    let rig = Arc::new(Rig::new());
    let (_oid, idx_oid) = born_index_after_own_row(&rig, b"dropix", 1, "a");

    // (1) The dropper holds AccessExclusive with its txn open; the scan
    // must park and must not complete under it.
    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_b = Arc::clone(&rig);
    let b = thread::spawn(move || {
        let t = rig_b.rc();
        let s = rig_b.core().visible_ts();
        ok(rig_b
            .catalog()
            .catalog_drop_index(&rig_b.ddl(&t, s), idx_oid));
        held_tx.send(()).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        rig_b.abort_txn(t);
    });
    held_rx
        .recv_timeout(BOUND)
        .expect("the dropper holds AccessExclusive");

    let (res_tx, res_rx) = mpsc::channel::<Result<usize, String>>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        let st = rig_a.stmt(&t, s);
        let r = rig_a
            .db
            .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded);
        let out = match r {
            Ok(rows) => Ok(rows.len()),
            Err(e) => Err(e.sqlstate().to_string()),
        };
        res_tx.send(out).expect("res signal");
        rig_a.abort_txn(t);
    });
    wait_until(BOUND, || !rig.wait_edges().is_empty());
    assert!(
        res_rx.try_recv().is_err(),
        "the scan must not complete under the DDL's AccessExclusive"
    );

    // The dropper aborts: the scan wakes and serves the still-live
    // index.
    release_tx.send(()).expect("release");
    match res_rx.recv_timeout(BOUND).expect("the scan completed") {
        Ok(n) => assert_eq!(n, 1, "the aborted drop leaves a live index, served"),
        Err(sqlstate) => panic!("an aborted drop must not fail the scan: {sqlstate}"),
    }
    b.join().expect("dropper join");
    a.join().expect("scanner join");

    // (2) A committed DROP INDEX is never served: the scan's unlocked
    // owner read already misses the row → 42704.
    let d = rig.rc();
    let sd = rig.core().visible_ts();
    ok(rig.catalog().catalog_drop_index(&rig.ddl(&d, sd), idx_oid));
    ok(rig.commit_rc(d));
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let err = rig
        .db
        .index_scan(&st2, idx_oid, Bound::Unbounded, Bound::Unbounded)
        .expect_err("a dropped index is not served");
    assert_eq!(err.sqlstate(), "42704");
    ok(rig.commit_rc(t2));
}

/// FU-6 (F-8's M3): the `built_at == S` boundary of §4's servability
/// rule — after a case-(b) creation commits (born at `S_e`), a statement
/// whose snapshot is **exactly** `built_at` (`register_at`, §3.1's AS OF
/// primitive) is served, not refused (the refusal bound is `built_at >
/// S`; flipping it to `>=` — the mutant — refuses a legal read).
#[test]
fn built_at_equal_to_the_snapshot_is_served() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"edge");

    // Born public on the empty pre-existing table (case (b)).
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = rig.stmt(&t0, s0);
    let idx_oid = ok(rig
        .db
        .create_index(&st0, oid, &idx_def(2, false, IndexState::Public)));
    ok(rig.commit_rc(t0));
    let row = rig.idx_row(idx_oid).expect("the idx row committed");
    let def = ok(IndexDef::decode(&row.def));
    assert_eq!(def.state, IndexState::Public);
    let built_at = def.built_at;
    assert!(built_at > 0, "case (b): born at its own S_e");

    // A row committed after the creation (its entry is invisible at
    // `built_at`).
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("a")]));
    ok(rig.commit_rc(t1));

    // The boundary statement: snapshot exactly `built_at`.
    let t2 = rig.rc();
    let guard = ok(rig.core().registry.register_at(Ts(built_at)));
    let st2 = ok(Stmt::new(&t2, Ts(built_at)));
    let rows = ok(rig
        .db
        .index_scan(&st2, idx_oid, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(
        rows.len(),
        0,
        "the boundary snapshot is served — and sees the table as it was at built_at"
    );
    ok(rig.commit_rc(t2));
    drop(guard);

    // A present statement sees the later row (the read really ran).
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st3 = rig.stmt(&t3, s3);
    let rows = ok(rig
        .db
        .index_scan(&st3, idx_oid, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows.len(), 1);
    ok(rig.commit_rc(t3));
}

/// C-T6b2bfix2 (the C-T6b2bfix gate's F-1, its binding FU-A): the
/// fencing gap the FU-5 work found, fixed and pinned. `index_scan`'s
/// post-lock catalog reads run at a fresh post-lock `visible_ts()` (the
/// pre-lock read only resolves the owner to lock), so a DROP INDEX that
/// COMMITS while the scanner waits for its AccessShare is reported as
/// 42704 — never served at the pre-drop snapshot. The second leg is the
/// gate's F-5 `(Some, None)` ghost-pair shape: a DROP TABLE committing
/// under the same wait leaves the relation row present at the RR
/// statement's snapshot and absent at the latest committed state, and
/// the fresh post-lock rel read closes the shape as 42P01 — exactly as
/// `open_table` closes it — where the defect's stale pre-lock ctx
/// served the dropped table's index.
#[test]
fn drop_committing_during_the_scan_wait_is_not_served() {
    let rig = Arc::new(Rig::new());
    let (_oid, idx_oid) = born_index_after_own_row(&rig, b"escl", 1, "a");

    // The dropper holds AccessExclusive; the scanner parks; the drop
    // then COMMITS under the wait.
    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_b = Arc::clone(&rig);
    let b = thread::spawn(move || {
        let t = rig_b.rc();
        let s = rig_b.core().visible_ts();
        ok(rig_b
            .catalog()
            .catalog_drop_index(&rig_b.ddl(&t, s), idx_oid));
        held_tx.send(()).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        ok(rig_b.commit_rc(t));
    });
    held_rx
        .recv_timeout(BOUND)
        .expect("the dropper holds AccessExclusive");

    let (res_tx, res_rx) = mpsc::channel::<Result<usize, String>>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        let st = rig_a.stmt(&t, s);
        let r = rig_a
            .db
            .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded);
        let out = match r {
            Ok(rows) => Ok(rows.len()),
            Err(e) => Err(e.sqlstate().to_string()),
        };
        res_tx.send(out).expect("res signal");
        rig_a.abort_txn(t);
    });
    wait_until(BOUND, || !rig.wait_edges().is_empty());
    release_tx.send(()).expect("release");

    // §4: the committed drop must be seen → 42704, never a serve.
    match res_rx.recv_timeout(BOUND).expect("the scan completed") {
        Err(sqlstate) => assert_eq!(sqlstate, "42704", "the committed drop is reported"),
        Ok(n) => panic!("the dropped index was served: {n} row(s)"),
    }
    b.join().expect("dropper join");
    a.join().expect("scanner join");

    // F-5: the `(Some, None)` ghost-pair shape — a DROP TABLE commits
    // under the same wait. The relation row is present at the RR
    // statement's snapshot and absent at the latest committed state;
    // the fresh post-lock rel read closes the shape as 42P01 (exactly
    // as `open_table` closes it), where the defect's stale pre-lock ctx
    // served the dropped table's index. The re-read running at a stale
    // ctx (REL-STALE) would reach the fresh idx re-read instead and
    // report 42704 — the wrong error for a gone relation.
    let (oid2, idx_oid2) = born_index_after_own_row(&rig, b"escl2", 1, "a");
    let (held2_tx, held2_rx) = mpsc::channel::<()>();
    let (release2_tx, release2_rx) = mpsc::channel::<()>();
    let rig_b2 = Arc::clone(&rig);
    let b2 = thread::spawn(move || {
        let t = rig_b2.rc();
        let s = rig_b2.core().visible_ts();
        ok(rig_b2
            .catalog()
            .catalog_drop_table(&rig_b2.ddl(&t, s), oid2));
        held2_tx.send(()).expect("held signal");
        release2_rx.recv_timeout(BOUND).expect("release signal");
        ok(rig_b2.commit_rc(t));
    });
    held2_rx
        .recv_timeout(BOUND)
        .expect("the dropper holds AccessExclusive");

    let (res2_tx, res2_rx) = mpsc::channel::<Result<usize, String>>();
    let rig_a2 = Arc::clone(&rig);
    let a2 = thread::spawn(move || {
        let (t, guard, s) = rig_a2.rr();
        let st = rig_a2.stmt(&t, s);
        let r = rig_a2
            .db
            .index_scan(&st, idx_oid2, Bound::Unbounded, Bound::Unbounded);
        let out = match r {
            Ok(rows) => Ok(rows.len()),
            Err(e) => Err(e.sqlstate().to_string()),
        };
        res2_tx.send(out).expect("res signal");
        rig_a2.abort_txn(t);
        drop(guard);
    });
    wait_until(BOUND, || !rig.wait_edges().is_empty());
    release2_tx.send(()).expect("release");

    // §4: the committed drop of the owning table must be seen → 42P01
    // (the `(Some, None)` shape is closed by the fresh post-lock read),
    // never a serve.
    match res2_rx.recv_timeout(BOUND).expect("the scan completed") {
        Err(sqlstate) => assert_eq!(sqlstate, "42P01", "the dropped table is reported"),
        Ok(n) => panic!("the dropped table's index was served: {n} row(s)"),
    }
    b2.join().expect("dropper join");
    a2.join().expect("scanner join");
}
