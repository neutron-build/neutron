//! C-T6b2a item 5, the EPQ and entry-delete rules: RC EvalPlanQual
//! computes maintenance from the **EPQ base**, not the snapshot row
//! (mutant N3); an entry delete over a newer tombstone still lands
//! (§5.0's blind rule; mutant N9: the delete driven through `row_op`
//! with the statement ctx would EPQ-skip on the tombstone); an RR
//! delete over an ingested entry version at `S_w + 1` never raises
//! 40001 (mutant N9b: `row_op` would hit the newer-version rule).

mod t6b_support;

use t6b_support::{
    i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, row_key_of, set_col, text_d, text_v,
    u_entry_key, Rig,
};

use nucleus_catalog::{IndexState, Stmt};
use nucleus_codec::row::{ColumnDef, TableSchema};
use nucleus_codec::{Collation, KeyColumn, KeyType};
use nucleus_kv::{Batch, Durability, OrderedKv};
use nucleus_txn::encoding::version_key;
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::Ts;

fn text_col() -> KeyColumn {
    KeyColumn::asc(KeyType::Text(Collation::C))
}

/// `CREATE TABLE t (id int4 PRIMARY KEY, a text, b text)` — the 3-column
/// table the EPQ value-side pin needs (a column a concurrent txn can
/// change that the statement's `set` never touches).
fn wide_schema() -> TableSchema {
    ok(TableSchema::new(
        vec![
            ColumnDef {
                attnum: 1,
                name: "id".into(),
                ty: KeyType::Int4,
                not_null: true,
                dropped: false,
                missing: None,
            },
            ColumnDef {
                attnum: 2,
                name: "a".into(),
                ty: KeyType::Text(Collation::C),
                not_null: false,
                dropped: false,
                missing: None,
            },
            ColumnDef {
                attnum: 3,
                name: "b".into(),
                ty: KeyType::Text(Collation::C),
                not_null: false,
                dropped: false,
                missing: None,
            },
        ],
        vec![1],
    ))
}

/// The raw stored versions (ts desc, header) of one entry logical key.
fn versions_of(rig: &Rig, isid: u64, key: &[u8]) -> Vec<(Option<Ts>, u8)> {
    rig.raw_entry_versions(isid)
        .into_iter()
        .filter(|(k, ..)| k.as_slice() == key)
        .map(|(_, ts, header, _)| (ts, header))
        .collect()
}

/// Mutant N3: maintenance computed from the snapshot row instead of the
/// EPQ base. The RC statement's snapshot sees `(1, a)`; X commits a key
/// change `a → b`; the statement updates `payload := c`: RC EPQ re-applies
/// its set to X's version, so the deleted old entry is `‖b` (the base),
/// never `‖a` (the snapshot row) — under N3, `‖b` stays live forever.
#[test]
fn rc_epq_maintenance_follows_the_epq_base() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // A unique Public index on `payload`.
    let (u_oid, u_sid) = rig.add_index(oid, &idx_def(2, true, IndexState::DeleteOnly));
    rig.set_index_def(u_oid, idx_def(2, true, IndexState::Public));
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);

    // Row (1, a) exists.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h1 = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h1, &[i32_d(1), text_d("a")]));
    let ts_a = ok(rig.commit_rc(t1));

    // The second txn's statement takes its snapshot BEFORE X commits.
    let y = rig.rc();
    let s_y = rig.core().visible_ts();
    let st_y = ok(Stmt::new(&y, s_y));

    // X commits a key change a → b (entries: ‖a tombstoned, ‖b live).
    let x = rig.rc();
    let s_x = rig.core().visible_ts();
    let st_x = rig.stmt(&x, s_x);
    assert!(ok(rig.db.update(
        &st_x,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d("b"))]
    )),);
    let ts_x = ok(rig.commit_rc(x));

    // Y updates payload := c: its read sees (1, a) at s_y, the row op
    // meets X's version above s_y, RC EPQ re-applies the set to it.
    assert!(ok(rig.db.update(
        &st_y,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d("c"))]
    )),);
    let ts_y = ok(rig.commit_rc(y));

    // Entries follow the EPQ base: ‖a tombstoned by X, ‖b tombstoned by
    // Y (the base X left), ‖c live pointing at row 1.
    let u_a = u_entry_key(u_sid, &[text_col()], &[text_v("a".into())]);
    let u_b = u_entry_key(u_sid, &[text_col()], &[text_v("b".into())]);
    let u_c = u_entry_key(u_sid, &[text_col()], &[text_v("c".into())]);
    assert_eq!(
        versions_of(&rig, u_sid, &u_a),
        vec![(Some(ts_x), 0x02), (Some(ts_a), 0x00)],
        "X's key change tombstoned ‖a"
    );
    assert_eq!(
        versions_of(&rig, u_sid, &u_b),
        vec![(Some(ts_y), 0x02), (Some(ts_x), 0x00)],
        "Y deleted the EPQ base's entry ‖b, not the snapshot's ‖a (N3)"
    );
    assert_eq!(
        rig.scan_entries(u_sid),
        vec![(u_c, key1)],
        "only ‖c is live"
    );
}

/// Mutant O7 (M-1): the EPQ re-apply's **value side** — the twin of N3
/// above, which pins only maintenance. §5.2: "compute the update from
/// `v`": the written row is `set` re-applied to the base the row op
/// acted on. A 3-column table: Y's statement takes its snapshot before X
/// commits a change to the column Y's `set` does not touch; Y updates
/// the other column → the committed row carries X's value from the base
/// (under O7, which returns the pre-EPQ `Apply(Update)` value, the
/// written row keeps the snapshot's old column and X's commit is
/// silently lost).
#[test]
fn rc_epq_reapply_carries_a_concurrent_commit_into_the_written_row() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"wide", &wide_schema()));
    ok(rig.commit_rc(t0));

    // Row (1, x, y) exists.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h1 = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig
        .db
        .insert_with(&st1, &h1, &[i32_d(1), text_d("x"), text_d("y")]));
    ok(rig.commit_rc(t1));

    // The statement: RC, its snapshot taken BEFORE X commits.
    let y = rig.rc();
    let s_y = rig.core().visible_ts();
    let st_y = ok(Stmt::new(&y, s_y));

    // X commits b := y2 — a column Y's set never touches.
    let x = rig.rc();
    let s_x = rig.core().visible_ts();
    let st_x = rig.stmt(&x, s_x);
    assert!(ok(rig.db.update(
        &st_x,
        oid,
        &[i32_v(1)],
        &[set_col(3, text_d("y2"))]
    )),);
    ok(rig.commit_rc(x));

    // Y updates a := x2: its read sees (1, x, y) at s_y, the row op
    // meets X's version above s_y, RC EPQ re-applies the set to it.
    assert!(ok(rig.db.update(
        &st_y,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d("x2"))]
    )),);
    ok(rig.commit_rc(y));

    // The committed row is recomputed from the EPQ base: (1, x2, y2) —
    // X's y2 survives (under O7 it would be (1, x2, y)).
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(
        ok(rig.db.get(&st2, &h2, &[i32_v(1)])),
        Some(vec![i32_d(1), text_d("x2"), text_d("y2")]),
        "the written row carries X's commit from the EPQ base (O7)"
    );
    ok(rig.commit_rc(t2));
}

/// Mutant N9 (§5.0's blind entry delete): a DeleteOnly index; the RC
/// statement's snapshot predates X's delete; X deletes pk 1 (its DeleteOnly
/// maintenance tombstones the entry), Y re-inserts (1, a) (DeleteOnly:
/// no entry); the statement deletes pk 1 → `Ok(true)`, and a raw read
/// shows a tombstone on the entry at **the statement's commit ts** — a
/// `row_op`-driven delete would meet X's newer tombstone and skip.
#[test]
fn entry_delete_over_newer_tombstone() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (_do, do_sid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);

    // Row (1, a) predates the index; DeleteOnly never inserted an entry.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h1 = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h1, &[i32_d(1), text_d("a")]));
    ok(rig.commit_rc(t1));

    // The statement: RC, its snapshot taken before X's delete.
    let d = rig.rc();
    let s_d = rig.core().visible_ts();
    let st_d = ok(Stmt::new(&d, s_d));

    // X deletes pk 1 and commits: a blind tombstone on the entry at ts_x.
    let x = rig.rc();
    let s_x = rig.core().visible_ts();
    let st_x = rig.stmt(&x, s_x);
    assert!(ok(rig.db.delete(&st_x, oid, &[i32_v(1)])));
    let ts_x = ok(rig.commit_rc(x));

    // Y re-inserts (1, a) and commits: the row is live again, DeleteOnly
    // still writes no entry.
    let y = rig.rc();
    let s_y = rig.core().visible_ts();
    let st_y = rig.stmt(&y, s_y);
    let h_y = ok(rig.db.open_table(&st_y, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st_y, &h_y, &[i32_d(1), text_d("a")]));
    ok(rig.commit_rc(y));

    // The statement deletes pk 1: the row op EPQs onto Y's live row,
    // applies, and the entry delete lands blindly over X's tombstone.
    assert!(ok(rig.db.delete(&st_d, oid, &[i32_v(1)])));
    let ts_d = ok(rig.commit_rc(d));

    let e_a1 = i_entry_key(do_sid, &[text_col()], &[text_v("a".into())], &key1);
    assert_eq!(
        versions_of(&rig, do_sid, &e_a1),
        vec![(Some(ts_d), 0x02), (Some(ts_x), 0x02)],
        "the statement's tombstone at its own commit ts, over X's (N9)"
    );
}

/// Mutant O12 (M-4): the DELETE path's EPQ base — the twin of N3, which
/// mutates only `UpdateEpq`. `DeleteEpq::recheck` must rebase the
/// maintenance base onto the newest committed version, so the entry
/// delete tombstones the entry of the version the row op acted on. A
/// WriteOnly index; X deletes (1, a); Y re-inserts pk 1 as (1, b) —
/// with a **different** payload, so the base's entry key differs from
/// the snapshot row's; the RC statement (snapshot before X) deletes pk 1
/// → the tombstone lands on `‖b` at the statement's commit ts and no
/// live entry remains (under O12 the tombstone lands on `‖a` again and
/// `‖b` stays live forever pointing at a deleted row).
#[test]
fn rc_delete_epq_rebases_the_maintenance_base() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (wo, wo_sid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    rig.set_index_def(wo, idx_def(2, false, IndexState::WriteOnly));
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);

    // Row (1, a) under the WriteOnly index: entry ‖a live.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h1 = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h1, &[i32_d(1), text_d("a")]));
    let ts_a = ok(rig.commit_rc(t1));

    // The statement: RC, its snapshot taken BEFORE X's delete.
    let d = rig.rc();
    let s_d = rig.core().visible_ts();
    let st_d = ok(Stmt::new(&d, s_d));

    // X deletes pk 1 and commits: a tombstone on the entry ‖a.
    let x = rig.rc();
    let s_x = rig.core().visible_ts();
    let st_x = rig.stmt(&x, s_x);
    assert!(ok(rig.db.delete(&st_x, oid, &[i32_v(1)])));
    let ts_x = ok(rig.commit_rc(x));

    // Y re-inserts pk 1 as (1, b) and commits: the row is live again,
    // and the WriteOnly insert maintained the entry ‖b.
    let y = rig.rc();
    let s_y = rig.core().visible_ts();
    let st_y = rig.stmt(&y, s_y);
    let h_y = ok(rig.db.open_table(&st_y, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st_y, &h_y, &[i32_d(1), text_d("b")]));
    let ts_y = ok(rig.commit_rc(y));

    // The statement deletes pk 1: the row op EPQs onto Y's live (1, b),
    // applies, and the maintenance base is that version — the entry
    // delete tombstones ‖b, the base's entry.
    assert!(ok(rig.db.delete(&st_d, oid, &[i32_v(1)])));
    let ts_d = ok(rig.commit_rc(d));

    let e_a = i_entry_key(wo_sid, &[text_col()], &[text_v("a".into())], &key1);
    let e_b = i_entry_key(wo_sid, &[text_col()], &[text_v("b".into())], &key1);
    assert_eq!(
        versions_of(&rig, wo_sid, &e_b),
        vec![(Some(ts_d), 0x02), (Some(ts_y), 0x00)],
        "the statement's tombstone landed on the EPQ base's entry ‖b at its own commit ts (O12)"
    );
    assert_eq!(
        versions_of(&rig, wo_sid, &e_a),
        vec![(Some(ts_x), 0x02), (Some(ts_a), 0x00)],
        "‖a carries only X's delete tombstone — the statement's went to ‖b, not back to ‖a (O12)"
    );
    assert!(
        rig.scan_entries(wo_sid).is_empty(),
        "no live entry remains (under O12, ‖b would stay live naming a deleted row)"
    );

    // And the row itself is gone.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st2, &h2)).len(), 0);
    ok(rig.commit_rc(t2));
}

/// Mutant N9b (§5.0 again, the RR half): an entry version written raw at
/// `S_w + 1` (the shape §7.6's ingest leaves) while the row version is
/// `<= S_w`; an RR txn whose snapshot is `S_w` deletes the row → the
/// entry delete must not raise 40001 (a `row_op` would meet the entry
/// version above its snapshot and refuse).
#[test]
fn rr_delete_after_ingest_at_s() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // The index starts DeleteOnly (the online ladder's T0), so the row's
    // insert maintains nothing; it is driven to WriteOnly before the RR
    // txn starts, so the only entry version is the raw one below.
    let (wo, wo_sid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));

    // Row (1, a), committed before the RR snapshot.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h1 = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h1, &[i32_d(1), text_d("a")]));
    ok(rig.commit_rc(t1));
    rig.set_index_def(wo, idx_def(2, false, IndexState::WriteOnly));
    assert!(
        rig.raw_entry_versions(wo_sid).is_empty(),
        "the DeleteOnly insert maintained nothing"
    );

    // The RR txn, snapshot S_w = visible_ts (row version ts <= S_w).
    let (r, _guard, s_w) = rig.rr();

    // An ingested entry version, written raw at S_w + 1: stored key
    // `L ‖ 0x01 ‖ be64(MAX-ts) ‖ 0x01`, value header 0x00 (live), empty
    // /i/ payload (§2.2).
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let e_a1 = i_entry_key(wo_sid, &[text_col()], &[text_v("a".into())], &key1);
    let mut batch = Batch::default();
    batch.put(version_key(&e_a1, Ts(s_w.0 + 1)), vec![0x00]);
    ok(<_ as OrderedKv>::write(
        rig.kv.as_ref(),
        batch,
        Durability::No,
    ));

    // The RR delete: no 40001 (N9b), and the blind entry delete resolves
    // over the ingested version at the RR txn's commit ts.
    let st_r = ok(Stmt::new(&r, s_w));
    let deleted = rig.db.delete(&st_r, oid, &[i32_v(1)]);
    assert_eq!(
        deleted,
        Ok(true),
        "an entry version above the snapshot never 40001s the delete (N9b)"
    );
    let ts_r = ok(rig.commit_rc(r));

    assert_eq!(
        versions_of(&rig, wo_sid, &e_a1),
        vec![(Some(ts_r), 0x02)],
        "the resolution's tombstone replaced the ingested entry"
    );

    // The row is gone.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st2, &h2)).len(), 0);
    ok(rig.commit_rc(t2));
}

/// Belt-and-braces for the EPQ error path: an UPDATE whose `set` names an
/// unknown column is 42703 before any write, and a missing row is
/// `Ok(false)` (no maintenance, no error).
#[test]
fn update_set_names_columns_and_missing_rows_are_false() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));
    let (_wo, wo_sid) = rig.add_index(oid, &idx_def(2, false, IndexState::WriteOnly));

    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st2 = rig.stmt(&t1, s1);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st2, &h, &[i32_d(1), text_d("a")]));
    ok(rig.commit_rc(t1));

    // Missing row: Ok(false) for update and delete, nothing maintained.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    // Unknown column: 42703.
    let err = rig
        .db
        .update(&st, oid, &[i32_v(1)], &[set_col(7, text_d("x"))])
        .expect_err("unknown column");
    assert_eq!(err.sqlstate(), "42703");
    // A NULL on a NOT NULL column is 23502 (the id column).
    let err = rig
        .db
        .update(&st, oid, &[i32_v(1)], &[set_col(1, None)])
        .expect_err("NULL pk");
    assert_eq!(err.sqlstate(), "23502");
    rig.abort_txn(t);
    // The failed statements maintained nothing: the only stored entry is
    // the one the committed insert legitimately made.
    assert_eq!(
        rig.raw_entry_versions(wo_sid).len(),
        1,
        "only the insert's entry exists"
    );

    // And a delete of an already-deleted row (a second delete of pk 1
    // after a committed first one) is Ok(false): the read finds nothing.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    assert!(ok(rig.db.delete(&st3, oid, &[i32_v(1)])));
    ok(rig.commit_rc(t2));
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st4 = rig.stmt(&t3, s3);
    assert!(
        !ok(rig.db.delete(&st4, oid, &[i32_v(1)])),
        "the committed delete left nothing to delete"
    );
    rig.abort_txn(t3);
}
