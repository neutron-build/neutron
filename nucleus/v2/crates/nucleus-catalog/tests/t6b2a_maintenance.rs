//! C-T6b2a item 5: §6 maintenance by state, checked end to end —
//! insert / update (key-changing and not) / delete keep WriteOnly and
//! Public entries exact (raw version reads plus an index range read
//! through `read::scan`); a DeleteOnly index gets tombstones on delete
//! and on a key change, and nothing on insert (mutants N8: DeleteOnly
//! treated as WriteOnly; N4: a non-blind entry delete that skips when
//! the entry is absent); a PK change moves entries, `/u/` payloads
//! included; a unique `/u/` insert of a duplicate is 23505; the §6 key
//! limit is 54000 on insert and a skip on delete (mutant N17: the limit
//! applied to deletes).

mod t6b_support;

use t6b_support::{
    i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, row_key_of, set_col, text_d, text_v,
    u_entry_key, Rig,
};

use nucleus_catalog::{DbError, IndexState, Stmt};
use nucleus_codec::{Collation, KeyColumn, KeyType};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::Ts;

fn text_col() -> KeyColumn {
    KeyColumn::asc(KeyType::Text(Collation::C))
}

/// The raw stored versions (ts desc, header byte) of one entry logical
/// key — `0x02` is a tombstone, `0x00`/`0x01` a live entry.
fn versions_of(rig: &Rig, isid: u64, key: &[u8]) -> Vec<(Option<Ts>, u8)> {
    rig.raw_entry_versions(isid)
        .into_iter()
        .filter(|(k, ..)| k.as_slice() == key)
        .map(|(_, ts, header, _)| (ts, header))
        .collect()
}

/// WriteOnly and Public indexes are maintained exactly: inserts insert,
/// a key-changing update moves the entry (tombstone on the old key), a
/// value-identical update touches no entry at all, a delete tombstones.
#[test]
fn writeonly_and_public_entries_stay_exact() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // Two non-unique indexes on `payload` (attnum 2), created DeleteOnly
    // (the online ladder's T0) and driven to WriteOnly / Public.
    let (wo, wo_sid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    let (pu, pu_sid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    rig.set_index_def(wo, idx_def(2, false, IndexState::WriteOnly));
    rig.set_index_def(pu, idx_def(2, false, IndexState::Public));

    // Insert two rows in one statement: two live entries per index.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st, &h, &[i32_d(1), text_d("a")]));
    ok(rig.db.insert(&st, &h, &[i32_d(2), text_d("b")]));
    let ts_insert = ok(rig.commit_rc(t));

    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let key2 = row_key_of(sid, &pk_schema(), &[i32_v(2)]);
    for isid in [wo_sid, pu_sid] {
        let e_a1 = i_entry_key(isid, &[text_col()], &[text_v("a".into())], &key1);
        let e_b2 = i_entry_key(isid, &[text_col()], &[text_v("b".into())], &key2);
        assert_eq!(
            rig.scan_entries(isid),
            vec![(e_a1.clone(), Vec::new()), (e_b2.clone(), Vec::new())],
            "both rows have their entries, in key order"
        );
        assert_eq!(
            versions_of(&rig, isid, &e_a1),
            vec![(Some(ts_insert), 0x00)],
            "exactly one live version per entry"
        );
    }

    // A key-changing update moves the entry: tombstone on ‖a‖key1, live
    // ‖c‖key1 at the update's own commit ts.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    assert!(ok(rig.db.update(
        &st2,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d("c"))]
    )),);
    let ts_update = ok(rig.commit_rc(t2));
    for isid in [wo_sid, pu_sid] {
        let e_a1 = i_entry_key(isid, &[text_col()], &[text_v("a".into())], &key1);
        let e_c1 = i_entry_key(isid, &[text_col()], &[text_v("c".into())], &key1);
        assert_eq!(
            versions_of(&rig, isid, &e_a1),
            vec![(Some(ts_update), 0x02), (Some(ts_insert), 0x00)],
            "the old entry is tombstoned at the update's commit ts"
        );
        assert_eq!(
            versions_of(&rig, isid, &e_c1),
            vec![(Some(ts_update), 0x00)],
            "the new entry is live at the update's commit ts"
        );
        assert_eq!(rig.scan_entries(isid).len(), 2, "still two live entries");
    }

    // A value-identical update changes no entry: exactly one version on
    // ‖b‖key2 afterwards (no tombstone, no re-insert).
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st3 = rig.stmt(&t3, s3);
    assert!(
        ok(rig
            .db
            .update(&st3, oid, &[i32_v(2)], &[set_col(2, text_d("b"))])),
        "the row update applies even though no entry moves"
    );
    ok(rig.commit_rc(t3));
    let e_b2 = i_entry_key(wo_sid, &[text_col()], &[text_v("b".into())], &key2);
    assert_eq!(
        versions_of(&rig, wo_sid, &e_b2).len(),
        1,
        "an unchanged entry gets no new version"
    );

    // Delete row 1: its entry is tombstoned in both indexes.
    let t4 = rig.rc();
    let s4 = rig.core().visible_ts();
    let st4 = rig.stmt(&t4, s4);
    assert!(ok(rig.db.delete(&st4, oid, &[i32_v(1)])));
    let ts_delete = ok(rig.commit_rc(t4));
    for isid in [wo_sid, pu_sid] {
        let e_c1 = i_entry_key(isid, &[text_col()], &[text_v("c".into())], &key1);
        assert_eq!(
            versions_of(&rig, isid, &e_c1),
            vec![(Some(ts_delete), 0x02), (Some(ts_update), 0x00)],
            "the delete's tombstone sorts above the update's live entry"
        );
        assert_eq!(
            rig.scan_entries(isid).len(),
            1,
            "only row 2's entry is left"
        );
    }
}

/// A DeleteOnly index (mutants N4 and N8): an insert writes nothing
/// (N8: DeleteOnly treated as WriteOnly would write entries); a delete
/// and an entry-changing update place blind tombstones on entries that
/// never existed (N4: a non-blind delete that skips when the entry is
/// absent would place none).
#[test]
fn delete_only_gets_tombstones_and_nothing_on_insert() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (_do, do_sid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));

    // Insert: nothing (N8).
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st, &h, &[i32_d(1), text_d("a")]));
    ok(rig.commit_rc(t));
    assert!(
        rig.raw_entry_versions(do_sid).is_empty(),
        "a DeleteOnly index gets no entry from an insert (N8)"
    );

    // Delete: a blind tombstone on the (never-inserted) entry (N4).
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let e_a1 = i_entry_key(do_sid, &[text_col()], &[text_v("a".into())], &key1);
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    assert!(ok(rig.db.delete(&st2, oid, &[i32_v(1)])));
    let ts_del = ok(rig.commit_rc(t2));
    assert_eq!(
        versions_of(&rig, do_sid, &e_a1),
        vec![(Some(ts_del), 0x02)],
        "the delete's tombstone landed although no entry existed (N4)"
    );

    // A fresh row, then an entry-changing update: a tombstone on the old
    // entry of the base version, still no live entry anywhere.
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st3 = rig.stmt(&t3, s3);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st3, &h3, &[i32_d(2), text_d("b")]));
    ok(rig.commit_rc(t3));
    let t4 = rig.rc();
    let s4 = rig.core().visible_ts();
    let st4 = rig.stmt(&t4, s4);
    assert!(ok(rig.db.update(
        &st4,
        oid,
        &[i32_v(2)],
        &[set_col(2, text_d("c"))]
    )),);
    let ts_upd = ok(rig.commit_rc(t4));
    let key2 = row_key_of(sid, &pk_schema(), &[i32_v(2)]);
    let e_b2 = i_entry_key(do_sid, &[text_col()], &[text_v("b".into())], &key2);
    assert_eq!(
        versions_of(&rig, do_sid, &e_b2),
        vec![(Some(ts_upd), 0x02)],
        "the key change tombstoned the old entry of the base version"
    );
    assert!(
        rig.scan_entries(do_sid).is_empty(),
        "a DeleteOnly index never serves a live entry"
    );
}

/// A PK-changing update moves every entry: an `/i/` entry's row-key
/// suffix changes (old key tombstoned, new key live), and a `/u/`
/// entry's **payload** changes (same key, delete + re-insert, resolved
/// to one live version pointing at the new row key).
#[test]
fn pk_change_moves_entries() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // A unique index on `payload`, driven to Public; and a non-unique
    // one, to see the /i/ row-key suffix move.
    let (u_oid, u_sid) = rig.add_index(oid, &idx_def(2, true, IndexState::DeleteOnly));
    rig.set_index_def(u_oid, idx_def(2, true, IndexState::Public));
    let (_i_oid, i_sid) = rig.add_index(oid, &idx_def(2, false, IndexState::WriteOnly));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st, &h, &[i32_d(1), text_d("b")]));
    let ts_insert = ok(rig.commit_rc(t));

    // pk 1 → 5, payload unchanged.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    assert!(ok(rig.db.update(
        &st2,
        oid,
        &[i32_v(1)],
        &[set_col(1, i32_d(5))]
    )),);
    let ts_move = ok(rig.commit_rc(t2));

    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let key5 = row_key_of(sid, &pk_schema(), &[i32_v(5)]);

    // /u/: same key ‖b, payload now names row 5; the delete + re-insert
    // of the same logical key resolve to one live version at the commit
    // (the header's key_changed bit is not significant, §8).
    let u_b = u_entry_key(u_sid, &[text_col()], &[text_v("b".into())]);
    assert_eq!(rig.scan_entries(u_sid), vec![(u_b.clone(), key5.clone())]);
    let u_versions = versions_of(&rig, u_sid, &u_b);
    assert_eq!(u_versions.len(), 2, "the old row's entry then the new one");
    assert_eq!(u_versions[0].0, Some(ts_move));
    assert!(u_versions[0].1 <= 1, "live (key_changed bit ignored, §8)");
    assert_eq!(u_versions[1].0, Some(ts_insert));
    let payloads: Vec<Vec<u8>> = rig
        .raw_entry_versions(u_sid)
        .into_iter()
        .filter(|(k, ..)| k == &u_b)
        .map(|(_, _, _, payload)| payload)
        .collect();
    assert_eq!(
        payloads,
        vec![key5.clone(), key1.clone()],
        "the /u/ payload moved from key1 to key5"
    );

    // /i/: the row-key suffix moved — old key tombstoned, new key live.
    let i_b1 = i_entry_key(i_sid, &[text_col()], &[text_v("b".into())], &key1);
    let i_b5 = i_entry_key(i_sid, &[text_col()], &[text_v("b".into())], &key5);
    assert_eq!(
        versions_of(&rig, i_sid, &i_b1)[0].1,
        0x02,
        "old entry tombstoned"
    );
    assert_eq!(
        versions_of(&rig, i_sid, &i_b5),
        vec![(Some(ts_move), 0x00)],
        "new entry live at the update's commit ts"
    );
    assert_eq!(rig.scan_entries(i_sid), vec![(i_b5.clone(), Vec::new())]);

    // The row itself moved.
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st3 = rig.stmt(&t3, s3);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.get(&st3, &h3, &[i32_v(1)])), None);
    assert_eq!(
        ok(rig.db.get(&st3, &h3, &[i32_v(5)])),
        Some(vec![i32_d(5), text_d("b")])
    );
    ok(rig.commit_rc(t3));
}

/// A unique WriteOnly and a unique Public `/u/` entry: inserting a
/// duplicate key value is 23505 in both states; a NULL key column
/// (NULLS DISTINCT) goes to `/i/` and is never checked (§5.3).
#[test]
fn unique_writeonly_and_public_reject_duplicates() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (u_oid, u_sid) = rig.add_index(oid, &idx_def(2, true, IndexState::DeleteOnly));
    rig.set_index_def(u_oid, idx_def(2, true, IndexState::WriteOnly));

    fn insert_row(rig: &Rig, oid: u64, id: i32, payload: Option<&str>) -> Result<(), DbError> {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        let row = vec![i32_d(id), payload.map(text_d).unwrap_or(None)];
        let r = rig.db.insert(&st, &h, &row);
        if r.is_err() {
            rig.abort_txn(t);
        } else {
            ok(rig.commit_rc(t));
        }
        r
    }

    ok(insert_row(&rig, oid, 1, Some("dup")));

    // WriteOnly: a duplicate is 23505.
    let err = insert_row(&rig, oid, 2, Some("dup")).expect_err("duplicate under WriteOnly");
    assert_eq!(err.sqlstate(), "23505");

    // Public: the same.
    rig.set_index_def(u_oid, idx_def(2, true, IndexState::Public));
    let err = insert_row(&rig, oid, 3, Some("dup")).expect_err("duplicate under Public");
    assert_eq!(err.sqlstate(), "23505");

    // NULL key columns (NULLS DISTINCT) are /i/ entries and unchecked.
    ok(insert_row(&rig, oid, 4, None));
    ok(insert_row(&rig, oid, 5, None));

    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let u_dup = u_entry_key(u_sid, &[text_col()], &[text_v("dup".into())]);
    // `scan_entries` covers both ranges of the storage: exactly one live
    // /u/ entry (the checked one) plus the two unchecked NULL /i/ ones.
    let u_lo = nucleus_txn::catalog::index_ranges(u_sid).remove(0).0;
    let u_entries: Vec<_> = rig
        .scan_entries(u_sid)
        .into_iter()
        .filter(|(k, _)| k.starts_with(&u_lo))
        .collect();
    assert_eq!(
        u_entries,
        vec![(u_dup.clone(), key1)],
        "only the checked /u/ entry serves reads"
    );
    // Two live NULL entries live under the same storage's /i/ range.
    let i_lo = nucleus_txn::catalog::index_ranges(u_sid).remove(1).0;
    let nulls: Vec<_> = rig
        .raw_entry_versions(u_sid)
        .into_iter()
        .filter(|(k, ts, header, _)| ts.is_some() && *header == 0x00 && k.starts_with(&i_lo))
        .collect();
    assert_eq!(nulls.len(), 2, "two unchecked NULL entries under /i/");
}

/// §6 key limit (mutant N17: the limit applied to deletes): a row with a
/// 3000-byte indexed value inserted before the index exists is deletable
/// once the index is WriteOnly (the oversize entry delete is skipped), a
/// fresh insert of another 3000-byte value is 54000.
#[test]
fn oversize_row_deletable_with_writeonly_index() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    let st2 = rig.stmt(&t0, s0);
    let h0 = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig
        .db
        .insert(&st2, &h0, &[i32_d(1), text_d(&"x".repeat(3000))]));
    ok(rig.commit_rc(t0));

    let (o_oid, isid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    rig.set_index_def(o_oid, idx_def(2, false, IndexState::WriteOnly));

    // Delete the oversize row: the entry delete is skipped, the delete
    // itself succeeds (N17 would make this 54000).
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    assert!(ok(rig.db.delete(&st, oid, &[i32_v(1)])));
    ok(rig.commit_rc(t));
    assert!(
        rig.raw_entry_versions(isid).is_empty(),
        "no entry was written or tombstoned"
    );

    // A fresh insert of another 3000-byte value: 54000 (entry insert).
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    let err = rig
        .db
        .insert(&st3, &h, &[i32_d(2), text_d(&"y".repeat(3000))])
        .expect_err("an oversize index key is 54000");
    assert_eq!(err.sqlstate(), "54000");
    rig.abort_txn(t2);

    // And the table is empty: only row 1 ever existed, and it is gone.
    let t4 = rig.rc();
    let s4 = rig.core().visible_ts();
    let st4 = rig.stmt(&t4, s4);
    let h4 = ok(rig.db.open_table(&st4, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st4, &h4)).len(), 0);
    ok(rig.commit_rc(t4));
}
