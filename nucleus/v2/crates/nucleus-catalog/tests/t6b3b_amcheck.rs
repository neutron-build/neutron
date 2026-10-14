//! C-T6b3b item 2, amcheck half: C-T6 §8 at a registered snapshot —
//! clean after a mixed history (inserts, key-changing updates, deletes,
//! a delete + re-insert in one txn — the resolved `/u/` entry carries
//! header `0x01`, so this is mutant N14's catcher — and a born-public
//! unique index), and a violation for each injection: a missing entry,
//! an extra entry (Public and WriteOnly — a WriteOnly index may also
//! miss entries legally), a visible entry under a DeleteOnly index
//! (mutant N22's catcher), an undecodable row, a row with a
//! `schema_version` ahead of the def, duplicate unique keys across two
//! visible rows (I-UNIQUE), and the catalog read at `at` and not the
//! latest (mutant N23's catcher: a def change committed after `at`
//! must not change the verdict taken at `at`). Raw versions are planted
//! through `Core::write` (`core_plant_version`).

mod t6b_support;

use t6b_support::{
    core_plant_version, i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, row_key_of,
    rowid_schema, text_d, text_v, u_entry_key, Rig,
};

use nucleus_catalog::amcheck::{check_table, Report};
use nucleus_catalog::{IndexState, Stmt};
use nucleus_codec::{Collation, KeyColumn, KeyType};
use nucleus_txn::catalog::IdxStorage;
use nucleus_txn::encoding::{HEADER_LIVE, HEADER_TOMBSTONE};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::Ts;

fn text_col() -> KeyColumn {
    KeyColumn::asc(KeyType::Text(Collation::C))
}

/// The committed storage id of index `idx_oid`.
fn isid_of(rig: &Rig, idx_oid: u64) -> u64 {
    let row = rig.idx_row(idx_oid).expect("idx row committed");
    let IdxStorage::Id(isid) = row.storage else {
        panic!("a catalog-created index has an id storage")
    };
    isid
}

/// One committed row `(id, payload)` in one txn of its own.
fn add_row(rig: &Rig, oid: u64, id: i32, payload: &str) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st, &h, &[i32_d(id), text_d(payload)]));
    ok(rig.commit_rc(t));
}

/// amcheck at a fresh snapshot of `rig`.
fn check_now(rig: &Rig, oid: u64) -> Report {
    let guard = rig.core().registry.take_snapshot();
    ok(check_table(&rig.db, oid, &guard))
}

/// §8 clean after a mixed history: a born-public unique index (§6.1 case
/// (a), `built_at = 0`) over inserts, key-changing updates and deletes;
/// a ladder index driven DeleteOnly → WriteOnly (a row inserted while
/// DeleteOnly legally misses its entry under WriteOnly); a delete +
/// re-insert in one txn, whose `/u/` entry resolves to a single version
/// with header `0x01` — payload-compared, header ignored (mutant N14: a
/// header-byte comparison reports a false violation exactly here); and
/// a DeleteOnly index that never held an entry.
#[test]
fn clean_after_a_mixed_history() {
    let rig = Rig::new();
    // One txn: the table, a row, and the born-public unique index (§6.1
    // case (a): the relation is the txn's own, `built_at = 0`).
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"mix", &pk_schema()));
    let st2 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st2, &h, &[i32_d(1), text_d("a")]));
    let st3 = rig.stmt(&t0, s0);
    let u_idx = ok(rig
        .db
        .create_index(&st3, oid, &idx_def(2, true, IndexState::DeleteOnly)));
    ok(rig.commit_rc(t0));
    assert!(
        isid_of(&rig, u_idx) > 0,
        "the born-public index has a storage"
    );

    // A non-unique ladder index: DeleteOnly over one insert (no entry),
    // then WriteOnly over an insert, a key-changing update and a delete.
    let (nu_idx, _nu_isid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    add_row(&rig, oid, 2, "b"); // while DeleteOnly: no entry, ever written
    rig.set_index_def(nu_idx, idx_def(2, false, IndexState::WriteOnly));
    add_row(&rig, oid, 3, "c"); // while WriteOnly: entry written
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.update(
            &st,
            oid,
            &[i32_v(3)],
            &[t6b_support::set_col(2, text_d("cc"))],
        ));
        ok(rig.commit_rc(t));
    }
    add_row(&rig, oid, 6, "x");
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        assert!(ok(rig.db.delete(&st, oid, &[i32_v(6)])));
        ok(rig.commit_rc(t));
    }

    // Delete + re-insert in ONE txn: the `/u/` entry of ‖a ends as one
    // resolved version with header `0x01` (key_changed, §2.1) whose
    // payload names the new row.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        assert!(ok(rig.db.delete(&st, oid, &[i32_v(1)])));
        let st2 = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st2, &h, &[i32_d(5), text_d("a")]));
        ok(rig.commit_rc(t));
    }

    // A DeleteOnly index over the final state: no entry, ever.
    let (_do_idx, _do_isid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));

    // The N14 anchor, pinned: the delete + re-insert of ‖a in one txn
    // resolved the entry's newest version to header 0x01 (key_changed,
    // §2.1/§7.3) over the original insert's 0x00 — stored newest-first.
    // amcheck must not care about that bit.
    let u_isid = isid_of(&rig, u_idx);
    let u_a = u_entry_key(u_isid, &[text_col()], &[text_v("a".into())]);
    let u_a_versions: Vec<u8> = rig
        .raw_entry_versions(u_isid)
        .into_iter()
        .filter(|(k, ..)| k == &u_a)
        .map(|(_, _, header, _)| header)
        .collect();
    assert_eq!(
        u_a_versions,
        vec![0x01, 0x00],
        "the delete+reinsert carries the key_changed header above the original insert"
    );

    let report = check_now(&rig, oid);
    assert!(
        report.is_clean(),
        "the mixed history is clean: {:?}",
        report.violations
    );
}

/// A tombstone planted above a live `/u/` entry hides it: the entry
/// computed from the visible row is missing (I-IDX).
#[test]
fn detects_an_injected_missing_entry() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"miss", &pk_schema()));
    let st2 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st2, &h, &[i32_d(1), text_d("a")]));
    let st3 = rig.stmt(&t0, s0);
    let u_idx = ok(rig
        .db
        .create_index(&st3, oid, &idx_def(2, true, IndexState::DeleteOnly)));
    ok(rig.commit_rc(t0));

    let isid = isid_of(&rig, u_idx);
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let entry = u_entry_key(isid, &[text_col()], &[text_v("a".into())]);
    // A tombstone at `visible_ts` sorts above the insert's live entry:
    // at any snapshot that sees it the entry is gone.
    core_plant_version(&rig, &entry, rig.core().visible_ts(), HEADER_TOMBSTONE, &[]);
    assert_eq!(
        rig.scan_entries(isid),
        Vec::<(Vec<u8>, Vec<u8>)>::new(),
        "the tombstone hides the entry"
    );

    let report = check_now(&rig, oid);
    assert!(!report.is_clean());
    assert_eq!(
        report.violations,
        vec![format!(
            "I-IDX: index {u_idx} of relation {oid}: entry {entry:?} computed from a visible row is missing at {:?}",
            rig.core().visible_ts()
        )],
        "exactly the missing entry, pointing at the row it belongs to ({key1:?} omitted)"
    );
}

/// A live `/u/` entry planted for a key value no visible row has: an
/// extra entry (I-IDX).
#[test]
fn detects_an_injected_extra_entry() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"extra", &pk_schema()));
    let st2 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st2, &h, &[i32_d(1), text_d("a")]));
    let st3 = rig.stmt(&t0, s0);
    let u_idx = ok(rig
        .db
        .create_index(&st3, oid, &idx_def(2, true, IndexState::DeleteOnly)));
    ok(rig.commit_rc(t0));

    let isid = isid_of(&rig, u_idx);
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let ghost_entry = u_entry_key(isid, &[text_col()], &[text_v("zz".into())]);
    core_plant_version(
        &rig,
        &ghost_entry,
        rig.core().visible_ts(),
        HEADER_LIVE,
        &key1,
    );

    let report = check_now(&rig, oid);
    assert!(!report.is_clean());
    assert_eq!(
        report.violations,
        vec![format!(
            "I-IDX: index {u_idx} of relation {oid}: visible entry {ghost_entry:?} is computed from no visible row"
        )]
    );
}

/// §8 I-IDX-WO: a WriteOnly index's visible entries must be computed
/// from visible rows — an extra planted entry is a violation, while the
/// entry of a row inserted while the index was DeleteOnly is legally
/// missing (that is the state's meaning mid-build, §7).
#[test]
fn writeonly_allows_missing_but_not_extra_entries() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"wo", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (nu_idx, nu_isid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    add_row(&rig, oid, 1, "a"); // while DeleteOnly: no entry
    rig.set_index_def(nu_idx, idx_def(2, false, IndexState::WriteOnly));
    add_row(&rig, oid, 2, "b"); // while WriteOnly: entry written

    // Clean first: row 1's missing entry is legal under WriteOnly.
    let clean = check_now(&rig, oid);
    assert!(clean.is_clean(), "{:?}", clean.violations);

    // An entry no visible row computes: violation (I-IDX-WO).
    let key9 = row_key_of(sid, &pk_schema(), &[i32_v(9)]);
    let ghost = i_entry_key(nu_isid, &[text_col()], &[text_v("zz".into())], &key9);
    core_plant_version(&rig, &ghost, rig.core().visible_ts(), HEADER_LIVE, &[]);

    let report = check_now(&rig, oid);
    assert!(!report.is_clean());
    assert_eq!(
        report.violations,
        vec![format!(
            "I-IDX-WO: index {nu_idx} of relation {oid}: visible entry {ghost:?} is computed from no visible row"
        )],
        "the missing entry of row 1 stays legal; only the extra one flags"
    );
}

/// §8 I-IDX-DO, mutant N22's catcher: a DeleteOnly index visible at the
/// snapshot has no visible entry — one planted above its storage is a
/// violation (an amcheck that skips the check reports clean).
#[test]
fn detects_a_visible_entry_under_a_deleteonly_index() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"do", &pk_schema()));
    ok(rig.commit_rc(t0));

    let (do_idx, do_isid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    add_row(&rig, oid, 1, "a"); // while DeleteOnly: no entry, correctly

    let clean = check_now(&rig, oid);
    assert!(clean.is_clean(), "{:?}", clean.violations);

    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let entry = i_entry_key(do_isid, &[text_col()], &[text_v("a".into())], &key1);
    core_plant_version(&rig, &entry, rig.core().visible_ts(), HEADER_LIVE, &[]);

    let report = check_now(&rig, oid);
    assert!(!report.is_clean(), "N22: I-IDX-DO must be checked");
    assert_eq!(report.violations.len(), 1, "{:?}", report.violations);
    assert!(
        report.violations[0].starts_with(&format!("I-IDX-DO: index {do_idx} of relation {oid}")),
        "{}",
        report.violations[0]
    );
    assert!(report.violations[0].contains(&format!("e.g. {entry:?}")));
}

/// I-ROWFMT's decode half: a raw row version whose payload is not a §2
/// row at all is a violation naming the row's key.
#[test]
fn detects_an_undecodable_row() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"junk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let key9 = row_key_of(sid, &pk_schema(), &[i32_v(9)]);
    core_plant_version(&rig, &key9, rig.core().visible_ts(), HEADER_LIVE, b"junk");

    let report = check_now(&rig, oid);
    assert!(!report.is_clean());
    assert_eq!(report.violations.len(), 1, "{:?}", report.violations);
    assert!(
        report.violations[0].starts_with(&format!(
            "I-ROWFMT: relation {oid} row {key9:?} does not decode"
        )),
        "{}",
        report.violations[0]
    );
}

/// I-ROWFMT's version half: a row encoded at a `schema_version` ahead
/// of the def at the snapshot does not decode (§3 decode rule).
#[test]
fn detects_a_row_with_schema_version_ahead() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"ahead", &pk_schema()));
    ok(rig.commit_rc(t0));

    let mut ahead = pk_schema();
    ahead.version = 99;
    let bytes = ok(ahead.encode_row(&[i32_d(9), text_d("z")]));
    let key9 = row_key_of(sid, &pk_schema(), &[i32_v(9)]);
    core_plant_version(&rig, &key9, rig.core().visible_ts(), HEADER_LIVE, &bytes);

    let report = check_now(&rig, oid);
    assert!(!report.is_clean());
    assert_eq!(report.violations.len(), 1, "{:?}", report.violations);
    assert!(
        report.violations[0].contains("row schema version 99 is ahead"),
        "{}",
        report.violations[0]
    );
}

/// I-UNIQUE: two visible rows with the same unique key value (the second
/// planted raw, so no maintenance refused it) — the shared `/u/` entry
/// is computed by both, and the missing second entry is reported
/// alongside the uniqueness violation.
#[test]
fn detects_two_visible_rows_with_one_unique_key() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"uni", &pk_schema()));
    let st2 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st2, &h, &[i32_d(1), text_d("dup")]));
    let st3 = rig.stmt(&t0, s0);
    let u_idx = ok(rig
        .db
        .create_index(&st3, oid, &idx_def(2, true, IndexState::DeleteOnly)));
    ok(rig.commit_rc(t0));

    // A second row with the same key value, planted raw: no /u/
    // maintenance ran for it, so both rows are visible and compute the
    // same /u/ entry.
    let schema = pk_schema();
    let bytes = ok(schema.encode_row(&[i32_d(9), text_d("dup")]));
    let key9 = row_key_of(sid, &pk_schema(), &[i32_v(9)]);
    core_plant_version(&rig, &key9, rig.core().visible_ts(), HEADER_LIVE, &bytes);

    let report = check_now(&rig, oid);
    assert!(!report.is_clean());
    let entry = u_entry_key(isid_of(&rig, u_idx), &[text_col()], &[text_v("dup".into())]);
    // The uniqueness violation names the shared entry…
    assert_eq!(
        report
            .violations
            .iter()
            .filter(|v| v.starts_with("I-UNIQUE:"))
            .count(),
        1,
        "{:?}",
        report.violations
    );
    assert!(
        report.violations[0].contains(&format!("{entry:?}")),
        "{}",
        report.violations[0]
    );
    // …and the I-IDX half flags the same entry: the one visible payload
    // cannot name both rows (a mismatch against the row that lost the
    // map slot, or a missing entry — either way the entry is named).
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.starts_with("I-IDX:") && v.contains(&format!("{entry:?}"))),
        "{:?}",
        report.violations
    );
}

/// Mutant N23's catcher: the catalog read is at `at`, not the latest. A
/// row whose attnum-2 datum is invalid UTF-8 is a violation under the
/// def at `at` (v1, payload text); a DROP COLUMN committed after `at`
/// makes the same bytes decode cleanly under the latest def (v2 skips
/// the dropped attnum) — an amcheck reading the latest catalog would
/// report the snapshot clean.
#[test]
fn catalog_is_read_at_at_not_the_latest() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"at", &pk_schema()));
    ok(rig.commit_rc(t0));

    // §2 row, schema_version 1, natts 2, no NULLs: id 7 inline, then an
    // attnum-2 datum whose value bytes are not UTF-8 — decodable only
    // when attnum 2 is not decoded at all.
    let mut row = Vec::new();
    row.extend_from_slice(&[0x52, 0x01]);
    row.extend_from_slice(&1u32.to_be_bytes());
    row.extend_from_slice(&2u16.to_be_bytes());
    row.push(0x00); // nullmap: both attnums present
    row.push(0x00);
    row.extend_from_slice(&4u32.to_be_bytes());
    row.extend_from_slice(&7i32.to_be_bytes());
    row.push(0x00);
    row.extend_from_slice(&2u32.to_be_bytes());
    row.extend_from_slice(&[0xff, 0xfe]); // not UTF-8
    let key7 = row_key_of(sid, &pk_schema(), &[i32_v(7)]);
    core_plant_version(&rig, &key7, rig.core().visible_ts(), HEADER_LIVE, &row);

    // The snapshot the verdict is taken at — before the def change.
    let guard = rig.core().registry.take_snapshot();
    let at = guard.ts();

    // The def change: DROP COLUMN attnum 2, committed after `at`. No row
    // is rewritten (§3), so the planted bytes stay as they are.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.drop_column(&st, oid, 2));
        ok(rig.commit_rc(t));
    }
    let latest = t6b_support::decode_def(&rig.rel_row(oid).expect("rel row").def);
    assert_eq!(latest.version, 2, "the drop committed after `at`");
    assert!(latest.columns[1].dropped);

    // At `at`: the def is v1, the row does not decode → I-ROWFMT.
    let report = ok(check_table(&rig.db, oid, &guard));
    assert!(
        !report.is_clean(),
        "N23: the def at `at` (v1) must judge the row, not the latest def"
    );
    assert!(
        report.violations.iter().any(|v| v.starts_with("I-ROWFMT:")),
        "{:?}",
        report.violations
    );

    // The same store at a fresh snapshot (def v2): clean — the dropped
    // attnum is skipped, so the verdict really was the def at `at`.
    let after = check_now(&rig, oid);
    assert!(after.is_clean(), "{:?}", after.violations);
    assert_ne!(at, Ts(0));
}

/// A hidden-rowid table (§3): the rowid is in the key only, never a
/// column — clean rows re-encode, and a row planted at a key whose value
/// part is not a canonical bytea encoding flags I-ROWFMT.
#[test]
fn rowid_table_clean_and_a_bad_key_flags() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"rid", &rowid_schema()));
    ok(rig.commit_rc(t0));
    for v in 1..=3 {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(v)]));
        ok(rig.commit_rc(t));
    }
    let clean = check_now(&rig, oid);
    assert!(clean.is_clean(), "{:?}", clean.violations);

    // A valid row at a key that is not a canonical bytea key value (the
    // §2 escape terminator never comes).
    let mut key = nucleus_catalog::table_lo(sid);
    key.extend_from_slice(b"\x09junk");
    let bytes = ok(rowid_schema().encode_row(&[i32_d(5)]));
    core_plant_version(&rig, &key, rig.core().visible_ts(), HEADER_LIVE, &bytes);

    let report = check_now(&rig, oid);
    assert!(!report.is_clean());
    assert_eq!(report.violations.len(), 1, "{:?}", report.violations);
    assert!(
        report.violations[0].starts_with(&format!(
            "I-ROWFMT: relation {oid} row key {key:?} does not re-encode"
        )),
        "{}",
        report.violations[0]
    );
}

/// A relation the catalog at `at` has no row for is 42P01, not a report.
#[test]
fn missing_relation_is_42p01() {
    let rig = Rig::new();
    let guard = rig.core().registry.take_snapshot();
    let err = check_table(&rig.db, 999, &guard).expect_err("no such relation");
    assert!(matches!(err, nucleus_catalog::DbError::UndefinedTable));
    assert_eq!(err.sqlstate(), "42P01");
}
