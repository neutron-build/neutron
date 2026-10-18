//! C-T6e3pin: pins for `index_scan`'s own-entry verification and the
//! skip branch — C-T6 §6's verification sentence, the e3 restatement:
//!
//! > `index_scan` verifies each fetched entry in the entry's own
//! > visibility regime: a snapshot-visible entry the txn did not write
//! > (a foreign entry) is verified against the row at the scan's
//! > snapshot, fetched without the txn's own overlay; an entry the txn
//! > itself wrote, against the own-overlaid row. An entry whose row is
//! > not visible in its regime, or whose row's key columns do not
//! > re-encode to the entry's key, is XX001. A snapshot entry whose row
//! > the txn has since overwritten is skipped — its row is the txn's
//! > own write now, served through the txn's own entry iff in range.
//!
//! The audit's own mutant on the `else` half of that rule (own entries
//! unverified) survived every index test — the own-entry regime the
//! C-T6e3 fix introduced had no pin — and the skip branch's carve-out
//! (serve nothing through the overwritten snapshot entry) was only
//! pinned through the delete-and-reinsert history of
//! `t6e3fix_reinsert_over_concurrent_delete_scans_clean`, never through
//! the UPDATE shape the §6 maintenance letter describes (delete old
//! entry, insert new). Every scene here is single-threaded and
//! hand-driven: the test thread is the scheduler, the corruptions are
//! planted through the rig's raw KV access (a `Core::write` batch, the
//! `core_plant_version` primitive — no maintenance ran, no statement
//! derived them), and each planted own-write intent carries the reading
//! txn's id and a layer seq below the reading statement's, so §4's own
//! rule treats it exactly as the txn's own write.
//!
//! - `t6e3pin_own_entry_whose_row_is_absent_is_xx001` — item 1(a): an
//!   own entry whose own-overlaid row is absent (a row no one ever
//!   wrote) is XX001 at the row fetch.
//! - `t6e3pin_own_entry_that_does_not_reencode_is_xx001` — item 1(b):
//!   an own entry whose row's key columns do not re-encode to the
//!   entry key is XX001 in the own-entry regime. **OWN-SKIP's catcher**
//!   (the audit's mutant: `if false &&` on the own-entry verification —
//!   under it the corrupt entry is served, not refused).
//! - `t6e3pin_update_serves_the_row_through_the_own_entry_only` — item
//!   2, first leg: the txn UPDATEs an indexed column of a committed
//!   row; the in-txn scan must not return the old key's row — the old
//!   entry is gone (§6 maintenance deleted it) and the row serves
//!   through the txn's new own entry iff its value is in range.
//! - `t6e3pin_stale_own_entry_left_by_the_update_is_xx001_in_txn_and_after_commit`
//!   — item 2, stale leg: the txn inserts a row and UPDATEs it; the
//!   stale own entry the UPDATE's entry delete should have removed is
//!   planted back as the txn's own live intent. §6's letter promises
//!   in-txn XX001 (an entry the txn itself wrote, verified against the
//!   own-overlaid row, whose key columns do not re-encode) and the
//!   engine provides it; after commit the same entry — resolved to a
//!   committed version through the write-set key the delete logged —
//!   is XX001 in the committed regime and flagged by amcheck I-IDX.
//! - `t6e3pin_skipped_snapshot_entry_is_not_served_and_is_caught_after_commit`
//!   — item 2, skip leg: the txn UPDATEs the committed row and the old
//!   entry's delete is erased, so the stale snapshot entry stays
//!   enumerated. In-txn the letter's carve-out applies — skipped, no
//!   XX001, never the old key's row (**SKIP-SERVES's catcher**: the
//!   skip branch serving the snapshot row instead of skipping fails
//!   the first assertion); after commit the leftover committed entry
//!   is XX001 by `index_scan` and flagged by amcheck I-IDX.

mod t6b_support;

use std::ops::Bound;

use nucleus_catalog::amcheck::check_table;
use nucleus_catalog::{DbError, IndexState, Stmt};
use nucleus_codec::{Collation, KeyColumn, KeyType};
use nucleus_kv::{Batch, Durability};
use nucleus_txn::encoding::{encode_intent, intent_key};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::{Intent, Layer, LayerData, RowLockMode, Seq, TxnId};

use t6b_support::{
    i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, row_key_of, set_col, text_d, text_v, Rig,
};

// ---- the raw-KV primitives (the card's "test rig's raw KV access") --------

/// The single text index column of the fixtures (payload ASC, C).
fn text_col() -> KeyColumn {
    KeyColumn::asc(KeyType::Text(Collation::C))
}

/// A single-layer live own-write intent for `txn` at layer `seq` — the
/// shape the §5 entry insert leaves at an `/i/` entry key (payload
/// empty). No statement wrote it; the bytes appear raw.
fn own_live_intent(txn: TxnId, seq: Seq) -> Intent {
    Intent {
        txn,
        layers: vec![Layer {
            seq,
            data_seq: seq,
            data: LayerData::Write {
                value: Vec::new(),
                key_changed: false,
            },
            lock: RowLockMode::NoKeyUpdate,
        }],
    }
}

/// Plants a raw live own-write intent at entry key `logical` (a
/// `Core::write` batch, `core_plant_version`'s primitive). Where the
/// txn already owns an intent there, the plant overwrites it — the
/// stored state is exactly as if the last layer written were a live
/// write. `seq` must be the seq of a statement that already ran (§4:
/// a later statement reads the layer as its own write).
fn plant_own_live_entry(rig: &Rig, txn: TxnId, logical: &[u8], seq: Seq) {
    let mut batch = Batch::default();
    batch.put(
        intent_key(logical),
        ok(encode_intent(&own_live_intent(txn, seq))),
    );
    ok(rig.core().write(batch, Durability::No));
}

/// Erases the stored intent bytes at `logical` (a raw KV delete): the
/// key keeps only its committed versions, as if the intent's writer
/// never wrote it — the state an UPDATE whose §6 entry delete never
/// ran leaves behind at the old entry key.
fn erase_stored_intent(rig: &Rig, logical: &[u8]) {
    let mut batch = Batch::default();
    batch.delete(intent_key(logical));
    ok(rig.core().write(batch, Durability::No));
}

// ---- fixtures ---------------------------------------------------------------

/// The storage id of a born-public index (§6.1(a): an id storage).
fn isid_of(rig: &Rig, idx: u64) -> u64 {
    let row = rig.idx_row(idx).expect("the idx row committed");
    let nucleus_txn::catalog::IdxStorage::Id(isid) = row.storage else {
        panic!("a born-public index has an id storage")
    };
    isid
}

/// `t(pk int4 PRIMARY KEY, payload text)` with a born-public non-unique
/// `/i/` index on payload (§6.1(a): created by the setup txn, `built_at
/// = 0`, committed empty). Returns `(oid, sid, idx)`.
fn fixture(rig: &Rig, name: &[u8]) -> (u64, u64, u64) {
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, name, &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let idx = ok(rig
        .db
        .create_index(&st1, oid, &idx_def(2, false, IndexState::Public)));
    ok(rig.commit_rc(t0));
    (oid, sid, idx)
}

/// The same fixture with one committed row `(1, "b")` — its entry
/// `("b" → row 1)` is a committed version at or below every later
/// snapshot (the `t6e3fix` fixture shape). Returns `(oid, sid, idx)`
/// and the setup's commit ts.
fn fixture_with_row(rig: &Rig, name: &[u8]) -> (u64, u64, u64, nucleus_txn::Ts) {
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, name, &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("b")]));
    let st2 = rig.stmt(&t0, s0);
    let idx = ok(rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public)));
    let ts = ok(rig.commit_rc(t0));
    (oid, sid, idx, ts)
}

/// An inclusive lower text bound (the `t6e3fix` shape): `b_incl("a")`
/// with `Bound::Excluded("c")` is the old key's range `["a", "c")`.
fn b_incl(v: &str) -> Bound<Vec<Option<nucleus_codec::Value>>> {
    Bound::Included(vec![text_v(v.to_string())])
}

// ---- item 1: own-entry pins ---------------------------------------------------

/// Item 1(a): inside one txn, an own entry whose own-overlaid row is
/// absent — the planted entry names row 2, a row no statement ever
/// wrote (no intent, no version). The own-overlay fetch of the row
/// finds nothing at all, and the scan must refuse the state with
/// XX001 ("index entry names a row not visible at the snapshot") — the
/// not-visible half of §6's sentence, in the own regime. The refusal
/// sits at the row fetch itself, ahead of the regime dispatch, so this
/// pin holds the behaviour; the re-encode half (and the audit's
/// OWN-SKIP mutant) is the next test's.
#[test]
fn t6e3pin_own_entry_whose_row_is_absent_is_xx001() {
    let rig = Rig::new();
    let (oid, sid, idx) = fixture(&rig, b"e3pin1a");
    let (rr, sguard, s) = rig.rr();

    // The txn's own row and own entry: (1, "b").
    let st_i = rig.stmt(&rr, s);
    let h = ok(rig.db.open_table(&st_i, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st_i, &h, &[i32_d(1), text_d("b")]));
    let seq_i = st_i.seq0();

    // Control: before the plant the healthy own entry serves the own row.
    let st_c = rig.stmt(&rr, s);
    let rows = ok(rig
        .db
        .index_scan(&st_c, idx, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows.len(), 1, "the own entry serves the own row");
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("b")]);

    // The corruption: the txn's own entry for row 2 under "q" — a row
    // that does not exist in any regime.
    let isid = isid_of(&rig, idx);
    let row2 = row_key_of(sid, &pk_schema(), &[i32_v(2)]);
    let ghost_entry = i_entry_key(isid, &[text_col()], &[text_v("q".into())], &row2);
    plant_own_live_entry(&rig, rr.id, &ghost_entry, seq_i);

    let st_s = rig.stmt(&rr, s);
    let err = rig
        .db
        .index_scan(&st_s, idx, Bound::Unbounded, Bound::Unbounded)
        .expect_err("an own entry whose own-overlaid row is absent is XX001");
    assert!(
        matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "index entry names a row not visible at the snapshot"
            ))
        ),
        "the corruption signature, not {err:?}"
    );
    assert_eq!(err.sqlstate(), "XX001");

    // The corrupted rig is abandoned (the plant was a raw write; no
    // abort undoes it — the planted key was never in any write-set).
    rig.abort_txn(rr);
    drop(sguard);
}

/// Item 1(b): inside one txn, an own entry whose row's key columns do
/// not re-encode to the entry key — the planted entry names row 1 under
/// "q", while the txn's own row for row 1 is `(1, "b")`. The own-entry
/// regime must verify the own-overlaid row against the entry and
/// refuse with XX001 ("index entry does not match its row"). This is
/// the regime the audit's OWN-SKIP mutant disarmed: with the
/// verification skipped the scan serves the row through the corrupt
/// entry instead of refusing.
#[test]
fn t6e3pin_own_entry_that_does_not_reencode_is_xx001() {
    let rig = Rig::new();
    let (oid, sid, idx) = fixture(&rig, b"e3pin1b");
    let (rr, sguard, s) = rig.rr();

    let st_i = rig.stmt(&rr, s);
    let h = ok(rig.db.open_table(&st_i, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st_i, &h, &[i32_d(1), text_d("b")]));
    let seq_i = st_i.seq0();

    // Control: the healthy own entry serves the own row.
    let st_c = rig.stmt(&rr, s);
    let rows = ok(rig
        .db
        .index_scan(&st_c, idx, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows.len(), 1, "the own entry serves the own row");
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("b")]);

    // The corruption: the txn's own entry for its own row 1 under a
    // value the row does not carry.
    let isid = isid_of(&rig, idx);
    let row1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let bad_entry = i_entry_key(isid, &[text_col()], &[text_v("q".into())], &row1);
    plant_own_live_entry(&rig, rr.id, &bad_entry, seq_i);

    let st_s = rig.stmt(&rr, s);
    let err = rig
        .db
        .index_scan(&st_s, idx, Bound::Unbounded, Bound::Unbounded)
        .expect_err("an own entry that does not re-encode to its row is XX001");
    assert!(
        matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "index entry does not match its row"
            ))
        ),
        "the corruption signature, not {err:?}"
    );
    assert_eq!(err.sqlstate(), "XX001");

    rig.abort_txn(rr);
    drop(sguard);
}

// ---- item 2: skip-branch pins -------------------------------------------------

/// Item 2, first leg: the txn UPDATEs an indexed column of a committed
/// row. §6 maintenance deletes the old entry (a blind tombstone intent
/// — the reader's own scan does not even enumerate it) and inserts the
/// new own entry, so the in-txn scan over the old key's range returns
/// nothing, the row serves through the txn's new own entry iff its new
/// value is in range, and the full scan serves the row exactly once.
/// The abort leaves the committed state untouched and amcheck clean.
#[test]
fn t6e3pin_update_serves_the_row_through_the_own_entry_only() {
    let rig = Rig::new();
    let (oid, _sid, idx, t_setup) = fixture_with_row(&rig, b"e3pin2a");
    let (rr, sguard, s) = rig.rr();
    assert!(
        t_setup <= s,
        "S={s:?} at or below the setup commit {t_setup:?}"
    );

    // The UPDATE moves payload "b" → "y": the old entry is tombstoned,
    // a new own entry lands under "y".
    let st_u = rig.stmt(&rr, s);
    assert!(
        ok(rig
            .db
            .update(&st_u, oid, &[i32_v(1)], &[set_col(2, text_d("y"))])),
        "the update applies"
    );

    // (1) The old key's range: nothing — the old entry is gone for this
    // txn and the row's new value is out of range.
    let st_c1 = rig.stmt(&rr, s);
    let rows1 = ok(rig.db.index_scan(
        &st_c1,
        idx,
        b_incl("a"),
        Bound::Excluded(vec![text_v("c".into())]),
    ));
    assert!(
        rows1.is_empty(),
        "the old key's row is not returned: {rows1:?}"
    );
    // (2) The new value's range: the row through its new own entry.
    let st_c2 = rig.stmt(&rr, s);
    let rows2 = ok(rig
        .db
        .index_scan(&st_c2, idx, b_incl("y"), Bound::Unbounded));
    assert_eq!(rows2.len(), 1, "the own entry serves the own row");
    assert_eq!(rows2[0].1, vec![i32_d(1), text_d("y")]);
    // (3) The full range: exactly once, through the new own entry.
    let st_c3 = rig.stmt(&rr, s);
    let rows3 = ok(rig
        .db
        .index_scan(&st_c3, idx, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows3.len(), 1, "the full scan serves the row exactly once");
    assert_eq!(rows3[0].1, vec![i32_d(1), text_d("y")]);

    // Nothing committed: the abort leaves the setup's state, amcheck
    // stays clean.
    rig.abort_txn(rr);
    drop(sguard);
    let t_r = rig.rc();
    let s_r = rig.core().visible_ts();
    let st_r = rig.stmt(&t_r, s_r);
    let h_r = ok(rig.db.open_table(&st_r, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st_r, &h_r));
    assert_eq!(rows.len(), 1, "the committed table is the setup's table");
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("b")]);
    ok(rig.commit_rc(t_r));
    let g = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g)).is_clean(),
        "amcheck after the abort"
    );
    drop(g);
}

/// Item 2, stale leg: the txn inserts a row, then UPDATEs it — §6
/// maintenance should delete the insert's own entry under "b" and
/// write the new one under "y". The stale own entry the delete should
/// have removed is planted back as the txn's own live intent (the
/// stored state is exactly as if the delete never ran). §6's letter
/// promises in-txn detection — an entry the txn itself wrote, verified
/// against the own-overlaid row `(1, "y")`, whose key columns do not
/// re-encode to "b" — and the engine's own-entry regime provides it:
/// the scan is XX001 before the commit. Committing then resolves the
/// planted intent through the write-set key the delete logged, so the
/// stale entry survives as a committed version — and is caught after
/// the commit by `index_scan` in the committed regime and by amcheck
/// I-IDX (the card's Goal: "after commit, as a committed entry, and by
/// amcheck").
#[test]
fn t6e3pin_stale_own_entry_left_by_the_update_is_xx001_in_txn_and_after_commit() {
    let rig = Rig::new();
    let (oid, sid, idx) = fixture(&rig, b"e3pin2b");
    let (rr, sguard, s) = rig.rr();

    // The txn's own row and own entry: (1, "b") under "b".
    let st_i = rig.stmt(&rr, s);
    let h = ok(rig.db.open_table(&st_i, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st_i, &h, &[i32_d(1), text_d("b")]));
    let seq_i = st_i.seq0();

    // Control: the healthy own entry serves the own row.
    let st_c = rig.stmt(&rr, s);
    let rows = ok(rig
        .db
        .index_scan(&st_c, idx, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows.len(), 1, "the own entry serves the own row");
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("b")]);

    // The UPDATE: maintenance deletes the own entry under "b" (a
    // Delete layer on its intent) and inserts the new own entry under
    // "y".
    let st_u = rig.stmt(&rr, s);
    assert!(
        ok(rig
            .db
            .update(&st_u, oid, &[i32_v(1)], &[set_col(2, text_d("y"))])),
        "the update applies"
    );

    // The corruption: the stale own entry under "b" is planted back —
    // the intent the UPDATE's delete left becomes a live own write
    // again, as if the delete never ran.
    let isid = isid_of(&rig, idx);
    let row1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let stale = i_entry_key(isid, &[text_col()], &[text_v("b".into())], &row1);
    plant_own_live_entry(&rig, rr.id, &stale, seq_i);

    // In-txn: the own-entry regime must refuse — XX001, never the stale
    // entry silently served (OWN-SKIP's failure mode).
    let st_s = rig.stmt(&rr, s);
    let err = rig
        .db
        .index_scan(&st_s, idx, Bound::Unbounded, Bound::Unbounded)
        .expect_err("the stale own entry is XX001 in the own-entry regime");
    assert!(
        matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "index entry does not match its row"
            ))
        ),
        "the corruption signature, not {err:?}"
    );
    assert_eq!(err.sqlstate(), "XX001");

    // Commit: the planted intent resolves to a committed live version
    // (the entry key is in the write-set log — the insert's entry write
    // and the update's entry delete both logged it), so the stale entry
    // is now a committed entry.
    let t_c = ok(rig.commit_rc(rr));
    drop(sguard);

    // After the commit: `index_scan` in the committed regime refuses —
    // the committed row is `(1, "y")`, which does not re-encode to "b".
    let t_r = rig.rc();
    let s_r = rig.core().visible_ts();
    assert!(
        t_c <= s_r,
        "the reader sees the commit ({t_c:?} <= {s_r:?})"
    );
    let st_r = rig.stmt(&t_r, s_r);
    let err = rig
        .db
        .index_scan(
            &st_r,
            idx,
            b_incl("a"),
            Bound::Excluded(vec![text_v("c".into())]),
        )
        .expect_err("the committed stale entry is XX001 in the committed regime");
    assert!(
        matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "index entry does not match its row"
            ))
        ),
        "the corruption signature, not {err:?}"
    );
    assert_eq!(err.sqlstate(), "XX001");
    ok(rig.commit_rc(t_r));

    // And amcheck I-IDX flags it: a visible entry computed from no
    // visible row.
    let g = rig.core().registry.take_snapshot();
    let report = ok(check_table(&rig.db, oid, &g));
    drop(g);
    assert!(!report.is_clean(), "the stale entry is corruption");
    assert_eq!(report.violations.len(), 1, "{:?}", report.violations);
    assert!(
        report.violations[0].contains(&format!(
            "visible entry {stale:?} is computed from no visible row"
        )),
        "the I-IDX violation names the stale entry: {:?}",
        report.violations
    );
}

/// Item 2, skip leg: the txn UPDATEs an indexed column of a committed
/// row, and the old entry's delete is erased — the stale snapshot
/// entry (committed, visible at S, never tombstoned) stays enumerated
/// while the txn's own row intent shadows its row. This is the skip
/// branch's scene: §6's letter promises the entry is **skipped** — no
/// XX001 in-txn (the snapshot pair still re-encodes; the row is the
/// txn's own write now), never the old key's row, the row serving
/// through its new own entry iff in range. **SKIP-SERVES's catcher**:
/// a skip branch that serves the snapshot row instead of skipping
/// breaks assertion (1). After the commit — the write-set resolution
/// finds no intent at the old key (Noop: no tombstone version lands) —
/// the stale entry survives as a committed entry and is caught by
/// `index_scan` in the committed regime and by amcheck I-IDX.
#[test]
fn t6e3pin_skipped_snapshot_entry_is_not_served_and_is_caught_after_commit() {
    let rig = Rig::new();
    let (oid, sid, idx, t_setup) = fixture_with_row(&rig, b"e3pin2c");
    let (rr, sguard, s) = rig.rr();
    assert!(
        t_setup <= s,
        "S={s:?} at or below the setup commit {t_setup:?}"
    );

    // The UPDATE moves payload "b" → "y"...
    let st_u = rig.stmt(&rr, s);
    assert!(
        ok(rig
            .db
            .update(&st_u, oid, &[i32_v(1)], &[set_col(2, text_d("y"))])),
        "the update applies"
    );
    // ...and its entry delete is erased: the state an UPDATE that left
    // the old entry behind produces (the card's "a stale entry left by
    // the txn's own UPDATE").
    let isid = isid_of(&rig, idx);
    let row1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let stale = i_entry_key(isid, &[text_col()], &[text_v("b".into())], &row1);
    erase_stored_intent(&rig, &stale);

    // (1) The old key's range: skipped, not served — no XX001 (the
    // letter's carve-out), and never the old key's row.
    let st_c1 = rig.stmt(&rr, s);
    let rows1 = ok(rig.db.index_scan(
        &st_c1,
        idx,
        b_incl("a"),
        Bound::Excluded(vec![text_v("c".into())]),
    ));
    assert!(
        rows1.is_empty(),
        "the skipped snapshot entry serves nothing: {rows1:?}"
    );
    // (2) The new value's range: the row through its new own entry.
    let st_c2 = rig.stmt(&rr, s);
    let rows2 = ok(rig
        .db
        .index_scan(&st_c2, idx, b_incl("y"), Bound::Unbounded));
    assert_eq!(rows2.len(), 1, "the own entry serves the own row");
    assert_eq!(rows2[0].1, vec![i32_d(1), text_d("y")]);
    // (3) The full range: exactly once — the stale entry is skipped.
    let st_c3 = rig.stmt(&rr, s);
    let rows3 = ok(rig
        .db
        .index_scan(&st_c3, idx, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows3.len(), 1, "the full scan serves the row exactly once");
    assert_eq!(rows3[0].1, vec![i32_d(1), text_d("y")]);

    // Commit: the old entry key's resolution finds no intent (Noop), so
    // no tombstone ever lands above the setup's live version — the
    // stale entry survives as a committed entry.
    let t_c = ok(rig.commit_rc(rr));
    drop(sguard);

    // After the commit: `index_scan` in the committed regime refuses —
    // the committed row `(1, "y")` does not re-encode to "b".
    let t_r = rig.rc();
    let s_r = rig.core().visible_ts();
    assert!(
        t_c <= s_r,
        "the reader sees the commit ({t_c:?} <= {s_r:?})"
    );
    let st_r = rig.stmt(&t_r, s_r);
    let err = rig
        .db
        .index_scan(
            &st_r,
            idx,
            b_incl("a"),
            Bound::Excluded(vec![text_v("c".into())]),
        )
        .expect_err("the leftover committed entry is XX001 in the committed regime");
    assert!(
        matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "index entry does not match its row"
            ))
        ),
        "the corruption signature, not {err:?}"
    );
    assert_eq!(err.sqlstate(), "XX001");
    ok(rig.commit_rc(t_r));

    // And amcheck I-IDX flags it.
    let g = rig.core().registry.take_snapshot();
    let report = ok(check_table(&rig.db, oid, &g));
    drop(g);
    assert!(!report.is_clean(), "the leftover entry is corruption");
    assert_eq!(report.violations.len(), 1, "{:?}", report.violations);
    assert!(
        report.violations[0].contains(&format!(
            "visible entry {stale:?} is computed from no visible row"
        )),
        "the I-IDX violation names the leftover entry: {:?}",
        report.violations
    );
}
