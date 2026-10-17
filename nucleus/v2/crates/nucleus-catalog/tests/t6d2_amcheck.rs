//! C-T6d2: amcheck's I-TOAST half (§5, §8) — the five injected
//! corruptions (an orphan chunk no pointer names, a dangling pointer
//! whose chunk is not visible, a short chunk of the wrong shape, a
//! shape-legal set of chunks that sums past `raw_len`, and a huge
//! chunk count beyond the storage's visible chunks) are each reported,
//! and the checker stays clean across C-T6d1's §5 scenarios: the
//! threshold boundary rows, a multi-chunk value, an update that
//! carries a pointer, a replace and a delete that tombstone their
//! chunks, a DROP COLUMN whose pre-drop rows still name live chunks
//! through their raw pointers, the re-add of the column, and the EPQ
//! skip that writes no chunk at all.
//!
//! The dropped-column scenario is mutant T8's catcher: a checker that
//! swept pointers with a live-columns `decode_row` (dropped attnums
//! ignored) would report the dropped column's still-live chunks as
//! false orphans exactly there.

mod t6b_support;

use std::collections::BTreeSet;
use std::ops::Bound;

use t6b_support::{
    core_plant_version, i32_d, i32_v, ok, pk_schema, row_key_of, set_col, text_d, Rig,
};

use nucleus_catalog::amcheck::{check_table, Report};
use nucleus_catalog::{chunk_key, Stmt, TOAST_CHUNK, TOAST_MARK, TOAST_THRESHOLD};
use nucleus_codec::row::{ColumnDef, Datum, TableSchema, ToastPointer};
use nucleus_codec::{Collation, KeyType};
use nucleus_txn::encoding::{HEADER_LIVE, HEADER_TOMBSTONE};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::read::{read_key, scan, NoSsi};
use nucleus_txn::visibility::ReadCtx;

/// `CREATE TABLE wide (id int4 PRIMARY KEY, a text, big text)` — C-T6d1's
/// toast fixture.
fn wide_schema() -> TableSchema {
    let col = |attnum: u16, name: &str, ty: KeyType| ColumnDef {
        attnum,
        name: name.into(),
        ty,
        not_null: attnum == 1,
        dropped: false,
        missing: None,
    };
    ok(TableSchema::new(
        vec![
            col(1, "id", KeyType::Int4),
            col(2, "a", KeyType::Text(Collation::C)),
            col(3, "big", KeyType::Text(Collation::C)),
        ],
        vec![1],
    ))
}

/// A large text of `len` bytes, all one ASCII letter `'a' + tag % 26` —
/// distinct per tag, one byte per char (t6d_toast's `blob`).
fn blob(tag: u8, len: usize) -> String {
    char::from(b'a' + (tag % 26)).to_string().repeat(len)
}

/// Creates the wide table, committed; returns `(oid, sid)`.
fn mk_wide(rig: &Rig) -> (u64, u64) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = ok(Stmt::new(&t, s));
    let r = ok(rig.db.create_table(&st, b"wide", &wide_schema()));
    ok(rig.commit_rc(t));
    r
}

/// One committed insert of `(id, a, big)` in its own txn.
fn add_row(rig: &Rig, oid: u64, id: i32, a: &str, big: &str) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig
        .db
        .insert_with(&st, &h, &[i32_d(id), text_d(a), text_d(big)]));
    ok(rig.commit_rc(t));
}

/// One committed update of `a` (carries the pointer) or of `big`.
fn update(rig: &Rig, oid: u64, id: i32, attnum: u16, value: &str) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    assert!(ok(rig.db.update(
        &st,
        oid,
        &[i32_v(id)],
        &[set_col(attnum, text_d(value))]
    )));
    ok(rig.commit_rc(t));
}

/// One committed delete, its own txn.
fn delete(rig: &Rig, oid: u64, id: i32) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    assert!(ok(rig.db.delete(&st, oid, &[i32_v(id)])));
    ok(rig.commit_rc(t));
}

/// amcheck at a fresh registered snapshot.
fn check_now(rig: &Rig, oid: u64) -> Report {
    let guard = rig.core().registry.take_snapshot();
    ok(check_table(&rig.db, oid, &guard))
}

/// The visible chunk keys of storage `sid` at `visible_ts` (§4) — the
/// oracle the orphan assertions count with.
fn visible_chunk_keys(rig: &Rig, sid: u64) -> Vec<nucleus_kv::Key> {
    let (lo, hi) = nucleus_txn::catalog::table_range(sid);
    let mut clo = lo.clone();
    clo.push(TOAST_MARK);
    let view = rig.core().open_view();
    let ctx = ReadCtx {
        txn: ghost(rig),
        snapshot: rig.core().visible_ts(),
        stmt_seq: 0,
    };
    ok(scan(
        rig.core(),
        &view,
        (
            Bound::Included(clo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        &ctx,
        &mut NoSsi,
    )
    .collect::<Result<Vec<_>, _>>())
    .into_iter()
    .map(|(k, _)| k)
    .collect()
}

/// A reader id this process never allocates (the support module's ghost).
fn ghost(rig: &Rig) -> nucleus_txn::TxnId {
    nucleus_txn::TxnId {
        epoch: rig.core().epoch(),
        n: u64::MAX,
    }
}

/// The toast pointers a stored row names, raw decode (dropped attnums
/// included).
fn row_pointers(rig: &Rig, sid: u64, id: i32) -> Vec<ToastPointer> {
    let key = row_key_of(sid, &wide_schema(), &[i32_v(id)]);
    let view = rig.core().open_view();
    let ctx = ReadCtx {
        txn: ghost(rig),
        snapshot: rig.core().visible_ts(),
        stmt_seq: 0,
    };
    let bytes = ok(read_key(rig.core(), &view, &key, &ctx, &mut NoSsi)).expect("row bytes");
    let (_, raw) = ok(nucleus_codec::row::decode_row_raw(&bytes));
    raw.into_iter()
        .flatten()
        .filter_map(|d| match d {
            nucleus_codec::row::RawDatum::Toast(p) => Some(p),
            nucleus_codec::row::RawDatum::Inline(_) => None,
        })
        .collect()
}

// ---- the injected corruptions ---------------------------------------------

/// A live chunk no pointer names (planted at a toast id no statement
/// ever minted) is an orphan: exactly one I-TOAST violation, naming the
/// chunk key.
#[test]
fn detects_an_injected_orphan_chunk() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_row(&rig, oid, 1, "x", &blob(0, TOAST_THRESHOLD + 1));
    let clean = check_now(&rig, oid);
    assert!(clean.is_clean(), "{:?}", clean.violations);

    // The orphan: a live chunk at a foreign id, visible at any snapshot
    // at or above the plant.
    let orphan = chunk_key(sid, &[0xAB; 20], 0);
    core_plant_version(
        &rig,
        &orphan,
        rig.core().visible_ts(),
        HEADER_LIVE,
        b"orphan",
    );

    let report = check_now(&rig, oid);
    assert!(!report.is_clean(), "the orphan chunk went unnoticed");
    assert_eq!(
        report.violations,
        vec![format!(
            "I-TOAST: relation {oid}: visible chunk {orphan:?} is named by no pointer of a visible row (orphan)"
        )],
        "exactly the orphan, naming its key"
    );
}

/// A pointer whose chunk is not visible (a tombstone planted above the
/// chunk's live version) is dangling: exactly one I-TOAST violation,
/// naming the row and the chunk index. The second row's out-of-line
/// value keeps the storage's visible-chunk count at the pointer's chunk
/// count after the tombstone, so the per-index walk — not the
/// count-above-visible bound — is the shape that reports it.
#[test]
fn detects_an_injected_missing_chunk() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_row(&rig, oid, 1, "x", &blob(1, 20_000));
    let p = row_pointers(&rig, sid, 1).remove(0);
    assert_eq!(p.chunks, 3, "three chunks");
    let clean = check_now(&rig, oid);
    assert!(clean.is_clean(), "{:?}", clean.violations);

    // A second commit so `visible_ts` moves past the chunk's own ts,
    // then a tombstone above chunk 1's live version hides it. The
    // second row's `big` is out of line (one chunk), so the storage
    // keeps a visible count of 3 — the bound's reach is a different
    // fixture's business (C-T6d2fix FU-5).
    add_row(&rig, oid, 2, "y", &blob(3, TOAST_THRESHOLD + 1));
    let now = rig.core().visible_ts();
    let k1 = chunk_key(sid, &p.id, 1);
    core_plant_version(&rig, &k1, now, HEADER_TOMBSTONE, b"");
    assert_eq!(
        visible_chunk_keys(&rig, sid).len(),
        3,
        "chunk 1 is gone; row 2's one chunk stays"
    );

    let report = check_now(&rig, oid);
    assert!(!report.is_clean(), "the dangling pointer went unnoticed");
    assert_eq!(
        report.violations,
        vec![format!(
            "I-TOAST: relation {oid} row {:?}: chunk 1 of pointer {:?} is not visible at {now:?} (dangling pointer)",
            row_key_of(sid, &wide_schema(), &[i32_v(1)]),
            p.id
        )],
        "exactly the missing chunk, naming the row and the index"
    );
}

/// A short chunk (a live version with the wrong payload shape planted
/// above the real one) breaks the §5 chunk shape: exactly one I-TOAST
/// violation, naming the chunk index and the length.
#[test]
fn detects_an_injected_short_chunk() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_row(&rig, oid, 1, "x", &blob(2, 20_000));
    let p = row_pointers(&rig, sid, 1).remove(0);
    assert_eq!(p.chunks, 3, "three chunks");

    // A live short chunk above the real chunk 1: the read path yields
    // the newest version, so chunk 1 is 100 bytes where §5 demands
    // TOAST_CHUNK (the sum check would fire too; the shape is reported).
    add_row(&rig, oid, 2, "y", "small");
    let now = rig.core().visible_ts();
    let k1 = chunk_key(sid, &p.id, 1);
    core_plant_version(&rig, &k1, now, HEADER_LIVE, &[7u8; 100]);

    let report = check_now(&rig, oid);
    assert!(!report.is_clean(), "the short chunk went unnoticed");
    assert_eq!(report.violations.len(), 1, "{:?}", report.violations);
    assert!(
        report.violations[0].starts_with(&format!(
            "I-TOAST: relation {oid} row {:?}: chunk 1 of pointer {:?} is 100 bytes, not the §5 shape",
            row_key_of(sid, &wide_schema(), &[i32_v(1)]),
            p.id
        )),
        "{}",
        report.violations[0]
    );
}

/// A pointer whose chunks all keep the §5 shape but sum past `raw_len`
/// — a full `TOAST_CHUNK`-byte live version planted above the real,
/// shorter last chunk of a 20 KB value — is the sum check's own report:
/// exactly one I-TOAST violation, naming the sum and the `raw_len`.
/// C-T6d2fix FU-4 (gate F-4/GA6): the injected fixtures all trip the
/// shape check or a miss first, so the sum arm — the only check that
/// pins the last chunk's exact length (the shape rule allows any
/// 1..=TOAST_CHUNK) — was unreachable defense-in-depth.
#[test]
fn detects_an_injected_overlong_last_chunk_sum() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_row(&rig, oid, 1, "x", &blob(2, 20_000));
    let p = row_pointers(&rig, sid, 1).remove(0);
    assert_eq!(p.chunks, 3, "three chunks");
    let clean = check_now(&rig, oid);
    assert!(clean.is_clean(), "{:?}", clean.violations);

    // A full TOAST_CHUNK-byte live version above the real last chunk
    // (3616 bytes): the read path yields the newest version, so chunk 2
    // is shape-legal — a last chunk may be 1..=TOAST_CHUNK bytes —
    // while the sum passes raw_len: 8192·3 = 24576 ≠ 20000.
    add_row(&rig, oid, 2, "y", "small");
    let now = rig.core().visible_ts();
    let k2 = chunk_key(sid, &p.id, 2);
    core_plant_version(&rig, &k2, now, HEADER_LIVE, &[7u8; TOAST_CHUNK]);

    let report = check_now(&rig, oid);
    assert!(!report.is_clean(), "the overlong last chunk went unnoticed");
    assert_eq!(
        report.violations,
        vec![format!(
            "I-TOAST: relation {oid} row {:?}: the chunks of pointer {:?} sum to {}, raw_len says {}",
            row_key_of(sid, &wide_schema(), &[i32_v(1)]),
            p.id,
            3 * TOAST_CHUNK,
            p.raw_len
        )],
        "exactly the sum violation, naming the sum and the raw_len"
    );
}

/// A pointer that counts more chunks than the storage has visible — a
/// planted huge-but-self-consistent `ToastPointer` (u32::MAX chunks,
/// raw_len u32::MAX, at an id no statement ever minted, over a row
/// whose `big` was inline) — is reported by the visible-chunk bound,
/// not iterated: exactly one I-TOAST violation, and the check returns
/// (C-T6d2fix, gate F-5: the walk would otherwise run 2³² iterations,
/// one `named` insert each — amcheck on such a corrupt store would spin
/// instead of reporting; the cannot-reach guard above the bound cannot
/// catch this shape, since u32::MAX chunks of 8192 bytes do reach a
/// raw_len of u32::MAX).
#[test]
fn detects_an_injected_huge_chunk_count() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_row(&rig, oid, 1, "x", "small");
    assert!(visible_chunk_keys(&rig, sid).is_empty(), "big stays inline");
    let clean = check_now(&rig, oid);
    assert!(clean.is_clean(), "{:?}", clean.violations);

    // The planted row version: the same row, its `big` the huge pointer
    // (a fresh id, so nothing is orphaned — the row's real bytes named
    // no chunks).
    add_row(&rig, oid, 2, "y", "small");
    let now = rig.core().visible_ts();
    let huge = ToastPointer {
        id: [0xB9; 20],
        raw_len: u32::MAX,
        chunks: u32::MAX,
    };
    let key = row_key_of(sid, &wide_schema(), &[i32_v(1)]);
    let bytes = ok(wide_schema().encode_row(&[i32_d(1), text_d("x"), Some(Datum::Toast(huge))]));
    core_plant_version(&rig, &key, now, HEADER_LIVE, &bytes);

    let report = check_now(&rig, oid);
    assert!(!report.is_clean(), "the huge chunk count went unnoticed");
    assert_eq!(
        report.violations,
        vec![format!(
            "I-TOAST: relation {oid} row {key:?}: pointer {:?} counts {} chunks, but the storage has {} visible (dangling at some index)",
            huge.id, huge.chunks, 0
        )],
        "exactly the bound's report; the pointer is not walked"
    );
}

// ---- clean across C-T6d1's scenarios ---------------------------------------

/// I-TOAST stays clean across d1's scenarios on one store: the threshold
/// boundary rows (2047, 2048 inline / 2049 out of line), a 3-chunk
/// value, an update that carries the pointer (chunk keys unchanged), a
/// replace that tombstones the old chunks, a delete that removes them, a
/// re-insert, and the EPQ skip that writes no chunk.
#[test]
fn clean_after_d1_scenarios() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);

    // The threshold boundary rows and the multi-chunk value (d1's round
    // trip).
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    ok(rig.db.insert_by_oid(
        &st,
        oid,
        &[
            i32_d(1),
            text_d("a"),
            text_d(&blob(10, TOAST_THRESHOLD - 1)),
        ],
    ));
    ok(rig.db.insert_by_oid(
        &st,
        oid,
        &[i32_d(2), text_d("a"), text_d(&blob(11, TOAST_THRESHOLD))],
    ));
    ok(rig.db.insert_by_oid(
        &st,
        oid,
        &[
            i32_d(3),
            text_d("a"),
            text_d(&blob(12, TOAST_THRESHOLD + 1)),
        ],
    ));
    ok(rig.commit_rc(t));
    assert_eq!(visible_chunk_keys(&rig, sid).len(), 1, "one chunk (row 3)");
    let clean = check_now(&rig, oid);
    assert!(clean.is_clean(), "{:?}", clean.violations);

    // The multi-chunk value.
    add_row(&rig, oid, 4, "a", &blob(13, 20_000));
    assert_eq!(visible_chunk_keys(&rig, sid).len(), 4, "1 + 3 chunks");
    assert!(check_now(&rig, oid).is_clean(), "multi-chunk clean");

    // An update of another column carries the pointer (T1).
    update(&rig, oid, 4, 2, "y");
    assert_eq!(visible_chunk_keys(&rig, sid).len(), 4, "carried: unchanged");
    assert!(check_now(&rig, oid).is_clean(), "the carried pointer clean");

    // A replace of `big` tombstones the old chunks (T2's site).
    update(&rig, oid, 4, 3, &blob(14, 20_000));
    assert_eq!(
        visible_chunk_keys(&rig, sid).len(),
        4,
        "the old 3 are gone, the new 3 are live"
    );
    assert!(
        check_now(&rig, oid).is_clean(),
        "the replace leaves no orphan"
    );

    // A delete removes the row and its chunks.
    delete(&rig, oid, 4);
    assert_eq!(
        visible_chunk_keys(&rig, sid).len(),
        1,
        "row 4's chunks died"
    );
    assert!(
        check_now(&rig, oid).is_clean(),
        "the delete leaves no orphan"
    );

    // The EPQ skip (T7): an update of a concurrently deleted row writes
    // no chunk.
    add_row(&rig, oid, 5, "x", "small");
    {
        let u = rig.rc();
        let s_u = rig.core().visible_ts();
        let st_u = ok(Stmt::new(&u, s_u));
        delete(&rig, oid, 5);
        assert!(
            !ok(rig.db.update(
                &st_u,
                oid,
                &[i32_v(5)],
                &[set_col(3, text_d(&blob(15, 20_000)))]
            )),
            "the update skips the concurrently deleted row"
        );
        ok(rig.commit_rc(u));
    }
    assert_eq!(
        visible_chunk_keys(&rig, sid).len(),
        1,
        "the skipped statement wrote no chunk (T7)"
    );
    assert!(check_now(&rig, oid).is_clean(), "the skip leaves no chunk");
}

/// A dropped toasted column's chunks stay live (no DDL rewrites rows) and
/// stay **named**: the raw sweep sees the pointer in the dropped attnum,
/// so I-TOAST holds — before the sweep (T8: a live-columns `decode_row`
/// sweep would report these live chunks as false orphans), after the
/// sweep that tombstones them, and after the column is re-added under a
/// fresh attnum with a new toasted value beside the old raw pointer.
#[test]
fn dropped_toasted_column_stays_clean_across_drop_sweep_and_readd() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_row(&rig, oid, 1, "x", &blob(20, 20_000));
    let p = row_pointers(&rig, sid, 1).remove(0);
    assert_eq!(p.chunks, 3);

    // DROP COLUMN big: the chunks stay (no rewrite) and the raw row
    // still names them.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.drop_column(&st, oid, 3));
        ok(rig.commit_rc(t));
    }
    assert_eq!(
        visible_chunk_keys(&rig, sid).len(),
        3,
        "the DDL rewrites no row; the chunks stay live"
    );
    let after_drop = check_now(&rig, oid);
    assert!(
        after_drop.is_clean(),
        "T8: the dropped column's live chunks are named by its raw pointer:\n{:?}",
        after_drop.violations
    );

    // The next write of the row does not carry the dropped pointer: its
    // chunks are tombstoned, and I-TOAST still holds.
    update(&rig, oid, 1, 2, "y");
    assert!(
        visible_chunk_keys(&rig, sid).is_empty(),
        "the sweep tombstoned the dropped column's chunks"
    );
    assert!(check_now(&rig, oid).is_clean(), "after the sweep");

    // Re-add `big` (a fresh attnum — attnums are never reused): a new
    // toasted value there lives beside the NULL the raw row still holds
    // at the dropped attnum.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig
            .db
            .add_column(&st, oid, "big", KeyType::Text(Collation::C), false, None));
        ok(rig.commit_rc(t));
    }
    update(&rig, oid, 1, 4, &blob(21, 20_000));
    let named: BTreeSet<_> = row_pointers(&rig, sid, 1)
        .iter()
        .flat_map(|p| (0..p.chunks).map(move |i| chunk_key(sid, &p.id, i)))
        .collect();
    assert_eq!(named.len(), 3, "one pointer, at the new attnum");
    assert_eq!(
        visible_chunk_keys(&rig, sid).len(),
        3,
        "exactly the new value's chunks are live"
    );
    assert!(check_now(&rig, oid).is_clean(), "after the re-add");
}

/// A table with no large value at all (the shape of every other suite's
/// fixture) checks clean: an empty chunk range, no pointer anywhere.
#[test]
fn inline_only_table_stays_clean() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"plain", &pk_schema()));
    ok(rig.commit_rc(t0));
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(1), text_d("p")]));
        ok(rig.commit_rc(t));
    }
    assert!(visible_chunk_keys(&rig, sid).is_empty());
    let report = check_now(&rig, oid);
    assert!(report.is_clean(), "{:?}", report.violations);
}
