//! C-T6d1: large values (§5) — the threshold and chunk layout, ids drawn
//! from the statement's ordinal counter, chunk writes after the row op's
//! `Applied` and deletes through `Core::delete_entry`, the detoast read
//! rules (at `S`, at an EPQ base's `register_at(v.ts)`, never a SIREAD),
//! TRUNCATE retiring chunks with the table range, and the dropped-column
//! raw-pointer sweep. Every test names the mutant it kills (T1–T10 of the
//! card's binding list; the card names no T8).
//!
//! C-T6d1fix: the gate follow-ups (F-1/F-3) — the same detoast read
//! rules pinned through the lenses the card's tests left silent
//! (`Db::scan`, `Db::index_scan`, an EPQ base above which a writer
//! committed: mutants G2, G9) and the sum-against-`raw_len` oracle of
//! `read_pointer` (G10).

mod t6b_support;

use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;

use t6b_support::{
    core_plant_version, i32_d, i32_v, idx_def, ok, pk_schema, plant_live_version, row_key_of,
    set_col, text_d, Rig, SharedFault, ThreadedRig,
};

use nucleus_catalog::{chunk_key, Db, IndexState, Stmt, TOAST_CHUNK, TOAST_THRESHOLD};
use nucleus_codec::row::{decode_row_raw, ColumnDef, Datum, RawDatum, TableSchema, ToastPointer};
use nucleus_codec::{Collation, KeyType, Value};
use nucleus_kv::fault::Fault;
use nucleus_kv::{Key, MemKv, OrderedKv as _, Snapshot as _};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{table_range, Catalog, RetiredRow};
use nucleus_txn::encoding::{parse_key, Entry};
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::jobs::{DropCleanup, JobOutcome};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::read::{read_key, scan, NoSsi};
use nucleus_txn::ssi::Siread;
use nucleus_txn::txn::Txn;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnError, TxnId};

// ---- fixtures and raw-storage oracles ---------------------------------------

/// The rig surface the shared fixtures drive — both the hand-driven
/// [`Rig`] and the commit-**threaded** [`ThreadedRig`]. The TRUNCATE test
/// drives [`DropCleanup`], whose step commits its marker delete through
/// the pipeline ([`Core::commit`] blocks for the ack), so it needs a rig
/// with a live commit thread; every other test runs on the hand-driven
/// one.
trait ToastRig {
    fn core(&self) -> &Arc<Core<SharedFault>>;
    fn kv(&self) -> &Arc<Fault<MemKv>>;
    fn db(&self) -> &Db<SharedFault>;
    fn rc(&self) -> Txn;
    fn stmt<'t>(&self, txn: &'t Txn, s: Ts) -> Stmt<'t>;
    fn commit_rc(&self, txn: Txn) -> Result<Ts, TxnError>;
}

impl ToastRig for Rig {
    fn core(&self) -> &Arc<Core<SharedFault>> {
        Rig::core(self)
    }
    fn kv(&self) -> &Arc<Fault<MemKv>> {
        &self.kv
    }
    fn db(&self) -> &Db<SharedFault> {
        &self.db
    }
    fn rc(&self) -> Txn {
        Rig::rc(self)
    }
    fn stmt<'t>(&self, txn: &'t Txn, s: Ts) -> Stmt<'t> {
        Rig::stmt(self, txn, s)
    }
    fn commit_rc(&self, txn: Txn) -> Result<Ts, TxnError> {
        Rig::commit_rc(self, txn)
    }
}

impl ToastRig for ThreadedRig {
    fn core(&self) -> &Arc<Core<SharedFault>> {
        ThreadedRig::core(self)
    }
    fn kv(&self) -> &Arc<Fault<MemKv>> {
        &self.kv
    }
    fn db(&self) -> &Db<SharedFault> {
        &self.db
    }
    fn rc(&self) -> Txn {
        ThreadedRig::rc(self)
    }
    fn stmt<'t>(&self, txn: &'t Txn, s: Ts) -> Stmt<'t> {
        ThreadedRig::stmt(self, txn, s)
    }
    fn commit_rc(&self, txn: Txn) -> Result<Ts, TxnError> {
        ThreadedRig::commit_rc(self, txn)
    }
}

/// `CREATE TABLE t (id int4 PRIMARY KEY, a text, big text)` — the toast
/// fixture: `big` carries the large values, `a` is the column an update
/// can touch instead.
fn wide_schema() -> TableSchema {
    let col = |attnum: u16, name: &str, ty: KeyType, not_null: bool| ColumnDef {
        attnum,
        name: name.into(),
        ty,
        not_null,
        dropped: false,
        missing: None,
    };
    ok(TableSchema::new(
        vec![
            col(1, "id", KeyType::Int4, true),
            col(2, "a", KeyType::Text(Collation::C), false),
            col(3, "big", KeyType::Text(Collation::C), false),
        ],
        vec![1],
    ))
}

/// A large text of `len` bytes, all one ASCII letter `'a' + (tag % 26)`
/// — distinct per tag, always one byte per char.
fn blob(tag: u8, len: usize) -> String {
    char::from(b'a' + (tag % 26)).to_string().repeat(len)
}

/// The chunk-key range of storage `sid`: `[table_lo ‖ 0xFF, table_hi)`
/// (§5 — everything past the mark is a chunk, none of it a row).
fn chunk_bounds(sid: u64) -> (Key, Key) {
    let (lo, hi) = table_range(sid);
    let mut clo = lo.clone();
    clo.push(0xFF);
    (clo, hi)
}

/// Every raw stored entry (intent or version) in `[lo, hi)`:
/// `(logical key, Some(ts) for a version / None for an intent, value
/// header byte)` in stored order.
fn raw_entries_in<R: ToastRig + ?Sized>(
    rig: &R,
    lo: &[u8],
    hi: &[u8],
) -> Vec<(Key, Option<Ts>, u8)> {
    let snap = rig.kv().snapshot();
    let mut out = Vec::new();
    for e in snap.scan((Bound::Included(lo), Bound::Excluded(hi)), false) {
        let (stored, value) = ok(e);
        let header = value.first().copied().unwrap_or(0xFF);
        match parse_key(&stored) {
            Some((l, Entry::Version(ts))) => out.push((l.to_vec(), Some(ts), header)),
            Some((l, Entry::Intent)) => out.push((l.to_vec(), None, header)),
            None => {}
        }
    }
    out
}

/// The distinct logical chunk keys of storage `sid` (any stored entry).
fn raw_chunk_keys<R: ToastRig + ?Sized>(rig: &R, sid: u64) -> Vec<Key> {
    let (lo, hi) = chunk_bounds(sid);
    raw_entries_in(rig, &lo, &hi)
        .into_iter()
        .map(|(k, _, _)| k)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The raw stored versions of one logical key (ts desc, header).
fn raw_versions_of<R: ToastRig + ?Sized>(rig: &R, key: &[u8]) -> Vec<(Option<Ts>, u8)> {
    let mut end = key.to_vec();
    end.push(0x02); // encoding::end_key
    raw_entries_in(rig, key, &end)
        .into_iter()
        .map(|(_, ts, h)| (ts, h))
        .collect()
}

/// The raw stored versions of one index entry logical key.
fn idx_versions_of(rig: &Rig, isid: u64, key: &[u8]) -> Vec<(Option<Ts>, u8)> {
    rig.raw_entry_versions(isid)
        .into_iter()
        .filter(|(k, ..)| k.as_slice() == key)
        .map(|(_, ts, header, _)| (ts, header))
        .collect()
}

/// The visible chunk rows of storage `sid` at `visible_ts` (§4).
fn visible_chunks<R: ToastRig + ?Sized>(rig: &R, sid: u64) -> Vec<(Key, Vec<u8>)> {
    let (lo, hi) = chunk_bounds(sid);
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
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        &ctx,
        &mut NoSsi,
    )
    .collect::<Result<Vec<_>, _>>())
}

/// The keys of [`visible_chunks`].
fn visible_chunk_keys<R: ToastRig + ?Sized>(rig: &R, sid: u64) -> Vec<Key> {
    visible_chunks(rig, sid)
        .into_iter()
        .map(|(k, _)| k)
        .collect()
}

/// A reader id this process never allocates (the support module's ghost).
fn ghost<R: ToastRig + ?Sized>(rig: &R) -> TxnId {
    TxnId {
        epoch: rig.core().epoch(),
        n: u64::MAX,
    }
}

/// The toast pointers a stored row names, raw decode, dropped attnums
/// included (`None` row = not visible at `visible_ts`).
fn row_pointers<R: ToastRig + ?Sized>(
    rig: &R,
    sid: u64,
    schema: &TableSchema,
    pk: i32,
) -> Vec<ToastPointer> {
    let key = row_key_of(sid, schema, &[i32_v(pk)]);
    let view = rig.core().open_view();
    let ctx = ReadCtx {
        txn: ghost(rig),
        snapshot: rig.core().visible_ts(),
        stmt_seq: 0,
    };
    let bytes = ok(read_key(rig.core(), &view, &key, &ctx, &mut NoSsi)).expect("row bytes");
    let (_, raw) = ok(decode_row_raw(&bytes));
    raw.into_iter()
        .flatten()
        .filter_map(|d| match d {
            RawDatum::Toast(p) => Some(p),
            RawDatum::Inline(_) => None,
        })
        .collect()
}

/// Creates the wide table, committed; returns `(oid, sid)`.
fn mk_wide<R: ToastRig + ?Sized>(rig: &R) -> (u64, u64) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = ok(Stmt::new(&t, s));
    let r = ok(rig.db().create_table(&st, b"wide", &wide_schema()));
    ok(rig.commit_rc(t));
    r
}

/// One committed insert of `(id, a, big)` in its own txn.
fn add_wide_row<R: ToastRig + ?Sized>(rig: &R, oid: u64, id: i32, a: &str, big: &str) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    ok(rig
        .db()
        .insert_by_oid(&st, oid, &[i32_d(id), text_d(a), text_d(big)]));
    ok(rig.commit_rc(t));
}

/// `(a, big)` of `id` read in a fresh RC txn (detoasted).
fn read_wide<R: ToastRig + ?Sized>(rig: &R, oid: u64, id: i32) -> (String, String) {
    let (a, b) = match &wide_row(rig, oid, id)[..] {
        [_, Some(Datum::Inline(Value::Text(a))), Some(Datum::Inline(Value::Text(b)))] => {
            (a.clone(), b.clone())
        }
        other => panic!("row does not decode: {other:?}"),
    };
    (a, b)
}

/// `Db::get` of the wide row in a fresh RC txn.
fn wide_row<R: ToastRig + ?Sized>(rig: &R, oid: u64, id: i32) -> Vec<Option<Datum>> {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db().open_table(&st, oid, RelLockMode::AccessShare));
    let row = ok(rig.db().get(&st, &h, &[i32_v(id)]));
    ok(rig.commit_rc(t));
    row.expect("row")
}

// ---- the layout and the round-trip ------------------------------------------

/// §5's threshold: value bytes **exceeding** 2048 go out of line (2048
/// itself stays inline), chunk `i` is 8192 bytes with the last shorter,
/// and the id is `be32(epoch) be64(txn n) be32(seq0) be32(ord)` of the
/// inserting statement — pinned byte for byte.
#[test]
fn threshold_minus_one_at_and_plus_one_and_multichunk_round_trip() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    ok(rig.db.insert_by_oid(
        &st,
        oid,
        &[i32_d(1), text_d("a"), text_d(&blob(0, TOAST_THRESHOLD - 1))],
    ));
    ok(rig.db.insert_by_oid(
        &st,
        oid,
        &[i32_d(2), text_d("a"), text_d(&blob(1, TOAST_THRESHOLD))],
    ));
    ok(rig.db.insert_by_oid(
        &st,
        oid,
        &[i32_d(3), text_d("a"), text_d(&blob(2, TOAST_THRESHOLD + 1))],
    ));
    let txn_id = t.id;
    let seq0 = st.seq0();
    ok(rig.commit_rc(t));

    // 2047 and 2048 stay inline; 2049 is one pointer to one chunk.
    assert!(
        row_pointers(&rig, sid, &wide_schema(), 1).is_empty(),
        "2047 bytes stay inline"
    );
    assert!(
        row_pointers(&rig, sid, &wide_schema(), 2).is_empty(),
        "2048 bytes stay inline (only a value exceeding it goes out of line)"
    );
    let ptrs = row_pointers(&rig, sid, &wide_schema(), 3);
    assert_eq!(ptrs.len(), 1, "2049 bytes go out of line");
    assert_eq!(ptrs[0].raw_len as usize, TOAST_THRESHOLD + 1);
    assert_eq!(ptrs[0].chunks, 1, "one chunk");
    assert_eq!(raw_chunk_keys(&rig, sid).len(), 1, "one chunk key stored");
    // The id: epoch ‖ txn n ‖ seq0 ‖ ord — the statement's first ord
    // (the inline inserts drew none).
    assert_eq!(&ptrs[0].id[..4], &txn_id.epoch.to_be_bytes(), "epoch");
    assert_eq!(&ptrs[0].id[4..12], &txn_id.n.to_be_bytes(), "txn n");
    assert_eq!(&ptrs[0].id[12..16], &seq0.to_be_bytes(), "seq0");
    assert_eq!(&ptrs[0].id[16..], &0u32.to_be_bytes(), "the first ord");

    assert_eq!(
        read_wide(&rig, oid, 3).1,
        blob(2, TOAST_THRESHOLD + 1),
        "the 2049-byte value round-trips"
    );

    // Multi-chunk: 20000 bytes = 3 chunks (8192, 8192, 3616).
    add_wide_row(&rig, oid, 4, "a", &blob(3, 20_000));
    let ptrs4 = row_pointers(&rig, sid, &wide_schema(), 4);
    assert_eq!(ptrs4.len(), 1);
    assert_eq!(ptrs4[0].chunks, 3, "three chunks");
    assert_eq!(raw_chunk_keys(&rig, sid).len(), 4, "1 + 3 chunk keys");
    let vis = visible_chunks(&rig, sid);
    for i in 0..ptrs4[0].chunks {
        let ck = chunk_key(sid, &ptrs4[0].id, i);
        let found = vis.iter().find(|(k, _)| k == &ck).expect("chunk visible");
        let want = if i + 1 < ptrs4[0].chunks {
            TOAST_CHUNK
        } else {
            20_000 - 2 * TOAST_CHUNK
        };
        assert_eq!(found.1.len(), want, "chunk {i} length");
    }
    assert_eq!(read_wide(&rig, oid, 4).1, blob(3, 20_000));
}

/// T6: `ord` not incremented between a statement's toast assignments —
/// two over-threshold columns of one statement must draw distinct ids
/// (the statement's single ordinal counter), each round-tripping its own
/// value; under the mutant both share one id and the second column's
/// chunks shadow the first's.
#[test]
fn two_big_columns_in_one_statement() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    ok(rig.db.insert_by_oid(
        &st,
        oid,
        &[
            i32_d(1),
            text_d(&blob(10, 20_000)),
            text_d(&blob(11, 20_000)),
        ],
    ));
    ok(rig.commit_rc(t));

    let ptrs = row_pointers(&rig, sid, &wide_schema(), 1);
    assert_eq!(ptrs.len(), 2, "both columns out of line");
    let ids: BTreeSet<[u8; 20]> = ptrs.iter().map(|p| p.id).collect();
    assert_eq!(ids.len(), 2, "distinct ids (T6)");
    assert_eq!(
        raw_chunk_keys(&rig, sid).len(),
        6,
        "3 + 3 disjoint chunk keys (T6)"
    );
    let (a, b) = read_wide(&rig, oid, 1);
    assert_eq!(a, blob(10, 20_000), "column a keeps its own value (T6)");
    assert_eq!(b, blob(11, 20_000), "column b keeps its own value (T6)");
}

/// T1: unchanged toasted columns rewritten — an update that does not name
/// `big` carries its pointer, so the very same chunk keys remain, one
/// live version each (no new id, no rewrite, no tombstone).
#[test]
fn update_of_another_column_keeps_chunk_keys() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(20, 20_000));
    let before = raw_chunk_keys(&rig, sid);
    assert_eq!(before.len(), 3);

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    assert!(ok(rig.db.update(
        &st,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d("y"))]
    )));
    ok(rig.commit_rc(t));

    let after = raw_chunk_keys(&rig, sid);
    assert_eq!(before, after, "the chunk keys are unchanged (T1)");
    for k in &after {
        let versions = raw_versions_of(&rig, k);
        assert_eq!(
            versions.len(),
            1,
            "no rewrite, no tombstone: one stored entry per chunk (T1)"
        );
        assert_eq!(versions[0].1, 0x00, "the one entry is live (T1)");
    }
    assert_eq!(read_wide(&rig, oid, 1).1, blob(20, 20_000));
}

/// T2: old chunks not deleted on replace — a replace moves to a new id
/// and every un-carried old chunk is tombstoned at the statement's commit
/// ts (raw read); the delete of the row then tombstones the new ones and
/// no visible chunk remains.
#[test]
fn replace_and_delete_tombstone_old_chunks() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(30, 20_000));
    let ts1 = rig.core().visible_ts();
    let old = row_pointers(&rig, sid, &wide_schema(), 1).remove(0);
    let old_keys: Vec<Key> = (0..old.chunks)
        .map(|i| chunk_key(sid, &old.id, i))
        .collect();

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    assert!(ok(rig.db.update(
        &st,
        oid,
        &[i32_v(1)],
        &[set_col(3, text_d(&blob(31, 20_000)))]
    )));
    let ts2 = ok(rig.commit_rc(t));

    let new = row_pointers(&rig, sid, &wide_schema(), 1).remove(0);
    assert_ne!(new.id, old.id, "the replace assigns a fresh id");
    for k in &old_keys {
        assert_eq!(
            raw_versions_of(&rig, k),
            vec![(Some(ts2), 0x02), (Some(ts1), 0x00)],
            "old chunk {k:?} tombstoned at the replace's commit ts (T2)"
        );
    }
    assert_eq!(
        visible_chunk_keys(&rig, sid).len(),
        new.chunks as usize,
        "only the new chunks are visible"
    );

    // The delete removes the new ones too.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    assert!(ok(rig.db.delete(&st2, oid, &[i32_v(1)])));
    let ts3 = ok(rig.commit_rc(t2));
    for i in 0..new.chunks {
        let k = chunk_key(sid, &new.id, i);
        assert_eq!(
            raw_versions_of(&rig, &k),
            vec![(Some(ts3), 0x02), (Some(ts2), 0x00)],
            "the delete tombstones the carried pointer's chunks"
        );
    }
    assert!(
        visible_chunk_keys(&rig, sid).is_empty(),
        "no visible chunk remains"
    );
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st3 = rig.stmt(&t3, s3);
    let h = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    assert!(ok(rig.db.scan(&st3, &h)).is_empty(), "the row is gone");
    ok(rig.commit_rc(t3));
}

/// §5's atomicity half: an aborted txn and a `ROLLBACK TO` leave no
/// visible chunk — the chunks are intents of the same statement and die
/// with it.
#[test]
fn abort_and_rollback_to_leave_no_chunk() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);

    // Abort: the intents (row and chunks) are cleaned, nothing resolves.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.insert_by_oid(
            &st,
            oid,
            &[i32_d(1), text_d("x"), text_d(&blob(40, 20_000))],
        ));
        rig.abort_txn(t);
        assert!(
            raw_entries_in_chunk_range(&rig, sid).is_empty(),
            "the abort leaves no chunk"
        );
    }

    // ROLLBACK TO before the insert: the statement's layers are dropped.
    {
        let t = rig.rc();
        let sp = ok(t.savepoint());
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.insert_by_oid(
            &st,
            oid,
            &[i32_d(1), text_d("x"), text_d(&blob(41, 20_000))],
        ));
        ok(rig.core().rollback_to(&t, sp));
        ok(rig.commit_rc(t));
        assert!(
            raw_entries_in_chunk_range(&rig, sid).is_empty(),
            "ROLLBACK TO leaves no chunk"
        );
    }
}

/// Every raw stored entry in the chunk range (ints and versions both —
/// the strictest "no chunk anywhere" oracle).
fn raw_entries_in_chunk_range<R: ToastRig + ?Sized>(
    rig: &R,
    sid: u64,
) -> Vec<(Key, Option<Ts>, u8)> {
    let (lo, hi) = chunk_bounds(sid);
    raw_entries_in(rig, &lo, &hi)
}

/// T3: detoast at `visible_ts` — an RR reader at `S` reads the old value
/// after a concurrent replace committed: the row and its chunks are read
/// at `S`, where the new version and the old chunks' tombstones are
/// invisible. (At `visible_ts` the tombstones would win and the read
/// would report corruption instead of the old value.)
#[test]
fn rr_reader_reads_the_old_value_after_a_concurrent_replace() {
    let rig = Rig::new();
    let (oid, _sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(50, 20_000));

    let (r, guard, s) = rig.rr();

    let w2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&w2, s2);
    assert!(ok(rig.db.update(
        &st2,
        oid,
        &[i32_v(1)],
        &[set_col(3, text_d(&blob(51, 20_000)))]
    )));
    let ts2 = ok(rig.commit_rc(w2));
    assert!(ts2 > s, "the replace commits after the reader's snapshot");

    let st_r = ok(Stmt::new(&r, s));
    let h = ok(rig.db.open_table(&st_r, oid, RelLockMode::AccessShare));
    assert_eq!(
        ok(rig.db.get(&st_r, &h, &[i32_v(1)])),
        Some(vec![i32_d(1), text_d("x"), text_d(&blob(50, 20_000))]),
        "the RR reader detoasts the old value at its snapshot (T3)"
    );
    ok(rig.core().abort(r));
    rig.settle();
    drop(guard);
}

/// G2 (the C-T6d1 gate's F-1): the T3 shape through `Db::scan` — the
/// same RR reader at `S`, the same concurrent replace, but the rows come
/// back through the scan path, whose detoast is its own site (a row
/// read at `S` detoasts at `S`, whatever `visible_ts` says). Detoasting
/// at `visible_ts` instead would hit the old chunks' tombstones (placed
/// by the replace) and report XX001 rather than the old value.
#[test]
fn rr_scan_reads_the_old_value_after_a_concurrent_replace() {
    let rig = Rig::new();
    let (oid, _sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(52, 20_000));

    let (r, guard, s) = rig.rr();

    let w2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&w2, s2);
    assert!(ok(rig.db.update(
        &st2,
        oid,
        &[i32_v(1)],
        &[set_col(3, text_d(&blob(53, 20_000)))]
    )));
    let ts2 = ok(rig.commit_rc(w2));
    assert!(ts2 > s, "the replace commits after the reader's snapshot");

    let st_r = ok(Stmt::new(&r, s));
    let h = ok(rig.db.open_table(&st_r, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st_r, &h));
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1,
        vec![i32_d(1), text_d("x"), text_d(&blob(52, 20_000))],
        "the RR scan detoasts the old value at its snapshot (G2)"
    );
    ok(rig.core().abort(r));
    rig.settle();
    drop(guard);
}

/// T4: an EPQ base detoasted at `S` — an RC delete of a row with a
/// **toasted indexed** column after a concurrent replace (the EPQ base's
/// commit ts is above `S`): the old index entry is computed from the base
/// detoasted at `register_at(v.ts)`, so the delete succeeds, tombstones
/// the base's entry and leaves no stale one. (At `S` the base's chunks
/// are invisible and the delete would raise XX001 instead.)
#[test]
fn rc_delete_toasted_indexed_column_after_concurrent_replace() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));
    // A Public /i/ index on the payload column.
    let (_ix, isid) = rig.add_index(oid, &idx_def(2, false, IndexState::Public));

    let ts1 = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig
            .db
            .insert_by_oid(&st, oid, &[i32_d(1), text_d(&blob(60, 2100))]));
        ok(rig.commit_rc(t))
    };
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let text_col = [nucleus_codec::KeyColumn::asc(KeyType::Text(Collation::C))];
    let e1 = t6b_support::i_entry_key(isid, &text_col, &[Some(Value::Text(blob(60, 2100)))], &key1);

    // The deleter's snapshot, taken before the replace.
    let d = rig.rc();
    let s_d = rig.core().visible_ts();
    let st_d = ok(Stmt::new(&d, s_d));

    // The replace: a fresh id and entry, the old entry tombstoned.
    let ts2 = {
        let w = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&w, s);
        assert!(ok(rig.db.update(
            &st,
            oid,
            &[i32_v(1)],
            &[set_col(2, text_d(&blob(61, 2100)))]
        )));
        ok(rig.commit_rc(w))
    };
    let e2 = t6b_support::i_entry_key(isid, &text_col, &[Some(Value::Text(blob(61, 2100)))], &key1);
    assert!(ts2 > s_d, "the replace committed above the deleter's S");

    // The delete: Ok(true) — no XX001 (T4), no 40001.
    assert!(
        ok(rig.db.delete(&st_d, oid, &[i32_v(1)])),
        "the delete of the EPQ base's row applies"
    );
    let ts3 = ok(rig.commit_rc(d));

    assert!(
        rig.scan_entries(isid).is_empty(),
        "no stale entry remains (the base's entry was deleted)"
    );
    assert_eq!(
        idx_versions_of(&rig, isid, &e1),
        vec![(Some(ts2), 0x02), (Some(ts1), 0x00)],
        "the replace tombstoned the insert's entry"
    );
    assert_eq!(
        idx_versions_of(&rig, isid, &e2),
        vec![(Some(ts3), 0x02), (Some(ts2), 0x00)],
        "the delete tombstoned the EPQ base's entry, computed from the base (T4)"
    );
    assert!(
        visible_chunk_keys(&rig, sid).is_empty(),
        "the base's chunks died with the row"
    );
}

/// G9 (the C-T6d1 gate's F-1): the T4 shape with a writer above the EPQ
/// base — the base's chunks carry a tombstone that is visible at
/// `visible_ts` but not at the base's own commit ts, so the base detoast
/// must read them at `register_at(v.ts)` and nowhere else. The tombstone
/// above the base is planted raw (`core_plant_version`, the corruption
/// oracle's primitive): a legal history cannot produce one — any writer
/// that tombstones the base's chunks first writes a newer row version,
/// which the row op's EPQ would take as its base instead — but the read
/// rule is what keeps the base's detoast immune to everything above
/// `v.ts`, a planted tombstone included. (Reading at `visible_ts` — the
/// gate's G9 — hits the planted tombstone: XX001, the delete fails.
/// Reading at the reader's `S` is T4's own mutant.)
#[test]
fn rc_delete_epq_base_detoasts_at_the_base_ts_above_a_later_writer() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));
    // A Public /i/ index on the payload column.
    let (_ix, isid) = rig.add_index(oid, &idx_def(2, false, IndexState::Public));

    let ts1 = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig
            .db
            .insert_by_oid(&st, oid, &[i32_d(1), text_d(&blob(62, 2100))]));
        ok(rig.commit_rc(t))
    };
    let key1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let text_col = [nucleus_codec::KeyColumn::asc(KeyType::Text(Collation::C))];
    let e1 = t6b_support::i_entry_key(isid, &text_col, &[Some(Value::Text(blob(62, 2100)))], &key1);

    // The deleter's snapshot, taken before the replace.
    let d = rig.rc();
    let s_d = rig.core().visible_ts();
    let st_d = ok(Stmt::new(&d, s_d));

    // The replace: a fresh id and entry; the EPQ base is its version.
    let ts2 = {
        let w = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&w, s);
        assert!(ok(rig.db.update(
            &st,
            oid,
            &[i32_v(1)],
            &[set_col(2, text_d(&blob(63, 2100)))]
        )));
        ok(rig.commit_rc(w))
    };
    let e2 = t6b_support::i_entry_key(isid, &text_col, &[Some(Value::Text(blob(63, 2100)))], &key1);
    assert!(ts2 > s_d, "the replace committed above the deleter's S");

    // The writer above the EPQ base: a commit on another table moves
    // `visible_ts` past `ts2` (the discriminator the T4 fixture lacks),
    // and a raw-planted tombstone on the base's chunks at that ts is
    // visible at `visible_ts` but not at `register_at(ts2)`.
    let ts3 = {
        let hb = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&hb, s);
        ok(rig.db.create_table(&st, b"hb", &pk_schema()).map(|_| ()));
        ok(rig.commit_rc(hb))
    };
    assert!(ts3 > ts2, "the heartbeat committed above the EPQ base");
    let base = row_pointers(&rig, sid, &pk_schema(), 1).remove(0);
    assert_eq!(base.chunks, 1, "the 2100-byte payload is one chunk");
    for i in 0..base.chunks {
        core_plant_version(&rig, &chunk_key(sid, &base.id, i), ts3, 0x02, b"");
    }

    // The delete: Ok(true) — no XX001 at `visible_ts` (G9), none at `S`
    // (T4's mutant); the base's entry is computed from the base
    // detoasted at `register_at(ts2)`, where its chunks still live.
    assert!(
        ok(rig.db.delete(&st_d, oid, &[i32_v(1)])),
        "the delete detoasts the EPQ base at the base's own ts (G9)"
    );
    let ts_del = ok(rig.commit_rc(d));

    assert!(
        rig.scan_entries(isid).is_empty(),
        "no stale entry remains (the base's entry was deleted)"
    );
    assert_eq!(
        idx_versions_of(&rig, isid, &e1),
        vec![(Some(ts2), 0x02), (Some(ts1), 0x00)],
        "the replace tombstoned the insert's entry"
    );
    assert_eq!(
        idx_versions_of(&rig, isid, &e2),
        vec![(Some(ts_del), 0x02), (Some(ts2), 0x00)],
        "the delete tombstoned the EPQ base's entry, computed from the base at its own ts (G9)"
    );
    assert!(
        visible_chunk_keys(&rig, sid).is_empty(),
        "the base's chunks died with the row"
    );
}

/// FU-1c (the C-T6d1 gate's F-1, optional leg): the same T3 shape
/// through `Db::index_scan` — an RR reader at `S` over an index on the
/// toasted column itself, after a concurrent replace committed. The
/// entries, the row fetches and the §6 rebuild-verify all run at `S`, so
/// the old value's entry verifies and the scan returns the old value. A
/// detoast that read chunks at `visible_ts` here fails loudly: the
/// fetched row's chunks are tombstoned there, and the §6 re-encode of
/// the detoasted value no longer matches the entry (XX001).
#[test]
fn rr_index_scan_reads_the_old_value_after_a_concurrent_replace() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st0, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));
    // A Public /i/ index on the payload column — the toasted one.
    let (ix, _isid) = rig.add_index(oid, &idx_def(2, false, IndexState::Public));

    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig
            .db
            .insert_by_oid(&st, oid, &[i32_d(1), text_d(&blob(64, 2100))]));
        ok(rig.commit_rc(t));
    }

    let (r, guard, s) = rig.rr();

    let w2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&w2, s2);
    assert!(ok(rig.db.update(
        &st2,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d(&blob(65, 2100)))]
    )));
    let ts2 = ok(rig.commit_rc(w2));
    assert!(ts2 > s, "the replace commits after the reader's snapshot");

    let st_r = ok(Stmt::new(&r, s));
    let rows = ok(rig
        .db
        .index_scan(&st_r, ix, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(rows.len(), 1, "only the snapshot-visible entry serves");
    assert_eq!(
        rows[0].1,
        vec![i32_d(1), text_d(&blob(64, 2100))],
        "the RR index scan detoasts the old value at its snapshot (FU-1c)"
    );
    ok(rig.core().abort(r));
    rig.settle();
    drop(guard);
}

/// T5: old pointers from a live-columns decode — DROP COLUMN of a toasted
/// column, then an update of another column: the raw row of the EPQ base
/// still names the dropped column's pointer (no DDL rewrites rows), the
/// update does not carry it, and its chunks are tombstoned — after GC no
/// chunk is left. (A `decode_row` sweep never sees a dropped attnum's
/// pointer and the chunks survive the GC.)
#[test]
fn drop_column_update_gc_leaves_no_chunk() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(70, 20_000));
    assert_eq!(raw_chunk_keys(&rig, sid).len(), 3, "the chunks exist");

    // DROP COLUMN big.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.drop_column(&st, oid, 3));
        ok(rig.commit_rc(t));
    }
    assert_eq!(
        raw_chunk_keys(&rig, sid).len(),
        3,
        "the DDL rewrites no row; the chunks stay until the next write"
    );

    // The update of `a` rewrites the row at the current def — the dropped
    // column is written NULL, so the base's pointer is not carried.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        assert!(ok(rig.db.update(
            &st,
            oid,
            &[i32_v(1)],
            &[set_col(2, text_d("y"))]
        )));
        ok(rig.commit_rc(t));
    }
    assert_eq!(
        raw_chunk_keys(&rig, sid).len(),
        3,
        "the chunk keys remain as tombstoned versions, not yet collected"
    );
    assert!(
        visible_chunk_keys(&rig, sid).is_empty(),
        "no chunk is live after the update (the pointer was not carried)"
    );

    // GC: publish, drop the tombstones, compact — nothing left (T5).
    let gc = ok(GcJob::install(rig.core(), GcConfig::default()));
    ok(gc.publish(0));
    ok(gc.drop_tombstones());
    rig.kv.with_live(|kv| kv.compact_all());
    assert!(
        raw_chunk_keys(&rig, sid).is_empty(),
        "no chunk left after GC (T5: the dropped column's pointer was swept from the raw row)"
    );

    // The row itself survives, decoded under the post-drop def.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st, &h));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("y")], "two live columns");
    ok(rig.commit_rc(t));
}

/// T7: chunks written before the row op — an RC update setting a 20 KB
/// value on a row a concurrent txn deleted: the row op EPQs onto the
/// tombstone, skips (`Ok(false)`) and **no chunk exists** under the table
/// range. (Written before the op, the chunks would survive the skip as
/// the txn's own intents.)
#[test]
fn epq_skip_leaves_no_chunk() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", "small");

    let u = rig.rc();
    let s_u = rig.core().visible_ts();
    let st_u = ok(Stmt::new(&u, s_u));

    let w = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&w, s);
    assert!(ok(rig.db.delete(&st, oid, &[i32_v(1)])));
    ok(rig.commit_rc(w));

    assert!(
        !ok(rig.db.update(
            &st_u,
            oid,
            &[i32_v(1)],
            &[set_col(3, text_d(&blob(80, 20_000)))]
        )),
        "the update skips the concurrently deleted row"
    );
    ok(rig.commit_rc(u));

    assert!(
        raw_entries_in_chunk_range(&rig, sid).is_empty(),
        "the skipped statement left no chunk (T7)"
    );
}

/// §5's placement: chunk keys live inside the table's storage range, so
/// TRUNCATE retires them with it — the retired row covers the range, the
/// drop-cleanup empties it (row and chunks), and the new storage's data
/// is untouched. Runs on [`ThreadedRig`]: the cleanup's step commits its
/// marker delete through the pipeline and blocks for the ack, which only
/// a live commit thread answers.
#[test]
fn truncate_retires_chunks_with_the_table_range() {
    let rig = ThreadedRig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(90, 20_000));
    let p = row_pointers(&rig, sid, &wide_schema(), 1).remove(0);
    assert_eq!(p.chunks, 3);
    // The chunk keys sit inside the table's storage range by construction.
    let (clo, _chi) = chunk_bounds(sid);
    let (tlo, thi) = table_range(sid);
    assert!(
        clo > tlo && clo < thi,
        "the chunk range is inside the table's"
    );
    for i in 0..p.chunks {
        let k = chunk_key(sid, &p.id, i);
        assert!(k >= tlo && k < thi, "chunk {k:?} inside the table range");
        assert!(k >= clo, "chunk {k:?} inside the chunk range");
    }

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let new_sid = ok(rig.catalog().catalog_truncate_table(&rig.ddl(&t, s), oid));
    ok(rig.commit_rc(t));
    assert_ne!(sid, new_sid, "TRUNCATE allocates a fresh storage");

    // The retirement record names exactly the old table range.
    {
        let view = rig.core().open_view();
        let ctx = ReadCtx {
            txn: ghost(&rig),
            snapshot: rig.core().visible_ts(),
            stmt_seq: 0,
        };
        let rows = ok(Catalog::scan_retired(rig.core(), &view, &ctx));
        assert_eq!(rows.len(), 1, "one retired row for the TRUNCATE");
        let RetiredRow { ranges, .. } = rows[0].1.clone();
        assert!(
            ranges.contains(&table_range(sid)),
            "the retired ranges cover the old table storage, chunks included"
        );
    }

    // Data in the new storage survives the cleanup.
    add_wide_row(&rig, oid, 2, "y", &blob(91, 20_000));
    assert_eq!(raw_chunk_keys(&rig, new_sid).len(), 3, "new chunks exist");

    // Drive the drop-cleanup: scan (gated), a heartbeat commit + publish
    // so W passes the scan, then clean to quiescence.
    let gc = std::sync::Arc::new(ok(GcJob::install(rig.core(), GcConfig::default())));
    let mut cleanup = DropCleanup::new(rig.core().clone(), gc.clone());
    ok(cleanup.run_once()); // scan at S; gated (S == W)
    add_wide_row(&rig, oid, 3, "z", "small"); // the heartbeat commit
    ok(gc.publish(0));
    for _ in 0..4 {
        if matches!(ok(cleanup.run_once()), JobOutcome::Idle) {
            break;
        }
    }
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Idle));

    let (lo, hi) = table_range(sid);
    assert!(
        raw_entries_in(&rig, &lo, &hi).is_empty(),
        "the retired range is raw-empty: rows and chunks (TRUNCATE retires them together)"
    );
    // Row 2's three chunks (row 3's heartbeat value is inline, none of
    // its own): the new storage's data is untouched by the cleanup.
    assert_eq!(
        raw_chunk_keys(&rig, new_sid).len(),
        3,
        "the new storage's chunks are untouched"
    );
    assert_eq!(read_wide(&rig, oid, 2).1, blob(91, 20_000));
    rig.shutdown();
}

/// §5: no SIREAD is ever registered on a chunk key — a SERIALIZABLE read
/// of a toasted row registers the row key only (`Ssi::sireads_of`), while
/// the detoast reads the chunks without touching SSI.
#[test]
fn ser_read_registers_no_siread_on_chunk_keys() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(100, 20_000));
    let chunks = raw_chunk_keys(&rig, sid);
    assert_eq!(chunks.len(), 3);
    let row_key = row_key_of(sid, &wide_schema(), &[i32_v(1)]);

    let ser = rig.ser(false);
    let st = ok(Stmt::new(&ser.txn, ser.s));
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    assert_eq!(
        ok(rig.db.get(&st, &h, &[i32_v(1)])),
        Some(vec![i32_d(1), text_d("x"), text_d(&blob(100, 20_000))]),
        "the SER read detoasts"
    );
    let sireads = rig.ssi().sireads_of(ser.id());
    assert!(
        sireads
            .iter()
            .any(|(s, _)| matches!(s, Siread::Point { key } if key == &row_key)),
        "the row's own point SIREAD registered (the read happened)"
    );
    for (s, _) in &sireads {
        for ck in &chunks {
            assert!(!covers(s, ck), "SIREAD {s:?} covers the chunk key {ck:?}");
        }
    }
    rig.abort_ser(ser);
}

/// Whether a SIREAD covers `k` (§8.1: a scan locks its bound, a point
/// read its key, a relation-level lock everything of the relation).
fn covers(s: &Siread, k: &[u8]) -> bool {
    match s {
        Siread::Point { key } => key.as_slice() == k,
        Siread::Range { lo, hi } => lo.as_slice() <= k && k < hi.as_slice(),
        Siread::Relation { .. } => true,
    }
}

/// T9: kept pointers taken from the snapshot row — an RC update of `a`
/// after a concurrent replace of `big` carries the **EPQ base's** pointer
/// (the version the row op acted on): `big` reads the concurrent txn's
/// value, every pointer the live row names has its chunks, and no live
/// chunk is orphaned. (From the snapshot row the new version would carry
/// the dead pointer and the read would corrupt; the live one would be
/// orphaned.)
#[test]
fn rc_update_other_column_after_concurrent_replace() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(110, 20_000));

    // W1's snapshot, taken before W2's replace.
    let w1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = ok(Stmt::new(&w1, s1));

    // W2 replaces big (a fresh id; the old chunks tombstoned at ts2).
    let ts2 = {
        let w2 = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&w2, s);
        assert!(ok(rig.db.update(
            &st,
            oid,
            &[i32_v(1)],
            &[set_col(3, text_d(&blob(111, 20_000)))]
        )));
        ok(rig.commit_rc(w2))
    };
    assert!(ts2 > s1);

    // W1 updates `a` — the row op EPQs onto W2's version; `big`'s pointer
    // is carried from that base.
    assert!(ok(rig.db.update(
        &st1,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d("y"))]
    )));
    ok(rig.commit_rc(w1));

    let (a, big) = read_wide(&rig, oid, 1);
    assert_eq!(a, "y");
    assert_eq!(
        big,
        blob(111, 20_000),
        "`big` reads the concurrent replace's value — the EPQ base's pointer (T9)"
    );

    // Raw invariants: every pointer the live row names has live chunks,
    // and no live chunk is orphaned.
    let ptrs = row_pointers(&rig, sid, &wide_schema(), 1);
    assert_eq!(ptrs.len(), 1, "one pointer in the live row");
    let mut named: BTreeSet<Key> = BTreeSet::new();
    for i in 0..ptrs[0].chunks {
        let k = chunk_key(sid, &ptrs[0].id, i);
        assert!(
            raw_versions_of(&rig, &k)
                .first()
                .is_some_and(|(_, h)| *h != 0x02),
            "the named chunk {k:?} is live"
        );
        named.insert(k);
    }
    let vis = visible_chunk_keys(&rig, sid);
    assert_eq!(vis.len(), 3, "exactly the three named chunks are live");
    for k in vis {
        assert!(named.contains(&k), "live chunk {k:?} is orphaned (T9)");
    }
}

/// T10: the toast ordinal from a per-call counter — two `update` calls of
/// ONE statement, each setting a distinct 20 KB value: the ids come from
/// the statement's shared ordinal counter, so both rows read back their
/// own value and the chunk-key sets are disjoint. (A counter reset per
/// call would mint one id twice and the second row would read the
/// first's chunks.)
#[test]
fn two_rows_one_stmt_toasted() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", "small");
    add_wide_row(&rig, oid, 2, "y", "small");

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    assert!(ok(rig.db.update(
        &st,
        oid,
        &[i32_v(1)],
        &[set_col(3, text_d(&blob(120, 20_000)))]
    )));
    assert!(ok(rig.db.update(
        &st,
        oid,
        &[i32_v(2)],
        &[set_col(3, text_d(&blob(121, 20_000)))]
    )));
    ok(rig.commit_rc(t));

    assert_eq!(
        read_wide(&rig, oid, 1).1,
        blob(120, 20_000),
        "row 1 reads its own value (T10)"
    );
    assert_eq!(
        read_wide(&rig, oid, 2).1,
        blob(121, 20_000),
        "row 2 reads its own value (T10)"
    );
    let keys = raw_chunk_keys(&rig, sid);
    assert_eq!(keys.len(), 6, "3 + 3 chunks");
    // Disjoint by id: the 20 bytes before the be32(i) suffix.
    let ids: BTreeSet<Vec<u8>> = keys
        .iter()
        .map(|k| k[k.len() - 24..k.len() - 4].to_vec())
        .collect();
    assert_eq!(ids.len(), 2, "two disjoint toast ids (T10)");
}

/// §5's corruption oracle: a missing chunk or a length mismatch is XX001,
/// never a partial or silently wrong value.
#[test]
fn corrupt_chunks_are_xx001() {
    let rig = Rig::new();
    let (oid, sid) = mk_wide(&rig);
    add_wide_row(&rig, oid, 1, "x", &blob(130, 20_000));
    let ts1 = rig.core().visible_ts();
    // A second commit so `visible_ts > ts1` (a tombstone above the
    // chunk's live version becomes plantable).
    add_wide_row(&rig, oid, 2, "y", "small");
    assert!(rig.core().visible_ts() > ts1);
    let p = row_pointers(&rig, sid, &wide_schema(), 1).remove(0);
    assert_eq!(p.chunks, 3);

    // (a) A middle chunk replaced by a short payload: length mismatch.
    let k1 = chunk_key(sid, &p.id, 1);
    plant_live_version(&rig, &k1, ts1, b"abc");
    assert_eq!(
        wide_row_sqlstate(&rig, oid, 1),
        "XX001",
        "a chunk length mismatch is corruption"
    );

    // (b) A missing chunk: a planted tombstone above the live version
    // makes chunk 2 read not-found.
    let now = rig.core().visible_ts();
    let k2 = chunk_key(sid, &p.id, 2);
    core_plant_version(&rig, &k2, now, 0x02, b"");
    assert_eq!(
        wide_row_sqlstate(&rig, oid, 1),
        "XX001",
        "a missing chunk is corruption"
    );

    // (c) A lying pointer (G10): a third row whose real 20000-byte value
    // lives as two full chunks and a shorter last one, but whose planted
    // row version says `raw_len = 10_000` over the same three chunks.
    // Every per-chunk length check passes — the chunks are the row's own,
    // untouched — and only the sum (8192 + 8192 + 3616 = 20000, not
    // 10000) trips: XX001 via "chunks do not sum to raw_len".
    let ts_row3 = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.insert_by_oid(
            &st,
            oid,
            &[i32_d(3), text_d("z"), text_d(&blob(131, 20_000))],
        ));
        ok(rig.commit_rc(t))
    };
    let p3 = row_pointers(&rig, sid, &wide_schema(), 3).remove(0);
    assert_eq!(p3.chunks, 3, "three real chunks");
    let lying = ToastPointer {
        id: p3.id,
        raw_len: 10_000,
        chunks: p3.chunks,
    };
    let row3 = row_key_of(sid, &wide_schema(), &[i32_v(3)]);
    let bytes = ok(wide_schema().encode_row(&[i32_d(3), text_d("z"), Some(Datum::Toast(lying))]));
    plant_live_version(&rig, &row3, ts_row3, &bytes);
    assert_eq!(
        wide_row_sqlstate(&rig, oid, 3),
        "XX001",
        "chunks that do not sum to raw_len are corruption (G10)"
    );
}

/// `Db::get` of the wide row in a fresh RC txn, as a SQLSTATE (Ok rows
/// panic — the oracle's callers expect corruption).
fn wide_row_sqlstate(rig: &Rig, oid: u64, id: i32) -> &'static str {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    let r = rig.db.get(&st, &h, &[i32_v(id)]);
    ok(rig.core().abort(t));
    match r {
        Ok(_) => panic!("expected corruption"),
        Err(e) => e.sqlstate(),
    }
}
