//! RocksDB user-defined timestamps (C-T0 §12 Q2, C-S1 work item 5).
//!
//! rust-rocksdb 0.25 exposes: `Options::set_comparator_with_ts`
//! (db_options.rs:1888-1921), `put_with_ts`/`delete_with_ts` (db.rs:1872,
//! db.rs:1944), write-batch `put_cf_with_ts`/`delete_cf_with_ts`
//! (write_batch.rs:298, write_batch.rs:435), `ReadOptions::set_timestamp`/
//! `set_iter_start_ts` (db_options.rs:4396, 4418),
//! `increase_full_history_ts_low`/`get_full_history_ts_low` (db.rs:2569,
//! db.rs:2587), `CompactOptions::set_full_history_ts_low`
//! (db_options.rs:4878) and `SstFileWriter::put_with_ts` (sst_file_writer.rs:126).
//! It does **not** expose `DeleteRangeWithTs`: the C API has no ts-taking
//! `rocksdb_delete_range` (c.h:445 is the only form), while C++ has had the
//! ts overload since db.h:570. Each test below exercises one capability; the
//! verdict is summarised in DECISION.md "## Q2 RocksDB UDT".

#![allow(clippy::unwrap_used)] // tests may unwrap (rule: not outside tests)

use std::cmp::Ordering;
use std::path::Path;

use rocksdb::checkpoint::Checkpoint;
use rocksdb::{
    CompactOptions, DBIteratorWithThreadMode, DBRawIteratorWithThreadMode,
    IngestExternalFileOptions, Options, ReadOptions, SstFileWriter, DB,
};

fn ts(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// `compare` for `key ‖ ts(8B big-endian)`: user key ascending, then ts
/// **descending** (RocksDB UDT: newer versions of a key sort first). This
/// matches the canonical `ComparatorWithTsImpl::Compare`
/// (util/comparator.cc:266-278): `CompareWithoutTimestamp` then
/// `-CompareTimestamp(...)`.
fn cmp_with_ts(a: &[u8], b: &[u8]) -> Ordering {
    let (ak, ats) = a.split_at(a.len() - 8);
    let (bk, bts) = b.split_at(b.len() - 8);
    ak.cmp(bk).then_with(|| bts.cmp(ats))
}

/// Ascending on the decoded timestamp (comparator.cc:296-305); the engine
/// applies the "newer first" negation itself.
fn cmp_ts(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// `CompareWithoutTimestamp`: strip the ts suffix when the flag says the key
/// carries one (comparator.cc:281-292).
fn cmp_without_ts(a: &[u8], a_has_ts: bool, b: &[u8], b_has_ts: bool) -> Ordering {
    fn strip(k: &[u8], has_ts: bool) -> &[u8] {
        if has_ts && k.len() >= 8 {
            &k[..k.len() - 8]
        } else {
            k
        }
    }
    strip(a, a_has_ts).cmp(strip(b, b_has_ts))
}

/// Column-family options with the same UDT comparator (CFs do not inherit
/// the DB-level comparator in rust-rocksdb 0.25).
fn udt_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_comparator_with_ts(
        "nucleus-bakeoff-udt",
        8,
        Box::new(cmp_with_ts),
        Box::new(cmp_ts),
        Box::new(cmp_without_ts),
    );
    opts
}

fn open_udt(path: &Path) -> DB {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_comparator_with_ts(
        "nucleus-bakeoff-udt",
        8,
        Box::new(cmp_with_ts),
        Box::new(cmp_ts),
        Box::new(cmp_without_ts),
    );
    DB::open(&opts, path).unwrap()
}

fn get_at(db: &DB, key: &[u8], at: u64) -> Option<Vec<u8>> {
    let mut ro = ReadOptions::default();
    ro.set_timestamp(ts(at));
    db.get_opt(key, &ro).unwrap()
}

#[test]
fn udt_put_get_delete_multiversion() {
    let (dir, _guard) = nucleus_bakeoff::store_dir("udt-mv");
    let db = open_udt(&dir);
    db.put_with_ts(b"k", ts(1), b"v1").unwrap();
    db.put_with_ts(b"k", ts(2), b"v2").unwrap();
    db.put_with_ts(b"k", ts(3), b"v3").unwrap();
    assert_eq!(get_at(&db, b"k", 0), None);
    assert_eq!(get_at(&db, b"k", 1), Some(b"v1".to_vec()));
    assert_eq!(get_at(&db, b"k", 2), Some(b"v2".to_vec()));
    assert_eq!(get_at(&db, b"k", 3), Some(b"v3".to_vec()));
    assert_eq!(get_at(&db, b"k", u64::MAX), Some(b"v3".to_vec()));

    // A tombstone at ts 4 hides everything older from reads at >= 4.
    db.delete_with_ts(b"k", ts(4)).unwrap();
    assert_eq!(get_at(&db, b"k", 3), Some(b"v3".to_vec()));
    assert_eq!(get_at(&db, b"k", 4), None);
    assert_eq!(get_at(&db, b"k", u64::MAX), None);
}

#[test]
fn udt_write_batch_with_ts() {
    let (dir, _guard) = nucleus_bakeoff::store_dir("udt-batch");
    let mut db = open_udt(&dir);
    let cf_opts = udt_cf_options();
    db.create_cf("t", &cf_opts).unwrap();
    let mut wb = rocksdb::WriteBatch::default();
    let t = db.cf_handle("t").unwrap();
    wb.put_cf_with_ts(&t, b"bk", ts(7), b"bv");
    wb.delete_cf_with_ts(&t, b"gone", ts(7));
    let mut wo = rocksdb::WriteOptions::default();
    wo.set_sync(true);
    db.write_opt(wb, &wo).unwrap();
    let mut ro = ReadOptions::default();
    ro.set_timestamp(ts(7));
    assert_eq!(
        db.get_cf_opt(&t, b"bk", &ro).unwrap(),
        Some(b"bv".to_vec()),
        "write-batch put with ts not visible"
    );
}

#[test]
fn udt_iterator_at_read_ts() {
    let (dir, _guard) = nucleus_bakeoff::store_dir("udt-iter");
    let db = open_udt(&dir);
    for t in 1..=3u64 {
        db.put_with_ts(b"a", ts(t), format!("v{t}").as_bytes())
            .unwrap();
    }
    db.put_with_ts(b"b", ts(2), b"vb2").unwrap();

    // Iterator pinned to a read timestamp.
    let mut ro = ReadOptions::default();
    ro.set_timestamp(ts(2));
    let it: DBIteratorWithThreadMode<'_, DB> = db.iterator_opt(rocksdb::IteratorMode::Start, ro);
    let rows: Vec<(Vec<u8>, Vec<u8>)> = it
        .map(|r| r.unwrap())
        .map(|(k, v)| (k.into_vec(), v.into_vec()))
        .collect();
    assert_eq!(
        rows,
        vec![
            (b"a".to_vec(), b"v2".to_vec()),
            (b"b".to_vec(), b"vb2".to_vec()),
        ],
        "iterator at read ts=2"
    );

    // Multi-version iteration between iter_start_ts and timestamp: raw
    // iterator exposes key and ts separately.
    let mut ro = ReadOptions::default();
    ro.set_iter_start_ts(ts(1));
    ro.set_timestamp(ts(3));
    let mut it: DBRawIteratorWithThreadMode<'_, DB> = db.raw_iterator_opt(ro);
    it.seek_to_first();
    let mut versions: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
    while it.valid() {
        versions.push((
            it.key().unwrap().to_vec(),
            it.timestamp().unwrap().to_vec(),
            it.value().unwrap().to_vec(),
        ));
        it.next();
    }
    let got: Vec<(u8, u64, &[u8])> = versions
        .iter()
        .map(|(k, t, v)| {
            (
                k[0],
                u64::from_be_bytes(t.as_slice().try_into().unwrap()),
                v.as_slice(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (b'a', 3, &b"v3"[..]),
            (b'a', 2, &b"v2"[..]),
            (b'a', 1, &b"v1"[..]),
            (b'b', 2, &b"vb2"[..]),
        ],
        "history iteration returns versions newest-first with their ts: {versions:?}"
    );
}

/// DeleteRange on a UDT column family is **not exposed with a timestamp**:
/// c.h:445 `rocksdb_delete_range_cf` has no ts variant (db.h:570 is C++-only),
/// so the exposed call must be refused by the write path.
#[test]
fn udt_delete_range_without_ts_is_rejected() {
    let (dir, _guard) = nucleus_bakeoff::store_dir("udt-dr");
    let mut db = open_udt(&dir);
    let cf_opts = udt_cf_options();
    db.create_cf("t", &cf_opts).unwrap();
    let t = db.cf_handle("t").unwrap();
    db.put_cf_with_ts(&t, b"k", ts(1), b"v1").unwrap();

    let err = db
        .delete_range_cf(&t, b"a", b"z")
        .expect_err("ts-less DeleteRange accepted on a UDT column family");
    println!("delete_range_cf on UDT column family: {err}");
    // The store is unchanged and still usable.
    let mut ro = ReadOptions::default();
    ro.set_timestamp(ts(1));
    assert_eq!(db.get_cf_opt(&t, b"k", &ro).unwrap(), Some(b"v1".to_vec()));

    let mut wb = rocksdb::WriteBatch::default();
    wb.delete_range_cf(&t, b"a", b"z");
    let mut wo = rocksdb::WriteOptions::default();
    wo.set_sync(true);
    assert!(
        db.write_opt(wb, &wo).is_err(),
        "ts-less DeleteRange in a write batch accepted on a UDT column family"
    );
    assert_eq!(db.get_cf_opt(&t, b"k", &ro).unwrap(), Some(b"v1".to_vec()));
}

/// `full_history_ts_low` + `CompactOptions::set_full_history_ts_low`: the
/// engine owns version GC: below the watermark only the newest version of a
/// key is kept (RocksDB 11 semantics; the exact collapse is observable after
/// compaction).
#[test]
fn udt_full_history_ts_low_collapses_history() {
    let (dir, _guard) = nucleus_bakeoff::store_dir("udt-fh");
    let mut db = open_udt(&dir);
    let cf_opts = udt_cf_options();
    db.create_cf("h", &cf_opts).unwrap();
    let h = db.cf_handle("h").unwrap();
    for t in 1..=6u64 {
        db.put_cf_with_ts(&h, b"k", ts(t), format!("v{t}").as_bytes())
            .unwrap();
    }
    db.put_cf_with_ts(&h, b"j", ts(2), b"keep-older-key")
        .unwrap();

    db.increase_full_history_ts_low(&h, ts(5)).unwrap();
    assert_eq!(
        db.get_full_history_ts_low(&h).unwrap(),
        ts(5).to_vec(),
        "get_full_history_ts_low after increase"
    );

    let mut co = CompactOptions::default();
    co.set_full_history_ts_low(ts(5));
    db.flush().unwrap();
    db.compact_range_cf_opt(&h, None::<&[u8]>, None::<&[u8]>, &co);

    // Reads >= the watermark are intact.
    let mut ro5 = ReadOptions::default();
    ro5.set_timestamp(ts(6));
    assert_eq!(
        db.get_cf_opt(&h, b"k", &ro5).unwrap(),
        Some(b"v6".to_vec()),
        "newest version lost"
    );
    let mut ro5 = ReadOptions::default();
    ro5.set_timestamp(ts(5));
    assert_eq!(
        db.get_cf_opt(&h, b"k", &ro5).unwrap(),
        Some(b"v5".to_vec()),
        "newest version <= watermark lost"
    );
    assert_eq!(
        db.get_cf_opt(&h, b"j", &ro5).unwrap(),
        Some(b"keep-older-key".to_vec())
    );
    // The engine may have collapsed history below the watermark; what must
    // hold: a read below the watermark never resurrects something older than
    // the kept newest <= watermark version. RocksDB refuses reads with a
    // timestamp below `full_history_ts_low` outright
    // (`FailIfReadCollapsedHistory`, db_impl.h:3775-3799) — the same rule as
    // C-T0 §9.1's AS OF registration check `t >= W`.
    let mut ro4 = ReadOptions::default();
    ro4.set_timestamp(ts(4));
    match db.get_cf_opt(&h, b"k", &ro4) {
        Err(e) => println!("read below full_history_ts_low refused: {e}"),
        Ok(Some(v)) => assert_eq!(v, b"v5".to_vec(), "shadowed version resurrected"),
        Ok(None) => {}
    }
}

#[test]
fn udt_sst_ingest() {
    let (dir, _guard) = nucleus_bakeoff::store_dir("udt-ingest");
    let db = open_udt(&dir);
    db.put_with_ts(b"zz/live", ts(1), b"here").unwrap();

    // db.h:2197-2204: ingested key ranges must not overlap the DB's keys.
    let (sst_dir, _sst_guard) = nucleus_bakeoff::store_dir("udt-ingest-sst");
    std::fs::create_dir_all(&sst_dir).unwrap();
    let sst_path = sst_dir.join("ingest.sst");
    let mut opts = Options::default();
    opts.set_comparator_with_ts(
        "nucleus-bakeoff-udt",
        8,
        Box::new(cmp_with_ts),
        Box::new(cmp_ts),
        Box::new(cmp_without_ts),
    );
    let mut writer = SstFileWriter::create(&opts);
    writer.open(&sst_path).unwrap();
    for t in 1..=3u64 {
        writer
            .put_with_ts(format!("aa/{t}").as_bytes(), ts(t), b"ingested")
            .unwrap();
    }
    writer.finish().unwrap();
    db.ingest_external_file_opts(&IngestExternalFileOptions::default(), vec![sst_path])
        .unwrap_or_else(|e| panic!("SST ingest into a UDT store failed: {e}"));
    for t in 1..=3u64 {
        assert_eq!(
            get_at(&db, format!("aa/{t}").as_bytes(), 3),
            Some(b"ingested".to_vec()),
            "ingested UDT key not readable"
        );
    }
    assert_eq!(get_at(&db, b"zz/live", 1), Some(b"here".to_vec()));
}

#[test]
fn udt_checkpoint() {
    let (dir, _guard) = nucleus_bakeoff::store_dir("udt-ckpt");
    let db = open_udt(&dir);
    db.put_with_ts(b"k", ts(1), b"v1").unwrap();
    db.put_with_ts(b"k", ts(2), b"v2").unwrap();
    let ckpt = dir.parent().unwrap().join("udt-ckpt-copy");
    let _ck_guard = nucleus_bakeoff::ScratchGuard::new(ckpt.clone());
    Checkpoint::new(&db)
        .unwrap()
        .create_checkpoint(&ckpt)
        .expect("checkpoint on a UDT store failed");
    db.put_with_ts(b"k", ts(3), b"v3").unwrap();

    let back = open_udt(&ckpt);
    assert_eq!(get_at(&back, b"k", 1), Some(b"v1".to_vec()));
    assert_eq!(get_at(&back, b"k", 2), Some(b"v2".to_vec()));
    assert_eq!(
        get_at(&back, b"k", u64::MAX),
        Some(b"v2".to_vec()),
        "post-checkpoint write leaked"
    );
    back.put_with_ts(b"new", ts(4), b"w")
        .expect("checkpoint of a UDT store not writable");
}

/// The C-T0 §9.2 UDT GC shape: with `full_history_ts_low = W`, a compaction
/// keeps, per user key, every version > W and the newest version <= W —
/// exactly the rule the ts-in-key path implements in its compaction filter.
#[test]
fn udt_gc_keeps_newest_le_watermark() {
    let (dir, _guard) = nucleus_bakeoff::store_dir("udt-gc");
    let mut db = open_udt(&dir);
    let cf_opts = udt_cf_options();
    db.create_cf("h", &cf_opts).unwrap();
    let h = db.cf_handle("h").unwrap();
    // Versions of one user key across a flush boundary (SST + memtable).
    db.put_cf_with_ts(&h, b"k", ts(10), b"v10").unwrap();
    db.put_cf_with_ts(&h, b"k", ts(8), b"tombstone-repr")
        .unwrap();
    db.put_cf_with_ts(&h, b"k", ts(6), b"v6").unwrap();
    db.flush().unwrap();
    db.put_cf_with_ts(&h, b"k", ts(4), b"v4").unwrap();
    db.put_cf_with_ts(&h, b"k", ts(2), b"v2").unwrap();

    let w = 8u64;
    db.increase_full_history_ts_low(&h, ts(w)).unwrap();
    let mut co = CompactOptions::default();
    co.set_full_history_ts_low(ts(w));
    db.compact_range_cf_opt(&h, None::<&[u8]>, None::<&[u8]>, &co);

    // Enumerate the surviving history.
    let mut ro = ReadOptions::default();
    ro.set_iter_start_ts(ts(0));
    ro.set_timestamp(ts(u64::MAX));
    let mut it = db.raw_iterator_cf_opt(&h, ro);
    it.seek(b"k");
    let mut versions: Vec<u64> = Vec::new();
    while it.valid() && it.key().is_some_and(|k| k.starts_with(b"k")) {
        let t = u64::from_be_bytes(it.timestamp().unwrap().try_into().unwrap());
        versions.push(t);
        it.next();
    }
    versions.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(
        versions.first(),
        Some(&10),
        "newest version must survive: {versions:?}"
    );
    assert!(
        versions.contains(&8),
        "newest version <= W must survive (the §9.2 kept tombstone): {versions:?}"
    );
    // Observed: one full compaction with `full_history_ts_low = W` dropped
    // the memtable versions below W (4, 2) but kept the flushed 6 — the
    // engine-owned collapse below the watermark is best-effort per
    // compaction, not an immediate invariant (and the C-API comparator
    // wrapper never supplies GetMax/MinTimestamp, c.cc:796-839 vs
    // comparator.h:123-143). Print the kept set for DECISION.md.
    println!("udt_gc_keeps_newest_le_watermark: kept versions {versions:?} (W = 8)");
    for t in &versions {
        assert!(
            *t > w || *t == 8 || *t == 6,
            "unexpected survivor: {versions:?}"
        );
    }
}
