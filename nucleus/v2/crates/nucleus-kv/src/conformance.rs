//! Shared conformance suite for `OrderedKv` backends (C-K3).
//!
//! A backend implements `Harness` and expands `kv_conformance_tests!` twice:
//! bare and wrapped in `FaultHarness`. Each case panics on a violation.
//!
//! Decisions recorded here, against C-T0 §9:
//! - **GC versus earlier snapshots.** A snapshot taken before a compaction must
//!   keep returning every key the filter kept, unchanged, and must never see a
//!   later write. Whether it still returns a key the filter dropped is
//!   unspecified: RocksDB (since 6.0) runs compaction filters on keys visible
//!   to live snapshots, flat-mode MemKv keeps them by structural sharing, and
//!   LSM-mode MemKv takes the adversarial choice (a drop is visible to
//!   already-open snapshots, C-T0 §11 G0-gc/G0-R5-1). I-GC does not depend on
//!   either: the filter only drops a version older than a kept version
//!   `<= W`, and every open reader has `S >= W`, so §4 stops at the
//!   kept version first.
//! - **The §9.2 drop rule (draft 4).** The kv honours the filter blindly.
//!   Within one compaction stream (keys in ascending order) a filter may drop
//!   any version for which it has already seen a newer version `<= W` of the
//!   same logical key in the same stream, and must never drop the newest
//!   version `<= W` it has seen for a key. `layout` implements the §2.2 key
//!   and version-value encoding, `SpecGcFilter` is the reference filter over
//!   it, and `SpecGcGuard` makes a rule-violating filter detectable. Versions
//!   of one logical key can span SST files (a stream may not see them all);
//!   MemKv's LSM mode reproduces the tombstone resurrection a violating
//!   filter causes (`gc_tombstone_resurrection_lsm`, C-K3b).
//! - **Crash.** WAL-prefix semantics are checked through `fault::Fault` over
//!   the backend, so they hold for the wrapper's model of the backend. A
//!   backend's own on-disk crash behaviour is C-G1's power-cut harness.

use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::fault::Fault;
use crate::{
    Batch, Durability, GcFilter, GcStream, Key, KvError, Op, OrderedKv, Result, Snapshot, Value,
};

pub mod layout;

/// How the suite drives one backend.
pub trait Harness: Clone + Send + Sync + 'static {
    type Kv: OrderedKv;
    /// A fresh, empty store.
    fn make(&self) -> Self::Kv;
    /// A full, synchronous compaction: every live key passes through the
    /// registered GC filter exactly once.
    fn compact(&self, kv: &Self::Kv) -> Result<()>;
    /// Opens a checkpoint written by `OrderedKv::checkpoint`.
    fn open_checkpoint(&self, dir: &Path) -> Result<Self::Kv>;
    /// Pushes written data to lower storage levels without running GC, so
    /// later writes and DeleteRange land "above" it. Default: nothing.
    fn settle(&self, _kv: &Self::Kv) -> Result<()> {
        Ok(())
    }
}

/// Runs a backend's suite through `Fault`.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultHarness<H>(pub H);

impl<H: Harness> Harness for FaultHarness<H> {
    type Kv = Fault<H::Kv>;

    fn make(&self) -> Self::Kv {
        fault_over(&self.0)
    }

    fn compact(&self, kv: &Self::Kv) -> Result<()> {
        kv.with_live(|k| self.0.compact(k))
    }

    fn open_checkpoint(&self, dir: &Path) -> Result<Self::Kv> {
        let initial = self.0.open_checkpoint(dir)?;
        let h = self.0.clone();
        let dir = dir.to_path_buf();
        Ok(Fault::new(initial, move || h.open_checkpoint(&dir)))
    }

    fn settle(&self, kv: &Self::Kv) -> Result<()> {
        kv.with_live(|k| self.0.settle(k))
    }
}

fn fault_over<H: Harness>(h: &H) -> Fault<H::Kv> {
    let base = h.clone();
    Fault::new(h.make(), move || Ok(base.make()))
}

/// Expands to `mod $name { #[test] fn <case>() ... }` with one test per case,
/// each on a fresh harness value from `$harness`.
#[macro_export]
macro_rules! kv_conformance_tests {
    (@cases $name:ident, $harness:expr; $($case:ident),* $(,)?) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            $(
                #[test]
                fn $case() {
                    let h = $harness;
                    $crate::conformance::$case(&h);
                }
            )*
        }
    };
    ($name:ident, $harness:expr) => {
        $crate::kv_conformance_tests!(@cases $name, $harness;
            batch_ops_apply_in_order,
            batch_is_atomic_to_concurrent_readers,
            concurrent_writers,
            delete_range_bounds,
            delete_range_before_and_after,
            snapshot_ignores_later_writes,
            scan_all_bounds,
            ingest_sorted_visible_and_isolated,
            ingest_sorted_rejects_unsorted_atomically,
            checkpoint_round_trip,
            crash_keeps_durable_and_drops_suffix,
            crash_seeded_is_prefix,
            sync_covers_earlier_writes,
            gc_streams_ascending_cover_every_key,
            gc_drops_and_keeps,
            gc_snapshot_before_compact,
            gc_spec_violation_detectable,
            gc_watermark_monotonic,
            gc_without_filter_keeps_all,
        );
    };
}

// ---- helpers ----

fn ok<T>(r: Result<T>, what: &str) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("{what}: {e}"),
    }
}

fn k(s: &str) -> Key {
    s.as_bytes().to_vec()
}

fn write<K: OrderedKv>(kv: &K, ops: Vec<Op>, sync: Durability) {
    ok(kv.write(Batch { ops }, sync), "write");
}

fn put<K: OrderedKv>(kv: &K, key: &str, val: &str) {
    write(kv, vec![Op::Put(k(key), k(val))], Durability::No);
}

fn get<K: OrderedKv>(kv: &K, key: &str) -> Option<Value> {
    ok(kv.get_latest(key.as_bytes()), "get_latest")
}

fn scan<S: Snapshot>(
    s: &S,
    lo: Bound<&[u8]>,
    hi: Bound<&[u8]>,
    reverse: bool,
) -> Vec<(Key, Value)> {
    s.scan((lo, hi), reverse).map(|r| ok(r, "scan")).collect()
}

fn dump<S: Snapshot>(s: &S) -> Vec<(Key, Value)> {
    scan(s, Bound::Unbounded, Bound::Unbounded, false)
}

type Model = BTreeMap<Key, Value>;

/// Asserts latest reads, a fresh snapshot (get, forward and reverse scan) all equal `model`.
fn assert_state<K: OrderedKv>(kv: &K, model: &Model, ctx: &str) {
    let snap = kv.snapshot();
    let want: Vec<(Key, Value)> = model.iter().map(|(a, b)| (a.clone(), b.clone())).collect();
    assert_eq!(dump(&snap), want, "{ctx}: forward scan");
    let mut rev = want.clone();
    rev.reverse();
    assert_eq!(
        scan(&snap, Bound::Unbounded, Bound::Unbounded, true),
        rev,
        "{ctx}: reverse scan"
    );
    for (key, v) in model {
        assert_eq!(
            ok(snap.get(key), "get").as_ref(),
            Some(v),
            "{ctx}: snapshot get {key:?}"
        );
        assert_eq!(
            ok(kv.get_latest(key), "get_latest").as_ref(),
            Some(v),
            "{ctx}: get_latest {key:?}"
        );
    }
}

fn state<K: OrderedKv>(kv: &K) -> Model {
    dump(&kv.snapshot()).into_iter().collect()
}

/// Unique, not-yet-existing path under the system temp dir.
pub fn scratch_dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "nucleus-kv-{tag}-{}-{nanos}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ))
}

// ---- batches ----

/// Ops in one batch apply in order; deleting an absent key and an empty batch are fine.
pub fn batch_ops_apply_in_order<H: Harness>(h: &H) {
    let kv = h.make();
    write(
        &kv,
        vec![
            Op::Put(k("a"), k("1")),
            Op::Put(k("b"), k("1")),
            Op::Put(k("c"), k("1")),
        ],
        Durability::No,
    );
    write(
        &kv,
        vec![
            Op::Put(k("a"), k("2")),
            Op::Delete(k("a")),
            Op::Put(k("b"), k("2")),
            Op::DeleteRange {
                start: k("b"),
                end: k("d"),
            },
            Op::Put(k("c"), k("3")),
            Op::Delete(k("zz")),
            Op::Put(k("e"), k("1")),
        ],
        Durability::Yes,
    );
    write(&kv, Vec::new(), Durability::No);
    let model: Model = [(k("c"), k("3")), (k("e"), k("1"))].into_iter().collect();
    assert_state(&kv, &model, "in-batch order");
    assert_eq!(get(&kv, "a"), None);
    assert_eq!(get(&kv, "b"), None);
}

/// No snapshot ever observes part of a batch, including one with a DeleteRange.
pub fn batch_is_atomic_to_concurrent_readers<H: Harness>(h: &H) {
    const N: usize = 16;
    const ROUNDS: u32 = 300;
    let kv = h.make();
    let keys: Vec<Key> = (0..N).map(|i| k(&format!("atom/{i:02}"))).collect();
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            for round in 1..=ROUNDS {
                let mut ops = Vec::new();
                if round % 7 == 0 {
                    ops.push(Op::DeleteRange {
                        start: k("atom/"),
                        end: k("atom0"),
                    });
                }
                for key in &keys {
                    ops.push(Op::Put(key.clone(), round.to_be_bytes().to_vec()));
                }
                write(&kv, ops, Durability::No);
            }
            done.store(true, Ordering::SeqCst);
        });
        let mut last = 0u32;
        loop {
            let finished = done.load(Ordering::SeqCst);
            let snap = kv.snapshot();
            let rows = scan(
                &snap,
                Bound::Included(b"atom/".as_slice()),
                Bound::Excluded(b"atom0".as_slice()),
                false,
            );
            if rows.is_empty() {
                assert_eq!(last, 0, "rows vanished after round {last}");
            } else {
                assert_eq!(rows.len(), N, "partial batch visible");
                let first = rows[0].1.clone();
                assert!(
                    rows.iter().all(|(_, v)| *v == first),
                    "mixed rounds in one snapshot"
                );
                let mut b = [0u8; 4];
                b.copy_from_slice(&first);
                let round = u32::from_be_bytes(b);
                assert!(round >= last, "snapshot went backwards: {round} < {last}");
                last = round;
            }
            if finished {
                break;
            }
        }
        assert_eq!(last, ROUNDS);
    });
}

/// `write` from many threads: every write lands.
pub fn concurrent_writers<H: Harness>(h: &H) {
    let kv = h.make();
    std::thread::scope(|s| {
        for t in 0..4 {
            let kv = &kv;
            s.spawn(move || {
                for i in 0..50 {
                    put(kv, &format!("w/{t}/{i:03}"), "x");
                    if i % 10 == 0 {
                        ok(kv.sync_wal(), "sync_wal");
                    }
                }
            });
        }
    });
    assert_eq!(state(&kv).len(), 200);
}

// ---- DeleteRange ----

/// `[start, end)`: start included, end excluded, byte-wise order, empty range no-op.
pub fn delete_range_bounds<H: Harness>(h: &H) {
    let kv = h.make();
    let keys: [&[u8]; 10] = [
        b"a",
        b"b",
        b"b\x00",
        b"ba",
        b"bz",
        b"c",
        b"c\x00",
        b"d",
        b"\xff",
        b"\xff\xff",
    ];
    let mut model = Model::new();
    for key in keys {
        write(&kv, vec![Op::Put(key.to_vec(), k("v"))], Durability::No);
        model.insert(key.to_vec(), k("v"));
    }
    let cases: [(&[u8], &[u8]); 4] = [
        (b"b", b"c"),
        (b"c", b"c"),
        (b"d", b"a"),
        (b"\xff", b"\xff\xff"),
    ];
    for (start, end) in cases {
        write(
            &kv,
            vec![Op::DeleteRange {
                start: start.to_vec(),
                end: end.to_vec(),
            }],
            Durability::No,
        );
        model.retain(|key, _| !(key.as_slice() >= start && key.as_slice() < end));
        assert_state(&kv, &model, &format!("delete_range {start:?}..{end:?}"));
    }
    let left: Vec<&[u8]> = model.keys().map(Vec::as_slice).collect();
    assert_eq!(
        left,
        vec![b"a".as_slice(), b"c", b"c\x00", b"d", b"\xff\xff"]
    );
}

/// DeleteRange removes keys written before it (also after `settle`, every
/// older version under the range) and leaves later writes alone.
pub fn delete_range_before_and_after<H: Harness>(h: &H) {
    let kv = h.make();
    for i in 1..=9 {
        put(&kv, &format!("r/{i}"), "old");
    }
    ok(h.settle(&kv), "settle");
    put(&kv, "r/5", "newer");
    write(
        &kv,
        vec![Op::DeleteRange {
            start: k("r/3"),
            end: k("r/7"),
        }],
        Durability::No,
    );
    let mut model: Model = [1, 2, 7, 8, 9]
        .iter()
        .map(|i| (k(&format!("r/{i}")), k("old")))
        .collect();
    assert_state(&kv, &model, "after range delete");
    ok(h.settle(&kv), "settle");
    assert_state(&kv, &model, "range delete after settle");
    put(&kv, "r/4", "after");
    model.insert(k("r/4"), k("after"));
    assert_state(&kv, &model, "write after range delete");
    ok(h.settle(&kv), "settle");
    assert_state(&kv, &model, "write after range delete, settled");
}

// ---- snapshots ----

/// A snapshot never sees a later put, overwrite, delete, DeleteRange or
/// ingest, and outlives its store.
pub fn snapshot_ignores_later_writes<H: Harness>(h: &H) {
    let kv = h.make();
    for c in ["a", "b", "c", "d", "e"] {
        put(&kv, &format!("s/{c}"), c);
    }
    ok(h.settle(&kv), "settle");
    let snap = kv.snapshot();
    let before = dump(&snap);
    put(&kv, "s/a", "changed");
    put(&kv, "s/f", "new");
    write(&kv, vec![Op::Delete(k("s/b"))], Durability::Yes);
    write(
        &kv,
        vec![Op::DeleteRange {
            start: k("s/c"),
            end: k("s/e"),
        }],
        Durability::No,
    );
    ok(
        kv.ingest_sorted(&mut vec![(k("t/1"), k("i")), (k("t/2"), k("i"))].into_iter()),
        "ingest",
    );
    ok(h.settle(&kv), "settle");
    let model: Model = [
        (k("s/a"), k("changed")),
        (k("s/e"), k("e")),
        (k("s/f"), k("new")),
        (k("t/1"), k("i")),
        (k("t/2"), k("i")),
    ]
    .into_iter()
    .collect();
    assert_state(&kv, &model, "latest");
    drop(kv);
    assert_eq!(dump(&snap), before, "old snapshot changed");
    let mut rev = before.clone();
    rev.reverse();
    assert_eq!(scan(&snap, Bound::Unbounded, Bound::Unbounded, true), rev);
    for (key, v) in &before {
        assert_eq!(ok(snap.get(key), "get").as_ref(), Some(v));
    }
    assert_eq!(ok(snap.get(b"s/f"), "get"), None);
    assert_eq!(ok(snap.get(b"t/1"), "get"), None);
}

/// Forward and reverse scans with every bound kind, against a model, including
/// inverted and empty ranges.
pub fn scan_all_bounds<H: Harness>(h: &H) {
    let kv = h.make();
    let keys: [&[u8]; 8] = [
        b"j", b"k/a", b"k/b", b"k/b\x00", b"k/c", b"k/d\xff", b"k/e", b"l",
    ];
    let mut model = Model::new();
    for (i, key) in keys.iter().enumerate() {
        write(
            &kv,
            vec![Op::Put(key.to_vec(), vec![i as u8 + 1])],
            Durability::No,
        );
        model.insert(key.to_vec(), vec![i as u8 + 1]);
    }
    let points: [&[u8]; 11] = [
        b"a", b"j", b"k/", b"k/a", b"k/aa", b"k/b", b"k/b\x00", b"k/c", b"k/e", b"k/z", b"z",
    ];
    let mut bounds: Vec<Bound<&[u8]>> = vec![Bound::Unbounded];
    for p in points {
        bounds.push(Bound::Included(p));
        bounds.push(Bound::Excluded(p));
    }
    let snap = kv.snapshot();
    for &lo in &bounds {
        for &hi in &bounds {
            let want: Vec<(Key, Value)> = model
                .iter()
                .filter(|(key, _)| (lo, hi).contains(key.as_slice()))
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect();
            assert_eq!(scan(&snap, lo, hi, false), want, "forward {lo:?}..{hi:?}");
            let mut rev = want;
            rev.reverse();
            assert_eq!(scan(&snap, lo, hi, true), rev, "reverse {lo:?}..{hi:?}");
        }
    }
}

// ---- ingest ----

/// Ingested data is visible all at once and invisible to earlier snapshots.
pub fn ingest_sorted_visible_and_isolated<H: Harness>(h: &H) {
    let kv = h.make();
    put(&kv, "x/1", "live");
    let before = kv.snapshot();
    let entries: Vec<(Key, Value)> = (0..100)
        .map(|i| (k(&format!("i/{i:03}")), k("v")))
        .collect();
    ok(kv.ingest_sorted(&mut entries.clone().into_iter()), "ingest");
    let mut model: Model = entries.into_iter().collect();
    model.insert(k("x/1"), k("live"));
    assert_state(&kv, &model, "after ingest");
    assert_eq!(dump(&before), vec![(k("x/1"), k("live"))]);
}

/// Unsorted or duplicate input fails and ingests nothing, not even its sorted prefix.
pub fn ingest_sorted_rejects_unsorted_atomically<H: Harness>(h: &H) {
    let kv = h.make();
    put(&kv, "x/1", "live");
    let bad: [Vec<(Key, Value)>; 2] = [
        vec![
            (k("u/1"), k("v")),
            (k("u/2"), k("v")),
            (k("u/5"), k("v")),
            (k("u/3"), k("v")),
        ],
        vec![(k("u/1"), k("v")), (k("u/1"), k("w"))],
    ];
    for entries in bad {
        assert!(
            kv.ingest_sorted(&mut entries.into_iter()).is_err(),
            "bad input accepted"
        );
        assert_eq!(
            state(&kv),
            [(k("x/1"), k("live"))].into_iter().collect::<Model>()
        );
    }
    ok(
        kv.ingest_sorted(&mut vec![(k("u/9"), k("v"))].into_iter()),
        "ingest",
    );
    assert_eq!(get(&kv, "u/9"), Some(k("v")));
}

// ---- checkpoint ----

/// A checkpoint reopens to exactly the state at checkpoint time, independent
/// of later writes to either store.
pub fn checkpoint_round_trip<H: Harness>(h: &H) {
    let kv = h.make();
    let keys: [&[u8]; 5] = [b"\x00", b"\x00\x00", b"c/1", b"\xff", b"\xff\xff"];
    for key in keys {
        write(
            &kv,
            vec![Op::Put(key.to_vec(), key.to_vec())],
            Durability::No,
        );
    }
    for i in 0..20 {
        put(&kv, &format!("c/r/{i:02}"), "row");
    }
    put(&kv, "c/big", &"x".repeat(100_000));
    write(&kv, vec![Op::Delete(k("c/1"))], Durability::No);
    write(
        &kv,
        vec![Op::DeleteRange {
            start: k("c/r/05"),
            end: k("c/r/10"),
        }],
        Durability::No,
    );
    let expect = state(&kv);
    let dir = scratch_dir("ckpt");
    ok(kv.checkpoint(&dir), "checkpoint");
    put(&kv, "c/after", "x");
    write(&kv, vec![Op::Delete(k("c/r/00"))], Durability::No);
    let back = ok(h.open_checkpoint(&dir), "open_checkpoint");
    assert_state(&back, &expect, "reopened checkpoint");
    put(&back, "c/new", "y");
    assert_eq!(get(&back, "c/new"), Some(k("y")));
    assert_eq!(get(&kv, "c/new"), None);
    assert_eq!(get(&kv, "c/after"), Some(k("x")));
    drop(back);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- crash (through Fault) ----

/// Durable writes survive; of the unsynced batches exactly the first `keep`
/// survive, each whole, including a DeleteRange inside one.
pub fn crash_keeps_durable_and_drops_suffix<H: Harness>(h: &H) {
    for keep in 0..=6usize {
        let f = fault_over(h);
        write(
            &f,
            vec![Op::Put(k("d/1"), k("v")), Op::Put(k("d/2"), k("v"))],
            Durability::Yes,
        );
        for i in 1..=5u8 {
            let mut ops = vec![
                Op::Put(k(&format!("b{i}/x")), vec![i]),
                Op::Put(k(&format!("b{i}/y")), vec![i]),
                Op::Put(k("ctr"), vec![i]),
            ];
            if i == 3 {
                ops.push(Op::DeleteRange {
                    start: k("d/1"),
                    end: k("d/2"),
                });
            }
            write(&f, ops, Durability::No);
        }
        assert_eq!(f.unsynced_len(), 5);
        let kept = ok(f.crash(keep), "crash");
        assert_eq!(kept, keep.min(5));
        assert_eq!(f.unsynced_len(), 0);
        assert_eq!(get(&f, "d/2"), Some(k("v")), "durable write lost");
        assert_eq!(
            get(&f, "d/1").is_some(),
            kept < 3,
            "range delete in batch 3, kept {kept}"
        );
        for i in 1..=5u8 {
            let x = get(&f, &format!("b{i}/x"));
            let y = get(&f, &format!("b{i}/y"));
            assert_eq!(x, y, "batch {i} torn");
            assert_eq!(
                x.is_some(),
                usize::from(i) <= kept,
                "batch {i}, kept {kept}"
            );
        }
        let ctr = get(&f, "ctr");
        assert_eq!(ctr, (kept > 0).then(|| vec![kept as u8]));
        put(&f, "post", "crash");
        assert_eq!(
            get(&f, "post"),
            Some(k("crash")),
            "store unusable after crash"
        );
    }
}

/// Seeded crashes keep a prefix of the unsynced batches, deterministically per seed.
pub fn crash_seeded_is_prefix<H: Harness>(h: &H) {
    let run = |seed: u64| -> usize {
        let f = fault_over(h);
        put(&f, "base", "v");
        ok(f.sync_wal(), "sync_wal");
        for i in 1..=8u8 {
            write(
                &f,
                vec![
                    Op::Put(vec![b'p', i, 0], vec![i]),
                    Op::Put(vec![b'p', i, 1], vec![i]),
                ],
                Durability::No,
            );
        }
        let kept = ok(f.crash_seeded(seed), "crash_seeded");
        assert_eq!(get(&f, "base"), Some(k("v")));
        let present: Vec<bool> = (1..=8u8)
            .map(|i| {
                let a = ok(f.get_latest(&[b'p', i, 0]), "get").is_some();
                let b = ok(f.get_latest(&[b'p', i, 1]), "get").is_some();
                assert_eq!(a, b, "seed {seed}: batch {i} torn");
                a
            })
            .collect();
        let n = present.iter().take_while(|p| **p).count();
        assert!(
            present[n..].iter().all(|p| !p),
            "seed {seed}: gap in kept prefix {present:?}"
        );
        assert_eq!(n, kept, "seed {seed}");
        kept
    };
    for seed in 0..32 {
        assert_eq!(run(seed), run(seed), "seed {seed} not deterministic");
    }
}

/// `sync_wal`, a `Durability::Yes` write and `ingest_sorted` each make every
/// earlier write durable; repeated crashes keep what was durable.
pub fn sync_covers_earlier_writes<H: Harness>(h: &H) {
    let f = fault_over(h);
    put(&f, "a", "1");
    put(&f, "b", "1");
    ok(f.sync_wal(), "sync_wal");
    put(&f, "c", "1");
    write(&f, vec![Op::Put(k("d"), k("1"))], Durability::Yes);
    put(&f, "e", "1");
    assert_eq!(ok(f.crash(0), "crash"), 0);
    let mut model: Model = ["a", "b", "c", "d"]
        .iter()
        .map(|s| (k(s), k("1")))
        .collect();
    assert_state(&f, &model, "after first crash");
    put(&f, "f", "1");
    assert_eq!(ok(f.crash(0), "crash"), 0);
    assert_state(&f, &model, "after second crash");
    put(&f, "g", "1");
    ok(
        f.ingest_sorted(&mut vec![(k("h"), k("1"))].into_iter()),
        "ingest",
    );
    ok(f.crash(0), "crash");
    model.insert(k("g"), k("1"));
    model.insert(k("h"), k("1"));
    assert_state(&f, &model, "after ingest barrier");
}

// ---- GC filter ----

type Streams = Arc<Mutex<Vec<Vec<(Key, Value)>>>>;

/// Records every stream's keys; drops values starting with `drop_prefix`.
struct Recorder {
    streams: Streams,
    drop_prefix: Option<Vec<u8>>,
}

struct RecorderStream {
    streams: Streams,
    idx: usize,
    drop_prefix: Option<Vec<u8>>,
}

impl GcFilter for Recorder {
    fn begin(&self) -> Box<dyn GcStream> {
        let mut s = self.streams.lock().unwrap_or_else(PoisonError::into_inner);
        s.push(Vec::new());
        Box::new(RecorderStream {
            streams: Arc::clone(&self.streams),
            idx: s.len() - 1,
            drop_prefix: self.drop_prefix.clone(),
        })
    }
}

impl GcStream for RecorderStream {
    fn drop_key(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.streams.lock().unwrap_or_else(PoisonError::into_inner)[self.idx]
            .push((key.to_vec(), value.to_vec()));
        self.drop_prefix
            .as_ref()
            .is_some_and(|p| value.starts_with(p))
    }
}

fn recorder(drop_prefix: Option<&str>) -> (Box<dyn GcFilter>, Streams) {
    let streams = Streams::default();
    let f = Recorder {
        streams: Arc::clone(&streams),
        drop_prefix: drop_prefix.map(k),
    };
    (Box::new(f), streams)
}

/// Writes `gk/00..20`: even keys hold `garbage`, odd keys `live`.
fn gc_fixture<K: OrderedKv>(kv: &K) -> (Model, Model) {
    let (mut dropped, mut kept) = (Model::new(), Model::new());
    for i in 0..20 {
        let key = k(&format!("gk/{i:02}"));
        let v = if i % 2 == 0 {
            k(&format!("garbage{i}"))
        } else {
            k(&format!("live{i}"))
        };
        write(kv, vec![Op::Put(key.clone(), v.clone())], Durability::No);
        if i % 2 == 0 {
            dropped.insert(key, v);
        } else {
            kept.insert(key, v);
        }
    }
    (dropped, kept)
}

/// The filter sees keys in ascending order within each stream (`begin` once
/// per stream), and every live key exactly once across streams.
pub fn gc_streams_ascending_cover_every_key<H: Harness>(h: &H) {
    let kv = h.make();
    for i in [7, 3, 9, 1, 0, 8, 2, 6, 4, 5] {
        put(&kv, &format!("g/{i:02}"), "v1");
    }
    ok(h.settle(&kv), "settle");
    for i in [3, 6, 11, 12] {
        put(&kv, &format!("g/{i:02}"), "v2");
    }
    write(
        &kv,
        vec![
            Op::Delete(k("g/04")),
            Op::DeleteRange {
                start: k("g/08"),
                end: k("g/10"),
            },
        ],
        Durability::No,
    );
    let want = state(&kv);
    let (filter, streams) = recorder(None);
    kv.set_gc_filter(filter);
    ok(h.compact(&kv), "compact");
    let streams = streams
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert!(!streams.is_empty(), "compaction never called begin()");
    for (n, s) in streams.iter().enumerate() {
        assert!(
            s.windows(2).all(|w| w[0].0 < w[1].0),
            "stream {n} not strictly ascending"
        );
    }
    let mut seen: Vec<(Key, Value)> = streams.into_iter().flatten().collect();
    seen.sort();
    let want: Vec<(Key, Value)> = want.into_iter().collect();
    assert_eq!(seen, want, "filter must see every live key exactly once");
    assert_state(&kv, &want.into_iter().collect(), "keep-all filter");
}

/// Dropped keys are gone from every read path; kept keys are intact; the
/// store keeps working, and a second compaction applies the filter again.
pub fn gc_drops_and_keeps<H: Harness>(h: &H) {
    let kv = h.make();
    let (dropped, kept) = gc_fixture(&kv);
    let (filter, _) = recorder(Some("garbage"));
    kv.set_gc_filter(filter);
    ok(h.compact(&kv), "compact");
    assert_state(&kv, &kept, "after compact");
    for key in dropped.keys() {
        assert_eq!(
            ok(kv.get_latest(key), "get"),
            None,
            "dropped key {key:?} still readable"
        );
    }
    put(&kv, "gk/00", "live-again");
    put(&kv, "gk/02", "garbage-again");
    let mut model = kept.clone();
    model.insert(k("gk/00"), k("live-again"));
    ok(h.compact(&kv), "compact");
    assert_state(&kv, &model, "second compact");
}

/// A snapshot taken before compaction keeps every kept key unchanged and
/// never sees later writes; a dropped key reads as its old value or absent
/// (unspecified, see module docs).
pub fn gc_snapshot_before_compact<H: Harness>(h: &H) {
    let kv = h.make();
    let (dropped, kept) = gc_fixture(&kv);
    let (filter, _) = recorder(Some("garbage"));
    kv.set_gc_filter(filter);
    let snap = kv.snapshot();
    ok(h.compact(&kv), "compact");
    put(&kv, "gk/new", "live");
    put(&kv, "gk/01", "overwritten");
    for (key, v) in &kept {
        assert_eq!(
            ok(snap.get(key), "get").as_ref(),
            Some(v),
            "kept key {key:?} changed"
        );
    }
    for (key, v) in &dropped {
        let got = ok(snap.get(key), "get");
        assert!(
            got.is_none() || got.as_ref() == Some(v),
            "dropped key {key:?} changed value"
        );
    }
    assert_eq!(ok(snap.get(b"gk/new"), "get"), None);
    let rows = dump(&snap);
    for (key, v) in &rows {
        assert!(
            kept.get(key) == Some(v) || dropped.get(key) == Some(v),
            "foreign row {key:?}"
        );
    }
    for key in kept.keys() {
        assert!(
            rows.iter().any(|(r, _)| r == key),
            "kept key {key:?} missing from scan"
        );
    }
}

/// Reference implementation of the C-T0 §9.2 (draft 4) drop rule over the
/// §2.2 layout: within one stream it may drop a version when it has already
/// seen a newer version `<= W` of the same logical key in this stream, and it
/// never drops the newest version `<= W` it has seen for a key. Versions
/// `> W`, intents and keys outside the layout are always kept.
pub struct SpecGcFilter {
    /// The GC watermark `W` (C-T0 §9.1).
    pub w: u64,
}

impl GcFilter for SpecGcFilter {
    fn begin(&self) -> Box<dyn GcStream> {
        Box::new(SpecGcStream {
            w: self.w,
            seen: std::collections::HashSet::new(),
        })
    }
}

struct SpecGcStream {
    w: u64,
    seen: std::collections::HashSet<Key>,
}

impl GcStream for SpecGcStream {
    fn drop_key(&mut self, key: &[u8], _value: &[u8]) -> bool {
        match layout::parse(key) {
            // Versions arrive newest first, so the first version `<= W` of a
            // logical key seen in this stream is the newest one: keep it and
            // remember it; every later (older) version of the same key is
            // shadowed by it and may be dropped.
            Some((l, layout::Entry::Version(ts))) if ts <= self.w => {
                if self.seen.insert(l) {
                    return false;
                }
                true
            }
            _ => false,
        }
    }
}

/// Wraps a filter and records every drop C-T0 §9.2 does not allow: a version
/// that is not shadowed by an already-seen newer version `<= W` of the same
/// logical key in the same stream (the newest version `<= W`, a version
/// `> W`, an intent, or a key outside the layout). Decisions pass through
/// unchanged, so the kv still honours a violating filter blindly.
pub struct SpecGcGuard {
    w: u64,
    inner: Box<dyn GcFilter>,
    violations: Arc<Mutex<Vec<Key>>>,
}

impl SpecGcGuard {
    /// Returns the guard and a handle on the keys it recorded.
    pub fn new(w: u64, inner: Box<dyn GcFilter>) -> (Self, Arc<Mutex<Vec<Key>>>) {
        let violations = Arc::new(Mutex::new(Vec::new()));
        let g = Self {
            w,
            inner,
            violations: Arc::clone(&violations),
        };
        (g, violations)
    }
}

struct GuardStream {
    w: u64,
    inner: Box<dyn GcStream>,
    seen: std::collections::HashSet<Key>,
    violations: Arc<Mutex<Vec<Key>>>,
}

impl GcFilter for SpecGcGuard {
    fn begin(&self) -> Box<dyn GcStream> {
        Box::new(GuardStream {
            w: self.w,
            inner: self.inner.begin(),
            seen: std::collections::HashSet::new(),
            violations: Arc::clone(&self.violations),
        })
    }
}

impl GcStream for GuardStream {
    fn drop_key(&mut self, key: &[u8], value: &[u8]) -> bool {
        let allowed = match layout::parse(key) {
            Some((l, layout::Entry::Version(ts))) if ts <= self.w => self.seen.contains(&l),
            // A version `> W`, an intent, or a foreign key: never droppable.
            _ => false,
        };
        if let Some((l, layout::Entry::Version(ts))) = layout::parse(key) {
            if ts <= self.w {
                self.seen.insert(l);
            }
        }
        let drop = self.inner.drop_key(key, value);
        if drop && !allowed {
            self.violations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(key.to_vec());
        }
        drop
    }
}

/// The seeded rule-2 bug (C-T0 §11 seed 3): drops every tombstone version
/// `<= W`. In the fixtures below the only such tombstone is also the newest
/// version `<= W` of its key, so dropping it resurrects the older versions
/// sitting outside the stream.
pub struct DropsTombstonesLeW {
    pub w: u64,
}

impl GcFilter for DropsTombstonesLeW {
    fn begin(&self) -> Box<dyn GcStream> {
        Box::new(DropsTombstonesLeWStream { w: self.w })
    }
}

struct DropsTombstonesLeWStream {
    w: u64,
}

impl GcStream for DropsTombstonesLeWStream {
    fn drop_key(&mut self, key: &[u8], value: &[u8]) -> bool {
        matches!(layout::parse(key), Some((_, layout::Entry::Version(ts))) if ts <= self.w)
            && layout::is_tombstone(value)
    }
}

/// Reads logical key `l` at snapshot ts `s` the way C-T0 §4 step 2 does over
/// the §2.2 layout: the first (newest) version with `ts <= s`; tombstones and
/// moved-tombstones read as not-found.
pub fn read_at<S: Snapshot>(snap: &S, l: &[u8], s: u64) -> Option<Value> {
    let rows = scan(
        snap,
        Bound::Included(layout::intent_key(l).as_slice()),
        Bound::Excluded(layout::end_key(l).as_slice()),
        false,
    );
    for (key, value) in rows {
        if let Some((_, layout::Entry::Version(ts))) = layout::parse(&key) {
            if ts <= s {
                return if layout::is_tombstone(&value) {
                    None
                } else {
                    Some(value)
                };
            }
        }
    }
    None
}

struct DropAll;
struct DropAllStream;
impl GcFilter for DropAll {
    fn begin(&self) -> Box<dyn GcStream> {
        Box::new(DropAllStream)
    }
}
impl GcStream for DropAllStream {
    fn drop_key(&mut self, _key: &[u8], _value: &[u8]) -> bool {
        true
    }
}

/// The kv honours a filter that breaks §9.2 (it does not second-guess), and
/// `SpecGcGuard` detects every disallowed drop; the reference `SpecGcFilter`
/// yields none, keeps the newest version `<= W` and drops the shadowed ones.
pub fn gc_spec_violation_detectable<H: Harness>(h: &H) {
    const W: u64 = 25;
    let l: &[u8] = b"row/7";
    let fill = |kv: &H::Kv| {
        write(
            kv,
            vec![
                Op::Put(layout::version_key(l, 30), layout::live_value(b"v30")),
                Op::Put(layout::version_key(l, 20), layout::tombstone_value()),
                Op::Put(layout::version_key(l, 10), layout::live_value(b"v10")),
                Op::Put(layout::intent_key(b"row/9"), b"intent".to_vec()),
            ],
            Durability::No,
        );
    };

    // A filter that drops everything. Dropping k@10 is *allowed* (shadowed by
    // the newer k@20 <= W seen earlier in the stream); the other three drops
    // violate the rule.
    let kv = h.make();
    fill(&kv);
    let (guard, violations) = SpecGcGuard::new(W, Box::new(DropAll));
    kv.set_gc_filter(Box::new(guard));
    ok(h.compact(&kv), "compact");
    let mut v = violations
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    v.sort();
    assert_eq!(
        v,
        vec![
            layout::version_key(l, 30),
            layout::version_key(l, 20),
            layout::intent_key(b"row/9"),
        ],
        "disallowed drops not detected"
    );
    assert!(state(&kv).is_empty(), "kv must honour the filter");

    // The seed-3 bug: dropping the newest tombstone <= W (k@20) resurrects
    // k@10 for reads at S in [20, 30) -- the §9.2 hazard, visible even
    // without files.
    let kv = h.make();
    fill(&kv);
    assert_eq!(read_at(&kv.snapshot(), l, 25), None, "tombstone at 20");
    let (guard, violations) = SpecGcGuard::new(W, Box::new(DropsTombstonesLeW { w: W }));
    kv.set_gc_filter(Box::new(guard));
    ok(h.compact(&kv), "compact");
    assert_eq!(
        violations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
        vec![layout::version_key(l, 20)],
        "tombstone drop not detected"
    );
    assert_eq!(
        read_at(&kv.snapshot(), l, 25),
        Some(layout::live_value(b"v10")),
        "resurrected k@10"
    );

    // The reference filter: compliant, keeps 30 (> W) and 20 (newest <= W),
    // drops 10 (shadowed by 20 in the same stream).
    let kv = h.make();
    fill(&kv);
    let (guard, violations) = SpecGcGuard::new(W, Box::new(SpecGcFilter { w: W }));
    kv.set_gc_filter(Box::new(guard));
    ok(h.compact(&kv), "compact");
    assert!(violations
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_empty());
    let model: Model = [
        (layout::version_key(l, 30), layout::live_value(b"v30")),
        (layout::version_key(l, 20), layout::tombstone_value()),
        (layout::intent_key(b"row/9"), b"intent".to_vec()),
    ]
    .into_iter()
    .collect();
    assert_state(&kv, &model, "reference filter");
    assert_eq!(read_at(&kv.snapshot(), l, 25), None, "still not found");
    assert_eq!(
        read_at(&kv.snapshot(), l, 35),
        Some(layout::live_value(b"v30")),
        "reads through the newest version"
    );
}

/// `set_gc_watermark` accepts an increase or an equal value and refuses a
/// decrease with `KvError::WatermarkRegressed`, leaving the watermark
/// unchanged (C-T0 §9.1: `W` is monotonic; the caller treats a regression as
/// fatal).
pub fn gc_watermark_monotonic<H: Harness>(h: &H) {
    let kv = h.make();
    ok(kv.set_gc_watermark(0), "initial (equal to the default)");
    ok(kv.set_gc_watermark(10), "increase");
    ok(kv.set_gc_watermark(10), "equal");
    ok(kv.set_gc_watermark(20), "increase");
    match kv.set_gc_watermark(5) {
        Err(KvError::WatermarkRegressed {
            current: 20,
            requested: 5,
        }) => {}
        other => panic!("regression not refused: {other:?}"),
    }
    // The refusal left W at 20: a value between 5 and 20 is still refused.
    match kv.set_gc_watermark(19) {
        Err(KvError::WatermarkRegressed {
            current: 20,
            requested: 19,
        }) => {}
        other => panic!("refused regression changed W: {other:?}"),
    }
    ok(kv.set_gc_watermark(20), "equal after refusal");
    ok(kv.set_gc_watermark(21), "increase after refusal");
}

/// Without a registered filter, compaction drops nothing.
pub fn gc_without_filter_keeps_all<H: Harness>(h: &H) {
    let kv = h.make();
    let (mut all, kept) = gc_fixture(&kv);
    all.extend(kept);
    ok(h.compact(&kv), "compact");
    assert_state(&kv, &all, "no filter");
}
