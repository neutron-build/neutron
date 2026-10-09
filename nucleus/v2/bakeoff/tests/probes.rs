//! PLAN D2 disqualifier probes, one test per backend (C-S1 work item 3).
//! Every test fails loudly on a violation; PASS evidence (stream orders,
//! kept sets, timings) is printed and summarised in `DECISION.md`. The
//! non-testable statements (on-disk format stability, F_FULLFSYNC
//! mechanics, bus factor) live in `DECISION.md` with source citations.

#![allow(clippy::unwrap_used)] // tests may unwrap (rule: not outside tests)

use std::io::{BufRead, BufReader};
use std::ops::Bound;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nucleus_kv::conformance::layout;
use nucleus_kv::{Batch, Durability, GcFilter, GcStream, KvError, Op, OrderedKv, Snapshot};

fn ok<T>(r: Result<T, KvError>, what: &str) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("{what}: {e}"),
    }
}

fn put<K: OrderedKv>(kv: &K, key: &[u8], val: &[u8]) {
    let mut b = Batch::default();
    b.put(key.to_vec(), val.to_vec());
    ok(kv.write(b, Durability::No), "write");
}

/// Every key in the store (latest state) through one snapshot scan.
fn dump_all<K: OrderedKv>(kv: &K) -> Vec<(Vec<u8>, Vec<u8>)> {
    let snap = kv.snapshot();
    snap.scan((Bound::Unbounded, Bound::Unbounded), false)
        .map(|r| ok(r, "scan item"))
        .collect()
}

/// Every key visible to an existing snapshot.
fn dump_snap<S: Snapshot>(snap: &S) -> Vec<(Vec<u8>, Vec<u8>)> {
    snap.scan((Bound::Unbounded, Bound::Unbounded), false)
        .map(|r| ok(r, "scan item"))
        .collect()
}

// ---- Probe 1: one WAL across keyspaces ------------------------------------
// A child process writes atomic cross-prefix batches (`a/…` + `z/…` + a
// counter) with `Durability::No`; the parent SIGKILLs it mid-stream and
// reopens: every batch must be all-or-nothing, the kept set a prefix, and
// the earlier durable write intact.

fn run_child_and_kill(dir: &Path, backend: &str) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_bakeoff-child"))
        .args([backend, dir.to_str().unwrap_or_default(), "200000"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("batch "), "child progress: {line:?}");
    // Let the child stream a few thousand batches, then SIGKILL mid-stream.
    std::thread::sleep(Duration::from_millis(25));
    child.kill().unwrap();
    let _ = child.wait();
}

fn verify_kill9<K: OrderedKv>(kv: &K) {
    assert_eq!(
        ok(kv.get_latest(b"durable/a"), "get").as_deref(),
        Some(&b"1"[..]),
        "durable write lost across kill -9"
    );
    let mut present: Vec<u64> = Vec::new();
    for (k, _) in dump_all(kv) {
        if k.starts_with(b"a/") {
            let id = std::str::from_utf8(&k[2..])
                .unwrap()
                .parse::<u64>()
                .unwrap();
            present.push(id);
        }
    }
    present.sort_unstable();
    assert!(!present.is_empty(), "no unsynced batches observed at all");
    for &i in &present {
        let key = format!("z/{i:012}");
        assert!(
            ok(kv.get_latest(key.as_bytes()), "get").is_some(),
            "batch {i} torn: a/{i} present, z/{i} missing"
        );
    }
    for w in present.windows(2) {
        assert_eq!(w[1], w[0] + 1, "hole in kept WAL prefix: {w:?}");
    }
    let last = *present.last().unwrap();
    assert_eq!(
        ok(kv.get_latest(b"ctr"), "get").as_deref(),
        Some(last.to_be_bytes().as_slice()),
        "ctr is not the last kept batch"
    );
    // The store is usable after recovery.
    put(kv, b"post/crash", b"ok");
    assert_eq!(
        ok(kv.get_latest(b"post/crash"), "get").as_deref(),
        Some(&b"ok"[..])
    );
}

macro_rules! kill9_test {
    ($name:ident, $backend:literal, $open:path) => {
        #[test]
        fn $name() {
            let (dir, _guard) = nucleus_bakeoff::store_dir(concat!($backend, "-kill9"));
            // Durable pre-child marker: the one WAL must keep it.
            {
                let kv = ok($open(&dir), "open");
                let mut b = Batch::default();
                b.put(b"durable/a".to_vec(), b"1".to_vec());
                b.put(b"durable/z".to_vec(), b"1".to_vec());
                ok(kv.write(b, Durability::Yes), "durable write");
            }
            run_child_and_kill(&dir, $backend);
            {
                let kv = ok($open(&dir), "reopen");
                verify_kill9(&kv);
            }
        }
    };
}

#[cfg(feature = "rocks")]
kill9_test!(
    rocks_one_wal_kill9,
    "rocks",
    nucleus_bakeoff::rocks::RocksKv::open
);
#[cfg(feature = "fjall")]
kill9_test!(
    fjall_one_wal_kill9,
    "fjall",
    nucleus_bakeoff::fjall_kv::FjallKv::open
);

// ---- Probe 2: snapshot-safe GC --------------------------------------------
// Versions of one logical key spread over separate settled files (the C-T0
// §9.2 cross-SST hazard), compacted through the reference `SpecGcFilter`:
// per-stream ascending key order, keep >W, keep newest <=W, drop shadowed,
// never the intent.

struct RecordingSpec {
    w: u64,
    streams: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

impl GcFilter for RecordingSpec {
    fn begin(&self) -> Box<dyn GcStream> {
        let filter = nucleus_kv::conformance::SpecGcFilter { w: self.w };
        self.streams.lock().unwrap().push(Vec::new());
        let idx = self.streams.lock().unwrap().len() - 1;
        Box::new(RecordingSpecStream {
            inner: filter.begin(),
            streams: Arc::clone(&self.streams),
            idx,
        })
    }
}

struct RecordingSpecStream {
    inner: Box<dyn GcStream>,
    streams: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
    idx: usize,
}

impl GcStream for RecordingSpecStream {
    fn drop_key(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.streams.lock().unwrap()[self.idx].push(key.to_vec());
        self.inner.drop_key(key, value)
    }
}

fn gc_probe<K: OrderedKv>(kv: &K, settle: impl Fn(&K), compact: impl Fn(&K)) {
    let l: &[u8] = b"row/7";
    ok(
        kv.write(
            Batch {
                ops: vec![
                    Op::Put(layout::version_key(l, 30), layout::live_value(b"v30")),
                    Op::Put(layout::intent_key(b"row/9"), b"intent".to_vec()),
                ],
            },
            Durability::No,
        ),
        "write v30",
    );
    settle(kv);
    ok(
        kv.write(
            Batch {
                ops: vec![Op::Put(
                    layout::version_key(l, 20),
                    layout::tombstone_value(),
                )],
            },
            Durability::No,
        ),
        "write v20",
    );
    settle(kv);
    ok(
        kv.write(
            Batch {
                ops: vec![Op::Put(
                    layout::version_key(l, 10),
                    layout::live_value(b"v10"),
                )],
            },
            Durability::No,
        ),
        "write v10",
    );
    settle(kv);

    let streams = Arc::new(Mutex::new(Vec::new()));
    kv.set_gc_filter(Box::new(RecordingSpec {
        w: 25,
        streams: Arc::clone(&streams),
    }));
    compact(kv);

    let streams = streams.lock().unwrap().clone();
    assert!(!streams.is_empty(), "compaction never called the filter");
    for (n, s) in streams.iter().enumerate() {
        assert!(
            s.windows(2).all(|w| w[0] < w[1]),
            "stream {n} not strictly ascending: {s:?}"
        );
    }

    let state: std::collections::BTreeMap<_, _> = dump_all(kv).into_iter().collect();
    let expect: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (layout::version_key(l, 30), layout::live_value(b"v30")),
        (layout::version_key(l, 20), layout::tombstone_value()),
        (layout::intent_key(b"row/9"), b"intent".to_vec()),
    ];
    for (k, v) in &expect {
        assert_eq!(state.get(k), Some(v), "wrong survivor set: {state:?}");
    }
    assert_eq!(state.len(), expect.len(), "wrong survivor set: {state:?}");
    let snap = kv.snapshot();
    assert_eq!(
        nucleus_kv::conformance::read_at(&snap, l, 25),
        None,
        "resurrected v10 after GC"
    );
    assert_eq!(
        nucleus_kv::conformance::read_at(&snap, l, 35),
        Some(layout::live_value(b"v30"))
    );
}

#[cfg(feature = "rocks")]
#[test]
fn rocks_snapshot_safe_gc() {
    let kv = nucleus_bakeoff::rocks::RocksKv::owned_scratch("rocks-gc-probe").unwrap();
    gc_probe(
        &kv,
        |k| ok(k.settle(), "settle"),
        |k| ok(k.compact_all(), "compact"),
    );
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_snapshot_safe_gc() {
    let kv = nucleus_bakeoff::fjall_kv::FjallKv::owned_scratch("fjall-gc-probe").unwrap();
    gc_probe(
        &kv,
        |k| ok(k.settle(), "settle"),
        |k| ok(k.compact_all(), "compact"),
    );
}

// ---- Probe 3: DeleteRange over multi-version keys --------------------------
// §2.2 GC range of the newest tombstone <= a snapshot must remove it and
// every older version, with no resurrection after compaction.

fn delete_range_probe<K: OrderedKv>(kv: &K, settle: impl Fn(&K), compact: impl Fn(&K)) {
    let l: &[u8] = b"row/3";
    // Newest first: v20, v10, then the tombstone at ts=5 with v3 beneath it.
    ok(
        kv.write(
            Batch {
                ops: vec![
                    Op::Put(layout::version_key(l, 20), layout::live_value(b"v20")),
                    Op::Put(layout::version_key(l, 10), layout::live_value(b"v10")),
                    Op::Put(layout::version_key(l, 5), layout::tombstone_value()),
                    Op::Put(layout::version_key(l, 3), layout::live_value(b"v3")),
                ],
            },
            Durability::No,
        ),
        "write versions",
    );
    settle(kv);
    let snap = kv.snapshot();
    assert_eq!(
        nucleus_kv::conformance::read_at(&snap, l, 12),
        Some(layout::live_value(b"v10")),
        "v10 visible at S=12 before GC"
    );
    assert_eq!(
        nucleus_kv::conformance::read_at(&snap, l, 7),
        None,
        "tombstone at ts=5 hides v3 at S=7 before GC"
    );
    // C-T0 §9.2: the GC range of the tombstone L@5 is
    // [version_key(l, 5), end(l)): the tombstone and every older version.
    ok(
        kv.write(
            Batch {
                ops: vec![Op::DeleteRange {
                    start: layout::version_key(l, 5),
                    end: layout::end_key(l),
                }],
            },
            Durability::No,
        ),
        "GC DeleteRange",
    );
    compact(kv);
    let state: std::collections::BTreeMap<_, _> = dump_all(kv).into_iter().collect();
    let expect = vec![
        (layout::version_key(l, 20), layout::live_value(b"v20")),
        (layout::version_key(l, 10), layout::live_value(b"v10")),
    ];
    for (k, v) in &expect {
        assert_eq!(state.get(k), Some(v), "wrong survivor set: {state:?}");
    }
    assert_eq!(
        state.len(),
        expect.len(),
        "tombstone or older version resurrected: {state:?}"
    );
    let snap = kv.snapshot();
    assert_eq!(
        nucleus_kv::conformance::read_at(&snap, l, 12),
        Some(layout::live_value(b"v10")),
        "read at S=12 damaged"
    );
    assert_eq!(
        nucleus_kv::conformance::read_at(&snap, l, 7),
        None,
        "still not found at S=7 (tombstone removed with its shadowed versions)"
    );
    assert_eq!(
        nucleus_kv::conformance::read_at(&snap, l, 4),
        None,
        "v3 resurrected for reads below the removed tombstone"
    );
}

#[cfg(feature = "rocks")]
#[test]
fn rocks_delete_range_multi_version() {
    let kv = nucleus_bakeoff::rocks::RocksKv::owned_scratch("rocks-dr-probe").unwrap();
    delete_range_probe(
        &kv,
        |k| ok(k.settle(), "settle"),
        |k| ok(k.compact_all(), "compact"),
    );
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_delete_range_multi_version() {
    let kv = nucleus_bakeoff::fjall_kv::FjallKv::owned_scratch("fjall-dr-probe").unwrap();
    delete_range_probe(
        &kv,
        |k| ok(k.settle(), "settle"),
        |k| ok(k.compact_all(), "compact"),
    );
}

// ---- Probe 4: bulk SST ingest ----------------------------------------------
// 10k sorted keys ingested at once: visible atomically to new readers,
// invisible to a snapshot taken before, values intact.

fn ingest_probe<K: OrderedKv>(kv: &K) {
    put(kv, b"x/live", b"here");
    let before = kv.snapshot();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000u32)
        .map(|i| {
            (
                format!("bulk/{i:06}").into_bytes(),
                i.to_be_bytes().to_vec(),
            )
        })
        .collect();
    ok(
        kv.ingest_sorted(&mut entries.clone().into_iter()),
        "ingest 10k",
    );
    assert_eq!(
        dump_snap(&before),
        vec![(b"x/live".to_vec(), b"here".to_vec())]
    );
    let state = dump_all(kv);
    assert_eq!(state.len(), 10_001, "ingest not fully visible");
    for (k, v) in &entries {
        assert_eq!(
            ok(kv.get_latest(k), "get").as_deref(),
            Some(v.as_slice()),
            "ingested value damaged at {k:?}"
        );
    }
}

#[cfg(feature = "rocks")]
#[test]
fn rocks_bulk_ingest() {
    let kv = nucleus_bakeoff::rocks::RocksKv::owned_scratch("rocks-ing-probe").unwrap();
    ingest_probe(&kv);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_bulk_ingest() {
    let kv = nucleus_bakeoff::fjall_kv::FjallKv::owned_scratch("fjall-ing-probe").unwrap();
    ingest_probe(&kv);
}

// ---- Probe 5: checkpoint + WAL tail ----------------------------------------
// A checkpoint holds exactly the completed writes at checkpoint time; the
// live store's WAL tail (unsynced writes after the checkpoint) stays in the
// live store only.

macro_rules! checkpoint_test {
    ($name:ident, $open:path) => {
        #[test]
        fn $name() {
            let (dir, _guard) = nucleus_bakeoff::store_dir(stringify!($name));
            let kv = ok($open(&dir), "open");
            put(&kv, b"ck/a", b"1");
            ok(kv.sync_wal(), "sync");
            let ckpt = dir.parent().unwrap().join(format!(
                "{}-ckpt",
                dir.file_name().unwrap().to_string_lossy()
            ));
            let _ck_guard = nucleus_bakeoff::ScratchGuard::new(ckpt.clone());
            ok(kv.checkpoint(&ckpt), "checkpoint");
            put(&kv, b"ck/tail", b"unsynced");
            {
                let back = ok($open(&ckpt), "open checkpoint");
                assert_eq!(
                    ok(back.get_latest(b"ck/a"), "get").as_deref(),
                    Some(&b"1"[..]),
                    "checkpoint missing a completed write"
                );
                assert_eq!(
                    ok(back.get_latest(b"ck/tail"), "get"),
                    None,
                    "WAL tail leaked into the checkpoint"
                );
                put(&back, b"ck/own", b"w");
                assert_eq!(
                    ok(back.get_latest(b"ck/own"), "get").as_deref(),
                    Some(&b"w"[..]),
                    "checkpoint not writable"
                );
            }
            // Reopening the checkpoint again still works (self-contained).
            {
                let back = ok($open(&ckpt), "reopen checkpoint");
                assert_eq!(
                    ok(back.get_latest(b"ck/a"), "get").as_deref(),
                    Some(&b"1"[..])
                );
            }
            // The live store keeps its tail.
            assert_eq!(
                ok(kv.get_latest(b"ck/tail"), "get").as_deref(),
                Some(&b"unsynced"[..]),
                "live store lost its WAL tail"
            );
        }
    };
}

#[cfg(feature = "rocks")]
checkpoint_test!(
    rocks_checkpoint_wal_tail,
    nucleus_bakeoff::rocks::RocksKv::open
);
#[cfg(feature = "fjall")]
checkpoint_test!(
    fjall_checkpoint_wal_tail,
    nucleus_bakeoff::fjall_kv::FjallKv::open
);

// ---- Probe 6: checksums report corruption as an error ----------------------
// Flush everything to data files, close the store, flip one byte inside the
// largest data file, and reopen: the corruption must surface as an error
// (from recovery or from reads), never as silently missing data.

fn flip_a_byte_in_largest(dir: &Path, pick: &dyn Fn(&Path) -> bool) -> std::path::PathBuf {
    let mut best: Option<(u64, std::path::PathBuf)> = None;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let rd = match std::fs::read_dir(&d) {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                if pick(&p) {
                    let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                    if len > 1024 && best.as_ref().is_none_or(|(l, _)| len > *l) {
                        best = Some((len, p.clone()));
                    }
                }
            }
        }
    }
    let (len, path) = best.expect("a data file to corrupt");
    let mut bytes = std::fs::read(&path).unwrap();
    let mid = (len / 2) as usize;
    bytes[mid] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
    path
}

/// RocksDB data files.
fn sst_file(p: &Path) -> bool {
    p.extension().is_some_and(|e| e == "sst")
}

/// lsm-tree table files (the `tables/` folder; the journal is a preallocated
/// sparse file whose tail past the last batch is truncated on recovery).
fn fjall_table_file(p: &Path) -> bool {
    p.ancestors()
        .any(|a| a.file_name().is_some_and(|n| n == "tables"))
}

macro_rules! corruption_test {
    ($name:ident, $open:path, $settle:expr, $pick:expr) => {
        #[test]
        fn $name() {
            let (dir, _guard) = nucleus_bakeoff::store_dir(stringify!($name));
            {
                let kv = ok($open(&dir), "open");
                for i in 0..20_000u32 {
                    put(
                        &kv,
                        format!("corrupt/{i:08}").as_bytes(),
                        vec![u8::try_from(i % 256).unwrap(); 64].as_slice(),
                    );
                }
                ok(kv.sync_wal(), "sync");
                $settle(&kv);
            }
            let flipped = flip_a_byte_in_largest(&dir, &$pick);
            let kv = match $open(&dir) {
                // Corruption detected during recovery already counts.
                Err(KvError::Corruption(e)) => {
                    println!(
                        "{}: recovery refused to open after flipping a byte in {flipped:?}: {e}",
                        stringify!($name)
                    );
                    return;
                }
                Err(e) => panic!("reopen failed with a non-corruption error: {e}"),
                Ok(kv) => kv,
            };
            let mut saw_err: Option<KvError> = None;
            for i in 0..20_000u32 {
                let key = format!("corrupt/{i:08}");
                match kv.get_latest(key.as_bytes()) {
                    Err(e) => {
                        saw_err = Some(e);
                        break;
                    }
                    Ok(v) => {
                        assert!(
                            v.is_none() || v.as_deref().is_some_and(|x| x.len() == 64),
                            "silent corruption at {key:?}"
                        );
                    }
                }
            }
            if saw_err.is_none() {
                // Point reads may dodge the flipped block; a full scan must
                // touch it.
                let snap = kv.snapshot();
                for r in snap.scan((Bound::Unbounded, Bound::Unbounded), false) {
                    if let Err(e) = r {
                        saw_err = Some(e);
                        break;
                    }
                }
            }
            let err = saw_err.expect("corruption not detected");
            assert!(
                matches!(err, KvError::Corruption(_)),
                "corruption reported as {err:?}"
            );
            println!(
                "{}: flipped a byte in {flipped:?}, read error: {err}",
                stringify!($name)
            );
        }
    };
}

#[cfg(feature = "rocks")]
corruption_test!(
    rocks_checksum_corruption_detected,
    nucleus_bakeoff::rocks::RocksKv::open,
    |kv: &nucleus_bakeoff::rocks::RocksKv| kv.settle().unwrap(),
    sst_file
);
#[cfg(feature = "fjall")]
corruption_test!(
    fjall_checksum_corruption_detected,
    nucleus_bakeoff::fjall_kv::FjallKv::open,
    |kv: &nucleus_bakeoff::fjall_kv::FjallKv| kv.settle().unwrap(),
    fjall_table_file
);

// ---- Probe 7 (partial): durability path exercise ---------------------------
// F_FULLFSYNC itself is a source-level statement (DECISION.md); here we
// exercise that a Durability::Yes write plus immediate kill -9 keeps the
// write (the durable marker of probe 1 already proves this; this probe adds
// the reverse: a `sync_wal` barrier).

macro_rules! sync_barrier_test {
    ($name:ident, $open:path) => {
        #[test]
        fn $name() {
            let (dir, _guard) = nucleus_bakeoff::store_dir(stringify!($name));
            {
                let kv = ok($open(&dir), "open");
                put(&kv, b"sb/unsynced-1", b"1");
                put(&kv, b"sb/unsynced-2", b"2");
                ok(kv.sync_wal(), "sync_wal barrier");
                put(&kv, b"sb/after", b"3");
            }
            let kv = ok($open(&dir), "reopen");
            for (k, v) in [("sb/unsynced-1", "1"), ("sb/unsynced-2", "2")] {
                assert_eq!(
                    ok(kv.get_latest(k.as_bytes()), "get").as_deref(),
                    Some(v.as_bytes()),
                    "write before the sync barrier lost"
                );
            }
        }
    };
}

#[cfg(feature = "rocks")]
sync_barrier_test!(
    rocks_sync_wal_barrier,
    nucleus_bakeoff::rocks::RocksKv::open
);
#[cfg(feature = "fjall")]
sync_barrier_test!(
    fjall_sync_wal_barrier,
    nucleus_bakeoff::fjall_kv::FjallKv::open
);
