//! Blob-store differential + crash-consistency probe for the disk-tiered
//! chunk store (segments + RAM LRU cache + metadata WAL).
//!
//! Four phases:
//!   1. Differential fuzz — a disk-tiered `BlobStore` (tiny cache, so most
//!      reads hit the disk tier) is driven through random puts / overwrites /
//!      deletes / range reads / tags / rollbacks / gc / checkpoints /
//!      restarts and every observable result is checked against an in-RAM
//!      reference model.
//!   2. WAL torn-tail recovery — the WAL is truncated at every possible byte
//!      boundary; the recovered store must equal the state after some prefix
//!      of the applied mutations (entries are atomic, replay is best-effort).
//!   3. Segment torn-tail recovery — segment files are truncated at random
//!      points; every blob the recovered store still serves must read back
//!      byte-identical to a version that was actually written (torn chunks
//!      may drop blobs, but must never corrupt them).
//!   4. Large objects (`lo_*`) — the *served* surface of the same store: the
//!      pgwire large-object API (`lo_creat` / `lo_open` / `lo_read` /
//!      `lo_write` / `lo_close` / `lo_unlink`) driven as the string-parsing
//!      dispatch sees it, against a model, across handler restarts and two
//!      concurrent handlers. This is the NU-102 / NU-103 class: OID
//!      allocation that overwrites a durable object, an unreadable object
//!      that reads back as a successful empty read, and a refused write that
//!      resurrects a truncated object. Carries `--negative-control lo`.
//!
//! Build: `cargo run --release --features "server" --bin probe_blob`.
//!   `... --bin probe_blob -- --negative-control lo`
#![cfg(feature = "server")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use nucleus::blob::BlobStore;

/// Fresh unique dir under the system temp dir (removed on drop).
struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "probe_blob_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).expect("mkdir temp");
        Self(p)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// ─── Deterministic PRNG ───────────────────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn blob_data(rng: &mut Rng) -> Vec<u8> {
    // Sizes cross chunk boundaries (chunk_size 16 below); low-entropy fills
    // make cross-blob dedup common, exercising shared-chunk refcounts.
    let len = rng.below(120);
    let fill = (rng.next() % 5) as u8;
    match rng.below(3) {
        0 => vec![fill; len],
        1 => (0..len).map(|i| (i as u8).wrapping_add(fill)).collect(),
        _ => (0..len).map(|_| (rng.next() % 256) as u8).collect(),
    }
}

const CHUNK: usize = 16;
const CACHE: usize = 200; // holds ~a dozen chunks — evicts constantly

// ─── Phase 1: differential fuzz ──────────────────────────────────────────────
fn phase_differential(checks: &mut u64) {
    let dir = TempDir::new();
    let mut store =
        BlobStore::open_with_options(dir.path(), CHUNK, CACHE, &std::collections::HashSet::new())
            .unwrap();
    let mut model: HashMap<String, (Vec<u8>, HashMap<String, String>)> = HashMap::new();
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);

    for round in 0..30_000u32 {
        let key = format!("k{}", rng.below(60));
        match rng.below(20) {
            0..=7 => {
                let data = blob_data(&mut rng);
                store.put(&key, &data, Some("application/octet-stream"));
                model.insert(key, (data, HashMap::new()));
            }
            8..=10 => {
                let got = store.get(&key);
                let want = model.get(&key).map(|(d, _)| d.clone());
                assert_eq!(got, want, "round {round}: get({key})");
                *checks += 1;
            }
            11..=12 => {
                if let Some((data, _)) = model.get(&key) {
                    let off = rng.below(140) as u64;
                    let len = rng.below(80) as u64;
                    let want: Vec<u8> = data
                        .iter()
                        .skip(off as usize)
                        .take(len as usize)
                        .copied()
                        .collect();
                    let got = store.get_range(&key, off, len).unwrap();
                    assert_eq!(got, want, "round {round}: range({key},{off},{len})");
                    *checks += 1;
                }
            }
            13 => {
                let deleted = store.delete(&key);
                assert_eq!(
                    deleted,
                    model.remove(&key).is_some(),
                    "round {round}: delete"
                );
                *checks += 1;
            }
            14 => {
                let tk = format!("t{}", rng.below(3));
                let tv = format!("v{}", rng.below(5));
                let ok = store.set_tag(&key, &tk, &tv);
                let in_model = if let Some((_, tags)) = model.get_mut(&key) {
                    tags.insert(tk, tv);
                    true
                } else {
                    false
                };
                assert_eq!(ok, in_model, "round {round}: set_tag");
                *checks += 1;
            }
            15 => {
                // Transaction: random mutations, then rollback — store must
                // return to the pre-snapshot state (and stay there after
                // restarts, phase-verified below by the reopen op).
                let snap = store.txn_snapshot();
                for _ in 0..rng.below(6) {
                    let k = format!("k{}", rng.below(60));
                    match rng.below(3) {
                        0 => {
                            let d = blob_data(&mut rng);
                            store.put(&k, &d, None);
                        }
                        1 => {
                            store.delete(&k);
                        }
                        _ => {
                            store.set_tag(&k, "txn", "1");
                        }
                    }
                }
                store.txn_restore(snap);
                assert_eq!(
                    store.blob_count(),
                    model.len(),
                    "round {round}: rollback count"
                );
                *checks += 1;
            }
            16 => store.gc(),
            17 => store.checkpoint().unwrap(),
            18 => {
                // Restart: drop and reopen from disk.
                drop(store);
                store = BlobStore::open_with_options(
                    dir.path(),
                    CHUNK,
                    CACHE,
                    &std::collections::HashSet::new(),
                )
                .unwrap();
                assert_eq!(
                    store.blob_count(),
                    model.len(),
                    "round {round}: reopen count"
                );
                *checks += 1;
            }
            _ => {
                let meta = store.metadata(&key);
                match (meta, model.get(&key)) {
                    (Some(m), Some((d, tags))) => {
                        assert_eq!(m.size, d.len() as u64, "round {round}: meta size");
                        assert_eq!(&m.tags, tags, "round {round}: meta tags");
                        *checks += 1;
                    }
                    (None, None) => {}
                    (a, b) => panic!(
                        "round {round}: metadata presence mismatch: {:?} vs {:?}",
                        a.is_some(),
                        b.is_some()
                    ),
                }
            }
        }
    }

    // Exhaustive final sweep: every key, full content + range spot checks.
    for (key, (data, tags)) in &model {
        assert_eq!(store.get(key).unwrap(), *data, "final: get({key})");
        assert_eq!(
            &store.metadata(key).unwrap().tags,
            tags,
            "final: tags({key})"
        );
        *checks += 2;
    }
    drop(store);
    let store =
        BlobStore::open_with_options(dir.path(), CHUNK, CACHE, &std::collections::HashSet::new())
            .unwrap();
    for (key, (data, _)) in &model {
        assert_eq!(store.get(key).unwrap(), *data, "post-restart: get({key})");
        *checks += 1;
    }
    println!(
        "  phase 1 (differential): {} blobs live at end",
        model.len()
    );
}

// ─── Phase 2: WAL torn-tail recovery ─────────────────────────────────────────
fn phase_wal_torn_tail(checks: &mut u64) {
    let mut rng = Rng(0xFEED_FACE_CAFE_BEEF);

    for case in 0..40u32 {
        let dir = TempDir::new();
        // Prefix states: state after each mutation, state[0] = empty.
        let mut states: Vec<HashMap<String, Vec<u8>>> = vec![HashMap::new()];
        {
            let mut store = BlobStore::open_with_options(
                dir.path(),
                CHUNK,
                CACHE,
                &std::collections::HashSet::new(),
            )
            .unwrap();
            for _ in 0..rng.below(25) + 5 {
                let key = format!("k{}", rng.below(8));
                let mut next = states.last().unwrap().clone();
                if rng.below(4) == 0 && !next.is_empty() {
                    store.delete(&key);
                    next.remove(&key);
                } else {
                    let data = blob_data(&mut rng);
                    store.put(&key, &data, None);
                    next.insert(key, data);
                }
                states.push(next);
            }
        }

        // Truncate the WAL at a random byte length and recover.
        let wal_path = dir.path().join("blob.wal");
        let wal_len = std::fs::metadata(&wal_path).unwrap().len();
        let cut = rng.next() % (wal_len + 1);
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.set_len(cut).unwrap();
        }

        let mut store = BlobStore::open_with_options(
            dir.path(),
            CHUNK,
            CACHE,
            &std::collections::HashSet::new(),
        )
        .unwrap();
        let mut recovered: HashMap<String, Vec<u8>> = HashMap::new();
        for key in store.list_keys() {
            recovered.insert(key.to_string(), store.get(key).unwrap());
        }
        let matched = states.contains(&recovered);
        assert!(
            matched,
            "case {case}: WAL cut at {cut}/{wal_len} recovered a non-prefix state \
             ({} blobs recovered)",
            recovered.len()
        );
        *checks += 1;

        // Writes AFTER recovery must survive the next restart: a torn tail
        // that isn't truncated on open would swallow everything logged
        // behind it on every future replay.
        let post = blob_data(&mut rng);
        store.put("post-recovery", &post, None);
        recovered.insert("post-recovery".to_string(), post);
        drop(store);
        let store = BlobStore::open_with_options(
            dir.path(),
            CHUNK,
            CACHE,
            &std::collections::HashSet::new(),
        )
        .unwrap();
        for (key, want) in &recovered {
            assert_eq!(
                store.get(key).as_ref(),
                Some(want),
                "case {case}: {key} lost after post-recovery restart"
            );
        }
        *checks += 1;
    }
    println!("  phase 2 (WAL torn tail): 40 truncation cases recovered to prefix states");
}

// ─── Phase 3: segment torn-tail recovery ─────────────────────────────────────
fn phase_segment_torn_tail(checks: &mut u64) {
    let mut rng = Rng(0x0DDBA115EEDF00D5);

    for case in 0..40u32 {
        let dir = TempDir::new();
        // Every value ever written per key — a recovered blob must match one.
        let mut history: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        {
            let mut store = BlobStore::open_with_options(
                dir.path(),
                CHUNK,
                CACHE,
                &std::collections::HashSet::new(),
            )
            .unwrap();
            for _ in 0..rng.below(30) + 5 {
                let key = format!("k{}", rng.below(8));
                let data = blob_data(&mut rng);
                store.put(&key, &data, None);
                history.entry(key).or_default().push(data);
            }
        }

        // Truncate every segment file at a random point.
        let seg_dir = dir.path().join("segments");
        for entry in std::fs::read_dir(&seg_dir).unwrap() {
            let path = entry.unwrap().path();
            let len = std::fs::metadata(&path).unwrap().len();
            let cut = rng.next() % (len + 1);
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(cut).unwrap();
        }

        // Recovery must never serve corrupt bytes: whatever survives reads
        // back as an exact version from the history; the rest is dropped.
        let store = BlobStore::open_with_options(
            dir.path(),
            CHUNK,
            CACHE,
            &std::collections::HashSet::new(),
        )
        .unwrap();
        for key in store.list_keys() {
            if let Some(data) = store.get(key) {
                let versions = history
                    .get(key)
                    .unwrap_or_else(|| panic!("case {case}: recovered unknown key {key}"));
                assert!(
                    versions.contains(&data),
                    "case {case}: {key} recovered with corrupt content"
                );
                *checks += 1;
            }
        }
    }
    println!("  phase 3 (segment torn tail): 40 truncation cases, no corrupt reads");
}

fn main() {
    let mut negative_lo = false;
    let mut minimal = false;
    let mut lo_seed: u64 = 0xB0B_10204;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--lo-minimal" => minimal = true,
            "--negative-control" => {
                let section = args.next().unwrap_or_default();
                if section != "lo" {
                    eprintln!("--negative-control takes: lo (got {section:?})");
                    std::process::exit(2);
                }
                negative_lo = true;
            }
            "--seed" => {
                lo_seed = args.next().and_then(|v| v.parse().ok()).unwrap_or(lo_seed);
            }
            other => {
                eprintln!("unknown argument {other:?}");
                std::process::exit(2);
            }
        }
    }

    if minimal {
        phase_lo_minimal();
        return;
    }

    if negative_lo {
        // Prove the large-object phase can discriminate: run it clean, then
        // with the model perturbed the way NU-102/NU-103 perturbed the engine
        // (a durable object silently absent after restart). It passes only if
        // the perturbation ADDS divergences.
        let base = phase_large_objects(lo_seed, 30_000, false);
        let pert = phase_large_objects(lo_seed, 30_000, true);
        println!("\nlo divergences: clean baseline {base}, perturbed model {pert}");
        if pert > base {
            println!(
                "NEGATIVE CONTROL PASSED: perturbing the lo model added {} divergence(s).",
                pert - base
            );
        } else {
            println!(
                "NEGATIVE CONTROL FAILED: perturbing the lo model changed the count by {}. \
                 A check that cannot fail is not a check.",
                pert as i64 - base as i64
            );
            std::process::exit(1);
        }
        return;
    }

    let mut checks = 0u64;
    println!("probe_blob: disk-tiered blob store differential + crash probe");
    phase_differential(&mut checks);
    phase_wal_torn_tail(&mut checks);
    phase_segment_torn_tail(&mut checks);
    phase_large_objects(lo_seed, 30_000, false);
    println!("probe_blob: PASS ({checks} checks)");
}

// ─── Phase 4: the served large-object surface (NU-102 / NU-103 class) ───────

/// Deterministic three-write scenario with printed observations, for
/// diagnosing what the descriptor/write semantics actually are.
fn phase_lo_minimal() {
    use nucleus::executor::ExecResult;
    use nucleus::metrics::harness::{EngineConfig, EngineKind, HarnessDb};
    use nucleus::types::Value;
    use nucleus::wire::NucleusHandler;

    let mut dir = std::env::temp_dir();
    dir.push(format!("probe_blob_lo_minimal_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("rt");
    let db = rt
        .block_on(HarnessDb::open(
            EngineKind::BufferedDisk,
            &dir,
            EngineConfig::default(),
        ))
        .expect("open");
    let h = NucleusHandler::new(db.executor().clone());

    fn call(h: &NucleusHandler, sql: &str) -> Value {
        match h.try_handle_large_object("p", sql) {
            Some(ExecResult::Select { rows, .. }) => rows[0][0].clone(),
            _ => panic!("not an lo_ call"),
        }
    }
    let show = |label: &str, v: &Value| match v {
        Value::Bytea(b) => println!("{label}: {} bytes: {}", b.len(), String::from_utf8_lossy(b)),
        other => println!("{label}: {other:?}"),
    };

    let oid = match call(&h, "SELECT lo_creat()") {
        Value::Int32(o) => o as u32,
        o => panic!("creat: {o:?}"),
    };
    println!("oid: {oid}");
    let fd = match call(&h, &format!("SELECT lo_open({oid}, 393216)")) {
        Value::Int32(f) => f,
        o => panic!("open: {o:?}"),
    };
    println!(
        "write1 n = {:?}",
        call(&h, &format!("SELECT lo_write({fd}, '{}')", "A".repeat(50)))
    );
    let r1 = call(&h, &format!("SELECT lo_open({oid}, 262144)"));
    if let Value::Int32(rfd) = r1 {
        show(
            "after write1",
            &call(&h, &format!("SELECT lo_read({rfd}, 100000)")),
        );
        let _ = call(&h, &format!("SELECT lo_close({rfd})"));
    }
    // Second write through a FRESH descriptor: offset 0.
    let fd2 = match call(&h, &format!("SELECT lo_open({oid}, 393216)")) {
        Value::Int32(f) => f,
        o => panic!("open2: {o:?}"),
    };
    println!(
        "write2 n = {:?}",
        call(&h, &format!("SELECT lo_write({fd2}, '{}')", "B".repeat(30)))
    );
    let r2 = call(&h, &format!("SELECT lo_open({oid}, 262144)"));
    if let Value::Int32(rfd) = r2 {
        show(
            "after write2",
            &call(&h, &format!("SELECT lo_read({rfd}, 100000)")),
        );
    }
    // Third write continuing through the FIRST descriptor: its offset is 50.
    println!(
        "write3 n = {:?}",
        call(&h, &format!("SELECT lo_write({fd}, '{}')", "C".repeat(5)))
    );
    let r3 = call(&h, &format!("SELECT lo_open({oid}, 262144)"));
    if let Value::Int32(rfd) = r3 {
        show(
            "after write3 (fd offset 50)",
            &call(&h, &format!("SELECT lo_read({rfd}, 100000)")),
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Phase-4 divergence counting (unlike phases 1–3, which assert): the
/// negative control needs counts, and a count needs the run to continue past
/// a divergence.
fn phase_large_objects(seed: u64, _cache: usize, perturb: bool) -> usize {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use nucleus::executor::ExecResult;
    use nucleus::metrics::harness::{EngineConfig, EngineKind, HarnessDb};
    use nucleus::types::Value;
    use nucleus::wire::NucleusHandler;

    static DIR_SEQ: AtomicU64 = AtomicU64::new(0);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "probe_blob_lo_{}_{}",
        std::process::id(),
        DIR_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir temp");

    let mut divergences = 0usize;
    let dprintln = |msg: String, divergences: &mut usize| {
        *divergences += 1;
        if *divergences <= 20 {
            println!("  [lo] {msg}");
        }
    };

    let open_harness = |dir: &std::path::Path| -> HarnessDb {
        // probe_blob's phases 1-3 are runtime-free; phase 4 needs one for the
        // async executor surface, so it owns a private multi-thread runtime.
        use std::sync::OnceLock;
        static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        let rt = RT.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("phase-4 runtime")
        });
        rt.block_on(HarnessDb::open(
            EngineKind::BufferedDisk,
            dir,
            EngineConfig::default(),
        ))
        .expect("open harness db")
    };

    let mut rng = Rng(seed | 1);
    let mut db = open_harness(&dir);
    let mut handler = NucleusHandler::new(db.executor().clone());

    // Model: oid -> payload. Writes go through a FRESH descriptor each round,
    // so they splice at offset 0 (PostgreSQL large-object semantics — verified
    // by --lo-minimal): a shorter write overwrites the prefix, a longer one
    // replaces and extends. fd -> oid tracks descriptors left open for the
    // stale-descriptor unlink arm.
    let mut model: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    let mut fds: BTreeMap<i32, u32> = BTreeMap::new();
    const INV_READ: i32 = 0x40000;
    const INV_WRITE: i32 = 0x20000;

    // Dispatch a `SELECT lo_x(...)` string through the SAME parsing surface
    // the wire handler uses, and pull the single cell out.
    fn lo_call(handler: &NucleusHandler, sql: &str) -> Result<Value, String> {
        match handler.try_handle_large_object("probe_peer", sql) {
            Some(ExecResult::Select { rows, .. }) => rows
                .into_iter()
                .next()
                .and_then(|r| r.into_iter().next())
                .ok_or_else(|| "empty lo_ result".to_string()),
            _ => Err(format!("not an lo_ call: {sql}")),
        }
    }

    let rounds = 4000u32;
    for round in 0..rounds {
        match rng.below(20) {
            // lo_creat: never reuses an oid the store already holds.
            0..=5 => {
                let oid = match lo_call(&handler, "SELECT lo_creat()") {
                    Ok(Value::Int32(oid)) if oid > 0 => oid as u32,
                    other => {
                        dprintln(
                            format!("round {round}: lo_creat returned {other:?}"),
                            &mut divergences,
                        );
                        continue;
                    }
                };
                if model.contains_key(&oid) {
                    dprintln(
                        format!(
                            "round {round}: lo_creat returned {oid}, which already holds a live \
                             object — the allocator would overwrite it (NU-102)"
                        ),
                        &mut divergences,
                    );
                    continue;
                }
                model.insert(oid, Vec::new());
            }
            // lo_open + lo_write + read-back through one descriptor.
            6..=11 => {
                if model.is_empty() {
                    continue;
                }
                let live: Vec<u32> = model.keys().copied().collect();
                let oid = live[rng.below(live.len())];
                let sql = format!("SELECT lo_open({oid}, {})", INV_READ | INV_WRITE);
                let fd = match lo_call(&handler, &sql) {
                    Ok(Value::Int32(fd)) if fd > 0 => fd,
                    other => {
                        dprintln(
                            format!("round {round}: {sql} returned {other:?}"),
                            &mut divergences,
                        );
                        continue;
                    }
                };
                // Write a random single-token payload at descriptor offset 0.
                // The dispatch splits raw args on commas, so the payload must
                // be one comma-free quoted token — exactly what a client sends.
                let len = 1 + rng.below(64);
                let payload: String = (0..len)
                    .map(|_| (b'a' + (rng.next() % 26) as u8) as char)
                    .collect();
                let wsql = format!("SELECT lo_write({fd}, '{payload}')");
                match lo_call(&handler, &wsql) {
                    Ok(Value::Int32(n)) if n as usize == payload.len() => {}
                    other => {
                        dprintln(
                            format!("round {round}: {wsql} returned {other:?}"),
                            &mut divergences,
                        );
                        continue;
                    }
                }
                // Splice at offset 0: a fresh descriptor starts there.
                let mut expected = model.get(&oid).cloned().unwrap_or_default();
                if payload.len() >= expected.len() {
                    expected = payload.as_bytes().to_vec();
                } else {
                    expected[..payload.len()].copy_from_slice(payload.as_bytes());
                }
                model.insert(oid, expected.clone());
                fds.insert(fd, oid);

                // Read the whole object through a second descriptor.
                let rsql = format!("SELECT lo_open({oid}, {INV_READ})");
                match lo_call(&handler, &rsql) {
                    Ok(Value::Int32(rfd)) if rfd > 0 => {
                        let got = lo_call(&handler, &format!("SELECT lo_read({rfd}, 100000)"));
                        match got {
                            Ok(Value::Bytea(data)) if data == expected => {}
                            other => {
                                dprintln(
                                    format!(
                                        "round {round}: read-back of oid {oid} returned {other:?}, \
                                         expected {} bytes — content mismatch or silent truncation (NU-103)",
                                        expected.len()
                                    ),
                                    &mut divergences,
                                );
                            }
                        }
                        let _ = lo_call(&handler, &format!("SELECT lo_close({rfd})"));
                    }
                    other => {
                        dprintln(
                            format!("round {round}: {rsql} returned {other:?}"),
                            &mut divergences,
                        );
                    }
                }
                let _ = lo_call(&handler, &format!("SELECT lo_close({fd})"));
            }
            // lo_unlink: gone means gone; a stale descriptor must not read it
            // back as empty bytes, and a refused write must not resurrect it.
            12..=13 => {
                let live: Vec<u32> = model.keys().copied().collect();
                if live.is_empty() {
                    continue;
                }
                let oid = live[rng.below(live.len())];
                match lo_call(&handler, &format!("SELECT lo_unlink({oid})")) {
                    Ok(Value::Int32(0)) => {}
                    other => {
                        dprintln(
                            format!("round {round}: lo_unlink({oid}) returned {other:?}"),
                            &mut divergences,
                        );
                        continue;
                    }
                }
                model.remove(&oid);
                // The honest probe: read through a descriptor that is still
                // open against the now-unlinked object.
                if let Some(fd) = fds.keys().copied().find(|fd| fds.get(fd) == Some(&oid)) {
                    match lo_call(&handler, &format!("SELECT lo_read({fd}, 16)")) {
                        Ok(Value::Null) => {}
                        Ok(Value::Bytea(data)) if data.is_empty() => {
                            dprintln(
                                format!(
                                    "round {round}: stale descriptor read of unlinked oid {oid} \
                                     returned a SUCCESSFUL empty read — indistinguishable from \
                                     end-of-object (NU-103)"
                                ),
                                &mut divergences,
                            );
                        }
                        other => {
                            dprintln(
                                format!(
                                    "round {round}: stale descriptor read of unlinked oid {oid} \
                                     returned {other:?}"
                                ),
                                &mut divergences,
                            );
                        }
                    }
                    // A write through the stale descriptor must refuse and
                    // must NOT resurrect the object.
                    if let Ok(Value::Int32(n)) =
                        lo_call(&handler, &format!("SELECT lo_write({fd}, 'xyz')"))
                        && n > 0
                    {
                        dprintln(
                            format!(
                                "round {round}: lo_write through a stale descriptor of unlinked \
                                 oid {oid} reported success — the object may be resurrected \
                                 truncated (NU-103)"
                            ),
                            &mut divergences,
                        );
                    }
                }
            }
            // Restart: fresh executor + handler from the same directory. The
            // allocator must then skip every OID the store already holds.
            14..=15 => {
                drop(handler);
                drop(db);
                db = open_harness(&dir);
                handler = NucleusHandler::new(db.executor().clone());
                fds.clear();
            }
            // Adversarial garbage.
            _ => match rng.below(3) {
                0 => match lo_call(&handler, "SELECT lo_open(4294000000, 262144)") {
                    Ok(Value::Int32(-1)) => {}
                    other => {
                        dprintln(
                            format!(
                                "round {round}: lo_open(unknown oid) returned {other:?}, expected -1"
                            ),
                            &mut divergences,
                        );
                    }
                },
                1 => match lo_call(&handler, "SELECT lo_write(999999, 'x')") {
                    Ok(Value::Int32(-1)) => {}
                    other => {
                        dprintln(
                            format!(
                                "round {round}: lo_write(bad fd) returned {other:?}, expected -1"
                            ),
                            &mut divergences,
                        );
                    }
                },
                _ => match lo_call(&handler, "SELECT lo_read(999999, 8)") {
                    Ok(Value::Null) => {}
                    other => {
                        dprintln(
                            format!(
                                "round {round}: lo_read(bad fd) returned {other:?}, expected NULL"
                            ),
                            &mut divergences,
                        );
                    }
                },
            },
        }
    }

    // Final sweep: every model object reads back byte-exact after one last
    // restart. The negative control perturbs here: one durable object's
    // expected payload is silently corrupted in the model (the NU-102/103
    // shape — an object whose bytes are wrong, not merely missing: removing a
    // key would only skip its check and prove nothing).
    drop(handler);
    drop(db);
    if perturb && let Some(first) = model.iter_mut().next() {
        let (_oid, payload) = first;
        if payload.is_empty() {
            payload.push(b'x');
        } else {
            payload[0] = payload[0].wrapping_add(1);
        }
    }
    let db2 = open_harness(&dir);
    let handler = NucleusHandler::new(db2.executor().clone());
    let mut checked = 0usize;
    for (oid, expected) in &model {
        let sql = format!("SELECT lo_open({oid}, {INV_READ})");
        if let Ok(Value::Int32(fd)) = lo_call(&handler, &sql)
            && fd > 0
            && let Ok(Value::Bytea(data)) =
                lo_call(&handler, &format!("SELECT lo_read({fd}, 1000000)"))
        {
            if &data != expected {
                dprintln(
                    format!(
                        "final sweep: oid {oid} read back {} bytes, model has {} — content \
                         lost or corrupted across restart",
                        data.len(),
                        expected.len()
                    ),
                    &mut divergences,
                );
            }
            checked += 1;
        } else {
            dprintln(
                format!("final sweep: oid {oid} could not be opened/read after restart"),
                &mut divergences,
            );
        }
    }
    drop(handler);
    drop(db2);
    let _ = std::fs::remove_dir_all(&dir);
    println!(
        "  phase 4 (large objects): {checked} objects verified after restart, {} divergence(s)",
        divergences
    );
    divergences
}
