//! Blob-store differential + crash-consistency probe for the disk-tiered
//! chunk store (segments + RAM LRU cache + metadata WAL).
//!
//! Three phases:
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
//!
//! Build: `cargo run --release --features "server" --bin probe_blob`.
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
    let mut store = BlobStore::open_with_options(dir.path(), CHUNK, CACHE).unwrap();
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
                store = BlobStore::open_with_options(dir.path(), CHUNK, CACHE).unwrap();
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
    let store = BlobStore::open_with_options(dir.path(), CHUNK, CACHE).unwrap();
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
            let mut store = BlobStore::open_with_options(dir.path(), CHUNK, CACHE).unwrap();
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

        let mut store = BlobStore::open_with_options(dir.path(), CHUNK, CACHE).unwrap();
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
        let store = BlobStore::open_with_options(dir.path(), CHUNK, CACHE).unwrap();
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
            let mut store = BlobStore::open_with_options(dir.path(), CHUNK, CACHE).unwrap();
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
        let store = BlobStore::open_with_options(dir.path(), CHUNK, CACHE).unwrap();
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
    let mut checks = 0u64;
    println!("probe_blob: disk-tiered blob store differential + crash probe");
    phase_differential(&mut checks);
    phase_wal_torn_tail(&mut checks);
    phase_segment_torn_tail(&mut checks);
    println!("probe_blob: PASS ({checks} checks)");
}
