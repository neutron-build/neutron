//! `concurrent.rs` (card X-0 Work 6): 8 writer threads over disjoint but
//! interleaved key ranges (keys ≡ t mod 8, so writers share leaves and
//! inner pages), each with its own model; 4 extra reader threads scanning
//! random ranges and asserting sortedness and the absence of torn values
//! (every value encodes its own key).
//!
//! At the end the union of the models equals the tree and `check()` is
//! clean. This file is the named catcher for the COUPLING-EARLY-RELEASE
//! mutation: releasing an ancestor before the child is safe lets two
//! writers interleave splits, misrouting keys into wrong subtrees (lost
//! from every scan) or producing separator violations that `check()` or
//! the union comparison trips over.

use e0_btree::{BTree, MemStore};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

const WRITERS: u64 = 8;
const READERS: usize = 4;
/// Keys are `k.to_be_bytes()` for `k < KEY_SPACE` with `k % WRITERS == t`.
const KEY_SPACE: u64 = 24_000;

/// Deterministic splitmix64.
pub struct Sm(u64);

impl Sm {
    /// New stream from a seed.
    pub fn new(seed: u64) -> Self {
        Sm(seed)
    }
    /// Next value in the stream.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// A value that encodes its own key: the first 8 bytes are the key, the
/// rest is writer-tagged filler. A torn or misrouted value fails the
/// reader's `value decodes to its key` assertion.
fn value_of(key: u64, writer: u64, seq: u64) -> Vec<u8> {
    let mut v = key.to_be_bytes().to_vec();
    v.push(writer as u8);
    v.extend_from_slice(&seq.to_be_bytes());
    v.extend_from_slice(&vec![(seq % 251) as u8; (seq % 64) as usize]);
    v
}

fn is_well_formed(key: &[u8], value: &[u8]) -> bool {
    value.len() >= 9 && value.get(..8) == Some(key)
}

#[test]
fn concurrent_writers_and_readers() {
    let store = MemStore::new(8192).unwrap();
    // The pool must cover threads x (height + 2) plus slack (PLAN-v3 §3.3
    // minimum pool size), or eviction would starve under the pin depth.
    let tree = Arc::new(BTree::new(store, 512).unwrap());
    let barrier = Arc::new(Barrier::new((WRITERS + READERS as u64) as usize));
    let stop = Arc::new(AtomicBool::new(false));
    type Models = Arc<Mutex<Vec<BTreeMap<Vec<u8>, Vec<u8>>>>>;
    let models: Models = Arc::new(Mutex::new((0..WRITERS).map(|_| BTreeMap::new()).collect()));

    let mut writer_handles = Vec::new();
    let mut reader_handles = Vec::new();

    // Writers: each owns the interleaved residue class `k % 8 == t`, so
    // neighbouring keys belong to different threads and every leaf and
    // inner page is shared. Phase 1 inserts all own keys (splits happen
    // here, concurrently); phase 2 mixes deletes, re-inserts and gets.
    for t in 0..WRITERS {
        let tree = Arc::clone(&tree);
        let barrier = Arc::clone(&barrier);
        let models = Arc::clone(&models);
        writer_handles.push(thread::spawn(move || {
            let mut rng = Sm::new(0xDEAD_0000 + t);
            let mut my_keys: Vec<u64> = (0..KEY_SPACE).filter(|k| k % WRITERS == t).collect();
            // insert in a per-thread random order
            for i in (1..my_keys.len()).rev() {
                let j = (rng.next_u64() as usize) % (i + 1);
                my_keys.swap(i, j);
            }
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            barrier.wait();

            for (seq, &k) in my_keys.iter().enumerate() {
                let key = k.to_be_bytes();
                let v = value_of(k, t, seq as u64);
                if std::env::var_os("E0_TRACE").is_some() && seq % 500 == 0 {
                    eprintln!("w{t} at {seq}");
                }
                let old = tree.insert(&key, &v).unwrap();
                let expected = model.insert(key.to_vec(), v.clone());
                assert_eq!(old, expected, "writer {t} insert {k}");
            }

            // mixed phase: delete a third, re-insert with a new value, and
            // re-read own keys; all against the own model.
            let mut seq = my_keys.len() as u64;
            for round in 0..3u64 {
                let mut round_keys = my_keys.clone();
                for i in (1..round_keys.len()).rev() {
                    let j = (rng.next_u64() as usize) % (i + 1);
                    round_keys.swap(i, j);
                }
                for (r2, k) in round_keys.iter().enumerate() {
                    if std::env::var_os("E0_TRACE").is_some() && r2 % 500 == 0 {
                        eprintln!("w{t} mixed r{round} at {r2}");
                    }
                    let key = k.to_be_bytes();
                    let pick = rng.next_u64() % 3;
                    if pick == 0 || round == 0 && k % 3 == 0 {
                        let old = tree.delete(&key).unwrap();
                        let expected = model.remove(key.as_slice());
                        assert_eq!(old, expected, "writer {t} delete {k}");
                    } else if pick == 1 {
                        seq += 1;
                        let v = value_of(*k, t, seq);
                        let old = tree.insert(&key, &v).unwrap();
                        let expected = model.insert(key.to_vec(), v.clone());
                        assert_eq!(old, expected, "writer {t} upsert {k}");
                    } else {
                        let got = tree.get(&key).unwrap();
                        let expected = model.get(key.as_slice()).cloned();
                        assert_eq!(got, expected, "writer {t} get {k}");
                    }
                }
            }
            models.lock().unwrap()[t as usize] = model;
        }));
    }

    // Readers: random windows; every scan must be sorted, every value must
    // decode to its key (no torn values), and no key may appear twice.
    for _ in 0..READERS {
        let tree = Arc::clone(&tree);
        let barrier = Arc::clone(&barrier);
        let stop = Arc::clone(&stop);
        reader_handles.push(thread::spawn(move || {
            let mut rng = Sm::new(0xBEEF_0000 + WRITERS);
            barrier.wait();
            let mut scans = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let lo = (rng.next_u64() % KEY_SPACE).to_be_bytes();
                let width = 1 + rng.next_u64() % 1_000;
                let hi = ((rng.next_u64() % KEY_SPACE) + width).to_be_bytes();
                let scan: Vec<(Vec<u8>, Vec<u8>)> =
                    match tree.range(&lo, &hi).unwrap().collect::<Result<_, _>>() {
                        Ok(v) => v,
                        Err(e) => panic!("reader scan failed: {e}"),
                    };
                for w in scan.windows(2) {
                    assert!(w[0].0 < w[1].0, "reader saw unsorted keys");
                }
                for (k, v) in scan.iter() {
                    assert!(
                        is_well_formed(k, v),
                        "torn value: key {k:02x?} value {:02x?}",
                        v.get(..16).map(|s| s.to_vec()).unwrap_or_default()
                    );
                }
                scans += 1;
                // A scan holds shared latches on its whole cursor stack; a
                // reader convoy would starve every writer on the root
                // latch (std's RwLock favours readers), so yield the CPUs
                // between scans to let writers interleave.
                std::thread::sleep(std::time::Duration::from_micros(300));
            }
            let _ = scans;
        }));
    }

    for h in writer_handles {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Release);
    for h in reader_handles {
        h.join().unwrap();
    }

    // The union of the writer models equals the tree, scanned in full.
    let mut union: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for m in models.lock().unwrap().drain(..) {
        for (k, v) in m {
            let prev = union.insert(k, v);
            assert!(prev.is_none(), "writer key spaces must be disjoint");
        }
    }
    let scan: Vec<(Vec<u8>, Vec<u8>)> = tree
        .range(&[0u8], &[0xffu8; 9])
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let want: Vec<(Vec<u8>, Vec<u8>)> = union.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(scan.len(), want.len(), "union size");
    assert_eq!(scan, want, "tree equals the union of the writer models");

    let stats = tree.check().unwrap();
    assert!(stats.height >= 2, "{stats:?}");
}
