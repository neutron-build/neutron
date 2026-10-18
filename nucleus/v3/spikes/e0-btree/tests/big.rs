//! `big.rs` (card X-0 Work 6): 200k random inserts, 50k deletes, a
//! full-range scan equal to the model, then `check()`.
//!
//! Single-threaded over `MemStore` with a 64-frame pool, so the tree
//! (thousands of pages) constantly evicts — the dirty write-back path runs
//! on nearly every descent.

use e0_btree::{BTree, MemStore};
use std::collections::BTreeMap;

/// Deterministic splitmix64 (no external RNG dependency).
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
    /// A value of 0–300 bytes for the workload.
    pub fn value(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }
}

const INSERTS: usize = 200_000;
const DELETES: usize = 50_000;
const CAPACITY: usize = 64;

#[test]
fn big_insert_delete_scan() {
    let mut rng = Sm::new(0xE0_E0_E0_E0_E0_E0_E0_E0);
    let store = MemStore::new(8192).unwrap();
    let tree = BTree::new(store, CAPACITY).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // 200k random 8-byte keys with random 0–300 byte values, in random
    // order (the splitmix stream is shuffled by construction).
    let keys: Vec<[u8; 8]> = (0..INSERTS).map(|_| rng.next_u64().to_be_bytes()).collect();
    for k in keys.iter() {
        let vlen = (rng.next_u64() % 301) as usize;
        let v = rng.value(vlen);
        let old = tree.insert(k, &v).unwrap();
        let expected = model.insert(k.to_vec(), v);
        assert_eq!(old, expected);
    }
    assert_eq!(model.len(), INSERTS, "keys must be unique");

    // 50k deletes of randomly chosen inserted keys.
    for _ in 0..DELETES {
        let k = keys[(rng.next_u64() as usize) % INSERTS];
        let old = tree.delete(&k).unwrap();
        let expected = model.remove(k.as_slice());
        assert_eq!(old, expected);
    }

    // Full-range scan equal to the model.
    let scan: Vec<(Vec<u8>, Vec<u8>)> = tree
        .range(&[0u8], &[0xffu8; 9])
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(scan.len(), want.len());
    assert_eq!(scan, want);

    // Spot point lookups on both survivors and deleted keys.
    for (i, k) in keys.iter().enumerate() {
        if i % 997 != 0 {
            continue;
        }
        assert_eq!(
            tree.get(k).unwrap(),
            model.get(k.as_slice()).cloned(),
            "key {k:?}"
        );
    }

    let stats = tree.check().unwrap();
    assert!(stats.height >= 3, "{stats:?}");
    assert!(stats.pages > 2_000, "{stats:?}");
    assert!(stats.fill_factor < 1.0, "{stats:?}");
}
