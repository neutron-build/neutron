//! `file.rs` (card X-0 Work 6): the `big.rs` workload over a `FileStore`
//! in a temp dir under `std::env::temp_dir()`, capacity 64; then
//! `flush_all()`, a reopen through a fresh pool, and the scan must be
//! equal.

use e0_btree::{BTree, FileStore, PageId};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

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

const INSERTS: usize = 200_000;
const DELETES: usize = 50_000;
const CAPACITY: usize = 64;

fn temp_page_file() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("e0-btree-file-{}-{nanos}.pgs", std::process::id()))
}

#[test]
fn file_store_reopen_is_equal() {
    let path = temp_page_file();
    let mut rng = Sm::new(0xF1E0_0001);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let keys: Vec<[u8; 8]> = (0..INSERTS).map(|_| rng.next_u64().to_be_bytes()).collect();

    let root;
    {
        let store = FileStore::open(&path, 8192).unwrap();
        let tree = BTree::new(store, CAPACITY).unwrap();
        for k in keys.iter() {
            let vlen = (rng.next_u64() % 301) as usize;
            let v: Vec<u8> = (0..vlen).map(|_| rng.next_u64() as u8).collect();
            let old = tree.insert(k, &v).unwrap();
            let expected = model.insert(k.to_vec(), v.clone());
            assert_eq!(old, expected);
        }
        for _ in 0..DELETES {
            let k = keys[(rng.next_u64() as usize) % INSERTS];
            let old = tree.delete(&k).unwrap();
            let expected = model.remove(k.as_slice());
            assert_eq!(old, expected);
        }
        let scan: Vec<(Vec<u8>, Vec<u8>)> = tree
            .range(&[0u8], &[0xffu8; 9])
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let want: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(scan, want);
        tree.check().unwrap();

        tree.flush_all().unwrap();
        root = tree.root_page().unwrap();
        drop(tree);
    }

    // Reopen through a fresh pool: every page must come back from the file.
    {
        let store = FileStore::open(&path, 8192).unwrap();
        let tree = BTree::open(store, CAPACITY, PageId(root.0)).unwrap();
        let scan: Vec<(Vec<u8>, Vec<u8>)> = tree
            .range(&[0u8], &[0xffu8; 9])
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let want: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(scan.len(), want.len());
        assert_eq!(scan, want, "reopened scan equals the model");
        for i in (0..INSERTS).step_by(1_009) {
            let k = keys[i];
            assert_eq!(tree.get(&k).unwrap(), model.get(k.as_slice()).cloned());
        }
        tree.check().unwrap();
    }

    // Best-effort cleanup of the scratch page file.
    let _ = std::fs::remove_file(&path);
}
