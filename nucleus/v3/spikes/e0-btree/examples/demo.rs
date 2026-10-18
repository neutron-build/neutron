//! Minimal usage demo for `e0_btree` (handy when poking at the spike).
#![allow(missing_docs)]

use e0_btree::{BTree, MemStore};

fn main() {
    let store = MemStore::new(8192).expect("8 KiB is a valid page size");
    let tree = BTree::new(store, 64).expect("pool of 64 frames is valid");
    for i in 0u64..10_000 {
        let key = i.to_be_bytes();
        let value = format!("value-{i}").into_bytes();
        tree.insert(&key, &value).expect("insert cannot fail here");
    }
    let stats = tree.check().expect("the demo tree is well formed");
    println!(
        "height={} pages={} fill={:.3}",
        stats.height, stats.pages, stats.fill_factor
    );
    let mut n = 0usize;
    let iter = tree
        .range(&5_000u64.to_be_bytes(), &5_100u64.to_be_bytes())
        .expect("range cannot fail here");
    for entry in iter {
        entry.expect("scan cannot fail here");
        n += 1;
    }
    println!("range(5000..5100) returned {n} keys");
}
