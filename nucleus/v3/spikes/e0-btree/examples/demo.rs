//! Minimal usage demo for `e0_btree` (handy when poking at the spike).
#![allow(missing_docs)]

use e0_btree::{BTree, MemStore};

fn main() {
    let store = match MemStore::new(8192) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("demo: {e}");
            std::process::exit(1);
        }
    };
    let tree = match BTree::new(store, 64) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("demo: {e}");
            std::process::exit(1);
        }
    };
    for i in 0u64..10_000 {
        let key = i.to_be_bytes();
        let value = format!("value-{i}").into_bytes();
        if let Err(e) = tree.insert(&key, &value) {
            eprintln!("demo: {e}");
            std::process::exit(1);
        }
    }
    let stats = match tree.check() {
        Ok(s) => s,
        Err(m) => {
            eprintln!("demo: {m}");
            std::process::exit(1);
        }
    };
    println!(
        "height={} pages={} fill={:.3}",
        stats.height, stats.pages, stats.fill_factor
    );
    let mut n = 0usize;
    let iter = match tree.range(&5_000u64.to_be_bytes(), &5_100u64.to_be_bytes()) {
        Ok(it) => it,
        Err(e) => {
            eprintln!("demo: {e}");
            std::process::exit(1);
        }
    };
    for entry in iter {
        if let Err(e) = entry {
            eprintln!("demo: {e}");
            std::process::exit(1);
        }
        n += 1;
    }
    println!("range(5000..5100) returned {n} keys");
}
