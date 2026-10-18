//! (Scratch file kept empty: this spike's demo lives in `examples/demo.rs`.)
use e0_btree::{BTree, MemStore};
use std::sync::Arc;
use std::thread;

fn main() {
    let writers = 8u64;
    let key_space = 24_000u64;
    let store = MemStore::new(8192).unwrap();
    let tree = Arc::new(BTree::new(store, 512).unwrap());
    let barrier = Arc::new(std::sync::Barrier::new(writers as usize));
    let mut handles = vec![];
    for t in 0..writers {
        let tree = Arc::clone(&tree);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut my: Vec<u64> = (0..key_space).filter(|k| k % writers == t).collect();
            // simple shuffle via xorshift
            let mut z = 0x1234_5678u64 + t;
            for i in (1..my.len()).rev() {
                z ^= z << 13; z ^= z >> 7; z ^= z << 17;
                let j = (z as usize) % (i + 1);
                my.swap(i, j);
            }
            barrier.wait();
            for (s, &k) in my.iter().enumerate() {
                let key = k.to_be_bytes();
                let mut v = key.to_vec();
                v.extend_from_slice(&s.to_be_bytes());
                v.extend_from_slice(&vec![(s % 64) as u8; (s % 64) as usize]);
                if let Err(e) = tree.insert(&key, &v) {
                    println!("w{t} ERR at {s}: {e}");
                    return;
                }
                if s % 500 == 0 {
                    println!("w{t} at {s}");
                }
            }
            println!("w{t} DONE");
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    let st = match tree.check() {
        Ok(s) => s,
        Err(m) => { println!("check ERR: {m}"); return; }
    };
    println!("h={} pages={} fill={:.3}", st.height, st.pages, st.fill_factor);
}
