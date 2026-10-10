//! C-T7 concurrency (card Work 4): multiple threads calling `seq_next`
//! get distinct values; a block reservation happens at most once per
//! block under the seq mutex; the reservation's sync runs inside it (the
//! module docs in `src/seq.rs` justify the choice — two threads syncing
//! the same `H` concurrently would hand out overlapping values).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::seq::BLOCK;

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

const THREADS: usize = 8;
const IDS: usize = 4;
const CALLS: usize = 10_000;

/// Drives `THREADS` threads over `IDS` sequence ids (each thread touches
/// every id round-robin), `CALLS` calls per thread, then checks per id:
/// every value distinct, the values gap-free `1..=n` (no crash, no skips
/// allowed within one run), and exactly one reservation per block.
fn run(block: Option<u64>, calls: usize) {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    if let Some(b) = block {
        core.seq_set_block_for_tests(b);
    }
    let block = block.unwrap_or(BLOCK);

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let core = Arc::clone(&core);
            thread::spawn(move || {
                let mut got: Vec<Vec<i64>> = vec![Vec::new(); IDS];
                for i in 0..calls {
                    let id = (t + i) % IDS;
                    got[id].push(ok(core.seq_next(id as u64)));
                }
                got
            })
        })
        .collect();
    let mut per_id: Vec<BTreeSet<i64>> = vec![BTreeSet::new(); IDS];
    for h in handles {
        for (id, vals) in h.join().expect("worker thread").into_iter().enumerate() {
            per_id[id].extend(vals);
        }
    }

    let n = (THREADS * calls / IDS) as i64;
    for (id, set) in per_id.iter().enumerate() {
        assert_eq!(
            set.len(),
            n as usize,
            "id {id}: {} distinct values, expected {n} — duplicates were handed out",
            set.len()
        );
        let sorted: Vec<i64> = set.iter().copied().collect();
        assert_eq!(
            sorted,
            (1..=n).collect::<Vec<_>>(),
            "id {id}: values are not the gap-free range 1..={n}"
        );
        let reservations = core.seq_reservations_for_tests(id as u64).expect("id used");
        assert_eq!(
            reservations,
            (n as u64).div_ceil(block),
            "id {id}: one reservation per block, no more"
        );
    }
}

#[test]
fn eight_threads_four_ids_ten_k_values_distinct_small_block() {
    // Block 2: a reservation on every other value — maximum contention on
    // the reservation path (and on the synced write inside the mutex).
    run(Some(2), CALLS);
}

#[test]
fn eight_threads_four_ids_distinct_default_block() {
    // The production block: reservations are rare, the fast path is pure
    // memory under the mutex.
    run(None, 2_000);
}

#[test]
fn eight_threads_one_id_all_distinct() {
    // Every thread hammers the same id: the mutex must serialise the
    // cursor completely.
    let core = Arc::new(ok(Core::open(MemKv::new())));
    core.seq_set_block_for_tests(4);
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let core = Arc::clone(&core);
            thread::spawn(move || (0..1_000).map(|_| ok(core.seq_next(0))).collect::<Vec<_>>())
        })
        .collect();
    let mut all = BTreeSet::new();
    for h in handles {
        for v in h.join().expect("worker thread") {
            assert!(all.insert(v), "value {v} handed out twice");
        }
    }
    assert_eq!(all.len(), THREADS * 1_000);
    assert_eq!(
        all.iter().copied().collect::<Vec<_>>(),
        (1..=(THREADS * 1_000) as i64).collect::<Vec<_>>()
    );
    assert_eq!(
        core.seq_reservations_for_tests(0).expect("id used"),
        (THREADS * 1_000).div_ceil(4) as u64
    );
}
