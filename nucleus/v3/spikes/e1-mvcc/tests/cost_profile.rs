//! Cost profile of the shared components (card X-1a-F1 Work 7): the
//! three measurements the X-1a verdict's F1 follow-up named, printed for
//! the RESULT block. One `#[ignore]`d test — it is a measurement, not an
//! assertion, and takes tens of seconds:
//!
//! ```text
//! cargo test --release -p e1-mvcc --test cost_profile -- --ignored --nocapture
//! ```
//!
//! It uses **only the pre-card public API** (`LockTable::{new,
//! with_escalate_at, acquire, release_all, escalations}`,
//! `commit_with`), so the identical file runs on the base branch
//! (`nucleus/v3-scaffold`) and on the card tip, and the two outputs are
//! comparable. Nothing here asserts a threshold; the deterministic
//! complexity bounds live in `tests/costs.rs`.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use e1_mvcc::api::Wait;
use e1_mvcc::common::{LockTable, Sequencer, commit_with};

/// 8-byte big-endian key of counter `i` (dense, allocation-light).
fn k(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

/// The card X-1a-F1 cost profile (see the module docs). Ignored: it is
/// a measurement whose output a human reads, not a pass/fail test.
#[test]
#[ignore]
fn cost_profile() {
    println!(
        "e1-mvcc shared-component cost profile (card X-1a-F1 Work 7); arch = {}, \
         available_parallelism = {}",
        std::env::consts::ARCH,
        thread::available_parallelism().map_or(0, |n| n.get()),
    );

    // (a) Mean `release_all` time of a 1-row txn while another txn
    // holds 1M rows. Before X-1a-F1 this walked every shard of a
    // 1M-entry table per release; the card makes it O(own keys).
    {
        const A_ROWS: u64 = 1_000_000;
        const SAMPLES: u64 = 2_000;
        let lt = LockTable::new();
        for i in 0..A_ROWS {
            lt.acquire(&k(i), 1, Wait::NoWait).unwrap();
        }
        let b = b"b-single-row";
        let mut total = Duration::ZERO;
        for _ in 0..SAMPLES {
            lt.acquire(b, 2, Wait::NoWait).unwrap();
            let t0 = Instant::now();
            lt.release_all(2);
            total += t0.elapsed();
        }
        lt.release_all(1);
        let mean = total / SAMPLES as u32;
        println!(
            "(a) release_all of a 1-row txn while another txn holds {A_ROWS} rows: \
             {SAMPLES} samples, mean = {} ns ({mean:.3?})",
            mean.as_nanos(),
        );
    }

    // (b) Total time for one txn to acquire 2M rows with
    // escalate_at = 1M while a second thread holds and cycles one
    // foreign row (the W6 head-of-line shape). Reports the elapsed time
    // and `escalations()`: whether the big txn ever replaced its rows
    // with the table-wide owner.
    {
        const A_ROWS: u64 = 2_000_000;
        let lt = LockTable::with_escalate_at(1_000_000);
        let stop = AtomicBool::new(false);
        let acquired = AtomicU64::new(0);
        thread::scope(|s| {
            let lt_f = &lt;
            let stop = &stop;
            s.spawn(move || {
                let f = b"foreign-row";
                while !stop.load(Ordering::Relaxed) {
                    if lt_f.acquire(f, 42, Wait::NoWait).is_ok() {
                        lt_f.release_all(42);
                    } else {
                        thread::yield_now();
                    }
                }
            });
            let t0 = Instant::now();
            for i in 0..A_ROWS {
                lt.acquire(black_box(&k(i)), 1, Wait::NoWait).unwrap();
                acquired.fetch_add(1, Ordering::Relaxed);
            }
            let elapsed = t0.elapsed();
            stop.store(true, Ordering::Relaxed);
            let n = acquired.load(Ordering::Relaxed);
            println!(
                "(b) {n} acquires by one txn, escalate_at = 1_000_000, one foreign \
                 row cycling: elapsed = {elapsed:.3?}, {:.0} acquires/s, escalations = {}",
                n as f64 / elapsed.as_secs_f64(),
                lt.escalations(),
            );
        });
    }

    // (c) `acquire` + `release_all` pairs per second for disjoint keys
    // at 1, 4, 8 and 16 threads (each thread owns one txn and one key).
    {
        const THREAD_COUNTS: &[usize] = &[1, 4, 8, 16];
        const PER_THREAD: u64 = 200_000;
        println!("(c) acquire + release_all pairs, disjoint keys:");
        for threads in THREAD_COUNTS {
            let lt = LockTable::new();
            let pairs = AtomicU64::new(0);
            let t0 = Instant::now();
            thread::scope(|s| {
                for t in 0..*threads {
                    let lt = &lt;
                    let pairs = &pairs;
                    s.spawn(move || {
                        let key = k(1_000_000 + t as u64);
                        for _ in 0..PER_THREAD {
                            if lt.acquire(&key, 10 + t as u64, Wait::NoWait).is_ok() {
                                lt.release_all(10 + t as u64);
                                pairs.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    });
                }
            });
            let elapsed = t0.elapsed();
            let n = pairs.load(Ordering::Relaxed);
            println!(
                "    threads = {threads:2}: {n} pairs in {elapsed:.3?} = {:.0} pairs/s",
                n as f64 / elapsed.as_secs_f64(),
            );
        }
    }

    // (d) The commit path the apply gate sits on: one unarmed commit
    // costs the gate hook plus a release_all of nothing.
    {
        const COMMITS: u64 = 1_000_000;
        let seq = Sequencer::new();
        let locks = LockTable::new();
        let poison = AtomicBool::new(false);
        let t0 = Instant::now();
        for i in 0..COMMITS {
            let ts = commit_with(&seq, &locks, &poison, 7, |ts| {
                black_box(ts);
                Ok(())
            })
            .unwrap();
            black_box(ts + i);
        }
        let elapsed = t0.elapsed();
        println!(
            "(d) {COMMITS} empty commit_with calls (unarmed apply gate): \
             elapsed = {elapsed:.3?}, {:.0} commits/s",
            COMMITS as f64 / elapsed.as_secs_f64(),
        );
    }
}
