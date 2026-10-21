//! Cost profile of the shared components (card X-1a-F1 Work 7): the
//! three measurements the X-1a verdict's F1 follow-up named, printed for
//! the RESULT block. One `#[ignore]`d test — it is a measurement, not an
//! assertion, and takes tens of seconds:
//!
//! ```text
//! cargo test --release -p e1-mvcc --test cost_profile -- --ignored --nocapture
//! ```
//!
//! It uses the X-1a-F1 public API (`LockTable::{new, with_escalate_at,
//! acquire, release_all, escalations, escalation_attempts,
//! escalation_retry_step}`, `commit_with` — `escalation_attempts` and
//! `escalation_retry_step` are on the scaffold since X-1a-F1), so the
//! identical file still runs on base and tip, and the two outputs are
//! comparable. Nothing here asserts a threshold; (b)'s counts are
//! asserted because they are deterministic; the deterministic
//! complexity bounds live in `tests/costs.rs`.
//!
//! Card X-1a-F1-F2 Work 6 adds the **(c′)** variant after (c): (c)
//! again, but with each thread's key **and** txn pre-selected so the 16
//! threads land on 16 distinct key shards **and** 16 distinct index
//! shards (probed through the `#[doc(hidden)]`
//! `LockTable::{shard_of_key, shard_of_txn}` helpers, which exist only
//! on the tip). (c)'s fixed keys collide on key shards often — with 64
//! shards, P(no collision among 16) ≈ 0.15 — so (c) mixes whatever
//! global per-acquire costs remain with shard collisions, while (c′)
//! isolates the global ones. (c′) is tip-only; the (a), (b1/b2), (c)
//! and (d) blocks above it are byte-for-byte the scaffold's, so base
//! and tip outputs stay comparable.

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

    // (b) Escalation behaviour of one txn acquiring 2M rows with
    // escalate_at = 1M, as two **fixed** variants (card X-1a-F1-F3
    // Work 3). The pre-F3 cycling foreign row made whether the big txn
    // escalates depend on whether the row happened to be free at an
    // attempt instant — one sample of a race. Both variants are
    // deterministic, so their counts are asserted: they are properties
    // of L-ESCALATE-DEFER, not timings.
    {
        const B_ROWS: u64 = 2_000_000;
        const ESCALATE_AT: u64 = 1_000_000;
        // (b1) A foreign row held for the whole run: txn 42 acquires it
        // before txn 1's first acquire and releases it after the last,
        // so every escalation attempt defers (escalation never waits).
        {
            let lt = LockTable::with_escalate_at(ESCALATE_AT as usize);
            lt.acquire(b"foreign-row", 42, Wait::NoWait).unwrap();
            let t0 = Instant::now();
            for i in 0..B_ROWS {
                lt.acquire(black_box(&k(i)), 1, Wait::NoWait).unwrap();
            }
            let elapsed = t0.elapsed();
            lt.release_all(42);
            // The L-ESCALATE-DEFER retry rule, computed (not
            // hard-coded) from the table: the first attempt is at
            // own-count `ESCALATE_AT + 1`, and each failed attempt
            // defers the next by one retry step, so attempts land at
            // `ESCALATE_AT + 1 + j·step` while that is `<= B_ROWS`.
            let step = lt.escalation_retry_step() as u64;
            let expected_attempts = 1 + (B_ROWS - (ESCALATE_AT + 1)) / step;
            println!(
                "(b1) {B_ROWS} acquires by one txn, escalate_at = {ESCALATE_AT}, one \
                 foreign row held for the whole run: elapsed = {elapsed:.3?}, {:.0} \
                 acquires/s, escalations = {}, attempts = {} (retry rule: 1 + \
                 ({B_ROWS} - {}) / {step} = {expected_attempts})",
                B_ROWS as f64 / elapsed.as_secs_f64(),
                lt.escalations(),
                lt.escalation_attempts(),
                ESCALATE_AT + 1,
            );
            assert_eq!(
                lt.escalations(),
                0,
                "(b1): a foreign row held across every attempt must defer them all"
            );
            assert_eq!(
                lt.escalation_attempts(),
                expected_attempts,
                "(b1): attempts follow the L-ESCALATE-DEFER retry rule"
            );
            lt.release_all(1);
        }
        // (b2) No foreign holder: the first attempt escalates, and the
        // remaining acquires are no-ops under the table owner.
        {
            let lt = LockTable::with_escalate_at(ESCALATE_AT as usize);
            let t0 = Instant::now();
            for i in 0..B_ROWS {
                lt.acquire(black_box(&k(i)), 1, Wait::NoWait).unwrap();
            }
            let elapsed = t0.elapsed();
            println!(
                "(b2) {B_ROWS} acquires by one txn, escalate_at = {ESCALATE_AT}, no \
                 foreign holder: elapsed = {elapsed:.3?}, {:.0} acquires/s, escalations \
                 = {}, attempts = {}",
                B_ROWS as f64 / elapsed.as_secs_f64(),
                lt.escalations(),
                lt.escalation_attempts(),
            );
            assert_eq!(
                lt.escalations(),
                1,
                "(b2): with no foreign row the first attempt escalates"
            );
            assert_eq!(
                lt.escalation_attempts(),
                1,
                "(b2): after the escalation every acquire is a no-op at step (1)"
            );
            lt.release_all(1);
        }
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

    // (c′) Card X-1a-F1-F2 Work 6: (c) with each thread's key and txn
    // pre-selected so the 16 threads land on 16 distinct key shards
    // and 16 distinct index shards, probed through the tip-only
    // `#[doc(hidden)]` helpers. This separates the global per-acquire
    // costs this card removes from shard collisions (module docs).
    {
        const THREAD_COUNTS: &[usize] = &[1, 4, 8, 16];
        const PER_THREAD: u64 = 200_000;
        const CP_THREADS: usize = 16;
        // 16 keys in 16 distinct key shards ...
        let mut keys: Vec<u64> = Vec::with_capacity(CP_THREADS);
        let mut key_shards = std::collections::BTreeSet::new();
        let mut j = 2_000_000u64;
        while keys.len() < CP_THREADS {
            if key_shards.insert(LockTable::shard_of_key(&k(j))) {
                keys.push(j);
            }
            j += 1;
        }
        // ... and 16 txn ids in 16 distinct index shards, counting up
        // from the same base (c) numbers its txns from.
        let mut txns: Vec<u64> = Vec::with_capacity(CP_THREADS);
        let mut idx_shards = std::collections::BTreeSet::new();
        let mut t = 10u64;
        while txns.len() < CP_THREADS {
            if idx_shards.insert(LockTable::shard_of_txn(t)) {
                txns.push(t);
            }
            t += 1;
        }
        println!(
            "(c′) acquire + release_all pairs, disjoint keys in 16 distinct \
             key shards and 16 distinct index shards (key shards \
             {key_shards:?}, index shards {idx_shards:?}):"
        );
        for threads in THREAD_COUNTS {
            let lt = LockTable::new();
            let pairs = AtomicU64::new(0);
            let t0 = Instant::now();
            thread::scope(|s| {
                for t in 0..*threads {
                    let lt = &lt;
                    let pairs = &pairs;
                    let key = k(keys[t]);
                    let txn = txns[t];
                    s.spawn(move || {
                        for _ in 0..PER_THREAD {
                            if lt.acquire(&key, txn, Wait::NoWait).is_ok() {
                                lt.release_all(txn);
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
