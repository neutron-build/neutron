//! Score benchmarks (C-S1 work item 6), release build. Numbers are printed
//! for `DECISION.md`; assertions are sanity-only (a benchmark is not a
//! correctness gate). Tests serialise on one lock so they do not contend.
//!
//! Not tested here (see DECISION.md): static linux musl cross-compile
//! (`x86_64-unknown-linux-musl` target not installed on this machine),
//! release binary size (measured from the `tiny-rocks`/`tiny-fjall` bins),
//! bus factor (crate metadata / repository).

#![allow(clippy::unwrap_used)] // tests may unwrap (rule: not outside tests)

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use nucleus_kv::{Batch, Durability, KvError, OrderedKv, Snapshot};

fn ok<T>(r: Result<T, KvError>, what: &str) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("{what}: {e}"),
    }
}

fn score_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Resident set size of this process in KiB, via `ps`.
fn rss_kib() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .trim()
            .parse()
            .unwrap_or(0),
        _ => 0,
    }
}

fn percentile(mut samples: Vec<Duration>, p: f64) -> Duration {
    samples.sort();
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((p / 100.0) * (samples.len() - 1) as f64).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

// ---- group commit throughput: N writers, every write a 1-key batch with
// Durability::Yes. ----

fn score_group_commit<K: OrderedKv>(kv: &K, backend: &str, threads: usize, total: usize) {
    let per_thread = total / threads;
    let start = Instant::now();
    std::thread::scope(|s| {
        for t in 0..threads {
            s.spawn(move || {
                for i in 0..per_thread {
                    let mut b = Batch::default();
                    b.put(
                        format!("gc/{t:03}/{i:07}").into_bytes(),
                        (i as u32).to_be_bytes().to_vec(),
                    );
                    ok(kv.write(b, Durability::Yes), "synced write");
                }
            });
        }
    });
    let el = start.elapsed();
    println!(
        "SCORE group_commit {backend} threads={threads}: {total} writes in {:.3}s = {:.0} ops/s",
        el.as_secs_f64(),
        total as f64 / el.as_secs_f64()
    );
}

// ---- write latency while a background thread forces compaction. ----

fn score_latency_under_compaction<K: OrderedKv>(
    kv: &K,
    backend: &str,
    preload: usize,
    writes: usize,
    compact: impl Fn(&K) + Send + Sync,
    settle: impl Fn(&K),
) {
    for i in 0..preload {
        let mut b = Batch::default();
        b.put(
            format!("lat/{i:07}").into_bytes(),
            vec![u8::try_from(i % 256).unwrap(); 100],
        );
        ok(kv.write(b, Durability::No), "preload write");
    }
    settle(kv);

    let stop = Arc::new(AtomicBool::new(false));
    let compactions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|s| {
        s.spawn({
            let stop = Arc::clone(&stop);
            let compactions = Arc::clone(&compactions);
            move || {
                while !stop.load(AtomicOrdering::Relaxed) {
                    compact(kv);
                    compactions.fetch_add(1, AtomicOrdering::Relaxed);
                }
            }
        });
        let mut lat: Vec<Duration> = Vec::with_capacity(writes);
        for i in 0..writes {
            let mut b = Batch::default();
            b.put(format!("latw/{i:07}").into_bytes(), vec![7u8; 100]);
            let t0 = Instant::now();
            ok(kv.write(b, Durability::Yes), "measured write");
            lat.push(t0.elapsed());
        }
        stop.store(true, AtomicOrdering::SeqCst);
        let p50 = percentile(lat.clone(), 50.0);
        let p99 = percentile(lat.clone(), 99.0);
        let p999 = percentile(lat, 99.9);
        println!(
            "SCORE latency_under_compaction {backend}: p50={p50:?} p99={p99:?} p999={p999:?} over {writes} synced writes, {} compactions",
            compactions.load(AtomicOrdering::Relaxed)
        );
        assert!(p999 < Duration::from_secs(30), "pathological p999");
    });
}

// ---- peak RSS for a 1M-key load. ----

const LOAD_KEYS: usize = 1_000_000;

fn score_load_rss<K: OrderedKv>(kv: &K, backend: &str, settle: impl Fn(&K)) {
    let base = rss_kib();
    let start = Instant::now();
    let mut b = Batch::default();
    for i in 0..LOAD_KEYS {
        b.put(
            format!("load/{i:08}").into_bytes(),
            vec![u8::try_from(i % 256).unwrap(); 100],
        );
        if b.ops.len() == 1000 {
            let take = std::mem::take(&mut b);
            ok(kv.write(take, Durability::No), "load batch");
        }
    }
    if !b.is_empty() {
        ok(kv.write(b, Durability::No), "load tail");
    }
    ok(kv.sync_wal(), "sync after load");
    settle(kv);
    let el = start.elapsed();
    let peak = rss_kib();
    println!(
        "SCORE load_rss {backend}: {LOAD_KEYS} keys in {:.1}s, rss before={} KiB after={} KiB delta={} MiB",
        el.as_secs_f64(),
        base,
        peak,
        peak.saturating_sub(base) / 1024
    );
    assert!(peak > base, "rss did not grow during load");
}

// ---- long-iterator cost: a full scan of 1M keys while 100k writes land. ----

fn score_long_iterator<K: OrderedKv>(kv: &K, backend: &str, settle: impl Fn(&K)) {
    let mut b = Batch::default();
    for i in 0..LOAD_KEYS {
        b.put(
            format!("scan/{i:08}").into_bytes(),
            vec![u8::try_from(i % 256).unwrap(); 100],
        );
        if b.ops.len() == 1000 {
            let take = std::mem::take(&mut b);
            ok(kv.write(take, Durability::No), "scan preload");
        }
    }
    if !b.is_empty() {
        ok(kv.write(b, Durability::No), "preload tail");
    }
    settle(kv);

    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|s| {
        s.spawn({
            let stop = Arc::clone(&stop);
            let written = Arc::clone(&written);
            move || {
                // 100k writes land while the scan runs.
                for i in 0..100 {
                    if stop.load(AtomicOrdering::Relaxed) {
                        break;
                    }
                    let mut wb = Batch::default();
                    for j in 0..1000 {
                        wb.put(
                            format!("concurrent/{i:03}/{j:05}").into_bytes(),
                            vec![9u8; 100],
                        );
                    }
                    ok(kv.write(wb, Durability::No), "concurrent write");
                    written.fetch_add(1000, AtomicOrdering::Relaxed);
                }
            }
        });

        let t_snap = Instant::now();
        let snap = kv.snapshot();
        let t_snap = t_snap.elapsed();
        let t0 = Instant::now();
        let mut n = 0usize;
        for r in snap.scan((Bound::Unbounded, Bound::Unbounded), false) {
            let _ = ok(r, "scan item");
            n += 1;
        }
        let scan = t0.elapsed();
        stop.store(true, AtomicOrdering::SeqCst);
        assert_eq!(n, LOAD_KEYS, "scan lost or gained rows");
        println!(
            "SCORE long_iterator {backend}: snapshot_open={t_snap:?} full_scan={scan:?} ({} rows, {:.0} rows/s), {} concurrent writes landed",
            n,
            n as f64 / scan.as_secs_f64().max(f64::MIN_POSITIVE),
            written.load(AtomicOrdering::Relaxed)
        );
    });
}

// ---- per-backend wiring ----

macro_rules! score_tests {
    ($mod_name:ident, $backend:literal, $open:path, $compact:expr, $settle:expr, $writes:literal) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn group_commit() {
                let _l = score_lock();
                let (dir, _guard) = nucleus_bakeoff::store_dir(concat!($backend, "-gc"));
                let kv = ok($open(&dir), "open");
                for threads in [1usize, 8, 64] {
                    score_group_commit(&kv, $backend, threads, 3000);
                }
            }

            #[test]
            fn latency_under_compaction() {
                let _l = score_lock();
                let (dir, _guard) = nucleus_bakeoff::store_dir(concat!($backend, "-lat"));
                let kv = ok($open(&dir), "open");
                score_latency_under_compaction(&kv, $backend, 200_000, $writes, $compact, $settle);
            }

            #[test]
            fn load_rss() {
                let _l = score_lock();
                let (dir, _guard) = nucleus_bakeoff::store_dir(concat!($backend, "-rss"));
                let kv = ok($open(&dir), "open");
                score_load_rss(&kv, $backend, $settle);
            }

            #[test]
            fn long_iterator() {
                let _l = score_lock();
                let (dir, _guard) = nucleus_bakeoff::store_dir(concat!($backend, "-scan"));
                let kv = ok($open(&dir), "open");
                score_long_iterator(&kv, $backend, $settle);
            }
        }
    };
}

#[cfg(feature = "rocks")]
score_tests!(
    rocks_scores,
    "rocks",
    nucleus_bakeoff::rocks::RocksKv::open,
    |kv: &nucleus_bakeoff::rocks::RocksKv| kv.compact_all().unwrap(),
    |kv: &nucleus_bakeoff::rocks::RocksKv| kv.settle().unwrap(),
    100_000
);

#[cfg(feature = "fjall")]
score_tests!(
    fjall_scores,
    "fjall",
    nucleus_bakeoff::fjall_kv::FjallKv::open,
    |kv: &nucleus_bakeoff::fjall_kv::FjallKv| kv.compact_all().unwrap(),
    |kv: &nucleus_bakeoff::fjall_kv::FjallKv| kv.settle().unwrap(),
    1500
);
