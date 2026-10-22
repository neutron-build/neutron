//! The E1′ measurement driver (card X-1b; PLAN-v3 §4 E1′, deciding §2
//! N3): one binary that runs the W1–W6 workloads on arm (b) (ts-in-key,
//! card X-2) and arm (c) (newest-in-place + undo, card X-3) through the
//! same [`TxnEngine`](e1_mvcc::api::TxnEngine) surface, plus W1 on arm
//! (a) — v2 today on `MemKv` — as the reference for the single-row mix.
//!
//! Invariants owned here (card Work 2–3, card Do-not):
//! - B-SAME: arms (b) and (c) run under one [`Config`] — same page size,
//!   same pool sizes, same thread counts, same value and key shapes,
//!   same PRNG seeds per (workload, run) — so only the version layout
//!   differs. Arm (a) runs the same W1 mix through its own API.
//! - B-REPORT: numbers are reported exactly as measured, three runs per
//!   workload, median plus min/max; nothing is tuned toward an outcome.
//! - B-ABORT: before any measurement the arms pass
//!   [`e1_mvcc::suite::run_all`] in this same binary (`--verify` is
//!   implied for full runs; the acceptance `--verify --quick` runs it
//!   too), and a failed arm aborts the bench.
//! - B-TOTAL: no `unwrap`/`expect`/`panic!` outside tests; every fallible
//!   step maps to [`BenchError`] and stops the run with a message.
//!
//! The `E0` pool preallocates `frames × page_size` bytes, so the pool
//! sizes are part of the configuration and are chosen per workload just
//! large enough to hold the data ("pool large enough for the data unless
//! stated otherwise"), identically for both arms.

pub mod arma;
pub mod json;
pub mod util;
pub mod verify;
pub mod w1;
pub mod w2;
pub mod w3;
pub mod w5;
pub mod w6;
pub mod workload;

use std::fmt;
use std::time::Duration;

use json::Json;

/// The error of every bench step: a context plus the underlying failure
/// (B-TOTAL; PLAN-v3 §6.1's "never a panic" applied to the driver).
#[derive(Debug)]
pub struct BenchError(pub String);

impl BenchError {
    /// Build the error from a context and anything printable.
    pub fn new(ctx: &str, e: impl fmt::Display) -> Self {
        BenchError(format!("{ctx}: {e}"))
    }
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BenchError {}

/// A workload result: the report objects of one run (usually one; W1
/// emits one per thread count).
pub type RunOut = Vec<Json>;

/// The whole measurement configuration (card Work 2 defaults; B-SAME).
/// One value is shared by every arm of a run.
#[derive(Clone, Debug)]
pub struct Config {
    /// `--quick`: every workload scaled down 100× (the acceptance shape).
    pub quick: bool,
    /// Runs per workload; the report carries median + min/max (Work 3).
    pub runs: u32,
    /// B+tree page size in bytes (identical on both arms).
    pub page_size: usize,
    /// Buffer-pool frames for the 1M-key workloads (W1, W2, W3/W4, W5).
    pub pool_frames: usize,
    /// Buffer-pool frames for W6's 10M-key tree.
    pub pool_frames_big: usize,
    /// Arm (c)'s undo budget in bytes (card Work 2 W5 mandates 64 MiB;
    /// the same budget is used everywhere so no workload tunes it).
    pub undo_budget_bytes: u64,
    /// Value length in bytes (100).
    pub value_len: usize,
    /// Preloaded keys for every workload but W3/W4's range and W6.
    pub preload_keys: u64,
    /// Keys per preload txn.
    pub preload_batch: u64,
    /// W1 thread counts (1, 4, 8, 16).
    pub w1_threads: Vec<usize>,
    /// W1 txns per thread per thread-count configuration.
    pub w1_txns_per_thread: u64,
    /// W2 reader threads (8).
    pub w2_readers: usize,
    /// W2 writer-thread counts (0, 10, 100).
    pub w2_writer_counts: Vec<usize>,
    /// W2 hot-set keys (10k of the preload).
    pub w2_hot_keys: u64,
    /// W2 measurement window.
    pub w2_duration: Duration,
    /// W3 keys in the versioned range (100k).
    pub w3_keys: u64,
    /// W3 committed versions per key (10, no GC).
    pub w3_versions: u64,
    /// W3 rows per scanned range (100).
    pub w3_scan_rows: u64,
    /// W3 timed passes over the whole range per snapshot.
    pub w3_passes: u32,
    /// W5 writer threads (4).
    pub w5_writers: usize,
    /// W5 total updates across the writers (2M).
    pub w5_updates: u64,
    /// W5 keys the long reader re-reads and verifies at the end.
    pub w5_sample_keys: u64,
    /// W6 keys (10M; preloaded for this workload only).
    pub w6_keys: u64,
    /// W6 big-txn and probe lock-wait bound (the big commit is slow; a
    /// 1-row probe txn must outwait it, not time out).
    pub w6_wait: Duration,
    /// W5 peak-sampler interval (each sample calls `stats()`, which walks
    /// the tree; see the README for the cost note).
    pub sampler_interval: Duration,
    /// W1/W2 writer lock-wait bound.
    pub write_wait: Duration,
}

/// The 100× scale the card mandates for `--quick` acceptance runs.
pub const QUICK_SCALE: u64 = 100;

impl Config {
    /// The full-scale configuration (card Work 2 defaults).
    pub fn full() -> Self {
        Config {
            quick: false,
            runs: 3,
            page_size: 8192,
            // 512 MiB of frames: ~10× the ~15k pages a 1M-entry tree
            // needs, so eviction never dominates (W5 grows to ~3M
            // versions ≈ 45k pages and still fits).
            pool_frames: 65_536,
            // 3 GiB of frames for W6's ~300k pages after the big commit.
            pool_frames_big: 393_216,
            undo_budget_bytes: 64 << 20,
            value_len: 100,
            preload_keys: 1_000_000,
            preload_batch: 1_000,
            w1_threads: vec![1, 4, 8, 16],
            // 100k txns/thread: at 16 threads the contended mix costs
            // ~50-60 µs CPU/txn, so this keeps every configuration well
            // inside the driver's wall-clock budget while giving the
            // 100 Hz process-CPU clock >= 40 ticks per single-thread
            // configuration (card Work 2 fixes the mix, not the count).
            w1_txns_per_thread: 100_000,
            w2_readers: 8,
            w2_writer_counts: vec![0, 10, 100],
            w2_hot_keys: 10_000,
            w2_duration: Duration::from_secs(5),
            w3_keys: 100_000,
            w3_versions: 10,
            w3_scan_rows: 100,
            w3_passes: 5,
            w5_writers: 4,
            w5_updates: 2_000_000,
            w5_sample_keys: 1_000,
            w6_keys: 10_000_000,
            w6_wait: Duration::from_secs(300),
            sampler_interval: Duration::from_millis(500),
            write_wait: Duration::from_secs(30),
        }
    }

    /// The `--quick` configuration: every data-sized parameter ÷
    /// [`QUICK_SCALE`] (thread counts and version depth stay — they are
    /// the axes of the measurement, not its size; card Acceptance).
    pub fn quick() -> Self {
        let mut c = Config::full();
        c.quick = true;
        c.preload_keys /= QUICK_SCALE;
        c.w1_txns_per_thread /= QUICK_SCALE;
        c.w2_hot_keys /= QUICK_SCALE;
        c.w2_duration /= QUICK_SCALE as u32;
        c.w3_keys /= QUICK_SCALE;
        c.w5_updates /= QUICK_SCALE;
        c.w6_keys /= QUICK_SCALE;
        c.w5_sample_keys = c.w5_sample_keys.min(c.preload_keys);
        c
    }

    /// A short human-readable tag of the scale, for the report.
    pub fn scale_tag(&self) -> &'static str {
        if self.quick { "quick (1/100)" } else { "full" }
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, QUICK_SCALE};

    /// The quick scale divides every data-sized field by exactly 100 and
    /// leaves the axes (threads, versions per key, value length) alone.
    #[test]
    fn quick_scales_sizes_not_axes() {
        let f = Config::full();
        let q = Config::quick();
        assert!(q.quick);
        assert_eq!(q.preload_keys * QUICK_SCALE, f.preload_keys);
        assert_eq!(q.w1_txns_per_thread * QUICK_SCALE, f.w1_txns_per_thread);
        assert_eq!(q.w2_hot_keys * QUICK_SCALE, f.w2_hot_keys);
        assert_eq!(q.w3_keys * QUICK_SCALE, f.w3_keys);
        assert_eq!(q.w5_updates * QUICK_SCALE, f.w5_updates);
        assert_eq!(q.w6_keys * QUICK_SCALE, f.w6_keys);
        assert_eq!(q.w3_versions, f.w3_versions);
        assert_eq!(q.value_len, f.value_len);
        assert_eq!(q.w1_threads, f.w1_threads);
        assert_eq!(
            q.w2_duration.as_millis() * QUICK_SCALE as u128,
            f.w2_duration.as_millis()
        );
        // The sample must fit the shrunken preload.
        assert!(q.w5_sample_keys <= q.preload_keys);
    }
}
