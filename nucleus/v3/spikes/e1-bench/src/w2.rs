//! W2 — reads under writers (card X-1b Work 2): 8 reader threads doing
//! point gets on a hot set of 10k keys while 0, 10 or 100 writer threads
//! update the same hot set. Reports reader ns/op p50/p99 and reader
//! ops/s (PLAN-v3 §4 E1′ "read cost at 0/10/100 concurrent writers").
//!
//! Invariants owned here:
//! - W2-FRESH: every writer-count configuration gets a fresh engine and
//!   preload (same reasoning as W1-FRESH).
//! - W2-OP: one reader op is a whole autocommit point SELECT — begin(RC),
//!   one `get`, abort — so the snapshot register/unregister is inside
//!   the measured latency, exactly as a session would pay it.
//! - W2-WINDOW: readers and writers all start at a barrier and run for
//!   the configured window; latency samples beyond 2M per reader are
//!   counted but not recorded (reported via `latency_samples_capped`) —
//!   the steady-state prefix carries the percentiles.
//! - W2-ERR: a writer lock timeout or commit error is counted and the
//!   writer continues (contention is the phenomenon measured); a reader
//!   error is a workload failure (reads never block, so one has nothing
//!   to count through).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use e1_mvcc::api::{Isolation, TxnEngine};

use crate::BenchError;
use crate::Config;
use crate::json::{Json, obj};
use crate::util;
use crate::workload::{W_W2, block, err, preload, rng_for};

/// Per-reader latency samples kept before recording stops (W2-WINDOW).
const MAX_SAMPLES_PER_READER: usize = 2_000_000;

/// One reader thread's loop (W2-OP, W2-WINDOW). Returns its recorded
/// samples, its total op count, and whether recording was capped.
fn reader_loop<E: TxnEngine>(
    engine: &E,
    hot: u64,
    deadline: Instant,
    rng_seed: u64,
    stop: &AtomicBool,
) -> Result<(Vec<u64>, u64, bool), BenchError> {
    let mut rng = util::Rng::new(rng_seed);
    let mut samples = Vec::new();
    let mut ops = 0u64;
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        let key = util::key_of(rng.below(hot));
        let t0 = Instant::now();
        let mut t = engine.begin(Isolation::ReadCommitted);
        let got = engine.get(&mut t, &key).map_err(|e| err("W2 get", e))?;
        engine.abort(t);
        let ns = t0.elapsed().as_nanos() as u64;
        if got.is_none() {
            // Every hot key was preloaded; a miss is a broken arm.
            return Err(BenchError(format!("W2 read miss on preloaded key {key:?}")));
        }
        ops += 1;
        if samples.len() < MAX_SAMPLES_PER_READER {
            samples.push(ns);
        }
    }
    let capped = (samples.len() as u64) < ops;
    Ok((samples, ops, capped))
}

/// One writer thread's loop: single-row update txns on the hot set until
/// the deadline (W2-WINDOW). Returns (attempts, errors) — W2-ERR.
fn writer_loop<E: TxnEngine>(
    engine: &E,
    hot: u64,
    deadline: Instant,
    rng_seed: u64,
    stop: &AtomicBool,
    wait: Duration,
) -> (u64, u64) {
    let mut rng = util::Rng::new(rng_seed);
    let mut attempts = 0u64;
    let mut errors = 0u64;
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        let idx = rng.below(hot);
        let key = util::key_of(idx);
        let value = util::value_of(idx, attempts.wrapping_add(1), 100);
        attempts += 1;
        match crate::workload::single_row_update(engine, &key, &value, block(wait)) {
            Ok(()) => {}
            Err(_) => errors += 1, // a lock timeout under contention; counted
        }
    }
    (attempts, errors)
}

/// Run one W2 configuration (one writer count) and return its report
/// object (card Work 2).
pub fn run<E: TxnEngine>(
    make: &dyn Fn(usize) -> Result<E, BenchError>,
    cfg: &Config,
    run: u32,
    writers: usize,
) -> Result<Json, BenchError> {
    let engine = Arc::new(make(cfg.pool_frames)?);
    preload(
        &*engine,
        cfg.preload_keys,
        cfg.preload_batch,
        cfg.value_len,
        0,
    )?;
    let hot = cfg.w2_hot_keys.min(cfg.preload_keys);

    let barrier = Arc::new(Barrier::new(cfg.w2_readers + writers + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let writer_attempts = Arc::new(AtomicU64::new(0));
    let writer_errors = Arc::new(AtomicU64::new(0));

    let (all_samples, reader_ops, capped, wall, failure) = thread::scope(|s| {
        let mut reader_handles = Vec::new();
        for tid in 0..cfg.w2_readers {
            let e = Arc::clone(&engine);
            let b = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let seed = rng_for(W_W2, run, tid, 0).next_u64();
            reader_handles.push(s.spawn(move || {
                b.wait();
                let deadline = Instant::now() + cfg.w2_duration;
                reader_loop(&*e, hot, deadline, seed, &stop)
            }));
        }
        let mut writer_handles = Vec::new();
        for tid in 0..writers {
            let e = Arc::clone(&engine);
            let b = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let att = Arc::clone(&writer_attempts);
            let errs = Arc::clone(&writer_errors);
            let seed = rng_for(W_W2, run, tid, 1).next_u64();
            writer_handles.push(s.spawn(move || {
                b.wait();
                let deadline = Instant::now() + cfg.w2_duration;
                let (a, x) = writer_loop(&*e, hot, deadline, seed, &stop, cfg.write_wait);
                att.fetch_add(a, Ordering::Relaxed);
                errs.fetch_add(x, Ordering::Relaxed);
            }));
        }
        // Every thread's deadline is its own begin + window; the wall
        // clock starts when the barrier opens.
        barrier.wait();
        let t0 = Instant::now();
        let mut samples = Vec::new();
        let mut ops = 0u64;
        let mut capped_any = false;
        let mut failure: Option<BenchError> = None;
        for h in reader_handles {
            match h.join() {
                Ok(Ok((v, n, c))) => {
                    samples.extend(v);
                    ops += n;
                    capped_any = capped_any || c;
                }
                Ok(Err(e)) => failure = Some(e),
                Err(_) => {
                    failure = Some(BenchError("W2 reader panicked".into()));
                }
            }
        }
        for h in writer_handles {
            if h.join().is_err() && failure.is_none() {
                failure = Some(BenchError("W2 writer panicked".into()));
            }
        }
        stop.store(true, Ordering::Release);
        (samples, ops, capped_any, t0.elapsed(), failure)
    });

    if let Some(e) = failure {
        return Err(e);
    }
    let (p50, p99) = util::latencies_p50_p99(&all_samples);
    let ops = reader_ops;
    let attempts = writer_attempts.load(Ordering::Relaxed);
    let errors = writer_errors.load(Ordering::Relaxed);

    Ok(obj(vec![
        ("workload", "W2".into()),
        ("readers", cfg.w2_readers.into()),
        ("writers", writers.into()),
        ("hot_keys", hot.into()),
        ("window_s", util::secs(cfg.w2_duration).into()),
        ("wall_s", util::secs(wall).into()),
        ("reader_ops", ops.into()),
        ("reader_ops_per_s", (ops as f64 / util::secs(wall)).into()),
        ("reader_p50_ns", p50.map(Json::from).unwrap_or(Json::Null)),
        ("reader_p99_ns", p99.map(Json::from).unwrap_or(Json::Null)),
        ("latency_samples", all_samples.len().into()),
        ("writer_attempts", attempts.into()),
        ("writer_errors", errors.into()),
        ("latency_samples_capped", capped.into()),
        (
            "note",
            "reader op = begin(RC) + one point get + abort; percentiles are nearest-rank over recorded samples"
                .into(),
        ),
    ]))
}
