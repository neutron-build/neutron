//! W1 — single-row write mix (card X-1b Work 2): RC txns, each updating
//! one random key of the preloaded set and committing, at 1, 4, 8 and 16
//! threads. Reports µs of CPU per txn (process CPU time / txns) and
//! bytes applied per txn (PLAN-v3 §4 E1′ "CPU and bytes per txn").
//!
//! Invariants owned here:
//! - W1-FRESH: every thread-count configuration gets a fresh engine and
//!   preload, so a configuration runs over exactly `preload_keys`
//!   versions per key, never over the versions an earlier configuration
//!   of the same run already wrote.
//! - W1-CPU: "CPU" is whole-process CPU (utime+stime of
//!   `/proc/self/stat`, resolution `CLK_TCK`), measured around the
//!   worker window only — preload and joins are excluded; every thread
//!   the workload started (workers only — no helper threads exist in
//!   W1) is inside the window.
//! - W1-BYTES: bytes per txn is the delta of
//!   [`EngineStats::bytes_applied`](e1_mvcc::api::EngineStats::bytes_applied)
//!   over the window divided by the txn count. The arms account
//!   differently — (b) counts the encoded tree entry, (c) the logical
//!   key+value — and the report says so rather than normalizing either
//!   away (B-REPORT).

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use e1_mvcc::api::{Isolation, TxnEngine};

use crate::BenchError;
use crate::Config;
use crate::json::{Json, obj};
use crate::util;
use crate::workload::{W_W1, block, err, preload, rng_for};

/// Run one W1 (one thread-count configuration) and return its report
/// object (card Work 2). `make(pool_frames)` builds a fresh engine.
pub fn run<E: TxnEngine>(
    make: &dyn Fn(usize) -> Result<E, BenchError>,
    cfg: &Config,
    run: u32,
    threads: usize,
) -> Result<Json, BenchError> {
    let engine = make(cfg.pool_frames)?;
    preload(
        &engine,
        cfg.preload_keys,
        cfg.preload_batch,
        cfg.value_len,
        0,
    )?;
    let before = engine.stats();

    let total_txns = threads as u64 * cfg.w1_txns_per_thread;
    let tps = util::clock_ticks_per_sec();
    let cpu0 = util::cpu_micros(tps);
    let wall0 = Instant::now();

    let engine = Arc::new(engine);
    thread::scope(|s| -> Result<(), BenchError> {
        let mut handles = Vec::new();
        for tid in 0..threads {
            let e = Arc::clone(&engine);
            handles.push(s.spawn(move || -> Result<u64, BenchError> {
                let mut rng = rng_for(W_W1, run, tid, 0);
                let mut done = 0u64;
                for n in 0..cfg.w1_txns_per_thread {
                    let key = util::key_of(rng.below(cfg.preload_keys));
                    let value = util::value_of(u64::from(run) ^ (n << 8), 1, cfg.value_len);
                    let mut t = e.begin(Isolation::ReadCommitted);
                    e.put(&mut t, &key, &value, block(cfg.write_wait))
                        .map_err(|x| err("W1 put", x))?;
                    e.commit(t).map_err(|x| err("W1 commit", x))?;
                    done += 1;
                }
                Ok(done)
            }));
        }
        for h in handles {
            let done = h
                .join()
                .map_err(|_| BenchError("W1 worker panicked".into()))??;
            if done != cfg.w1_txns_per_thread {
                return Err(BenchError("W1 worker stopped early".into()));
            }
        }
        Ok(())
    })?;

    let wall = wall0.elapsed();
    let cpu1 = util::cpu_micros(tps);
    let after = engine.stats();

    let cpu_us = cpu1.zip(cpu0).map(|(a, b)| a.saturating_sub(b));
    let bytes_delta = after.bytes_applied.saturating_sub(before.bytes_applied);
    Ok(obj(vec![
        ("workload", "W1".into()),
        ("threads", threads.into()),
        ("keys", cfg.preload_keys.into()),
        ("txns_per_thread", cfg.w1_txns_per_thread.into()),
        ("txns", total_txns.into()),
        ("wall_s", util::secs(wall).into()),
        (
            "cpu_us_per_txn",
            cpu_us
                .map(|us| (us as f64) / (total_txns as f64))
                .map(Json::Num)
                .unwrap_or(Json::Null),
        ),
        (
            "bytes_applied_per_txn",
            (bytes_delta as f64 / total_txns as f64).into(),
        ),
        ("txns_per_s", (total_txns as f64 / util::secs(wall)).into()),
        ("versions_after", after.versions.into()),
        (
            "cpu_us_per_txn_note",
            "process CPU (utime+stime, CLK_TCK resolution) / txns".into(),
        ),
        (
            "bytes_note",
            "arm b counts encoded tree entries, arm c logical key+value; see README".into(),
        ),
        (
            "stats",
            obj(vec![
                ("tree_bytes", after.tree_bytes.into()),
                ("versions", after.versions.into()),
            ]),
        ),
    ]))
}
