//! W6 — big txn (card X-1b Work 2): one txn UPDATEs 10M keys (preloaded
//! for this workload only). Reports the write-set bytes and lock entries
//! at commit, escalations, commit time, and the **head-of-line delay** —
//! the p99 latency of 1-row txns committed by another thread during the
//! big commit (PLAN-v3 §4 E1′ "lock and write-set memory on a 10M-row
//! UPDATE; large-txn head-of-line delay"; §3.12's deferred pre-apply is
//! triggered by exactly this number).
//!
//! Invariants owned here:
//! - W6-PHASE: the probe thread stamps every 1-row txn with the phase it
//!   started in — `filling` (the big txn is still buffering writes) or
//!   `committing` (between the big commit's entry and its return). The
//!   card's head-of-line number is the p99 over the `committing` window;
//!   the whole-window p99 is reported beside it for context.
//! - W6-SAME: the big txn and the probe run the identical mix on both
//!   arms — `put` per key with the same lock-wait bound, and the probe
//!   blocks (never times out) through the big commit, because the delay
//!   being measured *is* the wait.
//! - W6-AT-COMMIT: `write_set_bytes` and `lock_entries` are read after
//!   the last buffered write and before the commit call; `escalations`
//!   is the shared lock table's own counter (the E1′ lock table is
//!   identical in both arms by construction — card X-1a).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::thread;
use std::time::Instant;

use e1_mvcc::api::{Isolation, TxnEngine};

use crate::BenchError;
use crate::Config;
use crate::json::{Json, obj};
use crate::util;
use crate::workload::{W_W6, block, err, preload, rng_for, single_row_update};

/// Probe-phase stamp: the big txn is filling its write set.
const PHASE_FILLING: u8 = 0;
/// Probe-phase stamp: the big commit is running.
const PHASE_COMMITTING: u8 = 1;

/// Run W6 on one fresh engine and return its report object (card Work
/// 2).
pub fn run<E: TxnEngine>(
    make: &dyn Fn(usize) -> Result<E, BenchError>,
    cfg: &Config,
    run: u32,
) -> Result<Json, BenchError> {
    let engine = Arc::new(make(cfg.pool_frames_big)?);
    preload(&*engine, cfg.w6_keys, cfg.preload_batch, cfg.value_len, 0)?;

    let rss_reset = util::reset_peak_rss();
    let phase = Arc::new(AtomicU8::new(PHASE_FILLING));
    let stop = Arc::new(AtomicBool::new(false));

    // The probe thread must be 'static: copy the config fields it needs.
    let probe_keys = cfg.w6_keys;
    let probe_wait = cfg.w6_wait;
    let probe_value_len = cfg.value_len;

    // The probe: 1-row RC update txns on random keys, latency stamped
    // with the phase at txn start (W6-PHASE). It blocks through the big
    // commit (W6-SAME) — the wait is the measurement.
    let probe = {
        let e = Arc::clone(&engine);
        let phase = Arc::clone(&phase);
        let stop = Arc::clone(&stop);
        thread::spawn(move || -> Vec<(u8, u64)> {
            let mut rng = rng_for(W_W6, run, 0, 7);
            let mut out = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let at = phase.load(Ordering::Relaxed);
                let idx = rng.below(probe_keys);
                let key = util::key_of(idx);
                let value = util::value_of(idx, u64::from(at) + 41, probe_value_len);
                let t0 = Instant::now();
                if single_row_update(&*e, &key, &value, block(probe_wait)).is_err() {
                    break; // surfaced by the txn counts below
                }
                out.push((at, t0.elapsed().as_nanos() as u64));
            }
            out
        })
    };

    // The big txn: buffer one put per key, then commit (W6-SAME).
    let fill0 = Instant::now();
    let mut big = engine.begin(Isolation::ReadCommitted);
    for i in 0..cfg.w6_keys {
        let key = util::key_of(i);
        let value = util::value_of(i, 1, cfg.value_len);
        engine
            .put(&mut big, &key, &value, block(cfg.w6_wait))
            .map_err(|e| err(&format!("W6 big put key {i}"), e))?;
    }
    let fill = fill0.elapsed();

    // W6-AT-COMMIT: the memory and lock picture the commit walks in
    // with.
    let at_commit = engine.stats();
    let escalations = engine.lock_table().escalations();
    let escalation_attempts = engine.lock_table().escalation_attempts();

    phase.store(PHASE_COMMITTING, Ordering::Release);
    let commit0 = Instant::now();
    let commit_ts = engine.commit(big).map_err(|e| err("W6 big commit", e))?;
    let commit_time = commit0.elapsed();
    phase.store(PHASE_FILLING, Ordering::Release);
    stop.store(true, Ordering::Release);

    let probe_latencies = probe
        .join()
        .map_err(|_| BenchError("W6 probe panicked".into()))?;
    let after = engine.stats();
    let peak_rss = util::peak_rss_kib();

    let committing: Vec<u64> = probe_latencies
        .iter()
        .filter(|(p, _)| *p == PHASE_COMMITTING)
        .map(|(_, ns)| *ns)
        .collect();
    let filling: Vec<u64> = probe_latencies
        .iter()
        .filter(|(p, _)| *p == PHASE_FILLING)
        .map(|(_, ns)| *ns)
        .collect();
    let all: Vec<u64> = probe_latencies.iter().map(|(_, ns)| *ns).collect();
    let (cm_p50, cm_p99) = util::latencies_p50_p99(&committing);
    let (fl_p50, fl_p99) = util::latencies_p50_p99(&filling);
    let (_, all_p99) = util::latencies_p50_p99(&all);

    Ok(obj(vec![
        ("workload", "W6".into()),
        ("keys", cfg.w6_keys.into()),
        ("value_len", cfg.value_len.into()),
        ("fill_time_s", util::secs(fill).into()),
        ("commit_time_s", util::secs(commit_time).into()),
        ("commit_ts", commit_ts.into()),
        (
            "write_set_bytes_at_commit",
            at_commit.write_set_bytes.into(),
        ),
        ("lock_entries_at_commit", at_commit.lock_entries.into()),
        ("escalations", escalations.into()),
        ("escalation_attempts", escalation_attempts.into()),
        (
            "write_set_bytes_per_key",
            if cfg.w6_keys == 0 {
                Json::Null
            } else {
                Json::Num(at_commit.write_set_bytes as f64 / cfg.w6_keys as f64)
            },
        ),
        ("probe_txns", all.len().into()),
        ("probe_commit_window_txns", committing.len().into()),
        ("probe_fill_window_txns", filling.len().into()),
        (
            "head_of_line_p99_ns",
            cm_p99.map(Json::from).unwrap_or(Json::Null),
        ),
        (
            "head_of_line_p50_ns",
            cm_p50.map(Json::from).unwrap_or(Json::Null),
        ),
        ("probe_fill_p50_ns", fl_p50.map(Json::from).unwrap_or(Json::Null)),
        ("probe_fill_p99_ns", fl_p99.map(Json::from).unwrap_or(Json::Null)),
        ("probe_all_p99_ns", all_p99.map(Json::from).unwrap_or(Json::Null)),
        ("peak_rss_kib", peak_rss.map(Json::from).unwrap_or(Json::Null)),
        ("peak_rss_reset_worked", rss_reset.into()),
        (
            "after_stats",
            obj(vec![
                ("tree_bytes", after.tree_bytes.into()),
                ("versions", after.versions.into()),
                ("undo_bytes", after.undo_bytes.into()),
                ("undo_spilled_bytes", after.undo_spilled_bytes.into()),
                (
                    "bytes_applied_delta",
                    after
                        .bytes_applied
                        .saturating_sub(at_commit.bytes_applied)
                        .into(),
                ),
            ]),
        ),
        (
            "note",
            "head_of_line = p99 of 1-row txns that began while the big commit was running; probe txns block on the big txn's locks until its release"
                .into(),
        ),
    ]))
}
