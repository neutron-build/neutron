//! W6 — big txn (card X-1b Work 2): one txn UPDATEs 10M keys (preloaded
//! for this workload only). Reports the write-set bytes and lock entries
//! at commit, escalations, commit time, and the **head-of-line delay** —
//! the p99 latency of 1-row txns committed by another thread during the
//! big commit (PLAN-v3 §4 E1′ "lock and write-set memory on a 10M-row
//! UPDATE; large-txn head-of-line delay"; §3.12's deferred pre-apply is
//! triggered by exactly this number).
//!
//! Invariants owned here:
//! - W6-PHASE: every probe txn is stamped with the phase it started
//!   in — `locks` (the big txn is buffering writes; it already holds row
//!   locks on everything it has written and holds them until its commit
//!   releases them) or `apply` (between the big commit's entry and its
//!   return). Two probe crews measure the two windows: the fill crew
//!   runs from the start, and the apply crew holds back until the apply
//!   begins — a single crew parks on the first locked key during the
//!   fill and would never arrive during the apply at all.
//! - W6-HOL: the head-of-line population is the apply-window txns
//!   delayed ≥ [`HOL_THRESHOLD_NS`] — a 1-row txn never reaches the
//!   threshold uncontended (W1/W2 p99s are far below it), so those are
//!   exactly the txns the big commit made wait. The release of the big
//!   txn's locks is itself O(its keys) and takes milliseconds-to-seconds
//!   at full scale; 1-row txns that slip through that release tail are
//!   reported as `slipped`, not counted as delayed.
//! - W6-SAME: the big txn and every probe run the identical mix on both
//!   arms — `put` per key with the same lock-wait bound; the probes
//!   block (never time out) through the big commit, because the wait
//!   being measured *is* the phenomenon.
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

/// Probe-phase stamp: the big txn is buffering writes (`fill` window).
const PHASE_LOCKS: u8 = 1;
/// Probe-phase stamp: the big commit is running (`apply` window).
const PHASE_APPLY: u8 = 2;

/// A latency above this many ns means the probe txn waited on the big
/// txn (W6-HOL): uncontended 1-row txns stay an order of magnitude
/// below it (see W1/W2 in the same report).
const HOL_THRESHOLD_NS: u64 = 1_000_000;

/// How many probe threads run in each crew (identical on both arms).
const FILL_PROBES: usize = 2;
const APPLY_PROBES: usize = 4;

/// One probe txn's record: the phase it started in, its start offset
/// from the run's `t0`, and its latency (W6-PHASE).
struct Sample {
    phase: u8,
    #[allow(dead_code)] // kept for future timeline reports; the phase
    start_ns: u64, //   and latency carry today's numbers
    lat_ns: u64,
}

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
    let phase = Arc::new(AtomicU8::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // The probe threads must be 'static: copy the config fields they
    // need, plus the run's t0 every start offset is measured against.
    let probe_keys = cfg.w6_keys;
    let probe_wait = cfg.w6_wait;
    let probe_value_len = cfg.value_len;
    let t0 = Instant::now();

    /// Everything one probe crew needs (the `crew` bundle).
    struct CrewCtx<'a, E> {
        engine: &'a Arc<E>,
        phase: &'a AtomicU8,
        stop: &'a AtomicBool,
        t0: Instant,
        keys: u64,
        wait: std::time::Duration,
        value_len: usize,
    }

    /// One probe crew's loop (W6-PHASE, W6-SAME): 1-row RC update txns
    /// on random keys, each stamped with the phase at its start.
    fn crew<E: TxnEngine>(cx: CrewCtx<'_, E>, seed: u64, wait_for_apply: bool) -> Vec<Sample> {
        let mut rng = util::Rng::new(seed);
        let mut out = Vec::new();
        if wait_for_apply {
            while cx.phase.load(Ordering::Relaxed) != PHASE_APPLY {
                if cx.stop.load(Ordering::Relaxed) {
                    return out;
                }
                thread::yield_now();
            }
        }
        while !cx.stop.load(Ordering::Relaxed) {
            let at = cx.phase.load(Ordering::Relaxed);
            let idx = rng.below(cx.keys);
            let key = util::key_of(idx);
            let value = util::value_of(idx, u64::from(at) + 41, cx.value_len);
            let start = cx.t0.elapsed().as_nanos() as u64;
            if single_row_update(cx.engine.as_ref(), &key, &value, block(cx.wait)).is_err() {
                break; // an errored probe under-counts; the counts show it
            }
            out.push(Sample {
                phase: at,
                start_ns: start,
                lat_ns: (cx.t0.elapsed().as_nanos() as u64).saturating_sub(start),
            });
        }
        out
    }

    // The fill crew runs from the start (W6-PHASE); the apply crew
    // holds back until the big commit begins.
    let mut fill_handles = Vec::new();
    for tid in 0..FILL_PROBES {
        let e = Arc::clone(&engine);
        let phase = Arc::clone(&phase);
        let stop = Arc::clone(&stop);
        let seed = rng_for(W_W6, run, tid, 7).next_u64();
        fill_handles.push(thread::spawn(move || {
            let cx = CrewCtx {
                engine: &e,
                phase: &phase,
                stop: &stop,
                t0,
                keys: probe_keys,
                wait: probe_wait,
                value_len: probe_value_len,
            };
            crew(cx, seed, false)
        }));
    }
    let mut apply_handles = Vec::new();
    for tid in 0..APPLY_PROBES {
        let e = Arc::clone(&engine);
        let phase = Arc::clone(&phase);
        let stop = Arc::clone(&stop);
        let seed = rng_for(W_W6, run, 100 + tid, 7).next_u64();
        apply_handles.push(thread::spawn(move || {
            let cx = CrewCtx {
                engine: &e,
                phase: &phase,
                stop: &stop,
                t0,
                keys: probe_keys,
                wait: probe_wait,
                value_len: probe_value_len,
            };
            crew(cx, seed, true)
        }));
    }

    // The big txn: buffer one put per key, then commit (W6-SAME).
    phase.store(PHASE_LOCKS, Ordering::Release);
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

    phase.store(PHASE_APPLY, Ordering::Release);
    let commit0 = Instant::now();
    let commit_ts = engine.commit(big).map_err(|e| err("W6 big commit", e))?;
    let commit_time = commit0.elapsed();
    phase.store(0, Ordering::Release);
    stop.store(true, Ordering::Release);

    let mut samples = Vec::new();
    let mut failures = 0u64;
    for h in fill_handles.into_iter().chain(apply_handles) {
        match h.join() {
            Ok(v) => samples.extend(v),
            Err(_) => failures += 1,
        }
    }
    if failures > 0 {
        return Err(BenchError(format!("W6: {failures} probe threads panicked")));
    }
    let after = engine.stats();
    let peak_rss = util::peak_rss_kib();

    let apply_all: Vec<u64> = samples
        .iter()
        .filter(|s| s.phase == PHASE_APPLY)
        .map(|s| s.lat_ns)
        .collect();
    let apply_hol: Vec<u64> = apply_all
        .iter()
        .copied()
        .filter(|ns| *ns >= HOL_THRESHOLD_NS)
        .collect();
    let apply_slipped = apply_all.len() - apply_hol.len();
    let locks_all: Vec<u64> = samples
        .iter()
        .filter(|s| s.phase == PHASE_LOCKS)
        .map(|s| s.lat_ns)
        .collect();
    let locks_hol: Vec<u64> = locks_all
        .iter()
        .copied()
        .filter(|ns| *ns >= HOL_THRESHOLD_NS)
        .collect();
    let all: Vec<u64> = samples.iter().map(|s| s.lat_ns).collect();

    let (hol_p50, hol_p99) = util::latencies_p50_p99(&apply_hol);
    let hol_max = apply_hol.iter().copied().max();
    let (ap_p50, ap_p99) = util::latencies_p50_p99(&apply_all);
    let (lk_p50, lk_p99) = util::latencies_p50_p99(&locks_all);
    let lk_max = locks_hol.iter().copied().max();
    let (_, all_p99) = util::latencies_p50_p99(&all);

    Ok(obj(vec![
        ("workload", "W6".into()),
        ("keys", cfg.w6_keys.into()),
        ("value_len", cfg.value_len.into()),
        ("fill_time_s", util::secs(fill).into()),
        ("commit_time_s", util::secs(commit_time).into()),
        ("commit_ts", commit_ts.into()),
        ("write_set_bytes_at_commit", at_commit.write_set_bytes.into()),
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
        ("apply_window_txns", apply_all.len().into()),
        ("apply_window_p50_ns", ap_p50.map(Json::from).unwrap_or(Json::Null)),
        ("apply_window_p99_ns", ap_p99.map(Json::from).unwrap_or(Json::Null)),
        ("apply_delayed_txns", apply_hol.len().into()),
        ("apply_slipped_txns", apply_slipped.into()),
        (
            "head_of_line_p50_ns",
            hol_p50.map(Json::from).unwrap_or(Json::Null),
        ),
        (
            "head_of_line_p99_ns",
            hol_p99.map(Json::from).unwrap_or(Json::Null),
        ),
        ("head_of_line_max_ns", hol_max.map(Json::from).unwrap_or(Json::Null)),
        ("locks_window_txns", locks_all.len().into()),
        ("locks_window_p50_ns", lk_p50.map(Json::from).unwrap_or(Json::Null)),
        ("locks_window_p99_ns", lk_p99.map(Json::from).unwrap_or(Json::Null)),
        ("locks_window_max_delayed_ns", lk_max.map(Json::from).unwrap_or(Json::Null)),
        ("locks_window_delayed_txns", locks_hol.len().into()),
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
            "head_of_line = p50/p99/max over apply-window probe txns delayed >= 1 ms (a 1-row txn never reaches 1 ms uncontended); the big txn holds every row lock from its first write until its commit's release, whose O(keys) duration lets late arrivals slip through (reported as slipped)"
                .into(),
        ),
    ]))
}
