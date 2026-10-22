//! W3 + W4 — scan cost at 10 versions/key and space amplification (card
//! X-1b Work 2): a 100k-key range where every key has 10 committed
//! versions (no GC); the time to scan 100-row ranges at the latest
//! snapshot and at an old snapshot held before the updates (W3), then
//! `tree_bytes / logical_bytes` before and after `gc()` with no active
//! snapshots (W4).
//!
//! Invariants owned here:
//! - W3-SNAP: the old snapshot is one RR txn begun after the preload and
//!   before the first update round, and it stays open through every
//!   old-snapshot scan; "latest" is one RC txn whose snapshot is
//!   refreshed (next_statement) before every scan.
//! - W3-TIMED: a timed op is the `scan` call alone; verification reads
//!   (that the old snapshot still sees the preload values and the latest
//!   sees the last round's) run untimed after the measurement.
//! - W4-ORDER: W4's "before" is measured after the old reader closed
//!   (so its snapshot no longer pins anything), then one `gc()`, then
//!   "after" — the card's "after W3 plus gc() with no active snapshots".
//! - W4-RATIO: the reported ratios are `tree_bytes / logical_bytes` and
//!   `(tree_bytes + undo_bytes + undo_spilled_bytes) / logical_bytes` —
//!   arm (c) keeps its old versions in the undo log, and hiding that
//!   would hide the arm's real amplification (B-REPORT).

use std::time::Instant;

use e1_mvcc::api::{Isolation, TxnEngine};

use crate::BenchError;
use crate::Config;
use crate::json::{Json, obj};
use crate::util;
use crate::workload::{err, preload};

/// Scan every `scan_rows`-row range of `[0, keys)` once, timing each
/// `scan` call (W3-TIMED). Returns the per-scan nanoseconds of the pass.
fn scan_pass<E: TxnEngine>(
    engine: &E,
    t: &mut E::Txn,
    keys: u64,
    scan_rows: u64,
    rc: bool,
) -> Result<Vec<u64>, BenchError> {
    let mut out = Vec::new();
    let mut lo = 0u64;
    while lo < keys {
        let hi_key = (lo + scan_rows).min(keys);
        if rc {
            engine.next_statement(t);
        }
        let lo_key = util::key_of(lo);
        let hi_key_b = util::key_of(hi_key);
        let t0 = Instant::now();
        let rows = engine
            .scan(t, &lo_key, &hi_key_b, scan_rows as usize)
            .map_err(|e| err("W3 scan", e))?;
        out.push(t0.elapsed().as_nanos() as u64);
        if rows.len() != (hi_key - lo) as usize {
            return Err(BenchError(format!(
                "W3 scan [{lo},{hi_key}) returned {} rows",
                rows.len()
            )));
        }
        lo = hi_key;
    }
    Ok(out)
}

/// Untimed verification pass (W3-TIMED): through the still-open `old`
/// RR txn the values must be the preload's; at a fresh RC statement
/// they must be the final round's. The old txn arrives as an argument —
/// a freshly begun RR would see the latest state and prove nothing.
fn verify_reads<E: TxnEngine>(
    engine: &E,
    old: &mut E::Txn,
    keys: u64,
    versions: u64,
    value_len: usize,
) -> Result<bool, BenchError> {
    let mut latest = engine.begin(Isolation::ReadCommitted);
    engine.next_statement(&mut latest);
    let stride = usize::try_from((keys / 64).max(1)).unwrap_or(1);
    for i in (0..keys).step_by(stride) {
        let k = util::key_of(i);
        let want_old = util::value_of(i, 0, value_len);
        let want_new = util::value_of(i, versions - 1, value_len);
        let got_old = engine.get(old, &k).map_err(|e| err("W3 old get", e))?;
        let got_new = engine
            .get(&mut latest, &k)
            .map_err(|e| err("W3 new get", e))?;
        if got_old.as_deref() != Some(want_old.as_slice())
            || got_new.as_deref() != Some(want_new.as_slice())
        {
            engine.abort(latest);
            return Ok(false);
        }
    }
    engine.abort(latest);
    Ok(true)
}

/// Run W3 and W4 on one fresh engine and return both report objects
/// (card Work 2: W4 is measured on the W3 world).
pub fn run<E: TxnEngine>(
    make: &dyn Fn(usize) -> Result<E, BenchError>,
    cfg: &Config,
    _run: u32,
) -> Result<Vec<Json>, BenchError> {
    let engine = make(cfg.pool_frames)?;
    let keys = cfg.w3_keys;
    let versions = cfg.w3_versions.max(2);
    // Version 1: the preload. The old snapshot is taken here (W3-SNAP).
    preload(&engine, keys, cfg.preload_batch, cfg.value_len, 0)?;
    let mut old = engine.begin(Isolation::RepeatableRead);

    // Versions 2..=versions: `versions - 1` update rounds, each round a
    // pass of batched single-version commits (round r writes salt r).
    for round in 1..versions {
        preload(&engine, keys, cfg.preload_batch, cfg.value_len, round)?;
    }

    // W3: timed scans at both snapshots, then the untimed verification
    // while the old txn (and its snapshot) is still open (W3-TIMED).
    let mut old_ns = Vec::new();
    let mut new_ns = Vec::new();
    for _ in 0..cfg.w3_passes {
        let mut latest = engine.begin(Isolation::ReadCommitted);
        new_ns.extend(scan_pass(
            &engine,
            &mut latest,
            keys,
            cfg.w3_scan_rows,
            true,
        )?);
        engine.abort(latest);
        old_ns.extend(scan_pass(&engine, &mut old, keys, cfg.w3_scan_rows, false)?);
    }
    let verified = verify_reads(&engine, &mut old, keys, versions, cfg.value_len)?;
    engine.abort(old); // W4-ORDER: the old reader no longer pins anything

    // W4: before / gc / after (W4-ORDER, W4-RATIO).
    let before = engine.stats();
    let reclaimed = engine.gc();
    let after = engine.stats();
    let ratio = |over: u64, logical: u64| {
        if logical == 0 {
            Json::Null
        } else {
            Json::Num(over as f64 / logical as f64)
        }
    };

    let w3 = obj(vec![
        ("workload", "W3".into()),
        ("keys", keys.into()),
        ("versions_per_key", versions.into()),
        ("scan_rows", cfg.w3_scan_rows.into()),
        ("passes", cfg.w3_passes.into()),
        ("scans", (old_ns.len() + new_ns.len()).into()),
        (
            "latest_p50_ns",
            util::percentile(&sorted(&new_ns), 50.0).map(Json::from).unwrap_or(Json::Null),
        ),
        (
            "latest_p99_ns",
            util::percentile(&sorted(&new_ns), 99.0).map(Json::from).unwrap_or(Json::Null),
        ),
        (
            "latest_mean_ns",
            mean_ns(&new_ns).map(Json::Num).unwrap_or(Json::Null),
        ),
        (
            "old_p50_ns",
            util::percentile(&sorted(&old_ns), 50.0).map(Json::from).unwrap_or(Json::Null),
        ),
        (
            "old_p99_ns",
            util::percentile(&sorted(&old_ns), 99.0).map(Json::from).unwrap_or(Json::Null),
        ),
        ("old_mean_ns", mean_ns(&old_ns).map(Json::Num).unwrap_or(Json::Null)),
        ("values_verified", verified.into()),
        (
            "note",
            "old = one RR snapshot held from before the updates; latest = fresh RC snapshot per scan"
                .into(),
        ),
    ]);

    let w4 = obj(vec![
        ("workload", "W4".into()),
        ("keys", keys.into()),
        ("versions_per_key", versions.into()),
        ("before", stats_obj(&before)),
        ("after", stats_obj(&after)),
        ("gc_reclaimed_bytes", reclaimed.into()),
        (
            "tree_over_logical_before",
            ratio(before.tree_bytes, before.logical_bytes),
        ),
        (
            "tree_over_logical_after",
            ratio(after.tree_bytes, after.logical_bytes),
        ),
        (
            "tree_plus_undo_over_logical_before",
            ratio(
                before.tree_bytes + before.undo_bytes + before.undo_spilled_bytes,
                before.logical_bytes,
            ),
        ),
        (
            "tree_plus_undo_over_logical_after",
            ratio(
                after.tree_bytes + after.undo_bytes + after.undo_spilled_bytes,
                after.logical_bytes,
            ),
        ),
    ]);
    Ok(vec![w3, w4])
}

/// Sort a copy (the percentile helper takes sorted input).
fn sorted(v: &[u64]) -> Vec<u64> {
    let mut s = v.to_vec();
    s.sort_unstable();
    s
}

/// Mean of a latency slice in ns (None when empty).
fn mean_ns(v: &[u64]) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    Some(v.iter().map(|x| *x as f64).sum::<f64>() / v.len() as f64)
}

/// The stats snapshot W4 reports.
fn stats_obj(s: &e1_mvcc::api::EngineStats) -> Json {
    obj(vec![
        ("tree_bytes", s.tree_bytes.into()),
        ("undo_bytes", s.undo_bytes.into()),
        ("undo_spilled_bytes", s.undo_spilled_bytes.into()),
        ("versions", s.versions.into()),
        ("logical_bytes", s.logical_bytes.into()),
    ])
}
