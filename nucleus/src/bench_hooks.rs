//! Runtime switches that let a benchmark remove one half of the PRIMARY KEY
//! write path so its cost can be attributed by subtraction.
//!
//! A PK insert does two things: a uniqueness probe
//! (`check_unique_constraints` -> `index_lookup_sync`) and B-tree maintenance
//! (`DiskEngine::index_insert`). Together they cost ~38 us/row on the paged
//! engine, and three code-reading hypotheses about *where* inside that have
//! now measured as noise. The only way left to split the number is to delete
//! each half and re-measure.
//!
//! Env vars would force separate processes, and this machine drifts far enough
//! between measurement batches (the same binary: 1505 ms vs 1885 ms) that only
//! interleaved A/B inside one loop is meaningful. So these are atomics,
//! flipped between arms of a single run. All default to off; when off each
//! costs one `Relaxed` load, paid identically by every arm.
//!
//! **Not a supported feature.** The two `skip_*` switches make the engine
//! accept duplicate keys or stop maintaining indexes; `legacy_leaf_ops` is
//! merely slower. `attr_pk_write` is the only caller.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static SKIP_UNIQUE_PROBE: AtomicBool = AtomicBool::new(false);
static SKIP_INDEX_INSERT: AtomicBool = AtomicBool::new(false);
static LEGACY_LEAF_OPS: AtomicBool = AtomicBool::new(false);
static SKIP_INDEX_DELETE: AtomicBool = AtomicBool::new(false);

/// Skip PK/UNIQUE constraint checking on INSERT. Duplicate keys are accepted.
pub fn set_skip_unique_probe(on: bool) {
    SKIP_UNIQUE_PROBE.store(on, Ordering::Relaxed);
}

/// Skip B-tree index maintenance on INSERT. Indexes go stale immediately.
pub fn set_skip_index_insert(on: bool) {
    SKIP_INDEX_INSERT.store(on, Ordering::Relaxed);
}

/// Skip B-tree index maintenance on DELETE. Indexes keep entries for rows that
/// no longer exist, so this arm answers "what does leaf deletion cost" and
/// nothing else. `attr_delete` is the only caller.
pub fn set_skip_index_delete(on: bool) {
    SKIP_INDEX_DELETE.store(on, Ordering::Relaxed);
}

/// Use the old decode-and-rewrite B-tree leaf insert instead of the in-place
/// one. Same result, so this arm is safe to run — it is only slower. This is
/// the A/B that justified the in-place rewrite.
pub fn set_legacy_leaf_ops(on: bool) {
    LEGACY_LEAF_OPS.store(on, Ordering::Relaxed);
}

#[inline]
pub fn legacy_leaf_ops() -> bool {
    LEGACY_LEAF_OPS.load(Ordering::Relaxed)
}

/// Counters for the transactional-overlay investigation: how many times a
/// single statement rebuilds the buffered view, and over how many rows.
/// Call sites that rebuild the buffered view, so a single statement's rebuilds
/// can be attributed instead of guessed at.
pub const OVERLAY_SITES: [&str; 6] = [
    "scan",
    "scan_physical",
    "scan_where_eq_positions",
    "scan_limit",
    "other",
    "storage_for_write:before-image",
];

static OVERLAY_CALLS: [AtomicU64; 6] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static OVERLAY_ROWS: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn record_overlay(site: usize, rows: usize) {
    OVERLAY_CALLS[site.min(5)].fetch_add(1, Ordering::Relaxed);
    OVERLAY_ROWS.fetch_add(rows as u64, Ordering::Relaxed);
}

/// (per-site call counts, total rows materialised)
pub fn overlay_counters() -> ([u64; 6], u64) {
    let mut calls = [0u64; 6];
    for (i, c) in OVERLAY_CALLS.iter().enumerate() {
        calls[i] = c.load(Ordering::Relaxed);
    }
    (calls, OVERLAY_ROWS.load(Ordering::Relaxed))
}

pub fn reset_overlay_counters() {
    for c in OVERLAY_CALLS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    OVERLAY_ROWS.store(0, Ordering::Relaxed);
}

/// Plan-cache outcome counters, for attributing per-call query cost.
///
/// A query that re-plans on every execution and one that reuses a cached plan
/// look identical from the outside and differ by tens of microseconds. These
/// say which is happening instead of leaving it to be inferred from a timing.
/// Order: hit+reused, hit-but-replanned, miss (planned from scratch), and
/// not-eligible-for-the-plan-path-at-all.
pub const PLAN_SITES: [&str; 4] = ["reused", "hit_replanned", "miss_planned", "ineligible"];
static PLAN_COUNTS: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

#[inline]
pub fn record_plan(site: usize) {
    PLAN_COUNTS[site.min(3)].fetch_add(1, Ordering::Relaxed);
}

pub fn plan_counters() -> [u64; 4] {
    let mut out = [0u64; 4];
    for (i, c) in PLAN_COUNTS.iter().enumerate() {
        out[i] = c.load(Ordering::Relaxed);
    }
    out
}

pub fn reset_plan_counters() {
    for c in PLAN_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
}

/// Opt-in event log for the SSI conflict graph.
///
/// The serializability oracle is nondeterministic — the same binary on the same
/// seed gives 4 violations in one batch and 1 in the next — so a violation
/// cannot be re-run and inspected. The only way to see the structure that
/// escaped is to have recorded it while it happened. This log is written by
/// every SSI edge-creation and dooming site; the probe resets it per round and
/// dumps it when a round fails to match any serial order.
///
/// Off by default and behind one `Relaxed` load, like the switches above.
/// The mutex is a LEAF lock: never take another lock while holding it, or it
/// inverts against the canonical SSI lock order.
static SSI_TRACE: AtomicBool = AtomicBool::new(false);
static SSI_LOG: parking_lot::Mutex<Vec<String>> = parking_lot::Mutex::new(Vec::new());

pub fn set_ssi_trace(on: bool) {
    SSI_TRACE.store(on, Ordering::Relaxed);
}

#[inline]
pub fn ssi_trace_on() -> bool {
    SSI_TRACE.load(Ordering::Relaxed)
}

/// Record one SSI event. The closure is only called when tracing is on, so a
/// traced call site costs a load and nothing else in a normal run.
#[inline]
pub fn ssi_event(f: impl FnOnce() -> String) {
    if SSI_TRACE.load(Ordering::Relaxed) {
        SSI_LOG.lock().push(f());
    }
}

pub fn take_ssi_log() -> Vec<String> {
    std::mem::take(&mut *SSI_LOG.lock())
}

#[inline]
pub fn skip_unique_probe() -> bool {
    SKIP_UNIQUE_PROBE.load(Ordering::Relaxed)
}

#[inline]
pub fn skip_index_insert() -> bool {
    SKIP_INDEX_INSERT.load(Ordering::Relaxed)
}

#[inline]
pub fn skip_index_delete() -> bool {
    SKIP_INDEX_DELETE.load(Ordering::Relaxed)
}
