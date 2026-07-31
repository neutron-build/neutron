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

use std::sync::atomic::{AtomicBool, Ordering};

static SKIP_UNIQUE_PROBE: AtomicBool = AtomicBool::new(false);
static SKIP_INDEX_INSERT: AtomicBool = AtomicBool::new(false);
static LEGACY_LEAF_OPS: AtomicBool = AtomicBool::new(false);

/// Skip PK/UNIQUE constraint checking on INSERT. Duplicate keys are accepted.
pub fn set_skip_unique_probe(on: bool) {
    SKIP_UNIQUE_PROBE.store(on, Ordering::Relaxed);
}

/// Skip B-tree index maintenance on INSERT. Indexes go stale immediately.
pub fn set_skip_index_insert(on: bool) {
    SKIP_INDEX_INSERT.store(on, Ordering::Relaxed);
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

#[inline]
pub fn skip_unique_probe() -> bool {
    SKIP_UNIQUE_PROBE.load(Ordering::Relaxed)
}

#[inline]
pub fn skip_index_insert() -> bool {
    SKIP_INDEX_INSERT.load(Ordering::Relaxed)
}
