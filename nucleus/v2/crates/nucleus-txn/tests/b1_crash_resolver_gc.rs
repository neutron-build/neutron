//! C-B1a crash matrix: for every job-probe point in
//! `{ResolverAfterRemoval, ResolverBeforeTruncation,
//! ResolverAfterTruncation, GcAfterPublish, GcAfterWatermarkSync,
//! GcAfterTombstones, RetentionAfterPass}` × every occurrence × every
//! `keep` in `0..=unsynced` (looping until a kill keeps fewer than
//! asked), the workload is driven by hand (`Resolver::run_once`,
//! `GcJob::run_once`, one retention pass per round) until the point
//! fires and kills the KV; everything is dropped, the KV revived and
//! reopened, and the invariants re-derived:
//!
//! - before recovery: I1 (every acked `On` commit's keys read their
//!   value), I2 (each txn all-or-nothing, lost `Off` commits a suffix),
//!   I3 (aborted writes never visible), I5 (the reopened `/sys/gc_w` ≥
//!   every watermark the KV was handed);
//! - after the hand recovery: I4 (no intent remains), I6 (no `/sys/txn`
//!   record remains), I7 (I-GC-QUIESCE: no newest-`<= W` tombstone).
//!
//! Every listed point must fire at least once across the matrix. The
//! mutants this file kills (card item 11): M-SPLIT (a removal split into
//! two batches — some `keep` keeps the intent delete and drops the
//! version), M-S10 (truncation before removal + eligibility without the
//! count — a `/sys/txn` record dies while its intent lives), M-NOSYNC
//! (`/sys/gc_w` written `Durability::No` — I5), M-MEMO (tombstones
//! dropped only when `publish` raised `W` — I7).

mod b1_support;

use std::sync::Arc;

use b1_support::{
    build_workload, check_after_reopen, ok, recover, CrashAtPoint, CrashState, KillKv, Workload,
};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::jobs::JobPoint;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::ssi::{RetentionJob, Ssi};

/// The points of the matrix (SlotStep is a test-job point; it has no body
/// here).
const POINTS: [JobPoint; 7] = [
    JobPoint::ResolverAfterRemoval,
    JobPoint::ResolverBeforeTruncation,
    JobPoint::ResolverAfterTruncation,
    JobPoint::GcAfterPublish,
    JobPoint::GcAfterWatermarkSync,
    JobPoint::GcAfterTombstones,
    JobPoint::RetentionAfterPass,
];

/// How many resolver/GC/retention rounds one drive runs. The workload is
/// deterministic: every removal, truncation and tombstone happens in the
/// first two rounds, so a point that has not fired by this bound does not
/// exist at this occurrence.
const ROUNDS: usize = 6;

enum Outcome {
    /// The point never reached this occurrence (with the KV never killed).
    NeverFired,
    /// The kill fired and kept this many unsynced batches.
    Kept(usize),
}

/// One matrix cell: build the workload, drive it by hand until the point
/// fires (or cannot), crash-reopen, check, recover, check again.
fn scenario(point: JobPoint, occurrence: u64, keep: usize, fired_any: &mut bool) -> Outcome {
    let workload = build_workload();
    let kv: KillKv<MemKv> = workload.kv.clone();
    let state = CrashState::new();
    let probe = CrashAtPoint::new(
        workload.kv.clone(),
        point,
        occurrence,
        keep,
        Arc::clone(&state),
    );
    let core = Arc::clone(&workload.core);
    core.set_job_probe(Arc::new(probe));
    let ssi = Ssi::install(&core);
    let gc = ok(GcJob::install(&core, GcConfig::default()));
    let retention = RetentionJob::new(Arc::clone(&core), Arc::clone(&ssi));

    // Drive: one resolver round, one GC round, one retention pass per
    // iteration, all on this thread. Errors after the kill fired are the
    // crash; an error with the probe not yet fired is a determinism break
    // that silently truncates the matrix (the C-B1a gate's M3): it
    // panics instead of returning NeverFired.
    for _ in 0..ROUNDS {
        if let Err(e) = Resolver::run_once(&core) {
            if state.fired() {
                break;
            }
            panic!(
                "drive error at {point:?} occurrence {occurrence} before the probe fired: {e:?}"
            );
        }
        if state.fired() {
            break;
        }
        if let Err(e) = gc.run_once(0) {
            if state.fired() {
                break;
            }
            panic!(
                "drive error at {point:?} occurrence {occurrence} before the probe fired: {e:?}"
            );
        }
        retention.run_once();
        if state.fired() {
            break;
        }
    }
    if !state.fired() {
        // The workload quiesced without reaching this occurrence. The
        // jobs, the probe and the core go away before the next cell.
        drop(gc);
        drop(retention);
        drop(ssi);
        drop(core);
        return Outcome::NeverFired;
    }
    *fired_any = true;
    let kept = state.kept().unwrap_or(0);

    // Drop the KV-holding pieces (the GC job's install slot first, then
    // the core and the workload's own reference), revive, reopen. The
    // model is plain data and moves on.
    let Workload {
        kv: _,
        core: wl_core,
        model,
    } = workload;
    drop(gc);
    drop(retention);
    drop(ssi);
    drop(core);
    drop(wl_core);
    assert!(
        kv.killed(),
        "the probe fired but the KV was not killed (occurrence {occurrence})"
    );
    kv.revive();
    let core2 = Arc::new(ok(Core::open(kv.clone())));
    check_after_reopen(&model, &core2, &kv);
    let gc2 = ok(GcJob::install(&core2, GcConfig::default()));
    recover(&core2, &gc2);
    drop(gc2);
    drop(core2);
    Outcome::Kept(kept)
}

#[test]
fn crash_matrix_resolver_gc() {
    let start = std::time::Instant::now();
    for point in POINTS {
        let mut fired_any = false;
        'occurrences: for occurrence in 1.. {
            for keep in 0.. {
                match scenario(point, occurrence, keep, &mut fired_any) {
                    // The occurrence does not exist: on to the next point.
                    Outcome::NeverFired => break 'occurrences,
                    Outcome::Kept(kept) => {
                        if kept < keep {
                            // Fewer unsynced batches than asked: this
                            // occurrence's keep space is exhausted.
                            break;
                        }
                    }
                }
            }
        }
        assert!(fired_any, "{point:?} never fired across the whole matrix");
    }
    assert!(
        start.elapsed() < std::time::Duration::from_secs(30),
        "the crash matrix took {:?}",
        start.elapsed()
    );
}
