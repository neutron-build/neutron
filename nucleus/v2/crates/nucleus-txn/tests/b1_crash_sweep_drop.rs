//! C-B1b crash matrix: the C-B1a harness (workload → probe-driven jobs →
//! kill → reopen → recover → invariants) extended with a committed DROP
//! and TRUNCATE in the workload, their retired-storage rows, and the two
//! new jobs. For every point in `{SweepAfterRemoval, SweepBeforeRecord,
//! RetireAfterRange, RetireBeforeMarkerDelete, RetireAfterMarkerDelete}`
//! × every occurrence × every `keep` (looping until a kill keeps fewer
//! than asked):
//!
//! 1. the workload is built single-threaded (its commits all block);
//! 2. a first crash keeps everything and the store is reopened — that is
//!    what makes the sweep's and the cleanup's points reachable (their
//!    subjects are older-epoch intents and already-committed DDL rows);
//! 3. recovery is driven by hand on the reopened core with the probe
//!    armed — sweep, resolver, GC and cleanup rounds, plus a spawned
//!    commit thread (the cleanup's marker delete commits through it) —
//!    until the point fires and kills the KV;
//! 4. everything is dropped, the KV revived, reopened, and the recovery
//!    driven again to quiescence;
//! 5. the invariants are re-derived: before the recovery I1 (every acked
//!    `On` commit's keys read their value), I2 (each txn
//!    all-or-nothing, lost `Off` commits a suffix), I3 (aborted writes
//!    never visible), I5 (the reopened `/sys/gc_w` ≥ every watermark the
//!    KV was handed); after it I4 (no intent remains), I6 (no `/sys/txn`
//!    record remains), I7 (no newest-`<= W` tombstone) and I8 (every
//!    retired range raw-empty, no retired row left).
//!
//! Keys inside a retired range are excluded from the I1/I2 value checks
//! (a dropped relation's data is deleted by design — I8 is their check).
//!
//! Every listed point must fire at least once across the matrix. The
//! mutants this file kills (card item 7): DC-ORDER (the marker row
//! deleted durably **before** the ranges are retired — some `keep` keeps
//! the delete and drops the `DeleteRange`, and I8 finds a non-empty
//! range with no row left to re-run), plus the crash-restart halves of
//! the sweep and cleanup rules.

mod b1_support;

use std::sync::Arc;

use b1_support::{
    build_ddl_workload, check_after_reopen, check_quiesced, check_retired_cleaned, drive_recovery,
    ok, reopen_rig, CrashAtPoint, CrashState, KillKv,
};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::jobs::JobPoint;

/// The points of the matrix (card item 6).
const POINTS: [JobPoint; 5] = [
    JobPoint::SweepAfterRemoval,
    JobPoint::SweepBeforeRecord,
    JobPoint::RetireAfterRange,
    JobPoint::RetireBeforeMarkerDelete,
    JobPoint::RetireAfterMarkerDelete,
];

enum Outcome {
    /// The point never reached this occurrence (with the KV never killed).
    NeverFired,
    /// The kill fired and kept this many unsynced batches.
    Kept(usize),
}

/// One matrix cell: build the workload, crash-and-reopen once (making
/// the sweep/cleanup subjects live), drive the recovery with the probe
/// armed until the point fires, crash-reopen again, recover, check.
fn scenario(
    point: JobPoint,
    occurrence: u64,
    keep: usize,
    fired_any: &mut bool,
    cell: usize,
) -> Outcome {
    let (rig, model) = build_ddl_workload();
    let kv: KillKv<MemKv> = rig.kv.clone();

    // The first crash keeps everything: it is only there to end the
    // workload's epoch (the reopened core sees the intents and rows as
    // older-epoch / already-committed state).
    kv.kill(usize::MAX);
    let rig2 = reopen_rig(rig);
    let core2: Arc<Core<KillKv<MemKv>>> = Arc::clone(&rig2.core);

    let state = CrashState::new();
    let probe = CrashAtPoint::new(kv.clone(), point, occurrence, keep, Arc::clone(&state));
    core2.set_job_probe(Arc::new(probe));

    let gc2 = Arc::new(ok(GcJob::install(&core2, GcConfig::default())));
    drive_recovery(&core2, &gc2, None, Some(&state), cell);
    drop(gc2);
    if !state.fired() {
        // The recovery quiesced without reaching this occurrence; the
        // rig, the probe and the core go away before the next cell.
        drop(core2);
        drop(rig2);
        return Outcome::NeverFired;
    }
    *fired_any = true;
    let kept = state.kept().unwrap_or(0);

    // Drop the KV-holding pieces, revive, reopen, recover to quiescence.
    drop(core2);
    drop(rig2);
    assert!(
        kv.killed(),
        "the probe fired but the KV was not killed (occurrence {occurrence})"
    );
    kv.revive();
    let rig3 = b1_support::B1Rig::over(kv.clone());
    let core3: Arc<Core<KillKv<MemKv>>> = Arc::clone(&rig3.core);
    check_after_reopen(&model, &core3, &kv);
    let gc3 = Arc::new(ok(GcJob::install(&core3, GcConfig::default())));
    drive_recovery(&core3, &gc3, None, None, cell + 1000);
    check_quiesced(&core3);
    check_retired_cleaned(&core3, &kv, &model);
    drop(gc3);
    drop(core3);
    drop(rig3);
    Outcome::Kept(kept)
}

#[test]
fn crash_matrix_sweep_drop() {
    let start = std::time::Instant::now();
    let mut cell = 0usize;
    for point in POINTS {
        let mut fired_any = false;
        'occurrences: for occurrence in 1.. {
            for keep in 0.. {
                cell += 1;
                match scenario(point, occurrence, keep, &mut fired_any, cell) {
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
        start.elapsed() < std::time::Duration::from_secs(60),
        "the crash matrix took {:?} over {} cells",
        start.elapsed(),
        cell
    );
}
