//! C-B1c's crash-point gate: the full C-B1b workload (data commits On/Off,
//! an abort, a DROP, a TRUNCATE, a deleted key, older-epoch intents from a
//! first crash) plus the M4 tail (one `Off` commit after the last `On`, so
//! I2's lost-`Off`-suffix branch is reachable — a cell that loses it is
//! recorded). For **every** [`JobPoint::ALL`] (SlotStep through the §9.3
//! slot test job) × occurrences 1..=3 × every `keep` (looping until a kill
//! keeps fewer than asked):
//!
//! 1. the workload is built single-threaded, then crashed once with the
//!    cell's `keep` — ending its epoch (the sweep's and cleanup's
//!    subjects are older-epoch intents and already-committed DDL rows)
//!    and, at small `keep`, losing the tail's unsynced record;
//! 2. recovery is driven by hand on the reopened core with the probe
//!    armed — sweep, resolver, GC (publish + chunked tombstone pass), SSI
//!    retention and drop-cleanup rounds — until the point fires and kills
//!    the KV (a drive error before the probe fired is a determinism
//!    break and panics, the C-B1a gate's M3 rule);
//! 3. everything is dropped, the KV revived, reopened; the pre-recovery
//!    invariants I1 (every acked `On` commit's keys read their value),
//!    I2 (each txn all-or-nothing, lost `Off` commits a suffix — the M4
//!    cell record), I3 (aborted writes never visible) and I5 (the
//!    reopened `/sys/gc_w` ≥ every handed watermark) are re-derived;
//! 4. recovery is driven again to quiescence, one compaction with the §10
//!    filter runs over the live store, and the post-recovery checks
//!    follow: I4 (no intent remains), I6 (no `/sys/txn` record), I7
//!    (I-GC-QUIESCE, the catalog included) and I8 (every retired range
//!    raw-empty, no retired row left).
//!
//! Every point must fire at least once across the matrix (X-POINT: a
//! deleted probe call surfaces here). The gate must finish under 60 s in
//! debug.

mod b1_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use b1_support::{
    build_gate_workload, check_after_reopen, check_quiesced, check_retired_cleaned, compact_all,
    drive_recovery, lost_off_txns, ok, reopen_rig, CrashAtPoint, CrashState, KillKv, SlotJob,
};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::jobs::runner::JobSet;
use nucleus_txn::jobs::{JobCtx, JobKind, JobOutcome, JobPoint};
use nucleus_txn::ssi::RetentionJob;

/// The gate's point list: every `JobPoint`, none dropped (the card's
/// "shrink the workload, never the point list").
const POINTS: [JobPoint; 13] = JobPoint::ALL;

/// How many occurrences of each point the matrix drives (the card's
/// 1..=3).
const MAX_OCCURRENCE: u64 = 3;

enum Outcome {
    /// The point never reached this occurrence (with the KV never killed).
    NeverFired,
    /// The kill fired and kept this many unsynced batches.
    Kept(usize),
}

fn wait_until(deadline_secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(deadline_secs),
            "{what} did not happen within {deadline_secs}s"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// One matrix cell (see the module docs). `lost_off_cells` collects the
/// M4 record: cells where an `Off` commit was lost and I2 checked it.
fn scenario(
    point: JobPoint,
    occurrence: u64,
    keep: usize,
    fired_any: &mut bool,
    lost_off_cells: &mut Vec<String>,
    cell: usize,
) -> Outcome {
    let (rig, model) = build_gate_workload();
    let kv: KillKv<MemKv> = rig.kv.clone();

    // The first crash, with this cell's keep: it ends the workload's
    // epoch and, at a small keep, drops the tail `Off` commit's unsynced
    // batches (its intent and record are the last unsynced writes).
    kv.kill(keep);
    let rig2 = reopen_rig(rig);
    let core2: Arc<Core<KillKv<MemKv>>> = Arc::clone(&rig2.core);

    let state = CrashState::new();
    let probe = CrashAtPoint::new(kv.clone(), point, occurrence, keep, Arc::clone(&state));
    core2.set_job_probe(Arc::new(probe));

    if point == JobPoint::SlotStep {
        // SlotStep belongs to no crate body: drive it with the §9.3 slot
        // test job on a real runner (the card's "via the test job"). The
        // probe fires every step; the kill lands inside the runner's
        // step, exactly as for the hand-driven kinds. The wait is on the
        // STEP COUNT (not the probe): `occurrence + 2` steps guarantee
        // the occurrence was hit if its probe call exists, so a deleted
        // call surfaces as the gate's fired assertion, not this wait.
        let steps = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut set = JobSet::new(Arc::clone(&core2));
        {
            let steps = Arc::clone(&steps);
            ok(set.start(
                Box::new(SlotJob::new(JobKind::Merge, move |cx: &JobCtx| {
                    steps.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    cx.probe(JobPoint::SlotStep);
                    Ok(JobOutcome::Idle)
                })),
                nucleus_txn::jobs::JobConfig {
                    idle_interval: Duration::from_millis(1),
                },
            ));
        }
        wait_until(5, "the slot job ran its steps", || {
            steps.load(std::sync::atomic::Ordering::SeqCst) >= occurrence + 2
        });
        ok(set.shutdown());
    } else {
        let gc2 = Arc::new(ok(GcJob::install(&core2, GcConfig::default())));
        let retention = RetentionJob::new(Arc::clone(&core2), Arc::clone(&rig2.ssi));
        drive_recovery(&core2, &gc2, Some(&retention), Some(&state), cell);
        drop(retention);
        drop(gc2);
    }
    if !state.fired() {
        // The recovery quiesced without reaching this occurrence; the
        // rig, the probe and the core go away before the next cell.
        drop(core2);
        drop(rig2);
        return Outcome::NeverFired;
    }
    *fired_any = true;
    let kept = state.kept().unwrap_or(0);

    // Drop the KV-holding pieces, revive, reopen.
    drop(core2);
    drop(rig2);
    assert!(
        kv.killed(),
        "the probe fired but the KV was not killed (occurrence {occurrence})"
    );
    kv.revive();
    let rig3 = b1_support::B1Rig::over(kv.clone());
    let core3: Arc<Core<KillKv<MemKv>>> = Arc::clone(&rig3.core);

    // Pre-recovery invariants, and the M4 record: a cell that lost an
    // `Off` commit (I2's suffix rule checks it right after).
    let lost = lost_off_txns(&model, &core3);
    check_after_reopen(&model, &core3, &kv);
    if !lost.is_empty() {
        lost_off_cells.push(format!(
            "{point:?} occurrence {occurrence} keep {keep} (probe kept {kept}): lost {:?}",
            lost
        ));
    }

    // Recover to quiescence (sweep, resolver, GC + compaction, drop
    // cleanup; the rig's commit thread serves the marker deletes), then
    // the post-recovery checks.
    let gc3 = Arc::new(ok(GcJob::install(&core3, GcConfig::default())));
    let retention3 = RetentionJob::new(Arc::clone(&core3), Arc::clone(&rig3.ssi));
    drive_recovery(&core3, &gc3, Some(&retention3), None, cell + 1000);
    drop(retention3);
    let dropped = compact_all(&kv);
    let _ = dropped;
    check_quiesced(&core3);
    check_retired_cleaned(&core3, &kv, &model);
    drop(gc3);
    drop(core3);
    drop(rig3);
    Outcome::Kept(kept)
}

#[test]
fn crash_gate_over_every_job_point() {
    let start = std::time::Instant::now();
    let mut cell = 0usize;
    let mut lost_off_cells: Vec<String> = Vec::new();
    for point in POINTS {
        let mut fired_any = false;
        'occurrences: for occurrence in 1..=MAX_OCCURRENCE {
            for keep in 0.. {
                cell += 1;
                match scenario(
                    point,
                    occurrence,
                    keep,
                    &mut fired_any,
                    &mut lost_off_cells,
                    cell,
                ) {
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
        assert!(
            fired_any,
            "{point:?} never fired across the whole gate (X-POINT)"
        );
    }
    // M4: the lost-`Off`-suffix branch was reachable and I2 checked it.
    assert!(
        !lost_off_cells.is_empty(),
        "no cell lost an Off commit: I2's lost-suffix branch never ran"
    );
    println!(
        "crash gate: {} cells, first lost-Off cell: {}",
        cell, lost_off_cells[0]
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(60),
        "the crash gate took {elapsed:?} over {cell} cells"
    );
}
