//! C-T6g soak (card item 4): 3 fixed seeds × 4 sessions (3 DML at the
//! run's level + the DDL session) × 150 txns each, at each of RC, RR and
//! SER — every level a session claims is checked clean by the unchanged
//! G2 checker re-used verbatim from `nucleus-txn`'s test tree. The size
//! is an iteration count, never a wall-clock bound; timings are printed,
//! never asserted (the whole suite is calibrated to stay under 60 s in
//! a debug build).
//!
//! The DDL session runs the card's mix — ADD COLUMN, TRUNCATE (recorded
//! as `Del` of every live slot) and one online
//! `CREATE INDEX CONCURRENTLY` on the mirror column `m = pk` — so the
//! seeded G2 histories run against concurrent relation rewrites and an
//! index that serves reads only once published (§4's usability rule),
//! with reads going through the published index whenever the engine
//! will serve them.
//!
//! Asserted per run (the engine's promises, judged by the checker):
//!
//! - **RC**: never G0/G1a/G1b/G1c, no dirty writes, no garbage — and
//!   the same history judged at SERIALIZABLE shows cycles (the soak can
//!   fail).
//! - **RR**: additionally never a lost update, never a cycle without
//!   two adjacent rw edges, never a non-repeatable read; write skew
//!   (SER-level) may remain.
//! - **SER**: serializable — no anomaly at all — and every 40001
//!   explained.
//!
//! plus, at every level, no intent leaks once the resolver drains
//! (I-LEAK), and the surface floors: truncates and ADD COLUMNs
//! committed, the build published, index scans both served and refused
//! (the §4 gate observed mid-ladder).

mod g2x;

use std::time::Instant;

use g2x::g2::check::{check, Kind, Verdict};
use g2x::g2::history::Level;
use g2x::runner::{run_workload, Bug, Config, Counts, Run};

/// The soak's three fixed seeds.
const SEEDS: [u64; 3] = [0x6733_0001, 0x6733_0002, 0x6733_0003];

fn run_one(level: Level, seed: u64) -> (Run, Verdict) {
    let cfg = Config::new(seed, level);
    let t0 = Instant::now();
    let run = run_workload(&cfg);
    let t1 = Instant::now();
    let verdict = check(&run.history, level);
    // Reported, never asserted.
    println!(
        "t6g soak {} seed {seed:#x}: run {:?}, {} txns, {} committed; \
         truncates {}, adds {}, builds ok/failed {}/{}, iscan served/refused {}/{}",
        level.short(),
        t1 - t0,
        verdict.stats.txns,
        verdict.stats.committed,
        run.counts.truncates_committed,
        run.counts.add_columns_committed,
        run.counts.builds_ok,
        run.counts.builds_failed,
        run.counts.index_scans_served,
        run.counts.index_scan_fallbacks,
    );
    (run, verdict)
}

/// Fails the test with the whole trace of the seed when `verdict` has a
/// violation or the run leaked.
fn assert_sound(level: Level, seed: u64, run: &Run, verdict: &Verdict) {
    let violations = verdict.violations();
    if !violations.is_empty() || run.leaked_intents != 0 {
        eprintln!(
            "t6g soak FAILED: level {} seed {seed:#x}\n{}",
            level.short(),
            run.history.trace()
        );
        panic!(
            "level {} seed {seed:#x}: {} violation(s), {} leaked intent(s) \
             (full trace on stderr)\n{}{}",
            level.short(),
            violations.len(),
            run.leaked_intents,
            run.leak_report,
            verdict.report(&run.history)
        );
    }
}

#[test]
fn t6g_soak() {
    let mut agg = Counts::default();
    let mut rc_cycles = 0usize;
    for level in [
        Level::ReadCommitted,
        Level::RepeatableRead,
        Level::Serializable,
    ] {
        for seed in SEEDS {
            let (run, verdict) = run_one(level, seed);
            assert_sound(level, seed, &run, &verdict);
            // The level's own promises.
            match level {
                Level::ReadCommitted => {
                    // The soak can fail: the same history is not
                    // serializable.
                    if check(&run.history, Level::Serializable).has_cycle() {
                        rc_cycles += 1;
                    }
                }
                Level::RepeatableRead => {
                    for a in &verdict.anomalies {
                        assert!(
                            matches!(a.kind, Kind::WriteSkew | Kind::G2Item),
                            "unexpected RR anomaly {:?}",
                            a.kind
                        );
                    }
                }
                Level::Serializable => {
                    assert!(
                        verdict.serializable(),
                        "SER run not serializable:\n{}",
                        verdict.report(&run.history)
                    );
                    assert_eq!(verdict.stats.f_unexplained, 0);
                }
            }
            // The run really exercised the surfaces under test.
            assert!(
                verdict.stats.committed > 100,
                "too few commits: {:?}",
                verdict.stats
            );
            assert!(
                run.counts.truncates_committed > 10,
                "too few truncates: {:?}",
                run.counts
            );
            let c = run.counts;
            agg.truncates_committed += c.truncates_committed;
            agg.add_columns_committed += c.add_columns_committed;
            agg.builds_ok += c.builds_ok;
            agg.builds_failed += c.builds_failed;
            agg.index_scans_served += c.index_scans_served;
            agg.index_scan_fallbacks += c.index_scan_fallbacks;
        }
    }
    // The aggregate floors (per-seed fates depend on interleavings; the
    // aggregate is many times the bar). The §4 refusal arm (an IndexScan
    // answered by the pk scan because the ladder had not published) has
    // no floor here — it is a mid-flight race — and is pinned
    // deterministically by `t6g_regress`'s hand-scheduled history.
    assert!(agg.builds_ok >= 7, "too few published builds: {agg:?}");
    assert!(
        agg.add_columns_committed > 20,
        "too few ADD COLUMNs: {agg:?}"
    );
    assert!(
        agg.index_scans_served > 5,
        "too few served index scans: {agg:?}"
    );
    assert!(rc_cycles >= 1, "no RC run showed cycles at SER");
}

/// An RR claim on a store whose IndexScan statements take a fresh
/// snapshot: the soak notices (the threaded end-to-end half of inject
/// (a)'s deterministic proof).
#[test]
fn t6g_soak_broken_store_is_caught() {
    let mut cfg = Config::new(0x6733_00ff, Level::RepeatableRead);
    cfg.bug = Bug::IndexScanFreshSnapshot;
    let run = run_workload(&cfg);
    let verdict = check(&run.history, Level::RepeatableRead);
    assert!(
        !verdict.violations().is_empty(),
        "the broken store went unnoticed: {:?}",
        verdict.stats
    );
}
