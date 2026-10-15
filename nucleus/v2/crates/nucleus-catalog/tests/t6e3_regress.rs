//! C-T6e3 item 4: the pinned-seed regressions and the non-vacuity
//! proofs for the new differential arms — the seeds the item-5
//! mutation report's surfaces are pinned at (G3/G7: the ladder's step
//! ordering; G6/N9: the blind entry delete around a build's scan→ingest
//! gap; G8: the ingest's WAL sync, bound by `t6e3_crash`).
//!
//! - `t6e3_regress_pinned_seeds`: the extended differential (DML index
//!   scans + the DDL index ops) over two pinned seeds, clean, with the
//!   index surface actually exercised — a build published, a scan
//!   served, a scan refused, a resume ran.
//! - `t6e3_regress_a_range_is_pure`: the seeded a-range resolution is
//!   a pure function of its pick, and covers bounded and full scans.
//! - `t6e3_regress_checker_is_not_vacuous`: a clean mini run corrupted
//!   one index-scan record at a time — a served row's value, a served
//!   scan's post-sight (a WriteOnly index), a 0A000 refusal over a
//!   serving pre-sight, a 42704 over a present pre-sight — each is
//!   flagged by the arm it exists to exercise; uncorrupted, the run
//!   checks clean.

mod diff;
// diff/crash.rs (pulled in by diff/mod.rs) drives the t6b crash rig.
mod t6b_support;

use diff::runner::run_workload;
use diff::{a_range, check, Config, Fate, IdxSight, OpDetail, OpOut, Run, Verdict};

/// The pinned seeds (item 4's table: the report's runs are these). The
/// walk continues past them (still pinned, bounded) because two of the
/// coverage surfaces — a scan meeting the ladder mid-flight, and a
/// resume finding a leftover — are interleaving-dependent, and a loaded
/// machine can run both pinned seeds' DML streams to completion before
/// the DDL stream's first unfinished index appears.
const SEEDS: [u64; 2] = [0x7ce3_0001, 0x7ce3_0002];
const WALK_MAX: usize = 8;

/// A small config: enough DML/DDL for every index surface to bite,
/// small enough for a regress binary.
fn mini(seed: u64) -> Config {
    // The soak's shape (4 sessions × 200 programs against 200 DDL
    // txns): the refusal surface needs the DML stream to still be
    // alive while the DDL stream's unfinished indexes are — a smaller
    // shape's DML finishes first and never meets one.
    Config {
        seed,
        threads: 4,
        progs: 200,
        ddl_txns: 200,
        max_attempts: 8,
        deadlock_ms: 5,
    }
}

fn run_mini(seed: u64) -> Run {
    let cfg = mini(seed);
    let run = run_workload(&cfg);
    let verdict = check(&run);
    assert!(
        verdict.is_clean(),
        "seed {seed:#x} is not clean:\n{}",
        verdict.report()
    );
    run
}

/// The extended differential over the pinned seeds: clean, and the
/// online-build surface actually happened — a build published, an index
/// scan was served (the rows oracle's bite) and one was refused by a
/// mid-ladder state, and a resume ran on a leftover.
#[test]
fn t6e3_regress_pinned_seeds() {
    // Every seed in the walk is clean and exercises the deterministic
    // surfaces (a published build, served scans); the interleaving-
    // dependent ones (a refusal or mid-ladder sight, a resume on a
    // leftover) must appear somewhere inside the bounded walk.
    let mut refused = 0usize;
    let mut resumes = 0usize;
    let mut runs = 0usize;
    let walk = SEEDS
        .iter()
        .copied()
        .chain((SEEDS.len()..WALK_MAX).map(|i| 0x7ce3_0000 + i as u64));
    for seed in walk {
        let run = run_mini(seed);
        runs += 1;
        let verdict = check(&run);
        let s = &verdict.stats;
        println!(
            "t6e3 regress seed {seed:#x}: {} builds ok / {} failed, {} resumes, \
             {} drops, {} scans ({} served, {} refused, {} no-target), sights {}/{}",
            s.idx_builds_ok,
            s.idx_builds_failed,
            s.idx_resumes,
            s.idx_drops,
            s.idx_scans,
            s.idx_scans_served,
            s.idx_scans_refused,
            s.idx_scans_noop,
            s.idx_sights_delete_only,
            s.idx_sights_write_only
        );
        assert!(s.idx_builds_ok > 0, "no build published: {s:?}");
        assert!(s.idx_scans_served > 0, "no index scan served: {s:?}");
        assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);
        refused += s.idx_scans_refused + s.idx_sights_delete_only + s.idx_sights_write_only;
        resumes += s.idx_resumes;
        if refused > 0 && resumes > 0 {
            break;
        }
    }
    assert!(
        refused > 0,
        "no scan met the ladder mid-flight in {runs} pinned seeds"
    );
    assert!(resumes > 0, "no resume ran in {runs} pinned seeds");
}

/// The seeded a-range is a pure function of its pick, and the picks
/// cover bounded windows and the full scan.
#[test]
fn t6e3_regress_a_range_is_pure() {
    let mut saw_full = false;
    let mut saw_bounded = false;
    for pick in 0..200u32 {
        let a = a_range(pick);
        let b = a_range(pick);
        assert_eq!(a, b, "pick {pick}");
        match (a, b) {
            ((None, None), _) => saw_full = true,
            ((Some(lo), Some(hi)), _) => {
                saw_bounded = true;
                assert!(lo <= hi, "pick {pick}: {a:?}");
                // Every bounded window stays inside the setup's
                // `a = 1000 + pk` domain (the model's filter's world).
                assert!(
                    (1000..=1009).contains(&lo) && (1000..=1009).contains(&hi),
                    "pick {pick}: {a:?}"
                );
            }
            _ => panic!("pick {pick}: mixed bounds {a:?}"),
        }
    }
    assert!(saw_full && saw_bounded);
}

/// The checker's new arms can fail: a clean mini run, corrupted one
/// index-scan record at a time, is flagged by the arm each corruption
/// exists to exercise.
#[test]
fn t6e3_regress_checker_is_not_vacuous() {
    // The first pinned seed's run reliably records a served scan (the
    // setup's seed index serves every scan that finds it); the loop is
    // a guard, not a retry budget.
    let mut run = None;
    for seed in SEEDS.iter().copied().chain([0x7ce3_0003]) {
        let r = run_mini(seed);
        let has_served = r.history.txns.iter().any(|t| {
            t.ops.iter().any(|op| {
                matches!(
                    (&op.detail, &op.out),
                    (OpDetail::IndexScan { oid: Some(_), .. }, OpOut::RowsT(_))
                )
            })
        });
        if has_served {
            run = Some(r);
            break;
        }
        assert!(seed != 0x7ce3_0003, "no mini run recorded a served scan");
    }
    let run = run.expect("a served scan");
    let verdict = check(&run);
    assert!(verdict.is_clean(), "{}", verdict.report());

    // (a) A served scan's recorded row lies: the value no write derived.
    let mut bad_rows = run.clone();
    'outer: for txn in bad_rows.history.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            if !matches!(
                (&op.detail, &op.out),
                (OpDetail::IndexScan { oid: Some(_), .. }, OpOut::RowsT(_))
            ) {
                continue;
            }
            let rows = match &mut op.out {
                OpOut::RowsT(rows) if !rows.is_empty() => rows,
                _ => continue,
            };
            let Some((_, slot)) = rows[0].1.iter_mut().find(|(_, v)| v.is_some()) else {
                continue;
            };
            *slot = Some(nucleus_codec::Value::Text("\u{0}CORRUPT".into()));
            break 'outer;
        }
    }
    let verdict = check(&bad_rows);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("index scan pk")),
        "the corrupted served row went unnoticed: {}",
        verdict.report()
    );

    // (b) A served scan whose post-sight is a mid-ladder state: §4 says
    // a non-Public index (or built_at > S) must not serve.
    let mut bad_sight = run.clone();
    'outer2: for txn in bad_sight.history.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            if let OpDetail::IndexScan {
                oid: Some(_),
                post: Some(post),
                ..
            } = &mut op.detail
            {
                *post = IdxSight::Seen {
                    state: nucleus_catalog::IndexState::WriteOnly,
                    built_at: 0,
                };
                break 'outer2;
            }
        }
    }
    let verdict = check(&bad_sight);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("served a read at S=")),
        "the non-serving post-sight went unnoticed: {}",
        verdict.report()
    );

    // (c) A refusal over a serving pre-sight: the engine turned an
    // index §4 obliges it to serve away.
    let mut bad_refusal = run.clone();
    'outer3: for txn in bad_refusal.history.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            if let OpDetail::IndexScan {
                oid: Some(_),
                pre,
                post,
                ..
            } = &mut op.detail
            {
                let snap = op.snap;
                *pre = IdxSight::Seen {
                    state: nucleus_catalog::IndexState::Public,
                    built_at: 0,
                };
                *post = None;
                op.out = OpOut::Err("0A000".into());
                let _ = snap;
                break 'outer3;
            }
        }
    }
    let verdict = check(&bad_refusal);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("refused with 0A000")),
        "the unjustified refusal went unnoticed: {}",
        verdict.report()
    );

    // (d) A 42704 over a present pre-sight: the index the bracket saw
    // cannot have vanished under the statement's AccessShare.
    let mut bad_absent = run.clone();
    'outer4: for txn in bad_absent.history.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            if let OpDetail::IndexScan {
                oid: Some(_),
                pre,
                post,
                ..
            } = &mut op.detail
            {
                *pre = IdxSight::Seen {
                    state: nucleus_catalog::IndexState::Public,
                    built_at: 0,
                };
                *post = None;
                op.out = OpOut::Err("42704".into());
                break 'outer4;
            }
        }
    }
    let verdict = check(&bad_absent);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("answered 42704")),
        "the phantom absence went unnoticed: {}",
        verdict.report()
    );

    // The runs these corruptions were built from carry committed
    // index-DDL pseudo-txns (the stats' world), and the base stays
    // clean — re-asserted once more against the untouched clone.
    let v: Verdict = check(&run);
    assert!(v.is_clean(), "{}", v.report());
    let _ = Fate::Committed(0);
}
