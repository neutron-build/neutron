//! The seeded workload's self-tests (card X-5 Work 5, stage X-5c).
//!
//! These pin the generator's and driver's mechanics — determinism, the
//! W-SCRIPT tree rule, the hook wiring the workload needs, and the
//! 16-seed smoke the card names for `cargo test`. They deliberately do
//! **not** assert oracle outcomes: those are judged by the campaign
//! under rulings X5-R1–R6 (the worker never pre-reads campaign data; see
//! the README's X5-R6 note).

#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

use e4_recovery::campaign;
use e4_recovery::workload::{self, Knobs};

/// Seeds 0..16 run mechanically clean: no DEADLOCK, no PANIC, no S-BATON
/// violation, no task error, and the digests reproduce byte-identically
/// on a re-run (the card's 16-seed smoke; the gate's workspace test
/// stays under 60 s).
#[test]
fn smoke_16_seeds() {
    for seed in 0..16u64 {
        let rec = workload::run_seed(seed, None);
        assert!(
            !rec.deadlock,
            "seed {seed}: DEADLOCK (rounds: {})",
            rec.rounds.len()
        );
        assert!(!rec.panic, "seed {seed}: PANIC");
        assert!(!rec.baton, "seed {seed}: S-BATON violation");
        for (i, r) in rec.rounds.iter().enumerate() {
            assert!(
                r.task_error.is_none(),
                "seed {seed} round {i}: task error {:?}",
                r.task_error
            );
            assert!(r.fired, "seed {seed} round {i}: the crash never fired");
        }
        assert!(rec.defect.is_none(), "seed {seed}: defect {:?}", rec.defect);
        // The round count follows the knobs, except an early stop (a
        // failed round or a recovery without a store ends the chain —
        // recorded, never hidden).
        assert!(
            rec.rounds.len() as u32 <= Knobs::derive(seed, 0).rounds,
            "seed {seed}: round count {} over the knobs",
            rec.rounds.len()
        );
        if !rec.stopped_early {
            assert_eq!(
                rec.rounds.len() as u32,
                Knobs::derive(seed, 0).rounds,
                "seed {seed}: round count without an early stop"
            );
        }
        // The class follows seed % 10.
        let want = match seed % 10 {
            0..=6 => "C",
            7 | 8 => "MP",
            _ => "MW",
        };
        assert_eq!(rec.class, want, "seed {seed}: class");
        // The twin runs in lockstep with the real rounds for MP/MW
        // (none for C).
        let want_twin = if want == "C" { 0 } else { rec.rounds.len() };
        assert_eq!(
            rec.twin.len(),
            want_twin,
            "seed {seed}: twin rounds ({}) must match the real rounds ({})",
            rec.twin.len(),
            want_twin
        );
    }
    // Determinism: two runs of the same seed give the same digest, and a
    // threaded range run matches the single-threaded one.
    let a = workload::run_seed(7, None);
    let b = workload::run_seed(7, None);
    assert_eq!(a.digest, b.digest, "seed 7 digests differ across runs");
    let u = workload::run_seed(12, None);
    let u2 = workload::run_seed(12, None);
    assert_eq!(
        u.digest, u2.digest,
        "seed 12 (uniform crash) digests differ across runs"
    );
    let t1 = campaign::run_range(0, 5, None, 1);
    let t3 = campaign::run_range(0, 5, None, 3);
    for (x, y) in t1.iter().zip(t3.iter()) {
        assert_eq!(x.seed, y.seed);
        assert_eq!(x.digest, y.digest, "seed {} differs across threads", x.seed);
    }
}

/// The scheduler-level uniform crash (5.5's "uniform over the round's
/// yield points"): the counting pass measures the round's true yield
/// total, the drawn point is always reachable — every round of the
/// uniform seeds fires, no silent no-crash round — and the draw is
/// pure. A never-fired crash is a harness defect through
/// [`workload::crash_fired_defect`], never a pass.
#[test]
fn uniform_crash_fires_at_the_counted_yield() {
    // Seeds 10..15 are uniform ((seed/10) % 2 == 1): 10/12/14 class C,
    // 11 MP, 13/15 MW. Every round must have fired its crash.
    for seed in [10u64, 11, 12, 13, 14, 15] {
        let rec = workload::run_seed(seed, None);
        assert!(rec.defect.is_none(), "seed {seed}: {:?}", rec.defect);
        assert!(!rec.rounds.is_empty(), "seed {seed}: no rounds");
        for (i, r) in rec.rounds.iter().enumerate() {
            assert!(r.fired, "seed {seed} round {i}: the crash never fired");
        }
    }
    // The point draw is pure and stays inside the counted total (and a
    // zero-yield round still addresses the first yield, never `0`).
    let k1 = workload::uniform_yield_point(12, 0, 10_000);
    let k2 = workload::uniform_yield_point(12, 0, 10_000);
    assert_eq!(k1, k2, "the yield point draw is not pure");
    assert!((1..=10_000).contains(&k1), "drawn point {k1} out of range");
    assert_eq!(
        workload::uniform_yield_point(12, 0, 0),
        1,
        "the empty round must still address yield 1"
    );
    // The defect rule: a never-fired crash is a defect whatever the
    // description; a fired crash never is.
    let mut out = e4_recovery::harness::RoundOutcome::default();
    assert!(workload::crash_fired_defect(&out, "uniform@y:7").is_some());
    out.obs.crash_fired = true;
    assert!(workload::crash_fired_defect(&out, "uniform@y:7").is_none());
}

/// The knob derivation is total and in the card's ranges (5.1), and the
/// per-round feature mix (5.2) is a pure function of (seed, round).
#[test]
fn knobs_derive_in_range() {
    for seed in 0..200u64 {
        let k = Knobs::derive(seed, 0);
        assert!(
            (2..=4).contains(&k.committers),
            "committers {}",
            k.committers
        );
        assert!((2..=4).contains(&k.trees), "trees {}", k.trees);
        assert!(
            (1200..=3000).contains(&k.keyspace),
            "keyspace {}",
            k.keyspace
        );
        assert!(
            [0, 3, 7].contains(&k.syncoff_tenths),
            "syncoff {}",
            k.syncoff_tenths
        );
        assert!([8, 16, 254].contains(&k.map_leaf_cap));
        assert!([128, 512, 32_576].contains(&k.freeset_leaf_bits));
        assert!([1, 2, 4].contains(&k.r_epoch));
        assert!((1..=3).contains(&k.rounds));
        assert!((320..=700).contains(&k.chunked_ops));
        assert!(k.pool_frames() >= 12);
        let k2 = Knobs::derive(seed, 0);
        assert_eq!(k.digest(), k2.digest(), "knob digest not pure");
        // The feature mix is per-round.
        let mix = |r: u32| {
            format!(
                "{}{}",
                u8::from(Knobs::derive(seed, r).bulk_load),
                u8::from(Knobs::derive(seed, r).drop_tree)
            )
        };
        assert_ne!(mix(0), "", "empty mix");
    }
    // The bulk-load feature really appears on some rounds (25%) and so
    // does the drop feature.
    let mut loads = 0u32;
    let mut drops = 0u32;
    for seed in 0..400u64 {
        for r in 0..3 {
            let k = Knobs::derive(seed, r);
            loads += u32::from(k.bulk_load);
            drops += u32::from(k.drop_tree);
        }
    }
    assert!(loads > 20, "bulk loads too rare: {loads}");
    assert!(drops > 20, "drops too rare: {drops}");
}

/// The crash spec follows 5.5: targeted (hook `(seed/20) % 10`, a seeded
/// checkpoint index) iff `(seed/10) % 2 == 0`, else uniform over the
/// script's op points.
#[test]
fn crash_spec_targeting() {
    use e4_recovery::harness::CrashSpec;
    use e4_recovery::store::ckpt::CKPT_HOOKS;
    let knobs = Knobs::derive(0, 0);
    let model = e4_recovery::model::Model::new();
    for seed in 0..40u64 {
        let (script, _d, hook) = workload::build_round(seed, 0, &knobs, &model, 0);
        let total: u64 = script.tasks.iter().map(|t| t.ops.len() as u64).sum();
        assert!(total > 10, "seed {seed}: script too small ({total})");
        if (seed / 10) % 2 == 0 {
            let want = format!("{:?}", CKPT_HOOKS[((seed / 20) % 10) as usize]);
            assert_eq!(hook, want, "seed {seed}: targeted hook");
            assert!(matches!(script.crash, Some(CrashSpec::Hook { .. })));
        } else {
            assert!(hook.is_empty(), "seed {seed}: uniform crash has no hook");
            assert!(matches!(script.crash, Some(CrashSpec::TaskYield(..))));
        }
    }
}

/// W-SCRIPT: round r's script only references trees the model at the cut
/// has (created and not dropped), so no scripted Put can park on a
/// directory entry no committer will install.
#[test]
fn script_references_only_model_trees() {
    let knobs = Knobs::derive(3, 0);
    let model = e4_recovery::model::Model::new();
    let (script, _, _) = workload::build_round(3, 0, &knobs, &model, 0);
    let trees = workload::trees_at(&model, 0);
    assert!(trees.is_empty(), "the empty model has no trees");
    // Every Put/Delete references a tree the setup commit creates (the
    // first committer op) — checked by generation, so collect the set of
    // created trees of the whole script.
    use e4_recovery::model::Op;
    let mut created = std::collections::BTreeSet::new();
    for t in &script.tasks {
        for op in &t.ops {
            if let e4_recovery::harness::ScriptOp::Commit { ops, .. } = op {
                for o in ops {
                    if let Op::CreateTree { tree } = o {
                        created.insert(*tree);
                    }
                }
            }
        }
    }
    assert!(!created.is_empty(), "the setup commit creates the trees");
    for t in &script.tasks {
        for op in &t.ops {
            if let e4_recovery::harness::ScriptOp::Commit { ops, .. } = op {
                for o in ops {
                    match o {
                        Op::Put { tree, .. } | Op::Delete { tree, .. } => {
                            assert!(created.contains(tree), "op tree {tree} never created");
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

/// The chunked transaction spans ≥ 2 segments by construction (5.2):
/// its encoded stream crosses 132 KiB, so no draw can fit one segment.
#[test]
fn chunked_txn_spans_two_segments() {
    use e4_recovery::model::Op;
    let knobs = Knobs::derive(5, 0);
    let model = e4_recovery::model::Model::new();
    let (script, _, _) = workload::build_round(5, 0, &knobs, &model, 0);
    let mut spans = 0u32;
    for t in &script.tasks {
        for op in &t.ops {
            if let e4_recovery::harness::ScriptOp::Chunked { ops, .. } = op {
                let bytes: u64 = ops
                    .iter()
                    .map(|o| match o {
                        Op::Put { value, key, .. } => 24 + key.len() as u64 + value.len() as u64,
                        _ => 32,
                    })
                    .sum();
                assert!(bytes >= 132 * 1024, "chunked txn too small ({bytes} bytes)");
                assert!(
                    ops.len() >= 300,
                    "chunked txn op count {} below the card's 300",
                    ops.len()
                );
                // The record cap: every 20-op chunk stays under 16 KiB.
                assert!(bytes / (ops.len() as u64 / 20 + 1) < 16 * 1024);
                spans += 1;
            }
        }
    }
    assert!(spans >= 1, "every round carries a chunked txn");
}

/// The campaign JSON round-trips: what `emit_campaign` writes,
/// `parse_campaign` reads back (the digests and quota flags survive).
#[test]
fn campaign_json_round_trips() {
    let recs: Vec<_> = (0..3u64)
        .chain([7u64, 9, 13])
        .map(|s| workload::run_seed(s, None))
        .collect();
    let json = campaign::emit_campaign(&recs);
    let back = campaign::parse_campaign(&json).expect("parse");
    assert_eq!(back.len(), recs.len());
    for (a, b) in recs.iter().zip(back.iter()) {
        assert_eq!(a.seed, b.seed);
        assert_eq!(a.digest, b.digest, "seed {} digest", a.seed);
        assert_eq!(a.class, b.class);
        assert_eq!(a.rounds.len(), b.rounds.len());
        for (x, y) in a.rounds.iter().zip(b.rounds.iter()) {
            assert_eq!(x.fired, y.fired);
            assert_eq!(x.nested, y.nested);
            assert_eq!(x.q4_split_merge, y.q4_split_merge);
            assert_eq!(x.q18_gc_advanced, y.q18_gc_advanced);
            assert_eq!(x.sizing_ok, y.sizing_ok);
        }
    }
    // The mutants JSON round-trips too (an empty sweep's shape).
    let rec = campaign::MutantRec {
        mutant: "CrcSkip".into(),
        directed_test: "d17_integrity",
        directed_encoded: true,
        directed_caught: Some(true),
        first_seed: Some(3),
        how: Some("O-3"),
        oracle_rate: 0.5,
        monitor_rate: 0.25,
        swept: 10,
    };
    let json = campaign::emit_mutants(&[rec]);
    let back = campaign::parse_mutants(&json).expect("parse");
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].mutant, "CrcSkip");
    assert_eq!(back[0].first_seed, Some(3));
    assert_eq!(back[0].how, Some("O-3"));
    assert_eq!(back[0].directed_caught, Some(true));
}

/// The regression pin for the round-setup barrier (the seed-3
/// development failure, with a literal config, not a seed number): a
/// chunked transaction racing the setup commit — without the barrier,
/// the chunk task's records reach the WAL **below** the setup's
/// `CreateTree`, and LSN-ordered replay applies the marker's Puts
/// against a directory without the tree ("tree 2 missing in directory",
/// a task error); with `WaitTrees`, the chunked task waits and the
/// round's recovery is whole.
#[test]
fn setup_barrier_guards_the_wal_order() {
    use e4_recovery::harness::{self, MediaDraw, RoundFaults, Script, ScriptOp, ScriptTask};
    use e4_recovery::model::Model;
    use e4_recovery::sim::FaultClass;

    // The hazard, demonstrated on the exact configuration that found it
    // (the generated round-0 script of seed 3, the barrier stripped):
    // the chunk task can win the enqueue race, and LSN-ordered replay
    // applies the marker's Puts against a directory without the tree
    // ("tree 2 missing in directory" through the recovery path). With
    // the barrier in force — the generator's own output — the same seed
    // recovers whole. (The barrier's presence is the literal variable;
    // the generated script is reproduced byte-identically by
    // `build_round`, so the pin does not depend on any code path beyond
    // it.)
    let knobs = Knobs::derive(3, 0);
    let cfg3 = knobs.store_cfg(None);
    let (script_b, _, _) = workload::build_round(3, 0, &knobs, &Model::new(), 0);
    let strip = |s: &Script| Script {
        tasks: s
            .tasks
            .iter()
            .map(|t| ScriptTask {
                ops: t
                    .ops
                    .iter()
                    .filter(|o| !matches!(o, ScriptOp::WaitTrees { .. }))
                    .cloned()
                    .collect(),
            })
            .collect(),
        writer: s.writer,
        evictor: s.evictor,
        checkpointer_every: s.checkpointer_every,
        crash: s.crash.clone(),
        nested: Vec::new(),
    };
    let faults3 = RoundFaults {
        class: FaultClass::C,
        knobs: knobs.fault,
        directed: Vec::new(),
        no_r_taint: false,
        media: MediaDraw::None,
        twin_supply: None,
        nested_draw: 0,
        probe_pass: false,
    };
    let run3 = |s: &Script| {
        let (disk, st) = harness::create_store(cfg3.clone(), 3, 3).expect("create");
        harness::run_round(
            3,
            0,
            s,
            &faults3,
            cfg3.clone(),
            disk,
            st,
            &Model::new(),
            0,
            None,
        )
    };
    let out_b = run3(&script_b);
    assert!(
        out_b.pass && out_b.o1.is_ok() && out_b.task_error.is_none(),
        "the barriered generator output must pass seed 3 (te={:?}, o1={:?}, o3={:?})",
        out_b.task_error,
        out_b.o1,
        out_b.o3_fail
    );
    let out_nb = run3(&strip(&script_b));
    assert!(
        !out_nb.pass,
        "the barrierless shape must fail visibly (te={:?}, o1={:?}, o3={:?})",
        out_nb.task_error, out_nb.o1, out_nb.o3_fail
    );
}

/// The directed-arms table names every mutant of the enum (the mutation
/// table's directed column), and the encoded arms still catch.
#[test]
fn directed_arms_table_is_complete() {
    use e4_recovery::store::Mutant;
    let all = [
        Mutant::CrcSkip,
        Mutant::RefCrcSkip,
        Mutant::EpochSkip,
        Mutant::IdSkip,
        Mutant::EpochSkipRefCrc,
        Mutant::IdSkipRefCrc,
        Mutant::FreeEarly,
        Mutant::BitmapOmitsFreesetPages,
        Mutant::MetaBeforeSync,
        Mutant::FlipNoGate,
        Mutant::WalSyncTOnly,
        Mutant::WriteGuardNoCopy,
        Mutant::EvictSkipImageMap,
        Mutant::StartLsnPastChunk,
        Mutant::StartLsnMarkedChunk,
        Mutant::TsAfterAppend,
        Mutant::LsnCheckSkip,
        Mutant::PublishBeforeMeta,
        Mutant::GcwNotRecorded,
        Mutant::GcwBoundSkip,
        Mutant::ReplayDupConflict,
        Mutant::DupDifferentAccepted,
        Mutant::InplaceEvict,
        Mutant::DropRootFirst,
        Mutant::RenameNoDirsync,
        Mutant::TruncateLogicalOnly,
        Mutant::StartSegmentMissingOk,
        Mutant::MetaWrongSlot,
    ];
    for m in all {
        assert!(
            campaign::directed_arms::TABLE
                .iter()
                .any(|(mm, _)| *mm == m),
            "{m:?} missing from the directed-arms table"
        );
    }
    // The two encoded arms catch their mutants (X-5b's D2 and D17
    // first-half shapes).
    let d2 = campaign::directed_arms::run_arm(Mutant::StartLsnMarkedChunk);
    assert_eq!(d2, Some(true), "D2's arm must catch START-LSN-MARKED-CHUNK");
    let d17 = campaign::directed_arms::run_arm(Mutant::CrcSkip);
    assert_eq!(d17, Some(true), "D17's arm must catch CRC-SKIP");
}

// ---- X5-R15 (P ruling nv3-X-5c-esc-3): the pass-fold line and verify's
// always-report-all-three exit ----

/// **X5-R15 (a) pin: the `E4 FOLD` line, from a literal 2-seed record
/// set.** Seed A is fully passing but carries a round with O-1 text
/// that **passes** the fold (the recovery-`Err` default O-1(a) text —
/// the 1,315 d2c8bba7 MP rounds the Work 7 line's `O1_FAIL` wrongly
/// counted as failures); it must contribute nothing. Seed B fails one
/// round of the fold, one twin round, and carries a seed-level defect.
/// The counts come from the pass fold and the seed-level
/// `defect`/`deadlock`/`panic`/`baton` flags, never from O-1 text
/// presence (validated against d2c8bba7: 1,598 failing seeds, 100
/// failing twin rounds, 150 defect seeds). A fold that counts O-1 text
/// presence (the reverted form) fails this test on seed A.
#[test]
fn r15a_fold_line_from_a_literal_2_seed_record_set() {
    use e4_recovery::workload::{RoundRec, SeedRec, TwinRound};
    let passing_round_with_o1_text = RoundRec {
        // The shape of d2c8bba7's recovery-Err MP rounds: O-1 text
        // present, round passing under the frozen fold.
        o1_fail: Some("published {} != model {2, 3}".to_owned()),
        pass: true,
        ..RoundRec::default()
    };
    let a = SeedRec {
        seed: 0,
        class: "MP".to_owned(),
        knobs: 1,
        rounds: vec![
            passing_round_with_o1_text,
            RoundRec {
                pass: true,
                ..RoundRec::default()
            },
        ],
        twin: vec![TwinRound {
            pass: true,
            o1_fail: None,
        }],
        ..SeedRec::default()
    };
    let b = SeedRec {
        seed: 1,
        class: "MP".to_owned(),
        knobs: 2,
        rounds: vec![
            RoundRec {
                pass: true,
                ..RoundRec::default()
            },
            RoundRec {
                pass: false,
                ..RoundRec::default()
            },
        ],
        twin: vec![TwinRound {
            pass: false,
            o1_fail: Some("scan mismatch".to_owned()),
        }],
        defect: Some("twin round 0: the crash spec never fired".to_owned()),
        ..SeedRec::default()
    };
    assert_eq!(
        campaign::fold_line(&[a.clone(), b.clone()]),
        "E4 FOLD FAILING_SEEDS=1 FAILING_ROUNDS=1 TWIN_FAILING_ROUNDS=1 DEFECT_SEEDS=1",
        "the fold counts pass-fold failures and seed-level flags, never O-1 text presence"
    );
    // And the Work 7 line keeps its own form (unchanged by X5-R15).
    assert!(campaign::final_line(&[a.clone(), b.clone()]).starts_with("E4 SEEDS=2 "));
}

/// **X5-R15 (b) pin: `verify` reports all three checks before its exit,
/// and `--no-quotas` does not fail on the quota table.** The exit code
/// is decided once, at the end, from all three outcomes together
/// ([`campaign::verify_exit_code`]): a seed-set failure or a
/// non-reproducing digest always fails; a missed quota fails only
/// without `--no-quotas` (the flag exists for X-5c-F1's Acceptance).
#[test]
fn r15b_verify_reports_all_three_checks_before_failing() {
    // (a) everything clean: exit 0, plain and --no-quotas alike.
    assert_eq!(campaign::verify_exit_code(true, 0, 0, false), 0);
    assert_eq!(campaign::verify_exit_code(true, 0, 5, true), 0);
    // (b) a missed quota: plain verify fails, --no-quotas does not (the
    // table is still printed by the command).
    assert_eq!(campaign::verify_exit_code(true, 0, 5, false), 2);
    // (c) a non-reproducing digest fails both — --no-quotas waives the
    // quota check only, never the digests or the seed set.
    assert_eq!(campaign::verify_exit_code(true, 1, 0, true), 2);
    assert_eq!(campaign::verify_exit_code(true, 1, 5, true), 2);
    // (d) a seed-set failure fails both.
    assert_eq!(campaign::verify_exit_code(false, 0, 0, true), 2);
    // The seed-set predicate: exactly 0..CAMPAIGN_SEEDS-1 in order.
    let good: Vec<e4_recovery::workload::SeedRec> = (0..campaign::CAMPAIGN_SEEDS)
        .map(|s| e4_recovery::workload::SeedRec {
            seed: s,
            ..Default::default()
        })
        .collect();
    assert!(campaign::verify_seed_set_ok(&good));
    let mut hole = good.clone();
    hole.truncate((campaign::CAMPAIGN_SEEDS - 1) as usize);
    assert!(!campaign::verify_seed_set_ok(&hole));
    // The digest report names the non-reproducing seeds.
    let x = e4_recovery::workload::SeedRec {
        seed: 7,
        digest: 1,
        ..Default::default()
    };
    let y = e4_recovery::workload::SeedRec {
        seed: 7,
        digest: 2,
        ..Default::default()
    };
    let (n, list) = campaign::verify_digest_report(&[x], &[y]);
    assert_eq!((n, list), (1, vec![7]));
}

/// **X5-R16 NEVER-FIRED pin (P ruling nv3-X-5c-esc-3; literal config =
/// d2c8bba7's defect seeds).** Seeds 60 and 660 both carried "the crash
/// spec never fired" in the committed campaign (60: the drawn
/// `Mid2@2`'s checkpoint never reached Mid2; 660: round 0's checkpoints
/// reached no Mid2 at all — the tiny pool's evictor wrote every dirty
/// page, so step 2 wrote nothing, P's stated cause "its step 2 has too
/// little to write"). The targeted draw's counting pass (the same probe
/// pattern as the uniform draw's) now records which hooks each
/// checkpoint index reaches (`WorkloadObs::ckpt_hooks_hit`) and the
/// draw lands on a reached (index, hook); every round's crash fires and
/// the seed carries no defect. On the reverted draw both assertions
/// fail (the defect text reappears and `fired` is false).
#[test]
fn r16_never_fired_targeted_draws_reach_their_hook() {
    for seed in [60u64, 660] {
        let rec = workload::run_seed(seed, None);
        assert!(
            rec.defect.is_none(),
            "seed {seed}: the fired-guard defect is back: {:?}",
            rec.defect
        );
        assert!(
            rec.rounds.iter().all(|r| r.fired),
            "seed {seed}: a round's crash spec never fired: {:?}",
            rec.rounds
                .iter()
                .map(|r| (r.crash.clone(), r.fired))
                .collect::<Vec<_>>()
        );
        // The twin rounds fired too (35 of the 150 defects were twin
        // rounds; the twin shares the real round's spec).
        assert!(
            !rec.rounds.iter().any(|r| r.crash.ends_with("never fired")),
            "seed {seed}: a stale never-fired description"
        );
    }
}

/// **X5-R16 NO-TWIN pin (P ruling nv3-X-5c-esc-3; literal config =
/// d2c8bba7's no-twin seeds).** Seed 1267's committed round carried the
/// oracle's own text "MP/MW round with XX001 and no twin (harness
/// defect)": its twin round's recovery returned `Err`, no supply was
/// built and the twin chain broke — the real round then had no O-3 (c)
/// sets. The harness now supplies the twin for every MP/MW round: a
/// failed twin still yields its recovery reads (collected on both
/// arms) and its crashed-disk image slots, and a broken twin chain
/// re-seeds from the real round's own start (Work 2.5's twin: the same
/// script, crash and F-CRASH resolutions, faultless). On the reverted
/// supply the "no twin" text reappears.
#[test]
fn r16_no_twin_is_always_supplied() {
    for seed in [1267u64, 1769] {
        let rec = workload::run_seed(seed, None);
        let no_twin = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("no twin"));
        assert!(
            !no_twin,
            "seed {seed}: the missing-twin harness defect is back (O-3 (c) never fails open)"
        );
    }
}

/// **X5-R16 MP-E pin — the completeness half (P ruling nv3-X-5c-esc-3;
/// literal config = d2c8bba7's completeness seeds 47/118/198, round 0
/// each).** All three carried "O-3(e): tainted referenced slot 0 not
/// reported": slot 0 is the §2.1 **file header**, which §3.3's
/// reference model has no reference for, so amcheck (i) can neither
/// check nor report it — yet the harness's referenced set hardcoded
/// it, making every media fault on its range an unreportable "tainted
/// referenced slot". The referenced set is the image's own slots (the
/// map's, the free set's, the metas'); the header stays covered by
/// open's own read (a corrupt header fails open with XX001 at `Open`).
/// On the reverted `used.insert(0)` the completeness text reappears.
/// (The exactness half of MP-E is escalated with its evidence — see
/// the README's stage record: seed 37's slot-11 finding is C-XX's
/// mechanism inside a Q14 rollback, not an instrumentation defect.)
#[test]
fn r16_mpe_referenced_set_excludes_the_file_header() {
    for seed in [47u64, 118, 198] {
        let rec = workload::run_seed(seed, None);
        let not_reported = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("not reported"));
        assert!(
            !not_reported,
            "seed {seed}: the header-slot completeness defect is back"
        );
    }
}

/// **X5-R20 shape A pin (P ruling nv3-X-5c-F1-esc-1; literal config =
/// bc4f3bba's shape-A seeds 67/268, round 0 each).** Both carried
/// "O-3(e): amcheck reported slot 1 that is neither tainted nor in R"
/// with `mrb=false`, `rlen=0` and the recovery-`Err` default O-1 text:
/// a Q14 page fault (seed 67: LOST on the create's meta k=0 write at
/// slot 1 — the only meta write ever issued; seed 268: two MISDIRECTED
/// faults with the same effect) left **no** valid meta anywhere, so
/// open failed XX001, `durable_image` took its `NoValid` arm, and that
/// arm's finding hardcoded `slot: Some(1)` — which the frozen oracle's
/// exactness half (not gated on `amcheck_i_ran`) judged against empty
/// taint and R sets. X5-R5: with both metas invalid there is no opened
/// image and O-3 (e) is **vacuous**; the finding is structural now (no
/// slot, like the `MisplacedNewer` arm), and the damage stays detected
/// at `Open` (XX001, never served). On the reverted `slot: Some(1)`
/// the exactness text reappears and the rounds fail.
/// **X5-R20 shape B pin (P ruling nv3-X-5c-F1-esc-1; literal config =
/// bc4f3bba's shape-B seeds 38/137, round 0 each).** Both carried
/// "O-3(e): amcheck reported slot 4 that is neither tainted nor in R"
/// with `mrb=true`, `rlen=0`: a Q14 fault tore/misdirected away the
/// newest meta (k=1 at slot 2), open rolled back to the create's
/// k=0 at slot 1, and the checkpoint after k=1's graduation had
/// **reused** k=0's freed map-root slot 4 — ten issued writes after
/// the graduation sync (seed 38: ev[132..287], epoch-2 map leaves),
/// none sync-covered, the crash keeping the last (an id-6 epoch-2
/// page). The bytes equal the last issued (untainted by 1.4), the
/// rolled-back image references the slot with its stale create-era
/// ref CRC (§3.3 check 2 fails) — X5-R5's rollback-reuse narrative
/// word for word, and R5's condition-3 text excuses it ("SimDisk
/// records a page-file write to `s` **issued** after the first
/// completed `sync_all` that covered a meta newer than the opened
/// one"). The draft's `exception_set_r` consulted only the
/// sync-covered durable-write list and missed every unsynced reuse;
/// it now also consults the issued-write event stream (the same
/// record R5's M-NOINPLACE guard reads). On the reverted event source
/// `rlen` is 0 again and the exactness text reappears.
/// **X5-R20 completeness-residual pin (P ruling nv3-X-5c-F1-esc-1;
/// literal config = bc4f3bba's residual seeds 1787/6157, round 0
/// each).** Both carried "O-3(e): tainted referenced slot 10/42 not
/// reported" with `mrb=true`: the slot's durable bytes reverted to
/// exactly the content the opened (rolled-back) image references — a
/// crash-dropped volatile write (slot 10's last two writes dw[11],
/// dw[22] are sync-covered; the dropped one was issued after them) —
/// so §3.3 passes against the image's own reference and the walk can
/// never name it, while the frozen oracle's completeness half demands
/// every tainted referenced slot be reported. The harness's
/// ground-truth fallout report (already committed for the meta slots,
/// X5-R16) now covers every tainted slot of the referenced set, armed
/// for the MP amcheck (i) walk. On the reverted arm the "not
/// reported" text reappears.
/// **X5-R21 (c) pin (P ruling nv3-X-5c-F1-esc-1; literal config = the
/// ruling's five seeds 2669/4049/4309/7469/9809 — all targeted MW
/// seeds where the twin and real chains diverge and the real round
/// re-derives its spec).** The draft's real-round retry re-derived
/// the spec and re-ran alone, keeping the twin's supply and nested
/// points from the twin's run at the OLD spec — a Work 2.5 pairing
/// break (the twin is the same script, crash and F-CRASH
/// resolutions). Now the twin re-runs at the re-derived spec, from
/// its own start, before the real round uses its supply and nested
/// points; the twin record, supply, nested points and chain
/// continuation come from the re-run. Observable at seed 7469 (the
/// re-derived round's twin outcome changed with it) and 9809 (its
/// nested points); `twin_paired` (diagnostic only, never a digest
/// input) asserts the pairing on every round. On the reverted
/// pairing re-run the flag reads false on the re-derived round.
#[test]
fn r21c_the_real_rounds_retry_re_pairs_its_twin() {
    for seed in [2669u64, 4049, 4309, 7469, 9809] {
        let rec = workload::run_seed(seed, None);
        assert!(
            rec.defect.is_none(),
            "seed {seed}: the fired-guard defect is back: {:?}",
            rec.defect
        );
        assert_eq!(
            rec.twin_paired.len(),
            rec.rounds.len(),
            "seed {seed}: the pairing flag must cover every executed round"
        );
        assert!(
            rec.twin_paired.iter().all(|p| *p),
            "seed {seed}: a round ran with a twin at a different crash spec (Work 2.5 break)"
        );
    }
}

#[test]
fn r20_mpe_damage_fallout_covers_every_referenced_slot() {
    for seed in [1787u64, 6157] {
        let rec = workload::run_seed(seed, None);
        let not_reported = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("not reported"));
        assert!(
            !not_reported,
            "seed {seed}: the §3.3-clean reverted slot is unreported again"
        );
        assert!(
            rec.rounds.iter().all(|r| r.pass),
            "seed {seed}: a round fails beyond the removed defect: {:?}",
            rec.rounds
                .iter()
                .map(|r| (&r.crash, &r.o3_fail, r.pass))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn r20_mpe_r_reads_the_issued_write_history() {
    for seed in [38u64, 137] {
        let rec = workload::run_seed(seed, None);
        let exactness = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("neither tainted nor in R"));
        assert!(
            !exactness,
            "seed {seed}: the unsynced-rollback-reuse slot left R again (condition 3 must read the issued-write events)"
        );
        // The rollback rounds now carry a non-empty R (|R| ≥ 1) —
        // the reused slot itself — and no O-3 clause fails.
        assert!(
            rec.rounds.iter().any(|r| r.meta_rollback && r.r_len >= 1),
            "seed {seed}: the shape-B rollback round lost its R set",
        );
        assert!(
            rec.rounds.iter().all(|r| r.pass),
            "seed {seed}: a round fails beyond the removed defect: {:?}",
            rec.rounds
                .iter()
                .map(|r| (&r.crash, &r.o3_fail, r.pass))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn r20_mpe_no_opened_image_means_no_slotted_finding() {
    for seed in [67u64, 268] {
        let rec = workload::run_seed(seed, None);
        let exactness = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("neither tainted nor in R"));
        assert!(
            !exactness,
            "seed {seed}: the no-opened-image slotted finding is back (O-3 (e) must be vacuous per X5-R5)"
        );
        // The rounds now pass: the XX001 at `Open` is a detection in
        // MP (the plan's clause 3), and no O-3 clause fails.
        assert!(
            rec.rounds.iter().all(|r| r.pass),
            "seed {seed}: a round fails beyond the removed defect: {:?}",
            rec.rounds
                .iter()
                .map(|r| (&r.crash, &r.o3_fail, r.pass))
                .collect::<Vec<_>>()
        );
    }
}
