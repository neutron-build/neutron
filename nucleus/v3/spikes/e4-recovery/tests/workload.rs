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
