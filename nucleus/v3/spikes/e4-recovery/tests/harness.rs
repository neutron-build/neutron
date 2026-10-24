//! The round-harness tests (card X-5b Work 13): the same inputs give a
//! byte-identical digest, and a first end-to-end round (scripted
//! commits → crash → recovery → oracle → harness checkpoint) passes
//! clean in class C, with recovery finding exactly the durable prefix.

#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

use e4_recovery::harness::{self, CrashSpec, RoundFaults, Script, ScriptOp, ScriptTask};
use e4_recovery::model::{Model, Op};
use e4_recovery::sim::{FaultClass, FaultKnobs};
use e4_recovery::store::{Mutant, StoreCfg};

fn cfg(mutant: Option<Mutant>) -> StoreCfg {
    StoreCfg {
        map_leaf_cap: 8,
        freeset_leaf_bits: 128,
        pool_frames: 12,
        r_epoch: 2,
        mutant,
    }
}

fn drop_all() -> FaultKnobs {
    FaultKnobs {
        keep_latest_pm: 0,
        tear_pm: 0,
    }
}

fn round_faults() -> RoundFaults {
    RoundFaults {
        class: FaultClass::C,
        knobs: drop_all(),
        directed: Vec::new(),
    }
}

fn op_put(i: u64, v: u64) -> Op {
    Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v as u8; 8],
    }
}

/// Work 13's determinism test: the same inputs give a byte-identical
/// digest.
#[test]
fn round_digest_is_deterministic() {
    let mut digests = Vec::new();
    for run in 0..3u64 {
        let script = Script {
            tasks: vec![ScriptTask {
                ops: vec![
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: vec![Op::CreateTree { tree: 2 }],
                    },
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: vec![op_put(1, 1), op_put(2, 2)],
                    },
                    ScriptOp::Commit {
                        sync_on: false,
                        ops: vec![op_put(3, 3)],
                    },
                    ScriptOp::Checkpoint,
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: vec![op_put(4, 4)],
                    },
                ],
            }],
            writer: true,
            evictor: true,
            checkpointer_every: Some(3),
            crash: Some(CrashSpec::TaskYield(1, 3)),
            nested: Vec::new(),
        };
        let (disk, st) = harness::create_store(cfg(None), 900 + run, 77).expect("create");
        let model = Model::new();
        let outcome = harness::run_round(
            900 + run,
            0,
            &script,
            &round_faults(),
            cfg(None),
            disk,
            st,
            &model,
            0,
            None,
        );
        digests.push(outcome.digest);
    }
    assert_eq!(
        digests[0], digests[1],
        "runs 0 and 1 of the same seed differ"
    );
    assert_eq!(
        digests[0], digests[2],
        "runs 0 and 2 of the same seed differ"
    );
}

/// An end-to-end class-C round: commits (SyncOn and SyncOff), a
/// checkpoint, a crash that drops all volatile state, recovery, the
/// oracle, and the harness checkpoint. The round passes; the recovery
/// finds a durable prefix; no monitors fire.
#[test]
fn class_c_round_passes() {
    let script = Script {
        tasks: vec![ScriptTask {
            ops: vec![
                ScriptOp::Commit {
                    sync_on: true,
                    ops: vec![Op::CreateTree { tree: 2 }],
                },
                ScriptOp::Commit {
                    sync_on: true,
                    ops: (0..40).map(|i| op_put(i, 1)).collect(),
                },
                ScriptOp::Checkpoint,
                ScriptOp::Commit {
                    sync_on: true,
                    ops: (40..60).map(|i| op_put(i, 1)).collect(),
                },
            ],
        }],
        writer: true,
        evictor: true,
        checkpointer_every: Some(100),
        // Crash before the last commit op: the checkpointed prefix (the
        // first 40 keys, ts 2) is durable either way.
        crash: Some(CrashSpec::TaskYield(1, 3)),
        nested: Vec::new(),
    };
    let (disk, st) = harness::create_store(cfg(None), 4242, 99).expect("create");
    let model = Model::new();
    let outcome = harness::run_round(
        4242,
        0,
        &script,
        &round_faults(),
        cfg(None),
        disk,
        st,
        &model,
        0,
        None,
    );
    assert!(
        !outcome.deadlock && !outcome.baton && !outcome.panicked,
        "per-seed failure flags: {:?}",
        (outcome.deadlock, outcome.baton, outcome.panicked)
    );
    assert!(
        outcome.monitors.is_empty(),
        "monitors fired in a clean round: {:?}",
        outcome.monitors
    );
    assert!(
        outcome.o2_fail.is_none(),
        "O-2 failed: {:?}",
        outcome.o2_fail
    );
    assert!(
        outcome.o3_fail.is_none(),
        "O-3 failed: {:?}",
        outcome.o3_fail
    );
    let c = outcome.o1.expect("O-1 matched a cut");
    // The 40-key commit (ts 2) was checkpointed and SyncOn-acked; the
    // crash at a task yield keeps it durable.
    assert!(c >= 2, "the cut {c} lost the checkpointed prefix (ts 2)");
    assert!(outcome.amcheck_ii.is_empty(), "amcheck (ii) findings");
}
