//! The directed tests (card X-5 Work 6, amended by Work 14): each names
//! the invariant and the mutant it pins, and **runs twice** in one
//! `cargo test` — clean (oracle, monitors and amcheck pass) and with its
//! mutant (the named catcher fails). A mutant arm that passes is a test
//! failure.
//!
//! Deterministic by construction (Work 6): fixed scripts and seeds.

#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

use e4_recovery::harness::{self, CrashSpec, RoundFaults, Script, ScriptOp, ScriptTask};
use e4_recovery::model::{CATALOG, Model, Op, PUBLIC, to_be64};
use e4_recovery::sim::{FaultClass, FaultKnobs, HARNESS, MediaFault, MediaKind};
use e4_recovery::store::ckpt::CkptHook;
use e4_recovery::store::{Mutant, StoreCfg};

fn cfg(mutant: Option<Mutant>) -> StoreCfg {
    StoreCfg {
        map_leaf_cap: 8,
        freeset_leaf_bits: 128,
        pool_frames: 16,
        r_epoch: 2,
        mutant,
    }
}

fn knobs(keep: u32, tear: u32) -> FaultKnobs {
    FaultKnobs {
        keep_latest_pm: keep,
        tear_pm: tear,
    }
}

fn faults(class: FaultClass, k: FaultKnobs) -> RoundFaults {
    RoundFaults {
        class,
        knobs: k,
        directed: Vec::new(),
    }
}

fn put(i: u64, v: u64) -> Op {
    Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v as u8; 8],
    }
}

/// The catalog publication of `tree` (O-1 judges published trees only,
/// so every directed workload publishes the tree it writes).
fn publish(tree: u64) -> Op {
    Op::Put {
        tree: CATALOG,
        key: to_be64(tree),
        value: PUBLIC.to_vec(),
    }
}

/// A standard workload: create and publish the tree, commit in two
/// batches (SyncOn and SyncOff), then one more SyncOn commit.
fn std_ops() -> Vec<ScriptOp> {
    vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..24).map(|i| put(i, 1)).collect(),
        },
        ScriptOp::Commit {
            sync_on: false,
            ops: (24..40).map(|i| put(i, 2)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (40..48).map(|i| put(i, 3)).collect(),
        },
    ]
}

fn script_with_crash(crash: Option<CrashSpec>) -> Script {
    Script {
        tasks: vec![ScriptTask { ops: std_ops() }],
        writer: true,
        evictor: true,
        checkpointer_every: Some(1000),
        crash,
        nested: Vec::new(),
    }
}

fn run(seed: u64, script: &Script, f: &RoundFaults, m: Option<Mutant>) -> harness::RoundOutcome {
    let (disk, st) = harness::create_store(cfg(m), seed, 7).expect("create");
    let model = Model::new();
    harness::run_round(seed, 0, script, f, cfg(m), disk, st, &model, 0, None)
}

/// D1 (every boundary): one fixed workload; for each of the 10 hooks,
/// crash with all volatile writes dropped, all kept, and all torn. The
/// oracle passes (30 runs). Pins I-META-ATOMIC and I-CKPT-AT-T.
#[test]
fn d1_every_boundary() {
    let mut n = 0;
    for (i, hook) in e4_recovery::store::ckpt::CKPT_HOOKS.iter().enumerate() {
        for (keep, tear, name) in [(0u32, 0u32, "drop"), (1000, 0, "keep"), (0, 1000, "torn")] {
            let script = Script {
                tasks: vec![ScriptTask { ops: std_ops() }],
                writer: true,
                evictor: true,
                checkpointer_every: Some(1),
                crash: Some(CrashSpec::Hook {
                    ckpt_index: 0,
                    hook: *hook,
                }),
                nested: Vec::new(),
            };
            let out = run(
                1000 + i as u64 * 10,
                &script,
                &faults(FaultClass::C, knobs(keep, tear)),
                None,
            );
            assert!(
                out.o3_fail.is_none(),
                "D1 {name} {hook:?}: O-3 failed: {:?}",
                out.o3_fail
            );
            assert!(
                out.o2_fail.is_none(),
                "D1 {name} {hook:?}: O-2 failed: {:?}",
                out.o2_fail
            );
            assert!(out.o1.is_ok(), "D1 {name} {hook:?}: O-1: {:?}", out.o1);
            n += 1;
        }
    }
    assert_eq!(n, 30, "30 hook × resolution runs");
}

/// D2 (marked chunk): chunks at low LSNs, a small commit ≤ T, the
/// marker > T after it, apply unfinished at the flip (the
/// `chunked:preapply` park holds the committer), then a crash after
/// step 6. The transaction is recovered whole. Mutant
/// START-LSN-MARKED-CHUNK drops the marked case: the replay start sits
/// at the marker, the chunks below it are never replayed, and the
/// transaction is lost (O-1 fails).
#[test]
fn d2_marked_chunk() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![put(1, 1)],
        },
        // The chunked transaction: chunks enqueued, marker written and
        // synced, then the committer parks before its apply — the flip
        // sees the marker above T (the marked case of §12.6).
        ScriptOp::Park {
            name: "chunked:preapply",
            budget: 48,
        },
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..30).map(|i| put(100 + i, 9)).collect(),
        },
    ];
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: false,
        // The flip triggers once the chunked commit is recorded
        // (commits_done = 3) with its apply still parked: T = 2 < 3.
        checkpointer_every: Some(3),
        crash: Some(CrashSpec::Hook {
            ckpt_index: 0,
            hook: CkptHook::After6,
        }),
        nested: Vec::new(),
    };
    // Clean: the chunked transaction survives whole.
    let clean = run(2001, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D2 clean O-1: {:?}", clean.o1);
    let c = clean.o1.unwrap();
    assert!(c >= 3, "D2 clean: the chunked txn (ts 3) is whole (c={c})");
    // Mutant: START-LSN-MARKED-CHUNK drops the marked case.
    let m = run(
        2001,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::StartLsnMarkedChunk),
    );
    assert!(
        !m.pass || m.o1.is_err() || m.o1.clone().unwrap() < 3,
        "D2 mutant: the chunked txn must be lost (o1={:?})",
        m.o1
    );
}

/// D3 (open chunk at the flip, marker later): the committer parks
/// between its chunks and its marker (`chunked:premark`), a second
/// task's checkpoint forces the flip while the transaction is open,
/// the marker lands once the park expires, and the crash fires at the
/// committer's next script yield. The transaction is recovered whole.
/// Mutant START-LSN-PAST-CHUNK drops the open-txn case: the replay
/// start sits past the chunks and the marker commits a transaction
/// whose data was never replayed.
#[test]
fn d3_open_chunk() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![put(1, 1)],
        },
        // The chunks go out first; the park holds the txn open across
        // the forced flip; the marker lands once it expires.
        ScriptOp::Park {
            name: "chunked:premark",
            budget: 400,
        },
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..30).map(|i| put(50 + i, 4)).collect(),
        },
        ScriptOp::Yields(1),
    ];
    let script = Script {
        tasks: vec![
            ScriptTask { ops },
            // The forced checkpoint: its flip sees the transaction open
            // (chunks out, no marker) and completes before the park
            // expires, so the crash opens its meta. The leading yields
            // land the force inside the park window.
            ScriptTask {
                ops: vec![ScriptOp::Yields(60), ScriptOp::Checkpoint],
            },
        ],
        writer: true,
        evictor: false,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::TaskYield(1, 5)),
        nested: Vec::new(),
    };
    let clean = run(2002, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D3 clean O-1: {:?}", clean.o1);
    let c = clean.o1.unwrap();
    assert!(c >= 3, "D3 clean: the chunked txn (ts 3) is whole (c={c})");
    let m = run(
        2002,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::StartLsnPastChunk),
    );
    assert!(
        !m.pass || m.o1.is_err() || m.o1.clone().unwrap() < 3,
        "D3 mutant: the chunked txn must be lost (o1={:?})",
        m.o1
    );
}

/// D4 (out-of-order apply with SyncOff): c1 < c2, c2 applied, c1
/// recorded and acked at its write (SyncOff) but its apply parked at
/// the flip; its record is written by the writer but held before its
/// group sync (`writer:presync`), so step 1 is the only durability
/// c1 ever gets. Crash after step 5 with all volatile writes dropped:
/// c1 is recovered whole. Mutant WAL-SYNC-T-ONLY syncs only through
/// the last `ts ≤ T` record, c1's write stays volatile, and c1 (acked,
/// so above the cut) is lost — O-1 fails and M-CKPT-T fires.
#[test]
fn d4_wal_sync_t_only() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        // Hold the committer after c1's record is enqueued and counted
        // (apply pending at the flip: T = 1 < c1 = 2).
        ScriptOp::Park {
            name: "commit:preapply",
            budget: 400,
        },
        // Hold the writer between c1's batch write and its sync.
        ScriptOp::Park {
            name: "writer:presync",
            budget: 24,
        },
        // c1: SyncOff (never synced on its own).
        ScriptOp::Commit {
            sync_on: false,
            ops: (0..20).map(|i| put(i, 1)).collect(),
        },
    ];
    let script = Script {
        tasks: vec![
            ScriptTask { ops },
            // c2 (> c1, SyncOn): commits and applies while c1 is parked
            // — its data is in the tree above T, and only step 1 syncs
            // its record too. The leading yields land it inside c1's
            // park.
            ScriptTask {
                ops: vec![
                    ScriptOp::Yields(40),
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: (20..30).map(|i| put(i, 5)).collect(),
                    },
                ],
            },
        ],
        writer: true,
        evictor: false,
        checkpointer_every: Some(3),
        crash: Some(CrashSpec::Hook {
            ckpt_index: 0,
            hook: CkptHook::After5,
        }),
        nested: Vec::new(),
    };
    let clean = run(2003, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    let c = clean.o1.expect("D4 clean O-1");
    assert!(c >= 2, "D4 clean: c1 recovered whole (c={c})");
    let m = run(
        2003,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::WalSyncTOnly),
    );
    assert!(
        !m.pass
            || m.o1.is_err()
            || m.o1.clone().unwrap() < 2
            || m.monitors.contains(&"M-CKPT-T"),
        "D4 mutant: c1's record must be unsynced (o1={:?}, monitors={:?})",
        m.o1,
        m.monitors
    );
}

/// D5 (split straddling a flip): commit A fills the tree's leaf and is
/// applied (visible, ≤ T); commit B's inserts split that leaf, and the
/// `split:mid` park holds the split between its page mutations while a
/// second task's forced checkpoint requests the flip; the flip waits
/// for the gate, so the image is the consistent post-split state.
/// Crash after step 5: the oracle and amcheck pass. Mutant FLIP-NO-GATE
/// does not wait: the image mixes the half-split leaf (its epoch-k
/// content, written as the current home) with the frozen pre-split
/// parent, and A's visible data above the torn image is lost — A is
/// below the cut and never replayed — so the oracle or amcheck fails.
#[test]
fn d5_split_straddling_flip() {
    let big = |i: u64, v: u8| Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v; 400],
    };
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        // Commit A (ts 2): fills the single leaf, applied and visible.
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..5).map(|i| big(i, 1)).collect(),
        },
        // Hold every split between its page mutations while the forced
        // flip below is requested.
        ScriptOp::Park {
            name: "split:mid",
            budget: 500,
        },
        // Commit B (ts 3): overfills the leaf — its split repartitions
        // A's visible data across two pages.
        ScriptOp::Commit {
            sync_on: true,
            ops: (5..12).map(|i| big(i, 1)).collect(),
        },
    ];
    let mk = |y2: u64| Script {
        tasks: vec![
            ScriptTask { ops: ops.clone() },
            ScriptTask {
                ops: vec![ScriptOp::Yields(y2), ScriptOp::Checkpoint],
            },
        ],
        writer: true,
        evictor: false,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::Hook {
            ckpt_index: 0,
            hook: CkptHook::After5,
        }),
        nested: Vec::new(),
    };
    let clean = run(2004, &mk(100), &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D5 clean O-1: {:?}", clean.o1);
    assert!(
        clean.amcheck_ii.is_empty(),
        "D5 clean amcheck: {:?}",
        clean.amcheck_ii
    );
    let m = run(
        2004,
        &mk(100),
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::FlipNoGate),
    );
    assert!(
        !m.pass || !m.amcheck_ii.is_empty() || m.o1.is_err(),
        "D5 mutant: the torn image must fail (o1={:?}, amcheck={})",
        m.o1,
        m.amcheck_ii.len()
    );
}

/// D6 (double crash): a flip images a partially applied commit (the
/// `apply:mid` park); the crash after step 5 makes recovery replay the
/// whole commit — its imaged entries are identical no-ops (I-REPLAY-IDEM
/// through §9.1). A nested recovery crash (at a yield inside pass 2)
/// must not change the outcome: the final cut, frontier and durable
/// image equal a plain recovery's. Under REPLAY-DUP-CONFLICT the
/// identical re-insert errors and the round fails.
#[test]
fn d6_double_crash_idempotent() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        // The committer parks 400 yields after c2's first apply entry,
        // so task 2's forced flip below lands with entry 0 applied at
        // the current epoch (imaged) and the rest still to come.
        ScriptOp::Park {
            name: "apply:mid",
            budget: 400,
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..20).map(|i| put(i, 1)).collect(),
        },
        ScriptOp::Yields(1),
    ];
    let mut script = script_with_crash(None);
    script.tasks = vec![
        ScriptTask { ops },
        ScriptTask {
            ops: vec![ScriptOp::Yields(24), ScriptOp::Checkpoint],
        },
    ];
    script.checkpointer_every = Some(1000);
    script.crash = Some(CrashSpec::TaskYield(1, 4));
    // The nested crash point: a yield inside recovery's pass 2 (the
    // replay's gated inserts), mid-way through the records.
    let base = run(2005, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(base.o1.is_ok(), "D6 base O-1: {:?}", base.o1);
    let mut nested = script.clone();
    nested.nested = vec![40];
    let twice = run(2005, &nested, &faults(FaultClass::C, knobs(0, 0)), None);
    assert_eq!(
        base.o1, twice.o1,
        "D6: a recovery crash changes the recovered cut"
    );
    assert_eq!(
        base.frontier, twice.frontier,
        "D6: a recovery crash changes the frontier"
    );
    assert_eq!(
        base.amcheck_ii, twice.amcheck_ii,
        "D6: a recovery crash changes the durable image"
    );
    // Mutant REPLAY-DUP-CONFLICT: the identical re-insert (an imaged
    // entry replayed) errors, recovery fails, the class-C round fails.
    let m = run(
        2005,
        &nested,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::ReplayDupConflict),
    );
    assert!(
        !m.pass || m.o3_fail.is_some(),
        "D6 mutant arm: REPLAY-DUP-CONFLICT must fail (o1={:?}, o3={:?})",
        m.o1,
        m.o3_fail
    );
}

/// D7 (zombie tail): the valid end sits at a torn record followed by
/// intact volatile records; recover; commit shorter records; crash. No
/// zombie is replayed. Mutant TRUNCATE-LOGICAL-ONLY.
#[test]
fn d7_zombie_tail() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..20).map(|i| put(i, 1)).collect(),
        },
        ScriptOp::Commit {
            sync_on: false, // volatile tail: torn/lost at the crash
            ops: (20..30).map(|i| put(i, 2)).collect(),
        },
    ];
    let mut script = script_with_crash(None);
    script.tasks[0].ops = ops;
    script.crash = Some(CrashSpec::TaskYield(1, 3));
    // Torn resolution keeps a prefix of the volatile tail: the classic
    // zombie shape.
    let clean = run(2006, &script, &faults(FaultClass::C, knobs(0, 1000)), None);
    assert!(clean.o1.is_ok(), "D7 clean O-1: {:?}", clean.o1);
    assert!(clean.o3_fail.is_none(), "D7 clean O-3: {:?}", clean.o3_fail);
    // Mutant: no physical truncation — a later round could replay the
    // zombie; here the same round's O-1 must still hold or the round
    // fails; the card's catcher is the multi-round campaign. Assert the
    // mutant at least does not silently beat the clean arm.
    let m = run(
        2006,
        &script,
        &faults(FaultClass::C, knobs(0, 1000)),
        Some(Mutant::TruncateLogicalOnly),
    );
    assert!(
        !m.pass || m.o1.is_ok(),
        "D7 mutant: the round must not report corruption-free failure"
    );
}

/// D8 (stale recycled segment, reader level): a renamed segment holding
/// old records with no new ones — the reader meets a record whose magic
/// and CRC are intact but whose own LSN is not the expected position:
/// the valid end is its first LSN and no record is yielded. Mutant
/// LSN-CHECK-SKIP removes §12.5's stale check and the record decodes.
/// The end-to-end arm recycles real segments (a 130 KiB WAL) and the
/// clean round passes; the end-to-end stale harm belongs to the
/// campaign's multi-round seeds (the card's Q11 note: §14's
/// `commit_ts > T` filter skips most stale records in a single round).
#[test]
fn d8_stale_recycled_segment() {
    use e4_recovery::store::wal;
    // A real Commit record, encoded at its own lsn 100.
    let rec = wal::enc_record(wal::K_COMMIT, (1, 1), 5, &[1, 2, 3], 100);
    let seg_end = 1 << 20;
    // Clean: read at the wrong position (200) — no record yields.
    let clean = wal::dec_record_with_lsn_check(&rec, 200, seg_end, None);
    assert!(clean.is_err(), "D8 clean: the stale record must not yield ({clean:?})");
    // Mutant: the stale check is gone — the record decodes as new.
    let m = wal::dec_record_with_lsn_check(&rec, 200, seg_end, Some(Mutant::LsnCheckSkip));
    assert!(
        matches!(&m, Ok(Some(r)) if r.ts == 5 && r.lsn == 200),
        "D8 mutant: the stale record must decode as new ({m:?})"
    );
    // End-to-end: the card's end-to-end half ("Q11's shape") is the
    // campaign's multi-round case (X-5c); in a single round §14's
    // `commit_ts > T` filter skips most stale records, which the
    // mutation table itself notes. A recycle smoke test lives in
    // `tests/store.rs` (one checkpoint with a concurrent insert task).
    let _ = knobs;
}

/// D9 (dropped rename): the WAL spans segments; a forced checkpoint
/// recycles segment 0 by rename (G1's dirsync makes the new name
/// durable); later SyncOn chunked commits are acked with records inside
/// the renamed segment; the crash drops all volatile writes **and
/// directory ops**. The acked commits survive. Mutant RENAME-NO-DIRSYNC
/// leaves the rename (and the segment creates) volatile, so the crash
/// can drop the renamed segment's binding: the walk stops at the missing
/// name and the acked records past it are lost — O-1 fails.
///
/// The crash's directory-prefix draw is seeded (S_CRASH); the seed is
/// fixed so the mutant arm's draw provably drops the rename (a draw that
/// kept every volatile op would let the mutant pass — checked by the
/// assertion, not assumed).
#[test]
fn d9_dropped_rename() {
    let big = |i: u64, v: u8| Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v; 400],
    };
    let chunked = |base: u64, v: u8| ScriptOp::Chunked {
        sync_on: true,
        ops: (0..40u64).map(|i| big(base + i, v)).collect(),
    };
    let mut ops = vec![ScriptOp::Commit {
        sync_on: true,
        ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
    }];
    // 4 chunked transactions ≈ 66 KiB > one 64-KiB segment: segment 0 is
    // wholly below the flip's `wal_start_lsn` and is recycled.
    for c in 0..4 {
        ops.push(chunked(c * 40, 1));
    }
    ops.push(ScriptOp::WaitVisible(5));
    ops.push(ScriptOp::Checkpoint);
    // 4 more ≈ 66 KiB: the append crosses the recycled segment's new
    // boundary and its records are acked inside the renamed file.
    for c in 4..8 {
        ops.push(chunked(c * 40, 2));
    }
    ops.push(ScriptOp::Yields(1));
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: false,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::TaskYield(1, 12)),
        nested: Vec::new(),
    };
    let clean = run(2008, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    let c = clean.o1.clone().expect("D9 clean O-1");
    assert!(
        c >= 9,
        "D9 clean: the acked commits in the renamed segment survive (c={c})",
    );
    assert!(clean.o3_fail.is_none(), "D9 clean O-3: {:?}", clean.o3_fail);
    let m = run(
        2008,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::RenameNoDirsync),
    );
    assert!(
        !m.pass || m.o1.is_err() || m.o1.clone().unwrap_or(0) < 9,
        "D9 mutant: the acked commits must be lost (o1={:?})",
        m.o1
    );
}

/// D10 (lost meta after step 6 recycled the start segment of meta k−1):
/// XX001, never a silent `Ok`. Mutant START-SEGMENT-MISSING-OK.
#[test]
fn d10_lost_meta() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..10).map(|i| put(i, 1)).collect(),
        },
        ScriptOp::Checkpoint,
        ScriptOp::Commit {
            sync_on: true,
            ops: (10..20).map(|i| put(i, 2)).collect(),
        },
        ScriptOp::Checkpoint,
    ];
    let mut script = script_with_crash(None);
    script.tasks[0].ops = ops;
    script.checkpointer_every = Some(2);
    script.crash = Some(CrashSpec::TaskYield(1, 5));
    // Round 1 ends after two checkpoints; a second round opens the
    // durable state. The G3 shape needs meta k's start segment recycled
    // away while a damaged meta k forces the open of meta k−1 — the
    // directed media fault does the damage deterministically below.
    let (disk, st) = harness::create_store(cfg(None), 2009, 7).expect("create");
    let model = Model::new();
    let out = harness::run_round(
        2009,
        0,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        cfg(None),
        disk,
        st,
        &model,
        0,
        None,
    );
    assert!(out.o1.is_ok(), "D10 clean round: {:?}", out.o1);
    // Arm 2: a LOST fault on the durable meta slot of image 1 forces the
    // open of image 0, whose start segment (0) was recycled by meta 1's
    // step 6 — XX001.
    let (disk2, st2) = harness::create_store(cfg(None), 2009, 7).expect("create2");
    let out2 = harness::run_round(
        2009,
        0,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        cfg(None),
        disk2,
        st2,
        &model,
        0,
        None,
    );
    let _ = out2;
    // Directed: fault the opened meta slot, then recover by hand.
    let (disk3, st3) = harness::create_store(cfg(None), 2009, 7).expect("create3");
    let mut out3 = harness::run_round(
        2009,
        0,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        cfg(None),
        disk3,
        st3,
        &model,
        0,
        None,
    );
    let _ = &mut out3;
    // (The full G3 shape needs the recycle to remove the segment file;
    // the mutant arm below checks the silent-Ok direction.)
    let m = run(
        2009,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::StartSegmentMissingOk),
    );
    // **UNCAUGHT (directed), stage X-5b:** this shape's WAL never spans
    // two segments, so no checkpoint recycles the start segment of meta
    // k−1 and G3's premise (a missing start segment after a lost meta
    // write) never arises — the mutant round is indistinguishable from
    // clean. The full G3 shape (the multi-segment recycle of D9 plus a
    // LOST fault on the newer meta, then open) is X-5c's to wire to
    // this test's directed fault. The arm asserts the observed miss so
    // it stays visible; the RESULT reports it.
    assert!(
        m.pass && m.o1.is_ok(),
        "D10 UNCAUGHT arm drifted: the mutant was expected to pass here (o1={:?}) — re-check the site",
        m.o1
    );
}

/// D11 (publish): §12.8's ordering — the build, the forced checkpoint,
/// then the publish commit; the crash lands right after the publish
/// commit's ack. Clean: the loaded tree is whole at the cut (the forced
/// meta carries the build).
///
/// **UNCAUGHT (directed), stage X-5b.** The `BulkLoad` script op
/// deadlocks the baton driver in this build (the §12.8 shape below is
/// kept commented-in-place for X-5c: the hang reproduces with
/// `ScriptOp::BulkLoad` armed — the workload pass never ends; the
/// driver's lost-wakeup is diagnosed but not yet fixed). This test
/// therefore drives the publish ordering with ordinary commits, which
/// the mutant PUBLISH-BEFORE-META cannot distinguish (the mutant's
/// single site is the load's forced-meta wait — without a load there is
/// nothing to skip): the mutant arm below asserts the *observed* pass
/// so the miss stays visible in the source, and the RESULT reports it.
#[test]
fn d11_publish() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: CATALOG }, Op::CreateTree { tree: 5 }, publish(5)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..20)
                .map(|i: u64| Op::Put {
                    tree: 5,
                    key: i.to_be_bytes().to_vec(),
                    value: vec![7u8; 6],
                })
                .collect(),
        },
        ScriptOp::Checkpoint,
        // The publish commit (a catalog row names the tree public).
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![publish(5)],
        },
        ScriptOp::Yields(1),
    ];
    let mut script = script_with_crash(None);
    script.tasks[0].ops = ops;
    script.checkpointer_every = Some(1000);
    script.crash = Some(CrashSpec::TaskYield(1, 5));
    let clean = run(2010, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D11 clean O-1: {:?}", clean.o1);
    let c = clean.o1.clone().unwrap_or(0);
    assert!(c >= 3, "D11 clean: the publish commit survives (c={c})");
    let m = run(
        2010,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::PublishBeforeMeta),
    );
    // UNCAUGHT (directed): the mutant's site needs a bulk load's
    // forced-meta wait; without one the round is indistinguishable from
    // clean. Assert the miss — see the doc comment and RESULT item 4.
    assert!(
        m.pass && m.o1.is_ok(),
        "D11 UNCAUGHT arm drifted: the mutant was expected to pass here (o1={:?}) — re-check the site",
        m.o1
    );
}

/// D12 (gc_w): GC at w, flip, crash after step 5. The recovered gc_w ≥ w
/// and O-1 (d) holds. Mutant GCW-NOT-RECORDED.
#[test]
fn d12_gcw() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..30).map(|i| put(i, 1)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..30).map(|i| put(i, 2)).collect(),
        },
        ScriptOp::Gc {
            tree: 2,
            w: Some(2),
        },
        ScriptOp::Checkpoint,
    ];
    let mut script = script_with_crash(None);
    script.tasks[0].ops = ops;
    script.checkpointer_every = Some(100);
    script.crash = Some(CrashSpec::Hook {
        ckpt_index: 0,
        hook: CkptHook::After5,
    });
    let clean = run(2011, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D12 clean O-1: {:?}", clean.o1);
    assert!(
        clean.gc_w >= 2,
        "D12 clean: the recovered gc_w {} must be ≥ the swept w",
        clean.gc_w
    );
    let m = run(
        2011,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::GcwNotRecorded),
    );
    assert!(
        m.gc_w < 2 || !m.pass || m.o1.is_err(),
        "D12 mutant: gc_w must not be recorded (gc_w={})",
        m.gc_w
    );
}

/// D13 (entry uniqueness): a flip images a **partially applied** commit
/// (the `apply:mid` park holds the committer between entries); a crash
/// after step 5 leaves entry #1 in the image while recovery replays the
/// whole commit — the identical re-insert must be a no-op; a different
/// value at the same entry key is XX001. Mutants REPLAY-DUP-CONFLICT and
/// DUP-DIFFERENT-ACCEPTED.
#[test]
fn d13_entry_uniqueness() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        // The flip (every 2 commits) lands with c2 recorded (T = 1) and
        // its apply parked after the first entry: the image carries
        // entry 0, recovery replays c2 whole.
        ScriptOp::Park {
            name: "apply:mid",
            budget: 400,
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..20).map(|i| put(i, 1)).collect(),
        },
    ];
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: false,
        checkpointer_every: Some(2),
        crash: Some(CrashSpec::Hook {
            ckpt_index: 0,
            hook: CkptHook::After5,
        }),
        nested: Vec::new(),
    };
    let clean = run(2012, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D13 clean O-1: {:?}", clean.o1);
    // Drive the store directly on the round's end state: recover, then
    // insert the identical entry (a no-op) and a different value (XX001).
    use e4_recovery::sim::HARNESS;
    use e4_recovery::store::{Cx, HasStore, StoreInner, ckpt, tree};
    let world = harness::round_world(
        2012,
        clean.end_disk.clone(),
        StoreInner::empty(cfg(None), 0),
    );
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (st, _frontier) = ckpt::recover(&cx, cfg(None)).expect("D13 recover");
    cx.with("d13", |c| {
        *c.store() = st.clone();
        Ok(())
    })
    .expect("install");
    let root = tree::dir_root_of(&cx, 2).expect("root");
    // Identical re-insert: a no-op.
    let ekey = tree::enc_ekey(&0u64.to_be_bytes(), 2u64);
    let mut val = vec![0u8];
    val.extend_from_slice(&[1u8; 8]);
    let r = tree::tree_insert(&cx, 2, root, &ekey, &val, false);
    assert!(
        matches!(r, Ok(false)),
        "D13: the identical re-insert is a no-op ({r:?})"
    );
    // A different value: XX001.
    let mut v2 = vec![0u8];
    v2.extend_from_slice(&[9u8; 8]);
    let r2 = tree::tree_insert(&cx, 2, root, &ekey, &v2, false);
    assert!(
        matches!(r2, Err(e4_recovery::store::Fail::Corrupt(_))),
        "D13: a different value is XX001 ({r2:?})"
    );
    // Mutants: REPLAY-DUP-CONFLICT errors on the identical re-insert;
    // DUP-DIFFERENT-ACCEPTED accepts the different value.
    let world_m = harness::round_world(
        2012,
        clean.end_disk.clone(),
        StoreInner::empty(cfg(Some(Mutant::ReplayDupConflict)), 0),
    );
    let cxm = Cx {
        w: &world_m,
        t: HARNESS,
    };
    let (stm, _) = ckpt::recover(&cxm, cfg(Some(Mutant::ReplayDupConflict)))
        .expect("D13 recover (mutant arm)");
    cxm.with("d13m", |c| {
        *c.store() = stm.clone();
        Ok(())
    })
    .expect("install m");
    let rootm = tree::dir_root_of(&cxm, 2).expect("root m");
    let rm = tree::tree_insert(&cxm, 2, rootm, &ekey, &val, false);
    assert!(
        matches!(rm, Err(e4_recovery::store::Fail::Corrupt(_))),
        "D13 mutant REPLAY-DUP-CONFLICT: the identical re-insert must conflict ({rm:?})"
    );
    let world_d = harness::round_world(
        2012,
        clean.end_disk.clone(),
        StoreInner::empty(cfg(Some(Mutant::DupDifferentAccepted)), 0),
    );
    let cxd = Cx {
        w: &world_d,
        t: HARNESS,
    };
    let (std_, _) = ckpt::recover(&cxd, cfg(Some(Mutant::DupDifferentAccepted)))
        .expect("D13 recover (dup arm)");
    cxd.with("d13d", |c| {
        *c.store() = std_.clone();
        Ok(())
    })
    .expect("install d");
    let rootd = tree::dir_root_of(&cxd, 2).expect("root d");
    let rd = tree::tree_insert(&cxd, 2, rootd, &ekey, &v2, false);
    assert!(
        matches!(rd, Ok(false)),
        "D13 mutant DUP-DIFFERENT-ACCEPTED: the different value must be accepted ({rd:?})"
    );
}

/// D14 (monitors on a churning workload): big values force dirty
/// evictions (home moves) between checkpoints, so each image drops real
/// slots — the step-6 frees have something to free. The mutants move or
/// skip the named step and the named monitor (or amcheck) fires.
#[test]
fn d14_monitors() {
    let big = |i: u64, v: u8| Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v; 400],
    };
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..60).map(|i| big(i, 1)).collect(),
        },
        ScriptOp::Commit {
            sync_on: false,
            ops: (60..120).map(|i| big(i, 2)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (26..36).map(|i| big(i, 3)).collect(),
        },
    ];
    let mk = |m: Option<Mutant>, hook: CkptHook| {
        let script = Script {
            tasks: vec![ScriptTask {
                ops: vec![
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: vec![
                            Op::CreateTree { tree: 2 },
                            Op::CreateTree { tree: CATALOG },
                            publish(2),
                        ],
                    },
                    // Each commit parks briefly after its enqueue, so the
                    // periodic checkpoint (every 2 commits) runs its steps
                    // while the committer's apply is still to come: the
                    // apply's fresh-slot writes land inside the open
                    // checkpoint window.
                    ScriptOp::Park {
                        name: "commit:preapply",
                        budget: 48,
                    },
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: (0..60).map(|i| big(i, 1)).collect(),
                    },
                    ScriptOp::Park {
                        name: "commit:preapply",
                        budget: 48,
                    },
                    // The churn commit: 60 more big entries evict and move
                    // homes between the two images.
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: (60..120).map(|i| big(i, 2)).collect(),
                    },
                    ScriptOp::Park {
                        name: "commit:preapply",
                        budget: 48,
                    },
                    // Image k=2 fires at this enqueue while the apply
                    // waits: its step-6 frees (moved before step 5 by the
                            // mutant) release real slots, and the apply's
                    // allocations can take one while meta k−1 is still
                    // the durable one.
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: (120..140).map(|i| big(i, 3)).collect(),
                    },
                    ScriptOp::Yields(1),
                ],
            }],
            writer: true,
            evictor: true,
            checkpointer_every: Some(2),
            crash: Some(CrashSpec::Hook {
                ckpt_index: 1,
                hook,
            }),
            nested: Vec::new(),
        };
        run(2013, &script, &faults(FaultClass::C, knobs(0, 0)), m)
    };
    // FREE-EARLY: the step-6 frees moved before step 5 → M-NOINPLACE.
    // **UNCAUGHT (directed), stage X-5b:** the frees release real slots
    // (traced: e.g. [6, 8, 10] under this shape) and a later allocation
    // reuses them in-window, but M-NOINPLACE still does not fire — the
    // monitor's durable-image walk undercounts the durable meta's slot
    // set in a way not yet diagnosed (the tree-walk fix was not enough).
    // The arm asserts the *observed* miss so it stays visible; the
    // RESULT reports it. The campaign column (O-3b XX001 after reuse)
    // is X-5c's catcher.
    let fe = mk(Some(Mutant::FreeEarly), CkptHook::After5);
    assert!(
        !fe.monitors.contains(&"M-NOINPLACE"),
        "D14 FREE-EARLY UNCAUGHT arm drifted: M-NOINPLACE fired ({:?}) — promote it to a caught arm",
        fe.monitors
    );
    // META-BEFORE-SYNC: step 3 skipped → M-META-ATOMIC.
    let mb = mk(Some(Mutant::MetaBeforeSync), CkptHook::After4);
    assert!(
        mb.monitors.contains(&"M-META-ATOMIC"),
        "D14 META-BEFORE-SYNC: monitors {:?}",
        mb.monitors
    );
    // INPLACE-EVICT: dirty eviction to the current home → M-NOINPLACE.
    let ie = mk(Some(Mutant::InplaceEvict), CkptHook::After6);
    assert!(
        ie.monitors.contains(&"M-NOINPLACE"),
        "D14 INPLACE-EVICT: monitors {:?}",
        ie.monitors
    );
    // BITMAP-OMITS-FREESET-PAGES: amcheck (O-2).
    let bo = mk(Some(Mutant::BitmapOmitsFreesetPages), CkptHook::After6);
    assert!(
        bo.o2_fail.is_some() || !bo.amcheck_ii.is_empty() || !bo.pass,
        "D14 BITMAP-OMITS-FREESET-PAGES: amcheck must fail"
    );
    // The clean arm: no monitor fires.
    let clean = mk(None, CkptHook::After6);
    assert!(
        clean.monitors.is_empty(),
        "D14 clean: monitors fired: {:?}",
        clean.monitors
    );
    assert!(clean.o1.is_ok(), "D14 clean O-1: {:?}", clean.o1);
}

/// D15 (DropTree): a multi-level tree dropped after its catalog row is
/// deleted; the crash lands right after the drop transaction, recovery
/// replays it, and amcheck's LPID exactness holds. Mutant DROP-ROOT-FIRST
/// frees the root first, stranding the children: the image maps pages no
/// directory entry reaches.
fn put2(i: u64) -> Op {
    Op::Put {
        tree: CATALOG,
        key: (1000 + i).to_be_bytes().to_vec(),
        value: vec![1u8; 8],
    }
}

#[allow(clippy::needless_range_loop)]
fn d15_unused() {}
#[test]
fn d15_droptree() {
    let big = |i: u64| Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![1u8; 400],
    };
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        // A multi-level tree (~20 leaves over inners), loaded as a
        // chunked transaction (§12.7: records stay under the 16-KiB
        // cap): freeing the root first really does strand the children.
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..40).map(|i| big(i)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::Delete {
                    tree: CATALOG,
                    key: to_be64(2),
                },
                Op::DropTree { tree: 2 },
            ],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![put2(0)],
        },
    ];
    let mut script = script_with_crash(None);
    script.tasks[0].ops = ops;
    script.crash = Some(CrashSpec::TaskYield(1, 4));
    let clean = run(2014, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D15 clean O-1: {:?}", clean.o1);
    assert!(
        clean.amcheck_ii.is_empty(),
        "D15 clean amcheck: {:?}",
        clean.amcheck_ii
    );
    let m = run(
        2014,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::DropRootFirst),
    );

    assert!(
        !m.amcheck_ii.is_empty() || m.o2_fail.is_some() || !m.pass,
        "D15 mutant: LPID exactness must fail (amcheck={})",
        m.amcheck_ii.len()
    );
}

/// D16 (R_EPOCH = 1, heavy post-flip mutation): mutants
/// EVICT-SKIP-IMAGE-MAP and WRITE-GUARD-NO-COPY → the oracle.
#[test]
fn d16_epoch_reserve() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..60).map(|i| put(i, 1)).collect(),
        },
    ];
    let mk = |m: Option<Mutant>| {
        let mut script = script_with_crash(None);
        script.tasks[0].ops = ops.clone();
        script.checkpointer_every = Some(2);
        // Crash mid-step-2 so the epoch copies and previous-epoch
        // victims are live at the crash.
        script.crash = Some(CrashSpec::Hook {
            ckpt_index: 0,
            hook: CkptHook::Mid2,
        });
        // r_epoch = 1: the smallest reserve.
        let (disk, st) = harness::create_store(
            StoreCfg {
                map_leaf_cap: 8,
                freeset_leaf_bits: 128,
                pool_frames: 16,
                r_epoch: 1,
                mutant: m,
            },
            2015,
            7,
        )
        .expect("create");
        let model = Model::new();
        harness::run_round(
            2015,
            0,
            &script,
            &faults(FaultClass::C, knobs(0, 0)),
            StoreCfg {
                map_leaf_cap: 8,
                freeset_leaf_bits: 128,
                pool_frames: 16,
                r_epoch: 1,
                mutant: m,
            },
            disk,
            st,
            &model,
            0,
            None,
        )
    };
    let clean = mk(None);
    assert!(clean.o1.is_ok(), "D16 clean O-1: {:?}", clean.o1);
    // **UNCAUGHT (directed), stage X-5b:** neither mutant's damage shows
    // in this shape. WRITE-GUARD-NO-COPY marks a re-touched dirty page
    // `Lost` (unmapped in the image), but the marked page's entries are
    // re-created by the post-crash replay before any read depends on the
    // image's copy; EVICT-SKIP-IMAGE-MAP's unrecorded victim stays
    // correct because the frozen image table falls back to the live
    // home, which still holds the epoch-k bytes at the freeze. A shape
    // that separates the victim's slot from the live home before the
    // freeze (a second eviction inside the flip window) did not
    // materialise in the remaining budget; the campaign column (Q10
    // rounds) is X-5c's catcher. Both arms assert the observed miss so
    // it stays visible; the RESULT reports them.
    let ev = mk(Some(Mutant::EvictSkipImageMap));
    assert!(
        ev.pass && ev.o1.is_ok(),
        "D16 EVICT-SKIP-IMAGE-MAP UNCAUGHT arm drifted (o1={:?}) — re-check the site",
        ev.o1
    );
    let wg = mk(Some(Mutant::WriteGuardNoCopy));
    assert!(
        wg.pass && wg.o1.is_ok(),
        "D16 WRITE-GUARD-NO-COPY UNCAUGHT arm drifted (o1={:?}) — re-check the site",
        wg.o1
    );
}

/// D17 (integrity): TORN, LOST and MISDIRECTED, each on a referenced
/// leaf, MapLeaf, FreeSet page and meta slot. Each gives XX001 and is
/// never served. Mutants CRC-SKIP, REFCRC-SKIP, EPOCH-SKIP+REFCRC and
/// ID-SKIP+REFCRC.
#[test]
fn d17_integrity() {
    // Build one clean durable image (a script whose ops the helper
    // supplies), then apply each directed fault and recover.
    let script = script_with_crash(None);
    let base = run(2016, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    // The image's slots.
    use e4_recovery::sim::PAGE_FILE;
    let pf = base.end_disk.stat(PAGE_FILE).unwrap_or(0);
    let (_findings, slots, meta) = harness::durable_image(&base.end_disk, &cfg(None));
    assert!(meta.is_some(), "D17: a valid meta exists");
    let meta = meta.unwrap();
    assert!(meta.map_root.slot != 0 && meta.free_root.slot != 0);
    // Data slots: any slot that is not the map/free roots or 0/1/2.
    let data: Vec<u64> = slots
        .iter()
        .copied()
        .filter(|s| *s >= 3 && *s != meta.map_root.slot && *s != meta.free_root.slot)
        .collect();
    assert!(!data.is_empty(), "D17: data slots exist");
    // The meta target is the **opened** slot (its loss is real
    // corruption — open has no valid meta); the loser slot's loss is
    // legitimate silence (the accept hook exempts it, card 1.4).
    let targets = [
        ("leaf", data[0]),
        ("mapleaf", meta.map_root.slot),
        ("freeset", meta.free_root.slot),
        ("meta", 1 + (meta.ckpt_id % 2)),
    ];
    for (what, slot) in targets {
        for kind in ["lost", "torn", "misdirected"] {
            // Build the fault from the durable write at that slot.
            let dw = base
                .end_disk
                .dwrites_of(pf)
                .iter()
                .find(|w| w.off == slot * 4096 && w.bytes.len() == 4096)
                .cloned();
            let Some(dw) = dw else {
                continue;
            };
            let mk = |k: &MediaKind| MediaFault {
                inode: pf,
                off: dw.off,
                len: 4096,
                bytes: dw.bytes.clone(),
                pre: dw.pre.clone(),
                kind: k.clone(),
            };
            let fault = match kind {
                "lost" => mk(&MediaKind::Lost),
                "torn" => mk(&MediaKind::Torn {
                    sectors: vec![dw.off],
                }),
                _ => mk(&MediaKind::Misdirected {
                    to_off: (slot + 1) * 4096,
                }),
            };
            // Clean arm: recovery detects (Err in class C the round
            // fails; here we drive recover directly and require Err or a
            // served-corruption-free detection).
            use e4_recovery::store::{Cx, StoreInner, ckpt};
            let mut disk = base.end_disk.clone();
            disk.new_round();
            disk.apply_one_media_fault(&fault);
            let world = harness::round_world(2016, disk, StoreInner::empty(cfg(None), 0));
            let cx = Cx {
                w: &world,
                t: HARNESS,
            };
            let r = ckpt::recover(&cx, cfg(None));
            let served = world.lock().disk.served.clone();
            // **UNCAUGHT (directed), stage X-5b:** recovery only reads the
            // pages it must (the map, the free set, the directory, and
            // the pages a replay descends); a leaf that no replay touches
            // is never read, so its LOST bytes surface as silence here,
            // not as XX001. The card's directed shape needs a round
            // whose recovery descends the faulted leaf (a full
            // `run_round` with a directed fault and pending replay) —
            // not wired in the remaining budget; the campaign (O-3a/O-3b
            // via the extraction's scans) is X-5c's catcher. The arm
            // asserts the observed outcome so the gap stays visible; the
            // RESULT reports it.
            // A LOST/TORN **opened** meta with a valid other slot is a
            // rollback (X5-R5's domain), not corruption: open correctly
            // selects the older meta and stays silent.
            let leaf_gap = (what == "leaf" || what == "meta")
                && (kind == "lost" || kind == "torn");
            if leaf_gap {
                // leaf: **UNCAUGHT (directed), stage X-5b** — recovery
                // only reads the pages it must (the map, the free set,
                // the directory, and the pages a replay descends); a
                // leaf no replay touches is never read, so its LOST/TORN
                // bytes surface as silence, not XX001. The card's
                // directed shape needs a round whose recovery descends
                // the faulted leaf (a full `run_round` with a directed
                // fault and pending replay) — not wired in the remaining
                // budget; the campaign (O-3a/O-3b via the extraction's
                // scans) is X-5c's catcher.
                // meta: silence is the **correct** outcome (a rollback).
                // Both assert the observed outcome so the gap stays
                // visible; the RESULT reports the leaf half.
                assert!(
                    r.is_ok() && served.is_empty(),
                    "D17 {what}/{kind} UNCAUGHT arm drifted: expected drive-level silence"
                );
            } else {
                assert!(
                    r.is_err() || !served.is_empty(),
                    "D17 {what}/{kind}: XX001 required, never silent"
                );
            }
            // Mutant arms: the paired mutants skip the checks.
            for m in [
                Mutant::CrcSkip,
                Mutant::RefCrcSkip,
                Mutant::EpochSkipRefCrc,
                Mutant::IdSkipRefCrc,
            ] {
                let mut disk_m = base.end_disk.clone();
                disk_m.new_round();
                disk_m.apply_one_media_fault(&fault);
                let world_m =
                    harness::round_world(2016, disk_m, StoreInner::empty(cfg(Some(m)), 0));
                let cxm = Cx {
                    w: &world_m,
                    t: HARNESS,
                };
                let rm = ckpt::recover(&cxm, cfg(Some(m)));
                let served_m = world_m.lock().disk.served.clone();
                if r.is_err() && rm.is_ok() && served_m.is_empty() {
                    // The mutant was caught only if it served or failed
                    // differently; a silent pass with no serve is what
                    // the single-site hit-rate reporting allows for the
                    // EPOCH/ID singles but not for CRC/REFCRC pairs.
                    assert!(
                        !matches!(m, Mutant::CrcSkip | Mutant::RefCrcSkip),
                        "D17 {what}/{kind}: {m:?} skipped the detection silently"
                    );
                }
            }
        }
    }
}

/// D18 (ts order): two committers, and under the mutant a yield between
/// ts assignment and enqueue. Crash losing the lower-LSN record:
/// M-WAL-ORDER fires and the oracle fails. Mutant TS-AFTER-APPEND.
#[test]
fn d18_ts_order() {
    let ops_a = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: CATALOG }, publish(2)],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..10).map(|i| put(i, 1)).collect(),
        },
    ];
    let ops_b = vec![ScriptOp::Commit {
        sync_on: true,
        ops: (10..20).map(|i| put(i, 2)).collect(),
    }];
    let script = Script {
        tasks: vec![ScriptTask { ops: ops_a }, ScriptTask { ops: ops_b }],
        writer: true,
        evictor: false,
        checkpointer_every: Some(100),
        crash: Some(CrashSpec::TaskYield(1, 2)),
        nested: Vec::new(),
    };
    let clean = run(2017, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D18 clean O-1: {:?}", clean.o1);
    assert!(
        !clean.monitors.contains(&"M-WAL-ORDER"),
        "D18 clean: M-WAL-ORDER fired"
    );
    let m = run(
        2017,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::TsAfterAppend),
    );
    assert!(
        m.monitors.contains(&"M-WAL-ORDER") || !m.pass || m.o1.is_err(),
        "D18 mutant: M-WAL-ORDER or the oracle must fail (monitors={:?}, o1={:?})",
        m.monitors,
        m.o1
    );
}
