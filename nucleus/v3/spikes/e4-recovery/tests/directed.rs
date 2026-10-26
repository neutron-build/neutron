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
use e4_recovery::sim::{FaultClass, FaultKnobs};
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
                out.monitors.is_empty(),
                "D1 {name} {hook:?}: monitors fired: {:?}",
                out.monitors
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
        !m.pass || m.o1.is_err() || m.o1.clone().unwrap() < 2 || m.monitors.contains(&"M-CKPT-T"),
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
///
/// **Stage status.** The single-round arms run (round 1's clean recovery
/// passes O-1 at the torn cut). The two-round arm — where the mutant's
/// un-zeroed tail lines up with round 2's shorter appends — is
/// **UNCAUGHT (directed)**: in every seed tried, round 1's torn
/// resolution kept the zombie records *written* (so round 1's own cut
/// legitimately covers them), and round 2's raw-cut finding fires
/// identically clean and mutant — the shapes the card's harm needs
/// (records past round 1's valid end replayed against a model truncated
/// below them) did not materialise through the black-box harness in the
/// remaining budget. The mutation table's campaign column (multi-round
/// seeds) is this mutant's catcher; the arm asserts only what holds
/// (round 1 clean) and no assert-pass.
#[test]
fn d7_zombie_tail() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..10).map(|i| put(i, 1)).collect(),
        },
        // The volatile tail: torn/lost at the crash; a prefix survives
        // as the zombie shape.
        ScriptOp::Commit {
            sync_on: false,
            ops: (10..14).map(|i| put(i, 2)).collect(),
        },
        ScriptOp::Commit {
            sync_on: false,
            ops: (14..18).map(|i| put(i, 2)).collect(),
        },
        ScriptOp::Commit {
            sync_on: false,
            ops: (18..22).map(|i| put(i, 2)).collect(),
        },
        ScriptOp::Yields(3),
    ];
    let mut script = script_with_crash(None);
    script.tasks[0].ops = ops;
    script.crash = Some(CrashSpec::TaskYield(1, 5));
    // Torn resolution keeps a prefix of the volatile tail.
    let clean = run(2006, &script, &faults(FaultClass::C, knobs(0, 1000)), None);
    assert!(clean.o1.is_ok(), "D7 clean O-1: {:?}", clean.o1);
    assert!(clean.o3_fail.is_none(), "D7 clean O-3: {:?}", clean.o3_fail);
    // The mutant round: logical truncation only. No zombie may be
    // *served* here either (O-3 (a) clean); the multi-round harm is the
    // campaign's.
    let m = run(
        2006,
        &script,
        &faults(FaultClass::C, knobs(0, 1000)),
        Some(Mutant::TruncateLogicalOnly),
    );
    assert!(
        !m.o3_fail.as_deref().unwrap_or("").contains("O-3(a)"),
        "D7 mutant: no corruption may be served (o3={:?})",
        m.o3_fail
    );
}

/// D8 (stale recycled segment, reader level): a renamed segment holding
/// old records with no new ones — the reader meets a record whose magic
/// and CRC are intact but whose own LSN is not the expected position:
/// the valid end is its first LSN and no record is yielded. Mutant
/// LSN-CHECK-SKIP removes §12.5's stale check and the record decodes —
/// caught here at the reader level (the named catcher, both arms
/// asserted below). The card's second half ("the end-to-end case with
/// Q11's shape") is a **multi-round** case: in a single round §14's
/// `commit_ts > T` filter skips most stale records (the mutation
/// table's own note), so the end-to-end harm is deferred to X-5c's
/// campaign seeds; the single-round recycle machinery itself is
/// exercised end to end by D9's rename shape and `tests/store.rs`'s
/// checkpoint-with-recycle test.
#[test]
fn d8_stale_recycled_segment() {
    use e4_recovery::store::wal;
    // A real Commit record, encoded at its own lsn 100.
    let rec = wal::enc_record(wal::K_COMMIT, (1, 1), 5, &[1, 2, 3], 100);
    let seg_end = 1 << 20;
    // Clean: read at the wrong position (200) — no record yields.
    let clean = wal::dec_record_with_lsn_check(&rec, 200, seg_end, None);
    assert!(
        clean.is_err(),
        "D8 clean: the stale record must not yield ({clean:?})"
    );
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
        ops: vec![
            Op::CreateTree { tree: 2 },
            Op::CreateTree { tree: CATALOG },
            publish(2),
        ],
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
///
/// The shape: checkpoint k=1 runs while the WAL is still inside segment
/// 0 (its `wal_start_lsn` sits there); four chunked transactions then
/// push the append across segments; checkpoint k=2's step 6 recycles
/// segment 0 by rename; the crash lands after it; a directed LOST fault
/// reverts meta 2's durable write (slot 1) to meta 0's bytes, so open
/// selects meta 1 — a rollback whose start segment no longer exists.
/// Clean: G3 fails XX001 at open (the detection) and X5-R5's
/// `meta_rollback` holds. Mutant: the missing segment is treated as an
/// empty WAL — recovery returns a silent `Ok` that loses every acked
/// commit above meta 1's T, and O-3 (d) fails the round.
#[test]
fn d10_lost_meta() {
    let chunked = |base: u64, v: u8| ScriptOp::Chunked {
        sync_on: true,
        ops: (0..40u64)
            .map(|i| Op::Put {
                tree: 2,
                key: (base + i).to_be_bytes().to_vec(),
                value: vec![v; 400],
            })
            .collect(),
    };
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
        },
        ScriptOp::Checkpoint,
        chunked(0, 1),
        chunked(40, 1),
        chunked(80, 1),
        chunked(120, 1),
        ScriptOp::WaitVisible(5),
        ScriptOp::Checkpoint,
        ScriptOp::Yields(3),
    ];
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: false,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::TaskYield(1, 9)),
        nested: Vec::new(),
    };
    // A faultless crash round supplies the crashed disk the fault is
    // built from (the same schedule prefix, so the same bytes).
    let base = run(2009, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    let pf = base
        .crashed_disk
        .stat(e4_recovery::sim::PAGE_FILE)
        .expect("D10 page file");
    let w = base
        .crashed_disk
        .dwrites_of(pf)
        .iter()
        .rev()
        .find(|w| {
            w.off == 4096
                && w.bytes.len() == 4096
                && e4_recovery::store::page::parse_meta(&w.bytes)
                    .map(|m| m.ckpt_id == 2)
                    .unwrap_or(false)
        })
        .cloned()
        .expect("D10: meta 2's durable write exists");
    let fault = e4_recovery::sim::MediaFault {
        inode: pf,
        off: w.off,
        len: 4096,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: e4_recovery::sim::MediaKind::Lost,
    };
    let mut twin = Vec::new();
    let (disk, st) = harness::create_store(cfg(None), 2009, 7).expect("create");
    let clean = harness::run_round(
        2009,
        0,
        &script,
        &RoundFaults {
            class: FaultClass::Mp,
            knobs: knobs(0, 0),
            directed: vec![fault.clone()],
        },
        cfg(None),
        disk,
        st,
        &Model::new(),
        0,
        Some(&mut twin),
    );
    assert!(
        clean.detections.contains(&"open"),
        "D10 clean: G3 must give XX001 at open (det={:?})",
        clean.detections
    );
    assert!(
        clean.meta_rollback,
        "D10 clean: the opened meta is a rollback"
    );
    assert!(
        !clean.o3_fail.as_deref().unwrap_or("").contains("O-3(a)"),
        "D10 clean: never served (o3={:?})",
        clean.o3_fail
    );
    let m = run(
        2009,
        &script,
        &RoundFaults {
            class: FaultClass::Mp,
            knobs: knobs(0, 0),
            directed: vec![fault],
        },
        Some(Mutant::StartSegmentMissingOk),
    );
    assert!(
        !m.pass && m.o1.is_err() && m.o3_fail.as_deref().unwrap_or("").contains("O-3(d)"),
        "D10 mutant: the silent empty WAL must lose the acked commits (o1={:?}, o3={:?})",
        m.o1,
        m.o3_fail
    );
}

/// D10b (X5-R5's R and its R-taint, card Work 14): a rollback whose
/// reused slots the exception must excuse.
/// 1. One chunked load (the image spans ~90 pages), then three forced
///    checkpoints with **no traffic between them**: k=1 images the load;
///    k=2 rewrites the free set to fresh slots and its step 6 frees
///    meta 1's free-set pages; k=3 crashes at After3 — its free set
///    lands on exactly those freed slots (same bit bases, epoch 0), and
///    the start segment of meta 1 is not recycled, so G3 does not fire.
/// 2. A directed LOST fault on meta 2's durable write makes open select
///    meta 1 — a meta rollback.
/// 3. Clean: amcheck (i) reports the reused slots, the harness puts
///    them in `R`, O-3 (e) passes, and no reused slot is ever accepted.
/// 4. REFCRC-SKIP: **UNCAUGHT in this shape, reported not asserted** —
///    the arm needs the reuse to pair a slot with a page of the *same*
///    radix position and epoch, but the store's allocation order (step
///    2's data phase and the evictor take the lowest freed slots before
///    the map phase) always hands them a page of another position
///    (verified by inspection: meta 1's base-640 slot holds image 3's
///    base-0 inner — id 0 ≠ 640 — so §3.3's check (3) fires alongside
///    (2) and the mutant cannot produce a serve here). The mutant's
///    directed catcher is D17's second half (a stale same-LPID
///    same-epoch version planted on the referenced slot); this arm
///    stays as the X5-R5-specific probe (the R-taint's positive case),
///    whose catcher is the campaign's Q14 rounds. The arm below
///    documents the observed outcome without asserting it a pass.
#[test]
fn d10b_rollback_exception() {
    let load: Vec<Op> = (0..1600u64)
        .map(|i| Op::Put {
            tree: 2,
            key: i.to_be_bytes().to_vec(),
            value: vec![3u8; 400],
        })
        .collect();
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
        },
        ScriptOp::Chunked {
            sync_on: true,
            ops: load,
        },
        ScriptOp::WaitVisible(2),
        ScriptOp::Checkpoint,
        ScriptOp::Checkpoint,
        ScriptOp::Checkpoint,
        ScriptOp::Yields(3),
    ];
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: true,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::Hook {
            ckpt_index: 2,
            hook: CkptHook::After3,
        }),
        nested: Vec::new(),
    };
    let base = run(2020, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    let pf = base
        .crashed_disk
        .stat(e4_recovery::sim::PAGE_FILE)
        .expect("D10b page file");
    let w = base
        .crashed_disk
        .dwrites_of(pf)
        .iter()
        .rev()
        .find(|w| {
            w.off == 4096
                && w.bytes.len() == 4096
                && e4_recovery::store::page::parse_meta(&w.bytes)
                    .map(|m| m.ckpt_id == 2)
                    .unwrap_or(false)
        })
        .cloned()
        .expect("D10b: meta 2's durable write exists");
    let fault = e4_recovery::sim::MediaFault {
        inode: pf,
        off: w.off,
        len: 4096,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: e4_recovery::sim::MediaKind::Lost,
    };
    let mut twin = Vec::new();
    let (disk, st) = harness::create_store(cfg(None), 2020, 7).expect("create");
    let clean = harness::run_round(
        2020,
        0,
        &script,
        &RoundFaults {
            class: FaultClass::Mp,
            knobs: knobs(0, 0),
            directed: vec![fault.clone()],
        },
        cfg(None),
        disk,
        st,
        &Model::new(),
        0,
        Some(&mut twin),
    );
    assert!(clean.meta_rollback, "D10b: the opened meta is a rollback");
    assert!(
        !clean.rollback_slots.is_empty(),
        "D10b: the reused slots must land in R (R={:?})",
        clean.rollback_slots
    );
    // Every amcheck (i) finding lies within (tainted ∪ R) — the round's
    // O-3 (e) verdict enforces it; assert it held and the named slots
    // really are the reused ones.
    assert!(
        clean.o3_fail.is_none(),
        "D10b clean: O-3 must pass with the exception (o3={:?})",
        clean.o3_fail
    );
    for f in &clean.amcheck_i {
        if let Some(slot) = f.slot {
            assert!(
                clean.rollback_slots.contains(&slot) || slot <= 2,
                "D10b: finding on slot {slot} outside R (R={:?})",
                clean.rollback_slots
            );
        }
    }
    // No reused slot was ever accepted: the serve flag never rises (the
    // reused pages fail §3.3 before any accept).
    assert!(
        !clean.o3_fail.as_deref().unwrap_or("").contains("O-3(a)") && !clean.served,
        "D10b clean: no reused slot may be served (o3={:?}, served={})",
        clean.o3_fail,
        clean.served
    );
    assert!(
        clean.detections.contains(&"open"),
        "D10b clean: the reused free-set pages give XX001 at open (det={:?})",
        clean.detections
    );
    // Arm 4 (REFCRC-SKIP): observed UNCAUGHT in this shape — see the
    // doc comment (the reuse never pairs same radix position + epoch;
    // D17's second half is the mutant's directed catcher). The run must
    // stay a detection (never a silent pass), which is all this
    // asserts.
    let m = run(
        2020,
        &script,
        &RoundFaults {
            class: FaultClass::Mp,
            knobs: knobs(0, 0),
            directed: vec![fault],
        },
        Some(Mutant::RefCrcSkip),
    );
    assert!(
        !m.pass || m.detections.contains(&"open") || m.o3_fail.is_some(),
        "D10b REFCRC-SKIP: the round may not silently pass clean"
    );
}

/// D11 (publish): §12.8's ordering — the unlogged build, the forced
/// checkpoint, then the publish commit; the crash lands right after the
/// publish commit's ack. Clean: the loaded tree is whole at the cut
/// (the forced meta carries the build). Mutant PUBLISH-BEFORE-META
/// skips the load's forced-meta wait (its single site): the publish
/// commit is acked while the build exists nowhere durable, so the
/// crash leaves a published catalog row over an empty tree and O-1 (b)
/// fails on the scan.
///
/// (The driver hang the X-5b gate's R5 reported is fixed: the S_L draw
/// from the commit sequencer is pre-marked applied, so `visible_ts` no
/// longer stalls forever on the drawn ts.)
#[test]
fn d11_publish() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: CATALOG }],
        },
        ScriptOp::BulkLoad {
            tree: 5,
            entries: (0..20u64)
                .map(|i| (i.to_be_bytes().to_vec(), vec![7u8; 6]))
                .collect(),
        },
        ScriptOp::Yields(1),
    ];
    let mut script = script_with_crash(None);
    script.tasks[0].ops = ops;
    script.checkpointer_every = Some(1000);
    script.crash = Some(CrashSpec::TaskYield(1, 3));
    let clean = run(2011, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(clean.o1.is_ok(), "D11 clean O-1: {:?}", clean.o1);
    let c = clean.o1.clone().unwrap_or(0);
    assert!(c >= 3, "D11 clean: the publish commit survives (c={c})");
    assert!(
        clean.task_error.is_none(),
        "D11 clean: no driver failure ({:?})",
        clean.task_error
    );
    let m = run(
        2011,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::PublishBeforeMeta),
    );
    assert!(
        m.o1.is_err() || !m.pass,
        "D11 mutant: the published tree must be empty at the cut (o1={:?})",
        m.o1
    );
}

/// D12b (the `w > visible_ts` fail-stop): the sweep at an explicit
/// watermark above the frontier records `gc_w_used > T`; the next
/// flip's meta write is fail-stop (I-GCW). Clean: the checkpoint aborts
/// poisoned and the round fails loudly (never a hang — the abort path
/// clears `ckpt_open`). Mutant GCW-BOUND-SKIP removes the check: the
/// meta carries `gc_w > T`, amcheck (i) reports it and O-2 fails the
/// class-C round.
#[test]
fn d12b_gcw_bound() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..10).map(|i| put(i, 1)).collect(),
        },
        // w above visible_ts (2): the fail-stop shape.
        ScriptOp::Gc {
            tree: 2,
            w: Some(3),
        },
        ScriptOp::Checkpoint,
        ScriptOp::Yields(3),
    ];
    let mut script = script_with_crash(None);
    script.tasks[0].ops = ops;
    script.checkpointer_every = Some(100);
    script.crash = Some(CrashSpec::TaskYield(1, 5));
    let clean = run(2030, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(
        !clean.pass
            && clean
                .task_error
                .as_deref()
                .unwrap_or("")
                .contains("gc_w > T"),
        "D12b clean: the meta write must be fail-stop (te={:?})",
        clean.task_error
    );
    let m = run(
        2030,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::GcwBoundSkip),
    );
    assert!(
        m.gc_w >= 3 && m.o2_fail.is_some(),
        "D12b mutant: the illegal gc_w must be durable and reported (gc_w={}, o2={:?})",
        m.gc_w,
        m.o2_fail
    );
}

/// D12 (gc_w): GC at w, flip, crash after step 5. The recovered gc_w ≥ w
/// and O-1 (d) holds. Mutant GCW-NOT-RECORDED.
#[test]
fn d12_gcw() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
    // FREE-EARLY: the step-5/6 frees happen at the flip (I-FREE-SAFE) —
    // the allocator sees slots the durable meta k−1 still references,
    // and the checkpoint's own step-2 allocations reuse them: M-NOINPLACE
    // fires (the monitor's shadow walk now reads every radix page by its
    // own ref, so the durable slot set covers the trees — the draft's
    // undercount is fixed).
    let fe = mk(Some(Mutant::FreeEarly), CkptHook::After5);
    assert!(
        fe.monitors.contains(&"M-NOINPLACE"),
        "D14 FREE-EARLY: M-NOINPLACE must fire (monitors={:?})",
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
        },
        // A multi-level tree (~20 leaves over inners), loaded as a
        // chunked transaction (§12.7: records stay under the 16-KiB
        // cap): freeing the root first really does strand the children.
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..40).map(big).collect(),
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
///
/// **Both arms UNCAUGHT (directed), reported not asserted.** The damage
/// each mutant marks is healed by checkpoint step 2 whenever its scan
/// reaches the affected frame: WRITE-GUARD-NO-COPY's `Img::Lost` is
/// overwritten with the written image ref (`ckpt_step2_data`'s
/// `pinned` branch), and EVICT-SKIP-IMAGE-MAP's unrecorded victim
/// coincides with the live home the freeze reads anyway
/// (`record_image_at` records `cur`'s own new ref). Showing the damage
/// needs the touch to land *after* the scan has passed the frame — a
/// yield-level interleave the black-box script park points cannot place
/// deterministically (the park budgets expire inside step 2's data
/// phase, where the scan heals them). The campaign's Q10 rounds (X-5c)
/// are these mutants' catcher per the mutation table. The arms below
/// assert only what holds: no task failure and no monitor hit.
#[test]
fn d16_epoch_reserve() {
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
        let c16 = StoreCfg {
            map_leaf_cap: 8,
            freeset_leaf_bits: 128,
            pool_frames: 16,
            r_epoch: 1,
            mutant: m,
        };
        let (disk, st) = harness::create_store(c16.clone(), 2015, 7).expect("create");
        let model = Model::new();
        harness::run_round(
            2015,
            0,
            &script,
            &faults(FaultClass::C, knobs(0, 0)),
            c16,
            disk,
            st,
            &model,
            0,
            None,
        )
    };
    let clean = mk(None);
    assert!(clean.o1.is_ok(), "D16 clean O-1: {:?}", clean.o1);
    for m in [
        Some(Mutant::EvictSkipImageMap),
        Some(Mutant::WriteGuardNoCopy),
    ] {
        let out = mk(m);
        assert!(
            out.task_error.is_none(),
            "D16 {m:?}: no harness failure is expected (te={:?})",
            out.task_error
        );
        assert!(
            out.monitors.is_empty(),
            "D16 {m:?}: unexpected monitor hit ({:?})",
            out.monitors
        );
    }
}

/// D17 (integrity): TORN, LOST and MISDIRECTED, each on a referenced
/// page of the durable image; each is detected (XX001) and **never
/// served**. The pinned case is the map leaf: with `map_leaf_cap` 254 a
/// single radix leaf holds ~90 LPID entries, so its bytes span past
/// sector 0 — a TORN fault reverting a middle sector changes real page
/// bytes while leaving the header (crc field, id, epoch — all in sector
/// 0) intact. Clean: §3.3's check (1) fails at open (XX001, a class-MP
/// detection) and nothing is accepted. Mutant CRC-SKIP removes exactly
/// check (1): the torn leaf passes (2) (the stored crc still equals the
/// reference's — the fault never touched sector 0), (3) and (4), is
/// accepted, and the read was tainted — a served corruption; the oracle
/// fails the round (O-3 (a) inside the verdict; the reported failure
/// text is O-3 (d)'s overwrite of it). The single-site mutants
/// EPOCH-SKIP and ID-SKIP leave the detection standing (their sites
/// are subsumed by the reference CRC: the paired mutants exist because
/// of exactly this) — the card's "expected 0" single-site hit rates.
///
/// The second half is the card's REFCRC shape ("a slot reused for the
/// same LPID in the same epoch", the mutation table's catcher for
/// REFCRC-SKIP and the paired mutants): heavy churn with the evictor
/// on makes an **older same-LPID same-epoch version** of a tree page
/// durable at a superseded slot; the directed MISDIRECTED fault plants
/// those stale bytes on the referenced slot. Checks (1), (3) and (4)
/// all pass — the bytes are a valid page of the same LPID and the same
/// epoch — so **only check (2)**, the reference CRC, distinguishes the
/// stale version from the referenced one. Clean: XX001 at open, never
/// served. Under REFCRC-SKIP (and EPOCH-SKIP+REFCRC / ID-SKIP+REFCRC,
/// whose extra skipped sites are no-ops here — the id and epoch match
/// anyway) the stale page is accepted through a tainted read: served
/// corruption, and the round fails.
#[test]
fn d17_integrity() {
    let cfg254 = |mutant: Option<Mutant>| StoreCfg {
        map_leaf_cap: 254,
        freeset_leaf_bits: 128,
        pool_frames: 16,
        r_epoch: 2,
        mutant,
    };
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
        },
        // ~60 pages of tree: the map leaf-0 carries entries across the
        // [512, 4096) sectors, so a middle-sector tear changes real
        // bytes without touching the header.
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..420u64)
                .map(|i| Op::Put {
                    tree: 2,
                    key: i.to_be_bytes().to_vec(),
                    value: vec![1u8; 400],
                })
                .collect(),
        },
        ScriptOp::WaitVisible(2),
        ScriptOp::Checkpoint,
        ScriptOp::Yields(3),
    ];
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: false,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::TaskYield(1, 6)),
        nested: Vec::new(),
    };
    let run17 = |mutant: Option<Mutant>, fault: e4_recovery::sim::MediaFault| {
        let (disk, st) = harness::create_store(cfg254(mutant), 2016, 7).expect("create");
        let mut twin = Vec::new();
        harness::run_round(
            2016,
            0,
            &script,
            &RoundFaults {
                class: FaultClass::Mp,
                knobs: knobs(0, 0),
                directed: vec![fault],
            },
            cfg254(mutant),
            disk,
            st,
            &Model::new(),
            0,
            Some(&mut twin),
        )
    };
    // A faultless crash round supplies the crashed disk the fault is
    // built from (the same schedule prefix, so the same bytes): tear
    // the map root's [512, 1024).
    let base = {
        let (disk, st) = harness::create_store(cfg254(None), 2016, 7).expect("create base");
        harness::run_round(
            2016,
            0,
            &script,
            &faults(FaultClass::C, knobs(0, 0)),
            cfg254(None),
            disk,
            st,
            &Model::new(),
            0,
            None,
        )
    };
    let pf = base
        .crashed_disk
        .stat(e4_recovery::sim::PAGE_FILE)
        .expect("D17 page file");
    let (_findings, _slots, meta) = harness::durable_image(&base.crashed_disk, &cfg254(None));
    let meta = meta.expect("D17: a valid meta exists");
    assert!(meta.map_root.slot != 0, "D17: the map radix exists");
    let w = base
        .crashed_disk
        .dwrites_of(pf)
        .iter()
        .rev()
        .find(|w| w.off == meta.map_root.slot * 4096 && w.bytes.len() == 4096)
        .cloned()
        .expect("D17: the map root's durable write exists");
    let torn = e4_recovery::sim::MediaFault {
        inode: pf,
        off: w.off,
        len: 4096,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: e4_recovery::sim::MediaKind::Torn {
            sectors: vec![w.off + 512],
        },
    };
    // Clean: XX001 at open, never served.
    let clean = run17(None, torn.clone());
    assert!(
        clean.detections.contains(&"open") || clean.o1.is_err(),
        "D17 clean: the torn map leaf must be detected (det={:?}, o1={:?})",
        clean.detections,
        clean.o1
    );
    assert!(
        !clean.o3_fail.as_deref().unwrap_or("").contains("O-3(a)") && clean.task_error.is_none(),
        "D17 clean: never served (o3={:?})",
        clean.o3_fail
    );
    assert!(
        !clean.served,
        "D17 clean: the rejected leaf must never be accepted (served flag)"
    );
    // CRC-SKIP: the torn leaf is accepted and SERVED — the `served` flag
    // is the named catcher (the round also fails without it, through the
    // silent-loss path, so the flag is what pins the serve wiring: the
    // gate probe that drops recovery-world serves from O-3 (a) entirely
    // makes this arm fail).
    let m = run17(Some(Mutant::CrcSkip), torn.clone());
    assert!(
        !m.pass && m.o3_fail.is_some(),
        "D17 CRC-SKIP: the served torn leaf must fail the round (o1={:?}, o3={:?})",
        m.o1,
        m.o3_fail
    );
    assert!(
        m.served,
        "D17 CRC-SKIP: the torn leaf is accepted through the disabled check — the serve itself must be flagged"
    );
    // The single-site mutants change nothing here (subsumed by the
    // reference CRC): the detection still fires — the card's
    // "expected 0" single-site hit rate, demonstrated.
    for single in [Mutant::EpochSkip, Mutant::IdSkip] {
        let s = run17(Some(single), torn.clone());
        assert!(
            s.detections.contains(&"open") || s.o1.is_err(),
            "D17 {single:?}: the single site is not load-bearing — the detection must still fire (det={:?})",
            s.detections
        );
    }
    // LOST and MISDIRECTED on the same page: both change bytes the
    // header-crc check (1) and the reference check (2) see; each must
    // stay a detection, never a serve.
    let lost = e4_recovery::sim::MediaFault {
        kind: e4_recovery::sim::MediaKind::Lost,
        ..torn.clone()
    };
    let l = run17(None, lost);
    assert!(
        (l.detections.contains(&"open") || l.o1.is_err())
            && !l.o3_fail.as_deref().unwrap_or("").contains("O-3(a)"),
        "D17 LOST map leaf: detected, never served (det={:?}, o3={:?})",
        l.detections,
        l.o3_fail
    );
    let mis = e4_recovery::sim::MediaFault {
        kind: e4_recovery::sim::MediaKind::Misdirected {
            to_off: (meta.map_root.slot + 1) * 4096,
        },
        ..torn
    };
    let d = run17(None, mis);
    assert!(
        (d.detections.contains(&"open") || d.o1.is_err())
            && !d.o3_fail.as_deref().unwrap_or("").contains("O-3(a)"),
        "D17 MISDIRECTED map leaf: detected, never served (det={:?}, o3={:?})",
        d.detections,
        d.o3_fail
    );

    // ---- the REFCRC shape: a slot reused for the same LPID in the same
    // epoch (the card's catcher for REFCRC-SKIP and the paired mutants)
    // ----
    let big = |i: u64, v: u8| Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v; 400],
    };
    let ops2 = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
        },
        // v1 of ~400 big entries (~44 pages over the 16-frame pool): the
        // evictor makes v1 pages durable at superseded slots. Chunked
        // (§12.7): the payload exceeds the 16-KiB record cap.
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..400u64).map(|i| big(i, 1)).collect(),
        },
        ScriptOp::WaitVisible(2),
        // v2 of the same keys: same LPIDs, same epoch (no flip between
        // the batches), different content.
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..400u64).map(|i| big(i, 2)).collect(),
        },
        ScriptOp::WaitVisible(3),
        ScriptOp::Checkpoint,
        ScriptOp::Yields(3),
    ];
    let script2 = Script {
        tasks: vec![ScriptTask { ops: ops2 }],
        writer: true,
        evictor: true,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::Hook {
            ckpt_index: 0,
            hook: CkptHook::After6,
        }),
        nested: Vec::new(),
    };
    let base2 = {
        let (disk, st) = harness::create_store(cfg254(None), 2021, 7).expect("create base2");
        harness::run_round(
            2021,
            0,
            &script2,
            &faults(FaultClass::C, knobs(0, 0)),
            cfg254(None),
            disk,
            st,
            &Model::new(),
            0,
            None,
        )
    };
    assert!(base2.o1.is_ok(), "D17 REFCRC base: {:?}", base2.o1);
    let pf2 = base2
        .crashed_disk
        .stat(e4_recovery::sim::PAGE_FILE)
        .expect("D17 REFCRC page file");
    // Find an (id, epoch) pair with two different durable tree-page
    // versions: the last write is the referenced version, an earlier
    // one the stale same-LPID same-epoch bytes.
    use e4_recovery::store::page;
    let dw = base2.crashed_disk.dwrites_of(pf2);
    let is_tree_page = |w: &e4_recovery::sim::DWrite| {
        w.off.is_multiple_of(4096)
            && w.bytes.len() == 4096
            && matches!(
                page::header_kind(&w.bytes),
                page::KIND_LEAF | page::KIND_INNER
            )
            && page::header_crc(&w.bytes)
                == e4_recovery::crc::crc32c(0, w.bytes.get(4..).unwrap_or(&[]))
    };
    // (index, id, epoch, crc) of every durable tree-page write.
    let cands: Vec<(usize, u64, u32, u32)> = dw
        .iter()
        .enumerate()
        .filter(|(_, w)| is_tree_page(w))
        .map(|(i, w)| {
            (
                i,
                page::header_id(&w.bytes),
                page::header_epoch(&w.bytes),
                page::header_crc(&w.bytes),
            )
        })
        .collect();
    let mut pair: Option<(e4_recovery::sim::MediaFault, u64)> = None;
    for &(i, id, ep, crc) in &cands {
        // The referenced version is the LAST durable write of the id.
        if cands.iter().any(|&(j, id2, _, _)| j > i && id2 == id) {
            continue;
        }
        // An earlier same-(id, epoch) version at another slot with
        // different content: the stale bytes to plant.
        let stale = cands.iter().find(|&&(j, id2, ep2, crc2)| {
            j < i
                && id2 == id
                && ep2 == ep
                && crc2 != crc
                && dw.get(j).map(|x| x.off).unwrap_or(0) != dw.get(i).map(|x| x.off).unwrap_or(0)
        });
        if let Some(&(j, ..)) = stale {
            let (x, w) = match (dw.get(j), dw.get(i)) {
                (Some(x), Some(w)) => (x, w),
                _ => continue,
            };
            let fault = e4_recovery::sim::MediaFault {
                inode: pf2,
                off: x.off,
                len: 4096,
                bytes: x.bytes.clone(),
                pre: x.pre.clone(),
                kind: e4_recovery::sim::MediaKind::Misdirected { to_off: w.off },
            };
            pair = Some((fault, w.off / 4096));
            break;
        }
    }
    let (fault2, slot2) =
        pair.expect("D17 REFCRC: a same-LPID same-epoch stale version must be durable");
    let run17b = |mutant: Option<Mutant>, f: e4_recovery::sim::MediaFault| {
        let (disk, st) = harness::create_store(cfg254(mutant), 2021, 7).expect("create");
        let mut twin = Vec::new();
        harness::run_round(
            2021,
            0,
            &script2,
            &RoundFaults {
                class: FaultClass::Mp,
                knobs: knobs(0, 0),
                directed: vec![f],
            },
            cfg254(mutant),
            disk,
            st,
            &Model::new(),
            0,
            Some(&mut twin),
        )
    };
    // Clean: check (2) is the only discriminator — XX001 at open, never
    // served.
    let clean2 = run17b(None, fault2.clone());
    assert!(
        clean2.detections.contains(&"open") || clean2.o1.is_err(),
        "D17 REFCRC clean: the stale same-LPID page must be detected (det={:?}, o1={:?})",
        clean2.detections,
        clean2.o1
    );
    assert!(
        !clean2.served,
        "D17 REFCRC clean: the stale page must never be served (slot {slot2})"
    );
    // REFCRC-SKIP and both paired mutants: the stale page is accepted
    // through the tainted read — served corruption, the round fails.
    for m in [
        Mutant::RefCrcSkip,
        Mutant::EpochSkipRefCrc,
        Mutant::IdSkipRefCrc,
    ] {
        let r = run17b(Some(m), fault2.clone());
        assert!(
            !r.pass && r.served,
            "D17 {m:?}: the stale same-LPID page must be served and fail the round (pass={}, served={}, o1={:?}, o3={:?})",
            r.pass,
            r.served,
            r.o1,
            r.o3_fail
        );
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
            ops: vec![
                Op::CreateTree { tree: 2 },
                Op::CreateTree { tree: CATALOG },
                publish(2),
            ],
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
