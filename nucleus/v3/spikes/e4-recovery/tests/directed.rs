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
        no_r_taint: false,
    }
}

fn put(i: u64, v: u64) -> Op {
    Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v as u8; 8],
    }
}

/// P's F4 ruling (2026-10-26): an `o1` error is acceptable in a D17
/// cell only when its message is one of the read-failed forms — an
/// XX001 that surfaced through O-1's own reads (X-5c's pre-registered
/// X5-R6 case). Any other O-1 failure (a real scan mismatch) is silent
/// damage and fails the test.
fn is_read_failed(o1: &Result<u64, String>) -> bool {
    matches!(o1, Err(m)
        if m.starts_with("O-1(b): scan read failed")
            || m.starts_with("O-1(c): raw read failed")
            || m.starts_with("O-1(d): scan read failed"))
}

/// A put with an explicit value length (D7's record-length padding).
fn putv(i: u64, v: u8, n: usize) -> Op {
    Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v; n],
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
            assert!(
                out.pass,
                "D1 {name} {hook:?}: the clean round must pass (te={:?}, dl={}, baton={})",
                out.task_error, out.deadlock, out.baton
            );
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
    assert!(
        clean.pass,
        "D2 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
    let c = clean.o1.clone().expect("D2 clean O-1");
    assert!(c >= 3, "D2 clean: the chunked txn (ts 3) is whole (c={c})");
    // Mutant: START-LSN-MARKED-CHUNK drops the marked case.
    let m = run(
        2001,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::StartLsnMarkedChunk),
    );
    assert!(
        m.o1.is_err() || m.o1.clone().unwrap_or(0) < 3,
        "D2 mutant: the chunked txn must be lost — the named catcher, not any failure (o1={:?}, pass={})",
        m.o1,
        m.pass
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
    assert!(
        clean.pass,
        "D3 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
    let c = clean.o1.clone().expect("D3 clean O-1");
    assert!(c >= 3, "D3 clean: the chunked txn (ts 3) is whole (c={c})");
    let m = run(
        2002,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::StartLsnPastChunk),
    );
    assert!(
        m.o1.is_err() || m.o1.clone().unwrap_or(0) < 3,
        "D3 mutant: the chunked txn must be lost — the named catcher (o1={:?}, pass={})",
        m.o1,
        m.pass
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
    assert!(
        clean.pass,
        "D4 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
    let c = clean.o1.clone().expect("D4 clean O-1");
    assert!(c >= 2, "D4 clean: c1 recovered whole (c={c})");
    let m = run(
        2003,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::WalSyncTOnly),
    );
    assert!(
        m.o1.is_err() || m.o1.clone().unwrap_or(0) < 2 || m.monitors.contains(&"M-CKPT-T"),
        "D4 mutant: c1 must be lost or M-CKPT-T must fire — the named catchers (o1={:?}, monitors={:?})",
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
    assert!(
        clean.pass,
        "D5 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
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
/// an intact one; recover, append a record of the **same byte length**,
/// crash again — §12.5's position check lines the zombie up with the
/// new append (the gate's N6 construction). Round 1's tail records are
/// tuned: seven 190-byte puts make each record exactly 1536 bytes
/// (three sectors, at the §7.3 value cap), and the two padding commits
/// (a 19-byte c2 and a 399-byte filler) make the first tail start
/// sector-aligned at 1024 — so a directed TORN fault on tail 3's first
/// sector (offset 4096) leaves tail 4 intact at exactly
/// `valid_end + 1536`. (An in-place tear cannot be drawn
/// from F-CRASH here: the writer syncs every batch, so each sector has
/// a single volatile image and the draw is all-or-nothing per batch —
/// the directed fault is the deterministic equal of the random tear.)
/// Round 2 appends one 2560-byte commit at the valid end; the crash
/// drops volatile writes.
/// Clean: round 1's G2 physically zeroed the tail, so round 2's reader
/// stops at the zeros — no zombie replayed, the round passes.
/// Mutant TRUNCATE-LOGICAL-ONLY (recovery moves its append position
/// only): the zombie's bytes survive with a matching lsn, round 2's
/// recovery replays it, and the oracle fails O-1 on a commit round 2's
/// model (truncated at round 1's cut) no longer has.
#[test]
fn d7_zombie_tail() {
    let big = |i: u64, v: u8| Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v; 190],
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
        // The padding pair: c2 (10× 19-byte puts) and a 399-byte filler
        // put land the first tail record exactly at offset 1024.
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..10).map(|i| putv(i, 1, 19)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![putv(999, 5, 399)],
        },
        // Four 1536-byte tail records, sector-aligned.
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..7).map(|i| big(10 + i, 2)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..7).map(|i| big(20 + i, 2)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..7).map(|i| big(30 + i, 2)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..7).map(|i| big(40 + i, 2)).collect(),
        },
        ScriptOp::Yields(1),
    ];
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: false,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::TaskYield(1, 8)),
        nested: Vec::new(),
    };
    // The torn sector: tail 3's first sector (offset 1024 + 2·2560),
    // from the durable write covering it.
    let base = run(2006, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(
        base.pass,
        "D7 base: the faultless round passes (te={:?})",
        base.task_error
    );
    let wf = base
        .crashed_disk
        .stat("/wal/0000000000000000.log")
        .expect("D7: the WAL segment exists");
    let torn_at = 1024 + 2 * 1536;
    let w = base
        .crashed_disk
        .dwrites_of(wf)
        .iter()
        .rev()
        .find(|w| w.off <= torn_at && torn_at < w.off + w.bytes.len() as u64)
        .cloned()
        .expect("D7: the write covering tail 3's first sector");
    let fault = e4_recovery::sim::MediaFault {
        inode: wf,
        off: w.off,
        len: w.bytes.len() as u64,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: e4_recovery::sim::MediaKind::Torn {
            sectors: vec![torn_at],
        },
    };
    let mk1 = |mutant: Option<Mutant>| {
        let (disk, st) = harness::create_store(cfg(mutant), 2006, 7).expect("create");
        let mut twin = Vec::new();
        harness::run_round(
            2006,
            0,
            &script,
            &RoundFaults {
                class: FaultClass::Mw,
                knobs: knobs(0, 0),
                directed: vec![fault.clone()],
                no_r_taint: false,
            },
            cfg(mutant),
            disk,
            st,
            &Model::new(),
            0,
            Some(&mut twin),
        )
    };
    // Round 1: the valid end sits at tail 3 (torn), tail 4 intact past
    // it; both arms recover to the same cut (the zombie is unread
    // behind the valid end either way).
    let clean1 = mk1(None);
    assert!(
        clean1.pass && clean1.o1.is_ok(),
        "D7 round-1 clean: the torn cut recovers (o1={:?}, te={:?})",
        clean1.o1,
        clean1.task_error
    );
    let m1 = mk1(Some(Mutant::TruncateLogicalOnly));
    assert!(
        m1.pass && m1.o1.is_ok(),
        "D7 round-1 mutant: the single-round recovery still passes (o1={:?})",
        m1.o1
    );
    assert!(
        !m1.o3_fail.as_deref().unwrap_or("").contains("O-3(a)"),
        "D7 round-1 mutant: no corruption may be served (o3={:?})",
        m1.o3_fail
    );

    // Round 2: one 1536-byte commit (the same seven-put shape) appended
    // at the valid end, then a crash dropping every volatile write.
    let round2 = |mutant: Option<Mutant>| {
        let r1 = mk1(mutant);
        let script2 = Script {
            tasks: vec![ScriptTask {
                ops: vec![
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: (0..7).map(|i| big(50 + i, 9)).collect(),
                    },
                    ScriptOp::Yields(1),
                ],
            }],
            writer: true,
            evictor: false,
            checkpointer_every: Some(1000),
            crash: Some(CrashSpec::TaskYield(1, 2)),
            nested: Vec::new(),
        };
        harness::run_round(
            2007,
            1,
            &script2,
            &faults(FaultClass::C, knobs(0, 0)),
            cfg(mutant),
            r1.end_disk.clone(),
            r1.end_store.clone().expect("D7 round-1 end store"),
            &r1.end_model,
            r1.cut,
            None,
        )
    };
    // Clean: G2's zeros — the reader stops where the zombie was; the
    // round passes.
    let clean2 = round2(None);
    assert!(
        clean2.pass && clean2.o1.is_ok(),
        "D7 round-2 clean: the zeroed tail must not replay (o1={:?}, o3={:?}, te={:?})",
        clean2.o1,
        clean2.o3_fail,
        clean2.task_error
    );
    // Mutant: the zombie lines up positionally and is replayed — the
    // oracle fails on a commit round 2's model no longer has.
    let m2 = round2(Some(Mutant::TruncateLogicalOnly));
    assert!(
        !m2.pass && m2.o1.is_err(),
        "D7 round-2 mutant: the lined-up zombie must be replayed and fail O-1 (o1={:?}, pass={})",
        m2.o1,
        m2.pass
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
    assert!(
        clean.pass,
        "D9 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
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
        m.o1.is_err() || m.o1.clone().unwrap_or(0) < 9,
        "D9 mutant: the acked commits must be lost — the named catcher (o1={:?}, pass={})",
        m.o1,
        m.pass
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
            no_r_taint: false,
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
            no_r_taint: false,
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
///    checkpoints with **no traffic between them**: k=1 images the load,
///    k=2 rewrites the free set to fresh slots and its step 6 frees
///    meta 1's free-set pages, and k=3's own free-set allocations reuse
///    exactly those freed slots (the lowest free). The pairing is an
///    epoch-0 free-set page against an epoch-0 free-set ref at the
///    **same radix base and the same epoch with different bitmap
///    content** — the id and epoch checks pass, and §3.3's reference
///    CRC (check (2)) is what rejects the reuse in the clean arm (the
///    gate's N5 note: an earlier doc claimed a different radix base,
///    which would contradict arm 4's serve). k=3 crashes at After3; the
///    start segment of meta 1 is not recycled, so G3 does not fire.
/// 2. A directed LOST fault on meta 2's durable write makes open select
///    meta 1 — a meta rollback.
/// 3. Clean: amcheck (i) reports the reused slots, the harness puts
///    them in `R`, O-3 (e) passes, and no reused slot is ever accepted.
/// 4. REFCRC-SKIP (the card's own arm — the mutation table's
///    instrumentation check): the reuse pairs the same radix base and
///    epoch with different bitmap content, so the reference CRC is the
///    only discriminator; with it off the reused page is accepted
///    through the tainted read — the R-taint catches the serve and
///    O-3 (a) fails the round. ID-SKIP+REFCRC (arm 5) serves identically
///    (the gate's N5 refutation of the run-1 "UNCAUGHT-directed"
///    claim); D17's second half remains the same-LPID tree-page shape.
/// 5. The R-taint wiring pin (the gate's G5, call-site level): the same
///    ID-SKIP+REFCRC round with the R-taint forced off (`no_r_taint`)
///    **passes silently** — the serve goes unjudged. With the wiring in,
///    the same round fails O-3 (a); the two arms together pin the call
///    site (`r_taint_window_semantics` pins the function).
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
    let mk = |m: Option<Mutant>, no_r_taint: bool| {
        let (disk, st) = harness::create_store(cfg(m), 2020, 7).expect("create");
        let mut twin = Vec::new();
        harness::run_round(
            2020,
            0,
            &script,
            &RoundFaults {
                class: FaultClass::Mp,
                knobs: knobs(0, 0),
                directed: vec![fault.clone()],
                no_r_taint,
            },
            cfg(m),
            disk,
            st,
            &Model::new(),
            0,
            Some(&mut twin),
        )
    };
    let clean = mk(None, false);
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
        clean.pass && clean.o3_fail.is_none(),
        "D10b clean: the round must pass with the exception (o3={:?}, te={:?})",
        clean.o3_fail,
        clean.task_error
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
        !clean.served,
        "D10b clean: no reused slot may be served (o3={:?}, served={})",
        clean.o3_fail, clean.served
    );
    assert!(
        clean.detections.contains(&"open"),
        "D10b clean: the reused free-set pages give XX001 at open (det={:?})",
        clean.detections
    );
    // Arm 4 (REFCRC-SKIP alone — the card's own arm, the mutation
    // table's instrumentation check): the reuse pairs the same radix
    // base and epoch with different bitmap content, so check (2) is
    // the only discriminator; with it off, the reused page is accepted
    // through the tainted read — a serve the R-taint catches (O-3 (a)
    // fails the round).
    let refcrc = mk(Some(Mutant::RefCrcSkip), false);
    assert!(
        !refcrc.pass && refcrc.served,
        "D10b REFCRC-SKIP: the reused page must be served via the R-taint (pass={}, served={}, o3={:?})",
        refcrc.pass,
        refcrc.served,
        refcrc.o3_fail
    );
    // Arm 5 (ID-SKIP+REFCRC, the gate's N5 refutation): the same serve
    // with the id check off too.
    let idref = mk(Some(Mutant::IdSkipRefCrc), false);
    assert!(
        !idref.pass && idref.served,
        "D10b ID-SKIP+REFCRC: the reused slot must be served via the R-taint (pass={}, served={}, o3={:?})",
        idref.pass,
        idref.served,
        idref.o3_fail
    );
    // Arm 6 (the R-taint wiring pin, the gate's G5 at the call site):
    // the same round with the R-taint forced off passes silently — the
    // serve goes unjudged, which is exactly what the wiring exists to
    // prevent.
    let silent = mk(Some(Mutant::IdSkipRefCrc), true);
    assert!(
        silent.pass,
        "D10b G5 pin: without the R-taint the served corruption must pass silently (pass={}, o3={:?})",
        silent.pass, silent.o3_fail
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
        m.o1.is_err(),
        "D11 mutant: the published tree must be empty at the cut — O-1 fails (o1={:?}, pass={})",
        m.o1,
        m.pass
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
    // The abort path's `ckpt_open` clear (the gate's N7 pin): with the
    // clear removed, the stranded flag parks `wait_ckpt` forever and
    // the round ends in a scheduler DEADLOCK instead of the loud
    // fail-stop.
    assert!(
        !clean.deadlock,
        "D12b clean: the fail-stop must be loud, not a deadlock (dl={})",
        clean.deadlock
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
    assert!(
        clean.pass,
        "D12 clean: the round must pass (o1={:?})",
        clean.o1
    );
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
        m.gc_w < 2,
        "D12 mutant: gc_w must not be recorded — the named catcher (gc_w={})",
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
    assert!(
        clean.pass,
        "D13 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
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
                    // checkpoint window. The big commits are **chunked**
                    // (§12.2: 60 × 423 B ≈ 25 KiB is over the 16-KiB
                    // record cap), so their park point is the chunked
                    // committer's (`chunked:preapply`, after the marker's
                    // ack wait, before the apply — the same window).
                    ScriptOp::Park {
                        name: "chunked:preapply",
                        budget: 48,
                    },
                    ScriptOp::Chunked {
                        sync_on: true,
                        ops: (0..60).map(|i| big(i, 1)).collect(),
                    },
                    ScriptOp::Park {
                        name: "chunked:preapply",
                        budget: 48,
                    },
                    // The churn commit: 60 more big entries evict and move
                    // homes between the two images.
                    ScriptOp::Chunked {
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
        bo.o2_fail.is_some(),
        "D14 BITMAP-OMITS-FREESET-PAGES: O-2 must fail on the bitmap (o2={:?})",
        bo.o2_fail
    );
    // The clean arm: no monitor fires.
    let clean = mk(None, CkptHook::After6);
    assert!(
        clean.monitors.is_empty(),
        "D14 clean: monitors fired: {:?}",
        clean.monitors
    );
    assert!(
        clean.pass,
        "D14 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
}

/// N1 regression (the gate's 2026-10-26 round-2 verdict, finding 1): the
/// free set's own allocations can grow the file past the frozen
/// `slot_count`; §13 step 2's sizing note makes the checkpointer
/// recompute the page count after the growth and allocate the difference,
/// so the meta's `slot_count` covers every free-set slot it references.
/// The defect's shape (the verdict's): 60 puts with 200-byte values, then
/// SyncOff overwrites, a checkpoint every 3 commits, crash at After5 or
/// After6 of checkpoint 0 — clean class-C rounds whose amcheck (i)
/// reported "slot_count below a referenced slot" in 7 of 8 seeds at every
/// `R_EPOCH`. With the convergence loop, every seed is clean and passes.
#[test]
fn freeset_growth_covers_its_own_slots() {
    let big = |i: u64, v: u8| Op::Put {
        tree: 2,
        key: i.to_be_bytes().to_vec(),
        value: vec![v; 200],
    };
    for r_epoch in [1usize, 2, 4] {
        for hook in [CkptHook::After5, CkptHook::After6] {
            for seed in 2100u64..2108 {
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
                        ops: (0..60).map(|i| big(i, 1)).collect(),
                    },
                    // SyncOff overwrites: the evictions and the step-2
                    // writes churn the file while the periodic
                    // checkpoint images it.
                    ScriptOp::Commit {
                        sync_on: false,
                        ops: (0..60).map(|i| big(i, 2)).collect(),
                    },
                    ScriptOp::Yields(1),
                ];
                let script = Script {
                    tasks: vec![ScriptTask { ops }],
                    writer: true,
                    evictor: true,
                    checkpointer_every: Some(3),
                    crash: Some(CrashSpec::Hook {
                        ckpt_index: 0,
                        hook,
                    }),
                    nested: Vec::new(),
                };
                let c = StoreCfg {
                    map_leaf_cap: 8,
                    freeset_leaf_bits: 128,
                    pool_frames: 16,
                    r_epoch,
                    mutant: None,
                };
                let (disk, st) = harness::create_store(c.clone(), seed, 7).expect("create");
                let out = harness::run_round(
                    seed,
                    0,
                    &script,
                    &faults(FaultClass::C, knobs(0, 0)),
                    c,
                    disk,
                    st,
                    &Model::new(),
                    0,
                    None,
                );
                assert!(
                    out.pass && out.o2_fail.is_none() && out.amcheck_i.is_empty(),
                    "N1 r_epoch={r_epoch} {hook:?} seed={seed}: pass={} o2={:?} amcheck_i={:?} o1={:?} o3={:?} te={:?}",
                    out.pass,
                    out.o2_fail,
                    out.amcheck_i,
                    out.o1,
                    out.o3_fail,
                    out.task_error
                );
            }
        }
    }
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
    assert!(
        clean.pass,
        "D15 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
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
        !m.amcheck_ii.is_empty() || m.o2_fail.is_some(),
        "D15 mutant: LPID exactness must fail amcheck — the named catcher (amcheck={}, o2={:?})",
        m.amcheck_ii.len(),
        m.o2_fail
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
    assert!(
        clean.pass,
        "D16 clean: the round must pass (o1={:?}, te={:?})",
        clean.o1, clean.task_error
    );
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

/// The R4 positional-id pin (the gate's round-3 F5): two **empty** map
/// leaves — byte-identical except for the positional header id — are the
/// only plant shape where §3.3's check (3) alone discriminates. The
/// workload builds four trees so the radix holds two fully-unmapped
/// middle leaves (trees B and C dropped, their LPID ranges freed), then
/// a MISDIRECTED fault plants leaf 2's empty bytes on leaf 1's
/// referenced slot under REFCRC-SKIP: checks (1) and (4) pass (a valid
/// epoch-0 page), (2) is the mutant's off site, and the planted entries
/// parse to exactly the same all-`None` leaf — identical semantics, so
/// no structural check (LPID-exactness, free-set exactness, amcheck,
/// the harness walks) can see the swap. Only the **positional id**
/// (expected base 8, header 16) names the plant.
///
/// Clean (positional walkers): XX001 at open before any accept — an MP
/// detection that passes, never served. Verified mutant-style against
/// the gate's probe (positional → `header_id`), by hand: the
/// **all-three** revert (store walkers, amcheck, harness walks) and the
/// **store-walkers-only** revert both make this pin fail exactly as the
/// design says — the check is vacuous, `open` accepts leaf 1's slot
/// through the fault's tainted landing range, `served=true` and O-3 (a)
/// fails the round. The **harness-walks-only** and **amcheck-only**
/// reverts stay green, subsumed by construction: the harness walks
/// check with `mutant = None`, and a plant whose reference CRC passes
/// the harness's check (2) is byte-for-byte the referenced page, so its
/// positional id passes too — a (2)-passing/(3)-failing plant is
/// impossible there; and `open` walks the same radix with the same
/// check and rejects the page before amcheck ever runs. The store's
/// walkers — the one family that carries the round's mutant — are where
/// the positional id is load-bearing, and that is what this pin holds.
#[test]
fn r4_positional_id_discriminates_the_empty_leaf_plant() {
    let big = |tree: u64, i: u64| Op::Put {
        tree,
        key: i.to_be_bytes().to_vec(),
        value: vec![1u8; 400],
    };
    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }],
        },
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..20u64).map(|i| big(2, i)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 3 }],
        },
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..40u64).map(|i| big(3, i)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 4 }],
        },
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..40u64).map(|i| big(4, i)).collect(),
        },
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 5 }],
        },
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..20u64).map(|i| big(5, i)).collect(),
        },
        ScriptOp::Checkpoint,
        // The drop frees every LPID of trees 3 and 4: the middle radix
        // leaves become fully empty (all-`None` entries), byte-identical
        // except for their positional ids.
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::DropTree { tree: 3 }, Op::DropTree { tree: 4 }],
        },
        ScriptOp::Checkpoint,
        ScriptOp::Yields(3),
    ];
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: true,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::Hook {
            ckpt_index: 1,
            hook: CkptHook::After6,
        }),
        nested: Vec::new(),
    };
    let r4cfg = |mutant: Option<Mutant>| StoreCfg {
        map_leaf_cap: 8,
        freeset_leaf_bits: 128,
        pool_frames: 24,
        r_epoch: 2,
        mutant,
    };
    // The faultless base supplies the crashed disk the plant is built
    // from (the same schedule prefix, so the same slots).
    let base = {
        let (disk, st) = harness::create_store(r4cfg(None), 2077, 7).expect("create");
        harness::run_round(
            2077,
            0,
            &script,
            &faults(FaultClass::C, knobs(0, 0)),
            r4cfg(None),
            disk,
            st,
            &Model::new(),
            0,
            None,
        )
    };
    assert!(
        base.pass && base.o1.is_ok(),
        "R4 base: the faultless round must pass (o1={:?}, te={:?})",
        base.o1,
        base.task_error
    );
    let pf = base
        .crashed_disk
        .stat(e4_recovery::sim::PAGE_FILE)
        .expect("R4 page file");
    let (_f, _s, meta) = harness::durable_image(&base.crashed_disk, &r4cfg(None));
    let meta = meta.expect("R4: a valid meta");
    // Walk the persisted radix (the test is harness-side: durable bytes
    // only) and find two empty leaves with different bases.
    let inner = base
        .crashed_disk
        .durable_read(pf, meta.map_root.slot * 4096, 4096);
    let refs = e4_recovery::store::page::parse_radix_inner(&inner, 0, 254, &meta.map_root, None)
        .expect("R4: the map inner parses");
    let mut empties: Vec<(u64, u64)> = Vec::new(); // (base, slot)
    for (i, r) in refs.iter().enumerate() {
        if let Some(r) = r {
            let b = base.crashed_disk.durable_read(pf, r.slot * 4096, 4096);
            let entries = e4_recovery::store::page::parse_map_leaf(&b, (i as u64) * 8, 8, r, None)
                .expect("R4: the leaf parses");
            if entries.iter().all(|e| e.is_none()) {
                empties.push(((i as u64) * 8, r.slot));
            }
        }
    }
    assert!(
        empties.len() >= 2,
        "R4: the radix must hold two empty leaves for the plant (found {empties:?})"
    );
    let (base_x, slot_x) = empties[0];
    let (base_y, slot_y) = empties[1];
    assert!(
        base_y > base_x && slot_y != slot_x,
        "R4: the two empty leaves must differ (found {empties:?})"
    );
    // The plant: leaf Y's bytes onto leaf X's referenced slot.
    let y_bytes = base.crashed_disk.durable_read(pf, slot_y * 4096, 4096);
    let x_pre = base
        .crashed_disk
        .dwrites_of(pf)
        .iter()
        .rev()
        .find(|w| w.off == slot_x * 4096 && w.bytes.len() == 4096)
        .map(|w| w.pre.clone())
        .unwrap_or_default();
    let fault = e4_recovery::sim::MediaFault {
        inode: pf,
        off: slot_y * 4096,
        len: 4096,
        bytes: y_bytes,
        pre: x_pre,
        kind: e4_recovery::sim::MediaKind::Misdirected {
            to_off: slot_x * 4096,
        },
    };
    let run_r4 = |mutant: Option<Mutant>| {
        let (disk, st) = harness::create_store(r4cfg(mutant), 2077, 7).expect("create");
        let mut twin = Vec::new();
        harness::run_round(
            2077,
            0,
            &script,
            &RoundFaults {
                class: FaultClass::Mp,
                knobs: knobs(0, 0),
                directed: vec![fault.clone()],
                no_r_taint: false,
            },
            r4cfg(mutant),
            disk,
            st,
            &Model::new(),
            0,
            Some(&mut twin),
        )
    };
    let r = run_r4(Some(Mutant::RefCrcSkip));
    assert!(
        r.pass && !r.served && (r.detections.contains(&"open") || !r.amcheck_i.is_empty()),
        "R4 pin: the positional id must name the planted empty leaf under REFCRC-SKIP (pass={}, served={}, det={:?}, am_i={}, o1={:?}, o3={:?})",
        r.pass,
        r.served,
        r.detections,
        r.amcheck_i.len(),
        r.o1,
        r.o3_fail
    );
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
/// The 420-put load is a **chunked** transaction (§12.2's 16-KiB record
/// cap — the gate's F1/F2: the draft's single over-cap `Commit` made
/// the faultless base round itself fail while the Work 2.1 derivation
/// silently dropped the commit). The crash point sits past the script's
/// yield count, so the round is a **crash-resolution** round: the
/// workload completes, F-CRASH resolves (nothing volatile), the faults
/// apply, and recovery runs on the faulted durable image.
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
        // bytes without touching the header. **Chunked** (§12.2: 420 ×
        // 423 B ≈ 174 KiB is far over the 16-KiB record cap — the gate's
        // F1/F2: the draft's single Commit spread the transaction across
        // segments, and the faultless base round itself failed O-1(c)
        // while the derivation silently dropped the commit).
        ScriptOp::Chunked {
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
                no_r_taint: false,
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
    // Clean: XX001 at open, never served; the round passes (a legal
    // MP detection).
    let clean = run17(None, torn.clone());
    assert!(
        clean.pass,
        "D17 clean: the detection round must pass (det={:?}, o1={:?}, o3={:?})",
        clean.detections, clean.o1, clean.o3_fail
    );
    assert!(
        clean.detections.contains(&"open") || is_read_failed(&clean.o1),
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
            s.detections.contains(&"open") || is_read_failed(&s.o1),
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
        l.pass
            && (l.detections.contains(&"open") || is_read_failed(&l.o1) || !l.amcheck_i.is_empty())
            && !l.o3_fail.as_deref().unwrap_or("").contains("O-3(a)")
            && !l.served,
        "D17 LOST map leaf: detected, never served, passing (det={:?}, o3={:?})",
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
        d.pass
            && (d.detections.contains(&"open") || is_read_failed(&d.o1) || !d.amcheck_i.is_empty())
            && !d.o3_fail.as_deref().unwrap_or("").contains("O-3(a)")
            && !d.served,
        "D17 MISDIRECTED map leaf: detected, never served, passing (det={:?}, o3={:?})",
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
                no_r_taint: false,
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
        clean2.pass,
        "D17 REFCRC clean: the detection round must pass (det={:?}, o1={:?}, o3={:?})",
        clean2.detections, clean2.o1, clean2.o3_fail
    );
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

    // ---- the 9-cell grid (the gate's N8, on the F1/F2-fixed base):
    // LOST, TORN and MISDIRECTED, each on a referenced tree leaf, a
    // FreeSet page and the newest meta slot (the MapLeaf cells ran
    // above; the tree-page MISDIRECTED cell of the REFCRC shape also
    // ran). Measured on the chunked script (each cell re-run per arm):
    // - a **meta** LOST or TORN is a correct rollback (§2.2's two
    //   slots): open falls back to the older meta and the round passes;
    // - a **freeset** fault of any kind, and a **leaf** MISDIRECTED,
    //   are XX001 detections at open that are never served, and the
    //   round passes (a legal MP detection);
    // - a **leaf** LOST or TORN reaches the card's open X-5c ruling
    //   case (its Notes): recovery returns `Ok`, the extraction's scan
    //   hits the faulted page (XX001, `det=[]`), O-1 (b) reports "scan
    //   read failed" and O-3 (d) fails the round — whether O-1 is
    //   required when recovery returns `Ok` alongside an XX001 is
    //   X-5c's to rule, so **no verdict is asserted** on those two
    //   cells (only the ruling-independent halves: never served, no
    //   harness defect); the measured outcome is recorded in the stage
    //   RESULT item 8;
    // - a **meta** MISDIRECTED onto the twin slot is the gate's F3
    //   spec silence (§2.2 says nothing about the slot position vs
    //   `ckpt_id`): the plant is a valid copy of the newest meta read
    //   through the fault's tainted landing range, open takes it, and
    //   O-3 (a) fails the round — P must rule (a positional §2.2 meta
    //   check, or an oracle ruling); the cell runs for the record and
    //   asserts nothing verdict-shaped.
    // ----
    {
        let cell =
            |target: Cell| -> (e4_recovery::sim::MediaFault, e4_recovery::store::page::Meta) {
                // Build the fault from the crashed disk of a faultless base
                // round (the same schedule prefix, so the same bytes).
                let (disk, st) = harness::create_store(cfg254(None), 2016, 7).expect("create");
                let b = harness::run_round(
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
                );
                let pf = b
                    .crashed_disk
                    .stat(e4_recovery::sim::PAGE_FILE)
                    .expect("pf");
                let (_f, _s, meta) = harness::durable_image(&b.crashed_disk, &cfg254(None));
                let meta = meta.expect("meta");
                let (off, bytes, pre) = match target {
                    Cell::Leaf => {
                        // Any referenced tree leaf's durable write.
                        let w = b
                            .crashed_disk
                            .dwrites_of(pf)
                            .iter()
                            .rev()
                            .find(|w| {
                                w.off.is_multiple_of(4096)
                                    && w.bytes.len() == 4096
                                    && e4_recovery::store::page::header_kind(&w.bytes)
                                        == e4_recovery::store::page::KIND_LEAF
                                    && w.off > 3 * 4096
                            })
                            .cloned()
                            .expect("a tree leaf write");
                        (w.off, w.bytes, w.pre)
                    }
                    // The freeset root's and the newest meta's own writes:
                    // their whole content lives in sector 0 (the bitmap and
                    // the refs fit in the first 512 B at this scale), so the
                    // tear reverts sector 0.
                    Cell::Freeset | Cell::Meta => {
                        let at = if let Cell::Meta = target {
                            (1 + (meta.ckpt_id % 2)) * 4096
                        } else {
                            meta.free_root.slot * 4096
                        };
                        let w = b
                            .crashed_disk
                            .dwrites_of(pf)
                            .iter()
                            .rev()
                            .find(|w| w.off == at && w.bytes.len() == 4096)
                            .cloned()
                            .unwrap_or_else(|| panic!("a write at {at}"));
                        let bytes = b.crashed_disk.durable_read(pf, at, 4096);
                        let pre = if w.pre.is_empty() {
                            bytes.clone()
                        } else {
                            w.pre
                        };
                        (at, bytes, pre)
                    }
                };
                // A sector the write actually changed (a tear onto unchanged
                // bytes is a no-op): leaves differ in their cell sectors at
                // the page end, the freeset root and the meta in sector 0.
                let mut sector = off;
                for s in 0..8u64 {
                    let (a, b) = (
                        bytes.get((s * 512) as usize..(s * 512 + 512) as usize),
                        pre.get((s * 512) as usize..(s * 512 + 512) as usize),
                    );
                    if a.is_some_and(|a| b.is_some_and(|b| a != b)) {
                        sector = off + s * 512;
                        break;
                    }
                }
                (
                    e4_recovery::sim::MediaFault {
                        inode: pf,
                        off,
                        len: 4096,
                        bytes: bytes.clone(),
                        pre,
                        kind: e4_recovery::sim::MediaKind::Torn {
                            sectors: vec![sector],
                        },
                    },
                    meta,
                )
            };
        for target in [Cell::Leaf, Cell::Freeset, Cell::Meta] {
            for kind in [Kind::Lost, Kind::Torn, Kind::Misdirected] {
                let (mut fault, base_meta) = cell(target);
                fault.kind = match (target, kind) {
                    // F3's G7 catching shape (P's ruling, 2026-10-26):
                    // the plant lands on the **twin** meta slot — a
                    // valid copy of the newest meta, misplaced there by
                    // the fault, destroying the synced older meta the
                    // twin held.
                    (Cell::Meta, Kind::Misdirected) => {
                        assert!(
                            base_meta.ckpt_id >= 2,
                            "D17 meta Misdirected: the base's newest meta must be ≥ 2 so the twin holds a synced older meta (k={})",
                            base_meta.ckpt_id
                        );
                        e4_recovery::sim::MediaKind::Misdirected {
                            to_off: (1 + ((base_meta.ckpt_id + 1) % 2)) * 4096,
                        }
                    }
                    (Cell::Meta, Kind::Torn) => e4_recovery::sim::MediaKind::Torn {
                        sectors: vec![fault.off],
                    },
                    (_, Kind::Lost) => e4_recovery::sim::MediaKind::Lost,
                    (_, Kind::Torn) => e4_recovery::sim::MediaKind::Torn {
                        sectors: vec![fault.off],
                    },
                    (_, Kind::Misdirected) => e4_recovery::sim::MediaKind::Misdirected {
                        to_off: fault.off + 4096,
                    },
                };
                match (target, kind) {
                    // The two R6 cells (P's F4 ruling; X-5c's
                    // pre-registered X5-R6): recovery returned `Ok`, the
                    // extraction's scan hit the faulted page (an XX001
                    // that surfaced through O-1's own reads — hence the
                    // read-failed message), and the frozen oracle's
                    // strict reading fails the round through O-3 (d).
                    // Whether that reading is right is X-5c's to rule:
                    // assert neither `pass` nor `!pass`, only the
                    // reading-independent facts.
                    (Cell::Leaf, Kind::Lost) | (Cell::Leaf, Kind::Torn) => {
                        let r = run17(None, fault);
                        assert!(
                            r.end_store.is_some(),
                            "D17 leaf {kind:?}: the R6 case needs a recovered-Ok round (o1={:?})",
                            r.o1
                        );
                        assert!(
                            is_read_failed(&r.o1),
                            "D17 leaf {kind:?}: the o1 error must be a read-failed form — anything else is silent damage (o1={:?})",
                            r.o1
                        );
                        assert!(
                            !r.served && r.task_error.is_none(),
                            "D17 leaf {kind:?}: never served, no harness defect (served={}, te={:?})",
                            r.served,
                            r.task_error
                        );
                        println!(
                            "D17 [R6] leaf {kind:?}: o1={:?} o3={:?} pass={} (X-5c's X5-R6)",
                            r.o1, r.o3_fail, r.pass
                        );
                    }
                    // G7's catching test (the F3 ruling): clean — the
                    // misplaced newer meta is XX001 at open, never
                    // served, never fallen back past; mutant
                    // META-WRONG-SLOT (the positional test off at the
                    // store's open only) — the misplaced copy is opened
                    // from the tainted landing slot and served: O-3 (a)
                    // fails the round.
                    (Cell::Meta, Kind::Misdirected) => {
                        let r = run17(None, fault.clone());
                        assert!(
                            r.pass
                                && r.detections.contains(&"open")
                                && !r.served
                                && r.task_error.is_none(),
                            "D17 meta Misdirected clean (G7): the misplaced newer meta must be XX001 at open, never served, the round passing (pass={}, det={:?}, served={}, o3={:?})",
                            r.pass,
                            r.detections,
                            r.served,
                            r.o3_fail
                        );
                        let m = run17(Some(Mutant::MetaWrongSlot), fault);
                        assert!(
                            !m.pass
                                && m.served
                                && m.o3_fail.as_deref().unwrap_or("").starts_with("O-3(a)"),
                            "D17 META-WRONG-SLOT: the misplaced copy must be opened from the tainted landing slot and served (pass={}, served={}, o3={:?})",
                            m.pass,
                            m.served,
                            m.o3_fail
                        );
                    }
                    // A correct rollback: the older meta opens, nothing
                    // is served, the round passes (F2: with the chunked
                    // script the base is clean, so these cells mean what
                    // they say).
                    (Cell::Meta, _) => {
                        let r = run17(None, fault);
                        assert!(
                            r.pass && !r.served && r.meta_rollback,
                            "D17 meta {kind:?}: the rollback round must pass (pass={}, served={}, rb={}, o1={:?}, o3={:?})",
                            r.pass,
                            r.served,
                            r.meta_rollback,
                            r.o1,
                            r.o3_fail
                        );
                    }
                    // Detections at open, never served: the round passes.
                    _ => {
                        let r = run17(None, fault);
                        assert!(
                            r.pass
                                && !r.served
                                && (r.detections.contains(&"open")
                                    || is_read_failed(&r.o1)
                                    || !r.amcheck_i.is_empty()),
                            "D17 {target:?} {kind:?}: detected, never served, passing (pass={}, served={}, det={:?}, am_i={}, o1={:?}, o3={:?})",
                            r.pass,
                            r.served,
                            r.detections,
                            r.amcheck_i.len(),
                            r.o1,
                            r.o3_fail
                        );
                    }
                }
            }
        }
    }
}

/// A D17 cell target (the gate's N8 coverage grid).
#[derive(Debug, Clone, Copy)]
enum Cell {
    /// A referenced tree leaf.
    Leaf,
    /// The free-set root page.
    Freeset,
    /// The newest meta's slot.
    Meta,
}

/// A D17 fault kind (the MapLeaf TORN arm ran above; the grid adds all
/// three kinds for every target).
#[derive(Debug, Clone, Copy)]
enum Kind {
    Lost,
    Torn,
    Misdirected,
}

/// D18 (ts order): two committers, and under the mutant a yield between
/// ts assignment and enqueue (I-WAL-TS-ORDER). Task A creates and
/// publishes the tree, then races its SyncOn commit against task B's
/// SyncOff commit; the crash lands at A's script yield right after the
/// racing commit, dropping volatile writes. Clean: the critical section
/// is atomic (ts and enqueue in one section), so `commit_ts` increases
/// with `lsn`, M-WAL-ORDER stays silent and the oracle passes. Mutant
/// TS-AFTER-APPEND: A's section ends after the ts assignment, B — parked
/// at the section wait — takes the next ts and enqueues at the lower
/// LSN, so A's lower-ts record lands above it; M-WAL-ORDER fires on the
/// issued records and the oracle fails besides (the inversion also
/// breaks the ack/replay agreement the extraction sees).
///
/// (The run-1 shape's clean arm was vacuous — `task_error="Crashed"`
/// from `fail_of` masking a real "tree 2 missing in directory": task B's
/// apply raced ahead of the create-commit's apply. The harness now (a)
/// records the original failure and (b) waits for the creating commit's
/// directory entry — applies finish out of ts order by design, §12.4.)
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
        ScriptOp::Yields(1),
        ScriptOp::Commit {
            sync_on: true,
            ops: (0..10).map(|i| put(i, 1)).collect(),
        },
        ScriptOp::Yields(1),
    ];
    let ops_b = vec![ScriptOp::Commit {
        sync_on: false,
        ops: (10..20).map(|i| put(i, 2)).collect(),
    }];
    let script = Script {
        tasks: vec![ScriptTask { ops: ops_a }, ScriptTask { ops: ops_b }],
        writer: true,
        evictor: false,
        checkpointer_every: Some(100),
        crash: Some(CrashSpec::TaskYield(1, 4)),
        nested: Vec::new(),
    };
    let clean = run(2023, &script, &faults(FaultClass::C, knobs(0, 0)), None);
    assert!(
        clean.pass,
        "D18 clean: the round must pass (o1={:?}, te={:?}, mon={:?})",
        clean.o1, clean.task_error, clean.monitors
    );
    assert!(
        !clean.monitors.contains(&"M-WAL-ORDER"),
        "D18 clean: M-WAL-ORDER fired"
    );
    let m = run(
        2023,
        &script,
        &faults(FaultClass::C, knobs(0, 0)),
        Some(Mutant::TsAfterAppend),
    );
    assert!(
        m.monitors.contains(&"M-WAL-ORDER"),
        "D18 mutant: M-WAL-ORDER must fire on the inversion (monitors={:?})",
        m.monitors
    );
    assert!(
        !m.pass && m.o1.is_err(),
        "D18 mutant: the oracle must fail too (o1={:?}, pass={})",
        m.o1,
        m.pass
    );
}
