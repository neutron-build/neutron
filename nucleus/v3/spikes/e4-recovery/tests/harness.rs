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
        no_r_taint: false,
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

/// X5-R5's condition 3 (the gate's N7 pin): a slot whose writes hit
/// M-NOINPLACE is never excused by the exception set, however much it
/// otherwise qualifies; without the hit it qualifies. Removing the
/// condition from `exception_set_r` (which routes through
/// `r_slot_qualifies`) fails this pin.
#[test]
fn r_condition3_excludes_noinplace_hits() {
    use std::collections::BTreeSet;
    let image: BTreeSet<u64> = [7u64].into_iter().collect();
    let tainted: BTreeSet<u64> = BTreeSet::new();
    let hits: BTreeSet<u64> = [7u64].into_iter().collect();
    assert!(
        !e4_recovery::harness::r_slot_qualifies(7, &image, &tainted, &hits),
        "a slot with an M-NOINPLACE hit must not qualify for R"
    );
    let no_hits: BTreeSet<u64> = BTreeSet::new();
    assert!(
        e4_recovery::harness::r_slot_qualifies(7, &image, &tainted, &no_hits),
        "the same slot without a hit qualifies"
    );
    let taint: BTreeSet<u64> = [7u64].into_iter().collect();
    assert!(
        !e4_recovery::harness::r_slot_qualifies(7, &image, &taint, &no_hits),
        "a tainted slot never qualifies"
    );
}

/// X5-R5 condition 3 **at its call site** (the gate's round-3 F5): the
/// exception-set computation `exception_set_r` must actually consult the
/// M-NOINPLACE hit slots it is handed — the gate's probe (an empty hit
/// set at the call site, equivalent to removing condition 3 there) left
/// the whole suite green. A crafted durable-write history pins the
/// wiring: a slot that otherwise fully qualifies (referenced by the
/// opened image, untainted, written after the newer meta's graduation)
/// is excluded from `R` exactly because its write drew an M-NOINPLACE
/// hit — and qualifies the moment the hit set is empty.
#[test]
fn exception_set_r_consults_its_noinplace_hits() {
    use e4_recovery::sim::SimDisk;
    use e4_recovery::store::page::{self, Meta};
    use std::collections::BTreeSet;
    let mut disk = SimDisk::new();
    let pf = disk.create("/pages").expect("create");
    disk.sync_dir(e4_recovery::sim::ROOT_DIR).expect("dirsync");
    // The newer meta (ckpt_id 2 > opened 1), graduated by a sync_all:
    // the graduation point `g` of X5-R5's conditions.
    let m2 = page::build_meta(&Meta {
        ckpt_id: 2,
        ..Meta::default()
    });
    disk.write_at(pf, 4096, &m2).expect("meta write");
    disk.sync_all(pf).expect("meta sync");
    // A later graduated write to slot 7: after `g`, referenced by the
    // opened image, untainted — it qualifies for `R` on every condition
    // except a monitor hit.
    let w7 = page::build_leaf(7, 3, &[]);
    disk.write_at(pf, 7 * 4096, &w7).expect("slot 7 write");
    disk.sync_all(pf).expect("slot 7 sync");
    let image: BTreeSet<u64> = [7u64].into_iter().collect();
    let tainted: BTreeSet<u64> = BTreeSet::new();
    let hits: BTreeSet<u64> = [7u64].into_iter().collect();
    let r = e4_recovery::harness::exception_set_r(&disk, pf, 1, &image, &tainted, &hits);
    assert!(
        !r.contains(&7),
        "condition 3 at the call site: a slot with an M-NOINPLACE hit must not enter R (R={r:?})"
    );
    let no_hits: BTreeSet<u64> = BTreeSet::new();
    let r2 = e4_recovery::harness::exception_set_r(&disk, pf, 1, &image, &tainted, &no_hits);
    assert_eq!(
        r2,
        BTreeSet::from([7u64]),
        "the same slot without a hit qualifies (R={r2:?})"
    );
}

/// The pass fold (the gate's N7 pin): every per-seed failure flag folds
/// `pass` to false on its own — a verdict that passed the oracle is
/// still a failed round under any flag.
#[test]
fn pass_fold_flags_are_load_bearing() {
    for (name, te, dl, pan, bat) in [
        ("task_error", Some("x"), false, false, false),
        ("deadlock", None, true, false, false),
        ("panicked", None, false, true, false),
        ("baton", None, false, false, true),
    ] {
        assert!(
            !e4_recovery::harness::fold_pass(true, te, dl, pan, bat),
            "the {name} flag alone must fail the round"
        );
    }
    assert!(e4_recovery::harness::fold_pass(
        true, None, false, false, false
    ));
}

/// §12.2's record cap (the gate's round-3 F1): `wal_enqueue` refuses a
/// payload whose record exceeds `wal_seg_size / 4` — loudly, before any
/// placement. The draft had no cap, and a 177-KiB `Commit` was spread
/// across 64-KiB segments where §12.5's reader could never parse it; an
/// acked commit then vanished from both the recovered state and the
/// model, silently. Reverting the refusal (placing over-cap records
/// again) fails this pin.
#[test]
fn wal_record_cap_is_refused() {
    use e4_recovery::sim::HARNESS;
    use e4_recovery::store::{Cx, Fail, HasStore, StoreInner, wal};
    let cfgc = cfg(None);
    let world = harness::round_world(
        3210,
        e4_recovery::sim::SimDisk::new(),
        StoreInner::empty(cfgc.clone(), 0),
    );
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    // At the cap: a record of exactly `REC_MAX` places (the boundary is
    // inclusive).
    let at_cap = (wal::REC_MAX - wal::REC_FIXED) as usize;
    let r = wal::wal_enqueue(&cx, wal::K_COMMIT, (1, 1), 1, &vec![0u8; at_cap]);
    assert!(r.is_ok(), "a record at the cap must place ({r:?})");
    // One byte over: refused with `Fail::Invalid` (the loud path), and
    // nothing was appended.
    let over = at_cap + 1;
    let r2 = wal::wal_enqueue(&cx, wal::K_COMMIT, (1, 2), 2, &vec![0u8; over]);
    assert!(
        matches!(r2, Err(Fail::Invalid(ref m)) if m.contains("cap")),
        "an over-cap payload must be refused loudly ({r2:?})"
    );
    let appended = cx
        .with("cap:chk", |c| Ok(c.store().wal.append))
        .expect("check");
    let (lsn, len) = r.expect("at-cap placement");
    assert_eq!(
        appended,
        lsn + len,
        "the refused enqueue must not have appended anything"
    );
}

/// The Work 2.1 derivation's F1 half: a commit whose record header is
/// present in a WAL file's issued writes at its segment-relative
/// position, with its own lsn and a length past the segment end, is
/// **written but unparsable** — reported in the third return so the
/// harness fails the round, never a silent model drop (the round-3
/// demonstration's exact shape: an over-cap record placed at a segment
/// boundary). A parseable record and a never-written one stay silent.
#[test]
fn derivation_reports_written_unparsable_commits() {
    use e4_recovery::sim::DiskEvent;
    use e4_recovery::store::wal;
    use std::collections::BTreeMap;
    let inode = 4usize; // any non-page-file id
    // An over-cap Commit encoded at lsn 0 (a segment boundary): its
    // header parses, its length runs past the segment end — exactly the
    // F1 shape (§12.5's reader stops; §12.2 would never place it).
    let over_payload = vec![7u8; 70_000];
    let rec = wal::enc_record(wal::K_COMMIT, (1, 1), 5, &over_payload, 0);
    let events = vec![
        DiskEvent::WriteIssued {
            inode,
            off: 0,
            bytes: rec.clone(),
        },
        DiskEvent::WriteReturned {
            inode,
            off: 0,
            len: rec.len() as u64,
        },
        DiskEvent::SyncIssued { inode, all: false },
        DiskEvent::SyncReturned { inode, all: false },
    ];
    let rec_len = wal::REC_FIXED + over_payload.len() as u64;
    let mut commits: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    commits.insert(5u64, (true, 0u64, rec_len));
    let (written, _acked, unparsed) = harness::derive_written_acked(&events, &commits);
    assert!(
        written.is_empty(),
        "an unparsable record never marks the commit written"
    );
    assert_eq!(
        unparsed,
        vec![5u64],
        "the written-but-unparsable commit must be reported (F1)"
    );
    // A parseable record of the same commit is not reported: rebuild
    // the events with a legal record at lsn 0.
    let ok_payload = vec![7u8; 64];
    let ok_rec = wal::enc_record(wal::K_COMMIT, (1, 1), 5, &ok_payload, 0);
    let ok_events = vec![
        DiskEvent::WriteIssued {
            inode,
            off: 0,
            bytes: ok_rec.clone(),
        },
        DiskEvent::WriteReturned {
            inode,
            off: 0,
            len: ok_rec.len() as u64,
        },
        DiskEvent::SyncIssued { inode, all: false },
        DiskEvent::SyncReturned { inode, all: false },
    ];
    let ok_len = wal::REC_FIXED + ok_payload.len() as u64;
    let mut ok_commits: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    ok_commits.insert(5u64, (true, 0u64, ok_len));
    let (w2, a2, u2) = harness::derive_written_acked(&ok_events, &ok_commits);
    assert_eq!(
        u2,
        Vec::<u64>::new(),
        "a parseable record is never reported"
    );
    assert_eq!(w2, vec![5u64], "it marks the commit written");
    assert_eq!(a2, vec![5u64], "and acked (the sync covered it)");
    // A commit whose record was never written stays silent too.
    let mut none_commits: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    none_commits.insert(6u64, (true, 65536u64, 128u64));
    let (_, _, u3) = harness::derive_written_acked(&ok_events, &none_commits);
    assert_eq!(
        u3,
        Vec::<u64>::new(),
        "a never-written record is not a defect"
    );
}
