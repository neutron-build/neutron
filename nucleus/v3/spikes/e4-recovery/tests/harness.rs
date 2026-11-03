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
        media: e4_recovery::harness::MediaDraw::None,
        twin_supply: None,
        nested_draw: 0,
        probe_pass: false,
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
    let r =
        e4_recovery::harness::exception_set_r(&disk, pf, 1, &image, &tainted, &hits, &disk.events);
    assert!(
        !r.contains(&7),
        "condition 3 at the call site: a slot with an M-NOINPLACE hit must not enter R (R={r:?})"
    );
    let no_hits: BTreeSet<u64> = BTreeSet::new();
    let r2 = e4_recovery::harness::exception_set_r(
        &disk,
        pf,
        1,
        &image,
        &tainted,
        &no_hits,
        &disk.events,
    );
    assert_eq!(
        r2,
        BTreeSet::from([7u64]),
        "the same slot without a hit qualifies (R={r2:?})"
    );
}

/// **X-5c-F2 step 1 (d) pin (gate mutant M8): the events half's anchor
/// is the newer meta's `sync_all` RETURN, not its issue.** A literal
/// event stream — the newer meta issued, a page write to slot `s`, the
/// `sync_all` returning, then a page write to slot `t` — must leave
/// `s ∉ R` and `t ∈ R` (both referenced and untainted). Anchoring at
/// the meta's issue (M8) wrongly excuses `s`'s write, which the
/// graduation had not yet covered: its bytes were the opened image's
/// own content when they were issued.
#[test]
fn f1f2d_r_anchor_is_the_sync_not_the_issue() {
    use e4_recovery::sim::DiskEvent;
    use e4_recovery::sim::SimDisk;
    use e4_recovery::store::page::{self, Meta};
    use std::collections::BTreeSet;
    let mut disk = SimDisk::new();
    let pf = disk.create("/pages").expect("create");
    disk.sync_dir(e4_recovery::sim::ROOT_DIR).expect("dirsync");
    // The newer meta (ckpt_id 2 > opened 1), graduated by a sync_all —
    // the dwrites half's graduation point `g`.
    let m2 = page::build_meta(&Meta {
        ckpt_id: 2,
        ..Meta::default()
    });
    disk.write_at(pf, 4096, &m2).expect("meta write");
    disk.sync_all(pf).expect("meta sync");
    // The literal stream: meta issue, slot-7 write, the sync's return,
    // slot-9 write. Slot 7's write precedes the anchor; slot 9's
    // follows it.
    let w7 = page::build_leaf(7, 1, &[]);
    let w9 = page::build_leaf(9, 1, &[]);
    let events = vec![
        DiskEvent::WriteIssued {
            inode: pf,
            off: 4096,
            bytes: m2.clone(),
        },
        DiskEvent::WriteIssued {
            inode: pf,
            off: 7 * 4096,
            bytes: w7,
        },
        DiskEvent::SyncReturned {
            inode: pf,
            all: true,
        },
        DiskEvent::WriteIssued {
            inode: pf,
            off: 9 * 4096,
            bytes: w9,
        },
    ];
    let image: BTreeSet<u64> = [7u64, 9].into_iter().collect();
    let tainted: BTreeSet<u64> = BTreeSet::new();
    let hits: BTreeSet<u64> = BTreeSet::new();
    let r = e4_recovery::harness::exception_set_r(&disk, pf, 1, &image, &tainted, &hits, &events);
    assert!(
        !r.contains(&7),
        "a write issued before the newer meta's sync_all returned must not enter R (R={r:?})"
    );
    assert_eq!(
        r,
        BTreeSet::from([9u64]),
        "a write issued after the sync_all returned enters R (R={r:?})"
    );
}

/// **X-5c-F2 step 1 (e) pin: a literal stream holding a page-file
/// `SyncReturned { all: false }` yields the anchor-0 precondition
/// defect** — "page file synced by sync_data: X5-R5 anchor
/// precondition" — and a stream without it does not. The store only
/// ever `sync_all`s the page file, so on a real campaign this is a
/// tripwire; removing the check (the assert's revert: the function
/// answering `None` unconditionally) fails the pin.
#[test]
fn f1f2e_page_file_sync_data_yields_the_anchor0_defect() {
    use e4_recovery::sim::DiskEvent;
    let pf = 0usize;
    let with = vec![
        DiskEvent::WriteIssued {
            inode: pf,
            off: 4096,
            bytes: vec![1u8; 4096],
        },
        DiskEvent::SyncReturned {
            inode: pf,
            all: false,
        },
    ];
    assert_eq!(
        e4_recovery::harness::r5_anchor0_defect(&with, pf),
        Some("page file synced by sync_data: X5-R5 anchor precondition"),
        "a page-file sync_data return breaks the anchor-0 precondition"
    );
    let without = vec![
        DiskEvent::WriteIssued {
            inode: pf,
            off: 4096,
            bytes: vec![1u8; 4096],
        },
        DiskEvent::SyncReturned {
            inode: pf,
            all: true,
        },
        DiskEvent::SyncReturned {
            inode: pf + 1,
            all: false,
        },
    ];
    assert_eq!(
        e4_recovery::harness::r5_anchor0_defect(&without, pf),
        None,
        "a sync_all on the page file, or a sync_data on another file, is not the defect"
    );
}

/// **X5-R28 (a) unit pin: `V_k` is the history at the opened meta's
/// write, not the latest DWrite.** Literal dwrites: a page write `w1`
/// to slot 7, the opened meta k=1's write, a newer meta k=2's write,
/// then a page write `w2` to slot 7 which a LOST fault reverts — the
/// durable bytes are `w1` again while the last-issued bytes are `w2`
/// (the taint 1.4 sees). `V_k(7)` composes only the dwrites that
/// precede the opened meta's own write (`w1`), so the durable bytes
/// equal it and slot 7 is kept. The mutant "use the latest DWrite"
/// composes `w2` too, which is not the durable bytes, so the slot
/// falls out of K.
#[test]
fn r28_v_k_is_the_history_at_the_opened_meta() {
    use e4_recovery::sim::SimDisk;
    use e4_recovery::store::page::{self, Meta};
    use std::collections::BTreeSet;
    let mut disk = SimDisk::new();
    let pf = disk.create("/pages").expect("create");
    disk.sync_dir(e4_recovery::sim::ROOT_DIR).expect("dirsync");
    let m1 = page::build_meta(&Meta {
        ckpt_id: 1,
        ..Meta::default()
    });
    let m2 = page::build_meta(&Meta {
        ckpt_id: 2,
        ..Meta::default()
    });
    let w1 = page::build_leaf(7, 1, &[(b"a", b"one")]);
    let w2 = page::build_leaf(7, 2, &[(b"a", b"two")]);
    assert_ne!(w1, w2, "the two slot-7 pages must differ");
    // dw[0]: w1. dw[1]: the opened meta (k=1, slot 2). dw[2]: the
    // newer meta (k=2, slot 1) — the anchor. dw[3]: w2, issued after
    // it. (A meta k lives at slot 1 + (k mod 2).)
    disk.write_at(pf, 7 * 4096, &w1).expect("w1");
    disk.sync_all(pf).expect("sync");
    disk.write_at(pf, 8192, &m1).expect("meta k=1");
    disk.sync_all(pf).expect("sync");
    disk.write_at(pf, 4096, &m2).expect("meta k=2");
    disk.sync_all(pf).expect("sync");
    disk.write_at(pf, 7 * 4096, &w2).expect("w2");
    disk.sync_all(pf).expect("sync");
    // A LOST fault reverts w2: durable = w1, last issued = w2 — tainted.
    disk.apply_one_media_fault(&e4_recovery::sim::MediaFault {
        inode: pf,
        off: 7 * 4096,
        len: 4096,
        bytes: w2.clone(),
        pre: w1.clone(),
        kind: e4_recovery::sim::MediaKind::Lost,
    });
    assert!(
        disk.range_tainted(pf, 7 * 4096, 4096),
        "slot 7 is tainted (durable w1 != issued w2)"
    );
    assert_eq!(
        disk.durable_read(pf, 7 * 4096, 4096),
        w1,
        "the durable bytes are the reverted w1"
    );
    let image: BTreeSet<u64> = [7u64].into_iter().collect();
    let tainted: BTreeSet<u64> = [7u64].into_iter().collect();
    let hits: BTreeSet<u64> = BTreeSet::new();
    let k = e4_recovery::harness::kept_set_k(
        &disk,
        pf,
        1,
        &image,
        &tainted,
        &hits,
        &disk.events.clone(),
        true,
    );
    assert_eq!(
        k,
        BTreeSet::from([7u64]),
        "V_k composes the history at the opened meta's write (w1 = the durable bytes), so slot 7 is kept (K={k:?})"
    );
}

/// **X5-R28 (c) pin: the durable-image walk reports no ground-truth
/// meta-slot finding.** A literal disk with a tainted meta slot (slot
/// 1's durable bytes differ from its last-issued write) and a media
/// fault applied: `durable_image` emits **no slotted finding on slots
/// 1 or 2** — the rollback-fallout loop that pushed one (derived from
/// `range_tainted` ground truth, standing in for amcheck the way
/// X5-R23 banned) is deleted. Restoring the loop fails this pin.
#[test]
fn r28_no_ground_truth_meta_finding() {
    use e4_recovery::sim::SimDisk;
    use e4_recovery::store::page::{self, Meta};
    let mut disk = SimDisk::new();
    let pf = disk.create("/pages").expect("create");
    disk.sync_dir(e4_recovery::sim::ROOT_DIR).expect("dirsync");
    // A valid opened image: metas k=1 (slot 2) and k=2 (slot 1), the
    // newer winning. (A meta k lives at slot 1 + (k mod 2).)
    let m1 = page::build_meta(&Meta {
        ckpt_id: 1,
        ..Meta::default()
    });
    let m2 = page::build_meta(&Meta {
        ckpt_id: 2,
        ..Meta::default()
    });
    let stray = page::build_meta(&Meta {
        ckpt_id: 3,
        ..Meta::default()
    });
    disk.write_at(pf, 8192, &m1).expect("meta k=1");
    disk.sync_all(pf).expect("sync");
    disk.write_at(pf, 4096, &m2).expect("meta k=2");
    disk.sync_all(pf).expect("sync");
    // A media fault exists on the disk (a harmless LOST on a data
    // slot), so the deleted loop's gate was armed here. (The fault and
    // its write come BEFORE the stray issue below: a later sync would
    // graduate the stray into the durable bytes and untaint the slot.)
    let w5 = page::build_leaf(5, 1, &[]);
    disk.write_at(pf, 5 * 4096, &w5).expect("slot 5 write");
    disk.sync_all(pf).expect("sync");
    disk.apply_one_media_fault(&e4_recovery::sim::MediaFault {
        inode: pf,
        off: 5 * 4096,
        len: 4096,
        bytes: w5.clone(),
        pre: vec![0u8; 4096],
        kind: e4_recovery::sim::MediaKind::Lost,
    });
    // Slot 2 tainted: a k=3 meta issued at slot 2 but never synced —
    // the durable bytes stay k=1 while the issued bytes are k=3.
    disk.write_at(pf, 8192, &stray).expect("stray meta issue");
    assert!(
        disk.range_tainted(pf, 8192, 4096),
        "slot 2 is tainted (a crash-dropped volatile meta write)"
    );
    let cfg = e4_recovery::workload::Knobs::derive(0, 0).store_cfg(None);
    let (findings, used, meta) = e4_recovery::harness::durable_image(&disk, &cfg);
    assert!(meta.is_some(), "a valid meta must open (k=2)");
    assert!(
        used.contains(&1) && used.contains(&2),
        "slots 1 and 2 stay in the referenced set (O-3 (c)/R's guard read it)"
    );
    for f in &findings {
        assert!(
            !matches!(f.slot, Some(1) | Some(2)),
            "no slotted finding on a meta slot may be derived from ground truth: {f:?}"
        );
    }
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
    // X5-R34: the derivation takes `p0` and the durable `/wal/`
    // bindings (here: segment 0 durably bound to the inode, no dir ops
    // this round — the binding predates it, so `β` does not exist).
    let mut bindings: BTreeMap<String, usize> = BTreeMap::new();
    bindings.insert(harness::x5r34_seg_name(0), inode);
    let (written, _acked, unparsed) =
        harness::derive_written_acked(&events, &commits, 0, &bindings);
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
    let (w2, a2, u2) = harness::derive_written_acked(&ok_events, &ok_commits, 0, &bindings);
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
    let (_, _, u3) = harness::derive_written_acked(&ok_events, &none_commits, 0, &bindings);
    assert_eq!(
        u3,
        Vec::<u64>::new(),
        "a never-written record is not a defect"
    );
}

/// X5-R11 (a)+(b) (P ruling 2026-10-31, nv3-X-5c-esc-1): amcheck (i)
/// must apply §3.3 to **every page the opened map references**, and a
/// slotted finding must be recorded as an XX001 site the verdict can
/// see. The defect: `walk_map_disk` pre-inserts every mapped slot into
/// `used`, and the draft's directory→tree walk then `continue`d past
/// the **descent of inner pages** on their first visit (the `used`
/// dedup fired before the separators were parsed), so a tainted leaf
/// under an inner page — D17's slot 92, seeds 7–8's map-referenced
/// leaves — was never §3.3-checked, never named by amcheck (i), and
/// (before X5-R6) hid behind O-3 (d).
///
/// The literal shape: a tree with ≥ 2 radix levels (`map_leaf_cap` 8)
/// and a B+tree root that is an **inner** page; a directed TORN fault
/// on a *leaf* page under it (a sector the write changed); (a) the
/// `durable_image` walk of the faulted disk names the slot, and the
/// slot is a referenced (mapped), tainted one; (b) the full MP round
/// reports the detection site `amcheck_i` beside the read-site one and
/// the round passes (X5-R6's detected, explained loss).
#[test]
fn r11a_amcheck_reaches_a_torn_leaf_under_an_inner_page() {
    use e4_recovery::sim::{MediaFault, MediaKind, PAGE_FILE};
    use e4_recovery::store::page::{KIND_INNER, KIND_LEAF, header_kind};

    let ops = vec![
        ScriptOp::Commit {
            sync_on: true,
            ops: vec![Op::CreateTree { tree: 2 }, Op::CreateTree { tree: 0 }],
        },
        // Chunked (§12.2/§12.7): 600 puts exceed the 16-KiB record cap.
        ScriptOp::Chunked {
            sync_on: true,
            ops: (0..600u64).map(|i| op_put(i, 7)).collect(),
        },
        ScriptOp::Checkpoint,
        ScriptOp::Yields(2),
    ];
    let script = Script {
        tasks: vec![ScriptTask { ops }],
        writer: true,
        evictor: false,
        checkpointer_every: Some(1000),
        crash: Some(CrashSpec::TaskYield(1, 4)),
        nested: Vec::new(),
    };
    // A faultless base round supplies the crashed disk the fault is
    // built from (the same schedule prefix, so the same bytes).
    let (disk0, st0) = harness::create_store(cfg(None), 2033, 7).expect("create");
    let base = harness::run_round(
        2033,
        0,
        &script,
        &round_faults(),
        cfg(None),
        disk0,
        st0,
        &Model::new(),
        0,
        None,
    );
    assert!(
        base.pass,
        "R11 base: the faultless round passes (o1={:?}, o2={:?}, o3={:?}, te={:?})",
        base.o1, base.o2_fail, base.o3_fail, base.task_error
    );
    let pf = base.crashed_disk.stat(PAGE_FILE).expect("R11: page file");
    // The torn target: a mapped tree-leaf write (a leaf of tree 2 —
    // every leaf in the image belongs to it at this scale).
    let (_findings, image_slots, _meta) = harness::durable_image(&base.crashed_disk, &cfg(None));
    let w = base
        .crashed_disk
        .dwrites_of(pf)
        .iter()
        .rev()
        .find(|w| {
            w.off.is_multiple_of(4096)
                && w.bytes.len() == 4096
                && header_kind(&w.bytes) == KIND_LEAF
                && w.off > 3 * 4096
                && image_slots.contains(&(w.off / 4096))
        })
        .cloned()
        .expect("R11: a mapped tree-leaf write exists");
    let slot = w.off / 4096;
    // A sector the write actually changed (a tear onto unchanged bytes
    // is a no-op).
    let mut sector = w.off;
    for s in 0..8u64 {
        let a = w.bytes.get((s * 512) as usize..(s * 512 + 512) as usize);
        let b = w.pre.get((s * 512) as usize..(s * 512 + 512) as usize);
        if a.is_some_and(|a| b.is_some_and(|b| a != b)) {
            sector = w.off + s * 512;
            break;
        }
    }
    let fault = MediaFault {
        inode: pf,
        off: w.off,
        len: 4096,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: MediaKind::Torn {
            sectors: vec![sector],
        },
    };
    // (a) the unit pin: the walk of the faulted disk names the slot.
    let mut faulted = base.crashed_disk.clone();
    faulted.apply_one_media_fault(&fault);
    let (findings, used, _) = harness::durable_image(&faulted, &cfg(None));
    assert!(
        used.contains(&slot),
        "R11 (a): the torn leaf's slot {slot} is a referenced (mapped) page"
    );
    assert!(
        faulted.range_tainted(pf, slot * 4096, 4096),
        "R11 (a): the torn leaf's slot is tainted (the completeness half's input)"
    );
    assert!(
        findings.iter().any(|f| f.slot == Some(slot)),
        "R11 (a): amcheck (i) must §3.3-check every mapped LPID — the torn leaf under the inner page must be named (findings={findings:?})"
    );
    // The shape, checked last so the earlier assertions name the defect
    // precisely: the image holds an INNER B+tree page (the root of
    // tree 2 — every mapped page belongs to tree 2, the catalog or the
    // metadata at this scale), so the torn leaf truly sat under an
    // inner page — the page the draft's dedup-skipped walk never
    // descended.
    let has_inner = base
        .crashed_disk
        .dwrites_of(pf)
        .iter()
        .filter(|w| w.off.is_multiple_of(4096) && w.bytes.len() == 4096)
        .any(|w| header_kind(&w.bytes) == KIND_INNER && image_slots.contains(&(w.off / 4096)));
    assert!(
        has_inner,
        "R11: the image must hold an inner B+tree page for the leaf-under-inner shape"
    );
}

/// X-5c's park-loop discipline (found root-causing seed 7 round 1
/// after X5-R11 (a) let round 0 pass): a script op that fails with a
/// store error — an MP round's legitimately damaged page (XX001 on a
/// bricked tree, the plan's clause 3 working as designed) — records
/// its task error, wakes the park keys **once** (Mesa), and exits; the
/// peers parked in the `Applied`/`MetaDurable` waits re-check their
/// own still-false condition and parked again forever: the scheduler's
/// DEADLOCK, with the round's true cause hidden behind it. The fix
/// extends X-5b's N3 discipline (fail loudly, never spin into the
/// deadlock detector) to every harness park loop. Unit pin: with the
/// round already failed, `mark_applied`, `WaitVisible`, the commit
/// critical section and the forced-checkpoint wait all return loudly
/// instead of parking. The end-to-end catcher is the smoke's
/// no-DEADLOCK assertion over the 16 seeds (seed 7's round 1 is the
/// literal shape; it deadlocked before the fix — demonstrated by hand
/// and reverted).
#[test]
fn park_loops_exit_loudly_on_a_failed_round() {
    use e4_recovery::sim::HARNESS;
    use e4_recovery::store::Cx;

    // A world whose round has already failed (a task error, and
    // workload_done — exactly what the failing task sets), with a
    // sequencer whose visible_ts can never reach the waited ts.
    let (disk, st) = harness::create_store(cfg(None), 2036, 7).expect("create");
    let world = harness::round_world(2036, disk, st);
    {
        let mut g = world.lock();
        g.h.task_error = Some("Corrupt(\"integrity fail lpid 75 slot 87\")".to_owned());
        g.h.workload_done = true;
        g.h.tasks_left = 2;
        // A sequencer hole: the commit below draws ts 9 while visible_ts
        // stays 0 (the failed peer's ts 1..8 never arrive), so
        // mark_applied can only exit through the round-failed check.
        g.st.seq.next_ts = 9;
    }
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    // mark_applied's exit: visible_ts (0) can never reach ts 9.
    let r = harness::commit_txn(&cx, &[Op::CreateTree { tree: 9 }], true, false);
    // The commit path unwinds loudly somewhere inside (the CS wait, the
    // apply wait or mark_applied), never by parking; the recorded
    // error is the harness's loud unwind, not a hang.
    assert!(
        r.is_err(),
        "P1: a failed round's commit path must unwind loudly (got {r:?})"
    );
    let te = world.lock().h.task_error.clone();
    assert!(
        te.as_deref().is_some_and(|t| t.contains("integrity fail")),
        "P1: the original store error stays the recorded one (te={te:?})"
    );
    // WaitVisible's and the forced-checkpoint wait's exits, directly
    // through the ops.
    let vis = harness::script_op(&cx, &ScriptOp::WaitVisible(1 << 40));
    assert!(
        vis.is_err(),
        "P1: WaitVisible must unwind loudly, never park (got {vis:?})"
    );
    let ck = harness::script_op(&cx, &ScriptOp::Checkpoint);
    assert!(
        ck.is_err(),
        "P1: the forced-checkpoint wait must unwind loudly (got {ck:?})"
    );
}

/// X5-R13 (a): campaign stderr carries only the progress lines, real
/// errors and the watchdog's abort line — the `TEMP-PROBE (reverted)`
/// `eprintln!("SCRIPT-OP FAIL …")` (P's ruling: about 0.8 MB per seed,
/// about 8 GB per 10k pass, all of it from that one line) is gone. The
/// pin reads the harness source and fails if the probe (or any
/// SCRIPT-OP stderr print) comes back: the flood was stderr-side, so
/// the source is the deterministic observable.
#[test]
fn r13a_no_script_op_probe_on_stderr() {
    let src = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/harness.rs"))
        .expect("read src/harness.rs");
    assert!(
        !src.contains("SCRIPT-OP FAIL"),
        "X5-R13 (a): the TEMP-PROBE stderr line is back in src/harness.rs"
    );
    assert!(
        !src.contains("TEMP-PROBE"),
        "X5-R13 (a): a TEMP-PROBE marker is back in src/harness.rs"
    );
}

/// X5-R13 (b), the ruled directed pin — a literal config, not a seed
/// number: a task that only yields and never parks (1,050,000 bounded
/// `Yields`, comfortably past `YIELD_BUDGET` = 1,000,000 with no service
/// tasks adding noise). The world crosses the budget mid-round; the
/// scheduler fails it exactly as the deadlock path does, and the round
/// records `livelock` (with the tripping site, the yielding task's own
/// `h:yields`) and `deadlock`, never a pass. With the budget check
/// removed (verified mutant-style, then reverted), the task completes
/// all its yields, the round ends normally, `livelock` stays false and
/// this test fails.
#[test]
fn r13b_livelock_budget_trips_and_folds_into_deadlock() {
    let script = Script {
        tasks: vec![ScriptTask {
            ops: vec![ScriptOp::Yields(1_050_000)],
        }],
        writer: false,
        evictor: false,
        checkpointer_every: None,
        crash: None,
        nested: Vec::new(),
    };
    let (disk, st) = harness::create_store(cfg(None), 2099, 7).expect("create");
    let out = harness::run_round(
        2099,
        0,
        &script,
        &round_faults(),
        cfg(None),
        disk,
        st,
        &Model::new(),
        0,
        None,
    );
    assert!(out.livelock, "the round must record the livelock");
    assert!(
        out.deadlock,
        "the livelock folds into the round's deadlock flag"
    );
    assert_eq!(
        out.livelock_site, "h:yields",
        "the tripping site is the yielding task's own yield point"
    );
    assert!(!out.pass, "a livelock is never a pass");
    assert!(
        out.sched_yields > e4_recovery::sim::YIELD_BUDGET,
        "the trip happens past the budget (yields={})",
        out.sched_yields
    );
}

/// **X-5c-F3 pin, class C-XX's G3 family (a literal config: the fixture
/// below, one nested crash point swept over the recovery's yield
/// range).** A crash delivered at any single yield point inside
/// recovery must surface as a nested recovery crash — F-CRASH resolves
/// and the retry replays — never as corruption: open's G3 check read
/// `env().open`'s error as "segment missing", and a delivered crash at
/// that yield arrives as the same `Err(SimFail::Crashed)` as a missing
/// file, so the nested crash was swallowed into `Corrupt("start
/// segment … missing (G3)")` — an XX001 at Open in a faultless class C
/// round (seed 1769's family: the twin failed O-1 (a) via the recovery
/// `Err`, the media round failed O-3 (c) beside faults the twin never
/// covered), with `nested` recorded as 0. The sweep crashes the
/// recovery at every point `k` in `1..=120`; every round must pass.
/// Reverting the fix (mutant M-CXXb: the G3 probe reading `open`'s
/// error again) fails this pin at the `k` that lands on the probe's
/// yield, with exactly `O-3(b): XX001 in class C at [Open]`.
#[test]
fn cxx_a_crash_at_any_recovery_yield_is_never_corruption() {
    // The fixture: a checkpoint (T=2), then 18 single-op commits above
    // T in the WAL, and the crash at task 1's 16th yield — inside the
    // post-checkpoint commits (calibrated: earlier points crash before
    // the checkpoint, so `wal_start_lsn` is 0 and open's G3 probe never
    // runs; later points never fire).
    for k in 1u64..=120 {
        let mut ops = vec![
            ScriptOp::Commit {
                sync_on: true,
                ops: vec![Op::CreateTree { tree: 2 }],
            },
            ScriptOp::Commit {
                sync_on: true,
                ops: (0..40).map(|i| op_put(i, 1)).collect(),
            },
            ScriptOp::Checkpoint,
        ];
        for i in 40..60 {
            ops.push(ScriptOp::Commit {
                sync_on: true,
                ops: vec![op_put(i, 1)],
            });
        }
        let script = Script {
            tasks: vec![ScriptTask { ops }],
            writer: true,
            evictor: true,
            checkpointer_every: Some(100),
            crash: Some(CrashSpec::TaskYield(1, 16)),
            nested: vec![k],
        };
        let (disk, st) = harness::create_store(cfg(None), 5000 + k, 77).expect("create");
        let out = harness::run_round(
            5000 + k,
            0,
            &script,
            &round_faults(),
            cfg(None),
            disk,
            st,
            &Model::new(),
            0,
            None,
        );
        assert!(
            out.obs.crash_fired,
            "k={k}: the round's own crash never fired (calibrate the TaskYield point)"
        );
        assert!(
            out.pass,
            "k={k}: a crash at this recovery yield became a failure (not a nested retry): o1={:?} o2={:?} o3={:?}",
            out.o1, out.o2_fail, out.o3_fail
        );
    }
}

/// **X-5c-F3-F1 step 6, X5-R30 (c)'s record fields — the seed pins.**
/// `skx` (rule 1: every serve inside slots of `K`, class MP, X5-R5's
/// conditions 1+2, no workload-pass serve, no R-taint, no uncovered
/// accept) and `smx` (rule 2: every serve exactly the opened meta's
/// own slot, its durable bytes a `sync_all`-covered DWrite) are
/// diagnostics in the record, never oracle inputs; judging them is P's
/// (a served round passes iff `(skx ∨ smx) ∧ pass_na`, from the final
/// file with no re-run). At this card's tip the MP-A seeds carry:
/// `skx` for 537 r0 and 1887 r0; `smx` for 2927 r0, 7528 r0 and
/// 8728 r0 — each with `pass_na = true`. **X-5c-F3-F3a step 3's shape
/// maintenance:** the store change moved the workloads wholesale; the
/// skx seeds 1897 r0 and 4718 r0 no longer carry the shape (they fail
/// only at this test's `assert!(r.served)` precondition), re-pointed
/// at 537 r0 and 1887 r0 with the assertions byte-identical; the smx
/// seeds and 6149 r1 still hold.
#[test]
fn r30_skx_and_smx_record_fields() {
    for (seed, want_skx, want_smx) in [(537u64, true, false), (1887, true, false)] {
        let rec = e4_recovery::workload::run_seed(seed, None);
        let r = &rec.rounds[0];
        assert!(r.served, "seed {seed} r0: the MP-A serve must hold");
        assert_eq!(
            r.skx, want_skx,
            "seed {seed} r0: skx (rule 1's predicate, exact)"
        );
        assert_eq!(
            r.smx, want_smx,
            "seed {seed} r0: smx (rule 2's predicate, exact)"
        );
        assert_eq!(
            r.pass_na,
            Some(true),
            "seed {seed} r0: the no-taint re-verdict"
        );
    }
    for seed in [2927u64, 7528, 8728] {
        let rec = e4_recovery::workload::run_seed(seed, None);
        let r = &rec.rounds[0];
        assert!(r.served, "seed {seed} r0: the meta-slot serve must hold");
        assert!(!r.skx, "seed {seed} r0: a meta-slot serve is not inside K");
        assert!(
            r.smx,
            "seed {seed} r0: smx (the opened meta slot's own serve, bytes a sync_all-covered DWrite)"
        );
        assert_eq!(
            r.pass_na,
            Some(true),
            "seed {seed} r0: the no-taint re-verdict"
        );
    }
    // 6149 r1: not served after the SegmentEnd fix — both predicates
    // false and `pass_na` null (recorded only when served).
    let rec = e4_recovery::workload::run_seed(6149, None);
    let r = &rec.rounds[1];
    assert!(!r.served, "seed 6149 r1: the serve is gone after step 2");
    assert!(!r.skx && !r.smx, "seed 6149 r1: skx = smx = false");
    assert!(
        r.pass_na.is_none(),
        "seed 6149 r1: pass_na is null (not served)"
    );
}

/// **X-5c-F3-F1 step 6, X5-R30 (c)'s condition pins (a literal
/// fixture).** Each condition of [`harness::r30_predicates`] is
/// load-bearing: the same world with one condition broken must turn
/// the predicate off. The fixture: a page file whose meta slot 2 was
/// written and `sync_all`ed (a `DWrite` with the `all` mark), the
/// served range exactly that slot's read, class MP with X5-R5's
/// conditions 1+2 — `smx` holds. Each variant drops one condition.
#[test]
fn r30_predicate_conditions_are_load_bearing() {
    use e4_recovery::sim::SimDisk;
    // The fixture disk: "/pages" with meta slot 2 written and
    // sync_all'ed.
    let (pf, slot) = (0usize, 2u64);
    let mut disk = SimDisk::new();
    let f = disk.create("/pages").expect("create");
    assert_eq!(f, pf);
    let bytes = vec![7u8; 4096];
    disk.write_at(f, slot * 4096, &bytes).expect("write");
    disk.sync_all(f).expect("sync_all");
    let served = vec![(pf, slot * 4096, 4096u64)];
    let yes = |disk: &SimDisk,
               workload_empty: bool,
               r_taint: bool,
               class: e4_recovery::sim::FaultClass| {
        harness::r30_predicates(
            class,
            &served,
            workload_empty,
            r_taint,
            false,
            true,
            true,
            &std::collections::BTreeSet::new(),
            Some(slot),
            disk,
            pf,
        )
    };
    // The base world: smx holds (skx cannot — a meta slot is never in
    // K), and every dropped condition turns it off.
    let (skx, smx) = yes(&disk, true, false, e4_recovery::sim::FaultClass::Mp);
    assert!(!skx && smx, "the base world: smx holds, skx does not");
    // (b) a workload-pass serve was added: both predicates off.
    let (a, b) = yes(&disk, false, false, e4_recovery::sim::FaultClass::Mp);
    assert!(!a && !b, "a workload-pass serve turns both predicates off");
    // (c) R-taint: both off.
    let (a, b) = yes(&disk, true, true, e4_recovery::sim::FaultClass::Mp);
    assert!(!a && !b, "R-taint turns both predicates off");
    // (e) the accepted meta bytes match no sync_all-covered DWrite: a
    // rewrite that only `sync_data` covered changes the slot's durable
    // bytes, and the only `all`-covered DWrite's bytes no longer equal
    // them — smx off.
    let mut dirty2 = SimDisk::new();
    let f3 = dirty2.create("/pages").expect("create 3");
    dirty2.write_at(f3, slot * 4096, &bytes).expect("write");
    dirty2.sync_all(f3).expect("sync_all");
    dirty2
        .write_at(f3, slot * 4096, &[9u8; 4096])
        .expect("rewrite");
    dirty2.sync_data(f3).expect("sync_data the rewrite");
    let (a, b) = yes(&dirty2, true, false, e4_recovery::sim::FaultClass::Mp);
    assert!(
        !a && !b,
        "(e) durable bytes matching no sync_all-covered DWrite turn smx off"
    );
    // The same no-match shape via a LOST fault's pre-image revert (the
    // durable bytes change with no new DWrite at all).
    use e4_recovery::sim::{MediaFault, MediaKind};
    let mut faulted = SimDisk::new();
    let f4 = faulted.create("/pages").expect("create 4");
    faulted.write_at(f4, slot * 4096, &bytes).expect("write");
    faulted.sync_all(f4).expect("sync_all");
    let fw = MediaFault {
        inode: f4,
        off: slot * 4096,
        len: 4096,
        bytes: vec![9u8; 4096],
        pre: vec![0u8; 4096],
        kind: MediaKind::Lost,
    };
    faulted.apply_one_media_fault(&fw);
    let (a, b) = yes(&faulted, true, false, e4_recovery::sim::FaultClass::Mp);
    assert!(
        !a && !b,
        "(e) durable bytes matching no synced DWrite turn smx off"
    );
    // (f) class MW: both predicates false.
    let (a, b) = yes(&disk, true, false, e4_recovery::sim::FaultClass::Mw);
    assert!(!a && !b, "class MW turns both predicates off");
}

// ---- X5-R34 (g): the derivation's self-tests (literal event lists) ----

/// A tiny event-list builder for the X5-R34 self-tests: write, sync,
/// dir-op and dirsync events in index order (every env op is one atomic
/// sim step, so `WriteIssued`+`WriteReturned` — and
/// `SyncIssued`+`SyncReturned` — sit at adjacent indices).
mod x5r34 {
    use e4_recovery::sim::DiskEvent;
    use std::collections::BTreeMap;

    pub struct Ev {
        pub events: Vec<DiskEvent>,
    }

    impl Ev {
        pub fn new() -> Self {
            Ev { events: Vec::new() }
        }
        pub fn write(&mut self, inode: usize, off: u64, bytes: &[u8]) -> &mut Self {
            self.events.push(DiskEvent::WriteIssued {
                inode,
                off,
                bytes: bytes.to_vec(),
            });
            self.events.push(DiskEvent::WriteReturned {
                inode,
                off,
                len: bytes.len() as u64,
            });
            self
        }
        pub fn sync(&mut self, inode: usize) -> &mut Self {
            self.events
                .push(DiskEvent::SyncIssued { inode, all: false });
            self.events
                .push(DiskEvent::SyncReturned { inode, all: false });
            self
        }
        pub fn dirop(&mut self, what: &str) -> &mut Self {
            self.events.push(DiskEvent::DirOpIssued {
                dir: "/wal/".to_owned(),
                what: what.to_owned(),
            });
            self
        }
        pub fn dirsync(&mut self) -> &mut Self {
            self.events.push(DiskEvent::SyncDirReturned {
                dir: "/wal/".to_owned(),
            });
            self
        }
        pub fn bindings(pairs: &[(&str, usize)]) -> BTreeMap<String, usize> {
            pairs.iter().map(|(n, i)| ((*n).to_owned(), *i)).collect()
        }
    }

    impl Default for Ev {
        fn default() -> Self {
            Self::new()
        }
    }

    /// A real `K_COMMIT` record of exactly `len` bytes at `lsn` (the
    /// tests encode genuine records so both derivations' walks yield
    /// them; dummy bytes would hide the frozen form's verdicts).
    pub fn rec(ts: u64, lsn: u64, len: u64) -> Vec<u8> {
        let payload = vec![0u8; (len as usize).saturating_sub(45)];
        e4_recovery::store::wal::enc_record(
            e4_recovery::store::wal::K_COMMIT,
            (1, 1),
            ts,
            &payload,
            lsn,
        )
    }
}

/// **X5-R34 (g) 1 — 20 r2's shape:** an inode is written and synced in
/// its life as segment A, renamed to B and dir-synced; then a SyncOn
/// record is written at B's offset with no later sync. It must **not**
/// be acked: the bytes below the record are segment A's stale life
/// (only writes made after a name was bound to its inode belong to the
/// stream at that name), and the record itself was never synced.
#[test]
fn x5r34_stale_bytes_of_a_renamed_segment_never_ack() {
    use e4_recovery::harness;
    use std::collections::BTreeMap;
    let seg_a = 0x60000u64;
    let seg_b = 0xa0000u64;
    let name_a = harness::x5r34_seg_name(seg_a);
    let name_b = harness::x5r34_seg_name(seg_b);
    let inode = 7usize;
    // The old life as segment A: bytes [0, 12000) written and synced.
    let mut ev = x5r34::Ev::new();
    ev.write(inode, 0, &vec![1u8; 12000]).sync(inode);
    // The recycle: A renamed to B, dir-synced (B durably bound, A gone).
    ev.dirop(&format!("rename /wal/{name_a} -> /wal/{name_b}"))
        .dirsync();
    // The new record at B@5952, written, never synced.
    let lsn = seg_b + 5952;
    let len = 3573u64;
    ev.write(inode, 5952, &x5r34::rec(64, lsn, len));
    let mut commits: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    commits.insert(64u64, (true, lsn, len));
    let bindings = x5r34::Ev::bindings(&[(&name_b, inode)]);
    let (_w, acked, _u) = harness::derive_written_acked(&ev.events, &commits, seg_b, &bindings);
    assert!(
        !acked.contains(&64),
        "a SyncOn record over a renamed segment's stale life, never synced, must not be acked"
    );
    // D stops at B's first byte: even the stale prefix is not durable.
    let (d, _stop, rc) = harness::x5r34_durable_prefix(&ev.events, &bindings, seg_b);
    assert_eq!(d, seg_b, "D stops at segment B's first LSN (stale bytes)");
    assert!(
        !rc.contains(&name_b),
        "B is the rename target, not a source: no exemption"
    );
}

/// **X5-R34 (g) 2 — contiguity:** batch 2 is written and synced while
/// batch 1 below it is unwritten. Batch 2's SyncOn commit must not be
/// acked — with batch 2 on the same inode, and as a variant on the next
/// segment's inode. I-ACK-PREFIX: the frontiers advance only over the
/// contiguous prefix.
#[test]
fn x5r34_contiguous_prefix_required_two_batches() {
    use e4_recovery::harness;
    use std::collections::BTreeMap;
    let name0 = harness::x5r34_seg_name(0);
    // Same inode: batch 1 at [0, 2000) never written; batch 2 at
    // [2000, 2500) written and synced.
    let inode = 9usize;
    let mut ev = x5r34::Ev::new();
    ev.dirop(&format!("create /wal/{name0}"))
        .dirsync()
        .write(inode, 2000, &x5r34::rec(7, 2000, 500))
        .sync(inode);
    let mut commits: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    commits.insert(7u64, (true, 2000, 500));
    let bindings = x5r34::Ev::bindings(&[(&name0, inode)]);
    let (_w, acked, _u) = harness::derive_written_acked(&ev.events, &commits, 0, &bindings);
    assert!(
        !acked.contains(&7),
        "a synced batch above an unwritten earlier batch must not be acked (same inode)"
    );
    // The next segment's inode: batch 2 in segment 1, same hole below.
    let name1 = harness::x5r34_seg_name(65536);
    let inode1 = 11usize;
    let mut ev2 = x5r34::Ev::new();
    ev2.dirop(&format!("create /wal/{name0}"))
        .dirsync()
        .dirop(&format!("create /wal/{name1}"))
        .dirsync()
        .write(inode1, 0, &x5r34::rec(8, 65536, 500))
        .sync(inode1);
    let mut commits2: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    commits2.insert(8u64, (true, 65536, 500));
    let bindings2 = x5r34::Ev::bindings(&[(&name0, inode), (&name1, inode1)]);
    let (_w2, acked2, _u2) = harness::derive_written_acked(&ev2.events, &commits2, 0, &bindings2);
    assert!(
        !acked2.contains(&8),
        "a synced batch above an unwritten earlier batch must not be acked (next segment)"
    );
}

/// **X5-R34 (g) 3 — the directory:** a segment is created with no later
/// `sync_dir("/wal/")`. Its records must not be acked, even though they
/// are synced (G1: no record in a created or renamed segment counts
/// durable before the directory entry survives).
#[test]
fn x5r34_unsynced_directory_binding_blocks_acked() {
    use e4_recovery::harness;
    use std::collections::BTreeMap;
    let name0 = harness::x5r34_seg_name(0);
    let inode = 5usize;
    let mut ev = x5r34::Ev::new();
    ev.dirop(&format!("create /wal/{name0}"))
        .write(inode, 0, &x5r34::rec(3, 0, 500))
        .sync(inode);
    let mut commits: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    commits.insert(3u64, (true, 0, 500));
    // The durable bindings are empty: the create never survived a
    // sync_dir, exactly as a crash would leave it.
    let bindings = x5r34::Ev::bindings(&[]);
    let (_w, acked, _u) = harness::derive_written_acked(&ev.events, &commits, 0, &bindings);
    assert!(
        !acked.contains(&3),
        "a synced record in a segment whose directory binding never became durable must not be acked"
    );
}

/// **X5-R34 (g) 4 — a forged early ack:** `client_acked` holds a ts
/// whose record ends above `D`. The cross-check must name it, with the
/// ACK-OVER-HOLE text verbatim.
#[test]
fn x5r34_forged_ack_over_hole_fails_loudly() {
    use e4_recovery::harness;
    use std::collections::{BTreeMap, BTreeSet};
    let name0 = harness::x5r34_seg_name(0);
    let inode = 5usize;
    // ts 5: written and synced in a durably bound segment — a genuine
    // ack. ts 9: forged (its record sits in segment 1, never written).
    let mut ev = x5r34::Ev::new();
    ev.dirop(&format!("create /wal/{name0}"))
        .dirsync()
        .write(inode, 0, &x5r34::rec(5, 0, 500))
        .sync(inode);
    let mut commits: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    commits.insert(5u64, (true, 0, 500));
    commits.insert(9u64, (true, 65536, 500));
    let bindings = x5r34::Ev::bindings(&[(&name0, inode)]);
    let (d, _stop, rc) = harness::x5r34_durable_prefix(&ev.events, &bindings, 0);
    assert_eq!(d, 500, "D covers exactly the synced record");
    let mut client: BTreeSet<u64> = BTreeSet::new();
    client.insert(5);
    client.insert(9);
    let over = harness::x5r34_ack_over(&client, &commits, d, &rc);
    assert_eq!(over, vec![9u64], "only the forged ts is named");
    assert_eq!(
        harness::x5r34_ack_over_text(9, 65536, 500, d),
        "ACK-OVER-HOLE: client-acked SyncOn ts=9 ends at 66036 above durable_end 500 (I-ACK-PREFIX, X5-R34)",
        "the failure text is X5-R34 (f)'s, verbatim"
    );
    // The genuine ack is never named.
    let mut honest: BTreeSet<u64> = BTreeSet::new();
    honest.insert(5);
    assert!(
        harness::x5r34_ack_over(&honest, &commits, d, &rc).is_empty(),
        "a client ack at or below D is not a failure"
    );
}

/// **X5-R34 (g) 5 — must pass:** (a) a record written after its
/// segment's durable binding and then synced, contiguous from `p0`, is
/// acked; (b) a record in an `Rc` segment is acked, and exempt from
/// (f); (c) a SyncOn record placed after an exact segment end, in a
/// durably bound and synced next segment, is acked.
#[test]
fn x5r34_genuine_durability_acks() {
    use e4_recovery::harness;
    use std::collections::{BTreeMap, BTreeSet};
    // (a) written after the binding, synced, contiguous from p0.
    let name0 = harness::x5r34_seg_name(0);
    let inode = 5usize;
    let mut ev = x5r34::Ev::new();
    ev.dirop(&format!("create /wal/{name0}"))
        .dirsync()
        .write(inode, 0, &x5r34::rec(5, 0, 500))
        .sync(inode);
    let mut commits: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    commits.insert(5u64, (true, 0, 500));
    let bindings = x5r34::Ev::bindings(&[(&name0, inode)]);
    let (_w, acked, _u) = harness::derive_written_acked(&ev.events, &commits, 0, &bindings);
    assert!(
        acked.contains(&5),
        "(a) written after the durable binding, then synced: acked"
    );
    // (b) the segment was renamed away after the commit: Rc exempts it,
    // and (f) never names it.
    let name_dst = harness::x5r34_seg_name(65536);
    ev.dirop(&format!("rename /wal/{name0} -> /wal/{name_dst}"))
        .dirsync();
    let (d2, _s2, rc2) = harness::x5r34_durable_prefix(&ev.events, &bindings, 0);
    assert!(rc2.contains(&name0), "(b) the rename source is in Rc");
    let (_w2, acked2, _u2) = harness::derive_written_acked(&ev.events, &commits, 0, &bindings);
    assert!(
        acked2.contains(&5),
        "(b) a record in a recycled segment is acked (the exemption)"
    );
    let mut client: BTreeSet<u64> = BTreeSet::new();
    client.insert(5);
    assert!(
        harness::x5r34_ack_over(&client, &commits, d2, &rc2).is_empty(),
        "(b) an Rc commit is exempt from the cross-check"
    );
    // (c) a record ending exactly at segment 0's end, then a SyncOn
    // record in a durably bound and synced segment 1: both acked, and D
    // crosses the boundary.
    let inode1 = 6usize;
    let mut ev3 = x5r34::Ev::new();
    ev3.dirop(&format!("create /wal/{name0}")).dirsync();
    // Four cap-size records fill segment 0 exactly (4 x 16384 = 65536;
    // REC_MAX = WAL_SEG/4), so the last ends exactly at the boundary.
    let cap = 16384u64;
    for r in 0..4u64 {
        let lsn = r * cap;
        ev3.write(inode, lsn, &x5r34::rec(11 + r, lsn, cap));
    }
    ev3.sync(inode);
    let name1 = name1_of();
    ev3.dirop(&format!("create /wal/{name1}"))
        .dirsync()
        .write(inode1, 0, &x5r34::rec(12, 65536, 700))
        .sync(inode1);
    let mut commits3: BTreeMap<u64, (bool, u64, u64)> = BTreeMap::new();
    for r in 0..4u64 {
        commits3.insert(11 + r, (true, r * cap, cap));
    }
    commits3.insert(12u64, (true, 65536, 700));
    let bindings3 = x5r34::Ev::bindings(&[(&name0, inode), (&name1, inode1)]);
    let (d3, _s3, _rc3) = harness::x5r34_durable_prefix(&ev3.events, &bindings3, 0);
    assert_eq!(
        d3,
        65536 + 700,
        "(c) D crosses the exact segment end into the next segment"
    );
    let (_w3, acked3, _u3) = harness::derive_written_acked(&ev3.events, &commits3, 0, &bindings3);
    assert!(
        acked3.contains(&11) && acked3.contains(&12),
        "(c) both the boundary record and the next segment's SyncOn record are acked"
    );
}

/// Segment 1's name (a small readability helper for the test above).
fn name1_of() -> String {
    e4_recovery::harness::x5r34_seg_name(65536)
}
