//! Oracle self-tests (card X-5 Work 2.6, with the 2.7 additions).
//! P-written, binding: synthetic histories and recovered states, with no
//! store. Six cases must pass, seventeen must fail, and the five
//! instrumentation removals must each be load-bearing (the removed check
//! lets a previously failing case pass). Plus the gate REWORK (2026-10-23)
//! additions: must-fail 18 (the (ii) completeness half) and 19 (a missing
//! twin fails closed), and the pinned model and MISDIRECTED-landing-slot
//! fixes.

use std::collections::{BTreeMap, BTreeSet};

use e4_recovery::model::{CATALOG, Mode, Model, Op, PUBLIC, Scan, to_be64};
use e4_recovery::oracle::{
    Extracted, FaultTargets, Finding, OracleCfg, RawEntry, ReadFail, StateReader, TwinInfo, XxSite,
    check_round, seed_passes,
};
use e4_recovery::rng::Rng;
use e4_recovery::sim::{FaultClass, MediaFault, MediaKind};

/// A synthetic recovered state: per-tree scans and raw entries.
/// Per tree: a list of `(ts, key, val/tomb)` versions.
type SynthTree = Vec<(u64, Vec<u8>, Option<Vec<u8>>)>;
#[derive(Default, Clone)]
struct Synth {
    trees: BTreeMap<u64, SynthTree>,
    raw_extra: Vec<(u64, RawEntry)>, // extra raw entries per tree
}

impl Synth {
    fn add(&mut self, tree: u64, ts: u64, key: &[u8], val: Option<&[u8]>) {
        self.trees
            .entry(tree)
            .or_default()
            .push((ts, key.to_vec(), val.map(|v| v.to_vec())));
    }
    fn scan_of(&self, tree: u64, s: u64) -> Scan {
        let mut out: Scan = BTreeMap::new();
        if let Some(vers) = self.trees.get(&tree) {
            for (ts, k, v) in vers {
                if *ts <= s {
                    out.insert(k.clone(), v.clone());
                }
            }
        }
        out
    }
}

impl StateReader for Synth {
    fn scan(&mut self, tree: u64, s: u64) -> Result<Scan, ReadFail> {
        Ok(self.scan_of(tree, s))
    }
    fn raw(&mut self, tree: u64) -> Result<Vec<RawEntry>, ReadFail> {
        let mut out = Vec::new();
        if let Some(vers) = self.trees.get(&tree) {
            for (ts, k, v) in vers {
                out.push(RawEntry {
                    key: k.clone(),
                    ts: *ts,
                    tag: if v.is_some() { 0 } else { 1 },
                    value: v.clone().unwrap_or_default(),
                });
            }
        }
        out.extend(
            self.raw_extra
                .iter()
                .filter(|(t, _)| *t == tree)
                .map(|(_, e)| e.clone()),
        );
        Ok(out)
    }
}

/// Build a model: commits at ts 10 (Put a=1, SyncOn acked), 20 (Put a=2,
/// SyncOff written), 30 (Put b=3, SyncOff written), publishing tree 7 at
/// 10. Catalog rows at CATALOG.
fn base_model() -> Model {
    let mut m = Model::new();
    m.commit(
        10,
        Mode::SyncOn,
        vec![
            Op::CreateTree { tree: 7 },
            Op::Put {
                tree: CATALOG,
                key: to_be64(7),
                value: PUBLIC.to_vec(),
            },
            Op::Put {
                tree: 7,
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
        ],
    );
    m.commit(
        20,
        Mode::SyncOff,
        vec![Op::Put {
            tree: 7,
            key: b"a".to_vec(),
            value: b"2".to_vec(),
        }],
    );
    m.commit(
        30,
        Mode::SyncOff,
        vec![Op::Put {
            tree: 7,
            key: b"b".to_vec(),
            value: b"3".to_vec(),
        }],
    );
    m.mark_acked(10);
    m.mark_written(20);
    m.mark_written(30);
    // written flags: only 20 and 30 written; 10's record is in the image.
    m
}

/// The matching synthetic state for a cut at `c` (all commits ≤ c
/// applied, catalog published at 10).
fn state_at(m: &Model, c: u64) -> Synth {
    let mut s = Synth::default();
    let mut trees: BTreeSet<u64> = BTreeSet::new();
    for rec in m.commits.values() {
        if rec.ts > c {
            continue;
        }
        for op in &rec.ops {
            match op {
                Op::CreateTree { tree } | Op::DropTree { tree } => {
                    trees.insert(*tree);
                }
                Op::Put { tree, key, value } => {
                    trees.insert(*tree);
                    s.add(*tree, rec.ts, key, Some(value));
                }
                Op::Delete { tree, key } => {
                    trees.insert(*tree);
                    s.add(*tree, rec.ts, key, None);
                }
            }
        }
    }
    // Only the newest version per key survives a raw scan of a real tree,
    // but the store keeps versions; emulate versions for scans only.
    // Synth.scan keeps the newest ≤ S, so adding all versions is correct.
    s
}

fn ex_for(_synth: &Synth, c: u64, model: &Model) -> Extracted {
    Extracted {
        ok: true,
        xx001: vec![],
        served: false,
        frontier: c,
        gc_w: 0,
        opened_t: 0,
        published: model.published_at(c),
        amcheck_i: vec![],
        amcheck_ii_ran: false,
        amcheck_ii: vec![],
        image_slots: BTreeSet::new(),
        tainted_image_slots: BTreeSet::new(),
        meta_rollback: false,
        amcheck_i_ran: false,
        rollback_slots: BTreeSet::new(),
        image_slots_ii: BTreeSet::new(),
        tainted_image_slots_ii: BTreeSet::new(),
    }
}

fn run(
    model: &Model,
    class: FaultClass,
    synth: &mut Synth,
    ex: &Extracted,
    faults: &FaultTargets,
    twin: Option<&TwinInfo>,
) -> e4_recovery::oracle::RoundVerdict {
    let mut rng = Rng::new(1);
    check_round(
        model,
        class,
        0,
        ex,
        synth,
        faults,
        twin,
        &mut rng,
        &OracleCfg::default(),
    )
}

/// Must pass (1): the state at `c = lo`, at `c = hi`, and at a `c` in
/// between.
#[test]
fn mp1_state_at_lo_mid_hi() {
    let m = base_model();
    // lo = 10 (acked SyncOn), hi = 30 (written).
    for c in [10u64, 20, 30] {
        let mut s = state_at(&m, c);
        let mut ex = ex_for(&s, c, &m);
        ex.frontier = c;
        ex.gc_w = 0;
        let v = run(
            &m,
            FaultClass::C,
            &mut s,
            &ex,
            &FaultTargets::default(),
            None,
        );
        assert!(v.pass, "c={c} must pass: {:?}", v);
    }
}

/// Must pass (2): a history mismatch only at `S < gc_w`.
#[test]
fn mp2_mismatch_below_gc_w() {
    let mut m = base_model();
    // GC pruned a=1 (ts 10) with watermark 25: reads at S ≥ 25 are
    // unaffected; reads at S < 25 would differ.
    m.commit(
        25,
        Mode::SyncOn,
        vec![Op::Delete {
            tree: 7,
            key: b"nonexistent".to_vec(),
        }],
    );
    m.mark_acked(25);
    // lo is now 25 (acked SyncOn at 25); hi = 30.
    // Recovered state: everything at cut 30, but a=1 pruned (GC'd).
    let mut s = state_at(&m, 30);
    if let Some(vers) = s.trees.get_mut(&7) {
        vers.retain(|(ts, k, _)| !(k == b"a" && *ts == 10));
    }
    let mut ex = ex_for(&s, 30, &m);
    ex.frontier = 30;
    ex.gc_w = 25;
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(v.pass, "pruning below gc_w must pass: {:?}", v);
}

/// Must pass (3): contents of an unpublished tree that differ from any
/// model.
#[test]
fn mp3_unpublished_tree_garbage() {
    let m = base_model();
    let mut s = state_at(&m, 10);
    // Tree 9 exists in the store with junk — never published.
    s.add(9, 5, b"junk", Some(b"garbage"));
    s.add(9, 99, b"junk2", Some(b"garbage2"));
    let mut ex = ex_for(&s, 10, &m);
    ex.frontier = 10;
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(
        v.pass,
        "unpublished trees are excluded from (a)-(d): {:?}",
        v
    );
}

/// Must pass (4): MP XX001 where one fault hit a referenced slot.
#[test]
fn mp4_mp_detection_on_referenced_slot() {
    let m = base_model();
    let mut s = state_at(&m, 10);
    let mut ex = ex_for(&s, 10, &m);
    ex.ok = false; // recovery failed with XX001 at open
    ex.xx001 = vec![XxSite::Open];
    // One media fault hit referenced slot 5; amcheck (i) flags exactly it.
    let mut tainted = BTreeSet::new();
    tainted.insert(5u64);
    ex.tainted_image_slots = tainted;
    ex.image_slots = {
        let mut t = BTreeSet::new();
        t.insert(5u64);
        t
    };
    ex.amcheck_i = vec![Finding {
        slot: Some(5),
        what: "crc mismatch".to_owned(),
    }];
    let mut faults = FaultTargets::default();
    faults.page_slots.insert(5u64);
    let mut twin = TwinInfo {
        page_file: 0,
        recovery_reads: vec![],
        image_slots: BTreeSet::new(),
    };
    twin.image_slots.insert(5u64);
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(
        v.pass,
        "MP detection on a referenced slot must pass: {:?}",
        v
    );
    assert_eq!(v.detections, vec![XxSite::Open]);
}

/// Must pass (5): MW with `c < max acked` but `c ≥ T` (ruling X5-R1: in
/// MW, `lo := T` of the opened meta; acked commits lost are a known cost).
#[test]
fn mp5_mw_lost_acked_above_t() {
    let mut m = base_model();
    // The WAL above T was media-damaged: commits 20 and 30 were acked
    // (SyncOn) but their records are gone; the recovered state is at 10.
    if let Some(r) = m.commits.get_mut(&20) {
        r.mode = Mode::SyncOn;
        r.acked = true;
    }
    if let Some(r) = m.commits.get_mut(&30) {
        r.mode = Mode::SyncOn;
        r.acked = true;
    }
    let mut s = state_at(&m, 10);
    let mut ex = ex_for(&s, 10, &m);
    ex.frontier = 10;
    ex.opened_t = 10;
    let v = run(
        &m,
        FaultClass::Mw,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(v.pass, "MW at c = T must pass: {:?}", v);
    assert!(!v.acked_lost.is_empty(), "the acked loss is reported");
}

/// Must fail (1): `c = lo − 1` — the model just below `lo`.
#[test]
fn mf1_below_lo_fails() {
    let mut m = base_model();
    // lo = 10; recovered state = the commit at 5 (below every candidate).
    m.commit(
        5,
        Mode::SyncOn,
        vec![Op::Put {
            tree: 7,
            key: b"a".to_vec(),
            value: b"0".to_vec(),
        }],
    );
    // Commit 5 publishes nothing; tree 7 not yet published at 5, so the
    // recovered state has tree 7 present-but-unpublished.
    let mut s = Synth::default();
    s.add(7, 5, b"a", Some(b"0"));
    let mut ex = ex_for(&s, 5, &m);
    ex.frontier = 5;
    ex.published = BTreeSet::new();
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "state below lo must fail");
}

/// Must fail (2): `c = hi + 1` — an unwritten commit present.
#[test]
fn mf2_beyond_hi_fails() {
    let mut m = base_model();
    // hi = 30 (written 20, 30); commit 40 exists in the model but was
    // never written; the recovered state contains it.
    m.commit(
        40,
        Mode::SyncOff,
        vec![Op::Put {
            tree: 7,
            key: b"c".to_vec(),
            value: b"4".to_vec(),
        }],
    );
    let mut s = state_at(&m, 40);
    let mut ex = ex_for(&s, 40, &m);
    ex.frontier = 40;
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "an unwritten commit present must fail");
}

/// Must fail (3): a non-prefix — commit ts_k missing while ts_{k+1} is
/// present.
#[test]
fn mf3_non_prefix_fails() {
    let m = base_model();
    // Recovered = {10, 30}: commit 20's effect is missing.
    let mut s = state_at(&m, 30);
    if let Some(vers) = s.trees.get_mut(&7) {
        vers.retain(|(ts, _, _)| *ts != 20);
    }
    let mut ex = ex_for(&s, 30, &m);
    ex.frontier = 30;
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "a missing middle commit must fail");
}

/// Must fail (4): one extra raw entry with ts > c.
#[test]
fn mf4_extra_raw_entry_fails() {
    let m = base_model();
    let mut s = state_at(&m, 10);
    s.raw_extra.push((
        7,
        RawEntry {
            key: b"zzz".to_vec().to_vec(),
            ts: 35,
            tag: 0,
            value: b"5".to_vec(),
        },
    ));
    let mut ex = ex_for(&s, 10, &m);
    ex.frontier = 10;
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "an extra raw entry above the cut must fail");
}

/// Must fail (5): a history mismatch at `S = gc_w`.
#[test]
fn mf5_mismatch_at_gc_w_fails() {
    let m = base_model();
    // gc_w = 10; the recovered state pruned a=1 (visible at S = 10).
    let mut s = state_at(&m, 30);
    if let Some(vers) = s.trees.get_mut(&7) {
        vers.retain(|(ts, k, _)| !(k == b"a" && *ts == 10));
    }
    let mut ex = ex_for(&s, 30, &m);
    ex.frontier = 30;
    ex.gc_w = 10;
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "a mismatch at S = gc_w must fail");
}

/// Must fail (6): a dropped tree still published.
#[test]
fn mf6_dropped_tree_published_fails() {
    let mut m = base_model();
    m.commit(
        15,
        Mode::SyncOn,
        vec![
            Op::Delete {
                tree: CATALOG,
                key: to_be64(7),
            },
            Op::DropTree { tree: 7 },
        ],
    );
    m.mark_acked(15);
    m.mark_written(15);
    // lo = 15, hi = 30; recovered still publishes 7 at cut 30.
    let mut s = state_at(&m, 30);
    let mut ex = ex_for(&s, 30, &m);
    ex.frontier = 30;
    ex.published = {
        let mut t = BTreeSet::new();
        t.insert(7u64);
        t
    };
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "a dropped tree still published must fail");
}

/// Must fail (7): a tainted accepted read (served corruption).
#[test]
fn mf7_served_corruption_fails() {
    let m = base_model();
    let mut s = state_at(&m, 10);
    let mut ex = ex_for(&s, 10, &m);
    ex.frontier = 10;
    ex.served = true;
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "a served corruption must fail");
    assert!(v.o3_fail.clone().unwrap_or_default().contains("O-3(a)"));
}

/// Must fail (8): XX001 in class C.
#[test]
fn mf8_xx001_in_class_c_fails() {
    let m = base_model();
    let mut s = state_at(&m, 10);
    let mut ex = ex_for(&s, 10, &m);
    ex.frontier = 10;
    ex.ok = false;
    ex.xx001 = vec![XxSite::Open];
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "XX001 in class C must fail");
    assert!(v.o3_fail.clone().unwrap_or_default().contains("O-3(b)"));
}

/// Must fail (9): MP XX001 with all faults on dead slots (a false
/// positive).
#[test]
fn mf9_mp_false_positive_fails() {
    let m = base_model();
    let mut s = state_at(&m, 10);
    let mut ex = ex_for(&s, 10, &m);
    ex.ok = false;
    ex.xx001 = vec![XxSite::Replay];
    let mut faults = FaultTargets::default();
    faults.page_slots.insert(40u64); // dead slot
    let twin = TwinInfo {
        page_file: 0,
        recovery_reads: vec![],
        image_slots: {
            let mut t = BTreeSet::new();
            t.insert(5u64);
            t
        },
    };
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(!v.pass, "a false positive must fail");
    assert!(v.o3_fail.clone().unwrap_or_default().contains("O-3(c)"));
}

/// Must fail (10): an MP silent `Ok` with an O-1 mismatch.
#[test]
fn mf10_mp_silent_ok_fails() {
    let m = base_model();
    // Recovered = {10, 30}: a non-prefix (commit 20's effect missing), so
    // no candidate c satisfies O-1 — and silently, with `Ok` and no XX001.
    let mut s = state_at(&m, 30);
    if let Some(vers) = s.trees.get_mut(&7) {
        vers.retain(|(ts, _, _)| *ts != 20);
    }
    let mut ex = ex_for(&s, 30, &m);
    ex.frontier = 30;
    let v = run(
        &m,
        FaultClass::Mp,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(v.o1.is_err(), "the non-prefix must fail O-1");
    assert!(!v.pass, "a silent Ok with O-1 mismatch must fail");
    assert!(v.o3_fail.clone().unwrap_or_default().contains("O-3(d)"));
}

/// Must fail (11): a `DEADLOCK` seed.
#[test]
fn mf11_deadlock_seed_fails() {
    let m = base_model();
    let mut s = state_at(&m, 10);
    let ex = ex_for(&s, 10, &m);
    let v = run(
        &m,
        FaultClass::C,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(v.pass);
    assert!(!seed_passes(&[v], true, false), "a DEADLOCK seed fails");
    assert!(!seed_passes(&[], false, true), "a PANIC seed fails");
}

/// The X5-R5 shape (Work 2.7): class MP, a LOST page fault on meta slot
/// `fault_slot` (slot 1 for the condition-1 cases, an ordinary data slot
/// otherwise), a meta rollback, image slots {5, 8, 9} with tainted {5},
/// R = {8, 9}, and amcheck (i) reporting 5, 8 and 9 — legal only while
/// the exception is active. Amcheck (ii) ran and reports exactly the
/// tainted (ii) slot. Everything else (O-1 at c = 10, O-3 (b)/(c)) is
/// arranged to pass, so the verdict isolates O-3 (e).
fn rollback_world(fault_slot: u64) -> (Model, Synth, Extracted, FaultTargets, TwinInfo) {
    let m = base_model();
    let s = state_at(&m, 10);
    let mut ex = ex_for(&s, 10, &m);
    ex.frontier = 10;
    ex.ok = true;
    ex.xx001 = vec![XxSite::AmcheckI, XxSite::AmcheckIi];
    ex.amcheck_i_ran = true;
    let mut image = BTreeSet::new();
    image.insert(5u64);
    image.insert(8u64);
    image.insert(9u64);
    ex.image_slots = image.clone();
    ex.image_slots_ii = image;
    let mut tainted = BTreeSet::new();
    tainted.insert(5u64);
    ex.tainted_image_slots = tainted.clone();
    ex.tainted_image_slots_ii = tainted;
    let mut rset = BTreeSet::new();
    rset.insert(8u64);
    rset.insert(9u64);
    ex.rollback_slots = rset;
    ex.meta_rollback = true;
    ex.amcheck_i = vec![
        Finding {
            slot: Some(5),
            what: "crc mismatch".to_owned(),
        },
        Finding {
            slot: Some(8),
            what: "stale reference after meta rollback".to_owned(),
        },
        Finding {
            slot: Some(9),
            what: "stale reference after meta rollback".to_owned(),
        },
    ];
    ex.amcheck_ii_ran = true;
    ex.amcheck_ii = vec![Finding {
        slot: Some(5),
        what: "crc mismatch".to_owned(),
    }];
    // A durable LOST page fault at `fault_slot`'s offset, collected the
    // way the harness does (the oracle derives condition 1 itself).
    let fault = MediaFault {
        inode: 0,
        off: fault_slot * 4096,
        len: 4096,
        bytes: vec![1u8; 4096],
        pre: vec![0u8; 4096],
        kind: MediaKind::Lost,
    };
    let faults = FaultTargets::collect(&[fault], 0);
    let mut twin = TwinInfo {
        page_file: 0,
        recovery_reads: vec![],
        image_slots: BTreeSet::new(),
    };
    twin.image_slots.insert(fault_slot);
    twin.image_slots.insert(5u64);
    (m, s, ex, faults, twin)
}

/// Must pass (6): MP, a LOST fault on slot 1, `meta_rollback`, amcheck
/// (i) reporting every tainted referenced slot plus some slots of `R`
/// (X5-R5's exception, Work 2.7).
#[test]
fn mp6_meta_rollback_exception() {
    let (m, mut s, ex, faults, twin) = rollback_world(1);
    assert!(
        faults.meta_slot_fault,
        "a LOST fault on slot 1 is a meta-slot fault"
    );
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(
        v.pass,
        "the X5-R5 exception must accept the R findings: {:?}",
        v
    );
    assert!(v.o1.is_ok());
}

/// Must fail (12): the same findings with `meta_rollback` false —
/// condition 2 of X5-R5 is load-bearing; `R` is ignored and the strict
/// X5-R2 reading fails the R findings.
#[test]
fn mf12_no_meta_rollback_fails() {
    let (m, mut s, mut ex, faults, twin) = rollback_world(1);
    ex.meta_rollback = false;
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(!v.pass, "without a meta rollback the R findings must fail");
    assert!(v.o3_fail.clone().unwrap_or_default().contains("O-3(e)"));
}

/// Must fail (13): the same findings with `meta_rollback` true but no
/// fault on slot 1 or 2 — condition 1 of X5-R5 is load-bearing (checked
/// by the oracle itself, from the fault list).
#[test]
fn mf13_no_meta_slot_fault_fails() {
    let (m, mut s, ex, faults, twin) = rollback_world(5);
    assert!(
        !faults.meta_slot_fault,
        "a fault on slot 5 is not a meta-slot fault"
    );
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(
        !v.pass,
        "without a meta-slot fault the R findings must fail"
    );
    assert!(v.o3_fail.clone().unwrap_or_default().contains("O-3(e)"));
}

/// Must fail (14): a meta rollback with a meta-slot fault, and amcheck
/// reporting a slot outside (tainted ∪ `R`).
#[test]
fn mf14_finding_outside_tainted_and_r_fails() {
    let (m, mut s, mut ex, faults, twin) = rollback_world(1);
    ex.amcheck_i.push(Finding {
        slot: Some(12),
        what: "crc mismatch".to_owned(),
    });
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(!v.pass, "a finding outside tainted and R must fail");
    assert!(v.o3_fail.clone().unwrap_or_default().contains("O-3(e)"));
}

/// Must fail (15): `rollback_slots` holding a tainted slot, or a slot the
/// image does not reference — the structural guard (a harness defect
/// must never pass).
#[test]
fn mf15_malformed_r_fails() {
    // (a) R holds a tainted slot.
    {
        let (m, mut s, mut ex, faults, twin) = rollback_world(1);
        ex.rollback_slots.insert(5u64);
        let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
        assert!(!v.pass, "R holding a tainted slot must fail");
        assert!(
            v.o3_fail
                .clone()
                .unwrap_or_default()
                .contains("harness R set malformed"),
            "got {:?}",
            v.o3_fail
        );
    }
    // (b) R holds a slot the image does not reference.
    {
        let (m, mut s, mut ex, faults, twin) = rollback_world(1);
        ex.rollback_slots.insert(99u64);
        let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
        assert!(!v.pass, "R holding an unreferenced slot must fail");
        assert!(
            v.o3_fail
                .clone()
                .unwrap_or_default()
                .contains("harness R set malformed"),
            "got {:?}",
            v.o3_fail
        );
    }
}

/// Must fail (16): MP, a tainted referenced slot, amcheck (i) ran and
/// reported nothing, XX001 at open — the completeness fix (run 1 gated
/// the completeness half on findings existing, letting a fully missed
/// detection pass beside another XX001 of the round).
#[test]
fn mf16_fully_missed_detection_fails() {
    let (m, mut s, mut ex, faults, twin) = rollback_world(1);
    // Amcheck (i) ran and found nothing at all; the taint remains.
    ex.amcheck_i.clear();
    ex.amcheck_ii_ran = false;
    ex.amcheck_ii.clear();
    // Recovery failed with XX001 at open (another XX001 in the round).
    ex.ok = false;
    ex.xx001 = vec![XxSite::Open];
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(!v.pass, "a fully missed detection must fail");
    assert!(
        v.o3_fail
            .clone()
            .unwrap_or_default()
            .contains("tainted referenced slot 5 not reported"),
        "got {:?}",
        v.o3_fail
    );
}

/// Must fail (17): the same as must-pass 6, but on amcheck (ii) — (ii)
/// reports a slot outside (tainted (ii) ∪ `R`).
#[test]
fn mf17_ii_finding_outside_tainted_and_r_fails() {
    let (m, mut s, mut ex, faults, twin) = rollback_world(1);
    ex.amcheck_ii.push(Finding {
        slot: Some(12),
        what: "crc mismatch".to_owned(),
    });
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(!v.pass, "an (ii) finding outside tainted and R must fail");
    assert!(
        v.o3_fail
            .clone()
            .unwrap_or_default()
            .contains("(ii) reported slot 12"),
        "got {:?}",
        v.o3_fail
    );
}

/// Must fail (18, gate REWORK item 4): amcheck (ii) ran, a slot in
/// `tainted_image_slots_ii` is not reported, everything else passes — the
/// (ii) **completeness** half is load-bearing (the gate's G2 mutant was
/// UNCAUGHT at verdict time).
#[test]
fn mf18_ii_missed_detection_fails() {
    let (m, mut s, mut ex, faults, twin) = rollback_world(1);
    // Amcheck (i) reports everything it must; amcheck (ii) ran and
    // reported nothing at all.
    ex.amcheck_ii.clear();
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(!v.pass, "a fully missed (ii) detection must fail");
    assert!(
        v.o3_fail
            .clone()
            .unwrap_or_default()
            .contains("(ii) tainted referenced slot 5 not reported"),
        "got {:?}",
        v.o3_fail
    );
}

/// Must fail (19, gate REWORK item 5): an MP/MW round with XX001 and **no
/// twin** is a harness defect — O-3 (c) fails closed, never open.
#[test]
fn mf19_missing_twin_fails() {
    // MP: recovery failed with XX001 at open, no twin supplied.
    let (m, mut s, mut ex, faults, _twin) = rollback_world(1);
    ex.ok = false;
    ex.xx001 = vec![XxSite::Open];
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, None);
    assert!(!v.pass, "a missing twin must fail an MP round with XX001");
    assert!(
        v.o3_fail
            .clone()
            .unwrap_or_default()
            .contains("O-3(c): MP/MW round with XX001 and no twin"),
        "got {:?}",
        v.o3_fail
    );
    // MW too (XX001 with no twin, no media faults at all).
    let (m, mut s, mut ex, _faults, _twin) = rollback_world(1);
    ex.ok = false;
    ex.xx001 = vec![XxSite::Replay];
    let v = run(
        &m,
        FaultClass::Mw,
        &mut s,
        &ex,
        &FaultTargets::default(),
        None,
    );
    assert!(!v.pass, "a missing twin must fail an MW round with XX001");
    assert!(
        v.o3_fail
            .clone()
            .unwrap_or_default()
            .contains("O-3(c): MP/MW round with XX001 and no twin"),
        "got {:?}",
        v.o3_fail
    );
}

/// Gate REWORK item 3 (pinned): a load's entries are versions at `S_L` —
/// a later Put on a loaded key wins at every `S ≥` the Put, and the load
/// shows between `S_L` and the Put. The run-1 model applied load entries
/// after all commit ops, so it agreed with a store that silently lost
/// post-load updates.
#[test]
fn model_load_entries_are_versions_at_s_l() {
    let mut m = base_model();
    // Load tree 7's key "k" at S_L = 5 (before the publish at 10), then
    // overwrite it with a logged Put at 20.
    m.load(7, 5, vec![(b"k".to_vec(), b"loaded".to_vec())]);
    m.commit(
        20,
        Mode::SyncOn,
        vec![Op::Put {
            tree: 7,
            key: b"k".to_vec(),
            value: b"put".to_vec(),
        }],
    );
    m.mark_acked(20);
    // Above the Put (the gate's probe): the Put wins, not the load.
    let s30 = m.scan(7, 30);
    assert_eq!(
        s30.get(b"k".as_slice()),
        Some(&Some(b"put".to_vec())),
        "a Put at ts > S_L must beat the load entry"
    );
    // Between S_L and the Put: the load shows.
    let s15 = m.scan(7, 15);
    assert_eq!(s15.get(b"k".as_slice()), Some(&Some(b"loaded".to_vec())));
    // Below S_L: nothing.
    assert!(!m.scan(7, 3).contains_key(b"k".as_slice()));
    // And a Delete at ts > S_L wins the same way.
    let mut m2 = base_model();
    m2.load(7, 5, vec![(b"k".to_vec(), b"loaded".to_vec())]);
    m2.commit(
        20,
        Mode::SyncOn,
        vec![Op::Delete {
            tree: 7,
            key: b"k".to_vec(),
        }],
    );
    m2.mark_acked(20);
    assert_eq!(m2.scan(7, 30).get(b"k".as_slice()), Some(&None));
}

/// Gate REWORK item 6b (pinned): a MISDIRECTED page fault targets its
/// **landing slot** as well as its source — O-3 (c) must not call a
/// detection caused by the stray bytes landing on a referenced slot a
/// false positive.
#[test]
fn misdirected_landing_slot_is_a_fault_target() {
    let fault = MediaFault {
        inode: 0,
        off: 3 * 4096,
        len: 4096,
        bytes: vec![1u8; 4096],
        pre: vec![0u8; 4096],
        kind: MediaKind::Misdirected { to_off: 5 * 4096 },
    };
    // The mapping itself: both the source and the landing slot.
    let targets = FaultTargets::collect(std::slice::from_ref(&fault), 0);
    assert!(targets.page_slots.contains(&3), "the source slot targets");
    assert!(
        targets.page_slots.contains(&5),
        "the landing slot targets too"
    );
    // And the O-3 (c) consequence: XX001 whose only fault's stray bytes
    // land on a slot the twin's image references is not a false positive.
    let m = base_model();
    let mut s = state_at(&m, 10);
    let mut ex = ex_for(&s, 10, &m);
    ex.ok = false; // recovery failed with XX001 at open
    ex.xx001 = vec![XxSite::Open];
    let faults = FaultTargets::collect(&[fault], 0);
    let mut image = BTreeSet::new();
    image.insert(5u64);
    let twin = TwinInfo {
        page_file: 0,
        recovery_reads: vec![],
        image_slots: image,
    };
    let v = run(&m, FaultClass::Mp, &mut s, &ex, &faults, Some(&twin));
    assert!(
        v.pass,
        "the landing slot explains the detection, not a false positive: {:?}",
        v
    );
}

/// The five instrumentation removals are load-bearing: each removed check
/// lets a previously failing case pass (card "Instrumentation checks",
/// with the 2.7 addition of X5-R5's meta-slot-fault condition).
#[test]
fn instrumentation_removals_are_load_bearing() {
    let mut rng = Rng::new(1);
    // (1) O-1's lower bound: `c = lo − 1` now passes.
    {
        let mut m = base_model();
        m.commit(
            5,
            Mode::SyncOn,
            vec![Op::Put {
                tree: 7,
                key: b"a".to_vec(),
                value: b"0".to_vec(),
            }],
        );
        let mut s = Synth::default();
        s.add(7, 5, b"a", Some(b"0"));
        let mut ex = ex_for(&s, 5, &m);
        ex.frontier = 5;
        ex.published = BTreeSet::new();
        let cfg = OracleCfg {
            no_lower_bound: true,
            ..Default::default()
        };
        let v = check_round(
            &m,
            FaultClass::C,
            0,
            &ex,
            &mut s,
            &FaultTargets::default(),
            None,
            &mut rng,
            &cfg,
        );
        assert!(
            v.o1.is_ok(),
            "without the lower bound the below-lo state passes"
        );
    }
    // (2) O-1 (c): the extra raw entry above the cut now passes.
    {
        let m = base_model();
        let mut s = state_at(&m, 10);
        s.raw_extra.push((
            7,
            RawEntry {
                key: b"zzz".to_vec().to_vec(),
                ts: 35,
                tag: 0,
                value: b"5".to_vec(),
            },
        ));
        let mut ex = ex_for(&s, 10, &m);
        ex.frontier = 10;
        let cfg = OracleCfg {
            no_raw_cut: true,
            ..Default::default()
        };
        let v = check_round(
            &m,
            FaultClass::C,
            0,
            &ex,
            &mut s,
            &FaultTargets::default(),
            None,
            &mut rng,
            &cfg,
        );
        assert!(v.o1.is_ok(), "without the raw cut the extra entry passes");
    }
    // (3) The taint comparison: the served corruption now passes.
    {
        let m = base_model();
        let mut s = state_at(&m, 10);
        let mut ex = ex_for(&s, 10, &m);
        ex.frontier = 10;
        ex.served = true;
        let cfg = OracleCfg {
            no_taint: true,
            ..Default::default()
        };
        let v = check_round(
            &m,
            FaultClass::C,
            0,
            &ex,
            &mut s,
            &FaultTargets::default(),
            None,
            &mut rng,
            &cfg,
        );
        assert!(
            v.pass,
            "without the taint comparison the served read passes"
        );
    }
    // (4) The twin false-positive rule: the dead-slot XX001 now passes.
    {
        let m = base_model();
        let mut s = state_at(&m, 10);
        let mut ex = ex_for(&s, 10, &m);
        ex.ok = false;
        ex.xx001 = vec![XxSite::Replay];
        let mut faults = FaultTargets::default();
        faults.page_slots.insert(40u64);
        let twin = TwinInfo {
            page_file: 0,
            recovery_reads: vec![],
            image_slots: BTreeSet::new(),
        };
        let cfg = OracleCfg {
            no_twin_rule: true,
            ..Default::default()
        };
        let v = check_round(
            &m,
            FaultClass::Mp,
            0,
            &ex,
            &mut s,
            &faults,
            Some(&twin),
            &mut rng,
            &cfg,
        );
        assert!(v.pass, "without the twin rule the false positive passes");
    }
    // (5) X5-R5's meta-slot-fault condition (condition 1): must-fail 13's
    // world — meta rollback, findings on R slots, no fault on slot 1 or
    // 2 — now passes.
    {
        let (m, mut s, ex, faults, twin) = rollback_world(5);
        let cfg = OracleCfg {
            no_meta_slot_cond: true,
            ..Default::default()
        };
        let v = check_round(
            &m,
            FaultClass::Mp,
            0,
            &ex,
            &mut s,
            &faults,
            Some(&twin),
            &mut rng,
            &cfg,
        );
        assert!(
            v.pass,
            "without condition 1 the R findings without a meta-slot fault pass"
        );
    }
}
