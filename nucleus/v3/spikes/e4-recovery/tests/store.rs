//! Test-only assertions (`unwrap`/`expect`/`panic`) are allowed
//! throughout this file, matching the frozen X-5a test suites.
#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
//! The B1 store tests (card X-5b Work 11, "B1's tests"; P-specified):
//! create, open, empty scan; 10,000 inserts into one tree with leaf
//! splits and a root split, the scan equal to a `BTreeMap`; a GC sweep
//! that produces merges and a root collapse; one checkpoint (steps 0–6)
//! with a concurrent insert task and the background evictor at the
//! minimum `F`, then a reopen whose scan is equal and whose amcheck is
//! clean — each under three scheduler seeds, with no `DEADLOCK`, no
//! `PANIC` and no `SimFail::Baton`.
//!
//! Uncrashed by design (stage B1): crash behaviour is the directed
//! tests' job (stage B5/B6, `tests/directed.rs`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use e4_recovery::sim::{HARNESS, HasDisk, Sched, SchedCore, SchedHost, SimDisk, SimFail, TaskId};
use e4_recovery::store::ckpt::{self, CkptHook};
use e4_recovery::store::{Cx, Fail, HasStore, Mutant, StoreCfg, StoreInner, amcheck, prims, tree};

/// The test world: scheduling, disk and store slices plus a small shared
/// flag board the task bodies coordinate through.
struct TestWorld {
    sched: SchedCore,
    disk: SimDisk,
    st: StoreInner,
    flags: BTreeMap<&'static str, bool>,
}

impl SchedHost for TestWorld {
    fn sched_mut(&mut self) -> &mut SchedCore {
        &mut self.sched
    }
}

impl HasDisk for TestWorld {
    fn disk(&mut self) -> &mut SimDisk {
        &mut self.disk
    }
}

impl e4_recovery::store::HasStore for TestWorld {
    fn store(&mut self) -> &mut StoreInner {
        &mut self.st
    }
}

/// A test configuration (test-scale overrides of §2.1/§4.1, card 3.3).
fn tcfg(map_leaf_cap: u64, frames: usize, r_epoch: usize) -> StoreCfg {
    StoreCfg {
        map_leaf_cap,
        freeset_leaf_bits: 128,
        pool_frames: frames,
        r_epoch,
        mutant: None,
    }
}

/// What a pass observed (the S-BATON and PANIC report).
#[derive(Debug, Default, Clone, Copy)]
struct PassReport {
    deadlock: bool,
    baton: bool,
    panic: bool,
}

impl PassReport {
    fn assert_clean(&self, what: &str) {
        assert!(!self.deadlock, "{what}: DEADLOCK");
        assert!(!self.baton, "{what}: SimFail::Baton during the pass");
        assert!(!self.panic, "{what}: a task panicked");
    }
}

/// Run `body(task)` on `ntasks` threads under the baton over the given
/// initial `(disk, store)`; returns the world afterwards plus the pass
/// report (a task's `Err` that names `Baton` or `Deadlock` marks the
/// report; panics mark it too — the assertion happens in the caller,
/// with context).
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
fn run_world<F>(
    ntasks: usize,
    seed: u64,
    disk: SimDisk,
    st: StoreInner,
    body: F,
) -> (Sched<TestWorld>, PassReport)
where
    F: Fn(&Sched<TestWorld>, TaskId) -> Result<(), SimFail> + Send + Sync + 'static,
{
    let host = TestWorld {
        sched: SchedCore::new(ntasks, seed),
        disk,
        st,
        flags: BTreeMap::new(),
    };
    let sched = Arc::new(Sched::new(host));
    let body = &body;
    let panicked = Arc::new(AtomicBool::new(false));
    let saw_baton = Arc::new(AtomicBool::new(false));
    let saw_deadlock = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for t in 1..ntasks {
            let sched = Arc::clone(&sched);
            let panicked = Arc::clone(&panicked);
            let saw_baton = Arc::clone(&saw_baton);
            let saw_deadlock = Arc::clone(&saw_deadlock);
            handles.push(scope.spawn(move || {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&sched, t)));
                match r {
                    Err(_) => panicked.store(true, Ordering::SeqCst),
                    Ok(Err(SimFail::Baton(_))) => saw_baton.store(true, Ordering::SeqCst),
                    Ok(Err(SimFail::Deadlock)) => saw_deadlock.store(true, Ordering::SeqCst),
                    Ok(_) => {}
                }
                sched.finish(t);
            }));
        }
        let _ = sched.start();
        for h in handles {
            let _ = h.join();
        }
    });
    let sched_deadlock = sched.lock().sched.deadlock;
    let report = PassReport {
        deadlock: sched_deadlock || saw_deadlock.load(Ordering::SeqCst),
        baton: saw_baton.load(Ordering::SeqCst),
        panic: panicked.load(Ordering::SeqCst),
    };
    let owned = Arc::try_unwrap(sched).unwrap_or_else(|_| panic!("scheduler still shared"));
    (owned, report)
}

/// Build a world for a pass from a durable disk + store state.
fn pass_world(ntasks: usize, seed: u64, disk: SimDisk, st: StoreInner) -> Sched<TestWorld> {
    let host = TestWorld {
        sched: SchedCore::new(ntasks, seed),
        disk,
        st,
        flags: BTreeMap::new(),
    };
    Sched::new(host)
}

/// A flag step (shared board).
fn set_flag(sched: &Sched<TestWorld>, k: &'static str) {
    let mut g = sched.lock();
    g.flags.insert(k, true);
}

fn flag(sched: &Sched<TestWorld>, k: &'static str) -> bool {
    let g = sched.lock();
    g.flags.get(k).copied().unwrap_or(false)
}

/// The store's create pre-pass (§2.3 + G6): one task, no crash points.
/// Returns the durable disk and the created store (a tree `id` is also
/// created when `with_tree` is set).
fn create_store(cfg: StoreCfg, seed: u64, with_tree: Option<u64>) -> (SimDisk, StoreInner) {
    let host = TestWorld {
        sched: SchedCore::new(1, seed),
        disk: SimDisk::new(),
        st: StoreInner::empty(cfg.clone(), 0),
        flags: BTreeMap::new(),
    };
    let sched = Sched::new(host);
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    let st = ckpt::create(&cx, cfg, seed as u128).expect("create");
    cx.with("test:install", |c| {
        *c.store() = st;
        Ok(())
    })
    .expect("install");
    if let Some(id) = with_tree {
        tree::dir_create_tree(&cx, id).expect("CreateTree");
    }
    let g = sched.lock();
    (g.disk.clone(), g.st.clone())
}

/// A value for key `i` at version `v` (tag 0 + a recognizable body).
fn val(i: u64, v: u64) -> Vec<u8> {
    let mut b = vec![0u8];
    b.extend_from_slice(&i.to_le_bytes());
    b.push(v as u8);
    b
}

/// Insert `(key, ts)` into tree `id` under `root`.
fn ins(cx: &Cx<'_, TestWorld>, id: u64, root: u64, key: &[u8], ts: u64, value: &[u8]) {
    let ekey = tree::enc_ekey(key, ts);
    tree::tree_insert(cx, id, root, &ekey, value, false)
        .unwrap_or_else(|e| panic!("insert {key:?}@{ts}: {e:?}"));
}

/// Run one checkpoint as the harness task (test helper).
fn cx_checkpoint(sched: &Sched<TestWorld>, hooks: impl FnMut(CkptHook) -> Result<(), Fail>) {
    let cx = Cx {
        w: sched,
        t: HARNESS,
    };
    let mut h = hooks;
    ckpt::checkpoint(&cx, &mut h).expect("checkpoint");
}

/// 1: create, open, empty scan — and a clean amcheck of image 0.
#[test]
fn create_open_empty_scan() {
    for seed in 1u64..=3 {
        let cfg = tcfg(8, 8, 1);
        let (disk, st) = create_store(cfg.clone(), seed, None);
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        // A tree created post-open (idempotent twice).
        tree::dir_create_tree(&cx, 7).expect("CreateTree");
        tree::dir_create_tree(&cx, 7).expect("CreateTree (idempotent)");
        let root = tree::dir_root_of(&cx, 7).expect("root");
        let raw = tree::tree_raw(&cx, root).expect("raw scan");
        assert!(raw.is_empty(), "seed {seed}: fresh tree not empty");
        let scan = tree::tree_scan(&cx, root, 10).expect("scan");
        assert!(scan.is_empty(), "seed {seed}: fresh scan not empty");
        // amcheck over the created image 0 is clean.
        let findings = amcheck::amcheck(&cx, &cfg);
        assert!(
            findings.is_empty(),
            "seed {seed}: image 0 amcheck findings: {findings:?}"
        );
        // Checkpoint, then reopen from the durable bytes: tree 7 exists.
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        let (disk2, _st2) = (g.disk.clone(), g.st.clone());
        drop(g);
        let sched2 = pass_world(1, seed + 100, disk2, StoreInner::empty(cfg.clone(), 0));
        let cx2 = Cx {
            w: &sched2,
            t: HARNESS,
        };
        let (opened, meta2) = ckpt::open(&cx2, cfg.clone()).expect("open");
        assert_eq!(meta2.ckpt_id, 1, "seed {seed}: meta 1 is the opened image");
        cx2.with("test:install2", |c| {
            *c.store() = opened;
            Ok(())
        })
        .expect("install2");
        let root2 = tree::dir_root_of(&cx2, 7).expect("root after reopen");
        let raw2 = tree::tree_raw(&cx2, root2).expect("raw after reopen");
        assert!(raw2.is_empty(), "seed {seed}: reopened tree not empty");
        let findings2 = amcheck::amcheck(&cx2, &cfg);
        assert!(
            findings2.is_empty(),
            "seed {seed}: reopened amcheck findings: {findings2:?}"
        );
    }
}

/// 2: 10,000 inserts into one tree — leaf splits and a root split; the
/// scan equals a `BTreeMap`.
#[test]
fn ten_thousand_inserts() {
    for seed in 1u64..=3 {
        // Minimum F for one task and height ≤ 4: 1×(4+2) + R_EPOCH = 7.
        let cfg = tcfg(8, 7, 1);
        let (disk, st) = create_store(cfg.clone(), seed, Some(2));
        let body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            let root = tree::dir_root_of(&cx, 2).map_err(|_| SimFail::Crashed)?;
            for i in 0..10_000u64 {
                let key = i.to_be_bytes();
                ins(&cx, 2, root, &key, 1, &val(i, 1));
            }
            Ok(())
        };
        let (sched, report) = run_world(2, seed, disk, st, body);
        report.assert_clean("10k inserts");
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let root = tree::dir_root_of(&cx, 2).expect("root");
        // A root split happened: the stable root is an inner page.
        let (root_bytes, _) = prims::fetch_page(&cx, root, false).expect("fetch root");
        let kind = root_bytes.get(4).copied().unwrap_or(0);
        assert_eq!(
            kind,
            e4_recovery::store::page::KIND_INNER,
            "seed {seed}: the root did not split (still a leaf)"
        );
        // The scan equals the BTreeMap.
        let scan = tree::tree_scan(&cx, root, 10).expect("scan");
        let want: BTreeMap<Vec<u8>, Option<Vec<u8>>> = (0..10_000u64)
            .map(|i| (i.to_be_bytes().to_vec(), Some(val(i, 1)[1..].to_vec())))
            .collect();
        assert_eq!(
            scan.len(),
            want.len(),
            "seed {seed}: scan size {} != {}",
            scan.len(),
            want.len()
        );
        for (k, v) in &want {
            assert_eq!(
                scan.get(k),
                Some(v),
                "seed {seed}: key {k:?} mismatch after 10k inserts"
            );
        }
    }
}

/// 3: a GC sweep that produces merges and a root collapse; the scan still
/// equals the `BTreeMap` (live keys at their newest version, deleted keys
/// gone).
#[test]
fn gc_merges_and_root_collapse() {
    for seed in 1u64..=3 {
        let cfg = tcfg(8, 8, 1);
        let (disk, st) = create_store(cfg.clone(), seed, Some(3));
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let root = tree::dir_root_of(&cx, 3).expect("root");
        // 100 keys × 6 versions = 600 entries (~6 leaves at 4 KiB); then
        // delete keys 50..100 at ts 7.
        for v in 1..=6u64 {
            for i in 0..100u64 {
                ins(&cx, 3, root, &i.to_be_bytes(), v, &val(i, v));
            }
        }
        for i in 50..100u64 {
            let ekey = tree::enc_ekey(&i.to_be_bytes(), 7);
            tree::tree_insert(&cx, 3, root, &ekey, &[1u8], false)
                .unwrap_or_else(|e| panic!("delete {i}: {e:?}"));
        }
        // The root is an inner page before the sweep.
        let (rb, _) = prims::fetch_page(&cx, root, false).expect("root fetch");
        assert_eq!(
            rb.get(4).copied().unwrap_or(0),
            e4_recovery::store::page::KIND_INNER,
            "seed {seed}: the tree did not grow an inner root"
        );
        // The sweep: loop gc_step until done, at w = 7. A second sweep
        // elides the tombstones the first deferred across leaf
        // boundaries (their groups ended past the leaf, §10.2; the
        // merges of the first sweep brought those ends inside leaves).
        let mut steps = 0;
        for _sweep in 0..3 {
            let mut resume = Vec::new();
            loop {
                let next = tree::gc_step(&cx, 3, root, 7, &resume)
                    .unwrap_or_else(|e| panic!("gc step: {e:?}"));
                steps += 1;
                if next.is_empty() {
                    break;
                }
                resume = next;
                assert!(steps < 10_000, "seed {seed}: gc does not terminate");
            }
        }
        // Root collapse: the stable root is a leaf again.
        let (rb2, _) = prims::fetch_page(&cx, root, false).expect("root fetch 2");
        assert_eq!(
            rb2.get(4).copied().unwrap_or(0),
            e4_recovery::store::page::KIND_LEAF,
            "seed {seed}: the root did not collapse"
        );
        // The scan equals the model: keys 0..50 live at ts 6, keys
        // 50..100 absent.
        let scan = tree::tree_scan(&cx, root, 100).expect("scan");
        let want: BTreeMap<Vec<u8>, Option<Vec<u8>>> = (0..50u64)
            .map(|i| (i.to_be_bytes().to_vec(), Some(val(i, 6)[1..].to_vec())))
            .collect();
        assert_eq!(scan.len(), want.len(), "seed {seed}: post-GC scan size");
        for (k, v) in &want {
            assert_eq!(scan.get(k), Some(v), "seed {seed}: post-GC {k:?}");
        }
    }
}

/// 4: one checkpoint (steps 0–6) with a concurrent insert task and the
/// background evictor, at the minimum `F`; reopen; the scan is equal and
/// amcheck is clean. The insert task's phase 2 starts at the flip (the
/// `After0` hook sets the flag), so its entries are epoch k+1 mutations
/// that image k must not contain — the flip-window CoW discipline.
#[test]
fn checkpoint_with_concurrent_insert_and_evictor() {
    for seed in 1u64..=3 {
        let map_cap = 8u64;
        // Minimum F: 3 pool-touching tasks (insert, evictor, checkpointer)
        // × (max_height 3 + 2) + R_EPOCH 1 (Work 12: R_MAP = 0).
        let cfg = tcfg(map_cap, 3 * 5 + 1, 1);
        let (disk, st) = create_store(cfg.clone(), seed, Some(4));
        let body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            let root = tree::dir_root_of(&cx, 4).map_err(|_| SimFail::Crashed)?;
            match t {
                1 => {
                    // Phase 1: 600 keys at ts 1 (a multi-leaf tree).
                    for i in 0..600u64 {
                        ins(&cx, 4, root, &i.to_be_bytes(), 1, &val(i, 1));
                    }
                    set_flag(sched, "phase1");
                    // Phase 2 waits for the flip: these entries are epoch
                    // k+1 and must not be in image k.
                    while !flag(sched, "flipped") {
                        cx.yield_now("test:wait-flip").map_err(fail_of)?;
                    }
                    for i in 600..800u64 {
                        ins(&cx, 4, root, &i.to_be_bytes(), 1, &val(i, 1));
                    }
                    set_flag(sched, "phase2");
                    Ok(())
                }
                2 => {
                    // The background evictor.
                    while !flag(sched, "ckpt_done") {
                        if let Err(e) = prims::evict_one(&cx, false) {
                            return match e {
                                Fail::Sim(s) => Err(s),
                                _ => Ok(()),
                            };
                        }
                        cx.yield_now("test:evictor").map_err(fail_of)?;
                    }
                    Ok(())
                }
                3 => {
                    // The checkpointer: one checkpoint after phase 1.
                    while !flag(sched, "phase1") {
                        cx.yield_now("test:wait-phase1").map_err(fail_of)?;
                    }
                    let r = ckpt::checkpoint(&cx, &mut |hook| {
                        if hook == CkptHook::After0 {
                            set_flag(sched, "flipped");
                        }
                        Ok(())
                    });
                    set_flag(sched, "ckpt_done");
                    r.map_err(fail_of)?;
                    // Let the evictor see the done flag before it exits.
                    cx.yield_now("test:ckpt-end").map_err(fail_of)?;
                    Ok(())
                }
                _ => Ok(()),
            }
        };
        let (sched, report) = run_world(4, seed, disk, st, body);
        report.assert_clean("checkpoint round");
        // Take the durable disk out and reopen.
        let g = sched.lock();
        let disk_after = g.disk.clone();
        let no_served = g.disk.served.is_empty();
        let no_uncovered = g.disk.uncovered_accepts.is_empty();
        drop(g);
        assert!(
            no_served,
            "seed {seed}: served corruption in an uncrashed round"
        );
        assert!(
            no_uncovered,
            "seed {seed}: uncovered accepts in an uncrashed round"
        );
        let sched2 = pass_world(1, seed + 100, disk_after, StoreInner::empty(cfg.clone(), 0));
        let cx2 = Cx {
            w: &sched2,
            t: HARNESS,
        };
        let (opened, meta) = ckpt::open(&cx2, cfg.clone()).expect("reopen");
        assert_eq!(meta.ckpt_id, 1, "seed {seed}: meta 1 is the opened image");
        cx2.with("test:install3", |c| {
            *c.store() = opened;
            Ok(())
        })
        .expect("install3");
        let root2 = tree::dir_root_of(&cx2, 4).expect("root after reopen");
        let scan = tree::tree_scan(&cx2, root2, 10).expect("scan after reopen");
        // Image k holds exactly phase 1: keys 0..600.
        let want: BTreeMap<Vec<u8>, Option<Vec<u8>>> = (0..600u64)
            .map(|i| (i.to_be_bytes().to_vec(), Some(val(i, 1)[1..].to_vec())))
            .collect();
        assert_eq!(scan.len(), want.len(), "seed {seed}: reopened scan size");
        for (k, v) in &want {
            assert_eq!(scan.get(k), Some(v), "seed {seed}: reopened {k:?}");
        }
        let findings = amcheck::amcheck(&cx2, &cfg);
        assert!(
            findings.is_empty(),
            "seed {seed}: reopened amcheck findings: {findings:?}"
        );
    }
}

/// G7's meta-selection semantics (P's ruling on the round-3 F3,
/// 2026-10-26): the winner is the highest placed-valid meta; a
/// misplaced valid meta that does not outrank it is a loser (ignored);
/// one that does is `MisplacedNewer` (XX001 at open, with the
/// placed-valid winner as the fallback for the walks); the mutant's
/// `skip_slot_check` (the store's open only) opens the misplaced copy
/// from its own read slot.
#[test]
fn g7_meta_selection_semantics() {
    use e4_recovery::store::page::{self, Meta, MetaPick};
    let mk = |k: u64| {
        let m = Meta {
            ckpt_id: k,
            t: k,
            ..Meta::default()
        };
        page::build_meta(&m)
    };
    // Placed metas: slot 1 holds even ids, slot 2 odd.
    let (a, b) = (mk(2), mk(1));
    match page::select_meta(&a, &b, false) {
        MetaPick::Win { meta, slot } => {
            assert_eq!(meta.ckpt_id, 2, "the highest placed-valid meta wins");
            assert_eq!(slot, 1, "and it was read from its own slot");
        }
        other => panic!("placed metas must Win ({other:?})"),
    }
    // A misplaced **older** meta is a loser: meta 0's bytes in slot 2
    // (its own slot is 1) lose to the placed meta 2 in slot 1.
    match page::select_meta(&a, &mk(0), false) {
        MetaPick::Win { meta, slot } => {
            assert_eq!(meta.ckpt_id, 2, "the misplaced older copy is ignored");
            assert_eq!(slot, 1);
        }
        other => panic!("a misplaced older meta must lose ({other:?})"),
    }
    // Meta 2's bytes in slot 2: misplaced (their own slot is 1), and
    // they outrank the placed meta 0 in slot 1 — MisplacedNewer with the
    // fallback.
    match page::select_meta(&mk(0), &a, false) {
        MetaPick::MisplacedNewer {
            meta,
            slot,
            fallback,
        } => {
            assert_eq!(meta.ckpt_id, 2, "the misplaced newer meta is named");
            assert_eq!(slot, 2, "read from slot 2");
            assert_eq!(
                fallback.map(|m| m.ckpt_id),
                Some(0),
                "the placed-valid winner is the fallback"
            );
        }
        other => panic!("the misplaced newer meta must be MisplacedNewer ({other:?})"),
    }
    // META-WRONG-SLOT (the store's open only): the same bytes select the
    // misplaced copy — Win at its read slot (the accept names the bytes
    // actually used).
    match page::select_meta(&b, &a, true) {
        MetaPick::Win { meta, slot } => {
            assert_eq!(meta.ckpt_id, 2, "the mutant opens the misplaced copy");
            assert_eq!(slot, 2, "accepted from the slot it was read from");
        }
        other => panic!("skip_slot_check must Win the highest parse-ok meta ({other:?})"),
    }
    // No valid meta anywhere.
    assert!(matches!(
        page::select_meta(&[0u8; 4096], &[0u8; 4096], false),
        MetaPick::NoValid
    ));
    // A misplaced newer meta with no placed-valid one at all: still
    // MisplacedNewer (open fails XX001 either way; no fallback).
    match page::select_meta(&[0u8; 4096], &a, false) {
        MetaPick::MisplacedNewer { meta, fallback, .. } => {
            assert_eq!(meta.ckpt_id, 2);
            assert!(fallback.is_none());
        }
        other => panic!("({other:?})"),
    }
}

fn fail_of(f: Fail) -> SimFail {
    match f {
        Fail::Sim(s) => s,
        _ => SimFail::Crashed,
    }
}

/// 5 (Work 12's depth condition): `map_leaf_cap` drawn from {8, 16, 254}
/// gives the persisted radix ≥ 2 levels once the LPID space grows; a
/// checkpoint on a many-LPID store produces a multi-level map that
/// reopens cleanly.
#[test]
fn map_radix_depth_and_reopen() {
    for cap in [8u64, 16, 254] {
        let cfg = tcfg(cap, 16, 2);
        let (disk, st) = create_store(cfg.clone(), 77, Some(5));
        let sched = pass_world(1, 77, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let root = tree::dir_root_of(&cx, 5).expect("root");
        // Enough LPIDs to span ≥ 2 map leaves at this cap (leaves + splits
        // of a 900-entry tree), then a checkpoint and reopen.
        for i in 0..900u64 {
            ins(&cx, 5, root, &i.to_be_bytes(), 1, &val(i, 1));
        }
        ckpt::checkpoint(&cx, &mut |_| Ok(())).expect("checkpoint");
        let g = sched.lock();
        let disk2 = g.disk.clone();
        let st2 = g.st.clone();
        drop(g);
        let depth = tree::map_depth(&st2);
        if cap < 254 {
            assert!(
                depth >= 2,
                "cap {cap}: map depth {depth} (expected ≥ 2 levels)"
            );
        }
        let sched2 = pass_world(1, 78, disk2, StoreInner::empty(cfg.clone(), 0));
        let cx2 = Cx {
            w: &sched2,
            t: HARNESS,
        };
        let (opened, _meta) = ckpt::open(&cx2, cfg.clone()).expect("reopen");
        cx2.with("test:install4", |c| {
            *c.store() = opened;
            Ok(())
        })
        .expect("install4");
        let root2 = tree::dir_root_of(&cx2, 5).expect("root");
        let scan = tree::tree_scan(&cx2, root2, 10).expect("scan");
        assert_eq!(scan.len(), 900, "cap {cap}: reopened scan size");
        let findings = amcheck::amcheck(&cx2, &cfg);
        assert!(
            findings.is_empty(),
            "cap {cap}: reopened amcheck findings: {findings:?}"
        );
    }
}

/// Guard against accidental mutant configuration in the clean tests: the
/// enum exists and every variant is distinct (compile-time coverage of
/// the 28 sites' names for the directed tests — the 27 of the card's
/// mutation table plus G7's META-WRONG-SLOT, P's ruling on the round-3
/// F3).
#[test]
fn mutant_enum_is_complete() {
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
    let mut seen: Vec<Mutant> = Vec::new();
    for m in all {
        assert!(!seen.contains(&m), "duplicate mutant {m:?}");
        seen.push(m);
    }
    assert_eq!(seen.len(), 28, "the card's 27-mutant enum plus G7's");
}
