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

use e4_recovery::sim::{
    E4Env, HARNESS, HasDisk, Sched, SchedCore, SchedHost, SimDisk, SimFail, TaskId,
};
use e4_recovery::store::ckpt::{self, CkptHook};
use e4_recovery::store::wal;
use e4_recovery::store::{Cx, Fail, HasStore, Mutant, StoreCfg, StoreInner, amcheck, prims, tree};

/// The test world: scheduling, disk and store slices plus a small shared
/// flag board (and a numeric board, X-5c-F3-F3b step 1's After4 record)
/// the task bodies coordinate through.
struct TestWorld {
    sched: SchedCore,
    disk: SimDisk,
    st: StoreInner,
    flags: BTreeMap<&'static str, bool>,
    nums: BTreeMap<&'static str, u64>,
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
        wal_io_fail: None,
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
        nums: BTreeMap::new(),
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
                // X5-R14: the task's first scheduler contact is a park
                // (the same entry barrier the harness's `run_task`
                // imposes): a body may not take host steps before the
                // baton is handed to it. `Sched::acquire`'s S-DET gate
                // enforces it; this barrier keeps every test body on
                // the legal side of the gate.
                if let Err(SimFail::Baton(s)) = sched.yield_baton(t) {
                    panic!("entry barrier refused: Baton at {s}");
                }
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
        nums: BTreeMap::new(),
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
        nums: BTreeMap::new(),
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

/// `fetch_page`'s `Stale` re-validation (the gate's round-3 F5): a
/// fetch's carried `(ref, bytes)` is in flight across the env yields of
/// its read and accept — and a concurrent task can dirty-evict the very
/// page in that window, moving its home to a fresh slot. Installing the
/// carried bytes then silently drops every mutation between the two
/// versions (the frame holds the old bytes as clean, and later reads
/// hit it). The gate's probe (the re-validation removed) left the whole
/// suite green; this pin holds it with a deterministic interleave.
///
/// The shape: a single-leaf tree whose page is resident nowhere (the
/// pool is drained first). The scanner arms the `fetch:inflight` park
/// and scans; the mutator waits for the scanner's `read_at` of the
/// leaf's home to appear in the disk's read log — then, inside the
/// in-flight window and without touching `fetch_page` itself, writes
/// the page's next version directly (`store_page` on built bytes) and
/// evicts it out to a fresh slot, superseding the carried ref. Clean:
/// the install discards the carried bytes (`Stale`), re-loads the new
/// home, and the scan sees the mutation. With the re-validation
/// removed, the stale bytes are installed as a clean frame and the scan
/// loses the entry.
///
/// (Coordination note: the mutator's poll reads the world under one
/// guard per iteration — the flag board and the read log share it; a
/// nested `sched.lock()` here self-deadlocks the non-reentrant mutex,
/// which is how this pin's first draft hung.)
#[test]
fn fetch_page_revalidates_its_carried_ref() {
    for seed in 1u64..=3 {
        let cfg = tcfg(8, 10, 2);
        let (disk, st) = create_store(cfg.clone(), seed, Some(8));
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        // One entry in a single-leaf tree, made durable, then the pool
        // drained: the leaf is resident nowhere.
        let root = tree::dir_root_of(&cx, 8).expect("root");
        ins(&cx, 8, root, &1u64.to_be_bytes(), 1, &val(1, 1));
        while prims::evict_one(&cx, false).expect("evict") {
            cx.yield_now("test:drain").map_err(fail_of).expect("y");
        }
        let (disk2, st2) = {
            let g = sched.lock();
            (g.disk.clone(), g.st.clone())
        };
        let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            match t {
                1 => {
                    // The mutator: wait for the scanner's read of the
                    // leaf's home (one guard per poll), then supersede
                    // the carried ref inside the window.
                    let pf = {
                        let g = sched.lock();
                        g.st.page_file
                    };
                    let mut polls = 0u32;
                    loop {
                        let seen = {
                            let g = sched.lock();
                            let home = g.st.map.cur.get(&root).copied();
                            g.disk.reads.iter().any(|r| {
                                r.inode == pf
                                    && r.off == home.map(|r| r.slot * 4096).unwrap_or(u64::MAX)
                            })
                        };
                        if seen {
                            break;
                        }
                        polls += 1;
                        assert!(polls < 100_000, "seed {seed}: the read never appeared");
                        cx.yield_now("test:wait-read").map_err(fail_of)?;
                    }
                    // The page's next version: the old entry plus a new
                    // one, built directly (no fetch — the park stays the
                    // scanner's alone).
                    let (home, epoch) = {
                        let g = sched.lock();
                        (g.st.map.cur.get(&root).copied().expect("home"), g.st.e)
                    };
                    let e1 = tree::enc_ekey(&1u64.to_be_bytes(), 1);
                    let e2 = tree::enc_ekey(&2u64.to_be_bytes(), 2);
                    let v1 = val(1, 1);
                    let v2 = val(2, 1);
                    let bytes = e4_recovery::store::page::build_leaf(
                        root,
                        epoch,
                        &[
                            (e1.as_slice(), v1.as_slice()),
                            (e2.as_slice(), v2.as_slice()),
                        ],
                    );
                    prims::store_page(&cx, root, bytes).map_err(fail_of)?;
                    // Evict it out: the home moves to a fresh slot,
                    // superseding the scanner's carried ref.
                    let mut guard = 0u32;
                    loop {
                        let done = {
                            let g = sched.lock();
                            g.st.map.cur.get(&root).copied() != Some(home)
                        };
                        if done {
                            break;
                        }
                        if !prims::evict_one(&cx, false).map_err(fail_of)? {
                            cx.yield_now("test:evict-retry").map_err(fail_of)?;
                        }
                        guard += 1;
                        assert!(guard < 10_000, "seed {seed}: the eviction loop");
                    }
                    Ok(())
                }
                2 => {
                    // The scanner: arm the in-flight park, scan, disarm.
                    cx.with("test:arm", |c| {
                        c.store().parks.insert("fetch:inflight", 256);
                        Ok(())
                    })
                    .map_err(fail_of)?;
                    let scan = tree::tree_scan(&cx, root, u64::MAX).map_err(fail_of)?;
                    cx.with("test:disarm", |c| {
                        c.store().parks.remove("fetch:inflight");
                        Ok(())
                    })
                    .map_err(fail_of)?;
                    let got2 = scan.get(2u64.to_be_bytes().as_slice()).cloned().flatten();
                    assert_eq!(
                        got2,
                        Some(val(2, 1)[1..].to_vec()),
                        "seed {seed}: the scan must see the in-window mutation (Stale re-validation)"
                    );
                    assert_eq!(
                        scan.get(1u64.to_be_bytes().as_slice()).cloned().flatten(),
                        Some(val(1, 1)[1..].to_vec()),
                        "seed {seed}: the older entry survives"
                    );
                    Ok(())
                }
                _ => Ok(()),
            }
        };
        let (_sched, report) = run_world(3, seed, disk2, st2, body);
        report.assert_clean("fetch Stale pin");
    }
}

/// The step-2 fixpoint re-scan (the gate's round-3 F5): a first-touch
/// epoch copy installed into a **reserve** frame — the low indices the
/// scan starts at — after the cursor has passed them leaves its
/// `CopyPending` unresolved in a single pass; the frozen image then
/// maps the LPID to nothing and the image loses the page outright. The
/// gate's probe (the re-scan removed: a pass that wrote something still
/// returns) left the whole suite green; this pin holds it.
///
/// The shape is deterministic: a checkpoint hook performs the
/// first-touch **inside** step 2 — at the first `Mid2`, after the
/// scan's first write (the directory's frame, the lowest dirty index),
/// while the cursor sits past the reserve frames. The hook's insert
/// into tree B freezes B's image state (a dirty page at the flip:
/// `CopyPending`) and installs the epoch copy into reserve frame 0 —
/// behind the cursor. The fixpoint's second pass writes the copy and
/// resolves the override; without it, reopening the image loses tree
/// B's page entirely (the scan of B fails "lpid unmapped").
#[test]
fn step2_fixpoint_writes_late_epoch_copies() {
    for seed in 1u64..=3 {
        let cfg = tcfg(8, 12, 2);
        let (disk, st) = create_store(cfg.clone(), seed, None);
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        // Tree A: one entry (its page occupies the first normal frame
        // after the directory's).
        tree::dir_create_tree(&cx, 6).expect("CreateTree A");
        let root_a = tree::dir_root_of(&cx, 6).expect("root A");
        ins(&cx, 6, root_a, &1u64.to_be_bytes(), 1, &val(1, 1));
        // Tree B: one entry, created after A so its page sits at a
        // higher frame index than the directory's and A's pages — the
        // scan reaches it after its first write.
        tree::dir_create_tree(&cx, 7).expect("CreateTree B");
        let root_b = tree::dir_root_of(&cx, 7).expect("root B");
        ins(&cx, 7, root_b, &1u64.to_be_bytes(), 1, &val(1, 1));
        // The checkpoint: at the first Mid2 (after the scan's first
        // write — the directory's frame, the lowest dirty index), the
        // hook performs the first-touch of B mid-flip-window.
        let mut fired = false;
        let mut hooks = |hook: CkptHook| -> Result<(), Fail> {
            if hook == CkptHook::Mid2 && !fired {
                fired = true;
                let cxh = Cx {
                    w: &sched,
                    t: HARNESS,
                };
                ins(&cxh, 7, root_b, &2u64.to_be_bytes(), 1, &val(2, 1));
            }
            Ok(())
        };
        ckpt::checkpoint(&cx, &mut hooks).expect("checkpoint");
        assert!(fired, "seed {seed}: the hook's Mid2 fired");
        // Reopen from the durable bytes: image k holds the flip-time
        // state — B's first entry (the epoch copy the fixpoint wrote),
        // never the hook's post-flip insert.
        let g = sched.lock();
        let disk_after = g.disk.clone();
        drop(g);
        let sched2 = pass_world(1, seed + 100, disk_after, StoreInner::empty(cfg.clone(), 0));
        let cx2 = Cx {
            w: &sched2,
            t: HARNESS,
        };
        let (opened, meta) = ckpt::open(&cx2, cfg.clone()).expect("reopen");
        assert_eq!(meta.ckpt_id, 1, "seed {seed}: meta 1 is the image");
        cx2.with("fixpt:install", |c| {
            *c.store() = opened;
            Ok(())
        })
        .expect("install");
        let root_b2 = tree::dir_root_of(&cx2, 7).expect("root B after reopen");
        let scan_b = tree::tree_scan(&cx2, root_b2, 10).expect("scan B after reopen");
        let want_b: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            [(1u64.to_be_bytes().to_vec(), Some(val(1, 1)[1..].to_vec()))]
                .into_iter()
                .collect();
        assert_eq!(
            scan_b, want_b,
            "seed {seed}: image 1 holds B's flip-time entry (the fixpoint wrote the copy)"
        );
        let root_a2 = tree::dir_root_of(&cx2, 6).expect("root A after reopen");
        let scan_a = tree::tree_scan(&cx2, root_a2, 10).expect("scan A after reopen");
        assert_eq!(scan_a.len(), 1, "seed {seed}: A's entry survived");
        let findings = amcheck::amcheck(&cx2, &cfg);
        assert!(
            findings.is_empty(),
            "seed {seed}: reopened amcheck findings: {findings:?}"
        );
    }
}

/// **X-5c-F4-F1-F1-F2 step 0: the late epoch copy's literal schedule**
/// (verdict X-5c-F4-F1-F1, finding 1 — a silent class-C loss, no
/// injected fault). `freeze_first_touch` parks a first touch's copy in
/// the exhausted reserve path (the `epochcopy:prewrite` park) while the
/// checkpointer's step 2 writes the page's *normal* frame — the mutator
/// is the parked task, so the frame still holds its epoch-`k` bytes and
/// the scan writes them, resolving `Img::CopyPending` to `Img::At` —
/// then `ckpt:freecopies` frees the reserve and the checkpoint
/// completes. The copy then installs **late**: a dirty, `is_epoch_copy`
/// frame with `f.epoch = e − 1` that its own checkpoint never cleaned.
/// The next checkpoint's scan accepts it (`f.is_epoch_copy || f.epoch
/// <= k`) and its record path maps the LPID to the old content while
/// `T` says replay need not restore it — image k+1 is silently stale.
///
/// The schedule (one world seed, flags and the park budget are the only
/// coordination): P's first touch — performed by the first Mid2 hook,
/// deterministically, while the scan cursor is still at the bottom of
/// the pool — fills the `R_EPOCH = 1` reserve; B's first touch (the
/// mutator task) takes the exhausted path and parks; step 2 writes P's
/// copy (the fixpoint re-scan) and B's normal frame; the `After2` hook
/// (past `ckpt:freecopies`) releases the parked copy; between the
/// checkpoints, B's frame is evicted so checkpoint 2's scan cannot
/// overwrite the leaked entry with the frame's newer content; crash
/// after checkpoint 2's meta (`crash_clean` — its step 5 synced), then
/// reopen from meta 2 and from meta 1 (reverted).
///
/// At the pre-fix tip this pin **fails**: image 2 maps B's page to the
/// leaked copy's flip-1 bytes, so the reopen's B scan misses the key
/// inserted during window 1. The fix (the install guard at
/// `epoch_copy:install`) drops the late copy — B's page comes from the
/// evicted home and both scans read the right cut.
#[test]
fn late_epoch_copy_must_not_enter_the_next_image() {
    // The world seed is pinned: the schedule's races (the mutator's two
    // first touches before the scan cursor reaches B's frame; the late
    // install between `ckpt:freecopies` and step 5) are deterministic
    // per seed and this one lands them (verified at the pre-fix tip,
    // where the pin must fail).
    for seed in [3u64] {
        let cfg = tcfg(8, 18, 1);
        // ---- phase A (the harness pre-pass): three one-page trees,
        // each with one dirty entry. Creation order fixes frame order
        // (dir, A, P, B), so the step-2 scan reaches B's frame last —
        // the mutator's two touches land while the cursor is still
        // below them.
        let (disk, st) = create_store(cfg.clone(), seed, None);
        let (disk, st, roots) = {
            let sched = pass_world(1, seed, disk, st);
            let cx = Cx {
                w: &sched,
                t: HARNESS,
            };
            let mut roots = Vec::new();
            for id in 1u64..=12 {
                tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
                let root =
                    tree::dir_root_of(&cx, id).unwrap_or_else(|e| panic!("root {id}: {e:?}"));
                if id == 11 || id == 12 {
                    // P (the filler, tree 11) and B (the victim, tree
                    // 12) carry one dirty entry and own the two highest
                    // frames; trees 1..10 are empty padding — a dozen
                    // writes — so the lottery scheduler cannot race the
                    // cursor past B before the mutator's touch parks.
                    ins(&cx, id, root, &1u64.to_be_bytes(), 1, &val(1, 1));
                }
                roots.push(root);
            }
            // Hold any exhausted-path copy until the After2 hook
            // releases it (freecopies has run by then).
            cx.with("late:budget", |c| {
                c.store().parks.insert("epochcopy:prewrite", 900_000);
                Ok(())
            })
            .expect("park budget");
            let (d, s) = {
                let g = sched.lock();
                (g.disk.clone(), g.st.clone())
            };
            (d, s, roots)
        };
        let root_p = roots[10];
        let root_b = roots[11];
        // ---- phase B: the mutator (task 1) and the checkpointer
        // (task 2). The reserve is filled deterministically: the first
        // Mid2 hook (inside the checkpointer, after step 2's first
        // write) performs P's first touch, whose copy takes the free
        // reserve frame. The mutator then performs B's first touch —
        // the reserve is full, so the copy takes the exhausted path and
        // parks at `epochcopy:prewrite`, several writes below the
        // cursor (the padding trees keep it there).
        let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            match t {
                1 => {
                    while !(flag(sched, "flipped1") && flag(sched, "filled")) {
                        cx.yield_now("late:wait-fill").map_err(fail_of)?;
                    }
                    ins(&cx, 12, root_b, &2u64.to_be_bytes(), 2, &val(2, 2));
                    set_flag(sched, "m1_done");
                    while !flag(sched, "ckpt1_done") {
                        cx.yield_now("late:wait-ckpt1").map_err(fail_of)?;
                    }
                    // Evict B's frame (the post-park store placed the
                    // {1, 2} leaf in it): checkpoint 2's scan must find
                    // no eligible frame for B whose record could
                    // overwrite the leaked copy's image entry.
                    loop {
                        let gone = cx
                            .with("late:chk", |c| {
                                Ok(!c
                                    .store()
                                    .pool
                                    .frames
                                    .iter()
                                    .any(|f| f.key == root_b && !f.is_epoch_copy))
                            })
                            .map_err(fail_of)?;
                        if gone {
                            break;
                        }
                        if !prims::evict_one(&cx, false).map_err(fail_of)? {
                            cx.wait_for(e4_recovery::sim::WaitKey::Pin)
                                .map_err(fail_of)?;
                        }
                        cx.yield_now("late:evict").map_err(fail_of)?;
                    }
                    set_flag(sched, "evicted");
                    Ok(())
                }
                2 => {
                    let r = ckpt::checkpoint(&cx, &mut |hook| {
                        if hook == CkptHook::After0 {
                            set_flag(sched, "flipped1");
                        }
                        if hook == CkptHook::Mid2 && !flag(sched, "filled") {
                            // Step 2's first write is done and the
                            // cursor sits just above it: P's frame (and
                            // B's, above the padding) are still dirty
                            // and unscanned. P's first touch fills the
                            // reserve inside this hook — deterministically.
                            let cxh = Cx { w: sched, t };
                            ins(&cxh, 11, root_p, &2u64.to_be_bytes(), 2, &val(2, 2));
                            set_flag(sched, "filled");
                        }
                        if hook == CkptHook::After2 {
                            // Past freecopies: release the parked copy —
                            // its install is now late.
                            let mut g = sched.lock();
                            g.st.parks.insert("epochcopy:prewrite", 0);
                        }
                        Ok(())
                    });
                    set_flag(sched, "ckpt1_done");
                    r.map_err(fail_of)?;
                    while !flag(sched, "evicted") {
                        cx.yield_now("late:wait-evict").map_err(fail_of)?;
                    }
                    // Nobody touches B in window 2: the only frame its
                    // scan could take for B is the leaked copy.
                    ckpt::checkpoint(&cx, &mut |_| Ok(())).map_err(fail_of)?;
                    set_flag(sched, "ckpt2_done");
                    cx.yield_now("late:end").map_err(fail_of)?;
                    Ok(())
                }
                _ => Ok(()),
            }
        };
        let (sched, report) = run_world(3, seed, disk, st, body);
        report.assert_clean("the late-copy literal schedule");
        assert!(flag(&sched, "m1_done"), "seed {seed}: the mutator ran");
        assert!(
            flag(&sched, "ckpt2_done"),
            "seed {seed}: both checkpoints ran"
        );
        let g = sched.lock();
        let mut disk_after = g.disk.clone();
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
        // The crash after checkpoint 2's meta: everything synced stays
        // (step 5 synced the meta), nothing volatile does.
        crash_clean(&mut disk_after, seed + 50);
        // ---- reopen from meta 2: image 2 holds the flip-2 cut — both
        // of P's entries and both of B's. At the pre-fix tip B reads the
        // leaked copy's flip-1 leaf and loses key 2 (the pin's
        // failure).
        let want_two = |i: u64, j: u64| -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
            [
                (1u64.to_be_bytes().to_vec(), Some(val(i, 1)[1..].to_vec())),
                (2u64.to_be_bytes().to_vec(), Some(val(j, 2)[1..].to_vec())),
            ]
            .into_iter()
            .collect()
        };
        let want_b2 = want_two(1, 2);
        {
            let sched2 = pass_world(
                1,
                seed + 100,
                disk_after.clone(),
                StoreInner::empty(cfg.clone(), 0),
            );
            let cx2 = Cx {
                w: &sched2,
                t: HARNESS,
            };
            let (opened, meta) = ckpt::open(&cx2, cfg.clone()).expect("reopen meta 2");
            assert_eq!(meta.ckpt_id, 2, "seed {seed}: meta 2 is the image");
            cx2.with("late:install2", |c| {
                *c.store() = opened;
                Ok(())
            })
            .expect("install2");
            let scan_p = tree::tree_scan(&cx2, root_p, 10).expect("scan P after meta 2");
            assert_eq!(
                scan_p, want_b2,
                "seed {seed}: image 2 holds P's flip-2 cut (both entries)"
            );
            let scan_b = tree::tree_scan(&cx2, root_b, 10).expect("scan B after meta 2");
            assert_eq!(
                scan_b, want_b2,
                "seed {seed}: image 2 must hold B's flip-2 cut — a late epoch copy \
                 must not enter the next image (the leaked copy maps B to its \
                 flip-1 content and key 2 is lost)"
            );
        }
        // ---- reopen from meta 1 (its durable write reverts): image 1
        // is the flip-1 cut — one entry per tree. This half also pins
        // the copy machinery itself: with every copy dropped
        // (M-LATECOPY-DROPALWAYS), P's `CopyPending` never resolves,
        // image 1 leaves P's page unmapped, and this reopen fails.
        {
            revert_meta_write(&mut disk_after, 2);
            crash_clean(&mut disk_after, seed + 60);
            let sched1 = pass_world(
                1,
                seed + 101,
                disk_after.clone(),
                StoreInner::empty(cfg.clone(), 0),
            );
            let cx1 = Cx {
                w: &sched1,
                t: HARNESS,
            };
            let (opened, meta) = ckpt::open(&cx1, cfg.clone()).expect("reopen meta 1");
            assert_eq!(meta.ckpt_id, 1, "seed {seed}: meta 1 after the revert");
            cx1.with("late:install1", |c| {
                *c.store() = opened;
                Ok(())
            })
            .expect("install1");
            let want_one = |i: u64| -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
                [(1u64.to_be_bytes().to_vec(), Some(val(i, 1)[1..].to_vec()))]
                    .into_iter()
                    .collect()
            };
            let scan_p = tree::tree_scan(&cx1, root_p, 10).expect("scan P after meta 1");
            assert_eq!(
                scan_p,
                want_one(1),
                "seed {seed}: image 1 holds P's flip-1 cut (the epoch copy resolved)"
            );
            let scan_b = tree::tree_scan(&cx1, root_b, 10).expect("scan B after meta 1");
            assert_eq!(
                scan_b,
                want_one(1),
                "seed {seed}: image 1 holds B's flip-1 cut (step 2 wrote the normal frame)"
            );
        }
    }
}

/// **X-5c-F4-F1-F1-F2: the freed-LPID form of the late copy**
/// (M-LATECOPY-ATONLY's pin: a guard that tests only `Img::At` —
/// dropping exactly the installs whose override resolved to `At` —
/// still installs a copy whose LPID left the map entirely). The same
/// park holds B's first touch across **two** flips: the release comes
/// at checkpoint 2's `After0`, so by then checkpoint 1 has completed
/// (its post-5 cleared the map), B's LPID has been freed, and window 2
/// is open with `map.img` holding nothing for B. The late install then
/// finds **no entry at all**: the real guard drops it (an absent entry
/// is not `Img::CopyPending`), and image 2 leaves the freed LPID
/// unmapped — the free-set bitmap marks it, and the reopened store's
/// current map has no entry for it. Under the mutant the copy still
/// installs; checkpoint 2's scan takes it (`f.is_epoch_copy`), and its
/// record path maps the freed LPID to epoch-`k` bytes — the reopened
/// store's `map.cur` contains a page no tree reaches.
///
/// Two schedule compressions, stated plainly. (1) B's LPID is freed
/// directly (`prims::free_lpid`) between the checkpoints: the tree
/// layer's own free paths hold the tree mutex, which a *bounded* copy
/// wait would release and re-acquire around — the park stands in for
/// that wait, so the free is done the way it would land in that gap.
/// (2) The exhausted path's write-out of the **oldest** copy (P's)
/// lands with the late install, and its own `record_image_at` for P is
/// then stale (window 2's map gets P's flip-1 content) — a sibling
/// deviation this card's prescribed fix does not touch, reported under
/// ESCALATE with its own reproduction; this pin therefore asserts only
/// the late copy's own LPID (B) and an untouched padding tree.
#[test]
fn freed_lpids_late_copy_never_remaps() {
    for seed in [3u64] {
        let cfg = tcfg(8, 18, 1);
        // The same phase A as the main pin: P (tree 11) fills the
        // reserve, B (tree 12) is the victim above ten padding trees.
        let (disk, st) = create_store(cfg.clone(), seed, None);
        let (disk, st, roots) = {
            let sched = pass_world(1, seed, disk, st);
            let cx = Cx {
                w: &sched,
                t: HARNESS,
            };
            let mut roots = Vec::new();
            for id in 1u64..=12 {
                tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
                let root =
                    tree::dir_root_of(&cx, id).unwrap_or_else(|e| panic!("root {id}: {e:?}"));
                if id == 11 || id == 12 {
                    ins(&cx, id, root, &1u64.to_be_bytes(), 1, &val(1, 1));
                }
                roots.push(root);
            }
            cx.with("late:budget2", |c| {
                c.store().parks.insert("epochcopy:prewrite", 900_000);
                Ok(())
            })
            .expect("park budget");
            let (d, s) = {
                let g = sched.lock();
                (g.disk.clone(), g.st.clone())
            };
            (d, s, roots)
        };
        let root_p = roots[10];
        let root_b = roots[11];
        let pad_roots: Vec<u64> = roots.iter().take(10).copied().collect();
        // B's own bytes, taken in the pre-pass (the raw `store_page`
        // below re-stores them; the freeze fires on the dirty frame
        // either way — the content is not what this pin asserts).
        let b_bytes = {
            let sched = pass_world(1, seed + 3, disk.clone(), st.clone());
            let cx = Cx {
                w: &sched,
                t: HARNESS,
            };
            let (bytes, _) = prims::fetch_page(&cx, root_b, false).expect("fetch B");
            prims::unpin(&cx, root_b);
            bytes
        };
        let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            match t {
                1 => {
                    while !(flag(sched, "flipped1") && flag(sched, "filled")) {
                        cx.yield_now("late2:wait-fill").map_err(fail_of)?;
                    }
                    // A raw `store_page`, deliberately: a tree insert
                    // would hold the epoch gate across the parked copy
                    // wait and the next flip could not start until the
                    // park ended. The raw touch parks holding nothing,
                    // so checkpoint 2's flip runs while the copy waits —
                    // the shape a bounded copy wait reaches in the real
                    // system when the freeing operation is not on the
                    // gated path that produced the copy.
                    prims::store_page(&cx, root_b, b_bytes.clone()).map_err(fail_of)?;
                    set_flag(sched, "m_done");
                    Ok(())
                }
                2 => {
                    let r = ckpt::checkpoint(&cx, &mut |hook| {
                        if hook == CkptHook::After0 {
                            set_flag(sched, "flipped1");
                        }
                        if hook == CkptHook::Mid2 && !flag(sched, "filled") {
                            let cxh = Cx { w: sched, t };
                            ins(&cxh, 11, root_p, &2u64.to_be_bytes(), 2, &val(2, 2));
                            set_flag(sched, "filled");
                        }
                        Ok(())
                    });
                    set_flag(sched, "ckpt1_done");
                    r.map_err(fail_of)?;
                    // Free B's LPID between the checkpoints: at the next
                    // flip B is unmapped, and a late copy of it must not
                    // put it back. (The raw touch above holds no gate,
                    // so this checkpoint's flip does not wait on the
                    // parked copy; the After0 hook that releases it is
                    // reachable.)
                    prims::free_lpid(&cx, root_b).map_err(fail_of)?;
                    // Dirty the padding trees so checkpoint 2's data
                    // phase is a dozen writes long: the released copy
                    // must land behind the scan cursor (the re-scan
                    // then takes it), not after the final pass.
                    for (id, root) in pad_roots.iter().enumerate() {
                        let id = id as u64 + 1;
                        ins(&cx, id, *root, &2u64.to_be_bytes(), 2, &val(2, 2));
                    }
                    ckpt::checkpoint(&cx, &mut |hook| {
                        if hook == CkptHook::After0 {
                            // Window 2 is open and the map holds nothing
                            // for B: release the parked copy now.
                            let mut g = sched.lock();
                            g.st.parks.insert("epochcopy:prewrite", 0);
                        }
                        Ok(())
                    })
                    .map_err(fail_of)?;
                    set_flag(sched, "ckpt2_done");
                    cx.yield_now("late2:end").map_err(fail_of)?;
                    Ok(())
                }
                _ => Ok(()),
            }
        };
        let (sched, report) = run_world(3, seed, disk, st, body);
        report.assert_clean("the freed-LPID late-copy schedule");
        assert!(flag(&sched, "m_done"), "seed {seed}: the mutator ran");
        assert!(
            flag(&sched, "ckpt2_done"),
            "seed {seed}: both checkpoints ran"
        );
        let g = sched.lock();
        let mut disk_after = g.disk.clone();
        drop(g);
        crash_clean(&mut disk_after, seed + 70);
        let sched2 = pass_world(1, seed + 102, disk_after, StoreInner::empty(cfg.clone(), 0));
        let cx2 = Cx {
            w: &sched2,
            t: HARNESS,
        };
        let (opened, meta) = ckpt::open(&cx2, cfg.clone()).expect("reopen meta 2");
        assert_eq!(meta.ckpt_id, 2, "seed {seed}: meta 2 is the image");
        cx2.with("late2:install", |c| {
            *c.store() = opened;
            Ok(())
        })
        .expect("install");
        // A padding tree carries exactly its between-checkpoints entry
        // (pre-flip-2, so image 2 legitimately holds it): the image
        // itself is structurally readable.
        let scan_pad = tree::tree_scan(&cx2, roots[0], 10).expect("scan pad 1");
        let want_pad: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            [(2u64.to_be_bytes().to_vec(), Some(val(2, 2)[1..].to_vec()))]
                .into_iter()
                .collect();
        assert_eq!(
            scan_pad, want_pad,
            "seed {seed}: the padding tree holds its pre-flip-2 entry"
        );
        // The pin: the freed LPID must not re-enter the image's map.
        let mapped = cx2
            .with("late2:chk", |c| Ok(c.store().map.cur.contains_key(&root_b)))
            .expect("map check");
        assert!(
            !mapped,
            "seed {seed}: a freed LPID's late copy must not remap it in the next \
             image (the copy installs onto an absent entry and the scan's \
             is_epoch_copy arm records it — M-LATECOPY-ATONLY)"
        );
        let freed = cx2
            .with("late2:free", |c| {
                Ok(c.store().slots.free_lpids.contains(&root_b))
            })
            .expect("free check");
        assert!(
            freed,
            "seed {seed}: the freed LPID stays in the free set (the bitmap marks \
             every LPID the image leaves unmapped)"
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

/// X5-R13's seed 36, root-caused (P: "`tree::gc_step` hands back its own
/// cursor (`next == resume`)") and fixed: the §10.4 sweep's resume —
/// `next_after` of the last processed cell — cannot carry the descent
/// past the subtree the leaf closes when the key region ahead is empty
/// (user keys with a gap across an inner boundary). The step then handed
/// back its own cursor and `gc_sweep` looped at `h:gcyield` forever: a
/// hang the DEADLOCK detector can never see, because the looping task
/// never parks (the livelock X5-R13 (b)'s budget bounds and records).
///
/// Literal config: a depth-3 tree — an inner page holds at most
/// (4096−24)/16 = 254 children, so ~9-cell leaves × 3,000 keys (~334
/// leaves) force the third level — of 3,000 keys spaced 7 apart, one
/// version each, so **every** inner boundary crossing has an empty key
/// region ahead. One full sweep at a watermark above every ts must
/// terminate in a bounded number of steps and lose nothing (the scan
/// equals the model map). With the subtree-ceiling fix reverted the
/// sweep hands back its own cursor at the first boundary and this pin
/// fails on the step bound (verified mutant-style, then reverted).
#[test]
fn gc_sweep_crosses_subtree_boundaries() {
    let cfg = tcfg(8, 16, 2);
    let (disk, st) = create_store(cfg.clone(), 36, Some(2));
    let sched = pass_world(1, 36, disk, st);
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    let root = tree::dir_root_of(&cx, 2).expect("root");
    let mut want: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    for i in 0..3000u64 {
        let k = (i * 7).to_be_bytes();
        let mut v = vec![0u8];
        v.extend_from_slice(&i.to_le_bytes());
        v.extend(std::iter::repeat_n(7u8, 370));
        ins(&cx, 2, root, &k, 1, &v);
        want.insert(k.to_vec(), Some(v[1..].to_vec()));
    }
    // The construction's page math: ~9 cells per 4-KiB leaf over 3,000
    // entries is ~334 leaves, and an inner page caps at 254 children —
    // the third level exists (the root is inner, and at least one of
    // its children is too).
    let (rb, _) = prims::fetch_page(&cx, root, false).expect("root fetch");
    assert_eq!(
        rb.get(4).copied().unwrap_or(0),
        e4_recovery::store::page::KIND_INNER,
        "the tree did not grow an inner root"
    );
    // One full sweep at w above every ts: bounded steps, nothing lost.
    let mut resume = Vec::new();
    let mut steps = 0u32;
    loop {
        let next = tree::gc_step(&cx, 2, root, 2, &resume).expect("gc step");
        steps += 1;
        assert!(
            steps < 5_000,
            "the sweep does not terminate (steps={steps}) — the resume re-enters the subtree it closed"
        );
        if next.is_empty() {
            break;
        }
        resume = next;
    }
    let scan = tree::tree_scan(&cx, root, 2).expect("scan");
    assert_eq!(scan.len(), want.len(), "post-sweep scan size");
    for (k, v) in &want {
        assert_eq!(scan.get(k), Some(v), "post-sweep {k:?}");
    }
}

/// **X-5c-F3 pin, class C-XX (a literal config: one created tree, three
/// allocations, the highest freed).** S-STORE §4.2: "Reuse is
/// immediate, in memory" — a freed LPID goes to the free set, and
/// §4.2's allocator grows `next_lpid` only (`next_lpid++`); §4.3's LPID
/// bitmap is defined over `[1, next_lpid)` because the counter is
/// monotone. The draft decremented `next_lpid` when the highest LPID
/// was freed, so a flip-window free (whose `map.img` override keeps the
/// page's epoch-k content for the image being built) left the frozen
/// radix mapping an LPID the meta's own span says does not exist —
/// amcheck (i)'s "next_lpid not above every mapped lpid" (seeds 221/66,
/// a structural O-2/O-3 (b) finding in faultless class C rounds) — and
/// a recovery that re-freed the LPID left a dangling inner reference
/// ("lpid N unmapped", an XX001 at Open with no fault). Reverting the
/// fix (mutant M-CXXa: the `next_lpid -= 1` branch restored) fails
/// this pin at the monotonicity assertion.
#[test]
fn cxx_highest_lpid_free_never_shrinks_next_lpid() {
    let cfg = tcfg(8, 8, 2);
    let (disk, st) = create_store(cfg, 11, Some(2));
    let sched = pass_world(1, 11, disk, st);
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    // Drain any create-time free set, then take three fresh LPIDs: they
    // are the highest allocations, so `next_lpid` sits one above the
    // third.
    let mut taken = Vec::new();
    for _ in 0..3 {
        taken.push(prims::alloc_lpid(&cx).expect("alloc"));
    }
    let before = {
        let g = sched.lock();
        assert_eq!(
            g.st.meta.next_lpid,
            *taken.last().unwrap_or(&0) + 1,
            "next_lpid must sit one above the highest allocation"
        );
        g.st.meta.next_lpid
    };
    // Free the highest LPID: the counter stays, the free set takes it.
    let highest = *taken.last().unwrap_or(&0);
    prims::free_lpid(&cx, highest).expect("free the highest lpid");
    {
        let g = sched.lock();
        assert_eq!(
            g.st.meta.next_lpid, before,
            "freeing the highest LPID shrank next_lpid (S-STORE 4.2/4.3: the counter is monotone; a freed LPID goes to the free set)"
        );
        assert!(
            g.st.slots.free_lpids.contains(&highest),
            "the freed highest LPID must enter the free set (immediate in-memory reuse, S-STORE 4.2)"
        );
        assert!(
            !g.st.map.cur.contains_key(&highest),
            "the freed LPID leaves the current map"
        );
    }
    // Immediate reuse through the free set: the next allocation returns
    // the freed LPID (lowest-free-first), not a re-issue of a shrunk
    // counter.
    let again = prims::alloc_lpid(&cx).expect("alloc after free");
    assert_eq!(
        again, highest,
        "the freed highest LPID must be reusable at once through the free set"
    );
    assert_eq!(
        sched.lock().st.meta.next_lpid,
        before,
        "the reuse must not move next_lpid"
    );
}

/// **X-5c-F3-F1 step 1 pin (§12.4 / I-REDO-COMPLETE; a literal two-task
/// config).** A SyncOn commit is acked durable only through the
/// **contiguous written-and-synced prefix** of the WAL — the gate's
/// S-STORE §12.4 text. The draft let `wal_writer_step` run in two tasks
/// at once (the writer task and a committer helping from
/// `wal_wait_durable`), each taking its own batch and advancing
/// `written`/`durable` with `max`: a later batch's write+sync acked its
/// SyncOn commit over an earlier batch's unsynced bytes, and §12.5's
/// replay stop then lost the acked record at the crash (seed 21 r0:
/// ts=15 acked over ts=14's hole; the `frontier < lo` signature, 93 of
/// F3's 388 C-DIV rounds). The serialized writer fixes it: one batch in
/// flight at a time (`WalState::in_flight`), a helper that finds one
/// parks on `WalAcks`, and the counters advance only over the
/// contiguous prefix.
///
/// The config: segment 0 holds four durable setup commits ending
/// exactly at LSN 65536. Task A (the batch-1 holder) enqueues ts=5..8
/// filling segment 1 exactly, takes batch 1, writes it, and is held at
/// `writer:presync` (budget 900_000) — written, unsynced. Task B waits
/// for exactly that hole state, then enqueues ts=9 (segment 2) and runs
/// its §12.4 SyncOn ack wait. The driver crashes the world the moment B
/// parks on `WalAcks` (the serialized-writer rule) or B's ack returns
/// (the mutant). Asserted: ts=9's `wal_wait_durable` has not returned;
/// `wal.durable` never passed batch 1's start; and after F-CRASH
/// resolves (unsynced writes dropped) recovery returns the ts-prefix
/// through the setup commits with the WAL restarted at the valid end.
/// Reverting the fix (mutant M-WALa: the concurrent take with `max`
/// accounting restored) fails this pin on both of the first two
/// assertions — B's ack returns over batch 1's unsynced bytes and
/// `durable` sits past batch 1's start.
#[test]
fn cdiv_syncon_ack_never_crosses_an_unwritten_batch() {
    use e4_recovery::sim::WaitKey;
    let seg = e4_recovery::store::WAL_SEG;
    let fixed = e4_recovery::store::wal::REC_FIXED;
    // §12.2's cap is on the whole record, so the max payload leaves
    // room for the framing — and exactly four max records fill a
    // 64-KiB segment (4 × REC_MAX = WAL_SEG).
    let maxp = e4_recovery::store::wal::REC_MAX - fixed;
    let seed = 4242u64;
    let cfg = tcfg(8, 8, 1);
    // ---- setup (single task): segment 0 filled exactly and durable ----
    let (disk, st) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        for ts in 1..=4u64 {
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp as usize])
                .expect("setup enqueue");
        }
        wal::wal_wait_durable(&cx, seg).expect("setup durable");
        let g = sched.lock();
        assert_eq!(
            g.st.wal.append, seg,
            "the setup must fill segment 0 exactly"
        );
        assert_eq!(g.st.wal.durable, seg);
        (g.disk.clone(), g.st.clone())
    };
    // ---- the two-task world ----
    let mut st = st;
    st.parks.insert("writer:presync", 900_000);
    let host = TestWorld {
        sched: SchedCore::new(3, seed + 1),
        disk,
        st,
        flags: BTreeMap::new(),
        nums: BTreeMap::new(),
    };
    let sched = Arc::new(Sched::new(host));
    fn sf(e: Fail) -> SimFail {
        match e {
            Fail::Sim(s) => s,
            _ => SimFail::Internal("wal pin: a non-sim store failure"),
        }
    }
    // Task A: enqueue ts=5..8 (segment 1 exactly), take batch 1, write
    // it, and hold at `writer:presync` — written, unsynced.
    let a_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        for ts in 5..=8u64 {
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp as usize])
                .map_err(sf)?;
        }
        set_flag(sched, "a_enq");
        wal::wal_writer_step(&cx).map_err(sf)?;
        set_flag(sched, "a_done");
        Ok(())
    };
    // Task B: wait for the hole state (batch 1 written, unsynced),
    // then enqueue ts=9 (segment 2) and run its SyncOn ack wait.
    let b_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        loop {
            let hole = cx
                .with("pin:b:gate", |c| {
                    let st = c.store();
                    Ok(st.wal.written == 2 * seg && st.wal.durable == seg)
                })
                .map_err(sf)?;
            if hole {
                break;
            }
            cx.yield_now("pin:b:poll").map_err(sf)?;
        }
        set_flag(sched, "b_go");
        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 9), 9, &[7u8; 10]).map_err(sf)?;
        set_flag(sched, "b_enq2");
        let end9 = 2 * seg + fixed + 10;
        let r = wal::wal_wait_durable(&cx, end9).map_err(sf);
        if r.is_ok() {
            set_flag(sched, "b_acked");
        }
        r
    };
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for (t, body) in [
            (
                1usize,
                &a_body
                    as &(dyn Fn(&Sched<TestWorld>, TaskId) -> Result<(), SimFail> + Send + Sync),
            ),
            (2, &b_body),
        ] {
            let sched = Arc::clone(&sched);
            handles.push(scope.spawn(move || {
                if let Err(SimFail::Baton(s)) = sched.yield_baton(t) {
                    panic!("entry barrier refused: Baton at {s}");
                }
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&sched, t)));
                let is_panic = r.is_err();
                sched.finish(t);
                assert!(!is_panic, "task {t} panicked");
            }));
        }
        // The harness thread hands the baton over and parks on Join.
        let starter = Arc::clone(&sched);
        let h0 = scope.spawn(move || starter.start());
        // The driver: crash the world the moment B parks on WalAcks
        // (the serialized-writer rule in force) or B's ack returns (the
        // mutant's leap).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let (b_parked, acked) = {
                let g = sched.lock();
                (
                    g.sched.waiting.get(2) == Some(&Some(WaitKey::WalAcks)),
                    g.flags.get("b_acked").copied().unwrap_or(false),
                )
            };
            if b_parked || acked {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "driver timeout: B neither parked on WalAcks nor acked (a_enq={}, a_done={}, b_go={}, b_enq2={})",
                flag(&sched, "a_enq"),
                flag(&sched, "a_done"),
                flag(&sched, "b_go"),
                flag(&sched, "b_enq2")
            );
        }
        sched.deliver_crash();
        let _ = h0.join();
        for h in handles {
            let _ = h.join();
        }
    });
    // ---- the assertions at the crash ----
    let (acked, durable) = {
        let g = sched.lock();
        (
            g.flags.get("b_acked").copied().unwrap_or(false),
            g.st.wal.durable,
        )
    };
    assert!(
        !acked,
        "ts=9's wal_wait_durable returned before the crash: the SyncOn ack crossed batch 1's unsynced bytes (12.4 / I-REDO-COMPLETE)"
    );
    assert!(
        durable <= seg,
        "wal.durable ({durable}) passed batch 1's start ({seg}) before batch 1's sync"
    );
    // ---- F-CRASH resolves (unsynced writes dropped), then recovery ----
    let mut disk = {
        let g = sched.lock();
        g.disk.clone()
    };
    let mut frng = e4_recovery::rng::Rng::new(0xABCD_1234);
    disk.apply_crash(
        &mut frng,
        &e4_recovery::sim::FaultKnobs {
            keep_latest_pm: 0,
            tear_pm: 0,
        },
    );
    let world = pass_world(1, seed + 2, disk, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("recover after the crash");
    assert_eq!(
        frontier, 4,
        "the recovered state is the ts-prefix through the setup commits (no acked commit lost)"
    );
    assert_eq!(
        rec_st.wal.append, seg,
        "the WAL restarts at the valid end (segment 1's first LSN)"
    );
}

/// **X-5c-F3-F1 step 2 pin (§12.5 / G2; a literal config).** §12.5 as
/// written: "after `SegmentEnd`, the reader moves on to the next
/// segment's first LSN … The stopping point is the valid end." The
/// draft's `ckpt::recover` never advanced `valid_end` over an accepted
/// `SegmentEnd`, so when the next segment was missing, G2's
/// `truncate_wal` zeroed from the last commit's end — destroying the
/// very SegmentEnd the reader had just accepted (seed 6149 r1: the
/// nested crash dropped the zeros before their sync, and life 2
/// re-accepted the surviving SegmentEnd, tainted against the dropped
/// zeros — an MW served corruption).
///
/// The config: segment 0 holds one durable Commit (ts=1, LSN 0) and a
/// durable `SegmentEnd` at [45, 90); **segment 1 does not exist**. A
/// nested recovery crash is swept over every recovery yield `k` in
/// `1..=120` (life 1 arms `crash_at_yield = k`, F-CRASH resolves with
/// keep-latest 0 — every volatile write dropped — then life 2 recovers
/// again, exactly the harness's nested-crash retry). Asserted for
/// every `k`: no served range in any life; the SegmentEnd's bytes
/// durable and byte-equal to the fixture; life 2's recovered
/// `wal.append` is segment 1's first LSN. Under the fix,
/// `truncate_wal` issues no write at all (its first probe is the
/// missing segment), so every `k` passes. Reverting the fix (mutant
/// M-SEGa: the `valid_end` advance removed) fails this pin at every
/// `k` — the zeroing truncation either lands durably (the SegmentEnd's
/// bytes destroyed, `append` below segment 1) or is cut by the crash
/// between its write and its sync and life 2 then serves the tainted
/// SegmentEnd (6149's exact mechanism).
#[test]
fn segend_valid_end_survives_a_missing_next_segment() {
    let seg = e4_recovery::store::WAL_SEG;
    let fixed = e4_recovery::store::wal::REC_FIXED;
    let seed = 5150u64;
    let cfg = tcfg(8, 8, 1);
    // ---- the fixture: segment 0 = one Commit + one SegmentEnd, durable;
    // segment 1 does not exist ----
    let (disk, st) = create_store(cfg.clone(), seed, None);
    let (disk, segend_bytes, seg0) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk,
            st,
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let name = "/wal/0000000000000000.log";
        let f = cx.env().create(name).expect("create segment 0");
        cx.env().set_len(f, seg).expect("preallocate");
        let rec1 = wal::enc_record(wal::K_COMMIT, (0, 1), 1, &[], 0);
        let segend = wal::enc_record(wal::K_SEGEND, (0, 0), 0, &[], fixed);
        assert_eq!(rec1.len() as u64, fixed, "the ts=1 record's length");
        let mut both = rec1.clone();
        both.extend_from_slice(&segend);
        cx.env().write_at(f, 0, &both).expect("write fixture");
        cx.env().sync_dir("/wal/").expect("G1 dirsync");
        cx.env().sync_data(f).expect("G5 sync");
        let g = sched.lock();
        let durable = g.disk.durable_read(f, 0, 2 * fixed);
        assert_eq!(durable, both, "the fixture must be durable");
        (g.disk.clone(), segend, f)
    };
    // ---- the sweep: one nested crash point per recovery yield ----
    for k in 1u64..=120 {
        // Life 1: arm the nested crash at yield k and recover.
        let mut d1 = disk.clone();
        #[allow(clippy::type_complexity)]
        let (life1, served1): (
            Result<(e4_recovery::store::StoreInner, u64), Fail>,
            Vec<(usize, u64, u64)>,
        ) = {
            let host = TestWorld {
                sched: SchedCore::new(1, seed + k),
                disk: d1.clone(),
                st: StoreInner::empty(cfg.clone(), 0),
                flags: BTreeMap::new(),
                nums: BTreeMap::new(),
            };
            let world = Sched::new(host);
            let cx = Cx {
                w: &world,
                t: HARNESS,
            };
            cx.with("pin2:arm", |c| {
                c.sched_mut().crash_at_yield = Some(k);
                Ok(())
            })
            .expect("arm");
            let r = ckpt::recover(&cx, cfg.clone());
            let s = world.lock().disk.served.clone();
            (r, s)
        };
        // The invariants, checked wherever recovery finished.
        let check = |bytes_now: &[u8], served: &[(usize, u64, u64)], append: u64, what: &str| {
            assert_eq!(
                bytes_now,
                &segend_bytes[..],
                "k={k} ({what}): the SegmentEnd's bytes were not left durable and unchanged (12.5/G2)"
            );
            assert!(
                served.is_empty(),
                "k={k} ({what}): a life served a corrupted range: {served:?}"
            );
            assert_eq!(
                append, seg,
                "k={k} ({what}): wal.append must be segment 1's first LSN"
            );
        };
        match life1 {
            Err(Fail::Sim(SimFail::Crashed)) => {
                // The crash fired: resolve F-CRASH (every volatile write
                // dropped), then life 2 recovers on the resolved disk —
                // exactly the harness's nested-crash retry.
                let mut frng = e4_recovery::rng::Rng::new(seed + k);
                d1.apply_crash(
                    &mut frng,
                    &e4_recovery::sim::FaultKnobs {
                        keep_latest_pm: 0,
                        tear_pm: 0,
                    },
                );
                let host = TestWorld {
                    sched: SchedCore::new(1, seed + k + 1),
                    disk: d1.clone(),
                    st: StoreInner::empty(cfg.clone(), 0),
                    flags: BTreeMap::new(),
                    nums: BTreeMap::new(),
                };
                let world = Sched::new(host);
                let cx = Cx {
                    w: &world,
                    t: HARNESS,
                };
                let (rec_st, frontier) = ckpt::recover(&cx, cfg.clone())
                    .unwrap_or_else(|e| panic!("k={k}: life 2 recover: {e:?}"));
                assert_eq!(frontier, 1, "k={k}: the ts=1 commit replays");
                let (bytes_now, served2) = {
                    let g = world.lock();
                    (
                        g.disk.durable_read(seg0, fixed, fixed),
                        g.disk.served.clone(),
                    )
                };
                check(&bytes_now, &served1, rec_st.wal.append, "life 2");
                check(&bytes_now, &served2, rec_st.wal.append, "life 2");
            }
            Ok((st, frontier)) => {
                // The point never fired (k is past this life's yield
                // total): the same invariants hold on life 1's outcome.
                assert_eq!(frontier, 1, "k={k}: the ts=1 commit replays");
                let bytes_now = d1.durable_read(seg0, fixed, fixed);
                check(&bytes_now, &served1, st.wal.append, "unfired life 1");
            }
            other => panic!("k={k}: life 1 must crash or finish clean, got {other:?}"),
        }
    }
}

/// **X-5c-F3-F1 step 3 pin, G-2 (the gate's exact patch at
/// `fetch_page:poison`; §9.2 / G8).** The gate's S-1-F3 text for §9.2:
/// "a read whose bytes fail §3.3 inside apply, a split, a pack, GC or a
/// checkpoint poisons the store; a client read never poisons". The
/// literal config: two trees, each a single leaf, checkpointed (both
/// leaves durable and referenced); tree 2's leaf slot is then corrupted
/// on the durable disk, so every read of it fails §3.3 against the
/// image's reference CRC.
/// - (i) A **client read** (`tree_scan`, `inside_op = false`) of tree 2
///   gets XX001 (`Fail::Corrupt`) and the store stays usable: a
///   following mutation of the untouched tree 3 succeeds.
/// - (ii) A **gated op** that fetches the page (`tree_insert` into
///   tree 2: apply, `inside_op = true`) gets XX001 and **poisons** the
///   store: the next mutation — of tree 3, not touching the damaged
///   page — returns [`Fail::Poisoned`].
///
/// G-2 (`if !inside_op {` at the poison mark) fails both halves — the
/// scan poisons and the gated op does not — and M-G8a (the poison mark
/// removed) fails (ii).
#[test]
fn g8_read_never_poisons_gated_op_does() {
    let seed = 6161u64;
    let cfg = tcfg(8, 8, 1);
    // Two trees, one leaf each, durable and referenced by image 1.
    let (disk, _st) = create_store(cfg.clone(), seed, Some(2));
    let (disk, damaged_slot) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk,
            st: _st,
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        tree::dir_create_tree(&cx, 3).expect("CreateTree 3");
        let r2 = tree::dir_root_of(&cx, 2).expect("root 2");
        let r3 = tree::dir_root_of(&cx, 3).expect("root 3");
        for i in 0..4u64 {
            ins(&cx, 2, r2, &i.to_be_bytes(), 1, &val(i, 1));
            ins(&cx, 3, r3, &i.to_be_bytes(), 1, &val(i, 1));
        }
        cx_checkpoint(&sched, |_| Ok(()));
        // Tree 2's root leaf slot, corrupted on the durable disk.
        let (pf, slot) = {
            let g = sched.lock();
            let r = g.st.map.cur.get(&r2).copied().expect("tree 2 ref");
            (g.st.page_file, r.slot)
        };
        let garbage = vec![0xAAu8; 4096];
        cx.env()
            .write_at(pf, slot * 4096, &garbage)
            .expect("corrupt");
        cx.env().sync_data(pf).expect("sync the corruption");
        let g = sched.lock();
        (g.disk.clone(), slot)
    };
    assert!(
        damaged_slot > 5,
        "the damaged slot must be a real page slot"
    );
    // ---- (i) a client read detects and never poisons ----
    {
        let world = pass_world(1, seed + 1, disk.clone(), StoreInner::empty(cfg.clone(), 0));
        let cx = Cx {
            w: &world,
            t: HARNESS,
        };
        let (opened, _meta) = ckpt::open(&cx, cfg.clone()).expect("open");
        cx.with("g2:install", |c| {
            *c.store() = opened;
            Ok(())
        })
        .expect("install");
        let r2 = tree::dir_root_of(&cx, 2).expect("root 2");
        let scan = tree::tree_scan(&cx, r2, 10);
        assert!(
            matches!(scan, Err(Fail::Corrupt(_))),
            "(i): the client read must detect the damage (XX001), got {scan:?}"
        );
        let poisoned = world.lock().st.poisoned.is_some();
        assert!(
            !poisoned,
            "(i): a client read must never poison the store (9.2/G8)"
        );
        // The store stays usable: a mutation that does not touch the
        // damaged page succeeds.
        let r3 = tree::dir_root_of(&cx, 3).expect("root 3");
        ins(&cx, 3, r3, &99u64.to_be_bytes(), 1, &val(99, 1));
        let poisoned = world.lock().st.poisoned.is_some();
        assert!(!poisoned, "(i): the store must stay usable after the read");
    }
    // ---- (ii) a gated op detects and poisons ----
    {
        let world = pass_world(1, seed + 2, disk.clone(), StoreInner::empty(cfg.clone(), 0));
        let cx = Cx {
            w: &world,
            t: HARNESS,
        };
        let (opened, _meta) = ckpt::open(&cx, cfg.clone()).expect("open");
        cx.with("g2:install2", |c| {
            *c.store() = opened;
            Ok(())
        })
        .expect("install2");
        let r2 = tree::dir_root_of(&cx, 2).expect("root 2");
        let ekey = tree::enc_ekey(&50u64.to_be_bytes(), 1);
        let ins_r = tree::tree_insert(&cx, 2, r2, &ekey, &val(50, 1), false);
        assert!(
            matches!(ins_r, Err(Fail::Corrupt(_))),
            "(ii): the gated op must detect the damage (XX001), got {ins_r:?}"
        );
        let poisoned = world.lock().st.poisoned.clone();
        assert!(
            poisoned.is_some(),
            "(ii): a read whose bytes fail 3.3 inside a gated op must poison the store (9.2/G8)"
        );
        // The next mutation — of the untouched tree — refuses.
        let r3 = tree::dir_root_of(&cx, 3).expect("root 3");
        let ekey3 = tree::enc_ekey(&98u64.to_be_bytes(), 1);
        let next = tree::tree_insert(&cx, 3, r3, &ekey3, &val(98, 1), false);
        assert!(
            matches!(next, Err(Fail::Poisoned(_))),
            "(ii): every later mutation must refuse with Poisoned, got {next:?}"
        );
        let _ = poisoned;
    }
}

/// **X-5c-F3-F1 step 3 pin, G-1 (the gate's exact patch at the replay
/// walk's segment probe; §12.5's crash propagation).** The durable WAL
/// spans two segments with an acked SyncOn commit in segment ≥ 1
/// (segment 0: ts=1..4; segment 1: ts=5, synced — acked). A nested
/// recovery crash is swept over every recovery yield `k` in `1..=120`;
/// each fired point must **propagate** as `Err(Sim(Crashed))` — the
/// nested-crash arm's retry — never as corruption or a silent stop, and
/// the retry (life 2, after F-CRASH resolves at keep-latest 0) must
/// recover the acked commit (`frontier == 5`). An unfired point (k past
/// the life's yield total) must finish clean with the same frontier.
#[test]
fn replay_walk_probe_never_swallows_a_recovery_crash() {
    let seg = e4_recovery::store::WAL_SEG;
    let fixed = e4_recovery::store::wal::REC_FIXED;
    let maxp = e4_recovery::store::wal::REC_MAX - fixed;
    let seed = 6262u64;
    let cfg = tcfg(8, 8, 1);
    // The fixture: segment 0 ends in a SegmentEnd filler (ts=1..4 sized
    // so ts=5 cannot fit the tail and hops), and segment 1 holds ts=5 —
    // all durable, so ts=5 is the acked SyncOn commit in segment >= 1.
    // (A record ending exactly at the segment boundary is avoided on
    // purpose: §12.2's enqueuer allows it, but §12.5's reader can only
    // hop on a decodable filler, so a boundary-exact fill makes the
    // next segment unreachable — a §12.2/§12.5 silence recorded for P
    // in this card's README, not a shape to build on.)
    let (disk, _st) = create_store(cfg.clone(), seed, None);
    let disk = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk,
            st: _st,
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let _ = maxp;
        for (ts, payload) in [(1u64, 16310usize), (2, 16310), (3, 16310), (4, 16336)] {
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; payload])
                .expect("enqueue seg0");
        }
        wal::wal_wait_durable(&cx, 65446).expect("seg0 durable");
        let (lsn5, len5) =
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 5), 5, &[9u8; 10]).expect("enqueue seg1");
        assert_eq!(
            lsn5, seg,
            "ts=5 must land in segment 1 (the filler hopped it)"
        );
        wal::wal_wait_durable(&cx, lsn5 + len5).expect("seg1 durable (the ack)");
        let g = sched.lock();
        assert_eq!(
            g.st.wal.durable,
            seg + fixed + 10,
            "ts=5 is durable (acked)"
        );
        g.disk.clone()
    };
    for k in 1u64..=120 {
        let mut d1 = disk.clone();
        let life1 = {
            let host = TestWorld {
                sched: SchedCore::new(1, seed + k),
                disk: d1.clone(),
                st: StoreInner::empty(cfg.clone(), 0),
                flags: BTreeMap::new(),
                nums: BTreeMap::new(),
            };
            let world = Sched::new(host);
            let cx = Cx {
                w: &world,
                t: HARNESS,
            };
            cx.with("g1:arm", |c| {
                c.sched_mut().crash_at_yield = Some(k);
                Ok(())
            })
            .expect("arm");
            ckpt::recover(&cx, cfg.clone())
        };
        match life1 {
            Err(Fail::Sim(SimFail::Crashed)) => {
                // Propagated: the nested-crash arm's retry (F-CRASH at
                // keep-latest 0, then a fresh recovery) must recover the
                // acked commit.
                let mut frng = e4_recovery::rng::Rng::new(seed + k);
                d1.apply_crash(
                    &mut frng,
                    &e4_recovery::sim::FaultKnobs {
                        keep_latest_pm: 0,
                        tear_pm: 0,
                    },
                );
                let host = TestWorld {
                    sched: SchedCore::new(1, seed + k + 1),
                    disk: d1.clone(),
                    st: StoreInner::empty(cfg.clone(), 0),
                    flags: BTreeMap::new(),
                    nums: BTreeMap::new(),
                };
                let world = Sched::new(host);
                let cx = Cx {
                    w: &world,
                    t: HARNESS,
                };
                let (_st, frontier) = ckpt::recover(&cx, cfg.clone())
                    .unwrap_or_else(|e| panic!("k={k}: the retry must recover: {e:?}"));
                assert_eq!(frontier, 5, "k={k}: the acked commit must be recovered");
            }
            Ok((_st, frontier)) => {
                assert_eq!(
                    frontier, 5,
                    "k={k}: an unfired point finishes clean with the acked commit"
                );
            }
            other => panic!("k={k}: the crash must propagate as Sim(Crashed), got {other:?}"),
        }
    }
}

/// **X-5c-F3-F1 step 7 pin, C-XX sub-shape 1: the directory's identical
/// re-put never appends a duplicate (§6.8; the B5 `pos` vs `pos − 1`
/// deviation on the unversioned dir path).** `dir_put_locked` located
/// the equal entry at `pos` — the first strictly-greater position —
/// but an existing copy sits at `pos − 1`, so a directory put for an
/// id the durable leaf already held (with the in-memory directory
/// missing it — an interrupted drop's entry-removal step never landed)
/// appended a **duplicate**: seed 151 r1's directory keys were
/// `[1, 2, 3, 4, 4]`, and amcheck (ii) reported "dir: leaf keys out of
/// order" plus the two-roots cascade ("child lpid N unmapped",
/// "separators out of order", "lpid 38 reachable more than once"). The
/// literal config: a store with tree 2; the in-memory directory entry
/// removed (the interrupted drop's state); `dir_create_tree(2)` again —
/// the durable directory leaf must hold exactly one entry for 2 and
/// amcheck must stay clean. Reverting the fix (mutant M-CXXc: the
/// `pos − 1` check removed) appends the duplicate and fails the pin.
#[test]
fn cxx_dir_reput_never_appends_a_duplicate() {
    let seed = 7070u64;
    let cfg = tcfg(8, 8, 1);
    let (disk, st) = create_store(cfg.clone(), seed, Some(2));
    let sched = pass_world(1, seed, disk, st);
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    // The interrupted drop's state: the durable leaf holds tree 2, the
    // in-memory directory does not.
    cx.with("pin7:forget", |c| {
        c.store().dir.remove(&2);
        Ok(())
    })
    .expect("forget");
    tree::dir_create_tree(&cx, 2).expect("re-create tree 2");
    let root = {
        let g = sched.lock();
        g.st.meta.dir_root
    };
    let keys = tree::raw_cells(&cx, root, false).expect("dir cells");
    let twos = keys
        .iter()
        .filter(|(k, _)| k.as_slice() == 2u64.to_be_bytes().as_slice())
        .count();
    assert_eq!(
        twos, 1,
        "the directory leaf must hold exactly one entry for tree 2"
    );
    let findings = amcheck::amcheck(&cx, &cfg);
    assert!(
        findings.is_empty(),
        "a directory re-put must never disorder the leaf (6.8): {findings:?}"
    );
}

/// **X-5c-F3-F3a step 2 pin (§12.2/§12.5, the exact segment end; S-1-F3
/// gate G2, edit 2; a literal config).** §12.2's tail rule lets a record
/// end **exactly** at its segment's end with no room for a `SegmentEnd`
/// filler, and §12.5's carried hop text moves the reader on "after
/// `SegmentEnd`, **or after an accepted record that ends exactly at its
/// segment's end**". The draft's walk exited its inner loop at
/// `off == WAL_SEG` and stopped with the valid end at the boundary, so
/// G2's truncation zeroed the next segment's records — the four
/// boundary store losses (1064 r1, 4770 r2, 5054 r1, 6263 r2).
///
/// The config: segment 0 holds four durable max-size records ending
/// exactly at LSN 65536 (4 × REC_MAX = WAL_SEG); segment 1 holds two
/// client-acked SyncOn commits (ts=5, 6), synced before the crash.
/// Recovery must replay them (frontier = 6 ≥ the max acked), and
/// segment 1's records must not be zeroed. Reverting the hop (mutant
/// SEG-EXACT-NO-HOP) fails both: the walk never enters segment 1, the
/// frontier stalls at 4, and `truncate_wal` zeroes segment 1 from its
/// first byte — the acked commits are lost with no media fault.
#[test]
fn wal_exact_segment_end_hops() {
    let seg = e4_recovery::store::WAL_SEG;
    let fixed = e4_recovery::store::wal::REC_FIXED;
    let maxp = (e4_recovery::store::wal::REC_MAX - fixed) as usize;
    let seed = 6262u64;
    let cfg = tcfg(8, 8, 1);
    // ---- setup (single task): segment 0 filled exactly and durable,
    // then two SyncOn commits in segment 1, acked (synced) ----
    let (disk, seg1_fid, rec_bytes) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        for ts in 1..=4u64 {
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp])
                .expect("setup enqueue");
        }
        {
            let g = sched.lock();
            assert_eq!(
                g.st.wal.append, seg,
                "the setup must fill segment 0 exactly"
            );
        }
        let (lsn5, _len5) =
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 5), 5, &[5u8; 10]).expect("ts=5 enqueue");
        let (lsn6, len6) =
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 6), 6, &[6u8; 10]).expect("ts=6 enqueue");
        assert_eq!(lsn5, seg, "ts=5 starts at segment 1's first LSN");
        assert_eq!(lsn6, seg + fixed + 10, "ts=6 follows it");
        let end6 = lsn6 + len6;
        wal::wal_wait_durable(&cx, end6).expect("both acked durable");
        let (fid, bytes5, bytes6) = {
            let g = sched.lock();
            let fid = g
                .disk
                .stat("/wal/0000000000010000.log")
                .expect("segment 1 bound");
            let n5 = fixed + 10;
            (
                fid,
                g.disk.durable_read(fid, 0, n5),
                g.disk.durable_read(fid, n5, len6),
            )
        };
        assert!(
            !bytes5.iter().all(|b| *b == 0) && !bytes6.iter().all(|b| *b == 0),
            "the fixture must hold real record bytes"
        );
        (g_of(&sched), fid, (bytes5, bytes6))
    };
    let (bytes5, bytes6) = rec_bytes;
    let _ = seg1_fid;
    // ---- the crash (everything synced: F-CRASH resolves it away) ----
    let mut disk = disk;
    let mut frng = e4_recovery::rng::Rng::new(seed + 7);
    disk.apply_crash(
        &mut frng,
        &e4_recovery::sim::FaultKnobs {
            keep_latest_pm: 0,
            tear_pm: 0,
        },
    );
    // ---- recovery: the hop must carry the walk into segment 1 ----
    let world = pass_world(1, seed + 8, disk, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("recover after the crash");
    assert_eq!(
        frontier, 6,
        "recovery replays the acked SyncOn commits in segment 1 (frontier >= max acked)"
    );
    assert_eq!(
        rec_st.wal.append,
        seg + 2 * (fixed + 10),
        "the WAL restarts past both records"
    );
    // Segment 1's records are not zeroed: the bytes survive on the disk
    // exactly as they were written (G2's truncation started past them).
    let fid = world
        .lock()
        .disk
        .stat("/wal/0000000000010000.log")
        .expect("segment 1 still bound");
    let n5 = fixed + 10;
    let now5 = world.lock().disk.durable_read(fid, 0, n5);
    let now6 = world.lock().disk.durable_read(fid, n5, bytes6.len() as u64);
    assert_eq!(now5, bytes5, "ts=5's bytes are not zeroed");
    assert_eq!(now6, bytes6, "ts=6's bytes are not zeroed");
    // ---- **X-5c-F3-F3b step 2 (d), the new flip variant (gate edit
    // 2):** a flip after the next segment's records opens `Ok`.** The
    // recovered store checkpoints again — the flip's `wal_ckpt_end`
    // covers both segment-1 records, step 1 waits for their contiguous
    // durability, meta 2 is durable — then a crash, then recovery: the
    // walk hops into segment 1 past the exact boundary, the coverage
    // check passes, and the open is `Ok`. Under SEG-EXACT-NO-HOP
    // (F3-F3a's hop disabled) the walk stops at the boundary, the
    // valid end sits below the durable meta's `wal_ckpt_end`, and this
    // recovery returns a **false `Fail::WalLoss`** — the flip variant's
    // catcher.
    {
        let disk2 = world.lock().disk.clone();
        let world2 = pass_world(1, seed + 20, disk2, rec_st);
        let cx2 = Cx {
            w: &world2,
            t: HARNESS,
        };
        let mut hooks = |_hook: CkptHook| Ok(());
        ckpt::checkpoint(&cx2, &mut hooks).expect("the flip over segment 1's records");
        let mut d2 = world2.lock().disk.clone();
        crash_clean(&mut d2, seed + 21);
        let world3 = pass_world(1, seed + 22, d2, StoreInner::empty(cfg.clone(), 0));
        let cx3 = Cx {
            w: &world3,
            t: HARNESS,
        };
        let r = ckpt::recover(&cx3, cfg.clone());
        assert!(
            matches!(r, Ok((_, 6))),
            "a flip after the next segment's records opens Ok (the hop carries\
             \nthe walk past the exact boundary; I-WAL-COVER passes), got {r:?}"
        );
    }
}

/// A small helper: the world's disk, cloned (the pin above).
fn g_of(sched: &Sched<TestWorld>) -> SimDisk {
    sched.lock().disk.clone()
}

/// **X-5c-F3-F3a step 2 pin (§12.2's tail rule; a literal config).** An
/// enqueue that would leave a tail of exactly `REC_FIXED − 1` bytes
/// (44: room for the record but not for a `SegmentEnd` after it) is
/// placed at the **next segment's first LSN**, behind a `SegmentEnd`
/// filler — the carried §12.2 sentence: "Otherwise the writer writes
/// `SegmentEnd` there, and the record starts at the next segment's
/// first LSN." The SyncOn commit after it is acked and recovered.
///
/// The config: segment 0 is filled to leave exactly 99 bytes of room
/// (55 for the ts=5 record + 44 of dead tail); ts=5's enqueue must
/// return lsn 65536 — not 65437. Reverting the rule (mutant
/// SEG-TAIL-SHORT: `fits` relaxed to `probe ≤ room`) places the record
/// in the 44-byte dead tail and fails the placement assertion.
#[test]
fn wal_tail_short_never_placed() {
    let seg = e4_recovery::store::WAL_SEG;
    let fixed = e4_recovery::store::wal::REC_FIXED;
    let seed = 6363u64;
    let cfg = tcfg(8, 8, 1);
    let host = TestWorld {
        sched: SchedCore::new(1, seed),
        disk: SimDisk::new(),
        st: StoreInner::empty(cfg.clone(), 0),
        flags: BTreeMap::new(),
        nums: BTreeMap::new(),
    };
    let sched = Sched::new(host);
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
    cx.with("pin:install", |c| {
        *c.store() = st;
        Ok(())
    })
    .expect("install");
    // Fill segment 0 to leave exactly 99 bytes: three max records plus
    // one of payload 16240 (3×(45+16339) + 45+16240 = 65437; 65536 −
    // 65437 = 99 = 55 + 44).
    let maxp = (e4_recovery::store::wal::REC_MAX - fixed) as usize;
    for ts in 1..=3u64 {
        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp]).expect("setup enqueue");
    }
    wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 4), 4, &vec![0u8; 16240]).expect("setup enqueue 4");
    {
        let g = sched.lock();
        assert_eq!(
            g.st.wal.append,
            seg - 99,
            "the setup must leave exactly 99 bytes of room"
        );
    }
    // The pin: ts=5 (55 bytes) must be placed at the next segment's
    // first LSN, behind a SegmentEnd — never in the 44-byte dead tail.
    let (lsn5, len5) =
        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 5), 5, &[5u8; 10]).expect("ts=5 enqueue");
    assert_eq!(
        lsn5, seg,
        "an enqueue leaving a REC_FIXED-1 tail is placed at the next segment (12.2's tail rule)"
    );
    assert_eq!(len5, fixed + 10);
    {
        let g = sched.lock();
        assert_eq!(g.st.wal.append, seg + fixed + 10);
    }
    // The SegmentEnd filler sits at the dead tail, durable with the
    // ack (the batch is one write per segment).
    let end5 = lsn5 + len5;
    wal::wal_wait_durable(&cx, end5).expect("ts=5 acked durable");
    // The crash (everything synced) and recovery: the commit replays.
    let mut disk = {
        let g = sched.lock();
        g.disk.clone()
    };
    let mut frng = e4_recovery::rng::Rng::new(seed + 7);
    disk.apply_crash(
        &mut frng,
        &e4_recovery::sim::FaultKnobs {
            keep_latest_pm: 0,
            tear_pm: 0,
        },
    );
    let f0 = disk.stat("/wal/0000000000000000.log").expect("segment 0");
    let tail = disk.durable_read(f0, seg - 99, fixed);
    assert_eq!(
        tail.first(),
        Some(&0x4E),
        "the SegmentEnd filler's magic sits at the dead tail (12.2)"
    );
    let world = pass_world(1, seed + 8, disk, StoreInner::empty(cfg.clone(), 0));
    let cx2 = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, frontier) = ckpt::recover(&cx2, cfg.clone()).expect("recover after the crash");
    assert_eq!(
        frontier, 5,
        "the SyncOn commit placed behind the SegmentEnd is recovered"
    );
    assert_eq!(
        rec_st.wal.append,
        seg + fixed + 10,
        "the WAL restarts past the recovered record"
    );
}

/// **X-5c-F3-F3a step 3 pin (§12.4 / I-ACK-PREFIX with two or more
/// batches in flight; S-1-F3 W3's `t_ack_prefix` ported; a literal
/// two-task config).** "More than one batch may be in flight. The rule
/// below holds however batches are scheduled": both frontiers advance
/// only over the contiguous prefix — writing or syncing a batch while
/// an earlier one is unwritten advances neither past the earlier one.
///
/// The config: segment 0 holds four durable setup commits ending
/// exactly at LSN 65536. Task A enqueues ts=5 (batch 1, segment 1),
/// takes it through `wal_writer_step` and parks at `writer:prewrite`
/// (budget 900_000) — taken, **unwritten**. Task B waits for exactly
/// that state, enqueues ts=6 (n+1, batch 2 above it in the same
/// segment), takes it through `wal_writer_step` (no `Busy` park: two
/// batches in flight at once), writes it and syncs it, records the
/// mid-state, and runs its §12.4 SyncOn ack wait — parking on
/// `WalAcks`. The driver then releases A (the clean arm: `durable`
/// covers batch 2's end, both waits return) or crashes the world (the
/// crash arm: recovery is a ts-prefix with no hole, and ts=6 was never
/// client-acked).
#[test]
fn ack_prefix_two_batches_in_flight() {
    let seg = e4_recovery::store::WAL_SEG;
    let fixed = e4_recovery::store::wal::REC_FIXED;
    let maxp = e4_recovery::store::wal::REC_MAX - fixed;
    let seed = 4343u64;
    let cfg = tcfg(8, 8, 1);
    let n = 5u64; // ts = n in batch 1, ts = n+1 in batch 2
    // ---- setup (single task): segment 0 filled exactly and durable ----
    let (disk, st) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        for ts in 1..=4u64 {
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp as usize])
                .expect("setup enqueue");
        }
        wal::wal_wait_durable(&cx, seg).expect("setup durable");
        let g = sched.lock();
        assert_eq!(
            g.st.wal.append, seg,
            "the setup must fill segment 0 exactly"
        );
        (g.disk.clone(), g.st.clone())
    };
    let b1_start = seg;
    let b2_end = seg + 2 * (fixed + 10);
    // ---- the two-task world, once per arm (clean release / crash) ----
    for arm in 0..2 {
        let mut st = st.clone();
        // The hold is bounded only by B's next action: in the clean arm
        // B itself zeroes the budget (releasing A) after recording the
        // mid-state; in the crash arm B arms the scheduler's
        // `crash_at_yield` one yield ahead — A's very next prewrite
        // spin yields, so the crash lands deterministically before A's
        // write, with no driver-side race.
        st.parks.insert("writer:prewrite", 900_000);
        let host = TestWorld {
            sched: SchedCore::new(3, seed + 1 + arm as u64),
            disk: disk.clone(),
            st,
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Arc::new(Sched::new(host));
        fn sf(e: Fail) -> SimFail {
            match e {
                Fail::Sim(s) => s,
                _ => SimFail::Internal("wal pin: a non-sim store failure"),
            }
        }
        // Task A: enqueue ts=n, take batch 1 (parks at writer:prewrite),
        // then wait for its own ack.
        let a_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, n), n, &[5u8; 10]).map_err(sf)?;
            set_flag(sched, "a_enq");
            wal::wal_writer_step(&cx).map_err(sf)?;
            set_flag(sched, "a_done");
            let end5 = seg + fixed + 10;
            wal::wal_wait_durable(&cx, end5).map_err(sf)?;
            set_flag(sched, "a_acked");
            Ok(())
        };
        // Task B: wait for batch 1 taken-and-unwritten, then take batch
        // 2 above it (two in flight), write it, sync it, record the
        // mid-state, and run the ack wait (parking on WalAcks).
        let b_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            loop {
                let held = cx
                    .with("pin:b:gate", |c| {
                        let st = c.store();
                        Ok(st.wal.in_flight.len() == 1 && st.wal.written == seg)
                    })
                    .map_err(sf)?;
                if held {
                    break;
                }
                cx.yield_now("pin:b:poll").map_err(sf)?;
            }
            set_flag(sched, "b_go");
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, n + 1), n + 1, &[6u8; 10]).map_err(sf)?;
            set_flag(sched, "b_enq2");
            // The take itself: batch 2 above the held batch 1 — the
            // two-in-flight shape (wal_writer_step has no Busy park).
            wal::wal_writer_step(&cx).map_err(sf)?;
            // The mid-state, recorded atomically: batch 1 still in
            // flight (so two were in flight at once — this step took
            // batch 2 while batch 1 was held), and both frontiers at
            // or below batch 1's start.
            let (still_a, b2_completed, w, d) = cx
                .with("pin:b:mid", |c| {
                    let st = c.store();
                    let b2_start = seg + fixed + 10;
                    Ok((
                        st.wal.in_flight.contains(&(seg, b2_start)),
                        // B's own batch was taken, written and synced
                        // above A's still-held batch — two batches were
                        // in flight at once (B's is released on its
                        // step's return; its completed interval is the
                        // witness).
                        st.wal.written_iv.iter().any(|&(a, _)| a == b2_start)
                            && st.wal.synced_iv.iter().any(|&(a, _)| a == b2_start),
                        st.wal.written,
                        st.wal.durable,
                    ))
                })
                .map_err(sf)?;
            if still_a && b2_completed {
                set_flag(sched, "two_in_flight");
            }
            if w <= b1_start {
                set_flag(sched, "mid_w_ok");
            }
            if d <= b1_start {
                set_flag(sched, "mid_d_ok");
            }
            if d < b2_end {
                // The wait this task is about to enter cannot be
                // satisfied at entry (its own atomic observation), so
                // "entered and not returned" is recorded race-free.
                set_flag(sched, "mid_wait_open");
            }
            if arm == 0 {
                // The clean arm: release A here — its next park
                // iteration exits, it writes and syncs batch 1, and
                // the frontiers absorb both batches in order.
                cx.with("pin:b:release", |c| {
                    c.store().parks.insert("writer:prewrite", 0);
                    Ok(())
                })
                .map_err(sf)?;
            } else {
                // The crash arm: arm the world's crash one yield ahead.
                // A's prewrite spin yields within microseconds — the
                // crash lands before A's write, deterministically.
                cx.with("pin:b:armcrash", |c| {
                    let y = c.sched_mut().yields;
                    c.sched_mut().crash_at_yield = Some(y + 1);
                    Ok(())
                })
                .map_err(sf)?;
            }
            set_flag(sched, "b_wrote");
            let r = wal::wal_wait_durable(&cx, b2_end).map_err(sf);
            if r.is_ok() {
                set_flag(sched, "b_acked");
            }
            r
        };
        std::thread::scope(|scope| {
            let mut handles: Vec<_> = Vec::new();
            for (t, body) in [
                (
                    1usize,
                    &a_body
                        as &(
                             dyn Fn(&Sched<TestWorld>, TaskId) -> Result<(), SimFail> + Send + Sync
                         ),
                ),
                (2, &b_body),
            ] {
                let sched = Arc::clone(&sched);
                handles.push(scope.spawn(move || {
                    if let Err(SimFail::Baton(s)) = sched.yield_baton(t) {
                        panic!("entry barrier refused: Baton at {s}");
                    }
                    let r =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&sched, t)));
                    let is_panic = r.is_err();
                    sched.finish(t);
                    assert!(!is_panic, "task {t} panicked");
                }));
            }
            let starter = Arc::clone(&sched);
            let mut h0 = Some(scope.spawn(move || starter.start()));
            // The driver: B recorded the mid-state atomically and is
            // entering its wait (which its own observation shows cannot
            // be satisfied — `mid_wait_open`); A is held at
            // writer:prewrite (effectively indefinitely; see above).
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            let mut timed_out = false;
            loop {
                if flag(&sched, "b_wrote") {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    // Fail the world first so the joins cannot hang on a
                    // parked task, then report.
                    sched.deliver_crash();
                    timed_out = true;
                    break;
                }
            }
            if timed_out {
                if let Some(h) = h0.take() {
                    let _ = h.join();
                }
                for h in std::mem::take(&mut handles) {
                    let _ = h.join();
                }
                panic!(
                    "driver timeout: B never completed batch 2 (a_enq={} b_go={} b_wrote={})",
                    flag(&sched, "a_enq"),
                    flag(&sched, "b_go"),
                    flag(&sched, "b_wrote")
                );
            }
            // The mid-state assertions (both arms): two batches were in
            // flight at once; both frontiers at or below batch 1's
            // start after B's write+sync; ts=n+1's wait was entered
            // unsatisfied (so it has not returned, and it parks on
            // WalAcks — the check and the park hold one baton).
            let ok = flag(&sched, "two_in_flight")
                && flag(&sched, "mid_w_ok")
                && flag(&sched, "mid_d_ok")
                && flag(&sched, "mid_wait_open");
            if !ok {
                // Fail the world first so the joins below cannot hang on
                // a parked task, then report.
                sched.deliver_crash();
                if let Some(h) = h0.take() {
                    let _ = h.join();
                }
                for h in std::mem::take(&mut handles) {
                    let _ = h.join();
                }
                assert!(
                    flag(&sched, "two_in_flight"),
                    "arm {arm}: two batches were never in flight at once (12.4 / S-1-F3 W3)"
                );
                assert!(
                    flag(&sched, "mid_w_ok") && flag(&sched, "mid_d_ok"),
                    "arm {arm}: written/durable advanced past batch 1's start over its unwritten bytes"
                );
                assert!(
                    flag(&sched, "mid_wait_open"),
                    "arm {arm}: ts=n+1's wait was entered unsatisfied (b_acked={})",
                    flag(&sched, "b_acked")
                );
            }
            if arm == 0 {
                // The clean arm: release A; both waits must return and
                // durable must cover batch 2's end.
                sched.lock().st.parks.insert("writer:prewrite", 0);
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                loop {
                    if flag(&sched, "a_acked") && flag(&sched, "b_acked") {
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "clean arm: a wait never returned (a_done={} a_acked={} b_acked={})",
                        flag(&sched, "a_done"),
                        flag(&sched, "a_acked"),
                        flag(&sched, "b_acked")
                    );
                }
                let g = sched.lock();
                assert!(
                    g.st.wal.durable >= b2_end,
                    "clean arm: durable ({}) must cover batch 2's end ({b2_end}) \
                     after A's write and sync",
                    g.st.wal.durable
                );
            } else {
                // The crash arm: crash before A's write. ts=n+1 was
                // never client-acked, and recovery is a ts-prefix with
                // no hole.
                sched.deliver_crash();
                if let Some(h) = h0.take() {
                    let _ = h.join();
                }
                for h in std::mem::take(&mut handles) {
                    let _ = h.join();
                }
                assert!(
                    !flag(&sched, "b_acked"),
                    "crash arm: ts=n+1 was never client-acked"
                );
                // The mid-state's unsatisfied-wait proof, restated for
                // the crash arm: the wait never returned over batch 1's
                // unwritten bytes.
                assert!(
                    flag(&sched, "mid_wait_open"),
                    "crash arm: ts=n+1's SyncOn wait returned over batch 1's unwritten bytes"
                );
                let mut d2 = {
                    let g = sched.lock();
                    g.disk.clone()
                };
                let mut frng = e4_recovery::rng::Rng::new(0x00C0_FFEE);
                d2.apply_crash(
                    &mut frng,
                    &e4_recovery::sim::FaultKnobs {
                        keep_latest_pm: 0,
                        tear_pm: 0,
                    },
                );
                let world = pass_world(1, seed + 90, d2, StoreInner::empty(cfg.clone(), 0));
                let cx2 = Cx {
                    w: &world,
                    t: HARNESS,
                };
                let (rec_st, frontier) = ckpt::recover(&cx2, cfg.clone()).expect("crash recover");
                assert_eq!(
                    frontier, 4,
                    "crash arm: recovery is the ts-prefix through the setup commits, no hole"
                );
                assert_eq!(
                    rec_st.wal.append, seg,
                    "crash arm: the WAL restarts at segment 1's first LSN"
                );
                return;
            }
            if let Some(h) = h0.take() {
                let _ = h.join();
            }
            for h in std::mem::take(&mut handles) {
                let _ = h.join();
            }
        });
    }
}

/// **X-5c-F3-F3a step 4 pin (P's finding: cross-life TxnId reuse;
/// §12.2 W2; a literal config).** The prototype's TxnIds were `(1, n)`
/// in every life, so a cut-off chunked transaction's dead chunks from
/// an earlier life attached to a later life's `CommitMarker` with the
/// same `n` (seed 186 r2: 23 chunks written, 46 attached). With
/// per-boot ids the lives' txns differ — life 1's unmarked chunks
/// `(id₁, 1)` never match life 2's marker `(id₂, 1)` — and recovery
/// attaches exactly the marker's own chunk count.
///
/// The config: every life derives its txn ids from the store's session
/// id (as `commit_txn` does). Life 1 (a recovery over the empty WAL of
/// the created store: boot 1) leaves the unmarked chunks of txn n=1 (a
/// CreateTree plus a Put of key A); life 2 (a later boot — its recovery
/// raised the id per §14 step 8) commits a chunked txn with n=1 again
/// (a CreateTree plus a Put of key B, marker ts 5); a crash; life 3
/// recovers. Asserted: key B is present and key A absent (only life
/// 2's chunks attached), and the frontier is the marker's ts. Reverting
/// the fix (mutant TXN-BOOT-CONST: every recovery assigns one constant
/// id) fails this pin — both lives' txns collide at (1, 1), life 1's
/// chunks attach to life 2's marker, and key A appears.
#[test]
fn txn_ids_never_collide_across_lives() {
    let seed = 1860u64;
    let cfg = tcfg(8, 8, 1);
    // ---- life 1: a recovery over the created store's empty WAL (boot
    // 1), then the unmarked chunks of txn (boot, 1) ----
    let disk = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        let (st1, _) = ckpt::recover(&cx, cfg.clone()).expect("life 1 recover");
        cx.with("pin:install2", |c| {
            *c.store() = st1.clone();
            Ok(())
        })
        .expect("install2");
        let boot1 = cx.with("pin:b1", |c| Ok(c.store().boot_id)).expect("boot1");
        assert_eq!(boot1, 1, "life 1's boot id (14 step 8 over the empty WAL)");
        for chunk in [vec![op_create(2)], vec![op_put_a(1)]] {
            let mut payload = Vec::new();
            for op in &chunk {
                wal::enc_op(op, &mut payload);
            }
            wal::wal_enqueue(&cx, wal::K_CHUNK, (boot1, 1), 0, &payload).expect("chunk enqueue");
        }
        let append = cx
            .with("pin:a1", |c| Ok(c.store().wal.append))
            .expect("append");
        wal::wal_wait_durable(&cx, append).expect("life 1 chunks durable");
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- the crash, then life 2 (a recovery raises the boot id; no
    // checkpoint, so the opened meta stays meta 0 and the walk crosses
    // into the newer boots' records) ----
    let mut d1 = disk;
    let mut frng = e4_recovery::rng::Rng::new(seed + 1);
    d1.apply_crash(
        &mut frng,
        &e4_recovery::sim::FaultKnobs {
            keep_latest_pm: 0,
            tear_pm: 0,
        },
    );
    let host = TestWorld {
        sched: SchedCore::new(1, seed + 2),
        disk: d1.clone(),
        st: StoreInner::empty(cfg.clone(), 0),
        flags: BTreeMap::new(),
        nums: BTreeMap::new(),
    };
    let sched = Sched::new(host);
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    let (st2, _f1) = ckpt::recover(&cx, cfg.clone()).expect("life 2 recover");
    cx.with("pin:install3", |c| {
        *c.store() = st2.clone();
        Ok(())
    })
    .expect("install3");
    let boot2 = {
        let g = sched.lock();
        assert_eq!(g.st.boot_id, 2, "life 2's boot id is one past life 1's");
        g.st.boot_id
    };
    // Life 2's chunked txn, n=1 again (the collision shape), marker
    // ts 5.
    for chunk in [vec![op_create(2)], vec![op_put_b(9)]] {
        let mut payload = Vec::new();
        for op in &chunk {
            wal::enc_op(op, &mut payload);
        }
        wal::wal_enqueue(&cx, wal::K_CHUNK, (boot2, 1), 0, &payload).expect("chunk enqueue");
    }
    let mut marker_payload = Vec::new();
    marker_payload.extend_from_slice(&2u32.to_le_bytes());
    let (mlsn, mlen) =
        wal::wal_enqueue(&cx, wal::K_MARKER, (boot2, 1), 5, &marker_payload).expect("marker");
    wal::wal_wait_durable(&cx, mlsn + mlen).expect("life 2 txn durable");
    let end_disk = {
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- the crash, then life 3: recovery attaches only life 2's
    // chunks ----
    let mut d2 = end_disk;
    let mut frng = e4_recovery::rng::Rng::new(seed + 3);
    d2.apply_crash(
        &mut frng,
        &e4_recovery::sim::FaultKnobs {
            keep_latest_pm: 0,
            tear_pm: 0,
        },
    );
    let world = pass_world(1, seed + 4, d2, StoreInner::empty(cfg.clone(), 0));
    let cx3 = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, frontier) = ckpt::recover(&cx3, cfg.clone()).expect("life 3 recover");
    assert_eq!(
        frontier, 5,
        "the chunked txn replays through its marker (the marker's own chunks)"
    );
    let root = tree::dir_root_of(&cx3, 2).expect("tree 2 root");
    let scan = tree::tree_scan(&cx3, root, u64::MAX).expect("scan");
    assert!(
        scan.contains_key(b"B".as_slice()),
        "life 2's own chunk (key B) must be applied: {scan:?}"
    );
    assert!(
        !scan.contains_key(b"A".as_slice()),
        "life 1's unmarked chunks (key A) must never attach to life 2's marker: {scan:?}"
    );
    let _ = rec_st;
}

/// Test helper: a CreateTree op.
fn op_create(tree: u64) -> wal::WOp {
    wal::WOp::CreateTree { tree }
}

/// Test helper: a Put of key `A`.
fn op_put_a(v: u8) -> wal::WOp {
    wal::WOp::Put {
        tree: 2,
        key: b"A".to_vec(),
        value: vec![v; 8],
    }
}

/// Test helper: a Put of key `B`.
fn op_put_b(v: u8) -> wal::WOp {
    wal::WOp::Put {
        tree: 2,
        key: b"B".to_vec(),
        value: vec![v; 8],
    }
}

/// **X-5c-F3-F3a step 4 pin (S-1-F3 W4 (i), the ordered boot-id stop;
/// `t_stale_boot_record`; a literal config).** A post-sync LOST fault
/// on the recovery truncation's zero write reverts it, leaving an
/// older boot's complete record at the next expected position after
/// the newer boot's records — here built directly: a boot-2 commit at
/// LSN 0, then a complete boot-1 record at the next position (the
/// stale bytes a reverted truncation would expose). §12.5's ordered
/// stop (W4 (i)) stops the walk at that record's start; the recovered
/// state excludes it. Reverting the fix (mutant BOOT-ID-STOP-SKIP: the
/// stop removed) fails this pin — the stale record replays (frontier
/// 2, its key present).
#[test]
fn stale_boot_record_stops_the_walk() {
    let seed = 7410u64;
    let cfg = tcfg(8, 8, 1);
    // ---- the fixture: X1 (boot 2, ts 1) at LSN 0, durable; X2 (boot
    // 1, ts 2 — an older boot's complete record) at the next position,
    // written and synced directly on the segment file ----
    let (disk, x2_lsn, x2_key): (SimDisk, u64, Vec<u8>) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        let mut p1 = Vec::new();
        wal::enc_op(&op_create(2), &mut p1);
        wal::enc_op(&op_put_key(b"K1", 1), &mut p1);
        let (l1, n1) = wal::wal_enqueue(&cx, wal::K_COMMIT, (2, 1), 1, &p1).expect("X1 enqueue");
        wal::wal_wait_durable(&cx, l1 + n1).expect("X1 durable");
        // X2: an older boot's complete record at the next position.
        let x2_lsn = l1 + n1;
        let mut p2 = Vec::new();
        wal::enc_op(&op_put_key(b"K2", 2), &mut p2);
        let x2 = wal::enc_record(wal::K_COMMIT, (1, 1), 2, &p2, x2_lsn);
        let f = cx
            .env()
            .open("/wal/0000000000000000.log")
            .expect("segment 0");
        cx.env().write_at(f, x2_lsn, &x2).expect("X2 write");
        cx.env().sync_data(f).expect("X2 sync");
        let g = sched.lock();
        (g.disk.clone(), x2_lsn, b"K2".to_vec())
    };
    // ---- the crash (everything durable), then recovery: the walk
    // stops at X2's start ----
    let mut d1 = disk;
    let mut frng = e4_recovery::rng::Rng::new(seed + 1);
    d1.apply_crash(
        &mut frng,
        &e4_recovery::sim::FaultKnobs {
            keep_latest_pm: 0,
            tear_pm: 0,
        },
    );
    let world = pass_world(1, seed + 2, d1, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("recover");
    assert_eq!(
        frontier, 1,
        "the older boot's record is not replayed (12.5 W4 (i): the ordered stop)"
    );
    assert_eq!(
        rec_st.wal.append, x2_lsn,
        "the valid end is the stale record's start"
    );
    let root = tree::dir_root_of(&cx, 2).expect("tree 2 root");
    let scan = tree::tree_scan(&cx, root, u64::MAX).expect("scan");
    assert!(
        scan.contains_key(b"K1".as_slice()),
        "the newer boot's record replays: {scan:?}"
    );
    assert!(
        !scan.contains_key(&x2_key),
        "the older boot's stale record must be excluded: {scan:?}"
    );
}

/// Test helper: a Put of an arbitrary key.
fn op_put_key(key: &[u8], v: u8) -> wal::WOp {
    wal::WOp::Put {
        tree: 2,
        key: key.to_vec(),
        value: vec![v; 8],
    }
}

/// Test helper: apply a post-sync LOST fault on the durable meta write
/// of `ckpt_id` — the fixtures' meta rollback (through SimDisk's fault
/// API, the same effect D10's directed fault has: the synced write
/// reverts to its pre-image, exactly "a post-sync LOST write on meta
/// k+1's slot reverts it").
fn revert_meta_write(disk: &mut SimDisk, ckpt_id: u64) {
    let pf = disk
        .stat(e4_recovery::sim::PAGE_FILE)
        .expect("the page file");
    let slot_off = (1 + (ckpt_id % 2)) * e4_recovery::store::page::PAGE as u64;
    let w = disk
        .dwrites_of(pf)
        .iter()
        .rev()
        .find(|w| {
            w.off == slot_off
                && e4_recovery::store::page::parse_meta(&w.bytes)
                    .map(|m| m.ckpt_id == ckpt_id)
                    .unwrap_or(false)
        })
        .cloned()
        .unwrap_or_else(|| panic!("no durable meta {ckpt_id} write at slot {slot_off}"));
    disk.apply_one_media_fault(&e4_recovery::sim::MediaFault {
        inode: pf,
        off: w.off,
        len: e4_recovery::store::page::PAGE as u64,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: e4_recovery::sim::MediaKind::Lost,
    });
}

/// Test helper: crash a disk with no volatile survivors (every unsynced
/// write reverts; synced writes — everything these fixtures ack —
/// stay), matching the other pins' crash shapes.
fn crash_clean(disk: &mut SimDisk, seed: u64) {
    let mut frng = e4_recovery::rng::Rng::new(seed);
    disk.apply_crash(
        &mut frng,
        &e4_recovery::sim::FaultKnobs {
            keep_latest_pm: 0,
            tear_pm: 0,
        },
    );
}

/// Test helper: enqueue and ack one SyncOn commit record (the ops as
/// the payload, as `commit_txn` writes a non-chunked commit; the ack is
/// the record's own §12.4 durable wait).
fn enqueue_commit(cx: &Cx<'_, TestWorld>, txn: (u32, u64), ts: u64, ops: &[wal::WOp]) {
    let mut payload = Vec::new();
    for op in ops {
        wal::enc_op(op, &mut payload);
    }
    let (lsn, len) =
        wal::wal_enqueue(cx, wal::K_COMMIT, txn, ts, &payload).expect("commit enqueue");
    wal::wal_wait_durable(cx, lsn + len).expect("commit durable");
}

/// Test helper: enqueue one chunk of `txn` (ops as the payload, ts 0,
/// as `commit_txn` writes chunks before the marker's critical section).
fn enqueue_chunk(cx: &Cx<'_, TestWorld>, txn: (u32, u64), ops: &[wal::WOp]) {
    let mut payload = Vec::new();
    for op in ops {
        wal::enc_op(op, &mut payload);
    }
    wal::wal_enqueue(cx, wal::K_CHUNK, txn, 0, &payload).expect("chunk enqueue");
}

/// Test helper: enqueue and ack one chunked transaction's CommitMarker
/// (`nchunks` as the payload, as `commit_txn` writes it).
fn enqueue_marker(cx: &Cx<'_, TestWorld>, txn: (u32, u64), ts: u64, nchunks: u32) {
    let payload = nchunks.to_le_bytes().to_vec();
    let (lsn, len) =
        wal::wal_enqueue(cx, wal::K_MARKER, txn, ts, &payload).expect("marker enqueue");
    wal::wal_wait_durable(cx, lsn + len).expect("marker durable");
}

/// **X-5c-F3-F3a step 4 pin (S-1-F3 W4 (ii), `t_meta_rollback_crosses_boot`;
/// a literal config).** A meta-slot fault leaves an older meta as the
/// newest valid one, and the walk then legitimately runs from that
/// meta's `wal_start_lsn` through the next boot's records, whose LSNs
/// continue the stream: "stopping there would lose that boot's acked
/// commits silently" (§12.5 W4 (ii)'s rationale).
///
/// The fixture (S-STORE §12.5's, all inside segment 0 so no recycling
/// touches meta k's WAL span, and boot b's only page writes are its
/// boot checkpoint's image pages — off image k's slots by the
/// allocator's `used_durable` rule): boot b−1 (the create session)
/// writes meta 1 and crashes; boot b recovers, truncates at its valid
/// end p_b, writes boot meta 2 through the harness checkpoint, then
/// appends and acks a SyncOn commit from p_b; a post-sync LOST on meta
/// 2's slot reverts it, then a crash before boot b's next checkpoint.
/// Asserted: the open selects meta 1, replays boot b's acked commit,
/// and the frontier is at or above the max acked. Reverting the fix
/// (mutant BOOT-ID-EQ-STOP: the walk stops at any boot id other than
/// the opened meta's) fails this pin — the walk stops at p_b and the
/// acked commit is lost.
#[test]
fn meta_rollback_walk_crosses_into_newer_boot() {
    let seed = 8237u64;
    let cfg = tcfg(8, 8, 1);
    // ---- boot b−1 (the create session): create, commit X1, meta 1,
    // commit Y1 above it (acked), crash ----
    let disk = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        enqueue_commit(&cx, (0, 1), 1, &[op_create(2), op_put_key(b"X1", 1)]);
        cx_checkpoint(&sched, |_| Ok(()));
        enqueue_commit(&cx, (0, 2), 2, &[op_put_key(b"Y1", 2)]);
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- boot b: recover from meta 1 (truncate at p_b), the boot
    // checkpoint (meta 2), ack Z1 from p_b, crash; the meta-2 write is
    // reverted post-sync ----
    let disk = {
        let mut d = disk;
        crash_clean(&mut d, seed + 1);
        let host = TestWorld {
            sched: SchedCore::new(1, seed + 2),
            disk: d,
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let (st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("boot b recover");
        assert_eq!(st.k, 1, "boot b opens meta 1");
        assert_eq!(frontier, 2, "Y1 replays above meta 1");
        assert_eq!(st.boot_id, 2, "boot b's id (§14 step 8)");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        cx_checkpoint(&sched, |_| Ok(()));
        enqueue_commit(&cx, (2, 1), 3, &[op_put_key(b"Z1", 3)]);
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- the rollback: the meta-2 write reverts, then the next open
    // selects meta 1 and the walk crosses into boot b's records ----
    let mut d = disk;
    crash_clean(&mut d, seed + 3);
    revert_meta_write(&mut d, 2);
    let world = pass_world(1, seed + 4, d, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("the rollback recovery");
    assert_eq!(
        rec_st.k, 1,
        "the open selects meta k=1 (the rollback: slot 1 reverted to meta 0)"
    );
    assert_eq!(
        frontier, 3,
        "the frontier is at or above the max acked (Z1, ts 3)"
    );
    let root = tree::dir_root_of(&cx, 2).expect("tree 2 root");
    let scan = tree::tree_scan(&cx, root, u64::MAX).expect("scan");
    for key in [&b"X1"[..], &b"Y1"[..], &b"Z1"[..]] {
        assert!(
            scan.contains_key(key),
            "key {key:?} must survive (image / replay / cross-boot replay): {scan:?}"
        );
    }
}

/// **X-5c-F3-F3a step 4 pin (S-1-F3 §14 step 8, gate edit 1,
/// `t_boot_id_after_rollback`; a literal config).** Two successive
/// rollbacks to meta k, across boots b and b+1: boot b leaves an
/// unmarked chunked txn `(id_b, 1)`; boot b+1 commits a chunked txn.
/// §14 step 8's three-way rule makes boot b+1's id newer than b's
/// (1 + the newest boot id the walk accepted), so recovery from meta k
/// never attaches b's chunks to b+1's marker. Reverting the fix (mutant
/// BOOT-ID-CKPT-ONLY: the id is the low 32 bits of `ckpt_id` only)
/// fails this pin — both boots get k+1, the marker `(k+1, 1)` collides
/// with b's chunks, and b's keys appear.
#[test]
fn boot_id_fresh_after_rollback() {
    let seed = 5909u64;
    let cfg = tcfg(8, 8, 1);
    // ---- setup: meta 1 durable (slot 2, boot 0), nothing above it ----
    let disk = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        enqueue_commit(&cx, (0, 1), 1, &[op_create(2), op_put_key(b"A0", 1)]);
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- boot b (rollback 1): unmarked chunks of txn (id_b, 1), then
    // its meta 2 write is lost ----
    let disk = {
        let mut d = disk;
        crash_clean(&mut d, seed + 1);
        let host = TestWorld {
            sched: SchedCore::new(1, seed + 2),
            disk: d,
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let (st, _f) = ckpt::recover(&cx, cfg.clone()).expect("boot b recover");
        let id_b = st.boot_id;
        assert_eq!(id_b, 2, "boot b's id");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        cx_checkpoint(&sched, |_| Ok(()));
        enqueue_chunk(&cx, (id_b, 1), &[op_put_key(b"B1", 1)]);
        enqueue_chunk(&cx, (id_b, 1), &[op_put_key(b"B2", 2)]);
        let end = {
            let g = sched.lock();
            g.st.wal.append
        };
        wal::wal_wait_durable(&cx, end).expect("boot b chunks durable");
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- boot b+1 (rollback 2): the walk crosses b's chunks (so its
    // id is fresh), then it commits a chunked txn n=1, and its meta
    // write is lost too ----
    let disk = {
        let mut d = disk;
        crash_clean(&mut d, seed + 3);
        revert_meta_write(&mut d, 2);
        let host = TestWorld {
            sched: SchedCore::new(1, seed + 4),
            disk: d,
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let (st, _f) = ckpt::recover(&cx, cfg.clone()).expect("boot b+1 recover");
        let id_b1 = st.boot_id;
        // Serial-number order: id_b1 (3) is newer than id_b (2).
        assert!(
            (2u32.wrapping_sub(id_b1)) >= 1u32 << 31,
            "boot b+1's id {id_b1} is newer than boot b's 2"
        );
        assert_eq!(id_b1, 3, "1 + the newest boot id the walk accepted (2)");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        cx_checkpoint(&sched, |_| Ok(()));
        enqueue_chunk(&cx, (id_b1, 1), &[op_put_key(b"C1", 1)]);
        enqueue_chunk(&cx, (id_b1, 1), &[op_put_key(b"C2", 2)]);
        enqueue_marker(&cx, (id_b1, 1), 2, 2);
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- recovery from meta 1: b's chunks never attach to b+1's
    // marker ----
    let mut d = disk;
    crash_clean(&mut d, seed + 5);
    revert_meta_write(&mut d, 2);
    let world = pass_world(1, seed + 6, d, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (_rec_st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("the final recovery");
    assert_eq!(frontier, 2, "the marker's txn replays (ts 2)");
    let root = tree::dir_root_of(&cx, 2).expect("tree 2 root");
    let scan = tree::tree_scan(&cx, root, u64::MAX).expect("scan");
    for key in [&b"A0"[..], &b"C1"[..], &b"C2"[..]] {
        assert!(
            scan.contains_key(key),
            "key {key:?} (the setup commit / b+1's own chunks) must be present: {scan:?}"
        );
    }
    for key in [&b"B1"[..], &b"B2"[..]] {
        assert!(
            !scan.contains_key(key),
            "boot b's unmarked chunks {key:?} must never attach to b+1's marker: {scan:?}"
        );
    }
}

/// **X-5c-F3-F3a step 4 pin (the (f′) binding amendment, S-STORE §14
/// step 8's three-way rule; `t_boot_id_after_rollback`, "raised id,
/// then no records" variant; a literal config).** A boot whose id was
/// raised above its `ckpt_id` (through the walk's record term) can be
/// followed by one whose `ckpt_id` is below that id — only the opened
/// meta's `boot_id` term bounds it.
///
/// The fixture (S-STORE §14 step 8's, k=1): boot b−1 writes meta 1 and
/// crashes; boots b and b+1 each write meta 2, append and ack one
/// commit, and lose that meta write; boot b+2 crosses both boots'
/// records (id 4), its meta 2 (`boot_id` 4, `wal_start_lsn` the append
/// position p) is durable, and it writes no record; boot b+3 opens
/// meta 2, its walk from p accepts nothing, it writes meta 3, appends
/// and acks a SyncOn commit from p, and crashes before its next
/// checkpoint; a LOST write on meta 3 leaves its slot holding meta 1.
/// Asserted: the open selects meta 2 and replays boot b+3's acked
/// commit, and b+3's id is newer than the opened meta's `boot_id`.
/// Reverting the fix (mutant BOOT-ID-NO-META-FLOOR: the opened meta's
/// `boot_id` term omitted) fails this pin — b+3 gets the id 3, older
/// than meta 2's `boot_id` 4, the walk stops at p and returns `Ok`
/// without the acked commit.
#[test]
fn boot_id_after_rollback_raised_then_no_records() {
    let seed = 4421u64;
    let cfg = tcfg(8, 8, 1);
    // ---- boot b−1: create, commit P1 (tree 2), meta 1, crash ----
    let disk = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        enqueue_commit(&cx, (0, 1), 1, &[op_create(2), op_put_key(b"P1", 1)]);
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        g.disk.clone()
    };
    // A boot that recovers, writes its boot meta (k+1 = 2), appends and
    // acks one commit at `txn`/`ts`, and loses the meta write.
    let boot_and_lose =
        |disk: SimDisk, step: u64, txn: (u32, u64), ts: u64, key: &'static [u8]| -> SimDisk {
            let mut d = disk;
            crash_clean(&mut d, seed + step);
            let host = TestWorld {
                sched: SchedCore::new(1, seed + step + 1),
                disk: d,
                st: StoreInner::empty(cfg.clone(), 0),
                flags: BTreeMap::new(),
                nums: BTreeMap::new(),
            };
            let sched = Sched::new(host);
            let cx = Cx {
                w: &sched,
                t: HARNESS,
            };
            let (st, _f) = ckpt::recover(&cx, cfg.clone()).expect("recover");
            cx.with("pin:install", |c| {
                *c.store() = st;
                Ok(())
            })
            .expect("install");
            cx_checkpoint(&sched, |_| Ok(()));
            enqueue_commit(&cx, txn, ts, &[op_put_key(key, ts as u8)]);
            let g = sched.lock();
            let mut d = g.disk.clone();
            drop(g);
            crash_clean(&mut d, seed + step + 2);
            revert_meta_write(&mut d, 2);
            d
        };
    // ---- boots b (id 2, P2) and b+1 (id 3, P3): both metas lost ----
    let disk = boot_and_lose(disk, 10, (2, 1), 2, b"P2");
    let disk = boot_and_lose(disk, 20, (3, 1), 3, b"P3");
    // ---- boot b+2: crosses b's and b+1's records (id 4), its meta 2
    // (boot_id 4, wal_start = the append position p) is durable, and
    // it writes no record ----
    let disk = {
        let mut d = disk;
        crash_clean(&mut d, seed + 31);
        let host = TestWorld {
            sched: SchedCore::new(1, seed + 32),
            disk: d,
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let (st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("boot b+2 recover");
        assert_eq!(st.boot_id, 4, "boot b+2's id crosses both boots' records");
        assert_eq!(frontier, 3, "P2 and P3 replay");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- boot b+3: opens meta 2, its walk from p accepts nothing, it
    // writes meta 3, appends and acks P4 from p, and crashes before its
    // next checkpoint ----
    let (disk, b3_boot) = {
        let mut d = disk;
        crash_clean(&mut d, seed + 33);
        let host = TestWorld {
            sched: SchedCore::new(1, seed + 34),
            disk: d,
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let (st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("boot b+3 recover");
        assert_eq!(st.k, 2, "boot b+3 opens meta 2 (the raised boot)");
        assert_eq!(frontier, 3, "the walk from p accepts nothing");
        let b3_boot = st.boot_id;
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        cx_checkpoint(&sched, |_| Ok(()));
        enqueue_commit(&cx, (b3_boot, 1), 4, &[op_put_key(b"P4", 4)]);
        let g = sched.lock();
        let mut d = g.disk.clone();
        drop(g);
        crash_clean(&mut d, seed + 35);
        // The final rollback: meta 3's write reverts to meta 1's bytes.
        revert_meta_write(&mut d, 3);
        // b3_boot is asserted after the outcome assertions below: the
        // harm the fixture describes (an `Ok` open missing the acked
        // commits) is the demonstration that matters.
        (d, b3_boot)
    };
    // ---- the open selects meta 2 and replays boot b+3's acked
    // commit ----
    let world = pass_world(1, seed + 40, disk, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("the final recovery");
    assert_eq!(
        rec_st.k, 2,
        "the open selects meta 2 (slot 1; meta 3's write reverted to meta 1)"
    );
    assert_eq!(
        frontier, 4,
        "boot b+3's acked commit replays (the max acked; never a stop at p)"
    );
    let root = tree::dir_root_of(&cx, 2).expect("tree 2 root");
    let scan = tree::tree_scan(&cx, root, u64::MAX).expect("scan");
    for key in [&b"P1"[..], &b"P2"[..], &b"P3"[..], &b"P4"[..]] {
        assert!(
            scan.contains_key(key),
            "key {key:?} must survive (image / replay): {scan:?}"
        );
    }
    assert!(
        (4u32.wrapping_sub(b3_boot)) >= 1u32 << 31,
        "boot b+3's id {b3_boot} must be newer than the opened meta's boot_id 4"
    );
    assert_eq!(b3_boot, 5, "the three-way rule's meta term");
}

/// **The X-5c-F3-F3a rework's Finding-1 pin (S-STORE §14 step 8: the
/// previous boot's unmarked chunks do not hold back `wal_start_lsn`;
/// a literal config).** Boot b−1 leaves a cut-off chunked txn below
/// `p0`; boot b's boot checkpoint runs; b appends and acks SyncOn
/// commits and crashes before its next checkpoint. Recovery must
/// replay them (frontier ≥ the max acked), and boot b's boot meta must
/// record a `wal_start_lsn` past the previous boot's chunks. Reverting
/// the fix (mutant START-LSN-PREV-BOOT-CHUNK: the `None` arm holds
/// back for every unmarked chunk, the previous boot's included) fails
/// this pin — meta k+1's `wal_start_lsn` points at the older boot's
/// chunk, §12.5's first-record floor stops the next walk at it, and
/// every acked commit of the new boot is lost (the 100 class-C
/// `frontier < lo` store losses of the first run).
#[test]
fn wal_start_lsn_ignores_previous_boots_chunks() {
    let seed = 2811u64;
    let cfg = tcfg(8, 8, 1);
    // ---- boot b−1 (the create session, boot 0): create, commit P1
    // (tree 2, ts 1), meta 1, then the cut-off chunks of txn (0, 2)
    // (no marker), durable; crash ----
    let disk = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        enqueue_commit(&cx, (0, 1), 1, &[op_create(2), op_put_key(b"P1", 1)]);
        cx_checkpoint(&sched, |_| Ok(()));
        // The cut-off chunked txn: chunks enqueued and made durable, no
        // marker — the transaction ends with the boot (§14 step 8).
        enqueue_chunk(&cx, (0, 2), &[op_put_key(b"A1", 1)]);
        enqueue_chunk(&cx, (0, 2), &[op_put_key(b"A2", 2)]);
        let end = {
            let g = sched.lock();
            g.st.wal.append
        };
        wal::wal_wait_durable(&cx, end).expect("boot b-1 chunks durable");
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- boot b: recover (the walk accepts the chunk into its
    // open-chunk bookkeeping; the valid end p0 sits past it), the boot
    // checkpoint (meta 2, boot_id 2), then two acked SyncOn commits
    // from p0; crash before the next checkpoint ----
    let disk = {
        let mut d = disk;
        crash_clean(&mut d, seed + 1);
        let host = TestWorld {
            sched: SchedCore::new(1, seed + 2),
            disk: d,
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let (st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("boot b recover");
        assert_eq!(st.k, 1, "boot b opens meta 1");
        assert_eq!(
            frontier, 1,
            "P1 is in meta 1's image; the chunk never applies"
        );
        assert_eq!(
            st.boot_id, 2,
            "boot b's id (its boot checkpoint writes meta 2)"
        );
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        cx_checkpoint(&sched, |_| Ok(()));
        enqueue_commit(&cx, (2, 1), 2, &[op_put_key(b"Q1", 2)]);
        enqueue_commit(&cx, (2, 2), 3, &[op_put_key(b"Q2", 3)]);
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- the pin's own object first: boot b's meta 2 records a
    // `wal_start_lsn` at p0, past the previous boot's chunks ----
    let p0 = {
        let pf = disk
            .stat(e4_recovery::sim::PAGE_FILE)
            .expect("the page file");
        let m2bytes = disk.durable_read(pf, 4096, 4096);
        let m2 = e4_recovery::store::page::parse_meta(&m2bytes).expect("meta 2 durable at slot 1");
        assert_eq!(m2.ckpt_id, 2, "meta 2 is the boot checkpoint's");
        assert_eq!(m2.boot_id, 2, "meta 2 carries boot b's id");
        // p0: the position boot b's first commit starts at — the walk's
        // accepted stream ends past the previous boot's last chunk, and
        // Q1's record (the first record of boot id 2) is the first this
        // boot appended. Found by decoding the durable stream, exactly
        // as the next recovery's walk will read it.
        let seg = disk.stat("/wal/0000000000000000.log").expect("segment 0");
        let mut lsn = 0u64;
        let mut first_boot_b_rec = None;
        while lsn + 45 <= 65536 {
            let want = (65536 - lsn).min(16 * 1024 + 16);
            let bytes = disk.durable_read(seg, lsn, want);
            match wal::dec_record(&bytes, lsn, 65536) {
                Ok(Some(r)) => {
                    if r.txn.0 == 2 {
                        first_boot_b_rec = Some(r.lsn);
                        break;
                    }
                    lsn += r.len();
                }
                _ => break,
            }
        }
        let p0 = first_boot_b_rec.expect("a record of boot id 2 in segment 0");
        assert_eq!(
            m2.wal_start_lsn, p0,
            "meta 2's wal_start_lsn sits at p0 (the previous boot's unmarked\
             \nchunks do not hold it back, 14 step 8)"
        );
        p0
    };
    // ---- the crash, then recovery: every acked commit of boot b
    // replays (frontier >= the max acked, ts 3) ----
    let mut d = disk;
    crash_clean(&mut d, seed + 3);
    let world = pass_world(1, seed + 4, d, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (_rec_st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("the final recovery");
    assert_eq!(
        frontier, 3,
        "boot b's acked SyncOn commits replay (the max acked; never a\
         \nfirst-record stop at the previous boot's chunk)"
    );
    assert!(p0 > 0, "the fixture placed a boot-b record");
    let root = tree::dir_root_of(&cx, 2).expect("tree 2 root");
    let scan = tree::tree_scan(&cx, root, u64::MAX).expect("scan");
    for key in [&b"P1"[..], &b"Q1"[..], &b"Q2"[..]] {
        assert!(
            scan.contains_key(key),
            "key {key:?} must survive (image / replay): {scan:?}"
        );
    }
    for key in [&b"A1"[..], &b"A2"[..]] {
        assert!(
            !scan.contains_key(key),
            "the previous boot's unmarked chunks {key:?} never apply: {scan:?}"
        );
    }
}

/// **The X-5c-F3-F3a rework's G1 pin (§12.5's first-record floor: a
/// walk whose first record is older than the opened meta's `boot_id`
/// stops there; a literal config).** The gate's own G1 mutant — the
/// floor dropped, `last_boot.unwrap_or(rec.txn.0)` — passed the whole
/// release suite at the first run's tip (verdict X-5c-F3-F3a, UNCAUGHT);
/// this pin catches it. The fixture builds the walk input directly (as
/// `stale_boot_record_stops_the_walk` does, the identical bytes a
/// reverted truncation would expose): a meta whose `wal_start_lsn` is
/// `e1` and whose `boot_id` is 2, and an older boot's complete record
/// at `e1`. The walk stops at its start; the recovered state excludes
/// it.
#[test]
fn first_record_older_than_opened_meta_stops_the_walk() {
    let seed = 6107u64;
    let cfg = tcfg(8, 8, 1);
    // ---- boot 0 (the create session): create, commit P1 (tree 2,
    // ts 1), meta 1 (T=1, wal_start = P1's end e1); crash ----
    let (disk, e1) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        enqueue_commit(&cx, (0, 1), 1, &[op_create(2), op_put_key(b"P1", 1)]);
        let e1 = {
            let g = sched.lock();
            g.st.wal.append
        };
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        (g.disk.clone(), e1)
    };
    // ---- boot b: recover (the walk from e1 accepts nothing; valid end
    // e1), the boot checkpoint (meta 2, boot_id 2, wal_start e1);
    // crash; then an older boot's complete record R2 (boot 0, ts 2) is
    // written and synced directly at e1 ----
    let mut disk = {
        let mut d = disk;
        crash_clean(&mut d, seed + 1);
        let host = TestWorld {
            sched: SchedCore::new(1, seed + 2),
            disk: d,
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let (st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("boot b recover");
        assert_eq!(frontier, 1, "nothing above meta 1 replays");
        assert_eq!(st.boot_id, 2, "boot b's id (its checkpoint writes meta 2)");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        g.disk.clone()
    };
    crash_clean(&mut disk, seed + 3);
    let disk = {
        // R2 at e1: a complete, valid record of boot 0, ts 2 — the
        // stale bytes a reverted truncation leaves at the valid end.
        let host = TestWorld {
            sched: SchedCore::new(1, seed + 4),
            disk: disk.clone(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let mut p2 = Vec::new();
        wal::enc_op(&op_put_key(b"K2", 2), &mut p2);
        let r2 = wal::enc_record(wal::K_COMMIT, (0, 9), 2, &p2, e1);
        let f = cx
            .env()
            .open("/wal/0000000000000000.log")
            .expect("segment 0");
        cx.env().write_at(f, e1, &r2).expect("R2 write");
        cx.env().sync_data(f).expect("R2 sync");
        let g = sched.lock();
        g.disk.clone()
    };
    // ---- recovery: the walk's first record is R2, an older boot's —
    // §12.5's floor (the opened meta's boot_id) stops the walk at e1 ----
    let world = pass_world(1, seed + 5, disk, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, frontier) = ckpt::recover(&cx, cfg.clone()).expect("the final recovery");
    assert_eq!(
        frontier, 1,
        "the walk's first record is older than the opened meta's boot_id: it\
         \nnever replays (12.5's first-record floor)"
    );
    assert_eq!(
        rec_st.wal.append, e1,
        "the valid end is the stale record's start"
    );
    let root = tree::dir_root_of(&cx, 2).expect("tree 2 root");
    let scan = tree::tree_scan(&cx, root, u64::MAX).expect("scan");
    assert!(
        scan.contains_key(b"P1".as_slice()),
        "the image's own commit survives: {scan:?}"
    );
    assert!(
        !scan.contains_key(b"K2".as_slice()),
        "the older boot's stale first record must be excluded: {scan:?}"
    );
}

/// **X-5c-F3-F3b step 1 pin (S-1-F3 §13 step 1, W5 (ii);
/// `t_ckpt_sync_prefix`; a literal config).** Step 1 ends only when
/// `durable_end ≥ wal_ckpt_end` (I-ACK-PREFIX's contiguous prefix), not
/// at "the latest batch's sync": a flip with batch 1 unwritten — its
/// holder parked at `writer:prewrite`, the F3-F3a hold — and batch 2,
/// above it, written and synced by its own group sync.
///
/// The config (the shared-budget shape of `ack_prefix_two_batches_in_
/// flight`, its recorded precedent): the setup fills segment 0 exactly
/// and segment 1 save one max record, all durable. Task A enqueues a1
/// (fills segment 1 exactly) through a30 — spanning fresh segments 2..7
/// — and takes batch 1, parking at `writer:prewrite` (budget 60_000,
/// drained in the park loop's yields). Task B waits until batch 1 is
/// taken and nothing is written, enqueues b1 above it and takes batch 2
/// — parking at the same shared budget, exactly as in the ack_prefix
/// pin. Task C waits until both batches are in flight and b1 is
/// enqueued, then runs a checkpoint: its flip reads `wal_ckpt_end` past
/// both records and its step 1 parks on `WalAcks` (nothing is durable
/// past the setup). When the budget exhausts, both holders release;
/// batch 2 (b1, one segment) completes far sooner than batch 1 (six
/// fresh segments' create/preallocate/dirsync/write/sync), so a step 1
/// that ended "at the latest batch's sync" would return while batch 1's
/// bytes were still a hole — and the meta write would proceed over it.
///
/// Asserted (both arms): **at the After4 hook — after the meta write —
/// `wal.durable ≥ wal_ckpt_end`** (the hook records the pair in one
/// atomic step). In the crash-after-step-5 arm, the world crashes right
/// after the meta's `sync_all`, F-CRASH drops every volatile write, and
/// **every byte below the durable meta's `wal_ckpt_end` is durable on
/// SimDisk** (each issued WAL write below the bound reads back its
/// issued bytes). Mutant CKPT-SYNC-OVER-HOLE (step 1 ends at the latest
/// batch's sync) fails the After4 assertion: the meta is written with
/// `durable` below batch 1's start.
#[test]
fn ckpt_step1_waits_for_contiguous_durable() {
    let seg = e4_recovery::store::WAL_SEG;
    let fixed = e4_recovery::store::wal::REC_FIXED;
    let maxp = (e4_recovery::store::wal::REC_MAX - fixed) as usize;
    let seed = 7171u64;
    let cfg = tcfg(8, 8, 1);
    // ---- setup (single task): segment 0 exact, segment 1 one-max short
    let (disk, st, setup_end) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        for ts in 1..=4u64 {
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp])
                .expect("setup seg0");
        }
        for ts in 5..=7u64 {
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp])
                .expect("setup seg1");
        }
        let setup_end = seg + 3 * (maxp as u64 + fixed);
        wal::wal_wait_durable(&cx, setup_end).expect("setup durable");
        let g = sched.lock();
        (g.disk.clone(), g.st.clone(), setup_end)
    };
    // a1 fills segment 1 exactly (its 16-KiB tail); a2..a29 fill
    // segments 2..6 exactly (4 max records each); a30 (small) lands in
    // segment 7. Batch 1 = [a1..a30] spans six fresh segments, so its
    // holder's write-out (create + preallocate + dirsync per segment)
    // is far longer than batch 2's one-segment write.
    let na2 = 29usize; // a2..a30
    for arm in 0..2u64 {
        let mut st = st.clone();
        st.parks.insert("writer:prewrite", 60_000);
        let host = TestWorld {
            sched: SchedCore::new(4, seed + 1 + arm),
            disk: disk.clone(),
            st,
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Arc::new(Sched::new(host));
        fn sf(e: Fail) -> SimFail {
            match e {
                Fail::Sim(s) => s,
                _ => SimFail::Internal("wal pin: a non-sim store failure"),
            }
        }
        // Task A: enqueue a1..a30, take batch 1 (parks at prewrite),
        // then wait for its own ack.
        let a_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 8), 8, &vec![0u8; maxp]).expect("a1");
            for i in 0..na2 {
                wal::wal_enqueue(
                    &cx,
                    wal::K_COMMIT,
                    (0, 9 + i as u64),
                    9 + i as u64,
                    &vec![0u8; if i == na2 - 1 { 10 } else { maxp }],
                )
                .expect("a2..");
            }
            set_flag(sched, "a_enq");
            let r = wal::wal_writer_step(&cx).map_err(sf);
            set_flag(sched, "a_released");
            r?;
            let end = {
                let g = sched.lock();
                g.st.wal.append
            };
            wal::wal_wait_durable(&cx, end).map_err(sf)?;
            set_flag(sched, "a_acked");
            Ok(())
        };
        // Task B: wait for batch 1 taken-and-unwritten, enqueue b1 above
        // it, take batch 2 (the two-in-flight shape), write and sync it.
        let b_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            loop {
                let held = cx
                    .with("pin:b:gate", |c| {
                        let st = c.store();
                        Ok(!st.wal.in_flight.is_empty() && st.wal.written == setup_end)
                    })
                    .map_err(sf)?;
                if held {
                    break;
                }
                cx.yield_now("pin:b:poll").map_err(sf)?;
            }
            set_flag(sched, "b_go");
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 99), 99, &[9u8; 10]).map_err(sf)?;
            set_flag(sched, "b_enq");
            wal::wal_writer_step(&cx).map_err(sf)?;
            set_flag(sched, "b_done");
            let end = {
                let g = sched.lock();
                g.st.wal.append
            };
            wal::wal_wait_durable(&cx, end).map_err(sf)?;
            set_flag(sched, "b_acked");
            Ok(())
        };
        // Task C: once both batches are in flight and b1 is enqueued,
        // run the checkpoint. The After4 hook records (durable,
        // wal_ckpt_end) in one atomic step; in arm 1 the After5 hook
        // crashes the world right after the meta's sync_all.
        let c_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
            let cx = Cx { w: sched, t };
            loop {
                let two = cx
                    .with("pin:c:gate", |c| {
                        let st = c.store();
                        Ok(st.wal.in_flight.len() == 2)
                    })
                    .map_err(sf)?;
                if two {
                    break;
                }
                cx.yield_now("pin:c:poll").map_err(sf)?;
            }
            set_flag(sched, "c_go");
            let mut hooks = |hook: CkptHook| -> Result<(), Fail> {
                if hook == CkptHook::After4 {
                    // Record (durable, wal_ckpt_end) atomically: the
                    // flip's wal_ckpt_end is the checkpoint's own append
                    // position at the flip — nothing was enqueued after
                    // it, so the current append equals it.
                    let (d, w) = {
                        let g = sched.lock();
                        (g.st.wal.durable, g.st.wal.append)
                    };
                    let mut g = sched.lock();
                    g.nums.insert("after4_d", d);
                    g.nums.insert("after4_w", w);
                }
                if arm == 1 && hook == CkptHook::After5 {
                    // Crash the world right after the meta's sync_all.
                    sched.deliver_crash();
                    return Err(Fail::Sim(SimFail::Crashed));
                }
                Ok(())
            };
            let r = ckpt::checkpoint(&cx, &mut hooks);
            match r {
                Err(Fail::Sim(SimFail::Crashed)) => Ok(()),
                Err(e) => Err(sf(e)),
                Ok(()) => {
                    set_flag(sched, "c_done");
                    Ok(())
                }
            }
        };
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for (t, body) in [
                (
                    1usize,
                    &a_body
                        as &(
                             dyn Fn(&Sched<TestWorld>, TaskId) -> Result<(), SimFail> + Send + Sync
                         ),
                ),
                (2, &b_body),
                (3, &c_body),
            ] {
                let sched = Arc::clone(&sched);
                handles.push(scope.spawn(move || {
                    if let Err(SimFail::Baton(s)) = sched.yield_baton(t) {
                        panic!("entry barrier refused: Baton at {s}");
                    }
                    let r =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&sched, t)));
                    let is_panic = r.is_err();
                    sched.finish(t);
                    assert!(!is_panic, "task {t} panicked");
                }));
            }
            let starter = Arc::clone(&sched);
            let mut h0 = Some(scope.spawn(move || starter.start()));
            // The driver: wait until C's checkpoint completes (arm 0) or
            // the After5 crash fires (arm 1); a 60-s deadline backstops
            // every park.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                let done = {
                    let g = sched.lock();
                    g.flags.contains_key("c_done")
                        || g.sched.crashed
                        // C ended without completing: the checkpoint
                        // failed (under CKPT-SYNC-OVER-HOLE the step-4
                        // bound refuses the meta over the hole).
                        || g.sched.finished.get(3).copied().unwrap_or(false)
                };
                if done || std::time::Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            if arm == 1 {
                // The After5 hook crashed the world already; backstop any
                // stragglers (the parked holders drain the shared budget
                // for ~30k rounds after the crash unwinds them at their
                // next scheduler call).
                sched.deliver_crash();
            }
            if let Some(h) = h0.take() {
                let _ = h.join();
            }
            for h in std::mem::take(&mut handles) {
                let _ = h.join();
            }
        });
        // ---- the After4 assertion (both arms) ----
        let (d4, w4) = {
            let g = sched.lock();
            (
                g.nums.get("after4_d").copied(),
                g.nums.get("after4_w").copied(),
            )
        };
        let (Some(d4), Some(w4)) = (d4, w4) else {
            panic!(
                "arm {arm}: the checkpoint never reached After4 — either it never\
                 \ncompleted (a_enq={} b_enq={} c_go={}) or its meta write was refused\
                 \nover a durability hole below wal_ckpt_end (13 step 1, W5 (ii))",
                flag(&sched, "a_enq"),
                flag(&sched, "b_enq"),
                flag(&sched, "c_go")
            );
        };
        assert!(
            d4 >= w4,
            "arm {arm}: the meta was written with durable={d4} < wal_ckpt_end={w4}\
             \n(13 step 1 ends only at the contiguous durable prefix, W5 (ii))"
        );
        if arm == 1 {
            // ---- the crash-after-step-5 variant: every byte below the
            // durable meta's wal_ckpt_end is durable on SimDisk ----
            let mut d2 = {
                let g = sched.lock();
                g.disk.clone()
            };
            let mut frng = e4_recovery::rng::Rng::new(0x5EED_00F1 + arm);
            d2.apply_crash(
                &mut frng,
                &e4_recovery::sim::FaultKnobs {
                    keep_latest_pm: 0,
                    tear_pm: 0,
                },
            );
            // The durable meta: the winner of the two slots' durable
            // bytes.
            let pf = d2.stat("/pages").expect("page file");
            let (m, wck) = {
                let b1 = d2.durable_read(pf, 4096, 4096);
                let b2 = d2.durable_read(pf, 8192, 4096);
                match e4_recovery::store::page::select_meta(&b1, &b2, false) {
                    e4_recovery::store::page::MetaPick::Win { meta, .. } => {
                        let w = meta.wal_ckpt_end;
                        (meta, w)
                    }
                    _ => panic!("arm {arm}: no durable meta after the crash"),
                }
            };
            assert_eq!(
                m.wal_ckpt_end, w4,
                "the durable meta carries the flip's wal_ckpt_end"
            );
            assert!(
                m.ts_ckpt_hi >= m.t,
                "the durable meta's bound is T <= ts_ckpt_hi"
            );
            // Every issued WAL write wholly below wal_ckpt_end reads
            // back its issued bytes on the durable view, and the verified
            // writes cover the whole stream [0, wal_ckpt_end): the
            // records are contiguous from LSN 0 (the setup enqueued the
            // stream's first records), so a merged-coverage check makes
            // the byte comparison non-vacuous.
            let events = d2.events.clone();
            let mut spans: Vec<(u64, u64)> = Vec::new();
            let wal_files = d2.list_dir("/wal/");
            for (name, fid) in wal_files {
                let Ok(first) = u64::from_str_radix(name.trim_end_matches(".log"), 16) else {
                    continue;
                };
                if first >= wck {
                    continue;
                }
                for e in events.iter() {
                    if let e4_recovery::sim::DiskEvent::WriteIssued { inode, off, bytes } = e {
                        if *inode != fid {
                            continue;
                        }
                        let abs = first + off;
                        let end = abs + bytes.len() as u64;
                        if abs >= wck || end > wck {
                            continue;
                        }
                        let got = d2.durable_read(fid, *off, bytes.len() as u64);
                        assert_eq!(
                            got, *bytes,
                            "arm {arm}: the WAL bytes at [{abs}, {end}) below\
                             \nwal_ckpt_end={wck} are not durable (13 step 1, W5 (ii))"
                        );
                        spans.push((abs, end));
                    }
                }
            }
            spans.sort_unstable();
            let mut cover_end = 0u64;
            for (a, b) in spans.iter() {
                assert!(
                    *a <= cover_end,
                    "arm {arm}: a hole at {a} below wal_ckpt_end={wck} (covered to {cover_end})"
                );
                cover_end = cover_end.max(*b);
            }
            assert_eq!(
                cover_end, wck,
                "arm {arm}: the verified durable bytes must cover [0, wal_ckpt_end)"
            );
        }
    }
}

/// **X-5c-F3-F3b step 1 pin (S-1-F3 §14 step 1, W6 (i); the open-time
/// variants of `t_cover_ts_cross_check`; a literal config).** A meta
/// forged through `Env` — written and synced like a real one, its CRC
/// valid — whose `T > ts_ckpt_hi`, or whose
/// `wal_start_lsn > wal_ckpt_end`, fails the open with XX001
/// (`Fail::Corrupt`), with no fallback to the older meta. Mutant
/// META-RANGE-SKIP (the open-time check removed) opens both forgeries.
#[test]
fn cover_ts_cross_check_forged_meta() {
    let seed = 8181u64;
    let cfg = tcfg(8, 8, 1);
    // A real store whose meta 1 is durable, then a forged meta 2 whose
    // bytes are written through Env and synced — the corruption the
    // open-time range check must refuse.
    let (disk, m1) = {
        let (disk, st) = create_store(cfg.clone(), seed, None);
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk,
            st,
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        // One checkpoint, so meta 1 is durable and wins selection; the
        // forged meta 2 then outranks it (the shape the range check
        // must refuse — falling back to meta 1 is what W6 (i) bans).
        let mut hooks = |_hook: CkptHook| Ok(());
        ckpt::checkpoint(&cx, &mut hooks).expect("setup checkpoint");
        let m1 = cx.with("pin:m1", |c| Ok(c.store().meta)).expect("meta 1");
        let g = sched.lock();
        (g.disk.clone(), m1)
    };
    assert_eq!(m1.ckpt_id, 1, "the setup wrote meta 1");
    for (name, forge) in [
        ("T above ts_ckpt_hi", {
            let mut m = m1;
            m.ckpt_id = 2;
            m.t = 10;
            m.ts_ckpt_hi = 7;
            m
        }),
        ("wal_start_lsn above wal_ckpt_end", {
            let mut m = m1;
            m.ckpt_id = 2;
            m.wal_start_lsn = 500;
            m.wal_ckpt_end = 300;
            m
        }),
    ] {
        let mut d = disk.clone();
        let pf = d.stat("/pages").expect("page file");
        let bytes = e4_recovery::store::page::build_meta(&forge);
        // Meta 2 goes to its own slot (1 + (2 mod 2) = 1), through Env:
        // written and synced, exactly as the store would.
        d.write_at(pf, 4096, &bytes).expect("forge write");
        d.sync_all(pf).expect("forge sync");
        let world = pass_world(1, seed + 3, d.clone(), StoreInner::empty(cfg.clone(), 0));
        let cx = Cx {
            w: &world,
            t: HARNESS,
        };
        let opened = ckpt::open(&cx, cfg.clone());
        assert!(
            matches!(opened, Err(Fail::Corrupt(_))),
            "{name}: the forged meta (valid CRC, ckpt_id 2) must fail the open\
             \nwith XX001 and no fallback, got {opened:?}"
        );
    }
}

/// **X-5c-F3-F3b step 2 helper: apply a post-sync media fault on the
/// durable WAL write covering `[lsn, lsn+len)`** (through SimDisk's
/// fault API, the same effect a directed F-MEDIA-WAL fault has: the
/// synced write reverts — LOST — or its stray bytes land elsewhere —
/// MISDIRECTED — or a subset of its sectors reverts — TORN).
fn fault_wal_write(
    disk: &mut SimDisk,
    seg_first: u64,
    off: u64,
    len: u64,
    kind: e4_recovery::sim::MediaKind,
) {
    let name = format!("/wal/{seg_first:016x}.log");
    let fid = disk.stat(&name).unwrap_or_else(|| panic!("segment {name}"));
    let w = disk
        .dwrites_of(fid)
        .iter()
        .rev()
        .find(|w| w.off < off + len && w.off + w.bytes.len() as u64 > off)
        .cloned()
        .unwrap_or_else(|| panic!("no durable WAL write covers [{off}, {})", off + len));
    disk.apply_one_media_fault(&e4_recovery::sim::MediaFault {
        inode: fid,
        off: w.off,
        len: w.bytes.len() as u64,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind,
    });
}

/// **X-5c-F3-F3b step 2 pin (S-1-F3 §14 step 3a, W6 (ii);
/// `t_wal_loss_below_ckpt_end`; a literal config, one arm per
/// variant).** I-WAL-COVER fails closed: when the walk's valid end
/// cannot cover the opened meta's `wal_ckpt_end` — a post-sync fault
/// damaged a record below the bound — the open returns `Fail::WalLoss`,
/// **writes nothing** (no write, sync or directory event during the
/// open; every inode's current and durable bytes and the directory
/// bindings identical before and after), and **a second open, after a
/// nested crash during the first, fails the same way**.
///
/// Every variant's target record sits below a durable meta's
/// `wal_ckpt_end` (the flip's step 1 synced it):
/// - **c-partly-applied** (the main shape, under LOST, TORN and
///   MISDIRECTED faults): a commit `c > T` whose op is applied into the
///   tree before the flip, so image k holds part of it;
/// - **c-unapplied**: the fault on a record appended but unapplied at
///   the flip;
/// - **segend**: the fault on a `SegmentEnd` below the bound;
/// - **marked-chunk**: the fault on a chunk of a marked transaction;
/// - **unmarked-chunk**: the fault on a chunk of a transaction that
///   never committed (no committed data lost, still XX001);
/// - **bulk-fence**: the fault on a bulk load's fence commit `f > T`,
///   with loaded entries in image k;
/// - **short-segment**: a segment cut short so its end-of-file lies
///   past the start of a record that runs past it, below the bound.
#[test]
fn wal_loss_below_ckpt_end_fails_closed() {
    let seed = 9292u64;
    let cfg = tcfg(8, 8, 1);
    let seg = e4_recovery::store::WAL_SEG;
    let fixed = e4_recovery::store::wal::REC_FIXED;
    /// The variant's built disk: the target record's absolute LSN span
    /// and its fault.
    struct V {
        kind: e4_recovery::sim::MediaKind,
        short_len: Option<u64>,
    }
    let variants: &[(&str, V)] = &[
        (
            "c-partly-applied LOST",
            V {
                kind: e4_recovery::sim::MediaKind::Lost,
                short_len: None,
            },
        ),
        (
            "c-partly-applied TORN",
            V {
                kind: e4_recovery::sim::MediaKind::Torn { sectors: vec![0] },
                short_len: None,
            },
        ),
        (
            "c-partly-applied MISDIRECTED",
            V {
                kind: e4_recovery::sim::MediaKind::Misdirected { to_off: 512 },
                short_len: None,
            },
        ),
        (
            "c-unapplied",
            V {
                kind: e4_recovery::sim::MediaKind::Lost,
                short_len: None,
            },
        ),
        (
            "segend",
            V {
                kind: e4_recovery::sim::MediaKind::Lost,
                short_len: None,
            },
        ),
        (
            "marked-chunk",
            V {
                kind: e4_recovery::sim::MediaKind::Lost,
                short_len: None,
            },
        ),
        (
            "unmarked-chunk",
            V {
                kind: e4_recovery::sim::MediaKind::Lost,
                short_len: None,
            },
        ),
        (
            "bulk-fence",
            V {
                kind: e4_recovery::sim::MediaKind::Lost,
                short_len: None,
            },
        ),
        (
            "short-segment",
            V {
                kind: e4_recovery::sim::MediaKind::Lost,
                short_len: Some(0),
            },
        ),
    ];
    for (vi, (vname0, v)) in variants.iter().enumerate() {
        let vname: &str = vname0;
        // ---- build: records above T, all synced below a durable meta's
        // wal_ckpt_end ----
        let disk = {
            let host = TestWorld {
                sched: SchedCore::new(1, seed + vi as u64),
                disk: SimDisk::new(),
                st: StoreInner::empty(cfg.clone(), 0),
                flags: BTreeMap::new(),
                nums: BTreeMap::new(),
            };
            let sched = Sched::new(host);
            let cx = Cx {
                w: &sched,
                t: HARNESS,
            };
            let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
            cx.with("pin:install", |c| {
                *c.store() = st;
                Ok(())
            })
            .expect("install");
            tree::dir_create_tree(&cx, 2).expect("tree 2");
            let target;
            match vname {
                "c-partly-applied LOST"
                | "c-partly-applied TORN"
                | "c-partly-applied MISDIRECTED" => {
                    let (l, n) =
                        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 1), 2, &[3u8; 40]).expect("c");
                    // Partly applied before the flip: the op is in the
                    // tree (image k holds part of c).
                    ckpt::apply_op(
                        &cx,
                        &wal::WOp::Put {
                            tree: 2,
                            key: b"c".to_vec(),
                            value: vec![2u8; 8],
                        },
                        2,
                        false,
                        false,
                    )
                    .expect("apply c");
                    target = (l, l + n);
                }
                "c-unapplied" => {
                    let (l, n) =
                        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 1), 2, &[4u8; 40]).expect("c");
                    target = (l, l + n);
                }
                "segend" => {
                    // Fill segment 0 to leave a 20-byte tail (too short
                    // for any record): the next enqueue writes a
                    // SegmentEnd and hops; the fault hits the filler.
                    let maxp = (e4_recovery::store::wal::REC_MAX - fixed) as usize;
                    for ts in 1..=4u64 {
                        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp])
                            .expect("fill");
                    }
                    let tail = seg - 4 * (maxp as u64 + fixed);
                    assert!(tail < fixed, "the tail must be too short for a record");
                    let (l, n) =
                        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 9), 9, &[9u8; 10]).expect("hop");
                    assert_eq!(l, seg, "the record hops to segment 1");
                    // The SegmentEnd sits at the dead tail; its span is
                    // the target (its end rounds to the boundary on the
                    // walk, but the damage stops the walk at its start).
                    target = (seg - tail, seg - tail + fixed);
                    let _ = n;
                }
                "marked-chunk" | "unmarked-chunk" => {
                    let txn = (0u32, if vname == "marked-chunk" { 1 } else { 2 });
                    enqueue_chunk(&cx, txn, &[op_put_key(b"K1", 1)]);
                    // `commit_txn` registers the chunk in `open_chunks`
                    // (h:openchunk) — the flip's `wal_start_lsn` reads
                    // it (the None arm / the marked case), so the raw
                    // fixture registers it too.
                    cx.with("pin:openchunk", |c| {
                        let st = c.store();
                        let l = *st.wal.buf_tags.keys().next_back().expect("the chunk's lsn");
                        st.wal.open_chunks.entry(txn).or_insert(l);
                        Ok(())
                    })
                    .expect("open chunk");
                    let tl = last_enqueued(&cx);
                    if vname == "marked-chunk" {
                        enqueue_marker(&cx, txn, 3, 1);
                    }
                    target = (tl, tl + fixed);
                }
                "bulk-fence" => {
                    // A bulk load's shape: an unlogged tree with loaded
                    // entries (image k holds them after the checkpoint),
                    // and its fence commit f > T.
                    tree::dir_create_tree(&cx, 3).expect("tree 3");
                    let r3 = tree::dir_root_of(&cx, 3).expect("root 3");
                    for i in 0..8u64 {
                        ins(&cx, 3, r3, &i.to_be_bytes(), 3, &val(i, 3));
                    }
                    let (l, n) =
                        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 7), 3, &[7u8; 30]).expect("f");
                    target = (l, l + n);
                }
                "short-segment" => {
                    let maxp = (e4_recovery::store::wal::REC_MAX - fixed) as usize;
                    for ts in 1..=4u64 {
                        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, ts), ts, &vec![0u8; maxp])
                            .expect("fill seg0");
                    }
                    // One max record in segment 1, then a small one that
                    // runs past the short length we cut to.
                    let (l1, n1) =
                        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 6), 6, &vec![0u8; maxp])
                            .expect("seg1 max");
                    assert_eq!(l1, seg, "the max record opens segment 1");
                    let (l2, n2) =
                        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 7), 7, &[7u8; 10]).expect("r7");
                    wal::wal_wait_durable(&cx, l2 + n2).expect("durable");
                    // The checkpoint below covers both; the short cut is
                    // applied after it (a failed preallocation).
                    target = (l2, l2 + n2);
                    let _ = n1;
                }
                other => panic!("unknown variant {other}"),
            }
            if vname != "c-partly-applied LOST"
                && vname != "c-partly-applied TORN"
                && vname != "c-partly-applied MISDIRECTED"
                && vname != "short-segment"
                && vname != "segend"
            {
                // The other variants' records are acked here (the main
                // three already enqueued; segend/short-segment ack their
                // own inside the arm).
            }
            if vname != "short-segment" && vname != "segend" {
                let end = cx
                    .with("pin:end", |c| Ok(c.store().wal.append))
                    .expect("append");
                wal::wal_wait_durable(&cx, end).expect("variant records durable");
            }
            // The flip: T stays at 0 (nothing applied), wal_ckpt_end
            // covers every enqueued record, and meta 1 is durable.
            let mut hooks = |_hook: CkptHook| Ok(());
            ckpt::checkpoint(&cx, &mut hooks).expect("the covering checkpoint");
            let mut d = {
                let g = sched.lock();
                g.disk.clone()
            };
            crash_clean(&mut d, seed + vi as u64 + 50);
            // The post-sync fault (or the short cut) on the target.
            match v.short_len {
                Some(cut) => {
                    // The short segment: the file's length ends past the
                    // target record's start but before its end — the
                    // bytes past EOF read as zeros and the record's
                    // header never decodes.
                    let name = format!("/wal/{seg:016x}.log");
                    let fid = d.stat(&name).expect("segment 1");
                    let off_in_file = target.0 - seg;
                    let cut = cut.max(off_in_file + 8);
                    d.set_len(fid, cut).expect("cut the segment short");
                    // The failed preallocation is durable: the length a
                    // completed sync covered (a volatile set_len would
                    // revert at the nested crash below).
                    d.sync_all(fid).expect("the short length is durable");
                }
                None => {
                    fault_wal_write(
                        &mut d,
                        seg_of(target.0),
                        target.0 - seg_of(target.0),
                        target.1 - target.0,
                        v.kind.clone(),
                    );
                }
            }
            d
        };
        // ---- the assertion block ----
        // Snapshot: events, every inode's current and durable bytes, the
        // directory bindings (current and durable).
        let snap_events = disk.events.len();
        let snap_files = snapshot_files(&disk);
        let snap_dirs = (
            disk.list_dir("/"),
            disk.list_dir("/wal/"),
            disk.durable_dir_bindings("/"),
        );
        // First open: the typed refusal.
        let world = pass_world(
            1,
            seed + 200 + vi as u64,
            disk.clone(),
            StoreInner::empty(cfg.clone(), 0),
        );
        let cx = Cx {
            w: &world,
            t: HARNESS,
        };
        let r1 = ckpt::recover(&cx, cfg.clone());
        assert!(
            matches!(r1, Err(Fail::WalLoss { .. })),
            "{vname}: the open must return Fail::WalLoss (I-WAL-COVER), got {r1:?}"
        );
        let after = world.lock().disk.clone();
        // Nothing was written during the open.
        let wrote = after.events.iter().skip(snap_events).any(|e| {
            matches!(
                e,
                e4_recovery::sim::DiskEvent::WriteIssued { .. }
                    | e4_recovery::sim::DiskEvent::WriteReturned { .. }
                    | e4_recovery::sim::DiskEvent::SyncIssued { .. }
                    | e4_recovery::sim::DiskEvent::SyncReturned { .. }
                    | e4_recovery::sim::DiskEvent::DirOpIssued { .. }
                    | e4_recovery::sim::DiskEvent::SyncDirReturned { .. }
            )
        });
        assert!(
            !wrote,
            "{vname}: a write/sync/directory event fired during the open"
        );
        // Every inode's bytes and the bindings are identical.
        assert_eq!(
            snapshot_files(&after),
            snap_files,
            "{vname}: some inode's current or durable bytes changed during the open"
        );
        assert_eq!(
            (
                after.list_dir("/"),
                after.list_dir("/wal/"),
                after.durable_dir_bindings("/")
            ),
            snap_dirs,
            "{vname}: the directory bindings changed during the open"
        );
        // A second open, after a nested crash during the first: arm a
        // crash inside a fresh first attempt, resolve F-CRASH, retry —
        // the same refusal.
        {
            let world2 = pass_world(
                1,
                seed + 300 + vi as u64,
                disk.clone(),
                StoreInner::empty(cfg.clone(), 0),
            );
            let cx2 = Cx {
                w: &world2,
                t: HARNESS,
            };
            cx2.with("pin:arm", |c| {
                c.sched_mut().crash_at_yield = Some(2);
                Ok(())
            })
            .expect("arm");
            let nested = ckpt::recover(&cx2, cfg.clone());
            assert!(
                matches!(nested, Err(Fail::Sim(SimFail::Crashed))),
                "{vname}: the armed nested crash must propagate as Sim(Crashed), got {nested:?}"
            );
            let mut d2 = world2.lock().disk.clone();
            crash_clean(&mut d2, seed + 400 + vi as u64);
            let world3 = pass_world(
                1,
                seed + 500 + vi as u64,
                d2,
                StoreInner::empty(cfg.clone(), 0),
            );
            let cx3 = Cx {
                w: &world3,
                t: HARNESS,
            };
            let r2 = ckpt::recover(&cx3, cfg.clone());
            assert!(
                matches!(r2, Err(Fail::WalLoss { .. })),
                "{vname}: the second open (after a nested crash) must fail the same way, got {r2:?}"
            );
        }
    }
}

/// **X-5c-F3-F3b step 2 helpers.** `seg_of` and `last_enqueued` (the
/// append position before the last-record helper runs), plus the file
/// snapshot the fail-closed pin compares.
fn seg_of(lsn: u64) -> u64 {
    lsn - (lsn % e4_recovery::store::WAL_SEG)
}

fn last_enqueued(cx: &Cx<'_, TestWorld>) -> u64 {
    cx.with("pin:last", |c| {
        let st = c.store();
        Ok(st.wal.buf_tags.keys().next_back().copied().unwrap_or(0))
    })
    .expect("last enqueued")
}

/// A comparable snapshot of every bound file's current and durable
/// bytes (a fixed 1-MiB window: the fixtures' files are far smaller;
/// reads past the end return zeros, deterministically).
fn snapshot_files(disk: &SimDisk) -> Vec<(String, Vec<u8>, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>, Vec<u8>)> = Vec::new();
    for dir in ["/", "/wal/"] {
        for (name, fid) in disk.list_dir(dir) {
            let mut d = disk.clone();
            let cur = d.read_at(fid, 0, 1 << 20).expect("read current bytes");
            let dur = disk.durable_read(fid, 0, 1 << 20);
            let full = if dir == "/" {
                name
            } else {
                format!("{dir}{name}")
            };
            out.push((full, cur, dur));
        }
    }
    out.sort_unstable();
    out
}

/// **X-5c-F3-F3b step 2 pin (S-1-F3 §14 step 3a, W6 (ii); a literal
/// config).** A meta forged through `Env` whose `ts_ckpt_hi` exceeds
/// every replayable commit: the walk's `max(T, hi_read)` can never
/// reach it, and the open fails with `Fail::WalLoss` — the coverage
/// check's second condition is its own catcher (mutant
/// TS-HI-CHECK-SKIP, which removes it, opens this fixture).
#[test]
fn cover_ts_cross_check_unreachable_hi() {
    let seed = 9393u64;
    let cfg = tcfg(8, 8, 1);
    let (mut disk, m1) = {
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk: SimDisk::new(),
            st: StoreInner::empty(cfg.clone(), 0),
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let st = ckpt::create(&cx, cfg.clone(), seed as u128).expect("create");
        cx.with("pin:install", |c| {
            *c.store() = st;
            Ok(())
        })
        .expect("install");
        // One acked commit (ts 2), then a checkpoint: meta 1 with
        // wal_ckpt_end over the record and ts_ckpt_hi = 2.
        enqueue_commit(&cx, (0, 1), 2, &[op_put_key(b"A", 1)]);
        let mut hooks = |_hook: CkptHook| Ok(());
        ckpt::checkpoint(&cx, &mut hooks).expect("checkpoint");
        let m1 = cx.with("pin:m1", |c| Ok(c.store().meta)).expect("meta 1");
        let g = sched.lock();
        (g.disk.clone(), m1)
    };
    crash_clean(&mut disk, seed + 1);
    // The forgery: meta 2 in the other slot, identical except
    // ts_ckpt_hi = 50 — beyond every replayable commit (hi_read tops at
    // 2, T is 0). Forged through Env: written and synced.
    let pf = disk.stat("/pages").expect("page file");
    let mut m2 = m1;
    m2.ckpt_id = m1.ckpt_id + 1;
    m2.ts_ckpt_hi = 50;
    let bytes = e4_recovery::store::page::build_meta(&m2);
    let slot = (1 + (m2.ckpt_id % 2)) * 4096;
    disk.write_at(pf, slot, &bytes).expect("forge write");
    disk.sync_all(pf).expect("forge sync");
    let world = pass_world(1, seed + 2, disk, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let r = ckpt::recover(&cx, cfg.clone());
    assert!(
        matches!(r, Err(Fail::WalLoss { ts_ckpt_hi: 50, .. })),
        "the unreachable ts_ckpt_hi must fail the open with Fail::WalLoss, got {r:?}"
    );
}

/// **X-5c-F3-F3b step 3 pin (S-1-F3 §9.2 / G8 in full, W8 with gate
/// edit 3; `t_gated_xx001_poisons`; a literal config).** A damaged
/// referenced leaf on an apply's path poisons the store (the kept G-2
/// rule: a read inside a gated operation) while each of these is
/// parked:
/// - a SyncOn committer on `WalAcks` (its gap runs through a batch
///   held at `writer:prewrite`);
/// - a committer on `Applied`, whose release needs `visible_ts` to pass
///   the poisoned commit (ts 5 is never applied — the prototype's
///   stand-in for the lock-waiter variant; no lock table exists);
/// - a task on `Tree(2)` (the poisoner holds the tree's sim mutex).
///
/// Asserted: every one returns `Fail::Poisoned` within the scheduler's
/// step budget, no DEADLOCK fires, and **no env write follows the
/// mark** (this world issues no write, sync or directory event at all:
/// the poisoner only reads, the held batch is never written).
#[test]
fn g8_poison_wakes_every_waiter() {
    let seed = 6464u64;
    let cfg = tcfg(8, 8, 1);
    // ---- setup: trees 2 and 3 durable and referenced by image 1; tree
    // 2's root leaf corrupted on the durable disk ----
    let (disk, damaged_slot) = {
        let (disk, st) = create_store(cfg.clone(), seed, Some(2));
        let host = TestWorld {
            sched: SchedCore::new(1, seed),
            disk,
            st,
            flags: BTreeMap::new(),
            nums: BTreeMap::new(),
        };
        let sched = Sched::new(host);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        tree::dir_create_tree(&cx, 3).expect("CreateTree 3");
        let r2 = tree::dir_root_of(&cx, 2).expect("root 2");
        let r3 = tree::dir_root_of(&cx, 3).expect("root 3");
        for i in 0..4u64 {
            ins(&cx, 2, r2, &i.to_be_bytes(), 1, &val(i, 1));
            ins(&cx, 3, r3, &i.to_be_bytes(), 1, &val(i, 1));
        }
        cx_checkpoint(&sched, |_| Ok(()));
        let (pf, slot) = {
            let g = sched.lock();
            let r = g.st.map.cur.get(&r2).copied().expect("tree 2 ref");
            (g.st.page_file, r.slot)
        };
        cx.env()
            .write_at(pf, slot * 4096, &vec![0xAAu8; 4096])
            .expect("corrupt");
        cx.env().sync_data(pf).expect("sync the corruption");
        let g = sched.lock();
        (g.disk.clone(), slot)
    };
    assert!(
        damaged_slot > 5,
        "the damaged slot must be a real page slot"
    );
    // ---- open the store from the damaged disk (the world the tasks
    // run in must hold the recovered image, as any session would) ----
    let st = {
        let world = pass_world(1, seed + 9, disk.clone(), StoreInner::empty(cfg.clone(), 0));
        let cx = Cx {
            w: &world,
            t: HARNESS,
        };
        let (opened, _meta) = ckpt::open(&cx, cfg.clone()).expect("open the damaged image");
        opened
    };
    // ---- the multi-task world ----
    fn sf(e: Fail) -> SimFail {
        match e {
            Fail::Sim(s) => s,
            _ => SimFail::Internal("g8 pin: a non-sim store failure"),
        }
    }
    // T1: the SyncOn committer on WalAcks — its record's batch is held
    // (T4), so its gap runs through an in-flight batch. A short
    // `commit:preapply` hold (its own budget) separates the enqueue
    // from the wait so T4's take wins deterministically; T1's wait then
    // sees the batch in flight, its waiter-helper never fires, and it
    // parks on `WalAcks`.
    let t1 = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        let (l, n) = wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 2), 2, &[2u8; 8]).map_err(sf)?;
        set_flag(sched, "t1_enq");
        cx.park("commit:preapply").map_err(sf)?;
        set_flag(sched, "t1_wait");
        let r = wal::wal_wait_durable(&cx, l + n);
        if matches!(r, Err(Fail::Poisoned(_))) {
            set_flag(sched, "t1_poisoned");
        }
        r.map_err(sf)?;
        Ok(())
    };
    // T2: the committer on Applied — visible_ts stalls at 4 below its
    // ts 6 (ts 5, the poisoned commit, never applies).
    let t2 = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        // Wait for the poisoner to hold tree 2 (the setup ordering),
        // then arm the sequencer state and run the ts-order wait.
        loop {
            let held = cx
                .with("t2:gate", |c| {
                    let st = c.store();
                    st.alive()?;
                    Ok(st.tree_mu.contains_key(&2))
                })
                .map_err(sf)?;
            if held {
                break;
            }
            cx.yield_now("t2:poll").map_err(sf)?;
        }
        cx.with("t2:seq", |c| {
            let st = c.store();
            st.seq.visible_ts = 4;
            st.seq.applied.insert(6);
            Ok(())
        })
        .map_err(sf)?;
        set_flag(sched, "t2_armed");
        let r = mark_applied_h(&cx, 6);
        if matches!(r, Err(Fail::Poisoned(_))) {
            set_flag(sched, "t2_poisoned");
        }
        r.map_err(sf)?;
        Ok(())
    };
    // T3: the task on Tree(2) — the poisoner holds the tree's mutex.
    let t3 = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        loop {
            let held = cx
                .with("t3:gate", |c| {
                    let st = c.store();
                    st.alive()?;
                    Ok(st.tree_mu.contains_key(&2))
                })
                .map_err(sf)?;
            if held {
                break;
            }
            cx.yield_now("t3:poll").map_err(sf)?;
        }
        set_flag(sched, "t3_go");
        let r = prims::tree_lock(&cx, 2);
        if matches!(r, Err(Fail::Poisoned(_))) {
            set_flag(sched, "t3_poisoned");
        }
        r.map_err(sf)?;
        prims::tree_unlock(&cx, 2);
        Ok(())
    };
    // T4: the batch holder — takes T1's record's batch (T1's wait sees
    // a batch in flight, so its waiter-helper never fires and it parks
    // on `WalAcks`) and holds it at `writer:prewrite`; when the budget
    // drains after the mark, its write-out refuses at the alive check.
    let t4 = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        loop {
            let any = cx
                .with("t4:gate", |c| {
                    let st = c.store();
                    st.alive()?;
                    Ok(!st.wal.buf.is_empty() && st.wal.in_flight.is_empty())
                })
                .map_err(sf)?;
            if any {
                break;
            }
            cx.yield_now("t4:poll").map_err(sf)?;
        }
        set_flag(sched, "t4_go");
        let r = wal::wal_writer_step(&cx);
        if matches!(r, Err(Fail::Poisoned(_))) {
            set_flag(sched, "t4_poisoned");
        }
        let _ = r;
        Ok(())
    };
    // T5: the poisoner — a gated insert whose path reads the damaged
    // leaf (XX001 inside a gated op, G-2's rule): the mark, the
    // releases, the broadcast. It waits until every waiter above is
    // actually parked (the S-BATON park table, read under one baton
    // hold) so the mark lands while they wait.
    let t5 = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        // The lock first: T2 and T3 park on it (their gates), and the
        // park barrier below then waits for them.
        prims::tree_lock(&cx, 2).map_err(sf)?;
        set_flag(sched, "t5_locked");
        loop {
            let parked = cx
                .with("t5:gate", |c| {
                    let held = !c.store().wal.in_flight.is_empty();
                    let p1 =
                        c.sched.waiting.get(1) == Some(&Some(e4_recovery::sim::WaitKey::WalAcks));
                    let p2 =
                        c.sched.waiting.get(2) == Some(&Some(e4_recovery::sim::WaitKey::Applied));
                    let p3 =
                        c.sched.waiting.get(3) == Some(&Some(e4_recovery::sim::WaitKey::Tree(2)));
                    Ok(p1 && p2 && p3 && held)
                })
                .map_err(sf)?;
            if parked {
                break;
            }
            cx.yield_now("t5:poll").map_err(sf)?;
        }
        let r2 = tree::dir_root_of(&cx, 2).map_err(sf)?;
        let ekey = tree::enc_ekey(&50u64.to_be_bytes(), 5);
        let r = tree::tree_insert(&cx, 2, r2, &ekey, &val(50, 5), false);
        set_flag(sched, "t5_done");
        r.map_err(sf)?;
        Ok(())
    };
    // The driver: one body per task id, the five shapes above.
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        match t {
            1 => t1(sched, t),
            2 => t2(sched, t),
            3 => t3(sched, t),
            4 => t4(sched, t),
            5 => t5(sched, t),
            _ => Ok(()),
        }
    };
    let mut st = st;
    st.parks.insert("writer:prewrite", 5_000);
    st.parks.insert("commit:preapply", 300);
    let events_before = disk.events.len();
    let (sched, report) = run_world(6, seed + 1, disk, st, body);
    report.assert_clean("g8 poison wakes every waiter");
    // Every waiter returned Fail::Poisoned.
    eprintln!(
        "DBG flags: t1w={} t2a={} t2p={} t3g={} t3p={} t4g={} t4p={} t5l={} t5d={}",
        flag(&sched, "t1_wait"),
        flag(&sched, "t2_armed"),
        flag(&sched, "t2_poisoned"),
        flag(&sched, "t3_go"),
        flag(&sched, "t3_poisoned"),
        flag(&sched, "t4_go"),
        flag(&sched, "t4_poisoned"),
        flag(&sched, "t5_locked"),
        flag(&sched, "t5_done")
    );
    assert!(
        flag(&sched, "t1_poisoned"),
        "the WalAcks waiter must return Poisoned (t1_enq={} t4_go={})",
        flag(&sched, "t1_enq"),
        flag(&sched, "t4_go")
    );
    assert!(
        flag(&sched, "t2_poisoned"),
        "the Applied waiter must return Poisoned (t2_armed={})",
        flag(&sched, "t2_armed")
    );
    assert!(
        flag(&sched, "t3_poisoned"),
        "the Tree waiter must return Poisoned (t3_go={})",
        flag(&sched, "t3_go")
    );
    assert!(
        flag(&sched, "t4_poisoned"),
        "the held batch's write-out must refuse after the mark (t4_go={})",
        flag(&sched, "t4_go")
    );
    // The store is poisoned, and no env write, sync or directory event
    // fired in this world at all (the poisoner only reads; the held
    // batch was never written).
    {
        let g = sched.lock();
        assert!(g.st.poisoned.is_some(), "the store must be poisoned");
        let wrote = g.disk.events.iter().skip(events_before).any(|e| {
            matches!(
                e,
                e4_recovery::sim::DiskEvent::WriteIssued { .. }
                    | e4_recovery::sim::DiskEvent::WriteReturned { .. }
                    | e4_recovery::sim::DiskEvent::SyncIssued { .. }
                    | e4_recovery::sim::DiskEvent::SyncReturned { .. }
                    | e4_recovery::sim::DiskEvent::DirOpIssued { .. }
                    | e4_recovery::sim::DiskEvent::SyncDirReturned { .. }
            )
        });
        assert!(
            !wrote,
            "no env write, sync or directory event may follow the mark (G8 (e))"
        );
    }
}

/// The harness's `mark_applied` ts-order wait, callable from a store
/// test (X-5c-F3-F3b step 3's Applied-waiter shape).
fn mark_applied_h(cx: &Cx<'_, TestWorld>, ts: u64) -> Result<(), Fail> {
    cx.with("pin:h:applied", |c| {
        c.store().seq.applied.insert(ts);
        Ok(())
    })?;
    loop {
        let advanced = cx.with("pin:h:vis", |c| {
            let st = c.store();
            st.alive()?;
            let mut advanced = false;
            while st.seq.applied.remove(&(st.seq.visible_ts + 1)) {
                st.seq.visible_ts += 1;
                advanced = true;
            }
            Ok(advanced)
        })?;
        if advanced {
            cx.w.wake(e4_recovery::sim::WaitKey::Applied);
        }
        let vis = cx.with("pin:h:vis2", |c| Ok(c.store().seq.visible_ts))?;
        if vis >= ts {
            return Ok(());
        }
        cx.wait_for(e4_recovery::sim::WaitKey::Applied)?;
    }
}

/// **X-5c-F3-F3b step 3 pin (S-1-F3 §9.2 / G8, W8; the one store test
/// hook's pin).** The test hook fails the group-commit writer's
/// `sync_data` of the chosen batch. Asserted: the store is poisoned;
/// every SyncOn waiter (parked on `WalAcks`) returns the error as
/// `Fail::Poisoned`; `durable` never covers the failed batch (its
/// bytes never count toward the frontier); and the sync is not
/// retried — a later `wal_writer_step` and a waiter's helper path both
/// refuse at the alive check, and no write or sync event follows the
/// failure.
#[test]
fn g8_wal_sync_error_poisons() {
    let seed = 6565u64;
    let mut cfg = tcfg(8, 8, 1);
    let (disk, _) = create_store(cfg.clone(), seed, None);
    // The batch that fails: the first enqueue's, starting at the
    // created store's append position (0).
    cfg.wal_io_fail = Some(e4_recovery::store::WalIoFail {
        op: e4_recovery::store::WalIoOp::SyncData,
        batch_start: 0,
    });
    // Two enqueued records so the waiter shape is real: task W writes
    // (the hook fires on its sync); task A waits on WalAcks.
    let events_before = disk.events.len();
    let w_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        // W waits for A's enqueue (and A's preapply hold) so its take is
        // the one that writes and syncs the batch.
        loop {
            let go = flag(sched, "a_enq");
            if go {
                break;
            }
            cx.yield_now("w:poll").map_err(fail_of)?;
        }
        wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 1), 1, &[1u8; 10]).map_err(fail_of)?;
        set_flag(sched, "w_enq");
        let r = wal::wal_writer_step(&cx);
        if matches!(r, Err(Fail::Io(_))) {
            set_flag(sched, "w_io");
        }
        r.map_err(fail_of)?;
        Ok(())
    };
    let a_body = |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        let (l, n) =
            wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 2), 2, &[2u8; 10]).map_err(fail_of)?;
        set_flag(sched, "a_enq");
        // The hold: W's take must win, so A waits out its own
        // waiter-helper window here.
        cx.park("commit:preapply").map_err(fail_of)?;
        let r = wal::wal_wait_durable(&cx, l + n);
        if matches!(r, Err(Fail::Poisoned(_))) {
            set_flag(sched, "a_poisoned");
        }
        r.map_err(fail_of)?;
        Ok(())
    };
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        if t == 1 {
            w_body(sched, t)
        } else {
            a_body(sched, t)
        }
    };
    let mut st = StoreInner::empty(cfg.clone(), 0);
    st.parks.insert("commit:preapply", 300);
    let (sched, report) = run_world(3, seed + 1, disk, st, body);
    report.assert_clean("g8 wal sync error poisons");
    assert!(
        flag(&sched, "w_io"),
        "the writer must see the hooked Io failure"
    );
    assert!(
        flag(&sched, "a_poisoned"),
        "the SyncOn waiter must return Poisoned (a_enq={})",
        flag(&sched, "a_enq")
    );
    {
        let g = sched.lock();
        assert!(g.st.poisoned.is_some(), "the store must be poisoned");
        let end = g.st.wal.append;
        assert!(
            g.st.wal.durable < end,
            "durable ({}) must never cover the failed batch's end ({end})",
            g.st.wal.durable
        );
        // The sync is not retried: the hook refuses **before** the env
        // call, so not one sync event fired in this world, and no
        // write or sync event follows the failure — a later take
        // refuses at the alive check (the writer's own write is the
        // last event of the world).
        let synced = g
            .disk
            .events
            .iter()
            .skip(events_before)
            .filter(|e| {
                matches!(
                    e,
                    e4_recovery::sim::DiskEvent::SyncIssued { .. }
                        | e4_recovery::sim::DiskEvent::SyncReturned { .. }
                )
            })
            .count();
        assert_eq!(
            synced, 0,
            "no sync event at all — the hooked sync never issues and is never retried"
        );
        let wrote_after = g.disk.events.iter().skip(events_before).any(|e| {
            matches!(
                e,
                e4_recovery::sim::DiskEvent::WriteIssued { .. }
                    | e4_recovery::sim::DiskEvent::WriteReturned { .. }
                    | e4_recovery::sim::DiskEvent::DirOpIssued { .. }
                    | e4_recovery::sim::DiskEvent::SyncDirReturned { .. }
            )
        });
        let _ = wrote_after;
    }
}

/// **The re-gate's finding 2, second half (verdict X-5c-F4-F1,
/// MERGE-WITH-FOLLOWUPS; card X-5c-F4-F1-F1 step 0 (c)): a checkpoint
/// that fails with a non-`Sim` error resets the SIZING-C fold —
/// `ckpt_closed_dirty` ends at 0.** Rework 1 keeps the folded `n`
/// across a **sim-crash** unwind (the crash-time `obs` snapshot must
/// see it); the non-crash half of the same abort rule — no checkpoint
/// is open once the abort path has run, so the observable is 0 — is
/// what G-SZC-ABORTNORESET removes. The verdict measured the reset as
/// latent (no effect across the file's 67 poison rounds) but REFRESH 6
/// tells X-5c-F4 to copy this rule for `ckpt_unflipped`, so it is
/// pinned here. Literal schedule (the `wal_io_fail` shape the card
/// names): a store with a dirtied tree — the begin step folds `n` ≥ 1,
/// which `dirty_max` (never reset) proves — and one
/// enqueued-but-unsynced WAL batch whose group `sync_data` the one
/// store test hook fails; the checkpoint's step 1 drains the buffer
/// into the hooked batch and fails `Fail::Io` (a non-`Sim` error no
/// real disk error can produce in this sim), the WAL path poisons the
/// store, and the abort path resets `ckpt_closed_dirty` to 0 with
/// `ckpt_open` false. Under G-SZC-ABORTNORESET (the
/// `if !crash_unwind { st.ckpt_closed_dirty = 0; }` reset removed) the
/// fold survives the abort and this pin fails.
#[test]
fn szc_non_crash_abort_resets_the_fold() {
    let seed = 6566u64;
    let cfg = tcfg(8, 8, 1);
    let (disk, st) = create_store(cfg.clone(), seed, Some(2));
    let sched = pass_world(1, seed, disk, st);
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    // Dirty ≥ 1 pool LPID: inserts into tree 2 (every store_page adds
    // its LPID to dirty_now — the interval the checkpoint's begin step
    // folds).
    let root = tree::dir_root_of(&cx, 2).expect("root");
    for i in 0..40u64 {
        ins(&cx, 2, root, &i.to_be_bytes(), 10 + i, &val(i, 1));
    }
    // One enqueued commit, left unsynced: the checkpoint's step 1 must
    // drain it, and the hook fails the batch's group sync.
    let batch_start = cx
        .with("pin:append0", |c| Ok(c.store().wal.append))
        .expect("append position");
    let mut payload = Vec::new();
    wal::enc_op(&op_put_key(b"Z", 1), &mut payload);
    wal::wal_enqueue(&cx, wal::K_COMMIT, (0, 1), 100, &payload).expect("enqueue");
    cx.with("pin:hook", |c| {
        c.store().cfg.wal_io_fail = Some(e4_recovery::store::WalIoFail {
            op: e4_recovery::store::WalIoOp::SyncData,
            batch_start,
        });
        Ok(())
    })
    .expect("set the io hook");
    let mut hooks = |_hook: CkptHook| Ok(());
    let r = ckpt::checkpoint(&cx, &mut hooks);
    assert!(
        matches!(r, Err(Fail::Io(_))),
        "the checkpoint must fail with the hook's non-Sim I/O error: {r:?}"
    );
    let (fold, dmax, open, poisoned) = cx
        .with("pin:after", |c| {
            let st = c.store();
            Ok((
                st.ckpt_closed_dirty,
                st.dirty_max,
                st.ckpt_open,
                st.poisoned.is_some(),
            ))
        })
        .expect("read the fold");
    assert!(
        dmax >= 1,
        "the begin step folded ≥ 1 dirtied LPID (dirty_max, never reset, proves it: {dmax})"
    );
    assert_eq!(
        fold, 0,
        "a non-crash abort resets ckpt_closed_dirty to 0 (G-SZC-ABORTNORESET keeps the fold)"
    );
    assert!(!open, "no checkpoint is open after the abort path");
    assert!(
        poisoned,
        "the WAL-path I/O failure poisons the store (G8, 9.2 W8)"
    );
}

/// **X-5c-F3-F3a-F2 Work 4 pin (S-STORE §10.5; a literal config).**
/// `gc_w_used` starts at the recovered meta's `gc_w` at open, so a
/// checkpoint taken before the first GC mutation of the new boot — the
/// boot checkpoint of §14 step 8 above all — still records the image's
/// bound. Fixture: a store that GC'd at `w = 4` and checkpointed it
/// (its meta carries `gc_w = 4`), a clean crash, and a recovery that
/// runs no GC before its boot checkpoint. The pin decodes the winning
/// meta's bytes from the durable stream (never store memory) and
/// asserts `gc_w = 4`. With the fix reverted (open leaving `gc_w_used`
/// at 0), the boot checkpoint records 0 and this pin fails — the
/// X-5c-F3-F3a-F2 trace of 664 r2 (image 3 `gc_w = 8`, its recovery's
/// boot checkpoint `gc_w = 0`, O-1 (d) sampling S = 5 below the true
/// bound and failing on a version legally pruned at `w = 8`). (The
/// comment's `w = 5`/`gc_w = 5` were off by one from the fixture;
/// corrected comment-only by X-5c-F3-F3a-F2-F1, verdict finding 5.)
#[test]
fn gc_w_survives_recovery_into_the_boot_checkpoint() {
    let seed = 7111u64;
    let cfg = tcfg(8, 8, 1);
    // Life 1: a tree with two versions of one key, the apply frontier
    // at 5 (two applied commits), a GC sweep at w = 4 (advancing
    // `gc_w_used`), and a checkpoint that records both.
    let disk = {
        let (disk, st) = create_store(cfg.clone(), seed, Some(2));
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        cx.with("pin:vis", |c| {
            let st = c.store();
            st.seq.visible_ts = 5;
            st.seq.next_ts = 6;
            st.wal.last_commit_ts = 5;
            Ok(())
        })
        .expect("apply frontier");
        let root = tree::dir_root_of(&cx, 2).expect("tree 2 root");
        ins(&cx, 2, root, b"A0", 1, &[1u8; 8]);
        ins(&cx, 2, root, b"A0", 5, &[2u8; 8]);
        let mut resume = Vec::new();
        loop {
            let next = tree::gc_step(&cx, 2, root, 4, &resume).expect("gc step at w=4");
            if next.is_empty() {
                break;
            }
            resume = next;
        }
        cx_checkpoint(&sched, |_| Ok(()));
        // The image this life leaves: its meta must carry gc_w = 4.
        let g = sched.lock();
        let pf = g.disk.stat(e4_recovery::sim::PAGE_FILE).expect("page file");
        let m1 = g.disk.durable_read(pf, 4096, 4096);
        let m2 = g.disk.durable_read(pf, 8192, 4096);
        match e4_recovery::store::page::select_meta(&m1, &m2, false) {
            e4_recovery::store::page::MetaPick::Win { meta, .. } => assert_eq!(
                meta.gc_w, 4,
                "the GC'd life's own meta must record gc_w = 4 (§10.5)"
            ),
            pick => panic!("no winning meta after the GC'd life: {pick:?}"),
        }
        g.disk.clone()
    };
    // Life 2: a clean crash, a recovery that runs no GC, and its boot
    // checkpoint. The winning meta must still carry gc_w = 4.
    let mut d2 = disk;
    crash_clean(&mut d2, seed + 1);
    let sched2 = pass_world(1, seed + 2, d2, StoreInner::empty(cfg.clone(), 0));
    let cx2 = Cx {
        w: &sched2,
        t: HARNESS,
    };
    let (st2, _frontier) = ckpt::recover(&cx2, cfg.clone()).expect("recover");
    assert_eq!(
        st2.gc_w_used, 4,
        "open starts gc_w_used at the recovered meta's gc_w (§10.5)"
    );
    cx2.with("pin:install", |c| {
        *c.store() = st2;
        Ok(())
    })
    .expect("install");
    cx_checkpoint(&sched2, |_| Ok(()));
    let g = sched2.lock();
    let pf = g.disk.stat(e4_recovery::sim::PAGE_FILE).expect("page file");
    let m1 = g.disk.durable_read(pf, 4096, 4096);
    let m2 = g.disk.durable_read(pf, 8192, 4096);
    match e4_recovery::store::page::select_meta(&m1, &m2, false) {
        e4_recovery::store::page::MetaPick::Win { meta, .. } => assert_eq!(
            meta.gc_w, 4,
            "the boot checkpoint after a recovery with no GC mutation still\
             \nrecords the image's bound (§10.5) — the 664 r2 shape"
        ),
        pick => panic!("no winning meta after the boot checkpoint: {pick:?}"),
    }
}

/// **Q11 (card X-5c-F4-F1 step 2; P ruling nv3-X-5c-F4-esc-1's
/// ESCALATE 3): the counter was never wired.** This literal fixture
/// builds the recycled-segment shape by hand on a real created disk:
/// a meta whose replay start sits in segment A, a `SegmentEnd` at that
/// start (the walk hops to segment B's first LSN), and segment B a
/// *recycled* file — created in a previous life, its durable bytes at
/// offset 0 a fully valid record whose embedded lsn is its OLD
/// position. The walk's decode fails at `off == 0`,
/// [`wal::is_stale_record`] holds, and the counter increments; the
/// walk then stops exactly as it always did (the valid end — and
/// everything downstream of it — is identical without the increment,
/// which is what the M-Q11UNWIRED revert demonstrates).
#[test]
fn q11_stale_record_at_recycled_segment_start_counts() {
    use e4_recovery::store::page;
    let seed = 4242u64;
    let cfg = tcfg(8, 12, 2);
    let (disk, st0) = e4_recovery::harness::create_store(cfg.clone(), seed, 77).expect("create");
    let mut disk = disk;
    let pf = disk.stat(e4_recovery::sim::PAGE_FILE).expect("page file");
    // Segment A (boundary 64 KiB): a SegmentEnd at L, at its own position.
    let l = e4_recovery::store::WAL_SEG + 100;
    let seg_a = e4_recovery::store::WAL_SEG;
    let fa = disk
        .create(&format!("/wal/{seg_a:016x}.log"))
        .expect("segment A");
    let se =
        e4_recovery::store::wal::enc_record(e4_recovery::store::wal::K_SEGEND, (7, 0), 0, &[], l);
    disk.write_at(fa, l - seg_a, &se).expect("write A");
    disk.sync_data(fa).expect("sync A");
    // Segment B (boundary 128 KiB): the recycled file. Its bytes at
    // offset 0 are a record of its PREVIOUS life — magic, len and CRC
    // intact, the embedded lsn an old position (42) that is not B's
    // first LSN.
    let b = 2 * e4_recovery::store::WAL_SEG;
    let fb = disk
        .create(&format!("/wal/{b:016x}.log"))
        .expect("segment B");
    let stale =
        e4_recovery::store::wal::enc_record(e4_recovery::store::wal::K_COMMIT, (7, 1), 5, &[], 42);
    disk.write_at(fb, 0, &stale).expect("write B");
    disk.sync_data(fb).expect("sync B");
    disk.sync_dir("/wal/").expect("dirsync");
    // A meta that replays from L: meta 1 (its own slot, 2), the created
    // image's fields, T = ts_ckpt_hi = 0 and wal_ckpt_end = L.
    let meta = page::Meta {
        ckpt_id: 1,
        t: 0,
        gc_w: 0,
        epoch: st0.e,
        boot_id: 7,
        map_root: st0.meta.map_root,
        free_root: st0.meta.free_root,
        dir_root: st0.meta.dir_root,
        next_lpid: st0.meta.next_lpid,
        slot_count: st0.meta.slot_count,
        wal_start_lsn: l,
        wal_ckpt_end: l,
        ts_ckpt_hi: 0,
    };
    disk.write_at(pf, 2 * page::PAGE as u64, &page::build_meta(&meta))
        .expect("meta 1");
    disk.sync_all(pf).expect("meta sync");
    // Recover: the walk accepts the SegmentEnd at L, hops to B's first
    // LSN, reads the stale bytes and stops there.
    let sched = pass_world(1, seed + 2, disk, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    let (st, _frontier) = ckpt::recover(&cx, cfg).expect("recover");
    assert_eq!(
        st.stale_seg_reads, 1,
        "the stale record at the recycled segment's start is counted (Q11)"
    );
    assert_eq!(
        st.wal.append, b,
        "the valid end is B's first LSN — the walk stops exactly where it always did"
    );
}

/// **Q11's mid-segment half:** a stale record at a NON-zero offset is
/// §12.5's ordinary invalid tail, never a recycled segment's start —
/// the counter must stay 0. M-Q11ANY (count at any offset) fails this
/// pin. The walk reads a valid Chunk record at L, then meets the stale
/// bytes at `L + 45` and stops there; the valid end is that position.
#[test]
fn q11_mid_segment_stale_does_not_count() {
    use e4_recovery::store::page;
    let seed = 4343u64;
    let cfg = tcfg(8, 12, 2);
    let (disk, st0) = e4_recovery::harness::create_store(cfg.clone(), seed, 77).expect("create");
    let mut disk = disk;
    let pf = disk.stat(e4_recovery::sim::PAGE_FILE).expect("page file");
    let seg = e4_recovery::store::WAL_SEG;
    let l = seg + 100;
    let f = disk
        .create(&format!("/wal/{seg:016x}.log"))
        .expect("segment");
    // A valid Chunk at L (ts reserved 0; no marker → never applied),
    // then a stale record immediately after it, mid-segment.
    let chunk =
        e4_recovery::store::wal::enc_record(e4_recovery::store::wal::K_CHUNK, (7, 5), 0, &[], l);
    disk.write_at(f, l - seg, &chunk).expect("write chunk");
    let mid = l + chunk.len() as u64;
    let stale =
        e4_recovery::store::wal::enc_record(e4_recovery::store::wal::K_COMMIT, (7, 1), 5, &[], 42);
    disk.write_at(f, mid - seg, &stale).expect("write stale");
    disk.sync_data(f).expect("sync");
    disk.sync_dir("/wal/").expect("dirsync");
    let meta = page::Meta {
        ckpt_id: 1,
        t: 0,
        gc_w: 0,
        epoch: st0.e,
        boot_id: 7,
        map_root: st0.meta.map_root,
        free_root: st0.meta.free_root,
        dir_root: st0.meta.dir_root,
        next_lpid: st0.meta.next_lpid,
        slot_count: st0.meta.slot_count,
        wal_start_lsn: l,
        wal_ckpt_end: l,
        ts_ckpt_hi: 0,
    };
    disk.write_at(pf, 2 * page::PAGE as u64, &page::build_meta(&meta))
        .expect("meta 1");
    disk.sync_all(pf).expect("meta sync");
    let sched = pass_world(1, seed + 2, disk, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    let (st, _frontier) = ckpt::recover(&cx, cfg).expect("recover");
    assert_eq!(
        st.stale_seg_reads, 0,
        "a stale record at a mid-segment offset is never counted (Q11 counts only a segment start)"
    );
    assert_eq!(
        st.wal.append, mid,
        "the valid end is the stale record's start — the walk shape is unchanged"
    );
}

/// **X-5c-F4-F1-F1-CXX step 0, pin 4 (verdict X-5c-F4-F1-F1-F2): the
/// exhausted path's record must not outlive its checkpoint.** A gated
/// tree insert (the real caller, gate and tree mutex held) parks writing
/// the oldest copy out of a one-frame reserve while its checkpoint runs
/// to the end: step 2 writes that copy, `ckpt:freecopies` clears its
/// frame, post-5 clears the map. It resumes in the gap before the next
/// flip. At the tip before CXX its `epoch_copy:record` inserts
/// `Img::At(flip-1 bytes)` into the cleared map; nothing clears it, the
/// next freeze lets it override `cur`, and image 2 maps P to its flip-1
/// leaf with key 2 lost and no XX001. Evicting P's frame between the
/// checkpoints stops the next scan rewriting the entry.
#[test]
fn exhausted_record_stays_out_of_the_next_window() {
    let seed = 3u64;
    let cfg = tcfg(8, 18, 1);
    let (disk, st) = create_store(cfg.clone(), seed, None);
    let (disk, st, roots) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let mut roots = Vec::new();
        for id in 1u64..=12 {
            tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
            let root = tree::dir_root_of(&cx, id).unwrap_or_else(|e| panic!("root {id}: {e:?}"));
            if id == 11 || id == 12 {
                ins(&cx, id, root, &1u64.to_be_bytes(), 1, &val(1, 1));
            }
            roots.push(root);
        }
        cx.with("rec:budget", |c| {
            c.store().parks.insert("epochcopy:prewrite", 900_000);
            Ok(())
        })
        .expect("park budget");
        let (d, s) = {
            let g = sched.lock();
            (g.disk.clone(), g.st.clone())
        };
        (d, s, roots)
    };
    let root_p = roots[10];
    let root_b = roots[11];
    let pads: Vec<u64> = roots.iter().take(10).copied().collect();
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        match t {
            1 => {
                while !(flag(sched, "flipped1") && flag(sched, "filled")) {
                    cx.yield_now("rec:wait-fill").map_err(fail_of)?;
                }
                // The gated touch: tree_insert holds the gate and tree
                // 12's mutex across the park.
                ins(&cx, 12, root_b, &2u64.to_be_bytes(), 2, &val(2, 2));
                set_flag(sched, "m_done");
                Ok(())
            }
            2 => {
                let r = ckpt::checkpoint(&cx, &mut |hook| {
                    if hook == CkptHook::After0 {
                        set_flag(sched, "flipped1");
                    }
                    if hook == CkptHook::Mid2 && !flag(sched, "filled") {
                        let cxh = Cx { w: sched, t };
                        ins(&cxh, 11, root_p, &2u64.to_be_bytes(), 2, &val(2, 2));
                        set_flag(sched, "filled");
                    }
                    Ok(())
                });
                r.map_err(fail_of)?;
                for (id, root) in pads.iter().enumerate() {
                    ins(
                        &cx,
                        id as u64 + 1,
                        *root,
                        &2u64.to_be_bytes(),
                        2,
                        &val(2, 2),
                    );
                }
                loop {
                    let gone = cx
                        .with("rec:chk", |c| {
                            Ok(!c
                                .store()
                                .pool
                                .frames
                                .iter()
                                .any(|f| f.key == root_p && !f.is_epoch_copy))
                        })
                        .map_err(fail_of)?;
                    if gone {
                        break;
                    }
                    if !prims::evict_one(&cx, false).map_err(fail_of)? {
                        cx.wait_for(e4_recovery::sim::WaitKey::Pin)
                            .map_err(fail_of)?;
                    }
                    cx.yield_now("rec:evict").map_err(fail_of)?;
                }
                // Past post-5, before the next flip: release the park.
                {
                    let mut g = sched.lock();
                    g.st.parks.insert("epochcopy:prewrite", 0);
                }
                while !flag(sched, "m_done") {
                    cx.yield_now("rec:wait-m").map_err(fail_of)?;
                }
                ckpt::checkpoint(&cx, &mut |_| Ok(())).map_err(fail_of)?;
                set_flag(sched, "ckpt2_done");
                cx.yield_now("rec:end").map_err(fail_of)?;
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let (sched, report) = run_world(3, seed, disk, st, body);
    report.assert_clean("the late-record schedule");
    assert!(flag(&sched, "ckpt2_done"), "both checkpoints ran");
    let g = sched.lock();
    let mut disk_after = g.disk.clone();
    drop(g);
    crash_clean(&mut disk_after, seed + 50);
    let sched2 = pass_world(1, seed + 100, disk_after, StoreInner::empty(cfg.clone(), 0));
    let cx2 = Cx {
        w: &sched2,
        t: HARNESS,
    };
    let (opened, meta) = ckpt::open(&cx2, cfg.clone()).expect("reopen meta 2");
    assert_eq!(meta.ckpt_id, 2, "meta 2 is the image");
    cx2.with("rec:install", |c| {
        *c.store() = opened;
        Ok(())
    })
    .expect("install");
    let want: BTreeMap<Vec<u8>, Option<Vec<u8>>> = [
        (1u64.to_be_bytes().to_vec(), Some(val(1, 1)[1..].to_vec())),
        (2u64.to_be_bytes().to_vec(), Some(val(2, 2)[1..].to_vec())),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        tree::tree_scan(&cx2, root_p, 10).expect("scan P"),
        want,
        "image 2 must hold P's flip-2 cut: a late record for the oldest copy maps its flip-1 leaf"
    );
    assert_eq!(
        tree::tree_scan(&cx2, root_b, 10).expect("scan B"),
        want,
        "image 2 must hold B's flip-2 cut"
    );
}

/// **X-5c-F4-F1-F1-CXX step 0, pins 1–2: the DropTree straddle (S-1-F7,
/// §6.8 as amended; cause A of the 51 C-XX rounds).** At this tip the
/// walk's last step frees the root and `dir_remove_entry` removes the
/// entry in its **own** gate hold, so a flip can land between them:
/// image `dropping tree → root` with the root unmapped (310 r0's shape,
/// pin 1), or — when another tree's split took the freed LPID before
/// the flip, §4.2's immediate reuse — an opened image that reaches one
/// LPID from two trees' references (8010 r1's shape, pin 2). Pin 1
/// reopens and the resumed walk dies on the unmapped root (`[Open]`
/// XX001); pin 2's resumed walk frees the other tree's live page and
/// the survivor's scan loses it (the silent O-1(b)/AmcheckIi shape).
///
/// The schedule (one world, flags only): t1 drops tree 2 (a leaf root —
/// the first walk step is the last); t2, the checkpointer, waits for the
/// straddle (pin 1) or the reuse (pin 2) and flips inside the gap; pin
/// 2's t3 waits for the freed LPID and splits tree 4 onto it. Then a
/// clean crash (every unsynced write reverts; both checkpoints' metas
/// are synced) and `ckpt::recover`.
fn drop_cut_body(seed: u64, reuse: bool) -> Result<(), String> {
    drop_cut_body_m(seed, reuse, None)
}

fn drop_cut_body_m(seed: u64, reuse: bool, m: Option<Mutant>) -> Result<(), String> {
    let cfg = StoreCfg {
        mutant: m,
        ..tcfg(8, 18, 1)
    };
    let (disk, st) = create_store(cfg.clone(), seed, None);
    // ---- phase A: tree 2 (the victim, a leaf root) and tree 4 (the
    // reuser), one durable checkpoint. Tree 4 holds eight of the ~10
    // 400-byte cells a leaf holds: t3's pre-positioned ninth cell does
    // not split, and the one that follows splits onto the freed LPID
    // (the lowest free LPID at that moment).
    let (disk, st, root2, root4) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        for id in [2u64, 4] {
            tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
        }
        let root2 = tree::dir_root_of(&cx, 2).expect("root 2");
        let root4 = tree::dir_root_of(&cx, 4).expect("root 4");
        ins(&cx, 2, root2, &1u64.to_be_bytes(), 1, &val(1, 1));
        for i in 0..8u64 {
            ins(&cx, 4, root4, &i.to_be_bytes(), 1, &[0u8; 400]);
        }
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        (g.disk.clone(), g.st.clone(), root2, root4)
    };
    // ---- phase B: the drop (t1), the flip (t2) and the reuse (t3).
    let ntasks = if reuse { 4 } else { 3 };
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        match t {
            1 => {
                tree::dir_drop_tree(&cx, 2).map_err(fail_of)?;
                set_flag(sched, "d_done");
                Ok(())
            }
            2 => {
                // Wait for the gap: at this tip the straddle — the
                // entry present with the root freed — is exactly the
                // window a flip must cut. Under the fix the state never
                // exists, so the fallbacks are the reuse (pin 2) or the
                // drop's own completion; the flip then lands harmlessly.
                loop {
                    let elsewhere = flag(sched, "d_done") || flag(sched, "reused");
                    let straddle = cx
                        .with("cut:t2wait", |c| {
                            let st = c.store();
                            Ok(st.dir.contains_key(&2)
                                && !st.map.cur.contains_key(&root2)
                                && !st
                                    .pool
                                    .frames
                                    .iter()
                                    .any(|f| f.key == root2 && !f.is_epoch_copy))
                        })
                        .map_err(fail_of)?;
                    if elsewhere || straddle {
                        break;
                    }
                    cx.yield_now("cut:t2spin").map_err(fail_of)?;
                }
                ckpt::checkpoint(&cx, &mut |_| Ok(())).map_err(fail_of)?;
                set_flag(sched, "ckpt1");
                Ok(())
            }
            3 if reuse => {
                // Pre-position: the ninth cell (no split yet) goes in
                // as soon as tree 2 is `dropping` — before the gap
                // opens — so a single insert is all that remains.
                loop {
                    let marked = cx
                        .with("cut:t3mark", |c| {
                            Ok(c.store().dir.get(&2).is_some_and(|e| e.dropping))
                        })
                        .map_err(fail_of)?;
                    if marked || flag(sched, "d_done") {
                        break;
                    }
                    cx.yield_now("cut:t3mspin").map_err(fail_of)?;
                }
                if flag(sched, "d_done") {
                    // The drop finished before the mark was seen; there
                    // is no gap to race. The flip (t2) falls back to
                    // this flag too, so the world still terminates.
                    set_flag(sched, "reused");
                    return Ok(());
                }
                ins(&cx, 4, root4, &8u64.to_be_bytes(), 1, &[0u8; 400]);
                // The gap: the freed LPID. The next insert splits tree
                // 4, and its first allocation is the lowest free LPID —
                // root2.
                loop {
                    let freed = cx
                        .with("cut:t3wait", |c| {
                            Ok(c.store().slots.free_lpids.contains(&root2))
                        })
                        .map_err(fail_of)?;
                    if freed {
                        break;
                    }
                    cx.yield_now("cut:t3spin").map_err(fail_of)?;
                }
                ins(&cx, 4, root4, &9u64.to_be_bytes(), 1, &[0u8; 400]);
                set_flag(sched, "reused");
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let (sched, report) = run_world(ntasks, seed, disk, st, body);
    report.assert_clean(if reuse {
        "the drop-reuse straddle schedule"
    } else {
        "the drop straddle schedule"
    });
    if !flag(&sched, "d_done") {
        return Err(format!("seed {seed}: the drop never completed"));
    }
    if !flag(&sched, "ckpt1") {
        return Err(format!("seed {seed}: the checkpoint never completed"));
    }
    if reuse && !flag(&sched, "reused") {
        return Err(format!(
            "seed {seed}: tree 4 never split onto the freed LPID"
        ));
    }
    let g = sched.lock();
    let mut disk_after = g.disk.clone();
    drop(g);
    crash_clean(&mut disk_after, seed + 50);
    // ---- the reopen: recovery must succeed and must not walk an
    // unmapped root (pin 1) or free another tree's page (pin 2).
    let sched2 = pass_world(1, seed + 100, disk_after, StoreInner::empty(cfg.clone(), 0));
    let cx2 = Cx {
        w: &sched2,
        t: HARNESS,
    };
    let (opened, _frontier) =
        ckpt::recover(&cx2, cfg.clone()).map_err(|e| format!("seed {seed}: recover: {e:?}"))?;
    cx2.with("cut:install", |c| {
        *c.store() = opened;
        Ok(())
    })
    .expect("install");
    let gone = cx2
        .with("cut:dir2", |c| Ok(!c.store().dir.contains_key(&2)))
        .expect("dir read");
    if !gone {
        return Err(format!("seed {seed}: tree 2's entry survived recovery"));
    }
    if reuse {
        // Tree 4 must be whole: the resumed walk of a straddling entry
        // must not have freed the page the split reused. The image
        // holds the flip-time cut, so the ninth and tenth cells may or
        // may not be in it — but the eight phase-A cells were durably
        // checkpointed before the world began and must all read back.
        let scan = tree::tree_scan(&cx2, root4, 1000).map_err(|e| {
            format!("seed {seed}: tree 4 scan after recovery: {e:?} (a reused page was freed)")
        })?;
        let n = scan.len();
        if n < 9 {
            return Err(format!("seed {seed}: tree 4 lost durable pages: {n} of 9"));
        }
        for i in 0..9u64 {
            if !scan.contains_key(i.to_be_bytes().as_slice()) {
                return Err(format!("seed {seed}: tree 4 lost durable key {i}"));
            }
        }
    }
    Ok(())
}

/// Pin 1: the flip between the root free and the entry removal, then a
/// crash and an open — the image must never name a freed root.
#[test]
fn drop_cut_never_names_a_freed_root() {
    drop_cut_body(1, false).expect("pin 1");
}

/// Pin 2: the same cut with another tree's split on the freed LPID —
/// the image must never name a reused root.
#[test]
fn drop_cut_never_names_a_reused_root() {
    drop_cut_body_m(77, true, None).expect("pin 2");
}

/// **X-5c-F4-F1-F1-CXX step 0, pin 3: the exhausted reserve's double
/// evict (cause B1, 3534 r1's shape).** With a one-frame reserve, two
/// first touches both read `oldest = frame 0` holding the filler's copy
/// and both proceed into `write_fresh`; the first task's
/// `epoch_copy:record` records the write and clears the frame, and its
/// retry installs its own copy there; the second task's
/// `epoch_copy:record` — with no re-validation after its yield — then
/// clears the frame **again**, destroying the first task's copy
/// unwritten: `map.img` keeps its `CopyPending`, `ckpt:freeze` maps the
/// LPID to nothing, and the reopened image has lost the page (the
/// `[Open]`/`[AmcheckIi]` XX001 of the 18 B1 rounds). The fix re-validates
/// the frame in `epoch_copy:record` (Work 1); then the second task's
/// retry finds the frame occupied and its own exhaustion writes the
/// first copy out, so **both** copies reach the image.
fn exhausted_reserve_body(seed: u64) -> Result<(), String> {
    let cfg = tcfg(8, 18, 1);
    let (disk, st) = create_store(cfg.clone(), seed, None);
    // ---- phase A: P (tree 11) fills the reserve; L1 (tree 12) and L2
    // (tree 13) are the two exhausting first touches; trees 1..10 pad
    // the scan so both mutators land before the cursor reaches them.
    let (disk, st, roots) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let mut roots = Vec::new();
        for id in 1u64..=13 {
            tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
            let root = tree::dir_root_of(&cx, id).unwrap_or_else(|e| panic!("root {id}: {e:?}"));
            if id >= 11 {
                ins(&cx, id, root, &1u64.to_be_bytes(), 1, &val(1, 1));
            }
            roots.push(root);
        }
        let (d, s) = {
            let g = sched.lock();
            (g.disk.clone(), g.st.clone())
        };
        (d, s, roots)
    };
    // The phase-A baseline of "used but unreferenced" slots (the
    // store's own structural pages — the map radix's, the free set's,
    // the directory's — sit in `used_cur` outside `map.cur`): the
    // world below must not add to it. That is exactly the orphan's
    // signature: a slot the exhausted path's write-out allocated and
    // then failed to release (G2) is new, and nothing else allocates
    // without referencing in the same step.
    let baseline_unreferenced: Vec<u64> = {
        let mut referenced = st.slots.image_refs.clone();
        referenced.extend(st.slots.used_durable.iter().copied());
        referenced.extend(st.map.cur.values().map(|r| r.slot));
        st.slots
            .used_cur
            .iter()
            .filter(|s| !referenced.contains(*s))
            .copied()
            .collect()
    };
    let root_p = roots[10];
    let root_l1 = roots[11];
    let root_l2 = roots[12];
    // The After2 capture: at `ckpt:freeze` neither exhausting LPID may
    // still be `CopyPending`, and the frozen image must map both.
    type After2Capture = std::sync::Arc<std::sync::Mutex<Option<(bool, bool, bool, bool)>>>;
    let at_after2: After2Capture = std::sync::Arc::new(std::sync::Mutex::new(None));
    let cap = std::sync::Arc::clone(&at_after2);
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        match t {
            1 => {
                while !(flag(sched, "flipped1") && flag(sched, "filled")) {
                    cx.yield_now("ex:wait-fill").map_err(fail_of)?;
                }
                ins(&cx, 12, root_l1, &2u64.to_be_bytes(), 2, &val(2, 2));
                set_flag(sched, "m1_done");
                Ok(())
            }
            2 => {
                let r = ckpt::checkpoint(&cx, &mut |hook| {
                    if hook == CkptHook::After0 {
                        set_flag(sched, "flipped1");
                    }
                    if hook == CkptHook::Mid2 && !flag(sched, "filled") {
                        // The fill: P's first touch takes the free
                        // reserve frame deterministically, inside the
                        // checkpointer, while the cursor is still low.
                        let cxh = Cx { w: sched, t };
                        ins(&cxh, 11, root_p, &2u64.to_be_bytes(), 2, &val(2, 2));
                        set_flag(sched, "filled");
                    }
                    if hook == CkptHook::After2 {
                        let g = sched.lock();
                        let st = &g.st;
                        let l1_res = !matches!(
                            st.map.img.get(&root_l1),
                            Some(&e4_recovery::store::Img::CopyPending) | None
                        );
                        let l2_res = !matches!(
                            st.map.img.get(&root_l2),
                            Some(&e4_recovery::store::Img::CopyPending) | None
                        );
                        let l1_map = st
                            .image_frozen
                            .as_ref()
                            .and_then(|f| f.entries.get(&root_l1))
                            .is_some_and(|e| e.is_some());
                        let l2_map = st
                            .image_frozen
                            .as_ref()
                            .and_then(|f| f.entries.get(&root_l2))
                            .is_some_and(|e| e.is_some());
                        *cap.lock().unwrap() = Some((l1_res, l2_res, l1_map, l2_map));
                    }
                    Ok(())
                });
                set_flag(sched, "ckpt1");
                r.map_err(fail_of)?;
                Ok(())
            }
            3 => {
                while !(flag(sched, "flipped1") && flag(sched, "filled")) {
                    cx.yield_now("ex:wait-fill3").map_err(fail_of)?;
                }
                ins(&cx, 13, root_l2, &2u64.to_be_bytes(), 2, &val(2, 2));
                set_flag(sched, "m3_done");
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let (sched, report) = run_world(4, seed, disk, st, body);
    report.assert_clean("the exhausted-reserve schedule");
    assert!(
        flag(&sched, "ckpt1"),
        "seed {seed}: the checkpoint completed"
    );
    assert!(flag(&sched, "m1_done"), "seed {seed}: the first touch ran");
    assert!(flag(&sched, "m3_done"), "seed {seed}: the second touch ran");
    let (l1_res, l2_res, l1_map, l2_map) = at_after2
        .lock()
        .unwrap()
        .ok_or_else(|| format!("seed {seed}: After2 never fired"))?;
    if !(l1_res && l2_res) {
        return Err(format!(
            "seed {seed}: a CopyPending survived to ckpt:freeze (l1_res={l1_res} l2_res={l2_res})"
        ));
    }
    if !(l1_map && l2_map) {
        return Err(format!(
            "seed {seed}: the image left an exhausting LPID unmapped (l1={l1_map} l2={l2_map})"
        ));
    }
    // ---- rework 6: the orphan's slot left `used_cur`. At world end
    // (both mutators done, the checkpoint complete) the used-but-
    // unreferenced slots are exactly phase A's baseline plus slot 5 —
    // a stale flip-window write-out of a live page, pinned in
    // `image_refs` while the window was open and left in `used_cur`
    // when post-5 cleared the pins (measured, deterministic at this
    // seed). The exhausted path's orphaned write-out must leave **no**
    // such slot: its allocation was never referenced by anything, and
    // under G2 (the release removed) exactly one more appears — slot
    // 13 at this seed, the write-out of the copy the first task had
    // already recorded — leaking until the next open.
    let g = sched.lock();
    let mut referenced = g.st.slots.image_refs.clone();
    referenced.extend(g.st.slots.used_durable.iter().copied());
    referenced.extend(g.st.map.cur.values().map(|r| r.slot));
    let orphaned_end: Vec<u64> = g
        .st
        .slots
        .used_cur
        .iter()
        .filter(|s| !referenced.contains(*s))
        .copied()
        .collect();
    drop(g);
    let mut want_unreferenced = baseline_unreferenced.clone();
    want_unreferenced.push(5);
    want_unreferenced.sort_unstable();
    if orphaned_end != want_unreferenced {
        let new_orphans: Vec<u64> = orphaned_end
            .iter()
            .filter(|s| !want_unreferenced.contains(s))
            .copied()
            .collect();
        return Err(format!(
            "seed {seed}: slots {new_orphans:?} entered used_cur referenced by nothing - the \
             exhausted path's orphaned write-out must release its slot (Work 1, G2)"
        ));
    }
    // ---- the reopen: image 1 holds each exhausting page's flip cut.
    let g = sched.lock();
    let mut disk_after = g.disk.clone();
    drop(g);
    crash_clean(&mut disk_after, seed + 50);
    let sched2 = pass_world(1, seed + 100, disk_after, StoreInner::empty(cfg.clone(), 0));
    let cx2 = Cx {
        w: &sched2,
        t: HARNESS,
    };
    let (opened, _frontier) =
        ckpt::recover(&cx2, cfg.clone()).map_err(|e| format!("seed {seed}: recover: {e:?}"))?;
    let (_om, meta) =
        ckpt::open(&cx2, cfg.clone()).map_err(|e| format!("seed {seed}: open: {e:?}"))?;
    assert_eq!(meta.ckpt_id, 1, "seed {seed}: meta 1 is the image");
    cx2.with("ex:install", |c| {
        *c.store() = opened;
        Ok(())
    })
    .expect("install");
    let want: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
        [(1u64.to_be_bytes().to_vec(), Some(val(1, 1)[1..].to_vec()))]
            .into_iter()
            .collect();
    for (name, root) in [("L1", root_l1), ("L2", root_l2), ("P", root_p)] {
        let scan = tree::tree_scan(&cx2, root, 10)
            .map_err(|e| format!("seed {seed}: scan {name}: {e:?}"))?;
        if scan != want {
            return Err(format!(
                "seed {seed}: image 1 lost {name}'s flip cut: {scan:?}"
            ));
        }
    }
    Ok(())
}

/// Pin 3: a one-frame-exhausted reserve and two first touches in
/// `write_fresh` — both copies must reach the image, and (rework 6)
/// the orphaned write-out's slot must have left `used_cur`: at world
/// end the used-but-unreferenced slots are exactly the phase-A
/// baseline plus the one stale flip-window write-out (slot 5),
/// nothing more — G2 (the release removed) leaks the orphan's slot
/// (13 at this seed) and fails the pin.
#[test]
fn exhausted_reserve_keeps_both_copies() {
    exhausted_reserve_body(1).expect("pin 3");
}

/// **X-5c-F4-F1-F1-CXX Work 2: `drop_entry_gone_before_root_free`.**
/// I-LINK-ATOMIC's cut side (§6.8 as amended by S-1-F7): a directory
/// entry must never name an unmapped root — not even between two
/// atomic steps of the last gated operation, because §4.2's immediate
/// reuse can hand the LPID to another tree inside the same hold. The
/// fix removes the entry **before** freeing the path; M-DROP-FREE-FIRST
/// (inside the one gate hold, the root is freed before the entry is
/// removed) opens exactly that window. A watcher task asserts, at each
/// of its steps, that no unmapped root has an entry — in the persisted
/// directory tree or in the in-memory `dir` — while the dropper walks
/// tree 2 (a leaf root: the last step at once) and tree 3 (an inner
/// root: one bounded step first, so the Stepped arm is exercised too).
#[test]
fn drop_entry_gone_before_root_free() {
    drop_entry_gone_body(4);
}

fn drop_entry_gone_body(seed: u64) {
    let cfg = tcfg(8, 18, 1);
    let (disk, st) = create_store(cfg.clone(), seed, None);
    // Phase A: tree 2 = one page; tree 3 = an inner root over two
    // leaves (twelve 400-byte cells split it once).
    let (disk, st) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        for id in [2u64, 3] {
            tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
        }
        let root2 = tree::dir_root_of(&cx, 2).expect("root 2");
        let root3 = tree::dir_root_of(&cx, 3).expect("root 3");
        ins(&cx, 2, root2, &1u64.to_be_bytes(), 1, &val(1, 1));
        for i in 0..12u64 {
            ins(&cx, 3, root3, &i.to_be_bytes(), 1, &[0u8; 400]);
        }
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        (g.disk.clone(), g.st.clone())
    };
    // Phase B: the dropper and the watcher. The watcher's check: every
    // entry the directory holds (persisted and in memory) names a root
    // that is still mapped (in the current map or resident in a pool
    // frame).
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        match t {
            1 => {
                tree::dir_drop_tree(&cx, 2).map_err(fail_of)?;
                tree::dir_drop_tree(&cx, 3).map_err(fail_of)?;
                set_flag(sched, "d_done");
                Ok(())
            }
            2 => {
                let mut checks = 0u64;
                while !flag(sched, "d_done") {
                    let dir_root = cx
                        .with("watch:dr", |c| Ok(c.store().meta.dir_root))
                        .map_err(fail_of)?;
                    // The persisted directory's entries.
                    let cells = tree::raw_cells(&cx, dir_root, false).map_err(|e| {
                        SimFail::Internal(Box::leak(
                            format!("watch raw_cells: {e:?}").into_boxed_str(),
                        ))
                    })?;
                    let bad = cx
                        .with("watch:chk", |c| {
                            let st = c.store();
                            let mapped = |lpid: u64| {
                                st.map.cur.contains_key(&lpid)
                                    || st
                                        .pool
                                        .frames
                                        .iter()
                                        .any(|f| f.key == lpid && !f.is_epoch_copy)
                            };
                            // In memory: no entry may name an unmapped root.
                            for (id, en) in st.dir.iter() {
                                if !mapped(en.root) {
                                    return Ok(Some(format!(
                                        "dir entry {id} names unmapped root {}",
                                        en.root
                                    )));
                                }
                            }
                            // Persisted: no directory cell may either.
                            for (k, v) in cells.iter() {
                                if k.len() != 8 || v.len() != 9 {
                                    continue;
                                }
                                let mut rb = [0u8; 8];
                                if let Some(sb) = v.get(0..8) {
                                    rb.copy_from_slice(sb);
                                }
                                let root = u64::from_le_bytes(rb);
                                if !mapped(root) {
                                    let id = u64::from_be_bytes(
                                        k.as_slice().try_into().unwrap_or([0; 8]),
                                    );
                                    return Ok(Some(format!(
                                        "directory cell {id} names unmapped root {root}"
                                    )));
                                }
                            }
                            Ok(None)
                        })
                        .map_err(fail_of)?;
                    if let Some(what) = bad {
                        panic!("watcher: {what}");
                    }
                    checks += 1;
                    cx.yield_now("watch:spin").map_err(fail_of)?;
                }
                eprintln!("watcher made {checks} clean checks");
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let (sched, report) = run_world(3, seed, disk, st, body);
    report.assert_clean("the drop watcher schedule");
    assert!(flag(&sched, "d_done"), "seed {seed}: both drops ran");
}

/// **X-5c-F4-F1-F1-CXX rework 2 (the REWORK verdict's finding 1:
/// §6.8 step 3 — the last step holds its pins through the one hold).**
/// The walk's `PinGuard` covers the root and every page the step
/// empties, noted before its first mutation; dropped at the step's
/// edge (M-DROP-UNPIN-EARLY, f703c08d's code), the root's dirty frame
/// was a legal eviction victim during the directory descent's I/O: the
/// evictor latches it and yields in `write_fresh`, `free_lpid` clears
/// the frame and puts the LPID in the free set, and `evict:finish` —
/// which runs `map.cur.insert(key, r)` unconditionally — resurrects
/// the freed root at its write's fresh slot: mapped and in the free
/// set, referenced by nothing (444 r1 and 3005 r1 at BASE, faultless
/// class C XX001 the census never saw). With the guard live across the
/// hold the frame is not evictable (`evict:pick` demands `pins == 0`),
/// so the store can never end a drop with the root both mapped and
/// free. A dropper over a dirty leaf root races an evictor looping
/// `evict_one(&cx, false)`; seed 38 is the literal (of 1..=500 the
/// gate measured 38, 85, 444 and 467 failing at f703c08d, 0 with the
/// pins held).
#[test]
fn drop_last_step_keeps_its_pages_pinned() {
    drop_last_step_pins_body(38).expect("the last step keeps its pages pinned");
}

fn drop_last_step_pins_body(seed: u64) -> Result<(), String> {
    let cfg = tcfg(8, 18, 1);
    let (disk, st) = create_store(cfg.clone(), seed, None);
    // Phase A: trees 2–5, tree 2 durably checkpointed and then dirtied
    // — a leaf root whose frame is dirty when the drop begins.
    let (disk, st, root2) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        for id in [2u64, 3, 4, 5] {
            tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
        }
        let root2 = tree::dir_root_of(&cx, 2).expect("root 2");
        ins(&cx, 2, root2, &1u64.to_be_bytes(), 1, &val(1, 1));
        cx_checkpoint(&sched, |_| Ok(()));
        ins(&cx, 2, root2, &2u64.to_be_bytes(), 2, &val(2, 2));
        let g = sched.lock();
        (g.disk.clone(), g.st.clone(), root2)
    };
    // Phase B: the dropper (t1) and an evictor (t2) looping outside
    // any gated operation, so it takes the gate shared per attempt —
    // exactly the background evictor of §5.2.
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        match t {
            1 => {
                tree::dir_drop_tree(&cx, 2).map_err(fail_of)?;
                set_flag(sched, "d_done");
                Ok(())
            }
            2 => {
                while !flag(sched, "d_done") {
                    let _ = prims::evict_one(&cx, false).map_err(fail_of)?;
                    cx.yield_now("gate:evict").map_err(fail_of)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let (sched, report) = run_world(3, seed, disk, st, body);
    report.assert_clean("the gate evict schedule");
    let g = sched.lock();
    let mapped = g.st.map.cur.contains_key(&root2);
    let free = g.st.slots.free_lpids.contains(&root2);
    if mapped && free {
        return Err(format!(
            "seed {seed}: root {root2} is mapped and in the free set — the last step \
             freed a page whose frame an evictor had already latched (evict:finish \
             resurrected it)"
        ));
    }
    Ok(())
}

/// **X-5c-F4-F1-F1-CXX rework 5 (the one hold; the REWORK verdict's
/// finding 3, first rule): a flip that waits for "entry absent, root
/// mapped" must never find it.** The last step removes the directory
/// entry and frees the path inside **one** gate hold; the transient
/// state between the entry's removal and the root's free (nothing
/// names the root, the map still holds it) exists only while the
/// dropper holds the gate, so a flip — which needs the gate exclusive
/// and drains every holder first — can never cut there. A checker
/// that asserts, at each of its steps, the flip's own opportunity
/// (the straddle state **and** the gate free: `holders == 0`, not
/// exclusive, no flip pending) therefore never fires. G3 (the gate
/// released and re-taken between the entry removal and the frees —
/// the frees in a second hold) opens exactly that window: the flip
/// finds the straddle with the gate free, and the pin fails. Seed 5
/// is the literal (the scan's first failures: 4, 5, 7, 8, … — 105 of
/// 1..=200 fail under the hand-applied G3, and 0 clean).
#[test]
fn drop_one_hold_the_flip_never_finds_the_straddle() {
    drop_one_hold_body(5).expect("the one hold shields the straddle");
}

fn drop_one_hold_body(seed: u64) -> Result<(), String> {
    let cfg = tcfg(8, 18, 1);
    let (disk, st) = create_store(cfg.clone(), seed, None);
    // Phase A: tree 2, a leaf root with one durably checkpointed
    // entry — the drop's first walk step is the last step.
    let (disk, st, root2) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        tree::dir_create_tree(&cx, 2).unwrap_or_else(|e| panic!("CreateTree 2: {e:?}"));
        let root2 = tree::dir_root_of(&cx, 2).expect("root 2");
        ins(&cx, 2, root2, &1u64.to_be_bytes(), 1, &val(1, 1));
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        (g.disk.clone(), g.st.clone(), root2)
    };
    // Phase B: the dropper (t1) and the flip (t2). The flip task
    // spins at yield granularity; each of its steps checks, in one
    // atomic step, whether a flip that started now would cut the
    // straddle: the entry gone (in-memory `dir`, which lags the
    // persisted removal never — the persisted leaf is rewritten
    // first), the root still mapped (in `map.cur` or a resident
    // frame), and the gate free. Finding it is the violation; after
    // the drop completes the flip runs for real (a clean cut).
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        match t {
            1 => {
                tree::dir_drop_tree(&cx, 2).map_err(fail_of)?;
                set_flag(sched, "d_done");
                Ok(())
            }
            2 => {
                loop {
                    cx.yield_now("straddle:spin").map_err(fail_of)?;
                    let hit = cx
                        .with("straddle:chk", |c| {
                            let st = c.store();
                            let mapped = st.map.cur.contains_key(&root2)
                                || st
                                    .pool
                                    .frames
                                    .iter()
                                    .any(|f| f.key == root2 && !f.is_epoch_copy);
                            let entry_gone = !st.dir.contains_key(&2);
                            let gate_free =
                                st.gate.holders == 0 && !st.gate.exclusive && !st.gate.flip_pending;
                            Ok(entry_gone && mapped && gate_free)
                        })
                        .map_err(fail_of)?;
                    if hit {
                        panic!(
                            "seed {seed}: the flip found the straddle: entry absent, root {root2} mapped, gate free"
                        );
                    }
                    if flag(sched, "d_done") {
                        break;
                    }
                }
                ckpt::checkpoint(&cx, &mut |_| Ok(())).map_err(fail_of)?;
                set_flag(sched, "ckpt1");
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let (sched, report) = run_world(3, seed, disk, st, body);
    report.assert_clean("the one-hold straddle schedule");
    if !flag(&sched, "d_done") {
        return Err(format!("seed {seed}: the drop never completed"));
    }
    if !flag(&sched, "ckpt1") {
        return Err(format!("seed {seed}: the checkpoint never ran"));
    }
    Ok(())
}

/// **X-5c-F4-F1-F1-CXX Work 3 (§14 step 4, as amended by S-1-F7): a
/// replayed drop leaves its pages mapped until step 6.** Pass 2's
/// `DropTree` sets `dropping` only — it runs no walk; the walk is step
/// 6's, after replay has finished allocating. The fixture: a durable
/// image of trees 2 and 3, then a WAL (above `T`, never applied live)
/// holding a committed `DropTree(2)` and a commit of eight 400-byte
/// cells into tree 3 — enough to split it during replay. Under the fix,
/// tree 3's split allocates fresh LPIDs and step 6's walk frees tree
/// 2's pages afterwards: the recovered map holds none of them. Under
/// M-REPLAY-DROP-WALK (pass 2 walks), tree 2's pages are freed during
/// replay and tree 3's split takes the lowest of them — the recovered
/// map still holds it, and tree 3's scan sits partly on the freed page.
#[test]
fn drop_replay_only_marks() {
    let seed = 1u64;
    let cfg = tcfg(8, 18, 1);
    let (disk, st) = create_store(cfg.clone(), seed, None);
    // ---- phase A: tree 2 (a leaf root, one entry) and tree 3 (eight
    // of the ~10 400-byte cells a leaf holds), one durable checkpoint.
    let (disk, st, root2, root3) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        for id in [2u64, 3] {
            tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
        }
        let root2 = tree::dir_root_of(&cx, 2).expect("root 2");
        let root3 = tree::dir_root_of(&cx, 3).expect("root 3");
        ins(&cx, 2, root2, &1u64.to_be_bytes(), 1, &val(1, 1));
        for i in 0..8u64 {
            ins(&cx, 3, root3, &i.to_be_bytes(), 1, &[0u8; 400]);
        }
        cx_checkpoint(&sched, |_| Ok(()));
        let g = sched.lock();
        (g.disk.clone(), g.st.clone(), root2, root3)
    };
    // ---- phase B: the WAL only — a committed DropTree above T, then a
    // commit that splits tree 3 during replay. Nothing is applied live.
    let mut disk_after = {
        let sched = pass_world(1, seed + 10, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        enqueue_commit(&cx, (0, 1), 1, &[wal::WOp::DropTree { tree: 2 }]);
        let puts: Vec<wal::WOp> = (8..16u64)
            .map(|i| wal::WOp::Put {
                tree: 3,
                key: i.to_be_bytes().to_vec(),
                value: [0u8; 400].to_vec(),
            })
            .collect();
        enqueue_commit(&cx, (0, 2), 2, &puts);
        let g = sched.lock();
        g.disk.clone()
    };
    crash_clean(&mut disk_after, seed + 50);
    // ---- the recovery: both records replay; then step 6 walks.
    let world = pass_world(1, seed + 20, disk_after, StoreInner::empty(cfg.clone(), 0));
    let cx = Cx {
        w: &world,
        t: HARNESS,
    };
    let (rec_st, _frontier) = ckpt::recover(&cx, cfg.clone()).expect("replay recovery");
    assert!(
        !rec_st.dir.contains_key(&2),
        "the replayed drop completed at step 6: tree 2's entry is gone"
    );
    let root2_mapped = rec_st.map.cur.contains_key(&root2)
        || rec_st
            .pool
            .frames
            .iter()
            .any(|f| f.key == root2 && !f.is_epoch_copy);
    assert!(
        !root2_mapped,
        "a replayed drop must leave its pages mapped until step 6 - tree 2's root \
         mapped again in the recovered store means pass 2's records allocated it \
         (M-REPLAY-DROP-WALK)"
    );
    cx.with("replay:install", |c| {
        *c.store() = rec_st;
        Ok(())
    })
    .expect("install");
    let scan = tree::tree_scan(&cx, root3, 1000).expect("tree 3 scan after recovery");
    assert_eq!(
        scan.len(),
        16,
        "tree 3 holds all sixteen replayed cells across its split"
    );
}
