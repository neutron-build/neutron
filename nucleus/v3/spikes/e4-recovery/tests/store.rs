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
