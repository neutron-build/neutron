//! SimDisk and scheduler self-tests (card X-5 Work 1.5). P-written checks:
//! per-sector prefix semantics, sync coverage per file, `sync_data` length
//! growth, the directory-op prefix rule, taint across lives, scheduler
//! determinism under a fixed seed, and deadlock detection. Plus the gate
//! REWORK (2026-10-23) pinned tests: taint over a read starting inside an
//! issued range, the accept hook's covering-read sub-range rule, a synced
//! shrink surviving the crash, and F-MEDIA-WAL drawing across all
//! segments.

use std::sync::Arc;

use e4_recovery::rng::Rng;
use e4_recovery::sim::{
    DiskEvent, E4Env, EnvRef, FaultKnobs, HARNESS, HasDisk, PAGE_FILE, ROOT_DIR, Sched, SchedCore,
    SchedHost, SimDisk, SimFail, TaskId, WAL_DIR, WaitKey,
};

/// A minimal world for these tests: just the scheduler core and the disk.
struct TestHost {
    sched: SchedCore,
    disk: SimDisk,
}

impl SchedHost for TestHost {
    fn sched_mut(&mut self) -> &mut SchedCore {
        &mut self.sched
    }
}

impl HasDisk for TestHost {
    fn disk(&mut self) -> &mut SimDisk {
        &mut self.disk
    }
}

/// Run `body(task)` on `ntasks` threads under the baton; returns the world
/// afterwards. Panics inside a task fail the test (PANIC).
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
fn run_world<F>(ntasks: usize, seed: u64, body: F) -> Sched<TestHost>
where
    F: Fn(&Sched<TestHost>, TaskId) -> Result<(), SimFail> + Send + Sync + 'static,
{
    let host = TestHost {
        sched: SchedCore::new(ntasks, seed),
        disk: SimDisk::new(),
    };
    let sched = Arc::new(Sched::new(host));
    let body = &body;
    let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for t in 1..ntasks {
            let sched = Arc::clone(&sched);
            let panicked = Arc::clone(&panicked);
            handles.push(scope.spawn(move || {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&sched, t)));
                if r.is_err() {
                    panicked.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                sched.finish(t);
            }));
        }
        // The harness parks until all tasks end.
        let _ = sched.start();
        for h in handles {
            let _ = h.join();
        }
    });
    assert!(
        !panicked.load(std::sync::atomic::Ordering::SeqCst),
        "a task panicked"
    );
    Arc::try_unwrap(sched).unwrap_or_else(|_| panic!("scheduler still shared"))
}

/// 1.5 (1): per-sector prefix semantics — no sector ever takes a value
/// outside its own history (durable + this-life volatile write images).
#[test]
fn sector_prefix_semantics() {
    for seed in 0..64u64 {
        let mut rng = Rng::new(seed);
        for (keep, tear) in [(0, 0), (100, 900), (900, 100), (500, 500)] {
            let mut disk = SimDisk::new();
            let f = disk.create(PAGE_FILE).unwrap();
            disk.sync_dir(ROOT_DIR).unwrap();
            // Durable base: bytes 0..1024 = 0xAA, synced.
            let base = vec![0xAAu8; 1024];
            disk.write_at(f, 0, &base).unwrap();
            disk.sync_data(f).unwrap();
            // W1 covers sectors 0..2 (bytes 0..1024) with 0x11.
            disk.write_at(f, 0, &[0x11u8; 1024]).unwrap();
            // W2 covers only sector 1 (bytes 512..1024) with 0x22.
            disk.write_at(f, 512, &[0x22u8; 512]).unwrap();
            let knobs = FaultKnobs {
                keep_latest_pm: keep,
                tear_pm: tear,
            };
            disk.apply_crash(&mut rng, &knobs);
            // Sector 0 history: {AA..} durable, {11..} after W1.
            let s0 = disk.durable_read(f, 0, 512);
            assert!(
                s0.iter().all(|&b| b == 0xAA) || s0.iter().all(|&b| b == 0x11),
                "sector 0 took a value outside its history: {s0:?}.."
            );
            // Sector 1 history: {AA}, {11}, {22}.
            let s1 = disk.durable_read(f, 512, 512);
            let all_aa = s1.iter().all(|&b| b == 0xAA);
            let all_11 = s1.iter().all(|&b| b == 0x11);
            let all_22 = s1.iter().all(|&b| b == 0x22);
            assert!(
                all_aa || all_11 || all_22,
                "sector 1 took a value outside its history: {s1:?}.."
            );
        }
    }
}

/// 1.5 (2): sync coverage per file — a synced file's writes survive a
/// drop-everything crash; an unsynced file's do not.
#[test]
fn sync_covers_only_its_file() {
    let mut rng = Rng::new(7);
    let mut disk = SimDisk::new();
    let a = disk.create("/a").unwrap();
    let b = disk.create("/b").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    disk.write_at(a, 0, &[1u8; 64]).unwrap();
    disk.write_at(b, 0, &[2u8; 64]).unwrap();
    disk.sync_data(a).unwrap();
    let knobs = drop_all();
    disk.apply_crash(&mut rng, &knobs);
    assert_eq!(disk.durable_read(a, 0, 64), vec![1u8; 64]);
    assert_eq!(
        disk.durable_read(b, 0, 64),
        vec![0u8; 64],
        "b was never synced"
    );
}

/// 1.5 (3): `sync_data` covers the length growth needed to read the data
/// back (POSIX `fdatasync`).
#[test]
fn sync_data_covers_length_growth() {
    let mut rng = Rng::new(11);
    let mut disk = SimDisk::new();
    let f = disk.create("/f").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    disk.write_at(f, 0, &[9u8; 16]).unwrap();
    disk.sync_data(f).unwrap();
    assert_eq!(disk.durable_len(f), 16);
    // Grow by writing past the end, then crash dropping all volatile
    // writes: the earlier synced extent survives.
    disk.write_at(f, 600, &[7u8; 32]).unwrap();
    disk.apply_crash(&mut rng, &drop_all());
    assert_eq!(disk.durable_len(f), 16);
    assert_eq!(disk.durable_read(f, 0, 16), vec![9u8; 16]);
    // Now with a sync: the growth is durable.
    disk.write_at(f, 600, &[7u8; 32]).unwrap();
    disk.sync_data(f).unwrap();
    assert_eq!(disk.durable_len(f), 632);
}

/// 1.5 (4): the directory-op prefix rule — volatile ops persist as a
/// prefix of issue order; `sync_dir` makes them durable.
#[test]
fn directory_prefix_rule() {
    for seed in 0..32u64 {
        let mut rng = Rng::new(1000 + seed);
        let mut disk = SimDisk::new();
        // No sync_dir at all: every op below is volatile, including the
        // first create. The durable base namespace is empty.
        let f = disk.create("/one").unwrap();
        let _ = disk.write_at(f, 0, &[1u8; 8]);
        let _g = disk.create("/two").unwrap();
        disk.rename("/one", "/uno").unwrap();
        let _h = disk.create("/three").unwrap();
        disk.unlink("/two").unwrap();
        disk.apply_crash(
            &mut rng,
            &FaultKnobs {
                keep_latest_pm: 0,
                tear_pm: 0,
            },
        );
        // The surviving ops are some prefix of the five; the namespace must
        // equal the replay of one of the six prefixes.
        let seq = [
            "create /one",
            "create /two",
            "rename /one /uno",
            "create /three",
            "unlink /two",
        ];
        let mut matched = false;
        for k in 0..=seq.len() {
            let mut ns: Vec<&str> = Vec::new();
            for op in seq.iter().take(k) {
                match *op {
                    "create /one" => ns.push("/one"),
                    "create /two" => ns.push("/two"),
                    "rename /one /uno" => {
                        ns.retain(|n| *n != "/one");
                        ns.push("/uno");
                    }
                    "create /three" => ns.push("/three"),
                    "unlink /two" => ns.retain(|n| *n != "/two"),
                    _ => {}
                }
            }
            let mut got: Vec<String> = disk
                .list_dir(ROOT_DIR)
                .into_iter()
                .map(|(n, _)| format!("/{n}"))
                .collect();
            got.sort_unstable();
            let mut want: Vec<String> = ns.iter().map(|s| s.to_string()).collect();
            want.sort_unstable();
            if got == want {
                matched = true;
                break;
            }
        }
        assert!(
            matched,
            "namespace is no prefix replay: {:?}",
            disk.list_dir(ROOT_DIR)
        );
    }
    // And with a sync_dir first, everything persists even on a total drop.
    let mut rng = Rng::new(3);
    let mut disk = SimDisk::new();
    let _ = disk.create("/x").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    disk.apply_crash(&mut rng, &drop_all());
    assert_eq!(disk.list_dir(ROOT_DIR).len(), 1);
}

/// 1.5 (5): taint tracking across lives — a read that returns bytes other
/// than the last issued bytes is tainted, in the next life too.
#[test]
fn taint_across_lives() {
    let mut rng = Rng::new(5);
    let mut disk = SimDisk::new();
    let f = disk.create("/f").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    disk.write_at(f, 0, &[1u8; 64]).unwrap();
    disk.sync_data(f).unwrap();
    disk.write_at(f, 0, &[2u8; 64]).unwrap(); // volatile, will drop
    disk.apply_crash(&mut rng, &drop_all());
    disk.new_round();
    // Next life: reading the range returns the durable 1s; the last issued
    // bytes were the 2s: tainted.
    let _ = disk.read_at(f, 0, 64).unwrap();
    assert!(disk.reads.last().unwrap().tainted);
    // A range never issued to is untainted whatever it returns.
    let _ = disk.read_at(f, 4096, 64).unwrap();
    assert!(!disk.reads.last().unwrap().tainted);
    // Accepting the tainted read is a served corruption.
    disk.accepted(f, 0, 64);
    assert_eq!(disk.served.len(), 1);
}

/// 1.5 (6): determinism of the scheduler under a fixed seed — identical
/// handoff traces across runs, and a different seed changes the trace.
#[test]
fn scheduler_determinism() {
    let body = |sched: &Sched<TestHost>, t: TaskId| -> Result<(), SimFail> {
        for i in 0..40u64 {
            // Deterministic work interleaved with yields.
            let _ = (t as u128) + (i as u128);
            sched.yield_baton(t)?;
        }
        // Block on a key that task 0 never releases until the end.
        if t == 2 {
            sched.wait_for(t, WaitKey::Tree(9))?;
        }
        Ok(())
    };
    let w1 = run_world(4, 42, body);
    let w2 = run_world(4, 42, body);
    let mut s1 = w1.lock();
    let mut s2 = w2.lock();
    assert_eq!(
        s1.sched_mut().trace,
        s2.sched_mut().trace,
        "same seed, same trace"
    );
    assert_eq!(s1.sched_mut().yields, s2.sched_mut().yields);
    drop(s1);
    drop(s2);
    let w3 = run_world(4, 43, body);
    let mut s3 = w3.lock();
    let s4 = run_world(4, 42, body);
    let mut s4 = s4.lock();
    assert_ne!(
        s3.sched_mut().trace,
        s4.sched_mut().trace,
        "different seed should change the schedule"
    );
}

/// 1.5 (7): deadlock detection — two tasks each blocked forever on a key
/// nobody releases.
#[test]
fn deadlock_detection() {
    let body = |sched: &Sched<TestHost>, t: TaskId| -> Result<(), SimFail> {
        if t == 1 {
            // Let task 2 park first, deterministically: yield a few times
            // before parking ourselves.
            for _ in 0..3 {
                sched.yield_baton(t)?;
            }
            sched.wait_for(t, WaitKey::Tree(1))
        } else if t == 2 {
            sched.wait_for(t, WaitKey::Tree(2))
        } else {
            Ok(())
        }
    };
    let w = run_world(3, 9, body);
    let mut h = w.lock();
    assert!(h.sched_mut().deadlock, "expected a detected deadlock");
}

/// Extra: the crash point fires at the armed yield and every task observes
/// `Crashed` (S-CRASH).
#[test]
fn crash_at_armed_yield() {
    let _body = |sched: &Sched<TestHost>, t: TaskId| -> Result<(), SimFail> {
        let mut n = 0u64;
        loop {
            n += 1;
            sched.yield_baton(t)?;
            if n > 10000 {
                return Ok(());
            }
        }
    };
    // Arm a crash at yield 50 from the harness side before start: build
    // the world manually for this one.
    let host = TestHost {
        sched: SchedCore::new(3, 1),
        disk: SimDisk::new(),
    };
    let sched = Sched::new(host);
    {
        let mut h = sched.lock();
        h.sched_mut().crash_at_yield = Some(50);
    }
    let s2: &Sched<TestHost> = &sched;
    let mut results: Vec<Result<(), SimFail>> = Vec::new();
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for t in 1..3 {
            let sh = &s2;
            handles.push(scope.spawn(move || {
                let r = sh.yield_baton_until_limited(t);
                sh.finish(t);
                r
            }));
        }
        let _ = sched.start();
        for h in handles {
            results.push(h.join().unwrap());
        }
    });
    assert!(results.iter().all(|r| r.is_err()));
    let mut h = sched.lock();
    assert!(h.sched_mut().crashed);
    assert_eq!(h.sched_mut().yields, 50);
}

/// Test-only helper: a task that yields until it crashes or 10k yields.
trait YieldLimited {
    fn yield_baton_until_limited(&self, t: TaskId) -> Result<(), SimFail>;
}

impl YieldLimited for Sched<TestHost> {
    fn yield_baton_until_limited(&self, t: TaskId) -> Result<(), SimFail> {
        for _ in 0..10000 {
            self.yield_baton(t)?;
        }
        Ok(())
    }
}

/// Extra: every `E4Env` call is a yield point and a possible crash point
/// (the operation never executes when the crash fires at it).
#[test]
fn env_calls_yield_and_crash() {
    let host = TestHost {
        sched: SchedCore::new(2, 1),
        disk: SimDisk::new(),
    };
    let sched = Sched::new(host);
    let fid = {
        let mut h = sched.lock();
        let id = h.disk().create("/f").unwrap();
        h.sched_mut().crash_at_yield = Some(1);
        id
    };
    std::thread::scope(|scope| {
        let sp = &sched;
        let handle = scope.spawn(move || {
            let mut env = EnvRef {
                sched: sp,
                task: HARNESS,
            };
            // The env call crashes at its leading yield: the write never
            // executes.
            assert_eq!(env.write_at(fid, 0, &[7u8; 16]), Err(SimFail::Crashed));
        });
        // No workload tasks exist in this world; no start() is needed.
        handle.join().unwrap();
    });
    let mut h = sched.lock();
    let writes = h
        .disk()
        .events
        .iter()
        .filter(|e| matches!(e, DiskEvent::WriteIssued { .. }))
        .count();
    assert_eq!(writes, 0, "no write ran after the crash");
}

/// Extra: media faults only target durable writes whose bytes are current,
/// and a LOST fault reverts the range to its pre-image.
#[test]
fn media_lost_reverts_to_pre() {
    let mut rng = Rng::new(2);
    let mut disk = SimDisk::new();
    let f = disk.create(PAGE_FILE).unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    // Page 3 first (0x33), synced, then overwritten by 0x44 — the 0x33
    // write is no longer current, only the 0x44 one is a candidate.
    disk.write_at(f, 3 * 4096, &[0x33u8; 4096]).unwrap();
    disk.sync_data(f).unwrap();
    disk.write_at(f, 3 * 4096, &[0x44u8; 4096]).unwrap();
    disk.sync_data(f).unwrap();
    // A second, referenced-in-nothing page 4.
    disk.write_at(f, 4 * 4096, &[0x55u8; 4096]).unwrap();
    disk.sync_data(f).unwrap();
    let faults = disk.apply_media_page(&mut rng, &Default::default());
    assert!(!faults.is_empty());
    for fault in &faults {
        // The reverted range must hold the pre-image of the faulted write.
        if let e4_recovery::sim::MediaKind::Lost = fault.kind {
            let got = disk.durable_read(f, fault.off, fault.len);
            assert_eq!(got, fault.pre);
        }
    }
    // The disk saw the write events this round.
    assert!(
        disk.events
            .iter()
            .any(|e| matches!(e, DiskEvent::WriteIssued { .. }))
    );
}

fn drop_all() -> FaultKnobs {
    FaultKnobs {
        keep_latest_pm: 0,
        tear_pm: 0,
    }
}

/// Gate REWORK item 1 (pinned): taint covers a read that **starts inside**
/// an issued range. Write 8 KiB at 0 and sync it, issue 8 KiB again
/// (dropped by the crash), crash: reading `[4096, 8192)` must be tainted —
/// the issued interval starts at 0, before the read, and the run-1
/// `issued.range(off..end)` walk skipped it. Every WAL read that begins
/// mid-batch has this shape.
#[test]
fn taint_covers_read_starting_inside_issued_range() {
    let mut rng = Rng::new(9);
    let mut disk = SimDisk::new();
    let f = disk.create("/f").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    disk.write_at(f, 0, &[0x11u8; 8192]).unwrap();
    disk.sync_data(f).unwrap();
    disk.write_at(f, 0, &[0x22u8; 8192]).unwrap(); // volatile, will drop
    disk.apply_crash(&mut rng, &drop_all());
    disk.new_round();
    // The full range: tainted (the last issued bytes were the 0x22s).
    let _ = disk.read_at(f, 0, 8192).unwrap();
    assert!(disk.reads.last().unwrap().tainted);
    // The second half, starting inside the issued range: tainted too.
    let _ = disk.read_at(f, 4096, 4096).unwrap();
    assert!(
        disk.reads.last().unwrap().tainted,
        "a read starting inside an issued range is tainted"
    );
    // The observer view agrees (O-3 (e)'s tainted referenced slots).
    assert!(disk.range_tainted(f, 4096, 4096));
}

/// Gate REWORK item 2 (pinned): the accept hook judges the accepted
/// sub-range against the **most recent read that covers it**. A tainted
/// 8 KiB batch read followed by `accepted(f, 0, 100)` is a served
/// corruption (an exact `(off, len)` match could never fire); after the
/// store re-issues the bytes and re-reads, the same accept is clean.
#[test]
fn accept_hook_judges_subrange_of_covering_read() {
    let mut rng = Rng::new(12);
    let mut disk = SimDisk::new();
    let f = disk.create("/f").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    disk.write_at(f, 0, &[1u8; 8192]).unwrap();
    disk.sync_data(f).unwrap();
    disk.write_at(f, 0, &[2u8; 8192]).unwrap(); // volatile, will drop
    disk.apply_crash(&mut rng, &drop_all());
    disk.new_round();
    // A batched read of the whole range, tainted; the store accepts one
    // record inside it.
    let _ = disk.read_at(f, 0, 8192).unwrap();
    disk.accepted(f, 0, 100);
    assert_eq!(
        disk.served.len(),
        1,
        "accepting a sub-range of a tainted covering read is served corruption"
    );
    // The most recent covering read governs: the store re-issues exactly
    // the surviving bytes, reads again (clean), and the same accept is
    // not served a second time.
    disk.write_at(f, 0, &[1u8; 8192]).unwrap();
    let _ = disk.read_at(f, 0, 8192).unwrap();
    assert!(!disk.reads.last().unwrap().tainted);
    disk.accepted(f, 0, 100);
    assert_eq!(
        disk.served.len(),
        1,
        "an accept over a clean re-read is not served corruption"
    );
}

/// Gate REWORK item 2 (pinned, sub-range granularity): only the tainted
/// part of a covering read makes an accept served. The volatile write
/// covers just the first sector, so after the drop-only crash exactly
/// `[0, 512)` is tainted.
#[test]
fn accept_hook_subrange_taint_only() {
    let mut rng = Rng::new(13);
    let mut disk = SimDisk::new();
    let f = disk.create("/f").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    disk.write_at(f, 0, &[1u8; 8192]).unwrap();
    disk.sync_data(f).unwrap();
    disk.write_at(f, 0, &[2u8; 512]).unwrap(); // volatile, will drop
    disk.apply_crash(&mut rng, &drop_all());
    disk.new_round();
    let _ = disk.read_at(f, 0, 8192).unwrap();
    assert_eq!(
        disk.reads.last().unwrap().tainted_ranges,
        vec![(0, 512)],
        "exactly the first sector is tainted"
    );
    // Accepting bytes outside the tainted interval: not served.
    disk.accepted(f, 4096, 512);
    assert!(
        disk.served.is_empty(),
        "an accept outside the tainted interval is not served corruption"
    );
    // Accepting bytes inside it: served.
    disk.accepted(f, 0, 100);
    assert_eq!(disk.served.len(), 1);
}

/// Gate REWORK item 6a (pinned): a **synced shrink** survives the crash —
/// after `set_len(4096)` + `sync_all` and a crash, the stale bytes behind
/// the smaller durable length are gone (reading at 4096 yields zeros,
/// card 1.1). The run-1 collapse set `cur = durable` without truncating
/// to `durable_len`, so the old tail stayed readable.
#[test]
fn synced_shrink_survives_crash() {
    let mut rng = Rng::new(17);
    let mut disk = SimDisk::new();
    let f = disk.create("/f").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    disk.write_at(f, 0, &[0xABu8; 8192]).unwrap();
    disk.sync_all(f).unwrap();
    disk.set_len(f, 4096).unwrap();
    disk.sync_all(f).unwrap();
    disk.apply_crash(&mut rng, &drop_all());
    assert_eq!(disk.durable_len(f), 4096);
    assert_eq!(disk.read_at(f, 0, 16).unwrap(), vec![0xABu8; 16]);
    assert_eq!(
        disk.read_at(f, 4096, 64).unwrap(),
        vec![0u8; 64],
        "no stale bytes behind the durable length"
    );
    assert_eq!(
        disk.read_at(f, 8128, 64).unwrap(),
        vec![0u8; 64],
        "no stale bytes at the old tail either"
    );
}

/// Gate REWORK item 6c (pinned): F-MEDIA-WAL draws uniformly over every
/// segment's candidates. With candidates in both `/wal/` segments, the
/// higher-id (active) segment is hit for some seed — the run-1 loop
/// faulted only the lowest-id segment that had candidates.
#[test]
fn media_wal_draws_across_all_segments() {
    let mut hit_low = false;
    let mut hit_high = false;
    for seed in 0..64u64 {
        let mut rng = Rng::new(300 + seed);
        let mut disk = SimDisk::new();
        let lo = disk.create("/wal/0001").unwrap();
        let hi = disk.create("/wal/0002").unwrap();
        disk.sync_dir(WAL_DIR).unwrap();
        disk.write_at(lo, 0, &[0x11u8; 1024]).unwrap();
        disk.sync_data(lo).unwrap();
        disk.write_at(hi, 0, &[0x22u8; 1024]).unwrap();
        disk.sync_data(hi).unwrap();
        let faults = disk.apply_media_wal(&mut rng);
        assert!(!faults.is_empty(), "seed {seed}: candidates exist");
        for fault in &faults {
            if fault.inode == lo {
                hit_low = true;
            }
            if fault.inode == hi {
                hit_high = true;
            }
        }
    }
    assert!(
        hit_high,
        "the active (higher-id) segment is faulted for some seed"
    );
    assert!(hit_low, "the low segment is faulted for some seed too");
}
