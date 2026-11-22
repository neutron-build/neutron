//! SimEnv: the S-ENV layer's own tests (card A-1b Work 6) — every
//! shared conformance check of `common` run inside a one-task life,
//! the S-2-F2-F1 power-cut losses of unsynced creates, renames and
//! `set_len`, the §12.5 writer-rule substrate `t_seg_ack_durable`
//! (A-6b) needs, and the S-1-F3 post-sync media-fault substrates
//! (`t_stale_boot_record`'s truncation write, `SegmentEnd`, the meta
//! slot). The E4-copied pins stay in `tests/sim.rs`; the harness-port
//! tests are A-8a's. The trait's contract is the crate doc of
//! `nucleus-env`; PLAN-v3 §3.2 and S-STORE §12.5, §14 and §15 are the
//! normative sources.

#![allow(missing_docs)]

mod common;

use nucleus_env::sim::{
    DiskEvent, FaultKnobs, LifeOutcome, MediaFault, MediaKind, SIM_MAX_LEN, Sched, SchedCore,
    SchedHost, Sim, SimEnv, WaitKey,
};
use nucleus_env::{Env, EnvError, File, OpenMode};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

/// Verbatim from E4's `tests/sim.rs` (card A-1b Work 6): the knobs
/// that drop every unsynced byte and every unsynced directory op.
fn drop_all() -> FaultKnobs {
    FaultKnobs {
        keep_latest_pm: 0,
        tear_pm: 0,
    }
}

/// Asserts a life neither panicked nor halted (Work 6's per-test bar).
fn assert_life_ok(out: &LifeOutcome) {
    assert!(!out.panicked, "the life panicked");
    assert!(
        !out.crashed && !out.deadlock,
        "the life halted: crashed={} deadlock={}",
        out.crashed,
        out.deadlock
    );
    assert!(
        out.livelock.is_none(),
        "the life livelocked at {:?}",
        out.livelock
    );
    for (i, r) in out.results.iter().enumerate() {
        assert_eq!(r, &Ok(()), "task {}'s result", i + 1);
    }
}

/// Runs one shared conformance check inside a one-task life: a failed
/// check surfaces as the task's `Halted` result naming it.
fn conformance(check: fn(&SimEnv) -> Result<(), String>) {
    let mut sim = Sim::new(1, drop_all());
    let out = sim.run_life(1, None, |env, _t| {
        check(env).map_err(|m| EnvError::Halted { what: m })
    });
    assert_life_ok(&out);
}

// ---- Conformance on SimEnv: A-1a's twelve checks ----

#[test]
fn sim_open_modes() {
    conformance(common::open_modes);
}

#[test]
fn sim_read_write_roundtrip() {
    conformance(common::read_write_roundtrip);
}

#[test]
fn sim_read_handle_refuses_writes() {
    conformance(common::read_handle_refuses_writes);
}

#[test]
fn sim_namespace_ops() {
    conformance(common::namespace_ops);
}

#[test]
fn sim_invalid_paths_rejected() {
    conformance(common::invalid_paths_rejected);
}

#[test]
fn sim_sync_dir_root_and_subdir() {
    conformance(common::sync_dir_root_and_subdir);
}

#[test]
fn sim_locks_exclusive_excludes_all() {
    conformance(common::locks_exclusive_excludes_all);
}

#[test]
fn sim_locks_shared_readers_coexist() {
    conformance(common::locks_shared_readers_coexist);
}

#[test]
fn sim_locks_shared_excludes_writer() {
    conformance(common::locks_shared_excludes_writer);
}

#[test]
fn sim_lock_released_on_unlock_and_drop() {
    conformance(common::lock_released_on_unlock_and_drop);
}

#[test]
fn sim_lock_creates_no_file() {
    conformance(common::lock_creates_no_file);
}

#[test]
fn sim_clock_and_rand() {
    conformance(common::clock_and_rand);
}

// ---- Conformance: A-1b-0's six (the A-1a-F1/F2/F3 fold) ----

#[test]
fn sim_sync_on_writable_handles() {
    conformance(common::sync_on_writable_handles);
}

#[test]
fn sim_handle_survives_rename() {
    conformance(common::handle_survives_rename);
}

#[test]
fn sim_lock_moves_with_rename_until_drop() {
    conformance(common::lock_moves_with_rename_until_drop);
}

#[test]
fn sim_handle_survives_remove_and_recreate() {
    conformance(common::handle_survives_remove_and_recreate);
}

#[test]
fn sim_kind_mismatches() {
    conformance(common::kind_mismatches);
}

#[test]
fn sim_io_edges() {
    conformance(common::io_edges);
}

// ---- Conformance: Work 0c's check ----

#[test]
fn sim_lock_mode_on_same_handle() {
    conformance(common::lock_mode_on_same_handle);
}

/// S-ENV "File ops", the cap (A-1b-0-F2): past `SIM_MAX_LEN` an end
/// or length is `Io { FileTooLarge }` naming the path; below it the
/// file still works, and the life neither panicked nor halted.
#[test]
fn sim_len_cap() {
    let mut sim = Sim::new(1, drop_all());
    let out = sim.run_life(1, None, |env, _t| {
        let f = env.open("page", OpenMode::CreateNew)?;
        f.write_at(0, b"abc")?;
        let io_too_large = |e: &EnvError| {
            matches!(
                e,
                EnvError::Io { kind: std::io::ErrorKind::FileTooLarge, path, .. } if path == "page"
            )
        };
        let e = f.write_at(SIM_MAX_LEN, b"x").unwrap_err();
        assert!(io_too_large(&e), "write_at at the cap: {e:?}");
        let e = f.set_len(SIM_MAX_LEN + 1).unwrap_err();
        assert!(io_too_large(&e), "set_len past the cap: {e:?}");
        assert_eq!(f.len()?, 3, "a refused length changed the file");
        f.write_at(3, b"d")?;
        assert_eq!(f.len()?, 4);
        Ok(())
    });
    assert_life_ok(&out);
}

/// S-ENV "Namespace", the fold's query/creation rule: only root
/// files, `wal`, and files in `wal/` exist. A creation outside them
/// is `InvalidPath`; a query answers as missing; `wal/…` is
/// `NotFound` until `create_dir("wal")`, and `list_dir("")` shows
/// `wal` afterwards.
#[test]
fn sim_namespace_is_root_plus_wal() {
    let mut sim = Sim::new(1, drop_all());
    let out = sim.run_life(1, None, |env, _t| {
        let invalid = |e: &EnvError| matches!(e, EnvError::InvalidPath { why, .. } if *why == "SimEnv models the root and wal/ only");
        assert!(
            invalid(&env.create_dir("data").unwrap_err()),
            "create_dir(data) must be InvalidPath"
        );
        match env.open("a/b/c", OpenMode::CreateNew) {
            Err(e) => assert!(
                invalid(&e),
                "CreateNew outside the model must be InvalidPath, got {e:?}"
            ),
            Ok(_) => panic!("CreateNew outside the model succeeded"),
        }
        assert!(!env.exists("a/b/c")?);
        assert!(matches!(
            env.sync_dir("data"),
            Err(EnvError::NotFound { .. })
        ));
        assert!(matches!(
            env.list_dir("data"),
            Err(EnvError::NotFound { .. })
        ));
        // Before the wal directory exists, every wal/… path is NotFound.
        assert!(!env.exists("wal")?);
        assert!(matches!(
            env.open("wal/x", OpenMode::Read),
            Err(EnvError::NotFound { .. })
        ));
        assert!(matches!(
            env.open("wal/x", OpenMode::CreateNew),
            Err(EnvError::NotFound { .. })
        ));
        env.create_dir("wal")?;
        assert!(env.exists("wal")?);
        let root = env.list_dir("")?;
        assert!(root.iter().any(|n| n == "wal"), "root is {root:?}");
        Ok(())
    });
    assert_life_ok(&out);
}

/// E4's `env_calls_yield_and_crash`, rewritten for this layer (S-ENV
/// yield then step): with the crash armed at the `write_at`'s yield,
/// the write returns `EnvError::Crashed` and never executes — the
/// life's `DiskEvent`s contain no `WriteIssued`. (The card names
/// `Some(1)`; with `open` a yield point too, the write_at is the
/// life's second yield, so the arm is `Some(2)` — the assertion, and
/// mutant OP-BEFORE-YIELD's catch, are the card's.)
#[test]
fn env_calls_yield_and_crash() {
    let mut sim = Sim::new(1, drop_all());
    // The file, created and synced before the life under test.
    let setup = sim.run_life(1, None, |env, _t| {
        let f = env.open("page", OpenMode::CreateNew)?;
        f.write_at(0, b"abc")?;
        f.sync_all()?;
        env.sync_dir("")?;
        Ok(())
    });
    assert_life_ok(&setup);

    let out = sim.run_life(1, Some(2), |env, _t| {
        let f = env.open("page", OpenMode::ReadWrite)?;
        // The write crashes at its leading yield: it never executes.
        let r = f.write_at(0, &[7u8; 16]);
        assert_eq!(r, Err(EnvError::Crashed));
        Ok(())
    });
    assert!(out.crashed, "the life must have crashed at the armed yield");
    assert!(!out.panicked);
    let writes = sim
        .disk()
        .events
        .iter()
        .filter(|e| matches!(e, DiskEvent::WriteIssued { .. }))
        .count();
    assert_eq!(writes, 0, "no write ran after the crash");
}

/// S-DET through the new layer: three tasks run a fixed mix of
/// creates, writes, syncs, a rename, `sleep` and `rand`, with
/// `yield_now` between steps (the `rand` draws are written to a file,
/// so they are compared with the rest of the disk). The same seed
/// twice gives an identical trace, identical results, identical
/// bytes — and after `power_cut()`, identical bytes again.
#[test]
fn sim_life_is_deterministic() {
    let body = |env: &SimEnv, t: nucleus_env::sim::TaskId| -> Result<(), EnvError> {
        env.yield_now("mix:start")?;
        let name = format!("r{t}");
        let f = env.open(&name, OpenMode::CreateNew)?;
        let mut step = 0u64;
        while step < 4 {
            let v = env.rand();
            f.write_at(step * 8, &v.to_be_bytes())?;
            env.yield_now("mix:step")?;
            step += 1;
        }
        f.sync_all()?;
        if t == 1 {
            env.create_dir("wal")?;
            let w = env.open("wal/seg", OpenMode::CreateNew)?;
            w.write_at(0, &[0xA5u8; 32])?;
            w.sync_all()?;
            env.sync_dir("wal")?;
            env.rename("wal/seg", "wal/seg2")?;
            env.sleep(Duration::from_millis(1));
        }
        Ok(())
    };
    let snapshot = |sim: &Sim| -> Vec<(String, u64, Vec<u8>)> {
        ["r1", "r2", "r3", "wal/seg", "wal/seg2"]
            .iter()
            .filter_map(|p| sim.file_id(p).unwrap_or(None))
            .map(|id| {
                let len = sim.disk().cur_len(id);
                (format!("f{id}"), len, sim.disk().durable_read(id, 0, len))
            })
            .collect()
    };
    let mut a = Sim::new(7, drop_all());
    let out_a = a.run_life(3, None, body);
    assert_life_ok(&out_a);
    let mut b = Sim::new(7, drop_all());
    let out_b = b.run_life(3, None, body);
    assert_life_ok(&out_b);
    assert_eq!(out_a.trace, out_b.trace, "same seed, same trace");
    assert_eq!(out_a.results, out_b.results, "same seed, same results");
    assert_eq!(out_a.yields, out_b.yields);
    let snap_a = snapshot(&a);
    let snap_b = snapshot(&b);
    assert_eq!(
        snap_a, snap_b,
        "same seed, same bytes (durable and current)"
    );

    a.power_cut();
    b.power_cut();
    assert_eq!(
        snapshot(&a),
        snapshot(&b),
        "same seed, same bytes after the cut"
    );
}

/// The schedule is seeded (S-ENV "Streams"): over seeds 1–8 of the
/// determinism body, at least two traces differ.
#[test]
fn sim_seeds_differ() {
    let body = |env: &SimEnv, t: nucleus_env::sim::TaskId| -> Result<(), EnvError> {
        env.yield_now("mix:start")?;
        let name = format!("r{t}");
        let f = env.open(&name, OpenMode::CreateNew)?;
        let mut step = 0u64;
        while step < 4 {
            let v = env.rand();
            f.write_at(step * 8, &v.to_be_bytes())?;
            env.yield_now("mix:step")?;
            step += 1;
        }
        f.sync_all()?;
        if t == 1 {
            env.create_dir("wal")?;
            let w = env.open("wal/seg", OpenMode::CreateNew)?;
            w.write_at(0, &[0xA5u8; 32])?;
            w.sync_all()?;
            env.sync_dir("wal")?;
            env.rename("wal/seg", "wal/seg2")?;
            env.sleep(Duration::from_millis(1));
        }
        Ok(())
    };
    let mut traces = Vec::new();
    for seed in 1..=8u64 {
        let mut sim = Sim::new(seed, drop_all());
        let out = sim.run_life(3, None, body);
        assert_life_ok(&out);
        traces.push(out.trace);
    }
    let mut differ = false;
    for (i, t) in traces.iter().enumerate() {
        for u in traces.iter().skip(i + 1) {
            if t != u {
                differ = true;
            }
        }
    }
    assert!(differ, "every seed 1..=8 drew the same schedule");
}

/// S-ENV "Crash kinds", the process crash: a life writes and renames
/// with no sync at all and then crashes at an armed yield. With no
/// `power_cut`, the page cache survives: the next life reads every
/// byte and sees the new name.
#[test]
fn sim_process_crash_keeps_page_cache() {
    let mut sim = Sim::new(5, drop_all());
    let out = sim.run_life(1, Some(12), |env, _t| {
        env.create_dir("wal")?;
        let f = env.open("wal/a", OpenMode::CreateNew)?;
        f.write_at(0, b"unsynced but cached")?;
        env.rename("wal/a", "wal/b")?;
        // Spin at yield points until the armed crash fires: the
        // process dies with unsynced writes in the page cache.
        for _ in 0..100 {
            env.yield_now("drain")?;
        }
        Ok(())
    });
    assert!(out.crashed, "the life must crash");
    assert!(!out.panicked);

    let next = sim.run_life(1, None, |env, _t| {
        assert!(!env.exists("wal/a")?, "the old name is gone");
        assert!(env.exists("wal/b")?, "the new name is live");
        let f = env.open("wal/b", OpenMode::Read)?;
        let mut buf = [0u8; 19];
        let n = f.read_at(0, &mut buf)?;
        assert_eq!(n, 19);
        assert_eq!(&buf, b"unsynced but cached");
        Ok(())
    });
    assert_life_ok(&next);
}

/// S-ENV "Locks": a life holds the exclusive lock and crashes,
/// followed by `power_cut`. The next life's `lock_exclusive`
/// succeeds — a new life starts with no locks, so a crash releases
/// every lock.
#[test]
fn sim_power_cut_releases_locks() {
    let mut sim = Sim::new(9, drop_all());
    let out = sim.run_life(1, Some(10), |env, _t| {
        let f = env.open("page", OpenMode::CreateNew)?;
        drop(f);
        env.sync_dir("")?;
        let h = env.open("page", OpenMode::ReadWrite)?;
        h.lock_exclusive()?;
        for _ in 0..100 {
            env.yield_now("drain")?;
        }
        Ok(())
    });
    assert!(out.crashed);
    assert!(!out.panicked);
    sim.power_cut();

    let next = sim.run_life(1, None, |env, _t| {
        let h = env.open("page", OpenMode::ReadWrite)?;
        h.lock_exclusive()?;
        Ok(())
    });
    assert_life_ok(&next);
}

// ---- The S-2-F2-F1 losses (seeds 0–31, `drop_all`) ----

/// The power cut loses an unsynced create (S-2-F2-F1): with no
/// `sync_dir("wal")` at least one seed loses `wal/a.log`, and every
/// seed that keeps it reads the synced bytes; with the directory
/// sync, every seed keeps the file and its bytes.
#[test]
fn power_cut_loses_unsynced_create() {
    for dir_sync in [false, true] {
        let mut lost = 0u32;
        let mut kept_bad = 0u32;
        for seed in 0..32u64 {
            let mut sim = Sim::new(seed, drop_all());
            let out = sim.run_life(1, None, |env, _t| {
                env.create_dir("wal")?;
                env.sync_dir("")?;
                let f = env.open("wal/a.log", OpenMode::CreateNew)?;
                f.write_at(0, b"synced record bytes")?;
                f.sync_all()?;
                if dir_sync {
                    env.sync_dir("wal")?;
                }
                Ok(())
            });
            assert_life_ok(&out);
            sim.power_cut();
            match sim.file_id("wal/a.log").unwrap_or(None) {
                None => lost += 1,
                Some(id) => {
                    if sim.disk().durable_read(id, 0, 19) != b"synced record bytes" {
                        kept_bad += 1;
                    }
                }
            }
        }
        if dir_sync {
            assert_eq!(lost, 0, "a dir-synced create was lost");
            assert_eq!(kept_bad, 0, "a dir-synced create lost its bytes");
        } else {
            assert!(lost > 0, "no seed lost the unsynced create");
            assert_eq!(kept_bad, 0, "a kept file lost its synced bytes");
        }
    }
}

/// The power cut loses an unsynced rename (S-2-F2-F1): a dir-synced
/// `wal/a.log` renamed without a further `sync_dir` — at least one
/// seed has `a.log` and not `b.log`, and no seed has both or neither.
/// With the sync after the rename, every seed has only `b.log`.
#[test]
fn power_cut_loses_unsynced_rename() {
    for dir_sync in [false, true] {
        let mut old_name = 0u32;
        let mut new_name = 0u32;
        let mut both_or_neither = 0u32;
        for seed in 0..32u64 {
            let mut sim = Sim::new(seed, drop_all());
            let setup = sim.run_life(1, None, |env, _t| {
                env.create_dir("wal")?;
                env.sync_dir("")?;
                let f = env.open("wal/a.log", OpenMode::CreateNew)?;
                f.write_at(0, b"synced record bytes")?;
                f.sync_all()?;
                env.sync_dir("wal")?;
                Ok(())
            });
            assert_life_ok(&setup);
            let out = sim.run_life(1, None, |env, _t| {
                env.rename("wal/a.log", "wal/b.log")?;
                if dir_sync {
                    env.sync_dir("wal")?;
                }
                Ok(())
            });
            assert_life_ok(&out);
            sim.power_cut();
            let a = sim.file_id("wal/a.log").unwrap_or(None).is_some();
            let b = sim.file_id("wal/b.log").unwrap_or(None).is_some();
            match (a, b) {
                (true, false) => old_name += 1,
                (false, true) => new_name += 1,
                _ => both_or_neither += 1,
            }
        }
        assert_eq!(both_or_neither, 0, "a seed has both or neither name");
        if dir_sync {
            assert_eq!((old_name, new_name), (0, 32), "a dir-synced rename");
        } else {
            assert!(old_name > 0, "no seed lost the unsynced rename");
            assert!(new_name > 0, "no seed kept the unsynced rename");
        }
    }
}

/// The power cut loses an unsynced `set_len` (S-2-F2-F1): a synced,
/// dir-synced, empty `wal/a.log` grows to 1 MiB. With no sync every
/// seed still has `len == 0`; with `sync_data` only, every seed still
/// has `len == 0` (E-DURABLE's pessimistic `fdatasync`, which E4's
/// `sync_data` models); with `sync_all` every seed has the length.
#[test]
fn power_cut_loses_unsynced_set_len() {
    for mode in [0u8, 1, 2] {
        let mut full = 0u32;
        for seed in 0..32u64 {
            let mut sim = Sim::new(seed, drop_all());
            let setup = sim.run_life(1, None, |env, _t| {
                env.create_dir("wal")?;
                env.sync_dir("")?;
                let f = env.open("wal/a.log", OpenMode::CreateNew)?;
                f.sync_all()?;
                env.sync_dir("wal")?;
                f.set_len(1024 * 1024)?;
                match mode {
                    0 => {}
                    1 => f.sync_data()?,
                    _ => f.sync_all()?,
                }
                Ok(())
            });
            assert_life_ok(&setup);
            sim.power_cut();
            let next = sim.run_life(1, None, |env, _t| {
                let f = env.open("wal/a.log", OpenMode::Read)?;
                let len = f.len()?;
                match mode {
                    2 => assert_eq!(len, 1024 * 1024, "sync_all must keep the length"),
                    _ => assert_eq!(len, 0, "mode {mode}: the unsynced length survived"),
                }
                if len == 1024 * 1024 {
                    return Ok(());
                }
                Ok(())
            });
            assert_life_ok(&next);
            if matches!(next.results.first(), Some(Ok(()))) && mode == 2 {
                full += 1;
            }
        }
        if mode == 2 {
            assert_eq!(full, 32, "not every seed kept the synced length");
        }
    }
}

/// §12.5's writer rule, the substrate `t_seg_ack_durable` (A-6b)
/// needs: a segment is created (case *create*) or recycled by rename
/// (case *rename*), then `set_len(seg)`, `sync_all`,
/// `sync_dir("wal")`, a record is written and `sync_data`'d (the
/// ack), then the power cut. With every step, the record and the
/// full length survive; with `sync_dir` skipped, at least one seed
/// loses the segment (create) or still has the old name (rename) —
/// the clean stop short of an acked record that
/// SEG-ACK-BEFORE-DURABLE produces; with `sync_all` skipped, every
/// seed loses the preallocated length beyond the record.
#[test]
fn sim_segment_ack_substrate() {
    const SEG: u64 = 8192;
    const RECORD: &[u8] = b"acked record bytes!!";
    for case in ["create", "rename"] {
        for arm in [0u8, 1, 2] {
            let (mut lost, mut old_name, mut len_bad) = (0u32, 0u32, 0u32);
            for seed in 0..32u64 {
                let mut sim = Sim::new(seed, drop_all());
                let setup = sim.run_life(1, None, |env, _t| {
                    env.create_dir("wal")?;
                    env.sync_dir("")?;
                    if case == "rename" {
                        let f = env.open("wal/old.log", OpenMode::CreateNew)?;
                        f.sync_all()?;
                        env.sync_dir("wal")?;
                    }
                    Ok(())
                });
                assert_life_ok(&setup);
                let out = sim.run_life(1, None, |env, _t| {
                    let f = if case == "rename" {
                        env.rename("wal/old.log", "wal/new.log")?;
                        env.open("wal/new.log", OpenMode::ReadWrite)?
                    } else {
                        env.open("wal/new.log", OpenMode::CreateNew)?
                    };
                    f.set_len(SEG)?;
                    if arm != 2 {
                        f.sync_all()?;
                    }
                    if arm != 1 {
                        env.sync_dir("wal")?;
                    }
                    f.write_at(0, RECORD)?;
                    f.sync_data()?; // the ack
                    Ok(())
                });
                assert_life_ok(&out);
                sim.power_cut();
                let new_id = sim.file_id("wal/new.log").unwrap_or(None);
                let old_id = if case == "rename" {
                    sim.file_id("wal/old.log").unwrap_or(None)
                } else {
                    None
                };
                let record_ok = |sim: &Sim, id: usize| {
                    sim.disk().durable_read(id, 0, RECORD.len() as u64) == RECORD
                };
                match (arm, new_id) {
                    // The create or rename was lost: the segment is
                    // gone (create) or back under its old name
                    // (rename) — the clean stop short of an acked
                    // record that SEG-ACK-BEFORE-DURABLE produces.
                    (1, None) => {
                        lost += 1;
                        if case == "rename" {
                            assert!(old_id.is_some(), "{case}: both names are gone");
                            old_name += 1;
                        }
                    }
                    // The create or rename survived: the acked record
                    // and the length it needs must have survived with
                    // it. With `sync_all` skipped (arm 2) the
                    // preallocated length beyond the record is lost
                    // in every seed; with it (arm 0) the full length
                    // survives in every seed.
                    (_, Some(id)) => {
                        assert!(record_ok(&sim, id), "{case}: the acked record was lost");
                        // A recycled segment leaves its old name behind
                        // only when the rename was lost; with every
                        // step taken the old name is gone (mutant
                        // DIRSYNC-NOOP's stale directory ops would
                        // re-bind it at the cut).
                        if case == "rename" {
                            assert!(
                                old_id.is_none(),
                                "{case}: the recycled old name is back beside the new"
                            );
                        }
                        let want_len = if arm == 2 { RECORD.len() as u64 } else { SEG };
                        if sim.disk().cur_len(id) != want_len {
                            len_bad += 1;
                        }
                    }
                    _ => lost += 1,
                }
            }
            match arm {
                0 => {
                    assert_eq!(lost, 0, "{case}: the acked record was lost");
                    assert_eq!(len_bad, 0, "{case}: the preallocated length was lost");
                }
                1 => {
                    if case == "create" {
                        assert!(lost > 0, "create: no seed lost the unsynced segment");
                    } else {
                        assert!(old_name > 0, "rename: no seed still had the old name");
                    }
                }
                _ => {
                    assert_eq!(lost, 0, "{case}: the acked record was lost");
                    assert_eq!(len_bad, 0, "{case}: a seed kept the unsynced length");
                }
            }
        }
    }
}

// ---- The S-1-F3 additions (post-sync media faults) ----

/// `t_stale_boot_record`'s substrate (§14 step 5, §15 I-WAL-COVER and
/// I-REDO-COMPLETE): a record `OLD` is written and synced, then
/// recovery's truncation writes zeros over it and syncs. A LOST fault
/// on the zero write brings `OLD` back, read by a new life; a TORN
/// fault reverts a strict, non-empty subset of its sectors; a
/// MISDIRECTED fault leaves `OLD` at the source and zeros at a
/// 512-aligned offset inside the same segment.
#[test]
fn sim_truncation_write_loss_restores_stale_record() {
    const OLD: &[u8] = &[0xA5u8; 600];
    const ZERO_LEN: u64 = 1024;
    let fixture = |seed: u64| -> (Sim, usize) {
        let mut sim = Sim::new(seed, drop_all());
        let out = sim.run_life(1, None, |env, _t| {
            env.create_dir("wal")?;
            env.sync_dir("")?;
            let f = env.open("wal/0000000000000000.log", OpenMode::CreateNew)?;
            f.write_at(0, OLD)?;
            f.sync_all()?;
            env.sync_dir("wal")?;
            // Recovery's step-5 truncation: zeros over the record,
            // synced.
            f.write_at(0, &[0u8; ZERO_LEN as usize])?;
            f.sync_all()?;
            Ok(())
        });
        assert_life_ok(&out);
        let id = sim
            .file_id("wal/0000000000000000.log")
            .unwrap_or(None)
            .unwrap();
        (sim, id)
    };

    // LOST: the truncation write reverts to its pre-image, so OLD is
    // back — read by a new life.
    let (mut sim, id) = fixture(1);
    let w = sim
        .disk()
        .dwrites_of(id)
        .iter()
        .rev()
        .find(|w| w.off == 0 && w.bytes.len() as u64 == ZERO_LEN)
        .unwrap()
        .clone();
    sim.media_one(&MediaFault {
        inode: id,
        off: w.off,
        len: w.bytes.len() as u64,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: MediaKind::Lost,
    });
    assert_eq!(
        sim.disk().durable_read(id, 0, OLD.len() as u64),
        OLD.to_vec()
    );
    let next = sim.run_life(1, None, |env, _t| {
        let f = env.open("wal/0000000000000000.log", OpenMode::Read)?;
        let mut buf = vec![0u8; OLD.len()];
        let n = f.read_at(0, &mut buf)?;
        assert_eq!(n, OLD.len());
        assert_eq!(buf, OLD.to_vec(), "the new life reads the stale record");
        Ok(())
    });
    assert_life_ok(&next);

    // TORN: a strict, non-empty subset of the zero write's sectors
    // reverts — sector 1's bytes [512, 600) are OLD again, sector 0
    // stays zeros.
    let (mut sim, id) = fixture(2);
    let w = sim
        .disk()
        .dwrites_of(id)
        .iter()
        .rev()
        .find(|w| w.off == 0 && w.bytes.len() as u64 == ZERO_LEN)
        .unwrap()
        .clone();
    sim.media_one(&MediaFault {
        inode: id,
        off: w.off,
        len: w.bytes.len() as u64,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: MediaKind::Torn { sectors: vec![512] },
    });
    assert_eq!(
        sim.disk().durable_read(id, 0, 512),
        vec![0u8; 512],
        "the un-torn sector stays zeroed"
    );
    assert_eq!(
        sim.disk().durable_read(id, 512, 88),
        OLD[512..600].to_vec(),
        "the torn sector reverts to the stale record's tail"
    );

    // MISDIRECTED: OLD is back at the source, and the zeros land at a
    // 512-aligned offset inside the same segment.
    let (mut sim, id) = fixture(3);
    let w = sim
        .disk()
        .dwrites_of(id)
        .iter()
        .rev()
        .find(|w| w.off == 0 && w.bytes.len() as u64 == ZERO_LEN)
        .unwrap()
        .clone();
    sim.media_one(&MediaFault {
        inode: id,
        off: w.off,
        len: w.bytes.len() as u64,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: MediaKind::Misdirected { to_off: 512 },
    });
    assert_eq!(
        sim.disk().durable_read(id, 0, 512),
        OLD[..512].to_vec(),
        "OLD is back at the source"
    );
    assert_eq!(
        sim.disk().durable_read(id, 512, 1024),
        vec![0u8; 1024],
        "the zeros land at 512, inside the same segment"
    );
}

/// F-MEDIA-WAL reaches recovery's step-5 truncation write (S-1-F3):
/// in the truncation fixture the zero write is the only current
/// durable WAL write, so for every seed 0–15 every fault
/// `media_wal()` draws targets the zero write's range.
#[test]
fn sim_media_wal_hits_truncation_write() {
    for seed in 0..16u64 {
        let mut sim = Sim::new(seed, drop_all());
        let out = sim.run_life(1, None, |env, _t| {
            env.create_dir("wal")?;
            env.sync_dir("")?;
            let f = env.open("wal/0000000000000000.log", OpenMode::CreateNew)?;
            f.write_at(0, &[0xA5u8; 600])?;
            f.sync_all()?;
            env.sync_dir("wal")?;
            f.write_at(0, &[0u8; 1024])?;
            f.sync_all()?;
            Ok(())
        });
        assert_life_ok(&out);
        let faults = sim.media_wal();
        assert!(!faults.is_empty(), "seed {seed}: candidates exist");
        for f in &faults {
            assert_eq!(
                (f.off, f.len),
                (0, 1024),
                "seed {seed}: a fault missed the truncation write: off={} len={}",
                f.off,
                f.len
            );
        }
    }
}

/// `SegmentEnd` takes post-sync faults too (S-1-F3): a short record
/// ending exactly at the segment's last byte, crossing a sector
/// boundary, is synced, and LOST, TORN and MISDIRECTED each apply to
/// it with E4's semantics — LOST restores the pre-image, MISDIRECTED
/// keeps its landing inside the segment, TORN reverts a strict subset
/// of its sectors.
#[test]
fn sim_segment_end_write_takes_post_sync_faults() {
    const SEG: u64 = 1024;
    const END: &[u8] = &[0x5Eu8; 624]; // [400, 1024): crosses sector 512
    let fixture = |seed: u64| -> (Sim, usize) {
        let mut sim = Sim::new(seed, drop_all());
        let out = sim.run_life(1, None, |env, _t| {
            env.create_dir("wal")?;
            env.sync_dir("")?;
            let f = env.open("wal/0000000000000000.log", OpenMode::CreateNew)?;
            f.set_len(SEG)?;
            f.sync_all()?;
            env.sync_dir("wal")?;
            f.write_at(400, END)?;
            f.sync_all()?;
            Ok(())
        });
        assert_life_ok(&out);
        let id = sim
            .file_id("wal/0000000000000000.log")
            .unwrap_or(None)
            .unwrap();
        (sim, id)
    };
    let end_write = |sim: &Sim, id: usize| -> MediaFault {
        let w = sim
            .disk()
            .dwrites_of(id)
            .iter()
            .rev()
            .find(|w| w.off == 400 && w.bytes.len() == END.len())
            .unwrap()
            .clone();
        MediaFault {
            inode: id,
            off: w.off,
            len: w.bytes.len() as u64,
            bytes: w.bytes.clone(),
            pre: w.pre.clone(),
            kind: MediaKind::Lost,
        }
    };

    // LOST restores the pre-image (zeros).
    let (mut sim, id) = fixture(1);
    let mut f = end_write(&sim, id);
    sim.media_one(&f);
    assert_eq!(
        sim.disk().durable_read(id, 400, END.len() as u64),
        vec![0u8; END.len()],
        "LOST must restore the SegmentEnd pre-image"
    );

    // TORN reverts a strict subset: sector 0's slice of the write
    // ([400, 512)) reverts, sector 1's ([512, 1024)) keeps END.
    let (mut sim, id) = fixture(2);
    f = end_write(&sim, id);
    f.kind = MediaKind::Torn { sectors: vec![0] };
    sim.media_one(&f);
    assert_eq!(
        sim.disk().durable_read(id, 400, 112),
        vec![0u8; 112],
        "the torn sector reverts"
    );
    assert_eq!(
        sim.disk().durable_read(id, 512, 512),
        END[112..].to_vec(),
        "the un-torn sector keeps the SegmentEnd bytes"
    );

    // MISDIRECTED keeps its landing inside the segment: the source
    // reverts to its pre-image and the bytes land at 0 (the landing
    // overlaps the source's head, exactly as E4's drawn WAL
    // misdirects do — the draw is 512-aligned within `[0, dlen-len]`).
    let (mut sim, id) = fixture(3);
    f = end_write(&sim, id);
    f.kind = MediaKind::Misdirected { to_off: 0 };
    sim.media_one(&f);
    assert_eq!(
        sim.disk().durable_read(id, 0, END.len() as u64),
        END.to_vec(),
        "the stray bytes land at 0, inside the segment"
    );
    assert_eq!(
        sim.disk()
            .durable_read(id, 400 + END.len() as u64, SEG - 400 - END.len() as u64),
        vec![0u8; (SEG - 400 - END.len() as u64) as usize],
        "the source's tail beyond the landing reverts to the pre-image"
    );
}

/// I-META-SLOT, `t_meta_rollback_crosses_boot`'s substrate (S-1-F3;
/// D1's test): the page file is `pages` with 8192-byte slots. Slot 1
/// is written, then slot 2, then slot 2 again, each with `sync_all`.
/// A directed LOST on slot 2's latest write reverts slot 2 to its
/// previous contents: the older meta returns after a power cut and a
/// new life. Over seeds 0–15, `media_page("pages", 8192, {1, 2})`
/// targets only 8192-aligned, 8192-byte writes, lands every
/// MISDIRECTED at a multiple of 8192 within `[8192, durable_len)`,
/// and draws at least one fault on slot 1 or 2.
#[test]
fn sim_meta_slot_media_faults() {
    const SLOT: u64 = 8192;
    let v1 = vec![0x11u8; SLOT as usize];
    let v2 = vec![0x22u8; SLOT as usize];
    let v3 = vec![0x33u8; SLOT as usize];

    // The directed LOST: slot 2's latest write reverts to its
    // previous contents, read after a power cut by a new life.
    let mut sim = Sim::new(1, drop_all());
    let out = sim.run_life(1, None, |env, _t| {
        let f = env.open("pages", OpenMode::CreateNew)?;
        f.sync_all()?;
        env.sync_dir("")?;
        f.write_at(SLOT, &v1)?;
        f.sync_all()?;
        f.write_at(2 * SLOT, &v2)?;
        f.sync_all()?;
        f.write_at(2 * SLOT, &v3)?;
        f.sync_all()?;
        Ok(())
    });
    assert_life_ok(&out);
    let id = sim.file_id("pages").unwrap_or(None).unwrap();
    let w = sim
        .disk()
        .dwrites_of(id)
        .iter()
        .rev()
        .find(|w| w.off == 2 * SLOT && w.bytes == v3)
        .unwrap()
        .clone();
    sim.media_one(&MediaFault {
        inode: id,
        off: w.off,
        len: w.bytes.len() as u64,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        kind: MediaKind::Lost,
    });
    sim.power_cut();
    let next = sim.run_life(1, None, |env, _t| {
        let f = env.open("pages", OpenMode::Read)?;
        let mut buf = vec![0u8; SLOT as usize];
        let n = f.read_at(2 * SLOT, &mut buf)?;
        assert_eq!(n as u64, SLOT);
        assert_eq!(buf, v2, "the older meta returns after the cut");
        let mut buf = vec![0u8; SLOT as usize];
        let n = f.read_at(SLOT, &mut buf)?;
        assert_eq!(n as u64, SLOT);
        assert_eq!(buf, v1, "slot 1 is untouched");
        Ok(())
    });
    assert_life_ok(&next);

    // The drawn faults, over seeds 0–15.
    for seed in 0..16u64 {
        let mut sim = Sim::new(seed, drop_all());
        let out = sim.run_life(1, None, |env, _t| {
            let f = env.open("pages", OpenMode::CreateNew)?;
            f.sync_all()?;
            env.sync_dir("")?;
            f.write_at(SLOT, &v1)?;
            f.sync_all()?;
            f.write_at(2 * SLOT, &v2)?;
            f.sync_all()?;
            f.write_at(2 * SLOT, &v3)?;
            f.sync_all()?;
            Ok(())
        });
        assert_life_ok(&out);
        let id = sim.file_id("pages").unwrap_or(None).unwrap();
        let durable_len = sim.disk().durable_len(id);
        let referenced: BTreeSet<u64> = [1u64, 2].into_iter().collect();
        let faults = sim.media_page("pages", SLOT, &referenced).unwrap();
        assert!(
            !faults.is_empty(),
            "seed {seed}: candidates exist, so a fault must be drawn"
        );
        for f in &faults {
            assert_eq!(
                (f.off % SLOT, f.len),
                (0, SLOT),
                "seed {seed}: a fault targeted a non-slot write at {}",
                f.off
            );
            assert!(
                f.off == SLOT || f.off == 2 * SLOT,
                "seed {seed}: a fault targeted slot {}",
                f.off / SLOT
            );
            if let MediaKind::Misdirected { to_off } = &f.kind {
                assert_eq!(to_off % SLOT, 0, "seed {seed}: a landing off-slot");
                assert!(
                    *to_off >= SLOT && *to_off < durable_len,
                    "seed {seed}: landing {to_off} outside [{SLOT}, {durable_len})"
                );
            }
        }
    }
}

/// S-ENV "Tasks": an Env call made from a thread the task body spawns
/// gives `EnvError::Halted` — thread-locals are not inherited.
#[test]
fn sim_halted_off_task() {
    let mut sim = Sim::new(1, drop_all());
    let out = sim.run_life(1, None, |env, _t| {
        let e = env.clone();
        let handle =
            std::thread::spawn(move || matches!(e.exists("page"), Err(EnvError::Halted { .. })));
        let halted = handle.join().map_err(|_| EnvError::Halted {
            what: "the spawned thread panicked".to_string(),
        })?;
        assert!(halted, "the off-task call must be Halted");
        Ok(())
    });
    assert_life_ok(&out);
}

// ---- FLAKE-FIX-1 (card FLAKE-FIX-1 Work 4): the start/finish race pins ----

/// **SIMENV-FLAKE-1, forced deterministically (FLAKE-FIX-1 Work 4):**
/// the racy interleaving itself — a workload task's wrapper runs the
/// body and `Sched::finish` before the harness thread (descheduled
/// between `scope.spawn` and `sched.start()` in `run_life`) has parked.
/// Pre-fix, `handoff_locked` found zero runnable candidates with
/// `any_alive` true and failed the healthy world as `DEADLOCK`;
/// FLAKE-FIX-1's guard recognises the baton-home-all-finished world as
/// a completed life. Joining the finisher first makes `finish` strictly
/// precede the harness's park — the exact race window, without the
/// race. Direct-`Sched` shape of `r14_pre_baton_host_step_is_refused`.
#[test]
fn sim_start_after_finish_is_not_deadlock() {
    /// The minimal host: just the scheduler core.
    struct Host {
        sched: SchedCore,
    }
    impl SchedHost for Host {
        fn sched_mut(&mut self) -> &mut SchedCore {
            &mut self.sched
        }
    }
    let sched = Arc::new(Sched::new(Host {
        sched: SchedCore::new(2, 991),
    }));
    let finisher = Arc::clone(&sched);
    let t = std::thread::spawn(move || {
        finisher.finish(1);
    });
    t.join().unwrap();
    assert!(
        sched.start().is_ok(),
        "a start() that parks after every task already finished is a completed life, not a deadlock (SIMENV-FLAKE-1)"
    );
}

/// **FLAKE-FIX-1 pin 2: a wait/wake pair completes a life.** Task 1
/// parks on `Tree(1)`; task 2 wakes it a thousand times, each wake
/// after its own yield — the yield hands the baton around, so a wake
/// that lands before the waiter parks is always followed by another
/// after it: the lost-wakeup window is un-hittable for any fixed seed.
/// The life must complete cleanly: no false deadlock, no livelock, no
/// hang.
#[test]
fn sim_wait_wake_completes() {
    let mut sim = Sim::new(2, drop_all());
    let out = sim.run_life(2, None, |env, t| {
        if t == 1 {
            env.wait_for(WaitKey::Tree(1))?;
            Ok(())
        } else {
            for _ in 0..1000 {
                env.yield_now("wake:step")?;
                env.wake(WaitKey::Tree(1));
            }
            Ok(())
        }
    });
    assert_life_ok(&out);
}

/// **FLAKE-FIX-1 pin 3: a genuinely all-parked world is still
/// reported.** The only task parks on a key nobody ever wakes — a real
/// deadlock. `run_life` must return (not hang), the world flags must
/// name the deadlock, and the task's own result must show it unwound
/// through the failure — never a panic, never a quiet `Ok`. (Card
/// Work 4 named only `Err(EnvError::Crashed)`; the task that trips
/// the detector unwinds through its own `park_locked`, which returns
/// `SimFail::Deadlock` → `Halted { "deadlock" }` — `Crashed` is what
/// every *other* task sees at its next scheduler call — so both arms
/// are caught here. Verbatim-body evidence in the card's RESULT.)
#[test]
fn sim_real_deadlock_reported() {
    let mut sim = Sim::new(1, drop_all());
    let out = sim.run_life(1, None, |env, _t| match env.wait_for(WaitKey::Tree(7)) {
        Ok(()) => panic!("woke with no waker"),
        Err(EnvError::Crashed) | Err(EnvError::Halted { .. }) => Ok(()),
        Err(e) => Err(e),
    });
    assert!(
        out.deadlock,
        "a genuinely all-parked world is reported as deadlock"
    );
    assert!(out.crashed, "the deadlock fails the world as a crash does");
    assert!(
        !out.panicked,
        "the deadlock unwinds the task, it does not panic it"
    );
    assert!(
        out.livelock.is_none(),
        "a parked world is a deadlock, not a livelock"
    );
    assert_eq!(
        out.results,
        vec![Ok(())],
        "the task observes the world failure and unwinds"
    );
}
