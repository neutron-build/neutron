//! SimEnv fidelity pins of card A-6b-0, the simulator the WAL and
//! recovery tests of A-6b and A-7d rely on: a task's body waits for its
//! first baton and `sleep` advances the clock after a crash (A-1b-F1
//! (a), (b)); the two argument checks (A-1b-F1 (e)); the lengths of a
//! synced shrink and of a power cut's survivors (A-1b-F2, `disk.rs`
//! departure D3); and the power cut that loses unsynced directory ops
//! independently (departure D4, S-STORE §12.5's `t_wal_start`). The
//! S-ENV invariants these pin are stated in `src/sim/env.rs`; PLAN-v3
//! §3.2 and S-STORE §12.5, §13 step 6 and §15 are the normative sources.
//! Every life here runs one task, except `sim_first_baton`'s three; no
//! test holds a test-side lock across an Env call, and none waits on
//! real time.

#![allow(missing_docs)]

use nucleus_env::sim::{DirLoss, FaultKnobs, LifeOutcome, Sim, SimEnv, TaskId};
use nucleus_env::{Env, EnvError, File, OpenMode};
use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::sync::Mutex;
use std::time::Duration;

/// The knobs that drop every unsynced byte and directory op (E4's
/// `drop_all`, as in `tests/sim.rs` and `tests/sim_env.rs`).
fn drop_all() -> FaultKnobs {
    FaultKnobs {
        keep_latest_pm: 0,
        tear_pm: 0,
    }
}

/// Asserts a life ended cleanly: no crash, deadlock, livelock or panic,
/// every task `Ok`, and no halt raised (A-1b-F1 (f)).
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
    assert!(
        out.halts.is_empty(),
        "the life raised halts: {:?}",
        out.halts
    );
}

/// A-1b-F1 (a): a task's body starts only once the baton first reaches
/// it, so its calls, `rand` included, never act before its task holds
/// the baton. Three tasks each draw 2,000 values with no other Env call
/// — a body that never yields, so it runs whole once its task holds the
/// baton — and then store `(task, draws)` in a test-side vector, the
/// only test-side lock, taken once, after the loop. Twenty fresh lives
/// of seed 42: every task returns `Ok`, no halt is raised, no draw is 0
/// (what a refused `rand` answers), and the three vectors equal the
/// first life's, task by task. Under FIRST-BATON-SKIP, a task whose
/// thread runs while another task's 2,000-draw body holds the baton, or
/// before the harness starts the life, draws refused zeros and records
/// halts; that one kill rests on OS scheduling (card "For the gate"),
/// and the long bodies make the race wide. On a correct build the test
/// is deterministic.
#[test]
fn sim_first_baton() {
    let mut first: Option<Vec<(TaskId, Vec<u64>)>> = None;
    for run in 0..20 {
        let stored: Mutex<Vec<(TaskId, Vec<u64>)>> = Mutex::new(Vec::new());
        let mut sim = Sim::new(42, drop_all());
        let out = sim.run_life(3, None, |env, t| {
            let mut v = Vec::with_capacity(2_000);
            for _ in 0..2_000 {
                v.push(env.rand());
            }
            stored.lock().unwrap().push((t, v));
            Ok(())
        });
        assert_eq!(
            out.results,
            vec![Ok(()), Ok(()), Ok(())],
            "run {run}: the tasks' results"
        );
        assert!(
            out.halts.is_empty(),
            "run {run}: a draw was refused: {:?}",
            out.halts
        );
        assert!(
            !out.crashed && !out.deadlock && !out.panicked && out.livelock.is_none(),
            "run {run}: crashed={} deadlock={} panicked={} livelock={:?}",
            out.crashed,
            out.deadlock,
            out.panicked,
            out.livelock
        );
        let mut vecs = stored.into_inner().unwrap();
        vecs.sort_by_key(|(t, _)| *t);
        assert_eq!(
            vecs.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "run {run}: every task stored its draws"
        );
        for (t, v) in &vecs {
            assert_eq!(v.len(), 2_000, "run {run}: task {t}'s draws");
            assert!(
                !v.contains(&0),
                "run {run}: task {t} drew a refused 0 (its body ran before its baton)"
            );
        }
        match &first {
            None => first = Some(vecs),
            Some(f) => assert_eq!(&vecs, f, "run {run}: the draws differ from run 0's"),
        }
    }
}

/// A-1b-F1 (b): in a failed world `sleep` still advances the clock by
/// its `d`, so a time-bounded loop ends after a crash; a task observes
/// the crash only through a fallible call. A dry run of a body that only
/// creates `x` gives the life's yield count `Y`; then, with the crash
/// armed at `Y`, the body's `open` returns `Crashed` (recorded, else
/// ignored) and a 10 ms deadline loop of 1 ms sleeps runs. It ends after
/// exactly 10 sleeps, with no halt. The cap of 1,000 iterations is the
/// watchdog — no real-time wait is involved: under SLEEP-CLOCK-STUCK the
/// loop ends at the cap with `n == 1_000` instead of hanging.
#[test]
fn sim_sleep_after_crash() {
    let open_x = |env: &SimEnv| env.open("x", OpenMode::CreateNew).map(drop);
    let mut dry = Sim::new(3, drop_all());
    let out = dry.run_life(1, None, |env, _t| open_x(env));
    assert_life_ok(&out);
    let y = out.yields;

    let opened = Mutex::new(None);
    let slept = Mutex::new(None);
    let mut sim = Sim::new(3, drop_all());
    let out = sim.run_life(1, Some(y), |env, _t| {
        let r = open_x(env);
        *opened.lock().unwrap() = Some(r);
        let deadline = env.now() + Duration::from_millis(10);
        let mut n = 0u32;
        while env.now() < deadline {
            env.sleep(Duration::from_millis(1));
            n += 1;
            if n == 1_000 {
                break;
            }
        }
        *slept.lock().unwrap() = Some(n);
        Ok(())
    });
    assert!(out.crashed, "the life must crash at the armed yield");
    assert!(!out.panicked, "the life panicked");
    assert_eq!(
        opened.into_inner().unwrap(),
        Some(Err(EnvError::Crashed)),
        "the open is where the crash fires"
    );
    assert_eq!(
        slept.into_inner().unwrap(),
        Some(10),
        "the deadline loop must end after 10 sleeps of 1 ms"
    );
    assert!(out.halts.is_empty(), "halts: {:?}", out.halts);
}

/// A-1b-F1 (e): `Sim::media_page` with a zero slot, and
/// `SimFile::accepted` with an overflowing `off + len`, are each
/// `Io { InvalidInput }` with exactly these `op` values, before any disk
/// call: the disk's event log is unchanged by either. The page file
/// holds a durable write, so a draw past the check would reach the
/// copied slot arithmetic (MEDIA-SLOT-ZERO: a division by zero), and
/// the overflowing accept would overflow inside the world, with it held
/// (ACCEPTED-OVERFLOW).
#[test]
fn sim_argument_checks() {
    let setup = |sim: &mut Sim| {
        let out = sim.run_life(1, None, |env, _t| {
            let f = env.open("pages", OpenMode::CreateNew)?;
            f.write_at(0, &[1u8; 4096])?;
            f.sync_all()?;
            env.sync_dir("")?;
            Ok(())
        });
        assert_life_ok(&out);
    };

    // media_page with slot 0.
    let mut sim = Sim::new(1, drop_all());
    setup(&mut sim);
    let events = format!("{:?}", sim.disk().events);
    let media = sim.disk().media.len();
    match sim.media_page("pages", 0, &BTreeSet::new()) {
        Err(EnvError::Io {
            op: "media_page",
            path,
            kind: ErrorKind::InvalidInput,
            ..
        }) => assert_eq!(path, "pages", "media_page names the page file"),
        other => panic!("media_page with slot 0 gave {other:?}"),
    }
    assert_eq!(
        format!("{:?}", sim.disk().events),
        events,
        "media_page with slot 0 changed the event log"
    );
    assert_eq!(sim.disk().media.len(), media, "a media fault was drawn");

    // accepted with an overflowing range, in a life; compared with the
    // same life without the calls.
    let seen = Mutex::new(Vec::new());
    let mut with = Sim::new(1, drop_all());
    setup(&mut with);
    let out = with.run_life(1, None, |env, _t| {
        let f = env.open("pages", OpenMode::ReadWrite)?;
        let a = f.accepted(u64::MAX, 1);
        let b = f.accepted(1, u64::MAX);
        seen.lock().unwrap().extend([a, b]);
        Ok(())
    });
    assert_life_ok(&out);
    let seen = seen.into_inner().unwrap();
    assert_eq!(seen.len(), 2);
    for r in seen {
        match r {
            Err(EnvError::Io {
                op: "accepted",
                path,
                kind: ErrorKind::InvalidInput,
                ..
            }) => assert_eq!(path, "pages", "accepted names the handle's path"),
            other => panic!("an overflowing accepted gave {other:?}"),
        }
    }
    let mut without = Sim::new(1, drop_all());
    setup(&mut without);
    let out = without.run_life(1, None, |env, _t| {
        let _f = env.open("pages", OpenMode::ReadWrite)?;
        Ok(())
    });
    assert_life_ok(&out);
    assert_eq!(
        format!("{:?}", with.disk().events),
        format!("{:?}", without.disk().events),
        "an overflowing accepted changed the event log"
    );
    assert!(with.disk().accepts.is_empty(), "an accept was recorded");
}

/// One life that creates `name` and makes it durable and dir-synced
/// (the fixture of `sim_disk_lengths`).
fn durable_empty_file(sim: &mut Sim, name: &'static str) {
    let out = sim.run_life(1, None, |env, _t| {
        let f = env.open(name, OpenMode::CreateNew)?;
        f.sync_all()?;
        env.sync_dir("")?;
        Ok(())
    });
    assert_life_ok(&out);
}

/// `name`'s length and bytes, read through SimEnv in a new life (`None`
/// if the life did not deliver them; the life itself is asserted clean).
fn read_back(sim: &mut Sim, name: &'static str) -> Option<(u64, Vec<u8>)> {
    let got = Mutex::new(None);
    let out = sim.run_life(1, None, |env, _t| {
        let f = env.open(name, OpenMode::Read)?;
        let len = f.len()?;
        let mut buf = vec![0u8; len as usize];
        let n = f.read_at(0, &mut buf)?;
        assert_eq!(n as u64, len, "a short read below the length");
        if let Ok(mut g) = got.lock() {
            *g = Some((len, buf));
        }
        Ok(())
    });
    assert_life_ok(&out);
    got.into_inner().ok().flatten()
}

/// A-1b-F2 (D3), the A-1b verdict's Findings 3–4, through SimEnv on
/// dir-synced files. (i) 1,024 × `0xAA` synced, a synced shrink to 100,
/// a synced grow to 1,000, a power cut: `[100, 1000)` reads zeros and
/// the length is 1,000 (D3 (a); SHRINK-KEEPS-BYTES fails it). (ii) a
/// synced shrink to 100, then an unsynced 10-byte write of `0x55` at
/// 200 that the cut keeps: `[100, 200)` reads zeros, `[200, 210)` the
/// write, and the length is 210 (D3 (a) and (b)). (iii) a new empty
/// file and an unsynced 10-byte write at 0 that the cut keeps: the
/// length is 10, the end of the written data, not the sector's 512
/// (D3 (b); LEN-SECTOR-END fails it). (iv) the shrink and the grow under
/// one `sync_all`, with the bytes behind the shrink durable (iv-a) or
/// written and still unsynced (iv-b) before it: `[100, 1000)` reads
/// zeros, as in (i) — `sync_all` makes the current bytes durable,
/// exactly (D3 (a), this card's judgment call over truncation alone).
/// (v) a new empty file and three unsynced writes of nonzero bytes over
/// four sectors, cut at `keep_latest_pm` 500 and `tear_pm` 500 over seeds
/// 0..32: the length is 0 or its last byte is one a surviving write put
/// there, never a zero past the written data — whatever prefix each
/// sector keeps (D3 (b)'s prefix and its clip at the sector's end).
#[test]
fn sim_disk_lengths() {
    let aa = [0xAAu8; 1024];

    // (i) a synced shrink, then a synced grow, then the cut.
    let mut sim = Sim::new(1, drop_all());
    durable_empty_file(&mut sim, "f");
    let out = sim.run_life(1, None, |env, _t| {
        let f = env.open("f", OpenMode::ReadWrite)?;
        f.write_at(0, &aa)?;
        f.sync_all()?;
        f.set_len(100)?;
        f.sync_all()?;
        f.set_len(1000)?;
        f.sync_all()?;
        Ok(())
    });
    assert_life_ok(&out);
    sim.power_cut();
    let (len, bytes) = read_back(&mut sim, "f").unwrap();
    assert_eq!(len, 1000, "(i): the synced length");
    assert_eq!(&bytes[..100], &aa[..100], "(i): the bytes below the shrink");
    assert!(
        bytes[100..].iter().all(|&b| b == 0),
        "(i): {} pre-shrink bytes came back in [100, 1000)",
        bytes[100..].iter().filter(|&&b| b != 0).count()
    );

    // (ii) a synced shrink, then an unsynced write past it that the cut
    // keeps.
    let keep = FaultKnobs {
        keep_latest_pm: 1000,
        tear_pm: 0,
    };
    let mut sim = Sim::new(1, keep);
    durable_empty_file(&mut sim, "f");
    let out = sim.run_life(1, None, |env, _t| {
        let f = env.open("f", OpenMode::ReadWrite)?;
        f.write_at(0, &aa)?;
        f.sync_all()?;
        f.set_len(100)?;
        f.sync_all()?;
        f.write_at(200, &[0x55u8; 10])?;
        Ok(())
    });
    assert_life_ok(&out);
    sim.power_cut();
    let (len, bytes) = read_back(&mut sim, "f").unwrap();
    assert_eq!(
        &bytes[..100],
        &aa[..100],
        "(ii): the bytes below the shrink"
    );
    assert!(
        bytes[100..200].iter().all(|&b| b == 0),
        "(ii): pre-shrink bytes came back in [100, 200)"
    );
    assert_eq!(&bytes[200..210], &[0x55u8; 10], "(ii): the kept write");
    assert_eq!(len, 210, "(ii): the length ends at the kept write's end");

    // (iii) a new empty file, an unsynced write at 0 that the cut keeps.
    let mut sim = Sim::new(1, keep);
    durable_empty_file(&mut sim, "g");
    let out = sim.run_life(1, None, |env, _t| {
        let f = env.open("g", OpenMode::ReadWrite)?;
        f.write_at(0, &[0x77u8; 10])?;
        Ok(())
    });
    assert_life_ok(&out);
    sim.power_cut();
    let (len, bytes) = read_back(&mut sim, "g").unwrap();
    assert_eq!(len, 10, "(iii): the length is the written data's end");
    assert_eq!(bytes, vec![0x77u8; 10], "(iii): the kept write");

    // (iv) the shrink and the grow under one sync_all.
    for synced_before in [true, false] {
        let mut sim = Sim::new(1, drop_all());
        durable_empty_file(&mut sim, "f");
        let out = sim.run_life(1, None, |env, _t| {
            let f = env.open("f", OpenMode::ReadWrite)?;
            f.write_at(0, &aa)?;
            if synced_before {
                f.sync_all()?;
            }
            f.set_len(100)?;
            f.set_len(1000)?;
            f.sync_all()?;
            Ok(())
        });
        assert_life_ok(&out);
        sim.power_cut();
        let (len, bytes) = read_back(&mut sim, "f").unwrap();
        assert_eq!(len, 1000, "(iv, synced_before={synced_before}): the length");
        assert_eq!(
            &bytes[..100],
            &aa[..100],
            "(iv, synced_before={synced_before}): the bytes below the shrink"
        );
        assert!(
            bytes[100..].iter().all(|&b| b == 0),
            "(iv, synced_before={synced_before}): {} pre-shrink bytes came back in [100, 1000)",
            bytes[100..].iter().filter(|&&b| b != 0).count()
        );
    }

    // (v) any prefix per sector: the length ends at written data.
    let mixed = FaultKnobs {
        keep_latest_pm: 500,
        tear_pm: 500,
    };
    let mut unaligned = 0u32;
    for seed in 0..32u64 {
        let mut sim = Sim::new(seed, mixed);
        durable_empty_file(&mut sim, "h");
        let out = sim.run_life(1, None, |env, _t| {
            let f = env.open("h", OpenMode::ReadWrite)?;
            f.write_at(0, &[0x11u8; 700])?;
            f.write_at(600, &[0x22u8; 500])?;
            f.write_at(1500, &[0x33u8; 100])?;
            Ok(())
        });
        assert_life_ok(&out);
        sim.power_cut();
        let (len, bytes) = read_back(&mut sim, "h").unwrap();
        if len > 0 {
            assert_ne!(
                bytes.last(),
                Some(&0),
                "(v) seed {seed}: the length {len} ends past the written data"
            );
        }
        if len % 512 != 0 {
            unaligned += 1;
        }
    }
    assert!(unaligned > 0, "(v): no seed kept a length off a sector end");
}

/// The names bound after a power cut, from a fixed list (`wal/<name>`).
fn bound(sim: &Sim, names: &[&'static str]) -> BTreeSet<&'static str> {
    names
        .iter()
        .copied()
        .filter(|n| matches!(sim.file_id(&format!("wal/{n}")), Ok(Some(_))))
        .collect()
}

/// A `Sim` whose `wal` directory holds `names`, each created, written,
/// synced and dir-synced.
fn durable_wal(seed: u64, knobs: FaultKnobs, mode: DirLoss, names: &'static [&'static str]) -> Sim {
    let mut sim = Sim::new(seed, knobs);
    sim.set_dir_loss(mode);
    let out = sim.run_life(1, None, |env, _t| {
        env.create_dir("wal")?;
        env.sync_dir("")?;
        for n in names {
            let f = env.open(&format!("wal/{n}"), OpenMode::CreateNew)?;
            f.write_at(0, n.as_bytes())?;
            f.sync_all()?;
        }
        env.sync_dir("wal")?;
        Ok(())
    });
    assert_life_ok(&out);
    sim
}

/// D4 (card A-6b-0; S-STORE §12.5's `t_wal_start`): a power cut under
/// `DirLoss::Independent` keeps each unsynced directory op
/// independently, applied in issue order where it still applies;
/// `DirLoss::Prefix`, the default, is E4's rule with E4's draws.
/// (i) durable `wal/a`–`wal/d`, a life renaming `a → e` then `b → f`
/// with no `sync_dir`, a cut, seeds 0..64 at `keep_latest_pm` 500: under
/// `Independent` some seed keeps the later rename and loses the earlier
/// (`f` bound, `e` not) — RECYCLE-UNORDERED's in-order form — and under
/// `Prefix` none does (DIR-INDEPENDENT-AS-PREFIX fails it). (ii) the
/// same at `keep_latest_pm` 0 loses both renames, and at 1000 keeps
/// both. (iii) an unsynced create of `wal/x` then a rename `x → y`,
/// under `Independent`: `y` survives only with the create, naming the
/// file the create made, with `x` unbound and no op dropped (the cut's
/// count), and some seed ends with neither name, the rename dropped with
/// its source's create (DIR-INDEPENDENT-NO-CLOSURE fails it). (iv) the
/// file data a cut keeps is the same under both modes, seeds 0..16 at
/// `keep_latest_pm` 500 and `tear_pm` 500, with no directory op and with
/// one beside the writes. (v) the Base's golden: three unsynced creates
/// `wal/p0`–`p2`, seeds 0..32 at `keep_latest_pm` 500, give exactly the
/// survivors E4's rule gave on the Base, under the default and under an
/// explicit `Prefix` (PREFIX-DRAWS-MOVED fails it). (vi) the
/// applicability of `Create` and `Unlink` too: durable `wal/a`, then an
/// unsynced rename `a → b`, a create of a new `a` and its removal, under
/// `Independent` at `keep_latest_pm` 500: the first file keeps exactly
/// one name, `a` or `b`, whichever its own rename left it, so neither the
/// new file's create nor its unlink ever takes `a` from it.
#[test]
fn sim_dir_loss_independent() {
    const ABCD: &[&str] = &["a", "b", "c", "d"];
    let half = FaultKnobs {
        keep_latest_pm: 500,
        tear_pm: 0,
    };
    let renames = |sim: &mut Sim| {
        let out = sim.run_life(1, None, |env, _t| {
            env.rename("wal/a", "wal/e")?;
            env.rename("wal/b", "wal/f")?;
            Ok(())
        });
        assert_life_ok(&out);
        sim.power_cut();
    };

    // (i) a later rename kept while an earlier one is lost.
    for mode in [DirLoss::Independent, DirLoss::Prefix] {
        let mut later_only = 0u32;
        for seed in 0..64u64 {
            let mut sim = durable_wal(seed, half, mode, ABCD);
            renames(&mut sim);
            let names = bound(&sim, &["a", "b", "c", "d", "e", "f"]);
            assert!(
                names.contains("a") != names.contains("e")
                    && names.contains("b") != names.contains("f"),
                "(i) {mode:?} seed {seed}: a file has two names or none: {names:?}"
            );
            assert!(
                names.contains("c") && names.contains("d"),
                "(i) {mode:?} seed {seed}: a durable name was lost: {names:?}"
            );
            if names.contains("f") && !names.contains("e") {
                later_only += 1;
            }
        }
        match mode {
            DirLoss::Independent => assert!(
                later_only > 0,
                "(i) Independent: no seed kept the later rename and lost the earlier"
            ),
            DirLoss::Prefix => assert_eq!(
                later_only, 0,
                "(i) Prefix: a seed kept the later rename and lost the earlier"
            ),
        }
    }

    // (ii) every op lost at 0, every applicable op kept at 1000.
    for (pm, want) in [(0u32, ["a", "b"]), (1000, ["e", "f"])] {
        let knobs = FaultKnobs {
            keep_latest_pm: pm,
            tear_pm: 0,
        };
        for seed in 0..64u64 {
            let mut sim = durable_wal(seed, knobs, DirLoss::Independent, ABCD);
            renames(&mut sim);
            let names = bound(&sim, &["a", "b", "e", "f"]);
            assert_eq!(
                names,
                want.into_iter().collect::<BTreeSet<_>>(),
                "(ii) keep_latest_pm {pm} seed {seed}"
            );
        }
    }

    // (iii) a rename whose source's create was lost is lost too.
    let mut neither = 0u32;
    for seed in 0..64u64 {
        let mut sim = durable_wal(seed, half, DirLoss::Independent, &[]);
        let out = sim.run_life(1, None, |env, _t| {
            let f = env.open("wal/x", OpenMode::CreateNew)?;
            f.write_at(0, b"x")?;
            env.rename("wal/x", "wal/y")?;
            Ok(())
        });
        assert_life_ok(&out);
        let made = sim.file_id("wal/y").unwrap().unwrap();
        let dropped = sim.power_cut();
        let x = sim.file_id("wal/x").unwrap();
        let y = sim.file_id("wal/y").unwrap();
        match (x, y) {
            (None, Some(id)) => {
                assert_eq!(id, made, "(iii) seed {seed}: y names another file");
                assert_eq!(
                    dropped, 0,
                    "(iii) seed {seed}: y survived although an op was dropped (its create?)"
                );
            }
            (Some(id), None) => {
                assert_eq!(id, made, "(iii) seed {seed}: x names another file");
                assert_eq!(dropped, 1, "(iii) seed {seed}: x alone, the rename lost");
            }
            (None, None) => {
                assert_eq!(dropped, 2, "(iii) seed {seed}: neither name, both ops lost");
                neither += 1;
            }
            (Some(_), Some(_)) => panic!("(iii) seed {seed}: both names are bound"),
        }
    }
    assert!(
        neither > 0,
        "(iii): no seed lost the create (and with it the rename)"
    );

    // (vi) a kept op that no longer applies is dropped: Create and
    // Unlink as well as Rename.
    let mut renamed = 0u32;
    let mut stayed = 0u32;
    for seed in 0..64u64 {
        let mut sim = durable_wal(seed, half, DirLoss::Independent, &["a"]);
        let first = sim.file_id("wal/a").unwrap().unwrap();
        let out = sim.run_life(1, None, |env, _t| {
            env.rename("wal/a", "wal/b")?;
            env.open("wal/a", OpenMode::CreateNew)?;
            env.remove("wal/a")?;
            Ok(())
        });
        assert_life_ok(&out);
        sim.power_cut();
        let a = sim.file_id("wal/a").unwrap();
        let b = sim.file_id("wal/b").unwrap();
        match (a == Some(first), b == Some(first)) {
            (true, false) => stayed += 1,
            (false, true) => renamed += 1,
            _ => panic!("(vi) seed {seed}: the first file is under a={a:?} b={b:?}"),
        }
        if let Some(id) = a {
            assert!(
                id == first || b == Some(first),
                "(vi) seed {seed}: the new file took a while the first file kept no name"
            );
        }
    }
    assert!(
        renamed > 0 && stayed > 0,
        "(vi): renamed {renamed}, stayed {stayed}: both outcomes must occur"
    );

    // (iv) the file data is resolved the same way in both modes.
    let mixed = FaultKnobs {
        keep_latest_pm: 500,
        tear_pm: 500,
    };
    for with_dir_op in [false, true] {
        for seed in 0..16u64 {
            let mut outcomes = Vec::new();
            for mode in [DirLoss::Prefix, DirLoss::Independent] {
                let mut sim = durable_wal(seed, mixed, mode, &["d"]);
                let out = sim.run_life(1, None, |env, _t| {
                    let f = env.open("wal/d", OpenMode::ReadWrite)?;
                    f.write_at(0, &[0x11u8; 1500])?;
                    f.write_at(300, &[0x22u8; 900])?;
                    f.write_at(1400, &[0x33u8; 700])?;
                    if with_dir_op {
                        env.open("wal/z", OpenMode::CreateNew)?;
                    }
                    Ok(())
                });
                assert_life_ok(&out);
                sim.power_cut();
                let id = sim.file_id("wal/d").unwrap().unwrap();
                let len = sim.disk().cur_len(id);
                outcomes.push((len, sim.disk().durable_read(id, 0, len)));
            }
            assert_eq!(
                outcomes[0], outcomes[1],
                "(iv) seed {seed}, dir op {with_dir_op}: the modes resolved the data differently"
            );
        }
    }

    // (v) Prefix is unchanged: the golden recorded on the Base
    // (c140f463, before disk.rs changed) for three unsynced creates.
    const GOLDEN: [&[&str]; 32] = [
        &["p0", "p1", "p2"], // seed 0
        &["p0", "p1", "p2"], // seed 1
        &["p0", "p1", "p2"], // seed 2
        &["p0", "p1"],       // seed 3
        &["p0", "p1", "p2"], // seed 4
        &["p0", "p1"],       // seed 5
        &["p0", "p1", "p2"], // seed 6
        &[],                 // seed 7
        &["p0", "p1", "p2"], // seed 8
        &["p0", "p1", "p2"], // seed 9
        &["p0", "p1", "p2"], // seed 10
        &["p0", "p1", "p2"], // seed 11
        &["p0", "p1"],       // seed 12
        &["p0", "p1", "p2"], // seed 13
        &["p0", "p1", "p2"], // seed 14
        &["p0"],             // seed 15
        &["p0", "p1", "p2"], // seed 16
        &["p0", "p1", "p2"], // seed 17
        &["p0", "p1", "p2"], // seed 18
        &["p0", "p1", "p2"], // seed 19
        &["p0"],             // seed 20
        &[],                 // seed 21
        &["p0"],             // seed 22
        &["p0"],             // seed 23
        &["p0", "p1", "p2"], // seed 24
        &["p0", "p1", "p2"], // seed 25
        &["p0", "p1"],       // seed 26
        &[],                 // seed 27
        &[],                 // seed 28
        &["p0", "p1", "p2"], // seed 29
        &[],                 // seed 30
        &["p0", "p1", "p2"], // seed 31
    ];
    for explicit in [false, true] {
        for (seed, want) in (0u64..).zip(GOLDEN) {
            let mut sim = Sim::new(seed, half);
            if explicit {
                sim.set_dir_loss(DirLoss::Prefix);
            }
            let setup = sim.run_life(1, None, |env, _t| {
                env.create_dir("wal")?;
                env.sync_dir("")?;
                env.sync_dir("wal")?;
                Ok(())
            });
            assert_life_ok(&setup);
            let out = sim.run_life(1, None, |env, _t| {
                for n in ["wal/p0", "wal/p1", "wal/p2"] {
                    let _f = env.open(n, OpenMode::CreateNew)?;
                }
                Ok(())
            });
            assert_life_ok(&out);
            sim.power_cut();
            assert_eq!(
                bound(&sim, &["p0", "p1", "p2"]),
                want.iter().copied().collect::<BTreeSet<_>>(),
                "(v) seed {seed}, explicit Prefix {explicit}: not the Base's survivors"
            );
        }
    }
}
