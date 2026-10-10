//! G2 soak: 8 threads over the 16-slot key space at each of RC, RR and SER,
//! with deterministic seeds. The size is an iteration count (programs per
//! thread, calibrated so the three levels run well under 20 s together in a
//! debug build), never a wall-clock bound.
//!
//! Asserted per level (the engine's promises, C-T0 §8 and §11):
//!
//! - **SER**: no cycle of any kind among the committed txns (I-SER), no G1,
//!   and every 40001 explained (a cycle through the victim, a dangerous
//!   structure -- counted as a false positive, which is legal -- or a write
//!   conflict).
//! - **RR**: write skew may appear, but never G0/G1, never a lost update,
//!   never a cycle without two adjacent rw edges (no G-single, no long fork),
//!   never a non-repeatable read.
//! - **RC**: never G0/G1a/G1b/G1c.
//!
//! plus, at every level, every 40P01 sits on a potential wait-for cycle, 23505
//! and 23503 have something to collide with, and no intent leaks once the
//! resolver drains (I-LEAK). A failing seed prints the whole history in the
//! compact trace format on stderr, and the violation report in the panic.
//!
//! The soak must also be able to fail: a run of the same workload on a
//! deliberately broken store (an RR claim with per-statement snapshots) is
//! flagged, and an RC run judged as if it were SERIALIZABLE shows cycles.

use super::check::{check, Kind, Verdict};
use super::history::{Fate, History, Level, OpKind};
use super::runner::{gen_prog, run_workload, Bug, Config, Mix, POp, Prog, Run};

/// Programs per thread (8 threads): the calibration knob.
const PROGS_PER_THREAD: u32 = 1500;

const SEEDS_RC: [u64; 1] = [0x4732_0001];
const SEEDS_RR: [u64; 1] = [0x4732_0002];
const SEEDS_SER: [u64; 1] = [0x4732_0003];

fn run_seed(level: Level, seed: u64, progs: u32, mix: Mix) -> (Run, Verdict) {
    let mut cfg = Config::new(seed, level);
    cfg.progs = progs;
    cfg.mix = mix;
    let t0 = std::time::Instant::now();
    let run = run_workload(&cfg);
    let t1 = std::time::Instant::now();
    let verdict = check(&run.history, level);
    // Reported, never asserted.
    println!(
        "g2 soak {} seed {seed:#x}: run {:?}, check {:?}",
        level.short(),
        t1 - t0,
        t1.elapsed()
    );
    (run, verdict)
}

/// Fails the test with the whole trace of the seed when `verdict` has a
/// violation or the run leaked.
fn assert_sound(level: Level, seed: u64, run: &Run, verdict: &Verdict) {
    let violations = verdict.violations();
    if !violations.is_empty() || run.leaked_intents != 0 {
        eprintln!(
            "g2 soak FAILED: level {} seed {seed:#x}\n{}",
            level.short(),
            run.history.trace()
        );
        panic!(
            "level {} seed {seed:#x}: {} violation(s), {} leaked intent(s) (full trace on stderr)\n{}{}",
            level.short(),
            violations.len(),
            run.leaked_intents,
            run.leak_report,
            verdict.report(&run.history)
        );
    }
}

fn summary(level: Level, seed: u64, v: &Verdict) {
    let s = &v.stats;
    println!(
        "g2 soak {} seed {seed:#x}: {} txns, {} committed, {} rolled back; edges ww {} wr {} rw {}; \
         40001 {} (cycle {}, dangerous/false-positive {}, conflict {}, unexplained {}); \
         40P01 {} (explained {}); 23505 {}; 23503 {}; anomalies found: {:?}",
        level.short(),
        s.txns,
        s.committed,
        s.rolled_back,
        s.ww,
        s.wr,
        s.rw,
        s.e40001,
        s.f_cycle,
        s.f_dangerous,
        s.f_conflict,
        s.f_unexplained,
        s.e40p01,
        s.deadlocks_explained,
        s.e23505,
        s.e23503,
        v.anomalies.iter().map(|a| a.kind).collect::<Vec<_>>()
    );
}

#[test]
fn g2_soak_serializable() {
    for seed in SEEDS_SER {
        let (run, verdict) = run_seed(Level::Serializable, seed, PROGS_PER_THREAD, Mix::SER);
        summary(Level::Serializable, seed, &verdict);
        assert_sound(Level::Serializable, seed, &run, &verdict);
        assert!(
            verdict.serializable(),
            "SER run not serializable:\n{}",
            verdict.report(&run.history)
        );
        assert!(verdict.stats.committed > 500, "{:?}", verdict.stats);
        assert_eq!(verdict.stats.f_unexplained, 0);
    }
}

#[test]
fn g2_soak_repeatable_read() {
    for seed in SEEDS_RR {
        let (run, verdict) = run_seed(Level::RepeatableRead, seed, PROGS_PER_THREAD, Mix::STANDARD);
        summary(Level::RepeatableRead, seed, &verdict);
        assert_sound(Level::RepeatableRead, seed, &run, &verdict);
        // Only write skew (adjacent rw cycles) may remain.
        for a in &verdict.anomalies {
            assert!(
                matches!(a.kind, Kind::WriteSkew | Kind::G2Item),
                "unexpected RR anomaly {:?}",
                a.kind
            );
        }
        assert!(verdict.stats.committed > 500, "{:?}", verdict.stats);
    }
}

#[test]
fn g2_soak_read_committed() {
    for seed in SEEDS_RC {
        let (run, verdict) = run_seed(Level::ReadCommitted, seed, PROGS_PER_THREAD, Mix::STANDARD);
        summary(Level::ReadCommitted, seed, &verdict);
        assert_sound(Level::ReadCommitted, seed, &run, &verdict);
        assert!(verdict.stats.committed > 500, "{:?}", verdict.stats);
        // The soak can fail: the same history is not serializable.
        let as_ser = check(&run.history, Level::Serializable);
        assert!(
            as_ser.has_cycle(),
            "an RC run with this much interference should show cycles; stats {:?}",
            as_ser.stats
        );
    }
}

/// An RR claim on a store that snapshots per statement: the soak notices.
#[test]
fn g2_soak_broken_store_is_caught() {
    let mut cfg = Config::new(0x4732_00ff, Level::RepeatableRead);
    cfg.progs = 60;
    cfg.bug = Bug::PerStmtSnapshot;
    let run = run_workload(&cfg);
    let verdict = check(&run.history, Level::RepeatableRead);
    assert!(
        !verdict.violations().is_empty(),
        "the broken store went unnoticed: {:?}",
        verdict.stats
    );
}

// ----- determinism -------------------------------------------------------------

#[test]
fn g2_programs_are_a_pure_function_of_the_seed() {
    for session in 1..=8u16 {
        for idx in 0..50 {
            let a = gen_prog(0x1234, session, idx, &Mix::STANDARD);
            let b = gen_prog(0x1234, session, idx, &Mix::STANDARD);
            assert_eq!(a, b);
        }
    }
    let differs = (0..50)
        .any(|i| gen_prog(0x1234, 1, i, &Mix::STANDARD) != gen_prog(0x1235, 1, i, &Mix::STANDARD));
    assert!(differs, "a different seed must give different programs");
}

/// The statement shape a program is recorded as.
fn shape(p: &Prog) -> Vec<(OpKind, Vec<u8>)> {
    p.ops
        .iter()
        .map(|op| match *op {
            POp::Read(k) => (OpKind::Read, vec![k]),
            POp::Scan(lo, hi) => (OpKind::Scan, vec![lo, hi]),
            POp::Insert(k) => (OpKind::Insert, vec![k]),
            POp::Update(k) => (OpKind::Update, vec![k]),
            POp::Delete(k) => (OpKind::Delete, vec![k]),
            POp::Upsert(k) => (OpKind::Upsert, vec![k]),
            POp::FkInsert(c) => (OpKind::FkInsert, vec![c, super::history::parent_of(c)]),
            POp::SpUpdate(k) => (OpKind::SpUpdate, vec![k]),
        })
        .collect()
}

/// Same seed, same recorded op stream per session: every txn that ran its
/// whole program (committed or rolled back on purpose) recorded exactly the
/// statements the seed's program for `(session, index)` says, however the
/// threads interleaved; and the checker's verdict is a function of the
/// history it is given.
#[test]
fn g2_same_seed_same_op_streams() {
    let seed = 0x4732_0042;
    let mut histories: Vec<History> = Vec::new();
    for _ in 0..2 {
        let mut cfg = Config::new(seed, Level::ReadCommitted);
        cfg.threads = 4;
        cfg.progs = 40;
        histories.push(run_workload(&cfg).history);
    }
    for h in &histories {
        let mut finished = 0;
        for t in &h.txns {
            if t.session == 0 || !matches!(t.fate, Fate::Committed(_) | Fate::Rolledback) {
                continue;
            }
            let prog = gen_prog(seed, t.session, t.prog, &Mix::STANDARD);
            let recorded: Vec<(OpKind, Vec<u8>)> =
                t.ops.iter().map(|o| (o.kind, o.keys.clone())).collect();
            assert_eq!(
                recorded,
                shape(&prog),
                "session {} prog {}",
                t.session,
                t.prog
            );
            finished += 1;
        }
        assert!(finished > 100, "only {finished} finished programs");
    }
    // The same history checked twice gives the same verdict.
    for h in &histories {
        assert_eq!(
            check(h, Level::ReadCommitted),
            check(h, Level::ReadCommitted)
        );
    }
}

// ----- ESCALATE -----------------------------------------------------------------

/// ESCALATE (engine liveness bug found by the soak; the engine is not
/// touched by this card). Two sessions each hold a row and then upsert the
/// row the other holds. Every restart of an `INSERT .. ON CONFLICT` attempt
/// (C-T0 5.3.1: "abandon exactly as ROLLBACK TO") bumps the abandoning txn's
/// wake generation and wakes its waiters (5.5) even when the abandon dropped
/// nothing, so the two upserts keep waking each other, neither wait lasts
/// `deadlock_timeout`, no 40P01 is ever raised and both threads spin (I-LIVE
/// b is violated). The same script with plain UPDATEs ends in one 40P01
/// (`g2_live_ww_deadlock_is_broken_once`). The soak's generator keeps an
/// upsert off any txn that already holds a lock, so it does not trip this.
#[test]
#[ignore = "ESCALATE: ON CONFLICT restart livelocks two upserts that wait on each other (no 40P01)"]
fn g2_escalate_upsert_cycle_livelocks() {
    use super::history::SqlErr;
    use super::runner::Rig;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Barrier;

    let rig = Rig::new(5);
    rig.setup();
    let barrier = Barrier::new(2);
    let finished = AtomicBool::new(false);
    let go = |id: u16, first: u8, second: u8| -> Result<(), SqlErr> {
        let mut s = rig.session(id, Level::ReadCommitted);
        s.update(first)?;
        barrier.wait();
        s.upsert(second)?;
        s.commit()
    };
    let (r1, r2) = std::thread::scope(|sc| {
        // Not a timing assertion: it only turns a livelock into a failure.
        sc.spawn(|| {
            for _ in 0..200 {
                std::thread::sleep(std::time::Duration::from_millis(100));
                if finished.load(Ordering::SeqCst) {
                    return;
                }
            }
            eprintln!("g2: the upsert cycle never resolved (livelock)");
            std::process::abort();
        });
        let a = sc.spawn(|| go(1, 0, 2));
        let b = sc.spawn(|| go(2, 2, 0));
        let r = (a.join().expect("t1"), b.join().expect("t2"));
        finished.store(true, Ordering::SeqCst);
        r
    });
    let deadlocks = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Err(SqlErr::Deadlock)))
        .count();
    assert_eq!(deadlocks, 1, "{r1:?} {r2:?}");
}

/// ESCALATE (engine SSI bug found by the soak; the engine is not touched by
/// this card). `Core::fk_check_child` registers the SIREAD on the parent but
/// reads it with `NoSsi`, so the reader-side rw edge to a parent writer whose
/// intent was placed *before* the SIREAD is dropped (C-T0 8.2 relies on that
/// edge: "if W's intent placement precedes the opening of the view R reads k
/// from, R sees the intent and records the edge"). Result: write skew that
/// SERIALIZABLE lets through. A reads child c0 (absent) and updates parent p0;
/// B inserts c0 and so reads p0. A plain `Ssi::read_key` in B's place aborts
/// one of them (`g2_live_ser_plain_parent_read_aborts_one`).
#[test]
#[ignore = "ESCALATE: fk_check_child drops the reader-side rw edge (NoSsi): SER write skew"]
fn g2_escalate_fk_parent_read_drops_rw_edge() {
    use super::runner::Rig;
    let rig = Rig::new(5);
    rig.setup();
    let mut a = rig.session(1, Level::Serializable);
    let mut b = rig.session(2, Level::Serializable);
    assert!(!a.update(14).expect("A reads c0"), "c0 does not exist yet");
    assert!(a.update(12).expect("A updates p0"));
    b.fk_insert(14).expect("B inserts c0 under p0");
    a.commit().expect("A commits");
    b.commit()
        .expect("B commits: SSI missed the rw edge B -> A");
    let run = rig.finish();
    let verdict = check(&run.history, Level::Serializable);
    assert!(
        verdict.serializable(),
        "SERIALIZABLE committed a non-serializable pair:\n{}",
        verdict.report(&run.history)
    );
}

/// The control for the ESCALATE above: the same shape with B reading the
/// parent through the SSI read path loses one of the two.
#[test]
fn g2_live_ser_plain_parent_read_aborts_one() {
    use super::runner::Rig;
    let rig = Rig::new(5);
    rig.setup();
    let mut a = rig.session(1, Level::Serializable);
    let mut b = rig.session(2, Level::Serializable);
    assert!(!a.update(14).expect("A reads c0"));
    assert!(a.update(12).expect("A updates p0"));
    b.read(12).expect("B reads p0");
    b.insert(14).expect("B inserts c0");
    let ra = a.commit();
    let rb = b.commit();
    assert!(
        ra.is_err() != rb.is_err(),
        "exactly one of the pair aborts: {ra:?} {rb:?}"
    );
    let run = rig.finish();
    let verdict = check(&run.history, Level::Serializable);
    assert!(verdict.serializable(), "{}", verdict.report(&run.history));
}

/// The full-mix SERIALIZABLE soak (FK parents written too): fails with a
/// cycle through an FK parent read until the ESCALATE above is fixed.
#[test]
#[ignore = "ESCALATE: fk_check_child drops the reader-side rw edge (NoSsi): SER write skew"]
fn g2_soak_serializable_fk_parent_writes() {
    for seed in [0x4732_0013, 0x4732_0014, 0x4732_0015, 0x4732_0016] {
        let (run, verdict) = run_seed(Level::Serializable, seed, PROGS_PER_THREAD, Mix::STANDARD);
        summary(Level::Serializable, seed, &verdict);
        assert_sound(Level::Serializable, seed, &run, &verdict);
    }
}
