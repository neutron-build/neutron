//! G2 soak: 8 threads over the 16-slot key space at each of RC, RR and SER,
//! with deterministic seeds. The size is an iteration count (programs per
//! thread), never a wall-clock bound. Every level runs the full
//! `Mix::STANDARD` workload — upserts wherever they fall, writes on the FK
//! parents — now that the engine fixes it existed to provoke are merged;
//! the SER levels retry more (the mix's strictness is the point), so a SER
//! seed costs on the order of ten seconds in a debug build.
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
//! deliberately broken store (an RR claim with per-statement *read*
//! snapshots; a hand-scheduled history of the same defect sits next to it)
//! is flagged, and an RC run judged as if it were SERIALIZABLE shows cycles.

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
        let (run, verdict) = run_seed(Level::Serializable, seed, PROGS_PER_THREAD, Mix::STANDARD);
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

/// An RR claim on a store that snapshots its reads per statement: the soak
/// notices. The deterministic half of the check is the hand-scheduled
/// history below; this one supplies the real-threads end-to-end half and
/// must not flake: the `PerStmtReadSnapshot` defect leaves the writes on
/// the txn snapshot (no abort churn to close the windows), and 240
/// programs per thread on 8 real threads leave plenty of them — a
/// concurrent commit landing between two read statements of one RR txn.
#[test]
fn g2_soak_broken_store_is_caught() {
    let mut cfg = Config::new(0x4732_00ff, Level::RepeatableRead);
    cfg.progs = 240;
    cfg.bug = Bug::PerStmtReadSnapshot;
    let run = run_workload(&cfg);
    let verdict = check(&run.history, Level::RepeatableRead);
    assert!(
        !verdict.violations().is_empty(),
        "the broken store went unnoticed: {:?}",
        verdict.stats
    );
}

/// The deterministic half of the broken-store check: a hand-scheduled
/// history of exactly what a per-statement-read-snapshot store produces —
/// an RR txn reads k00 at the setup value, D commits a new k00, the txn's
/// next read sees it — and the checker flags the non-repeatable read.
/// This cannot flake, whatever the thread scheduler does to the soak
/// above. (The same history also reads skew — T both anti-depends on D
/// and read its value — which the checker names as a same-key rw+wr
/// two-cycle, LostUpdate.)
#[test]
fn g2_broken_store_history_is_caught() {
    use super::history::{Hb, Obs, Val};
    let iv = |k: u8| Val::new(1, u16::from(k) + 1); // the setup value of k
    let mut hb = Hb::new();
    let su = hb.begin(1, Level::ReadCommitted);
    for &k in &[0u8, 2] {
        hb.insert(su, k, iv(k));
    }
    hb.commit(su, 1);
    let t = hb.begin(2, Level::RepeatableRead);
    let d = hb.begin(3, Level::RepeatableRead);
    hb.read(t, 0, Obs::Val(iv(0)));
    hb.update(d, 0, iv(0), Val::new(3, 1));
    hb.commit(d, 2);
    // The broken store's per-statement read snapshot: T sees D's commit
    // mid-txn and reads the new k00.
    hb.read(t, 0, Obs::Val(Val::new(3, 1)));
    hb.commit(t, 0);
    let h = hb.finish();
    let verdict = check(&h, Level::RepeatableRead);
    assert!(
        verdict.violates(Kind::NonRepeatableRead),
        "the checker stayed silent about the broken store:\n{}{}",
        verdict.report(&h),
        h.trace()
    );
    assert!(verdict.violates(Kind::LostUpdate), "{}", verdict.report(&h));
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

// ----- pins of the merged engine fixes -----------------------------------------
//
// G2 once shipped these as `#[ignore = "ESCALATE: .."]` repros of two
// engine bugs and two workload restrictions that kept the soaks green
// around them. Both bugs are fixed on the base (C-OC-LIVE: an ON CONFLICT
// restart cycle ends in exactly one 40P01; C-FK-SSI: the SER FK child
// check records its reader-side rw edges), so the repros are now plain
// assertions of the fixed behaviour and the restrictions are gone.

/// The pin of the C-OC-LIVE fix (the former ESCALATE-1 repro): two
/// sessions each hold a row and then upsert (`ON CONFLICT (pk) DO UPDATE`)
/// the row the other holds. With `rollback_to` bumping the wake generation
/// only when it released something (C-T0 5.5) and non-arbiter unique waits
/// waiting in place (5.3.1), the two restarting upserts leave a stable
/// wait-for cycle and the deadlock check breaks it with exactly one 40P01
/// at `deadlock_timeout`. Each repeat is asserted to finish in 5 s, and a
/// detached watchdog aborts the process at 10 s: a livelock regression
/// never returns from the joins, so the assertion alone would leave the
/// test hanging (the C-OC-LIVE revert did exactly that in the G2r3 gate).
/// Ten repeats, each on a fresh rig.
#[test]
fn g2_upsert_cycle_ends_in_one_deadlock() {
    use super::history::SqlErr;
    use super::runner::Rig;
    use std::sync::Barrier;
    use std::time::{Duration, Instant};

    const REPEATS: usize = 10;
    let mut total_deadlocks = 0usize;
    for _ in 0..REPEATS {
        let rig = Rig::new(5);
        rig.setup();
        let barrier = Barrier::new(2);
        let go = |id: u16, first: u8, second: u8| -> Result<(), SqlErr> {
            let mut s = rig.session(id, Level::ReadCommitted);
            s.update(first)?;
            barrier.wait();
            s.upsert(second)?;
            s.commit()
        };
        // The watchdog holds the receiver; dropping the sender after the
        // joins disconnects it, so only a repeat that never returns aborts.
        let (done, watch) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            if let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                watch.recv_timeout(Duration::from_secs(10))
            {
                eprintln!("g2: the upsert cycle did not resolve in 10 s: livelock (no 40P01)");
                std::process::abort();
            }
        });
        let t0 = Instant::now();
        let (r1, r2) = std::thread::scope(|sc| {
            let a = sc.spawn(|| go(1, 0, 2));
            let b = sc.spawn(|| go(2, 2, 0));
            (a.join().expect("t1"), b.join().expect("t2"))
        });
        drop(done);
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "the upsert cycle resolved in {:?} (deadlock_timeout is 5 ms)",
            t0.elapsed()
        );
        let deadlocks = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(SqlErr::Deadlock)))
            .count();
        assert_eq!(deadlocks, 1, "exactly one 40P01: {r1:?} {r2:?}");
        assert_eq!(
            [&r1, &r2].iter().filter(|r| r.is_ok()).count(),
            1,
            "the other side completes: {r1:?} {r2:?}"
        );
        total_deadlocks += deadlocks;
        // The history agrees: the 40P01 sat on a potential wait-for cycle,
        // nothing leaked, and the surviving txn left no anomaly.
        let run = rig.finish();
        let verdict = check(&run.history, Level::ReadCommitted);
        assert_eq!(verdict.stats.e40p01, 1, "{}", verdict.report(&run.history));
        assert_eq!(verdict.stats.deadlocks_explained, 1);
        assert!(
            verdict.violations().is_empty(),
            "{}{}",
            verdict.report(&run.history),
            run.leak_report
        );
        assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);
    }
    assert_eq!(total_deadlocks, REPEATS, "every repeat broke its cycle");
}

/// The pin of the C-FK-SSI fix (the former ESCALATE-2 repro): the
/// SERIALIZABLE FK child check registers the SIREAD on the parent and
/// reads it through SSI, so the reader-side rw edge to a concurrent parent
/// writer survives and the write skew (A reads c0 absent and updates p0;
/// B inserts c0 and so reads p0) closes into a cycle that SSI breaks by
/// aborting exactly one of the pair. The control below proves the plain
/// parent-read path already did.
#[test]
fn g2_fk_parent_read_records_rw_edge() {
    use super::history::SqlErr;
    use super::runner::Rig;

    let rig = Rig::new(5);
    rig.setup();
    let mut a = rig.session(1, Level::Serializable);
    let mut b = rig.session(2, Level::Serializable);
    assert!(!a.update(14).expect("A reads c0"), "c0 does not exist yet");
    assert!(a.update(12).expect("A updates p0"));
    b.fk_insert(14).expect("B inserts c0 under p0");
    let ra = a.commit();
    let rb = b.commit();
    assert!(
        ra.is_err() != rb.is_err(),
        "exactly one of the pair aborts: {ra:?} {rb:?}"
    );
    for r in [ra, rb] {
        if let Err(e) = r {
            assert_eq!(e, SqlErr::SerFailure, "the abort is a 40001: {e:?}");
        }
    }
    let run = rig.finish();
    let verdict = check(&run.history, Level::Serializable);
    assert!(
        verdict.serializable(),
        "SERIALIZABLE let a non-serializable pair through:\n{}",
        verdict.report(&run.history)
    );
}

/// The control for the pin above: the same shape with B reading the
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

/// The full-mix SERIALIZABLE soak (FK parents written too) over four more
/// seeds: with the FK child check recording its reader-side rw edges
/// (C-FK-SSI, merged) the strict workload — the one `Mix::STANDARD` always
/// ran at RC and RR — holds at SER as well.
#[test]
fn g2_soak_serializable_fk_parent_writes() {
    for seed in [0x4732_0013, 0x4732_0014, 0x4732_0015, 0x4732_0016] {
        let (run, verdict) = run_seed(Level::Serializable, seed, PROGS_PER_THREAD, Mix::STANDARD);
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
