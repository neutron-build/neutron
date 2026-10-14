//! C-T6c1a's soak, carrying C-T6c1b's checks: seeded multi-session
//! histories over `nucleus-catalog::Db` on `Core<MemKv>` — 4 DML sessions
//! at RC/RR/SER over `t(pk int8 PK, a int8, b text)` (pk ∈ 0..8) and
//! `u(a int8, b text)` with its hidden rowid, plus a DDL session issuing
//! ADD COLUMN, DROP COLUMN, SET/DROP NOT NULL and TRUNCATE each in its
//! own txn — checked against the sequential reference model
//! (`tests/diff/`): every read at its snapshot plus own writes (c1a),
//! every write effect re-derived at its commit position, duplicates
//! refused, SET NOT NULL outcomes validated, the committed SERIALIZABLE
//! sub-history's serialization graph acyclic (c1b), amcheck at three
//! mid-run snapshots and at the end, and final-state scans.
//!
//! The size is an iteration count (3 fixed seeds × 200 txns per session),
//! never a wall-clock bound; timings are printed, not asserted. The
//! `#[ignore]`d long variant runs 50 seeds.
//!
//! Mutation evidence (binding, card item 5): the four card-named mutants
//! are each caught —
//!
//! - **E1** — `decode_row` ignoring `missing` (nucleus-codec): every
//!   read of a row older than an ADD COLUMN with a default renders NULL
//!   where the model renders the default → a Get/ScanT mismatch. Caught
//!   by this soak failing in 5 of 5 runs.
//! - **E2** — `open_table` skipping the RR storage-id check
//!   (nucleus-catalog): a successful RR/SER read over the post-TRUNCATE
//!   storage returns rows the model at `S` does not have (or misses the
//!   ones it does) → a mismatch, plus the explicit TRUNCATE-window
//!   violation. Caught by this soak failing in 5 of 5 runs.
//! - **E3** — `catalog_alter_rel_def` locking RowExclusive instead of
//!   AccessExclusive (nucleus-txn): a writer no longer blocks behind an
//!   in-flight ALTER and encodes at the stale definition. Caught
//!   deterministically by `t6c_regress`'s parked-DDL schedule.
//! - **E4** — writes encoding at `version - 1` (the row header's
//!   schema_version): caught deterministically by `t6c_regress`'s
//!   single-thread ADD COLUMN → insert → decode check.

mod diff;
// diff/crash.rs (pulled in by diff/mod.rs) drives the t6b crash rig.
mod t6b_support;

use diff::{check, gen_ddl, gen_prog, run_workload, Config, PDdl, Run, Verdict};

/// The soak's three fixed seeds.
const SEEDS: [u64; 3] = [0x7c1a_0001, 0x7c1a_0002, 0x7c1a_0003];
/// The long variant's fifty.
const LONG_SEEDS: u64 = 50;
const LONG_BASE: u64 = 0x7c1a_1000;

fn run_seed(seed: u64) -> (Run, Verdict) {
    let cfg = Config::new(seed);
    let t0 = std::time::Instant::now();
    let run = run_workload(&cfg);
    let t1 = std::time::Instant::now();
    let verdict = check(&run);
    // Reported, never asserted: no wall-clock verdicts.
    println!(
        "t6c soak seed {seed:#x}: run {:?}, check {:?}, leaked {}",
        t1 - t0,
        t1.elapsed(),
        run.leaked_intents
    );
    (run, verdict)
}

fn summary(seed: u64, v: &Verdict) {
    let s = &v.stats;
    println!(
        "t6c soak seed {seed:#x}: {} txns ({} committed, {} failed, {} retried); \
         {} reads checked; {} writes; {} DDL ({} truncates committed); \
         {} amcheck points; c1b: {} writes checked ({} effects recomputed, \
         {} RC carve-outs), {} SET NOT NULL verified ({} skipped), \
         {} SER txns / {} reads / {} wr + {} rw edges; violations: {}",
        s.txns,
        s.committed,
        s.failed,
        s.retryable_failures,
        s.reads_checked,
        s.writes,
        s.ddl,
        s.truncates_committed,
        s.amcheck_points,
        s.writes_checked,
        s.effects_recomputed,
        s.rc_window_carveouts,
        s.snn_checked,
        s.snn_23502_skipped,
        s.ser_txns,
        s.ser_reads,
        s.ser_wr_edges,
        s.ser_rw_edges,
        v.violations.len()
    );
}

/// Fails with the verdict report when the run is not clean.
fn assert_clean(seed: u64, verdict: &Verdict) {
    if !verdict.is_clean() {
        panic!(
            "t6c soak seed {seed:#x} is not clean:\n{}",
            verdict.report()
        );
    }
}

#[test]
fn t6c_soak() {
    for seed in SEEDS {
        let (run, verdict) = run_seed(seed);
        summary(seed, &verdict);
        assert_clean(seed, &verdict);
        let s = &verdict.stats;
        // The workload actually exercised the surfaces under test: reads
        // were checked, DDL and truncates committed, amcheck ran, and
        // commits happened at every level (the mix generates all three).
        assert!(s.reads_checked > 100, "too few reads checked: {s:?}");
        assert!(s.truncates_committed > 3, "too few truncates: {s:?}");
        assert!(s.ddl > 30, "too little DDL: {s:?}");
        assert!(
            s.amcheck_points == 8,
            "3 mid-run points × 2 tables + the end: {s:?}"
        );
        assert!(s.committed > 200, "too few commits: {s:?}");
        assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);
        // C-T6c1b's surfaces were exercised too: the write oracle ran,
        // and the serialization graph had SER txns to bite on (the mix
        // makes ~a third of the 800 programs SERIALIZABLE).
        assert!(s.writes_checked > 100, "too few writes checked: {s:?}");
        assert!(s.ser_txns > 5, "too few committed SER txns: {s:?}");
    }
}

/// The long variant: 50 seeds, `#[ignore]`d (the card's item 5).
#[test]
#[ignore = "long: 50 seeds"]
fn t6c_soak_long() {
    for i in 0..LONG_SEEDS {
        let seed = LONG_BASE + i;
        let (run, verdict) = run_seed(seed);
        summary(seed, &verdict);
        assert_clean(seed, &verdict);
        assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);
    }
}

// ----- determinism ------------------------------------------------------------

/// Programs are a pure function of `(seed, session, index)`: same triple,
/// same program; a different seed gives different programs.
#[test]
fn t6c_programs_are_a_pure_function_of_the_seed() {
    for session in 1..=5u16 {
        for idx in 0..60 {
            let a = gen_prog(0x1234, session, idx);
            let b = gen_prog(0x1234, session, idx);
            assert_eq!(a, b, "session {session} prog {idx}");
            let a = gen_ddl(0x1234, idx);
            let b = gen_ddl(0x1234, idx);
            assert_eq!(a, b, "ddl {idx}");
        }
    }
    let differs = (0..60).any(|i| gen_prog(0x1234, 1, i) != gen_prog(0x1235, 1, i));
    assert!(differs, "a different seed must give different programs");
    let ddl_differs = (0..60).any(|i| gen_ddl(0x1234, i) != gen_ddl(0x1235, i));
    assert!(ddl_differs, "a different seed must give different DDL");
    // The DDL mix actually contains every kind (each is issuable).
    let mut kinds = std::collections::BTreeSet::new();
    for i in 0..400 {
        match gen_ddl(0x1234, i) {
            PDdl::AddColumn(_) => {
                kinds.insert("add");
            }
            PDdl::DropColumn => {
                kinds.insert("drop");
            }
            PDdl::SetNotNull => {
                kinds.insert("snn");
            }
            PDdl::DropNotNull => {
                kinds.insert("dnn");
            }
            PDdl::Truncate => {
                kinds.insert("trunc");
            }
        }
    }
    assert_eq!(kinds.len(), 5, "every DDL kind appears: {kinds:?}");
}

// ----- the checker can fail ---------------------------------------------------

/// The checker is not vacuous: a small clean run whose history has one
/// recorded read corrupted (a value the engine never returned) must be
/// flagged. The uncorrupted run must itself be clean — the same
/// assertion the soak makes, at a tenth of the size.
#[test]
fn t6c_checker_flags_a_corrupted_read() {
    let mut cfg = Config::new(0x7c1a_00f1);
    cfg.threads = 3;
    cfg.progs = 30;
    cfg.ddl_txns = 30;
    let run = run_workload(&cfg);
    let verdict = check(&run);
    assert!(verdict.is_clean(), "{}", verdict.report());

    // Corrupt one successful read: the first Get that returned a row, or
    // failing that the first non-empty ScanT/ScanU, has one of its
    // values changed to something no write derived.
    let mut corrupted = run.history.clone();
    let mut corrupted_one = false;
    'outer: for txn in corrupted.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            let rows: &mut Vec<(u16, Option<nucleus_codec::Value>)> = match &mut op.out {
                diff::OpOut::Row(Some(row)) => row,
                diff::OpOut::RowsT(rows) if !rows.is_empty() => &mut rows[0].1,
                diff::OpOut::RowsU(rows) if !rows.is_empty() => &mut rows[0].1,
                _ => continue,
            };
            if let Some((_, val)) = rows.iter_mut().find(|(_, v)| v.is_some()) {
                *val = Some(nucleus_codec::Value::Text("\u{0}CORRUPT".into()));
                corrupted_one = true;
                break 'outer;
            }
        }
    }
    assert!(corrupted_one, "the mini run recorded no row-bearing read");
    let bad = diff::Run {
        history: corrupted,
        ..run.clone()
    };
    let verdict = check(&bad);
    assert!(
        !verdict.is_clean(),
        "the corrupted read went unnoticed: {}",
        verdict.report()
    );
}
