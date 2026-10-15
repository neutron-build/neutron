//! C-T6c1a's soak, carrying C-T6c1b's checks and C-T6d2's large-value
//! surface: seeded multi-session histories over `nucleus-catalog::Db` on
//! `Core<MemKv>` — 4 DML sessions at RC/RR/SER over `t(pk int8 PK, a
//! int8, b text, big text)` (pk ∈ 0..8; `big` takes 100/3000/20000-byte
//! values and NULLs, C-T6d2 §5) and `u(a int8, b text)` with its hidden
//! rowid, plus a DDL session issuing ADD COLUMN, DROP COLUMN, SET/DROP
//! NOT NULL, TRUNCATE, DROP COLUMN big and its re-add, each in its own
//! txn — checked against the sequential reference model (`tests/diff/`):
//! every read at its snapshot plus own writes (c1a), every write effect
//! re-derived at its commit position, duplicates refused, SET NOT NULL
//! outcomes validated, the committed SERIALIZABLE sub-history's
//! serialization graph acyclic (c1b), amcheck — I-TOAST included (d2) —
//! at three mid-run snapshots and at the end, and final-state scans.
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
// This card's test-side checker extensions (the write half of the
// TRUNCATE-window rule, the per-level read split).
mod t6c_support;

use diff::{check, gen_ddl, gen_prog, run_workload, Config, PDdl, Run, Verdict};

/// The soak's three fixed seeds.
const SEEDS: [u64; 3] = [0x7c1a_0001, 0x7c1a_0002, 0x7c1a_0003];
/// The long variant's fifty.
const LONG_SEEDS: u64 = 50;
const LONG_BASE: u64 = 0x7c1a_1000;
/// A pk outside the workload's 0..8 domain, for the phantom final row
/// the can-fail test plants when a late truncate left `t` empty.
const PHANTOM_PK: i64 = 77;

fn run_seed(seed: u64) -> (Run, Verdict) {
    let cfg = Config::new(seed);
    let t0 = std::time::Instant::now();
    let run = run_workload(&cfg);
    let t1 = std::time::Instant::now();
    // The merged checker's every check, plus this card's write-side
    // TRUNCATE-window pass (c1b FU-B).
    let verdict = t6c_support::check_with_write_windows(&run);
    // Reported, never asserted: no wall-clock verdicts.
    println!(
        "t6c soak seed {seed:#x}: run {:?}, check {:?}, leaked {}",
        t1 - t0,
        t1.elapsed(),
        run.leaked_intents
    );
    (run, verdict)
}

fn summary(seed: u64, v: &Verdict, run: &Run) {
    let s = &v.stats;
    let lv = t6c_support::read_levels(&run.history);
    println!(
        "t6c soak seed {seed:#x}: {} txns ({} committed, {} failed, {} retried); \
         {} reads checked (RC {}, RR {}, SER {}); {} writes; {} DDL ({} truncates committed); \
         {} amcheck points; c1b: {} writes checked ({} effects recomputed, \
         {} RC carve-outs), {} SET NOT NULL verified ({} skipped), \
         {} SER txns / {} reads / {} wr + {} rw edges; c1d2: {} big writes \
         (lens {}/{}/{}), {} big drops / {} adds; c1e3: {} builds ok / {} failed, \
         {} resumes ({} no-op), {} drops, {} index scans ({} served, {} refused, \
         {} absent, {} no-target), sights DO {}/WO {}; violations: {}",
        s.txns,
        s.committed,
        s.failed,
        s.retryable_failures,
        s.reads_checked,
        lv.rc,
        lv.rr,
        lv.ser,
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
        s.big_writes,
        s.big_lens[0],
        s.big_lens[1],
        s.big_lens[2],
        s.big_drops,
        s.big_adds,
        s.idx_builds_ok,
        s.idx_builds_failed,
        s.idx_resumes,
        s.idx_resumes_noop,
        s.idx_drops,
        s.idx_scans,
        s.idx_scans_served,
        s.idx_scans_refused,
        s.idx_scans_absent,
        s.idx_scans_noop,
        s.idx_sights_delete_only,
        s.idx_sights_write_only,
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
    // C-T6d2's coverage bars are taken over the three seeds together:
    // how many of a run's SetBig ops land depends on scheduling (a pk
    // the truncate-heavy mix leaves absent, a 40001 that exhausts the
    // retry budget), so per-seed floors would flake on interleavings,
    // while the aggregate floor is many times the bar.
    let mut big = diff::Verdict::default().stats;
    for seed in SEEDS {
        let (run, verdict) = run_seed(seed);
        summary(seed, &verdict, &run);
        assert_clean(seed, &verdict);
        let s = &verdict.stats;
        // The workload actually exercised the surfaces under test: reads
        // were checked, DDL and truncates committed, amcheck ran, and
        // commits happened at every level (the mix generates all three).
        assert!(s.reads_checked > 100, "too few reads checked: {s:?}");
        // The level split (c1a FU-5 / c1b FU-C): successful reads must
        // happen at every level — an over-refusing engine that 40001'd
        // all RR/SER reads starves two of the three counters (refused
        // txns leave no effects, so nothing else would notice).
        let lv = t6c_support::read_levels(&run.history);
        assert!(lv.rc > 50, "too few RC reads checked: {lv:?} of {s:?}");
        assert!(lv.rr > 50, "too few RR reads checked: {lv:?} of {s:?}");
        assert!(lv.ser > 50, "too few SER reads checked: {lv:?} of {s:?}");
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
        // The aggregate of the large-value surface (Stats is Copy; the
        // per-field sums are what the bars below run on).
        big.big_writes += s.big_writes;
        for (agg, one) in big.big_lens.iter_mut().zip(s.big_lens) {
            *agg += one;
        }
        big.big_drops += s.big_drops;
        big.big_adds += s.big_adds;
        // The same for C-T6e3's online-build surface (the c1d2 pattern:
        // per-seed floors would flake on interleavings — a build's fate
        // depends on which truncate or drop raced it — while the
        // aggregate floor is many times the bar).
        big.idx_builds_ok += s.idx_builds_ok;
        big.idx_builds_failed += s.idx_builds_failed;
        big.idx_resumes += s.idx_resumes;
        big.idx_drops += s.idx_drops;
        big.idx_scans_served += s.idx_scans_served;
        big.idx_scans_refused += s.idx_scans_refused;
        big.idx_sights_delete_only += s.idx_sights_delete_only;
        big.idx_sights_write_only += s.idx_sights_write_only;
        // c1b FU-D: the SET NOT NULL surface stays exercised — a
        // workload or engine change that quietly stopped producing
        // successful SET NOT NULLs (or witnessed 23502s) would zero
        // this without failing anything else.
        assert!(s.snn_checked > 0, "no SET NOT NULL verified: {s:?}");
    }
    // C-T6d2's surface: large-value writes committed at every length
    // class the card names (inline / one chunk / three chunks), and the
    // drop/re-add cycle of the column they land on.
    assert!(big.big_writes > 12, "too few big writes: {big:?}");
    assert!(
        big.big_lens.iter().all(|&n| n > 0),
        "every big length class (100/3000/20000) must commit: {big:?}"
    );
    assert!(big.big_drops > 0, "no committed DropBig: {big:?}");
    assert!(big.big_adds > 0, "no committed AddBig: {big:?}");
    // C-T6e3's surface: builds published and failed (the seeded
    // sabotage's 55000s), unfinished ones were resumed and dropped, and
    // the DML scans both served through a published index and were
    // refused by the ladder's mid-states — an engine that never
    // published, or one that never refused, starves one of each pair
    // (the served side is also the rows-equality oracle's only bite).
    assert!(
        big.idx_builds_ok > 3,
        "too few published online builds: {big:?}"
    );
    assert!(
        big.idx_builds_failed > 0,
        "no failed online build (the seeded sabotage never bit): {big:?}"
    );
    assert!(big.idx_resumes > 0, "no resume ran on a target: {big:?}");
    assert!(big.idx_drops > 0, "no committed DROP INDEX: {big:?}");
    assert!(
        big.idx_scans_served > 10,
        "too few served index scans: {big:?}"
    );
    assert!(
        big.idx_scans_refused > 0,
        "no justified index-scan refusal (the ladder's mid-states were \
         never seen mid-flight): {big:?}"
    );
    // The bracketed sights saw the ladder's first two states somewhere
    // in the run — the states amcheck's I-IDX-DO/I-IDX-WO arms check.
    assert!(
        big.idx_sights_delete_only > 0,
        "no DeleteOnly sight: {big:?}"
    );
    assert!(big.idx_sights_write_only > 0, "no WriteOnly sight: {big:?}");
}

/// The long variant: 50 seeds, `#[ignore]`d (the card's item 5).
#[test]
#[ignore = "long: 50 seeds"]
fn t6c_soak_long() {
    for i in 0..LONG_SEEDS {
        let seed = LONG_BASE + i;
        let (run, verdict) = run_seed(seed);
        summary(seed, &verdict, &run);
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
            PDdl::DropBig => {
                kinds.insert("dropbig");
            }
            PDdl::AddBig => {
                kinds.insert("addbig");
            }
            // C-T6e3: both CONCURRENTLY modes of the online build, and
            // its cleanup pair.
            PDdl::CreateIndex(true) => {
                kinds.insert("cidx_c");
            }
            PDdl::CreateIndex(false) => {
                kinds.insert("cidx_p");
            }
            PDdl::DropIndex => {
                kinds.insert("didx");
            }
            PDdl::ResumeBuild => {
                kinds.insert("rsmb");
            }
        }
    }
    assert_eq!(kinds.len(), 11, "every DDL kind appears: {kinds:?}");
}

// ----- the checker can fail ---------------------------------------------------

/// The checker is not vacuous: a small clean run, corrupted one leg at
/// a time, must be flagged — every check leg can fail (c1a FU-1). The
/// uncorrupted run must itself be clean — the same assertion the soak
/// makes, at half the size (large enough that its interleavings
/// reliably produce a refused RR/SER read with a later foreign
/// truncate, the shape leg (d) corrupts). The corruptions:
///
/// - a recorded read (a value the engine never returned);
/// - a final-scan row of `run.final_t` (a phantom row when a late
///   truncate left `t` empty) — the final scans are compared against
///   the model, never against themselves;
/// - one amcheck report's `violations` — a dirty report reaches the
///   verdict, it is never silently dropped;
/// - a refused RR/SER read rewritten as a successful one that answered
///   from the storage of a foreign truncate committed after its
///   snapshot — exactly the shape §4 obliges the engine to 40001 (the
///   record is rebuilt whole: a statement refused in its `open_table`
///   never recorded its parameters, so its detail is `Pending`).
#[test]
fn t6c_checker_flags_a_corrupted_read() {
    let mut cfg = Config::new(0x7c1a_00f1);
    cfg.threads = 4;
    // The soak's size (C-T6e3): the index DDL the stream now carries
    // slows it enough that a smaller mini run's refused-read-plus-later
    // truncate shape no longer lands reliably.
    cfg.progs = 200;
    cfg.ddl_txns = 200;
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

    // (a) The final-state leg: one row of the run's own final scan
    // corrupted (a phantom row added instead when a late truncate left
    // `t` empty at the end — `u`'s scan would do as well, but `t` is
    // the leg the soak's stats lean on).
    let mut bad_final = run.clone();
    match bad_final
        .final_t
        .1
        .iter_mut()
        .find_map(|(_, row)| row.iter_mut().find(|(_, v)| v.is_some()))
    {
        Some((_, val)) => *val = Some(nucleus_codec::Value::Text("\u{0}CORRUPT".into())),
        None => bad_final.final_t.1.push((
            PHANTOM_PK,
            vec![(1, Some(nucleus_codec::Value::Int8(PHANTOM_PK)))],
        )),
    }
    let verdict = check(&bad_final);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("final state of t")),
        "the corrupted final scan went unnoticed: {}",
        verdict.report()
    );

    // (b) The amcheck leg: one report's violations, corrupted.
    let mut bad_amcheck = run.clone();
    assert!(
        !bad_amcheck.amcheck.is_empty(),
        "the mini run recorded no amcheck point"
    );
    bad_amcheck.amcheck[0]
        .report
        .violations
        .push("\u{0}CORRUPT".to_string());
    let verdict = check(&bad_amcheck);
    assert!(
        verdict.violations.iter().any(|v| v.contains("amcheck")),
        "the corrupted amcheck report went unnoticed: {}",
        verdict.report()
    );

    // (c) The TRUNCATE-window leg: a refused RR/SER read rewritten to a
    // successful one whose handle read the new storage of a foreign
    // truncate committed after its snapshot (`snap < ts` and the
    // session difference are preserved by the rewrite — only the
    // outcome, the snapshot and the storage id lie; C-T6e3 also moves
    // the snapshot itself under the chosen truncate, so the leg no
    // longer depends on the organic window landing — the index DDL the
    // stream now carries slowed it enough that a smaller mini run's
    // shape no longer did).
    let truncates = t6c_support::committed_truncates(&run.history);
    let mut corrupted = run.history.clone();
    let mut rewritten = false;
    'find: for txn in corrupted.txns.iter_mut() {
        if !matches!(
            txn.level,
            diff::Level::RepeatableRead | diff::Level::Serializable
        ) {
            continue;
        }
        for op in txn.ops.iter_mut() {
            // A refused read statement failed in its `open_table`, before
            // its parameters were recorded (`Pending`), so the rewrite
            // rebuilds the whole record as the successful read it lies
            // about being — kind and outcome first, then the snapshot
            // and the storage id.
            let refused_read = matches!(op.kind, diff::OpKind::Get | diff::OpKind::ScanT)
                && matches!(&op.out, diff::OpOut::Err(code) if code == "40001");
            if !refused_read {
                continue;
            }
            let Some((ts, new_sid, _)) = truncates
                .iter()
                .find(|(ts, _, session)| *ts > 1 && *session != txn.session)
                .copied()
            else {
                continue;
            };
            match op.kind {
                diff::OpKind::Get => {
                    op.detail = diff::OpDetail::Get { pk: 0 };
                    op.out = diff::OpOut::Row(None);
                }
                _ => {
                    op.detail = diff::OpDetail::ScanT;
                    op.out = diff::OpOut::RowsT(Vec::new());
                }
            }
            op.snap = ts - 1;
            op.sid = Some(new_sid);
            rewritten = true;
            break 'find;
        }
    }
    assert!(
        rewritten,
        "the mini run recorded no refused RR/SER read with a later foreign truncate"
    );
    let bad = diff::Run {
        history: corrupted,
        ..run.clone()
    };
    let verdict = check(&bad);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("40001 was required")),
        "the rewritten in-window read went unnoticed: {}",
        verdict.report()
    );
}
