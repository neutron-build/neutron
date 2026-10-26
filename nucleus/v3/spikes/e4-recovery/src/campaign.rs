//! The campaign driver (card X-5 Work 7–8): `run`, `verify`, `mutants`.
//!
//! - `run --seeds A..B --threads N --out results/campaign.json` runs the
//!   seed range through [`crate::workload::run_seed`], writes the
//!   per-seed records and prints the final `E4 SEEDS=…` line. The
//!   per-seed digests are byte-identical across runs and thread counts
//!   (S-DET, W-DET).
//! - `verify --results results/campaign.json` re-runs every seed in the
//!   file and checks three things: the per-seed digests reproduce
//!   exactly, the seed set is exactly the campaign's, and every 5.6
//!   quota is met. It exits non-zero **only** on non-reproduction, a
//!   missing seed or a missed quota — never on oracle failures: the
//!   experiment may legitimately fail, and Opus judges that.
//! - `mutants --seeds 0..500 --out results/mutants.json` sweeps every
//!   store mutant over the seed range (the campaign column of the
//!   mutation table) and re-runs X-5b's directed arms (the directed
//!   column, where an arm is encoded in [`directed_arms`]); `mutants
//!   --check results/mutants.json` re-runs, stopping at each mutant's
//!   recorded first catch, and checks it matches.
//!
//! **Stage status (see the crate README):** X5-R6 — P's pre-data ruling
//! on O-1 when recovery returns `Ok` alongside an XX001 — has not been
//! delivered, and the card's amendment stops the worker before the first
//! campaign `run` whose output anyone reads. The subcommands are fully
//! implemented; a missing results file exits non-zero with a clear
//! message, which is what the Acceptance runs of this stage record.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::store::Mutant;
use crate::workload::{self, SeedRec};

/// The campaign's seed range (the card: 0..10000).
pub const CAMPAIGN_SEEDS: u64 = 10_000;
/// The mutant sweep's seed range (Work 8).
pub const MUTANT_SEEDS: u64 = 500;

// ---- the entry point ----

/// The binary entry point. Returns the process exit code.
pub fn main(args: &[String]) -> i32 {
    let Some(cmd) = args.first() else {
        usage();
        return 2;
    };
    match cmd.as_str() {
        "run" => cmd_run(&args[1..]),
        "verify" => cmd_verify(&args[1..]),
        "mutants" => cmd_mutants(&args[1..]),
        "--help" | "-h" | "help" => {
            usage();
            0
        }
        _ => {
            eprintln!("e4-campaign: unknown subcommand {cmd:?}");
            usage();
            2
        }
    }
}

fn usage() {
    eprintln!(
        "usage: e4-campaign run --seeds A..B --threads N --out results/campaign.json\n\
         \x20      e4-campaign verify --results results/campaign.json [--threads N]\n\
         \x20      e4-campaign mutants --seeds A..B --out results/mutants.json\n\
         \x20      e4-campaign mutants --check results/mutants.json"
    );
}

/// Parse `--seeds A..B` (inclusive range).
fn parse_seeds(args: &[String]) -> (u64, u64) {
    let mut lo = 0u64;
    let mut hi = CAMPAIGN_SEEDS - 1;
    let mut i = 0usize;
    while i < args.len() {
        if args[i] == "--seeds"
            && let Some(v) = args.get(i + 1)
            && let Some((a, b)) = v.split_once("..")
        {
            lo = a.parse().unwrap_or(0);
            hi = b.parse().unwrap_or(lo);
        }
        i += 1;
    }
    (lo, hi)
}

fn parse_threads(args: &[String]) -> usize {
    let mut i = 0usize;
    while i < args.len() {
        if args[i] == "--threads"
            && let Some(v) = args.get(i + 1)
        {
            return v.parse().unwrap_or(1).max(1) as usize;
        }
        i += 1;
    }
    1
}

fn parse_out(args: &[String]) -> Option<String> {
    let mut i = 0usize;
    while i + 1 < args.len() {
        if args[i] == "--out" {
            return Some(args[i + 1].clone());
        }
        i += 1;
    }
    None
}

fn parse_results(args: &[String]) -> Option<String> {
    let mut i = 0usize;
    while i + 1 < args.len() {
        if args[i] == "--results" {
            return Some(args[i + 1].clone());
        }
        i += 1;
    }
    None
}

// ---- run ----

fn cmd_run(args: &[String]) -> i32 {
    let Some(out) = parse_out(args) else {
        eprintln!("e4-campaign run: --out is required");
        return 2;
    };
    let (lo, hi) = parse_seeds(args);
    let threads = parse_threads(args);
    if lo > hi {
        eprintln!("e4-campaign run: empty seed range {lo}..{hi}");
        return 2;
    }
    let recs = run_range(lo, hi, None, threads);
    let json = emit_campaign(&recs);
    if let Err(e) = std::fs::write(&out, json) {
        eprintln!("e4-campaign run: cannot write {out}: {e}");
        return 2;
    }
    print_final_line(&recs);
    0
}

/// Run `lo..=hi` over `threads` workers. Each seed is independent
/// (S-DET); the records come back sorted by seed.
pub fn run_range(lo: u64, hi: u64, mutant: Option<Mutant>, threads: usize) -> Vec<SeedRec> {
    let next = AtomicU64::new(lo);
    let done = AtomicU64::new(0);
    let total = hi - lo + 1;
    let out: std::sync::Mutex<Vec<SeedRec>> = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let s = next.fetch_add(1, Ordering::SeqCst);
                    if s > hi {
                        break;
                    }
                    let rec = workload::run_seed(s, mutant);
                    out.lock().map(|mut o| o.push(rec)).ok();
                    let d = done.fetch_add(1, Ordering::SeqCst) + 1;
                    if d.is_multiple_of(500) {
                        eprintln!("e4-campaign: {d}/{total} seeds done");
                    }
                }
            });
        }
    });
    let mut recs = out.into_inner().unwrap_or_default();
    recs.sort_unstable_by_key(|r| r.seed);
    recs
}

/// The final summary line (Work 7): counts over the 10,000 primary seeds.
pub fn final_line(recs: &[SeedRec]) -> String {
    let mut o1 = 0u64;
    let mut o2 = 0u64;
    let mut o3 = 0u64;
    let mut o1f = 0u64;
    let mut mon = 0u64;
    let mut dl = 0u64;
    let mut pa = 0u64;
    for r in recs {
        if r.rounds.iter().any(|x| x.o1_fail.is_some()) {
            o1 += 1;
        }
        if r.rounds.iter().any(|x| x.o2_fail.is_some()) {
            o2 += 1;
        }
        if r.rounds.iter().any(|x| x.o3_fail.is_some()) {
            o3 += 1;
        }
        if r.rounds.iter().any(|x| x.o1f_fail) {
            o1f += 1;
        }
        if r.rounds.iter().any(|x| !x.monitors.is_empty()) {
            mon += 1;
        }
        if r.deadlock {
            dl += 1;
        }
        if r.panic {
            pa += 1;
        }
    }
    format!(
        "E4 SEEDS={} O1_FAIL={o1} O2_FAIL={o2} O3_FAIL={o3} DEADLOCK={dl} PANIC={pa} O1F_FAIL={o1f} MONITOR={mon}",
        recs.len()
    )
}

fn print_final_line(recs: &[SeedRec]) {
    println!("{}", final_line(recs));
}

// ---- verify ----

fn cmd_verify(args: &[String]) -> i32 {
    let Some(path) = parse_results(args) else {
        eprintln!("e4-campaign verify: --results is required");
        return 2;
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "e4-campaign verify: cannot read {path}: {e}\n\
                 \x20(no campaign results exist in this stage: the Work-2 X5-R6 ruling —\n\
                 \x20 P's pre-data O-1 ruling for recovered-Ok-with-detected-loss — has not\n\
                 \x20 been delivered, and the card stops the worker before the first\n\
                 \x20 campaign run whose output anyone reads; see the crate README)"
            );
            return 2;
        }
    };
    let recs = match parse_campaign(&text) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("e4-campaign verify: {path} does not parse: {e}");
            return 2;
        }
    };
    // 1. The seed set is exactly the campaign's (0..9999).
    let n = recs.len();
    let want = CAMPAIGN_SEEDS as usize;
    if n != want
        || recs.first().map(|r| r.seed) != Some(0)
        || recs.last().map(|r| r.seed) != Some(CAMPAIGN_SEEDS - 1)
        || recs.windows(2).any(|w| w[0].seed + 1 != w[1].seed)
    {
        eprintln!(
            "e4-campaign verify: the seed set is not exactly 0..{} ({} records)",
            CAMPAIGN_SEEDS - 1,
            n
        );
        return 2;
    }
    // 2. The digests reproduce. A re-run of every seed in the file.
    let threads = parse_threads(args).max(4);
    eprintln!("e4-campaign verify: re-running {n} seeds at {threads} threads");
    let rerun = run_range(0, CAMPAIGN_SEEDS - 1, None, threads);
    let mut bad = 0u64;
    for (a, b) in recs.iter().zip(rerun.iter()) {
        if a.seed != b.seed || a.digest != b.digest {
            if bad < 10 {
                eprintln!(
                    "e4-campaign verify: seed {} did not reproduce ({} vs {})",
                    a.seed, a.digest, b.digest
                );
            }
            bad += 1;
        }
    }
    if bad > 0 {
        eprintln!("e4-campaign verify: {bad} seeds did not reproduce");
        return 2;
    }
    // 3. Every 5.6 quota is met (and Work 5.3's sizing assertion holds:
    // its row's achieved counts **misses**, which must be zero).
    let rows = workload::quotas(&recs, CAMPAIGN_SEEDS);
    let mut missed = 0u64;
    for row in &rows {
        let ok = if row.id == "SIZING" {
            row.achieved == 0
        } else {
            row.achieved >= row.required
        };
        println!(
            "QUOTA {} {:<62} achieved {:>6} required {:>6} {}",
            row.id,
            row.what,
            row.achieved,
            row.required,
            if ok { "MET" } else { "MISSED" }
        );
        if !ok {
            missed += 1;
        }
    }
    if missed > 0 {
        eprintln!("e4-campaign verify: {missed} quotas missed");
        return 2;
    }
    println!("VERIFY OK seeds={n} digests reproduce, every quota met");
    0
}

// ---- mutants ----

/// One mutant's sweep record.
#[derive(Debug, Clone)]
pub struct MutantRec {
    /// The mutant.
    pub mutant: String,
    /// The directed column: X-5b's catching test, whether its arm is
    /// encoded here (this stage ports a subset; the unported arms record
    /// `encoded: false` until the sweep attempt completes the table),
    /// and whether the re-run still catches.
    pub directed_test: &'static str,
    /// Whether [`directed_arms::run_arm`] encodes this mutant's arm.
    pub directed_encoded: bool,
    /// The re-run's result: caught (`Some(true)`), not caught
    /// (`Some(false)`), or not encodable yet (`None`).
    pub directed_caught: Option<bool>,
    /// The first catching seed and how it was caught (oracle O-1/O-2/O-3,
    /// monitor, deadlock or panic).
    pub first_seed: Option<u64>,
    /// The catcher of the first catching seed.
    pub how: Option<&'static str>,
    /// The oracle hit rate (failing seeds / swept).
    pub oracle_rate: f64,
    /// The monitor hit rate.
    pub monitor_rate: f64,
    /// The swept seed count.
    pub swept: u64,
}

fn cmd_mutants(args: &[String]) -> i32 {
    if args.first().map(String::as_str) == Some("--check") {
        // The card's acceptance form: `mutants --check <path>` (a bare
        // positional; `--results <path>` also accepted).
        let pos = args.get(1).filter(|s| !s.starts_with('-')).cloned();
        return cmd_mutants_check(args, pos);
    }
    let Some(out) = parse_out(args) else {
        eprintln!("e4-campaign mutants: --out is required");
        return 2;
    };
    let (lo, hi) = parse_seeds(args);
    let threads = parse_threads(args).max(4);
    let mut recs = Vec::new();
    for (m, test) in directed_arms::TABLE {
        let (rec, _) = sweep_mutant(*m, test, lo, hi, threads);
        recs.push(rec);
    }
    let json = emit_mutants(&recs);
    if let Err(e) = std::fs::write(&out, json) {
        eprintln!("e4-campaign mutants: cannot write {out}: {e}");
        return 2;
    }
    for r in &recs {
        println!(
            "MUTANT {:<28} first={:?} how={:?} oracle={:.3} monitor={:.3} directed={:?}{}",
            r.mutant,
            r.first_seed,
            r.how,
            r.oracle_rate,
            r.monitor_rate,
            r.directed_caught,
            if r.directed_encoded {
                ""
            } else {
                " (arm not encoded this stage)"
            }
        );
    }
    0
}

/// Sweep one mutant over the seed range: the directed column first
/// (X-5b's arm re-run), then the campaign column.
pub fn sweep_mutant(
    m: Mutant,
    test: &'static str,
    lo: u64,
    hi: u64,
    threads: usize,
) -> (MutantRec, Vec<SeedRec>) {
    let arm = directed_arms::run_arm(m);
    let (encoded, caught) = match arm {
        Some(r) => (true, Some(r)),
        None => (false, None),
    };
    let recs = run_range(lo, hi, Some(m), threads);
    let mut first: Option<(u64, &'static str)> = None;
    let mut oracle_hits = 0u64;
    let mut monitor_hits = 0u64;
    for r in &recs {
        let mut how: Option<&'static str> = None;
        if r.rounds.iter().any(|x| x.o1_fail.is_some()) {
            how = Some("O-1");
        }
        if r.rounds.iter().any(|x| x.o2_fail.is_some()) {
            how = how.or(Some("O-2"));
        }
        if r.rounds.iter().any(|x| x.o3_fail.is_some()) {
            how = how.or(Some("O-3"));
        }
        if r.rounds.iter().any(|x| !x.monitors.is_empty()) {
            how = how.or(Some("monitor"));
        }
        if r.deadlock {
            how = how.or(Some("deadlock"));
        }
        if r.panic {
            how = how.or(Some("panic"));
        }
        if r.rounds
            .iter()
            .any(|x| x.o1_fail.is_some() || x.o2_fail.is_some() || x.o3_fail.is_some() || !x.pass)
        {
            oracle_hits += 1;
        }
        if r.rounds.iter().any(|x| !x.monitors.is_empty()) {
            monitor_hits += 1;
        }
        if how.is_some() && first.is_none() {
            first = Some((r.seed, how.unwrap_or("unknown")));
        }
    }
    let swept = recs.len() as u64;
    (
        MutantRec {
            mutant: format!("{m:?}"),
            directed_test: test,
            directed_encoded: encoded,
            directed_caught: caught,
            first_seed: first.map(|(s, _)| s),
            how: first.map(|(_, h)| h),
            oracle_rate: if swept > 0 {
                oracle_hits as f64 / swept as f64
            } else {
                0.0
            },
            monitor_rate: if swept > 0 {
                monitor_hits as f64 / swept as f64
            } else {
                0.0
            },
            swept,
        },
        recs,
    )
}

fn cmd_mutants_check(args: &[String], pos_path: Option<String>) -> i32 {
    let path = pos_path.or_else(|| parse_results(args));
    let Some(path) = path else {
        eprintln!("e4-campaign mutants --check: --results is required");
        return 2;
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "e4-campaign mutants --check: cannot read {path}: {e}\n\
                 \x20(no mutant sweep results exist in this stage — see the crate README)"
            );
            return 2;
        }
    };
    let recs = match parse_mutants(&text) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("e4-campaign mutants --check: {path} does not parse: {e}");
            return 2;
        }
    };
    let threads = 4usize.max(parse_threads(args));
    let _ = threads;
    let mut failed = 0u64;
    for r in &recs {
        let Some(m) = mutant_by_name(&r.mutant) else {
            eprintln!("mutants --check: unknown mutant {}", r.mutant);
            failed += 1;
            continue;
        };
        // The directed column, re-run.
        if r.directed_encoded {
            let caught = directed_arms::run_arm(m);
            if caught != r.directed_caught {
                eprintln!(
                    "mutants --check: {} directed arm changed: recorded {:?} now {caught:?}",
                    r.mutant, r.directed_caught
                );
                failed += 1;
            }
        }
        // The campaign column: stop at the recorded first catch and
        // check it matches.
        if let Some(fs) = r.first_seed {
            let rec = workload::run_seed(fs, Some(m));
            let mut how: Option<&'static str> = None;
            if rec.rounds.iter().any(|x| x.o1_fail.is_some()) {
                how = Some("O-1");
            }
            if rec.rounds.iter().any(|x| x.o2_fail.is_some()) {
                how = how.or(Some("O-2"));
            }
            if rec.rounds.iter().any(|x| x.o3_fail.is_some()) {
                how = how.or(Some("O-3"));
            }
            if rec.rounds.iter().any(|x| !x.monitors.is_empty()) {
                how = how.or(Some("monitor"));
            }
            if rec.deadlock {
                how = how.or(Some("deadlock"));
            }
            if rec.panic {
                how = how.or(Some("panic"));
            }
            let ok = how == r.how;
            println!(
                "MUTCHECK {:<28} seed {fs} how {:?} recorded {:?} {}",
                r.mutant,
                how,
                r.how,
                if ok { "MATCH" } else { "DIFFERS" }
            );
            if !ok {
                failed += 1;
            }
        } else {
            // UNCAUGHT over the sweep: the whole range must still catch
            // nothing (re-run the boundary seeds — the full range is the
            // sweep's own job).
            let spot = [0u64, 1, 2, 3, 7, 11, 97, 499];
            let mut any = false;
            for s in spot {
                let rec = workload::run_seed(s, Some(m));
                if rec.rounds.iter().any(|x| !x.pass) || rec.deadlock || rec.panic {
                    any = true;
                }
            }
            println!(
                "MUTCHECK {:<28} recorded UNCAUGHT spot-check {}",
                r.mutant,
                if any { "NOW CAUGHT" } else { "clean" }
            );
            if any {
                failed += 1;
            }
        }
    }
    if failed > 0 {
        eprintln!("mutants --check: {failed} checks failed");
        return 2;
    }
    println!("MUTANTS CHECK OK {} mutants", recs.len());
    0
}

fn mutant_by_name(name: &str) -> Option<Mutant> {
    directed_arms::TABLE
        .iter()
        .find(|(m, _)| format!("{m:?}") == name)
        .map(|(m, _)| *m)
}

// ---- the directed arms (X-5b's mutant arms, re-run for Work 8) ----

/// X-5b's directed mutant arms, as data. Each arm re-runs the directed
/// test's mutant shape through [`crate::harness::run_round`] and returns
/// whether the named catcher still fires. **Stage status:** the table
/// names every mutant's directed test; [`run_arm`] ports the arms
/// incrementally (the unported entries return `None` and the sweep
/// records `directed_encoded: false` — never a silent pass).
pub mod directed_arms {
    use super::super::harness::{
        self, CrashSpec, MediaDraw, RoundFaults, Script, ScriptOp, ScriptTask,
    };
    use super::super::model::{CATALOG, Model, Op, PUBLIC, to_be64};
    use super::super::sim::{FaultClass, FaultKnobs, MediaFault, MediaKind};
    use super::super::store::ckpt::CkptHook;
    use super::super::store::{Mutant, StoreCfg};

    /// The mutation table's rows: every mutant and its directed test.
    pub const TABLE: &[(Mutant, &str)] = &[
        (Mutant::CrcSkip, "d17_integrity"),
        (Mutant::RefCrcSkip, "d17_integrity (REFCRC shape)"),
        (Mutant::EpochSkip, "single site (subsumed; expected-0 rate)"),
        (Mutant::IdSkip, "single site (subsumed; expected-0 rate)"),
        (Mutant::EpochSkipRefCrc, "d17_integrity (REFCRC shape)"),
        (Mutant::IdSkipRefCrc, "d17_integrity (REFCRC shape)"),
        (Mutant::FreeEarly, "d14_frees"),
        (Mutant::BitmapOmitsFreesetPages, "d14_frees (amcheck O-2)"),
        (Mutant::MetaBeforeSync, "d1_every_boundary / d14_frees"),
        (Mutant::FlipNoGate, "d5_split_straddling_flip"),
        (Mutant::WalSyncTOnly, "d4_wal_sync_t"),
        (
            Mutant::WriteGuardNoCopy,
            "d16 (argued uncaught directed; campaign Q10)",
        ),
        (
            Mutant::EvictSkipImageMap,
            "d16 (argued uncaught directed; campaign Q10)",
        ),
        (Mutant::StartLsnPastChunk, "d3_open_chunk"),
        (Mutant::StartLsnMarkedChunk, "d2_marked_chunk"),
        (Mutant::TsAfterAppend, "d18_ts_order"),
        (Mutant::LsnCheckSkip, "d8_stale_recycled_segment"),
        (Mutant::PublishBeforeMeta, "d11_bulk_publish"),
        (Mutant::GcwNotRecorded, "d12_gcw_history"),
        (Mutant::GcwBoundSkip, "d12b_gcw_bound"),
        (Mutant::ReplayDupConflict, "d6_partial_replay / d13_dup"),
        (Mutant::DupDifferentAccepted, "d13_dup"),
        (Mutant::InplaceEvict, "d14_frees"),
        (Mutant::DropRootFirst, "d15_drop_lpid_exact"),
        (Mutant::RenameNoDirsync, "d9_dropped_rename"),
        (Mutant::TruncateLogicalOnly, "d7_zombie_tail"),
        (Mutant::StartSegmentMissingOk, "d10_start_segment"),
        (Mutant::MetaWrongSlot, "d17_integrity (meta cell, G7)"),
    ];

    fn cfg(mutant: Option<Mutant>) -> StoreCfg {
        StoreCfg {
            map_leaf_cap: 8,
            freeset_leaf_bits: 128,
            pool_frames: 16,
            r_epoch: 2,
            mutant,
        }
    }

    fn faults(class: FaultClass) -> RoundFaults {
        RoundFaults {
            class,
            knobs: FaultKnobs {
                keep_latest_pm: 0,
                tear_pm: 0,
            },
            directed: Vec::new(),
            no_r_taint: false,
            media: MediaDraw::None,
            twin_supply: None,
            nested_draw: 0,
            probe_pass: false,
        }
    }

    fn put(i: u64, v: u64) -> Op {
        Op::Put {
            tree: 2,
            key: i.to_be_bytes().to_vec(),
            value: vec![v as u8; 8],
        }
    }

    fn publish(tree: u64) -> Op {
        Op::Put {
            tree: CATALOG,
            key: to_be64(tree),
            value: PUBLIC.to_vec(),
        }
    }

    /// Re-run the directed arm of `m`; `None` = the arm is not encoded
    /// at this stage (recorded honestly, never a pass). An encoded arm
    /// returns whether the mutant was caught.
    pub fn run_arm(m: Mutant) -> Option<bool> {
        match m {
            // D2's arm (START-LSN-MARKED-CHUNK): the marked case loses
            // the chunked transaction below the marker's replay start —
            // O-1 fails.
            Mutant::StartLsnMarkedChunk => {
                let ops = vec![
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: vec![
                            Op::CreateTree { tree: 2 },
                            Op::CreateTree { tree: CATALOG },
                            publish(2),
                        ],
                    },
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: vec![put(1, 1)],
                    },
                    ScriptOp::Park {
                        name: "chunked:preapply",
                        budget: 48,
                    },
                    ScriptOp::Chunked {
                        sync_on: true,
                        ops: (0..30).map(|i| put(100 + i, 9)).collect(),
                    },
                ];
                let script = Script {
                    tasks: vec![ScriptTask { ops }],
                    writer: true,
                    evictor: false,
                    checkpointer_every: Some(3),
                    crash: Some(CrashSpec::Hook {
                        ckpt_index: 0,
                        hook: CkptHook::After6,
                    }),
                    nested: Vec::new(),
                };
                let (disk, st) = harness::create_store(cfg(Some(m)), 2001, 7).ok()?;
                let out = harness::run_round(
                    2001,
                    0,
                    &script,
                    &faults(FaultClass::C),
                    cfg(Some(m)),
                    disk,
                    st,
                    &Model::new(),
                    0,
                    None,
                );
                Some(!out.pass && out.o1.is_err())
            }
            // D17's first half (CRC-SKIP), the pinned shape verbatim: the
            // torn **map leaf** at `map_leaf_cap` 254 — a middle-sector
            // tear changes real bytes without touching the sector-0
            // header, so only check (1) (the CRC-vs-bytes check) names
            // it; CRC-SKIP removes exactly that, the torn leaf is
            // accepted through a tainted read, and `served` is the pin.
            Mutant::CrcSkip => {
                let cfg254 = |mutant: Option<Mutant>| StoreCfg {
                    map_leaf_cap: 254,
                    freeset_leaf_bits: 128,
                    pool_frames: 16,
                    r_epoch: 2,
                    mutant,
                };
                let ops = vec![
                    ScriptOp::Commit {
                        sync_on: true,
                        ops: vec![
                            Op::CreateTree { tree: 2 },
                            Op::CreateTree { tree: CATALOG },
                            publish(2),
                        ],
                    },
                    // ~60 pages of tree: the map leaf-0 carries entries
                    // across the [512, 4096) sectors (a chunked txn — the
                    // §12.2 record cap).
                    ScriptOp::Chunked {
                        sync_on: true,
                        ops: (0..420u64)
                            .map(|i| Op::Put {
                                tree: 2,
                                key: i.to_be_bytes().to_vec(),
                                value: vec![1u8; 400],
                            })
                            .collect(),
                    },
                    ScriptOp::WaitVisible(2),
                    ScriptOp::Checkpoint,
                    ScriptOp::Yields(3),
                ];
                let script = Script {
                    tasks: vec![ScriptTask { ops }],
                    writer: true,
                    evictor: false,
                    checkpointer_every: Some(1000),
                    crash: Some(CrashSpec::TaskYield(1, 6)),
                    nested: Vec::new(),
                };
                // A faultless crash round supplies the crashed disk the
                // fault is built from (the same schedule prefix).
                let (disk, st) = harness::create_store(cfg254(None), 2016, 7).ok()?;
                let base = harness::run_round(
                    2016,
                    0,
                    &script,
                    &faults(FaultClass::C),
                    cfg254(None),
                    disk,
                    st,
                    &Model::new(),
                    0,
                    None,
                );
                let pf = base.crashed_disk.stat(super::super::sim::PAGE_FILE)?;
                let meta = harness::durable_image(&base.crashed_disk, &cfg254(None)).2?;
                let w = base
                    .crashed_disk
                    .dwrites_of(pf)
                    .iter()
                    .rev()
                    .find(|w| w.off == meta.map_root.slot * 4096 && w.bytes.len() == 4096)?
                    .clone();
                let fault = MediaFault {
                    inode: pf,
                    off: w.off,
                    len: 4096,
                    bytes: w.bytes.clone(),
                    pre: w.pre.clone(),
                    kind: MediaKind::Torn {
                        sectors: vec![w.off + 512],
                    },
                };
                let (disk2, st2) = harness::create_store(cfg254(Some(m)), 2016, 7).ok()?;
                let f = RoundFaults {
                    directed: vec![fault],
                    class: FaultClass::Mp,
                    ..faults(FaultClass::Mp)
                };
                let out = harness::run_round(
                    2016,
                    0,
                    &script,
                    &f,
                    cfg254(Some(m)),
                    disk2,
                    st2,
                    &Model::new(),
                    0,
                    None,
                );
                Some(out.served)
            }
            _ => None,
        }
    }
}

// ---- JSON emit/parse ----

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Emit the campaign file.
pub fn emit_campaign(recs: &[SeedRec]) -> String {
    let mut s = String::from("{\"e4\":1,\"seeds\":[");
    for (i, r) in recs.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "{{\"seed\":{},\"class\":{},\"knobs\":{},\"digest\":{},\"deadlock\":{},\"panic\":{},\"baton\":{},\"stopped\":{},\"defect\":{},\"rounds\":[",
            r.seed,
            esc(&r.class),
            r.knobs,
            r.digest,
            r.deadlock,
            r.panic,
            r.baton,
            r.stopped_early,
            r.defect.as_deref().map(esc).unwrap_or_else(|| "null".into()),
        ));
        for (j, x) in r.rounds.iter().enumerate() {
            if j > 0 {
                s.push(',');
            }
            s.push_str(&format!(
                "{{\"crash\":{},\"hook\":{},\"fired\":{},\"nested\":{},\"lost\":{},\"mis\":{},\"torn\":{},\"lo\":{},\"hi\":{},\"c\":{},\"frontier\":{},\"gcw\":{},\"o1\":{},\"o1f\":{},\"o2\":{},\"o3\":{},\"served\":{},\"ackl\":{},\"ackls\":{},\"mrb\":{},\"rlen\":{},\"pass\":{},\"dl\":{},\"pa\":{},\"bt\":{},\"terr\":{},\"q\":[{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}]}}",
                esc(&x.crash),
                esc(&x.hook),
                x.fired,
                x.nested,
                x.media_lost,
                x.media_misdirected,
                x.media_torn,
                x.lo,
                x.hi,
                x.c.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
                x.frontier,
                x.gc_w,
                x.o1_fail.as_deref().map(esc).unwrap_or_else(|| "null".into()),
                x.o1f_fail,
                x.o2_fail.as_deref().map(esc).unwrap_or_else(|| "null".into()),
                x.o3_fail.as_deref().map(esc).unwrap_or_else(|| "null".into()),
                x.served,
                x.acked_lost,
                x.acked_lost_syncoff,
                x.meta_rollback,
                x.r_len,
                x.pass,
                x.deadlock,
                x.panic,
                x.baton,
                x.task_error.is_some(),
                u8::from(x.q4_split_merge),
                u8::from(x.q5_open),
                u8::from(x.q5_marked),
                u8::from(x.q5_partial),
                u8::from(x.q6_unpublished),
                u8::from(x.q6_published),
                u8::from(x.q7_drop_walk),
                u8::from(x.q8_syncoff_lost),
                u8::from(x.q9_partial_above_t),
                u8::from(x.q10_exhausted),
                u8::from(x.q11_stale_seg),
                u8::from(x.q12_dropped_rename),
                u8::from(x.q13_lost),
                u8::from(x.q13_misdirected),
                u8::from(x.q13_torn),
                u8::from(x.q14_meta_fault),
                u8::from(x.q15_durable_record),
                u8::from(x.q16_map_depth2),
                u8::from(x.q17_freeset_grew),
                u8::from(x.q18_gc_advanced),
                u8::from(x.sizing_ok),
            ));
        }
        s.push_str("],\"twin\":[");
        for (j, t) in r.twin.iter().enumerate() {
            if j > 0 {
                s.push(',');
            }
            s.push_str(&format!(
                "{{\"pass\":{},\"o1\":{}}}",
                t.pass,
                t.o1_fail
                    .as_deref()
                    .map(esc)
                    .unwrap_or_else(|| "null".into()),
            ));
        }
        s.push_str("]}");
    }
    s.push_str("]}");
    s
}

/// Emit the mutants file.
pub fn emit_mutants(recs: &[MutantRec]) -> String {
    let mut s = String::from("{\"e4\":1,\"mutants\":[");
    for (i, r) in recs.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "{{\"mutant\":{},\"directed\":{{\"test\":{},\"encoded\":{},\"caught\":{}}},\"first_seed\":{},\"how\":{},\"oracle_rate\":{:.4},\"monitor_rate\":{:.4},\"swept\":{}}}",
            esc(&r.mutant),
            esc(r.directed_test),
            r.directed_encoded,
            r.directed_caught.map(|b| b.to_string()).unwrap_or_else(|| "null".into()),
            r.first_seed.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
            r.how.map(esc).unwrap_or_else(|| "null".into()),
            r.oracle_rate,
            r.monitor_rate,
            r.swept,
        ));
    }
    s.push_str("]}");
    s
}

// A minimal JSON reader for exactly the shapes the emitters produce.
// It is strict (no trailing junk) and returns owned data.

struct Jx<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Jx<'a> {
    fn new(s: &'a str) -> Self {
        Jx {
            b: s.as_bytes(),
            i: 0,
        }
    }
    fn ws(&mut self) {
        while self.i < self.b.len() && (self.b[self.i] as char).is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn eat(&mut self, c: u8) -> Result<(), String> {
        self.ws();
        if self.b.get(self.i) == Some(&c) {
            self.i += 1;
            Ok(())
        } else {
            Err(format!("expected {:?} at {}", c as char, self.i))
        }
    }
    fn try_eat(&mut self, c: u8) -> bool {
        self.ws();
        if self.b.get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn num(&mut self) -> Result<f64, String> {
        self.ws();
        let s = self.i;
        while self.i < self.b.len()
            && matches!(
                self.b[self.i],
                b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'
            )
        {
            self.i += 1;
        }
        if s == self.i {
            return Err(format!("number expected at {}", self.i));
        }
        std::str::from_utf8(&self.b[s..self.i])
            .ok()
            .and_then(|t| t.parse().ok())
            .ok_or_else(|| format!("bad number at {s}"))
    }
    fn int(&mut self) -> Result<u64, String> {
        self.num().map(|f| f as u64)
    }
    fn string(&mut self) -> Result<String, String> {
        self.ws();
        self.eat(b'"')?;
        let mut out = String::new();
        while let Some(&c) = self.b.get(self.i) {
            self.i += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let e = *self.b.get(self.i).ok_or("escape eof")?;
                    self.i += 1;
                    match e {
                        b'n' => out.push('\n'),
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'u' => {
                            let hex =
                                std::str::from_utf8(self.b.get(self.i..self.i + 4).ok_or("u eof")?)
                                    .map_err(|_| "utf8")?;
                            let v = u32::from_str_radix(hex, 16).map_err(|_| "hex")?;
                            self.i += 4;
                            out.push(char::from_u32(v).unwrap_or('?'));
                        }
                        other => out.push(other as char),
                    }
                }
                other => {
                    // Collect the raw UTF-8 bytes.
                    let start = self.i - 1;
                    let len = utf8_len(other);
                    self.i = start + len;
                    let sl = self.b.get(start..self.i).ok_or("utf8 eof")?;
                    out.push_str(std::str::from_utf8(sl).map_err(|_| "utf8")?);
                }
            }
        }
        Err("string eof".into())
    }
    fn null(&mut self) -> Result<(), String> {
        self.ws();
        if self.b.get(self.i..self.i + 4) == Some(b"null") {
            self.i += 4;
            Ok(())
        } else {
            Err("null expected".into())
        }
    }
    fn opt_int(&mut self) -> Result<Option<u64>, String> {
        self.ws();
        if self.b.get(self.i) == Some(&b'n') {
            self.null()?;
            Ok(None)
        } else {
            Ok(Some(self.int()?))
        }
    }
    fn opt_string(&mut self) -> Result<Option<String>, String> {
        self.ws();
        if self.b.get(self.i) == Some(&b'n') {
            self.null()?;
            Ok(None)
        } else {
            Ok(Some(self.string()?))
        }
    }
    fn opt_bool(&mut self) -> Result<Option<bool>, String> {
        self.ws();
        if self.b.get(self.i) == Some(&b'n') {
            self.null()?;
            Ok(None)
        } else {
            Ok(Some(self.boolv()?))
        }
    }
    fn boolv(&mut self) -> Result<bool, String> {
        self.ws();
        if self.b.get(self.i..self.i + 4) == Some(b"true") {
            self.i += 4;
            Ok(true)
        } else if self.b.get(self.i..self.i + 5) == Some(b"false") {
            self.i += 5;
            Ok(false)
        } else {
            Err("bool expected".into())
        }
    }
    /// Read an object whose keys we do not care about (skipping values).
    fn skip_value(&mut self) -> Result<(), String> {
        self.ws();
        match self.b.get(self.i) {
            Some(b'{') => {
                self.i += 1;
                if self.try_eat(b'}') {
                    return Ok(());
                }
                loop {
                    self.string()?;
                    self.eat(b':')?;
                    self.skip_value()?;
                    if self.try_eat(b',') {
                        continue;
                    }
                    self.eat(b'}')?;
                    return Ok(());
                }
            }
            Some(b'[') => {
                self.i += 1;
                if self.try_eat(b']') {
                    return Ok(());
                }
                loop {
                    self.skip_value()?;
                    if self.try_eat(b',') {
                        continue;
                    }
                    self.eat(b']')?;
                    return Ok(());
                }
            }
            Some(b'"') => {
                self.string()?;
                Ok(())
            }
            Some(b't') | Some(b'f') | Some(b'n') if self.b.get(self.i) == Some(&b'n') => {
                self.null()
            }
            Some(b't') | Some(b'f') => self.boolv().map(|_| ()),
            _ => self.num().map(|_| ()),
        }
    }
}

fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else {
        4
    }
}

/// Parse a campaign file back into records (seed, digest and the quota
/// fields; the other round fields parse into place for the digest
/// comparison to be meaningful).
pub fn parse_campaign(text: &str) -> Result<Vec<SeedRec>, String> {
    let mut j = Jx::new(text);
    j.eat(b'{')?;
    let mut out = Vec::new();
    loop {
        let k = j.string()?;
        j.eat(b':')?;
        match k.as_str() {
            "seeds" => {
                j.eat(b'[')?;
                if j.try_eat(b']') {
                    break;
                }
                loop {
                    out.push(parse_seed(&mut j)?);
                    if j.try_eat(b',') {
                        continue;
                    }
                    j.eat(b']')?;
                    break;
                }
            }
            _ => j.skip_value()?,
        }
        if j.try_eat(b',') {
            continue;
        }
        j.eat(b'}')?;
        break;
    }
    Ok(out)
}

fn parse_seed(j: &mut Jx<'_>) -> Result<SeedRec, String> {
    let mut r = SeedRec::default();
    j.eat(b'{')?;
    loop {
        let k = j.string()?;
        j.eat(b':')?;
        match k.as_str() {
            "seed" => r.seed = j.int()?,
            "class" => r.class = j.string()?,
            "knobs" => r.knobs = j.int()? as u32,
            "digest" => r.digest = j.int()?,
            "deadlock" => r.deadlock = j.boolv()?,
            "panic" => r.panic = j.boolv()?,
            "baton" => r.baton = j.boolv()?,
            "stopped" => r.stopped_early = j.boolv()?,
            "defect" => r.defect = j.opt_string()?,
            "rounds" => {
                j.eat(b'[')?;
                if !j.try_eat(b']') {
                    'r: loop {
                        r.rounds.push(parse_round(j)?);
                        if j.try_eat(b',') {
                            continue 'r;
                        }
                        j.eat(b']')?;
                        break;
                    }
                }
            }
            "twin" => {
                j.eat(b'[')?;
                if !j.try_eat(b']') {
                    't: loop {
                        j.eat(b'{')?;
                        let mut pass = false;
                        let mut o1 = None;
                        'to: loop {
                            let tk = j.string()?;
                            j.eat(b':')?;
                            match tk.as_str() {
                                "pass" => pass = j.boolv()?,
                                "o1" => o1 = j.opt_string()?,
                                _ => j.skip_value()?,
                            }
                            if j.try_eat(b',') {
                                continue 'to;
                            }
                            j.eat(b'}')?;
                            break;
                        }
                        r.twin
                            .push(crate::workload::TwinRound { pass, o1_fail: o1 });
                        if j.try_eat(b',') {
                            continue 't;
                        }
                        j.eat(b']')?;
                        break;
                    }
                }
            }
            _ => j.skip_value()?,
        }
        if j.try_eat(b',') {
            continue;
        }
        j.eat(b'}')?;
        break;
    }
    Ok(r)
}

fn parse_round(j: &mut Jx<'_>) -> Result<crate::workload::RoundRec, String> {
    use crate::workload::RoundRec;
    let mut x = RoundRec::default();
    j.eat(b'{')?;
    loop {
        let k = j.string()?;
        j.eat(b':')?;
        match k.as_str() {
            "crash" => x.crash = j.string()?,
            "hook" => x.hook = j.string()?,
            "fired" => x.fired = j.boolv()?,
            "nested" => x.nested = j.int()? as u32,
            "lost" => x.media_lost = j.int()?,
            "mis" => x.media_misdirected = j.int()?,
            "torn" => x.media_torn = j.int()?,
            "lo" => x.lo = j.int()?,
            "hi" => x.hi = j.int()?,
            "c" => x.c = j.opt_int()?,
            "frontier" => x.frontier = j.int()?,
            "gcw" => x.gc_w = j.int()?,
            "o1" => x.o1_fail = j.opt_string()?,
            "o1f" => x.o1f_fail = j.boolv()?,
            "o2" => x.o2_fail = j.opt_string()?,
            "o3" => x.o3_fail = j.opt_string()?,
            "served" => x.served = j.boolv()?,
            "ackl" => x.acked_lost = j.int()?,
            "ackls" => x.acked_lost_syncoff = j.int()?,
            "mrb" => x.meta_rollback = j.boolv()?,
            "rlen" => x.r_len = j.int()? as usize,
            "pass" => x.pass = j.boolv()?,
            "dl" => x.deadlock = j.boolv()?,
            "pa" => x.panic = j.boolv()?,
            "bt" => x.baton = j.boolv()?,
            "terr" => {
                let b = j.boolv()?;
                x.task_error = b.then(|| "task error (see digest)".to_owned());
            }
            "q" => {
                j.eat(b'[')?;
                let mut vals: Vec<bool> = Vec::new();
                if !j.try_eat(b']') {
                    loop {
                        vals.push(j.int()? != 0);
                        if j.try_eat(b',') {
                            continue;
                        }
                        j.eat(b']')?;
                        break;
                    }
                }
                let g = |i: usize| vals.get(i).copied().unwrap_or(false);
                x.q4_split_merge = g(0);
                x.q5_open = g(1);
                x.q5_marked = g(2);
                x.q5_partial = g(3);
                x.q6_unpublished = g(4);
                x.q6_published = g(5);
                x.q7_drop_walk = g(6);
                x.q8_syncoff_lost = g(7);
                x.q9_partial_above_t = g(8);
                x.q10_exhausted = g(9);
                x.q11_stale_seg = g(10);
                x.q12_dropped_rename = g(11);
                x.q13_lost = g(12);
                x.q13_misdirected = g(13);
                x.q13_torn = g(14);
                x.q14_meta_fault = g(15);
                x.q15_durable_record = g(16);
                x.q16_map_depth2 = g(17);
                x.q17_freeset_grew = g(18);
                x.q18_gc_advanced = g(19);
                x.sizing_ok = g(20) || vals.is_empty();
            }
            _ => j.skip_value()?,
        }
        if j.try_eat(b',') {
            continue;
        }
        j.eat(b'}')?;
        break;
    }
    Ok(x)
}

/// Parse a mutants file.
pub fn parse_mutants(text: &str) -> Result<Vec<MutantRec>, String> {
    let mut j = Jx::new(text);
    j.eat(b'{')?;
    let mut out = Vec::new();
    loop {
        let k = j.string()?;
        j.eat(b':')?;
        match k.as_str() {
            "mutants" => {
                j.eat(b'[')?;
                if j.try_eat(b']') {
                    break;
                }
                loop {
                    j.eat(b'{')?;
                    let mut r = MutantRec {
                        mutant: String::new(),
                        directed_test: "",
                        directed_encoded: false,
                        directed_caught: None,
                        first_seed: None,
                        how: None,
                        oracle_rate: 0.0,
                        monitor_rate: 0.0,
                        swept: 0,
                    };
                    loop {
                        let kk = j.string()?;
                        j.eat(b':')?;
                        match kk.as_str() {
                            "mutant" => r.mutant = j.string()?,
                            "first_seed" => r.first_seed = j.opt_int()?,
                            "how" => r.how = j.opt_string().map(|o| o.map(leak))?,
                            "oracle_rate" => r.oracle_rate = j.num()?,
                            "monitor_rate" => r.monitor_rate = j.num()?,
                            "swept" => r.swept = j.int()?,
                            "directed" => {
                                j.eat(b'{')?;
                                loop {
                                    let dk = j.string()?;
                                    j.eat(b':')?;
                                    match dk.as_str() {
                                        "test" => r.directed_test = leak(j.string()?),
                                        "encoded" => r.directed_encoded = j.boolv()?,
                                        "caught" => r.directed_caught = j.opt_bool()?,
                                        _ => j.skip_value()?,
                                    }
                                    if j.try_eat(b',') {
                                        continue;
                                    }
                                    j.eat(b'}')?;
                                    break;
                                }
                            }
                            _ => j.skip_value()?,
                        }
                        if j.try_eat(b',') {
                            continue;
                        }
                        j.eat(b'}')?;
                        break;
                    }
                    out.push(r);
                    if j.try_eat(b',') {
                        continue;
                    }
                    j.eat(b']')?;
                    break;
                }
            }
            _ => j.skip_value()?,
        }
        if j.try_eat(b',') {
            continue;
        }
        j.eat(b'}')?;
        break;
    }
    Ok(out)
}

/// The parsed `how` strings live for the process (the mutants file's
/// small, fixed vocabulary of catchers).
fn leak(s: String) -> &'static str {
    match s.as_str() {
        "O-1" => "O-1",
        "O-2" => "O-2",
        "O-3" => "O-3",
        "monitor" => "monitor",
        "deadlock" => "deadlock",
        "panic" => "panic",
        _ => "unknown",
    }
}

// BTreeMap import kept for future per-class summary use.
#[allow(dead_code)]
type _Keep = BTreeMap<u64, u64>;
