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
//! **Stage status (see the crate README):** the rulings X5-R6–R12 are
//! delivered and recorded verbatim in the crate README (before the first
//! campaign `run`, per X5-R8); X5-R13's memory and liveness
//! instrumentation — the liveness budget, the memory watchdog and the
//! half-open seed range — guards every campaign-class command from this
//! stage on.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

/// Parse `--seeds A..B` (X5-R13 (d): **half-open**, A ≤ seed < B, as in
/// Rust and in the card — `0..10000` is 10,000 seeds, `0..500` is 500,
/// `5..5` is empty). `default` is the exclusive upper bound when the
/// flag is absent (the campaign's 10,000; the sweep's 500).
fn parse_seeds(args: &[String], default: u64) -> (u64, u64) {
    let mut lo = 0u64;
    let mut hi = default;
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

/// The seed count of a half-open range `lo..hi` (X5-R13 (d)'s pin:
/// `0..10000` is 10,000; `5..5` is 0, an error at the command level).
pub fn seed_count(lo: u64, hi: u64) -> u64 {
    hi.saturating_sub(lo)
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

// ---- the X5-R13 (c) memory self-check ----

/// The default process memory limit (MiB) of the watchdog; override with
/// the `E4_MEM_LIMIT_MIB` environment variable. X5-R13 (c), P ruling
/// nv3-X-5c-esc-2: the watchdog is process-level only — it never feeds a
/// record, a digest or a sim-visible decision, which is why its 1-second
/// wall-clock sleep does not fall under the card's wall-clock ban (and
/// only the watchdog is exempt).
pub const E4_MEM_LIMIT_MIB_DEFAULT: u64 = 20_480;

/// The trip check as a pure function of `(rss, limit)` — X5-R13 (c)'s
/// pin: unit-tested on both sides of the limit. Trips strictly above.
pub fn mem_trip(rss_mib: u64, limit_mib: u64) -> bool {
    rss_mib > limit_mib
}

/// The memory limit in force: `E4_MEM_LIMIT_MIB` from the environment,
/// else the default (20 GiB).
pub fn mem_limit_mib() -> u64 {
    std::env::var("E4_MEM_LIMIT_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(E4_MEM_LIMIT_MIB_DEFAULT)
}

/// Read `VmRSS` from `/proc/self/status` (KiB → MiB, std only). `None`
/// when the file or the field is absent (a non-Linux host, say); the
/// watchdog then never trips, which is the fail-open direction P's
/// ruling accepts (the address-space `ulimit` backstop remains).
pub fn rss_mib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kib: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kib / 1024);
        }
    }
    None
}

/// The watchdog's shared state: the stop flag the dispatch loops honor,
/// the trip record, and the latest RSS sample for the progress lines.
/// The trip is set exactly once; `finished` retires the watchdog once
/// the command completed normally, so a spurious last-instant sample
/// cannot abort a finished run between its result write and its exit.
#[derive(Debug)]
pub struct Watch {
    limit_mib: u64,
    stop: AtomicBool,
    tripped_rss: AtomicU64,
    last_rss: AtomicU64,
    finished: AtomicBool,
}

impl Watch {
    /// A watchdog state over `limit_mib`.
    pub fn new(limit_mib: u64) -> Self {
        Self {
            limit_mib,
            stop: AtomicBool::new(false),
            tripped_rss: AtomicU64::new(0),
            last_rss: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        }
    }

    /// The limit in force.
    pub fn limit_mib(&self) -> u64 {
        self.limit_mib
    }

    /// Dispatch must stop: the watchdog tripped.
    pub fn stop(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// The RSS at trip, if tripped.
    pub fn tripped(&self) -> Option<u64> {
        match self.tripped_rss.load(Ordering::SeqCst) {
            0 => None,
            v => Some(v),
        }
    }

    /// The latest sample (0 before the first).
    pub fn rss(&self) -> u64 {
        self.last_rss.load(Ordering::SeqCst)
    }

    /// Retire the watchdog: the command finished; a later sample may not
    /// abort it.
    pub fn finish(&self) {
        self.finished.store(true, Ordering::SeqCst);
    }

    /// One sample: record the RSS, trip (once) when it is above the
    /// limit. `force_trip` (tests) plants a trip without `/proc`.
    fn sample(&self) {
        let Some(rss) = rss_mib() else {
            return;
        };
        self.record_and_maybe_trip(rss);
    }

    fn record_and_maybe_trip(&self, rss: u64) {
        self.last_rss.store(rss, Ordering::SeqCst);
        if self.tripped_rss.load(Ordering::SeqCst) == 0 && mem_trip(rss, self.limit_mib) {
            self.tripped_rss.store(rss, Ordering::SeqCst);
            // Stop dispatch before anything else (the ruling's order).
            self.stop.store(true, Ordering::SeqCst);
        }
    }

    /// Plant a trip at `rss` MiB (tests: the dispatch-stop path without
    /// depending on the host's memory).
    pub fn force_trip(&self, rss: u64) {
        self.record_and_maybe_trip(rss.max(self.limit_mib + 1));
    }
}

/// The dispatch counters the abort line reports: seeds handed to workers
/// (`taken`), completed (`done`), and the sweep's total.
#[derive(Debug)]
pub struct Dispatch {
    /// Seeds handed out (≈ `next − lo`).
    pub taken: AtomicU64,
    /// Seeds completed.
    pub done: AtomicU64,
    /// The sweep's seed total.
    pub total: u64,
}

impl Dispatch {
    /// Counters for a sweep of `total` seeds.
    pub fn new(total: u64) -> Self {
        Self {
            taken: AtomicU64::new(0),
            done: AtomicU64::new(0),
            total,
        }
    }
}

/// What the watchdog writes to `<out>.partial` on a trip: the completed
/// seed records (`run`) or the completed mutant records (`mutants`).
/// `verify` and `mutants --check` carry no partial sink (the ruling
/// names `<out>.partial` for the record-writing commands; their re-runs
/// produce nothing worth persisting — a `.partial` file is diagnostic
/// only either way).
pub enum PartialSink {
    /// The completed [`SeedRec`]s of a `run` (or a `verify` re-run, if
    /// one ever carries a sink), sorted by seed.
    Seeds {
        /// The `<out>.partial` path.
        path: String,
        /// The live shared records.
        recs: std::sync::Arc<std::sync::Mutex<Vec<SeedRec>>>,
    },
    /// The completed [`MutantRec`]s of a `mutants` sweep.
    Mutants {
        /// The `<out>.partial` path.
        path: String,
        /// The live shared records.
        recs: std::sync::Arc<std::sync::Mutex<Vec<MutantRec>>>,
    },
}

/// Write the partial file of a trip: the completed records, sorted by
/// seed (`Seeds`), emitted with the same writer as the real output.
pub fn write_partial_seeds(path: &str, recs: &[SeedRec]) -> std::io::Result<()> {
    let mut sorted = recs.to_vec();
    sorted.sort_unstable_by_key(|r| r.seed);
    std::fs::write(path, emit_campaign(&sorted))
}

/// Write the partial file of a trip (the mutants form).
pub fn write_partial_mutants(path: &str, recs: &[MutantRec]) -> std::io::Result<()> {
    std::fs::write(path, emit_mutants(recs))
}

/// Start the watchdog thread (X5-R13 (c)): sample once per second; on a
/// trip, write the partial file (if any), print the abort line and exit
/// 3 — without writing `<out>` and without a final `E4` line. The thread
/// is retired by [`Watch::finish`] when the command completes normally;
/// it never feeds any sim-visible decision, so its wall-clock sleep is
/// the one the ruling exempts from the card's wall-clock ban.
pub fn start_watchdog(
    watch: std::sync::Arc<Watch>,
    dispatch: std::sync::Arc<Dispatch>,
    partial: Option<std::sync::Arc<PartialSink>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        loop {
            watch.sample();
            if !watch.finished.load(Ordering::SeqCst)
                && let Some(rss) = watch.tripped()
            {
                let done = dispatch.done.load(Ordering::SeqCst);
                let taken = dispatch.taken.load(Ordering::SeqCst).min(dispatch.total);
                if let Some(sink) = &partial {
                    let r = match sink.as_ref() {
                        PartialSink::Seeds { path, recs } => {
                            let mut sorted = recs.lock().map(|m| m.clone()).unwrap_or_default();
                            sorted.sort_unstable_by_key(|x| x.seed);
                            std::fs::write(path, emit_campaign(&sorted))
                        }
                        PartialSink::Mutants { path, recs } => {
                            let recs = recs.lock().map(|m| m.clone()).unwrap_or_default();
                            std::fs::write(path, emit_mutants(&recs))
                        }
                    };
                    if let Err(e) = r {
                        // A real error (the abort line still goes out).
                        eprintln!("e4-campaign: cannot write the partial file: {e}");
                    }
                }
                eprintln!(
                    "E4 ABORT rss={rss} limit={} done={}/{} inflight={}",
                    watch.limit_mib(),
                    done,
                    dispatch.total,
                    taken.saturating_sub(done)
                );
                std::process::exit(3);
            }
            if watch.finished.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    })
}

// ---- run ----

fn cmd_run(args: &[String]) -> i32 {
    let Some(out) = parse_out(args) else {
        eprintln!("e4-campaign run: --out is required");
        return 2;
    };
    let (lo, hi) = parse_seeds(args, CAMPAIGN_SEEDS);
    let threads = parse_threads(args);
    // X5-R13 (d): the range is half-open; `5..5` is empty, an error.
    if lo >= hi {
        eprintln!("e4-campaign run: empty seed range {lo}..{hi}");
        return 2;
    }
    // X5-R13 (c): the watchdog rides with the run — on a trip it stops
    // dispatch, writes the completed records to `<out>.partial`, prints
    // the abort line and exits 3, so this function never reaches its own
    // writes. `WatchRetire` retires it the moment the run completes
    // normally (a last-instant sample may not abort a finished run).
    let watch = std::sync::Arc::new(Watch::new(mem_limit_mib()));
    let dispatch = std::sync::Arc::new(Dispatch::new(seed_count(lo, hi)));
    let shared: std::sync::Arc<std::sync::Mutex<Vec<SeedRec>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = std::sync::Arc::new(PartialSink::Seeds {
        path: format!("{out}.partial"),
        recs: std::sync::Arc::clone(&shared),
    });
    let _wd = start_watchdog(
        std::sync::Arc::clone(&watch),
        std::sync::Arc::clone(&dispatch),
        Some(sink),
    );
    run_range_into(&shared, lo, hi, None, threads, Some((&watch, &dispatch)));
    let _retire = WatchRetire(std::sync::Arc::clone(&watch));
    let mut recs = shared.lock().map(|m| m.clone()).unwrap_or_default();
    recs.sort_unstable_by_key(|r| r.seed);
    let json = emit_campaign(&recs);
    if let Err(e) = std::fs::write(&out, json) {
        eprintln!("e4-campaign run: cannot write {out}: {e}");
        return 2;
    }
    print_final_line(&recs);
    0
}

/// Retires a watchdog on drop (the command's own writes are about to
/// happen; a last-instant sample may not abort them).
struct WatchRetire(std::sync::Arc<Watch>);

impl Drop for WatchRetire {
    fn drop(&mut self) {
        self.0.finish();
    }
}

/// Run the half-open range `lo..hi` over `threads` workers (X5-R13
/// (d)). Each seed is independent (S-DET); the records come back sorted
/// by seed.
pub fn run_range(lo: u64, hi: u64, mutant: Option<Mutant>, threads: usize) -> Vec<SeedRec> {
    let shared: std::sync::Arc<std::sync::Mutex<Vec<SeedRec>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    run_range_into(&shared, lo, hi, mutant, threads, None);
    let mut recs = shared.lock().map(|m| m.clone()).unwrap_or_default();
    recs.sort_unstable_by_key(|r| r.seed);
    recs
}

/// The dispatch loop behind [`run_range`]: workers pull seeds from the
/// half-open range `lo..hi` (X5-R13 (d)), push records into `shared`,
/// and honor the watchdog's stop flag (X5-R13 (c): a tripped watchdog
/// stops dispatch — no worker takes a new seed; the abort itself is the
/// watchdog's). Progress lines print every 500 completions with the
/// latest RSS sample, `rss=<MiB>`.
pub fn run_range_into(
    shared: &std::sync::Arc<std::sync::Mutex<Vec<SeedRec>>>,
    lo: u64,
    hi: u64,
    mutant: Option<Mutant>,
    threads: usize,
    watch: Option<(&Watch, &Dispatch)>,
) {
    let next = AtomicU64::new(lo);
    let done = AtomicU64::new(0);
    let total = seed_count(lo, hi);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    if let Some((w, _)) = watch
                        && w.stop()
                    {
                        // The watchdog tripped: stop dispatch. In-flight
                        // seeds finish (the livelock budget bounds them),
                        // their records land, and the watchdog's abort
                        // path takes the process.
                        break;
                    }
                    let s = next.fetch_add(1, Ordering::SeqCst);
                    if s >= hi {
                        break;
                    }
                    if let Some((_, d)) = watch {
                        d.taken.fetch_add(1, Ordering::SeqCst);
                    }
                    let rec = workload::run_seed(s, mutant);
                    if let Ok(mut o) = shared.lock() {
                        o.push(rec);
                    }
                    let d = done.fetch_add(1, Ordering::SeqCst) + 1;
                    if let Some((_, disp)) = watch {
                        disp.done.store(d, Ordering::SeqCst);
                    }
                    if d.is_multiple_of(500) {
                        let rss = match watch {
                            Some((w, _)) if w.rss() > 0 => w.rss(),
                            _ => rss_mib().unwrap_or(0),
                        };
                        eprintln!("e4-campaign: {d}/{total} seeds done rss={rss}");
                    }
                }
            });
        }
    });
}

/// The livelock count inside `DEADLOCK` (X5-R13's RESULT item 2): seeds
/// with at least one livelocked round (every one of them also carries
/// the seed `deadlock` flag, so the counts nest by construction).
pub fn livelock_seeds(recs: &[SeedRec]) -> u64 {
    recs.iter()
        .filter(|r| r.rounds.iter().any(|x| x.livelock))
        .count() as u64
}

/// **X5-R15 (a) (P ruling nv3-X-5c-esc-3): the pass-fold line.** Its
/// counts come from the pass fold and the seed-level
/// `defect`/`deadlock`/`panic`/`baton` flags — never from O-1/O-2/O-3
/// text presence: the Work 7 line's `O1_FAIL` counts every record that
/// carries an O-1 text, which in d2c8bba7 included 1,315 MP
/// recovery-`Err` rounds that **pass** under the frozen fold (they
/// carry the Err branch's default O-1(a) text). A seed fails the fold
/// when any round fails it, any twin round fails it, or a seed-level
/// flag (defect/deadlock/panic/baton) is set; RESULT item 7's n is
/// `FAILING_SEEDS` (validated against d2c8bba7: 1,598 — the ruling's
/// corrected count, not the 1,560 the README's first record carried).
pub fn fold_line(recs: &[SeedRec]) -> String {
    let mut failing_seeds = 0u64;
    for r in recs {
        let round_fail = r.rounds.iter().any(|x| !x.pass);
        let twin_fail = r.twin.iter().any(|t| !t.pass);
        if !r.passes() || twin_fail || round_fail {
            failing_seeds += 1;
        }
    }
    let failing_rounds = recs
        .iter()
        .flat_map(|r| r.rounds.iter())
        .filter(|x| !x.pass)
        .count() as u64;
    let twin_failing_rounds = recs
        .iter()
        .flat_map(|r| r.twin.iter())
        .filter(|t| !t.pass)
        .count() as u64;
    let defect_seeds = recs.iter().filter(|r| r.defect.is_some()).count() as u64;
    format!(
        "E4 FOLD FAILING_SEEDS={failing_seeds} FAILING_ROUNDS={failing_rounds} \
         TWIN_FAILING_ROUNDS={twin_failing_rounds} DEFECT_SEEDS={defect_seeds}"
    )
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
    // X5-R15 (a): the pass-fold line after the Work 7 line, unchanged
    // in form ("Keep the Work 7 line exactly as it is. After it, print
    // one more line").
    println!("{}", fold_line(recs));
}

// ---- verify ----

/// X5-R15 (b): the seed-set check, pure — exactly `0..CAMPAIGN_SEEDS-1`
/// in order, nothing missing, nothing extra.
pub fn verify_seed_set_ok(recs: &[SeedRec]) -> bool {
    recs.len() == CAMPAIGN_SEEDS as usize
        && recs.first().map(|r| r.seed) == Some(0)
        && recs.last().map(|r| r.seed) == Some(CAMPAIGN_SEEDS - 1)
        && !recs.windows(2).any(|w| w[0].seed + 1 != w[1].seed)
}

/// X5-R15 (b): the digest comparison, pure — the count and the list of
/// non-reproducing seeds (the report prints the list; the re-run itself
/// is the command's job).
pub fn verify_digest_report(recs: &[SeedRec], rerun: &[SeedRec]) -> (u64, Vec<u64>) {
    let mut bad_list = Vec::new();
    for (a, b) in recs.iter().zip(rerun.iter()) {
        if a.seed != b.seed || a.digest != b.digest {
            bad_list.push(a.seed);
        }
    }
    (bad_list.len() as u64, bad_list)
}

/// X5-R15 (b): the exit decision, pure and **at the end** — every check
/// has already been reported when this runs. Non-zero on a seed-set
/// failure or a digest failure always; on a missed quota only without
/// `--no-quotas` (the flag exists for X-5c-F1's Acceptance; X-5c's own
/// keeps plain `verify`).
pub fn verify_exit_code(
    seed_set_ok: bool,
    digest_bad: u64,
    quota_missed: u64,
    no_quotas: bool,
) -> i32 {
    if !seed_set_ok {
        return 2;
    }
    if digest_bad > 0 {
        return 2;
    }
    if quota_missed > 0 && !no_quotas {
        return 2;
    }
    0
}

fn cmd_verify(args: &[String]) -> i32 {
    // X5-R15 (b) (P ruling nv3-X-5c-esc-3): `verify` reports everything
    // before it exits — the seed-set check, the digest check (with the
    // list of non-reproducing seeds) and the full quota table all
    // print, whatever failed, and the exit code is decided at the end.
    // `--no-quotas` checks the seed set and the digests and prints the
    // quota table without failing on it; it exists only for X-5c-F1's
    // Acceptance (X-5c's own keeps plain `verify`).
    let no_quotas = args.iter().any(|a| a == "--no-quotas");
    let Some(path) = parse_results(args) else {
        eprintln!("e4-campaign verify: --results is required");
        return 2;
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "e4-campaign verify: cannot read {path}: {e}\n\
                 \x20(no campaign results there yet: run `run --seeds 0..10000 --threads N\n\
                 \x20 --out <path>` first; see the crate README)"
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
    // X5-R15 (b): the exit code is decided once, at the end, after all
    // three checks have been reported (see `verify_exit_code` below).
    // 1. The seed set is exactly the campaign's (0..9999).
    let n = recs.len();
    let seed_set_ok = verify_seed_set_ok(&recs);
    if !seed_set_ok {
        eprintln!(
            "e4-campaign verify: the seed set is not exactly 0..{} ({} records)",
            CAMPAIGN_SEEDS - 1,
            n
        );
    }
    // 2. The digests reproduce. A re-run of every seed in the file.
    // X5-R13 (b) bounds the workers at 16 and digests are independent
    // of the thread count (pinned), so the re-run defaults to 16: at
    // the card's 10,000 seeds a 4-thread re-run would not fit a
    // 60-minute command budget.
    let threads = parse_threads(args).max(16);
    let rss = rss_mib().unwrap_or(0);
    eprintln!("e4-campaign verify: re-running {n} seeds at {threads} threads rss={rss}");
    // X5-R13 (c): the watchdog rides with the re-run (no partial sink —
    // a `.partial` file is diagnostic only, and the ruling names it for
    // the record-writing commands; a trip still stops dispatch, prints
    // the abort line and exits 3).
    let watch = std::sync::Arc::new(Watch::new(mem_limit_mib()));
    let dispatch = std::sync::Arc::new(Dispatch::new(CAMPAIGN_SEEDS));
    let _wd = start_watchdog(
        std::sync::Arc::clone(&watch),
        std::sync::Arc::clone(&dispatch),
        None,
    );
    let shared: std::sync::Arc<std::sync::Mutex<Vec<SeedRec>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    run_range_into(
        &shared,
        0,
        CAMPAIGN_SEEDS,
        None,
        threads,
        Some((&watch, &dispatch)),
    );
    let _retire = WatchRetire(std::sync::Arc::clone(&watch));
    let mut rerun = shared.lock().map(|m| m.clone()).unwrap_or_default();
    rerun.sort_unstable_by_key(|r| r.seed);
    let (bad, bad_list) = verify_digest_report(&recs, &rerun);
    if bad > 0 {
        for seed in bad_list.iter().take(10) {
            let a = recs.iter().find(|r| r.seed == *seed);
            let b = rerun.iter().find(|r| r.seed == *seed);
            if let (Some(a), Some(b)) = (a, b) {
                eprintln!(
                    "e4-campaign verify: seed {} did not reproduce ({} vs {})",
                    a.seed, a.digest, b.digest
                );
            }
        }
        eprintln!(
            "e4-campaign verify: {bad} seeds did not reproduce: {}",
            if bad_list.len() <= 32 {
                format!("{bad_list:?}")
            } else {
                format!("{}.. (first 32 shown)", bad_list.len())
            }
        );
    }
    // 3. Every 5.6 quota is met (and Work 5.3's sizing assertion holds:
    // its row's achieved counts **misses**, which must be zero). The
    // table always prints; with `--no-quotas` a miss is reported but
    // does not fail the exit.
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
    // **SIZING-C (card X-5c-F4-F1 step 4; ESCALATE 2): one INFO line,
    // a diagnostic pending OQ-1 — never an exit input.** Crash rounds
    // that cut a ≥ 4·F interval (the crash fired with the open interval
    // already at 4·F, or an open checkpoint whose folded interval was
    // ≥ 4·F), by class.
    {
        let mut by_class = [("C", 0u64), ("MP", 0u64), ("MW", 0u64)];
        for rec in recs.iter() {
            if let Some((_, n)) = by_class.iter_mut().find(|(c, _)| *c == rec.class) {
                *n += rec.rounds.iter().filter(|r| r.szc).count() as u64;
            }
        }
        println!(
            "INFO SIZING-C crash rounds cutting a >= 4*F interval: C {} MP {} MW {}",
            by_class[0].1, by_class[1].1, by_class[2].1
        );
    }
    if missed > 0 && !no_quotas {
        eprintln!("e4-campaign verify: {missed} quotas missed");
    }
    if missed > 0 && no_quotas {
        // X5-R15 (b): `--no-quotas` prints the table without failing
        // on it (the digests and the seed set still do).
        eprintln!("e4-campaign verify: {missed} quotas missed (--no-quotas: not failing the exit)");
    }
    // X5-R15 (b): every check has now been reported; the exit code is
    // decided once, at the end, from all three outcomes together.
    let exit = verify_exit_code(seed_set_ok, bad, missed, no_quotas);
    if exit == 0 {
        if no_quotas {
            println!(
                "VERIFY OK (--no-quotas) seeds={n} digests reproduce; the quota table is printed above without judging it"
            );
        } else {
            println!("VERIFY OK seeds={n} digests reproduce, every quota met");
        }
    } else {
        println!("VERIFY FAILED seeds={n} (all three checks reported above; exit {exit})");
    }
    exit
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
    let (lo, hi) = parse_seeds(args, MUTANT_SEEDS);
    let threads = parse_threads(args).max(4);
    // X5-R13 (d): the range is half-open (`0..500` is 500 seeds, the
    // card's sweep form); `5..5` is empty, an error.
    if lo >= hi {
        eprintln!("e4-campaign mutants: empty seed range {lo}..{hi}");
        return 2;
    }
    // X5-R13 (c): the watchdog rides with the whole sweep; its partial
    // sink holds the completed mutant records (each mutant's seed sweep
    // feeds the dispatch counters, so the abort line names the sweep in
    // flight when the trip came).
    let watch = std::sync::Arc::new(Watch::new(mem_limit_mib()));
    let dispatch = std::sync::Arc::new(Dispatch::new(seed_count(lo, hi)));
    let mut recs = Vec::new();
    let shared: std::sync::Arc<std::sync::Mutex<Vec<MutantRec>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = std::sync::Arc::new(PartialSink::Mutants {
        path: format!("{out}.partial"),
        recs: std::sync::Arc::clone(&shared),
    });
    let _wd = start_watchdog(
        std::sync::Arc::clone(&watch),
        std::sync::Arc::clone(&dispatch),
        Some(sink),
    );
    for (m, test) in directed_arms::TABLE {
        let (rec, _) = sweep_mutant(*m, test, lo, hi, threads, (&watch, &dispatch), &shared);
        recs.push(rec);
    }
    let _retire = WatchRetire(std::sync::Arc::clone(&watch));
    let json = emit_mutants(&recs);
    if let Err(e) = std::fs::write(&out, json) {
        eprintln!("e4-campaign mutants: cannot write {out}: {e}");
        return 2;
    }
    for r in &recs {
        let rss = watch.rss();
        println!(
            "MUTANT {:<28} first={:?} how={:?} oracle={:.3} monitor={:.3} directed={:?}{} rss={rss}",
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
/// (X-5b's arm re-run), then the campaign column. The completed record
/// also lands in `shared_mutants` (the watchdog's partial sink, X5-R13
/// (c)); `watch`/`dispatch` ride with the seed sweep.
pub fn sweep_mutant(
    m: Mutant,
    test: &'static str,
    lo: u64,
    hi: u64,
    threads: usize,
    watch: (&Watch, &Dispatch),
    shared_mutants: &std::sync::Arc<std::sync::Mutex<Vec<MutantRec>>>,
) -> (MutantRec, Vec<SeedRec>) {
    let arm = directed_arms::run_arm(m);
    let (encoded, caught) = match arm {
        Some(r) => (true, Some(r)),
        None => (false, None),
    };
    // Each mutant's sweep restarts the dispatch counters (the abort
    // line names the sweep in flight).
    watch.1.taken.store(0, Ordering::SeqCst);
    watch.1.done.store(0, Ordering::SeqCst);
    let shared: std::sync::Arc<std::sync::Mutex<Vec<SeedRec>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    run_range_into(&shared, lo, hi, Some(m), threads, Some(watch));
    let mut recs = shared.lock().map(|m| m.clone()).unwrap_or_default();
    recs.sort_unstable_by_key(|r| r.seed);
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
    let rec = MutantRec {
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
    };
    if let Ok(mut m) = shared_mutants.lock() {
        m.push(rec.clone());
    }
    (rec, recs)
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
    // X5-R13 (c): a process-level watchdog for the re-runs too (no
    // dispatch to stop and no partial sink — its single-seed calls are
    // bounded by the livelock budget; a trip still prints the abort
    // line and exits 3).
    let watch = std::sync::Arc::new(Watch::new(mem_limit_mib()));
    let dispatch = std::sync::Arc::new(Dispatch::new(0));
    let _wd = start_watchdog(
        std::sync::Arc::clone(&watch),
        std::sync::Arc::clone(&dispatch),
        None,
    );
    let _retire = WatchRetire(std::sync::Arc::clone(&watch));
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
            wal_io_fail: None,
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
                    wal_io_fail: None,
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
                "{{\"crash\":{},\"hook\":{},\"fired\":{},\"nested\":{},\"lost\":{},\"mis\":{},\"torn\":{},\"lo\":{},\"hi\":{},\"c\":{},\"frontier\":{},\"gcw\":{},\"o1\":{},\"o1f\":{},\"o2\":{},\"o3\":{},\"served\":{},\"sk\":{},\"ackl\":{},\"ackls\":{},\"mrb\":{},\"rlen\":{},\"o1dl\":{},\"r6\":{},\"r32\":{},\"skx\":{},\"smx\":{},\"pass_na\":{},\"ack_over\":[{}],\"cover_err\":{},\"x33h\":{},\"x33a\":{},\"x33x\":{},\"reuse\":{},\"cxx\":{},\"pass\":{},\"dl\":{},\"ll\":{},\"lls\":{},\"pa\":{},\"bt\":{},\"terr\":{},\"cwin\":{},\"szc\":{},\"q4sp\":{},\"q4mg\":{},\"q\":[{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}]}}",
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
                x.served_k,
                x.acked_lost,
                x.acked_lost_syncoff,
                x.meta_rollback,
                x.r_len,
                x.o1_detected_loss,
                x.r6_round,
                x.r32_round,
                x.skx,
                x.smx,
                x.pass_na
                    .map(|b| b.to_string())
                    .unwrap_or_else(|| "null".into()),
                x.ack_over
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                x.cover_err,
                x.x33h,
                x.x33a,
                x.x33x,
                x.reuse_on_dropping,
                x.client_xx001,
                x.pass,
                x.deadlock,
                x.livelock,
                esc(&x.livelock_site),
                x.panic,
                x.baton,
                x.task_error.is_some(),
                esc(x.cwin.unwrap_or("u")),
                u8::from(x.szc),
                x.q4sp,
                x.q4mg,
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
            "sk" => x.served_k = j.boolv()?,
            "ackl" => x.acked_lost = j.int()?,
            "ackls" => x.acked_lost_syncoff = j.int()?,
            "mrb" => x.meta_rollback = j.boolv()?,
            "rlen" => x.r_len = j.int()? as usize,
            "o1dl" => x.o1_detected_loss = j.int()? as u32,
            "r6" => x.r6_round = j.boolv()?,
            "r32" => x.r32_round = j.boolv()?,
            "skx" => x.skx = j.boolv()?,
            "smx" => x.smx = j.boolv()?,
            "pass_na" => x.pass_na = j.opt_bool()?,
            "cover_err" => x.cover_err = j.boolv()?,
            "x33h" => x.x33h = j.num()? as u64,
            "x33a" => x.x33a = j.num()? as u64,
            "x33x" => x.x33x = j.boolv()?,
            "reuse" => x.reuse_on_dropping = j.boolv()?,
            "ack_over" => {
                j.eat(b'[')?;
                if !j.try_eat(b']') {
                    loop {
                        let t = j.int()?;
                        x.ack_over.push(t);
                        if j.try_eat(b',') {
                            continue;
                        }
                        j.eat(b']')?;
                        break;
                    }
                }
            }
            "cxx" => x.client_xx001 = j.int()? as u32,
            "pass" => x.pass = j.boolv()?,
            "dl" => x.deadlock = j.boolv()?,
            "ll" => x.livelock = j.boolv()?,
            "lls" => x.livelock_site = j.string()?,
            "pa" => x.panic = j.boolv()?,
            "bt" => x.baton = j.boolv()?,
            "terr" => {
                let b = j.boolv()?;
                x.task_error = b.then(|| "task error (see digest)".to_owned());
            }
            "szc" => {
                // SIZING-C (step 4): the crash-cut diagnostic.
                x.szc = j.int()? != 0;
            }
            "q4sp" => {
                // X-5c-F4-F1-F1 step 1: the Q4 halves (splits inside the
                // crash-time checkpoint window). Absent in older files,
                // where it defaults to 0.
                x.q4sp = j.int()?;
            }
            "q4mg" => {
                // The same for merges.
                x.q4mg = j.int()?;
            }
            "cwin" => {
                // X5-R38: the window pick of record ("u" = the plain
                // uniform point / a targeted round).
                x.cwin = match j.string()?.as_str() {
                    "w11" => Some("w11"),
                    "w12" => Some("w12"),
                    "w11-" => Some("w11-"),
                    "w12-" => Some("w12-"),
                    _ => None,
                };
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

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::atomic::Ordering;

    /// X5-R13 (c)'s ruled pin: the trip check is a function of
    /// `(rss, limit)`, unit-tested on both sides of the limit — at the
    /// limit it holds (the trip is strictly above), one MiB over it
    /// trips, and the degenerate corners are stable.
    #[test]
    fn r13c_mem_trip_both_sides_of_the_limit() {
        const LIMIT: u64 = 20_480;
        assert!(
            !mem_trip(LIMIT, LIMIT),
            "at the limit the process lives (strictly above trips)"
        );
        assert!(mem_trip(LIMIT + 1, LIMIT), "one MiB above the limit trips");
        assert!(!mem_trip(0, LIMIT), "an empty RSS never trips");
        assert!(!mem_trip(0, 0), "limit 0: nothing is above 0");
        assert!(mem_trip(1, 0), "any resident page over limit 0 trips");
        // The default limit is P's ruling number.
        assert_eq!(E4_MEM_LIMIT_MIB_DEFAULT, 20_480);
    }

    /// The host probe reads a real `VmRSS` on Linux (the watchdog's
    /// input; when absent the watchdog never trips — fail-open by
    /// design, with the `ulimit` backstop behind it).
    #[test]
    fn r13c_rss_probe_reads_proc_status() {
        let rss = rss_mib();
        assert!(rss.is_some(), "/proc/self/status must carry VmRSS here");
        let v = rss.unwrap_or(0);
        assert!(v > 0, "a resident process has nonzero VmRSS ({v})");
    }

    /// A tripped watchdog stops dispatch before the first seed: no
    /// worker takes a seed, nothing runs, and the loop returns (never a
    /// hang) — the abort actions themselves (partial write, abort line,
    /// exit 3) belong to the watchdog thread, which the CLI starts only
    /// in its own process.
    #[test]
    fn r13c_tripped_watch_stops_dispatch_before_the_first_seed() {
        let watch = Watch::new(20_480);
        watch.force_trip(99_999);
        assert_eq!(watch.tripped(), Some(99_999));
        assert!(watch.stop(), "dispatch must stop after the trip");
        let dispatch = Dispatch::new(4);
        let shared: std::sync::Arc<std::sync::Mutex<Vec<SeedRec>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        run_range_into(&shared, 0, 3, None, 2, Some((&watch, &dispatch)));
        let n = shared.lock().map(|m| m.len()).unwrap_or(usize::MAX);
        assert_eq!(n, 0, "a tripped watchdog must leave no seed run ({n})");
        assert_eq!(dispatch.taken.load(Ordering::SeqCst), 0);
    }

    /// An untripped watchdog leaves the dispatch untouched: the same
    /// range runs to completion beside it, byte-identical to a run
    /// without one (the watchdog never feeds a record or a digest).
    #[test]
    fn r13c_untripped_watch_changes_nothing() {
        let watch = Watch::new(20_480);
        let dispatch = Dispatch::new(2);
        let shared: std::sync::Arc<std::sync::Mutex<Vec<SeedRec>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        run_range_into(&shared, 10, 12, None, 2, Some((&watch, &dispatch)));
        let with_watch = run_range(10, 12, None, 2);
        let mut got = shared.lock().map(|m| m.clone()).unwrap_or_default();
        got.sort_unstable_by_key(|r| r.seed);
        assert_eq!(got.len(), 2);
        for (a, b) in got.iter().zip(with_watch.iter()) {
            assert_eq!(a.digest, b.digest, "seed {} digest", a.seed);
        }
        assert!(watch.tripped().is_none(), "never tripped at the default");
        assert!(!watch.stop());
    }

    /// The partial writer records the completed records, sorted by seed
    /// (X5-R13 (c): `<out>.partial` is diagnostic only, but it is
    /// well-formed — the campaign's own emitter and parser).
    #[test]
    fn r13c_partial_file_round_trips() {
        let mut recs: Vec<SeedRec> = (0..3u64).map(|s| workload::run_seed(s, None)).collect();
        // Deliberately unsorted input, with seeds out of order.
        recs.reverse();
        let p = "/tmp/opencode/e4-r13c-partial-test.json";
        write_partial_seeds(p, &recs).expect("write partial");
        let text = std::fs::read_to_string(p).expect("read partial");
        let back = parse_campaign(&text).expect("parse partial");
        assert_eq!(
            back.iter().map(|r| r.seed).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "the partial file is sorted by seed"
        );
        let mut sorted_in = recs.clone();
        sorted_in.sort_unstable_by_key(|r| r.seed);
        for (a, b) in sorted_in.iter().zip(back.iter()) {
            assert_eq!(a.digest, b.digest, "seed {} digest", a.seed);
        }
    }

    /// **X-5c-F2 step 1 (c) pin (gate mutant M13): the FOLD counts a
    /// twin-only failure.** A literal `SeedRec` whose primary rounds
    /// all pass, with one failing twin round, gives
    /// `FAILING_SEEDS=1 FAILING_ROUNDS=0 TWIN_FAILING_ROUNDS=1` —
    /// dropping the twin term from `fold_line` (M13) would report
    /// `FAILING_SEEDS=0` and let a Work 2.5 failure hide inside a
    /// passing-looking campaign.
    #[test]
    fn f1f2c_fold_counts_a_twin_only_failure() {
        let recs = vec![SeedRec {
            seed: 1,
            rounds: vec![workload::RoundRec {
                pass: true,
                ..workload::RoundRec::default()
            }],
            twin: vec![workload::TwinRound {
                pass: false,
                o1_fail: Some("O-1(b): scan mismatch".to_owned()),
            }],
            ..SeedRec::default()
        }];
        let line = fold_line(&recs);
        assert_eq!(
            line, "E4 FOLD FAILING_SEEDS=1 FAILING_ROUNDS=0 TWIN_FAILING_ROUNDS=1 DEFECT_SEEDS=0",
            "a seed failing only through its twin must count in FAILING_SEEDS"
        );
    }

    /// X5-R13 (d)'s pins: `0..10000` is 10,000 seeds; `5..5` is empty,
    /// which is an error at the command level; the sweep default is the
    /// card's `0..500`; and the dispatch loop's bounds are half-open
    /// (`run_range(0, 3)` runs exactly seeds 0, 1, 2).
    #[test]
    fn r13d_seed_range_is_half_open() {
        let args: Vec<String> = ["--seeds", "0..10000"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (lo, hi) = parse_seeds(&args, CAMPAIGN_SEEDS);
        assert_eq!((lo, hi), (0, CAMPAIGN_SEEDS));
        assert_eq!(seed_count(lo, hi), 10_000, "0..10000 is 10,000 seeds");
        let args: Vec<String> = ["--seeds", "5..5"].iter().map(|s| s.to_string()).collect();
        let (lo, hi) = parse_seeds(&args, CAMPAIGN_SEEDS);
        assert_eq!(seed_count(lo, hi), 0, "5..5 is empty");
        // The command level rejects the empty range without running.
        let run_args: Vec<String> = ["--seeds", "5..5", "--out", "/tmp/opencode/x5c-d-err.json"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(cmd_run(&run_args), 2, "the empty range is an error");
        // The sweep's default is the card's 0..500.
        let empty: Vec<String> = Vec::new();
        let (lo, hi) = parse_seeds(&empty, MUTANT_SEEDS);
        assert_eq!(seed_count(lo, hi), 500);
        // The dispatch bounds are half-open end to end.
        let recs = run_range(0, 3, None, 2);
        assert_eq!(
            recs.iter().map(|r| r.seed).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "run_range(0, 3) runs exactly three seeds"
        );
    }
}
