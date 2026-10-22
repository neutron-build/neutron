//! The `e1-bench` binary (card X-1b Work 1): parse the flags, verify the
//! arms (card Work 3), run the workload matrix, and print one JSON
//! report (methodology of Work 3: release build, 3 runs per workload
//! with median + min/max, nothing pinned, `lscpu`/`nproc`/`rustc -V`/
//! git commit recorded).
//!
//! Usage:
//! ```text
//! e1-bench [--quick] [--verify] [--no-verify] [--arm a,b,c] [--only W1,W2,W3,W4,W5,W6] [--out FILE]
//! ```
//! - `--quick` scales every workload down 100× (the acceptance shape).
//! - `--verify` runs the arms' behaviour suite before measuring; a full
//!   run always verifies (`B-ABORT`), `--verify` extends that to quick
//!   runs — the acceptance command (`--verify --quick`) therefore
//!   verifies **and** runs the quick matrix. `--no-verify` skips it for
//!   focused re-measurement invocations of a campaign that already
//!   verified.
//! - `--arm` / `--only` select arms and workloads (default: all arms,
//!   all workloads; arm (a) exists only in W1).
//!
//! Exit codes: 0 success, 1 usage, 2 an arm failed verification,
//! 3 a workload failed.

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use e1_arm_b::ArmB;
use e1_arm_c::ArmC;

use e1_bench::json::{self, Json, obj};
use e1_bench::util;
use e1_bench::{Config, arma, verify, w1, w2, w3, w5, w6};

/// A run of one workload on one arm, collected for the summary.
struct Entry {
    id: String,
    arm: String,
    report: Json,
}

fn main() {
    if let Err(code) = real_main() {
        std::process::exit(code);
    }
}

/// The real main, returning a process exit code (0 success).
fn real_main() -> Result<(), i32> {
    let mut quick = false;
    let mut verify_flag = false;
    let mut no_verify = false;
    let mut arms = String::from("a,b,c");
    let mut only = String::from("W1,W2,W3,W4,W5,W6");
    let mut out_path: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--quick" => quick = true,
            "--verify" => verify_flag = true,
            "--no-verify" => no_verify = true,
            "--arm" => match args.next() {
                Some(v) => arms = v,
                None => return usage("--arm needs a value"),
            },
            "--only" => match args.next() {
                Some(v) => only = v,
                None => return usage("--only needs a value"),
            },
            "--out" => match args.next() {
                Some(v) => out_path = Some(PathBuf::from(v)),
                None => return usage("--out needs a value"),
            },
            other => return usage(&format!("unknown argument {other}")),
        }
    }

    let cfg = if quick {
        Config::quick()
    } else {
        Config::full()
    };
    let want_arms: Vec<String> = arms
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    let want_workloads: Vec<String> = only
        .split(',')
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if want_workloads
        .iter()
        .any(|w| !["W1", "W2", "W3", "W4", "W5", "W6"].contains(&w.as_str()))
    {
        let bad = want_workloads.join(",");
        return usage(&format!("unknown workload in {bad}"));
    }

    let root: PathBuf = spill_root();
    let mut report = obj(vec![]);
    push_meta(&mut report, &cfg, quick);

    // Verification (B-ABORT): every full run verifies; --verify extends
    // it to quick runs (the acceptance command). --no-verify skips it
    // for focused re-measurement invocations of a campaign that already
    // verified (the suite result does not change between invocations of
    // one build).
    let must_verify = (verify_flag || !quick) && !no_verify;
    if must_verify {
        eprintln!("verifying arms (e1_mvcc::suite::run_all) ...");
        let v = match verify::run(&root) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("verification failed to run: {e}");
                return Err(2);
            }
        };
        let passed = matches!(
            (
                json::field(&v, "arm_b_passed"),
                json::field(&v, "arm_c_passed")
            ),
            (Some(Json::Bool(true)), Some(Json::Bool(true)))
        );
        push(&mut report, "verify", v);
        if !passed {
            eprintln!("an arm failed the behaviour suite; aborting the bench");
            print_report(&report, out_path.as_deref());
            return Err(2);
        }
        eprintln!("both arms passed");
    }

    let mut entries: Vec<Entry> = Vec::new();
    let make_b = |pool: usize| Ok(ArmB::new(pool, cfg.page_size));
    let engine_n = AtomicU64::new(0);
    let make_c = |pool: usize| {
        let d = root.join(format!(
            "armc-{}-{}",
            std::process::id(),
            engine_n.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&d).map_err(|e| e1_bench::BenchError::new("arm c spill dir", e))?;
        Ok(ArmC::new(pool, cfg.page_size, cfg.undo_budget_bytes, &d))
    };

    let sel = |w: &str| want_workloads.iter().any(|x| x == w);
    let arm_b = want_arms.iter().any(|a| a == "b");
    let arm_c = want_arms.iter().any(|a| a == "c");
    let arm_a = want_arms.iter().any(|a| a == "a");

    for run in 0..cfg.runs {
        if sel("W1") {
            for threads in &cfg.w1_threads {
                if arm_b {
                    let r = w1::run(&make_b, &cfg, run, *threads)
                        .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                    record(
                        &mut entries,
                        format!("W1[t={threads}]"),
                        "b",
                        run,
                        with_arm(r, "b (ts-in-key)"),
                    );
                }
                if arm_c {
                    let r = w1::run(&make_c, &cfg, run, *threads)
                        .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                    record(
                        &mut entries,
                        format!("W1[t={threads}]"),
                        "c",
                        run,
                        with_arm(r, "c (newest+undo)"),
                    );
                }
                if arm_a {
                    let r = arma::w1(&cfg, run, *threads)
                        .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                    record(&mut entries, format!("W1[t={threads}]"), "a", run, r);
                }
            }
        }
        if sel("W2") {
            for writers in &cfg.w2_writer_counts {
                if arm_b {
                    let r = w2::run(&make_b, &cfg, run, *writers)
                        .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                    record(
                        &mut entries,
                        format!("W2[w={writers}]"),
                        "b",
                        run,
                        with_arm(r, "b (ts-in-key)"),
                    );
                }
                if arm_c {
                    let r = w2::run(&make_c, &cfg, run, *writers)
                        .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                    record(
                        &mut entries,
                        format!("W2[w={writers}]"),
                        "c",
                        run,
                        with_arm(r, "c (newest+undo)"),
                    );
                }
            }
        }
        if sel("W3") || sel("W4") {
            if arm_b {
                let rs = w3::run(&make_b, &cfg, run)
                    .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                for r in rs {
                    let id = workload_id(&r);
                    let wl = id_letter(&r);
                    if sel(&wl) {
                        record(&mut entries, id, "b", run, with_arm(r, "b (ts-in-key)"));
                    }
                }
            }
            if arm_c {
                let rs = w3::run(&make_c, &cfg, run)
                    .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                for r in rs {
                    let id = workload_id(&r);
                    let wl = id_letter(&r);
                    if sel(&wl) {
                        record(&mut entries, id, "c", run, with_arm(r, "c (newest+undo)"));
                    }
                }
            }
        }
        if sel("W5") {
            if arm_b {
                let r = w5::run(&make_b, &cfg, run)
                    .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                record(
                    &mut entries,
                    "W5".into(),
                    "b",
                    run,
                    with_arm(r, "b (ts-in-key)"),
                );
            }
            if arm_c {
                let r = w5::run(&make_c, &cfg, run)
                    .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                record(
                    &mut entries,
                    "W5".into(),
                    "c",
                    run,
                    with_arm(r, "c (newest+undo)"),
                );
            }
        }
        if sel("W6") {
            if arm_b {
                let r = w6::run(&make_b, &cfg, run)
                    .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                record(
                    &mut entries,
                    "W6".into(),
                    "b",
                    run,
                    with_arm(r, "b (ts-in-key)"),
                );
            }
            if arm_c {
                let r = w6::run(&make_c, &cfg, run)
                    .map_err(|e| fatal(e, &report, out_path.as_deref()))?;
                record(
                    &mut entries,
                    "W6".into(),
                    "c",
                    run,
                    with_arm(r, "c (newest+undo)"),
                );
            }
        }
        eprintln!("run {}/{} complete", run + 1, cfg.runs);
    }

    push(&mut report, "results", summarize(&entries));
    print_report(&report, out_path.as_deref());
    Ok(())
}

/// Record one entry and keep the operator's progress feed alive (a
/// full-scale W1 configuration can run for minutes without one).
fn record(entries: &mut Vec<Entry>, id: String, arm: &str, run: u32, report: Json) {
    eprintln!("{id} {arm} run {} recorded", run + 1);
    entries.push(Entry {
        id,
        arm: arm.to_string(),
        report,
    });
}

/// Print the usage line and return exit code 1.
fn usage(msg: &str) -> Result<(), i32> {
    eprintln!(
        "{msg}\nusage: e1-bench [--quick] [--verify] [--no-verify] [--arm a,b,c] \
         [--only W1,W2,W3,W4,W5,W6] [--out FILE]"
    );
    Err(1)
}

/// Report the workload error, flush what is known, and stop (exit 3).
fn fatal(e: e1_bench::BenchError, report: &Json, out: Option<&std::path::Path>) -> i32 {
    eprintln!("workload failed: {e}");
    let mut full = report.clone();
    // Drop a partial results section (a workload died mid-matrix); the
    // fatal line carries the reason.
    if let Json::Obj(pairs) = &mut full {
        pairs.retain(|(k, _)| k != "results");
        pairs.push(("fatal".into(), Json::Str(e.to_string())));
    }
    print_report(&full, out);
    3
}

/// The scratch root for arm (c) spill files: `/tmp/opencode/e1-bench`
/// when it can be created (the environment's preferred scratch), the
/// system temp dir otherwise.
fn spill_root() -> PathBuf {
    let p = std::path::Path::new("/tmp/opencode").join("e1-bench");
    if std::fs::create_dir_all(&p).is_ok() {
        return p;
    }
    let t = std::env::temp_dir().join("e1-bench");
    let _ = std::fs::create_dir_all(&t);
    t
}

/// Record the methodology metadata (card Work 3: `lscpu` model name,
/// nproc, `rustc -V`, the git commit, plus the run's configuration).
fn push_meta(report: &mut Json, cfg: &Config, quick: bool) {
    let model = std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| {
        s.lines()
            .find(|l| l.starts_with("model name"))
            .and_then(|l| l.split(':').nth(1))
            .map(|v| v.trim().to_string())
    });
    let nproc = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    let rustc = Command::new("rustc")
        .arg("-V")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());
    let git = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    push(
        report,
        "meta",
        obj(vec![
            (
                "cpu_model",
                model.map(Json::Str).unwrap_or(Json::Null),
            ),
            ("nproc", nproc.into()),
            ("rustc", rustc.map(Json::Str).unwrap_or(Json::Null)),
            ("git_commit", git.map(Json::Str).unwrap_or(Json::Null)),
            ("started_unix_s", started.into()),
            ("scale", cfg.scale_tag().into()),
            ("quick", quick.into()),
            ("runs_per_workload", cfg.runs.into()),
            ("page_size", cfg.page_size.into()),
            ("pool_frames_1m", cfg.pool_frames.into()),
            ("pool_frames_10m", cfg.pool_frames_big.into()),
            ("undo_budget_bytes", cfg.undo_budget_bytes.into()),
            ("value_len", cfg.value_len.into()),
            ("pinned", Json::Bool(false)),
            (
                "note",
                "3 runs per workload; median + min/max in each summary; process CPU at CLK_TCK resolution"
                    .into(),
            ),
        ]),
    );
}

/// The `workload` field of a report object ("W1".."W6").
fn id_letter(r: &Json) -> String {
    if let Json::Str(s) = json::field(r, "workload").unwrap_or(Json::Null) {
        s
    } else {
        String::new()
    }
}

/// The identity of a report: its workload letter plus the
/// configuration axis it varies (W1 the thread count, W2 the writer
/// count), used as the summary key.
fn workload_id(r: &Json) -> String {
    let wl = id_letter(r);
    let axis = match (
        wl.as_str(),
        json::field(r, "threads"),
        json::field(r, "writers"),
    ) {
        ("W1", Some(Json::Int(t)), _) => format!("[t={t}]"),
        ("W2", _, Some(Json::Int(w))) => format!("[w={w}]"),
        _ => String::new(),
    };
    format!("{wl}{axis}")
}

/// Prepend the arm field to a report object (arm (a) sets its own).
fn with_arm(mut r: Json, arm: &str) -> Json {
    if let Json::Obj(pairs) = &mut r {
        pairs.insert(0, ("arm".to_string(), Json::Str(arm.to_string())));
    }
    r
}

/// Group the entries by (id, arm) and compute per-key median/min/max
/// across the runs (card Work 3).
fn summarize(entries: &[Entry]) -> Json {
    // A workload's runs interleave with its other configurations' (W1
    // pushes t=1,4,8,16 per run), so groups are collected across the
    // whole matrix by first-seen (id, arm), not by contiguity.
    let mut keys: Vec<(String, String)> = Vec::new();
    for e in entries {
        let k = (e.id.clone(), e.arm.clone());
        if !keys.contains(&k) {
            keys.push(k);
        }
    }
    let mut out: Vec<Json> = Vec::new();
    for (id, arm) in keys {
        let group: Vec<&Entry> = entries
            .iter()
            .filter(|e| e.id == id && e.arm == arm)
            .collect();
        out.push(summarize_group(&id, &arm, &group));
    }
    Json::Arr(out)
}

/// One (id, arm) group's summary: every run plus the per-key
/// median/min/max over numeric fields (non-numeric fields carry the
/// first run's value).
fn summarize_group(id: &str, arm: &str, group: &[&Entry]) -> Json {
    let runs: Vec<Json> = group.iter().map(|e| e.report.clone()).collect();
    let mut summary: Vec<(String, Json)> = Vec::new();
    if let Some(Json::Obj(first)) = runs.first() {
        for (k, v) in first {
            let mut nums: Vec<f64> = Vec::new();
            let mut numeric = true;
            for r in &runs {
                match json::field(r, k) {
                    Some(Json::Int(x)) => nums.push(x as f64),
                    Some(Json::Num(x)) => nums.push(x),
                    _ => {
                        numeric = false;
                        break;
                    }
                }
            }
            if numeric && !nums.is_empty() {
                let mut sorted = nums.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let min = sorted.first().copied().unwrap_or(0.0);
                let max = sorted.last().copied().unwrap_or(0.0);
                let med = util::median(&sorted).unwrap_or(0.0);
                summary.push((
                    k.clone(),
                    obj(vec![
                        ("median", num_json(med)),
                        ("min", num_json(min)),
                        ("max", num_json(max)),
                    ]),
                ));
            } else {
                summary.push((k.clone(), v.clone()));
            }
        }
    }
    obj(vec![
        ("workload", id.into()),
        ("arm", arm.into()),
        ("runs", Json::Arr(runs)),
        ("summary", Json::Obj(summary)),
    ])
}

/// A number that renders as an int when it is one (summaries of ints
/// stay ints).
fn num_json(v: f64) -> Json {
    if v.fract() == 0.0 && v.abs() < 9.0e15 {
        Json::Int(v as i64)
    } else {
        Json::Num(v)
    }
}

/// Push a key into the report object.
fn push(report: &mut Json, key: &str, v: Json) {
    if let Json::Obj(pairs) = report {
        pairs.push((key.to_string(), v));
    }
}

/// Print the report to stdout and, when given, to `out`.
fn print_report(report: &Json, out: Option<&std::path::Path>) {
    let text = report.render();
    println!("{text}");
    if let Some(p) = out
        && let Err(e) = std::fs::write(p, text.as_bytes())
    {
        eprintln!("could not write {}: {e}", p.display());
    }
}
