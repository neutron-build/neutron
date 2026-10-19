//! Row orchestration: containers, drivers, digests, stats, results.
//!
//! Invariants owned here (PLAN-v3 §5.2, card B-0 Work items 2-4):
//! - One command reproduces a row (rule 7): every input the drivers see is
//!   derived from the row definition, the pinned image file and the run
//!   parameters, all recorded in the results JSON (rule 6).
//! - A strict digest mismatch marks the run VOID and stops measurement
//!   (rule 4); the results file keeps the evidence. The decision lives
//!   in [`crate::digest`] (compare → decide → [`apply_digest`]), each
//!   step unit-tested, so the rule cannot silently invert (gate
//!   nv3-B-0-gate-1 defect 3).
//! - Warm runs are aggregated as median + 95% bootstrap CI (rule 5) with
//!   the deterministic seed from [`crate::stats`].
//! - Competitors run rootless under `--network host` with a named volume
//!   and recorded CPU/memory limits (rule 1, card Work item 2), and every
//!   server start uses a *named durability variant*, never an ad-hoc flag
//!   (rule 3).
//! - Host contention is recorded *and* flagged: provenance carries the
//!   load at row start and at results-write time, and an end-of-row
//!   `load1` above the host CPU count — the same comparison the chain
//!   scripts apply at row start — is appended to the row's anomalies
//!   (§5.2 rule 1; escalation nv3-B-0-esc-1, rework round 4 item a).

use crate::digest;
use crate::error::{Error, Result};
use crate::host;
use crate::json::{self, Value};
use crate::params::Params;
use crate::parse::{self, ParsedRun};
use crate::podman;
use crate::report;
use crate::rows::{self, Ready, RowDef, Step, SystemDef, SystemKind};
use crate::stats;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// What a completed row run produced.
pub struct RunOutcome {
    /// Path of the written results JSON.
    pub results_path: PathBuf,
    /// Path of the written (regenerated) Markdown report.
    pub report_path: PathBuf,
    /// True when a strict digest mismatch voided the run.
    pub voided: bool,
}

/// Convert days since the Unix epoch to a civil (y, m, d) date.
/// Howard Hinnant's algorithm; keeps results dirs UTC-stable.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Current UTC date as `YYYY-MM-DD` (dirs are dated, not timestamped, so a
/// re-run of the same day lands in the same results directory).
pub fn utc_date() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Resolve the harness root: the directory holding `systems/`, `workloads/`,
/// `results/`. Derived from this executable's location, with an override
/// for `cargo run` from the workspace root.
pub fn harness_root() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("NBENCH_ROOT") {
        return Ok(PathBuf::from(p));
    }
    let exe = std::env::current_exe().map_err(Error::Io)?;
    // target/debug/nbench -> ../../../ (bench/nbench)
    let mut dir = exe
        .parent()
        .ok_or_else(|| Error::Usage("no exe dir".into()))?
        .to_path_buf();
    for _ in 0..3 {
        dir.pop();
    }
    Ok(dir)
}

fn read_image_pin(root: &Path, system: &SystemDef) -> Result<String> {
    let path = root.join("systems").join(system.name).join("image.txt");
    let text = std::fs::read_to_string(&path).map_err(|e| {
        Error::Usage(format!(
            "cannot read image pin {}: {e} (every system needs a pinned image)",
            path.display()
        ))
    })?;
    let line = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with('#'));
    line.map(|l| l.to_string())
        .filter(|l| l.contains('@'))
        .ok_or_else(|| {
            Error::Usage(format!(
                "{} must contain a repo:tag@sha256:... pin",
                path.display()
            ))
        })
}

fn stop_system(system: &SystemDef) {
    if system.kind != SystemKind::Server {
        return;
    }
    podman::unit_stop(&format!("nbench-{}", system.name));
    podman::try_podman(&["rm", "-f", &system.container()]);
}

fn start_system(root: &Path, system: &SystemDef, image: &str, variant: Option<&str>) -> Result<()> {
    stop_system(system);
    // pgvector needs its extension-creation SQL in the image's init dir;
    // bind-mount the tracked file read-only (the image honours
    // /docker-entrypoint-initdb.d on first initdb only).
    let mut pgvec_init_mount: Option<String> = None;
    if system.name == "pgvector17" {
        let src = root.join("systems").join("pgvector17").join("init");
        if !src.join("00-vector.sql").exists() {
            return Err(Error::Usage(format!(
                "missing pgvector init script {}",
                src.join("00-vector.sql").display()
            )));
        }
        pgvec_init_mount = Some(format!("{}:/docker-entrypoint-initdb.d:ro", src.display()));
    }
    let mut args: Vec<String> = vec![
        "run".into(),
        "--rm".into(),
        "--name".into(),
        system.container(),
        "--network".into(),
        "host".into(),
        "--memory".into(),
        system.mem_limit.into(),
        "--cpus".into(),
        system.cpus.into(),
        "-v".into(),
        format!("{}:{}", system.volume, system.mount),
    ];
    for extra in system.run_extra {
        args.push((*extra).to_string());
    }
    if let Some(mount) = pgvec_init_mount {
        args.push("-v".into());
        args.push(mount);
    }
    args.push(image.to_string());
    for a in system.base_args {
        args.push((*a).to_string());
    }
    if let Some(v) = variant {
        let variant_args = system
            .variants
            .iter()
            .find(|(name, _)| *name == v)
            .map(|(_, args)| args)
            .ok_or_else(|| Error::Usage(format!("unknown variant {v} for {}", system.name)))?;
        for a in variant_args.iter() {
            args.push((*a).to_string());
        }
    }
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    podman::unit_start(&format!("nbench-{}", system.name), &arg_refs)?;

    wait_ready(system, root)
}

/// Shell snippet that pings a RESP server and succeeds only on PONG.
/// Kept pure for a unit test: the probe must carry the server's port
/// (Valkey 6379, Redis 6380 share the host network).
fn resp_ping_probe(cli: &str, port: u16) -> String {
    format!("{cli} -p {port} ping | grep -q PONG")
}

fn wait_ready(system: &SystemDef, _root: &Path) -> Result<()> {
    match system.ready {
        Ready::None => Ok(()),
        Ready::PgIsready { db, port } => {
            let ok = podman::wait_until_exec_ok(
                &system.container(),
                &[
                    "pg_isready",
                    "-U",
                    "postgres",
                    "-d",
                    db,
                    "-p",
                    &port.to_string(),
                ],
                Duration::from_secs(300),
            );
            ok.then_some(()).ok_or_else(|| Error::Command {
                name: format!("pg_isready for {}", system.name),
                status: 1,
                stderr_tail: "not ready after 300s".into(),
            })
        }
        Ready::RespPing { cli, port } => {
            let probe = resp_ping_probe(cli, port);
            let ok = podman::wait_until_exec_ok(
                &system.container(),
                &["sh", "-c", &probe],
                Duration::from_secs(120),
            );
            ok.then_some(()).ok_or_else(|| Error::Command {
                name: format!("ping for {}", system.name),
                status: 1,
                stderr_tail: "not ready after 120s".into(),
            })
        }
        Ready::Http { port } => {
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                if let Ok(resp) = crate::http::request(
                    "127.0.0.1",
                    port,
                    "GET",
                    "/healthz",
                    b"",
                    Duration::from_secs(5),
                ) && resp.status == 200
                {
                    return Ok(());
                }
                if Instant::now() > deadline {
                    return Err(Error::Command {
                        name: format!("healthz for {}", system.name),
                        status: 1,
                        stderr_tail: "not ready after 120s".into(),
                    });
                }
                std::thread::sleep(Duration::from_millis(300));
            }
        }
    }
}

fn series_string(series: &[(String, String)]) -> String {
    series
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Context for one driver execution (keeps the argument list small).
struct ExecCtx<'a> {
    root: &'a Path,
    row: &'a RowDef,
    system: &'a SystemDef,
    image: &'a str,
    params: &'a Params,
}

/// Execute one driver script for one planned execution.
fn exec_driver(
    ctx: &ExecCtx<'_>,
    mode: &str,
    series: &[(String, String)],
    index: usize,
    workdir: &Path,
) -> Result<ParsedRun> {
    let ExecCtx {
        root,
        row,
        system,
        image,
        params,
    } = ctx;
    let workload_dir = root.join("workloads").join(row.id);
    let driver = workload_dir.join("driver.sh");
    if !driver.exists() {
        return Err(Error::Usage(format!("missing driver {}", driver.display())));
    }
    let tag = if series.is_empty() {
        format!("{mode}-{index}")
    } else {
        format!("{}-{}-{index}", series_string(series), mode)
    };
    let log = workdir.join(format!("{tag}.log"));
    let metrics_file = workdir.join(format!("{tag}.metrics.json"));

    let exe = std::env::current_exe().map_err(Error::Io)?;
    let bin_dir = root.join("bin");

    let mut cmd = Command::new("bash");
    cmd.arg(&driver)
        .env("NB_HARNESS", root)
        .env("NB_ROW", row.id)
        .env("NB_SYSTEM", system.name)
        .env("NB_CONTAINER", system.container())
        .env("NB_PORT", system.port().to_string())
        .env("NB_DB", "nbench")
        .env("NB_MODE", mode)
        .env("NB_SERIES", series_string(series))
        .env("NB_RUN_INDEX", index.to_string())
        .env("NB_LOG", &log)
        .env("NB_METRICS", &metrics_file)
        .env("NB_OUT", workdir)
        .env("NB_BIN", &exe)
        .env("NB_IMAGE", image)
        .env("NB_DATA", root.join("data"))
        .env("NBENCH_ROOT", root);
    for (k, v) in params.to_pairs() {
        let key = format!("NB_P_{}", k.to_uppercase());
        cmd.env(key, v);
    }
    let path = std::env::var("PATH").unwrap_or_default();
    cmd.env("PATH", format!("{}:{}", bin_dir.display(), path));

    let out = cmd.output().map_err(Error::Io)?;
    if !out.status.success() {
        return Err(Error::Command {
            name: format!("driver {} [{tag}]", row.id),
            status: out.status.code().unwrap_or(-1),
            stderr_tail: crate::podman::stderr_tail(&out.stderr),
        });
    }
    if mode == "init" || mode == "reinit" {
        // Init/reinit logs describe dataset construction, not a
        // measurement; their evidence is the digest step that follows.
        return Ok(ParsedRun::default());
    }
    parse_result(row, &log, &metrics_file)
}

fn parse_result(row: &RowDef, log: &Path, metrics_file: &Path) -> Result<ParsedRun> {
    if metrics_file.exists() {
        let text = std::fs::read_to_string(metrics_file).map_err(Error::Io)?;
        let v = json::parse(&text)?;
        let mut metrics = Vec::new();
        for (k, val) in v.pairs().unwrap_or(&[]) {
            if let Some(n) = val.as_f64() {
                metrics.push((k.to_string(), n));
            }
        }
        if metrics.is_empty() {
            return Err(Error::Missing(format!(
                "metrics file {} has no numeric metrics",
                metrics_file.display()
            )));
        }
        return Ok(ParsedRun {
            metrics,
            digest: None,
        });
    }
    let text = std::fs::read_to_string(log).map_err(|e| Error::Parse {
        what: "driver log",
        line: format!("cannot read {}: {e}", log.display()),
    })?;
    match row.parser {
        rows::Parser::Pgbench => {
            let mut parsed = parse::parse_pgbench_summary(&text)?;
            // pgbench --log writes per-client files; the driver concatenates
            // them into <log>.txnlog for percentile extraction.
            let extra = PathBuf::from(format!("{}.txnlog", log.display()));
            if extra.exists() {
                let more = parse::parse_pgbench_log_file(&extra)?;
                parsed.metrics.extend(more.metrics);
            }
            Ok(parsed)
        }
        rows::Parser::Sysbench => parse::parse_sysbench(&text),
        rows::Parser::Memtier => parse::parse_memtier(&text),
        rows::Parser::Speedtest1 => parse::parse_speedtest1(&text),
        rows::Parser::VectorSearch => Err(Error::Usage(
            "vector rows must write NB_METRICS json".into(),
        )),
    }
}

/// Context for one digest invocation (keeps the argument list small).
struct DigestCtx<'a> {
    root: &'a Path,
    row: &'a RowDef,
    system: &'a SystemDef,
    params: &'a Params,
    workdir: &'a Path,
}

/// Run the digest script for one phase and return the measured digest.
/// The expectation decision (record / match / mismatch-voids) is
/// [`digest::decide`], applied by [`apply_digest`] — both unit-tested.
fn run_digest(ctx: &DigestCtx<'_>, phase: &str) -> Result<String> {
    let root = ctx.root;
    let row = ctx.row;
    let system = ctx.system;
    let params = ctx.params;
    let workdir = ctx.workdir;
    let script = root.join("workloads").join(row.id).join("digest.sh");
    if !script.exists() {
        return Err(Error::Usage(format!(
            "missing digest script {}",
            script.display()
        )));
    }
    let exe = std::env::current_exe().map_err(Error::Io)?;
    let mut cmd = Command::new("bash");
    cmd.arg(&script)
        .env("NB_HARNESS", root)
        .env("NB_ROW", row.id)
        .env("NB_SYSTEM", system.name)
        .env("NB_CONTAINER", system.container())
        .env("NB_PORT", system.port().to_string())
        .env("NB_DB", "nbench")
        .env("NB_PHASE", phase)
        .env("NB_OUT", workdir)
        .env("NB_BIN", &exe)
        .env("NB_IMAGE", "")
        .env("NBENCH_ROOT", root)
        .env(
            "PATH",
            format!(
                "{}:{}",
                root.join("bin").display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        );
    // digest scripts see the same parameters as drivers (dataset shape
    // determines the digest)
    for (k, v) in params.to_pairs() {
        cmd.env(format!("NB_P_{}", k.to_uppercase()), v);
    }
    let out = cmd.output().map_err(Error::Io)?;
    if !out.status.success() {
        return Err(Error::Command {
            name: format!("digest {} [{phase}]", row.id),
            status: out.status.code().unwrap_or(-1),
            stderr_tail: crate::podman::stderr_tail(&out.stderr),
        });
    }
    let value = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if value.is_empty() {
        return Err(Error::Parse {
            what: "digest",
            line: "digest script printed nothing".into(),
        });
    }
    // The expectation comparison, first-run recording and void decision
    // live in `crate::digest` (unit-tested there; §5.2 rule 4 seam —
    // gate nv3-B-0-gate-1 defect 3). This function only produces the
    // measured value.
    Ok(value)
}

/// Apply one digest decision to the run state: append the evidence
/// record, set `voided` when a **strict** mismatch occurred (§5.2
/// rule 4), and return whether the plan loop must stop.
///
/// Unit-tested here so the void/stop wiring — not just the comparison —
/// is a tested seam (gate nv3-B-0-gate-1 defect 3, mutant M2).
fn apply_digest(
    digest_records: &mut Vec<(String, Value)>,
    voided: &mut bool,
    phase: &str,
    value: &str,
    decision: &digest::Decision,
    strict: bool,
) -> bool {
    digest_records.push((phase.to_string(), decision.evidence(value)));
    if decision.voids_run() && strict {
        *voided = true;
        return true;
    }
    if decision.voids_run() {
        // non-strict phases (e.g. the pgbench final digest after a
        // destructive benchmark) keep the evidence but not the veto
        eprintln!("digest mismatch at {phase} (non-strict): recorded, run continues");
    }
    false
}

/// Aggregate warm runs into median + bootstrap CI per metric.
fn aggregate(runs: &[ParsedRun], row: &RowDef, series: &str) -> Result<Value> {
    let mut by_metric: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    for r in runs {
        for (k, v) in &r.metrics {
            by_metric.entry(k.clone()).or_default().push(*v);
        }
    }
    let registry: Vec<&str> = row.metrics.iter().map(|(m, _)| *m).collect();
    let mut fields: Vec<(String, Value)> = vec![("runs".into(), Value::n(runs.len() as f64))];
    let mut metrics_obj: Vec<(String, Value)> = Vec::new();
    for name in registry.iter() {
        if let Some(samples) = by_metric.get(*name) {
            if samples.is_empty() {
                continue;
            }
            let med = stats::median(samples)?;
            let seed = stats::seed_for(&format!("{}#{}", row.id, series), name);
            let (lo, hi) = stats::bootstrap_ci_median(samples, 10_000, seed)?;
            metrics_obj.push((
                name.to_string(),
                Value::obj(vec![
                    (
                        "samples",
                        Value::Arr(samples.iter().map(|s| Value::n(*s)).collect()),
                    ),
                    ("median", Value::n(med)),
                    ("ci95", Value::Arr(vec![Value::n(lo), Value::n(hi)])),
                ]),
            ));
        }
    }
    for (k, samples) in &by_metric {
        if !registry.contains(&k.as_str()) {
            // unexpected metric: still aggregate, but flagged by absence
            // from the registry-driven report table
            let med = stats::median(samples)?;
            let seed = stats::seed_for(&format!("{}#{}", row.id, series), k);
            let (lo, hi) = stats::bootstrap_ci_median(samples, 10_000, seed)?;
            metrics_obj.push((
                k.clone(),
                Value::obj(vec![
                    (
                        "samples",
                        Value::Arr(samples.iter().map(|s| Value::n(*s)).collect()),
                    ),
                    ("median", Value::n(med)),
                    ("ci95", Value::Arr(vec![Value::n(lo), Value::n(hi)])),
                ]),
            ));
        }
    }
    fields.push(("metrics".into(), Value::Obj(metrics_obj)));
    Ok(Value::obj(
        fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect(),
    ))
}

fn parsed_to_metrics_value(p: &ParsedRun) -> Value {
    Value::obj(
        p.metrics
            .iter()
            .map(|(k, v)| (k.as_str(), Value::n(*v)))
            .collect(),
    )
}

/// The end-of-row load rule (escalation nv3-B-0-esc-1, rework round 4
/// item a): `load1` at results-write time strictly above the host CPU
/// count — the same comparison the chain scripts apply at row start —
/// means the row's numbers were produced on a host busier than its own
/// CPU budget (§5.2 rule 1), and the row is flagged.
///
/// The note quotes the `load1` reading exactly as recorded in
/// `provenance.load_at_end`, so the anomaly is re-derivable from the
/// results file alone. An unreadable or unparseable reading produces
/// no anomaly: nothing was measured, so nothing is claimed.
fn end_load_anomaly(load_at_end: &Value, host_cpus: usize) -> Option<String> {
    let raw = load_at_end.get("load1").and_then(Value::as_str)?;
    let load1 = raw.parse::<f64>().ok()?;
    (load1 > host_cpus as f64).then(|| {
        format!("load1 {raw} exceeded {host_cpus} (host cpus) at row end (provenance.load_at_end)")
    })
}

/// Build the row's anomaly list: the operator's `NB_ANOMALIES` notes
/// (one entry per line or separated by `;`), in order, then the
/// harness's own end-of-row load anomaly when [`end_load_anomaly`]
/// fires.
///
/// Kept as a pure seam so the wiring — operator notes preserved AND
/// the end check applied — is unit-tested: the round-3 gate showed an
/// unwired check passes every test silently (mutant G4).
fn final_anomalies(env_value: &str, load_at_end: &Value, host_cpus: usize) -> Vec<Value> {
    let mut out: Vec<Value> = env_value
        .split(['\n', ';'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(Value::s)
        .collect();
    if let Some(note) = end_load_anomaly(load_at_end, host_cpus) {
        out.push(Value::s(&note));
    }
    out
}

/// Run one row end to end.
pub fn run_row(
    root: &Path,
    row_id: &str,
    params: &Params,
    tag: Option<&str>,
) -> Result<RunOutcome> {
    let row = RowDef::find(row_id).ok_or_else(|| Error::UnknownRow(row_id.into()))?;
    let system = SystemDef::find(row.system)
        .ok_or_else(|| Error::Usage(format!("unknown system {}", row.system)))?;
    let image = read_image_pin(root, system)?;
    let date = utc_date();
    let smoke = tag == Some("smoke");
    let results_dir = if smoke {
        root.join("results").join("smoke").join(&date)
    } else {
        root.join("results").join(&date)
    };
    std::fs::create_dir_all(&results_dir).map_err(Error::Io)?;
    let mut work_name = row.id.to_string();
    if let Some(t) = tag {
        work_name.push('-');
        work_name.push_str(t);
    }
    let workdir = root.join("work").join(&date).join(&work_name);
    std::fs::create_dir_all(&workdir).map_err(Error::Io)?;

    // Host snapshot at row start: load average and live nbench units
    // are part of the environment the numbers are produced in (§5.2
    // rule 1; gate nv3-B-0-gate-1 finding 4). The results document
    // pairs this with a second load reading taken at write time.
    let host_at_start = host::collect()?;

    // Optional one-time workload preparation (client images, datasets).
    let prep = root.join("workloads").join(row.id).join("prepare.sh");
    if prep.exists() {
        let out = Command::new("bash")
            .arg(&prep)
            .env("NB_HARNESS", root)
            .env("NB_OUT", &workdir)
            .env("NB_DATA", root.join("data"))
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    root.join("bin").display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .output()
            .map_err(Error::Io)?;
        if !out.status.success() {
            return Err(Error::Command {
                name: format!("prepare {row_id}"),
                status: out.status.code().unwrap_or(-1),
                stderr_tail: crate::podman::stderr_tail(&out.stderr),
            });
        }
    }

    stop_system(system);

    let mut digest_records: Vec<(String, Value)> = Vec::new();
    let mut voided = false;
    /// One (series, mode) group and its parsed executions.
    type Executions = Vec<(Vec<(String, String)>, &'static str, Vec<ParsedRun>)>;
    let mut executions: Executions = Vec::new();

    let exec_ctx = ExecCtx {
        root,
        row,
        system,
        image: &image,
        params,
    };

    for step in rows::plan(row, params) {
        match step {
            Step::Start { variant } => {
                start_system(root, system, &image, variant.as_deref())?;
            }
            Step::Exec {
                mode,
                series,
                index,
            } => {
                let parsed = exec_driver(&exec_ctx, mode, &series, index, &workdir)?;
                if let Some(entry) = executions
                    .iter_mut()
                    .find(|(s, m, _)| *m == mode && s == &series)
                {
                    entry.2.push(parsed);
                } else {
                    executions.push((series, mode, vec![parsed]));
                }
            }
            Step::Digest { phase, strict } => {
                let digest_ctx = DigestCtx {
                    root,
                    row,
                    system,
                    params,
                    workdir: &workdir,
                };
                let value = run_digest(&digest_ctx, phase)?;
                let path = digest::expectation_path(root, row.id, phase, smoke);
                let decision = digest::decide(&path, &value, strict)?;
                if apply_digest(
                    &mut digest_records,
                    &mut voided,
                    phase,
                    &value,
                    &decision,
                    strict,
                ) {
                    let report = Error::DigestMismatch {
                        phase: phase.to_string(),
                        expected: decision.expected().unwrap_or_default(),
                        got: value.clone(),
                    };
                    eprintln!("{report} — run VOIDED");
                    break;
                }
            }
        }
    }
    stop_system(system);

    // Cross-run digest equality: when the tool itself prints a per-run
    // content digest (speedtest1 --verify Verification Hash), every run
    // of a series must show
    // the same digest, or the row is void (§5.2 rule 4).
    for (series, _mode, runs) in &executions {
        let digests: Vec<&str> = runs.iter().filter_map(|r| r.digest.as_deref()).collect();
        if digests.len() > 1 {
            let first = digests[0];
            if digests.iter().any(|d| *d != first) {
                voided = true;
                eprintln!(
                    "DIGEST MISMATCH within series {}: runs disagree ({}) — run VOIDED",
                    series_string(series),
                    digests.join(" vs ")
                );
            }
        }
    }

    // Build the results document.
    let mut series_out: Vec<Value> = Vec::new();
    let all_series = (row.sweep)(params);
    for series in &all_series {
        let key = series_string(series);
        let cold = executions
            .iter()
            .find(|(s, m, _)| *m == "cold" && s == series)
            .and_then(|(_, _, runs)| runs.first())
            .map(parsed_to_metrics_value);
        let warm_runs: Vec<&ParsedRun> = executions
            .iter()
            .find(|(s, m, _)| *m == "run" && s == series)
            .map(|(_, _, runs)| runs.iter().collect())
            .unwrap_or_default();
        let warm = if warm_runs.is_empty() {
            None
        } else {
            let owned: Vec<ParsedRun> = warm_runs.into_iter().cloned().collect();
            Some(aggregate(&owned, row, &key)?)
        };
        series_out.push(Value::obj(vec![
            (
                "config",
                Value::Arr(
                    series
                        .iter()
                        .map(|(k, v)| Value::Arr(vec![Value::s(k), Value::s(v)]))
                        .collect(),
                ),
            ),
            (
                "cold",
                match cold {
                    Some(c) => Value::obj(vec![("metrics", c)]),
                    None => Value::Null,
                },
            ),
            (
                "warm",
                match warm {
                    Some(w) => w,
                    None => Value::Null,
                },
            ),
        ]));
    }

    let versions_path = workdir.join("versions.txt");
    // Rule 6 provenance is mandatory, not best-effort: a driver that
    // fails to write versions.txt used to vanish silently into an empty
    // string (gate nv3-B-0-gate-1 finding 8) — absence is now loud.
    let versions = std::fs::read_to_string(&versions_path).map_err(|e| {
        Error::Missing(format!(
            "versions.txt not written by the driver ({}): {e} — \
             every row's init mode must record tool versions (§5.2 rule 6)",
            versions_path.display()
        ))
    })?;

    // Anomalies are operator observations the harness cannot measure:
    // co-tenant load spikes, gate activity on the shared host, known
    // host incidents during the run (gate nv3-B-0-gate-1 finding 4).
    // Supplied as `NB_ANOMALIES`, one entry per line or separated by
    // ';'. The harness adds its own end-of-row load observation
    // (rework round 4 item a) from the same reading that lands in
    // provenance.load_at_end below — one reading, two roles, so the
    // anomaly note and the provenance can never disagree.
    let load_at_end = host::loadavg_value();
    let anomalies = final_anomalies(
        &std::env::var("NB_ANOMALIES").unwrap_or_default(),
        &load_at_end,
        host::cpu_count(),
    );

    let digest_obj = {
        let mut fields = digest_records.clone();
        fields.push(("voided".into(), Value::Bool(voided)));
        Value::Obj(fields)
    };

    let params_obj = Value::Obj(
        params
            .to_pairs()
            .into_iter()
            .map(|(k, v)| (k, Value::s(&v)))
            .collect(),
    );

    let results = Value::obj(vec![
        ("row", Value::s(row.id)),
        ("system", Value::s(system.name)),
        ("date", Value::s(&date)),
        ("tag", Value::s(tag.unwrap_or(""))),
        ("params", params_obj),
        (
            "provenance",
            Value::obj(vec![
                ("host", host_at_start),
                (
                    // load at results-write time complements the start
                    // snapshot: a row measured through a load spike is
                    // visible as start/end disagreement (finding 4),
                    // and feeds the end-of-row load anomaly (rework
                    // round 4 item a)
                    "load_at_end",
                    load_at_end.clone(),
                ),
                (
                    "images",
                    Value::Obj(vec![(system.name.to_string(), Value::s(&image))]),
                ),
                (
                    "limits",
                    Value::obj(vec![
                        ("memory", Value::s(system.mem_limit)),
                        ("cpus", Value::s(system.cpus)),
                        ("network", Value::s("host")),
                        ("volume", Value::s(system.volume)),
                    ]),
                ),
                ("versions", Value::s(versions.trim())),
            ]),
        ),
        ("digest", digest_obj),
        ("anomalies", Value::Arr(anomalies)),
        ("series", Value::Arr(series_out)),
    ]);

    let file_name = if smoke {
        format!("{}.smoke.json", row.id)
    } else {
        format!("{}.json", row.id)
    };
    let results_path = results_dir.join(&file_name);
    std::fs::write(&results_path, json::to_string(&results)?).map_err(Error::Io)?;

    let report_path = regenerate_report(&results_dir)?;

    Ok(RunOutcome {
        results_path,
        report_path,
        voided,
    })
}

/// Scan a results directory and write REPORT.md.
pub fn regenerate_report(dir: &Path) -> Result<PathBuf> {
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(Error::Io)? {
        let entry = entry.map_err(Error::Io)?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".json") {
            files.push((name.clone(), entry.path()));
        }
    }
    files.sort();
    let mut parsed: Vec<(String, Value)> = Vec::new();
    for (name, path) in &files {
        let text = std::fs::read_to_string(path).map_err(Error::Io)?;
        match json::parse(&text) {
            Ok(v) => parsed.push((name.clone(), v)),
            Err(e) => eprintln!("skipping unparseable results file {}: {e}", path.display()),
        }
    }
    let date = dir
        .file_name()
        .map(|d| d.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".into());
    let md = report::generate(&date, &parsed)?;
    let out = dir.join("REPORT.md");
    std::fs::write(&out, md).map_err(Error::Io)?;
    Ok(out)
}

/// Stop every nbench server (used by `nbench cleanup`).
pub fn cleanup() {
    for system in rows::SYSTEMS {
        stop_system(system);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_mismatch_voids_and_stops() {
        // The §5.2 rule 4 wiring this rework exists for: a strict
        // mismatch sets `voided`, records the evidence, and stops the
        // plan loop (gate nv3-B-0-gate-1 defect 3, previously-uncaught
        // mutant M2: inverting this decision passed every test).
        let mut records = Vec::new();
        let mut voided = false;
        let decision = digest::Decision::Mismatch {
            expected: "recorded".into(),
            got: "measured".into(),
        };
        let stop = apply_digest(
            &mut records,
            &mut voided,
            "init",
            "measured",
            &decision,
            true,
        );
        assert!(stop, "a strict mismatch must stop the plan loop");
        assert!(voided, "a strict mismatch must void the run");
        assert_eq!(records.len(), 1);
        let (phase, ev) = &records[0];
        assert_eq!(phase, "init");
        assert_eq!(ev.get("expected").and_then(Value::as_str), Some("recorded"));
        assert_eq!(ev.get("value").and_then(Value::as_str), Some("measured"));
        assert_eq!(ev.get("match").and_then(Value::as_bool), Some(false));
    }

    #[test]
    fn non_strict_mismatch_records_but_neither_voids_nor_stops() {
        let mut records = Vec::new();
        let mut voided = false;
        let decision = digest::Decision::Mismatch {
            expected: "e".into(),
            got: "g".into(),
        };
        let stop = apply_digest(&mut records, &mut voided, "final", "g", &decision, false);
        assert!(!stop);
        assert!(!voided, "non-strict phases keep the evidence, not the veto");
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn match_and_first_run_continue_the_plan() {
        for decision in [
            digest::Decision::Match,
            digest::Decision::FirstRun { recorded: true },
            digest::Decision::FirstRun { recorded: false },
        ] {
            let mut records = Vec::new();
            let mut voided = false;
            let stop = apply_digest(&mut records, &mut voided, "init", "v", &decision, true);
            assert!(!stop, "{decision:?} must not stop the loop");
            assert!(!voided);
            assert_eq!(
                records[0].1.get("match").and_then(Value::as_bool),
                Some(true)
            );
        }
    }

    #[test]
    fn end_load_rule_fires_only_above_host_cpus() {
        // Round 4 item (a): the same rule the chain scripts apply at
        // row start — load1 strictly above the host CPU count — applied
        // by the runner when the row finishes. The note must quote the
        // reading exactly as provenance.load_at_end records it.
        let load = |l1: &str| {
            Value::obj(vec![
                ("load1", Value::s(l1)),
                ("load5", Value::s("1.00")),
                ("load15", Value::s("1.00")),
            ])
        };
        assert_eq!(
            end_load_anomaly(&load("74.30"), 16),
            Some(
                "load1 74.30 exceeded 16 (host cpus) at row end (provenance.load_at_end)"
                    .to_string()
            )
        );
        // quiet host: nothing to flag
        assert_eq!(end_load_anomaly(&load("2.92"), 16), None);
        // exactly at the CPU count is not "exceeded" (strict rule)
        assert_eq!(end_load_anomaly(&load("16.00"), 16), None);
        // an unmeasured reading claims nothing
        assert_eq!(end_load_anomaly(&load("unknown"), 16), None);
    }

    #[test]
    fn final_anomalies_keep_operator_notes_and_apply_the_end_check() {
        // The wiring seam (round-3 mutant G4 lesson: an unwired check
        // passed 92/92): operator notes survive in order AND the
        // end-of-row load anomaly lands as the last entry.
        let high = Value::obj(vec![("load1", Value::s("67.92"))]);
        let v = final_anomalies("co-tenant gate build at 09:20\nsecond note; ", &high, 16);
        assert_eq!(v.len(), 3, "two notes plus the end-load anomaly");
        assert_eq!(v[0].as_str(), Some("co-tenant gate build at 09:20"));
        assert_eq!(v[1].as_str(), Some("second note"));
        let note = v[2].as_str().expect("end-load anomaly appended");
        assert!(
            note.starts_with("load1 67.92 exceeded 16 (host cpus)"),
            "note quotes the recorded reading and the rule: {note}"
        );
        // a quiet host keeps exactly the operator's notes
        let quiet = Value::obj(vec![("load1", Value::s("1.06"))]);
        assert!(final_anomalies("", &quiet, 16).is_empty());
        assert_eq!(
            final_anomalies("single note", &quiet, 16),
            vec![Value::s("single note")]
        );
    }

    #[test]
    fn civil_dates_match_known_days() {
        // Values cross-checked with `date -u -d <date> +%s / 86400`.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20743), (2026, 10, 17));
        assert_eq!(civil_from_days(19723), (2024, 1, 1));
        assert_eq!(civil_from_days(10957), (2000, 1, 1));
    }

    #[test]
    fn series_string_is_stable() {
        let s = vec![
            ("clients".to_string(), "16".to_string()),
            ("mode".to_string(), "on".to_string()),
        ];
        assert_eq!(series_string(&s), "clients=16,mode=on");
    }

    #[test]
    fn resp_ping_probe_targets_the_configured_port() {
        // Valkey shares the host network with Redis; a probe without the
        // port would hit whatever listens on 6379 (card B-0 regression).
        assert_eq!(
            resp_ping_probe("redis-cli", 6380),
            "redis-cli -p 6380 ping | grep -q PONG"
        );
        assert_eq!(
            resp_ping_probe("valkey-cli", 6379),
            "valkey-cli -p 6379 ping | grep -q PONG"
        );
    }

    #[test]
    fn aggregate_reports_median_ci_and_all_runs() {
        let row = RowDef::find("kv-memtier-valkey8").expect("row");
        let runs: Vec<ParsedRun> = (0..5)
            .map(|i| ParsedRun {
                metrics: vec![
                    ("ops_sec".to_string(), 1000.0 + i as f64),
                    ("p99_ms".to_string(), 2.0),
                ],
                digest: None,
            })
            .collect();
        let v = aggregate(&runs, row, "appendfsync=always,pipeline=1").expect("agg");
        assert_eq!(v.get("runs").and_then(Value::as_f64), Some(5.0));
        let ops = v
            .get("metrics")
            .and_then(|m| m.get("ops_sec"))
            .expect("ops");
        assert_eq!(ops.get("median").and_then(Value::as_f64), Some(1002.0));
        let ci = ops.get("ci95").and_then(Value::as_arr).expect("ci");
        let lo = ci.first().and_then(Value::as_f64).expect("lo");
        let hi = ci.get(1).and_then(Value::as_f64).expect("hi");
        assert!(lo <= 1002.0 && 1002.0 <= hi);
        // deterministic across calls
        let v2 = aggregate(&runs, row, "appendfsync=always,pipeline=1").expect("agg");
        assert_eq!(
            v.get("metrics")
                .and_then(|m| m.get("ops_sec"))
                .and_then(|o| o.get("ci95"))
                .cloned(),
            v2.get("metrics")
                .and_then(|m| m.get("ops_sec"))
                .and_then(|o| o.get("ci95"))
                .cloned(),
        );
    }
}
