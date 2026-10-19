//! `nbench` — the NBench harness binary (PLAN-v3 §5, card B-0).
//!
//! Subcommands:
//! - `list`                — rows and systems in the registry
//! - `run <row>`           — execute one row end to end (`all` for every row)
//! - `report`              — regenerate the Markdown report for a date
//! - `cleanup`             — stop every nbench server container
//! - `vec-*`               — vector workload helpers used by driver scripts
//! - `host-info`           — print host provenance JSON
//!
//! Invariant owned here: argument parsing never panics and every usage
//! mistake prints the usage text with exit code 2.

mod digest;
mod error;
mod host;
mod http;
mod json;
mod params;
mod parse;
mod podman;
mod report;
mod rows;
mod runner;
mod stats;
mod vector;

use error::{Error, Result};
use json::Value;
use params::Params;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const USAGE: &str = "\
nbench — NBench harness (PLAN-v3 §5, card B-0)

USAGE:
  nbench list
  nbench run <row|all> [--tag NAME] [--set key=value]...
  nbench report [--date YYYY-MM-DD] [--tag smoke] [--out FILE]
  nbench cleanup
  nbench host-info
  nbench vec-qdrant-load   --base F --host H --port P [--collection C] [--m N] [--ef-construct N] [--batch N]
  nbench vec-qdrant-search --base F --query F --truth F --host H --port P --ef N [--limit K] [--metrics-out F]
  nbench vec-qdrant-digest --host H --port P [--collection C]
  nbench vec-qdrant-version --host H --port P
  nbench vec-pg-copy       --base F
  nbench vec-pg-search     --container C --query F --truth F --ef N [--limit K] [--port P] [--metrics-out F]
";

fn arg_value(args: &[String], name: &str) -> Option<String> {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        // accept "-tag v", "--tag v" and "--tag=v"
        let bare = a.trim_start_matches('-');
        if bare == name {
            return it.next().cloned();
        }
        if let Some(rest) = a.strip_prefix(&format!("--{name}=")) {
            return Some(rest.to_string());
        }
    }
    None
}

fn required(args: &[String], name: &str) -> Result<String> {
    arg_value(args, name).ok_or_else(|| Error::Usage(format!("missing --{name}")))
}

fn collect_sets(args: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        // accept both "--set k=v" and "--set=k=v"
        let value = if let Some(kv) = a.strip_prefix("--set=") {
            Some(kv.to_string())
        } else if a == "--set" {
            it.next().cloned()
        } else {
            None
        };
        if let Some(kv) = value
            && let Some((k, v)) = kv.split_once('=')
        {
            out.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    out
}

fn write_metrics_json(path: Option<&str>, v: &Value) -> Result<()> {
    let text = json::to_string(v)?;
    match path {
        Some(p) => std::fs::write(p, text).map_err(Error::Io)?,
        None => {
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "{text}").map_err(Error::Io)?;
        }
    }
    Ok(())
}

/// Feed `input` to the child's stdin from a helper thread while its
/// output drains, then wait for exit.
///
/// Invariant owned here (2026-10-19 pgvector hang, latent since the row
/// was first attempted): writing a large query stream on the caller
/// thread *before* reading stdout deadlocks — the child (psql) stops
/// reading stdin once its own stdout pipe fills (~64 KiB of id lines ≈
/// 800 queries), backpressure seals the input relay, and both sides
/// wait forever. The helper thread keeps the write side flowing while
/// [`std::process::Child::wait_with_output`] drains stdout/stderr
/// concurrently.
fn feed_and_collect(mut child: std::process::Child, input: &[u8]) -> Result<std::process::Output> {
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::Io(std::io::Error::other("no stdin")))?;
    let payload = input.to_vec();
    let feeder = std::thread::Builder::new()
        .name("nbench-stdin-feed".into())
        .spawn(move || stdin.write_all(&payload).map(|()| payload.len()))?;
    let out = child.wait_with_output().map_err(Error::Io)?;
    let fed = feeder
        .join()
        .map_err(|_| Error::Io(std::io::Error::other("stdin feeder panicked")))??;
    if fed != input.len() {
        return Err(Error::Io(std::io::Error::other(
            "short write to child stdin",
        )));
    }
    Ok(out)
}

fn percentile_of(sorted: &[f64], q: f64) -> f64 {
    stats::percentile_sorted(sorted, q)
}

fn cmd_vec_qdrant_load(args: &[String]) -> Result<()> {
    let base = required(args, "base")?;
    let host = required(args, "host")?;
    let port: u16 = required(args, "port")?
        .parse()
        .map_err(|_| Error::Usage("bad port".into()))?;
    let collection = arg_value(args, "collection").unwrap_or_else(|| "nbench-sift".into());
    let m: u64 = arg_value(args, "m")
        .unwrap_or_else(|| "16".into())
        .parse()
        .map_err(|_| Error::Usage("bad m".into()))?;
    let ef_construct: u64 = arg_value(args, "ef-construct")
        .unwrap_or_else(|| "200".into())
        .parse()
        .map_err(|_| Error::Usage("bad ef-construct".into()))?;
    let batch: usize = arg_value(args, "batch")
        .unwrap_or_else(|| "1024".into())
        .parse()
        .map_err(|_| Error::Usage("bad batch".into()))?;

    let vectors = vector::read_fvecs(&PathBuf::from(&base))?;
    let dim = vectors.first().map(|v| v.len()).unwrap_or(0);
    vector::qdrant_create(&host, port, &collection, dim, m, ef_construct)?;
    let start = Instant::now();
    for (i, chunk) in vectors.chunks(batch).enumerate() {
        vector::qdrant_upload(&host, port, &collection, i * batch, chunk)?;
        if i % 50 == 0 {
            eprintln!(
                "uploaded {} / {}",
                (i + 1) * batch.min(chunk.len().max(1)),
                vectors.len()
            );
        }
    }
    eprintln!(
        "upload done in {:.1}s; waiting for green",
        start.elapsed().as_secs_f64()
    );
    vector::qdrant_wait_green(
        &host,
        port,
        &collection,
        vectors.len(),
        Duration::from_secs(3600),
    )?;
    eprintln!(
        "collection green with {} points in {:.1}s total",
        vectors.len(),
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

fn cmd_vec_qdrant_search(args: &[String]) -> Result<()> {
    let query_path = required(args, "query")?;
    let truth_path = required(args, "truth")?;
    let host = required(args, "host")?;
    let port: u16 = required(args, "port")?
        .parse()
        .map_err(|_| Error::Usage("bad port".into()))?;
    let ef: u64 = required(args, "ef")?
        .parse()
        .map_err(|_| Error::Usage("bad ef".into()))?;
    let limit: usize = arg_value(args, "limit")
        .unwrap_or_else(|| "10".into())
        .parse()
        .map_err(|_| Error::Usage("bad limit".into()))?;
    let collection = arg_value(args, "collection").unwrap_or_else(|| "nbench-sift".into());
    let metrics_out = arg_value(args, "metrics-out");

    let queries = vector::read_fvecs(&PathBuf::from(&query_path))?;
    let truth = vector::read_ivecs(&PathBuf::from(&truth_path))?;

    // One persistent connection for the whole pass (gate nv3-B-0-gate-1
    // finding 5): the pgvector arm times a single psql session, so
    // paying TCP setup per query here made the arms overhead-unmatched.
    // The connection is opened *before* the clock starts; a transport
    // failure mid-pass fails the row (measurement requests never retry).
    let mut conn = crate::http::Connection::connect(&host, port, Duration::from_secs(120))?;
    let mut results: Vec<Vec<i64>> = Vec::with_capacity(queries.len());
    let mut lat_ms: Vec<f64> = Vec::with_capacity(queries.len());
    let start = Instant::now();
    for q in &queries {
        let t0 = Instant::now();
        let ids = vector::qdrant_search_on(&mut conn, &collection, q, limit, ef)?;
        lat_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        results.push(ids);
    }
    let wall = start.elapsed().as_secs_f64();
    let qps = queries.len() as f64 / wall;
    let recall = vector::recall_at_k(&results, &truth, limit)?;
    lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let v = Value::obj(vec![
        ("qps", Value::n(qps)),
        ("recall_at_10", Value::n(recall)),
        ("queries", Value::n(queries.len() as f64)),
        ("p50_ms", Value::n(percentile_of(&lat_ms, 0.5))),
        ("p99_ms", Value::n(percentile_of(&lat_ms, 0.99))),
        ("ef_search", Value::n(ef as f64)),
        (
            "timing_note",
            Value::s("one persistent connection (HTTP keep-alive) for the whole pass"),
        ),
    ]);
    write_metrics_json(metrics_out.as_deref(), &v)?;
    eprintln!(
        "ef={ef}: qps={qps:.1} recall@{limit}={recall:.4} p50={:.3}ms p99={:.3}ms",
        percentile_of(&lat_ms, 0.5),
        percentile_of(&lat_ms, 0.99)
    );
    Ok(())
}

fn cmd_vec_qdrant_version(args: &[String]) -> Result<()> {
    let host = required(args, "host")?;
    let port: u16 = required(args, "port")?
        .parse()
        .map_err(|_| Error::Usage("bad port".into()))?;
    println!("{}", vector::qdrant_version(&host, port)?);
    Ok(())
}

fn cmd_vec_qdrant_digest(args: &[String]) -> Result<()> {
    let host = required(args, "host")?;
    let port: u16 = required(args, "port")?
        .parse()
        .map_err(|_| Error::Usage("bad port".into()))?;
    let collection = arg_value(args, "collection").unwrap_or_else(|| "nbench-sift".into());
    let count = vector::qdrant_count(&host, port, &collection)?;
    // Deterministic sample: ids divisible by 997, capped at 128 points.
    let sample_ids: Vec<usize> = (0..count).filter(|i| i % 997 == 0).take(128).collect();
    // Point-get request shape per the Qdrant REST docs: an object with
    // `ids` and explicit `with_vector` (the pinned image rejects a bare
    // id array with 400, probed 2026-10-17).
    let body = json::to_string(&Value::obj(vec![
        (
            "ids",
            Value::Arr(sample_ids.iter().map(|i| Value::n(*i as f64)).collect()),
        ),
        ("with_vector", Value::Bool(true)),
        ("with_payload", Value::Bool(false)),
    ]))?;
    let resp = http::request(
        &host,
        port,
        "POST",
        &format!("/collections/{collection}/points"),
        body.as_bytes(),
        Duration::from_secs(30),
    )?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant point get (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    let v = json::parse(&String::from_utf8_lossy(&resp.body))?;
    let result = v
        .get("result")
        .and_then(Value::as_arr)
        .ok_or_else(|| Error::Missing("point get result".into()))?;
    // Hash vector bytes in id order (the response order follows the request).
    let mut ordered: Vec<(usize, Vec<f32>)> = Vec::new();
    for p in result {
        let id = p
            .get("id")
            .and_then(Value::as_f64)
            .ok_or_else(|| Error::Missing("id".into()))? as usize;
        let vec = p
            .get("vector")
            .and_then(Value::as_arr)
            .ok_or_else(|| Error::Missing("vector".into()))?
            .iter()
            .filter_map(Value::as_f64)
            .map(|x| x as f32)
            .collect();
        ordered.push((id, vec));
    }
    ordered.sort_by_key(|(id, _)| *id);
    let mut hash: u64 = 0xcbf29ce484222325;
    for (id, vec) in &ordered {
        hash ^= *id as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        for x in vec {
            hash ^= x.to_bits() as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    println!("count={} sample={} fnv={:016x}", count, ordered.len(), hash);
    Ok(())
}

fn cmd_vec_pg_copy(args: &[String]) -> Result<()> {
    let base = required(args, "base")?;
    let vectors = vector::read_fvecs(&PathBuf::from(&base))?;
    let mut stdout = std::io::stdout().lock();
    vector::emit_pg_copy(&vectors, &mut stdout)?;
    stdout.flush().map_err(Error::Io)?;
    Ok(())
}

fn cmd_vec_pg_search(args: &[String]) -> Result<()> {
    let container = required(args, "container")?;
    let query_path = required(args, "query")?;
    let truth_path = required(args, "truth")?;
    let ef: u64 = required(args, "ef")?
        .parse()
        .map_err(|_| Error::Usage("bad ef".into()))?;
    let limit: usize = arg_value(args, "limit")
        .unwrap_or_else(|| "10".into())
        .parse()
        .map_err(|_| Error::Usage("bad limit".into()))?;
    // The pgvector server runs on a non-default port (5433) so it can
    // coexist with the postgres17 system; psql must be told explicitly.
    let port: u16 = arg_value(args, "port")
        .unwrap_or_else(|| "5432".into())
        .parse()
        .map_err(|_| Error::Usage("bad port".into()))?;
    let metrics_out = arg_value(args, "metrics-out");

    let queries = vector::read_fvecs(&PathBuf::from(&query_path))?;
    let truth = vector::read_ivecs(&PathBuf::from(&truth_path))?;

    let mut sql: Vec<u8> = Vec::new();
    vector::emit_pg_search_sql(&queries, ef, limit, &mut sql)?;

    let child = Command::new("systemd-run")
        .args([
            "--user",
            "--wait",
            "--pipe",
            "--quiet",
            "podman",
            "exec",
            "-i",
            &container,
            "psql",
            "-q",
            "-t",
            "-A",
            "-p",
            &port.to_string(),
            "-U",
            "postgres",
            "-d",
            "nbench",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(Error::Io)?;
    // Time the whole search pass: the query stream is fed from a helper
    // thread while the server processes and the results drain
    // concurrently (write-then-read deadlocks once the result pipe
    // fills — the 2026-10-19 pass hung on exactly that; see
    // feed_and_collect).
    let start = Instant::now();
    let out = feed_and_collect(child, &sql)?;
    let wall = start.elapsed().as_secs_f64();
    if !out.status.success() {
        return Err(Error::Command {
            name: format!("psql search (ef={ef})"),
            status: out.status.code().unwrap_or(-1),
            stderr_tail: podman::stderr_tail(&out.stderr),
        });
    }
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    let results = vector::parse_pg_search_output(&text, queries.len())?;
    let recall = vector::recall_at_k(&results, &truth, limit)?;
    let qps = queries.len() as f64 / wall;
    let v = Value::obj(vec![
        ("qps", Value::n(qps)),
        ("recall_at_10", Value::n(recall)),
        ("queries", Value::n(queries.len() as f64)),
        ("ef_search", Value::n(ef as f64)),
        // note: per-query latency is not visible through one psql session;
        // QPS is the timed metric for pgvector (documented in the report).
        ("timing_note", Value::s("qps over a single psql session")),
    ]);
    write_metrics_json(metrics_out.as_deref(), &v)?;
    eprintln!("ef={ef}: qps={qps:.1} recall@{limit}={recall:.4}");
    Ok(())
}

fn cmd_run(args: &[String]) -> Result<()> {
    let row_id = args
        .first()
        .cloned()
        .ok_or_else(|| Error::Usage("run needs a row id (or all)".into()))?;
    let tag = arg_value(args, "tag");
    let sets = collect_sets(args);
    let set_refs: Vec<(&str, &str)> = sets.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let params = Params::with_overrides(&set_refs)?;
    let root = runner::harness_root()?;
    let targets: Vec<&str> = if row_id == "all" {
        rows::ROWS.iter().map(|r| r.id).collect()
    } else {
        // allow comma-separated rows
        row_id
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect()
    };
    for target in &targets {
        if rows::RowDef::find(target).is_none() {
            return Err(Error::UnknownRow(target.to_string()));
        }
    }
    let mut failures = Vec::new();
    for target in targets {
        eprintln!(
            "=== nbench run {target} (tag {})",
            tag.clone().unwrap_or_default()
        );
        match runner::run_row(&root, target, &params, tag.as_deref()) {
            Ok(outcome) => {
                eprintln!("results: {}", outcome.results_path.display());
                eprintln!("report:  {}", outcome.report_path.display());
                if outcome.voided {
                    failures.push(format!("{target}: VOID (digest mismatch)"));
                }
            }
            Err(e) => {
                eprintln!("row {target} FAILED: {e}");
                failures.push(format!("{target}: {e}"));
                runner::cleanup();
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(Error::Command {
            name: "nbench run".into(),
            status: 1,
            stderr_tail: failures.join("; "),
        })
    }
}

fn cmd_report(args: &[String]) -> Result<()> {
    let root = runner::harness_root()?;
    let tag = arg_value(args, "tag");
    let date = arg_value(args, "date").unwrap_or_else(runner::utc_date);
    let dir = if tag.as_deref() == Some("smoke") {
        root.join("results").join("smoke").join(&date)
    } else {
        root.join("results").join(&date)
    };
    let out = runner::regenerate_report(&dir)?;
    if let Some(dest) = arg_value(args, "out") {
        let text = std::fs::read_to_string(&out).map_err(Error::Io)?;
        std::fs::write(&dest, text).map_err(Error::Io)?;
        println!("{dest}");
    } else {
        println!("{}", out.display());
    }
    Ok(())
}

fn cmd_list() -> Result<()> {
    println!("rows:");
    for r in rows::ROWS {
        println!("  {} (system: {})", r.id, r.system);
    }
    println!("systems:");
    for s in rows::SYSTEMS {
        println!(
            "  {} ({:?} volume={} mem={} cpus={})",
            s.name, s.kind, s.volume, s.mem_limit, s.cpus
        );
    }
    Ok(())
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    // Subcommand errors are printed, never swallowed: a silent exit 1
    // inside a driver pipeline hides the cause (seen during B-0 bring-up).
    let code = match argv.first().map(String::as_str) {
        Some("list") => ok_or_print(cmd_list()),
        Some("run") => ok_or_print(cmd_run(&argv[1..])),
        Some("report") => ok_or_print(cmd_report(&argv[1..])),
        Some("cleanup") => {
            runner::cleanup();
            true
        }
        Some("host-info") => ok_or_print(host::collect().and_then(|v| {
            json::to_string(&v).map(|s| {
                println!("{s}");
            })
        })),
        Some("vec-qdrant-load") => ok_or_print(cmd_vec_qdrant_load(&argv[1..])),
        Some("vec-qdrant-search") => ok_or_print(cmd_vec_qdrant_search(&argv[1..])),
        Some("vec-qdrant-digest") => ok_or_print(cmd_vec_qdrant_digest(&argv[1..])),
        Some("vec-qdrant-version") => ok_or_print(cmd_vec_qdrant_version(&argv[1..])),
        Some("vec-pg-copy") => ok_or_print(cmd_vec_pg_copy(&argv[1..])),
        Some("vec-pg-search") => ok_or_print(cmd_vec_pg_search(&argv[1..])),
        _ => {
            eprint!("{USAGE}");
            std::process::exit(2);
        }
    };
    if !code {
        std::process::exit(1);
    }
}

/// Print a subcommand error and report failure.
fn ok_or_print(r: Result<()>) -> bool {
    match r {
        Ok(()) => true,
        Err(e) => {
            eprintln!("nbench: {e}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    /// A child that amplifies its input (every line echoed twice) so its
    /// stdout outruns its stdin: a caller that writes everything before
    /// reading deadlocks once ~64 KiB of output backs up (the exact
    /// 2026-10-19 pgvector hang). [`feed_and_collect`] must complete.
    /// The test is timing-independent in the pass direction: the naive
    /// shape hangs *forever* (it would time the test out), the fixed
    /// shape finishes in well under a second.
    #[test]
    fn feed_and_collect_survives_output_backpressure() {
        // ~1.6 MB of input lines -> ~3.2 MB of output, far past the
        // pipe capacity; nothing about this completes unless stdin
        // feeding and stdout draining run concurrently.
        let input = b"0123456789abcdef\n".repeat(100_000);
        // each 17-byte line (16 chars + \n) amplifies to 33 bytes
        // (16+16 chars + one \n): 2x the payload, minus the undoubled
        // newline — still far past the 64 KiB pipe capacity
        let expected_out = input.len() * 2 - 100_000;
        let child = Command::new("sh")
            .arg("-c")
            .arg("while IFS= read -r line; do printf '%s%s\\n' \"$line\" \"$line\"; done")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn amplifier");
        let out = feed_and_collect(child, &input).expect("no deadlock");
        assert!(out.status.success());
        assert_eq!(out.stdout.len(), expected_out);
        // every amplified line round-trips
        assert!(
            out.stdout
                .starts_with(b"0123456789abcdef0123456789abcdef\n")
        );
    }

    #[test]
    fn feed_and_collect_reports_child_failure() {
        let child = Command::new("sh")
            .arg("-c")
            .arg("cat > /dev/null; exit 3")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn");
        let out = feed_and_collect(child, b"payload\n").expect("collected");
        assert_eq!(out.status.code(), Some(3));
    }
}
