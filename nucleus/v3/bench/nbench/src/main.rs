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
use std::fmt::Write as _;
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
  nbench vec-qdrant-search --query F --truth F --host H --port P --ef N [--limit K] [--metrics-out F] [--pass-kind cold|warmup|run] [--latency-out F] [--ids-out F] [--exact] [--max-queries N]
  nbench vec-qdrant-info   --host H --port P [--collection C]
  nbench qdrant-latency-verdict --client-p50-ms X --server-p50-ms Y
  nbench vec-recall          --ids F --truth F [--k N]
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

/// Whether a boolean flag (e.g. `--exact`) is present. Accepts the
/// `-exact`/`--exact` spellings `arg_value` allows for valued options.
fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a.trim_start_matches('-') == name)
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
    // Card B-0-F7 Work 2: the index state the passes will measure —
    // segment count and the raw config — is part of every init log.
    // `green` does not mean the optimizer finished (the preserved
    // chain10 collection was grey with 92 segments and 3.86x indexed
    // vectors), so the summary line is recorded, never assumed.
    if let Ok(info) = vector::qdrant_collection_info(&host, port, &collection)
        && let Ok(s) = vector::collection_summary(&info)
    {
        eprintln!(
            "index state at init end: status={} points={} indexed={} segments={} \
             update_queue={}",
            s.status, s.points, s.indexed, s.segments, s.update_queue
        );
    }
    Ok(())
}

/// The measured numbers of one Qdrant search pass.
struct QdrantPassNumbers {
    /// `hnsw_ef` the pass ran with.
    ef: u64,
    /// Top-k requested (`limit`).
    limit: usize,
    /// Query count of the pass.
    queries: usize,
    /// Queries per second over the whole pass.
    qps: f64,
    /// recall@limit against the dataset ground truth.
    recall: f64,
    /// Median per-query latency in milliseconds.
    p50_ms: f64,
    /// 99th percentile per-query latency in milliseconds.
    p99_ms: f64,
    /// Median **server-reported** per-query time in milliseconds (card
    /// B-0-F7 Work 1), when the pass recorded it.
    server_p50_ms: Option<f64>,
    /// 99th percentile server-reported time in milliseconds.
    server_p99_ms: Option<f64>,
    /// Brute-force control pass (`params.exact`): measures the same
    /// collection with no HNSW hint; its latency is not comparable to
    /// an ANN pass and it is never sanity-checked (card B-0-F7).
    exact: bool,
}

/// Assemble the Qdrant pass metrics object, enforcing the per-query
/// p50 sanity check first (verdict ddb00ba5 defect 1).
///
/// Extracted from `cmd_vec_qdrant_search` so the *wiring* — sanity
/// before metrics, cold passes exempt, unknown kinds enforced — is a
/// unit-tested seam (gate round-3 mutant G4 unwired the check inside
/// the command and passed 92/92). A failed sanity check returns `Err`
/// before any metrics exist, so a stalled pass can never record.
///
/// Card B-0-F7: the server-reported p50 travels in the metrics next to
/// the client p50 whenever the pass recorded one, and an `exact`
/// control pass is exempt from the ANN latency sanity (it pays a full
/// scan by design; the exemption is exactly this flag, never a missing
/// value — a pass without the marker is enforced as before).
fn qdrant_pass_metrics(pass_kind: &str, n: &QdrantPassNumbers) -> Result<Value> {
    if !n.exact && vector::latency_sanity_enforced(pass_kind) {
        vector::qdrant_latency_sanity(n.p50_ms)?;
    }
    let mut fields: Vec<(&str, Value)> = vec![
        ("qps", Value::n(n.qps)),
        ("recall_at_10", Value::n(n.recall)),
        ("queries", Value::n(n.queries as f64)),
        ("p50_ms", Value::n(n.p50_ms)),
        ("p99_ms", Value::n(n.p99_ms)),
        ("ef_search", Value::n(n.ef as f64)),
        (
            "timing_note",
            Value::s(
                "one persistent connection (HTTP keep-alive, TCP_NODELAY, single write \
                 per request) for the whole pass; warm-pass p50 sanity-checked at 10 ms",
            ),
        ),
        ("pass_kind", Value::s(pass_kind)),
        ("limit", Value::n(n.limit as f64)),
    ];
    if let Some(s) = n.server_p50_ms {
        fields.push(("server_p50_ms", Value::n(s)));
    }
    if let Some(s) = n.server_p99_ms {
        fields.push(("server_p99_ms", Value::n(s)));
    }
    if n.exact {
        fields.push(("exact", Value::Bool(true)));
    }
    Ok(Value::obj(fields))
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
    // The driver passes its NB_MODE (cold/warmup/run): cold passes are
    // exempt from the p50 sanity limit (engine warm-up after restart is
    // what cold measures); any missing or unknown kind is enforced.
    let pass_kind = arg_value(args, "pass-kind").unwrap_or_default();
    // Card B-0-F7 Work 1: optional per-query client/server latency CSV.
    let latency_out = arg_value(args, "latency-out");
    // Card B-0-F7: optional per-query result ids (JSONL, one array per
    // query), written *before* the sanity check so a refused pass keeps
    // its recall evidence — the diagnosis must be able to recompute
    // recall from a pass the 10 ms limit refused.
    let ids_out = arg_value(args, "ids-out");
    // Card B-0-F7 recall control: `exact` asks Qdrant for a brute-force
    // pass (no `hnsw_ef`), which must reproduce the ground truth on the
    // same collection; such a pass is a control, never a sanity-checked
    // measurement (see `qdrant_pass_metrics`).
    let exact = has_flag(args, "exact");
    // Truncate the pass to the first N queries (used by the exact
    // control, which scans the whole collection per query).
    let max_queries: usize = match arg_value(args, "max-queries") {
        Some(s) => s
            .parse()
            .map_err(|_| Error::Usage("bad max-queries".into()))?,
        None => usize::MAX,
    };

    let mut queries = vector::read_fvecs(&PathBuf::from(&query_path))?;
    let mut truth = vector::read_ivecs(&PathBuf::from(&truth_path))?;
    if max_queries < queries.len() {
        queries.truncate(max_queries);
        truth.truncate(max_queries);
    }
    if queries.is_empty() {
        return Err(Error::Usage(
            "search pass has no queries (check --max-queries and the query file)".into(),
        ));
    }

    // One persistent connection for the whole pass (gate nv3-B-0-gate-1
    // finding 5): the pgvector arm times a single psql session, so
    // paying TCP setup per query here made the arms overhead-unmatched.
    // The connection is opened *before* the clock starts; a transport
    // failure mid-pass fails the row (measurement requests never retry).
    let mut conn = crate::http::Connection::connect(&host, port, Duration::from_secs(120))?;
    let mut results: Vec<Vec<i64>> = Vec::with_capacity(queries.len());
    let mut lat_ms: Vec<f64> = Vec::with_capacity(queries.len());
    let mut server_ms: Vec<f64> = Vec::with_capacity(queries.len());
    let start = Instant::now();
    for q in &queries {
        let t0 = Instant::now();
        let out = vector::qdrant_search_on(&mut conn, &collection, q, limit, ef, exact)?;
        lat_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        server_ms.push(out.server_time_s * 1000.0);
        results.push(out.ids);
    }
    let wall = start.elapsed().as_secs_f64();
    let qps = queries.len() as f64 / wall;
    let recall = vector::recall_at_k(&results, &truth, limit)?;
    // Card B-0-F7 Work 1: per-query client/server pairs are evidence,
    // not just a summary. Written in query order BEFORE the vectors are
    // sorted for percentiles, so each row keeps its own pairing; an
    // all-zero server column is the card's first named instrumentation
    // mutant and is impossible here — a missing `time` field fails the
    // pass loudly inside `parse_search_response`.
    if let Some(path) = latency_out.as_deref() {
        std::fs::write(path, latency_csv(&lat_ms, &server_ms)).map_err(Error::Io)?;
    }
    // Written before any sanity refusal: a refused pass still happened,
    // and its recall evidence survives (card B-0-F7).
    if let Some(path) = ids_out.as_deref() {
        let mut jsonl = String::new();
        for ids in &results {
            let arr = Value::Arr(ids.iter().map(|i| Value::n(*i as f64)).collect());
            let _ = writeln!(jsonl, "{}", json::to_string(&arr)?);
        }
        std::fs::write(path, jsonl).map_err(Error::Io)?;
    }
    lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    server_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = percentile_of(&lat_ms, 0.5);
    let server_p50 = percentile_of(&server_ms, 0.5);
    let v = qdrant_pass_metrics(
        &pass_kind,
        &QdrantPassNumbers {
            ef,
            limit,
            queries: queries.len(),
            qps,
            recall,
            p50_ms: p50,
            p99_ms: percentile_of(&lat_ms, 0.99),
            server_p50_ms: Some(server_p50),
            server_p99_ms: Some(percentile_of(&server_ms, 0.99)),
            exact,
        },
    )?;
    write_metrics_json(metrics_out.as_deref(), &v)?;
    eprintln!(
        "ef={ef}: qps={qps:.1} recall@{limit}={recall:.4} p50={p50:.3}ms p99={:.3}ms \
         server_p50={server_p50:.3}ms",
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

/// The per-query latency CSV (card B-0-F7 Work 1): `client_ms,server_ms`
/// per query, in query order. Pure so the evidence format itself is
/// pinned — the c−s table is built from this file, and a server column
/// that is empty, zero-filled, or a copy of the client column would
/// silently change the diagnosis.
fn latency_csv(client_ms: &[f64], server_ms: &[f64]) -> String {
    let mut csv = String::from("client_ms,server_ms\n");
    for (c, s) in client_ms.iter().zip(server_ms.iter()) {
        let _ = writeln!(csv, "{c:.6},{s:.6}");
    }
    csv
}

/// Recompute recall@k for an ids JSONL file (one JSON array of result
/// ids per query) against the dataset ground truth (card B-0-F7: the
/// recall of a pass the sanity limit refused stays recomputable from
/// its `--ids-out` evidence; one implementation, the tested
/// `vector::recall_at_k`).
fn cmd_vec_recall(args: &[String]) -> Result<()> {
    let ids_path = required(args, "ids")?;
    let truth_path = required(args, "truth")?;
    let k: usize = arg_value(args, "k")
        .unwrap_or_else(|| "10".into())
        .parse()
        .map_err(|_| Error::Usage("bad k".into()))?;
    let truth = vector::read_ivecs(&PathBuf::from(&truth_path))?;
    let text = std::fs::read_to_string(&ids_path).map_err(Error::Io)?;
    let mut results: Vec<Vec<i64>> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        results.push(parse_ids_line(line)?);
    }
    let recall = vector::recall_at_k(&results, &truth, k)?;
    println!("{recall:.6}");
    Ok(())
}

/// Parse one `--ids-out` line: a JSON array of point ids.
fn parse_ids_line(line: &str) -> Result<Vec<i64>> {
    let v = json::parse(line)?;
    let arr = v
        .as_arr()
        .ok_or_else(|| Error::Parse {
            what: "ids line",
            line: line.to_string(),
        })?
        .to_vec();
    let mut ids = Vec::with_capacity(arr.len());
    for item in &arr {
        ids.push(
            item.as_f64()
                .map(|n| n as i64)
                .ok_or_else(|| Error::Parse {
                    what: "ids line",
                    line: line.to_string(),
                })?,
        );
    }
    Ok(ids)
}

/// Apply the pre-registered card B-0-F7 Work 4 rule to one pass's p50s
/// and print the verdict (used by the diagnosis script so the rule has
/// exactly one implementation — the unit-tested one in `vector.rs`).
fn cmd_qdrant_latency_verdict(args: &[String]) -> Result<()> {
    let c: f64 = required(args, "client-p50-ms")?
        .parse()
        .map_err(|_| Error::Usage("bad client-p50-ms".into()))?;
    let s: f64 = required(args, "server-p50-ms")?
        .parse()
        .map_err(|_| Error::Usage("bad server-p50-ms".into()))?;
    let verdict = vector::diagnose_client_server_latency(c, s)?;
    let (c, s, d) = verdict.numbers();
    let arm = match verdict {
        vector::LatencyVerdict::ClientExcess { .. } => "CLIENT-DEFECT",
        vector::LatencyVerdict::ServerSide { .. } => "SERVER-SIDE",
        vector::LatencyVerdict::WithinTolerances { .. } => "WITHIN-TOLERANCES",
    };
    println!("VERDICT {arm} c={c:.3}ms s={s:.3}ms c-s={d:.3}ms");
    Ok(())
}

/// Print a collection's info document (card B-0-F7 Work 2): the raw
/// `result` JSON on stdout (the committed artefact), a one-line
/// diagnosis summary on stderr (the init log's provenance line).
fn cmd_vec_qdrant_info(args: &[String]) -> Result<()> {
    let host = required(args, "host")?;
    let port: u16 = required(args, "port")?
        .parse()
        .map_err(|_| Error::Usage("bad port".into()))?;
    let collection = arg_value(args, "collection").unwrap_or_else(|| "nbench-sift".into());
    let result = vector::qdrant_collection_info(&host, port, &collection)?;
    let summary = vector::collection_summary(&result)?;
    eprintln!(
        "collection {collection}: status={} points={} indexed={} ({}x points) \
         segments={} update_queue={}",
        summary.status,
        summary.points,
        summary.indexed,
        summary.indexed as f64 / summary.points.max(1) as f64,
        summary.segments,
        summary.update_queue
    );
    println!("{}", json::to_string(&result)?);
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

    // Delivery: write the SQL to the row's work directory, `podman cp`
    // it into the container, then execute with `psql -f`. No stdin is
    // streamed: under this host's mandatory `systemd-run --user --wait
    // --pipe` wrapper (card B-0 environment note) the multi-relay stdin
    // path delivered only the first statement of a 10k-statement script
    // before stalling, in both the naive write-then-read shape and a
    // dedicated feeder thread (2026-10-19; the 2026-10-18 chain died
    // in the same place — its ef_search=16-cold-0.log is 0 bytes).
    // Greedy consumers (COPY FROM STDIN) survive the relay;
    // statement-at-a-time psql does not. A container-local file with
    // `psql -f` removes the relay from the data path entirely.
    let sql_path = match metrics_out.as_deref() {
        Some(m) => std::path::PathBuf::from(format!("{m}.sql")),
        None => std::env::temp_dir().join(format!("nbench-search-{ef}.sql")),
    };
    std::fs::write(&sql_path, &sql).map_err(Error::Io)?;
    podman::podman(&[
        "cp",
        &sql_path.to_string_lossy(),
        &format!("{container}:/tmp/nbench-search.sql"),
    ])?;

    // Time the whole search pass: one psql session executing the file's
    // 10k statements and printing their results (per-query latency is
    // not visible through one session; QPS is the timed metric).
    let start = Instant::now();
    let out = Command::new("systemd-run")
        .args([
            "--user",
            "--wait",
            "--pipe",
            "--quiet",
            "podman",
            "exec",
            &container,
            "psql",
            "-q",
            "-t",
            "-A",
            "-v",
            "ON_ERROR_STOP=1",
            "-p",
            &port.to_string(),
            "-U",
            "postgres",
            "-d",
            "nbench",
            "-f",
            "/tmp/nbench-search.sql",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(Error::Io)?;
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
        (
            "timing_note",
            Value::s("qps over a single psql session (psql -f on a container-local file)"),
        ),
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
        Some("vec-qdrant-info") => ok_or_print(cmd_vec_qdrant_info(&argv[1..])),
        Some("qdrant-latency-verdict") => ok_or_print(cmd_qdrant_latency_verdict(&argv[1..])),
        Some("vec-recall") => ok_or_print(cmd_vec_recall(&argv[1..])),
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

    #[test]
    fn pass_metrics_enforce_the_sanity_on_warm_kinds() {
        // Gate round-3 mutant G4 (sanity call unwired in the command)
        // passed every test; the wiring is the seam now: a warm pass in
        // the transport-stall regime must fail BEFORE metrics exist.
        for kind in ["run", "warmup", "", "unknown"] {
            let r = qdrant_pass_metrics(
                kind,
                &QdrantPassNumbers {
                    ef: 16,
                    limit: 10,
                    queries: 10_000,
                    qps: 22.0,
                    recall: 0.94,
                    p50_ms: 44.07,
                    p99_ms: 49.0,
                    server_p50_ms: Some(1.8),
                    server_p99_ms: Some(2.2),
                    exact: false,
                },
            );
            let err = r.expect_err(&format!("kind {kind:?} must be enforced"));
            assert!(
                format!("{err}").contains("latency sanity"),
                "kind {kind:?}: {err}"
            );
            // the healthy regime records, with the measured p50 in it
            let v = qdrant_pass_metrics(
                kind,
                &QdrantPassNumbers {
                    ef: 16,
                    limit: 10,
                    queries: 10_000,
                    qps: 500.0,
                    recall: 0.94,
                    p50_ms: 1.8,
                    p99_ms: 2.4,
                    server_p50_ms: Some(1.7),
                    server_p99_ms: Some(2.1),
                    exact: false,
                },
            )
            .expect("healthy warm pass records");
            assert_eq!(v.get("p50_ms").and_then(Value::as_f64), Some(1.8));
            assert_eq!(v.get("pass_kind").and_then(Value::as_str), Some(kind));
            // card B-0-F7 Work 1: the server p50 sits next to the
            // client p50 whenever the pass recorded one — the c−s
            // table is built from these two numbers.
            assert_eq!(v.get("server_p50_ms").and_then(Value::as_f64), Some(1.7));
            assert_eq!(v.get("server_p99_ms").and_then(Value::as_f64), Some(2.1));
        }
    }

    #[test]
    fn latency_csv_keeps_query_order_and_both_columns() {
        // Card B-0-F7 Work 1 evidence format: one row per query, the
        // client and server numbers of THE SAME query side by side.
        // Sorting would destroy the pairing; a duplicated column would
        // fake a zero c−s delta.
        let csv = latency_csv(&[10.5, 2.25, 7.0], &[1.5, 2.0, 6.75]);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[0], "client_ms,server_ms");
        assert_eq!(lines[1], "10.500000,1.500000");
        assert_eq!(lines[2], "2.250000,2.000000");
        assert_eq!(lines[3], "7.000000,6.750000");
        assert_eq!(lines.len(), 4);
        assert!(csv.ends_with('\n'));
    }

    #[test]
    fn ids_jsonl_roundtrips_through_recall() {
        // Card B-0-F7: a refused pass keeps its recall evidence as an
        // ids JSONL; `parse_ids_line` + `vector::recall_at_k` must
        // reproduce the pass's recall from that file alone.
        let ids = vec![
            parse_ids_line("[1,2,3,4,5,6,7,8,9,10]").expect("line 1"),
            parse_ids_line("[5,6,7,8,9,10,11,12,13,14]").expect("line 2"),
        ];
        assert_eq!(ids[0], vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let truth = vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10i32]; 2];
        let r = vector::recall_at_k(&ids, &truth, 10).expect("recall");
        assert!((r - 0.8).abs() < 1e-12, "one perfect hit + one 6/10: {r}");
        // not an array, or non-numeric members, are loud errors
        assert!(parse_ids_line("{\"a\":1}").is_err());
        assert!(parse_ids_line("[1,\"x\"]").is_err());
        assert!(parse_ids_line("garbage").is_err());
    }

    #[test]
    fn pass_metrics_exempt_cold_passes_only() {
        // Cold = first search after a container restart: engine warm-up
        // is what cold measures (chain9: p50 15.37 ms cold, load-12
        // host, fixed client), and §5.2 published statistics come from
        // warm runs. The exemption records the slow p50 honestly
        // instead of voiding the row.
        let v = qdrant_pass_metrics(
            "cold",
            &QdrantPassNumbers {
                ef: 16,
                limit: 10,
                queries: 10_000,
                qps: 65.0,
                recall: 0.94,
                p50_ms: 15.37,
                p99_ms: 40.0,
                server_p50_ms: Some(14.9),
                server_p99_ms: Some(38.0),
                exact: false,
            },
        )
        .expect("cold pass is exempt from the sanity limit");
        assert_eq!(v.get("p50_ms").and_then(Value::as_f64), Some(15.37));
        // the exemption is exactly one kind, not a range
        assert!(!vector::latency_sanity_enforced("cold"));
        assert!(vector::latency_sanity_enforced("run"));
        assert!(vector::latency_sanity_enforced("warmup"));
        assert!(vector::latency_sanity_enforced(""));
    }

    #[test]
    fn pass_metrics_exempt_exact_controls_and_mark_them() {
        // Card B-0-F7 recall control: an `exact` pass brute-force scans
        // the whole collection per query — its latency is a different
        // quantity from an ANN pass and must not be judged by the ANN
        // sanity limit. Like the cold exemption, the exemption is one
        // explicit marker (`exact: true`), carried in the metrics so a
        // control can never masquerade as a measured pass; a pass
        // without the marker is enforced exactly as before (previous
        // test).
        let v = qdrant_pass_metrics(
            "run",
            &QdrantPassNumbers {
                ef: 16,
                limit: 10,
                queries: 100,
                qps: 2.0,
                recall: 1.0,
                p50_ms: 480.0,
                p99_ms: 600.0,
                server_p50_ms: Some(470.0),
                server_p99_ms: Some(590.0),
                exact: true,
            },
        )
        .expect("exact control is exempt from the ANN sanity limit");
        assert_eq!(v.get("exact").and_then(Value::as_bool), Some(true));
        assert_eq!(v.get("p50_ms").and_then(Value::as_f64), Some(480.0));
        assert_eq!(v.get("server_p50_ms").and_then(Value::as_f64), Some(470.0));
    }
}
