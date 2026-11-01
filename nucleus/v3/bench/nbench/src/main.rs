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
             (measured rows need a release build; --tag smoke is exempt — card B-0-F7-F1 J5)
  nbench report [--date YYYY-MM-DD] [--tag smoke] [--out FILE]
  nbench cleanup
  nbench host-info
  nbench vec-qdrant-load   --base F --host H --port P [--collection C] [--m N] [--ef-construct N] [--batch N] [--state-out F]
  nbench vec-qdrant-settle --host H --port P [--collection C] --expected-points N [--window-s W] [--timeout-s T] --state-out F [--expect-state F]
  nbench vec-qdrant-search --query F --truth F --host H --port P --ef N [--limit K] [--metrics-out F] [--pass-kind cold|warmup|run] [--latency-out F] [--ids-out F] [--exact] [--max-queries N] [--index-state F]
  nbench vec-qdrant-info   --host H --port P [--collection C]
  nbench qdrant-latency-verdict --client-p50-ms X --server-p50-ms Y
  nbench qdrant-repro-verdict --a F --b F
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
    // Card B-0-F7-F1 J4: the settled state is written to a file so the
    // post-restart settles can compare against it.
    let state_out = arg_value(args, "state-out");

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
        "upload done in {:.1}s; applying the tail indexing rule (J3) and \
         waiting for the index to settle (J2)",
        start.elapsed().as_secs_f64()
    );
    // Card B-0-F7-F1 J3: the tail rule is unconditional — one build
    // procedure for every run — and J2 replaces the single green poll
    // that let three different index states measure as "ready"
    // (verdict B-0-F7 finding 1).
    vector::qdrant_index_tail(&host, port, &collection)?;
    let state = vector::qdrant_wait_settled(
        &host,
        port,
        &collection,
        vectors.len(),
        None,
        Duration::from_secs(3600),
    )?;
    eprintln!("{}", state.line());
    if let Some(path) = state_out.as_deref() {
        std::fs::write(path, json::to_string(&state.to_value())?).map_err(Error::Io)?;
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

/// Append the index-state numbers to a pass's metrics (card B-0-F7-F1
/// J4). Pure so the wiring — state rides in the metrics, and only when
/// a state file was given — is a unit-tested seam: the numbers must
/// survive into the results JSON (only numeric metrics make it through
/// `parse_result`), and without `--index-state` the metrics must stay
/// byte-identical to the pre-F1 shape.
///
/// STATE-NOT-IN-METRICS (returning the metrics unchanged) empties the
/// row of its per-series index evidence and fails the test below.
fn with_index_state(metrics: Value, st: &vector::IndexState) -> Value {
    match metrics {
        Value::Obj(mut pairs) => {
            pairs.push(("index_points".into(), Value::n(st.points as f64)));
            pairs.push(("index_indexed".into(), Value::n(st.indexed as f64)));
            pairs.push(("index_segments".into(), Value::n(st.segments as f64)));
            pairs.push((
                "index_threshold_kb".into(),
                Value::n(st.indexing_threshold_kb as f64),
            ));
            pairs.push(("index_waited_s".into(), Value::n(st.waited_s)));
            Value::Obj(pairs)
        }
        // `qdrant_pass_metrics` always returns an object; anything else
        // is passed through untouched rather than guessed into one.
        other => other,
    }
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
    // Card B-0-F7-F1 J4: the settled index state recorded before this
    // pass rides in the metrics as numbers (the parser keeps numeric
    // metrics only), so every pass's evidence carries the state it
    // measured. Without the flag the metrics are byte-identical to
    // before this card.
    let index_state_path = arg_value(args, "index-state");

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
    // Card B-0-F7-F1 J4: after the metrics (and their sanity check)
    // exist, append the index-state numbers from the settle that ran
    // before this pass — outside the timed loop, which is untouched.
    let v = match index_state_path.as_deref() {
        Some(path) => {
            let text = std::fs::read_to_string(path).map_err(Error::Io)?;
            let st = vector::IndexState::from_value(&json::parse(&text)?)?;
            with_index_state(v, &st)
        }
        None => v,
    };
    write_metrics_json(metrics_out.as_deref(), &v)?;
    eprintln!(
        "ef={ef}: qps={qps:.1} recall@{limit}={recall:.4} p50={p50:.3}ms p99={:.3}ms \
         server_p50={server_p50:.3}ms",
        percentile_of(&lat_ms, 0.99)
    );
    Ok(())
}

/// Card B-0-F7-F1 J4: wait for the collection to settle again (after a
/// container restart or between passes), write the settled state, and
/// — with `--expect-state` — refuse the row when the index is not the
/// state the init settled in. `--window-s` absent means the full J2
/// window (the post-restart case); `0` means the one-poll re-check the
/// warm passes make.
fn cmd_vec_qdrant_settle(args: &[String]) -> Result<()> {
    let host = required(args, "host")?;
    let port: u16 = required(args, "port")?
        .parse()
        .map_err(|_| Error::Usage("bad port".into()))?;
    let collection = arg_value(args, "collection").unwrap_or_else(|| "nbench-sift".into());
    let expected_points: usize = required(args, "expected-points")?
        .parse()
        .map_err(|_| Error::Usage("bad expected-points".into()))?;
    let window: Option<f64> = match arg_value(args, "window-s") {
        Some(s) => Some(s.parse().map_err(|_| Error::Usage("bad window-s".into()))?),
        None => None,
    };
    let timeout_s: u64 = arg_value(args, "timeout-s")
        .unwrap_or_else(|| "3600".into())
        .parse()
        .map_err(|_| Error::Usage("bad timeout-s".into()))?;
    let state_out = required(args, "state-out")?;
    let expect_state = arg_value(args, "expect-state");

    let state = vector::qdrant_wait_settled(
        &host,
        port,
        &collection,
        expected_points,
        window,
        Duration::from_secs(timeout_s),
    )?;
    eprintln!("{}", state.line());
    std::fs::write(&state_out, json::to_string(&state.to_value())?).map_err(Error::Io)?;

    if let Some(expected_path) = expect_state.as_deref() {
        let text = std::fs::read_to_string(expected_path).map_err(Error::Io)?;
        let expected = vector::IndexState::from_value(&json::parse(&text)?)?;
        if let Err(diff) = vector::same_index_state(&expected, &state) {
            // Both states are printed in full: the refusal is its own
            // evidence (J4 — "a row whose state changes is refused").
            eprintln!("index state mismatch against {expected_path}: {diff}");
            eprintln!("expected state: {}", json::to_string(&expected.to_value())?);
            eprintln!("actual state:   {}", json::to_string(&state.to_value())?);
            return Err(diff);
        }
    }
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

// ---- card B-0-F7-F1: the reproducibility rule (J6, pre-registered) ----

/// J6: warm-median `qps` and `p50_ms` must agree within this fraction
/// of the **smaller** value (|a − b| ≤ 0.15 × min(a, b); boundaries
/// inclusive). REPRO-MAX-BASE (using max) is a mutant of exactly this
/// constant's use.
const REPRO_REL_BOUND: f64 = 0.15;

/// J6: warm-median `recall_at_10` must agree within this absolute
/// distance.
const REPRO_RECALL_BOUND: f64 = 0.002;

/// J6: boundaries are inclusive "with a 1e-9 tolerance so float
/// rounding cannot decide a decimal boundary" (0.002 is exactly on the
/// recall boundary in f64 only within this slack).
const REPRO_TOL: f64 = 1e-9;

/// The outcome of the J6 rule applied to two results documents.
struct ReproVerdict {
    /// `REPRODUCED`, `NOT-REPRODUCED` or `INVALID`.
    word: &'static str,
    /// One line per ef point and metric (both values, the spread and
    /// pass/fail), plus the explanation when the word is not
    /// REPRODUCED.
    lines: Vec<String>,
}

/// The `ef_search` value of one series' `config` pairs, if the series
/// has one. The pair is a `[key, value]` array of strings.
fn series_ef(series: &Value) -> Option<u64> {
    let config = series.get("config").and_then(Value::as_arr)?;
    let mut found = None;
    for pair in config {
        let kv = pair.as_arr()?;
        if kv.first().and_then(Value::as_str) == Some("ef_search")
            && let Some(v) = kv.get(1).and_then(Value::as_str)
            && let Some(n) = v.parse::<u64>().ok()
        {
            found = Some(n);
            break;
        }
    }
    found
}

/// Every series' ef point of a run, in series order.
fn run_efs(run: &Value) -> Vec<Option<u64>> {
    run.get("series")
        .and_then(Value::as_arr)
        .map(|series| series.iter().map(series_ef).collect())
        .unwrap_or_default()
}

/// The warm-median of one metric at one ef point of a run.
fn warm_median(run: &Value, ef: u64, metric: &str) -> Option<f64> {
    for series in run.get("series").and_then(Value::as_arr)? {
        if series_ef(series) == Some(ef) {
            return series
                .get("warm")?
                .get("metrics")?
                .get(metric)?
                .get("median")
                .and_then(Value::as_f64);
        }
    }
    None
}

/// The J6 INVALID preconditions (checked first — a bounds failure must
/// never mask them, and vice versa an INVALID never needs bounds):
/// `digest.voided`, `provenance.nbench_build.profile != "release"`,
/// and "any pass without the index-state metrics" — every series'
/// `cold.metrics` must carry `index_indexed == index_points`, and
/// every warm aggregate `index_segments`. One line per violation.
fn repro_precondition_lines(a: &Value, b: &Value) -> Vec<String> {
    let mut lines = Vec::new();
    for (name, run) in [("A", a), ("B", b)] {
        if run
            .get("digest")
            .and_then(|d| d.get("voided"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            lines.push(format!("INVALID: run {name} is digest.voided"));
        }
        let profile = run
            .get("provenance")
            .and_then(|p| p.get("nbench_build"))
            .and_then(|nb| nb.get("profile"))
            .and_then(Value::as_str);
        if profile != Some("release") {
            lines.push(format!(
                "INVALID: run {name} provenance.nbench_build.profile is {}, not \"release\"",
                profile.unwrap_or("<missing>")
            ));
        }
        for series in run.get("series").and_then(Value::as_arr).unwrap_or(&[]) {
            let ef = series_ef(series)
                .map(|e| e.to_string())
                .unwrap_or("?".into());
            let cold_metrics = series.get("cold").and_then(|c| c.get("metrics"));
            let idx = cold_metrics
                .and_then(|m| m.get("index_indexed"))
                .and_then(Value::as_f64);
            let pts = cold_metrics
                .and_then(|m| m.get("index_points"))
                .and_then(Value::as_f64);
            match (idx, pts) {
                (Some(i), Some(p)) if i == p => {}
                (Some(i), Some(p)) => lines.push(format!(
                    "INVALID: run {name} ef{ef}: cold metrics record index_indexed {i} != index_points {p}"
                )),
                _ => lines.push(format!(
                    "INVALID: run {name} ef{ef}: cold metrics lack the index-state numbers"
                )),
            }
            let warm_has_segments = series
                .get("warm")
                .and_then(|w| w.get("metrics"))
                .and_then(|m| m.get("index_segments"))
                .is_some();
            if !warm_has_segments {
                lines.push(format!(
                    "INVALID: run {name} ef{ef}: warm aggregate lacks index_segments"
                ));
            }
        }
    }
    lines
}

/// Apply the pre-registered J6 rule to two results documents (pure).
///
/// INVALID is decided first (see [`repro_precondition_lines`]); a run
/// whose shape cannot be evaluated at all (a series without an
/// `ef_search` pair, a missing warm median) is also INVALID — the rule
/// cannot say "the runs disagree" about numbers it does not have.
/// ef sets that differ (as sets) are NOT-REPRODUCED per the card.
/// Otherwise one line per ef point and metric, and the verdict is
/// REPRODUCED iff every bound holds at **every** ef point.
fn repro_verdict(a: &Value, b: &Value) -> ReproVerdict {
    let mut lines = repro_precondition_lines(a, b);
    if !lines.is_empty() {
        return ReproVerdict {
            word: "INVALID",
            lines,
        };
    }
    let a_efs = run_efs(a);
    let b_efs = run_efs(b);
    let mut a_sorted: Vec<u64> = match a_efs.iter().copied().collect::<Option<Vec<_>>>() {
        Some(v) => v,
        None => {
            return ReproVerdict {
                word: "INVALID",
                lines: vec!["INVALID: run A has a series without an ef_search config pair".into()],
            };
        }
    };
    let mut b_sorted: Vec<u64> = match b_efs.iter().copied().collect::<Option<Vec<_>>>() {
        Some(v) => v,
        None => {
            return ReproVerdict {
                word: "INVALID",
                lines: vec!["INVALID: run B has a series without an ef_search config pair".into()],
            };
        }
    };
    a_sorted.sort_unstable();
    b_sorted.sort_unstable();
    if a_sorted != b_sorted {
        return ReproVerdict {
            word: "NOT-REPRODUCED",
            lines: vec![format!(
                "ef_search sets differ: run A {:?} vs run B {:?}",
                a_sorted, b_sorted
            )],
        };
    }
    // One line per ef point and metric. The bounds use min(a, b) as
    // the base (J6: |a − b| ≤ 0.15 × min), inclusive with the 1e-9
    // tolerance; recall is absolute. A missing median is INVALID: the
    // rule cannot compare numbers that do not exist.
    let metrics: [(&str, bool); 3] = [("qps", true), ("p50_ms", true), ("recall_at_10", false)];
    let mut all_pass = true;
    for ef in &a_efs {
        let ef = ef.unwrap_or(0);
        for (metric, relative) in metrics {
            let (av, bv) = match (warm_median(a, ef, metric), warm_median(b, ef, metric)) {
                (Some(x), Some(y)) => (x, y),
                _ => {
                    return ReproVerdict {
                        word: "INVALID",
                        lines: vec![format!(
                            "INVALID: ef{ef}: a warm aggregate lacks {metric}'s median"
                        )],
                    };
                }
            };
            let spread = (av - bv).abs();
            let (bound, kind) = if relative {
                (REPRO_REL_BOUND * av.min(bv), "15% of min")
            } else {
                (REPRO_RECALL_BOUND, "absolute")
            };
            let pass = spread <= bound + REPRO_TOL;
            if !pass {
                all_pass = false;
            }
            lines.push(format!(
                "ef{ef} {metric}: a={av} b={bv} spread={spread} bound={} ({kind}) {}",
                bound,
                if pass { "PASS" } else { "FAIL" }
            ));
        }
    }
    ReproVerdict {
        word: if all_pass {
            "REPRODUCED"
        } else {
            "NOT-REPRODUCED"
        },
        lines,
    }
}

/// Apply [`repro_verdict`] to two results files and print the table
/// plus the final `REPRO VERDICT=<word>` line (card B-0-F7-F1 Work 6).
/// Exits 0 whenever the rule could be evaluated — the word carries the
/// outcome — and non-zero only for unreadable input.
fn cmd_qdrant_repro_verdict(args: &[String]) -> Result<()> {
    let a_path = required(args, "a")?;
    let b_path = required(args, "b")?;
    let a_text = std::fs::read_to_string(&a_path).map_err(Error::Io)?;
    let b_text = std::fs::read_to_string(&b_path).map_err(Error::Io)?;
    let a = json::parse(&a_text)?;
    let b = json::parse(&b_text)?;
    let verdict = repro_verdict(&a, &b);
    for line in &verdict.lines {
        println!("{line}");
    }
    println!("REPRO VERDICT={}", verdict.word);
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
    // Card B-0-F7-F1 J5: FIRST, before Params, harness_root or anything
    // that touches podman — a refused run starts nothing. A debug
    // client's per-query JSON work (~0.3–0.4 ms measured) is harness
    // time inside the competitor's latency, and the pgvector arm times
    // a release binary; only `--tag smoke` is exempt.
    runner::measured_build_allowed(
        tag.as_deref(),
        runner::build_profile(cfg!(debug_assertions)),
    )?;
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
        Some("vec-qdrant-settle") => ok_or_print(cmd_vec_qdrant_settle(&argv[1..])),
        Some("vec-qdrant-search") => ok_or_print(cmd_vec_qdrant_search(&argv[1..])),
        Some("vec-qdrant-digest") => ok_or_print(cmd_vec_qdrant_digest(&argv[1..])),
        Some("vec-qdrant-version") => ok_or_print(cmd_vec_qdrant_version(&argv[1..])),
        Some("vec-qdrant-info") => ok_or_print(cmd_vec_qdrant_info(&argv[1..])),
        Some("qdrant-latency-verdict") => ok_or_print(cmd_qdrant_latency_verdict(&argv[1..])),
        Some("qdrant-repro-verdict") => ok_or_print(cmd_qdrant_repro_verdict(&argv[1..])),
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

    // ---- card B-0-F7-F2: the search command's server-column glue ----
    //
    // Verdict B-0-F7 (39d72c2f) finding 4: three gate mutants inside
    // `cmd_vec_qdrant_search` passed 113/113 — M1b-CMD-PUSH-ZERO (the
    // command pushes 0 into the server column), O4-SERVER-P50-FROM-CLIENT
    // (the server p50 is computed from the client column) and
    // O7-SERVER-MS-UNIT (the server time is kept in seconds). The parse
    // seam was tested (`vector::search_response_records_the_server_time_
    // next_to_the_ids`), but the command glue between the parse and the
    // metrics/CSV was not. These tests drive the REAL command against a
    // scripted keep-alive HTTP server (the `keepalive_server` pattern
    // from http.rs, private to that module's tests): the actual push
    // site, the CSV write, the sorts and the percentiles are exercised.
    // Test-only by design — no seam is extracted, no non-test code
    // changes.

    // `Write` for the socket comes from `use super::*` above; `Read`
    // is not imported outside, so it is named here.
    use std::io::Read as _;

    /// A temp dir unique to one test (the `write_tmp` pattern from
    /// vector.rs, which is private to that module): process id plus test
    /// name, so the two command tests never share files.
    fn cmd_test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("nbench-tests").join(format!(
            "{}-search-command-{}",
            std::process::id(),
            name
        ));
        std::fs::create_dir_all(&dir).expect("mkdir test dir");
        dir
    }

    /// fvecs bytes (dim:u32, then dim f32 per vector) — a copy of
    /// vector.rs's private `fvec_bytes` test helper.
    fn cmd_fvec_bytes(vectors: &[&[f32]]) -> Vec<u8> {
        let mut b = Vec::new();
        for v in vectors {
            b.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for x in *v {
                b.extend_from_slice(&x.to_le_bytes());
            }
        }
        b
    }

    /// ivecs bytes (dim:u32, then dim i32 per row) — the ivecs twin of
    /// [`cmd_fvec_bytes`].
    fn cmd_ivec_bytes(rows: &[&[i32]]) -> Vec<u8> {
        let mut b = Vec::new();
        for r in rows {
            b.extend_from_slice(&(r.len() as u32).to_le_bytes());
            for x in *r {
                b.extend_from_slice(&x.to_le_bytes());
            }
        }
        b
    }

    /// Offset of the first `\r\n\r\n` (the end of an HTTP request head).
    fn request_head_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    /// The `Content-Length` of a request head (0 when the header is
    /// absent, e.g. a bodyless GET).
    fn request_content_length(head: &str) -> usize {
        for line in head.split('\n') {
            if let Some((k, v)) = line.trim_end().split_once(':')
                && k.trim().eq_ignore_ascii_case("content-length")
                && let Some(n) = v.trim().parse::<usize>().ok()
            {
                return n;
            }
        }
        0
    }

    /// A scripted Qdrant search server for one command pass (card
    /// B-0-F7-F2 Work 1) with a zero delay on every request — the
    /// shape the F2 command tests use. See [`scripted_qdrant_delayed`],
    /// which owns the framing contract and the delay semantics.
    fn scripted_qdrant(times_s: Vec<f64>) -> (u16, std::thread::JoinHandle<usize>) {
        scripted_qdrant_delayed(times_s.into_iter().map(|t| (t, Duration::ZERO)).collect())
    }

    /// A scripted Qdrant search server for one command pass (card
    /// B-0-F7-F2 Work 1, given per-request delays by card B-0-F7-F3
    /// Work 1): binds `127.0.0.1:0`, accepts exactly ONE connection —
    /// the command's whole pass shares one socket, per the keep-alive
    /// invariant `cmd_vec_qdrant_search` owns — and answers
    /// `script.len()` Content-Length-framed responses on it. For each
    /// `(t, delay)` entry it reads one full request (head, then
    /// `Content-Length` bytes of body), SLEEPS `delay`, and only then
    /// answers `200 OK` with the body
    /// `{"result":[{"id":0,"score":0.0},… ids 0..=9 …],"status":"ok",
    /// "time":<t>}` where `<t>` is the entry formatted with `{}`. The
    /// times must be binary-exact (`0.0625`, `0.03125`, `0.125`), so
    /// the seconds→ms conversion in the command is exact and the CSV
    /// fields compare as strings.
    ///
    /// Why the delay exists (card B-0-F7-F3, verdict B-0-F7-F2 finding
    /// 1): it runs AFTER the request is read and BEFORE the response
    /// is written, so the client's measured round trip for THAT query
    /// is at least `delay`, while the scripted server time stays
    /// binary-exact. One query can thus be made deterministically the
    /// slowest on the client side, which pins the per-row
    /// client↔server pairing of the latency CSV — which query a client
    /// field belongs to — that no test could see before (the gate
    /// mutant G3-CSV-AFTER-CLIENT-SORT-ONLY passed 115/115). A
    /// zero-delay entry behaves exactly like the original zero-delay
    /// server, which is all [`scripted_qdrant`] asks for.
    ///
    /// The join value is the number of requests served: a read error,
    /// EOF or the 30 s read timeout returns the count so far, so a
    /// failed pass fails the test instead of hanging it.
    fn scripted_qdrant_delayed(
        script: Vec<(f64, Duration)>,
    ) -> (u16, std::thread::JoinHandle<usize>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind scripted server");
        let port = listener.local_addr().expect("local addr").port();
        let handle = std::thread::spawn(move || {
            let Ok((mut conn, _)) = listener.accept() else {
                return 0;
            };
            conn.set_read_timeout(Some(Duration::from_secs(30)))
                .expect("set read timeout");
            let mut served = 0usize;
            let mut pending: Vec<u8> = Vec::new();
            for (t, delay) in script {
                // one full request: the head, then the body bytes
                let (sep, body_len) = loop {
                    if let Some(sep) = request_head_end(&pending) {
                        let head = String::from_utf8_lossy(&pending[..sep]).to_string();
                        let body_len = request_content_length(&head);
                        if pending.len() >= sep + 4 + body_len {
                            break (sep, body_len);
                        }
                    }
                    let mut chunk = [0u8; 8192];
                    match conn.read(&mut chunk) {
                        Ok(0) | Err(_) => return served,
                        Ok(n) => pending.extend_from_slice(&chunk[..n]),
                    }
                };
                // consume this request; leftovers belong to the next one
                pending.drain(..sep + 4 + body_len);
                // Card B-0-F7-F3: sleep between the read and the write —
                // the one sleep this module owns (server thread only;
                // B-0-F7's no-sleep rule is relaxed exactly this far, by
                // this card's authority), so the client's row for THIS
                // query carries at least the delay.
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                let hits = (0..10)
                    .map(|id| format!("{{\"id\":{id},\"score\":0.0}}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let body = format!("{{\"result\":[{hits}],\"status\":\"ok\",\"time\":{t}}}");
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                if conn.write_all(resp.as_bytes()).is_err() {
                    return served;
                }
                served += 1;
            }
            served
        });
        (port, handle)
    }

    /// Run the real `cmd_vec_qdrant_search` against
    /// [`scripted_qdrant`] with the card's inputs (card B-0-F7-F2
    /// Work 2-3) and zero per-request delays. The contract lives on
    /// [`run_scripted_search_command_delayed`]; this is the zero-delay
    /// entry the F2 tests call.
    fn run_scripted_search_command(name: &str, times_s: Vec<f64>) -> (PathBuf, PathBuf, usize) {
        run_search_command_against(name, scripted_qdrant(times_s))
    }

    /// Run the real `cmd_vec_qdrant_search` against
    /// [`scripted_qdrant_delayed`] with the card's inputs (card
    /// B-0-F7-F2 Work 2-3; the per-entry delay is card B-0-F7-F3
    /// Work 1): 3 dim-2 query vectors, truth rows of the ids 0..=9,
    /// `--limit 10`, and `--pass-kind cold` — cold is exempt from the
    /// 10 ms p50 sanity check, so a slow test host cannot flake the
    /// pass on the client column. Each script entry pairs a scripted
    /// server time with the delay the server sleeps before answering
    /// that query. Returns (metrics path, latency CSV path, requests
    /// served).
    fn run_scripted_search_command_delayed(
        name: &str,
        script: Vec<(f64, Duration)>,
    ) -> (PathBuf, PathBuf, usize) {
        run_search_command_against(name, scripted_qdrant_delayed(script))
    }

    /// The body shared by both runners above (card B-0-F7-F2 Work 2-3):
    /// write the 3-query/truth files, run the REAL command against the
    /// already-spawned scripted server (it blocks on `accept` until the
    /// command connects, so spawning before the files are written is
    /// inert), join it, and return (metrics path, latency CSV path,
    /// requests served).
    fn run_search_command_against(
        name: &str,
        server: (u16, std::thread::JoinHandle<usize>),
    ) -> (PathBuf, PathBuf, usize) {
        let (port, server) = server;
        let dir = cmd_test_dir(name);
        let query = dir.join("query.fvecs");
        let truth = dir.join("truth.ivecs");
        let metrics = dir.join("metrics.json");
        let latency = dir.join("latency.csv");
        std::fs::write(
            &query,
            cmd_fvec_bytes(&[&[1.0, 2.0], &[3.0, 4.0], &[5.0, 6.0]]),
        )
        .expect("write query.fvecs");
        let ids: Vec<i32> = (0..10).collect();
        std::fs::write(&truth, cmd_ivec_bytes(&[&ids, &ids, &ids])).expect("write truth.ivecs");
        let up = |p: &std::path::Path| p.to_str().expect("temp paths are utf-8").to_string();
        let args: Vec<String> = [
            ("--query", up(&query)),
            ("--truth", up(&truth)),
            ("--host", "127.0.0.1".to_string()),
            ("--port", port.to_string()),
            ("--ef", "16".to_string()),
            ("--limit", "10".to_string()),
            ("--pass-kind", "cold".to_string()),
            ("--metrics-out", up(&metrics)),
            ("--latency-out", up(&latency)),
        ]
        .into_iter()
        .flat_map(|(flag, value)| [flag.to_string(), value])
        .collect();
        cmd_vec_qdrant_search(&args)
            .expect("the real search command succeeds against the scripted server");
        let served = server.join().expect("scripted server thread finished");
        (metrics, latency, served)
    }

    #[test]
    fn search_command_records_server_ms_per_query_in_order() {
        // Card B-0-F7-F2 Work 3 (verdict B-0-F7 finding 4): the server
        // column of the latency CSV must carry the parsed server time
        // converted to MILLISECONDS, one row per query, in query order.
        // String-exact fields, because the scripted times are
        // binary-exact: 0.0625 s → "62.500000". M1b-CMD-PUSH-ZERO
        // prints "0.000000" everywhere; O7-SERVER-MS-UNIT prints
        // "0.062500" (seconds, unconverted).
        let (_metrics, latency, served) =
            run_scripted_search_command("records-server-ms", vec![0.0625, 0.03125, 0.125]);
        assert_eq!(
            served, 3,
            "one request per query on the pass's single connection"
        );
        let text = std::fs::read_to_string(&latency).expect("read latency csv");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4, "header + one row per query:\n{text}");
        assert_eq!(lines[0], "client_ms,server_ms");
        let server_fields: Vec<&str> = lines[1..]
            .iter()
            .map(|row| row.split(',').nth(1).expect("two csv fields"))
            .collect();
        assert_eq!(
            server_fields,
            vec!["62.500000", "31.250000", "125.000000"],
            "server column in query order, in ms"
        );
        for (i, row) in lines[1..].iter().enumerate() {
            let client: f64 = row
                .split(',')
                .next()
                .expect("client field present")
                .parse()
                .expect("client field parses as a number");
            assert!(
                client.is_finite() && client >= 0.0,
                "row {i} client field must be finite and >= 0: {row}"
            );
        }
    }

    #[test]
    fn search_command_server_percentiles_come_from_the_server_column() {
        // Card B-0-F7-F2 Work 4 (verdict B-0-F7 finding 4): the server
        // percentiles in the metrics JSON must come from the SERVER
        // column, not the client column. Sorted scripted server times:
        // [31.25, 62.5, 125] ms → type-7 p50 = 62.5 and
        // p99 = 62.5 + 0.98 × (125 − 62.5) = 123.75.
        // O4-SERVER-P50-FROM-CLIENT (and its p99 twin O4b) would put
        // the loopback client numbers there instead.
        let (metrics, _latency, served) =
            run_scripted_search_command("server-percentiles", vec![0.0625, 0.03125, 0.125]);
        assert_eq!(
            served, 3,
            "one request per query on the pass's single connection"
        );
        let text = std::fs::read_to_string(&metrics).expect("read metrics json");
        let v = json::parse(&text).expect("metrics json parses");
        let server_p50 = v
            .get("server_p50_ms")
            .and_then(Value::as_f64)
            .expect("server_p50_ms recorded");
        assert!(
            (server_p50 - 62.5).abs() < 1e-9,
            "server p50 must be the scripted 62.5 ms, got {server_p50}"
        );
        let server_p99 = v
            .get("server_p99_ms")
            .and_then(Value::as_f64)
            .expect("server_p99_ms recorded");
        assert!(
            (server_p99 - 123.75).abs() < 1e-9,
            "server p99 must be the scripted 123.75 ms (type-7 at 0.99), got {server_p99}"
        );
        // the scripted hits are exactly the truth ids
        assert_eq!(
            v.get("recall_at_10").and_then(Value::as_f64),
            Some(1.0),
            "scripted ids 0..=9 against truth rows 0..=9"
        );
        // Column independence (this test's own guard): the client
        // column is loopback transport time (microseconds), so its p50
        // cannot coincide with the scripted server p50 — if it ever
        // did, the assertions above could not tell the two columns
        // apart. If a pathologically slow host makes this flaky, scale
        // the scripted times by 16x (binary-exact again) rather than
        // dropping the check (card B-0-F7-F2 Work 4).
        let client_p50 = v
            .get("p50_ms")
            .and_then(Value::as_f64)
            .expect("p50_ms recorded");
        assert!(
            (client_p50 - 62.5).abs() >= 1e-6,
            "client p50 {client_p50} is suspiciously equal to the server p50; \
             the two columns must be independent measurements"
        );
    }

    #[test]
    fn search_command_csv_rows_pair_client_and_server_per_query() {
        // Card B-0-F7-F3 (verdict B-0-F7-F2 finding 1, gate mutant
        // G3-CSV-AFTER-CLIENT-SORT-ONLY): one CSV row must carry ONE
        // query's client time next to the SAME query's server time.
        // G3 moves the latency write between the command's two sorts,
        // so the client column comes out sorted while the server
        // column keeps query order — every property the F2 tests
        // assert still held, and the full suite passed 115/115,
        // because no test could tell which query a client field
        // belonged to. Here query 0 is made deterministically the
        // slowest on the client side: the scripted server sleeps
        // 200 ms before answering it (the only sleep this module
        // owns; server thread only, this card's relaxation of
        // B-0-F7's no-sleep rule) while its scripted server time
        // stays binary-exact. Row 1 must carry both. Lower bound
        // only: a query's client time is at least the delay the
        // server slept before answering it, which holds on any
        // host, however slow; no upper bound is asserted anywhere.
        let (_metrics, latency, served) = run_scripted_search_command_delayed(
            "csv-rows-pair",
            vec![
                (0.0625, Duration::from_millis(200)),
                (0.03125, Duration::ZERO),
                (0.125, Duration::ZERO),
            ],
        );
        assert_eq!(
            served, 3,
            "one request per query on the pass's single connection"
        );
        let text = std::fs::read_to_string(&latency).expect("read latency csv");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4, "header + one row per query:\n{text}");
        // Row 1 IS query 0. Its server field is query 0's scripted
        // time, string-exact (binary-exact seconds → ms), and its
        // client field must include the 200 ms the server slept
        // before answering query 0 — that is the pairing. Under G3
        // the client column is sorted ascending, so row 1 gets the
        // fastest query's loopback time instead, far below 200 ms;
        // under G3b-CLIENT-REVERSED query 0's slow time lands in
        // row 3.
        assert_eq!(
            lines[1].split(',').nth(1),
            Some("62.500000"),
            "row 1 must carry query 0's server time, in ms:\n{text}"
        );
        let row1_client: f64 = lines[1]
            .split(',')
            .next()
            .expect("row 1 client field present")
            .parse()
            .expect("row 1 client field parses as a number");
        assert!(
            row1_client >= 200.0,
            "row 1's client time must include the 200 ms the server slept \
             before answering query 0 — the client field of this row belongs \
             to query 0, and a sorted (G3) or reversed (G3b) client column \
             would put a fast query's time here; got {row1_client}\n{text}"
        );
        // Rows 2 and 3 are queries 1 and 2: the server column keeps
        // query order (O8-CSV-AFTER-SORT sorts it and fails here and
        // in search_command_records_server_ms_per_query_in_order).
        assert_eq!(
            lines[2].split(',').nth(1),
            Some("31.250000"),
            "row 2 must carry query 1's server time, in ms:\n{text}"
        );
        assert_eq!(
            lines[3].split(',').nth(1),
            Some("125.000000"),
            "row 3 must carry query 2's server time, in ms:\n{text}"
        );
        // The only other claim about the client column: every field
        // is finite and >= 0 — no upper bound, no ordering between
        // rows 2 and 3.
        for (i, row) in lines[1..].iter().enumerate() {
            let client: f64 = row
                .split(',')
                .next()
                .expect("client field present")
                .parse()
                .expect("client field parses as a number");
            assert!(
                client.is_finite() && client >= 0.0,
                "row {i} client field must be finite and >= 0: {row}"
            );
        }
    }

    // ---- card B-0-F7-F1: index state in the metrics (J4) ----

    #[test]
    fn index_state_rides_in_the_pass_metrics() {
        // The settle's numbers must survive into the recorded metrics
        // (only numeric metrics make it through the runner's parser, so
        // the state travels as numbers). STATE-NOT-IN-METRICS (returning
        // the metrics unchanged) fails here.
        let metrics = qdrant_pass_metrics(
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
        .expect("cold pass records");
        let st = vector::IndexState::from_summary(
            vector::CollectionSummary {
                status: "green",
                points: 1_000_000,
                indexed: 1_000_000,
                segments: 8,
                update_queue: 0,
                optimizer_ok: true,
                indexing_threshold_kb: 1,
                flush_interval_sec: 5,
            },
            30.0,
            64.0,
            33,
        );
        let with = with_index_state(metrics, &st);
        assert_eq!(
            with.get("index_points").and_then(Value::as_f64),
            Some(1_000_000.0)
        );
        assert_eq!(
            with.get("index_indexed").and_then(Value::as_f64),
            Some(1_000_000.0)
        );
        assert_eq!(
            with.get("index_segments").and_then(Value::as_f64),
            Some(8.0)
        );
        assert_eq!(
            with.get("index_threshold_kb").and_then(Value::as_f64),
            Some(1.0)
        );
        assert_eq!(
            with.get("index_waited_s").and_then(Value::as_f64),
            Some(64.0)
        );
        // the pass's own numbers are untouched
        assert_eq!(with.get("qps").and_then(Value::as_f64), Some(65.0));
        assert_eq!(with.get("pass_kind").and_then(Value::as_str), Some("cold"));
    }

    #[test]
    fn metrics_without_index_state_are_unchanged() {
        // Without --index-state the metrics must be byte-identical to
        // the pre-F1 shape — pinned as an exact JSON string so any field
        // or ordering drift is caught, not just the absence of the new
        // keys.
        let v = qdrant_pass_metrics(
            "run",
            &QdrantPassNumbers {
                ef: 16,
                limit: 10,
                queries: 100,
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
        let text = json::to_string(&v).expect("serialize");
        assert_eq!(
            text,
            "{\"qps\":500,\"recall_at_10\":0.94,\"queries\":100,\"p50_ms\":1.8,\
\"p99_ms\":2.4,\"ef_search\":16,\"timing_note\":\"one persistent connection \
(HTTP keep-alive, TCP_NODELAY, single write per request) for the whole pass; \
warm-pass p50 sanity-checked at 10 ms\",\"pass_kind\":\"run\",\
\"limit\":10,\"server_p50_ms\":1.7,\"server_p99_ms\":2.1}"
        );
        // and with_index_state appends AFTER those fields, in order
        let st = vector::IndexState::from_summary(
            vector::CollectionSummary {
                status: "green",
                points: 100,
                indexed: 100,
                segments: 2,
                update_queue: 0,
                optimizer_ok: true,
                indexing_threshold_kb: 1,
                flush_interval_sec: 5,
            },
            0.0,
            0.0,
            1,
        );
        let with = json::to_string(&with_index_state(v, &st)).expect("serialize with state");
        assert!(with.ends_with(
            "\"server_p99_ms\":2.1,\"index_points\":100,\"index_indexed\":100,\
             \"index_segments\":2,\"index_threshold_kb\":1,\"index_waited_s\":0}"
        ));
    }

    // ---- card B-0-F7-F1: the reproducibility rule (J6) ----

    /// One series of a results document at one ef point: a cold pass
    /// whose metrics carry the index-state numbers (`cold_indexed`
    /// overrides `index_indexed`; `None` means fully indexed), and a
    /// warm aggregate with the three J6 medians plus `index_segments`.
    fn repro_series(ef: u64, qps: f64, p50: f64, recall: f64, cold_indexed: Option<f64>) -> Value {
        let points = 1_000_000.0;
        let indexed = cold_indexed.unwrap_or(points);
        let cold = Value::obj(vec![(
            "metrics",
            Value::obj(vec![
                ("qps", Value::n(qps / 2.0)),
                ("index_points", Value::n(points)),
                ("index_indexed", Value::n(indexed)),
                ("index_segments", Value::n(8.0)),
            ]),
        )]);
        let med = |x: f64| {
            Value::obj(vec![
                ("samples", Value::Arr(vec![Value::n(x)])),
                ("median", Value::n(x)),
                ("ci95", Value::Arr(vec![Value::n(x), Value::n(x)])),
            ])
        };
        let warm = Value::obj(vec![
            ("runs", Value::n(1.0)),
            (
                "metrics",
                Value::obj(vec![
                    ("qps", med(qps)),
                    ("p50_ms", med(p50)),
                    ("recall_at_10", med(recall)),
                    ("index_segments", med(8.0)),
                ]),
            ),
        ]);
        Value::obj(vec![
            (
                "config",
                Value::Arr(vec![Value::Arr(vec![
                    Value::s("ef_search"),
                    Value::s(&ef.to_string()),
                ])]),
            ),
            ("cold", cold),
            ("warm", warm),
        ])
    }

    /// Gate round 1, finding R4 (mutant G6 was uncaught): a series
    /// whose warm aggregate carries the three J6 medians but NOT the
    /// index-state numbers (`index_segments`) — a pass without the
    /// state metrics, which J6 must refuse as INVALID rather than
    /// compare. Same shape as [`repro_series`] minus that one key.
    fn repro_series_warm_without_state(ef: u64, qps: f64, p50: f64, recall: f64) -> Value {
        let cold = Value::obj(vec![(
            "metrics",
            Value::obj(vec![
                ("qps", Value::n(qps / 2.0)),
                ("index_points", Value::n(1_000_000.0)),
                ("index_indexed", Value::n(1_000_000.0)),
                ("index_segments", Value::n(8.0)),
            ]),
        )]);
        let med = |x: f64| {
            Value::obj(vec![
                ("samples", Value::Arr(vec![Value::n(x)])),
                ("median", Value::n(x)),
                ("ci95", Value::Arr(vec![Value::n(x), Value::n(x)])),
            ])
        };
        let warm = Value::obj(vec![
            ("runs", Value::n(1.0)),
            (
                "metrics",
                Value::obj(vec![
                    ("qps", med(qps)),
                    ("p50_ms", med(p50)),
                    ("recall_at_10", med(recall)),
                ]),
            ),
        ]);
        Value::obj(vec![
            (
                "config",
                Value::Arr(vec![Value::Arr(vec![
                    Value::s("ef_search"),
                    Value::s(&ef.to_string()),
                ])]),
            ),
            ("cold", cold),
            ("warm", warm),
        ])
    }

    /// A results document with the J6 provenance fields.
    fn repro_run(profile: &str, voided: bool, series: Vec<Value>) -> Value {
        Value::obj(vec![
            ("row", Value::s("vector-sift1m-qdrant")),
            (
                "digest",
                Value::obj(vec![
                    ("init", Value::s("count=1000000 sample=128 fnv=x")),
                    ("voided", Value::Bool(voided)),
                ]),
            ),
            (
                "provenance",
                Value::obj(vec![
                    ("versions", Value::s("qdrant 1.19.2")),
                    (
                        "nbench_build",
                        Value::obj(vec![("profile", Value::s(profile))]),
                    ),
                ]),
            ),
            ("series", Value::Arr(series)),
        ])
    }

    #[test]
    fn repro_verdict_applies_the_preregistered_rule_verbatim() {
        // Two ef points, base numbers from the committed 2026-10-31 row.
        let base = || {
            (
                repro_run(
                    "release",
                    false,
                    vec![
                        repro_series(16, 2404.4, 0.34433, 0.9410, None),
                        repro_series(64, 664.0, 1.566, 0.9951, None),
                    ],
                ),
                repro_run(
                    "release",
                    false,
                    vec![
                        repro_series(16, 2404.4, 0.34433, 0.9410, None),
                        repro_series(64, 664.0, 1.566, 0.9951, None),
                    ],
                ),
            )
        };

        // identical runs → REPRODUCED, one table line per ef and metric
        let (a, b) = base();
        let v = repro_verdict(&a, &b);
        assert_eq!(v.word, "REPRODUCED");
        assert_eq!(
            v.lines.len(),
            6,
            "two ef points x three metrics: {:?}",
            v.lines
        );
        assert!(v.lines.iter().all(|l| l.contains("PASS")), "{:?}", v.lines);

        // qps 115 vs 100 → REPRODUCED (exactly 15% of min)
        let a = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 115.0, 0.34433, 0.9410, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        let b = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 100.0, 0.34433, 0.9410, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        assert_eq!(repro_verdict(&a, &b).word, "REPRODUCED");

        // 115.1 vs 100 → NOT-REPRODUCED (REPRO-MAX-BASE's mutant lets
        // this through and fails here)
        let a2 = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 115.1, 0.34433, 0.9410, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        let v = repro_verdict(&a2, &b);
        assert_eq!(v.word, "NOT-REPRODUCED");
        assert!(
            v.lines
                .iter()
                .any(|l| l.contains("ef16 qps") && l.contains("FAIL")),
            "{:?}",
            v.lines
        );

        // 100 vs 115 → same as 115 vs 100 (symmetry)
        assert_eq!(repro_verdict(&b, &a).word, "REPRODUCED");
        assert_eq!(repro_verdict(&b, &a2).word, "NOT-REPRODUCED");

        // p50 alone out of bounds → NOT-REPRODUCED
        let p50_a = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 2404.4, 1.0, 0.9410, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        let p50_b = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 2404.4, 1.5, 0.9410, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        assert_eq!(repro_verdict(&p50_a, &p50_b).word, "NOT-REPRODUCED");

        // recall 0.9410 vs 0.9430 → REPRODUCED (exactly 0.002)
        let rec_a = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9410, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        let rec_b = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9430, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        assert_eq!(repro_verdict(&rec_a, &rec_b).word, "REPRODUCED");

        // recall 0.9410 vs 0.9431 → NOT-REPRODUCED (REPRO-RECALL-SKIPPED
        // passes this and fails here)
        let rec_c = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9431, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        assert_eq!(repro_verdict(&rec_a, &rec_c).word, "NOT-REPRODUCED");

        // all bounds fine at ef16 but qps 2x off at the second ef →
        // NOT-REPRODUCED (REPRO-EF16-ONLY passes this and fails here)
        let ef64_b = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9410, None),
                repro_series(64, 332.0, 1.566, 0.9951, None),
            ],
        );
        let (a3, _) = base();
        let v = repro_verdict(&a3, &ef64_b);
        assert_eq!(v.word, "NOT-REPRODUCED");
        assert!(
            v.lines
                .iter()
                .any(|l| l.contains("ef64 qps") && l.contains("FAIL")),
            "{:?}",
            v.lines
        );

        // ef sets differ → NOT-REPRODUCED
        let efs_b = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9410, None),
                repro_series(128, 409.0, 2.450, 0.9989, None),
            ],
        );
        assert_eq!(repro_verdict(&a3, &efs_b).word, "NOT-REPRODUCED");

        // run B voided → INVALID
        let voided_b = repro_run(
            "release",
            true,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9410, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        let v = repro_verdict(&a3, &voided_b);
        assert_eq!(v.word, "INVALID");
        assert!(
            v.lines.iter().any(|l| l.contains("voided")),
            "{:?}",
            v.lines
        );

        // run A profile debug → INVALID
        let debug_a = repro_run(
            "debug",
            false,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9410, None),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        let (_, b4) = base();
        let v = repro_verdict(&debug_a, &b4);
        assert_eq!(v.word, "INVALID");
        assert!(
            v.lines.iter().any(|l| l.contains("profile")),
            "{:?}",
            v.lines
        );

        // a cold pass with index_indexed != index_points → INVALID
        let tail_b = repro_run(
            "release",
            false,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9410, Some(984_064.0)),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        let v = repro_verdict(&a3, &tail_b);
        assert_eq!(v.word, "INVALID");
        assert!(
            v.lines
                .iter()
                .any(|l| l.contains("index_indexed") && l.contains("ef16")),
            "{:?}",
            v.lines
        );

        // a warm aggregate lacking index_segments → INVALID (gate
        // round 1 finding R4: dropping this arm passed 130/130)
        let nostate_b = repro_run(
            "release",
            false,
            vec![
                repro_series_warm_without_state(16, 2404.4, 0.34433, 0.9410),
                repro_series(64, 664.0, 1.566, 0.9951, None),
            ],
        );
        let v = repro_verdict(&a3, &nostate_b);
        assert_eq!(v.word, "INVALID");
        assert!(
            v.lines
                .iter()
                .any(|l| l.contains("index_segments") && l.contains("ef16")),
            "{:?}",
            v.lines
        );

        // INVALID wins over a bounds failure (REPRO-INVALID-LAST checks
        // bounds first, answers NOT-REPRODUCED here, and fails)
        let bad_and_voided = repro_run(
            "release",
            true,
            vec![
                repro_series(16, 2404.4, 0.34433, 0.9410, None),
                repro_series(64, 332.0, 1.566, 0.9951, None),
            ],
        );
        let v = repro_verdict(&a3, &bad_and_voided);
        assert_eq!(v.word, "INVALID");
    }
}
