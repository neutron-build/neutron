//! SIFT1M vector workload support (ann-benchmarks dataset and protocol).
//!
//! Invariants owned here:
//! - fvecs/ivecs decoding validates every length field against the
//!   remaining bytes, so a truncated dataset is an error, never a short
//!   vector that would silently skew recall (PLAN-v3 §5.2 rule 4 in
//!   spirit: wrong answers void the run).
//! - recall@10 is computed against the dataset's own ground truth with
//!   0-based ids, the same convention the groundtruth file uses.
//! - Qdrant is driven over its REST API with `wait=true` writes and
//!   collection status polled to `green` before any search is timed, so
//!   the QPS numbers never include background indexing. Readiness is
//!   [`collection_ready`] (green + all points; tail segments may stay
//!   unindexed), a pure seam unit-tested against the alternative
//!   predicate it replaced.
//! - The timed search pass must measure Qdrant, not TCP mechanics:
//!   [`qdrant_latency_sanity`] rejects a per-query p50 above
//!   [`QDRANT_P50_SANITY_LIMIT_MS`] (gate verdict ddb00ba5 defect 1 — a
//!   Nagle + delayed-ACK stall measured 44 ms p50 / 22 qps where healthy
//!   single-write probes return in 1.6–1.9 ms).
//! - Every timed query records the server-side handling time reported
//!   by Qdrant next to the client-side latency (card B-0-F7 Work 1), so
//!   latency can be attributed: the pre-registered client-vs-server
//!   rule ([`diagnose_client_server_latency`], card B-0-F7 Work 4)
//!   decides whether a slow pass is a client/transport defect, a
//!   Qdrant-side question, or within tolerances. A response without a
//!   `time` field is a loud error — silently recording zeros would
//!   empty the client-vs-server table (the card's first named mutant).

use crate::error::{Error, Result};
use crate::http;
use crate::json::{self, Value};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

fn read_u32_le(b: &[u8], pos: usize, what: &'static str) -> Result<u32> {
    let end = pos + 4;
    if end > b.len() {
        return Err(Error::Parse {
            what,
            line: format!("truncated at byte {pos}"),
        });
    }
    Ok(u32::from_le_bytes([
        b[pos],
        b.get(pos + 1).copied().unwrap_or(0),
        b.get(pos + 2).copied().unwrap_or(0),
        b.get(pos + 3).copied().unwrap_or(0),
    ]))
}

fn read_f32_le(b: &[u8], pos: usize) -> f32 {
    f32::from_le_bytes([b[pos], b[pos + 1], b[pos + 2], b[pos + 3]])
}

fn read_i32_le(b: &[u8], pos: usize) -> i32 {
    i32::from_le_bytes([b[pos], b[pos + 1], b[pos + 2], b[pos + 3]])
}

/// Read an fvecs file (dim:u32, then dim f32 per vector).
pub fn read_fvecs(path: &Path) -> Result<Vec<Vec<f32>>> {
    let b = std::fs::read(path).map_err(Error::Io)?;
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < b.len() {
        let dim = read_u32_le(&b, pos, "fvecs dim")? as usize;
        pos += 4;
        let end = pos + dim * 4;
        if end > b.len() || dim == 0 || dim > 100_000 {
            return Err(Error::Parse {
                what: "fvecs",
                line: format!("bad dimension {dim} at offset {pos}"),
            });
        }
        let mut v = Vec::with_capacity(dim);
        for i in 0..dim {
            v.push(read_f32_le(&b, pos + i * 4));
        }
        out.push(v);
        pos = end;
    }
    Ok(out)
}

/// Read an ivecs file (dim:u32, then dim i32 per row).
pub fn read_ivecs(path: &Path) -> Result<Vec<Vec<i32>>> {
    let b = std::fs::read(path).map_err(Error::Io)?;
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < b.len() {
        let dim = read_u32_le(&b, pos, "ivecs dim")? as usize;
        pos += 4;
        let end = pos + dim * 4;
        if end > b.len() || dim == 0 || dim > 100_000 {
            return Err(Error::Parse {
                what: "ivecs",
                line: format!("bad dimension {dim} at offset {pos}"),
            });
        }
        let mut v = Vec::new();
        for i in 0..dim {
            v.push(read_i32_le(&b, pos + i * 4));
        }
        out.push(v);
        pos = end;
    }
    Ok(out)
}

/// recall@k: mean over queries of |distinct(result[0..k]) ∩ truth_topk| / k.
/// Duplicated ids in a result cannot inflate recall.
pub fn recall_at_k(results: &[Vec<i64>], truth: &[Vec<i32>], k: usize) -> Result<f64> {
    if results.len() != truth.len() || results.is_empty() || k == 0 {
        return Err(Error::Usage(format!(
            "recall shape mismatch: {} results, {} truth rows, k={k}",
            results.len(),
            truth.len()
        )));
    }
    let mut sum = 0.0f64;
    for (got, want) in results.iter().zip(truth.iter()) {
        let want_k: std::collections::HashSet<i64> =
            want.iter().take(k).map(|&x| x as i64).collect();
        let distinct: std::collections::HashSet<i64> = got.iter().take(k).copied().collect();
        let hits = distinct.iter().filter(|id| want_k.contains(id)).count();
        sum += hits.min(k) as f64 / k as f64;
    }
    Ok(sum / results.len() as f64)
}

// ---- Qdrant REST ----

/// Attempts for one idempotent load-path request before giving up.
const LOAD_RETRY_ATTEMPTS: usize = 5;

/// Backoff before load-path retry `attempt` (0-based): 1s, 2s, 4s, 8s.
fn load_retry_backoff(attempt: usize) -> Duration {
    Duration::from_secs(1u64 << attempt.min(3))
}

/// A status the server may recover from on an idempotent re-send.
fn is_transient_status(status: u16) -> bool {
    status >= 500 || status == 429
}

/// Perform one Qdrant request with bounded retries, for the *load path
/// only* (collection create/delete, point upserts, status polls — all
/// idempotent by construction).
///
/// Invariant owned here: measurement requests never retry. Transports
/// hiccup during long bulk loads — the 2026-10-18 SIFT1M upload lost one
/// response mid-stream at ~564k/1M points with the server healthy
/// (client saw `no header terminator`) — and an hour of loading must not
/// be voided by one dropped connection. But [`qdrant_search`] is a timed
/// measurement pass: a transient failure there must surface as an error
/// (the row is re-run), never hide inside a re-send that the clock does
/// not account for.
fn request_with_retry(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    body: &[u8],
    timeout: Duration,
) -> Result<crate::http::Response> {
    request_with_retry_attempts(host, port, method, path, body, timeout, LOAD_RETRY_ATTEMPTS)
}

/// Same as [`request_with_retry`] with an explicit attempt budget, so
/// the give-up path is testable in milliseconds instead of the full
/// production backoff.
fn request_with_retry_attempts(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    body: &[u8],
    timeout: Duration,
    attempts: usize,
) -> Result<crate::http::Response> {
    let mut last_err: Option<Error> = None;
    for attempt in 0..attempts.max(1) {
        if attempt > 0 {
            let wait = load_retry_backoff(attempt - 1);
            eprintln!(
                "qdrant {method} {path}: attempt {}/{} failed ({}), retrying in {wait:?}",
                attempt,
                attempts,
                last_err.as_ref().map(|e| e.to_string()).unwrap_or_default()
            );
            std::thread::sleep(wait);
        }
        match http::request(host, port, method, path, body, timeout) {
            Ok(resp) if !is_transient_status(resp.status) => return Ok(resp),
            Ok(resp) => {
                last_err = Some(Error::Command {
                    name: format!("qdrant {method} (HTTP {})", resp.status),
                    status: resp.status as i32,
                    stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
                });
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Usage("retry loop made no attempts".into())))
}

/// Create the collection with the card's HNSW parameters, deleting any
/// previous collection of the same name first (idempotent reinit).
pub fn qdrant_create(
    host: &str,
    port: u16,
    collection: &str,
    dim: usize,
    m: u64,
    ef_construct: u64,
) -> Result<()> {
    // 404 (not found) is the expected answer on a fresh volume.
    let existing = http::request(
        host,
        port,
        "GET",
        &format!("/collections/{collection}"),
        b"",
        Duration::from_secs(30),
    );
    if let Ok(resp) = &existing
        && resp.status == 200
    {
        let del = request_with_retry(
            host,
            port,
            "DELETE",
            &format!("/collections/{collection}"),
            b"",
            Duration::from_secs(120),
        )?;
        if del.status != 200 {
            return Err(Error::Command {
                name: format!("qdrant delete collection (HTTP {})", del.status),
                status: del.status as i32,
                stderr_tail: String::from_utf8_lossy(&del.body).to_string(),
            });
        }
    }
    let body = json::to_string(&Value::obj(vec![
        (
            "vectors",
            Value::obj(vec![
                ("size", Value::n(dim as f64)),
                ("distance", Value::s("Euclid")),
            ]),
        ),
        (
            "hnsw_config",
            Value::obj(vec![
                ("m", Value::n(m as f64)),
                ("ef_construct", Value::n(ef_construct as f64)),
            ]),
        ),
    ]))?;
    let resp = request_with_retry(
        host,
        port,
        "PUT",
        &format!("/collections/{collection}"),
        body.as_bytes(),
        Duration::from_secs(60),
    )?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant create collection (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    Ok(())
}

fn points_batch_json(ids: impl Iterator<Item = usize>, vecs: &[Vec<f32>]) -> Result<String> {
    let mut points = Vec::with_capacity(vecs.len());
    for (v, id) in vecs.iter().zip(ids) {
        let coords: Vec<Value> = v.iter().map(|x| Value::n(*x as f64)).collect();
        points.push(Value::obj(vec![
            ("id", Value::n(id as f64)),
            ("vector", Value::Arr(coords)),
        ]));
    }
    json::to_string(&Value::obj(vec![("points", Value::Arr(points))]))
}

/// Upload a batch of points with `wait=true` (PUT upsert: newer Qdrant
/// rejects POST for this body shape). Idempotent, so it uses the
/// load-path retry: a dropped connection re-sends the same batch.
pub fn qdrant_upload(
    host: &str,
    port: u16,
    collection: &str,
    start_id: usize,
    vecs: &[Vec<f32>],
) -> Result<()> {
    let body = points_batch_json(start_id..start_id + vecs.len(), vecs)?;
    let resp = request_with_retry(
        host,
        port,
        "PUT",
        &format!("/collections/{collection}/points?wait=true"),
        body.as_bytes(),
        Duration::from_secs(300),
    )?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant upload (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    Ok(())
}

/// Build the JSON body of one search request (pure seam so the request
/// shape is unit-testable — card B-0-F7 Work 1/Mutation evidence: the
/// body must carry the pass's own `hnsw_ef`; a mutant that ignores `ef`
/// mislabels the diagnosis and every recorded pass).
///
/// `exact` swaps the HNSW hint for Qdrant's brute-force `exact` search
/// (card B-0-F7 recall control: an exact pass over the same collection
/// must return the ground-truth neighbours, which validates the
/// distance convention and the harness recall computation against the
/// live index state instead of trusting it).
pub fn search_request_body(vec: &[f32], limit: usize, ef: u64, exact: bool) -> Result<String> {
    let params = if exact {
        Value::obj(vec![("exact", Value::Bool(true))])
    } else {
        Value::obj(vec![("hnsw_ef", Value::n(ef as f64))])
    };
    json::to_string(&Value::obj(vec![
        (
            "vector",
            Value::Arr(vec.iter().map(|x| Value::n(*x as f64)).collect()),
        ),
        ("limit", Value::n(limit as f64)),
        ("with_payload", Value::Bool(false)),
        ("params", params),
    ]))
}

/// Search one vector over an existing keep-alive connection; returns
/// ids in rank order and the server-measured handling time. Used for
/// the timed search pass so the whole pass shares one socket (gate
/// nv3-B-0-gate-1 finding 5: per-query TCP setup made the Qdrant and
/// pgvector arms overhead-unmatched). Measurement requests never
/// retry: a transport failure here fails the row instead of hiding
/// inside a re-send.
///
/// Card B-0-F7 Work 1: the server's own `time` travels with the ids so
/// the pass can attribute client-vs-server latency.
pub fn qdrant_search_on(
    conn: &mut crate::http::Connection,
    collection: &str,
    vec: &[f32],
    limit: usize,
    ef: u64,
    exact: bool,
) -> Result<SearchOutcome> {
    let body = search_request_body(vec, limit, ef, exact)?;
    let resp = conn.request(
        "POST",
        &format!("/collections/{collection}/points/search"),
        body.as_bytes(),
    )?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant search (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    parse_search_response(&resp.body)
}

/// One search response, with the server's own accounting of how long
/// it took (card B-0-F7 Work 1).
#[derive(Debug)]
pub struct SearchOutcome {
    /// Hit ids in rank order.
    pub ids: Vec<i64>,
    /// Qdrant's top-level `time` field, in **seconds** (e.g. the
    /// live collection answered a status poll in `0.001054475`).
    pub server_time_s: f64,
}

/// Parse a search response into ids plus the server-side time.
///
/// Invariant owned here (card B-0-F7 Work 1): the `time` field is
/// **mandatory**. Qdrant puts it on every response; treating it as
/// optional would record a silent 0 for the server column and the
/// client-vs-server table would be empty — exactly the card's first
/// named instrumentation mutant — so its absence is a loud error.
pub fn parse_search_response(body: &[u8]) -> Result<SearchOutcome> {
    let v = json::parse(&String::from_utf8_lossy(body))?;
    let server_time_s = v.get("time").and_then(Value::as_f64).ok_or_else(|| {
        Error::Missing(
            "search response `time` field (card B-0-F7: server latency is recorded next \
                 to every client latency; a missing field means attribution is impossible)"
                .into(),
        )
    })?;
    Ok(SearchOutcome {
        ids: parse_search_hits(body)?,
        server_time_s,
    })
}

/// Parse a search response body into id lists.
fn parse_search_hits(body: &[u8]) -> Result<Vec<i64>> {
    let text = String::from_utf8_lossy(body).to_string();
    let v = json::parse(&text)?;
    let arr = v
        .get("result")
        .and_then(Value::as_arr)
        .ok_or_else(|| Error::Missing("search result array".into()))?;
    let mut ids = Vec::with_capacity(arr.len());
    for hit in arr {
        let id = hit
            .get("id")
            .and_then(Value::as_f64)
            .ok_or_else(|| Error::Missing("hit id".into()))?;
        ids.push(id as i64);
    }
    Ok(ids)
}

/// Poll the collection until it is `green` with all points present.
///
/// `indexed_vectors_count` equals `points_count` only when Qdrant's
/// optimizer decides every segment warrants an HNSW graph; with the
/// default optimizer settings small tail segments legitimately stay
/// unindexed (exact-scanned) while the collection is green. Readiness is
/// therefore green + all points; the indexed fraction is printed as
/// provenance into the row's init log.
pub fn qdrant_wait_green(
    host: &str,
    port: u16,
    collection: &str,
    expected_points: usize,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        // Status polls are pure reads: retry transient transport errors
        // so a long wait is not voided by one dropped poll response.
        let resp = request_with_retry(
            host,
            port,
            "GET",
            &format!("/collections/{collection}"),
            b"",
            Duration::from_secs(30),
        )?;
        if resp.status == 200 {
            let v = json::parse(&String::from_utf8_lossy(&resp.body))?;
            let result = v.get("result").cloned().unwrap_or(Value::Null);
            let status = result
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let points = result
                .get("points_count")
                .and_then(Value::as_f64)
                .unwrap_or(0.0) as usize;
            let indexed = result
                .get("indexed_vectors_count")
                .and_then(Value::as_f64)
                .unwrap_or(0.0) as usize;
            if collection_ready(&status, points, indexed, expected_points) {
                eprintln!(
                    "collection green: {points} points, {indexed} indexed vectors \
                     ({:.1}% in HNSW segments; the remainder is exact-scanned, which \
                     is Qdrant's default optimizer policy for small tail segments)",
                    100.0 * indexed as f64 / points.max(1) as f64
                );
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            return Err(Error::Command {
                name: "qdrant wait green".into(),
                status: 1,
                stderr_tail: format!("collection not green after {timeout:?}"),
            });
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Readiness of a Qdrant collection: `green` **and** all expected points
/// present.
///
/// Kept pure so the predicate is a unit-tested seam (gate verdict
/// ddb00ba5 left the round-2 mutant M-D — reverting this to the older
/// `indexed_vectors_count == points_count` bar — uncaught; the first
/// assertion below is exactly the tail-segment state that predicate
/// rejects). `indexed` is accepted (and ignored) because it is part of
/// the status document the caller already parsed; it is printed as
/// provenance by [`qdrant_wait_green`], never gated on.
pub fn collection_ready(
    status: &str,
    points: usize,
    indexed: usize,
    expected_points: usize,
) -> bool {
    let _ = indexed;
    status == "green" && points == expected_points
}

/// Fetch a collection's info document (`GET /collections/{c}`) and
/// return its `result` object verbatim — status, counts, segment
/// count, config and optimizer state (card B-0-F7 Work 2).
pub fn qdrant_collection_info(host: &str, port: u16, collection: &str) -> Result<Value> {
    let resp = request_with_retry(
        host,
        port,
        "GET",
        &format!("/collections/{collection}"),
        b"",
        Duration::from_secs(30),
    )?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant collection info (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    let v = json::parse(&String::from_utf8_lossy(&resp.body))?;
    v.get("result")
        .cloned()
        .ok_or_else(|| Error::Missing("collection info result".into()))
}

/// The index-state facts a card B-0-F7 diagnosis needs from a
/// collection info `result` document (Work 2), as a pure seam.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CollectionSummary {
    /// Collection status as reported (`green`/`yellow`/`grey`/`red`).
    pub status: &'static str,
    /// `points_count`.
    pub points: usize,
    /// `indexed_vectors_count` (may exceed `points` mid-optimization:
    /// merging segments builds the new HNSW index while the old ones
    /// still count — the preserved chain10 collection reported
    /// 3 861 632 indexed for 1 000 000 points).
    pub indexed: usize,
    /// `segments_count` — how many segments a search fans out to.
    pub segments: usize,
    /// `update_queue.length`.
    pub update_queue: usize,
}

/// Classify a collection status string into the four values Qdrant
/// reports (anything else is kept as `other`, never guessed).
fn static_status(s: &str) -> &'static str {
    match s {
        "green" => "green",
        "yellow" => "yellow",
        "grey" => "grey",
        "red" => "red",
        _ => "other",
    }
}

/// Read a dotted numeric field (`a.b`) from a nested object.
fn nested_num(v: &Value, path: &[&str]) -> Result<usize> {
    let mut cur = v;
    for (i, key) in path.iter().enumerate() {
        cur = cur
            .get(key)
            .ok_or_else(|| Error::Missing(path[..=i].join(".")))?;
    }
    cur.as_f64()
        .map(|n| n as usize)
        .ok_or_else(|| Error::Missing(path.join(".")))
}

/// Extract the diagnosis summary from a collection info `result`
/// document. Pure; unit-tested against the live shape the preserved
/// chain10 collection returned (grey, 92 segments).
pub fn collection_summary(result: &Value) -> Result<CollectionSummary> {
    let raw_status = result
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Missing("collection status".into()))?;
    let num = |k: &str| -> Result<usize> {
        result
            .get(k)
            .and_then(Value::as_f64)
            .map(|n| n as usize)
            .ok_or_else(|| Error::Missing(k.into()))
    };
    Ok(CollectionSummary {
        status: static_status(raw_status),
        points: num("points_count")?,
        indexed: num("indexed_vectors_count")?,
        segments: num("segments_count")?,
        update_queue: nested_num(result, &["update_queue", "length"])?,
    })
}

/// Tolerated client-side excess over the server's own time (card
/// B-0-F7 Work 4, pre-registered): `c − s` above this means the client
/// or transport is adding real time — a harness defect to fix; the
/// sanity limit stays.
pub const CLIENT_EXCESS_TOLERANCE_MS: f64 = 2.0;

/// Server p50 above which a pass — with the client within tolerance —
/// is a Qdrant configuration question for a P judgement (card B-0-F7
/// Work 4, pre-registered). Any sanity-limit change needs a P verdict
/// with evidence.
pub const SERVER_P50_QUESTION_MS: f64 = 5.0;

/// The pre-registered client-vs-server rule (card B-0-F7 Work 4). The
/// card's arms are ordered; the first that fires decides, which is
/// what an if / else-if reading of the card prescribes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LatencyVerdict {
    /// `c − s > 2 ms`: client or transport defect — fix it; the limit
    /// stays.
    ClientExcess {
        /// Client-measured p50, ms.
        client_p50_ms: f64,
        /// Server-reported p50, ms.
        server_p50_ms: f64,
        /// `c − s`, ms.
        excess_ms: f64,
    },
    /// `c − s ≤ 2 ms` and `s > 5 ms`: the time is Qdrant's — a
    /// configuration question for a P judgement; any limit change
    /// needs a P verdict with evidence.
    ServerSide {
        /// Client-measured p50, ms.
        client_p50_ms: f64,
        /// Server-reported p50, ms.
        server_p50_ms: f64,
        /// `c − s`, ms.
        excess_ms: f64,
    },
    /// Neither arm fired: no defect indicated by this rule.
    WithinTolerances {
        /// Client-measured p50, ms.
        client_p50_ms: f64,
        /// Server-reported p50, ms.
        server_p50_ms: f64,
        /// `c − s`, ms.
        excess_ms: f64,
    },
}

impl LatencyVerdict {
    /// The pass's `(client_p50_ms, server_p50_ms, excess_ms)`.
    pub fn numbers(&self) -> (f64, f64, f64) {
        match *self {
            LatencyVerdict::ClientExcess {
                client_p50_ms,
                server_p50_ms,
                excess_ms,
            }
            | LatencyVerdict::ServerSide {
                client_p50_ms,
                server_p50_ms,
                excess_ms,
            }
            | LatencyVerdict::WithinTolerances {
                client_p50_ms,
                server_p50_ms,
                excess_ms,
            } => (client_p50_ms, server_p50_ms, excess_ms),
        }
    }
}

/// Apply the card B-0-F7 Work 4 rule to one pass's p50s. A NaN input
/// is an error, never a verdict: a broken measurement must not
/// classify itself.
pub fn diagnose_client_server_latency(
    client_p50_ms: f64,
    server_p50_ms: f64,
) -> Result<LatencyVerdict> {
    if !client_p50_ms.is_finite() || !server_p50_ms.is_finite() {
        return Err(Error::Usage(format!(
            "latency diagnosis needs finite p50s, got client={client_p50_ms} server={server_p50_ms}"
        )));
    }
    let excess_ms = client_p50_ms - server_p50_ms;
    if excess_ms > CLIENT_EXCESS_TOLERANCE_MS {
        Ok(LatencyVerdict::ClientExcess {
            client_p50_ms,
            server_p50_ms,
            excess_ms,
        })
    } else if server_p50_ms > SERVER_P50_QUESTION_MS {
        Ok(LatencyVerdict::ServerSide {
            client_p50_ms,
            server_p50_ms,
            excess_ms,
        })
    } else {
        Ok(LatencyVerdict::WithinTolerances {
            client_p50_ms,
            server_p50_ms,
            excess_ms,
        })
    }
}

/// Per-query p50 upper bound for a healthy Qdrant search pass, in
/// milliseconds (gate verdict ddb00ba5 defect 1).
///
/// Basis: single-write probes against the running SIFT1M collection
/// returned in 1.6–1.9 ms, and a Nagle + delayed-ACK stall measured
/// p50 44.07 ms uniformly — 10 ms separates the two regimes with ~5x
/// margin on either side. A legitimate HNSW m=16 search on 1M SIFT
/// vectors stays comfortably below it; a pass above the limit is
/// measuring transport mechanics or a pathologically loaded host, and
/// the row must fail loudly rather than publish those numbers.
pub const QDRANT_P50_SANITY_LIMIT_MS: f64 = 10.0;

/// Whether a search pass of this kind must pass the latency sanity
/// check.
///
/// Cold passes are exempt: they are the first search after a container
/// restart, so they measure Qdrant re-opening its storage and warming
/// its caches by design (chain9 measured p50 15.37 ms cold on a
/// load-12 host with the fixed client, against ~2 ms warm) — and the
/// row's published §5.2 statistics are medians over **warm** runs,
/// which stay fully guarded. The default for an unknown kind is to
/// enforce (fail-closed): a missing marker can never silently disable
/// the check.
pub fn latency_sanity_enforced(pass_kind: &str) -> bool {
    pass_kind != "cold"
}

/// Sanity-check the per-query p50 of a Qdrant search pass (verdict
/// ddb00ba5 defect 1): `Ok` when the pass plausibly measured Qdrant, a
/// loud error when it more likely measured the transport.
///
/// Called by the `vec-qdrant-search` pass after every query latency is
/// in; the error fails the driver (and therefore the row) instead of
/// writing metrics that would poison every §5.3 vector ratio.
pub fn qdrant_latency_sanity(p50_ms: f64) -> Result<()> {
    // NaN is rejected explicitly: a NaN p50 is a broken measurement,
    // and `NaN > limit` would be false.
    if p50_ms.is_nan() || p50_ms > QDRANT_P50_SANITY_LIMIT_MS {
        return Err(Error::Command {
            name: "qdrant search latency sanity".into(),
            status: 1,
            stderr_tail: format!(
                "per-query p50 {p50_ms:.2} ms exceeds {:.0} ms — the pass is \
                 measuring transport mechanics (e.g. Nagle + delayed-ACK, ~40 ms/query \
                 as in gate verdict ddb00ba5 defect 1) or an overloaded host, not \
                 Qdrant (healthy single-write probes: 1.6-1.9 ms); refusing to record",
                QDRANT_P50_SANITY_LIMIT_MS
            ),
        });
    }
    Ok(())
}

/// Fetch the server's own version string from `GET /` (Qdrant reports
/// `{"title":..., "version":"1.x.y", ...}`). Used by the qdrant driver
/// to fill `versions.txt` (§5.2 rule 6; gate nv3-B-0-gate-1 finding 8:
/// the row previously shipped without any recorded version).
pub fn qdrant_version(host: &str, port: u16) -> Result<String> {
    let resp = http::request(host, port, "GET", "/", b"", Duration::from_secs(30))?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant version probe (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    parse_version_body(&resp.body)
}

/// Extract `qdrant <version>` from a `GET /` response body.
fn parse_version_body(body: &[u8]) -> Result<String> {
    let v = json::parse(&String::from_utf8_lossy(body))?;
    let version = v
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Missing("qdrant version field".into()))?;
    Ok(format!("qdrant {version}"))
}

/// Point count (for digests).
pub fn qdrant_count(host: &str, port: u16, collection: &str) -> Result<usize> {
    let resp = http::request(
        host,
        port,
        "POST",
        &format!("/collections/{collection}/points/count"),
        br#"{"exact": true}"#,
        Duration::from_secs(30),
    )?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant count (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    let v = json::parse(&String::from_utf8_lossy(&resp.body))?;
    parse_count_response(&v)
}

/// Parse a Qdrant count response body.
///
/// Current Qdrant wraps the number: `{"result": {"count": N}, ...}`;
/// versions before ~1.13 printed the bare number `{"result": N}`. Both
/// are accepted so a Qdrant upgrade changes the digest value, not the
/// harness (a shape change here is loud: `Missing("count")`).
pub fn parse_count_response(v: &Value) -> Result<usize> {
    let result = v
        .get("result")
        .ok_or_else(|| Error::Missing("count".into()))?;
    let n = result
        .as_f64()
        .or_else(|| result.get("count").and_then(Value::as_f64))
        .ok_or_else(|| Error::Missing("count".into()))?;
    Ok(n as usize)
}

// ---- pgvector ----

/// Emit CSV rows `id,"[v1,v2,...]"` for `COPY items FROM STDIN`.
pub fn emit_pg_copy(base: &[Vec<f32>], out: &mut dyn Write) -> Result<()> {
    for (i, v) in base.iter().enumerate() {
        let coords: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
        writeln!(out, "{},\"[{}]\"", i, coords.join(",")).map_err(Error::Io)?;
    }
    Ok(())
}

/// Build the SQL for one search pass: SET ef, then one top-k SELECT per
/// query vector, each printing a comma-joined id list.
///
/// The ORDER BY uses `<->` (pgvector's **L2** operator) because the
/// index is built `vector_l2_ops` and the SIFT1M ground truth is
/// Euclidean. Gate nv3-B-0-gate-1 defect 1: this emitted `<=>` (cosine)
/// for two commits — an operator the L2 index cannot serve, forcing a
/// 1M-row seq scan per query (ef_search a no-op) and ranking in the
/// wrong space against Euclidean truth. A unit test pins the operator.
pub fn emit_pg_search_sql(
    queries: &[Vec<f32>],
    ef: u64,
    k: usize,
    out: &mut dyn Write,
) -> Result<()> {
    writeln!(out, "SET hnsw.ef_search = {ef};").map_err(Error::Io)?;
    for q in queries {
        let coords: Vec<String> = q.iter().map(|x| format!("{x}")).collect();
        writeln!(
            out,
            "SELECT string_agg(id::text, ',') FROM \
             (SELECT id FROM items ORDER BY v <-> '[{}]'::vector LIMIT {k}) s;",
            coords.join(",")
        )
        .map_err(Error::Io)?;
    }
    Ok(())
}

/// Parse psql output of [`emit_pg_search_sql`] back into id lists.
pub fn parse_pg_search_output(text: &str, expected: usize) -> Result<Vec<Vec<i64>>> {
    let mut out = Vec::with_capacity(expected);
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let ids: Vec<i64> = line
            .split(',')
            .map(|t| t.trim().parse::<i64>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| Error::Parse {
                what: "pgvector ids",
                line: line.to_string(),
            })?;
        out.push(ids);
    }
    if out.len() != expected {
        return Err(Error::Parse {
            what: "pgvector ids",
            line: format!("expected {expected} result lines, got {}", out.len()),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Spawn a one-connection-per-thread TCP server that plays a scripted
    /// sequence of behaviors, one per accepted connection: `Drop` closes
    /// without responding (the observed 2026-10-18 failure shape), and
    /// `Status(u16)` answers with that status and a two-byte body.
    /// Returns the port and a connection counter.
    fn flaky_server(script: Vec<&'static str>) -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&count);
        std::thread::spawn(move || {
            for behavior in script {
                let Ok((mut conn, _)) = listener.accept() else {
                    break;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                // Drain the request until 100ms of quiet so the socket
                // never closes with unread data queued (that turns the
                // close into an RST and races the client's read).
                let _ = conn.set_read_timeout(Some(Duration::from_millis(100)));
                let mut buf = [0u8; 512];
                loop {
                    match conn.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                if behavior == "Drop" {
                    continue; // close without any response
                }
                if let Some(code) = behavior.strip_prefix("Status:") {
                    let status: u16 = code.parse().expect("status code");
                    let resp = format!("HTTP/1.1 {status} x\r\nContent-Length: 2\r\n\r\nok");
                    let _ = conn.write_all(resp.as_bytes());
                }
            }
        });
        (port, count)
    }

    #[test]
    fn load_retry_survives_dropped_connections() {
        // Two dropped responses, then a good one: the load path must
        // recover (this is the 2026-10-18 upload failure, replayed).
        let (port, count) = flaky_server(vec!["Drop", "Drop", "Status:200"]);
        let resp = request_with_retry_attempts(
            "127.0.0.1",
            port,
            "PUT",
            "/points?wait=true",
            b"{}",
            Duration::from_secs(5),
            5,
        )
        .expect("retry succeeds");
        assert_eq!(resp.status, 200);
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn load_retry_gives_up_after_the_budget() {
        let (port, count) = flaky_server(vec!["Drop", "Drop", "Drop"]);
        let r = request_with_retry_attempts(
            "127.0.0.1",
            port,
            "PUT",
            "/points?wait=true",
            b"{}",
            Duration::from_secs(5),
            2,
        );
        assert!(r.is_err());
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn client_errors_are_not_retried() {
        // A 400 is the server's answer, not a hiccup: exactly one
        // connection, and the caller sees the status to report it.
        let (port, count) = flaky_server(vec!["Status:400"]);
        let resp = request_with_retry_attempts(
            "127.0.0.1",
            port,
            "PUT",
            "/points",
            b"{}",
            Duration::from_secs(5),
            5,
        )
        .expect("response returned without retry");
        assert_eq!(resp.status, 400);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn transient_status_classification() {
        for s in [200u16, 301, 400, 404, 418] {
            assert!(!is_transient_status(s), "{s} must not retry");
        }
        for s in [429u16, 500, 502, 503, 504] {
            assert!(is_transient_status(s), "{s} must retry");
        }
        assert_eq!(load_retry_backoff(0), Duration::from_secs(1));
        assert_eq!(load_retry_backoff(1), Duration::from_secs(2));
        assert_eq!(load_retry_backoff(9), Duration::from_secs(8));
    }

    #[test]
    fn readiness_is_green_plus_all_points_not_indexed_equality() {
        // The tail-segment state the old predicate (verdict round-2
        // mutant M-D: `indexed_vectors_count == points_count`) rejected:
        // green with all 1M points and ~98.5% indexed IS ready — Qdrant's
        // default optimizer policy leaves small tail segments
        // exact-scanned. This assertion is what makes that mutant catchable.
        assert!(collection_ready("green", 1_000_000, 985_000, 1_000_000));
        // fully indexed and green is of course ready too
        assert!(collection_ready("green", 1_000_000, 1_000_000, 1_000_000));
        // points still arriving: not ready even if what exists is indexed
        assert!(!collection_ready("green", 999_999, 999_999, 1_000_000));
        // optimizer still running (yellow/grey): not ready at full count
        assert!(!collection_ready("yellow", 1_000_000, 1_000_000, 1_000_000));
        assert!(!collection_ready("grey", 0, 0, 1_000_000));
    }

    #[test]
    fn latency_sanity_separates_search_from_transport_stalls() {
        // Healthy passes (probes 1.6–1.9 ms; a loaded host a few ms
        // more) pass; the Nagle + delayed-ACK regime (~40 ms) fails.
        assert!(qdrant_latency_sanity(0.5).is_ok());
        assert!(qdrant_latency_sanity(1.7).is_ok());
        assert!(qdrant_latency_sanity(9.99).is_ok());
        assert!(
            qdrant_latency_sanity(10.0).is_ok(),
            "the limit itself is sane"
        );
        let err = qdrant_latency_sanity(44.07).expect_err("the defect-1 regime must fail");
        let text = format!("{err}");
        assert!(
            text.contains("latency sanity"),
            "error names the check: {text}"
        );
        // NaN cannot sneak through a `>` comparison
        assert!(
            qdrant_latency_sanity(f64::NAN).is_err(),
            "NaN p50 must not pass"
        );
    }

    fn write_tmp(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("nbench-tests");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let p = dir.join(format!("{}-{}", std::process::id(), name));
        std::fs::write(&p, bytes).expect("write");
        p
    }

    fn fvec_bytes(vectors: &[&[f32]]) -> Vec<u8> {
        let mut b = Vec::new();
        for v in vectors {
            b.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for x in *v {
                b.extend_from_slice(&x.to_le_bytes());
            }
        }
        b
    }

    #[test]
    fn fvecs_roundtrip() {
        let p = write_tmp("t.fvecs", &fvec_bytes(&[&[1.0, 2.0], &[3.5, -4.25]]));
        let v = read_fvecs(&p).expect("read");
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], vec![1.0, 2.0]);
        assert_eq!(v[1], vec![3.5, -4.25]);
    }

    #[test]
    fn fvecs_truncated_is_an_error() {
        let good = fvec_bytes(&[&[1.0, 2.0]]);
        let p = write_tmp("trunc.fvecs", &good[..good.len() - 1]);
        assert!(read_fvecs(&p).is_err());
        // and a bogus dim
        let p2 = write_tmp("bogusdim.fvecs", &99999u32.to_le_bytes());
        assert!(read_fvecs(&p2).is_err());
    }

    #[test]
    fn ivecs_roundtrip() {
        let mut b = Vec::new();
        b.extend_from_slice(&3u32.to_le_bytes());
        for x in [7i32, 8, 9] {
            b.extend_from_slice(&x.to_le_bytes());
        }
        let p = write_tmp("t.ivecs", &b);
        let v = read_ivecs(&p).expect("read");
        assert_eq!(v, vec![vec![7, 8, 9]]);
    }

    #[test]
    fn recall_counts_intersections() {
        let truth = vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10i32]];
        let got = vec![vec![1, 2, 3, 4, 5, 11, 12, 13, 14, 15i64]];
        let r = recall_at_k(&got, &truth, 10).expect("recall");
        assert!((r - 0.5).abs() < 1e-12);
        // duplicates in the result must not inflate recall
        let dup = vec![vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1i64]];
        let r2 = recall_at_k(&dup, &truth, 10).expect("recall");
        assert!((r2 - 0.1).abs() < 1e-12);
        assert!(recall_at_k(&got, &truth, 0).is_err());
        assert!(recall_at_k(&[], &[], 10).is_err());
    }

    #[test]
    fn pg_copy_and_search_roundtrip() {
        let base = vec![vec![1.0, 2.5], vec![-3.0, 4.0]];
        let mut csv = Vec::new();
        emit_pg_copy(&base, &mut csv).expect("copy");
        let text = String::from_utf8(csv).expect("utf8");
        assert_eq!(text, "0,\"[1,2.5]\"\n1,\"[-3,4]\"\n");

        let mut sql = Vec::new();
        emit_pg_search_sql(&base, 64, 10, &mut sql).expect("sql");
        let sql_text = String::from_utf8(sql).expect("utf8");
        assert!(sql_text.starts_with("SET hnsw.ef_search = 64;\n"));
        // The operator must be L2 (`<->`): it must match the
        // vector_l2_ops HNSW index and the Euclidean ground truth.
        // `<=>` (cosine) forces a seq scan and ranks in the wrong
        // space (gate nv3-B-0-gate-1 defect 1).
        assert!(sql_text.contains("ORDER BY v <-> '[1,2.5]'::vector LIMIT 10"));
        assert!(!sql_text.contains("<=>"), "cosine operator must not appear");
        assert!(sql_text.contains("ORDER BY v <-> '[-3,4]'::vector LIMIT 10"));

        let parsed = parse_pg_search_output("0,1\n1,0\n\n", 2).expect("ids");
        assert_eq!(parsed, vec![vec![0, 1], vec![1, 0]]);
        assert!(parse_pg_search_output("0,1\n", 2).is_err());
        assert!(parse_pg_search_output("oops\n", 1).is_err());
    }

    #[test]
    fn points_batch_json_shape() {
        let s = points_batch_json(5..7, &[vec![0.5, 1.5], vec![2.0, 3.0]]).expect("json");
        let v = json::parse(&s).expect("parse");
        let pts = v.get("points").and_then(Value::as_arr).expect("points");
        assert_eq!(pts.len(), 2);
        assert_eq!(pts[0].get("id").and_then(Value::as_f64), Some(5.0));
    }

    #[test]
    fn count_response_accepts_both_documented_shapes() {
        // Current Qdrant (pinned image, probed 2026-10-17) wraps the count
        // in an object; older versions printed the bare number.
        let wrapped =
            json::parse(r#"{"result":{"count":1000000},"status":"ok"}"#).expect("wrapped json");
        assert_eq!(parse_count_response(&wrapped).expect("wrapped"), 1_000_000);
        let bare = json::parse(r#"{"result":1000000,"status":"ok"}"#).expect("bare json");
        assert_eq!(parse_count_response(&bare).expect("bare"), 1_000_000);
        // anything else is a loud error, never a guess
        let bad = json::parse(r#"{"result":{"exact":true}}"#).expect("bad json");
        assert!(parse_count_response(&bad).is_err());
        assert!(parse_count_response(&Value::Null).is_err());
    }

    #[test]
    fn version_probe_parses_the_root_document() {
        // Shape captured from the pinned qdrant image's GET / output.
        let body =
            br#"{"title":"qdrant - vector search engine","version":"1.15.1","commit":"abc"}"#;
        assert_eq!(parse_version_body(body).expect("version"), "qdrant 1.15.1");
        // a body without a version field is a loud error, never a guess
        assert!(parse_version_body(br#"{"title":"x"}"#).is_err());
        assert!(parse_version_body(b"not json").is_err());
    }

    #[test]
    fn search_parses_hits_and_rejects_bad_bodies() {
        let hits = br#"{"result":[{"id":7,"score":1.0},{"id":3,"score":0.9}],"status":"ok"}"#;
        assert_eq!(parse_search_hits(hits).expect("ids"), vec![7i64, 3]);
        assert!(parse_search_hits(br#"{"result":{}}"#).is_err());
        assert!(parse_search_hits(br#"{"result":[{"score":1.0}]}"#).is_err());
        assert!(parse_search_hits(b"garbage").is_err());
    }

    #[test]
    fn search_response_records_the_server_time_next_to_the_ids() {
        // Card B-0-F7 Work 1, and the card's first named mutant
        // ("server-time not recorded -> the c−s table empty"): every
        // Qdrant response carries a top-level `time` in seconds, and
        // the parse must surface it beside the ids — or fail loudly
        // when it is missing. A silent 0 would make every pass look
        // like a pure client-side stall.
        let body = br#"{"result":[{"id":7,"score":1.0},{"id":3,"score":0.9}],"status":"ok","time":0.00154225}"#;
        let out = parse_search_response(body).expect("outcome");
        assert_eq!(out.ids, vec![7i64, 3]);
        assert!(
            (out.server_time_s - 0.00154225).abs() < 1e-12,
            "server time in seconds, full precision: {}",
            out.server_time_s
        );
        // a response without `time` is a loud error, never 0.0
        let no_time = br#"{"result":[{"id":7,"score":1.0}],"status":"ok"}"#;
        let err = parse_search_response(no_time)
            .expect_err("missing time field must fail the pass loudly");
        assert!(
            format!("{err}").contains("time"),
            "error names the missing field: {err}"
        );
        assert!(parse_search_response(b"garbage").is_err());
    }

    #[test]
    fn search_request_body_carries_the_pass_ef_and_the_exact_control() {
        // Card B-0-F7 mutation evidence (the card's second named
        // mutant, "recall recomputation ignoring ef"): the request the
        // client sends must carry THIS pass's `hnsw_ef`. A body that
        // ignores the requested ef mislabels every recorded pass —
        // recall numbers would be attributed to the wrong point of the
        // sweep, and the diagnosis table with them.
        let body = search_request_body(&[0.5, -1.25], 10, 16, false).expect("body");
        let v = json::parse(&body).expect("parse");
        assert_eq!(
            v.get("params")
                .and_then(|p| p.get("hnsw_ef"))
                .and_then(Value::as_f64),
            Some(16.0),
            "hnsw_ef must be the requested ef"
        );
        assert!(
            v.get("params").and_then(|p| p.get("exact")).is_none(),
            "a normal pass must not ask for exact"
        );
        let body64 = search_request_body(&[0.5], 10, 64, false).expect("body");
        let v64 = json::parse(&body64).expect("parse");
        assert_eq!(
            v64.get("params")
                .and_then(|p| p.get("hnsw_ef"))
                .and_then(Value::as_f64),
            Some(64.0),
            "ef flows through per pass, not a constant"
        );
        // the exact control swaps the hint for brute force
        let exact = search_request_body(&[0.5], 10, 16, true).expect("body");
        let ve = json::parse(&exact).expect("parse");
        assert_eq!(
            ve.get("params")
                .and_then(|p| p.get("exact"))
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(
            ve.get("params").and_then(|p| p.get("hnsw_ef")).is_none(),
            "an exact control must not also pin hnsw_ef"
        );
    }

    /// The `result` document the preserved chain10 collection returned
    /// on 2026-10-31 (restart + idle): grey, 92 segments,
    /// indexed_vectors_count 3.86x points_count, update_queue empty.
    /// This is the live index-state shape the diagnosis recorded.
    const CHAIN10_ERA_COLLECTION_INFO: &str = r#"{
        "status":"grey","optimizer_status":"ok",
        "indexed_vectors_count":3861632,"points_count":1000000,
        "segments_count":92,
        "config":{"params":{"vectors":{"size":128,"distance":"Euclid"},
          "shard_number":1,"replication_factor":1},
          "hnsw_config":{"m":16,"ef_construct":200,"full_scan_threshold":10000},
          "optimizer_config":{"indexing_threshold":10000,"default_segment_number":0}},
        "payload_schema":{},"update_queue":{"length":0}}"#;

    #[test]
    fn collection_summary_reads_the_live_chain10_era_shape() {
        // Card B-0-F7 Work 2: the summary must read exactly what the
        // server said — the 92-segment, 3.86x-indexed, grey state that
        // answers the card's recall question is data, not narrative.
        let v = json::parse(CHAIN10_ERA_COLLECTION_INFO).expect("fixture json");
        let s = collection_summary(&v).expect("summary");
        assert_eq!(s.status, "grey");
        assert_eq!(s.points, 1_000_000);
        assert_eq!(s.indexed, 3_861_632);
        assert_eq!(s.segments, 92);
        assert_eq!(s.update_queue, 0);
        // a settled collection reads just as truthfully
        let settled = json::parse(
            r#"{"status":"green","indexed_vectors_count":985000,"points_count":1000000,
                "segments_count":2,"update_queue":{"length":0}}"#,
        )
        .expect("settled json");
        let s2 = collection_summary(&settled).expect("summary");
        assert_eq!(s2.status, "green");
        assert_eq!(s2.segments, 2);
        // missing fields are loud errors, never zero-filled guesses
        assert!(collection_summary(&json::parse("{}").expect("empty")).is_err());
        assert!(
            collection_summary(
                &json::parse(r#"{"status":"green","points_count":1}"#).expect("partial")
            )
            .is_err()
        );
    }

    #[test]
    fn the_preregistered_client_server_rule_applies_verbatim() {
        use super::LatencyVerdict::*;
        // Arm order and boundaries, straight from card B-0-F7 Work 4:
        // 1. c − s > 2 ms  -> client or transport defect (fix it; the
        //    limit stays)
        // 2. else s > 5 ms -> Qdrant configuration question for P
        // 3. else          -> within tolerances
        let v = diagnose_client_server_latency(10.01, 9.95).expect("verdict");
        assert!(
            matches!(v, ServerSide { excess_ms, .. } if (excess_ms - 0.06).abs() < 1e-9),
            "chain10's refused warmup, if the server really took 9.95, is server-side: {v:?}"
        );
        // a 10 ms client p50 over a healthy 1.7 ms server is the client
        let v2 = diagnose_client_server_latency(10.01, 1.7).expect("verdict");
        assert!(
            matches!(v2, ClientExcess { excess_ms, .. } if (excess_ms - 8.31).abs() < 1e-9),
            "a slow client over a healthy server is client excess: {v2:?}"
        );
        // the tolerance boundary itself does not fire (strictly >)
        let boundary = diagnose_client_server_latency(3.0, 1.0).expect("verdict");
        assert!(matches!(boundary, WithinTolerances { .. }), "{boundary:?}");
        // one tick above it does
        let over = diagnose_client_server_latency(3.01, 1.0).expect("verdict");
        assert!(matches!(over, ClientExcess { .. }), "{over:?}");
        // arm 2: client within tolerance, server slow (boundary again
        // strict: s == 5 is within tolerances)
        let s5 = diagnose_client_server_latency(6.0, 5.0).expect("verdict");
        assert!(matches!(s5, WithinTolerances { .. }), "{s5:?}");
        let slow = diagnose_client_server_latency(10.0, 9.0).expect("verdict");
        assert!(matches!(slow, ServerSide { .. }), "{slow:?}");
        // when both arms' conditions hold, the card's first arm wins
        // (its if/else order)
        let both = diagnose_client_server_latency(12.0, 9.0).expect("verdict");
        assert!(matches!(both, ClientExcess { .. }), "{both:?}");
        // a healthy pass (chain7-era probes: c ≈ s ≈ 1.7) is within
        let healthy = diagnose_client_server_latency(1.9, 1.6).expect("verdict");
        assert!(matches!(healthy, WithinTolerances { .. }), "{healthy:?}");
        // numbers travel with the verdict
        let (c, s, d) = ClientExcess {
            client_p50_ms: 10.0,
            server_p50_ms: 2.0,
            excess_ms: 8.0,
        }
        .numbers();
        assert_eq!((c, s, d), (10.0, 2.0, 8.0));
        // NaN never classifies
        assert!(diagnose_client_server_latency(f64::NAN, 1.0).is_err());
        assert!(diagnose_client_server_latency(1.0, f64::NAN).is_err());
    }
}
