//! SIFT1M vector workload support (ann-benchmarks dataset and protocol).
//!
//! Invariants owned here:
//! - fvecs/ivecs decoding validates every length field against the
//!   remaining bytes, so a truncated dataset is an error, never a short
//!   vector that would silently skew recall (PLAN-v3 §5.2 rule 4 in
//!   spirit: wrong answers void the run).
//! - recall@10 is computed against the dataset's own ground truth with
//!   0-based ids, the same convention the groundtruth file uses.
//! - Qdrant is driven over its REST API with `wait=true` writes, and
//!   readiness is a **settled index**, not a green poll (card
//!   B-0-F7-F1 judgements J2/J3; verdict B-0-F7 finding 1: one green
//!   poll raced the optimizer and let the same inputs land in three
//!   different index states). After the upload the loader always
//!   applies the tail rule — PATCH
//!   `{"optimizers_config":{"indexing_threshold":1}}`
//!   ([`QDRANT_TAIL_INDEXING_THRESHOLD_KB`]) — and then
//!   [`qdrant_wait_settled`] requires green, optimizer ok, an empty
//!   update queue, all points present, `indexed == points`, and
//!   identical `(segments, indexed)` on every poll across a trailing
//!   window of `max(30 s, 2 × flush_interval_sec)`
//!   ([`settle_decision`], the pure seam). A collection whose state
//!   drifts across restarts is refused by [`same_index_state`] (J4).
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

/// The tail rule of card B-0-F7-F1 judgement J3 (pre-registered): the
/// value sent as `optimizers_config.indexing_threshold` (in KB — Qdrant
/// docs: "Maximum size (in kilobytes) of vectors allowed for plain
/// index, exceeding this threshold will enable vector indexing … To
/// disable vector indexing, set to `0`") after every upload, before any
/// readiness wait. 1 KB indexes every segment, including the tail a
/// default threshold (10000 KB ≈ 19.5k 128-d vectors) leaves
/// exact-scanned; that unindexed tail made the row's latency
/// non-deterministic (9x apart at ef16 — verdict B-0-F7 finding 1).
/// Unconditional, so every build follows one procedure, and the value
/// is recorded in provenance (§5.2 rules 2 and 6). **0 must never be
/// sent: it disables indexing entirely.**
pub const QDRANT_TAIL_INDEXING_THRESHOLD_KB: usize = 1;

/// The settle window of judgement J2: `max(30, 2 × flush_interval_sec)`
/// seconds. The optimizer's flush interval bounds how long a state can
/// hide from a status poll, so a window of twice it (never under 30 s)
/// is the shortest trailing history that can trust "nothing is still
/// moving".
pub fn settle_window_s(flush_interval_sec: usize) -> f64 {
    let twice_flush = 2.0 * flush_interval_sec as f64;
    let floor = 30.0;
    if twice_flush > floor {
        twice_flush
    } else {
        floor
    }
}

/// One status poll: the collection summary plus the wall-clock second
/// (relative to the start of the wait) it was taken at. `at_s` is what
/// gives the settle window its span.
#[derive(Clone, Copy, Debug)]
pub struct StatePoll {
    /// Seconds since the wait began.
    pub at_s: f64,
    /// The summary read at that moment.
    pub s: CollectionSummary,
}

/// The verdict of [`settle_decision`] (card B-0-F7-F1 J2/J3):
/// `Settled` only when the whole trailing window says the index is
/// done and stable; `Waiting(reason)` while it does not; `Refused`
/// when the collection was not built by this procedure at all (the
/// tail rule is absent, or there are more points than expected).
#[derive(Clone, Debug, PartialEq)]
pub enum Settle {
    /// Every J2 condition holds on every poll in the window.
    Settled,
    /// Not settled yet; the reason names the first failing condition
    /// and its values.
    Waiting(String),
    /// The collection cannot settle under this procedure (J3 not
    /// applied, or points above the expectation).
    Refused(String),
}

/// Apply the settle predicate of card B-0-F7-F1 judgement J2 (pure, so
/// the readiness rule is a unit-tested seam — the rule it replaces, a
/// single green poll, passed 98.5%-indexed collections that then cost
/// every query an exact scan).
///
/// - `Refused` when the **last** poll shows the tail rule was not
///   applied (`indexing_threshold_kb != QDRANT_TAIL_INDEXING_THRESHOLD_KB`)
///   or `points > expected_points` — waiting cannot fix either.
/// - `window_s == 0.0` considers only the last poll (the cheap
///   re-check warm passes make against an already-settled collection).
/// - `window_s > 0` takes the **shortest suffix** of `polls` whose span
///   (`last.at_s − first.at_s`) is ≥ `window_s`; if no suffix spans the
///   window the answer is `Waiting("history shorter than the window")`.
/// - `Settled` iff every poll in that suffix is green, optimizer-ok,
///   has an empty update queue, exactly `expected_points` points,
///   `indexed == points`, and all of them carry identical
///   `(segments, indexed)`. Otherwise `Waiting(reason)` naming the
///   first failing condition and its values.
pub fn settle_decision(polls: &[StatePoll], expected_points: usize, window_s: f64) -> Settle {
    let Some(last) = polls.last() else {
        return Settle::Waiting("no polls yet".into());
    };
    if last.s.indexing_threshold_kb != QDRANT_TAIL_INDEXING_THRESHOLD_KB {
        return Settle::Refused(format!(
            "indexing_threshold {} KB is not the pre-registered tail rule {} KB (J3): \
             this collection was not built by this procedure",
            last.s.indexing_threshold_kb, QDRANT_TAIL_INDEXING_THRESHOLD_KB
        ));
    }
    if last.s.points > expected_points {
        return Settle::Refused(format!(
            "points_count {} exceeds the expected {}",
            last.s.points, expected_points
        ));
    }
    // The suffix the J2 conditions must hold on. window_s <= 0 is the
    // one-poll mode; otherwise walk backwards until a suffix spans the
    // window (the first hit is the shortest). A 1e-9 slack keeps float
    // elapsed times from deciding a whole-second boundary.
    let suffix: &[StatePoll] = if window_s <= 0.0 {
        &polls[polls.len() - 1..]
    } else {
        let n = polls.len();
        let mut start = n; // exclusive end of the prefix to drop
        for k in 1..=n {
            let span = polls[n - 1].at_s - polls[n - k].at_s;
            if span + 1e-9 >= window_s {
                start = n - k;
                break;
            }
        }
        if start == n {
            return Settle::Waiting("history shorter than the window".into());
        }
        &polls[start..]
    };
    // Stability anchors are the LAST poll's numbers: every poll in the
    // window must agree with them on segments and indexed count (J2's
    // last condition), and the per-poll conditions are checked in J2's
    // order so the reason names the first failure.
    let (want_segments, want_indexed) = (last.s.segments, last.s.indexed);
    for p in suffix {
        let s = &p.s;
        if s.status != "green" {
            return Settle::Waiting(format!(
                "status {} at t={:.1}s (want green)",
                s.status, p.at_s
            ));
        }
        if !s.optimizer_ok {
            return Settle::Waiting(format!(
                "optimizer_status is not \"ok\" at t={:.1}s",
                p.at_s
            ));
        }
        if s.update_queue != 0 {
            return Settle::Waiting(format!(
                "update_queue {} at t={:.1}s (want 0)",
                s.update_queue, p.at_s
            ));
        }
        if s.points != expected_points {
            return Settle::Waiting(format!(
                "points {} != expected {} at t={:.1}s",
                s.points, expected_points, p.at_s
            ));
        }
        if s.indexed != s.points {
            return Settle::Waiting(format!(
                "indexed {} != points {} at t={:.1}s (the tail is not indexed)",
                s.indexed, s.points, p.at_s
            ));
        }
        if s.segments != want_segments {
            return Settle::Waiting(format!(
                "segments {} at t={:.1}s != {} on the other polls in the window",
                s.segments, p.at_s, want_segments
            ));
        }
        if s.indexed != want_indexed {
            return Settle::Waiting(format!(
                "indexed {} at t={:.1}s != {} on the other polls in the window",
                s.indexed, p.at_s, want_indexed
            ));
        }
    }
    Settle::Settled
}

/// The exact PATCH body of the J3 tail rule (pure so the bytes are
/// pinned by a test — a `0` here would silently disable indexing, and
/// a differently-shaped body would touch settings J3 forbids).
fn tail_indexing_body() -> String {
    let v = Value::obj(vec![(
        "optimizers_config",
        Value::obj(vec![(
            "indexing_threshold",
            Value::n(QDRANT_TAIL_INDEXING_THRESHOLD_KB as f64),
        )]),
    )]);
    // `json::to_string` only fails on non-finite numbers and the single
    // number here is the constant 1, so this cannot fail; the fallback
    // exists because panics are forbidden and would surface as the
    // server rejecting an empty body.
    json::to_string(&v).unwrap_or_default()
}

/// Apply the J3 tail rule: `PATCH /collections/{c}` with
/// `{"optimizers_config":{"indexing_threshold":1}}`. Sent after every
/// upload, before any readiness wait, unconditionally — one procedure
/// for every build. The PATCH is idempotent, so it uses the load-path
/// retry; a non-200 response is a loud error carrying the body.
pub fn qdrant_index_tail(host: &str, port: u16, collection: &str) -> Result<()> {
    let resp = request_with_retry(
        host,
        port,
        "PATCH",
        &format!("/collections/{collection}"),
        tail_indexing_body().as_bytes(),
        Duration::from_secs(60),
    )?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant index tail (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    Ok(())
}

/// The settled index state of a collection, as a `qdrant_wait_settled`
/// wait left it: the summary fields plus the wait's own facts (J4 —
/// the state that every restart must reproduce, and that rides in the
/// pass metrics as numbers).
#[derive(Clone, Debug, PartialEq)]
pub struct IndexState {
    /// Collection status at settle time (`green`, by construction of
    /// the settle rule).
    pub status: &'static str,
    /// `points_count`.
    pub points: usize,
    /// `indexed_vectors_count` (== `points` when settled).
    pub indexed: usize,
    /// `segments_count`.
    pub segments: usize,
    /// `update_queue.length` (0 when settled).
    pub update_queue: usize,
    /// `optimizer_status == "ok"`.
    pub optimizer_ok: bool,
    /// `config.optimizer_config.indexing_threshold` in KB.
    pub indexing_threshold_kb: usize,
    /// `config.optimizer_config.flush_interval_sec`.
    pub flush_interval_sec: usize,
    /// The window the settle used, seconds.
    pub window_s: f64,
    /// How long the wait took, seconds.
    pub waited_s: f64,
    /// Polls the wait took.
    pub polls: usize,
}

impl IndexState {
    /// Capture the outcome of a settle wait: the last poll's summary
    /// plus the wait's own facts.
    pub fn from_summary(s: CollectionSummary, window_s: f64, waited_s: f64, polls: usize) -> Self {
        IndexState {
            status: s.status,
            points: s.points,
            indexed: s.indexed,
            segments: s.segments,
            update_queue: s.update_queue,
            optimizer_ok: s.optimizer_ok,
            indexing_threshold_kb: s.indexing_threshold_kb,
            flush_interval_sec: s.flush_interval_sec,
            window_s,
            waited_s,
            polls,
        }
    }

    /// Serialize to the compact JSON written by `--state-out` (flat, so
    /// shell checks can `grep -o` the numbers out of it).
    pub fn to_value(&self) -> Value {
        Value::obj(vec![
            ("status", Value::s(self.status)),
            ("points", Value::n(self.points as f64)),
            ("indexed", Value::n(self.indexed as f64)),
            ("segments", Value::n(self.segments as f64)),
            ("update_queue", Value::n(self.update_queue as f64)),
            ("optimizer_ok", Value::Bool(self.optimizer_ok)),
            (
                "indexing_threshold_kb",
                Value::n(self.indexing_threshold_kb as f64),
            ),
            (
                "flush_interval_sec",
                Value::n(self.flush_interval_sec as f64),
            ),
            ("window_s", Value::n(self.window_s)),
            ("waited_s", Value::n(self.waited_s)),
            ("polls", Value::n(self.polls as f64)),
        ])
    }

    /// Parse the JSON [`IndexState::to_value`] wrote. A missing field
    /// is [`Error::Missing`], never a default — a state file that
    /// cannot be read in full must not be compared as if it were.
    pub fn from_value(v: &Value) -> Result<IndexState> {
        let status = v
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Missing("index state status".into()))?;
        let num = |k: &str| -> Result<usize> {
            v.get(k)
                .and_then(Value::as_f64)
                .map(|n| n as usize)
                .ok_or_else(|| Error::Missing(format!("index state {k}")))
        };
        let fnum = |k: &str| -> Result<f64> {
            v.get(k)
                .and_then(Value::as_f64)
                .ok_or_else(|| Error::Missing(format!("index state {k}")))
        };
        let optimizer_ok = v
            .get("optimizer_ok")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::Missing("index state optimizer_ok".into()))?;
        Ok(IndexState {
            status: static_status(status),
            points: num("points")?,
            indexed: num("indexed")?,
            segments: num("segments")?,
            update_queue: num("update_queue")?,
            optimizer_ok,
            indexing_threshold_kb: num("indexing_threshold_kb")?,
            flush_interval_sec: num("flush_interval_sec")?,
            window_s: fnum("window_s")?,
            waited_s: fnum("waited_s")?,
            polls: num("polls")?,
        })
    }

    /// The one-line form every settle prints (`vec-qdrant-load`,
    /// `vec-qdrant-settle`): the numbers of J2/J3/J4 at a glance.
    pub fn line(&self) -> String {
        format!(
            "index state: settled status={} points={} indexed={} segments={} \
             threshold_kb={} window_s={} waited_s={:.1}",
            self.status,
            self.points,
            self.indexed,
            self.segments,
            self.indexing_threshold_kb,
            self.window_s,
            self.waited_s
        )
    }
}

/// Poll the collection every 2 s until [`settle_decision`] says
/// `Settled` (card B-0-F7-F1 J2), and return the [`IndexState`] it
/// settled in.
///
/// `window: None` derives the window from the polled
/// `flush_interval_sec` ([`settle_window_s`], J2); `Some(0.0)` is the
/// one-poll re-check; any other `Some(w)` pins it. A `Refused`
/// collection fails at once (waiting cannot fix it); at `timeout` the
/// wait fails with the last `Waiting` reason and the last summary, so
/// the log shows exactly what the optimizer was still doing. Status
/// polls are idempotent reads through the load-path retry, so a
/// dropped poll response does not void a long wait.
pub fn qdrant_wait_settled(
    host: &str,
    port: u16,
    collection: &str,
    expected_points: usize,
    window: Option<f64>,
    timeout: Duration,
) -> Result<IndexState> {
    let start = Instant::now();
    let mut history: Vec<StatePoll> = Vec::new();
    loop {
        let info = qdrant_collection_info(host, port, collection)?;
        let summary = collection_summary(&info)?;
        let at_s = start.elapsed().as_secs_f64();
        history.push(StatePoll { at_s, s: summary });
        let window_s = window.unwrap_or_else(|| settle_window_s(summary.flush_interval_sec));
        match settle_decision(&history, expected_points, window_s) {
            Settle::Settled => {
                let waited_s = start.elapsed().as_secs_f64();
                let polls = history.len();
                return Ok(IndexState::from_summary(summary, window_s, waited_s, polls));
            }
            Settle::Refused(reason) => {
                return Err(Error::Command {
                    name: "qdrant settle".into(),
                    status: 1,
                    stderr_tail: reason,
                });
            }
            // At the timeout the wait fails with this (the last)
            // Waiting reason and the last summary, so the log shows
            // exactly what the optimizer was still doing.
            Settle::Waiting(reason) => {
                if start.elapsed() > timeout {
                    return Err(Error::Command {
                        name: "qdrant wait settled".into(),
                        status: 1,
                        stderr_tail: format!(
                            "not settled after {timeout:?}: {reason}; last poll: status={} \
                             points={} indexed={} segments={} update_queue={} threshold_kb={}",
                            summary.status,
                            summary.points,
                            summary.indexed,
                            summary.segments,
                            summary.update_queue,
                            summary.indexing_threshold_kb
                        ),
                    });
                }
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// The J4 state-equality seam: two settles (typically init and a
/// post-restart pass) describe the **same** index iff `points`,
/// `indexed`, `segments` and `indexing_threshold_kb` all agree. Wait
/// facts (`window_s`, `waited_s`, `polls`) are deliberately not
/// compared — they describe the wait, not the index. On a difference
/// the error names every differing field with both values, so the
/// refusal is its own evidence.
pub fn same_index_state(a: &IndexState, b: &IndexState) -> Result<()> {
    let mut diffs: Vec<String> = Vec::new();
    if a.points != b.points {
        diffs.push(format!("points {} vs {}", a.points, b.points));
    }
    if a.indexed != b.indexed {
        diffs.push(format!("indexed {} vs {}", a.indexed, b.indexed));
    }
    if a.segments != b.segments {
        diffs.push(format!("segments {} vs {}", a.segments, b.segments));
    }
    if a.indexing_threshold_kb != b.indexing_threshold_kb {
        diffs.push(format!(
            "indexing_threshold_kb {} vs {}",
            a.indexing_threshold_kb, b.indexing_threshold_kb
        ));
    }
    if diffs.is_empty() {
        return Ok(());
    }
    Err(Error::Command {
        name: "index state changed".into(),
        status: 1,
        stderr_tail: diffs.join("; "),
    })
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

/// The index-state facts a settle or diagnosis needs from a collection
/// info `result` document (card B-0-F7 Work 2; B-0-F7-F1 Work 1), as a
/// pure seam. Every field is mandatory: a missing field is
/// [`Error::Missing`], never a default — a status document that cannot
/// be read in full must not settle or diagnose as if it were green.
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
    /// `optimizer_status` is exactly the string `"ok"` (Qdrant reports
    /// `"ok"` or an error object such as `{"error": …}`; anything but
    /// the string is `false`, never an error, so a broken optimizer
    /// reads as not-ok and keeps the settle waiting).
    pub optimizer_ok: bool,
    /// `config.optimizer_config.indexing_threshold`, KB (the J3 knob).
    pub indexing_threshold_kb: usize,
    /// `config.optimizer_config.flush_interval_sec` (the J2 window
    /// derives from it).
    pub flush_interval_sec: usize,
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
/// chain10 collection returned (grey, 92 segments) and against an
/// optimizer error object.
pub fn collection_summary(result: &Value) -> Result<CollectionSummary> {
    let raw_status = result
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Missing("collection status".into()))?;
    let optimizer_ok = match result.get("optimizer_status") {
        // Qdrant reports "ok" or an error object; only the exact
        // string is ok (card B-0-F7-F1 J2).
        Some(Value::Str(s)) => s == "ok",
        Some(_) => false,
        None => return Err(Error::Missing("optimizer_status".into())),
    };
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
        optimizer_ok,
        indexing_threshold_kb: nested_num(
            result,
            &["config", "optimizer_config", "indexing_threshold"],
        )?,
        flush_interval_sec: nested_num(
            result,
            &["config", "optimizer_config", "flush_interval_sec"],
        )?,
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

    // ---- card B-0-F7-F1: the settle predicate (J2/J3) ----
    //
    // These tests replace `readiness_is_green_plus_all_points_not_
    // indexed_equality`, which pinned the "green + all points, ~98.5%
    // indexed is ready" rule that verdict B-0-F7 finding 1 found
    // non-deterministic (the gate's build settled 984,064 of 1,000,000
    // indexed and paid an exact scan on every query). The replacement
    // is stricter, never looser — that replacement is authorised by the
    // P card itself.

    /// A settled 1M-point collection under the J3 tail rule: green,
    /// optimizer ok, queue empty, fully indexed, 8 segments, threshold
    /// 1 KB, flush 5 s (the live shape the 2026-10-31 row recorded).
    fn settled_summary() -> CollectionSummary {
        CollectionSummary {
            status: "green",
            points: 1_000_000,
            indexed: 1_000_000,
            segments: 8,
            update_queue: 0,
            optimizer_ok: true,
            indexing_threshold_kb: QDRANT_TAIL_INDEXING_THRESHOLD_KB,
            flush_interval_sec: 5,
        }
    }

    /// Polls every 2 s from `from_s` to `to_s` inclusive, all carrying
    /// the settled summary.
    fn polls_between(from_s: f64, to_s: f64) -> Vec<StatePoll> {
        let mut out = Vec::new();
        let mut t = from_s;
        while t <= to_s + 1e-9 {
            out.push(StatePoll {
                at_s: t,
                s: settled_summary(),
            });
            t += 2.0;
        }
        out
    }

    #[test]
    fn settle_requires_the_whole_window() {
        // J2: every condition must hold across a trailing window, so a
        // perfect-but-short history is Waiting, not Settled. SETTLE-
        // ONE-GREEN (the old single-poll rule) settles both and fails
        // here.
        let short = polls_between(0.0, 28.0);
        assert_eq!(
            settle_decision(&short, 1_000_000, 30.0),
            Settle::Waiting("history shorter than the window".into())
        );
        let spans = polls_between(0.0, 30.0);
        assert_eq!(settle_decision(&spans, 1_000_000, 30.0), Settle::Settled);
        // a longer history than the window still settles (the suffix
        // covers it)
        let long = polls_between(0.0, 60.0);
        assert_eq!(settle_decision(&long, 1_000_000, 30.0), Settle::Settled);
        // no polls at all is Waiting, never a guess
        assert_eq!(
            settle_decision(&[], 1_000_000, 30.0),
            Settle::Waiting("no polls yet".into())
        );
    }

    #[test]
    fn settle_requires_a_full_index() {
        // Verdict B-0-F7 finding 1, the gate's build: green with all
        // points but 984,064 of 1,000,000 indexed, held for 60 s —
        // still Waiting. SETTLE-PARTIAL-INDEX (drop the
        // indexed == points condition) settles these and fails here.
        let mut polls = polls_between(0.0, 60.0);
        for p in &mut polls {
            p.s.indexed = 984_064;
        }
        assert!(matches!(
            settle_decision(&polls, 1_000_000, 30.0),
            Settle::Waiting(ref r) if r.contains("indexed") && r.contains("984064")
        ));
        // chain10's wedged state: indexed 3.86x points
        let mut wedged = polls_between(0.0, 60.0);
        for p in &mut wedged {
            p.s.indexed = 3_861_632;
        }
        assert!(matches!(
            settle_decision(&wedged, 1_000_000, 30.0),
            Settle::Waiting(_)
        ));
    }

    #[test]
    fn settle_requires_stable_segments_and_counts() {
        // One poll mid-window with 9 segments instead of 8: the window
        // caught a merge in progress, so it is not settled. SETTLE-
        // UNSTABLE-OK (drop the identical-(segments, indexed)
        // condition) settles the first case and fails here.
        let mut drift = polls_between(0.0, 30.0);
        drift[7].s.segments = 9;
        assert!(matches!(
            settle_decision(&drift, 1_000_000, 30.0),
            Settle::Waiting(ref r) if r.contains("segments")
        ));
        // likewise one poll with a different indexed count
        let mut other = polls_between(0.0, 30.0);
        other[7].s.indexed = 984_064;
        assert!(matches!(
            settle_decision(&other, 1_000_000, 30.0),
            Settle::Waiting(ref r) if r.contains("indexed")
        ));
    }

    #[test]
    fn settle_requires_green_ok_and_an_empty_queue() {
        // Each J2 condition fails its own poll mid-window; every one of
        // them must keep the settle Waiting. SETTLE-OPTIMIZER-IGNORED
        // (drop optimizer_ok) lets the third case settle and fails
        // here.
        type Case = (&'static str, fn(&mut CollectionSummary));
        let cases: Vec<Case> = vec![
            ("status", |s| s.status = "yellow"),
            ("status", |s| s.status = "grey"),
            ("optimizer", |s| s.optimizer_ok = false),
            ("update_queue", |s| s.update_queue = 3),
        ];
        for (needle, mutate) in cases {
            let mut polls = polls_between(0.0, 30.0);
            mutate(&mut polls[7].s);
            let verdict = settle_decision(&polls, 1_000_000, 30.0);
            assert!(
                matches!(&verdict, Settle::Waiting(r) if r.contains(needle)),
                "case {needle:?}: {verdict:?}"
            );
        }
    }

    #[test]
    fn settle_refuses_a_collection_without_the_tail_rule() {
        // J3 was not applied (the default threshold 10000): waiting
        // cannot fix it — Refused. TAIL-RULE-UNCHECKED (drop this arm)
        // turns this into Waiting/Settled and fails here.
        let mut untouched = polls_between(0.0, 30.0);
        for p in &mut untouched {
            p.s.indexing_threshold_kb = 10_000;
        }
        assert!(matches!(
            settle_decision(&untouched, 1_000_000, 30.0),
            Settle::Refused(_)
        ));
        // more points than expected is refused the same way
        let mut extra = polls_between(0.0, 30.0);
        for p in &mut extra {
            p.s.points = 1_000_001;
            p.s.indexed = 1_000_001;
        }
        assert!(matches!(
            settle_decision(&extra, 1_000_000, 30.0),
            Settle::Refused(_)
        ));
    }

    #[test]
    fn settle_with_zero_window_checks_one_poll() {
        // window 0 is the warm-pass re-check: one poll, the last one.
        // A bad earlier poll is ignored; a bad last poll waits.
        let mut history = polls_between(0.0, 4.0);
        history[0].s.status = "yellow";
        assert_eq!(
            settle_decision(&history, 1_000_000, 0.0),
            Settle::Settled,
            "an earlier poll outside the one-poll window is ignored"
        );
        let mut bad_last = polls_between(0.0, 4.0);
        let n = bad_last.len();
        bad_last[n - 1].s.update_queue = 1;
        assert!(matches!(
            settle_decision(&bad_last, 1_000_000, 0.0),
            Settle::Waiting(_)
        ));
    }

    #[test]
    fn settle_window_is_twice_flush_or_30s() {
        // J2: max(30 s, 2 x flush_interval_sec)
        assert_eq!(settle_window_s(5), 30.0);
        assert_eq!(settle_window_s(20), 40.0);
        assert_eq!(settle_window_s(0), 30.0);
        assert_eq!(settle_window_s(15), 30.0);
    }

    #[test]
    fn tail_body_is_the_preregistered_patch_exactly() {
        // J3: the PATCH body must be exactly this document. TAIL-BODY-
        // ZERO (send 0, which *disables* indexing) fails here.
        assert_eq!(
            tail_indexing_body(),
            r#"{"optimizers_config":{"indexing_threshold":1}}"#
        );
    }

    #[test]
    fn index_state_roundtrips_through_json() {
        // J4: --state-out files are the equality evidence across
        // restarts, so to_value -> from_value must be lossless, and a
        // missing field must be a loud error, never a default.
        let st = IndexState::from_summary(settled_summary(), 30.0, 64.0, 33);
        let back = IndexState::from_value(&st.to_value()).expect("round trip");
        assert_eq!(back, st);
        assert_eq!(back.status, "green");
        assert_eq!(back.window_s, 30.0);
        assert_eq!(back.waited_s, 64.0);
        assert_eq!(back.polls, 33);
        let partial = json::parse(r#"{"status":"green","points":1000000}"#).expect("partial");
        assert!(IndexState::from_value(&partial).is_err());
        assert!(IndexState::from_value(&Value::Null).is_err());
        // the printed line carries the J2/J3 numbers
        let line = st.line();
        assert!(
            line.starts_with("index state: settled status=green points=1000000"),
            "{line}"
        );
        assert!(line.contains("segments=8 threshold_kb=1"), "{line}");
    }

    #[test]
    fn same_index_state_names_every_difference() {
        // J4: the equality seam compares points, indexed, segments and
        // indexing_threshold_kb — and names every differing field with
        // both values. STATE-EQ-NO-SEGMENTS (ignore segments) passes
        // the first case and fails here.
        let a = IndexState::from_summary(settled_summary(), 30.0, 64.0, 33);
        let mut b = IndexState::from_summary(settled_summary(), 0.0, 2.0, 1);
        b.segments = 9;
        let err = same_index_state(&a, &b).expect_err("segments differ");
        let text = format!("{err}");
        assert!(text.contains("segments"), "{text}");
        assert!(text.contains('8') && text.contains('9'), "{text}");
        b.segments = a.segments;
        b.indexed = 999_999;
        let err2 = same_index_state(&a, &b).expect_err("indexed differs");
        assert!(format!("{err2}").contains("indexed"), "{err2}");
        // wait facts are not index facts: different window/waited/polls
        // with the same index is Ok
        b.indexed = a.indexed;
        assert!(same_index_state(&a, &b).is_ok());
        // every differing field is named at once, not just the first
        let mut c = IndexState::from_summary(settled_summary(), 30.0, 64.0, 33);
        c.segments = 12;
        c.indexing_threshold_kb = 10_000;
        let both = format!("{}", same_index_state(&a, &c).expect_err("two differences"));
        assert!(
            both.contains("segments") && both.contains("indexing_threshold_kb"),
            "{both}"
        );
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
    /// This is the live index-state shape the diagnosis recorded. The
    /// optimizer_config values (indexing_threshold 10000,
    /// flush_interval_sec 5) are read from the committed
    /// `results/2026-10-31/vector-sift1m-qdrant.diagnosis-collection-info-chain10-era.txt`,
    /// not invented.
    const CHAIN10_ERA_COLLECTION_INFO: &str = r#"{
        "status":"grey","optimizer_status":"ok",
        "indexed_vectors_count":3861632,"points_count":1000000,
        "segments_count":92,
        "config":{"params":{"vectors":{"size":128,"distance":"Euclid"},
          "shard_number":1,"replication_factor":1},
          "hnsw_config":{"m":16,"ef_construct":200,"full_scan_threshold":10000},
          "optimizer_config":{"indexing_threshold":10000,
            "default_segment_number":0,"flush_interval_sec":5}},
        "payload_schema":{},"update_queue":{"length":0}}"#;

    #[test]
    fn collection_summary_reads_the_live_chain10_era_shape() {
        // Card B-0-F7 Work 2: the summary must read exactly what the
        // server said — the 92-segment, 3.86x-indexed, grey state that
        // answers the card's recall question is data, not narrative.
        // Card B-0-F7-F1 Work 1: optimizer_status and the optimizer
        // config are part of that data now (J2/J3 read them).
        let v = json::parse(CHAIN10_ERA_COLLECTION_INFO).expect("fixture json");
        let s = collection_summary(&v).expect("summary");
        assert_eq!(s.status, "grey");
        assert_eq!(s.points, 1_000_000);
        assert_eq!(s.indexed, 3_861_632);
        assert_eq!(s.segments, 92);
        assert_eq!(s.update_queue, 0);
        assert!(
            s.optimizer_ok,
            "the chain10-era dump reported optimizer_status \"ok\""
        );
        assert_eq!(s.indexing_threshold_kb, 10_000);
        assert_eq!(s.flush_interval_sec, 5);
        // a settled collection reads just as truthfully
        let settled = json::parse(
            r#"{"status":"green","optimizer_status":"ok",
                "indexed_vectors_count":985000,"points_count":1000000,
                "segments_count":2,"update_queue":{"length":0},
                "config":{"optimizer_config":{"indexing_threshold":10000,
                  "flush_interval_sec":5}}}"#,
        )
        .expect("settled json");
        let s2 = collection_summary(&settled).expect("summary");
        assert_eq!(s2.status, "green");
        assert_eq!(s2.segments, 2);
        assert!(s2.optimizer_ok);
        // missing fields are loud errors, never zero-filled guesses
        assert!(collection_summary(&json::parse("{}").expect("empty")).is_err());
        assert!(
            collection_summary(
                &json::parse(r#"{"status":"green","points_count":1}"#).expect("partial")
            )
            .is_err()
        );
        // a config without the new nested fields is a Missing error
        // too (card B-0-F7-F1 Work 1: never a default)
        let no_flush = json::parse(
            r#"{"status":"green","optimizer_status":"ok","points_count":1,
                "indexed_vectors_count":1,"segments_count":1,
                "update_queue":{"length":0},
                "config":{"optimizer_config":{"indexing_threshold":1}}}"#,
        )
        .expect("no flush_interval_sec");
        let err = collection_summary(&no_flush).expect_err("flush_interval_sec is mandatory");
        assert!(format!("{err}").contains("flush_interval_sec"), "{err}");
    }

    #[test]
    fn collection_summary_reads_an_optimizer_error_as_not_ok() {
        // Card B-0-F7-F1 Work 1: Qdrant reports optimizer_status as the
        // string "ok" or an error object; only the exact string counts
        // as ok, and the object is not a parse error — the settle keeps
        // waiting on it instead of crashing.
        let error_obj = json::parse(
            r#"{"status":"green","optimizer_status":{"error":"optimization failed"},
                "indexed_vectors_count":100,"points_count":100,
                "segments_count":1,"update_queue":{"length":0},
                "config":{"optimizer_config":{"indexing_threshold":1,
                  "flush_interval_sec":5}}}"#,
        )
        .expect("optimizer error json");
        let s = collection_summary(&error_obj).expect("summary");
        assert!(!s.optimizer_ok, "an error object is not \"ok\"");
        // a missing optimizer_status is a loud error, never a default
        let missing = json::parse(
            r#"{"status":"green","indexed_vectors_count":100,"points_count":100,
                "segments_count":1,"update_queue":{"length":0},
                "config":{"optimizer_config":{"indexing_threshold":1,
                  "flush_interval_sec":5}}}"#,
        )
        .expect("missing optimizer_status");
        assert!(collection_summary(&missing).is_err());
        // and a settle over an error-object collection waits on it
        let mut polls = polls_between(0.0, 30.0);
        for p in &mut polls {
            p.s.optimizer_ok = false;
        }
        assert!(matches!(
            settle_decision(&polls, 1_000_000, 30.0),
            Settle::Waiting(ref r) if r.contains("optimizer_status")
        ));
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
