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
//!   the QPS numbers never include background indexing.

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

/// Search one vector; returns ids in rank order.
pub fn qdrant_search(
    host: &str,
    port: u16,
    collection: &str,
    vec: &[f32],
    limit: usize,
    ef: u64,
) -> Result<Vec<i64>> {
    let body = json::to_string(&Value::obj(vec![
        (
            "vector",
            Value::Arr(vec.iter().map(|x| Value::n(*x as f64)).collect()),
        ),
        ("limit", Value::n(limit as f64)),
        ("with_payload", Value::Bool(false)),
        ("params", Value::obj(vec![("hnsw_ef", Value::n(ef as f64))])),
    ]))?;
    let resp = http::request(
        host,
        port,
        "POST",
        &format!("/collections/{collection}/points/search"),
        body.as_bytes(),
        Duration::from_secs(120),
    )?;
    if resp.status != 200 {
        return Err(Error::Command {
            name: format!("qdrant search (HTTP {})", resp.status),
            status: resp.status as i32,
            stderr_tail: String::from_utf8_lossy(&resp.body).to_string(),
        });
    }
    let text = String::from_utf8_lossy(&resp.body).to_string();
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
            if status == "green" && points == expected_points {
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
             (SELECT id FROM items ORDER BY v <=> '[{}]'::vector LIMIT {k}) s;",
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
        assert!(sql_text.contains("ORDER BY v <=> '[1,2.5]'::vector LIMIT 10"));

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
}
