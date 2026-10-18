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

/// Create the collection with the card's HNSW parameters.
pub fn qdrant_create(
    host: &str,
    port: u16,
    collection: &str,
    dim: usize,
    m: u64,
    ef_construct: u64,
) -> Result<()> {
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
    let resp = http::request(
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

/// Upload a batch of points with `wait=true`.
pub fn qdrant_upload(
    host: &str,
    port: u16,
    collection: &str,
    start_id: usize,
    vecs: &[Vec<f32>],
) -> Result<()> {
    let body = points_batch_json(start_id..start_id + vecs.len(), vecs)?;
    let resp = http::request(
        host,
        port,
        "POST",
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

/// Poll the collection until it is `green` with all points indexed.
pub fn qdrant_wait_green(
    host: &str,
    port: u16,
    collection: &str,
    expected_points: usize,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let resp = http::request(
            host,
            port,
            "GET",
            &format!("/collections/{collection}"),
            b"",
            Duration::from_secs(30),
        )?;
        if resp.status == 200 {
            let v = json::parse(&String::from_utf8_lossy(&resp.body))?;
            let status = v
                .get("result")
                .and_then(|r| r.get("status"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let points = v
                .get("result")
                .and_then(|r| r.get("points_count"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0) as usize;
            let indexed = v
                .get("result")
                .and_then(|r| r.get("indexed_vectors_count"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0) as usize;
            if status == "green" && points == expected_points && indexed == expected_points {
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
    Ok(v.get("result")
        .and_then(Value::as_f64)
        .ok_or_else(|| Error::Missing("count".into()))? as usize)
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
}
