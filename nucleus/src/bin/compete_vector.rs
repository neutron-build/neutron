//! Cross-engine vector benchmark — Nucleus HNSW vs pgvector vs Qdrant, apples
//! to apples.
//!
//! `bench_paired` deliberately measures Nucleus against an inline brute-force
//! reference and says so: its numbers are Nucleus-only and must not be
//! published as a cross-system win. This binary is the other half — the same
//! recall methodology run against a real competitor so the comparison is
//! publishable.
//!
//! What "apples to apples" means here, concretely:
//!
//! - **One corpus, one query set.** Both engines index bit-identical vectors
//!   from the same seeded generator and answer the same queries in the same
//!   order.
//! - **One ground truth.** Recall for both engines is measured against a single
//!   exact brute-force k-NN computed once, in this process. Neither engine
//!   grades its own homework.
//! - **Matched index parameters.** Same `m`, same `ef_construction`, same
//!   `ef_search`, same L2 metric. An HNSW comparison at mismatched `ef` is not a
//!   comparison, it is a choice of operating point.
//! - **One index per engine, swept.** `ef_search` is a query-time dial, so the
//!   sweep varies it against a FIXED graph in both engines. Rebuilding per
//!   point — which this harness used to do on both sides — measured a
//!   different randomized graph at each ef, so recall was not monotonic in ef
//!   and the curve meant nothing. `build_s` is therefore one measurement per
//!   engine, repeated on each row, and it is a property of `ef_construction`
//!   rather than of the point.
//! - **The transport difference is measured, not hidden.** Nucleus runs
//!   in-process; pgvector answers over a loopback socket, and Qdrant over
//!   loopback HTTP into a Linux VM when podman is the runtime. That gap is real
//!   and it favours Nucleus, so the harness measures each competitor's
//!   round-trip floor with a trivial request (`SELECT 1`, `GET /healthz`) and
//!   prints it on every run. Subtract it before claiming a latency win.
//! - **Recall is what the operating points are compared AT.** Every engine's
//!   `ef_search` sweep produces a (recall, latency) curve against the one shared
//!   ground truth. Read the curves at equal recall. Comparing the ef=64 row of
//!   one engine with the ef=64 row of another compares parameter values, not
//!   engines — an engine that returns worse answers faster is not faster.
//!
//! Usage:
//! ```text
//! cargo run --release --features bench-tools --bin compete_vector -- \
//!     --n 50000 --dim 128 --k 10 --queries 200 \
//!     --pg "host=localhost port=5432 dbname=nucleus_bench" \
//!     --qdrant http://127.0.0.1:56333
//! ```
//! Skip a competitor with `--skip-pg` / `--skip-qdrant`.
//!
//! Qdrant notes, because its defaults are not pgvector's:
//!
//! - It shards a collection into several segments, each with its own HNSW graph,
//!   and merges their answers. `--qdrant-segments` pins that count (default: 1,
//!   so the comparison is one graph against one graph); `0` leaves Qdrant's
//!   CPU-derived default.
//! - Indexing is a background optimizer, not a blocking `CREATE INDEX`. The
//!   harness uploads with `indexing_threshold = 0` (indexing off), then turns it
//!   on and polls until `status = green` and every vector is indexed. That
//!   elapsed time is `build_s`, and it excludes upload.
//! - `hnsw_config.full_scan_threshold` is set to Qdrant's minimum, 10 KB, so the
//!   graph is always preferred — the analogue of `SET enable_seqscan = off` on
//!   the pgvector arm. At its default (10 MB) a segment below that size answers
//!   EXACTLY, which reads as perfect recall at a latency that is not an HNSW
//!   latency at all.

use std::time::Instant;

use nucleus::bench_paired::{Rng, VectorDist, gen_query, gen_vectors};
use nucleus::vector::{DistanceMetric, HnswConfig, HnswIndex, Vector, exact_search};

struct Config {
    n: usize,
    dim: usize,
    k: usize,
    queries: usize,
    seed: u64,
    m: usize,
    ef_construction: usize,
    ef_search: Vec<usize>,
    dist: VectorDist,
    pg_conn: String,
    skip_pg: bool,
    qdrant_url: String,
    skip_qdrant: bool,
    /// Segments per Qdrant collection. `1` gives one HNSW graph, matching the
    /// other two arms; `0` leaves Qdrant's CPU-derived default.
    qdrant_segments: usize,
    /// Qdrant optimizer threads during the index build. `1` matches Nucleus's
    /// single-threaded build; `0` leaves Qdrant's default (parallel).
    qdrant_opt_threads: usize,
    /// How many times the whole query sweep is replayed over the SAME index.
    /// This laptop has measured 95.4% worst-case deviation on green runs, so a
    /// single pass of `queries` samples is not enough to separate an engine
    /// difference from scheduler noise. Recall is deterministic and does not
    /// change across repeats; only the latency sample count grows.
    repeats: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            n: 50_000,
            dim: 128,
            k: 10,
            queries: 200,
            seed: 42,
            m: 16,
            ef_construction: 200,
            ef_search: vec![40, 64, 128],
            dist: VectorDist::Clustered,
            pg_conn: "host=localhost port=5432 dbname=nucleus_bench".into(),
            skip_pg: false,
            qdrant_url: "http://127.0.0.1:56333".into(),
            skip_qdrant: false,
            qdrant_segments: 1,
            qdrant_opt_threads: 1,
            repeats: 3,
        }
    }
}

/// Peak resident set size of this process, in bytes.
///
/// Used as a build-memory probe: the corpus is allocated before the index is
/// built and nothing is freed in between, so the delta across the build is the
/// index's footprint. It is a PEAK, so it is a lower bound on nothing and an
/// upper bound on nothing — it is the high-water mark, and it is reported as
/// such rather than as "index size".
#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    // SAFETY: `getrusage` writes a plain POD struct through the pointer; the
    // zeroed value is a valid `rusage` and the call cannot fail for RUSAGE_SELF.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return 0;
        }
        // macOS reports ru_maxrss in bytes, Linux in kilobytes.
        if cfg!(target_os = "macos") {
            usage.ru_maxrss as u64
        } else {
            (usage.ru_maxrss as u64) * 1024
        }
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> u64 {
    0
}

/// One engine's result at one `ef_search` operating point.
struct Measurement {
    engine: &'static str,
    ef_search: usize,
    build_s: f64,
    avg_recall: f64,
    min_recall: f64,
    p50_us: f64,
    p95_us: f64,
    qps: f64,
    /// Queries for which the engine returned NONE of the true top-k. Reported
    /// separately from average recall because it is the number that matters for
    /// retrieval: a mean of 0.995 hides a query that got nothing back, and a
    /// user hitting that query sees an empty-feeling result, not a 0.5% error.
    zero_recall: usize,
}

fn percentile(sorted_us: &[f64], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[idx]
}

fn summarize(
    engine: &'static str,
    ef_search: usize,
    build_s: f64,
    recalls: &[f64],
    mut latencies_us: Vec<f64>,
) -> Measurement {
    let failed: Vec<usize> = recalls
        .iter()
        .enumerate()
        .filter(|(_, r)| **r <= 0.0)
        .map(|(i, _)| i)
        .collect();
    latencies_us.sort_by(|a, b| a.partial_cmp(b).expect("latencies are finite"));
    let n = recalls.len().max(1) as f64;
    let avg_recall = recalls.iter().sum::<f64>() / n;
    let min_recall = recalls.iter().cloned().fold(1.0f64, f64::min);
    let mean_us = latencies_us.iter().sum::<f64>() / latencies_us.len().max(1) as f64;
    Measurement {
        engine,
        ef_search,
        build_s,
        avg_recall,
        min_recall,
        p50_us: percentile(&latencies_us, 0.50),
        p95_us: percentile(&latencies_us, 0.95),
        qps: if mean_us > 0.0 {
            1_000_000.0 / mean_us
        } else {
            0.0
        },
        zero_recall: failed.len(),
    }
}

/// Squared L2 between a corpus vector and a query. Used for BOTH the ground
/// truth threshold and the scoring of every engine's answer, so no engine is
/// measured in a distance space another engine did not use.
fn l2_sq(a: &Vector, b: &Vector) -> f64 {
    a.data
        .iter()
        .zip(b.data.iter())
        .map(|(x, y)| {
            let d = (*x - *y) as f64;
            d * d
        })
        .sum()
}

/// Recall@k measured by DISTANCE, not by id.
///
/// Id-overlap recall is wrong across engines whenever the corpus contains ties
/// or near-duplicates — which clustered, embedding-like data always does. Two
/// engines can each return a perfectly correct top-k made of different but
/// equidistant points, and id-overlap scores that as a miss; the first run of
/// this harness reported pgvector min-recall 0.000 and recall FALLING as
/// ef_search rose, which is impossible for a real engine and was this artifact.
///
/// A returned point counts as a hit when it is at least as close as the k-th
/// true neighbour, within a relative epsilon for float noise. That is the
/// ann-benchmarks convention and it is tie-blind by construction.
fn recall_by_distance(
    got_ids: &[u64],
    kth_true_dist: f64,
    corpus: &[Vector],
    query: &Vector,
    k: usize,
) -> f64 {
    const EPS: f64 = 1e-6;
    let threshold = kth_true_dist * (1.0 + EPS) + EPS;
    let hits = got_ids
        .iter()
        .filter(|id| {
            corpus
                .get(**id as usize)
                .is_some_and(|v| l2_sq(v, query) <= threshold)
        })
        .count();
    hits as f64 / k.max(1) as f64
}

fn vector_literal(v: &Vector) -> String {
    let mut s = String::with_capacity(v.dim() * 8 + 2);
    s.push('[');
    for (i, x) in v.data.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&x.to_string());
    }
    s.push(']');
    s
}

fn bench_nucleus(
    cfg: &Config,
    corpus: &[Vector],
    queries: &[Vector],
    truth_kth: &[f64],
) -> (Vec<Measurement>, u64) {
    // ONE index, swept. It used to be rebuilt per operating point, which made
    // the curve uninterpretable: HNSW construction is randomized (layer
    // assignment), so each rebuild is a different graph and recall at ef=64
    // could beat ef=128 through graph luck alone. Recall must be monotonic in
    // ef for a FIXED index — that is what the dial means — and comparing points
    // measured on different graphs cannot show it. Building once also stops
    // reporting a build time per point for an index built once per
    // `ef_construction`, which is the parameter build cost actually depends on.
    let index_cfg = HnswConfig {
        m: cfg.m,
        m_max0: cfg.m * 2,
        ef_construction: cfg.ef_construction,
        // Not the sweep value: every query below passes its beam explicitly.
        ef_search: cfg.ef_search.first().copied().unwrap_or(64),
        metric: DistanceMetric::L2,
    };
    let mut index = HnswIndex::new(index_cfg);
    let rss_before = peak_rss_bytes();
    let t_build = Instant::now();
    for (id, v) in corpus.iter().enumerate() {
        index.insert(id as u64, v.clone());
    }
    let build_s = t_build.elapsed().as_secs_f64();
    // The graph stores its own copy of every vector, so this delta covers the
    // vectors AND the adjacency lists — the same thing Qdrant's segment holds.
    let rss_delta = peak_rss_bytes().saturating_sub(rss_before);

    let mut out = Vec::new();
    for &ef in &cfg.ef_search {
        let mut recalls = Vec::with_capacity(queries.len());
        let mut latencies = Vec::with_capacity(queries.len() * cfg.repeats);
        for (pass, (qi, q)) in (0..cfg.repeats)
            .flat_map(|pass| std::iter::repeat(pass).zip(queries.iter().enumerate()))
        {
            let t0 = Instant::now();
            // `search_ef`, not `search`: `search` raises the beam to
            // `max(ef_search, min(n/2048, 512))`, so at 50k rows the reported
            // ef and the ef actually used would differ. A sweep whose x-axis is
            // not the value under test is not a sweep.
            let got = index.search_ef(q, cfg.k, ef);
            latencies.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            let ids: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
            let r = recall_by_distance(&ids, truth_kth[qi], corpus, q, cfg.k);
            // Recall is deterministic over a fixed graph, so it is recorded once
            // and the later passes only add latency samples.
            if pass == 0 {
                recalls.push(r);
            }
        }
        out.push(summarize("nucleus", ef, build_s, &recalls, latencies));
    }
    (out, rss_delta)
}

#[cfg(feature = "bench-tools")]
async fn bench_pgvector(
    cfg: &Config,
    corpus: &[Vector],
    queries: &[Vector],
    truth_kth: &[f64],
) -> Result<(Vec<Measurement>, f64), Box<dyn std::error::Error>> {
    use tokio_postgres::NoTls;

    let (client, connection) = tokio_postgres::connect(&cfg.pg_conn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("pg connection error: {e}");
        }
    });

    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS vector")
        .await?;
    let version: String = client
        .query_one(
            "SELECT extversion FROM pg_extension WHERE extname = 'vector'",
            &[],
        )
        .await?
        .get(0);
    println!("pgvector version: {version}");

    // The loopback round-trip floor. Printed so a latency comparison against an
    // in-process engine can be read honestly instead of crediting Nucleus with
    // the absence of a socket.
    let mut rtt = Vec::with_capacity(200);
    for _ in 0..200 {
        let t0 = Instant::now();
        client.query_one("SELECT 1", &[]).await?;
        rtt.push(t0.elapsed().as_nanos() as f64 / 1000.0);
    }
    rtt.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let rtt_p50 = percentile(&rtt, 0.50);

    client
        .batch_execute("DROP TABLE IF EXISTS bench_vectors")
        .await?;
    client
        .batch_execute(&format!(
            "CREATE TABLE bench_vectors (id BIGINT PRIMARY KEY, v vector({}))",
            cfg.dim
        ))
        .await?;

    // Multi-row INSERTs, with each row's id equal to its corpus index so an id
    // returned by either engine means the same vector. Load time is not a
    // reported metric (it is dominated by client-side literal formatting);
    // only index build time is.
    const CHUNK: usize = 500;
    let mut loaded = 0usize;
    while loaded < corpus.len() {
        let end = (loaded + CHUNK).min(corpus.len());
        let mut sql = String::from("INSERT INTO bench_vectors (id, v) VALUES ");
        for (offset, v) in corpus[loaded..end].iter().enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({}, '{}')", loaded + offset, vector_literal(v)));
        }
        client.batch_execute(&sql).await?;
        loaded = end;
    }

    // One index for the whole sweep, matching the Nucleus arm: `hnsw.ef_search`
    // is a runtime setting, so rebuilding per point measured a different
    // randomized graph at each ef and cost three index builds to learn nothing
    // extra.
    client
        .batch_execute("DROP INDEX IF EXISTS bench_vectors_hnsw")
        .await?;
    let t_build = Instant::now();
    client
        .batch_execute(&format!(
            "CREATE INDEX bench_vectors_hnsw ON bench_vectors \
             USING hnsw (v vector_l2_ops) WITH (m = {}, ef_construction = {})",
            cfg.m, cfg.ef_construction
        ))
        .await?;
    let build_s = t_build.elapsed().as_secs_f64();

    let mut out = Vec::new();
    for &ef in &cfg.ef_search {
        client
            .batch_execute(&format!("SET hnsw.ef_search = {ef}"))
            .await?;
        // Force the index path so a sequential scan cannot masquerade as
        // perfect recall at low latency.
        client.batch_execute("SET enable_seqscan = off").await?;

        // The parameter is bound as TEXT and cast server-side. Writing
        // `$1::vector` instead makes tokio-postgres infer the parameter's type
        // as `vector`, which it has no serializer for.
        let stmt = client
            .prepare(
                "SELECT id FROM bench_vectors \
                 ORDER BY v <-> CAST($1::text AS vector) LIMIT $2",
            )
            .await?;

        let mut recalls = Vec::with_capacity(queries.len());
        let mut latencies = Vec::with_capacity(queries.len() * cfg.repeats);
        for (pass, (qi, q)) in (0..cfg.repeats)
            .flat_map(|pass| std::iter::repeat(pass).zip(queries.iter().enumerate()))
        {
            let literal = vector_literal(q);
            let limit = cfg.k as i64;
            let t0 = Instant::now();
            let rows = client.query(&stmt, &[&literal, &limit]).await?;
            latencies.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            let ids: Vec<u64> = rows.iter().map(|r| r.get::<_, i64>(0) as u64).collect();
            let r = recall_by_distance(&ids, truth_kth[qi], corpus, q, cfg.k);
            if pass == 0 {
                recalls.push(r);
            }
        }
        out.push(summarize("pgvector", ef, build_s, &recalls, latencies));
    }

    Ok((out, rtt_p50))
}

/// Everything the Qdrant arm reports beyond the per-ef rows.
#[cfg(feature = "bench-tools")]
struct QdrantRun {
    rows: Vec<Measurement>,
    version: String,
    /// p50 of a trivial `GET /healthz`, the loopback-HTTP floor under every
    /// query latency in `rows`.
    rtt_p50_us: f64,
    /// Segments the collection actually ended up with, read back rather than
    /// assumed — the requested count is a hint, not a guarantee.
    segments: usize,
    indexed_vectors: u64,
    upload_s: f64,
    /// `(ef_search, p50, p95)` of Qdrant's OWN reported handling time, in
    /// microseconds. Self-reported, so it is evidence about the transport, not
    /// a substitute for the measured client-side latency.
    server_us_by_ef: Vec<(usize, f64, f64)>,
}

/// Nucleus HNSW vs Qdrant over its REST API.
///
/// Qdrant is a purpose-built vector database and this arm is written so that it
/// can win cleanly: same corpus, same queries, same `m`/`ef_construct`, the same
/// `ef_search` sweep against ONE index, and recall scored by the same external
/// brute-force truth used for every other engine.
#[cfg(feature = "bench-tools")]
async fn bench_qdrant(
    cfg: &Config,
    corpus: &[Vector],
    queries: &[Vector],
    truth_kth: &[f64],
) -> Result<QdrantRun, Box<dyn std::error::Error>> {
    use serde_json::{Value, json};

    let base = cfg.qdrant_url.trim_end_matches('/').to_string();
    let coll = format!("{base}/collections/bench_vectors");
    let http = reqwest::Client::builder()
        // Qdrant's index build on a large corpus can outlast a default timeout,
        // and a timeout mid-build would be reported as a Qdrant failure rather
        // than as a harness limit.
        .timeout(std::time::Duration::from_secs(900))
        .build()?;

    /// A non-2xx from Qdrant must abort, never be timed as a success. That is
    /// the exact defect that discredited the numbers under `docs/benchmarks/`.
    async fn ok_json(
        resp: reqwest::Response,
        what: &str,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(format!("qdrant {what} failed: HTTP {status}: {body}").into());
        }
        Ok(serde_json::from_str(&body).unwrap_or(Value::Null))
    }

    let version = ok_json(http.get(&base).send().await?, "GET /")
        .await?
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    // Loopback floor. Measured against `/healthz`, which touches no collection,
    // so it is transport and HTTP framing only. Under podman on macOS this also
    // includes the hop into the Linux VM, which is exactly why it is printed.
    let mut rtt = Vec::with_capacity(200);
    for _ in 0..200 {
        let t0 = Instant::now();
        let r = http.get(format!("{base}/healthz")).send().await?;
        if !r.status().is_success() {
            return Err(format!("qdrant healthz: HTTP {}", r.status()).into());
        }
        let _ = r.bytes().await?;
        rtt.push(t0.elapsed().as_nanos() as f64 / 1000.0);
    }
    rtt.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let rtt_p50_us = percentile(&rtt, 0.50);

    // Recreate from scratch: a leftover collection would be measured with the
    // previous run's parameters and nothing in the output would say so.
    let _ = http.delete(&coll).send().await?;

    let mut optimizers = json!({
        // 0 disables HNSW construction. Points land unindexed, so upload cost
        // and build cost are two measurements instead of one blended number.
        "indexing_threshold": 0,
    });
    if cfg.qdrant_segments > 0 {
        optimizers["default_segment_number"] = json!(cfg.qdrant_segments);
    }
    if cfg.qdrant_opt_threads > 0 {
        optimizers["max_optimization_threads"] = json!(cfg.qdrant_opt_threads);
    }
    let create = json!({
        "vectors": { "size": cfg.dim, "distance": "Euclid", "on_disk": false },
        "hnsw_config": {
            "m": cfg.m,
            "ef_construct": cfg.ef_construction,
            // 10 KB is Qdrant's minimum accepted value (it rejects smaller with
            // HTTP 422). Always prefer the graph: the default, 10 MB, answers
            // any smaller segment exactly, which would score perfect recall at
            // a latency that is not an HNSW latency.
            "full_scan_threshold": 10,
            "on_disk": false,
        },
        "optimizers_config": optimizers,
        "shard_number": 1,
        "replication_factor": 1,
    });
    ok_json(
        http.put(&coll).json(&create).send().await?,
        "create collection",
    )
    .await?;

    // Upload unindexed. `wait=true` so the harness never races the write path.
    const CHUNK: usize = 500;
    let t_upload = Instant::now();
    let mut loaded = 0usize;
    while loaded < corpus.len() {
        let end = (loaded + CHUNK).min(corpus.len());
        let points: Vec<Value> = corpus[loaded..end]
            .iter()
            .enumerate()
            .map(|(offset, v)| json!({ "id": loaded + offset, "vector": v.data }))
            .collect();
        ok_json(
            http.put(format!("{coll}/points?wait=true"))
                .json(&json!({ "points": points }))
                .send()
                .await?,
            "upsert points",
        )
        .await?;
        loaded = end;
    }
    let upload_s = t_upload.elapsed().as_secs_f64();

    // Turn indexing on and time it to completion. `status = green` alone is not
    // enough — a collection with zero indexed vectors is also green — so the
    // loop waits on `indexed_vectors_count` reaching the corpus size.
    let t_build = Instant::now();
    ok_json(
        http.patch(&coll)
            .json(&json!({ "optimizers_config": { "indexing_threshold": 1 } }))
            .send()
            .await?,
        "enable indexing",
    )
    .await?;
    let (build_s, indexed_vectors, segments) = loop {
        let info = ok_json(http.get(&coll).send().await?, "collection info").await?;
        let result = info.get("result").cloned().unwrap_or(Value::Null);
        let status = result.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let indexed = result
            .get("indexed_vectors_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let segments = result
            .get("segments_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        if status == "red" {
            return Err(format!("qdrant collection went red during build: {result}").into());
        }
        if status == "green" && indexed >= corpus.len() as u64 {
            break (t_build.elapsed().as_secs_f64(), indexed, segments);
        }
        if t_build.elapsed().as_secs_f64() > 900.0 {
            return Err(format!(
                "qdrant index build did not finish in 900s (status={status}, indexed={indexed})"
            )
            .into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };

    // One index, swept — `hnsw_ef` is a per-request parameter in Qdrant, so no
    // rebuild is needed and none is done. Queries are sequential and
    // single-threaded, matching the other two arms.
    let mut rows = Vec::new();
    let mut server_us_by_ef = Vec::new();
    for &ef in &cfg.ef_search {
        let mut recalls = Vec::with_capacity(queries.len());
        let mut latencies = Vec::with_capacity(queries.len() * cfg.repeats);
        // Qdrant reports its own server-side handling time on every response.
        // It is self-reported and therefore NOT the headline number, but it is
        // the only way to see past a loopback-into-a-VM transport that is
        // hundreds of microseconds on its own.
        let mut server_us: Vec<f64> = Vec::with_capacity(queries.len() * cfg.repeats);
        for (pass, (qi, q)) in (0..cfg.repeats)
            .flat_map(|pass| std::iter::repeat(pass).zip(queries.iter().enumerate()))
        {
            let body = json!({
                "vector": q.data,
                "limit": cfg.k,
                // `exact: false` is the default, stated so a future Qdrant
                // default flip cannot silently turn this into a brute-force arm
                // scoring perfect recall.
                "params": { "hnsw_ef": ef, "exact": false },
                "with_payload": false,
                "with_vector": false,
            });
            let t0 = Instant::now();
            let resp = http
                .post(format!("{coll}/points/search"))
                .json(&body)
                .send()
                .await?;
            let status = resp.status();
            let text = resp.text().await?;
            latencies.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            if !status.is_success() {
                return Err(format!("qdrant search failed: HTTP {status}: {text}").into());
            }
            let parsed: Value = serde_json::from_str(&text)?;
            let hits = parsed
                .get("result")
                .and_then(|v| v.as_array())
                .ok_or_else(|| format!("qdrant search returned no result array: {text}"))?;
            if hits.len() != cfg.k {
                return Err(format!(
                    "qdrant returned {} hits, expected k={} — an under-full answer \
                     would be scored as low recall at low latency, which is not a \
                     measurement",
                    hits.len(),
                    cfg.k
                )
                .into());
            }
            let ids: Vec<u64> = hits
                .iter()
                .filter_map(|h| h.get("id").and_then(|v| v.as_u64()))
                .collect();
            if let Some(t) = parsed.get("time").and_then(|v| v.as_f64()) {
                server_us.push(t * 1_000_000.0);
            }
            let r = recall_by_distance(&ids, truth_kth[qi], corpus, q, cfg.k);
            if pass == 0 {
                recalls.push(r);
            }
        }
        server_us.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        server_us_by_ef.push((
            ef,
            percentile(&server_us, 0.50),
            percentile(&server_us, 0.95),
        ));
        rows.push(summarize("qdrant", ef, build_s, &recalls, latencies));
    }

    Ok(QdrantRun {
        rows,
        version,
        rtt_p50_us,
        segments,
        indexed_vectors,
        upload_s,
        server_us_by_ef,
    })
}

fn parse_args() -> Config {
    let mut cfg = Config::default();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--n" => cfg.n = next(&mut i).parse().unwrap_or(cfg.n),
            "--dim" => cfg.dim = next(&mut i).parse().unwrap_or(cfg.dim),
            "--k" => cfg.k = next(&mut i).parse().unwrap_or(cfg.k),
            "--queries" => cfg.queries = next(&mut i).parse().unwrap_or(cfg.queries),
            "--seed" => cfg.seed = next(&mut i).parse().unwrap_or(cfg.seed),
            "--m" => cfg.m = next(&mut i).parse().unwrap_or(cfg.m),
            "--ef-construction" => {
                cfg.ef_construction = next(&mut i).parse().unwrap_or(cfg.ef_construction)
            }
            "--ef-search" => {
                cfg.ef_search = next(&mut i)
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect()
            }
            "--dist" => {
                cfg.dist = match next(&mut i).as_str() {
                    "uniform" => VectorDist::Uniform,
                    _ => VectorDist::Clustered,
                }
            }
            "--pg" => cfg.pg_conn = next(&mut i),
            "--skip-pg" => cfg.skip_pg = true,
            "--qdrant" => cfg.qdrant_url = next(&mut i),
            "--skip-qdrant" => cfg.skip_qdrant = true,
            "--qdrant-segments" => {
                cfg.qdrant_segments = next(&mut i).parse().unwrap_or(cfg.qdrant_segments)
            }
            "--qdrant-opt-threads" => {
                cfg.qdrant_opt_threads = next(&mut i).parse().unwrap_or(cfg.qdrant_opt_threads)
            }
            "--repeats" => cfg.repeats = next(&mut i).parse().unwrap_or(cfg.repeats).max(1),
            _ => {}
        }
        i += 1;
    }
    cfg
}

fn main() {
    let cfg = parse_args();
    println!(
        "corpus n={} dim={} dist={:?} k={} queries={} repeats={} seed={}",
        cfg.n, cfg.dim, cfg.dist, cfg.k, cfg.queries, cfg.repeats, cfg.seed
    );
    println!(
        "matched index params: m={} ef_construction={} ef_search={:?} metric=L2",
        cfg.m, cfg.ef_construction, cfg.ef_search
    );

    // One corpus, one query set, one ground truth — shared by every engine.
    let mut rng = Rng::new(cfg.seed);
    let corpus = gen_vectors(&mut rng, cfg.n, cfg.dim, cfg.dist);
    let queries: Vec<Vector> = (0..cfg.queries)
        .map(|_| gen_query(&mut rng, cfg.dim, cfg.dist, &corpus))
        .collect();

    println!("computing exact ground truth (brute force) ...");
    let reference: Vec<(u64, Vector)> = corpus
        .iter()
        .enumerate()
        .map(|(id, v)| (id as u64, v.clone()))
        .collect();
    let t_truth = Instant::now();
    // The k-th true neighbour's distance is the recall threshold. It is
    // recomputed here with the same `l2_sq` used to score every engine's
    // answer, so the threshold and the scores cannot drift apart through a
    // difference in distance convention (squared vs not).
    let truth_kth: Vec<f64> = queries
        .iter()
        .map(|q| {
            exact_search(&reference, q, cfg.k, DistanceMetric::L2)
                .into_iter()
                .map(|(id, _)| l2_sq(&corpus[id as usize], q))
                .fold(0.0f64, f64::max)
        })
        .collect();
    println!(
        "ground truth for {} queries in {:.1}s",
        cfg.queries,
        t_truth.elapsed().as_secs_f64()
    );

    let (mut results, nucleus_rss) = bench_nucleus(&cfg, &corpus, &queries, &truth_kth);
    let mut notes: Vec<String> = vec![format!(
        "nucleus build peak-RSS delta = {:.1} MB ({:.0} bytes/vector over {} vectors, \
         graph + its own copy of each vector)",
        nucleus_rss as f64 / (1024.0 * 1024.0),
        nucleus_rss as f64 / cfg.n.max(1) as f64,
        cfg.n
    )];
    if cfg.skip_pg {
        notes.push("pgvector skipped".into());
    }
    if cfg.skip_qdrant {
        notes.push("qdrant skipped".into());
    }

    // One runtime for every networked arm. Errors are recorded and printed, not
    // swallowed: a competitor that failed must never look like a competitor that
    // lost.
    #[cfg(feature = "bench-tools")]
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    #[cfg(feature = "bench-tools")]
    if !cfg.skip_pg {
        match runtime.block_on(bench_pgvector(&cfg, &corpus, &queries, &truth_kth)) {
            Ok((mut pg, rtt_p50)) => {
                results.append(&mut pg);
                notes.push(format!("pgvector loopback SELECT 1 p50 = {rtt_p50:.0} us"));
            }
            Err(e) => {
                notes.push(format!("pgvector FAILED: {e}"));
            }
        }
    }

    #[cfg(feature = "bench-tools")]
    if !cfg.skip_qdrant {
        match runtime.block_on(bench_qdrant(&cfg, &corpus, &queries, &truth_kth)) {
            Ok(mut q) => {
                notes.push(format!(
                    "qdrant {} at {} — segments={} indexed_vectors={} upload={:.1}s \
                     (upload is NOT in build_s); loopback GET /healthz p50 = {:.0} us",
                    q.version,
                    cfg.qdrant_url,
                    q.segments,
                    q.indexed_vectors,
                    q.upload_s,
                    q.rtt_p50_us
                ));
                notes.push(format!(
                    "qdrant requested segments={} opt_threads={} repeats={} (0 = qdrant default)",
                    cfg.qdrant_segments, cfg.qdrant_opt_threads, cfg.repeats
                ));
                for (ef, p50, p95) in &q.server_us_by_ef {
                    notes.push(format!(
                        "qdrant SELF-REPORTED handling time at ef={ef}: p50 {p50:.0} us, \
                         p95 {p95:.0} us (excludes transport; not the headline number)"
                    ));
                }
                results.append(&mut q.rows);
            }
            Err(e) => {
                notes.push(format!("qdrant FAILED: {e}"));
            }
        }
    }

    println!();
    println!(
        "{:<10} {:>10} {:>10} {:>9} {:>9} {:>8} {:>10} {:>10} {:>10}",
        "engine", "ef_search", "build_s", "recall", "min_rec", "zero_q", "p50_us", "p95_us", "qps"
    );
    // `build_s` repeats down each engine's rows on purpose: one index is built
    // per engine and swept, so it is the same measurement, not three.
    for r in &results {
        println!(
            "{:<10} {:>10} {:>10.1} {:>9.3} {:>9.3} {:>8} {:>10.0} {:>10.0} {:>10.0}",
            r.engine,
            r.ef_search,
            r.build_s,
            r.avg_recall,
            r.min_recall,
            r.zero_recall,
            r.p50_us,
            r.p95_us,
            r.qps
        );
    }
    println!();
    for n in &notes {
        println!("{n}");
    }
    println!(
        "NOTE: Nucleus runs in-process; pgvector answers over a loopback socket and \
         Qdrant over loopback HTTP. Subtract each round-trip floor above before \
         reading a latency gap as an engine difference. Queries are sequential and \
         single-threaded. COMPARE AT EQUAL RECALL, not at equal ef_search."
    );
}
