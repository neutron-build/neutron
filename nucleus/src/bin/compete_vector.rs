//! Cross-engine vector benchmark — Nucleus HNSW vs pgvector, apples to apples.
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
//!   in-process; pgvector answers over a loopback socket. That gap is real and
//!   it favours Nucleus, so the harness measures the round-trip floor with a
//!   trivial `SELECT 1` and prints it on every run. Subtract it before claiming
//!   a latency win.
//!
//! Usage:
//! ```text
//! cargo run --release --features bench-tools --bin compete_vector -- \
//!     --n 50000 --dim 128 --k 10 --queries 200 --pg "host=localhost port=5432 dbname=nucleus_bench"
//! ```
//! Skip pgvector with `--skip-pg` to get the Nucleus column alone.

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
        }
    }
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
) -> Vec<Measurement> {
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
    let t_build = Instant::now();
    for (id, v) in corpus.iter().enumerate() {
        index.insert(id as u64, v.clone());
    }
    let build_s = t_build.elapsed().as_secs_f64();

    let mut out = Vec::new();
    for &ef in &cfg.ef_search {
        let mut recalls = Vec::with_capacity(queries.len());
        let mut latencies = Vec::with_capacity(queries.len());
        for (qi, q) in queries.iter().enumerate() {
            let t0 = Instant::now();
            // `search_ef`, not `search`: `search` raises the beam to
            // `max(ef_search, min(n/2048, 512))`, so at 50k rows the reported
            // ef and the ef actually used would differ. A sweep whose x-axis is
            // not the value under test is not a sweep.
            let got = index.search_ef(q, cfg.k, ef);
            latencies.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            let ids: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
            recalls.push(recall_by_distance(&ids, truth_kth[qi], corpus, q, cfg.k));
        }
        out.push(summarize("nucleus", ef, build_s, &recalls, latencies));
    }
    out
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
        let mut latencies = Vec::with_capacity(queries.len());
        for (qi, q) in queries.iter().enumerate() {
            let literal = vector_literal(q);
            let limit = cfg.k as i64;
            let t0 = Instant::now();
            let rows = client.query(&stmt, &[&literal, &limit]).await?;
            latencies.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            let ids: Vec<u64> = rows.iter().map(|r| r.get::<_, i64>(0) as u64).collect();
            recalls.push(recall_by_distance(&ids, truth_kth[qi], corpus, q, cfg.k));
        }
        out.push(summarize("pgvector", ef, build_s, &recalls, latencies));
    }

    Ok((out, rtt_p50))
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
            _ => {}
        }
        i += 1;
    }
    cfg
}

fn main() {
    let cfg = parse_args();
    println!(
        "corpus n={} dim={} dist={:?} k={} queries={} seed={}",
        cfg.n, cfg.dim, cfg.dist, cfg.k, cfg.queries, cfg.seed
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

    let mut results = bench_nucleus(&cfg, &corpus, &queries, &truth_kth);
    let mut rtt_note = String::from("pgvector skipped");

    #[cfg(feature = "bench-tools")]
    if !cfg.skip_pg {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        match runtime.block_on(bench_pgvector(&cfg, &corpus, &queries, &truth_kth)) {
            Ok((mut pg, rtt_p50)) => {
                results.append(&mut pg);
                rtt_note = format!("pgvector loopback SELECT 1 p50 = {rtt_p50:.0} us");
            }
            Err(e) => {
                rtt_note = format!("pgvector FAILED: {e}");
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
    println!("{rtt_note}");
    println!(
        "NOTE: Nucleus runs in-process; pgvector answers over a loopback socket. \
         Subtract the round-trip floor above before reading any latency gap as an \
         engine difference. Queries are sequential and single-threaded."
    );
}
