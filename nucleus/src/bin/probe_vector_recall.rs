//! Vector recall-regression harness.
//!
//! The coherence oracle (probe_index_coherence) proves the vector index never
//! returns stale/duplicate/wrong rows, but it deliberately does NOT check
//! recall (how many of the true k nearest the index actually finds). An
//! incremental-maintenance refactor could silently degrade recall without
//! tripping any coherence check, so this harness closes that gap: it compares
//! the indexed KNN path against a brute-force top-k reference and asserts
//! average recall@k stays above a per-index-type floor — both on a freshly
//! built index AND after sustained churn (deletes + updates), which is where a
//! bad incremental posting update would show up.
//!
//! Section 2 (added 2026-08-25) is the CLUSTERED STABILITY gate at bench
//! scale, directly against `HnswIndex` — the shape BENCH_VS_QDRANT measured
//! (2026-08-20): first-perfect-recall `ef` ranged 96 → never → 192 → 96
//! across four runs of identical work, with one query returning none of its
//! true top-10 even at ef=256. Root causes fixed the same day: layer
//! assignment drew from a global RNG (per-boot graph lottery), and the
//! ef=1 greedy descent handed layer 0 a single entry point that could sit in
//! the wrong cluster entirely (measured 8.6 away from the query while every
//! true neighbour sat at ≤1.04). This section pins the fix: across several
//! corpus seeds, recall must reach 1.000 by a MODEST, STABLE ef and no query
//! may return zero of its true top-10 at any practical ef. The stability
//! across seeds is the deliverable — an engine whose recall depends on which
//! boot built the graph has no recall guarantee at all.
//!
//! `cargo run --release --features server --bin probe_vector_recall`
#![cfg(feature = "server")]
#![allow(clippy::unusual_byte_groupings)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use nucleus::bench_paired::{Rng as BRng, VectorDist, gen_query, gen_vectors};
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;
use nucleus::vector::{DistanceMetric, HnswConfig, HnswIndex, Vector, exact_search};

const DIM: usize = 16;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
    fn unit(&mut self) -> f32 {
        (self.below(20000) as f32) / 10000.0 - 1.0
    }
}

fn rand_vec(rng: &mut Rng) -> Vec<f32> {
    (0..DIM).map(|_| rng.unit()).collect()
}

fn vec_lit(v: &[f32]) -> String {
    let body: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
    format!("VECTOR('[{}]')", body.join(","))
}

fn l2_sq(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = (*x as f64) - (*y as f64);
            d * d
        })
        .sum()
}

fn exec(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

fn query_ids(ex: &Executor, sql: &str) -> Vec<i64> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => rows
                .iter()
                .filter_map(|row| match row.first() {
                    Some(Value::Int32(n)) => Some(*n as i64),
                    Some(Value::Int64(n)) => Some(*n),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// True top-k ids for `q` by brute force over the reference model.
fn brute_topk(model: &BTreeMap<i64, Vec<f32>>, q: &[f32], k: usize) -> Vec<i64> {
    let mut scored: Vec<(f64, i64)> = model.iter().map(|(id, v)| (l2_sq(q, v), *id)).collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

/// Average recall@k of the indexed path vs brute force over `queries` probes.
fn measure_recall(
    ex: &Executor,
    model: &BTreeMap<i64, Vec<f32>>,
    metric: &str,
    k: usize,
    queries: usize,
    rng: &mut Rng,
) -> f64 {
    if model.len() < k {
        return 1.0;
    }
    let mut sum = 0.0;
    for _ in 0..queries {
        let q = rand_vec(rng);
        let truth: std::collections::HashSet<i64> = brute_topk(model, &q, k).into_iter().collect();
        let sql = format!(
            "SELECT id FROM vr ORDER BY VECTOR_DISTANCE(v, {}, '{metric}') ASC LIMIT {k}",
            vec_lit(&q)
        );
        let got = query_ids(ex, &sql);
        let hit = got.iter().filter(|id| truth.contains(id)).count();
        sum += hit as f64 / k as f64;
    }
    sum / queries as f64
}

struct Case {
    kind: &'static str,
    metric: &'static str,
    fresh_floor: f64,
    churn_floor: f64,
}

fn run_case(case: &Case, n: usize, k: usize, queries: usize, seed: u64) -> Option<String> {
    let mut rng = Rng(seed | 1);
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    if !exec(&ex, "CREATE TABLE vr (id INT PRIMARY KEY, v VECTOR(16))") {
        return Some(format!("{}: CREATE TABLE failed", case.kind));
    }
    let mut model: BTreeMap<i64, Vec<f32>> = BTreeMap::new();
    let mut next_id = 1i64;
    for _ in 0..n {
        let id = next_id;
        next_id += 1;
        let v = rand_vec(&mut rng);
        if exec(
            &ex,
            &format!("INSERT INTO vr VALUES ({id}, {})", vec_lit(&v)),
        ) {
            model.insert(id, v);
        }
    }
    if !exec(
        &ex,
        &format!("CREATE INDEX vr_v ON vr USING {} (v)", case.kind),
    ) {
        return Some(format!("{}: CREATE INDEX failed", case.kind));
    }

    let fresh = measure_recall(&ex, &model, case.metric, k, queries, &mut rng);
    println!(
        "  {:>8} {:>6}  fresh recall@{k} = {:.3}  (floor {:.2})",
        case.kind, case.metric, fresh, case.fresh_floor
    );
    if fresh < case.fresh_floor {
        return Some(format!(
            "{} {}: fresh recall {:.3} below floor {:.2}",
            case.kind, case.metric, fresh, case.fresh_floor
        ));
    }

    // Churn: delete ~30% and update ~30% of the rows, then re-measure. This is
    // where an incremental-maintenance refactor would regress recall.
    let ids: Vec<i64> = model.keys().copied().collect();
    for (i, id) in ids.iter().enumerate() {
        if i % 10 < 3 {
            if exec(&ex, &format!("DELETE FROM vr WHERE id = {id}")) {
                model.remove(id);
            }
        } else if i % 10 < 6 {
            let v = rand_vec(&mut rng);
            if exec(
                &ex,
                &format!("UPDATE vr SET v = {} WHERE id = {id}", vec_lit(&v)),
            ) {
                model.insert(*id, v);
            }
        }
    }
    // Add fresh rows post-churn to exercise insert-after-delete maintenance.
    for _ in 0..(n / 4) {
        let id = next_id;
        next_id += 1;
        let v = rand_vec(&mut rng);
        if exec(
            &ex,
            &format!("INSERT INTO vr VALUES ({id}, {})", vec_lit(&v)),
        ) {
            model.insert(id, v);
        }
    }

    let churned = measure_recall(&ex, &model, case.metric, k, queries, &mut rng);
    println!(
        "  {:>8} {:>6}  churn recall@{k} = {:.3}  (floor {:.2})",
        case.kind, case.metric, churned, case.churn_floor
    );
    if churned < case.churn_floor {
        return Some(format!(
            "{} {}: post-churn recall {:.3} below floor {:.2}",
            case.kind, case.metric, churned, case.churn_floor
        ));
    }

    // Delete-heavy phase (HNSW only): sustained pure deletes accumulate
    // tombstones until the compaction heuristic (tombstones >= 64 && > live)
    // fires a rebuild — repeatedly, as the ratio keeps re-crossing. The mixed
    // churn above rarely reaches that ratio, so this is the only phase that
    // exercises compaction. Recall on the survivors must survive the rebuild: a
    // compaction that dropped or mis-mapped rows would sink it. (IvfFlat deletes
    // full-rebuild and never compact, so this phase is HNSW-specific.)
    if case.kind == "hnsw" {
        let live: Vec<i64> = model.keys().copied().collect();
        // Keep a small survivor set so tombstones dominate the live set.
        let survivors = (live.len() / 4).clamp(16, 64);
        for id in live.iter().take(live.len().saturating_sub(survivors)) {
            if exec(&ex, &format!("DELETE FROM vr WHERE id = {id}")) {
                model.remove(id);
            }
        }
        let compacted = measure_recall(&ex, &model, case.metric, k, queries, &mut rng);
        println!(
            "  {:>8} {:>6}  del-heavy recall@{k} = {:.3}  (floor {:.2})",
            case.kind, case.metric, compacted, case.churn_floor
        );
        if compacted < case.churn_floor {
            return Some(format!(
                "{} {}: post-compaction recall {:.3} below floor {:.2}",
                case.kind, case.metric, compacted, case.churn_floor
            ));
        }
    }
    None
}

/// Section 2: clustered recall stability at bench scale, direct `HnswIndex`
/// (the BENCH_VS_QDRANT shape: same generator, same m=16/ef_construction=200,
/// k=10, L2, one index swept over ef). For each corpus seed this builds ONE
/// index, sweeps, and reports (recall, zero-recall-query count) per ef plus
/// the first ef where recall hits 1.000.
///
/// Gates, chosen from the post-fix measured curves with headroom (post-fix
/// first-perfect ef was 32–48 across six seeds; pre-fix it was
/// 192 → never-256 → 192 → 48 with zero-recall queries persisting to
/// ef=128–256 in three of four runs):
///
/// - recall 1.000 must be reached by ef ≤ 96 on EVERY seed;
/// - zero_q must be 0 for every ef ≥ 32.
fn run_clustered_stability(
    n: usize,
    dim: usize,
    k: usize,
    queries: usize,
    seeds: &[u64],
    sweep: &[usize],
) -> Option<String> {
    const PERFECT_EF_CEILING: usize = 96;
    const ZERO_Q_MIN_EF: usize = 32;

    let mut failure = None;
    for &seed in seeds {
        let mut rng = BRng::new(seed);
        let corpus = gen_vectors(&mut rng, n, dim, VectorDist::Clustered);
        let qs: Vec<Vector> = (0..queries)
            .map(|_| gen_query(&mut rng, dim, VectorDist::Clustered, &corpus))
            .collect();

        let mut index = HnswIndex::new(HnswConfig {
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search: 50,
            metric: DistanceMetric::L2,
        });
        let t0 = Instant::now();
        for (id, v) in corpus.iter().enumerate() {
            index.insert(id as u64, v.clone());
        }
        let build_s = t0.elapsed().as_secs_f64();

        let reference: Vec<(u64, Vector)> = corpus
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, v)| (i as u64, v))
            .collect();
        // Brute-force truth, one query at a time, scored by ID overlap — the
        // corpus has no exact ties by construction (Gaussian jitter), so id
        // and distance scoring agree here.
        let truths: Vec<std::collections::HashSet<u64>> = qs
            .iter()
            .map(|q| {
                exact_search(&reference, q, k, DistanceMetric::L2)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect()
            })
            .collect();

        let mut first_perfect: Option<usize> = None;
        let mut latencies: Vec<f64> = Vec::new();
        println!("  seed {seed}: built {n} vectors in {build_s:.1}s");
        for &ef in sweep {
            let mut sum = 0.0f64;
            let mut zero_q = 0usize;
            for (q, truth) in qs.iter().zip(truths.iter()) {
                let t0 = Instant::now();
                let got = index.search_ef(q, k, ef);
                latencies.push(t0.elapsed().as_nanos() as f64 / 1000.0);
                let hits = got.iter().filter(|(id, _)| truth.contains(id)).count();
                if hits == 0 {
                    zero_q += 1;
                }
                sum += hits as f64 / k as f64;
            }
            let mean = sum / queries as f64;
            if first_perfect.is_none() && mean == 1.0 {
                first_perfect = Some(ef);
            }
            println!("  seed {seed}: ef={ef:>3} recall={mean:.3} zero_q={zero_q}");
            if ef >= ZERO_Q_MIN_EF && zero_q > 0 && failure.is_none() {
                failure = Some(format!(
                    "clustered stability seed {seed}: {zero_q} queries returned \
                     none of the true top-{k} at ef={ef} — a basin trap, not an \
                     operating point"
                ));
            }
        }
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p50 = latencies[latencies.len() / 2];
        match first_perfect {
            Some(ef) if ef <= PERFECT_EF_CEILING => println!(
                "  seed {seed}: first perfect recall at ef={ef} (ceiling \
                 {PERFECT_EF_CEILING}); ef=64 p50 {p50:.0}us"
            ),
            Some(ef) => {
                let msg = format!(
                    "clustered stability seed {seed}: recall 1.000 only at \
                     ef={ef}, above the stability ceiling {PERFECT_EF_CEILING}"
                );
                println!("  seed {seed}: FAIL {msg}");
                failure = failure.or(Some(msg));
            }
            None => {
                let msg = format!(
                    "clustered stability seed {seed}: recall never reached \
                     1.000 in the sweep"
                );
                println!("  seed {seed}: FAIL {msg}");
                failure = failure.or(Some(msg));
            }
        }
    }
    failure
}

fn main_impl() {
    std::panic::set_hook(Box::new(|_| {}));
    let args: Vec<String> = std::env::args().collect();
    let mut seed: u64 = 0x5EED_1234_ABCD;
    let mut n: usize = 600;
    let mut k: usize = 10;
    let mut queries: usize = 40;
    let mut direct_n: usize = 50_000;
    let mut direct_dim: usize = 128;
    let mut direct_seeds: Vec<u64> = vec![42, 7, 1234, 99999];
    let mut skip_direct = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(seed);
            }
            "--n" => {
                i += 1;
                n = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(n);
            }
            "--k" => {
                i += 1;
                k = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(k);
            }
            "--queries" => {
                i += 1;
                queries = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(queries);
            }
            "--direct-n" => {
                i += 1;
                direct_n = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(direct_n);
            }
            "--direct-dim" => {
                i += 1;
                direct_dim = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(direct_dim);
            }
            "--direct-seeds" => {
                i += 1;
                direct_seeds = args
                    .get(i)
                    .map(|s| {
                        s.split(',')
                            .filter_map(|p| p.trim().parse().ok())
                            .collect::<Vec<u64>>()
                    })
                    .filter(|v| !v.is_empty())
                    .unwrap_or(direct_seeds);
            }
            "--skip-direct" => skip_direct = true,
            _ => {}
        }
        i += 1;
    }

    // Floors sit well below observed values (hnsw fresh ~1.0 / churn ~0.72-0.76;
    // ivfflat fresh ~0.78-0.83 / churn ~0.68-0.78 across seeds) so the gate is
    // robust to seed variance while still catching a real regression — an
    // incremental-maintenance bug drops recall far more than this headroom, or
    // trips the coherence oracle outright.
    let cases = [
        Case {
            kind: "hnsw",
            metric: "l2",
            fresh_floor: 0.80,
            churn_floor: 0.55,
        },
        Case {
            kind: "hnsw",
            metric: "cosine",
            fresh_floor: 0.75,
            churn_floor: 0.55,
        },
        Case {
            kind: "ivfflat",
            metric: "l2",
            fresh_floor: 0.45,
            churn_floor: 0.25,
        },
    ];

    println!("probe_vector_recall: n={n} k={k} queries={queries} seed={seed:#x}");
    let mut failures = Vec::new();
    for (ci, case) in cases.iter().enumerate() {
        if let Some(f) = run_case(
            case,
            n,
            k,
            queries,
            seed.wrapping_add(ci as u64 * 0x9E3779B1),
        ) {
            failures.push(f);
        }
    }

    if !skip_direct {
        println!("\n── clustered stability (direct HnswIndex, bench shape) ──");
        if let Some(f) = run_clustered_stability(
            direct_n,
            direct_dim,
            10,
            200,
            &direct_seeds,
            &[10, 16, 24, 32, 48, 64, 96, 128, 192, 256],
        ) {
            failures.push(f);
        }
    }

    println!("\n════ SUMMARY ════");
    if failures.is_empty() {
        println!("All vector recall floors met (fresh + post-churn).");
    } else {
        for f in &failures {
            println!("FAIL: {f}");
        }
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
