//! Correctness-paired micro-benchmarks for the three specialty index models
//! that had a benchmark OR a correctness oracle, but never both wired together
//! in a single run: vector (HNSW) KNN, full-text search, and graph traversal.
//!
//! Project rule (learned the hard way — an unverified marketing number had to
//! be walked back once): never report a latency/throughput number without a
//! correctness number measured on the *same* data in the *same* run. A fast
//! index that returns wrong rows is a regression, not a speedup.
//!
//! Every routine here returns a result struct carrying BOTH the timing and a
//! correctness metric computed against an inline reference implementation:
//!   - vector: recall@k vs an exact brute-force linear scan
//!   - FTS:    exact result-set equality vs an independent term-set matcher
//!   - graph:  exact distance/cost equality vs textbook BFS / Dijkstra
//!
//! No external database is required — the reference is always computed here.
//!
//! The `bench_*` functions are pure (no printing); the `bench_paired` bin
//! prints them for humans, and the `#[test]`s below assert the correctness
//! metric stays exact / above floor so a silent regression fails `cargo test`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::fts::{InvertedIndex, tokenize};
use crate::graph::{Direction, GraphStore, PropValue, props};
use crate::vector::{DistanceMetric, HnswConfig, HnswIndex, Vector, exact_search};

// ============================================================================
// Deterministic RNG (xorshift64) — reproducible across runs, no rand dep.
// ============================================================================

/// Small reproducible PRNG so every benchmark/oracle run over a given seed is
/// bit-for-bit identical (a correctness gate that flakes is worthless).
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 { 0 } else { (self.next_u64() % n as u64) as usize }
    }
    /// Uniform f32 in [-1.0, 1.0).
    fn unit_f32(&mut self) -> f32 {
        (self.below(20_000) as f32) / 10_000.0 - 1.0
    }
    /// Approximately-Gaussian f32 (mean 0), scaled by `sd`. Sum of 4 uniforms
    /// (central-limit) so cluster noise looks like real embedding jitter rather
    /// than a uniform box.
    fn gauss_f32(&mut self, sd: f32) -> f32 {
        let s: f32 = (0..4).map(|_| self.unit_f32()).sum();
        s * 0.5 * sd
    }
    /// Uniform f64 in [1.0, 10.0] — positive edge weights for Dijkstra.
    fn weight(&mut self) -> f64 {
        1.0 + (self.below(9_000) as f64) / 1_000.0
    }
}

// ============================================================================
// 1. Vector (HNSW) KNN — latency paired with recall@k vs brute force
// ============================================================================

#[derive(Debug, Clone)]
pub struct VectorBenchResult {
    pub n: usize,
    pub dim: usize,
    pub k: usize,
    pub queries: usize,
    /// Mean recall@k of the HNSW path vs the exact top-k (1.0 == perfect).
    pub avg_recall: f64,
    /// Worst single-query recall@k observed (the number a floor must clear).
    pub min_recall: f64,
    /// Mean HNSW search latency, microseconds.
    pub hnsw_avg_us: f64,
    /// Mean brute-force (reference) latency, microseconds — the thing HNSW beats.
    pub brute_avg_us: f64,
    /// HNSW queries per second (derived from `hnsw_avg_us`).
    pub qps: f64,
}

fn rand_vector(rng: &mut Rng, dim: usize) -> Vector {
    Vector::new((0..dim).map(|_| rng.unit_f32()).collect())
}

/// Data distribution for the vector benchmark.
///
/// `Uniform` (i.i.d. uniform coords) is the ANN-recall *worst case*: in high
/// dimension all pairwise distances concentrate, so the true top-k is barely
/// nearer than random and any graph-navigation slip drops a true neighbour.
/// `Clustered` mimics real embeddings — points sit near a handful of centres
/// with low-intrinsic-dimension structure, which is the regime HNSW targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorDist {
    Uniform,
    Clustered,
}

/// Generate `n` vectors under `dist`. For `Clustered`, points are drawn around
/// `n/50`-ish centres with Gaussian jitter (sd 0.10) so within-cluster nearest
/// neighbours are unambiguous — the structure real embeddings have.
fn gen_vectors(rng: &mut Rng, n: usize, dim: usize, dist: VectorDist) -> Vec<Vector> {
    match dist {
        VectorDist::Uniform => (0..n).map(|_| rand_vector(rng, dim)).collect(),
        VectorDist::Clustered => {
            let n_clusters = (n / 50).clamp(4, 256);
            let centers: Vec<Vec<f32>> = (0..n_clusters)
                .map(|_| (0..dim).map(|_| rng.unit_f32()).collect())
                .collect();
            (0..n)
                .map(|_| {
                    let c = &centers[rng.below(n_clusters)];
                    Vector::new((0..dim).map(|d| c[d] + rng.gauss_f32(0.10)).collect())
                })
                .collect()
        }
    }
}

/// Draw a query from the same distribution as the corpus, so recall is measured
/// in-distribution (an out-of-distribution query is a different, easier check).
fn gen_query(rng: &mut Rng, dim: usize, dist: VectorDist, corpus: &[Vector]) -> Vector {
    match dist {
        VectorDist::Uniform => rand_vector(rng, dim),
        VectorDist::Clustered => {
            // Perturb a random existing point → lands inside a real cluster.
            let base = &corpus[rng.below(corpus.len().max(1))];
            Vector::new(base.data.iter().map(|x| x + rng.gauss_f32(0.10)).collect())
        }
    }
}

/// Build an HNSW index of `n` `dim`-vectors under the default (uniform)
/// distribution, then run `queries` KNN probes timing the index path and, on
/// the SAME query, computing recall@k against an exact brute-force scan.
pub fn bench_vector(n: usize, dim: usize, k: usize, queries: usize, seed: u64) -> VectorBenchResult {
    bench_vector_dist(n, dim, k, queries, seed, VectorDist::Uniform)
}

/// Same as [`bench_vector`] but choosing the corpus/query distribution — used to
/// separate the data artifact (uniform worst case) from real engine recall
/// (clustered, embedding-like).
pub fn bench_vector_dist(
    n: usize,
    dim: usize,
    k: usize,
    queries: usize,
    seed: u64,
    dist: VectorDist,
) -> VectorBenchResult {
    let mut rng = Rng::new(seed);
    let config = HnswConfig {
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search: 64.max(k * 4),
        metric: DistanceMetric::L2,
    };
    let mut index = HnswIndex::new(config);
    let corpus = gen_vectors(&mut rng, n, dim, dist);
    let mut reference: Vec<(u64, Vector)> = Vec::with_capacity(n);
    for (id, v) in corpus.iter().enumerate() {
        index.insert(id as u64, v.clone());
        reference.push((id as u64, v.clone()));
    }

    let mut recall_sum = 0.0f64;
    let mut recall_min = 1.0f64;
    let mut hnsw_nanos = 0u128;
    let mut brute_nanos = 0u128;

    for _ in 0..queries {
        let q = gen_query(&mut rng, dim, dist, &corpus);

        let t0 = Instant::now();
        let got = index.search(&q, k);
        hnsw_nanos += t0.elapsed().as_nanos();

        let t1 = Instant::now();
        let truth = exact_search(&reference, &q, k, DistanceMetric::L2);
        brute_nanos += t1.elapsed().as_nanos();

        let truth_ids: HashSet<u64> = truth.iter().map(|(id, _)| *id).collect();
        let hits = got.iter().filter(|(id, _)| truth_ids.contains(id)).count();
        let recall = hits as f64 / k.max(1) as f64;
        recall_sum += recall;
        recall_min = recall_min.min(recall);
    }

    let q = queries.max(1) as f64;
    let hnsw_avg_us = (hnsw_nanos as f64 / q) / 1000.0;
    VectorBenchResult {
        n,
        dim,
        k,
        queries,
        avg_recall: recall_sum / q,
        min_recall: recall_min,
        hnsw_avg_us,
        brute_avg_us: (brute_nanos as f64 / q) / 1000.0,
        qps: if hnsw_avg_us > 0.0 { 1_000_000.0 / hnsw_avg_us } else { f64::INFINITY },
    }
}

// ============================================================================
// 2. FTS (inverted index) — throughput paired with exact result-set equality
// ============================================================================

#[derive(Debug, Clone)]
pub struct FtsBenchResult {
    pub docs: usize,
    pub queries: usize,
    pub avg_query_us: f64,
    pub qps: f64,
    /// True iff the engine's returned doc-set equalled the reference OR-match
    /// set on EVERY query (the correctness gate).
    pub all_sets_exact: bool,
    /// Number of queries whose engine set diverged from the reference set.
    pub mismatches: usize,
    /// Mean reference match-set size (so throughput isn't measured on empties).
    pub avg_hits: f64,
}

/// Non-stopword, single-token vocabulary. Both the engine and the reference
/// tokenize identically, so the check exercises the engine's posting-list
/// union/dedup logic against the set-theoretic definition of an OR match.
const VOCAB: &[&str] = &[
    "quantum", "neural", "database", "vector", "kernel", "rust", "index",
    "query", "storage", "graph", "tensor", "cluster", "protocol", "cipher",
    "matrix", "photon", "lattice", "entropy", "gradient", "spectral",
    "columnar", "raft", "consensus", "replica", "shard", "posting", "recall",
    "traversal", "dijkstra", "inverted", "corpus", "throughput", "latency",
    "engine", "planner", "executor", "wal", "checkpoint", "durable", "oracle",
];

fn random_doc(rng: &mut Rng, words: usize) -> String {
    (0..words)
        .map(|_| VOCAB[rng.below(VOCAB.len())])
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build a synthetic corpus, then time `queries` searches while checking each
/// returned doc-set against an independent term-set reference match.
pub fn bench_fts(docs: usize, queries: usize, seed: u64) -> FtsBenchResult {
    let mut rng = Rng::new(seed);
    let mut index = InvertedIndex::new();
    // Independent reference: per-doc set of tokens, derived from the raw text
    // via the same tokenizer but NOT from the engine's internal postings.
    let mut token_sets: Vec<HashSet<String>> = Vec::with_capacity(docs);

    for id in 0..docs as u64 {
        let text = random_doc(&mut rng, 12);
        index.add_document(id, &text);
        token_sets.push(tokenize(&text).into_iter().map(|t| t.term).collect());
    }

    let mut nanos = 0u128;
    let mut mismatches = 0usize;
    let mut hits_sum = 0usize;

    for _ in 0..queries {
        // 1-3 query terms.
        let nterms = 1 + rng.below(3);
        let query_words: Vec<&str> = (0..nterms).map(|_| VOCAB[rng.below(VOCAB.len())]).collect();
        let query = query_words.join(" ");
        let query_terms: HashSet<String> = tokenize(&query).into_iter().map(|t| t.term).collect();

        // Reference OR-match: any doc sharing >=1 token with the query.
        let reference: HashSet<u64> = token_sets
            .iter()
            .enumerate()
            .filter(|(_, set)| !set.is_disjoint(&query_terms))
            .map(|(id, _)| id as u64)
            .collect();
        hits_sum += reference.len();

        // Engine: limit == docs so BM25 truncation never trims the set.
        let t0 = Instant::now();
        let engine_hits = index.search(&query, docs);
        nanos += t0.elapsed().as_nanos();
        let engine: HashSet<u64> = engine_hits.into_iter().map(|(id, _)| id).collect();

        if engine != reference {
            mismatches += 1;
        }
    }

    let q = queries.max(1) as f64;
    let avg_query_us = (nanos as f64 / q) / 1000.0;
    FtsBenchResult {
        docs,
        queries,
        avg_query_us,
        qps: if avg_query_us > 0.0 { 1_000_000.0 / avg_query_us } else { f64::INFINITY },
        all_sets_exact: mismatches == 0,
        mismatches,
        avg_hits: hits_sum as f64 / q,
    }
}

// ============================================================================
// 3. Graph traversal — throughput paired with exact BFS / Dijkstra equality
// ============================================================================

#[derive(Debug, Clone)]
pub struct GraphBenchResult {
    pub nodes: usize,
    pub edges: usize,
    pub queries: usize,
    // --- unweighted shortest path (BFS hops) ---
    pub sp_avg_us: f64,
    pub sp_qps: f64,
    pub sp_correct: usize,
    pub sp_total: usize,
    // --- weighted shortest path (Dijkstra cost) ---
    pub dij_avg_us: f64,
    pub dij_qps: f64,
    pub dij_correct: usize,
    pub dij_total: usize,
    // --- BFS reachable-set ---
    pub bfs_avg_us: f64,
    pub bfs_correct: usize,
    pub bfs_total: usize,
}

/// Reference unweighted single-source distances (hops) via textbook BFS.
fn ref_bfs_dist(adj: &[Vec<(usize, f64)>], start: usize) -> HashMap<usize, usize> {
    let mut dist = HashMap::new();
    let mut queue = std::collections::VecDeque::new();
    dist.insert(start, 0usize);
    queue.push_back(start);
    while let Some(u) = queue.pop_front() {
        let d = dist[&u];
        for &(v, _) in &adj[u] {
            if let std::collections::hash_map::Entry::Vacant(slot) = dist.entry(v) {
                slot.insert(d + 1);
                queue.push_back(v);
            }
        }
    }
    dist
}

/// Reference non-negative single-source shortest costs via textbook Dijkstra.
fn ref_dijkstra(adj: &[Vec<(usize, f64)>], start: usize) -> HashMap<usize, f64> {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    #[derive(PartialEq)]
    struct St(f64, usize);
    impl Eq for St {}
    impl PartialOrd for St {
        fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for St {
        fn cmp(&self, o: &Self) -> Ordering {
            // Min-heap: reverse the cost ordering.
            o.0.partial_cmp(&self.0).unwrap_or(Ordering::Equal)
        }
    }

    let mut dist: HashMap<usize, f64> = HashMap::new();
    let mut heap = BinaryHeap::new();
    dist.insert(start, 0.0);
    heap.push(St(0.0, start));
    while let Some(St(d, u)) = heap.pop() {
        if d > *dist.get(&u).unwrap_or(&f64::INFINITY) {
            continue;
        }
        for &(v, w) in &adj[u] {
            let nd = d + w;
            if nd < *dist.get(&v).unwrap_or(&f64::INFINITY) {
                dist.insert(v, nd);
                heap.push(St(nd, v));
            }
        }
    }
    dist
}

/// Build a random directed weighted graph, then time shortest-path / Dijkstra /
/// BFS queries while verifying each answer against the inline reference above.
pub fn bench_graph(
    nodes: usize,
    edges_per_node: usize,
    queries: usize,
    seed: u64,
) -> GraphBenchResult {
    let mut rng = Rng::new(seed);
    let mut store = GraphStore::new();

    // 0-based index -> engine NodeId, plus a mirror adjacency list for the
    // reference implementations.
    let mut node_ids: Vec<u64> = Vec::with_capacity(nodes);
    for i in 0..nodes {
        let id = store.create_node(
            vec!["N".to_string()],
            props(vec![("i", PropValue::Int(i as i64))]),
        );
        node_ids.push(id);
    }
    let mut adj: Vec<Vec<(usize, f64)>> = vec![Vec::new(); nodes];
    for from in 0..nodes {
        for _ in 0..edges_per_node {
            let to = rng.below(nodes);
            if to == from {
                continue;
            }
            let w = rng.weight();
            store.create_edge(
                node_ids[from],
                node_ids[to],
                "E".to_string(),
                props(vec![("w", PropValue::Float(w))]),
            );
            adj[from].push((to, w));
        }
    }
    let idx_of: HashMap<u64, usize> =
        node_ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();

    let mut sp_nanos = 0u128;
    let mut sp_correct = 0usize;
    let mut dij_nanos = 0u128;
    let mut dij_correct = 0usize;
    let mut bfs_nanos = 0u128;
    let mut bfs_correct = 0usize;

    for _ in 0..queries {
        let a = rng.below(nodes);
        let b = rng.below(nodes);
        let (from_id, to_id) = (node_ids[a], node_ids[b]);

        // --- unweighted shortest path: verify hop-count == reference BFS ---
        let ref_dist = ref_bfs_dist(&adj, a);
        let t0 = Instant::now();
        let sp = store.shortest_path(from_id, to_id, Direction::Outgoing, None);
        sp_nanos += t0.elapsed().as_nanos();
        let sp_ok = match (sp, ref_dist.get(&b)) {
            (Some(path), Some(&d)) => {
                // valid endpoints, correct hop count, and every step is a real edge
                path.first() == Some(&from_id)
                    && path.last() == Some(&to_id)
                    && path.len().saturating_sub(1) == d
                    && path.windows(2).all(|w| {
                        let (u, v) = (idx_of[&w[0]], idx_of[&w[1]]);
                        adj[u].iter().any(|&(t, _)| t == v)
                    })
            }
            (None, None) => true, // both agree: unreachable
            _ => false,
        };
        if sp_ok {
            sp_correct += 1;
        }

        // --- weighted shortest path: verify cost == reference Dijkstra ---
        let ref_cost = ref_dijkstra(&adj, a);
        let t1 = Instant::now();
        let dij = store.dijkstra(from_id, to_id, Direction::Outgoing, "w");
        dij_nanos += t1.elapsed().as_nanos();
        let dij_ok = match (dij, ref_cost.get(&b)) {
            (Some((cost, _)), Some(&rc)) => (cost - rc).abs() < 1e-6,
            (None, None) => true,
            (_, Some(&rc)) if a == b => rc == 0.0, // self: cost 0
            _ => false,
        };
        if dij_ok {
            dij_correct += 1;
        }

        // --- BFS reachable set: verify set equality vs reference ---
        let t2 = Instant::now();
        let visited = store.bfs(from_id, Direction::Outgoing, None);
        bfs_nanos += t2.elapsed().as_nanos();
        let engine_set: HashSet<usize> = visited.iter().map(|id| idx_of[id]).collect();
        let ref_set: HashSet<usize> = ref_dist.keys().copied().collect();
        if engine_set == ref_set {
            bfs_correct += 1;
        }
    }

    let q = queries.max(1) as f64;
    let sp_avg_us = (sp_nanos as f64 / q) / 1000.0;
    let dij_avg_us = (dij_nanos as f64 / q) / 1000.0;
    let total_edges: usize = adj.iter().map(|a| a.len()).sum();
    GraphBenchResult {
        nodes,
        edges: total_edges,
        queries,
        sp_avg_us,
        sp_qps: if sp_avg_us > 0.0 { 1_000_000.0 / sp_avg_us } else { f64::INFINITY },
        sp_correct,
        sp_total: queries,
        dij_avg_us,
        dij_qps: if dij_avg_us > 0.0 { 1_000_000.0 / dij_avg_us } else { f64::INFINITY },
        dij_correct,
        dij_total: queries,
        bfs_avg_us: (bfs_nanos as f64 / q) / 1000.0,
        bfs_correct,
        bfs_total: queries,
    }
}

// ============================================================================
// Correctness gates — real `cargo test --lib` tests, not ad-hoc scripts.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::{DistanceMetric, HnswConfig, HnswIndex, exact_search};

    /// Diagnostic (ignored — run on demand, prints, asserts nothing costly):
    /// `cargo test --lib -- --ignored --nocapture ef_search_sweep`
    ///
    /// Answers "does the UNIFORM path have real recall headroom, or is it just
    /// the search-effort dial set low against genuinely hard (distance-
    /// concentrated) data?" by sweeping the layer-0 beam width `ef` over ONE
    /// built graph and reporting two numbers per ef:
    ///
    /// - `recall@k`: ID-set overlap with the exact top-k (what we floor on).
    /// - `dist_ratio`: mean(returned_i_dist / true_i_dist) over the k ranks,
    ///   `>= 1.0`, where 1.0 == returned neighbours are exactly as close as the
    ///   true ones. On concentrated data recall under-counts quality: an
    ///   equidistant tie-swap reads as a "miss" (recall < 1) while dist_ratio
    ///   stays ~1.000.
    ///
    /// Reading: if recall climbs toward 1.0 as ef grows -> graph is navigable,
    /// the "headroom" is the recall/latency knob (raise ef or the default).
    /// If recall plateaus well below 1.0 AND dist_ratio stays > ~1.0 -> a
    /// build-side connectivity deficiency worth chasing. If recall plateaus but
    /// dist_ratio ~1.000 -> the misses are tie-swaps, i.e. no real headroom.
    #[test]
    #[ignore = "diagnostic; prints an ef sweep, run with --ignored --nocapture"]
    fn ef_search_sweep_uniform() {
        let seed = 42u64;
        for &(n, dim, k) in &[(5_000usize, 128usize, 10usize), (10_000, 128, 20)] {
            let mut rng = Rng::new(seed);
            let config = HnswConfig {
                m: 16,
                m_max0: 32,
                ef_construction: 200,
                ef_search: 64, // overridden per-query below
                metric: DistanceMetric::L2,
            };
            let mut index = HnswIndex::new(config);
            let corpus = gen_vectors(&mut rng, n, dim, VectorDist::Uniform);
            let reference: Vec<(u64, Vector)> =
                corpus.iter().enumerate().map(|(i, v)| (i as u64, v.clone())).collect();
            for (id, v) in corpus.iter().enumerate() {
                index.insert(id as u64, v.clone());
            }

            let queries: Vec<Vector> =
                (0..100).map(|_| gen_query(&mut rng, dim, VectorDist::Uniform, &corpus)).collect();

            println!("\n== uniform n={n} dim={dim} k={k} (build ef_c=200) ==");
            println!("   {:>5} {:>8} {:>10} {:>10}", "ef", "recall", "dist_ratio", "avg_us");
            for &ef in &[16usize, 32, 64, 128, 256, 512, 1024] {
                let mut recall_sum = 0.0f64;
                let mut ratio_sum = 0.0f64;
                let mut nanos = 0u128;
                for q in &queries {
                    let t0 = Instant::now();
                    let got = index.search_ef(q, k, ef);
                    nanos += t0.elapsed().as_nanos();
                    let truth = exact_search(&reference, q, k, DistanceMetric::L2);
                    let truth_ids: HashSet<u64> = truth.iter().map(|(id, _)| *id).collect();
                    let hits = got.iter().filter(|(id, _)| truth_ids.contains(id)).count();
                    recall_sum += hits as f64 / k.max(1) as f64;
                    // Rank-aligned distance ratio: returned i-th vs true i-th.
                    let mut per_q = 0.0f64;
                    let mut counted = 0usize;
                    for i in 0..k.min(got.len()).min(truth.len()) {
                        let td = truth[i].1 as f64;
                        if td > 0.0 {
                            per_q += got[i].1 as f64 / td;
                            counted += 1;
                        }
                    }
                    if counted > 0 {
                        ratio_sum += per_q / counted as f64;
                    }
                }
                let qn = queries.len() as f64;
                println!(
                    "   {:>5} {:>8.3} {:>10.4} {:>10.1}",
                    ef,
                    recall_sum / qn,
                    ratio_sum / qn,
                    (nanos as f64 / qn) / 1000.0
                );
            }
        }
    }

    /// HNSW must recover the true nearest neighbors: mean recall@k stays high
    /// and no single query collapses. If an index refactor returned wrong rows
    /// fast, this fails — which is the entire point of pairing the two numbers.
    #[test]
    fn vector_recall_floor_paired_with_latency() {
        for seed in [1u64, 7, 42, 12345] {
            let r = bench_vector(1000, 32, 10, 50, seed);
            assert!(
                r.avg_recall >= 0.85,
                "seed {seed}: avg recall@{} = {:.3} below floor 0.85 (hnsw {:.1}us vs brute {:.1}us)",
                r.k, r.avg_recall, r.hnsw_avg_us, r.brute_avg_us
            );
            assert!(
                r.min_recall >= 0.50,
                "seed {seed}: min single-query recall {:.3} collapsed below 0.50",
                r.min_recall
            );
            assert!(r.hnsw_avg_us > 0.0 && r.qps > 0.0);
        }
    }

    /// Recall floor on CLUSTERED (embedding-like) data. This is the regression
    /// guard for the neighbour-selection bug: with simple take-M-closest
    /// selection the HNSW graph had no inter-cluster bridge edges, so greedy
    /// search dead-ended in the wrong cluster and returned as few as ZERO of the
    /// true top-k (avg recall 0.37-0.77, min 0.0) — while uniform data, which the
    /// old test used, looked "only" mediocre and hid it. With the diversifying
    /// heuristic (Alg. 4) clustered recall is essentially perfect, so the floor
    /// is deliberately high: a regression here means bridges are being starved
    /// again. Structured queries are the real-world case, so this must hold.
    #[test]
    fn vector_recall_floor_clustered_high_bar() {
        for seed in [1u64, 7, 42, 12345] {
            for &(n, dim, k) in &[(1000usize, 64usize, 10usize), (5000, 128, 10)] {
                let r = bench_vector_dist(n, dim, k, 50, seed, VectorDist::Clustered);
                assert!(
                    r.avg_recall >= 0.98,
                    "seed {seed} n{n} d{dim}: clustered avg recall@{} = {:.3} below 0.98 — \
                     inter-cluster bridge edges are being starved (neighbour-selection regression)",
                    r.k, r.avg_recall
                );
                assert!(
                    r.min_recall >= 0.80,
                    "seed {seed} n{n} d{dim}: clustered min single-query recall {:.3} below 0.80 — \
                     some query is dead-ending in the wrong cluster",
                    r.min_recall
                );
            }
        }
    }

    /// The FTS engine's returned doc-set must exactly equal the independent
    /// term-set OR-match reference on every query — no fast-but-wrong results.
    #[test]
    fn fts_result_set_exact_paired_with_throughput() {
        for seed in [2u64, 9, 100, 55555] {
            let r = bench_fts(800, 60, seed);
            assert!(
                r.all_sets_exact,
                "seed {seed}: {} / {} queries diverged from reference match set",
                r.mismatches, r.queries
            );
            // Guard against measuring throughput on empty result sets.
            assert!(r.avg_hits > 1.0, "seed {seed}: corpus produced near-empty matches");
            assert!(r.avg_query_us > 0.0 && r.qps > 0.0);
        }
    }

    /// Graph shortest-path (hops), Dijkstra (cost) and BFS (reachable set) must
    /// each match the textbook reference on every query.
    #[test]
    fn graph_paths_exact_paired_with_throughput() {
        for seed in [3u64, 11, 777, 24680] {
            let r = bench_graph(400, 4, 60, seed);
            assert_eq!(
                r.sp_correct, r.sp_total,
                "seed {seed}: {}/{} shortest-path (hops) queries wrong",
                r.sp_total - r.sp_correct, r.sp_total
            );
            assert_eq!(
                r.dij_correct, r.dij_total,
                "seed {seed}: {}/{} Dijkstra (cost) queries wrong",
                r.dij_total - r.dij_correct, r.dij_total
            );
            assert_eq!(
                r.bfs_correct, r.bfs_total,
                "seed {seed}: {}/{} BFS reachable-set queries wrong",
                r.bfs_total - r.bfs_correct, r.bfs_total
            );
            assert!(r.sp_avg_us > 0.0 && r.dij_avg_us > 0.0 && r.bfs_avg_us > 0.0);
        }
    }
}
