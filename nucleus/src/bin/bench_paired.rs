//! Correctness-paired benchmark harness for the vector (HNSW), full-text, and
//! graph models. Every latency/throughput line prints a correctness number
//! measured on the SAME data in the SAME run — recall@k for vector, exact
//! result-set equality for FTS, exact BFS/Dijkstra agreement for graph.
//!
//! No external database (pgvector / Neo4j / Tantivy) is used or assumed: the
//! reference is an inline brute-force / textbook implementation. Numbers are
//! therefore Nucleus-only and must NOT be published as a cross-system win.
//!
//! `cargo run --release --bin bench_paired`
//!
//! The correctness gates also run as `#[test]`s under `cargo test --lib`
//! (see `nucleus::bench_paired` tests); this bin exists to surface the raw
//! latency/throughput/recall numbers a human wants to eyeball.

use nucleus::bench_paired::{
    VectorDist, bench_fts, bench_graph, bench_vector_dist, bench_vector_scale_sweep,
};

fn main() {
    // One-shot scale mode: `bench_paired scale [n] [dim] [k] [queries]` —
    // builds one clustered HNSW at the given size (default 1M x 64d), asserts
    // the recall floor holds there, prints build time + search latency. This
    // is the Phase-2 "vector at scale" gate; the default matrix below stays
    // the fast everyday run.
    let args: Vec<String> = std::env::args().collect();
    // `bench_paired sweep [n] [dim] [k] [queries]` — one clustered build,
    // recall/min-recall across an ef sweep (scale-dip diagnosis).
    if args.get(1).map(String::as_str) == Some("sweep") {
        let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
        let dim: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
        let k: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(10);
        let queries: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(50);
        println!("VECTOR SCALE SWEEP — clustered, n={n} dim={dim} k={k} queries={queries}");
        bench_vector_scale_sweep(n, dim, k, queries, 0xB0BA_CAFE);
        return;
    }

    if args.get(1).map(String::as_str) == Some("scale") {
        let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
        let dim: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
        let k: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(10);
        let queries: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(50);
        let seed = 0xB0BA_CAFEu64;
        println!("VECTOR SCALE — clustered, n={n} dim={dim} k={k} queries={queries}");
        let t0 = std::time::Instant::now();
        let r = bench_vector_dist(n, dim, k, queries, seed, VectorDist::Clustered);
        println!(
            "  total {:.1}s  hnsw_us {:.1}  brute_us {:.1}  qps {:.0}  recall {:.3}  min_rec {:.3}",
            t0.elapsed().as_secs_f64(),
            r.hnsw_avg_us,
            r.brute_avg_us,
            r.qps,
            r.avg_recall,
            r.min_recall
        );
        // Same floors as the lib-test gate: clustered data must stay near-exact.
        if r.avg_recall < 0.98 || r.min_recall < 0.80 {
            println!("SCALE RECALL FLOOR FAILED (avg>=0.98, min>=0.80 required)");
            std::process::exit(1);
        }
        println!("scale recall floor holds ✅");
        return;
    }

    let seed: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xB0BA_CAFE);

    println!("bench_paired — Nucleus-only (no external reference DB in this environment)\n");

    // ---- Vector (HNSW) ----
    // Two distributions, because they answer different questions:
    //   uniform   = ANN worst case (distance concentration) → pessimistic floor
    //   clustered = embedding-like structure → representative real-world recall
    println!("VECTOR (HNSW KNN) — latency paired with recall@k vs exact brute force");
    println!(
        "  {:>9} {:>6} {:>4} {:>3} {:>7} {:>10} {:>11} {:>10} {:>9} {:>9}",
        "dist", "n", "dim", "k", "queries", "hnsw_us", "brute_us", "qps", "recall", "min_rec"
    );
    for &(dist, label) in &[
        (VectorDist::Uniform, "uniform"),
        (VectorDist::Clustered, "clustered"),
    ] {
        for &(n, dim, k) in &[
            (1_000usize, 64usize, 10usize),
            (5_000, 64, 10),
            (5_000, 128, 10),
            (10_000, 128, 20),
        ] {
            let r = bench_vector_dist(n, dim, k, 100, seed, dist);
            println!(
                "  {:>9} {:>6} {:>4} {:>3} {:>7} {:>10.1} {:>11.1} {:>10.0} {:>9.3} {:>9.3}",
                label,
                r.n, r.dim, r.k, r.queries, r.hnsw_avg_us, r.brute_avg_us, r.qps, r.avg_recall, r.min_recall
            );
        }
    }

    // ---- FTS ----
    println!("\nFTS (inverted index) — throughput paired with exact result-set equality");
    println!(
        "  {:>7} {:>7} {:>10} {:>10} {:>9} {:>10}",
        "docs", "queries", "query_us", "qps", "avg_hits", "set_exact"
    );
    for &docs in &[1_000usize, 5_000, 20_000] {
        let r = bench_fts(docs, 200, seed);
        println!(
            "  {:>7} {:>7} {:>10.2} {:>10.0} {:>9.1} {:>10}",
            r.docs,
            r.queries,
            r.avg_query_us,
            r.qps,
            r.avg_hits,
            if r.all_sets_exact { "YES" } else { "NO!" }
        );
        if !r.all_sets_exact {
            eprintln!("  !! {} / {} queries diverged from reference", r.mismatches, r.queries);
        }
    }

    // ---- Graph ----
    println!("\nGRAPH — traversal throughput paired with exact BFS/Dijkstra agreement");
    println!(
        "  {:>6} {:>7} {:>7} {:>9} {:>9} {:>9} {:>16}",
        "nodes", "edges", "queries", "sp_us", "dij_us", "bfs_us", "correct(sp/dij/bfs)"
    );
    for &(nodes, epn) in &[(1_000usize, 4usize), (5_000, 6), (20_000, 8)] {
        let r = bench_graph(nodes, epn, 100, seed);
        println!(
            "  {:>6} {:>7} {:>7} {:>9.2} {:>9.2} {:>9.2}   {:>4}/{:<4}{:>4}/{:<4}{:>4}/{:<4}",
            r.nodes,
            r.edges,
            r.queries,
            r.sp_avg_us,
            r.dij_avg_us,
            r.bfs_avg_us,
            r.sp_correct,
            r.sp_total,
            r.dij_correct,
            r.dij_total,
            r.bfs_correct,
            r.bfs_total,
        );
        let all_ok = r.sp_correct == r.sp_total
            && r.dij_correct == r.dij_total
            && r.bfs_correct == r.bfs_total;
        if !all_ok {
            eprintln!("  !! graph correctness divergence at nodes={nodes}");
        }
    }

    println!("\nAll correctness checks are computed inline against brute-force / textbook");
    println!("references. Numbers are Nucleus-only; do not publish as a cross-system result.");
}
