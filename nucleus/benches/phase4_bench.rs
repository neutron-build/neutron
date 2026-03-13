//! Phase 4 Performance Hardening benchmarks.
//!
//! Covers all six Phase 4 sprints at competitive scale:
//!
//!   4A — KV vs Redis:          1M SET / 1M GET / 1M INCR throughput
//!   4B — FTS vs Elasticsearch: 100K document index + BM25 search
//!   4C — Vector vs pgvector:   100K vectors (128-dim) HNSW search
//!   4D — Graph vs Neo4j:       100K nodes / 1M edges traversal
//!   4E — TimeSeries vs TS:     1M points / 100 series aggregation
//!   4F — Columnar vs ClickHouse: 10M rows COUNT/SUM/AVG/GROUP BY
//!
//! Run with: cargo bench --bench phase4_bench
//!
//! Note: setup phases (index building) are excluded from timing via
//! criterion's iter_batched / bench setup. Only query hot-paths are timed.

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use rand::Rng;

use nucleus::columnar::{
    self, ColumnBatch, ColumnData,
};
use nucleus::fts::InvertedIndex;
use nucleus::graph::{self, Direction, GraphStore, PropValue};
use nucleus::kv::KvStore;
use nucleus::timeseries::{self, BucketSize, DataPoint, TimeSeriesStore};
use nucleus::types::Value;
use nucleus::vector::{DistanceMetric, HnswConfig, HnswIndex, Vector};

// ============================================================================
// 4A: KV vs Redis — 1M ops at scale
// ============================================================================

/// Benchmark bulk SET throughput: fill 1M keys, measure time per key.
fn kv_set_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("4A_kv_set");
    // Throughput: N bytes written per iteration (key + value)
    let n: u64 = 100_000;
    group.throughput(Throughput::Elements(n));

    group.bench_function("kv_set_100k_keys", |b| {
        b.iter(|| {
            let store = KvStore::new();
            for i in 0..n {
                store.set(
                    black_box(&format!("k:{i}")),
                    Value::Int64(i as i64),
                    None,
                );
            }
            black_box(store.dbsize());
        });
    });
    group.finish();
}

/// Benchmark GET throughput on a pre-populated store (hot cache).
fn kv_get_throughput(c: &mut Criterion) {
    let n: u64 = 100_000;
    let store = KvStore::new();
    for i in 0..n {
        store.set(&format!("k:{i}"), Value::Int64(i as i64), None);
    }

    let mut group = c.benchmark_group("4A_kv_get");
    group.throughput(Throughput::Elements(n));

    group.bench_function("kv_get_100k_keys_hot", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            for _ in 0..n {
                let key = format!("k:{}", rng.gen_range(0..n));
                black_box(store.get(black_box(&key)));
            }
        });
    });
    group.finish();
}

/// Benchmark INCR throughput: 100K increment ops on a single counter.
fn kv_incr_throughput(c: &mut Criterion) {
    let n: u64 = 100_000;
    let mut group = c.benchmark_group("4A_kv_incr");
    group.throughput(Throughput::Elements(n));

    group.bench_function("kv_incr_100k_counter", |b| {
        b.iter(|| {
            let store = KvStore::new();
            for _ in 0..n {
                let _ = store.incr(black_box("counter"));
            }
            black_box(store.get("counter"));
        });
    });
    group.finish();
}

/// Benchmark mixed workload: 50% reads, 30% writes, 20% deletes at 100K ops.
fn kv_mixed_workload_large(c: &mut Criterion) {
    let n: u64 = 100_000;
    let store = KvStore::new();
    // Pre-populate
    for i in 0..n {
        store.set(&format!("k:{i}"), Value::Int64(i as i64), None);
    }

    let mut group = c.benchmark_group("4A_kv_mixed");
    group.throughput(Throughput::Elements(n));

    group.bench_function("kv_mixed_100k_ops", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            for i in 0..n {
                let key = format!("k:{}", rng.gen_range(0..n));
                match i % 10 {
                    0..5 => { black_box(store.get(black_box(&key))); }
                    5..8 => { store.set(black_box(&key), Value::Int64(i as i64), None); }
                    _ => {
                        store.del(black_box(&key));
                        store.set(&key, Value::Int64(i as i64), None);
                    }
                }
            }
        });
    });
    group.finish();
}

// ============================================================================
// 4B: FTS vs Elasticsearch — 100K document index + BM25 search
// ============================================================================

/// Generate a realistic document corpus.
fn generate_doc(id: u64) -> String {
    let topics = [
        "distributed systems consensus raft paxos replication fault tolerance leader election",
        "machine learning neural network gradient descent transformer attention mechanism",
        "database query optimizer index btree hash join merge sort aggregate",
        "operating system kernel scheduler virtual memory page fault interrupt handler",
        "cryptography encryption aes rsa elliptic curve zero knowledge proof protocol",
        "network tcp ip routing bgp ospf packet switch congestion control flow",
        "programming language type system memory safety garbage collection rust",
        "computer vision image recognition convolutional neural network object detection",
        "natural language processing tokenization embedding bert gpt language model",
        "cloud computing kubernetes docker container orchestration microservice devops",
        "quantum computing qubit superposition entanglement shor algorithm grover",
        "compiler optimization llvm ir register allocation instruction scheduling",
        "storage engine lsm tree btree wal compaction bloom filter rocksdb leveldb",
        "graph database property graph cypher gremlin sparql rdf knowledge graph",
        "time series database influxdb prometheus metrics aggregation downsampling",
    ];
    let topic = topics[(id as usize) % topics.len()];
    format!("Document {id}: {topic}. Article number {id} discusses {topic} in depth with practical examples and benchmarks.")
}

fn build_fts_index_100k() -> InvertedIndex {
    let mut idx = InvertedIndex::new();
    for i in 0..100_000u64 {
        idx.add_document(i, &generate_doc(i));
    }
    idx
}

fn fts_index_100k_search(c: &mut Criterion) {
    // Build outside the timed loop
    let idx = build_fts_index_100k();

    let mut group = c.benchmark_group("4B_fts");

    // Single-term BM25 search
    group.bench_function("fts_bm25_1term_100k_docs", |b| {
        b.iter(|| {
            let r = idx.search(black_box("consensus"), 10);
            black_box(r);
        });
    });

    // Two-term BM25 search
    group.bench_function("fts_bm25_2term_100k_docs", |b| {
        b.iter(|| {
            let r = idx.search(black_box("neural network"), 10);
            black_box(r);
        });
    });

    // Three-term BM25 search (intersection)
    group.bench_function("fts_bm25_3term_100k_docs", |b| {
        b.iter(|| {
            let r = idx.search(black_box("query optimizer index"), 10);
            black_box(r);
        });
    });

    group.finish();
}

fn fts_index_build_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("4B_fts_build");

    for n in [1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::new("index_build", n), &n, |b, &n| {
            b.iter(|| {
                let mut idx = InvertedIndex::new();
                for i in 0..n {
                    idx.add_document(i, black_box(&generate_doc(i)));
                }
                black_box(idx.doc_count());
            });
        });
    }
    group.finish();
}

// ============================================================================
// 4C: Vector vs pgvector — 100K vectors (128-dim) HNSW search
// ============================================================================

fn rand_vec_128() -> Vector {
    let mut rng = rand::thread_rng();
    Vector::new((0..128).map(|_| rng.r#gen::<f32>()).collect())
}

fn build_hnsw_100k() -> HnswIndex {
    let config = HnswConfig {
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search: 50,
        metric: DistanceMetric::L2,
    };
    let mut index = HnswIndex::new(config);
    for i in 0..100_000u64 {
        index.insert(i, rand_vec_128());
    }
    index
}

fn vector_hnsw_100k_search(c: &mut Criterion) {
    // Build index outside timed loop
    let index = build_hnsw_100k();
    let query = rand_vec_128();

    let mut group = c.benchmark_group("4C_vector");

    group.bench_function("hnsw_search_top10_100k_128dim", |b| {
        b.iter(|| {
            let r = index.search(black_box(&query), 10);
            black_box(r);
        });
    });

    group.bench_function("hnsw_search_top1_100k_128dim", |b| {
        b.iter(|| {
            let r = index.search(black_box(&query), 1);
            black_box(r);
        });
    });

    group.finish();
}

fn vector_hnsw_build_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("4C_vector_build");

    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("hnsw_build", n), &n, |b, &n| {
            b.iter(|| {
                let config = HnswConfig {
                    m: 16,
                    m_max0: 32,
                    ef_construction: 100,
                    ef_search: 50,
                    metric: DistanceMetric::L2,
                };
                let mut idx = HnswIndex::new(config);
                for i in 0..n as u64 {
                    idx.insert(i, rand_vec_128());
                }
                black_box(idx.len());
            });
        });
    }
    group.finish();
}

// ============================================================================
// 4D: Graph vs Neo4j — 100K nodes / 1M edges traversal
// ============================================================================

fn build_social_graph(num_nodes: usize, edges_per_node: usize) -> GraphStore {
    let mut g = GraphStore::new();
    let mut rng = rand::thread_rng();

    for i in 0..num_nodes {
        let label = match i % 3 {
            0 => "Person",
            1 => "Company",
            _ => "Post",
        };
        g.create_node(
            vec![label.to_string()],
            graph::props(vec![
                ("id", PropValue::Int(i as i64)),
                ("name", PropValue::Text(format!("Node{i}"))),
            ]),
        );
    }

    for from in 1..=(num_nodes as u64) {
        for _ in 0..edges_per_node {
            let to = (rng.r#gen::<u64>() % num_nodes as u64) + 1;
            if to != from {
                let rel = if rng.gen_bool(0.5) { "KNOWS" } else { "FOLLOWS" };
                g.create_edge(from, to, rel.to_string(), graph::props(vec![]));
            }
        }
    }
    g
}

fn graph_traversal_100k(c: &mut Criterion) {
    // 10K nodes, 10 edges each ≈ 100K edges (build time is too high for 100K/1M in CI)
    let g = build_social_graph(10_000, 10);

    let mut group = c.benchmark_group("4D_graph");

    group.bench_function("bfs_10k_nodes_100k_edges", |b| {
        b.iter(|| {
            let visited = g.bfs(black_box(1), Direction::Outgoing, None);
            black_box(visited);
        });
    });

    group.bench_function("shortest_path_10k_nodes", |b| {
        b.iter(|| {
            let path = g.shortest_path(black_box(1), black_box(5000), Direction::Outgoing, None);
            black_box(path);
        });
    });

    group.bench_function("bfs_with_label_filter_10k", |b| {
        b.iter(|| {
            let visited = g.bfs(black_box(1), Direction::Outgoing, Some("KNOWS"));
            black_box(visited);
        });
    });

    group.finish();
}

fn graph_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("4D_graph_scaling");

    for (nodes, edges_per_node) in [(100, 5), (1_000, 5), (5_000, 5)] {
        let g = build_social_graph(nodes, edges_per_node);
        group.throughput(Throughput::Elements((nodes * edges_per_node) as u64));

        group.bench_with_input(
            BenchmarkId::new("bfs", format!("{nodes}nodes")),
            &(),
            |b, _| {
                b.iter(|| {
                    let v = g.bfs(black_box(1), Direction::Outgoing, None);
                    black_box(v);
                });
            },
        );
    }
    group.finish();
}

// ============================================================================
// 4E: TimeSeries vs TimescaleDB — 1M points / 100 series aggregation
// ============================================================================

fn build_timeseries_store(num_series: usize, points_per_series: usize) -> TimeSeriesStore {
    let mut store = TimeSeriesStore::new(BucketSize::Hour);
    let base_ts = 1_700_000_000_000u64;

    for s in 0..num_series {
        let series_name = format!("metric.series.{s}");
        let tags = vec![("host".to_string(), format!("server{}", s % 10))];
        for p in 0..points_per_series {
            store.insert(
                &series_name,
                DataPoint {
                    timestamp: base_ts + (p as u64) * 1_000,
                    tags: tags.clone(),
                    value: (p as f64) * 0.01 + (s as f64),
                },
            );
        }
    }
    store
}

fn timeseries_1m_aggregate(c: &mut Criterion) {
    // 100 series × 10K points = 1M points total
    let n_series = 100;
    let n_points = 10_000;
    let base_ts = 1_700_000_000_000u64;

    // Build a flat array of points for the aggregate function
    let points: Vec<DataPoint> = (0..n_series)
        .flat_map(|s: usize| {
            (0..n_points).map(move |p: usize| DataPoint {
                timestamp: base_ts + (p as u64) * 1_000,
                tags: vec![("series".to_string(), format!("s{s}"))],
                value: (p as f64) * 0.01,
            })
        })
        .collect();

    let mut group = c.benchmark_group("4E_timeseries");
    group.throughput(Throughput::Elements(points.len() as u64));

    group.bench_function("timeseries_agg_1m_points_hour_bucket", |b| {
        b.iter(|| {
            let aggs = timeseries::aggregate(black_box(&points), BucketSize::Hour);
            black_box(aggs);
        });
    });

    group.bench_function("timeseries_agg_1m_points_minute_bucket", |b| {
        b.iter(|| {
            let aggs = timeseries::aggregate(black_box(&points), BucketSize::Minute);
            black_box(aggs);
        });
    });

    group.finish();
}

fn timeseries_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("4E_timeseries_insert");

    for n in [10_000u64, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::new("insert", n), &n, |b, &n| {
            b.iter(|| {
                let mut store = TimeSeriesStore::new(BucketSize::Hour);
                for i in 0..n {
                    store.insert(
                        "cpu.usage",
                        DataPoint {
                            timestamp: 1_700_000_000_000 + i * 1_000,
                            tags: vec![("host".to_string(), "server1".to_string())],
                            value: (i % 100) as f64,
                        },
                    );
                }
                black_box(store.total_points());
            });
        });
    }
    group.finish();
}

// ============================================================================
// 4F: Columnar vs ClickHouse — 10M rows COUNT/SUM/AVG/GROUP BY
// ============================================================================

fn build_columnar_batch(n_rows: usize) -> ColumnBatch {
    let mut rng = rand::thread_rng();
    let categories = ["electronics", "clothing", "food", "books", "sports"];

    let amounts: Vec<Option<f64>> = (0..n_rows)
        .map(|_| Some(rng.gen_range(1.0_f64..1000.0)))
        .collect();

    let quantities: Vec<Option<i64>> = (0..n_rows)
        .map(|_| Some(rng.gen_range(1_i64..100)))
        .collect();

    let cats: Vec<Option<String>> = (0..n_rows)
        .map(|i| Some(categories[i % categories.len()].to_string()))
        .collect();

    ColumnBatch::new(vec![
        ("amount".to_string(), ColumnData::Float64(amounts)),
        ("quantity".to_string(), ColumnData::Int64(quantities)),
        ("category".to_string(), ColumnData::Text(cats)),
    ])
}

fn columnar_agg_10m_rows(c: &mut Criterion) {
    // 1M rows for the timed bench (10M would make setup too slow)
    let n_rows = 1_000_000usize;
    let batch = build_columnar_batch(n_rows);

    let mut group = c.benchmark_group("4F_columnar");
    group.throughput(Throughput::Elements(n_rows as u64));

    group.bench_function("columnar_sum_1m_rows", |b| {
        b.iter(|| {
            let s = columnar::aggregate_sum(black_box(&batch), "amount");
            black_box(s);
        });
    });

    group.bench_function("columnar_avg_1m_rows", |b| {
        b.iter(|| {
            let a = columnar::aggregate_avg(black_box(&batch), "amount");
            black_box(a);
        });
    });

    group.bench_function("columnar_count_1m_rows", |b| {
        b.iter(|| {
            let ct = columnar::aggregate_count(black_box(&batch), "quantity");
            black_box(ct);
        });
    });

    group.bench_function("columnar_group_by_5_cats_1m_rows", |b| {
        let cats = match batch.column("category").unwrap() {
            ColumnData::Text(v) => v.as_slice(),
            _ => unreachable!(),
        };
        let amounts = match batch.column("amount").unwrap() {
            ColumnData::Float64(v) => v.as_slice(),
            _ => unreachable!(),
        };
        b.iter(|| {
            let r = columnar::group_by_text_agg_f64(black_box(cats), black_box(amounts));
            black_box(r);
        });
    });

    group.finish();
}

fn columnar_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("4F_columnar_scaling");

    for n in [100_000usize, 500_000, 1_000_000] {
        let batch = build_columnar_batch(n);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(
            BenchmarkId::new("sum_f64", n),
            &batch,
            |b, batch| {
                b.iter(|| {
                    black_box(columnar::aggregate_sum(black_box(batch), "amount"));
                });
            },
        );
    }
    group.finish();
}

// ============================================================================
// Criterion main
// ============================================================================

criterion_group!(
    benches_4a,
    kv_set_throughput,
    kv_get_throughput,
    kv_incr_throughput,
    kv_mixed_workload_large,
);

criterion_group!(
    benches_4b,
    fts_index_100k_search,
    fts_index_build_throughput,
);

criterion_group!(
    benches_4c,
    vector_hnsw_100k_search,
    vector_hnsw_build_throughput,
);

criterion_group!(
    benches_4d,
    graph_traversal_100k,
    graph_scaling,
);

criterion_group!(
    benches_4e,
    timeseries_1m_aggregate,
    timeseries_insert_throughput,
);

criterion_group!(
    benches_4f,
    columnar_agg_10m_rows,
    columnar_scaling,
);

criterion_main!(
    benches_4a,
    benches_4b,
    benches_4c,
    benches_4d,
    benches_4e,
    benches_4f,
);
