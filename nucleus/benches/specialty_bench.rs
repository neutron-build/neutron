//! Specialty index benchmarks — vector search, full-text search, graph traversals,
//! geospatial distance, time-series bucketing, and key-value operations.
//!
//! Run with: cargo bench --bench specialty_bench
//!
//! These benchmarks cover the extended index types that make Nucleus a
//! single-engine replacement for Elasticsearch, Neo4j, PostGIS, TimescaleDB,
//! Pinecone, Redis, etc.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;

use nucleus::fts::InvertedIndex;
use nucleus::geo::{self, Point};
use nucleus::graph::{self, Direction, GraphStore, Properties, PropValue};
use nucleus::graph::cypher::parse_cypher;
use nucleus::graph::cypher_executor::execute_cypher;
use nucleus::timeseries::{self, BucketSize, DataPoint, TimeSeriesStore};
use nucleus::vector::{DistanceMetric, HnswConfig, HnswIndex, IvfFlatIndex, Vector};
use nucleus::kv::KvStore;
use nucleus::types::Value;

// ============================================================================
// Helpers
// ============================================================================

fn rand_vec(dim: usize) -> Vector {
    let mut rng = rand::thread_rng();
    Vector::new((0..dim).map(|_| rng.r#gen::<f32>()).collect())
}

fn rand_vec_data(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.r#gen::<f32>()).collect()
}

// ============================================================================
// Vector search benchmarks
// ============================================================================

fn vector_hnsw_search(c: &mut Criterion) {
    let dim = 128;
    let n = 1000;

    let config = HnswConfig {
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search: 50,
        metric: DistanceMetric::Cosine,
    };
    let mut index = HnswIndex::new(config);

    for i in 0..n {
        index.insert(i as u64, rand_vec(dim));
    }

    let query = rand_vec(dim);

    c.bench_function("hnsw_search_top10_1000vecs_128dim", |b| {
        b.iter(|| {
            let results = index.search(black_box(&query), 10);
            black_box(results);
        });
    });
}

fn vector_ivfflat_search(c: &mut Criterion) {
    let dim = 128;
    let n = 1000;
    let nlist = 16;
    let nprobe = 4;

    // Generate training data
    let training: Vec<Vec<f32>> = (0..n).map(|_| rand_vec_data(dim)).collect();

    let mut index = IvfFlatIndex::new(dim, nlist, nprobe, DistanceMetric::L2);
    index.train(&training);

    for (i, v) in training.iter().enumerate() {
        index.add(i, v.clone());
    }

    let query = rand_vec_data(dim);

    c.bench_function("ivfflat_search_top10_1000vecs_128dim", |b| {
        b.iter(|| {
            let results = index.search(black_box(&query), 10);
            black_box(results);
        });
    });
}

// ============================================================================
// Full-text search benchmarks
// ============================================================================

fn fts_ranking(c: &mut Criterion) {
    let mut idx = InvertedIndex::new();

    // 100 documents with varied content
    let topics = [
        "quantum computing research advances breakthrough algorithm",
        "machine learning neural network deep learning transformer model",
        "database systems query optimization index storage engine",
        "distributed systems consensus raft paxos replication",
        "programming language rust memory safety performance",
        "cloud computing kubernetes containers microservices",
        "cryptography encryption security protocol zero knowledge",
        "computer vision image recognition object detection",
        "natural language processing text generation embeddings",
        "operating systems kernel scheduler virtual memory",
    ];

    for i in 0..100u64 {
        let topic = topics[(i as usize) % topics.len()];
        let doc = format!(
            "Document {} about {}. This is article number {} discussing {}.",
            i, topic, i, topic
        );
        idx.add_document(i, &doc);
    }

    c.bench_function("fts_bm25_search_100_docs", |b| {
        b.iter(|| {
            let results = idx.search(black_box("quantum computing algorithm"), 10);
            black_box(results);
        });
    });
}

// ============================================================================
// Graph benchmarks
// ============================================================================

/// Build a graph with `num_nodes` nodes and approximately `edges_per_node`
/// outgoing edges per node (randomly assigned).
fn build_random_graph(num_nodes: usize, edges_per_node: usize) -> GraphStore {
    let mut g = GraphStore::new();
    let mut rng = rand::thread_rng();

    // Create nodes with labels
    for i in 0..num_nodes {
        let label = if i % 3 == 0 {
            "Person"
        } else if i % 3 == 1 {
            "Place"
        } else {
            "Thing"
        };
        g.create_node(
            vec![label.to_string()],
            graph::props(vec![
                ("name", PropValue::Text(format!("Node{}", i))),
                ("value", PropValue::Int(i as i64)),
            ]),
        );
    }

    // Create random edges
    for from in 1..=(num_nodes as u64) {
        for _ in 0..edges_per_node {
            let to = (rng.r#gen::<u64>() % (num_nodes as u64)) + 1;
            if to != from {
                let edge_type = if rng.r#gen::<bool>() { "KNOWS" } else { "LINKS" };
                g.create_edge(from, to, edge_type.to_string(), Properties::new());
            }
        }
    }

    g
}

fn graph_bfs_traversal(c: &mut Criterion) {
    let g = build_random_graph(100, 3);

    c.bench_function("graph_bfs_100_nodes_300_edges", |b| {
        b.iter(|| {
            let visited = g.bfs(black_box(1), Direction::Outgoing, None);
            black_box(visited);
        });
    });
}

fn graph_shortest_path(c: &mut Criterion) {
    let g = build_random_graph(100, 3);

    c.bench_function("graph_shortest_path_100_nodes", |b| {
        b.iter(|| {
            let path = g.shortest_path(
                black_box(1),
                black_box(50),
                Direction::Outgoing,
                None,
            );
            black_box(path);
        });
    });
}

fn graph_cypher_match(c: &mut Criterion) {
    let mut g = GraphStore::new();

    // Build a labeled graph for Cypher pattern matching
    for i in 0..50 {
        g.create_node(
            vec!["Person".to_string()],
            graph::props(vec![
                ("name", PropValue::Text(format!("Person{}", i))),
                ("age", PropValue::Int(20 + (i % 40))),
            ]),
        );
    }
    for i in 0..20 {
        g.create_node(
            vec!["Company".to_string()],
            graph::props(vec![
                ("name", PropValue::Text(format!("Company{}", i))),
            ]),
        );
    }

    // Person -> Person (FRIENDS)
    for i in 1..50u64 {
        g.create_edge(i, (i % 49) + 1, "FRIENDS".to_string(), Properties::new());
    }
    // Person -> Company (WORKS_AT)
    for i in 1..=50u64 {
        let company_id = 50 + ((i - 1) % 20) + 1;
        g.create_edge(
            i,
            company_id,
            "WORKS_AT".to_string(),
            graph::props(vec![("since", PropValue::Int(2015 + (i as i64 % 10)))]),
        );
    }

    let stmt = parse_cypher(
        "MATCH (n:Person)-[r:WORKS_AT]->(m:Company) RETURN n.name, m.name"
    ).unwrap();

    c.bench_function("graph_cypher_match_person_works_at_company", |b| {
        b.iter(|| {
            let result = execute_cypher(black_box(&mut g), black_box(&stmt));
            let _ = black_box(result);
        });
    });
}

// ============================================================================
// Geospatial benchmarks
// ============================================================================

fn geo_distance(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    // Pre-generate 1000 point pairs (random lat/lon coordinates)
    let points: Vec<(Point, Point)> = (0..1000)
        .map(|_| {
            let a = Point::new(
                rng.r#gen::<f64>() * 360.0 - 180.0, // longitude
                rng.r#gen::<f64>() * 180.0 - 90.0,  // latitude
            );
            let b = Point::new(
                rng.r#gen::<f64>() * 360.0 - 180.0,
                rng.r#gen::<f64>() * 180.0 - 90.0,
            );
            (a, b)
        })
        .collect();

    c.bench_function("haversine_distance_1000_pairs", |b| {
        b.iter(|| {
            let mut total = 0.0f64;
            for (a, b) in &points {
                total += geo::haversine_distance(black_box(a), black_box(b));
            }
            black_box(total);
        });
    });
}

// ============================================================================
// Time-series benchmarks
// ============================================================================

fn timeseries_bucket(c: &mut Criterion) {
    // Create 10 000 timestamped records and benchmark time bucketing.
    let base_ts = 1_700_000_000_000u64;
    let points: Vec<DataPoint> = (0..10_000)
        .map(|i| DataPoint {
            timestamp: base_ts + i as u64 * 1_000, // 1 point per second
            tags: vec![("host".to_string(), "server1".to_string())],
            value: (i as f64) * 0.5 + 10.0,
        })
        .collect();

    c.bench_function("timeseries_aggregate_10k_points_minute_buckets", |b| {
        b.iter(|| {
            let aggs = timeseries::aggregate(black_box(&points), BucketSize::Minute);
            black_box(aggs);
        });
    });

    // Also benchmark store insert throughput
    c.bench_function("timeseries_store_insert_10k_points", |b| {
        b.iter(|| {
            let mut store = TimeSeriesStore::new(BucketSize::Hour);
            for point in &points {
                store.insert("cpu.usage", point.clone());
            }
            black_box(store.total_points());
        });
    });
}


// ============================================================================
// Key-Value store benchmarks
// ============================================================================

fn kv_put_get_delete(c: &mut Criterion) {
    c.bench_function("kv_put_get_delete_1000_keys", |b| {
        b.iter(|| {
            let store = KvStore::new();

            // PUT 1000 keys
            for i in 0..1000 {
                store.set(
                    black_box(&format!("key:{}", i)),
                    Value::Text(format!("value-{}", i)),
                    None,
                );
            }

            // GET all 1000 keys
            for i in 0..1000 {
                let val = store.get(black_box(&format!("key:{}", i)));
                black_box(val);
            }

            // DELETE all 1000 keys
            for i in 0..1000 {
                store.del(black_box(&format!("key:{}", i)));
            }
        });
    });
}

fn kv_incr_throughput(c: &mut Criterion) {
    c.bench_function("kv_incr_10000_ops", |b| {
        b.iter(|| {
            let store = KvStore::new();
            for _ in 0..10_000 {
                let _ = store.incr(black_box("counter"));
            }
            let val = store.get("counter");
            black_box(val);
        });
    });
}

fn kv_mixed_workload(c: &mut Criterion) {
    // Simulate a realistic mixed workload: 50% reads, 30% writes, 20% deletes
    let store = KvStore::new();

    // Pre-populate with 500 keys
    for i in 0..500 {
        store.set(
            &format!("key:{}", i),
            Value::Int64(i as i64),
            None,
        );
    }

    c.bench_function("kv_mixed_workload_1000_ops", |b| {
        b.iter(|| {
            for i in 0..1000u64 {
                let key = format!("key:{}", i % 500);
                match i % 10 {
                    0..5 => {
                        // 50% reads
                        black_box(store.get(black_box(&key)));
                    }
                    5..8 => {
                        // 30% writes
                        store.set(
                            black_box(&key),
                            Value::Int64(i as i64),
                            None,
                        );
                    }
                    _ => {
                        // 20% deletes (then re-insert so store stays populated)
                        store.del(black_box(&key));
                        store.set(&key, Value::Int64(i as i64), None);
                    }
                }
            }
        });
    });
}

criterion_group!(
    specialty_benches,
    vector_hnsw_search,
    vector_ivfflat_search,
    fts_ranking,
    graph_bfs_traversal,
    graph_shortest_path,
    graph_cypher_match,
    geo_distance,
    timeseries_bucket,
    kv_put_get_delete,
    kv_incr_throughput,
    kv_mixed_workload,
);
criterion_main!(specialty_benches);
