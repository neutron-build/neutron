//! benchmark — Standalone Nucleus performance report.
//!
//! Runs every data model in-process (no network), measures throughput and latency,
//! and compares against published competitor numbers.
//!
//! Usage:
//!   cargo run --release --features bench-tools --bin benchmark
//!   cargo run --release --features bench-tools --bin benchmark -- --scale large
//!   cargo run --release --features bench-tools --bin benchmark -- --models kv,fts

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Cell, Color, Table};

use nucleus::embedded::Database;
use nucleus::graph::Direction;
use nucleus::types::Value;

// ─── Published competitor reference numbers ──────────────────────────────────

struct Reference {
    name: &'static str,
    ops_per_sec: Option<u64>,
    #[allow(dead_code)]
    latency_us: Option<f64>,
}

fn kv_references() -> Vec<Reference> {
    vec![
        Reference { name: "Redis 7 (single-thread)", ops_per_sec: Some(100_000), latency_us: Some(10.0) },
        Reference { name: "SurrealDB 3 reads", ops_per_sec: Some(508_000), latency_us: None },
        Reference { name: "SurrealDB 3 writes", ops_per_sec: Some(155_000), latency_us: None },
    ]
}

fn sql_references() -> Vec<Reference> {
    vec![
        Reference { name: "PostgreSQL 17 writes", ops_per_sec: Some(205_000), latency_us: None },
        Reference { name: "SurrealDB 3 writes", ops_per_sec: Some(155_000), latency_us: None },
    ]
}

// ─── Statistics ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Stats {
    #[allow(dead_code)]
    name: String,
    samples: Vec<Duration>,
}

impl Stats {
    fn new(name: &str) -> Self {
        Self { name: name.to_string(), samples: Vec::new() }
    }

    fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }

    fn sorted(&self) -> Vec<Duration> {
        let mut s = self.samples.clone();
        s.sort();
        s
    }

    fn p50(&self) -> Duration {
        let s = self.sorted();
        if s.is_empty() { return Duration::ZERO; }
        s[s.len() / 2]
    }

    fn p95(&self) -> Duration {
        let s = self.sorted();
        if s.is_empty() { return Duration::ZERO; }
        s[(s.len() as f64 * 0.95) as usize]
    }

    fn p99(&self) -> Duration {
        let s = self.sorted();
        if s.is_empty() { return Duration::ZERO; }
        s[std::cmp::min((s.len() as f64 * 0.99) as usize, s.len() - 1)]
    }

    fn avg(&self) -> Duration {
        if self.samples.is_empty() { return Duration::ZERO; }
        let total: Duration = self.samples.iter().sum();
        total / self.samples.len() as u32
    }

}

// ─── Benchmark result ────────────────────────────────────────────────────────

struct BenchResult {
    category: String,
    workload: String,
    ops: usize,
    total_time: Duration,
    stats: Stats,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.total_time.as_secs_f64()
    }

    fn avg_latency_us(&self) -> f64 {
        self.stats.avg().as_nanos() as f64 / 1_000.0
    }
}

// ─── Scale ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Scale {
    Small,  // Quick sanity check
    Normal, // Default
    Large,  // 10x for serious benchmarking
}

impl Scale {
    fn multiplier(self) -> usize {
        match self {
            Scale::Small => 1,
            Scale::Normal => 10,
            Scale::Large => 100,
        }
    }
}

// ─── KV Benchmarks ──────────────────────────────────────────────────────────

fn bench_kv(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let n = 10_000 * scale.multiplier();

    // SET throughput
    {
        let kv = db.kv();
        let start = Instant::now();
        let mut stats = Stats::new("KV SET");
        for i in 0..n {
            let t = Instant::now();
            kv.set(&format!("key:{i}"), Value::Text(format!("value-{i}")), None);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "KV".into(), workload: format!("SET {n}"), ops: n, total_time: total, stats });
    }

    // GET throughput (all hits)
    {
        let kv = db.kv();
        let start = Instant::now();
        let mut stats = Stats::new("KV GET");
        for i in 0..n {
            let t = Instant::now();
            let _ = kv.get(&format!("key:{i}"));
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "KV".into(), workload: format!("GET {n}"), ops: n, total_time: total, stats });
    }

    // INCR throughput
    {
        let kv = db.kv();
        let incr_n = n / 10;
        let start = Instant::now();
        let mut stats = Stats::new("KV INCR");
        for _ in 0..incr_n {
            let t = Instant::now();
            let _ = kv.incr("counter:bench");
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "KV".into(), workload: format!("INCR {incr_n}"), ops: incr_n, total_time: total, stats });
    }

    // Mixed workload: 50% GET, 30% SET, 20% DEL
    {
        let kv = db.kv();
        let mix_n = n;
        let start = Instant::now();
        let mut stats = Stats::new("KV Mixed");
        for i in 0..mix_n {
            let t = Instant::now();
            match i % 10 {
                0..=4 => { let _ = kv.get(&format!("key:{}", i % n)); }
                5..=7 => { kv.set(&format!("mix:{i}"), Value::Text(format!("v{i}")), None); }
                _ => { let _ = kv.del(&format!("mix:{}", i.wrapping_sub(2))); }
            }
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "KV".into(), workload: format!("Mixed 50R/30W/20D {mix_n}"), ops: mix_n, total_time: total, stats });
    }

    results
}

// ─── SQL/CRUD Benchmarks ────────────────────────────────────────────────────

async fn bench_sql(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let n = 1_000 * scale.multiplier();

    // Create tables
    db.execute("CREATE TABLE bench_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INT)")
        .await.unwrap();
    db.execute("CREATE TABLE bench_orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, amount FLOAT NOT NULL, status TEXT NOT NULL)")
        .await.unwrap();

    // Bulk INSERT
    {
        let start = Instant::now();
        let mut stats = Stats::new("SQL INSERT");
        for i in 0..n {
            let t = Instant::now();
            db.execute(&format!(
                "INSERT INTO bench_users VALUES ({i}, 'user_{i}', 'user{i}@test.com', {})",
                20 + (i % 50)
            )).await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "SQL".into(), workload: format!("INSERT {n} rows"), ops: n, total_time: total, stats });
    }

    // Insert orders
    for i in 0..(n * 5) {
        db.execute(&format!(
            "INSERT INTO bench_orders VALUES ({i}, {}, {:.2}, '{}')",
            i % n,
            (i as f64) * 1.23,
            if i % 3 == 0 { "shipped" } else if i % 3 == 1 { "pending" } else { "delivered" }
        )).await.unwrap();
    }

    // Point query by PK
    {
        let iters = std::cmp::min(n, 1000);
        let start = Instant::now();
        let mut stats = Stats::new("SQL Point Query");
        for i in 0..iters {
            let t = Instant::now();
            db.query(&format!("SELECT * FROM bench_users WHERE id = {i}")).await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "SQL".into(), workload: format!("Point Query x{iters}"), ops: iters, total_time: total, stats });
    }

    // Range scan
    {
        let iters = std::cmp::min(n / 10, 100);
        let start = Instant::now();
        let mut stats = Stats::new("SQL Range Scan");
        for i in 0..iters {
            let t = Instant::now();
            db.query(&format!("SELECT * FROM bench_users WHERE id BETWEEN {} AND {}", i * 10, i * 10 + 99)).await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "SQL".into(), workload: format!("Range Scan 100 rows x{iters}"), ops: iters, total_time: total, stats });
    }

    // Prepared INSERT
    {
        let prep = db.prepare("INSERT INTO bench_users VALUES ($1, $2, $3, $4)").unwrap();
        // Use a separate range to avoid PK conflicts with earlier inserts
        let prep_start = n;
        let prep_n = std::cmp::min(n, 10_000);
        let start = Instant::now();
        let mut stats = Stats::new("SQL Prepared INSERT");
        for i in 0..prep_n {
            let id = prep_start + i;
            let t = Instant::now();
            db.execute_prepared(&prep, &[
                Value::Int64(id as i64),
                Value::Text(format!("prep_user_{id}")),
                Value::Text(format!("prep{id}@test.com")),
                Value::Int64((20 + (id % 50)) as i64),
            ]).await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "SQL".into(), workload: format!("Prepared INSERT {prep_n} rows"), ops: prep_n, total_time: total, stats });
    }

    // Prepared Point Query
    {
        let prep = db.prepare("SELECT * FROM bench_users WHERE id = $1").unwrap();
        let iters = std::cmp::min(n, 1000);
        let start = Instant::now();
        let mut stats = Stats::new("SQL Prepared Point Query");
        for i in 0..iters {
            let t = Instant::now();
            db.query_prepared(&prep, &[Value::Int64(i as i64)]).await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "SQL".into(), workload: format!("Prepared Point Query x{iters}"), ops: iters, total_time: total, stats });
    }

    // COUNT(*)
    {
        let iters = 100;
        let start = Instant::now();
        let mut stats = Stats::new("SQL COUNT");
        for _ in 0..iters {
            let t = Instant::now();
            db.query("SELECT COUNT(*) FROM bench_orders").await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "SQL".into(), workload: format!("COUNT(*) x{iters}"), ops: iters, total_time: total, stats });
    }

    // GROUP BY + SUM
    {
        let iters = 100;
        let start = Instant::now();
        let mut stats = Stats::new("SQL GROUP BY");
        for _ in 0..iters {
            let t = Instant::now();
            db.query("SELECT status, COUNT(*), SUM(amount) FROM bench_orders GROUP BY status").await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "SQL".into(), workload: format!("GROUP BY+SUM x{iters}"), ops: iters, total_time: total, stats });
    }

    results
}

// ─── FTS Benchmarks ─────────────────────────────────────────────────────────

fn bench_fts(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let n = 1_000 * scale.multiplier();

    let fts = db.fts();

    // Corpus of sample sentences
    let corpus = [
        "The quick brown fox jumps over the lazy dog",
        "Machine learning and artificial intelligence transform industries",
        "Rust programming language offers memory safety without garbage collection",
        "Database systems require efficient indexing strategies",
        "Cloud computing enables scalable distributed applications",
        "Natural language processing advances with transformer models",
        "Quantum computing promises exponential speedups for certain problems",
        "Cybersecurity threats evolve as systems become more complex",
        "Blockchain technology provides decentralized consensus mechanisms",
        "Edge computing reduces latency for real-time applications",
    ];

    // Index documents
    {
        let start = Instant::now();
        let mut stats = Stats::new("FTS Index");
        for i in 0..n {
            let t = Instant::now();
            fts.index(i as u64, corpus[i % corpus.len()]);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "FTS".into(), workload: format!("Index {n} docs"), ops: n, total_time: total, stats });
    }

    // BM25 search
    {
        let queries = ["machine learning", "rust programming", "database", "computing", "systems"];
        let iters = 1_000;
        let start = Instant::now();
        let mut stats = Stats::new("FTS Search");
        for i in 0..iters {
            let t = Instant::now();
            let _ = fts.search(queries[i % queries.len()], 10);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "FTS".into(), workload: format!("BM25 Search x{iters}"), ops: iters, total_time: total, stats });
    }

    results
}

// ─── Graph Benchmarks ───────────────────────────────────────────────────────

fn bench_graph(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let nodes = 1_000 * scale.multiplier();
    let edges_per_node = 5;

    let graph = db.graph();

    // Create nodes
    {
        let start = Instant::now();
        let mut stats = Stats::new("Graph Add Node");
        let mut g = graph.write();
        for i in 0..nodes {
            let t = Instant::now();
            let mut props = BTreeMap::new();
            props.insert("name".to_string(), nucleus::graph::PropValue::Text(format!("node_{i}")));
            g.create_node(vec!["Person".into()], props);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Graph".into(), workload: format!("Add {nodes} nodes"), ops: nodes, total_time: total, stats });
    }

    // Create edges
    {
        let total_edges = nodes * edges_per_node;
        let start = Instant::now();
        let mut stats = Stats::new("Graph Add Edge");
        let mut g = graph.write();
        for i in 0..total_edges {
            let src = (i % nodes) as u64 + 1;
            let dst = ((i * 7 + 3) % nodes) as u64 + 1;
            if src == dst { continue; }
            let t = Instant::now();
            g.create_edge(src, dst, "knows".to_string(), BTreeMap::new());
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Graph".into(), workload: format!("Add ~{total_edges} edges"), ops: total_edges, total_time: total, stats });
    }

    // BFS traversal
    {
        let iters = 100;
        let start = Instant::now();
        let mut stats = Stats::new("Graph BFS");
        let g = graph.read();
        for i in 0..iters {
            let t = Instant::now();
            let _ = g.bfs((i % nodes) as u64 + 1, Direction::Outgoing, None);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Graph".into(), workload: format!("BFS 3-hop x{iters}"), ops: iters, total_time: total, stats });
    }

    // Shortest path
    {
        let iters = 100;
        let start = Instant::now();
        let mut stats = Stats::new("Graph Shortest Path");
        let g = graph.read();
        for i in 0..iters {
            let src = (i % nodes) as u64 + 1;
            let dst = ((i * 13 + 7) % nodes) as u64 + 1;
            let t = Instant::now();
            let _ = g.shortest_path(src, dst, Direction::Outgoing, None);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Graph".into(), workload: format!("Shortest Path x{iters}"), ops: iters, total_time: total, stats });
    }

    results
}

// ─── Vector Benchmarks ──────────────────────────────────────────────────────

async fn bench_vector(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let n = 1_000 * scale.multiplier();
    let dims = 128;

    // Create table with VECTOR column
    db.execute(&format!(
        "CREATE TABLE bench_vecs (id INT PRIMARY KEY, embedding VECTOR({dims}))"
    )).await.unwrap();

    // Insert vectors
    {
        let start = Instant::now();
        let mut stats = Stats::new("Vector Insert");
        for i in 0..n {
            let vec_str: String = (0..dims)
                .map(|d| format!("{:.4}", ((i * dims + d) as f64 * 0.001).sin()))
                .collect::<Vec<_>>()
                .join(",");
            let t = Instant::now();
            db.execute(&format!(
                "INSERT INTO bench_vecs VALUES ({i}, VECTOR('[{vec_str}]'))"
            )).await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Vector".into(), workload: format!("Insert {n} x {dims}d"), ops: n, total_time: total, stats });
    }

    // Create HNSW index (after data load for realistic benchmark)
    db.execute(
        "CREATE INDEX bench_vecs_idx ON bench_vecs USING hnsw (embedding)"
    ).await.unwrap();

    // Top-10 search via VECTOR_DISTANCE + ORDER BY + LIMIT
    {
        let iters = std::cmp::min(100, n);
        let start = Instant::now();
        let mut stats = Stats::new("Vector Search");
        for i in 0..iters {
            let query_vec: String = (0..dims)
                .map(|d| format!("{:.4}", ((i * dims + d) as f64 * 0.002).cos()))
                .collect::<Vec<_>>()
                .join(",");
            let t = Instant::now();
            db.query(&format!(
                "SELECT id FROM bench_vecs ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[{query_vec}]'), 'l2') LIMIT 10"
            )).await.unwrap();
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Vector".into(), workload: format!("Top-10 HNSW x{iters}"), ops: iters, total_time: total, stats });
    }

    results
}

// ─── TimeSeries Benchmarks ──────────────────────────────────────────────────

fn bench_timeseries(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let n = 10_000 * scale.multiplier();

    let ts = db.ts();

    // Insert points
    {
        let start = Instant::now();
        let mut stats = Stats::new("TS Insert");
        for i in 0..n {
            let t = Instant::now();
            ts.insert("cpu_usage", nucleus::timeseries::DataPoint {
                timestamp: 1_700_000_000_000 + (i as u64 * 1_000), // 1s intervals
                tags: vec![("host".to_string(), "server-1".to_string())],
                value: 20.0 + (i as f64 * 0.01).sin() * 30.0,
            });
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "TimeSeries".into(), workload: format!("Insert {n} points"), ops: n, total_time: total, stats });
    }

    // Range aggregation
    {
        let iters = 100;
        let start = Instant::now();
        let mut stats = Stats::new("TS Range Agg");
        for i in 0..iters {
            let range_start = 1_700_000_000_000 + (i as u64 * 10_000);
            let range_end = range_start + 1_000_000;
            let t = Instant::now();
            let _ = ts.range_sum("cpu_usage", range_start, range_end);
            let _ = ts.range_count("cpu_usage", range_start, range_end);
            let _ = ts.range_avg("cpu_usage", range_start, range_end);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "TimeSeries".into(), workload: format!("Range SUM/COUNT/AVG x{iters}"), ops: iters, total_time: total, stats });
    }

    results
}

// ─── Document Benchmarks ────────────────────────────────────────────────────

fn bench_document(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let n = 1_000 * scale.multiplier();

    let doc = db.doc();

    // Insert documents
    {
        let start = Instant::now();
        let mut stats = Stats::new("Doc Insert");
        for i in 0..n {
            let mut obj = BTreeMap::new();
            obj.insert("name".to_string(), nucleus::document::JsonValue::Str(format!("user_{i}")));
            obj.insert("age".to_string(), nucleus::document::JsonValue::Number((20 + i % 50) as f64));
            obj.insert("city".to_string(), nucleus::document::JsonValue::Str(
                ["NYC", "LA", "Chicago", "Houston", "Phoenix"][i % 5].to_string()
            ));
            let t = Instant::now();
            doc.insert(nucleus::document::JsonValue::Object(obj));
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Document".into(), workload: format!("Insert {n} docs"), ops: n, total_time: total, stats });
    }

    // Query by path
    {
        let iters = 1_000;
        let start = Instant::now();
        let mut stats = Stats::new("Doc Query");
        for i in 0..iters {
            let city = ["NYC", "LA", "Chicago", "Houston", "Phoenix"][i % 5];
            let t = Instant::now();
            let _ = doc.query_by_path(&["city"], &nucleus::document::JsonValue::Str(city.to_string()));
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Document".into(), workload: format!("Query by path x{iters}"), ops: iters, total_time: total, stats });
    }

    // Get by ID
    {
        let iters = std::cmp::min(n, 1000);
        let start = Instant::now();
        let mut stats = Stats::new("Doc Get");
        for i in 0..iters {
            let t = Instant::now();
            let _ = doc.get((i + 1) as u64);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Document".into(), workload: format!("Get by ID x{iters}"), ops: iters, total_time: total, stats });
    }

    results
}

// ─── Columnar Benchmarks ────────────────────────────────────────────────────

fn bench_columnar(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let n = 10_000 * scale.multiplier();

    let col = db.columnar();

    // Create table and insert
    {
        let mut c = col.write();
        c.create_table("metrics");
    }

    {
        let start = Instant::now();
        let mut stats = Stats::new("Columnar Insert");
        let batch_size = 1000;
        let batches = n / batch_size;
        for b in 0..batches {
            let t = Instant::now();
            let values: Vec<Option<f64>> = (0..batch_size)
                .map(|i| Some(((b * batch_size + i) as f64) * 1.5))
                .collect();
            let batch = nucleus::columnar::ColumnBatch::new(vec![
                ("value".to_string(), nucleus::columnar::ColumnData::Float64(values)),
            ]);
            col.write().append("metrics", batch);
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Columnar".into(), workload: format!("Insert {n} rows (batched)"), ops: n, total_time: total, stats });
    }

    results
}

// ─── Blob Benchmarks ────────────────────────────────────────────────────────

fn bench_blob(db: &Database, scale: Scale) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let n = 100 * scale.multiplier();

    let blob = db.blob();

    // Store blobs (1KB each)
    let data = vec![0x42u8; 1024];
    {
        let start = Instant::now();
        let mut stats = Stats::new("Blob Store");
        for i in 0..n {
            let t = Instant::now();
            blob.put(&format!("blob_{i}.bin"), &data, Some("application/octet-stream"));
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Blob".into(), workload: format!("Store {n} x 1KB"), ops: n, total_time: total, stats });
    }

    // Get blobs
    {
        let start = Instant::now();
        let mut stats = Stats::new("Blob Get");
        for i in 0..n {
            let t = Instant::now();
            let _ = blob.get(&format!("blob_{i}.bin"));
            stats.record(t.elapsed());
        }
        let total = start.elapsed();
        results.push(BenchResult { category: "Blob".into(), workload: format!("Get {n} x 1KB"), ops: n, total_time: total, stats });
    }

    results
}

// ─── Output ─────────────────────────────────────────────────────────────────

fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.2}M/s", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.1}K/s", ops / 1_000.0)
    } else {
        format!("{:.0}/s", ops)
    }
}

fn format_duration(d: Duration) -> String {
    let us = d.as_nanos() as f64 / 1_000.0;
    if us < 1.0 {
        format!("{:.0}ns", d.as_nanos())
    } else if us < 1_000.0 {
        format!("{:.1}us", us)
    } else {
        format!("{:.2}ms", us / 1_000.0)
    }
}

fn print_results(results: &[BenchResult]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec![
            Cell::new("Category").fg(Color::Cyan),
            Cell::new("Workload").fg(Color::Cyan),
            Cell::new("Throughput").fg(Color::Cyan),
            Cell::new("Avg Latency").fg(Color::Cyan),
            Cell::new("p50").fg(Color::Cyan),
            Cell::new("p95").fg(Color::Cyan),
            Cell::new("p99").fg(Color::Cyan),
            Cell::new("Total").fg(Color::Cyan),
        ]);

    for r in results {
        table.add_row(vec![
            Cell::new(&r.category),
            Cell::new(&r.workload),
            Cell::new(format_ops(r.ops_per_sec())).fg(Color::Green),
            Cell::new(format_duration(r.stats.avg())),
            Cell::new(format_duration(r.stats.p50())),
            Cell::new(format_duration(r.stats.p95())),
            Cell::new(format_duration(r.stats.p99())),
            Cell::new(format_duration(r.total_time)),
        ]);
    }

    println!("\n{table}\n");
}

fn print_comparison(results: &[BenchResult]) {
    println!("  Comparison vs Published Numbers:");
    println!("  {:-<70}", "");

    // KV comparisons
    let kv_set = results.iter().find(|r| r.workload.starts_with("SET"));
    let kv_get = results.iter().find(|r| r.workload.starts_with("GET"));

    if let Some(set) = kv_set {
        let nucleus_ops = set.ops_per_sec();
        for r in kv_references() {
            if let Some(ref_ops) = r.ops_per_sec {
                let ratio = nucleus_ops / ref_ops as f64;
                let verdict = if ratio >= 1.0 { "FASTER" } else { "SLOWER" };
                println!("  KV SET vs {}: {:.1}x {}", r.name, ratio, verdict);
            }
        }
    }

    if let Some(get) = kv_get {
        let nucleus_ops = get.ops_per_sec();
        println!("  KV GET: {} (Nucleus direct API, no network)", format_ops(nucleus_ops));
    }

    // SQL comparisons
    let sql_insert = results.iter().find(|r| r.workload.starts_with("INSERT") && r.category == "SQL");
    if let Some(ins) = sql_insert {
        let nucleus_ops = ins.ops_per_sec();
        for r in sql_references() {
            if let Some(ref_ops) = r.ops_per_sec {
                let ratio = nucleus_ops / ref_ops as f64;
                let verdict = if ratio >= 1.0 { "FASTER" } else { "SLOWER" };
                println!("  SQL INSERT vs {}: {:.1}x {}", r.name, ratio, verdict);
            }
        }
    }

    println!();
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut scale = Scale::Normal;
    let mut model_filter: Option<Vec<String>> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--scale" => {
                i += 1;
                scale = match args.get(i).map(|s| s.as_str()) {
                    Some("small") => Scale::Small,
                    Some("large") => Scale::Large,
                    _ => Scale::Normal,
                };
            }
            "--models" => {
                i += 1;
                if let Some(models) = args.get(i) {
                    model_filter = Some(models.split(',').map(|s| s.trim().to_lowercase()).collect());
                }
            }
            _ => {}
        }
        i += 1;
    }

    let should_run = |model: &str| -> bool {
        model_filter.as_ref().map_or(true, |f| f.iter().any(|m| m == model))
    };

    println!("========================================");
    println!("  Nucleus Standalone Benchmark");
    println!("  Scale: {:?}x", scale.multiplier());
    println!("========================================\n");

    let db = Database::memory();
    let mut all_results = Vec::new();

    if should_run("kv") {
        println!("  Running KV benchmarks...");
        all_results.extend(bench_kv(&db, scale));
    }

    if should_run("sql") || should_run("crud") {
        println!("  Running SQL/CRUD benchmarks...");
        all_results.extend(bench_sql(&db, scale).await);
    }

    if should_run("fts") {
        println!("  Running FTS benchmarks...");
        all_results.extend(bench_fts(&db, scale));
    }

    if should_run("graph") {
        println!("  Running Graph benchmarks...");
        all_results.extend(bench_graph(&db, scale));
    }

    if should_run("vector") {
        println!("  Running Vector benchmarks...");
        all_results.extend(bench_vector(&db, scale).await);
    }

    if should_run("timeseries") || should_run("ts") {
        println!("  Running TimeSeries benchmarks...");
        all_results.extend(bench_timeseries(&db, scale));
    }

    if should_run("document") || should_run("doc") {
        println!("  Running Document benchmarks...");
        all_results.extend(bench_document(&db, scale));
    }

    if should_run("columnar") {
        println!("  Running Columnar benchmarks...");
        all_results.extend(bench_columnar(&db, scale));
    }

    if should_run("blob") {
        println!("  Running Blob benchmarks...");
        all_results.extend(bench_blob(&db, scale));
    }

    print_results(&all_results);
    print_comparison(&all_results);

    // Write JSON report
    let json_results: Vec<serde_json::Value> = all_results.iter().map(|r| {
        serde_json::json!({
            "category": r.category,
            "workload": r.workload,
            "ops": r.ops,
            "total_time_ms": r.total_time.as_millis(),
            "ops_per_sec": r.ops_per_sec(),
            "avg_latency_us": r.avg_latency_us(),
            "p50_us": r.stats.p50().as_nanos() as f64 / 1_000.0,
            "p95_us": r.stats.p95().as_nanos() as f64 / 1_000.0,
            "p99_us": r.stats.p99().as_nanos() as f64 / 1_000.0,
        })
    }).collect();

    let report = serde_json::json!({
        "timestamp": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        "scale": scale.multiplier(),
        "results": json_results,
    });

    let filename = "benchmark_results.json";
    if let Ok(json) = serde_json::to_string_pretty(&report) {
        std::fs::write(filename, json).ok();
        println!("  Results written to {filename}");
    }
}
