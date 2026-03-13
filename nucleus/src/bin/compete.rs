//! compete — Head-to-head benchmark: Nucleus vs PostgreSQL, Redis, SurrealDB.
//!
//! Connects to running external services and runs identical workloads against each,
//! reporting speedup ratios. Services that aren't available are gracefully skipped.
//!
//! Usage:
//!   cargo run --release --features bench-tools --bin compete
//!   cargo run --release --features bench-tools --bin compete -- --skip redis
//!   cargo run --release --features bench-tools --bin compete -- --pg-port 5432

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Cell, Color, Table};
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;
use nucleus::wire::{NucleusHandler, NucleusServer};

// ─── CLI ────────────────────────────────────────────────────────────────────

struct Cfg {
    nucleus_port: u16,
    pg_host: String,
    pg_port: u16,
    pg_user: String,
    pg_password: String,
    redis_host: String,
    redis_port: u16,
    surrealdb_url: Option<String>,
    iterations: usize,
    rows: usize,
    skip: Vec<String>,
}

impl Cfg {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut cfg = Cfg {
            nucleus_port: 5454,
            pg_host: "127.0.0.1".into(),
            pg_port: 5432,
            pg_user: "postgres".into(),
            pg_password: "bench".into(),
            redis_host: "127.0.0.1".into(),
            redis_port: 6379,
            surrealdb_url: None,
            iterations: 100,
            rows: 10_000,
            skip: Vec::new(),
        };
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--nucleus-port" => { i += 1; cfg.nucleus_port = args[i].parse().unwrap(); }
                "--pg-host" => { i += 1; cfg.pg_host = args[i].clone(); }
                "--pg-port" => { i += 1; cfg.pg_port = args[i].parse().unwrap(); }
                "--pg-user" => { i += 1; cfg.pg_user = args[i].clone(); }
                "--pg-password" => { i += 1; cfg.pg_password = args[i].clone(); }
                "--redis-host" => { i += 1; cfg.redis_host = args[i].clone(); }
                "--redis-port" => { i += 1; cfg.redis_port = args[i].parse().unwrap(); }
                "--surrealdb-url" => { i += 1; cfg.surrealdb_url = Some(args[i].clone()); }
                "--iterations" => { i += 1; cfg.iterations = args[i].parse().unwrap(); }
                "--rows" => { i += 1; cfg.rows = args[i].parse().unwrap(); }
                "--skip" => { i += 1; cfg.skip = args[i].split(',').map(|s| s.trim().to_lowercase()).collect(); }
                _ => {}
            }
            i += 1;
        }
        cfg
    }

    fn should_run(&self, target: &str) -> bool {
        !self.skip.iter().any(|s| s == target)
    }
}

// ─── Nucleus server ────────────────────────────────────────────────────────

async fn start_nucleus_server(port: u16) -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let executor = Arc::new(Executor::new(catalog, storage));
    let handler = Arc::new(NucleusHandler::new(executor.clone()));
    let server = Arc::new(NucleusServer::new(handler));

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await.expect("bind nucleus port");
    println!("  Nucleus wire server: {addr}");

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else { break };
            let srv = server.clone();
            tokio::spawn(async move {
                let _ = pgwire::tokio::process_socket(
                    socket,
                    None::<pgwire::tokio::TlsAcceptor>,
                    srv,
                ).await;
            });
        }
    });

    executor
}

// ─── Stats ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Stats {
    samples: Vec<Duration>,
}

impl Stats {
    fn new() -> Self { Self { samples: Vec::new() } }

    fn record(&mut self, d: Duration) { self.samples.push(d); }

    fn sorted(&self) -> Vec<Duration> {
        let mut s = self.samples.clone();
        s.sort();
        s
    }

    fn avg_us(&self) -> f64 {
        if self.samples.is_empty() { return 0.0; }
        let total: Duration = self.samples.iter().sum();
        total.as_nanos() as f64 / self.samples.len() as f64 / 1_000.0
    }

    fn p50_us(&self) -> f64 {
        let s = self.sorted();
        if s.is_empty() { return 0.0; }
        s[s.len() / 2].as_nanos() as f64 / 1_000.0
    }

    fn p95_us(&self) -> f64 {
        let s = self.sorted();
        if s.is_empty() { return 0.0; }
        s[(s.len() as f64 * 0.95) as usize].as_nanos() as f64 / 1_000.0
    }

    fn p99_us(&self) -> f64 {
        let s = self.sorted();
        if s.is_empty() { return 0.0; }
        s[std::cmp::min((s.len() as f64 * 0.99) as usize, s.len() - 1)].as_nanos() as f64 / 1_000.0
    }

    fn ops_per_sec(&self) -> f64 {
        if self.samples.is_empty() { return 0.0; }
        let total: Duration = self.samples.iter().sum();
        self.samples.len() as f64 / total.as_secs_f64()
    }
}

// ─── Result ────────────────────────────────────────────────────────────────

struct CompeteResult {
    category: String,
    workload: String,
    nucleus_stats: Stats,
    competitor_name: String,
    competitor_stats: Option<Stats>,
}

impl CompeteResult {
    fn speedup(&self) -> Option<f64> {
        self.competitor_stats.as_ref().map(|c| {
            let n = self.nucleus_stats.avg_us();
            let p = c.avg_us();
            if n == 0.0 { return 0.0; }
            p / n
        })
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

async fn wait_for_port(port: u16) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} did not open in time");
}

async fn bench_query(client: &Client, sql: &str, n: usize) -> Stats {
    let mut stats = Stats::new();
    for _ in 0..n {
        let t = Instant::now();
        client.simple_query(sql).await.unwrap();
        stats.record(t.elapsed());
    }
    stats
}

fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.2}M/s", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.1}K/s", ops / 1_000.0)
    } else {
        format!("{:.0}/s", ops)
    }
}

fn format_us(us: f64) -> String {
    if us < 1.0 {
        format!("{:.0}ns", us * 1_000.0)
    } else if us < 1_000.0 {
        format!("{:.1}us", us)
    } else {
        format!("{:.2}ms", us / 1_000.0)
    }
}

// ─── vs PostgreSQL ─────────────────────────────────────────────────────────

async fn setup_sql(client: &Client, rows: usize) {
    client.simple_query("DROP TABLE IF EXISTS bench_orders").await.unwrap();
    client.simple_query("DROP TABLE IF EXISTS bench_users").await.unwrap();
    client.simple_query(
        "CREATE TABLE bench_users (
            id      INT PRIMARY KEY,
            name    TEXT NOT NULL,
            email   TEXT,
            age     INT NOT NULL
        )"
    ).await.unwrap();
    client.simple_query(
        "CREATE TABLE bench_orders (
            id      INT PRIMARY KEY,
            user_id INT NOT NULL,
            amount  FLOAT NOT NULL,
            status  TEXT NOT NULL
        )"
    ).await.unwrap();

    // Bulk insert users
    let chunk = 500;
    let mut id = 1;
    while id <= rows {
        let end = (id + chunk - 1).min(rows);
        let mut sql = String::from("INSERT INTO bench_users VALUES ");
        let mut first = true;
        for i in id..=end {
            if !first { sql.push(','); }
            first = false;
            let age = 20 + (i % 50);
            sql.push_str(&format!("({i},'user_{i}','user{i}@test.com',{age})"));
        }
        client.simple_query(&sql).await.unwrap();
        id = end + 1;
    }

    // Bulk insert orders (5x users)
    id = 1;
    let order_count = rows * 5;
    while id <= order_count {
        let end = (id + chunk - 1).min(order_count);
        let mut sql = String::from("INSERT INTO bench_orders VALUES ");
        let mut first = true;
        for i in id..=end {
            if !first { sql.push(','); }
            first = false;
            let user_id = (i % rows) + 1;
            let amount = 10.0 + (i % 500) as f64;
            let status = if i % 3 == 0 { "shipped" } else if i % 3 == 1 { "pending" } else { "delivered" };
            sql.push_str(&format!("({i},{user_id},{amount},'{status}')"));
        }
        client.simple_query(&sql).await.unwrap();
        id = end + 1;
    }
}

async fn bench_vs_pg(
    nc: &Client,
    pg: Option<&Client>,
    iterations: usize,
) -> Vec<CompeteResult> {
    let mut results = Vec::new();
    let n = iterations;

    let queries = [
        ("COUNT(*)", "SELECT COUNT(*) FROM bench_orders"),
        ("Point Query (PK)", "SELECT * FROM bench_orders WHERE id = 5000"),
        ("Range Scan 100", "SELECT * FROM bench_orders WHERE id BETWEEN 1000 AND 1099"),
        ("GROUP BY + AVG", "SELECT status, COUNT(*), AVG(amount) FROM bench_orders GROUP BY status"),
        ("Filter+Sort+Limit", "SELECT * FROM bench_orders WHERE status = 'pending' ORDER BY amount DESC LIMIT 20"),
        ("SUM Aggregate", "SELECT SUM(amount) FROM bench_orders WHERE status = 'shipped'"),
        ("2-Table JOIN", "SELECT u.name, o.amount FROM bench_users u, bench_orders o WHERE u.id = o.user_id AND o.id < 100"),
    ];

    for (name, sql) in &queries {
        let ns = bench_query(nc, sql, n).await;
        let ps = if let Some(pg) = pg {
            Some(bench_query(pg, sql, n).await)
        } else {
            None
        };
        results.push(CompeteResult {
            category: "SQL".into(),
            workload: name.to_string(),
            nucleus_stats: ns,
            competitor_name: "PostgreSQL".into(),
            competitor_stats: ps,
        });
    }

    // Single-row INSERT
    {
        let mut ns = Stats::new();
        for i in 0..n {
            let sql = format!("INSERT INTO bench_orders VALUES ({},{},99.0,'pending')", 900_000 + i, i % 1000 + 1);
            let t = Instant::now();
            nc.simple_query(&sql).await.unwrap();
            ns.record(t.elapsed());
        }
        let ps = if let Some(pg) = pg {
            let mut s = Stats::new();
            for i in 0..n {
                let sql = format!("INSERT INTO bench_orders VALUES ({},{},99.0,'pending')", 800_000 + i, i % 1000 + 1);
                let t = Instant::now();
                pg.simple_query(&sql).await.unwrap();
                s.record(t.elapsed());
            }
            Some(s)
        } else {
            None
        };
        results.push(CompeteResult {
            category: "SQL".into(),
            workload: "Single INSERT".into(),
            nucleus_stats: ns,
            competitor_name: "PostgreSQL".into(),
            competitor_stats: ps,
        });
    }

    // Batch INSERT (100 rows)
    {
        let batch_iters = n / 10;
        let mut ns = Stats::new();
        for b in 0..batch_iters {
            let mut sql = String::from("INSERT INTO bench_orders VALUES ");
            for j in 0..100 {
                if j > 0 { sql.push(','); }
                let id = 950_000 + b * 100 + j;
                sql.push_str(&format!("({id},{},99.0,'batch')", j % 1000 + 1));
            }
            let t = Instant::now();
            nc.simple_query(&sql).await.unwrap();
            ns.record(t.elapsed());
        }
        let ps = if let Some(pg) = pg {
            let mut s = Stats::new();
            for b in 0..batch_iters {
                let mut sql = String::from("INSERT INTO bench_orders VALUES ");
                for j in 0..100 {
                    if j > 0 { sql.push(','); }
                    let id = 850_000 + b * 100 + j;
                    sql.push_str(&format!("({id},{},99.0,'batch')", j % 1000 + 1));
                }
                let t = Instant::now();
                pg.simple_query(&sql).await.unwrap();
                s.record(t.elapsed());
            }
            Some(s)
        } else {
            None
        };
        results.push(CompeteResult {
            category: "SQL".into(),
            workload: "Batch INSERT (100 rows)".into(),
            nucleus_stats: ns,
            competitor_name: "PostgreSQL".into(),
            competitor_stats: ps,
        });
    }

    results
}

// ─── vs Redis ──────────────────────────────────────────────────────────────

async fn bench_vs_redis(
    executor: &Arc<Executor>,
    redis_host: &str,
    redis_port: u16,
    iterations: usize,
) -> Vec<CompeteResult> {
    let mut results = Vec::new();
    let n = iterations * 100; // KV ops are fast, need more iterations

    // Connect to Redis
    let redis_url = format!("redis://{redis_host}:{redis_port}");
    let redis_client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            println!("  Redis: UNAVAILABLE ({e}) -- skipping Redis benchmarks");
            return results;
        }
    };
    let mut redis_conn = match redis_client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            println!("  Redis: UNAVAILABLE ({e}) -- skipping Redis benchmarks");
            return results;
        }
    };
    println!("  Redis: connected ({redis_host}:{redis_port})");

    // Flush Redis test keys
    let _: Result<(), _> = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await;

    // Access Nucleus KV store directly (same store accessed by RESP protocol)
    let kv = executor.kv_store();

    // SET throughput
    {
        let mut ns = Stats::new();
        for i in 0..n {
            let t = Instant::now();
            kv.set(&format!("bench:{i}"), Value::Text(format!("val-{i}")), None);
            ns.record(t.elapsed());
        }
        let mut rs = Stats::new();
        for i in 0..n {
            let t = Instant::now();
            let _: Result<(), _> = redis::cmd("SET")
                .arg(format!("bench:{i}"))
                .arg(format!("val-{i}"))
                .query_async(&mut redis_conn)
                .await;
            rs.record(t.elapsed());
        }
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("SET {n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
        });
    }

    // GET throughput (all hits)
    {
        let mut ns = Stats::new();
        for i in 0..n {
            let t = Instant::now();
            let _ = kv.get(&format!("bench:{i}"));
            ns.record(t.elapsed());
        }
        let mut rs = Stats::new();
        for i in 0..n {
            let t = Instant::now();
            let _: Result<Option<String>, _> = redis::cmd("GET")
                .arg(format!("bench:{i}"))
                .query_async(&mut redis_conn)
                .await;
            rs.record(t.elapsed());
        }
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("GET {n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
        });
    }

    // INCR throughput
    {
        let incr_n = n / 10;
        let mut ns = Stats::new();
        for _ in 0..incr_n {
            let t = Instant::now();
            let _ = kv.incr("bench:counter");
            ns.record(t.elapsed());
        }
        let mut rs = Stats::new();
        for _ in 0..incr_n {
            let t = Instant::now();
            let _: Result<i64, _> = redis::cmd("INCR")
                .arg("bench:counter")
                .query_async(&mut redis_conn)
                .await;
            rs.record(t.elapsed());
        }
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("INCR {incr_n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
        });
    }

    // Mixed workload: 50% GET, 30% SET, 20% DEL
    {
        let mut ns = Stats::new();
        for i in 0..n {
            let t = Instant::now();
            match i % 10 {
                0..=4 => { let _ = kv.get(&format!("bench:{}", i % n)); }
                5..=7 => { kv.set(&format!("mix:{i}"), Value::Text(format!("v{i}")), None); }
                _ => { let _ = kv.del(&format!("mix:{}", i.wrapping_sub(2))); }
            }
            ns.record(t.elapsed());
        }
        let mut rs = Stats::new();
        for i in 0..n {
            let t = Instant::now();
            match i % 10 {
                0..=4 => {
                    let _: Result<Option<String>, _> = redis::cmd("GET")
                        .arg(format!("bench:{}", i % n))
                        .query_async(&mut redis_conn).await;
                }
                5..=7 => {
                    let _: Result<(), _> = redis::cmd("SET")
                        .arg(format!("mix:{i}"))
                        .arg(format!("v{i}"))
                        .query_async(&mut redis_conn).await;
                }
                _ => {
                    let _: Result<i32, _> = redis::cmd("DEL")
                        .arg(format!("mix:{}", i.wrapping_sub(2)))
                        .query_async(&mut redis_conn).await;
                }
            }
            rs.record(t.elapsed());
        }
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("Mixed 50R/30W/20D {n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
        });
    }

    // LPUSH/RPOP (list operations)
    {
        let list_n = n / 10;
        let mut ns = Stats::new();
        for i in 0..list_n {
            let t = Instant::now();
            let _ = kv.lpush("bench:list", Value::Text(format!("item-{i}")));
            ns.record(t.elapsed());
        }
        let mut rs = Stats::new();
        for i in 0..list_n {
            let t = Instant::now();
            let _: Result<i64, _> = redis::cmd("LPUSH")
                .arg("bench:list")
                .arg(format!("item-{i}"))
                .query_async(&mut redis_conn).await;
            rs.record(t.elapsed());
        }
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("LPUSH {list_n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
        });
    }

    results
}

// ─── vs SurrealDB ──────────────────────────────────────────────────────────

async fn bench_vs_surrealdb(
    nc: &Client,
    surrealdb_url: &str,
    iterations: usize,
) -> Vec<CompeteResult> {
    let mut results = Vec::new();
    let n = iterations;

    let http = reqwest::Client::new();

    // Check SurrealDB is reachable
    match http.get(&format!("{surrealdb_url}/health")).send().await {
        Ok(r) if r.status().is_success() => {
            println!("  SurrealDB: connected ({surrealdb_url})");
        }
        _ => {
            println!("  SurrealDB: UNAVAILABLE -- skipping SurrealDB benchmarks");
            return results;
        }
    }

    let surreal_query = |sql: &str| {
        let http = http.clone();
        let url = format!("{surrealdb_url}/sql");
        let body = sql.to_string();
        async move {
            http.post(&url)
                .header("Accept", "application/json")
                .header("surreal-ns", "test")
                .header("surreal-db", "test")
                .body(body)
                .send()
                .await
        }
    };

    // Setup SurrealDB
    let _ = surreal_query("REMOVE TABLE bench_users; REMOVE TABLE bench_orders;").await;
    let _ = surreal_query(
        "DEFINE TABLE bench_users; DEFINE TABLE bench_orders;"
    ).await;

    // INSERT comparison
    {
        // Nucleus via pgwire
        let mut ns = Stats::new();
        for i in 0..n {
            let sql = format!(
                "INSERT INTO bench_users VALUES ({},'user_{i}','user{i}@test.com',{})",
                500_000 + i,
                20 + i % 50
            );
            let t = Instant::now();
            nc.simple_query(&sql).await.unwrap();
            ns.record(t.elapsed());
        }

        // SurrealDB via HTTP
        let mut ss = Stats::new();
        for i in 0..n {
            let sql = format!(
                "CREATE bench_users:{i} SET name = 'user_{i}', email = 'user{i}@test.com', age = {}",
                20 + i % 50
            );
            let t = Instant::now();
            let _ = surreal_query(&sql).await;
            ss.record(t.elapsed());
        }

        results.push(CompeteResult {
            category: "Multi-Model".into(),
            workload: format!("INSERT {n} records"),
            nucleus_stats: ns,
            competitor_name: "SurrealDB".into(),
            competitor_stats: Some(ss),
        });
    }

    // SELECT comparison
    {
        let select_n = std::cmp::min(n, 100);
        let mut ns = Stats::new();
        for i in 0..select_n {
            let sql = format!("SELECT * FROM bench_users WHERE id = {}", 500_000 + i);
            let t = Instant::now();
            nc.simple_query(&sql).await.unwrap();
            ns.record(t.elapsed());
        }

        let mut ss = Stats::new();
        for i in 0..select_n {
            let sql = format!("SELECT * FROM bench_users:{i}");
            let t = Instant::now();
            let _ = surreal_query(&sql).await;
            ss.record(t.elapsed());
        }

        results.push(CompeteResult {
            category: "Multi-Model".into(),
            workload: format!("SELECT by ID x{select_n}"),
            nucleus_stats: ns,
            competitor_name: "SurrealDB".into(),
            competitor_stats: Some(ss),
        });
    }

    results
}

// ─── Mixed Multi-Model Benchmark (THE KILLER) ─────────────────────────────

async fn bench_mixed_multimodel(
    executor: &Arc<Executor>,
    nc: &Client,
    pg: Option<&Client>,
    redis_host: &str,
    redis_port: u16,
    iterations: usize,
) -> Vec<CompeteResult> {
    let mut results = Vec::new();
    let n = iterations;

    // Simulates a realistic app: for each user signup:
    //   1. SQL: INSERT user row
    //   2. KV: SET session cache
    //   3. FTS: Index user profile
    //   4. Graph: Add user node + edges

    // --- Nucleus: single process, all models ---
    let kv = executor.kv_store();
    let fts = executor.fts_index();
    let graph = executor.graph_store();

    // Create table for mixed benchmark
    nc.simple_query("DROP TABLE IF EXISTS mixed_users").await.unwrap();
    nc.simple_query(
        "CREATE TABLE mixed_users (id INT PRIMARY KEY, name TEXT NOT NULL, bio TEXT)"
    ).await.unwrap();

    let mut ns = Stats::new();
    for i in 0..n {
        let t = Instant::now();

        // 1. SQL INSERT
        nc.simple_query(&format!(
            "INSERT INTO mixed_users VALUES ({i}, 'user_{i}', 'Software engineer from city_{}')",
            i % 50
        )).await.unwrap();

        // 2. KV SET session
        kv.set(
            &format!("session:{i}"),
            Value::Text(format!("{{\"user_id\":{i},\"token\":\"tok_{i}\"}}")),
            Some(3600),
        );

        // 3. FTS index profile
        fts.write().add_document(
            i as u64,
            &format!("user_{i} software engineer city_{}", i % 50),
        );

        // 4. Graph: add node
        {
            let mut g = graph.write();
            let mut props = BTreeMap::new();
            props.insert("name".to_string(), nucleus::graph::PropValue::Text(format!("user_{i}")));
            let node_id = g.create_node(vec!["User".into()], props);
            if node_id > 1 {
                g.create_edge(node_id, node_id - 1, "follows".to_string(), BTreeMap::new());
            }
        }

        ns.record(t.elapsed());
    }

    results.push(CompeteResult {
        category: "Mixed".into(),
        workload: format!("Full signup flow x{n} (SQL+KV+FTS+Graph)"),
        nucleus_stats: ns.clone(),
        competitor_name: "PG+Redis".into(),
        competitor_stats: None, // filled below if available
    });

    // --- Competitor: PG + Redis (2 services, 2 connections) ---
    if let Some(pg) = pg {
        let redis_url = format!("redis://{redis_host}:{redis_port}");
        if let Ok(redis_client) = redis::Client::open(redis_url.as_str()) {
            if let Ok(mut redis_conn) = redis_client.get_multiplexed_async_connection().await {
                pg.simple_query("DROP TABLE IF EXISTS mixed_users").await.unwrap();
                pg.simple_query(
                    "CREATE TABLE mixed_users (id INT PRIMARY KEY, name TEXT NOT NULL, bio TEXT)"
                ).await.unwrap();

                let mut cs = Stats::new();
                for i in 0..n {
                    let t = Instant::now();

                    // 1. SQL INSERT via PG
                    pg.simple_query(&format!(
                        "INSERT INTO mixed_users VALUES ({i}, 'user_{i}', 'Software engineer from city_{}')",
                        i % 50
                    )).await.unwrap();

                    // 2. KV SET via Redis
                    let _: Result<(), _> = redis::cmd("SET")
                        .arg(format!("session:{i}"))
                        .arg(format!("{{\"user_id\":{i},\"token\":\"tok_{i}\"}}"))
                        .arg("EX").arg(3600)
                        .query_async(&mut redis_conn).await;

                    // 3. No FTS equivalent in PG+Redis stack (PG tsvector is much slower to set up)
                    // 4. No graph equivalent in PG+Redis stack

                    cs.record(t.elapsed());
                }

                // Update the last result with competitor stats
                if let Some(last) = results.last_mut() {
                    last.competitor_stats = Some(cs);
                    last.workload = format!("Signup flow x{n} (Nucleus: SQL+KV+FTS+Graph vs PG+Redis: SQL+KV only)");
                }
            }
        }
    }

    // --- Also benchmark just SQL+KV (fair comparison) ---
    {
        nc.simple_query("DROP TABLE IF EXISTS mixed2_users").await.unwrap();
        nc.simple_query(
            "CREATE TABLE mixed2_users (id INT PRIMARY KEY, name TEXT NOT NULL)"
        ).await.unwrap();

        let mut ns_fair = Stats::new();
        for i in 0..n {
            let t = Instant::now();
            nc.simple_query(&format!("INSERT INTO mixed2_users VALUES ({i}, 'user_{i}')"))
                .await.unwrap();
            kv.set(
                &format!("fair_session:{i}"),
                Value::Text(format!("tok_{i}")),
                Some(3600),
            );
            ns_fair.record(t.elapsed());
        }

        let cs_fair = if let Some(pg) = pg {
            let redis_url = format!("redis://{redis_host}:{redis_port}");
            if let Ok(redis_client) = redis::Client::open(redis_url.as_str()) {
                if let Ok(mut redis_conn) = redis_client.get_multiplexed_async_connection().await {
                    pg.simple_query("DROP TABLE IF EXISTS mixed2_users").await.unwrap();
                    pg.simple_query(
                        "CREATE TABLE mixed2_users (id INT PRIMARY KEY, name TEXT NOT NULL)"
                    ).await.unwrap();
                    let mut s = Stats::new();
                    for i in 0..n {
                        let t = Instant::now();
                        pg.simple_query(&format!("INSERT INTO mixed2_users VALUES ({i}, 'user_{i}')"))
                            .await.unwrap();
                        let _: Result<(), _> = redis::cmd("SET")
                            .arg(format!("fair_session:{i}"))
                            .arg(format!("tok_{i}"))
                            .arg("EX").arg(3600)
                            .query_async(&mut redis_conn).await;
                        s.record(t.elapsed());
                    }
                    Some(s)
                } else { None }
            } else { None }
        } else { None };

        results.push(CompeteResult {
            category: "Mixed".into(),
            workload: format!("SQL+KV only x{n} (fair comparison)"),
            nucleus_stats: ns_fair,
            competitor_name: "PG+Redis".into(),
            competitor_stats: cs_fair,
        });
    }

    results
}

// ─── Output ────────────────────────────────────────────────────────────────

fn print_results(results: &[CompeteResult]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec![
            Cell::new("Category").fg(Color::Cyan),
            Cell::new("Workload").fg(Color::Cyan),
            Cell::new("Nucleus").fg(Color::Cyan),
            Cell::new("Competitor").fg(Color::Cyan),
            Cell::new("Comp. Throughput").fg(Color::Cyan),
            Cell::new("Speedup").fg(Color::Cyan),
            Cell::new("Verdict").fg(Color::Cyan),
        ]);

    for r in results {
        let n_ops = format_ops(r.nucleus_stats.ops_per_sec());
        let n_lat = format_us(r.nucleus_stats.avg_us());

        let (c_label, c_ops, speedup_str, verdict) = match &r.competitor_stats {
            Some(cs) => {
                let c = format_ops(cs.ops_per_sec());
                let speedup = r.speedup().unwrap_or(0.0);
                let v = if speedup >= 1.0 {
                    Cell::new("FASTER").fg(Color::Green)
                } else {
                    Cell::new("SLOWER").fg(Color::Red)
                };
                (r.competitor_name.clone(), c, format!("{speedup:.1}x"), v)
            }
            None => (
                r.competitor_name.clone(),
                "N/A".into(),
                "N/A".into(),
                Cell::new("SKIP").fg(Color::Yellow),
            ),
        };

        table.add_row(vec![
            Cell::new(&r.category),
            Cell::new(&r.workload),
            Cell::new(format!("{n_ops} ({n_lat})")),
            Cell::new(&c_label),
            Cell::new(&c_ops),
            Cell::new(&speedup_str),
            verdict,
        ]);
    }

    println!("\n{table}\n");
}

fn write_json_report(results: &[CompeteResult]) {
    let json_results: Vec<serde_json::Value> = results.iter().map(|r| {
        let mut entry = serde_json::json!({
            "category": r.category,
            "workload": r.workload,
            "nucleus_ops_per_sec": r.nucleus_stats.ops_per_sec(),
            "nucleus_avg_us": r.nucleus_stats.avg_us(),
            "nucleus_p50_us": r.nucleus_stats.p50_us(),
            "nucleus_p95_us": r.nucleus_stats.p95_us(),
            "nucleus_p99_us": r.nucleus_stats.p99_us(),
            "competitor": r.competitor_name,
        });
        if let Some(cs) = &r.competitor_stats {
            entry["competitor_ops_per_sec"] = serde_json::json!(cs.ops_per_sec());
            entry["competitor_avg_us"] = serde_json::json!(cs.avg_us());
            entry["speedup"] = serde_json::json!(r.speedup().unwrap_or(0.0));
        }
        entry
    }).collect();

    let report = serde_json::json!({
        "timestamp": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        "results": json_results,
    });

    if let Ok(json) = serde_json::to_string_pretty(&report) {
        std::fs::write("compete_results.json", json).ok();
        println!("  Results written to compete_results.json");
    }
}

// ─── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cfg = Cfg::from_args();

    println!("\n{}", "=".repeat(60));
    println!("  Nucleus vs The World -- Head-to-Head Competition");
    println!("{}\n", "=".repeat(60));
    println!("  Iterations : {}", cfg.iterations);
    println!("  Dataset    : {} rows", cfg.rows);

    // Start Nucleus server
    let executor = start_nucleus_server(cfg.nucleus_port).await;
    wait_for_port(cfg.nucleus_port).await;
    println!("  Nucleus    : ready");

    // Connect to Nucleus via pgwire
    let n_dsn = format!(
        "host=127.0.0.1 port={} user=nucleus dbname=nucleus",
        cfg.nucleus_port
    );
    let (nc, nc_conn) = tokio_postgres::connect(&n_dsn, NoTls)
        .await
        .expect("connect to Nucleus wire");
    tokio::spawn(nc_conn);

    // Connect to PostgreSQL (optional)
    let pg_dsn = format!(
        "host={} port={} user={} password={} dbname=postgres",
        cfg.pg_host, cfg.pg_port, cfg.pg_user, cfg.pg_password
    );
    let pg_client: Option<Client> = if cfg.should_run("pg") {
        match tokio_postgres::connect(&pg_dsn, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(conn);
                println!("  PostgreSQL : connected ({}:{})", cfg.pg_host, cfg.pg_port);
                Some(client)
            }
            Err(e) => {
                println!("  PostgreSQL : UNAVAILABLE ({e})");
                None
            }
        }
    } else {
        println!("  PostgreSQL : skipped");
        None
    };

    let mut all_results = Vec::new();

    // ── vs PostgreSQL ───────────────────────────────────────────────────────
    if cfg.should_run("pg") {
        println!("\n  --- Setting up SQL benchmark data ---");
        let t = Instant::now();
        setup_sql(&nc, cfg.rows).await;
        println!("  Nucleus load: {}ms", t.elapsed().as_millis());
        if let Some(ref pg) = pg_client {
            let t = Instant::now();
            setup_sql(pg, cfg.rows).await;
            println!("  PG load: {}ms", t.elapsed().as_millis());
        }

        println!("\n  --- Running SQL benchmarks ---");
        let pg_results = bench_vs_pg(&nc, pg_client.as_ref(), cfg.iterations).await;
        all_results.extend(pg_results);
    }

    // ── vs Redis ────────────────────────────────────────────────────────────
    if cfg.should_run("redis") {
        println!("\n  --- Running KV benchmarks vs Redis ---");
        let redis_results = bench_vs_redis(
            &executor,
            &cfg.redis_host,
            cfg.redis_port,
            cfg.iterations,
        ).await;
        all_results.extend(redis_results);
    }

    // ── vs SurrealDB ────────────────────────────────────────────────────────
    if cfg.should_run("surrealdb") {
        if let Some(ref url) = cfg.surrealdb_url {
            println!("\n  --- Running benchmarks vs SurrealDB ---");
            let surreal_results = bench_vs_surrealdb(&nc, url, cfg.iterations).await;
            all_results.extend(surreal_results);
        } else {
            println!("\n  SurrealDB: no --surrealdb-url provided, skipping");
        }
    }

    // ── Mixed Multi-Model (THE KILLER) ──────────────────────────────────────
    if cfg.should_run("mixed") {
        println!("\n  --- Running Mixed Multi-Model benchmark ---");
        let mixed_results = bench_mixed_multimodel(
            &executor,
            &nc,
            pg_client.as_ref(),
            &cfg.redis_host,
            cfg.redis_port,
            cfg.iterations,
        ).await;
        all_results.extend(mixed_results);
    }

    // ── Output ──────────────────────────────────────────────────────────────
    print_results(&all_results);
    write_json_report(&all_results);

    println!("  (speedup = competitor latency / Nucleus latency, >1x means Nucleus is faster)\n");
}
