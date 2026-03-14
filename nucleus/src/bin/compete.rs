//! compete — Fair head-to-head benchmark: Nucleus vs PostgreSQL and Redis.
//!
//! METHODOLOGY:
//!   - SQL tests: identical pgwire TCP protocol, same queries, same schema, same indexes
//!   - KV tests: Nucleus embedded API vs Redis network (labeled as architectural comparison)
//!   - All measurements: warm-up phase (discarded), then N timed iterations
//!   - Percentiles computed from timed iterations only
//!   - Environment: localhost, single machine, both services running concurrently
//!   - PG config: default installation (out-of-box comparison)
//!
//! Usage:
//!   cargo run --release --features bench-tools --bin compete
//!   cargo run --release --features bench-tools --bin compete -- --skip redis
//!   cargo run --release --features bench-tools --bin compete -- --iterations 2000 --rows 50000

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
    iterations: usize,
    warmup_pct: usize,
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
            pg_password: "".into(),
            redis_host: "127.0.0.1".into(),
            redis_port: 6379,
            iterations: 1000,
            warmup_pct: 20,
            rows: 50_000,
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
                "--iterations" => { i += 1; cfg.iterations = args[i].parse().unwrap(); }
                "--warmup" => { i += 1; cfg.warmup_pct = args[i].parse().unwrap(); }
                "--rows" => { i += 1; cfg.rows = args[i].parse().unwrap(); }
                "--skip" => { i += 1; cfg.skip = args[i].split(',').map(|s| s.trim().to_lowercase()).collect(); }
                "--help" | "-h" => {
                    println!("Usage: compete [OPTIONS]");
                    println!("  --iterations N     Timed iterations per benchmark (default: 1000)");
                    println!("  --warmup N         Warm-up iterations as %% of iterations (default: 20)");
                    println!("  --rows N           Dataset size (default: 50000)");
                    println!("  --pg-port N        PostgreSQL port (default: 5432)");
                    println!("  --pg-user S        PostgreSQL user (default: postgres)");
                    println!("  --pg-password S    PostgreSQL password (default: empty)");
                    println!("  --redis-port N     Redis port (default: 6379)");
                    println!("  --skip LIST        Comma-separated: pg,redis,mixed");
                    std::process::exit(0);
                }
                _ => {}
            }
            i += 1;
        }
        cfg
    }

    fn warmup_n(&self) -> usize {
        self.iterations * self.warmup_pct / 100
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

    fn median_us(&self) -> f64 {
        let s = self.sorted();
        if s.is_empty() { return 0.0; }
        s[s.len() / 2].as_nanos() as f64 / 1_000.0
    }

    fn avg_us(&self) -> f64 {
        if self.samples.is_empty() { return 0.0; }
        let total: Duration = self.samples.iter().sum();
        total.as_nanos() as f64 / self.samples.len() as f64 / 1_000.0
    }

    fn p50_us(&self) -> f64 { self.median_us() }

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
    note: Option<String>,
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

/// Run a query `warmup + n` times, return stats from the last `n` only.
async fn bench_query(client: &Client, sql: &str, warmup: usize, n: usize) -> Stats {
    // Warm-up: run but discard results
    for _ in 0..warmup {
        client.simple_query(sql).await.unwrap();
    }
    // Timed iterations
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

// ─── Schema + Data Setup ──────────────────────────────────────────────────

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

    // Create indexes on commonly queried non-PK columns — SAME for both databases.
    // This ensures PG can use index scans where applicable, matching Nucleus behavior.
    client.simple_query("CREATE INDEX IF NOT EXISTS idx_orders_status ON bench_orders(status)").await.unwrap();
    client.simple_query("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON bench_orders(user_id)").await.unwrap();
    client.simple_query("CREATE INDEX IF NOT EXISTS idx_users_age ON bench_users(age)").await.unwrap();
}

// ─── Section 1: SQL via pgwire (Apples-to-Apples) ─────────────────────────

async fn bench_vs_pg(
    nc: &Client,
    pg: Option<&Client>,
    warmup: usize,
    iterations: usize,
) -> Vec<CompeteResult> {
    let mut results = Vec::new();
    let n = iterations;
    let w = warmup;

    // ── Read Queries ──
    let queries = [
        ("COUNT(*)",               "SELECT COUNT(*) FROM bench_orders"),
        ("Point Query (PK)",       "SELECT * FROM bench_orders WHERE id = 5000"),
        ("Range Scan (BETWEEN)",   "SELECT * FROM bench_orders WHERE id BETWEEN 1000 AND 1099"),
        ("GROUP BY + AVG",         "SELECT status, COUNT(*), AVG(amount) FROM bench_orders GROUP BY status"),
        ("Filter + Sort + Limit",  "SELECT * FROM bench_orders WHERE status = 'pending' ORDER BY amount DESC LIMIT 20"),
        ("SUM with WHERE",         "SELECT SUM(amount) FROM bench_orders WHERE status = 'shipped'"),
        ("2-Table JOIN",           "SELECT u.name, o.amount FROM bench_users u, bench_orders o WHERE u.id = o.user_id AND o.id < 100"),
    ];

    for (name, sql) in &queries {
        print!("    {name:<30}");
        let ns = bench_query(nc, sql, w, n).await;
        let ps = if let Some(pg) = pg {
            Some(bench_query(pg, sql, w, n).await)
        } else {
            None
        };
        let speedup = ps.as_ref().map(|p| p.avg_us() / ns.avg_us()).unwrap_or(0.0);
        println!(" Nucleus: {:>10}  PG: {:>10}  {:.1}x",
            format_ops(ns.ops_per_sec()),
            ps.as_ref().map(|p| format_ops(p.ops_per_sec())).unwrap_or("N/A".into()),
            speedup);
        results.push(CompeteResult {
            category: "SQL".into(),
            workload: name.to_string(),
            nucleus_stats: ns,
            competitor_name: "PostgreSQL".into(),
            competitor_stats: ps,
            note: Some("pgwire".into()),
        });
    }

    // ── Single-row INSERT ──
    {
        print!("    Single INSERT              ");
        // Warm-up
        for i in 0..w {
            let sql = format!("INSERT INTO bench_orders VALUES ({},{},99.0,'pending')", 700_000 + i, i % 1000 + 1);
            nc.simple_query(&sql).await.unwrap();
        }
        if let Some(pg) = pg {
            for i in 0..w {
                let sql = format!("INSERT INTO bench_orders VALUES ({},{},99.0,'pending')", 600_000 + i, i % 1000 + 1);
                pg.simple_query(&sql).await.unwrap();
            }
        }
        // Timed
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
        let speedup = ps.as_ref().map(|p| p.avg_us() / ns.avg_us()).unwrap_or(0.0);
        println!(" Nucleus: {:>10}  PG: {:>10}  {:.1}x",
            format_ops(ns.ops_per_sec()),
            ps.as_ref().map(|p| format_ops(p.ops_per_sec())).unwrap_or("N/A".into()),
            speedup);
        results.push(CompeteResult {
            category: "SQL".into(),
            workload: "Single INSERT".into(),
            nucleus_stats: ns,
            competitor_name: "PostgreSQL".into(),
            competitor_stats: ps,
            note: Some("pgwire".into()),
        });
    }

    // ── Batch INSERT (100 rows per statement) ──
    {
        print!("    Batch INSERT (100 rows)    ");
        let batch_iters = n / 10;
        let batch_warmup = w / 10;
        // Warm-up
        for b in 0..batch_warmup {
            let mut sql = String::from("INSERT INTO bench_orders VALUES ");
            for j in 0..100 {
                if j > 0 { sql.push(','); }
                let id = 1_100_000 + b * 100 + j;
                sql.push_str(&format!("({id},{},99.0,'batch')", j % 1000 + 1));
            }
            nc.simple_query(&sql).await.unwrap();
        }
        if let Some(pg) = pg {
            for b in 0..batch_warmup {
                let mut sql = String::from("INSERT INTO bench_orders VALUES ");
                for j in 0..100 {
                    if j > 0 { sql.push(','); }
                    let id = 1_200_000 + b * 100 + j;
                    sql.push_str(&format!("({id},{},99.0,'batch')", j % 1000 + 1));
                }
                pg.simple_query(&sql).await.unwrap();
            }
        }
        // Timed
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
        let speedup = ps.as_ref().map(|p| p.avg_us() / ns.avg_us()).unwrap_or(0.0);
        println!(" Nucleus: {:>10}  PG: {:>10}  {:.1}x",
            format_ops(ns.ops_per_sec()),
            ps.as_ref().map(|p| format_ops(p.ops_per_sec())).unwrap_or("N/A".into()),
            speedup);
        results.push(CompeteResult {
            category: "SQL".into(),
            workload: "Batch INSERT (100 rows)".into(),
            nucleus_stats: ns,
            competitor_name: "PostgreSQL".into(),
            competitor_stats: ps,
            note: Some("pgwire".into()),
        });
    }

    // ── UPDATE by PK ──
    {
        print!("    UPDATE by PK               ");
        let ns = bench_query(nc, "UPDATE bench_orders SET amount = amount + 1 WHERE id = 5000", w, n).await;
        let ps = if let Some(pg) = pg {
            Some(bench_query(pg, "UPDATE bench_orders SET amount = amount + 1 WHERE id = 5000", w, n).await)
        } else {
            None
        };
        let speedup = ps.as_ref().map(|p| p.avg_us() / ns.avg_us()).unwrap_or(0.0);
        println!(" Nucleus: {:>10}  PG: {:>10}  {:.1}x",
            format_ops(ns.ops_per_sec()),
            ps.as_ref().map(|p| format_ops(p.ops_per_sec())).unwrap_or("N/A".into()),
            speedup);
        results.push(CompeteResult {
            category: "SQL".into(),
            workload: "UPDATE by PK".into(),
            nucleus_stats: ns,
            competitor_name: "PostgreSQL".into(),
            competitor_stats: ps,
            note: Some("pgwire".into()),
        });
    }

    // ── DELETE + re-INSERT (to keep table size stable) ──
    {
        print!("    DELETE by PK               ");
        // Use IDs that won't collide with other tests
        for i in 0..w {
            let id = 2_000_000 + i;
            nc.simple_query(&format!("INSERT INTO bench_orders VALUES ({id},1,50.0,'del')")).await.ok();
        }
        if let Some(pg) = pg {
            for i in 0..w {
                let id = 2_100_000 + i;
                pg.simple_query(&format!("INSERT INTO bench_orders VALUES ({id},1,50.0,'del')")).await.ok();
            }
        }
        // Warm-up
        for i in 0..w {
            nc.simple_query(&format!("DELETE FROM bench_orders WHERE id = {}", 2_000_000 + i)).await.ok();
        }
        if let Some(pg) = pg {
            for i in 0..w {
                pg.simple_query(&format!("DELETE FROM bench_orders WHERE id = {}", 2_100_000 + i)).await.ok();
            }
        }
        // Insert rows for timed deletes
        for i in 0..n {
            let id = 2_200_000 + i;
            nc.simple_query(&format!("INSERT INTO bench_orders VALUES ({id},1,50.0,'del')")).await.ok();
        }
        if let Some(pg) = pg {
            for i in 0..n {
                let id = 2_300_000 + i;
                pg.simple_query(&format!("INSERT INTO bench_orders VALUES ({id},1,50.0,'del')")).await.ok();
            }
        }
        // Timed
        let mut ns = Stats::new();
        for i in 0..n {
            let t = Instant::now();
            nc.simple_query(&format!("DELETE FROM bench_orders WHERE id = {}", 2_200_000 + i)).await.ok();
            ns.record(t.elapsed());
        }
        let ps = if let Some(pg) = pg {
            let mut s = Stats::new();
            for i in 0..n {
                let t = Instant::now();
                pg.simple_query(&format!("DELETE FROM bench_orders WHERE id = {}", 2_300_000 + i)).await.ok();
                s.record(t.elapsed());
            }
            Some(s)
        } else {
            None
        };
        let speedup = ps.as_ref().map(|p| p.avg_us() / ns.avg_us()).unwrap_or(0.0);
        println!(" Nucleus: {:>10}  PG: {:>10}  {:.1}x",
            format_ops(ns.ops_per_sec()),
            ps.as_ref().map(|p| format_ops(p.ops_per_sec())).unwrap_or("N/A".into()),
            speedup);
        results.push(CompeteResult {
            category: "SQL".into(),
            workload: "DELETE by PK".into(),
            nucleus_stats: ns,
            competitor_name: "PostgreSQL".into(),
            competitor_stats: ps,
            note: Some("pgwire".into()),
        });
    }

    results
}

// ─── Section 2: KV (Architectural Comparison) ─────────────────────────────
//
// NOTE: This compares Nucleus embedded KV (in-process, zero network) against
// Redis over localhost TCP. This is NOT an apples-to-apples engine comparison.
// It measures the real-world architectural advantage of having KV built into
// your database process — eliminating the network hop that every Redis call
// requires, even on localhost (~50-100us per roundtrip).
//
// For apps that use Redis solely as a cache alongside their SQL database,
// Nucleus eliminates that entire network layer.

async fn bench_vs_redis(
    executor: &Arc<Executor>,
    redis_host: &str,
    redis_port: u16,
    warmup: usize,
    iterations: usize,
) -> Vec<CompeteResult> {
    let mut results = Vec::new();
    let n = iterations * 100; // KV ops are fast, need more iterations
    let w = warmup * 100;

    // Connect to Redis
    let redis_url = format!("redis://{redis_host}:{redis_port}");
    let redis_client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            println!("    Redis: UNAVAILABLE ({e}) -- skipping");
            return results;
        }
    };
    let mut redis_conn = match redis_client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            println!("    Redis: UNAVAILABLE ({e}) -- skipping");
            return results;
        }
    };
    println!("    Redis: connected ({redis_host}:{redis_port})");

    let _: Result<(), _> = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await;

    let kv = executor.kv_store();

    // SET
    {
        print!("    SET {n:<25}");
        // Warm-up
        for i in 0..w {
            kv.set(&format!("w:{i}"), Value::Text(format!("v-{i}")), None);
        }
        for i in 0..w {
            let _: Result<(), _> = redis::cmd("SET")
                .arg(format!("w:{i}")).arg(format!("v-{i}"))
                .query_async(&mut redis_conn).await;
        }
        // Timed
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
                .arg(format!("bench:{i}")).arg(format!("val-{i}"))
                .query_async(&mut redis_conn).await;
            rs.record(t.elapsed());
        }
        let speedup = rs.avg_us() / ns.avg_us();
        println!(" Nucleus: {:>10}  Redis: {:>10}  {:.0}x",
            format_ops(ns.ops_per_sec()), format_ops(rs.ops_per_sec()), speedup);
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("SET {n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
            note: Some("embedded vs network".into()),
        });
    }

    // GET (all hits)
    {
        print!("    GET {n:<25}");
        // Warm-up
        for i in 0..w { let _ = kv.get(&format!("bench:{i}")); }
        for i in 0..w {
            let _: Result<Option<String>, _> = redis::cmd("GET")
                .arg(format!("bench:{i}")).query_async(&mut redis_conn).await;
        }
        // Timed
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
                .arg(format!("bench:{i}")).query_async(&mut redis_conn).await;
            rs.record(t.elapsed());
        }
        let speedup = rs.avg_us() / ns.avg_us();
        println!(" Nucleus: {:>10}  Redis: {:>10}  {:.0}x",
            format_ops(ns.ops_per_sec()), format_ops(rs.ops_per_sec()), speedup);
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("GET {n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
            note: Some("embedded vs network".into()),
        });
    }

    // INCR
    {
        let incr_n = n / 10;
        let incr_w = w / 10;
        print!("    INCR {incr_n:<24}");
        for _ in 0..incr_w { let _ = kv.incr("warmup:counter"); }
        for _ in 0..incr_w {
            let _: Result<i64, _> = redis::cmd("INCR")
                .arg("warmup:counter").query_async(&mut redis_conn).await;
        }
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
                .arg("bench:counter").query_async(&mut redis_conn).await;
            rs.record(t.elapsed());
        }
        let speedup = rs.avg_us() / ns.avg_us();
        println!(" Nucleus: {:>10}  Redis: {:>10}  {:.0}x",
            format_ops(ns.ops_per_sec()), format_ops(rs.ops_per_sec()), speedup);
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("INCR {incr_n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
            note: Some("embedded vs network".into()),
        });
    }

    // Mixed workload: 50% GET, 30% SET, 20% DEL
    {
        print!("    Mixed 50R/30W/20D {n:<8}");
        // Warm-up both sides
        for i in 0..w {
            match i % 10 {
                0..=4 => { let _ = kv.get(&format!("bench:{}", i % n)); }
                5..=7 => { kv.set(&format!("wmix:{i}"), Value::Text(format!("v{i}")), None); }
                _ => { let _ = kv.del(&format!("wmix:{}", i.wrapping_sub(2))); }
            }
        }
        for i in 0..w {
            match i % 10 {
                0..=4 => { let _: Result<Option<String>, _> = redis::cmd("GET").arg(format!("bench:{}", i % n)).query_async(&mut redis_conn).await; }
                5..=7 => { let _: Result<(), _> = redis::cmd("SET").arg(format!("wmix:{i}")).arg(format!("v{i}")).query_async(&mut redis_conn).await; }
                _ => { let _: Result<i32, _> = redis::cmd("DEL").arg(format!("wmix:{}", i.wrapping_sub(2))).query_async(&mut redis_conn).await; }
            }
        }
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
                0..=4 => { let _: Result<Option<String>, _> = redis::cmd("GET").arg(format!("bench:{}", i % n)).query_async(&mut redis_conn).await; }
                5..=7 => { let _: Result<(), _> = redis::cmd("SET").arg(format!("mix:{i}")).arg(format!("v{i}")).query_async(&mut redis_conn).await; }
                _ => { let _: Result<i32, _> = redis::cmd("DEL").arg(format!("mix:{}", i.wrapping_sub(2))).query_async(&mut redis_conn).await; }
            }
            rs.record(t.elapsed());
        }
        let speedup = rs.avg_us() / ns.avg_us();
        println!(" Nucleus: {:>10}  Redis: {:>10}  {:.0}x",
            format_ops(ns.ops_per_sec()), format_ops(rs.ops_per_sec()), speedup);
        results.push(CompeteResult {
            category: "KV".into(),
            workload: format!("Mixed 50R/30W/20D {n}"),
            nucleus_stats: ns,
            competitor_name: "Redis".into(),
            competitor_stats: Some(rs),
            note: Some("embedded vs network".into()),
        });
    }

    results
}

// ─── Section 3: Mixed Multi-Model (Architectural) ─────────────────────────

async fn bench_mixed_multimodel(
    executor: &Arc<Executor>,
    nc: &Client,
    pg: Option<&Client>,
    redis_host: &str,
    redis_port: u16,
    warmup: usize,
    iterations: usize,
) -> Vec<CompeteResult> {
    let mut results = Vec::new();
    let n = iterations;
    let w = warmup;

    let kv = executor.kv_store();
    let fts = executor.fts_index();
    let graph = executor.graph_store();

    // ── SQL+KV fair comparison (both use same number of operations) ──
    nc.simple_query("DROP TABLE IF EXISTS mixed_users").await.unwrap();
    nc.simple_query(
        "CREATE TABLE mixed_users (id INT PRIMARY KEY, name TEXT NOT NULL)"
    ).await.unwrap();

    // Warm-up Nucleus
    for i in 0..w {
        let id = 5_000_000 + i;
        nc.simple_query(&format!("INSERT INTO mixed_users VALUES ({id}, 'wu_{i}')")).await.unwrap();
        kv.set(&format!("wsess:{i}"), Value::Text(format!("tok_{i}")), Some(3600));
    }

    // Timed: Nucleus SQL (pgwire) + KV (embedded)
    let mut ns = Stats::new();
    for i in 0..n {
        let t = Instant::now();
        // 1. SQL INSERT via pgwire (same protocol as PG)
        nc.simple_query(&format!("INSERT INTO mixed_users VALUES ({i}, 'user_{i}')"))
            .await.unwrap();
        // 2. KV SET (embedded — this is the architectural advantage)
        kv.set(
            &format!("session:{i}"),
            Value::Text(format!("tok_{i}")),
            Some(3600),
        );
        ns.record(t.elapsed());
    }

    let cs = if let Some(pg) = pg {
        let redis_url = format!("redis://{redis_host}:{redis_port}");
        if let Ok(redis_client) = redis::Client::open(redis_url.as_str()) {
            if let Ok(mut redis_conn) = redis_client.get_multiplexed_async_connection().await {
                pg.simple_query("DROP TABLE IF EXISTS mixed_users").await.unwrap();
                pg.simple_query(
                    "CREATE TABLE mixed_users (id INT PRIMARY KEY, name TEXT NOT NULL)"
                ).await.unwrap();
                // Warm-up PG+Redis
                for i in 0..w {
                    let id = 5_000_000 + i;
                    pg.simple_query(&format!("INSERT INTO mixed_users VALUES ({id}, 'wu_{i}')")).await.unwrap();
                    let _: Result<(), _> = redis::cmd("SET")
                        .arg(format!("wsess:{i}")).arg(format!("tok_{i}"))
                        .arg("EX").arg(3600)
                        .query_async(&mut redis_conn).await;
                }
                // Timed: PG (pgwire) + Redis (network)
                let mut s = Stats::new();
                for i in 0..n {
                    let t = Instant::now();
                    pg.simple_query(&format!("INSERT INTO mixed_users VALUES ({i}, 'user_{i}')"))
                        .await.unwrap();
                    let _: Result<(), _> = redis::cmd("SET")
                        .arg(format!("session:{i}")).arg(format!("tok_{i}"))
                        .arg("EX").arg(3600)
                        .query_async(&mut redis_conn).await;
                    s.record(t.elapsed());
                }
                Some(s)
            } else { None }
        } else { None }
    } else { None };

    let speedup = cs.as_ref().map(|c| c.avg_us() / ns.avg_us()).unwrap_or(0.0);
    print!("    SQL+KV x{n:<20}");
    println!(" Nucleus: {:>10}  PG+Redis: {:>10}  {:.1}x",
        format_ops(ns.ops_per_sec()),
        cs.as_ref().map(|c| format_ops(c.ops_per_sec())).unwrap_or("N/A".into()),
        speedup);

    results.push(CompeteResult {
        category: "Multi-Model".into(),
        workload: format!("SQL+KV x{n} (Nucleus: pgwire+embedded KV vs PG+Redis: pgwire+network KV)"),
        nucleus_stats: ns,
        competitor_name: "PG+Redis".into(),
        competitor_stats: cs,
        note: Some("Nucleus SQL via pgwire (same as PG), KV via embedded API (no network)".into()),
    });

    // ── Full signup flow: SQL + KV + FTS + Graph ──
    // NOTE: PG+Redis cannot do FTS+Graph in-process, so this shows what Nucleus
    // can do in a single process that would require 3-4 services otherwise.
    nc.simple_query("DROP TABLE IF EXISTS signup_users").await.unwrap();
    nc.simple_query(
        "CREATE TABLE signup_users (id INT PRIMARY KEY, name TEXT NOT NULL, bio TEXT)"
    ).await.unwrap();

    // Warm-up
    for i in 0..w {
        let id = 6_000_000 + i;
        nc.simple_query(&format!(
            "INSERT INTO signup_users VALUES ({id}, 'wu_{i}', 'engineer from city_{}')", i % 50
        )).await.unwrap();
        kv.set(&format!("wsignup:{i}"), Value::Text(format!("tok_{i}")), Some(3600));
        fts.write().add_document(1_000_000 + i as u64, &format!("wu_{i} engineer city_{}", i % 50));
        {
            let mut g = graph.write();
            let mut props = BTreeMap::new();
            props.insert("name".to_string(), nucleus::graph::PropValue::Text(format!("wu_{i}")));
            g.create_node(vec!["User".into()], props);
        }
    }

    let mut ns_full = Stats::new();
    for i in 0..n {
        let t = Instant::now();
        nc.simple_query(&format!(
            "INSERT INTO signup_users VALUES ({i}, 'user_{i}', 'engineer from city_{}')", i % 50
        )).await.unwrap();
        kv.set(&format!("signup:{i}"), Value::Text(format!("tok_{i}")), Some(3600));
        fts.write().add_document(i as u64, &format!("user_{i} engineer city_{}", i % 50));
        {
            let mut g = graph.write();
            let mut props = BTreeMap::new();
            props.insert("name".to_string(), nucleus::graph::PropValue::Text(format!("user_{i}")));
            let node_id = g.create_node(vec!["User".into()], props);
            if node_id > 1 {
                g.create_edge(node_id, node_id - 1, "follows".to_string(), BTreeMap::new());
            }
        }
        ns_full.record(t.elapsed());
    }

    print!("    Full signup x{n:<16}");
    println!(" Nucleus: {:>10} (SQL+KV+FTS+Graph, single process)",
        format_ops(ns_full.ops_per_sec()));

    results.push(CompeteResult {
        category: "Multi-Model".into(),
        workload: format!("Full signup x{n} (SQL+KV+FTS+Graph in single process)"),
        nucleus_stats: ns_full,
        competitor_name: "PG+Redis+Elastic+Neo4j".into(),
        competitor_stats: None,
        note: Some("No direct competitor — would require 4 services".into()),
    });

    results
}

// ─── Output ────────────────────────────────────────────────────────────────

fn print_results(results: &[CompeteResult]) {
    println!();
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec![
            Cell::new("Category").fg(Color::Cyan),
            Cell::new("Workload").fg(Color::Cyan),
            Cell::new("Nucleus ops/s").fg(Color::Cyan),
            Cell::new("Nucleus p50").fg(Color::Cyan),
            Cell::new("Competitor").fg(Color::Cyan),
            Cell::new("Comp. ops/s").fg(Color::Cyan),
            Cell::new("Comp. p50").fg(Color::Cyan),
            Cell::new("Speedup").fg(Color::Cyan),
            Cell::new("Note").fg(Color::Cyan),
        ]);

    for r in results {
        let n_ops = format_ops(r.nucleus_stats.ops_per_sec());
        let n_p50 = format_us(r.nucleus_stats.p50_us());

        let (c_ops, c_p50, speedup_str, color) = match &r.competitor_stats {
            Some(cs) => {
                let speedup = r.speedup().unwrap_or(0.0);
                let color = if speedup >= 1.0 { Color::Green } else { Color::Red };
                (
                    format_ops(cs.ops_per_sec()),
                    format_us(cs.p50_us()),
                    format!("{speedup:.1}x"),
                    color,
                )
            }
            None => ("N/A".into(), "N/A".into(), "N/A".into(), Color::Yellow),
        };

        table.add_row(vec![
            Cell::new(&r.category),
            Cell::new(&r.workload),
            Cell::new(&n_ops),
            Cell::new(&n_p50),
            Cell::new(&r.competitor_name),
            Cell::new(&c_ops),
            Cell::new(&c_p50),
            Cell::new(&speedup_str).fg(color),
            Cell::new(r.note.as_deref().unwrap_or("")),
        ]);
    }

    println!("{table}");
    println!();
    println!("  Speedup = competitor_latency / nucleus_latency (>1x = Nucleus faster)");
    println!("  p50 = median latency");
    println!();
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
            "note": r.note,
        });
        if let Some(cs) = &r.competitor_stats {
            entry["competitor_ops_per_sec"] = serde_json::json!(cs.ops_per_sec());
            entry["competitor_avg_us"] = serde_json::json!(cs.avg_us());
            entry["competitor_p50_us"] = serde_json::json!(cs.p50_us());
            entry["competitor_p95_us"] = serde_json::json!(cs.p95_us());
            entry["competitor_p99_us"] = serde_json::json!(cs.p99_us());
            entry["speedup"] = serde_json::json!(r.speedup().unwrap_or(0.0));
        }
        entry
    }).collect();

    let report = serde_json::json!({
        "timestamp": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        "methodology": {
            "sql_protocol": "pgwire TCP (identical for both databases)",
            "kv_comparison": "Nucleus embedded API vs Redis localhost TCP",
            "warmup": "20% of iterations discarded before timing",
            "indexes": "Same B-tree indexes on both databases (PK + status + user_id + age)",
            "pg_config": "default installation, no tuning",
        },
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

    println!();
    println!("{}", "=".repeat(70));
    println!("  Nucleus Competitive Benchmark");
    println!("{}", "=".repeat(70));
    println!();
    println!("  METHODOLOGY:");
    println!("    SQL:     Both databases receive identical SQL over pgwire TCP");
    println!("    KV:      Nucleus embedded API vs Redis localhost TCP");
    println!("             (measures architectural advantage, not engine speed)");
    println!("    Indexes: Same B-tree indexes on PK + status + user_id + age");
    println!("    Warm-up: {}% of iterations ({}) discarded before timing",
        cfg.warmup_pct, cfg.warmup_n());
    println!("    Timed:   {} iterations per benchmark", cfg.iterations);
    println!("    Dataset: {} users + {} orders", cfg.rows, cfg.rows * 5);
    println!();

    // Start Nucleus server
    let executor = start_nucleus_server(cfg.nucleus_port).await;
    wait_for_port(cfg.nucleus_port).await;
    println!("  Nucleus    : ready (127.0.0.1:{})", cfg.nucleus_port);

    // Connect to Nucleus via pgwire
    let n_dsn = format!(
        "host=127.0.0.1 port={} user=nucleus dbname=nucleus",
        cfg.nucleus_port
    );
    let (nc, nc_conn) = tokio_postgres::connect(&n_dsn, NoTls)
        .await
        .expect("connect to Nucleus wire");
    tokio::spawn(nc_conn);

    // Connect to PostgreSQL
    let pg_dsn = if cfg.pg_password.is_empty() {
        format!(
            "host={} port={} user={} dbname=postgres",
            cfg.pg_host, cfg.pg_port, cfg.pg_user
        )
    } else {
        format!(
            "host={} port={} user={} password={} dbname=postgres",
            cfg.pg_host, cfg.pg_port, cfg.pg_user, cfg.pg_password
        )
    };
    let pg_client: Option<Client> = if cfg.should_run("pg") {
        match tokio_postgres::connect(&pg_dsn, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(conn);
                // Get PG version for transparency
                let ver = client.simple_query("SELECT version()").await
                    .ok()
                    .and_then(|r| {
                        r.into_iter().find_map(|msg| {
                            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                                row.get(0).map(|s| s.to_string())
                            } else { None }
                        })
                    })
                    .unwrap_or_else(|| "unknown".into());
                println!("  PostgreSQL : {} ({}:{})", ver.split(',').next().unwrap_or(&ver), cfg.pg_host, cfg.pg_port);
                Some(client)
            }
            Err(e) => {
                println!("  PostgreSQL : UNAVAILABLE ({e})");
                println!("    DSN: {pg_dsn}");
                None
            }
        }
    } else {
        println!("  PostgreSQL : skipped");
        None
    };

    let mut all_results = Vec::new();

    // ── Section 1: SQL via pgwire ──
    if cfg.should_run("pg") {
        println!();
        println!("  --- Section 1: SQL via pgwire (apples-to-apples) ---");
        println!();
        let t = Instant::now();
        setup_sql(&nc, cfg.rows).await;
        println!("    Nucleus data load: {}ms", t.elapsed().as_millis());
        if let Some(ref pg) = pg_client {
            let t = Instant::now();
            setup_sql(pg, cfg.rows).await;
            println!("    PG data load:     {}ms", t.elapsed().as_millis());
        }
        println!();

        let pg_results = bench_vs_pg(&nc, pg_client.as_ref(), cfg.warmup_n(), cfg.iterations).await;
        all_results.extend(pg_results);
    }

    // ── Section 2: KV (architectural comparison) ──
    if cfg.should_run("redis") {
        println!();
        println!("  --- Section 2: KV — embedded vs network (architectural) ---");
        println!("    NOTE: Nucleus KV = in-process API (0 network hops)");
        println!("          Redis     = localhost TCP (~50-100us per roundtrip)");
        println!();

        let redis_results = bench_vs_redis(
            &executor, &cfg.redis_host, cfg.redis_port,
            cfg.warmup_n(), cfg.iterations,
        ).await;
        all_results.extend(redis_results);
    }

    // ── Section 3: Mixed Multi-Model ──
    if cfg.should_run("mixed") {
        println!();
        println!("  --- Section 3: Multi-Model workloads (architectural) ---");
        println!("    Nucleus: single process (SQL via pgwire + KV/FTS/Graph embedded)");
        println!("    PG+Redis: two services, two network connections");
        println!();

        let mixed_results = bench_mixed_multimodel(
            &executor, &nc, pg_client.as_ref(),
            &cfg.redis_host, cfg.redis_port,
            cfg.warmup_n(), cfg.iterations,
        ).await;
        all_results.extend(mixed_results);
    }

    // ── Results ──
    print_results(&all_results);
    write_json_report(&all_results);
}
