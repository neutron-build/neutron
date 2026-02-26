//! pg_compare — head-to-head benchmark: Nucleus vs PostgreSQL 17
//!
//! Starts an in-process Nucleus pgwire server on port 5454, then runs
//! identical workloads against both databases via tokio-postgres client.
//!
//! Usage:
//!   cargo run --bin pg_compare
//!   cargo run --bin pg_compare -- --pg-port 5432 --pg-user postgres --iterations 200

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{ColumnarStorageEngine, MemoryEngine, MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer};

// ─── CLI ──────────────────────────────────────────────────────────────────────

struct Cfg {
    nucleus_port: u16,
    pg_host: String,
    pg_port: u16,
    pg_user: String,
    iterations: usize,
    rows: usize,
    engine: String,
}

impl Cfg {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut cfg = Cfg {
            nucleus_port: 5454,
            pg_host: "127.0.0.1".into(),
            pg_port: 5432,
            pg_user: "postgres".into(),
            iterations: 100,
            rows: 10_000,
            engine: "mvcc".into(),
        };
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--nucleus-port" => { i += 1; cfg.nucleus_port = args[i].parse().unwrap(); }
                "--pg-host" => { i += 1; cfg.pg_host = args[i].clone(); }
                "--pg-port" => { i += 1; cfg.pg_port = args[i].parse().unwrap(); }
                "--pg-user" => { i += 1; cfg.pg_user = args[i].clone(); }
                "--iterations" => { i += 1; cfg.iterations = args[i].parse().unwrap(); }
                "--rows" => { i += 1; cfg.rows = args[i].parse().unwrap(); }
                "--engine" => { i += 1; cfg.engine = args[i].clone(); }
                _ => {}
            }
            i += 1;
        }
        cfg
    }
}

// ─── Nucleus server ───────────────────────────────────────────────────────────

async fn start_nucleus_server(port: u16, engine: &str) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = match engine {
        "memory"   => Arc::new(MemoryEngine::new()),
        "columnar" => Arc::new(ColumnarStorageEngine::new()),
        _          => Arc::new(MvccStorageAdapter::new()),
    };
    let executor = Arc::new(Executor::new(catalog, storage));
    let handler = Arc::new(NucleusHandler::new(executor));
    let server = Arc::new(NucleusServer::new(handler));

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await.expect("bind nucleus port");
    println!("  Nucleus wire server: {addr}");

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else { break };
            let srv = server.clone();
            tokio::spawn(async move {
                // None = no TLS (plain TCP)
                let _ = pgwire::tokio::process_socket(
                    socket,
                    None::<pgwire::tokio::TlsAcceptor>,
                    srv,
                ).await;
            });
        }
    });
}

// ─── Benchmark helpers ────────────────────────────────────────────────────────

async fn wait_for_port(port: u16) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} did not open in time");
}

struct Stats {
    min: u64,
    median: u64,
    p95: u64,
    max: u64,
    avg: u64,
}

async fn bench_simple(client: &Client, sql: &str, n: usize) -> Stats {
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        client.simple_query(sql).await.unwrap();
        samples.push(t.elapsed());
    }
    samples.sort();
    let us = |d: Duration| d.as_micros() as u64;
    Stats {
        min: us(samples[0]),
        median: us(samples[n / 2]),
        p95: us(samples[(n * 95) / 100]),
        max: us(*samples.last().unwrap()),
        avg: samples.iter().map(|d| us(*d)).sum::<u64>() / n as u64,
    }
}

async fn bench_inserts(client: &Client, start_id: usize, n: usize) -> Stats {
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let id = start_id + i;
        let sql = format!("INSERT INTO bench_orders VALUES ({id},1,99.0,'pending')");
        let t = Instant::now();
        client.simple_query(&sql).await.unwrap();
        samples.push(t.elapsed());
    }
    samples.sort();
    let us = |d: Duration| d.as_micros() as u64;
    Stats {
        min: us(samples[0]),
        median: us(samples[n / 2]),
        p95: us(samples[(n * 95) / 100]),
        max: us(*samples.last().unwrap()),
        avg: samples.iter().map(|d| us(*d)).sum::<u64>() / n as u64,
    }
}

// ─── Schema / data setup ──────────────────────────────────────────────────────

async fn setup(client: &Client, rows: usize) {
    client.simple_query("DROP TABLE IF EXISTS bench_orders").await.unwrap();
    client.simple_query(
        "CREATE TABLE bench_orders (
             id      INT PRIMARY KEY,
             user_id INT NOT NULL,
             amount  FLOAT NOT NULL,
             status  TEXT NOT NULL
         )"
    ).await.unwrap();

    // Bulk insert in chunks of 500
    let chunk = 500usize;
    let mut id = 1usize;
    while id <= rows {
        let end = (id + chunk - 1).min(rows);
        let mut sql = String::from("INSERT INTO bench_orders VALUES ");
        let mut first = true;
        for i in id..=end {
            if !first { sql.push(','); }
            first = false;
            let user_id = (i % 1000) + 1;
            let amount = 10.0 + (i % 500) as f64;
            let status = if i % 3 == 0 { "shipped" } else if i % 3 == 1 { "pending" } else { "cancelled" };
            sql.push_str(&format!("({i},{user_id},{amount},'{status}')"));
        }
        client.simple_query(&sql).await.unwrap();
        id = end + 1;
    }
}

// ─── Print table ──────────────────────────────────────────────────────────────

fn print_header(label: &str) {
    println!("\n{label}");
    println!("{}", "─".repeat(label.len()));
}

fn print_row(query: &str, n: &Stats, p: Option<&Stats>) {
    let speedup = p.map(|pg| {
        if pg.avg == 0 { "  —".to_string() }
        else {
            let r = pg.avg as f64 / n.avg as f64;
            format!("{r:>5.1}×")
        }
    }).unwrap_or_else(|| " n/a".into());

    match p {
        Some(pg) => println!(
            "  {:<32}  Nucleus {:>7}µs avg  PG {:>7}µs avg  speedup {}",
            query, n.avg, pg.avg, speedup
        ),
        None => println!(
            "  {:<32}  Nucleus {:>7}µs avg  (PG unavailable)",
            query, n.avg
        ),
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cfg = Cfg::from_args();

    println!("\n╔══════════════════════════════════════════════════╗");
    println!("║  Nucleus vs PostgreSQL 17 — Wire Protocol Bench  ║");
    println!("╚══════════════════════════════════════════════════╝\n");
    println!("  Iterations : {}", cfg.iterations);
    println!("  Dataset    : {} rows", cfg.rows);
    println!("  Engine     : {}", cfg.engine);

    // ── Start Nucleus server ──────────────────────────────────────────────────
    start_nucleus_server(cfg.nucleus_port, &cfg.engine).await;
    wait_for_port(cfg.nucleus_port).await;
    println!("  Nucleus    : ready\n");

    // ── Connect to Nucleus via wire protocol ──────────────────────────────────
    let n_dsn = format!(
        "host=127.0.0.1 port={} user=nucleus dbname=nucleus",
        cfg.nucleus_port
    );
    let (nc, nc_conn) = tokio_postgres::connect(&n_dsn, NoTls)
        .await
        .expect("connect to Nucleus wire");
    tokio::spawn(nc_conn);

    // ── Connect to PostgreSQL (optional) ──────────────────────────────────────
    let pg_dsn = format!(
        "host={} port={} user={} dbname=postgres",
        cfg.pg_host, cfg.pg_port, cfg.pg_user
    );
    let pg_client: Option<Client> = match tokio_postgres::connect(&pg_dsn, NoTls).await {
        Ok((client, conn)) => {
            tokio::spawn(conn);
            println!("  PostgreSQL : connected ({}:{})", cfg.pg_host, cfg.pg_port);
            Some(client)
        }
        Err(e) => {
            println!("  PostgreSQL : UNAVAILABLE ({e}) — Nucleus-only mode");
            None
        }
    };

    // ── Load identical data ───────────────────────────────────────────────────
    println!("\n  Loading data...");
    let t_setup = Instant::now();
    setup(&nc, cfg.rows).await;
    let nucleus_load = t_setup.elapsed().as_millis();
    let pg_load: Option<u128> = if let Some(ref pg) = pg_client {
        let t = Instant::now();
        setup(pg, cfg.rows).await;
        Some(t.elapsed().as_millis())
    } else {
        None
    };

    match pg_load {
        Some(pg) => println!("  Bulk insert {} rows: Nucleus {}ms  PG {}ms", cfg.rows, nucleus_load, pg),
        None => println!("  Bulk insert {} rows: Nucleus {}ms", cfg.rows, nucleus_load),
    }

    let n = cfg.iterations;

    // ── 1. COUNT(*) ───────────────────────────────────────────────────────────
    let ns_count = bench_simple(&nc, "SELECT COUNT(*) FROM bench_orders", n).await;
    let pg_count: Option<Stats> = if let Some(ref pg) = pg_client {
        Some(bench_simple(pg, "SELECT COUNT(*) FROM bench_orders", n).await)
    } else { None };

    // ── 2. Point query by PK ──────────────────────────────────────────────────
    let ns_pk = bench_simple(&nc, "SELECT * FROM bench_orders WHERE id = 5000", n).await;
    let pg_pk: Option<Stats> = if let Some(ref pg) = pg_client {
        Some(bench_simple(pg, "SELECT * FROM bench_orders WHERE id = 5000", n).await)
    } else { None };

    // ── 3. Range scan (100 rows) ──────────────────────────────────────────────
    let ns_range = bench_simple(&nc, "SELECT * FROM bench_orders WHERE id BETWEEN 1000 AND 1099", n).await;
    let pg_range: Option<Stats> = if let Some(ref pg) = pg_client {
        Some(bench_simple(pg, "SELECT * FROM bench_orders WHERE id BETWEEN 1000 AND 1099", n).await)
    } else { None };

    // ── 4. GROUP BY aggregate ─────────────────────────────────────────────────
    let ns_grp = bench_simple(
        &nc,
        "SELECT status, COUNT(*), AVG(amount) FROM bench_orders GROUP BY status",
        n,
    ).await;
    let pg_grp: Option<Stats> = if let Some(ref pg) = pg_client {
        Some(bench_simple(
            pg,
            "SELECT status, COUNT(*), AVG(amount) FROM bench_orders GROUP BY status",
            n,
        ).await)
    } else { None };

    // ── 5. Filter + ORDER BY + LIMIT ─────────────────────────────────────────
    let ns_filter = bench_simple(
        &nc,
        "SELECT * FROM bench_orders WHERE status = 'pending' ORDER BY amount DESC LIMIT 20",
        n,
    ).await;
    let pg_filter: Option<Stats> = if let Some(ref pg) = pg_client {
        Some(bench_simple(
            pg,
            "SELECT * FROM bench_orders WHERE status = 'pending' ORDER BY amount DESC LIMIT 20",
            n,
        ).await)
    } else { None };

    // ── 6. SUM aggregate ─────────────────────────────────────────────────────
    let ns_sum = bench_simple(
        &nc,
        "SELECT SUM(amount) FROM bench_orders WHERE status = 'shipped'",
        n,
    ).await;
    let pg_sum: Option<Stats> = if let Some(ref pg) = pg_client {
        Some(bench_simple(
            pg,
            "SELECT SUM(amount) FROM bench_orders WHERE status = 'shipped'",
            n,
        ).await)
    } else { None };

    // ── 7. Single-row INSERT ──────────────────────────────────────────────────
    let ns_ins = bench_inserts(&nc, cfg.rows + 1, n).await;
    let pg_ins: Option<Stats> = if let Some(ref pg) = pg_client {
        Some(bench_inserts(pg, cfg.rows + n + 1, n).await)
    } else { None };

    // ── Results ───────────────────────────────────────────────────────────────
    print_header("Results (avg latency per query, lower = better)");
    print_row("COUNT(*)",                  &ns_count,  pg_count.as_ref());
    print_row("Point query (PK lookup)",   &ns_pk,     pg_pk.as_ref());
    print_row("Range scan (100 rows)",     &ns_range,  pg_range.as_ref());
    print_row("GROUP BY + AVG",            &ns_grp,    pg_grp.as_ref());
    print_row("Filter + ORDER + LIMIT 20", &ns_filter, pg_filter.as_ref());
    print_row("SUM aggregate",             &ns_sum,    pg_sum.as_ref());
    print_row("Single-row INSERT",         &ns_ins,    pg_ins.as_ref());

    // ── Detailed stats ────────────────────────────────────────────────────────
    print_header("Nucleus detailed (µs): min / median / p95 / max");
    for (name, s) in [
        ("COUNT(*)",           &ns_count),
        ("Point query",        &ns_pk),
        ("Range scan",         &ns_range),
        ("GROUP BY",           &ns_grp),
        ("Filter+ORDER+LIMIT", &ns_filter),
        ("SUM",                &ns_sum),
        ("INSERT",             &ns_ins),
    ] {
        println!("  {:<28}  {:>5} / {:>5} / {:>5} / {:>5}", name, s.min, s.median, s.p95, s.max);
    }

    if pg_client.is_some() {
        print_header("PostgreSQL detailed (µs): min / median / p95 / max");
        for (name, s) in [
            ("COUNT(*)",           pg_count.as_ref().unwrap()),
            ("Point query",        pg_pk.as_ref().unwrap()),
            ("Range scan",         pg_range.as_ref().unwrap()),
            ("GROUP BY",           pg_grp.as_ref().unwrap()),
            ("Filter+ORDER+LIMIT", pg_filter.as_ref().unwrap()),
            ("SUM",                pg_sum.as_ref().unwrap()),
            ("INSERT",             pg_ins.as_ref().unwrap()),
        ] {
            println!("  {:<28}  {:>5} / {:>5} / {:>5} / {:>5}", name, s.min, s.median, s.p95, s.max);
        }
    }

    println!("\n  (speedup = PG avg / Nucleus avg, >1× means Nucleus is faster)\n");
}
