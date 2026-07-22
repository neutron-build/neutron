//! Long-running soak harness for the durable engine (roadmap T1.4).
//!
//! Drives a sustained, concurrent, mixed-model workload against a WAL-backed
//! MVCC database and validates the long-running-DB invariants:
//!   * no unbounded memory growth (leak detection) under a BOUNDED working set
//!     — each worker keeps a fixed-size ring of live rows (insert new, delete
//!     oldest), so table size plateaus and any RSS growth is a leak, not data;
//!   * no crashes / no unexpected error storm under concurrency;
//!   * index coherence survives sustained churn (PK uniqueness, btree and
//!     encrypted equality) — the same bug class the coherence oracle guards;
//!   * durability: after closing and reopening, committed rows survive and
//!     stay coherent.
//!
//! Exit code is non-zero on any failed invariant, so CI can gate on it.
//!
//! `cargo run --release --features server --bin probe_soak -- --duration-secs 30`
#![cfg(feature = "server")]
#![allow(clippy::unusual_byte_groupings)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nucleus::embedded::Database;
use nucleus::types::Value;

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
}

/// Current resident set size in bytes from /proc/self/status (Linux). Returns
/// 0 on platforms without /proc (e.g. macOS); the leak check treats 0 as
/// "RSS unavailable" and is a no-op there. CI/production runs on Linux, where
/// this is true current RSS and the leak gate is meaningful.
fn rss_bytes() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:")
                && let Some(kb) = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
            {
                return kb * 1024;
            }
        }
    }
    0
}

struct Shared {
    ops: AtomicU64,
    errors: AtomicU64,
    stop: AtomicBool,
    err_samples: std::sync::Mutex<Vec<String>>,
}

impl Shared {
    fn record(&self, r: Result<(), String>) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = r {
            self.errors.fetch_add(1, Ordering::Relaxed);
            let mut s = self.err_samples.lock().unwrap();
            if s.len() < 25 {
                s.push(e);
            }
        }
    }
}

/// One statement, mapping the DB error to a short string for the sample log.
async fn run(db: &Database, sql: &str) -> Result<(), String> {
    db.execute(sql)
        .await
        .map(|_| ())
        .map_err(|e| format!("{sql} -> {e}"))
}

const CAP: usize = 160;
const DIM: usize = 4;

fn vlit(r: &mut Rng) -> String {
    let body: Vec<String> = (0..DIM)
        .map(|_| format!("{:.2}", (r.below(2000) as f64) / 100.0 - 10.0))
        .collect();
    format!("VECTOR('[{}]')", body.join(","))
}

async fn worker(db: Arc<Database>, shared: Arc<Shared>, id: usize, seed: u64, deadline: Instant) {
    let base: i64 = (id as i64) * 100_000_000;
    let mut counter: i64 = 0;
    let mut live: VecDeque<i64> = VecDeque::new();
    let mut rng = Rng(seed.wrapping_add(id as u64).wrapping_mul(0x100000001b3) | 1);

    while Instant::now() < deadline && !shared.stop.load(Ordering::Relaxed) {
        let choice = rng.below(100);
        if live.len() < CAP / 2 || choice < 42 {
            // INSERT a fresh row with a globally-unique id and code.
            let rid = base + counter;
            counter += 1;
            let val = rng.below(64) as i64;
            let sql = format!(
                "INSERT INTO soak (id, val, code, v) VALUES ({rid}, {val}, 'k{rid}', {})",
                vlit(&mut rng)
            );
            let r = run(&db, &sql).await;
            if r.is_ok() {
                live.push_back(rid);
            }
            shared.record(r);
        } else if choice < 62 {
            // UPDATE a live row's indexed columns.
            let rid = live[rng.below(live.len())];
            let val = rng.below(64) as i64;
            let sql = format!(
                "UPDATE soak SET val = {val}, v = {} WHERE id = {rid}",
                vlit(&mut rng)
            );
            shared.record(run(&db, &sql).await);
        } else if choice < 80 {
            // SELECT via the indexes.
            let rid = live[rng.below(live.len())];
            let target = rng.below(64) as i64;
            let q = if choice.is_multiple_of(2) {
                format!("SELECT id FROM soak WHERE id = {rid}")
            } else {
                format!("SELECT id FROM soak WHERE val = {target}")
            };
            shared.record(
                db.query(&q)
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("{q} -> {e}")),
            );
        } else if choice < 90 {
            // KV op — a different model sharing the same engine.
            let k = base + rng.below(256) as i64;
            let sql = format!("SELECT KV_SET('sk{k}', 'v{counter}')");
            shared.record(run(&db, &sql).await);
        } else if !live.is_empty() {
            // DELETE the oldest — this is the position-shifting op.
            let rid = live.pop_front().unwrap();
            shared.record(run(&db, &format!("DELETE FROM soak WHERE id = {rid}")).await);
        }

        // Keep the working set bounded so RSS growth means a leak, not data.
        while live.len() > CAP {
            let rid = live.pop_front().unwrap();
            shared.record(run(&db, &format!("DELETE FROM soak WHERE id = {rid}")).await);
        }
    }
}

async fn create_schema(db: &Database) -> Result<(), String> {
    run(
        db,
        "CREATE TABLE soak (id BIGINT PRIMARY KEY, val INT, code TEXT, v VECTOR(4))",
    )
    .await?;
    run(db, "CREATE INDEX soak_val ON soak (val)").await?;
    run(db, "CREATE INDEX soak_v ON soak USING hnsw (v)").await?;
    run(db, "CREATE INDEX soak_code ON soak USING encrypted (code)").await?;
    Ok(())
}

/// Post-soak / post-recovery invariant checks. Returns the failure list.
async fn coherence_failures(db: &Database) -> Vec<String> {
    let mut fails = Vec::new();

    // PK uniqueness: no id may appear twice (the create_index dup bug class).
    match db
        .query("SELECT id FROM soak GROUP BY id HAVING COUNT(*) > 1 LIMIT 5")
        .await
    {
        Ok(rows) if !rows.is_empty() => {
            fails.push(format!(
                "{} duplicate PK id(s) present after churn",
                rows.len()
            ));
        }
        Err(e) => fails.push(format!("pk-uniqueness query failed: {e}")),
        _ => {}
    }

    // Sample live rows: PK equality returns exactly one, encrypted code exactly one.
    let sample = db
        .query("SELECT id FROM soak LIMIT 40")
        .await
        .unwrap_or_default();
    for row in &sample {
        let Some(Value::Int64(rid)) = row.first() else {
            continue;
        };
        let rid = *rid;
        match db
            .query(&format!("SELECT id FROM soak WHERE id = {rid}"))
            .await
        {
            Ok(rows) if rows.len() != 1 => {
                fails.push(format!(
                    "pk id={rid} returned {} rows (expected 1)",
                    rows.len()
                ));
            }
            Err(e) => fails.push(format!("pk id={rid} query failed: {e}")),
            _ => {}
        }
        let enc = db
            .query_one(&format!(
                "SELECT ENCRYPTED_LOOKUP('soak_code', 'k{rid}') FROM soak LIMIT 1"
            ))
            .await;
        if let Ok(Some(Value::Text(s))) = enc {
            let n = s.split(',').filter(|p| !p.trim().is_empty()).count();
            if n != 1 {
                fails.push(format!(
                    "encrypted lookup k{rid}: {n} postings (expected 1)"
                ));
            }
        }
        if fails.len() > 10 {
            break;
        }
    }
    fails
}

async fn row_count(db: &Database) -> i64 {
    match db.query_one("SELECT COUNT(*) FROM soak").await {
        Ok(Some(Value::Int64(n))) => n,
        Ok(Some(Value::Int32(n))) => n as i64,
        _ => -1,
    }
}

fn parse_args() -> (u64, usize, u64, u64, u64) {
    let args: Vec<String> = std::env::args().collect();
    let mut duration_secs = 20u64;
    let mut concurrency = 8usize;
    let mut seed = 0x50AC_BEEF_1234u64;
    let mut leak_limit_mb = 96u64;
    let mut rows_target = 0u64;
    let mut i = 1;
    while i < args.len() {
        let take = |i: &mut usize| -> Option<String> {
            *i += 1;
            args.get(*i).cloned()
        };
        match args[i].as_str() {
            "--duration-secs" => {
                if let Some(v) = take(&mut i).and_then(|s| s.parse().ok()) {
                    duration_secs = v;
                }
            }
            "--concurrency" => {
                if let Some(v) = take(&mut i).and_then(|s| s.parse().ok()) {
                    concurrency = v;
                }
            }
            "--seed" => {
                if let Some(v) = take(&mut i).and_then(|s| s.parse().ok()) {
                    seed = v;
                }
            }
            "--leak-limit-mb" => {
                if let Some(v) = take(&mut i).and_then(|s| s.parse().ok()) {
                    leak_limit_mb = v;
                }
            }
            // Scale mode: bulk-load a separate `big` table to this many rows
            // BEFORE the churn phase, then keep every invariant (bounded RSS,
            // coherence, durability) with multi-million-row data resident.
            "--rows-target" => {
                if let Some(v) = take(&mut i).and_then(|s| s.parse().ok()) {
                    rows_target = v;
                }
            }
            _ => {}
        }
        i += 1;
    }
    (seed, concurrency, duration_secs, leak_limit_mb, rows_target)
}

fn main() {
    unsafe {
        std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
    }
    let (seed, concurrency, duration_secs, leak_limit_mb, rows_target) = parse_args();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((concurrency + 2).min(16))
        .enable_all()
        .build()
        .unwrap();
    let failed = rt.block_on(run_soak(
        seed,
        concurrency,
        duration_secs,
        leak_limit_mb,
        rows_target,
    ));
    if failed {
        std::process::exit(1);
    }
}

async fn run_soak(
    seed: u64,
    concurrency: usize,
    duration_secs: u64,
    leak_limit_mb: u64,
    rows_target: u64,
) -> bool {
    let dir = std::env::temp_dir().join(format!("nucleus_soak_{seed:x}"));
    let _ = std::fs::remove_dir_all(&dir);
    println!(
        "probe_soak: dir={} concurrency={concurrency} duration={duration_secs}s leak_limit={leak_limit_mb}MB",
        dir.display()
    );

    let db = Arc::new(Database::durable_mvcc(&dir).expect("open durable mvcc"));
    if let Err(e) = create_schema(&db).await {
        println!("FAIL: schema setup: {e}");
        return true;
    }

    // ── Scale phase: grow `big` to rows_target before churn ──────────────
    let mut big_expected: i64 = 0;
    if rows_target > 0 {
        if let Err(e) = run(
            &db,
            "CREATE TABLE big (id BIGINT PRIMARY KEY, grp INT, payload TEXT)",
        )
        .await
        {
            println!("FAIL: big-table setup: {e}");
            return true;
        }
        if let Err(e) = run(&db, "CREATE INDEX big_grp ON big (grp)").await {
            println!("FAIL: big-table index: {e}");
            return true;
        }
        let t0 = Instant::now();
        const BATCH: u64 = 1000;
        // Load with synchronous_commit=off: batches stay autocommit (an
        // explicit transaction would force the in-txn seq-scan unique check —
        // O(n) per insert on a growing table) while the WAL force is deferred
        // to the explicit sync below, exactly the bulk-load posture the docs
        // recommend. Durability of the load is still asserted: sync() runs
        // before the count check and again before the reopen check.
        let _ = run(&db, "SET synchronous_commit = off").await;
        let mut loaded = 0u64;
        while loaded < rows_target {
            let n = BATCH.min(rows_target - loaded);
            let mut sql = String::with_capacity(64 * n as usize);
            sql.push_str("INSERT INTO big VALUES ");
            for k in 0..n {
                let id = loaded + k;
                if k > 0 {
                    sql.push(',');
                }
                sql.push_str(&format!("({id}, {}, 'p{id}')", id % 1000));
            }
            if let Err(e) = run(&db, &sql).await {
                println!("FAIL: bulk load at row {loaded}: {e}");
                return true;
            }
            loaded += n;
            if loaded.is_multiple_of(1_000_000) {
                println!(
                    "  bulk load: {loaded}/{rows_target} rows  ({:.0} rows/s, RSS {} MB)",
                    loaded as f64 / t0.elapsed().as_secs_f64().max(0.001),
                    rss_bytes() / (1024 * 1024)
                );
            }
        }
        let _ = run(&db, "SET synchronous_commit = on").await;
        let _ = db.sync();
        big_expected = rows_target as i64;
        let counted = match db.query_one("SELECT COUNT(*) FROM big").await {
            Ok(Some(Value::Int64(n))) => n,
            Ok(Some(Value::Int32(n))) => n as i64,
            _ => -1,
        };
        println!(
            "  bulk load done: {rows_target} rows in {:.1}s (COUNT(*)={counted}, RSS {} MB)",
            t0.elapsed().as_secs_f64(),
            rss_bytes() / (1024 * 1024)
        );
        if counted != big_expected {
            println!("FAIL: bulk load count mismatch: expected {big_expected}, got {counted}");
            return true;
        }
    }

    let shared = Arc::new(Shared {
        ops: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        stop: AtomicBool::new(false),
        err_samples: std::sync::Mutex::new(Vec::new()),
    });

    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration_secs);

    // RSS sampler.
    let sampler_stop = shared.clone();
    let sampler = tokio::spawn(async move {
        let mut series: Vec<(f64, u64)> = Vec::new();
        while Instant::now() < deadline && !sampler_stop.stop.load(Ordering::Relaxed) {
            series.push((start.elapsed().as_secs_f64(), rss_bytes()));
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        series
    });

    let mut handles = Vec::new();
    for w in 0..concurrency {
        let db = db.clone();
        let sh = shared.clone();
        handles.push(tokio::spawn(worker(db, sh, w, seed, deadline)));
    }
    for h in handles {
        let _ = h.await;
    }
    shared.stop.store(true, Ordering::Relaxed);
    let series = sampler.await.unwrap_or_default();

    let ops = shared.ops.load(Ordering::Relaxed);
    let errors = shared.errors.load(Ordering::Relaxed);
    let rows_before = row_count(&db).await;
    if big_expected > 0 {
        match db.query_one("SELECT COUNT(*) FROM big").await {
            Ok(Some(v)) => println!("  scale count before close: {v:?}"),
            Ok(None) => println!("  scale count before close: none"),
            Err(e) => println!("  scale count ERROR before close: {e}"),
        }
    }

    // ---- crash-recovery: close and reopen the durable DB ----
    let _ = db.sync();
    drop(db);
    let db2 = match Database::durable_mvcc(&dir) {
        Ok(d) => d,
        Err(e) => {
            println!("FAIL: reopen after close: {e}");
            return true;
        }
    };
    let rows_after = row_count(&db2).await;

    // ---- evaluate invariants ----
    let mut failed = false;

    // 1. error budget (benign MVCC conflicts tolerated; a storm is not).
    let err_rate = if ops > 0 {
        errors as f64 / ops as f64
    } else {
        0.0
    };
    if err_rate > 0.02 {
        failed = true;
        println!(
            "FAIL: error rate {:.3}% exceeds 2% ({errors}/{ops})",
            err_rate * 100.0
        );
        for s in shared.err_samples.lock().unwrap().iter().take(8) {
            println!("      err: {s}");
        }
    }

    // 2. leak detection over the post-warmup window.
    let leak = analyze_leak(&series, duration_secs, leak_limit_mb);
    if let Some(msg) = &leak.failure {
        failed = true;
        println!("FAIL: {msg}");
    }

    // 3. coherence after sustained churn.
    let churn_fails = coherence_failures(&db2).await;
    if !churn_fails.is_empty() {
        failed = true;
        println!("FAIL: post-soak coherence:");
        for f in &churn_fails {
            println!("      {f}");
        }
    }

    // 4. durability: committed rows survive reopen and stay coherent.
    if rows_before >= 0 && rows_after != rows_before {
        failed = true;
        println!("FAIL: durability: {rows_before} rows before close, {rows_after} after reopen");
    }

    // 4b. scale table: the churn phase never touches `big`, so its count must
    // be exactly rows_target after the run AND after reopen, and an indexed
    // group lookup must return its slice.
    if big_expected > 0 {
        let big_after = match db2.query_one("SELECT COUNT(*) FROM big").await {
            Ok(Some(Value::Int64(n))) => n,
            Ok(Some(Value::Int32(n))) => n as i64,
            Ok(other) => {
                println!("  scale count returned unexpected value: {other:?}");
                -1
            }
            Err(e) => {
                println!("  scale count ERROR after reopen: {e}");
                -1
            }
        };
        if big_after != big_expected {
            failed = true;
            println!("FAIL: scale table after reopen: expected {big_expected} rows, got {big_after}");
        }
        let grp = match db2
            .query_one("SELECT COUNT(*) FROM big WHERE grp = 7")
            .await
        {
            Ok(Some(Value::Int64(n))) => n,
            Ok(Some(Value::Int32(n))) => n as i64,
            _ => -1,
        };
        let expect_grp = big_expected / 1000 + i64::from(big_expected % 1000 > 7);
        if grp != expect_grp {
            failed = true;
            println!("FAIL: scale table indexed group count: expected {expect_grp}, got {grp}");
        }
    }
    let recover_fails = coherence_failures(&db2).await;
    if !recover_fails.is_empty() {
        failed = true;
        println!("FAIL: post-recovery coherence:");
        for f in &recover_fails {
            println!("      {f}");
        }
    }

    // 5. dump/restore at size: a logical dump of the post-churn database must
    // replay into a fresh in-memory instance with identical row counts. Only
    // run in scale mode — this is the "backup works at multi-million rows"
    // gate, and the dump of a tiny churn table adds no signal.
    if big_expected > 0 {
        let t0 = Instant::now();
        match db2.executor().dump_logical().await {
            Ok(script) => {
                println!(
                    "  dump: {:.1} MB in {:.1}s",
                    script.len() as f64 / (1024.0 * 1024.0),
                    t0.elapsed().as_secs_f64()
                );
                let mem = Database::mvcc();
                let t1 = Instant::now();
                match mem.executor().restore_logical(&script).await {
                    Ok(()) => {
                        let n = match mem.query_one("SELECT COUNT(*) FROM big").await {
                            Ok(Some(Value::Int64(n))) => n,
                            Ok(Some(Value::Int32(n))) => n as i64,
                            _ => -1,
                        };
                        println!("  restore: {:.1}s", t1.elapsed().as_secs_f64());
                        if n != big_expected {
                            failed = true;
                            println!(
                                "FAIL: dump/restore row count: expected {big_expected}, got {n}"
                            );
                        }
                    }
                    Err(e) => {
                        failed = true;
                        println!("FAIL: restore_logical: {e}");
                    }
                }
            }
            Err(e) => {
                failed = true;
                println!("FAIL: dump_logical: {e}");
            }
        }
    }

    let _ = std::fs::remove_dir_all(&dir);

    println!("\n════ SOAK SUMMARY ════");
    println!("duration        : {duration_secs}s   concurrency: {concurrency}");
    println!(
        "ops             : {ops}   errors: {errors} ({:.3}%)",
        err_rate * 100.0
    );
    println!(
        "throughput      : {:.0} ops/s",
        ops as f64 / duration_secs.max(1) as f64
    );
    println!("rows before/after reopen: {rows_before} / {rows_after}");
    if big_expected > 0 {
        println!("scale table     : {big_expected} rows (bulk-loaded, verified after reopen)");
    }
    println!(
        "RSS start/peak/end: {} / {} / {} MB",
        leak.first_mb, leak.peak_mb, leak.last_mb
    );
    println!(
        "RSS post-warmup growth : {} MB (limit {leak_limit_mb})",
        leak.growth_mb
    );
    if failed {
        println!("\nSOAK FAILED");
    } else {
        println!("\nSoak passed: bounded memory, coherent under churn, durable across reopen. ✅");
    }
    failed
}

struct LeakReport {
    first_mb: u64,
    last_mb: u64,
    peak_mb: u64,
    growth_mb: i64,
    failure: Option<String>,
}

/// Compare RSS in the post-warmup early window vs the final window. A bounded
/// workload should plateau; sustained growth beyond `limit_mb` is a leak.
fn analyze_leak(series: &[(f64, u64)], duration_secs: u64, limit_mb: u64) -> LeakReport {
    let mb = |b: u64| b / (1024 * 1024);
    if series.len() < 4 {
        return LeakReport {
            first_mb: series.first().map(|s| mb(s.1)).unwrap_or(0),
            last_mb: series.last().map(|s| mb(s.1)).unwrap_or(0),
            peak_mb: series.iter().map(|s| mb(s.1)).max().unwrap_or(0),
            growth_mb: 0,
            failure: None,
        };
    }
    let warmup = duration_secs as f64 * 0.25;
    let post: Vec<u64> = series
        .iter()
        .filter(|(t, _)| *t >= warmup)
        .map(|(_, r)| *r)
        .collect();
    let post = if post.len() >= 4 {
        post
    } else {
        series.iter().map(|(_, r)| *r).collect()
    };
    let q = post.len() / 4;
    let early = post[..q.max(1)].iter().copied().max().unwrap_or(0);
    let late = post[post.len() - q.max(1)..]
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    let growth_mb = mb(late.saturating_sub(early)) as i64;
    let failure = if growth_mb > limit_mb as i64 {
        Some(format!(
            "RSS grew {growth_mb} MB post-warmup (early {} MB -> late {} MB), exceeds {limit_mb} MB leak limit",
            mb(early),
            mb(late)
        ))
    } else {
        None
    };
    LeakReport {
        first_mb: mb(series.first().unwrap().1),
        last_mb: mb(series.last().unwrap().1),
        peak_mb: series.iter().map(|s| mb(s.1)).max().unwrap_or(0),
        growth_mb,
        failure,
    }
}
