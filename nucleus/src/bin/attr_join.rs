//! attr_join — split a 2-table join's cost between its parts.
//!
//! The join in `compete` runs at ~97 us against SQLite's ~11 us. Before that
//! gap gets attributed to "the join", this measures where the time actually
//! goes, by running the whole query and each of its pieces in the same process
//! and subtracting:
//!
//!   full    outer scan + index probes + row assembly + projection, with the
//!           RESULT CACHE invalidated first — repeating one SELECT otherwise
//!           measures a 30-second-TTL cache hit, not the query. That trap cost
//!           an entire attribution pass: every arm read ~9 us and the join
//!           looked faster than SQLite.
//!   cached  the same query WITHOUT invalidating, i.e. the cache hit itself,
//!           kept as an arm so the difference is visible rather than lurking
//!   prep    the same query through prepare/execute_prepared, which is what
//!           `compete` measures — kept as its own arm because the two differed
//!           by 14x and only one of them is what the benchmark reports
//!   outer   outer scan only        (SELECT ... FROM orders WHERE id < N)
//!   probes  index probes only      (raw storage index_lookup, no SQL)
//!   plan    parse + plan only      (EXPLAIN, so nothing executes)
//!
//!   full - outer - probes = row assembly, projection and executor overhead
//!
//! The point is to distinguish an algorithmic problem from a per-row constant.
//! An index nested-loop that reads exactly the rows it returns is already
//! algorithmically right; if the remainder is in assembly, the fix is the
//! materialisation model (every stage builds `Vec<Row>` of owned `Value`s),
//! not the join.
//!
//! Arms are interleaved inside one process and rotated per round, because this
//! machine drifts far enough between batches to invent a 40% win — the same
//! discipline `attr_pk_write` uses. See `docs/BENCH_VS_POSTGRES.md`.
//!
//! Build:
//!   cargo run --release --features server --bin attr_join -- [--rows N] [--rounds N] [--sel N]

#![cfg(feature = "server")]

use std::sync::Arc;
use std::time::Instant;

use nucleus::bench_hooks;
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

/// Rows returned by a SELECT, so an arm that silently returns nothing cannot
/// be mistaken for a fast one.
fn row_count(r: &ExecResult) -> usize {
    match r {
        ExecResult::Select { rows, .. } => rows.len(),
        _ => 0,
    }
}

/// Returns the executor plus the storage handle, because `Executor::storage_for`
/// is private and this harness must probe the index directly to time it apart
/// from the SQL layer.
async fn build(rows: usize) -> (Arc<Executor>, Arc<dyn StorageEngine>) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, Arc::clone(&storage)));

    // Schema copied from `compete`, deliberately: `amount` is DECIMAL there,
    // not FLOAT, and orders is 5x users. Matching an approximation of the
    // benchmark instead of the benchmark is how an attribution harness ends up
    // explaining a cost the real query does not have.
    ex.execute(
        "CREATE TABLE bench_users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT NOT NULL, city TEXT NOT NULL)",
    )
    .await
    .expect("create users");
    ex.execute(
        "CREATE TABLE bench_orders (id INT PRIMARY KEY, user_id INT NOT NULL, amount DECIMAL(10,2) NOT NULL, status TEXT NOT NULL)",
    )
    .await
    .expect("create orders");

    let chunk = 1000;
    let mut id = 1;
    while id <= rows {
        let end = (id + chunk - 1).min(rows);
        let mut u = String::from("INSERT INTO bench_users VALUES ");
        for i in id..=end {
            if i > id {
                u.push(',');
            }
            let city = [
                "NYC", "LA", "CHI", "HOU", "PHX", "PHI", "SAN", "DEN", "BOS", "SEA",
            ][i % 10];
            u.push_str(&format!("({i},'user{i}',{},'{city}')", 20 + (i % 50)));
        }
        ex.execute(&u).await.expect("insert users");
        id = end + 1;
    }
    // 5x users, as in `compete`.
    let orders = rows * 5;
    let mut id = 1;
    while id <= orders {
        let end = (id + chunk - 1).min(orders);
        let mut o = String::from("INSERT INTO bench_orders VALUES ");
        for i in id..=end {
            if i > id {
                o.push(',');
            }
            let status = match i % 3 {
                0 => "shipped",
                1 => "pending",
                _ => "cancelled",
            };
            o.push_str(&format!(
                "({i},{},{}.50,'{status}')",
                (i % rows) + 1,
                10 + (i % 500)
            ));
        }
        ex.execute(&o).await.expect("insert orders");
        id = end + 1;
    }
    (ex, storage)
}

fn summary(name: &str, samples: &[u128]) {
    let mut s: Vec<u128> = samples.to_vec();
    s.sort_unstable();
    let median = s[s.len() / 2];
    let mean = s.iter().sum::<u128>() / s.len() as u128;
    let all: Vec<String> = samples.iter().map(|v| v.to_string()).collect();
    println!(
        "  {name:<8} median {median:>8} us   mean {mean:>8} us   [{}]",
        all.join(" ")
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut rows = 50_000usize;
    let mut rounds = 7usize;
    let mut sel = 100usize;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" => {
                i += 1;
                rows = args[i].parse().unwrap();
            }
            "--rounds" => {
                i += 1;
                rounds = args[i].parse().unwrap();
            }
            "--sel" => {
                i += 1;
                sel = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }

    println!("attr_join: {rows} rows/table, selectivity {sel}, {rounds} rounds\n");
    let (ex, storage) = build(rows).await;

    // Exactly the shape `compete` measures.
    let full_sql = format!(
        "SELECT u.name, o.amount FROM bench_users u, bench_orders o \
         WHERE u.id = o.user_id AND o.id < {sel}"
    );
    let outer_sql = format!("SELECT id, user_id, amount FROM bench_orders WHERE id < {sel}");
    let explain_sql = format!("EXPLAIN {full_sql}");

    // The keys the join probes, collected once so the `probes` arm measures
    // only the lookups and not the scan that produced them.
    let keys: Vec<Value> = {
        let r = ex.execute(&outer_sql).await.expect("outer");
        match &r[0] {
            ExecResult::Select { rows, .. } => rows.iter().map(|row| row[1].clone()).collect(),
            _ => Vec::new(),
        }
    };
    // Verified against the engine below rather than assumed: a probe arm that
    // silently looks up a non-existent index would time as very fast.
    let pk_index = "bench_users_pkey".to_string();

    {
        let probe = keys.first().expect("outer produced no keys");
        let hit = storage
            .index_lookup("bench_users", &pk_index, probe)
            .await
            .expect("index lookup");
        assert!(
            hit.is_some_and(|r| !r.is_empty()),
            "index '{pk_index}' did not answer — the probe arm would be timing nothing"
        );
    }

    // Warm-up, discarded: the first execution pays for plan-cache population.
    let warm = ex.execute(&full_sql).await.expect("warm");
    let expect_rows = row_count(&warm[0]);
    println!("  warm-up: {expect_rows} rows (discarded)\n");
    assert!(expect_rows > 0, "fixture produced an empty join");

    let handle = ex.prepare(&full_sql).expect("prepare");
    let _ = ex.execute_prepared(&handle, &[]).await;

    let (mut full, mut outer, mut probes, mut plan, mut prep, mut cached) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    for round in 0..rounds {
        // Rotate so no arm always runs on a cold cache.
        let mut order = [0usize, 1, 2, 3, 4, 5];
        order.rotate_left(round % 6);
        for arm in order {
            // Outside the timer: the result cache would otherwise answer every
            // repeat of an identical SELECT.
            if matches!(arm, 0 | 1 | 4) {
                ex.query_cache_invalidate_all();
            }
            let t = Instant::now();
            match arm {
                0 => {
                    let r = ex.execute(&full_sql).await.expect("full");
                    assert_eq!(row_count(&r[0]), expect_rows, "full arm changed its answer");
                }
                1 => {
                    let r = ex.execute(&outer_sql).await.expect("outer");
                    assert!(row_count(&r[0]) > 0, "outer arm returned nothing");
                }
                5 => {
                    let r = ex.execute(&full_sql).await.expect("cached");
                    assert_eq!(
                        row_count(&r[0]),
                        expect_rows,
                        "cached arm changed its answer"
                    );
                }
                2 => {
                    let mut hits = 0usize;
                    for k in &keys {
                        if let Ok(Some(rows)) =
                            storage.index_lookup("bench_users", &pk_index, k).await
                        {
                            hits += rows.len();
                        }
                    }
                    assert!(hits > 0, "probe arm matched nothing");
                }
                3 => {
                    ex.execute(&explain_sql).await.expect("explain");
                }
                _ => {
                    let r = ex.execute_prepared(&handle, &[]).await.expect("prepared");
                    assert_eq!(
                        row_count(&r),
                        expect_rows,
                        "prepared arm changed its answer"
                    );
                }
            }
            let us = t.elapsed().as_micros();
            match arm {
                0 => full.push(us),
                1 => outer.push(us),
                2 => probes.push(us),
                3 => plan.push(us),
                5 => cached.push(us),
                _ => prep.push(us),
            }
        }
    }

    // Which plan-cache outcome each SQL arm actually took. A 9 us "full" that
    // reused a plan and an 82 us one that re-planned are the same query on two
    // different code paths, and the latency alone does not say which.
    let between_sql = format!(
        "SELECT id, user_id, amount FROM bench_orders WHERE id BETWEEN 1 AND {}",
        sel - 1
    );
    for (label, sql) in [
        ("full", &full_sql),
        ("outer", &outer_sql),
        ("between", &between_sql),
    ] {
        bench_hooks::reset_plan_counters();
        let idx_before = ex.metrics().index_scan_served.get();
        let scanned_before = ex.metrics().rows_scanned.get();
        let t = Instant::now();
        for _ in 0..100 {
            // The result cache answers an identical repeat, so it has to go
            // here too — without this the counters describe 1 real execution
            // and 99 cache hits.
            ex.query_cache_invalidate_all();
            ex.execute(sql).await.expect("counted run");
        }
        let per = t.elapsed().as_micros() as f64 / 100.0;
        let idx = ex.metrics().index_scan_served.get() - idx_before;
        let scanned = ex.metrics().rows_scanned.get() - scanned_before;
        let c = bench_hooks::plan_counters();
        let parts: Vec<String> = bench_hooks::PLAN_SITES
            .iter()
            .zip(c.iter())
            .filter(|(_, n)| **n > 0)
            .map(|(s, n)| format!("{s}={n}"))
            .collect();
        println!(
            "  {label:<8} {per:>6.1} us/call  {:<32} index_scans={idx:<5} rows_scanned={scanned}",
            parts.join(" ")
        );
    }
    {
        bench_hooks::reset_plan_counters();
        for _ in 0..100 {
            ex.execute_prepared(&handle, &[]).await.expect("counted");
        }
        let c = bench_hooks::plan_counters();
        let parts: Vec<String> = bench_hooks::PLAN_SITES
            .iter()
            .zip(c.iter())
            .filter(|(_, n)| **n > 0)
            .map(|(s, n)| format!("{s}={n}"))
            .collect();
        println!("  plan outcome prep  : {}", parts.join(" "));
    }

    println!();
    summary("full", &full);
    summary("prepared", &prep);
    summary("cached", &cached);
    summary("outer", &outer);
    summary("probes", &probes);
    summary("plan", &plan);

    let med = |v: &Vec<u128>| {
        let mut s = v.clone();
        s.sort_unstable();
        s[s.len() / 2] as f64
    };
    let (f, o, p) = (med(&full), med(&outer), med(&probes));
    let rest = f - o - p;
    println!("\n  full {f:.0} us = outer {o:.0} + probes {p:.0} + assembly/projection {rest:.0}");
    println!(
        "  per returned row ({expect_rows}): full {:.2} us, assembly {:.2} us",
        f / expect_rows as f64,
        rest / expect_rows as f64
    );
}
