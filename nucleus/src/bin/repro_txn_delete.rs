//! Temporary repro: DELETE inside an explicit transaction on the paged engine.
//!
//! 2,000 single-row DELETEs in one transaction on a 20k-row table ran 20
//! minutes; the same deletes in autocommit take ~10 s. This times increasing
//! delete counts to see whether the per-delete cost grows with the number of
//! deletes already done in the transaction (i.e. quadratic).

#![cfg(feature = "server")]

use std::sync::Arc;
use std::time::Instant;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{DiskEngine, StorageEngine, buffered_engine::BufferedDiskEngine};

async fn build(dir: &std::path::Path, rows: usize) -> Executor {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(DiskEngine::open(&dir.join("r.db"), catalog.clone()).unwrap());
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    let ex = Executor::new(catalog, storage);
    ex.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)")
        .await
        .unwrap();
    let mut id = 1usize;
    while id <= rows {
        let end = (id + 499).min(rows);
        let mut sql = String::from("INSERT INTO t VALUES ");
        for i in id..=end {
            if i > id {
                sql.push(',');
            }
            sql.push_str(&format!("({i},{i})"));
        }
        ex.execute(&sql).await.unwrap();
        id = end + 1;
    }
    ex
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows: usize = args
        .iter()
        .position(|a| a == "--rows")
        .map(|i| args[i + 1].parse().unwrap())
        .unwrap_or(20_000);
    let txn = !args.iter().any(|a| a == "--autocommit");

    let dir = std::env::temp_dir().join(format!("nucleus-repro-{}", std::process::id()));
    println!(
        "rows={rows} mode={}",
        if txn { "txn" } else { "autocommit" }
    );

    for &n in &[50usize, 400, 1000, 2000] {
        let ex = build(&dir, rows).await;
        if txn {
            ex.execute("BEGIN").await.unwrap();
        }
        nucleus::bench_hooks::reset_overlay_counters();
        let t = Instant::now();
        for i in 1..=n {
            ex.execute(&format!("DELETE FROM t WHERE id = {i}"))
                .await
                .unwrap();
        }
        let per = t.elapsed().as_micros() as f64 / n as f64;
        let (calls, orows) = nucleus::bench_hooks::overlay_counters();
        let sites: Vec<String> = nucleus::bench_hooks::OVERLAY_SITES
            .iter()
            .zip(calls.iter())
            .filter(|(_, c)| **c > 0)
            .map(|(name, c)| format!("{name}={c}"))
            .collect();
        println!(
            "  {n:>4} deletes: {:>8} ms  {per:>8.0} us/delete   overlay {:.1}/stmt [{}]  {} rows materialised",
            t.elapsed().as_millis(),
            calls.iter().sum::<u64>() as f64 / n as f64,
            sites.join(" "),
            orows
        );
        if txn {
            let c = Instant::now();
            ex.execute("COMMIT").await.unwrap();
            println!("       commit: {:>8} ms", c.elapsed().as_millis());
        }
        drop(ex);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
