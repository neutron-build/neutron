//! attr_delete — is `delete_leaf_entry` worth fixing?
//!
//! `delete_leaf_entry` has the same decode-and-rewrite shape that made
//! `try_insert_leaf` cost ~37 us/row: `collect_leaf_entries` allocates a
//! `Vec<u8>` per entry already on the page (~200 on a 4 KiB leaf), each
//! surviving entry is cloned again, and the whole page is rewritten. Fixing
//! the insert side in place measured -59% to -70%.
//!
//! It was left alone because it looked unmeasurable: an autocommit DELETE is
//! fsync-bound, so the leaf work hides under a 4 ms drive barrier. That is no
//! longer the only shape available — deletes inside one transaction apply
//! their index maintenance in a burst at COMMIT, with one fsync for the lot.
//!
//! So this measures the size of the prize BEFORE anything is optimised, the
//! same way `attr_pk_write` did, by deleting the maintenance and subtracting:
//!
//!   full     row removal + B-tree leaf maintenance
//!   noidx    row removal                            (maintenance deleted)
//!
//!   full - noidx = what `delete_leaf_entry` costs
//!
//! `noidx` leaves the index pointing at rows that no longer exist, so that arm
//! is not a correct database — it exists only to be subtracted.
//!
//! Arms are interleaved inside one process and the order rotates per round,
//! because this machine drifts far enough between batches to invent a 40% win.
//!
//! Build:
//!   cargo run --release --features server --bin attr_delete -- [--rows N] [--deletes N] [--rounds N]

#![cfg(feature = "server")]

use std::sync::Arc;
use std::time::Instant;

use nucleus::bench_hooks;
use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{DiskEngine, StorageEngine, buffered_engine::BufferedDiskEngine};

/// One timed run: load `rows`, then delete `deletes` of them inside a single
/// transaction. Only the DELETE transaction is timed.
async fn run_delete(
    dir: &std::path::Path,
    rows: usize,
    deletes: usize,
    skip_idx: bool,
    txn: bool,
) -> u128 {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("create bench dir");

    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(
        DiskEngine::open(&dir.join("bench.db"), catalog.clone()).expect("open disk engine"),
    );
    // Exactly what main.rs builds for a server.
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    let ex = Executor::new(catalog, storage);

    ex.execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT NOT NULL, s TEXT NOT NULL)")
        .await
        .expect("create table");

    let chunk = 500;
    let mut id = 1;
    while id <= rows {
        let end = (id + chunk - 1).min(rows);
        let mut sql = String::from("INSERT INTO t VALUES ");
        for i in id..=end {
            if i > id {
                sql.push(',');
            }
            sql.push_str(&format!("({i},{},'row{i}')", i * 7));
        }
        ex.execute(&sql).await.expect("insert");
        id = end + 1;
    }

    // Spread the deleted keys across the index rather than taking a prefix, so
    // the leaves touched are not all the same page.
    let stride = (rows / deletes).max(1);

    bench_hooks::set_skip_index_delete(skip_idx);
    let scanned_before = ex.metrics().rows_scanned.get();
    let t = Instant::now();
    if txn {
        ex.execute("BEGIN").await.expect("begin");
    }
    bench_hooks::reset_overlay_counters();
    let stmt_start = Instant::now();
    for k in 0..deletes {
        let key = 1 + k * stride;
        ex.execute(&format!("DELETE FROM t WHERE id = {key}"))
            .await
            .expect("delete");
    }
    let stmt_us = stmt_start.elapsed().as_micros();
    let commit_start = Instant::now();
    if txn {
        ex.execute("COMMIT").await.expect("commit");
    }
    let commit_us = commit_start.elapsed().as_micros();
    // Per-statement work and commit work are different problems with different
    // fixes; the total cannot tell them apart.
    let (ov_calls, ov_rows) = bench_hooks::overlay_counters();
    let ov: Vec<String> = bench_hooks::OVERLAY_SITES
        .iter()
        .zip(ov_calls.iter())
        .filter(|(_, n)| **n > 0)
        .map(|(s, n)| format!("{s}={n}"))
        .collect();
    println!(
        "        statements {stmt_us} us ({:.0} us/delete) + commit {commit_us} us",
        stmt_us as f64 / deletes as f64
    );
    // Rebuilding the buffered view is O(table); doing it per statement is the
    // shape that made this path quadratic before. Count it, do not assume.
    println!(
        "        overlay rebuilds: {}  rows materialised={ov_rows} ({:.0} rows per delete)",
        if ov.is_empty() {
            "none".into()
        } else {
            ov.join(" ")
        },
        ov_rows as f64 / deletes as f64
    );
    let elapsed = t.elapsed().as_micros();
    let scanned = ex.metrics().rows_scanned.get() - scanned_before;
    // Rows READ to perform `deletes` deletions by primary key. If this is not
    // ~= `deletes`, the DELETE is not using the index and no amount of B-tree
    // leaf tuning matters.
    println!(
        "        rows_scanned={scanned} for {deletes} PK deletes ({:.0} rows read per delete)",
        scanned as f64 / deletes as f64
    );
    bench_hooks::set_skip_index_delete(false);

    drop(ex);
    let _ = std::fs::remove_dir_all(dir);
    elapsed
}

fn summary(name: &str, samples: &[u128], deletes: usize) {
    let mut s: Vec<u128> = samples.to_vec();
    s.sort_unstable();
    let median = s[s.len() / 2];
    let per = median as f64 / deletes as f64;
    let all: Vec<String> = samples.iter().map(|v| (v / 1000).to_string()).collect();
    println!(
        "  {name:<7} median {:>7} ms   {per:>7.1} us/delete   [{} ms]",
        median / 1000,
        all.join(" ")
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut rows = 50_000usize;
    let mut deletes = 2_000usize;
    let mut rounds = 5usize;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" => {
                i += 1;
                rows = args[i].parse().unwrap();
            }
            "--deletes" => {
                i += 1;
                deletes = args[i].parse().unwrap();
            }
            "--rounds" => {
                i += 1;
                rounds = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }

    println!("attr_delete: {rows} rows, {deletes} deletes in one txn, {rounds} rounds\n");
    let dir = std::env::temp_dir().join(format!("nucleus-attr-del-{}", std::process::id()));

    let warm = run_delete(&dir, rows, deletes, false, true).await;
    println!("  warm-up: {} ms (discarded)\n", warm / 1000);

    // Is BEGIN..COMMIT actually batching the durability work? If one
    // transaction costs the same as N autocommits, it is not, and every
    // per-delete number below is really one F_FULLFSYNC per row.
    let in_txn = run_delete(&dir, rows, deletes, false, true).await;
    let auto = run_delete(&dir, rows, deletes, false, false).await;
    println!(
        "  batching check: {deletes} deletes in ONE txn {} ms vs autocommit {} ms ({:.1}x)\n",
        in_txn / 1000,
        auto / 1000,
        auto as f64 / in_txn as f64
    );

    let (mut full, mut noidx) = (Vec::new(), Vec::new());
    for round in 0..rounds {
        // Alternate which arm runs first so neither is always cold.
        let order = if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for skip in order {
            let us = run_delete(&dir, rows, deletes, skip, true).await;
            if skip { &mut noidx } else { &mut full }.push(us);
            let name = if skip { "noidx" } else { "full " };
            println!("  round {round} {name}  {:>7} ms", us / 1000);
        }
    }

    println!();
    summary("full", &full, deletes);
    summary("noidx", &noidx, deletes);

    let med = |v: &Vec<u128>| {
        let mut s = v.clone();
        s.sort_unstable();
        s[s.len() / 2] as f64
    };
    let (f, n) = (med(&full), med(&noidx));
    let delta = f - n;
    println!(
        "\n  B-tree leaf maintenance = {:.0} ms of {:.0} ms  ({:.1} us/delete, {:.0}% of the total)",
        delta / 1000.0,
        f / 1000.0,
        delta / deletes as f64,
        (delta / f) * 100.0
    );
}
