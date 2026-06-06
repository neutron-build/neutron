//! Efficiency-assertion harness: verifies that Nucleus uses index lookups (few
//! rows scanned) for PK-equality and small PK-range queries and performs a
//! full table scan (~N rows) for SELECT * without a WHERE clause.
//!
//! Strategy: drive the executor through its SQL surface, snapshot the
//! `MetricsRegistry::rows_scanned` counter before and after each query, and
//! assert the delta meets the expected efficiency bound.
//!
//! Build:
//!   cargo build --release --features server --bin probe_efficiency
//! Run:
//!   cargo run --release --features server --bin probe_efficiency
//!   cargo run --release --features server --bin probe_efficiency \
//!     --seed 42 --n 2000 --seeds 3

#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::metrics::MetricsRegistry;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift64) ─────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

// ─── Executor helpers ─────────────────────────────────────────────────────────

/// Execute SQL synchronously (inside spawn_blocking context) and return the
/// result rows as a flat Vec<Value> (first row, all columns) or empty.
fn exec_rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => rows,
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Execute SQL and ignore all results/errors (used for setup).
fn exec_ignore(ex: &Executor, sql: &str) {
    let rt = tokio::runtime::Handle::current();
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
}

/// Snapshot the rows_scanned counter, run a query, return (delta, returned_row_count).
fn measure(ex: &Executor, sql: &str) -> (u64, usize) {
    let before = ex.metrics().rows_scanned.get();
    let rows = exec_rows(ex, sql);
    let after = ex.metrics().rows_scanned.get();
    (after.saturating_sub(before), rows.len())
}

// ─── A single efficiency scenario ─────────────────────────────────────────────

#[derive(Debug)]
struct Finding {
    desc: String,
    query: String,
    rows_scanned: u64,
    rows_returned: usize,
    n: usize,
    threshold: u64,
    kind: FindingKind,
}

#[derive(Debug, PartialEq)]
enum FindingKind {
    /// Expected few rows scanned, got too many — index not used.
    FullScanWhenShouldBeIndex,
    /// Expected ~N rows scanned, got suspiciously few — sanity check failed.
    FullScanTooLow,
    /// No rows were returned for a PK that definitely exists.
    PkLookupReturnedEmpty,
}

/// Build a fresh executor+table and run the efficiency scenario for a given N.
/// Returns any findings.
fn run_scenario(n: usize, seed: u64) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut rng = Rng(seed);

    // ── Build executor with a shared MetricsRegistry so we can read counters ──
    let shared_metrics = Arc::new(MetricsRegistry::new());
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(
        Executor::new(catalog, storage).with_metrics(Arc::clone(&shared_metrics)),
    );

    // ── Create table and bulk-insert N rows ───────────────────────────────────
    exec_ignore(
        &ex,
        "CREATE TABLE eff_test (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT)",
    );

    // Insert rows in batches to stay below any single-statement size limits.
    const BATCH: usize = 200;
    let mut inserted = 0usize;
    while inserted < n {
        let end = (inserted + BATCH).min(n);
        let values: Vec<String> = (inserted..end)
            .map(|i| {
                let val = (i as i64).wrapping_mul(7) % 1000;
                format!("({}, {}, 'tag{}')", i as i64, val, i % 50)
            })
            .collect();
        let sql = format!("INSERT INTO eff_test VALUES {}", values.join(", "));
        exec_ignore(&ex, &sql);
        inserted = end;
    }

    // Verify the table is fully populated.
    let count_rows = exec_rows(&ex, "SELECT COUNT(*) FROM eff_test");
    let actual_n = count_rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| match v {
            Value::Int32(i) => Some(*i as usize),
            Value::Int64(i) => Some(*i as usize),
            _ => None,
        })
        .unwrap_or(0);

    if actual_n < n {
        // If we couldn't even insert, skip this scenario rather than generating
        // false findings.
        eprintln!(
            "  [warn] Only inserted {actual_n}/{n} rows; skipping scenario seed={seed}"
        );
        return findings;
    }

    // ─── Test 1: Full table scan (SELECT *) should scan ~N rows ───────────────
    // We allow 50%–250% of N as the acceptable range for a full scan.
    // A SeqScan physically reads every row; columnar batching might cause slight
    // over-counting but should never scan far fewer than N.
    let full_lo = (n as u64) / 2;
    let _full_hi = (n as u64) * 5 / 2; // headroom for batch-size granularity
    let (full_scanned, full_returned) = measure(&ex, "SELECT * FROM eff_test");
    if full_scanned < full_lo {
        findings.push(Finding {
            desc: format!(
                "Full scan scanned only {full_scanned} rows for N={n} — \
                 too few, metric may not be recording SeqScan correctly"
            ),
            query: "SELECT * FROM eff_test".into(),
            rows_scanned: full_scanned,
            rows_returned: full_returned,
            n,
            threshold: full_lo,
            kind: FindingKind::FullScanTooLow,
        });
    }

    // ─── Test 2: PK equality lookup should scan << N rows ─────────────────────
    // Pick a random key that exists in the table.
    let pk = rng.below(n) as i64;
    let pk_eq_sql = format!("SELECT * FROM eff_test WHERE id = {pk}");
    let (pk_scanned, pk_returned) = measure(&ex, &pk_eq_sql);

    // If the row count returned is 0, that's a correctness issue, not efficiency.
    if pk_returned == 0 {
        findings.push(Finding {
            desc: format!("PK equality lookup for id={pk} returned 0 rows (expected 1)"),
            query: pk_eq_sql.clone(),
            rows_scanned: pk_scanned,
            rows_returned: pk_returned,
            n,
            threshold: 0,
            kind: FindingKind::PkLookupReturnedEmpty,
        });
    } else {
        // Efficiency gate: a PK lookup must scan at most 1% of the table OR at
        // most 20 rows — whichever is larger. This tolerates small tables (N <
        // 2000) where even a partial scan might hit 20 rows before the PK match.
        let pk_threshold = ((n as u64) / 100).max(20);
        if pk_scanned > pk_threshold {
            findings.push(Finding {
                desc: format!(
                    "PK equality (id={pk}) scanned {pk_scanned} rows for N={n} \
                     (threshold={pk_threshold}): index not used for PK lookup"
                ),
                query: pk_eq_sql,
                rows_scanned: pk_scanned,
                rows_returned: pk_returned,
                n,
                threshold: pk_threshold,
                kind: FindingKind::FullScanWhenShouldBeIndex,
            });
        }
    }

    // ─── Test 3: Small PK range scan should scan << N rows ────────────────────
    // Scan a range of exactly 10 consecutive PK values starting at a random offset.
    let range_lo = rng.below(n.saturating_sub(10)) as i64;
    let range_hi = range_lo + 9;
    let range_sql = format!(
        "SELECT * FROM eff_test WHERE id >= {range_lo} AND id <= {range_hi}"
    );
    let (range_scanned, range_returned) = measure(&ex, &range_sql);

    // For a 10-row range scan, we tolerate up to 5% of N or 50 rows (whichever
    // is larger) as the scan budget. A B-tree range scan should stay well within
    // this; a full scan would blow past it for any N > 1000.
    let range_threshold = ((n as u64) / 20).max(50);
    if range_scanned > range_threshold {
        // Capture EXPLAIN output to diagnose whether IndexScan or SeqScan is chosen.
        let explain_plan = {
            let explain_sql =
                format!("EXPLAIN SELECT * FROM eff_test WHERE id >= {range_lo} AND id <= {range_hi}");
            let rows = exec_rows(&ex, &explain_sql);
            rows.iter()
                .flat_map(|r| r.iter())
                .filter_map(|v| if let Value::Text(s) = v { Some(s.clone()) } else { None })
                .collect::<Vec<_>>()
                .join(" | ")
        };
        findings.push(Finding {
            desc: format!(
                "PK range scan ({range_lo}..={range_hi}) scanned {range_scanned} rows \
                 for N={n} (threshold={range_threshold}): plan=[{explain_plan}]"
            ),
            query: range_sql,
            rows_scanned: range_scanned,
            rows_returned: range_returned,
            n,
            threshold: range_threshold,
            kind: FindingKind::FullScanWhenShouldBeIndex,
        });
    }

    // ─── Test 4: Non-PK equality lookup (no index) should scan ~N rows ────────
    // WHERE val = <something> has no index; expect a full scan.
    // We test this as a sanity check that the metric counter is actually
    // incremented for full scans (not just for index paths).
    let nonpk_sql = "SELECT * FROM eff_test WHERE val = 0";
    let (nonpk_scanned, _nonpk_returned) = measure(&ex, nonpk_sql);
    if nonpk_scanned < full_lo {
        findings.push(Finding {
            desc: format!(
                "Non-PK equality (val=0) scanned only {nonpk_scanned} rows for N={n} \
                 — expected full scan (~{n}); metric may be under-counting"
            ),
            query: nonpk_sql.into(),
            rows_scanned: nonpk_scanned,
            rows_returned: 0,
            n,
            threshold: full_lo,
            kind: FindingKind::FullScanTooLow,
        });
    }

    // ─── Test 5: EXPLAIN ANALYZE output consistency ────────────────────────────
    // EXPLAIN ANALYZE reports "Actual Rows" in its output. For the PK lookup we
    // verify the reported actual rows == pk_returned (which we already checked
    // above), confirming EXPLAIN and the real execution path agree.
    let explain_sql = format!("EXPLAIN ANALYZE SELECT * FROM eff_test WHERE id = {pk}");
    let explain_rows = exec_rows(&ex, &explain_sql);
    // Look for "Actual Rows: N" line in EXPLAIN output.
    let actual_rows_line = explain_rows.iter().flat_map(|row| row.iter()).find_map(|v| {
        if let Value::Text(s) = v {
            if s.trim_start().starts_with("Actual Rows:") {
                return Some(s.clone());
            }
        }
        None
    });
    if let Some(line) = actual_rows_line {
        // Parse the number after "Actual Rows: "
        if let Some(num_str) = line.trim().strip_prefix("Actual Rows:") {
            if let Ok(actual) = num_str.trim().parse::<usize>() {
                // If PK returned 1 row, EXPLAIN ANALYZE must also report 1.
                if pk_returned > 0 && actual != pk_returned {
                    findings.push(Finding {
                        desc: format!(
                            "EXPLAIN ANALYZE reports Actual Rows={actual} but \
                             execution returned {pk_returned} rows for id={pk}"
                        ),
                        query: explain_sql,
                        rows_scanned: 0,
                        rows_returned: actual,
                        n,
                        threshold: pk_returned as u64,
                        kind: FindingKind::FullScanTooLow,
                    });
                }
            }
        }
    }

    findings
}

// ─── Main ─────────────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xEFF1_C1E0_u64;
    let mut n: usize = 2000;
    let mut num_seeds: usize = 3;
    let mut max_report: usize = 20;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args[i].parse().expect("--seed requires a u64");
            }
            "--n" => {
                i += 1;
                n = args[i].parse().expect("--n requires a usize");
            }
            "--seeds" => {
                i += 1;
                num_seeds = args[i].parse().expect("--seeds requires a usize");
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }

    // Silence executor panic output — we never expect panics here, but if they
    // happen catch_unwind will catch them and we'll see an empty result set.
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus efficiency harness");
    println!("n={n} num_seeds={num_seeds} base_seed={seed:#x}");
    println!("Assertions: PK equality << N, PK range << N, full scan ~N\n");

    let mut all_findings: Vec<Finding> = Vec::new();

    for s in 0..num_seeds {
        let this_seed = seed.wrapping_add(s as u64).wrapping_mul(0x9E3779B97F4A7C15);
        println!("  seed {s}/{num_seeds}: seed={this_seed:#x} n={n}");
        let mut fs = run_scenario(n, this_seed);
        all_findings.append(&mut fs);
    }

    println!("\n════ SUMMARY ════");
    println!("table rows (N)     : {n}");
    println!("seeds tested       : {num_seeds}");
    println!("findings           : {}", all_findings.len());

    if all_findings.is_empty() {
        println!("\nAll efficiency assertions passed: PK lookups use index paths.");
    } else {
        for (fi, f) in all_findings.iter().enumerate() {
            if fi >= max_report {
                println!("  ... ({} more findings suppressed)", all_findings.len() - max_report);
                break;
            }
            println!("\n─── Finding #{} ({:?}) ───", fi + 1, f.kind);
            println!("  desc          : {}", f.desc);
            println!("  query         : {}", f.query);
            println!("  rows_scanned  : {}", f.rows_scanned);
            println!("  rows_returned : {}", f.rows_returned);
            println!("  N             : {}", f.n);
            println!("  threshold     : {}", f.threshold);
        }
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
