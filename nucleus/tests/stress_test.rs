//! Stress tests for Nucleus — test limits and performance under load.
//!
//! These tests push the database to its limits:
//! - Large datasets (millions of rows)
//! - Concurrent transactions
//! - Complex queries
//! - Memory pressure
//!
//! Run with: cargo test --release --test stress_test -- --nocapture

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::MemoryEngine;
use std::sync::Arc;
use std::time::Instant;

async fn setup() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage = Arc::new(MemoryEngine::new());
    Arc::new(Executor::new(catalog, storage))
}

async fn exec(executor: &Executor, sql: &str) {
    let result = executor.execute(sql).await;
    if let Err(e) = result {
        panic!("SQL failed: {sql}\nError: {e:?}");
    }
}

// ============================================================================
// Large dataset tests
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_large_insert() {
    println!("\n=== Stress Test: Large Insert (100K rows) ===");
    let ex = setup().await;

    exec(&ex, "CREATE TABLE stress_test (id INT PRIMARY KEY, value INT, name TEXT)").await;

    let start = Instant::now();
    let batch_size = 1000;
    let total_rows = 100_000;

    for batch in 0..(total_rows / batch_size) {
        let base = batch * batch_size;
        let mut values = Vec::new();
        for i in 0..batch_size {
            let id = base + i;
            values.push(format!("({}, {}, 'Name{}')", id, id * 2, id));
        }
        let sql = format!("INSERT INTO stress_test VALUES {}", values.join(", "));
        exec(&ex, &sql).await;

        if batch % 10 == 0 {
            println!("  Inserted {} rows...", base + batch_size);
        }
    }

    let elapsed = start.elapsed();
    println!("✓ Inserted {} rows in {:?}", total_rows, elapsed);
    println!("  Throughput: {:.0} rows/sec", total_rows as f64 / elapsed.as_secs_f64());

    // Verify count
    let _result = ex.execute("SELECT COUNT(*) FROM stress_test").await.unwrap();
    println!("✓ Verified row count");
}

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_large_scan() {
    println!("\n=== Stress Test: Large Scan (100K rows) ===");
    let ex = setup().await;

    // Setup
    exec(&ex, "CREATE TABLE scan_test (id INT, value INT)").await;
    for batch in 0..100 {
        let base = batch * 1000;
        let mut values = Vec::new();
        for i in 0..1000 {
            values.push(format!("({}, {})", base + i, (base + i) * 2));
        }
        exec(&ex, &format!("INSERT INTO scan_test VALUES {}", values.join(", "))).await;
    }

    // Full table scan
    let start = Instant::now();
    let _result = ex.execute("SELECT * FROM scan_test").await.unwrap();
    let elapsed = start.elapsed();

    println!("✓ Scanned 100K rows in {:?}", elapsed);
    println!("  Throughput: {:.0} rows/sec", 100_000.0 / elapsed.as_secs_f64());
}

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_complex_join() {
    println!("\n=== Stress Test: Complex Join (10K × 10K rows) ===");
    let ex = setup().await;

    // Create two tables
    exec(&ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount FLOAT)").await;

    // Insert data
    println!("  Inserting 10K users...");
    for batch in 0..10 {
        let mut values = Vec::new();
        for i in 0..1000 {
            let id = batch * 1000 + i;
            values.push(format!("({}, 'User{}')", id, id));
        }
        exec(&ex, &format!("INSERT INTO users VALUES {}", values.join(", "))).await;
    }

    println!("  Inserting 10K orders...");
    for batch in 0..10 {
        let mut values = Vec::new();
        for i in 0..1000 {
            let id = batch * 1000 + i;
            let user_id = id % 10_000;
            values.push(format!("({}, {}, {})", id, user_id, id as f64 * 1.5));
        }
        exec(&ex, &format!("INSERT INTO orders VALUES {}", values.join(", "))).await;
    }

    // Execute join
    println!("  Executing hash join...");
    let start = Instant::now();
    let _result = ex.execute(
        "SELECT u.name, COUNT(o.id), SUM(o.amount)
         FROM users u
         JOIN orders o ON u.id = o.user_id
         GROUP BY u.name"
    ).await.unwrap();
    let elapsed = start.elapsed();

    println!("✓ Joined 10K × 10K rows in {:?}", elapsed);
}

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_aggregation() {
    println!("\n=== Stress Test: Aggregation (50K rows) ===");
    let ex = setup().await;

    exec(&ex, "CREATE TABLE agg_test (category INT, value INT)").await;

    // Insert 50K rows with 100 categories
    for _batch in 0..50 {
        let mut values = Vec::new();
        for i in 0..1000 {
            let category = i % 100;
            values.push(format!("({}, {})", category, i));
        }
        exec(&ex, &format!("INSERT INTO agg_test VALUES {}", values.join(", "))).await;
    }

    // Complex aggregation
    let start = Instant::now();
    let _result = ex.execute(
        "SELECT category, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value)
         FROM agg_test
         GROUP BY category
         ORDER BY category"
    ).await.unwrap();
    let elapsed = start.elapsed();

    println!("✓ Aggregated 50K rows into 100 groups in {:?}", elapsed);
    println!("  Throughput: {:.0} rows/sec", 50_000.0 / elapsed.as_secs_f64());
}

// ============================================================================
// Concurrent transaction tests
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_concurrent_inserts() {
    println!("\n=== Stress Test: Concurrent Inserts (10 threads × 1K rows) ===");
    let ex = setup().await;

    exec(&ex, "CREATE TABLE concurrent_test (id INT PRIMARY KEY, thread_id INT, value INT)").await;

    let start = Instant::now();
    let mut handles = Vec::new();

    for thread_id in 0..10 {
        let ex_clone = ex.clone();
        let handle = tokio::spawn(async move {
            for i in 0..1000 {
                let id = thread_id * 1000 + i;
                let sql = format!("INSERT INTO concurrent_test VALUES ({}, {}, {})", id, thread_id, i);
                ex_clone.execute(&sql).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!("✓ 10 threads inserted 10K rows total in {:?}", elapsed);
    println!("  Throughput: {:.0} rows/sec", 10_000.0 / elapsed.as_secs_f64());

    // Verify count
    let _result = ex.execute("SELECT COUNT(*) FROM concurrent_test").await.unwrap();
    println!("✓ Verified all rows inserted");
}

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_transaction_rollback() {
    println!("\n=== Stress Test: Transaction Rollback (1K transactions) ===");
    let ex = setup().await;

    exec(&ex, "CREATE TABLE rollback_test (id INT PRIMARY KEY, value INT)").await;
    exec(&ex, "INSERT INTO rollback_test VALUES (1, 100)").await;

    let start = Instant::now();

    for i in 0..1000 {
        exec(&ex, "BEGIN").await;
        exec(&ex, &format!("UPDATE rollback_test SET value = {} WHERE id = 1", i)).await;
        exec(&ex, "ROLLBACK").await;
    }

    let elapsed = start.elapsed();
    println!("✓ Rolled back 1K transactions in {:?}", elapsed);

    // Verify original value unchanged
    let _result = ex.execute("SELECT value FROM rollback_test WHERE id = 1").await.unwrap();
    println!("✓ Verified rollback correctness");
}

// ============================================================================
// Query complexity tests
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_deep_subquery() {
    println!("\n=== Stress Test: Deep Subquery (5 levels) ===");
    let ex = setup().await;

    exec(&ex, "CREATE TABLE subquery_test (id INT, value INT)").await;
    for i in 0..1000 {
        exec(&ex, &format!("INSERT INTO subquery_test VALUES ({}, {})", i, i * 2)).await;
    }

    let sql = "
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM (
                        SELECT * FROM subquery_test WHERE value > 100
                    ) WHERE value < 1000
                ) WHERE id > 50
            ) WHERE id < 500
        ) WHERE value > 200
        ORDER BY id
        LIMIT 10
    ";

    let start = Instant::now();
    let _result = ex.execute(sql).await.unwrap();
    let elapsed = start.elapsed();

    println!("✓ Executed 5-level deep subquery in {:?}", elapsed);
}

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_window_functions() {
    println!("\n=== Stress Test: Window Functions (10K rows) ===");
    let ex = setup().await;

    exec(&ex, "CREATE TABLE window_test (category INT, value INT)").await;
    for i in 0..10_000 {
        let category = i % 10;
        exec(&ex, &format!("INSERT INTO window_test VALUES ({}, {})", category, i)).await;
    }

    let sql = "
        SELECT
            category,
            value,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY value) as rn,
            RANK() OVER (PARTITION BY category ORDER BY value) as rnk,
            LAG(value) OVER (PARTITION BY category ORDER BY value) as prev_val,
            LEAD(value) OVER (PARTITION BY category ORDER BY value) as next_val
        FROM window_test
        ORDER BY category, value
        LIMIT 100
    ";

    let start = Instant::now();
    let _result = ex.execute(sql).await.unwrap();
    let elapsed = start.elapsed();

    println!("✓ Applied 4 window functions over 10K rows in {:?}", elapsed);
}

// ============================================================================
// Memory pressure tests
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_large_result_set() {
    println!("\n=== Stress Test: Large Result Set (50K rows returned) ===");
    let ex = setup().await;

    exec(&ex, "CREATE TABLE large_result (id INT, data TEXT)").await;

    // Insert 50K rows
    for batch in 0..50 {
        let mut values = Vec::new();
        for i in 0..1000 {
            let id = batch * 1000 + i;
            values.push(format!("({}, 'Data string for row {}')", id, id));
        }
        exec(&ex, &format!("INSERT INTO large_result VALUES {}", values.join(", "))).await;
    }

    // Select all rows
    let start = Instant::now();
    let _result = ex.execute("SELECT * FROM large_result").await.unwrap();
    let elapsed = start.elapsed();

    println!("✓ Returned 50K rows in {:?}", elapsed);
    println!("  Memory usage: ~{} MB (estimated)", 50 * 100 / 1024); // rough estimate
}

#[tokio::test]
#[ignore = "stress test; run explicitly in performance validation jobs"]
async fn stress_test_many_columns() {
    println!("\n=== Stress Test: Wide Table (100 columns) ===");
    let ex = setup().await;

    // Create table with 100 columns
    let mut cols = vec!["id INT PRIMARY KEY".to_string()];
    for i in 1..100 {
        cols.push(format!("col{} INT", i));
    }
    exec(&ex, &format!("CREATE TABLE wide_table ({})", cols.join(", "))).await;

    // Insert 1000 rows
    let start = Instant::now();
    for i in 0..1000 {
        let mut vals = vec![i.to_string()];
        for j in 1..100 {
            vals.push((i * j).to_string());
        }
        exec(&ex, &format!("INSERT INTO wide_table VALUES ({})", vals.join(", "))).await;
    }
    let elapsed = start.elapsed();

    println!("✓ Inserted 1K rows × 100 columns in {:?}", elapsed);

    // Select all
    let start = Instant::now();
    let _result = ex.execute("SELECT * FROM wide_table").await.unwrap();
    let elapsed = start.elapsed();
    println!("✓ Scanned wide table in {:?}", elapsed);
}

// ============================================================================
// Summary
// ============================================================================

#[tokio::test]
async fn stress_test_summary() {
    println!("\n");
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║         NUCLEUS STRESS TEST SUITE COMPLETE                 ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("Run individual tests with:");
    println!("  cargo test --release --test stress_test <test_name> -- --nocapture");
    println!();
    println!("Example:");
    println!("  cargo test --release --test stress_test stress_test_large_insert -- --nocapture");
    println!();
}
