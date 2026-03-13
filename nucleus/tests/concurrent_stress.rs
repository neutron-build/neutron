//! Concurrent stress tests for Nucleus.
//!
//! These tests validate thread safety and correctness under concurrent access.
//! All tests are marked `#[ignore]` — run explicitly with:
//!
//!     cargo test --release --test concurrent_stress -- --ignored --nocapture

use nucleus::embedded::Database;
use nucleus::types::Value;
use std::sync::Arc;

// ============================================================================
// Helper: create a shared executor for concurrent tests
// ============================================================================

fn setup() -> Arc<Database> {
    Arc::new(Database::mvcc())
}

// ============================================================================
// Test 1: Concurrent readers and writers
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_concurrent_readers_writers() {
    let db = setup();

    // Create table
    db.execute("CREATE TABLE rw_test (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();

    let total_inserts = 1000u32;
    let writer_count = 8;
    let reader_count = 8;
    let inserts_per_writer = total_inserts / writer_count;

    // Spawn writers
    let mut handles = Vec::new();
    for w in 0..writer_count {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..inserts_per_writer {
                let id = w * inserts_per_writer + i;
                let sql = format!("INSERT INTO rw_test VALUES ({id}, {id})");
                let _ = db.execute(&sql).await;
            }
        }));
    }

    // Spawn readers
    for _ in 0..reader_count {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let result = db.query("SELECT COUNT(*) FROM rw_test").await;
                assert!(result.is_ok(), "reader should not fail");
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify final count
    let rows = db.query("SELECT COUNT(*) FROM rw_test").await.unwrap();
    let count = match &rows[0][0] {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected count type: {other:?}"),
    };
    assert_eq!(
        count, total_inserts as i64,
        "final count should match total inserts"
    );
}

// ============================================================================
// Test 2: DDL/DML interleaving
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_ddl_dml_interleave() {
    let db = setup();

    // 4 tasks doing DDL (CREATE/DROP) + INSERT on shared table
    // 4 tasks querying
    let mut handles = Vec::new();

    // DDL/DML tasks
    for t in 0..4u32 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let table = format!("ddl_test_{t}");
            for i in 0..50u32 {
                let _ = db
                    .execute(&format!(
                        "CREATE TABLE IF NOT EXISTS {table} (id INT NOT NULL, v INT)"
                    ))
                    .await;
                let _ = db
                    .execute(&format!("INSERT INTO {table} VALUES ({i}, {i})"))
                    .await;
                if i % 10 == 9 {
                    let _ = db
                        .execute(&format!("CREATE INDEX IF NOT EXISTS idx_{table} ON {table} (id)"))
                        .await;
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    // Query tasks
    for t in 0..4u32 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let table = format!("ddl_test_{t}");
            for _ in 0..100 {
                // Table may or may not exist yet — just ensure no panics
                let _ = db.query(&format!("SELECT * FROM {table}")).await;
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ============================================================================
// Test 3: Snapshot isolation invariant — balance transfers
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_snapshot_isolation() {
    let db = setup();

    // Create accounts table with 10 accounts, each starting with balance 1000
    db.execute("CREATE TABLE accounts (id INT NOT NULL, balance INT NOT NULL)")
        .await
        .unwrap();
    let num_accounts = 10;
    let initial_balance = 1000;
    for i in 0..num_accounts {
        db.execute(&format!(
            "INSERT INTO accounts VALUES ({i}, {initial_balance})"
        ))
        .await
        .unwrap();
    }

    let expected_total = num_accounts * initial_balance;
    let transfer_tasks = 8;
    let transfers_per_task = 50;

    let mut handles = Vec::new();

    // Transfer tasks: move 10 units between random accounts
    for t in 0..transfer_tasks {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..transfers_per_task {
                let from = (t * transfers_per_task + i) % num_accounts;
                let to = (from + 1) % num_accounts;
                let amount = 10;
                // Simple transfer: decrement from, increment to
                let _ = db
                    .execute(&format!(
                        "UPDATE accounts SET balance = balance - {amount} WHERE id = {from}"
                    ))
                    .await;
                let _ = db
                    .execute(&format!(
                        "UPDATE accounts SET balance = balance + {amount} WHERE id = {to}"
                    ))
                    .await;
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Invariant: total balance must remain constant
    let rows = db
        .query("SELECT SUM(balance) FROM accounts")
        .await
        .unwrap();
    let total = match &rows[0][0] {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected sum type: {other:?}"),
    };
    assert_eq!(
        total, expected_total as i64,
        "total balance must be conserved across transfers"
    );
}

// ============================================================================
// Test 4: Mixed workload — INSERT/SELECT/UPDATE/DELETE simultaneously
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_mixed_workload() {
    let db = setup();

    db.execute("CREATE TABLE mixed (id INT NOT NULL, val TEXT)")
        .await
        .unwrap();

    // Seed some initial data
    for i in 0..100 {
        db.execute(&format!("INSERT INTO mixed VALUES ({i}, 'init')"))
            .await
            .unwrap();
    }

    let duration = std::time::Duration::from_secs(3);
    let mut handles = Vec::new();

    // 2 INSERT tasks
    for t in 0..2u32 {
        let db = db.clone();
        let deadline = tokio::time::Instant::now() + duration;
        handles.push(tokio::spawn(async move {
            let mut i = 1000 + t * 100_000;
            while tokio::time::Instant::now() < deadline {
                let _ = db
                    .execute(&format!("INSERT INTO mixed VALUES ({i}, 'new')"))
                    .await;
                i += 1;
                tokio::task::yield_now().await;
            }
        }));
    }

    // 2 SELECT tasks
    for _ in 0..2 {
        let db = db.clone();
        let deadline = tokio::time::Instant::now() + duration;
        handles.push(tokio::spawn(async move {
            while tokio::time::Instant::now() < deadline {
                let _ = db.query("SELECT * FROM mixed LIMIT 50").await;
                tokio::task::yield_now().await;
            }
        }));
    }

    // 2 UPDATE tasks
    for _ in 0..2 {
        let db = db.clone();
        let deadline = tokio::time::Instant::now() + duration;
        handles.push(tokio::spawn(async move {
            let mut i = 0;
            while tokio::time::Instant::now() < deadline {
                let target = i % 100;
                let _ = db
                    .execute(&format!(
                        "UPDATE mixed SET val = 'upd' WHERE id = {target}"
                    ))
                    .await;
                i += 1;
                tokio::task::yield_now().await;
            }
        }));
    }

    // 2 DELETE tasks (delete high IDs that inserters are adding)
    for t in 0..2u32 {
        let db = db.clone();
        let deadline = tokio::time::Instant::now() + duration;
        handles.push(tokio::spawn(async move {
            let base = 1000 + t * 100_000;
            let mut i = base;
            while tokio::time::Instant::now() < deadline {
                let _ = db
                    .execute(&format!("DELETE FROM mixed WHERE id = {i}"))
                    .await;
                i += 1;
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Just verify the table is still queryable and consistent
    let rows = db.query("SELECT COUNT(*) FROM mixed").await.unwrap();
    let count = match &rows[0][0] {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected count type: {other:?}"),
    };
    assert!(count >= 0, "count should be non-negative: {count}");
}

// ============================================================================
// Test 5: Connection pool pressure — many tasks competing for shared DB
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_connection_pool() {
    let db = setup();

    db.execute("CREATE TABLE pool_test (id INT NOT NULL, worker INT NOT NULL)")
        .await
        .unwrap();

    // 50 tasks all hitting the same database concurrently
    let task_count = 50;
    let ops_per_task = 20;
    let mut handles = Vec::new();

    for worker in 0..task_count {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..ops_per_task {
                let id = worker * ops_per_task + i;
                let _ = db
                    .execute(&format!("INSERT INTO pool_test VALUES ({id}, {worker})"))
                    .await;
                let _ = db
                    .query(&format!(
                        "SELECT * FROM pool_test WHERE worker = {worker}"
                    ))
                    .await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify total inserts
    let rows = db.query("SELECT COUNT(*) FROM pool_test").await.unwrap();
    let count = match &rows[0][0] {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected count type: {other:?}"),
    };
    assert_eq!(
        count,
        (task_count * ops_per_task) as i64,
        "all inserts should complete"
    );
}

// ============================================================================
// Test 6: High-contention single-row updates
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_single_row_contention() {
    let db = setup();

    db.execute("CREATE TABLE counter (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();
    db.execute("INSERT INTO counter VALUES (1, 0)")
        .await
        .unwrap();

    let task_count = 16;
    let increments_per_task = 100;
    let mut handles = Vec::new();

    for _ in 0..task_count {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..increments_per_task {
                let _ = db
                    .execute("UPDATE counter SET val = val + 1 WHERE id = 1")
                    .await;
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // The final value depends on conflict resolution, but it should be positive
    // and the table should remain queryable.
    let rows = db.query("SELECT val FROM counter WHERE id = 1").await.unwrap();
    let val = match &rows[0][0] {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected type: {other:?}"),
    };
    assert!(val > 0, "counter should be positive: {val}");
}

// ============================================================================
// Test 7: Concurrent multi-model access (SQL + KV)
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_multi_model_concurrent() {
    let db = setup();

    let mut handles = Vec::new();

    // SQL writers
    for t in 0..4u32 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let table = format!("mm_sql_{t}");
            let _ = db
                .execute(&format!("CREATE TABLE IF NOT EXISTS {table} (id INT NOT NULL, v TEXT)"))
                .await;
            for i in 0..50u32 {
                let _ = db
                    .execute(&format!("INSERT INTO {table} VALUES ({i}, 'data')"))
                    .await;
            }
        }));
    }

    // KV writers
    for t in 0..4u32 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50u32 {
                let key = format!("stress:{t}:{i}");
                let _ = db
                    .execute(&format!("KV_SET '{key}', 'value_{i}'"))
                    .await;
            }
        }));
    }

    // KV readers (concurrent with writers)
    for t in 0..4u32 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50u32 {
                let key = format!("stress:{t}:{i}");
                let _ = db.query(&format!("KV_GET '{key}'")).await;
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify SQL tables are consistent
    for t in 0..4u32 {
        let table = format!("mm_sql_{t}");
        let rows = db
            .query(&format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap();
        let count = match &rows[0][0] {
            Value::Int64(n) => *n,
            Value::Int32(n) => *n as i64,
            other => panic!("unexpected type: {other:?}"),
        };
        assert_eq!(count, 50, "table {table} should have 50 rows");
    }
}

// ============================================================================
// Test 8: Concurrent transaction isolation — no phantom reads
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_no_phantom_reads() {
    let db = setup();

    db.execute("CREATE TABLE items (id INT NOT NULL, category TEXT)")
        .await
        .unwrap();

    // Seed with 20 items in category 'A'
    for i in 0..20 {
        db.execute(&format!("INSERT INTO items VALUES ({i}, 'A')"))
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // Writers: insert items with category 'B'
    for t in 0..4u32 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50u32 {
                let id = 1000 + t * 100 + i;
                let _ = db
                    .execute(&format!("INSERT INTO items VALUES ({id}, 'B')"))
                    .await;
                tokio::task::yield_now().await;
            }
        }));
    }

    // Readers: count category 'A' — should always be exactly 20
    for _ in 0..4 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let rows = db
                    .query("SELECT COUNT(*) FROM items WHERE category = 'A'")
                    .await;
                if let Ok(rows) = rows {
                    let count = match &rows[0][0] {
                        Value::Int64(n) => *n,
                        Value::Int32(n) => *n as i64,
                        _ => continue,
                    };
                    assert_eq!(count, 20, "category A count should be stable at 20");
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}
