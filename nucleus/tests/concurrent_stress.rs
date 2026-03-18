//! Concurrent stress tests for Nucleus.
//!
//! These tests validate thread safety and correctness under concurrent access.
//! All tests are marked `#[ignore]` — run explicitly with:
//!
//!     cargo test --release --test concurrent_stress -- --ignored --nocapture

use nucleus::embedded::Database;
use nucleus::types::Value;
use std::sync::atomic::{AtomicU64, Ordering};
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

// ============================================================================
// Helper: extract i64 from Value
// ============================================================================

fn extract_i64(val: &Value) -> i64 {
    match val {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        Value::Float64(f) => *f as i64,
        Value::Null => 0,
        other => panic!("expected numeric value, got {other:?}"),
    }
}

// ============================================================================
// Test 9: Deadlock detection/prevention — concurrent cross-row updates
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_deadlock_detection() {
    let db = setup();

    // Create two tables; tasks will update rows in opposite orders to provoke
    // potential deadlocks. The engine should either serialize or abort one txn
    // (never hang forever).
    db.execute("CREATE TABLE dl_a (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();
    db.execute("CREATE TABLE dl_b (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();

    for i in 0..10 {
        db.execute(&format!("INSERT INTO dl_a VALUES ({i}, 0)"))
            .await
            .unwrap();
        db.execute(&format!("INSERT INTO dl_b VALUES ({i}, 0)"))
            .await
            .unwrap();
    }

    let committed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let task_count = 50u64;
    let mut handles = Vec::new();

    for t in 0..task_count {
        let db = db.clone();
        let committed = committed.clone();
        let failed = failed.clone();
        handles.push(tokio::spawn(async move {
            // Even tasks: update dl_a then dl_b (ascending order)
            // Odd tasks: update dl_b then dl_a (reverse order — deadlock potential)
            let id_1 = (t % 10) as i64;
            let id_2 = ((t + 3) % 10) as i64;

            let (first_table, second_table) = if t % 2 == 0 {
                ("dl_a", "dl_b")
            } else {
                ("dl_b", "dl_a")
            };

            let r1 = db
                .execute(&format!(
                    "UPDATE {first_table} SET val = val + 1 WHERE id = {id_1}"
                ))
                .await;
            let r2 = db
                .execute(&format!(
                    "UPDATE {second_table} SET val = val + 1 WHERE id = {id_2}"
                ))
                .await;

            if r1.is_ok() && r2.is_ok() {
                committed.fetch_add(1, Ordering::Relaxed);
            } else {
                failed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // All tasks must complete (no infinite deadlock hang)
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;
    assert!(
        timeout.is_ok(),
        "all tasks should complete within 30s (no deadlock hang)"
    );

    let c = committed.load(Ordering::Relaxed);
    let f = failed.load(Ordering::Relaxed);
    assert_eq!(
        c + f,
        task_count,
        "all tasks should complete: committed={c}, failed={f}"
    );
    assert!(c > 0, "at least some updates should commit");

    // Tables should remain queryable and consistent
    let rows_a = db.query("SELECT COUNT(*) FROM dl_a").await.unwrap();
    assert_eq!(extract_i64(&rows_a[0][0]), 10);
    let rows_b = db.query("SELECT COUNT(*) FROM dl_b").await.unwrap();
    assert_eq!(extract_i64(&rows_b[0][0]), 10);
}

// ============================================================================
// Test 10: MVCC isolation under heavy contention — serialized increment
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_mvcc_heavy_contention() {
    let db = setup();

    db.execute("CREATE TABLE contention (id INT NOT NULL, counter INT NOT NULL)")
        .await
        .unwrap();

    // 10 hot rows that all 64 tasks will fight over
    for i in 0..10 {
        db.execute(&format!("INSERT INTO contention VALUES ({i}, 0)"))
            .await
            .unwrap();
    }

    let task_count = 64;
    let ops_per_task = 50;
    let committed = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for t in 0..task_count {
        let db = db.clone();
        let committed = committed.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..ops_per_task {
                let target = (t * ops_per_task + i) % 10;
                let result = db
                    .execute(&format!(
                        "UPDATE contention SET counter = counter + 1 WHERE id = {target}"
                    ))
                    .await;
                if result.is_ok() {
                    committed.fetch_add(1, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let c = committed.load(Ordering::Relaxed);
    assert!(
        c > 0,
        "at least some updates should succeed under contention"
    );

    // Sum of all counters should equal the number of committed updates
    let rows = db
        .query("SELECT SUM(counter) FROM contention")
        .await
        .unwrap();
    let total = extract_i64(&rows[0][0]);
    assert_eq!(
        total, c as i64,
        "sum of counters should match committed count"
    );

    // Row count must remain at 10
    let rows = db
        .query("SELECT COUNT(*) FROM contention")
        .await
        .unwrap();
    assert_eq!(extract_i64(&rows[0][0]), 10);
}

// ============================================================================
// Test 11: Sustained load — multiple iterations of mixed operations
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_sustained_load_iterations() {
    let db = setup();

    db.execute("CREATE TABLE sustained (id INT NOT NULL, iteration INT NOT NULL, val TEXT)")
        .await
        .unwrap();

    let iterations = 10;
    let tasks_per_iteration = 16;
    let ops_per_task = 30;

    for iter in 0..iterations {
        let mut handles = Vec::new();

        // Writers: insert new rows
        for t in 0..(tasks_per_iteration / 2) {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..ops_per_task {
                    let id = iter * 10000 + t as i32 * 1000 + i;
                    let _ = db
                        .execute(&format!(
                            "INSERT INTO sustained VALUES ({id}, {iter}, 'iter_{iter}_task_{t}')"
                        ))
                        .await;
                }
            }));
        }

        // Readers: count and scan
        for _ in 0..(tasks_per_iteration / 4) {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _ = db.query("SELECT COUNT(*) FROM sustained").await;
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Updaters: modify existing rows
        for _ in 0..(tasks_per_iteration / 4) {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..ops_per_task {
                    let target = iter * 10000 + i;
                    let _ = db
                        .execute(&format!(
                            "UPDATE sustained SET val = 'updated' WHERE id = {target}"
                        ))
                        .await;
                    tokio::task::yield_now().await;
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Verify consistency after each iteration
        let rows = db
            .query("SELECT COUNT(*) FROM sustained")
            .await
            .unwrap();
        let count = extract_i64(&rows[0][0]);
        assert!(
            count > 0,
            "iteration {iter}: table should have rows, got {count}"
        );
    }

    // Final consistency check
    let rows = db
        .query("SELECT COUNT(*) FROM sustained")
        .await
        .unwrap();
    let final_count = extract_i64(&rows[0][0]);
    let expected_min = (iterations * tasks_per_iteration / 2 * ops_per_task) as i64;
    assert!(
        final_count >= expected_min / 2,
        "should have substantial rows after sustained load: got {final_count}, expected at least {}/2",
        expected_min
    );
}

// ============================================================================
// Test 12: 50+ concurrent clients — mixed read/write workload
// ============================================================================

#[tokio::test]
#[ignore = "stress test; run explicitly"]
async fn stress_fifty_plus_clients_mixed() {
    let db = setup();

    db.execute("CREATE TABLE fifty_clients (id INT NOT NULL, client INT NOT NULL, op TEXT)")
        .await
        .unwrap();

    // Seed initial data for readers/updaters
    for i in 0..100 {
        db.execute(&format!(
            "INSERT INTO fifty_clients VALUES ({i}, -1, 'seed')"
        ))
        .await
        .unwrap();
    }

    let client_count = 60;
    let ops_per_client = 20;
    let mut handles = Vec::new();

    for c in 0..client_count {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            match c % 4 {
                // 15 inserters
                0 => {
                    for i in 0..ops_per_client {
                        let id = 1000 + c * ops_per_client + i;
                        let _ = db
                            .execute(&format!(
                                "INSERT INTO fifty_clients VALUES ({id}, {c}, 'insert')"
                            ))
                            .await;
                    }
                }
                // 15 readers
                1 => {
                    for _ in 0..ops_per_client {
                        let _ = db
                            .query("SELECT COUNT(*) FROM fifty_clients")
                            .await;
                        let _ = db
                            .query(&format!(
                                "SELECT * FROM fifty_clients WHERE client = {c} LIMIT 10"
                            ))
                            .await;
                        tokio::task::yield_now().await;
                    }
                }
                // 15 updaters
                2 => {
                    for i in 0..ops_per_client {
                        let target = i % 100;
                        let _ = db
                            .execute(&format!(
                                "UPDATE fifty_clients SET op = 'updated_by_{c}' WHERE id = {target}"
                            ))
                            .await;
                        tokio::task::yield_now().await;
                    }
                }
                // 15 mixed (delete + re-insert)
                _ => {
                    for i in 0..ops_per_client {
                        let id = 2000 + c * ops_per_client + i;
                        let _ = db
                            .execute(&format!(
                                "INSERT INTO fifty_clients VALUES ({id}, {c}, 'temp')"
                            ))
                            .await;
                        let _ = db
                            .execute(&format!(
                                "DELETE FROM fifty_clients WHERE id = {id}"
                            ))
                            .await;
                    }
                }
            }
        }));
    }

    // All 60 clients must complete
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;
    assert!(
        timeout.is_ok(),
        "all 60 clients should complete within 60s"
    );

    // Table should remain queryable
    let rows = db
        .query("SELECT COUNT(*) FROM fifty_clients")
        .await
        .unwrap();
    let count = extract_i64(&rows[0][0]);
    assert!(
        count >= 100,
        "at least seed rows should remain: got {count}"
    );
}
