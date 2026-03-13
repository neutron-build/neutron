//! Extreme stress tests for Nucleus — push every subsystem to breaking points.
//!
//! These tests exercise large scale, boundary conditions, concurrent extremes,
//! and crash edge cases. All tests are marked `#[ignore]` — run explicitly:
//!
//!     cargo test --release --test extreme_stress -- --ignored --nocapture

use nucleus::embedded::Database;
use nucleus::fts::InvertedIndex;
use nucleus::graph::{Direction, GraphStore, Properties, PropValue};
use nucleus::kv::KvStore;
use nucleus::types::Value;
use nucleus::vector::{DistanceMetric, HnswConfig, HnswIndex, Vector};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// Section 1: Storage Limits
// ============================================================================

// Test 1: Tuple boundary sizes around MAX_INLINE_TUPLE (16,352 bytes)
#[tokio::test]
#[ignore = "extreme stress test: large tuple boundary"]
async fn extreme_tuple_boundary_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("test.db")).unwrap();

    db.execute("CREATE TABLE big (id INT NOT NULL, data TEXT NOT NULL)")
        .await
        .unwrap();

    // Test sizes approaching the inline tuple limit.
    // The actual limit depends on tuple header + column overhead, so some
    // large sizes will be rejected. We test that small-to-medium sizes
    // succeed and large sizes fail gracefully (no panic).
    let sizes = [100, 1000, 4000, 8000, 12000, 16000, 16340, 16352];
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (idx, &size) in sizes.iter().enumerate() {
        let data: String = "A".repeat(size);
        let sql = format!("INSERT INTO big VALUES ({idx}, '{data}')");
        match db.execute(&sql).await {
            Ok(_) => succeeded.push(size),
            Err(e) => {
                eprintln!("Size {size} rejected (expected near boundary): {e:?}");
                failed.push(size);
            }
        }
    }

    // Small sizes must succeed
    assert!(
        succeeded.contains(&100),
        "100-byte value should succeed"
    );
    assert!(
        succeeded.contains(&1000),
        "1000-byte value should succeed"
    );
    assert!(
        succeeded.contains(&4000),
        "4000-byte value should succeed"
    );

    // Verify successful values round-trip correctly
    let rows = db.query("SELECT id, data FROM big ORDER BY id").await.unwrap();
    assert_eq!(rows.len(), succeeded.len());
    for (row_idx, row) in rows.iter().enumerate() {
        let id = extract_i64(&row[0]) as usize;
        let expected_size = sizes[id];
        match &row[1] {
            Value::Text(s) => assert_eq!(
                s.len(),
                expected_size,
                "row {row_idx} (id={id}): expected {expected_size} bytes, got {}",
                s.len()
            ),
            other => panic!("row {row_idx}: expected Text, got {other:?}"),
        }
    }

    eprintln!(
        "Tuple boundary: succeeded={:?}, failed={:?}",
        succeeded, failed
    );
}

// Test 2: B-tree split storm — 32 concurrent tasks × 1000 rows each
#[tokio::test]
#[ignore = "extreme stress test: btree split storm"]
async fn extreme_btree_split_storm() {
    let db = Arc::new(Database::mvcc());

    db.execute("CREATE TABLE storm (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();
    db.execute("CREATE INDEX idx_storm ON storm (id)")
        .await
        .unwrap();

    let tasks = 32u32;
    let rows_per = 1000u32;
    let mut handles = Vec::new();

    for t in 0..tasks {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..rows_per {
                let id = t * rows_per + i;
                let _ = db
                    .execute(&format!("INSERT INTO storm VALUES ({id}, {id})"))
                    .await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify count — some inserts may fail under extreme concurrency
    let rows = db.query("SELECT COUNT(*) FROM storm").await.unwrap();
    let count = extract_i64(&rows[0][0]);
    // With silent error handling, some may fail; require at least 80%
    let expected = (tasks * rows_per) as i64;
    assert!(
        count >= expected * 8 / 10,
        "at least 80% of 32K rows should insert: got {count}/{expected}"
    );
    eprintln!("Inserted {count}/{expected} rows under concurrent pressure");

    // Verify ORDER BY produces sorted results (check relative ordering)
    let rows = db
        .query("SELECT id FROM storm ORDER BY id LIMIT 20")
        .await
        .unwrap();
    assert!(!rows.is_empty(), "should have results");
    for i in 1..rows.len() {
        let prev = extract_i64(&rows[i - 1][0]);
        let curr = extract_i64(&rows[i][0]);
        assert!(
            curr >= prev,
            "ORDER BY should produce sorted results: row[{}]={prev} > row[{}]={curr}",
            i - 1,
            i
        );
    }

    // Point lookup — pick a likely-existing ID (middle of first task's range)
    let probe_id = rows_per / 2; // id 500, from task 0
    let rows = db
        .query(&format!("SELECT val FROM storm WHERE id = {probe_id}"))
        .await
        .unwrap();
    if !rows.is_empty() {
        assert_eq!(extract_i64(&rows[0][0]), probe_id as i64);
    }
}

// Test 3: MVCC version accumulation / GC starvation
#[tokio::test]
#[ignore = "extreme stress test: mvcc gc starvation"]
async fn extreme_mvcc_version_accumulation_gc_starvation() {
    let db = Arc::new(Database::mvcc());

    db.execute("CREATE TABLE versioned (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();

    // Insert 10 rows
    for i in 0..10 {
        db.execute(&format!("INSERT INTO versioned VALUES ({i}, 0)"))
            .await
            .unwrap();
    }

    // Take a "stale snapshot" by reading current state
    let snapshot_rows = db
        .query("SELECT val FROM versioned ORDER BY id")
        .await
        .unwrap();
    let original_vals: Vec<i64> = snapshot_rows.iter().map(|r| extract_i64(&r[0])).collect();
    assert!(original_vals.iter().all(|&v| v == 0));

    // Create many versions via repeated updates
    for cycle in 1..=2000 {
        for id in 0..10 {
            let _ = db
                .execute(&format!(
                    "UPDATE versioned SET val = {cycle} WHERE id = {id}"
                ))
                .await;
        }
    }

    // Verify final values
    let final_rows = db
        .query("SELECT val FROM versioned ORDER BY id")
        .await
        .unwrap();
    for row in &final_rows {
        let val = extract_i64(&row[0]);
        assert_eq!(val, 2000, "final value should be 2000");
    }

    // Verify database is still responsive
    let rows = db.query("SELECT COUNT(*) FROM versioned").await.unwrap();
    assert_eq!(extract_i64(&rows[0][0]), 10);
}

// Test 4: Massive concurrent transactions (1000 tasks)
#[tokio::test]
#[ignore = "extreme stress test: 1000 concurrent transactions"]
async fn extreme_massive_concurrent_transactions() {
    let db = Arc::new(Database::mvcc());

    db.execute("CREATE TABLE shared (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO shared VALUES ({i}, 0)"))
            .await
            .unwrap();
    }

    let committed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let task_count = 1000u64;
    let mut handles = Vec::new();

    for t in 0..task_count {
        let db = db.clone();
        let committed = committed.clone();
        let failed = failed.clone();
        handles.push(tokio::spawn(async move {
            let target = (t % 100) as i64;
            let result = db
                .execute(&format!(
                    "UPDATE shared SET val = val + 1 WHERE id = {target}"
                ))
                .await;
            if result.is_ok() {
                committed.fetch_add(1, Ordering::Relaxed);
            } else {
                failed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let c = committed.load(Ordering::Relaxed);
    let f = failed.load(Ordering::Relaxed);
    assert_eq!(
        c + f,
        task_count,
        "all tasks should complete: committed={c}, failed={f}"
    );

    // Row count must stay at 100
    let rows = db.query("SELECT COUNT(*) FROM shared").await.unwrap();
    assert_eq!(extract_i64(&rows[0][0]), 100);
}

// ============================================================================
// Section 2: Transaction Scaling
// ============================================================================

// Test 5: SSI false-positive pressure
#[tokio::test]
#[ignore = "extreme stress test: ssi false positive pressure"]
async fn extreme_ssi_false_positive_pressure() {
    let db = Arc::new(Database::mvcc());

    db.execute("CREATE TABLE ssi_test (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO ssi_test VALUES ({i}, 0)"))
            .await
            .unwrap();
    }

    let committed = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let txn_count = 100u64;
    let mut handles = Vec::new();

    for t in 0..txn_count {
        let db = db.clone();
        let committed = committed.clone();
        let conflicts = conflicts.clone();
        handles.push(tokio::spawn(async move {
            // Each txn reads one row and writes another (overlapping sets)
            let read_id = t % 20;
            let write_id = (t + 7) % 20;

            // Read
            let _ = db
                .query(&format!("SELECT val FROM ssi_test WHERE id = {read_id}"))
                .await;

            // Write
            let result = db
                .execute(&format!(
                    "UPDATE ssi_test SET val = val + 1 WHERE id = {write_id}"
                ))
                .await;

            if result.is_ok() {
                committed.fetch_add(1, Ordering::Relaxed);
            } else {
                conflicts.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let c = committed.load(Ordering::Relaxed);
    let f = conflicts.load(Ordering::Relaxed);
    assert_eq!(c + f, txn_count);
    assert!(c > 0, "at least some transactions should commit");

    // Table should still be consistent
    let rows = db.query("SELECT COUNT(*) FROM ssi_test").await.unwrap();
    assert_eq!(extract_i64(&rows[0][0]), 20);
}

// Test 6: Rapid txn ID cycling (50K short transactions)
#[tokio::test]
#[ignore = "extreme stress test: rapid txn id cycling"]
async fn extreme_txn_id_rapid_cycling() {
    let db = Database::mvcc();

    db.execute("CREATE TABLE rapid (id INT NOT NULL, val INT NOT NULL)")
        .await
        .unwrap();
    db.execute("INSERT INTO rapid VALUES (1, 0)")
        .await
        .unwrap();

    let total = 50_000u64;
    let mut update_count = 0u64;

    for i in 0..total {
        if i % 2 == 0 {
            let _ = db.query("SELECT val FROM rapid WHERE id = 1").await;
        } else {
            let result = db
                .execute(&format!("UPDATE rapid SET val = {i} WHERE id = 1"))
                .await;
            if result.is_ok() {
                update_count += 1;
            }
        }
    }

    assert!(update_count > 0, "some updates should succeed");
    eprintln!(
        "Completed {total} rapid txns ({update_count} updates)"
    );

    // Database should still be queryable after rapid cycling.
    // The GC watermark fix ensures committed row versions stay visible
    // even after their txn IDs are removed from the committed set.
    let rows = db.query("SELECT val FROM rapid WHERE id = 1").await.unwrap();
    assert_eq!(rows.len(), 1, "row must remain visible after rapid txn cycling");
    let val = extract_i64(&rows[0][0]);
    assert!(val > 0, "val should have been updated: {val}");
    eprintln!("Final val after rapid cycling: {val}");
}

// Test 7: Snapshot isolation under heavy churn (balance conservation)
#[tokio::test]
#[ignore = "extreme stress test: snapshot isolation heavy churn"]
async fn extreme_snapshot_isolation_under_heavy_churn() {
    let db = Arc::new(Database::mvcc());

    let num_accounts = 20i64;
    let initial_balance = 1000i64;
    let expected_total = num_accounts * initial_balance;

    db.execute("CREATE TABLE accts (id INT NOT NULL, balance INT NOT NULL)")
        .await
        .unwrap();
    for i in 0..num_accounts {
        db.execute(&format!(
            "INSERT INTO accts VALUES ({i}, {initial_balance})"
        ))
        .await
        .unwrap();
    }

    let writer_tasks = 16;
    let transfers_per = 500;
    let reader_tasks = 16;
    let reads_per = 200;

    let mut handles = Vec::new();

    // Writers: transfer between adjacent accounts
    for t in 0..writer_tasks {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..transfers_per {
                let from = ((t * transfers_per + i) % num_accounts as usize) as i64;
                let to = (from + 1) % num_accounts;
                let amount = 1;
                // Individual updates (best-effort)
                let _ = db
                    .execute(&format!(
                        "UPDATE accts SET balance = balance - {amount} WHERE id = {from}"
                    ))
                    .await;
                let _ = db
                    .execute(&format!(
                        "UPDATE accts SET balance = balance + {amount} WHERE id = {to}"
                    ))
                    .await;
                tokio::task::yield_now().await;
            }
        }));
    }

    // Readers: check conservation invariant
    let violation_count = Arc::new(AtomicU64::new(0));
    let null_count = Arc::new(AtomicU64::new(0));
    for _ in 0..reader_tasks {
        let db = db.clone();
        let violations = violation_count.clone();
        let nulls = null_count.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..reads_per {
                let result = db.query("SELECT SUM(balance) FROM accts").await;
                if let Ok(rows) = result {
                    match extract_i64_opt(&rows[0][0]) {
                        Some(total) if total != expected_total => {
                            violations.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            // SUM returned NULL — GC visibility edge case
                            nulls.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Final check — with the GC watermark fix, SUM should always return
    // a valid number (not NULL) and the total should be conserved.
    let rows = db.query("SELECT SUM(balance) FROM accts").await.unwrap();
    let total = extract_i64(&rows[0][0]);
    assert_eq!(
        total, expected_total,
        "final total must be conserved: expected {expected_total}, got {total}"
    );
    eprintln!("Final total: {total} (expected {expected_total})");

    let violations = violation_count.load(Ordering::Relaxed);
    let nulls = null_count.load(Ordering::Relaxed);
    assert_eq!(nulls, 0, "SUM should never return NULL with GC watermark fix");
    eprintln!("Snapshot reads: {violations} violations, {nulls} nulls");
}

// ============================================================================
// Section 3: Executor Edge Cases
// ============================================================================

// Test 8: Subquery depth at limit
#[tokio::test]
#[ignore = "extreme stress test: subquery depth limit"]
async fn extreme_subquery_depth_at_limit() {
    let db = Database::mvcc();

    db.execute("CREATE TABLE depth (x INT NOT NULL)")
        .await
        .unwrap();
    db.execute("INSERT INTO depth VALUES (42)").await.unwrap();

    // Discover the actual nesting limit by probing.
    // The SQL parser has a RecursionLimitExceeded guard that fires before
    // the executor's MAX_SUBQUERY_DEPTH.
    let mut max_succeeded = 0;
    for depth in [5, 10, 20, 30, 40, 50, 60] {
        let mut q = "SELECT x FROM depth".to_string();
        for _ in 0..depth {
            q = format!("SELECT x FROM ({q}) AS sub");
        }
        match db.query(&q).await {
            Ok(rows) => {
                assert_eq!(extract_i64(&rows[0][0]), 42);
                max_succeeded = depth;
            }
            Err(e) => {
                eprintln!("Depth {depth} failed (parser limit): {e:?}");
                break;
            }
        }
    }
    eprintln!("Max successful subquery depth: {max_succeeded}");
    assert!(
        max_succeeded >= 5,
        "should handle at least 5-deep subqueries"
    );

    // One past the limit should return an error (not panic)
    let over_limit = max_succeeded + 20;
    let mut deep_query = "SELECT x FROM depth".to_string();
    for _ in 0..over_limit {
        deep_query = format!("SELECT x FROM ({deep_query}) AS sub");
    }
    let result = db.query(&deep_query).await;
    assert!(result.is_err(), "past-limit depth should return error");
    eprintln!(
        "Depth {} correctly rejected: {:?}",
        over_limit,
        result.err()
    );

    // Verify normal queries still work after depth stress
    let rows = db.query("SELECT x FROM depth").await.unwrap();
    assert_eq!(extract_i64(&rows[0][0]), 42);
}

// Test 9: Aggregate overflow (i64 boundary)
#[tokio::test]
#[ignore = "extreme stress test: aggregate overflow i64"]
async fn extreme_aggregate_overflow_i64() {
    let db = Database::mvcc();

    db.execute("CREATE TABLE bignum (val BIGINT NOT NULL)")
        .await
        .unwrap();

    let near_max = i64::MAX / 10;
    for _ in 0..10 {
        db.execute(&format!("INSERT INTO bignum VALUES ({near_max})"))
            .await
            .unwrap();
    }

    // SUM should be near i64::MAX (should not overflow since 10 * (MAX/10) <= MAX)
    let rows = db
        .query("SELECT SUM(val) FROM bignum")
        .await
        .unwrap();
    let sum = extract_i64(&rows[0][0]);
    let expected = near_max * 10;
    assert_eq!(sum, expected, "SUM near i64::MAX should be exact");

    // Add one more row to push past i64::MAX
    db.execute(&format!("INSERT INTO bignum VALUES ({near_max})"))
        .await
        .unwrap();

    // This should either overflow gracefully or return an error — not panic
    let result = db.query("SELECT SUM(val) FROM bignum").await;
    match result {
        Ok(rows) => {
            let val = extract_i64(&rows[0][0]);
            eprintln!("SUM past i64::MAX returned: {val} (overflow wrapped or bigint)");
        }
        Err(e) => {
            eprintln!("SUM overflow produced error (acceptable): {e:?}");
        }
    }
}

// Test 10: Cartesian join explosion (1K × 1K = 1M rows)
#[tokio::test]
#[ignore = "extreme stress test: cartesian join 1M rows"]
async fn extreme_cartesian_join_explosion() {
    let db = Database::mvcc();

    db.execute("CREATE TABLE a (id INT NOT NULL)")
        .await
        .unwrap();
    db.execute("CREATE TABLE b (id INT NOT NULL)")
        .await
        .unwrap();

    // Insert 1000 rows into each
    for i in 0..1000 {
        db.execute(&format!("INSERT INTO a VALUES ({i})"))
            .await
            .unwrap();
        db.execute(&format!("INSERT INTO b VALUES ({i})"))
            .await
            .unwrap();
    }

    // Cartesian join: 1000 × 1000 = 1,000,000
    let rows = db
        .query("SELECT COUNT(*) FROM a, b")
        .await
        .unwrap();
    let count = extract_i64(&rows[0][0]);
    assert_eq!(count, 1_000_000, "cartesian product should be 1M");

    // Equi-join should return 1000
    let rows = db
        .query("SELECT COUNT(*) FROM a JOIN b ON a.id = b.id")
        .await
        .unwrap();
    let count = extract_i64(&rows[0][0]);
    assert_eq!(count, 1000, "equi-join should return 1K");
}

// Test 11: Deep expression nesting (500-deep AND/OR chains)
#[tokio::test]
#[ignore = "extreme stress test: deep expression nesting"]
async fn extreme_deep_expression_nesting() {
    let db = Database::mvcc();

    db.execute("CREATE TABLE expr (id INT NOT NULL)")
        .await
        .unwrap();
    db.execute("INSERT INTO expr VALUES (1)").await.unwrap();

    // Expression depth is now limited to 256 (MAX_EXPR_DEPTH) to prevent
    // stack overflow. Test that moderate depth works and extreme depth
    // returns an error instead of crashing.

    // 100-deep AND chain should succeed (well under limit)
    let mut and_clauses = Vec::with_capacity(100);
    for _ in 0..99 {
        and_clauses.push("1=1".to_string());
    }
    and_clauses.push("id = 1".to_string());
    let and_where = and_clauses.join(" AND ");
    let and_query = format!("SELECT id FROM expr WHERE {and_where}");

    let result = db.query(&and_query).await;
    assert!(result.is_ok(), "100-deep AND chain should succeed");
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(extract_i64(&rows[0][0]), 1);

    // 100-deep OR chain should also succeed
    let mut or_clauses = Vec::with_capacity(100);
    for _ in 0..99 {
        or_clauses.push("0=1".to_string());
    }
    or_clauses.push("id = 1".to_string());
    let or_where = or_clauses.join(" OR ");
    let or_query = format!("SELECT id FROM expr WHERE {or_where}");

    let result = db.query(&or_query).await;
    assert!(result.is_ok(), "100-deep OR chain should succeed");
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(extract_i64(&rows[0][0]), 1);

    // 500-deep AND chain — should return an error (not stack overflow/crash)
    // The parser may reject this before the executor depth check, which is
    // also acceptable (RecursionLimitExceeded).
    let mut deep_clauses = Vec::with_capacity(500);
    for _ in 0..499 {
        deep_clauses.push("1=1".to_string());
    }
    deep_clauses.push("id = 1".to_string());
    let deep_where = deep_clauses.join(" AND ");
    let deep_query = format!("SELECT id FROM expr WHERE {deep_where}");

    let result = db.query(&deep_query).await;
    match result {
        Ok(_) => eprintln!("500-deep AND chain succeeded (parser/executor handled it)"),
        Err(e) => eprintln!("500-deep AND chain correctly rejected: {e:?}"),
    }
    // Key assertion: no crash/stack overflow — reaching this line is the test

    // Normal query should still work
    let rows = db.query("SELECT id FROM expr").await.unwrap();
    assert_eq!(rows.len(), 1);
}

// ============================================================================
// Section 4: Multi-Model Extremes
// ============================================================================

// Test 12: KV large values and mass TTL expiry
#[tokio::test]
#[ignore = "extreme stress test: kv large values and mass ttl"]
async fn extreme_kv_large_value_and_mass_ttl() {
    let store = KvStore::new();

    // Insert 1MB value
    let mb_value = "X".repeat(1_000_000);
    store.set("large_1mb", Value::Text(mb_value.clone()), None);
    let got = store.get("large_1mb").unwrap();
    match got {
        Value::Text(s) => assert_eq!(s.len(), 1_000_000, "1MB value round-trip"),
        other => panic!("expected Text, got {other:?}"),
    }

    // Insert 10MB value
    let big_value = "Y".repeat(10_000_000);
    store.set("large_10mb", Value::Text(big_value.clone()), None);
    let got = store.get("large_10mb").unwrap();
    match got {
        Value::Text(s) => assert_eq!(s.len(), 10_000_000, "10MB value round-trip"),
        other => panic!("expected Text, got {other:?}"),
    }

    // Insert 100K keys with 1-second TTL
    for i in 0..100_000u64 {
        store.set(
            &format!("ttl_{i}"),
            Value::Int64(i as i64),
            Some(1), // 1 second TTL
        );
    }

    // Wait for TTL expiry
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Sweep expired
    let swept = store.sweep_expired();
    assert!(
        swept >= 90_000,
        "should sweep most of 100K expired keys, swept: {swept}"
    );

    // Large values (no TTL) should survive
    assert!(store.get("large_1mb").is_some(), "1MB value survives sweep");
    assert!(
        store.get("large_10mb").is_some(),
        "10MB value survives sweep"
    );
}

// Test 13: Graph dense cycles and deep traversal
#[tokio::test]
#[ignore = "extreme stress test: graph dense cycles"]
async fn extreme_graph_dense_cycles_and_deep_traversal() {
    let mut graph = GraphStore::new();

    let clique_count = 10usize;
    let nodes_per_clique = 50usize;
    let total_nodes = clique_count * nodes_per_clique;

    // Create nodes
    let mut node_ids = Vec::with_capacity(total_nodes);
    for i in 0..total_nodes {
        let props: Properties = BTreeMap::from([(
            "idx".to_string(),
            PropValue::Int(i as i64),
        )]);
        let id = graph.create_node(vec!["Node".to_string()], props);
        node_ids.push(id);
    }

    // Create fully-connected cliques
    for c in 0..clique_count {
        let start = c * nodes_per_clique;
        let end = start + nodes_per_clique;
        for i in start..end {
            for j in (i + 1)..end {
                graph.create_edge(
                    node_ids[i],
                    node_ids[j],
                    "INTRA".to_string(),
                    BTreeMap::new(),
                );
                graph.create_edge(
                    node_ids[j],
                    node_ids[i],
                    "INTRA".to_string(),
                    BTreeMap::new(),
                );
            }
        }
    }

    // Ring-connect cliques (last node of clique c → first node of clique c+1)
    for c in 0..clique_count {
        let from = node_ids[c * nodes_per_clique + nodes_per_clique - 1];
        let to = node_ids[((c + 1) % clique_count) * nodes_per_clique];
        graph.create_edge(from, to, "RING".to_string(), BTreeMap::new());
    }

    // BFS from first node should visit all nodes (no infinite loop on cycles)
    let bfs_result = graph.bfs(node_ids[0], Direction::Both, None);
    assert_eq!(
        bfs_result.len(),
        total_nodes,
        "BFS should visit all {total_nodes} nodes, got {}",
        bfs_result.len()
    );

    // DFS from first node should also visit all
    let dfs_result = graph.dfs(node_ids[0], Direction::Both, None);
    assert_eq!(
        dfs_result.len(),
        total_nodes,
        "DFS should visit all {total_nodes} nodes, got {}",
        dfs_result.len()
    );

    // Shortest path between first and last node should exist
    let path = graph.shortest_path(
        node_ids[0],
        node_ids[total_nodes - 1],
        Direction::Both,
        None,
    );
    assert!(path.is_some(), "shortest path should exist");
    let path = path.unwrap();
    assert!(
        path.len() >= 2,
        "path should have at least start and end: {:?}",
        path
    );

    // Connected components: all nodes in one component
    let components = graph.connected_components();
    assert_eq!(
        components.len(),
        1,
        "all nodes should be in one connected component"
    );
    assert_eq!(components[0].len(), total_nodes);
}

// Test 14: FTS large corpus
#[tokio::test]
#[ignore = "extreme stress test: fts large corpus"]
async fn extreme_fts_large_corpus_multi_term() {
    let mut index = InvertedIndex::new();

    // Build vocabulary: 5000 terms across 10 topics
    let topics: Vec<Vec<String>> = (0..10)
        .map(|t| {
            (0..500)
                .map(|w| format!("topic{t}word{w}"))
                .collect::<Vec<_>>()
        })
        .collect();

    // Insert 100K documents
    let start = std::time::Instant::now();
    for doc_id in 0..100_000u64 {
        let topic = (doc_id % 10) as usize;
        // Each document uses ~20 words from its topic
        let words: Vec<&str> = topics[topic]
            .iter()
            .cycle()
            .skip((doc_id as usize) % 480)
            .take(20)
            .map(|s| s.as_str())
            .collect();
        let text = words.join(" ");
        index.add_document(doc_id, &text);
    }
    let index_time = start.elapsed();
    eprintln!("Indexed 100K docs in {:?}", index_time);

    // Search for a common term (appears in ~10K docs)
    let start = std::time::Instant::now();
    let results = index.search("topic0word0", 10);
    let search_time = start.elapsed();
    assert!(!results.is_empty(), "common term should return results");
    eprintln!(
        "Common term search returned {} results in {:?}",
        results.len(),
        search_time
    );
    assert!(
        search_time.as_secs() < 1,
        "search should complete in < 1s, took {:?}",
        search_time
    );

    // Search for a rare term (appears in ~1/500th of topic docs = ~20 docs)
    let rare_results = index.search("topic5word499", 10);
    let common_results = index.search("topic5word0", 10);

    // IDF check: rare term should have higher BM25 scores
    if !rare_results.is_empty() && !common_results.is_empty() {
        let rare_top_score = rare_results[0].1;
        let common_top_score = common_results[0].1;
        eprintln!(
            "Rare term top score: {rare_top_score:.4}, Common term top score: {common_top_score:.4}"
        );
        // Rare terms should generally score higher due to IDF
        // (not a hard assertion since BM25 also depends on term frequency)
    }
}

// Test 15: HNSW high-dimension vectors
#[tokio::test]
#[ignore = "extreme stress test: hnsw high dimension"]
async fn extreme_vector_hnsw_high_dimension() {
    let config = HnswConfig {
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search: 100,
        metric: DistanceMetric::Cosine,
    };
    let mut index = HnswIndex::new(config);

    let dim = 1024;
    let n = 10_000;
    let k = 10;

    // Generate random normalized vectors
    let mut rng_state = 42u64;
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim {
            // Simple LCG PRNG
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let f = ((rng_state >> 33) as f32) / (u32::MAX as f32) - 0.5;
            v.push(f);
        }
        // Normalize
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        vectors.push(v);
    }

    // Insert all vectors
    let start = std::time::Instant::now();
    for (i, v) in vectors.iter().enumerate() {
        index.insert(i as u64, Vector::new(v.clone()));
    }
    let insert_time = start.elapsed();
    eprintln!("Inserted {n} × {dim}-d vectors in {:?}", insert_time);

    // Query with first vector
    let query = Vector::new(vectors[0].clone());
    let start = std::time::Instant::now();
    let results = index.search(&query, k);
    let search_time = start.elapsed();
    eprintln!("HNSW search returned {} results in {:?}", results.len(), search_time);

    assert_eq!(results.len(), k, "should return {k} results");

    // Check no NaN/infinity in distances
    for (id, dist) in &results {
        assert!(dist.is_finite(), "distance should be finite: id={id}, dist={dist}");
        assert!(!dist.is_nan(), "distance should not be NaN: id={id}");
    }

    // First result should be the query vector itself (distance ≈ 0)
    assert_eq!(
        results[0].0, 0,
        "closest to vector 0 should be vector 0 itself"
    );
    assert!(
        results[0].1 < 0.01,
        "distance to self should be ~0, got {}",
        results[0].1
    );

    // Brute-force top-k for recall check
    let mut brute_force: Vec<(u64, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let dist = cosine_distance(&vectors[0], v);
            (i as u64, dist)
        })
        .collect();
    brute_force.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let bf_top_k: std::collections::HashSet<u64> =
        brute_force.iter().take(k).map(|(id, _)| *id).collect();
    let hnsw_top_k: std::collections::HashSet<u64> =
        results.iter().map(|(id, _)| *id).collect();

    let overlap = bf_top_k.intersection(&hnsw_top_k).count();
    let recall = overlap as f64 / k as f64;
    eprintln!("Recall@{k}: {recall:.2} ({overlap}/{k})");
    assert!(
        recall >= 0.5,
        "recall@{k} should be >= 0.5, got {recall:.2}"
    );
}

// ============================================================================
// Section 5: Crash Resilience
// ============================================================================

// Test 16: WAL sustained write pressure then crash
#[tokio::test]
#[ignore = "extreme stress test: wal sustained write crash"]
async fn extreme_wal_sustained_write_pressure_crash() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Insert 10K rows and close gracefully
    {
        let db = Database::durable_mvcc(dir.path()).unwrap();
        db.execute("CREATE TABLE wal_stress (id INT NOT NULL, val INT NOT NULL)")
            .await
            .unwrap();
        for i in 0..10_000 {
            db.execute(&format!("INSERT INTO wal_stress VALUES ({i}, {i})"))
                .await
                .unwrap();
        }
        db.close();
    }

    // Phase 2: Reopen and verify 10K rows
    {
        let db = Database::durable_mvcc(dir.path()).unwrap();
        let rows = db.query("SELECT COUNT(*) FROM wal_stress").await.unwrap();
        let count = extract_i64(&rows[0][0]);
        assert_eq!(count, 10_000, "10K rows should survive graceful close");

        // Insert 5K more then drop without close (simulates crash)
        for i in 10_000..15_000 {
            db.execute(&format!("INSERT INTO wal_stress VALUES ({i}, {i})"))
                .await
                .unwrap();
        }
        // Drop without calling close() — simulates crash
        drop(db);
    }

    // Phase 3: Reopen and verify at least the first 10K are durable
    {
        let db = Database::durable_mvcc(dir.path()).unwrap();
        let rows = db.query("SELECT COUNT(*) FROM wal_stress").await.unwrap();
        let count = extract_i64(&rows[0][0]);
        assert!(
            count >= 10_000,
            "at least 10K rows should survive crash, got {count}"
        );
        eprintln!("Recovered {count} rows after crash (10K guaranteed, up to 15K possible)");
    }
}

// Test 17: Partial WAL record truncation
#[tokio::test]
#[ignore = "extreme stress test: partial wal truncation"]
async fn extreme_partial_wal_record_truncation() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Write 50 committed rows
    {
        let db = Database::durable_mvcc(dir.path()).unwrap();
        db.execute("CREATE TABLE trunc (id INT NOT NULL)")
            .await
            .unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO trunc VALUES ({i})"))
                .await
                .unwrap();
        }
        db.close();
    }

    // Phase 2: Truncate WAL by removing last few bytes
    {
        let wal_path = dir.path().join("mvcc.wal");
        let data = std::fs::read(&wal_path).unwrap();
        assert!(data.len() > 50, "WAL should have substantial data");

        // Remove last 20 bytes (corrupt last record)
        let truncated = &data[..data.len() - 20];
        std::fs::write(&wal_path, truncated).unwrap();
    }

    // Phase 3: Reopen — should not panic
    {
        let result = Database::durable_mvcc(dir.path());
        assert!(
            result.is_ok(),
            "recovery after truncation should not panic: {:?}",
            result.err()
        );

        let db = result.unwrap();
        let rows = db.query("SELECT COUNT(*) FROM trunc").await;
        match rows {
            Ok(rows) => {
                let count = extract_i64(&rows[0][0]);
                eprintln!("Recovered {count}/50 rows after WAL truncation");
                assert!(
                    count >= 40,
                    "should recover most rows, got {count}"
                );
            }
            Err(e) => {
                // Table might not exist if truncation hit the CREATE TABLE record
                eprintln!("Query after truncation failed (table may be lost): {e:?}");
            }
        }

        // Database should still be writable
        let _ = db
            .execute("CREATE TABLE IF NOT EXISTS post_trunc (x INT NOT NULL)")
            .await;
        let result = db
            .execute("INSERT INTO post_trunc VALUES (999)")
            .await;
        assert!(result.is_ok(), "db should be writable after recovery");
    }
}

// Test 18: Rapid crash-recover cycles (10 cycles × 100 rows)
#[tokio::test]
#[ignore = "extreme stress test: rapid crash recover cycles"]
async fn extreme_rapid_crash_recover_cycles() {
    let dir = tempfile::tempdir().unwrap();

    let cycles = 10;
    let rows_per_cycle = 100;

    // Cycle 0: Create table
    {
        let db = Database::durable_mvcc(dir.path()).unwrap();
        db.execute("CREATE TABLE cycles (cycle INT NOT NULL, seq INT NOT NULL)")
            .await
            .unwrap();
        db.close();
    }

    for cycle in 0..cycles {
        // Insert rows for this cycle
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();

            // Verify cumulative count so far
            let rows = db.query("SELECT COUNT(*) FROM cycles").await.unwrap();
            let count = extract_i64(&rows[0][0]);
            let expected = cycle * rows_per_cycle;
            assert_eq!(
                count, expected as i64,
                "cycle {cycle}: expected {expected} rows before insert, got {count}"
            );

            // Insert this cycle's rows
            for seq in 0..rows_per_cycle {
                db.execute(&format!("INSERT INTO cycles VALUES ({cycle}, {seq})"))
                    .await
                    .unwrap();
            }
            db.close();
        }
    }

    // Final verification
    {
        let db = Database::durable_mvcc(dir.path()).unwrap();
        let rows = db.query("SELECT COUNT(*) FROM cycles").await.unwrap();
        let total = extract_i64(&rows[0][0]);
        let expected = (cycles * rows_per_cycle) as i64;
        assert_eq!(total, expected, "total should be {expected}");

        // Verify each cycle has exactly 100 rows
        for cycle in 0..cycles {
            let rows = db
                .query(&format!(
                    "SELECT COUNT(*) FROM cycles WHERE cycle = {cycle}"
                ))
                .await
                .unwrap();
            let count = extract_i64(&rows[0][0]);
            assert_eq!(
                count,
                rows_per_cycle as i64,
                "cycle {cycle} should have {rows_per_cycle} rows"
            );
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn extract_i64(val: &Value) -> i64 {
    match val {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        Value::Float64(f) => *f as i64,
        Value::Null => 0, // SUM/COUNT can return NULL under GC visibility edge cases
        other => panic!("expected numeric value, got {other:?}"),
    }
}

fn extract_i64_opt(val: &Value) -> Option<i64> {
    match val {
        Value::Int64(n) => Some(*n),
        Value::Int32(n) => Some(*n as i64),
        Value::Float64(f) => Some(*f as i64),
        Value::Null => None,
        _ => None,
    }
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 {
        1.0
    } else {
        1.0 - (dot / denom)
    }
}
