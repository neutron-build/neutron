use super::*;

// Multi-statement transaction test
// ======================================================================

#[tokio::test]
async fn test_transaction_flow() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE txn_t (id INT, val TEXT)").await;
    let results = exec(&ex, "BEGIN").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "BEGIN"),
        _ => panic!("expected command"),
    }
    exec(&ex, "INSERT INTO txn_t VALUES (1, 'a')").await;
    exec(&ex, "INSERT INTO txn_t VALUES (2, 'b')").await;
    let results = exec(&ex, "COMMIT").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "COMMIT"),
        _ => panic!("expected command"),
    }
    let results = exec(&ex, "SELECT COUNT(*) FROM txn_t").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));
}

// ======================================================================

// Transaction tests
// ======================================================================

#[tokio::test]
async fn test_transaction_commit() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE txn_test (id INT, name TEXT)").await;

    // BEGIN a transaction
    let results = exec(&ex, "BEGIN").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "BEGIN"),
        _ => panic!("expected Command result for BEGIN"),
    }

    // INSERT inside the transaction
    exec(&ex, "INSERT INTO txn_test VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO txn_test VALUES (2, 'bob')").await;

    // COMMIT
    let results = exec(&ex, "COMMIT").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "COMMIT"),
        _ => panic!("expected Command result for COMMIT"),
    }

    // Data should persist after commit
    let results = exec(&ex, "SELECT * FROM txn_test ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][1], Value::Text("alice".into()));
    assert_eq!(r[1][1], Value::Text("bob".into()));
}

#[tokio::test]
async fn test_transaction_rollback() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE txn_rb (id INT, name TEXT)").await;

    // Insert a row before the transaction
    exec(&ex, "INSERT INTO txn_rb VALUES (1, 'pre-existing')").await;

    // BEGIN
    exec(&ex, "BEGIN").await;

    // INSERT inside the transaction
    exec(&ex, "INSERT INTO txn_rb VALUES (2, 'should-vanish')").await;

    // Verify the row is visible during the transaction
    let results = exec(&ex, "SELECT * FROM txn_rb ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);

    // ROLLBACK
    let results = exec(&ex, "ROLLBACK").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "ROLLBACK"),
        _ => panic!("expected Command result for ROLLBACK"),
    }

    // Only the pre-existing row should remain
    let results = exec(&ex, "SELECT * FROM txn_rb ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("pre-existing".into()));
}

#[tokio::test]
async fn test_transaction_rollback_update() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE txn_upd (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO txn_upd VALUES (1, 'original')").await;

    // BEGIN
    exec(&ex, "BEGIN").await;

    // UPDATE inside the transaction
    exec(&ex, "UPDATE txn_upd SET val = 'modified' WHERE id = 1").await;

    // Verify the update is visible
    let results = exec(&ex, "SELECT val FROM txn_upd WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Text("modified".into()));

    // ROLLBACK
    exec(&ex, "ROLLBACK").await;

    // Original value should be restored
    let results = exec(&ex, "SELECT val FROM txn_upd WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Text("original".into()));
}

#[tokio::test]
async fn test_nested_begin_warning() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE txn_nest (id INT)").await;

    // First BEGIN
    let results = exec(&ex, "BEGIN").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "BEGIN"),
        _ => panic!("expected Command result for BEGIN"),
    }

    // Second BEGIN should return a warning but not error
    let results = exec(&ex, "BEGIN").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => {
            assert!(tag.contains("already in a transaction"), "expected warning, got: {tag}");
        }
        _ => panic!("expected Command result for nested BEGIN"),
    }

    // COMMIT should still work fine
    let results = exec(&ex, "COMMIT").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "COMMIT"),
        _ => panic!("expected Command result for COMMIT"),
    }
}

// ======================================================================

// Savepoints
// ======================================================================

#[tokio::test]
async fn test_savepoints() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sp_test (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO sp_test VALUES (1, 'original')").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO sp_test VALUES (2, 'txn')").await;
    exec(&ex, "SAVEPOINT sp1").await;
    exec(&ex, "INSERT INTO sp_test VALUES (3, 'after_sp')").await;

    // Rollback to savepoint should undo insert of (3)
    exec(&ex, "ROLLBACK TO SAVEPOINT sp1").await;

    let results = exec(&ex, "SELECT * FROM sp_test").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2); // (1, original) and (2, txn)

    exec(&ex, "COMMIT").await;

    let results = exec(&ex, "SELECT * FROM sp_test").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_nested_savepoints() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nsp_test (id INT)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO nsp_test VALUES (1)").await;
    exec(&ex, "SAVEPOINT sp1").await;
    exec(&ex, "INSERT INTO nsp_test VALUES (2)").await;
    exec(&ex, "SAVEPOINT sp2").await;
    exec(&ex, "INSERT INTO nsp_test VALUES (3)").await;

    // Rollback to sp1 should undo both (2) and (3)
    exec(&ex, "ROLLBACK TO SAVEPOINT sp1").await;

    let results = exec(&ex, "SELECT * FROM nsp_test").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);

    exec(&ex, "COMMIT").await;
}

// ======================================================================

// MVCC Snapshot Isolation Integration Tests
// ======================================================================

/// Create an MVCC-backed executor for testing snapshot isolation.
fn mvcc_executor() -> Executor {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(
        crate::storage::MvccStorageAdapter::new()
    );
    Executor::new(catalog, storage)
}

#[tokio::test]
async fn test_mvcc_basic_operations() {
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;
    let results = exec(&ex, "SELECT * FROM t").await;
    assert_eq!(rows(&results[0]).len(), 2);
}

#[tokio::test]
async fn test_mvcc_commit_persists() {
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE t (id INT)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;
    exec(&ex, "INSERT INTO t VALUES (2)").await;
    exec(&ex, "COMMIT").await;

    let results = exec(&ex, "SELECT * FROM t").await;
    assert_eq!(rows(&results[0]).len(), 2);
}

#[tokio::test]
async fn test_mvcc_rollback_undoes() {
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await; // auto-committed

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO t VALUES (2)").await;
    exec(&ex, "INSERT INTO t VALUES (3)").await;
    exec(&ex, "ROLLBACK").await;

    // Only the first auto-committed row should survive
    let results = exec(&ex, "SELECT * FROM t").await;
    assert_eq!(rows(&results[0]).len(), 1);
}

#[tokio::test]
async fn test_mvcc_rollback_update() {
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE t (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'original')").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "UPDATE t SET val = 'changed' WHERE id = 1").await;
    exec(&ex, "ROLLBACK").await;

    // Value should still be 'original'
    let results = exec(&ex, "SELECT val FROM t WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("original".into()));
}

#[tokio::test]
async fn test_mvcc_rollback_delete() {
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;
    exec(&ex, "INSERT INTO t VALUES (2)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "DELETE FROM t WHERE id = 1").await;
    exec(&ex, "ROLLBACK").await;

    // Both rows should still exist
    let results = exec(&ex, "SELECT * FROM t").await;
    assert_eq!(rows(&results[0]).len(), 2);
}

#[tokio::test]
async fn test_mvcc_multiple_txn_cycles() {
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE t (id INT)").await;

    // Cycle 1: insert + commit
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;
    exec(&ex, "COMMIT").await;

    // Cycle 2: insert + rollback
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO t VALUES (2)").await;
    exec(&ex, "ROLLBACK").await;

    // Cycle 3: insert + commit
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO t VALUES (3)").await;
    exec(&ex, "COMMIT").await;

    let results = exec(&ex, "SELECT * FROM t ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2); // 1 and 3, not 2
}

#[tokio::test]
async fn test_mvcc_delete_in_transaction() {
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'a')").await;
    exec(&ex, "INSERT INTO t VALUES (2, 'b')").await;
    exec(&ex, "INSERT INTO t VALUES (3, 'c')").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "DELETE FROM t WHERE id = 2").await;
    exec(&ex, "COMMIT").await;

    let results = exec(&ex, "SELECT * FROM t ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_mvcc_update_in_transaction() {
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE t (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'one')").await;
    exec(&ex, "INSERT INTO t VALUES (2, 'two')").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "UPDATE t SET val = 'TWO' WHERE id = 2").await;
    exec(&ex, "COMMIT").await;

    let results = exec(&ex, "SELECT val FROM t WHERE id = 2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("TWO".into()));
}

// ======================================================================

// ======================================================================
// Cross-Model Transaction Rollback Tests
// ======================================================================

#[tokio::test]
async fn test_rollback_reverts_kv_mutations() {
    let ex = test_executor();
    // Set a KV value before transaction
    exec(&ex, "SELECT kv_set('pre', 'original')").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT kv_set('pre', 'changed')").await;
    exec(&ex, "SELECT kv_set('new_key', 'new_val')").await;
    exec(&ex, "ROLLBACK").await;

    // 'pre' should be restored to 'original'
    let r = exec(&ex, "SELECT kv_get('pre')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("original".into()),
        "KV value should be rolled back to original");

    // 'new_key' should not exist
    let r = exec(&ex, "SELECT kv_get('new_key')").await;
    assert_eq!(scalar(&r[0]), &Value::Null,
        "New KV key should be rolled back (not exist)");
}

#[tokio::test]
async fn test_rollback_reverts_graph_mutations() {
    let ex = test_executor();
    // Add a node before txn
    exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{}')").await;
    assert_eq!(ex.graph_store().read().node_count(), 1);

    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT GRAPH_ADD_NODE('Company', '{}')").await;
    assert_eq!(ex.graph_store().read().node_count(), 2);
    exec(&ex, "ROLLBACK").await;

    // Should be back to 1 node
    assert_eq!(ex.graph_store().read().node_count(), 1,
        "Graph node added in txn should be rolled back");
}

#[tokio::test]
async fn test_rollback_reverts_datalog_mutations() {
    let ex = test_executor();
    exec(&ex, "SELECT DATALOG_ASSERT('base_fact(a, b)')").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT DATALOG_ASSERT('txn_fact(x, y)')").await;
    exec(&ex, "ROLLBACK").await;

    // base_fact should still exist
    let r = exec(&ex, "SELECT DATALOG_QUERY('base_fact(X, Y)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("\"a\""), "base_fact should survive rollback");

    // txn_fact should be gone
    let r = exec(&ex, "SELECT DATALOG_QUERY('txn_fact(X, Y)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert_eq!(json, "[]", "txn_fact should be rolled back");
}

#[tokio::test]
async fn test_commit_preserves_cross_model_changes() {
    let ex = test_executor();

    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT kv_set('committed_key', 'committed_val')").await;
    exec(&ex, "SELECT GRAPH_ADD_NODE('Committed', '{}')").await;
    exec(&ex, "SELECT DATALOG_ASSERT('committed(a, b)')").await;
    exec(&ex, "COMMIT").await;

    // All changes should persist after COMMIT
    let r = exec(&ex, "SELECT kv_get('committed_key')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("committed_val".into()));

    assert_eq!(ex.graph_store().read().node_count(), 1);

    let r = exec(&ex, "SELECT DATALOG_QUERY('committed(X, Y)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("\"a\""), "committed fact should persist");
}

#[tokio::test]
async fn test_rollback_reverts_mixed_cross_model() {
    let ex = test_executor();
    // Setup: relational + KV + graph + datalog before txn
    exec(&ex, "CREATE TABLE items (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO items VALUES (1, 'original')").await;
    exec(&ex, "SELECT kv_set('k1', 'v1')").await;
    exec(&ex, "SELECT GRAPH_ADD_NODE('A', '{}')").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO items VALUES (2, 'txn_item')").await;
    exec(&ex, "SELECT kv_set('k1', 'overwritten')").await;
    exec(&ex, "SELECT GRAPH_ADD_NODE('B', '{}')").await;
    exec(&ex, "SELECT DATALOG_ASSERT('temp(x)')").await;
    exec(&ex, "ROLLBACK").await;

    // Relational: should have only the original row
    let r = exec(&ex, "SELECT * FROM items").await;
    assert_eq!(rows(&r[0]).len(), 1, "relational should rollback to 1 row");

    // KV: k1 should be 'v1'
    let r = exec(&ex, "SELECT kv_get('k1')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("v1".into()));

    // Graph: should have only 1 node
    assert_eq!(ex.graph_store().read().node_count(), 1);

    // Datalog: temp should be gone
    let r = exec(&ex, "SELECT DATALOG_QUERY('temp(X)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert_eq!(json, "[]");
}

// ======================================================================

// ========================================================================
// Tiered storage wiring tests
// ========================================================================

#[tokio::test]
async fn test_executor_disk_mode_opens_durable_stores() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    let ex = Executor::new_with_persistence(
        catalog,
        storage,
        None,
        Some(dir.path()),
    );
    // All three stores should have cold tiers
    assert!(ex.kv_store().has_cold_tier(), "KV store should have cold tier in disk mode");
    assert!(ex.doc_store().read().has_cold_tier(), "Doc store should have cold tier in disk mode");
    assert!(ex.graph_store().read().has_cold_tier(), "Graph store should have cold tier in disk mode");
}

#[tokio::test]
async fn test_executor_memory_mode_no_cold_tier() {
    let ex = test_executor();
    assert!(!ex.kv_store().has_cold_tier(), "KV store should not have cold tier in memory mode");
    assert!(!ex.doc_store().read().has_cold_tier(), "Doc store should not have cold tier in memory mode");
    assert!(!ex.graph_store().read().has_cold_tier(), "Graph store should not have cold tier in memory mode");
}

#[tokio::test]
async fn test_executor_disk_kv_survives_cold_tier() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    let ex = Executor::new_with_persistence(
        catalog,
        storage,
        None,
        Some(dir.path()),
    );
    // Use SQL to set a KV value
    exec(&ex, "SELECT kv_set('mykey', 'myval')").await;
    let r = exec(&ex, "SELECT kv_get('mykey')").await;
    assert_eq!(*scalar(&r[0]), Value::Text("myval".into()));
}

#[tokio::test]
async fn test_index_visibility_within_transaction() {
    // Regression: indexes were stale during explicit transactions because
    // index updates were deferred to COMMIT. A SELECT using IndexScan would
    // miss rows inserted earlier in the same transaction. Fixed by falling
    // back to SeqScan when the table is dirty within an explicit txn.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE idx_vis (id INT PRIMARY KEY, val TEXT)").await;
    exec(&ex, "CREATE INDEX idx_vis_id ON idx_vis(id)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO idx_vis VALUES (1, 'a')").await;
    exec(&ex, "INSERT INTO idx_vis VALUES (2, 'b')").await;
    exec(&ex, "INSERT INTO idx_vis VALUES (3, 'c')").await;

    // Point query by PK — should find the row even though indexes
    // haven't been rebuilt yet (because we're still in the transaction).
    let results = exec(&ex, "SELECT * FROM idx_vis WHERE id = 2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1, "should find row by PK within transaction");
    assert_eq!(r[0][1], Value::Text("b".into()));

    // Range query — should also work
    let results = exec(&ex, "SELECT * FROM idx_vis WHERE id BETWEEN 1 AND 3 ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3, "range query should find all 3 rows within transaction");

    // COUNT(*) within transaction
    let results = exec(&ex, "SELECT COUNT(*) FROM idx_vis").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(3));

    exec(&ex, "COMMIT").await;

    // After commit, index should be rebuilt and queries should still work
    let results = exec(&ex, "SELECT * FROM idx_vis WHERE id = 2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("b".into()));
}

#[tokio::test]
async fn test_index_visibility_rollback() {
    // After ROLLBACK, indexes should not contain phantom rows.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE idx_rb (id INT PRIMARY KEY, val TEXT)").await;
    exec(&ex, "CREATE INDEX idx_rb_id ON idx_rb(id)").await;

    // Pre-populate some data
    exec(&ex, "INSERT INTO idx_rb VALUES (1, 'existing')").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO idx_rb VALUES (2, 'phantom')").await;

    // Should see both rows inside transaction
    let results = exec(&ex, "SELECT COUNT(*) FROM idx_rb").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));

    exec(&ex, "ROLLBACK").await;

    // After rollback, only the original row should exist
    let results = exec(&ex, "SELECT COUNT(*) FROM idx_rb").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1));

    // Index-driven query should NOT find the rolled-back row
    let results = exec(&ex, "SELECT * FROM idx_rb WHERE id = 2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 0, "rolled-back row should not be visible");
}

// ========================================================================
