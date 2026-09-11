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
            assert!(
                tag.contains("already in a transaction"),
                "expected warning, got: {tag}"
            );
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
    let storage: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
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
    assert_eq!(
        scalar(&r[0]),
        &Value::Text("original".into()),
        "KV value should be rolled back to original"
    );

    // 'new_key' should not exist
    let r = exec(&ex, "SELECT kv_get('new_key')").await;
    assert_eq!(
        scalar(&r[0]),
        &Value::Null,
        "New KV key should be rolled back (not exist)"
    );
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
    assert_eq!(
        ex.graph_store().read().node_count(),
        1,
        "Graph node added in txn should be rolled back"
    );
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
    let ex = Executor::new_with_persistence(catalog, storage, None, Some(dir.path()));
    // All three stores should have cold tiers
    assert!(
        ex.kv_store().has_cold_tier(),
        "KV store should have cold tier in disk mode"
    );
    assert!(
        ex.doc_store().read().has_cold_tier(),
        "Doc store should have cold tier in disk mode"
    );
    assert!(
        ex.graph_store().read().has_cold_tier(),
        "Graph store should have cold tier in disk mode"
    );
}

#[tokio::test]
async fn test_executor_memory_mode_no_cold_tier() {
    let ex = test_executor();
    assert!(
        !ex.kv_store().has_cold_tier(),
        "KV store should not have cold tier in memory mode"
    );
    assert!(
        !ex.doc_store().read().has_cold_tier(),
        "Doc store should not have cold tier in memory mode"
    );
    assert!(
        !ex.graph_store().read().has_cold_tier(),
        "Graph store should not have cold tier in memory mode"
    );
}

#[tokio::test]
async fn test_executor_disk_kv_survives_cold_tier() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    let ex = Executor::new_with_persistence(catalog, storage, None, Some(dir.path()));
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
    let results = exec(
        &ex,
        "SELECT * FROM idx_vis WHERE id BETWEEN 1 AND 3 ORDER BY id",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(
        r.len(),
        3,
        "range query should find all 3 rows within transaction"
    );

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

// ── T1.3: idle-in-transaction sweep ─────────────────────────────────────────

#[cfg(feature = "server")]
#[tokio::test]
async fn idle_in_transaction_sweep_releases_and_respects_activity() {
    use std::sync::atomic::Ordering;
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE idt (id INT)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    assert!(ex.session_in_transaction(sid), "in transaction after BEGIN");

    // Disabled (timeout 0) never sweeps.
    assert_eq!(ex.sweep_idle_in_transaction(0).await, 0);
    // Fresh activity: a large timeout finds nothing.
    assert_eq!(ex.sweep_idle_in_transaction(60_000).await, 0);
    assert!(
        ex.session_in_transaction(sid),
        "not idle long enough — must stay in transaction"
    );

    // Backdate last activity so the session looks abandoned, then sweep.
    ex.get_session(sid)
        .last_activity_ms
        .store(0, Ordering::Relaxed);
    assert_eq!(
        ex.sweep_idle_in_transaction(1).await,
        1,
        "an idle-in-transaction session must be rolled back"
    );
    assert!(
        !ex.session_in_transaction(sid),
        "the transaction (and its snapshot) must be released"
    );
    // Idempotent: nothing left to sweep.
    assert_eq!(ex.sweep_idle_in_transaction(1).await, 0);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn idle_sweep_skips_executing_session() {
    use std::sync::atomic::Ordering;
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    // Looks idle by timestamp, but a command is in flight (a long query): the
    // sweep must not mistake it for abandoned.
    let sess = ex.get_session(sid);
    sess.last_activity_ms.store(0, Ordering::Relaxed);
    sess.executing.store(true, Ordering::Relaxed);
    assert_eq!(
        ex.sweep_idle_in_transaction(1).await,
        0,
        "an executing session must never be swept"
    );
    assert!(
        ex.session_in_transaction(sid),
        "transaction must remain open"
    );
}

// ======================================================================
// NU-217: lock contention is not information about the transaction
//
// `session_in_transaction` used `txn_state.try_read().unwrap_or(false)`, so a
// write-locked state answered "not in a transaction". Its callers are exactly
// the guards that disable the wire layer's autocommit fast paths inside a
// transaction — paths that write straight to storage and survive ROLLBACK — so
// contention could open the guard on a session that was mid-transaction.
//
// It cannot simply take the lock: `txn_state` is a tokio lock and two of the
// three callers are synchronous (`sync_transaction_status` and a `Drop` impl).
// The answer is an atomic mirror written in the same critical section.
// ======================================================================

#[cfg(feature = "server")]
#[tokio::test]
async fn in_transaction_probe_is_correct_while_the_state_is_write_locked() {
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    assert!(ex.session_in_transaction(sid));

    // Hold the write lock, exactly as a concurrent BEGIN/COMMIT would.
    let session = ex.get_session(sid);
    let guard = session.txn_state.write().await;
    assert!(
        ex.session_in_transaction(sid),
        "a write-locked transaction state was reported as no transaction — \
         the wire layer would take an autocommit fast path inside this txn"
    );
    drop(guard);

    ex.execute_with_session(sid, "ROLLBACK").await.unwrap();
    assert!(!ex.session_in_transaction(sid));
}

/// The mirror is only safe while it agrees with the state it mirrors. Every
/// path that ends a transaction is exercised here; a new one that forgets to
/// update the flag fails this test rather than silently opening the guard.
#[cfg(feature = "server")]
#[tokio::test]
async fn txn_active_mirrors_state() {
    async fn agrees(ex: &Executor, sid: u64, expect: bool, where_: &str) {
        let session = ex.get_session(sid);
        let under_lock = session.txn_state.read().await.active;
        let mirror = ex.session_in_transaction(sid);
        assert_eq!(under_lock, expect, "txn_state.active after {where_}");
        assert_eq!(mirror, expect, "mirror after {where_}");
    }

    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE m217 (id INT)")
        .await
        .unwrap();
    agrees(&ex, sid, false, "session creation").await;

    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    agrees(&ex, sid, true, "BEGIN").await;
    ex.execute_with_session(sid, "COMMIT").await.unwrap();
    agrees(&ex, sid, false, "COMMIT").await;

    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    ex.execute_with_session(sid, "INSERT INTO m217 VALUES (1)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "ROLLBACK").await.unwrap();
    agrees(&ex, sid, false, "ROLLBACK").await;

    // Session reset (connection reuse) also clears it.
    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    agrees(&ex, sid, true, "BEGIN before reset").await;
    ex.get_session(sid).reset().await;
    agrees(&ex, sid, false, "Session::reset").await;

    // And the idle-in-transaction sweep, which rolls back from outside.
    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    ex.get_session(sid)
        .last_activity_ms
        .store(0, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(ex.sweep_idle_in_transaction(1).await, 1);
    agrees(&ex, sid, false, "the idle-in-transaction sweep").await;
}

// ======================================================================
// DISCARD ALL vs an active transaction (audit A21)
// ======================================================================

/// DISCARD ALL must refuse inside a transaction instead of destroying the
/// transaction's undo bookkeeping (`*txn = TxnState::new()` dropped
/// engine_snapshots, security_pending and savepoints with no rollback, so the
/// transaction's writes stayed applied). PostgreSQL refuses with "DISCARD ALL
/// cannot run inside a transaction block"; ROLLBACK must still restore.
#[tokio::test]
async fn discard_all_refuses_inside_transaction_and_rollback_still_restores() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE disc (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO disc VALUES (1)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO disc VALUES (2)").await;
    let err = ex
        .execute("DISCARD ALL")
        .await
        .expect_err("DISCARD ALL must refuse inside a transaction");
    assert!(
        err.to_string().contains("cannot run inside a transaction block"),
        "wrong refusal: {err}"
    );

    // The refusal is a statement error like any other: the transaction is
    // aborted until ROLLBACK.
    assert!(
        ex.execute("SELECT 1").await.is_err(),
        "statement after the refused DISCARD must report the aborted transaction"
    );

    exec(&ex, "ROLLBACK").await;
    let after = rows(&exec(&ex, "SELECT id FROM disc").await[0]).clone();
    assert_eq!(
        after,
        vec![vec![Value::Int32(1)]],
        "ROLLBACK after the refused DISCARD must still restore the pre-BEGIN rows"
    );

    // Idle DISCARD ALL keeps working and still clears prepared statements.
    exec(&ex, "PREPARE p AS SELECT 1").await;
    exec(&ex, "EXECUTE p").await;
    exec(&ex, "DISCARD ALL").await;
    assert!(
        ex.execute("EXECUTE p").await.is_err(),
        "DISCARD ALL must still deallocate prepared statements when idle"
    );
}

// ======================================================================
// ROLLBACK TO SAVEPOINT vs the aborted state (audit A11)
// ======================================================================

/// A successful ROLLBACK TO SAVEPOINT must clear `aborted` — only BEGIN and
/// the savepoint restore used to, so a transaction that errored and then
/// restored to a savepoint kept answering 25P02 for every later statement
/// despite the successful restore. Missing savepoints and failed restores
/// must leave the flag set.
#[tokio::test]
async fn rollback_to_savepoint_clears_aborted_state() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE spab (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO spab VALUES (1)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "SAVEPOINT s").await;
    exec(&ex, "INSERT INTO spab VALUES (2)").await;
    // A statement error aborts the transaction.
    assert!(
        ex.execute("INSERT INTO spab VALUES (1)").await.is_err(),
        "duplicate PK must error"
    );
    assert!(
        ex.execute("SELECT id FROM spab").await.is_err(),
        "statement after the error must report 25P02 (aborted)"
    );

    // ROLLBACK TO SAVEPOINT is itself admitted while aborted (it is a
    // transaction-end statement for the gate) and clears the flag.
    exec(&ex, "ROLLBACK TO SAVEPOINT s").await;
    let after = rows(&exec(&ex, "SELECT id FROM spab").await[0]).clone();
    assert_eq!(
        after,
        vec![vec![Value::Int32(1)]],
        "restored savepoint state must be visible after clearing aborted"
    );
    exec(&ex, "INSERT INTO spab VALUES (3)").await;
    exec(&ex, "COMMIT").await;
    let committed = rows(&exec(&ex, "SELECT id FROM spab ORDER BY id").await[0]).clone();
    assert_eq!(
        committed,
        vec![vec![Value::Int32(1)], vec![Value::Int32(3)]],
        "writes after the savepoint restore must be committable"
    );
}

/// The flag survives a ROLLBACK TO a savepoint that does not exist: that is a
/// failed statement inside an already-aborted transaction, not a restore.
#[tokio::test]
async fn rollback_to_missing_savepoint_keeps_aborted_state() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE spmiss (id INT)").await;
    exec(&ex, "BEGIN").await;
    assert!(ex.execute("SELECT 1/0").await.is_err());
    assert!(
        ex.execute("ROLLBACK TO SAVEPOINT nope").await.is_err(),
        "rolling back to a missing savepoint must error"
    );
    assert!(
        ex.execute("SELECT 1").await.is_err(),
        "a failed savepoint restore must keep the transaction aborted"
    );
    exec(&ex, "ROLLBACK").await;
}

// ======================================================================
// Cancelled statement futures must not leak depth/locks (audit A12)
// ======================================================================

/// The wire layer drops statement futures (pgwire CancelRequest `select!`,
/// statement-timeout `tokio::time::timeout`). The depth counter used to be
/// fetch_add → await → fetch_sub with no cancellation guard, so the dropped
/// statement leaked its increment: row locks were never released at depth 1
/// and the error-state handling never ran. Drive the same drop here with a
/// real parked statement — an autocommit claim that has taken one row lock
/// and is waiting on another session's.
#[cfg(feature = "server")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_statement_releases_depth_and_row_locks() {
    use std::sync::atomic::Ordering;

    let ex = test_executor();
    exec(&ex, "CREATE TABLE cl (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO cl VALUES (1), (2)").await;

    // Session A holds row 2 for the life of its transaction.
    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "SELECT id FROM cl WHERE id = 2 FOR UPDATE")
        .await
        .unwrap();

    // Session B's autocommit claim takes row 1 (sorted first), then parks
    // waiting for A's lock on row 2 — mid-statement, at depth 1.
    let b = ex.create_session();
    let claim = ex.execute_with_session(b, "SELECT id FROM cl WHERE id IN (1, 2) FOR UPDATE");
    let parked =
        tokio::time::timeout(std::time::Duration::from_millis(300), claim).await;
    assert!(parked.is_err(), "claim should be parked on A's row lock");

    // The dropped future must have returned the depth to zero...
    assert_eq!(
        ex.get_session(b).statement_depth.load(Ordering::SeqCst),
        0,
        "cancelled statement leaked its depth increment"
    );
    // ...and released the row it had already locked (autocommit: the
    // statement IS the transaction).
    assert_eq!(
        ex.row_locks.session_held_count(b),
        0,
        "row lock from the cancelled statement was never released"
    );
    // So a third session can claim that row right now.
    let c = ex.create_session();
    ex.execute_with_session(c, "SELECT id FROM cl WHERE id = 1 FOR UPDATE NOWAIT")
        .await
        .expect("row 1 must be claimable after the cancelled statement");

    ex.drop_session(a);
    ex.drop_session(b);
    ex.drop_session(c);
}

/// Cancellation must also drive the error-state cleanup a failed statement
/// gets: an open transaction whose statement future is dropped mid-flight is
/// aborted until ROLLBACK, because the statement's outcome is unknown.
#[cfg(feature = "server")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_statement_aborts_open_transaction() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ca (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO ca VALUES (1), (2)").await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "SELECT id FROM ca WHERE id = 2 FOR UPDATE")
        .await
        .unwrap();

    let b = ex.create_session();
    ex.execute_with_session(b, "BEGIN").await.unwrap();
    let claim = ex.execute_with_session(b, "SELECT id FROM ca WHERE id = 2 FOR UPDATE");
    let parked =
        tokio::time::timeout(std::time::Duration::from_millis(300), claim).await;
    assert!(parked.is_err(), "claim should be parked on A's row lock");

    let rejected = ex.execute_with_session(b, "SELECT 1").await;
    let rejected_desc = rejected.map(|_| ()).unwrap_err().to_string();
    assert!(
        rejected_desc.contains("aborted"),
        "a cancelled statement must abort its transaction, got {rejected_desc}"
    );
    // ROLLBACK ends the aborted transaction and releases its row locks.
    ex.execute_with_session(b, "ROLLBACK").await.unwrap();
    ex.execute_with_session(a, "ROLLBACK").await.unwrap();
    assert_eq!(ex.row_locks.session_held_count(b), 0);
    ex.drop_session(a);
    ex.drop_session(b);
}

/// A mid-transaction disconnect must also release the MVCC storage
/// transaction: `drop_storage_session` only removed the session state, and
/// dropping an Active storage txn without aborting it left its id in the
/// transaction manager's active set forever — pinning the GC watermark for
/// the life of the process (the same unbounded-growth failure the
/// idle-in-transaction sweep exists to prevent).
#[cfg(feature = "server")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_session_releases_the_storage_transaction() {
    use crate::storage::MvccStorageAdapter;

    let adapter = std::sync::Arc::new(MvccStorageAdapter::new());
    let storage: std::sync::Arc<dyn crate::storage::StorageEngine> = adapter.clone();
    let ex = Executor::new(std::sync::Arc::new(crate::catalog::Catalog::new()), storage);
    exec(&ex, "CREATE TABLE gcpin (id INT)").await;
    let baseline = adapter.txn_mgr().active_count();

    let sid = ex.create_session();
    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    ex.execute_with_session(sid, "INSERT INTO gcpin VALUES (1)")
        .await
        .unwrap();
    assert_eq!(
        adapter.txn_mgr().active_count(),
        baseline + 1,
        "the open transaction must be in the active set"
    );

    ex.drop_session(sid);
    assert_eq!(
        adapter.txn_mgr().active_count(),
        baseline,
        "drop_session leaked an active storage transaction — the GC watermark stays pinned"
    );
}

// ======================================================================
// Policy publication is tied to the commit decision (audit A7)
// ======================================================================

/// A FAILED commit must publish nothing and wipe nothing. Policy used to be
/// published to the shared catalog and persisted BEFORE `commit_txn`; on a
/// commit failure the code restored the BEGIN-era whole-catalog snapshot —
/// erasing OTHER sessions' policy DDL committed since this BEGIN — and a
/// crash in the publish→persist→commit window left durable policy the
/// transaction never committed. A deterministic commit failure via a
/// SERIALIZABLE write-skew drives the branch end to end.
#[cfg(feature = "server")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_commit_publishes_no_policy_and_keeps_other_sessions_changes() {
    use crate::storage::MvccStorageAdapter;

    let adapter = std::sync::Arc::new(MvccStorageAdapter::new());
    let storage: std::sync::Arc<dyn crate::storage::StorageEngine> = adapter.clone();
    let ex = Executor::new(std::sync::Arc::new(crate::catalog::Catalog::new()), storage);
    exec(&ex, "CREATE TABLE guarded (id INT, owner TEXT)").await;
    exec(&ex, "CREATE TABLE skew (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)").await;
    exec(&ex, "INSERT INTO skew VALUES (1,1),(2,1)").await;

    // T stages policy DDL and its read half of the write skew.
    let t = ex.create_session();
    ex.execute_with_session(t, "BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap();
    ex.execute_with_session(t, "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    ex.execute_with_session(
        t,
        "CREATE POLICY t_policy ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await
    .unwrap();
    ex.execute_with_session(t, "SELECT v FROM skew WHERE id = 2")
        .await
        .unwrap();

    // Another session commits its own policy DDL after T staged, closes the
    // write-skew cycle, and commits successfully (first committer wins).
    let other = ex.create_session();
    ex.execute_with_session(other, "BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap();
    ex.execute_with_session(
        other,
        "CREATE POLICY other_policy ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await
    .unwrap();
    ex.execute_with_session(other, "SELECT v FROM skew WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(other, "UPDATE skew SET v = 0 WHERE id = 2")
        .await
        .unwrap();
    ex.execute_with_session(other, "COMMIT").await.unwrap();

    // T's write half, then a COMMIT that loses the write-skew race.
    ex.execute_with_session(t, "UPDATE skew SET v = 0 WHERE id = 1")
        .await
        .unwrap();
    let failed = ex.execute_with_session(t, "COMMIT").await;
    assert!(failed.is_err(), "the write-skew commit must fail");

    // Published catalog: other session's DDL intact, T's staged DDL absent.
    let names = rows(&exec(&ex, "SELECT policyname FROM pg_policies").await[0]).clone();
    let listed: Vec<String> = names
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    assert_eq!(
        listed,
        vec!["other_policy".to_string()],
        "a failed commit must neither publish its own policy nor wipe another \
         session's committed policy"
    );

    // T's transaction survived the failure: ROLLBACK runs cleanly and clears
    // the staged catalog without publishing it.
    ex.execute_with_session(t, "ROLLBACK").await.unwrap();
    let names = rows(&exec(&ex, "SELECT policyname FROM pg_policies").await[0]).clone();
    assert_eq!(names.len(), 1);
    ex.drop_session(t);
    ex.drop_session(other);
}

// ======================================================================
// Successful COMMIT merges staged policy with concurrent changes (audit A6)
// ======================================================================

/// The staged security catalog is a whole-catalog clone taken at this
/// session's first policy write, so publishing it wholesale used to silently
/// revert any policy another session committed after this BEGIN — the A7 fix
/// closed the failure path; this is the success path's residual. COMMIT must
/// merge this session's deltas onto the live catalog: both sessions' policies
/// survive.
#[tokio::test]
async fn commit_merges_staged_policy_with_concurrent_committed_policy() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE guarded (id INT PRIMARY KEY, owner TEXT)").await;
    exec(&ex, "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY").await;

    let stager = ex.create_session();
    ex.execute_with_session(stager, "BEGIN").await.unwrap();
    // First policy write of the session: the whole-catalog clone is staged
    // HERE, before the concurrent commit below.
    ex.execute_with_session(
        stager,
        "CREATE POLICY staged_p ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await
    .unwrap();

    // A concurrent autocommit session commits its own policy AFTER the
    // staging above: wholesale publication would revert it.
    exec(
        &ex,
        "CREATE POLICY concurrent_p ON guarded TO PUBLIC USING (id >= 0)",
    )
    .await;

    ex.execute_with_session(stager, "COMMIT").await.unwrap();

    let names = rows(&exec(&ex, "SELECT policyname FROM pg_policies ORDER BY policyname").await[0]).clone();
    let listed: Vec<String> = names
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    assert_eq!(
        listed,
        vec!["concurrent_p".to_string(), "staged_p".to_string()],
        "COMMIT must publish this session's policy without reverting the \
         concurrently committed one"
    );
    ex.drop_session(stager);
}

/// The same merge for RLS enablement: a concurrent DISABLE committed while
/// this session held a staged catalog must not be undone by the staged
/// catalog's stale enabled-bit.
#[tokio::test]
async fn commit_merges_staged_policy_with_concurrent_rls_toggle() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE guarded (id INT PRIMARY KEY, owner TEXT)").await;
    exec(&ex, "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY").await;

    let stager = ex.create_session();
    ex.execute_with_session(stager, "BEGIN").await.unwrap();
    ex.execute_with_session(
        stager,
        "CREATE POLICY staged_p ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await
    .unwrap();

    // Concurrent autocommit: disable RLS on a DIFFERENT table than the staged
    // policy's — the staged clone still carries it as enabled, and wholesale
    // publication would silently re-enable it.
    exec(&ex, "ALTER TABLE guarded DISABLE ROW LEVEL SECURITY").await;

    ex.execute_with_session(stager, "COMMIT").await.unwrap();

    // The staged policy published, and the concurrent disable survived it.
    assert!(
        ex.security.read().rls.policy("guarded", "staged_p").is_some(),
        "the staged policy must publish"
    );
    assert!(
        !ex.security.read().rls.is_enabled("guarded"),
        "the concurrent DISABLE must survive the staged catalog's publication"
    );
    ex.drop_session(stager);
}
