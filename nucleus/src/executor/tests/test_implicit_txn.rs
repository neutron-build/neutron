//! X08: a multi-statement simple query is one implicit transaction, data
//! included. Before the fix only the SET state was scoped to the message:
//! `insert 1; insert 2; <failing insert>` kept both rows, and an in-message
//! ROLLBACK did not undo data. Each test runs on the non-MVCC memory engine
//! (before-image rollback) and on the MVCC adapter.

use super::*;

fn engines() -> Vec<(&'static str, Executor)> {
    let mvcc: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
    vec![
        ("memory", test_executor()),
        ("mvcc", Executor::new(Arc::new(Catalog::new()), mvcc)),
    ]
}

async fn count(ex: &Executor, sid: u64, table: &str) -> i64 {
    let r = ex
        .execute_with_session(sid, &format!("SELECT count(*) FROM {table}"))
        .await
        .unwrap()
        .remove(0);
    match scalar(&r) {
        Value::Int64(n) => *n,
        Value::Int32(n) => i64::from(*n),
        other => panic!("count: {other:?}"),
    }
}

async fn setup(ex: &Executor) -> u64 {
    exec(ex, "CREATE TABLE x08_t (id INT PRIMARY KEY)").await;
    ex.create_session()
}

#[tokio::test]
async fn failing_statement_keeps_no_row_of_the_message() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        let r = ex
            .execute_with_session(
                sid,
                "INSERT INTO x08_t VALUES (1); INSERT INTO x08_t VALUES (2); \
                 INSERT INTO x08_t VALUES (1)",
            )
            .await;
        assert!(r.is_err(), "{name}: duplicate key must fail the message");
        assert_eq!(count(&ex, sid, "x08_t").await, 0, "{name}");
        // The connection is usable and not left inside a transaction.
        assert!(!ex.get_session(sid).txn_active.load(Ordering::SeqCst));
        exec_on(&ex, sid, "INSERT INTO x08_t VALUES (1)").await;
        assert_eq!(count(&ex, sid, "x08_t").await, 1, "{name}");
    }
}

async fn exec_on(ex: &Executor, sid: u64, q: &str) {
    ex.execute_with_session(sid, q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

#[tokio::test]
async fn successful_message_commits_every_statement() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        exec_on(
            &ex,
            sid,
            "INSERT INTO x08_t VALUES (1); INSERT INTO x08_t VALUES (2)",
        )
        .await;
        assert_eq!(count(&ex, sid, "x08_t").await, 2, "{name}");
        assert!(!ex.get_session(sid).txn_active.load(Ordering::SeqCst));
        // A second session sees the committed rows.
        let other = ex.create_session();
        assert_eq!(count(&ex, other, "x08_t").await, 2, "{name}");
    }
}

/// PostgreSQL 17: `insert 10; rollback; insert 11; select count(*)` -> 1.
#[tokio::test]
async fn in_message_rollback_undoes_earlier_data() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        let r = ex
            .execute_with_session(
                sid,
                "INSERT INTO x08_t VALUES (10); ROLLBACK; INSERT INTO x08_t VALUES (11); \
                 SELECT count(*) FROM x08_t",
            )
            .await
            .unwrap();
        assert!(matches!(
            scalar(r.last().unwrap()),
            Value::Int64(1) | Value::Int32(1)
        ));
        assert_eq!(count(&ex, sid, "x08_t").await, 1, "{name}");
    }
}

/// PostgreSQL 17: a COMMIT inside the message keeps what came before it; the
/// block that follows is rolled back by a later error.
#[tokio::test]
async fn in_message_commit_ends_the_block() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        let r = ex
            .execute_with_session(
                sid,
                "INSERT INTO x08_t VALUES (1); COMMIT; INSERT INTO x08_t VALUES (2); \
                 INSERT INTO x08_t VALUES (2)",
            )
            .await;
        assert!(r.is_err(), "{name}");
        assert_eq!(count(&ex, sid, "x08_t").await, 1, "{name}");
    }
}

/// PostgreSQL 17: a BEGIN inside the message converts the block, earlier
/// statements included, and leaves it open after the message.
#[tokio::test]
async fn in_message_begin_converts_the_block_to_an_explicit_transaction() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        exec_on(
            &ex,
            sid,
            "INSERT INTO x08_t VALUES (1); BEGIN; INSERT INTO x08_t VALUES (2)",
        )
        .await;
        let session = ex.get_session(sid);
        assert!(session.txn_active.load(Ordering::SeqCst), "{name}");
        assert!(!session.implicit_txn.load(Ordering::SeqCst), "{name}");
        assert_eq!(count(&ex, sid, "x08_t").await, 2, "{name}");
        exec_on(&ex, sid, "ROLLBACK").await;
        assert_eq!(count(&ex, sid, "x08_t").await, 0, "{name}");
    }
}

/// A message cancelled mid-flight (statement timeout, CancelRequest) leaves
/// neither its writes nor an open transaction behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_message_rolls_back_its_data() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x08_t (id INT PRIMARY KEY)").await;
    exec(&ex, "CREATE TABLE x08_locked (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO x08_locked VALUES (1)").await;
    let sid = ex.create_session();

    let holder = ex.create_session();
    exec_on(&ex, holder, "BEGIN").await;
    exec_on(
        &ex,
        holder,
        "SELECT id FROM x08_locked WHERE id = 1 FOR UPDATE",
    )
    .await;

    let blocked = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        ex.execute_with_session(
            sid,
            "INSERT INTO x08_t VALUES (1); \
             SELECT id FROM x08_locked WHERE id = 1 FOR UPDATE",
        ),
    )
    .await;
    assert!(blocked.is_err(), "the message must still be blocked");
    assert!(!ex.get_session(sid).txn_active.load(Ordering::SeqCst));
    assert_eq!(count(&ex, sid, "x08_t").await, 0);
}

#[tokio::test]
async fn begin_modes_apply_at_the_start_of_a_multi_statement_message() {
    let mvcc: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
    let ex = Executor::new(Arc::new(Catalog::new()), mvcc);
    let sid = ex.create_session();
    let results = ex
        .execute_with_session(
            sid,
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY; \
             SHOW transaction_isolation; SHOW transaction_read_only; ROLLBACK",
        )
        .await
        .unwrap();
    assert_eq!(scalar(&results[1]), &Value::Text("serializable".into()));
    assert_eq!(scalar(&results[2]), &Value::Text("on".into()));
}

#[tokio::test]
async fn begin_cannot_change_isolation_after_an_earlier_query_in_the_message() {
    let mvcc: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
    let ex = Executor::new(Arc::new(Catalog::new()), mvcc);
    let sid = ex.create_session();
    let result = ex
        .execute_with_session(sid, "SELECT 1; BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await;
    assert!(
        result.is_err(),
        "PostgreSQL refuses a late isolation change"
    );
    // An explicit block may remain aborted; ROLLBACK always restores usability.
    exec_on(&ex, sid, "ROLLBACK").await;
    exec_on(&ex, sid, "SELECT 1").await;
}

#[tokio::test]
async fn read_only_rejects_dml_ddl_locks_and_mutating_scalar_functions() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        exec_on(&ex, sid, "INSERT INTO x08_t VALUES (1)").await;
        for sql in [
            "INSERT INTO x08_t VALUES (2)",
            "UPDATE x08_t SET id = 2",
            "DELETE FROM x08_t",
            "CREATE TABLE x08_no_create (id INT)",
            "SELECT id FROM x08_t FOR UPDATE",
            "SELECT kv_set('x08_ro', 'forbidden')",
        ] {
            exec_on(&ex, sid, "BEGIN READ ONLY").await;
            let err = ex.execute_with_session(sid, sql).await.unwrap_err();
            assert!(
                matches!(err, ExecError::ReadOnly(_)),
                "{name}: {sql}: {err:?}"
            );
            exec_on(&ex, sid, "ROLLBACK").await;
        }
        assert_eq!(count(&ex, sid, "x08_t").await, 1, "{name}");
        assert!(ex.catalog.get_table("x08_no_create").await.is_none());
    }
}

#[tokio::test]
async fn session_transaction_defaults_are_scoped_by_rollback() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        exec_on(&ex, sid, "BEGIN").await;
        exec_on(
            &ex,
            sid,
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        )
        .await;
        exec_on(&ex, sid, "ROLLBACK").await;
        exec_on(&ex, sid, "INSERT INTO x08_t VALUES (1)").await;
        assert_eq!(count(&ex, sid, "x08_t").await, 1, "{name}");
    }
}

#[tokio::test]
async fn plain_begin_uses_read_committed_visibility() {
    let mvcc: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
    let ex = Executor::new(Arc::new(Catalog::new()), mvcc);
    let reader = setup(&ex).await;
    let writer = ex.create_session();
    exec_on(&ex, reader, "BEGIN").await;
    assert_eq!(count(&ex, reader, "x08_t").await, 0);
    exec_on(&ex, writer, "INSERT INTO x08_t VALUES (1)").await;
    assert_eq!(count(&ex, reader, "x08_t").await, 1);
    exec_on(&ex, reader, "ROLLBACK").await;
}

#[tokio::test]
async fn begin_read_write_overrides_read_only_session_default_in_a_message() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        exec_on(
            &ex,
            sid,
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        )
        .await;
        exec_on(
            &ex,
            sid,
            "BEGIN READ WRITE; INSERT INTO x08_t VALUES (1); COMMIT",
        )
        .await;
        assert_eq!(count(&ex, sid, "x08_t").await, 1, "{name}");
    }
}

#[tokio::test]
async fn session_characteristics_override_an_earlier_local_default_on_commit() {
    for (name, ex) in engines() {
        let sid = setup(&ex).await;
        exec_on(&ex, sid, "BEGIN").await;
        exec_on(&ex, sid, "SET LOCAL default_transaction_read_only = on").await;
        exec_on(
            &ex,
            sid,
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        )
        .await;
        exec_on(&ex, sid, "COMMIT").await;
        let result = ex
            .execute_with_session(sid, "SHOW default_transaction_read_only")
            .await
            .unwrap();
        assert_eq!(scalar(&result[0]), &Value::Text("on".into()), "{name}");
    }
}

#[tokio::test]
async fn generic_transaction_mode_settings_fail_clearly_instead_of_lying() {
    let ex = test_executor();
    let sid = ex.create_session();
    for sql in [
        "SET transaction_isolation = 'read committed'",
        "SET transaction_read_only = on",
    ] {
        let error = ex.execute_with_session(sid, sql).await.unwrap_err();
        assert!(matches!(error, ExecError::Unsupported(_)), "{sql}: {error}");
    }
}

struct PausingRestoreEngine {
    inner: crate::storage::MemoryEngine,
    pause_scan: std::sync::atomic::AtomicBool,
    pause_only_when_aborted: bool,
}

#[async_trait::async_trait]
impl StorageEngine for PausingRestoreEngine {
    async fn create_table(&self, table: &str) -> Result<(), crate::storage::StorageError> {
        self.inner.create_table(table).await
    }
    async fn drop_table(&self, table: &str) -> Result<(), crate::storage::StorageError> {
        self.inner.drop_table(table).await
    }
    async fn insert(
        &self,
        table: &str,
        row: crate::types::Row,
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.insert(table, row).await
    }
    async fn scan(
        &self,
        table: &str,
    ) -> Result<Vec<crate::types::Row>, crate::storage::StorageError> {
        self.inner.scan(table).await
    }
    async fn scan_physical(
        &self,
        table: &str,
    ) -> Result<Vec<(usize, crate::types::Row)>, crate::storage::StorageError> {
        let aborted = crate::executor::session::CURRENT_SESSION
            .try_with(|session| session.txn_state.try_read().is_ok_and(|txn| txn.aborted))
            .unwrap_or(false);
        if (!self.pause_only_when_aborted || aborted)
            && self.pause_scan.swap(false, Ordering::SeqCst)
        {
            std::future::pending::<()>().await;
        }
        self.inner.scan_physical(table).await
    }
    async fn delete(
        &self,
        table: &str,
        positions: &[usize],
    ) -> Result<usize, crate::storage::StorageError> {
        self.inner.delete(table, positions).await
    }
    async fn update(
        &self,
        table: &str,
        updates: &[(usize, crate::types::Row)],
    ) -> Result<usize, crate::storage::StorageError> {
        self.inner.update(table, updates).await
    }
}

#[tokio::test]
async fn cancelled_rollback_keeps_before_images_until_restoration_finishes() {
    let storage = Arc::new(PausingRestoreEngine {
        inner: crate::storage::MemoryEngine::new(),
        pause_scan: false.into(),
        pause_only_when_aborted: false,
    });
    let ex = Executor::new(Arc::new(Catalog::new()), storage.clone());
    let sid = setup(&ex).await;
    exec_on(&ex, sid, "BEGIN").await;
    exec_on(&ex, sid, "INSERT INTO x08_t VALUES (1)").await;
    storage.pause_scan.store(true, Ordering::SeqCst);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            ex.execute_with_session(sid, "ROLLBACK")
        )
        .await
        .is_err()
    );
    assert!(ex.get_session(sid).txn_active.load(Ordering::SeqCst));
    assert!(
        !ex.get_session(sid)
            .txn_state
            .read()
            .await
            .engine_snapshots
            .is_empty()
    );
    exec_on(&ex, sid, "ROLLBACK").await;
    assert_eq!(count(&ex, sid, "x08_t").await, 0);
}

#[tokio::test]
async fn cancelled_implicit_close_remains_armed_through_rollback() {
    let storage = Arc::new(PausingRestoreEngine {
        inner: crate::storage::MemoryEngine::new(),
        pause_scan: false.into(),
        pause_only_when_aborted: true,
    });
    let ex = Executor::new(Arc::new(Catalog::new()), storage.clone());
    let sid = setup(&ex).await;
    // The first scan_physical belongs to rollback after the duplicate-key
    // error. Drop must finish this cancelled implicit close synchronously.
    storage.pause_scan.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        ex.execute_with_session(
            sid,
            "INSERT INTO x08_t VALUES (1); INSERT INTO x08_t VALUES (1)",
        ),
    )
    .await;
    assert!(result.is_err());
    assert!(!ex.get_session(sid).txn_active.load(Ordering::SeqCst));
    assert_eq!(count(&ex, sid, "x08_t").await, 0);
}

#[tokio::test]
async fn x08_disk_refuses_isolation_it_cannot_enforce() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(
        crate::storage::disk_engine::DiskEngine::open(
            &dir.path().join("isolation.db"),
            catalog.clone(),
        )
        .unwrap(),
    );
    let storage = Arc::new(crate::storage::buffered_engine::BufferedDiskEngine::new(
        disk,
    ));
    let ex = Executor::new(catalog, storage);
    for level in ["REPEATABLE READ", "SERIALIZABLE"] {
        let sid = ex.create_session();
        let result = ex
            .execute_with_session(sid, &format!("BEGIN ISOLATION LEVEL {level}"))
            .await;
        assert!(
            matches!(result, Err(ExecError::Unsupported(_))),
            "{level} was accepted"
        );
        assert!(!ex.get_session(sid).txn_active.load(Ordering::SeqCst));
        let result = ex
            .execute_with_session(
                sid,
                &format!("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}"),
            )
            .await;
        assert!(matches!(result, Err(ExecError::Unsupported(_))));
    }
}

/// Rollback repair must run even while its cancellation recovery state remains
/// active. A failed message previously left a one-row zone map for its ghost
/// insert, which pruned the surviving committed row on a range scan.
#[tokio::test]
async fn failed_message_rebuilds_derived_state_from_restored_rows() {
    for (name, ex) in engines() {
        let sid = ex.create_session();
        exec_on(
            &ex,
            sid,
            "CREATE TABLE x08_derived (id INT PRIMARY KEY, val INT)",
        )
        .await;
        exec_on(&ex, sid, "INSERT INTO x08_derived VALUES (3, 17), (11, 11)").await;
        exec_on(&ex, sid, "DELETE FROM x08_derived WHERE id = 11").await;
        let err = ex
            .execute_with_session(
                sid,
                "INSERT INTO x08_derived VALUES (12, 12); INSERT INTO x08_derived VALUES (12, 12)",
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{name}: {err}");
        let result = ex
            .execute_with_session(sid, "SELECT id FROM x08_derived WHERE val > 13")
            .await
            .unwrap();
        assert_eq!(
            rows(&result[0]),
            &vec![vec![Value::Int32(3)]],
            "{name}: ghost granule pruned committed data"
        );
        assert!(!ex.get_session(sid).txn_active.load(Ordering::SeqCst));
    }
}
