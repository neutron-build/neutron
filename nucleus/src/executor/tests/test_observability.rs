//! M11 observability ledger — the surfaces the incident runbook relies on.
//!
//! Each test here pins one exposure named by the M11 items: maintenance
//! CHECKPOINT from SQL, WAL/checkpoint status outside the replication
//! manager, open-transaction state per session, and degraded-state reporting
//! through `SHOW SUBSYSTEM_HEALTH`. The common failure mode these guard
//! against is *declared-but-unwired*: a surface that exists in the registry
//! or the runbook but returns nothing (or lies) when an operator asks for it
//! mid-incident.

use std::path::Path;
use std::sync::Arc;

use super::super::Executor;
use super::{exec, rows, test_executor};
use crate::catalog::Catalog;
use crate::storage::persistence::CatalogPersistence;
use crate::storage::{DiskEngine, StorageEngine};
use crate::types::Value;

/// Open a durable executor with 1 MB WAL segments, mirroring the server's
/// segmented-WAL configuration, so WAL traffic is observable on disk.
async fn disk_executor(dir: &Path) -> (Executor, Arc<DiskEngine>) {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());
    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog(&catalog).await.ok();
    let engine = Arc::new(
        DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog.clone(),
            64,
            1,
            crate::storage::wal::SyncMode::Fsync,
        )
        .unwrap(),
    );
    let storage: Arc<dyn StorageEngine> = engine.clone();
    let ex = Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir));
    ex.load_meta().await;
    (ex, engine)
}

/// `CHECKPOINT` must be a real maintenance command that drives the storage
/// engine's existing checkpoint path (flush dirty pages, checkpoint record,
/// truncate reclaimed segments) — the admission docs already promise it as
/// the degraded-mode recovery path, so the surface has to exist.
#[tokio::test]
async fn checkpoint_command_drives_storage_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, engine) = disk_executor(dir.path()).await;
    exec(&ex, "CREATE TABLE t (id INT)").await;
    for i in 0..20 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i})")).await;
    }
    engine.flush().unwrap();
    let before = engine.current_wal_lsn();
    assert!(before > 0, "traffic must advance the WAL before CHECKPOINT");

    let results = ex.execute("CHECKPOINT").await.expect("CHECKPOINT must run");
    match &results[0] {
        crate::executor::ExecResult::Command { tag, .. } => {
            assert_eq!(tag, "CHECKPOINT");
        }
        other => panic!("CHECKPOINT should return a command tag, got {other:?}"),
    }
    // A checkpoint writes a checkpoint record, so the LSN must advance.
    assert!(
        engine.current_wal_lsn() > before,
        "CHECKPOINT must write a checkpoint record (lsn {} -> {})",
        before,
        engine.current_wal_lsn()
    );
}

/// WAL/checkpoint status must be visible without a replication manager
/// (single node): current LSN, checkpoint horizon, and on-disk WAL size —
/// the numbers the incident runbook uses to diagnose WAL growth.
#[tokio::test]
async fn show_wal_status_reports_lsns_and_size() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, engine) = disk_executor(dir.path()).await;
    exec(&ex, "CREATE TABLE t (id INT)").await;
    for i in 0..20 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i})")).await;
    }
    engine.flush().unwrap();

    let results = exec(&ex, "SHOW WAL_STATUS").await;
    let r = rows(&results[0]);
    let metrics: std::collections::HashMap<String, String> = r
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Text(k), Value::Text(v)) => (k.clone(), v.clone()),
            other => panic!("expected text pairs, got {other:?}"),
        })
        .collect();
    let current: u64 = metrics["current_lsn"].parse().unwrap();
    assert!(current > 0, "current_lsn must be positive, got {current}");
    assert!(
        metrics.contains_key("checkpoint_lsn"),
        "checkpoint horizon must be reported"
    );
    let size: u64 = metrics["wal_size_bytes"].parse().unwrap();
    assert!(size > 0, "WAL traffic must leave bytes on disk, got {size}");

    // After CHECKPOINT the horizon must advance to at least the traffic LSN.
    exec(&ex, "CHECKPOINT").await;
    let results = exec(&ex, "SHOW WAL_STATUS").await;
    let r = rows(&results[0]);
    let metrics: std::collections::HashMap<String, String> = r
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Text(k), Value::Text(v)) => (k.clone(), v.clone()),
            other => panic!("expected text pairs, got {other:?}"),
        })
        .collect();
    let horizon: u64 = metrics["checkpoint_lsn"].parse().unwrap();
    assert!(
        horizon >= current,
        "checkpoint_lsn {horizon} must cover pre-checkpoint lsn {current}"
    );
}

/// Open-transaction state per session: an abandoned BEGIN must be findable
/// (session id + idle age) — the runbook's "database growing without bound"
/// triage — instead of only the aggregate `nucleus_open_transactions` count.
#[tokio::test]
async fn show_transactions_lists_open_transactions_with_idle_age() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT)").await;

    // No transactions open: no rows.
    let results = exec(&ex, "SHOW TRANSACTIONS").await;
    assert!(rows(&results[0]).is_empty(), "no open transactions yet");

    // One session with an open transaction, one without.
    let idle_session = ex.create_session();
    ex.execute_with_session(idle_session, "BEGIN")
        .await
        .unwrap();
    exec(&ex, "INSERT INTO t VALUES (1)").await; // autocommit, not open afterwards

    let results = exec(&ex, "SHOW TRANSACTIONS").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1, "exactly one open transaction, got {r:?}");
    assert_eq!(r[0][0], Value::Int64(idle_session as i64));
    assert_eq!(r[0][1], Value::Bool(true), "transaction_active");

    ex.execute_with_session(idle_session, "ROLLBACK")
        .await
        .unwrap();
    let results = exec(&ex, "SHOW TRANSACTIONS").await;
    assert!(
        rows(&results[0]).is_empty(),
        "ROLLBACK closes the transaction"
    );
}

/// The memory subsystem is registered and degraded by the RSS watchdog, so
/// `SHOW SUBSYSTEM_HEALTH` must be able to report it. At HEAD the fixed
/// subsystem list omitted "memory": the watchdog could mark it degraded and
/// the operator surface stayed all-healthy.
#[tokio::test]
async fn subsystem_health_reports_memory_degraded() {
    let ex = test_executor();
    ex.health_registry()
        .write()
        .mark_degraded("memory", "rss past critical threshold (test)");

    let results = exec(&ex, "SHOW SUBSYSTEM_HEALTH").await;
    let r = rows(&results[0]);
    let memory = r
        .iter()
        .find(|row| row[0] == Value::Text("memory".into()))
        .unwrap_or_else(|| panic!("memory row missing from SHOW SUBSYSTEM_HEALTH: {r:?}"));
    assert_eq!(memory[1], Value::Text("degraded".into()));

    ex.health_registry().write().mark_healthy("memory");
    let results = exec(&ex, "SHOW SUBSYSTEM_HEALTH").await;
    let r = rows(&results[0]);
    let memory = r
        .iter()
        .find(|row| row[0] == Value::Text("memory".into()))
        .unwrap();
    assert_eq!(memory[1], Value::Text("healthy".into()));
}

/// The disk watermark degrades the service to read-only; that state must be
/// visible in `SHOW SUBSYSTEM_HEALTH` too, not only in 53100 refusals.
#[tokio::test]
async fn subsystem_health_reports_disk_degraded() {
    let ex = test_executor();
    ex.health_registry()
        .write()
        .mark_degraded("disk", "free space below readonly watermark (test)");

    let results = exec(&ex, "SHOW SUBSYSTEM_HEALTH").await;
    let r = rows(&results[0]);
    let disk = r
        .iter()
        .find(|row| row[0] == Value::Text("disk".into()))
        .unwrap_or_else(|| panic!("disk row missing from SHOW SUBSYSTEM_HEALTH: {r:?}"));
    assert_eq!(disk[1], Value::Text("degraded".into()));
}

/// CHECKPOINT is one of the two documented recovery paths out of a disk
/// watermark (the other is VACUUM), so a degraded read-only server must
/// still run it — refusing it would make the degraded state unrecoverable
/// from SQL.
#[tokio::test]
async fn checkpoint_is_admissible_while_degraded_read_only() {
    use crate::ops::DegradeReason;

    let ex = test_executor();
    let service = ex.service().clone();
    service.enter_read_only(DegradeReason::DiskWatermark, String::from("test watermark"));
    let results = ex
        .execute("CHECKPOINT")
        .await
        .expect("CHECKPOINT must stay available while degraded");
    match &results[0] {
        crate::executor::ExecResult::Command { tag, .. } => assert_eq!(tag, "CHECKPOINT"),
        other => panic!("expected command tag, got {other:?}"),
    }

    // The read-only observability surfaces stay admitted too.
    ex.execute("SHOW WAL_STATUS")
        .await
        .expect("SHOW WAL_STATUS while degraded");
    ex.execute("SHOW TRANSACTIONS")
        .await
        .expect("SHOW TRANSACTIONS while degraded");
}

/// The slow-query threshold is a session setting in milliseconds, disabled
/// by default, and unparseable values disable rather than mis-log.
#[tokio::test]
async fn slow_query_threshold_is_session_local_ms_and_off_by_default() {
    let ex = test_executor();
    assert_eq!(ex.slow_query_log_ms(), 0, "disabled by default");

    exec(&ex, "SET slow_query_log_ms = 250").await;
    assert_eq!(ex.slow_query_log_ms(), 250);

    exec(&ex, "SET slow_query_log_ms = 0").await;
    assert_eq!(ex.slow_query_log_ms(), 0, "0 disables");

    exec(&ex, "SET slow_query_log_ms = 'not-a-number'").await;
    assert_eq!(
        ex.slow_query_log_ms(),
        0,
        "an unparseable value must disable, not guess"
    );

    // Other sessions are unaffected: the knob is per-session.
    let other = ex.create_session();
    ex.execute_with_session(other, "SET slow_query_log_ms = 250")
        .await
        .unwrap();
    assert_eq!(ex.slow_query_log_ms(), 0, "default session still disabled");
}

/// A statement over the threshold must be recorded with its query id and
/// duration; below the threshold (or disabled) nothing is recorded.
#[tokio::test]
async fn slow_query_log_records_statement_over_threshold() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT)").await;
    for i in 0..1500 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i})")).await;
    }

    // Disabled: a 1500x1500 cartesian COUNT (2.25M evaluated pairs) records
    // nothing even though it is slow in wall-clock terms.
    let heavy = "SELECT COUNT(*) FROM t a, t b WHERE a.id + b.id >= 0";
    exec(&ex, heavy).await;
    assert!(
        ex.last_slow_query().is_none(),
        "nothing may be recorded while the threshold is disabled"
    );

    // 1 ms threshold: the same query must cross it, and the record must
    // carry a positive query id, the duration, and the statement.
    exec(&ex, "SET slow_query_log_ms = 1").await;
    exec(&ex, heavy).await;
    let (query_id, duration_ms, statement) = ex
        .last_slow_query()
        .expect("a 2.25M-pair debug-mode cartesian count must cross a 1ms threshold");
    assert!(query_id > 0, "query ids start at 1");
    assert!(
        duration_ms >= 1.0,
        "recorded duration {duration_ms} must be over the threshold"
    );
    assert!(
        statement.to_uppercase().starts_with("SELECT"),
        "statement preview must be the executed statement, got {statement:?}"
    );
}
