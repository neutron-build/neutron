//! observe-nucleus `llm_traces`, 2026-09-24: an in-place-upgraded table that
//! no v1.1.1 read could decode, and an ADD COLUMN that failed half-applied.
//!
//! The on-disk history, rebuilt here byte for byte:
//!
//! 1. Rows written by a pre-NU-239 engine, which stored a Text `''` handed to
//!    a JSONB column with the Text layout: a zero length prefix and no bytes.
//!    Those engines read it back as the JSON null document; NU-239's strict
//!    decode called it corruption, so every full read of the table failed on
//!    the first such row (`page 6 slot 0 does not decode against the column
//!    types of 'llm_traces'`).
//! 2. Migration 054's `ALTER TABLE llm_traces ADD COLUMN IF NOT EXISTS` pair.
//!    ADD COLUMN persisted the widened catalog BEFORE its backfill scan, the
//!    scan hit (1) and failed, and the catalog kept the column. The re-run's
//!    `IF NOT EXISTS` skipped it, so the migration ledger recorded 054 applied
//!    over rows that never received either column.
//! 3. Rows inserted after that at the catalog's (wider) width, so the table
//!    holds tuples of two widths.
//!
//! The fix: the legacy empty payload decodes as what it always meant, ADD
//! COLUMN no longer persists the catalog ahead of a backfill that can fail,
//! and `REPAIR TABLE` widens rows an interrupted ADD COLUMN left short.

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

use nucleus::catalog::{Catalog, ColumnDef};
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::wal::SyncMode;
use nucleus::storage::{DiskEngine, StorageEngine};
use nucleus::types::{DataType, Value};

struct Stack {
    exec: Arc<Executor>,
    disk: Arc<DiskEngine>,
    catalog: Arc<Catalog>,
}

/// Server-shaped boot (segmented-WAL DiskEngine under BufferedDiskEngine,
/// catalog persistence), as main.rs does it.
async fn boot(data: &Path) -> Stack {
    std::fs::create_dir_all(data).unwrap();
    let catalog = Arc::new(Catalog::new());
    let catalog_path = data.join("catalog.json");
    let _ = CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await;
    let db_path = data.join("nucleus.db");
    let disk = Arc::new(
        DiskEngine::open_segmented_with_sync(&db_path, catalog.clone(), 1024, 16, SyncMode::Fsync)
            .unwrap(),
    );
    for table in catalog.table_names().await {
        let _ = disk.create_table(&table).await;
    }
    let buffered = Arc::new(BufferedDiskEngine::new(disk.clone()));
    let exec = Arc::new(Executor::new_with_persistence(
        catalog.clone(),
        buffered as Arc<dyn StorageEngine>,
        Some(catalog_path),
        Some(data),
    ));
    exec.restore_table_engines().await;
    Stack {
        exec,
        disk,
        catalog,
    }
}

async fn shutdown(stack: Stack) {
    stack.disk.checkpoint().unwrap();
    drop(stack);
}

async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().unwrap()
}

async fn rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

async fn columns(ex: &Executor, table: &str) -> Vec<String> {
    rows(
        ex,
        &format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '{table}' ORDER BY ordinal_position"
        ),
    )
    .await
    .into_iter()
    .map(|r| match &r[0] {
        Value::Text(s) => s.clone(),
        other => panic!("column_name {other:?}"),
    })
    .collect()
}

/// The 009 shape, trimmed to the columns that matter: TEXT columns, a BIGINT,
/// and the JSONB `metadata` last.
const CREATE: &str = "CREATE TABLE llm_traces (
    trace_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    prompt TEXT NOT NULL DEFAULT '',
    metadata JSONB
)";

const MIGRATION_054: [&str; 2] = [
    "ALTER TABLE llm_traces ADD COLUMN IF NOT EXISTS token_source TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE llm_traces ADD COLUMN IF NOT EXISTS cost_source TEXT NOT NULL DEFAULT ''",
];

/// What a pre-NU-239 engine put on disk for `metadata = ''`: a Text value in
/// the JSONB slot. Written at the storage layer, below the executor's
/// Text->JSONB coercion that refuses `''` today.
async fn insert_legacy_row(disk: &DiskEngine, id: &str, ts: i64) {
    disk.insert(
        "llm_traces",
        vec![
            Value::Text(id.into()),
            Value::Text("s1".into()),
            Value::Int64(ts),
            Value::Text("Hello".into()),
            Value::Text(String::new()),
        ],
    )
    .await
    .unwrap();
}

/// Steps 1-3 of the module doc, ending with a restart, so the directory on
/// disk is exactly the live one's shape.
async fn build_live_history(data: &Path) {
    let s = boot(data).await;
    exec(&s.exec, CREATE).await;
    for i in 0..200 {
        insert_legacy_row(&s.disk, &format!("legacy-{i}"), 1_700_000_000_000 + i).await;
    }
    exec(
        &s.exec,
        "INSERT INTO llm_traces VALUES ('modern-0', 's1', 1800000000000, 'p', '{\"k\":1}'), \
         ('modern-1', 's1', 1800000000001, 'p', NULL)",
    )
    .await;

    // Migration 054 as v1.1.1 ran it: the catalog learned both columns, the
    // backfill never ran.
    let mut widened = (*s.catalog.get_table("llm_traces").await.unwrap()).clone();
    for name in ["token_source", "cost_source"] {
        let id = widened.next_column_id();
        widened.columns.push(ColumnDef {
            name: name.into(),
            data_type: DataType::Text,
            nullable: false,
            default_expr: Some("''".into()),
            id,
            analyzer: None,
        });
    }
    s.catalog.update_table(widened).await.unwrap();
    CatalogPersistence::new(&data.join("catalog.json"))
        .save_catalog(&s.catalog)
        .await
        .unwrap();
    shutdown(s).await;

    // Ingest kept running at the catalog's width.
    let s = boot(data).await;
    exec(
        &s.exec,
        "INSERT INTO llm_traces VALUES \
         ('after-0', 's1', 1900000000000, 'p', NULL, 'reported', 'estimated')",
    )
    .await;
    shutdown(s).await;
}

/// The legacy JSONB spelling alone — no ALTER involved — made the table
/// unreadable. It must read back as the null document, which is what the
/// engines that wrote it returned.
#[tokio::test]
async fn legacy_empty_jsonb_rows_read_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let s = boot(tmp.path()).await;
    exec(&s.exec, CREATE).await;
    insert_legacy_row(&s.disk, "legacy-0", 1).await;
    exec(
        &s.exec,
        "INSERT INTO llm_traces VALUES ('modern-0', 's1', 2, 'p', '{\"k\":1}')",
    )
    .await;
    shutdown(s).await;

    let s = boot(tmp.path()).await;
    let got = rows(
        &s.exec,
        "SELECT trace_id, metadata FROM llm_traces ORDER BY trace_id",
    )
    .await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0][0], Value::Text("legacy-0".into()));
    assert_eq!(got[0][1], Value::Jsonb(serde_json::Value::Null));
    assert_eq!(got[1][1], Value::Jsonb(serde_json::json!({"k": 1})));

    // And migration 054 now completes on the table as it stands.
    for sql in MIGRATION_054 {
        exec(&s.exec, sql).await;
    }
    let got = rows(
        &s.exec,
        "SELECT trace_id, token_source, cost_source FROM llm_traces ORDER BY trace_id",
    )
    .await;
    assert_eq!(got.len(), 2);
    for row in &got {
        assert_eq!(row[1], Value::Text(String::new()));
        assert_eq!(row[2], Value::Text(String::new()));
    }
}

/// A failing ADD COLUMN must leave the catalog as it found it — in memory and
/// across a restart — so a re-run of `ADD COLUMN IF NOT EXISTS` does the work
/// instead of skipping it.
#[tokio::test]
async fn failed_add_column_does_not_persist_the_column() {
    let tmp = tempfile::tempdir().unwrap();
    let s = boot(tmp.path()).await;
    exec(&s.exec, CREATE).await;
    // A genuinely corrupt JSONB payload (non-empty, not JSON): still reported.
    s.disk
        .insert(
            "llm_traces",
            vec![
                Value::Text("bad".into()),
                Value::Text("s1".into()),
                Value::Int64(1),
                Value::Text("p".into()),
                Value::Text("~not json".into()),
            ],
        )
        .await
        .unwrap();

    let err = s
        .exec
        .execute(MIGRATION_054[0])
        .await
        .expect_err("the backfill scan must fail on a corrupt row");
    assert!(
        err.to_string().contains("does not decode"),
        "unexpected error: {err}"
    );
    assert!(
        !columns(&s.exec, "llm_traces")
            .await
            .contains(&"token_source".to_string()),
        "a failed ADD COLUMN left the column in the catalog"
    );
    shutdown(s).await;

    let s = boot(tmp.path()).await;
    assert!(
        !columns(&s.exec, "llm_traces")
            .await
            .contains(&"token_source".to_string()),
        "a failed ADD COLUMN left the column in the persisted catalog"
    );
}

/// The live residue: legacy rows, a catalog wider than the rows, and newer
/// rows at the wider width. `REPAIR TABLE` makes every row readable, keeps
/// every value that was stored, fills the added columns with their defaults,
/// and is idempotent.
#[tokio::test]
async fn repair_table_widens_rows_an_interrupted_add_column_left_short() {
    let tmp = tempfile::tempdir().unwrap();
    build_live_history(tmp.path()).await;

    let s = boot(tmp.path()).await;
    assert_eq!(
        columns(&s.exec, "llm_traces").await,
        [
            "trace_id",
            "site_id",
            "timestamp",
            "prompt",
            "metadata",
            "token_source",
            "cost_source"
        ]
    );
    // The broken state, as found live: count(*) answers, reads do not.
    assert_eq!(
        rows(&s.exec, "SELECT count(*) FROM llm_traces").await[0][0],
        Value::Int64(203)
    );
    assert!(
        s.exec
            .execute("SELECT trace_id, cost_source FROM llm_traces")
            .await
            .is_err(),
        "rows short of the catalog must not read as if they were whole"
    );

    let report = rows(&s.exec, "REPAIR TABLE llm_traces").await;
    assert_eq!(
        report,
        vec![vec![
            Value::Text("llm_traces".into()),
            Value::Int64(202),
            Value::Int64(0),
        ]],
        "200 legacy + 2 pre-ALTER rows widened; the post-ALTER row was already whole"
    );

    let got = rows(
        &s.exec,
        "SELECT trace_id, metadata, token_source, cost_source FROM llm_traces \
         ORDER BY timestamp",
    )
    .await;
    assert_eq!(got.len(), 203);
    assert_eq!(got[0][0], Value::Text("legacy-0".into()));
    assert_eq!(got[0][1], Value::Jsonb(serde_json::Value::Null));
    assert_eq!(got[0][2], Value::Text(String::new()));
    assert_eq!(got[0][3], Value::Text(String::new()));
    assert_eq!(got[200][0], Value::Text("modern-0".into()));
    assert_eq!(got[200][1], Value::Jsonb(serde_json::json!({"k": 1})));
    assert_eq!(got[201][1], Value::Null);
    assert_eq!(got[202][0], Value::Text("after-0".into()));
    assert_eq!(got[202][2], Value::Text("reported".into()));
    assert_eq!(got[202][3], Value::Text("estimated".into()));

    // Idempotent, and migration 054 re-runs cleanly on the repaired table.
    assert_eq!(
        rows(&s.exec, "REPAIR TABLE llm_traces").await,
        vec![vec![
            Value::Text("llm_traces".into()),
            Value::Int64(0),
            Value::Int64(0),
        ]]
    );
    for sql in MIGRATION_054 {
        exec(&s.exec, sql).await;
    }
    exec(
        &s.exec,
        "INSERT INTO llm_traces VALUES \
         ('after-1', 's1', 1900000000001, 'p', NULL, 'estimated', 'reported')",
    )
    .await;
    shutdown(s).await;

    // Durable: the widened rows survive a restart.
    let s = boot(tmp.path()).await;
    let got = rows(
        &s.exec,
        "SELECT count(*), count(metadata), min(cost_source), max(token_source) FROM llm_traces",
    )
    .await;
    assert_eq!(got[0][0], Value::Int64(204));
    assert_eq!(
        rows(&s.exec, "REPAIR TABLE llm_traces").await[0][1],
        Value::Int64(0)
    );
}

/// Real damage is still damage: REPAIR refuses, names the row, and writes
/// nothing.
#[tokio::test]
async fn repair_table_refuses_corruption_and_changes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let s = boot(tmp.path()).await;
    exec(&s.exec, CREATE).await;
    insert_legacy_row(&s.disk, "legacy-0", 1).await;
    s.disk
        .insert(
            "llm_traces",
            vec![
                Value::Text("bad".into()),
                Value::Text("s1".into()),
                Value::Int64(2),
                Value::Text("p".into()),
                Value::Text("~not json".into()),
            ],
        )
        .await
        .unwrap();
    let err = s
        .exec
        .execute("REPAIR TABLE llm_traces")
        .await
        .expect_err("a row that fits no width is corruption");
    assert!(err.to_string().contains("decodes at no width"), "{err}");
}
