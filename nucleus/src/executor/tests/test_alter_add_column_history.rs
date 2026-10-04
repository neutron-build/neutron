//! Regression tests for the 2026-10 teploy-observe report: `ALTER TABLE … ADD
//! COLUMN … DEFAULT` on a populated store failed with
//!
//! ```text
//! corrupt tuple: page 6 slot 0 does not decode against the column types of 'llm_traces'
//! ```
//!
//! and left the table unreadable (12,080 rows) until the rows were rebuilt.
//!
//! ## Root cause
//!
//! The server's disk stack is `BufferedDiskEngine` over `DiskEngine`. A
//! migration runner sends a file as ONE transaction, and inside a transaction
//! the buffered engine queues `update()` until COMMIT — but `sync_schema()`
//! (and the catalog change beside it) take effect immediately. ADD COLUMN did
//! `catalog.update_table → scan → sync_schema → update → rebuild`, so after the
//! first ALTER in the transaction the engine's column list was one column wider
//! than every tuple on disk, while the widened rows sat in the write buffer. The
//! next statement that read the table — the second ALTER of the migration —
//! decoded old tuples against the new list and failed. The transaction aborted,
//! the buffered rows were discarded, and the catalog kept the new column(s):
//!
//! * the table was left with a longer column list than any tuple (persisted, so
//!   it survived restarts), and
//! * `ADD COLUMN IF NOT EXISTS` on the next boot saw the column in the catalog,
//!   skipped it, and the half-applied migration was recorded as applied.
//!
//! None of that needs an "old" store: it reproduced on a table created a moment
//! earlier. What it does need is (a) the disk stack, (b) a transaction, and
//! (c) a table big enough that a read after the first ALTER hits an old tuple —
//! which is why in-memory mode, autocommit, and empty tables never showed it.
//!
//! These tests nevertheless build the table with real schema history (columns
//! added and dropped across restarts, rows grown and deleted, a
//! rename-aside rebuild), because that is the shape of the store the bug was
//! found on, and because a fix that only handles a freshly created table would
//! not be evidence the history cannot matter.

use std::path::Path;
use std::sync::Arc;

use super::{exec, rows, test_executor};
use crate::catalog::Catalog;
use crate::executor::Executor;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::persistence::CatalogPersistence;
use crate::storage::{DiskEngine, StorageEngine};

const TOKEN_SOURCE: &str =
    "ALTER TABLE llm_traces ADD COLUMN IF NOT EXISTS token_source TEXT NOT NULL DEFAULT ''";
const COST_SOURCE: &str =
    "ALTER TABLE llm_traces ADD COLUMN IF NOT EXISTS cost_source TEXT NOT NULL DEFAULT ''";

/// The server's storage shape: `BufferedDiskEngine` over `DiskEngine`, with the
/// catalog persisted next to it so "restart" means what it does in production.
async fn open_server_executor(dir: &Path) -> Executor {
    let catalog_path = dir.join("catalog.json");
    let catalog = Arc::new(Catalog::new());
    CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await
        .ok();
    let disk = Arc::new(DiskEngine::open(&dir.join("nucleus.db"), catalog.clone()).unwrap());
    // main.rs re-registers every catalog table at boot, which refreshes the
    // engine's cached column layout FROM THE CATALOG. Skipping it would let the
    // layout persisted in the storage directory mask a catalog/tuple mismatch.
    for table in catalog.table_names().await {
        disk.create_table(&table).await.unwrap();
    }
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    let ex = Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir));
    ex.load_meta().await;
    ex
}

/// `INSERT` rows `lo..hi` in batches. Bodies vary in length so tuples are
/// uneven and pages fill at different rates.
async fn insert_range(ex: &Executor, lo: i64, hi: i64, cols: &str) {
    let mut id = lo;
    while id < hi {
        let end = (id + 100).min(hi);
        let values: Vec<String> = (id..end)
            .map(|i| {
                let body = "b".repeat(120 + (i % 53) as usize);
                format!(
                    "({i}, 'trace-{i}', 'model-{}', {}, {i}.25, {}, '{body}'{})",
                    i % 5,
                    i % 997,
                    i % 2 == 0,
                    if cols == "with_latency" {
                        format!(", {}", i % 31)
                    } else {
                        String::new()
                    }
                )
            })
            .collect();
        exec(
            ex,
            &format!("INSERT INTO llm_traces VALUES {}", values.join(",")),
        )
        .await;
        id = end;
    }
}

/// Order-independent fingerprint of everything the migration must not disturb.
async fn fingerprint(ex: &Executor) -> String {
    let r = ex
        .execute(
            "SELECT COUNT(*), SUM(id), SUM(prompt_tokens), SUM(LENGTH(body)), \
             MIN(trace_id), MAX(trace_id) FROM llm_traces",
        )
        .await
        .unwrap_or_else(|e| panic!("fingerprint read failed: {e}"));
    format!("{:?}", rows(&r[0]))
}

async fn column_names(ex: &Executor) -> Vec<String> {
    let r = exec(
        ex,
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'llm_traces' ORDER BY ordinal_position",
    )
    .await;
    rows(&r[0])
        .iter()
        .map(|row| match &row[0] {
            crate::types::Value::Text(s) => s.clone(),
            other => panic!("unexpected column_name {other:?}"),
        })
        .collect()
}

async fn count(ex: &Executor) -> i64 {
    let r = exec(ex, "SELECT COUNT(*) FROM llm_traces").await;
    match &rows(&r[0])[0][0] {
        crate::types::Value::Int64(n) => *n,
        crate::types::Value::Int32(n) => *n as i64,
        other => panic!("unexpected count {other:?}"),
    }
}

/// Build `llm_traces` the way an upgraded observe store looks: created narrow,
/// widened and narrowed across restarts, rows grown (forcing tuple relocation)
/// and deleted (dead slots), then rebuilt by rename-aside + copy. Ends at 8
/// columns, so the two migration ALTERs cross the 8→9 null-bitmap boundary
/// (one bitmap byte becomes two).
///
/// Returns the fingerprint of the finished table.
async fn build_aged_table(dir: &Path) -> String {
    {
        let ex = open_server_executor(dir).await;
        exec(
            &ex,
            "CREATE TABLE llm_traces (id BIGINT PRIMARY KEY, trace_id TEXT NOT NULL, \
             model TEXT, prompt_tokens INT, cost FLOAT, ok BOOLEAN, body TEXT)",
        )
        .await;
        insert_range(&ex, 0, 1500, "base").await;
        // An earlier migration: widen with a default.
        exec(
            &ex,
            "ALTER TABLE llm_traces ADD COLUMN latency_ms INT NOT NULL DEFAULT 0",
        )
        .await;
        insert_range(&ex, 1500, 2400, "with_latency").await;
    }
    {
        // Restart, then churn: grow some tuples past their slots, delete others.
        let ex = open_server_executor(dir).await;
        exec(
            &ex,
            "UPDATE llm_traces SET body = body || REPEAT('g', 90) WHERE id % 7 = 0",
        )
        .await;
        exec(&ex, "DELETE FROM llm_traces WHERE id % 10 = 3").await;
        // Add a column, populate it, then drop it again: leaves a dropped
        // column id behind and rewrites every tuple twice.
        exec(&ex, "ALTER TABLE llm_traces ADD COLUMN legacy_flag TEXT").await;
        exec(
            &ex,
            "UPDATE llm_traces SET legacy_flag = 'x' WHERE id % 4 = 0",
        )
        .await;
        exec(&ex, "ALTER TABLE llm_traces DROP COLUMN legacy_flag").await;
    }
    {
        // Restart, then the observe-convention rebuild: rename aside, create
        // anew, copy across, drop the old.
        let ex = open_server_executor(dir).await;
        exec(&ex, "ALTER TABLE llm_traces RENAME TO llm_traces_old").await;
        exec(
            &ex,
            "CREATE TABLE llm_traces (id BIGINT PRIMARY KEY, trace_id TEXT NOT NULL, \
             model TEXT, prompt_tokens INT, cost FLOAT, ok BOOLEAN, body TEXT, \
             latency_ms INT NOT NULL DEFAULT 0)",
        )
        .await;
        exec(&ex, "INSERT INTO llm_traces SELECT * FROM llm_traces_old").await;
        exec(&ex, "DROP TABLE llm_traces_old").await;
        // Post-rebuild writes land on fresh pages next to the copied ones.
        insert_range(&ex, 2400, 3000, "with_latency").await;
    }
    let ex = open_server_executor(dir).await;
    let cols = column_names(&ex).await;
    assert_eq!(cols.len(), 8, "history should end at 8 columns: {cols:?}");
    let body_bytes = exec(&ex, "SELECT SUM(LENGTH(body)) FROM llm_traces").await;
    let total = match &rows(&body_bytes[0])[0][0] {
        crate::types::Value::Int64(n) => *n,
        crate::types::Value::Int32(n) => *n as i64,
        crate::types::Value::Float64(n) => *n as i64,
        other => panic!("unexpected sum {other:?}"),
    };
    // >= 40 pages of bodies alone (more with the other columns): genuinely multi-page.
    assert!(
        total > 8192 * 40,
        "fixture too small to span pages: {total}"
    );
    fingerprint(&ex).await
}

/// THE BUG: the migration pair, run as one transaction, on an aged populated
/// disk table. Before the fix the second ALTER (or the COMMIT) failed with
/// `corrupt tuple: page N slot 0 does not decode …`.
#[tokio::test]
async fn migration_pair_in_one_transaction_on_aged_populated_table() {
    let dir = tempfile::tempdir().unwrap();
    let before = build_aged_table(dir.path()).await;

    {
        let ex = open_server_executor(dir.path()).await;
        ex.execute(&format!("BEGIN; {TOKEN_SOURCE}; {COST_SOURCE}; COMMIT"))
            .await
            .unwrap_or_else(|e| panic!("migration transaction failed: {e}"));
        assert_eq!(fingerprint(&ex).await, before, "rows changed by ALTER");
        let cols = column_names(&ex).await;
        assert_eq!(&cols[8..], ["token_source", "cost_source"]);
    }

    // After a restart the widened rows, the persisted layout, and the catalog
    // must still agree.
    let ex = open_server_executor(dir.path()).await;
    assert_eq!(fingerprint(&ex).await, before, "rows differ after restart");
    let backfilled = exec(
        &ex,
        "SELECT COUNT(*) FROM llm_traces WHERE token_source = '' AND cost_source = ''",
    )
    .await;
    assert_eq!(
        format!("{:?}", rows(&backfilled[0])),
        format!("[[Int64({})]]", count(&ex).await)
    );
    // Writes after the migration use the new shape and read back.
    exec(
        &ex,
        "INSERT INTO llm_traces VALUES \
         (9000, 't', 'm', 1, 1.5, true, 'b', 3, 'tok', 'cost')",
    )
    .await;
    let r = exec(
        &ex,
        "SELECT token_source, cost_source FROM llm_traces WHERE id = 9000",
    )
    .await;
    assert_eq!(
        format!("{:?}", rows(&r[0])),
        r#"[[Text("tok"), Text("cost")]]"#
    );
    // Re-running the migration (next boot) is a clean no-op.
    ex.execute(&format!("BEGIN; {TOKEN_SOURCE}; {COST_SOURCE}; COMMIT"))
        .await
        .unwrap();
}

/// The same migration without a transaction (autocommit), with a restart
/// between the two ALTERs. This variant never failed; it pins that the fix did
/// not trade it away.
#[tokio::test]
async fn migration_pair_autocommit_with_restart_between() {
    let dir = tempfile::tempdir().unwrap();
    let before = build_aged_table(dir.path()).await;
    {
        let ex = open_server_executor(dir.path()).await;
        exec(&ex, TOKEN_SOURCE).await;
        assert_eq!(fingerprint(&ex).await, before);
    }
    let ex = open_server_executor(dir.path()).await;
    assert_eq!(
        fingerprint(&ex).await,
        before,
        "after first ALTER + restart"
    );
    exec(&ex, COST_SOURCE).await;
    assert_eq!(fingerprint(&ex).await, before, "after second ALTER");
    drop(ex);
    let ex = open_server_executor(dir.path()).await;
    assert_eq!(fingerprint(&ex).await, before, "after second restart");
    assert_eq!(column_names(&ex).await.len(), 10);
}

/// A migration transaction that fails AFTER its ALTERs must leave the table
/// exactly readable, and must not leave the new columns recorded — otherwise the
/// next `ADD COLUMN IF NOT EXISTS` skips them and the migration is recorded as
/// applied over a table that was never widened.
#[tokio::test]
async fn failed_migration_transaction_leaves_table_readable_and_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let before = build_aged_table(dir.path()).await;
    let ex = open_server_executor(dir.path()).await;

    // The third statement fails (no such table); the whole script aborts.
    let err = ex
        .execute(&format!(
            "BEGIN; {TOKEN_SOURCE}; {COST_SOURCE}; INSERT INTO no_such_table VALUES (1); COMMIT"
        ))
        .await;
    assert!(err.is_err(), "script should have failed");
    let _ = ex.execute("ROLLBACK").await;

    // The table is readable straight away, rows untouched, and after a restart.
    assert_eq!(fingerprint(&ex).await, before, "unreadable after abort");
    drop(ex);
    let ex = open_server_executor(dir.path()).await;
    assert_eq!(fingerprint(&ex).await, before, "unreadable after restart");

    // Whatever the abort did to the column list, the retry must end with BOTH
    // columns present and every row decodable — not "recorded as applied".
    ex.execute(&format!("BEGIN; {TOKEN_SOURCE}; {COST_SOURCE}; COMMIT"))
        .await
        .unwrap();
    assert_eq!(fingerprint(&ex).await, before);
    let cols = column_names(&ex).await;
    assert!(cols.contains(&"token_source".to_string()), "{cols:?}");
    assert!(cols.contains(&"cost_source".to_string()), "{cols:?}");
    exec(&ex, "SELECT token_source, cost_source FROM llm_traces").await;
}

/// ADD COLUMN must be all-or-nothing when the backfill itself fails. The
/// default is evaluated per row, so `1/0` fails on a populated table — after the
/// catalog has already been updated.
#[tokio::test]
async fn add_column_with_failing_default_rolls_back_completely() {
    let dir = tempfile::tempdir().unwrap();
    let before = build_aged_table(dir.path()).await;
    let ex = open_server_executor(dir.path()).await;
    let cols_before = column_names(&ex).await;

    let bad = "ALTER TABLE llm_traces ADD COLUMN IF NOT EXISTS broken INT NOT NULL DEFAULT (1/0)";
    assert!(ex.execute(bad).await.is_err(), "backfill should fail");
    assert_eq!(column_names(&ex).await, cols_before, "column left behind");
    assert_eq!(fingerprint(&ex).await, before);

    // Inside a transaction too.
    assert!(
        ex.execute(&format!("BEGIN; {bad}; COMMIT")).await.is_err(),
        "backfill should fail in a transaction"
    );
    let _ = ex.execute("ROLLBACK").await;
    assert_eq!(column_names(&ex).await, cols_before);
    assert_eq!(fingerprint(&ex).await, before);

    // A retry of the same column name with a valid default works: nothing stale
    // is left for IF NOT EXISTS to trip over.
    exec(
        &ex,
        "ALTER TABLE llm_traces ADD COLUMN IF NOT EXISTS broken INT NOT NULL DEFAULT 7",
    )
    .await;
    let r = exec(&ex, "SELECT COUNT(*) FROM llm_traces WHERE broken = 7").await;
    assert_eq!(
        format!("{:?}", rows(&r[0])),
        format!("[[Int64({})]]", count(&ex).await)
    );
    drop(ex);
    let ex = open_server_executor(dir.path()).await;
    assert_eq!(fingerprint(&ex).await, before);
}

/// DROP COLUMN had the same unit-of-work flaw (catalog first, buffered rewrite,
/// immediate layout change). A read after it in the same transaction must work.
#[tokio::test]
async fn drop_then_add_column_in_one_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let before_all = build_aged_table(dir.path()).await;
    let ex = open_server_executor(dir.path()).await;
    ex.execute(&format!(
        "BEGIN; ALTER TABLE llm_traces DROP COLUMN latency_ms; {TOKEN_SOURCE}; \
         ALTER TABLE llm_traces DROP COLUMN token_source; {COST_SOURCE}; COMMIT"
    ))
    .await
    .unwrap_or_else(|e| panic!("drop/add transaction failed: {e}"));
    // Same row set; prompt_tokens/body/etc. untouched.
    assert_eq!(fingerprint(&ex).await, before_all);
    let cols = column_names(&ex).await;
    assert_eq!(cols.last().map(String::as_str), Some("cost_source"));
    assert!(!cols.contains(&"latency_ms".to_string()));
    drop(ex);
    let ex = open_server_executor(dir.path()).await;
    assert_eq!(fingerprint(&ex).await, before_all);
}

/// A table created by the same transaction that then ALTERs it. The buffered
/// `CreateTable` carried the PRE-ALTER schema, so COMMIT built the table with
/// one column too few: a debug assertion in `serialize_row` (release builds
/// would silently drop the column's values).
#[tokio::test]
async fn create_then_add_column_in_one_transaction() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_server_executor(dir.path()).await;
        ex.execute(
            "BEGIN; \
             CREATE TABLE fresh (id INT PRIMARY KEY, a TEXT); \
             INSERT INTO fresh VALUES (1, 'x'), (2, 'y'); \
             ALTER TABLE fresh ADD COLUMN b TEXT NOT NULL DEFAULT 'z'; \
             ALTER TABLE fresh ADD COLUMN c INT DEFAULT 5; \
             COMMIT",
        )
        .await
        .unwrap_or_else(|e| panic!("create+alter transaction failed: {e}"));
        exec(&ex, "INSERT INTO fresh VALUES (3, 'w', 'v', 6)").await;
    }
    let ex = open_server_executor(dir.path()).await;
    let r = exec(&ex, "SELECT * FROM fresh ORDER BY id").await;
    assert_eq!(
        format!("{:?}", rows(&r[0])),
        r#"[[Int32(1), Text("x"), Text("z"), Int32(5)], [Int32(2), Text("y"), Text("z"), Int32(5)], [Int32(3), Text("w"), Text("v"), Int32(6)]]"#
    );
}

/// Uncommitted writes to an EXISTING table are in the old shape; applying them
/// at COMMIT against the new column list would corrupt it. The ALTER is refused,
/// and the refusal must not leave the column in the catalog.
#[tokio::test]
async fn add_column_refused_over_uncommitted_writes_leaves_no_trace() {
    let dir = tempfile::tempdir().unwrap();
    let before = build_aged_table(dir.path()).await;
    let ex = open_server_executor(dir.path()).await;
    let cols_before = column_names(&ex).await;
    let sid = ex.create_session();
    ex.execute_with_session(sid, "BEGIN").await.unwrap();
    ex.execute_with_session(
        sid,
        "INSERT INTO llm_traces VALUES (7777, 't', 'm', 1, 1.5, true, 'b', 3)",
    )
    .await
    .unwrap();
    let err = ex.execute_with_session(sid, TOKEN_SOURCE).await;
    assert!(
        err.is_err(),
        "ALTER over uncommitted writes must be refused"
    );
    let _ = ex.execute_with_session(sid, "ROLLBACK").await;
    assert_eq!(column_names(&ex).await, cols_before);
    assert_eq!(fingerprint(&ex).await, before);
    // With the transaction gone, the same ALTER succeeds.
    exec(&ex, TOKEN_SOURCE).await;
    assert_eq!(fingerprint(&ex).await, before);
}

/// Memory mode shares the executor path; it never failed, and must keep working
/// through the same history-shaped sequence (DROP/ADD, transactional pair).
#[tokio::test]
async fn migration_pair_in_memory_mode() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE llm_traces (id BIGINT PRIMARY KEY, trace_id TEXT NOT NULL, \
         model TEXT, prompt_tokens INT, cost FLOAT, ok BOOLEAN, body TEXT)",
    )
    .await;
    insert_range(&ex, 0, 1500, "base").await;
    exec(
        &ex,
        "ALTER TABLE llm_traces ADD COLUMN latency_ms INT DEFAULT 0",
    )
    .await;
    exec(&ex, "DELETE FROM llm_traces WHERE id % 10 = 3").await;
    let before = fingerprint(&ex).await;
    ex.execute(&format!("BEGIN; {TOKEN_SOURCE}; {COST_SOURCE}; COMMIT"))
        .await
        .unwrap();
    assert_eq!(fingerprint(&ex).await, before);
    assert_eq!(column_names(&ex).await.len(), 10);
}

/// A store already damaged by the pre-fix bug — catalog names a column the
/// tuples never got — is repaired in place by rolling the CATALOG back to the
/// columns the tuples have, then re-running the migration on the fixed binary.
///
/// The damage is catalog-only: the failed transaction's widened rows were only
/// ever buffered and were discarded, so every tuple is still at its original
/// width. (Confirmed against a store produced by the pre-fix binary: after the
/// failed migration and a restart it listed `token_source` but not
/// `cost_source`, and every full-row read failed.) The harness reproduces that
/// state by editing `catalog.json` the way the failed ALTER left it.
///
/// Note the verification step reads WHOLE ROWS: `SELECT COUNT(*)` never decodes
/// a tuple and succeeds on the damaged table.
#[tokio::test]
async fn catalog_rollback_repairs_a_table_damaged_by_the_old_bug() {
    let dir = tempfile::tempdir().unwrap();
    let before = build_aged_table(dir.path()).await;
    let catalog_path = dir.path().join("catalog.json");
    let pristine = std::fs::read_to_string(&catalog_path).unwrap();

    // Damage: the catalog gains `token_source`; no tuple does.
    let mut doc: serde_json::Value = serde_json::from_str(&pristine).unwrap();
    for table in doc["tables"].as_array_mut().unwrap() {
        if table["name"] == "llm_traces" {
            table["columns"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "name": "token_source", "data_type": "Text", "nullable": false,
                    "default_expr": "''", "id": 9
                }));
        }
    }
    std::fs::write(&catalog_path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
    {
        let ex = open_server_executor(dir.path()).await;
        assert_eq!(count(&ex).await, 2760, "COUNT(*) does not decode tuples");
        let err = ex.execute("SELECT * FROM llm_traces").await.unwrap_err();
        assert!(err.to_string().contains("does not decode"), "{err}");
    }

    // Repair: drop the columns the tuples never had from the catalog entry.
    for table in doc["tables"].as_array_mut().unwrap() {
        if table["name"] == "llm_traces" {
            table["columns"]
                .as_array_mut()
                .unwrap()
                .retain(|c| c["name"] != "token_source" && c["name"] != "cost_source");
        }
    }
    std::fs::write(&catalog_path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
    let ex = open_server_executor(dir.path()).await;
    assert_eq!(fingerprint(&ex).await, before, "rows differ after repair");
    // And the migration now applies, in one transaction, as it should have.
    ex.execute(&format!("BEGIN; {TOKEN_SOURCE}; {COST_SOURCE}; COMMIT"))
        .await
        .unwrap();
    assert_eq!(fingerprint(&ex).await, before);
    assert_eq!(column_names(&ex).await.len(), 10);
}
