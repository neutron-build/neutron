//! Buffered disk engine wrapper — adds transaction atomicity to DiskEngine.
//!
//! Wraps a `DiskEngine` with write buffering: during an explicit transaction
//! (`BEGIN`), all inserts/deletes/updates are buffered in memory. On `COMMIT`,
//! the buffered operations are applied atomically to the underlying engine.
//! On `ROLLBACK`, the buffer is discarded. In auto-commit mode (no explicit
//! transaction), writes pass through directly.
//!
//! This provides:
//! - Transaction atomicity (all-or-nothing commit)
//! - Rollback support (currently impossible on bare DiskEngine)
//! - Scan isolation: uncommitted writes are visible within the transaction
//!   but not to other sessions
//!
//! Transactions are PER-SESSION, keyed by the `STORAGE_SESSION_ID` task-local:
//! each connection gets its own buffer, so an abandoned transaction on one
//! connection can neither block another connection's BEGIN nor swallow its
//! writes. (The original single-global-buffer design did both: a client that
//! disconnected mid-transaction left the buffer active forever, and every
//! later connection's writes were silently buffered into the orphan and lost.)
//!
//! Limitations:
//! - No full MVCC snapshot isolation between concurrent sessions (buffered
//!   writes are invisible to others, but reads see the inner engine's latest
//!   committed state rather than a stable snapshot)
//! - Buffered data is in memory — very large transactions may use significant RAM

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use super::disk_engine::DiskEngine;
use super::{StorageEngine, StorageError};
use crate::types::{Row, Value};

/// A buffered write operation within a transaction.
#[derive(Debug, Clone)]
enum BufferedOp {
    Insert {
        table: String,
        row: Row,
    },
    Delete {
        table: String,
        positions: Vec<usize>,
    },
    Update {
        table: String,
        updates: Vec<(usize, Row)>,
    },
    CreateTable {
        table: String,
    },
    DropTable {
        table: String,
    },
}

/// Transaction state — holds buffered operations until commit/abort.
struct TxnBuffer {
    ops: Vec<BufferedOp>,
}

impl TxnBuffer {
    fn new() -> Self {
        Self { ops: Vec::new() }
    }
}

/// Wraps [`DiskEngine`] with per-session transaction write buffering.
pub struct BufferedDiskEngine {
    inner: Arc<DiskEngine>,
    /// Per-session transaction buffers, keyed by storage session id.
    /// An entry exists iff that session has an explicit transaction open.
    txn_bufs: RwLock<HashMap<u64, TxnBuffer>>,
}

/// The storage session id of the current execution context (0 = default /
/// embedded).
fn current_session_id() -> u64 {
    #[cfg(feature = "server")]
    {
        super::STORAGE_SESSION_ID.try_with(|&id| id).unwrap_or(0)
    }
    #[cfg(not(feature = "server"))]
    {
        super::get_storage_session_id()
    }
}

impl BufferedDiskEngine {
    pub fn new(inner: Arc<DiskEngine>) -> Self {
        Self {
            inner,
            txn_bufs: RwLock::new(HashMap::new()),
        }
    }

    /// Get the underlying DiskEngine for direct access (flush, buffer pool, etc.).
    pub fn inner(&self) -> &Arc<DiskEngine> {
        &self.inner
    }

    fn is_in_txn(&self) -> bool {
        self.txn_bufs.read().contains_key(&current_session_id())
    }

    /// Apply all buffered operations to the underlying engine.
    async fn apply_buffer(&self, ops: Vec<BufferedOp>) -> Result<(), StorageError> {
        for op in ops {
            match op {
                BufferedOp::Insert { table, row } => {
                    self.inner.insert(&table, row).await?;
                }
                BufferedOp::Delete { table, positions } => {
                    self.inner.delete(&table, &positions).await?;
                }
                BufferedOp::Update { table, updates } => {
                    self.inner.update(&table, &updates).await?;
                }
                BufferedOp::CreateTable { table } => {
                    self.inner.create_table(&table).await?;
                }
                BufferedOp::DropTable { table } => {
                    self.inner.drop_table(&table).await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageEngine for BufferedDiskEngine {
    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.ops.push(BufferedOp::CreateTable {
                table: table.to_string(),
            });
            return Ok(());
        }
        self.inner.create_table(table).await
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.ops.push(BufferedOp::DropTable {
                table: table.to_string(),
            });
            return Ok(());
        }
        self.inner.drop_table(table).await
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.ops.push(BufferedOp::Insert {
                table: table.to_string(),
                row,
            });
            return Ok(());
        }
        self.inner.insert(table, row).await
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        let mut rows = self.inner.scan(table).await?;

        // If this session is in a transaction, apply ITS buffered ops so it
        // reads its own uncommitted writes (other sessions' buffers stay
        // invisible).
        let bufs = self.txn_bufs.read();
        if let Some(txn) = bufs.get(&current_session_id()) {
            for op in &txn.ops {
                match op {
                    BufferedOp::Insert { table: t, row } if t == table => {
                        rows.push(row.clone());
                    }
                    BufferedOp::Delete {
                        table: t,
                        positions,
                    } if t == table => {
                        // Mark deleted positions (apply in reverse order to handle shifts)
                        let mut deleted = vec![false; rows.len()];
                        for &pos in positions {
                            if pos < deleted.len() {
                                deleted[pos] = true;
                            }
                        }
                        rows = rows
                            .into_iter()
                            .enumerate()
                            .filter(|(i, _)| !deleted.get(*i).copied().unwrap_or(false))
                            .map(|(_, r)| r)
                            .collect();
                    }
                    BufferedOp::Update { table: t, updates } if t == table => {
                        for (pos, new_row) in updates {
                            if *pos < rows.len() {
                                rows[*pos] = new_row.clone();
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(rows)
    }

    async fn scan_limit(&self, table: &str, limit: usize) -> Result<Vec<Row>, StorageError> {
        // With a live transaction buffer the limit must be applied to the
        // buffered view (buffered inserts/deletes shift which rows are the
        // "first n"), so fall back to the full materialize-then-truncate path.
        // In auto-commit mode there is no buffer, so delegate to the inner
        // engine's early-exit scan.
        if self.is_in_txn() {
            let mut rows = self.scan(table).await?;
            rows.truncate(limit);
            return Ok(rows);
        }
        self.inner.scan_limit(table, limit).await
    }

    fn fast_count_all(&self, table: &str) -> Option<usize> {
        let mut count = self.inner.fast_count_all(table)?;
        // Adjust for this session's buffered transaction ops
        let bufs = self.txn_bufs.read();
        if let Some(txn) = bufs.get(&current_session_id()) {
            for op in &txn.ops {
                match op {
                    BufferedOp::Insert { table: t, .. } if t == table => count += 1,
                    BufferedOp::Delete {
                        table: t,
                        positions,
                    } if t == table => {
                        count = count.saturating_sub(positions.len());
                    }
                    _ => {}
                }
            }
        }
        Some(count)
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.ops.push(BufferedOp::Delete {
                table: table.to_string(),
                positions: positions.to_vec(),
            });
            return Ok(positions.len());
        }
        self.inner.delete(table, positions).await
    }

    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError> {
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.ops.push(BufferedOp::Update {
                table: table.to_string(),
                updates: updates.to_vec(),
            });
            return Ok(updates.len());
        }
        self.inner.update(table, updates).await
    }

    async fn sync_schema(&self, table: &str) -> Result<(), StorageError> {
        self.inner.sync_schema(table).await
    }

    async fn rebuild_table_indexes(&self, table: &str) -> Result<(), StorageError> {
        self.inner.rebuild_table_indexes(table).await
    }

    // -- Transaction lifecycle --

    async fn begin_txn(&self) -> Result<(), StorageError> {
        let id = current_session_id();
        let mut bufs = self.txn_bufs.write();
        if bufs.contains_key(&id) {
            return Err(StorageError::Io("transaction already active".into()));
        }
        bufs.insert(id, TxnBuffer::new());
        Ok(())
    }

    async fn commit_txn(&self) -> Result<(), StorageError> {
        let ops = {
            let mut bufs = self.txn_bufs.write();
            match bufs.remove(&current_session_id()) {
                Some(txn) => txn.ops,
                None => return Ok(()), // no active txn — no-op
            }
        };
        self.apply_buffer(ops).await?;
        // COMMIT is the durability point for the buffered ops just applied.
        // The executor's statement-level make_durable skipped them while the
        // transaction was open (writes were only in the in-memory buffer), so
        // force the WAL here before COMMIT is acked.
        self.inner.make_durable().await
    }

    async fn abort_txn(&self) -> Result<(), StorageError> {
        // Discard this session's buffered operations.
        self.txn_bufs.write().remove(&current_session_id());
        Ok(())
    }

    fn drop_storage_session(&self, id: u64) {
        // A client that disconnects mid-transaction must not leave an orphaned
        // buffer behind — it would hold "transaction already active" against
        // the session id forever.
        self.txn_bufs.write().remove(&id);
    }

    fn supports_mvcc(&self) -> bool {
        true // We provide transaction atomicity + rollback
    }

    async fn make_durable(&self) -> Result<(), StorageError> {
        if self.is_in_txn() {
            // Writes are buffered in memory until COMMIT — nothing has
            // reached the inner engine yet, so there is nothing to force.
            // commit_txn() forces after applying the buffer.
            return Ok(());
        }
        self.inner.make_durable().await
    }

    fn durability_pending(&self) -> bool {
        !self.is_in_txn() && self.inner.durability_pending()
    }

    async fn flush_schema(&self) -> Result<(), StorageError> {
        self.inner.flush_schema().await
    }

    // -- Delegate everything else to inner DiskEngine --

    async fn create_index(
        &self,
        table: &str,
        index_name: &str,
        col_idx: usize,
    ) -> Result<(), StorageError> {
        self.inner.create_index(table, index_name, col_idx).await
    }

    async fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        self.inner.drop_index(index_name).await
    }

    async fn index_lookup(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.inner.index_lookup(table, index_name, value).await
    }

    async fn index_lookup_range(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.inner
            .index_lookup_range(table, index_name, low, high)
            .await
    }

    fn index_lookup_sync(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.inner.index_lookup_sync(table, index_name, value)
    }

    fn index_lookup_range_sync(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.inner
            .index_lookup_range_sync(table, index_name, low, high)
    }

    fn index_only_scan(
        &self,
        table: &str,
        index_name: &str,
        eq_value: Option<&Value>,
        range: Option<(&Value, &Value)>,
    ) -> Option<Vec<Row>> {
        self.inner
            .index_only_scan(table, index_name, eq_value, range)
    }

    async fn flush_all_dirty(&self) -> Result<(), StorageError> {
        self.inner.flush_all_dirty().await
    }

    async fn vacuum(&self, table: &str) -> Result<(usize, usize, usize, usize), StorageError> {
        self.inner.vacuum(table).await
    }

    async fn vacuum_all(&self) -> Result<(usize, usize, usize, usize), StorageError> {
        self.inner.vacuum_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef, TableDef};
    use crate::storage::disk_engine::DiskEngine;
    use crate::types::{DataType, Value};

    async fn setup(path: &std::path::Path) -> (Arc<BufferedDiskEngine>, Arc<Catalog>) {
        let catalog = Arc::new(Catalog::new());
        let disk = Arc::new(DiskEngine::open(path, catalog.clone()).unwrap());
        let engine = Arc::new(BufferedDiskEngine::new(disk));
        // Register a test table
        catalog
            .create_table(TableDef {
                name: "t".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Text,
                        nullable: true,
                        default_expr: None,
                    },
                ],
                constraints: vec![],
                append_only: false,
                epoch: 0,
            })
            .await
            .unwrap();
        engine.create_table("t").await.unwrap();
        (engine, catalog)
    }

    fn row(id: i32, name: &str) -> Row {
        vec![Value::Int32(id), Value::Text(name.to_string())]
    }

    #[tokio::test]
    async fn per_session_transactions_are_isolated() {
        // Regression (ORM harness / SQLAlchemy): the engine used ONE global
        // txn buffer, so (a) a session that disconnected mid-transaction
        // blocked every later BEGIN with "transaction already active", and
        // (b) OTHER sessions' autocommit writes were silently buffered into
        // the orphaned transaction and lost. Buffers are per-session now.
        use crate::storage::STORAGE_SESSION_ID;
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("sessions.db").as_path()).await;

        // Session 1 opens a transaction and buffers a row… then "disconnects".
        STORAGE_SESSION_ID
            .scope(1, async {
                engine.begin_txn().await.unwrap();
                engine.insert("t", row(10, "orphan")).await.unwrap();
            })
            .await;

        // Session 2: BEGIN must succeed (own buffer), and its autocommit-style
        // committed txn must be independent of session 1's open buffer.
        STORAGE_SESSION_ID
            .scope(2, async {
                engine.begin_txn().await.unwrap();
                engine.insert("t", row(20, "s2")).await.unwrap();
                engine.commit_txn().await.unwrap();
            })
            .await;

        // Session 3 (no txn): a plain write passes straight through and is
        // NOT swallowed by session 1's still-open buffer.
        STORAGE_SESSION_ID
            .scope(3, async {
                engine.insert("t", row(30, "s3")).await.unwrap();
            })
            .await;

        // Outside any buffering session: only committed rows are visible.
        let rows = engine.scan("t").await.unwrap();
        let ids: Vec<i32> = rows
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Int32(n)) => Some(*n),
                _ => None,
            })
            .collect();
        assert!(ids.contains(&20) && ids.contains(&30), "committed rows visible: {ids:?}");
        assert!(!ids.contains(&10), "orphaned buffered row must not be visible: {ids:?}");

        // Disconnect cleanup releases session 1's orphan; a fresh BEGIN on the
        // same id succeeds and the orphaned row is gone for good.
        engine.drop_storage_session(1);
        STORAGE_SESSION_ID
            .scope(1, async {
                engine.begin_txn().await.unwrap();
                engine.abort_txn().await.unwrap();
            })
            .await;
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn auto_commit_passthrough() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("auto.db").as_path()).await;

        engine.insert("t", row(1, "alice")).await.unwrap();
        engine.insert("t", row(2, "bob")).await.unwrap();

        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn scan_limit_early_exit_in_auto_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("limit_auto.db").as_path()).await;
        for i in 0..50 {
            engine.insert("t", row(i, "x")).await.unwrap();
        }
        let full = engine.scan("t").await.unwrap();
        let limited = engine.scan_limit("t", 5).await.unwrap();
        assert_eq!(limited, full[..5]);
    }

    #[tokio::test]
    async fn scan_limit_respects_buffered_view_in_txn() {
        // Inside a transaction, scan_limit must reflect buffered inserts (it
        // falls back to the full buffered scan then truncates), not the inner
        // engine's early-exit which is blind to the buffer.
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("limit_txn.db").as_path()).await;
        engine.insert("t", row(1, "committed")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(2, "buffered")).await.unwrap();
        engine.insert("t", row(3, "buffered")).await.unwrap();

        let limited = engine.scan_limit("t", 2).await.unwrap();
        assert_eq!(limited.len(), 2);
        // Buffered inserts append after the committed row, so the first two are
        // the committed row and the first buffered row.
        assert_eq!(limited[0], row(1, "committed"));
        assert_eq!(limited[1], row(2, "buffered"));
    }

    #[tokio::test]
    async fn commit_applies_buffered_inserts() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("commit.db").as_path()).await;

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(1, "alice")).await.unwrap();
        engine.insert("t", row(2, "bob")).await.unwrap();

        // During txn: scan should show buffered rows
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);

        engine.commit_txn().await.unwrap();

        // After commit: data persisted to DiskEngine
        let rows = engine.inner().scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn rollback_discards_inserts() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("rollback.db").as_path()).await;

        engine.insert("t", row(0, "pre-txn")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(1, "will-rollback")).await.unwrap();
        engine.insert("t", row(2, "also-gone")).await.unwrap();
        engine.abort_txn().await.unwrap();

        // After rollback: only the pre-txn row remains
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("pre-txn".to_string()));
    }

    #[tokio::test]
    async fn rollback_discards_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("rollback_del.db").as_path()).await;

        engine.insert("t", row(1, "keep")).await.unwrap();
        engine.insert("t", row(2, "keep")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.delete("t", &[0, 1]).await.unwrap();
        engine.abort_txn().await.unwrap();

        // Rows should still be there after rollback
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn commit_applies_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("commit_del.db").as_path()).await;

        engine.insert("t", row(1, "a")).await.unwrap();
        engine.insert("t", row(2, "b")).await.unwrap();
        engine.insert("t", row(3, "c")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.delete("t", &[1]).await.unwrap(); // delete row at position 1 ("b")
        engine.commit_txn().await.unwrap();

        let rows = engine.inner().scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn commit_applies_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("commit_upd.db").as_path()).await;

        engine.insert("t", row(1, "old")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.update("t", &[(0, row(1, "new"))]).await.unwrap();
        engine.commit_txn().await.unwrap();

        let rows = engine.inner().scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("new".to_string()));
    }

    #[tokio::test]
    async fn rollback_discards_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("rollback_upd.db").as_path()).await;

        engine.insert("t", row(1, "original")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.update("t", &[(0, row(1, "changed"))]).await.unwrap();
        engine.abort_txn().await.unwrap();

        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("original".to_string()));
    }

    #[tokio::test]
    async fn nested_begin_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("nested.db").as_path()).await;

        engine.begin_txn().await.unwrap();
        let result = engine.begin_txn().await;
        assert!(result.is_err());
        engine.abort_txn().await.unwrap();
    }

    #[tokio::test]
    async fn supports_mvcc_returns_true() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("mvcc.db").as_path()).await;
        assert!(engine.supports_mvcc());
    }
}
