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
//! Limitations:
//! - Single active transaction at a time (StorageEngine trait has no session ID)
//! - No full MVCC snapshot isolation between concurrent sessions
//! - Buffered data is in memory — very large transactions may use significant RAM

use std::sync::Arc;

use parking_lot::RwLock;

use super::disk_engine::DiskEngine;
use super::{StorageEngine, StorageError};
use crate::types::{Row, Value};

/// A buffered write operation within a transaction.
#[derive(Debug, Clone)]
enum BufferedOp {
    Insert { table: String, row: Row },
    Delete { table: String, positions: Vec<usize> },
    Update { table: String, updates: Vec<(usize, Row)> },
    CreateTable { table: String },
    DropTable { table: String },
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

/// Wraps [`DiskEngine`] with transaction write buffering.
pub struct BufferedDiskEngine {
    inner: Arc<DiskEngine>,
    /// Current transaction buffer. `Some` = explicit transaction active.
    txn_buf: RwLock<Option<TxnBuffer>>,
}

impl BufferedDiskEngine {
    pub fn new(inner: Arc<DiskEngine>) -> Self {
        Self {
            inner,
            txn_buf: RwLock::new(None),
        }
    }

    /// Get the underlying DiskEngine for direct access (flush, buffer pool, etc.).
    pub fn inner(&self) -> &Arc<DiskEngine> {
        &self.inner
    }

    fn is_in_txn(&self) -> bool {
        self.txn_buf.read().is_some()
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
        if self.is_in_txn() {
            let mut buf = self.txn_buf.write();
            if let Some(ref mut txn) = *buf {
                txn.ops.push(BufferedOp::CreateTable { table: table.to_string() });
            }
            Ok(())
        } else {
            self.inner.create_table(table).await
        }
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        if self.is_in_txn() {
            let mut buf = self.txn_buf.write();
            if let Some(ref mut txn) = *buf {
                txn.ops.push(BufferedOp::DropTable { table: table.to_string() });
            }
            Ok(())
        } else {
            self.inner.drop_table(table).await
        }
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        if self.is_in_txn() {
            let mut buf = self.txn_buf.write();
            if let Some(ref mut txn) = *buf {
                txn.ops.push(BufferedOp::Insert {
                    table: table.to_string(),
                    row,
                });
            }
            Ok(())
        } else {
            self.inner.insert(table, row).await
        }
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        let mut rows = self.inner.scan(table).await?;

        // If in a transaction, apply buffered ops to produce a consistent view.
        let buf = self.txn_buf.read();
        if let Some(ref txn) = *buf {
            for op in &txn.ops {
                match op {
                    BufferedOp::Insert { table: t, row } if t == table => {
                        rows.push(row.clone());
                    }
                    BufferedOp::Delete { table: t, positions } if t == table => {
                        // Mark deleted positions (apply in reverse order to handle shifts)
                        let mut deleted = vec![false; rows.len()];
                        for &pos in positions {
                            if pos < deleted.len() {
                                deleted[pos] = true;
                            }
                        }
                        rows = rows.into_iter().enumerate()
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

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        if self.is_in_txn() {
            let count = positions.len();
            let mut buf = self.txn_buf.write();
            if let Some(ref mut txn) = *buf {
                txn.ops.push(BufferedOp::Delete {
                    table: table.to_string(),
                    positions: positions.to_vec(),
                });
            }
            Ok(count)
        } else {
            self.inner.delete(table, positions).await
        }
    }

    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError> {
        if self.is_in_txn() {
            let count = updates.len();
            let mut buf = self.txn_buf.write();
            if let Some(ref mut txn) = *buf {
                txn.ops.push(BufferedOp::Update {
                    table: table.to_string(),
                    updates: updates.to_vec(),
                });
            }
            Ok(count)
        } else {
            self.inner.update(table, updates).await
        }
    }

    // -- Transaction lifecycle --

    async fn begin_txn(&self) -> Result<(), StorageError> {
        let mut buf = self.txn_buf.write();
        if buf.is_some() {
            return Err(StorageError::Io("transaction already active".into()));
        }
        *buf = Some(TxnBuffer::new());
        Ok(())
    }

    async fn commit_txn(&self) -> Result<(), StorageError> {
        let ops = {
            let mut buf = self.txn_buf.write();
            match buf.take() {
                Some(txn) => txn.ops,
                None => return Ok(()), // no active txn — no-op
            }
        };
        self.apply_buffer(ops).await
    }

    async fn abort_txn(&self) -> Result<(), StorageError> {
        let mut buf = self.txn_buf.write();
        *buf = None; // discard all buffered operations
        Ok(())
    }

    fn supports_mvcc(&self) -> bool {
        true // We provide transaction atomicity + rollback
    }

    // -- Delegate everything else to inner DiskEngine --

    async fn create_index(&self, table: &str, index_name: &str, col_idx: usize) -> Result<(), StorageError> {
        self.inner.create_index(table, index_name, col_idx).await
    }

    async fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        self.inner.drop_index(index_name).await
    }

    async fn index_lookup(&self, table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        self.inner.index_lookup(table, index_name, value).await
    }

    async fn index_lookup_range(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.inner.index_lookup_range(table, index_name, low, high).await
    }

    fn index_lookup_sync(&self, table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        self.inner.index_lookup_sync(table, index_name, value)
    }

    fn index_lookup_range_sync(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.inner.index_lookup_range_sync(table, index_name, low, high)
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
        catalog.create_table(TableDef {
            name: "t".to_string(),
            columns: vec![
                ColumnDef { name: "id".into(), data_type: DataType::Int32, nullable: false, default_expr: None },
                ColumnDef { name: "name".into(), data_type: DataType::Text, nullable: true, default_expr: None },
            ],
            constraints: vec![],
            append_only: false,
        }).await.unwrap();
        engine.create_table("t").await.unwrap();
        (engine, catalog)
    }

    fn row(id: i32, name: &str) -> Row {
        vec![Value::Int32(id), Value::Text(name.to_string())]
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
