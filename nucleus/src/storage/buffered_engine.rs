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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;

use super::disk_engine::DiskEngine;
use super::{StorageEngine, StorageError};
use crate::types::{Row, Value};

/// First synthetic position handed out for a row that exists only in a
/// transaction buffer.
///
/// Positions are the inner [`DiskEngine`]'s stable row addresses —
/// `(page_id, slot_idx)` packed into a `usize`, which occupies the low 48 bits.
/// A row that has not reached the engine yet has no address, so the buffered
/// view numbers it from above that range; the two spaces can then never
/// collide, and a mutation aimed at a not-yet-written row is recognisable as
/// such. Before stable addresses, positions were scan ordinals over the
/// buffered view and a buffered DELETE/UPDATE replayed at COMMIT landed on
/// whichever row happened to sit at that ordinal in the engine.
const PENDING_POS_BASE: usize = 1 << 48;

fn is_pending_pos(pos: usize) -> bool {
    pos >= PENDING_POS_BASE
}

/// A buffered write operation within a transaction.
#[derive(Debug, Clone)]
enum BufferedOp {
    Insert {
        table: String,
        /// Synthetic position this row answers to until it is written.
        pending_pos: usize,
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
    /// Savepoint stack: `(name, ops.len() at SAVEPOINT time)`.
    ///
    /// Nothing in this buffer has touched the underlying engine yet, so a
    /// savepoint is just a mark and `ROLLBACK TO SAVEPOINT` is a truncate.
    /// That only holds while the op log stays append-only, so a buffered row
    /// that is later deleted or updated is resolved at COMMIT rather than
    /// edited in place here.
    savepoints: Vec<(String, usize)>,
    /// Next synthetic position for a buffered insert.
    next_pending: usize,
}

impl TxnBuffer {
    fn new() -> Self {
        Self {
            ops: Vec::new(),
            savepoints: Vec::new(),
            next_pending: PENDING_POS_BASE,
        }
    }

    fn take_pending_pos(&mut self) -> usize {
        let pos = self.next_pending;
        self.next_pending += 1;
        pos
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
    ///
    /// Ops naming a synthetic position never reach the engine: a DELETE of a
    /// row this transaction inserted cancels that insert, and an UPDATE of one
    /// rewrites the row the insert will write. Everything else names a real
    /// engine address.
    ///
    /// # Why real positions are collapsed to one write each
    ///
    /// A real position is a physical `(page_id, slot_idx)` address, and applying
    /// a write can MOVE the row away from it: `DiskEngine::update_at` relocates
    /// a row that no longer fits its slot, into an earlier dead slot on the page
    /// or onto another page entirely. Replaying ops one by one therefore breaks
    /// as soon as two of them name the same position — the first write moves the
    /// row, and every later write lands on a slot that is now dead, where
    /// `update_at`/`delete_at` skip it and return success.
    ///
    /// That is silent data loss from ordinary single-session SQL:
    /// `BEGIN; UPDATE t SET c = repeat('q',2000) WHERE id=3; DELETE FROM t WHERE
    /// id=3; COMMIT;` reported `DELETE 1` and left the row on disk, surviving a
    /// reopen. The buffered view is what made it invisible until COMMIT — the
    /// overlay rewrites a buffered row in place and keeps its position, so both
    /// statements resolve the same address and the transaction looks coherent
    /// right up to the point where it is replayed.
    ///
    /// So each real position gets exactly one write, carrying its final value:
    /// the last op naming it decides whether that is a delete or an update, and
    /// intermediate updates are dropped. This is also what the ops MEAN — a
    /// transaction that updates then deletes a row has deleted it.
    async fn apply_buffer(&self, ops: Vec<BufferedOp>) -> Result<(), StorageError> {
        // The last op naming each real position wins; `usize` is its index in
        // `ops`, so replay can tell "this is the deciding write" from "this is
        // an intermediate one that must not be applied".
        let mut last_touch: HashMap<usize, usize> = HashMap::new();
        for (i, op) in ops.iter().enumerate() {
            match op {
                BufferedOp::Delete { positions, .. } => {
                    for pos in positions.iter().copied().filter(|p| !is_pending_pos(*p)) {
                        last_touch.insert(pos, i);
                    }
                }
                BufferedOp::Update { updates, .. } => {
                    for (pos, _) in updates.iter().filter(|(p, _)| !is_pending_pos(*p)) {
                        last_touch.insert(*pos, i);
                    }
                }
                _ => {}
            }
        }

        // Resolve the fate of each buffered insert before replaying anything.
        let mut cancelled: HashSet<usize> = HashSet::new();
        let mut rewritten: HashMap<usize, Row> = HashMap::new();
        for op in &ops {
            match op {
                BufferedOp::Delete { positions, .. } => {
                    for pos in positions.iter().copied().filter(|p| is_pending_pos(*p)) {
                        cancelled.insert(pos);
                        rewritten.remove(&pos);
                    }
                }
                BufferedOp::Update { updates, .. } => {
                    for (pos, row) in updates.iter().filter(|(p, _)| is_pending_pos(*p)) {
                        if !cancelled.contains(pos) {
                            rewritten.insert(*pos, row.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        for (i, op) in ops.into_iter().enumerate() {
            match op {
                BufferedOp::Insert {
                    table,
                    pending_pos,
                    row,
                } => {
                    if cancelled.contains(&pending_pos) {
                        continue;
                    }
                    let row = rewritten.remove(&pending_pos).unwrap_or(row);
                    self.inner.insert(&table, row).await?;
                }
                BufferedOp::Delete { table, positions } => {
                    // Only the deciding op for each position writes; an earlier
                    // op naming a position that is written again later would
                    // move the row and strand every write after it.
                    let positions: Vec<usize> = positions
                        .into_iter()
                        .filter(|p| !is_pending_pos(*p))
                        .filter(|p| last_touch.get(p) == Some(&i))
                        .collect();
                    if !positions.is_empty() {
                        self.inner.delete(&table, &positions).await?;
                    }
                }
                BufferedOp::Update { table, updates } => {
                    let updates: Vec<(usize, Row)> = updates
                        .into_iter()
                        .filter(|(p, _)| !is_pending_pos(*p))
                        .filter(|(p, _)| last_touch.get(p) == Some(&i))
                        .collect();
                    if !updates.is_empty() {
                        self.inner.update(&table, &updates).await?;
                    }
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

    /// This session's view of a table as `(position, row)` pairs: the engine's
    /// committed rows at their real addresses, with this transaction's own
    /// buffered writes overlaid at synthetic ones.
    async fn overlay(&self, table: &str) -> Result<Vec<(usize, Row)>, StorageError> {
        let mut rows = self.inner.scan_physical(table).await?;

        let bufs = self.txn_bufs.read();
        let Some(txn) = bufs.get(&current_session_id()) else {
            return Ok(rows);
        };
        for op in &txn.ops {
            match op {
                BufferedOp::Insert {
                    table: t,
                    pending_pos,
                    row,
                } if t == table => {
                    rows.push((*pending_pos, row.clone()));
                }
                BufferedOp::Delete {
                    table: t,
                    positions,
                } if t == table => {
                    let drop: HashSet<usize> = positions.iter().copied().collect();
                    rows.retain(|(pos, _)| !drop.contains(pos));
                }
                BufferedOp::Update { table: t, updates } if t == table => {
                    for (pos, new_row) in updates {
                        if let Some(slot) = rows.iter_mut().find(|(p, _)| p == pos) {
                            slot.1 = new_row.clone();
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl StorageEngine for BufferedDiskEngine {
    fn as_backup_coordinator(&self) -> Option<&dyn crate::backup::BackupCoordinator> {
        // Delegate to the disk engine underneath: the buffering layer holds no
        // durable state of its own, and per-session transaction buffers are
        // uncommitted by definition, so they are correctly excluded from a
        // snapshot.
        self.inner.as_backup_coordinator()
    }

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
            let pending_pos = txn.take_pending_pos();
            txn.ops.push(BufferedOp::Insert {
                table: table.to_string(),
                pending_pos,
                row,
            });
            return Ok(());
        }
        self.inner.insert(table, row).await
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        // Outside a transaction there is nothing to overlay, so take the inner
        // engine's own scan (which is cheaper than materializing positions).
        if !self.is_in_txn() {
            return self.inner.scan(table).await;
        }
        Ok(self
            .overlay(table)
            .await?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    async fn scan_physical(&self, table: &str) -> Result<Vec<(usize, Row)>, StorageError> {
        if !self.is_in_txn() {
            return self.inner.scan_physical(table).await;
        }
        self.overlay(table).await
    }

    async fn scan_where_eq_positions(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
    ) -> Result<Vec<(usize, Row)>, StorageError> {
        if !self.is_in_txn() {
            return self
                .inner
                .scan_where_eq_positions(table, col_idx, value)
                .await;
        }
        Ok(self
            .overlay(table)
            .await?
            .into_iter()
            .filter(|(_, row)| row.get(col_idx).is_some_and(|v| v.loose_eq(value)))
            .collect())
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

    async fn update_if_unchanged(
        &self,
        table: &str,
        updates: &[(usize, Row, Row)],
    ) -> Result<usize, StorageError> {
        if self.is_in_txn() {
            // Buffered writes are this session's alone and are replayed against
            // the engine only at COMMIT, so nothing can have moved underneath
            // them yet; the identity re-check happens when the buffer applies.
            let plain: Vec<(usize, Row)> = updates
                .iter()
                .map(|(pos, _read, new_row)| (*pos, new_row.clone()))
                .collect();
            return self.update(table, &plain).await;
        }
        self.inner.update_if_unchanged(table, updates).await
    }

    async fn delete_if_unchanged(
        &self,
        table: &str,
        targets: &[(usize, Row)],
    ) -> Result<usize, StorageError> {
        if self.is_in_txn() {
            let positions: Vec<usize> = targets.iter().map(|(pos, _)| *pos).collect();
            return self.delete(table, &positions).await;
        }
        self.inner.delete_if_unchanged(table, targets).await
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

    /// Mark the current position in the write buffer.
    ///
    /// Savepoints were previously inherited from the `StorageEngine` default,
    /// which is a silent `Ok(())` no-op. Because this engine also reports
    /// `supports_mvcc() == true`, the executor delegated to it and believed the
    /// work was done, so on the disk stack that every server deployment runs,
    /// `ROLLBACK TO SAVEPOINT` acknowledged success and discarded nothing. The
    /// in-memory `MvccStorageAdapter` implements savepoints properly, which is
    /// why the library test suite never saw it.
    async fn savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut bufs = self.txn_bufs.write();
        let Some(txn) = bufs.get_mut(&current_session_id()) else {
            return Err(StorageError::NoActiveTransaction);
        };
        let mark = txn.ops.len();
        txn.savepoints.push((name.to_string(), mark));
        Ok(())
    }

    /// Discard every buffered operation made after the named savepoint.
    async fn rollback_to_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut bufs = self.txn_bufs.write();
        let Some(txn) = bufs.get_mut(&current_session_id()) else {
            return Err(StorageError::NoActiveTransaction);
        };
        let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == name) else {
            return Err(StorageError::Io(format!("savepoint {name} does not exist")));
        };
        let mark = txn.savepoints[pos].1;
        txn.ops.truncate(mark);
        // The savepoint stays live after rolling back to it (Postgres
        // semantics); the ones nested inside it do not.
        txn.savepoints.truncate(pos + 1);
        Ok(())
    }

    /// Drop the named savepoint and everything nested inside it, keeping the
    /// buffered work.
    async fn release_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut bufs = self.txn_bufs.write();
        let Some(txn) = bufs.get_mut(&current_session_id()) else {
            return Err(StorageError::NoActiveTransaction);
        };
        let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == name) else {
            return Err(StorageError::Io(format!("savepoint {name} does not exist")));
        };
        txn.savepoints.truncate(pos);
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
        // Inside a transaction the inner engine's index reflects only COMMITTED
        // rows — it cannot see this transaction's own buffered inserts. A unique
        // check that trusted it would miss an in-transaction duplicate (a second
        // INSERT of the same key within one BEGIN..COMMIT was accepted). Return
        // None so the caller falls back to `scan`, which the buffer overlays.
        if self.is_in_txn() {
            return Ok(None);
        }
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
                        id: 0,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Text,
                        nullable: true,
                        default_expr: None,
                        id: 0,
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
        let b = engine.scan_physical("t").await.unwrap()[1].0; // the row "b"
        engine.delete("t", &[b]).await.unwrap();
        engine.commit_txn().await.unwrap();

        let rows = engine.inner().scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter().all(|r| r[1] != Value::Text("b".to_string())),
            "committed delete removed the wrong row: {rows:?}"
        );
    }

    #[tokio::test]
    async fn commit_applies_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("commit_upd.db").as_path()).await;

        engine.insert("t", row(1, "old")).await.unwrap();

        engine.begin_txn().await.unwrap();
        let pos = engine.scan_physical("t").await.unwrap()[0].0;
        engine.update("t", &[(pos, row(1, "new"))]).await.unwrap();
        engine.commit_txn().await.unwrap();

        let rows = engine.inner().scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("new".to_string()));
    }

    #[tokio::test]
    async fn buffered_row_can_be_deleted_and_updated_before_commit() {
        // A row inserted inside a transaction has no engine address yet, so
        // the buffered view numbers it separately. A DELETE or UPDATE naming
        // one of those positions has to act on the pending insert; replaying it
        // against the engine would hit whatever committed row sat at that
        // number.
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("pending_ops.db").as_path()).await;

        engine.insert("t", row(1, "committed")).await.unwrap();
        engine.insert("t", row(2, "committed")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(3, "doomed")).await.unwrap();
        engine.insert("t", row(4, "revised")).await.unwrap();

        let view = engine.scan_physical("t").await.unwrap();
        assert_eq!(view.len(), 4, "buffered inserts must be visible in-txn");
        let doomed = view.iter().find(|(_, r)| r[0] == Value::Int32(3)).unwrap().0;
        let revised = view.iter().find(|(_, r)| r[0] == Value::Int32(4)).unwrap().0;

        engine.delete("t", &[doomed]).await.unwrap();
        engine
            .update("t", &[(revised, row(4, "final"))])
            .await
            .unwrap();
        engine.commit_txn().await.unwrap();

        let mut rows = engine.inner().scan("t").await.unwrap();
        rows.sort_by_key(|r| format!("{:?}", r[0]));
        assert_eq!(rows.len(), 3, "committed row set is wrong: {rows:?}");
        assert!(
            rows.iter().all(|r| r[0] != Value::Int32(3)),
            "a row deleted before COMMIT was written anyway: {rows:?}"
        );
        assert!(
            rows.contains(&row(4, "final")),
            "an update to a buffered row was lost: {rows:?}"
        );
        assert!(
            rows.contains(&row(1, "committed")) && rows.contains(&row(2, "committed")),
            "buffered ops damaged committed rows: {rows:?}"
        );
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
