//! Storage engine — manages actual row data.
//!
//! Provides both in-memory (MemoryEngine) and disk-based (DiskEngine) backends.
//! All storage access goes through the StorageEngine trait.
//! Principle 1: subsystems interact through clean abstractions.

pub mod btree;
pub mod buffer;
pub mod buffered_engine;
pub mod columnar_engine;
pub mod columnar_wal;
pub mod compression;
pub mod kv_wal;
pub mod disk;
pub mod disk_engine;
pub mod fsm;
pub mod encrypted_index;
pub mod encryption;
pub mod io_uring;
pub mod lsm;
pub mod lsm_engine;
pub mod mvcc;
pub mod mvcc_wal;
pub mod page;
pub mod tuple;
pub mod txn;
pub mod persistence;
pub mod wal;

use std::collections::{BTreeMap, HashMap};
use tokio::sync::RwLock;

use crate::types::{Row, Value};

// Sync RwLock for index structures (never held across .await points).
use parking_lot::RwLock as SyncRwLock;

pub use columnar_engine::ColumnarStorageEngine;
pub use disk_engine::DiskEngine;
pub use lsm_engine::LsmStorageEngine;

tokio::task_local! {
    /// Session ID for per-connection storage isolation.
    /// Set by the executor when running within a session scope.
    /// 0 or unset = default/embedded (no explicit session).
    pub static STORAGE_SESSION_ID: u64;
}

/// The storage engine trait. All storage backends implement this.
/// Principle 1: subsystems interact through clean abstractions.
#[async_trait::async_trait]
pub trait StorageEngine: Send + Sync {
    async fn create_table(&self, table: &str) -> Result<(), StorageError>;
    async fn drop_table(&self, table: &str) -> Result<(), StorageError>;
    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError>;
    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError>;
    /// Scan with an early-exit limit. Returns at most `limit` rows.
    /// Default: delegates to scan() + truncate.
    async fn scan_limit(&self, table: &str, limit: usize) -> Result<Vec<Row>, StorageError> {
        let mut rows = self.scan(table).await?;
        rows.truncate(limit);
        Ok(rows)
    }
    /// Stream rows in batches through an mpsc channel. Default implementation
    /// delegates to `scan()` and sends all rows as a single chunk. Engines with
    /// page-level access (e.g. DiskEngine) can override to send rows as pages
    /// are read, enabling O(batch_size) peak memory for consumers.
    async fn scan_chunked(
        &self,
        table: &str,
        tx: tokio::sync::mpsc::Sender<Vec<Row>>,
        batch_size: usize,
    ) -> Result<(), StorageError> {
        let rows = self.scan(table).await?;
        for chunk in rows.chunks(batch_size.max(1)) {
            if tx.send(chunk.to_vec()).await.is_err() {
                break; // Receiver dropped — consumer is done
            }
        }
        Ok(())
    }

    /// Delete rows at the given scan-order positions. Returns number deleted.
    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError>;
    /// Update rows at the given scan-order positions with new row values.
    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError>;

    /// Notify the storage engine of a table's column schema (for WAL persistence).
    /// Default: no-op. Durable engines override this to log schema to WAL.
    fn store_table_schema(&self, _table: &str, _columns: &[(String, crate::types::DataType)]) {}

    /// Batch-insert multiple rows. Default implementation loops over `insert()`.
    /// Override for engines that can hold one lock / one transaction for the batch.
    async fn insert_batch(&self, table: &str, rows: Vec<Row>) -> Result<(), StorageError> {
        for row in rows {
            self.insert(table, row).await?;
        }
        Ok(())
    }

    // -- Index operations (default: no-op / unsupported) --

    /// Build a B-tree index on the given column. `col_idx` is the 0-based
    /// column position in the table schema.
    async fn create_index(&self, _table: &str, _index_name: &str, _col_idx: usize) -> Result<(), StorageError> { Ok(()) }
    /// Drop an index by name.
    async fn drop_index(&self, _index_name: &str) -> Result<(), StorageError> { Ok(()) }
    /// Point-lookup rows via a named index. Returns matching rows.
    async fn index_lookup(&self, _table: &str, _index_name: &str, _value: &Value) -> Result<Option<Vec<Row>>, StorageError> { Ok(None) }
    /// Range-lookup rows via a named index for inclusive bounds.
    async fn index_lookup_range(
        &self,
        _table: &str,
        _index_name: &str,
        _low: &Value,
        _high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> { Ok(None) }
    /// Synchronous point-lookup for use in contexts where `.await` is unsafe
    /// (e.g. deeply nested `Box::pin` futures on single-threaded runtimes).
    /// Engines with synchronous internals should override this.
    fn index_lookup_sync(&self, _table: &str, _index_name: &str, _value: &Value) -> Result<Option<Vec<Row>>, StorageError> { Ok(None) }
    /// Scan a table returning only the projected columns.
    ///
    /// `projection` contains 0-based column indices into the table schema.
    /// The returned rows contain only those columns, in projection order.
    /// Default: delegates to `scan()` and post-filters columns.
    /// Engines with page-level access (e.g. DiskEngine) override this to
    /// skip decoding non-projected columns during deserialization.
    async fn scan_projected(
        &self,
        table: &str,
        projection: &[usize],
    ) -> Result<Vec<Row>, StorageError> {
        let rows = self.scan(table).await?;
        Ok(rows
            .into_iter()
            .map(|row| projection.iter().filter_map(|&i| row.get(i).cloned()).collect())
            .collect())
    }

    /// Synchronous inclusive range lookup for index scans.
    fn index_lookup_range_sync(
        &self,
        _table: &str,
        _index_name: &str,
        _low: &Value,
        _high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> { Ok(None) }

    // -- Transaction lifecycle (default: auto-commit / no-op) --

    /// Set the isolation level for the next BEGIN. Default: Snapshot.
    /// Recognized values: "read committed", "repeatable read", "snapshot", "serializable".
    fn set_next_isolation_level(&self, _level: &str) {}
    /// Fsync the WAL to stable storage. Ensures all auto-committed writes are
    /// durable against OS/power crashes. No-op for engines without a WAL.
    fn sync(&self) -> Result<(), StorageError> { Ok(()) }

    /// Begin an explicit transaction. Engines that support MVCC will take a
    /// snapshot; simple engines do nothing.
    async fn begin_txn(&self) -> Result<(), StorageError> { Ok(()) }
    /// Commit the current transaction.
    async fn commit_txn(&self) -> Result<(), StorageError> { Ok(()) }
    /// Abort (rollback) the current transaction.
    async fn abort_txn(&self) -> Result<(), StorageError> { Ok(()) }
    /// Create a savepoint within the current transaction.
    async fn savepoint(&self, _name: &str) -> Result<(), StorageError> { Ok(()) }
    /// Rollback to a named savepoint.
    async fn rollback_to_savepoint(&self, _name: &str) -> Result<(), StorageError> { Ok(()) }
    /// Release a named savepoint.
    async fn release_savepoint(&self, _name: &str) -> Result<(), StorageError> { Ok(()) }
    /// Whether this engine supports real MVCC transactions.
    fn supports_mvcc(&self) -> bool { false }
    /// Flush all dirty data to stable storage. Engines that don't persist
    /// can no-op (the default).
    async fn flush_all_dirty(&self) -> Result<(), StorageError> { Ok(()) }
    /// Perform a checkpoint: flush dirty pages, write a WAL checkpoint record,
    /// and truncate old WAL segments. Engines without WAL can no-op (the default).
    async fn checkpoint(&self) -> Result<(), StorageError> { Ok(()) }
    /// Vacuum a table: reclaim dead tuples and compact pages.
    /// Returns (pages_scanned, dead_tuples_reclaimed, pages_freed, bytes_reclaimed).
    async fn vacuum(&self, _table: &str) -> Result<(usize, usize, usize, usize), StorageError> {
        Ok((0, 0, 0, 0))
    }
    /// Vacuum all tables. Returns total (pages_scanned, dead_tuples_reclaimed, pages_freed, bytes_reclaimed).
    async fn vacuum_all(&self) -> Result<(usize, usize, usize, usize), StorageError> {
        Ok((0, 0, 0, 0))
    }

    // ── Aggregate fast paths (default: None = engine doesn't support it) ──────

    /// Fast COUNT(*) without scanning rows. Returns None if unsupported.
    fn fast_count_all(&self, _table: &str) -> Option<usize> { None }

    /// Fast SUM + non-null-count for a numeric column addressed by scan-order
    /// index. Returns `(sum, non_null_count)` so the caller can derive AVG.
    /// Returns None if unsupported or if the column is non-numeric.
    fn fast_sum_f64(&self, _table: &str, _col_idx: usize) -> Option<(f64, usize)> { None }

    /// Fast GROUP BY: `(key_value, row_count, avg_of_val_col)` triples.
    /// `key_col` and `val_col` are scan-order column indexes.
    /// Returns None if unsupported.
    fn fast_group_by(
        &self,
        _table: &str,
        _key_col: usize,
        _val_col: Option<usize>,
    ) -> Option<Vec<(crate::types::Value, i64, Option<f64>)>> { None }

    /// Fast COUNT where `filter_col == filter_val`. Returns None if unsupported.
    fn fast_count_filtered(
        &self,
        _table: &str,
        _filter_col: usize,
        _filter_val: &crate::types::Value,
    ) -> Option<usize> { None }

    /// Fast SUM + non-null-count filtered by a single equality predicate.
    /// Returns `(sum, count)` over rows where `filter_col == filter_val`.
    fn fast_sum_f64_filtered(
        &self,
        _table: &str,
        _val_col: usize,
        _filter_col: usize,
        _filter_val: &crate::types::Value,
    ) -> Option<(f64, usize)> { None }

    /// Fast MIN over a numeric column. Returns None if unsupported or column is empty.
    fn fast_min_f64(&self, _table: &str, _col_idx: usize) -> Option<f64> { None }

    /// Fast MAX over a numeric column. Returns None if unsupported or column is empty.
    fn fast_max_f64(&self, _table: &str, _col_idx: usize) -> Option<f64> { None }

    /// Scan rows matching a single equality predicate, avoiding full materialization.
    /// Returns `None` if the engine does not support this optimisation (caller falls
    /// back to full scan + filter). The returned rows always include ALL columns.
    fn fast_scan_where_eq(
        &self,
        _table: &str,
        _filter_col: usize,
        _filter_val: &Value,
    ) -> Option<Vec<Row>> { None }

    /// Scan rows where `filter_col` is between `low` and `high` (inclusive).
    /// Returns `None` if the engine does not support this optimisation.
    fn fast_scan_where_range(
        &self,
        _table: &str,
        _filter_col: usize,
        _low: &Value,
        _high: &Value,
    ) -> Option<Vec<Row>> { None }

    // ── Per-connection session lifecycle (default: no-op) ──────

    /// Create per-connection storage state for a new session.
    /// Engines with per-session transaction isolation (e.g. MVCC) override this.
    fn create_storage_session(&self, _id: u64) {}
    /// Drop per-connection storage state when a connection closes.
    fn drop_storage_session(&self, _id: u64) {}
}

/// In-memory index: maps a value to the rows that match it.
/// BTreeMap keeps keys sorted for O(log n + k) range lookups.
struct MemIdx {
    col_idx: usize,
    map: BTreeMap<Value, Vec<Row>>,
}

/// In-memory storage engine. Simple HashMap of table name → rows.
/// Useful for testing and for when persistence is not needed.
pub struct MemoryEngine {
    tables: RwLock<HashMap<String, Vec<Row>>>,
    /// index_name → MemIdx
    indexes: SyncRwLock<HashMap<String, MemIdx>>,
    /// table → [index_name] for fast index lookup during insert
    table_idx_names: SyncRwLock<HashMap<String, Vec<String>>>,
}

impl Default for MemoryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            indexes: SyncRwLock::new(HashMap::new()),
            table_idx_names: SyncRwLock::new(HashMap::new()),
        }
    }

    /// Rebuild all indexes for a table from its current rows.
    /// Call after any mutation that changes row positions (delete, update).
    async fn rebuild_indexes(&self, table: &str) {
        // Clone rows out of the async lock before taking sync locks.
        let rows = {
            let tables = self.tables.read().await;
            match tables.get(table) {
                Some(r) => r.clone(),
                None => return,
            }
        };
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() { return; }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                idx.map.clear();
                for row in &rows {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map.entry(val).or_default().push(row.clone());
                }
            }
        }
    }

    /// Update all indexes for a table when new rows are appended.
    fn update_indexes_for_new_rows(&self, table: &str, new_rows: &[Row]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() { return; }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                for row in new_rows {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map.entry(val).or_default().push(row.clone());
                }
            }
        }
    }
}

impl std::fmt::Debug for MemoryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryEngine").finish()
    }
}

#[async_trait::async_trait]
impl StorageEngine for MemoryEngine {
    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        let mut tables = self.tables.write().await;
        tables.insert(table.to_string(), Vec::new());
        Ok(())
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        let mut tables = self.tables.write().await;
        if tables.remove(table).is_none() {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        Ok(())
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        {
            let mut tables = self.tables.write().await;
            let rows = tables
                .get_mut(table)
                .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
            rows.push(row.clone());
        }
        self.update_indexes_for_new_rows(table, std::slice::from_ref(&row));
        Ok(())
    }

    async fn insert_batch(&self, table: &str, rows: Vec<Row>) -> Result<(), StorageError> {
        if rows.is_empty() { return Ok(()); }
        {
            let mut tables = self.tables.write().await;
            let tbl = tables
                .get_mut(table)
                .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
            for row in &rows { tbl.push(row.clone()); }
        }
        self.update_indexes_for_new_rows(table, &rows);
        Ok(())
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        let tables = self.tables.read().await;
        let rows = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        Ok(rows.clone())
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        let count = {
            let mut tables = self.tables.write().await;
            let rows = tables
                .get_mut(table)
                .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
            let mut sorted = positions.to_vec();
            sorted.sort_unstable();
            sorted.dedup();
            let count = sorted.len();
            for &pos in sorted.iter().rev() {
                if pos < rows.len() { rows.remove(pos); }
            }
            count
        };
        self.rebuild_indexes(table).await;
        Ok(count)
    }

    async fn update(
        &self,
        table: &str,
        updates: &[(usize, Row)],
    ) -> Result<usize, StorageError> {
        let count = {
            let mut tables = self.tables.write().await;
            let rows = tables
                .get_mut(table)
                .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
            let mut count = 0;
            for (pos, new_row) in updates {
                if *pos < rows.len() {
                    rows[*pos] = new_row.clone();
                    count += 1;
                }
            }
            count
        };
        self.rebuild_indexes(table).await;
        Ok(count)
    }

    async fn create_index(&self, table: &str, index_name: &str, col_idx: usize) -> Result<(), StorageError> {
        // Build index from existing rows.
        let rows = {
            let tables = self.tables.read().await;
            tables.get(table).cloned().unwrap_or_default()
        };
        let mut map: BTreeMap<Value, Vec<Row>> = BTreeMap::new();
        for row in &rows {
            let val = row.get(col_idx).cloned().unwrap_or(Value::Null);
            map.entry(val).or_default().push(row.clone());
        }
        {
            let mut indexes = self.indexes.write();
            indexes.insert(index_name.to_string(), MemIdx { col_idx, map });
        }
        {
            let mut tnames = self.table_idx_names.write();
            tnames.entry(table.to_string()).or_default().push(index_name.to_string());
        }
        Ok(())
    }

    async fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        self.indexes.write().remove(index_name);
        let mut tnames = self.table_idx_names.write();
        for names in tnames.values_mut() {
            names.retain(|n| n != index_name);
        }
        Ok(())
    }

    async fn index_lookup(&self, _table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        self.index_lookup_sync(_table, index_name, value)
    }

    async fn index_lookup_range(
        &self,
        _table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.index_lookup_range_sync(_table, index_name, low, high)
    }

    fn index_lookup_sync(&self, _table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        let indexes = self.indexes.read();
        match indexes.get(index_name) {
            Some(idx) => Ok(Some(idx.map.get(value).cloned().unwrap_or_default())),
            None => Ok(None),
        }
    }

    fn index_lookup_range_sync(
        &self,
        _table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        let indexes = self.indexes.read();
        match indexes.get(index_name) {
            Some(idx) => {
                // BTreeMap::range gives O(log n) seek + O(k) scan — no sort needed since
                // BTreeMap iterates in key order (= index column order).
                let rows: Vec<Row> = idx.map
                    .range(low..=high)
                    .flat_map(|(_, r)| r.iter().cloned())
                    .collect();
                Ok(Some(rows))
            }
            None => Ok(None),
        }
    }

    // ── Aggregate fast paths ─────────────────────────────────────────────────
    //
    // These are sync methods. MemoryEngine's `tables` field uses a
    // tokio::sync::RwLock (async), so we use `try_read()` to attempt a
    // non-blocking lock acquisition. If a writer holds the lock (rare —
    // only during concurrent inserts/deletes), we return `None` to fall
    // back to the normal scan path. This is safe and correct: returning
    // `None` just means "engine can't answer quickly right now".

    fn fast_count_all(&self, table: &str) -> Option<usize> {
        let guard = self.tables.try_read().ok()?;
        guard.get(table).map(|rows| rows.len())
    }

    fn fast_sum_f64(&self, table: &str, col_idx: usize) -> Option<(f64, usize)> {
        let guard = self.tables.try_read().ok()?;
        let rows = guard.get(table)?;
        let mut sum = 0.0f64;
        let mut count = 0usize;
        for row in rows {
            if let Some(val) = row.get(col_idx) {
                match val {
                    Value::Int32(n) => { sum += *n as f64; count += 1; }
                    Value::Int64(n) => { sum += *n as f64; count += 1; }
                    Value::Float64(f) => { sum += f; count += 1; }
                    Value::Null => {}
                    _ => return None, // non-numeric column — bail
                }
            }
        }
        Some((sum, count))
    }

    fn fast_count_filtered(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<usize> {
        let guard = self.tables.try_read().ok()?;
        let rows = guard.get(table)?;
        let count = rows.iter()
            .filter(|row| row.get(filter_col) == Some(filter_val))
            .count();
        Some(count)
    }

    fn fast_sum_f64_filtered(
        &self,
        table: &str,
        val_col: usize,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<(f64, usize)> {
        let guard = self.tables.try_read().ok()?;
        let rows = guard.get(table)?;
        let mut sum = 0.0f64;
        let mut count = 0usize;
        for row in rows {
            if row.get(filter_col) == Some(filter_val)
                && let Some(val) = row.get(val_col) {
                    match val {
                        Value::Int32(n) => { sum += *n as f64; count += 1; }
                        Value::Int64(n) => { sum += *n as f64; count += 1; }
                        Value::Float64(f) => { sum += f; count += 1; }
                        Value::Null => {}
                        _ => return None,
                    }
            }
        }
        Some((sum, count))
    }

    fn fast_min_f64(&self, table: &str, col_idx: usize) -> Option<f64> {
        let guard = self.tables.try_read().ok()?;
        let rows = guard.get(table)?;
        let mut min_val: Option<f64> = None;
        for row in rows {
            if let Some(val) = row.get(col_idx) {
                let f = match val {
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    Value::Float64(f) => *f,
                    Value::Null => continue,
                    _ => return None,
                };
                min_val = Some(match min_val {
                    Some(cur) => cur.min(f),
                    None => f,
                });
            }
        }
        min_val
    }

    fn fast_max_f64(&self, table: &str, col_idx: usize) -> Option<f64> {
        let guard = self.tables.try_read().ok()?;
        let rows = guard.get(table)?;
        let mut max_val: Option<f64> = None;
        for row in rows {
            if let Some(val) = row.get(col_idx) {
                let f = match val {
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    Value::Float64(f) => *f,
                    Value::Null => continue,
                    _ => return None,
                };
                max_val = Some(match max_val {
                    Some(cur) => cur.max(f),
                    None => f,
                });
            }
        }
        max_val
    }

    fn fast_group_by(
        &self,
        table: &str,
        key_col: usize,
        val_col: Option<usize>,
    ) -> Option<Vec<(Value, i64, Option<f64>)>> {
        let guard = self.tables.try_read().ok()?;
        let rows = guard.get(table)?;
        // Accumulate per-group: (count, sum, non_null_count)
        let mut groups: HashMap<Value, (i64, f64, usize)> = HashMap::new();
        for row in rows {
            let key = row.get(key_col).cloned().unwrap_or(Value::Null);
            let entry = groups.entry(key).or_insert((0, 0.0, 0));
            entry.0 += 1;
            if let Some(vc) = val_col
                && let Some(val) = row.get(vc) {
                    match val {
                        Value::Int32(n) => { entry.1 += *n as f64; entry.2 += 1; }
                        Value::Int64(n) => { entry.1 += *n as f64; entry.2 += 1; }
                        Value::Float64(f) => { entry.1 += f; entry.2 += 1; }
                        Value::Null => {}
                        _ => return None,
                    }
            }
        }
        Some(groups.into_iter().map(|(key, (count, sum, nn))| {
            let avg = if nn > 0 { Some(sum / nn as f64) } else { None };
            (key, count, avg)
        }).collect())
    }

    fn fast_scan_where_eq(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<Vec<Row>> {
        let guard = self.tables.try_read().ok()?;
        let rows = guard.get(table)?;
        Some(rows.iter()
            .filter(|row| row.get(filter_col) == Some(filter_val))
            .cloned()
            .collect())
    }

    fn fast_scan_where_range(
        &self,
        table: &str,
        filter_col: usize,
        low: &Value,
        high: &Value,
    ) -> Option<Vec<Row>> {
        let guard = self.tables.try_read().ok()?;
        let rows = guard.get(table)?;
        Some(rows.iter()
            .filter(|row| {
                row.get(filter_col).is_some_and(|v| v >= low && v <= high)
            })
            .cloned()
            .collect())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("table '{0}' not found in storage")]
    TableNotFound(String),
    #[error("I/O error: {0}")]
    Io(String),
    #[error("write-write conflict: {0}")]
    WriteConflict(String),
    #[error("no active transaction")]
    NoActiveTransaction,
    #[error("ERROR: {0}")]
    SerializationFailure(String),
}

pub use mvcc::MvccStorageAdapter;
