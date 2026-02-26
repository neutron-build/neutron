//! Columnar storage engine — StorageEngine implementation backed by ColumnarStore.
//!
//! Tables are stored in column-oriented format (ColumnBatch slices) rather than
//! row vectors. Aggregate queries (COUNT, SUM, AVG, GROUP BY) are answered by
//! fast columnar passes that never materialise full rows.
//!
//! Column naming convention: columns within each ColumnBatch are named by their
//! zero-based scan-order position as a string: "0", "1", ..., "n-1". This
//! matches how MemoryEngine positions work and avoids any catalog dependency
//! inside the storage layer.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::columnar::{
    aggregate_count, aggregate_sum, group_by_text_agg_f64, ColumnBatch, ColumnData, ColumnarStore,
};
use crate::storage::columnar_wal::ColumnarWal;
use crate::storage::{StorageEngine, StorageError};
use crate::types::{Row, Value};

// ─── Write buffer ─────────────────────────────────────────────────────────────

/// Pending single-row inserts are buffered per-table. When this threshold is
/// reached the buffer is flushed as a single ColumnBatch to the store.
const WRITE_BUF_CAPACITY: usize = 256;

// ─── Index helpers ────────────────────────────────────────────────────────────

struct ColIdx {
    col_idx: usize,
    /// Key → sorted list of row positions in the columnar store.
    /// Storing positions (usize) instead of full rows keeps the BTreeMap
    /// ~4× smaller, dramatically improving cache utilisation during range scans.
    /// Rows are fetched on demand from the ColumnData arrays via
    /// `fetch_rows_by_positions`.
    ///
    /// Invariant: all positions refer to rows that have been flushed to the
    /// ColumnarStore. Write-buffer rows are NOT indexed until flush.
    map: BTreeMap<Value, Vec<usize>>,
}

// ─── Engine ──────────────────────────────────────────────────────────────────

/// In-memory columnar storage engine with optional WAL-backed durability.
///
/// Uses parking_lot (sync) locks throughout because all operations are
/// pure in-memory — no I/O means no need for async locks.
///
/// - `ColumnarStorageEngine::new()` — purely in-memory, no durability.
/// - `ColumnarStorageEngine::open(dir)` — persists mutations to a WAL file
///   in `dir` and recovers state on restart.
pub struct ColumnarStorageEngine {
    store: RwLock<ColumnarStore>,
    /// index_name → ColIdx
    indexes: RwLock<HashMap<String, ColIdx>>,
    /// table → [index_name]
    table_idx_names: RwLock<HashMap<String, Vec<String>>>,
    /// Pending single-row inserts per table, flushed when full or on any read.
    write_buffers: RwLock<HashMap<String, Vec<Row>>>,
    /// WAL for crash-recovery. None = purely in-memory.
    wal: Option<Arc<ColumnarWal>>,
}

impl ColumnarStorageEngine {
    /// Create a purely in-memory columnar engine (no durability).
    pub fn new() -> Self {
        Self {
            store: RwLock::new(ColumnarStore::new()),
            indexes: RwLock::new(HashMap::new()),
            table_idx_names: RwLock::new(HashMap::new()),
            write_buffers: RwLock::new(HashMap::new()),
            wal: None,
        }
    }

    /// Open (or create) a WAL-backed columnar engine in `dir`.
    ///
    /// Existing data is recovered by replaying the WAL log. Subsequent
    /// mutations are appended to the log; `flush_all_dirty` checkpoints the
    /// log to a compact single-snapshot file.
    pub fn open(dir: &std::path::Path) -> Result<Self, StorageError> {
        let (wal, state) = ColumnarWal::open(dir)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let mut store = ColumnarStore::new();
        // Restore tables from WAL state.
        for (table_name, rows) in &state.tables {
            store.create_table(table_name);
            if !rows.is_empty() {
                store.append(table_name, rows_to_batch(rows.clone()));
            }
        }
        Ok(Self {
            store: RwLock::new(store),
            indexes: RwLock::new(HashMap::new()),
            table_idx_names: RwLock::new(HashMap::new()),
            write_buffers: RwLock::new(HashMap::new()),
            wal: Some(Arc::new(wal)),
        })
    }

    /// Collect current table state for WAL checkpoint / snapshot.
    fn snapshot_tables(&self) -> Vec<(String, Vec<Row>)> {
        let store = self.store.read();
        store
            .table_names()
            .into_iter()
            .map(|name| {
                let rows = batches_to_rows(store.batches(&name));
                (name, rows)
            })
            .collect()
    }
}

impl Default for ColumnarStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ColumnarStorageEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarStorageEngine").finish()
    }
}

// ─── Value ↔ ColumnData helpers ──────────────────────────────────────────────

#[allow(dead_code)]
fn val_to_coldata(v: Value) -> ColumnData {
    match v {
        Value::Bool(b) => ColumnData::Bool(vec![Some(b)]),
        Value::Int32(n) => ColumnData::Int32(vec![Some(n)]),
        Value::Int64(n) => ColumnData::Int64(vec![Some(n)]),
        Value::Float64(f) => ColumnData::Float64(vec![Some(f)]),
        Value::Text(s) => ColumnData::Text(vec![Some(s)]),
        Value::Null => ColumnData::Text(vec![None]),
        other => ColumnData::Text(vec![Some(other.to_string())]),
    }
}

fn vals_to_coldata(vals: Vec<Value>) -> ColumnData {
    // Determine type from first non-null value.
    let first_non_null = vals.iter().find(|v| !matches!(v, Value::Null));
    match first_non_null {
        Some(Value::Bool(_)) => ColumnData::Bool(
            vals.into_iter()
                .map(|v| match v {
                    Value::Bool(b) => Some(b),
                    Value::Null => None,
                    _ => None,
                })
                .collect(),
        ),
        Some(Value::Int32(_)) => ColumnData::Int32(
            vals.into_iter()
                .map(|v| match v {
                    Value::Int32(n) => Some(n),
                    Value::Null => None,
                    _ => None,
                })
                .collect(),
        ),
        Some(Value::Int64(_)) => ColumnData::Int64(
            vals.into_iter()
                .map(|v| match v {
                    Value::Int64(n) => Some(n),
                    Value::Int32(n) => Some(n as i64),
                    Value::Null => None,
                    _ => None,
                })
                .collect(),
        ),
        Some(Value::Float64(_)) => ColumnData::Float64(
            vals.into_iter()
                .map(|v| match v {
                    Value::Float64(f) => Some(f),
                    Value::Int64(n) => Some(n as f64),
                    Value::Int32(n) => Some(n as f64),
                    Value::Null => None,
                    _ => None,
                })
                .collect(),
        ),
        _ => ColumnData::Text(
            vals.into_iter()
                .map(|v| match v {
                    Value::Null => None,
                    Value::Text(s) => Some(s),
                    other => Some(other.to_string()),
                })
                .collect(),
        ),
    }
}

/// Convert a single row to a one-row ColumnBatch with columns named "0","1",...
#[allow(dead_code)]
fn row_to_batch(row: Row) -> ColumnBatch {
    let columns = row
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i.to_string(), val_to_coldata(v)))
        .collect();
    ColumnBatch::new(columns)
}

/// Convert N rows to one wide ColumnBatch. Each column is a contiguous slice of
/// all rows' values at that position — the key performance win vs row-at-a-time.
fn rows_to_batch(rows: Vec<Row>) -> ColumnBatch {
    if rows.is_empty() {
        return ColumnBatch::new(Vec::new());
    }
    let n_cols = rows[0].len();
    let columns = (0..n_cols)
        .map(|col_i| {
            let vals: Vec<Value> = rows.iter().map(|row| row[col_i].clone()).collect();
            (col_i.to_string(), vals_to_coldata(vals))
        })
        .collect();
    ColumnBatch::new(columns)
}

/// Extract a Value from a ColumnData at `idx`.
fn coldata_get(col: &ColumnData, idx: usize) -> Value {
    match col {
        ColumnData::Bool(v) => v.get(idx).copied().flatten().map(Value::Bool).unwrap_or(Value::Null),
        ColumnData::Int32(v) => v.get(idx).copied().flatten().map(Value::Int32).unwrap_or(Value::Null),
        ColumnData::Int64(v) => v.get(idx).copied().flatten().map(Value::Int64).unwrap_or(Value::Null),
        ColumnData::Float64(v) => v.get(idx).copied().flatten().map(Value::Float64).unwrap_or(Value::Null),
        ColumnData::Text(v) => v
            .get(idx)
            .and_then(|o| o.as_ref())
            .map(|s| Value::Text(s.clone()))
            .unwrap_or(Value::Null),
    }
}

/// Reconstruct `Vec<Row>` from a slice of ColumnBatches.
fn batches_to_rows(batches: &[ColumnBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_i in 0..batch.row_count {
            let row: Row = (0..batch.columns.len())
                .map(|col_i| {
                    let (_, col) = &batch.columns[col_i];
                    coldata_get(col, row_i)
                })
                .collect();
            rows.push(row);
        }
    }
    rows
}

/// Reconstruct only rows where `batches[*][filter_col] == filter_val`.
/// Avoids allocating Value objects for non-matching rows.
fn batches_to_rows_where_eq(
    batches: &[ColumnBatch],
    filter_col: usize,
    filter_val: &Value,
) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let Some((_, filter_data)) = batch.columns.get(filter_col) else { continue };
        let mask = eq_mask(filter_data, filter_val);
        let n_cols = batch.columns.len();
        for row_i in 0..batch.row_count {
            if !mask.get(row_i).copied().unwrap_or(false) {
                continue;
            }
            let row: Row = (0..n_cols)
                .map(|col_i| {
                    let (_, col) = &batch.columns[col_i];
                    coldata_get(col, row_i)
                })
                .collect();
            rows.push(row);
        }
    }
    rows
}

/// Fetch rows from a slice of ColumnBatches by their global (scan-order) positions.
///
/// Global position 0 = first row of first batch, len(batch0) = first row of second batch, etc.
/// Positions need not be sorted. Unresolvable positions produce a row of Nulls.
fn fetch_rows_by_positions(batches: &[ColumnBatch], positions: &[usize]) -> Vec<Row> {
    if positions.is_empty() || batches.is_empty() {
        return Vec::new();
    }
    let n_cols = batches[0].columns.len();

    // Precompute cumulative batch offsets so we can binary-search to the right batch.
    let mut offsets = Vec::with_capacity(batches.len() + 1);
    offsets.push(0usize);
    for b in batches {
        offsets.push(offsets.last().unwrap() + b.row_count);
    }
    let total = *offsets.last().unwrap();

    let mut result = Vec::with_capacity(positions.len());
    for &global_pos in positions {
        if global_pos >= total {
            result.push(vec![Value::Null; n_cols]);
            continue;
        }
        // Binary-search for the batch that contains global_pos.
        let batch_idx = offsets.partition_point(|&o| o <= global_pos) - 1;
        let local_pos = global_pos - offsets[batch_idx];
        let batch = &batches[batch_idx];
        let row: Row = (0..n_cols)
            .map(|col_i| {
                let (_, col) = &batch.columns[col_i];
                coldata_get(col, local_pos)
            })
            .collect();
        result.push(row);
    }
    result
}

/// Compute an equality boolean mask for one column.
fn eq_mask(col: &ColumnData, val: &Value) -> Vec<bool> {
    match (col, val) {
        (ColumnData::Text(v), Value::Text(s)) => {
            v.iter().map(|o| o.as_deref() == Some(s.as_str())).collect()
        }
        (ColumnData::Int64(v), Value::Int64(n)) => {
            v.iter().map(|o| o == &Some(*n)).collect()
        }
        (ColumnData::Int32(v), Value::Int32(n)) => {
            v.iter().map(|o| o == &Some(*n)).collect()
        }
        // Cross-type: Int32 stored, Int64 predicate
        (ColumnData::Int32(v), Value::Int64(n)) => {
            if let Ok(n32) = i32::try_from(*n) {
                v.iter().map(|o| o == &Some(n32)).collect()
            } else {
                vec![false; v.len()]
            }
        }
        // Cross-type: Int64 stored, Int32 predicate
        (ColumnData::Int64(v), Value::Int32(n)) => {
            let n64 = *n as i64;
            v.iter().map(|o| o == &Some(n64)).collect()
        }
        (ColumnData::Float64(v), Value::Float64(f)) => {
            v.iter().map(|o| o == &Some(*f)).collect()
        }
        (ColumnData::Bool(v), Value::Bool(b)) => {
            v.iter().map(|o| o == &Some(*b)).collect()
        }
        _ => vec![false; col.len()],
    }
}

/// Sum the numeric values in `col` at positions where `mask[i]` is true.
fn sum_masked(col: &ColumnData, mask: &[bool]) -> (f64, usize) {
    let mut sum = 0.0f64;
    let mut count = 0usize;
    match col {
        ColumnData::Float64(v) => {
            for (opt, &keep) in v.iter().zip(mask) {
                if keep {
                    if let Some(f) = opt {
                        sum += f;
                        count += 1;
                    }
                }
            }
        }
        ColumnData::Int64(v) => {
            for (opt, &keep) in v.iter().zip(mask) {
                if keep {
                    if let Some(n) = opt {
                        sum += *n as f64;
                        count += 1;
                    }
                }
            }
        }
        ColumnData::Int32(v) => {
            for (opt, &keep) in v.iter().zip(mask) {
                if keep {
                    if let Some(n) = opt {
                        sum += *n as f64;
                        count += 1;
                    }
                }
            }
        }
        _ => {}
    }
    (sum, count)
}

// ─── Index helpers ────────────────────────────────────────────────────────────

impl ColumnarStorageEngine {
    /// Assign positions to `new_rows` starting at `starting_pos` and record them
    /// in every active index for `table`. Must only be called after the rows have
    /// been appended to the ColumnarStore (so positions are stable).
    fn update_indexes_at_positions(&self, table: &str, new_rows: &[Row], starting_pos: usize) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() {
            return;
        }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                for (i, row) in new_rows.iter().enumerate() {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map.entry(val).or_default().push(starting_pos + i);
                }
            }
        }
    }

    /// Flush any buffered single-row inserts for `table` to the columnar store,
    /// then update all active indexes with the correct positions for those rows.
    /// WAL logging for these rows already happened in `insert()`.
    fn flush_write_buffer(&self, table: &str) {
        let buf = {
            let mut bufs = self.write_buffers.write();
            match bufs.get_mut(table) {
                Some(b) if !b.is_empty() => std::mem::take(b),
                _ => return,
            }
        };
        // The starting position for buffered rows = current store row count (before append).
        let starting_pos = self.store.read().row_count(table);
        self.store.write().append(table, rows_to_batch(buf.clone()));
        // Now assign stable positions to the newly flushed rows.
        self.update_indexes_at_positions(table, &buf, starting_pos);
    }

    fn rebuild_indexes(&self, table: &str) {
        // Ensure buffered rows are in the store before rebuilding index.
        self.flush_write_buffer(table);
        // Rebuild position-based index from store contents.
        let row_count = self.store.read().row_count(table);
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() {
            return;
        }
        // Read all rows once — we need values per column for each row.
        let rows = batches_to_rows(self.store.read().batches(table));
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                idx.map.clear();
                for (pos, row) in rows.iter().enumerate().take(row_count) {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map.entry(val).or_default().push(pos);
                }
            }
        }
    }
}

// ─── StorageEngine impl ───────────────────────────────────────────────────────

#[async_trait]
impl StorageEngine for ColumnarStorageEngine {
    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        self.store.write().create_table(table);
        if let Some(wal) = &self.wal {
            let _ = wal.log_create_table(table);
        }
        Ok(())
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        // Discard any pending write buffer for this table.
        self.write_buffers.write().remove(table);
        let existed = self.store.write().drop_table(table);
        if !existed {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        if let Some(wal) = &self.wal {
            let _ = wal.log_drop_table(table);
        }
        // Remove index entries for this table.
        let names: Vec<String> = {
            let mut tnames = self.table_idx_names.write();
            tnames.remove(table).unwrap_or_default()
        };
        if !names.is_empty() {
            let mut indexes = self.indexes.write();
            for name in names {
                indexes.remove(&name);
            }
        }
        Ok(())
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        // Verify table exists before buffering.
        if !self.store.read().table_exists(table) {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        // WAL: log the row immediately so it is durable even before the
        // write buffer flushes to the in-memory store.
        if let Some(wal) = &self.wal {
            let _ = wal.log_insert_rows(table, std::slice::from_ref(&row));
        }
        let should_flush = {
            let mut bufs = self.write_buffers.write();
            let buf = bufs.entry(table.to_string()).or_default();
            buf.push(row.clone());
            buf.len() >= WRITE_BUF_CAPACITY
        };
        if should_flush {
            // flush_write_buffer also updates indexes with stable positions.
            self.flush_write_buffer(table);
        }
        // Index update for buffered rows is DEFERRED to flush_write_buffer.
        // This is correct because index_lookup_sync / index_lookup_range_sync
        // always call flush_write_buffer before querying the index.
        Ok(())
    }

    async fn insert_batch(&self, table: &str, rows: Vec<Row>) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }
        // Compute starting position before appending (store count before rows land).
        let starting_pos = self.store.read().row_count(table);
        {
            let mut store = self.store.write();
            if !store.table_exists(table) {
                return Err(StorageError::TableNotFound(table.to_string()));
            }
            // Single contiguous batch for all rows — the key perf win.
            store.append(table, rows_to_batch(rows.clone()));
        }
        if let Some(wal) = &self.wal {
            let _ = wal.log_insert_rows(table, &rows);
        }
        // Rows are now in the store with stable positions — update indexes immediately.
        self.update_indexes_at_positions(table, &rows, starting_pos);
        Ok(())
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        Ok(batches_to_rows(store.batches(table)))
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        if positions.is_empty() {
            return Ok(0);
        }
        self.flush_write_buffer(table);
        let pos_set: std::collections::HashSet<usize> = positions.iter().copied().collect();
        let count = {
            let mut store = self.store.write();
            if !store.table_exists(table) {
                return Err(StorageError::TableNotFound(table.to_string()));
            }
            let old_rows = batches_to_rows(store.batches(table));
            let total = old_rows.len();
            let new_rows: Vec<Row> = old_rows
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !pos_set.contains(i))
                .map(|(_, r)| r)
                .collect();
            let kept = new_rows.len();
            store.clear(table);
            if !new_rows.is_empty() {
                store.append(table, rows_to_batch(new_rows));
            }
            total - kept
        };
        self.rebuild_indexes(table);
        // DELETE can't be expressed as an INSERT — checkpoint full state.
        if let Some(wal) = &self.wal {
            let tables = self.snapshot_tables();
            let refs: Vec<(&str, Vec<Row>)> = tables.iter().map(|(n, r)| (n.as_str(), r.clone())).collect();
            let _ = wal.checkpoint(&refs);
        }
        Ok(count)
    }

    async fn update(
        &self,
        table: &str,
        updates: &[(usize, Row)],
    ) -> Result<usize, StorageError> {
        if updates.is_empty() {
            return Ok(0);
        }
        self.flush_write_buffer(table);
        let update_map: HashMap<usize, &Row> = updates.iter().map(|(p, r)| (*p, r)).collect();
        let count = {
            let mut store = self.store.write();
            if !store.table_exists(table) {
                return Err(StorageError::TableNotFound(table.to_string()));
            }
            let old_rows = batches_to_rows(store.batches(table));
            let mut changed = 0usize;
            let new_rows: Vec<Row> = old_rows
                .into_iter()
                .enumerate()
                .map(|(i, row)| {
                    if let Some(&new_row) = update_map.get(&i) {
                        changed += 1;
                        new_row.clone()
                    } else {
                        row
                    }
                })
                .collect();
            store.clear(table);
            if !new_rows.is_empty() {
                store.append(table, rows_to_batch(new_rows));
            }
            changed
        };
        self.rebuild_indexes(table);
        // UPDATE can't be expressed as an INSERT — checkpoint full state.
        if let Some(wal) = &self.wal {
            let tables = self.snapshot_tables();
            let refs: Vec<(&str, Vec<Row>)> = tables.iter().map(|(n, r)| (n.as_str(), r.clone())).collect();
            let _ = wal.checkpoint(&refs);
        }
        Ok(count)
    }

    async fn create_index(
        &self,
        table: &str,
        index_name: &str,
        col_idx: usize,
    ) -> Result<(), StorageError> {
        // Flush write buffer so all rows have stable positions in the store.
        self.flush_write_buffer(table);
        let rows = {
            let store = self.store.read();
            batches_to_rows(store.batches(table))
        };
        // Build position-based index: key → list of row positions.
        let mut map: BTreeMap<Value, Vec<usize>> = BTreeMap::new();
        for (pos, row) in rows.iter().enumerate() {
            let val = row.get(col_idx).cloned().unwrap_or(Value::Null);
            map.entry(val).or_default().push(pos);
        }
        {
            let mut indexes = self.indexes.write();
            indexes.insert(index_name.to_string(), ColIdx { col_idx, map });
        }
        {
            let mut tnames = self.table_idx_names.write();
            tnames
                .entry(table.to_string())
                .or_default()
                .push(index_name.to_string());
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

    fn index_lookup_sync(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // Flush write buffer so all single-row inserts have stable positions.
        self.flush_write_buffer(table);
        let positions: Vec<usize> = {
            let indexes = self.indexes.read();
            match indexes.get(index_name) {
                Some(idx) => idx.map.get(value).cloned().unwrap_or_default(),
                None => return Ok(None),
            }
        };
        let store = self.store.read();
        Ok(Some(fetch_rows_by_positions(store.batches(table), &positions)))
    }

    fn index_lookup_range_sync(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // Flush write buffer so all single-row inserts have stable positions.
        self.flush_write_buffer(table);
        let positions: Vec<usize> = {
            let indexes = self.indexes.read();
            match indexes.get(index_name) {
                Some(idx) => {
                    // BTreeMap::range: O(log n) seek + O(k) scan over compact position entries.
                    idx.map
                        .range(low..=high)
                        .flat_map(|(_, pos)| pos.iter().copied())
                        .collect()
                }
                None => return Ok(None),
            }
        };
        let store = self.store.read();
        Ok(Some(fetch_rows_by_positions(store.batches(table), &positions)))
    }

    // ─── Aggregate fast paths ─────────────────────────────────────────────────

    fn fast_count_all(&self, table: &str) -> Option<usize> {
        self.flush_write_buffer(table);
        let store = self.store.read();
        if store.table_exists(table) {
            Some(store.row_count(table))
        } else {
            None
        }
    }

    fn fast_sum_f64(&self, table: &str, col_idx: usize) -> Option<(f64, usize)> {
        self.flush_write_buffer(table);
        let col_name = col_idx.to_string();
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let (total, n) =
            store
                .batches(table)
                .iter()
                .fold((0.0f64, 0usize), |(s, c), batch| {
                    let sum = aggregate_sum(batch, &col_name);
                    let cnt = aggregate_count(batch, &col_name);
                    (s + sum, c + cnt)
                });
        Some((total, n))
    }

    fn fast_group_by(
        &self,
        table: &str,
        key_col: usize,
        val_col: Option<usize>,
    ) -> Option<Vec<(Value, i64, Option<f64>)>> {
        self.flush_write_buffer(table);
        let key_col_name = key_col.to_string();
        let val_col_name = val_col.map(|c| c.to_string());
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let batches = store.batches(table);

        // Collect the key and value columns across all batches.
        let mut key_vec: Vec<Option<String>> = Vec::new();
        let mut val_vec: Vec<Option<f64>> = Vec::new();

        for batch in batches {
            let n = batch.row_count;
            // Key column — always text-converted.
            match batch.column(&key_col_name) {
                Some(ColumnData::Text(v)) => key_vec.extend(v.iter().cloned()),
                Some(ColumnData::Int32(v)) => key_vec
                    .extend(v.iter().map(|o| o.map(|n| n.to_string()))),
                Some(ColumnData::Int64(v)) => key_vec
                    .extend(v.iter().map(|o| o.map(|n| n.to_string()))),
                Some(ColumnData::Float64(v)) => key_vec
                    .extend(v.iter().map(|o| o.map(|n| n.to_string()))),
                Some(ColumnData::Bool(v)) => key_vec
                    .extend(v.iter().map(|o| o.map(|b| b.to_string()))),
                None => key_vec.extend(std::iter::repeat_n(None, n)),
            }
            // Value column (optional) — numeric only.
            if let Some(ref vc) = val_col_name {
                match batch.column(vc) {
                    Some(ColumnData::Float64(v)) => val_vec.extend(v.iter().copied()),
                    Some(ColumnData::Int64(v)) => {
                        val_vec.extend(v.iter().map(|o| o.map(|n| n as f64)))
                    }
                    Some(ColumnData::Int32(v)) => {
                        val_vec.extend(v.iter().map(|o| o.map(|n| n as f64)))
                    }
                    _ => val_vec.extend(std::iter::repeat_n(None, n)),
                }
            } else {
                val_vec.extend(std::iter::repeat_n(None, n));
            }
        }

        if val_col_name.is_some() {
            // SUM/AVG requested — use the full vectorized path.
            let result = group_by_text_agg_f64(&key_vec, &val_vec);
            Some(
                result
                    .groups
                    .into_iter()
                    .map(|g| (Value::Text(g.key), g.count as i64, g.avg))
                    .collect(),
            )
        } else {
            // COUNT(*) only — count occurrences of each key directly.
            let mut counts: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            for k in key_vec.into_iter().flatten() {
                *counts.entry(k).or_insert(0) += 1;
            }
            Some(
                counts
                    .into_iter()
                    .map(|(k, cnt)| (Value::Text(k), cnt, None))
                    .collect(),
            )
        }
    }

    fn fast_count_filtered(&self, table: &str, filter_col: usize, filter_val: &Value) -> Option<usize> {
        self.flush_write_buffer(table);
        let filter_col_name = filter_col.to_string();
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let count = store.batches(table).iter().map(|batch| {
            match batch.column(&filter_col_name) {
                Some(col) => eq_mask(col, filter_val).iter().filter(|&&b| b).count(),
                None => 0,
            }
        }).sum();
        Some(count)
    }

    fn fast_sum_f64_filtered(
        &self,
        table: &str,
        val_col: usize,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<(f64, usize)> {
        self.flush_write_buffer(table);
        let val_col_name = val_col.to_string();
        let filter_col_name = filter_col.to_string();
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let (sum, count) = store.batches(table).iter().fold((0.0f64, 0usize), |(s, c), batch| {
            let filter_data = match batch.column(&filter_col_name) {
                Some(d) => d,
                None => return (s, c),
            };
            let mask = eq_mask(filter_data, filter_val);
            let val_data = match batch.column(&val_col_name) {
                Some(d) => d,
                None => return (s, c),
            };
            let (bs, bc) = sum_masked(val_data, &mask);
            (s + bs, c + bc)
        });
        Some((sum, count))
    }

    fn fast_min_f64(&self, table: &str, col_idx: usize) -> Option<f64> {
        self.flush_write_buffer(table);
        let col_name = col_idx.to_string();
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let mut min: Option<f64> = None;
        for batch in store.batches(table) {
            match batch.column(&col_name) {
                Some(ColumnData::Float64(v)) => {
                    for f in v.iter().flatten() {
                        min = Some(min.map_or(*f, |m: f64| m.min(*f)));
                    }
                }
                Some(ColumnData::Int64(v)) => {
                    for n in v.iter().flatten() {
                        let f = *n as f64;
                        min = Some(min.map_or(f, |m: f64| m.min(f)));
                    }
                }
                Some(ColumnData::Int32(v)) => {
                    for n in v.iter().flatten() {
                        let f = *n as f64;
                        min = Some(min.map_or(f, |m: f64| m.min(f)));
                    }
                }
                _ => {}
            }
        }
        min
    }

    fn fast_max_f64(&self, table: &str, col_idx: usize) -> Option<f64> {
        self.flush_write_buffer(table);
        let col_name = col_idx.to_string();
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let mut max: Option<f64> = None;
        for batch in store.batches(table) {
            match batch.column(&col_name) {
                Some(ColumnData::Float64(v)) => {
                    for f in v.iter().flatten() {
                        max = Some(max.map_or(*f, |m: f64| m.max(*f)));
                    }
                }
                Some(ColumnData::Int64(v)) => {
                    for n in v.iter().flatten() {
                        let f = *n as f64;
                        max = Some(max.map_or(f, |m: f64| m.max(f)));
                    }
                }
                Some(ColumnData::Int32(v)) => {
                    for n in v.iter().flatten() {
                        let f = *n as f64;
                        max = Some(max.map_or(f, |m: f64| m.max(f)));
                    }
                }
                _ => {}
            }
        }
        max
    }

    fn fast_scan_where_eq(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<Vec<Row>> {
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        Some(batches_to_rows_where_eq(store.batches(table), filter_col, filter_val))
    }

    async fn flush_all_dirty(&self) -> Result<(), StorageError> {
        // Flush all per-table write buffers to the columnar store.
        let tables: Vec<String> = self.write_buffers.read().keys().cloned().collect();
        for table in tables {
            self.flush_write_buffer(&table);
        }
        // Checkpoint WAL to a compact single-snapshot file.
        if let Some(wal) = &self.wal {
            let snap = self.snapshot_tables();
            let refs: Vec<(&str, Vec<Row>)> = snap.iter().map(|(n, r)| (n.as_str(), r.clone())).collect();
            wal.checkpoint(&refs).map_err(|e| StorageError::Io(e.to_string()))?;
        }
        Ok(())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64, name: &str, amount: f64) -> Row {
        vec![Value::Int64(id), Value::Text(name.to_string()), Value::Float64(amount)]
    }

    #[tokio::test]
    async fn test_create_insert_scan() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        eng.insert("t", row(1, "alice", 10.0)).await.unwrap();
        eng.insert("t", row(2, "bob", 20.0)).await.unwrap();
        let rows = eng.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int64(1));
        assert_eq!(rows[1][1], Value::Text("bob".into()));
    }

    #[tokio::test]
    async fn test_insert_batch() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        let rows_in: Vec<Row> = (1..=5).map(|i| row(i, "x", i as f64)).collect();
        eng.insert_batch("t", rows_in).await.unwrap();
        let rows = eng.scan("t").await.unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[tokio::test]
    async fn test_delete() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        eng.insert("t", row(1, "alice", 1.0)).await.unwrap();
        eng.insert("t", row(2, "bob", 2.0)).await.unwrap();
        eng.insert("t", row(3, "carol", 3.0)).await.unwrap();
        eng.delete("t", &[1]).await.unwrap();
        let rows = eng.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r[1] != Value::Text("bob".into())));
    }

    #[tokio::test]
    async fn test_update() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        eng.insert("t", row(1, "alice", 1.0)).await.unwrap();
        eng.insert("t", row(2, "bob", 2.0)).await.unwrap();
        eng.update("t", &[(0, row(99, "updated", 99.0))]).await.unwrap();
        let rows = eng.scan("t").await.unwrap();
        assert_eq!(rows[0][0], Value::Int64(99));
    }

    #[tokio::test]
    async fn test_drop_table() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        eng.insert("t", row(1, "a", 1.0)).await.unwrap();
        eng.drop_table("t").await.unwrap();
        let r = eng.scan("t").await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_fast_count_all() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        for i in 1..=100i64 {
            eng.insert("t", row(i, "x", i as f64)).await.unwrap();
        }
        assert_eq!(eng.fast_count_all("t"), Some(100));
    }

    #[tokio::test]
    async fn test_fast_sum_f64() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        let rows_in: Vec<Row> = (1..=10).map(|i| row(i, "x", i as f64)).collect();
        eng.insert_batch("t", rows_in).await.unwrap();
        // col 2 is amount (Float64)
        let (sum, cnt) = eng.fast_sum_f64("t", 2).unwrap();
        assert!((sum - 55.0).abs() < 1e-9);
        assert_eq!(cnt, 10);
    }

    #[tokio::test]
    async fn test_fast_group_by() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        // 2 rows with status "a", 1 with "b"
        eng.insert("t", vec![Value::Text("a".into()), Value::Float64(10.0)]).await.unwrap();
        eng.insert("t", vec![Value::Text("a".into()), Value::Float64(20.0)]).await.unwrap();
        eng.insert("t", vec![Value::Text("b".into()), Value::Float64(30.0)]).await.unwrap();
        let groups = eng.fast_group_by("t", 0, Some(1)).unwrap();
        let a = groups.iter().find(|(k, _, _)| k == &Value::Text("a".into())).unwrap();
        let b = groups.iter().find(|(k, _, _)| k == &Value::Text("b".into())).unwrap();
        assert_eq!(a.1, 2);
        assert!((a.2.unwrap() - 15.0).abs() < 1e-9);
        assert_eq!(b.1, 1);
    }

    #[tokio::test]
    async fn test_index_lookup() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        eng.insert("t", row(1, "alice", 10.0)).await.unwrap();
        eng.insert("t", row(2, "bob", 20.0)).await.unwrap();
        eng.create_index("t", "t_id_idx", 0).await.unwrap();
        let result = eng.index_lookup_sync("t", "t_id_idx", &Value::Int64(2)).unwrap();
        assert!(result.is_some());
        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(2));
    }

    #[tokio::test]
    async fn test_insert_not_found() {
        let eng = ColumnarStorageEngine::new();
        let result = eng.insert("missing", row(1, "x", 1.0)).await;
        assert!(result.is_err());
    }

    // ─── Write buffer tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_write_buffer_accumulates_and_flushes_on_scan() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        // Insert fewer than WRITE_BUF_CAPACITY rows → stays in buffer
        for i in 0..10i64 {
            eng.insert("t", vec![Value::Int64(i), Value::Float64(i as f64)]).await.unwrap();
        }
        // scan() should flush the buffer and return all 10 rows
        let rows = eng.scan("t").await.unwrap();
        assert_eq!(rows.len(), 10);
    }

    #[tokio::test]
    async fn test_write_buffer_auto_flush_at_capacity() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        // Insert exactly WRITE_BUF_CAPACITY rows — triggers auto-flush on the 256th
        for i in 0..(WRITE_BUF_CAPACITY as i64) {
            eng.insert("t", vec![Value::Int64(i)]).await.unwrap();
        }
        // Buffer should be empty now (auto-flushed); store has all rows
        let buf_len = eng.write_buffers.read().get("t").map_or(0, |b| b.len());
        assert_eq!(buf_len, 0, "buffer should be empty after auto-flush");
        let rows = eng.scan("t").await.unwrap();
        assert_eq!(rows.len(), WRITE_BUF_CAPACITY);
    }

    #[tokio::test]
    async fn test_write_buffer_fast_count_sees_buffered_rows() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        // 5 rows buffered (not yet flushed to store)
        for i in 0..5i64 {
            eng.insert("t", vec![Value::Int64(i), Value::Float64(i as f64)]).await.unwrap();
        }
        // fast_count_all should flush then count
        assert_eq!(eng.fast_count_all("t"), Some(5));
    }

    #[tokio::test]
    async fn test_write_buffer_flush_all_dirty() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        for i in 0..20i64 {
            eng.insert("t", vec![Value::Int64(i)]).await.unwrap();
        }
        eng.flush_all_dirty().await.unwrap();
        // After flush_all_dirty, buffer should be empty
        let buf_len = eng.write_buffers.read().get("t").map_or(0, |b| b.len());
        assert_eq!(buf_len, 0);
        let rows = eng.scan("t").await.unwrap();
        assert_eq!(rows.len(), 20);
    }

    // ─── WAL-backed engine tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_wal_engine_open_create_insert_recover() {
        let dir = tempfile::tempdir().unwrap();

        // Session 1: create table and insert rows.
        {
            let eng = ColumnarStorageEngine::open(dir.path()).unwrap();
            eng.create_table("orders").await.unwrap();
            eng.insert_batch("orders", vec![
                row(1, "alice", 10.0),
                row(2, "bob",   20.0),
                row(3, "carol", 30.0),
            ]).await.unwrap();
        }

        // Session 2: reopen — rows must survive.
        {
            let eng = ColumnarStorageEngine::open(dir.path()).unwrap();
            let rows = eng.scan("orders").await.unwrap();
            assert_eq!(rows.len(), 3, "expected 3 rows after WAL recovery");
            assert_eq!(rows[0][0], Value::Int64(1));
            assert_eq!(rows[2][1], Value::Text("carol".into()));
        }
    }

    #[tokio::test]
    async fn test_wal_engine_drop_table_recovered() {
        let dir = tempfile::tempdir().unwrap();

        {
            let eng = ColumnarStorageEngine::open(dir.path()).unwrap();
            eng.create_table("t").await.unwrap();
            eng.insert("t", row(1, "x", 1.0)).await.unwrap();
            eng.drop_table("t").await.unwrap();
        }

        {
            let eng = ColumnarStorageEngine::open(dir.path()).unwrap();
            assert!(eng.scan("t").await.is_err(), "table should not exist after drop+recovery");
        }
    }

    #[tokio::test]
    async fn test_wal_engine_checkpoint_compacts() {
        let dir = tempfile::tempdir().unwrap();

        {
            let eng = ColumnarStorageEngine::open(dir.path()).unwrap();
            eng.create_table("t").await.unwrap();
            // Insert in many small batches to grow the WAL.
            for i in 0..50i64 {
                eng.insert("t", vec![Value::Int64(i)]).await.unwrap();
            }
            // flush_all_dirty writes a checkpoint (compact snapshot).
            eng.flush_all_dirty().await.unwrap();
            // Insert a few more rows after checkpoint.
            eng.insert("t", vec![Value::Int64(100)]).await.unwrap();
        }

        {
            let eng = ColumnarStorageEngine::open(dir.path()).unwrap();
            // The WAL file must have been compacted — verify all rows are present.
            let rows = eng.scan("t").await.unwrap();
            assert_eq!(rows.len(), 51, "50 initial + 1 post-checkpoint row");
        }
    }

    #[tokio::test]
    async fn test_wal_engine_fast_count_after_recovery() {
        let dir = tempfile::tempdir().unwrap();

        {
            let eng = ColumnarStorageEngine::open(dir.path()).unwrap();
            eng.create_table("t").await.unwrap();
            let batch: Vec<Row> = (1..=20).map(|i| vec![Value::Int64(i)]).collect();
            eng.insert_batch("t", batch).await.unwrap();
        }

        {
            let eng = ColumnarStorageEngine::open(dir.path()).unwrap();
            assert_eq!(eng.fast_count_all("t"), Some(20));
        }
    }
}
