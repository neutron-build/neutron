//! Columnar storage engine — native column-oriented storage for analytics.
//!
//! Stores data in column batches rather than rows. Each column batch contains
//! values of a single type, enabling:
//!   - Vectorized scan/filter/aggregate operations
//!   - Better compression (similar values stored together)
//!   - Cache-efficient sequential reads for analytical queries
//!
//! Replaces ClickHouse for OLAP workloads within Nucleus.

pub mod segment;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use regex::Regex;

use crate::storage::columnar_wal::ColumnarWal;
use crate::types::{Row, Value};

use segment::{ColdPartInfo, SegmentReader, SegmentWriter, estimate_batch_size, DEFAULT_COLD_THRESHOLD_BYTES};

// ============================================================================
// Column types
// ============================================================================

/// A column of typed values stored contiguously.
#[derive(Debug, Clone)]
pub enum ColumnData {
    Bool(Vec<Option<bool>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Text(Vec<Option<String>>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Bool(v) => v.len(),
            ColumnData::Int32(v) => v.len(),
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::Text(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A batch of columns representing a chunk of a table.
#[derive(Debug, Clone)]
pub struct ColumnBatch {
    pub columns: Vec<(String, ColumnData)>,
    pub row_count: usize,
}

impl ColumnBatch {
    pub fn new(columns: Vec<(String, ColumnData)>) -> Self {
        let row_count = columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        Self { columns, row_count }
    }

    /// Get a column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnData> {
        self.columns.iter().find(|(n, _)| n == name).map(|(_, c)| c)
    }
}

// ============================================================================
// Vectorized operations
// ============================================================================

/// Sum of an Int64 column (ignoring NULLs).
pub fn sum_i64(col: &[Option<i64>]) -> i64 {
    col.iter().filter_map(|v| v.as_ref()).sum()
}

/// Sum of a Float64 column (ignoring NULLs).
pub fn sum_f64(col: &[Option<f64>]) -> f64 {
    col.iter().filter_map(|v| v.as_ref()).sum()
}

/// Average of an Int64 column.
pub fn avg_i64(col: &[Option<i64>]) -> Option<f64> {
    let mut sum = 0i64;
    let mut count = 0usize;
    for v in col.iter().flatten() {
        sum += v;
        count += 1;
    }
    if count == 0 {
        None
    } else {
        Some(sum as f64 / count as f64)
    }
}

/// Average of a Float64 column.
pub fn avg_f64(col: &[Option<f64>]) -> Option<f64> {
    let mut sum = 0.0f64;
    let mut count = 0usize;
    for v in col.iter().flatten() {
        sum += v;
        count += 1;
    }
    if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    }
}

/// Min of an Int64 column.
pub fn min_i64(col: &[Option<i64>]) -> Option<i64> {
    col.iter().filter_map(|v| v.as_ref()).copied().min()
}

/// Max of an Int64 column.
pub fn max_i64(col: &[Option<i64>]) -> Option<i64> {
    col.iter().filter_map(|v| v.as_ref()).copied().max()
}

/// Min of a Float64 column.
pub fn min_f64(col: &[Option<f64>]) -> Option<f64> {
    col.iter()
        .filter_map(|v| v.as_ref())
        .copied()
        .reduce(f64::min)
}

/// Max of a Float64 column.
pub fn max_f64(col: &[Option<f64>]) -> Option<f64> {
    col.iter()
        .filter_map(|v| v.as_ref())
        .copied()
        .reduce(f64::max)
}

/// Count non-NULL values.
pub fn count_non_null(col: &ColumnData) -> usize {
    match col {
        ColumnData::Bool(v) => v.iter().filter(|x| x.is_some()).count(),
        ColumnData::Int32(v) => v.iter().filter(|x| x.is_some()).count(),
        ColumnData::Int64(v) => v.iter().filter(|x| x.is_some()).count(),
        ColumnData::Float64(v) => v.iter().filter(|x| x.is_some()).count(),
        ColumnData::Text(v) => v.iter().filter(|x| x.is_some()).count(),
    }
}

// ============================================================================
// Vectorized filter (predicate pushdown)
// ============================================================================

/// Filter predicate for column scans.
pub enum Predicate {
    /// Column equals a constant value.
    EqI64(i64),
    /// Column is greater than a constant.
    GtI64(i64),
    /// Column is less than a constant.
    LtI64(i64),
    /// Column is between two values (inclusive).
    BetweenI64(i64, i64),
    /// Column equals a string.
    EqText(String),
    /// Column contains a substring.
    ContainsText(String),
    /// Column starts with a prefix string.
    StartsWithText(String),
    /// Column matches a regex pattern.
    RegexText(String),
}

/// Apply a filter predicate to an Int64 column, returning a boolean mask.
pub fn filter_i64(col: &[Option<i64>], pred: &Predicate) -> Vec<bool> {
    match pred {
        Predicate::EqI64(target) => col.iter().map(|v| v.as_ref() == Some(target)).collect(),
        Predicate::GtI64(target) => col
            .iter()
            .map(|v| v.map(|x| x > *target).unwrap_or(false))
            .collect(),
        Predicate::LtI64(target) => col
            .iter()
            .map(|v| v.map(|x| x < *target).unwrap_or(false))
            .collect(),
        Predicate::BetweenI64(lo, hi) => col
            .iter()
            .map(|v| v.map(|x| x >= *lo && x <= *hi).unwrap_or(false))
            .collect(),
        _ => vec![true; col.len()],
    }
}

/// Apply a filter predicate to a Text column, returning a boolean mask.
pub fn filter_text(col: &[Option<String>], pred: &Predicate) -> Vec<bool> {
    match pred {
        Predicate::EqText(target) => col
            .iter()
            .map(|v| v.as_deref() == Some(target.as_str()))
            .collect(),
        Predicate::ContainsText(sub) => col
            .iter()
            .map(|v| {
                v.as_ref()
                    .map(|s| s.contains(sub.as_str()))
                    .unwrap_or(false)
            })
            .collect(),
        Predicate::StartsWithText(prefix) => col
            .iter()
            .map(|v| {
                v.as_ref()
                    .map(|s| s.starts_with(prefix.as_str()))
                    .unwrap_or(false)
            })
            .collect(),
        Predicate::RegexText(pattern) => {
            // Limit regex pattern length to prevent excessive NFA compilation time.
            const MAX_REGEX_PATTERN_LEN: usize = 1000;
            if pattern.len() > MAX_REGEX_PATTERN_LEN {
                return vec![false; col.len()];
            }
            match Regex::new(pattern) {
                Ok(re) => col.iter()
                    .map(|v| {
                        v.as_ref()
                            .map(|s| re.is_match(s))
                            .unwrap_or(false)
                    })
                    .collect(),
                Err(_) => vec![false; col.len()], // invalid regex matches nothing
            }
        }
        _ => vec![true; col.len()],
    }
}

/// Apply a boolean mask to select matching rows from a column batch.
pub fn apply_mask(batch: &ColumnBatch, mask: &[bool]) -> ColumnBatch {
    let mut new_columns = Vec::with_capacity(batch.columns.len());

    for (name, col) in &batch.columns {
        let filtered = match col {
            ColumnData::Bool(v) => ColumnData::Bool(
                v.iter()
                    .zip(mask.iter())
                    .filter(|&(_, &m)| m)
                    .map(|(v, _)| *v)
                    .collect(),
            ),
            ColumnData::Int32(v) => ColumnData::Int32(
                v.iter()
                    .zip(mask.iter())
                    .filter(|&(_, &m)| m)
                    .map(|(v, _)| *v)
                    .collect(),
            ),
            ColumnData::Int64(v) => ColumnData::Int64(
                v.iter()
                    .zip(mask.iter())
                    .filter(|&(_, &m)| m)
                    .map(|(v, _)| *v)
                    .collect(),
            ),
            ColumnData::Float64(v) => ColumnData::Float64(
                v.iter()
                    .zip(mask.iter())
                    .filter(|&(_, &m)| m)
                    .map(|(v, _)| *v)
                    .collect(),
            ),
            ColumnData::Text(v) => ColumnData::Text(
                v.iter()
                    .zip(mask.iter())
                    .filter(|&(_, &m)| m)
                    .map(|(v, _)| v.clone())
                    .collect(),
            ),
        };
        new_columns.push((name.clone(), filtered));
    }

    ColumnBatch::new(new_columns)
}

// ============================================================================
// Group-by aggregation
// ============================================================================

/// Result of a group-by aggregation.
#[derive(Debug)]
pub struct GroupByResult {
    pub groups: Vec<GroupByRow>,
}

/// A single group with its key and aggregated values.
#[derive(Debug)]
pub struct GroupByRow {
    pub key: String,
    pub count: usize,
    pub sum: Option<f64>,
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// Group-by on a text column with aggregation on a numeric column.
pub fn group_by_text_agg_f64(
    key_col: &[Option<String>],
    val_col: &[Option<f64>],
) -> GroupByResult {
    let mut groups: HashMap<String, (usize, f64, f64, f64)> = HashMap::new();
    // (count, sum, min, max)

    for (key, val) in key_col.iter().zip(val_col.iter()) {
        if let (Some(k), Some(v)) = (key, val) {
            let entry = groups.entry(k.clone()).or_insert((0, 0.0, f64::MAX, f64::MIN));
            entry.0 += 1;
            entry.1 += v;
            entry.2 = entry.2.min(*v);
            entry.3 = entry.3.max(*v);
        }
    }

    let rows = groups
        .into_iter()
        .map(|(key, (count, sum, min, max))| GroupByRow {
            key,
            count,
            sum: Some(sum),
            avg: Some(sum / count as f64),
            min: Some(min),
            max: Some(max),
        })
        .collect();

    GroupByResult { groups: rows }
}

// ============================================================================
// Columnar table store
// ============================================================================

/// In-memory columnar table store with optional WAL-backed persistence.
pub struct ColumnarStore {
    /// Table name → list of column batches.
    tables: HashMap<String, Vec<ColumnBatch>>,
    /// Table name → (column name → dictionary-encoded column).
    /// Populated by `append_with_dict` for eligible text columns.
    dict_columns: HashMap<String, HashMap<String, DictColumn>>,
    /// WAL for crash-recovery. None = purely in-memory.
    wal: Option<Arc<ColumnarWal>>,
    /// Table name → MergeTree instance for tables created with ENGINE = MergeTree.
    merge_trees: HashMap<String, MergeTree>,
}

impl std::fmt::Debug for ColumnarStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarStore")
            .field("tables", &self.tables)
            .field("dict_columns", &self.dict_columns)
            .field("wal", &self.wal.as_ref().map(|_| "ColumnarWal(...)"))
            .field("merge_trees", &self.merge_trees)
            .finish()
    }
}

impl Default for ColumnarStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarStore {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            dict_columns: HashMap::new(),
            wal: None,
            merge_trees: HashMap::new(),
        }
    }

    /// Open (or create) a WAL-backed columnar store in `dir`.
    ///
    /// Replays the WAL to recover table state, then attaches the WAL for
    /// subsequent mutation logging. Subsequent calls to `create_table`,
    /// `drop_table`, `append`, and `clear` are durably logged.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        let (wal, state) = ColumnarWal::open(dir)?;
        let wal = Arc::new(wal);
        let mut store = Self {
            tables: HashMap::new(),
            dict_columns: HashMap::new(),
            wal: Some(Arc::clone(&wal)),
            merge_trees: HashMap::new(),
        };
        // Replay recovered state: CREATE_TABLE entries first, then INSERT_ROWS.
        for (table_name, rows) in state.tables {
            store.tables.entry(table_name.clone()).or_default();
            if !rows.is_empty() {
                let batch = rows_to_batch(rows);
                store.tables.get_mut(&table_name).unwrap().push(batch);
            }
        }
        Ok(store)
    }

    /// Append a batch to a table.
    ///
    /// If the table has a MergeTree backing store, the batch is inserted there
    /// (sorted by PK, zone-mapped). Otherwise falls back to raw batch storage.
    /// If a WAL is attached the rows are logged before the in-memory update.
    pub fn append(&mut self, table: &str, batch: ColumnBatch) {
        if let Some(ref wal) = self.wal {
            let rows = batch_to_rows(&batch);
            if let Err(e) = wal.log_insert_rows(table, &rows) {
                eprintln!("columnar WAL: failed to log insert_rows for {table}: {e}");
            }
        }
        if let Some(mt) = self.merge_trees.get_mut(table) {
            mt.insert(batch);
        } else {
            self.tables
                .entry(table.to_string())
                .or_default()
                .push(batch);
        }
    }

    /// Get all batches for a table as a slice reference.
    ///
    /// For raw (non-MergeTree) tables this returns the stored Vec<ColumnBatch>.
    /// For MergeTree-backed tables, use `batches_all()` instead (this returns
    /// an empty slice because MergeTree data lives in parts, not in `tables`).
    pub fn batches(&self, table: &str) -> &[ColumnBatch] {
        self.tables.get(table).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get all batches for a table, including MergeTree-backed tables.
    ///
    /// Returns owned Vec. For MergeTree-backed tables, cold parts are loaded
    /// from disk as needed.
    pub fn batches_all(&self, table: &str) -> Vec<ColumnBatch> {
        if let Some(mt) = self.merge_trees.get(table) {
            mt.scan_all()
        } else {
            self.tables.get(table).cloned().unwrap_or_default()
        }
    }

    /// Total row count across all batches for a table.
    pub fn row_count(&self, table: &str) -> usize {
        if let Some(mt) = self.merge_trees.get(table) {
            mt.total_rows()
        } else {
            self.batches(table).iter().map(|b| b.row_count).sum()
        }
    }

    /// Ensure a table entry exists (creates an empty table if absent).
    pub fn create_table(&mut self, table: &str) {
        if let Some(ref wal) = self.wal {
            if let Err(e) = wal.log_create_table(table) {
                eprintln!("columnar WAL: failed to log create_table {table}: {e}");
            }
        }
        self.tables.entry(table.to_string()).or_default();
    }

    /// Create a MergeTree-backed table with the given primary key columns.
    ///
    /// The table entry is also created in `tables` for compatibility, but
    /// all insert/scan operations will be routed through the MergeTree.
    pub fn create_merge_tree_table(&mut self, table: &str, order_by: Vec<String>) {
        self.create_merge_tree_table_with_strategy(table, order_by, MergeStrategy::Default);
    }

    /// Create a MergeTree-backed table with a specific merge strategy.
    pub fn create_merge_tree_table_with_strategy(
        &mut self,
        table: &str,
        order_by: Vec<String>,
        strategy: MergeStrategy,
    ) {
        if let Some(ref wal) = self.wal {
            if let Err(e) = wal.log_create_table(table) {
                eprintln!("columnar WAL: failed to log create_table {table}: {e}");
            }
        }
        self.tables.entry(table.to_string()).or_default();
        self.merge_trees.insert(table.to_string(), MergeTree::new_with_strategy(order_by, strategy));
    }

    /// Returns true if the table is backed by a MergeTree.
    pub fn is_merge_tree(&self, table: &str) -> bool {
        self.merge_trees.contains_key(table)
    }

    /// Get the MergeTree for a table (if it exists).
    pub fn get_merge_tree(&self, table: &str) -> Option<&MergeTree> {
        self.merge_trees.get(table)
    }

    /// Get a mutable reference to the MergeTree for a table (if it exists).
    pub fn get_merge_tree_mut(&mut self, table: &str) -> Option<&mut MergeTree> {
        self.merge_trees.get_mut(table)
    }

    /// Scan a MergeTree-backed table with zone map pruning.
    pub fn scan_merge_tree(
        &self,
        table: &str,
        predicate_col: &str,
        op: CmpOp,
        value: &ScalarValue,
    ) -> Vec<ColumnBatch> {
        if let Some(mt) = self.merge_trees.get(table) {
            mt.scan(predicate_col, op, value)
        } else {
            // Fallback: return all batches (no pruning for raw tables)
            self.tables.get(table).cloned().unwrap_or_default()
        }
    }

    /// Remove a table and all its batches. Returns true if the table existed.
    pub fn drop_table(&mut self, table: &str) -> bool {
        self.merge_trees.remove(table);
        let existed = self.tables.remove(table).is_some();
        if existed {
            if let Some(ref wal) = self.wal {
                if let Err(e) = wal.log_drop_table(table) {
                    eprintln!("columnar WAL: failed to log drop_table {table}: {e}");
                }
            }
        }
        existed
    }

    /// Remove all batches from a table without dropping the table entry.
    ///
    /// Logged as DROP + CREATE so replay produces the same empty-table state.
    pub fn clear(&mut self, table: &str) {
        if let Some(mt) = self.merge_trees.get_mut(table) {
            // Re-create the MergeTree with the same PK columns and strategy
            let pk = mt.primary_key.clone();
            let strategy = mt.merge_strategy.clone();
            *mt = MergeTree::new_with_strategy(pk, strategy);
        }
        if let Some(v) = self.tables.get_mut(table) {
            v.clear();
            if let Some(ref wal) = self.wal {
                if let Err(e) = wal.log_drop_table(table) {
                    eprintln!("columnar WAL: failed to log clear(drop) {table}: {e}");
                }
                if let Err(e) = wal.log_create_table(table) {
                    eprintln!("columnar WAL: failed to log clear(create) {table}: {e}");
                }
            }
        }
    }

    /// Returns true if the table exists in this store.
    pub fn table_exists(&self, table: &str) -> bool {
        self.tables.contains_key(table) || self.merge_trees.contains_key(table)
    }

    /// Return all table names (order unspecified).
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        for k in self.merge_trees.keys() {
            if !names.contains(k) {
                names.push(k.clone());
            }
        }
        names
    }
}

// ============================================================================
// Row ↔ ColumnBatch conversion helpers (WAL persistence)
// ============================================================================

/// Convert a Vec<Row> into a single ColumnBatch.
///
/// Column names are the zero-based string index ("0", "1", ...) matching the
/// convention used by ColumnarStorageEngine.
fn rows_to_batch(rows: Vec<Row>) -> ColumnBatch {
    if rows.is_empty() {
        return ColumnBatch::new(Vec::new());
    }
    let n_cols = rows[0].len();
    let columns = (0..n_cols)
        .map(|col_i| {
            let vals: Vec<Value> = rows.iter().map(|row| {
                row.get(col_i).cloned().unwrap_or(Value::Null)
            }).collect();
            (col_i.to_string(), vals_to_coldata(vals))
        })
        .collect();
    ColumnBatch::new(columns)
}

/// Extract a single row-column Value from a ColumnData at `idx`.
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

/// Reconstruct Vec<Row> from a single ColumnBatch (for WAL logging).
fn batch_to_rows(batch: &ColumnBatch) -> Vec<Row> {
    let mut rows = Vec::with_capacity(batch.row_count);
    for row_i in 0..batch.row_count {
        let row: Row = (0..batch.columns.len())
            .map(|col_i| {
                let (_, col) = &batch.columns[col_i];
                coldata_get(col, row_i)
            })
            .collect();
        rows.push(row);
    }
    rows
}

/// Convert a Vec<Value> into the best-fit ColumnData, determined by the first
/// non-null value.
fn vals_to_coldata(vals: Vec<Value>) -> ColumnData {
    let first_non_null = vals.iter().find(|v| !matches!(v, Value::Null));
    match first_non_null {
        Some(Value::Bool(_)) => ColumnData::Bool(
            vals.into_iter()
                .map(|v| match v { Value::Bool(b) => Some(b), _ => None })
                .collect(),
        ),
        Some(Value::Int32(_)) => ColumnData::Int32(
            vals.into_iter()
                .map(|v| match v { Value::Int32(n) => Some(n), _ => None })
                .collect(),
        ),
        Some(Value::Int64(_)) => ColumnData::Int64(
            vals.into_iter()
                .map(|v| match v {
                    Value::Int64(n) => Some(n),
                    Value::Int32(n) => Some(n as i64),
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
                    _ => None,
                })
                .collect(),
        ),
        _ => ColumnData::Text(
            vals.into_iter()
                .map(|v| match v {
                    Value::Text(s) => Some(s),
                    Value::Null => None,
                    other => Some(other.to_string()),
                })
                .collect(),
        ),
    }
}

// ============================================================================
// Text predicates (standalone enum for typed text filtering)
// ============================================================================

/// Standalone text predicate for use with ColumnarStore::scan_with_predicate.
#[derive(Debug, Clone)]
pub enum TextPredicate {
    /// Filter rows where the column value starts with the given prefix.
    StartsWith(String),
    /// Filter rows where the column value contains the given substring.
    Contains(String),
    /// Filter rows where the column value matches the given regex pattern.
    Regex(String),
}

/// Apply a TextPredicate to a text column, returning a boolean mask.
pub fn apply_text_predicate(col: &[Option<String>], pred: &TextPredicate) -> Vec<bool> {
    match pred {
        TextPredicate::StartsWith(prefix) => col
            .iter()
            .map(|v| {
                v.as_ref()
                    .map(|s| s.starts_with(prefix.as_str()))
                    .unwrap_or(false)
            })
            .collect(),
        TextPredicate::Contains(sub) => col
            .iter()
            .map(|v| {
                v.as_ref()
                    .map(|s| s.contains(sub.as_str()))
                    .unwrap_or(false)
            })
            .collect(),
        TextPredicate::Regex(pattern) => {
            // Limit regex pattern length to prevent excessive NFA compilation time.
            const MAX_REGEX_PATTERN_LEN: usize = 1000;
            if pattern.len() > MAX_REGEX_PATTERN_LEN {
                return vec![false; col.len()];
            }
            match Regex::new(pattern) {
                Ok(re) => col.iter()
                    .map(|v| {
                        v.as_ref()
                            .map(|s| re.is_match(s))
                            .unwrap_or(false)
                    })
                    .collect(),
                Err(_) => vec![false; col.len()], // invalid regex matches nothing
            }
        }
    }
}

// ============================================================================
// Vectorized batch-level aggregation
// ============================================================================

/// A generic aggregation value returned from batch-level min/max.
#[derive(Debug, Clone, PartialEq)]
pub enum AggValue {
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bool(bool),
    Null,
}

/// Sum all numeric values in a named column of a batch, returning f64.
///
/// Works on Int32, Int64, and Float64 columns. Returns 0.0 for empty or
/// non-numeric columns.
pub fn aggregate_sum(batch: &ColumnBatch, column: &str) -> f64 {
    match batch.column(column) {
        Some(ColumnData::Int32(v)) => v.iter().filter_map(|x| x.as_ref()).map(|&x| x as f64).sum(),
        Some(ColumnData::Int64(v)) => v.iter().filter_map(|x| x.as_ref()).map(|&x| x as f64).sum(),
        Some(ColumnData::Float64(v)) => v.iter().filter_map(|x| x.as_ref()).sum(),
        _ => 0.0,
    }
}

/// Find the minimum value in a named column of a batch.
pub fn aggregate_min(batch: &ColumnBatch, column: &str) -> AggValue {
    match batch.column(column) {
        Some(ColumnData::Int32(v)) => v.iter().filter_map(|x| x.as_ref()).copied().min().map(AggValue::Int32).unwrap_or(AggValue::Null),
        Some(ColumnData::Int64(v)) => v.iter().filter_map(|x| x.as_ref()).copied().min().map(AggValue::Int64).unwrap_or(AggValue::Null),
        Some(ColumnData::Float64(v)) => v.iter().filter_map(|x| x.as_ref()).copied().reduce(f64::min).map(AggValue::Float64).unwrap_or(AggValue::Null),
        Some(ColumnData::Text(v)) => v.iter().filter_map(|x| x.as_ref()).min().cloned().map(AggValue::Text).unwrap_or(AggValue::Null),
        Some(ColumnData::Bool(v)) => v.iter().filter_map(|x| x.as_ref()).copied().min().map(AggValue::Bool).unwrap_or(AggValue::Null),
        None => AggValue::Null,
    }
}

/// Find the maximum value in a named column of a batch.
pub fn aggregate_max(batch: &ColumnBatch, column: &str) -> AggValue {
    match batch.column(column) {
        Some(ColumnData::Int32(v)) => v.iter().filter_map(|x| x.as_ref()).copied().max().map(AggValue::Int32).unwrap_or(AggValue::Null),
        Some(ColumnData::Int64(v)) => v.iter().filter_map(|x| x.as_ref()).copied().max().map(AggValue::Int64).unwrap_or(AggValue::Null),
        Some(ColumnData::Float64(v)) => v.iter().filter_map(|x| x.as_ref()).copied().reduce(f64::max).map(AggValue::Float64).unwrap_or(AggValue::Null),
        Some(ColumnData::Text(v)) => v.iter().filter_map(|x| x.as_ref()).max().cloned().map(AggValue::Text).unwrap_or(AggValue::Null),
        Some(ColumnData::Bool(v)) => v.iter().filter_map(|x| x.as_ref()).copied().max().map(AggValue::Bool).unwrap_or(AggValue::Null),
        None => AggValue::Null,
    }
}

/// Count non-null values in a named column of a batch.
pub fn aggregate_count(batch: &ColumnBatch, column: &str) -> usize {
    match batch.column(column) {
        Some(col) => count_non_null(col),
        None => 0,
    }
}

/// Average of numeric values in a named column of a batch.
///
/// Returns f64::NAN if the column is empty, non-numeric, or all nulls.
pub fn aggregate_avg(batch: &ColumnBatch, column: &str) -> f64 {
    let count = aggregate_count(batch, column);
    if count == 0 {
        return f64::NAN;
    }
    aggregate_sum(batch, column) / count as f64
}

// ============================================================================
// ColumnarStore text predicate scan
// ============================================================================

impl ColumnarStore {
    /// Scan a table, applying a text predicate on the given column.
    ///
    /// Returns filtered batches where only matching rows are retained.
    /// Works with both raw and MergeTree-backed tables.
    pub fn scan_with_predicate(
        &self,
        table: &str,
        column: &str,
        predicate: &TextPredicate,
    ) -> Vec<ColumnBatch> {
        self.batches_all(table)
            .iter()
            .map(|batch| {
                let mask = match batch.column(column) {
                    Some(ColumnData::Text(v)) => apply_text_predicate(v, predicate),
                    _ => vec![false; batch.row_count],
                };
                apply_mask(batch, &mask)
            })
            .filter(|b| b.row_count > 0)
            .collect()
    }
}


// ============================================================================
// Zone Maps (min/max metadata per column per batch)
// ============================================================================

/// Scalar value used in zone map comparisons.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bool(bool),
}

/// Comparison operator for zone map skip checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
}

/// Min/max statistics for a single column within a batch.
#[derive(Debug, Clone)]
pub struct ColumnZoneMap {
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub null_count: usize,
    pub row_count: usize,
}

/// Zone maps for all columns in a batch, keyed by column name.
#[derive(Debug, Clone)]
pub struct ZoneMap {
    pub columns: HashMap<String, ColumnZoneMap>,
}
impl ZoneMap {
    /// Compute zone maps for every column in a batch.
    pub fn from_batch(batch: &ColumnBatch) -> Self {
        let mut columns = HashMap::new();
        for (name, col) in &batch.columns {
            let czm = ColumnZoneMap::from_column(col);
            columns.insert(name.clone(), czm);
        }
        ZoneMap { columns }
    }

    /// Check if a batch can be entirely skipped for a predicate on col_name.
    ///
    /// Returns true if the zone map guarantees NO rows can match.
    pub fn can_skip(&self, col_name: &str, op: CmpOp, value: &ScalarValue) -> bool {
        let czm = match self.columns.get(col_name) {
            Some(c) => c,
            None => return false,
        };
        if czm.null_count == czm.row_count {
            return true;
        }
        let (min_val, max_val) = match (&czm.min, &czm.max) {
            (Some(mn), Some(mx)) => (mn, mx),
            _ => return false,
        };
        match op {
            CmpOp::Eq => {
                scalar_lt(value, min_val) || scalar_lt(max_val, value)
            }
            CmpOp::Gt => {
                !scalar_lt(value, max_val)
            }
            CmpOp::Lt => {
                !scalar_lt(min_val, value)
            }
            CmpOp::Gte => {
                scalar_lt(max_val, value)
            }
            CmpOp::Lte => {
                scalar_lt(value, min_val)
            }
        }
    }
}
/// Returns true if a < b, for comparable scalar types. Mixed types return false.
fn scalar_lt(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Int32(x), ScalarValue::Int32(y)) => x < y,
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => x < y,
        (ScalarValue::Float64(x), ScalarValue::Float64(y)) => x < y,
        (ScalarValue::Text(x), ScalarValue::Text(y)) => x < y,
        (ScalarValue::Bool(x), ScalarValue::Bool(y)) => !x & y,
        _ => false,
    }
}

impl ColumnZoneMap {
    /// Build zone map stats from a single column.
    pub fn from_column(col: &ColumnData) -> Self {
        match col {
            ColumnData::Bool(v) => {
                let mut null_count = 0usize;
                let mut has_true = false;
                let mut has_false = false;
                for val in v {
                    match val {
                        None => null_count += 1,
                        Some(true) => has_true = true,
                        Some(false) => has_false = true,
                    }
                }
                let min = if has_false || has_true {
                    Some(ScalarValue::Bool(!has_false))
                } else {
                    None
                };
                let max = if has_false || has_true {
                    Some(ScalarValue::Bool(has_true))
                } else {
                    None
                };
                ColumnZoneMap { min, max, null_count, row_count: v.len() }
            }
            ColumnData::Int32(v) => {
                let mut null_count = 0usize;
                let mut mn: Option<i32> = None;
                let mut mx: Option<i32> = None;
                for val in v {
                    match val {
                        None => null_count += 1,
                        Some(x) => {
                            mn = Some(mn.map_or(*x, |m: i32| m.min(*x)));
                            mx = Some(mx.map_or(*x, |m: i32| m.max(*x)));
                        }
                    }
                }
                ColumnZoneMap {
                    min: mn.map(ScalarValue::Int32),
                    max: mx.map(ScalarValue::Int32),
                    null_count,
                    row_count: v.len(),
                }
            }
            ColumnData::Int64(v) => {
                let mut null_count = 0usize;
                let mut mn: Option<i64> = None;
                let mut mx: Option<i64> = None;
                for val in v {
                    match val {
                        None => null_count += 1,
                        Some(x) => {
                            mn = Some(mn.map_or(*x, |m: i64| m.min(*x)));
                            mx = Some(mx.map_or(*x, |m: i64| m.max(*x)));
                        }
                    }
                }
                ColumnZoneMap {
                    min: mn.map(ScalarValue::Int64),
                    max: mx.map(ScalarValue::Int64),
                    null_count,
                    row_count: v.len(),
                }
            }
            ColumnData::Float64(v) => {
                let mut null_count = 0usize;
                let mut mn: Option<f64> = None;
                let mut mx: Option<f64> = None;
                for val in v {
                    match val {
                        None => null_count += 1,
                        Some(x) => {
                            mn = Some(mn.map_or(*x, |m: f64| m.min(*x)));
                            mx = Some(mx.map_or(*x, |m: f64| m.max(*x)));
                        }
                    }
                }
                ColumnZoneMap {
                    min: mn.map(ScalarValue::Float64),
                    max: mx.map(ScalarValue::Float64),
                    null_count,
                    row_count: v.len(),
                }
            }
            ColumnData::Text(v) => {
                let mut null_count = 0usize;
                let mut mn: Option<String> = None;
                let mut mx: Option<String> = None;
                for val in v {
                    match val {
                        None => null_count += 1,
                        Some(x) => {
                            mn = Some(match mn {
                                None => x.clone(),
                                Some(m) => if x.as_str() < m.as_str() { x.clone() } else { m },
                            });
                            mx = Some(match mx {
                                None => x.clone(),
                                Some(m) => if x.as_str() > m.as_str() { x.clone() } else { m },
                            });
                        }
                    }
                }
                ColumnZoneMap {
                    min: mn.map(ScalarValue::Text),
                    max: mx.map(ScalarValue::Text),
                    null_count,
                    row_count: v.len(),
                }
            }
        }
    }
}
// ============================================================================
// Null Bitmaps (compact null tracking)
// ============================================================================

/// A compact bitmap tracking null status for rows.
///
/// Each bit in the underlying Vec<u64> represents one row.
/// A set bit (1) means the row is NULL; a clear bit (0) means non-null.
#[derive(Debug, Clone)]
pub struct NullBitmap {
    bits: Vec<u64>,
    len: usize,
}

impl NullBitmap {
    /// Create a new bitmap for len rows, all initially non-null.
    pub fn new(len: usize) -> Self {
        let num_words = len.div_ceil(64);
        NullBitmap {
            bits: vec![0u64; num_words],
            len,
        }
    }

    /// Create a bitmap from a column existing Option-based null info.
    pub fn from_column(col: &ColumnData) -> Self {
        let len = col.len();
        let mut bm = NullBitmap::new(len);
        match col {
            ColumnData::Bool(v) => {
                for (i, val) in v.iter().enumerate() {
                    if val.is_none() { bm.set_null(i); }
                }
            }
            ColumnData::Int32(v) => {
                for (i, val) in v.iter().enumerate() {
                    if val.is_none() { bm.set_null(i); }
                }
            }
            ColumnData::Int64(v) => {
                for (i, val) in v.iter().enumerate() {
                    if val.is_none() { bm.set_null(i); }
                }
            }
            ColumnData::Float64(v) => {
                for (i, val) in v.iter().enumerate() {
                    if val.is_none() { bm.set_null(i); }
                }
            }
            ColumnData::Text(v) => {
                for (i, val) in v.iter().enumerate() {
                    if val.is_none() { bm.set_null(i); }
                }
            }
        }
        bm
    }
    /// Mark row at idx as NULL.
    pub fn set_null(&mut self, idx: usize) {
        assert!(idx < self.len, "index {} out of bounds (len={})", idx, self.len);
        let word = idx / 64;
        let bit = idx % 64;
        self.bits[word] |= 1u64 << bit;
    }

    /// Clear the null flag for row at idx (mark as non-null).
    pub fn clear_null(&mut self, idx: usize) {
        assert!(idx < self.len, "index {} out of bounds (len={})", idx, self.len);
        let word = idx / 64;
        let bit = idx % 64;
        self.bits[word] &= !(1u64 << bit);
    }

    /// Check if row at idx is NULL.
    pub fn is_null(&self, idx: usize) -> bool {
        assert!(idx < self.len, "index {} out of bounds (len={})", idx, self.len);
        let word = idx / 64;
        let bit = idx % 64;
        (self.bits[word] >> bit) & 1 == 1
    }

    /// Count total NULL rows.
    pub fn count_nulls(&self) -> usize {
        let full_words = self.len / 64;
        let remainder = self.len % 64;
        let mut count: usize = 0;
        for i in 0..full_words {
            count += self.bits[i].count_ones() as usize;
        }
        if remainder > 0 {
            let mask = (1u64 << remainder) - 1;
            count += (self.bits[full_words] & mask).count_ones() as usize;
        }
        count
    }

    /// Count total non-NULL rows.
    pub fn count_non_nulls(&self) -> usize {
        self.len - self.count_nulls()
    }

    /// Total number of rows tracked.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the bitmap is empty (tracks zero rows).
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
// ============================================================================
// Late Materialization
// ============================================================================

/// Return indices of rows matching a predicate on a specific column.
///
/// Instead of copying all columns, this returns just the matching row positions.
/// Combine with gather() to only materialize the columns you actually need.
pub fn filter_positions(batch: &ColumnBatch, predicate_col: &str, predicate: &Predicate) -> Vec<usize> {
    let col = match batch.column(predicate_col) {
        Some(c) => c,
        None => return Vec::new(),
    };
    match col {
        ColumnData::Int64(v) => {
            let mask = filter_i64(v, predicate);
            mask.iter().enumerate()
                .filter(|&(_, &m)| m)
                .map(|(i, _)| i)
                .collect()
        }
        ColumnData::Int32(v) => {
            match predicate {
                Predicate::EqI64(target) => {
                    let t = *target as i32;
                    v.iter().enumerate()
                        .filter(|(_, val)| val.as_ref() == Some(&t))
                        .map(|(i, _)| i)
                        .collect()
                }
                Predicate::GtI64(target) => {
                    let t = *target as i32;
                    v.iter().enumerate()
                        .filter(|(_, val)| val.map(|x| x > t).unwrap_or(false))
                        .map(|(i, _)| i)
                        .collect()
                }
                Predicate::LtI64(target) => {
                    let t = *target as i32;
                    v.iter().enumerate()
                        .filter(|(_, val)| val.map(|x| x < t).unwrap_or(false))
                        .map(|(i, _)| i)
                        .collect()
                }
                Predicate::BetweenI64(lo, hi) => {
                    let lo32 = *lo as i32;
                    let hi32 = *hi as i32;
                    v.iter().enumerate()
                        .filter(|(_, val)| val.map(|x| x >= lo32 && x <= hi32).unwrap_or(false))
                        .map(|(i, _)| i)
                        .collect()
                }
                _ => (0..v.len()).collect(),
            }
        }
        ColumnData::Text(v) => {
            let mask = filter_text(v, predicate);
            mask.iter().enumerate()
                .filter(|&(_, &m)| m)
                .map(|(i, _)| i)
                .collect()
        }
        ColumnData::Float64(v) => {
            match predicate {
                Predicate::GtI64(target) => {
                    let t = *target as f64;
                    v.iter().enumerate()
                        .filter(|(_, val)| val.map(|x| x > t).unwrap_or(false))
                        .map(|(i, _)| i)
                        .collect()
                }
                Predicate::LtI64(target) => {
                    let t = *target as f64;
                    v.iter().enumerate()
                        .filter(|(_, val)| val.map(|x| x < t).unwrap_or(false))
                        .map(|(i, _)| i)
                        .collect()
                }
                Predicate::EqI64(target) => {
                    let t = *target as f64;
                    v.iter().enumerate()
                        .filter(|(_, val)| val.map(|x| (x - t).abs() < f64::EPSILON).unwrap_or(false))
                        .map(|(i, _)| i)
                        .collect()
                }
                _ => (0..v.len()).collect(),
            }
        }
        ColumnData::Bool(_) => {
            (0..col.len()).collect()
        }
    }
}
/// Materialize only specific columns at specific row positions from a batch.
///
/// This is the gather step of late materialization: given row positions from
/// filter_positions(), extract only the columns the query actually needs.
/// If columns is empty, all columns are gathered.
pub fn gather(batch: &ColumnBatch, positions: &[usize], columns: &[&str]) -> ColumnBatch {
    let selected_cols: Vec<&(String, ColumnData)> = if columns.is_empty() {
        batch.columns.iter().collect()
    } else {
        batch.columns.iter()
            .filter(|(name, _)| columns.contains(&name.as_str()))
            .collect()
    };

    let mut new_columns = Vec::with_capacity(selected_cols.len());
    for (name, col) in selected_cols {
        let gathered = match col {
            ColumnData::Bool(v) => {
                ColumnData::Bool(positions.iter().map(|&i| v[i]).collect())
            }
            ColumnData::Int32(v) => {
                ColumnData::Int32(positions.iter().map(|&i| v[i]).collect())
            }
            ColumnData::Int64(v) => {
                ColumnData::Int64(positions.iter().map(|&i| v[i]).collect())
            }
            ColumnData::Float64(v) => {
                ColumnData::Float64(positions.iter().map(|&i| v[i]).collect())
            }
            ColumnData::Text(v) => {
                ColumnData::Text(positions.iter().map(|&i| v[i].clone()).collect())
            }
        };
        new_columns.push((name.clone(), gathered));
    }

    ColumnBatch::new(new_columns)
}

// ============================================================================
// Gap 9: Per-Column Adaptive Compression
// ============================================================================

/// Compression codec for columnar data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    /// No compression.
    None,
    /// Run-length encoding — good for columns with many repeated values.
    Rle,
    /// Delta encoding — good for sorted/sequential integer columns.
    Delta,
    /// Dictionary encoding — good for low-cardinality text columns.
    Dictionary,
    /// Frame-of-reference — good for integer columns in narrow ranges.
    FrameOfReference,
}

/// Compressed column data.
#[derive(Debug, Clone)]
pub struct CompressedColumn {
    pub codec: CompressionCodec,
    pub original_len: usize,
    pub data: CompressedData,
}

/// The actual compressed representation.
#[derive(Debug, Clone)]
pub enum CompressedData {
    None(ColumnData),
    /// Run-length encoded: (value, count) pairs for i64 columns.
    /// Uses Option<i64> to properly represent NULLs without sentinel values.
    RleI64(Vec<(Option<i64>, u32)>),
    /// Run-length encoded: (value, count) pairs for text columns.
    /// Uses Option<String> to properly represent NULLs without sentinel values.
    RleText(Vec<(Option<String>, u32)>),
    /// Delta encoded: base value + deltas for i64 columns.
    /// Null bitmap tracks which positions in the original column were NULL.
    DeltaI64 { base: i64, deltas: Vec<i32>, null_bitmap: Vec<bool> },
    /// Dictionary encoded: dictionary + indices for text columns.
    DictionaryText {
        dict: Vec<String>,
        indices: Vec<u32>,
        nulls: Vec<bool>,
    },
    /// Frame-of-reference: min value + offsets for i64 columns.
    /// Null bitmap tracks which positions in the original column were NULL.
    ForI64 { min_val: i64, offsets: Vec<u16>, null_bitmap: Vec<bool> },
}

/// Analyze a column and select the best compression codec.
pub fn select_codec(col: &ColumnData) -> CompressionCodec {
    match col {
        ColumnData::Int32(vals) => {
            let as_i64: Vec<Option<i64>> = vals.iter().map(|v| v.map(|x| x as i64)).collect();
            select_codec_i64(&as_i64)
        }
        ColumnData::Int64(vals) => select_codec_i64(vals),
        ColumnData::Float64(_) => CompressionCodec::None,
        ColumnData::Text(vals) => select_codec_text(vals),
        ColumnData::Bool(_) => CompressionCodec::Rle,
    }
}

fn select_codec_i64(vals: &[Option<i64>]) -> CompressionCodec {
    if vals.is_empty() {
        return CompressionCodec::None;
    }

    let non_nulls: Vec<i64> = vals.iter().filter_map(|v| *v).collect();
    if non_nulls.is_empty() {
        return CompressionCodec::None;
    }

    // Check for sorted/sequential data → delta encoding
    let is_sorted = non_nulls.windows(2).all(|w| w[1] >= w[0]);
    if is_sorted && non_nulls.len() > 10 {
        // Check if deltas fit in i32
        let max_delta = non_nulls.windows(2).map(|w| (w[1] - w[0]).unsigned_abs()).max().unwrap_or(0);
        if max_delta <= i32::MAX as u64 {
            return CompressionCodec::Delta;
        }
    }

    // Check for narrow range → frame-of-reference
    let min_val = non_nulls.iter().copied().min().unwrap();
    let max_val = non_nulls.iter().copied().max().unwrap();
    let range = (max_val - min_val).unsigned_abs();
    if range <= u16::MAX as u64 {
        return CompressionCodec::FrameOfReference;
    }

    // Check for many runs → RLE
    let run_count = count_runs_i64(vals);
    if run_count * 3 < vals.len() {
        return CompressionCodec::Rle;
    }

    CompressionCodec::None
}

fn select_codec_text(vals: &[Option<String>]) -> CompressionCodec {
    if vals.is_empty() {
        return CompressionCodec::None;
    }

    // Count distinct non-null values
    let mut distinct = std::collections::HashSet::new();
    let mut non_null_count = 0usize;
    for s in vals.iter().flatten() {
        distinct.insert(s.as_str());
        non_null_count += 1;
    }

    if non_null_count == 0 {
        return CompressionCodec::None;
    }

    // Low cardinality → dictionary encoding
    let cardinality_ratio = distinct.len() as f64 / non_null_count as f64;
    if cardinality_ratio < 0.5 || distinct.len() <= 256 {
        return CompressionCodec::Dictionary;
    }

    // Check for runs → RLE
    let run_count = count_runs_text(vals);
    if run_count * 3 < vals.len() {
        return CompressionCodec::Rle;
    }

    CompressionCodec::None
}

fn count_runs_i64(vals: &[Option<i64>]) -> usize {
    if vals.is_empty() {
        return 0;
    }
    let mut runs = 1;
    for w in vals.windows(2) {
        if w[0] != w[1] {
            runs += 1;
        }
    }
    runs
}

fn count_runs_text(vals: &[Option<String>]) -> usize {
    if vals.is_empty() {
        return 0;
    }
    let mut runs = 1;
    for w in vals.windows(2) {
        if w[0] != w[1] {
            runs += 1;
        }
    }
    runs
}

/// Compress a column using the specified codec.
pub fn compress_column(col: &ColumnData, codec: CompressionCodec) -> CompressedColumn {
    let original_len = col.len();
    let data = match codec {
        CompressionCodec::None => CompressedData::None(col.clone()),
        CompressionCodec::Rle => compress_rle(col),
        CompressionCodec::Delta => compress_delta(col),
        CompressionCodec::Dictionary => compress_dictionary(col),
        CompressionCodec::FrameOfReference => compress_for(col),
    };
    CompressedColumn {
        codec,
        original_len,
        data,
    }
}

/// Compress with automatic codec selection.
pub fn compress_adaptive(col: &ColumnData) -> CompressedColumn {
    let codec = select_codec(col);
    compress_column(col, codec)
}

fn compress_rle(col: &ColumnData) -> CompressedData {
    match col {
        ColumnData::Int64(vals) => {
            let mut runs: Vec<(Option<i64>, u32)> = Vec::new();
            let mut i = 0;
            while i < vals.len() {
                let val = vals[i];
                let mut run_len = 1u32;
                loop {
                    let next = i + run_len as usize;
                    if next >= vals.len() { break; }
                    if vals[next] != val { break; }
                    run_len += 1;
                }
                runs.push((val, run_len));
                i += run_len as usize;
            }
            CompressedData::RleI64(runs)
        }
        ColumnData::Text(vals) => {
            let mut runs: Vec<(Option<String>, u32)> = Vec::new();
            let mut i = 0;
            while i < vals.len() {
                let val = &vals[i];
                let mut run_len = 1u32;
                loop {
                    let next = i + run_len as usize;
                    if next >= vals.len() { break; }
                    if vals[next] != *val { break; }
                    run_len += 1;
                }
                runs.push((val.clone(), run_len));
                i += run_len as usize;
            }
            CompressedData::RleText(runs)
        }
        _ => CompressedData::None(col.clone()),
    }
}

fn compress_delta(col: &ColumnData) -> CompressedData {
    match col {
        ColumnData::Int64(vals) => {
            let null_bitmap: Vec<bool> = vals.iter().map(|v| v.is_none()).collect();
            let non_nulls: Vec<i64> = vals.iter().filter_map(|v| *v).collect();
            if non_nulls.is_empty() {
                return CompressedData::None(col.clone());
            }
            let base = non_nulls[0];
            let deltas: Vec<i32> = non_nulls.windows(2).map(|w| (w[1] - w[0]) as i32).collect();
            CompressedData::DeltaI64 { base, deltas, null_bitmap }
        }
        _ => CompressedData::None(col.clone()),
    }
}

fn compress_dictionary(col: &ColumnData) -> CompressedData {
    match col {
        ColumnData::Text(vals) => {
            let mut dict = Vec::new();
            let mut dict_map = std::collections::HashMap::new();
            let mut indices = Vec::with_capacity(vals.len());
            let mut nulls = Vec::with_capacity(vals.len());

            for v in vals {
                match v {
                    Some(s) => {
                        let idx = dict_map.entry(s.clone()).or_insert_with(|| {
                            let i = dict.len() as u32;
                            dict.push(s.clone());
                            i
                        });
                        indices.push(*idx);
                        nulls.push(false);
                    }
                    None => {
                        indices.push(0);
                        nulls.push(true);
                    }
                }
            }

            CompressedData::DictionaryText {
                dict,
                indices,
                nulls,
            }
        }
        _ => CompressedData::None(col.clone()),
    }
}

fn compress_for(col: &ColumnData) -> CompressedData {
    match col {
        ColumnData::Int64(vals) => {
            let null_bitmap: Vec<bool> = vals.iter().map(|v| v.is_none()).collect();
            let non_nulls: Vec<i64> = vals.iter().filter_map(|v| *v).collect();
            if non_nulls.is_empty() {
                return CompressedData::None(col.clone());
            }
            let min_val = *non_nulls.iter().min().unwrap();
            let offsets: Vec<u16> = non_nulls.iter().map(|v| (v - min_val) as u16).collect();
            CompressedData::ForI64 { min_val, offsets, null_bitmap }
        }
        _ => CompressedData::None(col.clone()),
    }
}

/// Decompress a compressed column back to its original form.
pub fn decompress_column(compressed: &CompressedColumn) -> ColumnData {
    match &compressed.data {
        CompressedData::None(col) => col.clone(),
        CompressedData::RleI64(runs) => {
            let mut vals = Vec::with_capacity(compressed.original_len);
            for (val, count) in runs {
                for _ in 0..*count {
                    vals.push(*val);
                }
            }
            ColumnData::Int64(vals)
        }
        CompressedData::RleText(runs) => {
            let mut vals = Vec::with_capacity(compressed.original_len);
            for (val, count) in runs {
                for _ in 0..*count {
                    vals.push(val.clone());
                }
            }
            ColumnData::Text(vals)
        }
        CompressedData::DeltaI64 { base, deltas, null_bitmap } => {
            let mut non_null_vals = Vec::with_capacity(deltas.len() + 1);
            let mut current = *base;
            non_null_vals.push(current);
            for &d in deltas {
                current += d as i64;
                non_null_vals.push(current);
            }
            // Reconstruct with nulls using the bitmap
            let mut vals = Vec::with_capacity(null_bitmap.len());
            let mut nn_idx = 0;
            for &is_null in null_bitmap {
                if is_null {
                    vals.push(None);
                } else if nn_idx < non_null_vals.len() {
                    vals.push(Some(non_null_vals[nn_idx]));
                    nn_idx += 1;
                } else {
                    vals.push(None);
                }
            }
            ColumnData::Int64(vals)
        }
        CompressedData::DictionaryText {
            dict,
            indices,
            nulls,
        } => {
            let vals: Vec<Option<String>> = indices
                .iter()
                .zip(nulls.iter())
                .map(|(&idx, &is_null)| {
                    if is_null {
                        None
                    } else {
                        dict.get(idx as usize).cloned()
                    }
                })
                .collect();
            ColumnData::Text(vals)
        }
        CompressedData::ForI64 { min_val, offsets, null_bitmap } => {
            // Reconstruct non-null values from offsets
            let non_null_vals: Vec<i64> = offsets.iter().map(|&o| *min_val + o as i64).collect();
            // Reconstruct with nulls using the bitmap
            let mut vals = Vec::with_capacity(null_bitmap.len());
            let mut nn_idx = 0;
            for &is_null in null_bitmap {
                if is_null {
                    vals.push(None);
                } else if nn_idx < non_null_vals.len() {
                    vals.push(Some(non_null_vals[nn_idx]));
                    nn_idx += 1;
                } else {
                    vals.push(None);
                }
            }
            ColumnData::Int64(vals)
        }
    }
}

/// Estimate the compressed size in bytes.
pub fn compressed_size(compressed: &CompressedColumn) -> usize {
    match &compressed.data {
        CompressedData::None(col) => col.len() * 8, // approximate
        CompressedData::RleI64(runs) => runs.len() * 13, // (Option<i64> + u32) per run, +1 for null flag
        CompressedData::RleText(runs) => runs.iter().map(|(s, _)| s.as_ref().map_or(1, |v| v.len() + 1) + 4).sum(),
        CompressedData::DeltaI64 { deltas, null_bitmap, .. } => 8 + deltas.len() * 4 + null_bitmap.len(), // base + deltas + bitmap
        CompressedData::DictionaryText { dict, indices, nulls } => {
            dict.iter().map(|s| s.len()).sum::<usize>() + indices.len() * 4 + nulls.len()
        }
        CompressedData::ForI64 { offsets, null_bitmap, .. } => 8 + offsets.len() * 2 + null_bitmap.len(), // min + offsets + bitmap
    }
}

// ============================================================================
// Dictionary Encoding for Low-Cardinality Text Columns
// ============================================================================

/// Dictionary-encoded text column — stores distinct values once and references
/// them by integer codes. Dramatically reduces memory for low-cardinality
/// columns like status, category, country, etc.
///
/// NULLs are represented as `u32::MAX` in the codes array.
#[derive(Debug, Clone)]
pub struct DictColumn {
    /// The distinct values (dictionary). Index position = code.
    pub dict: Vec<String>,
    /// Per-row code indexing into `dict`. `u32::MAX` = NULL.
    pub codes: Vec<u32>,
}

/// Sentinel code value representing NULL in a DictColumn.
pub const DICT_NULL_CODE: u32 = u32::MAX;

impl DictColumn {
    /// Number of rows in this column.
    pub fn len(&self) -> usize {
        self.codes.len()
    }

    /// Whether the column has zero rows.
    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    /// Number of distinct non-NULL values (dictionary size).
    pub fn cardinality(&self) -> usize {
        self.dict.len()
    }

    /// Count of NULL rows.
    pub fn null_count(&self) -> usize {
        self.codes.iter().filter(|&&c| c == DICT_NULL_CODE).count()
    }

    /// Look up the value for a given row. Returns None for NULLs.
    pub fn get(&self, row: usize) -> Option<&str> {
        let code = self.codes[row];
        if code == DICT_NULL_CODE {
            None
        } else {
            self.dict.get(code as usize).map(|s| s.as_str())
        }
    }
}

/// Encode a slice of optional strings into a dictionary-encoded column.
///
/// Builds a dictionary of distinct values and maps each row to its dictionary
/// index. NULLs are encoded as `DICT_NULL_CODE`.
pub fn dict_encode(values: &[Option<String>]) -> DictColumn {
    let mut dict = Vec::new();
    let mut dict_map: HashMap<String, u32> = HashMap::new();
    let mut codes = Vec::with_capacity(values.len());

    for v in values {
        match v {
            Some(s) => {
                let code = dict_map.entry(s.clone()).or_insert_with(|| {
                    let idx = dict.len() as u32;
                    dict.push(s.clone());
                    idx
                });
                codes.push(*code);
            }
            None => {
                codes.push(DICT_NULL_CODE);
            }
        }
    }

    DictColumn { dict, codes }
}

/// Decode a dictionary-encoded column back to a vector of optional strings.
pub fn dict_decode(col: &DictColumn) -> Vec<Option<String>> {
    col.codes
        .iter()
        .map(|&code| {
            if code == DICT_NULL_CODE {
                None
            } else {
                col.dict.get(code as usize).cloned()
            }
        })
        .collect()
}

/// Perform GROUP BY COUNT on a dictionary-encoded column in O(cardinality) time.
///
/// Instead of hashing every row's string value, this counts occurrences of each
/// dictionary code in a single pass, then maps codes back to strings. This is
/// O(n) in rows but the counting array is only `cardinality` elements, making
/// it extremely cache-friendly for low-cardinality columns.
///
/// Returns `(value, count)` pairs sorted by value. NULLs are excluded.
pub fn dict_group_by_count(col: &DictColumn) -> Vec<(String, usize)> {
    if col.dict.is_empty() {
        return Vec::new();
    }

    // Count occurrences per code — array indexed by code, O(cardinality) space
    let mut counts = vec![0usize; col.dict.len()];
    for &code in &col.codes {
        if code != DICT_NULL_CODE
            && let Some(c) = counts.get_mut(code as usize) {
                *c += 1;
            }
    }

    // Map codes back to strings, skip zero-count entries
    let mut result: Vec<(String, usize)> = col
        .dict
        .iter()
        .enumerate()
        .filter(|&(i, _)| counts[i] > 0)
        .map(|(i, s)| (s.clone(), counts[i]))
        .collect();

    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

/// Perform GROUP BY with SUM aggregation on a dictionary-encoded key column
/// paired with a float64 value column.
///
/// Same O(cardinality) counting trick: accumulates sums in a cardinality-sized
/// array instead of a hash map.
pub fn dict_group_by_sum_f64(
    key_col: &DictColumn,
    val_col: &[Option<f64>],
) -> Vec<(String, usize, f64)> {
    if key_col.dict.is_empty() {
        return Vec::new();
    }
    assert_eq!(
        key_col.len(),
        val_col.len(),
        "key and value columns must have equal length"
    );

    let card = key_col.dict.len();
    let mut counts = vec![0usize; card];
    let mut sums = vec![0.0f64; card];

    for (i, &code) in key_col.codes.iter().enumerate() {
        if code != DICT_NULL_CODE
            && let Some(v) = val_col[i] {
                let idx = code as usize;
                if idx < card {
                    counts[idx] += 1;
                    sums[idx] += v;
                }
            }
    }

    let mut result: Vec<(String, usize, f64)> = key_col
        .dict
        .iter()
        .enumerate()
        .filter(|&(i, _)| counts[i] > 0)
        .map(|(i, s)| (s.clone(), counts[i], sums[i]))
        .collect();

    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

/// Threshold: auto-apply dictionary encoding when a text column in a batch
/// has fewer than this many distinct values.
const DICT_AUTO_MAX_CARDINALITY: usize = 256;
/// Threshold: minimum rows before auto-applying dictionary encoding.
const DICT_AUTO_MIN_ROWS: usize = 1000;

impl ColumnarStore {
    /// Append a batch to a table, automatically dictionary-encoding eligible
    /// text columns.
    ///
    /// A text column is eligible if the batch has >= `DICT_AUTO_MIN_ROWS` rows
    /// and the column has < `DICT_AUTO_MAX_CARDINALITY` distinct values.
    ///
    /// Dictionary-encoded columns are stored alongside the batch in `dict_columns`.
    pub fn append_with_dict(&mut self, table: &str, batch: ColumnBatch) {
        if let Some(ref wal) = self.wal {
            let rows = batch_to_rows(&batch);
            if let Err(e) = wal.log_insert_rows(table, &rows) {
                eprintln!("columnar WAL: failed to log insert_rows (dict) for {table}: {e}");
            }
        }
        let row_count = batch.row_count;
        if row_count >= DICT_AUTO_MIN_ROWS {
            // Check each text column for low cardinality
            for (name, col) in &batch.columns {
                if let ColumnData::Text(vals) = col {
                    let mut distinct = std::collections::HashSet::new();
                    let mut over_limit = false;
                    for s in vals.iter().flatten() {
                        distinct.insert(s.as_str());
                        if distinct.len() > DICT_AUTO_MAX_CARDINALITY {
                            over_limit = true;
                            break;
                        }
                    }
                    if !over_limit && !distinct.is_empty() {
                        let dict_col = dict_encode(vals);
                        self.dict_columns
                            .entry(table.to_string())
                            .or_default()
                            .insert(name.clone(), dict_col);
                    }
                }
            }
        }
        if let Some(mt) = self.merge_trees.get_mut(table) {
            mt.insert(batch);
        } else {
            self.tables.entry(table.to_string()).or_default().push(batch);
        }
    }

    /// Retrieve dictionary-encoded columns for a table, if any.
    pub fn get_dict_columns(&self, table: &str) -> Option<&HashMap<String, DictColumn>> {
        self.dict_columns.get(table)
    }

    /// Perform GROUP BY COUNT using dictionary encoding if available,
    /// falling back to hash-based counting otherwise.
    pub fn dict_group_by_count_for(
        &self,
        table: &str,
        column: &str,
    ) -> Vec<(String, usize)> {
        // Try dictionary-encoded fast path first
        if let Some(dict_cols) = self.dict_columns.get(table)
            && let Some(dict_col) = dict_cols.get(column) {
                return dict_group_by_count(dict_col);
            }

        // Fallback: scan all batches with HashMap
        let mut counts: HashMap<String, usize> = HashMap::new();
        for batch in self.batches(table) {
            if let Some(ColumnData::Text(vals)) = batch.column(column) {
                for v in vals.iter().flatten() {
                    *counts.entry(v.clone()).or_insert(0) += 1;
                }
            }
        }
        let mut result: Vec<(String, usize)> = counts.into_iter().collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }
}

// ============================================================================
// Gap 10: MergeTree Storage Engine
// ============================================================================

/// Merge strategy controlling how duplicate PK rows are handled during compaction.
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    /// Standard MergeTree — keeps all rows, just sorts and compacts.
    Default,
    /// ReplacingMergeTree — dedup by PK, keeps latest version.
    /// The version_column is used to determine which row wins when PKs match.
    Replacing { version_column: Option<String> },
    /// AggregatingMergeTree — partial aggregate during merge.
    /// Columns not in PK are aggregated (SUM for numerics, MAX for strings).
    Aggregating {
        group_columns: Vec<String>,
        sum_columns: Vec<String>,
        count_columns: Vec<String>,
    },
}

/// A "part" in the MergeTree — a sorted chunk of data.
#[derive(Debug, Clone)]
pub struct MergeTreePart {
    /// Unique part ID.
    pub id: u64,
    /// The data in this part, sorted by the primary key columns.
    pub data: ColumnBatch,
    /// Number of rows.
    pub row_count: usize,
    /// Zone map for partition pruning.
    pub zone_map: ZoneMap,
    /// Compressed columns (lazy, populated on compact).
    pub compressed: Option<Vec<(String, CompressedColumn)>>,
}

// ============================================================================
// Background Merge Infrastructure
// ============================================================================

/// A task describing a set of parts that should be merged in the background.
#[derive(Debug)]
pub struct MergeTask {
    /// The table name this merge belongs to.
    pub table: String,
    /// Parts to merge (cloned from the MergeTree).
    pub parts: Vec<MergeTreePart>,
    /// The IDs of the source parts that were selected for merging.
    pub source_part_ids: Vec<u64>,
    /// Primary key columns for sort-merge.
    pub primary_key: Vec<String>,
    /// Merge strategy to apply during this merge.
    pub merge_strategy: MergeStrategy,
}

/// Result of a completed background merge, ready to be applied back.
#[derive(Debug)]
pub struct MergeResult {
    /// The table name this merge belongs to.
    pub table: String,
    /// IDs of the source parts that were merged (to remove).
    pub source_part_ids: Vec<u64>,
    /// The newly merged part.
    pub merged_part: MergeTreePart,
}

/// Sender half for queuing merge tasks to a background worker.
pub type MergeTaskSender = std::sync::mpsc::Sender<MergeTask>;
/// Receiver half for consuming merge tasks in a background worker.
pub type MergeTaskReceiver = std::sync::mpsc::Receiver<MergeTask>;

/// Sender half for returning merge results from the background worker.
pub type MergeResultSender = std::sync::mpsc::Sender<MergeResult>;
/// Receiver half for consuming merge results in the MergeTree owner.
pub type MergeResultReceiver = std::sync::mpsc::Receiver<MergeResult>;

/// Execute a merge task and produce a MergeResult.
///
/// This is the pure computation that runs off the hot path. It sorts,
/// combines, and compresses the source parts into a single merged part.
pub fn execute_merge_task(task: MergeTask, new_part_id: u64) -> MergeResult {
    // Merge all source parts by iteratively merge-sorting pairs
    let mut batches: Vec<ColumnBatch> = task.parts.into_iter().map(|p| p.data).collect();

    let sorted_batch = if batches.is_empty() {
        ColumnBatch::new(vec![])
    } else {
        let mut acc = batches.remove(0);
        for b in batches {
            acc = merge_sorted_batches(&acc, &b, &task.primary_key);
        }
        acc
    };

    // Apply merge strategy to the sorted result
    let merged_batch = match &task.merge_strategy {
        MergeStrategy::Default => sorted_batch,
        MergeStrategy::Replacing { version_column } => {
            merge_replacing(&sorted_batch, &task.primary_key, version_column.as_deref())
        }
        MergeStrategy::Aggregating { group_columns: _, sum_columns, count_columns } => {
            merge_aggregating(&sorted_batch, &task.primary_key, sum_columns, count_columns)
        }
    };

    let row_count = merged_batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
    let zone_map = ZoneMap::from_batch(&merged_batch);

    // Compress merged data using adaptive codec selection
    let compressed = merged_batch.columns.iter()
        .map(|(name, col)| (name.clone(), compress_adaptive(col)))
        .collect::<Vec<_>>();

    let merged_part = MergeTreePart {
        id: new_part_id,
        data: merged_batch,
        row_count,
        zone_map,
        compressed: Some(compressed),
    };

    MergeResult {
        table: task.table,
        source_part_ids: task.source_part_ids,
        merged_part,
    }
}

/// Spawn a background merge worker thread that processes merge tasks.
///
/// The worker reads `MergeTask` from `task_rx`, executes the merge, and
/// sends the `MergeResult` back via `result_tx`. The worker stops when
/// the task channel is closed or `running` is set to false.
///
/// Returns a `JoinHandle` for the worker thread.
pub fn spawn_merge_worker(
    task_rx: MergeTaskReceiver,
    result_tx: MergeResultSender,
    running: Arc<AtomicBool>,
    next_part_id: Arc<AtomicU64>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("nucleus-merge-worker".into())
        .spawn(move || {
            while running.load(AtomicOrdering::SeqCst) {
                match task_rx.recv_timeout(std::time::Duration::from_millis(50)) {
                    Ok(task) => {
                        let part_id = next_part_id.fetch_add(1, AtomicOrdering::SeqCst);
                        let result = execute_merge_task(task, part_id);
                        if result_tx.send(result).is_err() {
                            break;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        })
        .expect("failed to spawn merge worker thread")
}

/// MergeTree storage engine — LSM-tree inspired columnar storage with:
/// - Sorted data parts
/// - Background part merges via execute_merge_task()
/// - Partition pruning via zone maps
/// - Primary key ordering
/// - Hot/cold tiering: small parts stay in memory, large parts flush to disk
#[derive(Debug)]
pub struct MergeTree {
    /// Primary key columns (data is sorted by these columns).
    pub primary_key: Vec<String>,
    /// Hot parts — in-memory, below the cold threshold.
    parts: Vec<MergeTreePart>,
    /// Cold parts — flushed to disk segment files. Only zone maps are in memory.
    cold_parts: Vec<ColdPartInfo>,
    /// Next part ID.
    next_part_id: u64,
    /// Maximum rows per part before splitting on insert.
    pub max_part_rows: usize,
    /// Maximum number of parts before triggering a merge.
    pub max_parts: usize,
    /// Data directory for segment files. `None` = in-memory only.
    pub data_dir: Option<PathBuf>,
    /// Size threshold in bytes above which a part is flushed to disk.
    pub cold_threshold_bytes: usize,
    /// Optional sender for queuing merge tasks to a background worker.
    merge_sender: Option<MergeTaskSender>,
    /// Table name for background merge task identification.
    table_name: String,
    /// IDs of parts currently being merged in the background.
    merging_part_ids: Vec<u64>,
    /// Merge strategy (Default, Replacing, or Aggregating).
    pub merge_strategy: MergeStrategy,
}

impl MergeTree {
    pub fn new(primary_key: Vec<String>) -> Self {
        MergeTree {
            primary_key,
            parts: Vec::new(),
            cold_parts: Vec::new(),
            next_part_id: 1,
            max_part_rows: 8192,
            max_parts: 10,
            data_dir: None,
            cold_threshold_bytes: DEFAULT_COLD_THRESHOLD_BYTES,
            merge_sender: None,
            table_name: String::new(),
            merging_part_ids: Vec::new(),
            merge_strategy: MergeStrategy::Default,
        }
    }

    pub fn new_with_strategy(primary_key: Vec<String>, strategy: MergeStrategy) -> Self {
        let mut tree = Self::new(primary_key);
        tree.merge_strategy = strategy;
        tree
    }

    pub fn set_table_name(&mut self, name: &str) { self.table_name = name.to_string(); }
    pub fn set_background_merger(&mut self, sender: MergeTaskSender) { self.merge_sender = Some(sender); }
    pub fn clear_background_merger(&mut self) { self.merge_sender = None; }
    pub fn has_background_merger(&self) -> bool { self.merge_sender.is_some() }

    pub fn poll_merge_results(&mut self, result_rx: &MergeResultReceiver) {
        while let Ok(result) = result_rx.try_recv() {
            self.apply_merge_result(result);
        }
    }

    fn queue_background_merge(&mut self) {
        let sender = match &self.merge_sender { Some(s) => s, None => return };
        let mut candidates: Vec<(usize, usize)> = self.parts.iter().enumerate()
            .filter(|(_, p)| !self.merging_part_ids.contains(&p.id))
            .map(|(i, p)| (i, p.row_count)).collect();
        candidates.sort_by_key(|&(_, s)| s);
        if candidates.len() < 2 { return; }
        let (idx_a, idx_b) = (candidates[0].0, candidates[1].0);
        let source_ids = vec![self.parts[idx_a].id, self.parts[idx_b].id];
        let task = MergeTask {
            table: self.table_name.clone(),
            parts: vec![self.parts[idx_a].clone(), self.parts[idx_b].clone()],
            source_part_ids: source_ids.clone(),
            primary_key: self.primary_key.clone(),
            merge_strategy: self.merge_strategy.clone(),
        };
        if sender.send(task).is_ok() { self.merging_part_ids.extend(source_ids); }
    }

    /// Apply a completed merge result from the background worker.
    pub fn apply_merge_result(&mut self, result: MergeResult) -> bool {
        self.merging_part_ids.retain(|id| !result.source_part_ids.contains(id));
        let all_present = result.source_part_ids.iter().all(|id| {
            self.parts.iter().any(|p| p.id == *id)
        });
        if !all_present { return false; }
        self.parts.retain(|p| !result.source_part_ids.contains(&p.id));
        self.parts.push(result.merged_part);
        true
    }

    /// Create a disk-backed MergeTree that flushes cold parts to `dir`.
    pub fn open(primary_key: Vec<String>, dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut tree = Self::new(primary_key);
        tree.data_dir = Some(dir.to_path_buf());
        tree.load_segments_from_dir(dir)?;
        Ok(tree)
    }

    /// Insert a batch of data. The batch will be sorted by the primary key
    /// and stored as a new part.
    pub fn insert(&mut self, batch: ColumnBatch) {
        let sorted = self.sort_by_pk(&batch);
        let row_count = sorted.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        let zone_map = ZoneMap::from_batch(&sorted);

        let part_id = self.next_part_id;
        self.next_part_id += 1;

        let part = MergeTreePart {
            id: part_id,
            data: sorted,
            row_count,
            zone_map,
            compressed: None,
        };
        self.parts.push(part);

        // Auto-merge if too many parts
        if self.parts.len() > self.max_parts {
            if self.merge_sender.is_some() {
                self.queue_background_merge();
            } else {
                while self.parts.len() > self.max_parts {
                    self.merge_smallest_parts();
                }
            }
        }

        // Flush cold parts to disk after merge
        self.flush_cold_parts();
    }

    /// Sort a batch by the primary key columns (multi-column composite sort).
    ///
    /// Sorts by the first PK column as primary, second as secondary, etc.
    fn sort_by_pk(&self, batch: &ColumnBatch) -> ColumnBatch {
        if self.primary_key.is_empty() || batch.columns.is_empty() {
            return batch.clone();
        }

        let row_count = batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        if row_count <= 1 {
            return batch.clone();
        }

        let mut indices: Vec<usize> = (0..row_count).collect();

        indices.sort_by(|&a, &b| {
            for pk_col_name in &self.primary_key {
                let ord = compare_column_values(batch.column(pk_col_name), a, b);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Reorder all columns by the sort indices
        let mut new_columns = Vec::with_capacity(batch.columns.len());
        for (name, col) in &batch.columns {
            let reordered = reorder_column(col, &indices);
            new_columns.push((name.clone(), reordered));
        }
        ColumnBatch::new(new_columns)
    }

    /// Merge the two smallest parts into one.
    fn merge_smallest_parts(&mut self) {
        if self.parts.len() < 2 {
            return;
        }

        // Find the two smallest parts
        let mut sizes: Vec<(usize, usize)> = self
            .parts
            .iter()
            .enumerate()
            .map(|(i, p)| (i, p.row_count))
            .collect();
        sizes.sort_by_key(|&(_, s)| s);

        let idx_a = sizes[0].0;
        let idx_b = sizes[1].0;

        // Remove in reverse order to maintain indices
        let (first, second) = if idx_a < idx_b {
            let b = self.parts.remove(idx_b);
            let a = self.parts.remove(idx_a);
            (a, b)
        } else {
            let a = self.parts.remove(idx_a);
            let b = self.parts.remove(idx_b);
            (b, a)
        };

        let sorted_batch = merge_sorted_batches(&first.data, &second.data, &self.primary_key);
        let merged_batch = match &self.merge_strategy {
            MergeStrategy::Default => sorted_batch,
            MergeStrategy::Replacing { version_column } => {
                merge_replacing(&sorted_batch, &self.primary_key, version_column.as_deref())
            }
            MergeStrategy::Aggregating { group_columns: _, sum_columns, count_columns } => {
                merge_aggregating(&sorted_batch, &self.primary_key, sum_columns, count_columns)
            }
        };
        let row_count = merged_batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        let zone_map = ZoneMap::from_batch(&merged_batch);

        // Compress merged data using adaptive codec selection
        let compressed = merged_batch.columns.iter()
            .map(|(name, col)| (name.clone(), compress_adaptive(col)))
            .collect::<Vec<_>>();

        let part_id = self.next_part_id;
        self.next_part_id += 1;

        let merged = MergeTreePart {
            id: part_id,
            data: merged_batch,
            row_count,
            zone_map,
            compressed: Some(compressed),
        };
        self.parts.push(merged);
    }

    /// Scan all parts, pruning those whose zone maps exclude the predicate.
    ///
    /// Hot parts are returned directly. Cold parts whose zone maps indicate
    /// a possible match are loaded from disk on demand.
    pub fn scan(&self, predicate_col: &str, op: CmpOp, value: &ScalarValue) -> Vec<ColumnBatch> {
        let mut result = Vec::new();

        // Hot parts
        for part in &self.parts {
            if !part.zone_map.can_skip(predicate_col, op, value) {
                result.push(part.data.clone());
            }
        }

        // Cold parts — check zone map, load from disk if needed
        for cold in &self.cold_parts {
            if !cold.zone_map.can_skip(predicate_col, op, value) {
                match SegmentReader::open(&cold.path) {
                    Ok(reader) => {
                        if let Ok(batch) = reader.read_batch() {
                            result.push(batch);
                        }
                    }
                    Err(e) => {
                        eprintln!("MergeTree: failed to read cold segment {:?}: {e}", cold.path);
                    }
                }
            }
        }

        result
    }

    /// Scan all parts (no predicate, full scan).
    ///
    /// Loads cold parts from disk as needed.
    pub fn scan_all(&self) -> Vec<ColumnBatch> {
        let mut result: Vec<ColumnBatch> = self.parts.iter().map(|p| p.data.clone()).collect();

        for cold in &self.cold_parts {
            match SegmentReader::open(&cold.path) {
                Ok(reader) => {
                    if let Ok(batch) = reader.read_batch() {
                        result.push(batch);
                    }
                }
                Err(e) => {
                    eprintln!("MergeTree: failed to read cold segment {:?}: {e}", cold.path);
                }
            }
        }

        result
    }

    /// Number of hot (in-memory) parts.
    pub fn part_count(&self) -> usize {
        self.parts.len()
    }

    /// Number of cold (on-disk) parts.
    pub fn cold_part_count(&self) -> usize {
        self.cold_parts.len()
    }

    /// Total number of parts (hot + cold).
    pub fn total_part_count(&self) -> usize {
        self.parts.len() + self.cold_parts.len()
    }

    /// Total row count across all parts (hot + cold).
    pub fn total_rows(&self) -> usize {
        let hot: usize = self.parts.iter().map(|p| p.row_count).sum();
        let cold: usize = self.cold_parts.iter().map(|p| p.row_count).sum();
        hot + cold
    }

    /// Force merge all hot parts into one.
    pub fn optimize(&mut self) {
        while self.parts.len() > 1 {
            self.merge_smallest_parts();
        }
    }

    /// Compress all hot parts using adaptive compression.
    pub fn compact(&mut self) {
        for part in &mut self.parts {
            if part.compressed.is_some() {
                continue;
            }
            let mut compressed_cols = Vec::new();
            for (name, col) in &part.data.columns {
                compressed_cols.push((name.clone(), compress_adaptive(col)));
            }
            part.compressed = Some(compressed_cols);
        }
    }

    // ─── Hot/Cold Tiering ─────────────────────────────────────────────────────

    /// Flush parts exceeding the cold threshold to disk segment files.
    ///
    /// Only operates when `data_dir` is set. Parts that are flushed are removed
    /// from the hot `parts` vec and replaced with `ColdPartInfo` entries that
    /// retain the zone map for pruning.
    pub fn flush_cold_parts(&mut self) {
        let data_dir = match &self.data_dir {
            Some(d) => d.clone(),
            None => return,
        };

        let mut to_flush = Vec::new();
        let mut i = 0;
        while i < self.parts.len() {
            let size = estimate_batch_size(&self.parts[i].data);
            if size > self.cold_threshold_bytes {
                to_flush.push(i);
            }
            i += 1;
        }

        // Flush in reverse index order so removal doesn't shift indices
        for &idx in to_flush.iter().rev() {
            let part = self.parts.remove(idx);
            let seg_path = data_dir.join(format!("part_{:016x}.seg", part.id));

            match SegmentWriter::write(&seg_path, &part.data, CompressionCodec::None, part.id) {
                Ok(()) => {
                    self.cold_parts.push(ColdPartInfo {
                        part_id: part.id,
                        path: seg_path,
                        zone_map: part.zone_map,
                        row_count: part.row_count,
                    });
                }
                Err(e) => {
                    eprintln!("MergeTree: failed to flush part {} to disk: {e}", part.id);
                    // Put it back as a hot part
                    self.parts.push(part);
                }
            }
        }
    }

    /// Load segment files from a directory into the cold parts list.
    fn load_segments_from_dir(&mut self, dir: &Path) -> std::io::Result<()> {
        let mut seg_files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
            .collect();
        seg_files.sort();

        for path in seg_files {
            match SegmentReader::open(&path) {
                Ok(reader) => {
                    let part_id = reader.part_id();
                    let zone_map = reader.read_zone_map();
                    let row_count = reader.row_count();

                    if part_id >= self.next_part_id {
                        self.next_part_id = part_id + 1;
                    }

                    self.cold_parts.push(ColdPartInfo {
                        part_id,
                        path,
                        zone_map,
                        row_count,
                    });
                }
                Err(e) => {
                    eprintln!("MergeTree: skipping corrupt segment {:?}: {e}", path);
                }
            }
        }

        Ok(())
    }

    /// Recover state: load cold segments from disk, then apply WAL-recovered
    /// hot parts on top. This is the full crash-recovery path.
    pub fn recover(primary_key: Vec<String>, dir: &Path, wal_batches: Vec<ColumnBatch>) -> std::io::Result<Self> {
        let mut tree = Self::open(primary_key, dir)?;
        // WAL-recovered batches are unflushed data — insert as hot parts
        for batch in wal_batches {
            let sorted = tree.sort_by_pk(&batch);
            let row_count = sorted.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
            let zone_map = ZoneMap::from_batch(&sorted);
            let part_id = tree.next_part_id;
            tree.next_part_id += 1;
            tree.parts.push(MergeTreePart {
                id: part_id,
                data: sorted,
                row_count,
                zone_map,
                compressed: None,
            });
        }
        Ok(tree)
    }
}

fn reorder_column(col: &ColumnData, indices: &[usize]) -> ColumnData {
    match col {
        ColumnData::Int32(vals) => {
            ColumnData::Int32(indices.iter().map(|&i| vals[i]).collect())
        }
        ColumnData::Int64(vals) => {
            ColumnData::Int64(indices.iter().map(|&i| vals[i]).collect())
        }
        ColumnData::Float64(vals) => {
            ColumnData::Float64(indices.iter().map(|&i| vals[i]).collect())
        }
        ColumnData::Text(vals) => {
            ColumnData::Text(indices.iter().map(|&i| vals[i].clone()).collect())
        }
        ColumnData::Bool(vals) => {
            ColumnData::Bool(indices.iter().map(|&i| vals[i]).collect())
        }
    }
}

/// Compare values in a column at two row indices. Returns Ordering for sort.
/// NULLs sort last (treated as maximum).
fn compare_column_values(col: Option<&ColumnData>, a: usize, b: usize) -> std::cmp::Ordering {
    match col {
        Some(ColumnData::Int64(vals)) => {
            let va = vals[a].unwrap_or(i64::MAX);
            let vb = vals[b].unwrap_or(i64::MAX);
            va.cmp(&vb)
        }
        Some(ColumnData::Int32(vals)) => {
            let va = vals[a].unwrap_or(i32::MAX);
            let vb = vals[b].unwrap_or(i32::MAX);
            va.cmp(&vb)
        }
        Some(ColumnData::Float64(vals)) => {
            let va = vals[a].unwrap_or(f64::MAX);
            let vb = vals[b].unwrap_or(f64::MAX);
            va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
        }
        Some(ColumnData::Text(vals)) => {
            let va = vals[a].as_deref().unwrap_or("");
            let vb = vals[b].as_deref().unwrap_or("");
            va.cmp(vb)
        }
        Some(ColumnData::Bool(vals)) => {
            let va = vals[a].unwrap_or(false) as u8;
            let vb = vals[b].unwrap_or(false) as u8;
            va.cmp(&vb)
        }
        None => std::cmp::Ordering::Equal,
    }
}

fn merge_sorted_batches(a: &ColumnBatch, b: &ColumnBatch, primary_key: &[String]) -> ColumnBatch {
    // Simple concatenation + sort approach
    let mut columns = Vec::new();
    for (name, col_a) in &a.columns {
        if let Some(col_b) = b.column(name) {
            let merged = concat_columns(col_a, col_b);
            columns.push((name.clone(), merged));
        }
    }

    let batch = ColumnBatch::new(columns);

    // Sort by PK if available (multi-column composite sort)
    if primary_key.is_empty() {
        return batch;
    }

    let row_count = batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
    let mut indices: Vec<usize> = (0..row_count).collect();

    indices.sort_by(|&a, &b| {
        for pk_col_name in primary_key {
            let ord = compare_column_values(batch.column(pk_col_name), a, b);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    let mut new_columns = Vec::with_capacity(batch.columns.len());
    for (name, col) in &batch.columns {
        new_columns.push((name.clone(), reorder_column(col, &indices)));
    }
    ColumnBatch::new(new_columns)
}

fn concat_columns(a: &ColumnData, b: &ColumnData) -> ColumnData {
    match (a, b) {
        (ColumnData::Int32(va), ColumnData::Int32(vb)) => {
            let mut result = va.clone();
            result.extend_from_slice(vb);
            ColumnData::Int32(result)
        }
        (ColumnData::Int64(va), ColumnData::Int64(vb)) => {
            let mut result = va.clone();
            result.extend_from_slice(vb);
            ColumnData::Int64(result)
        }
        (ColumnData::Float64(va), ColumnData::Float64(vb)) => {
            let mut result = va.clone();
            result.extend_from_slice(vb);
            ColumnData::Float64(result)
        }
        (ColumnData::Text(va), ColumnData::Text(vb)) => {
            let mut result = va.clone();
            result.extend_from_slice(vb);
            ColumnData::Text(result)
        }
        (ColumnData::Bool(va), ColumnData::Bool(vb)) => {
            let mut result = va.clone();
            result.extend_from_slice(vb);
            ColumnData::Bool(result)
        }
        _ => a.clone(),
    }
}

// ============================================================================
// ReplacingMergeTree: dedup by PK, keep latest version
// ============================================================================

/// Compare two rows in a batch by primary key columns. Returns true if they are equal.
fn pk_rows_equal(batch: &ColumnBatch, a: usize, b: usize, primary_key: &[String]) -> bool {
    for pk_col in primary_key {
        if compare_column_values(batch.column(pk_col), a, b) != std::cmp::Ordering::Equal {
            return false;
        }
    }
    true
}

/// Get a numeric "version" value from a column at a given row index.
/// Used for ReplacingMergeTree to decide which row wins.
fn version_value_at(col: Option<&ColumnData>, idx: usize) -> i64 {
    match col {
        Some(ColumnData::Int32(v)) => v[idx].unwrap_or(0) as i64,
        Some(ColumnData::Int64(v)) => v[idx].unwrap_or(0),
        Some(ColumnData::Float64(v)) => v[idx].unwrap_or(0.0) as i64,
        _ => 0,
    }
}

/// Append row `idx` from `src` to the column builders in `builders`.
fn append_row_to_builders(builders: &mut Vec<(String, ColumnBuilder)>, src: &ColumnBatch, idx: usize) {
    for (name, builder) in builders.iter_mut() {
        if let Some(col) = src.column(name) {
            builder.push_from(col, idx);
        }
    }
}

/// Column builder — accumulates values row by row to build a ColumnData.
#[derive(Debug)]
enum ColumnBuilder {
    Bool(Vec<Option<bool>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Text(Vec<Option<String>>),
}

impl ColumnBuilder {
    fn from_type(col: &ColumnData) -> Self {
        match col {
            ColumnData::Bool(_) => ColumnBuilder::Bool(Vec::new()),
            ColumnData::Int32(_) => ColumnBuilder::Int32(Vec::new()),
            ColumnData::Int64(_) => ColumnBuilder::Int64(Vec::new()),
            ColumnData::Float64(_) => ColumnBuilder::Float64(Vec::new()),
            ColumnData::Text(_) => ColumnBuilder::Text(Vec::new()),
        }
    }

    fn push_from(&mut self, col: &ColumnData, idx: usize) {
        match (self, col) {
            (ColumnBuilder::Bool(v), ColumnData::Bool(src)) => v.push(src[idx]),
            (ColumnBuilder::Int32(v), ColumnData::Int32(src)) => v.push(src[idx]),
            (ColumnBuilder::Int64(v), ColumnData::Int64(src)) => v.push(src[idx]),
            (ColumnBuilder::Float64(v), ColumnData::Float64(src)) => v.push(src[idx]),
            (ColumnBuilder::Text(v), ColumnData::Text(src)) => v.push(src[idx].clone()),
            _ => {}
        }
    }

    fn build(self) -> ColumnData {
        match self {
            ColumnBuilder::Bool(v) => ColumnData::Bool(v),
            ColumnBuilder::Int32(v) => ColumnData::Int32(v),
            ColumnBuilder::Int64(v) => ColumnData::Int64(v),
            ColumnBuilder::Float64(v) => ColumnData::Float64(v),
            ColumnBuilder::Text(v) => ColumnData::Text(v),
        }
    }
}

/// ReplacingMergeTree merge: dedup by PK, keep latest version.
///
/// The input batch must already be sorted by PK. Consecutive rows with
/// identical PK values are collapsed. If `version_column` is set, the row
/// with the highest version wins. Otherwise the last row (highest insertion
/// order) wins.
fn merge_replacing(batch: &ColumnBatch, primary_key: &[String], version_column: Option<&str>) -> ColumnBatch {
    let row_count = batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
    if row_count <= 1 || primary_key.is_empty() {
        return batch.clone();
    }

    // Build output column builders
    let mut builders: Vec<(String, ColumnBuilder)> = batch.columns.iter()
        .map(|(name, col)| (name.clone(), ColumnBuilder::from_type(col)))
        .collect();

    let mut i = 0;
    while i < row_count {
        // Find the range of rows with the same PK
        let mut j = i + 1;
        while j < row_count && pk_rows_equal(batch, i, j, primary_key) {
            j += 1;
        }

        // Among rows [i..j), pick the winner
        let winner = if j == i + 1 {
            i
        } else if let Some(vcol) = version_column {
            // Pick row with highest version
            let vc = batch.column(vcol);
            let mut best = i;
            let mut best_v = version_value_at(vc, i);
            for k in (i + 1)..j {
                let v = version_value_at(vc, k);
                if v >= best_v {
                    best_v = v;
                    best = k;
                }
            }
            best
        } else {
            // No version column: keep last row (latest insert)
            j - 1
        };

        append_row_to_builders(&mut builders, batch, winner);
        i = j;
    }

    let columns: Vec<(String, ColumnData)> = builders.into_iter()
        .map(|(name, b)| (name, b.build()))
        .collect();
    ColumnBatch::new(columns)
}

// ============================================================================
// AggregatingMergeTree: group by PK, aggregate specified columns
// ============================================================================

/// Sum two optional numeric values.
fn sum_opt_i32(a: Option<i32>, b: Option<i32>) -> Option<i32> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x + y),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn sum_opt_i64(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x + y),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn sum_opt_f64(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x + y),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

/// AggregatingMergeTree merge: group by PK, aggregate specified columns.
///
/// The input batch must already be sorted by PK. Consecutive rows with
/// identical PK values are collapsed:
/// - `sum_columns` and `count_columns`: values are summed
/// - Other numeric columns not in PK: keep last value
/// - Text columns not in PK: keep last value
fn merge_aggregating(
    batch: &ColumnBatch,
    primary_key: &[String],
    sum_columns: &[String],
    count_columns: &[String],
) -> ColumnBatch {
    let row_count = batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
    if row_count <= 1 || primary_key.is_empty() {
        return batch.clone();
    }

    // Build output column builders
    let mut builders: Vec<(String, ColumnBuilder)> = batch.columns.iter()
        .map(|(name, col)| (name.clone(), ColumnBuilder::from_type(col)))
        .collect();

    let is_aggregate_col = |name: &str| -> bool {
        sum_columns.iter().any(|s| s == name) || count_columns.iter().any(|s| s == name)
    };

    let mut i = 0;
    while i < row_count {
        // Find the range of rows with the same PK
        let mut j = i + 1;
        while j < row_count && pk_rows_equal(batch, i, j, primary_key) {
            j += 1;
        }

        if j == i + 1 {
            // Single row group — just copy it
            append_row_to_builders(&mut builders, batch, i);
        } else {
            // Multiple rows with same PK — aggregate
            for (name, builder) in builders.iter_mut() {
                let col = match batch.column(name) {
                    Some(c) => c,
                    None => continue,
                };

                let is_pk = primary_key.iter().any(|pk| pk == name);
                if is_pk {
                    // PK columns: take from first row (all equal)
                    builder.push_from(col, i);
                } else if is_aggregate_col(name) {
                    // Sum all values in the group
                    match (builder, col) {
                        (ColumnBuilder::Int32(out), ColumnData::Int32(src)) => {
                            let mut acc = None;
                            for k in i..j {
                                acc = sum_opt_i32(acc, src[k]);
                            }
                            out.push(acc);
                        }
                        (ColumnBuilder::Int64(out), ColumnData::Int64(src)) => {
                            let mut acc = None;
                            for k in i..j {
                                acc = sum_opt_i64(acc, src[k]);
                            }
                            out.push(acc);
                        }
                        (ColumnBuilder::Float64(out), ColumnData::Float64(src)) => {
                            let mut acc = None;
                            for k in i..j {
                                acc = sum_opt_f64(acc, src[k]);
                            }
                            out.push(acc);
                        }
                        (b, _) => {
                            // Non-numeric aggregate column: keep last
                            b.push_from(col, j - 1);
                        }
                    }
                } else {
                    // Non-aggregate, non-PK column: keep last value
                    builder.push_from(col, j - 1);
                }
            }
        }
        i = j;
    }

    let columns: Vec<(String, ColumnData)> = builders.into_iter()
        .map(|(name, b)| (name, b.build()))
        .collect();
    ColumnBatch::new(columns)
}

// ============================================================================
// Parallel Scan / Aggregation (std::thread::scope, zero new dependencies)
// ============================================================================

/// Default minimum number of batches required to justify spawning threads.
/// Below this threshold, parallel methods fall back to single-threaded execution.
pub const PAR_BATCH_THRESHOLD: usize = 4;

/// Determine the number of worker threads to use for a given batch count.
/// Returns 1 if below threshold, otherwise min(available_cpus, batch_count).
fn par_thread_count(batch_count: usize, threshold: usize) -> usize {
    if batch_count < threshold {
        return 1;
    }
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    cpus.min(batch_count)
}

// ---------------------------------------------------------------------------
// Free functions operating on &[ColumnBatch] — safe to call from code that
// already holds a read lock on the store.
// ---------------------------------------------------------------------------

/// Parallel SUM across column batches. Returns the total sum as f64.
/// Falls back to single-threaded for small batch counts.
pub fn par_sum_batches(batches: &[ColumnBatch], column: &str, threshold: usize) -> f64 {
    let threads = par_thread_count(batches.len(), threshold);
    if threads <= 1 {
        return batches.iter().map(|b| aggregate_sum(b, column)).sum();
    }
    let chunk_size = batches.len().div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(|| chunk.iter().map(|b| aggregate_sum(b, column)).sum::<f64>())
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .sum::<f64>()
    })
}

/// Parallel COUNT of total rows across column batches.
/// Falls back to single-threaded for small batch counts.
pub fn par_count_batches(batches: &[ColumnBatch], threshold: usize) -> usize {
    let threads = par_thread_count(batches.len(), threshold);
    if threads <= 1 {
        return batches.iter().map(|b| b.row_count).sum();
    }
    let chunk_size = batches.len().div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(|| chunk.iter().map(|b| b.row_count).sum::<usize>())
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .sum::<usize>()
    })
}

/// Parallel MIN across column batches. Returns the minimum f64 value found.
/// Falls back to single-threaded for small batch counts.
pub fn par_min_batches(batches: &[ColumnBatch], column: &str, threshold: usize) -> Option<f64> {
    let threads = par_thread_count(batches.len(), threshold);
    if threads <= 1 {
        return batches
            .iter()
            .filter_map(|b| batch_min_f64(b, column))
            .reduce(f64::min);
    }
    let chunk_size = batches.len().div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(|| {
                    chunk
                        .iter()
                        .filter_map(|b| batch_min_f64(b, column))
                        .reduce(f64::min)
                })
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|h| h.join().unwrap())
            .reduce(f64::min)
    })
}

/// Parallel MAX across column batches. Returns the maximum f64 value found.
/// Falls back to single-threaded for small batch counts.
pub fn par_max_batches(batches: &[ColumnBatch], column: &str, threshold: usize) -> Option<f64> {
    let threads = par_thread_count(batches.len(), threshold);
    if threads <= 1 {
        return batches
            .iter()
            .filter_map(|b| batch_max_f64(b, column))
            .reduce(f64::max);
    }
    let chunk_size = batches.len().div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(|| {
                    chunk
                        .iter()
                        .filter_map(|b| batch_max_f64(b, column))
                        .reduce(f64::max)
                })
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|h| h.join().unwrap())
            .reduce(f64::max)
    })
}

/// Parallel filtered scan: return all batches with only matching rows retained,
/// where `column` equals `value` (AggValue).
/// Falls back to single-threaded for small batch counts.
pub fn par_scan_where_eq_batches(
    batches: &[ColumnBatch],
    column: &str,
    value: &AggValue,
    threshold: usize,
) -> Vec<ColumnBatch> {
    let threads = par_thread_count(batches.len(), threshold);
    if threads <= 1 {
        return batches
            .iter()
            .map(|b| scan_batch_eq(b, column, value))
            .filter(|b| b.row_count > 0)
            .collect();
    }
    let chunk_size = batches.len().div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(|| {
                    chunk
                        .iter()
                        .map(|b| scan_batch_eq(b, column, value))
                        .filter(|b| b.row_count > 0)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    })
}

/// Parallel full scan: collect all batches in parallel.
/// Each thread processes a chunk of batches and returns them as-is.
/// Falls back to single-threaded clone for small batch counts.
pub fn par_scan_all_batches(batches: &[ColumnBatch], threshold: usize) -> Vec<ColumnBatch> {
    let threads = par_thread_count(batches.len(), threshold);
    if threads <= 1 {
        return batches.to_vec();
    }
    let chunk_size = batches.len().div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .chunks(chunk_size)
            .map(|chunk| s.spawn(|| chunk.to_vec()))
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    })
}

// ---------------------------------------------------------------------------
// Helpers for per-batch min/max as f64 (works across Int32/Int64/Float64)
// ---------------------------------------------------------------------------

fn batch_min_f64(batch: &ColumnBatch, column: &str) -> Option<f64> {
    match batch.column(column) {
        Some(ColumnData::Int32(v)) => v
            .iter()
            .filter_map(|x| x.as_ref())
            .map(|&x| x as f64)
            .reduce(f64::min),
        Some(ColumnData::Int64(v)) => v
            .iter()
            .filter_map(|x| x.as_ref())
            .map(|&x| x as f64)
            .reduce(f64::min),
        Some(ColumnData::Float64(v)) => v
            .iter()
            .filter_map(|x| x.as_ref())
            .copied()
            .reduce(f64::min),
        _ => None,
    }
}

fn batch_max_f64(batch: &ColumnBatch, column: &str) -> Option<f64> {
    match batch.column(column) {
        Some(ColumnData::Int32(v)) => v
            .iter()
            .filter_map(|x| x.as_ref())
            .map(|&x| x as f64)
            .reduce(f64::max),
        Some(ColumnData::Int64(v)) => v
            .iter()
            .filter_map(|x| x.as_ref())
            .map(|&x| x as f64)
            .reduce(f64::max),
        Some(ColumnData::Float64(v)) => v
            .iter()
            .filter_map(|x| x.as_ref())
            .copied()
            .reduce(f64::max),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Helper: equality filter on a single batch using AggValue
// ---------------------------------------------------------------------------

fn scan_batch_eq(batch: &ColumnBatch, column: &str, value: &AggValue) -> ColumnBatch {
    let col = match batch.column(column) {
        Some(c) => c,
        None => return ColumnBatch::new(vec![]),
    };
    let mask: Vec<bool> = match (col, value) {
        (ColumnData::Int32(v), AggValue::Int32(n)) => {
            v.iter().map(|o| o == &Some(*n)).collect()
        }
        (ColumnData::Int64(v), AggValue::Int64(n)) => {
            v.iter().map(|o| o == &Some(*n)).collect()
        }
        (ColumnData::Float64(v), AggValue::Float64(f)) => {
            v.iter().map(|o| o == &Some(*f)).collect()
        }
        (ColumnData::Text(v), AggValue::Text(s)) => {
            v.iter().map(|o| o.as_deref() == Some(s.as_str())).collect()
        }
        (ColumnData::Bool(v), AggValue::Bool(b)) => {
            v.iter().map(|o| o == &Some(*b)).collect()
        }
        _ => vec![false; batch.row_count],
    };
    apply_mask(batch, &mask)
}

// ---------------------------------------------------------------------------
// ColumnarStore convenience methods (take read lock once, then delegate)
// ---------------------------------------------------------------------------

impl ColumnarStore {
    /// Parallel SUM of a numeric column across all batches of a table.
    /// Returns `None` if the table does not exist, `Some(0.0)` if it is empty.
    pub fn par_aggregate_sum(&self, table: &str, column: &str) -> Option<f64> {
        let batches = self.tables.get(table)?;
        Some(par_sum_batches(batches, column, PAR_BATCH_THRESHOLD))
    }

    /// Parallel COUNT of all rows across all batches of a table.
    /// Returns `None` if the table does not exist.
    pub fn par_aggregate_count(&self, table: &str) -> Option<usize> {
        let batches = self.tables.get(table)?;
        Some(par_count_batches(batches, PAR_BATCH_THRESHOLD))
    }

    /// Parallel MIN of a numeric column across all batches of a table.
    /// Returns `None` if the table does not exist or has no non-null values.
    pub fn par_aggregate_min(&self, table: &str, column: &str) -> Option<f64> {
        let batches = self.tables.get(table)?;
        par_min_batches(batches, column, PAR_BATCH_THRESHOLD)
    }

    /// Parallel MAX of a numeric column across all batches of a table.
    /// Returns `None` if the table does not exist or has no non-null values.
    pub fn par_aggregate_max(&self, table: &str, column: &str) -> Option<f64> {
        let batches = self.tables.get(table)?;
        par_max_batches(batches, column, PAR_BATCH_THRESHOLD)
    }

    /// Parallel filtered scan returning batches with only rows matching
    /// `column == value`.
    pub fn par_scan_where_eq(
        &self,
        table: &str,
        column: &str,
        value: &AggValue,
    ) -> Vec<ColumnBatch> {
        let batches = match self.tables.get(table) {
            Some(b) => b,
            None => return vec![],
        };
        par_scan_where_eq_batches(batches, column, value, PAR_BATCH_THRESHOLD)
    }

    /// Parallel full scan returning cloned batches.
    pub fn par_scan_all(&self, table: &str) -> Vec<ColumnBatch> {
        let batches = match self.tables.get(table) {
            Some(b) => b,
            None => return vec![],
        };
        par_scan_all_batches(batches, PAR_BATCH_THRESHOLD)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_batch() -> ColumnBatch {
        ColumnBatch::new(vec![
            (
                "name".into(),
                ColumnData::Text(vec![
                    Some("Alice".into()),
                    Some("Bob".into()),
                    Some("Charlie".into()),
                    Some("Alice".into()),
                    Some("Bob".into()),
                ]),
            ),
            (
                "age".into(),
                ColumnData::Int64(vec![
                    Some(30),
                    Some(25),
                    Some(35),
                    Some(28),
                    Some(32),
                ]),
            ),
            (
                "salary".into(),
                ColumnData::Float64(vec![
                    Some(80000.0),
                    Some(65000.0),
                    Some(90000.0),
                    Some(75000.0),
                    Some(70000.0),
                ]),
            ),
        ])
    }

    #[test]
    fn sum_and_avg() {
        let batch = sample_batch();
        let age_col = match batch.column("age").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        assert_eq!(sum_i64(age_col), 150); // 30+25+35+28+32
        assert!((avg_i64(age_col).unwrap() - 30.0).abs() < 1e-10);
    }

    #[test]
    fn min_max() {
        let batch = sample_batch();
        let age_col = match batch.column("age").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        assert_eq!(min_i64(age_col), Some(25));
        assert_eq!(max_i64(age_col), Some(35));
    }

    #[test]
    fn filter_and_scan() {
        let batch = sample_batch();
        let age_col = match batch.column("age").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };

        // Filter age > 30
        let mask = filter_i64(age_col, &Predicate::GtI64(30));
        assert_eq!(mask, vec![false, false, true, false, true]); // Charlie=35, Bob=32

        let filtered = apply_mask(&batch, &mask);
        assert_eq!(filtered.row_count, 2);
    }

    #[test]
    fn group_by_aggregation() {
        let batch = sample_batch();
        let name_col = match batch.column("name").unwrap() {
            ColumnData::Text(v) => v,
            _ => panic!("expected Text"),
        };
        let salary_col = match batch.column("salary").unwrap() {
            ColumnData::Float64(v) => v,
            _ => panic!("expected Float64"),
        };

        let result = group_by_text_agg_f64(name_col, salary_col);
        assert_eq!(result.groups.len(), 3); // Alice, Bob, Charlie

        let alice = result.groups.iter().find(|g| g.key == "Alice").unwrap();
        assert_eq!(alice.count, 2);
        assert!((alice.sum.unwrap() - 155000.0).abs() < 1e-10);
        assert!((alice.avg.unwrap() - 77500.0).abs() < 1e-10);
    }

    #[test]
    fn columnar_store_basic() {
        let mut store = ColumnarStore::new();
        let batch = sample_batch();
        store.append("employees", batch);

        assert_eq!(store.row_count("employees"), 5);
        assert_eq!(store.batches("employees").len(), 1);
    }

    #[test]
    fn count_nulls() {
        let col = ColumnData::Int64(vec![Some(1), None, Some(3), None, Some(5)]);
        assert_eq!(count_non_null(&col), 3);
    }

    #[test]
    fn between_filter() {
        let col = vec![Some(10i64), Some(20), Some(30), Some(40), Some(50)];
        let mask = filter_i64(&col, &Predicate::BetweenI64(20, 40));
        assert_eq!(mask, vec![false, true, true, true, false]);
    }

    #[test]
    fn mixed_column_types_in_store() {
        let mut store = ColumnarStore::new();
        let batch = ColumnBatch::new(vec![
            ("active".into(), ColumnData::Bool(vec![Some(true), Some(false), None, Some(true)])),
            ("score_i32".into(), ColumnData::Int32(vec![Some(100), Some(200), Some(300), None])),
            ("score_i64".into(), ColumnData::Int64(vec![Some(1000), None, Some(3000), Some(4000)])),
            ("rating".into(), ColumnData::Float64(vec![Some(4.5), Some(3.2), None, Some(4.8)])),
            ("label".into(), ColumnData::Text(vec![Some("A".into()), Some("B".into()), Some("C".into()), None])),
        ]);
        assert_eq!(batch.row_count, 4);
        store.append("mixed", batch);
        let batches = store.batches("mixed");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].row_count, 4);
        assert!(matches!(batches[0].column("active"), Some(ColumnData::Bool(_))));
        assert!(matches!(batches[0].column("score_i32"), Some(ColumnData::Int32(_))));
        assert!(matches!(batches[0].column("score_i64"), Some(ColumnData::Int64(_))));
        assert!(matches!(batches[0].column("rating"), Some(ColumnData::Float64(_))));
        assert!(matches!(batches[0].column("label"), Some(ColumnData::Text(_))));
        assert!(batches[0].column("nonexistent").is_none());
    }

    #[test]
    fn aggregation_sum_avg_min_max_f64() {
        let col = vec![Some(10.5), Some(20.3), None, Some(5.1), Some(30.0), None];
        assert!((sum_f64(&col) - 65.9).abs() < 1e-10);
        assert!((avg_f64(&col).unwrap() - (65.9 / 4.0)).abs() < 1e-10);
        assert!((min_f64(&col).unwrap() - 5.1).abs() < 1e-10);
        assert!((max_f64(&col).unwrap() - 30.0).abs() < 1e-10);
    }

    #[test]
    fn aggregation_on_all_nulls() {
        let empty_i64: Vec<Option<i64>> = vec![None, None, None];
        assert_eq!(sum_i64(&empty_i64), 0);
        assert!(avg_i64(&empty_i64).is_none());
        assert!(min_i64(&empty_i64).is_none());
        assert!(max_i64(&empty_i64).is_none());
        let empty_f64: Vec<Option<f64>> = vec![None, None];
        assert!((sum_f64(&empty_f64) - 0.0).abs() < 1e-10);
        assert!(avg_f64(&empty_f64).is_none());
        assert!(min_f64(&empty_f64).is_none());
        assert!(max_f64(&empty_f64).is_none());
    }

    #[test]
    fn filter_predicate_eq_and_lt() {
        let col = vec![Some(10i64), Some(20), None, Some(30), Some(20)];
        let mask = filter_i64(&col, &Predicate::EqI64(20));
        assert_eq!(mask, vec![false, true, false, false, true]);
        let mask = filter_i64(&col, &Predicate::LtI64(20));
        assert_eq!(mask, vec![true, false, false, false, false]);
    }

    #[test]
    fn predicate_pushdown_with_mask_application() {
        let batch = ColumnBatch::new(vec![
            ("city".into(), ColumnData::Text(vec![
                Some("NYC".into()), Some("LA".into()), Some("NYC".into()),
                Some("Chicago".into()), Some("LA".into()),
            ])),
            ("revenue".into(), ColumnData::Int64(vec![
                Some(100), Some(200), Some(150), Some(50), Some(300),
            ])),
        ]);
        let rev_col = match batch.column("revenue").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        let mask = filter_i64(rev_col, &Predicate::GtI64(100));
        let filtered = apply_mask(&batch, &mask);
        assert_eq!(filtered.row_count, 3);
        let filtered_rev = match filtered.column("revenue").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        assert_eq!(sum_i64(filtered_rev), 650);
        assert_eq!(min_i64(filtered_rev), Some(150));
        assert_eq!(max_i64(filtered_rev), Some(300));
    }

    #[test]
    fn null_handling_count_all_types() {
        let bool_col = ColumnData::Bool(vec![Some(true), None, Some(false), None]);
        assert_eq!(count_non_null(&bool_col), 2);
        let i32_col = ColumnData::Int32(vec![None, None, Some(42)]);
        assert_eq!(count_non_null(&i32_col), 1);
        let text_col = ColumnData::Text(vec![Some("hello".into()), None, None, Some("world".into()), None]);
        assert_eq!(count_non_null(&text_col), 2);
        let empty_col = ColumnData::Float64(vec![]);
        assert_eq!(count_non_null(&empty_col), 0);
        assert!(empty_col.is_empty());
    }

    #[test]
    fn batch_operations_multiple_batches() {
        let mut store = ColumnarStore::new();
        for i in 0..3 {
            let base = (i * 10) as i64;
            let batch = ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64(vec![
                    Some(base + 1), Some(base + 2), Some(base + 3),
                ])),
            ]);
            store.append("events", batch);
        }
        assert_eq!(store.batches("events").len(), 3);
        assert_eq!(store.row_count("events"), 9);
        let batches = store.batches("events");
        let first_id_col = match batches[0].column("id").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        assert_eq!(first_id_col, &vec![Some(1), Some(2), Some(3)]);
        let third_id_col = match batches[2].column("id").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        assert_eq!(third_id_col, &vec![Some(21), Some(22), Some(23)]);
    }

    #[test]
    fn large_column_count() {
        let num_cols = 50;
        let num_rows = 100;
        let mut columns = Vec::with_capacity(num_cols);
        for c in 0..num_cols {
            let data: Vec<Option<i64>> = (0..num_rows)
                .map(|r| Some((c * num_rows + r) as i64)).collect();
            columns.push((format!("col_{}", c), ColumnData::Int64(data)));
        }
        let batch = ColumnBatch::new(columns);
        assert_eq!(batch.row_count, num_rows);
        assert_eq!(batch.columns.len(), num_cols);
        let first = match batch.column("col_0").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        assert_eq!(first[0], Some(0));
        assert_eq!(first[99], Some(99));
        let last = match batch.column("col_49").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        assert_eq!(last[0], Some(4900));
        assert_eq!(last[99], Some(4999));
        assert_eq!(sum_i64(first), 4950);
    }

    #[test]
    fn group_by_with_nulls() {
        let key_col: Vec<Option<String>> = vec![
            Some("X".into()), Some("Y".into()), None,
            Some("X".into()), None, Some("Y".into()),
        ];
        let val_col: Vec<Option<f64>> = vec![
            Some(10.0), Some(20.0), Some(99.0),
            Some(30.0), Some(99.0), None,
        ];
        let result = group_by_text_agg_f64(&key_col, &val_col);
        let x = result.groups.iter().find(|g| g.key == "X").unwrap();
        assert_eq!(x.count, 2);
        assert!((x.sum.unwrap() - 40.0).abs() < 1e-10);
        let y = result.groups.iter().find(|g| g.key == "Y").unwrap();
        assert_eq!(y.count, 1);
        assert!((y.sum.unwrap() - 20.0).abs() < 1e-10);
    }

    // ========================================================================
    // Text predicate tests
    // ========================================================================

    fn text_batch() -> ColumnBatch {
        ColumnBatch::new(vec![
            (
                "email".into(),
                ColumnData::Text(vec![
                    Some("alice@example.com".into()),
                    Some("bob@test.org".into()),
                    Some("charlie@example.com".into()),
                    None,
                    Some("dave@test.org".into()),
                    Some("eve@sample.net".into()),
                ]),
            ),
            (
                "score".into(),
                ColumnData::Int64(vec![Some(95), Some(80), Some(70), Some(60), Some(85), Some(90)]),
            ),
        ])
    }

    #[test]
    fn text_predicate_starts_with() {
        let batch = text_batch();
        let col = match batch.column("email").unwrap() {
            ColumnData::Text(v) => v, _ => panic!("expected Text"),
        };
        let mask = filter_text(col, &Predicate::StartsWithText("alice".into()));
        assert_eq!(mask, vec![true, false, false, false, false, false]);
        let filtered = apply_mask(&batch, &mask);
        assert_eq!(filtered.row_count, 1);
    }

    #[test]
    fn text_predicate_contains() {
        let batch = text_batch();
        let col = match batch.column("email").unwrap() {
            ColumnData::Text(v) => v, _ => panic!("expected Text"),
        };
        let mask = filter_text(col, &Predicate::ContainsText("example".into()));
        assert_eq!(mask, vec![true, false, true, false, false, false]);
        let filtered = apply_mask(&batch, &mask);
        assert_eq!(filtered.row_count, 2);
    }

    // ========================================================================
    // Zone Map tests
    // ========================================================================

    #[test]
    fn zone_map_basic_stats() {
        let batch = sample_batch();
        let zm = ZoneMap::from_batch(&batch);

        let age_zm = zm.columns.get("age").unwrap();
        assert_eq!(age_zm.min, Some(ScalarValue::Int64(25)));
        assert_eq!(age_zm.max, Some(ScalarValue::Int64(35)));
        assert_eq!(age_zm.null_count, 0);
        assert_eq!(age_zm.row_count, 5);

        let name_zm = zm.columns.get("name").unwrap();
        assert_eq!(name_zm.min, Some(ScalarValue::Text("Alice".into())));
        assert_eq!(name_zm.max, Some(ScalarValue::Text("Charlie".into())));
    }

    #[test]
    fn zone_map_can_skip_gt() {
        let batch = sample_batch();
        let zm = ZoneMap::from_batch(&batch);
        assert!(zm.can_skip("age", CmpOp::Gt, &ScalarValue::Int64(50)));
        assert!(!zm.can_skip("age", CmpOp::Gt, &ScalarValue::Int64(34)));
        assert!(zm.can_skip("age", CmpOp::Gt, &ScalarValue::Int64(35)));
    }

    #[test]
    fn zone_map_can_skip_lt() {
        let batch = sample_batch();
        let zm = ZoneMap::from_batch(&batch);
        assert!(zm.can_skip("age", CmpOp::Lt, &ScalarValue::Int64(10)));
        assert!(zm.can_skip("age", CmpOp::Lt, &ScalarValue::Int64(25)));
        assert!(!zm.can_skip("age", CmpOp::Lt, &ScalarValue::Int64(26)));
    }

    #[test]
    fn zone_map_can_skip_eq() {
        let batch = sample_batch();
        let zm = ZoneMap::from_batch(&batch);
        assert!(zm.can_skip("age", CmpOp::Eq, &ScalarValue::Int64(20)));
        assert!(zm.can_skip("age", CmpOp::Eq, &ScalarValue::Int64(40)));
        assert!(!zm.can_skip("age", CmpOp::Eq, &ScalarValue::Int64(30)));
        assert!(!zm.can_skip("age", CmpOp::Eq, &ScalarValue::Int64(25)));
    }
    #[test]
    fn zone_map_can_skip_gte_lte() {
        let batch = sample_batch();
        let zm = ZoneMap::from_batch(&batch);
        assert!(zm.can_skip("age", CmpOp::Gte, &ScalarValue::Int64(36)));
        assert!(!zm.can_skip("age", CmpOp::Gte, &ScalarValue::Int64(35)));
        assert!(zm.can_skip("age", CmpOp::Lte, &ScalarValue::Int64(24)));
        assert!(!zm.can_skip("age", CmpOp::Lte, &ScalarValue::Int64(25)));
    }

    #[test]
    fn zone_map_all_nulls_skips() {
        let batch = ColumnBatch::new(vec![
            ("val".into(), ColumnData::Int64(vec![None, None, None])),
        ]);
        let zm = ZoneMap::from_batch(&batch);
        assert!(zm.can_skip("val", CmpOp::Eq, &ScalarValue::Int64(0)));
        assert!(zm.can_skip("val", CmpOp::Gt, &ScalarValue::Int64(0)));
        assert!(zm.can_skip("val", CmpOp::Lt, &ScalarValue::Int64(0)));
    }

    #[test]
    fn zone_map_unknown_column_does_not_skip() {
        let batch = sample_batch();
        let zm = ZoneMap::from_batch(&batch);
        assert!(!zm.can_skip("nonexistent", CmpOp::Eq, &ScalarValue::Int64(0)));
    }

    #[test]
    fn zone_map_text_column() {
        let batch = text_batch();
        let zm = ZoneMap::from_batch(&batch);
        let email_zm = zm.columns.get("email").unwrap();
        assert_eq!(email_zm.null_count, 1);
        assert_eq!(email_zm.row_count, 6);
        assert_eq!(email_zm.min, Some(ScalarValue::Text("alice@example.com".into())));
    }

    #[test]
    fn zone_map_float64_column() {
        let batch = sample_batch();
        let zm = ZoneMap::from_batch(&batch);
        let sal_zm = zm.columns.get("salary").unwrap();
        assert_eq!(sal_zm.min, Some(ScalarValue::Float64(65000.0)));
        assert_eq!(sal_zm.max, Some(ScalarValue::Float64(90000.0)));
        assert!(zm.can_skip("salary", CmpOp::Gt, &ScalarValue::Float64(100000.0)));
        assert!(!zm.can_skip("salary", CmpOp::Gt, &ScalarValue::Float64(80000.0)));
    }

    #[test]
    fn zone_map_bool_column() {
        let batch = ColumnBatch::new(vec![
            ("flag".into(), ColumnData::Bool(vec![Some(true), Some(false), None])),
        ]);
        let zm = ZoneMap::from_batch(&batch);
        let flag_zm = zm.columns.get("flag").unwrap();
        assert_eq!(flag_zm.min, Some(ScalarValue::Bool(false)));
        assert_eq!(flag_zm.max, Some(ScalarValue::Bool(true)));
        assert_eq!(flag_zm.null_count, 1);
    }
    // ========================================================================
    // Null Bitmap tests
    // ========================================================================

    #[test]
    fn null_bitmap_new_all_non_null() {
        let bm = NullBitmap::new(100);
        assert_eq!(bm.len(), 100);
        assert_eq!(bm.count_nulls(), 0);
        assert_eq!(bm.count_non_nulls(), 100);
        assert!(!bm.is_null(0));
        assert!(!bm.is_null(99));
    }

    #[test]
    fn null_bitmap_set_and_check() {
        let mut bm = NullBitmap::new(200);
        bm.set_null(0);
        bm.set_null(63);
        bm.set_null(64);
        bm.set_null(127);
        bm.set_null(199);
        assert!(bm.is_null(0));
        assert!(bm.is_null(63));
        assert!(bm.is_null(64));
        assert!(bm.is_null(127));
        assert!(bm.is_null(199));
        assert!(!bm.is_null(1));
        assert!(!bm.is_null(65));
        assert!(!bm.is_null(198));
        assert_eq!(bm.count_nulls(), 5);
        assert_eq!(bm.count_non_nulls(), 195);
    }

    #[test]
    fn null_bitmap_clear_null() {
        let mut bm = NullBitmap::new(10);
        bm.set_null(3);
        bm.set_null(7);
        assert_eq!(bm.count_nulls(), 2);
        bm.clear_null(3);
        assert!(!bm.is_null(3));
        assert_eq!(bm.count_nulls(), 1);
        bm.clear_null(7);
        assert_eq!(bm.count_nulls(), 0);
    }

    #[test]
    fn null_bitmap_from_column() {
        let col = ColumnData::Int64(vec![Some(1), None, Some(3), None, Some(5)]);
        let bm = NullBitmap::from_column(&col);
        assert_eq!(bm.len(), 5);
        assert!(!bm.is_null(0));
        assert!(bm.is_null(1));
        assert!(!bm.is_null(2));
        assert!(bm.is_null(3));
        assert!(!bm.is_null(4));
        assert_eq!(bm.count_nulls(), 2);
        assert_eq!(bm.count_non_nulls(), 3);
    }

    #[test]
    fn null_bitmap_from_column_text() {
        let col = ColumnData::Text(vec![Some("a".into()), None, None]);
        let bm = NullBitmap::from_column(&col);
        assert_eq!(bm.count_nulls(), 2);
        assert_eq!(bm.count_non_nulls(), 1);
    }

    #[test]
    fn null_bitmap_empty() {
        let bm = NullBitmap::new(0);
        assert!(bm.is_empty());
        assert_eq!(bm.count_nulls(), 0);
        assert_eq!(bm.count_non_nulls(), 0);
    }

    #[test]
    fn null_bitmap_exact_word_boundary() {
        let mut bm = NullBitmap::new(64);
        bm.set_null(0);
        bm.set_null(63);
        assert_eq!(bm.count_nulls(), 2);
        assert_eq!(bm.count_non_nulls(), 62);
    }

    #[test]
    fn null_bitmap_large() {
        let mut bm = NullBitmap::new(1000);
        for i in (0..1000).step_by(3) {
            bm.set_null(i);
        }
        assert_eq!(bm.count_nulls(), 334);
        assert_eq!(bm.count_non_nulls(), 666);
    }

    #[test]
    fn null_bitmap_memory_efficiency() {
        let bm = NullBitmap::new(1_000_000);
        assert_eq!(bm.len(), 1_000_000);
        assert_eq!(bm.count_nulls(), 0);
    }
    // ========================================================================
    // Late Materialization tests
    // ========================================================================

    #[test]
    fn filter_positions_basic() {
        let batch = sample_batch();
        let positions = filter_positions(&batch, "age", &Predicate::GtI64(30));
        assert_eq!(positions, vec![2, 4]);
    }

    #[test]
    fn filter_positions_no_match() {
        let batch = sample_batch();
        let positions = filter_positions(&batch, "age", &Predicate::GtI64(100));
        assert!(positions.is_empty());
    }

    #[test]
    fn filter_positions_all_match() {
        let batch = sample_batch();
        let positions = filter_positions(&batch, "age", &Predicate::GtI64(0));
        assert_eq!(positions, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn filter_positions_unknown_column() {
        let batch = sample_batch();
        let positions = filter_positions(&batch, "nonexistent", &Predicate::GtI64(0));
        assert!(positions.is_empty());
    }

    #[test]
    fn filter_positions_text_column() {
        let batch = text_batch();
        let positions = filter_positions(&batch, "email", &Predicate::ContainsText("example".into()));
        assert_eq!(positions, vec![0, 2]);
    }

    #[test]
    fn gather_selected_columns() {
        let batch = sample_batch();
        let positions = filter_positions(&batch, "age", &Predicate::GtI64(30));
        assert_eq!(positions, vec![2, 4]);
        let result = gather(&batch, &positions, &["name"]);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns.len(), 1);
        let names = match result.column("name").unwrap() {
            ColumnData::Text(v) => v,
            _ => panic!("expected Text"),
        };
        assert_eq!(names, &vec![Some("Charlie".into()), Some("Bob".into())]);
    }

    #[test]
    fn gather_all_columns() {
        let batch = sample_batch();
        let positions = vec![0, 2];
        let result = gather(&batch, &positions, &[]);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns.len(), 3);
        let ages = match result.column("age").unwrap() {
            ColumnData::Int64(v) => v,
            _ => panic!("expected Int64"),
        };
        assert_eq!(ages, &vec![Some(30), Some(35)]);
    }

    #[test]
    fn gather_empty_positions() {
        let batch = sample_batch();
        let result = gather(&batch, &[], &["name", "age"]);
        assert_eq!(result.row_count, 0);
    }
    #[test]
    fn late_materialization_end_to_end() {
        let batch = sample_batch();
        let zm = ZoneMap::from_batch(&batch);
        assert!(!zm.can_skip("age", CmpOp::Gt, &ScalarValue::Int64(30)));
        let positions = filter_positions(&batch, "age", &Predicate::GtI64(30));
        assert_eq!(positions, vec![2, 4]);
        let result = gather(&batch, &positions, &["name"]);
        assert_eq!(result.row_count, 2);
        let names = match result.column("name").unwrap() {
            ColumnData::Text(v) => v,
            _ => panic!("expected Text"),
        };
        assert_eq!(names[0], Some("Charlie".into()));
        assert_eq!(names[1], Some("Bob".into()));
    }

    #[test]
    fn late_materialization_with_zone_map_skip() {
        let young_batch = ColumnBatch::new(vec![
            ("name".into(), ColumnData::Text(vec![Some("A".into()), Some("B".into())])),
            ("age".into(), ColumnData::Int64(vec![Some(20), Some(22)])),
        ]);
        let old_batch = ColumnBatch::new(vec![
            ("name".into(), ColumnData::Text(vec![Some("C".into()), Some("D".into())])),
            ("age".into(), ColumnData::Int64(vec![Some(50), Some(55)])),
        ]);

        let mut store = ColumnarStore::new();
        store.append("people", young_batch);
        store.append("people", old_batch);

        let target = ScalarValue::Int64(40);
        let mut results = Vec::new();
        for batch in store.batches("people") {
            let zm = ZoneMap::from_batch(batch);
            if zm.can_skip("age", CmpOp::Gt, &target) {
                continue;
            }
            let positions = filter_positions(batch, "age", &Predicate::GtI64(40));
            if !positions.is_empty() {
                results.push(gather(batch, &positions, &["name"]));
            }
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_count, 2);
    }

    // ================================================================
    // Adaptive Compression tests
    // ================================================================

    #[test]
    fn compress_rle_i64() {
        let col = ColumnData::Int64(vec![Some(1), Some(1), Some(1), Some(2), Some(2), Some(3)]);
        let compressed = compress_column(&col, CompressionCodec::Rle);
        assert_eq!(compressed.codec, CompressionCodec::Rle);
        assert_eq!(compressed.original_len, 6);
        let decompressed = decompress_column(&compressed);
        assert_eq!(decompressed.len(), 6);
        if let ColumnData::Int64(vals) = decompressed {
            assert_eq!(vals, vec![Some(1), Some(1), Some(1), Some(2), Some(2), Some(3)]);
        } else {
            panic!("wrong type");
        }
    }

    #[test]
    fn compress_delta_i64() {
        let col = ColumnData::Int64(vec![Some(100), Some(102), Some(105), Some(110)]);
        let compressed = compress_column(&col, CompressionCodec::Delta);
        if let CompressedData::DeltaI64 { base, deltas, .. } = &compressed.data {
            assert_eq!(*base, 100);
            assert_eq!(deltas, &[2, 3, 5]);
        }
        let decompressed = decompress_column(&compressed);
        if let ColumnData::Int64(vals) = decompressed {
            assert_eq!(vals, vec![Some(100), Some(102), Some(105), Some(110)]);
        }
    }

    #[test]
    fn compress_dictionary_text() {
        let col = ColumnData::Text(vec![
            Some("red".into()), Some("blue".into()), Some("red".into()),
            None, Some("blue".into()), Some("red".into()),
        ]);
        let compressed = compress_column(&col, CompressionCodec::Dictionary);
        if let CompressedData::DictionaryText { dict, indices, nulls } = &compressed.data {
            assert!(dict.len() <= 2); // only "red" and "blue"
            assert_eq!(indices.len(), 6);
            assert!(nulls[3]); // null at index 3
        }
        let decompressed = decompress_column(&compressed);
        if let ColumnData::Text(vals) = decompressed {
            assert_eq!(vals[0], Some("red".into()));
            assert_eq!(vals[3], None);
        }
    }

    #[test]
    fn compress_for_i64() {
        let col = ColumnData::Int64(vec![Some(1000), Some(1005), Some(1002), Some(1010)]);
        let compressed = compress_column(&col, CompressionCodec::FrameOfReference);
        if let CompressedData::ForI64 { min_val, offsets, .. } = &compressed.data {
            assert_eq!(*min_val, 1000);
            assert_eq!(offsets, &[0, 5, 2, 10]);
        }
        let decompressed = decompress_column(&compressed);
        if let ColumnData::Int64(vals) = decompressed {
            assert_eq!(vals, vec![Some(1000), Some(1005), Some(1002), Some(1010)]);
        }
    }

    #[test]
    fn adaptive_selects_delta_for_sorted() {
        let col = ColumnData::Int64((0..100).map(|i| Some(i * 3)).collect());
        let codec = select_codec(&col);
        assert_eq!(codec, CompressionCodec::Delta);
    }

    #[test]
    fn adaptive_selects_for_narrow_range() {
        let col = ColumnData::Int64(vec![Some(500), Some(510), Some(505), Some(520), Some(530)]);
        let codec = select_codec(&col);
        assert_eq!(codec, CompressionCodec::FrameOfReference);
    }

    #[test]
    fn adaptive_selects_dictionary_for_low_cardinality() {
        let col = ColumnData::Text((0..100).map(|i| Some(if i % 3 == 0 { "A" } else { "B" }.to_string())).collect());
        let codec = select_codec(&col);
        assert_eq!(codec, CompressionCodec::Dictionary);
    }

    #[test]
    fn compress_adaptive_roundtrip() {
        let col = ColumnData::Int64(vec![Some(10), Some(10), Some(10), Some(20), Some(20)]);
        let compressed = compress_adaptive(&col);
        let decompressed = decompress_column(&compressed);
        assert_eq!(decompressed.len(), 5);
    }

    #[test]
    fn compressed_size_smaller() {
        // RLE should be much smaller for repeated data
        let col = ColumnData::Int64(vec![Some(42); 1000]);
        let compressed = compress_column(&col, CompressionCodec::Rle);
        let raw_size = 1000 * 8;
        let comp_size = compressed_size(&compressed);
        assert!(comp_size < raw_size);
    }

    #[test]
    fn compress_empty_column() {
        let col = ColumnData::Int64(vec![]);
        let compressed = compress_adaptive(&col);
        let decompressed = decompress_column(&compressed);
        assert_eq!(decompressed.len(), 0);
    }

    // ================================================================
    // MergeTree tests
    // ================================================================

    #[test]
    fn mergetree_insert_and_scan() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(3), Some(1), Some(2)])),
            ("name".into(), ColumnData::Text(vec![
                Some("charlie".into()), Some("alice".into()), Some("bob".into()),
            ])),
        ]);
        mt.insert(batch);

        assert_eq!(mt.part_count(), 1);
        assert_eq!(mt.total_rows(), 3);

        // Data should be sorted by id
        let parts = mt.scan_all();
        if let Some(ColumnData::Int64(ids)) = parts[0].column("id") {
            assert_eq!(ids, &[Some(1), Some(2), Some(3)]);
        }
    }

    #[test]
    fn mergetree_auto_merge() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 3;

        for i in 0..5 {
            let batch = ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64(vec![Some(i)])),
            ]);
            mt.insert(batch);
        }

        // After 5 inserts with max_parts=3, some merges should have occurred
        assert!(mt.part_count() <= 3);
        assert_eq!(mt.total_rows(), 5);
    }

    #[test]
    fn mergetree_optimize() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 100; // don't auto-merge

        for i in 0..5 {
            let batch = ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64(vec![Some(i)])),
            ]);
            mt.insert(batch);
        }

        assert_eq!(mt.part_count(), 5);
        mt.optimize();
        assert_eq!(mt.part_count(), 1);
        assert_eq!(mt.total_rows(), 5);
    }

    #[test]
    fn mergetree_partition_pruning() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 100;

        // Part 1: ids 1-10
        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((1..=10).map(|i| Some(i)).collect())),
        ]);
        mt.insert(batch);

        // Part 2: ids 100-110
        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((100..=110).map(|i| Some(i)).collect())),
        ]);
        mt.insert(batch);

        // Scanning for id > 50 should prune part 1
        let results = mt.scan("id", CmpOp::Gt, &ScalarValue::Int64(50));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn mergetree_compact_compresses() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((0..100).map(|i| Some(i)).collect())),
        ]);
        mt.insert(batch);

        assert!(mt.parts[0].compressed.is_none());
        mt.compact();
        assert!(mt.parts[0].compressed.is_some());
    }

    #[test]
    fn mergetree_merge_preserves_sort() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 100;

        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(5), Some(3), Some(1)])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(4), Some(2), Some(6)])),
        ]));

        mt.optimize();

        let parts = mt.scan_all();
        if let Some(ColumnData::Int64(ids)) = parts[0].column("id") {
            // Should be sorted after merge
            let sorted: Vec<Option<i64>> = (1..=6).map(Some).collect();
            assert_eq!(ids, &sorted);
        }
    }

    #[test]
    fn mergetree_empty() {
        let mt = MergeTree::new(vec!["id".into()]);
        assert_eq!(mt.part_count(), 0);
        assert_eq!(mt.total_rows(), 0);
        assert!(mt.scan_all().is_empty());
    }

    // ================================================================
    // Dictionary Encoding tests
    // ================================================================

    #[test]
    fn dict_encode_basic() {
        let values = vec![
            Some("red".into()),
            Some("blue".into()),
            Some("red".into()),
            Some("green".into()),
            Some("blue".into()),
            Some("red".into()),
        ];
        let col = dict_encode(&values);
        assert_eq!(col.len(), 6);
        assert_eq!(col.cardinality(), 3); // red, blue, green
        assert_eq!(col.null_count(), 0);

        // Codes should be consistent: same value = same code
        assert_eq!(col.codes[0], col.codes[2]); // red == red
        assert_eq!(col.codes[0], col.codes[5]); // red == red
        assert_eq!(col.codes[1], col.codes[4]); // blue == blue
        assert_ne!(col.codes[0], col.codes[1]); // red != blue
        assert_ne!(col.codes[0], col.codes[3]); // red != green

        // get() should return the original values
        assert_eq!(col.get(0), Some("red"));
        assert_eq!(col.get(1), Some("blue"));
        assert_eq!(col.get(3), Some("green"));
    }

    #[test]
    fn dict_encode_with_nulls() {
        let values = vec![
            Some("active".into()),
            None,
            Some("inactive".into()),
            None,
            Some("active".into()),
        ];
        let col = dict_encode(&values);
        assert_eq!(col.len(), 5);
        assert_eq!(col.cardinality(), 2); // active, inactive
        assert_eq!(col.null_count(), 2);

        assert_eq!(col.codes[1], DICT_NULL_CODE);
        assert_eq!(col.codes[3], DICT_NULL_CODE);
        assert_eq!(col.get(0), Some("active"));
        assert_eq!(col.get(1), None);
        assert_eq!(col.get(2), Some("inactive"));
        assert_eq!(col.get(3), None);
    }

    #[test]
    fn dict_encode_all_nulls() {
        let values: Vec<Option<String>> = vec![None, None, None];
        let col = dict_encode(&values);
        assert_eq!(col.len(), 3);
        assert_eq!(col.cardinality(), 0);
        assert_eq!(col.null_count(), 3);
    }

    #[test]
    fn dict_encode_empty() {
        let values: Vec<Option<String>> = vec![];
        let col = dict_encode(&values);
        assert!(col.is_empty());
        assert_eq!(col.cardinality(), 0);
    }

    #[test]
    fn dict_decode_roundtrip() {
        let original = vec![
            Some("US".into()),
            Some("UK".into()),
            None,
            Some("US".into()),
            Some("DE".into()),
            None,
            Some("UK".into()),
        ];
        let encoded = dict_encode(&original);
        let decoded = dict_decode(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn dict_decode_single_value() {
        let values: Vec<Option<String>> =
            (0..100).map(|_| Some("constant".into())).collect();
        let encoded = dict_encode(&values);
        assert_eq!(encoded.cardinality(), 1);
        let decoded = dict_decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn dict_group_by_count_basic() {
        let values = vec![
            Some("cat".into()),
            Some("dog".into()),
            Some("cat".into()),
            Some("bird".into()),
            Some("dog".into()),
            Some("cat".into()),
            Some("dog".into()),
        ];
        let col = dict_encode(&values);
        let groups = dict_group_by_count(&col);

        // Should be sorted alphabetically
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0], ("bird".to_string(), 1));
        assert_eq!(groups[1], ("cat".to_string(), 3));
        assert_eq!(groups[2], ("dog".to_string(), 3));
    }

    #[test]
    fn dict_group_by_count_with_nulls() {
        let values = vec![
            Some("A".into()),
            None,
            Some("B".into()),
            None,
            Some("A".into()),
            None,
        ];
        let col = dict_encode(&values);
        let groups = dict_group_by_count(&col);

        // NULLs should be excluded
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], ("A".to_string(), 2));
        assert_eq!(groups[1], ("B".to_string(), 1));
    }

    #[test]
    fn dict_group_by_count_empty() {
        let col = dict_encode(&[]);
        let groups = dict_group_by_count(&col);
        assert!(groups.is_empty());
    }

    #[test]
    fn dict_group_by_sum_f64_basic() {
        let keys = vec![
            Some("X".into()),
            Some("Y".into()),
            Some("X".into()),
            Some("Y".into()),
            Some("X".into()),
        ];
        let vals = vec![
            Some(10.0),
            Some(20.0),
            Some(30.0),
            Some(40.0),
            Some(50.0),
        ];
        let key_col = dict_encode(&keys);
        let groups = dict_group_by_sum_f64(&key_col, &vals);
        assert_eq!(groups.len(), 2);
        // X: count=3, sum=90
        assert_eq!(groups[0].0, "X");
        assert_eq!(groups[0].1, 3);
        assert!((groups[0].2 - 90.0).abs() < 1e-10);
        // Y: count=2, sum=60
        assert_eq!(groups[1].0, "Y");
        assert_eq!(groups[1].1, 2);
        assert!((groups[1].2 - 60.0).abs() < 1e-10);
    }

    #[test]
    fn dict_store_auto_encoding() {
        // Build a batch with 1500 rows and a low-cardinality text column
        let statuses = ["pending", "active", "inactive", "suspended", "closed"];
        let status_vals: Vec<Option<String>> = (0..1500)
            .map(|i| Some(statuses[i % statuses.len()].to_string()))
            .collect();
        let id_vals: Vec<Option<i64>> = (0..1500).map(|i| Some(i as i64)).collect();

        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(id_vals)),
            ("status".into(), ColumnData::Text(status_vals)),
        ]);

        let mut store = ColumnarStore::new();
        store.append_with_dict("orders", batch);

        // Should have auto-encoded the "status" column
        let dict_cols = store.get_dict_columns("orders").unwrap();
        let status_dict = dict_cols.get("status").unwrap();
        assert_eq!(status_dict.cardinality(), 5);
        assert_eq!(status_dict.len(), 1500);
        assert_eq!(status_dict.null_count(), 0);

        // Original data should still be in the store
        assert_eq!(store.row_count("orders"), 1500);
    }

    #[test]
    fn dict_store_no_encoding_small_batch() {
        // Batch with < 1000 rows should not auto-encode
        let values: Vec<Option<String>> = (0..500)
            .map(|i| Some(if i % 2 == 0 { "A" } else { "B" }.to_string()))
            .collect();
        let batch = ColumnBatch::new(vec![
            ("label".into(), ColumnData::Text(values)),
        ]);

        let mut store = ColumnarStore::new();
        store.append_with_dict("small", batch);

        // Should NOT have dict columns — batch too small
        assert!(store.get_dict_columns("small").is_none());
    }

    #[test]
    fn dict_store_group_by_count_uses_dict() {
        let categories = ["electronics", "clothing", "food", "books"];
        let cat_vals: Vec<Option<String>> = (0..2000)
            .map(|i| Some(categories[i % categories.len()].to_string()))
            .collect();

        let batch = ColumnBatch::new(vec![
            ("category".into(), ColumnData::Text(cat_vals)),
        ]);

        let mut store = ColumnarStore::new();
        store.append_with_dict("products", batch);

        // Use the dict-aware group by
        let groups = store.dict_group_by_count_for("products", "category");
        assert_eq!(groups.len(), 4);
        // Each category appears 500 times (2000 / 4)
        for (_, count) in &groups {
            assert_eq!(*count, 500);
        }
        assert_eq!(groups[0].0, "books");
        assert_eq!(groups[1].0, "clothing");
        assert_eq!(groups[2].0, "electronics");
        assert_eq!(groups[3].0, "food");
    }

    #[test]
    fn dict_store_group_by_fallback() {
        // When no dict encoding exists, should fall back to hash-based counting
        let batch = ColumnBatch::new(vec![
            ("color".into(), ColumnData::Text(vec![
                Some("red".into()), Some("blue".into()), Some("red".into()),
            ])),
        ]);

        let mut store = ColumnarStore::new();
        store.append("colors", batch); // regular append, no dict

        let groups = store.dict_group_by_count_for("colors", "color");
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], ("blue".to_string(), 1));
        assert_eq!(groups[1], ("red".to_string(), 2));
    }

    #[test]
    fn dict_benchmark_100k_group_by() {
        // Insert 100K rows with 10-value cardinality, then GROUP BY
        let values_set = [
            "pending", "active", "inactive", "completed", "cancelled",
            "archived", "draft", "review", "approved", "rejected",
        ];
        let text_vals: Vec<Option<String>> = (0..100_000)
            .map(|i| Some(values_set[i % values_set.len()].to_string()))
            .collect();
        let amount_vals: Vec<Option<f64>> = (0..100_000)
            .map(|i| Some((i % 1000) as f64 * 1.5))
            .collect();

        // -- Encode
        let col = dict_encode(&text_vals);
        assert_eq!(col.len(), 100_000);
        assert_eq!(col.cardinality(), 10);

        // -- GROUP BY COUNT (dict-accelerated)
        let start = std::time::Instant::now();
        let groups = dict_group_by_count(&col);
        let dict_elapsed = start.elapsed();
        assert_eq!(groups.len(), 10);
        // Each value appears 10K times
        for (_, count) in &groups {
            assert_eq!(*count, 10_000);
        }

        // -- GROUP BY SUM (dict-accelerated)
        let start2 = std::time::Instant::now();
        let sum_groups = dict_group_by_sum_f64(&col, &amount_vals);
        let dict_sum_elapsed = start2.elapsed();
        assert_eq!(sum_groups.len(), 10);

        // -- Compare: hash-based GROUP BY
        let start3 = std::time::Instant::now();
        let mut hash_counts: HashMap<String, usize> = HashMap::new();
        for v in text_vals.iter().flatten() {
            *hash_counts.entry(v.clone()).or_insert(0) += 1;
        }
        let hash_elapsed = start3.elapsed();
        assert_eq!(hash_counts.len(), 10);

        // -- Decode roundtrip
        let decoded = dict_decode(&col);
        assert_eq!(decoded.len(), 100_000);
        assert_eq!(decoded[0], Some("pending".to_string()));
        assert_eq!(decoded[9], Some("rejected".to_string()));

        // The dict approach should work (we can't assert times in CI,
        // but we verify correctness and print times for manual inspection)
        eprintln!(
            "dict_group_by_count: {:?}, dict_group_by_sum: {:?}, hash_group_by: {:?}",
            dict_elapsed, dict_sum_elapsed, hash_elapsed
        );
    }

    #[test]
    fn dict_memory_savings() {
        // Verify dictionary encoding uses less memory than raw strings
        let values: Vec<Option<String>> = (0..10_000)
            .map(|i| Some(format!("category_{}", i % 5)))
            .collect();

        let col = dict_encode(&values);

        // Raw: 10000 strings, each ~10 bytes + String overhead (~24 bytes)
        let raw_approx = 10_000 * (10 + 24);
        // Dict: 5 strings + 10000 u32 codes
        let dict_approx = 5 * (10 + 24) + 10_000 * 4;

        assert!(
            dict_approx < raw_approx,
            "dict ({}) should be smaller than raw ({})",
            dict_approx,
            raw_approx
        );
        assert_eq!(col.cardinality(), 5);
        assert_eq!(col.len(), 10_000);
    }

    // ========================================================================
    // Parallel scan / aggregation tests
    // ========================================================================

    /// Helper: create a store with `n_batches` batches, each containing
    /// `rows_per_batch` rows of (id: Int64, val: Float64, name: Text).
    fn make_par_store(n_batches: usize, rows_per_batch: usize) -> ColumnarStore {
        let mut store = ColumnarStore::new();
        store.create_table("t");
        for batch_i in 0..n_batches {
            let base = (batch_i * rows_per_batch) as i64;
            let ids: Vec<Option<i64>> = (0..rows_per_batch)
                .map(|r| Some(base + r as i64))
                .collect();
            let vals: Vec<Option<f64>> = (0..rows_per_batch)
                .map(|r| Some((base + r as i64) as f64 * 1.5))
                .collect();
            let names: Vec<Option<String>> = (0..rows_per_batch)
                .map(|r| {
                    if r % 3 == 0 {
                        Some("alpha".to_string())
                    } else if r % 3 == 1 {
                        Some("beta".to_string())
                    } else {
                        Some("gamma".to_string())
                    }
                })
                .collect();
            let batch = ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64(ids)),
                ("val".into(), ColumnData::Float64(vals)),
                ("name".into(), ColumnData::Text(names)),
            ]);
            store.append("t", batch);
        }
        store
    }

    #[test]
    fn par_aggregate_sum_matches_sequential() {
        let store = make_par_store(8, 500);
        // Sequential sum
        let seq_sum: f64 = store
            .batches("t")
            .iter()
            .map(|b| aggregate_sum(b, "val"))
            .sum();
        // Parallel sum (8 batches >= threshold 4)
        let par = store.par_aggregate_sum("t", "val").unwrap();
        assert!(
            (par - seq_sum).abs() < 1e-6,
            "par={} seq={}",
            par,
            seq_sum
        );
    }

    #[test]
    fn par_aggregate_count_matches_sequential() {
        let store = make_par_store(6, 1000);
        let seq_count: usize = store.batches("t").iter().map(|b| b.row_count).sum();
        let par = store.par_aggregate_count("t").unwrap();
        assert_eq!(par, seq_count);
        assert_eq!(par, 6000);
    }

    #[test]
    fn par_aggregate_small_data_fallback() {
        // 2 batches — below threshold (4), should fall back to sequential
        let store = make_par_store(2, 100);
        let seq_sum: f64 = store
            .batches("t")
            .iter()
            .map(|b| aggregate_sum(b, "val"))
            .sum();
        let par = store.par_aggregate_sum("t", "val").unwrap();
        assert!(
            (par - seq_sum).abs() < 1e-6,
            "par={} seq={}",
            par,
            seq_sum
        );
        let par_count = store.par_aggregate_count("t").unwrap();
        assert_eq!(par_count, 200);
    }

    #[test]
    fn par_aggregate_min_max() {
        let store = make_par_store(8, 500);
        // Sequential reference
        let seq_min = store
            .batches("t")
            .iter()
            .filter_map(|b| batch_min_f64(b, "val"))
            .reduce(f64::min);
        let seq_max = store
            .batches("t")
            .iter()
            .filter_map(|b| batch_max_f64(b, "val"))
            .reduce(f64::max);
        let par_min = store.par_aggregate_min("t", "val");
        let par_max = store.par_aggregate_max("t", "val");
        assert_eq!(par_min, seq_min);
        assert_eq!(par_max, seq_max);
        // val = id * 1.5; id ranges from 0 to (8*500-1)=3999
        assert!((par_min.unwrap() - 0.0).abs() < 1e-6);
        assert!((par_max.unwrap() - 3999.0 * 1.5).abs() < 1e-6);
    }

    #[test]
    fn par_scan_where_eq_matches_sequential() {
        let store = make_par_store(6, 500);
        // Sequential filtered scan
        let seq_results: Vec<ColumnBatch> = store
            .batches("t")
            .iter()
            .map(|b| scan_batch_eq(b, "name", &AggValue::Text("alpha".into())))
            .filter(|b| b.row_count > 0)
            .collect();
        let seq_total: usize = seq_results.iter().map(|b| b.row_count).sum();
        // Parallel filtered scan
        let par_results = store.par_scan_where_eq("t", "name", &AggValue::Text("alpha".into()));
        let par_total: usize = par_results.iter().map(|b| b.row_count).sum();
        assert_eq!(par_total, seq_total);
        // Every 3rd row is "alpha" — 500 rows per batch, 167 per batch (ceil(500/3))
        // 6 batches * 167 = 1002
        // Actually: 0,3,6,...,498 => 167 values per batch => 167 * 6 = 1002
        assert_eq!(par_total, 167 * 6);
    }

    #[test]
    fn par_scan_where_eq_int64() {
        let store = make_par_store(5, 100);
        // id=42 should appear exactly once (in batch 0, row 42)
        let results = store.par_scan_where_eq("t", "id", &AggValue::Int64(42));
        let total: usize = results.iter().map(|b| b.row_count).sum();
        assert_eq!(total, 1);
    }

    #[test]
    fn par_many_batches_large_dataset() {
        // 10 batches x 1000 rows = 10,000 rows — enough to exercise parallelism
        let store = make_par_store(10, 1000);
        let par_count = store.par_aggregate_count("t").unwrap();
        assert_eq!(par_count, 10_000);
        let par_sum = store.par_aggregate_sum("t", "id").unwrap();
        // Sum of 0..9999 = 9999*10000/2 = 49995000
        let expected_sum: f64 = (0..10_000i64).sum::<i64>() as f64;
        assert!(
            (par_sum - expected_sum).abs() < 1e-6,
            "par={} expected={}",
            par_sum,
            expected_sum
        );
    }

    #[test]
    fn par_scan_all_returns_all_batches() {
        let store = make_par_store(6, 200);
        let all = store.par_scan_all("t");
        assert_eq!(all.len(), 6);
        let total: usize = all.iter().map(|b| b.row_count).sum();
        assert_eq!(total, 1200);
    }

    #[test]
    fn par_aggregate_nonexistent_table() {
        let store = ColumnarStore::new();
        assert!(store.par_aggregate_sum("nope", "x").is_none());
        assert!(store.par_aggregate_count("nope").is_none());
        assert!(store.par_aggregate_min("nope", "x").is_none());
        assert!(store.par_aggregate_max("nope", "x").is_none());
    }

    #[test]
    fn par_aggregate_empty_table() {
        let mut store = ColumnarStore::new();
        store.create_table("empty");
        assert_eq!(store.par_aggregate_sum("empty", "x").unwrap(), 0.0);
        assert_eq!(store.par_aggregate_count("empty").unwrap(), 0);
        assert!(store.par_aggregate_min("empty", "x").is_none());
        assert!(store.par_aggregate_max("empty", "x").is_none());
    }

    #[test]
    fn par_free_functions_with_custom_threshold() {
        let store = make_par_store(6, 100);
        let batches = store.batches("t");
        // Force parallelism with threshold=2
        let sum = par_sum_batches(batches, "val", 2);
        let seq_sum: f64 = batches.iter().map(|b| aggregate_sum(b, "val")).sum();
        assert!((sum - seq_sum).abs() < 1e-6);
        // Force single-threaded with threshold=100
        let sum_st = par_sum_batches(batches, "val", 100);
        assert!((sum_st - seq_sum).abs() < 1e-6);
    }

    // ================================================================
    // MergeTree-backed ColumnarStore integration tests
    // ================================================================

    #[test]
    fn mergetree_store_create_and_insert() {
        let mut store = ColumnarStore::new();
        store.create_merge_tree_table("events", vec!["timestamp".into(), "metric".into()]);

        assert!(store.table_exists("events"));
        assert!(store.is_merge_tree("events"));

        let batch = ColumnBatch::new(vec![
            ("timestamp".into(), ColumnData::Int64(vec![Some(300), Some(100), Some(200)])),
            ("metric".into(), ColumnData::Text(vec![
                Some("cpu".into()), Some("mem".into()), Some("cpu".into()),
            ])),
            ("value".into(), ColumnData::Float64(vec![Some(0.9), Some(0.5), Some(0.7)])),
        ]);
        store.append("events", batch);

        assert_eq!(store.row_count("events"), 3);

        // Data should be sorted by (timestamp, metric) via MergeTree
        let batches = store.batches_all("events");
        assert_eq!(batches.len(), 1);
        if let Some(ColumnData::Int64(ts)) = batches[0].column("timestamp") {
            assert_eq!(ts, &[Some(100), Some(200), Some(300)]);
        } else {
            panic!("expected Int64 timestamp column");
        }
    }

    #[test]
    fn mergetree_store_scan_with_pruning() {
        let mut store = ColumnarStore::new();
        store.create_merge_tree_table("events", vec!["id".into()]);

        // Insert two separate batches (two parts)
        let batch1 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((1..=10).map(Some).collect())),
            ("val".into(), ColumnData::Float64((1..=10).map(|i| Some(i as f64)).collect())),
        ]);
        store.append("events", batch1);

        let batch2 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((100..=110).map(Some).collect())),
            ("val".into(), ColumnData::Float64((100..=110).map(|i| Some(i as f64)).collect())),
        ]);
        store.append("events", batch2);

        // Scan with predicate that should prune part 1
        let pruned = store.scan_merge_tree("events", "id", CmpOp::Gt, &ScalarValue::Int64(50));
        assert_eq!(pruned.len(), 1); // Only part 2 should survive

        // Full scan should return both parts
        let all = store.batches_all("events");
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn mergetree_store_multicolumn_sort() {
        let mut store = ColumnarStore::new();
        store.create_merge_tree_table("logs", vec!["level".into(), "timestamp".into()]);

        let batch = ColumnBatch::new(vec![
            ("level".into(), ColumnData::Text(vec![
                Some("error".into()), Some("warn".into()), Some("error".into()),
                Some("info".into()), Some("warn".into()),
            ])),
            ("timestamp".into(), ColumnData::Int64(vec![
                Some(300), Some(200), Some(100), Some(400), Some(100),
            ])),
            ("msg".into(), ColumnData::Text(vec![
                Some("e1".into()), Some("w1".into()), Some("e2".into()),
                Some("i1".into()), Some("w2".into()),
            ])),
        ]);
        store.append("logs", batch);

        let batches = store.batches_all("logs");
        assert_eq!(batches.len(), 1);

        // Should be sorted first by level (text), then by timestamp (int64)
        if let Some(ColumnData::Text(levels)) = batches[0].column("level") {
            assert_eq!(levels[0].as_deref(), Some("error"));
            assert_eq!(levels[1].as_deref(), Some("error"));
            assert_eq!(levels[2].as_deref(), Some("info"));
            assert_eq!(levels[3].as_deref(), Some("warn"));
            assert_eq!(levels[4].as_deref(), Some("warn"));
        }
        if let Some(ColumnData::Int64(ts)) = batches[0].column("timestamp") {
            // Within "error" group: 100 < 300
            assert_eq!(ts[0], Some(100));
            assert_eq!(ts[1], Some(300));
            // Within "warn" group: 100 < 200
            assert_eq!(ts[3], Some(100));
            assert_eq!(ts[4], Some(200));
        }
    }

    #[test]
    fn non_mergetree_table_still_works() {
        let mut store = ColumnarStore::new();
        store.create_table("raw");

        assert!(store.table_exists("raw"));
        assert!(!store.is_merge_tree("raw"));

        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(3), Some(1), Some(2)])),
        ]);
        store.append("raw", batch);

        // Raw tables don't sort
        let batches = store.batches("raw");
        assert_eq!(batches.len(), 1);
        if let Some(ColumnData::Int64(ids)) = batches[0].column("id") {
            assert_eq!(ids, &[Some(3), Some(1), Some(2)]); // insertion order
        }
        assert_eq!(store.row_count("raw"), 3);
    }

    #[test]
    fn mergetree_store_drop_and_clear() {
        let mut store = ColumnarStore::new();
        store.create_merge_tree_table("t", vec!["id".into()]);

        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
        ]);
        store.append("t", batch);
        assert_eq!(store.row_count("t"), 2);

        // Clear should reset the MergeTree
        store.clear("t");
        assert_eq!(store.row_count("t"), 0);
        assert!(store.is_merge_tree("t")); // still a MergeTree

        // Re-insert after clear
        let batch2 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(10)])),
        ]);
        store.append("t", batch2);
        assert_eq!(store.row_count("t"), 1);

        // Drop should remove MergeTree
        store.drop_table("t");
        assert!(!store.table_exists("t"));
        assert!(!store.is_merge_tree("t"));
    }

    #[test]
    fn mergetree_store_wal_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Create and populate
        {
            let mut store = ColumnarStore::open(dir_path).unwrap();
            store.create_merge_tree_table("events", vec!["ts".into()]);

            let batch = ColumnBatch::new(vec![
                ("ts".into(), ColumnData::Int64(vec![Some(300), Some(100), Some(200)])),
                ("val".into(), ColumnData::Float64(vec![Some(3.0), Some(1.0), Some(2.0)])),
            ]);
            store.append("events", batch);
            assert_eq!(store.row_count("events"), 3);
        }

        // Re-open and verify WAL replayed the data
        {
            let store = ColumnarStore::open(dir_path).unwrap();
            // The WAL replays into raw tables (MergeTree metadata is not persisted
            // in the WAL yet), but the data should be recoverable.
            assert!(store.table_exists("events"));
            assert_eq!(store.row_count("events"), 3);
        }
    }

    #[test]
    fn mergetree_store_batches_all_for_aggregation() {
        let mut store = ColumnarStore::new();
        store.create_merge_tree_table("metrics", vec!["ts".into()]);

        let batch = ColumnBatch::new(vec![
            ("ts".into(), ColumnData::Int64(vec![Some(1), Some(2), Some(3)])),
            ("value".into(), ColumnData::Float64(vec![Some(10.0), Some(20.0), Some(30.0)])),
        ]);
        store.append("metrics", batch);

        // batches_all should work for MergeTree tables
        let all = store.batches_all("metrics");
        let mut total = 0.0f64;
        for b in &all {
            total += aggregate_sum(b, "value");
        }
        assert!((total - 60.0).abs() < 1e-10);
    }

    #[test]
    fn mergetree_multicolumn_sort_with_int_columns() {
        let mut mt = MergeTree::new(vec!["a".into(), "b".into()]);
        let batch = ColumnBatch::new(vec![
            ("a".into(), ColumnData::Int64(vec![Some(2), Some(1), Some(1), Some(2)])),
            ("b".into(), ColumnData::Int64(vec![Some(20), Some(10), Some(20), Some(10)])),
            ("c".into(), ColumnData::Text(vec![
                Some("d".into()), Some("a".into()), Some("b".into()), Some("c".into()),
            ])),
        ]);
        mt.insert(batch);

        let parts = mt.scan_all();
        assert_eq!(parts.len(), 1);
        if let Some(ColumnData::Int64(a_vals)) = parts[0].column("a") {
            assert_eq!(a_vals, &[Some(1), Some(1), Some(2), Some(2)]);
        }
        if let Some(ColumnData::Int64(b_vals)) = parts[0].column("b") {
            assert_eq!(b_vals, &[Some(10), Some(20), Some(10), Some(20)]);
        }
        if let Some(ColumnData::Text(c_vals)) = parts[0].column("c") {
            assert_eq!(c_vals, &[Some("a".into()), Some("b".into()), Some("c".into()), Some("d".into())]);
        }
    }

    // ================================================================
    // Background Merge tests
    // ================================================================

    #[test]
    fn mergetree_sync_merge_inline() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 3;

        for i in 0..5 {
            let batch = ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64(vec![Some(i)])),
            ]);
            mt.insert(batch);
        }

        assert!(mt.part_count() <= 3);
        assert_eq!(mt.total_rows(), 5);
    }

    #[test]
    fn mergetree_execute_merge_task_produces_correct_result() {
        let task = MergeTask {
            table: "test".into(),
            parts: vec![
                MergeTreePart {
                    id: 1,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
                    ])),
                    compressed: None,
                },
                MergeTreePart {
                    id: 2,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
                    ])),
                    compressed: None,
                },
            ],
            source_part_ids: vec![1, 2],
            primary_key: vec!["id".into()],
            merge_strategy: MergeStrategy::Default,
        };

        let result = execute_merge_task(task, 100);

        assert_eq!(result.table, "test");
        assert_eq!(result.source_part_ids, vec![1, 2]);
        assert_eq!(result.merged_part.id, 100);
        assert_eq!(result.merged_part.row_count, 4);

        if let Some(ColumnData::Int64(ids)) = result.merged_part.data.column("id") {
            assert_eq!(ids, &[Some(1), Some(2), Some(3), Some(4)]);
        } else {
            panic!("expected Int64 id column in merged result");
        }

        assert!(result.merged_part.compressed.is_some());
    }

    #[test]
    fn mergetree_apply_merge_result() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 100;

        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
        ]));
        assert_eq!(mt.part_count(), 2);

        let part_id_a = mt.parts[0].id;
        let part_id_b = mt.parts[1].id;

        let merged_batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(2), Some(3), Some(4)])),
        ]);
        let result = MergeResult {
            table: "test".into(),
            source_part_ids: vec![part_id_a, part_id_b],
            merged_part: MergeTreePart {
                id: 999,
                data: merged_batch.clone(),
                row_count: 4,
                zone_map: ZoneMap::from_batch(&merged_batch),
                compressed: None,
            },
        };

        let applied = mt.apply_merge_result(result);
        assert!(applied);
        assert_eq!(mt.part_count(), 1);
        assert_eq!(mt.total_rows(), 4);

        let batches = mt.scan_all();
        if let Some(ColumnData::Int64(ids)) = batches[0].column("id") {
            assert_eq!(ids, &[Some(1), Some(2), Some(3), Some(4)]);
        }
    }

    #[test]
    fn mergetree_apply_stale_merge_result() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 100;

        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
        ]));
        assert_eq!(mt.part_count(), 1);

        let merged_batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
        ]);
        let result = MergeResult {
            table: "test".into(),
            source_part_ids: vec![999, 1000],
            merged_part: MergeTreePart {
                id: 50,
                data: merged_batch.clone(),
                row_count: 2,
                zone_map: ZoneMap::from_batch(&merged_batch),
                compressed: None,
            },
        };

        let applied = mt.apply_merge_result(result);
        assert!(!applied, "stale merge result should not be applied");
        assert_eq!(mt.part_count(), 1);
    }

    #[test]
    fn mergetree_background_merge_end_to_end() {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<MergeTask>();
        let (result_tx, result_rx) = std::sync::mpsc::channel::<MergeResult>();
        let running = Arc::new(AtomicBool::new(true));
        let next_id = Arc::new(AtomicU64::new(1000));

        let worker = spawn_merge_worker(task_rx, result_tx, Arc::clone(&running), next_id);

        // Build a merge task manually
        let task = MergeTask {
            table: "bg_test".into(),
            parts: vec![
                MergeTreePart {
                    id: 1,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
                    ])),
                    compressed: None,
                },
                MergeTreePart {
                    id: 2,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
                    ])),
                    compressed: None,
                },
            ],
            source_part_ids: vec![1, 2],
            primary_key: vec!["id".into()],
            merge_strategy: MergeStrategy::Default,
        };
        task_tx.send(task).unwrap();

        // Wait for worker to process
        let result = result_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert_eq!(result.merged_part.row_count, 4);

        // Apply to a MergeTree
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 100;
        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
        ]));
        let applied = mt.apply_merge_result(result);
        assert!(applied);
        assert_eq!(mt.part_count(), 1);
        assert_eq!(mt.total_rows(), 4);

        running.store(false, AtomicOrdering::SeqCst);
        let _ = worker.join();
    }

    #[test]
    fn mergetree_snapshot_reads_during_merge() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 100;

        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(3), Some(4)])),
        ]));

        let snapshot_before = mt.scan_all();
        let mut ids_before: Vec<i64> = Vec::new();
        for batch in &snapshot_before {
            if let Some(ColumnData::Int64(ids)) = batch.column("id") {
                for v in ids {
                    if let Some(val) = v {
                        ids_before.push(*val);
                    }
                }
            }
        }
        ids_before.sort();
        assert_eq!(ids_before, vec![1, 2, 3, 4]);

        mt.optimize();

        let snapshot_after = mt.scan_all();
        let mut ids_after: Vec<i64> = Vec::new();
        for batch in &snapshot_after {
            if let Some(ColumnData::Int64(ids)) = batch.column("id") {
                for v in ids {
                    if let Some(val) = v {
                        ids_after.push(*val);
                    }
                }
            }
        }
        ids_after.sort();

        assert_eq!(ids_before, ids_after);
        assert_eq!(mt.part_count(), 1);
    }

    #[test]
    fn mergetree_sync_merge_preserves_data() {
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 3;

        for i in 0..5 {
            let batch = ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64(vec![Some(i)])),
            ]);
            mt.insert(batch);
        }

        assert!(mt.part_count() <= 3);
        assert_eq!(mt.total_rows(), 5);

        let batches = mt.scan_all();
        let mut all_ids: Vec<i64> = Vec::new();
        for batch in &batches {
            if let Some(ColumnData::Int64(ids)) = batch.column("id") {
                for v in ids.iter().flatten() {
                    all_ids.push(*v);
                }
            }
        }
        all_ids.sort();
        assert_eq!(all_ids, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn mergetree_merge_result_preserves_sort_order() {
        let task = MergeTask {
            table: "sort_test".into(),
            parts: vec![
                MergeTreePart {
                    id: 1,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(5), Some(10)])),
                        ("name".into(), ColumnData::Text(vec![
                            Some("e".into()), Some("j".into()),
                        ])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(5), Some(10)])),
                    ])),
                    compressed: None,
                },
                MergeTreePart {
                    id: 2,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(7)])),
                        ("name".into(), ColumnData::Text(vec![
                            Some("a".into()), Some("g".into()),
                        ])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(7)])),
                    ])),
                    compressed: None,
                },
            ],
            source_part_ids: vec![1, 2],
            primary_key: vec!["id".into()],
            merge_strategy: MergeStrategy::Default,
        };

        let result = execute_merge_task(task, 42);

        if let Some(ColumnData::Int64(ids)) = result.merged_part.data.column("id") {
            assert_eq!(ids, &[Some(1), Some(5), Some(7), Some(10)]);
        } else {
            panic!("expected Int64 id column");
        }

        if let Some(ColumnData::Text(names)) = result.merged_part.data.column("name") {
            assert_eq!(names, &[
                Some("a".into()), Some("e".into()),
                Some("g".into()), Some("j".into()),
            ]);
        } else {
            panic!("expected Text name column");
        }
    }

    #[test]
    fn mergetree_apply_result_via_worker() {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<MergeTask>();
        let (result_tx, result_rx) = std::sync::mpsc::channel::<MergeResult>();
        let running = Arc::new(AtomicBool::new(true));
        let next_id = Arc::new(AtomicU64::new(1000));
        let worker = spawn_merge_worker(task_rx, result_tx, Arc::clone(&running), next_id);

        // Send a merge task directly to the worker
        let task = MergeTask {
            table: "test".into(),
            parts: vec![
                MergeTreePart {
                    id: 1,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
                    ])),
                    compressed: None,
                },
                MergeTreePart {
                    id: 2,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
                    ])),
                    compressed: None,
                },
            ],
            source_part_ids: vec![1, 2],
            primary_key: vec!["id".into()],
            merge_strategy: MergeStrategy::Default,
        };
        task_tx.send(task).unwrap();

        let result = result_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();

        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 100;
        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(2), Some(4)])),
        ]));
        let applied = mt.apply_merge_result(result);
        assert!(applied);
        assert_eq!(mt.total_rows(), 4);

        running.store(false, AtomicOrdering::SeqCst);
        let _ = worker.join();
    }

    #[test]
    fn mergetree_bg_merger_queues_and_polls() {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<MergeTask>();
        let (result_tx, result_rx) = std::sync::mpsc::channel::<MergeResult>();
        let running = Arc::new(AtomicBool::new(true));
        let next_id = Arc::new(AtomicU64::new(1000));
        let worker = spawn_merge_worker(task_rx, result_tx, Arc::clone(&running), next_id);

        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 3;
        mt.set_table_name("qp_test");
        mt.set_background_merger(task_tx);
        assert!(mt.has_background_merger());

        for i in 0..6i64 {
            mt.insert(ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64(vec![Some(i)])),
            ]));
        }
        assert!(mt.part_count() > 3);
        assert_eq!(mt.total_rows(), 6);

        std::thread::sleep(std::time::Duration::from_millis(300));
        mt.poll_merge_results(&result_rx);
        assert!(mt.part_count() < 6);
        assert_eq!(mt.total_rows(), 6);

        let batches = mt.scan_all();
        let mut all: Vec<i64> = batches.iter().flat_map(|b| {
            match b.column("id") {
                Some(ColumnData::Int64(v)) => v.iter().flatten().copied().collect::<Vec<_>>(),
                _ => vec![],
            }
        }).collect();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3, 4, 5]);

        running.store(false, AtomicOrdering::SeqCst);
        let _ = worker.join();
    }

    #[test]
    fn mergetree_clear_bg_merger_reverts_to_sync() {
        let (task_tx, _rx) = std::sync::mpsc::channel::<MergeTask>();
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.max_parts = 3;
        mt.set_background_merger(task_tx);
        assert!(mt.has_background_merger());
        mt.clear_background_merger();
        assert!(!mt.has_background_merger());

        for i in 0..5i64 {
            mt.insert(ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64(vec![Some(i)])),
            ]));
        }
        assert!(mt.part_count() <= 3);
        assert_eq!(mt.total_rows(), 5);
    }

    // ================================================================
    // ReplacingMergeTree tests
    // ================================================================

    #[test]
    fn replacing_mergetree_dedup_with_version_column() {
        let strategy = MergeStrategy::Replacing { version_column: Some("version".into()) };
        let mut mt = MergeTree::new_with_strategy(vec!["id".into()], strategy);
        mt.max_parts = 2; // trigger merge quickly

        // Insert duplicate PKs with different versions
        let batch1 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
            ("version".into(), ColumnData::Int64(vec![Some(1), Some(1)])),
            ("data".into(), ColumnData::Text(vec![Some("old_1".into()), Some("old_2".into())])),
        ]);
        mt.insert(batch1);

        let batch2 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
            ("version".into(), ColumnData::Int64(vec![Some(3), Some(2)])),
            ("data".into(), ColumnData::Text(vec![Some("new_1".into()), Some("new_2".into())])),
        ]);
        mt.insert(batch2);

        // Before merge: should have 4 rows across 2 parts (but merge auto-triggers)
        // After merge: only 2 rows, one per PK, with highest version
        mt.optimize();

        let batches = mt.scan_all();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        if let Some(ColumnData::Int64(ids)) = batch.column("id") {
            assert_eq!(ids, &[Some(1), Some(2)]);
        } else {
            panic!("expected Int64 id column");
        }
        if let Some(ColumnData::Int64(versions)) = batch.column("version") {
            assert_eq!(versions, &[Some(3), Some(2)]);
        } else {
            panic!("expected Int64 version column");
        }
        if let Some(ColumnData::Text(data)) = batch.column("data") {
            assert_eq!(data, &[Some("new_1".into()), Some("new_2".into())]);
        } else {
            panic!("expected Text data column");
        }
    }

    #[test]
    fn replacing_mergetree_dedup_without_version_column() {
        let strategy = MergeStrategy::Replacing { version_column: None };
        let mut mt = MergeTree::new_with_strategy(vec!["id".into()], strategy);
        mt.max_parts = 100; // prevent auto-merge

        // Insert duplicate PKs without version — last row wins
        let batch1 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
            ("data".into(), ColumnData::Text(vec![Some("first_1".into()), Some("first_2".into())])),
        ]);
        mt.insert(batch1);

        let batch2 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
            ("data".into(), ColumnData::Text(vec![Some("second_1".into()), Some("second_3".into())])),
        ]);
        mt.insert(batch2);

        // Before merge: 4 rows
        assert_eq!(mt.total_rows(), 4);

        // Merge
        mt.optimize();

        // After merge: 3 rows (id=1 deduped, id=2 unique, id=3 unique)
        let batches = mt.scan_all();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.row_count, 3);

        if let Some(ColumnData::Int64(ids)) = batch.column("id") {
            assert_eq!(ids, &[Some(1), Some(2), Some(3)]);
        }
        // For id=1, last row wins (from batch2)
        if let Some(ColumnData::Text(data)) = batch.column("data") {
            assert_eq!(data[0], Some("second_1".into()));
        }
    }

    #[test]
    fn replacing_mergetree_scan_before_and_after_merge() {
        let strategy = MergeStrategy::Replacing { version_column: Some("ver".into()) };
        let mut mt = MergeTree::new_with_strategy(vec!["id".into()], strategy);
        mt.max_parts = 100; // prevent auto-merge

        let batch1 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(10)])),
            ("ver".into(), ColumnData::Int64(vec![Some(1)])),
            ("val".into(), ColumnData::Float64(vec![Some(100.0)])),
        ]);
        mt.insert(batch1);

        let batch2 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(10)])),
            ("ver".into(), ColumnData::Int64(vec![Some(5)])),
            ("val".into(), ColumnData::Float64(vec![Some(999.0)])),
        ]);
        mt.insert(batch2);

        // Before merge: 2 rows across 2 parts
        assert_eq!(mt.total_rows(), 2);
        let pre_merge = mt.scan_all();
        assert_eq!(pre_merge.len(), 2);

        // After merge: 1 row
        mt.optimize();
        let post_merge = mt.scan_all();
        assert_eq!(post_merge.len(), 1);
        assert_eq!(post_merge[0].row_count, 1);

        if let Some(ColumnData::Float64(vals)) = post_merge[0].column("val") {
            assert_eq!(vals, &[Some(999.0)]); // version 5 wins
        }
    }

    // ================================================================
    // AggregatingMergeTree tests
    // ================================================================

    #[test]
    fn aggregating_mergetree_sum_columns() {
        let strategy = MergeStrategy::Aggregating {
            group_columns: vec!["session_id".into()],
            sum_columns: vec!["page_views".into(), "duration".into()],
            count_columns: vec![],
        };
        let mut mt = MergeTree::new_with_strategy(vec!["session_id".into()], strategy);
        mt.max_parts = 100;

        // Insert partial aggregates
        let batch1 = ColumnBatch::new(vec![
            ("session_id".into(), ColumnData::Text(vec![Some("s1".into()), Some("s2".into())])),
            ("page_views".into(), ColumnData::Int64(vec![Some(3), Some(5)])),
            ("duration".into(), ColumnData::Int64(vec![Some(100), Some(200)])),
        ]);
        mt.insert(batch1);

        let batch2 = ColumnBatch::new(vec![
            ("session_id".into(), ColumnData::Text(vec![Some("s1".into()), Some("s2".into())])),
            ("page_views".into(), ColumnData::Int64(vec![Some(7), Some(2)])),
            ("duration".into(), ColumnData::Int64(vec![Some(50), Some(300)])),
        ]);
        mt.insert(batch2);

        // Merge
        mt.optimize();

        let batches = mt.scan_all();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.row_count, 2);

        if let Some(ColumnData::Text(ids)) = batch.column("session_id") {
            assert_eq!(ids, &[Some("s1".into()), Some("s2".into())]);
        }
        if let Some(ColumnData::Int64(pv)) = batch.column("page_views") {
            assert_eq!(pv, &[Some(10), Some(7)]); // 3+7=10, 5+2=7
        }
        if let Some(ColumnData::Int64(dur)) = batch.column("duration") {
            assert_eq!(dur, &[Some(150), Some(500)]); // 100+50=150, 200+300=500
        }
    }

    #[test]
    fn aggregating_mergetree_count_columns() {
        let strategy = MergeStrategy::Aggregating {
            group_columns: vec!["url".into()],
            sum_columns: vec![],
            count_columns: vec!["hits".into()],
        };
        let mut mt = MergeTree::new_with_strategy(vec!["url".into()], strategy);
        mt.max_parts = 100;

        let batch1 = ColumnBatch::new(vec![
            ("url".into(), ColumnData::Text(vec![Some("/home".into())])),
            ("hits".into(), ColumnData::Int64(vec![Some(10)])),
        ]);
        mt.insert(batch1);

        let batch2 = ColumnBatch::new(vec![
            ("url".into(), ColumnData::Text(vec![Some("/home".into())])),
            ("hits".into(), ColumnData::Int64(vec![Some(25)])),
        ]);
        mt.insert(batch2);

        let batch3 = ColumnBatch::new(vec![
            ("url".into(), ColumnData::Text(vec![Some("/home".into())])),
            ("hits".into(), ColumnData::Int64(vec![Some(5)])),
        ]);
        mt.insert(batch3);

        mt.optimize();

        let batches = mt.scan_all();
        assert_eq!(batches.len(), 1);
        if let Some(ColumnData::Int64(hits)) = batches[0].column("hits") {
            assert_eq!(hits, &[Some(40)]); // 10+25+5
        }
    }

    #[test]
    fn aggregating_mergetree_multiple_merges_cumulative() {
        let strategy = MergeStrategy::Aggregating {
            group_columns: vec!["key".into()],
            sum_columns: vec!["total".into()],
            count_columns: vec![],
        };
        let mut mt = MergeTree::new_with_strategy(vec!["key".into()], strategy);
        mt.max_parts = 100;

        // First round of inserts
        mt.insert(ColumnBatch::new(vec![
            ("key".into(), ColumnData::Int64(vec![Some(1)])),
            ("total".into(), ColumnData::Int64(vec![Some(10)])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("key".into(), ColumnData::Int64(vec![Some(1)])),
            ("total".into(), ColumnData::Int64(vec![Some(20)])),
        ]));
        mt.optimize(); // First merge: total = 30

        // Second round
        mt.insert(ColumnBatch::new(vec![
            ("key".into(), ColumnData::Int64(vec![Some(1)])),
            ("total".into(), ColumnData::Int64(vec![Some(50)])),
        ]));
        mt.optimize(); // Second merge: total = 30 + 50 = 80

        let batches = mt.scan_all();
        assert_eq!(batches.len(), 1);
        if let Some(ColumnData::Int64(totals)) = batches[0].column("total") {
            assert_eq!(totals, &[Some(80)]);
        }
    }

    #[test]
    fn aggregating_mergetree_non_aggregate_columns_keep_last() {
        let strategy = MergeStrategy::Aggregating {
            group_columns: vec!["id".into()],
            sum_columns: vec!["amount".into()],
            count_columns: vec![],
        };
        let mut mt = MergeTree::new_with_strategy(vec!["id".into()], strategy);
        mt.max_parts = 100;

        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
            ("amount".into(), ColumnData::Int64(vec![Some(100)])),
            ("label".into(), ColumnData::Text(vec![Some("first".into())])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
            ("amount".into(), ColumnData::Int64(vec![Some(200)])),
            ("label".into(), ColumnData::Text(vec![Some("latest".into())])),
        ]));
        mt.optimize();

        let batches = mt.scan_all();
        assert_eq!(batches.len(), 1);
        if let Some(ColumnData::Int64(amounts)) = batches[0].column("amount") {
            assert_eq!(amounts, &[Some(300)]); // 100 + 200
        }
        if let Some(ColumnData::Text(labels)) = batches[0].column("label") {
            assert_eq!(labels, &[Some("latest".into())]); // last value
        }
    }

    #[test]
    fn aggregating_mergetree_float64_sum() {
        let strategy = MergeStrategy::Aggregating {
            group_columns: vec!["sensor".into()],
            sum_columns: vec!["reading".into()],
            count_columns: vec![],
        };
        let mut mt = MergeTree::new_with_strategy(vec!["sensor".into()], strategy);
        mt.max_parts = 100;

        mt.insert(ColumnBatch::new(vec![
            ("sensor".into(), ColumnData::Text(vec![Some("temp".into())])),
            ("reading".into(), ColumnData::Float64(vec![Some(23.5)])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("sensor".into(), ColumnData::Text(vec![Some("temp".into())])),
            ("reading".into(), ColumnData::Float64(vec![Some(24.5)])),
        ]));
        mt.optimize();

        let batches = mt.scan_all();
        if let Some(ColumnData::Float64(readings)) = batches[0].column("reading") {
            assert!((readings[0].unwrap() - 48.0).abs() < 1e-9);
        }
    }

    // ================================================================
    // ColumnarStore integration tests for new strategies
    // ================================================================

    #[test]
    fn store_create_replacing_mergetree() {
        let mut store = ColumnarStore::new();
        store.create_merge_tree_table_with_strategy(
            "events",
            vec!["id".into()],
            MergeStrategy::Replacing { version_column: Some("ver".into()) },
        );

        assert!(store.table_exists("events"));
        assert!(store.is_merge_tree("events"));

        store.append("events", ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
            ("ver".into(), ColumnData::Int64(vec![Some(1)])),
            ("data".into(), ColumnData::Text(vec![Some("old".into())])),
        ]));
        store.append("events", ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
            ("ver".into(), ColumnData::Int64(vec![Some(2)])),
            ("data".into(), ColumnData::Text(vec![Some("new".into())])),
        ]));

        // Optimize via MergeTree
        store.get_merge_tree_mut("events").unwrap().optimize();

        let batches = store.batches_all("events");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].row_count, 1);
        if let Some(ColumnData::Text(d)) = batches[0].column("data") {
            assert_eq!(d, &[Some("new".into())]);
        }
    }

    #[test]
    fn store_create_aggregating_mergetree() {
        let mut store = ColumnarStore::new();
        store.create_merge_tree_table_with_strategy(
            "stats",
            vec!["page".into()],
            MergeStrategy::Aggregating {
                group_columns: vec!["page".into()],
                sum_columns: vec!["views".into()],
                count_columns: vec!["visits".into()],
            },
        );

        store.append("stats", ColumnBatch::new(vec![
            ("page".into(), ColumnData::Text(vec![Some("/home".into())])),
            ("views".into(), ColumnData::Int64(vec![Some(100)])),
            ("visits".into(), ColumnData::Int64(vec![Some(50)])),
        ]));
        store.append("stats", ColumnBatch::new(vec![
            ("page".into(), ColumnData::Text(vec![Some("/home".into())])),
            ("views".into(), ColumnData::Int64(vec![Some(200)])),
            ("visits".into(), ColumnData::Int64(vec![Some(75)])),
        ]));

        store.get_merge_tree_mut("stats").unwrap().optimize();

        let batches = store.batches_all("stats");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].row_count, 1);
        if let Some(ColumnData::Int64(views)) = batches[0].column("views") {
            assert_eq!(views, &[Some(300)]);
        }
        if let Some(ColumnData::Int64(visits)) = batches[0].column("visits") {
            assert_eq!(visits, &[Some(125)]);
        }
    }

    #[test]
    fn store_clear_preserves_strategy() {
        let mut store = ColumnarStore::new();
        store.create_merge_tree_table_with_strategy(
            "t",
            vec!["id".into()],
            MergeStrategy::Replacing { version_column: Some("ver".into()) },
        );
        store.append("t", ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
            ("ver".into(), ColumnData::Int64(vec![Some(1)])),
        ]));
        assert_eq!(store.row_count("t"), 1);

        store.clear("t");
        assert_eq!(store.row_count("t"), 0);

        // Re-insert and verify strategy still works
        store.append("t", ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
            ("ver".into(), ColumnData::Int64(vec![Some(1)])),
        ]));
        store.append("t", ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
            ("ver".into(), ColumnData::Int64(vec![Some(2)])),
        ]));
        store.get_merge_tree_mut("t").unwrap().optimize();
        assert_eq!(store.row_count("t"), 1); // deduped
    }

    #[test]
    fn replacing_mergetree_execute_merge_task() {
        let task = MergeTask {
            table: "rmt_test".into(),
            parts: vec![
                MergeTreePart {
                    id: 1,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
                        ("ver".into(), ColumnData::Int64(vec![Some(1), Some(1)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
                    ])),
                    compressed: None,
                },
                MergeTreePart {
                    id: 2,
                    data: ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
                        ("ver".into(), ColumnData::Int64(vec![Some(5), Some(1)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("id".into(), ColumnData::Int64(vec![Some(1), Some(3)])),
                    ])),
                    compressed: None,
                },
            ],
            source_part_ids: vec![1, 2],
            primary_key: vec!["id".into()],
            merge_strategy: MergeStrategy::Replacing { version_column: Some("ver".into()) },
        };

        let result = execute_merge_task(task, 99);
        assert_eq!(result.merged_part.row_count, 3); // id=1 (deduped), id=2, id=3

        if let Some(ColumnData::Int64(ids)) = result.merged_part.data.column("id") {
            assert_eq!(ids, &[Some(1), Some(2), Some(3)]);
        }
        if let Some(ColumnData::Int64(vers)) = result.merged_part.data.column("ver") {
            assert_eq!(vers[0], Some(5)); // highest version for id=1
        }
    }

    #[test]
    fn aggregating_mergetree_execute_merge_task() {
        let task = MergeTask {
            table: "amt_test".into(),
            parts: vec![
                MergeTreePart {
                    id: 1,
                    data: ColumnBatch::new(vec![
                        ("key".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
                        ("total".into(), ColumnData::Int64(vec![Some(10), Some(20)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("key".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
                    ])),
                    compressed: None,
                },
                MergeTreePart {
                    id: 2,
                    data: ColumnBatch::new(vec![
                        ("key".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
                        ("total".into(), ColumnData::Int64(vec![Some(30), Some(40)])),
                    ]),
                    row_count: 2,
                    zone_map: ZoneMap::from_batch(&ColumnBatch::new(vec![
                        ("key".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
                    ])),
                    compressed: None,
                },
            ],
            source_part_ids: vec![1, 2],
            primary_key: vec!["key".into()],
            merge_strategy: MergeStrategy::Aggregating {
                group_columns: vec!["key".into()],
                sum_columns: vec!["total".into()],
                count_columns: vec![],
            },
        };

        let result = execute_merge_task(task, 99);
        assert_eq!(result.merged_part.row_count, 2);

        if let Some(ColumnData::Int64(totals)) = result.merged_part.data.column("total") {
            assert_eq!(totals, &[Some(40), Some(60)]); // 10+30, 20+40
        }
    }

    #[test]
    fn replacing_mergetree_multicolumn_pk() {
        let strategy = MergeStrategy::Replacing { version_column: Some("ver".into()) };
        let mut mt = MergeTree::new_with_strategy(
            vec!["tenant".into(), "id".into()],
            strategy,
        );
        mt.max_parts = 100;

        mt.insert(ColumnBatch::new(vec![
            ("tenant".into(), ColumnData::Text(vec![Some("a".into()), Some("a".into())])),
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(1)])),
            ("ver".into(), ColumnData::Int64(vec![Some(1), Some(2)])),
            ("data".into(), ColumnData::Text(vec![Some("v1".into()), Some("v2".into())])),
        ]));
        mt.insert(ColumnBatch::new(vec![
            ("tenant".into(), ColumnData::Text(vec![Some("b".into())])),
            ("id".into(), ColumnData::Int64(vec![Some(1)])),
            ("ver".into(), ColumnData::Int64(vec![Some(1)])),
            ("data".into(), ColumnData::Text(vec![Some("b_v1".into())])),
        ]));
        mt.optimize();

        let batches = mt.scan_all();
        assert_eq!(batches.len(), 1);
        // (a,1) deduped to ver=2, (b,1) stays
        assert_eq!(batches[0].row_count, 2);

        if let Some(ColumnData::Text(tenants)) = batches[0].column("tenant") {
            assert_eq!(tenants, &[Some("a".into()), Some("b".into())]);
        }
        if let Some(ColumnData::Int64(vers)) = batches[0].column("ver") {
            assert_eq!(vers, &[Some(2), Some(1)]);
        }
    }
}
