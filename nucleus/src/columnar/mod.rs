//! Columnar storage engine — native column-oriented storage for analytics.
//!
//! Stores data in column batches rather than rows. Each column batch contains
//! values of a single type, enabling:
//!   - Vectorized scan/filter/aggregate operations
//!   - Better compression (similar values stored together)
//!   - Cache-efficient sequential reads for analytical queries
//!
//! Replaces ClickHouse for OLAP workloads within Nucleus.

use std::collections::HashMap;
use regex::Regex;

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

/// In-memory columnar table store.
#[derive(Debug)]
pub struct ColumnarStore {
    /// Table name → list of column batches.
    tables: HashMap<String, Vec<ColumnBatch>>,
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
        }
    }

    /// Append a batch to a table.
    pub fn append(&mut self, table: &str, batch: ColumnBatch) {
        self.tables
            .entry(table.to_string())
            .or_default()
            .push(batch);
    }

    /// Get all batches for a table.
    pub fn batches(&self, table: &str) -> &[ColumnBatch] {
        self.tables.get(table).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Total row count across all batches for a table.
    pub fn row_count(&self, table: &str) -> usize {
        self.batches(table).iter().map(|b| b.row_count).sum()
    }

    /// Ensure a table entry exists (creates an empty table if absent).
    pub fn create_table(&mut self, table: &str) {
        self.tables.entry(table.to_string()).or_default();
    }

    /// Remove a table and all its batches. Returns true if the table existed.
    pub fn drop_table(&mut self, table: &str) -> bool {
        self.tables.remove(table).is_some()
    }

    /// Remove all batches from a table without dropping the table entry.
    pub fn clear(&mut self, table: &str) {
        if let Some(v) = self.tables.get_mut(table) {
            v.clear();
        }
    }

    /// Returns true if the table exists in this store.
    pub fn table_exists(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Return all table names (order unspecified).
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
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
    pub fn scan_with_predicate(
        &self,
        table: &str,
        column: &str,
        predicate: &TextPredicate,
    ) -> Vec<ColumnBatch> {
        self.batches(table)
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
                        Some(dict[idx as usize].clone())
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
// Gap 10: MergeTree Storage Engine
// ============================================================================

/// A "part" in the MergeTree — a sorted chunk of data.
#[derive(Debug)]
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

/// MergeTree storage engine — LSM-tree inspired columnar storage with:
/// - Sorted data parts
/// - Background part merges
/// - Partition pruning via zone maps
/// - Primary key ordering
#[derive(Debug)]
pub struct MergeTree {
    /// Primary key columns (data is sorted by these columns).
    pub primary_key: Vec<String>,
    /// Immutable parts, sorted by part ID.
    parts: Vec<MergeTreePart>,
    /// Next part ID.
    next_part_id: u64,
    /// Maximum rows per part before splitting on insert.
    pub max_part_rows: usize,
    /// Maximum number of parts before triggering a merge.
    pub max_parts: usize,
}

impl MergeTree {
    pub fn new(primary_key: Vec<String>) -> Self {
        MergeTree {
            primary_key,
            parts: Vec::new(),
            next_part_id: 1,
            max_part_rows: 8192,
            max_parts: 10,
        }
    }

    /// Insert a batch of data. The batch will be sorted by the primary key
    /// and stored as a new part.
    pub fn insert(&mut self, batch: ColumnBatch) {
        let sorted = self.sort_by_pk(&batch);
        let row_count = sorted.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        let zone_map = ZoneMap::from_batch(&sorted);

        let part = MergeTreePart {
            id: self.next_part_id,
            data: sorted,
            row_count,
            zone_map,
            compressed: None,
        };
        self.next_part_id += 1;
        self.parts.push(part);

        // Auto-merge if too many parts
        while self.parts.len() > self.max_parts {
            self.merge_smallest_parts();
        }
    }

    /// Sort a batch by the primary key columns.
    fn sort_by_pk(&self, batch: &ColumnBatch) -> ColumnBatch {
        if self.primary_key.is_empty() || batch.columns.is_empty() {
            return batch.clone();
        }

        let row_count = batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        if row_count <= 1 {
            return batch.clone();
        }

        // Build sort indices based on primary key columns
        let mut indices: Vec<usize> = (0..row_count).collect();

        // Get primary key column data for sorting
        let pk_col = batch.column(&self.primary_key[0]);
        if let Some(ColumnData::Int64(vals)) = pk_col {
            indices.sort_by(|&a, &b| {
                let va = vals[a].unwrap_or(i64::MAX);
                let vb = vals[b].unwrap_or(i64::MAX);
                va.cmp(&vb)
            });
        } else if let Some(ColumnData::Text(vals)) = pk_col {
            indices.sort_by(|&a, &b| {
                let va = vals[a].as_deref().unwrap_or("");
                let vb = vals[b].as_deref().unwrap_or("");
                va.cmp(vb)
            });
        }

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

        let merged_batch = merge_sorted_batches(&first.data, &second.data, &self.primary_key);
        let row_count = merged_batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        let zone_map = ZoneMap::from_batch(&merged_batch);

        let merged = MergeTreePart {
            id: self.next_part_id,
            data: merged_batch,
            row_count,
            zone_map,
            compressed: None,
        };
        self.next_part_id += 1;
        self.parts.push(merged);
    }

    /// Scan all parts, pruning those whose zone maps exclude the predicate.
    pub fn scan(&self, predicate_col: &str, op: CmpOp, value: &ScalarValue) -> Vec<&ColumnBatch> {
        self.parts
            .iter()
            .filter(|part| !part.zone_map.can_skip(predicate_col, op, value))
            .map(|part| &part.data)
            .collect()
    }

    /// Scan all parts (no predicate, full scan).
    pub fn scan_all(&self) -> Vec<&ColumnBatch> {
        self.parts.iter().map(|p| &p.data).collect()
    }

    /// Number of parts.
    pub fn part_count(&self) -> usize {
        self.parts.len()
    }

    /// Total row count across all parts.
    pub fn total_rows(&self) -> usize {
        self.parts.iter().map(|p| p.row_count).sum()
    }

    /// Force merge all parts into one.
    pub fn optimize(&mut self) {
        while self.parts.len() > 1 {
            self.merge_smallest_parts();
        }
    }

    /// Compress all parts using adaptive compression.
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

    // Sort by PK if available
    if primary_key.is_empty() {
        return batch;
    }

    let row_count = batch.columns.first().map(|(_, c)| c.len()).unwrap_or(0);
    let mut indices: Vec<usize> = (0..row_count).collect();

    if let Some(ColumnData::Int64(vals)) = batch.column(&primary_key[0]) {
        indices.sort_by(|&a, &b| {
            let va = vals[a].unwrap_or(i64::MAX);
            let vb = vals[b].unwrap_or(i64::MAX);
            va.cmp(&vb)
        });
    }

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
}
