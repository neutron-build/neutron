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
    ColumnBatch, ColumnData, ColumnarStore, aggregate_count, aggregate_sum, group_by_text_agg_f64,
};
use crate::storage::columnar_wal::ColumnarWal;
use crate::storage::{FilterOp, StorageEngine, StorageError};
use crate::types::{Row, Value};

// ColumnData intentionally has a compact primitive physical type set. Preserve
// exact logical scalars in its Text representation with private type tags;
// SQL text cannot contain NUL on the PostgreSQL wire, so ordinary TEXT cannot
// collide with this encoding.
const NUMERIC_TEXT_TAG: &str = "\0nucleus:numeric:";
const DATE_TEXT_TAG: &str = "\0nucleus:date:";
const TIMESTAMP_TEXT_TAG: &str = "\0nucleus:timestamp:";
const TIMESTAMPTZ_TEXT_TAG: &str = "\0nucleus:timestamptz:";
const INTERVAL_TEXT_TAG: &str = "\0nucleus:interval:";

fn encode_logical_text(value: &Value) -> Option<String> {
    match value {
        Value::Numeric(value) => Some(format!("{NUMERIC_TEXT_TAG}{value}")),
        Value::Date(value) => Some(format!("{DATE_TEXT_TAG}{value}")),
        Value::Timestamp(value) => Some(format!("{TIMESTAMP_TEXT_TAG}{value}")),
        Value::TimestampTz(value) => Some(format!("{TIMESTAMPTZ_TEXT_TAG}{value}")),
        Value::Interval {
            months,
            days,
            microseconds,
        } => Some(format!("{INTERVAL_TEXT_TAG}{months},{days},{microseconds}")),
        _ => None,
    }
}

fn decode_columnar_text(value: &str) -> Value {
    if let Some(raw) = value.strip_prefix(NUMERIC_TEXT_TAG) {
        return Value::Numeric(raw.to_owned());
    }
    if let Some(raw) = value.strip_prefix(DATE_TEXT_TAG)
        && let Ok(value) = raw.parse()
    {
        return Value::Date(value);
    }
    if let Some(raw) = value.strip_prefix(TIMESTAMP_TEXT_TAG)
        && let Ok(value) = raw.parse()
    {
        return Value::Timestamp(value);
    }
    if let Some(raw) = value.strip_prefix(TIMESTAMPTZ_TEXT_TAG)
        && let Ok(value) = raw.parse()
    {
        return Value::TimestampTz(value);
    }
    if let Some(raw) = value.strip_prefix(INTERVAL_TEXT_TAG) {
        let mut parts = raw.split(',');
        if let (Some(months), Some(days), Some(microseconds), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
            && let (Ok(months), Ok(days), Ok(microseconds)) =
                (months.parse(), days.parse(), microseconds.parse())
        {
            return Value::Interval {
                months,
                days,
                microseconds,
            };
        }
    }
    Value::Text(value.to_owned())
}

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
    /// Register this table as a MergeTree ordered by `order_by`, on THIS
    /// engine's store.
    ///
    /// Columnar and MergeTree tables are served by a per-table engine, each
    /// with its own `ColumnarStore`. DDL was registering the merge tree on the
    /// executor's store instead — a store that never serves the table's reads —
    /// so `ORDER BY` was parsed, persisted, restored at boot, registered, and
    /// then invisible to every scan: parts were never sorted and their zone
    /// maps never consulted.
    pub fn register_merge_tree(
        &self,
        table: &str,
        order_by: Vec<String>,
        strategy: crate::columnar::MergeStrategy,
    ) {
        self.store
            .write()
            .create_merge_tree_table_with_strategy(table, order_by, strategy);
    }

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
        let (wal, state) = ColumnarWal::open(dir).map_err(|e| StorageError::Io(e.to_string()))?;
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
                let rows = batches_to_rows(&store.batches_all(&name));
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

/// The underlying type of a GROUP BY key column. `fast_group_by` hashes keys as
/// strings, then uses this to reconstruct the native `Value` so an integer key
/// is returned (and later ordered) numerically rather than lexicographically.
#[derive(Clone, Copy)]
enum ColumnarKeyKind {
    Text,
    Int32,
    Int64,
    Float64,
    Bool,
}

impl ColumnarKeyKind {
    fn to_value(self, key: String) -> Value {
        match self {
            // Parse back to the native type; fall back to Text if parsing fails
            // (the string came from `to_string()` of that type, so it won't).
            ColumnarKeyKind::Int32 => key
                .parse::<i32>()
                .map(Value::Int32)
                .unwrap_or(Value::Text(key)),
            ColumnarKeyKind::Int64 => key
                .parse::<i64>()
                .map(Value::Int64)
                .unwrap_or(Value::Text(key)),
            ColumnarKeyKind::Float64 => key
                .parse::<f64>()
                .map(Value::Float64)
                .unwrap_or(Value::Text(key)),
            ColumnarKeyKind::Bool => key
                .parse::<bool>()
                .map(Value::Bool)
                .unwrap_or(Value::Text(key)),
            ColumnarKeyKind::Text => decode_columnar_text(&key),
        }
    }
}

#[allow(dead_code)]
fn val_to_coldata(v: Value) -> ColumnData {
    match v {
        Value::Bool(b) => ColumnData::Bool(vec![Some(b)]),
        Value::Int32(n) => ColumnData::Int32(vec![Some(n)]),
        Value::Int64(n) => ColumnData::Int64(vec![Some(n)]),
        Value::Float64(f) => ColumnData::Float64(vec![Some(f)]),
        Value::Text(s) => ColumnData::Text(vec![Some(s)]),
        logical @ (Value::Numeric(_)
        | Value::Date(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_)
        | Value::Interval { .. }) => ColumnData::Text(vec![encode_logical_text(&logical)]),
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
        Some(
            Value::Numeric(_)
            | Value::Date(_)
            | Value::Timestamp(_)
            | Value::TimestampTz(_)
            | Value::Interval { .. },
        ) => ColumnData::Text(
            vals.into_iter()
                .map(|v| match v {
                    Value::Null => None,
                    other => encode_logical_text(&other),
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
        ColumnData::Bool(v) => v
            .get(idx)
            .copied()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        ColumnData::Int32(v) => v
            .get(idx)
            .copied()
            .flatten()
            .map(Value::Int32)
            .unwrap_or(Value::Null),
        ColumnData::Int64(v) => v
            .get(idx)
            .copied()
            .flatten()
            .map(Value::Int64)
            .unwrap_or(Value::Null),
        ColumnData::Float64(v) => v
            .get(idx)
            .copied()
            .flatten()
            .map(Value::Float64)
            .unwrap_or(Value::Null),
        ColumnData::Text(v) => v
            .get(idx)
            .and_then(|o| o.as_ref())
            .map(|s| decode_columnar_text(s))
            .unwrap_or(Value::Null),
    }
}

/// Reconstruct `Vec<Row>` from a slice of ColumnBatches.
fn batches_to_rows<B: AsRef<ColumnBatch>>(batches: &[B]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let batch = batch.as_ref();
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

/// Reconstruct rows containing ONLY `projection`, in that order.
///
/// The whole point of a column store is not touching columns a query never
/// mentions. Without this the engine inherited the default `scan_projected`,
/// which calls `scan()` and discards columns afterwards — so a
/// `SELECT AVG(dur) … ` over a 17-column span table decoded every JSONB blob
/// in the table to throw them away, and the columnar layout was pure overhead.
/// That is why the columnar engine measured SLOWER than the row heap.
fn batches_to_rows_projected<B: AsRef<ColumnBatch>>(
    batches: &[B],
    projection: &[usize],
    limit: Option<usize>,
) -> Vec<Row> {

    let mut rows = Vec::new();
    for batch in batches {
        let batch = batch.as_ref();
        for row_i in 0..batch.row_count {
            if limit.is_some_and(|n| rows.len() >= n) {
                return rows;
            }
            let row: Row = projection
                .iter()
                .map(|&col_i| match batch.columns.get(col_i) {
                    Some((_, col)) => coldata_get(col, row_i),
                    // A projection index past this batch's width (schema
                    // evolution: the column was added after this batch was
                    // written) reads as NULL, matching a full scan.
                    None => crate::types::Value::Null,
                })
                .collect();
            rows.push(row);
        }
    }
    rows
}

/// Reconstruct at most `limit` rows, stopping as soon as the limit is reached so
/// the tail rows are never assembled (no `Value` allocation for them). Batches
/// are already dedup-resolved by `batches_all_for_select`, so the first `limit`
/// rows equal `batches_to_rows(..)` truncated to `limit`.
fn batches_to_rows_limit<B: AsRef<ColumnBatch>>(batches: &[B], limit: usize) -> Vec<Row> {
    let mut rows = Vec::with_capacity(limit.min(1024));
    if limit == 0 {
        return rows;
    }
    for batch in batches {
        let batch = batch.as_ref();
        for row_i in 0..batch.row_count {
            let row: Row = (0..batch.columns.len())
                .map(|col_i| {
                    let (_, col) = &batch.columns[col_i];
                    coldata_get(col, row_i)
                })
                .collect();
            rows.push(row);
            if rows.len() >= limit {
                return rows;
            }
        }
    }
    rows
}

/// Reconstruct only rows where `batches[*][filter_col] == filter_val`.
/// Avoids allocating Value objects for non-matching rows.
/// Returns `(matched_rows, rows_examined)`. `rows_examined` is the total number of
/// rows the scan inspected across all batches — the sequential-scan size the caller
/// reports to the `rows_scanned` metric (matching Postgres Seq Scan semantics).
fn batches_to_rows_where_eq(
    batches: &[ColumnBatch],
    filter_col: usize,
    filter_val: &Value,
) -> (Vec<Row>, usize) {
    let mut rows = Vec::new();
    let mut examined = 0usize;
    for batch in batches {
        let Some((_, filter_data)) = batch.columns.get(filter_col) else {
            continue;
        };
        examined += batch.row_count;
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
    (rows, examined)
}

/// Fetch rows from a slice of ColumnBatches by their global (scan-order) positions.
///
/// Global position 0 = first row of first batch, len(batch0) = first row of second batch, etc.
/// Positions need not be sorted. Unresolvable positions produce a row of Nulls.
/// Batch selection for read-time aggregates: dedup a replacing_mergetree table
/// (so superseded versions don't inflate SUM/AVG/MIN/MAX/GROUP BY), else return
/// physical batches. Mirrors the guard in `fast_count_all`.
/// NOTE: read paths only — mutations must use `batches_all` directly.
/// Batches for a read-only aggregate, borrowing the stored column data when
/// possible. Aggregate fast-paths (sum/count/group-by) only read columns, so
/// for a raw (non-MergeTree) table we hand back a borrow of `tables` instead of
/// cloning every column of every row — the clone, not the math, was the cost.
/// MergeTree and replacing tables still materialize (parts / dedup need owned).
fn batches_for_read<'a>(store: &'a ColumnarStore, table: &str) -> ReadBatches<'a> {
    if crate::columnar::replacing_config(table).is_some() {
        ReadBatches::Owned(store.batches_all_for_select(table))
    } else if let Some(shared) = store.batches_all_shared(table) {
        // A MergeTree's parts are not a contiguous slice, so this used to hand
        // back an owned deep copy — every column of every part, on every read,
        // including from the aggregate fast paths. The parts are already
        // `Arc`-held; sharing them costs a refcount.
        ReadBatches::Shared(shared)
    } else {
        ReadBatches::Borrowed(store.batches(table))
    }
}

/// Batches for a read, borrowed, shared, or owned depending on what the table's
/// storage can offer without copying.
enum ReadBatches<'a> {
    Borrowed(&'a [ColumnBatch]),
    Owned(Vec<ColumnBatch>),
    Shared(Vec<std::sync::Arc<ColumnBatch>>),
}

impl ReadBatches<'_> {
    /// One reference per batch. The vector is per-batch, not per-row — a table
    /// has tens of parts, not millions — so this is not the copy that mattered.
    fn refs(&self) -> Vec<&ColumnBatch> {
        match self {
            Self::Borrowed(b) => b.iter().collect(),
            Self::Owned(v) => v.iter().collect(),
            Self::Shared(v) => v.iter().map(|a| &**a).collect(),
        }
    }
}

/// Like `batches_for_read` but returns *physical* batches with no read-time
/// dedup — for index lookups, whose stored positions index physical rows. Using
/// the deduped/reordered `batches_for_read` here would fetch the wrong rows on a
/// replacing table (positions wouldn't line up). Borrows raw tables (no clone),
/// materializes MergeTree parts.
fn physical_batches_for_read<'a>(
    store: &'a ColumnarStore,
    table: &str,
) -> std::borrow::Cow<'a, [ColumnBatch]> {
    use std::borrow::Cow;
    if store.is_merge_tree(table) {
        Cow::Owned(store.batches_all(table))
    } else {
        Cow::Borrowed(store.batches(table))
    }
}

/// Reconstruct rows from part slices, reading only the projected columns of the
/// rows each slice covers.
fn slices_to_rows_projected(
    slices: &[crate::columnar::PartSlice],
    projection: &[usize],
    limit: Option<usize>,
) -> Vec<Row> {
    let mut rows = Vec::new();
    for slice in slices {
        for row_i in slice.start..slice.end {
            if limit.is_some_and(|n| rows.len() >= n) {
                return rows;
            }
            let row: Row = projection
                .iter()
                .map(|&col_i| match slice.batch.columns.get(col_i) {
                    Some((_, col)) => coldata_get(col, row_i),
                    None => Value::Null,
                })
                .collect();
            rows.push(row);
        }
    }
    rows
}

/// Translate an executor filter predicate into zone-map bounds.
///
/// Every bound returned must be one the statistics can *disprove*; a predicate
/// with no such bound returns empty and prunes nothing. `Between` yields both
/// ends, because a one-sided bound leaves half a window unpruned. Predicates
/// whose truth a min/max cannot rule out (`IN`, `LIKE`, null tests) are absent
/// on purpose.
fn zone_map_bounds(
    predicate: &crate::storage::granule_stats::FilterPredicate,
) -> Vec<(crate::columnar::CmpOp, crate::columnar::ScalarValue)> {
    use crate::columnar::CmpOp;
    use crate::storage::granule_stats::FilterPredicate as P;
    let one = |op, v: &Value| scalar_of(v).map(|s| vec![(op, s)]).unwrap_or_default();
    match predicate {
        P::Equal(v) => one(CmpOp::Eq, v),
        P::GreaterThan(v) => one(CmpOp::Gt, v),
        P::GreaterThanOrEqual(v) => one(CmpOp::Gte, v),
        P::LessThan(v) => one(CmpOp::Lt, v),
        P::LessThanOrEqual(v) => one(CmpOp::Lte, v),
        P::Between { min, max } => match (scalar_of(min), scalar_of(max)) {
            (Some(lo), Some(hi)) => vec![(CmpOp::Gte, lo), (CmpOp::Lte, hi)],
            _ => Vec::new(),
        },
        P::In(_) | P::IsNull | P::IsNotNull | P::Like { .. } => Vec::new(),
    }
}

/// A `Value` as the scalar a zone map is built from, or `None` when the two
/// cannot be compared.
///
/// Temporal values are stored as their integer representation, so their zone
/// maps are `Int64`/`Int32` — a bound must be converted the same way or
/// `can_skip` sees two different variants and prunes nothing.
fn scalar_of(v: &Value) -> Option<crate::columnar::ScalarValue> {
    use crate::columnar::ScalarValue as S;
    Some(match v {
        Value::Int32(n) => S::Int32(*n),
        Value::Int64(n) => S::Int64(*n),
        Value::Float64(f) => S::Float64(*f),
        Value::Text(t) => S::Text(t.clone()),
        Value::Bool(b) => S::Bool(*b),
        Value::Date(d) => S::Int32(*d),
        Value::Timestamp(t) | Value::TimestampTz(t) => S::Int64(*t),
        _ => return None,
    })
}

/// Shared handles to a MergeTree's parts, or `None` for any table where that
/// does not apply.
///
/// `batches_for_read` has to hand back owned data for a MergeTree — its parts
/// are not a contiguous slice — and that meant every read deep-copied every
/// column of every part. The parts are already `Arc`-held, so sharing them
/// costs a refcount. Replacing tables are excluded: their read-time dedup needs
/// all parts together and produces new batches anyway.
fn shared_merge_tree_batches(
    store: &ColumnarStore,
    table: &str,
) -> Option<Vec<std::sync::Arc<ColumnBatch>>> {
    if crate::columnar::replacing_config(table).is_some() {
        return None;
    }
    store.batches_all_shared(table)
}

fn fetch_rows_by_positions<B: AsRef<ColumnBatch>>(batches: &[B], positions: &[usize]) -> Vec<Row> {
    if positions.is_empty() || batches.is_empty() {
        return Vec::new();
    }
    let n_cols = batches[0].as_ref().columns.len();

    // Precompute cumulative batch offsets so we can binary-search to the right batch.
    let mut offsets = Vec::with_capacity(batches.len() + 1);
    offsets.push(0usize);
    for b in batches {
        offsets.push(offsets.last().unwrap() + b.as_ref().row_count);
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
        let batch = batches[batch_idx].as_ref();
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
        (ColumnData::Text(v), logical) if encode_logical_text(logical).is_some() => {
            let encoded = encode_logical_text(logical).expect("guarded logical scalar");
            v.iter()
                .map(|value| value.as_deref() == Some(encoded.as_str()))
                .collect()
        }
        (ColumnData::Int64(v), Value::Int64(n)) => v.iter().map(|o| o == &Some(*n)).collect(),
        (ColumnData::Int32(v), Value::Int32(n)) => v.iter().map(|o| o == &Some(*n)).collect(),
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
        (ColumnData::Float64(v), Value::Float64(f)) => v.iter().map(|o| o == &Some(*f)).collect(),
        (ColumnData::Bool(v), Value::Bool(b)) => v.iter().map(|o| o == &Some(*b)).collect(),
        _ => vec![false; col.len()],
    }
}

/// Read a predicate literal as f64 for numeric comparison, if it is numeric.
fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int32(n) => Some(*n as f64),
        Value::Int64(n) => Some(*n as f64),
        Value::Float64(f) => Some(*f),
        Value::Bool(b) => Some(*b as i64 as f64),
        _ => None,
    }
}

/// Does `ord` (the result of comparing a value to the predicate) satisfy `op`?
fn apply_ord(ord: std::cmp::Ordering, op: FilterOp) -> bool {
    use std::cmp::Ordering::*;
    match op {
        FilterOp::Eq => ord == Equal,
        FilterOp::Ne => ord != Equal,
        FilterOp::Lt => ord == Less,
        FilterOp::Le => ord != Greater,
        FilterOp::Gt => ord == Greater,
        FilterOp::Ge => ord != Less,
    }
}

/// Build a boolean mask for `col OP val`. NULLs are always false (a NULL
/// comparison is SQL-unknown, excluded from filtered COUNT/SUM). Eq delegates
/// to `eq_mask` (exact, cross-int-width aware); ordered ops compare numerics as
/// f64 and text lexically.
fn cmp_mask(col: &ColumnData, op: FilterOp, val: &Value) -> Vec<bool> {
    if op == FilterOp::Eq {
        return eq_mask(col, val);
    }
    if let Some(pred) = value_as_f64(val) {
        let test = |o: Option<f64>| match o {
            Some(x) => apply_ord(x.total_cmp(&pred), op),
            None => false,
        };
        return match col {
            ColumnData::Int32(v) => v.iter().map(|o| test(o.map(|x| x as f64))).collect(),
            ColumnData::Int64(v) => v.iter().map(|o| test(o.map(|x| x as f64))).collect(),
            ColumnData::Float64(v) => v.iter().map(|o| test(*o)).collect(),
            ColumnData::Bool(v) => v.iter().map(|o| test(o.map(|b| b as i64 as f64))).collect(),
            ColumnData::Text(_) => vec![false; col.len()],
        };
    }
    if let ColumnData::Text(values) = col
        && encode_logical_text(val).is_some()
    {
        return values
            .iter()
            .map(|value| {
                value
                    .as_deref()
                    .map(decode_columnar_text)
                    .is_some_and(|value| apply_ord(value.cmp(val), op))
            })
            .collect();
    }
    if let (ColumnData::Text(v), Value::Text(s)) = (col, val) {
        return v
            .iter()
            .map(|o| match o.as_deref() {
                Some(x) => apply_ord(x.cmp(s.as_str()), op),
                None => false,
            })
            .collect();
    }
    vec![false; col.len()]
}

/// Sum the numeric values in `col` at positions where `mask[i]` is true.
fn sum_masked(col: &ColumnData, mask: &[bool]) -> (f64, usize) {
    let mut sum = 0.0f64;
    let mut count = 0usize;
    match col {
        ColumnData::Float64(v) => {
            for (opt, &keep) in v.iter().zip(mask) {
                if keep && let Some(f) = opt {
                    sum += f;
                    count += 1;
                }
            }
        }
        ColumnData::Int64(v) => {
            for (opt, &keep) in v.iter().zip(mask) {
                if keep && let Some(n) = opt {
                    sum += *n as f64;
                    count += 1;
                }
            }
        }
        ColumnData::Int32(v) => {
            for (opt, &keep) in v.iter().zip(mask) {
                if keep && let Some(n) = opt {
                    sum += *n as f64;
                    count += 1;
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
        let rows = batches_to_rows(&self.store.read().batches_all(table));
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
    fn as_columnar(&self) -> Option<&ColumnarStorageEngine> {
        Some(self)
    }

    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        self.store.write().create_table(table);
        if let Some(wal) = &self.wal {
            wal.log_create_table(table)
                .map_err(|e| StorageError::Io(e.to_string()))?;
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
            wal.log_drop_table(table)
                .map_err(|e| StorageError::Io(e.to_string()))?;
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
            wal.log_insert_rows(table, std::slice::from_ref(&row))
                .map_err(|e| StorageError::Io(e.to_string()))?;
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
            // Use append_with_dict so low-cardinality text columns (browser, OS,
            // country, etc.) get automatic dictionary compression.
            store.append_with_dict(table, rows_to_batch(rows.clone()));
        }
        if let Some(wal) = &self.wal {
            wal.log_insert_rows(table, &rows)
                .map_err(|e| StorageError::Io(e.to_string()))?;
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
        // `batches_for_read` applies replacing_mergetree dedup for tables
        // registered via crate::columnar::register_replacing_table and
        // materializes MergeTree parts; a plain table is borrowed rather than
        // cloned.
        if let Some(shared) = shared_merge_tree_batches(&store, table) {
            return Ok(batches_to_rows(&shared));
        }
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        Ok(batches_to_rows(&batches))
    }

    /// Read only the projected columns. The default implementation scans every
    /// column and discards the unwanted ones, which defeats the entire purpose
    /// of a column store; `DiskEngine` already overrides it for the same reason.
    async fn scan_projected(
        &self,
        table: &str,
        projection: &[usize],
        limit: Option<usize>,
    ) -> Result<Vec<Row>, StorageError> {
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        // `batches_for_read` borrows the stored columns; `batches_all_for_select`
        // CLONES every column of every row first, so a column store paid for a
        // full copy of the table before the projection narrowed it to one
        // column. That copy was the entire cost of a columnar query — the
        // borrowing accessor already existed and only the aggregate fast paths
        // were using it.
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        Ok(batches_to_rows_projected(&batches, projection, limit))
    }

    /// Skip whole MergeTree parts whose zone maps prove they cannot match.
    ///
    /// The parts are already sorted by the declared `ORDER BY` and already
    /// carry min/max statistics, and `MergeTree::scan` already knew how to
    /// prune with them — nothing called it, and nothing could: the storage scan
    /// API had no way to receive a predicate.
    async fn scan_projected_pruned(
        &self,
        table: &str,
        projection: &[usize],
        limit: Option<usize>,
        prune: Option<(&str, &crate::storage::granule_stats::FilterPredicate)>,
    ) -> Result<Vec<Row>, StorageError> {
        let Some((col, predicate)) = prune else {
            return self.scan_projected(table, projection, limit).await;
        };
        let bounds = zone_map_bounds(predicate);
        if bounds.is_empty() {
            return self.scan_projected(table, projection, limit).await;
        }
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        // Replacing tables need read-time dedup across ALL parts, so a pruned
        // subset could resurrect a superseded row. Take the unpruned path.
        if crate::columnar::replacing_config(table).is_some() {
            let read = batches_for_read(&store, table);
        let batches = read.refs();
            return Ok(batches_to_rows_projected(&batches, projection, limit));
        }
        match store.batches_pruned_slices(table, col, &bounds) {
            Some(slices) => Ok(slices_to_rows_projected(&slices, projection, limit)),
            // Not a MergeTree: no declared order, so no range-coherent parts.
            None => {
                let read = batches_for_read(&store, table);
        let batches = read.refs();
                Ok(batches_to_rows_projected(&batches, projection, limit))
            }
        }
    }

    async fn scan_limit(&self, table: &str, limit: usize) -> Result<Vec<Row>, StorageError> {
        // Early-exit: assemble only the first `limit` rows from the (already
        // dedup-resolved) batches. Same order as scan(), so equals
        // scan()[..limit]. Safe here (the columnar engine records no SIREAD).
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        if let Some(shared) = shared_merge_tree_batches(&store, table) {
            return Ok(batches_to_rows_limit(&shared, limit));
        }
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        Ok(batches_to_rows_limit(&batches, limit))
    }

    async fn scan_where_eq_positions(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
    ) -> Result<Vec<(usize, Row)>, StorageError> {
        // UPDATE/DELETE need every physical row matching the predicate, so
        // mutations remove/overwrite all versions of a logical PK. The
        // default impl calls scan() which deduplicates for replacing tables —
        // that would leave older versions orphaned and they'd resurrect on
        // the next read. Override to scan physical batches directly.
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        let rows = batches_to_rows(&store.batches_all(table));
        Ok(rows
            .into_iter()
            .enumerate()
            // Coercing eq (loose_eq): a text-bound BIGINT PK (Int64) must match an
            // Int32/text WHERE literal, else UPDATE/DELETE by PK silently no-ops.
            .filter(|(_, row)| row.get(col_idx).is_some_and(|v| v.loose_eq(value)))
            .collect())
    }

    async fn scan_physical(&self, table: &str) -> Result<Vec<(usize, Row)>, StorageError> {
        // Same reasoning as scan_where_eq_positions, but for the no-WHERE-PK
        // path of UPDATE/DELETE: return physical batches (NOT batches_all_for_select)
        // so positions map to the rows update()/delete() actually rewrite.
        // scan() here would dedup a replacing/aggregating table and corrupt mutations.
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return Err(StorageError::TableNotFound(table.to_string()));
        }
        Ok(batches_to_rows(&store.batches_all(table))
            .into_iter()
            .enumerate()
            .collect())
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
            let old_rows = batches_to_rows(&store.batches_all(table));
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
            let refs: Vec<(&str, Vec<Row>)> = tables
                .iter()
                .map(|(n, r)| (n.as_str(), r.clone()))
                .collect();
            wal.checkpoint(&refs)
                .map_err(|e| StorageError::Io(e.to_string()))?;
        }
        Ok(count)
    }

    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError> {
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
            let old_rows = batches_to_rows(&store.batches_all(table));
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
            let refs: Vec<(&str, Vec<Row>)> = tables
                .iter()
                .map(|(n, r)| (n.as_str(), r.clone()))
                .collect();
            wal.checkpoint(&refs)
                .map_err(|e| StorageError::Io(e.to_string()))?;
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
            batches_to_rows(&store.batches_all(table))
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
            let names = tnames.entry(table.to_string()).or_default();
            // Idempotent: re-creating an existing index (e.g. a derived-state
            // rebuild) must not double-register the name, or per-name index
            // maintenance would insert every row twice.
            if !names.iter().any(|n| n == index_name) {
                names.push(index_name.to_string());
            }
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

    // The executor's point/range fast paths call the async index_lookup; without
    // these overrides they hit the trait default (Ok(None)) and fall back to a
    // full table scan, so a columnar table's PK lookups were O(n). Delegate to
    // the (synchronous, in-memory) index implementations.
    async fn index_lookup(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.index_lookup_sync(table, index_name, value)
    }

    async fn index_lookup_range(
        &self,
        table: &str,
        index_name: &str,
        low: std::ops::Bound<&Value>,
        high: std::ops::Bound<&Value>,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.index_lookup_range_sync(table, index_name, low, high)
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
        // No positions means no rows, and no batches are needed to say so.
        // Materializing first made every miss cost a full table read — and a
        // miss is the common case: `check_unique_constraints` probes the
        // primary key of every row being INSERTed, and a new key is always
        // absent. On a MergeTree, where `physical_batches_for_read` cannot
        // borrow, that was a deep copy of the entire table PER INSERTED ROW.
        if positions.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let store = self.store.read();
        // Index points at physical positions; fetch by those, then dedup so
        // SELECT via the index sees one row per logical PK. Borrow batches for
        // raw tables — cloning the whole table to fetch a handful of rows by
        // position defeats the point of the index.
        let batches = physical_batches_for_read(&store, table);
        let rows = fetch_rows_by_positions(&batches, &positions);
        let out = match crate::columnar::replacing_config(table) {
            Some(c) => crate::columnar::dedup_replacing_rows(rows, &c),
            None => rows,
        };
        Ok(Some(out))
    }

    fn index_lookup_range_sync(
        &self,
        table: &str,
        index_name: &str,
        low: std::ops::Bound<&Value>,
        high: std::ops::Bound<&Value>,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        if crate::storage::range_cannot_match(low, high) {
            return Ok(Some(Vec::new()));
        }
        // Flush write buffer so all single-row inserts have stable positions.
        self.flush_write_buffer(table);
        let positions: Vec<usize> = {
            let indexes = self.indexes.read();
            match indexes.get(index_name) {
                Some(idx) => {
                    // BTreeMap::range: O(log n) seek + O(k) scan over compact position entries.
                    idx.map
                        .range((low, high))
                        .flat_map(|(_, pos)| pos.iter().copied())
                        .collect()
                }
                None => return Ok(None),
            }
        };
        let store = self.store.read();
        let batches = physical_batches_for_read(&store, table);
        let rows = fetch_rows_by_positions(&batches, &positions);
        let out = match crate::columnar::replacing_config(table) {
            Some(c) => crate::columnar::dedup_replacing_rows(rows, &c),
            None => rows,
        };
        Ok(Some(out))
    }

    // ─── Aggregate fast paths ─────────────────────────────────────────────────

    fn fast_count_all(&self, table: &str) -> Option<usize> {
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        // For replacing_mergetree the deduped logical row count must match
        // SELECT *. Counting raw row_count would over-report by every
        // superseded version. batches_all_for_select collapses those.
        if crate::columnar::replacing_config(table).is_some() {
            let batches = store.batches_all_for_select(table);
            return Some(batches.iter().map(|b| b.row_count).sum());
        }
        Some(store.row_count(table))
    }

    fn fast_topk(&self, table: &str, sort_col: usize, desc: bool, k: usize) -> Option<Vec<Row>> {
        if k == 0 {
            return Some(Vec::new());
        }
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        let sort_name = sort_col.to_string();
        // Collect (numeric key, global position). Bail to the general sort on a
        // NULL or non-numeric column so their SQL ordering stays correct.
        let mut keyed: Vec<(f64, usize)> = Vec::with_capacity(store.row_count(table));
        let mut pos = 0usize;
        for batch in batches.iter() {
            macro_rules! push_keys {
                ($v:expr, $conv:expr) => {{
                    for o in $v {
                        match o {
                            Some(x) => {
                                keyed.push(($conv(*x), pos));
                                pos += 1;
                            }
                            None => return None,
                        }
                    }
                }};
            }
            match batch.column(&sort_name) {
                Some(ColumnData::Int32(v)) => push_keys!(v, |x: i32| x as f64),
                Some(ColumnData::Int64(v)) => push_keys!(v, |x: i64| x as f64),
                Some(ColumnData::Float64(v)) => push_keys!(v, |x: f64| x),
                Some(ColumnData::Bool(v)) => push_keys!(v, |x: bool| x as i64 as f64),
                _ => return None,
            }
        }
        let n = keyed.len();
        let cmp = |a: &(f64, usize), b: &(f64, usize)| {
            if desc {
                b.0.total_cmp(&a.0)
            } else {
                a.0.total_cmp(&b.0)
            }
        };
        // O(n) partition for the top-k, then O(k log k) sort of just those k.
        if k < n {
            keyed.select_nth_unstable_by(k - 1, cmp);
            keyed.truncate(k);
        }
        keyed.sort_unstable_by(cmp);
        let positions: Vec<usize> = keyed.iter().map(|(_, p)| *p).collect();
        Some(fetch_rows_by_positions(&batches, &positions))
    }

    fn fast_sum_f64(&self, table: &str, col_idx: usize) -> Option<(f64, usize)> {
        self.flush_write_buffer(table);
        let col_name = col_idx.to_string();
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        let (total, n) = batches.iter().fold((0.0f64, 0usize), |(s, c), batch| {
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
        let read = batches_for_read(&store, table);
        let batches = read.refs();

        // Collect the key and value columns across all batches.
        // Keys are accumulated as strings for hashing/grouping, but we remember
        // the underlying column kind so the native Value type is preserved on the
        // way out (e.g. an INTEGER group key must come back as Int*, ordered
        // numerically — not as Text, which would order lexicographically).
        let mut key_vec: Vec<Option<String>> = Vec::new();
        let mut val_vec: Vec<Option<f64>> = Vec::new();
        let mut key_kind = ColumnarKeyKind::Text;

        for batch in batches.iter() {
            let n = batch.row_count;
            // Key column — text-converted for grouping; kind tracked for output.
            match batch.column(&key_col_name) {
                Some(ColumnData::Text(v)) => key_vec.extend(v.iter().cloned()),
                Some(ColumnData::Int32(v)) => {
                    key_kind = ColumnarKeyKind::Int32;
                    key_vec.extend(v.iter().map(|o| o.map(|n| n.to_string())))
                }
                Some(ColumnData::Int64(v)) => {
                    key_kind = ColumnarKeyKind::Int64;
                    key_vec.extend(v.iter().map(|o| o.map(|n| n.to_string())))
                }
                Some(ColumnData::Float64(v)) => {
                    key_kind = ColumnarKeyKind::Float64;
                    key_vec.extend(v.iter().map(|o| o.map(|n| n.to_string())))
                }
                Some(ColumnData::Bool(v)) => {
                    key_kind = ColumnarKeyKind::Bool;
                    key_vec.extend(v.iter().map(|o| o.map(|b| b.to_string())))
                }
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
                    .map(|g| (key_kind.to_value(g.key), g.count as i64, g.avg))
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
                    .map(|(k, cnt)| (key_kind.to_value(k), cnt, None))
                    .collect(),
            )
        }
    }

    fn fast_count_filtered(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<usize> {
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        // For replacing_mergetree, count after dedup so superseded versions
        // don't inflate the result. Raw tables borrow (no clone).
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        let filter_col_name = filter_col.to_string();
        let count = batches
            .iter()
            .map(|batch| match batch.column(&filter_col_name) {
                Some(col) => eq_mask(col, filter_val).iter().filter(|&&b| b).count(),
                None => 0,
            })
            .sum();
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
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        let (sum, count) = batches.iter().fold((0.0f64, 0usize), |(s, c), batch| {
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

    fn fast_count_cmp(
        &self,
        table: &str,
        filter_col: usize,
        op: FilterOp,
        filter_val: &Value,
    ) -> Option<usize> {
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        let filter_col_name = filter_col.to_string();
        let count = batches
            .iter()
            .map(|batch| match batch.column(&filter_col_name) {
                Some(col) => cmp_mask(col, op, filter_val).iter().filter(|&&b| b).count(),
                None => 0,
            })
            .sum();
        Some(count)
    }

    fn fast_sum_f64_cmp(
        &self,
        table: &str,
        val_col: usize,
        filter_col: usize,
        op: FilterOp,
        filter_val: &Value,
    ) -> Option<(f64, usize)> {
        self.flush_write_buffer(table);
        let val_col_name = val_col.to_string();
        let filter_col_name = filter_col.to_string();
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        let (sum, count) = batches.iter().fold((0.0f64, 0usize), |(s, c), batch| {
            let filter_data = match batch.column(&filter_col_name) {
                Some(d) => d,
                None => return (s, c),
            };
            let mask = cmp_mask(filter_data, op, filter_val);
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
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        for batch in batches.iter() {
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
        let read = batches_for_read(&store, table);
        let batches = read.refs();
        for batch in batches.iter() {
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
    ) -> Option<(Vec<Row>, usize)> {
        self.flush_write_buffer(table);
        let store = self.store.read();
        if !store.table_exists(table) {
            return None;
        }
        // SELECT path — apply replacing_mergetree dedup so callers see one
        // row per logical PK. No-op for plain tables.
        let batches = store.batches_all_for_select(table);
        Some(batches_to_rows_where_eq(&batches, filter_col, filter_val))
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
            let refs: Vec<(&str, Vec<Row>)> =
                snap.iter().map(|(n, r)| (n.as_str(), r.clone())).collect();
            wal.checkpoint(&refs)
                .map_err(|e| StorageError::Io(e.to_string()))?;
        }
        Ok(())
    }

    async fn make_durable(&self) -> Result<(), StorageError> {
        // Mutations are appended to the WAL as they happen (inserts as row
        // batches, updates/deletes as snapshot rewrites) but only `write()`n
        // into the OS page cache. The commit point fsyncs via group commit.
        if let Some(wal) = &self.wal {
            wal.group_sync()
                .map_err(|e| StorageError::Io(e.to_string()))?;
        }
        Ok(())
    }

    fn durability_pending(&self) -> bool {
        self.wal.as_ref().is_some_and(|w| w.is_dirty())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64, name: &str, amount: f64) -> Row {
        vec![
            Value::Int64(id),
            Value::Text(name.to_string()),
            Value::Float64(amount),
        ]
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
    async fn scan_limit_early_exit_matches_prefix() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        for i in 1..=50 {
            eng.insert("t", row(i, "x", i as f64)).await.unwrap();
        }
        let full = eng.scan("t").await.unwrap();
        for n in [0usize, 1, 7, 50, 100] {
            let lim = eng.scan_limit("t", n).await.unwrap();
            assert_eq!(lim, full[..n.min(full.len())], "n={n}");
        }
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
        eng.update("t", &[(0, row(99, "updated", 99.0))])
            .await
            .unwrap();
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
        eng.insert("t", vec![Value::Text("a".into()), Value::Float64(10.0)])
            .await
            .unwrap();
        eng.insert("t", vec![Value::Text("a".into()), Value::Float64(20.0)])
            .await
            .unwrap();
        eng.insert("t", vec![Value::Text("b".into()), Value::Float64(30.0)])
            .await
            .unwrap();
        let groups = eng.fast_group_by("t", 0, Some(1)).unwrap();
        let a = groups
            .iter()
            .find(|(k, _, _)| k == &Value::Text("a".into()))
            .unwrap();
        let b = groups
            .iter()
            .find(|(k, _, _)| k == &Value::Text("b".into()))
            .unwrap();
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
        let result = eng
            .index_lookup_sync("t", "t_id_idx", &Value::Int64(2))
            .unwrap();
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
            eng.insert("t", vec![Value::Int64(i), Value::Float64(i as f64)])
                .await
                .unwrap();
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
            eng.insert("t", vec![Value::Int64(i), Value::Float64(i as f64)])
                .await
                .unwrap();
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
            eng.insert_batch(
                "orders",
                vec![
                    row(1, "alice", 10.0),
                    row(2, "bob", 20.0),
                    row(3, "carol", 30.0),
                ],
            )
            .await
            .unwrap();
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
            assert!(
                eng.scan("t").await.is_err(),
                "table should not exist after drop+recovery"
            );
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

#[cfg(test)]
mod merge_tree_pruning_tests {
    use super::*;
    use crate::columnar::{CmpOp, MergeStrategy, ScalarValue};
    use crate::storage::granule_stats::FilterPredicate;
    use crate::types::DataType;

    /// A MergeTree whose parts cover distinct, non-overlapping `ts` ranges.
    ///
    /// Note the column name used below is `"1"`, not `"ts"`. `rows_to_batch`
    /// names columns by POSITION, so every batch in this engine — including
    /// every MergeTree part — carries `"0"`, `"1"`, … and its zone maps are
    /// keyed by those. A query asks to prune on `ts`, finds no statistics under
    /// that name, and skips nothing; `sort_by_pk` looks up its key the same way
    /// and silently does not sort. That name mismatch, not the pruning logic,
    /// is what makes a declared `ORDER BY` inert — see the `tracing::warn!` in
    /// `execute_create_table`. These tests pin the mechanism so that fixing the
    /// naming contract turns pruning on rather than having to build it.
    async fn tree_with_parts() -> ColumnarStorageEngine {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("spans").await.unwrap();
        eng.register_merge_tree("spans", vec!["1".into()], MergeStrategy::Default);
        eng.store_table_schema(
            "spans",
            &[("id".into(), DataType::Int64), ("ts".into(), DataType::Int64)],
        );
        // A part is cut when the write buffer flushes, so read between inserts
        // to force separate parts. Sizes differ so the size-tiered merge policy
        // does not immediately consolidate them into one.
        for (decade, n) in [(0i64, 4usize), (1, 9), (2, 20), (3, 45)] {
            let rows: Vec<Row> = (0..n as i64)
                .map(|i| vec![Value::Int64(decade * 100 + i), Value::Int64(decade * 1000 + i)])
                .collect();
            eng.insert_batch("spans", rows).await.unwrap();
            eng.scan("spans").await.unwrap();
        }
        eng
    }

    fn total_rows(eng: &ColumnarStorageEngine) -> usize {
        eng.store.read().row_count("spans")
    }

    /// A window outside every part reads nothing — the parts are skipped by
    /// their zone maps, not read and then filtered.
    #[tokio::test]
    async fn test_window_outside_every_part_reads_no_rows() {
        let eng = tree_with_parts().await;
        let pred = FilterPredicate::Between {
            min: Value::Int64(90_000),
            max: Value::Int64(99_000),
        };
        let rows = eng
            .scan_projected_pruned("spans", &[0, 1], None, Some(("1", &pred)))
            .await
            .unwrap();
        assert!(
            rows.is_empty(),
            "every part's range is below the window, so all should be pruned; read {} rows",
            rows.len()
        );
    }

    /// A window covering part of the table reads less than the whole of it, and
    /// never drops a row the window covers.
    #[tokio::test]
    async fn test_window_prunes_without_losing_rows() {
        let eng = tree_with_parts().await;
        let all = total_rows(&eng);
        let pred = FilterPredicate::Between {
            min: Value::Int64(3000),
            max: Value::Int64(3044),
        };
        let pruned = eng
            .scan_projected_pruned("spans", &[0, 1], None, Some(("1", &pred)))
            .await
            .unwrap();
        let full = eng.scan_projected("spans", &[0, 1], None).await.unwrap();
        assert_eq!(full.len(), all);
        assert!(
            pruned.len() < full.len(),
            "at least one part is provably outside the window; read {} of {}",
            pruned.len(),
            full.len()
        );
        // Pruning is an optimization, never a filter.
        for r in &pruned {
            assert!(full.contains(r), "pruned scan invented a row: {r:?}");
        }
        let covered: Vec<&Row> = full
            .iter()
            .filter(|r| matches!(r[1], Value::Int64(t) if (3000..=3044).contains(&t)))
            .collect();
        for r in covered {
            assert!(pruned.contains(r), "pruning dropped a matching row: {r:?}");
        }
    }

    /// A bound the statistics cannot be compared against must read everything.
    /// `scalar_lt` answers false for every mixed pair, which made the `Gt`/`Lt`
    /// arms of `can_skip` skip EVERY part rather than none.
    #[tokio::test]
    async fn test_incomparable_bound_prunes_nothing() {
        let eng = tree_with_parts().await;
        let all = total_rows(&eng);
        for pred in [
            FilterPredicate::Between {
                min: Value::Text("3000".into()),
                max: Value::Text("3044".into()),
            },
            FilterPredicate::GreaterThan(Value::Text("3000".into())),
            FilterPredicate::LessThan(Value::Text("3000".into())),
        ] {
            let rows = eng
                .scan_projected_pruned("spans", &[0, 1], None, Some(("1", &pred)))
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                all,
                "a text bound against an integer column proves nothing, so nothing may be \
                 skipped — got {} of {all} rows for {pred:?}",
                rows.len()
            );
        }
    }

    /// The zone map is consulted below the scan, so a pruned read costs less
    /// than an unpruned one even before any filter runs.
    #[tokio::test]
    async fn test_pruning_happens_below_the_scan() {
        let eng = tree_with_parts().await;
        let store = eng.store.read();
        let bounds = [
            (CmpOp::Gte, ScalarValue::Int64(90_000)),
            (CmpOp::Lte, ScalarValue::Int64(99_000)),
        ];
        let kept = store
            .batches_pruned_shared("spans", "1", &bounds)
            .expect("registered as a MergeTree");
        assert!(
            kept.is_empty(),
            "no part can match, so the scan should be handed no batches at all"
        );
    }
}

#[cfg(test)]
mod intra_part_narrowing_tests {
    use super::*;
    use crate::columnar::{CmpOp, MergeStrategy, ScalarValue};
    use crate::storage::granule_stats::FilterPredicate;
    use crate::types::DataType;

    /// One MergeTree part holding a wide, sorted key range, so a window landing
    /// inside it has somewhere to be narrowed to.
    async fn one_wide_part() -> ColumnarStorageEngine {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("spans").await.unwrap();
        eng.register_merge_tree("spans", vec!["1".into()], MergeStrategy::Default);
        eng.store_table_schema(
            "spans",
            &[("id".into(), DataType::Int64), ("ts".into(), DataType::Int64)],
        );
        let rows: Vec<Row> = (0..5_000i64)
            .map(|i| vec![Value::Int64(i), Value::Int64(i * 10)])
            .collect();
        eng.insert_batch("spans", rows).await.unwrap();
        eng.scan("spans").await.unwrap(); // force the part to be cut
        eng
    }

    /// A window inside a part reads only the rows it covers — not the part.
    ///
    /// Zone maps answer "can this part match?", never "where in it". A part of
    /// 5,000 rows and a window over 11 of them were the same amount of work
    /// until the sorted key column was searched directly.
    #[tokio::test]
    async fn test_a_window_inside_a_part_reads_only_that_window() {
        let eng = one_wide_part().await;
        let pred = FilterPredicate::Between {
            min: Value::Int64(20_000),
            max: Value::Int64(20_100),
        };
        let rows = eng
            .scan_projected_pruned("spans", &[0, 1], None, Some(("1", &pred)))
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            11,
            "ts 20000..=20100 step 10 is 11 rows; reading {} means the whole \
             part was materialized and filtered above",
            rows.len()
        );
        for r in &rows {
            let Value::Int64(ts) = r[1] else {
                panic!("expected ts")
            };
            assert!((20_000..=20_100).contains(&ts), "row outside the window: {ts}");
        }
    }

    /// Narrowing must never drop a row the window covers, at either edge or
    /// past either end of the part.
    #[tokio::test]
    async fn test_narrowing_keeps_every_covered_row() {
        let eng = one_wide_part().await;
        let full = eng.scan_projected("spans", &[0, 1], None).await.unwrap();
        assert_eq!(full.len(), 5_000);

        for (label, lo, hi) in [
            ("first row", 0, 0),
            ("straddling the start", -500, 30),
            ("straddling the end", 49_980, 60_000),
            ("last row", 49_990, 49_990),
            ("whole part", -1, 50_000),
            ("empty, between two keys", 15, 19),
        ] {
            let pred = FilterPredicate::Between {
                min: Value::Int64(lo),
                max: Value::Int64(hi),
            };
            let got = eng
                .scan_projected_pruned("spans", &[0, 1], None, Some(("1", &pred)))
                .await
                .unwrap();
            let expected: Vec<&Row> = full
                .iter()
                .filter(|r| matches!(r[1], Value::Int64(ts) if (lo..=hi).contains(&ts)))
                .collect();
            assert_eq!(
                got.len(),
                expected.len(),
                "{label}: window [{lo}, {hi}] covers {} rows, narrowing returned {}",
                expected.len(),
                got.len()
            );
            for r in expected {
                assert!(got.contains(r), "{label}: narrowing dropped {r:?}");
            }
        }
    }

    /// A bound the column cannot be compared against, or a predicate on some
    /// other column, must fall back to the whole part rather than guess.
    #[tokio::test]
    async fn test_unnarrowable_predicates_read_the_part() {
        let eng = one_wide_part().await;
        for (label, col, pred) in [
            (
                "text bound against an integer key",
                "1",
                FilterPredicate::Between {
                    min: Value::Text("20000".into()),
                    max: Value::Text("20100".into()),
                },
            ),
            (
                "predicate on a column the part is not sorted by",
                "0",
                FilterPredicate::Between {
                    min: Value::Int64(0),
                    max: Value::Int64(4_999),
                },
            ),
        ] {
            let rows = eng
                .scan_projected_pruned("spans", &[0, 1], None, Some((col, &pred)))
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                5_000,
                "{label}: nothing is provable here, so the whole part must be read"
            );
        }
    }

    /// The search is over the sorted column itself, so the cost of a narrow
    /// window does not grow with the size of the part it lands in.
    #[tokio::test]
    async fn test_narrowing_is_sublinear_in_part_size() {
        let eng = ColumnarStorageEngine::new();
        eng.create_table("t").await.unwrap();
        eng.register_merge_tree("t", vec!["1".into()], MergeStrategy::Default);
        eng.store_table_schema(
            "t",
            &[("id".into(), DataType::Int64), ("k".into(), DataType::Int64)],
        );
        let rows: Vec<Row> = (0..50_000i64)
            .map(|i| vec![Value::Int64(i), Value::Int64(i)])
            .collect();
        eng.insert_batch("t", rows).await.unwrap();
        eng.scan("t").await.unwrap();

        let bounds = [
            (CmpOp::Gte, ScalarValue::Int64(25_000)),
            (CmpOp::Lte, ScalarValue::Int64(25_004)),
        ];
        let slices = eng
            .store
            .read()
            .batches_pruned_slices("t", "1", &bounds)
            .expect("registered as a MergeTree");
        let scanned: usize = slices.iter().map(|s| s.len()).sum();
        assert_eq!(
            scanned, 5,
            "a 5-row window in a 50,000-row part should hand the scan 5 rows, not {scanned}"
        );
    }
}
