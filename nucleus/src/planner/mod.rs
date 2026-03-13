//! Query planner — cost-based plan selection with real optimization.
//!
//! Principle 4: The planner uses a unified cost model where every access method
//! reports its cost in the same units. The planner compares costs and chooses
//! the cheapest execution plan.
//!
//! Supports:
//!   - Sequential scan (full table scan)
//!   - Index scan (when a B-tree index matches a WHERE predicate)
//!   - Hash join (for equi-joins on larger tables)
//!   - Nested loop join (fallback for non-equi joins)
//!   - Predicate pushdown through joins and projections
//!   - Join reordering heuristics (smaller tables first)
//!   - Index selection (match WHERE columns to available indexes)
//!   - Basic cost estimation model (row count estimates, selectivity)
//!   - ANALYZE statistics (row count, distinct values per column)
//!   - EXPLAIN command (shows the chosen plan)

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::catalog::{Catalog, IndexType};
use crate::cost::{self, AccessMethod};

// ============================================================================
// Cost model
// ============================================================================

/// Estimated cost of an operation, in arbitrary cost units.
/// Lower is cheaper. Costs are comparable across different access methods.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);

impl Cost {
    pub fn zero() -> Self {
        Cost(0.0)
    }
}

impl std::ops::Add for Cost {
    type Output = Cost;
    fn add(self, other: Cost) -> Cost {
        Cost(self.0 + other.0)
    }
}

/// Base cost constants (tuned for NVMe SSDs).
pub const SEQ_PAGE_COST: f64 = 1.0;
pub const RANDOM_PAGE_COST: f64 = 1.1;
pub const CPU_TUPLE_COST: f64 = 0.01;
pub const CPU_INDEX_COST: f64 = 0.005;
pub const CPU_OPERATOR_COST: f64 = 0.0025;
pub const CPU_HASH_COST: f64 = 0.02;
pub const HASH_BUILD_FACTOR: f64 = 2.0;

// ============================================================================
// Plan nodes
// ============================================================================

/// A query execution plan.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum PlanNode {
    /// Sequential scan: read all pages of a table.
    SeqScan {
        table: String,
        estimated_rows: usize,
        estimated_cost: Cost,
        filter: Option<String>,
        /// Pre-parsed filter expression (avoids re-parsing at execution time).
        filter_expr: Option<sqlparser::ast::Expr>,
        /// Pushed-down limit hint: stop scanning after this many rows.
        scan_limit: Option<usize>,
        /// Pushed-down projection: 0-based column indices into the table schema.
        /// When set, the executor uses `scan_projected()` for zero-copy partial
        /// deserialization, returning only the listed columns in projection order.
        projection: Option<Vec<usize>>,
    },
    /// Index scan: use a B-tree index to find matching rows.
    IndexScan {
        table: String,
        index_name: String,
        estimated_rows: usize,
        estimated_cost: Cost,
        /// Equality lookup: "col = val".  None when doing a range scan.
        lookup_key: Option<String>,
        /// Pre-parsed lookup key expression (avoids re-parsing at execution time).
        lookup_key_expr: Option<sqlparser::ast::Expr>,
        /// Range scan lower bound value string (inclusive for the btree; exclusive bounds
        /// handled by `range_predicate` post-filter).  None = no lower bound.
        range_lo: Option<String>,
        /// Pre-parsed range lower bound expression.
        range_lo_expr: Option<sqlparser::ast::Expr>,
        /// Range scan upper bound value string. None = no upper bound.
        range_hi: Option<String>,
        /// Pre-parsed range upper bound expression.
        range_hi_expr: Option<sqlparser::ast::Expr>,
        /// Original predicate expression string applied as a post-filter after the
        /// range scan to enforce strict (<, >) bounds and any residual conditions.
        range_predicate: Option<String>,
        /// Pre-parsed range predicate expression.
        range_predicate_expr: Option<sqlparser::ast::Expr>,
    },
    /// Nested loop join.
    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinPlanType,
        estimated_rows: usize,
        estimated_cost: Cost,
        condition: Option<String>,
        /// Pre-parsed join condition expression.
        condition_expr: Option<sqlparser::ast::Expr>,
    },
    /// Hash join for equi-joins.
    HashJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinPlanType,
        hash_keys: Vec<String>,
        estimated_rows: usize,
        estimated_cost: Cost,
    },
    /// Filter node (predicate applied after scan).
    Filter {
        input: Box<PlanNode>,
        predicate: String,
        /// Pre-parsed predicate expression (avoids re-parsing at execution time).
        predicate_expr: Option<sqlparser::ast::Expr>,
        estimated_rows: usize,
        estimated_cost: Cost,
    },
    /// Sort (ORDER BY).
    Sort {
        input: Box<PlanNode>,
        keys: Vec<String>,
        estimated_cost: Cost,
    },
    /// Limit / Offset.
    Limit {
        input: Box<PlanNode>,
        limit: Option<usize>,
        offset: Option<usize>,
        estimated_cost: Cost,
    },
    /// Hash aggregate (GROUP BY).
    HashAggregate {
        input: Box<PlanNode>,
        group_keys: Vec<String>,
        aggregates: Vec<String>,
        estimated_rows: usize,
        estimated_cost: Cost,
    },
    /// Projection (SELECT columns/expressions).
    Project {
        input: Box<PlanNode>,
        columns: Vec<String>,
        estimated_cost: Cost,
    },
    /// Simple aggregate (COUNT/SUM without GROUP BY).
    Aggregate {
        input: Box<PlanNode>,
        aggregates: Vec<String>,
        estimated_cost: Cost,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinPlanType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl PlanNode {
    pub fn total_cost(&self) -> Cost {
        match self {
            PlanNode::SeqScan { estimated_cost, .. }
            | PlanNode::IndexScan { estimated_cost, .. }
            | PlanNode::NestedLoopJoin { estimated_cost, .. }
            | PlanNode::HashJoin { estimated_cost, .. }
            | PlanNode::Filter { estimated_cost, .. }
            | PlanNode::Sort { estimated_cost, .. }
            | PlanNode::Limit { estimated_cost, .. }
            | PlanNode::HashAggregate { estimated_cost, .. }
            | PlanNode::Project { estimated_cost, .. }
            | PlanNode::Aggregate { estimated_cost, .. } => *estimated_cost,
        }
    }

    pub fn estimated_rows(&self) -> usize {
        match self {
            PlanNode::SeqScan { estimated_rows, .. }
            | PlanNode::IndexScan { estimated_rows, .. }
            | PlanNode::NestedLoopJoin { estimated_rows, .. }
            | PlanNode::HashJoin { estimated_rows, .. }
            | PlanNode::Filter { estimated_rows, .. }
            | PlanNode::HashAggregate { estimated_rows, .. } => *estimated_rows,
            PlanNode::Sort { input, .. } => input.estimated_rows(),
            PlanNode::Limit { limit, input, .. } => {
                limit.unwrap_or(input.estimated_rows()).min(input.estimated_rows())
            }
            PlanNode::Project { input, .. } => input.estimated_rows(),
            PlanNode::Aggregate { .. } => 1,
        }
    }

    pub fn table_name(&self) -> Option<&str> {
        match self {
            PlanNode::SeqScan { table, .. } | PlanNode::IndexScan { table, .. } => Some(table),
            PlanNode::Filter { input, .. } => input.table_name(),
            _ => None,
        }
    }
}

// ============================================================================
// Plan display (for EXPLAIN)
// ============================================================================

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indented(f, 0)
    }
}

impl PlanNode {
    fn fmt_indented(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let pad = " ".repeat(indent);
        match self {
            PlanNode::SeqScan { table, estimated_rows, estimated_cost, filter, projection, .. } => {
                write!(f, "{pad}Seq Scan on {table} (cost={:.2} rows={estimated_rows})", estimated_cost.0)?;
                if let Some(flt) = filter {
                    write!(f, "\n{pad}  Filter: {flt}")?;
                }
                if let Some(proj) = projection {
                    write!(f, "\n{pad}  Projection: cols {proj:?}")?;
                }
            }
            PlanNode::IndexScan { table, index_name, estimated_rows, estimated_cost, lookup_key, range_lo, range_hi, range_predicate, .. } => {
                write!(f, "{pad}Index Scan using {index_name} on {table} (cost={:.2} rows={estimated_rows})", estimated_cost.0)?;
                if let Some(key) = lookup_key {
                    write!(f, "\n{pad}  Index Cond: {key}")?;
                }
                if range_lo.is_some() || range_hi.is_some() {
                    let lo = range_lo.as_deref().unwrap_or("−∞");
                    let hi = range_hi.as_deref().unwrap_or("+∞");
                    write!(f, "\n{pad}  Index Range: [{lo}, {hi}]")?;
                }
                if let Some(pred) = range_predicate {
                    write!(f, "\n{pad}  Filter: {pred}")?;
                }
            }
            PlanNode::NestedLoopJoin { left, right, join_type, estimated_rows, estimated_cost, condition, .. } => {
                write!(f, "{pad}Nested Loop {join_type:?} Join (cost={:.2} rows={estimated_rows})", estimated_cost.0)?;
                if let Some(cond) = condition {
                    write!(f, "\n{pad}  Join Cond: {cond}")?;
                }
                writeln!(f)?;
                left.fmt_indented(f, indent + 2)?;
                writeln!(f)?;
                right.fmt_indented(f, indent + 2)?;
            }
            PlanNode::HashJoin { left, right, join_type, hash_keys, estimated_rows, estimated_cost } => {
                write!(f, "{pad}Hash {join_type:?} Join (cost={:.2} rows={estimated_rows})", estimated_cost.0)?;
                if !hash_keys.is_empty() {
                    write!(f, "\n{pad}  Hash Key: {}", hash_keys.join(", "))?;
                }
                writeln!(f)?;
                left.fmt_indented(f, indent + 2)?;
                writeln!(f)?;
                right.fmt_indented(f, indent + 2)?;
            }
            PlanNode::Filter { input, predicate, estimated_rows, estimated_cost, .. } => {
                write!(f, "{pad}Filter (cost={:.2} rows={estimated_rows})", estimated_cost.0)?;
                write!(f, "\n{pad}  Predicate: {predicate}")?;
                writeln!(f)?;
                input.fmt_indented(f, indent + 2)?;
            }
            PlanNode::Sort { input, keys, estimated_cost } => {
                write!(f, "{pad}Sort (cost={:.2})\n{pad}  Sort Key: {}", estimated_cost.0, keys.join(", "))?;
                writeln!(f)?;
                input.fmt_indented(f, indent + 2)?;
            }
            PlanNode::Limit { input, limit, offset, estimated_cost } => {
                write!(f, "{pad}Limit (cost={:.2}", estimated_cost.0)?;
                if let Some(l) = limit {
                    write!(f, " limit={l}")?;
                }
                if let Some(o) = offset {
                    write!(f, " offset={o}")?;
                }
                writeln!(f, ")")?;
                input.fmt_indented(f, indent + 2)?;
            }
            PlanNode::HashAggregate { input, group_keys, estimated_rows, estimated_cost, .. } => {
                write!(f, "{pad}HashAggregate (cost={:.2} rows={estimated_rows})", estimated_cost.0)?;
                if !group_keys.is_empty() {
                    write!(f, "\n{pad}  Group Key: {}", group_keys.join(", "))?;
                }
                writeln!(f)?;
                input.fmt_indented(f, indent + 2)?;
            }
            PlanNode::Project { input, columns, estimated_cost } => {
                write!(f, "{pad}Project [{}] (cost={:.2})", columns.join(", "), estimated_cost.0)?;
                writeln!(f)?;
                input.fmt_indented(f, indent + 2)?;
            }
            PlanNode::Aggregate { input, aggregates, estimated_cost } => {
                write!(f, "{pad}Aggregate [{}] (cost={:.2})", aggregates.join(", "), estimated_cost.0)?;
                writeln!(f)?;
                input.fmt_indented(f, indent + 2)?;
            }
        }
        Ok(())
    }
}

// ============================================================================
// Cost estimation helpers
// ============================================================================

/// Estimate sequential scan cost.
pub fn estimate_seq_scan_cost(total_pages: usize, total_rows: usize) -> Cost {
    Cost(total_pages as f64 * SEQ_PAGE_COST + total_rows as f64 * CPU_TUPLE_COST)
}

/// Estimate index scan cost (point lookup via B-tree: O(log n) pages).
pub fn estimate_index_scan_cost(tree_height: usize, estimated_matches: usize) -> Cost {
    let index_cost = tree_height as f64 * RANDOM_PAGE_COST + estimated_matches as f64 * CPU_INDEX_COST;
    let tuple_cost = estimated_matches as f64 * (RANDOM_PAGE_COST + CPU_TUPLE_COST);
    Cost(index_cost + tuple_cost)
}

/// Estimate hash index scan cost (O(1) lookup — constant cost for probe).
pub fn estimate_hash_index_cost(estimated_matches: usize) -> Cost {
    // O(1) probe: one hash computation + one bucket read (no tree traversal)
    let index_cost = CPU_HASH_COST + estimated_matches as f64 * CPU_INDEX_COST;
    let tuple_cost = estimated_matches as f64 * (RANDOM_PAGE_COST + CPU_TUPLE_COST);
    Cost(index_cost + tuple_cost)
}

/// Estimate nested loop join cost.
pub fn estimate_nested_loop_cost(left_rows: usize, right_cost: Cost, result_rows: usize) -> Cost {
    Cost(left_rows as f64 * right_cost.0 + result_rows as f64 * CPU_TUPLE_COST)
}

/// Estimate hash join cost.
pub fn estimate_hash_join_cost(left_rows: usize, right_rows: usize, result_rows: usize) -> Cost {
    let build_cost = left_rows as f64 * CPU_HASH_COST * HASH_BUILD_FACTOR;
    let probe_cost = right_rows as f64 * CPU_HASH_COST;
    let output_cost = result_rows as f64 * CPU_TUPLE_COST;
    Cost(build_cost + probe_cost + output_cost)
}

/// Estimate sort cost (O(n log n)).
pub fn estimate_sort_cost(input_rows: usize, input_cost: Cost) -> Cost {
    let sort_cost = if input_rows > 0 {
        input_rows as f64 * (input_rows as f64).log2() * CPU_OPERATOR_COST
    } else {
        0.0
    };
    Cost(input_cost.0 + sort_cost)
}

/// Estimate filter cost.
pub fn estimate_filter_cost(input_rows: usize, selectivity: f64, input_cost: Cost) -> Cost {
    let filter_cost = input_rows as f64 * CPU_OPERATOR_COST;
    let _output_rows = (input_rows as f64 * selectivity) as usize;
    Cost(input_cost.0 + filter_cost)
}

// ============================================================================
// Statistics store
// ============================================================================

/// Table statistics for cost estimation.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub table_name: String,
    pub row_count: usize,
    pub page_count: usize,
    /// Per-column statistics: column name -> ColumnStats
    pub column_stats: HashMap<String, ColumnStats>,
    /// When the table was last analyzed — epoch seconds (None if never).
    pub last_analyzed: Option<u64>,
}

/// Per-column statistics collected by ANALYZE.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: usize,
    pub null_fraction: f64,
    pub avg_width: usize,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

impl TableStats {
    pub fn new(table_name: &str, row_count: usize, page_count: usize) -> Self {
        Self {
            table_name: table_name.to_string(),
            row_count,
            page_count,
            column_stats: HashMap::new(),
            last_analyzed: None,
        }
    }

    /// Estimate selectivity of an equality predicate on a column.
    pub fn equality_selectivity(&self, column: Option<&str>) -> f64 {
        if self.row_count == 0 {
            return 0.0;
        }
        if let Some(col) = column
            && let Some(stats) = self.column_stats.get(col)
                && stats.distinct_count > 0 {
                    return 1.0 / stats.distinct_count as f64;
                }
        // Default: assume 10% selectivity
        0.1
    }

    /// Estimate selectivity of a range predicate (fallback constant).
    pub fn range_selectivity(&self) -> f64 {
        0.33
    }

    /// Estimate selectivity of a range predicate using actual column statistics.
    /// `low` / `high` are the predicate bounds as f64 (None = open-ended).
    /// Falls back to 0.33 when stats are absent or non-numeric.
    pub fn estimate_range_sel(&self, column: Option<&str>, low: Option<f64>, high: Option<f64>) -> f64 {
        if let Some(col) = column
            && let Some(cs) = self.column_stats.get(col)
                && let (Some(min_s), Some(max_s)) = (&cs.min_value, &cs.max_value)
                    && let (Ok(min_v), Ok(max_v)) = (min_s.parse::<f64>(), max_s.parse::<f64>())
                        && max_v > min_v {
                            let col_range = max_v - min_v;
                            let covered = match (low, high) {
                                (Some(lo), Some(hi)) => (hi.min(max_v) - lo.max(min_v)).max(0.0),
                                (Some(lo), None)     => (max_v - lo.max(min_v)).max(0.0),
                                (None,     Some(hi)) => (hi.min(max_v) - min_v).max(0.0),
                                (None,     None)     => col_range,
                            };
                            return ((covered / col_range) * (1.0 - cs.null_fraction))
                                .clamp(0.01, 1.0);
                        }
        0.33
    }

    /// Estimate selectivity of a LIKE predicate.
    pub fn like_selectivity(&self) -> f64 {
        0.05
    }

    /// Estimate selectivity of IS NULL.
    pub fn null_selectivity(&self, column: Option<&str>) -> f64 {
        if let Some(col) = column
            && let Some(stats) = self.column_stats.get(col) {
                return stats.null_fraction;
            }
        0.01
    }
}

/// Global statistics store, updated by ANALYZE.
pub struct StatsStore {
    stats: RwLock<HashMap<String, TableStats>>,
}

impl Default for StatsStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsStore {
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, table: &str) -> Option<TableStats> {
        self.stats.read().await.get(table).cloned()
    }

    pub async fn update(&self, stats: TableStats) {
        self.stats.write().await.insert(stats.table_name.clone(), stats);
    }

    /// Get or create default stats for a table using catalog info.
    pub async fn get_or_default(&self, table: &str, catalog: &Catalog) -> TableStats {
        if let Some(s) = self.get(table).await {
            return s;
        }
        // Create default stats from catalog — use sync cache to avoid async lock.
        let row_count = if catalog.get_table_cached(table).is_some() {
            1000 // default estimate
        } else {
            0
        };
        TableStats::new(table, row_count, (row_count / 100).max(1))
    }

    /// Persist all statistics to a JSON file (atomic write).
    pub async fn save(&self, path: &std::path::Path) -> Result<(), String> {
        let guard = self.stats.read().await;
        if guard.is_empty() {
            return Ok(());
        }
        let ser: Vec<TableStatsSer> = guard.values().map(|s| TableStatsSer {
            table_name: s.table_name.clone(),
            row_count: s.row_count,
            page_count: s.page_count,
            column_stats: s.column_stats.iter().map(|(k, v)| {
                (k.clone(), ColumnStatsSer {
                    distinct_count: v.distinct_count,
                    null_fraction: v.null_fraction,
                    avg_width: v.avg_width,
                    min_value: v.min_value.clone(),
                    max_value: v.max_value.clone(),
                })
            }).collect(),
            last_analyzed: s.last_analyzed,
        }).collect();
        drop(guard);

        let json = serde_json::to_string_pretty(&ser)
            .map_err(|e| format!("serialize stats: {e}"))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json.as_bytes()).map_err(|e| format!("write stats: {e}"))?;
        std::fs::rename(&tmp, path).map_err(|e| format!("rename stats: {e}"))?;
        Ok(())
    }

    /// Load statistics from a JSON file. Returns the number of tables loaded.
    pub async fn load(&self, path: &std::path::Path) -> Result<usize, String> {
        if !path.exists() {
            return Ok(0);
        }
        let json = std::fs::read_to_string(path)
            .map_err(|e| format!("read stats: {e}"))?;
        let entries: Vec<TableStatsSer> = serde_json::from_str(&json)
            .map_err(|e| format!("parse stats: {e}"))?;
        let count = entries.len();
        let mut guard = self.stats.write().await;
        for e in entries {
            let cs = e.column_stats.into_iter().map(|(k, v)| {
                (k, ColumnStats {
                    distinct_count: v.distinct_count,
                    null_fraction: v.null_fraction,
                    avg_width: v.avg_width,
                    min_value: v.min_value,
                    max_value: v.max_value,
                })
            }).collect();
            guard.insert(e.table_name.clone(), TableStats {
                table_name: e.table_name,
                row_count: e.row_count,
                page_count: e.page_count,
                column_stats: cs,
                last_analyzed: e.last_analyzed,
            });
        }
        Ok(count)
    }
}

/// Serializable form of [`TableStats`] for JSON persistence.
#[derive(serde::Serialize, serde::Deserialize)]
struct TableStatsSer {
    table_name: String,
    row_count: usize,
    page_count: usize,
    column_stats: HashMap<String, ColumnStatsSer>,
    last_analyzed: Option<u64>,
}

/// Serializable form of [`ColumnStats`].
#[derive(serde::Serialize, serde::Deserialize)]
struct ColumnStatsSer {
    distinct_count: usize,
    null_fraction: f64,
    avg_width: usize,
    min_value: Option<String>,
    max_value: Option<String>,
}

impl fmt::Debug for StatsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatsStore").finish()
    }
}

// ============================================================================
// Predicate analysis
// ============================================================================

/// Extract a numeric literal as f64 from an expression (for range bound estimation).
fn extract_literal_f64(expr: &sqlparser::ast::Expr) -> Option<f64> {
    if let sqlparser::ast::Expr::Value(v) = expr
        && let sqlparser::ast::Value::Number(n, _) = &v.value {
            return n.parse::<f64>().ok();
        }
    None
}

/// Extract the column name from a simple expression like `col = value` or `table.col = value`.
pub fn extract_column_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => Some(ident.value.clone()),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => {
            idents.last().map(|i| i.value.clone())
        }
        _ => None,
    }
}

/// Describes one side of a range bound extracted from a predicate.
#[derive(Debug)]
struct RangeBound {
    /// Serialized value string (the literal side of the comparison).
    val: String,
    /// Whether this bound is a lower bound (true) or upper bound (false).
    is_lo: bool,
}

/// If `expr` is a range comparison (`<`, `<=`, `>`, `>=`) on a column against a literal,
/// return `(col_name, RangeBound)`.  Returns None for equality, LIKE, or other forms.
fn extract_range_bound(expr: &sqlparser::ast::Expr) -> Option<(String, RangeBound)> {
    use sqlparser::ast::BinaryOperator;
    if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr {
        // col op literal
        if let Some(col) = extract_column_name(left) {
            let (is_lo, val) = match op {
                BinaryOperator::Gt | BinaryOperator::GtEq => (true, right.to_string()),
                BinaryOperator::Lt | BinaryOperator::LtEq => (false, right.to_string()),
                _ => return None,
            };
            return Some((col, RangeBound { val, is_lo }));
        }
        // literal op col (flipped)
        if let Some(col) = extract_column_name(right) {
            let (is_lo, val) = match op {
                // val > col  →  col < val  →  upper bound
                BinaryOperator::Gt | BinaryOperator::GtEq => (false, left.to_string()),
                // val < col  →  col > val  →  lower bound
                BinaryOperator::Lt | BinaryOperator::LtEq => (true, left.to_string()),
                _ => return None,
            };
            return Some((col, RangeBound { val, is_lo }));
        }
    }
    None
}

/// Scan `predicates` looking for range comparisons that bracket the same column.
///
/// Returns `(col, lo_val, hi_val, combined_predicate_str)` for the first column
/// that has both a lower AND an upper bound, or `None` if no such pair exists.
///
/// Only columns listed in `indexed_cols` (BTree index column names) are considered.
pub fn find_range_scan_opportunity(
    predicates: &[&sqlparser::ast::Expr],
    indexed_cols: &[String],
) -> Option<(String, String, String, String)> {
    use std::collections::HashMap;

    // Fast path: handle BETWEEN expressions directly (single AST node with both bounds).
    for pred in predicates {
        if let sqlparser::ast::Expr::Between { expr, negated, low, high } = pred
            && !negated
        {
            if let Some(col) = extract_column_name(expr) {
                if indexed_cols.iter().any(|ic| ic.eq_ignore_ascii_case(&col)) {
                    let lo_val = low.to_string();
                    let hi_val = high.to_string();
                    let pred_str = pred.to_string();
                    return Some((col.to_lowercase(), lo_val, hi_val, pred_str));
                }
            }
        }
    }

    // col → (lo_val, hi_val, predicate_strings)
    let mut lo_map: HashMap<String, String> = HashMap::new();
    let mut hi_map: HashMap<String, String> = HashMap::new();
    let mut pred_map: HashMap<String, Vec<String>> = HashMap::new();

    for pred in predicates {
        if let Some((col, bound)) = extract_range_bound(pred) {
            // Only consider columns with a BTree index.
            if !indexed_cols.iter().any(|ic| ic.eq_ignore_ascii_case(&col)) {
                continue;
            }
            let key = col.to_lowercase();
            pred_map.entry(key.clone()).or_default().push(pred.to_string());
            if bound.is_lo {
                lo_map.entry(key).or_insert(bound.val);
            } else {
                hi_map.entry(key).or_insert(bound.val);
            }
        }
    }

    // Return the first column that has both bounds.
    for (key, lo_val) in &lo_map {
        if let Some(hi_val) = hi_map.get(key) {
            let preds = pred_map.get(key).cloned().unwrap_or_default();
            let pred_str = preds.join(" AND ");
            // Return original-case column name (from the predicate).
            return Some((key.clone(), lo_val.clone(), hi_val.clone(), pred_str));
        }
    }
    None
}

/// Check if an expression is a simple equality predicate (col = literal).
pub fn is_equality_predicate(expr: &sqlparser::ast::Expr) -> Option<(String, String)> {
    if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr
        && matches!(op, sqlparser::ast::BinaryOperator::Eq) {
            if let Some(col) = extract_column_name(left) {
                return Some((col, right.to_string()));
            }
            if let Some(col) = extract_column_name(right) {
                return Some((col, left.to_string()));
            }
        }
    None
}

/// Check if an expression is an equi-join condition (left_table.col = right_table.col).
pub fn is_equi_join_condition(expr: &sqlparser::ast::Expr) -> Option<(String, String, String, String)> {
    if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr
        && matches!(op, sqlparser::ast::BinaryOperator::Eq)
            && let (
                sqlparser::ast::Expr::CompoundIdentifier(left_idents),
                sqlparser::ast::Expr::CompoundIdentifier(right_idents),
            ) = (left.as_ref(), right.as_ref())
                && left_idents.len() == 2 && right_idents.len() == 2 {
                    return Some((
                        left_idents[0].value.clone(),
                        left_idents[1].value.clone(),
                        right_idents[0].value.clone(),
                        right_idents[1].value.clone(),
                    ));
                }
    None
}

/// Split a conjunction (AND chain) into individual predicates.
pub fn split_conjunction(expr: &sqlparser::ast::Expr) -> Vec<&sqlparser::ast::Expr> {
    let mut result = Vec::new();
    split_conjunction_inner(expr, &mut result);
    result
}

fn split_conjunction_inner<'a>(
    expr: &'a sqlparser::ast::Expr,
    out: &mut Vec<&'a sqlparser::ast::Expr>,
) {
    if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr
        && matches!(op, sqlparser::ast::BinaryOperator::And) {
            split_conjunction_inner(left, out);
            split_conjunction_inner(right, out);
            return;
        }
    if let sqlparser::ast::Expr::Nested(inner) = expr
        && let sqlparser::ast::Expr::BinaryOp { op, .. } = inner.as_ref()
            && matches!(op, sqlparser::ast::BinaryOperator::And) {
                split_conjunction_inner(inner, out);
                return;
            }
    out.push(expr);
}

/// Estimate selectivity of a predicate.
pub fn estimate_selectivity(
    expr: &sqlparser::ast::Expr,
    stats: &TableStats,
) -> f64 {
    match expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            match op {
                sqlparser::ast::BinaryOperator::Eq => {
                    let col = extract_column_name(left)
                        .or_else(|| extract_column_name(right));
                    stats.equality_selectivity(col.as_deref())
                }
                sqlparser::ast::BinaryOperator::NotEq => {
                    let col = extract_column_name(left)
                        .or_else(|| extract_column_name(right));
                    1.0 - stats.equality_selectivity(col.as_deref())
                }
                sqlparser::ast::BinaryOperator::Lt | sqlparser::ast::BinaryOperator::LtEq => {
                    let col = extract_column_name(left).or_else(|| extract_column_name(right));
                    let bound = extract_literal_f64(right).or_else(|| extract_literal_f64(left));
                    stats.estimate_range_sel(col.as_deref(), None, bound)
                }
                sqlparser::ast::BinaryOperator::Gt | sqlparser::ast::BinaryOperator::GtEq => {
                    let col = extract_column_name(left).or_else(|| extract_column_name(right));
                    let bound = extract_literal_f64(right).or_else(|| extract_literal_f64(left));
                    stats.estimate_range_sel(col.as_deref(), bound, None)
                }
                sqlparser::ast::BinaryOperator::And => {
                    let left_sel = estimate_selectivity(left, stats);
                    let right_sel = estimate_selectivity(right, stats);
                    left_sel * right_sel
                }
                sqlparser::ast::BinaryOperator::Or => {
                    let left_sel = estimate_selectivity(left, stats);
                    let right_sel = estimate_selectivity(right, stats);
                    (left_sel + right_sel - left_sel * right_sel).min(1.0)
                }
                _ => 0.5,
            }
        }
        sqlparser::ast::Expr::IsNull(_) => stats.null_selectivity(None),
        sqlparser::ast::Expr::IsNotNull(_) => 1.0 - stats.null_selectivity(None),
        sqlparser::ast::Expr::Like { .. } | sqlparser::ast::Expr::ILike { .. } => stats.like_selectivity(),
        sqlparser::ast::Expr::InList { list, negated, .. } => {
            let sel = list.len() as f64 * stats.equality_selectivity(None);
            if *negated { 1.0 - sel.min(1.0) } else { sel.min(1.0) }
        }
        sqlparser::ast::Expr::Between { expr, low, high, negated } => {
            let col = extract_column_name(expr);
            let lo_val = extract_literal_f64(low);
            let hi_val = extract_literal_f64(high);
            let sel = stats.estimate_range_sel(col.as_deref(), lo_val, hi_val);
            if *negated { 1.0 - sel } else { sel }
        }
        sqlparser::ast::Expr::Nested(inner) => estimate_selectivity(inner, stats),
        _ => 0.5,
    }
}

// ============================================================================
// Query planner
// ============================================================================

/// The query planner creates optimal execution plans for SQL queries.
pub struct QueryPlanner {
    pub catalog: Arc<Catalog>,
    pub stats_store: Arc<StatsStore>,
}

impl QueryPlanner {
    pub fn new(catalog: Arc<Catalog>, stats_store: Arc<StatsStore>) -> Self {
        Self { catalog, stats_store }
    }

    /// Plan a table scan, choosing between seq scan and index scan.
    pub async fn plan_scan(
        &self,
        table: &str,
        predicates: &[&sqlparser::ast::Expr],
    ) -> PlanNode {
        let stats = self.stats_store.get_or_default(table, &self.catalog).await;

        // Use sync cached lookups to avoid async RwLock overhead on the hot path.
        // Falls back to the async path only if the sync cache misses AND the lock
        // is contended (extremely rare).
        let table_def_opt = self.catalog.get_table_cached(table);
        let indexes_opt = self.catalog.get_indexes_cached(table);

        // Check if any predicate matches an available index
        if let Some(table_def) = &table_def_opt {
            let indexes = indexes_opt.as_deref().unwrap_or(&[]);
            for pred in predicates {
                if let Some((col, val)) = is_equality_predicate(pred) {
                    // Check indexes on this table
                    for idx in indexes {
                        let idx_col = idx.columns.first().map(|c| c.as_str()).unwrap_or("");
                        if idx_col.eq_ignore_ascii_case(&col) && matches!(idx.index_type, IndexType::BTree) {
                            let is_unique = table_def
                                .primary_key_columns()
                                .map(|pk| pk.len() == 1 && pk[0].eq_ignore_ascii_case(&col))
                                .unwrap_or(false);
                            let estimated_matches = if is_unique {
                                1
                            } else {
                                (stats.row_count as f64 * stats.equality_selectivity(Some(&col))).max(1.0) as usize
                            };
                            let tree_height = ((stats.page_count as f64).log2() as usize).max(1);
                            let idx_cost = estimate_index_scan_cost(tree_height, estimated_matches);
                            let seq_cost = estimate_seq_scan_cost(stats.page_count, stats.row_count);

                            if idx_cost.0 < seq_cost.0 {
                                let lookup_str = format!("{col} = {val}");
                                let lookup_expr = parse_expr_safe(&lookup_str);
                                return PlanNode::IndexScan {
                                    table: table.to_string(),
                                    index_name: idx.name.clone(),
                                    estimated_rows: estimated_matches,
                                    estimated_cost: idx_cost,
                                    lookup_key: Some(lookup_str),
                                    lookup_key_expr: lookup_expr,
                                    range_lo: None,
                                    range_lo_expr: None,
                                    range_hi: None,
                                    range_hi_expr: None,
                                    range_predicate: None,
                                    range_predicate_expr: None,
                                };
                            }
                        }
                    }
                }
            }
        }

        // ── Range index scan ──────────────────────────────────────────────────
        // Check if predicates contain both a lower and upper bound on an indexed
        // column (e.g. `col > 10 AND col < 100`).  When found and cheaper than a
        // full seq scan, emit an IndexScan with range bounds.
        if let Some(table_def) = &table_def_opt {
            let indexes = indexes_opt.as_deref().unwrap_or(&[]);
            let btree_indexed_cols: Vec<String> = indexes
                .iter()
                .filter(|i| matches!(i.index_type, IndexType::BTree))
                .filter_map(|i| i.columns.first().cloned())
                .collect();

            if !btree_indexed_cols.is_empty()
                && let Some((col, lo_val, hi_val, range_pred_str)) =
                    find_range_scan_opportunity(predicates, &btree_indexed_cols)
            {
                // Find the index name for this column.
                let maybe_idx = indexes
                    .iter()
                    .find(|i| {
                        matches!(i.index_type, IndexType::BTree)
                            && i.columns
                                .first()
                                .map(|c| c.eq_ignore_ascii_case(&col))
                                .unwrap_or(false)
                    });

                if let Some(idx) = maybe_idx {
                    // Use 20% selectivity as a conservative range estimate.
                    // Real stats (histogram) would give a tighter bound, but
                    // even this rough estimate is almost always cheaper than a
                    // full sequential scan for selective ranges.
                    let _ = table_def; // accessed above for column check
                    let estimated_matches =
                        (stats.row_count as f64 * 0.20).max(1.0) as usize;
                    let tree_height =
                        ((stats.page_count as f64).log2() as usize).max(1);
                    let idx_cost =
                        estimate_index_scan_cost(tree_height, estimated_matches);
                    let seq_cost =
                        estimate_seq_scan_cost(stats.page_count, stats.row_count);

                    if idx_cost.0 < seq_cost.0 {
                        let lo_expr = parse_expr_safe(&lo_val);
                        let hi_expr = parse_expr_safe(&hi_val);
                        let (rp_str, rp_expr) = if range_pred_str.is_empty() {
                            (None, None)
                        } else {
                            let expr = parse_expr_safe(&range_pred_str);
                            (Some(range_pred_str), expr)
                        };
                        return PlanNode::IndexScan {
                            table: table.to_string(),
                            index_name: idx.name.clone(),
                            estimated_rows: estimated_matches,
                            estimated_cost: idx_cost,
                            lookup_key: None,
                            lookup_key_expr: None,
                            range_lo: Some(lo_val),
                            range_lo_expr: lo_expr,
                            range_hi: Some(hi_val),
                            range_hi_expr: hi_expr,
                            range_predicate: rp_str,
                            range_predicate_expr: rp_expr,
                        };
                    }
                }
            }
        }

        // Fallback: sequential scan
        let mut filter_sel = 1.0;
        for pred in predicates {
            filter_sel *= estimate_selectivity(pred, &stats);
        }
        let estimated_rows = (stats.row_count as f64 * filter_sel).max(1.0) as usize;
        let cost = estimate_seq_scan_cost(stats.page_count, stats.row_count);

        let (filter, filter_expr) = if predicates.is_empty() {
            (None, None)
        } else {
            let filter_str = predicates.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(" AND ");
            let expr = combine_predicate_exprs(predicates);
            (Some(filter_str), expr)
        };

        PlanNode::SeqScan {
            table: table.to_string(),
            estimated_rows,
            estimated_cost: cost,
            filter,
            filter_expr,
            scan_limit: None,
            projection: None,
        }
    }

    /// Classify a WHERE predicate into a cost-model PredicateType.
    fn classify_predicate(expr: &sqlparser::ast::Expr) -> Option<cost::PredicateType> {
        match expr {
            sqlparser::ast::Expr::BinaryOp { op, .. } => match op {
                sqlparser::ast::BinaryOperator::Eq => Some(cost::PredicateType::Equality),
                sqlparser::ast::BinaryOperator::Lt
                | sqlparser::ast::BinaryOperator::LtEq
                | sqlparser::ast::BinaryOperator::Gt
                | sqlparser::ast::BinaryOperator::GtEq => Some(cost::PredicateType::Range),
                _ => None,
            },
            sqlparser::ast::Expr::Between { negated, .. } if !negated => Some(cost::PredicateType::Range),
            sqlparser::ast::Expr::Like { .. } | sqlparser::ast::Expr::ILike { .. } => {
                Some(cost::PredicateType::Prefix)
            }
            sqlparser::ast::Expr::Function(f) => {
                let fname = f.name.to_string().to_uppercase();
                match fname.as_str() {
                    "VECTOR_DISTANCE" => Some(cost::PredicateType::VectorSimilarity),
                    "TS_MATCH" | "TS_RANK" | "TO_TSVECTOR" => {
                        Some(cost::PredicateType::FullTextMatch)
                    }
                    "ST_DISTANCE" | "ST_DWITHIN" | "ST_CONTAINS" => {
                        Some(cost::PredicateType::SpatialContains)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Build a cost-model AccessMethod for a given catalog IndexType.
    fn access_method_for_index_type(
        idx_type: &IndexType,
        row_count: f64,
    ) -> Option<Box<dyn AccessMethod>> {
        match idx_type {
            IndexType::BTree => Some(Box::new(cost::BTreeAccess::new(200.0))),
            IndexType::Hash => Some(Box::new(cost::HashAccess)),
            IndexType::Hnsw | IndexType::IvfFlat => {
                Some(Box::new(cost::HnswAccess::for_dataset(row_count)))
            }
            IndexType::Gin => Some(Box::new(cost::FtsAccess::new(500.0, 2.0))),
            IndexType::Gist | IndexType::Rtree => {
                Some(Box::new(cost::RTreeAccess::new(50.0)))
            }
        }
    }

    /// Plan a scan considering all available access methods (Principle 4).
    ///
    /// Uses the unified cost model to compare B-tree, HNSW, FTS, R-tree,
    /// and sequential scans, choosing the cheapest option.
    pub async fn plan_scan_unified(
        &self,
        table: &str,
        predicates: &[&sqlparser::ast::Expr],
    ) -> PlanNode {
        let stats = self.stats_store.get_or_default(table, &self.catalog).await;
        let row_count = stats.row_count as f64;
        let seq_access = cost::SeqScanAccess;
        let seq_cost = seq_access.estimate_cost(row_count, 1.0);

        let mut best_plan: Option<PlanNode> = None;
        let mut best_total = seq_cost.total();
        let mut best_pred_idx: Option<usize> = None; // which predicate the index covers

        // Check each predicate against available indexes.
        // Use sync cached lookups to avoid async RwLock overhead.
        if self.catalog.get_table_cached(table).is_some() {
            let indexes = self.catalog.get_indexes_cached(table).unwrap_or_default();
            for (pred_i, pred) in predicates.iter().enumerate() {
                let pred_type = Self::classify_predicate(pred);
                if pred_type.is_none() {
                    continue;
                }
                let pred_type = pred_type.unwrap();

                for idx in &indexes {
                    if let Some(access) = Self::access_method_for_index_type(&idx.index_type, row_count) {
                        if !access.supports_predicate(&pred_type) {
                            continue;
                        }

                        let col = idx.columns.first().map(|c| c.as_str()).unwrap_or("");

                        // Verify the index column matches the predicate column.
                        // Without this check, we'd pick an index on column A for a
                        // predicate on column B, producing zero results.
                        let pred_col = if let Some((c, _)) = is_equality_predicate(pred) {
                            Some(c)
                        } else if let sqlparser::ast::Expr::Between { expr, .. } = pred {
                            extract_column_name(expr)
                        } else if let Some((c, _)) = extract_range_bound(pred) {
                            Some(c)
                        } else {
                            None
                        };
                        if let Some(ref pc) = pred_col {
                            if !col.eq_ignore_ascii_case(pc) {
                                continue;
                            }
                        }
                        let selectivity = stats.equality_selectivity(Some(col));
                        let idx_cost = access.estimate_cost(row_count, selectivity);
                        let est = access.estimate_rows(row_count, selectivity);

                        if idx_cost.total() < best_total {
                            best_total = idx_cost.total();
                            best_pred_idx = Some(pred_i);
                            // BETWEEN predicates → range scan; equality → point lookup
                            let plan = if let sqlparser::ast::Expr::Between { low, high, .. } = pred {
                                PlanNode::IndexScan {
                                    table: table.to_string(),
                                    index_name: idx.name.clone(),
                                    estimated_rows: est.rows as usize,
                                    estimated_cost: Cost(idx_cost.total()),
                                    lookup_key: None,
                                    lookup_key_expr: None,
                                    range_lo: Some(low.to_string()),
                                    range_lo_expr: Some(*low.clone()),
                                    range_hi: Some(high.to_string()),
                                    range_hi_expr: Some(*high.clone()),
                                    range_predicate: Some(pred.to_string()),
                                    range_predicate_expr: Some((*pred).clone()),
                                }
                            } else {
                                PlanNode::IndexScan {
                                    table: table.to_string(),
                                    index_name: idx.name.clone(),
                                    estimated_rows: est.rows as usize,
                                    estimated_cost: Cost(idx_cost.total()),
                                    lookup_key: Some(pred.to_string()),
                                    lookup_key_expr: Some((*pred).clone()),
                                    range_lo: None,
                                    range_lo_expr: None,
                                    range_hi: None,
                                    range_hi_expr: None,
                                    range_predicate: None,
                                    range_predicate_expr: None,
                                }
                            };
                            best_plan = Some(plan);
                        }
                    }
                }
            }
        }

        // If a specialty index won, use it — with a Filter for remaining predicates.
        if let Some(plan) = best_plan {
            // Collect predicates NOT covered by the index scan.
            let remaining: Vec<&sqlparser::ast::Expr> = predicates.iter().enumerate()
                .filter(|(i, _)| Some(*i) != best_pred_idx)
                .map(|(_, p)| *p)
                .collect();
            if remaining.is_empty() {
                return plan;
            }
            // Combine remaining predicates with AND and wrap in a Filter node.
            let combined = remaining.iter()
                .map(|e| (*e).clone())
                .reduce(|a, b| sqlparser::ast::Expr::BinaryOp {
                    left: Box::new(a),
                    op: sqlparser::ast::BinaryOperator::And,
                    right: Box::new(b),
                })
                .unwrap();
            let est_rows = (plan.estimated_rows() / 2).max(1);
            let filter_cost = Cost(plan.total_cost().0 + plan.estimated_rows() as f64 * CPU_OPERATOR_COST);
            return PlanNode::Filter {
                input: Box::new(plan),
                predicate: combined.to_string(),
                predicate_expr: Some(combined),
                estimated_rows: est_rows,
                estimated_cost: filter_cost,
            };
        }

        // Fallback to standard plan_scan logic.
        self.plan_scan(table, predicates).await
    }

    /// Plan a join between two tables.
    pub fn plan_join(
        &self,
        left: PlanNode,
        right: PlanNode,
        join_type: JoinPlanType,
        condition: Option<&sqlparser::ast::Expr>,
    ) -> PlanNode {
        let left_rows = left.estimated_rows();
        let right_rows = right.estimated_rows();
        let result_rows = match join_type {
            JoinPlanType::Cross => left_rows * right_rows,
            JoinPlanType::Inner => (left_rows * right_rows) / (left_rows.max(right_rows).max(1)),
            JoinPlanType::Left => left_rows,
            JoinPlanType::Right => right_rows,
            JoinPlanType::Full => left_rows + right_rows,
        };

        // Check if this is an equi-join (can use hash join)
        if let Some(cond) = condition
            && let Some((lt, lc, rt, rc)) = is_equi_join_condition(cond) {
                let hash_cost = estimate_hash_join_cost(left_rows, right_rows, result_rows);
                let nl_cost = estimate_nested_loop_cost(left_rows, right.total_cost(), result_rows);

                if hash_cost.0 < nl_cost.0 && left_rows > 10 {
                    return PlanNode::HashJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type,
                        hash_keys: vec![format!("{lt}.{lc} = {rt}.{rc}")],
                        estimated_rows: result_rows.max(1),
                        estimated_cost: hash_cost,
                    };
                }
            }

        // Fallback: nested loop join
        let nl_cost = estimate_nested_loop_cost(left_rows, right.total_cost(), result_rows);
        PlanNode::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            estimated_rows: result_rows.max(1),
            estimated_cost: nl_cost,
            condition: condition.map(|c| c.to_string()),
            condition_expr: condition.cloned(),
        }
    }

    /// Reorder joins: put smaller tables on the left (build side for hash join).
    pub fn reorder_join_inputs(&self, left: PlanNode, right: PlanNode) -> (PlanNode, PlanNode) {
        if left.estimated_rows() <= right.estimated_rows() {
            (left, right)
        } else {
            (right, left)
        }
    }
}

/// Choose the cheapest scan method for a table given available indexes.
/// (Legacy API for backward compatibility)
pub fn choose_scan_plan(
    stats: &TableStats,
    has_index: bool,
    is_equality_lookup: bool,
    is_unique_index: bool,
    filter_desc: Option<String>,
    index_name: Option<String>,
    lookup_key: Option<String>,
) -> PlanNode {
    choose_scan_plan_ex(stats, has_index, is_equality_lookup, is_unique_index, false, filter_desc, index_name, lookup_key)
}

/// Choose the cheapest scan method, with hash index awareness.
#[allow(clippy::too_many_arguments)]
pub fn choose_scan_plan_ex(
    stats: &TableStats,
    has_index: bool,
    is_equality_lookup: bool,
    is_unique_index: bool,
    is_hash_index: bool,
    filter_desc: Option<String>,
    index_name: Option<String>,
    lookup_key: Option<String>,
) -> PlanNode {
    let seq_cost = estimate_seq_scan_cost(stats.page_count, stats.row_count);

    if has_index && is_equality_lookup {
        let estimated_matches = if is_unique_index {
            1
        } else {
            (stats.row_count as f64 * stats.equality_selectivity(None)).max(1.0) as usize
        };
        let idx_cost = if is_hash_index {
            // Hash index: O(1) probe cost — always cheaper than B-tree for equality
            estimate_hash_index_cost(estimated_matches)
        } else {
            let tree_height = if stats.page_count == 0 { 1 } else { ((stats.page_count as f64).log2() as usize).max(1) };
            estimate_index_scan_cost(tree_height, estimated_matches)
        };

        if idx_cost.0 < seq_cost.0 {
            let lookup_expr = lookup_key.as_ref().and_then(|s| parse_expr_safe(s));
            return PlanNode::IndexScan {
                table: stats.table_name.clone(),
                index_name: index_name.unwrap_or_else(|| "idx".into()),
                estimated_rows: estimated_matches.max(1),
                estimated_cost: idx_cost,
                lookup_key,
                lookup_key_expr: lookup_expr,
                range_lo: None,
                range_lo_expr: None,
                range_hi: None,
                range_hi_expr: None,
                range_predicate: None,
                range_predicate_expr: None,
            };
        }
    }

    let filter_expr = filter_desc.as_ref().and_then(|s| parse_expr_safe(s));
    PlanNode::SeqScan {
        table: stats.table_name.clone(),
        estimated_rows: stats.row_count,
        estimated_cost: seq_cost,
        filter: filter_desc,
        filter_expr,
        scan_limit: None,
        projection: None,
    }
}

// ============================================================================
// Expr parsing helpers (used to pre-parse plan node string fields)
// ============================================================================

/// Try to parse a SQL expression string into an AST Expr at plan time.
/// Returns None on failure (execution will fall back to re-parsing).
pub fn parse_expr_safe(s: &str) -> Option<sqlparser::ast::Expr> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    let sql = format!("SELECT {s}");
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &sql).ok()?;
    if let Some(sqlparser::ast::Statement::Query(q)) = stmts.into_iter().next()
        && let sqlparser::ast::SetExpr::Select(sel) = *q.body
        && let Some(sqlparser::ast::SelectItem::UnnamedExpr(expr)) = sel.projection.into_iter().next()
    {
        return Some(expr);
    }
    None
}

/// Combine multiple predicate expressions into a single AND conjunction.
/// Returns None if the slice is empty.
pub fn combine_predicate_exprs(predicates: &[&sqlparser::ast::Expr]) -> Option<sqlparser::ast::Expr> {
    if predicates.is_empty() {
        return None;
    }
    let mut combined = predicates[0].clone();
    for pred in &predicates[1..] {
        combined = sqlparser::ast::Expr::BinaryOp {
            left: Box::new(combined),
            op: sqlparser::ast::BinaryOperator::And,
            right: Box::new((*pred).clone()),
        };
    }
    Some(combined)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_scan_cost_increases_with_rows() {
        let small = estimate_seq_scan_cost(10, 100);
        let large = estimate_seq_scan_cost(100, 10000);
        assert!(large.0 > small.0);
    }

    #[test]
    fn index_cheaper_for_point_lookup() {
        let stats = TableStats::new("users", 100000, 1000);
        let plan = choose_scan_plan(
            &stats, true, true, true,
            None, Some("idx_users_id".into()), Some("id = 42".into()),
        );
        assert!(matches!(plan, PlanNode::IndexScan { .. }));
    }

    #[test]
    fn seq_scan_for_no_index() {
        let stats = TableStats::new("users", 100, 10);
        let plan = choose_scan_plan(
            &stats, false, true, false,
            Some("age > 30".into()), None, None,
        );
        assert!(matches!(plan, PlanNode::SeqScan { .. }));
    }

    #[test]
    fn explain_output() {
        let plan = PlanNode::Sort {
            input: Box::new(PlanNode::SeqScan {
                table: "orders".into(),
                estimated_rows: 1000,
                estimated_cost: estimate_seq_scan_cost(100, 1000),
                filter: Some("status = 'pending'".into()),
                filter_expr: parse_expr_safe("status = 'pending'"),
                scan_limit: None,
                projection: None,
            }),
            keys: vec!["created_at DESC".into()],
            estimated_cost: estimate_sort_cost(1000, estimate_seq_scan_cost(100, 1000)),
        };
        let output = format!("{plan}");
        assert!(output.contains("Sort"));
        assert!(output.contains("Seq Scan on orders"));
        assert!(output.contains("Filter: status = 'pending'"));
    }

    #[test]
    fn hash_join_cheaper_for_large_tables() {
        let left_rows = 10000;
        let right_rows = 50000;
        let result_rows = 10000;
        let hash_cost = estimate_hash_join_cost(left_rows, right_rows, result_rows);
        let right_cost = estimate_seq_scan_cost(500, right_rows);
        let nl_cost = estimate_nested_loop_cost(left_rows, right_cost, result_rows);
        assert!(hash_cost.0 < nl_cost.0, "Hash join should be cheaper for large equi-joins");
    }

    #[test]
    fn split_conjunction_works() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let sql = "SELECT 1 WHERE a = 1 AND b = 2 AND c > 3";
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        if let sqlparser::ast::Statement::Query(q) = &stmts[0] {
            if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(where_expr) = &sel.selection {
                    let preds = split_conjunction(where_expr);
                    assert_eq!(preds.len(), 3);
                }
            }
        }
    }

    #[test]
    fn selectivity_equality() {
        let mut stats = TableStats::new("users", 1000, 10);
        stats.column_stats.insert("id".to_string(), ColumnStats {
            distinct_count: 1000,
            null_fraction: 0.0,
            avg_width: 4,
            min_value: Some("1".into()),
            max_value: Some("1000".into()),
        });
        let sel = stats.equality_selectivity(Some("id"));
        assert!((sel - 0.001).abs() < 0.001);
    }

    #[test]
    fn selectivity_unknown_column() {
        let stats = TableStats::new("users", 1000, 10);
        let sel = stats.equality_selectivity(Some("nonexistent"));
        assert!((sel - 0.1).abs() < 0.01);
    }

    #[test]
    fn hash_join_display() {
        let plan = PlanNode::HashJoin {
            left: Box::new(PlanNode::SeqScan {
                table: "orders".into(),
                estimated_rows: 1000,
                estimated_cost: Cost(10.0),
                filter: None,
                filter_expr: None,
                scan_limit: None,
                projection: None,
            }),
            right: Box::new(PlanNode::SeqScan {
                table: "customers".into(),
                estimated_rows: 100,
                estimated_cost: Cost(5.0),
                filter: None,
                filter_expr: None,
                scan_limit: None,
                projection: None,
            }),
            join_type: JoinPlanType::Inner,
            hash_keys: vec!["customer_id = id".into()],
            estimated_rows: 1000,
            estimated_cost: Cost(15.0),
        };
        let output = format!("{plan}");
        assert!(output.contains("Hash Inner Join"));
        assert!(output.contains("Hash Key: customer_id = id"));
    }

    #[test]
    fn filter_display() {
        let plan = PlanNode::Filter {
            input: Box::new(PlanNode::SeqScan {
                table: "users".into(),
                estimated_rows: 1000,
                estimated_cost: Cost(10.0),
                filter: None,
                filter_expr: None,
                scan_limit: None,
                projection: None,
            }),
            predicate: "age > 18".into(),
            predicate_expr: parse_expr_safe("age > 18"),
            estimated_rows: 330,
            estimated_cost: Cost(12.5),
        };
        let output = format!("{plan}");
        assert!(output.contains("Filter"));
        assert!(output.contains("Predicate: age > 18"));
    }

    #[test]
    fn aggregate_display() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::SeqScan {
                table: "sales".into(),
                estimated_rows: 500,
                estimated_cost: Cost(5.0),
                filter: None,
                filter_expr: None,
                scan_limit: None,
                projection: None,
            }),
            aggregates: vec!["SUM(amount)".into(), "COUNT(*)".into()],
            estimated_cost: Cost(10.0),
        };
        let output = format!("{plan}");
        assert!(output.contains("Aggregate [SUM(amount), COUNT(*)]"));
    }

    #[tokio::test]
    async fn stats_store_get_set() {
        let store = StatsStore::new();
        assert!(store.get("users").await.is_none());

        let stats = TableStats::new("users", 5000, 50);
        store.update(stats).await;

        let retrieved = store.get("users").await.unwrap();
        assert_eq!(retrieved.row_count, 5000);
        assert_eq!(retrieved.page_count, 50);
    }

    #[tokio::test]
    async fn planner_chooses_seq_scan_no_index() {
        let catalog = Arc::new(Catalog::new());
        let stats_store = Arc::new(StatsStore::new());
        stats_store.update(TableStats::new("orders", 100, 10)).await;

        let planner = QueryPlanner::new(catalog, stats_store);
        let plan = planner.plan_scan("orders", &[]).await;
        assert!(matches!(plan, PlanNode::SeqScan { .. }));
    }

    #[test]
    fn join_reorder_smaller_left() {
        let catalog = Arc::new(Catalog::new());
        let stats_store = Arc::new(StatsStore::new());
        let planner = QueryPlanner::new(catalog, stats_store);

        let big = PlanNode::SeqScan {
            table: "big".into(),
            estimated_rows: 10000,
            estimated_cost: Cost(100.0),
            filter: None,
            filter_expr: None,
            scan_limit: None,
            projection: None,
        };
        let small = PlanNode::SeqScan {
            table: "small".into(),
            estimated_rows: 100,
            estimated_cost: Cost(10.0),
            filter: None,
            filter_expr: None,
            scan_limit: None,
            projection: None,
        };
        let (left, right) = planner.reorder_join_inputs(big, small);
        assert_eq!(left.estimated_rows(), 100);
        assert_eq!(right.estimated_rows(), 10000);
    }

    #[test]
    fn equi_join_detection() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let sql = "SELECT 1 WHERE a.id = b.user_id";
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        if let sqlparser::ast::Statement::Query(q) = &stmts[0] {
            if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(where_expr) = &sel.selection {
                    let result = is_equi_join_condition(where_expr);
                    assert!(result.is_some());
                    let (lt, lc, rt, rc) = result.unwrap();
                    assert_eq!(lt, "a");
                    assert_eq!(lc, "id");
                    assert_eq!(rt, "b");
                    assert_eq!(rc, "user_id");
                }
            }
        }
    }

    #[test]
    fn equality_predicate_detection() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let sql = "SELECT 1 WHERE name = 'alice'";
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        if let sqlparser::ast::Statement::Query(q) = &stmts[0] {
            if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(where_expr) = &sel.selection {
                    let result = is_equality_predicate(where_expr);
                    assert!(result.is_some());
                    let (col, val) = result.unwrap();
                    assert_eq!(col, "name");
                    assert!(val.contains("alice"));
                }
            }
        }
    }

    // ================================================================
    // Unified cost model integration tests
    // ================================================================

    #[test]
    fn classify_equality_predicate() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let sql = "SELECT 1 WHERE id = 5";
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        if let sqlparser::ast::Statement::Query(q) = &stmts[0] {
            if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(expr) = &sel.selection {
                    let pt = QueryPlanner::classify_predicate(expr);
                    assert_eq!(pt, Some(cost::PredicateType::Equality));
                }
            }
        }
    }

    #[test]
    fn classify_range_predicate() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let sql = "SELECT 1 WHERE age > 18";
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        if let sqlparser::ast::Statement::Query(q) = &stmts[0] {
            if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(expr) = &sel.selection {
                    let pt = QueryPlanner::classify_predicate(expr);
                    assert_eq!(pt, Some(cost::PredicateType::Range));
                }
            }
        }
    }

    #[test]
    fn access_method_for_btree() {
        let am = QueryPlanner::access_method_for_index_type(&IndexType::BTree, 1000.0);
        assert!(am.is_some());
        assert_eq!(am.unwrap().name(), "btree");
    }

    #[test]
    fn access_method_for_hnsw() {
        let am = QueryPlanner::access_method_for_index_type(&IndexType::Hnsw, 100000.0);
        assert!(am.is_some());
        assert_eq!(am.unwrap().name(), "hnsw");
    }

    #[test]
    fn access_method_for_rtree() {
        let am = QueryPlanner::access_method_for_index_type(&IndexType::Rtree, 5000.0);
        assert!(am.is_some());
        assert_eq!(am.unwrap().name(), "rtree");
    }

    #[test]
    fn access_method_for_gin() {
        let am = QueryPlanner::access_method_for_index_type(&IndexType::Gin, 10000.0);
        assert!(am.is_some());
        assert_eq!(am.unwrap().name(), "fts_inverted");
    }

    #[tokio::test]
    async fn unified_planner_falls_back_to_seqscan() {
        let catalog = Arc::new(Catalog::new());
        let stats_store = Arc::new(StatsStore::new());
        stats_store.update(TableStats::new("items", 1000, 10)).await;
        let planner = QueryPlanner::new(catalog, stats_store);
        let plan = planner.plan_scan_unified("items", &[]).await;
        assert!(matches!(plan, PlanNode::SeqScan { .. }));
    }
}
