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
pub enum PlanNode {
    /// Sequential scan: read all pages of a table.
    SeqScan {
        table: String,
        estimated_rows: usize,
        estimated_cost: Cost,
        filter: Option<String>,
    },
    /// Index scan: use a B-tree index to find matching rows.
    IndexScan {
        table: String,
        index_name: String,
        estimated_rows: usize,
        estimated_cost: Cost,
        lookup_key: Option<String>,
    },
    /// Nested loop join.
    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinPlanType,
        estimated_rows: usize,
        estimated_cost: Cost,
        condition: Option<String>,
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
            PlanNode::SeqScan { table, estimated_rows, estimated_cost, filter } => {
                write!(f, "{pad}Seq Scan on {table} (cost={:.2} rows={estimated_rows})", estimated_cost.0)?;
                if let Some(flt) = filter {
                    write!(f, "\n{pad}  Filter: {flt}")?;
                }
            }
            PlanNode::IndexScan { table, index_name, estimated_rows, estimated_cost, lookup_key } => {
                write!(f, "{pad}Index Scan using {index_name} on {table} (cost={:.2} rows={estimated_rows})", estimated_cost.0)?;
                if let Some(key) = lookup_key {
                    write!(f, "\n{pad}  Index Cond: {key}")?;
                }
            }
            PlanNode::NestedLoopJoin { left, right, join_type, estimated_rows, estimated_cost, condition } => {
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
            PlanNode::Filter { input, predicate, estimated_rows, estimated_cost } => {
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

/// Estimate index scan cost (point lookup).
pub fn estimate_index_scan_cost(tree_height: usize, estimated_matches: usize) -> Cost {
    let index_cost = tree_height as f64 * RANDOM_PAGE_COST + estimated_matches as f64 * CPU_INDEX_COST;
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
    /// When the table was last analyzed (None if never).
    pub last_analyzed: Option<std::time::Instant>,
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
        if let Some(col) = column {
            if let Some(stats) = self.column_stats.get(col) {
                if stats.distinct_count > 0 {
                    return 1.0 / stats.distinct_count as f64;
                }
            }
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
        if let Some(col) = column {
            if let Some(cs) = self.column_stats.get(col) {
                if let (Some(min_s), Some(max_s)) = (&cs.min_value, &cs.max_value) {
                    if let (Ok(min_v), Ok(max_v)) = (min_s.parse::<f64>(), max_s.parse::<f64>()) {
                        if max_v > min_v {
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
                    }
                }
            }
        }
        0.33
    }

    /// Estimate selectivity of a LIKE predicate.
    pub fn like_selectivity(&self) -> f64 {
        0.05
    }

    /// Estimate selectivity of IS NULL.
    pub fn null_selectivity(&self, column: Option<&str>) -> f64 {
        if let Some(col) = column {
            if let Some(stats) = self.column_stats.get(col) {
                return stats.null_fraction;
            }
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
        // Create default stats from catalog
        let row_count = if catalog.get_table(table).await.is_some() {
            1000 // default estimate
        } else {
            0
        };
        TableStats::new(table, row_count, (row_count / 100).max(1))
    }
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
    if let sqlparser::ast::Expr::Value(v) = expr {
        if let sqlparser::ast::Value::Number(n, _) = &v.value {
            return n.parse::<f64>().ok();
        }
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

/// Check if an expression is a simple equality predicate (col = literal).
pub fn is_equality_predicate(expr: &sqlparser::ast::Expr) -> Option<(String, String)> {
    if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr {
        if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
            if let Some(col) = extract_column_name(left) {
                return Some((col, right.to_string()));
            }
            if let Some(col) = extract_column_name(right) {
                return Some((col, left.to_string()));
            }
        }
    }
    None
}

/// Check if an expression is an equi-join condition (left_table.col = right_table.col).
pub fn is_equi_join_condition(expr: &sqlparser::ast::Expr) -> Option<(String, String, String, String)> {
    if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr {
        if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
            if let (
                sqlparser::ast::Expr::CompoundIdentifier(left_idents),
                sqlparser::ast::Expr::CompoundIdentifier(right_idents),
            ) = (left.as_ref(), right.as_ref())
            {
                if left_idents.len() == 2 && right_idents.len() == 2 {
                    return Some((
                        left_idents[0].value.clone(),
                        left_idents[1].value.clone(),
                        right_idents[0].value.clone(),
                        right_idents[1].value.clone(),
                    ));
                }
            }
        }
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
    if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr {
        if matches!(op, sqlparser::ast::BinaryOperator::And) {
            split_conjunction_inner(left, out);
            split_conjunction_inner(right, out);
            return;
        }
    }
    if let sqlparser::ast::Expr::Nested(inner) = expr {
        if let sqlparser::ast::Expr::BinaryOp { op, .. } = inner.as_ref() {
            if matches!(op, sqlparser::ast::BinaryOperator::And) {
                split_conjunction_inner(inner, out);
                return;
            }
        }
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

        // Check if any predicate matches an available index
        if let Some(table_def) = self.catalog.get_table(table).await {
            for pred in predicates {
                if let Some((col, val)) = is_equality_predicate(pred) {
                    // Check indexes on this table
                    for idx in self.catalog.get_indexes(table).await {
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
                                return PlanNode::IndexScan {
                                    table: table.to_string(),
                                    index_name: idx.name.clone(),
                                    estimated_rows: estimated_matches,
                                    estimated_cost: idx_cost,
                                    lookup_key: Some(format!("{col} = {val}")),
                                };
                            }
                        }
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

        let filter = if predicates.is_empty() {
            None
        } else {
            Some(predicates.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(" AND "))
        };

        PlanNode::SeqScan {
            table: table.to_string(),
            estimated_rows,
            estimated_cost: cost,
            filter,
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

        // Check each predicate against available indexes.
        if let Some(_table_def) = self.catalog.get_table(table).await {
            for pred in predicates {
                let pred_type = Self::classify_predicate(pred);
                if pred_type.is_none() {
                    continue;
                }
                let pred_type = pred_type.unwrap();

                for idx in self.catalog.get_indexes(table).await {
                    if let Some(access) = Self::access_method_for_index_type(&idx.index_type, row_count) {
                        if !access.supports_predicate(&pred_type) {
                            continue;
                        }

                        let col = idx.columns.first().map(|c| c.as_str()).unwrap_or("");
                        let selectivity = stats.equality_selectivity(Some(col));
                        let idx_cost = access.estimate_cost(row_count, selectivity);
                        let est = access.estimate_rows(row_count, selectivity);

                        if idx_cost.total() < best_total {
                            best_total = idx_cost.total();
                            best_plan = Some(PlanNode::IndexScan {
                                table: table.to_string(),
                                index_name: idx.name.clone(),
                                estimated_rows: est.rows as usize,
                                estimated_cost: Cost(idx_cost.total()),
                                lookup_key: Some(pred.to_string()),
                            });
                        }
                    }
                }
            }
        }

        // If a specialty index won, use it.
        if let Some(plan) = best_plan {
            return plan;
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
        if let Some(cond) = condition {
            if let Some((lt, lc, rt, rc)) = is_equi_join_condition(cond) {
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
    let seq_cost = estimate_seq_scan_cost(stats.page_count, stats.row_count);

    if has_index && is_equality_lookup {
        let estimated_matches = if is_unique_index {
            1
        } else {
            (stats.row_count as f64 * stats.equality_selectivity(None)).max(1.0) as usize
        };
        let tree_height = if stats.page_count == 0 { 1 } else { ((stats.page_count as f64).log2() as usize).max(1) };
        let idx_cost = estimate_index_scan_cost(tree_height, estimated_matches);

        if idx_cost.0 < seq_cost.0 {
            return PlanNode::IndexScan {
                table: stats.table_name.clone(),
                index_name: index_name.unwrap_or_else(|| "idx".into()),
                estimated_rows: estimated_matches.max(1),
                estimated_cost: idx_cost,
                lookup_key,
            };
        }
    }

    PlanNode::SeqScan {
        table: stats.table_name.clone(),
        estimated_rows: stats.row_count,
        estimated_cost: seq_cost,
        filter: filter_desc,
    }
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
            }),
            right: Box::new(PlanNode::SeqScan {
                table: "customers".into(),
                estimated_rows: 100,
                estimated_cost: Cost(5.0),
                filter: None,
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
            }),
            predicate: "age > 18".into(),
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
        };
        let small = PlanNode::SeqScan {
            table: "small".into(),
            estimated_rows: 100,
            estimated_cost: Cost(10.0),
            filter: None,
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
