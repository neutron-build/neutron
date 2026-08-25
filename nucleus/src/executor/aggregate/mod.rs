//! Aggregation (GROUP BY, HAVING, aggregate functions) and window function
//! execution methods extracted from the main executor module.
//!
//! This module also provides type-specialized hash tables (fastmap) for efficient
//! GROUP BY aggregations.

pub mod fastmap;

// Re-exports for future type-specialized GROUP BY optimization paths.
#[allow(unused_imports)]
pub use fastmap::{AggregateState, FastHashMap, select_fast_map};

use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;
use sqlparser::ast::{self, Expr, SelectItem};

use crate::simd;
use crate::types::{DataType, Row, Value};

use super::helpers::{
    compare_values, compute_window_frame_bounds, contains_aggregate, infer_expr_type,
    value_to_ast_expr, value_to_f64, value_to_i64, value_type,
};
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};

/// Resolve the column type for an aggregate projection: prefer the runtime
/// `value_type`, but when the aggregate evaluates to NULL (e.g. `MAX` over
/// an empty input) fall back to static AST inference so we still advertise
/// the right pgwire OID instead of TEXT.
/// Reject invalid aggregate/GROUP BY projections the way PostgreSQL does:
/// (a) a nested aggregate — an aggregate whose argument contains another
/// aggregate (`MAX(COUNT(*))`); (b) a column reference in the SELECT list that
/// is neither inside an aggregate nor a GROUP BY key
/// (`SELECT dept, sal FROM t GROUP BY dept`). Both were silently accepted and
/// returned wrong/NULL results. Conservative: only bare and compound
/// identifiers are checked for (b); anything more complex is left permissive
/// so no currently-valid query regresses.
/// All positional argument expressions of a window function call, in order —
/// e.g. lag(v, 2, -1) → [v, 2, -1]. Named args are ignored.
fn window_arg_exprs(func: &ast::Function) -> Vec<&Expr> {
    match &func.args {
        ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn validate_grouped_projection(
    projection: &[SelectItem],
    group_by: &[Expr],
) -> Result<(), ExecError> {
    use super::helpers::contains_aggregate;

    // ROLLUP / CUBE / GROUPING SETS make columns conditionally grouped — the
    // simple "must be a GROUP BY key" rule doesn't apply. Skip validation
    // entirely when any such construct is present (permissive).
    let has_grouping_set = group_by
        .iter()
        .any(|e| matches!(e, Expr::Rollup(_) | Expr::Cube(_) | Expr::GroupingSets(_)));
    if has_grouping_set {
        return Ok(());
    }

    // GROUP BY key spellings, normalized (whitespace-insensitive, lowercased).
    let norm = |e: &Expr| {
        e.to_string()
            .replace(char::is_whitespace, "")
            .to_lowercase()
    };
    let keys: HashSet<String> = group_by.iter().map(norm).collect();
    let has_group = !group_by.is_empty();

    // Recursively check one projection expression.
    fn check(
        expr: &Expr,
        keys: &HashSet<String>,
        has_group: bool,
        in_aggregate: bool,
        norm: &dyn Fn(&Expr) -> String,
    ) -> Result<(), ExecError> {
        // A GROUP BY key covers this whole subtree.
        if has_group && keys.contains(&norm(expr)) {
            return Ok(());
        }
        match expr {
            Expr::Function(func)
                if super::helpers::contains_aggregate(expr) && func.over.is_none() =>
            {
                let name = func.name.to_string().to_uppercase();
                // Must match `contains_aggregate`'s function list exactly, or a
                // recognized-elsewhere aggregate (argMax etc.) is treated as a
                // scalar wrapper and its column args wrongly demand GROUP BY.
                let is_agg = matches!(
                    name.as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STRING_AGG"
                        | "ARRAY_AGG"
                        | "JSON_AGG"
                        | "BOOL_AND"
                        | "BOOL_OR"
                        | "EVERY"
                        | "BIT_AND"
                        | "BIT_OR"
                        | "ARGMAX"
                        | "ARG_MAX"
                        | "ARGMIN"
                        | "ARG_MIN"
                        | "MEDIAN"
                        | "QUANTILE"
                        | "PERCENTILE_CONT"
                        | "PERCENTILE_DISC"
                );
                if is_agg {
                    if in_aggregate {
                        return Err(ExecError::Unsupported(
                            "aggregate function calls cannot be nested".into(),
                        ));
                    }
                    if let ast::FunctionArguments::List(list) = &func.args {
                        for a in &list.args {
                            if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner))
                            | ast::FunctionArg::Named {
                                arg: ast::FunctionArgExpr::Expr(inner),
                                ..
                            } = a
                            {
                                check(inner, keys, has_group, true, norm)?;
                            }
                        }
                    }
                    return Ok(());
                }
                // scalar fn wrapping an aggregate: recurse into args
                if let ast::FunctionArguments::List(list) = &func.args {
                    for a in &list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner))
                        | ast::FunctionArg::Named {
                            arg: ast::FunctionArgExpr::Expr(inner),
                            ..
                        } = a
                        {
                            check(inner, keys, has_group, in_aggregate, norm)?;
                        }
                    }
                }
                Ok(())
            }
            // Bare / qualified column reference not inside an aggregate and not
            // a GROUP BY key → invalid in a grouped/aggregate query.
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                if has_group && !in_aggregate {
                    Err(ExecError::Unsupported(format!(
                        "column \"{expr}\" must appear in the GROUP BY clause or be used in an aggregate function"
                    )))
                } else {
                    Ok(())
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                check(left, keys, has_group, in_aggregate, norm)?;
                check(right, keys, has_group, in_aggregate, norm)
            }
            Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => {
                check(expr, keys, has_group, in_aggregate, norm)
            }
            _ => Ok(()),
        }
    }

    for item in projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => continue,
        };
        // A projection with no aggregate and no GROUP BY is not our concern
        // (that path never reaches execute_aggregate anyway).
        if !has_group && !contains_aggregate(expr) {
            continue;
        }
        check(expr, &keys, has_group, false, &norm)?;
    }
    Ok(())
}

fn agg_column_type(val: &Value, expr: &Expr, col_meta: &[ColMeta]) -> DataType {
    if matches!(val, Value::Null) {
        infer_expr_type(expr, col_meta)
    } else {
        value_type(val)
    }
}

/// Column descriptors derived statically from the projection — used when a
/// GROUP BY produced ZERO groups, where the per-group loop never ran and the
/// old code emitted an empty column list. A derived table over such a result
/// then had no columns at all, so outer references like `alias.col` failed
/// with "column does not exist" (SQLAlchemy's domain-reflection subquery over
/// the empty pg_constraint hit exactly this).
fn agg_static_columns(projection: &[SelectItem], col_meta: &[ColMeta]) -> Vec<(String, DataType)> {
    let mut cols = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                cols.push((
                    crate::executor::helpers::default_output_name(expr),
                    infer_expr_type(expr, col_meta),
                ));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                cols.push((alias.value.clone(), infer_expr_type(expr, col_meta)));
            }
            _ => {}
        }
    }
    cols
}

impl Executor {
    // ========================================================================
    // Aggregation: GROUP BY, HAVING, aggregate functions
    // ========================================================================

    pub(super) fn execute_aggregate(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        rows: Vec<Row>,
        sorted_by_col: Option<&str>,
    ) -> Result<ExecResult, ExecError> {
        // Bound this operator's working set against the shared query-memory
        // budget: the group map holds every input row, so a huge GROUP BY would
        // otherwise grow unbounded and OOM the box. Reserving here converts that
        // into a clean MemoryExceeded (53200); the guard releases on return.
        let _mem = self.reserve_query_memory(Self::estimate_rows_bytes(&rows))?;
        let group_by_exprs: &[Expr] = match &select.group_by {
            ast::GroupByExpr::Expressions(exprs, _) => exprs,
            _ => &[],
        };
        validate_grouped_projection(&select.projection, group_by_exprs)?;

        // Check for GROUPING SETS / CUBE / ROLLUP in GROUP BY expressions
        let grouping_sets = self.extract_grouping_sets(group_by_exprs);
        if let Some(sets) = grouping_sets {
            return self.execute_grouping_sets_aggregate(select, col_meta, &rows, &sets);
        }

        // Fast path: single-column GROUP BY with specialised hash maps.
        // i64 path: HashMap<i64, Vec<usize>> eliminates Vec<Value> alloc per row.
        // Text path: HashMap<String, Vec<usize>> avoids Vec<Value> wrapping per row.
        if group_by_exprs.len() == 1
            && let Some(col_idx) = Self::resolve_col_idx_in_meta(&group_by_exprs[0], col_meta, 0)
        {
            let dtype = &col_meta[col_idx].dtype;
            if matches!(dtype, DataType::Int32 | DataType::Int64) {
                return self.execute_aggregate_i64_group(select, col_meta, rows, col_idx);
            }
            if matches!(dtype, DataType::Text) {
                return self.execute_aggregate_text_group(select, col_meta, rows, col_idx);
            }
        }

        // If no GROUP BY and we have aggregates, treat entire result as one group
        let groups: Vec<(Vec<Value>, Vec<Row>)> = if group_by_exprs.is_empty() {
            vec![(Vec::new(), rows)]
        } else {
            // When the input is already sorted by the GROUP BY column (e.g. after a B-tree
            // range scan), use a streaming linear pass instead of a HashMap.  This avoids
            // one Vec<Value> clone per row — a meaningful win for large range scan results.
            let use_streaming = if let Some(sort_col) = sorted_by_col {
                group_by_exprs.len() == 1 && {
                    let col = match &group_by_exprs[0] {
                        Expr::Identifier(id) => Some(id.value.as_str()),
                        Expr::CompoundIdentifier(ids) => ids.last().map(|id| id.value.as_str()),
                        _ => None,
                    };
                    col == Some(sort_col)
                }
            } else {
                false
            };

            if use_streaming {
                // Streaming pass: collect contiguous runs of the same key.
                let mut map: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
                for row in rows {
                    let key: Vec<Value> = group_by_exprs
                        .iter()
                        .map(|expr| self.eval_row_expr(expr, &row, col_meta))
                        .collect::<Result<_, _>>()?;
                    if map.last().is_none_or(|(last_key, _)| *last_key != key) {
                        map.push((key, vec![row]));
                    } else {
                        map.last_mut().unwrap().1.push(row);
                    }
                }
                map
            } else {
                let mut map: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
                let mut key_to_idx: HashMap<Vec<Value>, usize> = HashMap::new();
                for row in rows {
                    let key: Vec<Value> = group_by_exprs
                        .iter()
                        .map(|expr| self.eval_row_expr(expr, &row, col_meta))
                        .collect::<Result<_, _>>()?;
                    if let Some(&idx) = key_to_idx.get(&key) {
                        map[idx].1.push(row);
                    } else {
                        let idx = map.len();
                        key_to_idx.insert(key.clone(), idx);
                        map.push((key, vec![row]));
                    }
                }
                map
            }
        };

        // Evaluate projection for each group
        let mut result_columns: Option<Vec<(String, DataType)>> = None;
        let mut result_rows = Vec::new();

        for (_key, group_rows) in &groups {
            let trivial = Self::trivial_indices(group_rows.len());
            let mut row = Vec::new();
            let mut cols = Vec::new();

            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let val = self.eval_aggregate_expr(expr, group_rows, &trivial, col_meta)?;
                        cols.push((
                            crate::executor::helpers::default_output_name(expr),
                            agg_column_type(&val, expr, col_meta),
                        ));
                        row.push(val);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let val = self.eval_aggregate_expr(expr, group_rows, &trivial, col_meta)?;
                        cols.push((alias.value.clone(), agg_column_type(&val, expr, col_meta)));
                        row.push(val);
                    }
                    SelectItem::Wildcard(_) => {
                        return Err(ExecError::Unsupported("SELECT * with GROUP BY".into()));
                    }
                    _ => {
                        return Err(ExecError::Unsupported("unsupported select item".into()));
                    }
                }
            }

            if result_columns.is_none() {
                result_columns = Some(cols);
            }

            // Apply HAVING
            if let Some(ref having) = select.having {
                let hval = self.eval_aggregate_expr(having, group_rows, &trivial, col_meta)?;
                if hval != Value::Bool(true) {
                    continue;
                }
            }

            result_rows.push(row);
        }

        Ok(ExecResult::Select {
            columns: result_columns
                .unwrap_or_else(|| agg_static_columns(&select.projection, col_meta)),
            rows: result_rows,
        })
    }

    /// Fast path for `GROUP BY single_integer_column`.
    ///
    /// Uses `HashMap<i64, Vec<usize>>` (integer key -> row indices) instead of
    /// the generic `HashMap<Vec<Value>, usize>` path.  This eliminates one
    /// `Vec<Value>` allocation per row for the most common GROUP BY pattern.
    /// The i64 key is converted to `Vec<Value>` once per *group* (not per row)
    /// when building the final result.
    fn execute_aggregate_i64_group(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        rows: Vec<Row>,
        group_col_idx: usize,
    ) -> Result<ExecResult, ExecError> {
        // Build HashMap<i64, Vec<usize>>: integer key -> row indices
        // NULL rows tracked separately to avoid i64::MIN collision
        let mut key_order: Vec<i64> = Vec::new();
        let mut key_to_indices: HashMap<i64, Vec<usize>> = HashMap::new();
        let mut null_indices: Vec<usize> = Vec::new();

        for (row_idx, row) in rows.iter().enumerate() {
            match &row[group_col_idx] {
                Value::Int32(n) => {
                    let key_i64 = *n as i64;
                    if !key_to_indices.contains_key(&key_i64) {
                        key_order.push(key_i64);
                    }
                    key_to_indices.entry(key_i64).or_default().push(row_idx);
                }
                Value::Int64(n) => {
                    let key_i64 = *n;
                    if !key_to_indices.contains_key(&key_i64) {
                        key_order.push(key_i64);
                    }
                    key_to_indices.entry(key_i64).or_default().push(row_idx);
                }
                _ => {
                    // NULL and non-integer values go to the null group
                    null_indices.push(row_idx);
                }
            };
        }

        // Evaluate projection for each group
        let mut result_columns: Option<Vec<(String, DataType)>> = None;
        let mut result_rows = Vec::new();

        for &key_i64 in &key_order {
            let indices = &key_to_indices[&key_i64];
            // Fix 8: pass original rows + indices instead of cloning rows
            let mut row = Vec::new();
            let mut cols = Vec::new();

            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let val = self.eval_aggregate_expr(expr, &rows, indices, col_meta)?;
                        cols.push((
                            crate::executor::helpers::default_output_name(expr),
                            agg_column_type(&val, expr, col_meta),
                        ));
                        row.push(val);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let val = self.eval_aggregate_expr(expr, &rows, indices, col_meta)?;
                        cols.push((alias.value.clone(), agg_column_type(&val, expr, col_meta)));
                        row.push(val);
                    }
                    SelectItem::Wildcard(_) => {
                        return Err(ExecError::Unsupported("SELECT * with GROUP BY".into()));
                    }
                    _ => {
                        return Err(ExecError::Unsupported("unsupported select item".into()));
                    }
                }
            }

            if result_columns.is_none() {
                result_columns = Some(cols);
            }

            // Apply HAVING
            if let Some(ref having) = select.having {
                let hval = self.eval_aggregate_expr(having, &rows, indices, col_meta)?;
                if hval != Value::Bool(true) {
                    continue;
                }
            }

            result_rows.push(row);
        }

        // Process NULL group separately (rows with NULL or non-integer group keys)
        if !null_indices.is_empty() {
            // Fix 8: pass original rows + null_indices instead of cloning rows
            let mut row = Vec::new();
            let mut cols = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let val = self.eval_aggregate_expr(expr, &rows, &null_indices, col_meta)?;
                        cols.push((
                            crate::executor::helpers::default_output_name(expr),
                            agg_column_type(&val, expr, col_meta),
                        ));
                        row.push(val);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let val = self.eval_aggregate_expr(expr, &rows, &null_indices, col_meta)?;
                        cols.push((alias.value.clone(), agg_column_type(&val, expr, col_meta)));
                        row.push(val);
                    }
                    _ => {}
                }
            }
            if result_columns.is_none() {
                result_columns = Some(cols);
            }
            if let Some(ref having) = select.having {
                let hval = self.eval_aggregate_expr(having, &rows, &null_indices, col_meta)?;
                if hval == Value::Bool(true) {
                    result_rows.push(row);
                }
            } else {
                result_rows.push(row);
            }
        }

        Ok(ExecResult::Select {
            columns: result_columns
                .unwrap_or_else(|| agg_static_columns(&select.projection, col_meta)),
            rows: result_rows,
        })
    }

    /// Fast path for `GROUP BY single_text_column`.
    ///
    /// Uses `HashMap<String, Vec<usize>>` (string key -> row indices) instead of
    /// the generic `HashMap<Vec<Value>, usize>` path.  This eliminates the
    /// `Vec<Value>` wrapper allocation per row and avoids cloning rows into groups
    /// until the aggregate evaluation phase (where we clone only once per group,
    /// not once per row).
    fn execute_aggregate_text_group(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        rows: Vec<Row>,
        group_col_idx: usize,
    ) -> Result<ExecResult, ExecError> {
        // Use Value directly as group key (Hash+Eq implemented) — avoids format!("{:?}") allocation per row
        let mut key_order: Vec<Value> = Vec::new();
        let mut key_to_indices: HashMap<Value, Vec<usize>> = HashMap::new();

        for (row_idx, row) in rows.iter().enumerate() {
            let key_val = row[group_col_idx].clone();
            if let Some(indices) = key_to_indices.get_mut(&key_val) {
                indices.push(row_idx);
            } else {
                key_order.push(key_val.clone());
                key_to_indices.insert(key_val, vec![row_idx]);
            }
        }

        let mut result_columns: Option<Vec<(String, DataType)>> = None;
        let mut result_rows = Vec::new();

        for key_val in &key_order {
            let indices = &key_to_indices[key_val];
            // Fix 8: pass original rows + indices instead of cloning rows

            let mut row = Vec::new();
            let mut cols = Vec::new();

            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let val = self.eval_aggregate_expr(expr, &rows, indices, col_meta)?;
                        cols.push((
                            crate::executor::helpers::default_output_name(expr),
                            agg_column_type(&val, expr, col_meta),
                        ));
                        row.push(val);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let val = self.eval_aggregate_expr(expr, &rows, indices, col_meta)?;
                        cols.push((alias.value.clone(), agg_column_type(&val, expr, col_meta)));
                        row.push(val);
                    }
                    SelectItem::Wildcard(_) => {
                        return Err(ExecError::Unsupported("SELECT * with GROUP BY".into()));
                    }
                    _ => {
                        return Err(ExecError::Unsupported("unsupported select item".into()));
                    }
                }
            }

            if result_columns.is_none() {
                result_columns = Some(cols);
            }

            if let Some(ref having) = select.having {
                let hval = self.eval_aggregate_expr(having, &rows, indices, col_meta)?;
                if hval != Value::Bool(true) {
                    continue;
                }
            }

            result_rows.push(row);
        }

        Ok(ExecResult::Select {
            columns: result_columns
                .unwrap_or_else(|| agg_static_columns(&select.projection, col_meta)),
            rows: result_rows,
        })
    }

    /// Extract GROUPING SETS / CUBE / ROLLUP from GROUP BY expressions.
    /// Returns None if normal GROUP BY, Some(sets) if grouping sets are found.
    pub(super) fn extract_grouping_sets(&self, exprs: &[Expr]) -> Option<Vec<Vec<Expr>>> {
        for expr in exprs {
            match expr {
                Expr::GroupingSets(sets) => {
                    return Some(sets.clone());
                }
                Expr::Cube(cols) => {
                    // CUBE(a, b) = GROUPING SETS ((a,b), (a), (b), ())
                    let mut sets = Vec::new();
                    let n = cols.len();
                    for mask in 0..(1u64 << n) {
                        let mut set = Vec::new();
                        for (i, col) in cols.iter().enumerate() {
                            if mask & (1u64 << i) != 0 {
                                set.extend(col.clone());
                            }
                        }
                        sets.push(set);
                    }
                    return Some(sets);
                }
                Expr::Rollup(cols) => {
                    // ROLLUP(a, b, c) = GROUPING SETS ((a,b,c), (a,b), (a), ())
                    let mut sets = Vec::new();
                    for i in (0..=cols.len()).rev() {
                        let mut set = Vec::new();
                        for col in cols.iter().take(i) {
                            set.extend(col.clone());
                        }
                        sets.push(set);
                    }
                    return Some(sets);
                }
                _ => {}
            }
        }
        None
    }

    /// Execute aggregate with GROUPING SETS — runs the aggregate once per set.
    fn execute_grouping_sets_aggregate(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        rows: &[Row],
        sets: &[Vec<Expr>],
    ) -> Result<ExecResult, ExecError> {
        let mut all_result_rows = Vec::new();
        let mut result_columns: Option<Vec<(String, DataType)>> = None;

        for group_set in sets {
            // Group rows by the current grouping set
            let groups: Vec<(Vec<Value>, Vec<Row>)> = if group_set.is_empty() {
                vec![(Vec::new(), rows.to_vec())]
            } else {
                let mut map: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
                let mut key_to_group_idx: HashMap<Vec<Value>, usize> = HashMap::new();
                for row in rows {
                    let key: Vec<Value> = group_set
                        .iter()
                        .map(|expr| self.eval_row_expr(expr, row, col_meta))
                        .collect::<Result<_, _>>()?;
                    if let Some(idx) = key_to_group_idx.get(&key).copied() {
                        map[idx].1.push(row.clone());
                    } else {
                        let group_idx = map.len();
                        key_to_group_idx.insert(key.clone(), group_idx);
                        map.push((key, vec![row.clone()]));
                    }
                }
                map
            };

            for (_key, group_rows) in &groups {
                let trivial = Self::trivial_indices(group_rows.len());
                let mut row = Vec::new();
                let mut cols = Vec::new();

                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            let val =
                                self.eval_aggregate_expr(expr, group_rows, &trivial, col_meta)?;
                            cols.push((
                                crate::executor::helpers::default_output_name(expr),
                                agg_column_type(&val, expr, col_meta),
                            ));
                            row.push(val);
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let val =
                                self.eval_aggregate_expr(expr, group_rows, &trivial, col_meta)?;
                            cols.push((alias.value.clone(), agg_column_type(&val, expr, col_meta)));
                            row.push(val);
                        }
                        _ => {
                            return Err(ExecError::Unsupported(
                                "unsupported select item in GROUPING SETS".into(),
                            ));
                        }
                    }
                }

                if result_columns.is_none() {
                    result_columns = Some(cols);
                }

                // Apply HAVING
                if let Some(ref having) = select.having {
                    let hval = self.eval_aggregate_expr(having, group_rows, &trivial, col_meta)?;
                    if hval != Value::Bool(true) {
                        continue;
                    }
                }

                all_result_rows.push(row);
            }
        }

        Ok(ExecResult::Select {
            columns: result_columns
                .unwrap_or_else(|| agg_static_columns(&select.projection, col_meta)),
            rows: all_result_rows,
        })
    }

    /// Evaluate an expression in aggregate context: handles both aggregate functions
    /// and plain column references (which take the first row's value, like non-aggregated
    /// columns in GROUP BY).
    ///
    /// Accepts `all_rows` (the full row slice) and `indices` (which rows belong to
    /// this group).  This avoids cloning rows per group — callers that already have
    /// a contiguous `&[Row]` can pass a trivial `0..n` index slice via
    /// [`Self::trivial_indices`].
    pub(super) fn eval_aggregate_expr(
        &self,
        expr: &Expr,
        all_rows: &[Row],
        indices: &[usize],
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        match expr {
            Expr::Function(func) => {
                let fname = func.name.to_string().to_uppercase();
                if Self::is_aggregate_fn_name(&fname) && func.over.is_none() {
                    return self.eval_aggregate_fn(&fname, func, all_rows, indices, col_meta);
                }
                // Non-aggregate function (e.g. COALESCE, ROUND) that may still
                // wrap an aggregate in its arguments — e.g. COALESCE(MAX(id), 0).
                self.eval_scalar_over_aggregates(expr, all_rows, indices, col_meta)
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_aggregate_expr(left, all_rows, indices, col_meta)?;
                let r = self.eval_aggregate_expr(right, all_rows, indices, col_meta)?;
                self.eval_binary_op(&l, op, &r)
            }
            // Casts, CASE, etc. wrapping an aggregate — e.g. CAST(COUNT(*) AS TEXT).
            _ => self.eval_scalar_over_aggregates(expr, all_rows, indices, col_meta),
        }
    }

    /// True for the aggregate function names this executor recognizes.
    fn is_aggregate_fn_name(name: &str) -> bool {
        matches!(
            name,
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "STRING_AGG"
                | "ARRAY_AGG"
                | "JSON_AGG"
                | "BOOL_AND"
                | "BOOL_OR"
                | "EVERY"
                | "BIT_AND"
                | "BIT_OR"
                | "ARGMAX"
                | "ARG_MAX"
                | "ARGMIN"
                | "ARG_MIN"
                | "PERCENTILE_CONT"
                | "PERCENTILE_DISC"
                | "MEDIAN"
                | "QUANTILE"
        )
    }

    /// Evaluate a scalar expression (cast, CASE, COALESCE/ROUND/…) that may wrap
    /// aggregate calls in its sub-expressions. Aggregate sub-expressions are
    /// computed over the group and substituted with their literal values, then
    /// the resulting pure-scalar expression is evaluated with the full row
    /// evaluator against the group's first row (which also resolves any GROUP BY
    /// key column references in the projection).
    fn eval_scalar_over_aggregates(
        &self,
        expr: &Expr,
        all_rows: &[Row],
        indices: &[usize],
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        let empty: Row = Vec::new();
        let row: &Row = indices.first().map(|&i| &all_rows[i]).unwrap_or(&empty);
        if !contains_aggregate(expr) {
            // Pure scalar / group-key expression.
            return self.eval_row_expr(expr, row, col_meta);
        }
        let mut rewritten = expr.clone();
        self.substitute_aggregates_inplace(&mut rewritten, all_rows, indices, col_meta)?;
        // After substitution the expression has no aggregate calls left.
        self.eval_row_expr(&rewritten, row, col_meta)
    }

    /// Recursively replace aggregate function calls in `expr` with the literal
    /// value they evaluate to over the group. Non-aggregate nodes are left in
    /// place (their children are still visited, so nested aggregates inside a
    /// scalar function's arguments are substituted too).
    fn substitute_aggregates_inplace(
        &self,
        expr: &mut Expr,
        all_rows: &[Row],
        indices: &[usize],
        col_meta: &[ColMeta],
    ) -> Result<(), ExecError> {
        if let Expr::Function(func) = expr {
            let fname = func.name.to_string().to_uppercase();
            if func.over.is_none() && Self::is_aggregate_fn_name(&fname) {
                let v = self.eval_aggregate_fn(&fname, func, all_rows, indices, col_meta)?;
                *expr = value_to_ast_expr(&v);
                return Ok(());
            }
        }
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                self.substitute_aggregates_inplace(left, all_rows, indices, col_meta)?;
                self.substitute_aggregates_inplace(right, all_rows, indices, col_meta)?;
            }
            Expr::UnaryOp { expr: inner, .. }
            | Expr::Nested(inner)
            | Expr::Cast { expr: inner, .. } => {
                self.substitute_aggregates_inplace(inner, all_rows, indices, col_meta)?;
            }
            Expr::Function(func) => {
                if let ast::FunctionArguments::List(list) = &mut func.args {
                    for arg in &mut list.args {
                        match arg {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner))
                            | ast::FunctionArg::Named {
                                arg: ast::FunctionArgExpr::Expr(inner),
                                ..
                            } => {
                                self.substitute_aggregates_inplace(
                                    inner, all_rows, indices, col_meta,
                                )?;
                            }
                            _ => {}
                        }
                    }
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.substitute_aggregates_inplace(op, all_rows, indices, col_meta)?;
                }
                for cw in conditions.iter_mut() {
                    self.substitute_aggregates_inplace(
                        &mut cw.condition,
                        all_rows,
                        indices,
                        col_meta,
                    )?;
                    self.substitute_aggregates_inplace(
                        &mut cw.result,
                        all_rows,
                        indices,
                        col_meta,
                    )?;
                }
                if let Some(e) = else_result {
                    self.substitute_aggregates_inplace(e, all_rows, indices, col_meta)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Build a trivial index array `[0, 1, 2, ..., n-1]` for callers that already
    /// have a contiguous group-rows slice.
    #[inline]
    fn trivial_indices(n: usize) -> Vec<usize> {
        (0..n).collect()
    }

    fn eval_aggregate_fn(
        &self,
        fname: &str,
        func: &ast::Function,
        all_rows: &[Row],
        indices: &[usize],
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        // Extract function arguments
        let (arg_expr, arg_expr_2) = match &func.args {
            ast::FunctionArguments::List(list) => {
                let a1 = if list.args.is_empty() {
                    None
                } else {
                    match &list.args[0] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => None,
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    }
                };
                let a2 = if list.args.len() > 1 {
                    match &list.args[1] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    }
                } else {
                    None
                };
                (a1, a2)
            }
            _ => (None, None),
        };

        // Check for DISTINCT
        let is_distinct = match &func.args {
            ast::FunctionArguments::List(list) => {
                matches!(
                    list.duplicate_treatment,
                    Some(ast::DuplicateTreatment::Distinct)
                )
            }
            _ => false,
        };

        // Within-aggregate ORDER BY: `string_agg(t, ',' ORDER BY t)`.
        //
        // This is the only thing that fixes the ORDER of an order-sensitive
        // aggregate — without it `string_agg`/`array_agg` concatenate in scan
        // order, which is arbitrary and was silently wrong rather than
        // unsupported: the clause parsed, and the result came back in table
        // order. Found by `compat/pgregress`. Applied before FILTER only in
        // the sense that the sort is computed here and the filter narrows the
        // same index list below; both compose because sorting preserves
        // membership.
        let agg_order_by: &[ast::OrderByExpr] = match &func.args {
            ast::FunctionArguments::List(list) => list
                .clauses
                .iter()
                .find_map(|c| match c {
                    ast::FunctionArgumentClause::OrderBy(obs) => Some(obs.as_slice()),
                    _ => None,
                })
                .unwrap_or(&[]),
            _ => &[],
        };
        let ordered_indices: Vec<usize>;
        let indices: &[usize] = if agg_order_by.is_empty() {
            indices
        } else {
            let mut sorted = indices.to_vec();
            let mut err: Option<ExecError> = None;
            sorted.sort_by(|&a, &b| {
                if err.is_some() {
                    return std::cmp::Ordering::Equal;
                }
                for ob in agg_order_by {
                    let asc = ob.options.asc.unwrap_or(true);
                    // PostgreSQL's default: NULLs last for ASC, first for DESC.
                    let nulls_first = ob.options.nulls_first.unwrap_or(!asc);
                    let va = match self.eval_row_expr(&ob.expr, &all_rows[a], col_meta) {
                        Ok(v) => v,
                        Err(e) => {
                            err = Some(e);
                            return std::cmp::Ordering::Equal;
                        }
                    };
                    let vb = match self.eval_row_expr(&ob.expr, &all_rows[b], col_meta) {
                        Ok(v) => v,
                        Err(e) => {
                            err = Some(e);
                            return std::cmp::Ordering::Equal;
                        }
                    };
                    let ord = match (&va, &vb) {
                        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                        (Value::Null, _) => {
                            if nulls_first {
                                std::cmp::Ordering::Less
                            } else {
                                std::cmp::Ordering::Greater
                            }
                        }
                        (_, Value::Null) => {
                            if nulls_first {
                                std::cmp::Ordering::Greater
                            } else {
                                std::cmp::Ordering::Less
                            }
                        }
                        _ => {
                            let base = super::helpers::compare_values(&va, &vb)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if asc { base } else { base.reverse() }
                        }
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            if let Some(e) = err {
                return Err(e);
            }
            ordered_indices = sorted;
            &ordered_indices
        };

        // Apply FILTER clause if present — only aggregate over matching indices
        let filtered_indices: Vec<usize>;
        let effective_indices: &[usize] = if let Some(ref filter_expr) = func.filter {
            filtered_indices = indices
                .iter()
                .copied()
                .filter(|&i| {
                    self.eval_where(filter_expr, &all_rows[i], col_meta)
                        .unwrap_or(false)
                })
                .collect();
            &filtered_indices
        } else {
            filtered_indices = Vec::new();
            let _ = &filtered_indices; // suppress unused warning
            indices
        };

        // Collect values with optional DISTINCT (Fix 9: O(n) HashSet dedup)
        let collect_values = |expr: &Expr| -> Result<Vec<Value>, ExecError> {
            let mut vals = Vec::new();
            for &idx in effective_indices {
                let v = self.eval_row_expr(expr, &all_rows[idx], col_meta)?;
                if v != Value::Null {
                    vals.push(v);
                }
            }
            if is_distinct {
                let mut seen = HashSet::new();
                let deduped: Vec<Value> = vals
                    .into_iter()
                    .filter(|v| seen.insert(v.clone()))
                    .collect();
                Ok(deduped)
            } else {
                Ok(vals)
            }
        };

        // Helper: extract i64 column values directly from indexed rows (avoids
        // building a temporary Vec<Row> for the SIMD helpers).
        let extract_i64_indexed = |col_idx: usize| -> Vec<i64> {
            effective_indices
                .iter()
                .filter_map(|&i| {
                    all_rows[i].get(col_idx).and_then(|val| match val {
                        Value::Int32(n) => Some(*n as i64),
                        Value::Int64(n) => Some(*n),
                        _ => None,
                    })
                })
                .collect()
        };

        // Helper: extract f64 column values directly from indexed rows.
        let extract_f64_indexed = |col_idx: usize| -> Vec<f64> {
            effective_indices
                .iter()
                .filter_map(|&i| {
                    all_rows[i].get(col_idx).and_then(|val| match val {
                        Value::Float64(f) => Some(*f),
                        Value::Int32(n) => Some(*n as f64),
                        Value::Int64(n) => Some(*n as f64),
                        _ => None,
                    })
                })
                .collect()
        };

        match fname {
            "COUNT" => {
                if let Some(expr) = arg_expr {
                    // SIMD fast path: for a simple i64 column reference with 1000+
                    // rows, extract values directly instead of building Vec<Value>.
                    // Note: we use the vector length (not simd::aggregates::count_i64)
                    // because SQL COUNT counts all non-null values including zeros,
                    // while simd count_i64 counts only non-zero values.
                    if !is_distinct
                        && func.filter.is_none()
                        && effective_indices.len() >= 1000
                        && let Expr::Identifier(ident) = expr
                        && let Some(col_idx) = col_meta
                            .iter()
                            .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                    {
                        let first = indices.first().and_then(|&i| all_rows[i].get(col_idx));
                        if matches!(first, Some(Value::Int64(_) | Value::Int32(_))) {
                            let col = extract_i64_indexed(col_idx);
                            return Ok(Value::Int64(col.len() as i64));
                        }
                    }
                    let vals = collect_values(expr)?;
                    Ok(Value::Int64(vals.len() as i64))
                } else {
                    // COUNT(*)
                    Ok(Value::Int64(effective_indices.len() as i64))
                }
            }
            "SUM" => {
                let expr = arg_expr
                    .ok_or_else(|| ExecError::Unsupported("SUM requires an argument".into()))?;

                // SIMD fast path: if arg is a simple column reference on i64 data,
                // use vectorized sum.
                if !is_distinct
                    && func.filter.is_none()
                    && let Expr::Identifier(ident) = expr
                    && let Some(col_idx) = col_meta
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                {
                    let first_row = indices.first().map(|&i| &all_rows[i]);
                    let is_int = first_row.is_some_and(|row| {
                        matches!(row.get(col_idx), Some(Value::Int32(_) | Value::Int64(_)))
                    });
                    if is_int {
                        let col = extract_i64_indexed(col_idx);
                        if col.is_empty() {
                            return Ok(Value::Null);
                        }
                        return simd::sum_i64_checked(&col)
                            .map(Value::Int64)
                            .ok_or_else(|| ExecError::Runtime("integer out of range".into()));
                    }
                }

                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                // Use i128 accumulator for integer precision when mixing int+float
                let mut sum_int: i128 = 0;
                let mut sum_f64: f64 = 0.0;
                let mut sum_numeric = Decimal::ZERO;
                let mut is_float = false;
                let mut is_numeric = false;
                let mut has_value = false;
                for val in vals {
                    match val {
                        Value::Int32(n) => {
                            has_value = true;
                            sum_int += n as i128;
                        }
                        Value::Int64(n) => {
                            has_value = true;
                            sum_int += n as i128;
                        }
                        Value::Float64(n) => {
                            if is_numeric {
                                return Err(ExecError::Unsupported(
                                    "SUM cannot mix NUMERIC and floating-point values".into(),
                                ));
                            }
                            has_value = true;
                            is_float = true;
                            sum_f64 += n;
                        }
                        Value::Numeric(value) => {
                            if is_float {
                                return Err(ExecError::Unsupported(
                                    "SUM cannot mix NUMERIC and floating-point values".into(),
                                ));
                            }
                            has_value = true;
                            is_numeric = true;
                            let decimal =
                                crate::types::parse_numeric(&value).map_err(ExecError::Runtime)?;
                            sum_numeric = sum_numeric.checked_add(decimal).ok_or_else(|| {
                                ExecError::Runtime("numeric value out of range".into())
                            })?;
                        }
                        _ => return Err(ExecError::Unsupported("SUM on non-numeric".into())),
                    }
                }
                if !has_value {
                    return Ok(Value::Null);
                }
                if is_numeric {
                    let integer = crate::types::parse_numeric(&sum_int.to_string())
                        .map_err(|_| ExecError::Runtime("numeric value out of range".into()))?;
                    let total = sum_numeric
                        .checked_add(integer)
                        .ok_or_else(|| ExecError::Runtime("numeric value out of range".into()))?;
                    Ok(Value::Numeric(total.normalize().to_string()))
                } else if is_float {
                    Ok(Value::Float64(sum_f64 + sum_int as f64))
                } else {
                    // Return Int64 if it fits, otherwise use Numeric for large sums
                    match i64::try_from(sum_int) {
                        Ok(n) => Ok(Value::Int64(n)),
                        Err(_) => Ok(Value::Numeric(sum_int.to_string())),
                    }
                }
            }
            "AVG" => {
                let expr = arg_expr
                    .ok_or_else(|| ExecError::Unsupported("AVG requires an argument".into()))?;

                // SIMD fast path for simple column references.
                if !is_distinct
                    && func.filter.is_none()
                    && let Expr::Identifier(ident) = expr
                    && let Some(col_idx) = col_meta
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                {
                    let first = indices.first().and_then(|&i| all_rows[i].get(col_idx));
                    match first {
                        Some(Value::Int32(_) | Value::Int64(_)) => {
                            let col = extract_i64_indexed(col_idx);
                            if col.is_empty() {
                                return Ok(Value::Null);
                            }
                            // Checked like SUM's arm above: sum_i64 wraps by
                            // design; overflow must error, not average garbage.
                            return simd::sum_i64_checked(&col)
                                .map(|sum| Value::Float64(sum as f64 / col.len() as f64))
                                .ok_or_else(|| ExecError::Runtime("integer out of range".into()));
                        }
                        Some(Value::Float64(_)) => {
                            let col = extract_f64_indexed(col_idx);
                            if col.is_empty() {
                                return Ok(Value::Null);
                            }
                            return Ok(Value::Float64(simd::sum_f64(&col) / col.len() as f64));
                        }
                        _ => {}
                    }
                }

                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let mut sum: f64 = 0.0;
                // Integers accumulate in checked i64 (mirroring the SIMD arm
                // above): routing them through f64 rounds near-i64::MAX
                // inputs before the divide and silently returns a wrong
                // average instead of the SIMD arm's "integer out of range".
                let mut int_sum: i64 = 0;
                let mut int_overflow = false;
                let mut numeric_sum = Decimal::ZERO;
                let mut numeric = false;
                let mut non_numeric = false;
                let count = vals.len();
                for val in vals {
                    match val {
                        Value::Int32(n) => {
                            non_numeric = true;
                            int_sum = int_sum.checked_add(n as i64).unwrap_or_else(|| {
                                int_overflow = true;
                                i64::MAX
                            });
                            sum += n as f64;
                        }
                        Value::Int64(n) => {
                            non_numeric = true;
                            int_sum = int_sum.checked_add(n).unwrap_or_else(|| {
                                int_overflow = true;
                                i64::MAX
                            });
                            sum += n as f64;
                        }
                        Value::Float64(n) => {
                            non_numeric = true;
                            sum += n;
                        }
                        Value::Numeric(value) => {
                            numeric = true;
                            let decimal =
                                crate::types::parse_numeric(&value).map_err(ExecError::Runtime)?;
                            numeric_sum = numeric_sum.checked_add(decimal).ok_or_else(|| {
                                ExecError::Runtime("numeric value out of range".into())
                            })?;
                        }
                        _ => return Err(ExecError::Unsupported("AVG on non-numeric".into())),
                    }
                }
                if int_overflow {
                    return Err(ExecError::Runtime("integer out of range".into()));
                }
                if numeric {
                    if non_numeric {
                        return Err(ExecError::Unsupported(
                            "AVG cannot mix NUMERIC and floating-point values".into(),
                        ));
                    }
                    let divisor = Decimal::from(count as u64);
                    let average = numeric_sum
                        .checked_div(divisor)
                        .ok_or_else(|| ExecError::Runtime("numeric value out of range".into()))?;
                    Ok(Value::Numeric(average.normalize().to_string()))
                } else {
                    Ok(Value::Float64(sum / count as f64))
                }
            }
            "MIN" => {
                let expr = arg_expr
                    .ok_or_else(|| ExecError::Unsupported("MIN requires an argument".into()))?;

                // SIMD fast path for simple column references.
                // Int32 inputs are excluded so the scalar path can preserve Int32 type.
                if !is_distinct
                    && func.filter.is_none()
                    && let Expr::Identifier(ident) = expr
                    && let Some(col_idx) = col_meta
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                {
                    let first = indices.first().and_then(|&i| all_rows[i].get(col_idx));
                    match first {
                        Some(Value::Float64(_)) => {
                            let col = extract_f64_indexed(col_idx);
                            return Ok(simd::min_f64(&col)
                                .map(Value::Float64)
                                .unwrap_or(Value::Null));
                        }
                        Some(Value::Int64(_)) => {
                            let col = extract_i64_indexed(col_idx);
                            // Use SIMD aggregate kernel for 1000+ i64 values
                            if col.len() >= 1000 {
                                return Ok(simd::aggregates::min_i64(&col)
                                    .map(Value::Int64)
                                    .unwrap_or(Value::Null));
                            }
                            return Ok(col
                                .iter()
                                .copied()
                                .min()
                                .map(Value::Int64)
                                .unwrap_or(Value::Null));
                        }
                        _ => {}
                    }
                }

                let vals = collect_values(expr)?;
                let mut min: Option<Value> = None;
                for val in vals {
                    min = Some(match min {
                        None => val,
                        Some(cur) => {
                            if compare_values(&val, &cur) == Some(std::cmp::Ordering::Less) {
                                val
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(min.unwrap_or(Value::Null))
            }
            "MAX" => {
                let expr = arg_expr
                    .ok_or_else(|| ExecError::Unsupported("MAX requires an argument".into()))?;

                // SIMD fast path for simple column references.
                // Int32 inputs are excluded so the scalar path can preserve Int32 type.
                if !is_distinct
                    && func.filter.is_none()
                    && let Expr::Identifier(ident) = expr
                    && let Some(col_idx) = col_meta
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                {
                    let first = indices.first().and_then(|&i| all_rows[i].get(col_idx));
                    match first {
                        Some(Value::Float64(_)) => {
                            let col = extract_f64_indexed(col_idx);
                            return Ok(simd::max_f64(&col)
                                .map(Value::Float64)
                                .unwrap_or(Value::Null));
                        }
                        Some(Value::Int64(_)) => {
                            let col = extract_i64_indexed(col_idx);
                            // Use SIMD aggregate kernel for 1000+ i64 values
                            if col.len() >= 1000 {
                                return Ok(simd::aggregates::max_i64(&col)
                                    .map(Value::Int64)
                                    .unwrap_or(Value::Null));
                            }
                            return Ok(col
                                .iter()
                                .copied()
                                .max()
                                .map(Value::Int64)
                                .unwrap_or(Value::Null));
                        }
                        _ => {}
                    }
                }

                let vals = collect_values(expr)?;
                let mut max: Option<Value> = None;
                for val in vals {
                    max = Some(match max {
                        None => val,
                        Some(cur) => {
                            if compare_values(&val, &cur) == Some(std::cmp::Ordering::Greater) {
                                val
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(max.unwrap_or(Value::Null))
            }
            "STRING_AGG" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("STRING_AGG requires arguments".into())
                })?;
                let separator = if let Some(sep_expr) = arg_expr_2 {
                    match self.eval_const_expr(sep_expr) {
                        Ok(Value::Text(s)) => s,
                        _ => ",".to_string(),
                    }
                } else {
                    ",".to_string()
                };
                let vals = collect_values(expr)?;
                let strings: Vec<String> = vals
                    .iter()
                    .map(|v| match v {
                        Value::Text(s) => s.clone(),
                        other => format!("{other:?}"),
                    })
                    .collect();
                if strings.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Text(strings.join(&separator)))
                }
            }
            "ARRAY_AGG" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("ARRAY_AGG requires an argument".into())
                })?;
                let vals = collect_values(expr)?;
                Ok(Value::Array(vals))
            }
            "JSON_AGG" => {
                // Collect all non-null values into a JSON array
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("JSON_AGG requires an argument".into())
                })?;
                let acc = collect_values(expr)?;
                let arr: Vec<serde_json::Value> = acc
                    .iter()
                    .map(|v| match v {
                        Value::Null => serde_json::Value::Null,
                        Value::Bool(b) => serde_json::Value::Bool(*b),
                        Value::Int32(n) => serde_json::Value::Number((*n).into()),
                        Value::Int64(n) => serde_json::Value::Number((*n).into()),
                        Value::Float64(f) => serde_json::Value::Number(
                            serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
                        ),
                        Value::Text(s) => serde_json::Value::String(s.clone()),
                        Value::Jsonb(v) => v.clone(),
                        other => serde_json::Value::String(other.to_string()),
                    })
                    .collect();
                Ok(Value::Jsonb(serde_json::Value::Array(arr)))
            }
            "BOOL_AND" | "EVERY" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("BOOL_AND requires an argument".into())
                })?;
                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let result = vals.iter().all(|v| matches!(v, Value::Bool(true)));
                Ok(Value::Bool(result))
            }
            "BOOL_OR" => {
                let expr = arg_expr
                    .ok_or_else(|| ExecError::Unsupported("BOOL_OR requires an argument".into()))?;
                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let result = vals.iter().any(|v| matches!(v, Value::Bool(true)));
                Ok(Value::Bool(result))
            }
            "BIT_AND" => {
                let expr = arg_expr
                    .ok_or_else(|| ExecError::Unsupported("BIT_AND requires an argument".into()))?;
                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let mut result: i64 = !0; // all bits set
                for v in vals {
                    result &= value_to_i64(&v)?;
                }
                Ok(Value::Int64(result))
            }
            "BIT_OR" => {
                let expr = arg_expr
                    .ok_or_else(|| ExecError::Unsupported("BIT_OR requires an argument".into()))?;
                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let mut result: i64 = 0;
                for v in vals {
                    result |= value_to_i64(&v)?;
                }
                Ok(Value::Int64(result))
            }
            // argMax(value, ordering) / argMin(value, ordering): the `value` from
            // the row with the maximum / minimum `ordering`. The cleanest
            // dedup-then-pick primitive for newest-wins reads (teploy-observe D-4).
            "ARGMAX" | "ARG_MAX" | "ARGMIN" | "ARG_MIN" => {
                let val_expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported(format!("{fname} requires (value, ordering) arguments"))
                })?;
                let ord_expr = arg_expr_2.ok_or_else(|| {
                    ExecError::Unsupported(format!("{fname} requires a second ordering argument"))
                })?;
                let want_max = fname == "ARGMAX" || fname == "ARG_MAX";
                let mut best: Option<(Value, Value)> = None; // (ordering, value)
                for &idx in effective_indices {
                    let ord = self.eval_row_expr(ord_expr, &all_rows[idx], col_meta)?;
                    if ord == Value::Null {
                        continue;
                    }
                    match &best {
                        None => {
                            let val = self.eval_row_expr(val_expr, &all_rows[idx], col_meta)?;
                            best = Some((ord, val));
                        }
                        Some((cur_ord, _)) => {
                            let take = match compare_values(&ord, cur_ord) {
                                Some(std::cmp::Ordering::Greater) => want_max,
                                Some(std::cmp::Ordering::Less) => !want_max,
                                _ => false,
                            };
                            if take {
                                let val = self.eval_row_expr(val_expr, &all_rows[idx], col_meta)?;
                                best = Some((ord, val));
                            }
                        }
                    }
                }
                Ok(best.map(|(_, v)| v).unwrap_or(Value::Null))
            }
            // Ordered-set aggregates in functional form: PERCENTILE_CONT(expr,
            // fraction) interpolates, PERCENTILE_DISC(expr, fraction) returns an
            // actual data point, MEDIAN(expr) = percentile_cont 0.5, QUANTILE is a
            // PERCENTILE_CONT alias. Gives p50/p95/p99 without client-side math.
            "PERCENTILE_CONT" | "PERCENTILE_DISC" | "MEDIAN" | "QUANTILE" => {
                let val_expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported(format!("{fname} requires a value argument"))
                })?;
                let fraction = if fname == "MEDIAN" {
                    0.5
                } else {
                    let f_expr = arg_expr_2.ok_or_else(|| {
                        ExecError::Unsupported(format!("{fname} requires a fraction argument"))
                    })?;
                    let probe = effective_indices
                        .first()
                        .map(|&i| all_rows[i].as_slice())
                        .unwrap_or(&[]);
                    let fv = self.eval_row_expr(f_expr, &probe.to_vec(), col_meta)?;
                    value_to_f64(&fv)?
                };
                if !(0.0..=1.0).contains(&fraction) {
                    return Err(ExecError::Unsupported(format!(
                        "{fname} fraction must be between 0 and 1"
                    )));
                }
                let mut vals: Vec<f64> = Vec::new();
                for &idx in effective_indices {
                    let v = self.eval_row_expr(val_expr, &all_rows[idx], col_meta)?;
                    if v != Value::Null {
                        vals.push(value_to_f64(&v)?);
                    }
                }
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = vals.len();
                let result = if fname == "PERCENTILE_DISC" {
                    // Smallest value whose cumulative position >= fraction.
                    let idx = ((fraction * n as f64).ceil() as usize)
                        .saturating_sub(1)
                        .min(n - 1);
                    vals[idx]
                } else {
                    // Continuous: linear interpolation between adjacent ranks.
                    let rank = fraction * (n - 1) as f64;
                    let lo = rank.floor() as usize;
                    let hi = rank.ceil() as usize;
                    if lo == hi {
                        vals[lo]
                    } else {
                        vals[lo] + (vals[hi] - vals[lo]) * (rank - lo as f64)
                    }
                };
                Ok(Value::Float64(result))
            }
            _ => Err(ExecError::Unsupported(format!(
                "unknown aggregate: {fname}"
            ))),
        }
    }

    // ========================================================================
    // Window function execution
    // ========================================================================

    pub(super) fn execute_window_query(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        rows: Vec<Row>,
    ) -> Result<ExecResult, ExecError> {
        let mut result_columns = Vec::new();
        let mut result_rows: Vec<Row> = rows.iter().map(|_| Vec::new()).collect();

        for item in &select.projection {
            let (col_name, expr) = match item {
                SelectItem::UnnamedExpr(e) => (crate::executor::helpers::default_output_name(e), e),
                SelectItem::ExprWithAlias { expr, alias } => (alias.value.clone(), expr),
                SelectItem::Wildcard(_) => {
                    // Expand wildcard
                    for (ci, cm) in col_meta.iter().enumerate() {
                        result_columns.push((cm.name.clone(), cm.dtype.clone()));
                        for (ri, row) in rows.iter().enumerate() {
                            result_rows[ri].push(row[ci].clone());
                        }
                    }
                    continue;
                }
                _ => return Err(ExecError::Unsupported("unsupported select item".into())),
            };

            if let Expr::Function(func) = expr
                && func.over.is_some()
            {
                // Window function — evaluate over partition
                let window_vals = self.eval_window_function(func, &rows, col_meta)?;
                let dtype = if !window_vals.is_empty() {
                    value_type(&window_vals[0])
                } else {
                    DataType::Int64
                };
                result_columns.push((col_name, dtype));
                for (ri, val) in window_vals.into_iter().enumerate() {
                    result_rows[ri].push(val);
                }
                continue;
            }

            // Regular expression — eval per row
            let dtype = if let Some(first_row) = rows.first() {
                let val = self.eval_row_expr(expr, first_row, col_meta)?;
                value_type(&val)
            } else {
                DataType::Text
            };
            result_columns.push((col_name, dtype));
            for (ri, row) in rows.iter().enumerate() {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                result_rows[ri].push(val);
            }
        }

        Ok(ExecResult::Select {
            columns: result_columns,
            rows: result_rows,
        })
    }

    fn eval_window_function(
        &self,
        func: &ast::Function,
        all_rows: &[Row],
        col_meta: &[ColMeta],
    ) -> Result<Vec<Value>, ExecError> {
        let fname = func.name.to_string().to_uppercase();
        let over = func
            .over
            .as_ref()
            .ok_or_else(|| ExecError::Unsupported("window function without OVER".into()))?;

        let (partition_by, order_by_exprs, window_frame) = match over {
            ast::WindowType::WindowSpec(spec) => {
                let ob: Vec<&ast::OrderByExpr> = spec.order_by.iter().collect();
                (&spec.partition_by, ob, spec.window_frame.as_ref())
            }
            _ => {
                return Err(ExecError::Unsupported(
                    "named windows not yet supported".into(),
                ));
            }
        };

        // Build partition groups: (partition_key, Vec<(original_index, row)>)
        #[allow(clippy::type_complexity)]
        let mut partitions: Vec<(Vec<Value>, Vec<(usize, &Row)>)> = Vec::new();
        for (idx, row) in all_rows.iter().enumerate() {
            let key: Vec<Value> = partition_by
                .iter()
                .map(|e| self.eval_row_expr(e, row, col_meta))
                .collect::<Result<_, _>>()?;
            if let Some(part) = partitions.iter_mut().find(|(k, _)| k == &key) {
                part.1.push((idx, row));
            } else {
                partitions.push((key, vec![(idx, row)]));
            }
        }

        // Sort within each partition by ORDER BY
        for (_, members) in &mut partitions {
            if !order_by_exprs.is_empty() {
                members.sort_by(|(_, a), (_, b)| {
                    for ob in &order_by_exprs {
                        let va = self
                            .eval_row_expr(&ob.expr, a, col_meta)
                            .unwrap_or(Value::Null);
                        let vb = self
                            .eval_row_expr(&ob.expr, b, col_meta)
                            .unwrap_or(Value::Null);
                        let asc = ob.options.asc.unwrap_or(true);
                        // PostgreSQL window ORDER BY uses the same NULL default
                        // as a top-level ORDER BY: NULLS LAST for ASC, NULLS
                        // FIRST for DESC. The prior plain compare_values sorted
                        // NULL first for ASC (NULL ranks as smallest), which
                        // put NULL rows at rank 1.
                        let nulls_first = ob.options.nulls_first.unwrap_or(!asc);
                        let ord = super::helpers::cmp_with_nulls(&va, &vb, asc, nulls_first);
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        let mut results = vec![Value::Null; all_rows.len()];

        // Extract the function argument expression (for SUM, AVG, etc.)
        let arg_expr: Option<&Expr> = match &func.args {
            ast::FunctionArguments::List(arg_list) if !arg_list.args.is_empty() => {
                match &arg_list.args[0] {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                }
            }
            _ => None,
        };

        for (_, members) in &partitions {
            let partition_size = members.len();
            for (rank_in_partition, &(orig_idx, row)) in members.iter().enumerate() {
                // Compute window frame bounds for aggregate window functions
                let (frame_start, frame_end) = compute_window_frame_bounds(
                    window_frame,
                    rank_in_partition,
                    partition_size,
                    !order_by_exprs.is_empty(),
                )?;
                let val = match fname.as_str() {
                    "ROW_NUMBER" => Value::Int64(rank_in_partition as i64 + 1),
                    "RANK" => {
                        // RANK: same value gets same rank, with gaps
                        let mut rank = 1i64;
                        for i in 0..rank_in_partition {
                            let prev_row = members[i].1;
                            let curr_row = members[rank_in_partition].1;
                            let same = order_by_exprs.iter().all(|ob| {
                                let va = self
                                    .eval_row_expr(&ob.expr, prev_row, col_meta)
                                    .unwrap_or(Value::Null);
                                let vb = self
                                    .eval_row_expr(&ob.expr, curr_row, col_meta)
                                    .unwrap_or(Value::Null);
                                compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                            });
                            if !same {
                                rank = rank_in_partition as i64 + 1;
                            }
                        }
                        if rank_in_partition > 0 {
                            let prev_row = members[rank_in_partition - 1].1;
                            let curr_row = row;
                            let same = order_by_exprs.iter().all(|ob| {
                                let va = self
                                    .eval_row_expr(&ob.expr, prev_row, col_meta)
                                    .unwrap_or(Value::Null);
                                let vb = self
                                    .eval_row_expr(&ob.expr, curr_row, col_meta)
                                    .unwrap_or(Value::Null);
                                compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                            });
                            if same {
                                // Same rank as previous
                                rank = match &results[members[rank_in_partition - 1].0] {
                                    Value::Int64(r) => *r,
                                    _ => rank_in_partition as i64 + 1,
                                };
                            } else {
                                rank = rank_in_partition as i64 + 1;
                            }
                        }
                        Value::Int64(rank)
                    }
                    "DENSE_RANK" => {
                        let mut rank = 1i64;
                        for i in 1..=rank_in_partition {
                            let prev_row = members[i - 1].1;
                            let curr_row = members[i].1;
                            let same = order_by_exprs.iter().all(|ob| {
                                let va = self
                                    .eval_row_expr(&ob.expr, prev_row, col_meta)
                                    .unwrap_or(Value::Null);
                                let vb = self
                                    .eval_row_expr(&ob.expr, curr_row, col_meta)
                                    .unwrap_or(Value::Null);
                                compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                            });
                            if !same {
                                rank += 1;
                            }
                        }
                        Value::Int64(rank)
                    }
                    "NTILE" => {
                        let n_raw = arg_expr
                            .and_then(|e| self.eval_row_expr(e, row, col_meta).ok())
                            .and_then(|v| value_to_i64(&v).ok())
                            .unwrap_or(1);
                        // NTILE requires a positive bucket count. A negative arg
                        // previously wrapped via `as usize` to a huge value; clamp
                        // to >= 1 and compute in u128 to avoid the rank*n overflow.
                        let n = n_raw.max(1) as u128;
                        let bucket = if partition_size == 0 {
                            1
                        } else {
                            ((rank_in_partition as u128 * n) / partition_size as u128) as i64 + 1
                        };
                        Value::Int64(bucket)
                    }
                    // lag/lead(value [, offset [, default]]) — the offset and
                    // default arguments were ignored (always offset 1, NULL
                    // default). Read all three from the arg list.
                    "LAG" | "LEAD" => {
                        let win_args = window_arg_exprs(func);
                        let offset = match win_args.get(1) {
                            Some(e) => self.eval_row_expr(e, row, col_meta)?,
                            None => Value::Int64(1),
                        };
                        let offset = match offset {
                            Value::Int32(n) => n as isize,
                            Value::Int64(n) => n as isize,
                            _ => 1,
                        };
                        let target = if fname == "LAG" {
                            rank_in_partition as isize - offset
                        } else {
                            rank_in_partition as isize + offset
                        };
                        if target >= 0 && (target as usize) < partition_size {
                            let tgt_row = members[target as usize].1;
                            win_args
                                .first()
                                .map(|e| self.eval_row_expr(e, tgt_row, col_meta))
                                .transpose()?
                                .unwrap_or(Value::Null)
                        } else {
                            // Out of range → the default argument (or NULL).
                            match win_args.get(2) {
                                Some(e) => self.eval_row_expr(e, row, col_meta)?,
                                None => Value::Null,
                            }
                        }
                    }
                    "FIRST_VALUE" => {
                        // The FRAME's first row, not the partition's — the
                        // frame bounds computed above are what SUM/AVG/COUNT
                        // honor, and the value functions must agree.
                        let first_row = members[frame_start].1;
                        arg_expr
                            .map(|e| self.eval_row_expr(e, first_row, col_meta))
                            .transpose()?
                            .unwrap_or(Value::Null)
                    }
                    "LAST_VALUE" => {
                        let last_row = members[frame_end].1;
                        arg_expr
                            .map(|e| self.eval_row_expr(e, last_row, col_meta))
                            .transpose()?
                            .unwrap_or(Value::Null)
                    }
                    "NTH_VALUE" => {
                        // NTH_VALUE(expr, n): the n-th row of the window FRAME
                        // (1-based from frame_start), not of the partition.
                        // Non-positive or beyond-frame n yields NULL (PG).
                        let n = if let Some(second_arg) = self.get_fn_arg(func, 1) {
                            self.eval_row_expr(&second_arg, row, col_meta)
                                .ok()
                                .and_then(|v| value_to_i64(&v).ok())
                                .unwrap_or(1)
                        } else {
                            1
                        };
                        let frame_len = (frame_end - frame_start + 1) as i64;
                        let nth_row = (n >= 1 && n <= frame_len)
                            .then(|| members[frame_start + (n - 1) as usize].1);
                        match nth_row {
                            Some(nth_row) => arg_expr
                                .map(|e| self.eval_row_expr(e, nth_row, col_meta))
                                .transpose()?
                                .unwrap_or(Value::Null),
                            None => Value::Null,
                        }
                    }
                    "PERCENT_RANK" => {
                        if partition_size <= 1 {
                            Value::Float64(0.0)
                        } else {
                            // PERCENT_RANK = (rank - 1) / (partition_size - 1)
                            // rank is computed like RANK (with ties)
                            let mut rank = 1usize;
                            for member in members.iter().take(rank_in_partition) {
                                let prev_row = member.1;
                                let curr_row = row;
                                let same = order_by_exprs.iter().all(|ob| {
                                    let va = self
                                        .eval_row_expr(&ob.expr, prev_row, col_meta)
                                        .unwrap_or(Value::Null);
                                    let vb = self
                                        .eval_row_expr(&ob.expr, curr_row, col_meta)
                                        .unwrap_or(Value::Null);
                                    compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                                });
                                if !same {
                                    rank = rank_in_partition + 1;
                                }
                            }
                            if rank_in_partition > 0 {
                                let prev_row = members[rank_in_partition - 1].1;
                                let same = order_by_exprs.iter().all(|ob| {
                                    let va = self
                                        .eval_row_expr(&ob.expr, prev_row, col_meta)
                                        .unwrap_or(Value::Null);
                                    let vb = self
                                        .eval_row_expr(&ob.expr, row, col_meta)
                                        .unwrap_or(Value::Null);
                                    compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                                });
                                if same {
                                    rank = match &results[members[rank_in_partition - 1].0] {
                                        Value::Float64(r) => {
                                            (r * (partition_size - 1) as f64) as usize + 1
                                        }
                                        _ => rank_in_partition + 1,
                                    };
                                } else {
                                    rank = rank_in_partition + 1;
                                }
                            }
                            Value::Float64((rank - 1) as f64 / (partition_size - 1) as f64)
                        }
                    }
                    "CUME_DIST" => {
                        // CUME_DIST = (number of rows with value <= current) / partition_size
                        let mut count_leq = 0usize;
                        for &(_, other_row) in members.iter() {
                            let same_or_less = order_by_exprs.iter().all(|ob| {
                                let va = self
                                    .eval_row_expr(&ob.expr, other_row, col_meta)
                                    .unwrap_or(Value::Null);
                                let vb = self
                                    .eval_row_expr(&ob.expr, row, col_meta)
                                    .unwrap_or(Value::Null);
                                let asc = ob.options.asc.unwrap_or(true);
                                let ord =
                                    compare_values(&va, &vb).unwrap_or(std::cmp::Ordering::Equal);
                                let ord = if asc { ord } else { ord.reverse() };
                                matches!(ord, std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                            });
                            if same_or_less {
                                count_leq += 1;
                            }
                        }
                        Value::Float64(count_leq as f64 / partition_size as f64)
                    }
                    // Aggregate window functions: SUM, AVG, COUNT, MIN, MAX OVER()
                    "SUM" | "AVG" => {
                        // SQL-standard SUM is NULL-ignoring: skip NULL argument
                        // values and return NULL when the frame contains no
                        // non-NULL value at all (mirrors the non-window SUM's
                        // has_value logic above). Returning 0 for an all-NULL
                        // frame would diverge from Postgres/SQLite and from
                        // Nucleus's own plain-aggregate SUM.
                        let mut values = Vec::new();
                        for &(_, r) in &members[frame_start..=frame_end] {
                            if let Some(e) = arg_expr {
                                let v = self.eval_row_expr(e, r, col_meta)?;
                                if v == Value::Null {
                                    continue;
                                }
                                values.push(v);
                            }
                        }
                        if values.is_empty() {
                            Value::Null
                        } else if values
                            .iter()
                            .any(|value| matches!(value, Value::Numeric(_)))
                        {
                            let mut sum = Decimal::ZERO;
                            for value in &values {
                                let decimal = match value {
                                    Value::Numeric(raw) => crate::types::parse_numeric(raw)
                                        .map_err(ExecError::Runtime)?,
                                    _ => {
                                        return Err(ExecError::Runtime(
                                            "cannot mix NUMERIC with non-NUMERIC in a window aggregate"
                                                .into(),
                                        ));
                                    }
                                };
                                sum = sum.checked_add(decimal).ok_or_else(|| {
                                    ExecError::Runtime("numeric value out of range".into())
                                })?;
                            }
                            let result = if fname == "AVG" {
                                sum.checked_div(Decimal::from(values.len() as u64))
                                    .ok_or_else(|| {
                                        ExecError::Runtime("numeric value out of range".into())
                                    })?
                            } else {
                                sum
                            };
                            Value::Numeric(result.normalize().to_string())
                        } else {
                            let mut sum = 0.0f64;
                            for value in &values {
                                sum += value_to_f64(value)?;
                            }
                            if fname == "AVG" {
                                sum /= values.len() as f64;
                            }
                            Value::Float64(sum)
                        }
                    }
                    "COUNT" => {
                        if let Some(expr) = arg_expr {
                            let mut count = 0i64;
                            for &(_, row) in &members[frame_start..=frame_end] {
                                if self.eval_row_expr(expr, row, col_meta)? != Value::Null {
                                    count += 1;
                                }
                            }
                            Value::Int64(count)
                        } else {
                            Value::Int64((frame_end - frame_start + 1) as i64)
                        }
                    }
                    "MIN" => {
                        let mut min_val = Value::Null;
                        for &(_, r) in &members[frame_start..=frame_end] {
                            if let Some(e) = arg_expr {
                                let v = self.eval_row_expr(e, r, col_meta)?;
                                if min_val == Value::Null
                                    || compare_values(&v, &min_val)
                                        == Some(std::cmp::Ordering::Less)
                                {
                                    min_val = v;
                                }
                            }
                        }
                        min_val
                    }
                    "MAX" => {
                        let mut max_val = Value::Null;
                        for &(_, r) in &members[frame_start..=frame_end] {
                            if let Some(e) = arg_expr {
                                let v = self.eval_row_expr(e, r, col_meta)?;
                                if max_val == Value::Null
                                    || compare_values(&v, &max_val)
                                        == Some(std::cmp::Ordering::Greater)
                                {
                                    max_val = v;
                                }
                            }
                        }
                        max_val
                    }
                    _ => {
                        return Err(ExecError::Unsupported(format!(
                            "window function {fname} not supported"
                        )));
                    }
                };
                results[orig_idx] = val;
            }
        }

        Ok(results)
    }

    /// Get the Nth function argument as an Expr.
    fn get_fn_arg(&self, func: &ast::Function, n: usize) -> Option<Expr> {
        match &func.args {
            ast::FunctionArguments::List(arg_list) if arg_list.args.len() > n => {
                match &arg_list.args[n] {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}
