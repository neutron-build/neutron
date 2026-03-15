//! Expression evaluation methods for the query executor.
//!
//! Contains constant-expression evaluation, row-context expression evaluation,
//! binary/unary operators, JSONB operators, WHERE clause filtering (serial and
//! parallel via Rayon), lazy materialization via filter_positions, and type-casting logic.

use std::cell::Cell;
use std::cmp::Ordering;
use std::sync::atomic::Ordering as AtomicOrdering;

#[cfg(feature = "server")]
use rayon::prelude::*;
use sqlparser::ast::{self, Expr};

use crate::types::{Row, Value};
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};
use super::helpers::*;
use super::session::sync_block_on;

// ---------------------------------------------------------------------------
// Lazy Materialization — Phase 2C
// ---------------------------------------------------------------------------

/// Result of lazy WHERE clause evaluation: positions of matching rows only.
/// Memory usage: ~4 bytes per evaluated row (u32 index) instead of 100-1000 bytes
/// per full row materialization.
#[derive(Debug, Clone)]
pub struct FilterResult {
    /// Indices of rows that matched the filter predicate.
    pub matching_positions: Vec<u32>,
    /// Total number of rows evaluated (including non-matching).
    pub total_rows: u32,
}

impl FilterResult {
    /// Create a new empty filter result.
    pub fn empty() -> Self {
        Self {
            matching_positions: Vec::new(),
            total_rows: 0,
        }
    }

    /// Create a result that matches all rows (full scan, all match).
    pub fn all(total: u32) -> Self {
        Self {
            matching_positions: (0..total).collect(),
            total_rows: total,
        }
    }

    /// Memory savings estimate in bytes.
    /// Assumes ~100 bytes per full row (conservative).
    pub fn estimated_memory_savings(&self) -> u64 {
        let non_matching = self.total_rows as u64 - self.matching_positions.len() as u64;
        non_matching * 100
    }

    /// Hit rate: percentage of rows that matched filter.
    pub fn hit_rate(&self) -> f64 {
        if self.total_rows == 0 {
            100.0
        } else {
            (self.matching_positions.len() as f64 / self.total_rows as f64) * 100.0
        }
    }
}

// ---------------------------------------------------------------------------
// Expression depth guard — prevents stack overflow on deeply nested
// expressions (e.g., 500-deep AND/OR chains). Uses a thread-local counter
// so it works correctly with Rayon parallel evaluation.
// ---------------------------------------------------------------------------

const MAX_EXPR_DEPTH: u32 = 256;

thread_local! {
    static EXPR_DEPTH: Cell<u32> = const { Cell::new(0) };
}

struct ExprDepthGuard;

impl ExprDepthGuard {
    #[inline]
    fn enter() -> Result<Self, ExecError> {
        EXPR_DEPTH.with(|d| {
            let depth = d.get();
            if depth >= MAX_EXPR_DEPTH {
                return Err(ExecError::Runtime(
                    "expression nesting depth exceeded (limit 256)".into(),
                ));
            }
            d.set(depth + 1);
            Ok(ExprDepthGuard)
        })
    }
}

impl Drop for ExprDepthGuard {
    fn drop(&mut self) {
        EXPR_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

impl Executor {
    /// Evaluate a constant expression (no table context).
    pub(super) fn eval_const_expr(&self, expr: &Expr) -> Result<Value, ExecError> {
        let _guard = ExprDepthGuard::enter()?;
        match expr {
            Expr::Value(val) => self.eval_value(&val.value),
            Expr::UnaryOp { op, expr } => {
                let val = self.eval_const_expr(expr)?;
                match (op, val) {
                    (ast::UnaryOperator::Minus, Value::Int32(n)) => n.checked_neg().map(Value::Int32).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    (ast::UnaryOperator::Minus, Value::Int64(n)) => n.checked_neg().map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    (ast::UnaryOperator::Minus, Value::Float64(n)) => Ok(Value::Float64(-n)),
                    _ => Err(ExecError::Unsupported("unsupported unary op".into())),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_const_expr(left)?;
                let r = self.eval_const_expr(right)?;
                self.eval_binary_op(&l, op, &r)
            }
            Expr::Nested(inner) => self.eval_const_expr(inner),
            Expr::Cast { expr, data_type, .. } => {
                let val = self.eval_const_expr(expr)?;
                self.eval_cast(val, data_type)
            }
            Expr::Function(func) => {
                // Evaluate scalar function in constant context (no row)
                let empty_row: Row = Vec::new();
                let empty_meta: Vec<ColMeta> = Vec::new();
                self.eval_row_expr(expr, &empty_row, &empty_meta)
                    .or_else(|_| {
                        // If row_expr fails (e.g. needs row context), try as const
                        let fname = func.name.to_string().to_uppercase();
                        self.eval_scalar_fn(&fname, func, &empty_row, &empty_meta)
                    })
            }
            // Delegate special expressions (Trim, Substring, Ceil, Floor, Position, Overlay,
            // TypedString) to eval_row_expr with empty context
            Expr::TypedString(_)
            | Expr::Trim { .. }
            | Expr::Substring { .. }
            | Expr::Ceil { .. }
            | Expr::Floor { .. }
            | Expr::Position { .. }
            | Expr::Overlay { .. }
            | Expr::Extract { .. }
            | Expr::IsDistinctFrom(_, _)
            | Expr::IsNotDistinctFrom(_, _)
            | Expr::Array(_)
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. } => {
                let empty_row: Row = Vec::new();
                let empty_meta: Vec<ColMeta> = Vec::new();
                self.eval_row_expr(expr, &empty_row, &empty_meta)
            }
            // Subqueries in constant context
            Expr::Subquery(subquery) => {
                let sub_result = sync_block_on(self.execute_query(*subquery.clone()))?;
                match sub_result {
                    ExecResult::Select { rows, .. } => {
                        if rows.is_empty() || rows[0].is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(rows[0][0].clone())
                        }
                    }
                    _ => Ok(Value::Null),
                }
            }
            Expr::Exists { subquery, negated } => {
                let sub_result = sync_block_on(self.execute_query(*subquery.clone()))?;
                let has_rows = matches!(&sub_result, ExecResult::Select { rows, .. } if !rows.is_empty());
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            _ => Err(ExecError::Unsupported(format!("expression: {expr}"))),
        }
    }

    pub(super) fn eval_value(&self, val: &ast::Value) -> Result<Value, ExecError> {
        match val {
            ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i32>() {
                    Ok(Value::Int32(i))
                } else if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float64(f))
                } else {
                    Err(ExecError::Unsupported(format!("number: {n}")))
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                Ok(Value::Text(s.clone()))
            }
            ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
            ast::Value::Null => Ok(Value::Null),
            _ => Err(ExecError::Unsupported(format!("value: {val}"))),
        }
    }

    /// Evaluate JSONB arrow operator: `jsonb_val -> key` (returns JSONB).
    pub(super) fn eval_json_arrow(&self, left: &Value, key: &Value) -> Result<Value, ExecError> {
        let parsed_json;
        let json = match left {
            Value::Jsonb(v) => v,
            Value::Text(s) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => { parsed_json = v; &parsed_json }
                    Err(_) => return Ok(Value::Null),
                }
            }
            _ => return Ok(Value::Null),
        };

        let result = match key {
            Value::Text(k) => json.get(k.as_str()).cloned(),
            Value::Int32(i) => json.get(*i as usize).cloned(),
            Value::Int64(i) => json.get(*i as usize).cloned(),
            _ => None,
        };

        match result {
            Some(v) => Ok(Value::Jsonb(v)),
            None => Ok(Value::Null),
        }
    }

    /// Evaluate JSONB double arrow operator: `jsonb_val ->> key` (returns Text).
    pub(super) fn eval_json_double_arrow(&self, left: &Value, key: &Value) -> Result<Value, ExecError> {
        let result = self.eval_json_arrow(left, key)?;
        match result {
            Value::Jsonb(serde_json::Value::String(s)) => Ok(Value::Text(s)),
            Value::Jsonb(v) => Ok(Value::Text(v.to_string())),
            Value::Null => Ok(Value::Null),
            other => Ok(Value::Text(other.to_string())),
        }
    }

    /// Evaluate JSONB path arrow operator: `jsonb_val #> '{a,b}'` (returns JSONB).
    pub(super) fn eval_json_path_arrow(&self, left: &Value, path: &Value) -> Result<Value, ExecError> {
        let json = match left {
            Value::Jsonb(v) => v.clone(),
            Value::Text(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(v) => v,
                Err(_) => return Ok(Value::Null),
            },
            _ => return Ok(Value::Null),
        };
        let path_str = match path {
            Value::Text(s) => s.clone(),
            _ => return Ok(Value::Null),
        };
        let trimmed = path_str.trim();
        if trimmed.starts_with('{') && trimmed.ends_with('}') {
            let inner = &trimmed[1..trimmed.len() - 1];
            let keys: Vec<&str> = if inner.is_empty() { vec![] } else { inner.split(',').collect() };
            let mut current = json;
            for key in &keys {
                let k = key.trim();
                let next = if let Ok(idx) = k.parse::<usize>() {
                    current.get(idx).cloned()
                } else {
                    current.get(k).cloned()
                };
                match next {
                    Some(v) => current = v,
                    None => return Ok(Value::Null),
                }
            }
            Ok(Value::Jsonb(current))
        } else {
            Ok(Value::Null)
        }
    }

    /// Evaluate JSONB path long-arrow operator: `jsonb_val #>> '{a,b}'` (returns Text).
    pub(super) fn eval_json_path_long_arrow(&self, left: &Value, path: &Value) -> Result<Value, ExecError> {
        let result = self.eval_json_path_arrow(left, path)?;
        match result {
            Value::Jsonb(serde_json::Value::String(s)) => Ok(Value::Text(s)),
            Value::Jsonb(v) => Ok(Value::Text(v.to_string())),
            Value::Null => Ok(Value::Null),
            other => Ok(Value::Text(other.to_string())),
        }
    }

    pub(super) fn eval_binary_op(
        &self,
        left: &Value,
        op: &ast::BinaryOperator,
        right: &Value,
    ) -> Result<Value, ExecError> {
        // SQL 3-valued logic: comparisons with NULL yield NULL
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            match op {
                ast::BinaryOperator::Eq
                | ast::BinaryOperator::NotEq
                | ast::BinaryOperator::Lt
                | ast::BinaryOperator::Gt
                | ast::BinaryOperator::LtEq
                | ast::BinaryOperator::GtEq => return Ok(Value::Null),
                _ => {}
            }
        }
        // Comparison operators work across all comparable types
        match op {
            ast::BinaryOperator::Eq => {
                return Ok(Value::Bool(
                    compare_values(left, right) == Some(Ordering::Equal),
                ));
            }
            ast::BinaryOperator::NotEq => {
                return Ok(Value::Bool(
                    compare_values(left, right) != Some(Ordering::Equal),
                ));
            }
            ast::BinaryOperator::Lt => {
                return Ok(Value::Bool(
                    compare_values(left, right) == Some(Ordering::Less),
                ));
            }
            ast::BinaryOperator::Gt => {
                return Ok(Value::Bool(
                    compare_values(left, right) == Some(Ordering::Greater),
                ));
            }
            ast::BinaryOperator::LtEq => {
                return Ok(Value::Bool(matches!(
                    compare_values(left, right),
                    Some(Ordering::Less | Ordering::Equal)
                )));
            }
            ast::BinaryOperator::GtEq => {
                return Ok(Value::Bool(matches!(
                    compare_values(left, right),
                    Some(Ordering::Greater | Ordering::Equal)
                )));
            }
            // JSONB operators
            ast::BinaryOperator::Arrow => {
                return self.eval_json_arrow(left, right);
            }
            ast::BinaryOperator::LongArrow => {
                return self.eval_json_double_arrow(left, right);
            }
            ast::BinaryOperator::HashArrow => {
                return self.eval_json_path_arrow(left, right);
            }
            ast::BinaryOperator::HashLongArrow => {
                return self.eval_json_path_long_arrow(left, right);
            }
            // SQL 3-valued AND: FALSE AND anything = FALSE; TRUE AND NULL = NULL
            ast::BinaryOperator::And => {
                return match (left, right) {
                    (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
                    (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Ok(Value::Bool(false)),
                };
            }
            // SQL 3-valued OR: TRUE OR anything = TRUE; FALSE OR NULL = NULL
            ast::BinaryOperator::Or => {
                return match (left, right) {
                    (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                    (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Ok(Value::Bool(false)),
                };
            }
            _ => {}
        }

        // Arithmetic and string operations
        match (left, right) {
            (Value::Int32(l), Value::Int32(r)) => match op {
                ast::BinaryOperator::Plus => l.checked_add(*r).map(Value::Int32).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Minus => l.checked_sub(*r).map(Value::Int32).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Multiply => l.checked_mul(*r).map(Value::Int32).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Divide if *r == 0 => Err(ExecError::Runtime("division by zero".into())),
                ast::BinaryOperator::Divide => l.checked_div(*r).map(Value::Int32).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Modulo if *r == 0 => Err(ExecError::Runtime("division by zero".into())),
                ast::BinaryOperator::Modulo => Ok(Value::Int32(l % r)),
                _ => Err(ExecError::Unsupported(format!("op: {op}"))),
            },
            (Value::Int64(l), Value::Int64(r)) => match op {
                ast::BinaryOperator::Plus => l.checked_add(*r).map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Minus => l.checked_sub(*r).map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Multiply => l.checked_mul(*r).map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Divide if *r == 0 => Err(ExecError::Runtime("division by zero".into())),
                ast::BinaryOperator::Divide => l.checked_div(*r).map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Modulo if *r == 0 => Err(ExecError::Runtime("division by zero".into())),
                ast::BinaryOperator::Modulo => Ok(Value::Int64(l % r)),
                _ => Err(ExecError::Unsupported(format!("op: {op}"))),
            },
            // Cross-promote Int32 <-> Int64
            (Value::Int32(l), Value::Int64(_)) => {
                self.eval_binary_op(&Value::Int64(*l as i64), op, right)
            }
            (Value::Int64(_), Value::Int32(r)) => {
                self.eval_binary_op(left, op, &Value::Int64(*r as i64))
            }
            (Value::Float64(l), Value::Float64(r)) => match op {
                ast::BinaryOperator::Plus => Ok(Value::Float64(l + r)),
                ast::BinaryOperator::Minus => Ok(Value::Float64(l - r)),
                ast::BinaryOperator::Multiply => Ok(Value::Float64(l * r)),
                ast::BinaryOperator::Divide if *r == 0.0 => Err(ExecError::Runtime("division by zero".into())),
                ast::BinaryOperator::Divide => Ok(Value::Float64(l / r)),
                _ => Err(ExecError::Unsupported(format!("op: {op}"))),
            },
            // Promote int to float
            (Value::Int32(l), Value::Float64(_)) => {
                self.eval_binary_op(&Value::Float64(*l as f64), op, right)
            }
            (Value::Float64(_), Value::Int32(r)) => {
                self.eval_binary_op(left, op, &Value::Float64(*r as f64))
            }
            (Value::Int64(l), Value::Float64(_)) => {
                self.eval_binary_op(&Value::Float64(*l as f64), op, right)
            }
            (Value::Float64(_), Value::Int64(r)) => {
                self.eval_binary_op(left, op, &Value::Float64(*r as f64))
            }
            (Value::Text(l), Value::Text(r)) => match op {
                ast::BinaryOperator::StringConcat => Ok(Value::Text(format!("{l}{r}"))),
                _ => Err(ExecError::Unsupported(format!("op on text: {op}"))),
            },
            _ => Err(ExecError::Unsupported(format!(
                "type mismatch for {op}: {left:?} vs {right:?}"
            ))),
        }
    }

    /// Evaluate a WHERE clause expression against a row.
    pub(super) fn eval_where(&self, expr: &Expr, row: &Row, col_meta: &[ColMeta]) -> Result<bool, ExecError> {
        match self.eval_row_expr(expr, row, col_meta)? {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            other => Err(ExecError::Unsupported(format!("WHERE expects boolean, got {other}"))),
        }
    }

    /// Parallel WHERE filter for large result sets using Rayon.
    /// Falls back to serial for small sets (below `PARALLEL_THRESHOLD`).
    pub(super) fn parallel_filter(
        &self,
        rows: Vec<Row>,
        where_expr: &Expr,
        col_meta: &[ColMeta],
    ) -> Vec<Row> {
        /// Minimum row count before switching to parallel evaluation.
        const PARALLEL_THRESHOLD: usize = 10_000;

        if cfg!(feature = "server") && rows.len() >= PARALLEL_THRESHOLD {
            // Parallel path using Rayon (server builds only)
            #[cfg(feature = "server")]
            {
                rows.into_par_iter()
                    .filter(|row| self.eval_where(where_expr, row, col_meta).unwrap_or(false))
                    .collect()
            }
            #[cfg(not(feature = "server"))]
            { unreachable!() }
        } else {
            // Serial path for small result sets or non-server (WASM) builds
            rows.into_iter()
                .filter(|row| self.eval_where(where_expr, row, col_meta).unwrap_or(false))
                .collect()
        }
    }

    /// Lazy WHERE filter — returns only matching row indices instead of full rows.
    /// Phase 2C: Memory optimization using deferred materialization.
    ///
    /// For large result sets with selective WHERE filters, this returns 4 bytes per
    /// row (u32 index) instead of 100-1000 bytes per full row. Row reconstruction
    /// happens only for matching positions in downstream operators.
    ///
    /// # Parameters
    /// - `rows`: Input rows to evaluate
    /// - `where_expr`: Filter expression to apply
    /// - `col_meta`: Column metadata for resolving column references
    ///
    /// # Returns
    /// `FilterResult` containing matching row indices and statistics.
    #[allow(dead_code)]
    pub(super) fn filter_positions(
        &self,
        rows: &[Row],
        where_expr: &Expr,
        col_meta: &[ColMeta],
    ) -> Result<FilterResult, ExecError> {
        /// Minimum row count before switching to parallel evaluation.
        const PARALLEL_THRESHOLD: usize = 10_000;

        let total_rows = rows.len() as u32;

        if cfg!(feature = "server") && rows.len() >= PARALLEL_THRESHOLD {
            // Parallel path using Rayon (server builds only)
            #[cfg(feature = "server")]
            {
                let positions = rows
                    .par_iter()
                    .enumerate()
                    .filter(|(_, row)| self.eval_where(where_expr, row, col_meta).unwrap_or(false))
                    .map(|(idx, _)| idx as u32)
                    .collect();

                Ok(FilterResult {
                    matching_positions: positions,
                    total_rows,
                })
            }
            #[cfg(not(feature = "server"))]
            {
                // Fallback to serial for non-server builds
                let positions = rows
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| self.eval_where(where_expr, row, col_meta).unwrap_or(false))
                    .map(|(idx, _)| idx as u32)
                    .collect();

                Ok(FilterResult {
                    matching_positions: positions,
                    total_rows,
                })
            }
        } else {
            // Serial path for small result sets or non-server (WASM) builds
            let positions = rows
                .iter()
                .enumerate()
                .filter(|(_, row)| self.eval_where(where_expr, row, col_meta).unwrap_or(false))
                .map(|(idx, _)| idx as u32)
                .collect();

            Ok(FilterResult {
                matching_positions: positions,
                total_rows,
            })
        }
    }

    /// Reconstruct full rows from filtered positions.
    /// Used by downstream operators after WHERE evaluation.
    #[allow(dead_code)]
    pub(super) fn reconstruct_rows_from_positions(
        &self,
        all_rows: &[Row],
        positions: &[u32],
    ) -> Vec<Row> {
        positions
            .iter()
            .filter_map(|&idx| all_rows.get(idx as usize).cloned())
            .collect()
    }

    /// Evaluate an expression with row context (supports column references).
    pub(super) fn eval_row_expr(
        &self,
        expr: &Expr,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        let _guard = ExprDepthGuard::enter()?;
        match expr {
            Expr::Identifier(ident) => {
                let idx = self.resolve_column(col_meta, None, &ident.value)?;
                Ok(row[idx].clone())
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let idx =
                    self.resolve_column(col_meta, Some(&parts[0].value), &parts[1].value)?;
                Ok(row[idx].clone())
            }
            Expr::Value(val) => self.eval_value(&val.value),
            // Typed string literals: TIMESTAMP '2024-01-01', DATE '2024-01-01', UUID 'xxx'
            Expr::TypedString(ts) => {
                let s = match &ts.value.value {
                    ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => s.clone(),
                    other => other.to_string(),
                };
                match &ts.data_type {
                    ast::DataType::Timestamp(_, tz) => {
                        if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(&s) {
                            let days = crate::types::ymd_to_days(y, m, d) as i64;
                            let us = days * 86400 * 1_000_000
                                + h as i64 * 3_600_000_000
                                + min as i64 * 60_000_000
                                + sec as i64 * 1_000_000;
                            if matches!(tz, ast::TimezoneInfo::WithTimeZone) {
                                Ok(Value::TimestampTz(us))
                            } else {
                                Ok(Value::Timestamp(us))
                            }
                        } else {
                            Ok(Value::Text(s))
                        }
                    }
                    ast::DataType::TimestampNtz(_) => {
                        if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(&s) {
                            let days = crate::types::ymd_to_days(y, m, d) as i64;
                            let us = days * 86400 * 1_000_000
                                + h as i64 * 3_600_000_000
                                + min as i64 * 60_000_000
                                + sec as i64 * 1_000_000;
                            Ok(Value::Timestamp(us))
                        } else {
                            Ok(Value::Text(s))
                        }
                    }
                    ast::DataType::Date => {
                        let parts: Vec<&str> = s.splitn(3, '-').collect();
                        if parts.len() >= 3
                            && let (Ok(y), Ok(m), Ok(d)) = (
                                parts[0].parse::<i32>(),
                                parts[1].parse::<u32>(),
                                parts[2].trim().parse::<u32>(),
                            ) {
                                return Ok(Value::Date(crate::types::ymd_to_days(y, m, d)));
                            }
                        Ok(Value::Text(s))
                    }
                    ast::DataType::Uuid => {
                        match crate::types::parse_uuid(&s) {
                            Ok(bytes) => Ok(Value::Uuid(bytes)),
                            Err(_) => Ok(Value::Text(s)),
                        }
                    }
                    _ => Ok(Value::Text(s)),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let r = self.eval_row_expr(right, row, col_meta)?;
                self.eval_binary_op(&l, op, &r)
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match (op, val) {
                    (ast::UnaryOperator::Minus, Value::Int32(n)) => n.checked_neg().map(Value::Int32).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    (ast::UnaryOperator::Minus, Value::Int64(n)) => n.checked_neg().map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    (ast::UnaryOperator::Minus, Value::Float64(n)) => Ok(Value::Float64(-n)),
                    (ast::UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
                    _ => Err(ExecError::Unsupported("unsupported unary op".into())),
                }
            }
            Expr::Nested(inner) => self.eval_row_expr(inner, row, col_meta),
            Expr::IsNull(inner) => {
                let val = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(val == Value::Null))
            }
            Expr::IsNotNull(inner) => {
                let val = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(val != Value::Null))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let lo = self.eval_row_expr(low, row, col_meta)?;
                let hi = self.eval_row_expr(high, row, col_meta)?;
                // SQL 3-valued logic: BETWEEN with any NULL operand yields NULL
                if matches!(val, Value::Null) || matches!(lo, Value::Null) || matches!(hi, Value::Null) {
                    return Ok(Value::Null);
                }
                let in_range = matches!(compare_values(&val, &lo), Some(Ordering::Greater | Ordering::Equal))
                    && matches!(compare_values(&val, &hi), Some(Ordering::Less | Ordering::Equal));
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Cast { expr, data_type, .. } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                self.eval_cast(val, data_type)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                // Compare by reference — avoids cloning val for every list item
                let found = list
                    .iter()
                    .any(|item| self.eval_row_expr(item, row, col_meta).ok().as_ref() == Some(&val));
                Ok(Value::Bool(if *negated { !found } else { found }))
            }
            Expr::Function(func) => {
                let fname = func.name.to_string().to_uppercase();
                // Don't handle aggregates here -- they're handled in eval_aggregate_expr
                if matches!(fname.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    return Err(ExecError::Unsupported(
                        format!("aggregate function {fname} outside of aggregate context"),
                    ));
                }
                self.eval_scalar_fn(&fname, func, row, col_meta)
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let pat = self.eval_row_expr(pattern, row, col_meta)?;
                match (&val, &pat) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Text(s), Value::Text(p)) => {
                        let matched = like_match(s, p);
                        Ok(Value::Bool(if *negated { !matched } else { matched }))
                    }
                    _ => Ok(Value::Bool(false)),
                }
            }
            Expr::ILike {
                negated,
                expr,
                pattern,
                ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let pat = self.eval_row_expr(pattern, row, col_meta)?;
                match (&val, &pat) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Text(s), Value::Text(p)) => {
                        let matched = like_match(&s.to_lowercase(), &p.to_lowercase());
                        Ok(Value::Bool(if *negated { !matched } else { matched }))
                    }
                    _ => Ok(Value::Bool(false)),
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    // Simple CASE: CASE expr WHEN val1 THEN res1 ...
                    let op_val = self.eval_row_expr(op, row, col_meta)?;
                    for case_when in conditions {
                        let cond_val = self.eval_row_expr(&case_when.condition, row, col_meta)?;
                        if compare_values(&op_val, &cond_val) == Some(Ordering::Equal) {
                            return self.eval_row_expr(&case_when.result, row, col_meta);
                        }
                    }
                } else {
                    // Searched CASE: CASE WHEN cond1 THEN res1 ...
                    for case_when in conditions {
                        if self.eval_where(&case_when.condition, row, col_meta)? {
                            return self.eval_row_expr(&case_when.result, row, col_meta);
                        }
                    }
                }
                if let Some(else_expr) = else_result {
                    self.eval_row_expr(else_expr, row, col_meta)
                } else {
                    Ok(Value::Null)
                }
            }
            // -- Special expression types that sqlparser doesn't parse as Expr::Function --
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
                ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match val {
                    Value::Text(s) => {
                        let trimmed = if let Some(what) = trim_what {
                            let what_val = self.eval_row_expr(what, row, col_meta)?;
                            let chars: Vec<char> = what_val.to_string().chars().collect();
                            match trim_where {
                                Some(ast::TrimWhereField::Leading) => {
                                    s.trim_start_matches(chars.as_slice()).to_string()
                                }
                                Some(ast::TrimWhereField::Trailing) => {
                                    s.trim_end_matches(chars.as_slice()).to_string()
                                }
                                _ => s
                                    .trim_start_matches(chars.as_slice())
                                    .trim_end_matches(chars.as_slice())
                                    .to_string(),
                            }
                        } else {
                            match trim_where {
                                Some(ast::TrimWhereField::Leading) => {
                                    s.trim_start().to_string()
                                }
                                Some(ast::TrimWhereField::Trailing) => {
                                    s.trim_end().to_string()
                                }
                                _ => s.trim().to_string(),
                            }
                        };
                        Ok(Value::Text(trimmed))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(val.to_string().trim().to_string())),
                }
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match val {
                    Value::Text(s) => {
                        let from = if let Some(f) = substring_from {
                            let v = self.eval_row_expr(f, row, col_meta)?;
                            value_to_i64(&v).unwrap_or(1) as usize
                        } else {
                            1
                        };
                        // SQL SUBSTRING is 1-based
                        let start = if from > 0 { from - 1 } else { 0 };
                        // Use skip/take on char iterator — avoids Vec<char> allocation
                        let result: String = if let Some(f) = substring_for {
                            let v = self.eval_row_expr(f, row, col_meta)?;
                            let len = value_to_i64(&v).unwrap_or(0) as usize;
                            s.chars().skip(start).take(len).collect()
                        } else {
                            s.chars().skip(start).collect()
                        };
                        Ok(Value::Text(result))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("SUBSTRING on non-text".into())),
                }
            }
            Expr::Ceil { expr, .. } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match val {
                    Value::Float64(f) => Ok(Value::Float64(f.ceil())),
                    Value::Int32(n) => Ok(Value::Float64((n as f64).ceil())),
                    Value::Int64(n) => Ok(Value::Float64((n as f64).ceil())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("CEIL on non-numeric".into())),
                }
            }
            Expr::Floor { expr, .. } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match val {
                    Value::Float64(f) => Ok(Value::Float64(f.floor())),
                    Value::Int32(n) => Ok(Value::Float64((n as f64).floor())),
                    Value::Int64(n) => Ok(Value::Float64((n as f64).floor())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("FLOOR on non-numeric".into())),
                }
            }
            Expr::Position { expr, r#in } => {
                let needle = self.eval_row_expr(expr, row, col_meta)?;
                let haystack = self.eval_row_expr(r#in, row, col_meta)?;
                match (&needle, &haystack) {
                    (Value::Text(n), Value::Text(h)) => {
                        let pos = h.find(n.as_str()).map(|i| i + 1).unwrap_or(0);
                        Ok(Value::Int32(pos as i32))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Ok(Value::Int32(0)),
                }
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let base = self.eval_row_expr(expr, row, col_meta)?;
                let replacement = self.eval_row_expr(overlay_what, row, col_meta)?;
                let from = self.eval_row_expr(overlay_from, row, col_meta)?;
                match (&base, &replacement, &from) {
                    (Value::Text(s), Value::Text(r), _) => {
                        let start = value_to_i64(&from).unwrap_or(1) as usize;
                        let start_idx = if start > 0 { start - 1 } else { 0 };
                        let chars: Vec<char> = s.chars().collect();
                        let len = if let Some(f) = overlay_for {
                            let v = self.eval_row_expr(f, row, col_meta)?;
                            value_to_i64(&v).unwrap_or(r.len() as i64) as usize
                        } else {
                            r.len()
                        };
                        let mut result: String =
                            chars[..std::cmp::min(start_idx, chars.len())].iter().collect();
                        result.push_str(r);
                        let end = std::cmp::min(start_idx + len, chars.len());
                        result.extend(&chars[end..]);
                        Ok(Value::Text(result))
                    }
                    _ => Ok(Value::Null),
                }
            }
            // -- EXTRACT(field FROM expr) --
            Expr::Extract { field, expr, .. } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let field_str = field.to_string().to_lowercase();
                match val {
                    Value::Date(d) => {
                        let (y, m, day) = crate::types::days_to_ymd(d);
                        match field_str.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "dow" | "dayofweek" => {
                                let jdn = d + 2451545;
                                Ok(Value::Int32(jdn.rem_euclid(7)))
                            }
                            "doy" | "dayofyear" => {
                                let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                Ok(Value::Int32(d - jan1 + 1))
                            }
                            "epoch" => Ok(Value::Int64(d as i64 * 86400)),
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field_str}) from date"))),
                        }
                    }
                    Value::Timestamp(ts) => {
                        let total_secs = ts / 1_000_000;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = total_secs % 86400;
                        let (y, m, day) = crate::types::days_to_ymd(days);
                        match field_str.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "hour" => Ok(Value::Int32((time_secs / 3600) as i32)),
                            "minute" => Ok(Value::Int32(((time_secs % 3600) / 60) as i32)),
                            "second" => Ok(Value::Int32((time_secs % 60) as i32)),
                            "epoch" => Ok(Value::Int64(total_secs)),
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field_str}) from timestamp"))),
                        }
                    }
                    Value::Text(s) => {
                        if let Some((y, m, day, hour, minute, second)) = parse_timestamp_parts(&s) {
                            match field_str.as_str() {
                                "year" => Ok(Value::Int32(y)),
                                "month" => Ok(Value::Int32(m as i32)),
                                "day" => Ok(Value::Int32(day as i32)),
                                "hour" => Ok(Value::Int32(hour as i32)),
                                "minute" => Ok(Value::Int32(minute as i32)),
                                "second" => Ok(Value::Int32(second as i32)),
                                "dow" | "dayofweek" => {
                                    let d = crate::types::ymd_to_days(y, m, day);
                                    let jdn = d + 2451545;
                                    Ok(Value::Int32(jdn.rem_euclid(7)))
                                }
                                "doy" | "dayofyear" => {
                                    let d = crate::types::ymd_to_days(y, m, day);
                                    let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                    Ok(Value::Int32(d - jan1 + 1))
                                }
                                "epoch" => {
                                    let d = crate::types::ymd_to_days(y, m, day);
                                    let day_secs = d as i64 * 86400;
                                    let time_secs = hour as i64 * 3600 + minute as i64 * 60 + second as i64;
                                    Ok(Value::Int64(day_secs + time_secs))
                                }
                                _ => Err(ExecError::Unsupported(format!("EXTRACT({field_str}) from text"))),
                            }
                        } else {
                            Err(ExecError::Unsupported(format!("cannot parse date/time from text: {s}")))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(format!("EXTRACT from {val:?}"))),
                }
            }
            // -- IS DISTINCT FROM --
            Expr::IsDistinctFrom(left, right) => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let r = self.eval_row_expr(right, row, col_meta)?;
                // IS DISTINCT FROM treats NULL as a known value
                let distinct = match (&l, &r) {
                    (Value::Null, Value::Null) => false,
                    (Value::Null, _) | (_, Value::Null) => true,
                    _ => compare_values(&l, &r) != Some(Ordering::Equal),
                };
                Ok(Value::Bool(distinct))
            }
            Expr::IsNotDistinctFrom(left, right) => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let r = self.eval_row_expr(right, row, col_meta)?;
                let not_distinct = match (&l, &r) {
                    (Value::Null, Value::Null) => true,
                    (Value::Null, _) | (_, Value::Null) => false,
                    _ => compare_values(&l, &r) == Some(Ordering::Equal),
                };
                Ok(Value::Bool(not_distinct))
            }
            // -- ANY/ALL with subquery --
            Expr::AnyOp { left, compare_op, right, .. } => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                // Right side should evaluate to an array or subquery
                let r = self.eval_row_expr(right, row, col_meta)?;
                match r {
                    Value::Array(vals) => {
                        let found = vals.iter().any(|v| {
                            self.eval_binary_op(&l, compare_op, v).ok() == Some(Value::Bool(true))
                        });
                        Ok(Value::Bool(found))
                    }
                    _ => Err(ExecError::Unsupported("ANY requires array or subquery".into())),
                }
            }
            Expr::AllOp { left, compare_op, right, .. } => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let r = self.eval_row_expr(right, row, col_meta)?;
                match r {
                    Value::Array(vals) => {
                        let all_match = vals.iter().all(|v| {
                            self.eval_binary_op(&l, compare_op, v).ok() == Some(Value::Bool(true))
                        });
                        Ok(Value::Bool(all_match))
                    }
                    _ => Err(ExecError::Unsupported("ALL requires array or subquery".into())),
                }
            }
            // -- Array constructor --
            Expr::Array(ast::Array { elem, .. }) => {
                let mut vals = Vec::new();
                for e in elem {
                    vals.push(self.eval_row_expr(e, row, col_meta)?);
                }
                Ok(Value::Array(vals))
            }
            // -- Subquery expressions (with correlated subquery support) --
            Expr::Exists { subquery, negated } => {
                self.check_subquery_depth()?;
                let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
                let sub_result = sync_block_on(self.execute_query(resolved));
                self.query_depth.fetch_sub(1, AtomicOrdering::Relaxed);
                let sub_result = sub_result?;
                let has_rows = match &sub_result {
                    ExecResult::Select { rows, .. } => !rows.is_empty(),
                    _ => false,
                };
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                // Cache key is the canonical text of the subquery before outer-ref substitution.
                let cache_key = format!("{subquery}");
                // Check if we already have the result of this non-correlated subquery cached.
                if let Some(cached) = self.uncorrelated_subquery_cache.read().get(&cache_key).cloned() {
                    let found = cached.iter().any(|v| compare_values(&val, v) == Some(Ordering::Equal));
                    return Ok(Value::Bool(if *negated { !found } else { found }));
                }
                self.check_subquery_depth()?;
                let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
                let resolved_key = format!("{resolved}");
                let sub_result = sync_block_on(self.execute_query(resolved));
                self.query_depth.fetch_sub(1, AtomicOrdering::Relaxed);
                let sub_result = sub_result?;
                let values: std::sync::Arc<Vec<Value>> = match &sub_result {
                    ExecResult::Select { rows, .. } => {
                        std::sync::Arc::new(rows.iter().filter_map(|r| r.first().cloned()).collect())
                    }
                    _ => std::sync::Arc::new(vec![]),
                };
                // Only cache if non-correlated (resolved query text == original).
                if cache_key == resolved_key {
                    self.uncorrelated_subquery_cache.write().insert(cache_key, values.clone());
                }
                let found = values.iter().any(|v| compare_values(&val, v) == Some(Ordering::Equal));
                Ok(Value::Bool(if *negated { !found } else { found }))
            }
            Expr::Subquery(subquery) => {
                // Scalar subquery -- must return exactly one row, one column
                self.check_subquery_depth()?;
                let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
                let sub_result = sync_block_on(self.execute_query(resolved));
                self.query_depth.fetch_sub(1, AtomicOrdering::Relaxed);
                let sub_result = sub_result?;
                match sub_result {
                    ExecResult::Select { rows, .. } => {
                        if rows.is_empty() || rows[0].is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(rows[0][0].clone())
                        }
                    }
                    _ => Ok(Value::Null),
                }
            }
            _ => Err(ExecError::Unsupported(format!("expression: {expr}"))),
        }
    }

    // ========================================================================
    // Type casting
    // ========================================================================

    pub(super) fn eval_cast(&self, val: Value, target: &ast::DataType) -> Result<Value, ExecError> {
        match target {
            ast::DataType::JSONB | ast::DataType::JSON => {
                match val {
                    Value::Text(s) => {
                        let v: serde_json::Value = serde_json::from_str(&s)
                            .map_err(|e| ExecError::Unsupported(format!("invalid JSON: {e}")))?;
                        Ok(Value::Jsonb(v))
                    }
                    Value::Jsonb(_) => Ok(val),
                    _ => Err(ExecError::Unsupported(format!("cannot cast {val:?} to JSONB"))),
                }
            }
            ast::DataType::Text | ast::DataType::Varchar(_) => {
                match val {
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(val.to_string())),
                }
            }
            ast::DataType::Int(_) | ast::DataType::Integer(_) => {
                match val {
                    Value::Null => Ok(Value::Null),
                    Value::Int32(_) => Ok(val),
                    Value::Int64(n) => Ok(Value::Int32(n as i32)),
                    Value::Float64(n) => Ok(Value::Int32(n as i32)),
                    Value::Bool(b) => Ok(Value::Int32(if b { 1 } else { 0 })),
                    Value::Text(s) => s.parse::<i32>()
                        .map(Value::Int32)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to INT"))),
                    _ => Err(ExecError::Unsupported("cannot cast to INT".to_string())),
                }
            }
            ast::DataType::BigInt(_) => {
                match val {
                    Value::Null => Ok(Value::Null),
                    Value::Int32(n) => Ok(Value::Int64(n as i64)),
                    Value::Int64(_) => Ok(val),
                    Value::Float64(n) => Ok(Value::Int64(n as i64)),
                    Value::Bool(b) => Ok(Value::Int64(if b { 1 } else { 0 })),
                    Value::Text(s) => s.parse::<i64>()
                        .map(Value::Int64)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to BIGINT"))),
                    _ => Err(ExecError::Unsupported("cannot cast to BIGINT".to_string())),
                }
            }
            ast::DataType::Float(_) | ast::DataType::Double(_) | ast::DataType::DoublePrecision => {
                match val {
                    Value::Null => Ok(Value::Null),
                    Value::Int32(n) => Ok(Value::Float64(n as f64)),
                    Value::Int64(n) => Ok(Value::Float64(n as f64)),
                    Value::Float64(_) => Ok(val),
                    Value::Bool(b) => Ok(Value::Float64(if b { 1.0 } else { 0.0 })),
                    Value::Text(s) => s.parse::<f64>()
                        .map(Value::Float64)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to FLOAT"))),
                    _ => Err(ExecError::Unsupported("cannot cast to FLOAT".to_string())),
                }
            }
            ast::DataType::Boolean => {
                match val {
                    Value::Null => Ok(Value::Null),
                    Value::Bool(_) => Ok(val),
                    Value::Int32(n) => Ok(Value::Bool(n != 0)),
                    Value::Int64(n) => Ok(Value::Bool(n != 0)),
                    Value::Float64(n) => Ok(Value::Bool(n != 0.0)),
                    Value::Text(s) => match s.to_lowercase().as_str() {
                        "true" | "t" | "1" | "yes" => Ok(Value::Bool(true)),
                        "false" | "f" | "0" | "no" => Ok(Value::Bool(false)),
                        _ => Err(ExecError::Unsupported(format!("cannot cast '{s}' to BOOLEAN"))),
                    },
                    _ => Err(ExecError::Unsupported("cannot cast to BOOLEAN".to_string())),
                }
            }
            ast::DataType::Date => {
                match val {
                    Value::Date(_) => Ok(val),
                    Value::Text(s) => {
                        match parse_date_string(&s) {
                            Some(d) => Ok(Value::Date(d)),
                            None => Err(ExecError::Unsupported(format!("cannot cast '{s}' to DATE"))),
                        }
                    }
                    Value::Timestamp(ts) => {
                        Ok(Value::Date((ts / 1_000_000 / 86400) as i32))
                    }
                    Value::Int32(n) => Ok(Value::Date(n)),
                    _ => Err(ExecError::Unsupported("cannot cast to DATE".to_string())),
                }
            }
            ast::DataType::Timestamp(_, _) => {
                match val {
                    Value::Timestamp(_) | Value::TimestampTz(_) => Ok(val),
                    Value::Date(d) => Ok(Value::Timestamp(d as i64 * 86400 * 1_000_000)),
                    Value::Text(s) => {
                        match parse_date_string(&s) {
                            Some(d) => Ok(Value::Timestamp(d as i64 * 86400 * 1_000_000)),
                            None => Err(ExecError::Unsupported(format!("cannot cast '{s}' to TIMESTAMP"))),
                        }
                    }
                    Value::Int64(n) => Ok(Value::Timestamp(n * 1_000_000)),
                    Value::Int32(n) => Ok(Value::Timestamp(n as i64 * 1_000_000)),
                    _ => Err(ExecError::Unsupported("cannot cast to TIMESTAMP".to_string())),
                }
            }
            ast::DataType::Uuid => {
                match val {
                    Value::Uuid(_) => Ok(val),
                    Value::Text(s) => {
                        let bytes: Vec<u8> = s.replace('-', "")
                            .as_bytes()
                            .chunks(2)
                            .filter_map(|chunk| {
                                std::str::from_utf8(chunk).ok()
                                    .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                            })
                            .collect();
                        if bytes.len() == 16 {
                            let mut arr = [0u8; 16];
                            arr.copy_from_slice(&bytes);
                            Ok(Value::Uuid(arr))
                        } else {
                            Err(ExecError::Unsupported(format!("cannot cast '{s}' to UUID")))
                        }
                    }
                    _ => Err(ExecError::Unsupported("cannot cast to UUID".to_string())),
                }
            }
            ast::DataType::Bytea => {
                match val {
                    Value::Bytea(_) => Ok(val),
                    Value::Text(s) => Ok(Value::Bytea(s.into_bytes())),
                    _ => Err(ExecError::Unsupported("cannot cast to BYTEA".to_string())),
                }
            }
            ast::DataType::Numeric(_) | ast::DataType::Decimal(_) | ast::DataType::Dec(_) => {
                match val {
                    Value::Numeric(_) => Ok(val),
                    Value::Int32(n) => Ok(Value::Numeric(n.to_string())),
                    Value::Int64(n) => Ok(Value::Numeric(n.to_string())),
                    Value::Float64(n) => Ok(Value::Numeric(n.to_string())),
                    Value::Text(s) => Ok(Value::Numeric(s)),
                    _ => Err(ExecError::Unsupported("cannot cast to NUMERIC".to_string())),
                }
            }
            ast::DataType::Array(_) => {
                // Pass through arrays
                match val {
                    Value::Array(_) => Ok(val),
                    _ => Ok(Value::Array(vec![val])),
                }
            }
            ast::DataType::Char(_) | ast::DataType::Character(_) => {
                Ok(Value::Text(val.to_string()))
            }
            ast::DataType::Real => {
                match val {
                    Value::Float64(_) => Ok(val),
                    Value::Int32(n) => Ok(Value::Float64(n as f64)),
                    Value::Int64(n) => Ok(Value::Float64(n as f64)),
                    Value::Text(s) => s.parse::<f64>()
                        .map(Value::Float64)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to REAL"))),
                    _ => Err(ExecError::Unsupported("cannot cast to REAL".to_string())),
                }
            }
            ast::DataType::SmallInt(_) | ast::DataType::TinyInt(_) => {
                match val {
                    Value::Int32(_) => Ok(val),
                    Value::Int64(n) => Ok(Value::Int32(n as i32)),
                    Value::Float64(n) => Ok(Value::Int32(n as i32)),
                    Value::Text(s) => s.parse::<i32>()
                        .map(Value::Int32)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to SMALLINT"))),
                    _ => Err(ExecError::Unsupported("cannot cast to SMALLINT".to_string())),
                }
            }
            _ => Err(ExecError::Unsupported(format!("cast to {target}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn filter_result_empty() {
        let result = FilterResult::empty();
        assert_eq!(result.matching_positions.len(), 0);
        assert_eq!(result.total_rows, 0);
        assert_eq!(result.hit_rate(), 100.0);
    }

    #[test]
    fn filter_result_all() {
        let result = FilterResult::all(1000);
        assert_eq!(result.matching_positions.len(), 1000);
        assert_eq!(result.total_rows, 1000);
        assert_eq!(result.hit_rate(), 100.0);
    }

    #[test]
    fn filter_result_memory_savings() {
        // 1000 total rows, 300 match => 700 don't match
        // Assume 100 bytes per row => 70000 bytes saved
        let result = FilterResult {
            matching_positions: (0..300).collect(),
            total_rows: 1000,
        };
        assert_eq!(result.estimated_memory_savings(), 70000);
    }

    #[test]
    fn filter_result_hit_rate_calculations() {
        let result = FilterResult {
            matching_positions: (0..500).collect(),
            total_rows: 1000,
        };
        assert_eq!(result.hit_rate(), 50.0);

        let result2 = FilterResult {
            matching_positions: (0..100).collect(),
            total_rows: 1000,
        };
        assert_eq!(result2.hit_rate(), 10.0);

        let result3 = FilterResult {
            matching_positions: (0..1000).collect(),
            total_rows: 1000,
        };
        assert_eq!(result3.hit_rate(), 100.0);
    }

    #[test]
    fn filter_result_no_matches() {
        let result = FilterResult {
            matching_positions: Vec::new(),
            total_rows: 1000,
        };
        assert_eq!(result.hit_rate(), 0.0);
        assert_eq!(result.estimated_memory_savings(), 100000);
    }
}
