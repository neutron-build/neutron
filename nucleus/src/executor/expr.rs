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

use super::helpers::*;
use super::session::sync_block_on;
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};
use crate::types::{DataType, Row, Value, parse_numeric};

/// Seconds between the Unix epoch (1970-01-01) and Nucleus's internal temporal
/// epoch (2000-01-01). EXTRACT(EPOCH ...) reports seconds since 1970 (PG).
const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800;

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
    pub(super) fn session_time_zone(&self) -> Result<chrono_tz::Tz, ExecError> {
        let session = self.current_session();
        let value = session
            .settings
            .read()
            .get("timezone")
            .cloned()
            .unwrap_or_else(|| "UTC".to_string());
        parse_time_zone(&value)
    }

    /// Evaluate a constant expression (no table context).
    pub(super) fn eval_const_expr(&self, expr: &Expr) -> Result<Value, ExecError> {
        let _guard = ExprDepthGuard::enter()?;
        match expr {
            Expr::Value(val) => self.eval_value(&val.value),
            Expr::Interval(interval) => {
                let value = self.eval_const_expr(&interval.value)?;
                let Value::Text(raw) = value else {
                    return Err(ExecError::Runtime(
                        "INTERVAL value must be a string literal".into(),
                    ));
                };
                parse_interval_literal(&raw, interval.leading_field.as_ref())
            }
            Expr::Collate { expr, collation } => {
                validate_binary_collation(collation)?;
                self.eval_const_expr(expr)
            }
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => eval_at_time_zone(
                self.eval_const_expr(timestamp)?,
                self.eval_const_expr(time_zone)?,
            ),
            Expr::UnaryOp { op, expr } => {
                let val = self.eval_const_expr(expr)?;
                match (op, val) {
                    (ast::UnaryOperator::Minus, Value::Int32(n)) => n
                        .checked_neg()
                        .map(Value::Int32)
                        .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    (ast::UnaryOperator::Minus, Value::Int64(n)) => n
                        .checked_neg()
                        .map(Value::Int64)
                        .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    (ast::UnaryOperator::Minus, Value::Float64(n)) => Ok(Value::Float64(-n)),
                    (ast::UnaryOperator::Minus, Value::Numeric(raw)) => {
                        crate::types::numeric_neg(&raw)
                            .map(Value::Numeric)
                            .map_err(ExecError::Runtime)
                    }
                    (
                        ast::UnaryOperator::Minus,
                        Value::Interval {
                            months,
                            days,
                            microseconds,
                        },
                    ) => Ok(Value::Interval {
                        months: months.checked_neg().ok_or_else(|| {
                            ExecError::Runtime("interval value out of range".into())
                        })?,
                        days: days.checked_neg().ok_or_else(|| {
                            ExecError::Runtime("interval value out of range".into())
                        })?,
                        microseconds: microseconds.checked_neg().ok_or_else(|| {
                            ExecError::Runtime("interval value out of range".into())
                        })?,
                    }),
                    (ast::UnaryOperator::Not, Value::Bool(value)) => Ok(Value::Bool(!value)),
                    (ast::UnaryOperator::Not, Value::Null)
                    | (ast::UnaryOperator::Minus, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("unsupported unary op".into())),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_const_expr(left)?;
                let r = self.eval_const_expr(right)?;
                self.eval_binary_op(&l, op, &r)
            }
            Expr::Nested(inner) => self.eval_const_expr(inner),
            Expr::Cast {
                expr, data_type, ..
            } => {
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
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::IsDistinctFrom(_, _)
            | Expr::IsNotDistinctFrom(_, _)
            | Expr::Array(_)
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            // CASE in constant context (`SELECT CASE ... END` with no FROM) —
            // was missing from the delegation list and hit the catch-all error.
            | Expr::Case { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::CompoundFieldAccess { .. } => {
                let empty_row: Row = Vec::new();
                let empty_meta: Vec<ColMeta> = Vec::new();
                self.eval_row_expr(expr, &empty_row, &empty_meta)
            }
            // Subqueries in constant context
            Expr::Subquery(subquery) => {
                self.check_subquery_depth()?;
                let sub_result = sync_block_on(self.execute_query(*subquery.clone()));
                self.query_depth.fetch_sub(1, AtomicOrdering::Relaxed);
                match sub_result? {
                    ExecResult::Select { rows, .. } => {
                        // Scalar subquery: 0 rows → NULL, 1 row → its value,
                        // >1 row → error (PostgreSQL: "more than one row
                        // returned by a subquery used as an expression").
                        // Silently taking the first row was a wrong result.
                        if rows.len() > 1 {
                            return Err(ExecError::Runtime(
                                "more than one row returned by a subquery used as an expression"
                                    .into(),
                            ));
                        }
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
                let has_rows =
                    matches!(&sub_result, ExecResult::Select { rows, .. } if !rows.is_empty());
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
            // E'...' escape-string literal (psql sends E'\n' in \l). The
            // sqlparser tokenizer has already decoded the backslash escapes.
            ast::Value::EscapedStringLiteral(s) => Ok(Value::Text(s.clone())),
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
            Value::Text(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(v) => {
                    parsed_json = v;
                    &parsed_json
                }
                Err(_) => return Ok(Value::Null),
            },
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
    pub(super) fn eval_json_double_arrow(
        &self,
        left: &Value,
        key: &Value,
    ) -> Result<Value, ExecError> {
        let result = self.eval_json_arrow(left, key)?;
        match result {
            Value::Jsonb(serde_json::Value::String(s)) => Ok(Value::Text(s)),
            Value::Jsonb(v) => Ok(Value::Text(v.to_string())),
            Value::Null => Ok(Value::Null),
            other => Ok(Value::Text(other.to_string())),
        }
    }

    /// Evaluate JSONB path arrow operator: `jsonb_val #> '{a,b}'` (returns JSONB).
    pub(super) fn eval_json_path_arrow(
        &self,
        left: &Value,
        path: &Value,
    ) -> Result<Value, ExecError> {
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
            let keys: Vec<&str> = if inner.is_empty() {
                vec![]
            } else {
                inner.split(',').collect()
            };
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
    pub(super) fn eval_json_path_long_arrow(
        &self,
        left: &Value,
        path: &Value,
    ) -> Result<Value, ExecError> {
        let result = self.eval_json_path_arrow(left, path)?;
        match result {
            Value::Jsonb(serde_json::Value::String(s)) => Ok(Value::Text(s)),
            Value::Jsonb(v) => Ok(Value::Text(v.to_string())),
            Value::Null => Ok(Value::Null),
            other => Ok(Value::Text(other.to_string())),
        }
    }

    /// Evaluate JSONB containment operator: `left @> right`.
    ///
    /// Returns true when `left` contains all key-value pairs in `right`.
    /// Both sides are parsed as JSON; containment is checked recursively:
    /// - Object A contains Object B when every key in B exists in A and
    ///   A[key] contains B[key].
    /// - Array A contains Array B when every element in B has a matching
    ///   element in A (order-independent).
    /// - Scalars are compared for equality.
    pub(super) fn eval_json_contains(
        &self,
        left: &Value,
        right: &Value,
    ) -> Result<Value, ExecError> {
        let left_json = match left {
            Value::Jsonb(v) => v.clone(),
            Value::Text(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::Null),
            Value::Null => return Ok(Value::Null),
            _ => return Ok(Value::Bool(false)),
        };
        let right_json = match right {
            Value::Jsonb(v) => v.clone(),
            Value::Text(s) => match serde_json::from_str(s) {
                Ok(v) => v,
                Err(_) => return Ok(Value::Bool(false)),
            },
            Value::Null => return Ok(Value::Null),
            _ => return Ok(Value::Bool(false)),
        };
        Ok(Value::Bool(json_contains(&left_json, &right_json)))
    }

    /// SQL 3-valued `x [NOT] IN (candidates)`, shared by `IN (list)` and
    /// `IN (subquery)`:
    /// - FALSE (TRUE for NOT IN) when the candidate set is empty — even for a
    ///   NULL `val`: `x IN ()` is unconditionally FALSE in SQL because there is
    ///   nothing to be equal to (this is the empty-subquery case);
    /// - NULL if `val` is NULL, or `val` matches nothing but a candidate is NULL;
    /// - otherwise TRUE on a definite match / FALSE on a match-free non-NULL set,
    ///   with `negated` flipping the definite TRUE/FALSE (NULL stays NULL).
    ///
    /// This is what makes `WHERE x NOT IN (..)` correctly exclude NULL-involved
    /// rows instead of including them.
    /// Values for the right operand of `x op ANY/ALL (...)`: a subquery's whole
    /// first column (correlated refs substituted), an array value, or a
    /// Postgres array-literal text.
    /// `None` means the array operand itself was NULL — the whole ANY/ALL
    /// comparison is then NULL (PostgreSQL: `x = ANY(NULL::text[])` is NULL).
    fn any_all_operand(
        &self,
        right: &Expr,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Option<Vec<Value>>, ExecError> {
        let inner = match right {
            Expr::Nested(e) => e.as_ref(),
            other => other,
        };
        if let Expr::Subquery(subquery) = inner {
            self.check_subquery_depth()?;
            let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
            let sub_result = sync_block_on(self.execute_query(resolved));
            self.query_depth.fetch_sub(1, AtomicOrdering::Relaxed);
            return match sub_result? {
                ExecResult::Select { rows, .. } => Ok(Some(
                    rows.into_iter().filter_map(|r| r.into_iter().next()).collect(),
                )),
                _ => Ok(Some(Vec::new())),
            };
        }
        let r = self.eval_row_expr(right, row, col_meta)?;
        if matches!(r, Value::Null) {
            return Ok(None);
        }
        coerce_to_array(r)
            .map(Some)
            .ok_or_else(|| ExecError::Unsupported("ANY/ALL requires an array or subquery".into()))
    }

    pub(super) fn in_three_valued(val: &Value, candidates: &[Value], negated: bool) -> Value {
        // Empty set is decisive regardless of `val` (including NULL): IN → FALSE,
        // NOT IN → TRUE. Must precede the NULL-val check below.
        if candidates.is_empty() {
            return Value::Bool(negated);
        }
        if matches!(val, Value::Null) {
            return Value::Null;
        }
        let mut has_null = false;
        for v in candidates {
            if matches!(v, Value::Null) {
                has_null = true;
            } else if compare_values(val, v) == Some(Ordering::Equal) {
                return Value::Bool(!negated);
            }
        }
        if has_null {
            Value::Null
        } else {
            Value::Bool(negated)
        }
    }

    pub(super) fn eval_binary_op(
        &self,
        left: &Value,
        op: &ast::BinaryOperator,
        right: &Value,
    ) -> Result<Value, ExecError> {
        // SQL 3-valued logic: comparisons AND arithmetic/concat with a NULL
        // operand yield NULL (not an error). AND/OR are excluded here — they
        // have their own 3-valued rules below (e.g. FALSE AND NULL = FALSE).
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            match op {
                ast::BinaryOperator::Eq
                | ast::BinaryOperator::NotEq
                | ast::BinaryOperator::Lt
                | ast::BinaryOperator::Gt
                | ast::BinaryOperator::LtEq
                | ast::BinaryOperator::GtEq
                | ast::BinaryOperator::Plus
                | ast::BinaryOperator::Minus
                | ast::BinaryOperator::Multiply
                | ast::BinaryOperator::Divide
                | ast::BinaryOperator::Modulo
                | ast::BinaryOperator::StringConcat => return Ok(Value::Null),
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
            // POSIX regex operators (~, !~, ~*, !~*) — psql meta-commands
            // filter catalogs with these (`nspname !~ '^pg_'`), and psql also
            // spells them as OPERATOR(pg_catalog.~), handled below.
            ast::BinaryOperator::PGRegexMatch => {
                return eval_regex_match(left, right, false, false);
            }
            ast::BinaryOperator::PGRegexNotMatch => {
                return eval_regex_match(left, right, true, false);
            }
            ast::BinaryOperator::PGRegexIMatch => {
                return eval_regex_match(left, right, false, true);
            }
            ast::BinaryOperator::PGRegexNotIMatch => {
                return eval_regex_match(left, right, true, true);
            }
            ast::BinaryOperator::PGCustomBinaryOperator(parts) => {
                // OPERATOR(pg_catalog.~) etc. — resolve the schema-qualified
                // spelling to the same regex semantics.
                if let Some(op_name) = parts.last() {
                    match op_name.as_str() {
                        "~" => return eval_regex_match(left, right, false, false),
                        "!~" => return eval_regex_match(left, right, true, false),
                        "~*" => return eval_regex_match(left, right, false, true),
                        "!~*" => return eval_regex_match(left, right, true, true),
                        _ => {}
                    }
                }
                return Err(ExecError::Unsupported(format!(
                    "custom operator OPERATOR({})",
                    parts.join(".")
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
            // JSONB containment operator: left @> right
            ast::BinaryOperator::AtArrow => {
                return self.eval_json_contains(left, right);
            }
            // JSONB contained-by operator: left <@ right (reverse of @>)
            ast::BinaryOperator::ArrowAt => {
                return self.eval_json_contains(right, left);
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
        if matches!(op, ast::BinaryOperator::Plus | ast::BinaryOperator::Minus)
            && let Some(result) = eval_temporal_arithmetic(left, op, right)
        {
            return result;
        }
        if matches!(
            op,
            ast::BinaryOperator::Plus
                | ast::BinaryOperator::Minus
                | ast::BinaryOperator::Multiply
                | ast::BinaryOperator::Divide
                | ast::BinaryOperator::Modulo
        ) && let Some(result) = eval_numeric_arithmetic(left, op, right)
        {
            return result;
        }
        match (left, right) {
            (Value::Int32(l), Value::Int32(r)) => match op {
                ast::BinaryOperator::Plus => l
                    .checked_add(*r)
                    .map(Value::Int32)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Minus => l
                    .checked_sub(*r)
                    .map(Value::Int32)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Multiply => l
                    .checked_mul(*r)
                    .map(Value::Int32)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Divide if *r == 0 => {
                    Err(ExecError::Runtime("division by zero".into()))
                }
                ast::BinaryOperator::Divide => l
                    .checked_div(*r)
                    .map(Value::Int32)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Modulo if *r == 0 => {
                    Err(ExecError::Runtime("division by zero".into()))
                }
                ast::BinaryOperator::Modulo => Ok(Value::Int32(l % r)),
                _ => Err(ExecError::Unsupported(format!("op: {op}"))),
            },
            (Value::Int64(l), Value::Int64(r)) => match op {
                ast::BinaryOperator::Plus => l
                    .checked_add(*r)
                    .map(Value::Int64)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Minus => l
                    .checked_sub(*r)
                    .map(Value::Int64)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Multiply => l
                    .checked_mul(*r)
                    .map(Value::Int64)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Divide if *r == 0 => {
                    Err(ExecError::Runtime("division by zero".into()))
                }
                ast::BinaryOperator::Divide => l
                    .checked_div(*r)
                    .map(Value::Int64)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                ast::BinaryOperator::Modulo if *r == 0 => {
                    Err(ExecError::Runtime("division by zero".into()))
                }
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
                ast::BinaryOperator::Divide if *r == 0.0 => {
                    Err(ExecError::Runtime("division by zero".into()))
                }
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
            // `||` with one text operand coerces the other to text (PostgreSQL:
            // `'a' || 1` = 'a1'). NULL was already handled above (yields NULL).
            (Value::Text(l), r) if matches!(op, ast::BinaryOperator::StringConcat) => {
                Ok(Value::Text(format!("{l}{r}")))
            }
            (l, Value::Text(r)) if matches!(op, ast::BinaryOperator::StringConcat) => {
                Ok(Value::Text(format!("{l}{r}")))
            }
            _ => Err(ExecError::Unsupported(format!(
                "type mismatch for {op}: {left:?} vs {right:?}"
            ))),
        }
    }

    /// Evaluate a WHERE clause expression against a row.
    pub(super) fn eval_where(
        &self,
        expr: &Expr,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<bool, ExecError> {
        match self.eval_row_expr(expr, row, col_meta)? {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            other => Err(ExecError::Unsupported(format!(
                "WHERE expects boolean, got {other}"
            ))),
        }
    }

    /// Parallel WHERE filter for large result sets using Rayon.
    /// Falls back to serial for small sets (below `PARALLEL_THRESHOLD`).
    /// WHERE filter that PROPAGATES evaluation errors. An unresolvable column
    /// or failing expression must surface as an error — treating it as
    /// row-doesn't-match silently returned wrong (empty) results for typos
    /// like `WHERE zzz > 3`.
    pub(super) fn try_parallel_filter(
        &self,
        rows: Vec<Row>,
        where_expr: &Expr,
        col_meta: &[ColMeta],
    ) -> Result<Vec<Row>, ExecError> {
        /// Minimum row count before switching to parallel evaluation.
        const PARALLEL_THRESHOLD: usize = 10_000;

        if cfg!(feature = "server") && rows.len() >= PARALLEL_THRESHOLD {
            // Parallel path using Rayon (server builds only)
            #[cfg(feature = "server")]
            {
                // Rayon workers can't see the session task-local, so capture
                // the cancel flag here and poll it per row.
                let session = self.current_session_for_cancel();
                let run = || {
                    rows.into_par_iter()
                        .map(|row| {
                            if session
                                .cancel_requested
                                .swap(false, std::sync::atomic::Ordering::Relaxed)
                            {
                                return Err(ExecError::Runtime(Self::CANCEL_MESSAGE.into()));
                            }
                            self.eval_where(where_expr, &row, col_meta)
                                .map(|keep| (keep, row))
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map(|pairs| {
                            pairs
                                .into_iter()
                                .filter_map(|(keep, row)| keep.then_some(row))
                                .collect()
                        })
                };
                // A long rayon collect blocks this tokio worker; without
                // block_in_place the runtime can stall its IO driver behind
                // the compute, delaying even NEW connections (observed: a
                // CancelRequest not read until the filter finished).
                match tokio::runtime::Handle::try_current() {
                    Ok(handle)
                        if handle.runtime_flavor()
                            == tokio::runtime::RuntimeFlavor::MultiThread =>
                    {
                        tokio::task::block_in_place(run)
                    }
                    _ => run(),
                }
            }
            #[cfg(not(feature = "server"))]
            {
                unreachable!()
            }
        } else {
            // Serial path for small result sets or non-server (WASM) builds
            let mut out = Vec::new();
            for row in rows {
                if self.eval_where(where_expr, &row, col_meta)? {
                    out.push(row);
                }
            }
            Ok(out)
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
                let idx = self.resolve_column(col_meta, Some(&parts[0].value), &parts[1].value)?;
                Ok(row[idx].clone())
            }
            // schema.table.column — Prisma qualifies RETURNING columns as
            // "public"."users"."id". The schema part carries no information
            // (single-schema engine); resolve on table.column.
            Expr::CompoundIdentifier(parts) if parts.len() == 3 => {
                let idx = self.resolve_column(col_meta, Some(&parts[1].value), &parts[2].value)?;
                Ok(row[idx].clone())
            }
            Expr::Value(val) => self.eval_value(&val.value),
            Expr::Interval(interval) => {
                let value = self.eval_row_expr(&interval.value, row, col_meta)?;
                let Value::Text(raw) = value else {
                    return Err(ExecError::Runtime(
                        "INTERVAL value must be a string literal".into(),
                    ));
                };
                parse_interval_literal(&raw, interval.leading_field.as_ref())
            }
            Expr::Collate { expr, collation } => {
                validate_binary_collation(collation)?;
                self.eval_row_expr(expr, row, col_meta)
            }
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => eval_at_time_zone(
                self.eval_row_expr(timestamp, row, col_meta)?,
                self.eval_row_expr(time_zone, row, col_meta)?,
            ),
            // Typed string literals: TIMESTAMP '2024-01-01', DATE '2024-01-01', UUID 'xxx'
            Expr::TypedString(ts) => {
                let s = match &ts.value.value {
                    ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                        s.clone()
                    }
                    other => other.to_string(),
                };
                match &ts.data_type {
                    ast::DataType::Timestamp(_, tz) => {
                        let timestamp =
                            crate::types::parse_timestamp(&s).map_err(ExecError::Runtime)?;
                        if matches!(tz, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz) {
                            local_timestamp_at_time_zone(timestamp, self.session_time_zone()?)
                                .map(Value::TimestampTz)
                        } else {
                            Ok(Value::Timestamp(timestamp))
                        }
                    }
                    ast::DataType::TimestampNtz(_) => crate::types::parse_timestamp(&s)
                        .map(Value::Timestamp)
                        .map_err(ExecError::Runtime),
                    ast::DataType::Date => crate::types::parse_date(&s)
                        .map(Value::Date)
                        .map_err(ExecError::Runtime),
                    ast::DataType::Uuid => match crate::types::parse_uuid(&s) {
                        Ok(bytes) => Ok(Value::Uuid(bytes)),
                        Err(error) => Err(ExecError::Runtime(error)),
                    },
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
                    (ast::UnaryOperator::Minus, Value::Int32(n)) => n
                        .checked_neg()
                        .map(Value::Int32)
                        .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    (ast::UnaryOperator::Minus, Value::Int64(n)) => n
                        .checked_neg()
                        .map(Value::Int64)
                        .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    (ast::UnaryOperator::Minus, Value::Float64(n)) => Ok(Value::Float64(-n)),
                    (ast::UnaryOperator::Minus, Value::Numeric(raw)) => Ok(Value::Numeric(
                        parse_numeric(&raw)
                            .map(|d| (-d).to_string())
                            .unwrap_or_else(|_| {
                                if let Some(stripped) = raw.strip_prefix('-') {
                                    stripped.to_string()
                                } else {
                                    format!("-{raw}")
                                }
                            }),
                    )),
                    (
                        ast::UnaryOperator::Minus,
                        Value::Interval {
                            months,
                            days,
                            microseconds,
                        },
                    ) => Ok(Value::Interval {
                        months: months.checked_neg().ok_or_else(|| {
                            ExecError::Runtime("interval value out of range".into())
                        })?,
                        days: days.checked_neg().ok_or_else(|| {
                            ExecError::Runtime("interval value out of range".into())
                        })?,
                        microseconds: microseconds.checked_neg().ok_or_else(|| {
                            ExecError::Runtime("interval value out of range".into())
                        })?,
                    }),
                    (ast::UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
                    // SQL 3-valued logic: NOT NULL = NULL (unknown), not an error.
                    (ast::UnaryOperator::Not, Value::Null)
                    | (ast::UnaryOperator::Minus, Value::Null) => Ok(Value::Null),
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
            Expr::IsTrue(inner) => {
                let value = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(matches!(value, Value::Bool(true))))
            }
            Expr::IsNotTrue(inner) => {
                let value = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(!matches!(value, Value::Bool(true))))
            }
            Expr::IsFalse(inner) => {
                let value = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(matches!(value, Value::Bool(false))))
            }
            Expr::IsNotFalse(inner) => {
                let value = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(!matches!(value, Value::Bool(false))))
            }
            Expr::IsUnknown(inner) => {
                let value = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(matches!(value, Value::Null)))
            }
            Expr::IsNotUnknown(inner) => {
                let value = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(!matches!(value, Value::Null)))
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
                if matches!(val, Value::Null)
                    || matches!(lo, Value::Null)
                    || matches!(hi, Value::Null)
                {
                    return Ok(Value::Null);
                }
                let in_range = matches!(
                    compare_values(&val, &lo),
                    Some(Ordering::Greater | Ordering::Equal)
                ) && matches!(
                    compare_values(&val, &hi),
                    Some(Ordering::Less | Ordering::Equal)
                );
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                self.eval_cast(val, data_type)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let mut items = Vec::with_capacity(list.len());
                for item in list {
                    items.push(self.eval_row_expr(item, row, col_meta)?);
                }
                Ok(Self::in_three_valued(&val, &items, *negated))
            }
            Expr::Function(func) => {
                let fname = func.name.to_string().to_uppercase();
                // Don't handle aggregates here -- they're handled in eval_aggregate_expr
                if matches!(fname.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    return Err(ExecError::Unsupported(format!(
                        "aggregate function {fname} outside of aggregate context"
                    )));
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
                                Some(ast::TrimWhereField::Leading) => s.trim_start().to_string(),
                                Some(ast::TrimWhereField::Trailing) => s.trim_end().to_string(),
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
                        let mut result: String = chars[..std::cmp::min(start_idx, chars.len())]
                            .iter()
                            .collect();
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
                            // PG DOW: Sunday=0..Saturday=6. 2000-01-01 (d=0) is
                            // a Saturday, so DOW = (d + 6) mod 7 (the old
                            // +2451545 offset was off by one).
                            "dow" | "dayofweek" => Ok(Value::Int32((d + 6).rem_euclid(7))),
                            "doy" | "dayofyear" => {
                                let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                Ok(Value::Int32(d - jan1 + 1))
                            }
                            // PG epoch is seconds since 1970-01-01; Nucleus's
                            // internal epoch is 2000-01-01 (offset 946684800s).
                            "epoch" => Ok(Value::Int64(d as i64 * 86400 + PG_EPOCH_OFFSET_SECS)),
                            _ => Err(ExecError::Unsupported(format!(
                                "EXTRACT({field_str}) from date"
                            ))),
                        }
                    }
                    Value::Timestamp(ts) | Value::TimestampTz(ts) => {
                        let total_secs = ts.div_euclid(1_000_000);
                        let days = total_secs.div_euclid(86400) as i32;
                        let time_secs = total_secs.rem_euclid(86400);
                        let (y, m, day) = crate::types::days_to_ymd(days);
                        match field_str.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "hour" => Ok(Value::Int32((time_secs / 3600) as i32)),
                            "minute" => Ok(Value::Int32(((time_secs % 3600) / 60) as i32)),
                            "second" => Ok(Value::Int32((time_secs % 60) as i32)),
                            "dow" | "dayofweek" => Ok(Value::Int32((days + 6).rem_euclid(7))),
                            "doy" | "dayofyear" => {
                                let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                Ok(Value::Int32(days - jan1 + 1))
                            }
                            "epoch" => Ok(Value::Int64(total_secs + PG_EPOCH_OFFSET_SECS)),
                            _ => Err(ExecError::Unsupported(format!(
                                "EXTRACT({field_str}) from timestamp"
                            ))),
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
                                    Ok(Value::Int32((d + 6).rem_euclid(7)))
                                }
                                "doy" | "dayofyear" => {
                                    let d = crate::types::ymd_to_days(y, m, day);
                                    let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                    Ok(Value::Int32(d - jan1 + 1))
                                }
                                "epoch" => {
                                    let d = crate::types::ymd_to_days(y, m, day);
                                    let day_secs = d as i64 * 86400;
                                    let time_secs =
                                        hour as i64 * 3600 + minute as i64 * 60 + second as i64;
                                    Ok(Value::Int64(day_secs + time_secs + PG_EPOCH_OFFSET_SECS))
                                }
                                _ => Err(ExecError::Unsupported(format!(
                                    "EXTRACT({field_str}) from text"
                                ))),
                            }
                        } else {
                            Err(ExecError::Unsupported(format!(
                                "cannot parse date/time from text: {s}"
                            )))
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
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                // Right side: a subquery's whole first column, an array value,
                // or a Postgres array-literal text ('{a,b}', how array params
                // arrive). A subquery evaluated via eval_row_expr would collapse
                // to its first cell, so gather the column explicitly.
                let Some(vals) = self.any_all_operand(right, row, col_meta)? else {
                    return Ok(Value::Null);
                };
                // `op ANY (empty)` = FALSE; NULL element makes an otherwise-FALSE
                // result NULL (three-valued), matching PostgreSQL.
                let mut any_true = false;
                let mut any_null = matches!(l, Value::Null);
                for v in &vals {
                    match self.eval_binary_op(&l, compare_op, v)? {
                        Value::Bool(true) => {
                            any_true = true;
                            break;
                        }
                        Value::Null => any_null = true,
                        _ => {}
                    }
                }
                Ok(if any_true {
                    Value::Bool(true)
                } else if any_null {
                    Value::Null
                } else {
                    Value::Bool(false)
                })
            }
            Expr::AllOp {
                left,
                compare_op,
                right,
                ..
            } => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let Some(vals) = self.any_all_operand(right, row, col_meta)? else {
                    return Ok(Value::Null);
                };
                // `op ALL (empty)` = TRUE; any FALSE makes it FALSE; otherwise a
                // NULL element makes it NULL.
                let mut all_true = true;
                let mut any_null = matches!(l, Value::Null) && !vals.is_empty();
                for v in &vals {
                    match self.eval_binary_op(&l, compare_op, v)? {
                        Value::Bool(false) => {
                            all_true = false;
                            break;
                        }
                        Value::Null => any_null = true,
                        _ => {}
                    }
                }
                Ok(if !all_true {
                    Value::Bool(false)
                } else if any_null {
                    Value::Null
                } else {
                    Value::Bool(true)
                })
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
                if let Some(cached) = self
                    .uncorrelated_subquery_cache
                    .read()
                    .get(&cache_key)
                    .cloned()
                {
                    return Ok(Self::in_three_valued(&val, &cached, *negated));
                }
                self.check_subquery_depth()?;
                let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
                let resolved_key = format!("{resolved}");
                let sub_result = sync_block_on(self.execute_query(resolved));
                self.query_depth.fetch_sub(1, AtomicOrdering::Relaxed);
                let sub_result = sub_result?;
                let values: std::sync::Arc<Vec<Value>> = match &sub_result {
                    ExecResult::Select { rows, .. } => std::sync::Arc::new(
                        rows.iter().filter_map(|r| r.first().cloned()).collect(),
                    ),
                    _ => std::sync::Arc::new(vec![]),
                };
                // Only cache if non-correlated (resolved query text == original).
                if cache_key == resolved_key {
                    self.uncorrelated_subquery_cache
                        .write()
                        .insert(cache_key, values.clone());
                }
                Ok(Self::in_three_valued(&val, &values, *negated))
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
            // -- CompoundFieldAccess: column['key'] as sugar for column -> 'key' --
            Expr::CompoundFieldAccess { root, access_chain } => {
                let base = self.eval_row_expr(root, row, col_meta)?;
                if let Some(first) = access_chain.first() {
                    use sqlparser::ast::AccessExpr;
                    match first {
                        AccessExpr::Subscript(sub) => {
                            let key_expr = match sub {
                                sqlparser::ast::Subscript::Index { index } => index,
                                sqlparser::ast::Subscript::Slice { .. } => {
                                    return Err(ExecError::Unsupported(
                                        "array slice subscript".into(),
                                    ));
                                }
                            };
                            let key = self.eval_row_expr(key_expr, row, col_meta)?;
                            self.eval_json_arrow(&base, &key)
                        }
                        // Composite field access `(expr).field` — PostgreSQL
                        // record syntax. Nucleus represents the only composite
                        // producer (_pg_expandarray) as a JSON object, so
                        // extract the field with a typed result (JDBC compares
                        // `(keys).x` against integer attnum).
                        AccessExpr::Dot(Expr::Identifier(field)) => match &base {
                            Value::Jsonb(serde_json::Value::Object(map)) => {
                                Ok(match map.get(&field.value) {
                                    None | Some(serde_json::Value::Null) => Value::Null,
                                    Some(serde_json::Value::Number(n)) => {
                                        if let Some(i) = n.as_i64() {
                                            Value::Int64(i)
                                        } else {
                                            Value::Float64(n.as_f64().unwrap_or(f64::NAN))
                                        }
                                    }
                                    Some(serde_json::Value::String(s)) => Value::Text(s.clone()),
                                    Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
                                    Some(other) => Value::Jsonb(other.clone()),
                                })
                            }
                            Value::Null => Ok(Value::Null),
                            _ => Err(ExecError::Unsupported(format!(
                                "field access on non-composite value: {expr}"
                            ))),
                        },
                        _ => Err(ExecError::Unsupported(format!("expression: {expr}"))),
                    }
                } else {
                    Ok(base)
                }
            }
            _ => Err(ExecError::Unsupported(format!("expression: {expr}"))),
        }
    }

    // ========================================================================
    // Type casting
    // ========================================================================

    /// Reverse regclass resolution: OID -> relation name. Synthetic user-table
    /// OIDs (16384 + catalog position) resolve through the sync catalog
    /// snapshot; fixed system-catalog OIDs resolve through the static map.
    fn regclass_name(&self, oid: i32) -> Option<String> {
        if oid >= 16384 {
            let tables = self.catalog.list_tables_sync()?;
            return tables.get((oid - 16384) as usize).map(|t| t.name.clone());
        }
        match oid {
            1247 => Some("pg_type".into()),
            1249 => Some("pg_attribute".into()),
            1255 => Some("pg_proc".into()),
            1259 => Some("pg_class".into()),
            1260 => Some("pg_authid".into()),
            1262 => Some("pg_database".into()),
            2609 => Some("pg_description".into()),
            2610 => Some("pg_index".into()),
            2615 => Some("pg_namespace".into()),
            3079 => Some("pg_extension".into()),
            _ => None,
        }
    }

    pub(super) fn eval_cast(&self, val: Value, target: &ast::DataType) -> Result<Value, ExecError> {
        match target {
            // '<catalog name>'::regclass — psql meta-commands (\dx notably) use
            // this to reference system catalogs by name. Resolve the fixed
            // pg_catalog OIDs; unknown names (incl. user tables, which would
            // need async catalog access) yield NULL rather than an error so a
            // LEFT JOIN comparison degrades to no-match, matching what the
            // meta-command needs.
            ast::DataType::Regclass => Ok(match &val {
                Value::Text(s) => regclass_oid(s).map(Value::Int32).unwrap_or_else(|| {
                    // User table: resolve the synthetic OID (16384 + catalog
                    // position — the same assignment the virtual pg_catalog
                    // arms use). Quotes are stripped wholesale: real Nucleus
                    // names never contain '"', but introspection SQL passes
                    // spellings like '"public"."post_tags"'.
                    let bare = s.replace('"', "");
                    let bare = bare.strip_prefix("public.").unwrap_or(&bare);
                    self.catalog
                        .list_tables_sync()
                        .and_then(|ts| ts.iter().position(|t| t.name == bare))
                        .map(|i| Value::Int32(16384 + i as i32))
                        .unwrap_or(Value::Null)
                }),
                // OID -> regclass renders as the relation NAME (Postgres
                // displays regclass as text). A later ::text cast is then the
                // identity, which is exactly what introspection queries like
                // `attrelid::regclass::text` need. Unknown OIDs stay numeric.
                Value::Int32(n) => self
                    .regclass_name(*n)
                    .map(Value::Text)
                    .unwrap_or(val.clone()),
                Value::Int64(n) => self
                    .regclass_name(*n as i32)
                    .map(Value::Text)
                    .unwrap_or(Value::Int32(*n as i32)),
                _ => Value::Null,
            }),
            // ::regproc — function-name pseudo-type. Nucleus renders regproc
            // values as their text name already, so the cast is the identity
            // on text (prisma casts pg_type.typinput::regproc::text).
            ast::DataType::Custom(name, _)
                if name.to_string().eq_ignore_ascii_case("regproc") =>
            {
                Ok(match &val {
                    Value::Text(_) | Value::Int32(_) | Value::Int64(_) => val,
                    _ => Value::Null,
                })
            }
            // '<type name>'::regtype — sqlparser has no first-class REGTYPE, so
            // it arrives as a custom type. Resolves to the type OID.
            ast::DataType::Custom(name, _)
                if name.to_string().eq_ignore_ascii_case("regtype") =>
            {
                Ok(match &val {
                    Value::Text(s) => regtype_oid(s).map(Value::Int32).unwrap_or(Value::Null),
                    Value::Int32(_) => val,
                    Value::Int64(n) => Value::Int32(*n as i32),
                    _ => Value::Null,
                })
            }
            ast::DataType::JSONB | ast::DataType::JSON => match val {
                Value::Text(s) => {
                    let v: serde_json::Value = serde_json::from_str(&s)
                        .map_err(|e| ExecError::Unsupported(format!("invalid JSON: {e}")))?;
                    Ok(Value::Jsonb(v))
                }
                Value::Jsonb(_) => Ok(val),
                _ => Err(ExecError::Unsupported(format!(
                    "cannot cast {val:?} to JSONB"
                ))),
            },
            ast::DataType::Text | ast::DataType::Varchar(_) => match val {
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Text(val.to_string())),
            },
            ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::Int4(_) => match val {
                Value::Null => Ok(Value::Null),
                Value::Int32(_) => Ok(val),
                Value::Int64(n) => i32::try_from(n)
                    .map(Value::Int32)
                    .map_err(|_| ExecError::Runtime("integer out of range".into())),
                // PostgreSQL rounds float8→int half-to-even (42.5→42, 43.5→44,
                // 42.7→43); NUMERIC→int rounds half-AWAY-from-zero (42.5→43),
                // matching PG's distinct numeric/float rounding rules.
                Value::Float64(n) => f64_to_i32(n).map(Value::Int32),
                Value::Numeric(s) => parse_numeric(&s)
                    .ok()
                    .and_then(|d| {
                        d.round_dp_with_strategy(
                            0,
                            rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                        )
                        .to_string()
                        .parse::<i32>()
                        .ok()
                    })
                    .map(Value::Int32)
                    .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                Value::Bool(b) => Ok(Value::Int32(if b { 1 } else { 0 })),
                Value::Text(s) => s
                    .trim()
                    .parse::<i32>()
                    .map(Value::Int32)
                    .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to INT"))),
                _ => Err(ExecError::Unsupported("cannot cast to INT".to_string())),
            },
            ast::DataType::BigInt(_) | ast::DataType::Int8(_) => match val {
                Value::Null => Ok(Value::Null),
                Value::Int32(n) => Ok(Value::Int64(n as i64)),
                Value::Int64(_) => Ok(val),
                Value::Float64(n) => f64_to_i64(n).map(Value::Int64),
                Value::Numeric(s) => parse_numeric(&s)
                    .ok()
                    .and_then(|d| {
                        d.round_dp_with_strategy(
                            0,
                            rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                        )
                        .to_string()
                        .parse::<i64>()
                        .ok()
                    })
                    .map(Value::Int64)
                    .ok_or_else(|| ExecError::Runtime("bigint out of range".into())),
                Value::Bool(b) => Ok(Value::Int64(if b { 1 } else { 0 })),
                Value::Text(s) => s
                    .parse::<i64>()
                    .map(Value::Int64)
                    .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to BIGINT"))),
                _ => Err(ExecError::Unsupported("cannot cast to BIGINT".to_string())),
            },
            // float8/float4 are the PostgreSQL spellings of double/real; without
            // these arms `x::float8` fell to the catch-all cast error, so every
            // float-cast expression (incl. Infinity/NaN literals) errored.
            ast::DataType::Float(_)
            | ast::DataType::Double(_)
            | ast::DataType::DoublePrecision
            | ast::DataType::Float4
            | ast::DataType::Float8 => match val {
                Value::Null => Ok(Value::Null),
                Value::Int32(n) => Ok(Value::Float64(n as f64)),
                Value::Int64(n) => Ok(Value::Float64(n as f64)),
                Value::Float64(_) => Ok(val),
                Value::Numeric(s) => parse_numeric(&s)
                    .ok()
                    .and_then(|d| d.to_string().parse::<f64>().ok())
                    .map(Value::Float64)
                    .ok_or_else(|| ExecError::Runtime("invalid numeric".into())),
                Value::Bool(b) => Ok(Value::Float64(if b { 1.0 } else { 0.0 })),
                // f64::parse handles 'Infinity'/'-Infinity'/'NaN' (PG-compatible).
                Value::Text(s) => s
                    .trim()
                    .parse::<f64>()
                    .map(Value::Float64)
                    .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to FLOAT"))),
                _ => Err(ExecError::Unsupported("cannot cast to FLOAT".to_string())),
            },
            ast::DataType::Boolean => match val {
                Value::Null => Ok(Value::Null),
                Value::Bool(_) => Ok(val),
                Value::Int32(n) => Ok(Value::Bool(n != 0)),
                Value::Int64(n) => Ok(Value::Bool(n != 0)),
                Value::Float64(n) => Ok(Value::Bool(n != 0.0)),
                Value::Text(s) => match s.to_lowercase().as_str() {
                    "true" | "t" | "1" | "yes" => Ok(Value::Bool(true)),
                    "false" | "f" | "0" | "no" => Ok(Value::Bool(false)),
                    _ => Err(ExecError::Unsupported(format!(
                        "cannot cast '{s}' to BOOLEAN"
                    ))),
                },
                _ => Err(ExecError::Unsupported("cannot cast to BOOLEAN".to_string())),
            },
            ast::DataType::Date => val.cast(&DataType::Date).map_err(ExecError::Runtime),
            ast::DataType::Timestamp(_, timezone) => {
                let with_timezone = matches!(
                    timezone,
                    ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz
                );
                match (val, with_timezone) {
                    (Value::Null, _) => Ok(Value::Null),
                    (Value::TimestampTz(value), true) => Ok(Value::TimestampTz(value)),
                    (Value::Timestamp(value), false) => Ok(Value::Timestamp(value)),
                    (Value::Timestamp(value), true) => {
                        local_timestamp_at_time_zone(value, self.session_time_zone()?)
                            .map(Value::TimestampTz)
                    }
                    (Value::TimestampTz(value), false) => {
                        timestamptz_at_time_zone(value, self.session_time_zone()?)
                            .map(Value::Timestamp)
                    }
                    (Value::Date(value), true) => local_timestamp_at_time_zone(
                        value as i64 * 86_400_000_000,
                        self.session_time_zone()?,
                    )
                    .map(Value::TimestampTz),
                    (Value::Text(text), true) => {
                        let local =
                            crate::types::parse_timestamp(&text).map_err(ExecError::Runtime)?;
                        local_timestamp_at_time_zone(local, self.session_time_zone()?)
                            .map(Value::TimestampTz)
                    }
                    (value, false) => value.cast(&DataType::Timestamp).map_err(ExecError::Runtime),
                    (value, true) => value
                        .cast(&DataType::TimestampTz)
                        .map_err(ExecError::Runtime),
                }
            }
            ast::DataType::Uuid => match val {
                Value::Uuid(_) => Ok(val),
                Value::Text(s) => {
                    let bytes: Vec<u8> = s
                        .replace('-', "")
                        .as_bytes()
                        .chunks(2)
                        .filter_map(|chunk| {
                            std::str::from_utf8(chunk)
                                .ok()
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
            },
            ast::DataType::Bytea => match val {
                Value::Bytea(_) => Ok(val),
                // Delegate to Value::cast so the '\x' hex text form decodes
                // identically here and in the storage coercion path.
                Value::Text(_) => val
                    .cast(&crate::types::DataType::Bytea)
                    .map_err(ExecError::Runtime),
                _ => Err(ExecError::Unsupported("cannot cast to BYTEA".to_string())),
            },
            ast::DataType::Numeric(_) | ast::DataType::Decimal(_) | ast::DataType::Dec(_) => {
                val.cast(&DataType::Numeric).map_err(ExecError::Runtime)
            }
            ast::DataType::Interval { .. } => match val {
                Value::Interval { .. } => Ok(val),
                Value::Text(raw) => parse_interval_literal(&raw, None),
                _ => Err(ExecError::Runtime("cannot cast to INTERVAL".into())),
            },
            ast::DataType::Array(elem_def) => {
                // A text literal in Postgres array syntax ('{a,b,c}') casts
                // element-wise to the target element type. Anything else keeps
                // the old pass-through behavior.
                let elem_type = match elem_def {
                    ast::ArrayElemTypeDef::AngleBracket(t)
                    | ast::ArrayElemTypeDef::SquareBracket(t, _)
                    | ast::ArrayElemTypeDef::Parenthesis(t) => Some(t.as_ref()),
                    ast::ArrayElemTypeDef::None => None,
                };
                match (&val, elem_type) {
                    (Value::Text(s), Some(et)) if s.trim().starts_with('{') => {
                        let inner = s.trim().trim_start_matches('{').trim_end_matches('}');
                        let mut out = Vec::new();
                        for part in inner.split(',') {
                            let part = part.trim().trim_matches('"');
                            if part.is_empty() {
                                continue;
                            }
                            out.push(self.eval_cast(Value::Text(part.to_string()), et)?);
                        }
                        Ok(Value::Array(out))
                    }
                    _ => match val {
                        Value::Array(_) => Ok(val),
                        _ => Ok(Value::Array(vec![val])),
                    },
                }
            }
            ast::DataType::Char(_) | ast::DataType::Character(_) => {
                Ok(Value::Text(val.to_string()))
            }
            ast::DataType::Real => match val {
                Value::Float64(_) => Ok(val),
                Value::Int32(n) => Ok(Value::Float64(n as f64)),
                Value::Int64(n) => Ok(Value::Float64(n as f64)),
                Value::Text(s) => s
                    .parse::<f64>()
                    .map(Value::Float64)
                    .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to REAL"))),
                _ => Err(ExecError::Unsupported("cannot cast to REAL".to_string())),
            },
            ast::DataType::SmallInt(_) | ast::DataType::TinyInt(_) => {
                let n = match val {
                    Value::Int32(n) => i64::from(n),
                    Value::Int64(n) => n,
                    Value::Float64(f) => i64::from(f64_to_i32(f)?),
                    Value::Text(s) => s.trim().parse::<i64>().map_err(|_| {
                        ExecError::Unsupported(format!("cannot cast '{s}' to SMALLINT"))
                    })?,
                    _ => {
                        return Err(ExecError::Unsupported("cannot cast to SMALLINT".to_string()));
                    }
                };
                if (i64::from(i16::MIN)..=i64::from(i16::MAX)).contains(&n) {
                    Ok(Value::Int32(n as i32))
                } else {
                    Err(ExecError::Runtime("smallint out of range".into()))
                }
            }
            _ => Err(ExecError::Unsupported(format!("cast to {target}"))),
        }
    }
}

/// PostgreSQL float8→int4 semantics: round half-to-even, error out of range.
fn f64_to_i32(n: f64) -> Result<i32, ExecError> {
    if n.is_nan() || n.is_infinite() {
        return Err(ExecError::Runtime("cannot cast non-finite to integer".into()));
    }
    let r = n.round_ties_even();
    if r >= i32::MIN as f64 && r <= i32::MAX as f64 {
        Ok(r as i32)
    } else {
        Err(ExecError::Runtime("integer out of range".into()))
    }
}

/// PostgreSQL float8→int8 semantics: round half-to-even, error out of range.
fn f64_to_i64(n: f64) -> Result<i64, ExecError> {
    if n.is_nan() || n.is_infinite() {
        return Err(ExecError::Runtime("cannot cast non-finite to bigint".into()));
    }
    let r = n.round_ties_even();
    if r >= i64::MIN as f64 && r < 9_223_372_036_854_775_808.0 {
        Ok(r as i64)
    } else {
        Err(ExecError::Runtime("bigint out of range".into()))
    }
}

/// POSIX regex match for the `~` / `!~` / `~*` / `!~*` operators. NULL operand
/// yields NULL (SQL three-valued logic); a malformed pattern is a loud error,
/// matching Postgres.
pub(super) fn eval_regex_match(
    left: &Value,
    right: &Value,
    negated: bool,
    case_insensitive: bool,
) -> Result<Value, ExecError> {
    let (s, pat) = match (left, right) {
        (Value::Null, _) | (_, Value::Null) => return Ok(Value::Null),
        (Value::Text(s), Value::Text(p)) => (s.clone(), p.clone()),
        (l, Value::Text(p)) => (l.to_string(), p.clone()),
        _ => {
            return Err(ExecError::Unsupported(
                "regex match requires a text pattern".into(),
            ));
        }
    };
    let pattern = if case_insensitive {
        format!("(?i){pat}")
    } else {
        pat
    };
    let re = regex::Regex::new(&pattern)
        .map_err(|e| ExecError::Unsupported(format!("invalid regular expression: {e}")))?;
    let matched = re.is_match(&s);
    Ok(Value::Bool(matched != negated))
}

/// Resolve a `::regclass` cast of a system-catalog name to its fixed
/// PostgreSQL OID. Accepts an optional `pg_catalog.` prefix and surrounding
/// quotes. Returns `None` for anything else (user tables would need async
/// catalog access; callers map that to NULL).
pub(super) fn regclass_oid(name: &str) -> Option<i32> {
    let n = name.trim().trim_matches('\'').trim_matches('"');
    let n = n.strip_prefix("pg_catalog.").unwrap_or(n);
    match n {
        "pg_type" => Some(1247),
        "pg_attribute" => Some(1249),
        "pg_proc" => Some(1255),
        "pg_class" => Some(1259),
        "pg_authid" => Some(1260),
        "pg_database" => Some(1262),
        "pg_description" => Some(2609),
        "pg_index" => Some(2610),
        "pg_namespace" => Some(2615),
        "pg_extension" => Some(3079),
        _ => None,
    }
}

/// Resolve a `::regtype` cast of a type name to its PostgreSQL type OID.
/// Covers the names ORM introspection actually passes; unknown names map to
/// None (callers yield NULL, so comparisons degrade to no-match).
pub(super) fn regtype_oid(name: &str) -> Option<i32> {
    let n = name.trim().trim_matches('\'').trim_matches('"').to_ascii_lowercase();
    let n = n.strip_prefix("pg_catalog.").unwrap_or(&n);
    match n {
        "bool" | "boolean" => Some(16),
        "bytea" => Some(17),
        "int8" | "bigint" => Some(20),
        "int2" | "smallint" => Some(21),
        "int" | "int4" | "integer" => Some(23),
        "text" => Some(25),
        "json" => Some(114),
        "float4" | "real" => Some(700),
        "float8" | "double precision" => Some(701),
        "varchar" | "character varying" => Some(1043),
        "date" => Some(1082),
        "time" => Some(1083),
        "timestamp" => Some(1114),
        "timestamptz" | "timestamp with time zone" => Some(1184),
        "interval" => Some(1186),
        "numeric" | "decimal" => Some(1700),
        "uuid" => Some(2950),
        "jsonb" => Some(3802),
        _ => None,
    }
}

/// Treat a value as an array for ANY/ALL: real arrays pass through; a text
/// value in Postgres array-literal form is parsed element-wise (quoted
/// elements unescaped, unquoted NULL -> Null, numeric-looking elements
/// coerced so int comparisons work). Everything else is None.
fn coerce_to_array(v: Value) -> Option<Vec<Value>> {
    match v {
        Value::Array(vals) => Some(vals),
        Value::Text(s) if s.trim().starts_with('{') && s.trim().ends_with('}') => {
            Some(parse_pg_array_literal(s.trim()))
        }
        _ => None,
    }
}

/// Parse a one-dimensional Postgres array literal ('{a,"b,c",NULL}').
pub(super) fn parse_pg_array_literal(s: &str) -> Vec<Value> {
    let inner = &s[1..s.len() - 1];
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut was_quoted = false;
    let mut chars = inner.chars().peekable();
    let push = |cur: &mut String, was_quoted: bool, out: &mut Vec<Value>| {
        let raw = std::mem::take(cur);
        let trimmed = if was_quoted { raw } else { raw.trim().to_string() };
        if trimmed.is_empty() && !was_quoted {
            return;
        }
        if !was_quoted && trimmed.eq_ignore_ascii_case("null") {
            out.push(Value::Null);
        } else if !was_quoted && let Ok(n) = trimmed.parse::<i64>() {
            out.push(Value::Int64(n));
        } else if !was_quoted && let Ok(f) = trimmed.parse::<f64>() {
            out.push(Value::Float64(f));
        } else {
            out.push(Value::Text(trimmed));
        }
    };
    while let Some(c) = chars.next() {
        match c {
            '"' if !in_quotes => {
                in_quotes = true;
                was_quoted = true;
            }
            '"' if in_quotes => in_quotes = false,
            '\\' if in_quotes => {
                if let Some(esc) = chars.next() {
                    cur.push(esc);
                }
            }
            ',' if !in_quotes => {
                push(&mut cur, was_quoted, &mut out);
                was_quoted = false;
            }
            _ => cur.push(c),
        }
    }
    push(&mut cur, was_quoted, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

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
