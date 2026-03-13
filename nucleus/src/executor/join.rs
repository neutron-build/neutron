//! JOIN execution: inner, left, right, full outer, cross, and hash joins.
//!
//! Extracted from `mod.rs` to reduce file size. All methods are `pub(super)` so
//! the main executor module can delegate to them.

use std::collections::HashMap;

use sqlparser::ast::{self, Expr, SelectItem};

use crate::planner;
use crate::types::{Row, Value};

use super::types::{ColMeta, JoinType};
use super::{ExecError, ExecResult, Executor};
use super::helpers::value_type;

impl Executor {
    // ========================================================================
    // SELECT execution (single table, JOINs, aggregates)
    // ========================================================================

    pub(super) fn execute_select_expressions(
        &self,
        projection: &[SelectItem],
    ) -> Result<ExecResult, ExecError> {
        let mut columns = Vec::new();
        let mut row = Vec::new();
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let value = self.eval_const_expr(expr)?;
                    columns.push((format!("{expr}"), value_type(&value)));
                    row.push(value);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let value = self.eval_const_expr(expr)?;
                    columns.push((alias.value.clone(), value_type(&value)));
                    row.push(value);
                }
                _ => return Err(ExecError::Unsupported("unsupported select item".into())),
            }
        }
        Ok(ExecResult::Select {
            columns,
            rows: vec![row],
        })
    }

    pub(super) fn execute_join(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        right_meta: &[ColMeta],
        right_rows: &[Row],
        operator: &ast::JoinOperator,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        let combined_meta: Vec<ColMeta> = left_meta
            .iter()
            .chain(right_meta.iter())
            .cloned()
            .collect();
        let right_nulls: Row = right_meta.iter().map(|_| Value::Null).collect();
        let left_nulls: Row = left_meta.iter().map(|_| Value::Null).collect();

        let (condition, join_type) = match operator {
            ast::JoinOperator::Join(c) | ast::JoinOperator::Inner(c) => (c, JoinType::Inner),
            ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => (c, JoinType::Left),
            ast::JoinOperator::Right(c) | ast::JoinOperator::RightOuter(c) => (c, JoinType::Right),
            ast::JoinOperator::FullOuter(c) => (c, JoinType::Full),
            ast::JoinOperator::Semi(c) | ast::JoinOperator::LeftSemi(c) => (c, JoinType::Inner),
            ast::JoinOperator::Anti(c) | ast::JoinOperator::LeftAnti(c) => (c, JoinType::Left),
            ast::JoinOperator::CrossJoin(_) => {
                let (meta, rows) = self.cross_join(left_meta, left_rows, right_meta, right_rows);
                return Ok((meta, rows));
            }
            _ => return Err(ExecError::Unsupported("unsupported JOIN type".into())),
        };

        // Build ON expression from USING columns or direct ON clause
        let on_expr: Expr = match condition {
            ast::JoinConstraint::On(expr) => expr.clone(),
            ast::JoinConstraint::Using(columns) => {
                // Convert USING(col1, col2) to ON left.col1 = right.col1 AND left.col2 = right.col2
                let left_table = left_meta.first().and_then(|c| c.table.as_deref()).unwrap_or("left");
                let right_table = right_meta.first().and_then(|c| c.table.as_deref()).unwrap_or("right");
                let mut expr: Option<Expr> = None;
                for col in columns {
                    let col_name = col.to_string();
                    let eq = Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![
                            ast::Ident::new(left_table),
                            ast::Ident::new(&col_name),
                        ])),
                        op: ast::BinaryOperator::Eq,
                        right: Box::new(Expr::CompoundIdentifier(vec![
                            ast::Ident::new(right_table),
                            ast::Ident::new(&col_name),
                        ])),
                    };
                    expr = Some(match expr {
                        Some(prev) => Expr::BinaryOp {
                            left: Box::new(prev),
                            op: ast::BinaryOperator::And,
                            right: Box::new(eq),
                        },
                        None => eq,
                    });
                }
                expr.unwrap_or(Expr::Value(ast::ValueWithSpan {
                    value: ast::Value::Boolean(true),
                    span: sqlparser::tokenizer::Span::empty(),
                }))
            }
            ast::JoinConstraint::Natural => {
                // NATURAL JOIN: find common column names
                let mut expr: Option<Expr> = None;
                for lc in left_meta {
                    for rc in right_meta {
                        if lc.name == rc.name {
                            let eq = Expr::BinaryOp {
                                left: Box::new(Expr::CompoundIdentifier(vec![
                                    ast::Ident::new(lc.table.as_deref().unwrap_or("left")),
                                    ast::Ident::new(&lc.name),
                                ])),
                                op: ast::BinaryOperator::Eq,
                                right: Box::new(Expr::CompoundIdentifier(vec![
                                    ast::Ident::new(rc.table.as_deref().unwrap_or("right")),
                                    ast::Ident::new(&rc.name),
                                ])),
                            };
                            expr = Some(match expr {
                                Some(prev) => Expr::BinaryOp {
                                    left: Box::new(prev),
                                    op: ast::BinaryOperator::And,
                                    right: Box::new(eq),
                                },
                                None => eq,
                            });
                        }
                    }
                }
                expr.unwrap_or(Expr::Value(ast::ValueWithSpan {
                    value: ast::Value::Boolean(true),
                    span: sqlparser::tokenizer::Span::empty(),
                }))
            }
            _ => {
                return Err(ExecError::Unsupported("unsupported JOIN constraint".into()));
            }
        };
        let on_expr = &on_expr;

        // Try hash join for equi-join conditions (O(N+M) vs O(N*M))
        if let Some((left_keys, right_keys, residual)) =
            Self::extract_equijoin_keys(on_expr, left_meta, right_meta)
        {
            let result_rows = self.execute_hash_join(
                left_meta, left_rows, right_meta, right_rows,
                &left_keys, &right_keys, join_type,
                residual.as_ref(), &combined_meta,
            )?;
            return Ok((combined_meta, result_rows));
        }

        // Fallback: nested loop join (no equi-join keys found)
        //
        // Optimization: reuse a single scratch buffer for the combined row to
        // avoid allocating a new Vec for every (left, right) pair. We only
        // clone into the result set when the join condition passes.
        let combined_len = left_meta.len() + right_meta.len();
        let mut scratch: Row = vec![Value::Null; combined_len];
        let left_len = left_meta.len();
        let mut result_rows = Vec::new();
        match join_type {
            JoinType::Inner => {
                for lr in left_rows {
                    scratch[..left_len].clone_from_slice(lr);
                    for rr in right_rows {
                        scratch[left_len..].clone_from_slice(rr);
                        if self.eval_where(on_expr, &scratch, &combined_meta)? {
                            result_rows.push(scratch.clone());
                        }
                    }
                }
            }
            JoinType::Left => {
                for lr in left_rows {
                    let mut matched = false;
                    scratch[..left_len].clone_from_slice(lr);
                    for rr in right_rows {
                        scratch[left_len..].clone_from_slice(rr);
                        if self.eval_where(on_expr, &scratch, &combined_meta)? {
                            result_rows.push(scratch.clone());
                            matched = true;
                        }
                    }
                    if !matched {
                        scratch[left_len..].clone_from_slice(&right_nulls);
                        result_rows.push(scratch.clone());
                    }
                }
            }
            JoinType::Right => {
                for rr in right_rows {
                    let mut matched = false;
                    scratch[left_len..].clone_from_slice(rr);
                    for lr in left_rows {
                        scratch[..left_len].clone_from_slice(lr);
                        if self.eval_where(on_expr, &scratch, &combined_meta)? {
                            result_rows.push(scratch.clone());
                            matched = true;
                        }
                    }
                    if !matched {
                        scratch[..left_len].clone_from_slice(&left_nulls);
                        result_rows.push(scratch.clone());
                    }
                }
            }
            JoinType::Full => {
                let mut right_matched = vec![false; right_rows.len()];
                for lr in left_rows {
                    let mut left_matched = false;
                    scratch[..left_len].clone_from_slice(lr);
                    for (ri, rr) in right_rows.iter().enumerate() {
                        scratch[left_len..].clone_from_slice(rr);
                        if self.eval_where(on_expr, &scratch, &combined_meta)? {
                            result_rows.push(scratch.clone());
                            left_matched = true;
                            right_matched[ri] = true;
                        }
                    }
                    if !left_matched {
                        scratch[left_len..].clone_from_slice(&right_nulls);
                        result_rows.push(scratch.clone());
                    }
                }
                // Add unmatched right rows
                for (ri, rr) in right_rows.iter().enumerate() {
                    if !right_matched[ri] {
                        scratch[..left_len].clone_from_slice(&left_nulls);
                        scratch[left_len..].clone_from_slice(rr);
                        result_rows.push(scratch.clone());
                    }
                }
            }
        }

        Ok((combined_meta, result_rows))
    }

    /// Try to extract equi-join key column indices from an ON expression.
    /// Returns (left_key_indices, right_key_indices, residual_expr) where indices
    /// are column positions in the respective side's metadata.
    /// Only handles simple `left.col = right.col` (or `col = col` with unambiguous resolution).
    pub(super) fn extract_equijoin_keys(
        on_expr: &Expr,
        left_meta: &[ColMeta],
        right_meta: &[ColMeta],
    ) -> Option<(Vec<usize>, Vec<usize>, Option<Expr>)> {
        let conjuncts = planner::split_conjunction(on_expr);
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        let mut residual_parts: Vec<Expr> = Vec::new();

        let left_len = left_meta.len();

        for conj in conjuncts {
            if let Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } = conj {
                // Try to resolve each side to a column index in left or right metadata
                let l_idx = Self::resolve_col_idx_in_meta(left, left_meta, 0);
                let r_idx = Self::resolve_col_idx_in_meta(right, right_meta, left_len);
                let l_idx_r = Self::resolve_col_idx_in_meta(left, right_meta, left_len);
                let r_idx_l = Self::resolve_col_idx_in_meta(right, left_meta, 0);

                if let (Some(li), Some(ri)) = (l_idx, r_idx) {
                    left_keys.push(li);
                    right_keys.push(ri);
                    continue;
                }
                if let (Some(ri), Some(li)) = (l_idx_r, r_idx_l) {
                    // Swapped: left expr references right table, right expr references left table
                    left_keys.push(li);
                    right_keys.push(ri);
                    continue;
                }
            }
            residual_parts.push(conj.clone());
        }

        if left_keys.is_empty() {
            return None;
        }

        let residual = if residual_parts.is_empty() {
            None
        } else {
            let mut expr = residual_parts.remove(0);
            for part in residual_parts {
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op: ast::BinaryOperator::And,
                    right: Box::new(part),
                };
            }
            Some(expr)
        };

        Some((left_keys, right_keys, residual))
    }

    /// Resolve a column expression to a 0-based index within the given metadata.
    pub(super) fn resolve_col_idx_in_meta(
        expr: &Expr,
        meta: &[ColMeta],
        _offset: usize,
    ) -> Option<usize> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_lowercase();
                meta.iter().position(|c| c.name.to_lowercase() == name)
            }
            Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                let table = idents[0].value.to_lowercase();
                let col = idents[1].value.to_lowercase();
                meta.iter().position(|c| {
                    c.name.to_lowercase() == col
                        && c.table.as_ref().map(|t| t.to_lowercase()) == Some(table.clone())
                })
            }
            _ => None,
        }
    }

    /// Hash join: build a hash table on the build side, probe from the probe side.
    /// Dramatically faster than nested loop for equi-joins: O(N+M) vs O(N*M).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_hash_join(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        right_meta: &[ColMeta],
        right_rows: &[Row],
        left_keys: &[usize],
        right_keys: &[usize],
        join_type: JoinType,
        residual: Option<&Expr>,
        combined_meta: &[ColMeta],
    ) -> Result<Vec<Row>, ExecError> {
        use std::hash::{Hash, Hasher, DefaultHasher};

        // Hash function for a vector of Values — uses Value's Hash impl directly
        fn hash_key(vals: &[Value]) -> u64 {
            let mut h = DefaultHasher::new();
            for v in vals {
                v.hash(&mut h);
            }
            h.finish()
        }

        fn vals_eq(a: &[Value], b: &[Value]) -> bool {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| {
                match (x, y) {
                    (Value::Null, _) | (_, Value::Null) => false, // NULL != anything
                    _ => x == y,
                }
            })
        }

        let right_nulls: Row = right_meta.iter().map(|_| Value::Null).collect();
        let left_nulls: Row = left_meta.iter().map(|_| Value::Null).collect();
        let mut result_rows = Vec::new();

        // Build phase: hash the right side (typically smaller for INNER/LEFT joins)
        // For RIGHT join, we build on the left side instead.
        // Store extracted keys alongside row indices in the hash table to avoid
        // re-extracting keys on every collision during the probe phase.
        match join_type {
            JoinType::Inner | JoinType::Left => {
                // Build hash table on right side, probe with left
                let mut ht: HashMap<u64, Vec<(usize, Vec<Value>)>> = HashMap::new();
                for (ri, rr) in right_rows.iter().enumerate() {
                    let key: Vec<Value> = right_keys.iter().map(|&k| rr[k].clone()).collect();
                    let h = hash_key(&key);
                    ht.entry(h).or_default().push((ri, key));
                }

                for lr in left_rows {
                    let probe_key: Vec<Value> = left_keys.iter().map(|&k| lr[k].clone()).collect();
                    let h = hash_key(&probe_key);
                    let mut matched = false;

                    if let Some(bucket) = ht.get(&h) {
                        for (ri, stored_key) in bucket {
                            if vals_eq(&probe_key, stored_key) {
                                let rr = &right_rows[*ri];
                                let combined: Row = lr.iter().chain(rr.iter()).cloned().collect();
                                if let Some(res) = residual {
                                    if self.eval_where(res, &combined, combined_meta).unwrap_or(false) {
                                        result_rows.push(combined);
                                        matched = true;
                                    }
                                } else {
                                    result_rows.push(combined);
                                    matched = true;
                                }
                            }
                        }
                    }

                    if !matched && join_type == JoinType::Left {
                        let combined: Row = lr.iter().chain(right_nulls.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
            JoinType::Right => {
                // Build hash table on left side, probe with right
                let mut ht: HashMap<u64, Vec<(usize, Vec<Value>)>> = HashMap::new();
                for (li, lr) in left_rows.iter().enumerate() {
                    let key: Vec<Value> = left_keys.iter().map(|&k| lr[k].clone()).collect();
                    let h = hash_key(&key);
                    ht.entry(h).or_default().push((li, key));
                }

                for rr in right_rows {
                    let probe_key: Vec<Value> = right_keys.iter().map(|&k| rr[k].clone()).collect();
                    let h = hash_key(&probe_key);
                    let mut matched = false;

                    if let Some(bucket) = ht.get(&h) {
                        for (li, stored_key) in bucket {
                            if vals_eq(&probe_key, stored_key) {
                                let lr = &left_rows[*li];
                                let combined: Row = lr.iter().chain(rr.iter()).cloned().collect();
                                if let Some(res) = residual {
                                    if self.eval_where(res, &combined, combined_meta).unwrap_or(false) {
                                        result_rows.push(combined);
                                        matched = true;
                                    }
                                } else {
                                    result_rows.push(combined);
                                    matched = true;
                                }
                            }
                        }
                    }

                    if !matched {
                        let combined: Row = left_nulls.iter().chain(rr.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
            JoinType::Full => {
                // Build hash table on right side
                let mut ht: HashMap<u64, Vec<(usize, Vec<Value>)>> = HashMap::new();
                for (ri, rr) in right_rows.iter().enumerate() {
                    let key: Vec<Value> = right_keys.iter().map(|&k| rr[k].clone()).collect();
                    let h = hash_key(&key);
                    ht.entry(h).or_default().push((ri, key));
                }

                let mut right_matched = vec![false; right_rows.len()];

                for lr in left_rows {
                    let probe_key: Vec<Value> = left_keys.iter().map(|&k| lr[k].clone()).collect();
                    let h = hash_key(&probe_key);
                    let mut left_matched = false;

                    if let Some(bucket) = ht.get(&h) {
                        for (ri, stored_key) in bucket {
                            if vals_eq(&probe_key, stored_key) {
                                let rr = &right_rows[*ri];
                                let combined: Row = lr.iter().chain(rr.iter()).cloned().collect();
                                if let Some(res) = residual {
                                    if self.eval_where(res, &combined, combined_meta).unwrap_or(false) {
                                        result_rows.push(combined);
                                        left_matched = true;
                                        right_matched[*ri] = true;
                                    }
                                } else {
                                    result_rows.push(combined);
                                    left_matched = true;
                                    right_matched[*ri] = true;
                                }
                            }
                        }
                    }

                    if !left_matched {
                        let combined: Row = lr.iter().chain(right_nulls.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }

                // Unmatched right rows
                for (ri, rr) in right_rows.iter().enumerate() {
                    if !right_matched[ri] {
                        let combined: Row = left_nulls.iter().chain(rr.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
        }

        // Check that the result set fits within the query memory budget.
        // This acts as a circuit-breaker: if the join produces more data than
        // the configured limit, we fail fast instead of OOM-ing.
        // We immediately release the accounting since the budget is a
        // concurrent-query gate, not a long-lived reservation.
        if !result_rows.is_empty() {
            let accounted_bytes: u64 = result_rows.iter().map(Self::estimate_row_bytes).sum();
            self.query_memory.try_allocate(accounted_bytes).map_err(|_| {
                ExecError::Unsupported(format!(
                    "hash join result exceeded memory limit ({} MB)",
                    self.query_memory.limit() / (1024 * 1024)
                ))
            })?;
            self.query_memory.deallocate(accounted_bytes);
        }

        Ok(result_rows)
    }

    pub(super) fn cross_join(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        right_meta: &[ColMeta],
        right_rows: &[Row],
    ) -> (Vec<ColMeta>, Vec<Row>) {
        let combined_meta: Vec<ColMeta> = left_meta
            .iter()
            .chain(right_meta.iter())
            .cloned()
            .collect();
        let mut rows = Vec::with_capacity(left_rows.len() * right_rows.len());
        for lr in left_rows {
            for rr in right_rows {
                rows.push(lr.iter().chain(rr.iter()).cloned().collect());
            }
        }
        (combined_meta, rows)
    }
}
