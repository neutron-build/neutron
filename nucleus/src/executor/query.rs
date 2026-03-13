//! Query execution methods for the executor.
//!
//! Contains EXPLAIN, query planning, plan-driven execution, SELECT with
//! ORDER BY / LIMIT / OFFSET, set expressions (UNION / INTERSECT / EXCEPT),
//! CTE resolution (WITH clause), and all supporting helpers for index scans,
//! columnar fast aggregates, SIMD filters, and lateral joins.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sqlparser::ast::{self, Expr, SelectItem, SetExpr, Statement, TableFactor};

use crate::planner;
use crate::simd;
use crate::types::{DataType, Row, Value};

use super::types::{
    BoxedExecFuture, CacheLiteral, ColMeta, CteTableMap, IndexPredicates, IndexScanResult,
    JoinType, ProjectedResult, SelectResult,
};
use super::{ExecError, ExecResult, Executor};
use super::helpers::*;

impl Executor {
    // ========================================================================
    // EXPLAIN
    // ========================================================================

    pub(super) fn execute_explain(
        &self,
        stmt: Statement,
        analyze: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecResult, ExecError>> + Send + '_>> {
        Box::pin(async move {
            let plan = self.build_plan(&stmt).await?;

            if analyze {
                // EXPLAIN ANALYZE: actually execute the query and report actual rows + time.
                let start = std::time::Instant::now();
                let exec_result = self.execute_statement(stmt).await?;
                let elapsed = start.elapsed();
                let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

                let actual_rows = match &exec_result {
                    ExecResult::Select { rows, .. } => rows.len(),
                    ExecResult::Command { rows_affected, .. } => *rows_affected,
                    ExecResult::CopyOut { row_count, .. } => *row_count,
                };

                // Build annotated plan text with actual execution stats
                let plan_text = format!("{plan}");
                let mut lines: Vec<String> = Vec::new();
                for line in plan_text.lines() {
                    lines.push(line.to_string());
                }
                lines.push(String::new());
                lines.push(format!("Actual Rows: {actual_rows}"));
                lines.push(format!("Execution Time: {elapsed_ms:.3} ms"));

                Ok(ExecResult::Select {
                    columns: vec![("QUERY PLAN".into(), DataType::Text)],
                    rows: lines
                        .iter()
                        .map(|line| vec![Value::Text(line.clone())])
                        .collect(),
                })
            } else {
                // Basic EXPLAIN: show the query plan tree
                let explain_text = format!("{plan}");

                Ok(ExecResult::Select {
                    columns: vec![("QUERY PLAN".into(), DataType::Text)],
                    rows: explain_text
                        .lines()
                        .map(|line| vec![Value::Text(line.to_string())])
                        .collect(),
                })
            }
        })
    }

    /// Build a query plan for a statement (used by EXPLAIN).
    pub(super) async fn build_plan(&self, stmt: &Statement) -> Result<planner::PlanNode, ExecError> {
        match stmt {
            Statement::Query(query) => self.plan_query(query).await,
            _ => Err(ExecError::Unsupported(
                "EXPLAIN only supports SELECT queries".into(),
            )),
        }
    }

    /// Build a plan tree for a SELECT query.
    pub(super) async fn plan_query(&self, query: &ast::Query) -> Result<planner::PlanNode, ExecError> {
        let select = match query.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => {
                return Err(ExecError::Unsupported(
                    "EXPLAIN only supports simple SELECT".into(),
                ))
            }
        };

        let has_joins = select
            .from
            .first()
            .map(|f| !f.joins.is_empty())
            .unwrap_or(false);
        let mut remaining_join_preds: Vec<Expr> = if has_joins {
            match &select.selection {
                Some(expr) => planner::split_conjunction(expr).into_iter().cloned().collect(),
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // Build the base scan plan.
        let mut plan = if let Some(from) = select.from.first() {
            let base_where = if has_joins {
                let relation_names = Self::table_factor_names(&from.relation);
                let (pushable, remaining) =
                    Self::partition_predicates_for_relation(remaining_join_preds, &relation_names);
                remaining_join_preds = remaining;
                Self::combine_predicates(pushable)
            } else {
                select.selection.clone()
            };
            self.plan_table_scan(&from.relation, &base_where).await?
        } else {
            planner::PlanNode::SeqScan {
                table: "<values>".into(),
                estimated_rows: 1,
                estimated_cost: planner::Cost::zero(),
                filter: None,
                filter_expr: None,
                scan_limit: None,
                projection: None,
            }
        };

        // Joins — extract join conditions and use the planner to choose join strategy
        if let Some(from) = select.from.first() {
            for join in &from.joins {
                let right_where = if has_joins {
                    let relation_names = Self::table_factor_names(&join.relation);
                    let (pushable, remaining) =
                        Self::partition_predicates_for_relation(remaining_join_preds, &relation_names);
                    remaining_join_preds = remaining;
                    Self::combine_predicates(pushable)
                } else {
                    None
                };
                let right_plan = self.plan_table_scan(&join.relation, &right_where).await?;
                let join_type = match &join.join_operator {
                    ast::JoinOperator::Inner(_) | ast::JoinOperator::Join(_) => planner::JoinPlanType::Inner,
                    ast::JoinOperator::LeftOuter(_)
                    | ast::JoinOperator::Left(_) => planner::JoinPlanType::Left,
                    ast::JoinOperator::RightOuter(_)
                    | ast::JoinOperator::Right(_) => planner::JoinPlanType::Right,
                    ast::JoinOperator::CrossJoin(_) => planner::JoinPlanType::Cross,
                    _ => planner::JoinPlanType::Inner,
                };

                // Extract the join condition from the JoinConstraint
                let join_condition: Option<Expr> = match &join.join_operator {
                    ast::JoinOperator::Inner(c)
                    | ast::JoinOperator::Join(c)
                    | ast::JoinOperator::Left(c)
                    | ast::JoinOperator::LeftOuter(c)
                    | ast::JoinOperator::Right(c)
                    | ast::JoinOperator::RightOuter(c)
                    | ast::JoinOperator::FullOuter(c) => {
                        match c {
                            ast::JoinConstraint::On(expr) => Some(expr.clone()),
                            _ => None,
                        }
                    }
                    _ => None,
                };

                let query_planner = planner::QueryPlanner::new(
                    Arc::clone(&self.catalog),
                    Arc::clone(&self.stats_store),
                );

                plan = query_planner.plan_join(
                    plan,
                    right_plan,
                    join_type,
                    join_condition.as_ref(),
                );
            }
        }

        // Remaining WHERE predicates for join queries (applied after join composition).
        if has_joins
            && let Some(where_expr) = Self::combine_predicates(remaining_join_preds) {
                let estimated_rows = (plan.estimated_rows() / 2).max(1);
                let filter_cost = planner::Cost(
                    plan.total_cost().0 + plan.estimated_rows() as f64 * planner::CPU_OPERATOR_COST,
                );
                plan = planner::PlanNode::Filter {
                    input: Box::new(plan),
                    predicate: where_expr.to_string(),
                    predicate_expr: Some(where_expr),
                    estimated_rows,
                    estimated_cost: filter_cost,
                };
            }

        // Detect aggregate functions in the projection
        let agg_funcs = Self::extract_aggregate_names(&select.projection);

        // GROUP BY
        let has_group_by = if let ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
            if !exprs.is_empty() {
                let input_rows = plan.estimated_rows();
                let group_keys: Vec<String> = exprs.iter().map(|e| e.to_string()).collect();
                let distinct_groups = (input_rows / 10).max(1);
                let agg_cost =
                    planner::Cost(plan.total_cost().0 + input_rows as f64 * planner::CPU_TUPLE_COST);
                plan = planner::PlanNode::HashAggregate {
                    input: Box::new(plan),
                    group_keys,
                    aggregates: agg_funcs.clone(),
                    estimated_rows: distinct_groups,
                    estimated_cost: agg_cost,
                };
                true
            } else {
                false
            }
        } else {
            false
        };

        // Simple aggregate (e.g., SELECT COUNT(*) FROM t) without GROUP BY
        if !has_group_by && !agg_funcs.is_empty() {
            let agg_cost =
                planner::Cost(plan.total_cost().0 + plan.estimated_rows() as f64 * planner::CPU_TUPLE_COST);
            plan = planner::PlanNode::Aggregate {
                input: Box::new(plan),
                aggregates: agg_funcs,
                estimated_cost: agg_cost,
            };
        }

        // HAVING (applied after GROUP BY / aggregate computation)
        if let Some(having_expr) = &select.having {
            let estimated_rows = (plan.estimated_rows() / 2).max(1);
            let filter_cost = planner::Cost(
                plan.total_cost().0 + plan.estimated_rows() as f64 * planner::CPU_OPERATOR_COST,
            );
            plan = planner::PlanNode::Filter {
                input: Box::new(plan),
                predicate: having_expr.to_string(),
                predicate_expr: Some(having_expr.clone()),
                estimated_rows,
                estimated_cost: filter_cost,
            };
        }

        // Projection
        let proj_columns: Vec<String> = select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(e) => e.to_string(),
                SelectItem::ExprWithAlias { expr, alias } => format!("{expr} AS {alias}"),
                SelectItem::Wildcard(_) => "*".into(),
                _ => "?".into(),
            })
            .collect();

        if !proj_columns.iter().all(|c| c == "*") {
            let cost = planner::Cost(
                plan.total_cost().0 + plan.estimated_rows() as f64 * planner::CPU_OPERATOR_COST,
            );
            plan = planner::PlanNode::Project {
                input: Box::new(plan),
                columns: proj_columns,
                estimated_cost: cost,
            };

            // Try to push projection indices into the child SeqScan for
            // zero-copy partial deserialization via `scan_projected()`.
            self.try_push_projection_into_scan(&mut plan).await;
        }

        // ORDER BY
        if let Some(ref order_by) = query.order_by
            && let ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                let sort_keys: Vec<String> = exprs
                    .iter()
                    .map(|o| {
                        let dir = if o.options.asc.unwrap_or(true) { "ASC" } else { "DESC" };
                        let nulls = match o.options.nulls_first {
                            Some(true) => " NULLS FIRST",
                            Some(false) => " NULLS LAST",
                            None => "",
                        };
                        format!("{} {dir}{nulls}", o.expr)
                    })
                    .collect();
                let input_rows = plan.estimated_rows();
                let sort_cost = planner::estimate_sort_cost(input_rows, plan.total_cost());
                plan = planner::PlanNode::Sort {
                    input: Box::new(plan),
                    keys: sort_keys,
                    estimated_cost: sort_cost,
                };
            }

        // LIMIT / OFFSET
        let (limit_val, offset_val) = match &query.limit_clause {
            Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                let l = limit.as_ref().and_then(|e| self.plan_expr_to_usize(e));
                let o = offset.as_ref().and_then(|off| self.plan_expr_to_usize(&off.value));
                (l, o)
            }
            _ => (None, None),
        };

        if limit_val.is_some() || offset_val.is_some() {
            let cost = planner::Cost(
                plan.total_cost().0
                    + limit_val.unwrap_or(0) as f64 * planner::CPU_TUPLE_COST,
            );
            // LIMIT pushdown: if the child is a SeqScan (no Sort/Aggregate),
            // push limit into the scan to enable early exit.
            if let Some(lim) = limit_val {
                let effective = lim + offset_val.unwrap_or(0);
                Self::push_limit_into_scan(&mut plan, effective);
            }
            plan = planner::PlanNode::Limit {
                input: Box::new(plan),
                limit: limit_val,
                offset: offset_val,
                estimated_cost: cost,
            };
        }

        Ok(plan)
    }

    /// Try to push projection column indices from a `Project` node down into its
    /// child `SeqScan` node.  This enables `scan_projected()` — zero-copy partial
    /// deserialization that skips non-projected columns during page decoding.
    ///
    /// The pushdown is only attempted when:
    /// - The plan root is a `Project` whose immediate child is a `SeqScan`.
    /// - The `SeqScan` has no filter (filter evaluation may reference columns
    ///   outside the projection).
    /// - Every projection item is a plain column name (no expressions, no aliases
    ///   with computed expressions).
    async fn try_push_projection_into_scan(&self, plan: &mut planner::PlanNode) {
        let planner::PlanNode::Project { input, columns, .. } = plan else { return };
        let planner::PlanNode::SeqScan {
            table,
            filter,
            projection,
            ..
        } = input.as_mut() else { return };

        // Only push when there is no filter — filter evaluation may reference
        // columns outside the projection, so we need the full row.
        if filter.is_some() {
            return;
        }

        // Already has a projection (shouldn't happen, but guard against it).
        if projection.is_some() {
            return;
        }

        // Resolve the table schema to map column names → indices.
        let table_def = match self.get_table(table).await {
            Ok(td) => td,
            Err(_) => return, // schema unavailable — skip optimisation
        };

        let mut indices: Vec<usize> = Vec::with_capacity(columns.len());
        for col_spec in columns.iter() {
            // Strip " AS alias" suffix — we only care about the source column name.
            let col_name = if let Some(pos) = col_spec.to_uppercase().rfind(" AS ") {
                col_spec[..pos].trim()
            } else {
                col_spec.trim()
            };

            // Only push simple column references — skip expressions, *, etc.
            if col_name == "*" || col_name == "?" || col_name.contains('(') {
                return;
            }

            // Try to match against table columns (case-insensitive, strip table prefix).
            let unqualified = col_name
                .split('.')
                .next_back()
                .unwrap_or(col_name)
                .trim_matches('"');

            if let Some(idx) = table_def
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(unqualified))
            {
                indices.push(idx);
            } else {
                // Column not found in schema — probably a computed expression
                // that looks like a column name.  Bail out.
                return;
            }
        }

        // All columns resolved — push the projection into the SeqScan.
        *projection = Some(indices);
    }

    /// Push a LIMIT hint into a SeqScan node if no Sort or Aggregate sits between.
    /// Only pushes when the scan has no filter (exact row count is known).
    pub(super) fn push_limit_into_scan(plan: &mut planner::PlanNode, effective_limit: usize) {
        match plan {
            planner::PlanNode::SeqScan { filter: None, scan_limit, .. } => {
                *scan_limit = Some(effective_limit);
            }
            // Walk through Project/Filter but NOT Sort/Aggregate
            planner::PlanNode::Project { input, .. }
            | planner::PlanNode::Filter { input, .. } => {
                Self::push_limit_into_scan(input, effective_limit);
            }
            _ => {} // Don't push through Sort, Aggregate, Join, etc.
        }
    }

    /// Plan a single table scan with optional WHERE predicate.
    pub(super) async fn plan_table_scan(
        &self,
        table_factor: &TableFactor,
        where_clause: &Option<Expr>,
    ) -> Result<planner::PlanNode, ExecError> {
        let table_name = match table_factor {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(ExecError::Unsupported("subqueries in FROM not planned yet".into())),
        };

        let table_def = self.get_table(&table_name).await?;

        // ── Fast path: single PK equality (Fix 4) ──────────────────────
        // When the WHERE clause is exactly `pk_col = literal`, skip the
        // full cost-model planning and emit an IndexScan directly. This
        // handles the most common OLTP pattern (point lookup by PK) with
        // minimal overhead.
        if let Some(where_expr) = where_clause {
            let predicates = planner::split_conjunction(where_expr);
            if predicates.len() == 1
                && let Some((col, _val)) = planner::is_equality_predicate(predicates[0])
                && let Some(pk_cols) = table_def.primary_key_columns()
                && pk_cols.len() == 1
                && pk_cols[0].eq_ignore_ascii_case(&col)
            {
                // Find the B-tree index on this PK column (sync cache avoids async lock)
                let indexes = self.catalog.get_indexes_cached(&table_name)
                    .unwrap_or_default();
                let pk_index = indexes.iter().find(|idx| {
                    matches!(idx.index_type, crate::catalog::IndexType::BTree)
                        && idx.columns.len() == 1
                        && idx.columns[0].eq_ignore_ascii_case(&col)
                });
                if let Some(idx) = pk_index {
                    let lookup_str = predicates[0].to_string();
                    let lookup_expr = Some(predicates[0].clone());
                    return Ok(planner::PlanNode::IndexScan {
                        table: table_name,
                        index_name: idx.name.clone(),
                        estimated_rows: 1,
                        estimated_cost: planner::Cost(1.0),
                        lookup_key: Some(lookup_str),
                        lookup_key_expr: lookup_expr,
                        range_lo: None,
                        range_lo_expr: None,
                        range_hi: None,
                        range_hi_expr: None,
                        range_predicate: None,
                        range_predicate_expr: None,
                    });
                }
            }
        }

        // Use the shared StatsStore + QueryPlanner for cost-based scan selection.
        // If ANALYZE has been run, the planner uses real stats; otherwise defaults.
        let query_planner = planner::QueryPlanner::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.stats_store),
        );

        let plan = if let Some(where_expr) = where_clause {
            let predicates = planner::split_conjunction(where_expr);
            query_planner.plan_scan_unified(&table_name, &predicates).await
        } else {
            query_planner.plan_scan_unified(&table_name, &[]).await
        };
        Ok(plan)
    }

    /// Extract a usize from a constant expression (planner-only, returns Option).
    pub(super) fn plan_expr_to_usize(&self, expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n.parse::<usize>().ok(),
                _ => None,
            },
            _ => None,
        }
    }

    /// Extract aggregate function names from projection items.
    /// Returns names like "COUNT(*)", "SUM(amount)", etc.
    pub(super) fn extract_aggregate_names(projection: &[SelectItem]) -> Vec<String> {
        let mut agg_names = Vec::new();
        for item in projection {
            let expr = match item {
                SelectItem::UnnamedExpr(e) => Some(e),
                SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                _ => None,
            };
            if let Some(e) = expr {
                Self::collect_aggregates_from_expr(e, &mut agg_names);
            }
        }
        agg_names
    }

    /// Recursively collect aggregate function calls from an expression.
    pub(super) fn collect_aggregates_from_expr(expr: &Expr, out: &mut Vec<String>) {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                match name.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                    | "ARRAY_AGG" | "STRING_AGG" | "JSON_AGG" | "BOOL_AND" | "BOOL_OR"
                    | "STDDEV" | "VARIANCE" | "STDDEV_POP" | "STDDEV_SAMP"
                    | "VAR_POP" | "VAR_SAMP" => {
                        out.push(format!("{expr}"));
                    }
                    _ => {}
                }
                // Also recurse into function args in case of nested aggregates
                if let ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner)) = arg {
                            Self::collect_aggregates_from_expr(inner, out);
                        }
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_aggregates_from_expr(left, out);
                Self::collect_aggregates_from_expr(right, out);
            }
            Expr::Nested(inner) => {
                Self::collect_aggregates_from_expr(inner, out);
            }
            _ => {}
        }
    }

    // ========================================================================
    // Plan-driven execution
    // ========================================================================

    /// Check if a SELECT query is safe enough for plan-driven execution.
    /// Returns false for subqueries and expression features we still cannot
    /// evaluate correctly in the plan path.
    pub(super) fn query_eligible_for_plan(select: &ast::Select, query: &ast::Query) -> bool {
        // No DISTINCT ON (plain DISTINCT is ok)
        if let Some(ast::Distinct::On(_)) = &select.distinct { return false; }
        // Projection expressions must be evaluable by the plan path.
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if Self::expr_has_unsupported(expr) { return false; }
                }
                SelectItem::Wildcard(_) => {}
                // Qualified wildcards like t.* need special handling
                SelectItem::QualifiedWildcard(_, _) => return false,
            }
        }
        // No unsupported features in WHERE
        if let Some(ref where_expr) = select.selection
            && Self::expr_has_unsupported(where_expr) { return false; }
        // No unsupported features in HAVING
        if let Some(ref having_expr) = select.having
            && Self::expr_has_unsupported(having_expr) { return false; }
        if let Some(from) = select.from.first() {
            for join in &from.joins {
                match &join.join_operator {
                    ast::JoinOperator::LeftOuter(_) | ast::JoinOperator::Left(_)
                    | ast::JoinOperator::RightOuter(_) | ast::JoinOperator::Right(_)
                    | ast::JoinOperator::FullOuter(_) => return false,
                    _ => {}
                }
            }
            // No subqueries in FROM
            match &from.relation {
                TableFactor::Derived { .. } | TableFactor::NestedJoin { .. } => return false,
                _ => {}
            }
            // Table aliases in join queries break condition evaluation in the plan path
            // because SeqScan meta uses the actual table name, not the alias.
            // Fall back to AST execution when joins use aliases.
            if !from.joins.is_empty() {
                let base_alias = if let TableFactor::Table { alias, name, .. } = &from.relation {
                    alias.as_ref().map(|a| a.name.value.as_str() != name.to_string().as_str()).unwrap_or(false)
                } else { false };
                let join_has_alias = from.joins.iter().any(|j| {
                    if let TableFactor::Table { alias, name, .. } = &j.relation {
                        alias.as_ref().map(|a| a.name.value.as_str() != name.to_string().as_str()).unwrap_or(false)
                    } else { false }
                });
                if base_alias || join_has_alias { return false; }
            }
        }
        // No UNION/INTERSECT/EXCEPT
        if !matches!(*query.body, SetExpr::Select(_)) { return false; }
        // No unsupported expressions in ORDER BY
        if let Some(ref order_by) = query.order_by
            && let ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                for ob_expr in exprs {
                    if Self::expr_has_unsupported(&ob_expr.expr) { return false; }
                }
            }
        true
    }

    /// Check if an expression contains features unsupported by plan execution.
    pub(super) fn expr_has_unsupported(expr: &Expr) -> bool {
        match expr {
            // Aggregate functions in SELECT/HAVING are supported.
            Expr::Function(func) => !Self::is_supported_plan_function(func),
            // Subqueries
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
            // LIKE / ILIKE
            Expr::Like { .. } | Expr::ILike { .. } | Expr::SimilarTo { .. } => true,
            // CASE WHEN
            Expr::Case { .. } => true,
            // CAST
            Expr::Cast { .. } => true,
            // BETWEEN (non-negated) is supported in plan filtering.
            Expr::Between { expr, low, high, negated } => {
                if *negated {
                    true
                } else {
                    Self::expr_has_unsupported(expr)
                        || Self::expr_has_unsupported(low)
                        || Self::expr_has_unsupported(high)
                }
            }
            // IN (list) — we could handle simple lists but skip for safety
            Expr::InList { .. } => true,
            // Array/struct constructors
            Expr::Array(_) => true,
            // Recurse into compound expressions
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_unsupported(left) || Self::expr_has_unsupported(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_has_unsupported(expr),
            Expr::Nested(inner) => Self::expr_has_unsupported(inner),
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => Self::expr_has_unsupported(inner),
            // Simple identifiers and values are fine
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => false,
            // Anything else we don't recognize — skip plan execution
            _ => true,
        }
    }

    pub(super) fn is_supported_plan_function(func: &ast::Function) -> bool {
        // Window functions (OVER clause) are not handled by the plan execution path.
        if func.over.is_some() { return false; }

        let fn_name = func.name.to_string().to_uppercase();
        match fn_name.as_str() {
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                match &func.args {
                    ast::FunctionArguments::List(arg_list) => {
                        // DISTINCT aggregates (COUNT DISTINCT, SUM DISTINCT) are not handled
                        // by the plan execution path — fall back to AST.
                        if matches!(arg_list.duplicate_treatment, Some(ast::DuplicateTreatment::Distinct)) {
                            return false;
                        }
                        for arg in &arg_list.args {
                            match arg {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                    Expr::Identifier(_) | Expr::CompoundIdentifier(_),
                                )) => {
                                    // Only simple column references are supported inside aggregates.
                                    // Expressions like SUM(a * b) or SUM(a + 1) require expression
                                    // evaluation per row, which the plan aggregate path doesn't do.
                                }
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(_)) => {
                                    return false;
                                }
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {}
                                _ => return false,
                            }
                        }
                    }
                    _ => return false,
                }
                true
            }
            _ => false,
        }
    }

    pub(super) fn table_factor_names(factor: &TableFactor) -> HashSet<String> {
        let mut names = HashSet::new();
        if let TableFactor::Table { name, alias, .. } = factor {
            names.insert(name.to_string().to_lowercase());
            if let Some(a) = alias {
                names.insert(a.name.value.to_lowercase());
            }
        }
        names
    }

    pub(super) fn collect_expr_table_refs(expr: &Expr, out: &mut HashSet<String>) {
        match expr {
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                out.insert(parts[0].value.to_lowercase());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_expr_table_refs(left, out);
                Self::collect_expr_table_refs(right, out);
            }
            Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::Nested(expr) => Self::collect_expr_table_refs(expr, out),
            Expr::Between { expr, low, high, .. } => {
                Self::collect_expr_table_refs(expr, out);
                Self::collect_expr_table_refs(low, out);
                Self::collect_expr_table_refs(high, out);
            }
            Expr::InList { expr, list, .. } => {
                Self::collect_expr_table_refs(expr, out);
                for item in list {
                    Self::collect_expr_table_refs(item, out);
                }
            }
            Expr::Function(func) => {
                if let ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg {
                            Self::collect_expr_table_refs(e, out);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    pub(super) fn partition_predicates_for_relation(
        predicates: Vec<Expr>,
        relation_names: &HashSet<String>,
    ) -> (Vec<Expr>, Vec<Expr>) {
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for pred in predicates {
            let mut refs = HashSet::new();
            Self::collect_expr_table_refs(&pred, &mut refs);
            if !refs.is_empty() && refs.iter().all(|r| relation_names.contains(r)) {
                pushable.push(pred);
            } else {
                remaining.push(pred);
            }
        }
        (pushable, remaining)
    }

    pub(super) fn combine_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        predicates.into_iter().reduce(|a, b| Expr::BinaryOp {
            left: Box::new(a),
            op: ast::BinaryOperator::And,
            right: Box::new(b),
        })
    }

    pub(super) fn collect_from_relation_names(from: &[ast::TableWithJoins]) -> HashSet<String> {
        let mut names = HashSet::new();
        for twj in from {
            names.extend(Self::table_factor_names(&twj.relation));
            for join in &twj.joins {
                names.extend(Self::table_factor_names(&join.relation));
            }
        }
        names
    }

    pub(super) fn from_has_outer_join(from: &[ast::TableWithJoins]) -> bool {
        for twj in from {
            for join in &twj.joins {
                match &join.join_operator {
                    ast::JoinOperator::Left(_)
                    | ast::JoinOperator::LeftOuter(_)
                    | ast::JoinOperator::Right(_)
                    | ast::JoinOperator::RightOuter(_)
                    | ast::JoinOperator::FullOuter(_) => return true,
                    _ => {}
                }
            }
        }
        false
    }

    pub(super) async fn create_implicit_unique_indexes(&self, table_def: &crate::catalog::TableDef) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        let mut seen_columns: HashSet<String> = HashSet::new();
        for constraint in &table_def.constraints {
            let (columns, index_name) = match constraint {
                TableConstraint::PrimaryKey { columns } => {
                    (columns, format!("{}_pkey", table_def.name))
                }
                TableConstraint::Unique { name, columns } => {
                    let inferred = if columns.len() == 1 {
                        format!("{}_{}_key", table_def.name, columns[0])
                    } else {
                        format!("{}_{}_key", table_def.name, columns.join("_"))
                    };
                    (columns, name.clone().unwrap_or(inferred))
                }
                _ => continue,
            };

            // Storage engine currently supports one-column B-tree definitions in this path.
            if columns.len() != 1 {
                continue;
            }
            let column_name = columns[0].clone();
            if !seen_columns.insert(column_name.clone()) {
                continue;
            }
            let Some(col_idx) = table_def.column_index(&column_name) else {
                continue;
            };

            self.storage
                .create_index(&table_def.name, &index_name, col_idx)
                .await?;
            self.btree_indexes.write().insert(
                (table_def.name.clone(), column_name.clone()),
                index_name.clone(),
            );

            let index_def = crate::catalog::IndexDef {
                name: index_name,
                table_name: table_def.name.clone(),
                columns: vec![column_name],
                unique: true,
                index_type: crate::catalog::IndexType::BTree,
                options: HashMap::new(),
            };
            // Best-effort registration: if it already exists, continue.
            if let Err(e) = self.catalog.create_index(index_def).await
                && !matches!(e, crate::catalog::CatalogError::IndexExists(_)) {
                    return Err(e.into());
                }
        }
        Ok(())
    }

    pub(super) fn partition_where_for_ast_pushdown(
        &self,
        from: &[ast::TableWithJoins],
        where_expr: &Expr,
    ) -> (HashMap<String, Vec<Expr>>, Option<Expr>) {
        let relation_names = Self::collect_from_relation_names(from);
        let has_outer_join = Self::from_has_outer_join(from);
        let mut by_relation: HashMap<String, Vec<Expr>> = HashMap::new();
        let mut remaining: Vec<Expr> = Vec::new();
        for pred in planner::split_conjunction(where_expr).into_iter().cloned() {
            let mut refs = HashSet::new();
            Self::collect_expr_table_refs(&pred, &mut refs);
            if refs.len() == 1
                && let Some(name) = refs.iter().next()
                    && relation_names.contains(name) {
                        by_relation.entry(name.clone()).or_default().push(pred.clone());
                        // For outer joins, keep pushed predicates as post-join filters too.
                        // This preserves NULL-extension semantics while still enabling
                        // relation-level pushdown.
                        if has_outer_join {
                            remaining.push(pred);
                        }
                        continue;
                    }
            remaining.push(pred);
        }
        (by_relation, Self::combine_predicates(remaining))
    }

    pub(super) fn apply_pushdown_for_factor(
        &self,
        factor: &TableFactor,
        rows: Vec<Row>,
        col_meta: &[ColMeta],
        pushdown: Option<&HashMap<String, Vec<Expr>>>,
    ) -> Vec<Row> {
        let Some(pushdown_map) = pushdown else { return rows; };
        let factor_names = Self::table_factor_names(factor);
        if factor_names.is_empty() {
            return rows;
        }
        let mut preds = Vec::new();
        for name in &factor_names {
            if let Some(items) = pushdown_map.get(name) {
                preds.extend(items.clone());
            }
        }
        let Some(expr) = Self::combine_predicates(preds) else { return rows; };
        // Parallel Rayon filter for large sets, serial for small
        self.parallel_filter(rows, &expr, col_meta)
    }

    pub(super) async fn try_execute_index_join_for_factor(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        join: &ast::Join,
        cte_tables: &CteTableMap,
        pushdown: Option<&HashMap<String, Vec<Expr>>>,
    ) -> Result<Option<(Vec<ColMeta>, Vec<Row>)>, ExecError> {
        let (condition, join_type) = match &join.join_operator {
            ast::JoinOperator::Join(c) | ast::JoinOperator::Inner(c) => (c, JoinType::Inner),
            ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => (c, JoinType::Left),
            _ => return Ok(None),
        };

        let on_expr = match condition {
            ast::JoinConstraint::On(expr) => expr.clone(),
            _ => return Ok(None),
        };

        let (table_name, label) = match &join.relation {
            TableFactor::Table { name, alias, args: None, .. } => {
                let table_name = name.to_string();
                let label = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());
                (table_name, label)
            }
            _ => return Ok(None),
        };

        // CTEs/views/virtual factors are handled by the generic path.
        if cte_tables.contains_key(&table_name) {
            return Ok(None);
        }

        self.metrics.index_join_attempts.inc();
        let right_pushdown = Self::factor_pushdown_expr(&join.relation, pushdown);

        let table_def = match self.get_table(&table_name).await {
            Ok(t) => t,
            Err(_) => {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        };

        let right_meta: Vec<ColMeta> = table_def
            .columns
            .iter()
            .map(|c| ColMeta {
                table: Some(label.clone()),
                name: c.name.clone(),
                dtype: c.data_type.clone(),
            })
            .collect();

        let (left_keys, right_keys, residual_on) = match Self::extract_equijoin_keys(&on_expr, left_meta, &right_meta) {
            Some(keys) => keys,
            None => {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        };

        let probe_pair = {
            let indexes = self.btree_indexes.read();
            let mut chosen: Option<(usize, usize, String)> = None;
            for (li, ri) in left_keys.iter().zip(right_keys.iter()) {
                if let Some(right_col) = right_meta.get(*ri) {
                    let key = (table_name.clone(), right_col.name.clone());
                    if let Some(index_name) = indexes.get(&key) {
                        chosen = Some((*li, *ri, index_name.clone()));
                        break;
                    }
                }
            }
            chosen
        };
        let Some((left_probe_idx, right_probe_idx, right_index_name)) = probe_pair else {
            self.metrics.index_join_skipped.inc();
            return Ok(None);
        };
        let right_probe_col = right_meta[right_probe_idx].name.clone();

        let stats = self
            .stats_store
            .get_or_default(&table_name, &self.catalog)
            .await;
        let right_row_est = stats.row_count.max(1);

        // Determine uniqueness BEFORE the probe-limit gate.
        // Unique (PK / UNIQUE constraint) indexes return at most 1 row per probe,
        // so fan-out is guaranteed to be 1:1.  Cost is O(n * log m), which always
        // beats hash join O(n + m) and makes the cardinality-based gates below
        // misleading when stats are empty (right_row_est defaults to 1).
        let is_unique = self
            .catalog
            .get_indexes(&table_name)
            .await
            .iter()
            .any(|idx| idx.name == right_index_name && idx.unique);

        // Adaptive index-join gating (skipped entirely for unique indexes):
        // - allow more probes for larger right tables
        // - but skip if probe fan-out is estimated to exceed a full right scan
        if !is_unique {
            let dynamic_probe_limit =
                (((right_row_est as f64).sqrt() * 2.0).round() as usize).clamp(16, 256);
            if left_rows.len() > dynamic_probe_limit {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        }

        let mut expected_rows_per_probe =
            (right_row_est as f64 * stats.equality_selectivity(Some(&right_probe_col))).max(1.0);
        if is_unique {
            expected_rows_per_probe = 1.0;
        }
        // For unique indexes the probe work is always O(n * 1) -- skip the
        // estimated-work comparison that would incorrectly fire when stats are empty.
        if !is_unique && right_pushdown.is_none() {
            let estimated_probe_work = left_rows.len() as f64 * expected_rows_per_probe;
            let estimated_full_scan_work = right_row_est as f64;
            if estimated_probe_work > estimated_full_scan_work {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        }

        // Pre-flight: verify this storage backend actually supports index lookup.
        // MemoryEngine (used in tests) returns Ok(None) -- bail to hash join in that case.
        let first_probe_val = left_rows
            .iter()
            .flat_map(|r| r.get(left_probe_idx))
            .find(|v| !matches!(v, Value::Null));
        if let Some(probe_val) = first_probe_val
            && matches!(
                self.storage.index_lookup_sync(&table_name, &right_index_name, probe_val),
                Ok(None)
            ) {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }

        let combined_meta: Vec<ColMeta> = left_meta
            .iter()
            .chain(right_meta.iter())
            .cloned()
            .collect();
        let right_nulls: Row = right_meta.iter().map(|_| Value::Null).collect();
        let mut result_rows = Vec::new();

        for left_row in left_rows {
            let Some(lookup_val) = left_row.get(left_probe_idx) else {
                continue;
            };
            if matches!(lookup_val, Value::Null) {
                if join_type == JoinType::Left {
                    let combined: Row = left_row.iter().chain(right_nulls.iter()).cloned().collect();
                    result_rows.push(combined);
                }
                continue;
            }

            let mut matched = false;
            let probed_rows = match self
                .storage
                .index_lookup_sync(&table_name, &right_index_name, lookup_val)
            {
                Ok(Some(rows)) => rows,
                _ => Vec::new(),
            };
            self.metrics.rows_scanned.inc_by(probed_rows.len() as u64);

            for right_row in probed_rows {
                if let Some(ref pred) = right_pushdown
                    && !self.eval_where(pred, &right_row, &right_meta).unwrap_or(false) {
                        continue;
                    }

                // Validate all equi-join keys (the index probe may cover only one key).
                let mut keys_match = true;
                for (li, ri) in left_keys.iter().zip(right_keys.iter()) {
                    let lv = left_row.get(*li).unwrap_or(&Value::Null);
                    let rv = right_row.get(*ri).unwrap_or(&Value::Null);
                    if matches!(lv, Value::Null) || matches!(rv, Value::Null) || lv != rv {
                        keys_match = false;
                        break;
                    }
                }
                if !keys_match {
                    continue;
                }

                let combined: Row = left_row.iter().chain(right_row.iter()).cloned().collect();
                let residual_ok = if let Some(ref residual) = residual_on {
                    self.eval_where(residual, &combined, &combined_meta).unwrap_or(false)
                } else {
                    true
                };
                if residual_ok {
                    result_rows.push(combined);
                    matched = true;
                }
            }

            if !matched && join_type == JoinType::Left {
                let combined: Row = left_row.iter().chain(right_nulls.iter()).cloned().collect();
                result_rows.push(combined);
            }
        }

        self.metrics.index_join_used.inc();
        Ok(Some((combined_meta, result_rows)))
    }

    pub(super) fn factor_pushdown_expr(
        factor: &TableFactor,
        pushdown: Option<&HashMap<String, Vec<Expr>>>,
    ) -> Option<Expr> {
        let pushdown_map = pushdown?;
        let factor_names = Self::table_factor_names(factor);
        if factor_names.is_empty() {
            return None;
        }
        let mut preds = Vec::new();
        for name in &factor_names {
            if let Some(items) = pushdown_map.get(name) {
                preds.extend(items.clone());
            }
        }
        Self::combine_predicates(preds)
    }

    /// Check if a plan tree contains only nodes we can execute correctly.
    pub(super) fn plan_is_executable(plan: &planner::PlanNode) -> bool {
        match plan {
            planner::PlanNode::SeqScan { .. } | planner::PlanNode::IndexScan { .. } => true,
            planner::PlanNode::Filter { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::Sort { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::Limit { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::Project { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::Aggregate { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::HashAggregate { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::NestedLoopJoin { left, right, join_type, .. } => {
                matches!(join_type, planner::JoinPlanType::Inner | planner::JoinPlanType::Cross)
                    && Self::plan_is_executable(left)
                    && Self::plan_is_executable(right)
            }
            planner::PlanNode::HashJoin { left, right, join_type, .. } => {
                matches!(join_type, planner::JoinPlanType::Inner)
                    && Self::plan_is_executable(left)
                    && Self::plan_is_executable(right)
            }
        }
    }

    /// Parse a SQL expression string back into an AST Expr.
    pub(super) fn parse_expr_string(s: &str) -> Result<Expr, ExecError> {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let dialect = PostgreSqlDialect {};
        // Parse as "SELECT <expr>" and extract the expression
        let sql = format!("SELECT {s}");
        let stmts = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| ExecError::Unsupported(format!("Failed to parse plan expression '{s}': {e}")))?;
        if let Some(Statement::Query(q)) = stmts.into_iter().next()
            && let SetExpr::Select(sel) = *q.body
                && let Some(SelectItem::UnnamedExpr(expr)) = sel.projection.into_iter().next() {
                    return Ok(expr);
                }
        Err(ExecError::Unsupported(format!("Could not parse expression: {s}")))
    }

    /// Execute a plan node tree, returning column metadata and result rows.
    /// This recursively walks the PlanNode tree produced by the planner.
    pub(super) fn execute_plan_node<'a>(
        &'a self,
        plan: &'a planner::PlanNode,
        cte_tables: &'a CteTableMap,
    ) -> BoxedExecFuture<'a> {
        Box::pin(async move {
            match plan {
                planner::PlanNode::SeqScan { table, filter, filter_expr, scan_limit, projection, .. } => {
                    // Check CTEs first
                    if let Some((cols, rows)) = cte_tables.get(table.as_str()) {
                        let meta = cols.clone();
                        let mut result_rows = rows.clone();
                        // Use pre-parsed expr if available, otherwise fall back to parsing
                        let resolved_expr = filter_expr.as_ref().cloned()
                            .or_else(|| filter.as_ref().and_then(|s| Self::parse_expr_string(s).ok()));
                        if let Some(expr) = resolved_expr {
                                result_rows.retain(|row| {
                                    self.eval_where_plan(&expr, row, &meta).unwrap_or(false)
                                });
                            }
                        return Ok((meta, result_rows));
                    }

                    let table_def = self.get_table(table).await?;

                    // ── Projected scan fast-path ──────────────────────────────
                    // When the planner pushed projection indices into this node
                    // and there is no filter, use `scan_projected()` for
                    // zero-copy partial deserialization (DiskEngine skips
                    // non-projected columns during page decoding).
                    if let Some(proj_indices) = projection
                        && filter.is_none()
                    {
                        let proj_meta: Vec<ColMeta> = proj_indices
                            .iter()
                            .filter_map(|&idx| {
                                table_def.columns.get(idx).map(|c| ColMeta {
                                    table: Some(table.clone()),
                                    name: c.name.clone(),
                                    dtype: c.data_type.clone(),
                                })
                            })
                            .collect();
                        let storage = self.storage_for(table);
                        let rows = storage.scan_projected(table, proj_indices).await?;
                        self.metrics.rows_scanned.inc_by(rows.len() as u64);
                        return Ok((proj_meta, rows));
                        // If there IS a filter, fall through to the full scan
                        // path — the filter may reference columns outside the
                        // projection.
                    }

                    let meta: Vec<ColMeta> = table_def.columns.iter().map(|c| ColMeta {
                        table: Some(table.clone()),
                        name: c.name.clone(),
                        dtype: c.data_type.clone(),
                    }).collect();
                    let storage = self.storage_for(table);
                    let mut rows = if let Some(lim) = scan_limit {
                        storage.scan_limit(table, *lim).await?
                    } else {
                        storage.scan(table).await?
                    };
                    self.metrics.rows_scanned.inc_by(rows.len() as u64);

                    // Use pre-parsed expr if available, otherwise fall back to parsing
                    let resolved_expr = filter_expr.as_ref().cloned()
                        .or_else(|| filter.as_ref().and_then(|s| Self::parse_expr_string(s).ok()));
                    if let Some(expr) = resolved_expr {
                            // Try SIMD-accelerated filter first (simple col op literal predicates)
                            if let Some(indices) = self.try_simd_filter(&expr, &rows, &meta) {
                                rows = indices.into_iter().map(|i| rows[i].clone()).collect();
                            } else if rows.len() > 10_000 {
                                use rayon::prelude::*;
                                rows = rows.into_par_iter()
                                    .filter(|row| self.eval_where_plan(&expr, row, &meta).unwrap_or(false))
                                    .collect();
                            } else {
                                rows.retain(|row| {
                                    self.eval_where_plan(&expr, row, &meta).unwrap_or(false)
                                });
                            }
                        }
                    Ok((meta, rows))
                }

                planner::PlanNode::IndexScan { table, index_name, lookup_key, lookup_key_expr,
                    range_lo, range_lo_expr, range_hi, range_hi_expr,
                    range_predicate, range_predicate_expr, .. } => {
                    let table_def = self.get_table(table).await?;
                    let meta: Vec<ColMeta> = table_def.columns.iter().map(|c| ColMeta {
                        table: Some(table.clone()),
                        name: c.name.clone(),
                        dtype: c.data_type.clone(),
                    }).collect();

                    // ── Range index scan ──────────────────────────────────────
                    // Use pre-parsed exprs if available, otherwise fall back to parsing
                    let lo_resolved = range_lo_expr.as_ref().cloned()
                        .or_else(|| range_lo.as_ref().and_then(|s| Self::parse_expr_string(s).ok()));
                    let hi_resolved = range_hi_expr.as_ref().cloned()
                        .or_else(|| range_hi.as_ref().and_then(|s| Self::parse_expr_string(s).ok()));

                    if let (Some(lo_expr), Some(hi_expr)) = (lo_resolved, hi_resolved)
                        && let (Ok(lo_raw), Ok(hi_raw)) = (
                            self.eval_const_expr(&lo_expr),
                            self.eval_const_expr(&hi_expr),
                        )
                    {
                        // Coerce range bounds to indexed column type (Int64→Int32 for INT columns)
                        let (lo_val, hi_val) = Self::coerce_index_bounds(
                            lo_raw, hi_raw, table, index_name, &table_def, &self.catalog,
                        );
                        if let Ok(Some(mut rows)) = self.storage
                            .index_lookup_range(table, index_name, &lo_val, &hi_val)
                            .await
                        {
                            // Post-filter: enforce strict bounds and residual predicates.
                            let rp_resolved = range_predicate_expr.as_ref().cloned()
                                .or_else(|| range_predicate.as_ref().and_then(|s| Self::parse_expr_string(s).ok()));
                            if let Some(pred_expr) = rp_resolved {
                                rows.retain(|row| {
                                    self.eval_where_plan(&pred_expr, row, &meta)
                                        .unwrap_or(false)
                                });
                            }
                            self.metrics.rows_scanned.inc_by(rows.len() as u64);
                            return Ok((meta, rows));
                        }
                    }

                    // ── Equality index scan ───────────────────────────────────
                    // The lookup_key_expr is the full predicate (e.g. `id = 500`).
                    // We need to extract the literal value from the equality for the index lookup.
                    let key_resolved = lookup_key_expr.as_ref().cloned()
                        .or_else(|| lookup_key.as_ref().and_then(|s| Self::parse_expr_string(s).ok()));
                    if let Some(ref key_expr) = key_resolved {
                        // Try eval_const_expr first (works for simple literal expressions)
                        let key_val = self.eval_const_expr(key_expr).ok()
                            .or_else(|| {
                                // Extract the literal side of `col = literal` or `literal = col`
                                Self::extract_equality_value(key_expr)
                            });
                        if let Some(val) = key_val {
                            // Coerce to the indexed column's type (e.g. Int64→Int32 for INT columns)
                            let coerced = Self::coerce_index_value(
                                val, table, index_name, &table_def, &self.catalog,
                            );
                            if let Ok(Some(rows)) = self.storage.index_lookup(table, index_name, &coerced).await {
                                self.metrics.rows_scanned.inc_by(rows.len() as u64);
                                return Ok((meta, rows));
                            }
                        }
                    }

                    // Index lookup failed -- return error to trigger AST fallback
                    // (doing an unfiltered seq scan here would lose the WHERE clause)
                    Err(ExecError::Unsupported("IndexScan lookup failed, falling back to AST".into()))
                }

                planner::PlanNode::Filter { input, predicate, predicate_expr, .. } => {
                    let (meta, mut rows) = self.execute_plan_node(input, cte_tables).await?;
                    // Use pre-parsed expr if available, otherwise fall back to parsing
                    let resolved_expr = predicate_expr.as_ref().cloned()
                        .or_else(|| Self::parse_expr_string(predicate).ok());
                    if let Some(expr) = resolved_expr {
                        // Try SIMD-accelerated filter first (simple col op literal predicates)
                        if let Some(indices) = self.try_simd_filter(&expr, &rows, &meta) {
                            rows = indices.into_iter().map(|i| rows[i].clone()).collect();
                        } else if rows.len() > 10_000 {
                            // Parallel filter for large row sets — linear speedup on multi-core
                            use rayon::prelude::*;
                            rows = rows.into_par_iter()
                                .filter(|row| self.eval_where_plan(&expr, row, &meta).unwrap_or(false))
                                .collect();
                        } else {
                            rows.retain(|row| {
                                self.eval_where_plan(&expr, row, &meta).unwrap_or(false)
                            });
                        }
                    }
                    Ok((meta, rows))
                }

                planner::PlanNode::Sort { input, keys, .. } => {
                    let (meta, mut rows) = self.execute_plan_node(input, cte_tables).await?;
                    // Parse sort keys: "col_name ASC|DESC [NULLS FIRST|LAST]"
                    for key_str in keys.iter().rev() {
                        // Strip optional NULLS FIRST/LAST suffix
                        let (base, nulls_first) = if let Some(_s) = key_str.to_uppercase().strip_suffix("NULLS FIRST") {
                            (key_str[..key_str.len() - "NULLS FIRST".len()].trim(), Some(true))
                        } else if let Some(_s) = key_str.to_uppercase().strip_suffix("NULLS LAST") {
                            (key_str[..key_str.len() - "NULLS LAST".len()].trim(), Some(false))
                        } else {
                            (key_str.as_str(), None)
                        };
                        // Parse remaining "col_name ASC" or "col_name DESC"
                        let parts: Vec<&str> = base.rsplitn(2, ' ').collect();
                        let (col_name, desc) = if parts.len() == 2 {
                            (parts[1].trim(), parts[0].eq_ignore_ascii_case("DESC"))
                        } else {
                            (base, false)
                        };
                        // Default NULLS placement: ASC→NULLS LAST, DESC→NULLS FIRST (SQL standard)
                        let nulls_first = nulls_first.unwrap_or(desc);
                        if let Some(idx) = Self::resolve_plan_col_idx(&meta, col_name) {
                            rows.sort_by(|a, b| {
                                let a_null = a[idx] == Value::Null;
                                let b_null = b[idx] == Value::Null;
                                match (a_null, b_null) {
                                    (true, true) => std::cmp::Ordering::Equal,
                                    (true, false) => if nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
                                    (false, true) => if nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
                                    (false, false) => {
                                        let cmp = a[idx].cmp(&b[idx]);
                                        if desc { cmp.reverse() } else { cmp }
                                    }
                                }
                            });
                        }
                    }
                    Ok((meta, rows))
                }

                planner::PlanNode::Limit { input, limit, offset, .. } => {
                    let (meta, mut rows) = self.execute_plan_node(input, cte_tables).await?;
                    if let Some(off) = offset {
                        if *off < rows.len() {
                            rows = rows.split_off(*off);
                        } else {
                            rows.clear();
                        }
                    }
                    if let Some(lim) = limit {
                        rows.truncate(*lim);
                    }
                    Ok((meta, rows))
                }

                planner::PlanNode::Project { input, columns, .. } => {
                    let (meta, rows) = self.execute_plan_node(input, cte_tables).await?;
                    // If projection is just *, return as-is
                    if columns.len() == 1 && columns[0] == "*" {
                        return Ok((meta, rows));
                    }
                    // Map column names/expressions to indices or parsed expressions
                    enum ProjItem { ColIdx(usize), Expr(Box<Expr>) }
                    let mut proj_meta = Vec::new();
                    let mut proj_items: Vec<ProjItem> = Vec::new();
                    for col_spec in columns {
                        // Handle "expr AS alias"
                        let (col_name, alias) = if let Some(pos) = col_spec.to_uppercase().rfind(" AS ") {
                            (&col_spec[..pos], Some(col_spec[pos+4..].trim()))
                        } else {
                            (col_spec.as_str(), None)
                        };
                        if let Some(idx) = Self::resolve_plan_col_idx(&meta, col_name) {
                            proj_meta.push(ColMeta {
                                table: meta[idx].table.clone(),
                                name: alias.unwrap_or(&meta[idx].name).to_string(),
                                dtype: meta[idx].dtype.clone(),
                            });
                            proj_items.push(ProjItem::ColIdx(idx));
                        } else if let Ok(expr) = Self::parse_expr_string(col_name) {
                            // Expression column — evaluate per-row via eval_expr_plan
                            proj_meta.push(ColMeta {
                                table: None,
                                name: alias.unwrap_or(col_name).to_string(),
                                dtype: DataType::Text, // type inferred at runtime
                            });
                            proj_items.push(ProjItem::Expr(Box::new(expr)));
                        } else {
                            proj_meta.push(ColMeta {
                                table: None,
                                name: alias.unwrap_or(col_name).to_string(),
                                dtype: DataType::Text,
                            });
                            proj_items.push(ProjItem::ColIdx(usize::MAX)); // sentinel → Null
                        }
                    }
                    let projected_rows: Vec<Row> = rows.iter().map(|row| {
                        proj_items.iter().map(|item| {
                            match item {
                                ProjItem::ColIdx(idx) => row.get(*idx).cloned().unwrap_or(Value::Null),
                                ProjItem::Expr(expr) => self.eval_expr_plan(expr, row, &meta).unwrap_or(Value::Null),
                            }
                        }).collect()
                    }).collect();
                    Ok((proj_meta, projected_rows))
                }

                planner::PlanNode::NestedLoopJoin { left, right, condition, condition_expr, .. } => {
                    let (left_meta, left_rows) = self.execute_plan_node(left, cte_tables).await?;
                    let (right_meta, right_rows) = self.execute_plan_node(right, cte_tables).await?;

                    let mut combined_meta = left_meta.clone();
                    combined_meta.extend(right_meta.clone());

                    let mut result_rows = Vec::new();
                    // Use pre-parsed expr if available, otherwise fall back to parsing
                    let cond_expr = condition_expr.as_ref().cloned()
                        .or_else(|| condition.as_ref().and_then(|s| Self::parse_expr_string(s).ok()));

                    for lrow in &left_rows {
                        for rrow in &right_rows {
                            let mut combined = lrow.clone();
                            combined.extend(rrow.clone());
                            if let Some(ref expr) = cond_expr {
                                if self.eval_where_plan(expr, &combined, &combined_meta).unwrap_or(false) {
                                    result_rows.push(combined);
                                }
                            } else {
                                result_rows.push(combined); // cross join
                            }
                        }
                    }
                    Ok((combined_meta, result_rows))
                }

                planner::PlanNode::HashJoin { left, right, hash_keys, .. } => {
                    let (left_meta, left_rows) = self.execute_plan_node(left, cte_tables).await?;
                    let (right_meta, right_rows) = self.execute_plan_node(right, cte_tables).await?;

                    let mut combined_meta = left_meta.clone();
                    combined_meta.extend(right_meta.clone());
                    // Parse hash key: "left_col = right_col"
                    if let Some(key_str) = hash_keys.first() {
                        let parts: Vec<&str> = key_str.split('=').map(|s| s.trim()).collect();
                        if parts.len() == 2 {
                            let lhs = parts[0];
                            let rhs = parts[1];
                            let direct = (
                                Self::resolve_plan_col_idx_for_join_side(&left_meta, lhs),
                                Self::resolve_plan_col_idx_for_join_side(&right_meta, rhs),
                            );
                            let swapped = (
                                Self::resolve_plan_col_idx_for_join_side(&left_meta, rhs),
                                Self::resolve_plan_col_idx_for_join_side(&right_meta, lhs),
                            );
                            let (left_idx, right_idx) = if direct.0.is_some() && direct.1.is_some() {
                                direct
                            } else if swapped.0.is_some() && swapped.1.is_some() {
                                swapped
                            } else {
                                (None, None)
                            };

                            if let (Some(li), Some(ri)) = (left_idx, right_idx) {
                                // Build hash table on right side (skip NULL keys — NULL ≠ NULL in equi-join)
                                // Use Value directly as key (Hash+Eq implemented) — avoids format!("{:?}") allocation per row
                                let mut hash_table: HashMap<Value, Vec<&Row>> = HashMap::new();
                                for rrow in &right_rows {
                                    if rrow[ri] == Value::Null { continue; }
                                    hash_table.entry(rrow[ri].clone()).or_default().push(rrow);
                                }
                                // Probe with left side (skip NULL keys)
                                let mut result_rows = Vec::new();
                                for lrow in &left_rows {
                                    if lrow[li] == Value::Null { continue; }
                                    if let Some(matches) = hash_table.get(&lrow[li]) {
                                        for rrow in matches {
                                            let mut combined = lrow.clone();
                                            combined.extend((*rrow).clone());
                                            result_rows.push(combined);
                                        }
                                    }
                                }
                                return Ok((combined_meta, result_rows));
                            }
                        }
                    }
                    // Fallback: cross join
                    let mut result_rows = Vec::new();
                    for lrow in &left_rows {
                        for rrow in &right_rows {
                            let mut combined = lrow.clone();
                            combined.extend(rrow.clone());
                            result_rows.push(combined);
                        }
                    }
                    Ok((combined_meta, result_rows))
                }

                planner::PlanNode::Aggregate { input, aggregates, .. } => {
                    // Fast path: simple aggregate over bare SeqScan — use storage
                    // engine's O(1) count / O(n) sum instead of loading all rows.
                    if let planner::PlanNode::SeqScan { table, filter, .. } = input.as_ref()
                        && filter.is_none()
                    {
                        let tbl_storage = self.storage_for(table);
                        let col_info = self.table_columns.read().get(table.as_str()).cloned();
                        if let Some(ci) = &col_info
                            && tbl_storage.fast_count_all(table).is_some()
                        {
                            let mut result_meta = Vec::new();
                            let mut result_values = Vec::new();
                            let mut all_handled = true;
                            for agg_str in aggregates {
                                let (func_name, col_name) = parse_agg_spec(agg_str);
                                match func_name.as_str() {
                                    "COUNT" if col_name == "*" => {
                                        let n = tbl_storage.fast_count_all(table).unwrap_or(0) as i64;
                                        result_meta.push(ColMeta { table: None, name: agg_str.clone(), dtype: DataType::Int64 });
                                        result_values.push(Value::Int64(n));
                                    }
                                    "SUM" | "AVG" | "MIN" | "MAX" => {
                                        let resolve = |name: &str| -> Option<usize> {
                                            ci.iter().position(|(c, _)| c.eq_ignore_ascii_case(name))
                                        };
                                        if let Some(col_idx) = resolve(&col_name) {
                                            match func_name.as_str() {
                                                "SUM" => match tbl_storage.fast_sum_f64(table, col_idx) {
                                                    Some((sum, cnt)) => {
                                                        let v = if cnt == 0 { Value::Null } else {
                                                            let is_int = ci.get(col_idx).is_some_and(|(_, dt)| matches!(dt, DataType::Int32 | DataType::Int64));
                                                            if is_int { Value::Int64(sum as i64) } else { Value::Float64(sum) }
                                                        };
                                                        result_meta.push(ColMeta { table: None, name: agg_str.clone(), dtype: if cnt == 0 { DataType::Float64 } else { DataType::Int64 } });
                                                        result_values.push(v);
                                                    }
                                                    None => { all_handled = false; break; }
                                                },
                                                "AVG" => match tbl_storage.fast_sum_f64(table, col_idx) {
                                                    Some((sum, cnt)) => {
                                                        let v = if cnt == 0 { Value::Null } else { Value::Float64(sum / cnt as f64) };
                                                        result_meta.push(ColMeta { table: None, name: agg_str.clone(), dtype: DataType::Float64 });
                                                        result_values.push(v);
                                                    }
                                                    None => { all_handled = false; break; }
                                                },
                                                "MIN" => match tbl_storage.fast_min_f64(table, col_idx) {
                                                    Some(v) => {
                                                        let col_dt = ci.get(col_idx).map(|(_, dt)| dt);
                                                        let (dtype, val) = match col_dt {
                                                            Some(DataType::Int32) => (DataType::Int32, Value::Int32(v as i32)),
                                                            Some(DataType::Int64) => (DataType::Int64, Value::Int64(v as i64)),
                                                            _ => (DataType::Float64, Value::Float64(v)),
                                                        };
                                                        result_meta.push(ColMeta { table: None, name: agg_str.clone(), dtype });
                                                        result_values.push(val);
                                                    }
                                                    None => { all_handled = false; break; }
                                                },
                                                "MAX" => match tbl_storage.fast_max_f64(table, col_idx) {
                                                    Some(v) => {
                                                        let col_dt = ci.get(col_idx).map(|(_, dt)| dt);
                                                        let (dtype, val) = match col_dt {
                                                            Some(DataType::Int32) => (DataType::Int32, Value::Int32(v as i32)),
                                                            Some(DataType::Int64) => (DataType::Int64, Value::Int64(v as i64)),
                                                            _ => (DataType::Float64, Value::Float64(v)),
                                                        };
                                                        result_meta.push(ColMeta { table: None, name: agg_str.clone(), dtype });
                                                        result_values.push(val);
                                                    }
                                                    None => { all_handled = false; break; }
                                                },
                                                _ => { all_handled = false; break; }
                                            }
                                        } else {
                                            all_handled = false; break;
                                        }
                                    }
                                    _ => { all_handled = false; break; }
                                }
                            }
                            if all_handled {
                                return Ok((result_meta, vec![result_values]));
                            }
                        }
                    }

                    // Fallback: compute over all input rows
                    let (meta, rows) = self.execute_plan_node(input, cte_tables).await?;
                    let mut result_meta = Vec::new();
                    let mut result_values = Vec::new();
                    for agg_str in aggregates {
                        let (func_name, col_name) = parse_agg_spec(agg_str);
                        let col_idx = if col_name == "*" {
                            None
                        } else {
                            Self::resolve_plan_col_idx(&meta, &col_name)
                        };
                        // Try SIMD fast-path for SUM/MIN/MAX on numeric columns
                        let val = col_idx
                            .and_then(|ci| simd_aggregate(&func_name, ci, &meta, &rows))
                            .unwrap_or_else(|| compute_aggregate(&func_name, col_idx, &rows));
                        result_meta.push(ColMeta {
                            table: None,
                            name: agg_str.clone(),
                            dtype: match &val { Value::Int64(_) => DataType::Int64, Value::Float64(_) => DataType::Float64, _ => DataType::Text },
                        });
                        result_values.push(val);
                    }
                    Ok((result_meta, vec![result_values]))
                }

                planner::PlanNode::HashAggregate { input, group_keys, aggregates, .. } => {
                    // Fast path: single-key GROUP BY over a bare SeqScan — use storage
                    // engine's O(n) columnar aggregate instead of loading all rows first.
                    if let planner::PlanNode::SeqScan { table, filter, .. } = input.as_ref()
                        && filter.is_none()
                        && group_keys.len() == 1
                    {
                        let col_info = self.table_columns.read().get(table.as_str()).cloned();
                        if let Some(ci) = &col_info {
                            let resolve = |name: &str| -> Option<usize> {
                                ci.iter().position(|(c, _)| c.eq_ignore_ascii_case(name))
                            };
                            let key_idx = resolve(&group_keys[0]);
                            // Find the value column for SUM/AVG if present
                            let val_idx = aggregates.iter().find_map(|a| {
                                let (fname, col) = parse_agg_spec(a);
                                if (fname == "SUM" || fname == "AVG") && col != "*" {
                                    resolve(&col)
                                } else {
                                    None
                                }
                            });
                            if let Some(ki) = key_idx {
                                let tbl_storage = self.storage_for(table);
                                if let Some(groups) = tbl_storage.fast_group_by(table, ki, val_idx) {
                                    // Check if the value column is integer-typed for correct SUM return type
                                    let val_is_int = val_idx
                                        .and_then(|vi| ci.get(vi))
                                        .is_some_and(|(_, dt)| matches!(dt, DataType::Int32 | DataType::Int64));
                                    // Build result meta
                                    let mut result_meta = Vec::new();
                                    let key_dt = ci.get(ki).map(|(_, dt)| dt.clone()).unwrap_or(DataType::Text);
                                    result_meta.push(ColMeta { table: Some(table.clone()), name: group_keys[0].clone(), dtype: key_dt });
                                    for agg_str in aggregates {
                                        let (func_name, _) = parse_agg_spec(agg_str);
                                        let dtype = match func_name.as_str() {
                                            "COUNT" => DataType::Int64,
                                            "SUM" if val_is_int => DataType::Int64,
                                            _ => DataType::Float64,
                                        };
                                        result_meta.push(ColMeta {
                                            table: None,
                                            name: agg_str.clone(),
                                            dtype,
                                        });
                                    }
                                    // Build result rows from fast_group_by output
                                    let result_rows: Vec<Row> = groups.into_iter().map(|(key, count, avg)| {
                                        let mut row = vec![key];
                                        for agg_str in aggregates {
                                            let (func_name, _) = parse_agg_spec(agg_str);
                                            row.push(match func_name.as_str() {
                                                "COUNT" => Value::Int64(count),
                                                "SUM" if val_is_int => avg.map(|a| Value::Int64((a * count as f64) as i64)).unwrap_or(Value::Null),
                                                "SUM" => avg.map(|a| Value::Float64(a * count as f64)).unwrap_or(Value::Null),
                                                "AVG" => avg.map(Value::Float64).unwrap_or(Value::Null),
                                                _ => Value::Null,
                                            });
                                        }
                                        row
                                    }).collect();
                                    return Ok((result_meta, result_rows));
                                }
                            }
                        }
                    }

                    // Fallback: hash aggregate with hash-based grouping
                    let (meta, rows) = self.execute_plan_node(input, cte_tables).await?;
                    // Resolve group key column indices -- error if any key not found
                    let mut key_indices: Vec<usize> = Vec::with_capacity(group_keys.len());
                    for k in group_keys {
                        match Self::resolve_plan_col_idx(&meta, k) {
                            Some(idx) => key_indices.push(idx),
                            None => return Err(ExecError::Unsupported(
                                format!("HashAggregate: GROUP BY column '{k}' not found in input")
                            )),
                        }
                    }
                    // Build groups: store row indices instead of full row clones.
                    // Use Value directly as key (Hash+Eq) — avoids format!("{v:?}") String allocations.
                    let estimated_groups = (rows.len() / 5).max(16);

                    // Build result meta first (needed by both single-key and multi-key paths)
                    let mut result_meta = Vec::new();
                    for gk in group_keys {
                        if let Some(idx) = Self::resolve_plan_col_idx(&meta, gk) {
                            result_meta.push(meta[idx].clone());
                        }
                    }
                    for agg_str in aggregates {
                        let (func_name, _) = parse_agg_spec(agg_str);
                        result_meta.push(ColMeta {
                            table: None,
                            name: agg_str.clone(),
                            dtype: if func_name == "COUNT" { DataType::Int64 } else { DataType::Float64 },
                        });
                    }

                    if key_indices.len() == 1 {
                        // Single-key fast path: use Value directly (no Vec wrapper allocation)
                        let ki = key_indices[0];
                        let mut groups: HashMap<Value, Vec<usize>> = HashMap::with_capacity(estimated_groups);
                        for (idx, row) in rows.iter().enumerate() {
                            let key = row.get(ki).cloned().unwrap_or(Value::Null);
                            groups.entry(key).or_default().push(idx);
                        }
                        let mut result_rows = Vec::with_capacity(groups.len());
                        for group_indices in groups.values() {
                            let first = &rows[group_indices[0]];
                            let mut row_out: Vec<Value> = key_indices.iter().map(|&i| {
                                first.get(i).cloned().unwrap_or(Value::Null)
                            }).collect();
                            let group_rows: Vec<&Row> = group_indices.iter().map(|&i| &rows[i]).collect();
                            for agg_str in aggregates {
                                let (func_name, col_name) = parse_agg_spec(agg_str);
                                let col_idx = if col_name == "*" {
                                    None
                                } else {
                                    Self::resolve_plan_col_idx(&meta, &col_name)
                                };
                                row_out.push(compute_aggregate_refs(&func_name, col_idx, &group_rows));
                            }
                            result_rows.push(row_out);
                        }
                        return Ok((result_meta, result_rows));
                    }
                    // Multi-key path: use Vec<Value> as key
                    let mut groups: HashMap<Vec<Value>, Vec<usize>> = HashMap::with_capacity(estimated_groups);
                    for (idx, row) in rows.iter().enumerate() {
                        let key: Vec<Value> = key_indices.iter().map(|&i| {
                            row.get(i).cloned().unwrap_or(Value::Null)
                        }).collect();
                        groups.entry(key).or_default().push(idx);
                    }
                    // Compute per-group — reference rows by index instead of clones
                    let mut result_rows = Vec::new();
                    for group_indices in groups.values() {
                        let first = &rows[group_indices[0]];
                        let mut row_out: Vec<Value> = key_indices.iter().map(|&i| {
                            first.get(i).cloned().unwrap_or(Value::Null)
                        }).collect();
                        // Collect group rows for aggregate computation
                        let group_rows: Vec<&Row> = group_indices.iter().map(|&i| &rows[i]).collect();
                        for agg_str in aggregates {
                            let (func_name, col_name) = parse_agg_spec(agg_str);
                            let col_idx = if col_name == "*" {
                                None
                            } else {
                                Self::resolve_plan_col_idx(&meta, &col_name)
                            };
                            row_out.push(compute_aggregate_refs(&func_name, col_idx, &group_rows));
                        }
                        result_rows.push(row_out);
                    }
                    Ok((result_meta, result_rows))
                }
            }
        })
    }

    pub(super) fn resolve_plan_col_idx(meta: &[ColMeta], col_spec: &str) -> Option<usize> {
        if let Some(idx) = meta
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_spec.trim()))
        {
            return Some(idx);
        }

        let unqualified = col_spec
            .trim()
            .split('.')
            .next_back()
            .unwrap_or(col_spec)
            .trim_matches('"');
        meta.iter()
            .position(|c| c.name.eq_ignore_ascii_case(unqualified))
    }

    pub(super) fn resolve_plan_col_idx_for_join_side(meta: &[ColMeta], col_spec: &str) -> Option<usize> {
        let trimmed = col_spec.trim();
        if let Some(dot) = trimmed.rfind('.') {
            let table = trimmed[..dot].trim().trim_matches('"');
            let col = trimmed[dot + 1..].trim().trim_matches('"');
            if let Some(idx) = meta.iter().position(|c| {
                c.name.eq_ignore_ascii_case(col)
                    && c.table
                        .as_deref()
                        .map(|t| t.eq_ignore_ascii_case(table))
                        .unwrap_or(false)
            }) {
                return Some(idx);
            }
            return meta.iter().position(|c| c.name.eq_ignore_ascii_case(col));
        }
        Self::resolve_plan_col_idx(meta, trimmed)
    }

    /// Evaluate a WHERE expression against a row using plan-provided column metadata.
    pub(super) fn eval_where_plan(&self, expr: &Expr, row: &Row, meta: &[ColMeta]) -> Result<bool, ExecError> {
        let val = self.eval_expr_plan(expr, row, meta)?;
        match val {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            _ => Ok(false),
        }
    }

    /// Evaluate an expression against a row using plan column metadata.
    pub(super) fn eval_expr_plan(&self, expr: &Expr, row: &Row, meta: &[ColMeta]) -> Result<Value, ExecError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.as_str();
                if let Some(idx) = meta.iter().position(|c| c.name.eq_ignore_ascii_case(name)) {
                    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::CompoundIdentifier(parts) => {
                // table.column — use table qualifier for disambiguation in multi-table joins
                let col_name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                if parts.len() >= 2 {
                    let table_qual = parts[parts.len() - 2].value.as_str();
                    // First try exact table+column match
                    if let Some(idx) = meta.iter().position(|c| {
                        c.name.eq_ignore_ascii_case(col_name)
                            && c.table.as_deref().map(|t| t.eq_ignore_ascii_case(table_qual)).unwrap_or(false)
                    }) {
                        return Ok(row.get(idx).cloned().unwrap_or(Value::Null));
                    }
                }
                // Fallback: unqualified column name match
                if let Some(idx) = meta.iter().position(|c| c.name.eq_ignore_ascii_case(col_name)) {
                    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Value(v) => {
                match &v.value {
                    ast::Value::Number(n, _) => {
                        if let Ok(i) = n.parse::<i64>() {
                            Ok(Value::Int64(i))
                        } else if let Ok(f) = n.parse::<f64>() {
                            Ok(Value::Float64(f))
                        } else {
                            Ok(Value::Text(n.clone()))
                        }
                    }
                    ast::Value::SingleQuotedString(s) => Ok(Value::Text(s.clone())),
                    ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
                    ast::Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Null),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let lv = self.eval_expr_plan(left, row, meta)?;
                let rv = self.eval_expr_plan(right, row, meta)?;
                // SQL 3-valued logic: comparisons with NULL yield NULL
                if matches!(lv, Value::Null) || matches!(rv, Value::Null) {
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
                match op {
                    ast::BinaryOperator::Eq => Ok(Value::Bool(Self::plan_values_cmp(&lv, &rv) == std::cmp::Ordering::Equal)),
                    ast::BinaryOperator::NotEq => Ok(Value::Bool(Self::plan_values_cmp(&lv, &rv) != std::cmp::Ordering::Equal)),
                    ast::BinaryOperator::Lt => Ok(Value::Bool(Self::plan_values_cmp(&lv, &rv) == std::cmp::Ordering::Less)),
                    ast::BinaryOperator::LtEq => Ok(Value::Bool(matches!(Self::plan_values_cmp(&lv, &rv), std::cmp::Ordering::Less | std::cmp::Ordering::Equal))),
                    ast::BinaryOperator::Gt => Ok(Value::Bool(Self::plan_values_cmp(&lv, &rv) == std::cmp::Ordering::Greater)),
                    ast::BinaryOperator::GtEq => Ok(Value::Bool(matches!(Self::plan_values_cmp(&lv, &rv), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))),
                    ast::BinaryOperator::And => {
                        match (&lv, &rv) {
                            (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
                            (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
                            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                            _ => Ok(Value::Bool(false)),
                        }
                    }
                    ast::BinaryOperator::Or => {
                        match (&lv, &rv) {
                            (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                            (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
                            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                            _ => Ok(Value::Bool(false)),
                        }
                    }
                    ast::BinaryOperator::Plus => self.eval_arith_plan_checked(&lv, &rv, |a, b| a.checked_add(b), |a, b| Ok(a + b)),
                    ast::BinaryOperator::Minus => self.eval_arith_plan_checked(&lv, &rv, |a, b| a.checked_sub(b), |a, b| Ok(a - b)),
                    ast::BinaryOperator::Multiply => self.eval_arith_plan_checked(&lv, &rv, |a, b| a.checked_mul(b), |a, b| Ok(a * b)),
                    ast::BinaryOperator::Divide => self.eval_arith_plan_checked(&lv, &rv, |a, b| if b == 0 { None } else { a.checked_div(b) }, |a, b| if b == 0.0 { Err(ExecError::Runtime("division by zero".into())) } else { Ok(a / b) }),
                    _ => Ok(Value::Null),
                }
            }
            Expr::Between { expr, low, high, negated } => {
                let v = self.eval_expr_plan(expr, row, meta)?;
                let lo = self.eval_expr_plan(low, row, meta)?;
                let hi = self.eval_expr_plan(high, row, meta)?;
                if matches!(v, Value::Null) || matches!(lo, Value::Null) || matches!(hi, Value::Null) {
                    return Ok(Value::Null);
                }
                let in_range = matches!(Self::plan_values_cmp(&v, &lo), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    && matches!(Self::plan_values_cmp(&v, &hi), std::cmp::Ordering::Less | std::cmp::Ordering::Equal);
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Function(func) => {
                if !Self::is_supported_plan_function(func) {
                    return Ok(Value::Null);
                }
                // Aggregate functions in HAVING are materialized as output columns
                // in aggregate/hash-aggregate plan nodes.
                let col_name = func.to_string();
                if let Some(idx) = Self::resolve_plan_col_idx(meta, &col_name) {
                    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::IsNull(inner) => {
                let v = self.eval_expr_plan(inner, row, meta)?;
                Ok(Value::Bool(v == Value::Null))
            }
            Expr::IsNotNull(inner) => {
                let v = self.eval_expr_plan(inner, row, meta)?;
                Ok(Value::Bool(v != Value::Null))
            }
            Expr::Nested(inner) => self.eval_expr_plan(inner, row, meta),
            Expr::UnaryOp { op: ast::UnaryOperator::Not, expr } => {
                let v = self.eval_expr_plan(expr, row, meta)?;
                match v {
                    Value::Bool(b) => Ok(Value::Bool(!b)),
                    _ => Ok(Value::Null),
                }
            }
            _ => Ok(Value::Null),
        }
    }

    /// Helper for arithmetic in plan expression evaluation with overflow checking.
    pub(super) fn eval_arith_plan_checked(
        &self,
        lv: &Value,
        rv: &Value,
        int_op: impl Fn(i64, i64) -> Option<i64>,
        float_op: impl Fn(f64, f64) -> Result<f64, ExecError>,
    ) -> Result<Value, ExecError> {
        match (lv, rv) {
            (Value::Int32(a), Value::Int32(b)) => int_op(*a as i64, *b as i64).map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
            (Value::Int64(a), Value::Int64(b)) => int_op(*a, *b).map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
            (Value::Int32(a), Value::Int64(b)) => int_op(*a as i64, *b).map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
            (Value::Int64(a), Value::Int32(b)) => int_op(*a, *b as i64).map(Value::Int64).ok_or_else(|| ExecError::Runtime("integer out of range".into())),
            (Value::Float64(a), Value::Float64(b)) => float_op(*a, *b).map(Value::Float64),
            (Value::Float64(a), Value::Int64(b)) => float_op(*a, *b as f64).map(Value::Float64),
            (Value::Int64(a), Value::Float64(b)) => float_op(*a as f64, *b).map(Value::Float64),
            _ => Ok(Value::Null),
        }
    }

    /// Compare two Values with numeric type coercion (Int32 <-> Int64 <-> Float64).
    pub(super) fn plan_values_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            // Int32/Int64 cross-comparison
            (Value::Int32(x), Value::Int64(y)) => (*x as i64).cmp(y),
            (Value::Int64(x), Value::Int32(y)) => x.cmp(&(*y as i64)),
            // Int/Float cross-comparison
            (Value::Int32(x), Value::Float64(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float64(x), Value::Int32(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Int64(x), Value::Float64(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float64(x), Value::Int64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
            // Same-type: use Value's Ord
            _ => a.cmp(b),
        }
    }

    // ========================================================================
    // Query execution: SELECT with ORDER BY, LIMIT, OFFSET
    // ========================================================================

    pub(super) fn execute_query(&self, query: ast::Query) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecResult, ExecError>> + Send + '_>> {
        Box::pin(async move {
        // Take the plan cache key hint BEFORE resolving CTEs. If we don't,
        // nested execute_query() calls inside resolve_ctes() will steal the
        // hint meant for this (outer) query, causing the inner CTE query's
        // plan to be cached under the outer query's key — a catastrophic
        // cross-contamination that breaks ORDER BY, LIMIT, etc.
        let saved_plan_cache_key_hint = self.plan_cache_key_hint.lock().take();

        // Handle CTEs (WITH clause)
        let mut cte_tables = if let Some(ref with) = query.with {
            self.resolve_ctes(with).await?
        } else {
            HashMap::new()
        };

        // Merge in active CTEs from DML context (WITH ... INSERT/UPDATE/DELETE)
        // Only add CTEs that don't already exist (query-level CTEs take precedence)
        {
            let sess = self.current_session();
            let active = sess.active_ctes.read();
            for (name, data) in active.iter() {
                cte_tables.entry(name.clone()).or_insert_with(|| data.clone());
            }
        }

        // --- Columnar fast aggregate short-circuit (O(1) COUNT(*), fast SUM/AVG) ---
        // Only intercept simple single-row aggregates: no GROUP BY, no ORDER BY,
        // no LIMIT/OFFSET. These always return exactly one row, so post-processing
        // is unnecessary. GROUP BY queries need ORDER BY handling and go through
        // the normal path (which has its own fast aggregate call).
        if let SetExpr::Select(ref select) = *query.body
            && matches!(&select.group_by, ast::GroupByExpr::Expressions(e, _) if e.is_empty())
            && query.order_by.is_none()
            && query.limit_clause.is_none()
        {
            if let Ok(Some(result)) = self.try_columnar_fast_aggregate(select, &cte_tables) {
                return Ok(result);
            }
        }

        // --- Plan-driven execution (default ON, disable via SET plan_execution = off) ---
        // The plan-based path walks the PlanNode tree from the planner. It handles
        // SeqScan, IndexScan, Filter, Sort, Limit, Project, NestedLoopJoin, HashJoin.
        // Falls back to AST execution for unsupported features (LIKE/ILIKE, CASE, subqueries,
        // outer joins, UNION/INTERSECT, etc.) — plan_is_executable() guards the hot path.
        {
            let sess = self.current_session();
            let use_plan = sess.settings.read().get("plan_execution")
                .map(|v| !v.eq_ignore_ascii_case("off"))
                .unwrap_or(true); // default ON
            if use_plan
                && let SetExpr::Select(ref select) = *query.body
                    && Self::query_eligible_for_plan(select, &query) {
                        // Use pre-computed normalized key from parse_with_ast_cache()
                        // when available (avoids expensive query.to_string() + re-normalize).
                        let cache_key = saved_plan_cache_key_hint
                            .unwrap_or_else(|| {
                                let raw_sql = query.to_string();
                                Self::normalize_sql_for_cache(&raw_sql)
                            });
                        let cached_plan = self.plan_cache.write().get(&cache_key);

                        // On cache HIT: try to reuse the cached plan directly by
                        // transplanting the current query's WHERE clause expressions.
                        // This skips plan_query() entirely (~500-1000ns savings).
                        // Falls back to re-planning if transplanting fails.
                        let plan = if let Some(cached) = cached_plan {
                            if let Some(reused) = Self::try_reuse_plan(&cached, select) {
                                Some(reused)
                            } else {
                                // Transplant failed — re-plan with current AST
                                self.plan_query(&query).await.ok()
                            }
                        } else {
                            // Cache miss — plan from scratch, check executability
                            self.plan_query(&query).await.ok()
                                .filter(|p| Self::plan_is_executable(p))
                        };

                        if let Some(plan) = plan {
                                // Store/refresh in the plan cache under the normalized key.
                                self.plan_cache.write().insert(cache_key, plan.clone());
                                if let Ok((meta, rows)) = self.execute_plan_node(&plan, &cte_tables).await {
                                    let columns: Vec<(String, DataType)> = meta.iter()
                                        .map(|c| (c.name.clone(), c.dtype.clone()))
                                        .collect();
                                    let mut exec_result = ExecResult::Select { columns, rows };
                                    // Apply DISTINCT
                                    if let Some(ast::Distinct::Distinct) = &select.distinct
                                        && let ExecResult::Select { ref mut rows, .. } = exec_result {
                                            let mut seen: HashSet<Vec<Value>> = HashSet::new();
                                            rows.retain(|row| seen.insert(row.clone()));
                                        }
                                    return Ok(exec_result);
                                }
                            }
                    }
        }

        // --- AST-based execution fallback ---
        let order_by = query.order_by;
        let limit_clause = query.limit_clause;

        // Extract DISTINCT info from select body before consuming it
        let distinct_mode = if let SetExpr::Select(ref select) = *query.body {
            select.distinct.clone()
        } else {
            None
        };

        let result = self.execute_set_expr(*query.body, &cte_tables).await?;

        let mut exec_result = match result {
            // Aggregate queries are already fully projected -- ORDER BY works on output columns
            SelectResult::Projected(mut exec_result) => {
                let top_k = self.extract_top_k(limit_clause.as_ref());
                if let Some(ob) = order_by
                    && let ExecResult::Select {
                        ref columns,
                        ref mut rows,
                    } = exec_result
                    {
                        self.apply_order_by(rows, columns, &ob, None, None, top_k)?;
                    }
                if let Some(lc) = limit_clause
                    && let ExecResult::Select { ref mut rows, .. } = exec_result {
                        self.apply_limit_offset(rows, &lc)?;
                    }
                exec_result
            }
            // Non-aggregate queries return full rows -- ORDER BY resolves against source columns,
            // then we project
            SelectResult::Full {
                col_meta,
                mut rows,
                projection,
            } => {
                // Try vector index optimization: ORDER BY VECTOR_DISTANCE(...) LIMIT k
                let mut used_vec_index = false;
                if let Some(ref ob) = order_by
                    && let Some(optimized) = self.try_vector_index_scan(ob, &limit_clause, &rows, &col_meta) {
                        rows = optimized;
                        used_vec_index = true;
                    }

                // Fall back to standard ORDER BY + LIMIT if vector index not used
                if !used_vec_index {
                    let top_k = self.extract_top_k(limit_clause.as_ref());
                    let col_pairs: Vec<(String, DataType)> = col_meta
                        .iter()
                        .map(|c| (c.name.clone(), c.dtype.clone()))
                        .collect();

                    // Try heap-based top-K for simple column ORDER BY with small LIMIT.
                    // This path consumes the rows Vec and returns a compact K-element Vec,
                    // freeing N-K rows of capacity immediately.
                    let mut used_heap = false;
                    if let (Some(k), Some(ob)) = (top_k, &order_by)
                        && k <= 1024 && k < rows.len() {
                            // Try resolving all ORDER BY expressions to column indices
                            let exprs = match &ob.kind {
                                ast::OrderByKind::Expressions(exprs) => Some(exprs),
                                _ => None,
                            };
                            if let Some(exprs) = exprs {
                                let resolved: Vec<Option<(usize, bool, bool)>> = exprs.iter().map(|e| {
                                    let asc = e.options.asc.unwrap_or(true);
                                    let nulls_first = e.options.nulls_first.unwrap_or(!asc);
                                    self.resolve_order_by_expr(&e.expr, &col_pairs, Some(&col_meta), Some(&projection))
                                        .ok()
                                        .map(|idx| (idx, asc, nulls_first))
                                }).collect();

                                if resolved.iter().all(|r| r.is_some()) {
                                    let sort_cols: Vec<(usize, bool, bool)> = resolved.into_iter().map(|r| r.unwrap()).collect();
                                    rows = Self::top_k_heap_sort(rows, &sort_cols, k);
                                    used_heap = true;
                                }
                            }
                        }

                    if !used_heap
                        && let Some(ob) = order_by {
                            self.apply_order_by(&mut rows, &col_pairs, &ob, Some(&col_meta), Some(&projection), top_k)?;
                        }

                    // Apply LIMIT/OFFSET — needed even after heap sort since the heap
                    // returns limit+offset rows (top K) and OFFSET still needs stripping.
                    if let Some(lc) = limit_clause {
                        self.apply_limit_offset(&mut rows, &lc)?;
                    }
                }

                // Now project
                let (columns, projected) =
                    self.project_columns(&projection, &col_meta, &rows)?;
                ExecResult::Select {
                    columns,
                    rows: projected,
                }
            }
        };

        // Apply DISTINCT / DISTINCT ON
        if let Some(distinct) = distinct_mode
            && let ExecResult::Select { ref columns, ref mut rows } = exec_result {
                match distinct {
                    ast::Distinct::Distinct => {
                        // Remove duplicate rows using HashSet for O(n) dedup
                        let mut seen: HashSet<Vec<Value>> = HashSet::new();
                        rows.retain(|row| seen.insert(row.clone()));
                    }
                    ast::Distinct::On(on_exprs) => {
                        // DISTINCT ON: keep first row for each distinct value of on_exprs
                        let col_meta: Vec<ColMeta> = columns.iter().map(|(name, dtype)| ColMeta {
                            table: None,
                            name: name.clone(),
                            dtype: dtype.clone(),
                        }).collect();
                        let mut seen_keys: HashSet<Vec<Value>> = HashSet::new();
                        rows.retain(|row| {
                            let key: Vec<Value> = on_exprs.iter().filter_map(|expr| {
                                self.eval_row_expr(expr, row, &col_meta).ok()
                            }).collect();
                            seen_keys.insert(key)
                        });
                    }
                    ast::Distinct::All => {} // No deduplication
                }
            }

        Ok(exec_result)
        }) // end Box::pin
    }

    // ========================================================================
    // Set expressions: SELECT, UNION, INTERSECT, EXCEPT
    // ========================================================================

    pub(super) fn execute_set_expr<'a>(
        &'a self,
        body: SetExpr,
        cte_tables: &'a CteTableMap,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SelectResult, ExecError>> + Send + 'a>> {
        Box::pin(async move {
        match body {
            SetExpr::Select(select) => {
                self.execute_select_inner_with_ctes(&select, cte_tables).await
            }
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let left_result = self.execute_set_expr(*left, cte_tables).await?;
                let right_result = self.execute_set_expr(*right, cte_tables).await?;

                let (left_cols, left_rows) = self.select_result_to_rows(left_result)?;
                let (_right_cols, right_rows) = self.select_result_to_rows(right_result)?;

                let all = matches!(
                    set_quantifier,
                    ast::SetQuantifier::All | ast::SetQuantifier::AllByName
                );

                let combined_rows = match op {
                    ast::SetOperator::Union => {
                        if all {
                            left_rows.into_iter().chain(right_rows).collect()
                        } else {
                            let mut result: Vec<Row> = left_rows;
                            for row in right_rows {
                                if !result.contains(&row) {
                                    result.push(row);
                                }
                            }
                            // Also deduplicate the left side
                            let mut deduped = Vec::new();
                            for row in result {
                                if !deduped.contains(&row) {
                                    deduped.push(row);
                                }
                            }
                            deduped
                        }
                    }
                    ast::SetOperator::Intersect => {
                        let mut result = Vec::new();
                        for row in &left_rows {
                            if right_rows.contains(row)
                                && (all || !result.contains(row)) {
                                    result.push(row.clone());
                                }
                        }
                        result
                    }
                    ast::SetOperator::Except => {
                        let mut result = Vec::new();
                        for row in &left_rows {
                            if !right_rows.contains(row)
                                && (all || !result.contains(row)) {
                                    result.push(row.clone());
                                }
                        }
                        result
                    }
                    _ => {
                        return Err(ExecError::Unsupported("unsupported set operation".into()));
                    }
                };

                Ok(SelectResult::Projected(ExecResult::Select {
                    columns: left_cols,
                    rows: combined_rows,
                }))
            }
            SetExpr::Query(q) => {
                // Nested query: run as subquery
                let inner_result = self.execute_query(*q).await?;
                Ok(SelectResult::Projected(inner_result))
            }
            SetExpr::Values(values) => {
                // VALUES (1, 'a'), (2, 'b'), ...
                let mut result_rows = Vec::new();
                for row_exprs in &values.rows {
                    let mut row = Vec::new();
                    for expr in row_exprs {
                        row.push(self.eval_const_expr(expr)?);
                    }
                    result_rows.push(row);
                }
                let columns = if let Some(first) = result_rows.first() {
                    first
                        .iter()
                        .enumerate()
                        .map(|(i, v)| (format!("column{}", i + 1), value_type(v)))
                        .collect()
                } else {
                    Vec::new()
                };
                Ok(SelectResult::Projected(ExecResult::Select {
                    columns,
                    rows: result_rows,
                }))
            }
            // CTE + INSERT: WITH ... INSERT INTO ...
            SetExpr::Insert(Statement::Insert(insert)) => {
                // Store active CTEs so execute_query can find them when
                // executing the INSERT's source SELECT or subqueries.
                let sess = self.current_session();
                if !cte_tables.is_empty() {
                    *sess.active_ctes.write() = cte_tables.clone();
                }
                let result = self.execute_insert(insert).await;
                *sess.active_ctes.write() = HashMap::new();
                Ok(SelectResult::Projected(result?))
            }
            // CTE + UPDATE: WITH ... UPDATE ...
            SetExpr::Update(Statement::Update(update)) => {
                let sess = self.current_session();
                if !cte_tables.is_empty() {
                    *sess.active_ctes.write() = cte_tables.clone();
                }
                let result = self.execute_update(update).await;
                *sess.active_ctes.write() = HashMap::new();
                Ok(SelectResult::Projected(result?))
            }
            // CTE + DELETE: WITH ... DELETE FROM ...
            SetExpr::Delete(Statement::Delete(delete)) => {
                let sess = self.current_session();
                if !cte_tables.is_empty() {
                    *sess.active_ctes.write() = cte_tables.clone();
                }
                let result = self.execute_delete(delete).await;
                *sess.active_ctes.write() = HashMap::new();
                Ok(SelectResult::Projected(result?))
            }
            _ => Err(ExecError::Unsupported("unsupported set expression".into())),
        }
        }) // end Box::pin
    }

    pub(super) fn select_result_to_rows(
        &self,
        result: SelectResult,
    ) -> ProjectedResult {
        match result {
            SelectResult::Projected(ExecResult::Select { columns, rows }) => Ok((columns, rows)),
            SelectResult::Full {
                col_meta,
                rows,
                projection,
            } => {
                let (columns, projected) = self.project_columns(&projection, &col_meta, &rows)?;
                Ok((columns, projected))
            }
            _ => Err(ExecError::Unsupported("expected SELECT result".into())),
        }
    }

    // ========================================================================
    // CTE resolution (WITH clause)
    // ========================================================================

    pub(super) fn resolve_ctes(
        &self,
        with: &ast::With,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<CteTableMap, ExecError>> + Send + '_>> {
        let with = with.clone();
        Box::pin(async move {
            let mut cte_tables = HashMap::new();
            for cte in &with.cte_tables {
                let cte_name = cte.alias.name.value.clone();

                // Check for recursive CTE (WITH RECURSIVE ... UNION ALL)
                if with.recursive
                    && let SetExpr::SetOperation {
                        op: ast::SetOperator::Union,
                        set_quantifier,
                        ref left,
                        ref right,
                    } = *cte.query.body
                    {
                        let is_all = matches!(
                            set_quantifier,
                            ast::SetQuantifier::All | ast::SetQuantifier::AllByName
                        );
                        if is_all {
                            // Execute base case (left side of UNION ALL)
                            let base_result = self.execute_set_expr(*left.clone(), &cte_tables).await?;
                            let (base_cols, base_rows) = self.select_result_to_rows(base_result)?;
                            // Apply CTE alias column names if provided
                            let cte_col_names: Vec<String> = cte.alias.columns
                                .iter()
                                .map(|c| c.name.value.clone())
                                .collect();
                            let col_meta: Vec<ColMeta> = base_cols
                                .iter()
                                .enumerate()
                                .map(|(i, (name, dtype))| ColMeta {
                                    table: Some(cte_name.clone()),
                                    name: cte_col_names.get(i).cloned().unwrap_or_else(|| name.clone()),
                                    dtype: dtype.clone(),
                                })
                                .collect();
                            let mut all_rows = base_rows.clone();
                            let mut working_rows = base_rows;
                            const MAX_RECURSION: usize = 1000;
                            for _iteration in 0..MAX_RECURSION {
                                // Make current working set available as the CTE
                                cte_tables.insert(cte_name.clone(), (col_meta.clone(), working_rows));
                                // Execute recursive part (right side of UNION ALL)
                                let rec_result = self.execute_set_expr(*right.clone(), &cte_tables).await?;
                                let (_rec_cols, new_rows) = self.select_result_to_rows(rec_result)?;
                                if new_rows.is_empty() {
                                    break; // fixpoint reached
                                }
                                all_rows.extend(new_rows.clone());
                                working_rows = new_rows;
                            }
                            cte_tables.insert(cte_name, (col_meta, all_rows));
                            continue;
                        }
                    }

                // Non-recursive CTE
                let cte_result = self.execute_query(*cte.query.clone()).await?;
                if let ExecResult::Select { columns, rows } = cte_result {
                    let cte_col_names: Vec<String> = cte.alias.columns
                        .iter()
                        .map(|c| c.name.value.clone())
                        .collect();
                    let col_meta: Vec<ColMeta> = columns
                        .iter()
                        .enumerate()
                        .map(|(i, (name, dtype))| ColMeta {
                            table: Some(cte_name.clone()),
                            name: cte_col_names.get(i).cloned().unwrap_or_else(|| name.clone()),
                            dtype: dtype.clone(),
                        })
                        .collect();
                    cte_tables.insert(cte_name, (col_meta, rows));
                }
            }
            Ok(cte_tables)
        })
    }

    /// Extract simple equality predicates from a WHERE clause.
    /// Returns a list of (column_name, value) pairs for predicates of the form
    /// `column = literal` or `literal = column`, and the remaining expression
    /// that couldn't be pushed down (if any).
    pub(super) fn extract_index_predicates(
        &self,
        expr: &Expr,
    ) -> IndexPredicates {
        match expr {
            Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
                if let Some((col, val)) = self.try_extract_col_eq_literal(left, right) {
                    return (vec![(col, val)], vec![], None);
                }
                (vec![], vec![], Some(expr.clone()))
            }
            Expr::Between {
                expr: target,
                low,
                high,
                negated,
            } => {
                if !*negated
                    && let Some((col, lo, hi)) = self.try_extract_col_between_literals(target, low, high) {
                        return (vec![], vec![(col, lo, hi)], None);
                    }
                (vec![], vec![], Some(expr.clone()))
            }
            // Comparison operators: >, <, >=, <=
            // Convert to a range predicate with a sentinel bound on the open
            // end.  The full WHERE clause is reapplied after the index scan,
            // so inclusive sentinels are safe (the filter enforces strict vs
            // non-strict semantics).
            Expr::BinaryOp { left, op: ast::BinaryOperator::Gt, right }
            | Expr::BinaryOp { left, op: ast::BinaryOperator::Lt, right }
            | Expr::BinaryOp { left, op: ast::BinaryOperator::GtEq, right }
            | Expr::BinaryOp { left, op: ast::BinaryOperator::LtEq, right } => {
                let op = match expr {
                    Expr::BinaryOp { op, .. } => op.clone(),
                    _ => unreachable!(),
                };
                if let Some((col, val, col_on_left)) = self.try_extract_col_cmp_literal(left, right) {
                    // Normalise so we always think in terms of `col OP val`.
                    // If the column was on the right, flip the operator.
                    let effective_op = if col_on_left {
                        op
                    } else {
                        match op {
                            ast::BinaryOperator::Gt => ast::BinaryOperator::Lt,
                            ast::BinaryOperator::Lt => ast::BinaryOperator::Gt,
                            ast::BinaryOperator::GtEq => ast::BinaryOperator::LtEq,
                            ast::BinaryOperator::LtEq => ast::BinaryOperator::GtEq,
                            other => other,
                        }
                    };
                    let (lo, hi) = match effective_op {
                        // col > val  or  col >= val  ->  [val, MAX_SENTINEL]
                        ast::BinaryOperator::Gt | ast::BinaryOperator::GtEq => {
                            (val.clone(), Self::sentinel_max(&val))
                        }
                        // col < val  or  col <= val  ->  [MIN_SENTINEL, val]
                        ast::BinaryOperator::Lt | ast::BinaryOperator::LtEq => {
                            (Self::sentinel_min(&val), val.clone())
                        }
                        _ => unreachable!(),
                    };
                    return (vec![], vec![(col, lo, hi)], None);
                }
                (vec![], vec![], Some(expr.clone()))
            }
            Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
                let (mut left_eq, mut left_ranges, left_rest) = self.extract_index_predicates(left);
                let (right_eq, right_ranges, right_rest) = self.extract_index_predicates(right);
                left_eq.extend(right_eq);
                left_ranges.extend(right_ranges);
                let remaining = match (left_rest, right_rest) {
                    (None, None) => None,
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (Some(l), Some(r)) => Some(Expr::BinaryOp {
                        left: Box::new(l),
                        op: ast::BinaryOperator::And,
                        right: Box::new(r),
                    }),
                };
                (left_eq, left_ranges, remaining)
            }
            Expr::Nested(inner) => self.extract_index_predicates(inner),
            _ => (vec![], vec![], Some(expr.clone())),
        }
    }

    /// Extract `column BETWEEN low AND high` where low/high are constants.
    pub(super) fn try_extract_col_between_literals(
        &self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
    ) -> Option<(String, Value, Value)> {
        let col = self.expr_as_column_name(expr)?;
        let low_val = self.eval_const_expr(low).ok()?;
        let high_val = self.eval_const_expr(high).ok()?;
        Some((col, low_val, high_val))
    }

    pub(super) fn build_col_meta_from_cache(&self, table_name: &str, label: &str) -> Option<Vec<ColMeta>> {
        let columns = self.table_columns.read();
        let col_info = columns.get(table_name)?;
        Some(
            col_info
                .iter()
                .map(|(name, dtype)| ColMeta {
                    table: Some(label.to_string()),
                    name: name.clone(),
                    dtype: dtype.clone(),
                })
                .collect(),
        )
    }

    pub(super) fn value_as_i64(&self, value: &Value) -> Option<i64> {
        match value {
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub(super) fn try_index_lookup_range_sync(
        &self,
        table_name: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Option<Vec<Row>> {
        if let Ok(Some(rows)) = self.storage.index_lookup_range_sync(table_name, index_name, low, high) {
            return Some(rows);
        }

        // Fallback for storage backends without native range scans.
        const MAX_RANGE_LOOKUPS: i64 = 4096;
        let mut lo = self.value_as_i64(low)?;
        let mut hi = self.value_as_i64(high)?;
        if lo > hi {
            std::mem::swap(&mut lo, &mut hi);
        }
        if hi - lo > MAX_RANGE_LOOKUPS {
            return None;
        }

        let mut rows = Vec::new();
        for k in lo..=hi {
            // Probe both integer encodings to tolerate Int32/Int64 schema storage.
            let candidates = [Value::Int32(k as i32), Value::Int64(k)];
            let mut matched = false;
            for candidate in &candidates {
                if let Ok(Some(mut found)) = self.storage.index_lookup_sync(table_name, index_name, candidate)
                    && !found.is_empty() {
                        rows.append(&mut found);
                        matched = true;
                        break;
                    }
            }
            if matched {
                continue;
            }
        }
        Some(rows)
    }

    /// Try to extract a (column_name, literal_value) from an equality expression.
    pub(super) fn try_extract_col_eq_literal(&self, left: &Expr, right: &Expr) -> Option<(String, Value)> {
        // column = literal
        if let Some(col) = self.expr_as_column_name(left)
            && let Ok(val) = self.eval_const_expr(right) {
                return Some((col, val));
            }
        // literal = column
        if let Some(col) = self.expr_as_column_name(right)
            && let Ok(val) = self.eval_const_expr(left) {
                return Some((col, val));
            }
        None
    }

    /// Try to extract `(column_name, literal_value, col_is_on_left)` from a
    /// comparison expression.  Returns `col_is_on_left = true` when the column
    /// reference is the LHS operand, so the caller can flip the operator when
    /// the column appears on the right.
    pub(super) fn try_extract_col_cmp_literal(
        &self,
        left: &Expr,
        right: &Expr,
    ) -> Option<(String, Value, bool)> {
        // column OP literal
        if let Some(col) = self.expr_as_column_name(left)
            && let Ok(val) = self.eval_const_expr(right) {
                return Some((col, val, true));
            }
        // literal OP column  ->  column (flipped-OP) literal
        if let Some(col) = self.expr_as_column_name(right)
            && let Ok(val) = self.eval_const_expr(left) {
                return Some((col, val, false));
            }
        None
    }

    /// Return a sentinel value that sorts greater than or equal to any
    /// practical value of the same type.  Used as the upper bound for
    /// `>` / `>=` range predicates.
    pub(super) fn sentinel_max(val: &Value) -> Value {
        match val {
            Value::Int32(_) => Value::Int32(i32::MAX),
            Value::Int64(_) => Value::Int64(i64::MAX),
            Value::Float64(_) => Value::Float64(f64::MAX),
            Value::Text(_) => {
                Value::Text("\u{10FFFF}\u{10FFFF}\u{10FFFF}\u{10FFFF}".to_string())
            }
            Value::Date(_) => Value::Date(i32::MAX),
            Value::Timestamp(_) => Value::Timestamp(i64::MAX),
            Value::TimestampTz(_) => Value::TimestampTz(i64::MAX),
            // Fallback: Null sorts after everything (type_rank 255).
            _ => Value::Null,
        }
    }

    /// Return a sentinel value that sorts less than or equal to any
    /// practical value of the same type.  Used as the lower bound for
    /// `<` / `<=` range predicates.
    pub(super) fn sentinel_min(val: &Value) -> Value {
        match val {
            Value::Int32(_) => Value::Int32(i32::MIN),
            Value::Int64(_) => Value::Int64(i64::MIN),
            Value::Float64(_) => Value::Float64(f64::MIN),
            Value::Text(_) => Value::Text(String::new()), // empty string sorts first
            Value::Date(_) => Value::Date(i32::MIN),
            Value::Timestamp(_) => Value::Timestamp(i64::MIN),
            Value::TimestampTz(_) => Value::TimestampTz(i64::MIN),
            // Fallback: Bool(false) has the lowest type_rank (0).
            _ => Value::Bool(false),
        }
    }

    /// If the expression is a simple column reference, return the column name.
    pub(super) fn expr_as_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                // table.column -- return just the column name
                Some(parts[1].value.clone())
            }
            _ => None,
        }
    }

    /// Attempt to answer aggregate queries using the storage engine's columnar
    /// fast paths before any row scan. Returns `Ok(Some(result))` if handled,
    /// `Ok(None)` if the fast path is inapplicable (fall through to normal scan).
    ///
    /// Handles: COUNT(*), SUM(col), AVG(col) -- with or without a single-column
    /// GROUP BY. All fast-path methods are synchronous (parking_lot / blocking_read).
    pub(super) fn try_columnar_fast_aggregate(
        &self,
        select: &ast::Select,
        cte_tables: &CteTableMap,
    ) -> Result<Option<ExecResult>, ExecError> {
        // Guard 1: single FROM table, no JOINs
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return Ok(None);
        }
        // Guard 2: simple table reference (not a subquery)
        let table_name = match &select.from[0].relation {
            TableFactor::Table { name, args: None, .. } => name.to_string(),
            _ => return Ok(None),
        };
        // Guard 3: not a CTE
        if cte_tables.contains_key(&table_name) {
            return Ok(None);
        }
        // Guard 4: no HAVING; WHERE may be a single equality predicate (handled below)
        if select.having.is_some() {
            return Ok(None);
        }
        // Guard 5: resolve column names from sync cache -- bail if table not cached
        let col_info = {
            let cols = self.table_columns.read();
            match cols.get(&table_name) {
                Some(c) => c.clone(),
                None => return Ok(None),
            }
        };
        // Guard 6: test that the engine supports fast paths
        // (fast_count_all returns None for non-columnar engines by default)
        let tbl_storage = self.storage_for(&table_name);
        if tbl_storage.fast_count_all(&table_name).is_none() {
            return Ok(None);
        }
        let resolve_col = |name: &str| -> Option<usize> {
            col_info.iter().position(|(c, _)| c.eq_ignore_ascii_case(name))
        };

        // WHERE: attempt to extract a single equality predicate
        // If WHERE is present but isn't `col = literal`, bail out.
        // Returns None to fall through to normal scan for unsupported predicates.
        let eq_filter: Option<(usize, Value)> = match &select.selection {
            None => None,
            Some(expr) => {
                match Self::extract_fast_eq_filter(expr, &resolve_col) {
                    Some((col_idx, val)) => {
                        // Coerce the literal to match the column's declared type.
                        // SQL parser always produces Int64 for integer literals, but
                        // columns declared as INT store Int32 values.
                        let coerced = Self::coerce_to_column_type(&val, &col_info[col_idx].1);
                        Some((col_idx, coerced))
                    }
                    None => return Ok(None), // WHERE too complex
                }
            }
        };

        // GROUP BY + WHERE is not fast-pathed yet (would need filtered group-by)
        let group_by_col: Option<usize> = match &select.group_by {
            ast::GroupByExpr::Expressions(exprs, _) if exprs.len() == 1 => {
                if eq_filter.is_some() {
                    return Ok(None); // GROUP BY + WHERE: fall through
                }
                match &exprs[0] {
                    Expr::Identifier(id) => resolve_col(&id.value),
                    Expr::CompoundIdentifier(ids) => {
                        ids.last().and_then(|id| resolve_col(&id.value))
                    }
                    _ => return Ok(None),
                }
            }
            ast::GroupByExpr::Expressions(exprs, _) if exprs.is_empty() => None,
            _ => return Ok(None),
        };

        // Parse projection -- COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col),
        // and the GROUP BY key column are all fast-pathed.
        #[derive(Clone)]
        enum FastAgg { Count, Sum(usize), Avg(usize), Min(usize), Max(usize), GroupKey }

        let mut items: Vec<(String, FastAgg)> = Vec::new();

        for item in &select.projection {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(e) => (e, None::<&ast::Ident>),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
                _ => return Ok(None),
            };
            let col_label = alias
                .map(|a| a.value.clone())
                .unwrap_or_else(|| format!("{expr}"));

            match expr {
                Expr::Function(func) if func.over.is_none() => {
                    let fname = func.name.to_string().to_uppercase();
                    // DISTINCT aggregates (e.g. SUM(DISTINCT col)) cannot use
                    // the fast path because the engine sums all rows, not distinct values.
                    if let ast::FunctionArguments::List(l) = &func.args
                        && matches!(l.duplicate_treatment, Some(ast::DuplicateTreatment::Distinct)) {
                            return Ok(None);
                    }
                    // Extract the single column-reference argument, if present
                    let arg_col_idx = match &func.args {
                        ast::FunctionArguments::List(l) if l.args.len() == 1 => {
                            match &l.args[0] {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                    match e {
                                        Expr::Identifier(id) => resolve_col(&id.value),
                                        Expr::CompoundIdentifier(ids) => {
                                            ids.last().and_then(|id| resolve_col(&id.value))
                                        }
                                        _ => None,
                                    }
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    };
                    match fname.as_str() {
                        "COUNT" => {
                            let is_star = match &func.args {
                                ast::FunctionArguments::List(l) => {
                                    l.args.is_empty()
                                        || matches!(
                                            l.args[0],
                                            ast::FunctionArg::Unnamed(
                                                ast::FunctionArgExpr::Wildcard
                                            )
                                        )
                                }
                                _ => false,
                            };
                            if !is_star { return Ok(None); }
                            items.push((col_label, FastAgg::Count));
                        }
                        "SUM" => {
                            let ci = match arg_col_idx { Some(i) => i, None => return Ok(None) };
                            items.push((col_label, FastAgg::Sum(ci)));
                        }
                        "AVG" => {
                            let ci = match arg_col_idx { Some(i) => i, None => return Ok(None) };
                            items.push((col_label, FastAgg::Avg(ci)));
                        }
                        "MIN" => {
                            let ci = match arg_col_idx { Some(i) => i, None => return Ok(None) };
                            items.push((col_label, FastAgg::Min(ci)));
                        }
                        "MAX" => {
                            let ci = match arg_col_idx { Some(i) => i, None => return Ok(None) };
                            items.push((col_label, FastAgg::Max(ci)));
                        }
                        _ => return Ok(None),
                    }
                }
                // GROUP BY key column in projection
                Expr::Identifier(id) => {
                    if let Some(gc) = group_by_col
                        && let Some(idx) = resolve_col(&id.value)
                            && idx == gc { items.push((col_label, FastAgg::GroupKey)); continue; }
                    return Ok(None);
                }
                Expr::CompoundIdentifier(ids) => {
                    if let Some(gc) = group_by_col
                        && let Some(last) = ids.last()
                            && let Some(idx) = resolve_col(&last.value)
                                && idx == gc { items.push((col_label, FastAgg::GroupKey)); continue; }
                    return Ok(None);
                }
                _ => return Ok(None),
            }
        }

        if items.is_empty() {
            return Ok(None);
        }

        // GROUP BY path (no WHERE filter here -- filtered group-by not yet fast-pathed)
        if let Some(key_col) = group_by_col {
            let val_col = items.iter().find_map(|(_, t)| match t {
                FastAgg::Avg(i) | FastAgg::Sum(i) => Some(*i),
                _ => None,
            });
            let groups = match tbl_storage.fast_group_by(&table_name, key_col, val_col) {
                Some(g) => g,
                None => return Ok(None),
            };

            let col_defs: Vec<(String, DataType)> = items
                .iter()
                .map(|(name, t)| {
                    let dtype = match t {
                        FastAgg::GroupKey => DataType::Text,
                        FastAgg::Count => DataType::Int64,
                        FastAgg::Sum(_) | FastAgg::Avg(_) | FastAgg::Min(_) | FastAgg::Max(_) => DataType::Float64,
                    };
                    (name.clone(), dtype)
                })
                .collect();

            let rows: Vec<Row> = groups
                .into_iter()
                .map(|(key, count, avg)| {
                    items
                        .iter()
                        .map(|(_, t)| match t {
                            FastAgg::GroupKey => key.clone(),
                            FastAgg::Count => Value::Int64(count),
                            FastAgg::Sum(_) => avg
                                .map(|a| Value::Float64(a * count as f64))
                                .unwrap_or(Value::Null),
                            FastAgg::Avg(_) => avg.map(Value::Float64).unwrap_or(Value::Null),
                            FastAgg::Min(_) | FastAgg::Max(_) => Value::Null,
                        })
                        .collect()
                })
                .collect();

            return Ok(Some(ExecResult::Select { columns: col_defs, rows }));
        }

        // No GROUP BY -- single-row aggregate
        // With or without an equality WHERE filter.
        let mut result_row: Row = Vec::new();
        let mut col_defs: Vec<(String, DataType)> = Vec::new();

        // Helper: call the right sum variant based on whether a filter is active.
        let sum_or_filtered = |ci: usize| -> Option<(f64, usize)> {
            match &eq_filter {
                Some((fc, fv)) => tbl_storage.fast_sum_f64_filtered(&table_name, ci, *fc, fv),
                None => tbl_storage.fast_sum_f64(&table_name, ci),
            }
        };

        for (col_label, agg) in &items {
            match agg {
                FastAgg::Count => {
                    let n = match &eq_filter {
                        Some((fc, fv)) => match tbl_storage.fast_count_filtered(&table_name, *fc, fv) {
                            Some(c) => c as i64,
                            None => return Ok(None),
                        },
                        None => tbl_storage.fast_count_all(&table_name).unwrap_or(0) as i64,
                    };
                    col_defs.push((col_label.clone(), DataType::Int64));
                    result_row.push(Value::Int64(n));
                }
                FastAgg::Sum(ci) => match sum_or_filtered(*ci) {
                    Some((sum, cnt)) => {
                        // SUM of all NULLs returns NULL per SQL standard.
                        if cnt == 0 {
                            col_defs.push((col_label.clone(), DataType::Float64));
                            result_row.push(Value::Null);
                        } else {
                            // Return Int64 for integer columns to match the normal code path.
                            let is_int = col_info.get(*ci).is_some_and(|(_, dt)| {
                                matches!(dt, DataType::Int32 | DataType::Int64)
                            });
                            if is_int {
                                col_defs.push((col_label.clone(), DataType::Int64));
                                result_row.push(Value::Int64(sum as i64));
                            } else {
                                col_defs.push((col_label.clone(), DataType::Float64));
                                result_row.push(Value::Float64(sum));
                            }
                        }
                    }
                    None => return Ok(None),
                },
                FastAgg::Avg(ci) => match sum_or_filtered(*ci) {
                    Some((sum, cnt)) => {
                        let avg = if cnt == 0 { Value::Null } else { Value::Float64(sum / cnt as f64) };
                        col_defs.push((col_label.clone(), DataType::Float64));
                        result_row.push(avg);
                    }
                    None => return Ok(None),
                },
                FastAgg::Min(ci) => match tbl_storage.fast_min_f64(&table_name, *ci) {
                    Some(v) => {
                        let col_dt = col_info.get(*ci).map(|(_, dt)| dt);
                        match col_dt {
                            Some(DataType::Int32) => {
                                col_defs.push((col_label.clone(), DataType::Int32));
                                result_row.push(Value::Int32(v as i32));
                            }
                            Some(DataType::Int64) => {
                                col_defs.push((col_label.clone(), DataType::Int64));
                                result_row.push(Value::Int64(v as i64));
                            }
                            _ => {
                                col_defs.push((col_label.clone(), DataType::Float64));
                                result_row.push(Value::Float64(v));
                            }
                        }
                    }
                    None => return Ok(None),
                },
                FastAgg::Max(ci) => match tbl_storage.fast_max_f64(&table_name, *ci) {
                    Some(v) => {
                        let col_dt = col_info.get(*ci).map(|(_, dt)| dt);
                        match col_dt {
                            Some(DataType::Int32) => {
                                col_defs.push((col_label.clone(), DataType::Int32));
                                result_row.push(Value::Int32(v as i32));
                            }
                            Some(DataType::Int64) => {
                                col_defs.push((col_label.clone(), DataType::Int64));
                                result_row.push(Value::Int64(v as i64));
                            }
                            _ => {
                                col_defs.push((col_label.clone(), DataType::Float64));
                                result_row.push(Value::Float64(v));
                            }
                        }
                    }
                    None => return Ok(None),
                },
                FastAgg::GroupKey => return Ok(None),
            }
        }

        Ok(Some(ExecResult::Select {
            columns: col_defs,
            rows: vec![result_row],
        }))
    }

    /// Extract a simple `col = literal` equality predicate from a WHERE expression.
    /// Returns `(col_idx, value)` if the WHERE is exactly one equality comparison
    /// against a literal. Returns None for anything more complex.
    pub(super) fn extract_fast_eq_filter(
        expr: &Expr,
        resolve_col: &dyn Fn(&str) -> Option<usize>,
    ) -> Option<(usize, Value)> {
        match expr {
            Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
                // Determine which side is the column and which is the literal
                let (col_expr, lit_expr) = {
                    let left_is_col = matches!(
                        left.as_ref(),
                        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                    );
                    let right_is_col = matches!(
                        right.as_ref(),
                        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                    );
                    if left_is_col && !right_is_col {
                        (left.as_ref(), right.as_ref())
                    } else if right_is_col && !left_is_col {
                        (right.as_ref(), left.as_ref())
                    } else {
                        return None;
                    }
                };
                let col_name = match col_expr {
                    Expr::Identifier(id) => id.value.as_str(),
                    Expr::CompoundIdentifier(ids) => ids.last()?.value.as_str(),
                    _ => return None,
                };
                let col_idx = resolve_col(col_name)?;
                let val = Self::ast_expr_to_literal(lit_expr)?;
                Some((col_idx, val))
            }
            _ => None,
        }
    }

    /// Convert a literal AST expression to a Value. Returns None for non-literals.
    pub(super) fn ast_expr_to_literal(expr: &Expr) -> Option<Value> {
        match expr {
            Expr::Value(v) => match &v.value {
                ast::Value::Number(s, _) => {
                    if let Ok(i) = s.parse::<i64>() { return Some(Value::Int64(i)); }
                    if let Ok(f) = s.parse::<f64>() { return Some(Value::Float64(f)); }
                    None
                }
                ast::Value::SingleQuotedString(s) => Some(Value::Text(s.clone())),
                ast::Value::Boolean(b) => Some(Value::Bool(*b)),
                ast::Value::Null => Some(Value::Null),
                _ => None,
            },
            Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => {
                match Self::ast_expr_to_literal(expr) {
                    Some(Value::Int64(n)) => Some(Value::Int64(-n)),
                    Some(Value::Float64(f)) => Some(Value::Float64(-f)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Extract the literal value from an equality expression like `col = 500` or `500 = col`.
    /// Returns `None` if the expression isn't a simple equality with a literal.
    pub(super) fn extract_equality_value(expr: &Expr) -> Option<Value> {
        match expr {
            Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
                // Try right side as literal (col = literal)
                Self::ast_expr_to_literal(right)
                    .or_else(|| Self::ast_expr_to_literal(left))
            }
            _ => None,
        }
    }

    /// Coerce a Value to match a column's declared DataType for accurate comparison.
    /// SQL parser produces Int64 for all integer literals, but INT columns store Int32.
    pub(super) fn coerce_to_column_type(val: &Value, target: &DataType) -> Value {
        match (val, target) {
            (Value::Int64(n), DataType::Int32) if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 => {
                Value::Int32(*n as i32)
            }
            (Value::Int32(n), DataType::Int64) => Value::Int64(*n as i64),
            (Value::Int64(n), DataType::Float64) => Value::Float64(*n as f64),
            (Value::Float64(f), DataType::Int32) => Value::Int32(*f as i32),
            (Value::Float64(f), DataType::Int64) => Value::Int64(*f as i64),
            _ => val.clone(),
        }
    }

    /// Coerce range bounds to the indexed column's data type.
    fn coerce_index_bounds(
        lo: Value, hi: Value,
        table: &str, index_name: &str,
        table_def: &crate::catalog::TableDef,
        catalog: &crate::catalog::Catalog,
    ) -> (Value, Value) {
        let idx_defs = catalog.get_indexes_cached(table).unwrap_or_default();
        if let Some(idx) = idx_defs.iter().find(|i| i.name == index_name) {
            if let Some(col_name) = idx.columns.first() {
                if let Some(col) = table_def.columns.iter().find(|c| c.name.eq_ignore_ascii_case(col_name)) {
                    return (
                        Self::coerce_to_column_type(&lo, &col.data_type),
                        Self::coerce_to_column_type(&hi, &col.data_type),
                    );
                }
            }
        }
        (lo, hi)
    }

    /// Coerce an index lookup value to the indexed column's data type.
    fn coerce_index_value(
        val: Value,
        table: &str, index_name: &str,
        table_def: &crate::catalog::TableDef,
        catalog: &crate::catalog::Catalog,
    ) -> Value {
        let idx_defs = catalog.get_indexes_cached(table).unwrap_or_default();
        if let Some(idx) = idx_defs.iter().find(|i| i.name == index_name) {
            if let Some(col_name) = idx.columns.first() {
                if let Some(col) = table_def.columns.iter().find(|c| c.name.eq_ignore_ascii_case(col_name)) {
                    return Self::coerce_to_column_type(&val, &col.data_type);
                }
            }
        }
        val
    }

    /// Try to evaluate a simple WHERE predicate using SIMD-accelerated filters.
    ///
    /// Handles patterns: `col = N`, `col > N`, `col < N`, `col >= N`, `col <= N`,
    /// `col != N` on integer/float columns, and `col = 'str'` on text columns.
    /// Returns `Some(matching_row_indices)` on success, or `None` to fall back to
    /// per-row expression evaluation.
    ///
    /// Null values in the column are correctly skipped (never match).
    pub(super) fn try_simd_filter(
        &self,
        where_expr: &Expr,
        rows: &[Row],
        col_meta: &[ColMeta],
    ) -> Option<Vec<usize>> {
        // Only handle simple BinaryOp: col op literal  or  literal op col
        let (col_name, op, literal_val) = match where_expr {
            Expr::BinaryOp { left, op, right } => {
                // Try col op literal
                if let Some(col) = self.expr_as_column_name(left) {
                    if let Ok(val) = self.eval_const_expr(right) {
                        (col, op.clone(), val)
                    } else {
                        return None;
                    }
                }
                // Try literal op col (flip the operator)
                else if let Some(col) = self.expr_as_column_name(right) {
                    if let Ok(val) = self.eval_const_expr(left) {
                        // Flip: `5 > col` becomes `col < 5`
                        let flipped = match op {
                            ast::BinaryOperator::Eq => ast::BinaryOperator::Eq,
                            ast::BinaryOperator::NotEq => ast::BinaryOperator::NotEq,
                            ast::BinaryOperator::Gt => ast::BinaryOperator::Lt,
                            ast::BinaryOperator::Lt => ast::BinaryOperator::Gt,
                            ast::BinaryOperator::GtEq => ast::BinaryOperator::LtEq,
                            ast::BinaryOperator::LtEq => ast::BinaryOperator::GtEq,
                            _ => return None,
                        };
                        (col, flipped, val)
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            }
            _ => return None,
        };

        // Only handle Eq, NotEq, Gt, Lt, GtEq, LtEq
        if !matches!(op,
            ast::BinaryOperator::Eq | ast::BinaryOperator::NotEq
            | ast::BinaryOperator::Gt | ast::BinaryOperator::Lt
            | ast::BinaryOperator::GtEq | ast::BinaryOperator::LtEq
        ) {
            return None;
        }

        // Find column index
        let col_idx = col_meta.iter().position(|c| c.name == col_name)?;

        // Determine column type
        let dtype = &col_meta[col_idx].dtype;

        match dtype {
            DataType::Int32 | DataType::Int64 => {
                let threshold = match &literal_val {
                    Value::Int32(n) => *n as i64,
                    Value::Int64(n) => *n,
                    Value::Float64(f) => *f as i64,
                    _ => return None,
                };

                // Extract dense i64 column with row-index mapping (handles NULLs)
                let mut dense_vals: Vec<i64> = Vec::with_capacity(rows.len());
                let mut row_indices: Vec<usize> = Vec::with_capacity(rows.len());
                for (i, row) in rows.iter().enumerate() {
                    if let Some(val) = row.get(col_idx) {
                        match val {
                            Value::Int32(n) => {
                                dense_vals.push(*n as i64);
                                row_indices.push(i);
                            }
                            Value::Int64(n) => {
                                dense_vals.push(*n);
                                row_indices.push(i);
                            }
                            _ => {} // NULL or type mismatch -- skip
                        }
                    }
                }

                let matching_dense = match op {
                    ast::BinaryOperator::Eq => simd::filter_i64_equals(&dense_vals, threshold),
                    ast::BinaryOperator::NotEq => simd::filter_i64_not_equals(&dense_vals, threshold),
                    ast::BinaryOperator::Gt => simd::filter_i64_greater(&dense_vals, threshold),
                    ast::BinaryOperator::Lt => simd::filter_i64_less(&dense_vals, threshold),
                    ast::BinaryOperator::GtEq => simd::filter_i64_greater_eq(&dense_vals, threshold),
                    ast::BinaryOperator::LtEq => simd::filter_i64_less_eq(&dense_vals, threshold),
                    _ => return None,
                };

                // Map dense indices back to original row indices
                Some(matching_dense.into_iter().map(|di| row_indices[di]).collect())
            }
            DataType::Float64 => {
                let threshold = match &literal_val {
                    Value::Float64(f) => *f,
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    _ => return None,
                };

                // Extract dense f64 column with row-index mapping (handles NULLs)
                let mut dense_vals: Vec<f64> = Vec::with_capacity(rows.len());
                let mut row_indices: Vec<usize> = Vec::with_capacity(rows.len());
                for (i, row) in rows.iter().enumerate() {
                    if let Some(val) = row.get(col_idx) {
                        match val {
                            Value::Float64(f) => {
                                dense_vals.push(*f);
                                row_indices.push(i);
                            }
                            Value::Int32(n) => {
                                dense_vals.push(*n as f64);
                                row_indices.push(i);
                            }
                            Value::Int64(n) => {
                                dense_vals.push(*n as f64);
                                row_indices.push(i);
                            }
                            _ => {} // NULL or type mismatch -- skip
                        }
                    }
                }

                let matching_dense = match op {
                    ast::BinaryOperator::Eq => simd::filter_f64_equals(&dense_vals, threshold),
                    ast::BinaryOperator::NotEq => simd::filter_f64_not_equals(&dense_vals, threshold),
                    ast::BinaryOperator::Gt => simd::filter_f64_greater(&dense_vals, threshold),
                    ast::BinaryOperator::Lt => simd::filter_f64_less(&dense_vals, threshold),
                    ast::BinaryOperator::GtEq => simd::filter_f64_greater_eq(&dense_vals, threshold),
                    ast::BinaryOperator::LtEq => simd::filter_f64_less_eq(&dense_vals, threshold),
                    _ => return None,
                };

                // Map dense indices back to original row indices
                Some(matching_dense.into_iter().map(|di| row_indices[di]).collect())
            }
            DataType::Text => {
                // Fast text equality/inequality filter: direct string comparison without AST eval
                if !matches!(op, ast::BinaryOperator::Eq | ast::BinaryOperator::NotEq) {
                    return None;
                }
                let needle = match &literal_val {
                    Value::Text(s) => s.as_str(),
                    _ => return None,
                };
                let mut matching = Vec::with_capacity(rows.len() / 4);
                let negate = matches!(op, ast::BinaryOperator::NotEq);
                for (i, row) in rows.iter().enumerate() {
                    if let Some(Value::Text(s)) = row.get(col_idx) {
                        if negate { if s != needle { matching.push(i); } }
                        else { if s == needle { matching.push(i); } }
                    }
                }
                Some(matching)
            }
            _ => None, // Unsupported column type -- fall back
        }
    }

    /// Try to push a simple WHERE predicate into the storage engine so
    /// the engine can filter rows before materialising them.
    ///
    /// Supported patterns:
    ///   - `col = literal` -> `fast_scan_where_eq`
    ///   - `col BETWEEN low AND high` -> `fast_scan_where_range`
    ///   - `col >= low AND col <= high` -> `fast_scan_where_range`
    ///
    /// Returns `None` to fall back to a full table scan.
    pub(super) fn try_columnar_filtered_scan(
        &self,
        select: &ast::Select,
        cte_tables: &CteTableMap,
    ) -> Option<(Vec<ColMeta>, Vec<Row>)> {
        // Guard: single table, no JOINs
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        let where_expr = select.selection.as_ref()?;

        // Extract the predicate type
        enum FilterKind {
            Eq(String, Value),
            Range(String, Value, Value),
        }

        let kind = match where_expr {
            Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
                let (col, val) = self.try_extract_col_eq_literal(left, right)?;
                FilterKind::Eq(col, val)
            }
            Expr::Between { expr, low, high, negated } if !*negated => {
                let (col, lo, hi) = self.try_extract_col_between_literals(expr, low, high)?;
                FilterKind::Range(col, lo, hi)
            }
            // AND of two range comparisons on same column: col >= X AND col <= Y
            Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
                let (col, lo, hi) = self.try_extract_range_from_and(left, right)?;
                FilterKind::Range(col, lo, hi)
            }
            _ => return None,
        };

        let (table_name, label) = match &select.from[0].relation {
            TableFactor::Table { name, alias, args: None, .. } => {
                let t = name.to_string();
                let l = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| t.clone());
                (t, l)
            }
            _ => return None,
        };

        if cte_tables.contains_key(&table_name) {
            return None;
        }

        let col_meta = self.build_col_meta_from_cache(&table_name, &label)?;
        let storage = self.storage_for(&table_name);

        match kind {
            FilterKind::Eq(col_name, filter_val) => {
                let col_idx = col_meta.iter().position(|c| c.name == col_name)?;
                let rows = storage.fast_scan_where_eq(&table_name, col_idx, &filter_val)?;
                Some((col_meta, rows))
            }
            FilterKind::Range(col_name, lo, hi) => {
                let col_idx = col_meta.iter().position(|c| c.name == col_name)?;
                let rows = storage.fast_scan_where_range(&table_name, col_idx, &lo, &hi)?;
                Some((col_meta, rows))
            }
        }
    }

    /// Try to extract a range predicate from `col >= X AND col <= Y` (same column).
    pub(super) fn try_extract_range_from_and(
        &self,
        left: &Expr,
        right: &Expr,
    ) -> Option<(String, Value, Value)> {
        // Pattern: left is `col >= low` or `col > low`, right is `col <= high` or `col < high`
        // (or vice versa). Both must reference the same column.
        let (col1, val1, is_lower1) = self.try_extract_col_bound(left)?;
        let (col2, val2, is_lower2) = self.try_extract_col_bound(right)?;
        if col1 != col2 { return None; }
        // One must be a lower bound and one an upper bound
        if is_lower1 && !is_lower2 {
            Some((col1, val1, val2))
        } else if !is_lower1 && is_lower2 {
            Some((col1, val2, val1))
        } else {
            None
        }
    }

    /// Extract a single bound from `col >= val`, `col > val`, `col <= val`, `col < val`.
    /// Returns (column_name, value, is_lower_bound).
    pub(super) fn try_extract_col_bound(&self, expr: &Expr) -> Option<(String, Value, bool)> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    ast::BinaryOperator::GtEq | ast::BinaryOperator::Gt => {
                        // col >= val -> lower bound (col on left)
                        if let Some((col, val)) = self.try_extract_col_eq_literal(left, right) {
                            return Some((col, val, true));
                        }
                        // val >= col -> upper bound (col on right)
                        if let Some((col, val)) = self.try_extract_col_eq_literal(right, left) {
                            return Some((col, val, false));
                        }
                        None
                    }
                    ast::BinaryOperator::LtEq | ast::BinaryOperator::Lt => {
                        // col <= val -> upper bound (col on left)
                        if let Some((col, val)) = self.try_extract_col_eq_literal(left, right) {
                            return Some((col, val, false));
                        }
                        // val <= col -> lower bound (col on right)
                        if let Some((col, val)) = self.try_extract_col_eq_literal(right, left) {
                            return Some((col, val, true));
                        }
                        None
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Try to use storage-level fast scan for a simple predicate during table loading.
    /// Handles equality, BETWEEN, and simple range comparisons (col < val, col > val, etc.).
    /// Returns None if the predicate is too complex or the storage engine doesn't support it.
    pub(super) fn try_storage_fast_scan(
        &self,
        table_name: &str,
        where_expr: &Expr,
        col_meta: &[ColMeta],
    ) -> Option<Vec<Row>> {
        let storage = self.storage_for(table_name);

        match where_expr {
            Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
                let (col_name, filter_val) = self.try_extract_col_eq_literal(left, right)?;
                let col_idx = col_meta.iter().position(|c| c.name == col_name)?;
                storage.fast_scan_where_eq(table_name, col_idx, &filter_val)
            }
            Expr::Between { expr, low, high, negated } if !*negated => {
                let (col_name, lo, hi) = self.try_extract_col_between_literals(expr, low, high)?;
                let col_idx = col_meta.iter().position(|c| c.name == col_name)?;
                storage.fast_scan_where_range(table_name, col_idx, &lo, &hi)
            }
            // Single range comparison: col < val, col > val, col <= val, col >= val
            // Use a wide sentinel bound on the open end
            Expr::BinaryOp { left: _, op: ast::BinaryOperator::Lt | ast::BinaryOperator::LtEq |
                    ast::BinaryOperator::Gt | ast::BinaryOperator::GtEq, right: _ } =>
            {
                let (col_name, val, is_lower) = self.try_extract_col_bound(where_expr)?;
                let col_idx = col_meta.iter().position(|c| c.name == col_name)?;
                let (lo, hi) = if is_lower {
                    (val, Value::Int64(i64::MAX))
                } else {
                    (Value::Int64(i64::MIN), val)
                };
                // For strict comparisons (< / >), we pass inclusive bounds to the storage
                // engine and rely on the post-scan filter from apply_pushdown_for_factor
                // to enforce strict semantics. This is safe because the pushdown filter
                // is always applied after load_table_factor_with_ctes returns.
                storage.fast_scan_where_range(table_name, col_idx, &lo, &hi)
            }
            Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
                let (col_name, lo, hi) = self.try_extract_range_from_and(left, right)?;
                let col_idx = col_meta.iter().position(|c| c.name == col_name)?;
                storage.fast_scan_where_range(table_name, col_idx, &lo, &hi)
            }
            _ => None,
        }
    }

    /// Fully synchronous index scan attempt -- no `.await` points.
    /// Uses parking_lot caches and `index_lookup_sync` to avoid async deadlocks
    /// in the nested Box::pin future chain of execute_select_inner_with_ctes.
    pub(super) fn try_index_scan_sync(
        &self,
        table_name: &str,
        label: &str,
        where_expr: &Expr,
    ) -> IndexScanResult {
        let (eq_preds, range_preds, remaining) = self.extract_index_predicates(where_expr);
        if eq_preds.is_empty() && range_preds.is_empty() {
            return None;
        }

        // Check sync btree_indexes cache for a matching index.
        let indexes = self.btree_indexes.read();
        for (col_name, value) in &eq_preds {
            let key = (table_name.to_string(), col_name.clone());
            if let Some(index_name) = indexes.get(&key) {
                // Try synchronous index lookup via storage engine
                match self.storage.index_lookup_sync(table_name, index_name, value) {
                    Ok(Some(rows)) => {
                        self.metrics.rows_scanned.inc_by(rows.len() as u64);
                        let col_meta = self.build_col_meta_from_cache(table_name, label)?;

                        // Build remaining filter: other eq preds + remaining expr
                        let mut other_preds: Vec<Expr> = Vec::new();
                        for (other_col, other_val) in &eq_preds {
                            if other_col == col_name {
                                continue;
                            }
                            other_preds.push(Expr::BinaryOp {
                                left: Box::new(Expr::Identifier(ast::Ident::new(other_col.clone()))),
                                op: ast::BinaryOperator::Eq,
                                right: Box::new(self.value_to_expr(other_val)),
                            });
                        }
                        if let Some(rest) = &remaining {
                            other_preds.push(rest.clone());
                        }
                        let final_remaining = other_preds.into_iter().reduce(|a, b| {
                            Expr::BinaryOp {
                                left: Box::new(a),
                                op: ast::BinaryOperator::And,
                                right: Box::new(b),
                            }
                        });

                        return Some((col_meta, rows, final_remaining, None));
                    }
                    Ok(None) => continue,
                    Err(_) => continue,
                }
            }
        }

        // Range fallback: for integer BETWEEN predicates, probe the index for
        // each key in-range and let the normal filter path enforce full semantics.
        for (col_name, low, high) in &range_preds {
            let key = (table_name.to_string(), col_name.clone());
            if let Some(index_name) = indexes.get(&key)
                && let Some(rows) = self.try_index_lookup_range_sync(table_name, index_name, low, high) {
                    self.metrics.rows_scanned.inc_by(rows.len() as u64);
                    let col_meta = self.build_col_meta_from_cache(table_name, label)?;
                    let full_filter = Some(where_expr.clone());
                    // Range scan returns rows in B-tree key order -- tag with sorted column
                    // so the aggregate path can skip the HashMap and stream in order.
                    return Some((col_meta, rows, full_filter, Some(col_name.clone())));
                }
        }

        None
    }

    /// Convert a Value to an AST Expr for re-creating filter expressions.
    pub(super) fn value_to_expr(&self, val: &Value) -> Expr {
        let v = match val {
            Value::Int32(n) => ast::Value::Number(n.to_string(), false),
            Value::Int64(n) => ast::Value::Number(n.to_string(), false),
            Value::Float64(f) => ast::Value::Number(f.to_string(), false),
            Value::Text(s) => ast::Value::SingleQuotedString(s.clone()),
            Value::Bool(b) => ast::Value::Boolean(*b),
            Value::Null => ast::Value::Null,
            _ => ast::Value::SingleQuotedString(val.to_string()),
        };
        Expr::Value(ast::ValueWithSpan {
            value: v,
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    /// SELECT execution that is CTE-aware -- delegates to load_table_factor_with_ctes.
    pub(super) async fn execute_select_inner_with_ctes(
        &self,
        select: &ast::Select,
        cte_tables: &CteTableMap,
    ) -> Result<SelectResult, ExecError> {
        // Expression-only query: SELECT 1, SELECT 'hello', SELECT 1+1
        if select.from.is_empty() {
            return Ok(SelectResult::Projected(
                self.execute_select_expressions(&select.projection)?,
            ));
        }

        // Columnar fast-aggregate (before any row scan)
        // Intercepts COUNT(*) / SUM / AVG / GROUP BY on ColumnarStorageEngine tables.
        // Returns None if the engine doesn't support it or the pattern is unsupported.
        if let Some(fast) = self.try_columnar_fast_aggregate(select, cte_tables)? {
            return Ok(SelectResult::Projected(fast));
        }

        // Index-aware optimization (fully synchronous)
        // For simple single-table queries with WHERE equality predicates,
        // try to use a B-tree index instead of a full table scan.
        let index_result: IndexScanResult = if select.from.len() == 1
            && select.from[0].joins.is_empty()
        {
            if let (Some(selection), TableFactor::Table { name, alias, args: None, .. }) =
                (&select.selection, &select.from[0].relation) {
                let table_name = name.to_string();
                let label = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());
                // Don't try index scan for CTEs or virtual tables
                if !cte_tables.contains_key(&table_name) {
                    self.try_index_scan_sync(&table_name, &label, selection)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let (col_meta, filtered, sorted_by_col) = if let Some((col_meta, rows, remaining_where, sorted_by)) = index_result {
            // Index scan succeeded -- apply remaining predicates if any
            // Note: filtering preserves relative row order, so sorted_by remains valid.
            let filtered = if let Some(ref expr) = remaining_where {
                // Try SIMD-accelerated filter for simple col op literal predicates
                if let Some(indices) = self.try_simd_filter(expr, &rows, &col_meta) {
                    indices.into_iter().map(|i| rows[i].clone()).collect()
                } else {
                    // Parallel Rayon filter for large sets, serial for small
                    self.parallel_filter(rows, expr, &col_meta)
                }
            } else {
                rows
            };
            (col_meta, filtered, sorted_by)
        } else if let Some((col_meta, rows)) = self.try_columnar_filtered_scan(select, cte_tables) {
            // Columnar filter pushdown: engine filtered non-matching rows before
            // materialising -- no further WHERE evaluation needed.
            (col_meta, rows, None)
        } else {
            // Fall back to AST execution with safe relation-level WHERE pushdown.
            let (pushdown_map, remaining_where) = if let Some(ref where_expr) = select.selection {
                self.partition_where_for_ast_pushdown(&select.from, where_expr)
            } else {
                (HashMap::new(), None)
            };
            let (col_meta, combined_rows, unconsumed) =
                self.build_from_rows_with_ctes(&select.from, cte_tables, Some(&pushdown_map), remaining_where.as_ref()).await?;

            let filtered: Vec<Row> = if let Some(ref expr) = unconsumed {
                // Try SIMD-accelerated filter for simple col op literal predicates
                if let Some(indices) = self.try_simd_filter(expr, &combined_rows, &col_meta) {
                    indices.into_iter().map(|i| combined_rows[i].clone()).collect()
                } else {
                    // Parallel Rayon filter for large sets, serial for small
                    self.parallel_filter(combined_rows, expr, &col_meta)
                }
            } else {
                combined_rows
            };
            (col_meta, filtered, None)
        };

        // Check for window functions
        let has_window = select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_window_function(e)
            }
            _ => false,
        });

        // Check if query uses aggregates
        let has_aggregates = select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_aggregate(e)
            }
            _ => false,
        });

        let has_group_by = matches!(&select.group_by, ast::GroupByExpr::Expressions(exprs, _) if !exprs.is_empty());

        if has_window {
            // Window function query -- evaluate projection with window context
            Ok(SelectResult::Projected(
                self.execute_window_query(select, &col_meta, filtered)?,
            ))
        } else if has_aggregates || has_group_by {
            Ok(SelectResult::Projected(
                self.execute_aggregate(select, &col_meta, filtered, sorted_by_col.as_deref())?,
            ))
        } else {
            Ok(SelectResult::Full {
                col_meta,
                rows: filtered,
                projection: select.projection.clone(),
            })
        }
    }

    /// Build FROM rows with CTE awareness.
    ///
    /// `implicit_join_where` contains cross-table predicates from the WHERE clause
    /// that reference multiple tables (e.g. `a.id = b.aid`).  When processing
    /// comma-separated FROM tables, equi-join conditions from this predicate are
    /// used for hash join instead of cross join, dramatically reducing work from
    /// O(N*M) to O(N+M).  Unconsumed predicates are returned as the third element.
    pub(super) async fn build_from_rows_with_ctes(
        &self,
        from: &[ast::TableWithJoins],
        cte_tables: &CteTableMap,
        pushdown: Option<&HashMap<String, Vec<Expr>>>,
        implicit_join_where: Option<&Expr>,
    ) -> Result<(Vec<ColMeta>, Vec<Row>, Option<Expr>), ExecError> {
        if from.is_empty() {
            return Ok((Vec::new(), vec![Vec::new()], implicit_join_where.cloned()));
        }

        // Track which cross-table predicates have NOT yet been consumed by hash joins.
        let mut remaining_preds: Vec<Expr> = implicit_join_where
            .map(|e| planner::split_conjunction(e).into_iter().cloned().collect())
            .unwrap_or_default();

        let first = &from[0];
        let first_pushdown = Self::factor_pushdown_expr(&first.relation, pushdown);
        let (mut col_meta, rows0) = self
            .load_table_factor_with_ctes(&first.relation, cte_tables, first_pushdown.as_ref())
            .await?;
        let mut rows = self.apply_pushdown_for_factor(&first.relation, rows0, &col_meta, pushdown);

        for join in &first.joins {
            // Check for LATERAL derived table
            if matches!(&join.relation, TableFactor::Derived { lateral: true, .. }) {
                let (new_meta, new_rows) = self.execute_lateral_join(
                    &col_meta, &rows, &join.relation, &join.join_operator, cte_tables,
                ).await?;
                col_meta = new_meta;
                rows = new_rows;
                continue;
            }
            if let Some((new_meta, new_rows)) = self
                .try_execute_index_join_for_factor(&col_meta, &rows, join, cte_tables, pushdown)
                .await?
            {
                col_meta = new_meta;
                rows = new_rows;
                continue;
            }
            let right_pushdown = Self::factor_pushdown_expr(&join.relation, pushdown);
            let (right_meta, right_rows0) = self
                .load_table_factor_with_ctes(&join.relation, cte_tables, right_pushdown.as_ref())
                .await?;
            let right_rows = self.apply_pushdown_for_factor(&join.relation, right_rows0, &right_meta, pushdown);
            let (new_meta, new_rows) =
                self.execute_join(&col_meta, &rows, &right_meta, &right_rows, &join.join_operator)?;
            col_meta = new_meta;
            rows = new_rows;
        }

        for twj in &from[1..] {
            let twj_pushdown = Self::factor_pushdown_expr(&twj.relation, pushdown);
            let (right_meta, right_rows0) = self
                .load_table_factor_with_ctes(&twj.relation, cte_tables, twj_pushdown.as_ref())
                .await?;
            let right_rows = self.apply_pushdown_for_factor(&twj.relation, right_rows0, &right_meta, pushdown);

            // Optimization: implicit hash join for comma-separated FROM
            // Instead of O(N*M) cross join + filter, try to extract equi-join
            // conditions from the remaining cross-table WHERE predicates and
            // use O(N+M) hash join.
            if !remaining_preds.is_empty()
                && let Some(join_expr) = Self::combine_predicates(remaining_preds.clone())
                    && let Some((left_keys, right_keys, residual)) =
                        Self::extract_equijoin_keys(&join_expr, &col_meta, &right_meta)
                    {
                        let combined_meta: Vec<ColMeta> = col_meta
                            .iter()
                            .chain(right_meta.iter())
                            .cloned()
                            .collect();
                        let join_rows = self.execute_hash_join(
                            &col_meta, &rows, &right_meta, &right_rows,
                            &left_keys, &right_keys, JoinType::Inner,
                            residual.as_ref(), &combined_meta,
                        )?;
                        // Update remaining predicates: only keep the residual
                        remaining_preds = residual
                            .map(|e| planner::split_conjunction(&e).into_iter().cloned().collect())
                            .unwrap_or_default();
                        col_meta = combined_meta;
                        rows = join_rows;

                        // Process explicit JOINs on this table (rare with comma syntax)
                        for join in &twj.joins {
                            if let Some((nm, nr)) = self
                                .try_execute_index_join_for_factor(&col_meta, &rows, join, cte_tables, pushdown)
                                .await?
                            {
                                col_meta = nm;
                                rows = nr;
                                continue;
                            }
                            let join_pushdown = Self::factor_pushdown_expr(&join.relation, pushdown);
                            let (jm, jr0) = self
                                .load_table_factor_with_ctes(&join.relation, cte_tables, join_pushdown.as_ref())
                                .await?;
                            let jr = self.apply_pushdown_for_factor(&join.relation, jr0, &jm, pushdown);
                            let (nm, nr) =
                                self.execute_join(&col_meta, &rows, &jm, &jr, &join.join_operator)?;
                            col_meta = nm;
                            rows = nr;
                        }
                        continue;
                    }

            // Fallback: cross join (no equi-join keys found in WHERE)
            let (new_meta, new_rows) =
                self.cross_join(&col_meta, &rows, &right_meta, &right_rows);
            col_meta = new_meta;
            rows = new_rows;

            for join in &twj.joins {
                if let Some((nm, nr)) = self
                    .try_execute_index_join_for_factor(&col_meta, &rows, join, cte_tables, pushdown)
                    .await?
                {
                    col_meta = nm;
                    rows = nr;
                    continue;
                }
                let join_pushdown = Self::factor_pushdown_expr(&join.relation, pushdown);
                let (jm, jr0) = self
                    .load_table_factor_with_ctes(&join.relation, cte_tables, join_pushdown.as_ref())
                    .await?;
                let jr = self.apply_pushdown_for_factor(&join.relation, jr0, &jm, pushdown);
                let (nm, nr) =
                    self.execute_join(&col_meta, &rows, &jm, &jr, &join.join_operator)?;
                col_meta = nm;
                rows = nr;
            }
        }

        let unconsumed = Self::combine_predicates(remaining_preds);
        Ok((col_meta, rows, unconsumed))
    }

    /// Load a table factor, checking CTEs, views, and subqueries first.
    pub(super) async fn load_table_factor_with_ctes(
        &self,
        factor: &TableFactor,
        cte_tables: &CteTableMap,
        pushdown_expr: Option<&Expr>,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        match factor {
            TableFactor::Table { name, alias, args, .. } => {
                let table_name = name.to_string();
                let alias_str = alias.as_ref().map(|a| a.name.value.clone());
                let label = alias_str.unwrap_or_else(|| table_name.clone());

                // Check if this is a table function call (e.g., generate_series(1, 5))
                if let Some(fn_args) = args {
                    let func_name = table_name.to_lowercase();
                    let arg_values: Vec<Value> = fn_args.args.iter().filter_map(|a| {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = a {
                            self.eval_const_expr(e).ok()
                        } else {
                            None
                        }
                    }).collect();
                    return self.execute_table_function(&func_name, &arg_values, &label);
                }

                // Check CTE first
                if let Some((meta, rows)) = cte_tables.get(&table_name) {
                    let relabeled: Vec<ColMeta> = meta
                        .iter()
                        .map(|c| ColMeta {
                            table: Some(label.clone()),
                            name: c.name.clone(),
                            dtype: c.dtype.clone(),
                        })
                        .collect();
                    return Ok((relabeled, rows.clone()));
                }

                // Check materialized views
                let mv_opt = self.materialized_views.read().await.get(&table_name).cloned();
                if let Some(mv) = mv_opt {
                    let col_meta: Vec<ColMeta> = mv.columns
                        .iter()
                        .map(|(name, dtype)| ColMeta {
                            table: Some(label.clone()),
                            name: name.clone(),
                            dtype: dtype.clone(),
                        })
                        .collect();
                    return Ok((col_meta, mv.rows.clone()));
                }

                // Check views
                let view_opt = self.views.read().await.get(&table_name).cloned();
                if let Some(view_def) = view_opt {
                    let view_result = self.execute(&view_def.sql).await?;
                    if let Some(ExecResult::Select { columns, rows }) = view_result.into_iter().next()
                    {
                        let col_meta: Vec<ColMeta> = columns
                            .iter()
                            .map(|(name, dtype)| ColMeta {
                                table: Some(label.clone()),
                                name: name.clone(),
                                dtype: dtype.clone(),
                            })
                            .collect();
                        return Ok((col_meta, rows));
                    }
                    return Err(ExecError::Unsupported("view did not return SELECT result".into()));
                }

                // Check information_schema / pg_catalog virtual tables
                let lower_name = table_name.to_lowercase();
                if let Some(result) = self.load_virtual_table(&lower_name, &label).await? {
                    return Ok(result);
                }

                // For JOIN-aware AST execution, attempt indexed lookup using relation-local
                // pushdown predicates before falling back to full table scan.
                if let Some(where_expr) = pushdown_expr
                    && let Some((col_meta, rows, remaining_where, _sorted_by)) =
                        self.try_index_scan_sync(&table_name, &label, where_expr)
                    {
                        let filtered_rows = if let Some(ref expr) = remaining_where {
                            // Parallel Rayon filter for large sets, serial for small
                            self.parallel_filter(rows, expr, &col_meta)
                        } else {
                            rows
                        };
                        return Ok((col_meta, filtered_rows));
                    }

                // Regular table
                let table_def = self.get_table(&table_name).await?;
                let col_meta: Vec<ColMeta> = table_def
                    .columns
                    .iter()
                    .map(|c| ColMeta {
                        table: Some(label.clone()),
                        name: c.name.clone(),
                        dtype: c.data_type.clone(),
                    })
                    .collect();

                // Try storage-level filtered scan for pushed-down predicates
                if let Some(where_expr) = pushdown_expr
                    && let Some(rows) = self.try_storage_fast_scan(&table_name, where_expr, &col_meta) {
                        self.metrics.rows_scanned.inc_by(rows.len() as u64);
                        return Ok((col_meta, rows));
                    }

                let rows = self.storage_for(&table_name).scan(&table_name).await?;
                self.metrics.rows_scanned.inc_by(rows.len() as u64);
                Ok((col_meta, rows))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                // Subquery in FROM: SELECT * FROM (SELECT ...) AS alias
                let sub_result = self.execute_query(*subquery.clone()).await?;
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "subquery".into());
                if let ExecResult::Select { columns, rows } = sub_result {
                    let col_meta: Vec<ColMeta> = columns
                        .iter()
                        .map(|(name, dtype)| ColMeta {
                            table: Some(alias_name.clone()),
                            name: name.clone(),
                            dtype: dtype.clone(),
                        })
                        .collect();
                    Ok((col_meta, rows))
                } else {
                    Err(ExecError::Unsupported("subquery must return rows".into()))
                }
            }
            TableFactor::Function { name, args, alias, .. } => {
                let alias_name = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "func".into());

                let func_name = name.to_string().to_lowercase();
                let fn_args: Vec<Value> = args.iter().filter_map(|a| {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = a {
                        self.eval_const_expr(e).ok()
                    } else {
                        None
                    }
                }).collect();

                self.execute_table_function(&func_name, &fn_args, &alias_name)
            }
            TableFactor::UNNEST { alias, array_exprs, .. } => {
                let alias_name = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "unnest".into());
                let col_meta = vec![ColMeta {
                    table: Some(alias_name.clone()),
                    name: "unnest".into(),
                    dtype: DataType::Text,
                }];
                let mut rows = Vec::new();
                for expr in array_exprs {
                    if let Ok(Value::Array(vals)) = self.eval_const_expr(expr) {
                        for v in vals {
                            rows.push(vec![v]);
                        }
                    }
                }
                Ok((col_meta, rows))
            }
            _ => Err(ExecError::Unsupported("unsupported table factor".into())),
        }
    }

    /// Execute a LATERAL join: for each left row, substitute outer references
    /// into the subquery, execute it, then combine with the left row.
    pub(super) async fn execute_lateral_join(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        right_factor: &TableFactor,
        join_operator: &ast::JoinOperator,
        _cte_tables: &CteTableMap,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        let TableFactor::Derived { subquery, alias, .. } = right_factor else {
            return Err(ExecError::Unsupported("LATERAL only supported on derived tables".into()));
        };
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| "lateral".into());

        let is_left_join = matches!(
            join_operator,
            ast::JoinOperator::Left(..) | ast::JoinOperator::LeftOuter(..)
        );

        let mut result_meta: Option<Vec<ColMeta>> = None;
        let mut result_rows: Vec<Row> = Vec::new();

        for left_row in left_rows {
            // Substitute outer references in the subquery with literal values from left_row
            let substituted_query = substitute_outer_refs_in_query(subquery, left_row, left_meta);
            // Execute the substituted query
            let sub_result = self.execute_query(substituted_query).await?;
            let (sub_cols, sub_rows) = if let ExecResult::Select { columns, rows } = sub_result {
                (columns, rows)
            } else {
                continue;
            };

            let right_meta: Vec<ColMeta> = sub_cols
                .iter()
                .map(|(name, dtype)| ColMeta {
                    table: Some(alias_name.clone()),
                    name: name.clone(),
                    dtype: dtype.clone(),
                })
                .collect();

            if result_meta.is_none() {
                let combined: Vec<ColMeta> = left_meta.iter()
                    .chain(right_meta.iter()).cloned().collect();
                result_meta = Some(combined);
            }

            if sub_rows.is_empty() && is_left_join {
                let nulls: Vec<Value> = right_meta.iter().map(|_| Value::Null).collect();
                let combined: Row = left_row.iter().chain(nulls.iter()).cloned().collect();
                result_rows.push(combined);
            } else {
                for right_row in &sub_rows {
                    let combined: Row = left_row.iter().chain(right_row.iter()).cloned().collect();
                    result_rows.push(combined);
                }
            }
        }

        Ok((result_meta.unwrap_or_default(), result_rows))
    }

    /// Execute a table-returning function (generate_series, etc.)
    pub(super) fn execute_table_function(
        &self,
        name: &str,
        args: &[Value],
        alias: &str,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        match name {
            "generate_series" => {
                let start = match args.first() {
                    Some(Value::Int32(n)) => *n as i64,
                    Some(Value::Int64(n)) => *n,
                    _ => return Err(ExecError::Unsupported("generate_series requires integer arguments".into())),
                };
                let stop = match args.get(1) {
                    Some(Value::Int32(n)) => *n as i64,
                    Some(Value::Int64(n)) => *n,
                    _ => return Err(ExecError::Unsupported("generate_series requires integer arguments".into())),
                };
                let step = match args.get(2) {
                    Some(Value::Int32(n)) => *n as i64,
                    Some(Value::Int64(n)) => *n,
                    None => 1, // PostgreSQL default: always 1
                    _ => return Err(ExecError::Unsupported("generate_series step must be integer".into())),
                };

                if step == 0 {
                    return Err(ExecError::Unsupported("generate_series step cannot be zero".into()));
                }

                let col_meta = vec![ColMeta {
                    table: Some(alias.into()),
                    name: "generate_series".into(),
                    dtype: DataType::Int64,
                }];

                let mut rows = Vec::new();
                let mut val = start;
                if step > 0 {
                    while val <= stop {
                        rows.push(vec![Value::Int64(val)]);
                        val += step;
                    }
                } else {
                    while val >= stop {
                        rows.push(vec![Value::Int64(val)]);
                        val += step;
                    }
                }
                Ok((col_meta, rows))
            }
            _ => Err(ExecError::Unsupported(format!("unknown table function: {name}"))),
        }
    }

    /// BinaryHeap-based top-K: select K best rows from `rows` using a max-heap.
    /// Returns sorted result of exactly min(K, rows.len()) rows.
    ///
    /// This is O(N log K) time and O(K) memory for the result.
    /// The input `rows` Vec is consumed and freed during processing.
    ///
    /// `sort_cols`: (column_index, ascending, nulls_first) triples.
    pub(super) fn top_k_heap_sort(
        rows: Vec<Row>,
        sort_cols: &[(usize, bool, bool)],
        k: usize,
    ) -> Vec<Row> {
        if k == 0 {
            return Vec::new();
        }

        // Comparator for the DESIRED output order.
        // The BinaryHeap is a max-heap: pop() removes the element that compares greatest.
        // For top-K in ASC order we want to keep K smallest, so the heap should pop the
        // largest (= worst). Standard comparison does this.
        let cmp_row = |a: &Row, b: &Row| -> std::cmp::Ordering {
            for &(idx, asc, nulls_first) in sort_cols {
                let va = a.get(idx).unwrap_or(&Value::Null);
                let vb = b.get(idx).unwrap_or(&Value::Null);
                let ord = cmp_with_nulls(va, vb, asc, nulls_first);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        };

        // Bounded buffer approach: accumulate rows, periodically evict the worst.
        // For K ≤ 1024, this is efficient (small constant).
        if k <= 1024 {
            let mut buf: Vec<Row> = Vec::with_capacity(k + 1);
            for row in rows {
                buf.push(row);
                if buf.len() > k * 2 {
                    // Partial sort: keep only K best via select_nth_unstable_by
                    buf.select_nth_unstable_by(k - 1, |a, b| cmp_row(a, b));
                    buf.truncate(k);
                }
            }
            // Final sort of remaining ≤ 2K elements
            let final_k = k.min(buf.len());
            buf.select_nth_unstable_by(final_k - 1, |a, b| cmp_row(a, b));
            buf.truncate(k);
            buf.sort_by(|a, b| cmp_row(a, b));
            buf.shrink_to_fit();
            return buf;
        }

        // For larger K, fall back to in-place select_nth
        let mut rows = rows;
        let actual_k = k.min(rows.len());
        if actual_k > 0 {
            rows.select_nth_unstable_by(actual_k - 1, |a, b| cmp_row(a, b));
            rows.truncate(actual_k);
            rows.sort_by(|a, b| cmp_row(a, b));
            rows.shrink_to_fit();
        }
        rows
    }

    pub(super) fn apply_order_by(
        &self,
        rows: &mut Vec<Row>,
        columns: &[(String, DataType)],
        ob: &ast::OrderBy,
        col_meta: Option<&[ColMeta]>,
        projection: Option<&[SelectItem]>,
        top_k: Option<usize>,
    ) -> Result<(), ExecError> {
        let exprs = match &ob.kind {
            ast::OrderByKind::Expressions(exprs) => exprs,
            _ => return Err(ExecError::Unsupported("ORDER BY ALL".into())),
        };

        // Build sort key descriptors: either a column index or a computed expression
        enum SortKey {
            Column(usize),
            Expr(Box<ast::Expr>),
        }
        let mut sort_keys: Vec<(SortKey, bool, bool)> = Vec::new();
        for ob_expr in exprs {
            let asc = ob_expr.options.asc.unwrap_or(true);
            // PostgreSQL default: NULLS LAST for ASC, NULLS FIRST for DESC
            let nulls_first = ob_expr.options.nulls_first.unwrap_or(!asc);
            match self.resolve_order_by_expr(&ob_expr.expr, columns, col_meta, projection) {
                Ok(col_idx) => sort_keys.push((SortKey::Column(col_idx), asc, nulls_first)),
                Err(_) => sort_keys.push((SortKey::Expr(Box::new(ob_expr.expr.clone())), asc, nulls_first)),
            }
        }

        // Build the actual col_meta for evaluating expressions (use provided or derive from columns)
        let derived_meta: Vec<ColMeta>;
        let effective_meta = match col_meta {
            Some(m) => m,
            None => {
                derived_meta = columns.iter().map(|(name, dtype)| ColMeta {
                    table: None,
                    name: name.clone(),
                    dtype: dtype.clone(),
                }).collect();
                &derived_meta
            }
        };

        // For expression-based sort keys, precompute values for each row
        let needs_eval = sort_keys.iter().any(|(k, _, _)| matches!(k, SortKey::Expr(_)));
        let precomputed: Vec<Vec<Value>> = if needs_eval {
            rows.iter().map(|row| {
                sort_keys.iter().map(|(key, _, _)| match key {
                    SortKey::Column(idx) => row.get(*idx).cloned().unwrap_or(Value::Null),
                    SortKey::Expr(expr) => {
                        self.eval_row_expr(expr, row, effective_meta).unwrap_or(Value::Null)
                    }
                }).collect()
            }).collect()
        } else {
            Vec::new()
        };

        if needs_eval {
            // Sort using precomputed values (zip rows with their sort values)
            let mut indexed: Vec<(usize, &Row)> = rows.iter().enumerate().collect();
            // Top-K optimisation: O(n) partition + O(k log k) sort of prefix
            if let Some(k) = top_k
                && k > 0 && k < indexed.len() {
                    indexed.select_nth_unstable_by(k - 1, |a, b| {
                        for (i, (_, asc, nulls_first)) in sort_keys.iter().enumerate() {
                            let va = &precomputed[a.0][i];
                            let vb = &precomputed[b.0][i];
                            let ord = cmp_with_nulls(va, vb, *asc, *nulls_first);
                            if ord != std::cmp::Ordering::Equal { return ord; }
                        }
                        std::cmp::Ordering::Equal
                    });
                    indexed[..k].sort_by(|a, b| {
                        for (i, (_, asc, nulls_first)) in sort_keys.iter().enumerate() {
                            let va = &precomputed[a.0][i];
                            let vb = &precomputed[b.0][i];
                            let ord = cmp_with_nulls(va, vb, *asc, *nulls_first);
                            if ord != std::cmp::Ordering::Equal { return ord; }
                        }
                        std::cmp::Ordering::Equal
                    });
                    let sorted: Vec<Row> = indexed[..k].iter().map(|(_, r)| (*r).clone()).collect();
                    *rows = sorted;
                    rows.shrink_to_fit();
                    return Ok(());
                }
            indexed.sort_by(|a, b| {
                for (i, (_, asc, nulls_first)) in sort_keys.iter().enumerate() {
                    let va = &precomputed[a.0][i];
                    let vb = &precomputed[b.0][i];
                    let ord = cmp_with_nulls(va, vb, *asc, *nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            let sorted: Vec<Row> = indexed.into_iter().map(|(_, r)| r.clone()).collect();
            *rows = sorted;
        } else {
            // Top-K optimisation: O(n) partition + O(k log k) sort of prefix
            if let Some(k) = top_k
                && k > 0 && k < rows.len() {
                    rows.select_nth_unstable_by(k - 1, |a, b| {
                        for (key, asc, nulls_first) in &sort_keys {
                            if let SortKey::Column(idx) = key {
                                let ord = cmp_with_nulls(
                                    a.get(*idx).unwrap_or(&Value::Null),
                                    b.get(*idx).unwrap_or(&Value::Null),
                                    *asc, *nulls_first,
                                );
                                if ord != std::cmp::Ordering::Equal { return ord; }
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    rows[..k].sort_by(|a, b| {
                        for (key, asc, nulls_first) in &sort_keys {
                            if let SortKey::Column(idx) = key {
                                let ord = cmp_with_nulls(
                                    a.get(*idx).unwrap_or(&Value::Null),
                                    b.get(*idx).unwrap_or(&Value::Null),
                                    *asc, *nulls_first,
                                );
                                if ord != std::cmp::Ordering::Equal { return ord; }
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    rows.truncate(k);
                    rows.shrink_to_fit();
                    return Ok(());
                }
            rows.sort_by(|a, b| {
                for (key, asc, nulls_first) in &sort_keys {
                    if let SortKey::Column(idx) = key {
                        let ord = cmp_with_nulls(&a[*idx], &b[*idx], *asc, *nulls_first);
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        Ok(())
    }

    pub(super) fn resolve_order_by_expr(
        &self,
        expr: &Expr,
        columns: &[(String, DataType)],
        col_meta: Option<&[ColMeta]>,
        projection: Option<&[SelectItem]>,
    ) -> Result<usize, ExecError> {
        match expr {
            Expr::Identifier(ident) => {
                // First, try direct column name match
                if let Some(pos) = columns.iter().position(|(name, _)| name == &ident.value) {
                    return Ok(pos);
                }
                // If not found and we have projection info, check for column aliases
                // in the SELECT list. An alias like `SELECT id AS i` means ORDER BY i
                // should resolve to the column that `id` maps to.
                if let Some(proj) = projection
                    && let Some(meta) = col_meta {
                        for item in proj {
                            if let SelectItem::ExprWithAlias { expr: proj_expr, alias } = item
                                && alias.value == ident.value {
                                    // Found alias match -- resolve the underlying expression
                                    // to a column index in the source columns
                                    match proj_expr {
                                        Expr::Identifier(src_ident) => {
                                            return meta.iter().position(|c| c.name == src_ident.value)
                                                .ok_or_else(|| ExecError::ColumnNotFound(src_ident.value.clone()));
                                        }
                                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                                            let tbl = &parts[0].value;
                                            let col = &parts[1].value;
                                            return meta.iter().position(|c| {
                                                c.table.as_deref() == Some(tbl.as_str()) && c.name == *col
                                            }).ok_or_else(|| ExecError::ColumnNotFound(format!("{tbl}.{col}")));
                                        }
                                        _ => {
                                            // For complex expressions, fall through
                                        }
                                    }
                                }
                        }
                    }
                Err(ExecError::ColumnNotFound(ident.value.clone()))
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let tbl = &parts[0].value;
                let col = &parts[1].value;
                // Try col_meta first (has table info for qualified lookup)
                if let Some(meta) = col_meta
                    && let Some(pos) = meta.iter().position(|c| {
                        c.table.as_deref() == Some(tbl.as_str()) && c.name == *col
                    }) {
                        return Ok(pos);
                    }
                // Fallback: try matching "table.column" as a string against column names
                let qualified = format!("{tbl}.{col}");
                columns
                    .iter()
                    .position(|(name, _)| name == &qualified || name == col.as_str())
                    .ok_or(ExecError::ColumnNotFound(qualified))
            }
            Expr::Value(v) => {
                // ORDER BY 1, 2, 3 (positional)
                if let ast::Value::Number(n, _) = &v.value {
                    let pos: usize = n.parse().map_err(|_| {
                        ExecError::Unsupported(format!("invalid ORDER BY position: {n}"))
                    })?;
                    if pos == 0 || pos > columns.len() {
                        return Err(ExecError::Unsupported(format!(
                            "ORDER BY position {pos} out of range"
                        )));
                    }
                    Ok(pos - 1)
                } else {
                    Err(ExecError::Unsupported(format!("ORDER BY expression: {expr}")))
                }
            }
            _ => Err(ExecError::Unsupported(format!("ORDER BY expression: {expr}"))),
        }
    }

    pub(super) fn apply_limit_offset(
        &self,
        rows: &mut Vec<Row>,
        lc: &ast::LimitClause,
    ) -> Result<(), ExecError> {
        let (limit_expr, offset_expr) = match lc {
            ast::LimitClause::LimitOffset {
                limit, offset, ..
            } => (limit.as_ref(), offset.as_ref().map(|o| &o.value)),
            ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                (Some(limit), Some(offset))
            }
        };

        let offset = match offset_expr {
            Some(expr) => self.expr_to_usize(expr)?,
            None => 0,
        };

        if offset > 0 {
            if offset >= rows.len() {
                rows.clear();
            } else {
                *rows = rows.split_off(offset);
            }
        }

        if let Some(expr) = limit_expr {
            let limit = self.expr_to_usize(expr)?;
            rows.truncate(limit);
        }

        Ok(())
    }

    pub(super) fn expr_to_usize(&self, expr: &Expr) -> Result<usize, ExecError> {
        let val = self.eval_const_expr(expr)?;
        match val {
            Value::Int32(n) if n >= 0 => Ok(n as usize),
            Value::Int64(n) if n >= 0 => Ok(n as usize),
            _ => Err(ExecError::Unsupported("LIMIT/OFFSET must be non-negative integer".into())),
        }
    }

    /// Extract the top-K row count needed for ORDER BY + LIMIT optimisation.
    /// Returns `Some(limit + offset)` when the clause has a static integer limit,
    /// so ORDER BY can stop materialising after that many rows.
    pub(super) fn extract_top_k(&self, limit_clause: Option<&ast::LimitClause>) -> Option<usize> {
        let lc = limit_clause?;
        let (limit_expr, offset_expr) = match lc {
            ast::LimitClause::LimitOffset { limit, offset, .. } => {
                (limit.as_ref(), offset.as_ref().map(|o| &o.value))
            }
            ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                (Some(limit), Some(offset))
            }
        };
        let limit = self.expr_to_usize(limit_expr?).ok()?;
        let offset = offset_expr
            .and_then(|e| self.expr_to_usize(e).ok())
            .unwrap_or(0);
        Some(limit + offset)
    }
    // ========================================================================
    // Plan reuse: transplant expressions from current query into cached plan
    // ========================================================================

    /// Try to reuse a cached plan by transplanting the current query's WHERE
    /// clause expressions into the cached plan structure. This avoids re-planning
    /// (~500-1000ns savings). Returns None if transplanting isn't possible
    /// (JOINs, complex predicate splits, etc.), in which case the caller
    /// should fall back to full re-planning.
    fn try_reuse_plan(
        cached_plan: &planner::PlanNode,
        select: &ast::Select,
    ) -> Option<planner::PlanNode> {
        // Only handle single-table queries (no JOINs)
        if select.from.len() != 1 || !select.from.first().map_or(true, |f| f.joins.is_empty()) {
            return None;
        }
        let mut plan = cached_plan.clone();
        let where_expr = select.selection.clone();
        if Self::transplant_scan_exprs(&mut plan, &where_expr) {
            Some(plan)
        } else {
            None
        }
    }

    /// Recursively traverse the plan tree to find the leaf scan node and
    /// transplant the current WHERE clause expressions into it.
    /// Returns true if transplanting succeeded.
    fn transplant_scan_exprs(
        plan: &mut planner::PlanNode,
        where_expr: &Option<ast::Expr>,
    ) -> bool {
        match plan {
            // Leaf: SeqScan — replace filter with current WHERE
            planner::PlanNode::SeqScan { filter, filter_expr, .. } => {
                *filter_expr = where_expr.clone();
                *filter = where_expr.as_ref().map(|e| e.to_string());
                true
            }
            // Leaf: IndexScan with equality lookup — replace lookup key
            planner::PlanNode::IndexScan {
                lookup_key, lookup_key_expr,
                range_lo, range_lo_expr, range_hi, range_hi_expr,
                range_predicate, range_predicate_expr, ..
            } => {
                let expr = match where_expr {
                    Some(e) => e,
                    None => return false,
                };
                if lookup_key_expr.is_some() {
                    // Equality IndexScan: the WHERE is the lookup predicate
                    *lookup_key_expr = Some(expr.clone());
                    *lookup_key = Some(expr.to_string());
                    true
                } else if range_lo_expr.is_some() {
                    // Range IndexScan: extract bounds from BETWEEN
                    if let ast::Expr::Between { low, high, .. } = expr {
                        *range_lo_expr = Some(*low.clone());
                        *range_lo = Some(low.to_string());
                        *range_hi_expr = Some(*high.clone());
                        *range_hi = Some(high.to_string());
                        *range_predicate_expr = Some(expr.clone());
                        *range_predicate = Some(expr.to_string());
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            // Filter wrapping a scan — transplant child first, then
            // extract remaining predicates for the filter.
            planner::PlanNode::Filter {
                input, predicate, predicate_expr, ..
            } => {
                let full_where = match where_expr {
                    Some(e) => e,
                    None => return false,
                };
                // Split the WHERE into conjunction predicates
                let preds: Vec<&ast::Expr> = planner::split_conjunction(full_where);
                if preds.len() < 2 {
                    // Single predicate but plan has Filter+Scan — structure mismatch
                    return false;
                }
                // Determine which predicate the child scan consumed.
                // For IndexScan: the index column name tells us which predicate matches.
                let scan_pred_idx = if let planner::PlanNode::IndexScan { .. } = input.as_ref() {
                    // Find the predicate that references the indexed column
                    let idx_col = {
                        // Index name format is typically "idx_<table>_<col>" or user-defined.
                        // We check each predicate against the index name stored in the plan.
                        // The simplest heuristic: the planner picked this index for a reason,
                        // so one of the predicates should match.
                        let mut found = None;
                        for (i, p) in preds.iter().enumerate() {
                            // Check if this predicate is an equality or range on the
                            // index's column by checking the predicate text contains
                            // the index name's implied column.
                            // Use planner's extract helpers for accuracy.
                            if let Some((col, _)) = planner::is_equality_predicate(p) {
                                // Match against the cached IndexScan's column
                                if let planner::PlanNode::IndexScan { lookup_key_expr: Some(old_expr), .. } = input.as_ref() {
                                    if let Some((old_col, _)) = planner::is_equality_predicate(old_expr) {
                                        if col.eq_ignore_ascii_case(&old_col) {
                                            found = Some(i);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        found
                    };
                    idx_col
                } else {
                    None
                };

                let scan_idx = match scan_pred_idx {
                    Some(i) => i,
                    None => return false, // Can't determine which predicate the scan uses
                };

                // Transplant the scan predicate into the child
                let scan_where = Some(preds[scan_idx].clone());
                if !Self::transplant_scan_exprs(input, &scan_where) {
                    return false;
                }

                // Remaining predicates go to the Filter
                let remaining: Vec<ast::Expr> = preds.iter().enumerate()
                    .filter(|(i, _)| *i != scan_idx)
                    .map(|(_, p)| (*p).clone())
                    .collect();
                let combined = remaining.into_iter()
                    .reduce(|a, b| ast::Expr::BinaryOp {
                        left: Box::new(a),
                        op: ast::BinaryOperator::And,
                        right: Box::new(b),
                    })
                    .unwrap();
                *predicate_expr = Some(combined.clone());
                *predicate = combined.to_string();
                true
            }
            // Wrapper nodes: recurse into input
            planner::PlanNode::Sort { input, .. }
            | planner::PlanNode::Limit { input, .. }
            | planner::PlanNode::Project { input, .. }
            | planner::PlanNode::HashAggregate { input, .. }
            | planner::PlanNode::Aggregate { input, .. } => {
                Self::transplant_scan_exprs(input, where_expr)
            }
            // JOINs: don't try to reuse
            _ => false,
        }
    }

    // ========================================================================
    // SQL normalizer for plan-cache keys
    // ========================================================================

    /// Normalize a SQL string for use as a plan-cache key by replacing literal
    /// values with placeholders. Numeric literals become `$N` and string literals
    /// become `$S`. This allows queries that differ only in literal values (e.g.
    /// `WHERE id = 500` vs `WHERE id = 501`) to share a cache entry.
    ///
    /// The normalizer is a simple character-by-character scanner — not a full SQL
    /// parser. It avoids replacing tokens inside double-quoted identifiers or
    /// backtick-quoted identifiers. SQL keywords (NULL, TRUE, FALSE) are left
    /// intact since they are not numeric/string literals.
    pub(super) fn normalize_sql_for_cache(sql: &str) -> String {
        let bytes = sql.as_bytes();
        let len = bytes.len();
        let mut out = String::with_capacity(len);
        let mut i = 0;

        while i < len {
            let ch = bytes[i];

            // ── Skip double-quoted identifiers ────────────────────────
            if ch == b'"' {
                out.push('"');
                i += 1;
                while i < len {
                    out.push(bytes[i] as char);
                    if bytes[i] == b'"' {
                        i += 1;
                        // Handle escaped double-quote ("")
                        if i < len && bytes[i] == b'"' {
                            out.push('"');
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                continue;
            }

            // ── Skip backtick-quoted identifiers ──────────────────────
            if ch == b'`' {
                out.push('`');
                i += 1;
                while i < len {
                    out.push(bytes[i] as char);
                    if bytes[i] == b'`' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }

            // ── Replace string literals ('...') with $S ───────────────
            if ch == b'\'' {
                // Consume entire string including escaped single-quotes ('')
                i += 1;
                loop {
                    if i >= len {
                        break;
                    }
                    if bytes[i] == b'\'' {
                        i += 1;
                        // Escaped quote ''
                        if i < len && bytes[i] == b'\'' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                out.push_str("$S");
                continue;
            }

            // ── Replace numeric literals with $N ──────────────────────
            // A numeric literal starts with a digit and is NOT preceded by an
            // alphanumeric or underscore character (to avoid replacing parts of
            // identifiers like "col1").
            if ch.is_ascii_digit() {
                // Check the character before to make sure we're not mid-identifier
                let prev_is_ident = if out.is_empty() {
                    false
                } else {
                    let prev = out.as_bytes()[out.len() - 1];
                    prev.is_ascii_alphanumeric() || prev == b'_'
                };

                if prev_is_ident {
                    // Part of an identifier — emit literally
                    out.push(ch as char);
                    i += 1;
                    continue;
                }

                // Consume the full number (digits, optional dot, more digits)
                i += 1;
                let mut saw_dot = false;
                while i < len {
                    if bytes[i].is_ascii_digit() {
                        i += 1;
                    } else if bytes[i] == b'.' && !saw_dot {
                        // Only treat as decimal if followed by a digit
                        if i + 1 < len && bytes[i + 1].is_ascii_digit() {
                            saw_dot = true;
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                out.push_str("$N");
                continue;
            }

            // ── Everything else: copy through ─────────────────────────
            out.push(ch as char);
            i += 1;
        }

        out
    }
    // ========================================================================
    // AST cache: normalize + extract literals, substitute into cloned AST
    // ========================================================================

    /// Normalize a SQL string and simultaneously extract the literal values
    /// that were replaced. Returns `(normalized_key, ordered_literals)`.
    /// The literals are extracted in left-to-right order matching the SQL text,
    /// which corresponds to DFS order in the parsed AST.
    pub(super) fn normalize_sql_with_literals(sql: &str) -> (String, Vec<CacheLiteral>) {
        let bytes = sql.as_bytes();
        let len = bytes.len();
        let mut out = String::with_capacity(len);
        let mut literals = Vec::new();
        let mut i = 0;

        while i < len {
            let ch = bytes[i];

            // ── Skip double-quoted identifiers ────────────────────────
            if ch == b'"' {
                out.push('"');
                i += 1;
                while i < len {
                    out.push(bytes[i] as char);
                    if bytes[i] == b'"' {
                        i += 1;
                        if i < len && bytes[i] == b'"' {
                            out.push('"');
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                continue;
            }

            // ── Skip backtick-quoted identifiers ──────────────────────
            if ch == b'`' {
                out.push('`');
                i += 1;
                while i < len {
                    out.push(bytes[i] as char);
                    if bytes[i] == b'`' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }

            // ── Replace string literals ('...') with $S ───────────────
            if ch == b'\'' {
                i += 1;
                let mut value = String::new();
                loop {
                    if i >= len {
                        break;
                    }
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < len && bytes[i] == b'\'' {
                            value.push('\'');
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    value.push(bytes[i] as char);
                    i += 1;
                }
                literals.push(CacheLiteral::String(value));
                out.push_str("$S");
                continue;
            }

            // ── Replace numeric literals with $N ──────────────────────
            if ch.is_ascii_digit() {
                let prev_is_ident = if out.is_empty() {
                    false
                } else {
                    let prev = out.as_bytes()[out.len() - 1];
                    prev.is_ascii_alphanumeric() || prev == b'_'
                };

                if prev_is_ident {
                    out.push(ch as char);
                    i += 1;
                    continue;
                }

                let start = i;
                i += 1;
                let mut saw_dot = false;
                while i < len {
                    if bytes[i].is_ascii_digit() {
                        i += 1;
                    } else if bytes[i] == b'.' && !saw_dot {
                        if i + 1 < len && bytes[i + 1].is_ascii_digit() {
                            saw_dot = true;
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                let num_str = std::str::from_utf8(&bytes[start..i]).unwrap_or("0").to_string();
                literals.push(CacheLiteral::Number(num_str));
                out.push_str("$N");
                continue;
            }

            out.push(ch as char);
            i += 1;
        }

        (out, literals)
    }

    /// Substitute literal values into a cloned AST in DFS order.
    /// Returns the number of substitutions made. If this doesn't match
    /// `literals.len()`, the caller should fall back to re-parsing.
    pub(super) fn substitute_ast_literals(
        stmts: &mut [sqlparser::ast::Statement],
        literals: &[CacheLiteral],
    ) -> usize {
        let mut iter = literals.iter();
        let mut count = 0usize;
        for stmt in stmts.iter_mut() {
            Self::substitute_stmt_literals(stmt, &mut iter, &mut count);
        }
        count
    }

    fn substitute_stmt_literals(
        stmt: &mut sqlparser::ast::Statement,
        lits: &mut std::slice::Iter<'_, CacheLiteral>,
        count: &mut usize,
    ) {
        use sqlparser::ast::Statement;
        match stmt {
            Statement::Query(query) => {
                Self::substitute_query_literals(query, lits, count);
            }
            Statement::Insert(insert) => {
                // Source body (VALUES or subquery)
                if let Some(ref mut source) = insert.source {
                    Self::substitute_query_literals(source, lits, count);
                }
                // ON CONFLICT ... DO UPDATE SET ...
                if let Some(ref mut on_conflict) = insert.on {
                    match on_conflict {
                        sqlparser::ast::OnInsert::OnConflict(oc) => {
                            if let sqlparser::ast::OnConflictAction::DoUpdate(du) = &mut oc.action {
                                for assign in &mut du.assignments {
                                    Self::substitute_expr_literals(&mut assign.value, lits, count);
                                }
                                if let Some(ref mut sel) = du.selection {
                                    Self::substitute_expr_literals(sel, lits, count);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                // RETURNING expressions
                if let Some(ref mut returning) = insert.returning {
                    for item in returning {
                        Self::substitute_select_item_literals(item, lits, count);
                    }
                }
            }
            Statement::Update(update) => {
                for assign in &mut update.assignments {
                    Self::substitute_expr_literals(&mut assign.value, lits, count);
                }
                if let Some(ref mut sel) = update.selection {
                    Self::substitute_expr_literals(sel, lits, count);
                }
                if let Some(ref mut returning) = update.returning {
                    for item in returning {
                        Self::substitute_select_item_literals(item, lits, count);
                    }
                }
            }
            Statement::Delete(delete) => {
                if let Some(ref mut sel) = delete.selection {
                    Self::substitute_expr_literals(sel, lits, count);
                }
                if let Some(ref mut returning) = delete.returning {
                    for item in returning {
                        Self::substitute_select_item_literals(item, lits, count);
                    }
                }
            }
            // For other statement types, we don't substitute — count may mismatch
            // and the caller will fall back to re-parse.
            _ => {}
        }
    }

    fn substitute_query_literals(
        query: &mut sqlparser::ast::Query,
        lits: &mut std::slice::Iter<'_, CacheLiteral>,
        count: &mut usize,
    ) {
        // CTEs
        if let Some(ref mut with) = query.with {
            for cte in &mut with.cte_tables {
                Self::substitute_query_literals(&mut cte.query, lits, count);
            }
        }
        // Body
        Self::substitute_set_expr_literals(&mut query.body, lits, count);
        // ORDER BY
        if let Some(ref mut order_by) = query.order_by {
            if let ast::OrderByKind::Expressions(ref mut exprs) = order_by.kind {
                for item in exprs {
                    Self::substitute_expr_literals(&mut item.expr, lits, count);
                }
            }
        }
        // LIMIT / OFFSET
        if let Some(ref mut limit_clause) = query.limit_clause {
            match limit_clause {
                sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. } => {
                    if let Some(l) = limit {
                        Self::substitute_expr_literals(l, lits, count);
                    }
                    if let Some(o) = offset {
                        Self::substitute_expr_literals(&mut o.value, lits, count);
                    }
                }
                _ => {}
            }
        }
    }

    fn substitute_set_expr_literals(
        body: &mut sqlparser::ast::SetExpr,
        lits: &mut std::slice::Iter<'_, CacheLiteral>,
        count: &mut usize,
    ) {
        use sqlparser::ast::SetExpr;
        match body {
            SetExpr::Select(select) => {
                // Projection
                for item in &mut select.projection {
                    Self::substitute_select_item_literals(item, lits, count);
                }
                // FROM (join conditions)
                for table_with_joins in &mut select.from {
                    Self::substitute_table_factor_literals(&mut table_with_joins.relation, lits, count);
                    for join in &mut table_with_joins.joins {
                        Self::substitute_table_factor_literals(&mut join.relation, lits, count);
                        // Extract join constraint from the JoinOperator variant
                        let constraint = match &mut join.join_operator {
                            ast::JoinOperator::Join(c)
                            | ast::JoinOperator::Inner(c)
                            | ast::JoinOperator::Left(c)
                            | ast::JoinOperator::LeftOuter(c)
                            | ast::JoinOperator::Right(c)
                            | ast::JoinOperator::RightOuter(c)
                            | ast::JoinOperator::FullOuter(c) => Some(c),
                            _ => None,
                        };
                        if let Some(ast::JoinConstraint::On(expr)) = constraint {
                            Self::substitute_expr_literals(expr, lits, count);
                        }
                    }
                }
                // WHERE
                if let Some(ref mut sel) = select.selection {
                    Self::substitute_expr_literals(sel, lits, count);
                }
                // GROUP BY
                match &mut select.group_by {
                    sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
                        for expr in exprs {
                            Self::substitute_expr_literals(expr, lits, count);
                        }
                    }
                    _ => {}
                }
                // HAVING
                if let Some(ref mut having) = select.having {
                    Self::substitute_expr_literals(having, lits, count);
                }
            }
            SetExpr::Values(values) => {
                for row in &mut values.rows {
                    for expr in row {
                        Self::substitute_expr_literals(expr, lits, count);
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                Self::substitute_set_expr_literals(left, lits, count);
                Self::substitute_set_expr_literals(right, lits, count);
            }
            SetExpr::Query(q) => {
                Self::substitute_query_literals(q, lits, count);
            }
            _ => {}
        }
    }

    fn substitute_table_factor_literals(
        tf: &mut sqlparser::ast::TableFactor,
        lits: &mut std::slice::Iter<'_, CacheLiteral>,
        count: &mut usize,
    ) {
        match tf {
            sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                Self::substitute_query_literals(subquery, lits, count);
            }
            _ => {}
        }
    }

    fn substitute_select_item_literals(
        item: &mut sqlparser::ast::SelectItem,
        lits: &mut std::slice::Iter<'_, CacheLiteral>,
        count: &mut usize,
    ) {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                Self::substitute_expr_literals(expr, lits, count);
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                Self::substitute_expr_literals(expr, lits, count);
            }
            _ => {}
        }
    }

    /// Core expression literal substitution — DFS walk matching text order.
    fn substitute_expr_literals(
        expr: &mut sqlparser::ast::Expr,
        lits: &mut std::slice::Iter<'_, CacheLiteral>,
        count: &mut usize,
    ) {
        use sqlparser::ast::{Expr, Value as AstValue};
        match expr {
            Expr::Value(val_with_span) => {
                match &mut val_with_span.value {
                    AstValue::Number(s, _) => {
                        if let Some(CacheLiteral::Number(n)) = lits.next() {
                            *s = n.clone();
                            *count += 1;
                        }
                    }
                    AstValue::SingleQuotedString(s) => {
                        if let Some(CacheLiteral::String(v)) = lits.next() {
                            *s = v.clone();
                            *count += 1;
                        }
                    }
                    _ => {} // NULL, Bool, etc. — not normalized by our scanner
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::substitute_expr_literals(left, lits, count);
                Self::substitute_expr_literals(right, lits, count);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
            }
            Expr::Between { expr: inner, low, high, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
                Self::substitute_expr_literals(low, lits, count);
                Self::substitute_expr_literals(high, lits, count);
            }
            Expr::InList { expr: inner, list, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
                for item in list {
                    Self::substitute_expr_literals(item, lits, count);
                }
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                Self::substitute_expr_literals(inner, lits, count);
            }
            Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
                Self::substitute_expr_literals(a, lits, count);
                Self::substitute_expr_literals(b, lits, count);
            }
            Expr::Nested(inner) => {
                Self::substitute_expr_literals(inner, lits, count);
            }
            Expr::Cast { expr: inner, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
            }
            Expr::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    Self::substitute_expr_literals(op, lits, count);
                }
                // Each CaseWhen has condition + result — interleave to match SQL text order:
                // CASE WHEN <cond1> THEN <result1> WHEN <cond2> THEN <result2> ELSE <else> END
                for case_when in conditions.iter_mut() {
                    Self::substitute_expr_literals(&mut case_when.condition, lits, count);
                    Self::substitute_expr_literals(&mut case_when.result, lits, count);
                }
                if let Some(el) = else_result {
                    Self::substitute_expr_literals(el, lits, count);
                }
            }
            Expr::Function(func) => {
                match &mut func.args {
                    sqlparser::ast::FunctionArguments::List(args) => {
                        for arg in &mut args.args {
                            match arg {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(e),
                                ) => {
                                    Self::substitute_expr_literals(e, lits, count);
                                }
                                sqlparser::ast::FunctionArg::Named {
                                    arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                                    ..
                                } => {
                                    Self::substitute_expr_literals(e, lits, count);
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
                // FILTER clause
                if let Some(ref mut filter) = func.filter {
                    Self::substitute_expr_literals(filter, lits, count);
                }
                // OVER clause (window functions)
                if let Some(ref mut over) = func.over {
                    match over {
                        sqlparser::ast::WindowType::WindowSpec(ws) => {
                            for expr in &mut ws.partition_by {
                                Self::substitute_expr_literals(expr, lits, count);
                            }
                            for ob in &mut ws.order_by {
                                Self::substitute_expr_literals(&mut ob.expr, lits, count);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Expr::Subquery(query) => {
                Self::substitute_query_literals(query, lits, count);
            }
            Expr::InSubquery { expr: inner, subquery, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
                Self::substitute_query_literals(subquery, lits, count);
            }
            Expr::Exists { subquery, .. } => {
                Self::substitute_query_literals(subquery, lits, count);
            }
            Expr::Like { expr: inner, pattern, .. }
            | Expr::ILike { expr: inner, pattern, .. }
            | Expr::SimilarTo { expr: inner, pattern, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
                Self::substitute_expr_literals(pattern, lits, count);
            }
            Expr::Tuple(exprs) => {
                for e in exprs {
                    Self::substitute_expr_literals(e, lits, count);
                }
            }
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                Self::substitute_expr_literals(left, lits, count);
                Self::substitute_expr_literals(right, lits, count);
            }
            Expr::Extract { expr: inner, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
            }
            Expr::Position { expr: inner, r#in: in_expr, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
                Self::substitute_expr_literals(in_expr, lits, count);
            }
            Expr::Substring { expr: inner, substring_from, substring_for, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
                if let Some(f) = substring_from {
                    Self::substitute_expr_literals(f, lits, count);
                }
                if let Some(f) = substring_for {
                    Self::substitute_expr_literals(f, lits, count);
                }
            }
            Expr::Trim { expr: inner, trim_what, .. } => {
                Self::substitute_expr_literals(inner, lits, count);
                if let Some(tw) = trim_what {
                    Self::substitute_expr_literals(tw, lits, count);
                }
            }
            // Identifiers, wildcards, column refs — no literals to substitute
            _ => {}
        }
    }

    /// Parse SQL with AST caching. On cache hit, clones the cached AST and
    /// substitutes literal values via DFS walk (~5-10x faster than re-parsing).
    /// Falls back to full parse on count mismatch or unsupported statement types.
    pub(super) fn parse_with_ast_cache(
        &self,
        sql: &str,
    ) -> Result<Vec<sqlparser::ast::Statement>, crate::sql::ParseError> {
        let (norm_key, literals) = Self::normalize_sql_with_literals(sql);

        // Try cache lookup
        let cache_result = self.ast_cache.write().get(&norm_key);
        if let Some((cached_arc, expected_count)) = cache_result {
            if expected_count == literals.len() {
                if literals.is_empty() {
                    // No literals to substitute — return deep clone directly
                    let stmts = (*cached_arc).clone();
                    // Store hint for plan cache key (avoids query.to_string() + re-normalize)
                    if stmts.len() == 1 {
                        *self.plan_cache_key_hint.lock() = Some(norm_key);
                    }
                    return Ok(stmts);
                }
                let mut cloned = (*cached_arc).clone();
                let subs = Self::substitute_ast_literals(&mut cloned, &literals);
                if subs == expected_count {
                    // Store hint for plan cache key
                    if cloned.len() == 1 {
                        *self.plan_cache_key_hint.lock() = Some(norm_key);
                    }
                    return Ok(cloned);
                }
                // Substitution count mismatch — fall through to re-parse
            }
            // Literal count mismatch — fall through to re-parse
        }

        // Cache miss or mismatch — parse and cache
        let ast = crate::sql::parse(sql)?;
        // Store hint for plan cache key
        if ast.len() == 1 {
            *self.plan_cache_key_hint.lock() = Some(norm_key.clone());
        }
        self.ast_cache.write().insert(norm_key, ast.clone(), literals.len());
        Ok(ast)
    }
} // end impl Executor

// ========================================================================
// Unit tests for normalize_sql_for_cache
// ========================================================================

#[cfg(test)]
mod normalize_tests {
    use super::*;

    #[test]
    fn test_normalize_integer_literal() {
        let sql = "SELECT * FROM users WHERE id = 500";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM users WHERE id = $N");
    }

    #[test]
    fn test_normalize_different_integers_same_key() {
        let a = Executor::normalize_sql_for_cache("SELECT * FROM users WHERE id = 500");
        let b = Executor::normalize_sql_for_cache("SELECT * FROM users WHERE id = 501");
        assert_eq!(a, b);
    }

    #[test]
    fn test_normalize_string_literal() {
        let sql = "SELECT * FROM t WHERE name = 'hello' AND age = 25";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM t WHERE name = $S AND age = $N");
    }

    #[test]
    fn test_normalize_float_literal() {
        let sql = "SELECT * FROM t WHERE price > 19.99";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM t WHERE price > $N");
    }

    #[test]
    fn test_normalize_multiple_literals() {
        let sql = "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 'x'";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM t WHERE a = $N AND b = $N AND c = $S");
    }

    #[test]
    fn test_normalize_preserves_identifiers() {
        // col1, table2 should NOT have their digits replaced
        let sql = "SELECT col1 FROM table2 WHERE id = 42";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT col1 FROM table2 WHERE id = $N");
    }

    #[test]
    fn test_normalize_preserves_double_quoted_idents() {
        let sql = r#"SELECT "my col" FROM t WHERE id = 10"#;
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, r#"SELECT "my col" FROM t WHERE id = $N"#);
    }

    #[test]
    fn test_normalize_preserves_backtick_idents() {
        let sql = "SELECT `col` FROM t WHERE x = 5";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT `col` FROM t WHERE x = $N");
    }

    #[test]
    fn test_normalize_escaped_string_literal() {
        let sql = "SELECT * FROM t WHERE name = 'it''s'";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM t WHERE name = $S");
    }

    #[test]
    fn test_normalize_no_literals() {
        let sql = "SELECT * FROM users";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM users");
    }

    #[test]
    fn test_normalize_count_star() {
        // COUNT(*) should be unchanged
        let sql = "SELECT COUNT(*) FROM users";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT COUNT(*) FROM users");
    }

    #[test]
    fn test_normalize_insert_values() {
        let sql = "INSERT INTO t VALUES (1, 'a', 3.14)";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "INSERT INTO t VALUES ($N, $S, $N)");
    }

    #[test]
    fn test_normalize_negative_not_special() {
        // The minus sign is just an operator; the number after it gets normalized
        let sql = "SELECT * FROM t WHERE x = -5";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM t WHERE x = -$N");
    }

    #[test]
    fn test_normalize_limit_offset() {
        let sql = "SELECT * FROM t LIMIT 10 OFFSET 20";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM t LIMIT $N OFFSET $N");
    }

    #[test]
    fn test_normalize_between() {
        let sql = "SELECT * FROM t WHERE x BETWEEN 10 AND 20";
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, "SELECT * FROM t WHERE x BETWEEN $N AND $N");
    }

    #[test]
    fn test_normalize_double_quoted_with_number() {
        // Numbers inside double-quoted identifiers should NOT be replaced
        let sql = r#"SELECT "col123" FROM t WHERE "col123" = 99"#;
        let norm = Executor::normalize_sql_for_cache(sql);
        assert_eq!(norm, r#"SELECT "col123" FROM t WHERE "col123" = $N"#);
    }
}

// ========================================================================
// Unit tests for normalize_sql_with_literals and AST literal substitution
// ========================================================================

#[cfg(test)]
mod ast_cache_tests {
    use super::*;

    #[test]
    fn test_extract_literals_integer() {
        let (norm, lits) = Executor::normalize_sql_with_literals("SELECT * FROM t WHERE id = 42");
        assert_eq!(norm, "SELECT * FROM t WHERE id = $N");
        assert_eq!(lits.len(), 1);
        assert!(matches!(&lits[0], CacheLiteral::Number(n) if n == "42"));
    }

    #[test]
    fn test_extract_literals_string() {
        let (norm, lits) = Executor::normalize_sql_with_literals("SELECT * FROM t WHERE name = 'hello'");
        assert_eq!(norm, "SELECT * FROM t WHERE name = $S");
        assert_eq!(lits.len(), 1);
        assert!(matches!(&lits[0], CacheLiteral::String(s) if s == "hello"));
    }

    #[test]
    fn test_extract_literals_mixed() {
        let (norm, lits) = Executor::normalize_sql_with_literals(
            "INSERT INTO t VALUES (1, 'abc', 3.14)"
        );
        assert_eq!(norm, "INSERT INTO t VALUES ($N, $S, $N)");
        assert_eq!(lits.len(), 3);
        assert!(matches!(&lits[0], CacheLiteral::Number(n) if n == "1"));
        assert!(matches!(&lits[1], CacheLiteral::String(s) if s == "abc"));
        assert!(matches!(&lits[2], CacheLiteral::Number(n) if n == "3.14"));
    }

    #[test]
    fn test_extract_literals_no_literals() {
        let (norm, lits) = Executor::normalize_sql_with_literals("SELECT * FROM users");
        assert_eq!(norm, "SELECT * FROM users");
        assert!(lits.is_empty());
    }

    #[test]
    fn test_extract_literals_escaped_string() {
        let (norm, lits) = Executor::normalize_sql_with_literals("SELECT * FROM t WHERE x = 'it''s'");
        assert_eq!(norm, "SELECT * FROM t WHERE x = $S");
        assert_eq!(lits.len(), 1);
        assert!(matches!(&lits[0], CacheLiteral::String(s) if s == "it's"));
    }

    #[test]
    fn test_extract_preserves_identifiers() {
        let (norm, lits) = Executor::normalize_sql_with_literals("SELECT col1 FROM table2 WHERE id = 7");
        assert_eq!(norm, "SELECT col1 FROM table2 WHERE id = $N");
        assert_eq!(lits.len(), 1);
    }

    #[test]
    fn test_substitute_select_where() {
        // Parse: SELECT * FROM t WHERE id = 1
        let mut stmts = crate::sql::parse("SELECT * FROM t WHERE id = 1").unwrap();
        let new_lits = vec![CacheLiteral::Number("99".to_string())];
        let subs = Executor::substitute_ast_literals(&mut stmts, &new_lits);
        assert_eq!(subs, 1);
        // Verify the AST now contains 99
        let sql_out = stmts[0].to_string();
        assert!(sql_out.contains("99"), "Expected '99' in: {sql_out}");
        assert!(!sql_out.contains(" 1"), "Should not contain ' 1' in: {sql_out}");
    }

    #[test]
    fn test_substitute_insert_values() {
        let mut stmts = crate::sql::parse("INSERT INTO t (a, b) VALUES (10, 'hello')").unwrap();
        let new_lits = vec![
            CacheLiteral::Number("20".to_string()),
            CacheLiteral::String("world".to_string()),
        ];
        let subs = Executor::substitute_ast_literals(&mut stmts, &new_lits);
        assert_eq!(subs, 2);
        let sql_out = stmts[0].to_string();
        assert!(sql_out.contains("20"), "Expected '20' in: {sql_out}");
        assert!(sql_out.contains("world"), "Expected 'world' in: {sql_out}");
    }

    #[test]
    fn test_substitute_update_set() {
        let mut stmts = crate::sql::parse("UPDATE t SET x = 5 WHERE id = 1").unwrap();
        let new_lits = vec![
            CacheLiteral::Number("50".to_string()),
            CacheLiteral::Number("99".to_string()),
        ];
        let subs = Executor::substitute_ast_literals(&mut stmts, &new_lits);
        assert_eq!(subs, 2);
        let sql_out = stmts[0].to_string();
        assert!(sql_out.contains("50"), "Expected '50' in: {sql_out}");
        assert!(sql_out.contains("99"), "Expected '99' in: {sql_out}");
    }

    #[test]
    fn test_substitute_between() {
        let mut stmts = crate::sql::parse("SELECT * FROM t WHERE x BETWEEN 10 AND 20").unwrap();
        let new_lits = vec![
            CacheLiteral::Number("100".to_string()),
            CacheLiteral::Number("200".to_string()),
        ];
        let subs = Executor::substitute_ast_literals(&mut stmts, &new_lits);
        assert_eq!(subs, 2);
        let sql_out = stmts[0].to_string();
        assert!(sql_out.contains("100"), "Expected '100' in: {sql_out}");
        assert!(sql_out.contains("200"), "Expected '200' in: {sql_out}");
    }

    #[test]
    fn test_substitute_in_list() {
        let mut stmts = crate::sql::parse("SELECT * FROM t WHERE id IN (1, 2, 3)").unwrap();
        let new_lits = vec![
            CacheLiteral::Number("10".to_string()),
            CacheLiteral::Number("20".to_string()),
            CacheLiteral::Number("30".to_string()),
        ];
        let subs = Executor::substitute_ast_literals(&mut stmts, &new_lits);
        assert_eq!(subs, 3);
        let sql_out = stmts[0].to_string();
        assert!(sql_out.contains("10"), "Expected '10' in: {sql_out}");
        assert!(sql_out.contains("20"), "Expected '20' in: {sql_out}");
        assert!(sql_out.contains("30"), "Expected '30' in: {sql_out}");
    }

    #[test]
    fn test_substitute_function_args() {
        let mut stmts = crate::sql::parse("SELECT ROUND(3.14, 1) FROM t").unwrap();
        let new_lits = vec![
            CacheLiteral::Number("2.718".to_string()),
            CacheLiteral::Number("2".to_string()),
        ];
        let subs = Executor::substitute_ast_literals(&mut stmts, &new_lits);
        assert_eq!(subs, 2);
        let sql_out = stmts[0].to_string();
        assert!(sql_out.contains("2.718"), "Expected '2.718' in: {sql_out}");
    }

    #[test]
    fn test_substitute_limit_offset() {
        let mut stmts = crate::sql::parse("SELECT * FROM t LIMIT 10 OFFSET 20").unwrap();
        let new_lits = vec![
            CacheLiteral::Number("50".to_string()),
            CacheLiteral::Number("100".to_string()),
        ];
        let subs = Executor::substitute_ast_literals(&mut stmts, &new_lits);
        assert_eq!(subs, 2);
        let sql_out = stmts[0].to_string();
        assert!(sql_out.contains("50"), "Expected '50' in: {sql_out}");
        assert!(sql_out.contains("100"), "Expected '100' in: {sql_out}");
    }

    #[test]
    fn test_substitute_no_literals() {
        let mut stmts = crate::sql::parse("SELECT COUNT(*) FROM users").unwrap();
        let new_lits: Vec<CacheLiteral> = vec![];
        let subs = Executor::substitute_ast_literals(&mut stmts, &new_lits);
        assert_eq!(subs, 0);
    }

    #[test]
    fn test_normalize_with_literals_matches_normalizer() {
        // Verify normalize_sql_with_literals produces the same key as normalize_sql_for_cache
        let sqls = &[
            "SELECT * FROM t WHERE id = 42",
            "INSERT INTO t VALUES (1, 'hello', 3.14)",
            "SELECT COUNT(*) FROM users",
            "UPDATE t SET x = 5 WHERE y = 'abc'",
        ];
        for sql in sqls {
            let norm_old = Executor::normalize_sql_for_cache(sql);
            let (norm_new, _) = Executor::normalize_sql_with_literals(sql);
            assert_eq!(norm_old, norm_new, "Mismatch for: {sql}");
        }
    }

    /// End-to-end test: parse once, cache, parse again with different literals.
    /// The second parse should produce a correctly substituted AST.
    #[test]
    fn test_round_trip_different_literals() {
        let sql1 = "SELECT * FROM t WHERE id = 42 AND name = 'alice'";
        let sql2 = "SELECT * FROM t WHERE id = 99 AND name = 'bob'";

        // Parse both normally
        let stmts1 = crate::sql::parse(sql1).unwrap();
        let stmts2 = crate::sql::parse(sql2).unwrap();

        // Simulate cache: parse sql1 into template, substitute with sql2's literals
        let mut template = stmts1.clone();
        let (_, lits2) = Executor::normalize_sql_with_literals(sql2);
        let subs = Executor::substitute_ast_literals(&mut template, &lits2);
        assert_eq!(subs, 2);

        // The substituted template should produce the same SQL as parsing sql2 directly
        assert_eq!(template[0].to_string(), stmts2[0].to_string());
    }
}
