//! DML (Data Manipulation Language) methods: INSERT, UPDATE, DELETE.
//!
//! Extracted from `mod.rs` to reduce file size. All methods are `pub(super)` so
//! the main executor module can delegate to them.
//!
//! Contains:
//! - `execute_insert` — VALUES and SELECT source, ON CONFLICT/UPSERT, RETURNING,
//!   constraint checking, trigger firing
//! - `execute_update` — WHERE clause, RETURNING, constraint enforcement
//! - `execute_delete` — WHERE clause, cascading deletes, RETURNING
//! - Supporting helpers: `eval_returning`, `is_default_expr`, `eval_column_default`,
//!   `get_conflict_columns`, `check_unique_constraints`, `check_check_constraints`,
//!   `check_enum_constraints`, `check_fk_constraints`, `enforce_fk_on_parent_mutation`,
//!   `check_not_null_constraints`, `enforce_constraints`

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use sqlparser::ast::{self, Expr, SelectItem, SetExpr, Statement, TableFactor};

use crate::catalog::TableDef;
#[cfg(feature = "server")]
use crate::reactive::ChangeType;
use crate::sql;
use crate::storage::granule_stats::GranuleStats;
use crate::types::{DataType, Row, Value};

use super::schema_types::*;
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};

/// Granule size: 8192 rows per granule (matching zone map documentation).
const GRANULE_SIZE: u32 = 8192;

/// Compute a stable table_id from a table name for zone map indexing.
fn table_name_to_id(name: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish()
}

impl Executor {
    // ========================================================================
    // DML: INSERT, UPDATE, DELETE
    // ========================================================================

    pub(super) async fn execute_insert(&self, insert: ast::Insert) -> Result<ExecResult, ExecError> {
        let table_name = match insert.table {
            ast::TableObject::TableName(name) => name.to_string(),
            _ => return Err(ExecError::Unsupported("table functions not supported".into())),
        };

        // Check INSERT privilege
        if !self.check_privilege(&table_name, "INSERT").await {
            return Err(ExecError::PermissionDenied(format!(
                "permission denied for table {table_name}"
            )));
        }

        let table_def = self.get_table(&table_name).await?;

        // Extract column list (if specified, for partial inserts)
        let insert_columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
        let has_column_list = !insert_columns.is_empty();

        let source = insert
            .source
            .ok_or_else(|| ExecError::Unsupported("INSERT without VALUES".into()))?;

        // Determine source type without consuming the Query yet
        enum InsertSourceKind { Values, Select, Other }
        let source_kind = match &*source.body {
            SetExpr::Values(_) => InsertSourceKind::Values,
            SetExpr::Select(_) | SetExpr::Query(_) => InsertSourceKind::Select,
            _ => InsertSourceKind::Other,
        };

        // Support both VALUES and SELECT as source
        let source_rows: Vec<Row> = match source_kind {
            InsertSourceKind::Values => {
                let values = match *source.body {
                    SetExpr::Values(v) => v,
                    _ => unreachable!(),
                };
                let mut rows = Vec::new();
                for row_exprs in values.rows {
                    let expected = if has_column_list { insert_columns.len() } else { table_def.columns.len() };
                    // Check for DEFAULT keyword values
                    let has_defaults = row_exprs.iter().any(Self::is_default_expr);
                    if row_exprs.len() != expected {
                        // Allow if we have defaults and the count matches
                        if !has_defaults || row_exprs.len() != expected {
                            return Err(ExecError::ColumnCountMismatch {
                                expected,
                                got: row_exprs.len(),
                            });
                        }
                    }
                    // Determine column order for DEFAULT resolution
                    let col_order: Vec<usize> = if has_column_list {
                        insert_columns.iter().map(|c| {
                            table_def.columns.iter().position(|col| col.name == *c)
                                .ok_or_else(|| ExecError::ColumnNotFound(c.clone()))
                        }).collect::<Result<Vec<_>, _>>()?
                    } else {
                        (0..table_def.columns.len()).collect()
                    };
                    let mut vals: Vec<Value> = Vec::with_capacity(row_exprs.len());
                    for (i, expr) in row_exprs.iter().enumerate() {
                        if Self::is_default_expr(expr) {
                            // Resolve DEFAULT for this column
                            let col = &table_def.columns[col_order[i]];
                            vals.push(self.eval_column_default(col)?);
                        } else if let Expr::Value(val_with_span) = expr {
                            // Fast path: direct literal → Value (skip eval_const_expr overhead)
                            vals.push(self.eval_value(&val_with_span.value)?);
                        } else {
                            vals.push(self.eval_const_expr(expr)?);
                        }
                    }
                    // If column list specified, build full row with defaults
                    if has_column_list {
                        let mut full_row = Vec::with_capacity(table_def.columns.len());
                        for col in &table_def.columns {
                            if let Some(pos) = insert_columns.iter().position(|c| c == &col.name) {
                                full_row.push(vals[pos].clone());
                            } else {
                                full_row.push(self.eval_column_default(col)?);
                            }
                        }
                        rows.push(full_row);
                    } else {
                        rows.push(vals);
                    }
                }
                rows
            }
            InsertSourceKind::Select => {
                // INSERT ... SELECT — execute the full source query including
                // its WITH clause, ORDER BY, and LIMIT so CTEs propagate.
                let result = self.execute_query(*source).await?;
                match result {
                    ExecResult::Select { rows, .. } => {
                        // If column list specified, remap SELECT columns to table columns
                        if has_column_list {
                            let mut mapped_rows = Vec::new();
                            for select_row in rows {
                                let mut full_row = Vec::with_capacity(table_def.columns.len());
                                for col in &table_def.columns {
                                    if let Some(pos) = insert_columns.iter().position(|c| c == &col.name) {
                                        if pos < select_row.len() {
                                            full_row.push(select_row[pos].clone());
                                        } else {
                                            full_row.push(self.eval_column_default(col)?);
                                        }
                                    } else {
                                        full_row.push(self.eval_column_default(col)?);
                                    }
                                }
                                mapped_rows.push(full_row);
                            }
                            mapped_rows
                        } else {
                            rows
                        }
                    }
                    _ => return Err(ExecError::Unsupported("INSERT SELECT must produce rows".into())),
                }
            }
            InsertSourceKind::Other => {
                return Err(ExecError::Unsupported("unsupported INSERT source".into()));
            }
        };

        // Handle ON CONFLICT (extract from OnInsert wrapper)
        let on_conflict = match insert.on {
            Some(ast::OnInsert::OnConflict(oc)) => Some(oc),
            Some(ast::OnInsert::DuplicateKeyUpdate(assignments)) => {
                // MySQL ON DUPLICATE KEY UPDATE — treat like ON CONFLICT DO UPDATE
                Some(ast::OnConflict {
                    conflict_target: None,
                    action: ast::OnConflictAction::DoUpdate(ast::DoUpdate {
                        assignments,
                        selection: None,
                    }),
                })
            }
            None => None,
            _ => None, // Other OnInsert variants
        };

        // Handle RETURNING
        let returning = &insert.returning;

        let col_meta = self.table_col_meta(&table_def);

        // Pre-check: does this table have any INSERT triggers? (avoids 4N+2 async lock acquisitions)
        let has_triggers = {
            let triggers = self.triggers.read().await;
            triggers.iter().any(|t| t.table_name == table_name && t.events.contains(&TriggerEvent::Insert))
        };

        // Pre-check: does this table have CHECK, FK, or enum constraints?
        let has_check = table_def.constraints.iter().any(|c| matches!(c, crate::catalog::TableConstraint::Check { .. }));
        let has_fk = table_def.constraints.iter().any(|c| matches!(c, crate::catalog::TableConstraint::ForeignKey { .. }));
        let has_enum_cols = table_def.columns.iter().any(|c| matches!(&c.data_type, DataType::UserDefined(_)));

        // Fire BEFORE INSERT statement-level triggers
        if has_triggers {
            self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Insert, None, None, &col_meta, false).await;
        }

        let mut count = 0;
        let mut returned_rows = Vec::new();
        let mut inserted_rows: Vec<Row> = Vec::new();

        for row in source_rows {
            // Fire BEFORE INSERT row-level triggers (only if triggers exist)
            if has_triggers {
                self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Insert, None, Some(&row), &col_meta, true).await;
            }

            // Enforce NOT NULL, CHECK, FK, and enum constraints (hard-fail even with ON CONFLICT)
            Self::check_not_null_constraints(&table_def, &row)?;
            if has_check {
                self.check_check_constraints(&table_def, &row)?;
            }
            if has_fk {
                self.check_fk_constraints(&table_def, &row).await?;
            }
            if has_enum_cols {
                self.check_enum_constraints(&table_def, &row).await?;
            }

            // Check UNIQUE / PRIMARY KEY constraints
            match self.check_unique_constraints(&table_name, &table_def, &row, None).await {
                Ok(()) => {
                    // No conflict — stage for batch insert
                    if let Some(returning_items) = returning {
                        let returned = self.eval_returning(returning_items, &row, &col_meta)?;
                        returned_rows.push(returned);
                    }
                    self.update_vector_indexes_on_insert(&table_name, &row, &table_def);
                    self.update_encrypted_indexes_on_insert(&table_name, &row, &table_def);
                    // Fire AFTER INSERT row-level triggers (only if triggers exist)
                    if has_triggers {
                        self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Insert, None, Some(&row), &col_meta, true).await;
                    }
                    inserted_rows.push(row);
                    count += 1;
                }
                Err(ExecError::ConstraintViolation(_)) if on_conflict.is_some() => {
                    // Handle ON CONFLICT
                    let conflict = on_conflict.as_ref().unwrap();
                    match &conflict.action {
                        ast::OnConflictAction::DoNothing => {
                            // Skip this row silently
                        }
                        ast::OnConflictAction::DoUpdate(do_update) => {
                            // Find the conflicting row and update it
                            let existing_rows = self.storage_for(&table_name).scan(&table_name).await?;
                            let conflict_cols = self.get_conflict_columns(&table_def, conflict);
                            let conflict_indices: Vec<usize> = conflict_cols
                                .iter()
                                .filter_map(|c| table_def.column_index(c))
                                .collect();

                            // Build augmented metadata that includes EXCLUDED pseudo-table.
                            // The combined row is [existing_values..., excluded_values...]
                            // so EXCLUDED.col resolves to the new row being inserted.
                            let mut augmented_meta: Vec<ColMeta> = col_meta.clone();
                            for cm in &col_meta {
                                augmented_meta.push(ColMeta {
                                    table: Some("excluded".to_string()),
                                    name: cm.name.clone(),
                                    dtype: cm.dtype.clone(),
                                });
                            }

                            for (pos, existing) in existing_rows.iter().enumerate() {
                                let matches = conflict_indices.iter().all(|&i| {
                                    i < row.len() && i < existing.len() && row[i] == existing[i]
                                });
                                if matches {
                                    let mut updated = existing.clone();
                                    // Build combined row: [existing..., excluded(new)...]
                                    let mut combined_row = existing.clone();
                                    combined_row.extend(row.iter().cloned());
                                    for assign in &do_update.assignments {
                                        let col_name = match &assign.target {
                                            ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                                            _ => continue,
                                        };
                                        if let Some(idx) = table_def.column_index(&col_name) {
                                            updated[idx] = self.eval_row_expr(&assign.value, &combined_row, &augmented_meta)?;
                                        }
                                    }
                                    self.storage_for(&table_name).update(&table_name, &[(pos, updated.clone())]).await?;
                                    if let Some(returning_items) = returning {
                                        let returned = self.eval_returning(returning_items, &updated, &col_meta)?;
                                        returned_rows.push(returned);
                                    }
                                    count += 1;
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }

        // Batch-insert all staged rows — one lock/transaction per INSERT statement.
        if !inserted_rows.is_empty() {
            // Notify reactive subscribers *before* moving rows into storage.
            // The notification only borrows the data, so this avoids cloning the
            // entire row batch.  The subscriber sees the rows slightly before they
            // are durable, but this is fire-and-forget best-effort anyway.
            #[cfg(feature = "server")]
            self.notify_change_rows(&table_name, ChangeType::Insert, &inserted_rows, &[], &col_meta);

            // ── Zone map population ─────────────────────────────────────────
            // Update granule stats for the newly inserted rows so that future
            // filtered scans can skip non-matching granules.
            {
                let zm_table_id = table_name_to_id(&table_name);
                let column_ids: Vec<u32> = (0..table_def.columns.len() as u32).collect();
                let existing_granules = self.zone_map_index.get_table_granules(zm_table_id);
                // Determine the starting row offset for new rows: sum of row_count
                // across all existing granules.
                let base_row_offset: u32 = existing_granules
                    .iter()
                    .map(|g| g.row_count)
                    .sum();
                for (i, row) in inserted_rows.iter().enumerate() {
                    let row_idx = base_row_offset + i as u32;
                    let granule_id = row_idx / GRANULE_SIZE;
                    let mut granule = self
                        .zone_map_index
                        .get_granule(zm_table_id, granule_id)
                        .unwrap_or_else(|| GranuleStats::new(zm_table_id, granule_id));
                    granule.add_row(row, &column_ids);
                    self.zone_map_index
                        .update_granule(zm_table_id, granule_id, granule);
                }
            }

            self.storage_for(&table_name).insert_batch(&table_name, inserted_rows).await?;
        }

        // Fire AFTER INSERT statement-level triggers
        if has_triggers {
            self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Insert, None, None, &col_meta, false).await;
        }

        // ── Write-time materialized view refresh ────────────────────────────
        // After the main insert commits, check if any MVs depend on this table
        // and refresh them. For v0.1 we re-run the full MV query to ensure
        // correctness with aggregations, WHERE filters, and multi-table joins.
        if count > 0 {
            let dependent_mvs: Vec<(String, String)> = {
                let deps = self.mv_deps.read().await;
                if let Some(mv_names) = deps.get(&table_name) {
                    let views = self.materialized_views.read().await;
                    mv_names.iter()
                        .filter_map(|name| views.get(name).map(|mv| (name.clone(), mv.sql.clone())))
                        .collect()
                } else {
                    vec![]
                }
            };
            for (mv_name, mv_sql) in dependent_mvs {
                if let Ok(results) = self.execute(&mv_sql).await {
                    if let Some(ExecResult::Select { columns, rows }) = results.into_iter().next() {
                        let mut views = self.materialized_views.write().await;
                        if let Some(mv) = views.get_mut(&mv_name) {
                            mv.columns = columns;
                            mv.rows = rows;
                        }
                    }
                }
            }
        }

        if returning.is_some() && !returned_rows.is_empty() {
            let columns: Vec<(String, DataType)> = col_meta
                .iter()
                .map(|c| (c.name.clone(), c.dtype.clone()))
                .collect();
            Ok(ExecResult::Select {
                columns,
                rows: returned_rows,
            })
        } else {
            Ok(ExecResult::Command {
                tag: "INSERT".into(),
                rows_affected: count,
            })
        }
    }

    /// Evaluate RETURNING clause expressions against a row.
    pub(super) fn eval_returning(
        &self,
        returning: &[SelectItem],
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Row, ExecError> {
        let mut result = Vec::new();
        for item in returning {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    result.push(self.eval_row_expr(expr, row, col_meta)?);
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    result.push(self.eval_row_expr(expr, row, col_meta)?);
                }
                SelectItem::Wildcard(_) => {
                    result.extend(row.iter().cloned());
                }
                _ => {}
            }
        }
        Ok(result)
    }

    /// Check if an expression is the DEFAULT keyword.
    /// sqlparser parses DEFAULT in VALUES as an identifier.
    pub(super) fn is_default_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("DEFAULT"),
            _ => false,
        }
    }

    /// Evaluate a column's default expression, returning Null if no default is defined.
    pub(super) fn eval_column_default(&self, col: &crate::catalog::ColumnDef) -> Result<Value, ExecError> {
        if let Some(ref default_expr) = col.default_expr {
            let parsed = sql::parse(&format!("SELECT {default_expr}"));
            if let Ok(stmts) = parsed
                && let Some(Statement::Query(q)) = stmts.into_iter().next()
                    && let SetExpr::Select(sel) = *q.body
                        && let Some(SelectItem::UnnamedExpr(expr)) = sel.projection.first() {
                            let empty_row: Row = Vec::new();
                            let empty_meta: Vec<ColMeta> = Vec::new();
                            if let Ok(val) = self.eval_row_expr(expr, &empty_row, &empty_meta) {
                                // Coerce the default value to match the column's declared type.
                                // This handles SERIAL (Int32) columns whose nextval() returns Int64.
                                let coerced = match (&col.data_type, &val) {
                                    (DataType::Int32, Value::Int64(n)) => Value::Int32(*n as i32),
                                    (DataType::Int64, Value::Int32(n)) => Value::Int64(*n as i64),
                                    _ => val,
                                };
                                return Ok(coerced);
                            }
                        }
        }
        Ok(Value::Null)
    }

    /// Get conflict target columns from ON CONFLICT clause.
    pub(super) fn get_conflict_columns(&self, table_def: &TableDef, conflict: &ast::OnConflict) -> Vec<String> {
        match &conflict.conflict_target {
            Some(ast::ConflictTarget::Columns(cols)) => {
                cols.iter().map(|c| c.value.clone()).collect()
            }
            _ => {
                // Default to primary key columns
                table_def.primary_key_columns()
                    .map(|cols| cols.to_vec())
                    .unwrap_or_default()
            }
        }
    }

    /// Check UNIQUE and PRIMARY KEY constraints for a row.
    /// `skip_row_idx` is used during UPDATE to skip the row being updated.
    pub(super) async fn check_unique_constraints(
        &self,
        table_name: &str,
        table_def: &TableDef,
        new_row: &Row,
        skip_row_idx: Option<usize>,
    ) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        let mut unique_col_sets: Vec<Vec<usize>> = Vec::new();

        for constraint in &table_def.constraints {
            match constraint {
                TableConstraint::PrimaryKey { columns } | TableConstraint::Unique { columns, .. } => {
                    let indices: Vec<usize> = columns
                        .iter()
                        .filter_map(|col_name| table_def.column_index(col_name))
                        .collect();
                    if indices.len() == columns.len() {
                        unique_col_sets.push(indices);
                    }
                }
                _ => {}
            }
        }

        if unique_col_sets.is_empty() {
            return Ok(());
        }

        // Lazy scan: only fetched when no index is available for a constraint.
        let mut existing_rows: Option<Vec<Row>> = None;

        for col_indices in &unique_col_sets {
            // Fast path for the common single-column unique/primary-key case.
            if col_indices.len() == 1 {
                let idx = col_indices[0];
                if idx >= new_row.len() {
                    continue;
                }
                let new_val = &new_row[idx];
                if *new_val == Value::Null {
                    continue;
                }

                // Index-assisted probe for INSERT (skip_row_idx is None for inserts).
                // For UPDATE we fall through to the scan path to handle skip_row_idx correctly.
                if skip_row_idx.is_none() {
                    let col_name = table_def.columns[idx].name.clone();
                    let index_name_opt = self
                        .btree_indexes
                        .get(&(table_name.to_string(), col_name.clone()))
                        .map(|r| r.clone());
                    if let Some(index_name) = index_name_opt {
                        match self.storage.index_lookup_sync(table_name, &index_name, new_val) {
                            Ok(Some(rows)) if !rows.is_empty() => {
                                return Err(ExecError::ConstraintViolation(format!(
                                    "duplicate key value violates unique constraint on ({col_name})"
                                )));
                            }
                            // Empty result from an index-capable backend: no duplicate.
                            Ok(Some(_)) => continue,
                            // Ok(None): backend doesn't support sync index lookup
                            // (e.g. in-memory engine). Fall through to scan.
                            Ok(None) => {}
                            // Index error — fall through to scan.
                            Err(_) => {}
                        }
                        // Reach here on Ok(None) or Err — fall through to scan below.
                    }
                }

                // Scan fallback: used for UPDATE path or when no index is registered.
                if existing_rows.is_none() {
                    existing_rows = Some(self.storage_for(table_name).scan(table_name).await?);
                }
                let rows = existing_rows.as_ref().unwrap();
                for (row_idx, existing) in rows.iter().enumerate() {
                    if skip_row_idx == Some(row_idx) {
                        continue;
                    }
                    if idx < existing.len() && existing[idx] == *new_val {
                        let col_names: Vec<&str> = col_indices
                            .iter()
                            .map(|&i| table_def.columns[i].name.as_str())
                            .collect();
                        return Err(ExecError::ConstraintViolation(format!(
                            "duplicate key value violates unique constraint on ({})",
                            col_names.join(", ")
                        )));
                    }
                }
                continue;
            }

            // Generic multi-column unique check — always uses the scan path.
            let mut new_has_null = false;
            for &idx in col_indices {
                if idx >= new_row.len() || new_row[idx] == Value::Null {
                    new_has_null = true;
                    break;
                }
            }
            if new_has_null {
                continue;
            }
            if existing_rows.is_none() {
                existing_rows = Some(self.storage_for(table_name).scan(table_name).await?);
            }
            let rows = existing_rows.as_ref().unwrap();
            for (row_idx, existing) in rows.iter().enumerate() {
                // Skip the row being updated (for UPDATE operations)
                if skip_row_idx == Some(row_idx) {
                    continue;
                }
                let mut equal = true;
                for &idx in col_indices {
                    if idx >= existing.len() || existing[idx] != new_row[idx] {
                        equal = false;
                        break;
                    }
                }
                if equal {
                    let col_names: Vec<&str> = col_indices
                        .iter()
                        .map(|&i| table_def.columns[i].name.as_str())
                        .collect();
                    return Err(ExecError::ConstraintViolation(format!(
                        "duplicate key value violates unique constraint on ({})",
                        col_names.join(", ")
                    )));
                }
            }
        }
        Ok(())
    }

    /// Check CHECK constraints for a row.
    pub(super) fn check_check_constraints(
        &self,
        table_def: &TableDef,
        new_row: &Row,
    ) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        let col_meta = self.table_col_meta(table_def);

        for constraint in &table_def.constraints {
            if let TableConstraint::Check { name, expr } = constraint {
                // Strip CHECK(...) wrapper if present
                let clean_expr = {
                    let trimmed = expr.trim();
                    let upper = trimmed.to_uppercase();
                    if upper.starts_with("CHECK") {
                        let rest = trimmed["CHECK".len()..].trim();
                        if rest.starts_with('(') && rest.ends_with(')') {
                            rest[1..rest.len()-1].to_string()
                        } else {
                            rest.to_string()
                        }
                    } else {
                        trimmed.to_string()
                    }
                };
                // Parse the CHECK expression
                let parsed = sql::parse(&format!("SELECT {clean_expr}"));
                if let Ok(stmts) = parsed
                    && let Some(Statement::Query(q)) = stmts.into_iter().next()
                        && let SetExpr::Select(sel) = *q.body
                            && let Some(SelectItem::UnnamedExpr(check_expr)) = sel.projection.first() {
                                match self.eval_row_expr(check_expr, new_row, &col_meta) {
                                    Ok(Value::Bool(true)) => {} // constraint satisfied
                                    Ok(Value::Bool(false)) => {
                                        let constraint_name = name
                                            .as_deref()
                                            .unwrap_or("unnamed");
                                        return Err(ExecError::ConstraintViolation(
                                            format!(
                                                "new row violates check constraint \"{constraint_name}\""
                                            ),
                                        ));
                                    }
                                    Ok(Value::Null) => {
                                        // NULL result in CHECK is treated as true (SQL standard)
                                    }
                                    Ok(other) => {
                                        let constraint_name = name
                                            .as_deref()
                                            .unwrap_or("unnamed");
                                        return Err(ExecError::ConstraintViolation(
                                            format!(
                                                "check constraint \"{constraint_name}\" evaluated to non-boolean: {other:?}"
                                            ),
                                        ));
                                    }
                                    Err(e) => {
                                        let constraint_name = name
                                            .as_deref()
                                            .unwrap_or("unnamed");
                                        return Err(ExecError::ConstraintViolation(
                                            format!(
                                                "check constraint \"{constraint_name}\" could not be evaluated: {e}"
                                            ),
                                        ));
                                    }
                                }
                            }
            }
        }
        Ok(())
    }

    /// Validate that values in UserDefined (enum) columns are among the allowed labels.
    pub(super) async fn check_enum_constraints(
        &self,
        table_def: &TableDef,
        row: &Row,
    ) -> Result<(), ExecError> {
        for (i, col) in table_def.columns.iter().enumerate() {
            if let crate::types::DataType::UserDefined(type_name) = &col.data_type {
                let val = row.get(i).unwrap_or(&Value::Null);
                if matches!(val, Value::Null) {
                    continue; // NULLs are fine (nullable check handled separately)
                }
                let text_val = match val {
                    Value::Text(s) => s.as_str(),
                    _ => return Err(ExecError::ConstraintViolation(
                        format!("column \"{}\" expects a {} value (text)", col.name, type_name)
                    )),
                };
                if let Some(labels) = self.catalog.get_enum_type(type_name).await
                    && !labels.iter().any(|l| l == text_val) {
                        return Err(ExecError::ConstraintViolation(format!(
                            "invalid input value for enum {type_name}: \"{text_val}\""
                        )));
                    }
                // If enum type not in catalog, allow any value (graceful degradation).
            }
        }
        Ok(())
    }

    /// Check FOREIGN KEY constraints for a row.
    pub(super) async fn check_fk_constraints(
        &self,
        table_def: &TableDef,
        new_row: &Row,
    ) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        for constraint in &table_def.constraints {
            if let TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                ..
            } = constraint
            {
                // Resolve column indices in the source table
                let col_indices: Vec<usize> = columns
                    .iter()
                    .filter_map(|c| table_def.column_index(c))
                    .collect();
                if col_indices.len() != columns.len() {
                    continue; // skip if columns can't be resolved
                }

                // Extract the FK values from the new row
                let fk_values: Vec<&Value> = col_indices.iter().map(|&i| &new_row[i]).collect();

                // If any FK column is NULL, the constraint is satisfied (SQL standard)
                if fk_values.iter().any(|v| **v == Value::Null) {
                    continue;
                }

                // Look up the referenced table
                let ref_table_def = self.get_table(ref_table).await?;
                let ref_col_indices: Vec<usize> = ref_columns
                    .iter()
                    .filter_map(|c| ref_table_def.column_index(c))
                    .collect();
                if ref_col_indices.len() != ref_columns.len() {
                    return Err(ExecError::ConstraintViolation(format!(
                        "foreign key references non-existent columns in table \"{ref_table}\""
                    )));
                }

                // Scan the referenced table to see if the values exist
                let ref_rows = self.storage.scan(ref_table).await?;
                let found = ref_rows.iter().any(|ref_row| {
                    ref_col_indices
                        .iter()
                        .zip(fk_values.iter())
                        .all(|(&ri, fk_val)| ri < ref_row.len() && &ref_row[ri] == *fk_val)
                });

                if !found {
                    return Err(ExecError::ConstraintViolation(format!(
                        "insert or update on table \"{}\" violates foreign key constraint referencing \"{}\"",
                        table_def.name, ref_table
                    )));
                }
            }
        }
        Ok(())
    }

    /// Enforce FK actions on child tables when a parent table is mutated (DELETE or UPDATE).
    ///
    /// For DELETE: `deleted_rows` contains the rows being deleted from the parent.
    /// For UPDATE: `deleted_rows` contains the OLD values, `new_parent_rows` contains NEW values.
    ///
    /// `depth` guards against infinite recursion in circular FK graphs.
    pub(super) fn enforce_fk_on_parent_mutation<'a>(
        &'a self,
        parent_table: &'a str,
        deleted_rows: &'a [Row],
        new_parent_rows: Option<&'a [(Row, Row)]>, // (old_row, new_row) pairs for UPDATE
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ExecError>> + Send + 'a>> {
        Box::pin(async move {
        use crate::catalog::{FkAction, TableConstraint};

        const MAX_CASCADE_DEPTH: usize = 64;
        if depth >= MAX_CASCADE_DEPTH {
            return Err(ExecError::ConstraintViolation(
                "foreign key cascade depth limit exceeded".into(),
            ));
        }

        // Find all tables that have FK constraints referencing this parent table.
        let all_tables = self.catalog.list_tables().await;

        for child_table_def in &all_tables {
            if child_table_def.name == parent_table && new_parent_rows.is_none() {
                // For DELETE: skip self-referencing only if no special handling needed.
                // Actually, we should still handle self-referencing FKs.
            }

            for constraint in &child_table_def.constraints {
                if let TableConstraint::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                    on_delete,
                    on_update,
                    ..
                } = constraint
                {
                    if ref_table != parent_table {
                        continue;
                    }

                    // Resolve the parent table's ref_column indices
                    // We need the parent table def to map ref_columns to indices
                    let parent_def = self.get_table(parent_table).await?;
                    let ref_col_indices: Vec<usize> = ref_columns
                        .iter()
                        .filter_map(|c| parent_def.column_index(c))
                        .collect();
                    if ref_col_indices.len() != ref_columns.len() {
                        continue;
                    }

                    // Resolve child FK column indices
                    let child_col_indices: Vec<usize> = columns
                        .iter()
                        .filter_map(|c| child_table_def.column_index(c))
                        .collect();
                    if child_col_indices.len() != columns.len() {
                        continue;
                    }

                    let child_table = &child_table_def.name;
                    let child_storage = self.storage_for(child_table);

                    if let Some(update_pairs) = new_parent_rows {
                        // -- ON UPDATE handling --
                        let action = on_update;
                        for (old_row, new_row) in update_pairs {
                            // Extract old and new referenced values
                            let old_vals: Vec<&Value> =
                                ref_col_indices.iter().map(|&i| &old_row[i]).collect();
                            let new_vals: Vec<&Value> =
                                ref_col_indices.iter().map(|&i| &new_row[i]).collect();

                            // If the referenced columns didn't change, nothing to do
                            if old_vals == new_vals {
                                continue;
                            }

                            // Skip if old values contain NULL (NULL FK refs are not constrained)
                            if old_vals.iter().any(|v| **v == Value::Null) {
                                continue;
                            }

                            // Find child rows that reference the old values
                            let child_rows = child_storage.scan(child_table).await?;
                            let matching_positions: Vec<usize> = child_rows
                                .iter()
                                .enumerate()
                                .filter(|(_, row)| {
                                    child_col_indices
                                        .iter()
                                        .zip(old_vals.iter())
                                        .all(|(&ci, &ov)| ci < row.len() && &row[ci] == ov)
                                })
                                .map(|(pos, _)| pos)
                                .collect();

                            if matching_positions.is_empty() {
                                continue;
                            }

                            match action {
                                FkAction::Restrict | FkAction::NoAction => {
                                    return Err(ExecError::ConstraintViolation(format!(
                                        "update on table \"{}\" violates foreign key constraint on table \"{}\"",
                                        parent_table, child_table
                                    )));
                                }
                                FkAction::Cascade => {
                                    // Update child FK columns to new parent values
                                    let mut updates = Vec::new();
                                    for &pos in &matching_positions {
                                        let mut updated_row = child_rows[pos].clone();
                                        for (ci_idx, &ci) in child_col_indices.iter().enumerate() {
                                            updated_row[ci] = new_vals[ci_idx].clone();
                                        }
                                        updates.push((pos, updated_row));
                                    }
                                    // Recursively check if this child table is also a parent
                                    let cascade_pairs: Vec<(Row, Row)> = updates
                                        .iter()
                                        .map(|(pos, new)| (child_rows[*pos].clone(), new.clone()))
                                        .collect();
                                    child_storage.update(child_table, &updates).await?;
                                    self.enforce_fk_on_parent_mutation(
                                        child_table,
                                        &[],
                                        Some(&cascade_pairs),
                                        depth + 1,
                                    )
                                    .await?;
                                }
                                FkAction::SetNull => {
                                    let mut updates = Vec::new();
                                    for &pos in &matching_positions {
                                        let mut updated_row = child_rows[pos].clone();
                                        for &ci in &child_col_indices {
                                            updated_row[ci] = Value::Null;
                                        }
                                        updates.push((pos, updated_row));
                                    }
                                    child_storage.update(child_table, &updates).await?;
                                }
                                FkAction::SetDefault => {
                                    let mut updates = Vec::new();
                                    for &pos in &matching_positions {
                                        let mut updated_row = child_rows[pos].clone();
                                        for &ci in &child_col_indices {
                                            let default_val =
                                                self.eval_column_default(&child_table_def.columns[ci])?;
                                            updated_row[ci] = default_val;
                                        }
                                        updates.push((pos, updated_row));
                                    }
                                    child_storage.update(child_table, &updates).await?;
                                }
                            }
                        }
                    } else {
                        // -- ON DELETE handling --
                        let action = on_delete;
                        for deleted_row in deleted_rows {
                            // Extract the referenced column values from the deleted parent row
                            let parent_vals: Vec<&Value> =
                                ref_col_indices.iter().map(|&i| &deleted_row[i]).collect();

                            // Skip if parent values contain NULL
                            if parent_vals.iter().any(|v| **v == Value::Null) {
                                continue;
                            }

                            // Find child rows that reference these values
                            let child_rows = child_storage.scan(child_table).await?;
                            let matching_positions: Vec<usize> = child_rows
                                .iter()
                                .enumerate()
                                .filter(|(_, row)| {
                                    child_col_indices
                                        .iter()
                                        .zip(parent_vals.iter())
                                        .all(|(&ci, &pv)| ci < row.len() && &row[ci] == pv)
                                })
                                .map(|(pos, _)| pos)
                                .collect();

                            if matching_positions.is_empty() {
                                continue;
                            }

                            match action {
                                FkAction::Restrict | FkAction::NoAction => {
                                    return Err(ExecError::ConstraintViolation(format!(
                                        "delete on table \"{}\" violates foreign key constraint on table \"{}\"",
                                        parent_table, child_table
                                    )));
                                }
                                FkAction::Cascade => {
                                    // Recursively enforce FK on grandchildren before deleting
                                    let rows_to_delete: Vec<Row> = matching_positions
                                        .iter()
                                        .map(|&pos| child_rows[pos].clone())
                                        .collect();
                                    self.enforce_fk_on_parent_mutation(
                                        child_table,
                                        &rows_to_delete,
                                        None,
                                        depth + 1,
                                    )
                                    .await?;
                                    child_storage.delete(child_table, &matching_positions).await?;
                                }
                                FkAction::SetNull => {
                                    let mut updates = Vec::new();
                                    for &pos in &matching_positions {
                                        let mut updated_row = child_rows[pos].clone();
                                        for &ci in &child_col_indices {
                                            updated_row[ci] = Value::Null;
                                        }
                                        updates.push((pos, updated_row));
                                    }
                                    child_storage.update(child_table, &updates).await?;
                                }
                                FkAction::SetDefault => {
                                    let mut updates = Vec::new();
                                    for &pos in &matching_positions {
                                        let mut updated_row = child_rows[pos].clone();
                                        for &ci in &child_col_indices {
                                            let default_val =
                                                self.eval_column_default(&child_table_def.columns[ci])?;
                                            updated_row[ci] = default_val;
                                        }
                                        updates.push((pos, updated_row));
                                    }
                                    child_storage.update(child_table, &updates).await?;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
        }) // Box::pin(async move { ... })
    }

    /// Check NOT NULL constraints for a row.
    pub(super) fn check_not_null_constraints(
        table_def: &TableDef,
        new_row: &Row,
    ) -> Result<(), ExecError> {
        for (i, col) in table_def.columns.iter().enumerate() {
            if !col.nullable && i < new_row.len() && new_row[i] == Value::Null {
                return Err(ExecError::ConstraintViolation(format!(
                    "null value in column \"{}\" violates not-null constraint",
                    col.name
                )));
            }
        }
        Ok(())
    }

    /// Enforce all constraints (NOT NULL, CHECK, FOREIGN KEY, PRIMARY KEY, UNIQUE) on a row.
    /// `skip_row_idx` is used during UPDATE to skip the row being updated in uniqueness checks.
    pub(super) async fn enforce_constraints(
        &self,
        table_name: &str,
        table_def: &TableDef,
        new_row: &Row,
        skip_row_idx: Option<usize>,
        check_fk: bool,
        check_unique: bool,
        has_check_constraints: bool,
        has_enum_columns: bool,
    ) -> Result<(), ExecError> {
        Self::check_not_null_constraints(table_def, new_row)?;
        if has_check_constraints {
            self.check_check_constraints(table_def, new_row)?;
        }
        if has_enum_columns {
            self.check_enum_constraints(table_def, new_row).await?;
        }
        if check_fk {
            self.check_fk_constraints(table_def, new_row).await?;
        }
        if check_unique {
            self.check_unique_constraints(table_name, table_def, new_row, skip_row_idx).await?;
        }
        Ok(())
    }

    pub(super) async fn execute_update(&self, update: ast::Update) -> Result<ExecResult, ExecError> {
        let table_name = match &update.table.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(ExecError::Unsupported("complex UPDATE target".into())),
        };

        // Check UPDATE privilege
        if !self.check_privilege(&table_name, "UPDATE").await {
            return Err(ExecError::PermissionDenied(format!(
                "permission denied for table {table_name}"
            )));
        }

        let table_def = self.get_table(&table_name).await?;

        // Reject UPDATE on append-only tables.
        if table_def.append_only {
            return Err(ExecError::Unsupported(format!(
                "UPDATE not allowed on append-only table {table_name}"
            )));
        }

        // Build column metadata for expression evaluation
        let col_meta = self.table_col_meta(&table_def);

        // Fast path: PK/unique equality WHERE → filtered scan (avoids materializing all rows)
        let (all_rows, pre_filtered) = match Self::extract_pk_eq_value(&update.selection, &table_def) {
            Some((col_idx, eq_value)) => {
                let matches = self.storage_for(&table_name)
                    .scan_where_eq_positions(&table_name, col_idx, &eq_value).await?;
                self.metrics.rows_scanned.inc_by(1);
                (matches.into_iter().collect::<Vec<_>>(), true)
            }
            None => {
                let rows = self.storage_for(&table_name).scan(&table_name).await?;
                self.metrics.rows_scanned.inc_by(rows.len() as u64);
                (rows.into_iter().enumerate().collect::<Vec<_>>(), false)
            }
        };
        // Resolve assignments: (column_index, value_expr)
        let mut assign_targets = Vec::new();
        for a in &update.assignments {
            let col_name = match &a.target {
                ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                _ => {
                    return Err(ExecError::Unsupported("tuple assignment".into()));
                }
            };
            let idx = table_def
                .column_index(&col_name)
                .ok_or(ExecError::ColumnNotFound(col_name))?;
            assign_targets.push((idx, &a.value));
        }
        let updated_col_indices: HashSet<usize> = assign_targets.iter().map(|(idx, _)| *idx).collect();
        let mut check_fk = false;
        let mut check_unique = false;
        let mut has_check_constraints = false;
        for constraint in &table_def.constraints {
            match constraint {
                crate::catalog::TableConstraint::PrimaryKey { columns }
                | crate::catalog::TableConstraint::Unique { columns, .. } => {
                    if columns
                        .iter()
                        .filter_map(|c| table_def.column_index(c))
                        .any(|idx| updated_col_indices.contains(&idx))
                    {
                        check_unique = true;
                    }
                }
                crate::catalog::TableConstraint::ForeignKey { columns, .. } => {
                    if columns
                        .iter()
                        .filter_map(|c| table_def.column_index(c))
                        .any(|idx| updated_col_indices.contains(&idx))
                    {
                        check_fk = true;
                    }
                }
                crate::catalog::TableConstraint::Check { .. } => {
                    has_check_constraints = true;
                }
            }
        }
        // Pre-compute: does table have any enum (UserDefined) columns?
        let has_enum_columns = table_def.columns.iter().any(|c| {
            matches!(c.data_type, crate::types::DataType::UserDefined(_))
        });

        // Pre-check: does this table have any vector or encrypted indexes?
        let has_vector_indexes = {
            let indexes = self.vector_indexes.read();
            indexes.values().any(|e| e.table_name == table_name)
        };
        let has_encrypted_indexes = {
            let indexes = self.encrypted_indexes.read();
            indexes.values().any(|e| e.table_name == table_name)
        };

        // Pre-check: does this table have any UPDATE triggers?
        let has_triggers = {
            let triggers = self.triggers.read().await;
            triggers.iter().any(|t| t.table_name == table_name && t.events.contains(&TriggerEvent::Update))
        };
        // Fire BEFORE UPDATE statement-level triggers
        if has_triggers {
            self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Update, None, None, &col_meta, false).await;
        }

        let mut updates = Vec::new();
        let mut returned_rows = Vec::new();
        for (pos, row) in &all_rows {
            // If pre_filtered, all rows already match the WHERE clause
            let matches = if pre_filtered {
                true
            } else {
                match &update.selection {
                    Some(expr) => self.eval_where(expr, row, &col_meta)?,
                    None => true,
                }
            };
            if matches {
                let mut new_row = row.clone();
                for (col_idx, val_expr) in &assign_targets {
                    new_row[*col_idx] = self.eval_row_expr(val_expr, row, &col_meta)?;
                }

                // Fire BEFORE UPDATE row-level triggers (old = current row, new = updated row)
                if has_triggers {
                    self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Update, Some(row), Some(&new_row), &col_meta, true).await;
                }

                // Enforce all constraints on the updated row
                self.enforce_constraints(
                    &table_name,
                    &table_def,
                    &new_row,
                    Some(*pos),
                    check_fk,
                    check_unique,
                    has_check_constraints,
                    has_enum_columns,
                ).await?;
                if let Some(ref returning_items) = update.returning {
                    let returned = self.eval_returning(returning_items, &new_row, &col_meta)?;
                    returned_rows.push(returned);
                }

                // Fire AFTER UPDATE row-level triggers
                if has_triggers {
                    self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Update, Some(row), Some(&new_row), &col_meta, true).await;
                }

                updates.push((*pos, new_row));
            }
        }

        // Build position→row lookup for FK enforcement and change notification
        let row_by_pos: std::collections::HashMap<usize, &Row> = all_rows.iter()
            .map(|(pos, row)| (*pos, row))
            .collect();

        // Enforce FK actions on child tables when parent PK/unique columns are updated.
        // Only needed when PK/unique columns are being modified (child FKs reference those).
        if check_unique && !updates.is_empty() {
            let update_pairs: Vec<(Row, Row)> = updates
                .iter()
                .filter_map(|(pos, new_row)| {
                    row_by_pos.get(pos).map(|old| ((*old).clone(), new_row.clone()))
                })
                .collect();
            self.enforce_fk_on_parent_mutation(&table_name, &[], Some(&update_pairs), 0).await?;
        }

        // Maintain vector and encrypted indexes: remove old values, insert new
        // Skip entirely if no such indexes exist for this table
        if has_vector_indexes || has_encrypted_indexes {
            for (pos, new_row) in &updates {
                if let Some(&old_row) = row_by_pos.get(pos) {
                    if has_encrypted_indexes {
                        self.remove_from_encrypted_indexes(&table_name, old_row, *pos, &table_def);
                    }
                    if has_vector_indexes {
                        self.remove_from_vector_indexes(&table_name, *pos);
                    }
                }
                if has_encrypted_indexes {
                    self.update_encrypted_indexes_on_insert(&table_name, new_row, &table_def);
                }
                if has_vector_indexes {
                    self.update_vector_indexes_on_insert(&table_name, new_row, &table_def);
                }
            }
        }

        let count = self.storage_for(&table_name).update(&table_name, &updates).await?;

        // Invalidate zone map stats — column values may have changed,
        // making min/max bounds stale.
        if count > 0 {
            let zm_table_id = table_name_to_id(&table_name);
            self.zone_map_index.clear_table(zm_table_id);
        }

        // Fire AFTER UPDATE statement-level triggers
        if has_triggers {
            self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Update, None, None, &col_meta, false).await;
        }

        // Notify reactive subscribers with real before/after row data
        #[cfg(feature = "server")]
        {
            let old_rows: Vec<Row> = updates.iter()
                .filter_map(|(pos, _)| row_by_pos.get(pos).map(|r| (*r).clone()))
                .collect();
            let new_rows: Vec<Row> = updates.into_iter().map(|(_, row)| row).collect();
            self.notify_change_rows(&table_name, ChangeType::Update, &new_rows, &old_rows, &col_meta);
        }
        #[cfg(not(feature = "server"))]
        drop(updates);

        if update.returning.is_some() && !returned_rows.is_empty() {
            let columns: Vec<(String, DataType)> = col_meta
                .iter()
                .map(|c| (c.name.clone(), c.dtype.clone()))
                .collect();
            Ok(ExecResult::Select { columns, rows: returned_rows })
        } else {
            Ok(ExecResult::Command {
                tag: "UPDATE".into(),
                rows_affected: count,
            })
        }
    }

    pub(super) async fn execute_delete(&self, delete: ast::Delete) -> Result<ExecResult, ExecError> {
        let tables_with_joins = match delete.from {
            ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
        };
        if tables_with_joins.is_empty() {
            return Err(ExecError::Unsupported("DELETE without FROM".into()));
        }
        let table_name = match &tables_with_joins[0].relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(ExecError::Unsupported("complex DELETE target".into())),
        };

        // Check DELETE privilege
        if !self.check_privilege(&table_name, "DELETE").await {
            return Err(ExecError::PermissionDenied(format!(
                "permission denied for table {table_name}"
            )));
        }

        let table_def = self.get_table(&table_name).await?;

        // Reject DELETE on append-only tables.
        if table_def.append_only {
            return Err(ExecError::Unsupported(format!(
                "DELETE not allowed on append-only table {table_name}"
            )));
        }

        let col_meta = self.table_col_meta(&table_def);

        // Fast path: PK/unique equality WHERE → filtered scan
        let (all_rows, pre_filtered) = match Self::extract_pk_eq_value(&delete.selection, &table_def) {
            Some((col_idx, eq_value)) => {
                let matches = self.storage_for(&table_name)
                    .scan_where_eq_positions(&table_name, col_idx, &eq_value).await?;
                self.metrics.rows_scanned.inc_by(1);
                (matches, true)
            }
            None => {
                let rows = self.storage_for(&table_name).scan(&table_name).await?;
                self.metrics.rows_scanned.inc_by(rows.len() as u64);
                (rows.into_iter().enumerate().collect::<Vec<_>>(), false)
            }
        };

        // Pre-check: does this table have any DELETE triggers?
        let has_triggers = {
            let triggers = self.triggers.read().await;
            triggers.iter().any(|t| t.table_name == table_name && t.events.contains(&TriggerEvent::Delete))
        };

        // Fire BEFORE DELETE statement-level triggers
        if has_triggers {
            self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Delete, None, None, &col_meta, false).await;
        }

        let mut positions = Vec::new();
        let mut returned_rows = Vec::new();
        for (pos, row) in &all_rows {
            let matches = if pre_filtered {
                true
            } else {
                match &delete.selection {
                    Some(expr) => self.eval_where(expr, row, &col_meta)?,
                    None => true,
                }
            };
            if matches {
                // Fire BEFORE DELETE row-level triggers (old = row being deleted)
                if has_triggers {
                    self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Delete, Some(row), None, &col_meta, true).await;
                }

                if let Some(ref returning_items) = delete.returning {
                    let returned = self.eval_returning(returning_items, row, &col_meta)?;
                    returned_rows.push(returned);
                }
                positions.push(*pos);
            }
        }

        // Enforce FK actions on child tables referencing this parent before deletion.
        let deleted_rows: Vec<Row> = all_rows.iter()
            .filter(|(pos, _)| positions.contains(pos))
            .map(|(_, row)| row.clone())
            .collect();
        if !deleted_rows.is_empty() {
            self.enforce_fk_on_parent_mutation(&table_name, &deleted_rows, None, 0).await?;
        }

        // Build position→row lookup
        let row_by_pos: std::collections::HashMap<usize, &Row> = all_rows.iter()
            .map(|(pos, row)| (*pos, row))
            .collect();

        // Remove deleted rows from encrypted and vector indexes (skip if none exist)
        {
            let has_vec = self.vector_indexes.read().values().any(|e| e.table_name == table_name);
            let has_enc = self.encrypted_indexes.read().values().any(|e| e.table_name == table_name);
            if has_vec || has_enc {
                for &pos in &positions {
                    if let Some(&old_row) = row_by_pos.get(&pos) {
                        if has_enc { self.remove_from_encrypted_indexes(&table_name, old_row, pos, &table_def); }
                        if has_vec { self.remove_from_vector_indexes(&table_name, pos); }
                    }
                }
            }
        }

        let count = self.storage_for(&table_name).delete(&table_name, &positions).await?;

        // Invalidate zone map stats — row positions have shifted after delete,
        // so granule boundaries no longer align. Clear and let the next INSERT
        // repopulate.
        if count > 0 {
            let zm_table_id = table_name_to_id(&table_name);
            self.zone_map_index.clear_table(zm_table_id);
        }

        // Fire AFTER DELETE row-level triggers for each deleted row
        if has_triggers {
            for &pos in &positions {
                if let Some(&old_row) = row_by_pos.get(&pos) {
                    self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Delete, Some(old_row), None, &col_meta, true).await;
                }
            }

            // Fire AFTER DELETE statement-level triggers
            self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Delete, None, None, &col_meta, false).await;
        }

        // Notify reactive subscribers with real deleted row data
        #[cfg(feature = "server")]
        self.notify_change_rows(&table_name, ChangeType::Delete, &[], &deleted_rows, &col_meta);

        if delete.returning.is_some() && !returned_rows.is_empty() {
            let columns: Vec<(String, DataType)> = col_meta
                .iter()
                .map(|c| (c.name.clone(), c.dtype.clone()))
                .collect();
            Ok(ExecResult::Select { columns, rows: returned_rows })
        } else {
            Ok(ExecResult::Command {
                tag: "DELETE".into(),
                rows_affected: count,
            })
        }
    }

    /// Extract (col_idx, value) from a simple PK/unique equality WHERE clause.
    /// E.g., `WHERE id = 5000` → Some((0, Value::Int(5000)))
    fn extract_pk_eq_value(
        selection: &Option<ast::Expr>,
        table_def: &TableDef,
    ) -> Option<(usize, Value)> {
        let expr = selection.as_ref()?;
        if let Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } = expr {
            // Determine which side is the column and which is the literal
            let (col_name, lit_expr) = match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(ident), other) => (ident.value.to_lowercase(), other),
                (other, Expr::Identifier(ident)) => (ident.value.to_lowercase(), other),
                (Expr::CompoundIdentifier(parts), other) | (other, Expr::CompoundIdentifier(parts)) => {
                    (parts.last()?.value.to_lowercase(), other)
                }
                _ => return None,
            };
            // Check if this column is a single-column PK or UNIQUE
            let is_pk_or_unique = table_def.constraints.iter().any(|c| {
                match c {
                    crate::catalog::TableConstraint::PrimaryKey { columns }
                    | crate::catalog::TableConstraint::Unique { columns, .. } => {
                        columns.len() == 1 && columns[0].eq_ignore_ascii_case(&col_name)
                    }
                    _ => false,
                }
            });
            if !is_pk_or_unique { return None; }
            let col_idx = table_def.column_index(&col_name)?;
            // Extract literal value
            let ast_val = match lit_expr {
                Expr::Value(vws) => &vws.value,
                _ => return None,
            };
            let value = match ast_val {
                ast::Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                            Value::Int32(i as i32)
                        } else {
                            Value::Int64(i)
                        }
                    } else if let Ok(f) = n.parse::<f64>() {
                        Value::Float64(f)
                    } else {
                        return None;
                    }
                }
                ast::Value::SingleQuotedString(s) => Value::Text(s.clone()),
                _ => return None,
            };
            return Some((col_idx, value));
        }
        None
    }

}
