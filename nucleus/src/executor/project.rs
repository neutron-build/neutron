//! Column projection logic for SELECT queries.
//!
//! Extracts the requested columns (or evaluates expressions) from full result
//! rows, producing the final projected column metadata and row data.

use sqlparser::ast::{self, Expr, SelectItem};

use crate::types::{DataType, Row};

use super::ExecError;
use super::Executor;
use super::helpers::value_type;
use super::types::{ColMeta, ProjectedResult};

impl Executor {
    // ========================================================================
    // Column projection
    // ========================================================================

    pub(super) fn project_columns(
        &self,
        projection: &[SelectItem],
        col_meta: &[ColMeta],
        rows: &[Row],
    ) -> ProjectedResult {
        // Handle SELECT *
        if projection.len() == 1 && matches!(&projection[0], SelectItem::Wildcard(_)) {
            let columns = col_meta
                .iter()
                .map(|c| (c.name.clone(), c.dtype.clone()))
                .collect();
            return Ok((columns, rows.to_vec()));
        }

        let mut col_indices = Vec::new();
        let mut columns = Vec::new();
        let mut expr_items: Vec<Option<&Expr>> = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let idx = self.resolve_column(col_meta, None, &ident.value)?;
                    columns.push((col_meta[idx].name.clone(), col_meta[idx].dtype.clone()));
                    col_indices.push(idx);
                    expr_items.push(None);
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                    let idx =
                        self.resolve_column(col_meta, Some(&parts[0].value), &parts[1].value)?;
                    columns.push((col_meta[idx].name.clone(), col_meta[idx].dtype.clone()));
                    col_indices.push(idx);
                    expr_items.push(None);
                }
                SelectItem::UnnamedExpr(expr) => {
                    // Expression projection — evaluate per row
                    if let Some(first) = rows.first() {
                        let val = self.eval_row_expr(expr, first, col_meta)?;
                        columns.push((format!("{expr}"), value_type(&val)));
                    } else {
                        columns.push((format!("{expr}"), DataType::Text));
                    }
                    col_indices.push(usize::MAX); // sentinel
                    expr_items.push(Some(expr));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some(first) = rows.first() {
                        let val = self.eval_row_expr(expr, first, col_meta)?;
                        columns.push((alias.value.clone(), value_type(&val)));
                    } else {
                        columns.push((alias.value.clone(), DataType::Text));
                    }
                    col_indices.push(usize::MAX);
                    expr_items.push(Some(expr));
                }
                SelectItem::Wildcard(_) => {
                    for (i, c) in col_meta.iter().enumerate() {
                        columns.push((c.name.clone(), c.dtype.clone()));
                        col_indices.push(i);
                        expr_items.push(None);
                    }
                }
                SelectItem::QualifiedWildcard(kind, _) => {
                    let table_name = kind.to_string();
                    // Extract last identifier component for matching aliases
                    let last_part = match kind {
                        ast::SelectItemQualifiedWildcardKind::ObjectName(obj) => {
                            obj.0.last().and_then(|p| p.as_ident()).map(|id| id.value.clone()).unwrap_or_default()
                        }
                        _ => table_name.clone(),
                    };
                    for (i, c) in col_meta.iter().enumerate() {
                        if let Some(ref tbl) = c.table
                            && (tbl.eq_ignore_ascii_case(&table_name)
                                || tbl.eq_ignore_ascii_case(&last_part))
                            {
                                columns.push((c.name.clone(), c.dtype.clone()));
                                col_indices.push(i);
                                expr_items.push(None);
                            }
                    }
                }
            }
        }

        let projected_rows: Result<Vec<Row>, ExecError> = rows
            .iter()
            .map(|row| {
                col_indices
                    .iter()
                    .zip(expr_items.iter())
                    .map(|(&idx, expr_opt)| {
                        if idx == usize::MAX {
                            let expr = expr_opt.unwrap();
                            self.eval_row_expr(expr, row, col_meta)
                        } else {
                            Ok(row[idx].clone())
                        }
                    })
                    .collect()
            })
            .collect();

        Ok((columns, projected_rows?))
    }
}
