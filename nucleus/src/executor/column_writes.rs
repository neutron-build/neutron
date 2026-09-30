//! Write-time rules for columns the engine fills in or bounds: stored generated
//! columns, identity columns (`GENERATED ALWAYS | BY DEFAULT AS IDENTITY`,
//! `OVERRIDING {SYSTEM | USER} VALUE`) and `varchar(n)` / `char(n)` lengths.
//!
//! The rules follow PostgreSQL 17: an explicit value for a generated column, or
//! for an ALWAYS identity column without `OVERRIDING SYSTEM VALUE`, fails with
//! SQLSTATE 428C9; a value longer than the declared length fails with 22001
//! (trailing spaces past the limit are cut, as PostgreSQL does).

use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};

use crate::catalog::{ColumnDef, ColumnGeneration, TableDef};
use crate::sql::{self, Overriding};
use crate::types::{Row, Value};

use super::types::ColMeta;
use super::{ExecError, Executor};

/// What an INSERT does with the source value for one target column.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum InsertColumn {
    /// Keep the value the statement supplied.
    Keep,
    /// Discard it and use the column's default (`OVERRIDING USER VALUE`, or a
    /// generated column whose value is computed later).
    UseDefault,
}

/// Decide how an INSERT treats `col`. `is_default` is true when the statement
/// wrote the `DEFAULT` keyword (or omitted the column).
pub(super) fn insert_column_policy(
    col: &ColumnDef,
    overriding: Option<Overriding>,
    is_default: bool,
) -> Result<InsertColumn, ExecError> {
    match &col.generation {
        None => Ok(InsertColumn::Keep),
        Some(ColumnGeneration::Stored(_)) => {
            if is_default {
                Ok(InsertColumn::UseDefault)
            } else {
                Err(ExecError::Runtime(format!(
                    "cannot insert a non-DEFAULT value into column \"{}\" (generated column)",
                    col.name
                )))
            }
        }
        Some(ColumnGeneration::IdentityAlways) => match (is_default, overriding) {
            (true, _) | (_, Some(Overriding::System)) => Ok(InsertColumn::Keep),
            (false, Some(Overriding::User)) => Ok(InsertColumn::UseDefault),
            (false, None) => Err(ExecError::Runtime(format!(
                "cannot insert a non-DEFAULT value into column \"{}\" (identity column \
                 defined as GENERATED ALWAYS; use OVERRIDING SYSTEM VALUE to override)",
                col.name
            ))),
        },
        Some(ColumnGeneration::IdentityByDefault) => {
            if !is_default && overriding == Some(Overriding::User) {
                Ok(InsertColumn::UseDefault)
            } else {
                Ok(InsertColumn::Keep)
            }
        }
    }
}

/// An UPDATE (or ON CONFLICT DO UPDATE) may set a generated or ALWAYS identity
/// column only to DEFAULT.
pub(super) fn check_update_target(col: &ColumnDef, is_default: bool) -> Result<(), ExecError> {
    let refused = match &col.generation {
        Some(ColumnGeneration::Stored(_)) => Some("generated column"),
        Some(ColumnGeneration::IdentityAlways) => {
            Some("identity column defined as GENERATED ALWAYS")
        }
        _ => None,
    };
    match refused {
        Some(kind) if !is_default => Err(ExecError::Runtime(format!(
            "column \"{}\" can only be updated to DEFAULT ({kind})",
            col.name
        ))),
        _ => Ok(()),
    }
}

/// Enforce the declared `varchar(n)` / `char(n)` length on a value about to be
/// stored. Characters, not bytes; spaces beyond the limit are cut silently.
pub(super) fn enforce_max_len(value: &mut Value, col: &ColumnDef) -> Result<(), ExecError> {
    let (Some(limit), Value::Text(text)) = (col.max_len, &mut *value) else {
        return Ok(());
    };
    let limit = limit as usize;
    let Some((cut, _)) = text.char_indices().nth(limit) else {
        return Ok(());
    };
    if text[cut..].chars().all(|c| c == ' ') {
        text.truncate(cut);
        return Ok(());
    }
    Err(ExecError::Runtime(format!(
        "value too long for type character varying({limit})"
    )))
}

impl Executor {
    /// The stored-generated columns of a table with their parsed expressions.
    pub(super) fn generated_exprs(
        &self,
        table_def: &TableDef,
    ) -> Result<Vec<(usize, Expr)>, ExecError> {
        let mut out = Vec::new();
        for (i, col) in table_def.columns.iter().enumerate() {
            let Some(ColumnGeneration::Stored(text)) = &col.generation else {
                continue;
            };
            let parsed = sql::parse(&format!("SELECT {text}"))?;
            let expr = match parsed.into_iter().next() {
                Some(Statement::Query(q)) => match *q.body {
                    SetExpr::Select(sel) => match sel.projection.into_iter().next() {
                        Some(SelectItem::UnnamedExpr(expr)) => Some(expr),
                        _ => None,
                    },
                    _ => None,
                },
                _ => None,
            };
            out.push((
                i,
                expr.ok_or_else(|| {
                    ExecError::Runtime(format!(
                        "invalid generation expression for column \"{}\"",
                        col.name
                    ))
                })?,
            ));
        }
        Ok(out)
    }

    /// Compute every stored generated column of `row` from its other columns.
    pub(super) fn apply_generated(
        &self,
        exprs: &[(usize, Expr)],
        table_def: &TableDef,
        col_meta: &[ColMeta],
        row: &mut Row,
    ) -> Result<(), ExecError> {
        for (i, expr) in exprs {
            let mut value = self.eval_row_expr(expr, row, col_meta)?;
            super::dml::coerce_value_for_write(
                &mut value,
                &table_def.columns[*i],
                self.session_time_zone()?,
            )?;
            row[*i] = value;
        }
        Ok(())
    }
}

/// A deliberately bounded set of scalar builtins implemented without session
/// state or database access. Unknown functions (including SQL UDFs) are refused
/// until the catalog carries volatility metadata and bodies can be validated.
const IMMUTABLE_GENERATION_FUNCTIONS: &[&str] = &[
    "abs",
    "lower",
    "upper",
    "length",
    "char_length",
    "character_length",
    "octet_length",
    "bit_length",
    "trim",
    "ltrim",
    "rtrim",
    "replace",
    "concat",
    "concat_ws",
    "coalesce",
    "nullif",
    "greatest",
    "least",
    "round",
    "ceil",
    "ceiling",
    "floor",
];

/// CREATE TABLE / ADD COLUMN rules for generated and identity columns
/// The supported expression subset fails closed on unverified forms.
pub(super) fn validate_generated_columns(columns: &[ColumnDef]) -> Result<(), ExecError> {
    use std::ops::ControlFlow;
    for col in columns {
        let Some(ColumnGeneration::Stored(text)) = &col.generation else {
            if col.generation.is_some()
                && col
                    .default_expr
                    .as_deref()
                    .is_some_and(|d| !d.starts_with("nextval("))
            {
                return Err(ExecError::Runtime(format!(
                    "both default and identity specified for column \"{}\"",
                    col.name
                )));
            }
            continue;
        };
        let parsed = sql::parse(&format!("SELECT {text}"))?;
        let Some(Statement::Query(q)) = parsed.into_iter().next() else {
            continue;
        };
        let SetExpr::Select(sel) = *q.body else {
            continue;
        };
        let Some(SelectItem::UnnamedExpr(expr)) = sel.projection.first() else {
            continue;
        };
        let mut problem: Option<ExecError> = None;
        let _ = sqlparser::ast::visit_expressions(expr, |e| {
            let ident = match e {
                Expr::Identifier(id) => Some(id.value.clone()),
                Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
                Expr::Function(f) => {
                    let name = f.name.to_string().to_ascii_lowercase();
                    if !IMMUTABLE_GENERATION_FUNCTIONS.contains(&name.as_str())
                        || f.over.is_some()
                        || f.filter.is_some()
                        || !f.within_group.is_empty()
                        || !matches!(f.parameters, sqlparser::ast::FunctionArguments::None)
                        || matches!(f.args, sqlparser::ast::FunctionArguments::Subquery(_))
                    {
                        problem = Some(ExecError::Runtime(format!(
                            "generation expression is not immutable: {name}()"
                        )));
                        return ControlFlow::Break(());
                    }
                    None
                }
                Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                    problem = Some(ExecError::Runtime(
                        "cannot use subquery in column generation expression".into(),
                    ));
                    return ControlFlow::Break(());
                }
                // Keep the admitted expression language bounded. In particular,
                // temporal casts and special expressions can depend on session
                // timezone/current time even without an ordinary function call.
                Expr::Value(_)
                | Expr::Nested(_)
                | Expr::BinaryOp { .. }
                | Expr::UnaryOp { .. }
                | Expr::Case { .. }
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
                | Expr::Between { .. }
                | Expr::InList { .. }
                | Expr::Like { .. }
                | Expr::ILike { .. }
                | Expr::Substring { .. }
                | Expr::Trim { .. } => None,
                _ => {
                    problem = Some(ExecError::Unsupported(
                        "unverified expression form in stored generation expression".into(),
                    ));
                    return ControlFlow::Break(());
                }
            };
            if let Some(name) = ident {
                match columns.iter().find(|c| c.name == name) {
                    None => problem = Some(ExecError::ColumnNotFound(name)),
                    Some(target)
                        if matches!(target.generation, Some(ColumnGeneration::Stored(_))) =>
                    {
                        problem = Some(ExecError::Runtime(format!(
                            "cannot use generated column \"{name}\" in column generation expression"
                        )));
                    }
                    Some(_) => {}
                }
                if problem.is_some() {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        });
        if let Some(error) = problem {
            return Err(error);
        }
    }
    Ok(())
}

/// The stored generated column, if any, whose expression reads `column`.
pub(super) fn generated_column_reading(table_def: &TableDef, column: &str) -> Option<String> {
    table_def.columns.iter().find_map(|c| match &c.generation {
        Some(ColumnGeneration::Stored(expr))
            if c.name != column
                && expr
                    .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                    .any(|token| token.eq_ignore_ascii_case(column)) =>
        {
            Some(c.name.clone())
        }
        _ => None,
    })
}
