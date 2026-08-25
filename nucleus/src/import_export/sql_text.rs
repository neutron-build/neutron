//! SQL-text reader: parses `CREATE TABLE` / `INSERT` scripts (the shape
//! `nucleus dump` and plain `pg_dump --inserts` produce) through Nucleus's own
//! PostgreSQL-dialect parser. This is the reader the validation machinery is
//! tested against, and a usable import path on its own; statements it does not
//! translate are counted in the report, never dropped silently.

use super::{
    BoxFut, RowStream, SourceColumn, SourceConstraint, SourceConstraintKind, SourceDb, SourceError,
    SourceTable, SourceValue,
};
use sqlparser::ast;
use std::collections::HashMap;

pub struct SqlTextSource {
    script: String,
    detail: String,
    tables: Vec<SourceTable>,
    /// Rows keyed by table name: (optional explicit column list, value rows).
    rows: HashMap<String, TableRows>,
    skipped: Vec<(String, u64)>,
}

/// Parsed rows for one table: per row, the explicit column list (if given)
/// and the value tuple.
pub(super) type TableRows = Vec<(Option<Vec<String>>, Vec<Vec<SourceValue>>)>;

impl SqlTextSource {
    pub fn from_script(script: String) -> Self {
        Self {
            script,
            detail: "inline SQL script".to_string(),
            tables: Vec::new(),
            rows: HashMap::new(),
            skipped: Vec::new(),
        }
    }

    pub fn from_path(path: &std::path::Path) -> Result<Self, SourceError> {
        let script = std::fs::read_to_string(path)
            .map_err(|e| SourceError(format!("reading {}: {e}", path.display())))?;
        Ok(Self {
            script,
            detail: path.display().to_string(),
            tables: Vec::new(),
            rows: HashMap::new(),
            skipped: Vec::new(),
        })
    }

    fn ensure_parsed(&mut self) -> Result<(), SourceError> {
        if !self.tables.is_empty() || !self.rows.is_empty() || !self.skipped.is_empty() {
            return Ok(());
        }
        let stmts = crate::sql::parse(&self.script)
            .map_err(|e| SourceError(format!("parsing script: {e}")))?;
        for stmt in stmts {
            match stmt {
                ast::Statement::CreateTable(ct) => {
                    let name = object_name_tail(&ct.name);
                    let mut constraints = Vec::new();
                    let mut columns = Vec::new();
                    for cdef in ct.columns {
                        let column = column_from_ast(&cdef, &mut constraints);
                        columns.push(column);
                    }
                    for tc in &ct.constraints {
                        match table_constraint(tc) {
                            Some(c) => constraints.push(c),
                            None => self.record_skipped("inline index declaration"),
                        }
                    }
                    self.tables.push(SourceTable {
                        name,
                        columns,
                        constraints,
                    });
                }
                ast::Statement::Insert(ins) => {
                    let name = match &ins.table {
                        ast::TableObject::TableName(obj) => object_name_tail(obj),
                        _ => String::new(),
                    };
                    let col_names = if ins.columns.is_empty() {
                        None
                    } else {
                        Some(ins.columns.iter().map(|i| i.value.clone()).collect())
                    };
                    let mut value_rows = Vec::new();
                    if let Some(q) = &ins.source {
                        if let ast::SetExpr::Values(values) = q.body.as_ref() {
                            for expr_row in &values.rows {
                                value_rows.push(expr_row.iter().map(expr_to_value).collect());
                            }
                        } else {
                            self.record_skipped("INSERT ... SELECT");
                        }
                    }
                    self.rows
                        .entry(name)
                        .or_default()
                        .push((col_names, value_rows));
                }
                other => {
                    let kind = statement_kind(&other);
                    self.record_skipped(kind);
                }
            }
        }
        Ok(())
    }

    fn record_skipped(&mut self, kind: &str) {
        if let Some(entry) = self.skipped.iter_mut().find(|(k, _)| k == kind) {
            entry.1 += 1;
        } else {
            self.skipped.push((kind.to_string(), 1));
        }
    }
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn object_name_tail(name: &ast::ObjectName) -> String {
    name.0
        .last()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(i) => i.value.clone(),
            _ => String::new(),
        })
        .unwrap_or_default()
}

fn index_column_name(ic: &ast::IndexColumn) -> String {
    match &ic.column.expr {
        ast::Expr::Identifier(i) => i.value.clone(),
        other => other.to_string(),
    }
}

fn column_from_ast(cdef: &ast::ColumnDef, constraints: &mut Vec<SourceConstraint>) -> SourceColumn {
    let (type_name, udt_name) = ast_type_to_name(&cdef.data_type);
    let mut nullable = true;
    let mut default_expr = None;
    for opt in &cdef.options {
        match &opt.option {
            ast::ColumnOption::NotNull => nullable = false,
            ast::ColumnOption::Null => nullable = true,
            ast::ColumnOption::Default(e) => default_expr = Some(e.to_string()),
            ast::ColumnOption::PrimaryKey(pk) => constraints.push(SourceConstraint {
                kind: SourceConstraintKind::PrimaryKey,
                name: pk.name.as_ref().map(|n| n.value.clone()),
                definition: format!("PRIMARY KEY ({})", quote(&cdef.name.value)),
            }),
            ast::ColumnOption::Unique(u) => constraints.push(SourceConstraint {
                kind: SourceConstraintKind::Unique,
                name: u.name.as_ref().map(|n| n.value.clone()),
                definition: format!("UNIQUE ({})", quote(&cdef.name.value)),
            }),
            ast::ColumnOption::ForeignKey(fk) => constraints.push(SourceConstraint {
                kind: SourceConstraintKind::ForeignKey,
                name: fk.name.as_ref().map(|n| n.value.clone()),
                definition: format!(
                    "FOREIGN KEY ({}) REFERENCES {} ({})",
                    quote(&cdef.name.value),
                    quote(&object_name_tail(&fk.foreign_table)),
                    fk.referred_columns
                        .iter()
                        .map(|c| quote(&c.value))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }),
            ast::ColumnOption::Check(c) => constraints.push(SourceConstraint {
                kind: SourceConstraintKind::Check,
                name: c.name.as_ref().map(|n| n.value.clone()),
                definition: format!("CHECK ({})", c.expr),
            }),
            _ => {}
        }
    }
    SourceColumn {
        name: cdef.name.value.clone(),
        type_name,
        udt_name,
        nullable,
        default_expr,
    }
}

/// Render a sqlparser type to (display name, pg udt-ish name). Arrays render
/// as `BASE[]`; the runner's mapper recognises the suffix and recurses.
fn ast_type_to_name(dt: &ast::DataType) -> (String, Option<String>) {
    if let ast::DataType::Array(elem) = dt {
        let base = match elem {
            ast::ArrayElemTypeDef::None => return ("ARRAY".to_string(), None),
            ast::ArrayElemTypeDef::SquareBracket(t, _) => t.as_ref().clone(),
            ast::ArrayElemTypeDef::AngleBracket(t) => t.as_ref().clone(),
            ast::ArrayElemTypeDef::Parenthesis(t) => t.as_ref().clone(),
        };
        let (inner, _) = ast_type_to_name(&base);
        return (format!("{inner}[]"), None);
    }
    (dt.to_string(), None)
}

fn table_constraint(tc: &ast::TableConstraint) -> Option<SourceConstraint> {
    match tc {
        ast::TableConstraint::Unique(u) => Some(SourceConstraint {
            kind: SourceConstraintKind::Unique,
            name: u.name.as_ref().map(|n| n.value.clone()),
            definition: format!(
                "UNIQUE ({})",
                u.columns
                    .iter()
                    .map(|c| quote(&index_column_name(c)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }),
        ast::TableConstraint::PrimaryKey(pk) => Some(SourceConstraint {
            kind: SourceConstraintKind::PrimaryKey,
            name: pk.name.as_ref().map(|n| n.value.clone()),
            definition: format!(
                "PRIMARY KEY ({})",
                pk.columns
                    .iter()
                    .map(|c| quote(&index_column_name(c)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }),
        ast::TableConstraint::ForeignKey(fk) => Some(SourceConstraint {
            kind: SourceConstraintKind::ForeignKey,
            name: fk.name.as_ref().map(|n| n.value.clone()),
            definition: format!(
                "FOREIGN KEY ({}) REFERENCES {} ({})",
                fk.columns
                    .iter()
                    .map(|c| quote(&c.value))
                    .collect::<Vec<_>>()
                    .join(", "),
                quote(&object_name_tail(&fk.foreign_table)),
                fk.referred_columns
                    .iter()
                    .map(|c| quote(&c.value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }),
        ast::TableConstraint::Check(c) => Some(SourceConstraint {
            kind: SourceConstraintKind::Check,
            name: c.name.as_ref().map(|n| n.value.clone()),
            definition: format!("CHECK ({})", c.expr),
        }),
        _ => None,
    }
}

fn expr_to_value(e: &ast::Expr) -> SourceValue {
    use ast::Expr;
    match e {
        Expr::Value(v) => value_to_source(&v.value),
        Expr::TypedString(ts) => value_to_source(&ts.value.value),
        Expr::Cast { expr, .. } | Expr::Nested(expr) => expr_to_value(expr),
        Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr,
        } => match expr_to_value(expr) {
            SourceValue::Raw(inner) => SourceValue::Raw(format!("-{inner}")),
            SourceValue::Quoted(inner) => SourceValue::Raw(format!("-{inner}")),
            other => other,
        },
        other => SourceValue::Raw(other.to_string()),
    }
}

fn value_to_source(v: &ast::Value) -> SourceValue {
    match v {
        ast::Value::Number(n, _) => SourceValue::Raw(n.clone()),
        ast::Value::SingleQuotedString(s)
        | ast::Value::DoubleQuotedString(s)
        | ast::Value::EscapedStringLiteral(s) => SourceValue::Quoted(s.clone()),
        ast::Value::Boolean(b) => SourceValue::Raw(if *b { "TRUE" } else { "FALSE" }.to_string()),
        ast::Value::Null => SourceValue::Null,
        other => SourceValue::Raw(other.to_string()),
    }
}

fn statement_kind(stmt: &ast::Statement) -> &'static str {
    use ast::Statement;
    match stmt {
        Statement::CreateIndex { .. } => "CREATE INDEX",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::Delete(_) => "DELETE",
        Statement::Update { .. } => "UPDATE",
        Statement::CreateSequence { .. } => "CREATE SEQUENCE",
        Statement::Insert { .. } => "INSERT",
        _ => "other",
    }
}

impl SourceDb for SqlTextSource {
    fn kind(&self) -> &'static str {
        "sql"
    }

    fn detail(&self) -> String {
        self.detail.clone()
    }

    fn tables(&mut self) -> BoxFut<'_, Result<Vec<SourceTable>, SourceError>> {
        Box::pin(async move {
            self.ensure_parsed()?;
            Ok(self.tables.clone())
        })
    }

    fn skipped_statement_kinds(&self) -> Vec<(String, u64)> {
        self.skipped.clone()
    }

    fn scan<'a>(
        &'a mut self,
        table: &'a SourceTable,
    ) -> BoxFut<'a, Result<Box<dyn RowStream + 'a>, SourceError>> {
        Box::pin(async move {
            self.ensure_parsed()?;
            let column_order: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
            let mut resolved: Vec<Vec<SourceValue>> = Vec::new();
            if let Some(inserts) = self.rows.get(&table.name) {
                for (cols, value_rows) in inserts {
                    match cols {
                        None => resolved.extend(value_rows.iter().cloned()),
                        Some(names) => {
                            for row in value_rows {
                                let mut full = vec![SourceValue::Null; column_order.len()];
                                for (name, val) in names.iter().zip(row.iter()) {
                                    if let Some(i) = column_order.iter().position(|c| c == name) {
                                        full[i] = val.clone();
                                    }
                                }
                                resolved.push(full);
                            }
                        }
                    }
                }
            }
            Ok(Box::new(MaterializedRows {
                rows: resolved,
                pos: 0,
            }) as Box<dyn RowStream>)
        })
    }
}

struct MaterializedRows {
    rows: Vec<Vec<SourceValue>>,
    pos: usize,
}

impl RowStream for MaterializedRows {
    fn next_batch(&mut self) -> BoxFut<'_, Result<Option<Vec<Vec<SourceValue>>>, SourceError>> {
        Box::pin(async move {
            if self.pos >= self.rows.len() {
                return Ok(None);
            }
            let take = &self.rows[self.pos..];
            let n = take.len().min(256);
            let batch = take[..n].to_vec();
            self.pos += n;
            Ok(Some(batch))
        })
    }
}
