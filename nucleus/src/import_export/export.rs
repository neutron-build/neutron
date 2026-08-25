//! Dialect export: render Nucleus tables as PostgreSQL- or SQLite-dialect SQL
//! (CREATE TABLE + INSERT), with the same per-column validation report as
//! import. Types the target dialect cannot represent degrade to TEXT (SQLite)
//! or TEXT (PostgreSQL without pgvector) and every such column is itemized.

use super::report::{
    ColumnMapping, DroppedConstraint, ReportTotals, TableReport, TableStatus, ValidationReport,
};
use crate::catalog::{FkAction, TableConstraint};
use crate::executor::{ExecResult, Executor};
use crate::types::{DataType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportTarget {
    Postgres,
    Sqlite,
}

impl ExportTarget {
    fn kind(&self) -> &'static str {
        match self {
            ExportTarget::Postgres => "postgres",
            ExportTarget::Sqlite => "sqlite",
        }
    }
}

fn map_for_target(t: DataType, target: ExportTarget) -> (String, bool, Option<String>) {
    let pg = |t: &str| (t.to_string(), true, None);
    match target {
        ExportTarget::Postgres => match t {
            DataType::Bool => pg("BOOLEAN"),
            DataType::Int32 => pg("INTEGER"),
            DataType::Int64 => pg("BIGINT"),
            DataType::Float64 => pg("DOUBLE PRECISION"),
            DataType::Text => pg("TEXT"),
            DataType::Jsonb => pg("JSONB"),
            DataType::Date => pg("DATE"),
            DataType::Timestamp => pg("TIMESTAMP"),
            DataType::TimestampTz => pg("TIMESTAMP WITH TIME ZONE"),
            DataType::Numeric => pg("NUMERIC"),
            DataType::Uuid => pg("UUID"),
            DataType::Bytea => pg("BYTEA"),
            DataType::Interval => pg("INTERVAL"),
            DataType::Array(inner) => {
                let (name, lossless, note) = map_for_target(*inner, target);
                (format!("{name}[]"), lossless, note)
            }
            DataType::Vector(_) => (
                "TEXT".to_string(),
                false,
                Some("stock PostgreSQL has no vector type (pgvector not assumed); exported as text"
                    .to_string()),
            ),
            DataType::UserDefined(name) => (
                "TEXT".to_string(),
                false,
                Some(format!(
                    "enum type '{name}' is not emitted; values exported as text"
                )),
            ),
        },
        ExportTarget::Sqlite => match t {
            DataType::Bool => pg("BOOLEAN"),
            DataType::Int32 | DataType::Int64 => pg("INTEGER"),
            DataType::Float64 => pg("REAL"),
            DataType::Text => pg("TEXT"),
            DataType::Bytea => pg("BLOB"),
            DataType::Numeric => (
                "NUMERIC".to_string(),
                false,
                Some("SQLite numeric affinity stores fractional values as REAL (53-bit mantissa); high-precision numerics may lose digits".to_string()),
            ),
            DataType::Jsonb => (
                "TEXT".to_string(),
                false,
                Some("SQLite has no JSON type; JSONB exported as JSON text".to_string()),
            ),
            DataType::Date => (
                "TEXT".to_string(),
                false,
                Some("SQLite has no date type; exported as ISO-8601 text".to_string()),
            ),
            DataType::Timestamp | DataType::TimestampTz => (
                "TEXT".to_string(),
                false,
                Some("SQLite has no timestamp type; exported as ISO-8601 text".to_string()),
            ),
            DataType::Uuid => (
                "TEXT".to_string(),
                false,
                Some("SQLite has no UUID type; exported as text".to_string()),
            ),
            DataType::Interval => (
                "TEXT".to_string(),
                false,
                Some("SQLite has no interval type; exported as text".to_string()),
            ),
            DataType::Array(_) => (
                "TEXT".to_string(),
                false,
                Some("SQLite has no array type; exported as array-literal text".to_string()),
            ),
            DataType::Vector(_) => (
                "TEXT".to_string(),
                false,
                Some("SQLite has no vector type; exported as JSON text".to_string()),
            ),
            DataType::UserDefined(name) => (
                "TEXT".to_string(),
                false,
                Some(format!(
                    "enum type '{name}' is not emitted; values exported as text"
                )),
            ),
        },
    }
}

fn fk_action(a: &FkAction) -> &'static str {
    match a {
        FkAction::NoAction => "NO ACTION",
        FkAction::Restrict => "RESTRICT",
        FkAction::Cascade => "CASCADE",
        FkAction::SetNull => "SET NULL",
        FkAction::SetDefault => "SET DEFAULT",
    }
}

fn render_constraint(c: &TableConstraint) -> Option<String> {
    let q = |names: &[String]| {
        names
            .iter()
            .map(|n| format!("\"{}\"", n.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Some(match c {
        TableConstraint::PrimaryKey { columns, .. } => format!("PRIMARY KEY ({})", q(columns)),
        TableConstraint::Unique { columns, .. } => format!("UNIQUE ({})", q(columns)),
        TableConstraint::Check { expr, .. } => format!("CHECK ({expr})"),
        TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
            ..
        } => format!(
            "FOREIGN KEY ({}) REFERENCES \"{}\" ({}) ON DELETE {} ON UPDATE {}",
            q(columns),
            ref_table.replace('"', "\"\""),
            q(ref_columns),
            fk_action(on_delete),
            fk_action(on_update)
        ),
    })
}

fn render_value(v: &Value, target: ExportTarget, out: &mut TableReport) -> String {
    let s = |t: &str| format!("'{}'", t.replace('\'', "''"));
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => match target {
            ExportTarget::Postgres => bool_lit(*b),
            ExportTarget::Sqlite => bool_lit(*b),
        },
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => {
            if f.is_finite() {
                f.to_string()
            } else {
                match target {
                    ExportTarget::Postgres => format!("'{f}'::double precision"),
                    ExportTarget::Sqlite => {
                        out.constraints_dropped.push(DroppedConstraint {
                            kind: "value".to_string(),
                            name: None,
                            definition: f.to_string(),
                            reason: "SQLite REAL cannot represent NaN/Infinity; exported as NULL"
                                .to_string(),
                        });
                        "NULL".to_string()
                    }
                }
            }
        }
        Value::Numeric(n) => n.clone(),
        Value::Vector(vec) => format!(
            "'[{}]'",
            vec.iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Bytea(b) => match target {
            ExportTarget::Postgres => format!(
                "'\\x{}'",
                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
            ),
            ExportTarget::Sqlite => format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
            ),
        },
        Value::Text(t) => s(t),
        other => s(&other.to_string()),
    }
}

fn bool_lit(b: bool) -> String {
    if b {
        "TRUE".to_string()
    } else {
        "FALSE".to_string()
    }
}

/// Export every SQL-model table in the executor's catalog to `target`-dialect
/// SQL, reporting every lossy column mapping and dropped default.
pub async fn run_export(ex: &Executor, target: ExportTarget) -> (String, ValidationReport) {
    let mut report = ValidationReport {
        direction: "export".to_string(),
        source_kind: target.kind().to_string(),
        source_detail: "nucleus catalog".to_string(),
        tables: Vec::new(),
        skipped_statements: Vec::new(),
        totals: ReportTotals::default(),
    };
    let mut sql = String::new();
    if target == ExportTarget::Sqlite {
        sql.push_str("PRAGMA foreign_keys = ON;\n");
    }
    let tables = ex.catalog().list_tables().await;
    for t in &tables {
        let mut tr = TableReport {
            name: t.name.clone(),
            status: TableStatus::Imported,
            columns: Vec::new(),
            constraints_dropped: Vec::new(),
            rows_read: 0,
            rows_imported: 0,
            rows_rejected: 0,
            rejections: Vec::new(),
            rejections_truncated: false,
        };
        report.totals.tables_seen += 1;
        report.totals.columns += t.columns.len() as u64;

        let mut col_defs = Vec::new();
        for c in &t.columns {
            let (ty, lossless, note) = map_for_target(c.data_type.clone(), target);
            if !lossless {
                report.totals.lossy_columns += 1;
            }
            tr.columns.push(ColumnMapping {
                name: c.name.clone(),
                source_type: c.data_type.to_string(),
                target_type: ty.clone(),
                lossless,
                note: note.clone(),
            });
            let mut def = format!("\"{}\" {ty}", c.name.replace('"', "\"\""));
            if !c.nullable {
                def.push_str(" NOT NULL");
            }
            match (&c.default_expr, target) {
                (Some(d), ExportTarget::Postgres) => def.push_str(&format!(" DEFAULT {d}")),
                (Some(d), ExportTarget::Sqlite) => {
                    tr.constraints_dropped.push(DroppedConstraint {
                        kind: "default".to_string(),
                        name: Some(c.name.clone()),
                        definition: format!("DEFAULT {d}"),
                        reason: "SQLite column defaults are not emitted by dialect export"
                            .to_string(),
                    });
                }
                (None, _) => {}
            }
            col_defs.push(def);
        }
        for c in &t.constraints {
            if let Some(body) = render_constraint(c) {
                col_defs.push(body);
            }
        }
        sql.push_str(&format!(
            "CREATE TABLE \"{}\" ({});\n",
            t.name.replace('"', "\"\""),
            col_defs.join(", ")
        ));

        let select = format!("SELECT * FROM \"{}\"", t.name.replace('"', "\"\""));
        match ex.execute(&select).await {
            Ok(results) => {
                for r in results {
                    if let ExecResult::Select { rows, .. } = r {
                        for row in rows {
                            tr.rows_read += 1;
                            let values = row
                                .iter()
                                .map(|v| render_value(v, target, &mut tr))
                                .collect::<Vec<_>>()
                                .join(", ");
                            sql.push_str(&format!(
                                "INSERT INTO \"{}\" VALUES ({});\n",
                                t.name.replace('"', "\"\""),
                                values
                            ));
                        }
                    }
                }
                tr.rows_imported = tr.rows_read;
            }
            Err(e) => {
                tr.status = TableStatus::Skipped {
                    reason: format!("reading rows: {e}"),
                };
            }
        }
        report.totals.tables_imported += u64::from(matches!(tr.status, TableStatus::Imported));
        report.totals.tables_skipped += u64::from(!matches!(tr.status, TableStatus::Imported));
        report.totals.constraints_dropped += tr.constraints_dropped.len() as u64;
        report.totals.rows_read += tr.rows_read;
        report.totals.rows_imported += tr.rows_imported;
        report.tables.push(tr);
    }
    (sql, report)
}
