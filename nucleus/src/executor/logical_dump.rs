//! Logical (SQL-text) backup — T2.1 v2.
//!
//! Unlike the physical byte-copy in `crate::backup` (which is tied to the
//! on-disk page format and must be taken against a stopped instance), a logical
//! dump emits portable SQL — `CREATE TABLE` / `CREATE INDEX` / `INSERT` — that
//! **replays through the executor**, so it survives on-disk-format and
//! schema-version changes and cannot reintroduce corrupt rows (every INSERT is
//! re-checked against constraints, per T0.1). Restore into a fresh instance is
//! just running the script.
//!
//! Consistency: each table is scanned under its own MVCC snapshot, so the dump
//! is per-table consistent. For a whole-DB point-in-time image, take it against
//! a quiesced instance (as with physical backup).

use super::ExecError;
use super::helpers::value_to_text_string_impl;
use crate::catalog::{FkAction, IndexDef, TableConstraint, TableDef};
use crate::types::Value;

/// Wrap `s` as a single-quoted SQL string literal, doubling embedded quotes.
fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Render a `Value` as a SQL literal for an INSERT. Numbers/booleans are bare;
/// everything else is a quoted literal that the target column coerces on insert
/// (the same coercion an ordinary INSERT applies), except vectors which use the
/// `VECTOR('[...]')` constructor the parser expects.
pub(super) fn value_to_sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => {
            if f.is_finite() {
                f.to_string()
            } else if f.is_nan() {
                "'NaN'::double precision".to_string()
            } else if *f > 0.0 {
                "'Infinity'::double precision".to_string()
            } else {
                "'-Infinity'::double precision".to_string()
            }
        }
        Value::Numeric(n) => n.clone(),
        Value::Vector(vec) => format!(
            "VECTOR('[{}]')",
            vec.iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        // Text, Bytea, Timestamp, Date, TimestampTz, Uuid, Jsonb, Array, Interval:
        // quote their canonical text form and let column-type coercion parse it.
        other => quote_str(&value_to_text_string_impl(other)),
    }
}

fn fk_action_sql(a: &FkAction) -> &'static str {
    match a {
        FkAction::NoAction => "NO ACTION",
        FkAction::Restrict => "RESTRICT",
        FkAction::Cascade => "CASCADE",
        FkAction::SetNull => "SET NULL",
        FkAction::SetDefault => "SET DEFAULT",
    }
}

fn render_constraint(c: &TableConstraint) -> String {
    match c {
        TableConstraint::PrimaryKey { columns, .. } => {
            format!("PRIMARY KEY ({})", columns.join(", "))
        }
        TableConstraint::Unique { columns, .. } => {
            format!("UNIQUE ({})", columns.join(", "))
        }
        TableConstraint::Check { expr, .. } => format!("CHECK ({expr})"),
        TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
            ..
        } => format!(
            "FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
            columns.join(", "),
            ref_table,
            ref_columns.join(", "),
            fk_action_sql(on_delete),
            fk_action_sql(on_update),
        ),
    }
}

/// Render `CREATE TABLE` DDL for a table definition.
pub(super) fn render_create_table(def: &TableDef) -> String {
    let mut items: Vec<String> = def
        .columns
        .iter()
        .map(|c| {
            let mut s = format!("{} {}", c.name, c.data_type);
            if !c.nullable {
                s.push_str(" NOT NULL");
            }
            if let Some(d) = &c.default_expr {
                s.push_str(&format!(" DEFAULT {d}"));
            }
            s
        })
        .collect();
    for con in &def.constraints {
        items.push(render_constraint(con));
    }
    format!(
        "CREATE TABLE {} (\n  {}\n);",
        def.name,
        items.join(",\n  ")
    )
}

/// Render `CREATE INDEX` DDL. Encrypted indexes (BTree + an `encryption_mode`
/// option) round-trip as `USING encrypted`; vector/GIN/etc. use their type.
pub(super) fn render_create_index(idx: &IndexDef) -> String {
    use crate::catalog::IndexType;
    let using = if idx.options.contains_key("encryption_mode") {
        Some("encrypted".to_string())
    } else {
        match &idx.index_type {
            IndexType::BTree => None,
            other => Some(other.to_string().to_lowercase()),
        }
    };
    let unique = if idx.unique { "UNIQUE " } else { "" };
    match using {
        Some(u) => format!(
            "CREATE {unique}INDEX {} ON {} USING {u} ({});",
            idx.name,
            idx.table_name,
            idx.columns.join(", ")
        ),
        None => format!(
            "CREATE {unique}INDEX {} ON {} ({});",
            idx.name,
            idx.table_name,
            idx.columns.join(", ")
        ),
    }
}

/// Render an `INSERT` for one row against a table definition.
pub(super) fn render_insert(def: &TableDef, row: &[Value]) -> String {
    let cols: Vec<&str> = def.columns.iter().map(|c| c.name.as_str()).collect();
    let vals: Vec<String> = row.iter().map(value_to_sql_literal).collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({});",
        def.name,
        cols.join(", "),
        vals.join(", ")
    )
}

impl super::Executor {
    /// Produce a portable SQL script (CREATE TABLE + CREATE INDEX + INSERT) that
    /// reconstructs every user table when replayed through [`restore_logical`].
    pub async fn dump_logical(&self) -> Result<String, ExecError> {
        let mut out = String::new();
        out.push_str("-- Nucleus logical dump (portable SQL, replayable through the executor)\n");

        let tables = self.catalog.list_tables().await;

        // 1. Schema: CREATE TABLE for every table.
        for def in &tables {
            out.push_str(&render_create_table(def));
            out.push('\n');
        }
        // 2. Data: one INSERT per row, per table.
        for def in &tables {
            let rows = self.storage_for(&def.name).scan(&def.name).await?;
            if !rows.is_empty() {
                out.push('\n');
            }
            for row in &rows {
                out.push_str(&render_insert(def, row));
                out.push('\n');
            }
        }
        // 3. Indexes last, so the initial bulk load isn't slowed by maintenance.
        //    Skip indexes that merely back a PRIMARY KEY / UNIQUE constraint —
        //    the CREATE TABLE above already recreates those, so re-emitting them
        //    would fail with "index already exists".
        let mut wrote_index_header = false;
        for def in &tables {
            let constraint_cols: Vec<&[String]> = def
                .constraints
                .iter()
                .filter_map(|c| match c {
                    TableConstraint::PrimaryKey { columns, .. }
                    | TableConstraint::Unique { columns, .. } => Some(columns.as_slice()),
                    _ => None,
                })
                .collect();
            for idx in self.catalog.get_indexes(&def.name).await {
                if constraint_cols
                    .iter()
                    .any(|cols| *cols == idx.columns.as_slice())
                {
                    continue;
                }
                if !wrote_index_header {
                    out.push('\n');
                    wrote_index_header = true;
                }
                out.push_str(&render_create_index(&idx));
                out.push('\n');
            }
        }
        Ok(out)
    }

    /// Replay a logical dump produced by [`dump_logical`] into this instance.
    /// Statements run through the normal executor, so constraints are enforced
    /// and corrupt rows can't be reintroduced.
    pub async fn restore_logical(&self, script: &str) -> Result<(), ExecError> {
        // Strip full-line comments first so a leading `-- ...` header doesn't
        // glue onto the following statement (split is on `;`, not newlines).
        let cleaned = script
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        for stmt in split_sql_statements(&cleaned) {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.execute(trimmed).await?;
        }
        Ok(())
    }
}

/// Split a SQL script into statements on top-level `;`, respecting single-quoted
/// string literals (so a `;` inside a value doesn't split a statement). Good
/// enough for dumps this module emits (no dollar-quoting or comments mid-line).
fn split_sql_statements(script: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    let mut chars = script.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' if in_str => {
                cur.push(c);
                // Doubled '' is an escaped quote, not a terminator.
                if chars.peek() == Some(&'\'') {
                    cur.push(chars.next().unwrap());
                } else {
                    in_str = false;
                }
            }
            '\'' => {
                in_str = true;
                cur.push(c);
            }
            ';' if !in_str => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}
