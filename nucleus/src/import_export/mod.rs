//! Import/export between Nucleus and foreign databases, with validation
//! reports (S98 / DATABASE_COMPLETION 12.3).
//!
//! The runner is source-agnostic: a [`SourceDb`] yields tables (columns,
//! constraints) and row batches; the runner maps types, replays DDL and rows
//! through the executor, and records every lossy decision in a
//! [`ValidationReport`]. Readers ship for PostgreSQL (over the wire),
//! SQLite (via `rusqlite`, feature-gated) and plain SQL text.
//!
//! Nothing is dropped silently: unmapped types become TEXT with a per-column
//! note, constraints the target DDL rejects are dropped one category at a
//! time (each with the error that forced it), and rows the executor rejects
//! are itemized with their error. The report's exit code surfaces loss.

mod report;
mod sql_text;
mod type_map;

pub use report::{
    ColumnMapping, DroppedConstraint, ReportTotals, RowRejection, SkippedStatement, TableReport,
    TableStatus, ValidationReport,
};
pub use sql_text::SqlTextSource;
pub use type_map::{MappedType, map_pg_type, map_sqlite_type};

#[cfg(feature = "server")]
pub mod pg_source;
#[cfg(feature = "server")]
pub use pg_source::PgSource;

#[cfg(feature = "rusqlite")]
pub mod sqlite_source;
#[cfg(feature = "rusqlite")]
pub use sqlite_source::SqliteSource;

mod export;
pub use export::{ExportTarget, run_export};

#[cfg(test)]
mod tests;

use crate::executor::Executor;
use std::future::Future;
use std::pin::Pin;

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A source value in rendered-literal form. `Raw` is emitted verbatim (numbers,
/// TRUE/FALSE, constructor expressions); `Quoted` is a string literal escaped
/// at render time and coerced by the target column type on insert.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceValue {
    Null,
    Raw(String),
    Quoted(String),
}

impl SourceValue {
    fn render(&self) -> String {
        match self {
            SourceValue::Null => "NULL".to_string(),
            SourceValue::Raw(s) => s.clone(),
            SourceValue::Quoted(s) => format!("'{}'", s.replace('\'', "''")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceColumn {
    pub name: String,
    /// Source type name as the source reports it (PG data_type/udt_name,
    /// SQLite declared type, SQL-text rendered type).
    pub type_name: String,
    pub udt_name: Option<String>,
    pub nullable: bool,
    pub default_expr: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceConstraintKind {
    PrimaryKey,
    Unique,
    Check,
    ForeignKey,
}

impl SourceConstraintKind {
    fn as_str(self) -> &'static str {
        match self {
            SourceConstraintKind::PrimaryKey => "primary key",
            SourceConstraintKind::Unique => "unique",
            SourceConstraintKind::Check => "check",
            SourceConstraintKind::ForeignKey => "foreign key",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceConstraint {
    pub kind: SourceConstraintKind,
    pub name: Option<String>,
    /// Constraint body usable inside CREATE TABLE, e.g. `PRIMARY KEY ("id")`.
    pub definition: String,
}

#[derive(Debug, Clone)]
pub struct SourceTable {
    pub name: String,
    pub columns: Vec<SourceColumn>,
    pub constraints: Vec<SourceConstraint>,
}

#[derive(Debug, Clone)]
pub struct SourceError(pub String);

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Pluggable foreign-source reader. Methods return boxed futures so async
/// (PostgreSQL over the wire) and in-memory (SQL text) readers implement the
/// same trait without an async-trait dependency.
pub trait SourceDb: Send {
    fn kind(&self) -> &'static str;
    /// Human-facing detail (URL with password redacted, file path).
    fn detail(&self) -> String;
    fn tables(&mut self) -> BoxFut<'_, Result<Vec<SourceTable>, SourceError>>;
    /// Statement kinds the reader saw but does not translate (e.g. CREATE
    /// INDEX), so the report can say so instead of dropping them silently.
    fn skipped_statement_kinds(&self) -> Vec<(String, u64)> {
        Vec::new()
    }
    fn scan<'a>(
        &'a mut self,
        table: &'a SourceTable,
    ) -> BoxFut<'a, Result<Box<dyn RowStream + 'a>, SourceError>>;
}

pub trait RowStream: Send {
    /// Next batch of rows, or `None` at end of table.
    fn next_batch<'a>(
        &'a mut self,
    ) -> BoxFut<'a, Result<Option<Vec<Vec<SourceValue>>>, SourceError>>;
}

#[derive(Debug, Clone)]
pub struct ImportOptions {
    pub batch_rows: usize,
    pub max_itemized_rejections: usize,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            batch_rows: 256,
            max_itemized_rejections: 1000,
        }
    }
}

pub struct ImportOutcome {
    pub report: ValidationReport,
    /// Fatal error before any table was processed (connection/read failure).
    pub fatal: Option<String>,
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

struct PlannedColumn {
    source: SourceColumn,
    mapped: MappedType,
}

/// Constraint categories, dropped in this order when DDL fails: expressions
/// the target cannot parse come off first; column identity (the PK) last.
#[derive(Clone, Copy, PartialEq)]
enum LadderStep {
    Checks,
    ForeignKeys,
    Uniques,
    PrimaryKey,
    Defaults,
}

impl LadderStep {
    fn all() -> [LadderStep; 5] {
        [
            LadderStep::Checks,
            LadderStep::ForeignKeys,
            LadderStep::Uniques,
            LadderStep::PrimaryKey,
            LadderStep::Defaults,
        ]
    }
}

fn build_create_table(
    table: &str,
    planned: &[PlannedColumn],
    constraints: &[SourceConstraint],
    dropped_so_far: &[LadderStep],
) -> String {
    let off = |step: LadderStep| dropped_so_far.contains(&step);
    let mut cols: Vec<String> = planned
        .iter()
        .map(|p| {
            let mut s = format!("{} {}", quote_ident(&p.source.name), p.mapped.data_type);
            if !p.source.nullable {
                s.push_str(" NOT NULL");
            }
            if !off(LadderStep::Defaults)
                && let Some(d) = &p.source.default_expr
            {
                s.push_str(&format!(" DEFAULT {d}"));
            }
            s
        })
        .collect();
    for c in constraints {
        let keep = match c.kind {
            SourceConstraintKind::Check => !off(LadderStep::Checks),
            SourceConstraintKind::ForeignKey => !off(LadderStep::ForeignKeys),
            SourceConstraintKind::Unique => !off(LadderStep::Uniques),
            SourceConstraintKind::PrimaryKey => !off(LadderStep::PrimaryKey),
        };
        if !keep {
            continue;
        }
        match &c.name {
            Some(n) => cols.push(format!("CONSTRAINT {} {}", quote_ident(n), c.definition)),
            None => cols.push(c.definition.clone()),
        }
    }
    format!("CREATE TABLE {} ({})", quote_ident(table), cols.join(", "))
}

fn map_columns(table: &SourceTable, kind: &str) -> Vec<PlannedColumn> {
    table
        .columns
        .iter()
        .map(|c| {
            let mapped = match kind {
                "sqlite" => map_sqlite_type(&c.type_name),
                _ => match c.type_name.strip_suffix("[]") {
                    Some(base) => {
                        let inner = map_pg_type(base, c.udt_name.as_deref().unwrap_or(""));
                        MappedType {
                            data_type: crate::types::DataType::Array(Box::new(inner.data_type)),
                            lossless: inner.lossless,
                            note: inner.note,
                        }
                    }
                    None => map_pg_type(&c.type_name, c.udt_name.as_deref().unwrap_or("")),
                },
            };
            PlannedColumn {
                source: c.clone(),
                mapped,
            }
        })
        .collect()
}

/// Import every table from `source` into `ex`, recording every lossy decision.
pub async fn run_import(
    ex: &Executor,
    source: &mut dyn SourceDb,
    opts: &ImportOptions,
) -> ImportOutcome {
    let mut report = ValidationReport {
        direction: "import".to_string(),
        source_kind: source.kind().to_string(),
        source_detail: source.detail(),
        tables: Vec::new(),
        skipped_statements: Vec::new(),
        totals: ReportTotals::default(),
    };
    let tables = match source.tables().await {
        Ok(t) => t,
        Err(e) => {
            return ImportOutcome {
                report,
                fatal: Some(format!("listing source tables: {e}")),
            };
        }
    };
    report.skipped_statements = source
        .skipped_statement_kinds()
        .into_iter()
        .map(|(kind, count)| SkippedStatement { kind, count })
        .collect();
    for table in tables {
        let mut tr = TableReport {
            name: table.name.clone(),
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
        report.totals.columns += table.columns.len() as u64;

        let planned = map_columns(&table, source.kind());
        for p in &planned {
            if !p.mapped.lossless {
                report.totals.lossy_columns += 1;
            }
            tr.columns.push(ColumnMapping {
                name: p.source.name.clone(),
                source_type: p.source.type_name.clone(),
                target_type: p.mapped.data_type.to_string(),
                lossless: p.mapped.lossless,
                note: p.mapped.note.clone(),
            });
        }

        match create_with_ladder(ex, &table, &planned, &mut tr).await {
            CreateOutcome::Created => {}
            CreateOutcome::Failed(err) => {
                tr.status = TableStatus::Skipped {
                    reason: format!("CREATE TABLE failed at every relaxation step: {err}"),
                };
            }
        }
        if matches!(tr.status, TableStatus::Imported) {
            import_rows(ex, source, &table, &planned, opts, &mut tr).await;
            if matches!(tr.status, TableStatus::Imported) {
                report.totals.tables_imported += 1;
            } else {
                report.totals.tables_skipped += 1;
            }
        } else {
            report.totals.tables_skipped += 1;
        }
        report.totals.constraints_dropped += tr.constraints_dropped.len() as u64;
        report.totals.rows_read += tr.rows_read;
        report.totals.rows_imported += tr.rows_imported;
        report.totals.rows_rejected += tr.rows_rejected;
        report.tables.push(tr);
    }
    ImportOutcome {
        report,
        fatal: None,
    }
}

enum CreateOutcome {
    Created,
    Failed(String),
}

/// Try CREATE TABLE with everything; then with each single constraint
/// category dropped (so one bad foreign key does not shed healthy checks);
/// then accumulating drops in order (checks, foreign keys, uniques, primary
/// key, defaults); then bare. Each dropped constraint is recorded with the
/// error that forced the first relaxation.
async fn create_with_ladder(
    ex: &Executor,
    table: &SourceTable,
    planned: &[PlannedColumn],
    tr: &mut TableReport,
) -> CreateOutcome {
    async fn attempt(
        ex: &Executor,
        table: &SourceTable,
        planned: &[PlannedColumn],
        dropped: &[LadderStep],
    ) -> Result<(), String> {
        let ddl = build_create_table(&table.name, planned, &table.constraints, dropped);
        ex.execute(&ddl)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    let full_err = match attempt(ex, table, planned, &[]).await {
        Ok(_) => return CreateOutcome::Created,
        Err(e) => e,
    };
    for step in LadderStep::all() {
        if attempt(ex, table, planned, &[step]).await.is_ok() {
            record_step_drop(table, planned, step, &full_err, tr);
            return CreateOutcome::Created;
        }
    }
    let mut dropped: Vec<LadderStep> = Vec::new();
    let mut last_err = full_err.clone();
    for step in LadderStep::all() {
        dropped.push(step);
        match attempt(ex, table, planned, &dropped).await {
            Ok(_) => {
                for step in dropped {
                    record_step_drop(table, planned, step, &full_err, tr);
                }
                return CreateOutcome::Created;
            }
            Err(e) => last_err = e,
        }
    }
    CreateOutcome::Failed(last_err)
}

fn record_step_drop(
    table: &SourceTable,
    planned: &[PlannedColumn],
    step: LadderStep,
    err: &str,
    tr: &mut TableReport,
) {
    let kind = match step {
        LadderStep::Checks => Some(SourceConstraintKind::Check),
        LadderStep::ForeignKeys => Some(SourceConstraintKind::ForeignKey),
        LadderStep::Uniques => Some(SourceConstraintKind::Unique),
        LadderStep::PrimaryKey => Some(SourceConstraintKind::PrimaryKey),
        LadderStep::Defaults => None,
    };
    let reason = format!("CREATE TABLE failed: {err}");
    match kind {
        Some(k) => {
            for c in table.constraints.iter().filter(|c| c.kind == k) {
                tr.constraints_dropped.push(DroppedConstraint {
                    kind: c.kind.as_str().to_string(),
                    name: c.name.clone(),
                    definition: c.definition.clone(),
                    reason: reason.clone(),
                });
            }
        }
        None => {
            for p in planned {
                if let Some(d) = &p.source.default_expr {
                    tr.constraints_dropped.push(DroppedConstraint {
                        kind: "default".to_string(),
                        name: Some(p.source.name.clone()),
                        definition: format!("DEFAULT {d}"),
                        reason: reason.clone(),
                    });
                }
            }
        }
    }
}

async fn import_rows(
    ex: &Executor,
    source: &mut dyn SourceDb,
    table: &SourceTable,
    planned: &[PlannedColumn],
    opts: &ImportOptions,
    tr: &mut TableReport,
) {
    let mut scan = match source.scan(table).await {
        Ok(s) => s,
        Err(e) => {
            tr.status = TableStatus::Skipped {
                reason: format!("table created but source rows unreadable: {e}"),
            };
            return;
        }
    };
    let col_list = planned
        .iter()
        .map(|p| quote_ident(&p.source.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut pending: Vec<Vec<SourceValue>> = Vec::with_capacity(opts.batch_rows);
    let mut row_no: u64 = 0;
    let mut read_failed = false;
    loop {
        let batch = match scan.next_batch().await {
            Ok(Some(b)) => b,
            Ok(None) => break,
            Err(e) => {
                read_failed = true;
                record_rejection(
                    tr,
                    row_no + 1,
                    &format!("source read error, remainder of table lost: {e}"),
                    opts,
                );
                break;
            }
        };
        for row in batch {
            row_no += 1;
            tr.rows_read += 1;
            pending.push(row);
            if pending.len() >= opts.batch_rows {
                flush_batch(ex, &table.name, &col_list, &mut pending, row_no, tr, opts).await;
            }
        }
    }
    if !pending.is_empty() {
        flush_batch(ex, &table.name, &col_list, &mut pending, row_no, tr, opts).await;
    }
    if read_failed {
        tr.status = TableStatus::Skipped {
            reason: format!(
                "source read failed after {} rows; counts above are partial",
                tr.rows_read
            ),
        };
    }
}

/// Insert `rows` as one multi-row statement; on failure retry row-by-row so
/// one bad row does not discard its batch-mates. `last_row_no` is the ordinal
/// of the final row in this batch (rows are contiguous).
async fn flush_batch(
    ex: &Executor,
    table: &str,
    col_list: &str,
    rows: &mut Vec<Vec<SourceValue>>,
    last_row_no: u64,
    tr: &mut TableReport,
    opts: &ImportOptions,
) {
    let count = rows.len();
    let values = rows
        .iter()
        .map(|r| {
            format!(
                "({})",
                r.iter().map(|v| v.render()).collect::<Vec<_>>().join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_ident(table),
        col_list,
        values
    );
    match ex.execute(&sql).await {
        Ok(_) => {
            tr.rows_imported += count as u64;
            rows.clear();
        }
        Err(_) => {
            let first_row_no = last_row_no + 1 - count as u64;
            for (i, row) in rows.iter().enumerate() {
                let one = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    quote_ident(table),
                    col_list,
                    row.iter()
                        .map(|v| v.render())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                match ex.execute(&one).await {
                    Ok(_) => tr.rows_imported += 1,
                    Err(e) => {
                        record_rejection(tr, first_row_no + i as u64, &e.to_string(), opts);
                    }
                }
            }
            rows.clear();
        }
    }
}

fn record_rejection(tr: &mut TableReport, row_number: u64, reason: &str, opts: &ImportOptions) {
    tr.rows_rejected += 1;
    if tr.rejections.len() < opts.max_itemized_rejections {
        tr.rejections.push(RowRejection {
            row_number,
            reason: reason.to_string(),
        });
    } else {
        tr.rejections_truncated = true;
    }
}
