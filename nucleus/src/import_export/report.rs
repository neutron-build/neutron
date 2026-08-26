//! Validation report for import/export runs — the deliverable's heart.
//!
//! Every lossy decision is itemized: each column's type mapping (with a note
//! when the mapping is lossy), each constraint dropped (with the DDL error
//! that forced it), and each rejected row (with the executor's error). The
//! report serializes to JSON for machines and renders a human summary; the
//! exit code makes loss visible (`exit_code(false) == 3` when anything was
//! lost) so pipelines fail loudly instead of silently importing less data
//! than they were given.

use serde::{Deserialize, Serialize};

/// One table's outcome inside a [`ValidationReport`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableReport {
    pub name: String,
    pub status: TableStatus,
    pub columns: Vec<ColumnMapping>,
    pub constraints_dropped: Vec<DroppedConstraint>,
    pub rows_read: u64,
    pub rows_imported: u64,
    pub rows_rejected: u64,
    /// Itemized rejections, capped at the run's `max_itemized_rejections`;
    /// `rows_rejected` is always the true count.
    pub rejections: Vec<RowRejection>,
    pub rejections_truncated: bool,
    /// Itemized cell-level value losses (S95 findings 10-12), capped like
    /// `rejections`; the true count is `values_dropped` in
    /// [`ReportTotals`]. Additive with a default so reports written before
    /// it existed still deserialize.
    #[serde(default)]
    pub values_dropped: Vec<DroppedValue>,
    #[serde(default)]
    pub values_dropped_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TableStatus {
    Imported,
    Skipped { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnMapping {
    pub name: String,
    pub source_type: String,
    pub target_type: String,
    pub lossless: bool,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DroppedConstraint {
    pub kind: String,
    pub name: Option<String>,
    pub definition: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RowRejection {
    /// 1-based ordinal of the row within the source table scan.
    pub row_number: u64,
    pub reason: String,
}

/// A cell-level value the run could not carry over intact (S95 findings
/// 10-12): a source value with no column to land in, a truncated arity
/// mismatch, a lossy UTF-8 replacement, or a non-finite number exported as
/// NULL. Unlike a [`RowRejection`] the row itself was carried — this records
/// the loss inside it, naming the column and the value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DroppedValue {
    /// 1-based ordinal of the row within the table scan.
    pub row_number: u64,
    /// The column the value belonged to, when known.
    pub column: Option<String>,
    /// The value as the source rendered it.
    pub value: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ReportTotals {
    pub tables_seen: u64,
    pub tables_imported: u64,
    pub tables_skipped: u64,
    pub columns: u64,
    pub lossy_columns: u64,
    pub constraints_dropped: u64,
    pub rows_read: u64,
    pub rows_imported: u64,
    pub rows_rejected: u64,
    /// The true count of cell-level value losses across all tables (S95
    /// findings 10-12). Defaulted so pre-S95 reports still deserialize.
    #[serde(default)]
    pub values_dropped: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkippedStatement {
    /// Statement head, e.g. "CREATE INDEX", "CREATE VIEW".
    pub kind: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ValidationReport {
    /// "import" or "export".
    pub direction: String,
    /// Source kind ("postgresql", "sqlite", "sql") or export target name.
    pub source_kind: String,
    /// Connection string (password redacted) or file path.
    pub source_detail: String,
    pub tables: Vec<TableReport>,
    #[serde(default)]
    pub skipped_statements: Vec<SkippedStatement>,
    pub totals: ReportTotals,
}

impl ValidationReport {
    pub fn has_loss(&self) -> bool {
        self.totals.tables_skipped > 0
            || self.totals.lossy_columns > 0
            || self.totals.constraints_dropped > 0
            || self.totals.rows_rejected > 0
            || self.totals.values_dropped > 0
    }

    /// Process exit code: 0 clean, 3 completed-with-losses (unless
    /// `allow_lossy`), mirroring the CLI contract.
    pub fn exit_code(&self, allow_lossy: bool) -> i32 {
        if self.has_loss() && !allow_lossy {
            3
        } else {
            0
        }
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn human_summary(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "Nucleus {} report: source={} {}\n",
            self.direction, self.source_kind, self.source_detail
        ));
        for t in &self.tables {
            match &t.status {
                TableStatus::Imported => {
                    let verb = if self.direction == "export" {
                        "exported"
                    } else {
                        "imported"
                    };
                    out.push_str(&format!(
                        "  table {}: {} rows read, {} {}, {} rejected\n",
                        t.name, t.rows_read, t.rows_imported, verb, t.rows_rejected
                    ));
                }
                TableStatus::Skipped { reason } => {
                    out.push_str(&format!("  table {}: SKIPPED ({})\n", t.name, reason));
                }
            }
            for c in &t.columns {
                if !c.lossless {
                    out.push_str(&format!(
                        "    LOSSY column {}: {} -> {} ({})\n",
                        c.name,
                        c.source_type,
                        c.target_type,
                        c.note.as_deref().unwrap_or("no Nucleus equivalent")
                    ));
                }
            }
            for d in &t.constraints_dropped {
                out.push_str(&format!(
                    "    DROPPED {} constraint {} ({}): {}\n",
                    d.kind,
                    d.name.as_deref().unwrap_or("<unnamed>"),
                    d.definition,
                    d.reason
                ));
            }
            for r in &t.rejections {
                out.push_str(&format!(
                    "    REJECTED row {}: {}\n",
                    r.row_number, r.reason
                ));
            }
            if t.rejections_truncated {
                out.push_str(&format!(
                    "    ... {} of {} rejections itemized\n",
                    t.rejections.len(),
                    t.rows_rejected
                ));
            }
            for v in &t.values_dropped {
                out.push_str(&format!(
                    "    VALUE LOST row {} column {}: {} (was: {})\n",
                    v.row_number,
                    v.column.as_deref().unwrap_or("<unknown>"),
                    v.reason,
                    v.value
                ));
            }
            if t.values_dropped_truncated {
                out.push_str(&format!(
                    "    ... {} value losses itemized\n",
                    t.values_dropped.len()
                ));
            }
        }
        let tot = &self.totals;
        for s in &self.skipped_statements {
            out.push_str(&format!("  SKIPPED statements: {} x{}\n", s.kind, s.count));
        }
        let verb = if self.direction == "export" {
            "exported"
        } else {
            "imported"
        };
        out.push_str(&format!(
            "totals: tables {}/{} {verb} ({} skipped), columns {} mapped ({} lossy), \
             constraints_dropped: {}, rows_read: {}, rows_{verb}: {}, rows_rejected: {}, \
             values_dropped: {}\n",
            tot.tables_imported,
            tot.tables_seen,
            tot.tables_skipped,
            tot.columns,
            tot.lossy_columns,
            tot.constraints_dropped,
            tot.rows_read,
            tot.rows_imported,
            tot.rows_rejected,
            tot.values_dropped
        ));
        if self.has_loss() {
            out.push_str(
                "RESULT: LOSSY — see the itemized lines above; rerun with --allow-lossy to exit 0\n",
            );
        } else {
            out.push_str("RESULT: lossless\n");
        }
        out
    }
}
