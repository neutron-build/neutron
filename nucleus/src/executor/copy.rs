//! COPY command execution (COPY FROM STDIN / COPY TO STDOUT).
//!
//! Supports CSV and text formats with configurable delimiters and headers.

use super::helpers::{value_to_csv_string_impl, value_to_text_string_impl};
use super::{ExecError, ExecResult, Executor};
use crate::types::{DataType, Value};
use sqlparser::ast;

impl Executor {
    pub(super) async fn execute_copy(
        &self,
        source: ast::CopySource,
        to: bool,
        _target: ast::CopyTarget,
        options: Vec<ast::CopyOption>,
        values: Vec<Option<String>>,
    ) -> Result<ExecResult, ExecError> {
        // Parse options
        let mut format = "text".to_string();
        let mut delimiter = '\t';
        let mut header = false;

        for opt in &options {
            match opt {
                ast::CopyOption::Format(ident) => {
                    format = ident.value.to_lowercase();
                    if format == "csv" {
                        delimiter = ','; // CSV default delimiter
                    }
                }
                ast::CopyOption::Delimiter(c) => {
                    delimiter = *c;
                }
                ast::CopyOption::Header(h) => {
                    header = *h;
                }
                _ => {}
            }
        }

        // Privilege check: COPY FROM requires INSERT, COPY TO requires SELECT
        if let ast::CopySource::Table { table_name, .. } = &source {
            let tbl = table_name.to_string();
            let required = if to { "SELECT" } else { "INSERT" };
            if !self.check_privilege(&tbl, required).await {
                return Err(ExecError::PermissionDenied(format!(
                    "permission denied: COPY requires {required} privilege on {tbl}"
                )));
            }
        }

        if to {
            // COPY ... TO STDOUT
            self.execute_copy_to(source, format, delimiter, header)
                .await
        } else {
            // COPY ... FROM STDIN
            self.execute_copy_from(source, format, delimiter, header, values)
                .await
        }
    }

    pub(super) async fn execute_copy_from(
        &self,
        source: ast::CopySource,
        format: String,
        delimiter: char,
        has_header: bool,
        values: Vec<Option<String>>,
    ) -> Result<ExecResult, ExecError> {
        let table_name = match &source {
            ast::CopySource::Table { table_name, .. } => table_name.to_string(),
            ast::CopySource::Query(_) => {
                return Err(ExecError::Unsupported(
                    "COPY FROM with query not supported".into(),
                ));
            }
        };
        let table_def = self.get_table(&table_name).await?;
        let num_cols = table_def.columns.len();
        let mut count = 0;

        let mut payload_rows = self.copy_payload_rows(&values, &format, delimiter, num_cols);

        // Skip header if present
        if has_header && !payload_rows.is_empty() {
            payload_rows.remove(0);
        }

        // UNIQUE/PRIMARY KEY column sets, mirroring execute_insert.
        // ReplacingMergeTree-style tables keep multiple versions per key and so
        // opt out, exactly as they do on the INSERT path.
        let unique_col_sets: Vec<Vec<usize>> =
            if crate::columnar::replacing_config(&table_name).is_some() {
                Vec::new()
            } else {
                use crate::catalog::TableConstraint;
                table_def
                    .constraints
                    .iter()
                    .filter_map(|c| match c {
                        TableConstraint::PrimaryKey { columns, .. }
                        | TableConstraint::Unique { columns, .. } => {
                            let idxs: Vec<usize> = columns
                                .iter()
                                .filter_map(|n| table_def.column_index(n))
                                .collect();
                            (idxs.len() == columns.len()).then_some(idxs)
                        }
                        _ => None,
                    })
                    .collect()
            };
        let storage = self.storage_for(&table_name);

        for fields in &payload_rows {
            let mut row = Vec::with_capacity(num_cols);
            for (i, field) in fields.iter().enumerate() {
                if i < num_cols {
                    let parsed = match field {
                        // `\N` is the text-format NULL marker; it must not be
                        // parsed as the literal two-character string.
                        None => Value::Null,
                        Some(text) => self.parse_field(text, &table_def.columns[i].data_type),
                    };
                    row.push(parsed);
                }
            }
            // Pad with nulls if needed
            while row.len() < num_cols {
                row.push(Value::Null);
            }
            self.check_unique_constraints(&table_name, &table_def, &row, None)
                .await?;
            self.enforce_rls_new_row(&table_name, crate::security::PolicyCommand::Insert, &row)?;
            if unique_col_sets.is_empty() {
                storage.insert(&table_name, row).await?;
            } else {
                // Same atomic path INSERT uses: a bare append would let COPY be
                // the one write that can silently duplicate a primary key.
                storage
                    .insert_unique(&table_name, row, &unique_col_sets)
                    .await
                    .map_err(|e| match e {
                        crate::storage::StorageError::UniqueViolation(m) => {
                            ExecError::ConstraintViolation(format!(
                                "duplicate key value violates unique constraint: {m}"
                            ))
                        }
                        other => ExecError::Storage(other),
                    })?;
            }
            count += 1;
        }

        // COPY FROM is a bulk write but is not a Statement::Insert, so it is not
        // covered by the is_dml_write invalidation in the statement dispatcher.
        // Invalidate the query-result cache here so a previously cached SELECT
        // doesn't serve a stale (pre-COPY) row set for up to the cache TTL.
        //
        // The same gap applies to every derived structure: the rows went in
        // through a bare storage append, so B-tree/GIN/vector/encrypted postings
        // and zone maps still describe the pre-COPY table. A specialty index
        // that is merely stale (rather than absent) is worse than no index at
        // all — the scan intersects against it and silently drops the new rows.
        // Rebuild once for the whole batch rather than per row.
        if count > 0 {
            self.query_cache_invalidate_all();
            self.rebuild_table_derived_state(&table_name).await;
        }

        Ok(ExecResult::Command {
            tag: format!("COPY {count}"),
            rows_affected: count,
        })
    }

    /// Reconstruct COPY payload rows from the flat token list sqlparser returns.
    ///
    /// `Statement::Copy::values` is NOT a list of lines. `Parser::parse_tab_value`
    /// walks the payload token by token and pushes an entry on every tab *and*
    /// every newline, so row boundaries are erased and only the field sequence
    /// survives. It also emits two artifacts that must be stripped:
    ///
    ///   * a leading `Some("")` — the newline that ends the `COPY ... STDIN;`
    ///     line itself is consumed as a field terminator;
    ///   * a spurious `Some("")` after every `\N` — the NULL marker pushes
    ///     `None` without clearing the pending content, so the delimiter that
    ///     follows pushes the empty accumulator as an extra field.
    ///
    /// Treating each entry as a whole line (the previous behaviour) therefore
    /// turned an N-column row into N one-column rows, dropped `\N` fields
    /// outright, and inserted one all-NULL row per statement from the leading
    /// artifact — silent data corruption on every text-format `COPY FROM STDIN`,
    /// which is the shape `pg_dump` emits.
    ///
    /// Only the tab case loses row structure: a custom `DELIMITER` and CSV are
    /// not tokenizer-significant, so for those each surviving entry really is
    /// one line and is split here instead.
    pub(super) fn copy_payload_rows(
        &self,
        values: &[Option<String>],
        format: &str,
        delimiter: char,
        num_cols: usize,
    ) -> Vec<Vec<Option<String>>> {
        // Strip the leading newline artifact.
        let body = match values.first() {
            Some(Some(first)) if first.is_empty() => &values[1..],
            _ => values,
        };

        // Drop the empty field each `\N` leaves behind, keeping the NULL itself.
        let mut fields: Vec<Option<String>> = Vec::with_capacity(body.len());
        let mut skip_next_empty = false;
        for value in body {
            match value {
                None => {
                    fields.push(None);
                    skip_next_empty = true;
                }
                Some(text) => {
                    if skip_next_empty && text.is_empty() {
                        skip_next_empty = false;
                        continue;
                    }
                    skip_next_empty = false;
                    fields.push(Some(text.clone()));
                }
            }
        }

        let field_per_entry = format != "csv" && delimiter == '\t';
        if field_per_entry {
            if num_cols == 0 {
                return Vec::new();
            }
            return fields
                .chunks(num_cols)
                .map(|chunk| chunk.to_vec())
                .collect();
        }

        // Line-oriented: each entry is a whole line to be split here.
        fields
            .into_iter()
            .flatten()
            .map(|line| {
                if format == "csv" {
                    self.parse_csv_line(&line, delimiter)
                        .into_iter()
                        .map(Some)
                        .collect()
                } else {
                    line.split(delimiter).map(|s| Some(s.to_string())).collect()
                }
            })
            .collect()
    }

    pub(super) async fn execute_copy_to(
        &self,
        source: ast::CopySource,
        format: String,
        delimiter: char,
        include_header: bool,
    ) -> Result<ExecResult, ExecError> {
        let (columns, rows) = match &source {
            ast::CopySource::Table {
                table_name,
                columns,
            } => {
                let table_def = self.get_table(&table_name.to_string()).await?;
                let table = table_name.to_string();
                let all_rows = self.storage.scan(&table).await?;
                let all_rows =
                    self.rls_filter_rows(&table, crate::security::PolicyCommand::Select, all_rows);

                let col_names: Vec<String> = if columns.is_empty() {
                    table_def.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    columns.iter().map(|c| c.value.clone()).collect()
                };

                (col_names, all_rows)
            }
            ast::CopySource::Query(query) => {
                let result = self.execute_query(*query.clone()).await?;
                match result {
                    ExecResult::Select { columns, rows } => {
                        let col_names = columns.iter().map(|(name, _)| name.clone()).collect();
                        (col_names, rows)
                    }
                    _ => {
                        return Err(ExecError::Unsupported(
                            "COPY query did not return a result set".into(),
                        ));
                    }
                }
            }
        };

        // COPY TO buffers the entire result set and its serialized text form in
        // memory before handing it to the wire layer; bound that against the
        // shared query-memory budget so a full-table export can't OOM the box.
        let _mem = self.reserve_query_memory(Self::estimate_rows_bytes(&rows))?;

        let mut output = String::new();

        if format == "csv" {
            // CSV format
            if include_header {
                output.push_str(&self.format_csv_row(
                    &columns.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    delimiter,
                ));
                output.push('\n');
            }

            for row in &rows {
                let row_strings: Vec<String> =
                    row.iter().map(|v| self.value_to_csv_string(v)).collect();
                let row_refs: Vec<&str> = row_strings.iter().map(|s| s.as_str()).collect();
                output.push_str(&self.format_csv_row(&row_refs, delimiter));
                output.push('\n');
            }
        } else {
            // Text format (tab-delimited)
            for row in &rows {
                let row_strings: Vec<String> =
                    row.iter().map(|v| self.value_to_text_string(v)).collect();
                output.push_str(&row_strings.join(&delimiter.to_string()));
                output.push('\n');
            }
        }

        // Return a CopyOut result carrying the formatted data for the wire layer.
        let row_count = rows.len();
        Ok(ExecResult::CopyOut {
            data: output,
            row_count,
        })
    }

    pub(super) fn parse_csv_line(&self, line: &str, delimiter: char) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        // Check for escaped quote (double quote)
                        if chars.peek() == Some(&'"') {
                            current_field.push('"');
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                c if c == delimiter && !in_quotes => {
                    fields.push(current_field.clone());
                    current_field.clear();
                }
                _ => {
                    current_field.push(ch);
                }
            }
        }
        fields.push(current_field);
        fields
    }

    pub(super) fn format_csv_row(&self, fields: &[&str], delimiter: char) -> String {
        fields
            .iter()
            .map(|field| {
                // Quote field if it contains delimiter, quote, or newline
                if field.contains(delimiter)
                    || field.contains('"')
                    || field.contains('\n')
                    || field.contains('\r')
                {
                    format!("\"{}\"", field.replace('"', "\"\""))
                } else {
                    field.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(&delimiter.to_string())
    }

    pub(super) fn value_to_csv_string(&self, value: &Value) -> String {
        value_to_csv_string_impl(value)
    }

    pub(super) fn value_to_text_string(&self, value: &Value) -> String {
        value_to_text_string_impl(value)
    }

    pub(super) fn parse_field(&self, field: &str, data_type: &DataType) -> Value {
        match field {
            "" => Value::Null,    // Empty field = NULL in CSV
            "\\N" => Value::Null, // Explicit NULL marker
            s => match data_type {
                DataType::Int32 => s
                    .parse::<i32>()
                    .map(Value::Int32)
                    .unwrap_or(Value::Text(s.to_string())),
                DataType::Int64 => s
                    .parse::<i64>()
                    .map(Value::Int64)
                    .unwrap_or(Value::Text(s.to_string())),
                DataType::Float64 => s
                    .parse::<f64>()
                    .map(Value::Float64)
                    .unwrap_or(Value::Text(s.to_string())),
                DataType::Bool => match s.to_lowercase().as_str() {
                    "t" | "true" | "1" => Value::Bool(true),
                    "f" | "false" | "0" => Value::Bool(false),
                    _ => Value::Text(s.to_string()),
                },
                _ => Value::Text(s.to_string()),
            },
        }
    }
}
