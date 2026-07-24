//! COPY command execution (COPY FROM STDIN / COPY TO STDOUT).
//!
//! Supports CSV and text formats with configurable delimiters and headers.

use super::helpers::{value_to_csv_string_impl, value_to_text_string_impl};
use super::{ExecError, ExecResult, Executor};
use crate::types::{DataType, Row, Value};
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
            let tbl = crate::sql::object_name_key(table_name);
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
            ast::CopySource::Table { table_name, .. } => crate::sql::object_name_key(table_name),
            ast::CopySource::Query(_) => {
                return Err(ExecError::Unsupported(
                    "COPY FROM with query not supported".into(),
                ));
            }
        };
        if format == "binary" {
            // Binary COPY FROM decodes at the wire layer (it needs the raw
            // byte stream); the inline/embedded path only carries text lines.
            return Err(ExecError::Unsupported(
                "COPY FROM STDIN WITH (FORMAT binary) is only supported over the wire protocol"
                    .into(),
            ));
        }
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
        let is_csv = format == "csv";
        let (columns, rows) = match &source {
            ast::CopySource::Table {
                table_name,
                columns,
            } => {
                let table = crate::sql::object_name_key(table_name);
                let table_def = self.get_table(&table).await?;

                let col_names: Vec<String> = if columns.is_empty() {
                    table_def.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    columns.iter().map(|c| c.value.clone()).collect()
                };

                // Streaming COPY TO: for a full-column, non-RLS table export,
                // stream formatted chunks instead of buffering the whole table as
                // a Vec<Row> AND a String. On by default for a stream-capable wire
                // consumer (see copy_streaming_enabled); embedded/RESP/binary
                // callers materialize below. Output is byte-identical to the
                // materialized path (shared formatters). RLS or an explicit column
                // subset falls back below.
                #[cfg(feature = "server")]
                if format != "binary"
                    && columns.is_empty()
                    && !self.any_rls_active()
                    && self.copy_streaming_enabled()
                {
                    let storage = self.storage_for(&table);
                    let source_iter = Box::new(super::scan_stream::ChunkedScanIter::new(
                        storage,
                        table,
                        super::scan_stream::DEFAULT_STREAM_BATCH_ROWS,
                    ));
                    return Ok(ExecResult::CopyOutStream {
                        source: source_iter,
                        columns: col_names,
                        is_csv,
                        delimiter,
                        include_header,
                    });
                }

                let all_rows = self.storage.scan(&table).await?;
                let all_rows =
                    self.rls_filter_rows(&table, crate::security::PolicyCommand::Select, all_rows);
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

        // A named column subset projects (and reorders) the output columns.
        let (columns, rows, col_types) = match &source {
            ast::CopySource::Table {
                table_name,
                columns: named,
            } if !named.is_empty() => {
                let table = crate::sql::object_name_key(table_name);
                let table_def = self.get_table(&table).await?;
                let mut idxs = Vec::with_capacity(named.len());
                let mut types = Vec::with_capacity(named.len());
                for c in named {
                    let pos = table_def
                        .columns
                        .iter()
                        .position(|tc| tc.name.eq_ignore_ascii_case(&c.value))
                        .ok_or_else(|| ExecError::ColumnNotFound(c.value.clone()))?;
                    idxs.push(pos);
                    types.push(table_def.columns[pos].data_type.clone());
                }
                let projected: Vec<Row> = rows
                    .iter()
                    .map(|r| idxs.iter().map(|&i| r[i].clone()).collect())
                    .collect();
                (columns, projected, Some(types))
            }
            ast::CopySource::Table { table_name, .. } => {
                let table = crate::sql::object_name_key(table_name);
                let table_def = self.get_table(&table).await?;
                let types = table_def
                    .columns
                    .iter()
                    .map(|c| c.data_type.clone())
                    .collect();
                (columns, rows, Some(types))
            }
            ast::CopySource::Query(_) => (columns, rows, None),
        };

        let row_count = rows.len();
        if format == "binary" {
            // Types come from the table definition; for COPY (query) infer
            // from the first row's values.
            let types = match col_types {
                Some(t) => t,
                None => rows
                    .first()
                    .map(|r| r.iter().map(super::helpers::value_type).collect())
                    .unwrap_or_default(),
            };
            let data = encode_copy_binary(&rows, &types)?;
            return Ok(ExecResult::CopyOutBinary {
                data,
                row_count,
                columns: columns.len(),
            });
        }

        let mut output = String::new();
        if include_header {
            output.push_str(&format_copy_header(&columns, is_csv, delimiter));
        }
        output.push_str(&format_copy_body(&rows, is_csv, delimiter));

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

// ── Shared COPY TO formatting (used by both the materialized and streaming
// paths so their byte output is identical) ───────────────────────────────────

/// CSV-quote one field, matching `Executor::format_csv_row`: quote only if the
/// field contains the delimiter, a quote, or a newline; double embedded quotes.
fn csv_quote(field: &str, delimiter: char) -> String {
    if field.contains(delimiter)
        || field.contains('"')
        || field.contains('\n')
        || field.contains('\r')
    {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// The header line for a COPY TO. Only CSV emits a header; text format never
/// does (matching the historical behavior). Returns "" when not applicable.
pub(crate) fn format_copy_header(columns: &[String], is_csv: bool, delimiter: char) -> String {
    if !is_csv {
        return String::new();
    }
    let line = columns
        .iter()
        .map(|c| csv_quote(c, delimiter))
        .collect::<Vec<_>>()
        .join(&delimiter.to_string());
    format!("{line}\n")
}

/// Format a batch of rows to CSV or tab/text form (no header). Byte-identical to
/// the materialized `execute_copy_to` row loop.
pub(crate) fn format_copy_body(rows: &[Row], is_csv: bool, delimiter: char) -> String {
    let mut out = String::new();
    let d = delimiter.to_string();
    if is_csv {
        for row in rows {
            let line = row
                .iter()
                .map(|v| csv_quote(&value_to_csv_string_impl(v), delimiter))
                .collect::<Vec<_>>()
                .join(&d);
            out.push_str(&line);
            out.push('\n');
        }
    } else {
        for row in rows {
            let line = row
                .iter()
                .map(value_to_text_string_impl)
                .collect::<Vec<_>>()
                .join(&d);
            out.push_str(&line);
            out.push('\n');
        }
    }
    out
}

/// Encode rows as a complete PostgreSQL binary-COPY payload
/// (signature + per-tuple field data + trailer).
pub(super) fn encode_copy_binary(
    rows: &[Row],
    types: &[DataType],
) -> Result<Vec<u8>, ExecError> {
    let mut out = Vec::with_capacity(19 + rows.len() * (2 + types.len() * 8));
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0u32.to_be_bytes()); // flags
    out.extend_from_slice(&0u32.to_be_bytes()); // header extension length
    for row in rows {
        out.extend_from_slice(&(types.len() as i16).to_be_bytes());
        for (i, ty) in types.iter().enumerate() {
            match row.get(i).unwrap_or(&Value::Null) {
                Value::Null => out.extend_from_slice(&(-1i32).to_be_bytes()),
                v => {
                    let bytes = encode_binary_field(v, ty)?;
                    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    out.extend_from_slice(&bytes);
                }
            }
        }
    }
    out.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(out)
}

/// One value in PostgreSQL binary wire encoding, per the declared column type.
fn encode_binary_field(v: &Value, ty: &DataType) -> Result<Vec<u8>, ExecError> {
    let unsupported = || {
        ExecError::Unsupported(format!(
            "binary COPY does not support values of type {ty}"
        ))
    };
    Ok(match (v, ty) {
        (Value::Bool(b), _) => vec![*b as u8],
        (Value::Int32(n), DataType::Int64) => (*n as i64).to_be_bytes().to_vec(),
        (Value::Int32(n), DataType::Float64) => (*n as f64).to_be_bytes().to_vec(),
        (Value::Int64(n), DataType::Int32) => i32::try_from(*n)
            .map_err(|_| ExecError::Runtime(format!("integer out of range: {n}")))?
            .to_be_bytes()
            .to_vec(),
        (Value::Int64(n), DataType::Float64) => (*n as f64).to_be_bytes().to_vec(),
        (Value::Int32(n), _) => n.to_be_bytes().to_vec(),
        (Value::Int64(n), _) => n.to_be_bytes().to_vec(),
        (Value::Float64(f), _) => f.to_be_bytes().to_vec(),
        (Value::Text(s), _) => s.as_bytes().to_vec(),
        (Value::Bytea(b), _) => b.clone(),
        // Nucleus stores dates/timestamps against the PostgreSQL epoch
        // (2000-01-01), so the stored representation IS the wire encoding.
        (Value::Date(days), _) => days.to_be_bytes().to_vec(),
        (Value::Timestamp(us), _) | (Value::TimestampTz(us), _) => us.to_be_bytes().to_vec(),
        (Value::Uuid(bytes), _) => bytes.to_vec(),
        (Value::Numeric(s), _) => encode_binary_numeric(s)?,
        (Value::Jsonb(j), _) => {
            // jsonb binary format: version byte then the JSON text.
            let mut b = vec![1u8];
            b.extend_from_slice(j.to_string().as_bytes());
            b
        }
        _ => return Err(unsupported()),
    })
}

/// PostgreSQL binary NUMERIC (NBASE-10000): u16 ndigits, i16 weight,
/// u16 sign, u16 dscale, then ndigits base-10000 digit words.
fn encode_binary_numeric(text: &str) -> Result<Vec<u8>, ExecError> {
    let t = text.trim();
    let (neg, t) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (int_part, frac_part) = t.split_once('.').unwrap_or((t, ""));
    if int_part.chars().chain(frac_part.chars()).any(|c| !c.is_ascii_digit()) {
        return Err(ExecError::Runtime(format!("invalid numeric value: {text}")));
    }
    let dscale = frac_part.len() as u16;

    // Left-pad the integer digits to a 4-digit boundary, right-pad the
    // fraction, then group into base-10000 words.
    let int_digits = int_part.trim_start_matches('0');
    let lead_pad = (4 - int_digits.len() % 4) % 4;
    let mut digits: Vec<u8> = Vec::with_capacity(lead_pad + int_digits.len() + frac_part.len() + 3);
    digits.resize(lead_pad, 0);
    digits.extend(int_digits.bytes().map(|b| b - b'0'));
    let int_words = digits.len() / 4;
    digits.extend(frac_part.bytes().map(|b| b - b'0'));
    while !digits.len().is_multiple_of(4) {
        digits.push(0);
    }
    let mut words: Vec<u16> = digits
        .chunks(4)
        .map(|c| c.iter().fold(0u16, |acc, d| acc * 10 + *d as u16))
        .collect();
    let mut weight = int_words as i16 - 1;
    // Strip leading zero words (adjusting weight) and trailing zero words.
    while words.first() == Some(&0) {
        words.remove(0);
        weight -= 1;
    }
    while words.last() == Some(&0) {
        words.pop();
    }
    if words.is_empty() {
        weight = 0;
    }
    let mut out = Vec::with_capacity(8 + words.len() * 2);
    out.extend_from_slice(&(words.len() as u16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&if neg { 0x4000u16 } else { 0u16 }.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for w in words {
        out.extend_from_slice(&w.to_be_bytes());
    }
    Ok(out)
}
