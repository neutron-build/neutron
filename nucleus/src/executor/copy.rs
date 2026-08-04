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

        let mut payload_rows = self.copy_payload_rows(&values, &format, delimiter, num_cols);

        // Skip header if present
        if has_header && !payload_rows.is_empty() {
            payload_rows.remove(0);
        }

        let count = self
            .copy_insert_rows(&table_name, None, payload_rows)
            .await?;

        // COPY FROM is a bulk write but is not a Statement::Insert, so it is not
        // covered by the is_dml_write invalidation in the statement dispatcher.
        // Invalidate the query-result cache here so a previously cached SELECT
        // doesn't serve a stale (pre-COPY) row set for up to the cache TTL.
        // (Derived structures — B-tree/GIN/vector/encrypted postings and zone
        // maps — are now maintained by the INSERT path itself.)
        if count > 0 {
            self.query_cache_invalidate_all();
        }

        Ok(ExecResult::Command {
            tag: format!("COPY {count}"),
            rows_affected: count,
        })
    }

    /// Insert one parsed COPY payload as a single `INSERT` statement.
    ///
    /// COPY is a bulk INSERT and gets no exemption from anything INSERT
    /// enforces — and being the loader most likely to ingest untrusted data, it
    /// is the worst place to skip validation. The hand-rolled append loop this
    /// replaces skipped NOT NULL and CHECK entirely, never coerced a field to
    /// its declared column type (a DATE column got the raw `Value::Text`), and
    /// wrote row by row, so a violation partway down a payload left every
    /// earlier row inserted.
    ///
    /// `execute_insert` fixes all of that at once: it validates *every* row
    /// before it writes *any* — which is exactly PostgreSQL's all-or-nothing
    /// COPY — and it is the single place that enforces NOT NULL / CHECK / FK /
    /// enum / RLS `WITH CHECK` / UNIQUE, applies DEFAULTs to columns the
    /// payload omits, fires triggers, and maintains every derived index.
    ///
    /// Fields cross over as text literals, exactly as a client would have
    /// spelled them in `VALUES`, so the INSERT path's own coercion decides how
    /// each one lands in its declared type.
    pub(crate) async fn copy_insert_rows(
        &self,
        table: &str,
        columns: Option<&[String]>,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<usize, ExecError> {
        if rows.is_empty() {
            return Ok(0);
        }
        let insert = self.build_copy_insert(table, columns, rows)?;
        match self.execute_insert(insert).await? {
            ExecResult::Command { rows_affected, .. } => Ok(rows_affected),
            other => Err(ExecError::Runtime(format!(
                "COPY insert returned an unexpected result: {other:?}"
            ))),
        }
    }

    /// The `INSERT` statement a wire-protocol COPY payload is equivalent to.
    ///
    /// The wire handler runs this through the normal statement dispatcher, so a
    /// `\copy` gets byte-for-byte the same validation, atomicity and cache
    /// invalidation as an `INSERT` a client typed itself.
    #[cfg(feature = "server")]
    pub fn copy_insert_statement(
        &self,
        table: &str,
        columns: Option<&[String]>,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<ast::Statement, ExecError> {
        Ok(ast::Statement::Insert(
            self.build_copy_insert(table, columns, rows)?,
        ))
    }

    /// Build the `INSERT` AST for a COPY payload.
    ///
    /// A one-row skeleton is parsed and its `VALUES` swapped out: hand-building
    /// the AST would pin ~30 `Insert`/`Query` struct fields to this exact
    /// sqlparser version for no benefit, and the skeleton is a few bytes of SQL.
    fn build_copy_insert(
        &self,
        table: &str,
        columns: Option<&[String]>,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<ast::Insert, ExecError> {
        fn quote(ident: &str) -> String {
            format!("\"{}\"", ident.replace('"', "\"\""))
        }
        // The catalog key is the canonical (already case-resolved) name, so it
        // must be quoted back verbatim rather than re-folded by the parser.
        let target = table.split('.').map(quote).collect::<Vec<_>>().join(".");
        let col_clause = match columns {
            Some(cols) if !cols.is_empty() => format!(
                " ({})",
                cols.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ")
            ),
            _ => String::new(),
        };
        let skeleton = format!("INSERT INTO {target}{col_clause} VALUES (NULL)");
        let mut statements = crate::sql::parse(&skeleton)?;
        let Some(ast::Statement::Insert(mut insert)) = statements.pop() else {
            return Err(ExecError::Runtime(format!(
                "COPY target is not a valid insert target: {table}"
            )));
        };
        let value_rows: Vec<Vec<ast::Expr>> = rows
            .into_iter()
            .map(|fields| fields.into_iter().map(copy_field_expr).collect())
            .collect();
        let source = insert
            .source
            .as_mut()
            .ok_or_else(|| ExecError::Runtime("INSERT skeleton has no source".into()))?;
        *source.body = ast::SetExpr::Values(ast::Values {
            explicit_row: false,
            value_keyword: false,
            rows: value_rows,
        });
        Ok(insert)
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
                } else {
                    // Text format: `\N` is the NULL marker and nothing else is
                    // NULL — an empty field is the empty string.
                    line.split(delimiter)
                        .map(|s| (s != "\\N").then(|| s.to_string()))
                        .collect()
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
                    && !self.any_table_secured()
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
                let all_rows = self.mask_rows(&table, all_rows);
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

    /// Split one CSV line into fields. An *unquoted* empty field is NULL —
    /// PostgreSQL's default CSV NULL string is the empty string — while a
    /// *quoted* empty field (`""`) is the empty string. Collapsing the two
    /// loses a distinction PostgreSQL keeps.
    pub(super) fn parse_csv_line(&self, line: &str, delimiter: char) -> Vec<Option<String>> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut was_quoted = false;
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
                        was_quoted = true;
                    }
                }
                c if c == delimiter && !in_quotes => {
                    fields.push(csv_field(&mut current_field, was_quoted));
                    was_quoted = false;
                }
                _ => {
                    current_field.push(ch);
                }
            }
        }
        fields.push(csv_field(&mut current_field, was_quoted));
        fields
    }

    pub(super) fn value_to_text_string(&self, value: &Value) -> String {
        value_to_text_string_impl(value)
    }
}

/// One COPY field as the literal expression the INSERT path evaluates. `None`
/// is the format's NULL marker; `Some(text)` is a text literal — **including
/// `Some("")`**, the empty string, which PostgreSQL's text format distinguishes
/// from `\N`.
fn copy_field_expr(field: Option<String>) -> ast::Expr {
    ast::Expr::Value(ast::ValueWithSpan {
        value: match field {
            None => ast::Value::Null,
            Some(text) => ast::Value::SingleQuotedString(text),
        },
        span: sqlparser::tokenizer::Span::empty(),
    })
}

/// Finish one CSV field: quoted means "a value, possibly empty"; unquoted and
/// empty means NULL.
fn csv_field(current: &mut String, was_quoted: bool) -> Option<String> {
    let text = std::mem::take(current);
    (was_quoted || !text.is_empty()).then_some(text)
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
pub(super) fn encode_copy_binary(rows: &[Row], types: &[DataType]) -> Result<Vec<u8>, ExecError> {
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
    let unsupported =
        || ExecError::Unsupported(format!("binary COPY does not support values of type {ty}"));
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
    if int_part
        .chars()
        .chain(frac_part.chars())
        .any(|c| !c.is_ascii_digit())
    {
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
