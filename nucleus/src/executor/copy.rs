//! COPY command execution (COPY FROM STDIN / COPY TO STDOUT).
//!
//! Supports CSV and text formats with configurable delimiters and headers.

use sqlparser::ast;
use crate::types::{DataType, Value};
use super::{ExecError, ExecResult, Executor};
use super::helpers::{value_to_csv_string_impl, value_to_text_string_impl};

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
            self.execute_copy_to(source, format, delimiter, header).await
        } else {
            // COPY ... FROM STDIN
            self.execute_copy_from(source, format, delimiter, header, values).await
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
                return Err(ExecError::Unsupported("COPY FROM with query not supported".into()));
            }
        };
        let table_def = self.get_table(&table_name).await?;
        let num_cols = table_def.columns.len();
        let mut count = 0;

        let non_null_values: Vec<&str> = values.iter()
            .filter_map(|v| v.as_deref())
            .collect();

        let mut lines_iter = non_null_values.iter();

        // Skip header if present
        if has_header && format == "csv" {
            let _ = lines_iter.next();
        }

        for line in lines_iter {
            let fields = if format == "csv" {
                self.parse_csv_line(line, delimiter)
            } else {
                // Text format: tab-delimited
                line.split(delimiter).map(|s| s.to_string()).collect()
            };

            let mut row = Vec::with_capacity(num_cols);
            for (i, field) in fields.iter().enumerate() {
                if i < num_cols {
                    let parsed = self.parse_field(field, &table_def.columns[i].data_type);
                    row.push(parsed);
                }
            }
            // Pad with nulls if needed
            while row.len() < num_cols {
                row.push(Value::Null);
            }
            self.storage.insert(&table_name, row).await?;
            count += 1;
        }

        Ok(ExecResult::Command {
            tag: format!("COPY {count}"),
            rows_affected: count,
        })
    }

    pub(super) async fn execute_copy_to(
        &self,
        source: ast::CopySource,
        format: String,
        delimiter: char,
        include_header: bool,
    ) -> Result<ExecResult, ExecError> {
        let (columns, rows) = match &source {
            ast::CopySource::Table { table_name, columns } => {
                let table_def = self.get_table(&table_name.to_string()).await?;
                let all_rows = self.storage.scan(&table_name.to_string()).await?;

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
                        return Err(ExecError::Unsupported("COPY query did not return a result set".into()));
                    }
                }
            }
        };

        let mut output = String::new();

        if format == "csv" {
            // CSV format
            if include_header {
                output.push_str(&self.format_csv_row(&columns.iter().map(|s| s.as_str()).collect::<Vec<_>>(), delimiter));
                output.push('\n');
            }

            for row in &rows {
                let row_strings: Vec<String> = row.iter().map(|v| self.value_to_csv_string(v)).collect();
                let row_refs: Vec<&str> = row_strings.iter().map(|s| s.as_str()).collect();
                output.push_str(&self.format_csv_row(&row_refs, delimiter));
                output.push('\n');
            }
        } else {
            // Text format (tab-delimited)
            for row in &rows {
                let row_strings: Vec<String> = row.iter().map(|v| self.value_to_text_string(v)).collect();
                output.push_str(&row_strings.join(&delimiter.to_string()));
                output.push('\n');
            }
        }

        // Return a CopyOut result carrying the formatted data for the wire layer.
        let row_count = rows.len();
        Ok(ExecResult::CopyOut { data: output, row_count })
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
        fields.iter().map(|field| {
            // Quote field if it contains delimiter, quote, or newline
            if field.contains(delimiter) || field.contains('"') || field.contains('\n') || field.contains('\r') {
                format!("\"{}\"", field.replace('"', "\"\""))
            } else {
                field.to_string()
            }
        }).collect::<Vec<_>>().join(&delimiter.to_string())
    }

    pub(super) fn value_to_csv_string(&self, value: &Value) -> String {
        value_to_csv_string_impl(value)
    }

    pub(super) fn value_to_text_string(&self, value: &Value) -> String {
        value_to_text_string_impl(value)
    }

    pub(super) fn parse_field(&self, field: &str, data_type: &DataType) -> Value {
        match field {
            "" => Value::Null, // Empty field = NULL in CSV
            "\\N" => Value::Null, // Explicit NULL marker
            s => match data_type {
                DataType::Int32 => s.parse::<i32>().map(Value::Int32).unwrap_or(Value::Text(s.to_string())),
                DataType::Int64 => s.parse::<i64>().map(Value::Int64).unwrap_or(Value::Text(s.to_string())),
                DataType::Float64 => s.parse::<f64>().map(Value::Float64).unwrap_or(Value::Text(s.to_string())),
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
