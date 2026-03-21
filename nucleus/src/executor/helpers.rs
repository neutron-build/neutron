//! Free helper functions used throughout the executor.
//!
//! These are module-level functions (not methods on Executor) that handle
//! type conversions, comparisons, parsing, and utility operations.

use std::collections::HashMap;
use sqlparser::ast::{self, Expr};
use crate::types::{DataType, Row, Value};
use crate::graph::PropValue as GraphPropValue;
use super::ExecError;
use super::schema_types::Privilege;
use super::types::ColMeta;
use crate::geo;
use crate::timeseries;

/// Map a Nucleus DataType to its PostgreSQL `udt_name` (the short type name used in pg_type).
pub(super) fn datatype_to_udt_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Bool => "bool",
        DataType::Int32 => "int4",
        DataType::Int64 => "int8",
        DataType::Float64 => "float8",
        DataType::Text => "text",
        DataType::Jsonb => "jsonb",
        DataType::Date => "date",
        DataType::Timestamp => "timestamp",
        DataType::TimestampTz => "timestamptz",
        DataType::Numeric => "numeric",
        DataType::Uuid => "uuid",
        DataType::Bytea => "bytea",
        DataType::Array(_) => "_text",
        DataType::Vector(_) => "vector",
        DataType::Interval => "interval",
        DataType::UserDefined(_) => "text",
    }
}

/// Return (oid, typlen, typtype, typcategory) for a Nucleus DataType,
/// matching real PostgreSQL pg_type values.
pub(super) fn pg_type_info(dt: &DataType) -> (i32, i32, &'static str, &'static str) {
    match dt {
        DataType::Bool      => (16,   1,  "b", "B"),
        DataType::Int32     => (23,   4,  "b", "N"),
        DataType::Int64     => (20,   8,  "b", "N"),
        DataType::Float64   => (701,  8,  "b", "N"),
        DataType::Text      => (25,  -1,  "b", "S"),
        DataType::Jsonb     => (3802, -1, "b", "U"),
        DataType::Date      => (1082,  4, "b", "D"),
        DataType::Timestamp => (1114,  8, "b", "D"),
        DataType::TimestampTz => (1184, 8, "b", "D"),
        DataType::Numeric   => (1700, -1, "b", "N"),
        DataType::Uuid      => (2950, 16, "b", "U"),
        DataType::Bytea     => (17,   -1, "b", "U"),
        DataType::Array(_)  => (1009, -1, "b", "A"),
        DataType::Vector(_) => (16385, -1, "b", "U"), // Custom OID for vector type
        DataType::Interval => (1186, 16, "b", "T"),  // PostgreSQL interval OID
        DataType::UserDefined(_) => (25, -1, "e", "E"), // enum → text-like, typtype='e'
    }
}

/// Base PostgreSQL types that should always appear in pg_type.
pub(super) const BASE_PG_TYPES: &[(i32, &str, i32, &str, &str)] = &[
    (16,   "bool",        1,  "b", "B"),
    (23,   "int4",        4,  "b", "N"),
    (20,   "int8",        8,  "b", "N"),
    (701,  "float8",      8,  "b", "N"),
    (25,   "text",       -1,  "b", "S"),
    (3802, "jsonb",      -1,  "b", "U"),
    (1082, "date",        4,  "b", "D"),
    (1114, "timestamp",   8,  "b", "D"),
    (1184, "timestamptz", 8,  "b", "D"),
    (1700, "numeric",    -1,  "b", "N"),
    (2950, "uuid",       16,  "b", "U"),
    (17,   "bytea",      -1,  "b", "U"),
    (21,   "int2",        2,  "b", "N"),
    (700,  "float4",      4,  "b", "N"),
    (1043, "varchar",    -1,  "b", "S"),
    (1042, "bpchar",     -1,  "b", "S"),
];

/// Return (unit, category, short_desc) metadata for a setting name.
pub(super) fn pg_setting_metadata(name: &str) -> (&'static str, &'static str, &'static str) {
    match name {
        "search_path" => ("", "Client Connection Defaults", "Sets the schema search order for names that are not schema-qualified."),
        "client_encoding" => ("", "Client Connection Defaults", "Sets the client-side encoding (character set)."),
        "standard_conforming_strings" => ("", "Version and Platform Compatibility", "Causes '...' strings to treat backslashes literally."),
        "timezone" => ("", "Client Connection Defaults", "Sets the time zone for displaying and interpreting time stamps."),
        _ => ("", "Ungrouped", ""),
    }
}

pub(super) fn value_type(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Text,
        Value::Bool(_) => DataType::Bool,
        Value::Int32(_) => DataType::Int32,
        Value::Int64(_) => DataType::Int64,
        Value::Float64(_) => DataType::Float64,
        Value::Text(_) => DataType::Text,
        Value::Jsonb(_) => DataType::Jsonb,
        Value::Date(_) => DataType::Date,
        Value::Timestamp(_) => DataType::Timestamp,
        Value::TimestampTz(_) => DataType::TimestampTz,
        Value::Numeric(_) => DataType::Numeric,
        Value::Uuid(_) => DataType::Uuid,
        Value::Bytea(_) => DataType::Bytea,
        Value::Array(_) => DataType::Array(Box::new(DataType::Text)),
        Value::Vector(v) => DataType::Vector(v.len()),
        Value::Interval { .. } => DataType::Interval,
    }
}

pub(super) fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int32(a), Value::Int32(b)) => Some(a.cmp(b)),
        (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
        (Value::Int32(a), Value::Int64(b)) => Some((*a as i64).cmp(b)),
        (Value::Int64(a), Value::Int32(b)) => Some(a.cmp(&(*b as i64))),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
        // Cross-type: int ↔ float promotion
        (Value::Int32(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
        (Value::Float64(a), Value::Int32(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Int64(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
        (Value::Float64(a), Value::Int64(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        (Value::Jsonb(a), Value::Jsonb(b)) => {
            let sa = serde_json::to_string(a).unwrap_or_default();
            let sb = serde_json::to_string(b).unwrap_or_default();
            Some(sa.cmp(&sb))
        }
        (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::TimestampTz(a), Value::TimestampTz(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::TimestampTz(b)) => Some(a.cmp(b)),
        (Value::TimestampTz(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::Numeric(a), Value::Numeric(b)) => {
            let fa: f64 = a.parse().unwrap_or(0.0);
            let fb: f64 = b.parse().unwrap_or(0.0);
            fa.partial_cmp(&fb)
        }
        (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
        (Value::Bytea(a), Value::Bytea(b)) => Some(a.cmp(b)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        (Value::Null, _) => Some(std::cmp::Ordering::Less),
        (_, Value::Null) => Some(std::cmp::Ordering::Greater),
        _ => None,
    }
}

/// Compare two values for ORDER BY, respecting NULLS FIRST / NULLS LAST and ASC / DESC.
/// PostgreSQL default: NULLS LAST for ASC, NULLS FIRST for DESC.
pub(super) fn cmp_with_nulls(va: &Value, vb: &Value, asc: bool, nulls_first: bool) -> std::cmp::Ordering {
    let a_null = matches!(va, Value::Null);
    let b_null = matches!(vb, Value::Null);
    if a_null && b_null { return std::cmp::Ordering::Equal; }
    if a_null {
        return if nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
    }
    if b_null {
        return if nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
    }
    let ord = compare_values(va, vb).unwrap_or(std::cmp::Ordering::Equal);
    if asc { ord } else { ord.reverse() }
}

/// Check if an expression contains an aggregate function call.
pub(super) fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            if func.over.is_some() {
                return false; // Window functions are NOT aggregates
            }
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                | "STRING_AGG" | "ARRAY_AGG" | "JSON_AGG" | "BOOL_AND" | "BOOL_OR"
                | "EVERY" | "BIT_AND" | "BIT_OR")
        }
        Expr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::Nested(inner) => contains_aggregate(inner),
        Expr::Cast { expr: inner, .. } => contains_aggregate(inner),
        _ => false,
    }
}

pub(super) fn contains_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => func.over.is_some(),
        Expr::BinaryOp { left, right, .. } => {
            contains_window_function(left) || contains_window_function(right)
        }
        Expr::UnaryOp { expr, .. } => contains_window_function(expr),
        Expr::Nested(inner) => contains_window_function(inner),
        _ => false,
    }
}

/// Check if function args have the expected count.
pub(super) fn require_args(fname: &str, args: &[Value], expected: usize) -> Result<(), ExecError> {
    if args.len() < expected {
        Err(ExecError::Unsupported(format!(
            "{fname} requires {expected} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

/// Extract a non-negative u64 from a Value, returning an error if negative.
pub(super) fn val_to_u64(v: &Value, context: &str) -> Result<u64, ExecError> {
    match v {
        Value::Int32(n) if *n >= 0 => Ok(*n as u64),
        Value::Int64(n) if *n >= 0 => Ok(*n as u64),
        Value::Int32(n) => Err(ExecError::Unsupported(
            format!("{context}: value must be non-negative, got {n}"),
        )),
        Value::Int64(n) => Err(ExecError::Unsupported(
            format!("{context}: value must be non-negative, got {n}"),
        )),
        _ => Err(ExecError::Unsupported(format!("{context}: expected integer"))),
    }
}

/// Encode bytes as a lowercase hex string.
pub(super) fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a hex string into bytes. Returns Err on invalid hex.
pub(super) fn hex_decode(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("hex string must have even length".into());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| format!("invalid hex at position {i}"))
        })
        .collect()
}

/// Escape a string for safe embedding in a JSON string value.
/// Handles backslash, double-quote, and common control characters.
pub(super) fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"'  => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

pub(super) fn sanitize_sql_text_literal(value: &str) -> String {
    value
        .replace('\0', "")
        .replace('\\', "\\\\")
        .replace('\'', "''")
}

pub(super) fn sql_replacement_for_value(value: &Value) -> String {
    match value {
        Value::Text(s) => format!("'{}'", sanitize_sql_text_literal(s)),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        _ => format!("'{}'", sanitize_sql_text_literal(&value.to_string())),
    }
}

/// Substitute positional (`$1`) and named (`$name`) placeholders in SQL text.
/// Performs a single pass over the original SQL to avoid recursive substitution.
pub(super) fn substitute_sql_placeholders(
    sql: &str,
    positional: &[String],
    named: &HashMap<String, String>,
) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < bytes.len() {
        if in_line_comment {
            out.push(bytes[i] as char);
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                out.push('*');
                out.push('/');
                in_block_comment = false;
                i += 2;
            } else {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if in_single {
            out.push(bytes[i] as char);
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                } else {
                    in_single = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if in_double {
            out.push(bytes[i] as char);
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    out.push('"');
                    i += 2;
                } else {
                    in_double = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            out.push('-');
            out.push('-');
            in_line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            in_block_comment = true;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            out.push('\'');
            in_single = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            out.push('"');
            in_double = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'$' {
            let start = i;
            i += 1;
            if i < bytes.len() && bytes[i].is_ascii_digit() {
                let mut idx = 0usize;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    idx = idx * 10 + (bytes[i] - b'0') as usize;
                    i += 1;
                }
                if idx > 0 && idx <= positional.len() {
                    out.push_str(&positional[idx - 1]);
                } else {
                    out.push_str(&sql[start..i]);
                }
                continue;
            }
            if i < bytes.len()
                && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_')
            {
                let ident_start = i;
                i += 1;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                let ident = &sql[ident_start..i];
                if let Some(repl) = named.get(ident) {
                    out.push_str(repl);
                } else {
                    out.push_str(&sql[start..i]);
                }
                continue;
            }
            out.push('$');
            continue;
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out
}

/// Parse an aggregate specification like "SUM(amount)" → ("SUM", "amount").
pub(super) fn parse_agg_spec(spec: &str) -> (String, String) {
    if let Some(paren) = spec.find('(') {
        let func_name = spec[..paren].trim().to_uppercase();
        let col_name = spec[paren + 1..].trim_end_matches(')').trim().to_string();
        (func_name, col_name)
    } else {
        (spec.to_uppercase(), "*".to_string())
    }
}

/// Compute an aggregate function over rows.
pub(super) fn compute_aggregate(func: &str, col_idx: Option<usize>, rows: &[Row]) -> Value {
    match func {
        "COUNT" => Value::Int64(rows.len() as i64),
        "SUM" => {
            let col = col_idx.unwrap_or(0);
            let mut int_sum = 0i64;
            let mut float_sum = 0.0f64;
            let mut has_value = false;
            let mut has_float = false;
            for row in rows {
                if let Some(val) = row.get(col) {
                    match val {
                        Value::Int32(n) => { int_sum += *n as i64; float_sum += *n as f64; has_value = true; }
                        Value::Int64(n) => { int_sum += *n; float_sum += *n as f64; has_value = true; }
                        Value::Float64(f) => { float_sum += f; has_float = true; has_value = true; }
                        _ => {}
                    }
                }
            }
            // SQL standard: SUM of all-NULL input is NULL, not 0.
            // Preserve integer type when all inputs are integer.
            if !has_value { Value::Null } else if has_float { Value::Float64(float_sum) } else { Value::Int64(int_sum) }
        }
        "AVG" => {
            let col = col_idx.unwrap_or(0);
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for row in rows {
                if let Some(val) = row.get(col) {
                    match val {
                        Value::Int32(n) => { sum += *n as f64; count += 1; }
                        Value::Int64(n) => { sum += *n as f64; count += 1; }
                        Value::Float64(f) => { sum += f; count += 1; }
                        Value::Null => {}
                        _ => {}
                    }
                }
            }
            if count == 0 { Value::Null } else { Value::Float64(sum / count as f64) }
        }
        "MIN" => {
            let col = col_idx.unwrap_or(0);
            let mut min: Option<Value> = None;
            for row in rows {
                if let Some(val) = row.get(col) {
                    if *val == Value::Null { continue; }
                    min = Some(match min {
                        Some(ref m) if val < m => val.clone(),
                        Some(m) => m,
                        None => val.clone(),
                    });
                }
            }
            min.unwrap_or(Value::Null)
        }
        "MAX" => {
            let col = col_idx.unwrap_or(0);
            let mut max: Option<Value> = None;
            for row in rows {
                if let Some(val) = row.get(col) {
                    if *val == Value::Null { continue; }
                    max = Some(match max {
                        Some(ref m) if val > m => val.clone(),
                        Some(m) => m,
                        None => val.clone(),
                    });
                }
            }
            max.unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

/// Compute an aggregate function over borrowed row references.
/// Same logic as `compute_aggregate` but avoids requiring owned rows.
pub(super) fn compute_aggregate_refs(func: &str, col_idx: Option<usize>, rows: &[&Row]) -> Value {
    match func {
        "COUNT" => Value::Int64(rows.len() as i64),
        "SUM" => {
            let col = col_idx.unwrap_or(0);
            let mut int_sum = 0i64;
            let mut float_sum = 0.0f64;
            let mut has_value = false;
            let mut has_float = false;
            for row in rows {
                if let Some(val) = row.get(col) {
                    match val {
                        Value::Int32(n) => { int_sum += *n as i64; float_sum += *n as f64; has_value = true; }
                        Value::Int64(n) => { int_sum += *n; float_sum += *n as f64; has_value = true; }
                        Value::Float64(f) => { float_sum += f; has_float = true; has_value = true; }
                        _ => {}
                    }
                }
            }
            if !has_value { Value::Null } else if has_float { Value::Float64(float_sum) } else { Value::Int64(int_sum) }
        }
        "AVG" => {
            let col = col_idx.unwrap_or(0);
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for row in rows {
                if let Some(val) = row.get(col) {
                    match val {
                        Value::Int32(n) => { sum += *n as f64; count += 1; }
                        Value::Int64(n) => { sum += *n as f64; count += 1; }
                        Value::Float64(f) => { sum += f; count += 1; }
                        Value::Null => {}
                        _ => {}
                    }
                }
            }
            if count == 0 { Value::Null } else { Value::Float64(sum / count as f64) }
        }
        "MIN" => {
            let col = col_idx.unwrap_or(0);
            let mut min: Option<Value> = None;
            for row in rows {
                if let Some(val) = row.get(col) {
                    if *val == Value::Null { continue; }
                    min = Some(match min {
                        Some(ref m) if val < m => val.clone(),
                        Some(m) => m,
                        None => val.clone(),
                    });
                }
            }
            min.unwrap_or(Value::Null)
        }
        "MAX" => {
            let col = col_idx.unwrap_or(0);
            let mut max: Option<Value> = None;
            for row in rows {
                if let Some(val) = row.get(col) {
                    if *val == Value::Null { continue; }
                    max = Some(match max {
                        Some(ref m) if val > m => val.clone(),
                        Some(m) => m,
                        None => val.clone(),
                    });
                }
            }
            max.unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

/// SIMD fast-path for aggregate functions on numeric columns.
///
/// Handles SUM/MIN/MAX for Int32/Int64/Float64 columns using vectorized operations.
/// Returns `Some(value)` when a SIMD path applies, `None` to fall back to scalar.
pub(super) fn simd_aggregate(func: &str, col_idx: usize, col_meta: &[ColMeta], rows: &[Row]) -> Option<Value> {
    if rows.is_empty() {
        return None; // let scalar compute_aggregate handle the all-NULL / empty case
    }
    let dtype = col_meta.get(col_idx).map(|c| &c.dtype)?;
    match (func, dtype) {
        ("SUM", DataType::Int64 | DataType::Int32) => {
            let vals = crate::simd::extract_i64_column(rows, col_idx);
            if vals.is_empty() { return Some(Value::Null); }
            crate::simd::sum_i64_checked(&vals).map(Value::Int64)
        }
        ("SUM", DataType::Float64) => {
            let vals = crate::simd::extract_f64_column(rows, col_idx);
            if vals.is_empty() { return Some(Value::Null); }
            Some(Value::Float64(crate::simd::sum_f64(&vals)))
        }
        ("MIN", DataType::Float64) => {
            let vals = crate::simd::extract_f64_column(rows, col_idx);
            if vals.is_empty() { return Some(Value::Null); }
            crate::simd::min_f64(&vals).map(Value::Float64)
        }
        ("MAX", DataType::Float64) => {
            let vals = crate::simd::extract_f64_column(rows, col_idx);
            if vals.is_empty() { return Some(Value::Null); }
            crate::simd::max_f64(&vals).map(Value::Float64)
        }
        _ => None,
    }
}

/// Serialize a graph PropValue to a JSON string fragment.
pub(super) fn prop_value_to_json(v: &GraphPropValue) -> String {
    match v {
        GraphPropValue::Null => "null".into(),
        GraphPropValue::Bool(b) => b.to_string(),
        GraphPropValue::Int(n) => n.to_string(),
        GraphPropValue::Float(f) => {
            // NaN and Infinity are not valid JSON — serialize as null
            if f.is_finite() { format!("{f}") } else { "null".into() }
        }
        GraphPropValue::Text(s) => format!(r#""{}""#, json_escape(s)),
    }
}

/// Parse a JSON string into graph properties BTreeMap.
pub(super) fn parse_json_to_graph_props(text: &str) -> Result<std::collections::BTreeMap<String, GraphPropValue>, ExecError> {
    let serde_val: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| ExecError::Unsupported(format!("invalid JSON: {e}")))?;
    match serde_val {
        serde_json::Value::Object(map) => {
            let mut props = std::collections::BTreeMap::new();
            for (k, v) in map {
                let pv = match v {
                    serde_json::Value::Null => GraphPropValue::Null,
                    serde_json::Value::Bool(b) => GraphPropValue::Bool(b),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            GraphPropValue::Int(i)
                        } else {
                            GraphPropValue::Float(n.as_f64().unwrap_or(0.0))
                        }
                    }
                    serde_json::Value::String(s) => GraphPropValue::Text(s),
                    _ => GraphPropValue::Text(v.to_string()),
                };
                props.insert(k, pv);
            }
            Ok(props)
        }
        _ => Err(ExecError::Unsupported("graph properties must be a JSON object".into())),
    }
}

/// Parse a JSON text string into a document::JsonValue using serde_json.
pub(super) fn parse_json_to_doc(text: &str) -> Result<crate::document::JsonValue, String> {
    let serde_val: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    Ok(serde_to_doc(serde_val))
}

pub(super) fn serde_to_doc(v: serde_json::Value) -> crate::document::JsonValue {
    match v {
        serde_json::Value::Null => crate::document::JsonValue::Null,
        serde_json::Value::Bool(b) => crate::document::JsonValue::Bool(b),
        serde_json::Value::Number(n) => {
            // as_f64() can fail for u64 values > 2^53; use as_i64 fallback
            let f = n.as_f64()
                .or_else(|| n.as_i64().map(|i| i as f64))
                .or_else(|| n.as_u64().map(|u| u as f64))
                .unwrap_or(0.0);
            crate::document::JsonValue::Number(f)
        }
        serde_json::Value::String(s) => crate::document::JsonValue::Str(s),
        serde_json::Value::Array(arr) => {
            crate::document::JsonValue::Array(arr.into_iter().map(serde_to_doc).collect())
        }
        serde_json::Value::Object(map) => {
            let mut btree = std::collections::BTreeMap::new();
            for (k, v) in map {
                btree.insert(k, serde_to_doc(v));
            }
            crate::document::JsonValue::Object(btree)
        }
    }
}

/// Convert a Value to its CSV string representation.
pub(super) fn value_to_csv_string_impl(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bytea(b) => format!("\\x{}", b.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
        Value::Timestamp(ts) => ts.to_string(),
        Value::Date(d) => d.to_string(),
        Value::TimestampTz(ts) => ts.to_string(),
        Value::Numeric(n) => n.to_string(),
        Value::Uuid(u) => format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]),
        Value::Jsonb(j) => j.to_string(),
        Value::Array(arr) => format!("{{{}}}", arr.iter().map(value_to_csv_string_impl).collect::<Vec<_>>().join(",")),
        Value::Vector(vec) => format!("[{}]", vec.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(",")),
        Value::Interval { .. } => value.to_string(),
    }
}

/// Convert a Value to its text (tab-separated) string representation.
pub(super) fn value_to_text_string_impl(value: &Value) -> String {
    match value {
        Value::Null => "\\N".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bytea(b) => format!("\\x{}", b.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
        Value::Timestamp(ts) => ts.to_string(),
        Value::Date(d) => d.to_string(),
        Value::TimestampTz(ts) => ts.to_string(),
        Value::Numeric(n) => n.to_string(),
        Value::Uuid(u) => format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]),
        Value::Jsonb(j) => j.to_string(),
        Value::Array(arr) => format!("{{{}}}", arr.iter().map(value_to_text_string_impl).collect::<Vec<_>>().join(",")),
        Value::Vector(vec) => format!("[{}]", vec.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(",")),
        Value::Interval { .. } => value.to_string(),
    }
}

/// Strip dollar-quoting from a function body string (e.g., $$ SELECT 1 $$ → SELECT 1).
pub(super) fn strip_dollar_quotes(s: &str) -> String {
    let trimmed = s.trim();
    // Handle $tag$...$tag$ or $$...$$
    if let Some(stripped) = trimmed.strip_prefix('$')
        && let Some(end_tag_pos) = stripped.find('$') {
            let tag = &trimmed[..=end_tag_pos + 1];
            if trimmed.ends_with(tag) {
                let inner = &trimmed[tag.len()..trimmed.len() - tag.len()];
                return inner.trim().to_string();
            }
        }
    // Handle single-quoted strings
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return trimmed[1..trimmed.len() - 1].replace("''", "'");
    }
    trimmed.to_string()
}

/// Convert an internal Value to an AST literal expression for subquery substitution.
pub(super) fn value_to_ast_expr(val: &Value) -> Expr {
    let v = match val {
        Value::Null => ast::Value::Null,
        Value::Bool(b) => ast::Value::Boolean(*b),
        Value::Int32(n) => ast::Value::Number(n.to_string(), false),
        Value::Int64(n) => ast::Value::Number(n.to_string(), false),
        Value::Float64(f) => ast::Value::Number(f.to_string(), false),
        Value::Text(s) => ast::Value::SingleQuotedString(s.clone()),
        _ => ast::Value::Null,
    };
    Expr::Value(ast::ValueWithSpan {
        value: v,
        span: sqlparser::tokenizer::Span::empty(),
    })
}

/// Substitute outer column references in an expression tree with literal values.
/// Used for correlated subqueries where inner expressions reference outer table columns.
pub(super) fn substitute_outer_refs(expr: &Expr, outer_row: &Row, outer_meta: &[ColMeta]) -> Expr {
    match expr {
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            let table = &idents[0].value;
            let col = &idents[1].value;
            // Look for a match in outer columns
            for (i, meta) in outer_meta.iter().enumerate() {
                if let Some(ref t) = meta.table
                    && t.eq_ignore_ascii_case(table) && meta.name.eq_ignore_ascii_case(col)
                        && let Some(val) = outer_row.get(i) {
                            return value_to_ast_expr(val);
                        }
            }
            expr.clone()
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_outer_refs(left, outer_row, outer_meta)),
            op: op.clone(),
            right: Box::new(substitute_outer_refs(right, outer_row, outer_meta)),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(substitute_outer_refs(inner, outer_row, outer_meta)),
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(substitute_outer_refs(inner, outer_row, outer_meta))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(substitute_outer_refs(inner, outer_row, outer_meta))),
        Expr::Nested(inner) => Expr::Nested(Box::new(substitute_outer_refs(inner, outer_row, outer_meta))),
        _ => expr.clone(),
    }
}

/// Substitute outer column references in a query's WHERE/selection clauses.
pub(super) fn substitute_outer_refs_in_query(query: &ast::Query, outer_row: &Row, outer_meta: &[ColMeta]) -> ast::Query {
    let mut q = query.clone();
    if let ast::SetExpr::Select(ref mut sel) = *q.body
        && let Some(ref selection) = sel.selection {
            sel.selection = Some(substitute_outer_refs(selection, outer_row, outer_meta));
        }
    q
}

/// Compute the start and end row indices within a partition for a window frame.
///
/// If no frame is specified, the default frame is UNBOUNDED PRECEDING to CURRENT ROW
/// (the SQL standard default when ORDER BY is present).
/// Returns (start_idx, end_idx) inclusive, clamped to [0, partition_size - 1].
pub(super) fn compute_window_frame_bounds(
    frame: Option<&ast::WindowFrame>,
    current_row: usize,
    partition_size: usize,
) -> Result<(usize, usize), ExecError> {
    let frame = match frame {
        Some(f) => f,
        None => {
            // Default: UNBOUNDED PRECEDING to CURRENT ROW
            return Ok((0, current_row));
        }
    };

    let resolve_bound = |bound: &ast::WindowFrameBound, _is_start: bool| -> Result<usize, ExecError> {
        match bound {
            ast::WindowFrameBound::CurrentRow => Ok(current_row),
            ast::WindowFrameBound::Preceding(None) => {
                // UNBOUNDED PRECEDING
                Ok(0)
            }
            ast::WindowFrameBound::Preceding(Some(expr)) => {
                let n = expr_to_usize(expr)?;
                Ok(current_row.saturating_sub(n))
            }
            ast::WindowFrameBound::Following(None) => {
                // UNBOUNDED FOLLOWING
                Ok(partition_size.saturating_sub(1))
            }
            ast::WindowFrameBound::Following(Some(expr)) => {
                let n = expr_to_usize(expr)?;
                Ok(std::cmp::min(current_row + n, partition_size - 1))
            }
        }
    };

    let start = resolve_bound(&frame.start_bound, true)?;
    let end = match &frame.end_bound {
        Some(eb) => resolve_bound(eb, false)?,
        None => {
            // Shorthand form (e.g. ROWS 1 PRECEDING) means end = CURRENT ROW
            current_row
        }
    };

    // Clamp
    let start = std::cmp::min(start, partition_size.saturating_sub(1));
    let end = std::cmp::min(end, partition_size.saturating_sub(1));

    Ok((start, end))
}

/// Extract a usize from a SQL expression (expected to be a numeric literal).
pub(super) fn expr_to_usize(expr: &Expr) -> Result<usize, ExecError> {
    match expr {
        Expr::Value(val_with_span) => match &val_with_span.value {
            ast::Value::Number(s, _) => s
                .parse::<usize>()
                .map_err(|_| ExecError::Unsupported(format!("invalid frame offset: {s}"))),
            _ => Err(ExecError::Unsupported(format!(
                "non-numeric frame bound: {}", val_with_span.value
            ))),
        },
        _ => Err(ExecError::Unsupported(format!(
            "unsupported frame bound expression: {expr}"
        ))),
    }
}

/// Convert a Value to i64.
pub(super) fn value_to_i64(val: &Value) -> Result<i64, ExecError> {
    match val {
        Value::Int32(n) => Ok(*n as i64),
        Value::Int64(n) => Ok(*n),
        Value::Float64(n) => Ok(*n as i64),
        _ => Err(ExecError::Unsupported("expected numeric value".into())),
    }
}

/// Convert a Value to f64.
pub(super) fn value_to_f64(val: &Value) -> Result<f64, ExecError> {
    match val {
        Value::Int32(n) => Ok(*n as f64),
        Value::Int64(n) => Ok(*n as f64),
        Value::Float64(n) => Ok(*n),
        Value::Null => Ok(0.0),
        _ => Err(ExecError::Unsupported("expected numeric value".into())),
    }
}

/// Convert a Value to serde_json::Value.
pub(super) fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int32(n) => serde_json::json!(*n),
        Value::Int64(n) => serde_json::json!(*n),
        Value::Float64(n) => serde_json::json!(*n),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Jsonb(v) => v.clone(),
        Value::Date(d) => serde_json::json!(d),
        Value::Timestamp(us) => serde_json::json!(us),
        Value::TimestampTz(us) => serde_json::json!(us),
        Value::Numeric(s) => serde_json::Value::String(s.clone()),
        Value::Uuid(b) => serde_json::Value::String(Value::Uuid(*b).to_string()),
        Value::Bytea(b) => serde_json::Value::String(Value::Bytea(b.clone()).to_string()),
        Value::Array(vals) => {
            serde_json::Value::Array(vals.iter().map(value_to_json).collect())
        }
        Value::Vector(vec) => {
            serde_json::Value::Array(vec.iter().map(|f| serde_json::json!(f)).collect())
        }
        Value::Interval { months, days, microseconds } => {
            serde_json::json!({ "months": months, "days": days, "microseconds": microseconds })
        }
    }
}

/// Convert a Value (JSON array or text) to a Vector for vector operations.
pub(super) fn json_to_vector(val: &Value) -> Result<crate::vector::Vector, ExecError> {
    match val {
        Value::Jsonb(serde_json::Value::Array(arr)) => {
            let data: Vec<f32> = arr
                .iter()
                .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                .collect();
            Ok(crate::vector::Vector::new(data))
        }
        Value::Text(s) => {
            // Try parsing as JSON array: "[1.0, 2.0, 3.0]"
            if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(s) {
                let data: Vec<f32> = arr
                    .iter()
                    .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                    .collect();
                Ok(crate::vector::Vector::new(data))
            } else {
                Err(ExecError::Unsupported("cannot parse vector from text".into()))
            }
        }
        _ => Err(ExecError::Unsupported("vector must be JSON array or text".into())),
    }
}

/// Parse a WKT POINT string like "POINT(1.5 2.3)" into a geo::Point.
pub(super) fn parse_point_wkt(s: &str) -> Option<geo::Point> {
    let s = s.trim();
    let inner = if s.starts_with("POINT(") && s.ends_with(')') {
        &s[6..s.len() - 1]
    } else {
        // Try bare "x y" format
        s
    };
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() == 2 {
        let x = parts[0].parse::<f64>().ok()?;
        let y = parts[1].parse::<f64>().ok()?;
        Some(geo::Point::new(x, y))
    } else {
        None
    }
}

/// Parse a WKT POLYGON string like "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))".
pub(super) fn parse_polygon_wkt(s: &str) -> Option<geo::Polygon> {
    let s = s.trim();
    // Strip "POLYGON((" prefix and "))" suffix
    let inner = if s.starts_with("POLYGON((") && s.ends_with("))") {
        &s[9..s.len() - 2]
    } else {
        return None;
    };
    let points: Option<Vec<geo::Point>> = inner
        .split(',')
        .map(|coord_str| {
            let parts: Vec<&str> = coord_str.split_whitespace().collect();
            if parts.len() == 2 {
                let x = parts[0].parse::<f64>().ok()?;
                let y = parts[1].parse::<f64>().ok()?;
                Some(geo::Point::new(x, y))
            } else {
                None
            }
        })
        .collect();
    let points = points?;
    if points.len() < 3 {
        return None;
    }
    Some(geo::Polygon::new(points))
}

/// Parse a bucket size string like "1 minute", "hour", "1h", etc.
pub(super) fn parse_bucket_size(s: &str) -> Option<timeseries::BucketSize> {
    let s = s.trim().to_lowercase();
    // Handle formats: "1 minute", "minute", "1m", "1 hour", "hour", etc.
    let unit = s.trim_start_matches(|c: char| c.is_ascii_digit() || c == ' ');
    match unit {
        "second" | "seconds" | "s" | "sec" => Some(timeseries::BucketSize::Second),
        "minute" | "minutes" | "m" | "min" => Some(timeseries::BucketSize::Minute),
        "hour" | "hours" | "h" | "hr" => Some(timeseries::BucketSize::Hour),
        "day" | "days" | "d" => Some(timeseries::BucketSize::Day),
        "week" | "weeks" | "w" => Some(timeseries::BucketSize::Week),
        "month" | "months" | "mon" => Some(timeseries::BucketSize::Month),
        _ => None,
    }
}

/// Convert a Value (JSON object with indices/values) to a SparseVector.
pub(super) fn json_to_sparse_vec(val: &Value) -> Result<crate::sparse::SparseVector, ExecError> {
    match val {
        Value::Jsonb(serde_json::Value::Object(obj)) => {
            let mut entries = Vec::new();
            for (key, value) in obj {
                if let Ok(idx) = key.parse::<u32>() {
                    let v = value.as_f64().unwrap_or(0.0) as f32;
                    entries.push((idx, v));
                }
            }
            Ok(crate::sparse::SparseVector::new(entries))
        }
        Value::Text(s) => {
            if let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(s) {
                let mut entries = Vec::new();
                for (key, value) in &obj {
                    if let Ok(idx) = key.parse::<u32>() {
                        let v = value.as_f64().unwrap_or(0.0) as f32;
                        entries.push((idx, v));
                    }
                }
                Ok(crate::sparse::SparseVector::new(entries))
            } else {
                Err(ExecError::Unsupported("cannot parse sparse vector from text".into()))
            }
        }
        _ => Err(ExecError::Unsupported("sparse vector must be JSON object or text".into())),
    }
}

/// SQL LIKE pattern matching (supports % and _) using O(n*m) DP to prevent ReDoS.
pub(super) fn like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let m = p.len();

    // dp[j] = true means pattern[0..j] matches text[0..i] (updated per row)
    let mut dp = vec![false; m + 1];
    dp[0] = true;
    // Initialize: leading '%' chars match empty text
    for j in 0..m {
        if p[j] == '%' {
            dp[j + 1] = dp[j];
        } else {
            break;
        }
    }

    for &tc in &t {
        let mut prev = dp[0]; // dp_prev[0] (previous row, col 0)
        dp[0] = false; // text[0..i+1] never matches empty pattern
        for j in 0..m {
            let old = dp[j + 1]; // save dp_prev[j+1] before overwrite
            dp[j + 1] = match p[j] {
                '%' => {
                    // dp_prev[j+1] (skip char in text) || dp[j] (skip % in pattern)
                    old || dp[j]
                }
                '_' => prev, // dp_prev[j]: one char matched
                c => prev && tc == c,
            };
            prev = old;
        }
    }

    dp[m]
}

/// Format a unix timestamp as ISO-8601.
pub(super) fn format_timestamp(secs: u64) -> String {
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (y, m, d) = days_to_ymd(days);
    format!("{y:04}-{m:02}-{d:02} {hours:02}:{minutes:02}:{seconds:02}")
}

/// Convert days since epoch to (year, month, day).
pub(super) fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Simplified civil calendar calculation
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Extract a role name from a Grantee struct.
pub(super) fn grantee_name(grantee: &ast::Grantee) -> String {
    match &grantee.name {
        Some(ast::GranteeName::ObjectName(name)) => name.to_string(),
        _ => "public".to_string(),
    }
}

/// Parse SQL privilege specification into our Privilege enum.
pub(super) fn parse_privileges(privs: &ast::Privileges) -> Vec<Privilege> {
    match privs {
        ast::Privileges::All { .. } => vec![Privilege::All],
        ast::Privileges::Actions(actions) => {
            actions.iter().map(|a| {
                match a {
                    ast::Action::Select { .. } => Privilege::Select,
                    ast::Action::Insert { .. } => Privilege::Insert,
                    ast::Action::Update { .. } => Privilege::Update,
                    ast::Action::Delete => Privilege::Delete,
                    ast::Action::Create { .. } => Privilege::Create,
                    ast::Action::Usage => Privilege::Usage,
                    _ => Privilege::Select,
                }
            }).collect()
        }
    }
}

/// Parse grant objects into table name strings.
pub(super) fn parse_grant_objects(objects: &ast::GrantObjects) -> Vec<String> {
    match objects {
        ast::GrantObjects::Tables(tables) => tables.iter().map(|t| t.to_string()).collect(),
        ast::GrantObjects::AllTablesInSchema { schemas } => {
            schemas.iter().map(|s| format!("{s}.*")).collect()
        }
        ast::GrantObjects::Sequences(seqs) => seqs.iter().map(|s| s.to_string()).collect(),
        _ => vec!["*".to_string()],
    }
}

/// Parse a date string like "2024-03-15" into days since 2000-01-01.
pub(super) fn parse_date_string(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() >= 3 {
        let y = parts[0].parse::<i32>().ok()?;
        let m = parts[1].parse::<u32>().ok()?;
        let d = parts[2].split_whitespace().next()?.parse::<u32>().ok()?;
        Some(crate::types::ymd_to_days(y, m, d))
    } else {
        None
    }
}

/// Parse a date/timestamp string into (year, month, day, hour, minute, second).
/// Accepts formats: "YYYY-MM-DD" and "YYYY-MM-DD HH:MM:SS" (or with 'T' separator).
pub(super) fn parse_timestamp_parts(s: &str) -> Option<(i32, u32, u32, u32, u32, u32)> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() < 3 {
        return None;
    }
    let y = parts[0].parse::<i32>().ok()?;
    let m = parts[1].parse::<u32>().ok()?;
    // The day part might be followed by time: "15 14:30:00" or "15T14:30:00"
    let rest = parts[2];
    // Split on space or 'T'
    let (day_str, time_str) = if let Some(idx) = rest.find([' ', 'T']) {
        (&rest[..idx], Some(&rest[idx + 1..]))
    } else {
        (rest, None)
    };
    let d = day_str.parse::<u32>().ok()?;
    let (hour, minute, second) = if let Some(ts) = time_str {
        let time_parts: Vec<&str> = ts.split(':').collect();
        let h = time_parts.first().and_then(|p| p.parse::<u32>().ok()).unwrap_or(0);
        let min = time_parts.get(1).and_then(|p| p.parse::<u32>().ok()).unwrap_or(0);
        let sec = time_parts.get(2).and_then(|p| p.trim().parse::<u32>().ok()).unwrap_or(0);
        (h, min, sec)
    } else {
        (0, 0, 0)
    };
    Some((y, m, d, hour, minute, second))
}

/// Set a value at a path within a JSON value.
pub(super) fn jsonb_set_path(target: &mut serde_json::Value, path: &[String], new_val: serde_json::Value) {
    if path.is_empty() {
        return;
    }
    if path.len() == 1 {
        match target {
            serde_json::Value::Object(map) => {
                map.insert(path[0].clone(), new_val);
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = path[0].parse::<usize>()
                    && idx < arr.len() {
                        arr[idx] = new_val;
                    }
            }
            _ => {}
        }
    } else {
        let next = match target {
            serde_json::Value::Object(map) => map.get_mut(&path[0]),
            serde_json::Value::Array(arr) => {
                path[0].parse::<usize>().ok().and_then(|i| arr.get_mut(i))
            }
            _ => None,
        };
        if let Some(child) = next {
            jsonb_set_path(child, &path[1..], new_val);
        }
    }
}

/// Recursively strip null values from a JSON value.
pub(super) fn strip_json_nulls(val: &serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Object(map) => {
            let filtered: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), strip_json_nulls(v)))
                .collect();
            serde_json::Value::Object(filtered)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(strip_json_nulls).collect())
        }
        other => other.clone(),
    }
}

/// Recursive JSON containment check (`@>`).
///
/// Returns true when `left` contains all key-value pairs present in `right`.
/// - Object A contains Object B when every key in B exists in A and
///   A[key] contains B[key].
/// - Array A contains Array B when every element in B has a matching
///   element in A (order-independent).
/// - Scalars are compared for equality.
/// Convert a `Value` (Jsonb or Text containing JSON) to a `document::JsonValue`.
/// Returns `None` if the value is not valid JSON.
pub(super) fn value_to_doc_json(val: &Value) -> Option<crate::document::JsonValue> {
    match val {
        Value::Jsonb(v) => Some(serde_to_doc(v.clone())),
        Value::Text(s) => parse_json_to_doc(s).ok(),
        _ => None,
    }
}

pub(super) fn json_contains(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            b.iter().all(|(k, bv)| {
                a.get(k).is_some_and(|av| json_contains(av, bv))
            })
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            b.iter().all(|bv| a.iter().any(|av| json_contains(av, bv)))
        }
        (a, b) => a == b,
    }
}
