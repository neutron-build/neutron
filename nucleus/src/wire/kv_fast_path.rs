//! Byte-level fast path for common KV queries.
//!
//! Intercepts the 6 most common KV patterns before they hit the SQL parser,
//! eliminating ~900ns of parsing overhead per query. Non-matching queries
//! fall through with zero overhead (single `starts_with` check).

use std::sync::Arc;

use crate::executor::ExecResult;
use crate::kv::KvStore;
use crate::types::{DataType, Value};

// ============================================================================
// KvCommand
// ============================================================================

/// A parsed KV command ready for direct execution.
#[derive(Debug, Clone, PartialEq)]
pub enum KvCommand {
    /// `SELECT kv_get('key')`
    Get(String),
    /// `SELECT kv_set('key', 'value')` or `SELECT kv_set('key', 'value', ttl)`
    Set(String, String, Option<u64>),
    /// `SELECT kv_del('key')`
    Del(String),
    /// `SELECT kv_incr('key')`
    Incr(String),
    /// `SELECT kv_exists('key')`
    Exists(String),
}

// ============================================================================
// Parser
// ============================================================================

/// Try to parse a query as a fast-path KV command.
///
/// Returns `None` if the query doesn't match any KV pattern. The fast
/// rejection is a single case-insensitive prefix check on "SELECT kv_",
/// so non-KV queries incur zero overhead.
pub fn try_parse_kv(query: &str) -> Option<KvCommand> {
    let trimmed = query.trim();

    // Fast rejection: must start with "SELECT kv_" (case-insensitive).
    // We need at least 10 chars for "SELECT kv_".
    if trimmed.len() < 10 {
        return None;
    }

    // Find the boundary between SELECT and the rest, handling extra whitespace.
    let bytes = trimmed.as_bytes();
    if !bytes[..6].eq_ignore_ascii_case(b"SELECT") {
        return None;
    }

    // Skip whitespace after SELECT.
    let rest = skip_whitespace(&trimmed[6..]);
    if rest.is_empty() {
        return None;
    }

    // Must start with "kv_" (case-insensitive).
    if rest.len() < 3 || !rest.as_bytes()[..3].eq_ignore_ascii_case(b"kv_") {
        return None;
    }

    // Determine which function we're looking at.
    let after_kv = &rest[3..];

    if let Some(rest) = strip_prefix_ci(after_kv, "get(") {
        parse_one_arg(rest).map(KvCommand::Get)
    } else if let Some(rest) = strip_prefix_ci(after_kv, "set(") {
        parse_set_args(rest)
    } else if let Some(rest) = strip_prefix_ci(after_kv, "del(") {
        parse_one_arg(rest).map(KvCommand::Del)
    } else if let Some(rest) = strip_prefix_ci(after_kv, "incr(") {
        parse_one_arg(rest).map(KvCommand::Incr)
    } else if let Some(rest) = strip_prefix_ci(after_kv, "exists(") {
        parse_one_arg(rest).map(KvCommand::Exists)
    } else {
        None
    }
}

/// Execute a parsed KV command against the store, returning an `ExecResult`.
pub fn execute_kv_command(cmd: &KvCommand, kv: &Arc<KvStore>) -> ExecResult {
    match cmd {
        KvCommand::Get(key) => {
            let value = kv.get(key).unwrap_or(Value::Null);
            ExecResult::Select {
                columns: vec![("kv_get".to_string(), data_type_for_value(&value))],
                rows: vec![vec![value]],
            }
        }
        KvCommand::Set(key, value, ttl) => {
            let val = Value::Text(value.clone());
            kv.set(key, val.clone(), *ttl);
            ExecResult::Select {
                columns: vec![("kv_set".to_string(), DataType::Text)],
                rows: vec![vec![val]],
            }
        }
        KvCommand::Del(key) => {
            let deleted = kv.del(key);
            ExecResult::Select {
                columns: vec![("kv_del".to_string(), DataType::Bool)],
                rows: vec![vec![Value::Bool(deleted)]],
            }
        }
        KvCommand::Incr(key) => {
            match kv.incr(key) {
                Ok(new_val) => ExecResult::Select {
                    columns: vec![("kv_incr".to_string(), DataType::Int64)],
                    rows: vec![vec![Value::Int64(new_val)]],
                },
                Err(_) => {
                    // Fall through: let the normal path handle the error.
                    // This shouldn't normally happen from the fast path since
                    // we only get here with well-formed queries.
                    ExecResult::Select {
                        columns: vec![("kv_incr".to_string(), DataType::Text)],
                        rows: vec![vec![Value::Text(
                            "ERR value is not an integer".to_string(),
                        )]],
                    }
                }
            }
        }
        KvCommand::Exists(key) => {
            let exists = kv.exists(key);
            ExecResult::Select {
                columns: vec![("kv_exists".to_string(), DataType::Bool)],
                rows: vec![vec![Value::Bool(exists)]],
            }
        }
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Skip leading ASCII whitespace, returning the remaining slice.
fn skip_whitespace(s: &str) -> &str {
    s.trim_start()
}

/// Case-insensitive prefix strip. Returns the remainder after the prefix,
/// or `None` if the prefix doesn't match.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() < prefix.len() {
        return None;
    }
    if s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes()) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Parse a single-quoted string argument followed by `)` and optional `;`.
///
/// Input should start right after the opening `(`, e.g. `'key')` or `'key') ;`.
fn parse_one_arg(s: &str) -> Option<String> {
    let s = skip_whitespace(s);
    let (arg, rest) = parse_quoted_string(s)?;
    let rest = skip_whitespace(rest);
    // Expect closing paren
    let rest = rest.strip_prefix(')')?;
    // Must be end of query (optional whitespace + optional semicolon)
    if !is_end_of_query(rest) {
        return None;
    }
    Some(arg)
}

/// Parse kv_set arguments: `'key', 'value')` or `'key', 'value', ttl)`.
fn parse_set_args(s: &str) -> Option<KvCommand> {
    let s = skip_whitespace(s);

    // Parse key
    let (key, rest) = parse_quoted_string(s)?;
    let rest = skip_whitespace(rest);

    // Expect comma
    let rest = rest.strip_prefix(',')?;
    let rest = skip_whitespace(rest);

    // Parse value
    let (value, rest) = parse_quoted_string(rest)?;
    let rest = skip_whitespace(rest);

    // Check for optional TTL (comma + integer)
    if let Some(rest_after_comma) = rest.strip_prefix(',') {
        let rest_after_comma = skip_whitespace(rest_after_comma);
        let (ttl, rest) = parse_integer(rest_after_comma)?;
        let rest = skip_whitespace(rest);
        let rest = rest.strip_prefix(')')?;
        if !is_end_of_query(rest) {
            return None;
        }
        Some(KvCommand::Set(key, value, Some(ttl)))
    } else {
        // No TTL
        let rest = rest.strip_prefix(')')?;
        if !is_end_of_query(rest) {
            return None;
        }
        Some(KvCommand::Set(key, value, None))
    }
}

/// Parse a single-quoted SQL string, handling escaped quotes (`''` -> `'`).
///
/// Returns the unescaped string and the remaining input after the closing quote.
fn parse_quoted_string(s: &str) -> Option<(String, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'\'' {
        return None;
    }

    let mut result = String::new();
    let mut i = 1; // skip opening quote

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Check for escaped quote ('')
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                result.push('\'');
                i += 2;
            } else {
                // End of string
                return Some((result, &s[i + 1..]));
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }

    // Unterminated string
    None
}

/// Parse a non-negative integer from the start of the string.
/// Returns the parsed value and the remaining input.
fn parse_integer(s: &str) -> Option<(u64, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_digit() {
        return None;
    }

    let mut val: u64 = 0;
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        val = val.checked_mul(10)?.checked_add((bytes[i] - b'0') as u64)?;
        i += 1;
    }

    Some((val, &s[i..]))
}

/// Check that the remaining input is just optional whitespace and/or a semicolon.
fn is_end_of_query(s: &str) -> bool {
    let s = s.trim();
    s.is_empty() || s == ";"
}

/// Infer the DataType for a Value (for the result column descriptor).
fn data_type_for_value(v: &Value) -> DataType {
    match v {
        Value::Null => DataType::Text,
        Value::Bool(_) => DataType::Bool,
        Value::Int32(_) => DataType::Int32,
        Value::Int64(_) => DataType::Int64,
        Value::Float64(_) => DataType::Float64,
        Value::Text(_) => DataType::Text,
        _ => DataType::Text,
    }
}

// ============================================================================
// SQL OLTP Fast Path
// ============================================================================
//
// Intercepts the 4 most common OLTP SQL patterns before they hit the SQL parser,
// eliminating ~900ns of parsing overhead per query. Non-matching queries fall
// through with minimal overhead (a few byte comparisons).
//
// Patterns matched:
//   SELECT * FROM table WHERE pk_col = value
//   INSERT INTO table VALUES (v1, v2, ...)
//   UPDATE table SET col = val, ... WHERE pk_col = value
//   DELETE FROM table WHERE pk_col = value

/// A parsed SQL OLTP command ready for direct execution.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlFastPathCommand {
    /// `SELECT * FROM table WHERE pk = value`
    PointSelect {
        table: String,
        where_col: String,
        where_val: SqlLiteral,
    },
    /// `INSERT INTO table VALUES (v1, v2, ...)`
    SimpleInsert {
        table: String,
        values: Vec<SqlLiteral>,
    },
    /// `UPDATE table SET col1 = val1, ... WHERE pk = value`
    PointUpdate {
        table: String,
        assignments: Vec<(String, SqlLiteral)>,
        where_col: String,
        where_val: SqlLiteral,
    },
    /// `DELETE FROM table WHERE pk = value`
    PointDelete {
        table: String,
        where_col: String,
        where_val: SqlLiteral,
    },
}

/// A literal value parsed from a SQL fast-path query.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlLiteral {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl SqlLiteral {
    /// Convert to a Nucleus `Value`.
    pub fn to_value(&self) -> Value {
        match self {
            SqlLiteral::Null => Value::Null,
            SqlLiteral::Integer(n) => {
                if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    Value::Int32(*n as i32)
                } else {
                    Value::Int64(*n)
                }
            }
            SqlLiteral::Float(f) => Value::Float64(*f),
            SqlLiteral::Text(s) => Value::Text(s.clone()),
            SqlLiteral::Bool(b) => Value::Bool(*b),
        }
    }
}

/// Try to parse a query as a fast-path SQL OLTP command.
///
/// Returns `None` if the query doesn't match any supported pattern.
/// Fast rejection uses a single first-byte check, so non-matching
/// queries incur near-zero overhead.
pub fn try_parse_sql_fast_path(query: &str) -> Option<SqlFastPathCommand> {
    let trimmed = query.trim();
    if trimmed.len() < 10 {
        return None;
    }

    let first = trimmed.as_bytes()[0].to_ascii_uppercase();
    match first {
        b'S' => try_parse_point_select(trimmed),
        b'I' => try_parse_simple_insert(trimmed),
        b'U' => try_parse_point_update(trimmed),
        b'D' => try_parse_point_delete(trimmed),
        _ => None,
    }
}

/// Parse: `SELECT * FROM table WHERE col = value`
fn try_parse_point_select(s: &str) -> Option<SqlFastPathCommand> {
    // SELECT
    let rest = strip_prefix_ci(s, "SELECT")?;
    let rest = skip_whitespace(rest);

    // Must be `*`
    let rest = rest.strip_prefix('*')?;
    let rest = skip_whitespace(rest);

    // FROM
    let rest = strip_prefix_ci(rest, "FROM")?;
    let rest = skip_whitespace(rest);

    // table name
    let (table, rest) = parse_identifier(rest)?;
    let rest = skip_whitespace(rest);

    // WHERE
    let rest = strip_prefix_ci(rest, "WHERE")?;
    let rest = skip_whitespace(rest);

    // col = value
    let (col, rest) = parse_identifier(rest)?;
    let rest = skip_whitespace(rest);
    let rest = rest.strip_prefix('=')?;
    let rest = skip_whitespace(rest);
    let (val, rest) = parse_sql_literal(rest)?;

    if !is_end_of_query(rest) {
        return None;
    }

    Some(SqlFastPathCommand::PointSelect {
        table,
        where_col: col,
        where_val: val,
    })
}

/// Parse: `INSERT INTO table VALUES (v1, v2, ...)`
fn try_parse_simple_insert(s: &str) -> Option<SqlFastPathCommand> {
    // INSERT
    let rest = strip_prefix_ci(s, "INSERT")?;
    let rest = skip_whitespace(rest);

    // INTO
    let rest = strip_prefix_ci(rest, "INTO")?;
    let rest = skip_whitespace(rest);

    // table name
    let (table, rest) = parse_identifier(rest)?;
    let rest = skip_whitespace(rest);

    // VALUES
    let rest = strip_prefix_ci(rest, "VALUES")?;
    let rest = skip_whitespace(rest);

    // Opening paren
    let rest = rest.strip_prefix('(')?;

    // Parse comma-separated values
    let (values, rest) = parse_value_list(rest)?;

    // Closing paren
    let rest = skip_whitespace(rest);
    let rest = rest.strip_prefix(')')?;

    if !is_end_of_query(rest) {
        return None;
    }

    Some(SqlFastPathCommand::SimpleInsert { table, values })
}

/// Parse: `UPDATE table SET col1 = val1, col2 = val2 WHERE pk = value`
fn try_parse_point_update(s: &str) -> Option<SqlFastPathCommand> {
    // UPDATE
    let rest = strip_prefix_ci(s, "UPDATE")?;
    let rest = skip_whitespace(rest);

    // table name
    let (table, rest) = parse_identifier(rest)?;
    let rest = skip_whitespace(rest);

    // SET
    let rest = strip_prefix_ci(rest, "SET")?;
    let rest = skip_whitespace(rest);

    // Parse assignments: col = val [, col = val]*
    let (assignments, rest) = parse_assignments(rest)?;

    // WHERE
    let rest = skip_whitespace(rest);
    let rest = strip_prefix_ci(rest, "WHERE")?;
    let rest = skip_whitespace(rest);

    // col = value
    let (where_col, rest) = parse_identifier(rest)?;
    let rest = skip_whitespace(rest);
    let rest = rest.strip_prefix('=')?;
    let rest = skip_whitespace(rest);
    let (where_val, rest) = parse_sql_literal(rest)?;

    if !is_end_of_query(rest) {
        return None;
    }

    Some(SqlFastPathCommand::PointUpdate {
        table,
        assignments,
        where_col,
        where_val,
    })
}

/// Parse: `DELETE FROM table WHERE col = value`
fn try_parse_point_delete(s: &str) -> Option<SqlFastPathCommand> {
    // DELETE
    let rest = strip_prefix_ci(s, "DELETE")?;
    let rest = skip_whitespace(rest);

    // FROM
    let rest = strip_prefix_ci(rest, "FROM")?;
    let rest = skip_whitespace(rest);

    // table name
    let (table, rest) = parse_identifier(rest)?;
    let rest = skip_whitespace(rest);

    // WHERE
    let rest = strip_prefix_ci(rest, "WHERE")?;
    let rest = skip_whitespace(rest);

    // col = value
    let (col, rest) = parse_identifier(rest)?;
    let rest = skip_whitespace(rest);
    let rest = rest.strip_prefix('=')?;
    let rest = skip_whitespace(rest);
    let (val, rest) = parse_sql_literal(rest)?;

    if !is_end_of_query(rest) {
        return None;
    }

    Some(SqlFastPathCommand::PointDelete {
        table,
        where_col: col,
        where_val: val,
    })
}

/// Parse an unquoted or double-quoted SQL identifier, returned lowercased.
fn parse_identifier(s: &str) -> Option<(String, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    // Double-quoted identifier
    if bytes[0] == b'"' {
        let mut name = String::new();
        let mut i = 1;
        while i < bytes.len() {
            if bytes[i] == b'"' {
                // Escaped double-quote ""
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    name.push('"');
                    i += 2;
                } else {
                    return Some((name, &s[i + 1..]));
                }
            } else {
                name.push(bytes[i] as char);
                i += 1;
            }
        }
        return None; // unterminated quote
    }

    // Unquoted identifier: [a-zA-Z_][a-zA-Z0-9_]*
    if !bytes[0].is_ascii_alphabetic() && bytes[0] != b'_' {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    // SQL identifiers are case-insensitive; normalize to lowercase.
    let name = s[..i].to_ascii_lowercase();
    Some((name, &s[i..]))
}

/// Parse a SQL literal: integer, float, string, NULL, TRUE, FALSE.
fn parse_sql_literal(s: &str) -> Option<(SqlLiteral, &str)> {
    let s = skip_whitespace(s);
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    // NULL
    if let Some(rest) = strip_prefix_ci(s, "NULL") {
        // Make sure NULL is not a prefix of an identifier
        if rest.is_empty() || !rest.as_bytes()[0].is_ascii_alphanumeric() {
            return Some((SqlLiteral::Null, rest));
        }
    }

    // TRUE / FALSE
    if let Some(rest) = strip_prefix_ci(s, "TRUE")
        && (rest.is_empty() || !rest.as_bytes()[0].is_ascii_alphanumeric()) {
            return Some((SqlLiteral::Bool(true), rest));
        }
    if let Some(rest) = strip_prefix_ci(s, "FALSE")
        && (rest.is_empty() || !rest.as_bytes()[0].is_ascii_alphanumeric()) {
            return Some((SqlLiteral::Bool(false), rest));
        }

    // String literal
    if bytes[0] == b'\'' {
        let (text, rest) = parse_quoted_string(s)?;
        return Some((SqlLiteral::Text(text), rest));
    }

    // Numeric: [+-]?[0-9]+ or [+-]?[0-9]+.[0-9]+
    if bytes[0].is_ascii_digit() || ((bytes[0] == b'-' || bytes[0] == b'+') && bytes.len() > 1 && bytes[1].is_ascii_digit()) {
        let mut i = 0;
        if bytes[0] == b'-' || bytes[0] == b'+' {
            i = 1;
        }
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        // Check for decimal point
        if i < bytes.len() && bytes[i] == b'.' {
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            let f: f64 = s[..i].parse().ok()?;
            return Some((SqlLiteral::Float(f), &s[i..]));
        }
        let n: i64 = s[..i].parse().ok()?;
        return Some((SqlLiteral::Integer(n), &s[i..]));
    }

    None
}

/// Parse a comma-separated list of SQL literals (inside parentheses).
fn parse_value_list(s: &str) -> Option<(Vec<SqlLiteral>, &str)> {
    let mut values = Vec::new();
    let mut rest = skip_whitespace(s);

    // First value
    let (val, r) = parse_sql_literal(rest)?;
    values.push(val);
    rest = skip_whitespace(r);

    // Subsequent values
    while let Some(r) = rest.strip_prefix(',') {
        rest = skip_whitespace(r);
        let (val, r) = parse_sql_literal(rest)?;
        values.push(val);
        rest = skip_whitespace(r);
    }

    Some((values, rest))
}

/// Parse SET assignments: `col = val [, col = val]*`
fn parse_assignments(s: &str) -> Option<(Vec<(String, SqlLiteral)>, &str)> {
    let mut assignments = Vec::new();
    let mut rest = s;

    loop {
        let (col, r) = parse_identifier(rest)?;
        let r = skip_whitespace(r);
        let r = r.strip_prefix('=')?;
        let r = skip_whitespace(r);
        let (val, r) = parse_sql_literal(r)?;
        assignments.push((col, val));
        let r = skip_whitespace(r);

        // Check for comma (more assignments) vs WHERE (end of SET clause)
        if let Some(after_comma) = r.strip_prefix(',') {
            rest = skip_whitespace(after_comma);
        } else {
            return Some((assignments, r));
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── Basic parsing tests ─────────────────────────────────────────────

    #[test]
    fn parse_kv_get() {
        let cmd = try_parse_kv("SELECT kv_get('mykey')").unwrap();
        assert_eq!(cmd, KvCommand::Get("mykey".to_string()));
    }

    #[test]
    fn parse_kv_set_no_ttl() {
        let cmd = try_parse_kv("SELECT kv_set('mykey', 'myvalue')").unwrap();
        assert_eq!(
            cmd,
            KvCommand::Set("mykey".to_string(), "myvalue".to_string(), None)
        );
    }

    #[test]
    fn parse_kv_set_with_ttl() {
        let cmd = try_parse_kv("SELECT kv_set('mykey', 'myvalue', 3600)").unwrap();
        assert_eq!(
            cmd,
            KvCommand::Set("mykey".to_string(), "myvalue".to_string(), Some(3600))
        );
    }

    #[test]
    fn parse_kv_del() {
        let cmd = try_parse_kv("SELECT kv_del('mykey')").unwrap();
        assert_eq!(cmd, KvCommand::Del("mykey".to_string()));
    }

    #[test]
    fn parse_kv_incr() {
        let cmd = try_parse_kv("SELECT kv_incr('counter')").unwrap();
        assert_eq!(cmd, KvCommand::Incr("counter".to_string()));
    }

    #[test]
    fn parse_kv_exists() {
        let cmd = try_parse_kv("SELECT kv_exists('mykey')").unwrap();
        assert_eq!(cmd, KvCommand::Exists("mykey".to_string()));
    }

    // ── Case insensitivity ──────────────────────────────────────────────

    #[test]
    fn case_insensitive_select() {
        assert_eq!(
            try_parse_kv("select kv_get('k')").unwrap(),
            KvCommand::Get("k".to_string())
        );
        assert_eq!(
            try_parse_kv("Select kv_get('k')").unwrap(),
            KvCommand::Get("k".to_string())
        );
        assert_eq!(
            try_parse_kv("SELECT KV_GET('k')").unwrap(),
            KvCommand::Get("k".to_string())
        );
        assert_eq!(
            try_parse_kv("SELECT KV_SET('k', 'v')").unwrap(),
            KvCommand::Set("k".to_string(), "v".to_string(), None)
        );
    }

    // ── Whitespace handling ─────────────────────────────────────────────

    #[test]
    fn whitespace_variations() {
        // Leading/trailing spaces
        assert_eq!(
            try_parse_kv("  SELECT kv_get('k')  ").unwrap(),
            KvCommand::Get("k".to_string())
        );
        // Multiple spaces between SELECT and function
        assert_eq!(
            try_parse_kv("SELECT   kv_get('k')").unwrap(),
            KvCommand::Get("k".to_string())
        );
        // Spaces inside argument list
        assert_eq!(
            try_parse_kv("SELECT kv_set( 'k' , 'v' , 60 )").unwrap(),
            KvCommand::Set("k".to_string(), "v".to_string(), Some(60))
        );
    }

    // ── Quoted strings with escaped quotes ──────────────────────────────

    #[test]
    fn escaped_single_quotes() {
        // SQL escapes single quotes by doubling them: '' -> '
        let cmd = try_parse_kv("SELECT kv_get('it''s')").unwrap();
        assert_eq!(cmd, KvCommand::Get("it's".to_string()));

        let cmd = try_parse_kv("SELECT kv_set('key''s', 'val''ue')").unwrap();
        assert_eq!(
            cmd,
            KvCommand::Set("key's".to_string(), "val'ue".to_string(), None)
        );
    }

    // ── Semicolons ──────────────────────────────────────────────────────

    #[test]
    fn with_trailing_semicolon() {
        assert_eq!(
            try_parse_kv("SELECT kv_get('k');").unwrap(),
            KvCommand::Get("k".to_string())
        );
        assert_eq!(
            try_parse_kv("SELECT kv_set('k', 'v', 120);").unwrap(),
            KvCommand::Set("k".to_string(), "v".to_string(), Some(120))
        );
    }

    #[test]
    fn without_trailing_semicolon() {
        assert!(try_parse_kv("SELECT kv_get('k')").is_some());
    }

    // ── Non-matching queries fall through ───────────────────────────────

    #[test]
    fn non_matching_queries_return_none() {
        assert!(try_parse_kv("INSERT INTO t VALUES (1)").is_none());
        assert!(try_parse_kv("SELECT * FROM users").is_none());
        assert!(try_parse_kv("SELECT 1").is_none());
        assert!(try_parse_kv("UPDATE t SET x = 1").is_none());
        assert!(try_parse_kv("DELETE FROM t").is_none());
        assert!(try_parse_kv("SELECT kv_unknown('k')").is_none());
        assert!(try_parse_kv("").is_none());
        assert!(try_parse_kv("SEL").is_none());
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[test]
    fn empty_key() {
        let cmd = try_parse_kv("SELECT kv_get('')").unwrap();
        assert_eq!(cmd, KvCommand::Get("".to_string()));
    }

    #[test]
    fn empty_value() {
        let cmd = try_parse_kv("SELECT kv_set('k', '')").unwrap();
        assert_eq!(
            cmd,
            KvCommand::Set("k".to_string(), "".to_string(), None)
        );
    }

    #[test]
    fn long_key() {
        let long_key = "a".repeat(1000);
        let query = format!("SELECT kv_get('{long_key}')");
        let cmd = try_parse_kv(&query).unwrap();
        assert_eq!(cmd, KvCommand::Get(long_key));
    }

    #[test]
    fn ttl_zero() {
        let cmd = try_parse_kv("SELECT kv_set('k', 'v', 0)").unwrap();
        assert_eq!(
            cmd,
            KvCommand::Set("k".to_string(), "v".to_string(), Some(0))
        );
    }

    // ── Execution tests ─────────────────────────────────────────────────

    #[test]
    fn execute_get_missing_key() {
        let store = Arc::new(KvStore::new());
        let result = execute_kv_command(&KvCommand::Get("missing".to_string()), &store);
        match result {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Null);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn execute_set_then_get() {
        let store = Arc::new(KvStore::new());
        execute_kv_command(
            &KvCommand::Set("k".to_string(), "v".to_string(), None),
            &store,
        );
        let result = execute_kv_command(&KvCommand::Get("k".to_string()), &store);
        match result {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("v".to_string()));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn execute_del() {
        let store = Arc::new(KvStore::new());
        store.set("k", Value::Text("v".into()), None);
        let result = execute_kv_command(&KvCommand::Del("k".to_string()), &store);
        match result {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Bool(true));
            }
            _ => panic!("expected Select"),
        }
        // Verify key is gone
        assert!(store.get("k").is_none());
    }

    #[test]
    fn execute_incr() {
        let store = Arc::new(KvStore::new());
        let result = execute_kv_command(&KvCommand::Incr("counter".to_string()), &store);
        match result {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int64(1));
            }
            _ => panic!("expected Select"),
        }
        let result = execute_kv_command(&KvCommand::Incr("counter".to_string()), &store);
        match result {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int64(2));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn execute_exists() {
        let store = Arc::new(KvStore::new());
        let result = execute_kv_command(&KvCommand::Exists("k".to_string()), &store);
        match result {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Bool(false));
            }
            _ => panic!("expected Select"),
        }
        store.set("k", Value::Text("v".into()), None);
        let result = execute_kv_command(&KvCommand::Exists("k".to_string()), &store);
        match result {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Bool(true));
            }
            _ => panic!("expected Select"),
        }
    }

    // ====================================================================
    // SQL fast-path parsing tests
    // ====================================================================

    #[test]
    fn sql_fp_point_select() {
        let cmd = try_parse_sql_fast_path("SELECT * FROM users WHERE id = 42").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::PointSelect {
                table: "users".into(),
                where_col: "id".into(),
                where_val: SqlLiteral::Integer(42),
            }
        );
    }

    #[test]
    fn sql_fp_point_select_string_pk() {
        let cmd = try_parse_sql_fast_path("SELECT * FROM users WHERE email = 'alice@example.com'").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::PointSelect {
                table: "users".into(),
                where_col: "email".into(),
                where_val: SqlLiteral::Text("alice@example.com".into()),
            }
        );
    }

    #[test]
    fn sql_fp_point_select_case_insensitive() {
        let cmd = try_parse_sql_fast_path("select * from Users where ID = 1").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::PointSelect {
                table: "users".into(),
                where_col: "id".into(),
                where_val: SqlLiteral::Integer(1),
            }
        );
    }

    #[test]
    fn sql_fp_point_select_semicolon() {
        assert!(try_parse_sql_fast_path("SELECT * FROM t WHERE id = 1;").is_some());
    }

    #[test]
    fn sql_fp_simple_insert() {
        let cmd = try_parse_sql_fast_path("INSERT INTO users VALUES (1, 'alice', TRUE)").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::SimpleInsert {
                table: "users".into(),
                values: vec![
                    SqlLiteral::Integer(1),
                    SqlLiteral::Text("alice".into()),
                    SqlLiteral::Bool(true),
                ],
            }
        );
    }

    #[test]
    fn sql_fp_insert_null() {
        let cmd = try_parse_sql_fast_path("INSERT INTO t VALUES (NULL, 1)").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::SimpleInsert {
                table: "t".into(),
                values: vec![SqlLiteral::Null, SqlLiteral::Integer(1)],
            }
        );
    }

    #[test]
    fn sql_fp_insert_float() {
        let cmd = try_parse_sql_fast_path("INSERT INTO t VALUES (3.14)").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::SimpleInsert {
                table: "t".into(),
                values: vec![SqlLiteral::Float(3.14)],
            }
        );
    }

    #[test]
    fn sql_fp_point_update() {
        let cmd = try_parse_sql_fast_path("UPDATE users SET name = 'bob', age = 30 WHERE id = 1").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::PointUpdate {
                table: "users".into(),
                assignments: vec![
                    ("name".into(), SqlLiteral::Text("bob".into())),
                    ("age".into(), SqlLiteral::Integer(30)),
                ],
                where_col: "id".into(),
                where_val: SqlLiteral::Integer(1),
            }
        );
    }

    #[test]
    fn sql_fp_point_delete() {
        let cmd = try_parse_sql_fast_path("DELETE FROM users WHERE id = 42").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::PointDelete {
                table: "users".into(),
                where_col: "id".into(),
                where_val: SqlLiteral::Integer(42),
            }
        );
    }

    #[test]
    fn sql_fp_non_matching_falls_through() {
        // Multi-column WHERE
        assert!(try_parse_sql_fast_path("SELECT * FROM t WHERE a = 1 AND b = 2").is_none());
        // SELECT with specific columns
        assert!(try_parse_sql_fast_path("SELECT id FROM t WHERE id = 1").is_none());
        // INSERT with column list
        assert!(try_parse_sql_fast_path("INSERT INTO t (a, b) VALUES (1, 2)").is_none());
        // UPDATE without WHERE
        assert!(try_parse_sql_fast_path("UPDATE t SET x = 1").is_none());
        // DELETE without WHERE
        assert!(try_parse_sql_fast_path("DELETE FROM t").is_none());
        // JOIN
        assert!(try_parse_sql_fast_path("SELECT * FROM a JOIN b ON a.id = b.id").is_none());
        // Subquery
        assert!(try_parse_sql_fast_path("SELECT * FROM (SELECT 1)").is_none());
        // Empty
        assert!(try_parse_sql_fast_path("").is_none());
    }

    #[test]
    fn sql_fp_negative_integer() {
        let cmd = try_parse_sql_fast_path("SELECT * FROM t WHERE id = -1").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::PointSelect {
                table: "t".into(),
                where_col: "id".into(),
                where_val: SqlLiteral::Integer(-1),
            }
        );
    }

    #[test]
    fn sql_fp_whitespace_variations() {
        // Extra whitespace everywhere
        let cmd = try_parse_sql_fast_path("  SELECT  *  FROM  t  WHERE  id  =  1  ;  ").unwrap();
        assert_eq!(
            cmd,
            SqlFastPathCommand::PointSelect {
                table: "t".into(),
                where_col: "id".into(),
                where_val: SqlLiteral::Integer(1),
            }
        );
    }

    #[test]
    fn sql_fp_literal_to_value() {
        assert_eq!(SqlLiteral::Null.to_value(), Value::Null);
        assert_eq!(SqlLiteral::Integer(42).to_value(), Value::Int32(42));
        assert_eq!(SqlLiteral::Integer(i64::MAX).to_value(), Value::Int64(i64::MAX));
        assert_eq!(SqlLiteral::Float(3.14).to_value(), Value::Float64(3.14));
        assert_eq!(SqlLiteral::Text("hi".into()).to_value(), Value::Text("hi".into()));
        assert_eq!(SqlLiteral::Bool(true).to_value(), Value::Bool(true));
    }
}
