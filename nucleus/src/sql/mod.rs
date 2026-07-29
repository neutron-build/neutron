//! SQL parsing layer — wraps sqlparser-rs and converts AST to Nucleus types.

use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::catalog::{ColumnDef, FkAction};
use crate::types::DataType;

/// Convert a sqlparser `ReferentialAction` to our internal `FkAction`.
///
/// Also available as `convert_fk_action` for use by ALTER TABLE ADD CONSTRAINT.
pub fn convert_fk_action(action: &Option<ast::ReferentialAction>) -> FkAction {
    match action {
        None => FkAction::NoAction,
        Some(ast::ReferentialAction::NoAction) => FkAction::NoAction,
        Some(ast::ReferentialAction::Restrict) => FkAction::Restrict,
        Some(ast::ReferentialAction::Cascade) => FkAction::Cascade,
        Some(ast::ReferentialAction::SetNull) => FkAction::SetNull,
        Some(ast::ReferentialAction::SetDefault) => FkAction::SetDefault,
    }
}

/// Maximum allowed parenthesis-nesting depth in a SQL statement before we reject
/// it outright as too complex.
///
/// This is a DoS guard, NOT a semantic limit. It backstops absurd parenthesis
/// nesting (plain `(((...)))`, arithmetic, etc.). Plain paren nesting does NOT
/// exponentially backtrack in sqlparser — it hits the parser's own recursion
/// limit cleanly — so this cap can be generous. Real-world SQL nests only a
/// handful of parens deep, so 100 rejects no legitimate query.
const MAX_PARSE_NESTING_DEPTH: usize = 100;

/// Maximum allowed nesting depth of `CAST` / `TRY_CAST` / `SAFE_CAST` / `CONVERT`
/// expressions before we reject the statement.
///
/// This is the load-bearing part of the DoS guard. sqlparser's recursive-descent
/// expression parser backtracks EXPONENTIALLY on deeply-nested CAST grammar: a
/// depth-48 chain explores on the order of 2^48 alternative parse paths and pins
/// a CPU core for minutes. Empirically the cliff is sharp — depth 47 parses in
/// ~1.5 ms, depth 48 never completes — and it sits UNDER sqlparser's own
/// `RecursionCounter` (DEFAULT_REMAINING_DEPTH = 50), so the built-in guard never
/// fires. We must therefore reject deep CAST nesting BEFORE handing the input to
/// the parser.
///
/// 32 is far below the ~48 cliff (so the exponential never gets going) yet far
/// above any realistic query (real queries nest at most a couple of casts).
/// Postgres behaves analogously: beyond `max_stack_depth` it returns
/// `54001 statement too complex` immediately rather than spinning. Note this caps
/// the *simultaneously-open* CAST depth, not the total CAST count — a query with
/// thousands of non-nested casts (`SELECT CAST(a AS INT), CAST(b AS INT), ...`)
/// parses in linear time and is unaffected.
const MAX_CAST_NESTING_DEPTH: usize = 32;

/// Returns true if the keyword token ending just before `paren_idx` (skipping
/// whitespace) is a CAST-family keyword — i.e. this `(` opens a cast expression.
/// `bytes[..paren_idx]` is the SQL up to (not including) the `(`.
fn paren_is_cast(bytes: &[u8], paren_idx: usize) -> bool {
    // Walk back over whitespace between the keyword and the '('.
    let mut end = paren_idx;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    // Walk back over the identifier characters of the preceding token.
    let mut start = end;
    while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        start -= 1;
    }
    if start == end {
        return false;
    }
    // The token must not be the tail of a longer identifier (e.g. `mycast(`):
    // the char before `start` must not be an identifier char.
    if start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        return false;
    }
    let word = &bytes[start..end];
    word.eq_ignore_ascii_case(b"CAST")
        || word.eq_ignore_ascii_case(b"TRY_CAST")
        || word.eq_ignore_ascii_case(b"SAFE_CAST")
        || word.eq_ignore_ascii_case(b"CONVERT")
}

/// Cheap O(n) pre-parse complexity guard.
///
/// Scans the raw SQL once, tracking (a) running parenthesis-nesting depth and
/// (b) running CAST-expression nesting depth, and returns an error if either
/// exceeds its cap. This MUST run before `Parser::parse_sql` so the
/// exponential-backtracking inputs never reach the recursive-descent parser.
///
/// String literals (`'...'`), quoted/escaped identifiers (`"..."`,
/// `` `...` ``), dollar-quoted strings (`$tag$...$tag$`), and SQL comments
/// (`-- ...`, `/* ... */`) are skipped so that parentheses appearing inside
/// string data or comments are not miscounted — e.g. `WHERE note = '(((('`
/// must not be rejected.
fn check_nesting_depth(sql: &str) -> Result<(), ParseError> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let len = bytes.len();
    let mut depth: usize = 0;
    // Per-open-paren stack of "did this paren open a CAST expression?" plus a
    // running count of currently-open CAST parens.
    let mut cast_stack: Vec<bool> = Vec::new();
    let mut cast_depth: usize = 0;

    while i < len {
        let b = bytes[i];
        match b {
            // ── Single-quoted string literal: '...' with '' escaping ──────────
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        // Doubled '' is an escaped quote inside the literal.
                        if i + 1 < len && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break; // closing quote
                    }
                    i += 1;
                }
            }
            // ── Double-quoted identifier: "..." with "" escaping ──────────────
            b'"' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'"' {
                        if i + 1 < len && bytes[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            // ── Backtick-quoted identifier: `...` ─────────────────────────────
            b'`' => {
                i += 1;
                while i < len && bytes[i] != b'`' {
                    i += 1;
                }
            }
            // ── Line comment: -- to end of line ───────────────────────────────
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            // ── Block comment: /* ... */ (non-nested, matching sqlparser) ──────
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 1; // skip the closing '/'
            }
            // ── Dollar-quoted string: $tag$ ... $tag$ (Postgres) ──────────────
            b'$' => {
                // Find the closing '$' of the opening tag. A valid tag contains
                // only letters/digits/underscore; anything else means this '$'
                // is not a dollar-quote opener (e.g. a positional param $1).
                let tag_start = i;
                let mut j = i + 1;
                while j < len && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                    j += 1;
                }
                if j < len && bytes[j] == b'$' {
                    // tag = sql[tag_start..=j]  (includes both '$' delimiters)
                    let tag = &bytes[tag_start..=j];
                    i = j + 1;
                    // Scan for the closing tag.
                    while i < len {
                        if bytes[i] == b'$'
                            && i + tag.len() <= len
                            && &bytes[i..i + tag.len()] == tag
                        {
                            i += tag.len();
                            break;
                        }
                        i += 1;
                    }
                    continue; // i already advanced past the closing tag
                }
                // Not a dollar-quote; fall through and treat '$' as an ordinary
                // character (i is advanced by the tail increment below).
                i += 1;
                continue;
            }
            b'(' => {
                depth += 1;
                if depth > MAX_PARSE_NESTING_DEPTH {
                    return Err(ParseError::StatementTooComplex(MAX_PARSE_NESTING_DEPTH));
                }
                let is_cast = paren_is_cast(bytes, i);
                cast_stack.push(is_cast);
                if is_cast {
                    cast_depth += 1;
                    if cast_depth > MAX_CAST_NESTING_DEPTH {
                        return Err(ParseError::StatementTooComplex(MAX_CAST_NESTING_DEPTH));
                    }
                }
            }
            b')' => {
                depth = depth.saturating_sub(1);
                if cast_stack.pop() == Some(true) {
                    cast_depth = cast_depth.saturating_sub(1);
                }
            }
            _ => {}
        }
        i += 1;
    }

    Ok(())
}

/// Parse a SQL string into sqlparser AST statements.
pub fn parse(sql: &str) -> Result<Vec<ast::Statement>, ParseError> {
    // DoS guard: reject pathologically deep nesting BEFORE handing the input to
    // sqlparser, whose recursive-descent parser backtracks exponentially on deep
    // nested-CAST grammars (see `MAX_PARSE_NESTING_DEPTH`). This is the
    // load-bearing fix; it runs on every parse path (raw parse, the AST-cache
    // miss branch, and the wire fallback all funnel through here).
    check_nesting_depth(sql)?;

    // NOTE: we deliberately keep sqlparser's DEFAULT recursion limit (50). It is
    // well-tuned: deep single-path constructs (e.g. nested scalar subqueries
    // `SELECT (SELECT (...))`) error cleanly at 50 in well under a millisecond.
    // RAISING the limit re-introduces exponential blow-up for those forms, so the
    // pre-parse `check_nesting_depth` caps above (parens 100, CAST nesting 32) are
    // the load-bearing defense — the CAST cap sits below sqlparser's ~48 cliff,
    // which itself is below the default-50 recursion guard.
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)?;
    Ok(stmts)
}

/// Convert a sqlparser DataType to our internal DataType.
pub fn convert_data_type(dt: &ast::DataType) -> Result<DataType, ParseError> {
    match dt {
        ast::DataType::Boolean => Ok(DataType::Bool),
        ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(DataType::Int32),
        ast::DataType::BigInt(_) => Ok(DataType::Int64),
        ast::DataType::Float(_) | ast::DataType::Double(_) | ast::DataType::DoublePrecision => {
            Ok(DataType::Float64)
        }
        ast::DataType::Text
        | ast::DataType::Varchar(_)
        | ast::DataType::CharVarying(_)
        | ast::DataType::CharacterVarying(_) => Ok(DataType::Text),
        ast::DataType::Char(_) | ast::DataType::Character(_) => Ok(DataType::Text),
        ast::DataType::JSONB => Ok(DataType::Jsonb),
        ast::DataType::JSON => Ok(DataType::Jsonb),
        ast::DataType::Date => Ok(DataType::Date),
        ast::DataType::Timestamp(_, timezone) => {
            if matches!(
                timezone,
                ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz
            ) {
                Ok(DataType::TimestampTz)
            } else {
                Ok(DataType::Timestamp)
            }
        }
        ast::DataType::TimestampNtz(_) => Ok(DataType::Timestamp),
        ast::DataType::Interval { .. } => Ok(DataType::Interval),
        ast::DataType::Numeric(_) | ast::DataType::Decimal(_) | ast::DataType::Dec(_) => {
            Ok(DataType::Numeric)
        }
        ast::DataType::Uuid => Ok(DataType::Uuid),
        ast::DataType::Bytea => Ok(DataType::Bytea),
        ast::DataType::Blob(_) => Ok(DataType::Bytea),
        ast::DataType::SmallInt(_) | ast::DataType::TinyInt(_) => Ok(DataType::Int32),
        ast::DataType::Real => Ok(DataType::Float64),
        ast::DataType::Array(inner) => match inner {
            ast::ArrayElemTypeDef::AngleBracket(dt) => {
                Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
            }
            ast::ArrayElemTypeDef::SquareBracket(dt, _) => {
                Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
            }
            ast::ArrayElemTypeDef::Parenthesis(dt) => {
                Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
            }
            ast::ArrayElemTypeDef::None => Ok(DataType::Array(Box::new(DataType::Text))),
        },
        ast::DataType::Custom(name, args) => {
            // Handle VECTOR(n) custom type
            if let Some(part) = name.0.first()
                && let Some(ident) = part.as_ident()
            {
                match ident.value.to_lowercase().as_str() {
                    "vector" => {
                        if args.is_empty() {
                            // VECTOR without dimension defaults to 0 (unknown dimension)
                            return Ok(DataType::Vector(0));
                        }
                        // Extract dimensionality from args (args are Strings in sqlparser 0.61)
                        if args.len() == 1
                            && let Ok(dim) = args[0].parse::<usize>()
                        {
                            return Ok(DataType::Vector(dim));
                        }
                        return Err(ParseError::UnsupportedDataType(
                            "VECTOR type requires a numeric dimension, e.g., VECTOR(384)".into(),
                        ));
                    }
                    // Serial types: stored as Int32/Int64; executor auto-creates sequences.
                    "serial" | "serial4" => return Ok(DataType::Int32),
                    "bigserial" | "serial8" => return Ok(DataType::Int64),
                    "smallserial" | "serial2" => return Ok(DataType::Int32),
                    _ => {}
                }
                // Fall through: treat as a user-defined type (e.g. an enum).
                return Ok(DataType::UserDefined(ident.value.clone()));
            }
            Err(ParseError::UnsupportedDataType(format!("{name}")))
        }
        other => Err(ParseError::UnsupportedDataType(format!("{other}"))),
    }
}

/// Extract column definitions from a CREATE TABLE statement's columns.
pub fn extract_columns(columns: &[ast::ColumnDef]) -> Result<Vec<ColumnDef>, ParseError> {
    columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let data_type = convert_data_type(&col.data_type)?;
            let nullable = !col.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    ast::ColumnOption::NotNull | ast::ColumnOption::PrimaryKey(_)
                )
            });
            let default_expr = col.options.iter().find_map(|opt| match &opt.option {
                ast::ColumnOption::Default(expr) => Some(expr.to_string()),
                _ => None,
            });
            Ok(ColumnDef {
                name: col.name.value.clone(),
                data_type,
                nullable,
                default_expr,
                // 1-based so `0` stays available as "no id recorded" for
                // columns read from a pre-id snapshot.
                id: idx as u32 + 1,
                    analyzer: None,
            })
        })
        .collect()
}

/// Return which column names require an auto-sequence (SERIAL / BIGSERIAL / SMALLSERIAL /
/// GENERATED ALWAYS AS IDENTITY / GENERATED BY DEFAULT AS IDENTITY).
/// The returned list contains `(column_name, is_bigserial)` pairs where `is_bigserial`
/// determines whether the sequence value should be cast to Int64.
pub fn extract_serial_columns(columns: &[ast::ColumnDef]) -> Vec<(String, bool)> {
    let mut serials = Vec::new();
    for col in columns {
        // Check the type name first.
        let is_serial = if let ast::DataType::Custom(name, _) = &col.data_type {
            if let Some(part) = name.0.first() {
                if let Some(ident) = part.as_ident() {
                    matches!(
                        ident.value.to_lowercase().as_str(),
                        "serial" | "serial4" | "serial2" | "smallserial" | "bigserial" | "serial8"
                    )
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        let is_bigserial = if let ast::DataType::Custom(name, _) = &col.data_type {
            if let Some(part) = name.0.first() {
                if let Some(ident) = part.as_ident() {
                    matches!(ident.value.to_lowercase().as_str(), "bigserial" | "serial8")
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        // Also check for GENERATED ALWAYS/BY DEFAULT AS IDENTITY.
        let has_identity = col.options.iter().any(|opt| {
            matches!(
                &opt.option,
                ast::ColumnOption::Generated {
                    generation_expr: None,
                    ..
                } | ast::ColumnOption::Identity(_)
            )
        });

        if is_serial || has_identity {
            serials.push((col.name.value.clone(), is_bigserial));
        }
    }
    serials
}

/// Catalog key for a (possibly quoted, possibly schema-qualified) object name.
/// Each part contributes its bare identifier value — `"users"` and `users` are
/// the same relation, matching Postgres, where quoting affects case folding
/// but not identity for names that need no folding. A leading `public.`
/// qualifier is dropped because unqualified DDL stores bare names;
/// `pg_catalog.` / `information_schema.` prefixes are preserved for
/// virtual-table dispatch.
pub fn object_name_key(name: &ast::ObjectName) -> String {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|p| match p.as_ident() {
            Some(id) => id.value.clone(),
            None => p.to_string(),
        })
        .collect();
    let skip = usize::from(parts.len() > 1 && parts[0] == "public");
    parts[skip..].join(".")
}

/// Bare column name for a single- or compound-identifier target (e.g. an
/// UPDATE assignment `"users"."age"` or `"age"`): the last part's value.
pub fn object_name_last(name: &ast::ObjectName) -> String {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|id| id.value.clone())
        .unwrap_or_else(|| name.to_string())
}

/// Column name of an index/constraint column entry. Quoted identifiers
/// (`PRIMARY KEY("post_id")`) must resolve to the bare column name —
/// `expr.to_string()` would keep the quote characters and never match the
/// catalog.
pub fn index_column_name(col: &ast::IndexColumn) -> String {
    match &col.column.expr {
        ast::Expr::Identifier(ident) => ident.value.clone(),
        ast::Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| col.column.expr.to_string()),
        other => other.to_string(),
    }
}

/// Extract table-level constraints and inline column constraints from a CREATE TABLE.
pub fn extract_constraints(
    columns: &[ast::ColumnDef],
    table_constraints: &[ast::TableConstraint],
) -> Vec<crate::catalog::TableConstraint> {
    use crate::catalog::TableConstraint;
    let mut constraints = Vec::new();

    // Inline column constraints (PRIMARY KEY, UNIQUE on single columns)
    for col in columns {
        for opt in &col.options {
            match &opt.option {
                ast::ColumnOption::PrimaryKey(_) => {
                    let has_pk = constraints
                        .iter()
                        .any(|c| matches!(c, TableConstraint::PrimaryKey { .. }));
                    if !has_pk {
                        constraints.push(TableConstraint::PrimaryKey {
                            name: opt.name.as_ref().map(|name| name.to_string()),
                            columns: vec![col.name.value.clone()],
                        });
                    }
                }
                ast::ColumnOption::Unique(_) => {
                    constraints.push(TableConstraint::Unique {
                        name: opt.name.as_ref().map(|name| name.to_string()),
                        columns: vec![col.name.value.clone()],
                    });
                }
                ast::ColumnOption::Check(expr) => {
                    constraints.push(TableConstraint::Check {
                        name: opt.name.as_ref().map(|name| name.to_string()),
                        expr: expr.to_string(),
                    });
                }
                ast::ColumnOption::ForeignKey(fk) => {
                    constraints.push(TableConstraint::ForeignKey {
                        name: opt.name.as_ref().map(|name| name.to_string()),
                        columns: vec![col.name.value.clone()],
                        ref_table: object_name_key(&fk.foreign_table),
                        ref_columns: fk
                            .referred_columns
                            .iter()
                            .map(|c| c.value.clone())
                            .collect(),
                        on_delete: convert_fk_action(&fk.on_delete),
                        on_update: convert_fk_action(&fk.on_update),
                    });
                }
                _ => {}
            }
        }
    }

    // Table-level constraints
    for tc in table_constraints {
        match tc {
            ast::TableConstraint::PrimaryKey(pk) => {
                constraints.retain(|c| !matches!(c, TableConstraint::PrimaryKey { .. }));
                constraints.push(TableConstraint::PrimaryKey {
                    name: pk.name.as_ref().map(|name| name.to_string()),
                    columns: pk.columns.iter().map(index_column_name).collect(),
                });
            }
            ast::TableConstraint::Unique(u) => {
                constraints.push(TableConstraint::Unique {
                    name: u.name.as_ref().map(|n| n.to_string()),
                    columns: u.columns.iter().map(index_column_name).collect(),
                });
            }
            ast::TableConstraint::Check(ck) => {
                constraints.push(TableConstraint::Check {
                    name: ck.name.as_ref().map(|n| n.to_string()),
                    expr: ck.expr.to_string(),
                });
            }
            ast::TableConstraint::ForeignKey(fk) => {
                constraints.push(TableConstraint::ForeignKey {
                    name: fk.name.as_ref().map(|n| n.to_string()),
                    columns: fk.columns.iter().map(|c| c.value.clone()).collect(),
                    ref_table: object_name_key(&fk.foreign_table),
                    ref_columns: fk
                        .referred_columns
                        .iter()
                        .map(|c| c.value.clone())
                        .collect(),
                    on_delete: convert_fk_action(&fk.on_delete),
                    on_update: convert_fk_action(&fk.on_update),
                });
            }
            _ => {}
        }
    }

    constraints
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("SQL parse error: {0}")]
    SqlParser(#[from] sqlparser::parser::ParserError),
    #[error("unsupported data type: {0}")]
    UnsupportedDataType(String),
    #[error("unexpected statement: expected {0}")]
    UnexpectedStatement(String),
    #[error("statement too complex: parenthesis nesting exceeds maximum of {0}")]
    StatementTooComplex(usize),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select() {
        let stmts = parse("SELECT 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::Query(_)));
    }

    #[test]
    fn parse_create_table() {
        let stmts = parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::CreateTable(_)));
    }

    #[test]
    fn parse_insert() {
        let stmts = parse("INSERT INTO t VALUES (1, 'hello')").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::Insert(_)));
    }

    #[test]
    fn parse_update() {
        let stmts = parse("UPDATE t SET name = 'world' WHERE id = 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::Update(_)));
    }

    #[test]
    fn parse_delete() {
        let stmts = parse("DELETE FROM t WHERE id = 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::Delete(_)));
    }

    #[test]
    fn parse_multiple_statements() {
        let stmts = parse("SELECT 1; SELECT 2; SELECT 3").unwrap();
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn parse_error_on_invalid_sql() {
        let result = parse("SELECTOID BLOOP FROM");
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_string() {
        let stmts = parse("").unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn convert_data_type_int() {
        let dt = convert_data_type(&ast::DataType::Integer(None)).unwrap();
        assert_eq!(dt, DataType::Int32);
    }

    #[test]
    fn convert_data_type_bigint() {
        let dt = convert_data_type(&ast::DataType::BigInt(None)).unwrap();
        assert_eq!(dt, DataType::Int64);
    }

    #[test]
    fn convert_data_type_float() {
        let dt = convert_data_type(&ast::DataType::Float(ast::ExactNumberInfo::None)).unwrap();
        assert_eq!(dt, DataType::Float64);
    }

    #[test]
    fn convert_data_type_text() {
        let dt = convert_data_type(&ast::DataType::Text).unwrap();
        assert_eq!(dt, DataType::Text);
    }

    #[test]
    fn convert_data_type_bool() {
        let dt = convert_data_type(&ast::DataType::Boolean).unwrap();
        assert_eq!(dt, DataType::Bool);
    }

    #[test]
    fn convert_data_type_jsonb() {
        let dt = convert_data_type(&ast::DataType::JSONB).unwrap();
        assert_eq!(dt, DataType::Jsonb);
    }

    #[test]
    fn convert_data_type_date() {
        let dt = convert_data_type(&ast::DataType::Date).unwrap();
        assert_eq!(dt, DataType::Date);
    }

    #[test]
    fn convert_data_type_uuid() {
        let dt = convert_data_type(&ast::DataType::Uuid).unwrap();
        assert_eq!(dt, DataType::Uuid);
    }

    #[test]
    fn convert_data_type_bytea() {
        let dt = convert_data_type(&ast::DataType::Bytea).unwrap();
        assert_eq!(dt, DataType::Bytea);
    }

    #[test]
    fn convert_data_type_numeric() {
        let dt = convert_data_type(&ast::DataType::Numeric(ast::ExactNumberInfo::None)).unwrap();
        assert_eq!(dt, DataType::Numeric);
    }

    #[test]
    fn convert_data_type_varchar() {
        let dt = convert_data_type(&ast::DataType::Varchar(None)).unwrap();
        assert_eq!(dt, DataType::Text);
    }

    #[test]
    fn extract_columns_basic() -> Result<(), ParseError> {
        let stmts = parse("CREATE TABLE t (id INT NOT NULL, name TEXT, age BIGINT)")?;
        if let ast::Statement::CreateTable(ct) = &stmts[0] {
            let cols = extract_columns(&ct.columns)?;
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0].name, "id");
            assert_eq!(cols[0].data_type, DataType::Int32);
            assert!(!cols[0].nullable);
            assert_eq!(cols[1].name, "name");
            assert_eq!(cols[1].data_type, DataType::Text);
            assert!(cols[1].nullable);
            assert_eq!(cols[2].name, "age");
            assert_eq!(cols[2].data_type, DataType::Int64);
        } else {
            return Err(ParseError::UnexpectedStatement("CREATE TABLE".into()));
        }
        Ok(())
    }

    #[test]
    fn extract_constraints_primary_key() -> Result<(), ParseError> {
        let stmts = parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")?;
        if let ast::Statement::CreateTable(ct) = &stmts[0] {
            let constraints = extract_constraints(&ct.columns, &ct.constraints);
            assert!(constraints.iter().any(|c| matches!(c, crate::catalog::TableConstraint::PrimaryKey { columns, .. } if columns == &["id"])));
        } else {
            return Err(ParseError::UnexpectedStatement("CREATE TABLE".into()));
        }
        Ok(())
    }

    #[test]
    fn parse_complex_query() {
        let sql = "SELECT u.id, u.name, COUNT(o.id) as order_count \
                   FROM users u \
                   JOIN orders o ON u.id = o.user_id \
                   WHERE u.active = true \
                   GROUP BY u.id, u.name \
                   HAVING COUNT(o.id) > 5 \
                   ORDER BY order_count DESC \
                   LIMIT 10";
        let stmts = parse(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_cte() {
        let sql = "WITH active_users AS (SELECT * FROM users WHERE active = true) \
                   SELECT * FROM active_users";
        let stmts = parse(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_window_function() {
        let sql = "SELECT name, salary, RANK() OVER (ORDER BY salary DESC) FROM employees";
        let stmts = parse(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_create_index() {
        let stmts = parse("CREATE INDEX idx_name ON users (name)").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::CreateIndex(_)));
    }

    #[test]
    fn parse_alter_table() {
        let stmts = parse("ALTER TABLE users ADD COLUMN email TEXT").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::AlterTable(_)));
    }

    #[test]
    fn parse_transaction_statements() {
        assert!(parse("BEGIN").is_ok());
        assert!(parse("COMMIT").is_ok());
        assert!(parse("ROLLBACK").is_ok());
    }

    // ========================================================================
    // Property-based tests (proptest)
    // ========================================================================

    use proptest::prelude::*;

    /// Strategy for valid SQL identifiers: starts with a lowercase letter,
    /// followed by 0..20 lowercase alphanumeric or underscore characters.
    fn ident_strategy() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,20}".prop_map(|s| s)
    }

    /// Strategy for integer literal values.
    fn int_val_strategy() -> impl Strategy<Value = String> {
        (0i64..1_000_000i64).prop_map(|n| n.to_string())
    }

    proptest! {
        /// SELECT {col} FROM {table} roundtrips through parse -> to_string -> parse.
        #[test]
        fn prop_sql_select_roundtrip(
            col in ident_strategy(),
            table in ident_strategy(),
        ) {
            let sql = format!("SELECT {col} FROM {table}");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// INSERT INTO {table} ({col}) VALUES ({val}) roundtrips.
        #[test]
        fn prop_sql_insert_roundtrip(
            table in ident_strategy(),
            col in ident_strategy(),
            val in int_val_strategy(),
        ) {
            let sql = format!("INSERT INTO {table} ({col}) VALUES ({val})");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// CREATE TABLE {table} ({col1} INT, {col2} TEXT) roundtrips.
        #[test]
        fn prop_sql_create_table_roundtrip(
            table in ident_strategy(),
            col1 in ident_strategy(),
            col2 in ident_strategy(),
        ) {
            let sql = format!("CREATE TABLE {table} ({col1} INT, {col2} TEXT)");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// DELETE FROM {table} WHERE {col} = {val} roundtrips.
        #[test]
        fn prop_sql_delete_roundtrip(
            table in ident_strategy(),
            col in ident_strategy(),
            val in int_val_strategy(),
        ) {
            let sql = format!("DELETE FROM {table} WHERE {col} = {val}");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// UPDATE {table} SET {col} = {val} roundtrips.
        #[test]
        fn prop_sql_update_roundtrip(
            table in ident_strategy(),
            col in ident_strategy(),
            val in int_val_strategy(),
        ) {
            let sql = format!("UPDATE {table} SET {col} = {val}");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// Random garbage strings never cause panics in the parser.
        #[test]
        fn prop_sql_random_garbage_no_panic(s in "\\PC{0,200}") {
            // Should either parse or return an error, but never panic.
            let _ = parse(&s);
        }
    }
}
