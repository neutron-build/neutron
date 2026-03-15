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

/// Parse a SQL string into sqlparser AST statements.
pub fn parse(sql: &str) -> Result<Vec<ast::Statement>, ParseError> {
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
        ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
        ast::DataType::Numeric(_) | ast::DataType::Decimal(_) | ast::DataType::Dec(_) => {
            Ok(DataType::Numeric)
        }
        ast::DataType::Uuid => Ok(DataType::Uuid),
        ast::DataType::Bytea => Ok(DataType::Bytea),
        ast::DataType::Blob(_) => Ok(DataType::Bytea),
        ast::DataType::SmallInt(_) | ast::DataType::TinyInt(_) => Ok(DataType::Int32),
        ast::DataType::Real => Ok(DataType::Float64),
        ast::DataType::Array(inner) => {
            match inner {
                ast::ArrayElemTypeDef::AngleBracket(dt) => {
                    Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
                }
                ast::ArrayElemTypeDef::SquareBracket(dt, _) => {
                    Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
                }
                ast::ArrayElemTypeDef::Parenthesis(dt) => {
                    Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
                }
                ast::ArrayElemTypeDef::None => {
                    Ok(DataType::Array(Box::new(DataType::Text)))
                }
            }
        }
        ast::DataType::Custom(name, args) => {
            // Handle VECTOR(n) custom type
            if let Some(part) = name.0.first()
                && let Some(ident) = part.as_ident() {
                    match ident.value.to_lowercase().as_str() {
                        "vector" => {
                            if args.is_empty() {
                                // VECTOR without dimension defaults to 0 (unknown dimension)
                                return Ok(DataType::Vector(0));
                            }
                            // Extract dimensionality from args (args are Strings in sqlparser 0.61)
                            if args.len() == 1
                                && let Ok(dim) = args[0].parse::<usize>() {
                                    return Ok(DataType::Vector(dim));
                                }
                            return Err(ParseError::UnsupportedDataType(
                                "VECTOR type requires a numeric dimension, e.g., VECTOR(384)".into()
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
        .map(|col| {
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
                        "serial" | "serial4" | "serial2" | "smallserial"
                        | "bigserial" | "serial8"
                    )
                } else { false }
            } else { false }
        } else { false };

        let is_bigserial = if let ast::DataType::Custom(name, _) = &col.data_type {
            if let Some(part) = name.0.first() {
                if let Some(ident) = part.as_ident() {
                    matches!(ident.value.to_lowercase().as_str(), "bigserial" | "serial8")
                } else { false }
            } else { false }
        } else { false };

        // Also check for GENERATED ALWAYS/BY DEFAULT AS IDENTITY.
        let has_identity = col.options.iter().any(|opt| {
            matches!(
                &opt.option,
                ast::ColumnOption::Generated { generation_expr: None, .. }
                | ast::ColumnOption::Identity(_)
            )
        });

        if is_serial || has_identity {
            serials.push((col.name.value.clone(), is_bigserial));
        }
    }
    serials
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
                    let has_pk = constraints.iter().any(|c| matches!(c, TableConstraint::PrimaryKey { .. }));
                    if !has_pk {
                        constraints.push(TableConstraint::PrimaryKey {
                            columns: vec![col.name.value.clone()],
                        });
                    }
                }
                ast::ColumnOption::Unique(_) => {
                    constraints.push(TableConstraint::Unique {
                        name: None,
                        columns: vec![col.name.value.clone()],
                    });
                }
                ast::ColumnOption::Check(expr) => {
                    constraints.push(TableConstraint::Check {
                        name: None,
                        expr: expr.to_string(),
                    });
                }
                ast::ColumnOption::ForeignKey(fk) => {
                    constraints.push(TableConstraint::ForeignKey {
                        name: None,
                        columns: vec![col.name.value.clone()],
                        ref_table: fk.foreign_table.to_string(),
                        ref_columns: fk.referred_columns.iter().map(|c| c.value.clone()).collect(),
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
                    columns: pk.columns.iter().map(|c| c.column.expr.to_string()).collect(),
                });
            }
            ast::TableConstraint::Unique(u) => {
                constraints.push(TableConstraint::Unique {
                    name: u.name.as_ref().map(|n| n.to_string()),
                    columns: u.columns.iter().map(|c| c.column.expr.to_string()).collect(),
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
                    ref_table: fk.foreign_table.to_string(),
                    ref_columns: fk.referred_columns.iter().map(|c| c.value.clone()).collect(),
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
            assert!(constraints.iter().any(|c| matches!(c, crate::catalog::TableConstraint::PrimaryKey { columns } if columns == &["id"])));
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
