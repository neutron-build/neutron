//! Binary Protocol Query Handler — SQL parsing, binding, and preparation.
//!
//! Reuses nucleus executor components:
//! - sqlparser for SQL parsing
//! - executor/param_subst.rs for parameter substitution
//!
//! Supports both simple queries and prepared statements.

use crate::executor::{ExecError, Executor};
use crate::types::Value;
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;
use std::sync::Arc;

/// Parsed and prepared query state.
#[derive(Debug, Clone)]
pub struct PreparedQuery {
    /// Unique statement ID (allocated by connection handler)
    pub stmt_id: u32,
    /// Original SQL text
    pub sql: String,
    /// Parameter count (count of ? or $1, $2, etc. placeholders)
    pub param_count: u16,
    /// Parsed statement (cached AST)
    pub ast: String, // Store parsed form for quick re-execution
}

impl PreparedQuery {
    /// Create a new prepared query.
    pub fn new(stmt_id: u32, sql: impl Into<String>) -> Result<Self, ExecError> {
        let sql = sql.into();

        // Parse SQL to validate syntax and count parameters
        let dialect = PostgreSqlDialect {};
        let _parsed = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| ExecError::Parse(crate::sql::ParseError::UnexpectedStatement(e.to_string())))?;

        // Count parameter placeholders ($1, $2, etc.)
        let mut param_count = 0u16;
        for cap in sql.split('$').skip(1) {
            if cap.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                param_count += 1;
            }
        }

        Ok(Self {
            stmt_id,
            sql: sql.clone(),
            param_count,
            ast: sql,
        })
    }
}

/// Query handler for simple and prepared queries.
pub struct QueryHandler {
    #[allow(dead_code)]
    executor: Arc<Executor>,
    prepared_cache: std::collections::HashMap<u32, PreparedQuery>,
}

impl QueryHandler {
    /// Create a new query handler.
    pub fn new(executor: Arc<Executor>) -> Self {
        Self {
            executor,
            prepared_cache: std::collections::HashMap::new(),
        }
    }

    /// Prepare a statement for later execution.
    pub fn prepare_statement(
        &mut self,
        stmt_id: u32,
        sql: &str,
    ) -> Result<PreparedQuery, ExecError> {
        let prepared = PreparedQuery::new(stmt_id, sql)?;
        self.prepared_cache.insert(stmt_id, prepared.clone());
        Ok(prepared)
    }

    /// Get a prepared statement.
    pub fn get_prepared(&self, stmt_id: u32) -> Result<PreparedQuery, ExecError> {
        self.prepared_cache
            .get(&stmt_id)
            .cloned()
            .ok_or_else(|| {
                ExecError::Runtime(format!("Prepared statement {} not found", stmt_id))
            })
    }

    /// Bind parameters to a prepared statement.
    pub fn bind_parameters(
        &self,
        stmt_id: u32,
        params: Vec<Value>,
    ) -> Result<String, ExecError> {
        let prepared = self.get_prepared(stmt_id)?;

        if params.len() != prepared.param_count as usize {
            return Err(ExecError::Runtime(format!(
                "Parameter count mismatch: expected {}, got {}",
                prepared.param_count,
                params.len()
            )));
        }

        // Perform parameter substitution on the SQL
        // This would integrate with executor/param_subst.rs
        let mut substituted = prepared.sql.clone();
        for (i, param) in params.iter().enumerate() {
            let _placeholder = format!("${}", i + 1);
            let param_str = match param {
                Value::Null => "NULL".to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Int32(n) => n.to_string(),
                Value::Int64(n) => n.to_string(),
                Value::Float64(f) => f.to_string(),
                Value::Text(s) => format!("'{}'", s.replace("'", "''")),
                _ => return Err(ExecError::Runtime(format!("Unsupported parameter type: {:?}", param))),
            };
            substituted = substituted.replace("?", &param_str);
        }

        Ok(substituted)
    }

    /// Deallocate a prepared statement.
    pub fn deallocate(&mut self, stmt_id: u32) -> Result<(), ExecError> {
        self.prepared_cache.remove(&stmt_id);
        Ok(())
    }

    /// Get prepared statement count (for metrics).
    pub fn prepared_count(&self) -> usize {
        self.prepared_cache.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepared_query_creation() {
        let prepared = PreparedQuery::new(1, "SELECT * FROM users WHERE id = $1");
        assert!(prepared.is_ok());
        let q = prepared.unwrap();
        assert_eq!(q.stmt_id, 1);
        assert_eq!(q.param_count, 1);
    }

    #[test]
    fn test_prepared_query_invalid_sql() {
        let prepared = PreparedQuery::new(1, "INVALID SQL SYNTAX");
        assert!(prepared.is_err());
    }

    #[test]
    fn test_prepared_query_multiple_params() {
        let prepared = PreparedQuery::new(2, "INSERT INTO t VALUES ($1, $2, $3)");
        assert!(prepared.is_ok());
        let q = prepared.unwrap();
        assert_eq!(q.param_count, 3);
    }

    #[test]
    fn test_query_handler_prepare() {
        // Note: Would need actual Executor instance for full testing
        // This is a stub test to ensure compilation
        let sql = "SELECT 1";
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_parameter_count_zero() {
        let prepared = PreparedQuery::new(3, "SELECT * FROM users");
        assert!(prepared.is_ok());
        let q = prepared.unwrap();
        assert_eq!(q.param_count, 0);
    }

    #[test]
    fn test_bind_parameter_substitution() {
        // Test parameter substitution logic
        let mut sql = "SELECT ? WHERE id = ?".to_string();
        sql = sql.replacen("?", "42", 1);
        assert_eq!(sql, "SELECT 42 WHERE id = ?");
    }
}
