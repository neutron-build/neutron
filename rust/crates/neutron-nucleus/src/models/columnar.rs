//! Columnar analytics model — COLUMNAR_INSERT, COLUMNAR_COUNT, COLUMNAR_SUM,
//! COLUMNAR_AVG, COLUMNAR_MIN, COLUMNAR_MAX.

use serde_json;

use crate::error::NucleusError;
use crate::row_ext::RowExt;
use crate::models::is_valid_identifier;
use crate::pool::NucleusPool;

/// Handle for columnar analytics operations.
pub struct ColumnarModel {
    pool: NucleusPool,
}

impl ColumnarModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Insert a row into a columnar table. Values is a JSON object of column->value.
    ///
    /// `COLUMNAR_INSERT` is variadic: `(table, col1, val1, col2, val2, ...)`.
    pub async fn insert(
        &self,
        table: &str,
        values: &serde_json::Value,
    ) -> Result<(), NucleusError> {
        if !is_valid_identifier(table) {
            return Err(NucleusError::InvalidIdentifier(table.to_string()));
        }
        let obj = values
            .as_object()
            .ok_or_else(|| NucleusError::Serde("values must be a JSON object".to_string()))?;
        if obj.is_empty() {
            return Err(NucleusError::Serde(
                "values must contain at least one column".to_string(),
            ));
        }

        // Build: SELECT COLUMNAR_INSERT($1, $2, $3, $4, $5, ...)
        let mut params: Vec<String> = vec!["$1".to_string()];
        for (i, _) in obj.iter().enumerate() {
            params.push(format!("${}", i * 2 + 2));
            params.push(format!("${}", i * 2 + 3));
        }
        let sql = format!("SELECT COLUMNAR_INSERT({})", params.join(", "));

        // Owned, typed parameter values (col name, then value).
        let mut owned: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::new();
        owned.push(Box::new(table.to_string()));
        for (col, val) in obj {
            owned.push(Box::new(col.clone()));
            match val {
                serde_json::Value::Bool(b) => owned.push(Box::new(*b)),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        owned.push(Box::new(i));
                    } else {
                        owned.push(Box::new(n.as_f64().unwrap_or(f64::NAN)));
                    }
                }
                serde_json::Value::String(s) => owned.push(Box::new(s.clone())),
                serde_json::Value::Null => owned.push(Box::new(Option::<String>::None)),
                other => owned.push(Box::new(other.to_string())),
            }
        }
        let query_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            owned.iter().map(|b| b.as_ref()).collect();

        let conn = self.pool.get().await?;
        // Engine returns the text 'OK'.
        conn.client()
            .query_one(&sql, &query_params)
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    /// Return the number of rows in a columnar table.
    pub async fn count(&self, table: &str) -> Result<i64, NucleusError> {
        if !is_valid_identifier(table) {
            return Err(NucleusError::InvalidIdentifier(table.to_string()));
        }
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT COLUMNAR_COUNT($1)", &[&table])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<i64>(0)
    }

    /// Return the sum of a column.
    pub async fn sum(&self, table: &str, column: &str) -> Result<f64, NucleusError> {
        if !is_valid_identifier(table) {
            return Err(NucleusError::InvalidIdentifier(table.to_string()));
        }
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT COLUMNAR_SUM($1, $2)", &[&table, &column])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<f64>(0)
    }

    /// Return the average of a column.
    pub async fn avg(&self, table: &str, column: &str) -> Result<f64, NucleusError> {
        if !is_valid_identifier(table) {
            return Err(NucleusError::InvalidIdentifier(table.to_string()));
        }
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT COLUMNAR_AVG($1, $2)", &[&table, &column])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<f64>(0)
    }

    /// Return the minimum value of a column (as a string for type flexibility).
    pub async fn min(&self, table: &str, column: &str) -> Result<String, NucleusError> {
        if !is_valid_identifier(table) {
            return Err(NucleusError::InvalidIdentifier(table.to_string()));
        }
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT COLUMNAR_MIN($1, $2)::TEXT", &[&table, &column])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }

    /// Return the maximum value of a column (as a string for type flexibility).
    pub async fn max(&self, table: &str, column: &str) -> Result<String, NucleusError> {
        if !is_valid_identifier(table) {
            return Err(NucleusError::InvalidIdentifier(table.to_string()));
        }
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT COLUMNAR_MAX($1, $2)::TEXT", &[&table, &column])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }
}
