//! Columnar analytics model — COLUMNAR_INSERT, COLUMNAR_COUNT, COLUMNAR_SUM,
//! COLUMNAR_AVG, COLUMNAR_MIN, COLUMNAR_MAX.

use serde_json;

use crate::error::NucleusError;
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
    pub async fn insert(
        &self,
        table: &str,
        values: &serde_json::Value,
    ) -> Result<bool, NucleusError> {
        if !is_valid_identifier(table) {
            return Err(NucleusError::InvalidIdentifier(table.to_string()));
        }
        let values_str =
            serde_json::to_string(values).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT COLUMNAR_INSERT($1, $2)", &[&table, &values_str])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
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
        Ok(row.get::<_, i64>(0))
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
        Ok(row.get::<_, f64>(0))
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
        Ok(row.get::<_, f64>(0))
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
        Ok(row.get::<_, String>(0))
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
        Ok(row.get::<_, String>(0))
    }
}
