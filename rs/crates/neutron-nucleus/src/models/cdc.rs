//! Change Data Capture model — CDC_READ, CDC_COUNT, CDC_TABLE_READ.

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Handle for CDC (Change Data Capture) operations.
pub struct CdcModel {
    pool: NucleusPool,
}

impl CdcModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Read CDC events starting from the given offset.
    /// Returns raw CDC event data as a JSON string.
    pub async fn read(&self, offset: i64) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT CDC_READ($1)", &[&offset])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, String>(0))
    }

    /// Return the total number of CDC events.
    pub async fn count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT CDC_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Read CDC events for a specific table starting from the given offset.
    pub async fn table_read(&self, table: &str, offset: i64) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT CDC_TABLE_READ($1, $2)", &[&table, &offset])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, String>(0))
    }
}
