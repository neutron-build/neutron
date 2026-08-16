//! Change Data Capture model — CDC_READ, CDC_COUNT, CDC_TABLE_READ.

use crate::error::NucleusError;
use crate::row_ext::RowExt;
use crate::pool::NucleusPool;

/// Handle for CDC (Change Data Capture) operations.
pub struct CdcModel {
    pool: NucleusPool,
}

impl CdcModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Read up to `limit` CDC events after the given sequence number.
    /// Returns raw CDC event data as a JSON string.
    pub async fn read(&self, after_sequence: i64, limit: i64) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT CDC_READ($1, $2)", &[&after_sequence, &limit])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<String>(0)?)
    }

    /// Return the total number of CDC events.
    pub async fn count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT CDC_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<i64>(0)?)
    }

    /// Read up to `limit` CDC events for a specific table after the given
    /// sequence number.
    pub async fn table_read(
        &self,
        table: &str,
        after_sequence: i64,
        limit: i64,
    ) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT CDC_TABLE_READ($1, $2, $3)",
                &[&table, &after_sequence, &limit],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<String>(0)?)
    }
}
