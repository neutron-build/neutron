//! SQL model — standard relational query execution.
//!
//! This wraps the existing [`Db`](crate::Db) functionality but is accessible
//! through the unified [`NucleusClient`](crate::NucleusClient) API.

use tokio_postgres::types::ToSql;
use tokio_postgres::Row;

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Handle for standard SQL operations.
pub struct SqlModel {
    pool: NucleusPool,
}

impl SqlModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Execute a statement, returning the number of rows affected.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .execute(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Execute a query and return all matching rows.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Execute a query expecting exactly one row.
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query_one(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Execute a query expecting zero or one rows.
    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query_opt(sql, params)
            .await
            .map_err(NucleusError::Query)
    }
}
