//! Datalog reasoning model — DATALOG_ASSERT, DATALOG_RETRACT, DATALOG_RULE,
//! DATALOG_QUERY, DATALOG_CLEAR, DATALOG_IMPORT_GRAPH.

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Handle for Datalog reasoning operations.
pub struct DatalogModel {
    pool: NucleusPool,
}

impl DatalogModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Assert a fact into the Datalog knowledge base.
    pub async fn assert_fact(&self, fact: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_ASSERT($1)", &[&fact])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Retract a fact from the Datalog knowledge base.
    pub async fn retract(&self, fact: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_RETRACT($1)", &[&fact])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Define a Datalog rule with a head and body.
    pub async fn rule(&self, head: &str, body: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_RULE($1, $2)", &[&head, &body])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Evaluate a Datalog query. Returns results as CSV text.
    pub async fn query(&self, pattern: &str) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_QUERY($1)", &[&pattern])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, String>(0))
    }

    /// Clear all facts and rules from the Datalog knowledge base.
    pub async fn clear(&self) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_CLEAR()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Import graph data into the Datalog knowledge base.
    /// Returns the number of facts imported.
    pub async fn import_graph(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_IMPORT_GRAPH()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }
}
