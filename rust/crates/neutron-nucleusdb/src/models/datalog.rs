//! Datalog reasoning model — DATALOG_ASSERT, DATALOG_RETRACT, DATALOG_RULE,
//! DATALOG_QUERY, DATALOG_CLEAR, DATALOG_IMPORT_GRAPH.

use crate::error::NucleusError;
use crate::pool::NucleusPool;
use crate::row_ext::RowExt;

/// Handle for Datalog reasoning operations.
pub struct DatalogModel {
    pool: NucleusPool,
}

impl DatalogModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Assert a fact into the Datalog knowledge base.
    /// Returns the engine's status message.
    pub async fn assert_fact(&self, fact: &str) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_ASSERT($1)", &[&fact])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }

    /// Retract a fact from the Datalog knowledge base.
    /// Returns the engine's status message.
    pub async fn retract(&self, fact: &str) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_RETRACT($1)", &[&fact])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }

    /// Define a Datalog rule. The head and body are joined into the engine's
    /// single-string `head :- body` form. Returns the engine's status message.
    pub async fn rule(&self, head: &str, body: &str) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let rule = format!("{head} :- {body}");
        let row = conn
            .client()
            .query_one("SELECT DATALOG_RULE($1)", &[&rule])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }

    /// Evaluate a Datalog query. Returns results as a JSON array of arrays.
    pub async fn query(&self, pattern: &str) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_QUERY($1)", &[&pattern])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }

    /// Clear all facts and rules for a predicate.
    /// Returns the engine's status message.
    pub async fn clear(&self, predicate: &str) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_CLEAR($1)", &[&predicate])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }

    /// Import all graph edges as facts: `predicate(from_id, edge_type, to_id)`.
    /// Returns the engine's status message (`IMPORTED N edges into <predicate>`).
    pub async fn import_graph(&self, predicate: &str) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DATALOG_IMPORT_GRAPH($1)", &[&predicate])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }
}
