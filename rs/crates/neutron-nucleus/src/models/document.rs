//! Document/JSON model — DOC_INSERT, DOC_GET, DOC_QUERY, DOC_PATH, DOC_COUNT.

use serde_json;

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Handle for document store operations.
pub struct DocumentModel {
    pool: NucleusPool,
}

impl DocumentModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Insert a JSON document. Returns the generated document ID.
    pub async fn insert(&self, doc: &serde_json::Value) -> Result<i64, NucleusError> {
        let json_str =
            serde_json::to_string(doc).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DOC_INSERT($1)", &[&json_str])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Retrieve a document by ID. Returns `None` if not found.
    pub async fn get(&self, id: i64) -> Result<Option<serde_json::Value>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DOC_GET($1)", &[&id])
            .await
            .map_err(NucleusError::Query)?;
        let raw: Option<String> = row.get(0);
        match raw {
            Some(s) => {
                let val: serde_json::Value =
                    serde_json::from_str(&s).map_err(|e| NucleusError::Serde(e.to_string()))?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Query documents matching a JSON filter. Returns matching document IDs.
    pub async fn query(&self, filter: &serde_json::Value) -> Result<Vec<i64>, NucleusError> {
        let filter_str =
            serde_json::to_string(filter).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DOC_QUERY($1)", &[&filter_str])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<i64> = raw
            .split(',')
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .collect();
        Ok(ids)
    }

    /// Extract a nested value from a document using a key path.
    pub async fn path(&self, id: i64, keys: &[&str]) -> Result<Option<String>, NucleusError> {
        if keys.is_empty() {
            return Ok(None);
        }
        // Build: SELECT DOC_PATH($1, $2, $3, ...)
        let mut params: Vec<String> = vec!["$1".to_string()];
        for (i, _) in keys.iter().enumerate() {
            params.push(format!("${}", i + 2));
        }
        let sql = format!("SELECT DOC_PATH({})", params.join(", "));

        let conn = self.pool.get().await?;
        // Build dynamic params
        let mut query_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
        query_params.push(&id);
        for key in keys {
            query_params.push(key);
        }
        let row = conn
            .client()
            .query_one(&sql, &query_params)
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, Option<String>>(0))
    }

    /// Return the total number of stored documents.
    pub async fn count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT DOC_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }
}
