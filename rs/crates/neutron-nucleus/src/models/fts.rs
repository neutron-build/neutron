//! Full-text search model — FTS_INDEX, FTS_SEARCH, FTS_FUZZY_SEARCH,
//! FTS_REMOVE, FTS_DOC_COUNT, FTS_TERM_COUNT.

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// A single full-text search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsResult {
    pub doc_id: i64,
    pub score: f64,
}

/// Handle for full-text search operations.
pub struct FtsModel {
    pool: NucleusPool,
}

impl FtsModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Index a document's text for full-text search.
    pub async fn index(&self, doc_id: i64, text: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT FTS_INDEX($1, $2)", &[&doc_id, &text])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Perform an exact full-text search.
    pub async fn search(&self, query: &str, limit: i64) -> Result<Vec<FtsResult>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT FTS_SEARCH($1, $2)", &[&query, &limit])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        let results: Vec<FtsResult> =
            serde_json::from_str(&raw).map_err(|e| NucleusError::Serde(e.to_string()))?;
        Ok(results)
    }

    /// Perform a fuzzy full-text search with a maximum edit distance.
    pub async fn fuzzy_search(
        &self,
        query: &str,
        max_distance: i64,
        limit: i64,
    ) -> Result<Vec<FtsResult>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT FTS_FUZZY_SEARCH($1, $2, $3)",
                &[&query, &max_distance, &limit],
            )
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        let results: Vec<FtsResult> =
            serde_json::from_str(&raw).map_err(|e| NucleusError::Serde(e.to_string()))?;
        Ok(results)
    }

    /// Remove a document from the full-text index.
    pub async fn remove(&self, doc_id: i64) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT FTS_REMOVE($1)", &[&doc_id])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return the number of indexed documents.
    pub async fn doc_count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT FTS_DOC_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Return the number of indexed terms.
    pub async fn term_count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT FTS_TERM_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fts_result_serialize_deserialize() {
        let result = FtsResult {
            doc_id: 42,
            score: 0.95,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: FtsResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.doc_id, 42);
        assert!((deserialized.score - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn fts_result_vec_deserialize() {
        let json = r#"[
            {"doc_id": 1, "score": 0.9},
            {"doc_id": 2, "score": 0.7},
            {"doc_id": 3, "score": 0.5}
        ]"#;
        let results: Vec<FtsResult> = serde_json::from_str(json).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].doc_id, 1);
        assert!((results[0].score - 0.9).abs() < f64::EPSILON);
        assert_eq!(results[2].doc_id, 3);
    }

    #[test]
    fn fts_result_clone() {
        let result = FtsResult { doc_id: 10, score: 1.0 };
        let cloned = result.clone();
        assert_eq!(cloned.doc_id, 10);
        assert_eq!(cloned.score, 1.0);
    }

    #[test]
    fn fts_result_debug() {
        let result = FtsResult { doc_id: 1, score: 0.5 };
        let dbg = format!("{:?}", result);
        assert!(dbg.contains("FtsResult"));
        assert!(dbg.contains("doc_id"));
    }

    #[test]
    fn fts_result_zero_score() {
        let result = FtsResult { doc_id: 0, score: 0.0 };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: FtsResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.doc_id, 0);
        assert_eq!(deserialized.score, 0.0);
    }
}
