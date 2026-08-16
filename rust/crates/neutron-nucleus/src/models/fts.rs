//! Full-text search.
//!
//! Two shapes, and the choice between them matters:
//!
//! **Table-attached** — `create_index`, `matches`, `bm25`. Search is an index
//! on a table column, so hits come back as ordinary rows: joinable,
//! filterable, and covered by the same transactions and row-level security
//! policies as everything else. This is what you want for anything that
//! indexes table data.
//!
//! **Document store** — `index`, `search`, `fuzzy_search`. A separate
//! doc-id-keyed corpus that returns `(doc_id, score)` pairs rather than rows.
//! It remains for corpora with no table behind them, and for fuzzy search,
//! which the table-attached index does not yet expose.
//!
//! The table-attached index requires an integer `PRIMARY KEY`; documents are
//! keyed on it so maintenance survives deletes shifting physical row
//! positions. A table without one can still match with `@@` — it just scans
//! instead of using an index, and cannot rank with `BM25`.

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::NucleusError;
use crate::row_ext::RowExt;
use crate::pool::NucleusPool;

/// A single full-text search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsResult {
    pub doc_id: i64,
    pub score: f64,
}

/// A row returned by a ranked table-attached search.
///
/// Distinct from [`FtsResult`], which is a document-store hit: this `id` is a
/// real primary key in a real table, so it can be joined against.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedRow {
    pub id: i64,
    pub score: f64,
}

/// Handle for full-text search operations.
pub struct FtsModel {
    pool: NucleusPool,
}

/// Reject anything that is not a plain SQL identifier.
///
/// Table and column names interpolate into DDL and into the select list,
/// where bind parameters are not permitted. Rejecting is the only safe
/// option — quoting a hostile identifier still lets it terminate the quote.
fn validate_identifier(ident: &str) -> Result<(), NucleusError> {
    if ident.is_empty() || ident.len() > 63 {
        return Err(NucleusError::Serde(format!(
            "invalid SQL identifier {ident:?}: must be 1 to 63 characters"
        )));
    }
    let mut chars = ident.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(NucleusError::Serde(format!(
            "invalid SQL identifier {ident:?}: must start with a letter or underscore"
        )));
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(NucleusError::Serde(format!(
            "invalid SQL identifier {ident:?}: letters, digits, and underscores only"
        )));
    }
    Ok(())
}

impl FtsModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    // -----------------------------------------------------------------
    // Table-attached index
    // -----------------------------------------------------------------

    /// Create a full-text index on a table column.
    ///
    /// The table needs an integer `PRIMARY KEY`. Without one the server
    /// refuses, because index maintenance keys documents on it so that
    /// deleting a row does not silently re-point every document after it.
    ///
    /// Identifiers are validated rather than escaped: they interpolate into
    /// DDL, where a bind parameter is not allowed, so anything that is not a
    /// plain identifier is rejected outright instead of quoted and hoped for.
    pub async fn create_index(
        &self,
        index_name: &str,
        table: &str,
        column: &str,
    ) -> Result<(), NucleusError> {
        for ident in [index_name, table, column] {
            validate_identifier(ident)?;
        }
        let conn = self.pool.get().await?;
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {index_name} ON {table} USING FTS ({column})"
        );
        conn.client()
            .execute(&sql, &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    /// Drop a full-text index.
    pub async fn drop_index(&self, index_name: &str) -> Result<(), NucleusError> {
        validate_identifier(index_name)?;
        let conn = self.pool.get().await?;
        conn.client()
            .execute(&format!("DROP INDEX IF EXISTS {index_name}"), &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    /// Test whether a text value matches a query, using `@@`.
    ///
    /// Defined row-locally, so it is correct with or without an index on the
    /// column — the index only makes it faster. Matching is on stemmed terms
    /// with stopwords removed, and every query term must be present.
    pub async fn matches(&self, text: &str, query: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT $1::text @@ $2::text", &[&text, &query])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<bool>(0)?)
    }

    /// Score a text value against a query with BM25.
    ///
    /// Requires an FTS index on the column: the corpus-wide statistics BM25
    /// needs — document count, average length, per-term document frequency —
    /// are what the index supplies. Everything else is derivable from the
    /// row's own text, which is why this is an ordinary scalar function
    /// rather than something plumbed through the executor.
    pub async fn bm25(
        &self,
        table: &str,
        column: &str,
        query: &str,
        limit: i64,
    ) -> Result<Vec<RankedRow>, NucleusError> {
        validate_identifier(table)?;
        validate_identifier(column)?;

        let conn = self.pool.get().await?;
        let sql = format!(
            "SELECT id, BM25({column}, $1) AS score
               FROM {table}
              WHERE {column} @@ $1
              ORDER BY score DESC
              LIMIT $2"
        );
        let rows = conn
            .client()
            .query(&sql, &[&query, &limit])
            .await
            .map_err(NucleusError::Query)?;

        rows.iter()
            .map(|r| {
                Ok(RankedRow {
                    id: r.get_ck::<i64>(0)?,
                    score: r.get_ck::<f64>(1)?,
                })
            })
            .collect()
    }

    // -----------------------------------------------------------------
    // Document store
    // -----------------------------------------------------------------

    /// Index a document's text for full-text search.
    pub async fn index(&self, doc_id: i64, text: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT FTS_INDEX($1, $2)", &[&doc_id, &text])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<bool>(0)?)
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
        Ok(row.get_ck::<bool>(0)?)
    }

    /// Return the number of indexed documents.
    pub async fn doc_count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT FTS_DOC_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<i64>(0)?)
    }

    /// Return the number of indexed terms.
    pub async fn term_count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT FTS_TERM_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<i64>(0)?)
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
        let result = FtsResult {
            doc_id: 10,
            score: 1.0,
        };
        let cloned = result.clone();
        assert_eq!(cloned.doc_id, 10);
        assert_eq!(cloned.score, 1.0);
    }

    #[test]
    fn fts_result_debug() {
        let result = FtsResult {
            doc_id: 1,
            score: 0.5,
        };
        let dbg = format!("{:?}", result);
        assert!(dbg.contains("FtsResult"));
        assert!(dbg.contains("doc_id"));
    }

    #[test]
    fn identifiers_that_could_escape_a_query_are_rejected() {
        // These interpolate into DDL and into a select list, where a bind
        // parameter is not allowed. Quoting a hostile identifier still lets
        // it terminate the quote, so the only safe answer is refusal.
        for hostile in [
            "users; DROP TABLE users",
            "body\" , (SELECT 1) AS x --",
            "a'b",
            "a b",
            "",
            "1leading_digit",
            "-dash",
        ] {
            assert!(
                validate_identifier(hostile).is_err(),
                "identifier {hostile:?} should have been rejected"
            );
        }
    }

    #[test]
    fn ordinary_identifiers_are_accepted() {
        for ok in ["body", "articles", "_private", "col_1", "A1"] {
            assert!(
                validate_identifier(ok).is_ok(),
                "identifier {ok:?} should have been accepted"
            );
        }
    }

    #[test]
    fn identifier_length_is_bounded() {
        assert!(validate_identifier(&"a".repeat(63)).is_ok());
        assert!(validate_identifier(&"a".repeat(64)).is_err());
    }

    #[test]
    fn ranked_row_serialize_deserialize() {
        let row = RankedRow {
            id: 42,
            score: 0.6407,
        };
        let json = serde_json::to_string(&row).unwrap();
        let back: RankedRow = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, 42);
        assert!((back.score - 0.6407).abs() < f64::EPSILON);
    }

    #[test]
    fn fts_result_zero_score() {
        let result = FtsResult {
            doc_id: 0,
            score: 0.0,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: FtsResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.doc_id, 0);
        assert_eq!(deserialized.score, 0.0);
    }
}
