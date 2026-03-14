//! Vector similarity search model — typed wrappers for VECTOR(), VECTOR_DISTANCE(),
//! VECTOR_DIMS(), COSINE_DISTANCE(), INNER_PRODUCT().

use serde_json;

use crate::error::NucleusError;
use crate::models::is_valid_identifier;
use crate::pool::NucleusPool;

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// L2 (Euclidean) distance.
    L2,
    /// Cosine distance.
    Cosine,
    /// Inner product (dot product).
    InnerProduct,
}

impl DistanceMetric {
    fn as_str(&self) -> &'static str {
        match self {
            DistanceMetric::L2 => "l2",
            DistanceMetric::Cosine => "cosine",
            DistanceMetric::InnerProduct => "inner",
        }
    }
}

/// A single vector search result.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    /// Row ID.
    pub id: String,
    /// Associated metadata (JSONB column).
    pub metadata: serde_json::Value,
    /// Distance from the query vector.
    pub distance: f64,
}

/// Handle for vector similarity search operations.
pub struct VectorModel {
    pool: NucleusPool,
}

impl VectorModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Compute the distance between two vectors.
    pub async fn distance(
        &self,
        a: &[f32],
        b: &[f32],
        metric: DistanceMetric,
    ) -> Result<f64, NucleusError> {
        let a_json = serde_json::to_string(a).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let b_json = serde_json::to_string(b).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let metric_str = metric.as_str().to_string();
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT VECTOR_DISTANCE(VECTOR($1), VECTOR($2), $3)",
                &[&a_json, &b_json, &metric_str],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, f64>(0))
    }

    /// Return the dimensionality of a vector.
    pub async fn dims(&self, vec: &[f32]) -> Result<i64, NucleusError> {
        let vec_json =
            serde_json::to_string(vec).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT VECTOR_DIMS(VECTOR($1))", &[&vec_json])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Create a vector collection (table with id, embedding, metadata columns).
    pub async fn create_collection(
        &self,
        name: &str,
        dimension: i32,
        metric: DistanceMetric,
    ) -> Result<(), NucleusError> {
        if !is_valid_identifier(name) {
            return Err(NucleusError::InvalidIdentifier(name.to_string()));
        }
        let conn = self.pool.get().await?;
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, embedding VECTOR({}), metadata JSONB DEFAULT '{{}}')",
            name, dimension
        );
        conn.client()
            .execute(&create_sql, &[])
            .await
            .map_err(NucleusError::Query)?;

        let index_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_embedding ON {} USING VECTOR (embedding) WITH (metric = '{}')",
            name, name, metric.as_str()
        );
        conn.client()
            .execute(&index_sql, &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    /// Insert a vector with metadata into a collection.
    pub async fn insert(
        &self,
        collection: &str,
        id: &str,
        vector: &[f32],
        metadata: &serde_json::Value,
    ) -> Result<(), NucleusError> {
        if !is_valid_identifier(collection) {
            return Err(NucleusError::InvalidIdentifier(collection.to_string()));
        }
        let vec_json =
            serde_json::to_string(vector).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let meta_json =
            serde_json::to_string(metadata).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let conn = self.pool.get().await?;
        let sql = format!(
            "INSERT INTO {} (id, embedding, metadata) VALUES ($1, VECTOR($2), $3)",
            collection
        );
        conn.client()
            .execute(&sql, &[&id, &vec_json, &meta_json])
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    /// Search for nearest vectors in a collection.
    pub async fn search(
        &self,
        collection: &str,
        query: &[f32],
        metric: DistanceMetric,
        limit: i64,
    ) -> Result<Vec<VectorSearchResult>, NucleusError> {
        if !is_valid_identifier(collection) {
            return Err(NucleusError::InvalidIdentifier(collection.to_string()));
        }
        let vec_json =
            serde_json::to_string(query).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let metric_str = metric.as_str().to_string();
        let conn = self.pool.get().await?;
        let sql = format!(
            "SELECT id, metadata, VECTOR_DISTANCE(embedding, VECTOR($1), $2) AS distance FROM {} ORDER BY distance LIMIT $3",
            collection
        );
        let rows = conn
            .client()
            .query(&sql, &[&vec_json, &metric_str, &limit])
            .await
            .map_err(NucleusError::Query)?;

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            let id: String = row.get(0);
            let meta_raw: Option<serde_json::Value> = row.get(1);
            let dist: f64 = row.get(2);
            results.push(VectorSearchResult {
                id,
                metadata: meta_raw.unwrap_or(serde_json::Value::Null),
                distance: dist,
            });
        }
        Ok(results)
    }

    /// Delete a vector by ID from a collection.
    pub async fn delete(&self, collection: &str, id: &str) -> Result<bool, NucleusError> {
        if !is_valid_identifier(collection) {
            return Err(NucleusError::InvalidIdentifier(collection.to_string()));
        }
        let conn = self.pool.get().await?;
        let sql = format!("DELETE FROM {} WHERE id = $1", collection);
        let n = conn
            .client()
            .execute(&sql, &[&id])
            .await
            .map_err(NucleusError::Query)?;
        Ok(n > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_metric_l2_as_str() {
        assert_eq!(DistanceMetric::L2.as_str(), "l2");
    }

    #[test]
    fn distance_metric_cosine_as_str() {
        assert_eq!(DistanceMetric::Cosine.as_str(), "cosine");
    }

    #[test]
    fn distance_metric_inner_product_as_str() {
        assert_eq!(DistanceMetric::InnerProduct.as_str(), "inner");
    }

    #[test]
    fn distance_metric_equality() {
        assert_eq!(DistanceMetric::L2, DistanceMetric::L2);
        assert_ne!(DistanceMetric::L2, DistanceMetric::Cosine);
        assert_ne!(DistanceMetric::Cosine, DistanceMetric::InnerProduct);
    }

    #[test]
    fn distance_metric_clone() {
        let m = DistanceMetric::Cosine;
        let m2 = m;
        assert_eq!(m, m2);
    }

    #[test]
    fn distance_metric_debug() {
        let dbg = format!("{:?}", DistanceMetric::InnerProduct);
        assert_eq!(dbg, "InnerProduct");
    }

    #[test]
    fn vector_search_result_fields() {
        let result = VectorSearchResult {
            id: "vec-42".to_string(),
            metadata: serde_json::json!({"label": "cat"}),
            distance: 0.123,
        };
        assert_eq!(result.id, "vec-42");
        assert_eq!(result.metadata["label"], "cat");
        assert!((result.distance - 0.123).abs() < f64::EPSILON);
    }

    #[test]
    fn vector_search_result_clone() {
        let result = VectorSearchResult {
            id: "a".to_string(),
            metadata: serde_json::Value::Null,
            distance: 0.0,
        };
        let cloned = result.clone();
        assert_eq!(cloned.id, "a");
    }

    #[test]
    fn invalid_collection_name_rejected() {
        // The create_collection, insert, search, delete methods all check
        // is_valid_identifier. We can't call them without a pool, but we
        // can verify the identifier validation itself.
        assert!(!is_valid_identifier("table-name")); // hyphens not allowed
        assert!(!is_valid_identifier("123abc"));     // starts with digit
        assert!(!is_valid_identifier(""));           // empty
        assert!(is_valid_identifier("embeddings"));
        assert!(is_valid_identifier("my_vectors"));
    }
}
