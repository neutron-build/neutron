//! Unified multi-model Nucleus client.
//!
//! Wraps the existing [`NucleusPool`](crate::NucleusPool) and provides typed
//! access to all 14 Nucleus data models via `.kv()`, `.vector()`, `.sql()`, etc.
//!
//! Auto-detects Nucleus vs plain PostgreSQL on construction via `SELECT VERSION()`.
//!
//! ```rust,ignore
//! use neutron_nucleus::{NucleusClient, NucleusConfig};
//!
//! let client = NucleusClient::connect(NucleusConfig::default()).await?;
//!
//! // Use any model
//! client.kv().set("key", "value", None).await?;
//! let val = client.kv().get("key").await?;
//!
//! // Check if connected to Nucleus
//! if client.is_nucleus() {
//!     let count = client.graph().node_count().await?;
//! }
//! ```

use regex::Regex;
use std::sync::OnceLock;

use crate::error::NucleusError;
use crate::models::{
    blob::BlobModel,
    cdc::CdcModel,
    columnar::ColumnarModel,
    datalog::DatalogModel,
    document::DocumentModel,
    fts::FtsModel,
    geo::GeoModel,
    graph::GraphModel,
    kv::KvModel,
    pubsub::PubSubModel,
    sql::SqlModel,
    streams::StreamModel,
    timeseries::TimeSeriesModel,
    vector::VectorModel,
};
use crate::pool::{NucleusConfig, NucleusPool};

/// Detected database capabilities.
#[derive(Debug, Clone)]
pub struct Features {
    /// Whether the connected database is Nucleus (vs plain PostgreSQL).
    pub is_nucleus: bool,
    /// The raw version string from `SELECT VERSION()`.
    pub version: String,
    /// Extracted Nucleus version (e.g. "1.2.3"), if any.
    pub nucleus_version: Option<String>,
}

/// Unified client providing typed access to all 14 Nucleus data models.
///
/// Cheaply cloneable (wraps an `Arc` pool internally). Register it as
/// application state and access model handles as needed.
#[derive(Clone)]
pub struct NucleusClient {
    pool: NucleusPool,
    features: Features,
}

impl NucleusClient {
    /// Connect to the database, creating a pool and auto-detecting features.
    pub async fn connect(config: NucleusConfig) -> Result<Self, NucleusError> {
        let pool = NucleusPool::new(config);
        let features = detect_features(&pool).await?;
        Ok(Self { pool, features })
    }

    /// Create a client from an existing pool. Runs feature detection.
    pub async fn from_pool(pool: NucleusPool) -> Result<Self, NucleusError> {
        let features = detect_features(&pool).await?;
        Ok(Self { pool, features })
    }

    /// Return the detected database features.
    pub fn features(&self) -> &Features {
        &self.features
    }

    /// Return `true` if the connected database is Nucleus.
    pub fn is_nucleus(&self) -> bool {
        self.features.is_nucleus
    }

    /// Return the underlying connection pool.
    pub fn pool(&self) -> &NucleusPool {
        &self.pool
    }

    /// Verify the database connection.
    pub async fn ping(&self) -> Result<(), NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .execute("SELECT 1", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    // --- Model accessors ---

    /// Standard SQL operations.
    pub fn sql(&self) -> SqlModel {
        SqlModel::new(self.pool.clone())
    }

    /// Key-value store (Redis-compatible).
    pub fn kv(&self) -> KvModel {
        KvModel::new(self.pool.clone())
    }

    /// Vector similarity search.
    pub fn vector(&self) -> VectorModel {
        VectorModel::new(self.pool.clone())
    }

    /// Time-series data.
    pub fn timeseries(&self) -> TimeSeriesModel {
        TimeSeriesModel::new(self.pool.clone())
    }

    /// Document/JSON store.
    pub fn document(&self) -> DocumentModel {
        DocumentModel::new(self.pool.clone())
    }

    /// Graph database.
    pub fn graph(&self) -> GraphModel {
        GraphModel::new(self.pool.clone())
    }

    /// Full-text search.
    pub fn fts(&self) -> FtsModel {
        FtsModel::new(self.pool.clone())
    }

    /// Geospatial operations.
    pub fn geo(&self) -> GeoModel {
        GeoModel::new(self.pool.clone())
    }

    /// Binary object storage.
    pub fn blob(&self) -> BlobModel {
        BlobModel::new(self.pool.clone())
    }

    /// Append-only streams (Redis Streams-compatible).
    pub fn streams(&self) -> StreamModel {
        StreamModel::new(self.pool.clone())
    }

    /// Publish/subscribe messaging.
    pub fn pubsub(&self) -> PubSubModel {
        PubSubModel::new(self.pool.clone())
    }

    /// Columnar analytics.
    pub fn columnar(&self) -> ColumnarModel {
        ColumnarModel::new(self.pool.clone())
    }

    /// Datalog reasoning engine.
    pub fn datalog(&self) -> DatalogModel {
        DatalogModel::new(self.pool.clone())
    }

    /// Change Data Capture.
    pub fn cdc(&self) -> CdcModel {
        CdcModel::new(self.pool.clone())
    }

    /// Return an error if the connected database is not Nucleus.
    pub fn require_nucleus(&self, feature: &str) -> Result<(), NucleusError> {
        if !self.features.is_nucleus {
            return Err(NucleusError::NucleusRequired {
                feature: feature.to_string(),
            });
        }
        Ok(())
    }
}

/// Regex for extracting the Nucleus version from the VERSION() string.
fn nucleus_version_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"Nucleus\s+(\d+\.\d+\.\d+)").unwrap()
    })
}

/// Auto-detect whether the database is Nucleus by parsing `SELECT VERSION()`.
async fn detect_features(pool: &NucleusPool) -> Result<Features, NucleusError> {
    let conn = pool.get().await?;
    let row = conn
        .client()
        .query_one("SELECT VERSION()", &[])
        .await
        .map_err(NucleusError::Query)?;
    let version: String = row.get(0);

    let is_nucleus = version.contains("Nucleus");
    let nucleus_version = if is_nucleus {
        nucleus_version_re()
            .captures(&version)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
    } else {
        None
    };

    Ok(Features {
        is_nucleus,
        version,
        nucleus_version,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_nucleus_version() {
        let re = nucleus_version_re();

        let v1 = "PostgreSQL 16.0 (Nucleus 1.2.3 — The Definitive Database)";
        let caps = re.captures(v1).unwrap();
        assert_eq!(caps.get(1).unwrap().as_str(), "1.2.3");

        let v2 = "PostgreSQL 16.4";
        assert!(re.captures(v2).is_none());
    }

    #[test]
    fn parse_nucleus_version_various_formats() {
        let re = nucleus_version_re();

        // Multi-digit version
        let v = "Nucleus 12.34.567";
        let caps = re.captures(v).unwrap();
        assert_eq!(caps.get(1).unwrap().as_str(), "12.34.567");

        // Version at start of string
        let v2 = "Nucleus 0.1.0 alpha";
        let caps2 = re.captures(v2).unwrap();
        assert_eq!(caps2.get(1).unwrap().as_str(), "0.1.0");

        // Extra whitespace between Nucleus and version
        let v3 = "Nucleus  2.0.0";
        let caps3 = re.captures(v3).unwrap();
        assert_eq!(caps3.get(1).unwrap().as_str(), "2.0.0");
    }

    #[test]
    fn parse_no_nucleus_in_plain_pg() {
        let re = nucleus_version_re();
        assert!(re.captures("PostgreSQL 15.2").is_none());
        assert!(re.captures("MySQL 8.0").is_none());
        assert!(re.captures("").is_none());
    }

    #[test]
    fn features_plain_pg() {
        let f = Features {
            is_nucleus: false,
            version: "PostgreSQL 16.4".to_string(),
            nucleus_version: None,
        };
        assert!(!f.is_nucleus);
        assert!(f.nucleus_version.is_none());
        assert_eq!(f.version, "PostgreSQL 16.4");
    }

    #[test]
    fn features_nucleus_detected() {
        let f = Features {
            is_nucleus: true,
            version: "PostgreSQL 16.0 (Nucleus 1.0.0)".to_string(),
            nucleus_version: Some("1.0.0".to_string()),
        };
        assert!(f.is_nucleus);
        assert_eq!(f.nucleus_version.as_deref(), Some("1.0.0"));
    }

    #[test]
    fn features_clone() {
        let f = Features {
            is_nucleus: true,
            version: "test".to_string(),
            nucleus_version: Some("1.0.0".to_string()),
        };
        let f2 = f.clone();
        assert_eq!(f2.is_nucleus, f.is_nucleus);
        assert_eq!(f2.version, f.version);
        assert_eq!(f2.nucleus_version, f.nucleus_version);
    }

    #[test]
    fn features_debug() {
        let f = Features {
            is_nucleus: false,
            version: "PostgreSQL 16.4".to_string(),
            nucleus_version: None,
        };
        let dbg = format!("{:?}", f);
        assert!(dbg.contains("Features"));
        assert!(dbg.contains("is_nucleus"));
    }

    // --- Version detection logic (unit test the contains-based check) ---

    #[test]
    fn version_contains_nucleus() {
        let version = "PostgreSQL 16.0 (Nucleus 1.2.3)";
        assert!(version.contains("Nucleus"));
    }

    #[test]
    fn version_does_not_contain_nucleus() {
        let version = "PostgreSQL 16.4";
        assert!(!version.contains("Nucleus"));
    }
}
