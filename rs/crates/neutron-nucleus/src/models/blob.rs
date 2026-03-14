//! Blob storage model — BLOB_STORE, BLOB_GET, BLOB_DELETE, BLOB_META,
//! BLOB_TAG, BLOB_LIST, BLOB_COUNT, BLOB_DEDUP_RATIO.

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Metadata about a stored blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMeta {
    pub key: String,
    pub size: i64,
    pub content_type: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Handle for blob storage operations.
pub struct BlobModel {
    pool: NucleusPool,
}

impl BlobModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Store binary data as a hex-encoded blob.
    pub async fn store(
        &self,
        key: &str,
        data: &[u8],
        content_type: Option<&str>,
    ) -> Result<bool, NucleusError> {
        let hex_data = hex_encode(data);
        let conn = self.pool.get().await?;
        let row = if let Some(ct) = content_type {
            conn.client()
                .query_one("SELECT BLOB_STORE($1, $2, $3)", &[&key, &hex_data, &ct])
                .await
                .map_err(NucleusError::Query)?
        } else {
            conn.client()
                .query_one("SELECT BLOB_STORE($1, $2)", &[&key, &hex_data])
                .await
                .map_err(NucleusError::Query)?
        };
        Ok(row.get::<_, bool>(0))
    }

    /// Retrieve a blob as raw bytes. Returns `None` if not found.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT BLOB_GET($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        let raw: Option<String> = row.get(0);
        match raw {
            Some(hex) => {
                let bytes =
                    hex_decode(&hex).map_err(|e| NucleusError::Serde(format!("hex decode: {e}")))?;
                Ok(Some(bytes))
            }
            None => Ok(None),
        }
    }

    /// Delete a blob by key.
    pub async fn delete(&self, key: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT BLOB_DELETE($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return metadata for a blob.
    pub async fn meta(&self, key: &str) -> Result<Option<BlobMeta>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT BLOB_META($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        let raw: Option<String> = row.get(0);
        match raw {
            Some(s) => {
                let meta: BlobMeta =
                    serde_json::from_str(&s).map_err(|e| NucleusError::Serde(e.to_string()))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Set a metadata tag on a blob.
    pub async fn tag(
        &self,
        key: &str,
        tag_key: &str,
        tag_value: &str,
    ) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT BLOB_TAG($1, $2, $3)",
                &[&key, &tag_key, &tag_value],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// List blobs matching an optional prefix.
    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<BlobMeta>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = if let Some(pfx) = prefix {
            conn.client()
                .query_one("SELECT BLOB_LIST($1)", &[&pfx])
                .await
                .map_err(NucleusError::Query)?
        } else {
            conn.client()
                .query_one("SELECT BLOB_LIST()", &[])
                .await
                .map_err(NucleusError::Query)?
        };
        let raw: String = row.get(0);
        let metas: Vec<BlobMeta> =
            serde_json::from_str(&raw).map_err(|e| NucleusError::Serde(e.to_string()))?;
        Ok(metas)
    }

    /// Return the total number of stored blobs.
    pub async fn count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT BLOB_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Return the deduplication ratio.
    pub async fn dedup_ratio(&self) -> Result<f64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT BLOB_DEDUP_RATIO()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, f64>(0))
    }
}

// --- Hex helpers (avoid pulling in the `hex` crate) ---

fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for byte in data {
        s.push_str(&format!("{:02x}", byte));
    }
    s
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd hex string length".to_string());
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte =
            u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| format!("invalid hex: {e}"))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- hex_encode ---

    #[test]
    fn hex_encode_empty() {
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn hex_encode_single_byte() {
        assert_eq!(hex_encode(&[0x00]), "00");
        assert_eq!(hex_encode(&[0xff]), "ff");
        assert_eq!(hex_encode(&[0x0a]), "0a");
    }

    #[test]
    fn hex_encode_multiple_bytes() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn hex_encode_hello() {
        assert_eq!(hex_encode(b"hello"), "68656c6c6f");
    }

    #[test]
    fn hex_encode_all_zeros() {
        assert_eq!(hex_encode(&[0, 0, 0]), "000000");
    }

    // --- hex_decode ---

    #[test]
    fn hex_decode_empty() {
        assert_eq!(hex_decode("").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn hex_decode_single_byte() {
        assert_eq!(hex_decode("00").unwrap(), vec![0x00]);
        assert_eq!(hex_decode("ff").unwrap(), vec![0xff]);
        assert_eq!(hex_decode("FF").unwrap(), vec![0xff]);
    }

    #[test]
    fn hex_decode_deadbeef() {
        assert_eq!(hex_decode("deadbeef").unwrap(), vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn hex_decode_hello() {
        assert_eq!(hex_decode("68656c6c6f").unwrap(), b"hello".to_vec());
    }

    #[test]
    fn hex_decode_odd_length_error() {
        let err = hex_decode("abc").unwrap_err();
        assert!(err.contains("odd hex string length"));
    }

    #[test]
    fn hex_decode_invalid_chars_error() {
        let err = hex_decode("zzzz").unwrap_err();
        assert!(err.contains("invalid hex"));
    }

    // --- hex roundtrip ---

    #[test]
    fn hex_roundtrip() {
        let original = b"Nucleus database engine!";
        let encoded = hex_encode(original);
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn hex_roundtrip_all_byte_values() {
        let all_bytes: Vec<u8> = (0..=255).collect();
        let encoded = hex_encode(&all_bytes);
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(decoded, all_bytes);
    }

    // --- BlobMeta serde ---

    #[test]
    fn blob_meta_serialize_deserialize() {
        let meta = BlobMeta {
            key: "images/photo.png".to_string(),
            size: 1024,
            content_type: "image/png".to_string(),
            metadata: serde_json::json!({"width": 800, "height": 600}),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let deserialized: BlobMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.key, "images/photo.png");
        assert_eq!(deserialized.size, 1024);
        assert_eq!(deserialized.content_type, "image/png");
        assert_eq!(deserialized.metadata["width"], 800);
    }

    #[test]
    fn blob_meta_default_metadata() {
        // metadata field has #[serde(default)], so it can be missing from JSON
        let json = r#"{"key":"test","size":100,"content_type":"text/plain"}"#;
        let meta: BlobMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.key, "test");
        assert_eq!(meta.size, 100);
        assert!(meta.metadata.is_null());
    }

    #[test]
    fn blob_meta_vec_deserialize() {
        let json = r#"[
            {"key":"a","size":10,"content_type":"text/plain"},
            {"key":"b","size":20,"content_type":"image/png"}
        ]"#;
        let metas: Vec<BlobMeta> = serde_json::from_str(json).unwrap();
        assert_eq!(metas.len(), 2);
        assert_eq!(metas[0].key, "a");
        assert_eq!(metas[1].key, "b");
    }
}
