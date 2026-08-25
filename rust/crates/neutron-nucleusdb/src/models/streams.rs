//! Streams model (append-only logs) — STREAM_XADD, STREAM_XLEN, STREAM_XRANGE,
//! STREAM_XREAD, STREAM_XGROUP_CREATE, STREAM_XREADGROUP, STREAM_XACK.

use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

use crate::error::NucleusError;
use crate::pool::NucleusPool;
use crate::row_ext::RowExt;

/// A single entry in a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, serde_json::Value>,
}

/// Handle for stream operations.
pub struct StreamModel {
    pool: NucleusPool,
}

/// Parse a STREAM_XRANGE / STREAM_XREAD payload. The engine returns an empty
/// string (not `[]`) when the stream does not exist — treat that as no entries.
fn parse_entries(raw: &str) -> Result<Vec<StreamEntry>, NucleusError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(raw).map_err(|e| NucleusError::Serde(e.to_string()))
}

/// Parse a STREAM_XREADGROUP payload. Unlike [`parse_entries`], an empty
/// string is an error: since Nucleus v0.1.8 a missing group answers NOGROUP
/// as a statement error (surfaced as [`NucleusError::Query`] by the caller)
/// and an empty delivery is `"[]"`, so `""` is a contract violation and must
/// not read as "caught up".
fn parse_group_entries(raw: &str) -> Result<Vec<StreamEntry>, NucleusError> {
    serde_json::from_str(raw).map_err(|e| NucleusError::Serde(e.to_string()))
}

impl StreamModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Append an entry to a stream. Returns the generated entry ID.
    ///
    /// Fields are passed as key-value pairs that become the `STREAM_XADD` arguments.
    pub async fn xadd(
        &self,
        stream: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, NucleusError> {
        // Build: SELECT STREAM_XADD($1, $2, $3, $4, $5, ...)
        let mut params: Vec<String> = vec!["$1".to_string()];
        for (i, _) in fields.iter().enumerate() {
            params.push(format!("${}", i * 2 + 2));
            params.push(format!("${}", i * 2 + 3));
        }
        let sql = format!("SELECT STREAM_XADD({})", params.join(", "));

        let conn = self.pool.get().await?;
        let mut query_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
        query_params.push(&stream);
        for (k, v) in fields {
            query_params.push(k);
            query_params.push(v);
        }
        let row = conn
            .client()
            .query_one(&sql, &query_params)
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<String>(0)
    }

    /// Return the number of entries in a stream.
    pub async fn xlen(&self, stream: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT STREAM_XLEN($1)", &[&stream])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<i64>(0)
    }

    /// Return entries in a stream between start and end timestamps (inclusive).
    pub async fn xrange(
        &self,
        stream: &str,
        start_ms: i64,
        end_ms: i64,
        count: i64,
    ) -> Result<Vec<StreamEntry>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT STREAM_XRANGE($1, $2, $3, $4)",
                &[&stream, &start_ms, &end_ms, &count],
            )
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        parse_entries(&raw)
    }

    /// Read new entries from a stream after the given ID.
    pub async fn xread(
        &self,
        stream: &str,
        last_id_ms: i64,
        count: i64,
    ) -> Result<Vec<StreamEntry>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT STREAM_XREAD($1, $2, $3)",
                &[&stream, &last_id_ms, &count],
            )
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        parse_entries(&raw)
    }

    /// Create a consumer group on a stream.
    pub async fn xgroup_create(
        &self,
        stream: &str,
        group: &str,
        start_id: i64,
    ) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT STREAM_XGROUP_CREATE($1, $2, $3)",
                &[&stream, &group, &start_id],
            )
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<bool>(0)
    }

    /// Read entries from a consumer group.
    ///
    /// A missing group (or stream) answers NOGROUP as a statement error since
    /// Nucleus v0.1.8 and surfaces as [`NucleusError::Query`]; an empty
    /// delivery is a `"[]"` payload.
    pub async fn xreadgroup(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: i64,
    ) -> Result<Vec<StreamEntry>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT STREAM_XREADGROUP($1, $2, $3, $4)",
                &[&stream, &group, &consumer, &count],
            )
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        parse_group_entries(&raw)
    }

    /// Acknowledge processing of a stream entry in a consumer group.
    /// Returns the number of entries acknowledged.
    /// Acknowledge a processed entry by the id [`Self::xadd`] returned.
    ///
    /// `entry_id` is the `"<ms>-<seq>"` string. This took `id_ms` and `id_seq`
    /// as separate integers, so the two ends of the same API did not compose —
    /// every caller split `xadd`'s return value itself, and the consumer-group
    /// conformance case was xfail in all five SDKs for that reason.
    pub async fn xack(
        &self,
        stream: &str,
        group: &str,
        entry_id: &str,
    ) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT STREAM_XACK($1, $2, $3)",
                &[&stream, &group, &entry_id],
            )
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<i64>(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_entry_serialize_deserialize() {
        let mut fields = HashMap::new();
        fields.insert("temperature".to_string(), serde_json::json!(23.5));
        fields.insert("humidity".to_string(), serde_json::json!(45));

        let entry = StreamEntry {
            id: "1234567890-0".to_string(),
            fields,
        };

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: StreamEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "1234567890-0");
        assert_eq!(deserialized.fields["temperature"], 23.5);
        assert_eq!(deserialized.fields["humidity"], 45);
    }

    #[test]
    fn stream_entry_empty_fields() {
        let entry = StreamEntry {
            id: "0-0".to_string(),
            fields: HashMap::new(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: StreamEntry = serde_json::from_str(&json).unwrap();
        assert!(deserialized.fields.is_empty());
    }

    #[test]
    fn stream_entry_vec_deserialize() {
        let json = r#"[
            {"id": "100-0", "fields": {"key": "a"}},
            {"id": "200-0", "fields": {"key": "b"}}
        ]"#;
        let entries: Vec<StreamEntry> = serde_json::from_str(json).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "100-0");
        assert_eq!(entries[1].fields["key"], "b");
    }

    #[test]
    fn stream_entry_clone() {
        let mut fields = HashMap::new();
        fields.insert("x".to_string(), serde_json::json!(1));
        let entry = StreamEntry {
            id: "1-0".to_string(),
            fields,
        };
        let cloned = entry.clone();
        assert_eq!(cloned.id, "1-0");
        assert_eq!(cloned.fields["x"], 1);
    }

    #[test]
    fn parse_entries_empty_string_is_empty() {
        // Engine returns "" (not "[]") for a missing stream on XRANGE/XREAD.
        assert!(parse_entries("").unwrap().is_empty());
    }

    #[test]
    fn parse_entries_empty_array() {
        assert!(parse_entries("[]").unwrap().is_empty());
    }

    #[test]
    fn parse_group_entries_empty_delivery_is_empty() {
        assert!(parse_group_entries("[]").unwrap().is_empty());
    }

    #[test]
    fn parse_group_entries_empty_string_is_error() {
        // Since Nucleus v0.1.8, "" is not a possible XREADGROUP result: a
        // missing group answers NOGROUP (a query error, which never reaches
        // the parser) and an empty delivery is "[]". An empty payload is a
        // contract violation, not an empty batch.
        assert!(parse_group_entries("").is_err());
    }

    #[test]
    fn stream_entry_debug() {
        let entry = StreamEntry {
            id: "test".to_string(),
            fields: HashMap::new(),
        };
        let dbg = format!("{:?}", entry);
        assert!(dbg.contains("StreamEntry"));
    }
}
