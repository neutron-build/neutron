//! Change Data Capture model — CDC_READ, CDC_COUNT, CDC_TABLE_READ.

use serde::{Deserialize, Serialize};

use crate::error::NucleusError;
use crate::pool::NucleusPool;
use crate::row_ext::RowExt;

/// A single change-data-capture log entry as emitted by the engine.
///
/// `read` and `table_read` returned the engine's raw JSON string and there was
/// no event type at all, so every caller wrote its own deserialization — and
/// the cross-SDK conformance case asserting a list passed against a non-empty
/// string, because a non-empty string is truthy. Python and TypeScript both
/// returned parsed events; Go and Rust did not.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Monotonic sequence number of the change.
    #[serde(default)]
    pub seq: i64,
    /// Table the change applies to.
    #[serde(default)]
    pub table: String,
    /// Kind of change: `INSERT`, `UPDATE` or `DELETE`.
    #[serde(default)]
    pub change: String,
    /// Timestamp of the change, in epoch milliseconds.
    #[serde(default)]
    pub ts: i64,
}

/// Decode the engine's event array.
///
/// An empty result is an empty vector, never an error: "no changes since that
/// sequence" is the common case, not a failure. A malformed payload IS an error
/// rather than an empty vector, because silently reporting "no changes" when
/// the engine said something unparseable is the shape of bug this model exists
/// to detect.
fn parse_events(raw: &str) -> Result<Vec<CdcEvent>, NucleusError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(raw).map_err(|e| NucleusError::Serde(e.to_string()))
}

/// Handle for CDC (Change Data Capture) operations.
pub struct CdcModel {
    pool: NucleusPool,
}

impl CdcModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Read up to `limit` CDC events after the given sequence number.
    pub async fn read(
        &self,
        after_sequence: i64,
        limit: i64,
    ) -> Result<Vec<CdcEvent>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT CDC_READ($1, $2)", &[&after_sequence, &limit])
            .await
            .map_err(NucleusError::Query)?;
        parse_events(&row.get_ck::<String>(0)?)
    }

    /// Return the total number of CDC events.
    pub async fn count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT CDC_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<i64>(0)
    }

    /// Read up to `limit` CDC events for a specific table after the given
    /// sequence number.
    pub async fn table_read(
        &self,
        table: &str,
        after_sequence: i64,
        limit: i64,
    ) -> Result<Vec<CdcEvent>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT CDC_TABLE_READ($1, $2, $3)",
                &[&table, &after_sequence, &limit],
            )
            .await
            .map_err(NucleusError::Query)?;
        parse_events(&row.get_ck::<String>(0)?)
    }
}
