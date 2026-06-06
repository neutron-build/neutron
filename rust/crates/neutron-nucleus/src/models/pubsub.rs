//! PubSub model — PUBSUB_PUBLISH, PUBSUB_CHANNELS, PUBSUB_SUBSCRIBERS.

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Handle for PubSub operations.
pub struct PubSubModel {
    pool: NucleusPool,
}

impl PubSubModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Publish a message on a channel. Returns the number of subscribers reached.
    pub async fn publish(&self, channel: &str, message: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT PUBSUB_PUBLISH($1, $2)", &[&channel, &message])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Return active PubSub channels matching an optional pattern.
    pub async fn channels(&self, pattern: Option<&str>) -> Result<Vec<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = if let Some(pat) = pattern {
            conn.client()
                .query_one("SELECT PUBSUB_CHANNELS($1)", &[&pat])
                .await
                .map_err(NucleusError::Query)?
        } else {
            conn.client()
                .query_one("SELECT PUBSUB_CHANNELS()", &[])
                .await
                .map_err(NucleusError::Query)?
        };
        let raw: String = row.get(0);
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        Ok(raw.split(',').map(|s| s.trim().to_string()).collect())
    }

    /// Return the number of subscribers on a channel.
    pub async fn subscribers(&self, channel: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT PUBSUB_SUBSCRIBERS($1)", &[&channel])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }
}
