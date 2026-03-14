//! Time-series model — TS_INSERT, TS_LAST, TS_COUNT, TS_RANGE_COUNT,
//! TS_RANGE_AVG, TS_RETENTION, TS_MATCH, TIME_BUCKET.

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Handle for time-series operations.
pub struct TimeSeriesModel {
    pool: NucleusPool,
}

impl TimeSeriesModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Insert a data point into a time series.
    pub async fn insert(
        &self,
        series: &str,
        timestamp_ms: i64,
        value: f64,
    ) -> Result<(), NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .execute(
                "SELECT TS_INSERT($1, $2, $3)",
                &[&series, &timestamp_ms, &value],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    /// Return the most recent value for a series.
    pub async fn last(&self, series: &str) -> Result<Option<f64>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT TS_LAST($1)", &[&series])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, Option<f64>>(0))
    }

    /// Return the total number of data points in a series.
    pub async fn count(&self, series: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT TS_COUNT($1)", &[&series])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Return the number of data points in a time range.
    pub async fn range_count(
        &self,
        series: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT TS_RANGE_COUNT($1, $2, $3)",
                &[&series, &start_ms, &end_ms],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Return the average value of data points in a time range.
    pub async fn range_avg(
        &self,
        series: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Option<f64>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT TS_RANGE_AVG($1, $2, $3)",
                &[&series, &start_ms, &end_ms],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, Option<f64>>(0))
    }

    /// Set the data retention period for a series.
    pub async fn retention(&self, series: &str, days: i64) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT TS_RETENTION($1, $2)", &[&series, &days])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Find series names matching a pattern.
    pub async fn match_series(
        &self,
        series: &str,
        pattern: &str,
    ) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT TS_MATCH($1, $2)", &[&series, &pattern])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, String>(0))
    }

    /// Truncate a timestamp to a bucket boundary.
    ///
    /// Intervals: `"second"`, `"minute"`, `"hour"`, `"day"`, `"week"`, `"month"`.
    pub async fn time_bucket(
        &self,
        interval: &str,
        timestamp_ms: i64,
    ) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT TIME_BUCKET($1, $2)", &[&interval, &timestamp_ms])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }
}
