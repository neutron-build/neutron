//! Time-series model — TS_INSERT, TS_LAST, TS_COUNT, TS_RANGE_COUNT,
//! TS_RANGE_AVG, TS_RETENTION, TIME_BUCKET.

use crate::error::NucleusError;
use crate::row_ext::RowExt;
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
        Ok(row.get_ck::<Option<f64>>(0)?)
    }

    /// Return the raw data points stored in `[start_ms, end_ms]`.
    ///
    /// There was no method for this at all, and the reason is worth recording:
    /// raw point retrieval had no SQL surface, so Python synthesised it from
    /// sixty bucketed `TS_RANGE_AVG` calls, Go refused with "not supported by
    /// the engine", and TypeScript threw — three answers to one question.
    /// `TS_RANGE` now returns the points and every SDK uses it. Use
    /// [`Self::aggregate`] for bucketed averages; that is a different question.
    pub async fn range(
        &self,
        series: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<(i64, f64)>, NucleusError> {
        if end_ms <= start_ms {
            return Ok(Vec::new());
        }
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT TS_RANGE($1, $2, $3)", &[&series, &start_ms, &end_ms])
            .await
            .map_err(NucleusError::Query)?;
        let raw = row.get_ck::<String>(0)?;
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        #[derive(serde::Deserialize)]
        struct Point {
            t: i64,
            v: f64,
        }
        let points: Vec<Point> =
            serde_json::from_str(&raw).map_err(|e| NucleusError::Serde(e.to_string()))?;
        Ok(points.into_iter().map(|p| (p.t, p.v)).collect())
    }

    /// Return the total number of data points in a series.
    pub async fn count(&self, series: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT TS_COUNT($1)", &[&series])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<i64>(0)?)
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
        Ok(row.get_ck::<i64>(0)?)
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
        Ok(row.get_ck::<Option<f64>>(0)?)
    }

    /// Set the global time-series retention policy.
    ///
    /// `TS_RETENTION` takes a single argument: the maximum data-point age in
    /// milliseconds, applied across all series. The engine returns the text
    /// `'OK'`.
    pub async fn retention(&self, max_age_ms: i64) -> Result<(), NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query_one("SELECT TS_RETENTION($1)", &[&max_age_ms])
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    /// Truncate a timestamp to a bucket boundary.
    ///
    /// Intervals: `"second"`, `"minute"`, `"hour"`, `"day"`, `"week"`, `"month"`.
    /// Truncate a timestamp down to its `bucket_ms`-sized bucket.
    ///
    /// Takes the bucket size in MILLISECONDS. It previously took an interval
    /// name (`&str`) — "minute", "hour" — and the engine's `TIME_BUCKET` has
    /// always taken `(bucket_millis, ts)`, both `INT8`, so every call bound a
    /// text value where an integer was required and the function had never
    /// once worked. This is the same defect that was found and fixed in the
    /// Python client (L1); nothing can depend on the old signature, because
    /// nothing using it ever succeeded.
    pub async fn time_bucket(
        &self,
        bucket_ms: i64,
        timestamp_ms: i64,
    ) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT TIME_BUCKET($1, $2)", &[&bucket_ms, &timestamp_ms])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get_ck::<i64>(0)?)
    }

    /// Aggregate a series into fixed windows across a range.
    ///
    /// Returns one `(bucket_start_ms, value)` per `window_ms`-sized bucket
    /// between `start_ms` and `end_ms`, skipping buckets with no data.
    ///
    /// `func` is [`AggregateFn::Avg`] or [`AggregateFn::Count`]: the engine
    /// ships `TS_RANGE_AVG` and `TS_RANGE_COUNT` and nothing else, so sum, min,
    /// max, first and last are not offered rather than silently averaged.
    ///
    /// Buckets are aligned to `window_ms`, not to a calendar unit. Aligning a
    /// five-minute window to an hour boundary produces buckets that do not line
    /// up with the window the caller asked for, which is a wrong answer rather
    /// than an error.
    pub async fn aggregate(
        &self,
        series: &str,
        start_ms: i64,
        end_ms: i64,
        window_ms: i64,
        func: AggregateFn,
    ) -> Result<Vec<(i64, f64)>, NucleusError> {
        if window_ms <= 0 || end_ms <= start_ms {
            return Ok(Vec::new());
        }

        let mut bucket_start = self.time_bucket(window_ms, start_ms).await?;
        let mut out = Vec::new();

        while bucket_start < end_ms {
            let effective_end = (bucket_start + window_ms).min(end_ms);
            let conn = self.pool.get().await?;
            let sql = match func {
                AggregateFn::Avg => "SELECT TS_RANGE_AVG($1, $2, $3)",
                AggregateFn::Count => "SELECT TS_RANGE_COUNT($1, $2, $3)",
            };
            let row = conn
                .client()
                .query_one(sql, &[&series, &bucket_start, &effective_end])
                .await
                .map_err(NucleusError::Query)?;

            let value: Option<f64> = match func {
                AggregateFn::Avg => row.get_ck::<Option<f64>>(0)?,
                AggregateFn::Count => row.get_ck::<Option<i64>>(0)?.map(|n| n as f64),
            };
            if let Some(v) = value {
                out.push((bucket_start, v));
            }
            bucket_start += window_ms;
        }

        Ok(out)
    }
}

/// The aggregate functions the engine actually implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFn {
    Avg,
    Count,
}
