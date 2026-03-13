"""TimeSeries model — wraps Nucleus TS_* SQL functions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class TimeSeriesPoint(BaseModel):
    timestamp: datetime
    value: float
    tags: dict[str, str] = {}


class TimeSeriesModel:
    """Time-series data operations over Nucleus.

    Usage::

        await db.timeseries.write("cpu.usage", [
            TimeSeriesPoint(timestamp=now, value=72.5),
        ])
        points = await db.timeseries.query("cpu.usage", start, end)
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "TimeSeries")

    async def write(
        self, measurement: str, points: list[TimeSeriesPoint]
    ) -> None:
        """Insert time-series data points."""
        self._require()
        for point in points:
            ts_ms = int(point.timestamp.timestamp() * 1000)
            await self._exec.fetchval(
                "SELECT TS_INSERT($1, $2, $3)", measurement, ts_ms, point.value
            )

    async def last(self, measurement: str) -> float | None:
        """Get the latest value for a series."""
        self._require()
        return await self._exec.fetchval("SELECT TS_LAST($1)", measurement)

    async def count(self, measurement: str) -> int:
        """Count data points in a series."""
        self._require()
        return await self._exec.fetchval("SELECT TS_COUNT($1)", measurement)

    async def query(
        self,
        measurement: str,
        start: datetime,
        end: datetime,
        *,
        tags: dict[str, str] | None = None,
        buckets: int = 60,
    ) -> list[TimeSeriesPoint]:
        """Query data points in a time range.

        Returns one averaged data point per bucket (default 60 buckets).
        Each bucket spans an equal portion of [start, end].
        """
        self._require()
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        total_ms = end_ms - start_ms
        if total_ms <= 0:
            return []

        # Short-circuit if no data in range
        total_count = await self._exec.fetchval(
            "SELECT TS_RANGE_COUNT($1, $2, $3)", measurement, start_ms, end_ms
        )
        if not total_count:
            return []

        # Split range into equal buckets; return one averaged point per bucket
        bucket_ms = max(1, total_ms // buckets)
        points: list[TimeSeriesPoint] = []
        bucket_start = start_ms
        while bucket_start < end_ms:
            bucket_end = min(bucket_start + bucket_ms, end_ms)
            avg = await self._exec.fetchval(
                "SELECT TS_RANGE_AVG($1, $2, $3)",
                measurement,
                bucket_start,
                bucket_end,
            )
            if avg is not None:
                ts = datetime.fromtimestamp(bucket_start / 1000, tz=timezone.utc)
                points.append(TimeSeriesPoint(timestamp=ts, value=float(avg)))
            bucket_start = bucket_end

        return points

    async def aggregate(
        self,
        measurement: str,
        start: datetime,
        end: datetime,
        window: timedelta,
        fn: str = "avg",
    ) -> list[TimeSeriesPoint]:
        """Aggregate data points with time bucketing.

        Returns one data point per ``window``-sized bucket across the range.
        ``fn``: ``avg``, ``sum``, ``min``, ``max``, ``count``, ``first``, ``last``
        """
        self._require()
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        window_ms = int(window.total_seconds() * 1000)
        if window_ms <= 0 or end_ms <= start_ms:
            return []

        # Map fn to the correct Nucleus TS_RANGE_* SQL function
        fn_map = {
            "avg": "TS_RANGE_AVG",
            "count": "TS_RANGE_COUNT",
        }
        sql_fn = fn_map.get(fn.lower())
        if sql_fn is None:
            raise ValueError(
                f"Unsupported aggregation function: {fn}. "
                f"Supported: {', '.join(fn_map)}"
            )

        # Align start to TIME_BUCKET boundary
        secs = int(window.total_seconds())
        if secs <= 60:
            interval = "minute"
        elif secs <= 3600:
            interval = "hour"
        elif secs <= 86400:
            interval = "day"
        else:
            interval = "week"

        aligned = await self._exec.fetchval(
            "SELECT TIME_BUCKET($1, $2)", interval, start_ms
        )
        bucket_start = int(aligned) if aligned is not None else start_ms

        points: list[TimeSeriesPoint] = []
        while bucket_start < end_ms:
            bucket_end = bucket_start + window_ms
            effective_end = min(bucket_end, end_ms)
            result = await self._exec.fetchval(
                f"SELECT {sql_fn}($1, $2, $3)",
                measurement,
                bucket_start,
                effective_end,
            )
            if result is not None:
                ts = datetime.fromtimestamp(bucket_start / 1000, tz=timezone.utc)
                points.append(TimeSeriesPoint(timestamp=ts, value=float(result)))
            bucket_start = bucket_end

        return points

    async def match(
        self, measurement: str, pattern: str, *, limit: int = 100
    ) -> list[TimeSeriesPoint]:
        """Match time series entries by tag pattern.

        Uses the Nucleus TS_MATCH function for tag-based filtering.
        """
        self._require()
        rows = await self._exec.fetch(
            "SELECT * FROM TS_MATCH($1, $2, $3)", measurement, pattern, limit
        )
        points: list[TimeSeriesPoint] = []
        for row in rows:
            ts = datetime.fromtimestamp(
                int(row.get("timestamp", 0)) / 1000, tz=timezone.utc
            )
            points.append(
                TimeSeriesPoint(
                    timestamp=ts,
                    value=float(row.get("value", 0)),
                    tags=dict(row.get("tags", {}) or {}),
                )
            )
        return points

    async def retention(self, measurement: str, days: int) -> bool:
        """Set retention policy for a series."""
        self._require()
        return await self._exec.fetchval(
            "SELECT TS_RETENTION($1, $2)", measurement, days
        )
