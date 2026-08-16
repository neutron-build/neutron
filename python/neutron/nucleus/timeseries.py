"""TimeSeries model — wraps Nucleus TS_* SQL functions."""

from __future__ import annotations

import json
from typing import cast

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
        return cast("float | None", await self._exec.fetchval("SELECT TS_LAST($1)", measurement))

    async def count(self, measurement: str) -> int:
        """Count data points in a series."""
        self._require()
        return cast("int", await self._exec.fetchval("SELECT TS_COUNT($1)", measurement))

    async def query(
        self,
        measurement: str,
        start: datetime,
        end: datetime,
        *,
        tags: dict[str, str] | None = None,
    ) -> list[TimeSeriesPoint]:
        """Return the raw data points stored in ``[start, end]``.

        This used to synthesize the answer from ``buckets`` (default 60)
        ``TS_RANGE_AVG`` calls, because raw point retrieval had no SQL surface.
        That was sixty round trips to read points the engine already held, and
        it was lossy: any bucket containing more than one point returned their
        average as a single fabricated point at the bucket boundary, so the
        timestamps did not correspond to stored data.

        ``TS_RANGE`` now returns the points themselves, and every SDK uses it,
        which is what closes the three-different-answers gap this method was on
        the wrong side of. Use ``aggregate`` when bucketed averages are what is
        wanted — that is a different question and has its own method.
        """
        self._require()
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        if end_ms <= start_ms:
            return []

        raw = await self._exec.fetchval(
            "SELECT TS_RANGE($1, $2, $3)", measurement, start_ms, end_ms
        )
        if not raw:
            return []
        return [
            TimeSeriesPoint(
                timestamp=datetime.fromtimestamp(item["t"] / 1000, tz=timezone.utc),
                value=float(item["v"]),
            )
            for item in json.loads(raw)
        ]

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

        ``fn`` is ``avg`` or ``count``. The docstring used to promise ``sum``,
        ``min``, ``max``, ``first`` and ``last`` as well; the engine ships
        TS_RANGE_AVG and TS_RANGE_COUNT and nothing else, so those five raised
        ValueError from the map below. Documenting what exists rather than what
        was planned.
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

        # Align start to a TIME_BUCKET boundary.
        #
        # This passed an interval NAME here — "minute"/"hour"/"day"/"week" —
        # chosen by bucketing the window size. The engine's TIME_BUCKET takes
        # (bucket_millis, ts), both INT8, so every call raised on the type and
        # aggregate() had never once worked. Two bugs in one: even had the
        # string been accepted, aligning a 5-minute window to an hour boundary
        # would have produced buckets that do not line up with the window the
        # caller asked for. Align to window_ms, which is what "align to the
        # bucket size" means.
        aligned = await self._exec.fetchval(
            "SELECT TIME_BUCKET($1, $2)", window_ms, start_ms
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

    async def retention(self, max_age_ms: int) -> bool:
        """Set the global retention policy for all time series.

        The engine's TS_RETENTION takes a single ``max_age_ms`` argument and
        applies it globally — per-series retention is not supported.
        """
        self._require()
        result = await self._exec.fetchval("SELECT TS_RETENTION($1)", max_age_ms)
        return result == "OK"
