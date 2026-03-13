"""Load simulation results from Nucleus TimeSeries."""

from __future__ import annotations
import numpy as np


def load_results(
    conn,
    run_id: str,
    variables: list[str],
    t_start_ms: int | None = None,
    t_end_ms: int | None = None,
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    """Load simulation results from Nucleus TimeSeries (synchronous).

    Parameters
    ----------
    conn       : psycopg3 Connection
    run_id     : run identifier (same as used in store_results)
    variables  : list of variable names to load
    t_start_ms : start timestamp in ms (None = beginning)
    t_end_ms   : end timestamp in ms (None = end)

    Returns
    -------
    (t_ms_array, {var_name: values_array})
    """
    data: dict[str, list[tuple[int, float]]] = {}

    for vname in variables:
        series = f"sim:{run_id}:{vname}"

        if t_start_ms is not None and t_end_ms is not None:
            rows = conn.execute(
                "SELECT TS_RANGE(%s, %s, %s)",
                (series, t_start_ms, t_end_ms),
            ).fetchall()
        else:
            # Use a broad range
            rows = conn.execute(
                "SELECT TS_RANGE(%s, %s, %s)",
                (series, 0, 9_999_999_999_999),
            ).fetchall()

        # rows: list of (timestamp_ms, value) or similar — parse first column
        points: list[tuple[int, float]] = []
        for row in rows:
            raw = row[0]
            if isinstance(raw, (list, tuple)) and len(raw) == 2:
                points.append((int(raw[0]), float(raw[1])))
            elif hasattr(raw, "timestamp_ms"):
                points.append((int(raw.timestamp_ms), float(raw.value)))
        data[vname] = sorted(points, key=lambda p: p[0])

    if not data or not any(data.values()):
        return np.array([]), {v: np.array([]) for v in variables}

    # Align on common timestamps
    first_var = next(iter(data))
    t_ms = np.array([p[0] for p in data[first_var]])
    arrays: dict[str, np.ndarray] = {}
    for vname in variables:
        arrays[vname] = np.array([p[1] for p in data.get(vname, [])])

    return t_ms, arrays


async def load_results_async(
    conn,
    run_id: str,
    variables: list[str],
    t_start_ms: int | None = None,
    t_end_ms: int | None = None,
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    """Async version of :func:`load_results`."""
    data: dict[str, list[tuple[int, float]]] = {}

    for vname in variables:
        series = f"sim:{run_id}:{vname}"
        t0 = t_start_ms if t_start_ms is not None else 0
        t1 = t_end_ms if t_end_ms is not None else 9_999_999_999_999

        rows = await conn.fetch(
            "SELECT TS_RANGE($1, $2, $3)", series, t0, t1
        )
        points: list[tuple[int, float]] = []
        for row in rows:
            raw = row[0]
            if isinstance(raw, (list, tuple)):
                points.append((int(raw[0]), float(raw[1])))
        data[vname] = sorted(points, key=lambda p: p[0])

    if not data or not any(data.values()):
        return np.array([]), {v: np.array([]) for v in variables}

    first_var = next(iter(data))
    t_ms = np.array([p[0] for p in data[first_var]])
    arrays = {
        vname: np.array([p[1] for p in data.get(vname, [])])
        for vname in variables
    }
    return t_ms, arrays


def list_runs(conn, pattern: str = "sim:*") -> list[str]:
    """Return all run IDs matching *pattern* (uses MATCH_PATTERN)."""
    rows = conn.execute("SELECT MATCH_PATTERN(%s)", (pattern,)).fetchall()
    run_ids = set()
    for row in rows:
        series = row[0]
        # series looks like "sim:{run_id}:{var_name}"
        parts = series.split(":", 2)
        if len(parts) >= 2:
            run_ids.add(parts[1])
    return sorted(run_ids)
