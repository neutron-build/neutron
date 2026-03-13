"""Compare parameter sweep runs stored in Nucleus TimeSeries."""

from __future__ import annotations
import numpy as np
from typing import TYPE_CHECKING


def compare_runs(
    conn,
    run_ids: list[str],
    variable: str,
    t_start_ms: int | None = None,
    t_end_ms: int | None = None,
) -> dict[str, dict]:
    """Load and compare a variable across multiple simulation runs.

    Parameters
    ----------
    conn     : psycopg3 Connection
    run_ids  : list of run identifiers to compare
    variable : variable name to compare (e.g., "x")
    t_start_ms / t_end_ms : optional time window

    Returns
    -------
    dict mapping run_id → {t_ms, values, stats}
    """
    from .load import load_results

    results: dict[str, dict] = {}
    for run_id in run_ids:
        t_ms, arrays = load_results(
            conn, run_id, [variable], t_start_ms, t_end_ms
        )
        arr = arrays.get(variable, np.array([]))
        if len(arr) == 0:
            stats: dict = {"min": None, "max": None, "final": None, "mean": None}
        else:
            stats = {
                "min": float(arr.min()),
                "max": float(arr.max()),
                "final": float(arr[-1]),
                "mean": float(arr.mean()),
            }
        results[run_id] = {"t_ms": t_ms, "values": arr, "stats": stats}

    return results


def sweep_summary(
    conn,
    pattern: str,
    variable: str,
    stat: str = "max",
) -> list[tuple[str, float]]:
    """Summarize a parameter sweep by computing *stat* of *variable* per run.

    Parameters
    ----------
    pattern  : run_id prefix pattern (e.g., "sweep-stiffness-*")
    variable : variable to aggregate
    stat     : one of "min", "max", "mean", "final"

    Returns
    -------
    List of (run_id, stat_value) sorted by stat_value.
    """
    from .load import list_runs

    run_ids = [r for r in list_runs(conn, f"sim:{pattern}") if pattern.rstrip("*") in r]
    comparison = compare_runs(conn, run_ids, variable)

    summary: list[tuple[str, float]] = []
    for run_id, data in comparison.items():
        val = data["stats"].get(stat)
        if val is not None:
            summary.append((run_id, val))

    return sorted(summary, key=lambda x: x[1])
