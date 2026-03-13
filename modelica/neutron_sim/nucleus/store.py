"""Store simulation results in Nucleus TimeSeries."""

from __future__ import annotations
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..solvers.ode import SimulationResult
    from ..core.variable import Variable


def store_results(
    conn,
    run_id: str,
    result: "SimulationResult",
    variables: list | None = None,
    tags: dict | None = None,
    t_offset_ms: int | None = None,
) -> int:
    """Store simulation results in Nucleus TimeSeries (synchronous).

    Each variable becomes a series named ``sim:{run_id}:{variable_name}``.

    Parameters
    ----------
    conn        : psycopg3 Connection (or anything with ``execute()``/``fetchone()``)
    run_id      : unique identifier for this simulation run
    result      : SimulationResult from simulate()
    variables   : list of Variable objects or names to store (all if None)
    tags        : arbitrary metadata stored as KV tags (not yet used — future)
    t_offset_ms : base timestamp in ms (defaults to current wall clock)

    Returns
    -------
    Number of data points written.
    """
    from ..core.variable import Variable

    if t_offset_ms is None:
        t_offset_ms = int(time.time() * 1000)

    if variables is None:
        var_names = list(result._by_name.keys())
    else:
        var_names = [
            v.name if isinstance(v, Variable) else str(v) for v in variables
        ]

    count = 0
    for vname in var_names:
        if vname not in result._by_name:
            continue
        series = f"sim:{run_id}:{vname}"
        arr = result._by_name[vname]
        t_arr = result.t

        for i, (t_s, val) in enumerate(zip(t_arr, arr)):
            ts_ms = t_offset_ms + int(t_s * 1000)
            conn.execute(
                "SELECT TS_INSERT(%s, %s, %s)",
                (series, ts_ms, float(val)),
            )
            count += 1

    return count


async def store_results_async(
    conn,
    run_id: str,
    result: "SimulationResult",
    variables: list | None = None,
    tags: dict | None = None,
    t_offset_ms: int | None = None,
) -> int:
    """Async version of :func:`store_results` for asyncpg/psycopg3 async."""
    from ..core.variable import Variable

    if t_offset_ms is None:
        t_offset_ms = int(time.time() * 1000)

    if variables is None:
        var_names = list(result._by_name.keys())
    else:
        var_names = [
            v.name if isinstance(v, Variable) else str(v) for v in variables
        ]

    count = 0
    for vname in var_names:
        if vname not in result._by_name:
            continue
        series = f"sim:{run_id}:{vname}"
        arr = result._by_name[vname]
        t_arr = result.t

        for t_s, val in zip(t_arr, arr):
            ts_ms = t_offset_ms + int(t_s * 1000)
            await conn.execute(
                "SELECT TS_INSERT($1, $2, $3)",
                series, ts_ms, float(val),
            )
            count += 1

    return count
