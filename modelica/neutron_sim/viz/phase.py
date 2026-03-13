"""Phase portrait and state-space visualization."""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..solvers.ode import SimulationResult
    from ..core.variable import Variable


def phase_portrait(
    result: "SimulationResult",
    var_x,
    var_y,
    title: str = "",
    xlabel: str | None = None,
    ylabel: str | None = None,
    color_by_time: bool = True,
    figsize: tuple[float, float] = (6, 6),
    show: bool = True,
    ax=None,
):
    """Plot a 2D phase portrait (state-space trajectory).

    Parameters
    ----------
    result       : SimulationResult
    var_x        : Variable or name for x-axis
    var_y        : Variable or name for y-axis
    color_by_time: if True, color the trajectory by simulation time
    title        : plot title
    show         : call plt.show()
    ax           : existing Axes

    Returns
    -------
    matplotlib Figure
    """
    import matplotlib.pyplot as plt
    import numpy as np
    from ..core.variable import Variable

    xname = var_x.name if isinstance(var_x, Variable) else str(var_x)
    yname = var_y.name if isinstance(var_y, Variable) else str(var_y)

    x_arr = result._by_name.get(xname, np.array([]))
    y_arr = result._by_name.get(yname, np.array([]))

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.figure

    if color_by_time and len(result.t) > 1:
        points = np.array([x_arr, y_arr]).T.reshape(-1, 1, 2)
        segments = np.concatenate([points[:-1], points[1:]], axis=1)
        from matplotlib.collections import LineCollection
        from matplotlib.cm import viridis
        norm = plt.Normalize(result.t.min(), result.t.max())
        lc = LineCollection(segments, cmap="viridis", norm=norm)
        lc.set_array(result.t[:-1])
        lc.set_linewidth(1.5)
        ax.add_collection(lc)
        ax.autoscale()
        cbar = plt.colorbar(lc, ax=ax)
        cbar.set_label("Time [s]")
    else:
        ax.plot(x_arr, y_arr, lw=1.5)

    # Mark start and end
    if len(x_arr) > 0:
        ax.plot(x_arr[0], y_arr[0], "go", markersize=8, label="Start", zorder=5)
        ax.plot(x_arr[-1], y_arr[-1], "rs", markersize=8, label="End", zorder=5)
        ax.legend(fontsize=8)

    ax.set_xlabel(xlabel or xname)
    ax.set_ylabel(ylabel or yname)
    ax.set_title(title or f"Phase Portrait: {xname} vs {yname}")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    if show:
        plt.show()
    return fig


def poincare_section(
    result: "SimulationResult",
    var_x,
    var_y,
    section_var,
    section_value: float = 0.0,
    direction: str = "positive",
    title: str = "",
    figsize: tuple[float, float] = (6, 6),
    show: bool = True,
):
    """Plot a Poincaré section of the trajectory.

    Finds crossings where *section_var* passes through *section_value* in
    the specified *direction*, and plots the corresponding (var_x, var_y) points.

    Parameters
    ----------
    direction : "positive" (upward crossing), "negative" (downward), or "both"

    Returns
    -------
    matplotlib Figure
    """
    import matplotlib.pyplot as plt
    import numpy as np
    from ..core.variable import Variable

    xname = var_x.name if isinstance(var_x, Variable) else str(var_x)
    yname = var_y.name if isinstance(var_y, Variable) else str(var_y)
    sname = section_var.name if isinstance(section_var, Variable) else str(section_var)

    x_arr = result._by_name.get(xname, np.array([]))
    y_arr = result._by_name.get(yname, np.array([]))
    s_arr = result._by_name.get(sname, np.array([]))

    # Find crossings
    s_shifted = s_arr - section_value
    cross_x, cross_y = [], []
    for i in range(len(s_shifted) - 1):
        pos_cross = s_shifted[i] < 0 and s_shifted[i + 1] >= 0
        neg_cross = s_shifted[i] >= 0 and s_shifted[i + 1] < 0
        crossing = (
            (direction == "positive" and pos_cross) or
            (direction == "negative" and neg_cross) or
            (direction == "both" and (pos_cross or neg_cross))
        )
        if crossing:
            frac = abs(s_shifted[i]) / (abs(s_shifted[i]) + abs(s_shifted[i + 1]))
            cross_x.append(x_arr[i] + frac * (x_arr[i + 1] - x_arr[i]))
            cross_y.append(y_arr[i] + frac * (y_arr[i + 1] - y_arr[i]))

    fig, ax = plt.subplots(figsize=figsize)
    if cross_x:
        ax.plot(cross_x, cross_y, "o", markersize=4)
    ax.set_xlabel(xname)
    ax.set_ylabel(yname)
    ax.set_title(title or f"Poincaré Section ({sname}={section_value})")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    if show:
        plt.show()
    return fig
