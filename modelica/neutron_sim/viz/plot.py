"""Time-series visualization for simulation results."""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..solvers.ode import SimulationResult
    from ..core.variable import Variable


def plot_timeseries(
    result: "SimulationResult",
    variables=None,
    title: str = "",
    xlabel: str = "Time [s]",
    ylabel: str = "Value",
    figsize: tuple[float, float] = (10, 5),
    show: bool = True,
    ax=None,
):
    """Plot time-series from a SimulationResult.

    Parameters
    ----------
    result    : SimulationResult
    variables : list of Variable objects or names (all if None)
    title     : plot title
    xlabel    : x-axis label
    ylabel    : y-axis label
    figsize   : figure size (width, height) in inches
    show      : call plt.show() after plotting
    ax        : existing matplotlib Axes to plot on (creates new if None)

    Returns
    -------
    matplotlib Figure
    """
    import matplotlib.pyplot as plt
    from ..core.variable import Variable

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.figure

    targets = variables if variables is not None else list(result._by_name.keys())
    for var in targets:
        name = var.name if isinstance(var, Variable) else str(var)
        if name in result._by_name:
            ax.plot(result.t, result._by_name[name], label=name)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    if show:
        plt.show()
    return fig


def plot_comparison(
    results: dict[str, "SimulationResult"],
    variable: str,
    title: str = "",
    xlabel: str = "Time [s]",
    figsize: tuple[float, float] = (10, 5),
    show: bool = True,
):
    """Overlay a single variable across multiple simulation runs.

    Parameters
    ----------
    results  : dict mapping run_label → SimulationResult
    variable : variable name to plot
    title    : plot title

    Returns
    -------
    matplotlib Figure
    """
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=figsize)
    for label, result in results.items():
        if variable in result._by_name:
            ax.plot(result.t, result._by_name[variable], label=label)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(variable)
    ax.set_title(title or f"Comparison: {variable}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    if show:
        plt.show()
    return fig


def plot_parameter_sweep(
    sweep_data: list[tuple],
    param_name: str,
    output_name: str,
    title: str = "",
    figsize: tuple[float, float] = (8, 5),
    show: bool = True,
):
    """Plot output statistic vs parameter value from a sweep.

    Parameters
    ----------
    sweep_data  : list of (param_value, output_value) pairs
    param_name  : parameter name for x-axis
    output_name : output variable/stat name for y-axis

    Returns
    -------
    matplotlib Figure
    """
    import matplotlib.pyplot as plt

    xs = [p[0] for p in sweep_data]
    ys = [p[1] for p in sweep_data]

    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(xs, ys, "o-", markersize=6)
    ax.set_xlabel(param_name)
    ax.set_ylabel(output_name)
    ax.set_title(title or f"{output_name} vs {param_name}")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    if show:
        plt.show()
    return fig
