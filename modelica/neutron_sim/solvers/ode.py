"""SciPy solve_ivp wrapper for simple ODE systems."""

from __future__ import annotations
import numpy as np
from scipy.integrate import solve_ivp
from typing import TYPE_CHECKING

from ..core.variable import Variable, Der, BinOp, Constant, Equation, Expr
from ..core.system import System
from .auto_select import estimate_stiffness, select_method

if TYPE_CHECKING:
    pass


class SimulationResult:
    """
    Holds the output of a simulation run.

    Access variables by name or by Variable object:
        result["x"]      # by name
        result[mass.x]   # by Variable
        result.t         # time array
    """

    def __init__(
        self,
        t: np.ndarray,
        state_names: list[str],
        y: np.ndarray,
    ):
        self.t = t
        self._state_names = state_names
        self._y = y  # shape (n_states, n_times)
        self._by_name: dict[str, np.ndarray] = {
            name: y[i] for i, name in enumerate(state_names)
        }

    def __getitem__(self, key) -> np.ndarray:
        if isinstance(key, Variable):
            return self._by_name[key.name]
        return self._by_name[key]

    def __contains__(self, key) -> bool:
        name = key.name if isinstance(key, Variable) else key
        return name in self._by_name

    def items(self):
        return self._by_name.items()

    def summary(self) -> dict:
        return {
            name: {
                "min": float(arr.min()),
                "max": float(arr.max()),
                "final": float(arr[-1]),
            }
            for name, arr in self._by_name.items()
        }

    def plot(self, variables=None, title: str = "", show: bool = True):
        """Plot time-series for the given variables (or all if None)."""
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots()
        targets = variables if variables is not None else list(self._by_name.keys())
        for var in targets:
            name = var.name if isinstance(var, Variable) else var
            ax.plot(self.t, self._by_name[name], label=name)
        ax.set_xlabel("Time [s]")
        ax.legend()
        if title:
            ax.set_title(title)
        if show:
            plt.show()
        return fig

    def phase_plot(self, var_x, var_y, title: str = "", show: bool = True):
        """Plot a 2D phase portrait."""
        import matplotlib.pyplot as plt

        xname = var_x.name if isinstance(var_x, Variable) else var_x
        yname = var_y.name if isinstance(var_y, Variable) else var_y
        fig, ax = plt.subplots()
        ax.plot(self._by_name[xname], self._by_name[yname])
        ax.set_xlabel(xname)
        ax.set_ylabel(yname)
        if title:
            ax.set_title(title)
        if show:
            plt.show()
        return fig


def _extract_scaled_der(expr) -> tuple["Der", float] | None:
    """
    Try to extract (Der_node, coefficient) from an expression.
    Handles: der(x)  →  (Der(x), 1.0)
             c * der(x)  →  (Der(x), c)
             der(x) * c  →  (Der(x), c)
    Returns None if the expression is not in this form.
    """
    if isinstance(expr, Der):
        return (expr, 1.0)
    if isinstance(expr, BinOp) and expr.op == "*":
        if isinstance(expr.left, Der):
            try:
                c = expr.right.eval({})
                return (expr.left, c)
            except (KeyError, ZeroDivisionError):
                pass
        if isinstance(expr.right, Der):
            try:
                c = expr.left.eval({})
                return (expr.right, c)
            except (KeyError, ZeroDivisionError):
                pass
    return None


def _build_ode_rhs(system: System, state_vars: list[Variable]):
    """
    Build a callable f(t, y) -> dy/dt from the system equations.

    Handles:
      - der(x) == rhs
      - rhs == der(x)
      - scalar * der(x) == rhs  (normalises by dividing rhs by scalar)
    """
    eqs = system.flatten()

    # Map state variable name → (RHS expr, coefficient)
    # der(v) = rhs_expr / coeff
    der_map: dict[str, tuple[Expr, float]] = {}

    for eq in eqs:
        for side, other in [(eq.lhs, eq.rhs), (eq.rhs, eq.lhs)]:
            result = _extract_scaled_der(side)
            if result is not None:
                d_node, coeff = result
                der_map[d_node.variable.name] = (other, coeff)
                break  # don't double-register

    state_names = [v.name for v in state_vars]
    missing = [n for n in state_names if n not in der_map]
    if missing:
        raise ValueError(
            f"No derivative equation found for: {missing}. "
            "Only explicit ODE systems are supported. Use Julia bridge for DAEs."
        )

    def f(t: float, y: np.ndarray) -> list[float]:
        state = {state_names[i]: y[i] for i in range(len(state_names))}
        result = []
        for name in state_names:
            rhs_expr, coeff = der_map[name]
            result.append(rhs_expr.eval(state) / coeff)
        return result

    return f


def simulate(
    system: System,
    t_span: tuple[float, float],
    dt: float | None = None,
    method: str | None = None,
    rtol: float = 1e-6,
    atol: float = 1e-8,
) -> SimulationResult:
    """
    Simulate a System using SciPy solve_ivp.

    Parameters
    ----------
    system   : assembled System
    t_span   : (t_start, t_end)
    dt       : output time step (optional — only affects output density, not accuracy)
    method   : 'RK45', 'Radau', 'DOP853', etc. Auto-detected if None.
    rtol/atol: solver tolerances

    Returns
    -------
    SimulationResult with .t, .y arrays indexed by variable name.
    """
    state_vars = system.state_variables()
    if not state_vars:
        raise ValueError("System has no state variables (no der() equations found).")

    ics = system.initial_conditions()
    y0 = np.array([ics.get(v, 0.0) for v in state_vars], dtype=float)

    f = _build_ode_rhs(system, state_vars)

    if method is None:
        ratio = estimate_stiffness(f, t_span[0], y0)
        method = select_method(ratio)

    t_eval = None
    if dt is not None:
        n = int((t_span[1] - t_span[0]) / dt) + 1
        t_eval = np.linspace(t_span[0], t_span[1], n)

    sol = solve_ivp(
        f,
        t_span,
        y0,
        method=method,
        t_eval=t_eval,
        rtol=rtol,
        atol=atol,
        dense_output=False,
    )

    if not sol.success:
        raise RuntimeError(f"Solver failed: {sol.message}")

    return SimulationResult(
        t=sol.t,
        state_names=[v.name for v in state_vars],
        y=sol.y,
    )
