"""PyJulia bridge for calling DifferentialEquations.jl from Python.

Extends the basic ``neutron_sim.julia.bridge.JuliaBridge`` with:

1. Export of neutron_sim / neutron_modelica models as Julia ODE/DAE problems
2. Access to the full DifferentialEquations.jl solver suite
3. Import of Julia solutions back into Python-native data structures
4. Parameter sweep support for sensitivity analysis

Usage
-----
::

    from neutron_modelica.julia_bridge import JuliaDEBridge

    bridge = JuliaDEBridge()

    # Export a neutron_sim System as a Julia ODE problem
    result = bridge.solve_system(system, t_span=(0, 10))

    # Or define a Julia ODE directly
    result = bridge.solve_ode(
        rhs="function f(du, u, p, t)\\n  du[1] = u[2]\\n  du[2] = -p[1]*u[1]\\nend",
        u0=[1.0, 0.0],
        t_span=(0.0, 10.0),
        p=[10.0],
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from neutron_sim.core.system import System


@dataclass
class DEResult:
    """Result from a DifferentialEquations.jl solve.

    Attributes
    ----------
    t         : time points array
    u         : solution matrix (n_vars x n_times)
    var_names : names of solution variables
    retcode   : solver return code string (e.g. "Success", "MaxIters")
    stats     : dict of solver statistics (nf, nw, etc.)
    """
    t: np.ndarray
    u: np.ndarray
    var_names: list[str]
    retcode: str = "Success"
    stats: dict[str, Any] = None

    def __post_init__(self):
        if self.stats is None:
            self.stats = {}

    def __getitem__(self, key: str | int) -> np.ndarray:
        if isinstance(key, int):
            return self.u[key]
        if key in self.var_names:
            idx = self.var_names.index(key)
            return self.u[idx]
        raise KeyError(f"Variable '{key}' not found; available: {self.var_names}")

    def __contains__(self, key: str) -> bool:
        return key in self.var_names

    @property
    def variables(self) -> dict[str, np.ndarray]:
        return {name: self.u[i] for i, name in enumerate(self.var_names)}

    def summary(self) -> dict[str, dict[str, float]]:
        return {
            name: {
                "min": float(self.u[i].min()),
                "max": float(self.u[i].max()),
                "final": float(self.u[i, -1]),
            }
            for i, name in enumerate(self.var_names)
        }


class JuliaDEBridge:
    """Bridge to Julia's DifferentialEquations.jl ecosystem.

    Provides a higher-level interface than ``neutron_sim.julia.JuliaBridge``,
    focused on:
    - Converting neutron_sim Systems to Julia ODE problems
    - Solving Julia-defined ODEs/DAEs/SDEs
    - Importing results back into Python
    - Parameter sensitivity sweeps via DiffEqSensitivity.jl

    The Julia runtime is lazily initialized on first use.
    """

    def __init__(self):
        self._jl: Any = None
        self._ready: bool = False

    @property
    def is_available(self) -> bool:
        """True if juliacall is importable."""
        try:
            import juliacall  # noqa: F401
            return True
        except ImportError:
            return False

    # ── Julia Initialization ─────────────────────────────────────────────

    def _ensure_initialized(self):
        """Lazily initialize the Julia runtime and load packages."""
        if self._ready:
            return
        try:
            from juliacall import Main as jl
        except ImportError:
            raise ImportError(
                "juliacall is required for the Julia bridge.\n"
                "Install with: pip install juliacall\n"
                "Ensure Julia is installed: https://julialang.org/downloads/"
            )
        self._jl = jl
        # Load DifferentialEquations
        jl.seval("using Pkg")
        for pkg in ("DifferentialEquations",):
            try:
                jl.seval(f"using {pkg}")
            except Exception as exc:
                raise RuntimeError(
                    f"Julia package '{pkg}' not installed.\n"
                    f"Run in Julia REPL: Pkg.add(\"{pkg}\")\n"
                    f"Error: {exc}"
                )
        self._ready = True

    # ── Export neutron_sim System as Julia ODE ────────────────────────────

    def export_system_as_julia(self, system: "System") -> str:
        """Convert a neutron_sim System into Julia ODE function code.

        Generates a Julia function ``f!(du, u, p, t)`` that encodes
        the ODE right-hand side from the system's flattened equations.

        Parameters
        ----------
        system : assembled neutron_sim System

        Returns
        -------
        Julia source code string defining ``f!``, ``u0``, and ``tspan``.
        """
        from neutron_sim.solvers.ode import _build_ode_rhs, _extract_scaled_der
        from neutron_sim.core.variable import Der, BinOp, Variable, Constant, UnaryOp

        state_vars = system.state_variables()
        ics = system.initial_conditions()
        eqs = system.flatten()

        var_names = [v.name for v in state_vars]
        u0_vals = [ics.get(v, 0.0) for v in state_vars]

        # Build RHS expressions as Julia code
        rhs_lines = []
        for i, vname in enumerate(var_names):
            for eq in eqs:
                for side, other in [(eq.lhs, eq.rhs), (eq.rhs, eq.lhs)]:
                    extracted = _extract_scaled_der(side)
                    if extracted is not None:
                        d_node, coeff = extracted
                        if d_node.variable.name == vname:
                            expr_str = _expr_to_julia(other, var_names)
                            if coeff != 1.0:
                                rhs_lines.append(
                                    f"    du[{i+1}] = ({expr_str}) / {coeff}"
                                )
                            else:
                                rhs_lines.append(f"    du[{i+1}] = {expr_str}")
                            break
                else:
                    continue
                break

        body = "\n".join(rhs_lines)
        u0_str = "[" + ", ".join(str(v) for v in u0_vals) + "]"

        code = f"""function f!(du, u, p, t)
{body}
end

u0 = {u0_str}
# Variable mapping: {', '.join(f'u[{i+1}]={n}' for i, n in enumerate(var_names))}
"""
        return code

    def solve_system(
        self,
        system: "System",
        t_span: tuple[float, float],
        solver: str = "Tsit5()",
        saveat: float | None = None,
        rtol: float = 1e-6,
        atol: float = 1e-8,
    ) -> DEResult:
        """Export a neutron_sim System and solve it using Julia.

        Parameters
        ----------
        system   : assembled neutron_sim System
        t_span   : (t_start, t_end)
        solver   : Julia solver constructor string (e.g., "Tsit5()", "Rodas5()")
        saveat   : output save interval (None = adaptive)
        rtol/atol: solver tolerances

        Returns
        -------
        DEResult with time and solution arrays.
        """
        self._ensure_initialized()
        jl = self._jl

        julia_code = self.export_system_as_julia(system)
        jl.seval(julia_code)

        state_vars = system.state_variables()
        var_names = [v.name for v in state_vars]

        saveat_str = f", saveat={saveat}" if saveat is not None else ""
        solve_code = f"""
prob = ODEProblem(f!, u0, ({t_span[0]}, {t_span[1]}))
sol = solve(prob, {solver}, reltol={rtol}, abstol={atol}{saveat_str})
sol
"""
        sol = jl.seval(solve_code)
        return _convert_julia_solution(sol, var_names, jl)

    # ── Solve arbitrary Julia ODE ────────────────────────────────────────

    def solve_ode(
        self,
        rhs: str,
        u0: list[float],
        t_span: tuple[float, float],
        p: list[float] | None = None,
        var_names: list[str] | None = None,
        solver: str = "Tsit5()",
        saveat: float | None = None,
        rtol: float = 1e-6,
        atol: float = 1e-8,
    ) -> DEResult:
        """Solve an ODE defined by Julia code.

        Parameters
        ----------
        rhs       : Julia code defining ``f!(du, u, p, t)``
        u0        : initial condition vector
        t_span    : (t_start, t_end)
        p         : parameter vector (optional)
        var_names : names for the solution variables
        solver    : Julia solver constructor string
        saveat    : output save interval
        rtol/atol : solver tolerances

        Returns
        -------
        DEResult
        """
        self._ensure_initialized()
        jl = self._jl

        if var_names is None:
            var_names = [f"u{i+1}" for i in range(len(u0))]

        jl.seval(rhs)

        u0_str = "[" + ", ".join(str(v) for v in u0) + "]"
        p_str = "nothing"
        if p is not None:
            p_str = "[" + ", ".join(str(v) for v in p) + "]"

        saveat_str = f", saveat={saveat}" if saveat is not None else ""
        solve_code = f"""
prob = ODEProblem(f!, {u0_str}, ({t_span[0]}, {t_span[1]}), {p_str})
sol = solve(prob, {solver}, reltol={rtol}, abstol={atol}{saveat_str})
sol
"""
        sol = jl.seval(solve_code)
        return _convert_julia_solution(sol, var_names, jl)

    # ── Solve DAE ────────────────────────────────────────────────────────

    def solve_dae(
        self,
        rhs: str,
        u0: list[float],
        du0: list[float],
        t_span: tuple[float, float],
        differential_vars: list[bool] | None = None,
        var_names: list[str] | None = None,
        solver: str = "IDA()",
        saveat: float | None = None,
    ) -> DEResult:
        """Solve a DAE system using Julia's Sundials.IDA or similar.

        Parameters
        ----------
        rhs               : Julia code defining ``f!(resid, du, u, p, t)``
        u0                : initial state vector
        du0               : initial derivative vector
        t_span            : (t_start, t_end)
        differential_vars : which variables are differential (True) vs algebraic (False)
        var_names         : names for solution variables
        solver            : Julia DAE solver string
        saveat            : output save interval

        Returns
        -------
        DEResult
        """
        self._ensure_initialized()
        jl = self._jl

        if var_names is None:
            var_names = [f"u{i+1}" for i in range(len(u0))]

        jl.seval(rhs)

        u0_str = "[" + ", ".join(str(v) for v in u0) + "]"
        du0_str = "[" + ", ".join(str(v) for v in du0) + "]"

        if differential_vars is not None:
            dv_str = "[" + ", ".join(
                "true" if d else "false" for d in differential_vars
            ) + "]"
        else:
            dv_str = "nothing"

        saveat_str = f", saveat={saveat}" if saveat is not None else ""

        solve_code = f"""
prob = DAEProblem(f!, {du0_str}, {u0_str}, ({t_span[0]}, {t_span[1]}),
                  differential_vars={dv_str})
sol = solve(prob, {solver}{saveat_str})
sol
"""
        sol = jl.seval(solve_code)
        return _convert_julia_solution(sol, var_names, jl)

    # ── Parameter Sweep ──────────────────────────────────────────────────

    def parameter_sweep(
        self,
        rhs: str,
        u0: list[float],
        t_span: tuple[float, float],
        param_values: list[list[float]],
        var_names: list[str] | None = None,
        solver: str = "Tsit5()",
        saveat: float | None = None,
    ) -> list[DEResult]:
        """Run an ODE solve for each set of parameter values.

        Parameters
        ----------
        rhs          : Julia function code
        u0           : initial conditions (shared across sweeps)
        t_span       : time span
        param_values : list of parameter vectors to sweep over
        var_names    : variable names
        solver       : solver string
        saveat       : output interval

        Returns
        -------
        List of DEResult, one per parameter set.
        """
        results = []
        for p in param_values:
            r = self.solve_ode(
                rhs=rhs, u0=u0, t_span=t_span, p=p,
                var_names=var_names, solver=solver, saveat=saveat,
            )
            results.append(r)
        return results


# ── Expression-to-Julia Converter ────────────────────────────────────────────

def _expr_to_julia(expr, var_names: list[str]) -> str:
    """Convert a neutron_sim Expr to a Julia expression string.

    Maps Variable names to ``u[i]`` references based on var_names ordering.
    """
    from neutron_sim.core.variable import Variable, Constant, BinOp, UnaryOp, Der

    if isinstance(expr, Constant):
        return str(expr.value)

    if isinstance(expr, Variable):
        if expr.name in var_names:
            idx = var_names.index(expr.name) + 1
            return f"u[{idx}]"
        return str(expr.value if hasattr(expr, "value") else 0.0)

    if isinstance(expr, UnaryOp) and expr.op == "-":
        inner = _expr_to_julia(expr.operand, var_names)
        return f"(-{inner})"

    if isinstance(expr, BinOp):
        left = _expr_to_julia(expr.left, var_names)
        right = _expr_to_julia(expr.right, var_names)
        return f"({left} {expr.op} {right})"

    if isinstance(expr, Der):
        # Should not appear in RHS after flattening
        return "0.0"

    return "0.0"


def _convert_julia_solution(sol, var_names: list[str], jl) -> DEResult:
    """Convert a Julia ODE solution to a DEResult."""
    t_arr = np.array(jl.seval("Array(sol.t)"))
    n_vars = len(var_names)
    u = np.zeros((n_vars, len(t_arr)))

    for i in range(n_vars):
        try:
            arr = jl.seval(f"Array(sol[{i+1}, :])")
            u[i] = np.array(arr)
        except Exception:
            pass

    retcode = "Success"
    try:
        retcode = str(jl.seval("string(sol.retcode)"))
    except Exception:
        pass

    return DEResult(
        t=t_arr,
        u=u,
        var_names=var_names,
        retcode=retcode,
    )
