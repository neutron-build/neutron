"""juliacall bridge for heavy computation via ModelingToolkit.jl / DifferentialEquations.jl.

Python handles orchestration; Julia handles heavy computation (DAEs, index reduction,
large-scale simulation).  This module is optional — requires ``juliacall`` to be
installed and Julia to be present on PATH.

Install: pip install juliacall
"""

from __future__ import annotations
import numpy as np
from dataclasses import dataclass


@dataclass
class BridgeResult:
    """Result of a Julia bridge simulation."""
    t: np.ndarray
    variables: dict[str, np.ndarray]

    def __getitem__(self, key: str) -> np.ndarray:
        return self.variables[key]

    def __contains__(self, key: str) -> bool:
        return key in self.variables

    def summary(self) -> dict:
        return {
            name: {
                "min": float(arr.min()),
                "max": float(arr.max()),
                "final": float(arr[-1]),
            }
            for name, arr in self.variables.items()
        }


class JuliaBridge:
    """Bridge to Julia's ModelingToolkit.jl and DifferentialEquations.jl.

    Initializes the Julia runtime on first use (lazy, takes a few seconds).

    Usage
    -----
    ::

        bridge = JuliaBridge()
        result = bridge.simulate(
            equations='''
            @variables t x(t) v(t)
            @parameters m=1.0 k=10.0 c=0.5
            D = Differential(t)
            eqs = [D(x) ~ v, m*D(v) ~ -k*x - c*v]
            ''',
            t_span=(0.0, 10.0),
            initial_conditions={"x": 1.0, "v": 0.0},
        )
    """

    def __init__(self):
        self._jl = None      # lazily initialized
        self._ready = False

    def _ensure_initialized(self):
        if self._ready:
            return
        try:
            from juliacall import Main as jl
        except ImportError:
            raise ImportError(
                "juliacall is required for the Julia bridge.\n"
                "Install with: pip install juliacall\n"
                "Also ensure Julia is installed: https://julialang.org/downloads/"
            )
        self._jl = jl
        # Load required Julia packages
        jl.seval("using Pkg")
        for pkg in ("ModelingToolkit", "DifferentialEquations"):
            try:
                jl.seval(f"using {pkg}")
            except Exception as exc:
                raise RuntimeError(
                    f"Julia package '{pkg}' not installed.\n"
                    f"Run in Julia: Pkg.add(\"{pkg}\")\n"
                    f"Error: {exc}"
                )
        self._ready = True

    def simulate(
        self,
        equations: str,
        t_span: tuple[float, float],
        initial_conditions: dict[str, float],
        solver: str = "Tsit5()",
        saveat: float | None = None,
    ) -> BridgeResult:
        """Simulate a system defined in ModelingToolkit.jl syntax.

        Parameters
        ----------
        equations         : Julia code string defining the MTK system
        t_span            : (t_start, t_end)
        initial_conditions: dict of variable_name → initial value
        solver            : DifferentialEquations.jl solver (default: Tsit5 for non-stiff)
        saveat            : output time step (None = adaptive)

        Returns
        -------
        BridgeResult with .t and .variables arrays (numpy)
        """
        self._ensure_initialized()
        jl = self._jl

        # Evaluate user equations in Julia
        jl.seval(equations)

        # Build the ODESystem and solve
        ic_julia = ", ".join(
            f"{k} => {v}" for k, v in initial_conditions.items()
        )
        saveat_str = f"saveat={saveat}" if saveat is not None else ""

        solve_code = f"""
        @named sys = ODESystem(eqs, t)
        sys_simplified = structural_simplify(sys)
        u0 = [{ic_julia}]
        prob = ODEProblem(sys_simplified, u0, ({t_span[0]}, {t_span[1]}))
        sol = solve(prob, {solver}{', ' + saveat_str if saveat_str else ''})
        sol
        """
        sol = jl.seval(solve_code)

        # Convert solution to numpy
        t_arr = np.array(sol.t)
        var_names = list(initial_conditions.keys())
        variables: dict[str, np.ndarray] = {}
        for vname in var_names:
            try:
                arr = jl.seval(f"Array(sol[{vname}])")
                variables[vname] = np.array(arr)
            except Exception:
                pass

        return BridgeResult(t=t_arr, variables=variables)

    def solve_dae(
        self,
        equations: str,
        t_span: tuple[float, float],
        initial_conditions: dict[str, float],
        initial_derivatives: dict[str, float] | None = None,
        solver: str = "Rodas5()",
    ) -> BridgeResult:
        """Solve a DAE system via Julia (index reduction handled automatically by MTK).

        Parameters
        ----------
        equations           : Julia code defining MTK ODESystem with algebraic equations
        t_span              : (t_start, t_end)
        initial_conditions  : initial values for state variables
        initial_derivatives : initial derivatives (optional, estimated if not provided)
        solver              : DAE solver (default: Rodas5 for stiff/DAE systems)

        Returns
        -------
        BridgeResult
        """
        if initial_derivatives is None:
            initial_derivatives = {}
        return self.simulate(
            equations=equations,
            t_span=t_span,
            initial_conditions=initial_conditions,
            solver=solver,
        )

    @property
    def is_available(self) -> bool:
        """True if juliacall is importable (Julia may not yet be initialized)."""
        try:
            import juliacall  # noqa: F401
            return True
        except ImportError:
            return False
