"""Fixed-step co-simulation master algorithm.

Co-simulation allows multiple dynamical models to exchange data at discrete
*communication points* separated by *step_size* seconds.  Between communication
points each model advances independently using its own internal integrator.

Connection semantics
--------------------
A connection is a 4-tuple  ``(src_name, src_var, dst_name, dst_var)`` meaning
"at each communication point, the value of *src_var* from model *src_name* is
injected as *dst_var* into model *dst_name* for the next interval."

The injection is implemented as a *forcing override*: the destination model's
ODE RHS evaluates *dst_var* as a constant equal to the most recently exchanged
value.
"""

from __future__ import annotations
import numpy as np
from typing import Callable

from ..core.variable import Variable, Expr, Constant, BinOp, UnaryOp, Der
from ..core.system import System
from ..solvers.ode import SimulationResult, _build_ode_rhs


# ── Step-by-step ODE integrator ───────────────────────────────────────────────

class OdeStepper:
    """Advance a System one fixed step at a time (RK4) with optional forcing."""

    def __init__(self, system: System):
        self._system = system
        self._state_vars = system.state_variables()
        ics = system.initial_conditions()
        self._state = np.array(
            [ics.get(v, 0.0) for v in self._state_vars], dtype=float
        )
        self._names = [v.name for v in self._state_vars]
        self._t = 0.0
        self._f_base = _build_ode_rhs(system, self._state_vars)

    # --- public API -------------------------------------------------------

    def initialize(self, t0: float = 0.0) -> None:
        ics = self._system.initial_conditions()
        self._state = np.array(
            [ics.get(v, 0.0) for v in self._state_vars], dtype=float
        )
        self._t = t0

    def step(self, dt: float, forcing: dict[str, float] | None = None) -> dict[str, float]:
        """Advance by *dt* seconds using RK4.

        Parameters
        ----------
        forcing : variable overrides injected from co-sim connections.
                  Values are treated as constants over the step.
        """
        f = self._make_rhs(forcing)

        y = self._state
        k1 = np.array(f(self._t, y))
        k2 = np.array(f(self._t + dt / 2, y + dt / 2 * k1))
        k3 = np.array(f(self._t + dt / 2, y + dt / 2 * k2))
        k4 = np.array(f(self._t + dt, y + dt * k3))

        self._state = y + (dt / 6) * (k1 + 2 * k2 + 2 * k3 + k4)
        self._t += dt
        return self.state_dict()

    @property
    def t(self) -> float:
        return self._t

    def state_dict(self) -> dict[str, float]:
        return {name: float(self._state[i]) for i, name in enumerate(self._names)}

    # --- internal ---------------------------------------------------------

    def _make_rhs(self, forcing: dict[str, float] | None) -> Callable:
        if not forcing:
            return self._f_base

        f_base = self._f_base

        def f_forced(t: float, y: np.ndarray) -> list[float]:
            result = f_base(t, y)
            return result  # forcing is already injected via state dict override

        # Rebuild RHS injecting forcing as extra state entries
        state_vars = self._state_vars
        names = self._names
        _forcing = dict(forcing)

        def f(t: float, y: np.ndarray) -> list[float]:
            state: dict[str, float] = {names[i]: y[i] for i in range(len(names))}
            state.update(_forcing)  # override with external values

            eqs = self._system.flatten()
            from ..solvers.ode import _extract_scaled_der
            from ..core.variable import Expr

            result = []
            for vname in names:
                for eq in eqs:
                    for side, other in [(eq.lhs, eq.rhs), (eq.rhs, eq.lhs)]:
                        extracted = _extract_scaled_der(side)
                        if extracted is not None:
                            d_node, coeff = extracted
                            if d_node.variable.name == vname:
                                result.append(other.eval(state) / coeff)
                                break
                    else:
                        continue
                    break
            return result

        return f


# ── Co-Simulation Master ──────────────────────────────────────────────────────

class CoSimulation:
    """Fixed-step co-simulation master.

    Parameters
    ----------
    models      : list of ``(name, System)`` pairs
    connections : list of ``(src_model, src_var, dst_model, dst_var)`` tuples
    step_size   : communication step size [s]
    """

    def __init__(
        self,
        models: list[tuple[str, System]],
        connections: list[tuple[str, str, str, str]] | None = None,
        step_size: float = 0.001,
    ):
        self.models = models
        self.connections = connections or []
        self.step_size = step_size
        self._steppers: dict[str, OdeStepper] = {
            name: OdeStepper(sys) for name, sys in models
        }

    def run(
        self,
        t_span: tuple[float, float],
        record_all: bool = True,
    ) -> dict[str, SimulationResult]:
        """Run co-simulation from *t_span[0]* to *t_span[1]*.

        Returns
        -------
        dict mapping model name → :class:`SimulationResult`
        """
        t_start, t_end = t_span
        dt = self.step_size

        # Initialize all steppers
        for stepper in self._steppers.values():
            stepper.initialize(t_start)

        # Storage
        records: dict[str, list[dict]] = {name: [] for name, _ in self.models}
        t_records: dict[str, list[float]] = {name: [] for name, _ in self.models}

        # Current exchange values (initialized from ICs)
        exchange: dict[tuple[str, str], float] = {}
        for name, stepper in self._steppers.items():
            for vname, val in stepper.state_dict().items():
                exchange[(name, vname)] = val

        # Record initial state
        for name, stepper in self._steppers.items():
            t_records[name].append(stepper.t)
            records[name].append(stepper.state_dict())

        t = t_start
        while t < t_end - dt * 1e-9:
            h = min(dt, t_end - t)

            # Build forcing for each model from last exchange
            forcings: dict[str, dict[str, float]] = {name: {} for name, _ in self.models}
            for src_model, src_var, dst_model, dst_var in self.connections:
                val = exchange.get((src_model, src_var), 0.0)
                forcings[dst_model][dst_var] = val

            # Step each model forward
            for name, stepper in self._steppers.items():
                state = stepper.step(h, forcing=forcings.get(name))
                for vname, val in state.items():
                    exchange[(name, vname)] = val
                if record_all:
                    t_records[name].append(stepper.t)
                    records[name].append(state)

            t += h

        # Build SimulationResult for each model
        results: dict[str, SimulationResult] = {}
        for name, _ in self.models:
            recs = records[name]
            ts = np.array(t_records[name])
            if not recs:
                continue
            state_names = list(recs[0].keys())
            y = np.vstack([[r[n] for n in state_names] for r in recs]).T
            results[name] = SimulationResult(t=ts, state_names=state_names, y=y)

        return results
