"""Kirchhoff's law solver with Modified Nodal Analysis (MNA).

Implements a standalone circuit simulator that:

1. Builds a circuit graph from components (R, C, L, voltage/current sources)
2. Assembles the MNA matrix equation: ``G * x = s``
   - ``G`` is the conductance/stamp matrix
   - ``x`` is the vector of node voltages and branch currents
   - ``s`` is the source vector
3. Solves for DC operating point or runs transient simulation

KCL is enforced at every node (sum of currents = 0).
KVL is enforced implicitly through the MNA formulation.

Transient simulation uses either implicit Euler (first-order, A-stable)
or trapezoidal integration (second-order, A-stable).

Usage
-----
::

    from neutron_modelica.kirchhoff import (
        Circuit, Resistor, Capacitor, Inductor,
        VoltageSource, CurrentSource,
    )

    ckt = Circuit()
    ckt.add(VoltageSource("V1", "n1", "gnd", 5.0))
    ckt.add(Resistor("R1", "n1", "n2", 1000.0))
    ckt.add(Capacitor("C1", "n2", "gnd", 1e-6))

    # DC operating point
    dc = ckt.dc_operating_point()

    # Transient simulation
    result = ckt.transient(t_end=0.01, dt=1e-6, method="trapezoidal")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import numpy as np
from scipy import linalg


# ── Node Representation ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class CircuitNode:
    """A named node in the circuit. 'gnd' is the ground reference (node 0)."""
    name: str

    def __repr__(self) -> str:
        return self.name


GND = CircuitNode("gnd")


# ── Component Base ───────────────────────────────────────────────────────────

class Component:
    """Base class for all circuit components."""

    def __init__(self, name: str, node_p: str, node_n: str):
        self.name = name
        self.node_p = node_p  # positive terminal
        self.node_n = node_n  # negative terminal

    def stamp_dc(
        self,
        G: np.ndarray,
        s: np.ndarray,
        node_map: dict[str, int],
        vsrc_offset: int,
        vsrc_index: dict[str, int],
    ) -> None:
        """Stamp this component into the MNA matrix for DC analysis."""
        raise NotImplementedError

    def stamp_transient(
        self,
        G: np.ndarray,
        s: np.ndarray,
        node_map: dict[str, int],
        vsrc_offset: int,
        vsrc_index: dict[str, int],
        x_prev: np.ndarray,
        dt: float,
        method: str,
    ) -> None:
        """Stamp this component for transient analysis."""
        # Default: same as DC
        self.stamp_dc(G, s, node_map, vsrc_offset, vsrc_index)


class Resistor(Component):
    r"""Linear resistor: ``i = v / R`` (Ohm's law).

    MNA stamp: adds ``1/R`` to the diagonal conductance entries and
    ``-1/R`` to the off-diagonal entries for the two terminal nodes.

    Parameters
    ----------
    name   : component identifier (e.g., "R1")
    node_p : positive terminal node name
    node_n : negative terminal node name
    R      : resistance in ohms [Ohm]
    """

    def __init__(self, name: str, node_p: str, node_n: str, R: float):
        super().__init__(name, node_p, node_n)
        if R <= 0:
            raise ValueError(f"Resistor '{name}': R must be positive, got {R}")
        self.R = R

    def stamp_dc(self, G, s, node_map, vsrc_offset, vsrc_index):
        g = 1.0 / self.R
        ip = node_map.get(self.node_p, -1)
        in_ = node_map.get(self.node_n, -1)
        _stamp_conductance(G, ip, in_, g)

    def stamp_transient(self, G, s, node_map, vsrc_offset, vsrc_index,
                        x_prev, dt, method):
        self.stamp_dc(G, s, node_map, vsrc_offset, vsrc_index)


class Capacitor(Component):
    r"""Linear capacitor: ``i = C * dv/dt``.

    For transient analysis:
    - Implicit Euler: ``i_n = C/dt * (v_n - v_{n-1})``
      Equivalent to a conductance ``G_eq = C/dt`` in parallel with
      a current source ``I_eq = C/dt * v_{n-1}``.
    - Trapezoidal: ``i_n = 2C/dt * (v_n - v_{n-1}) - i_{n-1}``
      Equivalent to ``G_eq = 2C/dt`` with ``I_eq = 2C/dt * v_{n-1} + i_{n-1}``.

    Parameters
    ----------
    name   : component identifier
    node_p : positive terminal
    node_n : negative terminal
    C      : capacitance in farads [F]
    v0     : initial voltage across the capacitor [V]
    """

    def __init__(self, name: str, node_p: str, node_n: str, C: float,
                 v0: float = 0.0):
        super().__init__(name, node_p, node_n)
        if C <= 0:
            raise ValueError(f"Capacitor '{name}': C must be positive, got {C}")
        self.C = C
        self.v0 = v0
        self._i_prev: float = 0.0  # previous-step current (for trapezoidal)

    def stamp_dc(self, G, s, node_map, vsrc_offset, vsrc_index):
        # DC: capacitor is open circuit — no stamp needed
        pass

    def stamp_transient(self, G, s, node_map, vsrc_offset, vsrc_index,
                        x_prev, dt, method):
        ip = node_map.get(self.node_p, -1)
        in_ = node_map.get(self.node_n, -1)

        # Previous voltage across capacitor
        vp_prev = x_prev[ip] if ip >= 0 else 0.0
        vn_prev = x_prev[in_] if in_ >= 0 else 0.0
        v_prev = vp_prev - vn_prev

        if method == "trapezoidal":
            g_eq = 2.0 * self.C / dt
            i_eq = g_eq * v_prev + self._i_prev
        else:  # implicit_euler
            g_eq = self.C / dt
            i_eq = g_eq * v_prev

        # Stamp equivalent conductance
        _stamp_conductance(G, ip, in_, g_eq)
        # Stamp equivalent current source
        _stamp_current(s, ip, in_, i_eq)

    def update_history(self, x: np.ndarray, node_map: dict[str, int], dt: float,
                       method: str):
        """Update internal history after a time step (for trapezoidal method)."""
        ip = node_map.get(self.node_p, -1)
        in_ = node_map.get(self.node_n, -1)
        vp = x[ip] if ip >= 0 else 0.0
        vn = x[in_] if in_ >= 0 else 0.0
        v = vp - vn
        # Current approximation
        if method == "trapezoidal":
            self._i_prev = 2.0 * self.C / dt * v - self._i_prev
        else:
            self._i_prev = self.C / dt * v


class Inductor(Component):
    r"""Linear inductor: ``v = L * di/dt``.

    For transient analysis:
    - Implicit Euler: ``v_n = L/dt * (i_n - i_{n-1})``
      Equivalent to a conductance ``G_eq = dt/L`` with
      ``I_eq = i_{n-1}``.
    - Trapezoidal: ``v_n = 2L/dt * (i_n - i_{n-1}) - v_{n-1}``
      Equivalent to ``G_eq = dt/(2L)`` with ``I_eq = i_{n-1} + dt/(2L)*v_{n-1}``.

    Parameters
    ----------
    name   : component identifier
    node_p : positive terminal
    node_n : negative terminal
    L      : inductance in henrys [H]
    i0     : initial current through the inductor [A]
    """

    def __init__(self, name: str, node_p: str, node_n: str, L: float,
                 i0: float = 0.0):
        super().__init__(name, node_p, node_n)
        if L <= 0:
            raise ValueError(f"Inductor '{name}': L must be positive, got {L}")
        self.L = L
        self.i0 = i0
        self._i_prev: float = i0
        self._v_prev: float = 0.0  # for trapezoidal

    def stamp_dc(self, G, s, node_map, vsrc_offset, vsrc_index):
        # DC: inductor is short circuit — model as zero-resistance wire
        # Add as a voltage source with V=0
        if self.name not in vsrc_index:
            return
        idx = vsrc_offset + vsrc_index[self.name]
        ip = node_map.get(self.node_p, -1)
        in_ = node_map.get(self.node_n, -1)
        if ip >= 0:
            G[ip, idx] += 1.0
            G[idx, ip] += 1.0
        if in_ >= 0:
            G[in_, idx] -= 1.0
            G[idx, in_] -= 1.0
        s[idx] = 0.0  # v_p - v_n = 0

    def stamp_transient(self, G, s, node_map, vsrc_offset, vsrc_index,
                        x_prev, dt, method):
        ip = node_map.get(self.node_p, -1)
        in_ = node_map.get(self.node_n, -1)

        if method == "trapezoidal":
            g_eq = dt / (2.0 * self.L)
            i_eq = self._i_prev + g_eq * self._v_prev
        else:  # implicit_euler
            g_eq = dt / self.L
            i_eq = self._i_prev

        # Stamp equivalent conductance
        _stamp_conductance(G, ip, in_, g_eq)
        # Stamp equivalent current source
        _stamp_current(s, ip, in_, i_eq)

    def update_history(self, x: np.ndarray, node_map: dict[str, int], dt: float,
                       method: str):
        """Update internal history after a time step."""
        ip = node_map.get(self.node_p, -1)
        in_ = node_map.get(self.node_n, -1)
        vp = x[ip] if ip >= 0 else 0.0
        vn = x[in_] if in_ >= 0 else 0.0
        v = vp - vn

        if method == "trapezoidal":
            g_eq = dt / (2.0 * self.L)
            self._i_prev = 2.0 * g_eq * v - self._i_prev
            self._v_prev = v
        else:
            g_eq = dt / self.L
            self._i_prev = g_eq * v


class VoltageSource(Component):
    r"""Ideal voltage source: ``v_p - v_n = V``.

    MNA adds an extra row/column for the branch current through the source.
    The KVL equation ``v_p - v_n = V`` is added as an additional constraint.

    Parameters
    ----------
    name   : component identifier
    node_p : positive terminal (higher voltage)
    node_n : negative terminal
    V      : voltage [V]
    """

    def __init__(self, name: str, node_p: str, node_n: str, V: float):
        super().__init__(name, node_p, node_n)
        self.V = V

    def stamp_dc(self, G, s, node_map, vsrc_offset, vsrc_index):
        if self.name not in vsrc_index:
            return
        idx = vsrc_offset + vsrc_index[self.name]
        ip = node_map.get(self.node_p, -1)
        in_ = node_map.get(self.node_n, -1)

        # KCL contribution: branch current enters node_p, leaves node_n
        if ip >= 0:
            G[ip, idx] += 1.0
            G[idx, ip] += 1.0
        if in_ >= 0:
            G[in_, idx] -= 1.0
            G[idx, in_] -= 1.0

        # KVL: v_p - v_n = V
        s[idx] = self.V

    def stamp_transient(self, G, s, node_map, vsrc_offset, vsrc_index,
                        x_prev, dt, method):
        self.stamp_dc(G, s, node_map, vsrc_offset, vsrc_index)


class CurrentSource(Component):
    r"""Ideal current source: injects ``I`` amperes from node_n to node_p
    (current flows into node_p through the external circuit).

    Parameters
    ----------
    name   : component identifier
    node_p : terminal where current enters (from external circuit)
    node_n : terminal where current leaves (from external circuit)
    I      : current [A]
    """

    def __init__(self, name: str, node_p: str, node_n: str, I: float):
        super().__init__(name, node_p, node_n)
        self.I = I

    def stamp_dc(self, G, s, node_map, vsrc_offset, vsrc_index):
        ip = node_map.get(self.node_p, -1)
        in_ = node_map.get(self.node_n, -1)
        # Current enters node_p, leaves node_n
        _stamp_current(s, ip, in_, self.I)

    def stamp_transient(self, G, s, node_map, vsrc_offset, vsrc_index,
                        x_prev, dt, method):
        self.stamp_dc(G, s, node_map, vsrc_offset, vsrc_index)


# ── MNA Stamping Helpers ─────────────────────────────────────────────────────

def _stamp_conductance(G: np.ndarray, i: int, j: int, g: float):
    """Stamp a conductance ``g`` between nodes ``i`` and ``j``.

    Adds to the MNA matrix:
        G[i,i] += g
        G[j,j] += g
        G[i,j] -= g
        G[j,i] -= g

    Node index -1 represents ground (not stamped).
    """
    if i >= 0:
        G[i, i] += g
    if j >= 0:
        G[j, j] += g
    if i >= 0 and j >= 0:
        G[i, j] -= g
        G[j, i] -= g


def _stamp_current(s: np.ndarray, ip: int, in_: int, current: float):
    """Stamp a current source into the RHS vector.

    Current flows from node_n to node_p (enters node_p).
    """
    if ip >= 0:
        s[ip] += current
    if in_ >= 0:
        s[in_] -= current


# ── Transient Result ─────────────────────────────────────────────────────────

@dataclass
class TransientResult:
    """Result of a transient simulation.

    Attributes
    ----------
    t          : time array [s]
    node_voltages : dict mapping node name -> voltage array
    branch_currents : dict mapping voltage source name -> current array
    """
    t: np.ndarray
    node_voltages: dict[str, np.ndarray] = field(default_factory=dict)
    branch_currents: dict[str, np.ndarray] = field(default_factory=dict)

    def __getitem__(self, key: str) -> np.ndarray:
        if key in self.node_voltages:
            return self.node_voltages[key]
        if key in self.branch_currents:
            return self.branch_currents[key]
        raise KeyError(f"'{key}' not found in node voltages or branch currents")

    def __contains__(self, key: str) -> bool:
        return key in self.node_voltages or key in self.branch_currents

    def summary(self) -> dict[str, dict[str, float]]:
        result = {}
        for name, arr in self.node_voltages.items():
            result[name] = {
                "min": float(arr.min()),
                "max": float(arr.max()),
                "final": float(arr[-1]),
            }
        for name, arr in self.branch_currents.items():
            result[name] = {
                "min": float(arr.min()),
                "max": float(arr.max()),
                "final": float(arr[-1]),
            }
        return result


# ── Circuit Assembly ─────────────────────────────────────────────────────────

class Circuit:
    """Circuit graph builder and MNA solver.

    Add components, then solve for DC operating point or run transient
    simulation.

    Example
    -------
    ::

        ckt = Circuit()
        ckt.add(VoltageSource("V1", "n1", "gnd", 5.0))
        ckt.add(Resistor("R1", "n1", "n2", 1000.0))
        ckt.add(Capacitor("C1", "n2", "gnd", 1e-6))

        dc = ckt.dc_operating_point()
        print(dc["n2"])  # voltage at node n2
    """

    def __init__(self):
        self._components: list[Component] = []
        self._nodes: set[str] = set()

    def add(self, component: Component) -> "Circuit":
        """Add a component to the circuit.

        Returns self for method chaining.
        """
        self._components.append(component)
        self._nodes.add(component.node_p)
        self._nodes.add(component.node_n)
        return self

    @property
    def nodes(self) -> list[str]:
        """All non-ground node names (sorted for deterministic ordering)."""
        return sorted(n for n in self._nodes if n != "gnd")

    @property
    def components(self) -> list[Component]:
        return list(self._components)

    def _build_maps(self) -> tuple[dict[str, int], int, dict[str, int]]:
        """Build node-index map and voltage-source-index map.

        Returns
        -------
        node_map     : node name -> matrix index (ground excluded)
        vsrc_offset  : first matrix index for voltage source currents
        vsrc_index   : voltage source name -> index within vsrc block
        """
        nodes = self.nodes
        node_map = {name: i for i, name in enumerate(nodes)}
        vsrc_offset = len(nodes)

        # Identify voltage sources and inductors (which need extra rows)
        vsrc_names = []
        for c in self._components:
            if isinstance(c, (VoltageSource, Inductor)):
                vsrc_names.append(c.name)
        vsrc_index = {name: i for i, name in enumerate(vsrc_names)}

        return node_map, vsrc_offset, vsrc_index

    def _matrix_size(self) -> int:
        n_nodes = len(self.nodes)
        n_vsrc = sum(
            1 for c in self._components
            if isinstance(c, (VoltageSource, Inductor))
        )
        return n_nodes + n_vsrc

    # ── DC Operating Point ────────────────────────────────────────────────

    def dc_operating_point(self) -> dict[str, float]:
        """Solve for the DC operating point.

        Returns a dict mapping node names to voltages and voltage source
        names to branch currents.

        KCL is satisfied at every node; KVL is satisfied along every loop
        through the MNA formulation.
        """
        node_map, vsrc_offset, vsrc_index = self._build_maps()
        n = self._matrix_size()

        G = np.zeros((n, n))
        s = np.zeros(n)

        for comp in self._components:
            comp.stamp_dc(G, s, node_map, vsrc_offset, vsrc_index)

        x = linalg.solve(G, s)

        result: dict[str, float] = {}
        for name, idx in node_map.items():
            result[name] = float(x[idx])
        for name, idx in vsrc_index.items():
            result[f"I({name})"] = float(x[vsrc_offset + idx])
        return result

    # ── Transient Simulation ──────────────────────────────────────────────

    def transient(
        self,
        t_end: float,
        dt: float,
        method: Literal["implicit_euler", "trapezoidal"] = "trapezoidal",
        t_start: float = 0.0,
    ) -> TransientResult:
        """Run a transient simulation using implicit integration.

        Parameters
        ----------
        t_end   : end time [s]
        dt      : time step [s]
        method  : "implicit_euler" (1st order) or "trapezoidal" (2nd order)
        t_start : start time [s] (default 0)

        Returns
        -------
        TransientResult with time array, node voltages, and branch currents.
        """
        if method not in ("implicit_euler", "trapezoidal"):
            raise ValueError(
                f"Unknown method '{method}'; use 'implicit_euler' or 'trapezoidal'"
            )

        node_map, vsrc_offset, vsrc_index = self._build_maps()
        n = self._matrix_size()
        nodes = self.nodes

        # Initialize solution vector from DC operating point or ICs
        x = np.zeros(n)

        # Apply initial conditions for capacitors and inductors
        for comp in self._components:
            if isinstance(comp, Capacitor):
                ip = node_map.get(comp.node_p, -1)
                in_ = node_map.get(comp.node_n, -1)
                # Set initial voltage difference
                if ip >= 0:
                    x[ip] += comp.v0
            elif isinstance(comp, Inductor):
                if comp.name in vsrc_index:
                    idx = vsrc_offset + vsrc_index[comp.name]
                    x[idx] = comp.i0

        # Try to get a consistent DC initial condition
        try:
            dc = self.dc_operating_point()
            for name, idx in node_map.items():
                x[idx] = dc.get(name, 0.0)
            # Override with explicit ICs
            for comp in self._components:
                if isinstance(comp, Capacitor) and comp.v0 != 0.0:
                    ip = node_map.get(comp.node_p, -1)
                    in_ = node_map.get(comp.node_n, -1)
                    # Capacitor initial voltage overrides DC
                    if ip >= 0 and in_ < 0:
                        x[ip] = comp.v0
                    elif ip >= 0 and in_ >= 0:
                        x[ip] = comp.v0 + x[in_]
        except Exception:
            pass

        # Time stepping
        n_steps = int((t_end - t_start) / dt) + 1
        t_arr = np.linspace(t_start, t_end, n_steps)

        # Storage
        x_history = np.zeros((n_steps, n))
        x_history[0] = x

        # Reset component history
        for comp in self._components:
            if isinstance(comp, Inductor):
                comp._i_prev = comp.i0
                comp._v_prev = 0.0
            elif isinstance(comp, Capacitor):
                comp._i_prev = 0.0

        for step in range(1, n_steps):
            x_prev = x_history[step - 1]

            # Build transient MNA system
            G = np.zeros((n, n))
            s_vec = np.zeros(n)

            for comp in self._components:
                comp.stamp_transient(
                    G, s_vec, node_map, vsrc_offset, vsrc_index,
                    x_prev, dt, method
                )

            # Solve
            try:
                x_new = linalg.solve(G, s_vec)
            except linalg.LinAlgError:
                # Singular matrix — use previous solution
                x_new = x_prev.copy()

            x_history[step] = x_new

            # Update component histories (capacitor/inductor)
            for comp in self._components:
                if isinstance(comp, (Capacitor, Inductor)):
                    comp.update_history(x_new, node_map, dt, method)

        # Build result
        node_voltages = {}
        for name, idx in node_map.items():
            node_voltages[name] = x_history[:, idx]

        branch_currents = {}
        for name, idx in vsrc_index.items():
            branch_currents[f"I({name})"] = x_history[:, vsrc_offset + idx]

        return TransientResult(
            t=t_arr,
            node_voltages=node_voltages,
            branch_currents=branch_currents,
        )

    # ── KCL/KVL Verification ─────────────────────────────────────────────

    def verify_kcl(self, node_voltages: dict[str, float]) -> dict[str, float]:
        """Verify KCL at every node: compute the current residual.

        Returns a dict mapping node name to the sum of currents (should be ~0).
        """
        residuals: dict[str, float] = {}
        for node_name in self.nodes:
            current_sum = 0.0
            for comp in self._components:
                v_p = node_voltages.get(comp.node_p, 0.0)
                v_n = node_voltages.get(comp.node_n, 0.0)
                v_across = v_p - v_n
                if isinstance(comp, Resistor):
                    i = v_across / comp.R
                    if comp.node_p == node_name:
                        current_sum -= i  # current leaving node through resistor
                    if comp.node_n == node_name:
                        current_sum += i
                elif isinstance(comp, CurrentSource):
                    if comp.node_p == node_name:
                        current_sum += comp.I
                    if comp.node_n == node_name:
                        current_sum -= comp.I
            residuals[node_name] = current_sum
        return residuals

    def __repr__(self) -> str:
        return (
            f"Circuit({len(self._components)} components, "
            f"{len(self.nodes)} nodes)"
        )
