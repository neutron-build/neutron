"""
Fluid domain — incompressible flow components.

Connector semantics:
  - P (pressure, Pa) is the across variable -> equalized at connections
  - m_dot (mass flow rate, kg/s) is the through variable -> sum to zero at nodes (Kirchhoff)

This gives conservation of mass at every fluid node, enforced by
the generic connection mechanism in System.flatten().

Design note: same explicit-ODE approach as the other domains.
Tank state = h (fluid level). Linear components only for the solver;
the Valve uses a linearised approximation for ODE compatibility.
"""

from __future__ import annotations
from ...core.variable import Variable, Equation, der, Constant
from ...core.connector import Connector
from ...core.component import Component


# Gravitational constant for hydrostatic pressure
_G = 9.81


class FluidPort(Connector):
    """
    Fluid connector (port).

    P:     pressure [Pa]       — across (equalized at connections)
    m_dot: mass flow rate [kg/s] — through (Kirchhoff sum = 0 at connections)
    """
    _across = ("P",)
    _through = ("m_dot",)

    def __init__(self, prefix: str = ""):
        super().__init__(prefix)


class Pipe(Component):
    """
    Linear pressure drop: dP = R * m_dot (Hagen-Poiseuille analogy)

    Two FluidPorts: port_a, port_b.
    Flow goes from a to b when P_a > P_b.
    R: hydraulic resistance [Pa*s/kg]
    """

    def __init__(self, R: float = 1000.0, **kwargs):
        self.R = R
        self.port_a = FluidPort(prefix=f"pipe_a_{id(self):#x}")
        self.port_b = FluidPort(prefix=f"pipe_b_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        dP = self.port_a.P - self.port_b.P
        m_dot = dP * Constant(1.0 / self.R)
        return [
            self.port_a.m_dot == m_dot,
            self.port_b.m_dot == -m_dot,
        ]


class Tank(Component):
    """
    Fluid accumulator (open tank): A * dh/dt = m_dot / rho

    State variable: h (fluid level [m]).
    Port: port (the fluid connection at the bottom).
    Outlet pressure: P = rho * g * h (hydrostatic head).

    A:   cross-sectional area [m^2]
    rho: fluid density [kg/m^3]
    h0:  initial fluid level [m]
    """

    def __init__(self, A: float = 1.0, rho: float = 1000.0, h0: float = 1.0, **kwargs):
        self.A = A
        self.rho = rho
        self.h = Variable(f"h_{id(self):#x}")
        self.port = FluidPort(prefix=f"tank_{id(self):#x}")
        self._h0 = h0
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            Constant(self.A) * der(self.h) == self.port.m_dot * Constant(1.0 / self.rho),
            self.port.P == Constant(self.rho * _G) * self.h,
        ]

    def initial_conditions(self) -> dict[Variable, float]:
        return {self.h: self._h0}


class Pump(Component):
    """
    Ideal pressure source: P_b - P_a = P_set (constant pressure rise).

    Two FluidPorts: port_a (inlet), port_b (outlet).
    Sign convention: pump raises pressure from a to b.
    Flow passes through: m_dot_a + m_dot_b = 0.
    """

    def __init__(self, P_set: float = 100000.0, **kwargs):
        self.P_set = P_set
        self.port_a = FluidPort(prefix=f"pump_a_{id(self):#x}")
        self.port_b = FluidPort(prefix=f"pump_b_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.port_b.P - self.port_a.P == Constant(self.P_set),
            # Flow conservation: what flows in flows out
            self.port_a.m_dot == -self.port_b.m_dot,
        ]


class Valve(Component):
    """
    Flow valve: m_dot = Cv * (P_a - P_b) (linearised).

    For ODE compatibility, we use the linearised form rather than the
    nonlinear sqrt(|dP|)*sign(dP) characteristic. For nonlinear valve
    models, use the Julia bridge.

    Two FluidPorts: port_a, port_b.
    Cv: valve coefficient [kg/(s*Pa)]
    """

    def __init__(self, Cv: float = 0.001, **kwargs):
        self.Cv = Cv
        self.port_a = FluidPort(prefix=f"valve_a_{id(self):#x}")
        self.port_b = FluidPort(prefix=f"valve_b_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        dP = self.port_a.P - self.port_b.P
        m_dot = Constant(self.Cv) * dP
        return [
            self.port_a.m_dot == m_dot,
            self.port_b.m_dot == -m_dot,
        ]


class FixedPressure(Component):
    """
    Ideal pressure boundary: port.P = P (constant).

    Mass flow is free (reaction).
    """

    def __init__(self, P: float = 101325.0, **kwargs):
        self.P_fixed = P
        self.port = FluidPort(prefix=f"fixedP_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.port.P == Constant(self.P_fixed),
        ]
