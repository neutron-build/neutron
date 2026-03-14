"""
Thermal domain — heat transfer components.

Connector semantics:
  - T (temperature, K) is the across variable -> equalized at connections
  - Q (heat flow rate, W) is the through variable -> sum to zero at nodes (Kirchhoff)

This gives conservation of energy at every thermal node, enforced by
the generic connection mechanism in System.flatten().

Design note: same explicit-ODE approach as the electrical and mechanical domains.
ThermalCapacitance state = T (temperature of the thermal mass).
"""

from __future__ import annotations
from ...core.variable import Variable, Equation, der, Constant
from ...core.connector import Connector
from ...core.component import Component


class HeatPort(Connector):
    """
    Thermal connector (heat port).

    T: temperature [K]     — across (equalized at connections)
    Q: heat flow rate [W]  — through (Kirchhoff sum = 0 at connections)
    """
    _across = ("T",)
    _through = ("Q",)

    def __init__(self, prefix: str = ""):
        super().__init__(prefix)


class ThermalCapacitance(Component):
    """
    Thermal mass (capacitance): C * dT/dt = Q_net

    State variable: T (temperature of the thermal mass).
    Port: port (the thermal attachment point).
    """

    def __init__(self, C: float = 1.0, T0: float = 293.15, **kwargs):
        self.C = C
        self.T = Variable(f"T_{id(self):#x}")
        self.port = HeatPort(prefix=f"thermcap_{id(self):#x}")
        self._T0 = T0
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.C * der(self.T) == self.port.Q,
            self.port.T == self.T,
        ]

    def initial_conditions(self) -> dict[Variable, float]:
        return {self.T: self._T0}


class ThermalResistance(Component):
    """
    Thermal resistor: Q = (T_a - T_b) / R

    Two HeatPorts: port_a, port_b.
    Heat flows from a to b when T_a > T_b.
    """

    def __init__(self, R: float = 1.0, **kwargs):
        self.R = R
        self.port_a = HeatPort(prefix=f"thermres_a_{id(self):#x}")
        self.port_b = HeatPort(prefix=f"thermres_b_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        dT = self.port_a.T - self.port_b.T
        Q = dT * Constant(1.0 / self.R)
        return [
            self.port_a.Q == Q,
            self.port_b.Q == -Q,
        ]


class FixedTemperature(Component):
    """
    Ideal temperature source (boundary condition): port.T = T_fixed.

    Heat flow is free (reaction).
    """

    def __init__(self, T: float = 293.15, **kwargs):
        self.T_fixed = T
        self.port = HeatPort(prefix=f"fixedT_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.port.T == Constant(self.T_fixed),
        ]


class HeatSource(Component):
    """
    Ideal heat flow source: injects Q watts into the attached node.

    Sign convention (Modelica-style): the port "pushes" Q into the network.
    At a connection: source.port.Q + cap.port.Q == 0
    -> cap.port.Q = -source.port.Q = Q (positive = heats capacitance).
    We set port.Q = -Q so Kirchhoff delivers +Q to the thermal mass.
    """

    def __init__(self, Q: float = 100.0, **kwargs):
        self.Q_set = Q
        self.port = HeatPort(prefix=f"heatsrc_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.port.Q == Constant(-self.Q_set),
        ]


class Convection(Component):
    """
    Convective heat transfer: Q = h * A * (T_a - T_b)

    Two HeatPorts: port_a, port_b.
    h: convective heat transfer coefficient [W/(m^2*K)]
    A: surface area [m^2]
    """

    def __init__(self, h: float = 10.0, A: float = 1.0, **kwargs):
        self.h = h
        self.A = A
        self.port_a = HeatPort(prefix=f"conv_a_{id(self):#x}")
        self.port_b = HeatPort(prefix=f"conv_b_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        dT = self.port_a.T - self.port_b.T
        Q = Constant(self.h * self.A) * dT
        return [
            self.port_a.Q == Q,
            self.port_b.Q == -Q,
        ]
