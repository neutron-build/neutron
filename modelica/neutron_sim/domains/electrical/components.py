"""
Electrical circuit components using Kirchhoff's laws.

Connector semantics:
  - v (voltage) is the across variable → equalized at connections (KVL)
  - i (current) is the through variable → sum to zero at nodes (KCL)

This gives Kirchhoff's Current Law (KCL) at every node and
Kirchhoff's Voltage Law (KVL) along every loop, enforced by
the generic connection mechanism in System.flatten().

Design note: same explicit-ODE approach as the mechanical domain.
Capacitor state = v_cap (voltage across it), Inductor state = i_ind (current through it).
"""

from __future__ import annotations
from ...core.variable import Variable, Equation, der, Constant
from ...core.connector import Connector
from ...core.component import Component


class Pin(Connector):
    """
    Electrical pin (node).

    v: voltage [V]  — across (equalized at connections → KVL)
    i: current [A]  — through (sum to zero at connections → KCL)
    """
    _across = ("v",)
    _through = ("i",)

    def __init__(self, prefix: str = ""):
        super().__init__(prefix)


class Resistor(Component):
    """
    Linear resistor: v = R * i (Ohm's law)

    Two pins: p (positive), n (negative).
    v_across = v_p - v_n = R * i
    """

    def __init__(self, R: float = 1.0, **kwargs):
        self.R = R
        self.p = Pin(prefix=f"res_p_{id(self):#x}")
        self.n = Pin(prefix=f"res_n_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        v_across = self.p.v - self.n.v
        return [
            self.p.i == v_across * Constant(1.0 / self.R),
            self.n.i == -(v_across * Constant(1.0 / self.R)),
        ]


class Capacitor(Component):
    """
    Linear capacitor: i = C * dv/dt

    State variable: v_cap (voltage across capacitor).
    """

    def __init__(self, C: float = 1.0, v0: float = 0.0, **kwargs):
        self.C = C
        self.v_cap = Variable(f"v_cap_{id(self):#x}")
        self.p = Pin(prefix=f"cap_p_{id(self):#x}")
        self.n = Pin(prefix=f"cap_n_{id(self):#x}")
        self._v0 = v0
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.p.v - self.n.v == self.v_cap,
            self.C * der(self.v_cap) == self.p.i,
            self.n.i == -self.p.i,
        ]

    def initial_conditions(self) -> dict[Variable, float]:
        return {self.v_cap: self._v0}


class Inductor(Component):
    """
    Linear inductor: v = L * di/dt

    State variable: i_ind (current through inductor).
    """

    def __init__(self, L: float = 1.0, i0: float = 0.0, **kwargs):
        self.L = L
        self.i_ind = Variable(f"i_ind_{id(self):#x}")
        self.p = Pin(prefix=f"ind_p_{id(self):#x}")
        self.n = Pin(prefix=f"ind_n_{id(self):#x}")
        self._i0 = i0
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        v_across = self.p.v - self.n.v
        return [
            self.L * der(self.i_ind) == v_across,
            self.p.i == self.i_ind,
            self.n.i == -self.i_ind,
        ]

    def initial_conditions(self) -> dict[Variable, float]:
        return {self.i_ind: self._i0}


class Ground(Component):
    """
    Electrical ground: v = 0. Current is free (reaction).
    """

    def __init__(self, **kwargs):
        self.pin = Pin(prefix=f"gnd_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.pin.v == Constant(0.0),
        ]


class VoltageSource(Component):
    """
    Ideal voltage source: v_p - v_n = V (constant).

    Sign convention: positive terminal is higher voltage.
    Current flows from p through the external circuit to n.
    """

    def __init__(self, V: float = 1.0, **kwargs):
        self.V = V
        self.p = Pin(prefix=f"vsrc_p_{id(self):#x}")
        self.n = Pin(prefix=f"vsrc_n_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.p.v - self.n.v == Constant(self.V),
            # Current balance: what flows into p flows out of n
            self.p.i == -self.n.i,
        ]


class CurrentSource(Component):
    """
    Ideal current source: i = I (constant, from p to n through external circuit).
    """

    def __init__(self, I: float = 1.0, **kwargs):
        self.I = I
        self.p = Pin(prefix=f"isrc_p_{id(self):#x}")
        self.n = Pin(prefix=f"isrc_n_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.p.i == Constant(-self.I),
            self.n.i == Constant(self.I),
        ]


class IdealTransformer(Component):
    """
    Ideal transformer: v1/v2 = n, i1*n + i2 = 0

    n = turns ratio (primary:secondary).
    Primary: p1/n1, Secondary: p2/n2.
    Power balance: v1*i1 + v2*i2 = 0 (lossless).
    """

    def __init__(self, n: float = 1.0, **kwargs):
        self.n_ratio = n
        self.p1 = Pin(prefix=f"xfmr_p1_{id(self):#x}")
        self.n1 = Pin(prefix=f"xfmr_n1_{id(self):#x}")
        self.p2 = Pin(prefix=f"xfmr_p2_{id(self):#x}")
        self.n2 = Pin(prefix=f"xfmr_n2_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        v1 = self.p1.v - self.n1.v
        v2 = self.p2.v - self.n2.v
        return [
            # Voltage ratio: v1 = n * v2
            v1 == self.n_ratio * v2,
            # Current balance (power conservation): n * i1 + i2 = 0
            self.n_ratio * self.p1.i + self.p2.i == Constant(0.0),
            # Internal current continuity
            self.n1.i == -self.p1.i,
            self.n2.i == -self.p2.i,
        ]
