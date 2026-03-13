"""
Simple mechanical translational components.

Keeps only: Mass, Spring, Damper, Fixed, Force.
Educational use only — not a full Modelica standard library.
For multi-domain or complex models, use the Julia bridge.

Design note: Flange carries both x (position) and v (velocity) as across variables,
so Damper can use velocity directly without der() on connector variables. This keeps
the system in explicit ODE form, required for SciPy solve_ivp.
"""

from __future__ import annotations
from ...core.variable import Variable, Equation, der, Constant
from ...core.connector import Connector
from ...core.component import Component


class Flange(Connector):
    """
    Mechanical translational flange (1D).

    x: position [m]   — across (equalized at connections)
    v: velocity [m/s] — across (equalized at connections)
    f: force [N]      — through (Kirchhoff sum = 0 at connections)
    """
    _across = ("x", "v")
    _through = ("f",)

    def __init__(self, prefix: str = ""):
        super().__init__(prefix)


class Mass(Component):
    """
    Point mass: m * der(v) = f_net

    State variables: x (position), v (velocity)
    Port: flange (the attachment point)
    """

    def __init__(self, m: float = 1.0, **kwargs):
        self.m = m
        self.x = Variable(f"x_{id(self):#x}")
        self.v = Variable(f"v_{id(self):#x}")
        self.flange = Flange(prefix=f"mass_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            der(self.x) == self.v,
            self.m * der(self.v) == self.flange.f,
            self.flange.x == self.x,
            self.flange.v == self.v,
        ]

    def initial_conditions(self) -> dict[Variable, float]:
        return {self.x: 0.0, self.v: 0.0}


class Spring(Component):
    """
    Linear spring: f = k * (x_a - x_b)
    """

    def __init__(self, k: float = 1.0, **kwargs):
        self.k = k
        self.flange_a = Flange(prefix=f"spring_a_{id(self):#x}")
        self.flange_b = Flange(prefix=f"spring_b_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        dx = self.flange_a.x - self.flange_b.x
        f = self.k * dx
        return [
            self.flange_a.f == f,
            self.flange_b.f == -f,
        ]


class Damper(Component):
    """
    Linear viscous damper: f = c * (v_a - v_b)

    Uses flange velocity directly — no der() on connector variables.
    This keeps the system in explicit ODE form.
    """

    def __init__(self, c: float = 1.0, **kwargs):
        self.c = c
        self.flange_a = Flange(prefix=f"damper_a_{id(self):#x}")
        self.flange_b = Flange(prefix=f"damper_b_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        dv = self.flange_a.v - self.flange_b.v
        f = self.c * dv
        return [
            self.flange_a.f == f,
            self.flange_b.f == -f,
        ]


class Fixed(Component):
    """
    Fixed ground: x = 0, v = 0. Reaction force is free.
    """

    def __init__(self, **kwargs):
        self.flange = Flange(prefix=f"fixed_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        return [
            self.flange.x == Constant(0.0),
            self.flange.v == Constant(0.0),
        ]


class Force(Component):
    """
    Ideal constant force source: applies force F to the attached load.

    Sign convention (Modelica-style): the flange "pushes" F into the network.
    At a connection: force.flange.f + mass.flange.f == 0
    → mass.flange.f = -force.flange.f = F (positive = accelerates mass in +x).
    We set force.flange.f = -F so Kirchhoff delivers +F to the mass.
    """

    def __init__(self, F: float = 0.0, **kwargs):
        self.F = F
        self.flange = Flange(prefix=f"force_{id(self):#x}")
        super().__init__(**kwargs)

    def equations(self) -> list[Equation]:
        # flange.f = -F so that Kirchhoff gives mass.flange.f = F
        return [
            self.flange.f == Constant(-self.F),
        ]
