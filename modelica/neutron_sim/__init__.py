"""neutron_sim — Python orchestration layer for equation-based physics simulation."""

from .core.variable import Variable, Parameter, Equation, der
from .core.connector import Connector
from .core.component import Component
from .core.system import System, connect
from .solvers.ode import simulate, SimulationResult

__all__ = [
    "Variable", "Parameter", "Equation", "der",
    "Connector", "Component",
    "System", "connect",
    "simulate", "SimulationResult",
]
