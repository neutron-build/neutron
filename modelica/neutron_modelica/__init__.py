"""neutron_modelica — Extended physics simulation: FMI runtime, Kirchhoff solvers, Julia bridge."""

from .fmi import FMU, FMUVariable as FMIVariable, FMIVersion
from .kirchhoff import (
    CircuitNode,
    Resistor as KResistor,
    Capacitor as KCapacitor,
    Inductor as KInductor,
    VoltageSource as KVoltageSource,
    CurrentSource as KCurrentSource,
    Circuit,
    TransientResult,
)
from .julia_bridge import JuliaDEBridge, DEResult

__all__ = [
    "FMU", "FMIVariable", "FMIVersion",
    "CircuitNode", "KResistor", "KCapacitor", "KInductor",
    "KVoltageSource", "KCurrentSource", "Circuit", "TransientResult",
    "JuliaDEBridge", "DEResult",
]
