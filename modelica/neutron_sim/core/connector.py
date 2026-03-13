"""Connector types for acausal component modeling."""

from __future__ import annotations
from dataclasses import dataclass, field
from .variable import Variable


class Connector:
    """
    Base class for connectors (ports).

    Each connector instance has its own Variable objects.
    Across variables (effort) are equalized when connected.
    Through variables (flow) sum to zero (Kirchhoff's law).
    """

    # Subclasses declare which variables are "across" (effort) and "through" (flow)
    _across: tuple[str, ...] = ()
    _through: tuple[str, ...] = ()

    def __init__(self, prefix: str = ""):
        # Create unique Variable instances for each connector variable
        for attr in self._across + self._through:
            object.__setattr__(self, attr, Variable(f"{prefix}.{attr}" if prefix else attr))

    def across_vars(self) -> list[Variable]:
        return [getattr(self, a) for a in self._across]

    def through_vars(self) -> list[Variable]:
        return [getattr(self, t) for t in self._through]
