"""Component base class for acausal component modeling."""

from __future__ import annotations
from .variable import Equation


class Component:
    """
    Base class for all model components.

    Subclasses define parameters as class-level annotations with defaults
    and override equations() to return a list of Equation objects.
    """

    def __init__(self, **kwargs):
        # Apply parameter values passed at construction time
        for key, val in kwargs.items():
            if not hasattr(self, key):
                raise AttributeError(f"{type(self).__name__} has no parameter '{key}'")
            setattr(self, key, val)

    def equations(self) -> list[Equation]:
        """Return the list of equations this component contributes."""
        return []

    def initial_conditions(self) -> dict:
        """Return a dict of {Variable: float} for default ICs (override as needed)."""
        return {}
