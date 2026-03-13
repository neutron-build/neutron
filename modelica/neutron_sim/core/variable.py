"""Variable, Parameter, Equation, and der() — the core modeling primitives."""

from __future__ import annotations
import math
from typing import Any


class Expr:
    """Base class for all symbolic expressions (kept minimal — no SymPy)."""

    def __add__(self, other) -> BinOp:
        return BinOp("+", self, _wrap(other))

    def __radd__(self, other) -> BinOp:
        return BinOp("+", _wrap(other), self)

    def __sub__(self, other) -> BinOp:
        return BinOp("-", self, _wrap(other))

    def __rsub__(self, other) -> BinOp:
        return BinOp("-", _wrap(other), self)

    def __mul__(self, other) -> BinOp:
        return BinOp("*", self, _wrap(other))

    def __rmul__(self, other) -> BinOp:
        return BinOp("*", _wrap(other), self)

    def __truediv__(self, other) -> BinOp:
        return BinOp("/", self, _wrap(other))

    def __rtruediv__(self, other) -> BinOp:
        return BinOp("/", _wrap(other), self)

    def __neg__(self) -> UnaryOp:
        return UnaryOp("-", self)

    def __eq__(self, other) -> "Equation":  # type: ignore[override]
        return Equation(self, _wrap(other))

    def __hash__(self) -> int:
        return id(self)

    def eval(self, state: dict[str, float]) -> float:
        raise NotImplementedError

    def variables(self) -> set["Variable"]:
        """Return all Variable nodes referenced in this expression."""
        return set()

    def derivatives(self) -> set["Variable"]:
        """Return all Variable nodes that appear inside der()."""
        return set()

    def substitute(self, mapping: dict["Variable", "Expr"]) -> "Expr":
        """Return a new expression with variables replaced."""
        return self


class Constant(Expr):
    def __init__(self, value: float):
        self.value = value

    def eval(self, state: dict[str, float]) -> float:
        return self.value

    def substitute(self, mapping):
        return self

    def __repr__(self) -> str:
        return str(self.value)


class Variable(Expr):
    """A symbolic unknown quantity (state or algebraic)."""

    def __init__(self, name: str):
        self.name = name

    def eval(self, state: dict[str, float]) -> float:
        if self.name not in state:
            raise KeyError(f"Variable '{self.name}' not found in state")
        return state[self.name]

    def variables(self) -> set["Variable"]:
        return {self}

    def substitute(self, mapping: dict["Variable", "Expr"]) -> "Expr":
        result = mapping.get(self, self)
        if result is not self:
            # Recursively substitute inside the replacement to resolve chains
            # Guard: don't recurse if the replacement maps back to the same var
            # (cycle protection: remove self from mapping before recursing)
            inner = {k: v for k, v in mapping.items() if k is not self}
            return result.substitute(inner) if inner else result
        return result

    def __repr__(self) -> str:
        return self.name


class Parameter(Expr):
    """A known constant value in a model."""

    def __init__(self, name: str, value: float = 0.0):
        self.name = name
        self.value = value

    def eval(self, state: dict[str, float]) -> float:
        return self.value

    def substitute(self, mapping):
        return self

    def __repr__(self) -> str:
        return f"{self.name}={self.value}"


class Der(Expr):
    """Time derivative of a variable: der(x) represents dx/dt."""

    def __init__(self, variable: Variable):
        if not isinstance(variable, Variable):
            raise TypeError(f"der() requires a Variable, got {type(variable)}")
        self.variable = variable

    def eval(self, state: dict[str, float]) -> float:
        key = f"d{self.variable.name}"
        if key not in state:
            raise KeyError(f"Derivative 'd{self.variable.name}' not found in state")
        return state[key]

    def variables(self) -> set[Variable]:
        return {self.variable}

    def derivatives(self) -> set[Variable]:
        return {self.variable}

    def substitute(self, mapping: dict[Variable, Expr]) -> Expr:
        mapped = mapping.get(self.variable)
        if mapped is None:
            return self
        if isinstance(mapped, Constant):
            return Constant(0.0)   # der(constant) = 0
        if isinstance(mapped, Variable):
            return Der(mapped)     # der(alias) = der(new_var)
        # Complex expression — cannot compute symbolic derivative here
        return self

    def __repr__(self) -> str:
        return f"der({self.variable.name})"


class BinOp(Expr):
    def __init__(self, op: str, left: Expr, right: Expr):
        self.op = op
        self.left = left
        self.right = right

    def eval(self, state: dict[str, float]) -> float:
        l = self.left.eval(state)
        r = self.right.eval(state)
        if self.op == "+":
            return l + r
        if self.op == "-":
            return l - r
        if self.op == "*":
            return l * r
        if self.op == "/":
            return l / r
        raise ValueError(f"Unknown op: {self.op}")

    def variables(self) -> set[Variable]:
        return self.left.variables() | self.right.variables()

    def derivatives(self) -> set[Variable]:
        return self.left.derivatives() | self.right.derivatives()

    def substitute(self, mapping):
        return BinOp(self.op, self.left.substitute(mapping), self.right.substitute(mapping))

    def __repr__(self) -> str:
        return f"({self.left} {self.op} {self.right})"


class UnaryOp(Expr):
    def __init__(self, op: str, operand: Expr):
        self.op = op
        self.operand = operand

    def eval(self, state: dict[str, float]) -> float:
        v = self.operand.eval(state)
        if self.op == "-":
            return -v
        raise ValueError(f"Unknown unary op: {self.op}")

    def variables(self) -> set[Variable]:
        return self.operand.variables()

    def derivatives(self) -> set[Variable]:
        return self.operand.derivatives()

    def substitute(self, mapping):
        return UnaryOp(self.op, self.operand.substitute(mapping))

    def __repr__(self) -> str:
        return f"({self.op}{self.operand})"


class Equation:
    """lhs == rhs, i.e., lhs - rhs = 0."""

    def __init__(self, lhs: Expr, rhs: Expr):
        self.lhs = lhs
        self.rhs = rhs

    def residual(self, state: dict[str, float]) -> float:
        """Return lhs - rhs evaluated at state."""
        return self.lhs.eval(state) - self.rhs.eval(state)

    def variables(self) -> set[Variable]:
        return self.lhs.variables() | self.rhs.variables()

    def derivatives(self) -> set[Variable]:
        return self.lhs.derivatives() | self.rhs.derivatives()

    def substitute(self, mapping: dict[Variable, Expr]) -> "Equation":
        return Equation(self.lhs.substitute(mapping), self.rhs.substitute(mapping))

    def __repr__(self) -> str:
        return f"{self.lhs} == {self.rhs}"


def der(variable: Variable) -> Der:
    """Create a time-derivative expression: der(x) represents dx/dt."""
    return Der(variable)


def _wrap(value: Any) -> Expr:
    """Convert a plain number to a Constant, pass Expr through."""
    if isinstance(value, Expr):
        return value
    return Constant(float(value))
