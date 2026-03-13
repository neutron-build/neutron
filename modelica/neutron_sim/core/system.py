"""System assembly: flatten component equations and prepare for simulation."""

from __future__ import annotations
from typing import TYPE_CHECKING

from .variable import Variable, Equation, Expr, Der, BinOp, Constant, UnaryOp
from .connector import Connector

if TYPE_CHECKING:
    from .component import Component


class Connection:
    """A multi-way connection between two or more same-type connectors."""

    def __init__(self, connectors: list[Connector]):
        if len(connectors) < 2:
            raise ValueError("connect() requires at least 2 connectors")
        types = {type(c) for c in connectors}
        if len(types) > 1:
            raise TypeError(f"Cannot connect incompatible connector types: {types}")
        self.connectors = connectors


def connect(*connectors: Connector) -> Connection:
    """Connect two or more compatible connectors."""
    return Connection(list(connectors))


# ── substitution helpers ─────────────────────────────────────────────────────

def _build_subs(eqs: list[Equation], protected: set[int] | None = None) -> dict[Variable, Expr]:
    """
    Scan equations for var == expr where NEITHER side has any derivatives,
    and return a substitution map.  State variables (in protected) are never
    substituted away.
    """
    if protected is None:
        protected = _protected_ids(eqs)

    subs: dict[Variable, Expr] = {}
    for eq in eqs:
        lhs, rhs = eq.lhs, eq.rhs
        if lhs.derivatives() or rhs.derivatives():
            continue
        if isinstance(lhs, Variable) and id(lhs) not in protected:
            subs[lhs] = rhs
        elif isinstance(rhs, Variable) and id(rhs) not in protected:
            subs[rhs] = lhs
    return subs


def _apply_subs_once(
    eqs: list[Equation], subs: dict[Variable, Expr]
) -> list[Equation]:
    return [Equation(eq.lhs.substitute(subs), eq.rhs.substitute(subs)) for eq in eqs]


def _decompose_linear(
    expr: Expr, target: Variable
) -> tuple[Expr, float] | None:
    """
    Decompose `expr` into `(rest, coeff)` such that `expr = rest + coeff * target`,
    where `rest` contains no reference to `target`.

    Returns None if `expr` is not linear in `target` (e.g., appears in multiplication
    where the other factor also contains `target`).
    """
    if isinstance(expr, Variable):
        if expr is target:
            return (Constant(0.0), 1.0)
        return (expr, 0.0)

    if isinstance(expr, Constant):
        return (expr, 0.0)

    if isinstance(expr, Der):
        # target should never be inside Der after our substitution passes
        return (expr, 0.0)

    if isinstance(expr, UnaryOp) and expr.op == "-":
        sub = _decompose_linear(expr.operand, target)
        if sub is None:
            return None
        rest, coeff = sub
        return (-rest, -coeff)

    if isinstance(expr, BinOp):
        if expr.op in ("+", "-"):
            left = _decompose_linear(expr.left, target)
            right = _decompose_linear(expr.right, target)
            if left is None or right is None:
                return None
            rl, cl = left
            rr, cr = right
            if expr.op == "+":
                return (rl + rr, cl + cr)
            else:
                return (rl - rr, cl - cr)

        if expr.op == "*":
            # Linear only if target appears in exactly one side and the other
            # is a constant (no Variables at all, evaluates to a number).
            l_has = target in expr.left.variables()
            r_has = target in expr.right.variables()
            if l_has and r_has:
                return None  # quadratic — not supported
            if l_has:
                try:
                    scale = expr.right.eval({})  # must be a pure constant
                    sub = _decompose_linear(expr.left, target)
                    if sub is None:
                        return None
                    rest, coeff = sub
                    return (rest * expr.right, coeff * scale)
                except KeyError:
                    return None  # right has variables
            if r_has:
                try:
                    scale = expr.left.eval({})
                    sub = _decompose_linear(expr.right, target)
                    if sub is None:
                        return None
                    rest, coeff = sub
                    return (expr.left * rest, coeff * scale)
                except KeyError:
                    return None
            # Neither side has target
            return (expr, 0.0)

        if expr.op == "/":
            l_has = target in expr.left.variables()
            r_has = target in expr.right.variables()
            if r_has:
                return None  # target in denominator — non-linear
            if l_has:
                try:
                    scale = 1.0 / expr.right.eval({})
                    sub = _decompose_linear(expr.left, target)
                    if sub is None:
                        return None
                    rest, coeff = sub
                    return (rest / expr.right, coeff * scale)
                except (KeyError, ZeroDivisionError):
                    return None
            return (expr, 0.0)

    return None  # unknown expression type


def _solve_linear_unknowns(
    eqs: list[Equation], protected_ids: set[int]
) -> dict[Variable, Expr]:
    """
    For each pure-algebraic equation (no derivatives), attempt to solve for
    variables that are not protected (not state variables).

    Handles equations like:  k*x + c*v + mass_f == 0
    where x, v are protected (state vars) and mass_f is the unknown.
    """
    subs: dict[Variable, Expr] = {}
    for eq in eqs:
        if eq.lhs.derivatives() or eq.rhs.derivatives():
            continue
        # Combine into residual form: lhs - rhs == 0
        # We look at all unprotected variables in the equation
        all_vars = eq.lhs.variables() | eq.rhs.variables()
        unknowns = [v for v in all_vars if id(v) not in protected_ids]
        if len(unknowns) != 1:
            continue
        target = unknowns[0]

        # Build residual: lhs - rhs
        residual = eq.lhs - eq.rhs

        decomp = _decompose_linear(residual, target)
        if decomp is None or decomp[1] == 0.0:
            continue
        rest, coeff = decomp
        # target = -rest / coeff
        if coeff == 1.0:
            subs[target] = -rest
        elif coeff == -1.0:
            subs[target] = rest
        else:
            subs[target] = BinOp("/", -rest, Constant(coeff))

    return subs


def _apply_subs_fixed_point(
    eqs: list[Equation], max_rounds: int = 20
) -> list[Equation]:
    """
    Iteratively apply variable-equality substitutions until stable.
    Handles:
    - Direct equality: var == expr
    - Linear sum equations: expr + var == 0  (via _solve_linear_unknowns)
    Runs both passes in each round until convergence.
    """
    for _ in range(max_rounds):
        protected = _protected_ids(eqs)
        subs = _build_subs(eqs, protected)
        subs.update(_solve_linear_unknowns(eqs, protected))
        if not subs:
            break
        new_eqs = _apply_subs_once(eqs, subs)
        if all(repr(n) == repr(o) for n, o in zip(new_eqs, eqs)):
            break
        eqs = new_eqs
    return eqs


def _protected_ids(eqs: list[Equation]) -> set[int]:
    """Variables that appear inside any der() — these are state variables."""
    protected: set[int] = set()
    for eq in eqs:
        for v in eq.lhs.derivatives() | eq.rhs.derivatives():
            protected.add(id(v))
    return protected


def _build_der_subs(eqs: list[Equation]) -> dict[Variable, Variable]:
    """
    Find equations of the form  der(x) == v  (where v is a Variable),
    and return {x: v} so that Der(x) nodes can be replaced by Variable(v).
    This handles the Damper pattern where der(flange.x) needs to become the
    connected mass's velocity variable.
    """
    der_subs: dict[Variable, Variable] = {}
    for eq in eqs:
        if isinstance(eq.lhs, Der) and isinstance(eq.rhs, Variable):
            der_subs[eq.lhs.variable] = eq.rhs
        elif isinstance(eq.rhs, Der) and isinstance(eq.lhs, Variable):
            der_subs[eq.rhs.variable] = eq.lhs
    return der_subs


def _apply_der_subs(eqs: list[Equation], der_subs: dict[Variable, Variable]) -> list[Equation]:
    """
    Replace Der(x) with Variable(v) wherever der_subs[x] = v.
    Used to substitute der(position) → velocity throughout the system.
    """
    if not der_subs:
        return eqs

    class _DerSubstituter:
        """Walks an expression tree and replaces Der nodes per der_subs."""
        def __init__(self, mapping):
            self.mapping = mapping

        def visit(self, expr: Expr) -> Expr:
            if isinstance(expr, Der):
                v = self.mapping.get(expr.variable)
                return v if v is not None else expr
            if isinstance(expr, BinOp):
                return BinOp(expr.op, self.visit(expr.left), self.visit(expr.right))
            if isinstance(expr, UnaryOp):
                return UnaryOp(expr.op, self.visit(expr.operand))
            return expr

    visitor = _DerSubstituter(der_subs)
    return [Equation(visitor.visit(eq.lhs), visitor.visit(eq.rhs)) for eq in eqs]


# ── System ───────────────────────────────────────────────────────────────────

class System:
    """
    Assembled system of components and connections.

    Flattens all component equations, adds multi-way connection equations:
    - Across variables are equalized: x_1 == x_2 == x_3
    - Through variables sum to zero: f_1 + f_2 + ... + f_n == 0

    Then applies variable-equality substitutions to eliminate algebraic variables.
    Only handles simple ODE systems. For DAEs, use the Julia bridge.
    """

    def __init__(
        self,
        components: list,
        connections: list[Connection] | None = None,
        initial_conditions: dict[Variable, float] | None = None,
    ):
        self.components = components
        self.connections = connections or []
        self._user_ics: dict[Variable, float] = initial_conditions or {}
        self._equations: list[Equation] | None = None

    def flatten(self) -> list[Equation]:
        """Collect + connection equations + substitute algebraic vars."""
        if self._equations is not None:
            return self._equations

        eqs: list[Equation] = []

        # Component equations
        for comp in self.components:
            eqs.extend(comp.equations())

        # Multi-way connection equations
        for conn in self.connections:
            cs = conn.connectors
            first = cs[0]

            # Across: equalize all to first connector
            for other in cs[1:]:
                for va_first, va_other in zip(first.across_vars(), other.across_vars()):
                    eqs.append(va_first == va_other)

            # Through: Kirchhoff — sum of all == 0
            n_through = len(first.through_vars())
            for slot in range(n_through):
                tvars = [c.through_vars()[slot] for c in cs]
                total: Expr = tvars[0]
                for tv in tvars[1:]:
                    total = total + tv
                eqs.append(total == Constant(0.0))

        # Apply variable-equality substitutions iteratively
        # (handles both direct var==expr and linear sum equations)
        eqs = _apply_subs_fixed_point(eqs)

        self._equations = eqs
        return eqs

    def initial_conditions(self) -> dict[Variable, float]:
        """Merge user-supplied ICs with component defaults."""
        ics: dict[Variable, float] = {}
        for comp in self.components:
            ics.update(comp.initial_conditions())
        ics.update(self._user_ics)
        return ics

    def state_variables(self) -> list[Variable]:
        """
        Identify state variables: those that appear inside der() after flattening.
        Returns them in stable insertion order.
        """
        seen: dict[int, Variable] = {}
        for eq in self.flatten():
            for v in eq.derivatives():
                seen[id(v)] = v
        return list(seen.values())
