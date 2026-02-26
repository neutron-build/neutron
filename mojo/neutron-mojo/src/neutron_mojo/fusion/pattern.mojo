# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Pattern Matching for E-Graph Rewrites
# ===----------------------------------------------------------------------=== #

"""Pattern matching for e-graph rewrite rules.

Patterns represent expression templates with variables (e.g., `(add ?x 0)`).
Matching extracts variable bindings from e-graph nodes. This enables
declarative rewrite rules for equality saturation.
"""

from collections import List, Dict, Optional
from .graph import OpKind
from .eclass import ClassId
from .egraph import EGraph, CanonicalNode

# ===----------------------------------------------------------------------=== #
# PatternKind — Pattern node types
# ===----------------------------------------------------------------------=== #

struct PatternKind(Writable, TrivialRegisterPassable):
    """Enum for pattern node types."""
    var _value: Int

    # Variable pattern: matches any e-class and binds to a variable
    comptime Var = PatternKind(0)

    # Constant pattern: matches only a specific e-class
    comptime Const = PatternKind(1)

    # Operation pattern: matches an operation with sub-patterns as inputs
    comptime Op = PatternKind(2)

    @implicit
    fn __init__(out self, value: Int):
        self._value = value

    fn __eq__(self, other: PatternKind) -> Bool:
        return self._value == other._value

    fn __ne__(self, other: PatternKind) -> Bool:
        return self._value != other._value

    fn write_to[W: Writer](self, mut writer: W):
        if self._value == 0:
            writer.write("Var")
        elif self._value == 1:
            writer.write("Const")
        elif self._value == 2:
            writer.write("Op")
        else:
            writer.write("Unknown")


# ===----------------------------------------------------------------------=== #
# Pattern — Pattern node with optional operation and sub-patterns
# ===----------------------------------------------------------------------=== #

struct Pattern(Copyable, Movable):
    """A pattern node for matching against e-graph nodes.

    Patterns can be:
    - Variable: `?x` - matches any e-class, binds to variable
    - Constant: matches a specific e-class ID
    - Operation: `(add ?x ?y)` - matches an operation with sub-patterns
    """
    var kind: PatternKind
    var var_id: Int  # For Var: variable index (0, 1, 2, ...), -1 otherwise
    var class_id: Int  # For Const: the ClassId value, -1 otherwise
    var op: OpKind  # For Op: the operation kind
    var children: List[Pattern]  # For Op: sub-patterns

    fn __init__(out self, kind: PatternKind):
        """Create a pattern node."""
        self.kind = kind
        self.var_id = -1
        self.class_id = -1
        self.op = OpKind(0)
        self.children = List[Pattern]()

    fn __copyinit__(out self, existing: Self):
        self.kind = existing.kind
        self.var_id = existing.var_id
        self.class_id = existing.class_id
        self.op = existing.op
        self.children = List[Pattern]()
        for i in range(len(existing.children)):
            self.children.append(existing.children[i].copy())

    fn copy(self) -> Pattern:
        """Return a deep copy of this pattern."""
        var p = Pattern(self.kind)
        p.var_id = self.var_id
        p.class_id = self.class_id
        p.op = self.op
        for i in range(len(self.children)):
            p.children.append(self.children[i].copy())
        return p^

    @staticmethod
    fn variable(var_id: Int) -> Pattern:
        """Create a variable pattern (e.g., `?x`)."""
        var p = Pattern(PatternKind.Var)
        p.var_id = var_id
        return p^

    @staticmethod
    fn constant(class_id: ClassId) -> Pattern:
        """Create a constant pattern matching a specific e-class."""
        var p = Pattern(PatternKind.Const)
        p.class_id = class_id.id()
        return p^

    @staticmethod
    fn operation(op: OpKind) -> Pattern:
        """Create an operation pattern (e.g., `(add ?x ?y)`)."""
        var p = Pattern(PatternKind.Op)
        p.op = op
        return p^

    fn add_child(mut self, var child: Pattern):
        """Add a sub-pattern to this operation pattern."""
        self.children.append(child^)


# ===----------------------------------------------------------------------=== #
# Bindings — Variable bindings from pattern matching
# ===----------------------------------------------------------------------=== #

struct Bindings(Copyable, Movable):
    """Variable bindings from pattern matching.

    Maps variable indices to ClassIds. For example, if pattern `(add ?x ?y)`
    matches node `(add c1 c2)`, bindings would be `{0: c1, 1: c2}`.
    """
    var _bindings: List[Int]  # var_id -> ClassId.id(), -1 if unbound

    fn __init__(out self, num_vars: Int):
        """Create bindings for num_vars variables, all initially unbound."""
        self._bindings = List[Int]()
        for _ in range(num_vars):
            self._bindings.append(-1)

    fn __copyinit__(out self, existing: Self):
        self._bindings = existing._bindings.copy()

    fn bind(mut self, var_id: Int, class_id: ClassId) raises:
        """Bind a variable to an e-class.

        Raises error if variable is already bound to a different class.
        """
        if var_id < 0 or var_id >= len(self._bindings):
            raise Error("Variable index out of bounds")

        var current = self._bindings[var_id]
        if current == -1:
            # Not yet bound, bind it
            self._bindings[var_id] = class_id.id()
        elif current != class_id.id():
            # Already bound to a different class - match fails
            raise Error("Variable already bound to different class")
        # else: already bound to same class, OK

    fn get(self, var_id: Int) raises -> ClassId:
        """Get the ClassId bound to a variable.

        Raises error if variable is not bound.
        """
        if var_id < 0 or var_id >= len(self._bindings):
            raise Error("Variable index out of bounds")
        var cid = self._bindings[var_id]
        if cid == -1:
            raise Error("Variable not bound")
        return ClassId(cid)

    fn is_bound(self, var_id: Int) -> Bool:
        """Check if a variable is bound."""
        if var_id < 0 or var_id >= len(self._bindings):
            return False
        return self._bindings[var_id] != -1


# ===----------------------------------------------------------------------=== #
# Helper Functions — Pattern matching
# ===----------------------------------------------------------------------=== #

fn match_pattern(
    pattern: Pattern,
    class_id: ClassId,
    mut bindings: Bindings,
) raises -> Bool:
    """Match a pattern against an e-class (without EGraph — Var/Const only).

    Args:
        pattern: The pattern to match.
        class_id: The e-class to match against.
        bindings: Variable bindings (modified in-place on success).

    Returns:
        True if pattern matches, False otherwise.

    Raises:
        Error if binding conflicts occur.
    """
    if pattern.kind == PatternKind.Var:
        bindings.bind(pattern.var_id, class_id)
        return True

    elif pattern.kind == PatternKind.Const:
        return class_id.id() == pattern.class_id

    elif pattern.kind == PatternKind.Op:
        raise Error("Op pattern matching requires EGraph — use match_pattern_egraph()")

    return False


fn match_pattern_egraph(
    pattern: Pattern,
    class_id: ClassId,
    mut bindings: Bindings,
    mut egraph: EGraph,
) raises -> Bool:
    """Match a pattern against an e-class using full EGraph access.

    For Op patterns, looks up the canonical node in the e-class and
    recursively matches children against sub-patterns.

    Args:
        pattern: The pattern to match.
        class_id: The e-class to match against.
        bindings: Variable bindings (modified in-place on success).
        egraph: The e-graph for looking up nodes.

    Returns:
        True if pattern matches, False otherwise.
    """
    if pattern.kind == PatternKind.Var:
        bindings.bind(pattern.var_id, class_id)
        return True

    elif pattern.kind == PatternKind.Const:
        return class_id.id() == pattern.class_id

    elif pattern.kind == PatternKind.Op:
        # Find canonical class
        var canon_id = egraph.find(class_id)

        # Search all nodes in the e-graph for ones belonging to this class
        for node_idx in range(egraph.num_nodes()):
            # Check if this node's class matches
            if node_idx >= len(egraph.classes):
                continue
            var node_class = egraph.find(egraph.classes[node_idx].id)
            if node_class != canon_id:
                continue

            var node = egraph.nodes[node_idx].copy()

            # Check op matches
            if node.op != pattern.op:
                continue

            # Check arity matches
            if len(node.inputs) != len(pattern.children):
                continue

            # Recursively match children
            var child_bindings = bindings.copy()
            var all_match = True
            for i in range(len(pattern.children)):
                var child_class = egraph.find(node.inputs[i])
                if not match_pattern_egraph(
                    pattern.children[i], child_class, child_bindings, egraph
                ):
                    all_match = False
                    break

            if all_match:
                # Copy successful bindings back
                bindings = child_bindings^
                return True

    return False
