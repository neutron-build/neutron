# ===----------------------------------------------------------------------=== #
# Neutron Mojo — E-Graph Rewrite Rules
# ===----------------------------------------------------------------------=== #

"""Algebraic rewrite rules for e-graph equality saturation.

Implements the rule catalog from egraph_rules.md. Each rule transforms
expression patterns into equivalent forms. Rules are categorized by phase:
- Phase 1: Directed simplifications (always profitable)
- Phase 2: Bidirectional equality saturation (explore equivalent forms)

Reference: tystack/mojo/specs/egraph_rules.md
"""

from collections import List
from .graph import OpKind
from .pattern import Pattern, PatternKind
from .eclass import ClassId

# ===----------------------------------------------------------------------=== #
# RulePriority — Priority levels for rule application
# ===----------------------------------------------------------------------=== #

struct RulePriority(TrivialRegisterPassable):
    """Priority level for rewrite rules."""
    var _value: Int

    comptime High = RulePriority(2)
    comptime Medium = RulePriority(1)
    comptime Low = RulePriority(0)

    @implicit
    fn __init__(out self, value: Int):
        self._value = value

    fn __eq__(self, other: RulePriority) -> Bool:
        return self._value == other._value


# ===----------------------------------------------------------------------=== #
# RewriteRule — A single rewrite rule (pattern -> replacement)
# ===----------------------------------------------------------------------=== #

struct RewriteRule(Copyable, Movable):
    """A rewrite rule for equality saturation.

    Represents a transformation: `lhs -> rhs` where both are patterns.
    For example: `(add ?x 0) -> ?x` (additive identity).
    """
    var name: String
    var phase: Int  # 1 = simplification, 2 = saturation
    var priority: RulePriority
    var lhs: Pattern  # Left-hand side pattern to match
    var rhs: Pattern  # Right-hand side pattern (replacement)

    fn __init__(out self, name: String, phase: Int, priority: RulePriority, var lhs: Pattern, var rhs: Pattern):
        self.name = name
        self.phase = phase
        self.priority = priority
        self.lhs = lhs^
        self.rhs = rhs^

    fn __copyinit__(out self, existing: Self):
        self.name = existing.name
        self.phase = existing.phase
        self.priority = existing.priority
        self.lhs = existing.lhs.copy()
        self.rhs = existing.rhs.copy()

    fn copy(self) -> RewriteRule:
        """Return a deep copy of this rewrite rule."""
        return RewriteRule(self.name, self.phase, self.priority, self.lhs.copy(), self.rhs.copy())


# ===----------------------------------------------------------------------=== #
# Rule Constructors — Factory functions for common rules
# ===----------------------------------------------------------------------=== #

fn rule_add_identity() -> RewriteRule:
    """Rule 1: Add Identity — (add ?x 0) -> ?x

    Category: Identity
    Phase: 1 (simplification)
    Priority: High
    """
    # Pattern: (add ?x 0)
    var lhs = Pattern.operation(OpKind.Add)
    lhs.add_child(Pattern.variable(0)^)  # ?x
    lhs.add_child(Pattern.constant(ClassId(0))^)  # Assume 0 is ClassId(0)

    # Replacement: ?x
    var rhs = Pattern.variable(0)

    return RewriteRule("add_identity", 1, RulePriority.High, lhs^, rhs^)


fn rule_mul_identity() -> RewriteRule:
    """Rule 2: Mul Identity — (mul ?x 1) -> ?x

    Category: Identity
    Phase: 1
    Priority: High
    """
    var lhs = Pattern.operation(OpKind.Mul)
    lhs.add_child(Pattern.variable(0)^)  # ?x
    lhs.add_child(Pattern.constant(ClassId(1))^)  # Assume 1 is ClassId(1)

    var rhs = Pattern.variable(0)

    return RewriteRule("mul_identity", 1, RulePriority.High, lhs^, rhs^)


fn rule_mul_zero() -> RewriteRule:
    """Rule 3: Mul Zero — (mul ?x 0) -> 0

    Category: Collapse
    Phase: 1
    Priority: High
    Note: Requires fast-math or ?x known finite (NaN * 0 = NaN in IEEE 754)
    """
    var lhs = Pattern.operation(OpKind.Mul)
    lhs.add_child(Pattern.variable(0)^)  # ?x
    lhs.add_child(Pattern.constant(ClassId(0))^)  # 0

    var rhs = Pattern.constant(ClassId(0))  # 0

    return RewriteRule("mul_zero", 1, RulePriority.High, lhs^, rhs^)


fn rule_transpose_involution() -> RewriteRule:
    """Rule 26: Transpose Involution — (transpose (transpose ?x)) -> ?x

    Category: Cancellation
    Phase: 1
    Priority: High
    """
    # (transpose (transpose ?x))
    var inner_transpose = Pattern.operation(OpKind.Transpose)
    inner_transpose.add_child(Pattern.variable(0)^)  # ?x

    var lhs = Pattern.operation(OpKind.Transpose)
    lhs.add_child(inner_transpose^)

    var rhs = Pattern.variable(0)  # ?x

    return RewriteRule("transpose_involution", 1, RulePriority.High, lhs^, rhs^)


fn rule_add_commutativity() -> RewriteRule:
    """Rule 7: Add Commutativity — (add ?x ?y) -> (add ?y ?x)

    Category: Commutativity
    Phase: 2 (equality saturation)
    Priority: High
    Note: In practice, canonical ordering prevents infinite loops
    """
    # LHS: (add ?x ?y)
    var lhs = Pattern.operation(OpKind.Add)
    lhs.add_child(Pattern.variable(0)^)  # ?x
    lhs.add_child(Pattern.variable(1)^)  # ?y

    # RHS: (add ?y ?x)
    var rhs = Pattern.operation(OpKind.Add)
    rhs.add_child(Pattern.variable(1)^)  # ?y
    rhs.add_child(Pattern.variable(0)^)  # ?x

    return RewriteRule("add_commutativity", 2, RulePriority.High, lhs^, rhs^)


fn rule_mul_commutativity() -> RewriteRule:
    """Rule 8: Mul Commutativity — (mul ?x ?y) -> (mul ?y ?x)

    Category: Commutativity
    Phase: 2
    Priority: High
    """
    var lhs = Pattern.operation(OpKind.Mul)
    lhs.add_child(Pattern.variable(0)^)  # ?x
    lhs.add_child(Pattern.variable(1)^)  # ?y

    var rhs = Pattern.operation(OpKind.Mul)
    rhs.add_child(Pattern.variable(1)^)  # ?y
    rhs.add_child(Pattern.variable(0)^)  # ?x

    return RewriteRule("mul_commutativity", 2, RulePriority.High, lhs^, rhs^)


fn rule_add_associativity() -> RewriteRule:
    """Rule 9: Add Associativity — (add (add ?x ?y) ?z) -> (add ?x (add ?y ?z))

    Category: Associativity
    Phase: 2
    Priority: Medium
    Note: FP addition not truly associative; valid under fast-math
    """
    # LHS: (add (add ?x ?y) ?z)
    var inner_add = Pattern.operation(OpKind.Add)
    inner_add.add_child(Pattern.variable(0)^)  # ?x
    inner_add.add_child(Pattern.variable(1)^)  # ?y

    var lhs = Pattern.operation(OpKind.Add)
    lhs.add_child(inner_add^)
    lhs.add_child(Pattern.variable(2)^)  # ?z

    # RHS: (add ?x (add ?y ?z))
    var inner_add2 = Pattern.operation(OpKind.Add)
    inner_add2.add_child(Pattern.variable(1)^)  # ?y
    inner_add2.add_child(Pattern.variable(2)^)  # ?z

    var rhs = Pattern.operation(OpKind.Add)
    rhs.add_child(Pattern.variable(0)^)  # ?x
    rhs.add_child(inner_add2^)

    return RewriteRule("add_associativity", 2, RulePriority.Medium, lhs^, rhs^)


fn rule_mul_associativity() -> RewriteRule:
    """Rule 10: Mul Associativity — (mul (mul ?x ?y) ?z) -> (mul ?x (mul ?y ?z))

    Category: Associativity
    Phase: 2
    Priority: Medium
    """
    # LHS: (mul (mul ?x ?y) ?z)
    var inner_mul = Pattern.operation(OpKind.Mul)
    inner_mul.add_child(Pattern.variable(0)^)  # ?x
    inner_mul.add_child(Pattern.variable(1)^)  # ?y

    var lhs = Pattern.operation(OpKind.Mul)
    lhs.add_child(inner_mul^)
    lhs.add_child(Pattern.variable(2)^)  # ?z

    # RHS: (mul ?x (mul ?y ?z))
    var inner_mul2 = Pattern.operation(OpKind.Mul)
    inner_mul2.add_child(Pattern.variable(1)^)  # ?y
    inner_mul2.add_child(Pattern.variable(2)^)  # ?z

    var rhs = Pattern.operation(OpKind.Mul)
    rhs.add_child(Pattern.variable(0)^)  # ?x
    rhs.add_child(inner_mul2^)

    return RewriteRule("mul_associativity", 2, RulePriority.Medium, lhs^, rhs^)


# ===----------------------------------------------------------------------=== #
# Fusion Rule Constructors — Transform patterns into fused operations
# ===----------------------------------------------------------------------=== #

fn rule_rmsnorm_matmul_fusion() -> RewriteRule:
    """Fusion: (matmul ?w (rmsnorm ?x ?gamma)) -> (fused_rmsnorm_linear ?x ?gamma ?w)

    Fuses RMSNorm + linear projection into a single pass.
    Eliminates materializing the normalized intermediate vector.
    Phase: 1 (always profitable — saves one full vector write+read)
    Priority: High
    """
    # LHS: (matmul ?w (rmsnorm ?x ?gamma))
    var inner_norm = Pattern.operation(OpKind.RMSNorm)
    inner_norm.add_child(Pattern.variable(0)^)  # ?x
    inner_norm.add_child(Pattern.variable(1)^)  # ?gamma

    var lhs = Pattern.operation(OpKind.Matmul)
    lhs.add_child(Pattern.variable(2)^)  # ?w
    lhs.add_child(inner_norm^)

    # RHS: (fused_rmsnorm_linear ?x ?gamma ?w)
    var rhs = Pattern.operation(OpKind.FusedRMSNormLinear)
    rhs.add_child(Pattern.variable(0)^)  # ?x
    rhs.add_child(Pattern.variable(1)^)  # ?gamma
    rhs.add_child(Pattern.variable(2)^)  # ?w

    return RewriteRule("rmsnorm_matmul_fusion", 1, RulePriority.High, lhs^, rhs^)


fn rule_linear_residual_add_fusion() -> RewriteRule:
    """Fusion: (add ?residual (matmul ?w ?x)) -> (fused_linear_res_add ?residual ?w ?x)

    Fuses linear projection + residual addition into a single pass.
    Eliminates materializing the projection output before adding.
    Phase: 1 (always profitable — saves one vector write+read)
    Priority: High
    """
    # LHS: (add ?residual (matmul ?w ?x))
    var inner_matmul = Pattern.operation(OpKind.Matmul)
    inner_matmul.add_child(Pattern.variable(1)^)  # ?w
    inner_matmul.add_child(Pattern.variable(2)^)  # ?x

    var lhs = Pattern.operation(OpKind.Add)
    lhs.add_child(Pattern.variable(0)^)  # ?residual
    lhs.add_child(inner_matmul^)

    # RHS: (fused_linear_res_add ?residual ?w ?x)
    var rhs = Pattern.operation(OpKind.FusedLinearResAdd)
    rhs.add_child(Pattern.variable(0)^)  # ?residual
    rhs.add_child(Pattern.variable(1)^)  # ?w
    rhs.add_child(Pattern.variable(2)^)  # ?x

    return RewriteRule("linear_residual_add_fusion", 1, RulePriority.High, lhs^, rhs^)


fn rule_swiglu_fusion() -> RewriteRule:
    """Fusion: (mul (silu ?gate) ?up) -> (swiglu ?gate ?up)

    Fuses SiLU activation + element-wise multiply into a single pass.
    Phase: 1 (always profitable — avoids materializing SiLU output)
    Priority: High
    """
    # LHS: (mul (silu ?gate) ?up)
    var inner_silu = Pattern.operation(OpKind.SiLU)
    inner_silu.add_child(Pattern.variable(0)^)  # ?gate

    var lhs = Pattern.operation(OpKind.Mul)
    lhs.add_child(inner_silu^)
    lhs.add_child(Pattern.variable(1)^)  # ?up

    # RHS: (swiglu ?gate ?up)
    var rhs = Pattern.operation(OpKind.SwiGLU)
    rhs.add_child(Pattern.variable(0)^)  # ?gate
    rhs.add_child(Pattern.variable(1)^)  # ?up

    return RewriteRule("swiglu_fusion", 1, RulePriority.High, lhs^, rhs^)


# ===----------------------------------------------------------------------=== #
# RuleSet — Collection of rewrite rules
# ===----------------------------------------------------------------------=== #

struct RuleSet(Movable):
    """Collection of rewrite rules for equality saturation."""
    var rules: List[RewriteRule]

    fn __init__(out self):
        self.rules = List[RewriteRule]()

    fn add_rule(mut self, var rule: RewriteRule):
        """Add a rewrite rule to the set."""
        self.rules.append(rule^)

    fn num_rules(self) -> Int:
        """Return the number of rules in the set."""
        return len(self.rules)

    fn get_phase1_rules(self) -> List[RewriteRule]:
        """Get all Phase 1 (simplification) rules."""
        var phase1 = List[RewriteRule]()
        for i in range(len(self.rules)):
            if self.rules[i].phase == 1:
                phase1.append(self.rules[i].copy())
        return phase1^

    fn get_phase2_rules(self) -> List[RewriteRule]:
        """Get all Phase 2 (equality saturation) rules."""
        var phase2 = List[RewriteRule]()
        for i in range(len(self.rules)):
            if self.rules[i].phase == 2:
                phase2.append(self.rules[i].copy())
        return phase2^


fn create_default_ruleset() -> RuleSet:
    """Create the default rule set with high-priority algebraic rules.

    Implements the highest-priority rules from egraph_rules.md:
    - Identity rules (add, mul)
    - Collapse rules (mul zero)
    - Cancellation rules (transpose involution)
    - Commutativity rules (add, mul)
    - Associativity rules (add, mul)
    """
    var rs = RuleSet()

    # Phase 1: Simplification rules
    rs.add_rule(rule_add_identity()^)
    rs.add_rule(rule_mul_identity()^)
    rs.add_rule(rule_mul_zero()^)
    rs.add_rule(rule_transpose_involution()^)

    # Phase 1: Fusion rules
    rs.add_rule(rule_rmsnorm_matmul_fusion()^)
    rs.add_rule(rule_linear_residual_add_fusion()^)
    rs.add_rule(rule_swiglu_fusion()^)

    # Phase 2: Equality saturation rules
    rs.add_rule(rule_add_commutativity()^)
    rs.add_rule(rule_mul_commutativity()^)
    rs.add_rule(rule_add_associativity()^)
    rs.add_rule(rule_mul_associativity()^)

    return rs^
