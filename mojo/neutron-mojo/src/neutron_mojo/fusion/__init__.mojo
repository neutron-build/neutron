# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Graph optimization
# ===----------------------------------------------------------------------=== #

"""Graph optimization: computation graph, fusion engine, e-graph, rewrites."""

from .graph import OpKind, ValueId, ENode, ComputationGraph
from .eclass import ClassId, UnionFind, EClass
from .egraph import EGraph, CanonicalNode
from .pattern import (
    PatternKind,
    Pattern,
    Bindings,
    match_pattern,
    match_pattern_egraph,
)
from .rules import (
    RulePriority,
    RewriteRule,
    RuleSet,
    create_default_ruleset,
    rule_add_identity,
    rule_mul_identity,
    rule_mul_zero,
    rule_transpose_involution,
    rule_add_commutativity,
    rule_mul_commutativity,
    rule_add_associativity,
    rule_mul_associativity,
    rule_rmsnorm_matmul_fusion,
    rule_linear_residual_add_fusion,
    rule_swiglu_fusion,
)
from .rewrite import (
    Match,
    RewriteStats,
    RewriteEngine,
    apply_simple_rewrite,
    count_rewrites_applied,
)
from .executor import (
    TensorValue,
    GraphExecutor,
    optimize_and_execute,
)
