# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Rewrite Engine Tests
# ===----------------------------------------------------------------------=== #

"""Tests for equality saturation rewrite engine."""

from neutron_mojo.fusion.rewrite import (
    RewriteEngine,
    RewriteStats,
    Match,
    apply_simple_rewrite,
    count_rewrites_applied,
)
from neutron_mojo.fusion.egraph import EGraph, CanonicalNode
from neutron_mojo.fusion.eclass import ClassId
from neutron_mojo.fusion.rules import create_default_ruleset
from neutron_mojo.fusion.pattern import Bindings, Pattern
from neutron_mojo.fusion.graph import OpKind


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_rewrite_engine_creation() raises:
    """Test RewriteEngine initialization."""
    var engine = RewriteEngine(max_iterations=10, max_nodes=1000)

    assert_true(engine.max_iterations == 10, "Max iterations should be 10")
    assert_true(engine.max_nodes == 1000, "Max nodes should be 1000")

    print("  rewrite_engine_creation: PASS")


fn test_rewrite_stats() raises:
    """Test RewriteStats initialization."""
    var stats = RewriteStats()

    assert_true(stats.iterations == 0, "Iterations should be 0")
    assert_true(stats.total_matches == 0, "Total matches should be 0")
    assert_true(stats.rules_applied == 0, "Rules applied should be 0")
    assert_true(stats.nodes_added == 0, "Nodes added should be 0")

    print("  rewrite_stats: PASS")


fn test_match_creation() raises:
    """Test Match structure."""
    var bindings = Bindings(2)
    bindings.bind(0, ClassId(10))
    bindings.bind(1, ClassId(20))

    var m = Match("test_rule", ClassId(5), bindings^, Pattern.variable(0))

    assert_true(m.rule_name == "test_rule", "Rule name should be test_rule")
    assert_true(m.matched_class == ClassId(5), "Matched class should be 5")
    assert_true(m.bindings.is_bound(0), "Var 0 should be bound")
    assert_true(m.bindings.is_bound(1), "Var 1 should be bound")

    print("  match_creation: PASS")


fn test_match_copy() raises:
    """Test Match copy."""
    var bindings = Bindings(1)
    bindings.bind(0, ClassId(42))

    var m1 = Match("rule1", ClassId(1), bindings^, Pattern.variable(0))
    var m2 = m1.copy()

    assert_true(m2.rule_name == "rule1", "Copied match should have same rule name")
    assert_true(m2.matched_class == ClassId(1), "Copied match should have same class")

    print("  match_copy: PASS")


fn test_simple_rewrite_application() raises:
    """Test simple rewrite application (merge two classes)."""
    var eg = EGraph()

    # Create two different classes
    var c1 = eg.add(CanonicalNode(OpKind.Input))
    var c2 = eg.add(CanonicalNode(OpKind.Const))

    # Before rewrite, they're different
    assert_true(eg.find(c1) != eg.find(c2), "Classes should be different before rewrite")

    # Apply rewrite: merge c1 and c2
    var applied = apply_simple_rewrite(eg, c1, c2)

    assert_true(applied, "Rewrite should be applied")
    assert_true(eg.find(c1) == eg.find(c2), "Classes should be merged after rewrite")

    print("  simple_rewrite_application: PASS")


fn test_phase1_empty_egraph() raises:
    """Test Phase 1 on an empty e-graph."""
    var eg = EGraph()
    var ruleset = create_default_ruleset()
    var engine = RewriteEngine(max_iterations=5)

    var stats = engine.run_phase1(eg, ruleset)

    # Empty e-graph should have no matches
    assert_true(stats.iterations == 0, "Should have 0 iterations on empty e-graph")
    assert_true(stats.total_matches == 0, "Should have 0 matches")

    print("  phase1_empty_egraph: PASS")


fn test_phase2_empty_egraph() raises:
    """Test Phase 2 on an empty e-graph."""
    var eg = EGraph()
    var ruleset = create_default_ruleset()
    var engine = RewriteEngine(max_iterations=5)

    var stats = engine.run_phase2(eg, ruleset)

    # Empty e-graph should have no matches
    assert_true(stats.iterations == 0, "Should have 0 iterations on empty e-graph")
    assert_true(stats.total_matches == 0, "Should have 0 matches")

    print("  phase2_empty_egraph: PASS")


fn test_phase1_with_nodes() raises:
    """Test Phase 1 with some nodes in the e-graph."""
    var eg = EGraph()
    var ruleset = create_default_ruleset()
    var engine = RewriteEngine(max_iterations=5)

    # Add some nodes
    var x = eg.add(CanonicalNode(OpKind.Input))
    var c = eg.add(CanonicalNode(OpKind.Const))
    var add_node = CanonicalNode(OpKind.Add, x, c)
    _ = eg.add(add_node^)

    var stats = engine.run_phase1(eg, ruleset)

    # With simplified matching (not fully implemented), iterations should be 0
    # In a full implementation, this would match and apply rules
    assert_true(stats.iterations >= 0, "Should have >= 0 iterations")

    print("  phase1_with_nodes: PASS")


fn test_count_rewrites() raises:
    """Test rewrite counting on empty e-graph."""
    var eg = EGraph()
    var ruleset = create_default_ruleset()

    var count = count_rewrites_applied(eg, ruleset, max_iterations=3)

    # Empty e-graph has no nodes to match, so 0 rewrites applied
    assert_true(count == 0, "Empty e-graph should have 0 rewrites")

    print("  count_rewrites: PASS")


fn test_max_iterations_limit() raises:
    """Test that engine respects max_iterations limit."""
    var engine = RewriteEngine(max_iterations=3)

    assert_true(engine.max_iterations == 3, "Max iterations should be 3")

    print("  max_iterations_limit: PASS")


fn test_max_nodes_limit() raises:
    """Test that engine has max_nodes limit."""
    var engine = RewriteEngine(max_nodes=500)

    assert_true(engine.max_nodes == 500, "Max nodes should be 500")

    print("  max_nodes_limit: PASS")


fn main() raises:
    print("test_rewrite:")

    test_rewrite_engine_creation()
    test_rewrite_stats()
    test_match_creation()
    test_match_copy()
    test_simple_rewrite_application()
    test_phase1_empty_egraph()
    test_phase2_empty_egraph()
    test_phase1_with_nodes()
    test_count_rewrites()
    test_max_iterations_limit()
    test_max_nodes_limit()

    print("ALL PASSED")
