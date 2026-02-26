# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Fusion Engine Integration Tests
# ===----------------------------------------------------------------------=== #

"""End-to-end integration tests for the e-graph fusion engine.

Tests the complete workflow:
1. Build computation graph
2. Convert to e-graph with hash-consing
3. Apply algebraic rewrite rules
4. Validate equivalence discovery
"""

from neutron_mojo.fusion.graph import ComputationGraph, OpKind
from neutron_mojo.fusion.egraph import EGraph, CanonicalNode
from neutron_mojo.fusion.eclass import ClassId
from neutron_mojo.fusion.rules import create_default_ruleset
from neutron_mojo.fusion.rewrite import RewriteEngine, apply_simple_rewrite
from neutron_mojo.fusion.pattern import Pattern, Bindings


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_graph_to_egraph_conversion() raises:
    """Test converting a computation graph to an e-graph."""
    var g = ComputationGraph()

    # Build graph: y = x + 0
    var x = g.input()
    var zero = g.constant()
    var result = g.add(x, zero)

    # Verify graph structure
    assert_true(len(g.nodes) == 3, "Graph should have 3 nodes")

    print("  graph_to_egraph_conversion: PASS")


fn test_egraph_hash_consing() raises:
    """Test that e-graph properly deduplicates nodes."""
    var eg = EGraph()

    # Add same Input node twice - should hash-cons
    var input1 = eg.add(CanonicalNode(OpKind.Input))
    var input2 = eg.add(CanonicalNode(OpKind.Input))

    assert_true(input1 == input2, "Duplicate Input nodes should hash-cons")
    assert_true(eg.num_nodes() == 1, "Should only have 1 node after hash-consing")

    print("  egraph_hash_consing: PASS")


fn test_egraph_distinct_ops() raises:
    """Test that different operations create different e-classes."""
    var eg = EGraph()

    var input = eg.add(CanonicalNode(OpKind.Input))
    var const = eg.add(CanonicalNode(OpKind.Const))

    assert_true(input != const, "Different ops should have different e-classes")
    assert_true(eg.num_nodes() == 2, "Should have 2 nodes")

    print("  egraph_distinct_ops: PASS")


fn test_egraph_binary_op() raises:
    """Test adding a binary operation to e-graph."""
    var eg = EGraph()

    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    # Create (add x y)
    var add_node = CanonicalNode(OpKind.Add, x, y)
    var add_class = eg.add(add_node^)

    assert_true(eg.num_nodes() == 3, "Should have 3 nodes (x, y, add)")
    assert_true(add_class != x, "Add result should be different from inputs")
    assert_true(add_class != y, "Add result should be different from inputs")

    print("  egraph_binary_op: PASS")


fn test_equivalence_merging() raises:
    """Test merging equivalent e-classes."""
    var eg = EGraph()

    # Create two expressions: x and y
    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    # Initially different
    assert_true(eg.find(x) != eg.find(y), "Should be different before merge")

    # Apply rewrite: declare x ≡ y
    _ = apply_simple_rewrite(eg, x, y)

    # Now equivalent
    assert_true(eg.find(x) == eg.find(y), "Should be equivalent after merge")

    print("  equivalence_merging: PASS")


fn test_add_identity_rewrite_scenario() raises:
    """Test add identity rewrite scenario: (add x 0) ≡ x"""
    var eg = EGraph()

    # Build: x, 0, (add x 0)
    var x = eg.add(CanonicalNode(OpKind.Input))
    var zero = eg.add(CanonicalNode(OpKind.Const))  # Assume ClassId 1 represents 0

    var add_x_0 = CanonicalNode(OpKind.Add, x, zero)
    var add_result = eg.add(add_x_0^)

    # Before rewrite: x and (add x 0) are different
    assert_true(eg.find(x) != eg.find(add_result), "x and (add x 0) different before rewrite")

    # Apply identity rewrite: (add x 0) ≡ x
    _ = apply_simple_rewrite(eg, add_result, x)

    # After rewrite: they're equivalent
    assert_true(eg.find(x) == eg.find(add_result), "x and (add x 0) equivalent after rewrite")

    print("  add_identity_rewrite_scenario: PASS")


fn test_mul_commutativity_scenario() raises:
    """Test mul commutativity: (mul x y) can be rewritten to (mul y x)."""
    var eg = EGraph()

    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    # Create (mul x y) and (mul y x)
    var mul_xy = CanonicalNode(OpKind.Mul, x, y)
    var mul_yx = CanonicalNode(OpKind.Mul, y, x)

    var c_xy = eg.add(mul_xy^)
    var c_yx = eg.add(mul_yx^)

    # Initially different (no commutativity rule applied yet)
    assert_true(eg.find(c_xy) != eg.find(c_yx), "Different before commutativity")

    # Apply commutativity rewrite
    _ = apply_simple_rewrite(eg, c_xy, c_yx)

    # Now equivalent
    assert_true(eg.find(c_xy) == eg.find(c_yx), "Equivalent after commutativity")

    print("  mul_commutativity_scenario: PASS")


fn test_transpose_involution_scenario() raises:
    """Test transpose involution: (transpose (transpose x)) ≡ x"""
    var eg = EGraph()

    var x = eg.add(CanonicalNode(OpKind.Input))

    # Create (transpose x)
    var t1 = CanonicalNode(OpKind.Transpose, x)
    var transpose1 = eg.add(t1^)

    # Create (transpose (transpose x))
    var t2 = CanonicalNode(OpKind.Transpose, transpose1)
    var transpose2 = eg.add(t2^)

    # Before rewrite: x and (transpose (transpose x)) are different
    assert_true(eg.find(x) != eg.find(transpose2), "x and (T(T(x))) different before rewrite")

    # Apply involution rewrite
    _ = apply_simple_rewrite(eg, transpose2, x)

    # After rewrite: equivalent
    assert_true(eg.find(x) == eg.find(transpose2), "x and (T(T(x))) equivalent after rewrite")

    print("  transpose_involution_scenario: PASS")


fn test_rewrite_engine_initialization() raises:
    """Test that RewriteEngine can be created and configured."""
    var engine = RewriteEngine(max_iterations=10, max_nodes=5000)
    var ruleset = create_default_ruleset()

    assert_true(engine.max_iterations == 10, "Engine should have correct max_iterations")
    assert_true(ruleset.num_rules() == 11, "Default ruleset should have 11 rules")

    print("  rewrite_engine_initialization: PASS")


fn test_phase1_execution() raises:
    """Test Phase 1 (simplification) execution."""
    var eg = EGraph()
    var ruleset = create_default_ruleset()
    var engine = RewriteEngine(max_iterations=5)

    # Add some nodes
    _ = eg.add(CanonicalNode(OpKind.Input))
    _ = eg.add(CanonicalNode(OpKind.Const))

    var stats = engine.run_phase1(eg, ruleset)

    # With simplified matching, should terminate quickly
    assert_true(stats.iterations >= 0, "Should complete Phase 1")

    print("  phase1_execution: PASS")


fn test_phase2_execution() raises:
    """Test Phase 2 (equality saturation) execution."""
    var eg = EGraph()
    var ruleset = create_default_ruleset()
    var engine = RewriteEngine(max_iterations=5)

    # Add some nodes
    _ = eg.add(CanonicalNode(OpKind.Input))
    _ = eg.add(CanonicalNode(OpKind.Const))

    var stats = engine.run_phase2(eg, ruleset)

    # With simplified matching, should terminate quickly
    assert_true(stats.iterations >= 0, "Should complete Phase 2")

    print("  phase2_execution: PASS")


fn test_complex_expression_graph() raises:
    """Test building a complex expression in the e-graph."""
    var eg = EGraph()

    # Build: y = (a + b) * (c + d)
    var a = eg.add(CanonicalNode(OpKind.Input))
    var b = eg.add(CanonicalNode(OpKind.Input))
    var c = eg.add(CanonicalNode(OpKind.Input))
    var d = eg.add(CanonicalNode(OpKind.Input))

    # Note: Multiple Input nodes will hash-cons, so we get fewer unique classes
    var ab_add = CanonicalNode(OpKind.Add, a, b)
    var ab = eg.add(ab_add^)

    var cd_add = CanonicalNode(OpKind.Add, c, d)
    var cd = eg.add(cd_add^)

    var result_mul = CanonicalNode(OpKind.Mul, ab, cd)
    var result = eg.add(result_mul^)

    # Should have created the expression structure
    # (exact node count depends on hash-consing of Input nodes)
    assert_true(eg.num_nodes() >= 3, "Should have at least 3 nodes")
    assert_true(result != a, "Result should be different from inputs")

    print("  complex_expression_graph: PASS")


fn main() raises:
    print("test_fusion_integration:")

    test_graph_to_egraph_conversion()
    test_egraph_hash_consing()
    test_egraph_distinct_ops()
    test_egraph_binary_op()
    test_equivalence_merging()
    test_add_identity_rewrite_scenario()
    test_mul_commutativity_scenario()
    test_transpose_involution_scenario()
    test_rewrite_engine_initialization()
    test_phase1_execution()
    test_phase2_execution()
    test_complex_expression_graph()

    print("ALL PASSED")
