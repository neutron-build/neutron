# ===----------------------------------------------------------------------=== #
# Neutron Mojo — E-Graph Tests
# ===----------------------------------------------------------------------=== #

"""Tests for e-graph data structure with hash-consing."""

from neutron_mojo.fusion.egraph import EGraph, CanonicalNode
from neutron_mojo.fusion.eclass import ClassId
from neutron_mojo.fusion.graph import OpKind


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_add_leaf_nodes() raises:
    """Add leaf nodes (constants, inputs) to e-graph."""
    var eg = EGraph()

    var const_node = CanonicalNode(OpKind.Const)
    var input_node = CanonicalNode(OpKind.Input)

    var c0 = eg.add(const_node^)
    var c1 = eg.add(input_node^)

    assert_true(eg.num_nodes() == 2, "Should have 2 nodes")
    assert_true(eg.num_classes() == 2, "Should have 2 classes")
    assert_true(c0 != c1, "Different nodes should have different classes")

    print("  add_leaf_nodes: PASS")


fn test_hash_consing_same_op() raises:
    """Hash-consing: duplicate nodes should return same e-class."""
    var eg = EGraph()

    # Add two identical Const nodes
    var const1 = CanonicalNode(OpKind.Const)
    var const2 = CanonicalNode(OpKind.Const)

    var c0 = eg.add(const1^)
    var c1 = eg.add(const2^)

    assert_true(c0 == c1, "Duplicate Const nodes should hash-cons to same class")
    assert_true(eg.num_nodes() == 1, "Should only have 1 node after hash-consing")
    assert_true(eg.num_classes() == 1, "Should only have 1 class after hash-consing")

    print("  hash_consing_same_op: PASS")


fn test_hash_consing_different_ops() raises:
    """Hash-consing: different ops should create different classes."""
    var eg = EGraph()

    var const_node = CanonicalNode(OpKind.Const)
    var input_node = CanonicalNode(OpKind.Input)

    var c0 = eg.add(const_node^)
    var c1 = eg.add(input_node^)

    assert_true(c0 != c1, "Different ops should create different classes")
    assert_true(eg.num_nodes() == 2, "Should have 2 nodes")
    assert_true(eg.num_classes() == 2, "Should have 2 classes")

    print("  hash_consing_different_ops: PASS")


fn test_add_binary_op() raises:
    """Add a binary operation node to e-graph."""
    var eg = EGraph()

    # Create leaf nodes with different ops (Input and Const)
    # Note: Two Input nodes would hash-cons to the same class
    var input_node = CanonicalNode(OpKind.Input)
    var const_node = CanonicalNode(OpKind.Const)

    var c1 = eg.add(input_node^)
    var c2 = eg.add(const_node^)

    # Create Add node: (add c1 c2)
    var add_node = CanonicalNode(OpKind.Add, c1, c2)
    _ = eg.add(add_node^)

    assert_true(eg.num_nodes() == 3, "Should have 3 nodes (input + const + add)")
    assert_true(eg.num_classes() == 3, "Should have 3 classes")

    print("  add_binary_op: PASS")


fn test_hash_consing_binary_same_inputs() raises:
    """Hash-consing: (add x y) added twice should hash-cons."""
    var eg = EGraph()

    # Use different ops so they don't hash-cons
    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    # Add (add x y) twice
    var add1 = CanonicalNode(OpKind.Add, x, y)
    var add2 = CanonicalNode(OpKind.Add, x, y)

    var c_add1 = eg.add(add1^)
    var c_add2 = eg.add(add2^)

    assert_true(c_add1 == c_add2, "Duplicate (add x y) should hash-cons")
    assert_true(eg.num_nodes() == 3, "Should have 3 nodes (input + const + add)")

    print("  hash_consing_binary_same_inputs: PASS")


fn test_hash_consing_binary_different_order() raises:
    """Hash-consing: (add x y) and (add y x) are different (not commutative yet)."""
    var eg = EGraph()

    # Use different ops so they don't hash-cons
    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    # Add (add x y) and (add y x) - different node structures
    var add_xy = CanonicalNode(OpKind.Add, x, y)
    var add_yx = CanonicalNode(OpKind.Add, y, x)

    var c_xy = eg.add(add_xy^)
    var c_yx = eg.add(add_yx^)

    # Without commutativity rewrite, these are different nodes
    assert_true(c_xy != c_yx, "(add x y) and (add y x) should be different nodes initially")
    assert_true(eg.num_nodes() == 4, "Should have 4 nodes (input + const + 2 adds)")

    print("  hash_consing_binary_different_order: PASS")


fn test_merge_classes() raises:
    """Merge two e-classes and verify they're equivalent."""
    var eg = EGraph()

    # Use different ops so they create different classes
    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    # Before merge, different classes
    assert_true(eg.find(x) != eg.find(y), "Should be different before merge")

    # Merge them
    var merged = eg.merge(x, y)

    # After merge, same class
    assert_true(eg.find(x) == eg.find(y), "Should be same after merge")
    assert_true(eg.find(x) == merged, "Find should return merged class")

    print("  merge_classes: PASS")


fn test_canonicalize_after_merge() raises:
    """Canonicalize node inputs after merging classes."""
    var eg = EGraph()

    # Use different ops so they create different classes initially
    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    # Create node (add x y)
    var add_node = CanonicalNode(OpKind.Add, x, y)

    # Merge x and y
    _ = eg.merge(x, y)

    # Canonicalize the add node - both inputs should now point to same class
    var canonical = eg.canonicalize(add_node^)

    assert_true(canonical.inputs[0] == canonical.inputs[1], "After merge, both inputs should canonicalize to same class")
    assert_true(eg.find(x) == canonical.inputs[0], "Canonical input should be the merged class")

    print("  canonicalize_after_merge: PASS")


fn test_algebraic_example() raises:
    """E-graph example: (add x 0) and x should merge via rewrite rule.

    This tests the typical e-graph workflow:
    1. Add nodes to graph
    2. Apply rewrite rule (identity: add x 0 -> x)
    3. Merge equivalent classes
    4. Canonicalize to find equivalence
    """
    var eg = EGraph()

    # Create nodes: x, 0, (add x 0)
    var x = eg.add(CanonicalNode(OpKind.Input))
    var zero = eg.add(CanonicalNode(OpKind.Const))
    var add_x_0 = CanonicalNode(OpKind.Add, x, zero)
    var c_add = eg.add(add_x_0^)

    # Initially, x and (add x 0) are in different classes
    assert_true(eg.find(x) != eg.find(c_add), "Before rewrite, x and (add x 0) are different")

    # Apply rewrite rule: (add x 0) == x
    _ = eg.merge(c_add, x)

    # After merge, they're equivalent
    assert_true(eg.find(x) == eg.find(c_add), "After rewrite, x and (add x 0) are equivalent")

    print("  algebraic_example: PASS")


fn main() raises:
    print("test_egraph:")

    test_add_leaf_nodes()
    test_hash_consing_same_op()
    test_hash_consing_different_ops()
    test_add_binary_op()
    test_hash_consing_binary_same_inputs()
    test_hash_consing_binary_different_order()
    test_merge_classes()
    test_canonicalize_after_merge()
    test_algebraic_example()

    print("ALL PASSED")
