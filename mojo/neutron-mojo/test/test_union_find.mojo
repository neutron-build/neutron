# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Union-Find Tests
# ===----------------------------------------------------------------------=== #

"""Tests for union-find data structure."""

from neutron_mojo.fusion.eclass import UnionFind, ClassId, EClass


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_make_set() raises:
    """Create singleton sets and verify they're distinct."""
    var uf = UnionFind()
    var c0 = uf.make_set()
    var c1 = uf.make_set()
    var c2 = uf.make_set()

    assert_true(c0.id() == 0, "First set should have id 0")
    assert_true(c1.id() == 1, "Second set should have id 1")
    assert_true(c2.id() == 2, "Third set should have id 2")
    assert_true(uf.size() == 3, "Should have 3 sets")

    print("  make_set: PASS")


fn test_find_singleton() raises:
    """Find on a singleton returns itself."""
    var uf = UnionFind()
    var c0 = uf.make_set()
    var c1 = uf.make_set()

    var found0 = uf.find(c0)
    var found1 = uf.find(c1)

    assert_true(found0 == c0, "Singleton find should return itself")
    assert_true(found1 == c1, "Singleton find should return itself")

    print("  find_singleton: PASS")


fn test_merge_basic() raises:
    """Merge two sets and verify they're in the same class."""
    var uf = UnionFind()
    var c0 = uf.make_set()
    var c1 = uf.make_set()

    # Before merge, different classes
    assert_true(not uf.in_same_class(c0, c1), "Should be in different classes before merge")

    # Merge them
    _ = uf.merge(c0, c1)

    # After merge, same class
    assert_true(uf.in_same_class(c0, c1), "Should be in same class after merge")

    # Both should find the same canonical representative
    var found0 = uf.find(c0)
    var found1 = uf.find(c1)
    assert_true(found0 == found1, "Both should have same canonical representative")

    print("  merge_basic: PASS")


fn test_merge_transitive() raises:
    """Merge forms transitive equivalence: if a~b and b~c, then a~c."""
    var uf = UnionFind()
    var c0 = uf.make_set()
    var c1 = uf.make_set()
    var c2 = uf.make_set()

    # Merge 0 and 1
    _ = uf.merge(c0, c1)
    assert_true(uf.in_same_class(c0, c1), "0 and 1 should be equivalent")

    # Merge 1 and 2
    _ = uf.merge(c1, c2)
    assert_true(uf.in_same_class(c1, c2), "1 and 2 should be equivalent")

    # Transitivity: 0 and 2 should also be equivalent
    assert_true(uf.in_same_class(c0, c2), "0 and 2 should be equivalent (transitive)")

    print("  merge_transitive: PASS")


fn test_merge_idempotent() raises:
    """Merging already-merged sets is a no-op."""
    var uf = UnionFind()
    var c0 = uf.make_set()
    var c1 = uf.make_set()

    var merged1 = uf.merge(c0, c1)
    var merged2 = uf.merge(c0, c1)  # Merge again

    assert_true(merged1 == merged2, "Merging twice should return same root")
    assert_true(uf.in_same_class(c0, c1), "Should still be in same class")

    print("  merge_idempotent: PASS")


fn test_path_compression() raises:
    """Verify that path compression flattens the tree."""
    var uf = UnionFind()
    var c0 = uf.make_set()
    var c1 = uf.make_set()
    var c2 = uf.make_set()
    var c3 = uf.make_set()

    # Create a chain: 0 -> 1 -> 2 -> 3
    _ = uf.merge(c0, c1)
    _ = uf.merge(c1, c2)
    _ = uf.merge(c2, c3)

    # All should be in the same class
    assert_true(uf.in_same_class(c0, c3), "0 and 3 should be equivalent")

    # After find(c0), path should be compressed
    var root = uf.find(c0)
    assert_true(uf.find(c1) == root, "All should point to same root")
    assert_true(uf.find(c2) == root, "All should point to same root")
    assert_true(uf.find(c3) == root, "All should point to same root")

    print("  path_compression: PASS")


fn test_eclass_basic() raises:
    """Test EClass node management."""
    var uf = UnionFind()
    var c0 = uf.make_set()

    var eclass = EClass(c0)
    assert_true(eclass.id == c0, "EClass should store ClassId")
    assert_true(eclass.size() == 0, "New EClass should be empty")

    eclass.add_node(10)
    eclass.add_node(20)
    eclass.add_node(30)

    assert_true(eclass.size() == 3, "EClass should have 3 nodes")
    assert_true(eclass.nodes[0] == 10, "First node should be 10")
    assert_true(eclass.nodes[1] == 20, "Second node should be 20")
    assert_true(eclass.nodes[2] == 30, "Third node should be 30")

    print("  eclass_basic: PASS")


fn test_eclass_copy() raises:
    """Test EClass copy constructor."""
    var uf = UnionFind()
    var c0 = uf.make_set()

    var eclass1 = EClass(c0)
    eclass1.add_node(100)
    eclass1.add_node(200)

    # Copy it
    var eclass2 = eclass1.copy()

    # Both should have the same data
    assert_true(eclass2.id == c0, "Copied EClass should have same id")
    assert_true(eclass2.size() == 2, "Copied EClass should have same size")
    assert_true(eclass2.nodes[0] == 100, "Copied EClass should have same nodes")
    assert_true(eclass2.nodes[1] == 200, "Copied EClass should have same nodes")

    print("  eclass_copy: PASS")


fn main() raises:
    print("test_union_find:")

    test_make_set()
    test_find_singleton()
    test_merge_basic()
    test_merge_transitive()
    test_merge_idempotent()
    test_path_compression()
    test_eclass_basic()
    test_eclass_copy()

    print("ALL PASSED")
