# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Pattern Matching Tests
# ===----------------------------------------------------------------------=== #

"""Tests for pattern matching infrastructure."""

from neutron_mojo.fusion.pattern import Pattern, PatternKind, Bindings, match_pattern
from neutron_mojo.fusion.eclass import ClassId
from neutron_mojo.fusion.graph import OpKind


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_false(cond: Bool, msg: String) raises:
    if cond:
        raise Error("Assertion failed (expected false): " + msg)


fn test_variable_pattern() raises:
    """Variable patterns should match any e-class."""
    var p = Pattern.variable(0)

    assert_true(p.kind == PatternKind.Var, "Should be a Var pattern")
    assert_true(p.var_id == 0, "Variable ID should be 0")

    print("  variable_pattern: PASS")


fn test_constant_pattern() raises:
    """Constant patterns should store a ClassId."""
    var c = ClassId(42)
    var p = Pattern.constant(c)

    assert_true(p.kind == PatternKind.Const, "Should be a Const pattern")
    assert_true(p.class_id == 42, "ClassId should be 42")

    print("  constant_pattern: PASS")


fn test_operation_pattern() raises:
    """Operation patterns should store an OpKind."""
    var p = Pattern.operation(OpKind.Add)

    assert_true(p.kind == PatternKind.Op, "Should be an Op pattern")
    assert_true(p.op == OpKind.Add, "OpKind should be Add")
    assert_true(len(p.children) == 0, "Should have no children initially")

    print("  operation_pattern: PASS")


fn test_operation_pattern_with_children() raises:
    """Operation patterns can have sub-patterns."""
    var p = Pattern.operation(OpKind.Add)
    p.add_child(Pattern.variable(0)^)
    p.add_child(Pattern.variable(1)^)

    assert_true(len(p.children) == 2, "Should have 2 children")
    assert_true(p.children[0].kind == PatternKind.Var, "First child should be Var")
    assert_true(p.children[1].kind == PatternKind.Var, "Second child should be Var")
    assert_true(p.children[0].var_id == 0, "First child should be var 0")
    assert_true(p.children[1].var_id == 1, "Second child should be var 1")

    print("  operation_pattern_with_children: PASS")


fn test_bindings_creation() raises:
    """Create bindings for multiple variables."""
    var b = Bindings(3)

    assert_false(b.is_bound(0), "Var 0 should not be bound initially")
    assert_false(b.is_bound(1), "Var 1 should not be bound initially")
    assert_false(b.is_bound(2), "Var 2 should not be bound initially")

    print("  bindings_creation: PASS")


fn test_bindings_bind() raises:
    """Bind variables to ClassIds."""
    var b = Bindings(2)
    var c1 = ClassId(10)
    var c2 = ClassId(20)

    b.bind(0, c1)
    b.bind(1, c2)

    assert_true(b.is_bound(0), "Var 0 should be bound")
    assert_true(b.is_bound(1), "Var 1 should be bound")

    var retrieved1 = b.get(0)
    var retrieved2 = b.get(1)

    assert_true(retrieved1 == c1, "Var 0 should be bound to c1")
    assert_true(retrieved2 == c2, "Var 1 should be bound to c2")

    print("  bindings_bind: PASS")


fn test_bindings_rebind_same() raises:
    """Rebinding a variable to the same ClassId should succeed."""
    var b = Bindings(1)
    var c = ClassId(5)

    b.bind(0, c)
    b.bind(0, c)  # Re-bind to same class - should be OK

    assert_true(b.is_bound(0), "Var 0 should still be bound")
    var retrieved = b.get(0)
    assert_true(retrieved == c, "Var 0 should still be bound to c")

    print("  bindings_rebind_same: PASS")


fn test_bindings_rebind_different() raises:
    """Rebinding a variable to a different ClassId should raise an error."""
    var b = Bindings(1)
    var c1 = ClassId(5)
    var c2 = ClassId(10)

    b.bind(0, c1)

    var failed = False
    try:
        b.bind(0, c2)  # Should raise error
    except:
        failed = True

    assert_true(failed, "Rebinding to different class should raise error")

    print("  bindings_rebind_different: PASS")


fn test_match_variable() raises:
    """Variable pattern should match any e-class and bind."""
    var pattern = Pattern.variable(0)
    var class_id = ClassId(42)
    var bindings = Bindings(1)

    var matched = match_pattern(pattern, class_id, bindings)

    assert_true(matched, "Variable pattern should match")
    assert_true(bindings.is_bound(0), "Variable should be bound")
    var bound = bindings.get(0)
    assert_true(bound == class_id, "Variable should be bound to class_id")

    print("  match_variable: PASS")


fn test_match_constant_success() raises:
    """Constant pattern should match if ClassIds are equal."""
    var class_id = ClassId(42)
    var pattern = Pattern.constant(class_id)
    var bindings = Bindings(0)

    var matched = match_pattern(pattern, class_id, bindings)

    assert_true(matched, "Constant pattern should match same ClassId")

    print("  match_constant_success: PASS")


fn test_match_constant_failure() raises:
    """Constant pattern should not match if ClassIds differ."""
    var pattern_class = ClassId(42)
    var target_class = ClassId(99)
    var pattern = Pattern.constant(pattern_class)
    var bindings = Bindings(0)

    var matched = match_pattern(pattern, target_class, bindings)

    assert_false(matched, "Constant pattern should not match different ClassId")

    print("  match_constant_failure: PASS")


fn main() raises:
    print("test_pattern:")

    test_variable_pattern()
    test_constant_pattern()
    test_operation_pattern()
    test_operation_pattern_with_children()
    test_bindings_creation()
    test_bindings_bind()
    test_bindings_rebind_same()
    test_bindings_rebind_different()
    test_match_variable()
    test_match_constant_success()
    test_match_constant_failure()

    print("ALL PASSED")
