# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Rewrite Rules Tests
# ===----------------------------------------------------------------------=== #

"""Tests for algebraic rewrite rules."""

from neutron_mojo.fusion.rules import (
    RewriteRule,
    RulePriority,
    RuleSet,
    rule_add_identity,
    rule_mul_identity,
    rule_mul_zero,
    rule_transpose_involution,
    rule_add_commutativity,
    rule_mul_commutativity,
    rule_add_associativity,
    rule_mul_associativity,
    create_default_ruleset,
)
from neutron_mojo.fusion.pattern import PatternKind
from neutron_mojo.fusion.graph import OpKind


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_add_identity_rule() raises:
    """Test add identity rule construction."""
    var rule = rule_add_identity()

    assert_true(rule.name == "add_identity", "Rule name should be add_identity")
    assert_true(rule.phase == 1, "Should be phase 1 (simplification)")
    assert_true(rule.priority == RulePriority.High, "Should be high priority")

    # LHS should be (add ?x 0)
    assert_true(rule.lhs.kind == PatternKind.Op, "LHS should be Op")
    assert_true(rule.lhs.op == OpKind.Add, "LHS op should be Add")
    assert_true(len(rule.lhs.children) == 2, "Add should have 2 children")
    assert_true(rule.lhs.children[0].kind == PatternKind.Var, "First child should be Var")
    assert_true(rule.lhs.children[1].kind == PatternKind.Const, "Second child should be Const")

    # RHS should be ?x
    assert_true(rule.rhs.kind == PatternKind.Var, "RHS should be Var")

    print("  add_identity_rule: PASS")


fn test_mul_identity_rule() raises:
    """Test mul identity rule construction."""
    var rule = rule_mul_identity()

    assert_true(rule.name == "mul_identity", "Rule name should be mul_identity")
    assert_true(rule.lhs.op == OpKind.Mul, "LHS op should be Mul")
    assert_true(rule.rhs.kind == PatternKind.Var, "RHS should be Var")

    print("  mul_identity_rule: PASS")


fn test_mul_zero_rule() raises:
    """Test mul zero rule construction."""
    var rule = rule_mul_zero()

    assert_true(rule.name == "mul_zero", "Rule name should be mul_zero")
    assert_true(rule.lhs.op == OpKind.Mul, "LHS op should be Mul")
    assert_true(rule.rhs.kind == PatternKind.Const, "RHS should be Const (0)")

    print("  mul_zero_rule: PASS")


fn test_transpose_involution_rule() raises:
    """Test transpose involution rule construction."""
    var rule = rule_transpose_involution()

    assert_true(rule.name == "transpose_involution", "Rule name should be transpose_involution")
    assert_true(rule.phase == 1, "Should be phase 1")

    # LHS should be (transpose (transpose ?x))
    assert_true(rule.lhs.kind == PatternKind.Op, "LHS should be Op")
    assert_true(rule.lhs.op == OpKind.Transpose, "LHS op should be Transpose")
    assert_true(len(rule.lhs.children) == 1, "Transpose should have 1 child")
    assert_true(rule.lhs.children[0].kind == PatternKind.Op, "Child should be Op")
    assert_true(rule.lhs.children[0].op == OpKind.Transpose, "Child op should be Transpose")

    # RHS should be ?x
    assert_true(rule.rhs.kind == PatternKind.Var, "RHS should be Var")

    print("  transpose_involution_rule: PASS")


fn test_add_commutativity_rule() raises:
    """Test add commutativity rule construction."""
    var rule = rule_add_commutativity()

    assert_true(rule.name == "add_commutativity", "Rule name should be add_commutativity")
    assert_true(rule.phase == 2, "Should be phase 2 (equality saturation)")
    assert_true(rule.priority == RulePriority.High, "Should be high priority")

    # LHS: (add ?x ?y)
    assert_true(rule.lhs.op == OpKind.Add, "LHS should be Add")
    assert_true(rule.lhs.children[0].var_id == 0, "First child should be var 0")
    assert_true(rule.lhs.children[1].var_id == 1, "Second child should be var 1")

    # RHS: (add ?y ?x) - swapped
    assert_true(rule.rhs.op == OpKind.Add, "RHS should be Add")
    assert_true(rule.rhs.children[0].var_id == 1, "First child should be var 1 (swapped)")
    assert_true(rule.rhs.children[1].var_id == 0, "Second child should be var 0 (swapped)")

    print("  add_commutativity_rule: PASS")


fn test_mul_commutativity_rule() raises:
    """Test mul commutativity rule construction."""
    var rule = rule_mul_commutativity()

    assert_true(rule.name == "mul_commutativity", "Rule name should be mul_commutativity")
    assert_true(rule.lhs.op == OpKind.Mul, "LHS should be Mul")
    assert_true(rule.rhs.op == OpKind.Mul, "RHS should be Mul")
    assert_true(rule.rhs.children[0].var_id == 1, "Operands should be swapped")

    print("  mul_commutativity_rule: PASS")


fn test_add_associativity_rule() raises:
    """Test add associativity rule construction."""
    var rule = rule_add_associativity()

    assert_true(rule.name == "add_associativity", "Rule name should be add_associativity")
    assert_true(rule.phase == 2, "Should be phase 2")
    assert_true(rule.priority == RulePriority.Medium, "Should be medium priority")

    # LHS: (add (add ?x ?y) ?z)
    assert_true(rule.lhs.op == OpKind.Add, "LHS should be Add")
    assert_true(rule.lhs.children[0].kind == PatternKind.Op, "First child should be Op")
    assert_true(rule.lhs.children[0].op == OpKind.Add, "First child should be Add")

    # RHS: (add ?x (add ?y ?z))
    assert_true(rule.rhs.op == OpKind.Add, "RHS should be Add")
    assert_true(rule.rhs.children[1].kind == PatternKind.Op, "Second child should be Op")
    assert_true(rule.rhs.children[1].op == OpKind.Add, "Second child should be Add")

    print("  add_associativity_rule: PASS")


fn test_mul_associativity_rule() raises:
    """Test mul associativity rule construction."""
    var rule = rule_mul_associativity()

    assert_true(rule.name == "mul_associativity", "Rule name should be mul_associativity")
    assert_true(rule.lhs.op == OpKind.Mul, "LHS should be Mul")
    assert_true(rule.rhs.op == OpKind.Mul, "RHS should be Mul")

    print("  mul_associativity_rule: PASS")


fn test_ruleset_creation() raises:
    """Test RuleSet basic operations."""
    var rs = RuleSet()

    assert_true(rs.num_rules() == 0, "New ruleset should be empty")

    rs.add_rule(rule_add_identity()^)
    rs.add_rule(rule_mul_commutativity()^)

    assert_true(rs.num_rules() == 2, "Should have 2 rules")

    print("  ruleset_creation: PASS")


fn test_ruleset_phase_filtering() raises:
    """Test RuleSet phase filtering."""
    var rs = RuleSet()

    # Add phase 1 rules
    rs.add_rule(rule_add_identity()^)
    rs.add_rule(rule_mul_identity()^)

    # Add phase 2 rules
    rs.add_rule(rule_add_commutativity()^)
    rs.add_rule(rule_mul_commutativity()^)

    var phase1 = rs.get_phase1_rules()
    var phase2 = rs.get_phase2_rules()

    assert_true(len(phase1) == 2, "Should have 2 phase 1 rules")
    assert_true(len(phase2) == 2, "Should have 2 phase 2 rules")

    print("  ruleset_phase_filtering: PASS")


fn test_default_ruleset() raises:
    """Test default ruleset creation."""
    var rs = create_default_ruleset()

    assert_true(rs.num_rules() == 11, "Default ruleset should have 11 rules")

    var phase1 = rs.get_phase1_rules()
    var phase2 = rs.get_phase2_rules()

    assert_true(len(phase1) == 7, "Should have 7 phase 1 rules")
    assert_true(len(phase2) == 4, "Should have 4 phase 2 rules")

    print("  default_ruleset: PASS")


fn main() raises:
    print("test_rules:")

    test_add_identity_rule()
    test_mul_identity_rule()
    test_mul_zero_rule()
    test_transpose_involution_rule()
    test_add_commutativity_rule()
    test_mul_commutativity_rule()
    test_add_associativity_rule()
    test_mul_associativity_rule()
    test_ruleset_creation()
    test_ruleset_phase_filtering()
    test_default_ruleset()

    print("ALL PASSED")
