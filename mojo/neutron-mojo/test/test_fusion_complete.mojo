# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 22: Fusion Engine Completion Tests
# ===----------------------------------------------------------------------=== #

"""Tests for completed fusion engine (Op-pattern RHS + hash-consing).

Tests:
1. Add identity via engine (Var RHS)
2. Mul zero via engine (Const RHS)
3. SwiGLU fusion fires (Op RHS)
4. RMSNorm+Matmul fusion fires (Op RHS)
5. Linear+ResidualAdd fusion fires (Op RHS)
6. Add commutativity creates reversed node (Op RHS)
7. Mul commutativity creates reversed node (Op RHS)
8. Hash-consing O(1) with buckets (many nodes)
9. SwiGLU fusion has correct inputs
10. Multiple fusion rules in one pass
11. Fusion + identity rules coexist
12. Associativity creates nested node (Op RHS)
"""

from neutron_mojo.fusion.egraph import EGraph, CanonicalNode
from neutron_mojo.fusion.eclass import ClassId
from neutron_mojo.fusion.graph import OpKind
from neutron_mojo.fusion.pattern import Pattern, PatternKind, Bindings
from neutron_mojo.fusion.rules import (
    RuleSet,
    RulePriority,
    RewriteRule,
    create_default_ruleset,
    rule_swiglu_fusion,
    rule_rmsnorm_matmul_fusion,
    rule_linear_residual_add_fusion,
    rule_add_identity,
    rule_mul_zero,
    rule_add_commutativity,
    rule_mul_commutativity,
    rule_add_associativity,
)
from neutron_mojo.fusion.rewrite import RewriteEngine


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)

fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error("FAIL: " + msg + " expected=" + String(b) + " got=" + String(a))


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_add_identity_via_engine() raises:
    """Add identity rule fires via engine: (add ?x 0) -> ?x."""
    var eg = EGraph()

    # ClassId(0) = zero constant (must be first to match rule's Pattern.constant(ClassId(0)))
    var zero = eg.add(CanonicalNode(OpKind.Const))
    var x = eg.add(CanonicalNode(OpKind.Input))

    var add_node = CanonicalNode(OpKind.Add, x, zero)
    var add_class = eg.add(add_node^)

    assert_true(eg.find(x) != eg.find(add_class), "different before rewrite")

    # Use only add_identity rule
    var rs = RuleSet()
    rs.add_rule(rule_add_identity()^)
    var engine = RewriteEngine(max_iterations=3)
    var stats = engine.run_phase1(eg, rs)

    assert_true(eg.find(x) == eg.find(add_class), "merged after add_identity")
    assert_true(stats.rules_applied > 0, "rule applied")

    print("  add_identity_via_engine: PASS")


fn test_mul_zero_via_engine() raises:
    """Mul zero rule fires via engine: (mul ?x 0) -> 0 (Const RHS)."""
    var eg = EGraph()

    # ClassId(0) = zero
    var zero = eg.add(CanonicalNode(OpKind.Const))
    var x = eg.add(CanonicalNode(OpKind.Input))

    var mul_node = CanonicalNode(OpKind.Mul, x, zero)
    var mul_class = eg.add(mul_node^)

    assert_true(eg.find(mul_class) != eg.find(zero), "different before")

    var rs = RuleSet()
    rs.add_rule(rule_mul_zero()^)
    var engine = RewriteEngine(max_iterations=3)
    _ = engine.run_phase1(eg, rs)

    assert_true(eg.find(mul_class) == eg.find(zero), "mul_zero merged")

    print("  mul_zero_via_engine: PASS")


fn test_swiglu_fusion_fires() raises:
    """SwiGLU fusion: (mul (silu ?gate) ?up) -> (swiglu ?gate ?up)."""
    var eg = EGraph()

    # Add filler nodes to occupy ClassId(0) and ClassId(1) so identity rules
    # don't accidentally match our fusion operands
    _ = eg.add(CanonicalNode(OpKind.ReduceSum))   # ClassId(0)
    _ = eg.add(CanonicalNode(OpKind.ReduceMax))   # ClassId(1)

    var gate = eg.add(CanonicalNode(OpKind.Input))  # ClassId(2)
    var up = eg.add(CanonicalNode(OpKind.Const))    # ClassId(3)

    var silu_node = CanonicalNode(OpKind.SiLU, gate)
    var silu_class = eg.add(silu_node^)

    var mul_node = CanonicalNode(OpKind.Mul, silu_class, up)
    var mul_class = eg.add(mul_node^)

    var nodes_before = eg.num_nodes()

    # Use only swiglu fusion rule
    var rs = RuleSet()
    rs.add_rule(rule_swiglu_fusion()^)
    var engine = RewriteEngine(max_iterations=5)
    var stats = engine.run_phase1(eg, rs)

    # SwiGLU node should have been created
    assert_true(eg.num_nodes() > nodes_before, "new SwiGLU node added")
    assert_true(stats.rules_applied > 0, "fusion rule applied")

    var found_swiglu = False
    for i in range(eg.num_nodes()):
        if eg.nodes[i].op == OpKind.SwiGLU:
            found_swiglu = True
            assert_true(eg.find(eg.classes[i].id) == eg.find(mul_class),
                "SwiGLU merged with mul class")
            break

    assert_true(found_swiglu, "SwiGLU node created")

    print("  swiglu_fusion_fires: PASS")


fn test_rmsnorm_matmul_fusion_fires() raises:
    """Fusion: (matmul ?w (rmsnorm ?x ?gamma)) -> (fused_rmsnorm_linear ?x ?gamma ?w)."""
    var eg = EGraph()

    # Fillers to prevent identity rule interference
    _ = eg.add(CanonicalNode(OpKind.ReduceSum))
    _ = eg.add(CanonicalNode(OpKind.ReduceMax))

    var x = eg.add(CanonicalNode(OpKind.Input))
    var gamma = eg.add(CanonicalNode(OpKind.Const))
    # Distinct weight node
    var w_node = CanonicalNode(OpKind.Input)
    w_node.inputs.append(ClassId(999))
    var w = eg.add(w_node^)

    var rmsnorm_node = CanonicalNode(OpKind.RMSNorm, x, gamma)
    var rmsnorm_class = eg.add(rmsnorm_node^)

    var matmul_node = CanonicalNode(OpKind.Matmul, w, rmsnorm_class)
    var matmul_class = eg.add(matmul_node^)

    var rs = RuleSet()
    rs.add_rule(rule_rmsnorm_matmul_fusion()^)
    var engine = RewriteEngine(max_iterations=5)
    _ = engine.run_phase1(eg, rs)

    var found_fused = False
    for i in range(eg.num_nodes()):
        if eg.nodes[i].op == OpKind.FusedRMSNormLinear:
            found_fused = True
            assert_true(eg.find(eg.classes[i].id) == eg.find(matmul_class),
                "fused node merged with matmul class")
            break

    assert_true(found_fused, "FusedRMSNormLinear node created")

    print("  rmsnorm_matmul_fusion_fires: PASS")


fn test_linear_residual_add_fusion_fires() raises:
    """Fusion: (add ?res (matmul ?w ?x)) -> (fused_linear_res_add ?res ?w ?x)."""
    var eg = EGraph()

    _ = eg.add(CanonicalNode(OpKind.ReduceSum))
    _ = eg.add(CanonicalNode(OpKind.ReduceMax))

    var residual = eg.add(CanonicalNode(OpKind.Input))
    var w = eg.add(CanonicalNode(OpKind.Const))
    var x_node = CanonicalNode(OpKind.Input)
    x_node.inputs.append(ClassId(888))
    var x = eg.add(x_node^)

    var matmul_node = CanonicalNode(OpKind.Matmul, w, x)
    var matmul_class = eg.add(matmul_node^)

    var add_node = CanonicalNode(OpKind.Add, residual, matmul_class)
    var add_class = eg.add(add_node^)

    var rs = RuleSet()
    rs.add_rule(rule_linear_residual_add_fusion()^)
    var engine = RewriteEngine(max_iterations=5)
    _ = engine.run_phase1(eg, rs)

    var found_fused = False
    for i in range(eg.num_nodes()):
        if eg.nodes[i].op == OpKind.FusedLinearResAdd:
            found_fused = True
            assert_true(eg.find(eg.classes[i].id) == eg.find(add_class),
                "fused node merged with add class")
            break

    assert_true(found_fused, "FusedLinearResAdd node created")

    print("  linear_residual_add_fusion_fires: PASS")


fn test_add_commutativity_op_rhs() raises:
    """Add commutativity: (add ?x ?y) -> (add ?y ?x) creates reversed node."""
    var eg = EGraph()

    _ = eg.add(CanonicalNode(OpKind.ReduceSum))
    _ = eg.add(CanonicalNode(OpKind.ReduceMax))

    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    var add_xy = CanonicalNode(OpKind.Add, x, y)
    var xy_class = eg.add(add_xy^)

    assert_eq(eg.num_nodes(), 5, "5 nodes before commutativity")

    var rs = RuleSet()
    rs.add_rule(rule_add_commutativity()^)
    var engine = RewriteEngine(max_iterations=2)
    _ = engine.run_phase2(eg, rs)

    # (add y x) should now be in the same class
    var add_yx = CanonicalNode(OpKind.Add, y, x)
    var yx_class = eg.add(add_yx^)

    assert_true(eg.find(xy_class) == eg.find(yx_class),
        "commutative forms in same class")

    print("  add_commutativity_op_rhs: PASS")


fn test_mul_commutativity_op_rhs() raises:
    """Mul commutativity: (mul ?x ?y) -> (mul ?y ?x)."""
    var eg = EGraph()

    _ = eg.add(CanonicalNode(OpKind.ReduceSum))
    _ = eg.add(CanonicalNode(OpKind.ReduceMax))

    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))

    var mul_xy = CanonicalNode(OpKind.Mul, x, y)
    var xy_class = eg.add(mul_xy^)

    var rs = RuleSet()
    rs.add_rule(rule_mul_commutativity()^)
    var engine = RewriteEngine(max_iterations=2)
    _ = engine.run_phase2(eg, rs)

    var mul_yx = CanonicalNode(OpKind.Mul, y, x)
    var yx_class = eg.add(mul_yx^)

    assert_true(eg.find(xy_class) == eg.find(yx_class),
        "mul commutative forms in same class")

    print("  mul_commutativity_op_rhs: PASS")


fn test_hash_consing_buckets() raises:
    """Hash-consing with bucket-based lookup handles many nodes efficiently."""
    var eg = EGraph()

    # Create 100 distinct chained nodes: add(add(add(..., base), base), base)
    var base = eg.add(CanonicalNode(OpKind.Input))
    var prev = base

    for _ in range(100):
        var node = CanonicalNode(OpKind.Add, prev, base)
        prev = eg.add(node^)

    assert_eq(eg.num_nodes(), 101, "101 nodes total")

    # Adding a duplicate should hash-cons
    var first_add = CanonicalNode(OpKind.Add, base, base)
    _ = eg.add(first_add^)

    assert_eq(eg.num_nodes(), 101, "still 101 after hash-cons duplicate")

    print("  hash_consing_buckets: PASS")


fn test_swiglu_correct_inputs() raises:
    """SwiGLU fusion produces node with correct input ordering."""
    var eg = EGraph()

    _ = eg.add(CanonicalNode(OpKind.ReduceSum))
    _ = eg.add(CanonicalNode(OpKind.ReduceMax))

    var gate = eg.add(CanonicalNode(OpKind.Input))
    var up = eg.add(CanonicalNode(OpKind.Const))

    var silu_node = CanonicalNode(OpKind.SiLU, gate)
    var silu_class = eg.add(silu_node^)

    var mul_node = CanonicalNode(OpKind.Mul, silu_class, up)
    _ = eg.add(mul_node^)

    var rs = RuleSet()
    rs.add_rule(rule_swiglu_fusion()^)
    var engine = RewriteEngine(max_iterations=5)
    _ = engine.run_phase1(eg, rs)

    for i in range(eg.num_nodes()):
        if eg.nodes[i].op == OpKind.SwiGLU:
            assert_eq(len(eg.nodes[i].inputs), 2, "SwiGLU has 2 inputs")
            assert_true(eg.find(eg.nodes[i].inputs[0]) == eg.find(gate),
                "SwiGLU input 0 is gate")
            assert_true(eg.find(eg.nodes[i].inputs[1]) == eg.find(up),
                "SwiGLU input 1 is up")
            break

    print("  swiglu_correct_inputs: PASS")


fn test_multiple_fusions_one_pass() raises:
    """Multiple fusion rules fire in a single engine pass."""
    var eg = EGraph()

    _ = eg.add(CanonicalNode(OpKind.ReduceSum))
    _ = eg.add(CanonicalNode(OpKind.ReduceMax))

    # SwiGLU pattern
    var gate = eg.add(CanonicalNode(OpKind.Input))
    var up = eg.add(CanonicalNode(OpKind.Const))
    var silu_node = CanonicalNode(OpKind.SiLU, gate)
    var silu_class = eg.add(silu_node^)
    var mul_node = CanonicalNode(OpKind.Mul, silu_class, up)
    _ = eg.add(mul_node^)

    # RMSNorm+Matmul pattern (distinct nodes)
    var x_node = CanonicalNode(OpKind.Input)
    x_node.inputs.append(ClassId(777))
    var x = eg.add(x_node^)
    var gamma_node = CanonicalNode(OpKind.Const)
    gamma_node.inputs.append(ClassId(666))
    var gamma = eg.add(gamma_node^)
    var w_node = CanonicalNode(OpKind.Const)
    w_node.inputs.append(ClassId(555))
    var w = eg.add(w_node^)

    var rmsnorm_node = CanonicalNode(OpKind.RMSNorm, x, gamma)
    var rmsnorm_class = eg.add(rmsnorm_node^)
    var matmul_node = CanonicalNode(OpKind.Matmul, w, rmsnorm_class)
    _ = eg.add(matmul_node^)

    # Use ruleset with only fusion rules
    var rs = RuleSet()
    rs.add_rule(rule_swiglu_fusion()^)
    rs.add_rule(rule_rmsnorm_matmul_fusion()^)
    var engine = RewriteEngine(max_iterations=5)
    var stats = engine.run_phase1(eg, rs)

    var found_swiglu = False
    var found_fused_rmsnorm = False
    for i in range(eg.num_nodes()):
        if eg.nodes[i].op == OpKind.SwiGLU:
            found_swiglu = True
        if eg.nodes[i].op == OpKind.FusedRMSNormLinear:
            found_fused_rmsnorm = True

    assert_true(found_swiglu, "SwiGLU fusion fired")
    assert_true(found_fused_rmsnorm, "RMSNorm fusion fired")
    assert_true(stats.rules_applied >= 2, "at least 2 rules applied")

    print("  multiple_fusions_one_pass: PASS")


fn test_fusion_and_identity_coexist() raises:
    """Fusion rules and identity rules work together in full default ruleset."""
    var eg = EGraph()

    # ClassId(0) = zero, ClassId(1) = one
    var zero = eg.add(CanonicalNode(OpKind.Const))
    var one_node = CanonicalNode(OpKind.Const)
    one_node.inputs.append(ClassId(100))
    _ = eg.add(one_node^)

    # (add x 0) should simplify to x via add_identity
    var x = eg.add(CanonicalNode(OpKind.Input))
    var add_x_0 = CanonicalNode(OpKind.Add, x, zero)
    var add_class = eg.add(add_x_0^)

    # SwiGLU pattern — gate and up must not be ClassId(0) or ClassId(1)
    var gate_node = CanonicalNode(OpKind.Input)
    gate_node.inputs.append(ClassId(222))
    var gate = eg.add(gate_node^)
    var up_node = CanonicalNode(OpKind.Input)
    up_node.inputs.append(ClassId(333))
    var up = eg.add(up_node^)
    var silu_node = CanonicalNode(OpKind.SiLU, gate)
    var silu_class = eg.add(silu_node^)
    var mul_node = CanonicalNode(OpKind.Mul, silu_class, up)
    var mul_class = eg.add(mul_node^)

    var ruleset = create_default_ruleset()
    var engine = RewriteEngine(max_iterations=5)
    _ = engine.run_phase1(eg, ruleset)

    # Both should have fired
    assert_true(eg.find(x) == eg.find(add_class), "add_identity merged")

    var found_swiglu = False
    for i in range(eg.num_nodes()):
        if eg.nodes[i].op == OpKind.SwiGLU:
            found_swiglu = True
            break
    assert_true(found_swiglu, "SwiGLU fusion also fired")

    print("  fusion_and_identity_coexist: PASS")


fn test_associativity_nested_op() raises:
    """Associativity: (add (add ?x ?y) ?z) -> (add ?x (add ?y ?z))."""
    var eg = EGraph()

    _ = eg.add(CanonicalNode(OpKind.ReduceSum))
    _ = eg.add(CanonicalNode(OpKind.ReduceMax))

    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Const))
    var z_node = CanonicalNode(OpKind.Input)
    z_node.inputs.append(ClassId(444))
    var z = eg.add(z_node^)

    # Build (add (add x y) z)
    var inner = CanonicalNode(OpKind.Add, x, y)
    var inner_class = eg.add(inner^)
    var outer = CanonicalNode(OpKind.Add, inner_class, z)
    var outer_class = eg.add(outer^)

    var rs = RuleSet()
    rs.add_rule(rule_add_associativity()^)
    var engine = RewriteEngine(max_iterations=3)
    _ = engine.run_phase2(eg, rs)

    # The re-associated form (add x (add y z)) should exist and be in same class
    var inner2 = CanonicalNode(OpKind.Add, y, z)
    var inner2_class = eg.add(inner2^)  # hash-conses if already created by engine
    var outer2 = CanonicalNode(OpKind.Add, x, inner2_class)
    var outer2_class = eg.add(outer2^)

    assert_true(eg.find(outer_class) == eg.find(outer2_class),
        "associative forms in same class")

    print("  associativity_nested_op: PASS")


fn main() raises:
    print("test_fusion_complete:")

    test_add_identity_via_engine()
    test_mul_zero_via_engine()
    test_swiglu_fusion_fires()
    test_rmsnorm_matmul_fusion_fires()
    test_linear_residual_add_fusion_fires()
    test_add_commutativity_op_rhs()
    test_mul_commutativity_op_rhs()
    test_hash_consing_buckets()
    test_swiglu_correct_inputs()
    test_multiple_fusions_one_pass()
    test_fusion_and_identity_coexist()
    test_associativity_nested_op()

    print("ALL PASSED (12 tests)")
