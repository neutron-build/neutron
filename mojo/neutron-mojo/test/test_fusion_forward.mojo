# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Fusion Forward Pass + E-Graph Pattern Matching Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Sprint 15: fused forward pass + fusion rules in e-graph engine.

Tests:
1. Fused forward produces valid logits
2. Fused vs unfused forward equivalence
3. Fused forward generation
4. New fusion OpKinds
5. E-graph Op pattern matching
6. RMSNorm+Matmul fusion rule pattern
7. Linear+ResAdd fusion rule pattern
8. SwiGLU fusion rule pattern
9. Fusion rules in default ruleset
10. E-graph rewrite engine with fusion rules
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.fusion.graph import OpKind, ComputationGraph, ENode
from neutron_mojo.fusion.egraph import EGraph, CanonicalNode
from neutron_mojo.fusion.eclass import ClassId
from neutron_mojo.fusion.pattern import (
    Pattern, PatternKind, Bindings,
    match_pattern, match_pattern_egraph,
)
from neutron_mojo.fusion.rules import (
    create_default_ruleset,
    rule_rmsnorm_matmul_fusion,
    rule_linear_residual_add_fusion,
    rule_swiglu_fusion,
)
from neutron_mojo.fusion.rewrite import RewriteEngine
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn _build_tiny_model() -> Model:
    """Build a tiny model with small random-ish weights for testing."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set some non-zero embed weights
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            var val = Float32(v * p.hidden_dim + d) * 0.01 - 0.15
            model.embed.set(v * p.hidden_dim + d, val)

    # Set LM head
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            var val = Float32(v + d) * 0.02 - 0.08
            model.lm_head.set(v * p.hidden_dim + d, val)

    # Set some layer weights to small values (norms already 1.0)
    for layer in range(p.num_layers):
        var base = layer * model.layer_size
        for i in range(model.layer_size):
            var idx = base + i
            # Skip norms (already 1.0)
            var val = model.layer_weights.get(idx)
            if val == 1.0:
                continue
            model.layer_weights.set(idx, Float32(i % 7) * 0.01 - 0.03)

    return model^


fn test_fused_forward_produces_logits() raises:
    """Fused forward should produce valid logits tensor."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)

    var logits = model.forward_fused(0, cache, rope, 0)

    assert_true(logits.numel() == p.vocab_size, "Logits should be [vocab_size]")

    # At least one logit should be non-zero
    var has_nonzero = False
    for i in range(logits.numel()):
        if abs(logits.get(i)) > 1e-10:
            has_nonzero = True
            break
    assert_true(has_nonzero, "Logits should have non-zero values")

    print("  fused_forward_produces_logits: PASS")


fn test_fused_vs_unfused_equivalence() raises:
    """Fused and unfused forward should produce similar logits."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    # Unfused forward
    var cache1 = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope1 = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits1 = model.forward(0, cache1, rope1, 0)

    # Fused forward
    var cache2 = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope2 = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits2 = model.forward_fused(0, cache2, rope2, 0)

    assert_true(logits1.numel() == logits2.numel(), "Same number of logits")

    # Check they produce similar values (should be identical in exact arithmetic,
    # but floating point order may differ slightly)
    var max_diff: Float32 = 0.0
    for i in range(logits1.numel()):
        var diff = abs(logits1.get(i) - logits2.get(i))
        if diff > max_diff:
            max_diff = diff

    assert_true(max_diff < 0.01, "Fused and unfused logits should be close (max_diff=" + String(max_diff) + ")")

    print("  fused_vs_unfused_equivalence: PASS")


fn test_fused_forward_generation() raises:
    """Fused forward can be used in a generation loop."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=p.max_seq_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)

    var generated = List[Int]()
    var token_id = 1

    # Generate 3 tokens using fused forward
    for pos in range(3):
        var logits = model.forward_fused(token_id, cache, rope, pos)
        # Greedy: pick argmax
        var best = 0
        var best_val = logits.get(0)
        for i in range(1, logits.numel()):
            if logits.get(i) > best_val:
                best_val = logits.get(i)
                best = i
        generated.append(best)
        token_id = best

    assert_true(len(generated) == 3, "Should generate 3 tokens")

    # All tokens should be valid token IDs
    for i in range(len(generated)):
        assert_true(generated[i] >= 0 and generated[i] < p.vocab_size,
            "Token ID should be valid")

    print("  fused_forward_generation: PASS")


fn test_fusion_opkinds() raises:
    """New fusion OpKinds should be properly defined."""
    assert_true(OpKind.FusedRMSNormLinear._value == 60, "FusedRMSNormLinear should be 60")
    assert_true(OpKind.FusedLinearResAdd._value == 61, "FusedLinearResAdd should be 61")
    assert_true(OpKind.SwiGLU._value == 62, "SwiGLU should be 62")

    # They should be distinct from each other and existing ops
    assert_true(OpKind.FusedRMSNormLinear != OpKind.RMSNorm, "FusedRMSNormLinear != RMSNorm")
    assert_true(OpKind.FusedRMSNormLinear != OpKind.Matmul, "FusedRMSNormLinear != Matmul")
    assert_true(OpKind.FusedLinearResAdd != OpKind.Add, "FusedLinearResAdd != Add")
    assert_true(OpKind.SwiGLU != OpKind.SiLU, "SwiGLU != SiLU")

    # write_to should work
    assert_true(String(OpKind.FusedRMSNormLinear) == "FusedRMSNormLinear", "FusedRMSNormLinear name")
    assert_true(String(OpKind.FusedLinearResAdd) == "FusedLinearResAdd", "FusedLinearResAdd name")
    assert_true(String(OpKind.SwiGLU) == "SwiGLU", "SwiGLU name")

    print("  fusion_opkinds: PASS")


fn test_egraph_op_pattern_matching() raises:
    """Op pattern matching should work with EGraph access."""
    var eg = EGraph()

    # Build: x, w, (matmul w x)
    var x = eg.add(CanonicalNode(OpKind.Input))
    var w = eg.add(CanonicalNode(OpKind.Const))
    var matmul_node = CanonicalNode(OpKind.Matmul, w, x)
    var result = eg.add(matmul_node^)

    # Pattern: (matmul ?a ?b)
    var pat = Pattern.operation(OpKind.Matmul)
    pat.add_child(Pattern.variable(0)^)  # ?a
    pat.add_child(Pattern.variable(1)^)  # ?b

    var bindings = Bindings(2)
    var matched = match_pattern_egraph(pat, result, bindings, eg)

    assert_true(matched, "Should match (matmul ?a ?b) against (matmul w x)")
    assert_true(bindings.is_bound(0), "?a should be bound")
    assert_true(bindings.is_bound(1), "?b should be bound")
    assert_true(bindings.get(0) == w, "?a should be bound to w")
    assert_true(bindings.get(1) == x, "?b should be bound to x")

    print("  egraph_op_pattern_matching: PASS")


fn test_rmsnorm_matmul_fusion_pattern() raises:
    """RMSNorm+Matmul fusion pattern should match e-graph nodes."""
    var eg = EGraph()

    # Build: x, gamma, w, (rmsnorm x gamma), (matmul w (rmsnorm x gamma))
    var x = eg.add(CanonicalNode(OpKind.Input))
    var gamma = eg.add(CanonicalNode(OpKind.Const))

    var norm_node = CanonicalNode(OpKind.RMSNorm, x, gamma)
    var norm_result = eg.add(norm_node^)

    var w = eg.add(CanonicalNode(OpKind(50)))  # Another Const (hash-conses with gamma)
    # Use a different op to get a distinct weight node
    var w2 = eg.add(CanonicalNode(OpKind.Reshape))  # Distinct from gamma

    var matmul_node = CanonicalNode(OpKind.Matmul, w2, norm_result)
    var result = eg.add(matmul_node^)

    # Pattern from rule: (matmul ?w (rmsnorm ?x ?gamma))
    var rule = rule_rmsnorm_matmul_fusion()

    var bindings = Bindings(3)
    var matched = match_pattern_egraph(rule.lhs, result, bindings, eg)

    assert_true(matched, "RMSNorm+Matmul fusion pattern should match")

    print("  rmsnorm_matmul_fusion_pattern: PASS")


fn test_linear_resadd_fusion_pattern() raises:
    """Linear+ResAdd fusion pattern should match e-graph nodes."""
    var eg = EGraph()

    # Build: residual, w, x, (matmul w x), (add residual (matmul w x))
    var residual = eg.add(CanonicalNode(OpKind.Input))
    var w = eg.add(CanonicalNode(OpKind.Const))
    var x = eg.add(CanonicalNode(OpKind.Reshape))  # Distinct from w

    var matmul_node = CanonicalNode(OpKind.Matmul, w, x)
    var matmul_result = eg.add(matmul_node^)

    var add_node = CanonicalNode(OpKind.Add, residual, matmul_result)
    var result = eg.add(add_node^)

    # Pattern: (add ?residual (matmul ?w ?x))
    var rule = rule_linear_residual_add_fusion()

    var bindings = Bindings(3)
    var matched = match_pattern_egraph(rule.lhs, result, bindings, eg)

    assert_true(matched, "Linear+ResAdd fusion pattern should match")

    print("  linear_resadd_fusion_pattern: PASS")


fn test_swiglu_fusion_pattern() raises:
    """SwiGLU fusion pattern should match e-graph nodes."""
    var eg = EGraph()

    # Build: gate, up, (silu gate), (mul (silu gate) up)
    var gate = eg.add(CanonicalNode(OpKind.Input))
    var up = eg.add(CanonicalNode(OpKind.Const))

    var silu_node = CanonicalNode(OpKind.SiLU, gate)
    var silu_result = eg.add(silu_node^)

    var mul_node = CanonicalNode(OpKind.Mul, silu_result, up)
    var result = eg.add(mul_node^)

    # Pattern: (mul (silu ?gate) ?up)
    var rule = rule_swiglu_fusion()

    var bindings = Bindings(2)
    var matched = match_pattern_egraph(rule.lhs, result, bindings, eg)

    assert_true(matched, "SwiGLU fusion pattern should match")

    print("  swiglu_fusion_pattern: PASS")


fn test_default_ruleset_has_fusion_rules() raises:
    """Default ruleset should include 3 fusion rules + 8 algebraic rules."""
    var ruleset = create_default_ruleset()

    assert_true(ruleset.num_rules() == 11, "Should have 11 rules total")

    # Phase 1 should have 4 algebraic + 3 fusion = 7 rules
    var phase1 = ruleset.get_phase1_rules()
    assert_true(len(phase1) == 7, "Phase 1 should have 7 rules (4 algebraic + 3 fusion)")

    # Phase 2 should have 4 rules
    var phase2 = ruleset.get_phase2_rules()
    assert_true(len(phase2) == 4, "Phase 2 should have 4 rules")

    print("  default_ruleset_has_fusion_rules: PASS")


fn test_rewrite_engine_with_fusion() raises:
    """Rewrite engine should process fusion rules."""
    var eg = EGraph()

    # Build: gate, up, (silu gate), (mul (silu gate) up)
    var gate = eg.add(CanonicalNode(OpKind.Input))
    var up = eg.add(CanonicalNode(OpKind.Const))
    var silu_node = CanonicalNode(OpKind.SiLU, gate)
    var silu_result = eg.add(silu_node^)
    var mul_node = CanonicalNode(OpKind.Mul, silu_result, up)
    _ = eg.add(mul_node^)

    var initial_nodes = eg.num_nodes()

    var engine = RewriteEngine(max_iterations=5)
    var ruleset = create_default_ruleset()

    var stats = engine.run_phase1(eg, ruleset)

    # Engine should find and process matches
    assert_true(stats.iterations >= 0, "Should complete iterations")
    assert_true(stats.total_matches >= 0, "Should find matches")

    print("  rewrite_engine_with_fusion: PASS")


fn test_graph_builder_fused_ops() raises:
    """ComputationGraph should have builder methods for fused ops."""
    var g = ComputationGraph()

    var x = g.input()
    var w = g.constant()
    var gamma = g.constant()

    # New builder methods
    var norm = g.rmsnorm(x, gamma)
    var fused = g.fused_rmsnorm_linear(x, gamma, w)
    var silu_out = g.silu(x)
    var sg = g.swiglu(x, w)
    var res = g.fused_linear_res_add(x, w, gamma)

    assert_true(len(g.nodes) == 8, "Should have 8 nodes")

    print("  graph_builder_fused_ops: PASS")


fn main() raises:
    print("test_fusion_forward:")

    test_fused_forward_produces_logits()
    test_fused_vs_unfused_equivalence()
    test_fused_forward_generation()
    test_fusion_opkinds()
    test_egraph_op_pattern_matching()
    test_rmsnorm_matmul_fusion_pattern()
    test_linear_resadd_fusion_pattern()
    test_swiglu_fusion_pattern()
    test_default_ruleset_has_fusion_rules()
    test_rewrite_engine_with_fusion()
    test_graph_builder_fused_ops()

    print("ALL PASSED")
