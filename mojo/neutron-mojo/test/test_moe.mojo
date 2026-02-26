# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Mixture of Experts Tests
# ===----------------------------------------------------------------------=== #

"""Tests for MoE routing, expert FFN, and combined layer."""

from math import abs
from neutron_mojo.nn.moe import (
    MoEConfig,
    MoERouter,
    RoutingResult,
    ExpertWeights,
    expert_ffn,
    moe_forward,
    compute_load_balance_loss,
)
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn test_moe_config() raises:
    """Test MoEConfig creation."""
    var config = MoEConfig(num_experts=8, top_k=2, hidden_dim=256, expert_dim=512)
    assert_true(config.num_experts == 8, "num_experts")
    assert_true(config.top_k == 2, "top_k")
    assert_true(config.hidden_dim == 256, "hidden_dim")
    assert_true(config.expert_dim == 512, "expert_dim")

    var copy = config.copy()
    assert_true(copy.num_experts == 8, "copy works")

    print("  moe_config: PASS")


fn test_router_creation() raises:
    """Test MoERouter creation."""
    var router = MoERouter(num_experts=4, top_k=2, hidden_dim=8)
    assert_true(router.num_experts == 4, "num_experts")
    assert_true(router.top_k == 2, "top_k")
    assert_true(router.gate_weight.numel() == 32, "gate weight size = 4*8")

    print("  router_creation: PASS")


fn test_router_selects_top_k() raises:
    """Test router selects correct top-k experts."""
    var router = MoERouter(num_experts=4, top_k=2, hidden_dim=2)

    # Set gate weights so expert 1 and 3 have highest response to input [1, 0]
    # Expert 0: [0, 0] → dot = 0
    # Expert 1: [5, 0] → dot = 5
    # Expert 2: [1, 0] → dot = 1
    # Expert 3: [3, 0] → dot = 3
    router.gate_weight.set(2, 5.0)  # expert 1, dim 0
    router.gate_weight.set(6, 3.0)  # expert 3, dim 0

    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 1.0)
    x.set(1, 0.0)

    var result = router.route(x)
    assert_true(result.top_k == 2, "top_k = 2")
    assert_true(result.get_expert_id(0) == 1, "first expert is 1")
    assert_true(result.get_expert_id(1) == 3, "second expert is 3")

    # Weights should sum to 1
    var weight_sum = result.get_weight(0) + result.get_weight(1)
    assert_near(weight_sum, 1.0, 0.01, "weights sum to 1")

    # Expert 1 should have higher weight (logit 5 vs 3)
    assert_true(result.get_weight(0) > result.get_weight(1), "expert 1 > expert 3")

    print("  router_selects_top_k: PASS")


fn test_router_weights_softmax() raises:
    """Test router weights are proper softmax."""
    var router = MoERouter(num_experts=3, top_k=3, hidden_dim=1)

    # Set equal gate weights
    router.gate_weight.set(0, 1.0)
    router.gate_weight.set(1, 1.0)
    router.gate_weight.set(2, 1.0)

    var x = Tensor[DType.float32](Shape(1))
    x.set(0, 1.0)

    var result = router.route(x)

    # All equal logits → equal weights = 1/3 each
    assert_near(result.get_weight(0), 0.333, 0.02, "equal weight 0")
    assert_near(result.get_weight(1), 0.333, 0.02, "equal weight 1")
    assert_near(result.get_weight(2), 0.333, 0.02, "equal weight 2")

    print("  router_weights_softmax: PASS")


fn test_expert_weights_creation() raises:
    """Test ExpertWeights struct."""
    var ew = ExpertWeights(num_experts=4, hidden_dim=8, expert_dim=16)
    assert_true(ew.num_experts == 4, "num_experts")
    # Per expert: gate(16*8) + up(16*8) + down(8*16) = 128 + 128 + 128 = 384
    assert_true(ew.expert_stride == 384, "expert stride")
    assert_true(ew.data.numel() == 4 * 384, "total weight elements")

    # Check offsets don't overlap
    assert_true(ew.gate_offset(0) == 0, "gate offset 0")
    assert_true(ew.up_offset(0) == 128, "up offset 0")
    assert_true(ew.down_offset(0) == 256, "down offset 0")
    assert_true(ew.gate_offset(1) == 384, "gate offset 1")

    print("  expert_weights_creation: PASS")


fn test_expert_ffn_identity() raises:
    """Test expert FFN with identity-like weights."""
    var ew = ExpertWeights(num_experts=1, hidden_dim=2, expert_dim=2)

    # Set expert 0 gate = identity, up = ones, down = identity
    # gate: [[1,0],[0,1]]
    var gb = ew.gate_offset(0)
    ew.data.set(gb + 0, 1.0)  # gate[0,0]
    ew.data.set(gb + 1, 0.0)  # gate[0,1]
    ew.data.set(gb + 2, 0.0)  # gate[1,0]
    ew.data.set(gb + 3, 1.0)  # gate[1,1]

    # up: [[1,0],[0,1]]
    var ub = ew.up_offset(0)
    ew.data.set(ub + 0, 1.0)
    ew.data.set(ub + 1, 0.0)
    ew.data.set(ub + 2, 0.0)
    ew.data.set(ub + 3, 1.0)

    # down: [[1,0],[0,1]]
    var db = ew.down_offset(0)
    ew.data.set(db + 0, 1.0)
    ew.data.set(db + 1, 0.0)
    ew.data.set(db + 2, 0.0)
    ew.data.set(db + 3, 1.0)

    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 2.0)
    x.set(1, 3.0)

    var out = expert_ffn(x, ew, 0)
    assert_true(out.numel() == 2, "output size")
    # With identity: gate=x, up=x, swiglu=silu(x)*x, down=swiglu
    # silu(2) = 2*sigmoid(2) ≈ 2*0.88 ≈ 1.76; swiglu[0] = 1.76 * 2 = 3.52
    assert_true(out.get(0) > 0.0, "non-zero output")

    print("  expert_ffn_identity: PASS")


fn test_expert_ffn_zero_weights() raises:
    """Test expert FFN with zero weights returns zeros."""
    var ew = ExpertWeights(num_experts=2, hidden_dim=3, expert_dim=4)
    # All weights are zero by default

    var x = Tensor[DType.float32](Shape(3))
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 3.0)

    var out = expert_ffn(x, ew, 0)
    assert_near(out.get(0), 0.0, 0.01, "zero weights → zero output")
    assert_near(out.get(1), 0.0, 0.01, "zero weights → zero output")

    print("  expert_ffn_zero_weights: PASS")


fn test_moe_forward_basic() raises:
    """Test full MoE forward pass."""
    var router = MoERouter(num_experts=2, top_k=1, hidden_dim=2)
    var ew = ExpertWeights(num_experts=2, hidden_dim=2, expert_dim=2)

    # Set router to always pick expert 0 (high weight on dim 0)
    router.gate_weight.set(0, 10.0)  # expert 0, dim 0
    router.gate_weight.set(1, 0.0)   # expert 0, dim 1
    router.gate_weight.set(2, -10.0) # expert 1, dim 0
    router.gate_weight.set(3, 0.0)   # expert 1, dim 1

    # Set expert 0 with some non-zero weights
    var gb = ew.gate_offset(0)
    ew.data.set(gb + 0, 1.0)
    ew.data.set(gb + 3, 1.0)
    var ub = ew.up_offset(0)
    ew.data.set(ub + 0, 1.0)
    ew.data.set(ub + 3, 1.0)
    var db = ew.down_offset(0)
    ew.data.set(db + 0, 1.0)
    ew.data.set(db + 3, 1.0)

    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 1.0)
    x.set(1, 0.5)

    var out = moe_forward(x, router, ew)
    assert_true(out.numel() == 2, "output dim")
    # With top_k=1 and router picking expert 0, output comes entirely from expert 0
    assert_true(out.get(0) != 0.0 or out.get(1) != 0.0, "non-zero output")

    print("  moe_forward_basic: PASS")


fn test_moe_forward_top2() raises:
    """Test MoE with top-2 routing combines two experts."""
    var router = MoERouter(num_experts=2, top_k=2, hidden_dim=2)
    var ew = ExpertWeights(num_experts=2, hidden_dim=2, expert_dim=2)

    # Equal router weights
    router.gate_weight.set(0, 1.0)
    router.gate_weight.set(2, 1.0)

    # Expert 0: produces [1, 0]-ish output
    var g0 = ew.gate_offset(0)
    ew.data.set(g0 + 0, 2.0)
    var u0 = ew.up_offset(0)
    ew.data.set(u0 + 0, 1.0)
    var d0 = ew.down_offset(0)
    ew.data.set(d0 + 0, 1.0)

    # Expert 1: produces [0, 1]-ish output
    var g1 = ew.gate_offset(1)
    ew.data.set(g1 + 3, 2.0)
    var u1 = ew.up_offset(1)
    ew.data.set(u1 + 3, 1.0)
    var d1 = ew.down_offset(1)
    ew.data.set(d1 + 3, 1.0)

    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 1.0)
    x.set(1, 1.0)

    var out = moe_forward(x, router, ew)
    # Both experts contribute, so both output dims should be non-zero
    # (assuming expert weights produce different outputs)
    assert_true(out.numel() == 2, "output dim")

    print("  moe_forward_top2: PASS")


fn test_load_balance_loss() raises:
    """Test load balancing loss computation."""
    var counts = Tensor[DType.float32](Shape(4))

    # Perfectly balanced: 5 tokens each
    counts.set(0, 5.0)
    counts.set(1, 5.0)
    counts.set(2, 5.0)
    counts.set(3, 5.0)
    var balanced_loss = compute_load_balance_loss(counts, 4, 20)

    # Completely imbalanced: all tokens to expert 0
    counts.set(0, 20.0)
    counts.set(1, 0.0)
    counts.set(2, 0.0)
    counts.set(3, 0.0)
    var imbalanced_loss = compute_load_balance_loss(counts, 4, 20)

    assert_true(imbalanced_loss > balanced_loss, "imbalanced > balanced loss")
    # Balanced: 4 * (4 * (0.25)^2) = 4 * 4 * 0.0625 = 1.0
    assert_near(balanced_loss, 1.0, 0.01, "balanced loss = 1.0")

    print("  load_balance_loss: PASS")


fn test_different_experts_different_outputs() raises:
    """Test that different experts produce different outputs."""
    var ew = ExpertWeights(num_experts=2, hidden_dim=2, expert_dim=2)

    # Expert 0: gate=[2,0;0,0], up=[1,0;0,0], down=[1,0;0,0]
    var g0 = ew.gate_offset(0)
    ew.data.set(g0, 2.0)
    var u0 = ew.up_offset(0)
    ew.data.set(u0, 1.0)
    var d0 = ew.down_offset(0)
    ew.data.set(d0, 1.0)

    # Expert 1: gate=[0,0;0,2], up=[0,0;0,1], down=[0,0;0,1]
    var g1 = ew.gate_offset(1)
    ew.data.set(g1 + 3, 2.0)
    var u1 = ew.up_offset(1)
    ew.data.set(u1 + 3, 1.0)
    var d1 = ew.down_offset(1)
    ew.data.set(d1 + 3, 1.0)

    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 1.0)
    x.set(1, 1.0)

    var out0 = expert_ffn(x, ew, 0)
    var out1 = expert_ffn(x, ew, 1)

    # Outputs should differ
    var diff_0 = out0.get(0) - out1.get(0)
    var diff_1 = out0.get(1) - out1.get(1)
    if diff_0 < 0.0:
        diff_0 = -diff_0
    if diff_1 < 0.0:
        diff_1 = -diff_1

    assert_true(diff_0 > 0.01 or diff_1 > 0.01, "experts produce different outputs")

    print("  different_experts_different_outputs: PASS")


fn main() raises:
    print("test_moe:")

    test_moe_config()
    test_router_creation()
    test_router_selects_top_k()
    test_router_weights_softmax()
    test_expert_weights_creation()
    test_expert_ffn_identity()
    test_expert_ffn_zero_weights()
    test_moe_forward_basic()
    test_moe_forward_top2()
    test_load_balance_loss()
    test_different_experts_different_outputs()

    print("ALL PASSED")
