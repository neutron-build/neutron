# ===----------------------------------------------------------------------=== #
# Neutron Mojo — LoRA Tests
# ===----------------------------------------------------------------------=== #

"""Tests for LoRA configuration, forward, linear, merge, and unmerge."""

from math import abs
from neutron_mojo.nn.lora import (
    LoRAConfig,
    LoRAWeight,
    lora_forward,
    lora_linear,
    merge_lora,
    unmerge_lora,
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


fn test_lora_config() raises:
    """Test LoRAConfig creation and scaling."""
    var config = LoRAConfig(rank=8, alpha=16.0, in_features=64, out_features=32)
    assert_true(config.rank == 8, "rank")
    assert_true(config.in_features == 64, "in_features")
    assert_true(config.out_features == 32, "out_features")
    assert_near(config.scaling(), 2.0, 0.01, "scaling = alpha/rank = 16/8 = 2")

    # Zero rank → zero scaling
    var zero_config = LoRAConfig(rank=0, alpha=16.0, in_features=4, out_features=4)
    assert_near(zero_config.scaling(), 0.0, 0.01, "zero rank → zero scaling")

    # Copy
    var copy = config.copy()
    assert_true(copy.rank == 8, "copy rank")
    assert_near(copy.scaling(), 2.0, 0.01, "copy scaling")

    print("  lora_config: PASS")


fn test_lora_weight_creation() raises:
    """Test LoRAWeight creation with correct sizes."""
    var config = LoRAConfig(rank=4, alpha=4.0, in_features=8, out_features=6)
    var lora = LoRAWeight(config)

    # A: [rank * in_features] = 4 * 8 = 32
    assert_true(lora.lora_a.numel() == 32, "lora_a size")
    # B: [out_features * rank] = 6 * 4 = 24
    assert_true(lora.lora_b.numel() == 24, "lora_b size")

    # B initialized to zero → lora_forward should return zeros
    var x = Tensor[DType.float32](Shape(8))
    for i in range(8):
        x.set(i, Float32(i) * 0.1)

    var out = lora_forward(x, lora)
    for i in range(6):
        assert_near(out.get(i), 0.0, 0.001, "B=0 → zero output")

    print("  lora_weight_creation: PASS")


fn test_lora_forward_basic() raises:
    """Test LoRA forward with known weights."""
    var config = LoRAConfig(rank=1, alpha=1.0, in_features=2, out_features=2)
    var lora = LoRAWeight(config)

    # A = [1, 0] (rank=1, in=2) → A @ [x0, x1] = x0
    lora.lora_a.set(0, 1.0)
    lora.lora_a.set(1, 0.0)

    # B = [[1], [2]] (out=2, rank=1) → B @ [x0] = [x0, 2*x0]
    lora.lora_b.set(0, 1.0)
    lora.lora_b.set(1, 2.0)

    # scaling = alpha/rank = 1/1 = 1
    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 3.0)
    x.set(1, 7.0)

    # delta = 1.0 * B @ (A @ x) = B @ [3] = [3, 6]
    var out = lora_forward(x, lora)
    assert_near(out.get(0), 3.0, 0.01, "delta[0] = 3")
    assert_near(out.get(1), 6.0, 0.01, "delta[1] = 6")

    print("  lora_forward_basic: PASS")


fn test_lora_forward_scaling() raises:
    """Test that alpha/rank scaling is applied correctly."""
    var config = LoRAConfig(rank=2, alpha=4.0, in_features=2, out_features=1)
    var lora = LoRAWeight(config)

    # A = [[1, 0], [0, 1]] (rank=2, in=2) → identity
    lora.lora_a.set(0, 1.0)  # A[0,0]
    lora.lora_a.set(1, 0.0)  # A[0,1]
    lora.lora_a.set(2, 0.0)  # A[1,0]
    lora.lora_a.set(3, 1.0)  # A[1,1]

    # B = [[1, 1]] (out=1, rank=2) → sum
    lora.lora_b.set(0, 1.0)
    lora.lora_b.set(1, 1.0)

    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 2.0)
    x.set(1, 3.0)

    # Without scaling: B @ (A @ x) = B @ [2, 3] = [5]
    # With scaling (4/2 = 2): 2 * 5 = 10
    var out = lora_forward(x, lora)
    assert_near(out.get(0), 10.0, 0.01, "scaled output = 10")

    print("  lora_forward_scaling: PASS")


fn test_lora_linear() raises:
    """Test lora_linear combines base + LoRA correctly."""
    var config = LoRAConfig(rank=1, alpha=1.0, in_features=2, out_features=2)
    var lora = LoRAWeight(config)

    # Base weight: identity [[1,0],[0,1]]
    var base = Tensor[DType.float32](Shape(4))
    base.set(0, 1.0)
    base.set(1, 0.0)
    base.set(2, 0.0)
    base.set(3, 1.0)

    # LoRA: A=[1,0], B=[[0.5],[0.5]] → delta = 0.5*x0 for both dims
    lora.lora_a.set(0, 1.0)
    lora.lora_a.set(1, 0.0)
    lora.lora_b.set(0, 0.5)
    lora.lora_b.set(1, 0.5)

    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 4.0)
    x.set(1, 6.0)

    # base: W@x = [4, 6]
    # delta: (1/1) * B@(A@x) = B@[4] = [2, 2]
    # total: [6, 8]
    var out = lora_linear(x, base, lora)
    assert_near(out.get(0), 6.0, 0.01, "linear[0] = 4 + 2 = 6")
    assert_near(out.get(1), 8.0, 0.01, "linear[1] = 6 + 2 = 8")

    print("  lora_linear: PASS")


fn test_merge_lora() raises:
    """Test merge_lora adds LoRA into base weights."""
    var config = LoRAConfig(rank=1, alpha=2.0, in_features=2, out_features=2)
    var lora = LoRAWeight(config)

    # A = [1, 0]
    lora.lora_a.set(0, 1.0)
    lora.lora_a.set(1, 0.0)

    # B = [[1], [0]]
    lora.lora_b.set(0, 1.0)
    lora.lora_b.set(1, 0.0)

    # B @ A = [[1, 0], [0, 0]]
    # Scaled by alpha/rank = 2/1 = 2 → [[2, 0], [0, 0]]

    var base = Tensor[DType.float32](Shape(4))
    base.set(0, 1.0)  # W[0,0]
    base.set(1, 1.0)  # W[0,1]
    base.set(2, 1.0)  # W[1,0]
    base.set(3, 1.0)  # W[1,1]

    merge_lora(base, lora)

    # W_merged = W + 2*[[1,0],[0,0]] = [[3,1],[1,1]]
    assert_near(base.get(0), 3.0, 0.01, "merged[0,0] = 1 + 2 = 3")
    assert_near(base.get(1), 1.0, 0.01, "merged[0,1] = 1 + 0 = 1")
    assert_near(base.get(2), 1.0, 0.01, "merged[1,0] = 1 + 0 = 1")
    assert_near(base.get(3), 1.0, 0.01, "merged[1,1] = 1 + 0 = 1")

    print("  merge_lora: PASS")


fn test_unmerge_lora() raises:
    """Test unmerge_lora reverses merge."""
    var config = LoRAConfig(rank=1, alpha=2.0, in_features=2, out_features=2)
    var lora = LoRAWeight(config)

    lora.lora_a.set(0, 1.0)
    lora.lora_a.set(1, 0.0)
    lora.lora_b.set(0, 1.0)
    lora.lora_b.set(1, 0.0)

    # Original base weights
    var base = Tensor[DType.float32](Shape(4))
    base.set(0, 1.0)
    base.set(1, 1.0)
    base.set(2, 1.0)
    base.set(3, 1.0)

    # Merge then unmerge should restore originals
    merge_lora(base, lora)
    unmerge_lora(base, lora)

    assert_near(base.get(0), 1.0, 0.001, "roundtrip[0,0]")
    assert_near(base.get(1), 1.0, 0.001, "roundtrip[0,1]")
    assert_near(base.get(2), 1.0, 0.001, "roundtrip[1,0]")
    assert_near(base.get(3), 1.0, 0.001, "roundtrip[1,1]")

    print("  unmerge_lora: PASS")


fn test_merge_equals_separate() raises:
    """Test that merged weights produce same output as separate LoRA."""
    var config = LoRAConfig(rank=2, alpha=4.0, in_features=3, out_features=2)
    var lora = LoRAWeight(config)

    # Set some non-trivial A weights
    lora.lora_a.set(0, 0.5)
    lora.lora_a.set(1, -0.3)
    lora.lora_a.set(2, 0.1)
    lora.lora_a.set(3, 0.2)
    lora.lora_a.set(4, 0.4)
    lora.lora_a.set(5, -0.6)

    # Set some non-trivial B weights
    lora.lora_b.set(0, 0.7)
    lora.lora_b.set(1, -0.2)
    lora.lora_b.set(2, 0.3)
    lora.lora_b.set(3, 0.8)

    # Base weight [2, 3]
    var base = Tensor[DType.float32](Shape(6))
    base.set(0, 1.0)
    base.set(1, -0.5)
    base.set(2, 0.3)
    base.set(3, 0.2)
    base.set(4, 0.8)
    base.set(5, -0.1)

    var x = Tensor[DType.float32](Shape(3))
    x.set(0, 1.5)
    x.set(1, -0.7)
    x.set(2, 2.0)

    # Method 1: separate computation
    var out_separate = lora_linear(x, base, lora)

    # Method 2: merge into base, then plain matmul
    var merged = Tensor[DType.float32](Shape(6))
    for i in range(6):
        merged.set(i, base.get(i))
    merge_lora(merged, lora)

    var out_merged = Tensor[DType.float32](Shape(2))
    for i in range(2):
        var dot: Float32 = 0.0
        for j in range(3):
            dot += merged.get(i * 3 + j) * x.get(j)
        out_merged.set(i, dot)

    assert_near(out_separate.get(0), out_merged.get(0), 0.001, "merged == separate [0]")
    assert_near(out_separate.get(1), out_merged.get(1), 0.001, "merged == separate [1]")

    print("  merge_equals_separate: PASS")


fn test_lora_higher_rank() raises:
    """Test LoRA with higher rank (4)."""
    var config = LoRAConfig(rank=4, alpha=4.0, in_features=3, out_features=2)
    var lora = LoRAWeight(config)

    # Set A: [4, 3] — all 0.1
    for i in range(12):
        lora.lora_a.set(i, 0.1)

    # Set B: [2, 4] — all 0.2
    for i in range(8):
        lora.lora_b.set(i, 0.2)

    var x = Tensor[DType.float32](Shape(3))
    x.set(0, 1.0)
    x.set(1, 1.0)
    x.set(2, 1.0)

    # A @ x: each of 4 rows = 0.1*3 = 0.3 → hidden = [0.3, 0.3, 0.3, 0.3]
    # B @ hidden: each of 2 rows = 0.2*4*0.3 = 0.24 → [0.24, 0.24]
    # scaling = 4/4 = 1.0
    # output = [0.24, 0.24]
    var out = lora_forward(x, lora)
    assert_near(out.get(0), 0.24, 0.001, "rank4[0] = 0.24")
    assert_near(out.get(1), 0.24, 0.001, "rank4[1] = 0.24")

    print("  lora_higher_rank: PASS")


fn test_lora_zero_input() raises:
    """Test LoRA with zero input returns zero."""
    var config = LoRAConfig(rank=2, alpha=8.0, in_features=4, out_features=3)
    var lora = LoRAWeight(config)

    # Non-zero weights
    for i in range(8):
        lora.lora_a.set(i, 1.0)
    for i in range(6):
        lora.lora_b.set(i, 1.0)

    var x = Tensor[DType.float32](Shape(4))
    # x is all zeros by default

    var out = lora_forward(x, lora)
    for i in range(3):
        assert_near(out.get(i), 0.0, 0.001, "zero input → zero output")

    print("  lora_zero_input: PASS")


fn main() raises:
    print("test_lora:")

    test_lora_config()
    test_lora_weight_creation()
    test_lora_forward_basic()
    test_lora_forward_scaling()
    test_lora_linear()
    test_merge_lora()
    test_unmerge_lora()
    test_merge_equals_separate()
    test_lora_higher_rank()
    test_lora_zero_input()

    print("ALL PASSED")
