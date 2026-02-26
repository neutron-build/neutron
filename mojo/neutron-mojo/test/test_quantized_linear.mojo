# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantized Linear Tests
# ===----------------------------------------------------------------------=== #

"""Tests for quantized linear projections."""

from math import abs
from neutron_mojo.nn.quantized_linear import (
    Q8Weight,
    Q4Weight,
    quantize_weight_q8,
    quantize_weight_q4,
    q8_linear,
    q4_linear,
    quantization_error,
)
from neutron_mojo.nn.transformer import linear
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


fn test_q8_weight_creation() raises:
    """Test Q8Weight struct creation."""
    var qw = Q8Weight(out_features=4, in_features=8, block_size=4)
    assert_true(qw.out_features == 4, "out_features")
    assert_true(qw.in_features == 8, "in_features")
    assert_true(qw.block_size == 4, "block_size")
    assert_true(qw.num_blocks_per_row == 2, "num_blocks 8/4=2")

    print("  q8_weight_creation: PASS")


fn test_q8_quantize_identity() raises:
    """Test Q8 quantization of identity-like matrix."""
    var w = Tensor[DType.float32](Shape(2, 2))
    w.set(0, 1.0)   # [0,0]
    w.set(1, 0.0)   # [0,1]
    w.set(2, 0.0)   # [1,0]
    w.set(3, 1.0)   # [1,1]

    var qw = quantize_weight_q8(w, 2, 2, block_size=2)

    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 3.0)
    x.set(1, 5.0)

    var y = q8_linear(x, qw)
    # Should approximate identity: y ≈ [3, 5]
    assert_near(y.get(0), 3.0, 0.1, "q8 identity row 0")
    assert_near(y.get(1), 5.0, 0.1, "q8 identity row 1")

    print("  q8_quantize_identity: PASS")


fn test_q8_linear_vs_fp32() raises:
    """Test that Q8 linear matches FP32 linear closely."""
    var w = Tensor[DType.float32](Shape(3, 4))
    # Fill with known values
    w.set(0, 0.5)
    w.set(1, -0.3)
    w.set(2, 0.8)
    w.set(3, -0.1)
    w.set(4, 0.2)
    w.set(5, 0.7)
    w.set(6, -0.5)
    w.set(7, 0.4)
    w.set(8, -0.6)
    w.set(9, 0.1)
    w.set(10, 0.3)
    w.set(11, -0.9)

    var x = Tensor[DType.float32](Shape(4))
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, -1.0)
    x.set(3, 0.5)

    # FP32 reference
    var y_ref = linear(x, w)

    # Q8 version
    var qw = quantize_weight_q8(w, 3, 4, block_size=4)
    var y_q8 = q8_linear(x, qw)

    # Should be close (Q8_0 has ~0.5% error for normal distributions)
    var err = quantization_error(y_ref, y_q8, 3)
    assert_true(err < 0.1, "Q8 error < 0.1: " + String(err))

    print("  q8_linear_vs_fp32: PASS")


fn test_q8_large_values() raises:
    """Test Q8 with larger weight values."""
    var w = Tensor[DType.float32](Shape(2, 4))
    w.set(0, 10.0)
    w.set(1, -20.0)
    w.set(2, 15.0)
    w.set(3, -5.0)
    w.set(4, 8.0)
    w.set(5, -12.0)
    w.set(6, 3.0)
    w.set(7, -7.0)

    var x = Tensor[DType.float32](Shape(4))
    x.set(0, 0.1)
    x.set(1, 0.2)
    x.set(2, 0.3)
    x.set(3, 0.4)

    var y_ref = linear(x, w)
    var qw = quantize_weight_q8(w, 2, 4, block_size=4)
    var y_q8 = q8_linear(x, qw)

    var err = quantization_error(y_ref, y_q8, 2)
    assert_true(err < 0.5, "Q8 large values error: " + String(err))

    print("  q8_large_values: PASS")


fn test_q4_weight_creation() raises:
    """Test Q4Weight struct creation."""
    var qw = Q4Weight(out_features=4, in_features=16, block_size=8)
    assert_true(qw.num_blocks_per_row == 2, "num_blocks 16/8=2")

    print("  q4_weight_creation: PASS")


fn test_q4_linear_vs_fp32() raises:
    """Test Q4 linear (lower precision, higher error tolerated)."""
    var w = Tensor[DType.float32](Shape(2, 4))
    w.set(0, 0.5)
    w.set(1, -0.3)
    w.set(2, 0.8)
    w.set(3, -0.1)
    w.set(4, 0.2)
    w.set(5, 0.7)
    w.set(6, -0.5)
    w.set(7, 0.4)

    var x = Tensor[DType.float32](Shape(4))
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, -1.0)
    x.set(3, 0.5)

    var y_ref = linear(x, w)
    var qw = quantize_weight_q4(w, 2, 4, block_size=4)
    var y_q4 = q4_linear(x, qw)

    # Q4 has more error than Q8 — ~5-10%
    var err = quantization_error(y_ref, y_q4, 2)
    assert_true(err < 0.5, "Q4 error < 0.5: " + String(err))

    print("  q4_linear_vs_fp32: PASS")


fn test_q8_zero_weights() raises:
    """Test Q8 with zero weight matrix."""
    var w = Tensor[DType.float32](Shape(2, 2))
    # All zeros

    var qw = quantize_weight_q8(w, 2, 2, block_size=2)
    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 5.0)
    x.set(1, 3.0)

    var y = q8_linear(x, qw)
    assert_near(y.get(0), 0.0, 0.01, "zero weight row 0")
    assert_near(y.get(1), 0.0, 0.01, "zero weight row 1")

    print("  q8_zero_weights: PASS")


fn test_q8_multi_block() raises:
    """Test Q8 with multiple blocks per row."""
    # 2 rows, 8 cols, block_size=4 → 2 blocks per row
    var w = Tensor[DType.float32](Shape(2, 8))
    for i in range(16):
        w.set(i, Float32(i) * 0.1 - 0.8)

    var x = Tensor[DType.float32](Shape(8))
    for i in range(8):
        x.set(i, 1.0)

    var y_ref = linear(x, w)
    var qw = quantize_weight_q8(w, 2, 8, block_size=4)
    var y_q8 = q8_linear(x, qw)

    var err = quantization_error(y_ref, y_q8, 2)
    assert_true(err < 0.15, "multi-block Q8 error: " + String(err))

    print("  q8_multi_block: PASS")


fn test_quantization_error_fn() raises:
    """Test the error measurement function."""
    var a = Tensor[DType.float32](Shape(3))
    var b = Tensor[DType.float32](Shape(3))
    a.set(0, 1.0)
    a.set(1, 2.0)
    a.set(2, 3.0)
    b.set(0, 1.1)
    b.set(1, 1.9)
    b.set(2, 3.2)

    var err = quantization_error(a, b, 3)
    # |0.1| + |0.1| + |0.2| = 0.4 / 3 ≈ 0.133
    assert_near(err, 0.1333, 0.01, "error measurement")

    print("  quantization_error_fn: PASS")


fn main() raises:
    print("test_quantized_linear:")

    test_q8_weight_creation()
    test_q8_quantize_identity()
    test_q8_linear_vs_fp32()
    test_q8_large_values()
    test_q4_weight_creation()
    test_q4_linear_vs_fp32()
    test_q8_zero_weights()
    test_q8_multi_block()
    test_quantization_error_fn()

    print("ALL PASSED")
