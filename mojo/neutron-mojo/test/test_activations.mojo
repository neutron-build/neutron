# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Activation function tests
# ===----------------------------------------------------------------------=== #

"""Tests for GeLU, SiLU/Swish, and SwiGLU activation functions.

Reference: reference/activations.py
Tolerance: FP32 1e-6
"""

from neutron_mojo.tensor import Tensor, gelu, silu, swiglu, relu


fn assert_close[dtype: DType](
    a: Scalar[dtype], b: Scalar[dtype], rtol: Float64 = 1e-5, atol: Float64 = 1e-8
) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error(
            "Values not close: " + String(a) + " vs " + String(b) + " (diff=" + String(diff) + ")"
        )


fn assert_true(cond: Bool) raises:
    if not cond:
        raise Error("Assertion failed: condition is False")


# ===----------------------------------------------------------------------=== #
# GeLU tests
# ===----------------------------------------------------------------------=== #


fn test_gelu_zero() raises:
    """GeLU(0) = 0."""
    var x = Tensor[DType.float32](1)
    x.data_ptr().store(0, Float32(0.0))

    var out = gelu(x)
    assert_close(out.get(0), Float32(0.0), atol=1e-7)
    _ = out.numel()  # keepalive
    _ = x.numel()

    print("  gelu_zero: PASS")


fn test_gelu_large_positive() raises:
    """GeLU(x) ≈ x for large positive x."""
    var x = Tensor[DType.float32](1)
    x.data_ptr().store(0, Float32(10.0))

    var out = gelu(x)
    assert_close(out.get(0), Float32(10.0), rtol=1e-4)
    _ = out.numel()
    _ = x.numel()

    print("  gelu_large_positive: PASS")


fn test_gelu_large_negative() raises:
    """GeLU(x) ≈ 0 for large negative x."""
    var x = Tensor[DType.float32](1)
    x.data_ptr().store(0, Float32(-10.0))

    var out = gelu(x)
    assert_close(out.get(0), Float32(0.0), atol=1e-4)
    _ = out.numel()
    _ = x.numel()

    print("  gelu_large_negative: PASS")


fn test_gelu_properties() raises:
    """GeLU properties: smooth, approximately x for x>3, approximately 0 for x<-3."""
    var x = Tensor[DType.float32](5)
    x.data_ptr().store(0, Float32(-5.0))
    x.data_ptr().store(1, Float32(-1.0))
    x.data_ptr().store(2, Float32(0.0))
    x.data_ptr().store(3, Float32(1.0))
    x.data_ptr().store(4, Float32(5.0))

    var out = gelu(x)

    # GeLU(-5) should be near 0
    assert_true(abs(out.get(0)) < Float32(1e-3))
    # GeLU(0) = 0
    assert_close(out.get(2), Float32(0.0), atol=1e-6)
    # GeLU(5) should be near 5
    assert_close(out.get(4), Float32(5.0), rtol=1e-3)
    _ = out.numel()
    _ = x.numel()

    print("  gelu_properties: PASS")


# ===----------------------------------------------------------------------=== #
# SiLU tests
# ===----------------------------------------------------------------------=== #


fn test_silu_zero() raises:
    """SiLU(0) = 0."""
    var x = Tensor[DType.float32](1)
    x.data_ptr().store(0, Float32(0.0))

    var out = silu(x)
    assert_close(out.get(0), Float32(0.0), atol=1e-7)
    _ = out.numel()
    _ = x.numel()

    print("  silu_zero: PASS")


fn test_silu_large_positive() raises:
    """SiLU(x) ≈ x for large positive x."""
    var x = Tensor[DType.float32](1)
    x.data_ptr().store(0, Float32(10.0))

    var out = silu(x)
    # For x=10, sigmoid(10)≈1, so SiLU≈10
    assert_close(out.get(0), Float32(10.0), rtol=1e-3)
    _ = out.numel()
    _ = x.numel()

    print("  silu_large_positive: PASS")


fn test_silu_large_negative() raises:
    """SiLU(x) ≈ 0 for large negative x."""
    var x = Tensor[DType.float32](1)
    x.data_ptr().store(0, Float32(-10.0))

    var out = silu(x)
    assert_close(out.get(0), Float32(0.0), atol=1e-3)
    _ = out.numel()
    _ = x.numel()

    print("  silu_large_negative: PASS")


fn test_silu_specific_value() raises:
    """SiLU(1) ≈ 0.7311 (sigmoid(1) * 1)."""
    var x = Tensor[DType.float32](1)
    x.data_ptr().store(0, Float32(1.0))

    var out = silu(x)
    # sigmoid(1) ≈ 0.7311, so SiLU(1) ≈ 0.7311
    assert_close(out.get(0), Float32(0.7311), rtol=1e-3)
    _ = out.numel()
    _ = x.numel()

    print("  silu_specific_value: PASS")


# ===----------------------------------------------------------------------=== #
# SwiGLU tests
# ===----------------------------------------------------------------------=== #


fn test_swiglu_gate_zero() raises:
    """SwiGLU with gate=0 should output 0 (since silu(0)=0)."""
    var x = Tensor[DType.float32](3)
    x.data_ptr().store(0, Float32(1.0))
    x.data_ptr().store(1, Float32(2.0))
    x.data_ptr().store(2, Float32(3.0))

    var gate = Tensor[DType.float32](3)
    gate.data_ptr().store(0, Float32(0.0))
    gate.data_ptr().store(1, Float32(0.0))
    gate.data_ptr().store(2, Float32(0.0))

    var out = swiglu(x, gate)

    # silu(0) = 0, so all outputs should be 0
    assert_close(out.get(0), Float32(0.0), atol=1e-6)
    assert_close(out.get(1), Float32(0.0), atol=1e-6)
    assert_close(out.get(2), Float32(0.0), atol=1e-6)
    _ = out.numel()
    _ = x.numel()
    _ = gate.numel()

    print("  swiglu_gate_zero: PASS")


fn test_swiglu_gate_one() raises:
    """SwiGLU with gate=1: output ≈ x * 0.7311."""
    var x = Tensor[DType.float32](2)
    x.data_ptr().store(0, Float32(2.0))
    x.data_ptr().store(1, Float32(3.0))

    var gate = Tensor[DType.float32](2)
    gate.data_ptr().store(0, Float32(1.0))
    gate.data_ptr().store(1, Float32(1.0))

    var out = swiglu(x, gate)

    # silu(1) ≈ 0.7311
    var silu_1 = Float32(0.7311)
    assert_close(out.data_ptr().load(0), Float32(2.0 * silu_1), rtol=1e-3)
    assert_close(out.data_ptr().load(1), Float32(3.0 * silu_1), rtol=1e-3)
    _ = out.numel()  # keepalive
    _ = x.numel()
    _ = gate.numel()

    print("  swiglu_gate_one: PASS")


fn test_swiglu_large_gate() raises:
    """SwiGLU with large positive gate: output ≈ x (since silu(large)≈gate≈x when normalized)."""
    var x = Tensor[DType.float32](2)
    x.data_ptr().store(0, Float32(1.0))
    x.data_ptr().store(1, Float32(2.0))

    var gate = Tensor[DType.float32](2)
    gate.data_ptr().store(0, Float32(10.0))
    gate.data_ptr().store(1, Float32(10.0))

    var out = swiglu(x, gate)

    # silu(10) ≈ 10, so output ≈ x * 10
    assert_close(out.data_ptr().load(0), Float32(10.0), rtol=1e-2)
    assert_close(out.data_ptr().load(1), Float32(20.0), rtol=1e-2)
    _ = out.numel()  # keepalive
    _ = x.numel()
    _ = gate.numel()

    print("  swiglu_large_gate: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #


fn main() raises:
    print("test_activations:")

    # GeLU
    test_gelu_zero()
    test_gelu_large_positive()
    test_gelu_large_negative()
    test_gelu_properties()

    # SiLU
    test_silu_zero()
    test_silu_large_positive()
    test_silu_large_negative()
    test_silu_specific_value()

    # SwiGLU
    test_swiglu_gate_zero()
    test_swiglu_gate_one()
    test_swiglu_large_gate()

    print("ALL PASSED")
