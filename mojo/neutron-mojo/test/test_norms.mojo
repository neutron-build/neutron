# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Normalization tests
# ===----------------------------------------------------------------------=== #

"""Tests for RMSNorm and LayerNorm operations.

Reference implementations: reference/rmsnorm.py, reference/layernorm.py
Tolerance: FP32 1e-6, FP16 1e-3
"""

from math import sqrt
from neutron_mojo.tensor import Tensor, rmsnorm, layernorm


fn assert_equal[dtype: DType](a: Scalar[dtype], b: Scalar[dtype]) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + String(a) + " != " + String(b)
        )


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
# RMSNorm tests
# ===----------------------------------------------------------------------=== #


fn test_rmsnorm_basic() raises:
    """Basic RMSNorm correctness: [1, 2, 3, 4] with gamma=1."""
    var x = Tensor[DType.float32](4)
    x.data_ptr().store(0, Float32(1.0))
    x.data_ptr().store(1, Float32(2.0))
    x.data_ptr().store(2, Float32(3.0))
    x.data_ptr().store(3, Float32(4.0))

    var gamma = Tensor[DType.float32](4)
    for i in range(4):
        gamma.data_ptr().store(i, Float32(1.0))

    var out = rmsnorm(x, gamma)

    # Compute expected: RMS = sqrt(mean([1, 4, 9, 16]) + 1e-6) = sqrt(7.5 + 1e-6) ≈ 2.7386
    var mean_sq = (1.0 + 4.0 + 9.0 + 16.0) / 4.0  # 7.5
    var rms = sqrt(mean_sq + 1e-6)
    var expected_0 = 1.0 / rms
    var expected_1 = 2.0 / rms
    var expected_2 = 3.0 / rms
    var expected_3 = 4.0 / rms

    assert_close(out.data_ptr().load(0), Float32(expected_0), rtol=1e-5)
    assert_close(out.data_ptr().load(1), Float32(expected_1), rtol=1e-5)
    assert_close(out.data_ptr().load(2), Float32(expected_2), rtol=1e-5)
    assert_close(out.data_ptr().load(3), Float32(expected_3), rtol=1e-5)

    print("  rmsnorm_basic: PASS")


fn test_rmsnorm_scale() raises:
    """RMSNorm with gamma scaling: all-ones input should output gamma."""
    var x = Tensor[DType.float32](2, 4)
    for i in range(8):
        x.data_ptr().store(i, Float32(1.0))

    var gamma = Tensor[DType.float32](4)
    gamma.data_ptr().store(0, Float32(1.0))
    gamma.data_ptr().store(1, Float32(2.0))
    gamma.data_ptr().store(2, Float32(3.0))
    gamma.data_ptr().store(3, Float32(4.0))

    var out = rmsnorm(x, gamma)

    # RMS of all-ones is sqrt(1.0 + 1e-6) ≈ 1.0, so output should be x / 1.0 * gamma = gamma
    # First row
    assert_close(out.data_ptr().load(0), Float32(1.0), rtol=1e-5)
    assert_close(out.data_ptr().load(1), Float32(2.0), rtol=1e-5)
    assert_close(out.data_ptr().load(2), Float32(3.0), rtol=1e-5)
    assert_close(out.data_ptr().load(3), Float32(4.0), rtol=1e-5)
    # Second row (same)
    assert_close(out.data_ptr().load(4), Float32(1.0), rtol=1e-5)
    assert_close(out.data_ptr().load(5), Float32(2.0), rtol=1e-5)
    assert_close(out.data_ptr().load(6), Float32(3.0), rtol=1e-5)
    assert_close(out.data_ptr().load(7), Float32(4.0), rtol=1e-5)

    print("  rmsnorm_scale: PASS")


fn test_rmsnorm_2d() raises:
    """2D RMSNorm: (2, 4) tensor normalized row-wise."""
    var x = Tensor[DType.float32](2, 4)
    # Row 0: [1, 2, 3, 4]
    x.data_ptr().store(0, Float32(1.0))
    x.data_ptr().store(1, Float32(2.0))
    x.data_ptr().store(2, Float32(3.0))
    x.data_ptr().store(3, Float32(4.0))
    # Row 1: [5, 6, 7, 8]
    x.data_ptr().store(4, Float32(5.0))
    x.data_ptr().store(5, Float32(6.0))
    x.data_ptr().store(6, Float32(7.0))
    x.data_ptr().store(7, Float32(8.0))

    var gamma = Tensor[DType.float32](4)
    for i in range(4):
        gamma.data_ptr().store(i, Float32(1.0))

    var out = rmsnorm(x, gamma)

    # Row 0: RMS = sqrt((1+4+9+16)/4 + 1e-6) = sqrt(7.5 + 1e-6)
    var rms0 = sqrt(7.5 + 1e-6)
    assert_close(out.data_ptr().load(0), Float32(1.0 / rms0), rtol=1e-5)
    assert_close(out.data_ptr().load(1), Float32(2.0 / rms0), rtol=1e-5)
    assert_close(out.data_ptr().load(2), Float32(3.0 / rms0), rtol=1e-5)
    assert_close(out.data_ptr().load(3), Float32(4.0 / rms0), rtol=1e-5)

    # Row 1: RMS = sqrt((25+36+49+64)/4 + 1e-6) = sqrt(43.5 + 1e-6)
    var rms1 = sqrt(43.5 + 1e-6)
    assert_close(out.data_ptr().load(4), Float32(5.0 / rms1), rtol=1e-5)
    assert_close(out.data_ptr().load(5), Float32(6.0 / rms1), rtol=1e-5)
    assert_close(out.data_ptr().load(6), Float32(7.0 / rms1), rtol=1e-5)
    assert_close(out.data_ptr().load(7), Float32(8.0 / rms1), rtol=1e-5)

    print("  rmsnorm_2d: PASS")


fn test_rmsnorm_stability() raises:
    """RMSNorm numerical stability with large values."""
    var x = Tensor[DType.float32](4)
    x.data_ptr().store(0, Float32(1000.0))
    x.data_ptr().store(1, Float32(2000.0))
    x.data_ptr().store(2, Float32(3000.0))
    x.data_ptr().store(3, Float32(4000.0))

    var gamma = Tensor[DType.float32](4)
    for i in range(4):
        gamma.data_ptr().store(i, Float32(1.0))

    var out = rmsnorm(x, gamma)

    # Verify scale is reasonable (output should be roughly unit norm)
    var sum_sq_out = Float64(0.0)
    for i in range(4):
        var v = Float64(out.data_ptr().load(i))
        sum_sq_out += v * v
    var rms_out = sqrt(sum_sq_out / 4.0)
    assert_close(Float32(rms_out), Float32(1.0), rtol=1e-4)

    # Values should be finite and reasonable magnitude (not NaN/Inf)
    for i in range(4):
        var val = out.data_ptr().load(i)
        assert_true(abs(val) < Float32(100.0))  # Reasonable range after normalization

    print("  rmsnorm_stability: PASS")


# ===----------------------------------------------------------------------=== #
# LayerNorm tests
# ===----------------------------------------------------------------------=== #


fn test_layernorm_basic() raises:
    """Basic LayerNorm correctness: [1, 2, 3, 4] with gamma=1, beta=0."""
    var x = Tensor[DType.float32](4)
    x.data_ptr().store(0, Float32(1.0))
    x.data_ptr().store(1, Float32(2.0))
    x.data_ptr().store(2, Float32(3.0))
    x.data_ptr().store(3, Float32(4.0))

    var gamma = Tensor[DType.float32](4)
    var beta = Tensor[DType.float32](4)
    for i in range(4):
        gamma.data_ptr().store(i, Float32(1.0))
        beta.data_ptr().store(i, Float32(0.0))

    var out = layernorm(x, gamma, beta)

    # Expected: mean=2.5, var=1.25, std=sqrt(1.25+1e-5)
    # Output = (x - 2.5) / sqrt(1.25+1e-5)
    var mean = 2.5
    var variance = 1.25
    var std_inv = 1.0 / sqrt(variance + 1e-5)
    var expected_0 = (1.0 - mean) * std_inv
    var expected_1 = (2.0 - mean) * std_inv
    var expected_2 = (3.0 - mean) * std_inv
    var expected_3 = (4.0 - mean) * std_inv

    assert_close(out.data_ptr().load(0), Float32(expected_0), rtol=1e-5)
    assert_close(out.data_ptr().load(1), Float32(expected_1), rtol=1e-5)
    assert_close(out.data_ptr().load(2), Float32(expected_2), rtol=1e-5)
    assert_close(out.data_ptr().load(3), Float32(expected_3), rtol=1e-5)

    print("  layernorm_basic: PASS")


fn test_layernorm_zero_mean_unit_var() raises:
    """LayerNorm output should have mean≈0 and var≈1 when gamma=1, beta=0."""
    var x = Tensor[DType.float32](8)
    x.data_ptr().store(0, Float32(1.0))
    x.data_ptr().store(1, Float32(2.0))
    x.data_ptr().store(2, Float32(3.0))
    x.data_ptr().store(3, Float32(4.0))
    x.data_ptr().store(4, Float32(5.0))
    x.data_ptr().store(5, Float32(6.0))
    x.data_ptr().store(6, Float32(7.0))
    x.data_ptr().store(7, Float32(8.0))

    var gamma = Tensor[DType.float32](8)
    var beta = Tensor[DType.float32](8)
    for i in range(8):
        gamma.data_ptr().store(i, Float32(1.0))
        beta.data_ptr().store(i, Float32(0.0))

    var out = layernorm(x, gamma, beta)

    # Check mean ≈ 0
    var sum_out = Float64(0.0)
    for i in range(8):
        sum_out += Float64(out.data_ptr().load(i))
    var mean_out = sum_out / 8.0
    assert_close(Float32(mean_out), Float32(0.0), atol=1e-5)

    # Check variance ≈ 1
    var sum_sq_diff = Float64(0.0)
    for i in range(8):
        var diff = Float64(out.data_ptr().load(i)) - mean_out
        sum_sq_diff += diff * diff
    var var_out = sum_sq_diff / 8.0
    assert_close(Float32(var_out), Float32(1.0), rtol=1e-2)

    print("  layernorm_zero_mean_unit_var: PASS")


fn test_layernorm_affine() raises:
    """LayerNorm with gamma and beta scaling/shifting."""
    var x = Tensor[DType.float32](4)
    x.data_ptr().store(0, Float32(0.0))
    x.data_ptr().store(1, Float32(1.0))
    x.data_ptr().store(2, Float32(2.0))
    x.data_ptr().store(3, Float32(3.0))

    var gamma = Tensor[DType.float32](4)
    var beta = Tensor[DType.float32](4)
    for i in range(4):
        gamma.data_ptr().store(i, Float32(2.0))
        beta.data_ptr().store(i, Float32(1.0))

    var out = layernorm(x, gamma, beta)

    # After normalization (mean=0, var=1), scale by 2 and shift by 1
    # Mean should be approximately 1.0
    var sum_out = Float64(0.0)
    for i in range(4):
        sum_out += Float64(out.data_ptr().load(i))
    var mean_out = sum_out / 4.0
    assert_close(Float32(mean_out), Float32(1.0), rtol=1e-2)

    print("  layernorm_affine: PASS")


fn test_layernorm_2d() raises:
    """2D LayerNorm: (2, 4) tensor normalized row-wise."""
    var x = Tensor[DType.float32](2, 4)
    # Row 0: [1, 2, 3, 4]
    x.data_ptr().store(0, Float32(1.0))
    x.data_ptr().store(1, Float32(2.0))
    x.data_ptr().store(2, Float32(3.0))
    x.data_ptr().store(3, Float32(4.0))
    # Row 1: [5, 6, 7, 8]
    x.data_ptr().store(4, Float32(5.0))
    x.data_ptr().store(5, Float32(6.0))
    x.data_ptr().store(6, Float32(7.0))
    x.data_ptr().store(7, Float32(8.0))

    var gamma = Tensor[DType.float32](4)
    var beta = Tensor[DType.float32](4)
    for i in range(4):
        gamma.data_ptr().store(i, Float32(1.0))
        beta.data_ptr().store(i, Float32(0.0))

    var out = layernorm(x, gamma, beta)

    # Each row should have mean≈0, var≈1
    # Row 0
    var sum0 = Float64(0.0)
    for j in range(4):
        sum0 += Float64(out.data_ptr().load(j))
    assert_close(Float32(sum0 / 4.0), Float32(0.0), atol=1e-5)

    # Row 1
    var sum1 = Float64(0.0)
    for j in range(4):
        sum1 += Float64(out.data_ptr().load(4 + j))
    assert_close(Float32(sum1 / 4.0), Float32(0.0), atol=1e-5)

    print("  layernorm_2d: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #


fn main() raises:
    print("test_norms:")

    # RMSNorm
    test_rmsnorm_basic()
    test_rmsnorm_scale()
    test_rmsnorm_2d()
    test_rmsnorm_stability()

    # LayerNorm
    test_layernorm_basic()
    test_layernorm_zero_mean_unit_var()
    test_layernorm_affine()
    test_layernorm_2d()

    print("ALL PASSED")
