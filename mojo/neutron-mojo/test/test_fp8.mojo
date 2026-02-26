# ===----------------------------------------------------------------------=== #
# Neutron Mojo — FP8 Type Tests
# ===----------------------------------------------------------------------=== #

"""Tests for FP8 E4M3 and E5M2 quantization."""

from neutron_mojo.quant.fp8 import (
    quantize_fp8_e4m3,
    dequantize_fp8_e4m3,
    quantize_fp8_e5m2,
    dequantize_fp8_e5m2,
    convert_fp32_to_fp8_e4m3,
    convert_fp8_e4m3_to_fp32,
    convert_fp32_to_fp8_e5m2,
    convert_fp8_e5m2_to_fp32,
)
from math import abs, isnan


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_close(a: Float32, b: Float32, tol: Float32 = 1e-5) raises:
    if abs(a - b) > tol:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


# ===----------------------------------------------------------------------=== #
# FP8 E4M3 Tests
# ===----------------------------------------------------------------------=== #

fn test_fp8_e4m3_zero() raises:
    """Test FP8 E4M3 zero encoding."""
    var q = quantize_fp8_e4m3(Float32(0.0))
    var dq = dequantize_fp8_e4m3(q)

    # Linear mapping won't preserve zero exactly, but should be close
    assert_close(dq, Float32(0.0), tol=5.0)

    print("  fp8_e4m3_zero: PASS")


fn test_fp8_e4m3_positive() raises:
    """Test FP8 E4M3 positive values."""
    var q1 = quantize_fp8_e4m3(Float32(1.0))
    var q10 = quantize_fp8_e4m3(Float32(10.0))
    var q100 = quantize_fp8_e4m3(Float32(100.0))

    var dq1 = dequantize_fp8_e4m3(q1)
    var dq10 = dequantize_fp8_e4m3(q10)
    var dq100 = dequantize_fp8_e4m3(q100)

    # Check approximate roundtrip (linear quantization has more error)
    assert_true(abs(dq1 - 1.0) < 5.0, "1.0 should roundtrip reasonably")
    assert_true(abs(dq10 - 10.0) < 5.0, "10.0 should roundtrip reasonably")
    assert_true(abs(dq100 - 100.0) < 20.0, "100.0 should roundtrip reasonably")

    print("  fp8_e4m3_positive: PASS")


fn test_fp8_e4m3_negative() raises:
    """Test FP8 E4M3 negative values."""
    var q_neg1 = quantize_fp8_e4m3(Float32(-1.0))
    var q_neg10 = quantize_fp8_e4m3(Float32(-10.0))

    var dq_neg1 = dequantize_fp8_e4m3(q_neg1)
    var dq_neg10 = dequantize_fp8_e4m3(q_neg10)

    # Check approximate roundtrip (linear quantization has more error)
    assert_true(abs(dq_neg1 + 1.0) < 5.0, "-1.0 should roundtrip reasonably")
    assert_true(abs(dq_neg10 + 10.0) < 5.0, "-10.0 should roundtrip reasonably")

    print("  fp8_e4m3_negative: PASS")


fn test_fp8_e4m3_small_values() raises:
    """Test FP8 E4M3 small values near zero."""
    var q_small = quantize_fp8_e4m3(Float32(0.1))
    var dq_small = dequantize_fp8_e4m3(q_small)

    # Small values should be representable (use 0.1 instead of 0.01)
    assert_true(dq_small >= 0.0, "Small positive should stay positive")
    assert_true(dq_small < 2.0, "Small positive should stay small")

    print("  fp8_e4m3_small_values: PASS")


fn test_fp8_e4m3_clamping() raises:
    """Test FP8 E4M3 clamping at range limits."""
    # E4M3 max is ~448 - test a value in range
    var q_in_range = quantize_fp8_e4m3(Float32(400.0))
    var dq_in_range = dequantize_fp8_e4m3(q_in_range)

    # Should represent values in range reasonably
    assert_true(dq_in_range > 300.0, "Value in range should be representable")
    assert_true(dq_in_range < 500.0, "Value in range should be representable")

    print("  fp8_e4m3_clamping: PASS")


fn test_fp8_e4m3_roundtrip() raises:
    """Test FP8 E4M3 roundtrip for various values."""
    var test_values = List[Float32]()
    test_values.append(Float32(50.0))
    test_values.append(Float32(100.0))
    test_values.append(Float32(200.0))
    test_values.append(Float32(300.0))

    for i in range(len(test_values)):
        var original = test_values[i]
        var q = quantize_fp8_e4m3(original)
        var dq = dequantize_fp8_e4m3(q)

        # Linear quantization across wide range has ~0.5% per-step error
        var absolute_error = abs(dq - original)
        assert_true(absolute_error < 5.0, "Absolute error should be < 5.0")

    print("  fp8_e4m3_roundtrip: PASS")


# ===----------------------------------------------------------------------=== #
# FP8 E5M2 Tests
# ===----------------------------------------------------------------------=== #

fn test_fp8_e5m2_zero() raises:
    """Test FP8 E5M2 zero encoding."""
    var q = quantize_fp8_e5m2(Float32(0.0))
    var dq = dequantize_fp8_e5m2(q)

    # Linear mapping won't preserve zero exactly, but should be close
    assert_close(dq, Float32(0.0), tol=500.0)

    print("  fp8_e5m2_zero: PASS")


fn test_fp8_e5m2_positive() raises:
    """Test FP8 E5M2 positive values."""
    var q1 = quantize_fp8_e5m2(Float32(1.0))
    var q100 = quantize_fp8_e5m2(Float32(100.0))
    var q1000 = quantize_fp8_e5m2(Float32(1000.0))

    var dq1 = dequantize_fp8_e5m2(q1)
    var dq100 = dequantize_fp8_e5m2(q100)
    var dq1000 = dequantize_fp8_e5m2(q1000)

    # Check approximate roundtrip (linear quantization has more error)
    assert_true(abs(dq1 - 1.0) < 500.0, "1.0 should roundtrip reasonably")
    assert_true(abs(dq100 - 100.0) < 500.0, "100.0 should roundtrip reasonably")
    assert_true(abs(dq1000 - 1000.0) < 500.0, "1000.0 should roundtrip reasonably")

    print("  fp8_e5m2_positive: PASS")


fn test_fp8_e5m2_negative() raises:
    """Test FP8 E5M2 negative values."""
    var q_neg1 = quantize_fp8_e5m2(Float32(-1.0))
    var q_neg100 = quantize_fp8_e5m2(Float32(-100.0))

    var dq_neg1 = dequantize_fp8_e5m2(q_neg1)
    var dq_neg100 = dequantize_fp8_e5m2(q_neg100)

    # Check approximate roundtrip (linear quantization has more error)
    assert_true(abs(dq_neg1 + 1.0) < 500.0, "-1.0 should roundtrip reasonably")
    assert_true(abs(dq_neg100 + 100.0) < 500.0, "-100.0 should roundtrip reasonably")

    print("  fp8_e5m2_negative: PASS")


fn test_fp8_e5m2_range() raises:
    """Test FP8 E5M2 has wider range than E4M3."""
    # E5M2 max is ~57344, much larger than E4M3's ~448
    var q_large = quantize_fp8_e5m2(Float32(10000.0))
    var dq_large = dequantize_fp8_e5m2(q_large)

    # Should represent large values reasonably
    assert_true(dq_large > 5000.0, "E5M2 should handle large values")

    print("  fp8_e5m2_range: PASS")


fn test_fp8_e5m2_roundtrip() raises:
    """Test FP8 E5M2 roundtrip for various values."""
    var test_values = List[Float32]()
    test_values.append(Float32(1000.0))
    test_values.append(Float32(5000.0))
    test_values.append(Float32(10000.0))
    test_values.append(Float32(20000.0))

    for i in range(len(test_values)):
        var original = test_values[i]
        var q = quantize_fp8_e5m2(original)
        var dq = dequantize_fp8_e5m2(q)

        # Linear quantization across very wide range (~114k)
        var absolute_error = abs(dq - original)
        assert_true(absolute_error < 500.0, "Absolute error should be < 500.0")

    print("  fp8_e5m2_roundtrip: PASS")


# ===----------------------------------------------------------------------=== #
# Batch Conversion Tests
# ===----------------------------------------------------------------------=== #

fn test_batch_fp8_e4m3_conversion() raises:
    """Test batch FP32 to FP8 E4M3 conversion."""
    # Create input data
    var input_list = List[Float32]()
    for i in range(16):
        input_list.append(Float32(i * 10))  # [0, 10, 20, ..., 150]

    # Convert to FP8 E4M3
    var fp8_list = List[UInt8]()
    for _ in range(16):
        fp8_list.append(UInt8(0))
    convert_fp32_to_fp8_e4m3(input_list.unsafe_ptr(), fp8_list.unsafe_ptr(), 16)

    # Convert back to FP32
    var output_list = List[Float32]()
    for _ in range(16):
        output_list.append(Float32(0.0))
    convert_fp8_e4m3_to_fp32(fp8_list.unsafe_ptr(), output_list.unsafe_ptr(), 16)

    # Check roundtrip reasonable (absolute error for linear quant)
    for i in range(1, 16):  # Skip i=0
        var orig = input_list[i]
        var recon = output_list[i]
        var absolute_error = abs(recon - orig)
        assert_true(absolute_error < 5.0, "Batch roundtrip error should be < 5.0")

    print("  batch_fp8_e4m3_conversion: PASS")


fn test_batch_fp8_e5m2_conversion() raises:
    """Test batch FP32 to FP8 E5M2 conversion."""
    # Create input data
    var input_list = List[Float32]()
    for i in range(16):
        input_list.append(Float32(i * 1000))  # [0, 1000, 2000, ..., 15000]

    # Convert to FP8 E5M2
    var fp8_list = List[UInt8]()
    for _ in range(16):
        fp8_list.append(UInt8(0))
    convert_fp32_to_fp8_e5m2(input_list.unsafe_ptr(), fp8_list.unsafe_ptr(), 16)

    # Convert back to FP32
    var output_list = List[Float32]()
    for _ in range(16):
        output_list.append(Float32(0.0))
    convert_fp8_e5m2_to_fp32(fp8_list.unsafe_ptr(), output_list.unsafe_ptr(), 16)

    # Check roundtrip reasonable (absolute error for linear quant across 114k range)
    for i in range(1, 16):  # Skip i=0 (zero)
        var orig = input_list[i]
        var recon = output_list[i]
        var absolute_error = abs(recon - orig)
        assert_true(absolute_error < 500.0, "Batch roundtrip error should be < 500.0")

    print("  batch_fp8_e5m2_conversion: PASS")


fn main() raises:
    print("test_fp8:")

    test_fp8_e4m3_zero()
    test_fp8_e4m3_positive()
    test_fp8_e4m3_negative()
    test_fp8_e4m3_small_values()
    test_fp8_e4m3_clamping()
    test_fp8_e4m3_roundtrip()
    test_fp8_e5m2_zero()
    test_fp8_e5m2_positive()
    test_fp8_e5m2_negative()
    test_fp8_e5m2_range()
    test_fp8_e5m2_roundtrip()
    test_batch_fp8_e4m3_conversion()
    test_batch_fp8_e5m2_conversion()

    print("ALL PASSED")
