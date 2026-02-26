# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Q8_0 Quantization Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Q8_0 8-bit quantization."""

from neutron_mojo.quant.q8_0 import (
    q8_0_block_size,
    q8_0_bytes_per_block,
    quantize_q8_0,
    dequantize_q8_0,
    quantize_q8_0_block,
    dequantize_q8_0_block,
    calc_q8_0_buffer_size,
)
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_close(a: Float32, b: Float32, tol: Float32 = 1e-5) raises:
    if abs(a - b) > tol:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn test_q8_0_block_size() raises:
    """Test Q8_0 block size."""
    assert_true(q8_0_block_size() == 32, "Q8_0 block size should be 32")

    print("  q8_0_block_size: PASS")


fn test_q8_0_bytes_per_block() raises:
    """Test Q8_0 bytes per block."""
    # 2 bytes (FP16 scale) + 32 bytes (INT8 data) = 34 bytes
    assert_true(q8_0_bytes_per_block() == 34, "Q8_0 block should be 34 bytes")

    print("  q8_0_bytes_per_block: PASS")


fn test_quantize_q8_0_zero() raises:
    """Test quantizing zero."""
    var scale = Float32(1.0)
    var q = quantize_q8_0(Float32(0.0), scale)

    # Zero should quantize to 0
    assert_true(q == Int8(0), "Zero should quantize to 0")

    print("  quantize_q8_0_zero: PASS")


fn test_quantize_q8_0_positive() raises:
    """Test quantizing positive values."""
    var scale = Float32(1.0 / 127.0)  # Scale for full range

    var q_half = quantize_q8_0(Float32(0.5), scale)
    var q_one = quantize_q8_0(Float32(1.0), scale)

    # 0.5 / (1/127) = 63.5 -> rounds to 64
    # 1.0 / (1/127) = 127
    assert_true(abs(Int(q_half) - 64) <= 1, "0.5 should quantize near 64")
    assert_true(q_one == Int8(127), "1.0 should quantize to 127")

    print("  quantize_q8_0_positive: PASS")


fn test_quantize_q8_0_negative() raises:
    """Test quantizing negative values."""
    var scale = Float32(1.0 / 127.0)

    var q_neg_half = quantize_q8_0(Float32(-0.5), scale)
    var q_neg_one = quantize_q8_0(Float32(-1.0), scale)

    # -0.5 / (1/127) = -63.5 -> rounds to -64
    # -1.0 / (1/127) = -127
    assert_true(abs(Int(q_neg_half) + 64) <= 1, "-0.5 should quantize near -64")
    assert_true(q_neg_one == Int8(-127), "-1.0 should quantize to -127")

    print("  quantize_q8_0_negative: PASS")


fn test_quantize_q8_0_clamping() raises:
    """Test that quantization clamps to INT8 range."""
    var scale = Float32(0.001)  # Very small scale

    var q_over = quantize_q8_0(Float32(1000.0), scale)
    var q_under = quantize_q8_0(Float32(-1000.0), scale)

    # Should clamp to [-127, 127]
    assert_true(q_over == Int8(127), "Large positive should clamp to 127")
    assert_true(q_under == Int8(-127), "Large negative should clamp to -127")

    print("  quantize_q8_0_clamping: PASS")


fn test_dequantize_q8_0() raises:
    """Test dequantizing Q8_0 values."""
    var scale = Float32(2.0)

    var val_zero = dequantize_q8_0(Int8(0), scale)
    var val_pos = dequantize_q8_0(Int8(10), scale)
    var val_neg = dequantize_q8_0(Int8(-10), scale)

    assert_close(val_zero, Float32(0.0), tol=1e-6)
    assert_close(val_pos, Float32(20.0), tol=1e-5)  # 10 * 2.0
    assert_close(val_neg, Float32(-20.0), tol=1e-5)  # -10 * 2.0

    print("  dequantize_q8_0: PASS")


fn test_quantize_dequantize_roundtrip() raises:
    """Test Q8_0 roundtrip accuracy."""
    var scale = Float32(1.0 / 127.0)
    var original = Float32(0.456)

    var quantized = quantize_q8_0(original, scale)
    var dequantized = dequantize_q8_0(quantized, scale)

    # Should be close (within quantization error ~1/127)
    assert_true(abs(dequantized - original) < 0.01, "Roundtrip error should be small")

    print("  quantize_dequantize_roundtrip: PASS")


fn test_q8_0_block_quantize() raises:
    """Test Q8_0 block quantization."""
    # Create input data using List
    var input_list = List[Float32]()
    for i in range(32):
        input_list.append(Float32(i) / 31.0 - 0.5)  # Range [-0.5, 0.5]

    # Allocate output buffer (32 INT8 values)
    var output_list = List[Int8]()
    for _ in range(32):
        output_list.append(Int8(0))

    # Quantize block
    var scale = quantize_q8_0_block(input_list.unsafe_ptr(), output_list.unsafe_ptr(), 32)

    # Scale should be close to 0.5 / 127
    assert_true(scale > 0.0, "Scale should be positive")
    assert_true(scale < 0.01, "Scale should be small for [-0.5, 0.5] range")

    print("  q8_0_block_quantize: PASS")


fn test_q8_0_block_dequantize() raises:
    """Test Q8_0 block dequantization roundtrip."""
    # Original data using List
    var input_list = List[Float32]()
    for i in range(32):
        # Create varied test data
        var val = Float32(i - 16) / 16.0  # Range [-1.0, 1.0]
        input_list.append(val)

    # Quantize
    var packed_list = List[Int8]()
    for _ in range(32):
        packed_list.append(Int8(0))
    var scale = quantize_q8_0_block(input_list.unsafe_ptr(), packed_list.unsafe_ptr(), 32)

    # Dequantize
    var reconstructed_list = List[Float32]()
    for _ in range(32):
        reconstructed_list.append(Float32(0.0))
    dequantize_q8_0_block(packed_list.unsafe_ptr(), scale, reconstructed_list.unsafe_ptr(), 32)

    # Check roundtrip error is reasonable
    var max_error = Float32(0.0)
    for i in range(32):
        var orig = input_list[i]
        var recon = reconstructed_list[i]
        var error = abs(orig - recon)
        if error > max_error:
            max_error = error

    # Q8_0 should have very small error (< 1%)
    assert_true(max_error < 0.02, "Roundtrip error should be < 2%")

    print("  q8_0_block_dequantize: PASS")


fn test_calc_q8_0_buffer_size() raises:
    """Test Q8_0 buffer size calculation."""
    # 64 elements = 2 blocks * 34 bytes = 68 bytes
    var size_64 = calc_q8_0_buffer_size(64)
    assert_true(size_64 == 68, "64 elements should need 68 bytes")

    # 100 elements = 4 blocks * 34 bytes = 136 bytes (rounds up)
    var size_100 = calc_q8_0_buffer_size(100)
    assert_true(size_100 == 136, "100 elements should need 136 bytes")

    print("  calc_q8_0_buffer_size: PASS")


fn main() raises:
    print("test_q8_0:")

    test_q8_0_block_size()
    test_q8_0_bytes_per_block()
    test_quantize_q8_0_zero()
    test_quantize_q8_0_positive()
    test_quantize_q8_0_negative()
    test_quantize_q8_0_clamping()
    test_dequantize_q8_0()
    test_quantize_dequantize_roundtrip()
    test_q8_0_block_quantize()
    test_q8_0_block_dequantize()
    test_calc_q8_0_buffer_size()

    print("ALL PASSED")
