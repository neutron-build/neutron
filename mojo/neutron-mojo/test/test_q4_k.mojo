# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Q4_K Quantization Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Q4_K 4-bit k-quant quantization."""

from neutron_mojo.quant.q4_k import (
    q4_k_block_size,
    q4_k_subblock_size,
    q4_k_bytes_per_block,
    quantize_q4_k,
    dequantize_q4_k,
    quantize_q4_k_block,
    dequantize_q4_k_block,
    calc_q4_k_buffer_size,
)
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_close(a: Float32, b: Float32, tol: Float32 = 1e-5) raises:
    if abs(a - b) > tol:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn test_q4_k_block_size() raises:
    """Test Q4_K block size."""
    assert_true(q4_k_block_size() == 256, "Q4_K block size should be 256")
    assert_true(q4_k_subblock_size() == 32, "Q4_K sub-block size should be 32")

    print("  q4_k_block_size: PASS")


fn test_q4_k_bytes_per_block() raises:
    """Test Q4_K bytes per block."""
    # Approximate: 156 bytes per super-block
    var bytes = q4_k_bytes_per_block()
    assert_true(bytes > 0, "Bytes per block should be positive")
    assert_true(bytes == 156, "Q4_K block should be ~156 bytes")

    print("  q4_k_bytes_per_block: PASS")


fn test_quantize_q4_k_zero() raises:
    """Test quantizing zero."""
    var scale = Float32(1.0)
    var min_val = Float32(0.0)
    var q = quantize_q4_k(Float32(0.0), scale, min_val)

    # Zero should quantize to 0
    assert_true(q == UInt8(0), "Zero should quantize to 0")

    print("  quantize_q4_k_zero: PASS")


fn test_quantize_q4_k_range() raises:
    """Test quantizing values in range."""
    var scale = Float32(1.0)
    var min_val = Float32(0.0)

    var q0 = quantize_q4_k(Float32(0.0), scale, min_val)
    var q7 = quantize_q4_k(Float32(7.5), scale, min_val)
    var q15 = quantize_q4_k(Float32(15.0), scale, min_val)

    # Check quantization indices
    assert_true(q0 == UInt8(0), "0.0 should quantize to 0")
    assert_true(abs(Int(q7) - 8) <= 1, "7.5 should quantize near 8")
    assert_true(q15 == UInt8(15), "15.0 should quantize to 15")

    print("  quantize_q4_k_range: PASS")


fn test_quantize_q4_k_clamping() raises:
    """Test that quantization clamps to [0, 15]."""
    var scale = Float32(1.0)
    var min_val = Float32(0.0)

    var q_over = quantize_q4_k(Float32(100.0), scale, min_val)
    var q_under = quantize_q4_k(Float32(-10.0), scale, min_val)

    # Should clamp to [0, 15]
    assert_true(q_over == UInt8(15), "Large value should clamp to 15")
    assert_true(q_under == UInt8(0), "Negative value should clamp to 0")

    print("  quantize_q4_k_clamping: PASS")


fn test_dequantize_q4_k() raises:
    """Test dequantizing Q4_K values."""
    var scale = Float32(2.0)
    var min_val = Float32(10.0)

    var val0 = dequantize_q4_k(UInt8(0), scale, min_val)
    var val5 = dequantize_q4_k(UInt8(5), scale, min_val)
    var val15 = dequantize_q4_k(UInt8(15), scale, min_val)

    # 0 * 2.0 + 10.0 = 10.0
    # 5 * 2.0 + 10.0 = 20.0
    # 15 * 2.0 + 10.0 = 40.0
    assert_close(val0, Float32(10.0), tol=1e-5)
    assert_close(val5, Float32(20.0), tol=1e-5)
    assert_close(val15, Float32(40.0), tol=1e-5)

    print("  dequantize_q4_k: PASS")


fn test_quantize_dequantize_roundtrip() raises:
    """Test Q4_K roundtrip accuracy."""
    var scale = Float32(2.0)
    var min_val = Float32(5.0)
    var original = Float32(15.0)  # Within range [5, 35]

    var quantized = quantize_q4_k(original, scale, min_val)
    var dequantized = dequantize_q4_k(quantized, scale, min_val)

    # Should be close (within quantization error)
    assert_true(abs(dequantized - original) < 2.0, "Roundtrip error should be < 2.0")

    print("  quantize_dequantize_roundtrip: PASS")


fn test_q4_k_block_quantize() raises:
    """Test Q4_K block quantization."""
    # Create input data using List (64 elements for speed)
    var input_list = List[Float32]()
    for i in range(64):
        input_list.append(Float32(i) / 63.0 * 10.0)  # Range [0, 10]

    # Allocate output buffer (64 elements = 32 bytes packed)
    var output_list = List[UInt8]()
    for _ in range(32):
        output_list.append(UInt8(0))

    # Quantize block
    var params = quantize_q4_k_block(input_list.unsafe_ptr(), output_list.unsafe_ptr(), 64)
    var scale = params.scale
    var min_val = params.min_val

    # Check scale and min are reasonable
    assert_true(scale > 0.0, "Scale should be positive")
    assert_close(min_val, Float32(0.0), tol=0.1)  # Min should be near 0

    print("  q4_k_block_quantize: PASS")


fn test_q4_k_block_dequantize() raises:
    """Test Q4_K block dequantization roundtrip."""
    # Original data using List (64 elements)
    var input_list = List[Float32]()
    for i in range(64):
        var val = Float32(i - 32) / 8.0  # Range [-4, 4]
        input_list.append(val)

    # Quantize
    var packed_list = List[UInt8]()
    for _ in range(32):
        packed_list.append(UInt8(0))
    var params = quantize_q4_k_block(input_list.unsafe_ptr(), packed_list.unsafe_ptr(), 64)
    var scale = params.scale
    var min_val = params.min_val

    # Dequantize
    var reconstructed_list = List[Float32]()
    for _ in range(64):
        reconstructed_list.append(Float32(0.0))
    dequantize_q4_k_block(
        packed_list.unsafe_ptr(), scale, min_val, reconstructed_list.unsafe_ptr(), 64
    )

    # Check roundtrip error is reasonable
    var max_error = Float32(0.0)
    for i in range(64):
        var orig = input_list[i]
        var recon = reconstructed_list[i]
        var error = abs(orig - recon)
        if error > max_error:
            max_error = error

    # 4-bit quantization has larger error than 8-bit
    assert_true(max_error < 0.6, "Roundtrip error should be < 0.6")

    print("  q4_k_block_dequantize: PASS")


fn test_calc_q4_k_buffer_size() raises:
    """Test Q4_K buffer size calculation."""
    # 256 elements = 1 block * 156 bytes = 156 bytes
    var size_256 = calc_q4_k_buffer_size(256)
    assert_true(size_256 == 156, "256 elements should need 156 bytes")

    # 512 elements = 2 blocks * 156 bytes = 312 bytes
    var size_512 = calc_q4_k_buffer_size(512)
    assert_true(size_512 == 312, "512 elements should need 312 bytes")

    print("  calc_q4_k_buffer_size: PASS")


fn main() raises:
    print("test_q4_k:")

    test_q4_k_block_size()
    test_q4_k_bytes_per_block()
    test_quantize_q4_k_zero()
    test_quantize_q4_k_range()
    test_quantize_q4_k_clamping()
    test_dequantize_q4_k()
    test_quantize_dequantize_roundtrip()
    test_q4_k_block_quantize()
    test_q4_k_block_dequantize()
    test_calc_q4_k_buffer_size()

    print("ALL PASSED")
