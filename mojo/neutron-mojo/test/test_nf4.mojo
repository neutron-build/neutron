# ===----------------------------------------------------------------------=== #
# Neutron Mojo — NF4 Quantization Tests
# ===----------------------------------------------------------------------=== #

"""Tests for NF4 4-bit NormalFloat quantization."""

from neutron_mojo.quant.nf4 import (
    get_nf4_value,
    quantize_nf4,
    dequantize_nf4,
    quantize_nf4_block,
    dequantize_nf4_block,
    nf4_table_size,
    nf4_bytes_per_block,
)
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_close(a: Float32, b: Float32, tol: Float32 = 1e-5) raises:
    if abs(a - b) > tol:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn test_nf4_table_size() raises:
    """Test NF4 table size."""
    assert_true(nf4_table_size() == 16, "NF4 table should have 16 entries")

    print("  nf4_table_size: PASS")


fn test_nf4_table_values() raises:
    """Test NF4 lookup table values."""
    # Check symmetric values around 0
    assert_close(get_nf4_value(0), Float32(-1.0), tol=1e-6)
    assert_close(get_nf4_value(7), Float32(0.0), tol=1e-6)
    assert_close(get_nf4_value(15), Float32(1.0), tol=1e-6)

    # Check monotonicity: table should be sorted ascending
    for i in range(15):
        assert_true(
            get_nf4_value(i) < get_nf4_value(i + 1), "NF4 table should be sorted ascending"
        )

    print("  nf4_table_values: PASS")


fn test_quantize_nf4_zero() raises:
    """Test quantizing zero."""
    var scale = Float32(1.0)
    var q = quantize_nf4(Float32(0.0), scale)

    # Zero should map to index 7
    assert_true(q == UInt8(7), "Zero should quantize to index 7")

    print("  quantize_nf4_zero: PASS")


fn test_quantize_nf4_extremes() raises:
    """Test quantizing extreme values."""
    var scale = Float32(1.0)

    var q_neg = quantize_nf4(Float32(-1.0), scale)
    var q_pos = quantize_nf4(Float32(1.0), scale)

    # -1.0 should map to index 0, 1.0 to index 15
    assert_true(q_neg == UInt8(0), "-1.0 should quantize to index 0")
    assert_true(q_pos == UInt8(15), "1.0 should quantize to index 15")

    print("  quantize_nf4_extremes: PASS")


fn test_quantize_nf4_clamping() raises:
    """Test that quantization clamps values outside [-1, 1]."""
    var scale = Float32(1.0)

    var q_under = quantize_nf4(Float32(-2.0), scale)
    var q_over = quantize_nf4(Float32(2.0), scale)

    # Should clamp to -1.0 and 1.0
    assert_true(q_under == UInt8(0), "Values < -1 should clamp to index 0")
    assert_true(q_over == UInt8(15), "Values > 1 should clamp to index 15")

    print("  quantize_nf4_clamping: PASS")


fn test_dequantize_nf4() raises:
    """Test dequantizing NF4 values."""
    var scale = Float32(1.0)

    var val_zero = dequantize_nf4(UInt8(7), scale)
    var val_neg = dequantize_nf4(UInt8(0), scale)
    var val_pos = dequantize_nf4(UInt8(15), scale)

    assert_close(val_zero, Float32(0.0), tol=1e-6)
    assert_close(val_neg, Float32(-1.0), tol=1e-6)
    assert_close(val_pos, Float32(1.0), tol=1e-6)

    print("  dequantize_nf4: PASS")


fn test_dequantize_nf4_with_scale() raises:
    """Test dequantization with non-unit scale."""
    var scale = Float32(2.0)

    var val = dequantize_nf4(UInt8(15), scale)  # index 15 = 1.0

    # 1.0 * scale = 2.0
    assert_close(val, Float32(2.0), tol=1e-5)

    print("  dequantize_nf4_with_scale: PASS")


fn test_quantize_dequantize_roundtrip() raises:
    """Test quantize-dequantize roundtrip."""
    var scale = Float32(1.0)
    var original = Float32(0.5)

    var quantized = quantize_nf4(original, scale)
    var dequantized = dequantize_nf4(quantized, scale)

    # Should be close to original (within quantization error)
    assert_true(abs(dequantized - original) < 0.1, "Roundtrip error should be small")

    print("  quantize_dequantize_roundtrip: PASS")


fn test_nf4_block_quantize() raises:
    """Test NF4 block quantization."""
    # Create input data using List
    var input_list = List[Float32]()
    input_list.append(Float32(-1.0))
    input_list.append(Float32(-0.5))
    input_list.append(Float32(-0.25))
    input_list.append(Float32(0.0))
    input_list.append(Float32(0.25))
    input_list.append(Float32(0.5))
    input_list.append(Float32(0.75))
    input_list.append(Float32(1.0))

    # Allocate output buffer (8 elements = 4 bytes packed)
    var output_list = List[UInt8]()
    for _ in range(4):
        output_list.append(0)

    # Quantize block
    var scale = quantize_nf4_block(input_list.unsafe_ptr(), output_list.unsafe_ptr(), 8)

    # Scale should be absmax = 1.0
    assert_close(scale, Float32(1.0), tol=1e-5)

    # Check that data was packed (can't easily verify exact packing without unpacking)
    assert_true(scale > 0.0, "Scale should be positive")

    print("  nf4_block_quantize: PASS")


fn test_nf4_block_dequantize() raises:
    """Test NF4 block dequantization roundtrip."""
    # Original data using List
    var input_list = List[Float32]()
    input_list.append(Float32(-0.8))
    input_list.append(Float32(-0.4))
    input_list.append(Float32(-0.2))
    input_list.append(Float32(0.0))
    input_list.append(Float32(0.2))
    input_list.append(Float32(0.4))
    input_list.append(Float32(0.6))
    input_list.append(Float32(0.9))

    # Quantize
    var packed_list = List[UInt8]()
    for _ in range(4):
        packed_list.append(0)
    var scale = quantize_nf4_block(input_list.unsafe_ptr(), packed_list.unsafe_ptr(), 8)

    # Dequantize
    var reconstructed_list = List[Float32]()
    for _ in range(8):
        reconstructed_list.append(0.0)
    dequantize_nf4_block(packed_list.unsafe_ptr(), scale, reconstructed_list.unsafe_ptr(), 8)

    # Check roundtrip error is reasonable
    for i in range(8):
        var orig = input_list[i]
        var recon = reconstructed_list[i]
        var error = abs(orig - recon)
        assert_true(error < 0.15, "Roundtrip error should be < 0.15")

    print("  nf4_block_dequantize: PASS")


fn test_nf4_bytes_per_block() raises:
    """Test NF4 bytes calculation."""
    # Block size 64: 2 bytes (scale) + 32 bytes (packed data)
    var bytes = nf4_bytes_per_block(64)

    assert_true(bytes == 34, "64 elements should need 34 bytes (2 + 32)")

    print("  nf4_bytes_per_block: PASS")


fn main() raises:
    print("test_nf4:")

    test_nf4_table_size()
    test_nf4_table_values()
    test_quantize_nf4_zero()
    test_quantize_nf4_extremes()
    test_quantize_nf4_clamping()
    test_dequantize_nf4()
    test_dequantize_nf4_with_scale()
    test_quantize_dequantize_roundtrip()
    test_nf4_block_quantize()
    test_nf4_block_dequantize()
    test_nf4_bytes_per_block()

    print("ALL PASSED")
