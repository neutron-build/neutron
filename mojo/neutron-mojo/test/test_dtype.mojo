# ===----------------------------------------------------------------------=== #
# Tests for tensor/dtype.mojo — DType utilities and QuantConfig
# ===----------------------------------------------------------------------=== #

"""Tests: DType utils, QuantConfig, cast rules, DLPack code mapping."""

from testing import assert_true, assert_false, assert_equal

from neutron_mojo.tensor.dtype import (
    QuantConfig,
    NF4_CONFIG,
    Q4_K_CONFIG,
    Q8_0_CONFIG,
    bitwidth_of,
    is_floating_point,
    is_integer,
    is_signed,
    can_cast,
    optimal_simd_width,
    dtype_to_dlpack_code,
    dlpack_code_to_dtype,
    DLPACK_FLOAT,
    DLPACK_INT,
    DLPACK_UINT,
    DLPACK_BFLOAT,
    DLPACK_BOOL,
)


# --- QuantConfig ---


fn test_quant_config_nf4() raises:
    assert_equal(NF4_CONFIG.block_size, 64)
    assert_equal(NF4_CONFIG.bits_per_element, 4)
    assert_true(NF4_CONFIG.has_zero_point)
    print("  quant_config_nf4: PASS")


fn test_quant_config_q8_0() raises:
    assert_equal(Q8_0_CONFIG.block_size, 32)
    assert_equal(Q8_0_CONFIG.bits_per_element, 8)
    assert_false(Q8_0_CONFIG.has_zero_point)
    print("  quant_config_q8_0: PASS")


fn test_quant_config_q4_k() raises:
    assert_equal(Q4_K_CONFIG.block_size, 256)
    assert_equal(Q4_K_CONFIG.bits_per_element, 4)
    assert_true(Q4_K_CONFIG.has_zero_point)
    print("  quant_config_q4_k: PASS")


fn test_quant_config_writable() raises:
    var s = String(NF4_CONFIG)
    # Just check it doesn't crash and contains expected substrings
    assert_true("QuantConfig" in s)
    assert_true("block=64" in s)
    print("  quant_config_writable: PASS")


# --- Bitwidth ---


fn test_bitwidth() raises:
    assert_equal(bitwidth_of(DType.float32), 32)
    assert_equal(bitwidth_of(DType.float16), 16)
    assert_equal(bitwidth_of(DType.float64), 64)
    assert_equal(bitwidth_of(DType.int8), 8)
    assert_equal(bitwidth_of(DType.int32), 32)
    assert_equal(bitwidth_of(DType.uint16), 16)
    assert_equal(bitwidth_of(DType.bfloat16), 16)
    print("  bitwidth: PASS")


# --- Type predicates ---


fn test_is_floating_point() raises:
    assert_true(is_floating_point(DType.float32))
    assert_true(is_floating_point(DType.float16))
    assert_true(is_floating_point(DType.float64))
    assert_true(is_floating_point(DType.bfloat16))
    assert_false(is_floating_point(DType.int32))
    assert_false(is_floating_point(DType.uint8))
    print("  is_floating_point: PASS")


fn test_is_integer() raises:
    assert_true(is_integer(DType.int8))
    assert_true(is_integer(DType.int32))
    assert_true(is_integer(DType.uint64))
    assert_false(is_integer(DType.float32))
    assert_false(is_integer(DType.bfloat16))
    print("  is_integer: PASS")


fn test_is_signed() raises:
    assert_true(is_signed(DType.int8))
    assert_true(is_signed(DType.int32))
    assert_true(is_signed(DType.float32))
    assert_false(is_signed(DType.uint8))
    assert_false(is_signed(DType.uint32))
    print("  is_signed: PASS")


# --- Cast rules ---


fn test_can_cast_same_type() raises:
    assert_true(can_cast(DType.float32, DType.float32))
    assert_true(can_cast(DType.int8, DType.int8))
    print("  can_cast_same_type: PASS")


fn test_can_cast_widening_float() raises:
    assert_true(can_cast(DType.float16, DType.float32))
    assert_true(can_cast(DType.float32, DType.float64))
    assert_false(can_cast(DType.float64, DType.float32))
    assert_false(can_cast(DType.float32, DType.float16))
    print("  can_cast_widening_float: PASS")


fn test_can_cast_widening_int() raises:
    assert_true(can_cast(DType.int8, DType.int32))
    assert_true(can_cast(DType.int16, DType.int64))
    assert_false(can_cast(DType.int32, DType.int8))
    print("  can_cast_widening_int: PASS")


fn test_can_cast_unsigned_to_signed() raises:
    # uint8 -> int16 (needs extra bit, 16 > 8 so OK)
    assert_true(can_cast(DType.uint8, DType.int16))
    # uint8 -> int8 (needs extra bit, 8 == 8, not strictly greater)
    assert_false(can_cast(DType.uint8, DType.int8))
    print("  can_cast_unsigned_to_signed: PASS")


fn test_can_cast_int_to_float() raises:
    # int8 (8 bits) -> float32 (24-bit mantissa) — safe
    assert_true(can_cast(DType.int8, DType.float32))
    # int32 (32 bits) -> float32 (24-bit mantissa) — not enough mantissa
    assert_false(can_cast(DType.int32, DType.float32))
    # int32 (32 bits) -> float64 (53-bit mantissa) — safe
    assert_true(can_cast(DType.int32, DType.float64))
    print("  can_cast_int_to_float: PASS")


# --- SIMD width ---


fn test_optimal_simd_width() raises:
    # Just verify it returns a positive power of 2
    var w = optimal_simd_width[DType.float32]()
    assert_true(w >= 1)
    # Check power of 2
    assert_equal(w & (w - 1), 0)
    print("  optimal_simd_width: PASS (width=" + String(w) + ")")


# --- DLPack code mapping ---


fn test_dtype_to_dlpack_code() raises:
    assert_equal(dtype_to_dlpack_code(DType.float32), DLPACK_FLOAT)
    assert_equal(dtype_to_dlpack_code(DType.float16), DLPACK_FLOAT)
    assert_equal(dtype_to_dlpack_code(DType.float64), DLPACK_FLOAT)
    assert_equal(dtype_to_dlpack_code(DType.bfloat16), DLPACK_BFLOAT)
    assert_equal(dtype_to_dlpack_code(DType.int8), DLPACK_INT)
    assert_equal(dtype_to_dlpack_code(DType.int32), DLPACK_INT)
    assert_equal(dtype_to_dlpack_code(DType.uint8), DLPACK_UINT)
    assert_equal(dtype_to_dlpack_code(DType.bool), DLPACK_BOOL)
    print("  dtype_to_dlpack_code: PASS")


fn test_dlpack_round_trip() raises:
    """Verify dtype -> dlpack code -> dtype round-trip for common types."""
    var types = List[DType]()
    types.append(DType.float16)
    types.append(DType.float32)
    types.append(DType.float64)
    types.append(DType.bfloat16)
    types.append(DType.int8)
    types.append(DType.int16)
    types.append(DType.int32)
    types.append(DType.int64)
    types.append(DType.uint8)
    types.append(DType.uint16)
    types.append(DType.uint32)
    types.append(DType.uint64)
    types.append(DType.bool)

    for i in range(len(types)):
        var dt = types[i]
        var code = dtype_to_dlpack_code(dt)
        var bits = bitwidth_of(dt)
        if dt == DType.bool:
            bits = 8  # DLPack bool uses 8 bits
        var roundtrip = dlpack_code_to_dtype(code, bits)
        assert_true(
            roundtrip == dt,
        )

    print("  dlpack_round_trip: PASS")


# --- Main ---


fn main() raises:
    print("test_dtype:")
    test_quant_config_nf4()
    test_quant_config_q8_0()
    test_quant_config_q4_k()
    test_quant_config_writable()
    test_bitwidth()
    test_is_floating_point()
    test_is_integer()
    test_is_signed()
    test_can_cast_same_type()
    test_can_cast_widening_float()
    test_can_cast_widening_int()
    test_can_cast_unsigned_to_signed()
    test_can_cast_int_to_float()
    test_optimal_simd_width()
    test_dtype_to_dlpack_code()
    test_dlpack_round_trip()
    print("ALL PASSED")
