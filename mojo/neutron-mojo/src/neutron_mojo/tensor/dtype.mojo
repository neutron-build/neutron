# ===----------------------------------------------------------------------=== #
# Neutron Mojo — DType utilities and quantization config
# ===----------------------------------------------------------------------=== #

"""DType utility functions and quantization configuration.

Uses Mojo's built-in DType directly. This module adds utility functions
for querying type properties and a QuantConfig struct for block-quantized
types (NF4, Q4_K, Q8_0).
"""

from sys import simd_width_of, bit_width_of


# ===----------------------------------------------------------------------=== #
# QuantConfig — metadata for block-quantized types
# ===----------------------------------------------------------------------=== #


@fieldwise_init
struct QuantConfig(Writable, Copyable, Movable, ImplicitlyCopyable):
    """Configuration for block-quantized data types.

    Block quantization stores a group of weights together with per-block
    scale factors. This struct describes how a quantized block is laid out.
    """

    var block_size: Int
    var scale_dtype: DType
    var bits_per_element: Int
    var has_zero_point: Bool

    fn write_to[W: Writer](self, mut writer: W):
        var zp = String("yes") if self.has_zero_point else String("no")
        writer.write(
            "QuantConfig(block=",
            self.block_size,
            ", scale=",
            self.scale_dtype,
            ", bits=",
            self.bits_per_element,
            ", zp=",
            zp,
            ")",
        )


# Predefined quantization configs
comptime NF4_CONFIG = QuantConfig(
    block_size=64,
    scale_dtype=DType.float16,
    bits_per_element=4,
    has_zero_point=True,
)

comptime Q4_K_CONFIG = QuantConfig(
    block_size=256,
    scale_dtype=DType.float16,
    bits_per_element=4,
    has_zero_point=True,
)

comptime Q8_0_CONFIG = QuantConfig(
    block_size=32,
    scale_dtype=DType.float16,
    bits_per_element=8,
    has_zero_point=False,
)


# ===----------------------------------------------------------------------=== #
# DType utility functions
# ===----------------------------------------------------------------------=== #


fn bitwidth_of(dtype: DType) -> Int:
    """Returns the number of bits per element for the given DType (runtime)."""
    if dtype == DType.float64:
        return 64
    if dtype == DType.float32 or dtype == DType.int32 or dtype == DType.uint32:
        return 32
    if dtype == DType.float16 or dtype == DType.bfloat16 or dtype == DType.int16 or dtype == DType.uint16:
        return 16
    if dtype == DType.int8 or dtype == DType.uint8 or dtype == DType.bool:
        return 8
    if dtype == DType.int64 or dtype == DType.uint64:
        return 64
    return 0


fn is_floating_point(dtype: DType) -> Bool:
    """Returns True if dtype is a floating-point type (including bfloat16)."""
    return dtype.is_floating_point()


fn is_integer(dtype: DType) -> Bool:
    """Returns True if dtype is a signed or unsigned integer type."""
    return dtype.is_integral()


fn is_signed(dtype: DType) -> Bool:
    """Returns True if dtype is a signed numeric type."""
    return dtype.is_signed()


fn can_cast(source: DType, target: DType) -> Bool:
    """Returns True if source can be safely cast to target without data loss.

    Safe casts:
    - Same type -> always safe
    - Widening (e.g. float16 -> float32, int8 -> int32)
    - int -> float of sufficient width
    """
    if source == target:
        return True

    var src_bw = bitwidth_of(source)
    var tgt_bw = bitwidth_of(target)

    # Float -> wider float
    if source.is_floating_point() and target.is_floating_point():
        return tgt_bw >= src_bw

    # Int -> wider int (same signedness)
    if source.is_integral() and target.is_integral():
        if source.is_signed() == target.is_signed():
            return tgt_bw >= src_bw
        # Unsigned -> signed needs one extra bit
        if source.is_unsigned() and target.is_signed():
            return tgt_bw > src_bw
        return False

    # Int -> float (need enough mantissa bits)
    if source.is_integral() and target.is_floating_point():
        if target == DType.float64:
            return src_bw <= 53
        if target == DType.float32:
            return src_bw <= 24
        if target == DType.float16:
            return src_bw <= 11
        return False

    return False


fn optimal_simd_width[dtype: DType]() -> Int:
    """Returns the optimal SIMD vector width for the given DType on the current hardware."""
    return simd_width_of[dtype]()


# ===----------------------------------------------------------------------=== #
# DLPack type code mapping
# ===----------------------------------------------------------------------=== #

# DLPack type codes (from dlpack.h)
comptime DLPACK_INT: Int = 0
comptime DLPACK_UINT: Int = 1
comptime DLPACK_FLOAT: Int = 2
comptime DLPACK_BFLOAT: Int = 4
comptime DLPACK_BOOL: Int = 6


fn dtype_to_dlpack_code(dtype: DType) -> Int:
    """Maps a Mojo DType to the corresponding DLPack type code.

    Returns -1 for unsupported types.
    """
    if dtype == DType.bool:
        return DLPACK_BOOL
    if dtype == DType.bfloat16:
        return DLPACK_BFLOAT
    if dtype.is_floating_point():
        return DLPACK_FLOAT
    if dtype.is_unsigned():
        return DLPACK_UINT
    if dtype.is_signed() and dtype.is_integral():
        return DLPACK_INT
    return -1


fn dlpack_code_to_dtype(code: Int, bits: Int) -> DType:
    """Maps a DLPack type code and bit-width back to a Mojo DType.

    Returns DType.invalid for unrecognized combinations.
    """
    if code == DLPACK_BOOL:
        return DType.bool
    if code == DLPACK_BFLOAT and bits == 16:
        return DType.bfloat16
    if code == DLPACK_FLOAT:
        if bits == 16:
            return DType.float16
        if bits == 32:
            return DType.float32
        if bits == 64:
            return DType.float64
    if code == DLPACK_INT:
        if bits == 8:
            return DType.int8
        if bits == 16:
            return DType.int16
        if bits == 32:
            return DType.int32
        if bits == 64:
            return DType.int64
    if code == DLPACK_UINT:
        if bits == 8:
            return DType.uint8
        if bits == 16:
            return DType.uint16
        if bits == 32:
            return DType.uint32
        if bits == 64:
            return DType.uint64
    return DType.invalid
