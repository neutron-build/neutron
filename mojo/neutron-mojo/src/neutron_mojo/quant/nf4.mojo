# ===----------------------------------------------------------------------=== #
# Neutron Mojo — NF4 (4-bit NormalFloat) Quantization
# ===----------------------------------------------------------------------=== #

"""NF4 quantization for QLoRA-style efficient fine-tuning.

NF4 uses 16 values optimally spaced to represent a normal distribution.
This enables 4-bit quantization with minimal loss for normally-distributed
weights (common in neural networks after training).

Reference: "QLoRA: Efficient Finetuning of Quantized LLMs" (Dettmers et al., 2023)
"""

from math import abs

# ===----------------------------------------------------------------------=== #
# NF4 Lookup Table
# ===----------------------------------------------------------------------=== #

# NF4 lookup table: 16 values optimally spaced for N(0,1) distribution
# Indices 0-15 map to these FP32 values
comptime NF4_TABLE = (
    -1.0,
    -0.6961928009986877,
    -0.5250730514526367,
    -0.39491748809814453,
    -0.28444138169288635,
    -0.18477343022823334,
    -0.09105003625154495,
    0.0,
    0.07958029955625534,
    0.16093020141124725,
    0.24611230194568634,
    0.33791524171829224,
    0.44070982933044434,
    0.5626170039176941,
    0.7229568362236023,
    1.0,
)


fn get_nf4_value(index: Int) -> Float32:
    """Get the NF4 table value for a given 4-bit index.

    Args:
        index: Index 0-15.

    Returns:
        The FP32 value from the NF4 lookup table.
    """
    if index == 0:
        return Float32(-1.0)
    elif index == 1:
        return Float32(-0.6961928009986877)
    elif index == 2:
        return Float32(-0.5250730514526367)
    elif index == 3:
        return Float32(-0.39491748809814453)
    elif index == 4:
        return Float32(-0.28444138169288635)
    elif index == 5:
        return Float32(-0.18477343022823334)
    elif index == 6:
        return Float32(-0.09105003625154495)
    elif index == 7:
        return Float32(0.0)
    elif index == 8:
        return Float32(0.07958029955625534)
    elif index == 9:
        return Float32(0.16093020141124725)
    elif index == 10:
        return Float32(0.24611230194568634)
    elif index == 11:
        return Float32(0.33791524171829224)
    elif index == 12:
        return Float32(0.44070982933044434)
    elif index == 13:
        return Float32(0.5626170039176941)
    elif index == 14:
        return Float32(0.7229568362236023)
    else:  # index == 15
        return Float32(1.0)


fn quantize_nf4(value: Float32, scale: Float32) -> UInt8:
    """Quantize a single FP32 value to 4-bit NF4.

    Finds the closest NF4 table entry to (value / scale).

    Args:
        value: The FP32 value to quantize.
        scale: The block scale factor.

    Returns:
        4-bit index (0-15) as UInt8.
    """
    # Normalize by scale
    var normalized = Float32(value) / scale

    # Clamp to [-1, 1] range
    if normalized < -1.0:
        normalized = -1.0
    elif normalized > 1.0:
        normalized = 1.0

    # Find closest table entry
    var best_idx = 0
    var best_dist = abs(normalized - get_nf4_value(0))

    for i in range(1, 16):
        var table_val = get_nf4_value(i)
        var dist = abs(normalized - table_val)
        if dist < best_dist:
            best_dist = dist
            best_idx = i

    return UInt8(best_idx)


fn dequantize_nf4(index: UInt8, scale: Float32) -> Float32:
    """Dequantize a 4-bit NF4 value to FP32.

    Args:
        index: 4-bit index (0-15).
        scale: The block scale factor.

    Returns:
        Dequantized FP32 value.
    """
    var table_val = get_nf4_value(Int(index))
    return table_val * scale


fn quantize_nf4_block[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[Float32, input_origin],
    output: UnsafePointer[UInt8, output_origin],
    block_size: Int,
) -> Float32:
    """Quantize a block of FP32 values to NF4.

    Computes the block scale (absmax), then quantizes each value.
    Each pair of 4-bit values is packed into one byte.

    Args:
        input: Pointer to FP32 values (block_size elements).
        output: Pointer to output buffer (block_size/2 bytes).
        block_size: Number of elements in the block (must be even).

    Returns:
        The computed scale factor for this block.
    """
    # Find absmax for scale
    var absmax = Float32(0.0)
    for i in range(block_size):
        var val = abs(input.load(i))
        if val > absmax:
            absmax = val

    # Avoid division by zero
    if absmax == 0.0:
        absmax = 1.0

    var scale = absmax

    # Quantize and pack values (2 per byte)
    for i in range(0, block_size, 2):
        var q0 = quantize_nf4(input.load(i), scale)
        var q1 = quantize_nf4(input.load(i + 1), scale)

        # Pack two 4-bit values into one byte: [q1 | q0]
        var packed = (Int(q1) << 4) | Int(q0)
        output.store(i // 2, UInt8(packed))

    return scale


fn dequantize_nf4_block[input_origin: Origin, output_origin: Origin where output_origin.mut](
    input: UnsafePointer[UInt8, input_origin],
    scale: Float32,
    output: UnsafePointer[Float32, output_origin],
    block_size: Int,
):
    """Dequantize a block of NF4 values to FP32.

    Unpacks pairs of 4-bit values and dequantizes them.

    Args:
        input: Pointer to packed NF4 data (block_size/2 bytes).
        scale: The block scale factor.
        output: Pointer to output FP32 buffer (block_size elements).
        block_size: Number of elements in the block.
    """
    for i in range(0, block_size, 2):
        var packed = input.load(i // 2)

        # Unpack two 4-bit values: [q1 | q0]
        var q0 = UInt8(Int(packed) & 0xF)
        var q1 = UInt8((Int(packed) >> 4) & 0xF)

        output.store(i, dequantize_nf4(q0, scale))
        output.store(i + 1, dequantize_nf4(q1, scale))


# ===----------------------------------------------------------------------=== #
# Utilities
# ===----------------------------------------------------------------------=== #

fn nf4_table_size() -> Int:
    """Return the NF4 lookup table size (16)."""
    return 16


fn nf4_bytes_per_block(block_size: Int) -> Int:
    """Calculate bytes needed for NF4 block storage.

    Args:
        block_size: Number of elements in the block.

    Returns:
        Bytes needed: 2 (scale) + block_size/2 (packed data).
    """
    return 2 + (block_size // 2)
