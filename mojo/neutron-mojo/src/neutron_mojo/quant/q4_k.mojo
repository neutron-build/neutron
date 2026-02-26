# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Q4_K (4-bit K-Quant) Quantization
# ===----------------------------------------------------------------------=== #

"""Q4_K quantization format from GGML/GGUF.

Q4_K uses 4-bit quantization with hierarchical scaling:
- Super-blocks of 256 elements
- 8 sub-blocks of 32 elements each
- Per-super-block scale and min
- Per-sub-block 6-bit scales and mins

This format achieves better accuracy than simple Q4_0 by using
hierarchical quantization with multiple scale levels.

Reference: GGML k-quants
"""

from math import abs

# ===----------------------------------------------------------------------=== #
# Q4_K Constants
# ===----------------------------------------------------------------------=== #

comptime Q4_K_BLOCK_SIZE = 256
comptime Q4_K_NUM_SUBBLOCKS = 8
comptime Q4_K_SUBBLOCK_SIZE = 32


fn q4_k_block_size() -> Int:
    """Return the Q4_K super-block size (256)."""
    return 256


fn q4_k_subblock_size() -> Int:
    """Return the Q4_K sub-block size (32)."""
    return 32


fn q4_k_bytes_per_block() -> Int:
    """Calculate bytes per Q4_K super-block.

    Structure:
    - 2 bytes: FP16 super-scale
    - 2 bytes: FP16 super-min
    - 12 bytes: 8 sub-block 6-bit scales (packed)
    - 12 bytes: 8 sub-block 6-bit mins (packed)
    - 128 bytes: 256 4-bit values (packed 2 per byte)

    Total: 156 bytes (approximate, actual packing may vary)

    Returns:
        Bytes needed per Q4_K super-block.
    """
    return 156


# ===----------------------------------------------------------------------=== #
# Q4_K Quantization (Simplified)
# ===----------------------------------------------------------------------=== #

fn quantize_q4_k(value: Float32, scale: Float32, min_val: Float32) -> UInt8:
    """Quantize a single FP32 value to 4-bit.

    Args:
        value: The FP32 value to quantize.
        scale: The scale factor.
        min_val: The minimum value (offset).

    Returns:
        4-bit index (0-15) as UInt8.
    """
    # Normalize: (value - min) / scale
    var normalized = (value - min_val) / scale

    # Clamp to [0, 15] range
    var quantized = Int(normalized + 0.5)  # Round to nearest
    if quantized < 0:
        quantized = 0
    elif quantized > 15:
        quantized = 15

    return UInt8(quantized)


fn dequantize_q4_k(index: UInt8, scale: Float32, min_val: Float32) -> Float32:
    """Dequantize a 4-bit value to FP32.

    Args:
        index: 4-bit index (0-15).
        scale: The scale factor.
        min_val: The minimum value (offset).

    Returns:
        Dequantized FP32 value.
    """
    return Float32(Int(index)) * scale + min_val


# ===----------------------------------------------------------------------=== #
# Q4_K Block Quantization (Simplified Single-Scale Version)
# ===----------------------------------------------------------------------=== #

struct Q4KParams(Copyable):
    """Quantization parameters for Q4_K block."""
    var scale: Float32
    var min_val: Float32

    fn __init__(out self, scale: Float32, min_val: Float32):
        self.scale = scale
        self.min_val = min_val

    fn __copyinit__(out self, existing: Self):
        self.scale = existing.scale
        self.min_val = existing.min_val


fn quantize_q4_k_block[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[Float32, input_origin],
    output: UnsafePointer[UInt8, output_origin],
    block_size: Int,
) -> Q4KParams:
    """Quantize a block of FP32 values to Q4_K (simplified).

    This is a simplified version that uses a single scale and min
    for the entire block, rather than hierarchical sub-blocks.

    Args:
        input: Pointer to FP32 values (block_size elements).
        output: Pointer to output buffer (block_size/2 bytes).
        block_size: Number of elements in the block.

    Returns:
        Q4KParams containing scale and min_val for this block.
    """
    # Find min and max for asymmetric quantization
    var min_val = input.load(0)
    var max_val = input.load(0)

    for i in range(block_size):
        var val = input.load(i)
        if val < min_val:
            min_val = val
        if val > max_val:
            max_val = val

    # Compute scale for [0, 15] range
    var range_val = max_val - min_val
    if range_val == 0.0:
        range_val = 1.0

    var scale = range_val / 15.0

    # Quantize and pack values (2 per byte)
    for i in range(0, block_size, 2):
        var q0 = quantize_q4_k(input.load(i), scale, min_val)
        var q1 = quantize_q4_k(input.load(i + 1), scale, min_val)

        # Pack two 4-bit values into one byte: [q1 | q0]
        var packed = (Int(q1) << 4) | Int(q0)
        output.store(i // 2, UInt8(packed))

    return Q4KParams(scale, min_val)


fn dequantize_q4_k_block[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[UInt8, input_origin],
    scale: Float32,
    min_val: Float32,
    output: UnsafePointer[Float32, output_origin],
    block_size: Int,
):
    """Dequantize a block of Q4_K values to FP32 (simplified).

    Unpacks pairs of 4-bit values and dequantizes them.

    Args:
        input: Pointer to packed Q4_K data (block_size/2 bytes).
        scale: The block scale factor.
        min_val: The block minimum value.
        output: Pointer to output FP32 buffer (block_size elements).
        block_size: Number of elements in the block.
    """
    for i in range(0, block_size, 2):
        var packed = input.load(i // 2)

        # Unpack two 4-bit values: [q1 | q0]
        var q0 = UInt8(Int(packed) & 0xF)
        var q1 = UInt8((Int(packed) >> 4) & 0xF)

        output.store(i, dequantize_q4_k(q0, scale, min_val))
        output.store(i + 1, dequantize_q4_k(q1, scale, min_val))


# ===----------------------------------------------------------------------=== #
# Utilities
# ===----------------------------------------------------------------------=== #

fn calc_q4_k_buffer_size(num_elements: Int) -> Int:
    """Calculate total bytes needed for Q4_K quantized storage.

    Args:
        num_elements: Number of FP32 elements to quantize.

    Returns:
        Total bytes needed (simplified estimate).
    """
    var num_blocks = (num_elements + Q4_K_BLOCK_SIZE - 1) // Q4_K_BLOCK_SIZE
    return num_blocks * q4_k_bytes_per_block()
