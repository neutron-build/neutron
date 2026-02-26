# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Q8_0 (8-bit) Quantization
# ===----------------------------------------------------------------------=== #

"""Q8_0 quantization format from GGML/GGUF.

Q8_0 uses 8-bit signed integers with a per-block scale factor.
Block size is 32 elements, making it memory-efficient while maintaining
good accuracy for LLM weights.

Reference: GGML quantization schemes
"""

from math import abs

# ===----------------------------------------------------------------------=== #
# Q8_0 Constants
# ===----------------------------------------------------------------------=== #

comptime Q8_0_BLOCK_SIZE = 32


fn q8_0_block_size() -> Int:
    """Return the Q8_0 block size (32)."""
    return 32


fn q8_0_bytes_per_block() -> Int:
    """Calculate bytes per Q8_0 block.

    Returns:
        Bytes needed: 2 (FP16 scale) + 32 (INT8 values) = 34.
    """
    return 34


# ===----------------------------------------------------------------------=== #
# Q8_0 Quantization
# ===----------------------------------------------------------------------=== #

fn quantize_q8_0(value: Float32, scale: Float32) -> Int8:
    """Quantize a single FP32 value to INT8.

    Args:
        value: The FP32 value to quantize.
        scale: The block scale factor.

    Returns:
        INT8 quantized value.
    """
    # Normalize by scale and clamp to INT8 range [-127, 127]
    var normalized = value / scale

    # Round to nearest integer
    var quantized = Int(normalized + 0.5 if normalized >= 0 else normalized - 0.5)

    # Clamp to INT8 range (we use -127 to 127, avoiding -128 for symmetry)
    if quantized < -127:
        quantized = -127
    elif quantized > 127:
        quantized = 127

    return Int8(quantized)


fn dequantize_q8_0(value: Int8, scale: Float32) -> Float32:
    """Dequantize an INT8 value to FP32.

    Args:
        value: INT8 quantized value.
        scale: The block scale factor.

    Returns:
        Dequantized FP32 value.
    """
    return Float32(Int(value)) * scale


fn quantize_q8_0_block[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[Float32, input_origin],
    output: UnsafePointer[Int8, output_origin],
    block_size: Int,
) -> Float32:
    """Quantize a block of FP32 values to Q8_0.

    Computes the block scale (absmax), then quantizes each value to INT8.

    Args:
        input: Pointer to FP32 values (block_size elements).
        output: Pointer to output buffer (block_size INT8 values).
        block_size: Number of elements in the block (typically 32).

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

    # Scale to use full INT8 range [-127, 127]
    var scale = absmax / 127.0

    # Quantize values
    for i in range(block_size):
        var q = quantize_q8_0(input.load(i), scale)
        output.store(i, q)

    return scale


fn dequantize_q8_0_block[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[Int8, input_origin],
    scale: Float32,
    output: UnsafePointer[Float32, output_origin],
    block_size: Int,
):
    """Dequantize a block of Q8_0 values to FP32.

    Args:
        input: Pointer to INT8 data (block_size elements).
        scale: The block scale factor.
        output: Pointer to output FP32 buffer (block_size elements).
        block_size: Number of elements in the block.
    """
    for i in range(block_size):
        var q = input.load(i)
        output.store(i, dequantize_q8_0(q, scale))


# ===----------------------------------------------------------------------=== #
# Multi-Block Q8_0 Operations
# ===----------------------------------------------------------------------=== #

fn calc_q8_0_buffer_size(num_elements: Int) -> Int:
    """Calculate total bytes needed for Q8_0 quantized storage.

    Args:
        num_elements: Number of FP32 elements to quantize.

    Returns:
        Total bytes needed (including all block scales and data).
    """
    var num_blocks = (num_elements + Q8_0_BLOCK_SIZE - 1) // Q8_0_BLOCK_SIZE
    return num_blocks * q8_0_bytes_per_block()
