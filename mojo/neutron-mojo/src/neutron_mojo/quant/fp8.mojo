# ===----------------------------------------------------------------------=== #
# Neutron Mojo — FP8 (8-bit Floating Point) Types
# ===----------------------------------------------------------------------=== #

"""FP8 quantization formats for AI accelerators.

Two FP8 formats are supported:
- E4M3: 1 sign, 4 exponent, 3 mantissa bits (wider range, less precision)
- E5M2: 1 sign, 5 exponent, 2 mantissa bits (narrower range, more precision)

Reference: "FP8 Formats for Deep Learning" (Micikevicius et al., 2022)
"""

from math import abs, sqrt, isnan, isinf

# ===----------------------------------------------------------------------=== #
# FP8 E4M3 Format
# ===----------------------------------------------------------------------=== #

comptime FP8_E4M3_MAX = 448.0
comptime FP8_E4M3_MIN_NORMAL = 0.015625  # 2^-6
comptime FP8_E4M3_EXPONENT_BIAS = 7


fn quantize_fp8_e4m3(value: Float32) -> UInt8:
    """Quantize FP32 to FP8 E4M3 format (simplified linear mapping).

    E4M3: 1 sign bit, 4 exponent bits, 3 mantissa bits
    Range: approximately [-448, 448]

    NOTE: This is a simplified implementation using linear quantization.
    A full IEEE-754 compliant FP8 implementation would be more complex.

    Args:
        value: The FP32 value to quantize.

    Returns:
        FP8 E4M3 value packed into UInt8.
    """
    # Clamp to E4M3 range [-448, 448]
    var clamped = value
    if clamped > FP8_E4M3_MAX:
        clamped = FP8_E4M3_MAX
    elif clamped < -FP8_E4M3_MAX:
        clamped = -FP8_E4M3_MAX

    # Map to [0, 255] range
    # [-448, 448] -> [0, 255]
    var normalized = (clamped + FP8_E4M3_MAX) / (2.0 * FP8_E4M3_MAX) * 255.0
    var quantized = Int(normalized + 0.5)  # Round to nearest

    if quantized < 0:
        quantized = 0
    elif quantized > 255:
        quantized = 255

    return UInt8(quantized)


fn dequantize_fp8_e4m3(value: UInt8) -> Float32:
    """Dequantize FP8 E4M3 to FP32 (simplified linear mapping).

    Args:
        value: FP8 E4M3 value packed in UInt8.

    Returns:
        Dequantized FP32 value.
    """
    # Map from [0, 255] back to [-448, 448]
    var normalized = Float32(Int(value)) / 255.0
    return normalized * (2.0 * FP8_E4M3_MAX) - FP8_E4M3_MAX


# ===----------------------------------------------------------------------=== #
# FP8 E5M2 Format
# ===----------------------------------------------------------------------=== #

comptime FP8_E5M2_MAX = 57344.0
comptime FP8_E5M2_MIN_NORMAL = 0.00006103515625  # 2^-14
comptime FP8_E5M2_EXPONENT_BIAS = 15


fn quantize_fp8_e5m2(value: Float32) -> UInt8:
    """Quantize FP32 to FP8 E5M2 format (simplified linear mapping).

    E5M2: 1 sign bit, 5 exponent bits, 2 mantissa bits
    Range: approximately [-57344, 57344]

    NOTE: This is a simplified implementation using linear quantization.
    A full IEEE-754 compliant FP8 implementation would be more complex.

    Args:
        value: The FP32 value to quantize.

    Returns:
        FP8 E5M2 value packed into UInt8.
    """
    # Clamp to E5M2 range [-57344, 57344]
    var clamped = value
    if clamped > FP8_E5M2_MAX:
        clamped = FP8_E5M2_MAX
    elif clamped < -FP8_E5M2_MAX:
        clamped = -FP8_E5M2_MAX

    # Map to [0, 255] range
    # [-57344, 57344] -> [0, 255]
    var normalized = (clamped + FP8_E5M2_MAX) / (2.0 * FP8_E5M2_MAX) * 255.0
    var quantized = Int(normalized + 0.5)  # Round to nearest

    if quantized < 0:
        quantized = 0
    elif quantized > 255:
        quantized = 255

    return UInt8(quantized)


fn dequantize_fp8_e5m2(value: UInt8) -> Float32:
    """Dequantize FP8 E5M2 to FP32 (simplified linear mapping).

    Args:
        value: FP8 E5M2 value packed in UInt8.

    Returns:
        Dequantized FP32 value.
    """
    # Map from [0, 255] back to [-57344, 57344]
    var normalized = Float32(Int(value)) / 255.0
    return normalized * (2.0 * FP8_E5M2_MAX) - FP8_E5M2_MAX


# ===----------------------------------------------------------------------=== #
# Batch Conversion
# ===----------------------------------------------------------------------=== #

fn convert_fp32_to_fp8_e4m3[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[Float32, input_origin],
    output: UnsafePointer[UInt8, output_origin],
    count: Int,
):
    """Convert an array of FP32 values to FP8 E4M3.

    Args:
        input: Pointer to FP32 values.
        output: Pointer to output FP8 E4M3 buffer.
        count: Number of elements to convert.
    """
    for i in range(count):
        output.store(i, quantize_fp8_e4m3(input.load(i)))


fn convert_fp8_e4m3_to_fp32[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[UInt8, input_origin],
    output: UnsafePointer[Float32, output_origin],
    count: Int,
):
    """Convert an array of FP8 E4M3 values to FP32.

    Args:
        input: Pointer to FP8 E4M3 values.
        output: Pointer to output FP32 buffer.
        count: Number of elements to convert.
    """
    for i in range(count):
        output.store(i, dequantize_fp8_e4m3(input.load(i)))


fn convert_fp32_to_fp8_e5m2[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[Float32, input_origin],
    output: UnsafePointer[UInt8, output_origin],
    count: Int,
):
    """Convert an array of FP32 values to FP8 E5M2.

    Args:
        input: Pointer to FP32 values.
        output: Pointer to output FP8 E5M2 buffer.
        count: Number of elements to convert.
    """
    for i in range(count):
        output.store(i, quantize_fp8_e5m2(input.load(i)))


fn convert_fp8_e5m2_to_fp32[
    input_origin: Origin, output_origin: Origin where output_origin.mut
](
    input: UnsafePointer[UInt8, input_origin],
    output: UnsafePointer[Float32, output_origin],
    count: Int,
):
    """Convert an array of FP8 E5M2 values to FP32.

    Args:
        input: Pointer to FP8 E5M2 values.
        output: Pointer to output FP32 buffer.
        count: Number of elements to convert.
    """
    for i in range(count):
        output.store(i, dequantize_fp8_e5m2(input.load(i)))
