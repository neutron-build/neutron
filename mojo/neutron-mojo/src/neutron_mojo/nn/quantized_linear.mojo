# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantized Linear Projections
# ===----------------------------------------------------------------------=== #

"""Dequantize-on-the-fly linear projections for quantized weights.

Supports Q8_0 and Q4_0 quantized weight matrices.
Dequantizes each block during the dot product, avoiding full
materialization of the FP32 weight matrix.

Weight layout (Q8_0):
    - quant_data: Tensor[int8]  [out_features, in_features] quantized values
    - scales: Tensor[float32]   [out_features, num_blocks] per-row-block scales
    - block_size: typically 32

Weight layout (Q4_0):
    - quant_data: Tensor[uint8] [out_features, in_features/2] packed 4-bit values
    - scales: Tensor[float32]   [out_features, num_blocks]
    - block_size: typically 32
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import simd_q8_matvec


# ===----------------------------------------------------------------------=== #
# Q8_0 Quantized Weight Storage
# ===----------------------------------------------------------------------=== #

struct Q8Weight(Movable):
    """Q8_0 quantized weight matrix.

    Stores INT8 quantized values with per-block FP32 scales.
    """
    var data: Tensor[DType.float32]     # Stores int8 values as float32 for now
    var scales: Tensor[DType.float32]   # [out_features * num_blocks_per_row]
    var out_features: Int
    var in_features: Int
    var block_size: Int
    var num_blocks_per_row: Int

    fn __init__(out self, out_features: Int, in_features: Int, block_size: Int = 32):
        """Create storage for Q8_0 quantized weights.

        Args:
            out_features: Output dimension.
            in_features: Input dimension.
            block_size: Quantization block size.
        """
        self.out_features = out_features
        self.in_features = in_features
        self.block_size = block_size
        self.num_blocks_per_row = (in_features + block_size - 1) // block_size

        self.data = Tensor[DType.float32](Shape(out_features * in_features))
        self.scales = Tensor[DType.float32](Shape(out_features * self.num_blocks_per_row))

    fn __moveinit__(out self, deinit other: Self):
        self.data = other.data^
        self.scales = other.scales^
        self.out_features = other.out_features
        self.in_features = other.in_features
        self.block_size = other.block_size
        self.num_blocks_per_row = other.num_blocks_per_row


fn quantize_weight_q8(
    weight: Tensor[DType.float32],
    out_features: Int,
    in_features: Int,
    block_size: Int = 32,
) -> Q8Weight:
    """Quantize an FP32 weight matrix to Q8_0.

    Args:
        weight: FP32 weight [out_features, in_features].
        out_features: Output dimension.
        in_features: Input dimension.
        block_size: Block size for quantization.

    Returns:
        Q8Weight with quantized data and scales.
    """
    var qw = Q8Weight(out_features, in_features, block_size)
    var num_blocks = qw.num_blocks_per_row

    for row in range(out_features):
        for b in range(num_blocks):
            var start = b * block_size
            var end = start + block_size
            if end > in_features:
                end = in_features

            # Find absmax for this block
            var absmax: Float32 = 0.0
            for j in range(start, end):
                var val = weight.get(row, j)
                if val < 0.0:
                    val = -val
                if val > absmax:
                    absmax = val

            if absmax == 0.0:
                absmax = 1.0

            var scale = absmax / 127.0
            qw.scales.set(row * num_blocks + b, scale)

            # Quantize values
            for j in range(start, end):
                var val = weight.get(row, j)
                var q = val / scale
                # Clamp to [-127, 127] and round
                if q > 127.0:
                    q = 127.0
                elif q < -127.0:
                    q = -127.0
                # Round to nearest integer
                if q >= 0:
                    q = Float32(Int(q + 0.5))
                else:
                    q = Float32(Int(q - 0.5))
                qw.data.set(row * in_features + j, q)

    return qw^


fn q8_linear(
    x: Tensor[DType.float32],
    qw: Q8Weight,
) -> Tensor[DType.float32]:
    """Quantized linear projection: y = dequant(W_q) @ x.

    SIMD-accelerated dequant-on-the-fly. Scale is factored out of
    each block's dot product for efficient vectorization.

    Args:
        x: Input vector [in_features].
        qw: Q8_0 quantized weight matrix.

    Returns:
        Output vector [out_features].
    """
    var result = Tensor[DType.float32](Shape(qw.out_features))
    simd_q8_matvec(
        result, 0, qw.data, 0, qw.scales, 0,
        x, 0, qw.out_features, qw.in_features, qw.block_size,
    )
    return result^


# ===----------------------------------------------------------------------=== #
# Q4_0 Quantized Weight Storage
# ===----------------------------------------------------------------------=== #

struct Q4Weight(Movable):
    """Q4_0 quantized weight matrix.

    Stores 4-bit quantized values packed into uint8 (2 values per byte),
    with per-block FP32 scales.
    """
    var data: Tensor[DType.float32]     # Packed 4-bit as float32 (high nibble, low nibble)
    var scales: Tensor[DType.float32]   # [out_features * num_blocks_per_row]
    var out_features: Int
    var in_features: Int
    var block_size: Int
    var num_blocks_per_row: Int

    fn __init__(out self, out_features: Int, in_features: Int, block_size: Int = 32):
        self.out_features = out_features
        self.in_features = in_features
        self.block_size = block_size
        self.num_blocks_per_row = (in_features + block_size - 1) // block_size

        # Store individual 4-bit values as float32 for now (lean implementation)
        self.data = Tensor[DType.float32](Shape(out_features * in_features))
        self.scales = Tensor[DType.float32](Shape(out_features * self.num_blocks_per_row))

    fn __moveinit__(out self, deinit other: Self):
        self.data = other.data^
        self.scales = other.scales^
        self.out_features = other.out_features
        self.in_features = other.in_features
        self.block_size = other.block_size
        self.num_blocks_per_row = other.num_blocks_per_row


fn quantize_weight_q4(
    weight: Tensor[DType.float32],
    out_features: Int,
    in_features: Int,
    block_size: Int = 32,
) -> Q4Weight:
    """Quantize an FP32 weight matrix to Q4_0 (symmetric 4-bit).

    Maps values to [-8, 7] range.

    Args:
        weight: FP32 weight [out_features, in_features].
        out_features: Output dimension.
        in_features: Input dimension.
        block_size: Block size.

    Returns:
        Q4Weight with quantized data and scales.
    """
    var qw = Q4Weight(out_features, in_features, block_size)
    var num_blocks = qw.num_blocks_per_row

    for row in range(out_features):
        for b in range(num_blocks):
            var start = b * block_size
            var end = start + block_size
            if end > in_features:
                end = in_features

            var absmax: Float32 = 0.0
            for j in range(start, end):
                var val = weight.get(row, j)
                if val < 0.0:
                    val = -val
                if val > absmax:
                    absmax = val

            if absmax == 0.0:
                absmax = 1.0

            var scale = absmax / 7.0  # 4-bit signed: [-8, 7]
            qw.scales.set(row * num_blocks + b, scale)

            for j in range(start, end):
                var val = weight.get(row, j)
                var q = val / scale
                if q > 7.0:
                    q = 7.0
                elif q < -8.0:
                    q = -8.0
                if q >= 0:
                    q = Float32(Int(q + 0.5))
                else:
                    q = Float32(Int(q - 0.5))
                qw.data.set(row * in_features + j, q)

    return qw^


fn q4_linear(
    x: Tensor[DType.float32],
    qw: Q4Weight,
) -> Tensor[DType.float32]:
    """Quantized linear with Q4_0 weights.

    Args:
        x: Input vector [in_features].
        qw: Q4_0 quantized weight.

    Returns:
        Output vector [out_features].
    """
    var result = Tensor[DType.float32](Shape(qw.out_features))

    for row in range(qw.out_features):
        var sum: Float32 = 0.0
        var row_offset = row * qw.in_features

        for b in range(qw.num_blocks_per_row):
            var scale = qw.scales.get(row * qw.num_blocks_per_row + b)
            var start = b * qw.block_size
            var end = start + qw.block_size
            if end > qw.in_features:
                end = qw.in_features

            for j in range(start, end):
                var q_val = qw.data.get(row_offset + j)
                sum += (q_val * scale) * x.get(j)

        result.set(row, sum)

    return result^


# ===----------------------------------------------------------------------=== #
# Error Measurement
# ===----------------------------------------------------------------------=== #

fn quantization_error(
    original: Tensor[DType.float32],
    quantized_output: Tensor[DType.float32],
    size: Int,
) -> Float32:
    """Compute mean absolute error between original and quantized outputs.

    Args:
        original: Reference FP32 output.
        quantized_output: Output from quantized path.
        size: Number of elements.

    Returns:
        Mean absolute error.
    """
    var total_err: Float32 = 0.0
    for i in range(size):
        var diff = original.get(i) - quantized_output.get(i)
        if diff < 0.0:
            diff = -diff
        total_err += diff
    return total_err / Float32(size)
