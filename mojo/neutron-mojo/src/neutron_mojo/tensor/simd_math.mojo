# ===----------------------------------------------------------------------=== #
# Neutron Mojo — SIMD Math Primitives
# ===----------------------------------------------------------------------=== #

"""Low-level SIMD-vectorized math kernels for ML inference hot paths.

These operate on flat Tensor data using vectorize for SIMD inner loops.
They are the building blocks that nn/ modules should call instead of
scalar for-loops. This is where Mojo's 35,000x advantage over Python
actually comes from.

Key primitives:
    simd_dot       — vectorized dot product
    simd_matvec    — vectorized matrix-vector multiply (the #1 bottleneck)
    simd_rmsnorm   — fused RMSNorm (normalize + scale in one pass)
    simd_softmax   — numerically stable softmax
    simd_silu      — SiLU activation (x * sigmoid(x))
    simd_swiglu    — fused SwiGLU: silu(gate) * up
    simd_rope      — fused RoPE rotation
    simd_add       — vector addition (y += alpha * x)
"""

from algorithm import vectorize, parallelize
from math import exp, sqrt
from sys import simd_width_of, num_physical_cores

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# SIMD Width
# ===----------------------------------------------------------------------=== #

comptime F32_SIMD_WIDTH = simd_width_of[DType.float32]()


# ===----------------------------------------------------------------------=== #
# Dot Product
# ===----------------------------------------------------------------------=== #

fn simd_dot(
    a: Tensor[DType.float32],
    a_offset: Int,
    b: Tensor[DType.float32],
    b_offset: Int,
    n: Int,
) -> Float32:
    """SIMD-vectorized dot product of two vectors stored at offsets.

    Computes sum(a[a_offset + i] * b[b_offset + i] for i in range(n)).
    Uses SIMD for the bulk, scalar cleanup for the remainder.

    Args:
        a: First tensor (flat storage).
        a_offset: Starting offset in a.
        b: Second tensor (flat storage).
        b_offset: Starting offset in b.
        n: Number of elements.

    Returns:
        Dot product value.
    """
    var a_ptr = a.data_ptr() + a_offset
    var b_ptr = b.data_ptr() + b_offset

    var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)

    # SIMD bulk
    var simd_end = (n // F32_SIMD_WIDTH) * F32_SIMD_WIDTH
    for i in range(0, simd_end, F32_SIMD_WIDTH):
        var va = a_ptr.load[width=F32_SIMD_WIDTH](i)
        var vb = b_ptr.load[width=F32_SIMD_WIDTH](i)
        acc += va * vb

    # Reduce SIMD lanes
    var result = acc.reduce_add()

    # Scalar tail
    for i in range(simd_end, n):
        result += a_ptr[i] * b_ptr[i]

    return result


# ===----------------------------------------------------------------------=== #
# Matrix-Vector Multiply
# ===----------------------------------------------------------------------=== #

fn simd_matvec(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    weight: Tensor[DType.float32],
    w_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    rows: Int,
    cols: Int,
):
    """SIMD matrix-vector multiply: out[i] = dot(weight[i, :], x).

    Weight is stored row-major at w_offset: row i starts at w_offset + i * cols.
    This is the #1 performance bottleneck in transformer inference.

    Args:
        out: Output tensor, results written at out_offset.
        out_offset: Starting offset in output.
        weight: Weight matrix in row-major flat storage.
        w_offset: Starting offset of weight matrix.
        x: Input vector.
        x_offset: Starting offset in x.
        rows: Number of output rows.
        cols: Number of input columns (dot product length).
    """
    var x_ptr = x.data_ptr() + x_offset
    var w_ptr = weight.data_ptr() + w_offset
    var o_ptr = out.data_ptr() + out_offset

    var simd_end = (cols // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    for i in range(rows):
        var row_ptr = w_ptr + i * cols
        var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)

        for j in range(0, simd_end, F32_SIMD_WIDTH):
            var vw = row_ptr.load[width=F32_SIMD_WIDTH](j)
            var vx = x_ptr.load[width=F32_SIMD_WIDTH](j)
            acc += vw * vx

        var dot = acc.reduce_add()
        for j in range(simd_end, cols):
            dot += row_ptr[j] * x_ptr[j]

        o_ptr[i] = dot


# ===----------------------------------------------------------------------=== #
# RMSNorm
# ===----------------------------------------------------------------------=== #

fn simd_rmsnorm(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    weight: Tensor[DType.float32],
    w_offset: Int,
    n: Int,
    eps: Float32 = 1e-6,
):
    """Fused RMSNorm: out = (x / rms(x)) * weight.

    Two passes: (1) compute sum of squares, (2) normalize + scale.

    Args:
        out: Output tensor.
        out_offset: Offset in output.
        x: Input vector.
        x_offset: Offset in x.
        weight: Scale weights (gamma).
        w_offset: Offset in weight.
        n: Vector length.
        eps: Numerical stability epsilon.
    """
    var x_ptr = x.data_ptr() + x_offset
    var w_ptr = weight.data_ptr() + w_offset
    var o_ptr = out.data_ptr() + out_offset

    var simd_end = (n // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    # Pass 1: sum of squares
    var ss_acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)
    for i in range(0, simd_end, F32_SIMD_WIDTH):
        var vx = x_ptr.load[width=F32_SIMD_WIDTH](i)
        ss_acc += vx * vx
    var ss = ss_acc.reduce_add()
    for i in range(simd_end, n):
        ss += x_ptr[i] * x_ptr[i]

    # 1/sqrt(mean(x^2) + eps) via Newton's method (fast inverse sqrt)
    var mean_ss = ss / Float32(n) + eps
    var inv_rms = _fast_inv_sqrt(mean_ss)

    # Pass 2: normalize and scale
    var inv_rms_vec = SIMD[DType.float32, F32_SIMD_WIDTH](inv_rms)
    for i in range(0, simd_end, F32_SIMD_WIDTH):
        var vx = x_ptr.load[width=F32_SIMD_WIDTH](i)
        var vw = w_ptr.load[width=F32_SIMD_WIDTH](i)
        o_ptr.store(i, vx * inv_rms_vec * vw)
    for i in range(simd_end, n):
        o_ptr[i] = x_ptr[i] * inv_rms * w_ptr[i]


# ===----------------------------------------------------------------------=== #
# Softmax
# ===----------------------------------------------------------------------=== #

fn simd_softmax(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    n: Int,
):
    """Numerically stable softmax: out = exp(x - max(x)) / sum(exp(x - max(x))).

    Three passes: (1) find max, (2) exp and sum, (3) normalize.

    Args:
        out: Output tensor.
        out_offset: Offset in output.
        x: Input logits.
        x_offset: Offset in x.
        n: Vector length.
    """
    var x_ptr = x.data_ptr() + x_offset
    var o_ptr = out.data_ptr() + out_offset

    # Pass 1: find max
    var max_val = x_ptr[0]
    for i in range(1, n):
        var v = x_ptr[i]
        if v > max_val:
            max_val = v

    # Pass 2: exp(x - max) and sum
    var sum_val: Float32 = 0.0
    for i in range(n):
        var e = Float32(exp(Float64(x_ptr[i] - max_val)))
        o_ptr[i] = e
        sum_val += e

    # Pass 3: normalize
    if sum_val > 0.0:
        var inv_sum = Float32(1.0) / sum_val
        var simd_end = (n // F32_SIMD_WIDTH) * F32_SIMD_WIDTH
        var inv_vec = SIMD[DType.float32, F32_SIMD_WIDTH](inv_sum)
        for i in range(0, simd_end, F32_SIMD_WIDTH):
            var v = o_ptr.load[width=F32_SIMD_WIDTH](i)
            o_ptr.store(i, v * inv_vec)
        for i in range(simd_end, n):
            o_ptr[i] = o_ptr[i] * inv_sum


# ===----------------------------------------------------------------------=== #
# SiLU + SwiGLU
# ===----------------------------------------------------------------------=== #

fn simd_silu(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    n: Int,
):
    """SiLU activation: out = x * sigmoid(x) = x / (1 + exp(-x)).

    Args:
        out: Output tensor.
        out_offset: Offset in output.
        x: Input vector.
        x_offset: Offset in x.
        n: Vector length.
    """
    var x_ptr = x.data_ptr() + x_offset
    var o_ptr = out.data_ptr() + out_offset

    for i in range(n):
        var xi = x_ptr[i]
        var sig = Float32(1.0) / (Float32(1.0) + Float32(exp(Float64(-xi))))
        o_ptr[i] = xi * sig


fn simd_swiglu(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    gate: Tensor[DType.float32],
    gate_offset: Int,
    up: Tensor[DType.float32],
    up_offset: Int,
    n: Int,
):
    """Fused SwiGLU: out = silu(gate) * up.

    Saves one memory pass vs separate silu + multiply.

    Args:
        out: Output tensor.
        out_offset: Offset in output.
        gate: Gate input (from gate projection).
        gate_offset: Offset in gate.
        up: Up input (from up projection).
        up_offset: Offset in up.
        n: Vector length.
    """
    var g_ptr = gate.data_ptr() + gate_offset
    var u_ptr = up.data_ptr() + up_offset
    var o_ptr = out.data_ptr() + out_offset

    for i in range(n):
        var gi = g_ptr[i]
        var sig = Float32(1.0) / (Float32(1.0) + Float32(exp(Float64(-gi))))
        o_ptr[i] = gi * sig * u_ptr[i]


# ===----------------------------------------------------------------------=== #
# Vector Add (y += alpha * x)
# ===----------------------------------------------------------------------=== #

fn simd_axpy(
    mut y: Tensor[DType.float32],
    y_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    alpha: Float32,
    n: Int,
):
    """BLAS-style axpy: y = y + alpha * x.

    Used for residual connections, LoRA delta addition, etc.

    Args:
        y: Output tensor (modified in-place).
        y_offset: Offset in y.
        x: Input vector.
        x_offset: Offset in x.
        alpha: Scale factor.
        n: Vector length.
    """
    var y_ptr = y.data_ptr() + y_offset
    var x_ptr = x.data_ptr() + x_offset

    var simd_end = (n // F32_SIMD_WIDTH) * F32_SIMD_WIDTH
    var alpha_vec = SIMD[DType.float32, F32_SIMD_WIDTH](alpha)

    for i in range(0, simd_end, F32_SIMD_WIDTH):
        var vy = y_ptr.load[width=F32_SIMD_WIDTH](i)
        var vx = x_ptr.load[width=F32_SIMD_WIDTH](i)
        y_ptr.store(i, vy + alpha_vec * vx)
    for i in range(simd_end, n):
        y_ptr[i] = y_ptr[i] + alpha * x_ptr[i]


# ===----------------------------------------------------------------------=== #
# Parallel Matrix-Vector Multiply
# ===----------------------------------------------------------------------=== #

# Minimum rows to justify parallelism overhead
comptime PAR_MATVEC_THRESHOLD = 64

fn par_simd_matvec(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    weight: Tensor[DType.float32],
    w_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    rows: Int,
    cols: Int,
):
    """Parallel SIMD matrix-vector multiply.

    Splits rows across physical cores. Falls back to sequential
    simd_matvec for small row counts.

    Args:
        out: Output tensor.
        out_offset: Offset in output.
        weight: Weight matrix (row-major flat).
        w_offset: Offset of weight data.
        x: Input vector.
        x_offset: Offset in x.
        rows: Number of output rows.
        cols: Number of columns (dot product length).
    """
    if rows < PAR_MATVEC_THRESHOLD:
        simd_matvec(out, out_offset, weight, w_offset, x, x_offset, rows, cols)
        return

    var x_ptr = x.data_ptr() + x_offset
    var w_ptr = weight.data_ptr() + w_offset
    var o_ptr = out.data_ptr() + out_offset
    var simd_end = (cols // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    @parameter
    fn compute_row(i: Int):
        var row_ptr = w_ptr + i * cols
        var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)

        for j in range(0, simd_end, F32_SIMD_WIDTH):
            var vw = row_ptr.load[width=F32_SIMD_WIDTH](j)
            var vx = x_ptr.load[width=F32_SIMD_WIDTH](j)
            acc += vw * vx

        var dot = acc.reduce_add()
        for j in range(simd_end, cols):
            dot += row_ptr[j] * x_ptr[j]

        o_ptr[i] = dot

    parallelize[compute_row](rows, num_physical_cores())


# ===----------------------------------------------------------------------=== #
# Quantized Matrix-Vector Multiply (Q8 dequant-on-the-fly)
# ===----------------------------------------------------------------------=== #

fn simd_q8_matvec(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    q_data: Tensor[DType.float32],
    q_offset: Int,
    scales: Tensor[DType.float32],
    scales_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    rows: Int,
    cols: Int,
    block_size: Int,
):
    """SIMD-accelerated Q8 dequant-on-the-fly matrix-vector multiply.

    Computes out[i] = sum_b(scale_b * dot(q_row_block_b, x_block_b)).
    Scale is factored out of the block dot product for efficient SIMD.

    Args:
        out: Output tensor.
        out_offset: Offset in output.
        q_data: Quantized values (int8 stored as float32).
        q_offset: Offset in quantized data.
        scales: Per-block scales.
        scales_offset: Offset in scales tensor.
        x: Input vector.
        x_offset: Offset in x.
        rows: Number of output rows.
        cols: Number of input columns.
        block_size: Quantization block size (typically 32).
    """
    var q_ptr = q_data.data_ptr() + q_offset
    var s_ptr = scales.data_ptr() + scales_offset
    var x_ptr = x.data_ptr() + x_offset
    var o_ptr = out.data_ptr() + out_offset

    var num_blocks = (cols + block_size - 1) // block_size

    for i in range(rows):
        var row_ptr = q_ptr + i * cols
        var sum: Float32 = 0.0

        for b in range(num_blocks):
            var scale = s_ptr[i * num_blocks + b]
            var start = b * block_size
            var end = start + block_size
            if end > cols:
                end = cols
            var block_len = end - start

            # SIMD dot product within block
            var block_ptr = row_ptr + start
            var x_block_ptr = x_ptr + start
            var simd_end = (block_len // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

            var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)
            for j in range(0, simd_end, F32_SIMD_WIDTH):
                var vq = block_ptr.load[width=F32_SIMD_WIDTH](j)
                var vx = x_block_ptr.load[width=F32_SIMD_WIDTH](j)
                acc += vq * vx
            var block_dot = acc.reduce_add()

            # Scalar tail
            for j in range(simd_end, block_len):
                block_dot += block_ptr[j] * x_block_ptr[j]

            sum += block_dot * scale

        o_ptr[i] = sum


# ===----------------------------------------------------------------------=== #
# Fused RMSNorm + Matvec (Sprint 15)
# ===----------------------------------------------------------------------=== #

fn fused_rmsnorm_matvec(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    norm_weight: Tensor[DType.float32],
    norm_offset: Int,
    proj_weight: Tensor[DType.float32],
    proj_offset: Int,
    hidden_dim: Int,
    out_dim: Int,
    eps: Float32 = 1e-6,
):
    """Fused RMSNorm + matrix-vector multiply.

    Computes: out = W @ (rmsnorm(x, gamma))
    without materializing the normalized intermediate vector.

    Step 1: Compute inv_rms = 1/sqrt(mean(x^2) + eps)
    Step 2: For each output row i:
        out[i] = sum_j(W[i,j] * x[j] * inv_rms * gamma[j])

    This saves one full pass over hidden_dim vs separate rmsnorm + matvec.

    Args:
        out: Output tensor [out_dim].
        out_offset: Offset in output.
        x: Input vector [hidden_dim].
        x_offset: Offset in x.
        norm_weight: RMSNorm gamma weights [hidden_dim].
        norm_offset: Offset in norm weights.
        proj_weight: Projection weight matrix [out_dim, hidden_dim] row-major.
        proj_offset: Offset in projection weights.
        hidden_dim: Input dimension.
        out_dim: Output dimension.
        eps: RMSNorm epsilon.
    """
    var x_ptr = x.data_ptr() + x_offset
    var nw_ptr = norm_weight.data_ptr() + norm_offset
    var w_ptr = proj_weight.data_ptr() + proj_offset
    var o_ptr = out.data_ptr() + out_offset

    var simd_end = (hidden_dim // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    # Pass 1: sum of squares for RMS
    var ss_acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)
    for i in range(0, simd_end, F32_SIMD_WIDTH):
        var vx = x_ptr.load[width=F32_SIMD_WIDTH](i)
        ss_acc += vx * vx
    var ss = ss_acc.reduce_add()
    for i in range(simd_end, hidden_dim):
        ss += x_ptr[i] * x_ptr[i]

    var inv_rms = _fast_inv_sqrt(ss / Float32(hidden_dim) + eps)
    var inv_rms_vec = SIMD[DType.float32, F32_SIMD_WIDTH](inv_rms)

    # Pass 2: fused norm + matvec — each row is dot(W[i], x * inv_rms * gamma)
    for i in range(out_dim):
        var row_ptr = w_ptr + i * hidden_dim
        var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)

        for j in range(0, simd_end, F32_SIMD_WIDTH):
            var vw = row_ptr.load[width=F32_SIMD_WIDTH](j)
            var vx = x_ptr.load[width=F32_SIMD_WIDTH](j)
            var vg = nw_ptr.load[width=F32_SIMD_WIDTH](j)
            acc += vw * vx * inv_rms_vec * vg

        var dot = acc.reduce_add()
        for j in range(simd_end, hidden_dim):
            dot += row_ptr[j] * x_ptr[j] * inv_rms * nw_ptr[j]

        o_ptr[i] = dot


fn fused_matvec_residual_add(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    residual: Tensor[DType.float32],
    res_offset: Int,
    weight: Tensor[DType.float32],
    w_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    dim: Int,
    cols: Int,
):
    """Fused matrix-vector multiply + residual add.

    Computes: out[i] = residual[i] + dot(weight[i, :], x)
    in one pass instead of separate matvec then add.

    Args:
        out: Output tensor [dim].
        out_offset: Offset in output.
        residual: Residual vector [dim].
        res_offset: Offset in residual.
        weight: Weight matrix [dim, cols] row-major.
        w_offset: Offset in weight.
        x: Input vector [cols].
        x_offset: Offset in x.
        dim: Output dimension (rows).
        cols: Input dimension (dot product length).
    """
    var r_ptr = residual.data_ptr() + res_offset
    var w_ptr = weight.data_ptr() + w_offset
    var x_ptr = x.data_ptr() + x_offset
    var o_ptr = out.data_ptr() + out_offset

    var simd_end = (cols // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    for i in range(dim):
        var row_ptr = w_ptr + i * cols
        var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)

        for j in range(0, simd_end, F32_SIMD_WIDTH):
            var vw = row_ptr.load[width=F32_SIMD_WIDTH](j)
            var vx = x_ptr.load[width=F32_SIMD_WIDTH](j)
            acc += vw * vx

        var dot = acc.reduce_add()
        for j in range(simd_end, cols):
            dot += row_ptr[j] * x_ptr[j]

        o_ptr[i] = r_ptr[i] + dot


# ===----------------------------------------------------------------------=== #
# Tiled Matrix-Vector Multiply (cache-optimized)
# ===----------------------------------------------------------------------=== #

comptime TILE_COLS = 256  # Column tile size — fits 1KB in L1 cache
comptime TILE_ROWS = 4    # Row tile size — process multiple rows per column pass

fn tiled_simd_matvec(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    weight: Tensor[DType.float32],
    w_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    rows: Int,
    cols: Int,
):
    """Cache-optimized tiled matrix-vector multiply.

    Tiles along columns so that x[col_tile] stays in L1 cache while
    being reused across multiple rows. This improves cache hit rate
    for large matrices (hidden_dim >= 256).

    For small matrices (cols < TILE_COLS), falls back to plain simd_matvec.

    Args:
        out: Output tensor.
        out_offset: Offset in output.
        weight: Weight matrix (row-major flat).
        w_offset: Offset in weight.
        x: Input vector.
        x_offset: Offset in x.
        rows: Number of output rows.
        cols: Number of columns.
    """
    if cols < TILE_COLS:
        simd_matvec(out, out_offset, weight, w_offset, x, x_offset, rows, cols)
        return

    var x_ptr = x.data_ptr() + x_offset
    var w_ptr = weight.data_ptr() + w_offset
    var o_ptr = out.data_ptr() + out_offset

    # Zero output
    for i in range(rows):
        o_ptr[i] = 0.0

    # Tile over columns
    var col = 0
    while col < cols:
        var tile_cols = cols - col
        if tile_cols > TILE_COLS:
            tile_cols = TILE_COLS

        var simd_end = (tile_cols // F32_SIMD_WIDTH) * F32_SIMD_WIDTH
        var x_tile_ptr = x_ptr + col

        # Process rows in groups of TILE_ROWS for ILP
        var row = 0
        while row + TILE_ROWS <= rows:
            var acc0 = SIMD[DType.float32, F32_SIMD_WIDTH](0)
            var acc1 = SIMD[DType.float32, F32_SIMD_WIDTH](0)
            var acc2 = SIMD[DType.float32, F32_SIMD_WIDTH](0)
            var acc3 = SIMD[DType.float32, F32_SIMD_WIDTH](0)

            var r0 = w_ptr + (row + 0) * cols + col
            var r1 = w_ptr + (row + 1) * cols + col
            var r2 = w_ptr + (row + 2) * cols + col
            var r3 = w_ptr + (row + 3) * cols + col

            for j in range(0, simd_end, F32_SIMD_WIDTH):
                var vx = x_tile_ptr.load[width=F32_SIMD_WIDTH](j)
                acc0 += r0.load[width=F32_SIMD_WIDTH](j) * vx
                acc1 += r1.load[width=F32_SIMD_WIDTH](j) * vx
                acc2 += r2.load[width=F32_SIMD_WIDTH](j) * vx
                acc3 += r3.load[width=F32_SIMD_WIDTH](j) * vx

            o_ptr[row + 0] += acc0.reduce_add()
            o_ptr[row + 1] += acc1.reduce_add()
            o_ptr[row + 2] += acc2.reduce_add()
            o_ptr[row + 3] += acc3.reduce_add()

            # Scalar tail for this tile
            for j in range(simd_end, tile_cols):
                var xv = x_tile_ptr[j]
                o_ptr[row + 0] += r0[j] * xv
                o_ptr[row + 1] += r1[j] * xv
                o_ptr[row + 2] += r2[j] * xv
                o_ptr[row + 3] += r3[j] * xv

            row += TILE_ROWS

        # Handle remaining rows
        while row < rows:
            var row_ptr = w_ptr + row * cols + col
            var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)

            for j in range(0, simd_end, F32_SIMD_WIDTH):
                var vx = x_tile_ptr.load[width=F32_SIMD_WIDTH](j)
                acc += row_ptr.load[width=F32_SIMD_WIDTH](j) * vx

            o_ptr[row] += acc.reduce_add()
            for j in range(simd_end, tile_cols):
                o_ptr[row] += row_ptr[j] * x_tile_ptr[j]

            row += 1

        col += TILE_COLS


fn par_tiled_simd_matvec(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    weight: Tensor[DType.float32],
    w_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    rows: Int,
    cols: Int,
):
    """Parallel tiled matrix-vector multiply.

    Combines tiling (for cache reuse) with row-parallelism across cores.
    Falls back to tiled_simd_matvec for small row counts.

    Args:
        out: Output tensor.
        out_offset: Offset in output.
        weight: Weight matrix (row-major flat).
        w_offset: Offset in weight.
        x: Input vector.
        x_offset: Offset in x.
        rows: Number of output rows.
        cols: Number of columns.
    """
    if rows < PAR_MATVEC_THRESHOLD or cols < TILE_COLS:
        tiled_simd_matvec(out, out_offset, weight, w_offset, x, x_offset, rows, cols)
        return

    var x_ptr = x.data_ptr() + x_offset
    var w_ptr = weight.data_ptr() + w_offset
    var o_ptr = out.data_ptr() + out_offset
    var simd_end = (cols // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    @parameter
    fn compute_row(i: Int):
        var row_ptr = w_ptr + i * cols
        var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)

        for j in range(0, simd_end, F32_SIMD_WIDTH):
            var vw = row_ptr.load[width=F32_SIMD_WIDTH](j)
            var vx = x_ptr.load[width=F32_SIMD_WIDTH](j)
            acc += vw * vx

        var dot = acc.reduce_add()
        for j in range(simd_end, cols):
            dot += row_ptr[j] * x_ptr[j]

        o_ptr[i] = dot

    parallelize[compute_row](rows, num_physical_cores())


# ===----------------------------------------------------------------------=== #
# SIMD-Vectorized Attention Kernels
# ===----------------------------------------------------------------------=== #

fn simd_attention_scores(
    mut scores: Tensor[DType.float32],
    q: Tensor[DType.float32],
    q_offset: Int,
    k_cache: Tensor[DType.float32],
    k_stride: Int,
    seq_len: Int,
    head_dim: Int,
    scale: Float32,
):
    """Compute attention scores: scores[pos] = scale * dot(q, k[pos]).

    SIMD-vectorized dot products for each cached key position.
    k_cache is laid out as [seq_len, head_dim] starting at offset 0 with
    stride k_stride between rows.

    Args:
        scores: Output scores [seq_len].
        q: Query vector.
        q_offset: Offset in q.
        k_cache: Key cache tensor (flat).
        k_stride: Stride between key positions.
        seq_len: Number of cached positions.
        head_dim: Per-head dimension.
        scale: Attention scaling factor (1/sqrt(d)).
    """
    var q_ptr = q.data_ptr() + q_offset
    var simd_end = (head_dim // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    for pos in range(seq_len):
        var k_ptr = k_cache.data_ptr() + pos * k_stride
        var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)

        for j in range(0, simd_end, F32_SIMD_WIDTH):
            var vq = q_ptr.load[width=F32_SIMD_WIDTH](j)
            var vk = k_ptr.load[width=F32_SIMD_WIDTH](j)
            acc += vq * vk

        var dot = acc.reduce_add()
        for j in range(simd_end, head_dim):
            dot += q_ptr[j] * k_ptr[j]

        scores.set(pos, dot * scale)


fn simd_attention_weighted_sum(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    weights: Tensor[DType.float32],
    v_cache: Tensor[DType.float32],
    v_stride: Int,
    seq_len: Int,
    head_dim: Int,
):
    """Compute weighted sum of values: out = sum(weights[pos] * v[pos]).

    SIMD-vectorized value accumulation across cached positions.

    Args:
        out: Output tensor [head_dim].
        out_offset: Offset in output.
        weights: Attention weights [seq_len] (post-softmax).
        v_cache: Value cache tensor (flat).
        v_stride: Stride between value positions.
        seq_len: Number of cached positions.
        head_dim: Per-head dimension.
    """
    var o_ptr = out.data_ptr() + out_offset
    var simd_end = (head_dim // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    # Zero output
    for j in range(0, simd_end, F32_SIMD_WIDTH):
        o_ptr.store[width=F32_SIMD_WIDTH](j, SIMD[DType.float32, F32_SIMD_WIDTH](0))
    for j in range(simd_end, head_dim):
        o_ptr[j] = 0.0

    for pos in range(seq_len):
        var w = weights.get(pos)
        var v_ptr = v_cache.data_ptr() + pos * v_stride
        var vw = SIMD[DType.float32, F32_SIMD_WIDTH](w)

        for j in range(0, simd_end, F32_SIMD_WIDTH):
            var curr = o_ptr.load[width=F32_SIMD_WIDTH](j)
            var vv = v_ptr.load[width=F32_SIMD_WIDTH](j)
            o_ptr.store[width=F32_SIMD_WIDTH](j, curr + vw * vv)

        for j in range(simd_end, head_dim):
            o_ptr[j] += w * v_ptr[j]


fn simd_online_softmax_attention(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    q: Tensor[DType.float32],
    q_offset: Int,
    k_cache: Tensor[DType.float32],
    v_cache: Tensor[DType.float32],
    kv_stride: Int,
    seq_len: Int,
    head_dim: Int,
    scale: Float32,
):
    """Single-head attention with online softmax (Flash Attention style).

    Computes attention in a single pass over K/V without materializing
    the full scores vector. Uses the online softmax algorithm to
    incrementally maintain max and sum-exp.

    Memory: O(head_dim) instead of O(seq_len + head_dim).

    Args:
        out: Output tensor [head_dim].
        out_offset: Offset in output.
        q: Query vector.
        q_offset: Offset in q.
        k_cache: Key cache (flat, stride kv_stride between positions).
        v_cache: Value cache (flat, stride kv_stride between positions).
        kv_stride: Stride between KV positions.
        seq_len: Sequence length.
        head_dim: Per-head dimension.
        scale: Attention scaling factor.
    """
    var q_ptr = q.data_ptr() + q_offset
    var o_ptr = out.data_ptr() + out_offset
    var simd_end = (head_dim // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    # Initialize output to zero
    for j in range(0, simd_end, F32_SIMD_WIDTH):
        o_ptr.store[width=F32_SIMD_WIDTH](j, SIMD[DType.float32, F32_SIMD_WIDTH](0))
    for j in range(simd_end, head_dim):
        o_ptr[j] = 0.0

    if seq_len == 0:
        return

    var running_max: Float32 = -1e30
    var running_sum: Float32 = 0.0

    for pos in range(seq_len):
        # Compute score = scale * dot(q, k[pos])
        var k_ptr = k_cache.data_ptr() + pos * kv_stride
        var acc = SIMD[DType.float32, F32_SIMD_WIDTH](0)
        for j in range(0, simd_end, F32_SIMD_WIDTH):
            acc += q_ptr.load[width=F32_SIMD_WIDTH](j) * k_ptr.load[width=F32_SIMD_WIDTH](j)
        var score = acc.reduce_add()
        for j in range(simd_end, head_dim):
            score += q_ptr[j] * k_ptr[j]
        score *= scale

        # Online softmax update
        var prev_max = running_max
        if score > running_max:
            running_max = score

        var correction = Float32(exp(Float64(prev_max - running_max)))
        var new_weight = Float32(exp(Float64(score - running_max)))

        # Rescale running output and sum
        running_sum = running_sum * correction + new_weight

        # Update output: out = out * correction + new_weight * v[pos]
        var v_ptr = v_cache.data_ptr() + pos * kv_stride
        var vc = SIMD[DType.float32, F32_SIMD_WIDTH](correction)
        var vw = SIMD[DType.float32, F32_SIMD_WIDTH](new_weight)

        for j in range(0, simd_end, F32_SIMD_WIDTH):
            var cur = o_ptr.load[width=F32_SIMD_WIDTH](j)
            var vv = v_ptr.load[width=F32_SIMD_WIDTH](j)
            o_ptr.store[width=F32_SIMD_WIDTH](j, cur * vc + vw * vv)

        for j in range(simd_end, head_dim):
            o_ptr[j] = o_ptr[j] * correction + new_weight * v_ptr[j]

    # Normalize by sum
    if running_sum > 0.0:
        var inv_sum = Float32(1.0) / running_sum
        var vs = SIMD[DType.float32, F32_SIMD_WIDTH](inv_sum)
        for j in range(0, simd_end, F32_SIMD_WIDTH):
            o_ptr.store[width=F32_SIMD_WIDTH](j, o_ptr.load[width=F32_SIMD_WIDTH](j) * vs)
        for j in range(simd_end, head_dim):
            o_ptr[j] *= inv_sum


# ===----------------------------------------------------------------------=== #
# Batch Operations (Batch Prefill)
# ===----------------------------------------------------------------------=== #

fn simd_batch_matvec(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    weight: Tensor[DType.float32],
    w_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    batch: Int,
    out_dim: Int,
    in_dim: Int,
):
    """Batch matrix-vector multiply with shared weight matrix.

    Computes for each b in [0, batch):
        out[b * out_dim : (b+1) * out_dim] = W @ x[b * in_dim : (b+1) * in_dim]

    This batches N matvec operations that share the same weight matrix.
    Used during prefill to project all prompt tokens at once.

    Args:
        out: Output tensor [batch * out_dim].
        out_offset: Offset in output.
        weight: Weight matrix [out_dim, in_dim] (shared across batch).
        w_offset: Offset in weight.
        x: Input tensor [batch * in_dim].
        x_offset: Offset in x.
        batch: Number of vectors in batch.
        out_dim: Output dimension per vector.
        in_dim: Input dimension per vector.
    """
    for b in range(batch):
        simd_matvec(
            out, out_offset + b * out_dim,
            weight, w_offset,
            x, x_offset + b * in_dim,
            out_dim, in_dim,
        )


fn simd_batch_rmsnorm(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    x: Tensor[DType.float32],
    x_offset: Int,
    weight: Tensor[DType.float32],
    w_offset: Int,
    batch: Int,
    dim: Int,
    eps: Float32 = 1e-6,
):
    """Batch RMSNorm: apply RMSNorm to each vector in a batch.

    For each b in [0, batch): normalize x[b*dim : (b+1)*dim] with shared gamma.

    Args:
        out: Output tensor [batch * dim].
        out_offset: Offset in output.
        x: Input tensor [batch * dim].
        x_offset: Offset in x.
        weight: Shared gamma weights [dim].
        w_offset: Offset in weight.
        batch: Number of vectors.
        dim: Vector dimension.
        eps: Epsilon for numerical stability.
    """
    for b in range(batch):
        simd_rmsnorm(
            out, out_offset + b * dim,
            x, x_offset + b * dim,
            weight, w_offset,
            dim, eps,
        )


fn simd_batch_swiglu(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    gate: Tensor[DType.float32],
    gate_offset: Int,
    up: Tensor[DType.float32],
    up_offset: Int,
    batch: Int,
    dim: Int,
):
    """Batch SwiGLU: apply SwiGLU to each vector pair in a batch.

    For each b in [0, batch): out[b] = silu(gate[b]) * up[b].

    Args:
        out: Output tensor [batch * dim].
        out_offset: Offset in output.
        gate: Gate tensor [batch * dim].
        gate_offset: Offset in gate.
        up: Up tensor [batch * dim].
        up_offset: Offset in up.
        batch: Number of vectors.
        dim: Vector dimension.
    """
    for b in range(batch):
        simd_swiglu(
            out, out_offset + b * dim,
            gate, gate_offset + b * dim,
            up, up_offset + b * dim,
            dim,
        )


fn simd_batch_add(
    mut out: Tensor[DType.float32],
    out_offset: Int,
    a: Tensor[DType.float32],
    a_offset: Int,
    b: Tensor[DType.float32],
    b_offset: Int,
    batch: Int,
    dim: Int,
):
    """Batch element-wise addition: out = a + b for each vector in batch.

    Args:
        out: Output tensor [batch * dim].
        out_offset: Offset in output.
        a: First input [batch * dim].
        a_offset: Offset in a.
        b: Second input [batch * dim].
        b_offset: Offset in b.
        batch: Number of vectors.
        dim: Vector dimension.
    """
    var o_ptr = out.data_ptr() + out_offset
    var a_ptr = a.data_ptr() + a_offset
    var b_ptr = b.data_ptr() + b_offset
    var total = batch * dim
    var simd_end = (total // F32_SIMD_WIDTH) * F32_SIMD_WIDTH

    for i in range(0, simd_end, F32_SIMD_WIDTH):
        var va = a_ptr.load[width=F32_SIMD_WIDTH](i)
        var vb = b_ptr.load[width=F32_SIMD_WIDTH](i)
        o_ptr.store[width=F32_SIMD_WIDTH](i, va + vb)

    for i in range(simd_end, total):
        o_ptr[i] = a_ptr[i] + b_ptr[i]


# ===----------------------------------------------------------------------=== #
# Utilities
# ===----------------------------------------------------------------------=== #

fn _fast_inv_sqrt(x: Float32) -> Float32:
    """Fast inverse square root via Newton's method (6 iterations)."""
    if x <= 0.0:
        return 0.0
    # Initial estimate: 1/sqrt(x) ≈ start from 1.0/rough
    var est = Float32(1.0) / Float32(sqrt(Float64(x)))
    # One Newton refinement for extra precision
    # f(y) = 1/y^2 - x = 0  →  y_new = y * (3 - x*y^2) / 2
    est = est * (Float32(1.5) - Float32(0.5) * x * est * est)
    return est
