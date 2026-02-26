# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Tensor operations (CPU, SIMD-vectorized)
# ===----------------------------------------------------------------------=== #

"""Tensor operations: elementwise arithmetic, matmul, activations, reductions.

All operations are CPU-only with SIMD vectorization via `algorithm.vectorize`.
Supports broadcasting for elementwise ops. Tiled matmul with configurable
tile size.
"""

from algorithm import vectorize
from math import exp, sqrt, tanh
from sys import simd_width_of

from .shape import Shape
from .storage import Storage
from .tensor import Tensor


# ===----------------------------------------------------------------------=== #
# Internal helpers
# ===----------------------------------------------------------------------=== #


fn _broadcast_shapes(a: Shape, b: Shape) raises -> Shape:
    """Compute broadcast-compatible output shape."""
    return a.broadcast_with(b)


fn _broadcast_linear_index(
    flat_idx: Int,
    out_shape: Shape,
    src_shape: Shape,
    out_strides: List[Int],
) -> Int:
    """Map a flat output index to the corresponding flat source index.

    For broadcast dimensions (src size == 1), the index wraps to 0.
    """
    var ndim = out_shape.ndim()
    var src_ndim = src_shape.ndim()
    var remaining = flat_idx
    var src_offset = 0

    # Compute source strides
    var src_strides = src_shape.strides()

    for i in range(ndim):
        var coord = remaining // out_strides[i]
        remaining = remaining % out_strides[i]

        var src_dim_idx = src_ndim - ndim + i
        if src_dim_idx >= 0:
            var src_size = src_shape[src_dim_idx]
            if src_size == 1:
                pass  # broadcast: don't advance
            else:
                src_offset += coord * src_strides[src_dim_idx]

    return src_offset


# ===----------------------------------------------------------------------=== #
# Elementwise binary ops (SIMD fast path + broadcast slow path)
# ===----------------------------------------------------------------------=== #


fn _elementwise_binary_op[
    dtype: DType,
    op_fn: fn[w: Int] (SIMD[dtype, w], SIMD[dtype, w]) -> SIMD[dtype, w],
](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Generic elementwise binary operation with broadcast support.

    Fast path: same shape + contiguous -> SIMD vectorized
    Slow path: broadcast element-by-element
    """
    if a.shape() == b.shape() and a.is_contiguous() and b.is_contiguous():
        # Fast path: no broadcast, SIMD vectorize
        var result = Tensor[dtype](a.shape())
        var n = a.numel()
        var a_ptr = a.data_ptr()
        var b_ptr = b.data_ptr()
        var r_ptr = result.data_ptr()

        comptime simd_width = simd_width_of[dtype]()

        fn vec_op[w: Int](i: Int) unified {mut}:
            var va = a_ptr.load[width=w](i)
            var vb = b_ptr.load[width=w](i)
            r_ptr.store(i, op_fn[w](va, vb))

        vectorize[simd_width](n, vec_op)
        return result^

    # Slow path: broadcast
    var out_shape = _broadcast_shapes(a.shape(), b.shape())
    var result = Tensor[dtype](out_shape)
    var out_strides = out_shape.strides()
    var n = out_shape.numel()

    var a_ptr = a.data_ptr()
    var b_ptr = b.data_ptr()
    var r_ptr = result.data_ptr()

    for i in range(n):
        var a_idx = _broadcast_linear_index(i, out_shape, a.shape(), out_strides)
        var b_idx = _broadcast_linear_index(i, out_shape, b.shape(), out_strides)
        r_ptr.store(i, op_fn[1](a_ptr.load(a_idx), b_ptr.load(b_idx)))

    return result^


fn _simd_add[dtype: DType, w: Int](
    a: SIMD[dtype, w], b: SIMD[dtype, w]
) -> SIMD[dtype, w]:
    return a + b


fn _simd_sub[dtype: DType, w: Int](
    a: SIMD[dtype, w], b: SIMD[dtype, w]
) -> SIMD[dtype, w]:
    return a - b


fn _simd_mul[dtype: DType, w: Int](
    a: SIMD[dtype, w], b: SIMD[dtype, w]
) -> SIMD[dtype, w]:
    return a * b


fn _simd_div[dtype: DType, w: Int](
    a: SIMD[dtype, w], b: SIMD[dtype, w]
) -> SIMD[dtype, w]:
    return a / b


# ===----------------------------------------------------------------------=== #
# Public elementwise ops
# ===----------------------------------------------------------------------=== #


fn add[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise addition with broadcast support."""
    return _elementwise_binary_op[dtype, _simd_add[dtype]](a, b)


fn sub[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise subtraction with broadcast support."""
    return _elementwise_binary_op[dtype, _simd_sub[dtype]](a, b)


fn mul[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise multiplication with broadcast support."""
    return _elementwise_binary_op[dtype, _simd_mul[dtype]](a, b)


fn div[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise division with broadcast support."""
    return _elementwise_binary_op[dtype, _simd_div[dtype]](a, b)


# ===----------------------------------------------------------------------=== #
# Matrix multiplication — tiled 2D matmul
# ===----------------------------------------------------------------------=== #

comptime TILE_SIZE: Int = 32


fn _matmul_2d_kernel[
    dtype: DType
](
    a_ptr: UnsafePointer[Scalar[dtype], MutExternalOrigin],
    b_ptr: UnsafePointer[Scalar[dtype], MutExternalOrigin],
    c_ptr: UnsafePointer[Scalar[dtype], MutExternalOrigin],
    a_offset: Int,
    b_offset: Int,
    c_offset: Int,
    M: Int,
    K: Int,
    N: Int,
    transpose_a: Bool,
    transpose_b: Bool
):
    """2D matmul kernel that works on a slice of memory with offsets."""
    comptime simd_width = simd_width_of[dtype]()

    # Tiled matmul
    var ti = 0
    while ti < M:
        var tile_m = min(TILE_SIZE, M - ti)
        var tj = 0
        while tj < N:
            var tile_n = min(TILE_SIZE, N - tj)
            var tk = 0
            while tk < K:
                var tile_k = min(TILE_SIZE, K - tk)

                for i in range(tile_m):
                    for p in range(tile_k):
                        var a_idx: Int
                        if transpose_a:
                            a_idx = a_offset + (tk + p) * M + (ti + i)
                        else:
                            a_idx = a_offset + (ti + i) * K + (tk + p)

                        var a_val = a_ptr.load(a_idx)
                        var a_broadcast = SIMD[dtype, simd_width](a_val)

                        var j = 0
                        while j + simd_width <= tile_n:
                            if transpose_b:
                                break
                            else:
                                var b_vec = b_ptr.load[width=simd_width](
                                    b_offset + (tk + p) * N + (tj + j)
                                )
                                var c_vec = c_ptr.load[width=simd_width](
                                    c_offset + (ti + i) * N + (tj + j)
                                )
                                c_ptr.store(
                                    c_offset + (ti + i) * N + (tj + j),
                                    c_vec + a_broadcast * b_vec,
                                )
                                j += simd_width

                        while j < tile_n:
                            var b_idx: Int
                            if transpose_b:
                                b_idx = b_offset + (tj + j) * K + (tk + p)
                            else:
                                b_idx = b_offset + (tk + p) * N + (tj + j)

                            var b_val = b_ptr.load(b_idx)
                            var c_val = c_ptr.load(c_offset + (ti + i) * N + (tj + j))
                            c_ptr.store(
                                c_offset + (ti + i) * N + (tj + j),
                                c_val + a_val * b_val,
                            )
                            j += 1

                tk += TILE_SIZE
            tj += TILE_SIZE
        ti += TILE_SIZE


fn matmul[
    dtype: DType
](a: Tensor[dtype], b: Tensor[dtype], transpose_a: Bool = False, transpose_b: Bool = False) raises -> Tensor[dtype]:
    """Matrix multiplication: C = A @ B (with optional transposes).

    Supports 2D and 3D (batched) inputs with tiled algorithm and SIMD vectorization.

    Args:
        a: Left matrix/tensor
        b: Right matrix/tensor
        transpose_a: If True, compute A^T @ B instead of A @ B
        transpose_b: If True, compute A @ B^T instead of A @ B

    Shapes:
        2D: A: (M, K), B: (K, N) -> C: (M, N)
        3D: A: (B, M, K), B: (B, K, N) -> C: (B, M, N)
        (transposes apply to last 2 dims only)
    """
    if a.ndim() == 3 and b.ndim() == 3:
        # Batched matmul: loop over batch dimension
        var batch = a.shape()[0]
        if batch != b.shape()[0]:
            raise Error("matmul: batch dimensions must match")

        # Get result shape
        var M: Int
        var K: Int
        var K2: Int
        var N: Int

        if transpose_a:
            M = a.shape()[2]
            K = a.shape()[1]
        else:
            M = a.shape()[1]
            K = a.shape()[2]

        if transpose_b:
            K2 = b.shape()[2]
            N = b.shape()[1]
        else:
            K2 = b.shape()[1]
            N = b.shape()[2]

        if K != K2:
            raise Error("matmul: inner dimensions mismatch")

        # Allocate result
        var result = Tensor[dtype](batch, M, N)
        var a_ptr = a.data_ptr()
        var b_ptr = b.data_ptr()
        var c_ptr = result.data_ptr()

        # Process each batch
        var a_batch_stride = M * K
        var b_batch_stride = K * N
        var c_batch_stride = M * N

        for batch_idx in range(batch):
            var a_batch_offset = batch_idx * a_batch_stride
            var b_batch_offset = batch_idx * b_batch_stride
            var c_batch_offset = batch_idx * c_batch_stride

            # Call 2D matmul logic inline for this batch
            _matmul_2d_kernel[dtype](
                a_ptr, b_ptr, c_ptr,
                a_batch_offset, b_batch_offset, c_batch_offset,
                M, K, N,
                transpose_a, transpose_b
            )

        return result^

    elif a.ndim() != 2 or b.ndim() != 2:
        raise Error("matmul requires 2D or 3D tensors")

    # Determine effective dimensions based on transpose flags
    var M: Int
    var K: Int
    var K2: Int
    var N: Int

    if transpose_a:
        M = a.shape()[1]
        K = a.shape()[0]
    else:
        M = a.shape()[0]
        K = a.shape()[1]

    if transpose_b:
        K2 = b.shape()[1]
        N = b.shape()[0]
    else:
        K2 = b.shape()[0]
        N = b.shape()[1]

    if K != K2:
        raise Error(
            "matmul inner dimensions mismatch: "
            + String(K)
            + " vs "
            + String(K2)
        )

    var out_shape = Shape(M, N)
    var result = Tensor[dtype](out_shape)

    var a_ptr = a.data_ptr()
    var b_ptr = b.data_ptr()
    var c_ptr = result.data_ptr()

    # Call 2D kernel with zero offsets
    _matmul_2d_kernel[dtype](
        a_ptr, b_ptr, c_ptr,
        0, 0, 0,  # offsets all zero for non-batched case
        M, K, N,
        transpose_a, transpose_b
    )

    return result^


# ===----------------------------------------------------------------------=== #
# Activation functions
# ===----------------------------------------------------------------------=== #


fn relu[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """ReLU activation: max(0, x).

    Scalar loop — SIMD blocked by Mojo 0.26.2 limitation: SIMD comparison
    at width=1 returns Bool (no .select()), and @parameter if doesn't gate
    type-checking of dead branches. Revisit when Mojo fixes this.
    """
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()

    for i in range(n):
        var v = x_ptr.load(i)
        var z = Scalar[dtype](0)
        r_ptr.store(i, v if v > z else z)

    return result^


fn softmax[dtype: DType](x: Tensor[dtype], axis: Int = -1) raises -> Tensor[dtype]:
    """Numerically stable softmax along the given axis.

    Supports 1D tensors and 2D tensors (softmax along last axis).
    Uses the standard max-subtract trick for numerical stability.
    """
    var actual_axis = axis
    if actual_axis < 0:
        actual_axis += x.ndim()

    if x.ndim() == 1:
        return _softmax_1d[dtype](x)
    elif x.ndim() == 2 and actual_axis == 1:
        return _softmax_2d_last[dtype](x)
    else:
        raise Error("softmax currently supports 1D or 2D (axis=-1) tensors")


fn _softmax_1d[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Softmax for a 1D tensor."""
    var n = x.numel()
    var x_ptr = x.data_ptr()

    # Find max
    var max_val = Float64(x_ptr.load(0))
    for i in range(1, n):
        var v = Float64(x_ptr.load(i))
        if v > max_val:
            max_val = v

    # Compute exp(x - max) and sum
    var result = Tensor[dtype](x.shape())
    var r_ptr = result.data_ptr()
    var sum_val: Float64 = 0
    for i in range(n):
        var e = exp(Float64(x_ptr.load(i)) - max_val)
        r_ptr.store(i, Scalar[dtype](e))
        sum_val += e

    # Normalize
    var inv_sum = 1.0 / sum_val
    for i in range(n):
        r_ptr.store(i, Scalar[dtype](Float64(r_ptr.load(i)) * inv_sum))

    return result^


fn _softmax_2d_last[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Softmax along last axis of a 2D tensor."""
    var rows = x.shape()[0]
    var cols = x.shape()[1]
    var x_ptr = x.data_ptr()

    var result = Tensor[dtype](x.shape())
    var r_ptr = result.data_ptr()

    for row in range(rows):
        var base = row * cols

        # Find max for this row
        var max_val = Float64(x_ptr.load(base))
        for j in range(1, cols):
            var v = Float64(x_ptr.load(base + j))
            if v > max_val:
                max_val = v

        # Compute exp(x - max) and sum
        var sum_val: Float64 = 0
        for j in range(cols):
            var e = exp(Float64(x_ptr.load(base + j)) - max_val)
            r_ptr.store(base + j, Scalar[dtype](e))
            sum_val += e

        # Normalize
        var inv_sum = 1.0 / sum_val
        for j in range(cols):
            r_ptr.store(base + j, Scalar[dtype](Float64(r_ptr.load(base + j)) * inv_sum))

    return result^


# ===----------------------------------------------------------------------=== #
# Reductions
# ===----------------------------------------------------------------------=== #


fn reduce_sum[dtype: DType](x: Tensor[dtype], axis: Int = -1) raises -> Tensor[dtype]:
    """Sum reduction along the given axis.

    axis=-1 reduces the last dimension.
    For 1D, returns a 1-element tensor.
    For 2D with axis=1 (or -1), returns shape (rows, 1).
    """
    var actual_axis = axis
    if actual_axis < 0:
        actual_axis += x.ndim()

    if x.ndim() == 1:
        var total = Scalar[dtype](0)
        var x_ptr = x.data_ptr()
        for i in range(x.numel()):
            total += x_ptr.load(i)
        var result = Tensor[dtype](1)
        result._storage.store(0, total)
        return result^

    elif x.ndim() == 2 and actual_axis == 1:
        var rows = x.shape()[0]
        var cols = x.shape()[1]
        var result = Tensor[dtype](rows, 1)
        var x_ptr = x.data_ptr()
        var r_ptr = result.data_ptr()

        for row in range(rows):
            var total = Scalar[dtype](0)
            var base = row * cols
            for j in range(cols):
                total += x_ptr.load(base + j)
            r_ptr.store(row, total)
        return result^

    elif x.ndim() == 2 and actual_axis == 0:
        var rows = x.shape()[0]
        var cols = x.shape()[1]
        var result = Tensor[dtype](1, cols)
        var x_ptr = x.data_ptr()
        var r_ptr = result.data_ptr()

        for j in range(cols):
            var total = Scalar[dtype](0)
            for row in range(rows):
                total += x_ptr.load(row * cols + j)
            r_ptr.store(j, total)
        return result^

    else:
        raise Error(
            "reduce_sum: unsupported ndim=" + String(x.ndim()) + " axis=" + String(actual_axis)
        )


fn reduce_max[dtype: DType](x: Tensor[dtype], axis: Int = -1) raises -> Tensor[dtype]:
    """Max reduction along the given axis.

    axis=-1 reduces the last dimension.
    """
    var actual_axis = axis
    if actual_axis < 0:
        actual_axis += x.ndim()

    if x.ndim() == 1:
        var x_ptr = x.data_ptr()
        var max_val = x_ptr.load(0)
        for i in range(1, x.numel()):
            var v = x_ptr.load(i)
            if v > max_val:
                max_val = v
        var result = Tensor[dtype](1)
        result._storage.store(0, max_val)
        return result^

    elif x.ndim() == 2 and actual_axis == 1:
        var rows = x.shape()[0]
        var cols = x.shape()[1]
        var result = Tensor[dtype](rows, 1)
        var x_ptr = x.data_ptr()
        var r_ptr = result.data_ptr()

        for row in range(rows):
            var base = row * cols
            var max_val = x_ptr.load(base)
            for j in range(1, cols):
                var v = x_ptr.load(base + j)
                if v > max_val:
                    max_val = v
            r_ptr.store(row, max_val)
        return result^

    elif x.ndim() == 2 and actual_axis == 0:
        var rows = x.shape()[0]
        var cols = x.shape()[1]
        var result = Tensor[dtype](1, cols)
        var x_ptr = x.data_ptr()
        var r_ptr = result.data_ptr()

        for j in range(cols):
            var max_val = x_ptr.load(j)
            for row in range(1, rows):
                var v = x_ptr.load(row * cols + j)
                if v > max_val:
                    max_val = v
            r_ptr.store(j, max_val)
        return result^

    else:
        raise Error(
            "reduce_max: unsupported ndim=" + String(x.ndim()) + " axis=" + String(actual_axis)
        )


fn reduce_mean[dtype: DType](x: Tensor[dtype], axis: Int = -1) raises -> Tensor[dtype]:
    """Mean reduction along the specified axis.

    Same as reduce_sum but divides by the count along the axis.
    """
    var sum_result = reduce_sum(x, axis)
    var count = Float64(x.shape()[axis if axis >= 0 else x.ndim() + axis])

    # Divide each element by count
    var n = sum_result.numel()
    var ptr = sum_result.data_ptr()
    for i in range(n):
        ptr.store(i, Scalar[dtype](Float64(ptr.load(i)) / count))

    return sum_result^


fn sum_all[dtype: DType](x: Tensor[dtype]) -> Scalar[dtype]:
    """Sum all elements to a scalar."""
    var sum_val = Float64(0.0)
    var x_ptr = x.data_ptr()
    for i in range(x.numel()):
        sum_val += Float64(x_ptr.load(i))
    return Scalar[dtype](sum_val)


fn max_all[dtype: DType](x: Tensor[dtype]) raises -> Scalar[dtype]:
    """Max of all elements to a scalar."""
    if x.numel() == 0:
        raise Error("max_all: empty tensor")

    var x_ptr = x.data_ptr()
    var max_val = x_ptr.load(0)
    for i in range(1, x.numel()):
        var v = x_ptr.load(i)
        if v > max_val:
            max_val = v
    return max_val


# ===----------------------------------------------------------------------=== #
# Normalization
# ===----------------------------------------------------------------------=== #


fn rmsnorm[dtype: DType](
    x: Tensor[dtype], gamma: Tensor[dtype], eps: Float64 = 1e-6
) raises -> Tensor[dtype]:
    """RMS Normalization along the last axis.

    RMSNorm(x) = (x / sqrt(mean(x^2) + eps)) * gamma

    Used in Llama-3 and modern LLMs. Simpler than LayerNorm: no mean
    subtraction, no beta bias term.

    Args:
        x: Input tensor. Normalized along last axis.
        gamma: Scale parameters, shape matches last dim of x.
        eps: Epsilon for numerical stability.

    Returns:
        Normalized tensor with same shape as x.
    """
    if x.ndim() == 1:
        # 1D: single vector
        var n = x.shape()[0]
        if gamma.numel() != n:
            raise Error("rmsnorm: gamma size must match input size")

        # Compute mean of squares using Float64 for stability
        var sum_sq = Float64(0.0)
        var x_ptr = x.data_ptr()
        for i in range(n):
            var val = Float64(x_ptr.load(i))
            sum_sq += val * val

        var mean_sq = sum_sq / Float64(n)
        var rms = sqrt(mean_sq + eps)

        # Normalize and scale
        var result = Tensor[dtype](n)
        var r_ptr = result.data_ptr()
        var g_ptr = gamma.data_ptr()
        for i in range(n):
            var x_val = Float64(x_ptr.load(i))
            var g_val = Float64(g_ptr.load(i))
            r_ptr.store(i, Scalar[dtype]((x_val / rms) * g_val))

        return result^

    elif x.ndim() == 2:
        # 2D: (batch, hidden_dim), normalize along last axis
        var rows = x.shape()[0]
        var cols = x.shape()[1]

        if gamma.numel() != cols:
            raise Error("rmsnorm: gamma size must match last dim")

        var result = Tensor[dtype](rows, cols)
        var x_ptr = x.data_ptr()
        var r_ptr = result.data_ptr()
        var g_ptr = gamma.data_ptr()

        # Normalize each row independently
        for row in range(rows):
            var base = row * cols

            # Compute mean of squares for this row
            var sum_sq = Float64(0.0)
            for j in range(cols):
                var val = Float64(x_ptr.load(base + j))
                sum_sq += val * val

            var mean_sq = sum_sq / Float64(cols)
            var rms = sqrt(mean_sq + eps)

            # Normalize and scale
            for j in range(cols):
                var x_val = Float64(x_ptr.load(base + j))
                var g_val = Float64(g_ptr.load(j))
                r_ptr.store(base + j, Scalar[dtype]((x_val / rms) * g_val))

        return result^

    else:
        raise Error("rmsnorm: only 1D and 2D tensors supported, got ndim=" + String(x.ndim()))


fn layernorm[dtype: DType](
    x: Tensor[dtype], gamma: Tensor[dtype], beta: Tensor[dtype], eps: Float64 = 1e-5
) raises -> Tensor[dtype]:
    """Layer Normalization along the last axis.

    LayerNorm(x) = ((x - mean(x)) / sqrt(var(x) + eps)) * gamma + beta

    Used in GPT-2, BERT. Llama-3 uses RMSNorm instead, but LayerNorm is
    needed for compatibility with older models.

    Args:
        x: Input tensor. Normalized along last axis.
        gamma: Scale parameters, shape matches last dim of x.
        beta: Shift parameters, shape matches last dim of x.
        eps: Epsilon for numerical stability.

    Returns:
        Normalized tensor with same shape as x.
    """
    if x.ndim() == 1:
        # 1D: single vector
        var n = x.shape()[0]
        if gamma.numel() != n or beta.numel() != n:
            raise Error("layernorm: gamma and beta size must match input size")

        # Compute mean using Float64 for stability
        var sum_val = Float64(0.0)
        var x_ptr = x.data_ptr()
        for i in range(n):
            sum_val += Float64(x_ptr.load(i))
        var mean = sum_val / Float64(n)

        # Compute variance
        var sum_sq_diff = Float64(0.0)
        for i in range(n):
            var diff = Float64(x_ptr.load(i)) - mean
            sum_sq_diff += diff * diff
        var variance = sum_sq_diff / Float64(n)
        var std_inv = 1.0 / sqrt(variance + eps)

        # Normalize, scale, and shift
        var result = Tensor[dtype](n)
        var r_ptr = result.data_ptr()
        var g_ptr = gamma.data_ptr()
        var b_ptr = beta.data_ptr()
        for i in range(n):
            var x_val = Float64(x_ptr.load(i))
            var normed = (x_val - mean) * std_inv
            var scaled = normed * Float64(g_ptr.load(i)) + Float64(b_ptr.load(i))
            r_ptr.store(i, Scalar[dtype](scaled))

        return result^

    elif x.ndim() == 2:
        # 2D: (batch, hidden_dim), normalize along last axis
        var rows = x.shape()[0]
        var cols = x.shape()[1]

        if gamma.numel() != cols or beta.numel() != cols:
            raise Error("layernorm: gamma and beta size must match last dim")

        var result = Tensor[dtype](rows, cols)
        var x_ptr = x.data_ptr()
        var r_ptr = result.data_ptr()
        var g_ptr = gamma.data_ptr()
        var b_ptr = beta.data_ptr()

        # Normalize each row independently
        for row in range(rows):
            var base = row * cols

            # Compute mean for this row
            var sum_val = Float64(0.0)
            for j in range(cols):
                sum_val += Float64(x_ptr.load(base + j))
            var mean = sum_val / Float64(cols)

            # Compute variance
            var sum_sq_diff = Float64(0.0)
            for j in range(cols):
                var diff = Float64(x_ptr.load(base + j)) - mean
                sum_sq_diff += diff * diff
            var variance = sum_sq_diff / Float64(cols)
            var std_inv = 1.0 / sqrt(variance + eps)

            # Normalize, scale, and shift
            for j in range(cols):
                var x_val = Float64(x_ptr.load(base + j))
                var normed = (x_val - mean) * std_inv
                var scaled = normed * Float64(g_ptr.load(j)) + Float64(b_ptr.load(j))
                r_ptr.store(base + j, Scalar[dtype](scaled))

        return result^

    else:
        raise Error("layernorm: only 1D and 2D tensors supported, got ndim=" + String(x.ndim()))


# ===----------------------------------------------------------------------=== #
# Activation functions
# ===----------------------------------------------------------------------=== #


fn gelu[dtype: DType](x: Tensor[dtype]) raises -> Tensor[dtype]:
    """GeLU activation (tanh approximation).

    GeLU(x) = 0.5 * x * (1 + tanh(sqrt(2/pi) * (x + 0.044715 * x^3)))

    Used in GPT-2, BERT. This is the approximate version that matches
    PyTorch's default implementation.

    Reference: Hendrycks & Gimpel (2016)
    """
    var n = x.numel()
    var result: Tensor[dtype]

    # Create result with same shape as input
    if x.ndim() == 1:
        result = Tensor[dtype](x.shape()[0])
    elif x.ndim() == 2:
        result = Tensor[dtype](x.shape()[0], x.shape()[1])
    else:
        raise Error("gelu: only 1D and 2D tensors supported")

    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()

    # Constants
    var pi = 3.14159265358979323846
    var sqrt_2_over_pi = sqrt(2.0 / pi)
    var coef = 0.044715

    for i in range(n):
        var x_val = Float64(x_ptr.load(i))
        var x_cubed = x_val * x_val * x_val
        var inner = sqrt_2_over_pi * (x_val + coef * x_cubed)
        var tanh_val = tanh(inner)
        var out = 0.5 * x_val * (1.0 + tanh_val)
        r_ptr.store(i, Scalar[dtype](out))

    _ = result.numel()  # keepalive: result owns storage until after loop completes
    _ = x.numel()
    return result^


fn silu[dtype: DType](x: Tensor[dtype]) raises -> Tensor[dtype]:
    """SiLU (Swish) activation: x * sigmoid(x).

    SiLU(x) = x / (1 + exp(-x))

    Used in Llama-3, Mistral, and most modern LLMs.

    Reference: Elfwing et al. (2017)
    """
    var n = x.numel()
    var result: Tensor[dtype]

    # Create result with same shape as input
    if x.ndim() == 1:
        result = Tensor[dtype](x.shape()[0])
    elif x.ndim() == 2:
        result = Tensor[dtype](x.shape()[0], x.shape()[1])
    else:
        raise Error("silu: only 1D and 2D tensors supported")

    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()

    for i in range(n):
        var x_val = Float64(x_ptr.load(i))
        var sigmoid = 1.0 / (1.0 + exp(-x_val))
        var out = x_val * sigmoid
        r_ptr.store(i, Scalar[dtype](out))

    _ = result.numel()  # keepalive
    _ = x.numel()
    return result^


fn swiglu[dtype: DType](x: Tensor[dtype], gate: Tensor[dtype]) raises -> Tensor[dtype]:
    """SwiGLU gated activation: silu(gate) * x.

    Used in Llama-3 FFN: a linear layer produces [x, gate], then
    output = silu(gate) * x.

    Args:
        x: Input tensor
        gate: Gate tensor (same shape as x)

    Returns:
        Element-wise product of x and silu(gate)

    Reference: Shazeer (2020)
    """
    if x.ndim() != gate.ndim():
        raise Error("swiglu: x and gate must have same ndim")
    if x.shape() != gate.shape():
        raise Error("swiglu: x and gate must have same shape")

    var n = x.numel()
    var result: Tensor[dtype]

    # Create result with same shape as input
    if x.ndim() == 1:
        result = Tensor[dtype](x.shape()[0])
    elif x.ndim() == 2:
        result = Tensor[dtype](x.shape()[0], x.shape()[1])
    else:
        raise Error("swiglu: only 1D and 2D tensors supported")

    var x_ptr = x.data_ptr()
    var g_ptr = gate.data_ptr()
    var r_ptr = result.data_ptr()

    for i in range(n):
        var x_val = Float64(x_ptr.load(i))
        var g_val = Float64(g_ptr.load(i))
        var silu_gate = g_val / (1.0 + exp(-g_val))
        var out = x_val * silu_gate
        r_ptr.store(i, Scalar[dtype](out))

    _ = result.numel()  # keepalive
    _ = x.numel()  # keepalive input tensors
    _ = gate.numel()
    return result^


# ===----------------------------------------------------------------------=== #
# Extended math operations
# ===----------------------------------------------------------------------=== #


fn neg[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Negate all elements: -x."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, -x_ptr.load(i))
    return result^


fn abs_val[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Absolute value of all elements."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        var v = x_ptr.load(i)
        var z = Scalar[dtype](0)
        r_ptr.store(i, v if v >= z else -v)
    return result^


fn exp_op[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Elementwise exponential: exp(x)."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, Scalar[dtype](exp(Float64(x_ptr.load(i)))))
    return result^


fn log_op[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Elementwise natural log: log(x)."""
    from math import log
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, Scalar[dtype](log(Float64(x_ptr.load(i)))))
    return result^


fn sqrt_op[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Elementwise square root: sqrt(x)."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, Scalar[dtype](sqrt(Float64(x_ptr.load(i)))))
    return result^


fn sigmoid[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Sigmoid activation: 1 / (1 + exp(-x))."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        var v = Float64(x_ptr.load(i))
        r_ptr.store(i, Scalar[dtype](1.0 / (1.0 + exp(-v))))
    return result^


fn tanh_op[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Elementwise hyperbolic tangent: tanh(x)."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, Scalar[dtype](tanh(Float64(x_ptr.load(i)))))
    return result^


fn pow_scalar[dtype: DType](x: Tensor[dtype], exponent: Float64) -> Tensor[dtype]:
    """Raise all elements to a power: x^exponent."""
    from math import pow
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, Scalar[dtype](pow(Float64(x_ptr.load(i)), exponent)))
    return result^


fn clamp[dtype: DType](x: Tensor[dtype], min_val: Float64, max_val: Float64) -> Tensor[dtype]:
    """Clamp all elements to [min_val, max_val]."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    var lo = Scalar[dtype](min_val)
    var hi = Scalar[dtype](max_val)
    for i in range(n):
        var v = x_ptr.load(i)
        if v < lo:
            r_ptr.store(i, lo)
        elif v > hi:
            r_ptr.store(i, hi)
        else:
            r_ptr.store(i, v)
    return result^


fn scalar_mul[dtype: DType](x: Tensor[dtype], scalar: Float64) -> Tensor[dtype]:
    """Multiply all elements by a scalar."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    var s = Scalar[dtype](scalar)
    for i in range(n):
        r_ptr.store(i, x_ptr.load(i) * s)
    return result^


fn scalar_add[dtype: DType](x: Tensor[dtype], scalar: Float64) -> Tensor[dtype]:
    """Add a scalar to all elements."""
    var result = Tensor[dtype](x.shape())
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    var s = Scalar[dtype](scalar)
    for i in range(n):
        r_ptr.store(i, x_ptr.load(i) + s)
    return result^


# ===----------------------------------------------------------------------=== #
# Selection & indexing operations
# ===----------------------------------------------------------------------=== #


struct ArgResult(Copyable, Movable):
    """Result of argmax/argmin: index and value."""
    var index: Int
    var value: Float64

    fn __init__(out self, index: Int, value: Float64):
        self.index = index
        self.value = value

    fn __copyinit__(out self, other: Self):
        self.index = other.index
        self.value = other.value

    fn __moveinit__(out self, deinit other: Self):
        self.index = other.index
        self.value = other.value


fn argmax_tensor[dtype: DType](x: Tensor[dtype]) -> ArgResult:
    """Find the index and value of the maximum element (1D)."""
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var best_idx = 0
    var best_val = Float64(x_ptr.load(0))
    for i in range(1, n):
        var v = Float64(x_ptr.load(i))
        if v > best_val:
            best_val = v
            best_idx = i
    return ArgResult(best_idx, best_val)


fn argmin_tensor[dtype: DType](x: Tensor[dtype]) -> ArgResult:
    """Find the index and value of the minimum element (1D)."""
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var best_idx = 0
    var best_val = Float64(x_ptr.load(0))
    for i in range(1, n):
        var v = Float64(x_ptr.load(i))
        if v < best_val:
            best_val = v
            best_idx = i
    return ArgResult(best_idx, best_val)


fn argmax_axis[dtype: DType](x: Tensor[dtype], axis: Int) raises -> Tensor[dtype]:
    """Argmax along axis for 2D tensor. Returns indices as float values."""
    if x.ndim() != 2:
        raise Error("argmax_axis: only 2D tensors supported")
    var rows = x.shape()[0]
    var cols = x.shape()[1]
    var x_ptr = x.data_ptr()

    if axis == 0:
        # Reduce along rows -> 1D shape (cols,)
        var result = Tensor[dtype](cols)
        var r_ptr = result.data_ptr()
        for j in range(cols):
            var best_idx = 0
            var best_val = Float64(x_ptr.load(j))
            for row in range(1, rows):
                var v = Float64(x_ptr.load(row * cols + j))
                if v > best_val:
                    best_val = v
                    best_idx = row
            r_ptr.store(j, Scalar[dtype](best_idx))
        return result^
    elif axis == 1:
        # Reduce along cols -> 1D shape (rows,)
        var result = Tensor[dtype](rows)
        var r_ptr = result.data_ptr()
        for row in range(rows):
            var base = row * cols
            var best_idx = 0
            var best_val = Float64(x_ptr.load(base))
            for j in range(1, cols):
                var v = Float64(x_ptr.load(base + j))
                if v > best_val:
                    best_val = v
                    best_idx = j
            r_ptr.store(row, Scalar[dtype](best_idx))
        return result^
    else:
        raise Error("argmax_axis: axis must be 0 or 1")


fn topk[dtype: DType](x: Tensor[dtype], k: Int) raises -> Tensor[dtype]:
    """Top-k values from a 1D tensor. Returns a tensor of the k largest values sorted descending.

    Also stores indices in a secondary pattern — but since we can't return
    Tuple or custom struct with List + Tensor easily, just return the values tensor.
    Indices can be recovered by searching the original.
    """
    if x.ndim() != 1:
        raise Error("topk: only 1D tensors supported")
    var n = x.numel()
    if k > n:
        raise Error("topk: k > tensor size")

    var x_ptr = x.data_ptr()

    # Build sorted index array (insertion sort for simplicity)
    var indices = List[Int]()
    for i in range(n):
        indices.append(i)

    # Selection sort for top-k (only need k passes)
    for i in range(k):
        var best = i
        for j in range(i + 1, n):
            if Float64(x_ptr.load(indices[j])) > Float64(x_ptr.load(indices[best])):
                best = j
        if best != i:
            var tmp = indices[i]
            indices[i] = indices[best]
            indices[best] = tmp

    var result = Tensor[dtype](k)
    var r_ptr = result.data_ptr()
    for i in range(k):
        r_ptr.store(i, x_ptr.load(indices[i]))
    return result^


fn where_op[dtype: DType](
    condition: Tensor[dtype], x: Tensor[dtype], y: Tensor[dtype]
) raises -> Tensor[dtype]:
    """Conditional select: result[i] = x[i] if condition[i] > 0 else y[i]."""
    if condition.numel() != x.numel() or condition.numel() != y.numel():
        raise Error("where_op: all tensors must have same size")
    var n = condition.numel()
    var result = Tensor[dtype](x.shape())
    var c_ptr = condition.data_ptr()
    var x_ptr = x.data_ptr()
    var y_ptr = y.data_ptr()
    var r_ptr = result.data_ptr()
    var zero = Scalar[dtype](0)
    for i in range(n):
        if c_ptr.load(i) > zero:
            r_ptr.store(i, x_ptr.load(i))
        else:
            r_ptr.store(i, y_ptr.load(i))
    return result^


fn gather[dtype: DType](
    x: Tensor[dtype], dim: Int, indices: List[Int]
) raises -> Tensor[dtype]:
    """Gather elements along a dimension using indices.

    For 1D: result[i] = x[indices[i]]
    For 2D dim=0: result[i, j] = x[indices[i], j]
    For 2D dim=1: result[i, j] = x[i, indices[j]]
    """
    var x_ptr = x.data_ptr()

    if x.ndim() == 1:
        var k = len(indices)
        var result = Tensor[dtype](k)
        var r_ptr = result.data_ptr()
        for i in range(k):
            r_ptr.store(i, x_ptr.load(indices[i]))
        return result^
    elif x.ndim() == 2:
        var rows = x.shape()[0]
        var cols = x.shape()[1]
        var k = len(indices)
        if dim == 0:
            var result = Tensor[dtype](k, cols)
            var r_ptr = result.data_ptr()
            for i in range(k):
                var src_row = indices[i]
                for j in range(cols):
                    r_ptr.store(i * cols + j, x_ptr.load(src_row * cols + j))
            return result^
        elif dim == 1:
            var result = Tensor[dtype](rows, k)
            var r_ptr = result.data_ptr()
            for i in range(rows):
                for j in range(k):
                    r_ptr.store(i * k + j, x_ptr.load(i * cols + indices[j]))
            return result^
        else:
            raise Error("gather: dim must be 0 or 1 for 2D tensors")
    else:
        raise Error("gather: only 1D and 2D tensors supported")


fn index_select[dtype: DType](
    x: Tensor[dtype], dim: Int, indices: List[Int]
) raises -> Tensor[dtype]:
    """Select rows or columns by indices. Alias for gather with clearer semantics."""
    return gather(x, dim, indices)


fn eq[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise equal: returns 1.0 where equal, 0.0 otherwise."""
    if a.numel() != b.numel():
        raise Error("eq: tensors must have same size")
    var n = a.numel()
    var result = Tensor[dtype](a.shape())
    var a_ptr = a.data_ptr()
    var b_ptr = b.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        if a_ptr.load(i) == b_ptr.load(i):
            r_ptr.store(i, Scalar[dtype](1.0))
        else:
            r_ptr.store(i, Scalar[dtype](0.0))
    return result^


fn ne[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise not-equal: returns 1.0 where not equal, 0.0 otherwise."""
    if a.numel() != b.numel():
        raise Error("ne: tensors must have same size")
    var n = a.numel()
    var result = Tensor[dtype](a.shape())
    var a_ptr = a.data_ptr()
    var b_ptr = b.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        if a_ptr.load(i) != b_ptr.load(i):
            r_ptr.store(i, Scalar[dtype](1.0))
        else:
            r_ptr.store(i, Scalar[dtype](0.0))
    return result^


fn gt[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise greater-than: returns 1.0 where a > b, 0.0 otherwise."""
    if a.numel() != b.numel():
        raise Error("gt: tensors must have same size")
    var n = a.numel()
    var result = Tensor[dtype](a.shape())
    var a_ptr = a.data_ptr()
    var b_ptr = b.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        if a_ptr.load(i) > b_ptr.load(i):
            r_ptr.store(i, Scalar[dtype](1.0))
        else:
            r_ptr.store(i, Scalar[dtype](0.0))
    return result^


fn lt[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise less-than: returns 1.0 where a < b, 0.0 otherwise."""
    if a.numel() != b.numel():
        raise Error("lt: tensors must have same size")
    var n = a.numel()
    var result = Tensor[dtype](a.shape())
    var a_ptr = a.data_ptr()
    var b_ptr = b.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        if a_ptr.load(i) < b_ptr.load(i):
            r_ptr.store(i, Scalar[dtype](1.0))
        else:
            r_ptr.store(i, Scalar[dtype](0.0))
    return result^


fn ge[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise greater-or-equal: returns 1.0 where a >= b, 0.0 otherwise."""
    if a.numel() != b.numel():
        raise Error("ge: tensors must have same size")
    var n = a.numel()
    var result = Tensor[dtype](a.shape())
    var a_ptr = a.data_ptr()
    var b_ptr = b.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        if a_ptr.load(i) >= b_ptr.load(i):
            r_ptr.store(i, Scalar[dtype](1.0))
        else:
            r_ptr.store(i, Scalar[dtype](0.0))
    return result^


fn le[dtype: DType](a: Tensor[dtype], b: Tensor[dtype]) raises -> Tensor[dtype]:
    """Elementwise less-or-equal: returns 1.0 where a <= b, 0.0 otherwise."""
    if a.numel() != b.numel():
        raise Error("le: tensors must have same size")
    var n = a.numel()
    var result = Tensor[dtype](a.shape())
    var a_ptr = a.data_ptr()
    var b_ptr = b.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        if a_ptr.load(i) <= b_ptr.load(i):
            r_ptr.store(i, Scalar[dtype](1.0))
        else:
            r_ptr.store(i, Scalar[dtype](0.0))
    return result^
