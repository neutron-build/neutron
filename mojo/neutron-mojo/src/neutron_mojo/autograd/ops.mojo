# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Autograd Tracked Forward Operations
# ===----------------------------------------------------------------------=== #

"""Tracked forward functions that record operations on the tape.

Each function:
1. Reads input data from the tape
2. Computes the forward result
3. Stores the result as a new variable on the tape
4. Records a TapeEntry for backward pass
5. Returns the output variable index

NOTE: All functions use tape.get_data()/set_data() instead of data_ptr()
to avoid the Mojo 0.26.2 aliasing bug where data_ptr() on a mut struct
field returns a pointer to a temporary copy.
"""

from std.math import exp, sqrt, tanh, log

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from .tape import (
    Tape, TapeEntry,
    OP_ADD, OP_MUL, OP_MATMUL, OP_RELU, OP_SIGMOID, OP_TANH,
    OP_EXP, OP_LOG, OP_SOFTMAX, OP_SUM, OP_MEAN, OP_SUB,
    OP_SCALAR_MUL, OP_SCALAR_ADD, OP_NEG, OP_DIV,
)

# SIMD width for vectorized forward loops
comptime AUTOGRAD_SIMD_WIDTH = 4


def tracked_add(mut tape: Tape, a_idx: Int, b_idx: Int) raises -> Int:
    """Tracked elementwise addition: c = a + b. SIMD-accelerated."""
    tape.validate_same_shape(a_idx, b_idx)
    var n = tape.var_numel(a_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[a_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var c_idx = tape.add_variable(dims^, requires_grad=True)

    var a_off = tape.var_offset(a_idx)
    var b_off = tape.var_offset(b_idx)
    var c_off = tape.var_offset(c_idx)

    # SIMD bulk (width-4 loads/stores)
    comptime W = AUTOGRAD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        var v0 = tape.data_flat.get(a_off + i) + tape.data_flat.get(b_off + i)
        var v1 = tape.data_flat.get(a_off + i + 1) + tape.data_flat.get(b_off + i + 1)
        var v2 = tape.data_flat.get(a_off + i + 2) + tape.data_flat.get(b_off + i + 2)
        var v3 = tape.data_flat.get(a_off + i + 3) + tape.data_flat.get(b_off + i + 3)
        tape.data_flat.set(c_off + i, v0)
        tape.data_flat.set(c_off + i + 1, v1)
        tape.data_flat.set(c_off + i + 2, v2)
        tape.data_flat.set(c_off + i + 3, v3)
        i += W

    # Scalar remainder
    while i < n:
        var val = tape.data_flat.get(a_off + i) + tape.data_flat.get(b_off + i)
        tape.data_flat.set(c_off + i, val)
        i += 1

    tape.record(TapeEntry(OP_ADD(), a_idx, b_idx, c_idx))
    return c_idx


def tracked_sub(mut tape: Tape, a_idx: Int, b_idx: Int) raises -> Int:
    """Tracked elementwise subtraction: c = a - b."""
    tape.validate_same_shape(a_idx, b_idx)
    var n = tape.var_numel(a_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[a_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var c_idx = tape.add_variable(dims^, requires_grad=True)

    for i in range(n):
        var val = tape.get_data(a_idx, i) - tape.get_data(b_idx, i)
        tape.set_data(c_idx, i, val)

    tape.record(TapeEntry(OP_SUB(), a_idx, b_idx, c_idx))
    return c_idx


def tracked_mul(mut tape: Tape, a_idx: Int, b_idx: Int) raises -> Int:
    """Tracked elementwise multiplication: c = a * b. SIMD-accelerated."""
    tape.validate_same_shape(a_idx, b_idx)
    var n = tape.var_numel(a_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[a_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var c_idx = tape.add_variable(dims^, requires_grad=True)

    var a_off = tape.var_offset(a_idx)
    var b_off = tape.var_offset(b_idx)
    var c_off = tape.var_offset(c_idx)

    # SIMD bulk (width-4 loads/stores)
    comptime W = AUTOGRAD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        var v0 = tape.data_flat.get(a_off + i) * tape.data_flat.get(b_off + i)
        var v1 = tape.data_flat.get(a_off + i + 1) * tape.data_flat.get(b_off + i + 1)
        var v2 = tape.data_flat.get(a_off + i + 2) * tape.data_flat.get(b_off + i + 2)
        var v3 = tape.data_flat.get(a_off + i + 3) * tape.data_flat.get(b_off + i + 3)
        tape.data_flat.set(c_off + i, v0)
        tape.data_flat.set(c_off + i + 1, v1)
        tape.data_flat.set(c_off + i + 2, v2)
        tape.data_flat.set(c_off + i + 3, v3)
        i += W

    # Scalar remainder
    while i < n:
        var val = tape.data_flat.get(a_off + i) * tape.data_flat.get(b_off + i)
        tape.data_flat.set(c_off + i, val)
        i += 1

    tape.record(TapeEntry(OP_MUL(), a_idx, b_idx, c_idx))
    return c_idx


def tracked_matmul(mut tape: Tape, a_idx: Int, b_idx: Int, M: Int, K: Int, N: Int) raises -> Int:
    """Tracked matrix multiplication: C = A @ B.

    A is (M, K), B is (K, N), C is (M, N).
    """
    tape.validate_variable(a_idx)
    tape.validate_variable(b_idx)
    if M <= 0 or K <= 0 or N <= 0:
        raise Error("Tracked matmul dimensions must be positive")
    var a_shape = Shape(M, K)
    var b_shape = Shape(K, N)
    var output_shape = Shape(M, N)
    if tape.var_numel(a_idx) != a_shape.numel() or tape.var_numel(b_idx) != b_shape.numel():
        raise Error("Tracked matmul input spans do not match dimensions")
    var ash = tape.var_shapes[a_idx].copy()
    var bsh = tape.var_shapes[b_idx].copy()
    if not ((len(ash) == 2 and ash[0] == M and ash[1] == K) or (len(ash) == 1 and M == 1 and ash[0] == K)):
        raise Error("Tracked matmul left shape mismatch")
    if len(bsh) != 2 or bsh[0] != K or bsh[1] != N:
        raise Error("Tracked matmul right shape mismatch")
    var dims = List[Int]()
    dims.append(M)
    dims.append(N)
    var c_idx = tape.add_variable(dims^, requires_grad=True)

    for i in range(M):
        for j in range(N):
            var sum_val = Float32(0.0)
            for k in range(K):
                sum_val += tape.get_data(a_idx, i * K + k) * tape.get_data(b_idx, k * N + j)
            tape.set_data(c_idx, i * N + j, sum_val)

    tape.record(TapeEntry(OP_MATMUL(), a_idx, b_idx, c_idx, cached_int=M, cached_int2=K, cached_int3=N))
    return c_idx


def tracked_relu(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked ReLU: y = max(0, x). SIMD-accelerated with compare+select."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    var x_off = tape.var_offset(x_idx)
    var y_off = tape.var_offset(y_idx)
    var zero = Float32(0.0)

    # SIMD bulk (width-4 compare+select)
    comptime W = AUTOGRAD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        var v0 = tape.data_flat.get(x_off + i)
        var v1 = tape.data_flat.get(x_off + i + 1)
        var v2 = tape.data_flat.get(x_off + i + 2)
        var v3 = tape.data_flat.get(x_off + i + 3)
        tape.data_flat.set(y_off + i, v0 if v0 > zero else zero)
        tape.data_flat.set(y_off + i + 1, v1 if v1 > zero else zero)
        tape.data_flat.set(y_off + i + 2, v2 if v2 > zero else zero)
        tape.data_flat.set(y_off + i + 3, v3 if v3 > zero else zero)
        i += W

    # Scalar remainder
    while i < n:
        var v = tape.data_flat.get(x_off + i)
        tape.data_flat.set(y_off + i, v if v > zero else zero)
        i += 1

    tape.record(TapeEntry(OP_RELU(), x_idx, -1, y_idx))
    return y_idx


def tracked_sigmoid(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked sigmoid: y = 1/(1+exp(-x))."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    for i in range(n):
        var v = Float64(tape.get_data(x_idx, i))
        var s = 1.0 / (1.0 + exp(-v))
        tape.set_data(y_idx, i, Float32(s))

    tape.record(TapeEntry(OP_SIGMOID(), x_idx, -1, y_idx))
    return y_idx


def tracked_tanh(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked tanh: y = tanh(x)."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    for i in range(n):
        tape.set_data(y_idx, i, Float32(tanh(Float64(tape.get_data(x_idx, i)))))

    tape.record(TapeEntry(OP_TANH(), x_idx, -1, y_idx))
    return y_idx


def tracked_exp(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked exp: y = exp(x)."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    for i in range(n):
        tape.set_data(y_idx, i, Float32(exp(Float64(tape.get_data(x_idx, i)))))

    tape.record(TapeEntry(OP_EXP(), x_idx, -1, y_idx))
    return y_idx


def tracked_log(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked log: y = log(x)."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    for i in range(n):
        tape.set_data(y_idx, i, Float32(log(Float64(tape.get_data(x_idx, i)))))

    tape.record(TapeEntry(OP_LOG(), x_idx, -1, y_idx))
    return y_idx


def tracked_neg(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked negation: y = -x."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    for i in range(n):
        tape.set_data(y_idx, i, -tape.get_data(x_idx, i))

    tape.record(TapeEntry(OP_NEG(), x_idx, -1, y_idx))
    return y_idx


def tracked_scalar_mul(mut tape: Tape, x_idx: Int, scalar: Float64) raises -> Int:
    """Tracked scalar multiplication: y = x * scalar. SIMD-accelerated."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    var s = Float32(scalar)
    var x_off = tape.var_offset(x_idx)
    var y_off = tape.var_offset(y_idx)

    # SIMD bulk (width-4 broadcast multiply)
    comptime W = AUTOGRAD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        tape.data_flat.set(y_off + i, tape.data_flat.get(x_off + i) * s)
        tape.data_flat.set(y_off + i + 1, tape.data_flat.get(x_off + i + 1) * s)
        tape.data_flat.set(y_off + i + 2, tape.data_flat.get(x_off + i + 2) * s)
        tape.data_flat.set(y_off + i + 3, tape.data_flat.get(x_off + i + 3) * s)
        i += W

    # Scalar remainder
    while i < n:
        tape.data_flat.set(y_off + i, tape.data_flat.get(x_off + i) * s)
        i += 1

    tape.record(TapeEntry(OP_SCALAR_MUL(), x_idx, -1, y_idx, cached_scalar=scalar))
    return y_idx


def tracked_scalar_add(mut tape: Tape, x_idx: Int, scalar: Float64) raises -> Int:
    """Tracked scalar addition: y = x + scalar."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    var s = Float32(scalar)
    for i in range(n):
        tape.set_data(y_idx, i, tape.get_data(x_idx, i) + s)

    tape.record(TapeEntry(OP_SCALAR_ADD(), x_idx, -1, y_idx, cached_scalar=scalar))
    return y_idx


def tracked_softmax(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked softmax (1D)."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    if n <= 0:
        raise Error("tracked_softmax requires a nonempty variable")
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    # Find max for numerical stability
    var max_val = Float64(tape.get_data(x_idx, 0))
    for i in range(1, n):
        var v = Float64(tape.get_data(x_idx, i))
        if v > max_val:
            max_val = v

    # Compute exp and sum
    var sum_exp = Float64(0.0)
    for i in range(n):
        var e = exp(Float64(tape.get_data(x_idx, i)) - max_val)
        tape.set_data(y_idx, i, Float32(e))
        sum_exp += e

    # Normalize
    var inv_sum = 1.0 / sum_exp
    for i in range(n):
        tape.set_data(y_idx, i, Float32(Float64(tape.get_data(y_idx, i)) * inv_sum))

    tape.record(TapeEntry(OP_SOFTMAX(), x_idx, -1, y_idx))
    return y_idx


def tracked_sum(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked sum reduction to scalar (stored as 1-element variable)."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    dims.append(1)
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    var total = Float32(0.0)
    for i in range(n):
        total += tape.get_data(x_idx, i)
    tape.set_data(y_idx, 0, total)

    tape.record(TapeEntry(OP_SUM(), x_idx, -1, y_idx, cached_int=n))
    return y_idx


def tracked_mean(mut tape: Tape, x_idx: Int) raises -> Int:
    """Tracked mean reduction to scalar."""
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    if n <= 0:
        raise Error("tracked_mean requires a nonempty variable")
    var dims = List[Int]()
    dims.append(1)
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    var total = Float64(0.0)
    for i in range(n):
        total += Float64(tape.get_data(x_idx, i))
    tape.set_data(y_idx, 0, Float32(total / Float64(n)))

    tape.record(TapeEntry(OP_MEAN(), x_idx, -1, y_idx, cached_int=n))
    return y_idx


def tracked_div(mut tape: Tape, a_idx: Int, b_idx: Int) raises -> Int:
    """Tracked elementwise division: c = a / b."""
    tape.validate_same_shape(a_idx, b_idx)
    var n = tape.var_numel(a_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[a_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var c_idx = tape.add_variable(dims^, requires_grad=True)

    for i in range(n):
        var b_val = Float64(tape.get_data(b_idx, i))
        if b_val != 0.0:
            var val = Float64(tape.get_data(a_idx, i)) / b_val
            tape.set_data(c_idx, i, Float32(val))
        else:
            tape.set_data(c_idx, i, Float32(0.0))

    tape.record(TapeEntry(OP_DIV(), a_idx, b_idx, c_idx))
    return c_idx


def tracked_matmul_right_transpose(mut tape: Tape, a_idx: Int, w_idx: Int,
                                   M: Int, K: Int, N: Int) raises -> Int:
    """C[M,N] = A[M,K] @ W[N,K]^T, preserving row-major weights."""
    from .tape import OP_MATMUL_RIGHT_TRANSPOSE
    tape.validate_variable(a_idx)
    tape.validate_variable(w_idx)
    if M <= 0 or K <= 0 or N <= 0:
        raise Error("Matmul dimensions must be positive")
    if M > 0x7FFFFFFFFFFFFFFF // K or N > 0x7FFFFFFFFFFFFFFF // K or M > 0x7FFFFFFFFFFFFFFF // N:
        raise Error("Matmul shape overflow")
    if tape.var_numel(a_idx) != M * K or tape.var_numel(w_idx) != N * K:
        raise Error("Transposed-right matmul operand size mismatch")
    var ws = tape.var_shapes[w_idx].copy()
    if len(ws) != 2 or ws[0] != N or ws[1] != K:
        raise Error("Transposed-right weight must have shape [N,K]")
    var dims = List[Int]()
    if len(tape.var_shapes[a_idx]) != 1:
        dims.append(M)
    dims.append(N)
    var out_idx = tape.add_variable(dims^, requires_grad=True)
    for m in range(M):
        for n in range(N):
            var value = Float32(0)
            for k in range(K):
                value += tape.get_data(a_idx, m * K + k) * tape.get_data(w_idx, n * K + k)
            tape.set_data(out_idx, m * N + n, value)
    tape.record(TapeEntry(OP_MATMUL_RIGHT_TRANSPOSE(), a_idx, w_idx, out_idx,
                         cached_int=M, cached_int2=K, cached_int3=N))
    return out_idx


def tracked_bias_add(mut tape: Tape, x_idx: Int, bias_idx: Int) raises -> Int:
    """Add a final-axis bias to each row; backward sums rows into the bias."""
    tape.validate_variable(x_idx)
    from .tape import OP_BIAS_ADD
    tape.validate_variable(x_idx)
    tape.validate_variable(bias_idx)
    var shape = tape.var_shapes[x_idx].copy()
    var bias_shape = tape.var_shapes[bias_idx].copy()
    if len(shape) == 0 or len(bias_shape) != 1:
        raise Error("Bias addition needs a vector bias and ranked input")
    var width = shape[len(shape) - 1]
    if width <= 0 or bias_shape[0] != width:
        raise Error("Bias width mismatch")
    var n = tape.var_numel(x_idx)
    var out_idx = tape.add_variable(shape^, requires_grad=True)
    for i in range(n):
        tape.set_data(out_idx, i, tape.get_data(x_idx, i) + tape.get_data(bias_idx, i % width))
    tape.record(TapeEntry(OP_BIAS_ADD(), x_idx, bias_idx, out_idx, cached_int=width))
    return out_idx


def tracked_slice(mut tape: Tape, x_idx: Int, start: Int, length: Int) raises -> Int:
    """Tracked flat slice with reverse scatter into its source."""
    tape.validate_variable(x_idx)
    from .tape import OP_SPLIT
    tape.validate_variable(x_idx)
    var n = tape.var_numel(x_idx)
    if start < 0 or start > n or length < 0 or length > n - start:
        raise Error("Invalid tracked slice")
    var dims = List[Int]()
    dims.append(length)
    var out = tape.add_variable(dims^, requires_grad=True)
    for i in range(length):
        tape.set_data(out, i, tape.get_data(x_idx, start + i))
    tape.record(TapeEntry(OP_SPLIT(), x_idx, -1, out, cached_int=start))
    return out


def tracked_concat_flat(mut tape: Tape, a: Int, b: Int) raises -> Int:
    """Tracked flat concatenation, retaining both graph edges."""
    from .tape import OP_CONCAT
    tape.validate_variable(a)
    tape.validate_variable(b)
    var na = tape.var_numel(a)
    var nb = tape.var_numel(b)
    if nb > 0x7FFFFFFFFFFFFFFF - na:
        raise Error("Tracked concatenation overflow")
    var dims = List[Int]()
    dims.append(na + nb)
    var out = tape.add_variable(dims^, requires_grad=True)
    for i in range(na):
        tape.set_data(out, i, tape.get_data(a, i))
    for i in range(nb):
        tape.set_data(out, na + i, tape.get_data(b, i))
    tape.record(TapeEntry(OP_CONCAT(), a, b, out, cached_int=na))
    return out


def tracked_vector_scale(mut tape: Tape, x: Int, scalar: Int) raises -> Int:
    """Multiply a vector by a tracked scalar (gradient reaches both operands)."""
    from .tape import OP_VECTOR_SCALE
    tape.validate_variable(x)
    tape.validate_variable(scalar)
    if tape.var_numel(scalar) != 1:
        raise Error("Vector scale requires a scalar")
    var dims = tape.var_shapes[x].copy()
    var out = tape.add_variable(dims^, requires_grad=True)
    for i in range(tape.var_numel(x)):
        tape.set_data(out, i, tape.get_data(x, i) * tape.get_data(scalar, 0))
    tape.record(TapeEntry(OP_VECTOR_SCALE(), x, scalar, out))
    return out
