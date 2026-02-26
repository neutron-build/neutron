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

from math import exp, sqrt, tanh, log

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from .tape import (
    Tape, TapeEntry,
    OP_ADD, OP_MUL, OP_MATMUL, OP_RELU, OP_SIGMOID, OP_TANH,
    OP_EXP, OP_LOG, OP_SOFTMAX, OP_SUM, OP_MEAN, OP_SUB,
    OP_SCALAR_MUL, OP_SCALAR_ADD, OP_NEG, OP_DIV,
)

# SIMD width for vectorized forward loops
alias AUTOGRAD_SIMD_WIDTH = 4


fn tracked_add(mut tape: Tape, a_idx: Int, b_idx: Int) -> Int:
    """Tracked elementwise addition: c = a + b. SIMD-accelerated."""
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
    alias W = AUTOGRAD_SIMD_WIDTH
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


fn tracked_sub(mut tape: Tape, a_idx: Int, b_idx: Int) -> Int:
    """Tracked elementwise subtraction: c = a - b."""
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


fn tracked_mul(mut tape: Tape, a_idx: Int, b_idx: Int) -> Int:
    """Tracked elementwise multiplication: c = a * b. SIMD-accelerated."""
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
    alias W = AUTOGRAD_SIMD_WIDTH
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


fn tracked_matmul(mut tape: Tape, a_idx: Int, b_idx: Int, M: Int, K: Int, N: Int) -> Int:
    """Tracked matrix multiplication: C = A @ B.

    A is (M, K), B is (K, N), C is (M, N).
    """
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


fn tracked_relu(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked ReLU: y = max(0, x). SIMD-accelerated with compare+select."""
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
    alias W = AUTOGRAD_SIMD_WIDTH
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


fn tracked_sigmoid(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked sigmoid: y = 1/(1+exp(-x))."""
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


fn tracked_tanh(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked tanh: y = tanh(x)."""
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


fn tracked_exp(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked exp: y = exp(x)."""
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


fn tracked_log(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked log: y = log(x)."""
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


fn tracked_neg(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked negation: y = -x."""
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


fn tracked_scalar_mul(mut tape: Tape, x_idx: Int, scalar: Float64) -> Int:
    """Tracked scalar multiplication: y = x * scalar. SIMD-accelerated."""
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
    alias W = AUTOGRAD_SIMD_WIDTH
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


fn tracked_scalar_add(mut tape: Tape, x_idx: Int, scalar: Float64) -> Int:
    """Tracked scalar addition: y = x + scalar."""
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


fn tracked_softmax(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked softmax (1D)."""
    var n = tape.var_numel(x_idx)
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


fn tracked_sum(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked sum reduction to scalar (stored as 1-element variable)."""
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


fn tracked_mean(mut tape: Tape, x_idx: Int) -> Int:
    """Tracked mean reduction to scalar."""
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    dims.append(1)
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    var total = Float64(0.0)
    for i in range(n):
        total += Float64(tape.get_data(x_idx, i))
    tape.set_data(y_idx, 0, Float32(total / Float64(n)))

    tape.record(TapeEntry(OP_MEAN(), x_idx, -1, y_idx, cached_int=n))
    return y_idx


fn tracked_div(mut tape: Tape, a_idx: Int, b_idx: Int) -> Int:
    """Tracked elementwise division: c = a / b."""
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
