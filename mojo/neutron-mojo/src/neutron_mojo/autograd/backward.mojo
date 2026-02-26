# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Autograd Backward Pass
# ===----------------------------------------------------------------------=== #

"""Reverse-mode automatic differentiation backward pass.

Walks the tape in reverse, dispatching per-op backward functions
that accumulate gradients into input variables.

NOTE: All functions use tape.get_data()/get_grad()/accumulate_grad()
instead of data_ptr() to avoid the Mojo 0.26.2 aliasing bug.
"""

from math import exp, sqrt, tanh, log

from neutron_mojo.tensor.tensor import Tensor
from .tape import (
    Tape, TapeEntry,
    OP_ADD, OP_MUL, OP_MATMUL, OP_RELU, OP_SIGMOID, OP_TANH,
    OP_EXP, OP_LOG, OP_SOFTMAX, OP_SUM, OP_MEAN, OP_SUB,
    OP_DIV, OP_POW, OP_SQRT, OP_NEG, OP_CLAMP, OP_SCALAR_MUL,
    OP_RMSNORM, OP_LAYERNORM, OP_GELU, OP_SILU, OP_SWIGLU,
    OP_RESHAPE, OP_TRANSPOSE, OP_CONCAT, OP_SPLIT,
    OP_LOG_SOFTMAX, OP_CROSS_ENTROPY, OP_MSE, OP_EMBEDDING,
    OP_SCALAR_ADD, OP_L1, OP_BCE, OP_KL_DIV,
)

# SIMD width for vectorized backward loops
alias BACKWARD_SIMD_WIDTH = 4


fn run_backward(mut tape: Tape, loss_var_idx: Int):
    """Run backward pass: seed loss gradient with 1.0, reverse-walk tape.

    Args:
        tape: The autograd tape with recorded operations.
        loss_var_idx: The variable index of the scalar loss.
    """
    # Seed loss gradient
    tape.set_grad(loss_var_idx, 0, Float32(1.0))

    # Reverse walk
    var num_entries = tape.num_entries()
    var i = num_entries - 1
    while i >= 0:
        var entry = tape.get_entry(i)
        _dispatch_backward(tape, entry)
        i -= 1


fn _dispatch_backward(mut tape: Tape, entry: TapeEntry):
    """Dispatch to the appropriate backward function based on op code."""
    var op = entry.op_kind
    if op == OP_ADD():
        _backward_add(tape, entry)
    elif op == OP_SUB():
        _backward_sub(tape, entry)
    elif op == OP_MUL():
        _backward_mul(tape, entry)
    elif op == OP_MATMUL():
        _backward_matmul(tape, entry)
    elif op == OP_RELU():
        _backward_relu(tape, entry)
    elif op == OP_SIGMOID():
        _backward_sigmoid(tape, entry)
    elif op == OP_TANH():
        _backward_tanh(tape, entry)
    elif op == OP_EXP():
        _backward_exp(tape, entry)
    elif op == OP_LOG():
        _backward_log(tape, entry)
    elif op == OP_SOFTMAX():
        _backward_softmax(tape, entry)
    elif op == OP_SUM():
        _backward_sum(tape, entry)
    elif op == OP_MEAN():
        _backward_mean(tape, entry)
    elif op == OP_NEG():
        _backward_neg(tape, entry)
    elif op == OP_SCALAR_MUL():
        _backward_scalar_mul(tape, entry)
    elif op == OP_SCALAR_ADD():
        _backward_scalar_add(tape, entry)
    elif op == OP_DIV():
        _backward_div(tape, entry)
    elif op == OP_POW():
        _backward_pow(tape, entry)
    elif op == OP_SQRT():
        _backward_sqrt(tape, entry)
    elif op == OP_CLAMP():
        _backward_clamp(tape, entry)
    elif op == OP_RMSNORM():
        _backward_rmsnorm(tape, entry)
    elif op == OP_LAYERNORM():
        _backward_layernorm(tape, entry)
    elif op == OP_GELU():
        _backward_gelu(tape, entry)
    elif op == OP_SILU():
        _backward_silu(tape, entry)
    elif op == OP_SWIGLU():
        _backward_swiglu(tape, entry)
    elif op == OP_RESHAPE():
        _backward_reshape(tape, entry)
    elif op == OP_TRANSPOSE():
        _backward_transpose(tape, entry)
    elif op == OP_CONCAT():
        _backward_concat(tape, entry)
    elif op == OP_SPLIT():
        _backward_split(tape, entry)
    elif op == OP_LOG_SOFTMAX():
        _backward_log_softmax(tape, entry)
    elif op == OP_CROSS_ENTROPY():
        _backward_cross_entropy(tape, entry)
    elif op == OP_MSE():
        _backward_mse(tape, entry)
    elif op == OP_EMBEDDING():
        _backward_embedding(tape, entry)
    elif op == OP_L1():
        _backward_l1(tape, entry)
    elif op == OP_BCE():
        _backward_bce(tape, entry)
    elif op == OP_KL_DIV():
        _backward_kl_div(tape, entry)


# ===----------------------------------------------------------------------=== #
# Basic op backward functions
# ===----------------------------------------------------------------------=== #


fn _backward_add(mut tape: Tape, entry: TapeEntry):
    """d/da(a+b) = 1, d/db(a+b) = 1. SIMD gradient pass-through."""
    var n = tape.var_numel(entry.output_idx)
    var out_off = tape.var_offset(entry.output_idx)
    var has_a = entry.input0_idx >= 0 and tape.var_requires_grad[entry.input0_idx]
    var has_b = entry.input1_idx >= 0 and tape.var_requires_grad[entry.input1_idx]

    if has_a and has_b:
        _backward_add_both(tape, entry, n, out_off)
    elif has_a:
        _backward_add_single(tape, entry.input0_idx, n, out_off)
    elif has_b:
        _backward_add_single(tape, entry.input1_idx, n, out_off)


fn _backward_add_both(mut tape: Tape, entry: TapeEntry, n: Int, out_off: Int):
    """SIMD add backward for both inputs."""
    var a_off = tape.var_offset(entry.input0_idx)
    var b_off = tape.var_offset(entry.input1_idx)
    alias W = BACKWARD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        var g0 = tape.grad_flat.get(out_off + i)
        var g1 = tape.grad_flat.get(out_off + i + 1)
        var g2 = tape.grad_flat.get(out_off + i + 2)
        var g3 = tape.grad_flat.get(out_off + i + 3)
        tape.grad_flat.set(a_off + i, tape.grad_flat.get(a_off + i) + g0)
        tape.grad_flat.set(a_off + i + 1, tape.grad_flat.get(a_off + i + 1) + g1)
        tape.grad_flat.set(a_off + i + 2, tape.grad_flat.get(a_off + i + 2) + g2)
        tape.grad_flat.set(a_off + i + 3, tape.grad_flat.get(a_off + i + 3) + g3)
        tape.grad_flat.set(b_off + i, tape.grad_flat.get(b_off + i) + g0)
        tape.grad_flat.set(b_off + i + 1, tape.grad_flat.get(b_off + i + 1) + g1)
        tape.grad_flat.set(b_off + i + 2, tape.grad_flat.get(b_off + i + 2) + g2)
        tape.grad_flat.set(b_off + i + 3, tape.grad_flat.get(b_off + i + 3) + g3)
        i += W
    while i < n:
        var g = tape.grad_flat.get(out_off + i)
        tape.grad_flat.set(a_off + i, tape.grad_flat.get(a_off + i) + g)
        tape.grad_flat.set(b_off + i, tape.grad_flat.get(b_off + i) + g)
        i += 1


fn _backward_add_single(mut tape: Tape, in_idx: Int, n: Int, out_off: Int):
    """SIMD add backward for single input."""
    var in_off = tape.var_offset(in_idx)
    alias W = BACKWARD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        var g0 = tape.grad_flat.get(out_off + i)
        var g1 = tape.grad_flat.get(out_off + i + 1)
        var g2 = tape.grad_flat.get(out_off + i + 2)
        var g3 = tape.grad_flat.get(out_off + i + 3)
        tape.grad_flat.set(in_off + i, tape.grad_flat.get(in_off + i) + g0)
        tape.grad_flat.set(in_off + i + 1, tape.grad_flat.get(in_off + i + 1) + g1)
        tape.grad_flat.set(in_off + i + 2, tape.grad_flat.get(in_off + i + 2) + g2)
        tape.grad_flat.set(in_off + i + 3, tape.grad_flat.get(in_off + i + 3) + g3)
        i += W
    while i < n:
        var g = tape.grad_flat.get(out_off + i)
        tape.grad_flat.set(in_off + i, tape.grad_flat.get(in_off + i) + g)
        i += 1


fn _backward_sub(mut tape: Tape, entry: TapeEntry):
    """d/da(a-b) = 1, d/db(a-b) = -1."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        if entry.input0_idx >= 0:
            tape.accumulate_grad(entry.input0_idx, i, grad_out)
        if entry.input1_idx >= 0:
            tape.accumulate_grad(entry.input1_idx, i, -grad_out)


fn _backward_mul(mut tape: Tape, entry: TapeEntry):
    """d/da(a*b) = b, d/db(a*b) = a. SIMD cross-multiply."""
    var n = tape.var_numel(entry.output_idx)
    var out_off = tape.var_offset(entry.output_idx)
    var a_off = tape.var_offset(entry.input0_idx)
    var b_off = tape.var_offset(entry.input1_idx)
    var has_a = entry.input0_idx >= 0 and tape.var_requires_grad[entry.input0_idx]
    var has_b = entry.input1_idx >= 0 and tape.var_requires_grad[entry.input1_idx]

    alias W = BACKWARD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        var g0 = tape.grad_flat.get(out_off + i)
        var g1 = tape.grad_flat.get(out_off + i + 1)
        var g2 = tape.grad_flat.get(out_off + i + 2)
        var g3 = tape.grad_flat.get(out_off + i + 3)
        if has_a:
            var bv0 = tape.data_flat.get(b_off + i)
            var bv1 = tape.data_flat.get(b_off + i + 1)
            var bv2 = tape.data_flat.get(b_off + i + 2)
            var bv3 = tape.data_flat.get(b_off + i + 3)
            tape.grad_flat.set(a_off + i, tape.grad_flat.get(a_off + i) + g0 * bv0)
            tape.grad_flat.set(a_off + i + 1, tape.grad_flat.get(a_off + i + 1) + g1 * bv1)
            tape.grad_flat.set(a_off + i + 2, tape.grad_flat.get(a_off + i + 2) + g2 * bv2)
            tape.grad_flat.set(a_off + i + 3, tape.grad_flat.get(a_off + i + 3) + g3 * bv3)
        if has_b:
            var av0 = tape.data_flat.get(a_off + i)
            var av1 = tape.data_flat.get(a_off + i + 1)
            var av2 = tape.data_flat.get(a_off + i + 2)
            var av3 = tape.data_flat.get(a_off + i + 3)
            tape.grad_flat.set(b_off + i, tape.grad_flat.get(b_off + i) + g0 * av0)
            tape.grad_flat.set(b_off + i + 1, tape.grad_flat.get(b_off + i + 1) + g1 * av1)
            tape.grad_flat.set(b_off + i + 2, tape.grad_flat.get(b_off + i + 2) + g2 * av2)
            tape.grad_flat.set(b_off + i + 3, tape.grad_flat.get(b_off + i + 3) + g3 * av3)
        i += W

    # Scalar remainder
    while i < n:
        var g = tape.grad_flat.get(out_off + i)
        if has_a:
            var bv = tape.data_flat.get(b_off + i)
            tape.grad_flat.set(a_off + i, tape.grad_flat.get(a_off + i) + g * bv)
        if has_b:
            var av = tape.data_flat.get(a_off + i)
            tape.grad_flat.set(b_off + i, tape.grad_flat.get(b_off + i) + g * av)
        i += 1


fn _backward_matmul(mut tape: Tape, entry: TapeEntry):
    """C = A @ B where A:(M,K), B:(K,N), C:(M,N).
    dA = dC @ B^T, dB = A^T @ dC. SIMD inner loops.
    """
    var M = entry.cached_int
    var K = entry.cached_int2
    var N = entry.cached_int3

    # dA = dC @ B^T : (M,N) @ (N,K) -> (M,K)
    if entry.input0_idx >= 0 and tape.var_requires_grad[entry.input0_idx]:
        _backward_matmul_dA(tape, entry, M, K, N)

    # dB = A^T @ dC : (K,M) @ (M,N) -> (K,N)
    if entry.input1_idx >= 0 and tape.var_requires_grad[entry.input1_idx]:
        _backward_matmul_dB(tape, entry, M, K, N)


fn _backward_matmul_dA(
    mut tape: Tape, entry: TapeEntry, M: Int, K: Int, N: Int
):
    """dA = dC @ B^T with SIMD inner dot product over N."""
    var out_off = tape.var_offset(entry.output_idx)
    var b_off = tape.var_offset(entry.input1_idx)
    var a_off = tape.var_offset(entry.input0_idx)
    alias W = BACKWARD_SIMD_WIDTH

    for i in range(M):
        for k in range(K):
            var sum_val = Float32(0.0)
            # SIMD bulk over j (columns of dC / B)
            var j = 0
            while j + W <= N:
                var g0 = tape.grad_flat.get(out_off + i * N + j)
                var g1 = tape.grad_flat.get(out_off + i * N + j + 1)
                var g2 = tape.grad_flat.get(out_off + i * N + j + 2)
                var g3 = tape.grad_flat.get(out_off + i * N + j + 3)
                var b0 = tape.data_flat.get(b_off + k * N + j)
                var b1 = tape.data_flat.get(b_off + k * N + j + 1)
                var b2 = tape.data_flat.get(b_off + k * N + j + 2)
                var b3 = tape.data_flat.get(b_off + k * N + j + 3)
                sum_val += g0 * b0 + g1 * b1 + g2 * b2 + g3 * b3
                j += W
            while j < N:
                sum_val += tape.grad_flat.get(out_off + i * N + j) * tape.data_flat.get(b_off + k * N + j)
                j += 1
            tape.grad_flat.set(
                a_off + i * K + k,
                tape.grad_flat.get(a_off + i * K + k) + sum_val,
            )


fn _backward_matmul_dB(
    mut tape: Tape, entry: TapeEntry, M: Int, K: Int, N: Int
):
    """dB = A^T @ dC with SIMD inner dot product over M."""
    var out_off = tape.var_offset(entry.output_idx)
    var a_off = tape.var_offset(entry.input0_idx)
    var b_off = tape.var_offset(entry.input1_idx)
    alias W = BACKWARD_SIMD_WIDTH

    for k in range(K):
        for j in range(N):
            var sum_val = Float32(0.0)
            # SIMD bulk over i (rows of A / dC)
            var i = 0
            while i + W <= M:
                var a0 = tape.data_flat.get(a_off + i * K + k)
                var a1 = tape.data_flat.get(a_off + (i + 1) * K + k)
                var a2 = tape.data_flat.get(a_off + (i + 2) * K + k)
                var a3 = tape.data_flat.get(a_off + (i + 3) * K + k)
                var g0 = tape.grad_flat.get(out_off + i * N + j)
                var g1 = tape.grad_flat.get(out_off + (i + 1) * N + j)
                var g2 = tape.grad_flat.get(out_off + (i + 2) * N + j)
                var g3 = tape.grad_flat.get(out_off + (i + 3) * N + j)
                sum_val += a0 * g0 + a1 * g1 + a2 * g2 + a3 * g3
                i += W
            while i < M:
                sum_val += tape.data_flat.get(a_off + i * K + k) * tape.grad_flat.get(out_off + i * N + j)
                i += 1
            tape.grad_flat.set(
                b_off + k * N + j,
                tape.grad_flat.get(b_off + k * N + j) + sum_val,
            )


fn _backward_relu(mut tape: Tape, entry: TapeEntry):
    """d/dx ReLU(x) = 1 if x > 0 else 0. SIMD mask."""
    var n = tape.var_numel(entry.output_idx)
    if not tape.var_requires_grad[entry.input0_idx]:
        return
    var out_off = tape.var_offset(entry.output_idx)
    var x_off = tape.var_offset(entry.input0_idx)
    var zero = Float32(0.0)

    alias W = BACKWARD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        var x0 = tape.data_flat.get(x_off + i)
        var x1 = tape.data_flat.get(x_off + i + 1)
        var x2 = tape.data_flat.get(x_off + i + 2)
        var x3 = tape.data_flat.get(x_off + i + 3)
        var g0 = tape.grad_flat.get(out_off + i)
        var g1 = tape.grad_flat.get(out_off + i + 1)
        var g2 = tape.grad_flat.get(out_off + i + 2)
        var g3 = tape.grad_flat.get(out_off + i + 3)
        # Mask: pass gradient only if x > 0
        if x0 > zero:
            tape.grad_flat.set(x_off + i, tape.grad_flat.get(x_off + i) + g0)
        if x1 > zero:
            tape.grad_flat.set(x_off + i + 1, tape.grad_flat.get(x_off + i + 1) + g1)
        if x2 > zero:
            tape.grad_flat.set(x_off + i + 2, tape.grad_flat.get(x_off + i + 2) + g2)
        if x3 > zero:
            tape.grad_flat.set(x_off + i + 3, tape.grad_flat.get(x_off + i + 3) + g3)
        i += W

    # Scalar remainder
    while i < n:
        var x_val = tape.data_flat.get(x_off + i)
        if x_val > zero:
            var g = tape.grad_flat.get(out_off + i)
            tape.grad_flat.set(x_off + i, tape.grad_flat.get(x_off + i) + g)
        i += 1


fn _backward_sigmoid(mut tape: Tape, entry: TapeEntry):
    """d/dx sigmoid(x) = sigmoid(x) * (1 - sigmoid(x)).
    We use the output value s directly.
    """
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var s = tape.get_data(entry.output_idx, i)
        var local_grad = s * (Float32(1.0) - s)
        tape.accumulate_grad(entry.input0_idx, i, grad_out * local_grad)


fn _backward_tanh(mut tape: Tape, entry: TapeEntry):
    """d/dx tanh(x) = 1 - tanh(x)^2. Use output value."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var t = tape.get_data(entry.output_idx, i)
        var local_grad = Float32(1.0) - t * t
        tape.accumulate_grad(entry.input0_idx, i, grad_out * local_grad)


fn _backward_exp(mut tape: Tape, entry: TapeEntry):
    """d/dx exp(x) = exp(x). Use output value."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var y = tape.get_data(entry.output_idx, i)
        tape.accumulate_grad(entry.input0_idx, i, grad_out * y)


fn _backward_log(mut tape: Tape, entry: TapeEntry):
    """d/dx log(x) = 1/x."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var x_val = tape.get_data(entry.input0_idx, i)
        if Float64(x_val) != 0.0:
            tape.accumulate_grad(entry.input0_idx, i, Float32(Float64(grad_out) / Float64(x_val)))


fn _backward_neg(mut tape: Tape, entry: TapeEntry):
    """d/dx (-x) = -1."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        tape.accumulate_grad(entry.input0_idx, i, -grad_out)


fn _backward_scalar_mul(mut tape: Tape, entry: TapeEntry):
    """d/dx (x * s) = s. SIMD broadcast multiply."""
    var n = tape.var_numel(entry.output_idx)
    if not tape.var_requires_grad[entry.input0_idx]:
        return
    var s = Float32(entry.cached_scalar)
    var out_off = tape.var_offset(entry.output_idx)
    var x_off = tape.var_offset(entry.input0_idx)

    alias W = BACKWARD_SIMD_WIDTH
    var i = 0
    while i + W <= n:
        var g0 = tape.grad_flat.get(out_off + i) * s
        var g1 = tape.grad_flat.get(out_off + i + 1) * s
        var g2 = tape.grad_flat.get(out_off + i + 2) * s
        var g3 = tape.grad_flat.get(out_off + i + 3) * s
        tape.grad_flat.set(x_off + i, tape.grad_flat.get(x_off + i) + g0)
        tape.grad_flat.set(x_off + i + 1, tape.grad_flat.get(x_off + i + 1) + g1)
        tape.grad_flat.set(x_off + i + 2, tape.grad_flat.get(x_off + i + 2) + g2)
        tape.grad_flat.set(x_off + i + 3, tape.grad_flat.get(x_off + i + 3) + g3)
        i += W

    # Scalar remainder
    while i < n:
        var g = tape.grad_flat.get(out_off + i) * s
        tape.grad_flat.set(x_off + i, tape.grad_flat.get(x_off + i) + g)
        i += 1


fn _backward_scalar_add(mut tape: Tape, entry: TapeEntry):
    """d/dx (x + s) = 1. Gradient passes through."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        tape.accumulate_grad(entry.input0_idx, i, grad_out)


fn _backward_softmax(mut tape: Tape, entry: TapeEntry):
    """Softmax backward: dy_i/dx_j = s_i * (delta_ij - s_j).

    For each element i: grad_in[i] = sum_j(grad_out[j] * s[j] * (delta_ij - s[i]))
    Simplified: grad_in[i] = s[i] * (grad_out[i] - dot(grad_out, s))
    """
    var n = tape.var_numel(entry.output_idx)

    # Compute dot(grad_out, softmax_output)
    var dot_val = Float64(0.0)
    for i in range(n):
        dot_val += Float64(tape.get_grad(entry.output_idx, i)) * Float64(tape.get_data(entry.output_idx, i))

    # grad_in[i] = s[i] * (grad_out[i] - dot)
    for i in range(n):
        var s_i = Float64(tape.get_data(entry.output_idx, i))
        var g_i = Float64(tape.get_grad(entry.output_idx, i))
        var local_grad = Float32(s_i * (g_i - dot_val))
        tape.accumulate_grad(entry.input0_idx, i, local_grad)


fn _backward_sum(mut tape: Tape, entry: TapeEntry):
    """d/dx_i sum(x) = 1 for all i. Broadcast gradient."""
    var n = entry.cached_int  # original input size
    var grad_out = tape.get_grad(entry.output_idx, 0)
    for i in range(n):
        tape.accumulate_grad(entry.input0_idx, i, grad_out)


fn _backward_mean(mut tape: Tape, entry: TapeEntry):
    """d/dx_i mean(x) = 1/n for all i."""
    var n = entry.cached_int
    var grad_out = tape.get_grad(entry.output_idx, 0)
    var scale = Float32(Float64(grad_out) / Float64(n))
    for i in range(n):
        tape.accumulate_grad(entry.input0_idx, i, scale)


# ===----------------------------------------------------------------------=== #
# Extended backward functions (Sprint 52)
# ===----------------------------------------------------------------------=== #


fn _backward_div(mut tape: Tape, entry: TapeEntry):
    """d/da(a/b) = 1/b, d/db(a/b) = -a/b^2."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var a_val = Float64(tape.get_data(entry.input0_idx, i))
        var b_val = Float64(tape.get_data(entry.input1_idx, i))
        if entry.input0_idx >= 0:
            tape.accumulate_grad(entry.input0_idx, i, Float32(Float64(grad_out) / b_val))
        if entry.input1_idx >= 0:
            tape.accumulate_grad(entry.input1_idx, i, Float32(-Float64(grad_out) * a_val / (b_val * b_val)))


fn _backward_pow(mut tape: Tape, entry: TapeEntry):
    """d/dx(x^n) = n * x^(n-1)."""
    var n = tape.var_numel(entry.output_idx)
    var exponent = entry.cached_scalar
    from math import pow
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var x_val = Float64(tape.get_data(entry.input0_idx, i))
        var local_grad = Float32(exponent * pow(x_val, exponent - 1.0))
        tape.accumulate_grad(entry.input0_idx, i, grad_out * local_grad)


fn _backward_sqrt(mut tape: Tape, entry: TapeEntry):
    """d/dx sqrt(x) = 0.5 / sqrt(x)."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var y_val = Float64(tape.get_data(entry.output_idx, i))
        if y_val > 0.0:
            var local_grad = Float32(0.5 / y_val)
            tape.accumulate_grad(entry.input0_idx, i, grad_out * local_grad)


fn _backward_clamp(mut tape: Tape, entry: TapeEntry):
    """d/dx clamp(x) = 1 if min <= x <= max, else 0."""
    var n = tape.var_numel(entry.output_idx)
    var min_val = Float32(entry.cached_scalar)
    var max_val = Float32(entry.cached_scalar2)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var x_val = tape.get_data(entry.input0_idx, i)
        if x_val >= min_val and x_val <= max_val:
            tape.accumulate_grad(entry.input0_idx, i, grad_out)


fn _backward_rmsnorm(mut tape: Tape, entry: TapeEntry):
    """RMSNorm backward: compute d_x and d_gamma properly.

    y = (x / rms) * gamma, where rms = sqrt(mean(x^2) + eps).
    d_gamma[i] = grad_out[i] * x[i] / rms
    d_x[i] = gamma[i] * (grad_out[i] / rms - x[i] * dot / (n * rms^3))
    where dot = sum(grad_out[j] * gamma[j] * x[j])
    """
    var n = entry.cached_int
    if n == 0:
        n = tape.var_numel(entry.output_idx)
    var eps_val = entry.cached_scalar
    var x_idx = entry.input0_idx
    var gamma_idx = entry.input1_idx

    # Recompute RMS from input
    var sum_sq = Float64(0.0)
    for i in range(n):
        var v = Float64(tape.get_data(x_idx, i))
        sum_sq += v * v
    var rms = sqrt(sum_sq / Float64(n) + eps_val)
    var inv_rms = 1.0 / rms

    # Compute dot = sum(grad_out * gamma * x)
    var dot_val = Float64(0.0)
    for i in range(n):
        var go = Float64(tape.get_grad(entry.output_idx, i))
        var g = Float64(tape.get_data(gamma_idx, i))
        var x = Float64(tape.get_data(x_idx, i))
        dot_val += go * g * x

    var rms_cubed = rms * rms * rms
    var coeff = dot_val / (Float64(n) * rms_cubed)

    for i in range(n):
        var go = Float64(tape.get_grad(entry.output_idx, i))
        var g = Float64(tape.get_data(gamma_idx, i))
        var x = Float64(tape.get_data(x_idx, i))
        # d_x
        if x_idx >= 0 and tape.var_requires_grad[x_idx]:
            var dx = g * (go * inv_rms - x * coeff)
            tape.accumulate_grad(x_idx, i, Float32(dx))
        # d_gamma
        if gamma_idx >= 0 and tape.var_requires_grad[gamma_idx]:
            var dg = go * x * inv_rms
            tape.accumulate_grad(gamma_idx, i, Float32(dg))


fn _backward_layernorm(mut tape: Tape, entry: TapeEntry):
    """LayerNorm backward: compute d_x, d_gamma, d_beta.

    y = ((x - mean) / std) * gamma + beta
    """
    var n = entry.cached_int
    if n == 0:
        n = tape.var_numel(entry.output_idx)
    var eps_val = entry.cached_scalar
    var x_idx = entry.input0_idx
    var gamma_idx = entry.input1_idx
    var beta_idx = entry.cached_int3

    # Recompute mean
    var sum_val = Float64(0.0)
    for i in range(n):
        sum_val += Float64(tape.get_data(x_idx, i))
    var mean_val = sum_val / Float64(n)

    # Recompute variance and std_inv
    var sum_sq = Float64(0.0)
    for i in range(n):
        var diff = Float64(tape.get_data(x_idx, i)) - mean_val
        sum_sq += diff * diff
    var variance = sum_sq / Float64(n)
    var std_inv = 1.0 / sqrt(variance + eps_val)

    # Compute intermediate sums for dx
    var sum_dy_gamma = Float64(0.0)
    var sum_dy_gamma_xhat = Float64(0.0)
    for i in range(n):
        var go = Float64(tape.get_grad(entry.output_idx, i))
        var g = Float64(tape.get_data(gamma_idx, i))
        var xhat = (Float64(tape.get_data(x_idx, i)) - mean_val) * std_inv
        sum_dy_gamma += go * g
        sum_dy_gamma_xhat += go * g * xhat

    var inv_n = 1.0 / Float64(n)

    for i in range(n):
        var go = Float64(tape.get_grad(entry.output_idx, i))
        var g = Float64(tape.get_data(gamma_idx, i))
        var xhat = (Float64(tape.get_data(x_idx, i)) - mean_val) * std_inv

        # d_x
        if x_idx >= 0 and tape.var_requires_grad[x_idx]:
            var dx = std_inv * (go * g - inv_n * sum_dy_gamma - inv_n * xhat * sum_dy_gamma_xhat)
            tape.accumulate_grad(x_idx, i, Float32(dx))

        # d_gamma
        if gamma_idx >= 0 and tape.var_requires_grad[gamma_idx]:
            tape.accumulate_grad(gamma_idx, i, Float32(go * xhat))

        # d_beta
        if beta_idx >= 0 and tape.var_requires_grad[beta_idx]:
            tape.accumulate_grad(beta_idx, i, Float32(go))


fn _backward_gelu(mut tape: Tape, entry: TapeEntry):
    """GeLU backward: approximate derivative."""
    var n = tape.var_numel(entry.output_idx)
    var pi = 3.14159265358979323846
    var sqrt_2_over_pi = sqrt(2.0 / pi)
    var coef = 0.044715
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var x = Float64(tape.get_data(entry.input0_idx, i))
        var x3 = x * x * x
        var inner = sqrt_2_over_pi * (x + coef * x3)
        var tanh_val = tanh(inner)
        var sech2 = 1.0 - tanh_val * tanh_val
        var d_inner = sqrt_2_over_pi * (1.0 + 3.0 * coef * x * x)
        var d_gelu = 0.5 * (1.0 + tanh_val) + 0.5 * x * sech2 * d_inner
        tape.accumulate_grad(entry.input0_idx, i, Float32(Float64(grad_out) * d_gelu))


fn _backward_silu(mut tape: Tape, entry: TapeEntry):
    """SiLU backward: d/dx(x*sigmoid(x)) = sigmoid(x) + x*sigmoid(x)*(1-sigmoid(x))."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var x = Float64(tape.get_data(entry.input0_idx, i))
        var s = 1.0 / (1.0 + exp(-x))
        var d_silu = s + x * s * (1.0 - s)
        tape.accumulate_grad(entry.input0_idx, i, Float32(Float64(grad_out) * d_silu))


fn _backward_swiglu(mut tape: Tape, entry: TapeEntry):
    """SwiGLU backward: d/dx(silu(gate)*x) and d/dgate."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        var x_val = Float64(tape.get_data(entry.input0_idx, i))
        var g_val = Float64(tape.get_data(entry.input1_idx, i))
        var sig_g = 1.0 / (1.0 + exp(-g_val))
        var silu_g = g_val * sig_g
        # d/dx = silu(gate)
        tape.accumulate_grad(entry.input0_idx, i, Float32(Float64(grad_out) * silu_g))
        # d/dgate = x * d_silu(gate)
        var d_silu_g = sig_g + g_val * sig_g * (1.0 - sig_g)
        tape.accumulate_grad(entry.input1_idx, i, Float32(Float64(grad_out) * x_val * d_silu_g))


fn _backward_reshape(mut tape: Tape, entry: TapeEntry):
    """Reshape backward: just pass gradient through (same flat data)."""
    var n = tape.var_numel(entry.output_idx)
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        tape.accumulate_grad(entry.input0_idx, i, grad_out)


fn _backward_transpose(mut tape: Tape, entry: TapeEntry):
    """Transpose backward for 2D: transpose the gradient back."""
    var rows = entry.cached_int   # original rows (output cols)
    var cols = entry.cached_int2  # original cols (output rows)
    # Output is (cols, rows), input was (rows, cols)
    for i in range(cols):
        for j in range(rows):
            var g = tape.get_grad(entry.output_idx, i * rows + j)
            tape.accumulate_grad(entry.input0_idx, j * cols + i, g)


fn _backward_concat(mut tape: Tape, entry: TapeEntry):
    """Concat backward: split gradient back to inputs."""
    var n0 = tape.var_numel(entry.input0_idx)
    # First input gets first n0 gradients
    for i in range(n0):
        var g = tape.get_grad(entry.output_idx, i)
        tape.accumulate_grad(entry.input0_idx, i, g)
    # Second input gets remaining gradients
    if entry.input1_idx >= 0:
        var n1 = tape.var_numel(entry.input1_idx)
        for i in range(n1):
            var g = tape.get_grad(entry.output_idx, n0 + i)
            tape.accumulate_grad(entry.input1_idx, i, g)


fn _backward_split(mut tape: Tape, entry: TapeEntry):
    """Split backward: concatenate gradients back to input."""
    var n = tape.var_numel(entry.output_idx)
    var split_offset = entry.cached_int  # offset within input
    for i in range(n):
        var grad_out = tape.get_grad(entry.output_idx, i)
        tape.accumulate_grad(entry.input0_idx, split_offset + i, grad_out)


fn _backward_log_softmax(mut tape: Tape, entry: TapeEntry):
    """Log-softmax backward: grad_in = grad_out - softmax * sum(grad_out)."""
    var n = tape.var_numel(entry.output_idx)

    # Compute sum of grad_out
    var sum_grad = Float64(0.0)
    for i in range(n):
        sum_grad += Float64(tape.get_grad(entry.output_idx, i))

    # grad_in[i] = grad_out[i] - exp(log_softmax[i]) * sum_grad
    for i in range(n):
        var log_s = Float64(tape.get_data(entry.output_idx, i))
        var s = exp(log_s)
        var g = Float64(tape.get_grad(entry.output_idx, i))
        tape.accumulate_grad(entry.input0_idx, i, Float32(g - s * sum_grad))


fn _backward_cross_entropy(mut tape: Tape, entry: TapeEntry):
    """Cross-entropy backward: grad = softmax(logits) - one_hot(target).

    Fused and numerically stable. Target stored in cached_int.
    Vocab size stored in cached_int2.
    """
    var vocab_size = entry.cached_int2
    var target = entry.cached_int
    var loss_grad = tape.get_grad(entry.output_idx, 0)

    # Compute softmax of logits
    var max_val = Float64(tape.get_data(entry.input0_idx, 0))
    for i in range(1, vocab_size):
        var v = Float64(tape.get_data(entry.input0_idx, i))
        if v > max_val:
            max_val = v

    var sum_exp = Float64(0.0)
    for i in range(vocab_size):
        sum_exp += exp(Float64(tape.get_data(entry.input0_idx, i)) - max_val)

    for i in range(vocab_size):
        var softmax_i = exp(Float64(tape.get_data(entry.input0_idx, i)) - max_val) / sum_exp
        var target_val = 1.0 if i == target else 0.0
        var g = Float32(Float64(loss_grad) * (softmax_i - target_val))
        tape.accumulate_grad(entry.input0_idx, i, g)


fn _backward_mse(mut tape: Tape, entry: TapeEntry):
    """MSE backward: d/dx MSE = 2*(pred-target)/n."""
    var n = tape.var_numel(entry.input0_idx)
    var loss_grad = tape.get_grad(entry.output_idx, 0)
    var scale = Float32(2.0 * Float64(loss_grad) / Float64(n))
    for i in range(n):
        var pred = tape.get_data(entry.input0_idx, i)
        var target = tape.get_data(entry.input1_idx, i)
        var g = scale * (pred - target)
        tape.accumulate_grad(entry.input0_idx, i, g)
        tape.accumulate_grad(entry.input1_idx, i, -g)


fn _backward_embedding(mut tape: Tape, entry: TapeEntry):
    """Embedding backward: accumulate gradient into the looked-up row."""
    var embed_dim = entry.cached_int
    var token_id = entry.cached_int2
    # Accumulate output grad into the token_id-th row of embedding table
    for d in range(embed_dim):
        var g = tape.get_grad(entry.output_idx, d)
        tape.accumulate_grad(entry.input0_idx, token_id * embed_dim + d, g)


fn _backward_l1(mut tape: Tape, entry: TapeEntry):
    """L1 loss backward: d/dx L1 = sign(pred - target) / n."""
    var n = tape.var_numel(entry.input0_idx)
    var loss_grad = tape.get_grad(entry.output_idx, 0)
    var inv_n = Float32(Float64(loss_grad) / Float64(n))
    for i in range(n):
        var pred = Float64(tape.get_data(entry.input0_idx, i))
        var target = Float64(tape.get_data(entry.input1_idx, i))
        var diff = pred - target
        var sign_val = Float32(0.0)
        if diff > 0.0:
            sign_val = Float32(1.0)
        elif diff < 0.0:
            sign_val = Float32(-1.0)
        tape.accumulate_grad(entry.input0_idx, i, sign_val * inv_n)
        tape.accumulate_grad(entry.input1_idx, i, -sign_val * inv_n)


fn _backward_bce(mut tape: Tape, entry: TapeEntry):
    """Binary cross-entropy backward: d/dp BCE = (-t/p + (1-t)/(1-p)) / n."""
    var n = tape.var_numel(entry.input0_idx)
    var loss_grad = tape.get_grad(entry.output_idx, 0)
    for i in range(n):
        var p = Float64(tape.get_data(entry.input0_idx, i))
        var t = Float64(tape.get_data(entry.input1_idx, i))
        # Clamp p for numerical stability
        p = max(1e-7, min(1.0 - 1e-7, p))
        var dp = (-t / p + (1.0 - t) / (1.0 - p)) / Float64(n)
        tape.accumulate_grad(entry.input0_idx, i, Float32(Float64(loss_grad) * dp))


fn _backward_kl_div(mut tape: Tape, entry: TapeEntry):
    """KL divergence backward w.r.t. q: d/dq KL(p||q) = -p/q."""
    var n = tape.var_numel(entry.input0_idx)
    var loss_grad = tape.get_grad(entry.output_idx, 0)
    for i in range(n):
        var p_val = Float64(tape.get_data(entry.input0_idx, i))
        var q_val = Float64(tape.get_data(entry.input1_idx, i))
        if p_val > 1e-10 and q_val > 1e-10:
            # d/dq = -p/q
            var dq = -p_val / q_val
            tape.accumulate_grad(entry.input1_idx, i, Float32(Float64(loss_grad) * dq))
