# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 7 Integration Tests + Benchmarks
# ===----------------------------------------------------------------------=== #

"""Integration tests verifying SIMD acceleration produces correct results.
Includes timing benchmarks comparing SIMD vs scalar at realistic sizes.
"""

from math import abs
from time import perf_counter_ns
from neutron_mojo.tensor.simd_math import (
    simd_dot,
    simd_matvec,
    simd_rmsnorm,
    simd_softmax,
    simd_silu,
    simd_swiglu,
    simd_axpy,
    par_simd_matvec,
)
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


# ===----------------------------------------------------------------------=== #
# Scalar references for comparison
# ===----------------------------------------------------------------------=== #

fn scalar_matvec(
    mut out: Tensor[DType.float32], o_off: Int,
    w: Tensor[DType.float32], w_off: Int,
    x: Tensor[DType.float32], x_off: Int,
    rows: Int, cols: Int,
):
    for i in range(rows):
        var dot: Float32 = 0.0
        for j in range(cols):
            dot += w.get(w_off + i * cols + j) * x.get(x_off + j)
        out.set(o_off + i, dot)


fn scalar_dot(a: Tensor[DType.float32], b: Tensor[DType.float32], n: Int) -> Float32:
    var result: Float32 = 0.0
    for i in range(n):
        result += a.get(i) * b.get(i)
    return result


# ===----------------------------------------------------------------------=== #
# Correctness Tests
# ===----------------------------------------------------------------------=== #

fn test_simd_matvec_matches_scalar_at_scale() raises:
    """Verify SIMD matvec matches scalar at LLaMA-sized dimensions."""
    var rows = 256
    var cols = 1024

    var w = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))
    for i in range(rows * cols):
        w.set(i, Float32(i % 13) * 0.001 - 0.006)
    for i in range(cols):
        x.set(i, Float32(i % 9) * 0.01 - 0.04)

    var simd_out = Tensor[DType.float32](Shape(rows))
    var scalar_out = Tensor[DType.float32](Shape(rows))
    simd_matvec(simd_out, 0, w, 0, x, 0, rows, cols)
    scalar_matvec(scalar_out, 0, w, 0, x, 0, rows, cols)

    var max_err: Float32 = 0.0
    for i in range(rows):
        var err = abs(simd_out.get(i) - scalar_out.get(i))
        if err > max_err:
            max_err = err
    assert_true(max_err < 0.01, "SIMD matches scalar, max_err=" + String(max_err))

    print("  simd_matvec_matches_scalar_at_scale: PASS")


fn test_par_matvec_matches_sequential() raises:
    """Verify parallel matvec matches sequential at scale."""
    var rows = 512
    var cols = 256

    var w = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))
    for i in range(rows * cols):
        w.set(i, Float32(i % 17) * 0.002 - 0.015)
    for i in range(cols):
        x.set(i, Float32(i % 11) * 0.01 - 0.05)

    var seq_out = Tensor[DType.float32](Shape(rows))
    var par_out = Tensor[DType.float32](Shape(rows))
    simd_matvec(seq_out, 0, w, 0, x, 0, rows, cols)
    par_simd_matvec(par_out, 0, w, 0, x, 0, rows, cols)

    var max_err: Float32 = 0.0
    for i in range(rows):
        var err = abs(par_out.get(i) - seq_out.get(i))
        if err > max_err:
            max_err = err
    assert_true(max_err < 0.001, "parallel matches sequential")

    print("  par_matvec_matches_sequential: PASS")


fn test_rmsnorm_simd_matches_reference() raises:
    """Verify SIMD RMSNorm matches manual computation at scale."""
    var n = 512
    var x = Tensor[DType.float32](Shape(n))
    var w = Tensor[DType.float32](Shape(n))
    for i in range(n):
        x.set(i, Float32(i % 20) * 0.1 - 1.0)
        w.set(i, 1.0)

    var out = Tensor[DType.float32](Shape(n))
    simd_rmsnorm(out, 0, x, 0, w, 0, n)

    # Manual: compute rms
    var ss: Float32 = 0.0
    for i in range(n):
        ss += x.get(i) * x.get(i)
    from math import sqrt
    var rms = Float32(sqrt(Float64(ss / Float32(n) + 1e-6)))

    # Check a few values
    for i in range(0, n, 50):
        var expected = x.get(i) / rms
        assert_near(out.get(i), expected, 0.01, "rmsnorm[" + String(i) + "]")

    print("  rmsnorm_simd_matches_reference: PASS")


fn test_full_pipeline_simd() raises:
    """Test: matvec → rmsnorm → swiglu → matvec (mini transformer layer)."""
    var hidden = 64
    var ffn = 128

    # Input
    var x = Tensor[DType.float32](Shape(hidden))
    for i in range(hidden):
        x.set(i, Float32(i) * 0.01)

    # Norm weights
    var norm_w = Tensor[DType.float32](Shape(hidden))
    for i in range(hidden):
        norm_w.set(i, 1.0)

    # RMSNorm
    var normed = Tensor[DType.float32](Shape(hidden))
    simd_rmsnorm(normed, 0, x, 0, norm_w, 0, hidden)

    # Gate projection
    var w_gate = Tensor[DType.float32](Shape(ffn * hidden))
    for i in range(ffn * hidden):
        w_gate.set(i, Float32(i % 5) * 0.001)
    var gate = Tensor[DType.float32](Shape(ffn))
    simd_matvec(gate, 0, w_gate, 0, normed, 0, ffn, hidden)

    # Up projection
    var w_up = Tensor[DType.float32](Shape(ffn * hidden))
    for i in range(ffn * hidden):
        w_up.set(i, Float32(i % 7) * 0.001)
    var up = Tensor[DType.float32](Shape(ffn))
    simd_matvec(up, 0, w_up, 0, normed, 0, ffn, hidden)

    # Fused SwiGLU
    var swiglu_out = Tensor[DType.float32](Shape(ffn))
    simd_swiglu(swiglu_out, 0, gate, 0, up, 0, ffn)

    # Down projection
    var w_down = Tensor[DType.float32](Shape(hidden * ffn))
    for i in range(hidden * ffn):
        w_down.set(i, Float32(i % 3) * 0.001)
    var down = Tensor[DType.float32](Shape(hidden))
    simd_matvec(down, 0, w_down, 0, swiglu_out, 0, hidden, ffn)

    # Residual
    simd_axpy(down, 0, x, 0, 1.0, hidden)

    # Verify non-zero output
    var any_nonzero = False
    for i in range(hidden):
        if abs(down.get(i)) > 0.0001:
            any_nonzero = True
    assert_true(any_nonzero, "pipeline produces non-zero output")

    print("  full_pipeline_simd: PASS")


fn test_model_end_to_end_with_simd() raises:
    """Test that Model (which now uses SIMD internally) still generates."""
    from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate

    var p = tiny_test_params()
    var model = Model(p)

    # Set some non-trivial embeddings
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)

    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)

    var tokens = generate(model, prompt, max_new_tokens=3)
    assert_true(len(tokens) == 3, "generated 3 tokens")

    # All tokens should be valid
    for i in range(len(tokens)):
        assert_true(tokens[i] >= 0, "token >= 0")
        assert_true(tokens[i] < p.vocab_size, "token < vocab")

    print("  model_end_to_end_with_simd: PASS")


# ===----------------------------------------------------------------------=== #
# Benchmarks
# ===----------------------------------------------------------------------=== #

fn bench_matvec() raises:
    """Benchmark: SIMD vs scalar matvec at realistic sizes."""
    var rows = 4096
    var cols = 4096
    var iters = 5

    var w = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))
    for i in range(rows * cols):
        w.set(i, 0.001)
    for i in range(cols):
        x.set(i, 0.01)

    # Scalar benchmark
    var scalar_out = Tensor[DType.float32](Shape(rows))
    var t0 = perf_counter_ns()
    for _ in range(iters):
        scalar_matvec(scalar_out, 0, w, 0, x, 0, rows, cols)
    var scalar_ns = (perf_counter_ns() - t0) // iters

    # SIMD benchmark
    var simd_out = Tensor[DType.float32](Shape(rows))
    var t1 = perf_counter_ns()
    for _ in range(iters):
        simd_matvec(simd_out, 0, w, 0, x, 0, rows, cols)
    var simd_ns = (perf_counter_ns() - t1) // iters

    # Parallel benchmark
    var par_out = Tensor[DType.float32](Shape(rows))
    var t2 = perf_counter_ns()
    for _ in range(iters):
        par_simd_matvec(par_out, 0, w, 0, x, 0, rows, cols)
    var par_ns = (perf_counter_ns() - t2) // iters

    var scalar_ms = Float32(scalar_ns) / 1_000_000.0
    var simd_ms = Float32(simd_ns) / 1_000_000.0
    var par_ms = Float32(par_ns) / 1_000_000.0

    print("  bench_matvec (4096x4096):")
    print("    scalar:   " + String(scalar_ms) + " ms")
    print("    simd:     " + String(simd_ms) + " ms")
    print("    parallel: " + String(par_ms) + " ms")

    if simd_ms > 0.0:
        print("    SIMD speedup:     " + String(scalar_ms / simd_ms) + "x")
    if par_ms > 0.0:
        print("    Parallel speedup: " + String(scalar_ms / par_ms) + "x")

    # Correctness check
    for i in range(rows):
        assert_near(simd_out.get(i), scalar_out.get(i), 0.1, "bench correctness")


fn bench_dot() raises:
    """Benchmark: SIMD vs scalar dot product at transformer scale."""
    var n = 4096
    var iters = 100

    var a = Tensor[DType.float32](Shape(n))
    var b = Tensor[DType.float32](Shape(n))
    for i in range(n):
        a.set(i, Float32(i % 100) * 0.01)
        b.set(i, Float32(i % 50) * 0.02)

    # Scalar
    var t0 = perf_counter_ns()
    var s_result: Float32 = 0.0
    for _ in range(iters):
        s_result = scalar_dot(a, b, n)
    var scalar_ns = (perf_counter_ns() - t0) // iters

    # SIMD
    var t1 = perf_counter_ns()
    var v_result: Float32 = 0.0
    for _ in range(iters):
        v_result = simd_dot(a, 0, b, 0, n)
    var simd_ns = (perf_counter_ns() - t1) // iters

    var scalar_us = Float32(scalar_ns) / 1000.0
    var simd_us = Float32(simd_ns) / 1000.0

    print("  bench_dot (4096-dim):")
    print("    scalar: " + String(scalar_us) + " us")
    print("    simd:   " + String(simd_us) + " us")
    if simd_us > 0.0:
        print("    speedup: " + String(scalar_us / simd_us) + "x")

    assert_near(v_result, s_result, 1.0, "dot bench correctness")


fn main() raises:
    print("test_sprint7_integration:")

    # Correctness
    test_simd_matvec_matches_scalar_at_scale()
    test_par_matvec_matches_sequential()
    test_rmsnorm_simd_matches_reference()
    test_full_pipeline_simd()
    test_model_end_to_end_with_simd()

    print("")

    # Benchmarks
    bench_dot()
    bench_matvec()

    print("")
    print("ALL PASSED")
