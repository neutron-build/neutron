# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Tiled MatVec + Cache-Optimized Attention Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Sprint 16: tiled matrix-vector multiply and SIMD attention kernels.

Tests:
1. Tiled matvec correctness (vs plain simd_matvec)
2. Tiled matvec with small matrices (fallback path)
3. Tiled matvec with non-tile-aligned dimensions
4. Parallel tiled matvec correctness
5. SIMD attention scores correctness
6. SIMD attention weighted sum correctness
7. Online softmax attention correctness
8. Online softmax attention vs two-pass attention
9. Tiled matvec benchmark (print throughput)
10. Online attention benchmark (print throughput)
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import (
    simd_matvec,
    simd_softmax,
    tiled_simd_matvec,
    par_tiled_simd_matvec,
    simd_attention_scores,
    simd_attention_weighted_sum,
    simd_online_softmax_attention,
)
from math import abs, sqrt, exp
from time import perf_counter_ns


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn _fill_matrix(mut t: Tensor[DType.float32], rows: Int, cols: Int, offset: Int):
    """Fill matrix with deterministic values."""
    for i in range(rows):
        for j in range(cols):
            var val = Float32((i * 7 + j * 3 + 1) % 13) * 0.1 - 0.6
            t.set(offset + i * cols + j, val)


fn _fill_vector(mut t: Tensor[DType.float32], n: Int, offset: Int):
    """Fill vector with deterministic values."""
    for i in range(n):
        var val = Float32((i * 5 + 2) % 11) * 0.1 - 0.5
        t.set(offset + i, val)


fn _max_diff(
    a: Tensor[DType.float32], a_off: Int,
    b: Tensor[DType.float32], b_off: Int,
    n: Int,
) -> Float32:
    """Compute max absolute difference between two vectors."""
    var md: Float32 = 0.0
    for i in range(n):
        var d = abs(a.get(a_off + i) - b.get(b_off + i))
        if d > md:
            md = d
    return md


fn test_tiled_matvec_correctness() raises:
    """Tiled matvec should match plain simd_matvec for large matrices."""
    var rows = 128
    var cols = 512

    var weight = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))
    _fill_matrix(weight, rows, cols, 0)
    _fill_vector(x, cols, 0)

    var out_plain = Tensor[DType.float32](Shape(rows))
    var out_tiled = Tensor[DType.float32](Shape(rows))

    simd_matvec(out_plain, 0, weight, 0, x, 0, rows, cols)
    tiled_simd_matvec(out_tiled, 0, weight, 0, x, 0, rows, cols)

    var md = _max_diff(out_plain, 0, out_tiled, 0, rows)
    assert_true(md < 1e-4, "Tiled matvec should match plain (max_diff=" + String(md) + ")")

    print("  tiled_matvec_correctness: PASS")


fn test_tiled_matvec_small_fallback() raises:
    """Tiled matvec should work for small matrices (fallback path)."""
    var rows = 4
    var cols = 8

    var weight = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))
    _fill_matrix(weight, rows, cols, 0)
    _fill_vector(x, cols, 0)

    var out_plain = Tensor[DType.float32](Shape(rows))
    var out_tiled = Tensor[DType.float32](Shape(rows))

    simd_matvec(out_plain, 0, weight, 0, x, 0, rows, cols)
    tiled_simd_matvec(out_tiled, 0, weight, 0, x, 0, rows, cols)

    var md = _max_diff(out_plain, 0, out_tiled, 0, rows)
    assert_true(md < 1e-5, "Small tiled matvec should match plain")

    print("  tiled_matvec_small_fallback: PASS")


fn test_tiled_matvec_non_aligned() raises:
    """Tiled matvec with dimensions not aligned to tile sizes."""
    var rows = 37
    var cols = 311  # Not a multiple of 256

    var weight = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))
    _fill_matrix(weight, rows, cols, 0)
    _fill_vector(x, cols, 0)

    var out_plain = Tensor[DType.float32](Shape(rows))
    var out_tiled = Tensor[DType.float32](Shape(rows))

    simd_matvec(out_plain, 0, weight, 0, x, 0, rows, cols)
    tiled_simd_matvec(out_tiled, 0, weight, 0, x, 0, rows, cols)

    var md = _max_diff(out_plain, 0, out_tiled, 0, rows)
    assert_true(md < 1e-4, "Non-aligned tiled matvec should match plain (max_diff=" + String(md) + ")")

    print("  tiled_matvec_non_aligned: PASS")


fn test_par_tiled_matvec() raises:
    """Parallel tiled matvec should match plain simd_matvec."""
    var rows = 256
    var cols = 512

    var weight = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))
    _fill_matrix(weight, rows, cols, 0)
    _fill_vector(x, cols, 0)

    var out_plain = Tensor[DType.float32](Shape(rows))
    var out_par = Tensor[DType.float32](Shape(rows))

    simd_matvec(out_plain, 0, weight, 0, x, 0, rows, cols)
    par_tiled_simd_matvec(out_par, 0, weight, 0, x, 0, rows, cols)

    var md = _max_diff(out_plain, 0, out_par, 0, rows)
    assert_true(md < 1e-4, "Par tiled matvec should match plain (max_diff=" + String(md) + ")")

    print("  par_tiled_matvec: PASS")


fn test_simd_attention_scores() raises:
    """SIMD attention scores should match scalar computation."""
    var head_dim = 8
    var seq_len = 4
    var scale = Float32(1.0 / sqrt(Float64(head_dim)))

    var q = Tensor[DType.float32](Shape(head_dim))
    _fill_vector(q, head_dim, 0)

    # Build flat key cache: [seq_len * head_dim]
    var k_cache = Tensor[DType.float32](Shape(seq_len * head_dim))
    for pos in range(seq_len):
        for d in range(head_dim):
            var val = Float32((pos * 3 + d * 7 + 1) % 9) * 0.1 - 0.4
            k_cache.set(pos * head_dim + d, val)

    var scores = Tensor[DType.float32](Shape(seq_len))
    simd_attention_scores(scores, q, 0, k_cache, head_dim, seq_len, head_dim, scale)

    # Verify against scalar
    for pos in range(seq_len):
        var expected: Float32 = 0.0
        for d in range(head_dim):
            expected += q.get(d) * k_cache.get(pos * head_dim + d)
        expected *= scale
        var diff = abs(scores.get(pos) - expected)
        assert_true(diff < 1e-5, "Score mismatch at pos " + String(pos))

    print("  simd_attention_scores: PASS")


fn test_simd_attention_weighted_sum() raises:
    """SIMD weighted sum should match scalar computation."""
    var head_dim = 8
    var seq_len = 4

    # Create softmax-like weights (sum to 1)
    var weights = Tensor[DType.float32](Shape(seq_len))
    weights.set(0, 0.1)
    weights.set(1, 0.4)
    weights.set(2, 0.3)
    weights.set(3, 0.2)

    var v_cache = Tensor[DType.float32](Shape(seq_len * head_dim))
    for pos in range(seq_len):
        for d in range(head_dim):
            var val = Float32((pos * 5 + d * 2 + 3) % 7) * 0.2 - 0.6
            v_cache.set(pos * head_dim + d, val)

    var out = Tensor[DType.float32](Shape(head_dim))
    simd_attention_weighted_sum(out, 0, weights, v_cache, head_dim, seq_len, head_dim)

    # Verify against scalar
    for d in range(head_dim):
        var expected: Float32 = 0.0
        for pos in range(seq_len):
            expected += weights.get(pos) * v_cache.get(pos * head_dim + d)
        var diff = abs(out.get(d) - expected)
        assert_true(diff < 1e-5, "Weighted sum mismatch at d=" + String(d))

    print("  simd_attention_weighted_sum: PASS")


fn test_online_softmax_attention() raises:
    """Online softmax attention should produce valid output."""
    var head_dim = 8
    var seq_len = 4
    var scale = Float32(1.0 / sqrt(Float64(head_dim)))

    var q = Tensor[DType.float32](Shape(head_dim))
    _fill_vector(q, head_dim, 0)

    var k_cache = Tensor[DType.float32](Shape(seq_len * head_dim))
    var v_cache = Tensor[DType.float32](Shape(seq_len * head_dim))
    for pos in range(seq_len):
        for d in range(head_dim):
            k_cache.set(pos * head_dim + d, Float32((pos * 3 + d * 7 + 1) % 9) * 0.1 - 0.4)
            v_cache.set(pos * head_dim + d, Float32((pos * 5 + d * 2 + 3) % 7) * 0.2 - 0.6)

    var out = Tensor[DType.float32](Shape(head_dim))
    simd_online_softmax_attention(out, 0, q, 0, k_cache, v_cache, head_dim, seq_len, head_dim, scale)

    # Output should be non-zero (it's a weighted sum of values)
    var has_nonzero = False
    for d in range(head_dim):
        if abs(out.get(d)) > 1e-10:
            has_nonzero = True
            break
    assert_true(has_nonzero, "Online attention output should be non-zero")

    print("  online_softmax_attention: PASS")


fn test_online_vs_twopass_attention() raises:
    """Online softmax attention should match two-pass (scores + softmax + weighted sum)."""
    var head_dim = 8
    var seq_len = 6
    var scale = Float32(1.0 / sqrt(Float64(head_dim)))

    var q = Tensor[DType.float32](Shape(head_dim))
    _fill_vector(q, head_dim, 0)

    var k_cache = Tensor[DType.float32](Shape(seq_len * head_dim))
    var v_cache = Tensor[DType.float32](Shape(seq_len * head_dim))
    for pos in range(seq_len):
        for d in range(head_dim):
            k_cache.set(pos * head_dim + d, Float32((pos * 3 + d * 7 + 1) % 9) * 0.1 - 0.4)
            v_cache.set(pos * head_dim + d, Float32((pos * 5 + d * 2 + 3) % 7) * 0.2 - 0.6)

    # Two-pass: scores → softmax → weighted sum
    var scores_raw = Tensor[DType.float32](Shape(seq_len))
    simd_attention_scores(scores_raw, q, 0, k_cache, head_dim, seq_len, head_dim, scale)
    var scores = Tensor[DType.float32](Shape(seq_len))
    simd_softmax(scores, 0, scores_raw, 0, seq_len)
    var out_twopass = Tensor[DType.float32](Shape(head_dim))
    simd_attention_weighted_sum(out_twopass, 0, scores, v_cache, head_dim, seq_len, head_dim)

    # Online: single pass
    var out_online = Tensor[DType.float32](Shape(head_dim))
    simd_online_softmax_attention(out_online, 0, q, 0, k_cache, v_cache, head_dim, seq_len, head_dim, scale)

    var md = _max_diff(out_twopass, 0, out_online, 0, head_dim)
    assert_true(md < 1e-4, "Online attention should match two-pass (max_diff=" + String(md) + ")")

    print("  online_vs_twopass_attention: PASS")


fn test_tiled_matvec_benchmark() raises:
    """Benchmark tiled vs plain matvec (256x1024)."""
    var rows = 256
    var cols = 1024

    var weight = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))
    _fill_matrix(weight, rows, cols, 0)
    _fill_vector(x, cols, 0)

    var out = Tensor[DType.float32](Shape(rows))

    # Warmup
    for _ in range(3):
        simd_matvec(out, 0, weight, 0, x, 0, rows, cols)
        tiled_simd_matvec(out, 0, weight, 0, x, 0, rows, cols)

    # Benchmark plain
    var iters = 100
    var start = perf_counter_ns()
    for _ in range(iters):
        simd_matvec(out, 0, weight, 0, x, 0, rows, cols)
    var plain_ns = perf_counter_ns() - start

    # Benchmark tiled
    start = perf_counter_ns()
    for _ in range(iters):
        tiled_simd_matvec(out, 0, weight, 0, x, 0, rows, cols)
    var tiled_ns = perf_counter_ns() - start

    var plain_us = Float64(plain_ns) / 1000.0 / Float64(iters)
    var tiled_us = Float64(tiled_ns) / 1000.0 / Float64(iters)

    print("  tiled_matvec_benchmark: plain=" + String(Int(plain_us)) + "us, tiled=" + String(Int(tiled_us)) + "us")
    print("  tiled_matvec_benchmark: PASS")


fn test_online_attention_benchmark() raises:
    """Benchmark online vs two-pass attention."""
    var head_dim = 64
    var seq_len = 128
    var scale = Float32(1.0 / sqrt(Float64(head_dim)))

    var q = Tensor[DType.float32](Shape(head_dim))
    var k_cache = Tensor[DType.float32](Shape(seq_len * head_dim))
    var v_cache = Tensor[DType.float32](Shape(seq_len * head_dim))
    _fill_vector(q, head_dim, 0)
    for pos in range(seq_len):
        for d in range(head_dim):
            k_cache.set(pos * head_dim + d, Float32((pos * 3 + d * 7) % 13) * 0.08 - 0.5)
            v_cache.set(pos * head_dim + d, Float32((pos * 5 + d * 2) % 11) * 0.1 - 0.5)

    var out = Tensor[DType.float32](Shape(head_dim))
    var scores = Tensor[DType.float32](Shape(seq_len))

    # Warmup
    for _ in range(3):
        simd_attention_scores(scores, q, 0, k_cache, head_dim, seq_len, head_dim, scale)
        simd_online_softmax_attention(out, 0, q, 0, k_cache, v_cache, head_dim, seq_len, head_dim, scale)

    var iters = 200

    # Two-pass benchmark
    var scores_in = Tensor[DType.float32](Shape(seq_len))
    var start = perf_counter_ns()
    for _ in range(iters):
        simd_attention_scores(scores_in, q, 0, k_cache, head_dim, seq_len, head_dim, scale)
        simd_softmax(scores, 0, scores_in, 0, seq_len)
        simd_attention_weighted_sum(out, 0, scores, v_cache, head_dim, seq_len, head_dim)
    var twopass_ns = perf_counter_ns() - start

    # Online benchmark
    start = perf_counter_ns()
    for _ in range(iters):
        simd_online_softmax_attention(out, 0, q, 0, k_cache, v_cache, head_dim, seq_len, head_dim, scale)
    var online_ns = perf_counter_ns() - start

    var twopass_us = Float64(twopass_ns) / 1000.0 / Float64(iters)
    var online_us = Float64(online_ns) / 1000.0 / Float64(iters)

    print("  online_attention_benchmark: twopass=" + String(Int(twopass_us)) + "us, online=" + String(Int(online_us)) + "us")
    print("  online_attention_benchmark: PASS")


fn main() raises:
    print("test_tiled_matvec:")

    test_tiled_matvec_correctness()
    test_tiled_matvec_small_fallback()
    test_tiled_matvec_non_aligned()
    test_par_tiled_matvec()
    test_simd_attention_scores()
    test_simd_attention_weighted_sum()
    test_online_softmax_attention()
    test_online_vs_twopass_attention()
    test_tiled_matvec_benchmark()
    test_online_attention_benchmark()

    print("ALL PASSED")
