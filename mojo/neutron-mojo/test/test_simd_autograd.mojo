# ===----------------------------------------------------------------------=== #
# Neutron Mojo -- SIMD Autograd Operations Tests
# ===----------------------------------------------------------------------=== #

"""Tests for SIMD-accelerated autograd forward and backward operations.

Verifies that SIMD implementations produce identical results to scalar
for various tensor sizes (aligned, non-aligned, small).
"""

from time import perf_counter_ns

from neutron_mojo.autograd import (
    Tape, run_backward,
    tracked_add, tracked_mul, tracked_relu,
    tracked_scalar_mul, tracked_sum, tracked_matmul,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-4, atol: Float64 = 1e-5) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error(
            "Values not close: " + String(a) + " vs " + String(b)
            + " (diff=" + String(diff) + ")"
        )


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


# ===----------------------------------------------------------------------=== #
# SIMD forward tests
# ===----------------------------------------------------------------------=== #


fn test_simd_add_aligned() raises:
    """SIMD add with aligned size (divisible by 4)."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(8)
    var a = tape.add_variable(d.copy())
    var b = tape.add_variable(d.copy())
    for i in range(8):
        tape.set_data(a, i, Float32(i + 1))
        tape.set_data(b, i, Float32(10 + i))
    var c = tracked_add(tape, a, b)
    for i in range(8):
        var expected = Float32(i + 1) + Float32(10 + i)
        assert_close(tape.get_data(c, i), expected)
    print("  simd_add_aligned: PASS")


fn test_simd_add_non_aligned() raises:
    """SIMD add with non-aligned size (not divisible by 4)."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(7)
    var a = tape.add_variable(d.copy())
    var b = tape.add_variable(d.copy())
    for i in range(7):
        tape.set_data(a, i, Float32(i * 2))
        tape.set_data(b, i, Float32(i * 3))
    var c = tracked_add(tape, a, b)
    for i in range(7):
        var expected = Float32(i * 2) + Float32(i * 3)
        assert_close(tape.get_data(c, i), expected)
    print("  simd_add_non_aligned: PASS")


fn test_simd_mul_aligned() raises:
    """SIMD mul with aligned size."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(12)
    var a = tape.add_variable(d.copy())
    var b = tape.add_variable(d.copy())
    for i in range(12):
        tape.set_data(a, i, Float32(i + 1))
        tape.set_data(b, i, Float32(2.0))
    var c = tracked_mul(tape, a, b)
    for i in range(12):
        assert_close(tape.get_data(c, i), Float32((i + 1) * 2))
    print("  simd_mul_aligned: PASS")


fn test_simd_relu_mixed() raises:
    """SIMD relu with mixed positive/negative values."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(9)
    var x = tape.add_variable(d^)
    tape.set_data(x, 0, Float32(1.0))
    tape.set_data(x, 1, Float32(-2.0))
    tape.set_data(x, 2, Float32(3.0))
    tape.set_data(x, 3, Float32(-4.0))
    tape.set_data(x, 4, Float32(5.0))
    tape.set_data(x, 5, Float32(0.0))
    tape.set_data(x, 6, Float32(-1.0))
    tape.set_data(x, 7, Float32(7.0))
    tape.set_data(x, 8, Float32(-3.0))

    var y = tracked_relu(tape, x)
    assert_close(tape.get_data(y, 0), 1.0)
    assert_close(tape.get_data(y, 1), 0.0)
    assert_close(tape.get_data(y, 2), 3.0)
    assert_close(tape.get_data(y, 3), 0.0)
    assert_close(tape.get_data(y, 4), 5.0)
    assert_close(tape.get_data(y, 5), 0.0)
    assert_close(tape.get_data(y, 6), 0.0)
    assert_close(tape.get_data(y, 7), 7.0)
    assert_close(tape.get_data(y, 8), 0.0)
    print("  simd_relu_mixed: PASS")


fn test_simd_scalar_mul_aligned() raises:
    """SIMD scalar_mul with aligned size."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(8)
    var x = tape.add_variable(d^)
    for i in range(8):
        tape.set_data(x, i, Float32(i + 1))
    var y = tracked_scalar_mul(tape, x, 3.0)
    for i in range(8):
        assert_close(tape.get_data(y, i), Float32((i + 1) * 3))
    print("  simd_scalar_mul_aligned: PASS")


fn test_simd_scalar_mul_non_aligned() raises:
    """SIMD scalar_mul with non-aligned size (size=5)."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(5)
    var x = tape.add_variable(d^)
    for i in range(5):
        tape.set_data(x, i, Float32(i * 2 + 1))
    var y = tracked_scalar_mul(tape, x, -0.5)
    for i in range(5):
        var expected = Float32(i * 2 + 1) * Float32(-0.5)
        assert_close(tape.get_data(y, i), expected)
    print("  simd_scalar_mul_non_aligned: PASS")


# ===----------------------------------------------------------------------=== #
# SIMD backward tests
# ===----------------------------------------------------------------------=== #


fn test_simd_backward_add() raises:
    """SIMD backward add with larger vector."""
    var tape = Tape(8192)
    var d = List[Int]()
    d.append(16)
    var a = tape.add_variable(d.copy())
    var b = tape.add_variable(d.copy())
    for i in range(16):
        tape.set_data(a, i, Float32(i + 1))
        tape.set_data(b, i, Float32(16 - i))
    var c = tracked_add(tape, a, b)
    var loss = tracked_sum(tape, c)
    run_backward(tape, loss)
    # grad(a) = 1 for all elements
    for i in range(16):
        assert_close(tape.get_grad(a, i), 1.0)
        assert_close(tape.get_grad(b, i), 1.0)
    print("  simd_backward_add: PASS")


fn test_simd_backward_mul_large() raises:
    """SIMD backward mul with size > 4 to exercise SIMD path."""
    var tape = Tape(8192)
    var d = List[Int]()
    d.append(10)
    var a = tape.add_variable(d.copy())
    var b = tape.add_variable(d.copy())
    for i in range(10):
        tape.set_data(a, i, Float32(i + 1))
        tape.set_data(b, i, Float32(2.0))
    var c = tracked_mul(tape, a, b)
    var loss = tracked_sum(tape, c)
    run_backward(tape, loss)
    # grad(a) = b_val = 2.0, grad(b) = a_val
    for i in range(10):
        assert_close(tape.get_grad(a, i), 2.0)
        assert_close(tape.get_grad(b, i), Float32(i + 1))
    print("  simd_backward_mul_large: PASS")


fn test_simd_backward_relu_large() raises:
    """SIMD backward relu with larger vector."""
    var tape = Tape(8192)
    var d = List[Int]()
    d.append(10)
    var x = tape.add_variable(d^)
    # Alternating positive/negative
    for i in range(10):
        if i % 2 == 0:
            tape.set_data(x, i, Float32(i + 1))
        else:
            tape.set_data(x, i, Float32(-(i + 1)))
    var y = tracked_relu(tape, x)
    var loss = tracked_sum(tape, y)
    run_backward(tape, loss)
    for i in range(10):
        if i % 2 == 0:
            assert_close(tape.get_grad(x, i), 1.0)
        else:
            assert_close(tape.get_grad(x, i), 0.0)
    print("  simd_backward_relu_large: PASS")


fn test_simd_backward_scalar_mul_large() raises:
    """SIMD backward scalar_mul with larger vector."""
    var tape = Tape(8192)
    var d = List[Int]()
    d.append(13)
    var x = tape.add_variable(d^)
    for i in range(13):
        tape.set_data(x, i, Float32(i))
    var y = tracked_scalar_mul(tape, x, 2.5)
    var loss = tracked_sum(tape, y)
    run_backward(tape, loss)
    for i in range(13):
        assert_close(tape.get_grad(x, i), 2.5, atol=1e-4)
    print("  simd_backward_scalar_mul_large: PASS")


fn test_simd_backward_matmul() raises:
    """SIMD backward matmul correctness."""
    var tape = Tape(8192)
    var da = List[Int]()
    da.append(2)
    da.append(4)
    var db = List[Int]()
    db.append(4)
    db.append(3)
    var a = tape.add_variable(da^)
    var b = tape.add_variable(db^)
    # A = 2x4, B = 4x3
    for i in range(8):
        tape.set_data(a, i, Float32(i + 1))
    for i in range(12):
        tape.set_data(b, i, Float32(12 - i))
    var c = tracked_matmul(tape, a, b, 2, 4, 3)
    var loss = tracked_sum(tape, c)
    run_backward(tape, loss)

    # Verify gradients are non-zero
    var sum_ga = Float64(0.0)
    var sum_gb = Float64(0.0)
    for i in range(8):
        sum_ga += abs(Float64(tape.get_grad(a, i)))
    for i in range(12):
        sum_gb += abs(Float64(tape.get_grad(b, i)))
    if sum_ga < 1e-6:
        raise Error("Expected non-zero gradients for A")
    if sum_gb < 1e-6:
        raise Error("Expected non-zero gradients for B")
    print("  simd_backward_matmul: PASS")


fn test_simd_small_size() raises:
    """SIMD with size=1 (scalar remainder only, no SIMD bulk)."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(1)
    var a = tape.add_variable(d.copy())
    var b = tape.add_variable(d.copy())
    tape.set_data(a, 0, Float32(3.0))
    tape.set_data(b, 0, Float32(5.0))
    var c = tracked_add(tape, a, b)
    assert_close(tape.get_data(c, 0), 8.0)
    var loss = tracked_sum(tape, c)
    run_backward(tape, loss)
    assert_close(tape.get_grad(a, 0), 1.0)
    assert_close(tape.get_grad(b, 0), 1.0)
    print("  simd_small_size: PASS")


fn main() raises:
    print("test_simd_autograd:")

    # Forward SIMD tests
    test_simd_add_aligned()
    test_simd_add_non_aligned()
    test_simd_mul_aligned()
    test_simd_relu_mixed()
    test_simd_scalar_mul_aligned()
    test_simd_scalar_mul_non_aligned()

    # Backward SIMD tests
    test_simd_backward_add()
    test_simd_backward_mul_large()
    test_simd_backward_relu_large()
    test_simd_backward_scalar_mul_large()
    test_simd_backward_matmul()
    test_simd_small_size()

    # Timing comparison: size-1024 add forward+backward
    var tape_big = Tape(65536)
    var d1024 = List[Int]()
    d1024.append(1024)
    var xa = tape_big.add_variable(d1024.copy())
    var xb = tape_big.add_variable(d1024.copy())
    for i in range(1024):
        tape_big.set_data(xa, i, Float32(i))
        tape_big.set_data(xb, i, Float32(1024 - i))
    var t0 = perf_counter_ns()
    var xc = tracked_add(tape_big, xa, xb)
    var xloss = tracked_sum(tape_big, xc)
    run_backward(tape_big, xloss)
    var t1 = perf_counter_ns()
    var elapsed_us = Int(t1 - t0) // 1000
    print("  timing 1024-add fwd+bwd: " + String(elapsed_us) + " us")

    print("ALL PASSED (13 tests)")
