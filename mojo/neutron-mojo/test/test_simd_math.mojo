# ===----------------------------------------------------------------------=== #
# Neutron Mojo — SIMD Math Primitives Tests
# ===----------------------------------------------------------------------=== #

"""Tests for SIMD-vectorized math kernels.

Each test verifies that the SIMD implementation produces identical results
to a naive scalar reference. This ensures correctness before wiring into nn/.
"""

from math import abs, exp, sqrt
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
# Scalar references for validation
# ===----------------------------------------------------------------------=== #

fn scalar_dot(a: Tensor[DType.float32], a_off: Int, b: Tensor[DType.float32], b_off: Int, n: Int) -> Float32:
    var result: Float32 = 0.0
    for i in range(n):
        result += a.get(a_off + i) * b.get(b_off + i)
    return result


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


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_simd_dot_basic() raises:
    """Test SIMD dot product with known values."""
    var a = Tensor[DType.float32](Shape(4))
    var b = Tensor[DType.float32](Shape(4))
    a.set(0, 1.0)
    a.set(1, 2.0)
    a.set(2, 3.0)
    a.set(3, 4.0)
    b.set(0, 5.0)
    b.set(1, 6.0)
    b.set(2, 7.0)
    b.set(3, 8.0)

    var result = simd_dot(a, 0, b, 0, 4)
    # 1*5 + 2*6 + 3*7 + 4*8 = 5 + 12 + 21 + 32 = 70
    assert_near(result, 70.0, 0.01, "dot = 70")

    print("  simd_dot_basic: PASS")


fn test_simd_dot_large() raises:
    """Test SIMD dot vs scalar on large vector (exercises SIMD + tail)."""
    var n = 257  # Not a multiple of any SIMD width
    var a = Tensor[DType.float32](Shape(n))
    var b = Tensor[DType.float32](Shape(n))
    for i in range(n):
        a.set(i, Float32(i) * 0.01)
        b.set(i, Float32(n - i) * 0.01)

    var simd_result = simd_dot(a, 0, b, 0, n)
    var scalar_result = scalar_dot(a, 0, b, 0, n)
    assert_near(simd_result, scalar_result, 0.1, "simd_dot matches scalar")

    print("  simd_dot_large: PASS")


fn test_simd_dot_with_offset() raises:
    """Test SIMD dot product with non-zero offsets."""
    var a = Tensor[DType.float32](Shape(10))
    var b = Tensor[DType.float32](Shape(10))
    for i in range(10):
        a.set(i, Float32(i))
        b.set(i, Float32(i) * 2.0)

    # Dot of a[5:8] . b[2:5] = a[5]*b[2] + a[6]*b[3] + a[7]*b[4]
    # = 5*4 + 6*6 + 7*8 = 20 + 36 + 56 = 112
    var result = simd_dot(a, 5, b, 2, 3)
    assert_near(result, 112.0, 0.01, "offset dot = 112")

    print("  simd_dot_with_offset: PASS")


fn test_simd_matvec_basic() raises:
    """Test SIMD matvec with identity-like matrix."""
    # 2x3 matrix: [[1,0,0],[0,1,0]]
    var w = Tensor[DType.float32](Shape(6))
    w.set(0, 1.0)
    w.set(4, 1.0)

    var x = Tensor[DType.float32](Shape(3))
    x.set(0, 5.0)
    x.set(1, 7.0)
    x.set(2, 9.0)

    var out = Tensor[DType.float32](Shape(2))
    simd_matvec(out, 0, w, 0, x, 0, 2, 3)
    assert_near(out.get(0), 5.0, 0.01, "matvec[0] = 5")
    assert_near(out.get(1), 7.0, 0.01, "matvec[1] = 7")

    print("  simd_matvec_basic: PASS")


fn test_simd_matvec_large() raises:
    """Test SIMD matvec vs scalar on realistic sizes."""
    var rows = 64
    var cols = 128
    var w = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))

    # Fill with patterns
    for i in range(rows * cols):
        w.set(i, Float32(i % 7) * 0.01 - 0.03)
    for i in range(cols):
        x.set(i, Float32(i % 5) * 0.1 - 0.2)

    var simd_out = Tensor[DType.float32](Shape(rows))
    var scalar_out = Tensor[DType.float32](Shape(rows))
    simd_matvec(simd_out, 0, w, 0, x, 0, rows, cols)
    scalar_matvec(scalar_out, 0, w, 0, x, 0, rows, cols)

    for i in range(rows):
        assert_near(simd_out.get(i), scalar_out.get(i), 0.01, "matvec row " + String(i))

    print("  simd_matvec_large: PASS")


fn test_simd_matvec_with_offset() raises:
    """Test SIMD matvec with weight offset (like flat layer storage)."""
    # Simulate: weights at offset 100 in a large flat tensor
    var total = 200
    var rows = 2
    var cols = 3
    var w_offset = 100

    var w = Tensor[DType.float32](Shape(total))
    # Weight at offset 100: [[1,2,3],[4,5,6]]
    w.set(100, 1.0)
    w.set(101, 2.0)
    w.set(102, 3.0)
    w.set(103, 4.0)
    w.set(104, 5.0)
    w.set(105, 6.0)

    var x = Tensor[DType.float32](Shape(3))
    x.set(0, 1.0)
    x.set(1, 1.0)
    x.set(2, 1.0)

    var out = Tensor[DType.float32](Shape(2))
    simd_matvec(out, 0, w, w_offset, x, 0, rows, cols)
    # [1+2+3, 4+5+6] = [6, 15]
    assert_near(out.get(0), 6.0, 0.01, "offset matvec[0] = 6")
    assert_near(out.get(1), 15.0, 0.01, "offset matvec[1] = 15")

    print("  simd_matvec_with_offset: PASS")


fn test_simd_rmsnorm() raises:
    """Test SIMD RMSNorm matches manual computation."""
    var n = 4
    var x = Tensor[DType.float32](Shape(n))
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 3.0)
    x.set(3, 4.0)

    var w = Tensor[DType.float32](Shape(n))
    w.set(0, 1.0)
    w.set(1, 1.0)
    w.set(2, 1.0)
    w.set(3, 1.0)

    var out = Tensor[DType.float32](Shape(n))
    simd_rmsnorm(out, 0, x, 0, w, 0, n)

    # Manual: rms = sqrt((1+4+9+16)/4) = sqrt(7.5) ≈ 2.7386
    # normalized: [1/2.7386, 2/2.7386, 3/2.7386, 4/2.7386]
    var rms = Float32(sqrt(Float64(7.5)))
    assert_near(out.get(0), 1.0 / rms, 0.01, "rmsnorm[0]")
    assert_near(out.get(1), 2.0 / rms, 0.01, "rmsnorm[1]")
    assert_near(out.get(2), 3.0 / rms, 0.01, "rmsnorm[2]")
    assert_near(out.get(3), 4.0 / rms, 0.01, "rmsnorm[3]")

    print("  simd_rmsnorm: PASS")


fn test_simd_rmsnorm_with_scale() raises:
    """Test RMSNorm with non-unit scale weights."""
    var n = 3
    var x = Tensor[DType.float32](Shape(n))
    x.set(0, 3.0)
    x.set(1, 4.0)
    x.set(2, 0.0)

    var w = Tensor[DType.float32](Shape(n))
    w.set(0, 2.0)
    w.set(1, 0.5)
    w.set(2, 1.0)

    var out = Tensor[DType.float32](Shape(n))
    simd_rmsnorm(out, 0, x, 0, w, 0, n)

    # rms = sqrt((9+16+0)/3) = sqrt(25/3) ≈ 2.8868
    var rms = Float32(sqrt(Float64(25.0 / 3.0)))
    assert_near(out.get(0), 2.0 * 3.0 / rms, 0.02, "scaled rmsnorm[0]")
    assert_near(out.get(1), 0.5 * 4.0 / rms, 0.02, "scaled rmsnorm[1]")

    print("  simd_rmsnorm_with_scale: PASS")


fn test_simd_softmax() raises:
    """Test SIMD softmax produces valid probability distribution."""
    var n = 4
    var x = Tensor[DType.float32](Shape(n))
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 3.0)
    x.set(3, 4.0)

    var out = Tensor[DType.float32](Shape(n))
    simd_softmax(out, 0, x, 0, n)

    # Sum to 1
    var total = out.get(0) + out.get(1) + out.get(2) + out.get(3)
    assert_near(total, 1.0, 0.01, "softmax sums to 1")

    # Ordering preserved
    assert_true(out.get(3) > out.get(2), "p(3) > p(2)")
    assert_true(out.get(2) > out.get(1), "p(2) > p(1)")
    assert_true(out.get(1) > out.get(0), "p(1) > p(0)")

    print("  simd_softmax: PASS")


fn test_simd_softmax_stability() raises:
    """Test softmax numerical stability with large values."""
    var n = 3
    var x = Tensor[DType.float32](Shape(n))
    x.set(0, 1000.0)
    x.set(1, 1001.0)
    x.set(2, 1002.0)

    var out = Tensor[DType.float32](Shape(n))
    simd_softmax(out, 0, x, 0, n)

    var total = out.get(0) + out.get(1) + out.get(2)
    assert_near(total, 1.0, 0.01, "stable softmax")
    assert_true(out.get(0) > 0.0, "no underflow")

    print("  simd_softmax_stability: PASS")


fn test_simd_silu() raises:
    """Test SiLU activation."""
    var n = 4
    var x = Tensor[DType.float32](Shape(n))
    x.set(0, -2.0)
    x.set(1, 0.0)
    x.set(2, 1.0)
    x.set(3, 5.0)

    var out = Tensor[DType.float32](Shape(n))
    simd_silu(out, 0, x, 0, n)

    # silu(0) = 0
    assert_near(out.get(1), 0.0, 0.01, "silu(0) = 0")
    # silu(x) ≈ x for large positive x (sigmoid → 1)
    assert_near(out.get(3), 5.0 * (1.0 / (1.0 + Float32(exp(Float64(-5.0))))), 0.01, "silu(5)")
    # silu(x) ≈ 0 for large negative x
    assert_true(abs(out.get(0)) < 0.3, "silu(-2) near 0")

    print("  simd_silu: PASS")


fn test_simd_swiglu() raises:
    """Test fused SwiGLU: silu(gate) * up."""
    var n = 3
    var gate = Tensor[DType.float32](Shape(n))
    var up = Tensor[DType.float32](Shape(n))
    gate.set(0, 0.0)
    gate.set(1, 2.0)
    gate.set(2, -1.0)
    up.set(0, 5.0)
    up.set(1, 3.0)
    up.set(2, 2.0)

    var out = Tensor[DType.float32](Shape(n))
    simd_swiglu(out, 0, gate, 0, up, 0, n)

    # silu(0) * 5 = 0
    assert_near(out.get(0), 0.0, 0.01, "swiglu(0,5) = 0")
    # silu(2) * 3 ≈ 2*0.88 * 3 = 5.28
    var silu_2 = 2.0 * (1.0 / (1.0 + Float32(exp(Float64(-2.0)))))
    assert_near(out.get(1), silu_2 * 3.0, 0.01, "swiglu(2,3)")

    print("  simd_swiglu: PASS")


fn test_simd_axpy() raises:
    """Test BLAS-style axpy: y += alpha * x."""
    var n = 5
    var y = Tensor[DType.float32](Shape(n))
    var x = Tensor[DType.float32](Shape(n))
    for i in range(n):
        y.set(i, Float32(i))
        x.set(i, 1.0)

    simd_axpy(y, 0, x, 0, 10.0, n)

    # y[i] = i + 10*1 = i + 10
    for i in range(n):
        assert_near(y.get(i), Float32(i) + 10.0, 0.01, "axpy")

    print("  simd_axpy: PASS")


fn test_simd_axpy_large() raises:
    """Test axpy on large vector with SIMD tail handling."""
    var n = 259
    var y = Tensor[DType.float32](Shape(n))
    var x = Tensor[DType.float32](Shape(n))
    for i in range(n):
        y.set(i, 1.0)
        x.set(i, Float32(i) * 0.01)

    simd_axpy(y, 0, x, 0, 2.0, n)

    for i in range(n):
        var expected = 1.0 + 2.0 * Float32(i) * 0.01
        assert_near(y.get(i), expected, 0.001, "axpy large")

    print("  simd_axpy_large: PASS")


fn test_simd_dot_4096() raises:
    """Test dot product at transformer scale (4096-dim)."""
    var n = 4096
    var a = Tensor[DType.float32](Shape(n))
    var b = Tensor[DType.float32](Shape(n))
    for i in range(n):
        a.set(i, 0.01)
        b.set(i, 0.01)

    var result = simd_dot(a, 0, b, 0, n)
    # 4096 * 0.0001 = 0.4096
    assert_near(result, 0.4096, 0.001, "4096-dim dot")

    print("  simd_dot_4096: PASS")


fn test_simd_matvec_4096() raises:
    """Test matvec at transformer scale (32x4096)."""
    var rows = 32
    var cols = 4096
    var w = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))

    # All weights = 1/cols, all x = 1 → each output = 1.0
    var inv_cols = Float32(1.0) / Float32(cols)
    for i in range(rows * cols):
        w.set(i, inv_cols)
    for i in range(cols):
        x.set(i, 1.0)

    var out = Tensor[DType.float32](Shape(rows))
    simd_matvec(out, 0, w, 0, x, 0, rows, cols)

    for i in range(rows):
        assert_near(out.get(i), 1.0, 0.01, "4096-dim matvec row")

    print("  simd_matvec_4096: PASS")


fn test_par_simd_matvec() raises:
    """Test parallel matvec matches sequential."""
    var rows = 256
    var cols = 512
    var w = Tensor[DType.float32](Shape(rows * cols))
    var x = Tensor[DType.float32](Shape(cols))

    for i in range(rows * cols):
        w.set(i, Float32(i % 11) * 0.01 - 0.05)
    for i in range(cols):
        x.set(i, Float32(i % 7) * 0.1 - 0.3)

    var seq_out = Tensor[DType.float32](Shape(rows))
    var par_out = Tensor[DType.float32](Shape(rows))
    simd_matvec(seq_out, 0, w, 0, x, 0, rows, cols)
    par_simd_matvec(par_out, 0, w, 0, x, 0, rows, cols)

    for i in range(rows):
        assert_near(par_out.get(i), seq_out.get(i), 0.01, "par matches seq row " + String(i))

    print("  par_simd_matvec: PASS")


fn test_par_simd_matvec_small() raises:
    """Test parallel matvec falls back to sequential for small inputs."""
    var w = Tensor[DType.float32](Shape(12))
    w.set(0, 1.0)
    w.set(1, 2.0)
    w.set(2, 3.0)
    w.set(3, 4.0)
    w.set(4, 5.0)
    w.set(5, 6.0)
    w.set(6, 7.0)
    w.set(7, 8.0)
    w.set(8, 9.0)
    w.set(9, 10.0)
    w.set(10, 11.0)
    w.set(11, 12.0)

    var x = Tensor[DType.float32](Shape(3))
    x.set(0, 1.0)
    x.set(1, 1.0)
    x.set(2, 1.0)

    var out = Tensor[DType.float32](Shape(4))
    par_simd_matvec(out, 0, w, 0, x, 0, 4, 3)

    assert_near(out.get(0), 6.0, 0.01, "row 0")
    assert_near(out.get(1), 15.0, 0.01, "row 1")
    assert_near(out.get(2), 24.0, 0.01, "row 2")
    assert_near(out.get(3), 33.0, 0.01, "row 3")

    print("  par_simd_matvec_small: PASS")


fn main() raises:
    print("test_simd_math:")

    test_simd_dot_basic()
    test_simd_dot_large()
    test_simd_dot_with_offset()
    test_simd_matvec_basic()
    test_simd_matvec_large()
    test_simd_matvec_with_offset()
    test_simd_rmsnorm()
    test_simd_rmsnorm_with_scale()
    test_simd_softmax()
    test_simd_softmax_stability()
    test_simd_silu()
    test_simd_swiglu()
    test_simd_axpy()
    test_simd_axpy_large()
    test_simd_dot_4096()
    test_simd_matvec_4096()
    test_par_simd_matvec()
    test_par_simd_matvec_small()

    print("ALL PASSED")
