# ===----------------------------------------------------------------------=== #
# Tests for tensor/ops.mojo — arithmetic, matmul, activations, reductions
# ===----------------------------------------------------------------------=== #

"""Tests: add/sub/mul/div, broadcast ops, matmul vs known values, relu, softmax, reductions."""

from testing import assert_true, assert_equal
from math import abs

from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.ops import (
    add,
    sub,
    mul,
    div,
    matmul,
    relu,
    softmax,
    reduce_sum,
    reduce_max,
    reduce_mean,
    sum_all,
    max_all,
)


fn approx_equal(a: Float32, b: Float32, rtol: Float64 = 1e-5, atol: Float64 = 1e-6) -> Bool:
    """Check approximate equality with relative and absolute tolerance."""
    var diff = abs(Float64(a) - Float64(b))
    return diff <= atol + rtol * abs(Float64(b))


# ===----------------------------------------------------------------------=== #
# Elementwise ops
# ===----------------------------------------------------------------------=== #


fn test_add_same_shape() raises:
    var a = Tensor[DType.float32].full(Shape(4), Float32(2.0))
    var b = Tensor[DType.float32].full(Shape(4), Float32(3.0))
    var c = add(a, b)
    for i in range(4):
        assert_equal(c.data_ptr().load(i), Float32(5.0))
    print("  add_same_shape: PASS")


fn test_sub_same_shape() raises:
    var a = Tensor[DType.float32].full(Shape(4), Float32(5.0))
    var b = Tensor[DType.float32].full(Shape(4), Float32(2.0))
    var c = sub(a, b)
    for i in range(4):
        assert_equal(c.data_ptr().load(i), Float32(3.0))
    print("  sub_same_shape: PASS")


fn test_mul_same_shape() raises:
    var a = Tensor[DType.float32].full(Shape(4), Float32(3.0))
    var b = Tensor[DType.float32].full(Shape(4), Float32(4.0))
    var c = mul(a, b)
    for i in range(4):
        assert_equal(c.data_ptr().load(i), Float32(12.0))
    print("  mul_same_shape: PASS")


fn test_div_same_shape() raises:
    var a = Tensor[DType.float32].full(Shape(4), Float32(12.0))
    var b = Tensor[DType.float32].full(Shape(4), Float32(4.0))
    var c = div(a, b)
    for i in range(4):
        assert_equal(c.data_ptr().load(i), Float32(3.0))
    print("  div_same_shape: PASS")


fn test_add_broadcast() raises:
    """(3,) + (1,) = (3,) via broadcast."""
    var a = Tensor[DType.float32](3)
    a.data_ptr().store(0, Float32(1.0))
    a.data_ptr().store(1, Float32(2.0))
    a.data_ptr().store(2, Float32(3.0))

    var b = Tensor[DType.float32].full(Shape(1), Float32(10.0))
    var c = add(a, b)

    assert_equal(c.shape()[0], 3)
    assert_equal(c.data_ptr().load(0), Float32(11.0))
    assert_equal(c.data_ptr().load(1), Float32(12.0))
    assert_equal(c.data_ptr().load(2), Float32(13.0))
    print("  add_broadcast: PASS")


fn test_mul_broadcast_2d() raises:
    """(2, 3) * (1, 3) = (2, 3) via broadcast."""
    var a = Tensor[DType.float32](2, 3)
    for i in range(6):
        a.data_ptr().store(i, Float32(i + 1))  # [1,2,3,4,5,6]

    var b = Tensor[DType.float32](1, 3)
    b.data_ptr().store(0, Float32(10.0))
    b.data_ptr().store(1, Float32(20.0))
    b.data_ptr().store(2, Float32(30.0))

    var c = mul(a, b)
    assert_equal(c.shape()[0], 2)
    assert_equal(c.shape()[1], 3)
    assert_equal(c.data_ptr().load(0), Float32(10.0))   # 1 * 10
    assert_equal(c.data_ptr().load(1), Float32(40.0))   # 2 * 20
    assert_equal(c.data_ptr().load(2), Float32(90.0))   # 3 * 30
    assert_equal(c.data_ptr().load(3), Float32(40.0))   # 4 * 10
    assert_equal(c.data_ptr().load(4), Float32(100.0))  # 5 * 20
    assert_equal(c.data_ptr().load(5), Float32(180.0))  # 6 * 30
    print("  mul_broadcast_2d: PASS")


# ===----------------------------------------------------------------------=== #
# Matmul
# ===----------------------------------------------------------------------=== #


fn test_matmul_basic() raises:
    """2x3 @ 3x4 = 2x4 with known values from reference/matmul.py."""
    var a = Tensor[DType.float32](2, 3)
    # [[1, 2, 3], [4, 5, 6]]
    a.data_ptr().store(0, Float32(1))
    a.data_ptr().store(1, Float32(2))
    a.data_ptr().store(2, Float32(3))
    a.data_ptr().store(3, Float32(4))
    a.data_ptr().store(4, Float32(5))
    a.data_ptr().store(5, Float32(6))

    var b = Tensor[DType.float32](3, 4)
    # [[7, 8, 9, 10], [11, 12, 13, 14], [15, 16, 17, 18]]
    b.data_ptr().store(0, Float32(7))
    b.data_ptr().store(1, Float32(8))
    b.data_ptr().store(2, Float32(9))
    b.data_ptr().store(3, Float32(10))
    b.data_ptr().store(4, Float32(11))
    b.data_ptr().store(5, Float32(12))
    b.data_ptr().store(6, Float32(13))
    b.data_ptr().store(7, Float32(14))
    b.data_ptr().store(8, Float32(15))
    b.data_ptr().store(9, Float32(16))
    b.data_ptr().store(10, Float32(17))
    b.data_ptr().store(11, Float32(18))

    var c = matmul(a, b)
    assert_equal(c.shape()[0], 2)
    assert_equal(c.shape()[1], 4)

    # Expected: [[74, 80, 86, 92], [173, 188, 203, 218]]
    assert_equal(c.data_ptr().load(0), Float32(74))
    assert_equal(c.data_ptr().load(1), Float32(80))
    assert_equal(c.data_ptr().load(2), Float32(86))
    assert_equal(c.data_ptr().load(3), Float32(92))
    assert_equal(c.data_ptr().load(4), Float32(173))
    assert_equal(c.data_ptr().load(5), Float32(188))
    assert_equal(c.data_ptr().load(6), Float32(203))
    assert_equal(c.data_ptr().load(7), Float32(218))
    print("  matmul_basic: PASS")


fn test_matmul_identity() raises:
    """A @ I = A for a 4x4 matrix."""
    var a = Tensor[DType.float32](4, 4)
    for i in range(4):
        for j in range(4):
            a.data_ptr().store(i * 4 + j, Float32(i * 4 + j + 1))

    # Identity matrix
    var eye = Tensor[DType.float32](4, 4)
    for i in range(4):
        eye.data_ptr().store(i * 4 + i, Float32(1.0))

    var c = matmul(a, eye)
    for i in range(16):
        assert_true(approx_equal(c.data_ptr().load(i), a.data_ptr().load(i)))
    print("  matmul_identity: PASS")


fn test_matmul_dimension_mismatch() raises:
    """Matmul with incompatible inner dims should raise."""
    var a = Tensor[DType.float32](2, 3)
    var b = Tensor[DType.float32](4, 2)  # 3 != 4
    var raised = False
    try:
        _ = matmul(a, b)
    except:
        raised = True
    assert_true(raised)
    print("  matmul_dimension_mismatch: PASS")


# ===----------------------------------------------------------------------=== #
# Activations
# ===----------------------------------------------------------------------=== #


fn test_relu_basic() raises:
    var t = Tensor[DType.float32](5)
    t.data_ptr().store(0, Float32(-2))
    t.data_ptr().store(1, Float32(-1))
    t.data_ptr().store(2, Float32(0))
    t.data_ptr().store(3, Float32(1))
    t.data_ptr().store(4, Float32(2))

    var r = relu(t)
    assert_equal(r.data_ptr().load(0), Float32(0))
    assert_equal(r.data_ptr().load(1), Float32(0))
    assert_equal(r.data_ptr().load(2), Float32(0))
    assert_equal(r.data_ptr().load(3), Float32(1))
    assert_equal(r.data_ptr().load(4), Float32(2))
    print("  relu_basic: PASS")


fn test_relu_all_negative() raises:
    var t = Tensor[DType.float32].full(Shape(4), Float32(-5.0))
    var r = relu(t)
    for i in range(4):
        assert_equal(r.data_ptr().load(i), Float32(0))
    print("  relu_all_negative: PASS")


fn test_softmax_1d_basic() raises:
    """Softmax of [1, 2, 3] should sum to 1 and preserve ordering."""
    var t = Tensor[DType.float32](3)
    t.data_ptr().store(0, Float32(1.0))
    t.data_ptr().store(1, Float32(2.0))
    t.data_ptr().store(2, Float32(3.0))

    var s = softmax(t)
    var sp = s.data_ptr()
    # Sum should be ~1.0
    var total = sp.load(0) + sp.load(1) + sp.load(2)
    assert_true(approx_equal(total, Float32(1.0), atol=1e-5))
    # Ordering preserved
    assert_true(sp.load(2) > sp.load(1))
    assert_true(sp.load(1) > sp.load(0))
    _ = s.numel()  # keepalive
    print("  softmax_1d_basic: PASS")


fn test_softmax_stability() raises:
    """softmax([1000, 1001, 1002]) should produce finite results."""
    var t = Tensor[DType.float32](3)
    t.data_ptr().store(0, Float32(1000.0))
    t.data_ptr().store(1, Float32(1001.0))
    t.data_ptr().store(2, Float32(1002.0))

    var s = softmax(t)
    var ptr = s.data_ptr()
    for i in range(3):
        var v = ptr.load(i)
        # Check finite (not NaN or Inf)
        assert_true(v == v)  # NaN != NaN
        assert_true(v < Float32(1e10))  # not Inf

    var total = ptr.load(0) + ptr.load(1) + ptr.load(2)
    assert_true(approx_equal(total, Float32(1.0), atol=1e-5))
    _ = s.numel()  # keepalive
    print("  softmax_stability: PASS")


fn test_softmax_2d() raises:
    """Softmax along last axis of a 2x4 tensor."""
    var t = Tensor[DType.float32](2, 4)
    for i in range(8):
        t.data_ptr().store(i, Float32(i))

    var s = softmax(t, axis=-1)
    var ptr = s.data_ptr()

    # Each row should sum to 1
    var row0_sum = ptr.load(0) + ptr.load(1) + ptr.load(2) + ptr.load(3)
    var row1_sum = ptr.load(4) + ptr.load(5) + ptr.load(6) + ptr.load(7)
    assert_true(approx_equal(row0_sum, Float32(1.0), atol=1e-5))
    assert_true(approx_equal(row1_sum, Float32(1.0), atol=1e-5))
    _ = s.numel()  # keepalive
    print("  softmax_2d: PASS")


fn test_softmax_uniform() raises:
    """Uniform input -> uniform output."""
    var t = Tensor[DType.float32].full(Shape(5), Float32(5.0))
    var s = softmax(t)
    var sp = s.data_ptr()
    for i in range(5):
        assert_true(approx_equal(sp.load(i), Float32(0.2), atol=1e-5))
    _ = s.numel()  # keepalive
    print("  softmax_uniform: PASS")


# ===----------------------------------------------------------------------=== #
# Reductions
# ===----------------------------------------------------------------------=== #


fn test_reduce_sum_1d() raises:
    var t = Tensor[DType.float32](5)
    for i in range(5):
        t.data_ptr().store(i, Float32(i + 1))  # [1,2,3,4,5]

    var s = reduce_sum(t)
    assert_equal(s.data_ptr().load(0), Float32(15.0))
    _ = s.numel()  # keepalive
    print("  reduce_sum_1d: PASS")


fn test_reduce_sum_2d_last() raises:
    """Sum along last axis of (2, 3) tensor."""
    var t = Tensor[DType.float32](2, 3)
    # [[1, 2, 3], [4, 5, 6]]
    for i in range(6):
        t.data_ptr().store(i, Float32(i + 1))

    var s = reduce_sum(t, axis=-1)
    var sp = s.data_ptr()
    assert_equal(sp.load(0), Float32(6.0))   # 1+2+3
    assert_equal(sp.load(1), Float32(15.0))  # 4+5+6
    _ = s.numel()  # keepalive
    print("  reduce_sum_2d_last: PASS")


fn test_reduce_max_1d() raises:
    var t = Tensor[DType.float32](5)
    t.data_ptr().store(0, Float32(3.0))
    t.data_ptr().store(1, Float32(1.0))
    t.data_ptr().store(2, Float32(4.0))
    t.data_ptr().store(3, Float32(1.0))
    t.data_ptr().store(4, Float32(5.0))

    var m = reduce_max(t)
    assert_equal(m.data_ptr().load(0), Float32(5.0))
    _ = m.numel()  # keepalive
    print("  reduce_max_1d: PASS")


fn test_reduce_max_2d_last() raises:
    """Max along last axis of (2, 3) tensor."""
    var t = Tensor[DType.float32](2, 3)
    t.data_ptr().store(0, Float32(1.0))
    t.data_ptr().store(1, Float32(5.0))
    t.data_ptr().store(2, Float32(3.0))
    t.data_ptr().store(3, Float32(6.0))
    t.data_ptr().store(4, Float32(2.0))
    t.data_ptr().store(5, Float32(4.0))

    var m = reduce_max(t, axis=-1)
    var mp = m.data_ptr()
    assert_equal(mp.load(0), Float32(5.0))  # max(1,5,3)
    assert_equal(mp.load(1), Float32(6.0))  # max(6,2,4)
    _ = m.numel()  # keepalive
    print("  reduce_max_2d_last: PASS")


# --- Main ---


fn main() raises:
    print("test_ops:")

    # Elementwise
    test_add_same_shape()
    test_sub_same_shape()
    test_mul_same_shape()
    test_div_same_shape()
    test_add_broadcast()
    test_mul_broadcast_2d()

    # Matmul
    test_matmul_basic()
    test_matmul_identity()
    test_matmul_dimension_mismatch()

    # Activations
    test_relu_basic()
    test_relu_all_negative()
    test_softmax_1d_basic()
    test_softmax_stability()
    test_softmax_2d()
    test_softmax_uniform()

    # Reductions
    test_reduce_sum_1d()
    test_reduce_sum_2d_last()
    test_reduce_max_1d()
    test_reduce_max_2d_last()

    print("ALL PASSED")
