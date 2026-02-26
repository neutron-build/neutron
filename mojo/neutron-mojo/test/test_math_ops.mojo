# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Extended math ops tests
# ===----------------------------------------------------------------------=== #

"""Tests for extended math operations: neg, abs_val, exp_op, log_op, sqrt_op,
sigmoid, tanh_op, pow_scalar, clamp, scalar_mul, scalar_add."""

from math import exp, log, sqrt, tanh
from neutron_mojo.tensor import (
    Tensor, neg, abs_val, exp_op, log_op, sqrt_op,
    sigmoid, tanh_op, pow_scalar, clamp, scalar_mul, scalar_add,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-5, atol: Float64 = 1e-6) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error(
            "Values not close: " + String(a) + " vs " + String(b) + " (diff=" + String(diff) + ")"
        )


fn test_neg() raises:
    """Negate reverses sign."""
    var x = Tensor[DType.float32](4)
    x.set(0, 1.0)
    x.set(1, -2.0)
    x.set(2, 0.0)
    x.set(3, 3.5)
    var out = neg(x)
    assert_close(out.get(0), -1.0)
    assert_close(out.get(1), 2.0)
    assert_close(out.get(2), 0.0)
    assert_close(out.get(3), -3.5)
    print("  neg: PASS")


fn test_abs_val() raises:
    """Absolute value makes all elements non-negative."""
    var x = Tensor[DType.float32](4)
    x.set(0, -3.0)
    x.set(1, 2.0)
    x.set(2, 0.0)
    x.set(3, -0.5)
    var out = abs_val(x)
    assert_close(out.get(0), 3.0)
    assert_close(out.get(1), 2.0)
    assert_close(out.get(2), 0.0)
    assert_close(out.get(3), 0.5)
    print("  abs_val: PASS")


fn test_exp_op() raises:
    """Elementwise exp."""
    var x = Tensor[DType.float32](3)
    x.set(0, 0.0)
    x.set(1, 1.0)
    x.set(2, -1.0)
    var out = exp_op(x)
    assert_close(out.get(0), 1.0, atol=1e-5)
    assert_close(out.get(1), Float32(exp(Float64(1.0))), rtol=1e-5)
    assert_close(out.get(2), Float32(exp(Float64(-1.0))), rtol=1e-5)
    print("  exp_op: PASS")


fn test_log_op() raises:
    """Elementwise natural log."""
    var x = Tensor[DType.float32](3)
    x.set(0, 1.0)
    x.set(1, Float32(exp(Float64(1.0))))
    x.set(2, 10.0)
    var out = log_op(x)
    assert_close(out.get(0), 0.0, atol=1e-5)
    assert_close(out.get(1), 1.0, rtol=1e-4)
    assert_close(out.get(2), Float32(log(Float64(10.0))), rtol=1e-5)
    print("  log_op: PASS")


fn test_sqrt_op() raises:
    """Elementwise sqrt."""
    var x = Tensor[DType.float32](3)
    x.set(0, 4.0)
    x.set(1, 9.0)
    x.set(2, 1.0)
    var out = sqrt_op(x)
    assert_close(out.get(0), 2.0, atol=1e-5)
    assert_close(out.get(1), 3.0, atol=1e-5)
    assert_close(out.get(2), 1.0, atol=1e-5)
    print("  sqrt_op: PASS")


fn test_sigmoid() raises:
    """Sigmoid activation values."""
    var x = Tensor[DType.float32](3)
    x.set(0, 0.0)
    x.set(1, 10.0)
    x.set(2, -10.0)
    var out = sigmoid(x)
    assert_close(out.get(0), 0.5, atol=1e-5)
    assert_close(out.get(1), 1.0, atol=1e-3)
    assert_close(out.get(2), 0.0, atol=1e-3)
    print("  sigmoid: PASS")


fn test_tanh_op() raises:
    """Elementwise tanh."""
    var x = Tensor[DType.float32](3)
    x.set(0, 0.0)
    x.set(1, 10.0)
    x.set(2, -10.0)
    var out = tanh_op(x)
    assert_close(out.get(0), 0.0, atol=1e-5)
    assert_close(out.get(1), 1.0, atol=1e-3)
    assert_close(out.get(2), -1.0, atol=1e-3)
    print("  tanh_op: PASS")


fn test_pow_scalar() raises:
    """Raise elements to a power."""
    var x = Tensor[DType.float32](3)
    x.set(0, 2.0)
    x.set(1, 3.0)
    x.set(2, 4.0)
    var out = pow_scalar(x, 2.0)
    assert_close(out.get(0), 4.0, atol=1e-5)
    assert_close(out.get(1), 9.0, atol=1e-5)
    assert_close(out.get(2), 16.0, atol=1e-4)
    print("  pow_scalar: PASS")


fn test_clamp() raises:
    """Clamp elements to [min, max]."""
    var x = Tensor[DType.float32](5)
    x.set(0, -5.0)
    x.set(1, -1.0)
    x.set(2, 0.5)
    x.set(3, 1.0)
    x.set(4, 5.0)
    var out = clamp(x, -2.0, 2.0)
    assert_close(out.get(0), -2.0)
    assert_close(out.get(1), -1.0)
    assert_close(out.get(2), 0.5)
    assert_close(out.get(3), 1.0)
    assert_close(out.get(4), 2.0)
    print("  clamp: PASS")


fn test_scalar_mul() raises:
    """Multiply all elements by a scalar."""
    var x = Tensor[DType.float32](3)
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 3.0)
    var out = scalar_mul(x, 2.5)
    assert_close(out.get(0), 2.5)
    assert_close(out.get(1), 5.0)
    assert_close(out.get(2), 7.5)
    print("  scalar_mul: PASS")


fn test_scalar_add() raises:
    """Add a scalar to all elements."""
    var x = Tensor[DType.float32](3)
    x.set(0, 1.0)
    x.set(1, -1.0)
    x.set(2, 0.0)
    var out = scalar_add(x, 10.0)
    assert_close(out.get(0), 11.0)
    assert_close(out.get(1), 9.0)
    assert_close(out.get(2), 10.0)
    print("  scalar_add: PASS")


fn test_exp_log_roundtrip() raises:
    """exp(log(x)) ≈ x for positive x."""
    var x = Tensor[DType.float32](3)
    x.set(0, 1.0)
    x.set(1, 2.5)
    x.set(2, 0.1)
    var logged = log_op(x)
    var roundtrip = exp_op(logged)
    assert_close(roundtrip.get(0), 1.0, rtol=1e-5)
    assert_close(roundtrip.get(1), 2.5, rtol=1e-5)
    assert_close(roundtrip.get(2), 0.1, rtol=1e-4)
    print("  exp_log_roundtrip: PASS")


fn main() raises:
    print("test_math_ops:")
    test_neg()
    test_abs_val()
    test_exp_op()
    test_log_op()
    test_sqrt_op()
    test_sigmoid()
    test_tanh_op()
    test_pow_scalar()
    test_clamp()
    test_scalar_mul()
    test_scalar_add()
    test_exp_log_roundtrip()
    print("ALL PASSED (12 tests)")
