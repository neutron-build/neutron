# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Autograd extended ops tests
# ===----------------------------------------------------------------------=== #

"""Tests for extended autograd backward ops and gradient checking."""

from math import exp, sqrt, tanh
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.autograd import (
    Tape, run_backward, GradCheckResult, compare_gradients,
    tracked_add, tracked_mul, tracked_relu, tracked_sigmoid,
    tracked_exp, tracked_log, tracked_sum, tracked_mean,
    tracked_scalar_mul, tracked_neg, tracked_matmul,
)
from neutron_mojo.autograd.tape import (
    TapeEntry, OP_DIV, OP_POW, OP_SQRT, OP_CLAMP,
    OP_GELU, OP_SILU, OP_RESHAPE, OP_MSE,
)
from neutron_mojo.autograd.backward import run_backward


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-3, atol: Float64 = 1e-4) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error(
            "Values not close: " + String(a) + " vs " + String(b) + " (diff=" + String(diff) + ")"
        )


fn test_backward_div() raises:
    """Division backward: d/da(a/b) = 1/b, d/db(a/b) = -a/b^2."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(2)
    var a_idx = tape.add_variable(d.copy())
    var b_idx = tape.add_variable(d.copy())
    tape.set_data(a_idx, 0, Float32(6.0))
    tape.set_data(a_idx, 1, Float32(8.0))
    tape.set_data(b_idx, 0, Float32(2.0))
    tape.set_data(b_idx, 1, Float32(4.0))

    # Manual tracked div
    var c_dims = List[Int]()
    c_dims.append(2)
    var c_idx = tape.add_variable(c_dims^, requires_grad=True)
    var a_off = tape.var_offset(a_idx)
    var b_off = tape.var_offset(b_idx)
    var c_off = tape.var_offset(c_idx)
    var data = tape.data_flat.data_ptr()
    for i in range(2):
        data.store(c_off + i, data.load(a_off + i) / data.load(b_off + i))
    tape.record(TapeEntry(OP_DIV(), a_idx, b_idx, c_idx))

    var loss_idx = tracked_sum(tape, c_idx)
    run_backward(tape, loss_idx)

    # d/da[0] = 1/2 = 0.5
    assert_close(tape.get_grad(a_idx, 0), 0.5)
    # d/db[0] = -6/4 = -1.5
    assert_close(tape.get_grad(b_idx, 0), -1.5)
    print("  backward_div: PASS")


fn test_backward_pow() raises:
    """Power backward: d/dx(x^n) = n*x^(n-1)."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(2)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(3.0))
    tape.set_data(x_idx, 1, Float32(2.0))

    # Manual tracked pow
    from math import pow
    var y_dims = List[Int]()
    y_dims.append(2)
    var y_idx = tape.add_variable(y_dims^, requires_grad=True)
    var x_off = tape.var_offset(x_idx)
    var y_off = tape.var_offset(y_idx)
    var data = tape.data_flat.data_ptr()
    var exponent = 2.0
    for i in range(2):
        data.store(y_off + i, Float32(pow(Float64(data.load(x_off + i)), exponent)))
    tape.record(TapeEntry(OP_POW(), x_idx, -1, y_idx, cached_scalar=exponent))

    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    # d/dx(x^2) = 2x => grad[0] = 6, grad[1] = 4
    assert_close(tape.get_grad(x_idx, 0), 6.0)
    assert_close(tape.get_grad(x_idx, 1), 4.0)
    print("  backward_pow: PASS")


fn test_backward_sqrt() raises:
    """Sqrt backward: d/dx sqrt(x) = 0.5/sqrt(x)."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(2)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(4.0))
    tape.set_data(x_idx, 1, Float32(9.0))

    var y_dims = List[Int]()
    y_dims.append(2)
    var y_idx = tape.add_variable(y_dims^, requires_grad=True)
    var x_off = tape.var_offset(x_idx)
    var y_off = tape.var_offset(y_idx)
    var data = tape.data_flat.data_ptr()
    for i in range(2):
        data.store(y_off + i, Float32(sqrt(Float64(data.load(x_off + i)))))
    tape.record(TapeEntry(OP_SQRT(), x_idx, -1, y_idx))

    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    # d/dx sqrt(4) = 0.5/2 = 0.25
    assert_close(tape.get_grad(x_idx, 0), 0.25)
    # d/dx sqrt(9) = 0.5/3 ≈ 0.1667
    assert_close(tape.get_grad(x_idx, 1), Float32(1.0 / 6.0), rtol=1e-3)
    print("  backward_sqrt: PASS")


fn test_backward_clamp() raises:
    """Clamp backward: 1 if in range, 0 otherwise."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(4)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(-5.0))
    tape.set_data(x_idx, 1, Float32(0.5))
    tape.set_data(x_idx, 2, Float32(1.5))
    tape.set_data(x_idx, 3, Float32(5.0))

    var y_dims = List[Int]()
    y_dims.append(4)
    var y_idx = tape.add_variable(y_dims^, requires_grad=True)
    var x_off = tape.var_offset(x_idx)
    var y_off = tape.var_offset(y_idx)
    var data = tape.data_flat.data_ptr()
    var lo = Float32(-1.0)
    var hi = Float32(2.0)
    for i in range(4):
        var v = data.load(x_off + i)
        if v < lo:
            data.store(y_off + i, lo)
        elif v > hi:
            data.store(y_off + i, hi)
        else:
            data.store(y_off + i, v)
    tape.record(TapeEntry(OP_CLAMP(), x_idx, -1, y_idx, cached_scalar=-1.0, cached_scalar2=2.0))

    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(x_idx, 0), 0.0)  # clamped below
    assert_close(tape.get_grad(x_idx, 1), 1.0)  # in range
    assert_close(tape.get_grad(x_idx, 2), 1.0)  # in range
    assert_close(tape.get_grad(x_idx, 3), 0.0)  # clamped above
    print("  backward_clamp: PASS")


fn test_backward_gelu() raises:
    """GeLU backward via recorded op."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(3)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(0.0))
    tape.set_data(x_idx, 1, Float32(1.0))
    tape.set_data(x_idx, 2, Float32(-1.0))

    var pi = 3.14159265358979323846
    var sqrt_2_over_pi = sqrt(2.0 / pi)
    var coef = 0.044715

    var y_dims = List[Int]()
    y_dims.append(3)
    var y_idx = tape.add_variable(y_dims^, requires_grad=True)
    var x_off = tape.var_offset(x_idx)
    var y_off = tape.var_offset(y_idx)
    var data = tape.data_flat.data_ptr()
    for i in range(3):
        var x = Float64(data.load(x_off + i))
        var x3 = x * x * x
        var inner = sqrt_2_over_pi * (x + coef * x3)
        var out = 0.5 * x * (1.0 + tanh(inner))
        data.store(y_off + i, Float32(out))
    tape.record(TapeEntry(OP_GELU(), x_idx, -1, y_idx))

    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    # GeLU'(0) ≈ 0.5
    assert_close(tape.get_grad(x_idx, 0), 0.5, atol=0.01)
    # GeLU'(1) ≈ 1.083
    var g1 = tape.get_grad(x_idx, 1)
    if abs(Float64(g1)) < 0.5:
        raise Error("GeLU grad at x=1 too small: " + String(g1))
    print("  backward_gelu: PASS")


fn test_backward_silu() raises:
    """SiLU backward: sigmoid(x) + x*sigmoid(x)*(1-sigmoid(x))."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(2)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(0.0))
    tape.set_data(x_idx, 1, Float32(1.0))

    var y_dims = List[Int]()
    y_dims.append(2)
    var y_idx = tape.add_variable(y_dims^, requires_grad=True)
    var x_off = tape.var_offset(x_idx)
    var y_off = tape.var_offset(y_idx)
    var data = tape.data_flat.data_ptr()
    for i in range(2):
        var x = Float64(data.load(x_off + i))
        var s = 1.0 / (1.0 + exp(-x))
        data.store(y_off + i, Float32(x * s))
    tape.record(TapeEntry(OP_SILU(), x_idx, -1, y_idx))

    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    # SiLU'(0) = 0.5 + 0*0.5*0.5 = 0.5
    assert_close(tape.get_grad(x_idx, 0), 0.5, atol=1e-3)
    print("  backward_silu: PASS")


fn test_backward_reshape() raises:
    """Reshape backward: pass-through."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(6)
    var x_idx = tape.add_variable(d^)
    for i in range(6):
        tape.set_data(x_idx, i, Float32(i + 1))

    var y_dims = List[Int]()
    y_dims.append(2)
    y_dims.append(3)
    var y_idx = tape.add_variable(y_dims^, requires_grad=True)
    var x_off = tape.var_offset(x_idx)
    var y_off = tape.var_offset(y_idx)
    var data = tape.data_flat.data_ptr()
    for i in range(6):
        data.store(y_off + i, data.load(x_off + i))
    tape.record(TapeEntry(OP_RESHAPE(), x_idx, -1, y_idx))

    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    for i in range(6):
        assert_close(tape.get_grad(x_idx, i), 1.0)
    print("  backward_reshape: PASS")


fn test_backward_mse() raises:
    """MSE backward: 2*(pred-target)/n."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(3)
    var pred_idx = tape.add_variable(d.copy())
    var target_idx = tape.add_variable(d.copy())
    tape.set_data(pred_idx, 0, Float32(1.0))
    tape.set_data(pred_idx, 1, Float32(2.0))
    tape.set_data(pred_idx, 2, Float32(3.0))
    tape.set_data(target_idx, 0, Float32(1.5))
    tape.set_data(target_idx, 1, Float32(2.5))
    tape.set_data(target_idx, 2, Float32(2.0))

    # Manual MSE forward
    var loss_dims = List[Int]()
    loss_dims.append(1)
    var loss_idx = tape.add_variable(loss_dims^, requires_grad=True)
    var mse = Float64(0.0)
    for i in range(3):
        var diff = Float64(tape.get_data(pred_idx, i)) - Float64(tape.get_data(target_idx, i))
        mse += diff * diff
    mse /= 3.0
    tape.set_data(loss_idx, 0, Float32(mse))
    tape.record(TapeEntry(OP_MSE(), pred_idx, target_idx, loss_idx))

    run_backward(tape, loss_idx)

    # grad_pred[0] = 2*(1.0-1.5)/3 = -1/3
    assert_close(tape.get_grad(pred_idx, 0), Float32(-1.0 / 3.0), rtol=1e-3)
    # grad_pred[2] = 2*(3.0-2.0)/3 = 2/3
    assert_close(tape.get_grad(pred_idx, 2), Float32(2.0 / 3.0), rtol=1e-3)
    print("  backward_mse: PASS")


fn test_grad_check_utility() raises:
    """GradCheckResult comparison."""
    var a = Tensor[DType.float32](3)
    a.set(0, 1.0)
    a.set(1, 2.0)
    a.set(2, 3.0)
    var b = Tensor[DType.float32](3)
    b.set(0, 1.001)
    b.set(1, 2.001)
    b.set(2, 3.001)
    var result = compare_gradients(a, b, rtol=1e-2, atol=1e-2)
    if not result.passed:
        raise Error("Expected grad check to pass for close values")
    print("  grad_check_utility: PASS")


fn test_grad_check_fail() raises:
    """GradCheckResult detects large differences."""
    var a = Tensor[DType.float32](3)
    a.set(0, 1.0)
    a.set(1, 2.0)
    a.set(2, 3.0)
    var b = Tensor[DType.float32](3)
    b.set(0, 10.0)
    b.set(1, 20.0)
    b.set(2, 30.0)
    var result = compare_gradients(a, b, rtol=1e-3, atol=1e-5)
    if result.passed:
        raise Error("Expected grad check to fail for very different values")
    print("  grad_check_fail: PASS")


fn test_numerical_grad_add() raises:
    """Numerical gradient verification for add."""
    var eps = 1e-4

    # Analytical
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(2)
    var a_idx = tape.add_variable(d.copy())
    var b_idx = tape.add_variable(d.copy())
    tape.set_data(a_idx, 0, Float32(1.0))
    tape.set_data(a_idx, 1, Float32(2.0))
    tape.set_data(b_idx, 0, Float32(3.0))
    tape.set_data(b_idx, 1, Float32(4.0))
    var c_idx = tracked_add(tape, a_idx, b_idx)
    var loss_idx = tracked_sum(tape, c_idx)
    run_backward(tape, loss_idx)
    var analytical = tape.get_grad_copy(a_idx)

    # Numerical (finite difference for a[0])
    var numerical = Tensor[DType.float32](2)
    for idx in range(2):
        # f(x+eps)
        var tape_p = Tape(4096)
        var ap = tape_p.add_variable(d.copy())
        var bp = tape_p.add_variable(d.copy())
        tape_p.set_data(ap, 0, Float32(1.0))
        tape_p.set_data(ap, 1, Float32(2.0))
        tape_p.set_data(bp, 0, Float32(3.0))
        tape_p.set_data(bp, 1, Float32(4.0))
        tape_p.set_data(ap, idx, tape_p.get_data(ap, idx) + Float32(eps))
        var cp = tracked_add(tape_p, ap, bp)
        var lp = tracked_sum(tape_p, cp)
        var fp = Float64(tape_p.get_data(lp, 0))

        # f(x-eps)
        var tape_m = Tape(4096)
        var am = tape_m.add_variable(d.copy())
        var bm = tape_m.add_variable(d.copy())
        tape_m.set_data(am, 0, Float32(1.0))
        tape_m.set_data(am, 1, Float32(2.0))
        tape_m.set_data(bm, 0, Float32(3.0))
        tape_m.set_data(bm, 1, Float32(4.0))
        tape_m.set_data(am, idx, tape_m.get_data(am, idx) - Float32(eps))
        var cm = tracked_add(tape_m, am, bm)
        var lm = tracked_sum(tape_m, cm)
        var fm = Float64(tape_m.get_data(lm, 0))

        numerical.set(idx, Float32((fp - fm) / (2.0 * eps)))

    var result = compare_gradients(analytical, numerical, rtol=1e-2, atol=2e-3)
    if not result.passed:
        raise Error("Numerical grad check failed for add: max_abs=" + String(result.max_abs_diff))
    print("  numerical_grad_add: PASS")


fn test_numerical_grad_mul() raises:
    """Numerical gradient verification for mul."""
    var eps = 1e-4
    var d = List[Int]()
    d.append(2)

    # Analytical
    var tape = Tape(4096)
    var a_idx = tape.add_variable(d.copy())
    var b_idx = tape.add_variable(d.copy())
    tape.set_data(a_idx, 0, Float32(2.0))
    tape.set_data(a_idx, 1, Float32(3.0))
    tape.set_data(b_idx, 0, Float32(4.0))
    tape.set_data(b_idx, 1, Float32(5.0))
    var c_idx = tracked_mul(tape, a_idx, b_idx)
    var loss_idx = tracked_sum(tape, c_idx)
    run_backward(tape, loss_idx)
    var analytical = tape.get_grad_copy(a_idx)

    # Numerical
    var numerical = Tensor[DType.float32](2)
    for idx in range(2):
        var tape_p = Tape(4096)
        var ap = tape_p.add_variable(d.copy())
        var bp = tape_p.add_variable(d.copy())
        tape_p.set_data(ap, 0, Float32(2.0))
        tape_p.set_data(ap, 1, Float32(3.0))
        tape_p.set_data(bp, 0, Float32(4.0))
        tape_p.set_data(bp, 1, Float32(5.0))
        tape_p.set_data(ap, idx, tape_p.get_data(ap, idx) + Float32(eps))
        var cp = tracked_mul(tape_p, ap, bp)
        var lp = tracked_sum(tape_p, cp)
        var fp = Float64(tape_p.get_data(lp, 0))

        var tape_m = Tape(4096)
        var am = tape_m.add_variable(d.copy())
        var bm = tape_m.add_variable(d.copy())
        tape_m.set_data(am, 0, Float32(2.0))
        tape_m.set_data(am, 1, Float32(3.0))
        tape_m.set_data(bm, 0, Float32(4.0))
        tape_m.set_data(bm, 1, Float32(5.0))
        tape_m.set_data(am, idx, tape_m.get_data(am, idx) - Float32(eps))
        var cm = tracked_mul(tape_m, am, bm)
        var lm = tracked_sum(tape_m, cm)
        var fm = Float64(tape_m.get_data(lm, 0))

        numerical.set(idx, Float32((fp - fm) / (2.0 * eps)))

    var result = compare_gradients(analytical, numerical, rtol=1e-2, atol=1e-2)
    if not result.passed:
        raise Error("Numerical grad check failed for mul: max_abs=" + String(result.max_abs_diff))
    print("  numerical_grad_mul: PASS")


fn main() raises:
    print("test_autograd_extended:")
    test_backward_div()
    test_backward_pow()
    test_backward_sqrt()
    test_backward_clamp()
    test_backward_gelu()
    test_backward_silu()
    test_backward_reshape()
    test_backward_mse()
    test_grad_check_utility()
    test_grad_check_fail()
    test_numerical_grad_add()
    test_numerical_grad_mul()
    print("ALL PASSED (12 tests)")
