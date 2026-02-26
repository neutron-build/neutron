# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Autograd core tests
# ===----------------------------------------------------------------------=== #

"""Tests for autograd tape, variable, tracked ops, and backward pass."""

from math import exp, log, sqrt, tanh
from neutron_mojo.autograd import (
    Tape, Variable, TapeEntry, run_backward,
    tracked_add, tracked_sub, tracked_mul, tracked_matmul,
    tracked_relu, tracked_sigmoid, tracked_tanh,
    tracked_exp, tracked_log, tracked_neg,
    tracked_scalar_mul, tracked_scalar_add,
    tracked_softmax, tracked_sum, tracked_mean,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-4, atol: Float64 = 1e-5) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error(
            "Values not close: " + String(a) + " vs " + String(b) + " (diff=" + String(diff) + ")"
        )


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


# ===----------------------------------------------------------------------=== #
# Tape basic tests
# ===----------------------------------------------------------------------=== #


fn test_tape_add_variable() raises:
    """Add variables and check metadata."""
    var tape = Tape(1024)
    var dims = List[Int]()
    dims.append(3)
    var idx = tape.add_variable(dims^)
    assert_eq(idx, 0)
    assert_eq(tape.num_variables(), 1)
    assert_eq(tape.var_numel(0), 3)
    print("  tape_add_variable: PASS")


fn test_tape_data_access() raises:
    """Set and get data values."""
    var tape = Tape(1024)
    var dims = List[Int]()
    dims.append(4)
    var idx = tape.add_variable(dims^)
    tape.set_data(idx, 0, Float32(1.0))
    tape.set_data(idx, 1, Float32(2.0))
    tape.set_data(idx, 2, Float32(3.0))
    tape.set_data(idx, 3, Float32(4.0))
    assert_close(tape.get_data(idx, 0), 1.0)
    assert_close(tape.get_data(idx, 3), 4.0)
    print("  tape_data_access: PASS")


fn test_tape_grad_access() raises:
    """Set and get gradient values."""
    var tape = Tape(1024)
    var dims = List[Int]()
    dims.append(3)
    var idx = tape.add_variable(dims^)
    tape.set_grad(idx, 0, Float32(0.5))
    tape.accumulate_grad(idx, 0, Float32(0.3))
    assert_close(tape.get_grad(idx, 0), 0.8, atol=1e-6)
    print("  tape_grad_access: PASS")


fn test_tape_zero_grads() raises:
    """Zero all gradients."""
    var tape = Tape(1024)
    var dims = List[Int]()
    dims.append(3)
    var idx = tape.add_variable(dims^)
    tape.set_grad(idx, 0, Float32(1.0))
    tape.set_grad(idx, 1, Float32(2.0))
    tape.zero_all_grads()
    assert_close(tape.get_grad(idx, 0), 0.0)
    assert_close(tape.get_grad(idx, 1), 0.0)
    print("  tape_zero_grads: PASS")


fn test_tape_multiple_vars() raises:
    """Multiple variables with correct offsets."""
    var tape = Tape(1024)
    var d1 = List[Int]()
    d1.append(3)
    var d2 = List[Int]()
    d2.append(4)
    var idx1 = tape.add_variable(d1^)
    var idx2 = tape.add_variable(d2^)
    assert_eq(idx1, 0)
    assert_eq(idx2, 1)
    assert_eq(tape.var_offset(0), 0)
    assert_eq(tape.var_offset(1), 3)
    # Set data in second variable
    tape.set_data(idx2, 0, Float32(10.0))
    assert_close(tape.get_data(idx2, 0), 10.0)
    print("  tape_multiple_vars: PASS")


fn test_tape_capacity_growth() raises:
    """Tape grows capacity when needed."""
    var tape = Tape(8)  # very small
    var d1 = List[Int]()
    d1.append(4)
    var idx1 = tape.add_variable(d1^)
    tape.set_data(idx1, 0, Float32(1.0))
    # Add more than initial capacity
    var d2 = List[Int]()
    d2.append(8)
    var idx2 = tape.add_variable(d2^)
    tape.set_data(idx2, 0, Float32(99.0))
    assert_close(tape.get_data(idx1, 0), 1.0)
    assert_close(tape.get_data(idx2, 0), 99.0)
    print("  tape_capacity_growth: PASS")


# ===----------------------------------------------------------------------=== #
# Tracked forward + backward tests
# ===----------------------------------------------------------------------=== #


fn test_backward_add() raises:
    """grad(a+b) = (1, 1)."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(3)
    var a_idx = tape.add_variable(d.copy())
    var b_idx = tape.add_variable(d.copy())
    tape.set_data(a_idx, 0, Float32(1.0))
    tape.set_data(a_idx, 1, Float32(2.0))
    tape.set_data(a_idx, 2, Float32(3.0))
    tape.set_data(b_idx, 0, Float32(4.0))
    tape.set_data(b_idx, 1, Float32(5.0))
    tape.set_data(b_idx, 2, Float32(6.0))

    var c_idx = tracked_add(tape, a_idx, b_idx)
    # sum to get scalar loss
    var loss_idx = tracked_sum(tape, c_idx)

    run_backward(tape, loss_idx)
    assert_close(tape.get_grad(a_idx, 0), 1.0)
    assert_close(tape.get_grad(a_idx, 2), 1.0)
    assert_close(tape.get_grad(b_idx, 1), 1.0)
    print("  backward_add: PASS")


fn test_backward_mul() raises:
    """grad(a*b) = (b, a)."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(2)
    var a_idx = tape.add_variable(d.copy())
    var b_idx = tape.add_variable(d.copy())
    tape.set_data(a_idx, 0, Float32(3.0))
    tape.set_data(a_idx, 1, Float32(4.0))
    tape.set_data(b_idx, 0, Float32(5.0))
    tape.set_data(b_idx, 1, Float32(6.0))

    var c_idx = tracked_mul(tape, a_idx, b_idx)
    var loss_idx = tracked_sum(tape, c_idx)
    run_backward(tape, loss_idx)

    # grad_a = b, grad_b = a
    assert_close(tape.get_grad(a_idx, 0), 5.0)
    assert_close(tape.get_grad(a_idx, 1), 6.0)
    assert_close(tape.get_grad(b_idx, 0), 3.0)
    assert_close(tape.get_grad(b_idx, 1), 4.0)
    print("  backward_mul: PASS")


fn test_backward_matmul() raises:
    """Matmul backward: dA = dC @ B^T, dB = A^T @ dC."""
    var tape = Tape(1024)
    var da = List[Int]()
    da.append(2)
    da.append(3)
    var db = List[Int]()
    db.append(3)
    db.append(2)
    var a_idx = tape.add_variable(da^)
    var b_idx = tape.add_variable(db^)

    # A = [[1,2,3],[4,5,6]], B = [[1,0],[0,1],[1,1]]
    tape.set_data(a_idx, 0, Float32(1.0))
    tape.set_data(a_idx, 1, Float32(2.0))
    tape.set_data(a_idx, 2, Float32(3.0))
    tape.set_data(a_idx, 3, Float32(4.0))
    tape.set_data(a_idx, 4, Float32(5.0))
    tape.set_data(a_idx, 5, Float32(6.0))

    tape.set_data(b_idx, 0, Float32(1.0))
    tape.set_data(b_idx, 1, Float32(0.0))
    tape.set_data(b_idx, 2, Float32(0.0))
    tape.set_data(b_idx, 3, Float32(1.0))
    tape.set_data(b_idx, 4, Float32(1.0))
    tape.set_data(b_idx, 5, Float32(1.0))

    var c_idx = tracked_matmul(tape, a_idx, b_idx, 2, 3, 2)
    var loss_idx = tracked_sum(tape, c_idx)
    run_backward(tape, loss_idx)

    # Verify forward: C = [[1+0+3, 0+2+3], [4+0+6, 0+5+6]] = [[4, 5], [10, 11]]
    assert_close(tape.get_data(c_idx, 0), 4.0)
    assert_close(tape.get_data(c_idx, 1), 5.0)
    assert_close(tape.get_data(c_idx, 2), 10.0)
    assert_close(tape.get_data(c_idx, 3), 11.0)

    # Gradients exist and are non-zero
    var ga0 = tape.get_grad(a_idx, 0)
    if abs(Float64(ga0)) < 1e-10:
        raise Error("Expected non-zero gradient for a")
    print("  backward_matmul: PASS")


fn test_backward_relu() raises:
    """ReLU grad: 1 for x>0, 0 for x<=0."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(4)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(-2.0))
    tape.set_data(x_idx, 2, Float32(3.0))
    tape.set_data(x_idx, 3, Float32(0.0))

    var y_idx = tracked_relu(tape, x_idx)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(x_idx, 0), 1.0)
    assert_close(tape.get_grad(x_idx, 1), 0.0)
    assert_close(tape.get_grad(x_idx, 2), 1.0)
    assert_close(tape.get_grad(x_idx, 3), 0.0)
    print("  backward_relu: PASS")


fn test_backward_sigmoid() raises:
    """Sigmoid grad: s*(1-s)."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(1)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(0.0))  # sigmoid(0) = 0.5

    var y_idx = tracked_sigmoid(tape, x_idx)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    # sigmoid(0) = 0.5, grad = 0.5 * 0.5 = 0.25
    assert_close(tape.get_grad(x_idx, 0), 0.25, atol=1e-4)
    print("  backward_sigmoid: PASS")


fn test_backward_tanh() raises:
    """Tanh grad: 1 - tanh(x)^2."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(1)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(0.0))  # tanh(0) = 0

    var y_idx = tracked_tanh(tape, x_idx)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    # tanh(0) = 0, grad = 1 - 0 = 1
    assert_close(tape.get_grad(x_idx, 0), 1.0, atol=1e-4)
    print("  backward_tanh: PASS")


fn test_backward_exp() raises:
    """Exp grad: exp(x)."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(1)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(1.0))

    var y_idx = tracked_exp(tape, x_idx)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(x_idx, 0), Float32(exp(Float64(1.0))), rtol=1e-4)
    print("  backward_exp: PASS")


fn test_backward_log() raises:
    """Log grad: 1/x."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(1)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(2.0))

    var y_idx = tracked_log(tape, x_idx)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(x_idx, 0), 0.5, atol=1e-4)
    print("  backward_log: PASS")


fn test_backward_chain() raises:
    """Chain rule: y = relu(a*b + c), loss = sum(y)."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(3)
    var a_idx = tape.add_variable(d.copy())
    var b_idx = tape.add_variable(d.copy())
    var c_idx = tape.add_variable(d.copy())
    tape.set_data(a_idx, 0, Float32(1.0))
    tape.set_data(a_idx, 1, Float32(2.0))
    tape.set_data(a_idx, 2, Float32(-1.0))
    tape.set_data(b_idx, 0, Float32(2.0))
    tape.set_data(b_idx, 1, Float32(3.0))
    tape.set_data(b_idx, 2, Float32(4.0))
    tape.set_data(c_idx, 0, Float32(0.5))
    tape.set_data(c_idx, 1, Float32(-10.0))
    tape.set_data(c_idx, 2, Float32(1.0))

    var ab_idx = tracked_mul(tape, a_idx, b_idx)
    var sum_idx = tracked_add(tape, ab_idx, c_idx)
    var y_idx = tracked_relu(tape, sum_idx)
    var loss_idx = tracked_sum(tape, y_idx)

    run_backward(tape, loss_idx)

    # a*b = [2, 6, -4], a*b+c = [2.5, -4, -3], relu = [2.5, 0, 0]
    # relu pass-through for [2.5]: grad_sum[0]=1, grad_sum[1]=0, grad_sum[2]=0
    # grad_a[0] = b[0] * 1 = 2.0, grad_a[1] = 0 (relu killed it)
    assert_close(tape.get_grad(a_idx, 0), 2.0, atol=1e-4)
    assert_close(tape.get_grad(a_idx, 1), 0.0, atol=1e-4)
    assert_close(tape.get_grad(c_idx, 0), 1.0, atol=1e-4)
    assert_close(tape.get_grad(c_idx, 1), 0.0, atol=1e-4)
    print("  backward_chain: PASS")


fn test_backward_scalar_mul() raises:
    """Scalar mul grad: scalar."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(3)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))
    tape.set_data(x_idx, 2, Float32(3.0))

    var y_idx = tracked_scalar_mul(tape, x_idx, 2.5)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(x_idx, 0), 2.5, atol=1e-4)
    assert_close(tape.get_grad(x_idx, 2), 2.5, atol=1e-4)
    print("  backward_scalar_mul: PASS")


fn test_backward_mean() raises:
    """Mean grad: 1/n."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(4)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))
    tape.set_data(x_idx, 2, Float32(3.0))
    tape.set_data(x_idx, 3, Float32(4.0))

    var loss_idx = tracked_mean(tape, x_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(x_idx, 0), 0.25, atol=1e-4)
    assert_close(tape.get_grad(x_idx, 3), 0.25, atol=1e-4)
    print("  backward_mean: PASS")


fn test_backward_softmax() raises:
    """Softmax backward."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(3)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))
    tape.set_data(x_idx, 2, Float32(3.0))

    var s_idx = tracked_softmax(tape, x_idx)
    var loss_idx = tracked_sum(tape, s_idx)
    run_backward(tape, loss_idx)

    # Sum of softmax = 1, so d/dx sum(softmax(x)) = 0 for all x
    # (since softmax sums to 1 regardless of input)
    assert_close(tape.get_grad(x_idx, 0), 0.0, atol=1e-4)
    assert_close(tape.get_grad(x_idx, 1), 0.0, atol=1e-4)
    assert_close(tape.get_grad(x_idx, 2), 0.0, atol=1e-4)
    print("  backward_softmax: PASS")


fn test_backward_sub() raises:
    """Sub grad: (1, -1)."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(2)
    var a_idx = tape.add_variable(d.copy())
    var b_idx = tape.add_variable(d.copy())
    tape.set_data(a_idx, 0, Float32(5.0))
    tape.set_data(a_idx, 1, Float32(3.0))
    tape.set_data(b_idx, 0, Float32(2.0))
    tape.set_data(b_idx, 1, Float32(1.0))

    var c_idx = tracked_sub(tape, a_idx, b_idx)
    var loss_idx = tracked_sum(tape, c_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(a_idx, 0), 1.0)
    assert_close(tape.get_grad(b_idx, 0), -1.0)
    print("  backward_sub: PASS")


fn test_backward_neg() raises:
    """Neg grad: -1."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(2)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(3.0))
    tape.set_data(x_idx, 1, Float32(-2.0))

    var y_idx = tracked_neg(tape, x_idx)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(x_idx, 0), -1.0)
    assert_close(tape.get_grad(x_idx, 1), -1.0)
    print("  backward_neg: PASS")


fn test_backward_scalar_add() raises:
    """Scalar add grad: 1 (pass-through)."""
    var tape = Tape(1024)
    var d = List[Int]()
    d.append(2)
    var x_idx = tape.add_variable(d^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))

    var y_idx = tracked_scalar_add(tape, x_idx, 5.0)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    assert_close(tape.get_grad(x_idx, 0), 1.0)
    assert_close(tape.get_grad(x_idx, 1), 1.0)
    print("  backward_scalar_add: PASS")


fn main() raises:
    print("test_autograd:")

    # Tape basics
    test_tape_add_variable()
    test_tape_data_access()
    test_tape_grad_access()
    test_tape_zero_grads()
    test_tape_multiple_vars()
    test_tape_capacity_growth()

    # Individual backward ops
    test_backward_add()
    test_backward_sub()
    test_backward_mul()
    test_backward_matmul()
    test_backward_relu()
    test_backward_sigmoid()
    test_backward_tanh()
    test_backward_exp()
    test_backward_log()
    test_backward_neg()
    test_backward_scalar_mul()
    test_backward_scalar_add()
    test_backward_mean()
    test_backward_softmax()
    test_backward_chain()

    print("ALL PASSED (21 tests)")
