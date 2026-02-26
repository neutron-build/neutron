# ===----------------------------------------------------------------------=== #
# Test — Sprint 62: Norm Backward + requires_grad Gate
# ===----------------------------------------------------------------------=== #

"""Tests for RMSNorm/LayerNorm backward and requires_grad gating."""

from math import abs, sqrt
from testing import assert_true

from neutron_mojo.autograd.tape import Tape
from neutron_mojo.autograd.backward import run_backward
from neutron_mojo.train.modules import RMSNormModule, LayerNormModule, Linear
from neutron_mojo.train.losses import mse_loss


fn _make_var(mut tape: Tape, vals: List[Float32], requires_grad: Bool = True) -> Int:
    var dims = List[Int]()
    dims.append(len(vals))
    var idx = tape.add_variable(dims^, requires_grad=requires_grad)
    for i in range(len(vals)):
        tape.set_data(idx, i, vals[i])
    return idx


fn test_rmsnorm_gamma_gets_grad() raises:
    """RMSNorm gamma should receive non-zero gradients."""
    var tape = Tape(4096)
    var norm = RMSNormModule(4)
    norm.register(tape)

    var x_vals = List[Float32]()
    x_vals.append(1.0)
    x_vals.append(2.0)
    x_vals.append(3.0)
    x_vals.append(4.0)
    var x = _make_var(tape, x_vals)

    var y = norm.forward(tape, x)

    var tgt_vals = List[Float32]()
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    var gamma_grad_sum = Float64(0.0)
    for i in range(4):
        gamma_grad_sum += abs(Float64(tape.get_grad(norm.gamma_idx, i)))
    assert_true(gamma_grad_sum > 0.001, "RMSNorm gamma should get gradients")
    print("PASS: test_rmsnorm_gamma_gets_grad")


fn test_rmsnorm_x_gets_grad() raises:
    """RMSNorm input x should receive proper gradients."""
    var tape = Tape(4096)
    var norm = RMSNormModule(4)
    norm.register(tape)

    var x_vals = List[Float32]()
    x_vals.append(1.0)
    x_vals.append(-1.0)
    x_vals.append(2.0)
    x_vals.append(-2.0)
    var x = _make_var(tape, x_vals)

    var y = norm.forward(tape, x)
    var tgt_vals = List[Float32]()
    tgt_vals.append(0.5)
    tgt_vals.append(0.5)
    tgt_vals.append(0.5)
    tgt_vals.append(0.5)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    var x_grad_sum = Float64(0.0)
    for i in range(4):
        x_grad_sum += abs(Float64(tape.get_grad(x, i)))
    assert_true(x_grad_sum > 0.001, "RMSNorm x should get gradients")
    print("PASS: test_rmsnorm_x_gets_grad")


fn test_rmsnorm_finite_diff() raises:
    """RMSNorm backward matches finite differences for x."""
    var eps = Float64(1e-3)
    var x_vals = List[Float32]()
    x_vals.append(1.0)
    x_vals.append(2.0)
    x_vals.append(3.0)
    var tgt_vals = List[Float32]()
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)

    # Analytical gradient
    var tape = Tape(8192)
    var norm = RMSNormModule(3)
    norm.register(tape)
    var x = _make_var(tape, x_vals)
    var y = norm.forward(tape, x)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    # Finite diff for x[0]
    var tape_plus = Tape(8192)
    var norm_p = RMSNormModule(3)
    norm_p.register(tape_plus)
    # Copy same gamma
    for i in range(3):
        tape_plus.set_data(norm_p.gamma_idx, i, tape.get_data(norm.gamma_idx, i))
    var xp = _make_var(tape_plus, x_vals)
    tape_plus.set_data(xp, 0, x_vals[0] + Float32(eps))
    var yp = norm_p.forward(tape_plus, xp)
    var tp = _make_var(tape_plus, tgt_vals)
    var lp = mse_loss(tape_plus, yp, tp)

    var tape_minus = Tape(8192)
    var norm_m = RMSNormModule(3)
    norm_m.register(tape_minus)
    for i in range(3):
        tape_minus.set_data(norm_m.gamma_idx, i, tape.get_data(norm.gamma_idx, i))
    var xm = _make_var(tape_minus, x_vals)
    tape_minus.set_data(xm, 0, x_vals[0] - Float32(eps))
    var ym = norm_m.forward(tape_minus, xm)
    var tm = _make_var(tape_minus, tgt_vals)
    var lm = mse_loss(tape_minus, ym, tm)

    var fd = (Float64(tape_plus.get_data(lp, 0)) - Float64(tape_minus.get_data(lm, 0))) / (2.0 * eps)
    var ag = Float64(tape.get_grad(x, 0))
    assert_true(abs(fd - ag) < 0.1, "RMSNorm x[0] finite diff (fd=" + String(fd) + " ag=" + String(ag) + ")")
    print("PASS: test_rmsnorm_finite_diff")


fn test_layernorm_gamma_gets_grad() raises:
    """LayerNorm gamma should receive non-zero gradients."""
    var tape = Tape(4096)
    var norm = LayerNormModule(4)
    norm.register(tape)

    var x_vals = List[Float32]()
    x_vals.append(1.0)
    x_vals.append(2.0)
    x_vals.append(3.0)
    x_vals.append(4.0)
    var x = _make_var(tape, x_vals)

    var y = norm.forward(tape, x)
    var tgt_vals = List[Float32]()
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    var gamma_grad_sum = Float64(0.0)
    for i in range(4):
        gamma_grad_sum += abs(Float64(tape.get_grad(norm.gamma_idx, i)))
    assert_true(gamma_grad_sum > 0.001, "LayerNorm gamma should get gradients")
    print("PASS: test_layernorm_gamma_gets_grad")


fn test_layernorm_beta_gets_grad() raises:
    """LayerNorm beta should receive non-zero gradients."""
    var tape = Tape(4096)
    var norm = LayerNormModule(4)
    norm.register(tape)

    var x_vals = List[Float32]()
    x_vals.append(1.0)
    x_vals.append(2.0)
    x_vals.append(3.0)
    x_vals.append(4.0)
    var x = _make_var(tape, x_vals)

    var y = norm.forward(tape, x)
    var tgt_vals = List[Float32]()
    tgt_vals.append(1.0)
    tgt_vals.append(1.0)
    tgt_vals.append(1.0)
    tgt_vals.append(1.0)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    var beta_grad_sum = Float64(0.0)
    for i in range(4):
        beta_grad_sum += abs(Float64(tape.get_grad(norm.beta_idx, i)))
    assert_true(beta_grad_sum > 0.001, "LayerNorm beta should get gradients")
    print("PASS: test_layernorm_beta_gets_grad")


fn test_layernorm_x_gets_grad() raises:
    """LayerNorm x should receive proper gradients."""
    var tape = Tape(4096)
    var norm = LayerNormModule(3)
    norm.register(tape)

    var x_vals = List[Float32]()
    x_vals.append(1.0)
    x_vals.append(3.0)
    x_vals.append(5.0)
    var x = _make_var(tape, x_vals)

    var y = norm.forward(tape, x)
    # Use non-zero asymmetric targets so upstream gradient isn't proportional
    # to the normalized output (which would make dx=0 by LayerNorm math)
    var tgt_vals = List[Float32]()
    tgt_vals.append(1.0)
    tgt_vals.append(-0.5)
    tgt_vals.append(0.3)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    var x_grad_sum = Float64(0.0)
    for i in range(3):
        x_grad_sum += abs(Float64(tape.get_grad(x, i)))
    assert_true(x_grad_sum > 0.001, "LayerNorm x should get gradients")
    print("PASS: test_layernorm_x_gets_grad")


fn test_requires_grad_false_no_gradient() raises:
    """Variables with requires_grad=False should not accumulate gradients."""
    var tape = Tape(4096)

    # Create two variables, one with requires_grad=False
    var a_vals = List[Float32]()
    a_vals.append(2.0)
    a_vals.append(3.0)
    var a = _make_var(tape, a_vals, requires_grad=False)

    var b_vals = List[Float32]()
    b_vals.append(4.0)
    b_vals.append(5.0)
    var b = _make_var(tape, b_vals, requires_grad=True)

    # c = a + b (loss through MSE)
    from neutron_mojo.autograd.ops import tracked_add
    var c = tracked_add(tape, a, b)

    var tgt_vals = List[Float32]()
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, c, t)
    run_backward(tape, loss)

    # a should have zero gradients (requires_grad=False)
    var a_grad_sum = Float64(0.0)
    for i in range(2):
        a_grad_sum += abs(Float64(tape.get_grad(a, i)))
    assert_true(a_grad_sum < 1e-6, "requires_grad=False variable should have zero gradient")

    # b should have non-zero gradients
    var b_grad_sum = Float64(0.0)
    for i in range(2):
        b_grad_sum += abs(Float64(tape.get_grad(b, i)))
    assert_true(b_grad_sum > 0.001, "requires_grad=True variable should have gradient")
    print("PASS: test_requires_grad_false_no_gradient")


fn test_requires_grad_mul() raises:
    """requires_grad gate works through multiplication."""
    var tape = Tape(4096)
    var a_vals = List[Float32]()
    a_vals.append(2.0)
    var a = _make_var(tape, a_vals, requires_grad=False)

    var b_vals = List[Float32]()
    b_vals.append(3.0)
    var b = _make_var(tape, b_vals, requires_grad=True)

    from neutron_mojo.autograd.ops import tracked_mul, tracked_sum
    var c = tracked_mul(tape, a, b)
    var loss = tracked_sum(tape, c)
    run_backward(tape, loss)

    assert_true(abs(Float64(tape.get_grad(a, 0))) < 1e-6, "frozen a has zero grad in mul")
    assert_true(abs(Float64(tape.get_grad(b, 0)) - 2.0) < 0.01, "b gets grad=a_val=2.0 in mul")
    print("PASS: test_requires_grad_mul")


fn test_requires_grad_matmul() raises:
    """requires_grad gate works through matmul."""
    var tape = Tape(4096)

    # Frozen weight W (2x2)
    var w_dims = List[Int]()
    w_dims.append(2)
    w_dims.append(2)
    var w = tape.add_variable(w_dims^, requires_grad=False)
    tape.set_data(w, 0, Float32(1.0))
    tape.set_data(w, 1, Float32(0.0))
    tape.set_data(w, 2, Float32(0.0))
    tape.set_data(w, 3, Float32(1.0))

    # Trainable input x (1x2)
    var x_vals = List[Float32]()
    x_vals.append(3.0)
    x_vals.append(4.0)
    var x = _make_var(tape, x_vals, requires_grad=True)

    from neutron_mojo.autograd.ops import tracked_matmul, tracked_sum
    var y = tracked_matmul(tape, x, w, 1, 2, 2)
    var loss = tracked_sum(tape, y)
    run_backward(tape, loss)

    # w should have zero gradients
    var w_grad_sum = Float64(0.0)
    for i in range(4):
        w_grad_sum += abs(Float64(tape.get_grad(w, i)))
    assert_true(w_grad_sum < 1e-6, "frozen W has zero grad in matmul")
    print("PASS: test_requires_grad_matmul")


fn test_rmsnorm_gamma_frozen() raises:
    """RMSNorm gamma with requires_grad=False gets no gradient."""
    var tape = Tape(4096)
    var norm = RMSNormModule(3)
    norm.register(tape)
    # Freeze gamma
    tape.var_requires_grad[norm.gamma_idx] = False

    var x_vals = List[Float32]()
    x_vals.append(1.0)
    x_vals.append(2.0)
    x_vals.append(3.0)
    var x = _make_var(tape, x_vals)

    var y = norm.forward(tape, x)
    var tgt_vals = List[Float32]()
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    var gamma_grad_sum = Float64(0.0)
    for i in range(3):
        gamma_grad_sum += abs(Float64(tape.get_grad(norm.gamma_idx, i)))
    assert_true(gamma_grad_sum < 1e-6, "Frozen RMSNorm gamma gets no gradient")
    print("PASS: test_rmsnorm_gamma_frozen")


fn test_layernorm_finite_diff() raises:
    """LayerNorm backward matches finite differences for x."""
    var eps = Float64(1e-3)
    var x_vals = List[Float32]()
    x_vals.append(2.0)
    x_vals.append(4.0)
    x_vals.append(6.0)
    var tgt_vals = List[Float32]()
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)

    var tape = Tape(8192)
    var norm = LayerNormModule(3)
    norm.register(tape)
    var x = _make_var(tape, x_vals)
    var y = norm.forward(tape, x)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    # Finite diff for x[1]
    var tape_plus = Tape(8192)
    var norm_p = LayerNormModule(3)
    norm_p.register(tape_plus)
    for i in range(3):
        tape_plus.set_data(norm_p.gamma_idx, i, tape.get_data(norm.gamma_idx, i))
        tape_plus.set_data(norm_p.beta_idx, i, tape.get_data(norm.beta_idx, i))
    var xp = _make_var(tape_plus, x_vals)
    tape_plus.set_data(xp, 1, x_vals[1] + Float32(eps))
    var yp = norm_p.forward(tape_plus, xp)
    var tp = _make_var(tape_plus, tgt_vals)
    var lp = mse_loss(tape_plus, yp, tp)

    var tape_minus = Tape(8192)
    var norm_m = LayerNormModule(3)
    norm_m.register(tape_minus)
    for i in range(3):
        tape_minus.set_data(norm_m.gamma_idx, i, tape.get_data(norm.gamma_idx, i))
        tape_minus.set_data(norm_m.beta_idx, i, tape.get_data(norm.beta_idx, i))
    var xm = _make_var(tape_minus, x_vals)
    tape_minus.set_data(xm, 1, x_vals[1] - Float32(eps))
    var ym = norm_m.forward(tape_minus, xm)
    var tm = _make_var(tape_minus, tgt_vals)
    var lm = mse_loss(tape_minus, ym, tm)

    var fd = (Float64(tape_plus.get_data(lp, 0)) - Float64(tape_minus.get_data(lm, 0))) / (2.0 * eps)
    var ag = Float64(tape.get_grad(x, 1))
    assert_true(abs(fd - ag) < 0.15, "LayerNorm x[1] finite diff (fd=" + String(fd) + " ag=" + String(ag) + ")")
    print("PASS: test_layernorm_finite_diff")


fn test_requires_grad_linear_frozen_weight() raises:
    """Linear layer with frozen weights: only bias gets gradients."""
    var tape = Tape(4096)
    var lin = Linear(3, 2, has_bias=True)
    lin.register(tape)
    # Freeze the weight
    tape.var_requires_grad[lin.weight_idx] = False

    var x_vals = List[Float32]()
    x_vals.append(1.0)
    x_vals.append(2.0)
    x_vals.append(3.0)
    var x = _make_var(tape, x_vals)

    var y = lin.forward(tape, x)
    var tgt_vals = List[Float32]()
    tgt_vals.append(0.0)
    tgt_vals.append(0.0)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, y, t)
    run_backward(tape, loss)

    var w_grad_sum = Float64(0.0)
    for i in range(6):
        w_grad_sum += abs(Float64(tape.get_grad(lin.weight_idx, i)))
    assert_true(w_grad_sum < 1e-6, "Frozen weight gets no gradient")

    var b_grad_sum = Float64(0.0)
    for i in range(2):
        b_grad_sum += abs(Float64(tape.get_grad(lin.bias_idx, i)))
    assert_true(b_grad_sum > 0.001, "Unfrozen bias gets gradient")
    print("PASS: test_requires_grad_linear_frozen_weight")


fn test_multiple_frozen_in_chain() raises:
    """Multiple frozen variables in a computation chain."""
    var tape = Tape(4096)
    var a_vals = List[Float32]()
    a_vals.append(1.0)
    a_vals.append(2.0)
    var a = _make_var(tape, a_vals, requires_grad=False)

    var b_vals = List[Float32]()
    b_vals.append(3.0)
    b_vals.append(4.0)
    var b = _make_var(tape, b_vals, requires_grad=False)

    var c_vals = List[Float32]()
    c_vals.append(1.0)
    c_vals.append(1.0)
    var c = _make_var(tape, c_vals, requires_grad=True)

    from neutron_mojo.autograd.ops import tracked_add, tracked_mul, tracked_sum
    var ab = tracked_add(tape, a, b)
    var abc = tracked_mul(tape, ab, c)
    var loss = tracked_sum(tape, abc)
    run_backward(tape, loss)

    assert_true(abs(Float64(tape.get_grad(a, 0))) < 1e-6, "frozen a")
    assert_true(abs(Float64(tape.get_grad(b, 0))) < 1e-6, "frozen b")
    # c grad should be a+b = [4, 6]
    assert_true(abs(Float64(tape.get_grad(c, 0)) - 4.0) < 0.01, "c grad elem 0")
    assert_true(abs(Float64(tape.get_grad(c, 1)) - 6.0) < 0.01, "c grad elem 1")
    print("PASS: test_multiple_frozen_in_chain")


fn main() raises:
    print("=== Sprint 62: Norm Backward + requires_grad Tests ===")
    test_rmsnorm_gamma_gets_grad()
    test_rmsnorm_x_gets_grad()
    test_rmsnorm_finite_diff()
    test_layernorm_gamma_gets_grad()
    test_layernorm_beta_gets_grad()
    test_layernorm_x_gets_grad()
    test_requires_grad_false_no_gradient()
    test_requires_grad_mul()
    test_requires_grad_matmul()
    test_rmsnorm_gamma_frozen()
    test_layernorm_finite_diff()
    test_requires_grad_linear_frozen_weight()
    test_multiple_frozen_in_chain()
    print("")
    print("All 13 norm backward + requires_grad tests passed!")
