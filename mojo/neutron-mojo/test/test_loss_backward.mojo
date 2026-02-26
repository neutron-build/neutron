# ===----------------------------------------------------------------------=== #
# Test — Sprint 61: Loss Backward Correctness
# ===----------------------------------------------------------------------=== #

"""Finite-difference gradient verification for L1, BCE, KL divergence losses."""

from math import abs, log
from testing import assert_true

from neutron_mojo.autograd.tape import Tape
from neutron_mojo.autograd.backward import run_backward
from neutron_mojo.train.losses import (
    l1_loss, binary_cross_entropy, kl_divergence, mse_loss,
)


fn _make_var(mut tape: Tape, vals: List[Float32]) -> Int:
    """Helper: create a tape variable from a list of values."""
    var dims = List[Int]()
    dims.append(len(vals))
    var idx = tape.add_variable(dims^, requires_grad=True)
    for i in range(len(vals)):
        tape.set_data(idx, i, vals[i])
    return idx


fn test_l1_loss_forward() raises:
    """L1 forward computes mean absolute error."""
    var tape = Tape(1024)
    var pred_vals = List[Float32]()
    pred_vals.append(1.0)
    pred_vals.append(2.0)
    pred_vals.append(3.0)
    var tgt_vals = List[Float32]()
    tgt_vals.append(1.5)
    tgt_vals.append(2.5)
    tgt_vals.append(2.0)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    var loss = l1_loss(tape, p, t)
    # |1-1.5| + |2-2.5| + |3-2| = 0.5 + 0.5 + 1.0 = 2.0; /3 = 0.6667
    var val = Float64(tape.get_data(loss, 0))
    assert_true(abs(val - 0.6667) < 0.01, "L1 forward value")
    print("PASS: test_l1_loss_forward")


fn test_l1_backward_sign() raises:
    """L1 backward produces sign(pred - target) / n."""
    var tape = Tape(1024)
    var pred_vals = List[Float32]()
    pred_vals.append(3.0)
    pred_vals.append(1.0)
    var tgt_vals = List[Float32]()
    tgt_vals.append(2.0)
    tgt_vals.append(2.0)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    var loss = l1_loss(tape, p, t)
    run_backward(tape, loss)
    # pred[0]=3 > target[0]=2: sign=+1, grad = 1/2 = 0.5
    # pred[1]=1 < target[1]=2: sign=-1, grad = -1/2 = -0.5
    var g0 = Float64(tape.get_grad(p, 0))
    var g1 = Float64(tape.get_grad(p, 1))
    assert_true(abs(g0 - 0.5) < 0.01, "L1 backward sign positive")
    assert_true(abs(g1 - (-0.5)) < 0.01, "L1 backward sign negative")
    print("PASS: test_l1_backward_sign")


fn test_l1_finite_diff() raises:
    """L1 backward matches finite differences."""
    var eps = Float64(1e-4)
    var pred_vals = List[Float32]()
    pred_vals.append(2.0)
    pred_vals.append(3.0)
    pred_vals.append(1.0)
    var tgt_vals = List[Float32]()
    tgt_vals.append(1.0)
    tgt_vals.append(2.5)
    tgt_vals.append(3.0)

    # Compute analytical gradient
    var tape = Tape(2048)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    var loss = l1_loss(tape, p, t)
    run_backward(tape, loss)

    # Finite difference for each pred element
    for i in range(3):
        var tape_plus = Tape(2048)
        var pp = _make_var(tape_plus, pred_vals)
        var tp = _make_var(tape_plus, tgt_vals)
        tape_plus.set_data(pp, i, pred_vals[i] + Float32(eps))
        var lp = l1_loss(tape_plus, pp, tp)

        var tape_minus = Tape(2048)
        var pm = _make_var(tape_minus, pred_vals)
        var tm = _make_var(tape_minus, tgt_vals)
        tape_minus.set_data(pm, i, pred_vals[i] - Float32(eps))
        var lm = l1_loss(tape_minus, pm, tm)

        var fd = (Float64(tape_plus.get_data(lp, 0)) - Float64(tape_minus.get_data(lm, 0))) / (2.0 * eps)
        var ag = Float64(tape.get_grad(p, i))
        assert_true(abs(fd - ag) < 0.05, "L1 finite diff elem " + String(i))
    print("PASS: test_l1_finite_diff")


fn test_bce_forward() raises:
    """BCE forward computes -mean(t*log(p) + (1-t)*log(1-p))."""
    var tape = Tape(1024)
    var pred_vals = List[Float32]()
    pred_vals.append(0.8)
    pred_vals.append(0.2)
    var tgt_vals = List[Float32]()
    tgt_vals.append(1.0)
    tgt_vals.append(0.0)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    var loss = binary_cross_entropy(tape, p, t)
    # -(1*log(0.8) + 0*log(0.2)) + -(0*log(0.2) + 1*log(0.8)) / 2
    var expected = -(log(0.8) + log(0.8)) / 2.0
    var val = Float64(tape.get_data(loss, 0))
    assert_true(abs(val - expected) < 0.01, "BCE forward value")
    print("PASS: test_bce_forward")


fn test_bce_backward() raises:
    """BCE backward computes (-t/p + (1-t)/(1-p)) / n."""
    var tape = Tape(1024)
    var pred_vals = List[Float32]()
    pred_vals.append(0.7)
    pred_vals.append(0.3)
    var tgt_vals = List[Float32]()
    tgt_vals.append(1.0)
    tgt_vals.append(0.0)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    var loss = binary_cross_entropy(tape, p, t)
    run_backward(tape, loss)
    # elem 0: (-1/0.7 + 0/0.3) / 2 = -1/(0.7*2) = -0.7143
    # elem 1: (-0/0.3 + 1/0.7) / 2 = 1/(0.7*2) = 0.7143
    var g0 = Float64(tape.get_grad(p, 0))
    var g1 = Float64(tape.get_grad(p, 1))
    assert_true(abs(g0 - (-1.0 / 1.4)) < 0.05, "BCE backward elem 0")
    assert_true(abs(g1 - (1.0 / 1.4)) < 0.05, "BCE backward elem 1")
    print("PASS: test_bce_backward")


fn test_bce_finite_diff() raises:
    """BCE backward matches finite differences."""
    var eps = Float64(1e-4)
    var pred_vals = List[Float32]()
    pred_vals.append(0.6)
    pred_vals.append(0.4)
    var tgt_vals = List[Float32]()
    tgt_vals.append(0.8)
    tgt_vals.append(0.2)

    var tape = Tape(2048)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    var loss = binary_cross_entropy(tape, p, t)
    run_backward(tape, loss)

    for i in range(2):
        var tape_plus = Tape(2048)
        var pp = _make_var(tape_plus, pred_vals)
        var tp = _make_var(tape_plus, tgt_vals)
        tape_plus.set_data(pp, i, pred_vals[i] + Float32(eps))
        var lp = binary_cross_entropy(tape_plus, pp, tp)

        var tape_minus = Tape(2048)
        var pm = _make_var(tape_minus, pred_vals)
        var tm = _make_var(tape_minus, tgt_vals)
        tape_minus.set_data(pm, i, pred_vals[i] - Float32(eps))
        var lm = binary_cross_entropy(tape_minus, pm, tm)

        var fd = (Float64(tape_plus.get_data(lp, 0)) - Float64(tape_minus.get_data(lm, 0))) / (2.0 * eps)
        var ag = Float64(tape.get_grad(p, i))
        assert_true(abs(fd - ag) < 0.05, "BCE finite diff elem " + String(i))
    print("PASS: test_bce_finite_diff")


fn test_kl_forward() raises:
    """KL divergence forward: sum(p * log(p/q))."""
    var tape = Tape(1024)
    var p_vals = List[Float32]()
    p_vals.append(0.5)
    p_vals.append(0.5)
    var q_vals = List[Float32]()
    q_vals.append(0.5)
    q_vals.append(0.5)
    var p = _make_var(tape, p_vals)
    var q = _make_var(tape, q_vals)
    var loss = kl_divergence(tape, p, q)
    # p == q => KL = 0
    var val = Float64(tape.get_data(loss, 0))
    assert_true(abs(val) < 0.001, "KL forward identical distributions")
    print("PASS: test_kl_forward")


fn test_kl_backward_q_grad() raises:
    """KL backward w.r.t. q: d/dq KL = -p/q."""
    var tape = Tape(1024)
    var p_vals = List[Float32]()
    p_vals.append(0.3)
    p_vals.append(0.7)
    var q_vals = List[Float32]()
    q_vals.append(0.5)
    q_vals.append(0.5)
    var p = _make_var(tape, p_vals)
    var q = _make_var(tape, q_vals)
    var loss = kl_divergence(tape, p, q)
    run_backward(tape, loss)
    # d/dq[0] = -p[0]/q[0] = -0.3/0.5 = -0.6
    # d/dq[1] = -p[1]/q[1] = -0.7/0.5 = -1.4
    var gq0 = Float64(tape.get_grad(q, 0))
    var gq1 = Float64(tape.get_grad(q, 1))
    assert_true(abs(gq0 - (-0.6)) < 0.05, "KL backward q grad elem 0")
    assert_true(abs(gq1 - (-1.4)) < 0.05, "KL backward q grad elem 1")
    print("PASS: test_kl_backward_q_grad")


fn test_kl_finite_diff_q() raises:
    """KL backward w.r.t. q matches finite differences."""
    var eps = Float64(1e-4)
    var p_vals = List[Float32]()
    p_vals.append(0.4)
    p_vals.append(0.6)
    var q_vals = List[Float32]()
    q_vals.append(0.3)
    q_vals.append(0.7)

    var tape = Tape(2048)
    var p = _make_var(tape, p_vals)
    var q = _make_var(tape, q_vals)
    var loss = kl_divergence(tape, p, q)
    run_backward(tape, loss)

    for i in range(2):
        var tape_plus = Tape(2048)
        var pp = _make_var(tape_plus, p_vals)
        var qp = _make_var(tape_plus, q_vals)
        tape_plus.set_data(qp, i, q_vals[i] + Float32(eps))
        var lp = kl_divergence(tape_plus, pp, qp)

        var tape_minus = Tape(2048)
        var pm = _make_var(tape_minus, p_vals)
        var qm = _make_var(tape_minus, q_vals)
        tape_minus.set_data(qm, i, q_vals[i] - Float32(eps))
        var lm = kl_divergence(tape_minus, pm, qm)

        var fd = (Float64(tape_plus.get_data(lp, 0)) - Float64(tape_minus.get_data(lm, 0))) / (2.0 * eps)
        var ag = Float64(tape.get_grad(q, i))
        assert_true(abs(fd - ag) < 0.05, "KL finite diff q elem " + String(i))
    print("PASS: test_kl_finite_diff_q")


fn test_mse_still_works() raises:
    """Verify MSE loss is unaffected by the changes."""
    var tape = Tape(1024)
    var pred_vals = List[Float32]()
    pred_vals.append(1.0)
    pred_vals.append(2.0)
    var tgt_vals = List[Float32]()
    tgt_vals.append(1.5)
    tgt_vals.append(2.5)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    var loss = mse_loss(tape, p, t)
    run_backward(tape, loss)
    # MSE = (0.25 + 0.25) / 2 = 0.25
    var val = Float64(tape.get_data(loss, 0))
    assert_true(abs(val - 0.25) < 0.01, "MSE forward")
    # d/dp[0] = 2*(1-1.5)/2 = -0.5
    var g0 = Float64(tape.get_grad(p, 0))
    assert_true(abs(g0 - (-0.5)) < 0.01, "MSE backward")
    print("PASS: test_mse_still_works")


fn test_l1_op_code_not_mse() raises:
    """Verify L1 uses OP_L1 op code, not OP_MSE."""
    var tape = Tape(1024)
    var pred_vals = List[Float32]()
    pred_vals.append(1.0)
    var tgt_vals = List[Float32]()
    tgt_vals.append(2.0)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    _ = l1_loss(tape, p, t)
    var entry = tape.get_entry(tape.num_entries() - 1)
    assert_true(entry.op_kind == 32, "L1 should use OP_L1 (32), not OP_MSE (29)")
    print("PASS: test_l1_op_code_not_mse")


fn test_bce_op_code_not_mse() raises:
    """Verify BCE uses OP_BCE op code, not OP_MSE."""
    var tape = Tape(1024)
    var pred_vals = List[Float32]()
    pred_vals.append(0.5)
    var tgt_vals = List[Float32]()
    tgt_vals.append(1.0)
    var p = _make_var(tape, pred_vals)
    var t = _make_var(tape, tgt_vals)
    _ = binary_cross_entropy(tape, p, t)
    var entry = tape.get_entry(tape.num_entries() - 1)
    assert_true(entry.op_kind == 33, "BCE should use OP_BCE (33), not OP_MSE (29)")
    print("PASS: test_bce_op_code_not_mse")


fn main() raises:
    print("=== Sprint 61: Loss Backward Tests ===")
    test_l1_loss_forward()
    test_l1_backward_sign()
    test_l1_finite_diff()
    test_bce_forward()
    test_bce_backward()
    test_bce_finite_diff()
    test_kl_forward()
    test_kl_backward_q_grad()
    test_kl_finite_diff_q()
    test_mse_still_works()
    test_l1_op_code_not_mse()
    test_bce_op_code_not_mse()
    print("")
    print("All 12 loss backward tests passed!")
