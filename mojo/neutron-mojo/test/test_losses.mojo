# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Loss function tests
# ===----------------------------------------------------------------------=== #

"""Tests for cross_entropy, mse, l1, binary_cross_entropy, kl_divergence."""

from math import exp, log
from neutron_mojo.autograd import Tape, run_backward
from neutron_mojo.train.losses import (
    log_softmax, cross_entropy_loss, mse_loss, l1_loss,
    binary_cross_entropy, kl_divergence,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-3, atol: Float64 = 1e-3) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error("Values not close: " + String(a) + " vs " + String(b) + " (diff=" + String(diff) + ")")


fn test_log_softmax() raises:
    """Log-softmax produces correct values."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(3)
    var x_idx = tape.add_variable(dims^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))
    tape.set_data(x_idx, 2, Float32(3.0))

    var y_idx = log_softmax(tape, x_idx)
    # log_softmax values should be negative and sum(exp(log_softmax)) = 1
    var sum_exp = Float64(0.0)
    for i in range(3):
        var v = Float64(tape.get_data(y_idx, i))
        if v > 0.0:
            raise Error("log_softmax should be negative")
        sum_exp += exp(v)
    if abs(sum_exp - 1.0) > 0.01:
        raise Error("sum(exp(log_softmax)) should be 1, got " + String(sum_exp))
    print("  log_softmax: PASS")


fn test_cross_entropy_loss() raises:
    """Cross-entropy loss is positive and reasonable."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(5)
    var logits_idx = tape.add_variable(dims^)
    # Set logits so target class has highest value
    tape.set_data(logits_idx, 0, Float32(0.0))
    tape.set_data(logits_idx, 1, Float32(0.0))
    tape.set_data(logits_idx, 2, Float32(5.0))  # target
    tape.set_data(logits_idx, 3, Float32(0.0))
    tape.set_data(logits_idx, 4, Float32(0.0))

    var loss_idx = cross_entropy_loss(tape, logits_idx, 2, 5)
    var loss_val = Float64(tape.get_data(loss_idx, 0))
    # Loss should be positive and relatively small (target has high logit)
    if loss_val < 0.0:
        raise Error("Cross-entropy loss should be positive")
    if loss_val > 10.0:
        raise Error("Cross-entropy loss too high for correct prediction")
    print("  cross_entropy_loss: PASS")


fn test_cross_entropy_backward() raises:
    """Cross-entropy backward produces softmax - one_hot."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(3)
    var logits_idx = tape.add_variable(dims^)
    tape.set_data(logits_idx, 0, Float32(1.0))
    tape.set_data(logits_idx, 1, Float32(2.0))
    tape.set_data(logits_idx, 2, Float32(3.0))

    var loss_idx = cross_entropy_loss(tape, logits_idx, 1, 3)
    run_backward(tape, loss_idx)

    # Gradient should be softmax - one_hot
    # softmax([1,2,3]) ≈ [0.090, 0.245, 0.665]
    # target=1: grad ≈ [0.090, 0.245-1, 0.665] = [0.090, -0.755, 0.665]
    var g0 = tape.get_grad(logits_idx, 0)
    var g1 = tape.get_grad(logits_idx, 1)
    if Float64(g0) < -0.1:
        raise Error("Expected positive gradient for non-target class")
    if Float64(g1) > 0.0:
        raise Error("Expected negative gradient for target class")
    print("  cross_entropy_backward: PASS")


fn test_mse_loss() raises:
    """MSE loss computation."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(3)
    var pred_idx = tape.add_variable(dims.copy())
    var target_idx = tape.add_variable(dims.copy())
    tape.set_data(pred_idx, 0, Float32(1.0))
    tape.set_data(pred_idx, 1, Float32(2.0))
    tape.set_data(pred_idx, 2, Float32(3.0))
    tape.set_data(target_idx, 0, Float32(1.0))
    tape.set_data(target_idx, 1, Float32(2.0))
    tape.set_data(target_idx, 2, Float32(3.0))

    var loss_idx = mse_loss(tape, pred_idx, target_idx)
    # Perfect prediction: MSE = 0
    assert_close(tape.get_data(loss_idx, 0), 0.0, atol=1e-6)
    print("  mse_loss_zero: PASS")


fn test_mse_loss_nonzero() raises:
    """MSE loss with difference."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(2)
    var pred_idx = tape.add_variable(dims.copy())
    var target_idx = tape.add_variable(dims.copy())
    tape.set_data(pred_idx, 0, Float32(1.0))
    tape.set_data(pred_idx, 1, Float32(3.0))
    tape.set_data(target_idx, 0, Float32(2.0))
    tape.set_data(target_idx, 1, Float32(1.0))

    var loss_idx = mse_loss(tape, pred_idx, target_idx)
    # MSE = ((1-2)^2 + (3-1)^2) / 2 = (1 + 4) / 2 = 2.5
    assert_close(tape.get_data(loss_idx, 0), 2.5)
    print("  mse_loss_nonzero: PASS")


fn test_mse_backward() raises:
    """MSE backward produces correct gradients."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(2)
    var pred_idx = tape.add_variable(dims.copy())
    var target_idx = tape.add_variable(dims.copy())
    tape.set_data(pred_idx, 0, Float32(3.0))
    tape.set_data(pred_idx, 1, Float32(1.0))
    tape.set_data(target_idx, 0, Float32(1.0))
    tape.set_data(target_idx, 1, Float32(1.0))

    var loss_idx = mse_loss(tape, pred_idx, target_idx)
    run_backward(tape, loss_idx)

    # grad = 2*(pred-target)/n = 2*(3-1)/2 = 2.0 for first, 0 for second
    assert_close(tape.get_grad(pred_idx, 0), 2.0, atol=0.01)
    assert_close(tape.get_grad(pred_idx, 1), 0.0, atol=0.01)
    print("  mse_backward: PASS")


fn test_l1_loss() raises:
    """L1 loss computation."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(3)
    var pred_idx = tape.add_variable(dims.copy())
    var target_idx = tape.add_variable(dims.copy())
    tape.set_data(pred_idx, 0, Float32(1.0))
    tape.set_data(pred_idx, 1, Float32(4.0))
    tape.set_data(pred_idx, 2, Float32(2.0))
    tape.set_data(target_idx, 0, Float32(2.0))
    tape.set_data(target_idx, 1, Float32(1.0))
    tape.set_data(target_idx, 2, Float32(2.0))

    var loss_idx = l1_loss(tape, pred_idx, target_idx)
    # L1 = (|1-2| + |4-1| + |2-2|) / 3 = 4/3 ≈ 1.333
    assert_close(tape.get_data(loss_idx, 0), Float32(4.0 / 3.0), atol=0.01)
    print("  l1_loss: PASS")


fn test_binary_cross_entropy() raises:
    """Binary cross-entropy with perfect prediction is near zero."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(2)
    var pred_idx = tape.add_variable(dims.copy())
    var target_idx = tape.add_variable(dims.copy())
    tape.set_data(pred_idx, 0, Float32(0.99))
    tape.set_data(pred_idx, 1, Float32(0.01))
    tape.set_data(target_idx, 0, Float32(1.0))
    tape.set_data(target_idx, 1, Float32(0.0))

    var loss_idx = binary_cross_entropy(tape, pred_idx, target_idx)
    var loss_val = Float64(tape.get_data(loss_idx, 0))
    if loss_val > 0.1:
        raise Error("BCE with near-perfect predictions should be small")
    print("  binary_cross_entropy: PASS")


fn test_kl_divergence() raises:
    """KL divergence: identical distributions give 0."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(3)
    var p_idx = tape.add_variable(dims.copy())
    var q_idx = tape.add_variable(dims.copy())
    tape.set_data(p_idx, 0, Float32(0.3))
    tape.set_data(p_idx, 1, Float32(0.4))
    tape.set_data(p_idx, 2, Float32(0.3))
    tape.set_data(q_idx, 0, Float32(0.3))
    tape.set_data(q_idx, 1, Float32(0.4))
    tape.set_data(q_idx, 2, Float32(0.3))

    var loss_idx = kl_divergence(tape, p_idx, q_idx)
    assert_close(tape.get_data(loss_idx, 0), 0.0, atol=1e-5)
    print("  kl_divergence_zero: PASS")


fn test_kl_divergence_positive() raises:
    """KL divergence is positive for different distributions."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(3)
    var p_idx = tape.add_variable(dims.copy())
    var q_idx = tape.add_variable(dims.copy())
    tape.set_data(p_idx, 0, Float32(0.7))
    tape.set_data(p_idx, 1, Float32(0.2))
    tape.set_data(p_idx, 2, Float32(0.1))
    tape.set_data(q_idx, 0, Float32(0.3))
    tape.set_data(q_idx, 1, Float32(0.3))
    tape.set_data(q_idx, 2, Float32(0.4))

    var loss_idx = kl_divergence(tape, p_idx, q_idx)
    var kl_val = Float64(tape.get_data(loss_idx, 0))
    if kl_val <= 0.0:
        raise Error("KL divergence should be positive for different distributions")
    print("  kl_divergence_positive: PASS")


fn main() raises:
    print("test_losses:")
    test_log_softmax()
    test_cross_entropy_loss()
    test_cross_entropy_backward()
    test_mse_loss()
    test_mse_loss_nonzero()
    test_mse_backward()
    test_l1_loss()
    test_binary_cross_entropy()
    test_kl_divergence()
    test_kl_divergence_positive()
    print("ALL PASSED (10 tests)")
