# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Optimizer tests
# ===----------------------------------------------------------------------=== #

"""Tests for SGD, Adam, LRScheduler, grad_clip."""

from neutron_mojo.autograd import Tape, run_backward, tracked_sum, tracked_scalar_mul
from neutron_mojo.optim import SGD, Adam, LRScheduler, clip_grad_norm


fn assert_close(a: Float64, b: Float64, rtol: Float64 = 1e-3, atol: Float64 = 1e-4) raises:
    var diff = abs(a - b)
    var threshold = atol + rtol * abs(b)
    if diff > threshold:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn test_sgd_basic() raises:
    """SGD moves parameters in the right direction."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(3)
    var x_idx = tape.add_variable(dims^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))
    tape.set_data(x_idx, 2, Float32(3.0))

    # loss = sum(x * 2) => grad = 2 for all
    var y_idx = tracked_scalar_mul(tape, x_idx, 2.0)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    var params = List[Int]()
    params.append(x_idx)
    var sgd = SGD(lr=0.1)
    sgd.step(tape, params)

    # x[0] = 1.0 - 0.1 * 2.0 = 0.8
    assert_close(Float64(tape.get_data(x_idx, 0)), 0.8)
    assert_close(Float64(tape.get_data(x_idx, 1)), 1.8)
    print("  sgd_basic: PASS")


fn test_sgd_momentum() raises:
    """SGD with momentum accumulates velocity."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(2)
    var x_idx = tape.add_variable(dims^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(1.0))

    var params = List[Int]()
    params.append(x_idx)
    var sgd = SGD(lr=0.1, momentum=0.9)

    # Step 1
    tape.set_grad(x_idx, 0, Float32(1.0))
    tape.set_grad(x_idx, 1, Float32(1.0))
    sgd.step(tape, params)
    var after_step1 = Float64(tape.get_data(x_idx, 0))

    # Step 2 (same gradient)
    tape.set_grad(x_idx, 0, Float32(1.0))
    tape.set_grad(x_idx, 1, Float32(1.0))
    sgd.step(tape, params)
    var after_step2 = Float64(tape.get_data(x_idx, 0))

    # With momentum, step 2 should move more than step 1
    var move1 = abs(1.0 - after_step1)
    var move2 = abs(after_step1 - after_step2)
    if move2 <= move1:
        raise Error("Momentum should increase step size")
    print("  sgd_momentum: PASS")


fn test_adam_basic() raises:
    """Adam moves parameters."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(2)
    var x_idx = tape.add_variable(dims^)
    tape.set_data(x_idx, 0, Float32(5.0))
    tape.set_data(x_idx, 1, Float32(5.0))

    var params = List[Int]()
    params.append(x_idx)
    var adam = Adam(lr=0.01)

    # Set gradient
    tape.set_grad(x_idx, 0, Float32(2.0))
    tape.set_grad(x_idx, 1, Float32(-1.0))
    adam.step(tape, params)

    # x[0] should decrease (positive gradient)
    var v0 = Float64(tape.get_data(x_idx, 0))
    if v0 >= 5.0:
        raise Error("Adam should decrease x[0] with positive gradient")
    # x[1] should increase (negative gradient)
    var v1 = Float64(tape.get_data(x_idx, 1))
    if v1 <= 5.0:
        raise Error("Adam should increase x[1] with negative gradient")
    print("  adam_basic: PASS")


fn test_adam_convergence() raises:
    """Adam converges on simple quadratic."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(1)
    var x_idx = tape.add_variable(dims^)
    tape.set_data(x_idx, 0, Float32(10.0))

    var params = List[Int]()
    params.append(x_idx)
    var adam = Adam(lr=0.5)

    # Minimize x^2: grad = 2x
    for step in range(100):
        tape.zero_all_grads()
        var x_val = tape.get_data(x_idx, 0)
        tape.set_grad(x_idx, 0, Float32(2.0) * x_val)
        adam.step(tape, params)

    var final_val = abs(Float64(tape.get_data(x_idx, 0)))
    if final_val > 1.0:
        raise Error("Adam should converge near 0, got " + String(final_val))
    print("  adam_convergence: PASS")


fn test_lr_scheduler_constant() raises:
    """Constant LR scheduler."""
    var sched = LRScheduler(base_lr=0.001, schedule_type=0)
    assert_close(sched.get_lr(0), 0.001)
    assert_close(sched.get_lr(100), 0.001)
    print("  lr_scheduler_constant: PASS")


fn test_lr_scheduler_warmup() raises:
    """Linear warmup phase."""
    var sched = LRScheduler(base_lr=0.01, warmup_steps=10, schedule_type=0)
    # At step 0: lr = 0.01 * 1/10 = 0.001
    var lr0 = sched.get_lr(0)
    assert_close(lr0, 0.001)
    # At step 4: lr = 0.01 * 5/10 = 0.005
    var lr4 = sched.get_lr(4)
    assert_close(lr4, 0.005)
    # After warmup: full lr
    var lr10 = sched.get_lr(10)
    assert_close(lr10, 0.01)
    print("  lr_scheduler_warmup: PASS")


fn test_lr_scheduler_cosine() raises:
    """Cosine annealing."""
    var sched = LRScheduler(base_lr=0.01, total_steps=100, schedule_type=1)
    var lr0 = sched.get_lr(0)
    var lr50 = sched.get_lr(50)
    var lr100 = sched.get_lr(100)
    # Start: full lr
    assert_close(lr0, 0.01, atol=0.001)
    # Middle: ~half
    assert_close(lr50, 0.005, atol=0.002)
    # End: ~0
    assert_close(lr100, 0.0, atol=0.001)
    print("  lr_scheduler_cosine: PASS")


fn test_lr_scheduler_linear() raises:
    """Linear decay."""
    var sched = LRScheduler(base_lr=0.01, total_steps=100, schedule_type=2)
    var lr50 = sched.get_lr(50)
    assert_close(lr50, 0.005, atol=0.001)
    print("  lr_scheduler_linear: PASS")


fn test_grad_clip() raises:
    """Gradient clipping scales gradients."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^)
    # Set large gradients
    tape.set_grad(x_idx, 0, Float32(3.0))
    tape.set_grad(x_idx, 1, Float32(4.0))
    tape.set_grad(x_idx, 2, Float32(0.0))
    tape.set_grad(x_idx, 3, Float32(0.0))
    # Norm = sqrt(9+16) = 5

    var params = List[Int]()
    params.append(x_idx)
    var orig_norm = clip_grad_norm(tape, params, max_norm=1.0)
    assert_close(orig_norm, 5.0, atol=0.01)

    # After clipping, norm should be ~1.0
    var new_g0 = Float64(tape.get_grad(x_idx, 0))
    var new_g1 = Float64(tape.get_grad(x_idx, 1))
    from math import sqrt
    var new_norm = sqrt(new_g0 * new_g0 + new_g1 * new_g1)
    assert_close(new_norm, 1.0, atol=0.01)
    print("  grad_clip: PASS")


fn test_grad_clip_no_op() raises:
    """No clipping when norm is below threshold."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(2)
    var x_idx = tape.add_variable(dims^)
    tape.set_grad(x_idx, 0, Float32(0.1))
    tape.set_grad(x_idx, 1, Float32(0.1))

    var params = List[Int]()
    params.append(x_idx)
    var orig_norm = clip_grad_norm(tape, params, max_norm=10.0)

    # Gradients should be unchanged
    assert_close(Float64(tape.get_grad(x_idx, 0)), 0.1)
    assert_close(Float64(tape.get_grad(x_idx, 1)), 0.1)
    print("  grad_clip_no_op: PASS")


fn test_adam_weight_decay() raises:
    """Adam with weight decay shrinks parameters."""
    var tape = Tape(4096)
    var dims = List[Int]()
    dims.append(1)
    var x_idx = tape.add_variable(dims^)
    tape.set_data(x_idx, 0, Float32(10.0))

    var params = List[Int]()
    params.append(x_idx)
    var adam = Adam(lr=0.01, weight_decay=0.1)

    # Zero gradient — only weight decay acts
    tape.set_grad(x_idx, 0, Float32(0.0))
    adam.step(tape, params)

    var after = Float64(tape.get_data(x_idx, 0))
    if after >= 10.0:
        raise Error("Weight decay should shrink parameter")
    print("  adam_weight_decay: PASS")


fn main() raises:
    print("test_optimizers:")
    test_sgd_basic()
    test_sgd_momentum()
    test_adam_basic()
    test_adam_convergence()
    test_adam_weight_decay()
    test_lr_scheduler_constant()
    test_lr_scheduler_warmup()
    test_lr_scheduler_cosine()
    test_lr_scheduler_linear()
    test_grad_clip()
    test_grad_clip_no_op()
    print("ALL PASSED (11 tests)")
