# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Training loop tests
# ===----------------------------------------------------------------------=== #

"""Tests for TrainingConfig, TrainingState, TrainingMetrics."""

from math import exp
from neutron_mojo.train.loop import (
    TrainingConfig, TrainingState, TrainingMetrics, estimate_training_memory,
)


fn assert_close(a: Float64, b: Float64, rtol: Float64 = 1e-3, atol: Float64 = 1e-4) raises:
    var diff = abs(a - b)
    var threshold = atol + rtol * abs(b)
    if diff > threshold:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn test_config_defaults() raises:
    var c = TrainingConfig()
    assert_eq(c.epochs, 10)
    assert_eq(c.batch_size, 4)
    assert_close(c.lr, 1e-3)
    print("  config_defaults: PASS")


fn test_config_copy() raises:
    var c = TrainingConfig()
    c.lr = 0.01
    var c2 = c.copy()
    assert_close(c2.lr, 0.01)
    print("  config_copy: PASS")


fn test_state_avg_loss() raises:
    var s = TrainingState()
    s.record_loss(1.0)
    s.record_loss(3.0)
    assert_close(s.avg_loss(), 2.0)
    assert_eq(s.global_step, 2)
    print("  state_avg_loss: PASS")


fn test_state_reset() raises:
    var s = TrainingState()
    s.record_loss(5.0)
    s.reset_running_loss()
    assert_close(s.avg_loss(), 0.0)
    assert_eq(s.loss_count, 0)
    print("  state_reset: PASS")


fn test_metrics_record() raises:
    var m = TrainingMetrics()
    m.record(2.5, 0.001)
    m.record(1.5, 0.0005)
    assert_close(m.last_loss(), 1.5)
    assert_eq(m.num_records(), 2)
    print("  metrics_record: PASS")


fn test_metrics_perplexity() raises:
    var m = TrainingMetrics()
    m.record(1.0, 0.001)
    var ppl = m.perplexity()
    assert_close(ppl, exp(1.0), atol=0.01)
    print("  metrics_perplexity: PASS")


fn test_metrics_perplexity_high() raises:
    var m = TrainingMetrics()
    m.record(100.0, 0.001)
    var ppl = m.perplexity()
    assert_close(ppl, 1e10)  # capped
    print("  metrics_perplexity_high: PASS")


fn test_estimate_memory_sgd() raises:
    var c = TrainingConfig()
    c.use_adam = False
    var mem = estimate_training_memory(1000000, c)
    # params + grads + activations = 3 * 4MB = 12MB, no optimizer state
    assert_eq(mem, 1000000 * 4 * 3)
    print("  estimate_memory_sgd: PASS")


fn test_estimate_memory_adam() raises:
    var c = TrainingConfig()
    c.use_adam = True
    var mem = estimate_training_memory(1000000, c)
    # params + grads + optimizer(m+v) + activations = 5 * 4MB = 20MB
    assert_eq(mem, 1000000 * 4 * 5)
    print("  estimate_memory_adam: PASS")


fn test_state_best_eval() raises:
    var s = TrainingState()
    assert_close(s.best_eval_loss, 1e10)
    s.best_eval_loss = 2.0
    assert_close(s.best_eval_loss, 2.0)
    print("  state_best_eval: PASS")


fn main() raises:
    print("test_training_loop:")
    test_config_defaults()
    test_config_copy()
    test_state_avg_loss()
    test_state_reset()
    test_metrics_record()
    test_metrics_perplexity()
    test_metrics_perplexity_high()
    test_estimate_memory_sgd()
    test_estimate_memory_adam()
    test_state_best_eval()
    print("ALL PASSED (10 tests)")
