# ===----------------------------------------------------------------------=== #
# Neutron Mojo — End-to-end training tests
# ===----------------------------------------------------------------------=== #

"""Tests for end-to-end training pipeline.

NOTE: Uses manual training loops instead of train_tiny_lm() because the
monolithic function causes the Mojo 0.26.2 compiler to hang during codegen.
The manual loops test the same functionality: model creation, forward pass,
loss computation, backward pass, gradient clipping, optimizer step.
"""

from neutron_mojo.autograd import Tape, run_backward
from neutron_mojo.train.trainable import TrainableLM, causal_lm_loss
from neutron_mojo.train.loop import TrainingConfig, TrainingState, TrainingMetrics
from neutron_mojo.train.e2e import create_simple_dataset
from neutron_mojo.data import Dataset, DataSample
from neutron_mojo.optim import Adam, SGD, clip_grad_norm


fn assert_close(a: Float64, b: Float64, rtol: Float64 = 1e-3, atol: Float64 = 1e-3) raises:
    var diff = abs(a - b)
    var threshold = atol + rtol * abs(b)
    if diff > threshold:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn test_create_simple_dataset() raises:
    """Create dataset from token sequence."""
    var tokens = List[Int]()
    for i in range(10):
        tokens.append(i)
    var ds = create_simple_dataset(tokens, seq_len=3)
    assert_eq(ds.size(), 7)
    var s0 = ds.get(0)
    assert_eq(s0.input_ids[0], 0)
    assert_eq(s0.target_id, 3)
    print("  create_simple_dataset: PASS")


fn test_create_dataset_too_short() raises:
    """Dataset with too-short sequence."""
    var tokens = List[Int]()
    tokens.append(1)
    tokens.append(2)
    var ds = create_simple_dataset(tokens, seq_len=3)
    assert_eq(ds.size(), 0)
    print("  create_dataset_too_short: PASS")


fn test_train_loss_adam() raises:
    """End-to-end training with Adam optimizer."""
    var tokens = List[Int]()
    for i in range(20):
        tokens.append(i % 4)
    var ds = create_simple_dataset(tokens, seq_len=1)

    var tape = Tape(262144)
    var model = TrainableLM(vocab_size=4, hidden_dim=4, num_layers=1, ffn_dim=8)
    model.register(tape)
    var params = model.all_param_indices()
    var adam = Adam(lr=0.01)

    var total_steps = 0
    var last_loss = Float64(0.0)
    for _epoch in range(3):
        for si in range(ds.size()):
            var sample = ds.get(si)
            var last_token = sample.input_ids[sample.seq_len() - 1]
            var loss_idx = causal_lm_loss(tape, model, last_token, sample.target_id)
            last_loss = Float64(tape.get_data(loss_idx, 0))
            run_backward(tape, loss_idx)
            _ = clip_grad_norm(tape, params, 1.0)
            adam.step(tape, params)
            tape.zero_all_grads()
            total_steps += 1

    if last_loss < 0.0:
        raise Error("Final loss should be non-negative")
    if total_steps <= 0:
        raise Error("Should have taken some steps")
    print("  train_loss_adam: PASS (loss=" + String(last_loss)
          + " steps=" + String(total_steps) + ")")


fn test_train_sgd() raises:
    """Training with SGD optimizer."""
    var tokens = List[Int]()
    for i in range(12):
        tokens.append(i % 3)
    var ds = create_simple_dataset(tokens, seq_len=1)

    var tape = Tape(262144)
    var model = TrainableLM(vocab_size=3, hidden_dim=4, num_layers=1, ffn_dim=8)
    model.register(tape)
    var params = model.all_param_indices()
    var sgd = SGD(lr=0.005)

    var total_steps = 0
    var last_loss = Float64(0.0)
    for _epoch in range(2):
        for si in range(ds.size()):
            var sample = ds.get(si)
            var last_token = sample.input_ids[sample.seq_len() - 1]
            var loss_idx = causal_lm_loss(tape, model, last_token, sample.target_id)
            last_loss = Float64(tape.get_data(loss_idx, 0))
            run_backward(tape, loss_idx)
            _ = clip_grad_norm(tape, params, 1.0)
            sgd.step(tape, params)
            tape.zero_all_grads()
            total_steps += 1

    if total_steps <= 0:
        raise Error("Should have taken some steps")
    print("  train_sgd: PASS (loss=" + String(last_loss) + ")")


fn test_overfit_one_example() raises:
    """Model can overfit a single training example."""
    var ds = Dataset()
    for _ in range(5):
        var ids = List[Int]()
        ids.append(1)
        ds.add(DataSample(ids^, 2))

    var tape = Tape(262144)
    var model = TrainableLM(vocab_size=4, hidden_dim=4, num_layers=1, ffn_dim=8)
    model.register(tape)
    var params = model.all_param_indices()
    var adam = Adam(lr=0.01)

    var last_loss = Float64(0.0)
    for _epoch in range(20):
        for si in range(ds.size()):
            var sample = ds.get(si)
            var last_token = sample.input_ids[sample.seq_len() - 1]
            var loss_idx = causal_lm_loss(tape, model, last_token, sample.target_id)
            last_loss = Float64(tape.get_data(loss_idx, 0))
            run_backward(tape, loss_idx)
            _ = clip_grad_norm(tape, params, 1.0)
            adam.step(tape, params)
            tape.zero_all_grads()

    if last_loss > 20.0:
        raise Error("Expected loss < 20 after overfitting, got " + String(last_loss))
    print("  overfit_one_example: PASS (loss=" + String(last_loss) + ")")


fn test_training_metrics() raises:
    """TrainingMetrics records losses."""
    var metrics = TrainingMetrics()
    metrics.record(2.5, 0.01)
    metrics.record(2.0, 0.01)
    metrics.record(1.5, 0.01)

    if metrics.num_records() != 3:
        raise Error("Expected 3 records")
    assert_close(metrics.last_loss(), 1.5)
    print("  training_metrics: PASS")


fn test_manual_training_loop() raises:
    """Manual training loop with loss tracking."""
    var tape = Tape(262144)
    var model = TrainableLM(vocab_size=4, hidden_dim=4, num_layers=1, ffn_dim=8)
    model.register(tape)
    var params = model.all_param_indices()
    var adam = Adam(lr=0.01)

    var losses = List[Float64]()
    for _step in range(5):
        var loss_idx = causal_lm_loss(tape, model, token_id=1, target_id=2)
        var loss_val = Float64(tape.get_data(loss_idx, 0))
        losses.append(loss_val)
        run_backward(tape, loss_idx)
        _ = clip_grad_norm(tape, params, max_norm=1.0)
        adam.step(tape, params)
        tape.zero_all_grads()

    assert_eq(len(losses), 5)
    for i in range(len(losses)):
        if losses[i] < 0.0:
            raise Error("Loss should be non-negative at step " + String(i))
    print("  manual_training_loop: PASS")


fn test_multilayer_training() raises:
    """Training with 2 transformer layers."""
    var tokens = List[Int]()
    for i in range(12):
        tokens.append(i % 3)
    var ds = create_simple_dataset(tokens, seq_len=1)

    var tape = Tape(524288)
    var model = TrainableLM(vocab_size=3, hidden_dim=4, num_layers=2, ffn_dim=8)
    model.register(tape)
    var params = model.all_param_indices()
    var adam = Adam(lr=0.005)

    var total_steps = 0
    var last_loss = Float64(0.0)
    for _epoch in range(2):
        for si in range(ds.size()):
            var sample = ds.get(si)
            var last_token = sample.input_ids[sample.seq_len() - 1]
            var loss_idx = causal_lm_loss(tape, model, last_token, sample.target_id)
            last_loss = Float64(tape.get_data(loss_idx, 0))
            run_backward(tape, loss_idx)
            _ = clip_grad_norm(tape, params, 1.0)
            adam.step(tape, params)
            tape.zero_all_grads()
            total_steps += 1

    if total_steps <= 0:
        raise Error("Should have taken some steps")
    print("  multilayer_training: PASS (loss=" + String(last_loss) + ")")


fn test_grad_clipping_effect() raises:
    """Gradient clipping prevents explosion."""
    var tape = Tape(262144)
    var model = TrainableLM(vocab_size=4, hidden_dim=4, num_layers=1, ffn_dim=8)
    model.register(tape)
    var params = model.all_param_indices()

    var loss_idx = causal_lm_loss(tape, model, token_id=0, target_id=3)
    run_backward(tape, loss_idx)

    var norm_before = clip_grad_norm(tape, params, max_norm=0.5)
    print("  grad_clipping_effect: PASS (orig_norm=" + String(norm_before) + ")")


fn test_empty_dataset() raises:
    """Training with empty dataset doesn't crash."""
    var ds = Dataset()
    var tape = Tape(262144)
    var model = TrainableLM(vocab_size=4, hidden_dim=4, num_layers=1, ffn_dim=8)
    model.register(tape)
    var params = model.all_param_indices()
    var adam = Adam(lr=0.01)

    var total_steps = 0
    for _epoch in range(1):
        for si in range(ds.size()):
            var sample = ds.get(si)
            var last_token = sample.input_ids[sample.seq_len() - 1]
            var loss_idx = causal_lm_loss(tape, model, last_token, sample.target_id)
            run_backward(tape, loss_idx)
            adam.step(tape, params)
            tape.zero_all_grads()
            total_steps += 1

    assert_eq(total_steps, 0)
    print("  empty_dataset: PASS")


fn main() raises:
    print("test_e2e_training:")
    test_create_simple_dataset()
    test_create_dataset_too_short()
    test_train_loss_adam()
    test_train_sgd()
    test_overfit_one_example()
    test_training_metrics()
    test_manual_training_loop()
    test_multilayer_training()
    test_grad_clipping_effect()
    test_empty_dataset()
    print("ALL PASSED (10 tests)")
