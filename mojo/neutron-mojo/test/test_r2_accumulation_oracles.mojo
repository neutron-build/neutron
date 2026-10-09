"""Accumulation, decay, arena reuse and disconnected-NaN independent checks."""
from std.math import abs, isfinite
from std.random import seed
from std.testing import assert_true, assert_equal
from neutron_mojo.data.dataset import Dataset, DataSample
from neutron_mojo.train.e2e import train_tiny_lm
from neutron_mojo.train.loop import TrainingConfig
from neutron_mojo.train.trainable import TrainableLM
from neutron_mojo.autograd.tape import Tape
from neutron_mojo.autograd.ops import tracked_mul, tracked_sum
from neutron_mojo.autograd.backward import run_backward
from neutron_mojo.autograd.checkpoint import run_backward_checkpointed, auto_checkpoint_segments
from neutron_mojo.io.binary_reader import _u32_to_f32
from neutron_mojo.tensor.shape import Shape


def config(batch: Int, accumulation: Int, epochs: Int = 2) -> TrainingConfig:
    var cfg = TrainingConfig()
    cfg.batch_size = batch
    cfg.grad_accumulation_steps = accumulation
    cfg.epochs = epochs
    cfg.use_adam = False
    cfg.warmup_steps = 0
    cfg.max_grad_norm = 0
    cfg.lr = 0.01
    cfg.weight_decay = 0
    return cfg


def samples(count: Int) -> Dataset:
    var data = Dataset()
    for i in range(count):
        data.add(DataSample([i % 3], (i + 1) % 3))
    return data^


def test_accumulation_and_decay() raises:
    var data = samples(5)
    var batch = config(2, 1)
    var micro = config(1, 2)
    seed(8642)
    var grouped = train_tiny_lm(data, batch, 3, 2, 1, 2, verbose=False)
    seed(8642)
    var accumulated = train_tiny_lm(data, micro, 3, 2, 1, 2, verbose=False)
    assert_equal(grouped.total_steps, 6)
    assert_equal(accumulated.total_steps, 6)
    assert_equal(grouped.tape.num_variables(), accumulated.tape.num_variables())
    assert_equal(grouped.tape.total_used, accumulated.tape.total_used)
    assert_equal(grouped.tape.num_entries(), 0)
    for i in range(grouped.tape.total_used):
        assert_true(abs(grouped.tape.data_flat.get(i) - accumulated.tape.data_flat.get(i)) < 0.000001)
    # Same first-step gradient, independent SGD equation isolates decay.
    var one_step_data = samples(2)
    var plain = config(2, 1, 1)
    var decay = config(2, 1, 1)
    decay.weight_decay = 0.2
    seed(1357)
    var initial = Tape(2048)
    var model = TrainableLM(3, 2, 1, 2)
    model.register(initial)
    seed(1357)
    var without = train_tiny_lm(one_step_data, plain, 3, 2, 1, 2, verbose=False)
    seed(1357)
    var with_decay = train_tiny_lm(one_step_data, decay, 3, 2, 1, 2, verbose=False)
    assert_equal(without.total_steps, 1)
    assert_equal(with_decay.total_steps, 1)
    assert_equal(initial.total_used, without.tape.total_used)
    var witnessed = False
    for i in range(initial.total_used):
        var expected = -plain.lr * decay.weight_decay * Float64(initial.data_flat.get(i))
        var actual = Float64(with_decay.tape.data_flat.get(i)) - Float64(without.tape.data_flat.get(i))
        assert_true(abs(actual - expected) < 0.000002)
        if abs(expected) > 0.0001:
            witnessed = True
    assert_true(witnessed)


def test_reachable_reused_graph() raises:
    var reused = Tape(32)
    var x = reused.add_variable_from_shape(Shape(1))
    var nan = reused.add_variable_from_shape(Shape(1), requires_grad=False)
    reused.set_data(nan, 0, _u32_to_f32(0x7FC00000))
    var warm_capacity = reused.capacity
    for step in range(100):
        var value = Float32(step + 1) / 10
        reused.set_data(x, 0, value)
        reused.zero_all_grads()
        var unused = tracked_mul(reused, x, nan)
        var square = tracked_mul(reused, x, x)
        var loss = tracked_sum(reused, square)
        run_backward(reused, loss)
        var expected = 2 * value
        assert_true(isfinite(reused.get_grad(x, 0)))
        assert_true(abs(reused.get_grad(x, 0) - expected) < 0.000001)
        var fresh = Tape(32)
        var fx = fresh.add_variable_from_shape(Shape(1))
        fresh.set_data(fx, 0, value)
        var fsquare = tracked_mul(fresh, fx, fx)
        var floss = tracked_sum(fresh, fsquare)
        run_backward(fresh, floss)
        assert_equal(reused.get_grad(x, 0), fresh.get_grad(fx, 0))
        reused.zero_all_grads()
        var segments = auto_checkpoint_segments(reused, 2)
        run_backward_checkpointed(reused, loss, segments)
        assert_equal(reused.get_grad(x, 0), fresh.get_grad(fx, 0))
        reused.rewind_activations(2)
        assert_equal(reused.capacity, warm_capacity)
        assert_equal(reused.num_variables(), 2)
        assert_equal(reused.num_entries(), 0)
        assert_equal(reused.total_used, 2)
    print("PASS: fresh/reused/checkpoint/disconnected-NaN gradient oracle")


def main() raises:
    test_accumulation_and_decay()
    test_reachable_reused_graph()
    print("PASS: r2 accumulation/partial-window/SGD-decay oracles")
