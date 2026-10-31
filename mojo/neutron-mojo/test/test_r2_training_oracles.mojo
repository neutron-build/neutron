"""Independent r2 gradient/layout/arena oracles. Runtime pending capacity release."""
from std.testing import assert_true, assert_equal, assert_false
from std.random import seed
from std.math import abs
from neutron_mojo.autograd.tape import Tape
from neutron_mojo.autograd.ops import tracked_sum, tracked_add
from neutron_mojo.autograd.backward import run_backward
from neutron_mojo.autograd.checkpoint import gradients_match
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.train.modules import Linear, Dropout
from neutron_mojo.train.lora_train import TrainableLoRA
from neutron_mojo.fusion.executor import TensorValue


def close(actual: Float32, expected: Float32) raises:
    assert_true(abs(actual - expected) < 0.00001)


def close(actual: Float32, expected: Int) raises:
    close(actual, Float32(expected))


def test_linear_and_bias() raises:
    var tape = Tape(64)
    var layer = Linear(3, 2, has_bias=True)
    layer.register(tape)
    var x = tape.add_variable_from_shape(Shape(2, 3))
    var sentinel = tape.add_variable_from_shape(Shape(4))
    # W is [[1,2,3],[-2,4,1]]; x is [[1,-1,2],[3,2,-1]].
    var weights = [1,2,3,-2,4,1]
    var values = [1,-1,2,3,2,-1]
    for i in range(6):
        tape.set_data(layer.weight_idx, i, Float32(weights[i]))
        tape.set_data(x, i, Float32(values[i]))
    tape.set_data(layer.bias_idx, 0, Float32(10))
    tape.set_data(layer.bias_idx, 1, Float32(-3))
    for i in range(4):
        tape.set_data(sentinel, i, Float32(1234))
    var y = layer.forward(tape, x)
    var expected = [15,-7,14,-2]
    for i in range(4):
        close(tape.get_data(y, i), expected[i])
    var loss = tracked_sum(tape, y)
    run_backward(tape, loss)
    # dW is the row sum of x repeated for each output channel.
    var dw = [4,1,1,4,1,1]
    var dx = [-1,6,4,-1,6,4]
    for i in range(6):
        close(tape.get_grad(layer.weight_idx, i), dw[i])
        close(tape.get_grad(x, i), dx[i])
    for i in range(2):
        close(tape.get_grad(layer.bias_idx, i), 2)
    for i in range(4):
        close(tape.get_data(sentinel, i), 1234)
        close(tape.get_grad(sentinel, i), 0)
    var before = tape.num_variables()
    var refused = False
    try:
        _ = tracked_add(tape, x, layer.bias_idx)
    except:
        refused = True
    assert_true(refused)
    assert_equal(tape.num_variables(), before)


def test_lora() raises:
    var tape = Tape(64)
    var layer = TrainableLoRA(3, 2, 2)
    layer.register(tape)
    var x = tape.add_variable_from_shape(Shape(3))
    var a = [1,2,3,-2,4,1]
    var b = [2,-1,3,4]
    for i in range(6):
        tape.set_data(layer.a_idx, i, Float32(a[i]))
    for i in range(4):
        tape.set_data(layer.b_idx, i, Float32(b[i]))
    tape.set_data(x, 0, Float32(1))
    tape.set_data(x, 1, Float32(-1))
    tape.set_data(x, 2, Float32(2))
    var y = layer.forward(tape, x)
    close(tape.get_data(y, 0), 14)
    close(tape.get_data(y, 1), -1)
    var loss = tracked_sum(tape, y)
    run_backward(tape, loss)
    # mid=[5,-4], column sums of B=[5,3].
    var da = [5,-5,10,3,-3,6]
    var db = [5,-4,5,-4]
    var dx = [-1,22,18]
    for i in range(6):
        close(tape.get_grad(layer.a_idx, i), da[i])
    for i in range(4):
        close(tape.get_grad(layer.b_idx, i), db[i])
    for i in range(3):
        close(tape.get_grad(x, i), dx[i])


def test_dropout_and_rewind() raises:
    seed(7351)
    var tape = Tape(64)
    var x = tape.add_variable_from_shape(Shape(16))
    # All inputs are zero, so deriving a mask from forward output is invalid.
    var dropout = Dropout(0.5)
    var mask = tape.num_variables()
    var y = dropout.forward(tape, x)
    var loss = tracked_sum(tape, y)
    run_backward(tape, loss)
    var kept = 0
    var dropped = 0
    for i in range(16):
        close(tape.get_grad(x, i), tape.get_data(mask, i))
        if tape.get_data(mask, i) == 0:
            dropped += 1
        else:
            kept += 1
    assert_true(kept > 0 and dropped > 0)
    tape.rewind_activations(1)
    tape.zero_all_grads()
    var full = Dropout(1.0)
    y = full.forward(tape, x)
    loss = tracked_sum(tape, y)
    run_backward(tape, loss)
    for i in range(16):
        close(tape.get_data(y, i), 0)
        close(tape.get_grad(x, i), 0)
    tape.rewind_activations(1)
    var warmed_capacity = tape.capacity
    for step in range(2000):
        tape.zero_all_grads()
        y = full.forward(tape, x)
        loss = tracked_sum(tape, y)
        run_backward(tape, loss)
        tape.rewind_activations(1)
        assert_equal(tape.num_variables(), 1)
        assert_equal(tape.num_entries(), 0)
        assert_equal(tape.total_used, 16)
        assert_equal(tape.capacity, warmed_capacity)
    dropout.eval_mode()
    assert_equal(dropout.forward(tape, x), x)


def test_flat_copies_and_shapes() raises:
    var data = Tensor[DType.float32](Shape(2,3))
    for i in range(6):
        data.set(i, Float32(i + 11))
    var value = TensorValue(data^, 2, 3)
    var copied = value.copy()
    var implicitly_copied = value
    for i in range(6):
        close(copied.data.data_ptr()[i], Float32(i + 11))
        close(implicitly_copied.data.data_ptr()[i], Float32(i + 11))
    var tape = Tape(16)
    var x = tape.add_variable_from_shape(Shape(2,3))
    tape.set_data_from_tensor(x, value.data)
    for i in range(6):
        close(tape.get_data(x,i), Float32(i + 11))
    var other = Tape(16)
    var y = other.add_variable_from_shape(Shape(3,2))
    assert_false(gradients_match(tape, other, x))


def main() raises:
    test_linear_and_bias()
    test_lora()
    test_dropout_and_rewind()
    test_flat_copies_and_shapes()
    print("MJ-06, MJ-K05/K07/K08/K09/K10 independent r2 oracles PASS")
