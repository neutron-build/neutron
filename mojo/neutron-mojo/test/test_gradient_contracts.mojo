"""MJ-K04/K10/K18: independent norm derivatives and fail-closed comparisons."""
from std.testing import assert_true, assert_false
from std.math import sqrt, abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.dtype import can_cast
from neutron_mojo.io.binary_reader import _u32_to_f32
from neutron_mojo.autograd.tape import Tape, TapeEntry, OP_RMSNORM
from neutron_mojo.autograd.backward import _backward_rmsnorm
from neutron_mojo.autograd.grad_check import compare_gradients
from neutron_mojo.autograd.checkpoint import gradients_match


def loss(x0: Float64, x1: Float64, g0: Float64, g1: Float64) -> Float64:
    var rms = sqrt((x0*x0 + x1*x1)/2 + 0.000001)
    return (0.5*g0*x0 - 0.7*g1*x1)/rms


def test_norm_derivatives() raises:
    var tape = Tape()
    var x = tape.add_variable_from_shape(Shape(2))
    var gamma = tape.add_variable_from_shape(Shape(2))
    var y = tape.add_variable_from_shape(Shape(2))
    tape.set_data(x, 0, 1)
    tape.set_data(x, 1, -2)
    tape.set_data(gamma, 0, 2)
    tape.set_data(gamma, 1, 3)
    tape.set_grad(y, 0, 0.5)
    tape.set_grad(y, 1, -0.7)
    var entry = TapeEntry(OP_RMSNORM(), x, gamma, y, cached_scalar=0.000001, cached_int=2)
    _backward_rmsnorm(tape, entry)
    var h = Float64(0.0001)
    var dx0 = (loss(1+h,-2,2,3)-loss(1-h,-2,2,3))/(2*h)
    var dx1 = (loss(1,-2+h,2,3)-loss(1,-2-h,2,3))/(2*h)
    var dg0 = (loss(1,-2,2+h,3)-loss(1,-2,2-h,3))/(2*h)
    var dg1 = (loss(1,-2,2,3+h)-loss(1,-2,2,3-h))/(2*h)
    assert_true(abs(Float64(tape.get_grad(x,0))-dx0) < 0.00001)
    assert_true(abs(Float64(tape.get_grad(x,1))-dx1) < 0.00001)
    assert_true(abs(Float64(tape.get_grad(gamma,0))-dg0) < 0.00001)
    assert_true(abs(Float64(tape.get_grad(gamma,1))-dg1) < 0.00001)
    print("MJ-K04: non-unit gamma full input/gamma finite differences PASS")


def main() raises:
    test_norm_derivatives()
    var a = Tensor[DType.float32].full(Shape(2), 1000)
    var b = Tensor[DType.float32].full(Shape(2), 1000.5)
    assert_true(compare_gradients(a,b).passed)
    var short = Tensor[DType.float32].full(Shape(1), 1000)
    assert_false(compare_gradients(a,short).passed)
    assert_false(compare_gradients(a,b,rtol=-1).passed)
    var nan = _u32_to_f32(0x7fc00000)
    a.set(0,nan)
    assert_false(compare_gradients(a,b).passed)
    b.set(0,nan)
    assert_false(compare_gradients(a,b).passed)
    var ta = Tape()
    var tb = Tape()
    var va = ta.add_variable_from_shape(Shape(1))
    var vb = tb.add_variable_from_shape(Shape(1))
    ta.set_grad(va,0,nan)
    tb.set_grad(vb,0,nan)
    assert_false(gradients_match(ta,tb,0))
    assert_false(gradients_match(ta,tb,-1))
    assert_false(gradients_match(ta,tb,0,atol=-1))
    print("MJ-K10: scale-relative/mismatch/NaN/tolerance/index PASS")
    assert_false(can_cast(DType.float16,DType.bfloat16))
    assert_false(can_cast(DType.bfloat16,DType.float16))
    assert_true(can_cast(DType.float16,DType.float32))
    assert_true(can_cast(DType.bfloat16,DType.float32))
    print("MJ-K18: incomparable half/bfloat16 formats PASS")
