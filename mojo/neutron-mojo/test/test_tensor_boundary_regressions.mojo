"""MJ-K11: pointer-span refusals and legal empty slices."""
from std.testing import assert_true, assert_equal
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import gather, softmax, reduce_max, argmax_tensor, argmin_tensor, argmax_axis
from neutron_mojo.tensor.shape_ops import expand, concat2, split2, arange
from neutron_mojo.tensor.view import TensorView


def main() raises:
    var x = Tensor[DType.float32].ones(Shape(2))
    var rejected = False
    try:
        var y = expand(x,Shape(3))
    except:
        rejected = True
    assert_true(rejected)
    var bad: List[Int] = [-1,2]
    rejected = False
    try:
        var y = gather(x,0,bad)
    except:
        rejected = True
    assert_true(rejected)
    var good: List[Int] = [0]
    rejected = False
    try:
        var y = gather(x,1,good)
    except:
        rejected = True
    assert_true(rejected)
    var view = x.view()
    rejected = False
    try:
        var y = view.slice_dim(0,-1,1)
    except:
        rejected = True
    assert_true(rejected)
    rejected = False
    try:
        var y = view.slice_dim(0,1,2)
    except:
        rejected = True
    assert_true(rejected)
    var legal = view.slice_dim(0,2,0)
    assert_equal(legal.numel(),0)
    var empty = Tensor[DType.float32](Shape(0))
    rejected = False
    try:
        var y = softmax(empty)
    except:
        rejected = True
    assert_true(rejected)
    rejected = False
    try:
        var y = reduce_max(empty)
    except:
        rejected = True
    assert_true(rejected)
    rejected = False
    try:
        var y = softmax(x,3)
    except:
        rejected = True
    assert_true(rejected)
    _ = x.get(0)
    test_extended_admission()
    print("MJ-K11: expansion/gather/slice/empty reduction/axis/stride/shape admission PASS")


def test_extended_admission() raises:
    for case_idx in range(3):
        var refused = False
        try:
            if case_idx == 0:
                var bad = Shape(-1)
            elif case_idx == 1:
                var bad = Shape(0x7FFFFFFFFFFFFFFF,2)
            else:
                # A zero leading axis does not erase overflowing suffix strides.
                var bad = Shape(0,0x7FFFFFFFFFFFFFFF,2)
        except:
            refused = True
        assert_true(refused)
    var legal_zero = Shape(0x7FFFFFFFFFFFFFFF,0,2)
    assert_equal(legal_zero.numel(),0)
    var x = Tensor[DType.float32].ones(Shape(2))
    var view = x.view()
    for case_idx in range(5):
        var refused = False
        try:
            if case_idx == 0:
                var bad = view.transpose(0,1)
            elif case_idx == 1:
                var bad = view.broadcast_to(Shape())
            elif case_idx == 2:
                var bad = TensorView[DType.float32](x.data_ptr(),Shape(2),[0x7FFFFFFFFFFFFFFF])
            elif case_idx == 3:
                var bad = concat2(x,x,1)
            else:
                var bad = split2(x,1,1)
        except:
            refused = True
        assert_true(refused)
    var empty = Tensor[DType.float32](Shape(0))
    for case_idx in range(3):
        var refused = False
        try:
            if case_idx == 0:
                var bad = argmax_tensor(empty)
            elif case_idx == 1:
                var bad = argmin_tensor(empty)
            else:
                var two_d = Tensor[DType.float32](Shape(0,2))
                var bad = argmax_axis(two_d,0)
        except:
            refused = True
        assert_true(refused)
    var range_values = arange[DType.float32](0,1,0.25)
    assert_equal(range_values.numel(),4)
    for i in range(4):
        assert_equal(range_values.get(i),Float32(i)*0.25)
    var refused = False
    try:
        var huge = arange[DType.float32](0,1e30,1)
    except:
        refused = True
    assert_true(refused)
