# Quick test for new reduction ops: reduce_mean, sum_all, max_all

from neutron_mojo.tensor import Tensor, reduce_mean, sum_all, max_all
from math import abs

fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-5) raises:
    var diff = abs(Float64(a) - Float64(b))
    if diff > rtol * abs(Float64(b)) + 1e-6:
        raise Error("Not close: " + String(a) + " vs " + String(b))

fn main() raises:
    print("test_reductions_new:")

    # Test reduce_mean
    var x = Tensor[DType.float32](2, 3)
    x.data_ptr().store(0, Float32(1.0))
    x.data_ptr().store(1, Float32(2.0))
    x.data_ptr().store(2, Float32(3.0))
    x.data_ptr().store(3, Float32(4.0))
    x.data_ptr().store(4, Float32(5.0))
    x.data_ptr().store(5, Float32(6.0))

    # Mean along last axis: [mean([1,2,3]), mean([4,5,6])] = [2.0, 5.0]
    var mean_result = reduce_mean(x, axis=1)
    assert_close(mean_result.get(0, 0), Float32(2.0))
    assert_close(mean_result.get(1, 0), Float32(5.0))
    print("  reduce_mean: PASS")

    # Test sum_all
    var y = Tensor[DType.float32](3)
    y.data_ptr().store(0, Float32(1.0))
    y.data_ptr().store(1, Float32(2.0))
    y.data_ptr().store(2, Float32(3.0))
    var s = sum_all(y)
    assert_close(s, Float32(6.0))
    print("  sum_all: PASS")

    # Test max_all
    var z = Tensor[DType.float32](4)
    z.data_ptr().store(0, Float32(3.0))
    z.data_ptr().store(1, Float32(7.0))
    z.data_ptr().store(2, Float32(2.0))
    z.data_ptr().store(3, Float32(5.0))
    var m = max_all(z)
    assert_close(m, Float32(7.0))
    print("  max_all: PASS")

    print("ALL PASSED")
