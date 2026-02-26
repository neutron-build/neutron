# Test batched matmul (3D tensors)

from neutron_mojo.tensor import Tensor, matmul
from math import abs

fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-5) raises:
    var diff = abs(Float64(a) - Float64(b))
    if diff > rtol * abs(Float64(b)) + 1e-6:
        raise Error("Not close: " + String(a) + " vs " + String(b))

fn main() raises:
    print("test_batched_matmul:")

    # Create batched input: batch=2, each with 2x2 matrices
    # Batch 0: A0 = [[1, 2], [3, 4]], B0 = [[5, 6], [7, 8]]
    # Batch 1: A1 = [[2, 3], [4, 5]], B1 = [[6, 7], [8, 9]]
    var A = Tensor[DType.float32](2, 2, 2)
    # Batch 0
    A.data_ptr().store(0, Float32(1.0))
    A.data_ptr().store(1, Float32(2.0))
    A.data_ptr().store(2, Float32(3.0))
    A.data_ptr().store(3, Float32(4.0))
    # Batch 1
    A.data_ptr().store(4, Float32(2.0))
    A.data_ptr().store(5, Float32(3.0))
    A.data_ptr().store(6, Float32(4.0))
    A.data_ptr().store(7, Float32(5.0))

    var B = Tensor[DType.float32](2, 2, 2)
    # Batch 0
    B.data_ptr().store(0, Float32(5.0))
    B.data_ptr().store(1, Float32(6.0))
    B.data_ptr().store(2, Float32(7.0))
    B.data_ptr().store(3, Float32(8.0))
    # Batch 1
    B.data_ptr().store(4, Float32(6.0))
    B.data_ptr().store(5, Float32(7.0))
    B.data_ptr().store(6, Float32(8.0))
    B.data_ptr().store(7, Float32(9.0))

    # Test batched matmul
    var C = matmul(A, B)

    # Batch 0: [[1,2],[3,4]] @ [[5,6],[7,8]] = [[19,22],[43,50]]
    assert_close(C.get(0, 0, 0), Float32(19.0))  # 1*5 + 2*7
    assert_close(C.get(0, 0, 1), Float32(22.0))  # 1*6 + 2*8
    assert_close(C.get(0, 1, 0), Float32(43.0))  # 3*5 + 4*7
    assert_close(C.get(0, 1, 1), Float32(50.0))  # 3*6 + 4*8

    # Batch 1: [[2,3],[4,5]] @ [[6,7],[8,9]] = [[36,41],[64,73]]
    assert_close(C.get(1, 0, 0), Float32(36.0))  # 2*6 + 3*8
    assert_close(C.get(1, 0, 1), Float32(41.0))  # 2*7 + 3*9
    assert_close(C.get(1, 1, 0), Float32(64.0))  # 4*6 + 5*8
    assert_close(C.get(1, 1, 1), Float32(73.0))  # 4*7 + 5*9

    print("  batched matmul: PASS")

    print("ALL PASSED")
