# Test transpose-aware matmul

from neutron_mojo.tensor import Tensor, matmul
from math import abs

fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-5) raises:
    var diff = abs(Float64(a) - Float64(b))
    if diff > rtol * abs(Float64(b)) + 1e-6:
        raise Error("Not close: " + String(a) + " vs " + String(b))

fn main() raises:
    print("test_matmul_transpose:")

    # Test data: A = [[1, 2], [3, 4]]  (2x2)
    #            B = [[5, 6], [7, 8]]  (2x2)
    var A = Tensor[DType.float32](2, 2)
    A.data_ptr().store(0, Float32(1.0))
    A.data_ptr().store(1, Float32(2.0))
    A.data_ptr().store(2, Float32(3.0))
    A.data_ptr().store(3, Float32(4.0))

    var B = Tensor[DType.float32](2, 2)
    B.data_ptr().store(0, Float32(5.0))
    B.data_ptr().store(1, Float32(6.0))
    B.data_ptr().store(2, Float32(7.0))
    B.data_ptr().store(3, Float32(8.0))

    # Test 1: Regular matmul A @ B = [[19, 22], [43, 50]]
    var C1 = matmul(A, B, transpose_a=False, transpose_b=False)
    assert_close(C1.get(0, 0), Float32(19.0))  # 1*5 + 2*7
    assert_close(C1.get(0, 1), Float32(22.0))  # 1*6 + 2*8
    assert_close(C1.get(1, 0), Float32(43.0))  # 3*5 + 4*7
    assert_close(C1.get(1, 1), Float32(50.0))  # 3*6 + 4*8
    print("  regular matmul: PASS")

    # Test 2: A^T @ B where A^T = [[1, 3], [2, 4]]
    #         A^T @ B = [[26, 30], [38, 44]]
    var C2 = matmul(A, B, transpose_a=True, transpose_b=False)
    assert_close(C2.get(0, 0), Float32(26.0))  # 1*5 + 3*7
    assert_close(C2.get(0, 1), Float32(30.0))  # 1*6 + 3*8
    assert_close(C2.get(1, 0), Float32(38.0))  # 2*5 + 4*7
    assert_close(C2.get(1, 1), Float32(44.0))  # 2*6 + 4*8
    print("  transpose_a: PASS")

    # Test 3: A @ B^T where B^T = [[5, 7], [6, 8]]
    #         A @ B^T = [[17, 23], [39, 53]]
    var C3 = matmul(A, B, transpose_a=False, transpose_b=True)
    assert_close(C3.get(0, 0), Float32(17.0))  # 1*5 + 2*6
    assert_close(C3.get(0, 1), Float32(23.0))  # 1*7 + 2*8
    assert_close(C3.get(1, 0), Float32(39.0))  # 3*5 + 4*6
    assert_close(C3.get(1, 1), Float32(53.0))  # 3*7 + 4*8
    print("  transpose_b: PASS")

    # Test 4: A^T @ B^T where A^T = [[1,3],[2,4]], B^T = [[5,7],[6,8]]
    #         A^T @ B^T = [[23, 31], [34, 46]]
    var C4 = matmul(A, B, transpose_a=True, transpose_b=True)
    assert_close(C4.get(0, 0), Float32(23.0))  # 1*5 + 3*6
    assert_close(C4.get(0, 1), Float32(31.0))  # 1*7 + 3*8
    assert_close(C4.get(1, 0), Float32(34.0))  # 2*5 + 4*6
    assert_close(C4.get(1, 1), Float32(46.0))  # 2*7 + 4*8
    print("  transpose_a and transpose_b: PASS")

    print("ALL PASSED")
