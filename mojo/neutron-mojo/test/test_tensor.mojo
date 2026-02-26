# ===----------------------------------------------------------------------=== #
# Tests for tensor/storage.mojo, tensor/view.mojo, tensor/tensor.mojo
# ===----------------------------------------------------------------------=== #

"""Tests: Storage alloc/free, view slicing/transpose, tensor CRUD, factories."""

from testing import assert_true, assert_false, assert_equal

from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.storage import Storage, DeviceKind
from neutron_mojo.tensor.view import TensorView
from neutron_mojo.tensor.tensor import Tensor


# ===----------------------------------------------------------------------=== #
# Storage tests
# ===----------------------------------------------------------------------=== #


fn test_storage_alloc_and_zero() raises:
    """Storage should be zero-initialized."""
    var s = Storage[DType.float32](10)
    for i in range(10):
        assert_equal(s.load(i), Float32(0))
    assert_equal(s.size(), 10)
    assert_equal(s.size_bytes(), 40)  # 10 * 4 bytes
    print("  storage_alloc_and_zero: PASS")


fn test_storage_load_store() raises:
    var s = Storage[DType.float32](5)
    s.store(0, Float32(1.0))
    s.store(2, Float32(3.14))
    s.store(4, Float32(-2.5))
    assert_equal(s.load(0), Float32(1.0))
    assert_equal(s.load(2), Float32(3.14))
    assert_equal(s.load(4), Float32(-2.5))
    print("  storage_load_store: PASS")


fn test_storage_fill() raises:
    var s = Storage[DType.float32](8)
    s.fill(Float32(42.0))
    for i in range(8):
        assert_equal(s.load(i), Float32(42.0))
    print("  storage_fill: PASS")


fn test_storage_copy_from() raises:
    var src = Storage[DType.float32](4)
    src.store(0, Float32(1.0))
    src.store(1, Float32(2.0))
    src.store(2, Float32(3.0))
    src.store(3, Float32(4.0))

    var dst = Storage[DType.float32](4)
    dst.copy_from(src)

    for i in range(4):
        assert_equal(dst.load(i), src.load(i))
    print("  storage_copy_from: PASS")


fn test_storage_simd() raises:
    var s = Storage[DType.float32](8)
    var vec = SIMD[DType.float32, 4](1.0, 2.0, 3.0, 4.0)
    s.store_simd[4](0, vec)
    var loaded = s.load_simd[4](0)
    assert_equal(loaded[0], Float32(1.0))
    assert_equal(loaded[1], Float32(2.0))
    assert_equal(loaded[2], Float32(3.0))
    assert_equal(loaded[3], Float32(4.0))
    print("  storage_simd: PASS")


fn test_storage_device() raises:
    var s = Storage[DType.float32](4)
    assert_true(s.device() == DeviceKind.CPU)
    print("  storage_device: PASS")


fn test_device_kind_writable() raises:
    var cpu = String(DeviceKind.CPU)
    var cuda = String(DeviceKind.CUDA)
    var rocm = String(DeviceKind.ROCm)
    var metal = String(DeviceKind.Metal)
    assert_true("CPU" in cpu)
    assert_true("CUDA" in cuda)
    assert_true("ROCm" in rocm)
    assert_true("Metal" in metal)
    print("  device_kind_writable: PASS")


# ===----------------------------------------------------------------------=== #
# TensorView tests
# ===----------------------------------------------------------------------=== #


fn test_view_contiguous() raises:
    var s = Storage[DType.float32](12)
    for i in range(12):
        s.store(i, Float32(i))

    var shape = Shape(3, 4)
    var v = TensorView[DType.float32](s.unsafe_ptr(), shape)
    assert_true(v.is_contiguous())
    assert_equal(v.ndim(), 2)
    assert_equal(v.numel(), 12)

    # Check element access: v[1, 2] = row 1 * 4 + col 2 = 6
    assert_equal(v.load(1, 2), Float32(6))
    _ = s.load(0)  # keepalive: prevent early destruction of Storage
    print("  view_contiguous: PASS")


fn test_view_transpose() raises:
    var s = Storage[DType.float32](6)
    for i in range(6):
        s.store(i, Float32(i))

    var shape = Shape(2, 3)
    var v = TensorView[DType.float32](s.unsafe_ptr(), shape)

    # Transpose: (2,3) -> (3,2)
    var vt = v.transpose(0, 1)
    assert_equal(vt.shape[0], 3)
    assert_equal(vt.shape[1], 2)
    assert_false(vt.is_contiguous())

    # v[0,1] = 1, vt[1,0] should also be 1
    assert_equal(vt.load(1, 0), Float32(1))
    # v[1,2] = 5, vt[2,1] should also be 5
    assert_equal(vt.load(2, 1), Float32(5))
    _ = s.load(0)  # keepalive: prevent early destruction of Storage
    print("  view_transpose: PASS")


fn test_view_slice_dim() raises:
    var s = Storage[DType.float32](12)
    for i in range(12):
        s.store(i, Float32(i))

    var shape = Shape(3, 4)
    var v = TensorView[DType.float32](s.unsafe_ptr(), shape)

    # Slice row 1 (start=1, length=1)
    var sliced = v.slice_dim(0, 1, 1)
    assert_equal(sliced.shape[0], 1)
    assert_equal(sliced.shape[1], 4)
    # sliced[0, 0] should be v[1, 0] = 4
    assert_equal(sliced.load(0, 0), Float32(4))
    # sliced[0, 3] should be v[1, 3] = 7
    assert_equal(sliced.load(0, 3), Float32(7))
    _ = s.load(0)  # keepalive
    print("  view_slice_dim: PASS")


fn test_view_broadcast() raises:
    var s = Storage[DType.float32](3)
    s.store(0, Float32(10.0))
    s.store(1, Float32(20.0))
    s.store(2, Float32(30.0))

    # Shape (1, 3), broadcast to (4, 3)
    var v = TensorView[DType.float32](s.unsafe_ptr(), Shape(1, 3))
    var bv = v.broadcast_to(Shape(4, 3))
    assert_equal(bv.shape[0], 4)
    assert_equal(bv.shape[1], 3)

    # All rows should see the same data
    for row in range(4):
        assert_equal(bv.load(row, 0), Float32(10.0))
        assert_equal(bv.load(row, 1), Float32(20.0))
        assert_equal(bv.load(row, 2), Float32(30.0))

    _ = s.load(0)  # keepalive
    print("  view_broadcast: PASS")


fn test_view_reshape() raises:
    var s = Storage[DType.float32](12)
    for i in range(12):
        s.store(i, Float32(i))

    var v = TensorView[DType.float32](s.unsafe_ptr(), Shape(3, 4))
    var reshaped = v.reshape(Shape(6, 2))
    assert_equal(reshaped.shape[0], 6)
    assert_equal(reshaped.shape[1], 2)
    assert_true(reshaped.is_contiguous())

    # Element order preserved: reshaped[0,0] = 0, reshaped[0,1] = 1
    assert_equal(reshaped.load(0, 0), Float32(0))
    assert_equal(reshaped.load(0, 1), Float32(1))
    assert_equal(reshaped.load(5, 1), Float32(11))
    _ = s.load(0)  # keepalive
    print("  view_reshape: PASS")


# ===----------------------------------------------------------------------=== #
# Tensor tests
# ===----------------------------------------------------------------------=== #


fn test_tensor_zeros() raises:
    var t = Tensor[DType.float32].zeros(Shape(2, 3))
    assert_equal(t.ndim(), 2)
    assert_equal(t.numel(), 6)
    for i in range(6):
        assert_equal(t.data_ptr().load(i), Float32(0))
    print("  tensor_zeros: PASS")


fn test_tensor_ones() raises:
    var t = Tensor[DType.float32].ones(Shape(3, 4))
    assert_equal(t.numel(), 12)
    for i in range(12):
        assert_equal(t.data_ptr().load(i), Float32(1.0))
    print("  tensor_ones: PASS")


fn test_tensor_full() raises:
    var t = Tensor[DType.float32].full(Shape(2, 2), Float32(3.14))
    for i in range(4):
        assert_equal(t.data_ptr().load(i), Float32(3.14))
    print("  tensor_full: PASS")


fn test_tensor_get_set() raises:
    var t = Tensor[DType.float32](3, 4)
    var indices = List[Int]()
    indices.append(1)
    indices.append(2)
    t.set(indices, Float32(42.0))
    assert_equal(t.get(1, 2), Float32(42.0))
    print("  tensor_get_set: PASS")


fn test_tensor_rand() raises:
    var t = Tensor[DType.float32].rand(Shape(100))
    # All values should be in [0, 1)
    var ptr = t.data_ptr()
    for i in range(100):
        var v = ptr.load(i)
        assert_true(v >= Float32(0.0))
        assert_true(v < Float32(1.0))
    print("  tensor_rand: PASS")


fn test_tensor_simd_load_store() raises:
    var t = Tensor[DType.float32](8)
    var vec = SIMD[DType.float32, 4](10.0, 20.0, 30.0, 40.0)
    t.store_simd[4](0, vec)
    var loaded = t.load_simd[4](0)
    assert_equal(loaded[0], Float32(10.0))
    assert_equal(loaded[3], Float32(40.0))
    print("  tensor_simd_load_store: PASS")


fn test_tensor_shape_properties() raises:
    var t = Tensor[DType.float32](2, 3, 4)
    assert_equal(t.ndim(), 3)
    assert_equal(t.numel(), 24)
    assert_equal(t.shape()[0], 2)
    assert_equal(t.shape()[1], 3)
    assert_equal(t.shape()[2], 4)
    assert_true(t.is_contiguous())
    assert_true(t.device() == DeviceKind.CPU)
    print("  tensor_shape_properties: PASS")


fn test_tensor_transpose_view() raises:
    var t = Tensor[DType.float32](2, 3)
    # Fill: row 0 = [0,1,2], row 1 = [3,4,5]
    for i in range(6):
        t.data_ptr().store(i, Float32(i))

    var vt = t.transpose(0, 1)
    assert_equal(vt.shape[0], 3)
    assert_equal(vt.shape[1], 2)
    # vt[1, 0] = t[0, 1] = 1
    assert_equal(vt.load(1, 0), Float32(1))
    _ = t.numel()  # keepalive: Tensor owns Storage that TensorView borrows
    print("  tensor_transpose_view: PASS")


fn test_tensor_reshape_view() raises:
    var t = Tensor[DType.float32](3, 4)
    for i in range(12):
        t.data_ptr().store(i, Float32(i))

    var rv = t.reshape(Shape(6, 2))
    assert_equal(rv.shape[0], 6)
    assert_equal(rv.shape[1], 2)
    assert_equal(rv.load(5, 1), Float32(11))
    _ = t.numel()  # keepalive
    print("  tensor_reshape_view: PASS")


fn test_tensor_writable() raises:
    var t = Tensor[DType.float32](3)
    t.data_ptr().store(0, Float32(1.0))
    t.data_ptr().store(1, Float32(2.0))
    t.data_ptr().store(2, Float32(3.0))
    var s = String(t)
    assert_true("Tensor" in s)
    assert_true("float32" in s)
    print("  tensor_writable: PASS")


fn test_tensor_clone() raises:
    """Clone produces an independent deep copy."""
    var t = Tensor[DType.float32](3)
    t.data_ptr().store(0, Float32(1.0))
    t.data_ptr().store(1, Float32(2.0))
    t.data_ptr().store(2, Float32(3.0))

    var c = t.clone()
    # Same values
    assert_equal(c.data_ptr().load(0), Float32(1.0))
    assert_equal(c.data_ptr().load(1), Float32(2.0))
    assert_equal(c.data_ptr().load(2), Float32(3.0))
    # Independent storage: mutate original, clone unchanged
    t.data_ptr().store(0, Float32(99.0))
    assert_equal(c.get(0), Float32(1.0))
    assert_equal(t.get(0), Float32(99.0))
    _ = c.numel()  # keepalive
    _ = t.numel()  # keepalive
    print("  tensor_clone: PASS")


fn test_tensor_from_transposed_view() raises:
    """Tensor(view) materializes a transposed view into an owned contiguous tensor."""
    var t = Tensor[DType.float32](2, 3)
    # [[0, 1, 2], [3, 4, 5]]
    for i in range(6):
        t.data_ptr().store(i, Float32(i))

    var tv = t.transpose(0, 1)  # shape (3, 2), non-contiguous
    var materialized = Tensor[DType.float32](tv)

    # Shape should be (3, 2)
    assert_equal(materialized.shape()[0], 3)
    assert_equal(materialized.shape()[1], 2)
    assert_true(materialized.is_contiguous())

    # Transposed values: column-major read of original
    # materialized[0,0] = t[0,0] = 0, materialized[0,1] = t[1,0] = 3
    # materialized[1,0] = t[0,1] = 1, materialized[1,1] = t[1,1] = 4
    # materialized[2,0] = t[0,2] = 2, materialized[2,1] = t[1,2] = 5
    assert_equal(materialized.get(0, 0), Float32(0))
    assert_equal(materialized.get(0, 1), Float32(3))
    assert_equal(materialized.get(1, 0), Float32(1))
    assert_equal(materialized.get(1, 1), Float32(4))
    assert_equal(materialized.get(2, 0), Float32(2))
    assert_equal(materialized.get(2, 1), Float32(5))

    # Independent of original
    t.data_ptr().store(0, Float32(99.0))
    assert_equal(materialized.get(0, 0), Float32(0))
    _ = t.numel()  # keepalive
    print("  tensor_from_transposed_view: PASS")


# --- Main ---


fn main() raises:
    print("test_tensor:")

    # Storage
    test_storage_alloc_and_zero()
    test_storage_load_store()
    test_storage_fill()
    test_storage_copy_from()
    test_storage_simd()
    test_storage_device()
    test_device_kind_writable()

    # View
    test_view_contiguous()
    test_view_transpose()
    test_view_slice_dim()
    test_view_broadcast()
    test_view_reshape()

    # Tensor
    test_tensor_zeros()
    test_tensor_ones()
    test_tensor_full()
    test_tensor_get_set()
    test_tensor_rand()
    test_tensor_simd_load_store()
    test_tensor_shape_properties()
    test_tensor_transpose_view()
    test_tensor_reshape_view()
    test_tensor_writable()
    test_tensor_clone()
    test_tensor_from_transposed_view()

    print("ALL PASSED")
