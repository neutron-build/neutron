# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Shape & creation ops tests
# ===----------------------------------------------------------------------=== #

"""Tests for concat, split, squeeze, unsqueeze, flatten, expand, arange,
linspace, eye, tril, triu, sort, argsort."""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.shape_ops import (
    concat2, concat3, concat4,
    split2, split3,
    squeeze, unsqueeze, flatten, expand,
    arange, linspace, eye, tril, triu,
    sort, argsort,
    SplitResult2, SplitResult3, SortResult,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-5, atol: Float64 = 1e-6) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error(
            "Values not close: " + String(a) + " vs " + String(b) + " (diff=" + String(diff) + ")"
        )


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn _idx2(r: Int, c: Int) -> List[Int]:
    var l = List[Int]()
    l.append(r)
    l.append(c)
    return l^


fn test_concat2_1d() raises:
    """Concatenate two 1D tensors."""
    var a = Tensor[DType.float32](3)
    a.set(0, 1.0)
    a.set(1, 2.0)
    a.set(2, 3.0)
    var b = Tensor[DType.float32](2)
    b.set(0, 4.0)
    b.set(1, 5.0)
    var out = concat2(a, b)
    assert_eq(out.numel(), 5)
    assert_close(out.get(0), 1.0)
    assert_close(out.get(3), 4.0)
    assert_close(out.get(4), 5.0)
    print("  concat2_1d: PASS")


fn test_concat2_2d_dim0() raises:
    """Concatenate two 2D tensors along rows."""
    var a = Tensor[DType.float32](2, 3)
    a.set(_idx2(0, 0), Float32(1.0))
    a.set(_idx2(1, 2), Float32(6.0))
    var b = Tensor[DType.float32](1, 3)
    b.set(_idx2(0, 0), Float32(7.0))
    var out = concat2(a, b, dim=0)
    assert_eq(out.shape()[0], 3)
    assert_eq(out.shape()[1], 3)
    assert_close(out.get(0, 0), 1.0)
    assert_close(out.get(2, 0), 7.0)
    print("  concat2_2d_dim0: PASS")


fn test_concat3() raises:
    """Concatenate three 1D tensors."""
    var a = Tensor[DType.float32](2)
    a.set(0, 1.0)
    a.set(1, 2.0)
    var b = Tensor[DType.float32](1)
    b.set(0, 3.0)
    var c = Tensor[DType.float32](2)
    c.set(0, 4.0)
    c.set(1, 5.0)
    var out = concat3(a, b, c)
    assert_eq(out.numel(), 5)
    assert_close(out.get(2), 3.0)
    assert_close(out.get(4), 5.0)
    print("  concat3: PASS")


fn test_split2_1d() raises:
    """Split a 1D tensor into two parts."""
    var x = Tensor[DType.float32](5)
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 3.0)
    x.set(3, 4.0)
    x.set(4, 5.0)
    var sr = split2(x, 3)
    assert_eq(sr.part0.numel(), 3)
    assert_eq(sr.part1.numel(), 2)
    assert_close(sr.part0.get(2), 3.0)
    assert_close(sr.part1.get(0), 4.0)
    print("  split2_1d: PASS")


fn test_split2_2d() raises:
    """Split a 2D tensor along rows."""
    var x = Tensor[DType.float32](4, 2)
    for i in range(8):
        x.set(i, Float32(i + 1))
    var sr = split2(x, 2, dim=0)
    assert_eq(sr.part0.shape()[0], 2)
    assert_eq(sr.part1.shape()[0], 2)
    assert_close(sr.part0.get(0, 0), 1.0)
    assert_close(sr.part1.get(0, 0), 5.0)
    print("  split2_2d: PASS")


fn test_squeeze() raises:
    """Remove size-1 dimension."""
    var x = Tensor[DType.float32](3, 1)
    x.set(_idx2(0, 0), Float32(1.0))
    x.set(_idx2(1, 0), Float32(2.0))
    x.set(_idx2(2, 0), Float32(3.0))
    var out = squeeze(x, 1)
    assert_eq(out.ndim(), 1)
    assert_eq(out.numel(), 3)
    assert_close(out.get(0), 1.0)
    assert_close(out.get(2), 3.0)
    print("  squeeze: PASS")


fn test_unsqueeze() raises:
    """Insert size-1 dimension."""
    var x = Tensor[DType.float32](3)
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 3.0)
    var out = unsqueeze(x, 0)
    assert_eq(out.ndim(), 2)
    assert_eq(out.shape()[0], 1)
    assert_eq(out.shape()[1], 3)
    assert_close(out.get(0, 0), 1.0)
    print("  unsqueeze: PASS")


fn test_flatten() raises:
    """Flatten 2D to 1D."""
    var x = Tensor[DType.float32](2, 3)
    for i in range(6):
        x.set(i, Float32(i + 1))
    var out = flatten(x)
    assert_eq(out.ndim(), 1)
    assert_eq(out.numel(), 6)
    assert_close(out.get(0), 1.0)
    assert_close(out.get(5), 6.0)
    print("  flatten: PASS")


fn test_arange() raises:
    """Create range tensor."""
    var out = arange[DType.float32](0.0, 5.0, 1.0)
    assert_eq(out.numel(), 5)
    assert_close(out.get(0), 0.0)
    assert_close(out.get(4), 4.0)
    print("  arange: PASS")


fn test_linspace() raises:
    """Create linspace tensor."""
    var out = linspace[DType.float32](0.0, 1.0, 5)
    assert_eq(out.numel(), 5)
    assert_close(out.get(0), 0.0)
    assert_close(out.get(2), 0.5)
    assert_close(out.get(4), 1.0)
    print("  linspace: PASS")


fn test_eye() raises:
    """Create identity matrix."""
    var out = eye[DType.float32](3)
    assert_eq(out.shape()[0], 3)
    assert_eq(out.shape()[1], 3)
    assert_close(out.get(0, 0), 1.0)
    assert_close(out.get(0, 1), 0.0)
    assert_close(out.get(1, 1), 1.0)
    assert_close(out.get(2, 2), 1.0)
    print("  eye: PASS")


fn test_tril() raises:
    """Lower triangular mask."""
    var x = Tensor[DType.float32].ones(Shape(3, 3))
    var out = tril(x)
    assert_close(out.get(0, 0), 1.0)
    assert_close(out.get(0, 1), 0.0)
    assert_close(out.get(1, 0), 1.0)
    assert_close(out.get(1, 1), 1.0)
    assert_close(out.get(1, 2), 0.0)
    assert_close(out.get(2, 2), 1.0)
    print("  tril: PASS")


fn test_triu() raises:
    """Upper triangular mask."""
    var x = Tensor[DType.float32].ones(Shape(3, 3))
    var out = triu(x)
    assert_close(out.get(0, 0), 1.0)
    assert_close(out.get(1, 0), 0.0)
    assert_close(out.get(0, 1), 1.0)
    assert_close(out.get(2, 2), 1.0)
    assert_close(out.get(2, 1), 0.0)
    print("  triu: PASS")


fn test_sort_ascending() raises:
    """Sort ascending."""
    var x = Tensor[DType.float32](5)
    x.set(0, 3.0)
    x.set(1, 1.0)
    x.set(2, 4.0)
    x.set(3, 1.5)
    x.set(4, 2.0)
    var sr = sort(x)
    assert_close(sr.values.get(0), 1.0)
    assert_close(sr.values.get(1), 1.5)
    assert_close(sr.values.get(4), 4.0)
    assert_eq(sr.indices[0], 1)  # original index of smallest
    print("  sort_ascending: PASS")


fn test_sort_descending() raises:
    """Sort descending."""
    var x = Tensor[DType.float32](4)
    x.set(0, 1.0)
    x.set(1, 4.0)
    x.set(2, 2.0)
    x.set(3, 3.0)
    var sr = sort(x, descending=True)
    assert_close(sr.values.get(0), 4.0)
    assert_close(sr.values.get(3), 1.0)
    print("  sort_descending: PASS")


fn test_argsort() raises:
    """Argsort returns indices."""
    var x = Tensor[DType.float32](3)
    x.set(0, 3.0)
    x.set(1, 1.0)
    x.set(2, 2.0)
    var idx = argsort(x)
    assert_eq(idx[0], 1)
    assert_eq(idx[1], 2)
    assert_eq(idx[2], 0)
    print("  argsort: PASS")


fn main() raises:
    print("test_shape_ops:")
    test_concat2_1d()
    test_concat2_2d_dim0()
    test_concat3()
    test_split2_1d()
    test_split2_2d()
    test_squeeze()
    test_unsqueeze()
    test_flatten()
    test_arange()
    test_linspace()
    test_eye()
    test_tril()
    test_triu()
    test_sort_ascending()
    test_sort_descending()
    test_argsort()
    print("ALL PASSED (16 tests)")
