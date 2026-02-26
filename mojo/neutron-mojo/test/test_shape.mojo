# ===----------------------------------------------------------------------=== #
# Tests for tensor/shape.mojo — Shape broadcasting, strides, numel
# ===----------------------------------------------------------------------=== #

"""Tests: broadcast, reshape, strides, numel, negative indexing."""

from testing import assert_true, assert_false, assert_equal

from neutron_mojo.tensor.shape import Shape


# --- Basic properties ---


fn test_ndim() raises:
    var s = Shape(2, 3, 4)
    assert_equal(s.ndim(), 3)
    print("  ndim: PASS")


fn test_numel() raises:
    var s = Shape(2, 3, 4)
    assert_equal(s.numel(), 24)
    print("  numel: PASS")


fn test_numel_scalar() raises:
    var s = Shape(1)
    assert_equal(s.numel(), 1)
    print("  numel_scalar: PASS")


# --- Indexing ---


fn test_getitem_positive() raises:
    var s = Shape(2, 3, 4)
    assert_equal(s[0], 2)
    assert_equal(s[1], 3)
    assert_equal(s[2], 4)
    print("  getitem_positive: PASS")


fn test_getitem_negative() raises:
    var s = Shape(2, 3, 4)
    assert_equal(s[-1], 4)
    assert_equal(s[-2], 3)
    assert_equal(s[-3], 2)
    print("  getitem_negative: PASS")


# --- Strides ---


fn test_strides_2d() raises:
    var s = Shape(3, 4)
    var strides = s.strides()
    assert_equal(strides[0], 4)  # skip 4 elements per row
    assert_equal(strides[1], 1)  # last dim stride = 1
    print("  strides_2d: PASS")


fn test_strides_3d() raises:
    var s = Shape(2, 3, 4)
    var strides = s.strides()
    assert_equal(strides[0], 12)  # 3 * 4
    assert_equal(strides[1], 4)   # 4
    assert_equal(strides[2], 1)
    print("  strides_3d: PASS")


fn test_strides_1d() raises:
    var s = Shape(5)
    var strides = s.strides()
    assert_equal(strides[0], 1)
    print("  strides_1d: PASS")


# --- Broadcasting ---


fn test_broadcast_same_shape() raises:
    var a = Shape(3, 4, 5)
    var b = Shape(3, 4, 5)
    var c = a.broadcast_with(b)
    assert_true(c == Shape(3, 4, 5))
    print("  broadcast_same_shape: PASS")


fn test_broadcast_classic() raises:
    """(3, 1, 5) broadcast with (1, 4, 5) = (3, 4, 5)."""
    var a = Shape(3, 1, 5)
    var b = Shape(1, 4, 5)
    var c = a.broadcast_with(b)
    assert_true(c == Shape(3, 4, 5))
    print("  broadcast_classic: PASS")


fn test_broadcast_different_ndim() raises:
    """(5,) broadcast with (3, 5) = (3, 5)."""
    var a = Shape(5)
    var b = Shape(3, 5)
    var c = a.broadcast_with(b)
    assert_true(c == Shape(3, 5))
    print("  broadcast_different_ndim: PASS")


fn test_broadcast_scalar() raises:
    """(1,) broadcast with (4, 5) = (4, 5)."""
    var a = Shape(1)
    var b = Shape(4, 5)
    var c = a.broadcast_with(b)
    assert_true(c == Shape(4, 5))
    print("  broadcast_scalar: PASS")


fn test_broadcast_incompatible() raises:
    """(3, 4) broadcast with (3, 5) should raise."""
    var a = Shape(3, 4)
    var b = Shape(3, 5)
    var raised = False
    try:
        _ = a.broadcast_with(b)
    except:
        raised = True
    assert_true(raised)
    print("  broadcast_incompatible: PASS")


# --- Reshape ---


fn test_reshape_valid() raises:
    var a = Shape(2, 3, 4)
    var b = Shape(6, 4)
    assert_true(a.reshape_valid(b))
    print("  reshape_valid: PASS")


fn test_reshape_invalid() raises:
    var a = Shape(2, 3, 4)
    var b = Shape(5, 5)  # 25 != 24
    assert_false(a.reshape_valid(b))
    print("  reshape_invalid: PASS")


# --- Equality ---


fn test_equality() raises:
    assert_true(Shape(2, 3) == Shape(2, 3))
    assert_false(Shape(2, 3) == Shape(3, 2))
    assert_false(Shape(2, 3) == Shape(2, 3, 1))
    print("  equality: PASS")


# --- Writable ---


fn test_writable() raises:
    var s = String(Shape(2, 3, 4))
    assert_true("2" in s)
    assert_true("3" in s)
    assert_true("4" in s)
    print("  writable: PASS")


# --- Main ---


fn main() raises:
    print("test_shape:")
    test_ndim()
    test_numel()
    test_numel_scalar()
    test_getitem_positive()
    test_getitem_negative()
    test_strides_2d()
    test_strides_3d()
    test_strides_1d()
    test_broadcast_same_shape()
    test_broadcast_classic()
    test_broadcast_different_ndim()
    test_broadcast_scalar()
    test_broadcast_incompatible()
    test_reshape_valid()
    test_reshape_invalid()
    test_equality()
    test_writable()
    print("ALL PASSED")
