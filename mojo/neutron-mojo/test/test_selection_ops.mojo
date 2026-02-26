# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Selection & indexing ops tests
# ===----------------------------------------------------------------------=== #

"""Tests for argmax, argmin, topk, where, gather, index_select, comparisons."""

from neutron_mojo.tensor import (
    Tensor, ArgResult, argmax_tensor, argmin_tensor, argmax_axis,
    topk, where_op, gather, index_select, eq, ne, gt, lt, ge, le,
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


fn test_argmax_basic() raises:
    var x = Tensor[DType.float32](5)
    x.set(0, 1.0)
    x.set(1, 5.0)
    x.set(2, 3.0)
    x.set(3, 2.0)
    x.set(4, 4.0)
    var r = argmax_tensor(x)
    assert_eq(r.index, 1)
    assert_close(Float32(r.value), 5.0)
    print("  argmax_basic: PASS")


fn test_argmin_basic() raises:
    var x = Tensor[DType.float32](5)
    x.set(0, 3.0)
    x.set(1, 1.0)
    x.set(2, 5.0)
    x.set(3, 0.5)
    x.set(4, 2.0)
    var r = argmin_tensor(x)
    assert_eq(r.index, 3)
    assert_close(Float32(r.value), 0.5)
    print("  argmin_basic: PASS")


fn test_argmax_axis_0() raises:
    """Argmax along axis 0 (rows) of 2D tensor."""
    var x = Tensor[DType.float32](3, 2)
    x.set(_idx2(0, 0), Float32(1.0))
    x.set(_idx2(0, 1), Float32(4.0))
    x.set(_idx2(1, 0), Float32(5.0))
    x.set(_idx2(1, 1), Float32(2.0))
    x.set(_idx2(2, 0), Float32(3.0))
    x.set(_idx2(2, 1), Float32(6.0))
    var out = argmax_axis(x, 0)
    # Col 0: max at row 1 (val 5), Col 1: max at row 2 (val 6)
    assert_close(out.get(0), 1.0)
    assert_close(out.get(1), 2.0)
    print("  argmax_axis_0: PASS")


fn test_argmax_axis_1() raises:
    """Argmax along axis 1 (cols) of 2D tensor."""
    var x = Tensor[DType.float32](2, 3)
    x.set(_idx2(0, 0), Float32(1.0))
    x.set(_idx2(0, 1), Float32(3.0))
    x.set(_idx2(0, 2), Float32(2.0))
    x.set(_idx2(1, 0), Float32(5.0))
    x.set(_idx2(1, 1), Float32(4.0))
    x.set(_idx2(1, 2), Float32(6.0))
    var out = argmax_axis(x, 1)
    assert_close(out.get(0), 1.0)  # Row 0: max at col 1
    assert_close(out.get(1), 2.0)  # Row 1: max at col 2
    print("  argmax_axis_1: PASS")


fn test_topk() raises:
    """Top-3 from 5-element tensor."""
    var x = Tensor[DType.float32](5)
    x.set(0, 1.0)
    x.set(1, 5.0)
    x.set(2, 3.0)
    x.set(3, 4.0)
    x.set(4, 2.0)
    var out = topk(x, 3)
    assert_close(out.get(0), 5.0)
    assert_close(out.get(1), 4.0)
    assert_close(out.get(2), 3.0)
    print("  topk: PASS")


fn test_where_op() raises:
    """Conditional select between two tensors."""
    var cond = Tensor[DType.float32](4)
    cond.set(0, 1.0)
    cond.set(1, 0.0)
    cond.set(2, 1.0)
    cond.set(3, -1.0)
    var x = Tensor[DType.float32](4)
    x.set(0, 10.0)
    x.set(1, 20.0)
    x.set(2, 30.0)
    x.set(3, 40.0)
    var y = Tensor[DType.float32](4)
    y.set(0, -10.0)
    y.set(1, -20.0)
    y.set(2, -30.0)
    y.set(3, -40.0)
    var out = where_op(cond, x, y)
    assert_close(out.get(0), 10.0)   # cond > 0 -> x
    assert_close(out.get(1), -20.0)  # cond == 0 -> y
    assert_close(out.get(2), 30.0)   # cond > 0 -> x
    assert_close(out.get(3), -40.0)  # cond < 0 -> y
    print("  where_op: PASS")


fn test_gather_1d() raises:
    """Gather from 1D tensor."""
    var x = Tensor[DType.float32](5)
    x.set(0, 10.0)
    x.set(1, 20.0)
    x.set(2, 30.0)
    x.set(3, 40.0)
    x.set(4, 50.0)
    var idx = List[Int]()
    idx.append(4)
    idx.append(2)
    idx.append(0)
    var out = gather(x, 0, idx)
    assert_close(out.get(0), 50.0)
    assert_close(out.get(1), 30.0)
    assert_close(out.get(2), 10.0)
    print("  gather_1d: PASS")


fn test_gather_2d_rows() raises:
    """Gather rows from 2D tensor."""
    var x = Tensor[DType.float32](3, 2)
    x.set(_idx2(0, 0), Float32(1.0))
    x.set(_idx2(0, 1), Float32(2.0))
    x.set(_idx2(1, 0), Float32(3.0))
    x.set(_idx2(1, 1), Float32(4.0))
    x.set(_idx2(2, 0), Float32(5.0))
    x.set(_idx2(2, 1), Float32(6.0))
    var idx = List[Int]()
    idx.append(2)
    idx.append(0)
    var out = gather(x, 0, idx)
    assert_close(out.get(0, 0), 5.0)
    assert_close(out.get(0, 1), 6.0)
    assert_close(out.get(1, 0), 1.0)
    assert_close(out.get(1, 1), 2.0)
    print("  gather_2d_rows: PASS")


fn test_eq_ne() raises:
    """Equality and not-equal comparison."""
    var a = Tensor[DType.float32](4)
    a.set(0, 1.0)
    a.set(1, 2.0)
    a.set(2, 3.0)
    a.set(3, 4.0)
    var b = Tensor[DType.float32](4)
    b.set(0, 1.0)
    b.set(1, 5.0)
    b.set(2, 3.0)
    b.set(3, 0.0)
    var e = eq(a, b)
    assert_close(e.get(0), 1.0)
    assert_close(e.get(1), 0.0)
    assert_close(e.get(2), 1.0)
    assert_close(e.get(3), 0.0)
    var n = ne(a, b)
    assert_close(n.get(0), 0.0)
    assert_close(n.get(1), 1.0)
    assert_close(n.get(2), 0.0)
    assert_close(n.get(3), 1.0)
    print("  eq_ne: PASS")


fn test_gt_lt() raises:
    """Greater-than and less-than."""
    var a = Tensor[DType.float32](3)
    a.set(0, 1.0)
    a.set(1, 3.0)
    a.set(2, 2.0)
    var b = Tensor[DType.float32](3)
    b.set(0, 2.0)
    b.set(1, 1.0)
    b.set(2, 2.0)
    var g = gt(a, b)
    assert_close(g.get(0), 0.0)
    assert_close(g.get(1), 1.0)
    assert_close(g.get(2), 0.0)
    var l = lt(a, b)
    assert_close(l.get(0), 1.0)
    assert_close(l.get(1), 0.0)
    assert_close(l.get(2), 0.0)
    print("  gt_lt: PASS")


fn test_ge_le() raises:
    """Greater-or-equal and less-or-equal."""
    var a = Tensor[DType.float32](3)
    a.set(0, 1.0)
    a.set(1, 3.0)
    a.set(2, 2.0)
    var b = Tensor[DType.float32](3)
    b.set(0, 2.0)
    b.set(1, 1.0)
    b.set(2, 2.0)
    var g = ge(a, b)
    assert_close(g.get(0), 0.0)
    assert_close(g.get(1), 1.0)
    assert_close(g.get(2), 1.0)
    var l = le(a, b)
    assert_close(l.get(0), 1.0)
    assert_close(l.get(1), 0.0)
    assert_close(l.get(2), 1.0)
    print("  ge_le: PASS")


fn test_index_select() raises:
    """Index select is an alias for gather."""
    var x = Tensor[DType.float32](4)
    x.set(0, 10.0)
    x.set(1, 20.0)
    x.set(2, 30.0)
    x.set(3, 40.0)
    var idx = List[Int]()
    idx.append(3)
    idx.append(1)
    var out = index_select(x, 0, idx)
    assert_close(out.get(0), 40.0)
    assert_close(out.get(1), 20.0)
    print("  index_select: PASS")


fn test_topk_full() raises:
    """Top-k when k equals tensor size."""
    var x = Tensor[DType.float32](3)
    x.set(0, 3.0)
    x.set(1, 1.0)
    x.set(2, 2.0)
    var out = topk(x, 3)
    assert_close(out.get(0), 3.0)
    assert_close(out.get(1), 2.0)
    assert_close(out.get(2), 1.0)
    print("  topk_full: PASS")


fn test_where_op_all_true() raises:
    """Where with all-true condition returns x."""
    var cond = Tensor[DType.float32](3)
    cond.set(0, 1.0)
    cond.set(1, 1.0)
    cond.set(2, 1.0)
    var x = Tensor[DType.float32](3)
    x.set(0, 10.0)
    x.set(1, 20.0)
    x.set(2, 30.0)
    var y = Tensor[DType.float32](3)
    y.set(0, -1.0)
    y.set(1, -2.0)
    y.set(2, -3.0)
    var out = where_op(cond, x, y)
    assert_close(out.get(0), 10.0)
    assert_close(out.get(1), 20.0)
    assert_close(out.get(2), 30.0)
    print("  where_op_all_true: PASS")


fn main() raises:
    print("test_selection_ops:")
    test_argmax_basic()
    test_argmin_basic()
    test_argmax_axis_0()
    test_argmax_axis_1()
    test_topk()
    test_where_op()
    test_gather_1d()
    test_gather_2d_rows()
    test_eq_ne()
    test_gt_lt()
    test_ge_le()
    test_index_select()
    test_topk_full()
    test_where_op_all_true()
    print("ALL PASSED (14 tests)")
