# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Shape manipulation & creation ops
# ===----------------------------------------------------------------------=== #

"""Shape manipulation, concatenation, creation ops, and sorting.

Separate from ops.mojo to avoid concurrent edit conflicts.
All functions follow the same pattern: allocate result, scalar loop, return.
"""

from math import sqrt

from .shape import Shape
from .tensor import Tensor


# ===----------------------------------------------------------------------=== #
# Concatenation — fixed arity (List[Tensor] not possible)
# ===----------------------------------------------------------------------=== #


fn concat2[dtype: DType](
    a: Tensor[dtype], b: Tensor[dtype], dim: Int = 0
) raises -> Tensor[dtype]:
    """Concatenate two tensors along a dimension.

    For 1D: concat along axis 0.
    For 2D dim=0: stack rows. dim=1: stack columns.
    """
    if a.ndim() == 1 and b.ndim() == 1:
        var n_a = a.numel()
        var n_b = b.numel()
        var result = Tensor[dtype](n_a + n_b)
        var r_ptr = result.data_ptr()
        var a_ptr = a.data_ptr()
        var b_ptr = b.data_ptr()
        for i in range(n_a):
            r_ptr.store(i, a_ptr.load(i))
        for i in range(n_b):
            r_ptr.store(n_a + i, b_ptr.load(i))
        return result^

    elif a.ndim() == 2 and b.ndim() == 2:
        var a_rows = a.shape()[0]
        var a_cols = a.shape()[1]
        var b_rows = b.shape()[0]
        var b_cols = b.shape()[1]
        var a_ptr = a.data_ptr()
        var b_ptr = b.data_ptr()

        if dim == 0:
            if a_cols != b_cols:
                raise Error("concat2 dim=0: column count must match")
            var result = Tensor[dtype](a_rows + b_rows, a_cols)
            var r_ptr = result.data_ptr()
            for i in range(a_rows * a_cols):
                r_ptr.store(i, a_ptr.load(i))
            for i in range(b_rows * b_cols):
                r_ptr.store(a_rows * a_cols + i, b_ptr.load(i))
            return result^
        elif dim == 1:
            if a_rows != b_rows:
                raise Error("concat2 dim=1: row count must match")
            var out_cols = a_cols + b_cols
            var result = Tensor[dtype](a_rows, out_cols)
            var r_ptr = result.data_ptr()
            for row in range(a_rows):
                for j in range(a_cols):
                    r_ptr.store(row * out_cols + j, a_ptr.load(row * a_cols + j))
                for j in range(b_cols):
                    r_ptr.store(row * out_cols + a_cols + j, b_ptr.load(row * b_cols + j))
            return result^
        else:
            raise Error("concat2: dim must be 0 or 1 for 2D")
    else:
        raise Error("concat2: tensors must be same ndim (1D or 2D)")


fn concat3[dtype: DType](
    a: Tensor[dtype], b: Tensor[dtype], c: Tensor[dtype], dim: Int = 0
) raises -> Tensor[dtype]:
    """Concatenate three tensors along a dimension."""
    var ab = concat2(a, b, dim)
    return concat2(ab, c, dim)


fn concat4[dtype: DType](
    a: Tensor[dtype], b: Tensor[dtype], c: Tensor[dtype], d: Tensor[dtype], dim: Int = 0
) raises -> Tensor[dtype]:
    """Concatenate four tensors along a dimension."""
    var ab = concat2(a, b, dim)
    var cd = concat2(c, d, dim)
    return concat2(ab, cd, dim)


# ===----------------------------------------------------------------------=== #
# Split — fixed arity return structs
# ===----------------------------------------------------------------------=== #


struct SplitResult2[dtype: DType](Movable):
    """Result of splitting a tensor into 2 parts."""
    var part0: Tensor[Self.dtype]
    var part1: Tensor[Self.dtype]

    fn __init__(out self, var p0: Tensor[Self.dtype], var p1: Tensor[Self.dtype]):
        self.part0 = p0^
        self.part1 = p1^

    fn __moveinit__(out self, deinit other: Self):
        self.part0 = other.part0^
        self.part1 = other.part1^


struct SplitResult3[dtype: DType](Movable):
    """Result of splitting a tensor into 3 parts."""
    var part0: Tensor[Self.dtype]
    var part1: Tensor[Self.dtype]
    var part2: Tensor[Self.dtype]

    fn __init__(out self, var p0: Tensor[Self.dtype], var p1: Tensor[Self.dtype], var p2: Tensor[Self.dtype]):
        self.part0 = p0^
        self.part1 = p1^
        self.part2 = p2^

    fn __moveinit__(out self, deinit other: Self):
        self.part0 = other.part0^
        self.part1 = other.part1^
        self.part2 = other.part2^


fn split2[dtype: DType](
    x: Tensor[dtype], split_at: Int, dim: Int = 0
) raises -> SplitResult2[dtype]:
    """Split a tensor into 2 parts at the given index along dim."""
    var x_ptr = x.data_ptr()

    if x.ndim() == 1:
        var n = x.numel()
        if split_at < 0 or split_at > n:
            raise Error("split2: split_at out of range")
        var p0 = Tensor[dtype](split_at)
        var p1 = Tensor[dtype](n - split_at)
        var p0_ptr = p0.data_ptr()
        var p1_ptr = p1.data_ptr()
        for i in range(split_at):
            p0_ptr.store(i, x_ptr.load(i))
        for i in range(n - split_at):
            p1_ptr.store(i, x_ptr.load(split_at + i))
        return SplitResult2[dtype](p0^, p1^)

    elif x.ndim() == 2 and dim == 0:
        var rows = x.shape()[0]
        var cols = x.shape()[1]
        if split_at < 0 or split_at > rows:
            raise Error("split2: split_at out of range")
        var p0 = Tensor[dtype](split_at, cols)
        var p1 = Tensor[dtype](rows - split_at, cols)
        var p0_ptr = p0.data_ptr()
        var p1_ptr = p1.data_ptr()
        for i in range(split_at * cols):
            p0_ptr.store(i, x_ptr.load(i))
        for i in range((rows - split_at) * cols):
            p1_ptr.store(i, x_ptr.load(split_at * cols + i))
        return SplitResult2[dtype](p0^, p1^)

    elif x.ndim() == 2 and dim == 1:
        var rows = x.shape()[0]
        var cols = x.shape()[1]
        if split_at < 0 or split_at > cols:
            raise Error("split2: split_at out of range")
        var p0 = Tensor[dtype](rows, split_at)
        var p1 = Tensor[dtype](rows, cols - split_at)
        var p0_ptr = p0.data_ptr()
        var p1_ptr = p1.data_ptr()
        for row in range(rows):
            for j in range(split_at):
                p0_ptr.store(row * split_at + j, x_ptr.load(row * cols + j))
            for j in range(cols - split_at):
                p1_ptr.store(row * (cols - split_at) + j, x_ptr.load(row * cols + split_at + j))
        return SplitResult2[dtype](p0^, p1^)

    else:
        raise Error("split2: only 1D and 2D supported")


fn split3[dtype: DType](
    x: Tensor[dtype], split1: Int, split2_at: Int, dim: Int = 0
) raises -> SplitResult3[dtype]:
    """Split a tensor into 3 parts at two split points along dim."""
    var first_split = split2(x, split2_at, dim)
    var second_split = split2(first_split.part0^, split1, dim)
    return SplitResult3[dtype](second_split.part0^, second_split.part1^, first_split.part1^)


# ===----------------------------------------------------------------------=== #
# Shape manipulation
# ===----------------------------------------------------------------------=== #


fn squeeze[dtype: DType](x: Tensor[dtype], dim: Int) raises -> Tensor[dtype]:
    """Remove a dimension of size 1."""
    var s = x.shape()
    if dim < 0 or dim >= s.ndim():
        raise Error("squeeze: dim out of range")
    if s[dim] != 1:
        raise Error("squeeze: dimension " + String(dim) + " is not size 1")

    var new_dims = List[Int]()
    for i in range(s.ndim()):
        if i != dim:
            new_dims.append(s[i])

    var new_shape = Shape(new_dims^)
    var result = Tensor[dtype](new_shape)
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, x_ptr.load(i))
    return result^


fn unsqueeze[dtype: DType](x: Tensor[dtype], dim: Int) raises -> Tensor[dtype]:
    """Insert a dimension of size 1 at the given position."""
    var s = x.shape()
    if dim < 0 or dim > s.ndim():
        raise Error("unsqueeze: dim out of range")

    var new_dims = List[Int]()
    for i in range(s.ndim()):
        if i == dim:
            new_dims.append(1)
        new_dims.append(s[i])
    if dim == s.ndim():
        new_dims.append(1)

    var new_shape = Shape(new_dims^)
    var result = Tensor[dtype](new_shape)
    var n = x.numel()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, x_ptr.load(i))
    return result^


fn flatten[dtype: DType](x: Tensor[dtype]) -> Tensor[dtype]:
    """Flatten to 1D."""
    var n = x.numel()
    var result = Tensor[dtype](n)
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, x_ptr.load(i))
    return result^


fn expand[dtype: DType](x: Tensor[dtype], shape: Shape) raises -> Tensor[dtype]:
    """Expand a tensor to a larger shape by repeating along size-1 dims."""
    if x.ndim() != shape.ndim():
        raise Error("expand: ndim must match")

    var result = Tensor[dtype](shape)
    var n = shape.numel()
    var x_shape = x.shape()
    var out_strides = shape.strides()
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()

    for i in range(n):
        var remaining = i
        var src_offset = 0
        var src_strides = x_shape.strides()
        for d in range(shape.ndim()):
            var coord = remaining // out_strides[d]
            remaining = remaining % out_strides[d]
            if x_shape[d] == 1:
                pass  # broadcast
            else:
                src_offset += coord * src_strides[d]
        r_ptr.store(i, x_ptr.load(src_offset))

    return result^


# ===----------------------------------------------------------------------=== #
# Creation ops
# ===----------------------------------------------------------------------=== #


fn arange[dtype: DType](start: Float64, stop: Float64, step: Float64 = 1.0) raises -> Tensor[dtype]:
    """Create a 1D tensor with evenly spaced values."""
    if step == 0.0:
        raise Error("arange: step must be non-zero")
    var n: Int
    if step > 0:
        n = max(0, Int((stop - start + step - 1e-10) / step))
    else:
        n = max(0, Int((start - stop - step - 1e-10) / (-step)))
    if n == 0:
        return Tensor[dtype](0)

    var result = Tensor[dtype](n)
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, Scalar[dtype](start + Float64(i) * step))
    return result^


fn linspace[dtype: DType](start: Float64, stop: Float64, num: Int) raises -> Tensor[dtype]:
    """Create a 1D tensor with num evenly spaced values from start to stop (inclusive)."""
    if num < 1:
        raise Error("linspace: num must be >= 1")
    var result = Tensor[dtype](num)
    var r_ptr = result.data_ptr()
    if num == 1:
        r_ptr.store(0, Scalar[dtype](start))
        return result^
    var step = (stop - start) / Float64(num - 1)
    for i in range(num):
        r_ptr.store(i, Scalar[dtype](start + Float64(i) * step))
    return result^


fn eye[dtype: DType](n: Int) -> Tensor[dtype]:
    """Create an n x n identity matrix."""
    var result = Tensor[dtype](n, n)
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i * n + i, Scalar[dtype](1.0))
    return result^


fn tril[dtype: DType](x: Tensor[dtype], diagonal: Int = 0) raises -> Tensor[dtype]:
    """Lower triangular: zero out elements above the k-th diagonal."""
    if x.ndim() != 2:
        raise Error("tril: only 2D tensors supported")
    var rows = x.shape()[0]
    var cols = x.shape()[1]
    var result = Tensor[dtype](rows, cols)
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(rows):
        for j in range(cols):
            if j <= i + diagonal:
                r_ptr.store(i * cols + j, x_ptr.load(i * cols + j))
            # else: already 0
    return result^


fn triu[dtype: DType](x: Tensor[dtype], diagonal: Int = 0) raises -> Tensor[dtype]:
    """Upper triangular: zero out elements below the k-th diagonal."""
    if x.ndim() != 2:
        raise Error("triu: only 2D tensors supported")
    var rows = x.shape()[0]
    var cols = x.shape()[1]
    var result = Tensor[dtype](rows, cols)
    var x_ptr = x.data_ptr()
    var r_ptr = result.data_ptr()
    for i in range(rows):
        for j in range(cols):
            if j >= i + diagonal:
                r_ptr.store(i * cols + j, x_ptr.load(i * cols + j))
    return result^


# ===----------------------------------------------------------------------=== #
# Sorting
# ===----------------------------------------------------------------------=== #


struct SortResult[dtype: DType](Movable):
    """Result of sort: sorted values tensor and original indices."""
    var values: Tensor[Self.dtype]
    var indices: List[Int]

    fn __init__(out self, var values: Tensor[Self.dtype], var indices: List[Int]):
        self.values = values^
        self.indices = indices^

    fn __moveinit__(out self, deinit other: Self):
        self.values = other.values^
        self.indices = other.indices^


fn sort[dtype: DType](x: Tensor[dtype], descending: Bool = False) raises -> SortResult[dtype]:
    """Sort a 1D tensor. Returns sorted values and original indices."""
    if x.ndim() != 1:
        raise Error("sort: only 1D tensors supported")
    var n = x.numel()
    var x_ptr = x.data_ptr()

    # Build index array
    var indices = List[Int]()
    for i in range(n):
        indices.append(i)

    # Insertion sort
    for i in range(1, n):
        var key_idx = indices[i]
        var key_val = Float64(x_ptr.load(key_idx))
        var j = i - 1
        while j >= 0:
            var cmp_val = Float64(x_ptr.load(indices[j]))
            var should_swap: Bool
            if descending:
                should_swap = cmp_val < key_val
            else:
                should_swap = cmp_val > key_val
            if not should_swap:
                break
            indices[j + 1] = indices[j]
            j -= 1
        indices[j + 1] = key_idx

    var result = Tensor[dtype](n)
    var r_ptr = result.data_ptr()
    for i in range(n):
        r_ptr.store(i, x_ptr.load(indices[i]))
    return SortResult[dtype](result^, indices^)


fn argsort[dtype: DType](x: Tensor[dtype], descending: Bool = False) raises -> List[Int]:
    """Return indices that would sort a 1D tensor."""
    var sr = sort(x, descending)
    return sr.indices.copy()
