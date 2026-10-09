# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Non-owning strided tensor view
# ===----------------------------------------------------------------------=== #

"""Strided view into tensor data.

TensorView[dtype] is a non-owning view that supports slicing, transposing,
and broadcasting by manipulating shape and strides without copying data.
"""

from .shape import Shape
from std.sys import size_of


# ===----------------------------------------------------------------------=== #
# TensorView — non-owning strided view
# ===----------------------------------------------------------------------=== #


struct TensorView[dtype: DType](Writable, Copyable, Movable, ImplicitlyCopyable):
    """A non-owning strided view into tensor data.

    Holds a raw pointer (borrowed from a Storage), along with shape, strides,
    and an element offset. All view operations (transpose, slice, broadcast)
    produce new views without copying data.
    """

    var _ptr: Pointer[Scalar[Self.dtype], MutUntrackedOrigin]
    var shape: Shape
    var _strides: List[Int]
    var _offset: Int  # element offset from _ptr

    # --- Constructors ---

    def __init__(
        out self,
        ptr: Pointer[Scalar[Self.dtype], MutUntrackedOrigin],
        shape: Shape,
        strides: List[Int],
        offset: Int = 0,
    ) raises:
        """Create a view from a raw pointer, shape, strides, and offset."""
        self._ptr = ptr
        self.shape = shape.copy()
        self._strides = strides.copy()
        self._offset = offset
        self.validate()

    def __init__(
        out self,
        ptr: Pointer[Scalar[Self.dtype], MutUntrackedOrigin],
        shape: Shape,
    ) raises:
        """Create a contiguous view (computes row-major strides automatically)."""
        self._ptr = ptr
        self.shape = shape.copy()
        self._strides = shape.strides()
        self._offset = 0
        self.validate()

    def __init__(out self, *, copy: Self):
        """Copy constructor — shallow copy (non-owning view)."""
        self._ptr = copy._ptr
        self.shape = copy.shape.copy()
        self._strides = copy._strides.copy()
        self._offset = copy._offset

    def __init__(out self, *, deinit move: Self):
        """Move constructor."""
        self._ptr = move._ptr
        self.shape = move.shape^
        self._strides = move._strides^
        self._offset = move._offset

    def validate(self) raises:
        """Admit nonnegative element strides and checked addressed offsets.

        Raw-pointer allocation extent and lifetime remain the caller's contract.
        Negative-stride adapters are explicitly outside this view API.
        """
        self.shape.validate()
        if self._offset < 0 or len(self._strides) != self.shape.ndim():
            raise Error("Invalid tensor view offset or stride rank")
        var last = self._offset
        for d in range(self.shape.ndim()):
            var stride = self._strides[d]
            if stride < 0:
                raise Error("Negative tensor view strides are unsupported")
            if self.shape.numel() != 0 and self.shape[d] > 1:
                var steps = self.shape[d] - 1
                if stride > (0x7FFFFFFFFFFFFFFF - last) // steps:
                    raise Error("Tensor view address overflow")
                last += steps * stride
        if last > 0x7FFFFFFFFFFFFFFF // size_of[Scalar[Self.dtype]]():
            raise Error("Tensor view byte address overflow")

    # --- Element access ---

    def _linear_index(self, *indices: Int) -> Int:
        """Compute the linear memory offset for the given multi-dimensional indices."""
        var offset = self._offset
        var n = len(indices)
        for i in range(n):
            offset += indices[i] * self._strides[i]
        return offset

    def load(self, *indices: Int) -> Scalar[Self.dtype]:
        """Load a single element at the given indices."""
        var n = len(indices)
        var offset = self._offset
        for i in range(n):
            offset += indices[i] * self._strides[i]
        return self._ptr.load(offset)

    def store(self, *indices: Int, value: Scalar[Self.dtype]) raises:
        """Store a single element at the given indices."""
        var n = len(indices)
        var offset = self._offset
        for i in range(n):
            offset += indices[i] * self._strides[i]
        self._ptr.store(offset, value)

    # --- View operations ---

    def transpose(self, dim0: Int, dim1: Int) raises -> TensorView[Self.dtype]:
        """Returns a view with two dimensions swapped. No data copy.

        WARNING: The returned view borrows from the same memory as self.
        The caller must ensure the underlying Storage/Tensor outlives this view.
        Use Tensor(view) to materialize an owned copy if needed.
        """
        self.validate()
        var ndim = self.shape.ndim()
        if dim0 < 0 or dim0 >= ndim or dim1 < 0 or dim1 >= ndim:
            raise Error("transpose: invalid dimension")
        var new_dims = List[Int]()
        var new_strides = List[Int]()
        for i in range(ndim):
            new_dims.append(self.shape[i])
            new_strides.append(self._strides[i])

        # Swap dimensions
        var tmp_d = new_dims[dim0]
        new_dims[dim0] = new_dims[dim1]
        new_dims[dim1] = tmp_d

        # Swap strides
        var tmp_s = new_strides[dim0]
        new_strides[dim0] = new_strides[dim1]
        new_strides[dim1] = tmp_s

        return TensorView[Self.dtype](
            self._ptr,
            Shape(new_dims^),
            new_strides,
            self._offset,
        )

    def broadcast_to(self, target: Shape) raises -> TensorView[Self.dtype]:
        """Returns a view broadcast to target shape.

        Dimensions of size 1 get stride 0 (repeated without copying).
        Raises on incompatible shapes.

        WARNING: The returned view borrows from the same memory as self.
        The caller must ensure the underlying Storage/Tensor outlives this view.
        """
        self.validate()
        target.validate()
        var ndim_out = target.ndim()
        if ndim_out < self.shape.ndim():
            raise Error("broadcast_to cannot drop dimensions")
        var new_strides = List[Int]()
        for _ in range(ndim_out):
            new_strides.append(0)

        for i in range(ndim_out):
            var idx_self = self.shape.ndim() - ndim_out + i
            if idx_self < 0:
                # Dimension doesn't exist in self — broadcast with stride 0
                new_strides[i] = 0
            elif self.shape[idx_self] == target[i]:
                new_strides[i] = self._strides[idx_self]
            elif self.shape[idx_self] == 1:
                new_strides[i] = 0
            else:
                raise Error(
                    "Cannot broadcast dimension "
                    + String(self.shape[idx_self])
                    + " to "
                    + String(target[i])
                )

        return TensorView[Self.dtype](
            self._ptr,
            target,
            new_strides,
            self._offset,
        )

    def slice_dim(self, dim: Int, start: Int, length: Int) raises -> TensorView[Self.dtype]:
        """Returns a view sliced along a single dimension.

        WARNING: The returned view borrows from the same memory as self.
        The caller must ensure the underlying Storage/Tensor outlives this view.
        """
        self.validate()
        if dim < 0 or dim >= self.shape.ndim():
            raise Error("slice: invalid dimension")
        var extent = self.shape[dim]
        if start < 0 or start > extent or length < 0 or length > extent - start:
            raise Error("slice: invalid span")

        var new_dims = List[Int]()
        for i in range(self.shape.ndim()):
            if i == dim:
                new_dims.append(length)
            else:
                new_dims.append(self.shape[i])

        var new_strides = List[Int]()
        for i in range(self.shape.ndim()):
            new_strides.append(self._strides[i])

        if start > 0 and self._strides[dim] > (0x7FFFFFFFFFFFFFFF - self._offset) // start:
            raise Error("slice: offset overflow")
        var new_offset = self._offset + start * self._strides[dim]

        return TensorView[Self.dtype](
            self._ptr,
            Shape(new_dims^),
            new_strides,
            new_offset,
        )

    def reshape(self, new_shape: Shape) raises -> TensorView[Self.dtype]:
        """Returns a view with a new shape. Only valid for contiguous data.

        WARNING: The returned view borrows from the same memory as self.
        The caller must ensure the underlying Storage/Tensor outlives this view.
        """
        self.validate()
        new_shape.validate()
        if not self.is_contiguous():
            raise Error("Cannot reshape non-contiguous view")
        if self.shape.numel() != new_shape.numel():
            raise Error(
                "Cannot reshape "
                + String(self.shape)
                + " to "
                + String(new_shape)
                + ": element count mismatch"
            )
        # MJ-K12: preserve the storage OFFSET. The two-argument constructor
        # used here resets _offset to 0, so reshaping a contiguous SLICE
        # silently re-based it onto the underlying tensor's start — a slice
        # selecting [3,4,5] of [0..5] reshaped to (3,) read [0,1,2], and
        # writes landed on the wrong elements.
        return TensorView[Self.dtype](
            self._ptr,
            new_shape,
            new_shape.strides(),
            self._offset,
        )

    # --- Properties ---

    def is_contiguous(self) -> Bool:
        """Returns True if the view's memory layout is dense (C-contiguous)."""
        var expected = self.shape.strides()
        if len(expected) != len(self._strides):
            return False
        for i in range(len(expected)):
            if self._strides[i] != expected[i]:
                return False
        return True

    @always_inline
    def ndim(self) -> Int:
        """Returns the number of dimensions."""
        return self.shape.ndim()

    @always_inline
    def numel(self) -> Int:
        """Returns the total number of elements."""
        return self.shape.numel()

    @always_inline
    def offset(self) -> Int:
        """Returns the element offset."""
        return self._offset

    def strides(self) -> List[Int]:
        """Returns a copy of the strides list."""
        return self._strides.copy()

    # --- Writable ---

    def write_to(self, mut writer: Some[Writer]):
        writer.write("TensorView[", Self.dtype, "](shape=")
        self.shape.write_to(writer)
        writer.write(", contiguous=", self.is_contiguous(), ")")
