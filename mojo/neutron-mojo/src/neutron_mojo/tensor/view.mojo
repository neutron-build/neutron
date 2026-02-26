# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Non-owning strided tensor view
# ===----------------------------------------------------------------------=== #

"""Strided view into tensor data.

TensorView[dtype] is a non-owning view that supports slicing, transposing,
and broadcasting by manipulating shape and strides without copying data.
"""

from .shape import Shape


# ===----------------------------------------------------------------------=== #
# TensorView — non-owning strided view
# ===----------------------------------------------------------------------=== #


struct TensorView[dtype: DType](Writable, Copyable, Movable):
    """A non-owning strided view into tensor data.

    Holds a raw pointer (borrowed from a Storage), along with shape, strides,
    and an element offset. All view operations (transpose, slice, broadcast)
    produce new views without copying data.
    """

    var _ptr: UnsafePointer[Scalar[Self.dtype], MutExternalOrigin]
    var shape: Shape
    var _strides: List[Int]
    var _offset: Int  # element offset from _ptr

    # --- Constructors ---

    fn __init__(
        out self,
        ptr: UnsafePointer[Scalar[Self.dtype], MutExternalOrigin],
        shape: Shape,
        strides: List[Int],
        offset: Int = 0,
    ):
        """Create a view from a raw pointer, shape, strides, and offset."""
        self._ptr = ptr
        self.shape = shape.copy()
        self._strides = strides.copy()
        self._offset = offset

    fn __init__(
        out self,
        ptr: UnsafePointer[Scalar[Self.dtype], MutExternalOrigin],
        shape: Shape,
    ):
        """Create a contiguous view (computes row-major strides automatically)."""
        self._ptr = ptr
        self.shape = shape.copy()
        self._strides = shape.strides()
        self._offset = 0

    fn __copyinit__(out self, other: Self):
        """Copy constructor — shallow copy (non-owning view)."""
        self._ptr = other._ptr
        self.shape = other.shape.copy()
        self._strides = other._strides.copy()
        self._offset = other._offset

    fn __moveinit__(out self, deinit other: Self):
        """Move constructor."""
        self._ptr = other._ptr
        self.shape = other.shape^
        self._strides = other._strides^
        self._offset = other._offset

    # --- Element access ---

    fn _linear_index(self, *indices: Int) -> Int:
        """Compute the linear memory offset for the given multi-dimensional indices."""
        var offset = self._offset
        var n = len(indices)
        for i in range(n):
            offset += indices[i] * self._strides[i]
        return offset

    fn load(self, *indices: Int) -> Scalar[Self.dtype]:
        """Load a single element at the given indices."""
        var n = len(indices)
        var offset = self._offset
        for i in range(n):
            offset += indices[i] * self._strides[i]
        return self._ptr.load(offset)

    fn store(self, *indices: Int, value: Scalar[Self.dtype]):
        """Store a single element at the given indices."""
        var n = len(indices)
        var offset = self._offset
        for i in range(n):
            offset += indices[i] * self._strides[i]
        self._ptr.store(offset, value)

    # --- View operations ---

    fn transpose(self, dim0: Int, dim1: Int) -> TensorView[Self.dtype]:
        """Returns a view with two dimensions swapped. No data copy.

        WARNING: The returned view borrows from the same memory as self.
        The caller must ensure the underlying Storage/Tensor outlives this view.
        Use Tensor(view) to materialize an owned copy if needed.
        """
        var new_dims = List[Int]()
        var new_strides = List[Int]()
        var ndim = self.shape.ndim()
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

    fn broadcast_to(self, target: Shape) raises -> TensorView[Self.dtype]:
        """Returns a view broadcast to target shape.

        Dimensions of size 1 get stride 0 (repeated without copying).
        Raises on incompatible shapes.

        WARNING: The returned view borrows from the same memory as self.
        The caller must ensure the underlying Storage/Tensor outlives this view.
        """
        var ndim_out = target.ndim()
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

    fn slice_dim(self, dim: Int, start: Int, length: Int) -> TensorView[Self.dtype]:
        """Returns a view sliced along a single dimension.

        WARNING: The returned view borrows from the same memory as self.
        The caller must ensure the underlying Storage/Tensor outlives this view.
        """
        var new_dims = List[Int]()
        for i in range(self.shape.ndim()):
            if i == dim:
                new_dims.append(length)
            else:
                new_dims.append(self.shape[i])

        var new_strides = List[Int]()
        for i in range(self.shape.ndim()):
            new_strides.append(self._strides[i])

        var new_offset = self._offset + start * self._strides[dim]

        return TensorView[Self.dtype](
            self._ptr,
            Shape(new_dims^),
            new_strides,
            new_offset,
        )

    fn reshape(self, new_shape: Shape) raises -> TensorView[Self.dtype]:
        """Returns a view with a new shape. Only valid for contiguous data.

        WARNING: The returned view borrows from the same memory as self.
        The caller must ensure the underlying Storage/Tensor outlives this view.
        """
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
        return TensorView[Self.dtype](
            self._ptr,
            new_shape,
        )

    # --- Properties ---

    fn is_contiguous(self) -> Bool:
        """Returns True if the view's memory layout is dense (C-contiguous)."""
        var expected = self.shape.strides()
        if len(expected) != len(self._strides):
            return False
        for i in range(len(expected)):
            if self._strides[i] != expected[i]:
                return False
        return True

    @always_inline
    fn ndim(self) -> Int:
        """Returns the number of dimensions."""
        return self.shape.ndim()

    @always_inline
    fn numel(self) -> Int:
        """Returns the total number of elements."""
        return self.shape.numel()

    @always_inline
    fn offset(self) -> Int:
        """Returns the element offset."""
        return self._offset

    fn strides(self) -> List[Int]:
        """Returns a copy of the strides list."""
        return self._strides.copy()

    # --- Writable ---

    fn write_to[W: Writer](self, mut writer: W):
        writer.write("TensorView[", Self.dtype, "](shape=")
        self.shape.write_to(writer)
        writer.write(", contiguous=", self.is_contiguous(), ")")
