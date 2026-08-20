# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Core Tensor type
# ===----------------------------------------------------------------------=== #

"""The core Tensor[dtype] type — owns storage, exposes views.

Tensor[dtype] is parametric over DType with runtime shape. Provides
factory methods (zeros, ones, full, rand), element access, SIMD
load/store, and view operations (transpose, reshape).
"""

from std.memory import memcpy
from std.random import random_float64

from .shape import Shape
from .storage import Storage, DeviceKind
from .view import TensorView


# ===----------------------------------------------------------------------=== #
# Tensor
# ===----------------------------------------------------------------------=== #


struct Tensor[dtype: DType](Writable, Movable):
    """A multi-dimensional array with typed elements and runtime shape.

    Owns a Storage[dtype] and exposes a TensorView for strided access.
    """

    var _storage: Storage[Self.dtype]
    var _view: TensorView[Self.dtype]

    # --- Constructors ---

    def __init__(out self, shape: Shape):
        """Create a zero-initialized tensor with the given shape."""
        var numel = shape.numel()
        self._storage = Storage[Self.dtype](numel)
        self._view = TensorView[Self.dtype](self._storage.unsafe_ptr(), shape)

    def __init__(out self, *dims: Int):
        """Create a zero-initialized tensor from variadic dimension sizes."""
        var dim_list = List[Int]()
        for i in range(len(dims)):
            dim_list.append(dims[i])
        var shape = Shape(dim_list^)
        var numel = shape.numel()
        self._storage = Storage[Self.dtype](numel)
        self._view = TensorView[Self.dtype](self._storage.unsafe_ptr(), shape)

    def __init__(out self, view: TensorView[Self.dtype]):
        """Create an owned tensor by copying data from a (possibly non-contiguous) view.

        Use this to materialize a safe owned copy from a transposed, sliced,
        or broadcast view: `var owned = Tensor[dtype](some_view)`
        """
        var shape = view.shape.copy()
        var numel = shape.numel()
        self._storage = Storage[Self.dtype](numel)
        self._view = TensorView[Self.dtype](self._storage.unsafe_ptr(), shape)
        # Copy element by element (handles non-contiguous strides)
        var dst = self._storage.unsafe_ptr()
        for i in range(numel):
            # Compute multi-dimensional indices from flat index
            var remaining = i
            var src_strides = view.strides()
            var offset = view.offset()
            for d in range(shape.ndim()):
                var dim_stride = self._view.strides()[d]
                var coord = remaining // dim_stride
                remaining = remaining % dim_stride
                offset += coord * src_strides[d]
            dst.store(i, view._ptr.load(offset))

    def __init__(out self, *, deinit move: Self):
        """Move constructor."""
        self._storage = move._storage^
        self._view = move._view^

    # --- Factory methods ---

    @staticmethod
    def zeros(shape: Shape) -> Tensor[Self.dtype]:
        """Create a tensor filled with zeros."""
        return Tensor[Self.dtype](shape)

    @staticmethod
    def ones(shape: Shape) -> Tensor[Self.dtype]:
        """Create a tensor filled with ones."""
        var t = Tensor[Self.dtype](shape)
        t._storage.fill(Scalar[Self.dtype](1))
        return t^

    @staticmethod
    def full(shape: Shape, value: Scalar[Self.dtype]) -> Tensor[Self.dtype]:
        """Create a tensor filled with a constant value."""
        var t = Tensor[Self.dtype](shape)
        t._storage.fill(value)
        return t^

    @staticmethod
    def rand(shape: Shape) -> Tensor[Self.dtype]:
        """Create a tensor filled with uniform random values in [0, 1)."""
        var t = Tensor[Self.dtype](shape)
        for i in range(shape.numel()):
            t._storage.store(i, Scalar[Self.dtype](random_float64()))
        return t^

    @staticmethod
    def randn(shape: Shape) -> Tensor[Self.dtype]:
        """Create a tensor with approximate standard normal values.

        Uses Box-Muller transform on pairs of uniform samples.
        """
        from std.math import sqrt, log, cos

        comptime TWO_PI = 6.283185307179586

        var t = Tensor[Self.dtype](shape)
        var n = shape.numel()
        var i = 0
        while i + 1 < n:
            var u1 = random_float64()
            var u2 = random_float64()
            # Avoid log(0)
            if u1 < 1e-15:
                u1 = 1e-15
            var mag = sqrt(-2.0 * log(u1))
            var z0 = mag * cos(TWO_PI * u2)
            var z1 = mag * sqrt(1.0 - cos(TWO_PI * u2) * cos(TWO_PI * u2))
            if u2 > 0.5:
                z1 = -z1
            t._storage.store(i, Scalar[Self.dtype](z0))
            t._storage.store(i + 1, Scalar[Self.dtype](z1))
            i += 2
        if i < n:
            var u = random_float64()
            if u < 1e-15:
                u = 1e-15
            var z = sqrt(-2.0 * log(u))
            t._storage.store(i, Scalar[Self.dtype](z))
        return t^

    # --- Element access ---

    def get(self, *indices: Int) -> Scalar[Self.dtype]:
        """Get a single element by indices."""
        var offset = self._view.offset()
        for i in range(len(indices)):
            offset += indices[i] * self._view._strides[i]
        return self._storage.load(offset)

    def set(mut self, indices: List[Int], value: Scalar[Self.dtype]):
        """Set a single element by index list."""
        var offset = self._view.offset()
        for i in range(len(indices)):
            offset += indices[i] * self._view._strides[i]
        self._storage.store(offset, value)

    def set(mut self, flat_index: Int, value: Scalar[Self.dtype]):
        """Set a single element by flat index (for 1D tensors or raw offset)."""
        self._storage.store(flat_index + self._view.offset(), value)

    # --- SIMD access ---

    def load_simd[width: Int](self, flat_offset: Int) -> SIMD[Self.dtype, width]:
        """Load a SIMD vector of `width` contiguous elements from flat offset."""
        return self._storage.load_simd[width](flat_offset + self._view.offset())

    def store_simd[width: Int](
        mut self, flat_offset: Int, value: SIMD[Self.dtype, width]
    ):
        """Store a SIMD vector of `width` contiguous elements at flat offset."""
        self._storage.store_simd[width](flat_offset + self._view.offset(), value)

    # --- View operations ---

    def view(self) -> TensorView[Self.dtype]:
        """Returns the current view of this tensor."""
        return TensorView[Self.dtype](
            self._storage.unsafe_ptr(),
            self._view.shape,
            self._view.strides(),
            self._view.offset(),
        )

    def transpose(self, dim0: Int, dim1: Int) -> TensorView[Self.dtype]:
        """Returns a transposed view (no data copy).

        The returned view borrows this tensor's memory. Ensure this tensor
        outlives the view, or materialize with Tensor[dtype](view).
        """
        return self._view.transpose(dim0, dim1)

    def reshape(self, new_shape: Shape) raises -> TensorView[Self.dtype]:
        """Returns a reshaped view (must be contiguous).

        The returned view borrows this tensor's memory. Ensure this tensor
        outlives the view, or materialize with Tensor[dtype](view).
        """
        return self._view.reshape(new_shape)

    def clone(self) -> Tensor[Self.dtype]:
        """Returns a deep copy of this tensor with independent storage."""
        var t = Tensor[Self.dtype](self.shape())
        t._storage.copy_from(self._storage)
        return t^

    def is_contiguous(self) -> Bool:
        """Returns True if the tensor's view is contiguous."""
        return self._view.is_contiguous()

    # --- Properties ---

    @always_inline
    def shape(self) -> Shape:
        """Returns the runtime shape."""
        return self._view.shape.copy()

    @always_inline
    def ndim(self) -> Int:
        """Returns the number of dimensions."""
        return self._view.ndim()

    @always_inline
    def numel(self) -> Int:
        """Returns the total number of elements."""
        return self._view.numel()

    def dtype_val(self) -> DType:
        """Returns the element data type."""
        return Self.dtype

    def device(self) -> DeviceKind:
        """Returns the device this tensor resides on."""
        return self._storage.device()

    def data_ptr(self) -> Pointer[Scalar[Self.dtype], MutUntrackedOrigin]:
        """Returns a raw pointer to the tensor's data."""
        return self._storage.unsafe_ptr()

    # --- Writable ---

    def write_to(self, mut writer: Some[Writer]):
        writer.write("Tensor[", Self.dtype, "](")
        self._view.shape.write_to(writer)
        if self.numel() <= 20:
            writer.write(", data=[")
            for i in range(self.numel()):
                if i > 0:
                    writer.write(", ")
                writer.write(self._storage.load(i))
            writer.write("]")
        writer.write(")")
