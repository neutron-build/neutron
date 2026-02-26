# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Runtime tensor shape
# ===----------------------------------------------------------------------=== #

"""Runtime shape representation for dynamic shape operations.

Shape wraps a List[Int] of dimension sizes and provides broadcasting,
reshape validation, stride computation, and element counting.
"""


# ===----------------------------------------------------------------------=== #
# Shape
# ===----------------------------------------------------------------------=== #


struct Shape(Writable, Copyable, Movable):
    """Runtime tensor shape as a list of dimension sizes."""

    var _dims: List[Int]

    # --- Constructors ---

    fn __init__(out self, *dims: Int):
        """Create a shape from variadic dimension sizes."""
        self._dims = List[Int]()
        for i in range(len(dims)):
            self._dims.append(dims[i])

    fn __init__(out self, var dims: List[Int]):
        """Create a shape from a List[Int]."""
        self._dims = dims^

    fn __copyinit__(out self, other: Self):
        """Copy constructor."""
        self._dims = other._dims.copy()

    fn __moveinit__(out self, deinit other: Self):
        """Move constructor."""
        self._dims = other._dims^

    # --- Properties ---

    @always_inline
    fn ndim(self) -> Int:
        """Returns the number of dimensions."""
        return len(self._dims)

    fn numel(self) -> Int:
        """Returns the total number of elements (product of all dimensions)."""
        if self.ndim() == 0:
            return 0
        var total = 1
        for i in range(self.ndim()):
            total *= self._dims[i]
        return total

    fn __getitem__(self, idx: Int) -> Int:
        """Returns the size of dimension at index idx. Supports negative indexing."""
        var actual = idx
        if actual < 0:
            actual += self.ndim()
        return self._dims[actual]

    fn copy(self) -> Shape:
        """Returns an explicit copy of this shape."""
        return Shape(self._dims.copy())

    # --- Strides ---

    fn strides(self) -> List[Int]:
        """Returns row-major (C-contiguous) strides for this shape."""
        var n = self.ndim()
        var s = List[Int]()
        for _ in range(n):
            s.append(0)
        if n == 0:
            return s^
        s[n - 1] = 1
        var i = n - 2
        while i >= 0:
            s[i] = s[i + 1] * self._dims[i + 1]
            i -= 1
        return s^

    # --- Broadcasting ---

    fn broadcast_with(self, other: Shape) raises -> Shape:
        """Computes the broadcast-compatible output shape using NumPy rules."""
        var ndim_out = max(self.ndim(), other.ndim())
        var result = List[Int]()
        for _ in range(ndim_out):
            result.append(0)

        for i in range(ndim_out):
            var idx_a = self.ndim() - ndim_out + i
            var idx_b = other.ndim() - ndim_out + i

            var sa: Int = 1
            if idx_a >= 0:
                sa = self._dims[idx_a]

            var sb: Int = 1
            if idx_b >= 0:
                sb = other._dims[idx_b]

            if sa == sb:
                result[i] = sa
            elif sa == 1:
                result[i] = sb
            elif sb == 1:
                result[i] = sa
            else:
                raise Error(
                    "Cannot broadcast shapes: dimensions "
                    + String(sa)
                    + " and "
                    + String(sb)
                    + " are incompatible"
                )

        return Shape(result^)

    # --- Reshape validation ---

    fn reshape_valid(self, new_shape: Shape) -> Bool:
        """Returns True if this shape can be reshaped to new_shape."""
        return self.numel() == new_shape.numel()

    # --- Equality ---

    fn __eq__(self, other: Shape) -> Bool:
        if self.ndim() != other.ndim():
            return False
        for i in range(self.ndim()):
            if self._dims[i] != other._dims[i]:
                return False
        return True

    fn __ne__(self, other: Shape) -> Bool:
        return not (self == other)

    # --- Writable ---

    fn write_to[W: Writer](self, mut writer: W):
        writer.write("Shape(")
        for i in range(self.ndim()):
            if i > 0:
                writer.write(", ")
            writer.write(self._dims[i])
        writer.write(")")
