# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Device storage (CPU-only in Sprint 1)
# ===----------------------------------------------------------------------=== #

"""Typed device memory management for tensor data.

Storage[dtype] owns a contiguous block of memory and provides typed
load/store with SIMD support. CPU-only in Sprint 1; GPU DeviceBuffer
support planned for Sprint 2.
"""

from std.memory import memcpy, memset_zero, alloc
from std.sys import size_of


# ===----------------------------------------------------------------------=== #
# DeviceKind — device type enum
# ===----------------------------------------------------------------------=== #


struct DeviceKind(Writable, TrivialRegisterPassable):
    """Enumerates supported device types using integer codes."""

    var _value: Int

    comptime CPU = DeviceKind(0)
    comptime CUDA = DeviceKind(1)
    comptime ROCm = DeviceKind(2)
    comptime Metal = DeviceKind(3)

    @implicit
    def __init__(out self, value: Int):
        self._value = value

    def __eq__(self, other: DeviceKind) -> Bool:
        return self._value == other._value

    def __ne__(self, other: DeviceKind) -> Bool:
        return self._value != other._value

    def write_to(self, mut writer: Some[Writer]):
        if self._value == 0:
            writer.write("CPU")
        elif self._value == 1:
            writer.write("CUDA")
        elif self._value == 2:
            writer.write("ROCm")
        elif self._value == 3:
            writer.write("Metal")
        else:
            writer.write("Unknown")


# ===----------------------------------------------------------------------=== #
# Storage — typed device memory
# ===----------------------------------------------------------------------=== #


struct Storage[dtype: DType](Movable):
    """Owns a contiguous block of typed memory for tensor data.

    Manages allocation and deallocation via RAII. Provides scalar and
    SIMD load/store operations. CPU-only in Sprint 1.
    """

    var _ptr: Optional[Pointer[Scalar[Self.dtype], MutUntrackedOrigin]]
    var _size: Int  # number of elements (not bytes)
    var _device: DeviceKind

    # --- Constructors ---

    def __init__(out self, size: Int, device: DeviceKind = DeviceKind.CPU):
        """Allocate storage for `size` elements, zero-initialized."""
        self._size = size
        self._device = device
        self._ptr = alloc[Scalar[Self.dtype]](size)
        memset_zero(self._ptr.unsafe_value(), size)

    def __init__(out self, *, deinit move: Self):
        """Move constructor — transfers ownership; the source is left empty."""
        self._size = move._size
        self._device = move._device
        self._ptr = move._ptr
        move._ptr = None

    def __deinit__(deinit self):
        """Frees the underlying memory."""
        if self._ptr:
            self._ptr.unsafe_value().free()

    # --- Scalar access ---

    @always_inline
    def load(self, offset: Int) -> Scalar[Self.dtype]:
        """Load a single scalar element at the given offset."""
        return self._ptr.unsafe_value().load(offset)

    @always_inline
    def store(self, offset: Int, value: Scalar[Self.dtype]):
        """Store a single scalar element at the given offset."""
        self._ptr.unsafe_value().store(offset, value)

    # --- SIMD access ---

    @always_inline
    def load_simd[width: Int](self, offset: Int) -> SIMD[Self.dtype, width]:
        """Load a SIMD vector of `width` contiguous elements."""
        return self._ptr.unsafe_value().load[width=width](offset)

    @always_inline
    def store_simd[width: Int](self, offset: Int, value: SIMD[Self.dtype, width]):
        """Store a SIMD vector of `width` contiguous elements."""
        self._ptr.unsafe_value().store(offset, value)

    # --- Bulk operations ---

    def fill(self, value: Scalar[Self.dtype]):
        """Fill all elements with the given value."""
        for i in range(self._size):
            self._ptr.unsafe_value().store(i, value)

    def copy_from(self, other: Storage[Self.dtype]):
        """Copy data from another storage. Sizes must match."""
        debug_assert(self._size == other._size, "Storage size mismatch in copy_from")
        memcpy(
            dest=self._ptr.unsafe_value(),
            src=other._ptr.unsafe_value(),
            count=self._size,
        )

    # --- Properties ---

    @always_inline
    def unsafe_ptr(self) -> Pointer[Scalar[Self.dtype], MutUntrackedOrigin]:
        """Returns the raw pointer to the underlying data."""
        return self._ptr.unsafe_value()

    @always_inline
    def size(self) -> Int:
        """Returns the number of elements."""
        return self._size

    def size_bytes(self) -> Int:
        """Returns the total size in bytes."""
        return self._size * size_of[Scalar[Self.dtype]]()

    @always_inline
    def device(self) -> DeviceKind:
        """Returns the device kind."""
        return self._device
