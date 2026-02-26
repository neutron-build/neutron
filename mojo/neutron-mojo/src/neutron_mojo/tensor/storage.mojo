# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Device storage (CPU-only in Sprint 1)
# ===----------------------------------------------------------------------=== #

"""Typed device memory management for tensor data.

Storage[dtype] owns a contiguous block of memory and provides typed
load/store with SIMD support. CPU-only in Sprint 1; GPU DeviceBuffer
support planned for Sprint 2.
"""

from memory import memcpy, memset_zero, alloc
from sys import size_of


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
    fn __init__(out self, value: Int):
        self._value = value

    fn __eq__(self, other: DeviceKind) -> Bool:
        return self._value == other._value

    fn __ne__(self, other: DeviceKind) -> Bool:
        return self._value != other._value

    fn write_to[W: Writer](self, mut writer: W):
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

    var _ptr: UnsafePointer[Scalar[Self.dtype], MutExternalOrigin]
    var _size: Int  # number of elements (not bytes)
    var _device: DeviceKind

    # --- Constructors ---

    fn __init__(out self, size: Int, device: DeviceKind = DeviceKind.CPU):
        """Allocate storage for `size` elements, zero-initialized."""
        self._size = size
        self._device = device
        self._ptr = alloc[Scalar[Self.dtype]](size)
        memset_zero(self._ptr, size)

    fn __moveinit__(out self, deinit other: Self):
        """Move constructor — transfers ownership."""
        self._ptr = other._ptr
        self._size = other._size
        self._device = other._device

    fn __del__(deinit self):
        """Frees the underlying memory."""
        if self._ptr:
            self._ptr.free()

    # --- Scalar access ---

    @always_inline
    fn load(self, offset: Int) -> Scalar[Self.dtype]:
        """Load a single scalar element at the given offset."""
        return self._ptr.load(offset)

    @always_inline
    fn store(self, offset: Int, value: Scalar[Self.dtype]):
        """Store a single scalar element at the given offset."""
        self._ptr.store(offset, value)

    # --- SIMD access ---

    @always_inline
    fn load_simd[width: Int](self, offset: Int) -> SIMD[Self.dtype, width]:
        """Load a SIMD vector of `width` contiguous elements."""
        return self._ptr.load[width=width](offset)

    @always_inline
    fn store_simd[width: Int](self, offset: Int, value: SIMD[Self.dtype, width]):
        """Store a SIMD vector of `width` contiguous elements."""
        self._ptr.store(offset, value)

    # --- Bulk operations ---

    fn fill(self, value: Scalar[Self.dtype]):
        """Fill all elements with the given value."""
        for i in range(self._size):
            self._ptr.store(i, value)

    fn copy_from(self, other: Storage[Self.dtype]):
        """Copy data from another storage. Sizes must match."""
        debug_assert(self._size == other._size, "Storage size mismatch in copy_from")
        memcpy(dest=self._ptr, src=other._ptr, count=self._size)

    # --- Properties ---

    @always_inline
    fn unsafe_ptr(self) -> UnsafePointer[Scalar[Self.dtype], MutExternalOrigin]:
        """Returns the raw pointer to the underlying data."""
        return self._ptr

    @always_inline
    fn size(self) -> Int:
        """Returns the number of elements."""
        return self._size

    fn size_bytes(self) -> Int:
        """Returns the total size in bytes."""
        return self._size * size_of[Scalar[Self.dtype]]()

    @always_inline
    fn device(self) -> DeviceKind:
        """Returns the device kind."""
        return self._device
