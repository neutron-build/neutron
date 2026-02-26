# ===----------------------------------------------------------------------=== #
# Neutron Mojo — DLPack C ABI interop structs
# ===----------------------------------------------------------------------=== #

"""DLPack v1.0 C ABI struct definitions and Mojo DType conversion.

These structs mirror the C layout defined in dlpack.h exactly, enabling
zero-copy tensor exchange with PyTorch, JAX, NumPy, and other DLPack-
compliant frameworks.

All small value types use TrivialRegisterPassable for efficient
pass-by-value semantics matching C ABI expectations.
"""

from memory import UnsafePointer


# ===----------------------------------------------------------------------=== #
# DLPack constants
# ===----------------------------------------------------------------------=== #

# DLDataTypeCode
comptime kDLInt: UInt8 = 0
comptime kDLUInt: UInt8 = 1
comptime kDLFloat: UInt8 = 2
comptime kDLOpaqueHandle: UInt8 = 3
comptime kDLBfloat: UInt8 = 4
comptime kDLComplex: UInt8 = 5
comptime kDLBool: UInt8 = 6

# DLDeviceType
comptime kDLCPU: Int32 = 1
comptime kDLCUDA: Int32 = 2
comptime kDLCUDAHost: Int32 = 3
comptime kDLOpenCL: Int32 = 4
comptime kDLVulkan: Int32 = 7
comptime kDLMetal: Int32 = 8
comptime kDLROCM: Int32 = 10
comptime kDLROCMHost: Int32 = 11
comptime kDLCUDAManaged: Int32 = 13
comptime kDLOneAPI: Int32 = 14
comptime kDLWebGPU: Int32 = 15

# Flags
comptime DLPACK_FLAG_BITMASK_READ_ONLY: UInt64 = 1  # 1 << 0
comptime DLPACK_FLAG_BITMASK_IS_COPIED: UInt64 = 2  # 1 << 1


# ===----------------------------------------------------------------------=== #
# DLDataType — 4 bytes: {code: u8, bits: u8, lanes: u16}
# ===----------------------------------------------------------------------=== #


struct DLDataType(Writable, TrivialRegisterPassable):
    """DLPack data type descriptor (4 bytes, matching dlpack.h DLDataType).

    Fields:
    - code: DLDataTypeCode (kDLInt=0, kDLUInt=1, kDLFloat=2, etc.)
    - bits: bit-width per element (e.g., 32 for float32)
    - lanes: SIMD lanes (1 for scalar)
    """

    var code: UInt8
    var bits: UInt8
    var lanes: UInt16

    fn __init__(out self, code: UInt8, bits: UInt8, lanes: UInt16):
        self.code = code
        self.bits = bits
        self.lanes = lanes

    fn __eq__(self, other: DLDataType) -> Bool:
        return (
            self.code == other.code
            and self.bits == other.bits
            and self.lanes == other.lanes
        )

    fn __ne__(self, other: DLDataType) -> Bool:
        return not (self == other)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(
            "DLDataType(code=", self.code,
            ", bits=", self.bits,
            ", lanes=", self.lanes, ")",
        )


# ===----------------------------------------------------------------------=== #
# DLDevice — 8 bytes: {device_type: i32, device_id: i32}
# ===----------------------------------------------------------------------=== #


struct DLDevice(Writable, TrivialRegisterPassable):
    """DLPack device descriptor (8 bytes, matching dlpack.h DLDevice)."""

    var device_type: Int32
    var device_id: Int32

    fn __init__(out self, device_type: Int32, device_id: Int32):
        self.device_type = device_type
        self.device_id = device_id

    fn __eq__(self, other: DLDevice) -> Bool:
        return (
            self.device_type == other.device_type
            and self.device_id == other.device_id
        )

    fn __ne__(self, other: DLDevice) -> Bool:
        return not (self == other)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(
            "DLDevice(type=", self.device_type,
            ", id=", self.device_id, ")",
        )


# ===----------------------------------------------------------------------=== #
# DLPackVersion — 8 bytes: {major: u32, minor: u32}
# ===----------------------------------------------------------------------=== #


struct DLPackVersion(Writable, TrivialRegisterPassable):
    """DLPack protocol version (8 bytes)."""

    var major: UInt32
    var minor: UInt32

    fn __init__(out self, major: UInt32, minor: UInt32):
        self.major = major
        self.minor = minor

    fn __eq__(self, other: DLPackVersion) -> Bool:
        return self.major == other.major and self.minor == other.minor

    fn __ne__(self, other: DLPackVersion) -> Bool:
        return not (self == other)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.major, ".", self.minor)


# Current version
comptime DLPACK_VERSION = DLPackVersion(1, 0)


# ===----------------------------------------------------------------------=== #
# DLTensor — 48 bytes on 64-bit platforms
# ===----------------------------------------------------------------------=== #


struct DLTensor(ImplicitlyCopyable, Copyable, Movable):
    """DLPack tensor descriptor (48 bytes on 64-bit, matching dlpack.h).

    Describes a tensor's memory layout without owning the data.
    Strides are in elements, not bytes. NULL strides = C-contiguous.
    """

    var data: UnsafePointer[UInt8, MutExternalOrigin]
    var device: DLDevice
    var ndim: Int32
    var dtype: DLDataType
    var shape: UnsafePointer[Int64, MutExternalOrigin]
    var strides: UnsafePointer[Int64, MutExternalOrigin]
    var byte_offset: UInt64

    fn __init__(out self):
        """Create a zero-initialized DLTensor."""
        self.data = UnsafePointer[UInt8, MutExternalOrigin]()
        self.device = DLDevice(kDLCPU, 0)
        self.ndim = 0
        self.dtype = DLDataType(kDLFloat, 32, 1)
        self.shape = UnsafePointer[Int64, MutExternalOrigin]()
        self.strides = UnsafePointer[Int64, MutExternalOrigin]()
        self.byte_offset = 0

    fn __init__(
        out self,
        data: UnsafePointer[UInt8, MutExternalOrigin],
        device: DLDevice,
        ndim: Int32,
        dtype: DLDataType,
        shape: UnsafePointer[Int64, MutExternalOrigin],
        strides: UnsafePointer[Int64, MutExternalOrigin],
        byte_offset: UInt64,
    ):
        self.data = data
        self.device = device
        self.ndim = ndim
        self.dtype = dtype
        self.shape = shape
        self.strides = strides
        self.byte_offset = byte_offset

    fn __copyinit__(out self, other: Self):
        self.data = other.data
        self.device = other.device
        self.ndim = other.ndim
        self.dtype = other.dtype
        self.shape = other.shape
        self.strides = other.strides
        self.byte_offset = other.byte_offset

    fn __moveinit__(out self, deinit other: Self):
        self.data = other.data
        self.device = other.device
        self.ndim = other.ndim
        self.dtype = other.dtype
        self.shape = other.shape
        self.strides = other.strides
        self.byte_offset = other.byte_offset


# ===----------------------------------------------------------------------=== #
# DLManagedTensorVersioned — 80 bytes on 64-bit platforms (v1.0+)
# ===----------------------------------------------------------------------=== #


struct DLManagedTensorVersioned(ImplicitlyCopyable, Copyable, Movable):
    """DLPack v1.0 managed tensor with versioning, flags, and deleter.

    Layout (64-bit):
      offset 0:  version      (8B)
      offset 8:  manager_ctx  (8B)
      offset 16: deleter      (8B) — function pointer, stored as UnsafePointer
      offset 24: flags        (8B)
      offset 32: dl_tensor    (48B)
    Total: 80 bytes
    """

    var version: DLPackVersion
    var manager_ctx: UnsafePointer[UInt8, MutExternalOrigin]
    var deleter_ctx: UnsafePointer[UInt8, MutExternalOrigin]
    var flags: UInt64
    var dl_tensor: DLTensor

    fn __init__(out self):
        """Create a zero-initialized managed tensor."""
        self.version = DLPACK_VERSION
        self.manager_ctx = UnsafePointer[UInt8, MutExternalOrigin]()
        self.deleter_ctx = UnsafePointer[UInt8, MutExternalOrigin]()
        self.flags = 0
        self.dl_tensor = DLTensor()

    fn __copyinit__(out self, other: Self):
        self.version = other.version
        self.manager_ctx = other.manager_ctx
        self.deleter_ctx = other.deleter_ctx
        self.flags = other.flags
        self.dl_tensor = other.dl_tensor

    fn __moveinit__(out self, deinit other: Self):
        self.version = other.version
        self.manager_ctx = other.manager_ctx
        self.deleter_ctx = other.deleter_ctx
        self.flags = other.flags
        self.dl_tensor = other.dl_tensor

    fn is_read_only(self) -> Bool:
        """Check if the read-only flag is set."""
        return (self.flags & DLPACK_FLAG_BITMASK_READ_ONLY) != 0

    fn is_copied(self) -> Bool:
        """Check if the is-copied flag is set."""
        return (self.flags & DLPACK_FLAG_BITMASK_IS_COPIED) != 0


# ===----------------------------------------------------------------------=== #
# DType conversion functions
# ===----------------------------------------------------------------------=== #


fn mojo_dtype_to_dl(dtype: DType) -> DLDataType:
    """Convert a Mojo DType to a DLPack DLDataType."""
    if dtype == DType.bool:
        return DLDataType(kDLBool, 8, 1)
    if dtype == DType.int8:
        return DLDataType(kDLInt, 8, 1)
    if dtype == DType.int16:
        return DLDataType(kDLInt, 16, 1)
    if dtype == DType.int32:
        return DLDataType(kDLInt, 32, 1)
    if dtype == DType.int64:
        return DLDataType(kDLInt, 64, 1)
    if dtype == DType.uint8:
        return DLDataType(kDLUInt, 8, 1)
    if dtype == DType.uint16:
        return DLDataType(kDLUInt, 16, 1)
    if dtype == DType.uint32:
        return DLDataType(kDLUInt, 32, 1)
    if dtype == DType.uint64:
        return DLDataType(kDLUInt, 64, 1)
    if dtype == DType.float16:
        return DLDataType(kDLFloat, 16, 1)
    if dtype == DType.float32:
        return DLDataType(kDLFloat, 32, 1)
    if dtype == DType.float64:
        return DLDataType(kDLFloat, 64, 1)
    if dtype == DType.bfloat16:
        return DLDataType(kDLBfloat, 16, 1)
    # Fallback: invalid
    return DLDataType(0, 0, 0)


fn dl_to_mojo_dtype(dl: DLDataType) -> DType:
    """Convert a DLPack DLDataType back to a Mojo DType.

    Returns DType.invalid for unrecognized combinations.
    """
    var code = dl.code
    var bits = Int(dl.bits)

    if code == kDLBool:
        return DType.bool
    if code == kDLBfloat and bits == 16:
        return DType.bfloat16
    if code == kDLFloat:
        if bits == 16:
            return DType.float16
        if bits == 32:
            return DType.float32
        if bits == 64:
            return DType.float64
    if code == kDLInt:
        if bits == 8:
            return DType.int8
        if bits == 16:
            return DType.int16
        if bits == 32:
            return DType.int32
        if bits == 64:
            return DType.int64
    if code == kDLUInt:
        if bits == 8:
            return DType.uint8
        if bits == 16:
            return DType.uint16
        if bits == 32:
            return DType.uint32
        if bits == 64:
            return DType.uint64
    return DType.invalid
