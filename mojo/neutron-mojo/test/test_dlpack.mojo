# ===----------------------------------------------------------------------=== #
# Tests for dlpack/dlpack.mojo — struct definitions and dtype conversions
# ===----------------------------------------------------------------------=== #

"""Tests: struct sizes, dtype round-trips, field access, flags."""

from testing import assert_true, assert_false, assert_equal

from neutron_mojo.dlpack.dlpack import (
    DLDataType,
    DLDevice,
    DLPackVersion,
    DLTensor,
    DLManagedTensorVersioned,
    kDLInt,
    kDLUInt,
    kDLFloat,
    kDLBfloat,
    kDLBool,
    kDLCPU,
    kDLCUDA,
    kDLMetal,
    kDLROCM,
    DLPACK_FLAG_BITMASK_READ_ONLY,
    DLPACK_FLAG_BITMASK_IS_COPIED,
    DLPACK_VERSION,
    mojo_dtype_to_dl,
    dl_to_mojo_dtype,
)


# ===----------------------------------------------------------------------=== #
# DLDataType tests
# ===----------------------------------------------------------------------=== #


fn test_dl_datatype_float32() raises:
    var dt = DLDataType(kDLFloat, 32, 1)
    assert_equal(dt.code, kDLFloat)
    assert_equal(dt.bits, UInt8(32))
    assert_equal(dt.lanes, UInt16(1))
    print("  dl_datatype_float32: PASS")


fn test_dl_datatype_equality() raises:
    var a = DLDataType(kDLFloat, 32, 1)
    var b = DLDataType(kDLFloat, 32, 1)
    var c = DLDataType(kDLFloat, 16, 1)
    assert_true(a == b)
    assert_true(a != c)
    print("  dl_datatype_equality: PASS")


fn test_dl_datatype_writable() raises:
    var dt = DLDataType(kDLInt, 8, 1)
    var s = String(dt)
    assert_true("DLDataType" in s)
    print("  dl_datatype_writable: PASS")


# ===----------------------------------------------------------------------=== #
# DLDevice tests
# ===----------------------------------------------------------------------=== #


fn test_dl_device_cpu() raises:
    var dev = DLDevice(kDLCPU, 0)
    assert_equal(dev.device_type, kDLCPU)
    assert_equal(dev.device_id, Int32(0))
    print("  dl_device_cpu: PASS")


fn test_dl_device_cuda() raises:
    var dev = DLDevice(kDLCUDA, 1)
    assert_equal(dev.device_type, kDLCUDA)
    assert_equal(dev.device_id, Int32(1))
    print("  dl_device_cuda: PASS")


fn test_dl_device_equality() raises:
    var a = DLDevice(kDLCPU, 0)
    var b = DLDevice(kDLCPU, 0)
    var c = DLDevice(kDLCUDA, 0)
    assert_true(a == b)
    assert_true(a != c)
    print("  dl_device_equality: PASS")


# ===----------------------------------------------------------------------=== #
# DLPackVersion tests
# ===----------------------------------------------------------------------=== #


fn test_dl_version() raises:
    assert_equal(DLPACK_VERSION.major, UInt32(1))
    assert_equal(DLPACK_VERSION.minor, UInt32(0))
    var s = String(DLPACK_VERSION)
    assert_true("1" in s)
    print("  dl_version: PASS")


fn test_dl_version_equality() raises:
    var a = DLPackVersion(1, 0)
    var b = DLPackVersion(1, 0)
    var c = DLPackVersion(2, 0)
    assert_true(a == b)
    assert_true(a != c)
    print("  dl_version_equality: PASS")


# ===----------------------------------------------------------------------=== #
# DLTensor tests
# ===----------------------------------------------------------------------=== #


fn test_dl_tensor_default() raises:
    var t = DLTensor()
    assert_equal(t.ndim, Int32(0))
    assert_equal(t.byte_offset, UInt64(0))
    assert_equal(t.device.device_type, kDLCPU)
    assert_equal(t.dtype.code, kDLFloat)
    assert_equal(t.dtype.bits, UInt8(32))
    print("  dl_tensor_default: PASS")


# ===----------------------------------------------------------------------=== #
# DLManagedTensorVersioned tests
# ===----------------------------------------------------------------------=== #


fn test_dl_managed_default() raises:
    var mt = DLManagedTensorVersioned()
    assert_equal(mt.version.major, UInt32(1))
    assert_equal(mt.version.minor, UInt32(0))
    assert_equal(mt.flags, UInt64(0))
    assert_false(mt.is_read_only())
    assert_false(mt.is_copied())
    print("  dl_managed_default: PASS")


fn test_dl_managed_flags() raises:
    var mt = DLManagedTensorVersioned()
    mt.flags = DLPACK_FLAG_BITMASK_READ_ONLY
    assert_true(mt.is_read_only())
    assert_false(mt.is_copied())

    mt.flags = DLPACK_FLAG_BITMASK_IS_COPIED
    assert_false(mt.is_read_only())
    assert_true(mt.is_copied())

    mt.flags = DLPACK_FLAG_BITMASK_READ_ONLY | DLPACK_FLAG_BITMASK_IS_COPIED
    assert_true(mt.is_read_only())
    assert_true(mt.is_copied())
    print("  dl_managed_flags: PASS")


# ===----------------------------------------------------------------------=== #
# DType conversion round-trips
# ===----------------------------------------------------------------------=== #


fn test_mojo_to_dl_float32() raises:
    var dl = mojo_dtype_to_dl(DType.float32)
    assert_equal(dl.code, kDLFloat)
    assert_equal(dl.bits, UInt8(32))
    assert_equal(dl.lanes, UInt16(1))
    print("  mojo_to_dl_float32: PASS")


fn test_mojo_to_dl_bfloat16() raises:
    var dl = mojo_dtype_to_dl(DType.bfloat16)
    assert_equal(dl.code, kDLBfloat)
    assert_equal(dl.bits, UInt8(16))
    print("  mojo_to_dl_bfloat16: PASS")


fn test_mojo_to_dl_bool() raises:
    var dl = mojo_dtype_to_dl(DType.bool)
    assert_equal(dl.code, kDLBool)
    assert_equal(dl.bits, UInt8(8))
    print("  mojo_to_dl_bool: PASS")


fn test_dl_to_mojo_round_trip() raises:
    """Round-trip: Mojo DType -> DLDataType -> Mojo DType."""
    var types = List[DType]()
    types.append(DType.float16)
    types.append(DType.float32)
    types.append(DType.float64)
    types.append(DType.bfloat16)
    types.append(DType.int8)
    types.append(DType.int16)
    types.append(DType.int32)
    types.append(DType.int64)
    types.append(DType.uint8)
    types.append(DType.uint16)
    types.append(DType.uint32)
    types.append(DType.uint64)
    types.append(DType.bool)

    for i in range(len(types)):
        var dt = types[i]
        var dl = mojo_dtype_to_dl(dt)
        var back = dl_to_mojo_dtype(dl)
        assert_true(back == dt)

    print("  dl_to_mojo_round_trip: PASS")


fn test_dl_to_mojo_unknown() raises:
    """Unknown DLDataType should map to DType.invalid."""
    var dl = DLDataType(99, 99, 1)
    var result = dl_to_mojo_dtype(dl)
    assert_true(result == DType.invalid)
    print("  dl_to_mojo_unknown: PASS")


# --- Main ---


fn main() raises:
    print("test_dlpack:")

    # DLDataType
    test_dl_datatype_float32()
    test_dl_datatype_equality()
    test_dl_datatype_writable()

    # DLDevice
    test_dl_device_cpu()
    test_dl_device_cuda()
    test_dl_device_equality()

    # DLPackVersion
    test_dl_version()
    test_dl_version_equality()

    # DLTensor
    test_dl_tensor_default()

    # DLManagedTensorVersioned
    test_dl_managed_default()
    test_dl_managed_flags()

    # DType conversions
    test_mojo_to_dl_float32()
    test_mojo_to_dl_bfloat16()
    test_mojo_to_dl_bool()
    test_dl_to_mojo_round_trip()
    test_dl_to_mojo_unknown()

    print("ALL PASSED")
