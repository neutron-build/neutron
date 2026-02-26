# ===----------------------------------------------------------------------=== #
# Neutron Mojo — GGUF Parser Tests
# ===----------------------------------------------------------------------=== #

"""Tests for GGUF file format parser."""

from neutron_mojo.io.gguf import (
    GGUF_MAGIC,
    GGUFTensorType,
    GGUF_F32,
    GGUF_F16,
    GGUF_Q4_0,
    GGUF_Q4_1,
    GGUF_Q8_0,
    GGUF_Q4_K,
    GGUFTensorInfo,
    GGUFFile,
    gguf_type_to_dtype,
    dtype_to_gguf_type,
    calculate_tensor_size,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_gguf_tensor_type() raises:
    """Test GGUF tensor type enum."""
    var t_f32 = GGUF_F32()
    var t_f16 = GGUF_F16()
    var t_q4_0 = GGUF_Q4_0()
    var t_q8_0 = GGUF_Q8_0()

    assert_true(t_f32 == GGUF_F32(), "F32 type should match")
    assert_true(t_f16 != t_f32, "F16 and F32 should differ")
    assert_true(t_q4_0 == GGUF_Q4_0(), "Q4_0 type should match")

    print("  gguf_tensor_type: PASS")


fn test_gguf_tensor_info_creation() raises:
    """Test GGUFTensorInfo struct creation."""
    var info = GGUFTensorInfo()
    info.name = String("embedding.weight")
    info.n_dims = 2
    info.shape = List[Int]()
    info.shape.append(1024)
    info.shape.append(4096)
    info.tensor_type = GGUF_F16()
    info.offset = 0

    assert_true(info.numel() == 4194304, "1024*4096 = 4194304 elements")
    assert_true(info.n_dims == 2, "Should have 2 dimensions")

    print("  gguf_tensor_info_creation: PASS")


fn test_gguf_file_creation() raises:
    """Test GGUFFile struct creation."""
    var gguf = GGUFFile()

    assert_true(gguf.magic == 0, "Initial magic should be 0")
    assert_true(gguf.version == 0, "Initial version should be 0")
    assert_true(gguf.tensor_count == 0, "Initial tensor count should be 0")

    print("  gguf_file_creation: PASS")


fn test_gguf_magic_validation() raises:
    """Test GGUF magic number validation."""
    var gguf = GGUFFile()

    gguf.magic = GGUF_MAGIC
    assert_true(gguf.is_valid(), "Should be valid with correct magic")

    gguf.magic = 0x12345678
    assert_true(not gguf.is_valid(), "Should be invalid with wrong magic")

    print("  gguf_magic_validation: PASS")


fn test_gguf_register_tensor() raises:
    """Test manual tensor registration."""
    var gguf = GGUFFile()

    var shape = List[Int]()
    shape.append(512)
    shape.append(2048)

    gguf.register_tensor("layer.weight", shape, GGUF_F32(), 0)

    assert_true(gguf.has_tensor("layer.weight"), "Should have registered tensor")
    assert_true(not gguf.has_tensor("other.weight"), "Should not have unregistered tensor")

    print("  gguf_register_tensor: PASS")


fn test_gguf_get_tensor_info() raises:
    """Test getting tensor metadata."""
    var gguf = GGUFFile()

    var shape = List[Int]()
    shape.append(256)
    shape.append(256)
    shape.append(3)

    gguf.register_tensor("conv.weight", shape, GGUF_F16(), 1024)

    var info = gguf.get_tensor_info("conv.weight")

    assert_true(info.name == "conv.weight", "Name should match")
    assert_true(info.n_dims == 3, "Should have 3 dimensions")
    assert_true(info.shape[0] == 256, "First dim should be 256")
    assert_true(info.tensor_type == GGUF_F16(), "Type should be F16")
    assert_true(info.offset == 1024, "Offset should be 1024")

    print("  gguf_get_tensor_info: PASS")


fn test_gguf_tensor_offset() raises:
    """Test calculating absolute tensor offset."""
    var gguf = GGUFFile()
    gguf.data_offset = 5000  # Simulate header size

    var shape = List[Int]()
    shape.append(128)

    gguf.register_tensor("bias", shape, GGUF_F32(), 2000)

    var abs_offset = gguf.get_tensor_offset("bias")
    assert_true(abs_offset == 7000, "Absolute offset should be 5000 + 2000")

    print("  gguf_tensor_offset: PASS")


fn test_gguf_type_to_dtype() raises:
    """Test converting GGUF types to DType."""
    var dt_f32 = gguf_type_to_dtype(GGUF_F32())
    var dt_f16 = gguf_type_to_dtype(GGUF_F16())

    assert_true(dt_f32 == DType.float32, "F32 should map to float32")
    assert_true(dt_f16 == DType.float16, "F16 should map to float16")

    # Quantized types map to uint8 (placeholder)
    var dt_q4 = gguf_type_to_dtype(GGUF_Q4_0())
    assert_true(dt_q4 == DType.uint8, "Q4_0 should map to uint8")

    print("  gguf_type_to_dtype: PASS")


fn test_dtype_to_gguf_type() raises:
    """Test converting DType to GGUF types."""
    var t_f32 = dtype_to_gguf_type(DType.float32)
    var t_f16 = dtype_to_gguf_type(DType.float16)

    assert_true(t_f32 == GGUF_F32(), "float32 should map to F32")
    assert_true(t_f16 == GGUF_F16(), "float16 should map to F16")

    print("  dtype_to_gguf_type: PASS")


fn test_calculate_tensor_size_f32() raises:
    """Test calculating F32 tensor size."""
    var shape = List[Int]()
    shape.append(100)
    shape.append(200)

    # 100 * 200 * 4 bytes = 80000 bytes
    var size = calculate_tensor_size(shape, GGUF_F32())
    assert_true(size == 80000, "F32 tensor should be 80000 bytes")

    print("  calculate_tensor_size_f32: PASS")


fn test_calculate_tensor_size_f16() raises:
    """Test calculating F16 tensor size."""
    var shape = List[Int]()
    shape.append(512)
    shape.append(1024)

    # 512 * 1024 * 2 bytes = 1048576 bytes
    var size = calculate_tensor_size(shape, GGUF_F16())
    assert_true(size == 1048576, "F16 tensor should be 1048576 bytes")

    print("  calculate_tensor_size_f16: PASS")


fn test_calculate_tensor_size_q4_0() raises:
    """Test calculating Q4_0 tensor size."""
    var shape = List[Int]()
    shape.append(1024)  # 32 blocks * 18 bytes = 576 bytes

    var size = calculate_tensor_size(shape, GGUF_Q4_0())
    assert_true(size == 576, "Q4_0 with 1024 elements should be 576 bytes")

    print("  calculate_tensor_size_q4_0: PASS")


fn test_calculate_tensor_size_q8_0() raises:
    """Test calculating Q8_0 tensor size."""
    var shape = List[Int]()
    shape.append(1024)  # 32 blocks * 34 bytes = 1088 bytes

    var size = calculate_tensor_size(shape, GGUF_Q8_0())
    assert_true(size == 1088, "Q8_0 with 1024 elements should be 1088 bytes")

    print("  calculate_tensor_size_q8_0: PASS")


fn test_multiple_gguf_tensors() raises:
    """Test registering multiple tensors."""
    var gguf = GGUFFile()
    gguf.tensor_count = 3

    var shape1 = List[Int]()
    shape1.append(768)

    var shape2 = List[Int]()
    shape2.append(768)
    shape2.append(3072)

    var shape3 = List[Int]()
    shape3.append(3072)
    shape3.append(768)

    gguf.register_tensor("embed", shape1, GGUF_F16(), 0)
    gguf.register_tensor("mlp.up", shape2, GGUF_Q4_0(), 1536)
    gguf.register_tensor("mlp.down", shape3, GGUF_Q8_0(), 100000)

    assert_true(gguf.has_tensor("embed"), "Should have embed tensor")
    assert_true(gguf.has_tensor("mlp.up"), "Should have mlp.up tensor")
    assert_true(gguf.has_tensor("mlp.down"), "Should have mlp.down tensor")

    var info1 = gguf.get_tensor_info("embed")
    var info2 = gguf.get_tensor_info("mlp.up")
    var info3 = gguf.get_tensor_info("mlp.down")

    assert_true(info1.tensor_type == GGUF_F16(), "embed should be F16")
    assert_true(info2.tensor_type == GGUF_Q4_0(), "mlp.up should be Q4_0")
    assert_true(info3.tensor_type == GGUF_Q8_0(), "mlp.down should be Q8_0")

    print("  multiple_gguf_tensors: PASS")


fn main() raises:
    print("test_gguf:")

    test_gguf_tensor_type()
    test_gguf_tensor_info_creation()
    test_gguf_file_creation()
    test_gguf_magic_validation()
    test_gguf_register_tensor()
    test_gguf_get_tensor_info()
    test_gguf_tensor_offset()
    test_gguf_type_to_dtype()
    test_dtype_to_gguf_type()
    test_calculate_tensor_size_f32()
    test_calculate_tensor_size_f16()
    test_calculate_tensor_size_q4_0()
    test_calculate_tensor_size_q8_0()
    test_multiple_gguf_tensors()

    print("ALL PASSED")
