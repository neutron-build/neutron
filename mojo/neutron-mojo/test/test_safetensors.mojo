# ===----------------------------------------------------------------------=== #
# Neutron Mojo — SafeTensors Parser Tests
# ===----------------------------------------------------------------------=== #

"""Tests for SafeTensors file format parser."""

from neutron_mojo.io.safetensors import (
    TensorInfo,
    SafeTensorsFile,
    parse_dtype_string,
    dtype_to_safetensors,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_tensor_info_creation() raises:
    """Test TensorInfo struct creation."""
    var info = TensorInfo()
    info.dtype = String("F32")
    info.shape = List[Int]()
    info.shape.append(2)
    info.shape.append(3)
    info.data_offset_start = 0
    info.data_offset_end = 24  # 2*3*4 bytes

    assert_true(info.size_bytes() == 24, "Size should be 24 bytes")
    assert_true(info.numel() == 6, "Should have 6 elements")

    print("  tensor_info_creation: PASS")


fn test_tensor_info_shape() raises:
    """Test TensorInfo shape calculations."""
    var info = TensorInfo()
    info.shape = List[Int]()
    info.shape.append(10)
    info.shape.append(20)
    info.shape.append(30)

    assert_true(info.numel() == 6000, "10x20x30 = 6000 elements")

    print("  tensor_info_shape: PASS")


fn test_safetensors_file_creation() raises:
    """Test SafeTensorsFile struct creation."""
    var st = SafeTensorsFile()

    assert_true(st.header_size == 0, "Initial header size should be 0")
    assert_true(st.data_offset == 0, "Initial data offset should be 0")

    print("  safetensors_file_creation: PASS")


fn test_safetensors_register_tensor() raises:
    """Test manual tensor registration."""
    var st = SafeTensorsFile()

    var shape = List[Int]()
    shape.append(768)
    shape.append(3072)

    st.register_tensor("mlp.weight", "F32", shape, 0, 9437184)

    assert_true(st.has_tensor("mlp.weight"), "Should have registered tensor")
    assert_true(not st.has_tensor("other.weight"), "Should not have unregistered tensor")

    print("  safetensors_register_tensor: PASS")


fn test_safetensors_get_tensor_info() raises:
    """Test getting tensor metadata."""
    var st = SafeTensorsFile()

    var shape = List[Int]()
    shape.append(1024)
    shape.append(1024)

    st.register_tensor("embed.weight", "F16", shape, 0, 2097152)

    var info = st.get_tensor_info("embed.weight")

    assert_true(info.dtype == "F16", "Dtype should be F16")
    assert_true(len(info.shape) == 2, "Should have 2 dimensions")
    assert_true(info.shape[0] == 1024, "First dim should be 1024")
    assert_true(info.shape[1] == 1024, "Second dim should be 1024")

    print("  safetensors_get_tensor_info: PASS")


fn test_safetensors_get_tensor_size() raises:
    """Test getting tensor size."""
    var st = SafeTensorsFile()

    var shape = List[Int]()
    shape.append(512)

    st.register_tensor("bias", "F32", shape, 0, 2048)

    var size = st.get_tensor_size("bias")
    assert_true(size == 2048, "Size should be 2048 bytes")

    print("  safetensors_get_tensor_size: PASS")


fn test_safetensors_data_offset() raises:
    """Test calculating absolute data offset."""
    var st = SafeTensorsFile()
    st.data_offset = 1000  # Simulate header + metadata size

    var shape = List[Int]()
    shape.append(256)

    st.register_tensor("layer.weight", "F32", shape, 500, 1524)

    var abs_offset = st.get_data_offset("layer.weight")
    assert_true(abs_offset == 1500, "Absolute offset should be 1000 + 500")

    print("  safetensors_data_offset: PASS")


fn test_parse_dtype_string() raises:
    """Test parsing dtype strings."""
    var dt_f32 = parse_dtype_string("F32")
    var dt_f16 = parse_dtype_string("F16")
    var dt_i32 = parse_dtype_string("I32")

    assert_true(dt_f32 == DType.float32, "F32 should map to float32")
    assert_true(dt_f16 == DType.float16, "F16 should map to float16")
    assert_true(dt_i32 == DType.int32, "I32 should map to int32")

    print("  parse_dtype_string: PASS")


fn test_dtype_to_safetensors() raises:
    """Test converting DType to SafeTensors string."""
    var str_f32 = dtype_to_safetensors(DType.float32)
    var str_f16 = dtype_to_safetensors(DType.float16)
    var str_i32 = dtype_to_safetensors(DType.int32)

    assert_true(str_f32 == "F32", "float32 should map to F32")
    assert_true(str_f16 == "F16", "float16 should map to F16")
    assert_true(str_i32 == "I32", "int32 should map to I32")

    print("  dtype_to_safetensors: PASS")


fn test_multiple_tensors() raises:
    """Test registering multiple tensors."""
    var st = SafeTensorsFile()

    var shape1 = List[Int]()
    shape1.append(100)
    shape1.append(200)

    var shape2 = List[Int]()
    shape2.append(300)
    shape2.append(400)

    st.register_tensor("tensor1", "F32", shape1, 0, 80000)
    st.register_tensor("tensor2", "F16", shape2, 80000, 320000)

    assert_true(st.has_tensor("tensor1"), "Should have tensor1")
    assert_true(st.has_tensor("tensor2"), "Should have tensor2")

    var info1 = st.get_tensor_info("tensor1")
    var info2 = st.get_tensor_info("tensor2")

    assert_true(info1.dtype == "F32", "tensor1 should be F32")
    assert_true(info2.dtype == "F16", "tensor2 should be F16")
    assert_true(info1.numel() == 20000, "tensor1 should have 20000 elements")
    assert_true(info2.numel() == 120000, "tensor2 should have 120000 elements")

    print("  multiple_tensors: PASS")


fn main() raises:
    print("test_safetensors:")

    test_tensor_info_creation()
    test_tensor_info_shape()
    test_safetensors_file_creation()
    test_safetensors_register_tensor()
    test_safetensors_get_tensor_info()
    test_safetensors_get_tensor_size()
    test_safetensors_data_offset()
    test_parse_dtype_string()
    test_dtype_to_safetensors()
    test_multiple_tensors()

    print("ALL PASSED")
