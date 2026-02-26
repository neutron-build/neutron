# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Weight Loader Tests
# ===----------------------------------------------------------------------=== #

"""Tests for unified weight loading interface."""

from neutron_mojo.model.loader import (
    FileFormat,
    FMT_SAFETENSORS,
    FMT_GGUF,
    FMT_UNKNOWN,
    detect_format,
    WeightDescriptor,
    WeightIndex,
    register_safetensors_weight,
    register_gguf_weight,
    validate_weights_for_model,
)
from neutron_mojo.model.config import ModelConfig, llama3_8b_config
from neutron_mojo.io.safetensors import TensorInfo
from neutron_mojo.io.gguf import GGUFTensorInfo, GGUF_F16, GGUF_Q4_0, GGUF_Q8_0
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_detect_format() raises:
    """Test file format detection from extension."""
    var st = detect_format("model.safetensors")
    var gguf = detect_format("model-q4.gguf")
    var unknown = detect_format("model.bin")

    assert_true(st == FMT_SAFETENSORS(), "Should detect safetensors")
    assert_true(gguf == FMT_GGUF(), "Should detect gguf")
    assert_true(unknown == FMT_UNKNOWN(), "Should detect unknown")

    print("  detect_format: PASS")


fn test_weight_descriptor_creation() raises:
    """Test WeightDescriptor struct."""
    var desc = WeightDescriptor()
    desc.name = String("layer.0.weight")
    desc.dtype = DType.float16
    desc.shape = List[Int]()
    desc.shape.append(4096)
    desc.shape.append(4096)
    desc.size_bytes = 33554432  # 4096*4096*2
    desc.file_offset = 1000

    assert_true(desc.numel() == 16777216, "Should have 4096*4096 elements")
    assert_true(desc.ndim() == 2, "Should have 2 dims")
    assert_true(not desc.is_quantized, "Default should not be quantized")

    print("  weight_descriptor_creation: PASS")


fn test_weight_index_creation() raises:
    """Test WeightIndex struct."""
    var index = WeightIndex()

    assert_true(index.num_weights() == 0, "Should start empty")
    assert_true(index.total_size_bytes == 0, "Should start at 0 bytes")

    print("  weight_index_creation: PASS")


fn test_weight_index_add() raises:
    """Test adding weights to index."""
    var index = WeightIndex()

    var desc1 = WeightDescriptor()
    desc1.name = String("embed.weight")
    desc1.size_bytes = 1000

    var desc2 = WeightDescriptor()
    desc2.name = String("layer.weight")
    desc2.size_bytes = 2000

    index.add_weight(desc1)
    index.add_weight(desc2)

    assert_true(index.num_weights() == 2, "Should have 2 weights")
    assert_true(index.total_size_bytes == 3000, "Total should be 3000")
    assert_true(index.has_weight("embed.weight"), "Should have embed.weight")
    assert_true(index.has_weight("layer.weight"), "Should have layer.weight")
    assert_true(not index.has_weight("other"), "Should not have other")

    print("  weight_index_add: PASS")


fn test_weight_index_get() raises:
    """Test getting weight from index."""
    var index = WeightIndex()

    var desc = WeightDescriptor()
    desc.name = String("test.weight")
    desc.dtype = DType.float32
    desc.size_bytes = 4096
    desc.file_offset = 500
    index.add_weight(desc)

    var retrieved = index.get_weight("test.weight")

    assert_true(retrieved.name == "test.weight", "Name should match")
    assert_true(retrieved.size_bytes == 4096, "Size should match")
    assert_true(retrieved.file_offset == 500, "Offset should match")

    print("  weight_index_get: PASS")


fn test_total_size_mb() raises:
    """Test total size in MB."""
    var index = WeightIndex()

    var desc = WeightDescriptor()
    desc.name = String("big.weight")
    desc.size_bytes = 1048576  # 1 MB
    index.add_weight(desc)

    var mb = index.total_size_mb()
    assert_true(abs(mb - 1.0) < 0.01, "Should be ~1.0 MB")

    print("  total_size_mb: PASS")


fn test_register_safetensors_weight() raises:
    """Test registering a SafeTensors weight."""
    var index = WeightIndex()
    index.format = FMT_SAFETENSORS()

    var info = TensorInfo()
    info.dtype = String("F16")
    info.shape = List[Int]()
    info.shape.append(4096)
    info.shape.append(4096)
    info.data_offset_start = 0
    info.data_offset_end = 33554432

    register_safetensors_weight(index, "model.embed_tokens.weight", info, 1000)

    assert_true(index.has_weight("model.embed_tokens.weight"), "Should register weight")

    var w = index.get_weight("model.embed_tokens.weight")
    assert_true(w.dtype == DType.float16, "Should be F16")
    assert_true(w.file_offset == 1000, "Offset should be base + start")

    print("  register_safetensors_weight: PASS")


fn test_register_gguf_weight() raises:
    """Test registering a GGUF weight."""
    var index = WeightIndex()
    index.format = FMT_GGUF()

    var info = GGUFTensorInfo()
    info.name = String("blk.0.attn_q.weight")
    info.n_dims = 2
    info.shape = List[Int]()
    info.shape.append(4096)
    info.shape.append(4096)
    info.tensor_type = GGUF_Q8_0()
    info.offset = 500

    register_gguf_weight(index, "blk.0.attn_q.weight", info, 2000)

    assert_true(index.has_weight("blk.0.attn_q.weight"), "Should register weight")

    var w = index.get_weight("blk.0.attn_q.weight")
    assert_true(w.is_quantized, "Q8_0 should be quantized")
    assert_true(w.quant_type == "q8_0", "Should be q8_0")
    assert_true(w.file_offset == 2500, "Offset should be 2000 + 500")

    print("  register_gguf_weight: PASS")


fn test_register_gguf_weight_q4() raises:
    """Test registering a Q4_0 GGUF weight."""
    var index = WeightIndex()

    var info = GGUFTensorInfo()
    info.name = String("blk.0.ffn.weight")
    info.n_dims = 2
    info.shape = List[Int]()
    info.shape.append(1024)
    info.tensor_type = GGUF_Q4_0()
    info.offset = 0

    register_gguf_weight(index, "blk.0.ffn.weight", info, 0)

    var w = index.get_weight("blk.0.ffn.weight")
    assert_true(w.is_quantized, "Q4_0 should be quantized")
    assert_true(w.quant_type == "q4_0", "Should be q4_0")

    print("  register_gguf_weight_q4: PASS")


fn test_register_gguf_weight_f16() raises:
    """Test registering an F16 GGUF weight (not quantized)."""
    var index = WeightIndex()

    var info = GGUFTensorInfo()
    info.name = String("output.weight")
    info.n_dims = 2
    info.shape = List[Int]()
    info.shape.append(128)
    info.shape.append(256)
    info.tensor_type = GGUF_F16()
    info.offset = 0

    register_gguf_weight(index, "output.weight", info, 0)

    var w = index.get_weight("output.weight")
    assert_true(not w.is_quantized, "F16 should not be quantized")
    assert_true(w.quant_type == "none", "Should be none")

    print("  register_gguf_weight_f16: PASS")


fn test_validate_weights_pass() raises:
    """Test weight validation with all required weights."""
    var index = WeightIndex()
    var config = llama3_8b_config()

    # Add required weights
    var embed = WeightDescriptor()
    embed.name = String("model.embed_tokens.weight")
    embed.size_bytes = 100
    index.add_weight(embed)

    var norm = WeightDescriptor()
    norm.name = String("model.norm.weight")
    norm.size_bytes = 100
    index.add_weight(norm)

    var q_proj = WeightDescriptor()
    q_proj.name = String("model.layers.0.self_attn.q_proj.weight")
    q_proj.size_bytes = 100
    index.add_weight(q_proj)

    var valid = validate_weights_for_model(index, config)
    assert_true(valid, "Should validate with all required weights")

    print("  validate_weights_pass: PASS")


fn test_validate_weights_fail() raises:
    """Test weight validation with missing weights."""
    var index = WeightIndex()
    var config = llama3_8b_config()

    # Missing all required weights
    var ok = False
    try:
        _ = validate_weights_for_model(index, config)
    except:
        ok = True

    assert_true(ok, "Should fail validation without required weights")

    print("  validate_weights_fail: PASS")


fn main() raises:
    print("test_loader:")

    test_detect_format()
    test_weight_descriptor_creation()
    test_weight_index_creation()
    test_weight_index_add()
    test_weight_index_get()
    test_total_size_mb()
    test_register_safetensors_weight()
    test_register_gguf_weight()
    test_register_gguf_weight_q4()
    test_register_gguf_weight_f16()
    test_validate_weights_pass()
    test_validate_weights_fail()

    print("ALL PASSED")
