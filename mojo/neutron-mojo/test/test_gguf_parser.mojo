# ===----------------------------------------------------------------------=== #
# Neutron Mojo — GGUF Binary Parser Tests
# ===----------------------------------------------------------------------=== #

"""Tests for GGUF binary file parsing (parse_gguf_from_buffer, gguf_to_model_config)."""

from neutron_mojo.io.binary_reader import BinaryReader
from neutron_mojo.io.gguf import (
    GGUF_MAGIC,
    GGUF_DEFAULT_ALIGNMENT,
    GGUFFile,
    GGUFTensorInfo,
    GGUFTensorType,
    GGUF_F32,
    GGUF_F16,
    parse_gguf_from_buffer,
    gguf_to_model_config,
    build_test_gguf,
    _align_offset,
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
)
from neutron_mojo.model.config import ModelConfig


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected " + String(b) + " got " + String(a)
        )


fn test_alignment() raises:
    """Test _align_offset produces correct aligned offsets."""
    assert_eq(_align_offset(0, 32), 0, "0 aligned to 32")
    assert_eq(_align_offset(1, 32), 32, "1 aligned to 32")
    assert_eq(_align_offset(32, 32), 32, "32 aligned to 32")
    assert_eq(_align_offset(33, 32), 64, "33 aligned to 32")
    assert_eq(_align_offset(100, 32), 128, "100 aligned to 32")
    print("  alignment: PASS")


fn test_parse_minimal_gguf() raises:
    """Test parsing a minimal GGUF with no metadata and no tensors."""
    var buf = List[UInt8]()
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 0)
    _write_u64_le(buf, 0)

    var gguf = parse_gguf_from_buffer(buf^)
    assert_true(gguf.is_valid(), "should be valid GGUF")
    assert_eq(gguf.version, 3, "version should be 3")
    assert_eq(gguf.tensor_count, 0, "0 tensors")
    assert_eq(gguf.metadata_count, 0, "0 metadata")
    print("  parse_minimal_gguf: PASS")


fn test_invalid_magic() raises:
    """Test that invalid magic raises Error."""
    var buf = List[UInt8]()
    _write_u32_le(buf, 0x12345678)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 0)
    _write_u64_le(buf, 0)

    var caught = False
    try:
        var gguf = parse_gguf_from_buffer(buf^)
        _ = gguf^
    except:
        caught = True
    assert_true(caught, "should raise on invalid magic")
    print("  invalid_magic: PASS")


fn test_version_2_accepted() raises:
    """Test that version 2 is accepted."""
    var buf = List[UInt8]()
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 0)
    _write_u64_le(buf, 0)

    var gguf = parse_gguf_from_buffer(buf^)
    assert_eq(gguf.version, 2, "version should be 2")
    print("  version_2_accepted: PASS")


fn test_metadata_string() raises:
    """Test parsing string metadata."""
    var buf = List[UInt8]()
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 0)  # 0 tensors
    _write_u64_le(buf, 1)  # 1 metadata

    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)  # STRING type
    _write_string_gguf(buf, "llama")

    var gguf = parse_gguf_from_buffer(buf^)
    var arch = gguf.get_str("general.architecture", "unknown")
    assert_true(arch == "llama", "architecture should be 'llama'")
    print("  metadata_string: PASS")


fn test_metadata_int_and_float() raises:
    """Test parsing int and float metadata."""
    var buf = List[UInt8]()
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 0)
    _write_u64_le(buf, 2)

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)  # UINT32
    _write_u32_le(buf, 32)

    _write_string_gguf(buf, "llama.rope.freq_base")
    _write_u32_le(buf, 6)  # FLOAT32
    _write_f32_le(buf, Float32(500000.0))

    var gguf = parse_gguf_from_buffer(buf^)
    assert_eq(gguf.get_int("llama.block_count", 0), 32, "block_count 32")
    var theta = gguf.get_float("llama.rope.freq_base", 0.0)
    var diff = theta - 500000.0
    if diff < 0:
        diff = -diff
    assert_true(diff < 1.0, "rope.freq_base ~500000.0")
    print("  metadata_int_and_float: PASS")


fn test_tensor_info_parsing() raises:
    """Test parsing tensor info entries."""
    var buf = List[UInt8]()
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 2)  # 2 tensors
    _write_u64_le(buf, 0)

    # Tensor 1: embed.weight [8,4] F32 offset=0
    _write_string_gguf(buf, "embed.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)  # F32
    _write_u64_le(buf, 0)

    # Tensor 2: layer.weight [4,4] F16 offset=128
    _write_string_gguf(buf, "layer.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 4)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 1)  # F16
    _write_u64_le(buf, 128)

    var gguf = parse_gguf_from_buffer(buf^)
    assert_eq(gguf.tensor_count, 2, "2 tensors")
    assert_true(gguf.has_tensor("embed.weight"), "has embed.weight")
    assert_true(gguf.has_tensor("layer.weight"), "has layer.weight")

    var info1 = gguf.get_tensor_info("embed.weight")
    assert_eq(info1.n_dims, 2, "embed dims")
    assert_eq(info1.shape[0], 8, "embed shape[0]")
    assert_eq(info1.shape[1], 4, "embed shape[1]")
    assert_true(info1.tensor_type == GGUF_F32(), "embed type F32")

    var info2 = gguf.get_tensor_info("layer.weight")
    assert_true(info2.tensor_type == GGUF_F16(), "layer type F16")
    assert_eq(info2.offset, 128, "layer offset")

    print("  tensor_info_parsing: PASS")


fn test_build_and_parse_gguf() raises:
    """Test building GGUF with build_test_gguf and parsing it back."""
    var sk = List[String]()
    sk.append("general.architecture")
    var sv = List[String]()
    sv.append("llama")

    var ik = List[String]()
    ik.append("llama.block_count")
    ik.append("llama.embedding_length")
    var iv = List[Int]()
    iv.append(2)
    iv.append(4)

    var fk = List[String]()
    fk.append("llama.rope.freq_base")
    var fv = List[Float64]()
    fv.append(10000.0)

    var names = List[String]()
    names.append("model.embed_tokens.weight")
    names.append("model.norm.weight")

    var shapes = List[List[Int]]()
    var s1 = List[Int]()
    s1.append(8)
    s1.append(4)
    shapes.append(s1^)
    var s2 = List[Int]()
    s2.append(4)
    shapes.append(s2^)

    var types = List[Int]()
    types.append(0)
    types.append(0)

    var sizes = List[Int]()
    sizes.append(128)
    sizes.append(16)

    var buf = build_test_gguf(sk, sv, ik, iv, fk, fv, names, shapes, types, sizes)
    var gguf = parse_gguf_from_buffer(buf^)

    assert_true(gguf.is_valid(), "should be valid")
    assert_eq(gguf.tensor_count, 2, "2 tensors")
    assert_true(gguf.has_tensor("model.embed_tokens.weight"), "has embed")
    assert_true(gguf.has_tensor("model.norm.weight"), "has norm")

    var arch = gguf.get_str("general.architecture", "unknown")
    assert_true(arch == "llama", "arch llama")
    assert_eq(gguf.get_int("llama.block_count", 0), 2, "block_count 2")

    print("  build_and_parse_gguf: PASS")


fn test_gguf_to_model_config() raises:
    """Test extracting ModelConfig from parsed GGUF metadata."""
    var sk = List[String]()
    sk.append("general.architecture")
    var sv = List[String]()
    sv.append("llama")

    var ik = List[String]()
    ik.append("llama.block_count")
    ik.append("llama.embedding_length")
    ik.append("llama.attention.head_count")
    ik.append("llama.attention.head_count_kv")
    ik.append("llama.feed_forward_length")
    ik.append("llama.context_length")
    ik.append("tokenizer.ggml.bos_token_id")
    ik.append("tokenizer.ggml.eos_token_id")
    var iv = List[Int]()
    iv.append(4)
    iv.append(16)
    iv.append(4)
    iv.append(2)
    iv.append(32)
    iv.append(512)
    iv.append(1)
    iv.append(2)

    var fk = List[String]()
    fk.append("llama.rope.freq_base")
    var fv = List[Float64]()
    fv.append(10000.0)

    var names = List[String]()
    var shapes = List[List[Int]]()
    var types = List[Int]()
    var sizes = List[Int]()

    var buf = build_test_gguf(sk, sv, ik, iv, fk, fv, names, shapes, types, sizes)
    var gguf = parse_gguf_from_buffer(buf^)
    var cfg = gguf_to_model_config(gguf)

    assert_true(cfg.model_type == "llama", "model_type llama")
    assert_eq(cfg.num_hidden_layers, 4, "num_hidden_layers 4")
    assert_eq(cfg.hidden_size, 16, "hidden_size 16")
    assert_eq(cfg.num_attention_heads, 4, "num_attention_heads 4")
    assert_eq(cfg.num_key_value_heads, 2, "num_key_value_heads 2")
    assert_eq(cfg.intermediate_size, 32, "intermediate_size 32")
    assert_eq(cfg.max_position_embeddings, 512, "max_pos_embeddings 512")
    assert_eq(cfg.head_dim, 4, "head_dim 4")
    assert_eq(cfg.bos_token_id, 1, "bos_token_id 1")
    assert_eq(cfg.eos_token_id, 2, "eos_token_id 2")

    print("  gguf_to_model_config: PASS")


fn test_data_offset_alignment() raises:
    """Test that data_offset is correctly aligned."""
    var sk = List[String]()
    var sv = List[String]()
    var ik = List[String]()
    var iv = List[Int]()
    var fk = List[String]()
    var fv = List[Float64]()

    var names = List[String]()
    names.append("w")
    var shapes = List[List[Int]]()
    var s1 = List[Int]()
    s1.append(4)
    shapes.append(s1^)
    var types = List[Int]()
    types.append(0)
    var sizes = List[Int]()
    sizes.append(16)

    var buf = build_test_gguf(sk, sv, ik, iv, fk, fv, names, shapes, types, sizes)
    var gguf = parse_gguf_from_buffer(buf^)

    assert_eq(gguf.data_offset % 32, 0, "data_offset should be 32-aligned")
    print("  data_offset_alignment: PASS")


fn main() raises:
    print("test_gguf_parser:")

    test_alignment()
    test_parse_minimal_gguf()
    test_invalid_magic()
    test_version_2_accepted()
    test_metadata_string()
    test_metadata_int_and_float()
    test_tensor_info_parsing()
    test_build_and_parse_gguf()
    test_gguf_to_model_config()
    test_data_offset_alignment()

    print("ALL PASSED (10 tests)")
