# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Weight Reader Tests
# ===----------------------------------------------------------------------=== #

"""Tests for loading tensor data from GGUF files into Model."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.io.binary_reader import BinaryReader
from neutron_mojo.io.gguf import (
    GGUF_MAGIC,
    GGUF_DEFAULT_ALIGNMENT,
    GGUFFile,
    GGUF_F32,
    GGUF_F16,
    parse_gguf_from_buffer,
    gguf_to_model_config,
    _align_offset,
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
)
from neutron_mojo.model.weight_reader import (
    read_tensor_f32,
    read_tensor_f16_as_f32,
    load_gguf_model_from_buffer,
)
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected " + String(b) + " got " + String(a)
        )


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg
            + " got " + String(a) + " vs " + String(b)
        )


fn test_read_tensor_f32() raises:
    """Test reading F32 tensor from raw bytes."""
    var buf = List[UInt8]()
    # Write 3 floats: 1.0, 2.0, 3.0
    _write_f32_le(buf, Float32(1.0))
    _write_f32_le(buf, Float32(2.0))
    _write_f32_le(buf, Float32(3.0))

    var reader = BinaryReader(buf^)
    var t = read_tensor_f32(reader, 0, 3)
    assert_near(t.get(0), 1.0, 0.001, "first float")
    assert_near(t.get(1), 2.0, 0.001, "second float")
    assert_near(t.get(2), 3.0, 0.001, "third float")

    print("  read_tensor_f32: PASS")


fn test_read_tensor_f16() raises:
    """Test reading FP16 tensor and converting to F32."""
    var buf = List[UInt8]()
    # FP16 1.0 = 0x3C00
    buf.append(0x00)
    buf.append(0x3C)
    # FP16 -2.0 = 0xC000
    buf.append(0x00)
    buf.append(0xC0)

    var reader = BinaryReader(buf^)
    var t = read_tensor_f16_as_f32(reader, 0, 2)
    assert_near(t.get(0), 1.0, 0.001, "FP16 1.0")
    assert_near(t.get(1), -2.0, 0.001, "FP16 -2.0")

    print("  read_tensor_f16: PASS")


# ===----------------------------------------------------------------------=== #
# Helper: build a tiny GGUF with actual float data
# ===----------------------------------------------------------------------=== #

fn _build_tiny_gguf_with_data() raises -> List[UInt8]:
    """Build a GGUF with tiny model weights (vocab=8, hidden=4, 2 layers).

    Weight layout matches tiny_test_params():
        vocab_size=8, hidden_dim=4, num_q_heads=2, num_kv_heads=1,
        head_dim=2, ffn_dim=8, num_layers=2
    """
    var buf = List[UInt8]()

    # --- Header ---
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)  # version

    # We'll register 3 tensors: embed, final_norm, lm_head
    _write_u64_le(buf, 3)  # tensor_count

    # Metadata: config for tiny model
    var meta_count = 7
    _write_u64_le(buf, meta_count)

    # general.architecture = "llama"
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)  # STRING
    _write_string_gguf(buf, "llama")

    # llama.block_count = 2
    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)  # UINT32
    _write_u32_le(buf, 2)

    # llama.embedding_length = 4
    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 4)

    # llama.attention.head_count = 2
    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    # llama.attention.head_count_kv = 1
    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    # llama.feed_forward_length = 8
    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    # llama.vocab_size = 8
    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    # --- Tensor info ---
    # embed: [8, 4] F32 = 32 floats = 128 bytes
    _write_string_gguf(buf, "model.embed_tokens.weight")
    _write_u32_le(buf, 2)   # n_dims
    _write_u64_le(buf, 8)   # vocab_size
    _write_u64_le(buf, 4)   # hidden_dim
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 0)   # offset

    # final_norm: [4] F32 = 4 floats = 16 bytes
    _write_string_gguf(buf, "model.norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 128) # offset after embed

    # lm_head: [8, 4] F32 = 32 floats = 128 bytes
    _write_string_gguf(buf, "lm_head.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 144) # offset after norm

    # --- Align to 32 bytes ---
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # --- Tensor data ---
    # embed: 32 floats, set to small values
    for i in range(32):
        _write_f32_le(buf, Float32(0.01) * Float32(i + 1))

    # final_norm: 4 floats, all 1.0
    for _ in range(4):
        _write_f32_le(buf, Float32(1.0))

    # lm_head: 32 floats
    for i in range(32):
        _write_f32_le(buf, Float32(0.02) * Float32(i + 1))

    return buf^


fn test_load_gguf_model_from_buffer() raises:
    """Test loading a model from a synthetic GGUF buffer."""
    var buf = _build_tiny_gguf_with_data()

    # First parse to verify structure
    var buf_copy = buf.copy()
    var gguf = parse_gguf_from_buffer(buf_copy^)
    assert_true(gguf.is_valid(), "GGUF should be valid")
    assert_eq(gguf.tensor_count, 3, "3 tensors")

    # Now load the model
    var model = load_gguf_model_from_buffer(buf^)
    assert_eq(model.params.num_layers, 2, "2 layers")
    assert_eq(model.params.hidden_dim, 4, "hidden_dim 4")
    assert_eq(model.params.vocab_size, 8, "vocab_size 8")

    # Verify embed weights were loaded (first element should be 0.01)
    assert_near(model.embed.get(0), 0.01, 0.001, "embed[0]")

    # Verify final_norm weights (should be 1.0)
    assert_near(model.final_norm.get(0), 1.0, 0.001, "final_norm[0]")

    print("  load_gguf_model_from_buffer: PASS")


fn test_loaded_model_generates() raises:
    """Test that a loaded model can generate tokens without crashing."""
    var buf = _build_tiny_gguf_with_data()
    var model = load_gguf_model_from_buffer(buf^)

    var prompt = List[Int]()
    prompt.append(1)

    var output = generate(model, prompt, max_new_tokens=3)
    assert_true(len(output) == 3, "should generate 3 tokens")

    # Tokens should be in valid range
    for i in range(len(output)):
        assert_true(output[i] >= 0 and output[i] < 8, "token in range")

    print("  loaded_model_generates: PASS")


fn test_embed_weight_values() raises:
    """Test that specific weight values match what was written."""
    var buf = _build_tiny_gguf_with_data()
    var model = load_gguf_model_from_buffer(buf^)

    # Embed weights: 0.01*(i+1) for i in 0..31
    # Use data_ptr for 2D tensor flat access
    var ptr = model.embed.data_ptr()
    assert_near(ptr[0], 0.01, 0.001, "embed flat[0]")
    assert_near(ptr[1], 0.02, 0.001, "embed flat[1]")
    assert_near(ptr[31], 0.32, 0.001, "embed flat[31]")
    _ = model.embed.numel()  # keepalive

    print("  embed_weight_values: PASS")


fn main() raises:
    print("test_weight_reader:")

    test_read_tensor_f32()
    test_read_tensor_f16()
    test_load_gguf_model_from_buffer()
    test_loaded_model_generates()
    test_embed_weight_values()

    print("ALL PASSED (5 tests)")
