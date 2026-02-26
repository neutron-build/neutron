# ===----------------------------------------------------------------------=== #
# Neutron Mojo — GGUF Name Mapping Tests
# ===----------------------------------------------------------------------=== #

"""Tests for GGUF → HuggingFace tensor name normalization and loading."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.model.populate import normalize_weight_name
from neutron_mojo.model.weight_reader import load_gguf_model_from_buffer
from neutron_mojo.io.gguf import (
    GGUF_MAGIC,
    GGUF_DEFAULT_ALIGNMENT,
    GGUF_F32,
    _align_offset,
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq_str(a: String, b: String, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected '" + b + "' got '" + a + "'"
        )


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg
            + " got " + String(a) + " vs " + String(b)
        )


# ===----------------------------------------------------------------------=== #
# Tests: normalize_weight_name
# ===----------------------------------------------------------------------=== #

fn test_normalize_global_names() raises:
    """Test global tensor name mappings."""
    assert_eq_str(
        normalize_weight_name("token_embd.weight"),
        "model.embed_tokens.weight",
        "token_embd",
    )
    assert_eq_str(
        normalize_weight_name("output_norm.weight"),
        "model.norm.weight",
        "output_norm",
    )
    assert_eq_str(
        normalize_weight_name("output.weight"),
        "lm_head.weight",
        "output",
    )

    print("  normalize_global_names: PASS")


fn test_normalize_layer_names() raises:
    """Test all 9 per-layer suffix mappings."""
    var layer = "0"
    var gp = "blk." + layer + "."
    var hp = "model.layers." + layer + "."

    assert_eq_str(normalize_weight_name(gp + "attn_norm.weight"), hp + "input_layernorm.weight", "attn_norm")
    assert_eq_str(normalize_weight_name(gp + "attn_q.weight"), hp + "self_attn.q_proj.weight", "attn_q")
    assert_eq_str(normalize_weight_name(gp + "attn_k.weight"), hp + "self_attn.k_proj.weight", "attn_k")
    assert_eq_str(normalize_weight_name(gp + "attn_v.weight"), hp + "self_attn.v_proj.weight", "attn_v")
    assert_eq_str(normalize_weight_name(gp + "attn_output.weight"), hp + "self_attn.o_proj.weight", "attn_output")
    assert_eq_str(normalize_weight_name(gp + "ffn_norm.weight"), hp + "post_attention_layernorm.weight", "ffn_norm")
    assert_eq_str(normalize_weight_name(gp + "ffn_gate.weight"), hp + "mlp.gate_proj.weight", "ffn_gate")
    assert_eq_str(normalize_weight_name(gp + "ffn_up.weight"), hp + "mlp.up_proj.weight", "ffn_up")
    assert_eq_str(normalize_weight_name(gp + "ffn_down.weight"), hp + "mlp.down_proj.weight", "ffn_down")

    # Also test with layer > 9 (multi-digit)
    assert_eq_str(
        normalize_weight_name("blk.12.attn_q.weight"),
        "model.layers.12.self_attn.q_proj.weight",
        "layer 12",
    )

    print("  normalize_layer_names: PASS")


fn test_normalize_passthrough() raises:
    """Test that HF names pass through unchanged."""
    assert_eq_str(
        normalize_weight_name("model.embed_tokens.weight"),
        "model.embed_tokens.weight",
        "embed passthrough",
    )
    assert_eq_str(
        normalize_weight_name("model.layers.0.self_attn.q_proj.weight"),
        "model.layers.0.self_attn.q_proj.weight",
        "layer passthrough",
    )
    assert_eq_str(
        normalize_weight_name("lm_head.weight"),
        "lm_head.weight",
        "lm_head passthrough",
    )

    print("  normalize_passthrough: PASS")


# ===----------------------------------------------------------------------=== #
# Tests: Loading GGUF with GGUF-convention names
# ===----------------------------------------------------------------------=== #

fn _build_gguf_with_gguf_names() raises -> List[UInt8]:
    """Build a GGUF with GGUF-convention tensor names."""
    var buf = List[UInt8]()

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 3)  # tensor_count (embed, norm, lm_head)
    _write_u64_le(buf, 7)  # metadata_count

    # Metadata
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 4)

    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    # Tensor info — using GGUF names!
    # token_embd [8,4] F32 = 128 bytes
    _write_string_gguf(buf, "token_embd.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 0)

    # output_norm [4] F32 = 16 bytes
    _write_string_gguf(buf, "output_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, 128)

    # output [8,4] F32 = 128 bytes
    _write_string_gguf(buf, "output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, 144)

    # Align
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # Data: embed (128 bytes)
    for i in range(32):
        _write_f32_le(buf, Float32(i + 1) * 0.01)

    # norm (16 bytes)
    for _ in range(4):
        _write_f32_le(buf, Float32(1.0))

    # lm_head (128 bytes)
    for i in range(32):
        _write_f32_le(buf, Float32(i) * 0.02)

    return buf^


fn test_load_gguf_named_weights() raises:
    """Test loading GGUF with GGUF-convention tensor names."""
    var buf = _build_gguf_with_gguf_names()
    var model = load_gguf_model_from_buffer(buf^)

    # Verify model populated correctly
    assert_true(model.params.vocab_size == 8, "vocab_size=8")
    assert_true(model.params.hidden_dim == 4, "hidden_dim=4")

    # Verify embed was loaded (first value = 0.01)
    var ptr = model.embed.data_ptr()
    assert_near(ptr[0], 0.01, 0.001, "gguf embed[0]")
    assert_near(ptr[1], 0.02, 0.001, "gguf embed[1]")
    _ = model.embed.numel()

    # Verify norm was loaded
    assert_near(model.final_norm.get(0), 1.0, 0.001, "gguf norm[0]")

    # Verify lm_head was loaded
    var lm_ptr = model.lm_head.data_ptr()
    assert_near(lm_ptr[0], 0.0, 0.001, "gguf lm_head[0]")
    assert_near(lm_ptr[1], 0.02, 0.001, "gguf lm_head[1]")
    _ = model.lm_head.numel()

    print("  load_gguf_named_weights: PASS")


fn test_mixed_naming() raises:
    """Test GGUF where some tensors use GGUF names and some use HF names."""
    var buf = List[UInt8]()

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 3)
    _write_u64_le(buf, 7)

    # Metadata
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 4)

    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    # Tensors: mix of GGUF and HF names
    # GGUF name: token_embd.weight
    _write_string_gguf(buf, "token_embd.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, 0)

    # HF name: model.norm.weight
    _write_string_gguf(buf, "model.norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, 128)

    # GGUF name: output.weight
    _write_string_gguf(buf, "output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, 144)

    # Align
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # Data
    for i in range(32):
        _write_f32_le(buf, Float32(i) * 0.1)
    for _ in range(4):
        _write_f32_le(buf, Float32(1.0))
    for i in range(32):
        _write_f32_le(buf, Float32(i) * 0.05)

    var model = load_gguf_model_from_buffer(buf^)

    # Verify all loaded
    var ptr = model.embed.data_ptr()
    assert_near(ptr[0], 0.0, 0.001, "mixed embed[0]")
    assert_near(ptr[1], 0.1, 0.001, "mixed embed[1]")
    _ = model.embed.numel()

    assert_near(model.final_norm.get(0), 1.0, 0.001, "mixed norm[0]")

    var lm_ptr = model.lm_head.data_ptr()
    assert_near(lm_ptr[1], 0.05, 0.001, "mixed lm_head[1]")
    _ = model.lm_head.numel()

    print("  mixed_naming: PASS")


fn main() raises:
    print("test_gguf_names:")

    test_normalize_global_names()
    test_normalize_layer_names()
    test_normalize_passthrough()
    test_load_gguf_named_weights()
    test_mixed_naming()

    print("ALL PASSED (5 tests)")
