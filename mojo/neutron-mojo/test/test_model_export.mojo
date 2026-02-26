# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Export Tests
# ===----------------------------------------------------------------------=== #

"""Tests for NMF model serialization and deserialization."""

from neutron_mojo.io.model_export import (
    NMF_MAGIC, NMF_VERSION,
    NMFBuffer,
    serialize_params, deserialize_params,
    save_model_to_buffer, load_model_from_buffer,
)
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.model.architecture import arch_from_name, ArchitectureKind
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn approx_eq(a: Float32, b: Float32, tol: Float32 = 1e-4) -> Bool:
    return abs(a - b) < tol


fn test_nmf_magic() raises:
    """NMF magic and version constants."""
    assert_true(NMF_MAGIC() == 0x00464D4E, "Magic should be NMF\\0")
    assert_true(NMF_VERSION() == 1, "Version should be 1")
    print("  nmf_magic: PASS")


fn test_nmf_buffer_write_read_u32() raises:
    """NMFBuffer u32 write/read round-trip."""
    var buf = NMFBuffer()
    buf._write_u32(42)
    buf._write_u32(0xDEADBEEF)
    assert_true(buf._read_u32(0) == 42, "First u32")
    assert_true(buf._read_u32(4) == Int(0xDEADBEEF), "Second u32")
    assert_true(buf.size() == 8, "Buffer size should be 8")
    print("  nmf_buffer_write_read_u32: PASS")


fn test_nmf_buffer_write_read_f32() raises:
    """NMFBuffer f32 write/read round-trip."""
    var buf = NMFBuffer()
    buf._write_f32(3.14)
    buf._write_f32(-1.0)
    buf._write_f32(0.0)
    assert_true(approx_eq(buf._read_f32(0), 3.14, 0.01), "First f32")
    assert_true(approx_eq(buf._read_f32(4), -1.0), "Second f32")
    assert_true(approx_eq(buf._read_f32(8), 0.0), "Third f32")
    print("  nmf_buffer_write_read_f32: PASS")


fn test_serialize_params() raises:
    """Serialize ModelParams to text."""
    var p = tiny_test_params()
    var s = serialize_params(p)
    assert_true(len(s) > 0, "Serialized string should not be empty")

    # Check key fields are present
    fn contains(haystack: String, needle: String) -> Bool:
        if len(needle) > len(haystack):
            return False
        for i in range(len(haystack) - len(needle) + 1):
            var found = True
            for j in range(len(needle)):
                if ord(haystack[byte=i + j]) != ord(needle[byte=j]):
                    found = False
                    break
            if found:
                return True
        return False

    assert_true(contains(s, "num_layers=2"), "Has num_layers")
    assert_true(contains(s, "vocab_size=8"), "Has vocab_size")
    assert_true(contains(s, "hidden_dim=4"), "Has hidden_dim")
    assert_true(contains(s, "arch=Llama"), "Has arch")
    print("  serialize_params: PASS")


fn test_deserialize_params() raises:
    """Deserialize ModelParams from text."""
    var text = "num_layers=3\nvocab_size=100\nhidden_dim=16\nnum_q_heads=4\nnum_kv_heads=2\nhead_dim=4\nffn_dim=32\nmax_seq_len=64\narch=Llama\n"
    var p = deserialize_params(text)
    assert_true(p.num_layers == 3, "num_layers")
    assert_true(p.vocab_size == 100, "vocab_size")
    assert_true(p.hidden_dim == 16, "hidden_dim")
    assert_true(p.num_q_heads == 4, "num_q_heads")
    assert_true(p.num_kv_heads == 2, "num_kv_heads")
    assert_true(p.head_dim == 4, "head_dim")
    assert_true(p.ffn_dim == 32, "ffn_dim")
    assert_true(p.max_seq_len == 64, "max_seq_len")
    assert_true(p.arch.kind == ArchitectureKind.Llama, "arch should be Llama")
    print("  deserialize_params: PASS")


fn test_params_roundtrip() raises:
    """Serialize then deserialize ModelParams."""
    var p = tiny_test_params()
    var s = serialize_params(p)
    var p2 = deserialize_params(s)
    assert_true(p.num_layers == p2.num_layers, "num_layers roundtrip")
    assert_true(p.vocab_size == p2.vocab_size, "vocab_size roundtrip")
    assert_true(p.hidden_dim == p2.hidden_dim, "hidden_dim roundtrip")
    assert_true(p.num_q_heads == p2.num_q_heads, "num_q_heads roundtrip")
    assert_true(p.num_kv_heads == p2.num_kv_heads, "num_kv_heads roundtrip")
    assert_true(p.head_dim == p2.head_dim, "head_dim roundtrip")
    assert_true(p.ffn_dim == p2.ffn_dim, "ffn_dim roundtrip")
    assert_true(p.max_seq_len == p2.max_seq_len, "max_seq_len roundtrip")
    print("  params_roundtrip: PASS")


fn test_save_load_model() raises:
    """Save and load FP32 model via NMFBuffer."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set some non-zero weights
    for i in range(model.embed.numel()):
        model.embed.set(i, Float32(i) * 0.1)
    for i in range(model.lm_head.numel()):
        model.lm_head.set(i, Float32(i) * -0.05)
    for i in range(model.final_norm.numel()):
        model.final_norm.set(i, 1.0)
    for i in range(model.layer_weights.numel()):
        model.layer_weights.set(i, Float32(i) * 0.01)

    # Save
    var buf = save_model_to_buffer(model)
    assert_true(buf.size() > 12, "Buffer should have header + data")

    # Load
    var loaded = load_model_from_buffer(buf)

    # Verify params
    assert_true(loaded.params.num_layers == p.num_layers, "Loaded num_layers")
    assert_true(loaded.params.vocab_size == p.vocab_size, "Loaded vocab_size")
    assert_true(loaded.params.hidden_dim == p.hidden_dim, "Loaded hidden_dim")

    # Verify weights
    # embed and lm_head are 2D — use get(row, col) for correct element access
    var vocab = p.vocab_size
    var hidden = p.hidden_dim
    for row in range(vocab):
        for col in range(hidden):
            assert_true(approx_eq(loaded.embed.get(row, col), model.embed.get(row, col)), "Embed mismatch at " + String(row) + "," + String(col))
    for row in range(vocab):
        for col in range(hidden):
            assert_true(approx_eq(loaded.lm_head.get(row, col), model.lm_head.get(row, col)), "LM head mismatch at " + String(row) + "," + String(col))
    for i in range(model.final_norm.numel()):
        assert_true(approx_eq(loaded.final_norm.get(i), model.final_norm.get(i)), "Norm mismatch at " + String(i))
    for i in range(model.layer_weights.numel()):
        assert_true(approx_eq(loaded.layer_weights.get(i), model.layer_weights.get(i)), "Layer weights mismatch at " + String(i))

    print("  save_load_model: PASS")


fn test_header_validation() raises:
    """Invalid magic number raises error."""
    var buf = NMFBuffer()
    buf._write_u32(0x12345678)  # Wrong magic
    buf._write_u32(1)
    buf._write_u32(0)  # Empty params
    var caught = False
    try:
        _ = load_model_from_buffer(buf)
    except:
        caught = True
    assert_true(caught, "Should raise on invalid magic")
    print("  header_validation: PASS")


fn test_version_validation() raises:
    """Invalid version raises error."""
    var buf = NMFBuffer()
    buf._write_u32(NMF_MAGIC())
    buf._write_u32(99)  # Wrong version
    buf._write_u32(0)
    var caught = False
    try:
        _ = load_model_from_buffer(buf)
    except:
        caught = True
    assert_true(caught, "Should raise on invalid version")
    print("  version_validation: PASS")


fn test_model_sizes_match() raises:
    """Loaded model has same tensor sizes as original."""
    var p = tiny_test_params()
    var model = Model(p)
    var buf = save_model_to_buffer(model)
    var loaded = load_model_from_buffer(buf)
    assert_true(loaded.embed.numel() == model.embed.numel(), "Embed size")
    assert_true(loaded.lm_head.numel() == model.lm_head.numel(), "LM head size")
    assert_true(loaded.final_norm.numel() == model.final_norm.numel(), "Norm size")
    assert_true(loaded.layer_weights.numel() == model.layer_weights.numel(), "Layer weights size")
    print("  model_sizes_match: PASS")


fn test_buffer_size() raises:
    """Buffer size is correct for the model."""
    var p = tiny_test_params()
    var model = Model(p)
    var buf = save_model_to_buffer(model)

    # Header: 12 bytes (magic + version + params_len)
    # Params: variable
    # Weight sections: 4 sections, each has a u32 size prefix + N * 4 bytes
    var params_str = serialize_params(model.params)
    var expected_header = 12 + len(params_str)
    var expected_weights = (
        4 + model.layer_weights.numel() * 4 +
        4 + model.embed.numel() * 4 +
        4 + model.final_norm.numel() * 4 +
        4 + model.lm_head.numel() * 4
    )
    assert_true(buf.size() == expected_header + expected_weights, "Total buffer size")
    print("  buffer_size: PASS")


fn test_arch_roundtrip() raises:
    """Architecture info survives serialization."""
    var p = tiny_test_params()
    p.arch = arch_from_name("Mistral")
    var s = serialize_params(p)
    var p2 = deserialize_params(s)
    assert_true(p2.arch.kind == ArchitectureKind.Mistral, "Arch should be Mistral after roundtrip")
    print("  arch_roundtrip: PASS")


fn test_default_model_roundtrip() raises:
    """Save/load a default-initialized model (norms=1, weights=0, embed=0)."""
    var p = tiny_test_params()
    var model = Model(p)
    # Model.__init__ sets norms to 1.0, other weights to 0.0
    var buf = save_model_to_buffer(model)
    var loaded = load_model_from_buffer(buf)
    # Embed should be zero
    for row in range(p.vocab_size):
        for col in range(p.hidden_dim):
            assert_true(approx_eq(loaded.embed.get(row, col), 0.0), "Default embed should be zero")
    # final_norm should be 1.0 (Model.__init__ sets norms to 1.0)
    for i in range(p.hidden_dim):
        assert_true(approx_eq(loaded.final_norm.get(i), 1.0), "Default norm should be 1.0")
    # Layer weights should match original (norms=1, projections=0)
    for i in range(model.layer_weights.numel()):
        assert_true(approx_eq(loaded.layer_weights.get(i), model.layer_weights.get(i)), "Layer weights roundtrip at " + String(i))
    print("  default_model_roundtrip: PASS")


fn main() raises:
    print("test_model_export")
    test_nmf_magic()
    test_nmf_buffer_write_read_u32()
    test_nmf_buffer_write_read_f32()
    test_serialize_params()
    test_deserialize_params()
    test_params_roundtrip()
    test_save_load_model()
    test_header_validation()
    test_version_validation()
    test_model_sizes_match()
    test_buffer_size()
    test_arch_roundtrip()
    test_default_model_roundtrip()
    print("All 13 model export tests passed!")
