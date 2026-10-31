"""Container expectations independent of production GGUF writer; unexecuted."""
from std.testing import assert_true, assert_equal
from std.math import abs
from neutron_mojo.io.gguf import parse_gguf_from_buffer
from neutron_mojo.io.model_export import serialize_params, deserialize_params, save_model_to_buffer, load_model_from_buffer
from neutron_mojo.model.architecture import arch_from_name
from neutron_mojo.nn.model import Model, tiny_test_params
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.kv_cache import MultiLayerKVCache


def uint_le(mut bytes: List[UInt8], value: Int, count: Int):
    for i in range(count):
        bytes.append(UInt8((value >> (8 * i)) & 255))


def text(mut bytes: List[UInt8], value: String):
    uint_le(bytes, value.byte_length(), 8)
    var chars = value.as_bytes()
    for i in range(len(chars)):
        bytes.append(chars[i])


def alignment_file(alignment: Int, alignment_type: Int = 4, tensor_offset: Int = 0) -> List[UInt8]:
    var bytes = List[UInt8]()
    uint_le(bytes, 0x46554747, 4)
    uint_le(bytes, 3, 4)
    uint_le(bytes, 1, 8)
    uint_le(bytes, 1, 8)
    text(bytes, "general.alignment")
    uint_le(bytes, alignment_type, 4)
    uint_le(bytes, alignment, 4)
    text(bytes, "x")
    uint_le(bytes, 2, 4)
    uint_le(bytes, 2, 8)
    uint_le(bytes, 3, 8)
    uint_le(bytes, 0, 4)  # F32
    uint_le(bytes, tensor_offset, 8)
    # Fixture computes padding by remainder, independently of _align_offset.
    if alignment > 0:
        while len(bytes) % alignment != 0:
            bytes.append(0)
    for i in range(24):
        bytes.append(UInt8(i))
    return bytes^


def test_alignment_matrix() raises:
    for alignment in [32, 64, 128]:
        var bytes = alignment_file(alignment)
        var expected_base = len(bytes) - 24
        var gguf = parse_gguf_from_buffer(bytes^)
        assert_equal(gguf.alignment, alignment)
        assert_equal(gguf.data_offset, expected_base)
        assert_equal(gguf.data_offset % alignment, 0)
        var info = gguf.get_tensor_info("x")
        assert_equal(info.shape[0], 2)
        assert_equal(info.shape[1], 3)
    for alignment in [0, 3, 48]:
        var refused = False
        try:
            var bad = alignment_file(alignment)
            var ignored = parse_gguf_from_buffer(bad^)
        except:
            refused = True
        assert_true(refused)
    var refused = False
    try:
        var bad = alignment_file(64, alignment_type=5)  # I32 is not UInt32
        var ignored = parse_gguf_from_buffer(bad^)
    except:
        refused = True
    assert_true(refused)
    refused = False
    try:
        var bad = alignment_file(64, tensor_offset=4)
        var ignored = parse_gguf_from_buffer(bad^)
    except:
        refused = True
    assert_true(refused)


def test_full_architecture_fields() raises:
    for name in ["Llama", "Mistral", "Phi", "Gemma", "Qwen"]:
        var p = tiny_test_params()
        p.arch = arch_from_name(name)
        p.rope_theta = 12345.6789012345
        p.arch.use_sliding_window = True
        p.arch.window_size = 17
        p.arch.partial_rotary_factor = 0.625
        p.arch.use_gelu = True
        p.arch.use_pre_norm_bias = True
        p.arch.rope_scaling = 1.75
        p.arch.norm_eps = 0.00003125
        var q = deserialize_params(serialize_params(p))
        assert_true(q.arch.kind == p.arch.kind)
        assert_equal(q.rope_theta, p.rope_theta)
        assert_equal(q.arch.use_sliding_window, True)
        assert_equal(q.arch.window_size, 17)
        assert_equal(q.arch.partial_rotary_factor, Float32(0.625))
        assert_equal(q.arch.use_gelu, True)
        assert_equal(q.arch.use_pre_norm_bias, True)
        assert_equal(q.arch.rope_scaling, Float32(1.75))
        assert_equal(q.arch.norm_eps, Float32(0.00003125))


def test_nmf_logits() raises:
    # An explicitly supported GELU + partial rotary configuration with new epsilon.
    var p = tiny_test_params()
    p.head_dim = 4
    p.hidden_dim = 8
    p.arch.use_gelu = True
    p.arch.partial_rotary_factor = 0.5
    p.arch.norm_eps = 0.00003125
    p.rope_theta = 12345.6789
    var model = Model(p)
    for i in range(model.layer_weights.numel()):
        model.layer_weights.set(i, Float32((i % 13) - 6) * 0.02)
    for i in range(model.embed.numel()):
        model.embed.data_ptr()[i] = Float32((i % 7) - 3) * 0.1
        model.lm_head.data_ptr()[i] = Float32((i % 11) - 5) * 0.03
    for i in range(p.hidden_dim):
        model.final_norm.set(i, 1)
    var bytes = save_model_to_buffer(model)
    var loaded = load_model_from_buffer(bytes)
    var rope = RoPETable(p.head_dim, p.max_seq_len, theta=p.rope_theta, rotary_dim=p.rotary_dim())
    var cache_a = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var cache_b = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    for pos in range(3):
        var a = model.forward(pos + 1, cache_a, rope, pos)
        var b = loaded.forward(pos + 1, cache_b, rope, pos)
        for i in range(p.vocab_size):
            assert_equal(a.get(i), b.get(i))


def main() raises:
    test_alignment_matrix()
    test_full_architecture_fields()
    test_nmf_logits()
    print("R2 independent GGUF alignment and NMF architecture/logits oracles PASS")
