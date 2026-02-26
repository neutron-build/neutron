# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Direct Q8 Loading Tests
# ===----------------------------------------------------------------------=== #

"""Tests for direct Q8_0 GGUF loading: read_tensor_q8_0_as_quantized,
load_gguf_quantized_direct_from_buffer, and equivalence vs roundtrip path."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.io.binary_reader import BinaryReader, _fp16_to_fp32
from neutron_mojo.io.gguf import (
    GGUF_MAGIC,
    GGUF_DEFAULT_ALIGNMENT,
    GGUF_F32,
    GGUF_Q8_0,
    parse_gguf_from_buffer,
    gguf_to_model_config,
    _align_offset,
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
)
from neutron_mojo.model.weight_reader import (
    QuantizedTensorData,
    read_tensor_q8_0_as_quantized,
    read_tensor_f32,
    load_gguf_quantized_direct_from_buffer,
    load_gguf_model_from_buffer,
)
from neutron_mojo.nn.model import Model, ModelParams, generate
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model, q_generate


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


# ===----------------------------------------------------------------------=== #
# Q8_0 Binary Helpers
# ===----------------------------------------------------------------------=== #

fn _write_q8_block(mut buf: List[UInt8], scale_f32: Float32, values: List[Int]):
    """Write a Q8_0 block: 2-byte FP16 scale + 32 INT8 values.

    Note: For testing we encode the scale approximately as FP16 bits.
    We use a simple approach: store a known FP16 value.
    """
    # Convert F32 scale to FP16 bits (simplified: use 0x3C00 = 1.0, 0x4000 = 2.0, etc.)
    # For testing, we'll use specific known FP16 values:
    # 0x3C00 = 1.0, 0x4000 = 2.0, 0x3800 = 0.5, 0x3000 = 0.25
    var scale_bits: Int
    if scale_f32 == 1.0:
        scale_bits = 0x3C00
    elif scale_f32 == 2.0:
        scale_bits = 0x4000
    elif scale_f32 == 0.5:
        scale_bits = 0x3800
    elif scale_f32 == 0.25:
        scale_bits = 0x3400
    elif scale_f32 == 0.125:
        scale_bits = 0x3000
    elif scale_f32 == 0.1:
        # FP16 for 0.1 ≈ 0x2E66
        scale_bits = 0x2E66
    else:
        scale_bits = 0x3C00  # default to 1.0

    # Write FP16 scale as 2 bytes little-endian
    buf.append(UInt8(scale_bits & 0xFF))
    buf.append(UInt8((scale_bits >> 8) & 0xFF))

    # Write 32 INT8 values
    for i in range(32):
        if i < len(values):
            var v = values[i]
            if v < 0:
                v = v + 256  # Convert signed to unsigned byte
            buf.append(UInt8(v))
        else:
            buf.append(UInt8(0))


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_read_q8_0_as_quantized_basic() raises:
    """Single Q8_0 block -> verify data and scales."""
    var buf = List[UInt8]()

    # Write one Q8_0 block: scale=1.0, values=[10, -20, 30, 0, ...] (32 values)
    var values = List[Int]()
    values.append(10)
    values.append(-20)
    values.append(30)
    values.append(0)
    for _ in range(28):
        values.append(5)
    _write_q8_block(buf, 1.0, values)

    var reader = BinaryReader(buf^)
    var qtd = read_tensor_q8_0_as_quantized(reader, 0, 32, 32)

    # Verify INT8 values stored as Float32
    assert_near(qtd.data.get(0), 10.0, 0.01, "data[0]=10")
    assert_near(qtd.data.get(1), -20.0, 0.01, "data[1]=-20")
    assert_near(qtd.data.get(2), 30.0, 0.01, "data[2]=30")
    assert_near(qtd.data.get(3), 0.0, 0.01, "data[3]=0")
    assert_near(qtd.data.get(4), 5.0, 0.01, "data[4]=5")

    # Verify scale
    assert_near(qtd.scales.get(0), 1.0, 0.01, "scale=1.0")

    print("  read_q8_0_as_quantized_basic: PASS")


fn test_read_q8_0_as_quantized_multi() raises:
    """Multiple Q8_0 blocks -> verify data + scales for each."""
    var buf = List[UInt8]()

    # Block 0: scale=1.0, all values=10
    var vals0 = List[Int]()
    for _ in range(32):
        vals0.append(10)
    _write_q8_block(buf, 1.0, vals0)

    # Block 1: scale=2.0, all values=-5
    var vals1 = List[Int]()
    for _ in range(32):
        vals1.append(-5)
    _write_q8_block(buf, 2.0, vals1)

    var reader = BinaryReader(buf^)
    var qtd = read_tensor_q8_0_as_quantized(reader, 0, 64, 32)

    # Block 0 values
    assert_near(qtd.data.get(0), 10.0, 0.01, "blk0 data[0]=10")
    assert_near(qtd.data.get(31), 10.0, 0.01, "blk0 data[31]=10")
    assert_near(qtd.scales.get(0), 1.0, 0.01, "blk0 scale=1.0")

    # Block 1 values
    assert_near(qtd.data.get(32), -5.0, 0.01, "blk1 data[0]=-5")
    assert_near(qtd.data.get(63), -5.0, 0.01, "blk1 data[31]=-5")
    assert_near(qtd.scales.get(1), 2.0, 0.01, "blk1 scale=2.0")

    print("  read_q8_0_as_quantized_multi: PASS")


fn test_quantized_tensor_data_struct() raises:
    """QuantizedTensorData struct creation and access."""
    var data = Tensor[DType.float32](Shape(8))
    var scales = Tensor[DType.float32](Shape(2))
    for i in range(8):
        data.set(i, Float32(i))
    scales.set(0, 0.5)
    scales.set(1, 1.5)

    var qtd = QuantizedTensorData(data^, scales^)
    assert_near(qtd.data.get(3), 3.0, 0.01, "qtd.data[3]")
    assert_near(qtd.scales.get(1), 1.5, 0.01, "qtd.scales[1]")

    print("  quantized_tensor_data_struct: PASS")


# ===----------------------------------------------------------------------=== #
# Full GGUF Builder with Mixed F32 + Q8 Tensors
# ===----------------------------------------------------------------------=== #

fn _build_mixed_gguf() raises -> List[UInt8]:
    """Build GGUF with F32 embed/norm/lm_head + Q8_0 projection weights.

    Model: 1 layer, hidden=4, heads=2, kv_heads=1, head_dim=2, ffn=8, vocab=8
    This creates a model matching tiny_test_params except vocab=8.
    """
    var buf = List[UInt8]()

    var hidden = 4
    var q_dim = 4   # num_q_heads*head_dim = 2*2
    var kv_dim = 2  # num_kv_heads*head_dim = 1*2
    var ffn_dim = 8
    var vocab = 8

    # Tensor count: 12 (embed, output_norm, output, attn_norm, q, k, v, o, ffn_norm, gate, up, down)
    var tensor_count = 12
    var meta_count = 8  # arch, block_count, embed_len, head_count, head_count_kv, ffn_len, vocab_size, context_length

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)  # version
    _write_u64_le(buf, tensor_count)
    _write_u64_le(buf, meta_count)

    # Metadata
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)  # STRING
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)  # UINT32
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, hidden)

    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, ffn_dim)

    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, vocab)

    _write_string_gguf(buf, "llama.context_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 32)

    # Tensor info — compute data offsets
    # F32 tensors: token_embd (vocab*hidden), output_norm (hidden), output (vocab*hidden)
    # F32 tensors: blk.0.attn_norm (hidden), blk.0.ffn_norm (hidden)
    # Q8_0 tensors: blk.0.attn_q, attn_k, attn_v, attn_output, ffn_gate, ffn_up, ffn_down
    var data_cursor = 0

    # token_embd [vocab, hidden] F32
    var embed_numel = vocab * hidden
    _write_string_gguf(buf, "token_embd.weight")
    _write_u32_le(buf, 2)  # ndims
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)  # F32 type
    _write_u64_le(buf, data_cursor)
    data_cursor += embed_numel * 4

    # output_norm [hidden] F32
    _write_string_gguf(buf, "output_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # output [vocab, hidden] F32
    _write_string_gguf(buf, "output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += vocab * hidden * 4

    # blk.0.attn_norm [hidden] F32
    _write_string_gguf(buf, "blk.0.attn_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # Q8_0 projections — each is blocks of 32 elements
    # Q8_0 block size = 34 bytes (2 scale + 32 int8)
    # For tensors smaller than 32 elements, still 1 block

    # blk.0.attn_q [q_dim, hidden] = 16 elements -> 1 block = 34 bytes
    var attn_q_numel = q_dim * hidden
    var attn_q_blocks = (attn_q_numel + 31) // 32
    _write_string_gguf(buf, "blk.0.attn_q.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, q_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 8)  # Q8_0 type
    _write_u64_le(buf, data_cursor)
    data_cursor += attn_q_blocks * 34

    # blk.0.attn_k [kv_dim, hidden] = 8 elements -> 1 block
    var attn_k_numel = kv_dim * hidden
    var attn_k_blocks = (attn_k_numel + 31) // 32
    _write_string_gguf(buf, "blk.0.attn_k.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, kv_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 8)
    _write_u64_le(buf, data_cursor)
    data_cursor += attn_k_blocks * 34

    # blk.0.attn_v [kv_dim, hidden] = 8 elements -> 1 block
    var attn_v_numel = kv_dim * hidden
    var attn_v_blocks = (attn_v_numel + 31) // 32
    _write_string_gguf(buf, "blk.0.attn_v.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, kv_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 8)
    _write_u64_le(buf, data_cursor)
    data_cursor += attn_v_blocks * 34

    # blk.0.attn_output [hidden, q_dim] = 16 elements -> 1 block
    var attn_o_numel = hidden * q_dim
    var attn_o_blocks = (attn_o_numel + 31) // 32
    _write_string_gguf(buf, "blk.0.attn_output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, hidden)
    _write_u64_le(buf, q_dim)
    _write_u32_le(buf, 8)
    _write_u64_le(buf, data_cursor)
    data_cursor += attn_o_blocks * 34

    # blk.0.ffn_norm [hidden] F32
    _write_string_gguf(buf, "blk.0.ffn_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # blk.0.ffn_gate [ffn, hidden] = 32 elements -> 1 block
    var ffn_gate_numel = ffn_dim * hidden
    var ffn_gate_blocks = (ffn_gate_numel + 31) // 32
    _write_string_gguf(buf, "blk.0.ffn_gate.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, ffn_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 8)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_gate_blocks * 34

    # blk.0.ffn_up [ffn, hidden] = 32 elements -> 1 block
    var ffn_up_numel = ffn_dim * hidden
    var ffn_up_blocks = (ffn_up_numel + 31) // 32
    _write_string_gguf(buf, "blk.0.ffn_up.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, ffn_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 8)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_up_blocks * 34

    # blk.0.ffn_down [hidden, ffn] = 32 elements -> 1 block
    var ffn_down_numel = hidden * ffn_dim
    var ffn_down_blocks = (ffn_down_numel + 31) // 32
    _write_string_gguf(buf, "blk.0.ffn_down.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, hidden)
    _write_u64_le(buf, ffn_dim)
    _write_u32_le(buf, 8)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_down_blocks * 34

    # Align
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # --- Tensor Data ---

    # token_embd: vocab*hidden = 32 floats (F32)
    for i in range(embed_numel):
        _write_f32_le(buf, Float32(i) * 0.01)

    # output_norm: hidden = 4 floats (F32)
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # output/lm_head: vocab*hidden = 32 floats (F32)
    for i in range(vocab * hidden):
        _write_f32_le(buf, Float32(i % 7) * 0.02)

    # blk.0.attn_norm: hidden = 4 floats (F32)
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # Q8_0 projection data: attn_q (16 elements, 1 block)
    # scale=0.5, values = [1, 2, 3, ..., 16, 0, 0, ..., 0]
    var q_vals = List[Int]()
    for i in range(attn_q_numel):
        q_vals.append(i + 1)
    _write_q8_block(buf, 0.5, q_vals)

    # attn_k (8 elements, 1 block)
    var k_vals = List[Int]()
    for i in range(attn_k_numel):
        k_vals.append(i + 1)
    _write_q8_block(buf, 0.25, k_vals)

    # attn_v (8 elements, 1 block)
    var v_vals = List[Int]()
    for i in range(attn_v_numel):
        v_vals.append(i + 1)
    _write_q8_block(buf, 0.25, v_vals)

    # attn_output (16 elements, 1 block)
    var o_vals = List[Int]()
    for i in range(attn_o_numel):
        o_vals.append(i + 1)
    _write_q8_block(buf, 0.5, o_vals)

    # blk.0.ffn_norm: hidden = 4 floats (F32)
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # ffn_gate (32 elements, 1 block)
    var gate_vals = List[Int]()
    for i in range(ffn_gate_numel):
        gate_vals.append((i % 11) - 5)
    _write_q8_block(buf, 0.125, gate_vals)

    # ffn_up (32 elements, 1 block)
    var up_vals = List[Int]()
    for i in range(ffn_up_numel):
        up_vals.append((i % 13) - 6)
    _write_q8_block(buf, 0.125, up_vals)

    # ffn_down (32 elements, 1 block)
    var down_vals = List[Int]()
    for i in range(ffn_down_numel):
        down_vals.append((i % 7) - 3)
    _write_q8_block(buf, 0.125, down_vals)

    return buf^


fn test_load_direct_q8_embed_f32() raises:
    """F32 embed loads correctly alongside Q8 projections."""
    var buf = _build_mixed_gguf()
    var buf2 = buf.copy()

    # First check: does the regular FP32 loader work on this GGUF?
    var fp32_model = load_gguf_model_from_buffer(buf^)
    # embed is 2D Shape(vocab, hidden) — use (row, col) indexing
    assert_near(fp32_model.embed.get(0, 0), 0.0, 0.001, "fp32 embed[0,0]=0")
    assert_near(fp32_model.embed.get(0, 1), 0.01, 0.001, "fp32 embed[0,1]=0.01")

    # Now check the direct Q8 loader
    var model = load_gguf_quantized_direct_from_buffer(buf2^, block_size=32)

    # Verify embed loaded as F32 (2D Shape(vocab, hidden))
    assert_near(model.embed.get(0, 0), 0.0, 0.001, "embed[0,0]=0")
    assert_near(model.embed.get(0, 1), 0.01, 0.001, "embed[0,1]=0.01")

    # Verify final_norm is 1.0
    assert_near(model.final_norm.get(0), 1.0, 0.001, "norm[0]=1.0")

    print("  load_direct_q8_embed_f32: PASS")


fn test_load_direct_q8_from_buffer() raises:
    """Full GGUF buffer with Q8 projection + F32 embed -> QuantizedModel with correct weights."""
    var buf = _build_mixed_gguf()
    var model = load_gguf_quantized_direct_from_buffer(buf^, block_size=32)

    assert_eq(model.params.vocab_size, 8, "vocab=8")
    assert_eq(model.params.hidden_dim, 4, "hidden=4")
    assert_eq(model.params.num_layers, 1, "layers=1")
    assert_eq(model.block_size, 32, "block_size=32")

    # Verify Q8 projection weights are integer-valued (INT8 stored as Float32)
    var off = model._layer_offsets(0)
    var wq_val = model.layer_weights.get(off.wq)
    # The value should be the INT8 value (1.0 for first element)
    assert_near(wq_val, 1.0, 0.01, "wq[0] is INT8 value 1")

    var wq_val2 = model.layer_weights.get(off.wq + 1)
    assert_near(wq_val2, 2.0, 0.01, "wq[1] is INT8 value 2")

    # Verify scales loaded
    var soff = model._layer_scale_offsets(0)
    var wq_scale = model.layer_scales.get(soff.wq)
    assert_near(wq_scale, 0.5, 0.01, "wq scale=0.5")

    print("  load_direct_q8_from_buffer: PASS")


fn test_direct_vs_roundtrip_equivalence() raises:
    """Compare direct Q8 loading vs load-as-F32 + quantize_from_model.

    For the roundtrip path (F32 GGUF), both should produce identical results
    since the F32 path quantizes from the same source data.
    We build an all-F32 GGUF and compare both paths.
    """
    # Build all-F32 GGUF
    var buf1 = _build_all_f32_gguf()
    var buf2 = buf1.copy()

    # Path A: load as FP32, then quantize
    var fp32_model = load_gguf_model_from_buffer(buf1^)
    var roundtrip = quantize_from_model(fp32_model, block_size=32)

    # Path B: direct loading (F32 tensors go through quantize on load)
    var direct = load_gguf_quantized_direct_from_buffer(buf2^, block_size=32)

    # Compare logits from both models
    var prompt = List[Int]()
    prompt.append(1)

    var rt_tokens = q_generate(roundtrip, prompt, max_new_tokens=3)
    var dr_tokens = q_generate(direct, prompt, max_new_tokens=3)

    assert_eq(len(rt_tokens), 3, "roundtrip generated 3")
    assert_eq(len(dr_tokens), 3, "direct generated 3")

    # Both should produce valid tokens
    for i in range(3):
        assert_true(rt_tokens[i] >= 0 and rt_tokens[i] < 8, "rt valid")
        assert_true(dr_tokens[i] >= 0 and dr_tokens[i] < 8, "dr valid")

    # For F32 GGUF, both paths should give identical results
    for i in range(3):
        assert_eq(rt_tokens[i], dr_tokens[i], "roundtrip == direct token " + String(i))

    print("  direct_vs_roundtrip_equivalence: PASS")


fn test_direct_q8_generates() raises:
    """Loaded model can run forward pass and produce valid token IDs."""
    var buf = _build_mixed_gguf()
    var model = load_gguf_quantized_direct_from_buffer(buf^, block_size=32)

    var prompt = List[Int]()
    prompt.append(2)
    prompt.append(3)

    var tokens = q_generate(model, prompt, max_new_tokens=3)
    assert_eq(len(tokens), 3, "generated 3 tokens")

    for i in range(len(tokens)):
        assert_true(tokens[i] >= 0, "token >= 0")
        assert_true(tokens[i] < 8, "token < vocab_size")

    print("  direct_q8_generates: PASS")


# ===----------------------------------------------------------------------=== #
# All-F32 GGUF for equivalence testing
# ===----------------------------------------------------------------------=== #

fn _build_all_f32_gguf() raises -> List[UInt8]:
    """Build all-F32 GGUF for equivalence testing (same architecture as mixed).

    Model: 1 layer, hidden=4, heads=2, kv_heads=1, head_dim=2, ffn=8, vocab=8
    """
    var buf = List[UInt8]()

    var hidden = 4
    var q_dim = 4
    var kv_dim = 2
    var ffn_dim = 8
    var vocab = 8

    var tensor_count = 12
    var meta_count = 8

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, tensor_count)
    _write_u64_le(buf, meta_count)

    # Metadata (same as mixed)
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, hidden)

    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, ffn_dim)

    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, vocab)

    _write_string_gguf(buf, "llama.context_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 32)

    # All tensor info — all F32
    var data_cursor = 0

    # token_embd
    _write_string_gguf(buf, "token_embd.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += vocab * hidden * 4

    # output_norm
    _write_string_gguf(buf, "output_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # output
    _write_string_gguf(buf, "output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += vocab * hidden * 4

    # blk.0.attn_norm
    _write_string_gguf(buf, "blk.0.attn_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # blk.0.attn_q
    _write_string_gguf(buf, "blk.0.attn_q.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, q_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += q_dim * hidden * 4

    # blk.0.attn_k
    _write_string_gguf(buf, "blk.0.attn_k.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, kv_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += kv_dim * hidden * 4

    # blk.0.attn_v
    _write_string_gguf(buf, "blk.0.attn_v.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, kv_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += kv_dim * hidden * 4

    # blk.0.attn_output
    _write_string_gguf(buf, "blk.0.attn_output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, hidden)
    _write_u64_le(buf, q_dim)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * q_dim * 4

    # blk.0.ffn_norm
    _write_string_gguf(buf, "blk.0.ffn_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # blk.0.ffn_gate
    _write_string_gguf(buf, "blk.0.ffn_gate.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, ffn_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_dim * hidden * 4

    # blk.0.ffn_up
    _write_string_gguf(buf, "blk.0.ffn_up.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, ffn_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_dim * hidden * 4

    # blk.0.ffn_down
    _write_string_gguf(buf, "blk.0.ffn_down.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, hidden)
    _write_u64_le(buf, ffn_dim)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * ffn_dim * 4

    # Align
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # Tensor data — all F32
    # token_embd
    for i in range(vocab * hidden):
        _write_f32_le(buf, Float32(i) * 0.01)

    # output_norm
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # output/lm_head
    for i in range(vocab * hidden):
        _write_f32_le(buf, Float32(i % 7) * 0.02)

    # blk.0.attn_norm
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # blk.0.attn_q
    for i in range(q_dim * hidden):
        _write_f32_le(buf, Float32(i % 5) * 0.01)

    # blk.0.attn_k
    for i in range(kv_dim * hidden):
        _write_f32_le(buf, Float32(i % 3) * 0.01)

    # blk.0.attn_v
    for i in range(kv_dim * hidden):
        _write_f32_le(buf, Float32(i % 4) * 0.01)

    # blk.0.attn_output
    for i in range(hidden * q_dim):
        _write_f32_le(buf, Float32(i % 6) * 0.01)

    # blk.0.ffn_norm
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # blk.0.ffn_gate
    for i in range(ffn_dim * hidden):
        _write_f32_le(buf, Float32(i % 11) * 0.001)

    # blk.0.ffn_up
    for i in range(ffn_dim * hidden):
        _write_f32_le(buf, Float32(i % 13) * 0.001)

    # blk.0.ffn_down
    for i in range(hidden * ffn_dim):
        _write_f32_le(buf, Float32(i % 7) * 0.001)

    return buf^


fn test_all_f32_gguf_basic_load() raises:
    """Diagnostic: verify the all-F32 GGUF loads correctly."""
    var buf = _build_all_f32_gguf()
    var buf2 = buf.copy()
    var buf3 = buf.copy()

    # Step 1: Parse GGUF
    var gguf = parse_gguf_from_buffer(buf2^)
    var info = gguf.get_tensor_info("token_embd.weight")
    var abs_offset = gguf.data_offset + info.offset

    # Step 2: Create reader and read embed data directly
    var reader = BinaryReader(buf3^)
    var data = read_tensor_f32(reader, abs_offset, info.numel())
    # Use .get() which is reliable (data_ptr() has aliasing issues in Mojo)
    assert_near(data.get(0), 0.0, 0.001, "read data[0]=0")
    assert_near(data.get(1), 0.01, 0.001, "read data[1]=0.01")
    assert_near(data.get(2), 0.02, 0.001, "read data[2]=0.02")

    # Step 3: Create model and set embed using .get()
    var config = gguf_to_model_config(gguf)
    from neutron_mojo.model.populate import model_from_config
    var model = model_from_config(config)

    for i in range(info.numel()):
        model.embed.set(i, data.get(i))

    # embed is 2D Shape(vocab, hidden) — use (row, col) indexing
    assert_near(model.embed.get(0, 0), 0.0, 0.001, "embed[0,0]=0")
    assert_near(model.embed.get(0, 1), 0.01, 0.001, "embed[0,1]=0.01")

    print("  all_f32_gguf_basic_load: PASS")


fn main() raises:
    print("test_direct_q8_loading:")

    test_read_q8_0_as_quantized_basic()
    test_read_q8_0_as_quantized_multi()
    test_quantized_tensor_data_struct()
    test_all_f32_gguf_basic_load()
    test_load_direct_q8_embed_f32()
    test_load_direct_q8_from_buffer()
    test_direct_vs_roundtrip_equivalence()
    test_direct_q8_generates()

    print("ALL PASSED (7 tests)")
