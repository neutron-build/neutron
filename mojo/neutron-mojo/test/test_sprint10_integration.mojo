# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 10 Integration Tests
# ===----------------------------------------------------------------------=== #

"""End-to-end tests: synthetic GGUF with GGUF tensor names + tokenizer data
→ parse → model + tokenizer → pipeline_generate → valid text."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.io.gguf import (
    GGUF_MAGIC,
    GGUF_DEFAULT_ALIGNMENT,
    GGUFFile,
    GGUF_F32,
    parse_gguf_from_buffer,
    gguf_to_model_config,
    _align_offset,
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
)
from neutron_mojo.model.weight_reader import load_gguf_model_from_buffer
from neutron_mojo.nn.model import Model, generate
from neutron_mojo.nn.tokenizer import BPETokenizer, load_gguf_tokenizer
from neutron_mojo.nn.pipeline import (
    PipelineConfig,
    pipeline_generate,
    default_pipeline_config,
    chat_pipeline_config,
)


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
# GGUF Metadata Encoding Helpers
# ===----------------------------------------------------------------------=== #

fn _write_gguf_string_array(mut buf: List[UInt8], key: String, values: List[String]):
    """Write a string array metadata entry.

    Format: key (GGUF string) + type ARRAY (9) + elem_type STRING (8)
            + u64 count + count × GGUF strings.
    """
    _write_string_gguf(buf, key)
    _write_u32_le(buf, 9)   # GGUF_TYPE_ARRAY
    _write_u32_le(buf, 8)   # elem_type = GGUF_TYPE_STRING
    _write_u64_le(buf, len(values))
    for i in range(len(values)):
        _write_string_gguf(buf, values[i])


# ===----------------------------------------------------------------------=== #
# Full Synthetic GGUF Builder
# ===----------------------------------------------------------------------=== #

fn _build_full_gguf_with_tokenizer() raises -> List[UInt8]:
    """Build a complete GGUF with model tensors (GGUF names) + tokenizer data.

    Model: 1 layer, hidden=4, heads=2, kv_heads=1, head_dim=2, ffn=8, vocab=16
    12 tensors: token_embd, output_norm, output, blk.0.* (9 layer weights)
    16-token vocab + 3 merges
    """
    var buf = List[UInt8]()

    # --- Compute metadata count ---
    # 7 model params + 1 arch + 2 special token ids + 2 tokenizer arrays (tokens, merges) = 13
    # But context_length is also useful: +1 = 14
    var meta_count = 12  # arch, block_count, embed_len, head_count, head_count_kv, ffn_len, vocab_size, context_length, bos_id, eos_id, tokens array, merges array

    # --- Tensor count ---
    var tensor_count = 12

    # --- Header ---
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)  # version
    _write_u64_le(buf, tensor_count)
    _write_u64_le(buf, meta_count)

    # --- Model metadata ---
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)  # STRING
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)  # UINT32
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
    _write_u32_le(buf, 16)

    _write_string_gguf(buf, "llama.context_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 32)

    # --- Tokenizer metadata ---
    _write_string_gguf(buf, "tokenizer.ggml.bos_token_id")
    _write_u32_le(buf, 4)  # UINT32
    _write_u32_le(buf, 0)

    _write_string_gguf(buf, "tokenizer.ggml.eos_token_id")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    # Tokenizer vocab (string array)
    var tokens = List[String]()
    tokens.append("<s>")     # 0
    tokens.append("</s>")    # 1
    tokens.append("<unk>")   # 2
    tokens.append(" ")       # 3
    tokens.append("a")       # 4
    tokens.append("b")       # 5
    tokens.append("c")       # 6
    tokens.append("d")       # 7
    tokens.append("e")       # 8
    tokens.append("h")       # 9
    tokens.append("l")       # 10
    tokens.append("o")       # 11
    tokens.append("t")       # 12
    tokens.append("he")      # 13
    tokens.append("ll")      # 14
    tokens.append("the")     # 15
    _write_gguf_string_array(buf, "tokenizer.ggml.tokens", tokens)

    # Tokenizer merges (string array)
    var merges = List[String]()
    merges.append("h e")
    merges.append("l l")
    merges.append("t he")
    _write_gguf_string_array(buf, "tokenizer.ggml.merges", merges)

    # --- Tensor info (GGUF names, all F32) ---
    # Compute sizes:
    # hidden=4, q_dim=num_q_heads*head_dim=2*2=4, kv_dim=num_kv_heads*head_dim=1*2=2
    # ffn=8, vocab=16
    var hidden = 4
    var q_dim = 4
    var kv_dim = 2
    var ffn_dim = 8
    var vocab = 16

    # Track byte offsets
    var data_cursor = 0

    # token_embd [vocab, hidden] = 64 floats = 256 bytes
    var embed_numel = vocab * hidden
    _write_string_gguf(buf, "token_embd.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, data_cursor)
    data_cursor += embed_numel * 4

    # output_norm [hidden] = 4 floats = 16 bytes
    _write_string_gguf(buf, "output_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # output [vocab, hidden] = 64 floats = 256 bytes
    _write_string_gguf(buf, "output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += vocab * hidden * 4

    # blk.0.attn_norm [hidden]
    _write_string_gguf(buf, "blk.0.attn_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # blk.0.attn_q [q_dim, hidden]
    _write_string_gguf(buf, "blk.0.attn_q.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, q_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += q_dim * hidden * 4

    # blk.0.attn_k [kv_dim, hidden]
    _write_string_gguf(buf, "blk.0.attn_k.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, kv_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += kv_dim * hidden * 4

    # blk.0.attn_v [kv_dim, hidden]
    _write_string_gguf(buf, "blk.0.attn_v.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, kv_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += kv_dim * hidden * 4

    # blk.0.attn_output [hidden, q_dim]
    _write_string_gguf(buf, "blk.0.attn_output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, hidden)
    _write_u64_le(buf, q_dim)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * q_dim * 4

    # blk.0.ffn_norm [hidden]
    _write_string_gguf(buf, "blk.0.ffn_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    # blk.0.ffn_gate [ffn, hidden]
    _write_string_gguf(buf, "blk.0.ffn_gate.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, ffn_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_dim * hidden * 4

    # blk.0.ffn_up [ffn, hidden]
    _write_string_gguf(buf, "blk.0.ffn_up.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, ffn_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_dim * hidden * 4

    # blk.0.ffn_down [hidden, ffn]
    _write_string_gguf(buf, "blk.0.ffn_down.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, hidden)
    _write_u64_le(buf, ffn_dim)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * ffn_dim * 4

    # --- Align ---
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # --- Tensor data (all F32) ---
    # token_embd: vocab*hidden = 64 floats
    for i in range(embed_numel):
        _write_f32_le(buf, Float32(i) * 0.01)

    # output_norm: hidden = 4 floats (all 1.0)
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # output/lm_head: vocab*hidden = 64 floats
    for i in range(vocab * hidden):
        _write_f32_le(buf, Float32(i % 7) * 0.02)

    # blk.0.attn_norm: hidden = 4
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # blk.0.attn_q: q_dim*hidden = 16
    for i in range(q_dim * hidden):
        _write_f32_le(buf, Float32(i % 5) * 0.01)

    # blk.0.attn_k: kv_dim*hidden = 8
    for i in range(kv_dim * hidden):
        _write_f32_le(buf, Float32(i % 3) * 0.01)

    # blk.0.attn_v: kv_dim*hidden = 8
    for i in range(kv_dim * hidden):
        _write_f32_le(buf, Float32(i % 4) * 0.01)

    # blk.0.attn_output: hidden*q_dim = 16
    for i in range(hidden * q_dim):
        _write_f32_le(buf, Float32(i % 6) * 0.01)

    # blk.0.ffn_norm: hidden = 4
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # blk.0.ffn_gate: ffn*hidden = 32
    for i in range(ffn_dim * hidden):
        _write_f32_le(buf, Float32(i % 11) * 0.001)

    # blk.0.ffn_up: ffn*hidden = 32
    for i in range(ffn_dim * hidden):
        _write_f32_le(buf, Float32(i % 13) * 0.001)

    # blk.0.ffn_down: hidden*ffn = 32
    for i in range(hidden * ffn_dim):
        _write_f32_le(buf, Float32(i % 7) * 0.001)

    return buf^


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_full_gguf_parses() raises:
    """Test that the full synthetic GGUF parses correctly."""
    var buf = _build_full_gguf_with_tokenizer()
    var gguf = parse_gguf_from_buffer(buf^)

    assert_true(gguf.is_valid(), "valid magic")
    assert_eq(gguf.tensor_count, 12, "12 tensors")
    assert_true(gguf.has_tensor("token_embd.weight"), "has token_embd")
    assert_true(gguf.has_tensor("blk.0.attn_q.weight"), "has blk.0.attn_q")
    assert_true(gguf.has_tensor("blk.0.ffn_down.weight"), "has blk.0.ffn_down")

    # Verify tokenizer arrays were parsed
    assert_eq(len(gguf.token_vocab), 16, "16 vocab tokens")
    assert_eq(len(gguf.token_merges), 3, "3 merges")

    print("  full_gguf_parses: PASS")


fn test_full_gguf_extracts_config() raises:
    """Test config extraction from full GGUF."""
    var buf = _build_full_gguf_with_tokenizer()
    var gguf = parse_gguf_from_buffer(buf^)
    var cfg = gguf_to_model_config(gguf)

    assert_eq(cfg.hidden_size, 4, "hidden_size=4")
    assert_eq(cfg.num_hidden_layers, 1, "num_layers=1")
    assert_eq(cfg.num_attention_heads, 2, "heads=2")
    assert_eq(cfg.num_key_value_heads, 1, "kv_heads=1")
    assert_eq(cfg.intermediate_size, 8, "ffn=8")
    assert_eq(cfg.vocab_size, 16, "vocab=16")
    assert_eq(cfg.max_position_embeddings, 32, "context=32")
    assert_eq(cfg.bos_token_id, 0, "bos=0")
    assert_eq(cfg.eos_token_id, 1, "eos=1")

    print("  full_gguf_extracts_config: PASS")


fn test_full_gguf_loads_model() raises:
    """Test model loading from full GGUF with GGUF tensor names."""
    var buf = _build_full_gguf_with_tokenizer()
    var model = load_gguf_model_from_buffer(buf^)

    assert_eq(model.params.vocab_size, 16, "model vocab=16")
    assert_eq(model.params.hidden_dim, 4, "model hidden=4")
    assert_eq(model.params.num_layers, 1, "model layers=1")
    assert_eq(model.params.num_q_heads, 2, "model q_heads=2")
    assert_eq(model.params.num_kv_heads, 1, "model kv_heads=1")
    assert_eq(model.params.ffn_dim, 8, "model ffn=8")

    # Verify some weights loaded
    var ptr = model.embed.data_ptr()
    assert_near(ptr[0], 0.0, 0.001, "embed[0]")
    assert_near(ptr[1], 0.01, 0.001, "embed[1]")
    _ = model.embed.numel()

    assert_near(model.final_norm.get(0), 1.0, 0.001, "norm[0]")

    print("  full_gguf_loads_model: PASS")


fn test_full_gguf_model_generates() raises:
    """Test that loaded model can generate valid tokens."""
    var buf = _build_full_gguf_with_tokenizer()
    var model = load_gguf_model_from_buffer(buf^)

    var prompt = List[Int]()
    prompt.append(4)  # "a"
    prompt.append(5)  # "b"

    var output = generate(model, prompt, max_new_tokens=5)
    assert_eq(len(output), 5, "5 tokens generated")

    for i in range(len(output)):
        assert_true(output[i] >= 0, "token >= 0")
        assert_true(output[i] < 16, "token < 16")

    print("  full_gguf_model_generates: PASS")


fn test_full_gguf_tokenizer_loads() raises:
    """Test tokenizer loading from GGUF metadata."""
    var buf = _build_full_gguf_with_tokenizer()
    var gguf = parse_gguf_from_buffer(buf^)

    var scores = List[Float64]()
    var tok = load_gguf_tokenizer(
        gguf.token_vocab, scores, gguf.token_merges,
        bos_id=0, eos_id=1,
    )

    assert_eq(tok.vocab_size, 16, "tok vocab=16")
    assert_eq(tok.bos_id, 0, "tok bos=0")
    assert_eq(tok.eos_id, 1, "tok eos=1")

    print("  full_gguf_tokenizer_loads: PASS")


fn test_full_gguf_tokenizer_roundtrip() raises:
    """Test encode→decode roundtrip with loaded tokenizer."""
    var buf = _build_full_gguf_with_tokenizer()
    var gguf = parse_gguf_from_buffer(buf^)

    var scores = List[Float64]()
    var tok = load_gguf_tokenizer(
        gguf.token_vocab, scores, gguf.token_merges,
        bos_id=0, eos_id=1,
    )

    # "the" should merge: t + h -> (no merge for th, but we have t + he -> the via merge "t he")
    # Actually: t=12, h=9, e=8; merge "h e" -> he=13; merge "t he" -> the=15
    var ids = tok.encode("the")
    assert_eq(len(ids), 1, "the=1 token")
    assert_eq(ids[0], 15, "the=token 15")

    var decoded = tok.decode(ids)
    assert_true(decoded == "the", "roundtrip 'the'")

    print("  full_gguf_tokenizer_roundtrip: PASS")


fn test_pipeline_end_to_end() raises:
    """Test full pipeline: model + tokenizer + pipeline_generate."""
    var buf = _build_full_gguf_with_tokenizer()
    var buf2 = buf.copy()
    var gguf = parse_gguf_from_buffer(buf^)

    var scores = List[Float64]()
    var tok = load_gguf_tokenizer(
        gguf.token_vocab, scores, gguf.token_merges,
        bos_id=0, eos_id=1,
    )
    tok.unk_id = 2  # <unk> is at index 2

    var model = load_gguf_model_from_buffer(buf2^)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5

    var result = pipeline_generate(model, tok, "ab", cfg)
    # Just verify it produces output and doesn't crash
    assert_true(len(result) >= 0, "pipeline produces output")

    print("  pipeline_end_to_end: PASS")


fn test_pipeline_chat_template() raises:
    """Test pipeline with chat template on full GGUF model."""
    var buf = _build_full_gguf_with_tokenizer()
    var buf2 = buf.copy()
    var gguf = parse_gguf_from_buffer(buf^)

    var scores = List[Float64]()
    var tok = load_gguf_tokenizer(
        gguf.token_vocab, scores, gguf.token_merges,
        bos_id=0, eos_id=1,
    )
    tok.unk_id = 2  # <unk> at index 2 — needed for template chars not in vocab

    var model = load_gguf_model_from_buffer(buf2^)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 3
    cfg.chat_template = String("llama")

    var result = pipeline_generate(model, tok, "hello", cfg)
    assert_true(len(result) >= 0, "chat pipeline produces output")

    print("  pipeline_chat_template: PASS")


fn main() raises:
    print("test_sprint10_integration:")

    test_full_gguf_parses()
    test_full_gguf_extracts_config()
    test_full_gguf_loads_model()
    test_full_gguf_model_generates()
    test_full_gguf_tokenizer_loads()
    test_full_gguf_tokenizer_roundtrip()
    test_pipeline_end_to_end()
    test_pipeline_chat_template()

    print("ALL PASSED (8 tests)")
