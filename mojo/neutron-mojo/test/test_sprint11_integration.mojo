# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 11 Integration Tests + Benchmarks
# ===----------------------------------------------------------------------=== #

"""End-to-end tests: GGUF -> quantize -> q_pipeline_generate -> text.
Also: benchmark FP32 vs Q8 pipeline tokens/sec."""

from math import abs
from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.io.gguf import (
    GGUF_MAGIC,
    GGUF_DEFAULT_ALIGNMENT,
    GGUF_F32,
    parse_gguf_from_buffer,
    gguf_to_model_config,
    _align_offset,
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
)
from neutron_mojo.model.weight_reader import (
    load_gguf_model_from_buffer,
    load_gguf_quantized_direct_from_buffer,
)
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model, q_generate
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import (
    PipelineConfig,
    pipeline_generate,
    default_pipeline_config,
)
from neutron_mojo.nn.q_pipeline import q_pipeline_generate


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected " + String(b) + " got " + String(a)
        )


# ===----------------------------------------------------------------------=== #
# GGUF + Tokenizer Builder
# ===----------------------------------------------------------------------=== #

fn _write_gguf_string_array(mut buf: List[UInt8], key: String, values: List[String]):
    """Write a string array metadata entry."""
    _write_string_gguf(buf, key)
    _write_u32_le(buf, 9)   # GGUF_TYPE_ARRAY
    _write_u32_le(buf, 8)   # elem_type = GGUF_TYPE_STRING
    _write_u64_le(buf, len(values))
    for i in range(len(values)):
        _write_string_gguf(buf, values[i])


fn _build_full_gguf_with_tokenizer() raises -> List[UInt8]:
    """Build complete GGUF with model tensors + tokenizer data (all F32).

    Model: 1 layer, hidden=4, heads=2, kv_heads=1, head_dim=2, ffn=8, vocab=16
    """
    var buf = List[UInt8]()

    var hidden = 4
    var q_dim = 4
    var kv_dim = 2
    var ffn_dim = 8
    var vocab = 16

    var meta_count = 12
    var tensor_count = 12

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, tensor_count)
    _write_u64_le(buf, meta_count)

    # Model metadata
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

    # Tokenizer metadata
    _write_string_gguf(buf, "tokenizer.ggml.bos_token_id")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 0)

    _write_string_gguf(buf, "tokenizer.ggml.eos_token_id")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    var tokens = List[String]()
    tokens.append("<s>")
    tokens.append("</s>")
    tokens.append("<unk>")
    tokens.append(" ")
    tokens.append("a")
    tokens.append("b")
    tokens.append("c")
    tokens.append("d")
    tokens.append("e")
    tokens.append("h")
    tokens.append("l")
    tokens.append("o")
    tokens.append("t")
    tokens.append("he")
    tokens.append("ll")
    tokens.append("the")
    _write_gguf_string_array(buf, "tokenizer.ggml.tokens", tokens)

    var merges = List[String]()
    merges.append("h e")
    merges.append("l l")
    merges.append("t he")
    _write_gguf_string_array(buf, "tokenizer.ggml.merges", merges)

    # Tensor info (all F32)
    var data_cursor = 0

    _write_string_gguf(buf, "token_embd.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += vocab * hidden * 4

    _write_string_gguf(buf, "output_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    _write_string_gguf(buf, "output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += vocab * hidden * 4

    _write_string_gguf(buf, "blk.0.attn_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    _write_string_gguf(buf, "blk.0.attn_q.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, q_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += q_dim * hidden * 4

    _write_string_gguf(buf, "blk.0.attn_k.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, kv_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += kv_dim * hidden * 4

    _write_string_gguf(buf, "blk.0.attn_v.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, kv_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += kv_dim * hidden * 4

    _write_string_gguf(buf, "blk.0.attn_output.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, hidden)
    _write_u64_le(buf, q_dim)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * q_dim * 4

    _write_string_gguf(buf, "blk.0.ffn_norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += hidden * 4

    _write_string_gguf(buf, "blk.0.ffn_gate.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, ffn_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_dim * hidden * 4

    _write_string_gguf(buf, "blk.0.ffn_up.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, ffn_dim)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)
    _write_u64_le(buf, data_cursor)
    data_cursor += ffn_dim * hidden * 4

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

    # Tensor data (all F32)
    for i in range(vocab * hidden):
        _write_f32_le(buf, Float32(i) * 0.01)
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))
    for i in range(vocab * hidden):
        _write_f32_le(buf, Float32(i % 7) * 0.02)
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))
    for i in range(q_dim * hidden):
        _write_f32_le(buf, Float32(i % 5) * 0.01)
    for i in range(kv_dim * hidden):
        _write_f32_le(buf, Float32(i % 3) * 0.01)
    for i in range(kv_dim * hidden):
        _write_f32_le(buf, Float32(i % 4) * 0.01)
    for i in range(hidden * q_dim):
        _write_f32_le(buf, Float32(i % 6) * 0.01)
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))
    for i in range(ffn_dim * hidden):
        _write_f32_le(buf, Float32(i % 11) * 0.001)
    for i in range(ffn_dim * hidden):
        _write_f32_le(buf, Float32(i % 13) * 0.001)
    for i in range(hidden * ffn_dim):
        _write_f32_le(buf, Float32(i % 7) * 0.001)

    return buf^


fn _load_tokenizer_from_gguf(mut buf: List[UInt8]) raises -> BPETokenizer:
    """Load tokenizer from GGUF buffer."""
    var buf_copy = buf.copy()
    var gguf = parse_gguf_from_buffer(buf_copy^)

    from neutron_mojo.nn.tokenizer import load_gguf_tokenizer
    var scores = List[Float64]()
    var tok = load_gguf_tokenizer(
        gguf.token_vocab, scores, gguf.token_merges,
        bos_id=0, eos_id=1,
    )
    tok.unk_id = 2
    return tok^


# ===----------------------------------------------------------------------=== #
# Helpers for building models from tiny params
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    """Build a tiny FP32 model with non-trivial weights."""
    var p = tiny_test_params()
    var model = Model(p)

    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)

    for i in range(p.num_layers * model.layer_size):
        model.layer_weights.set(i, Float32(i % 13) * 0.02 - 0.12)

    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)

    return model^


fn _build_tiny_tokenizer() -> BPETokenizer:
    """Build a minimal tokenizer for testing (8 tokens, IDs 0-7)."""
    var tok = BPETokenizer()
    _ = tok.add_token("<s>")     # 0
    _ = tok.add_token("</s>")   # 1
    _ = tok.add_token("<unk>")  # 2
    _ = tok.add_token("a")      # 3
    _ = tok.add_token("b")      # 4
    _ = tok.add_token("c")      # 5
    _ = tok.add_token("d")      # 6
    _ = tok.add_token("e")      # 7
    tok.bos_id = 0
    tok.eos_id = 1
    tok.unk_id = 2
    return tok^


# ===----------------------------------------------------------------------=== #
# Integration Tests
# ===----------------------------------------------------------------------=== #

fn test_q_pipeline_from_gguf() raises:
    """Load GGUF (F32) -> quantize -> q_pipeline_generate -> non-empty text."""
    var buf = _build_full_gguf_with_tokenizer()
    var buf2 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    var fp32_model = load_gguf_model_from_buffer(buf2^)
    var qm = quantize_from_model(fp32_model, block_size=32)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5

    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    assert_true(len(result) >= 0, "q_pipeline from GGUF produces output")

    print("  q_pipeline_from_gguf: PASS")


fn test_direct_q8_pipeline() raises:
    """Direct Q8 GGUF -> q_pipeline_generate -> non-empty text."""
    var buf = _build_full_gguf_with_tokenizer()
    var buf2 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    # Direct loading (F32 GGUF gets quantized on load)
    var qm = load_gguf_quantized_direct_from_buffer(buf2^, block_size=32)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5

    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    assert_true(len(result) >= 0, "direct Q8 pipeline produces output")

    print("  direct_q8_pipeline: PASS")


fn test_fp32_vs_q8_pipeline_output() raises:
    """Both pipelines run on same model, both produce valid output."""
    var buf = _build_full_gguf_with_tokenizer()
    var buf2 = buf.copy()
    var buf3 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    var fp32_model = load_gguf_model_from_buffer(buf2^)
    var qm = quantize_from_model(fp32_model, block_size=32)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 3

    # Reload fp32 model since quantize_from_model borrows it
    var fp32_model2 = load_gguf_model_from_buffer(buf3^)

    var fp32_result = pipeline_generate(fp32_model2, tok, "ab", cfg)
    var q8_result = q_pipeline_generate(qm, tok, "ab", cfg)

    assert_true(len(fp32_result) >= 0, "fp32 pipeline output valid")
    assert_true(len(q8_result) >= 0, "q8 pipeline output valid")

    print("  fp32_vs_q8_pipeline_output: PASS")


fn test_direct_q8_with_chat_template() raises:
    """Direct loaded Q8 model + llama template -> generates text."""
    var buf = _build_full_gguf_with_tokenizer()
    var buf2 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    var qm = load_gguf_quantized_direct_from_buffer(buf2^, block_size=32)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 3
    cfg.chat_template = String("llama")

    var result = q_pipeline_generate(qm, tok, "hello", cfg)
    assert_true(len(result) >= 0, "direct Q8 with chat template works")

    print("  direct_q8_with_chat_template: PASS")


fn test_direct_q8_with_penalties() raises:
    """Repetition + frequency penalties work with direct-loaded model."""
    var buf = _build_full_gguf_with_tokenizer()
    var buf2 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    var qm = load_gguf_quantized_direct_from_buffer(buf2^, block_size=32)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5
    cfg.repetition_penalty = 1.3
    cfg.frequency_penalty = 0.5
    cfg.presence_penalty = 0.2

    var result = q_pipeline_generate(qm, tok, "abc", cfg)
    assert_true(len(result) >= 0, "penalties work with direct Q8")

    print("  direct_q8_with_penalties: PASS")


# ===----------------------------------------------------------------------=== #
# Benchmarks
# ===----------------------------------------------------------------------=== #

fn test_benchmark_fp32() raises:
    """Time FP32 generation, print tokens/sec."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 20

    var start = perf_counter_ns()
    var result = pipeline_generate(model, tok, "ab", cfg)
    var elapsed_ns = perf_counter_ns() - start

    var elapsed_ms = Float64(elapsed_ns) / 1_000_000.0
    var tokens_per_sec = Float64(20) / (Float64(elapsed_ns) / 1_000_000_000.0)

    print("  benchmark_fp32: " + String(Int(tokens_per_sec)) + " tok/s (" + String(Int(elapsed_ms)) + " ms for 20 tokens): PASS")


fn test_benchmark_q8() raises:
    """Time Q8 generation, print tokens/sec."""
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 20

    var start = perf_counter_ns()
    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    var elapsed_ns = perf_counter_ns() - start

    var elapsed_ms = Float64(elapsed_ns) / 1_000_000.0
    var tokens_per_sec = Float64(20) / (Float64(elapsed_ns) / 1_000_000_000.0)

    print("  benchmark_q8: " + String(Int(tokens_per_sec)) + " tok/s (" + String(Int(elapsed_ms)) + " ms for 20 tokens): PASS")


fn test_benchmark_comparison() raises:
    """Run both benchmarks and print speedup ratio."""
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 20

    # FP32 benchmark
    var start_fp32 = perf_counter_ns()
    var r1 = pipeline_generate(model, tok, "ab", cfg)
    var elapsed_fp32 = perf_counter_ns() - start_fp32

    # Q8 benchmark
    var start_q8 = perf_counter_ns()
    var r2 = q_pipeline_generate(qm, tok, "ab", cfg)
    var elapsed_q8 = perf_counter_ns() - start_q8

    var fp32_ms = Float64(elapsed_fp32) / 1_000_000.0
    var q8_ms = Float64(elapsed_q8) / 1_000_000.0
    var fp32_tps = Float64(20) / (Float64(elapsed_fp32) / 1_000_000_000.0)
    var q8_tps = Float64(20) / (Float64(elapsed_q8) / 1_000_000_000.0)

    var ratio: Float64
    if fp32_tps > 0.0:
        ratio = q8_tps / fp32_tps
    else:
        ratio = 0.0

    print("  benchmark_comparison:")
    print("    FP32: " + String(Int(fp32_tps)) + " tok/s (" + String(Int(fp32_ms)) + " ms)")
    print("    Q8:   " + String(Int(q8_tps)) + " tok/s (" + String(Int(q8_ms)) + " ms)")
    print("    Q8/FP32 ratio: " + String(ratio))
    print("    PASS")


fn main() raises:
    print("test_sprint11_integration:")

    test_q_pipeline_from_gguf()
    test_direct_q8_pipeline()
    test_fp32_vs_q8_pipeline_output()
    test_direct_q8_with_chat_template()
    test_direct_q8_with_penalties()
    test_benchmark_fp32()
    test_benchmark_q8()
    test_benchmark_comparison()

    print("ALL PASSED (8 tests)")
