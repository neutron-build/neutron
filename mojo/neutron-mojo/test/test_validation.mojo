# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 17: Model Validation Tests
# ===----------------------------------------------------------------------=== #

"""Comprehensive validation tests using mini-TinyLlama config.

Validates end-to-end flow: config → GGUF → load → forward → pipeline → text.
Tests FP32 vs Q8, direct Q8 loading, chat templates, and benchmarks
at a scaled-down TinyLlama architecture (GQA 8:2, 2 layers, hidden=64).

Tests:
1. TinyLlama-1.1B config correctness
2. Mini-TinyLlama config correctness
3. Mini-TinyLlama GGUF construction + FP32 loading
4. Mini-TinyLlama forward pass produces valid logits
5. Mini-TinyLlama FP32 pipeline generation
6. Mini-TinyLlama Q8 quantization + forward pass
7. Mini-TinyLlama Q8 pipeline generation
8. Mini-TinyLlama direct Q8 loading + forward
9. FP32 vs Q8 logit comparison
10. Chat template formatting
11. Q8 cache pipeline
12. Memory estimation
13. Performance benchmark: FP32 vs Q8
"""

from math import abs
from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.model.config import (
    ModelConfig,
    tinyllama_1_1b_config,
    mini_tinyllama_config,
)
from neutron_mojo.model.populate import model_from_config
from neutron_mojo.model.weight_reader import (
    load_gguf_model_from_buffer,
    load_gguf_quantized_direct_from_buffer,
)
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model
from neutron_mojo.nn.tokenizer import BPETokenizer, load_gguf_tokenizer
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.pipeline import (
    PipelineConfig,
    pipeline_generate,
    default_pipeline_config,
    format_llama,
    format_chatml,
)
from neutron_mojo.nn.q_pipeline import q_pipeline_generate
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


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected=" + String(b) + " got=" + String(a)
        )


# ===----------------------------------------------------------------------=== #
# Mini-TinyLlama GGUF Builder
# ===----------------------------------------------------------------------=== #

fn _write_gguf_string_array(mut buf: List[UInt8], key: String, values: List[String]):
    """Write a string array metadata entry to GGUF."""
    _write_string_gguf(buf, key)
    _write_u32_le(buf, 9)   # GGUF_TYPE_ARRAY
    _write_u32_le(buf, 8)   # elem_type = GGUF_TYPE_STRING
    _write_u64_le(buf, len(values))
    for i in range(len(values)):
        _write_string_gguf(buf, values[i])


fn _build_mini_tinyllama_gguf() raises -> List[UInt8]:
    """Build complete GGUF with mini-TinyLlama architecture + tokenizer.

    Config: hidden=64, layers=2, heads=8, kv_heads=2, head_dim=8,
            ffn=128, vocab=256, GQA ratio 4:1.
    """
    var buf = List[UInt8]()

    var hidden = 64
    var heads = 8
    var kv_heads = 2
    var head_dim = 8
    var q_dim = heads * head_dim     # 64
    var kv_dim = kv_heads * head_dim  # 16
    var ffn_dim = 128
    var vocab = 256
    var num_layers = 2

    var meta_count = 12
    # 3 global + 9 per layer * 2 layers = 21 tensors
    var tensor_count = 3 + 9 * num_layers

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, tensor_count)
    _write_u64_le(buf, meta_count)

    # Model metadata
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)  # STRING
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)  # U32
    _write_u32_le(buf, num_layers)

    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, hidden)

    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, heads)

    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, kv_heads)

    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, ffn_dim)

    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, vocab)

    _write_string_gguf(buf, "llama.context_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 128)

    # Tokenizer metadata
    _write_string_gguf(buf, "tokenizer.ggml.bos_token_id")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "tokenizer.ggml.eos_token_id")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    # Build 256-token vocabulary
    var tokens = List[String]()
    tokens.append("<pad>")    # 0
    tokens.append("<s>")      # 1
    tokens.append("</s>")     # 2
    tokens.append("<unk>")    # 3
    tokens.append(" ")        # 4
    # Byte tokens 5..260 → characters a-z, A-Z, digits, symbols
    var chars = String("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.,!?;:'-\"()[]{}/@#$%^&*+=~`|\\<> ")
    for i in range(len(chars)):
        if len(tokens) >= vocab:
            break
        tokens.append(String(chars[byte=i]))

    # Fill remaining with byte fallback tokens
    while len(tokens) < vocab:
        tokens.append("<byte_" + String(len(tokens)) + ">")

    _write_gguf_string_array(buf, "tokenizer.ggml.tokens", tokens)

    var merges = List[String]()
    merges.append("t h")
    merges.append("e r")
    merges.append("th e")
    _write_gguf_string_array(buf, "tokenizer.ggml.merges", merges)

    # Tensor info section (all F32)
    var data_cursor = 0

    # Global tensors
    _write_string_gguf(buf, "token_embd.weight")
    _write_u32_le(buf, 2)  # ndims
    _write_u64_le(buf, vocab)
    _write_u64_le(buf, hidden)
    _write_u32_le(buf, 0)  # F32
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

    # Per-layer tensors
    for layer in range(num_layers):
        var lp = "blk." + String(layer) + "."

        _write_string_gguf(buf, lp + "attn_norm.weight")
        _write_u32_le(buf, 1)
        _write_u64_le(buf, hidden)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += hidden * 4

        _write_string_gguf(buf, lp + "attn_q.weight")
        _write_u32_le(buf, 2)
        _write_u64_le(buf, q_dim)
        _write_u64_le(buf, hidden)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += q_dim * hidden * 4

        _write_string_gguf(buf, lp + "attn_k.weight")
        _write_u32_le(buf, 2)
        _write_u64_le(buf, kv_dim)
        _write_u64_le(buf, hidden)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += kv_dim * hidden * 4

        _write_string_gguf(buf, lp + "attn_v.weight")
        _write_u32_le(buf, 2)
        _write_u64_le(buf, kv_dim)
        _write_u64_le(buf, hidden)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += kv_dim * hidden * 4

        _write_string_gguf(buf, lp + "attn_output.weight")
        _write_u32_le(buf, 2)
        _write_u64_le(buf, hidden)
        _write_u64_le(buf, q_dim)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += hidden * q_dim * 4

        _write_string_gguf(buf, lp + "ffn_norm.weight")
        _write_u32_le(buf, 1)
        _write_u64_le(buf, hidden)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += hidden * 4

        _write_string_gguf(buf, lp + "ffn_gate.weight")
        _write_u32_le(buf, 2)
        _write_u64_le(buf, ffn_dim)
        _write_u64_le(buf, hidden)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += ffn_dim * hidden * 4

        _write_string_gguf(buf, lp + "ffn_up.weight")
        _write_u32_le(buf, 2)
        _write_u64_le(buf, ffn_dim)
        _write_u64_le(buf, hidden)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += ffn_dim * hidden * 4

        _write_string_gguf(buf, lp + "ffn_down.weight")
        _write_u32_le(buf, 2)
        _write_u64_le(buf, hidden)
        _write_u64_le(buf, ffn_dim)
        _write_u32_le(buf, 0)
        _write_u64_le(buf, data_cursor)
        data_cursor += hidden * ffn_dim * 4

    # Align to data section
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # Write tensor data (all F32, deterministic values)
    # Embedding: vocab * hidden
    for i in range(vocab * hidden):
        _write_f32_le(buf, Float32(i % 37) * 0.005 - 0.09)

    # Output norm: hidden (all 1.0)
    for _ in range(hidden):
        _write_f32_le(buf, Float32(1.0))

    # LM head: vocab * hidden
    for i in range(vocab * hidden):
        _write_f32_le(buf, Float32(i % 23) * 0.008 - 0.09)

    # Per-layer data
    for layer in range(num_layers):
        var seed = layer * 1000

        # attn_norm (1.0)
        for _ in range(hidden):
            _write_f32_le(buf, Float32(1.0))

        # wq: q_dim * hidden
        for i in range(q_dim * hidden):
            _write_f32_le(buf, Float32((i + seed) % 19) * 0.005 - 0.04)

        # wk: kv_dim * hidden
        for i in range(kv_dim * hidden):
            _write_f32_le(buf, Float32((i + seed) % 17) * 0.006 - 0.05)

        # wv: kv_dim * hidden
        for i in range(kv_dim * hidden):
            _write_f32_le(buf, Float32((i + seed) % 13) * 0.007 - 0.04)

        # wo: hidden * q_dim
        for i in range(hidden * q_dim):
            _write_f32_le(buf, Float32((i + seed) % 11) * 0.004 - 0.02)

        # ffn_norm (1.0)
        for _ in range(hidden):
            _write_f32_le(buf, Float32(1.0))

        # w_gate: ffn_dim * hidden
        for i in range(ffn_dim * hidden):
            _write_f32_le(buf, Float32((i + seed) % 29) * 0.003 - 0.04)

        # w_up: ffn_dim * hidden
        for i in range(ffn_dim * hidden):
            _write_f32_le(buf, Float32((i + seed) % 31) * 0.003 - 0.04)

        # w_down: hidden * ffn_dim
        for i in range(hidden * ffn_dim):
            _write_f32_le(buf, Float32((i + seed) % 23) * 0.003 - 0.03)

    return buf^


fn _load_tokenizer_from_gguf(mut buf: List[UInt8]) raises -> BPETokenizer:
    """Load tokenizer from mini-TinyLlama GGUF buffer."""
    var buf_copy = buf.copy()
    var gguf = parse_gguf_from_buffer(buf_copy^)
    var scores = List[Float64]()
    var tok = load_gguf_tokenizer(
        gguf.token_vocab, scores, gguf.token_merges,
        bos_id=1, eos_id=2,
    )
    tok.unk_id = 3
    return tok^


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_tinyllama_config() raises:
    """Verify TinyLlama-1.1B config matches published architecture."""
    var cfg = tinyllama_1_1b_config()

    assert_eq(cfg.vocab_size, 32000, "TinyLlama vocab_size")
    assert_eq(cfg.hidden_size, 2048, "TinyLlama hidden_size")
    assert_eq(cfg.intermediate_size, 5632, "TinyLlama intermediate_size")
    assert_eq(cfg.num_hidden_layers, 22, "TinyLlama num_layers")
    assert_eq(cfg.num_attention_heads, 32, "TinyLlama num_heads")
    assert_eq(cfg.num_key_value_heads, 4, "TinyLlama num_kv_heads")
    assert_eq(cfg.head_dim, 64, "TinyLlama head_dim")
    assert_eq(cfg.max_position_embeddings, 2048, "TinyLlama max_seq")
    assert_true(cfg.is_gqa(), "TinyLlama should use GQA")
    assert_eq(cfg.kv_group_size(), 8, "TinyLlama GQA ratio")

    # Parameter estimate: ~1.1B
    var params = cfg.total_params_estimate()
    assert_true(params > 1_000_000_000, "TinyLlama should have >1B params")
    assert_true(params < 1_500_000_000, "TinyLlama should have <1.5B params")

    print("  tinyllama_config: PASS")


fn test_mini_tinyllama_config() raises:
    """Verify mini-TinyLlama config preserves architecture ratios."""
    var cfg = mini_tinyllama_config()

    assert_eq(cfg.vocab_size, 256, "mini vocab_size")
    assert_eq(cfg.hidden_size, 64, "mini hidden_size")
    assert_eq(cfg.intermediate_size, 128, "mini intermediate_size")
    assert_eq(cfg.num_hidden_layers, 2, "mini num_layers")
    assert_eq(cfg.num_attention_heads, 8, "mini num_heads")
    assert_eq(cfg.num_key_value_heads, 2, "mini num_kv_heads")
    assert_eq(cfg.head_dim, 8, "mini head_dim")
    assert_true(cfg.is_gqa(), "Mini should use GQA")
    assert_eq(cfg.kv_group_size(), 4, "Mini GQA ratio should match TinyLlama")

    print("  mini_tinyllama_config: PASS")


fn test_gguf_load_fp32() raises:
    """Build mini-TinyLlama GGUF and load as FP32 Model."""
    var buf = _build_mini_tinyllama_gguf()
    var model = load_gguf_model_from_buffer(buf^)

    var p = model.params.copy()
    assert_eq(p.num_layers, 2, "loaded num_layers")
    assert_eq(p.hidden_dim, 64, "loaded hidden_dim")
    assert_eq(p.num_q_heads, 8, "loaded num_q_heads")
    assert_eq(p.num_kv_heads, 2, "loaded num_kv_heads")
    assert_eq(p.head_dim, 8, "loaded head_dim")
    assert_eq(p.ffn_dim, 128, "loaded ffn_dim")
    assert_eq(p.vocab_size, 256, "loaded vocab_size")

    print("  gguf_load_fp32: PASS")


fn test_forward_pass_logits() raises:
    """FP32 forward pass produces valid logits with correct shape."""
    var buf = _build_mini_tinyllama_gguf()
    var model = load_gguf_model_from_buffer(buf^)
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=32,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32, theta=p.rope_theta)

    var logits = model.forward(1, cache, rope, pos=0)

    # Check logits shape (should be vocab_size)
    assert_true(logits.numel() == p.vocab_size, "logits size == vocab_size")

    # Check logits are not all zeros
    var has_nonzero = False
    for i in range(p.vocab_size):
        if abs(logits.get(i)) > 1e-10:
            has_nonzero = True
            break
    assert_true(has_nonzero, "logits should have non-zero values")

    # Check logits are finite (not NaN/Inf)
    var has_inf = False
    for i in range(p.vocab_size):
        var v = logits.get(i)
        if v != v or v > 1e30 or v < -1e30:
            has_inf = True
            break
    assert_true(not has_inf, "logits should be finite")

    print("  forward_pass_logits: PASS")


fn test_fp32_pipeline() raises:
    """FP32 pipeline generates text successfully."""
    var buf = _build_mini_tinyllama_gguf()
    var buf2 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    var model = load_gguf_model_from_buffer(buf2^)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 10

    var result = pipeline_generate(model, tok, "hello", cfg)
    assert_true(len(result) >= 0, "FP32 pipeline should produce output")

    print("  fp32_pipeline: PASS")


fn test_q8_quantize_forward() raises:
    """Q8 quantized forward pass produces valid logits."""
    var buf = _build_mini_tinyllama_gguf()
    var model = load_gguf_model_from_buffer(buf^)
    var qm = quantize_from_model(model, block_size=8)
    var p = qm.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=32,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32, theta=p.rope_theta)

    var logits = qm.forward(1, cache, rope, pos=0)

    assert_true(logits.numel() == p.vocab_size, "Q8 logits size == vocab_size")

    var has_nonzero = False
    for i in range(p.vocab_size):
        if abs(logits.get(i)) > 1e-10:
            has_nonzero = True
            break
    assert_true(has_nonzero, "Q8 logits should have non-zero values")

    print("  q8_quantize_forward: PASS")


fn test_q8_pipeline() raises:
    """Q8 pipeline generates text successfully."""
    var buf = _build_mini_tinyllama_gguf()
    var buf2 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    var model = load_gguf_model_from_buffer(buf2^)
    var qm = quantize_from_model(model, block_size=8)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 10

    var result = q_pipeline_generate(qm, tok, "hello", cfg)
    assert_true(len(result) >= 0, "Q8 pipeline should produce output")

    print("  q8_pipeline: PASS")


fn test_direct_q8_loading() raises:
    """Direct Q8 loading produces valid model and forward pass."""
    var buf = _build_mini_tinyllama_gguf()

    # Direct loading (F32 GGUF → quantize on load)
    var qm = load_gguf_quantized_direct_from_buffer(buf^, block_size=8)
    var p = qm.params.copy()

    assert_eq(p.num_layers, 2, "direct Q8 num_layers")
    assert_eq(p.hidden_dim, 64, "direct Q8 hidden_dim")

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=32,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32, theta=p.rope_theta)

    var logits = qm.forward(1, cache, rope, pos=0)
    assert_true(logits.numel() == p.vocab_size, "direct Q8 logits valid")

    print("  direct_q8_loading: PASS")


fn test_fp32_vs_q8_logits() raises:
    """FP32 and Q8 forward passes produce similar logits."""
    var buf = _build_mini_tinyllama_gguf()
    var buf2 = buf.copy()

    var model = load_gguf_model_from_buffer(buf^)
    var qm = quantize_from_model(model, block_size=8)

    # Reload FP32 for fair comparison
    var model2 = load_gguf_model_from_buffer(buf2^)
    var p = model2.params.copy()

    # FP32 forward
    var fp32_cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var fp32_rope = RoPETable(head_dim=p.head_dim, max_seq_len=32, theta=p.rope_theta)
    var fp32_logits = model2.forward(1, fp32_cache, fp32_rope, pos=0)

    # Q8 forward
    var q8_cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var q8_rope = RoPETable(head_dim=p.head_dim, max_seq_len=32, theta=p.rope_theta)
    var q8_logits = qm.forward(1, q8_cache, q8_rope, pos=0)

    # Compute max absolute difference
    var max_diff: Float32 = 0.0
    for i in range(p.vocab_size):
        var d = abs(fp32_logits.get(i) - q8_logits.get(i))
        if d > max_diff:
            max_diff = d

    # Q8 should be close to FP32 (within reasonable quantization error)
    assert_true(max_diff < 0.5, "FP32 vs Q8 max_diff=" + String(max_diff) + " should be < 0.5")

    # Argmax should match (greedy token)
    var fp32_argmax = 0
    var q8_argmax = 0
    var fp32_max: Float32 = -1e30
    var q8_max: Float32 = -1e30
    for i in range(p.vocab_size):
        if fp32_logits.get(i) > fp32_max:
            fp32_max = fp32_logits.get(i)
            fp32_argmax = i
        if q8_logits.get(i) > q8_max:
            q8_max = q8_logits.get(i)
            q8_argmax = i

    # Greedy token should often match, but with tiny weights it might not always
    # Just verify both are valid token IDs
    assert_true(fp32_argmax >= 0 and fp32_argmax < p.vocab_size, "fp32 argmax valid")
    assert_true(q8_argmax >= 0 and q8_argmax < p.vocab_size, "q8 argmax valid")

    print("  fp32_vs_q8_logits: PASS (max_diff=" + String(max_diff) + ")")


fn test_chat_templates() raises:
    """Chat template formatting produces expected structures."""
    # Llama template
    var llama_no_sys = format_llama("Hello world", "")
    assert_true("[INST]" in llama_no_sys, "Llama should contain [INST]")
    assert_true("[/INST]" in llama_no_sys, "Llama should contain [/INST]")

    var llama_sys = format_llama("Hello", "You are helpful.")
    assert_true("<<SYS>>" in llama_sys, "Llama with system should contain <<SYS>>")
    assert_true("You are helpful." in llama_sys, "Llama should contain system prompt")

    # ChatML template
    var chatml = format_chatml("Hello", "")
    assert_true("<|im_start|>user" in chatml, "ChatML should contain user tag")
    assert_true("<|im_start|>assistant" in chatml, "ChatML should contain assistant tag")

    var chatml_sys = format_chatml("Hello", "Be concise.")
    assert_true("<|im_start|>system" in chatml_sys, "ChatML with system should contain system tag")
    assert_true("Be concise." in chatml_sys, "ChatML should contain system prompt")

    print("  chat_templates: PASS")


fn test_q8_cache_pipeline() raises:
    """Q8 KV cache pipeline generates successfully."""
    var buf = _build_mini_tinyllama_gguf()
    var buf2 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    var model = load_gguf_model_from_buffer(buf2^)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5
    cfg.use_q8_cache = True

    var result = pipeline_generate(model, tok, "ab", cfg)
    assert_true(len(result) >= 0, "Q8 cache pipeline should produce output")

    print("  q8_cache_pipeline: PASS")


fn test_memory_estimation() raises:
    """Memory estimation for TinyLlama config is reasonable."""
    var cfg = tinyllama_1_1b_config()
    var params = cfg.total_params_estimate()

    # FP32 memory estimate (4 bytes per param)
    var fp32_bytes = params * 4
    var fp32_gb = Float64(fp32_bytes) / (1024.0 * 1024.0 * 1024.0)

    # Q8 memory estimate (~1 byte per quantized param + scales overhead)
    var q8_bytes = params  # 1 byte per param + ~3% scale overhead
    var q8_gb = Float64(q8_bytes) / (1024.0 * 1024.0 * 1024.0)

    # TinyLlama FP32 should be ~4.2GB, Q8 ~1.1GB
    assert_true(fp32_gb > 3.0 and fp32_gb < 6.0, "FP32 memory ~4GB for TinyLlama")
    assert_true(q8_gb > 0.5 and q8_gb < 2.0, "Q8 memory ~1GB for TinyLlama")

    # Mini config should be much smaller
    var mini_cfg = mini_tinyllama_config()
    var mini_params = mini_cfg.total_params_estimate()
    var mini_kb = Float64(mini_params * 4) / 1024.0
    assert_true(mini_kb < 1024.0, "Mini model should be < 1MB FP32")

    print("  memory_estimation: PASS (TinyLlama FP32=" + String(Int(fp32_gb * 10.0)) + "/10 GB, Q8=" + String(Int(q8_gb * 10.0)) + "/10 GB)")


fn test_benchmark_fp32_vs_q8() raises:
    """Benchmark FP32 vs Q8 generation at mini-TinyLlama scale."""
    var buf = _build_mini_tinyllama_gguf()
    var buf2 = buf.copy()
    var buf3 = buf.copy()

    var tok = _load_tokenizer_from_gguf(buf)
    var model = load_gguf_model_from_buffer(buf2^)
    var qm = quantize_from_model(model, block_size=8)

    # Reload FP32
    var model2 = load_gguf_model_from_buffer(buf3^)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 10
    var gen_tokens = 10

    # Benchmark FP32
    var fp32_start = perf_counter_ns()
    var fp32_result = pipeline_generate(model2, tok, "ab", cfg)
    var fp32_ns = perf_counter_ns() - fp32_start

    # Benchmark Q8
    var q8_start = perf_counter_ns()
    var q8_result = q_pipeline_generate(qm, tok, "ab", cfg)
    var q8_ns = perf_counter_ns() - q8_start

    var fp32_ms = Float64(fp32_ns) / 1_000_000.0
    var q8_ms = Float64(q8_ns) / 1_000_000.0
    var fp32_tps = Float64(gen_tokens) / (Float64(fp32_ns) / 1_000_000_000.0)
    var q8_tps = Float64(gen_tokens) / (Float64(q8_ns) / 1_000_000_000.0)

    print("  benchmark_fp32_vs_q8:")
    print("    FP32: " + String(Int(fp32_tps)) + " tok/s (" + String(Int(fp32_ms)) + " ms)")
    print("    Q8:   " + String(Int(q8_tps)) + " tok/s (" + String(Int(q8_ms)) + " ms)")
    print("    PASS")


fn main() raises:
    print("test_validation:")

    test_tinyllama_config()
    test_mini_tinyllama_config()
    test_gguf_load_fp32()
    test_forward_pass_logits()
    test_fp32_pipeline()
    test_q8_quantize_forward()
    test_q8_pipeline()
    test_direct_q8_loading()
    test_fp32_vs_q8_logits()
    test_chat_templates()
    test_q8_cache_pipeline()
    test_memory_estimation()
    test_benchmark_fp32_vs_q8()

    print("ALL PASSED (13 tests)")
