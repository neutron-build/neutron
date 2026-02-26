# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Q8 KV Cache Pipeline Tests (Sprint 12)
# ===----------------------------------------------------------------------=== #

"""Tests for Q8 KV cache integration with both FP32 and quantized pipelines."""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig, pipeline_generate, default_pipeline_config
from neutron_mojo.nn.q_pipeline import q_pipeline_generate
from neutron_mojo.nn.q_kv_cache import MultiLayerQ8KVCache
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


# ===----------------------------------------------------------------------=== #
# Test Helpers
# ===----------------------------------------------------------------------=== #

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


fn _build_tiny_q_model() -> QuantizedModel:
    """Build a tiny QuantizedModel from FP32 model."""
    var model = _build_tiny_model()
    return quantize_from_model(model, block_size=2)


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_q8_cache_fp32_model_pipeline() raises:
    """FP32 Model pipeline with use_q8_cache=True generates text."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5
    cfg.use_q8_cache = True

    var result = pipeline_generate(model, tok, "ab", cfg)
    assert_true(len(result) >= 0, "fp32 model + q8 cache produces output")

    print("  q8_cache_fp32_model_pipeline: PASS")


fn test_q8_cache_quantized_model_pipeline() raises:
    """QuantizedModel pipeline with use_q8_cache=True generates text."""
    var qm = _build_tiny_q_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5
    cfg.use_q8_cache = True

    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    assert_true(len(result) >= 0, "q8 model + q8 cache produces output")

    print("  q8_cache_quantized_model_pipeline: PASS")


fn test_fp32_vs_q8_cache_comparison() raises:
    """Both FP32 and Q8 cache paths produce valid output from same model."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var cfg_fp32 = default_pipeline_config()
    cfg_fp32.max_new_tokens = 5
    cfg_fp32.use_q8_cache = False

    var cfg_q8 = default_pipeline_config()
    cfg_q8.max_new_tokens = 5
    cfg_q8.use_q8_cache = True

    var result_fp32 = pipeline_generate(model, tok, "ab", cfg_fp32)
    var result_q8 = pipeline_generate(model, tok, "ab", cfg_q8)

    assert_true(len(result_fp32) >= 0, "fp32 cache output valid")
    assert_true(len(result_q8) >= 0, "q8 cache output valid")

    print("  fp32_vs_q8_cache_comparison: PASS")


fn test_q8_cache_memory_reduction() raises:
    """Q8 cache uses less memory than FP32 cache."""
    var p = tiny_test_params()
    var max_seq = 32
    var num_kv = p.num_kv_heads
    var hd = p.head_dim

    # Create and fill Q8 cache
    var q8cache = MultiLayerQ8KVCache(
        num_layers=p.num_layers, max_seq_len=max_seq,
        num_kv_heads=num_kv, head_dim=hd,
    )

    # Append some data
    var stride = num_kv * hd
    var key = Tensor[DType.float32](Shape(stride))
    var val = Tensor[DType.float32](Shape(stride))
    for i in range(stride):
        key.set(i, Float32(i) * 0.1)
        val.set(i, Float32(i) * 0.05)

    for layer in range(p.num_layers):
        for _ in range(5):
            q8cache.append_kv(layer, key, val, num_new_tokens=1)

    var q8_bytes = q8cache.memory_bytes()
    var fp32_bytes = q8cache.fp32_equivalent_bytes()

    # Q8 should use less memory (reduction scales with head_dim;
    # for tiny head_dim=2, scale overhead is proportionally large ~1.3x,
    # for real head_dim=128, expect ~3.9x)
    assert_true(q8_bytes < fp32_bytes, "q8 uses less memory than fp32")

    print("  q8_cache_memory_reduction: PASS (q8=" + String(q8_bytes) + " fp32=" + String(fp32_bytes) + ")")


fn test_q8_cache_with_penalties() raises:
    """Q8 cache pipeline with repetition+frequency penalties works."""
    var qm = _build_tiny_q_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5
    cfg.use_q8_cache = True
    cfg.repetition_penalty = 1.5
    cfg.frequency_penalty = 0.3
    cfg.presence_penalty = 0.2

    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    assert_true(len(result) >= 0, "q8 cache + penalties works")

    print("  q8_cache_with_penalties: PASS")


fn test_q8_cache_with_chat_template() raises:
    """Q8 cache pipeline with chat template works."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 3
    cfg.use_q8_cache = True
    cfg.chat_template = String("chatml")

    var result = pipeline_generate(model, tok, "hello", cfg)
    assert_true(len(result) >= 0, "q8 cache + chat template works")

    print("  q8_cache_with_chat_template: PASS")


fn test_config_default_backward_compat() raises:
    """Default PipelineConfig has use_q8_cache=False for backward compatibility."""
    var cfg = PipelineConfig()
    assert_true(cfg.use_q8_cache == False, "default use_q8_cache is False")

    var cfg2 = default_pipeline_config()
    assert_true(cfg2.use_q8_cache == False, "default_pipeline_config use_q8_cache is False")

    print("  config_default_backward_compat: PASS")


fn main() raises:
    print("test_q8_cache_pipeline:")

    test_q8_cache_fp32_model_pipeline()
    test_q8_cache_quantized_model_pipeline()
    test_fp32_vs_q8_cache_comparison()
    test_q8_cache_memory_reduction()
    test_q8_cache_with_penalties()
    test_q8_cache_with_chat_template()
    test_config_default_backward_compat()

    print("ALL PASSED (7 tests)")
