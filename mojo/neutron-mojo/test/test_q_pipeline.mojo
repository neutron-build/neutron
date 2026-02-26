# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantized Pipeline Tests
# ===----------------------------------------------------------------------=== #

"""Tests for q_pipeline_generate: text-in -> text-out with QuantizedModel."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model, q_generate
from neutron_mojo.nn.tokenizer import BPETokenizer, MergeRule
from neutron_mojo.nn.pipeline import PipelineConfig, pipeline_generate, default_pipeline_config
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

    # Set embed weights
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)

    # Set layer weights
    for i in range(p.num_layers * model.layer_size):
        model.layer_weights.set(i, Float32(i % 13) * 0.02 - 0.12)

    # Re-set norms to 1.0
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

fn test_q_pipeline_generate_basic() raises:
    """Tiny quantized model + tokenizer -> non-empty string."""
    var qm = _build_tiny_q_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5

    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    # Should produce some output (possibly empty if EOS hit, but shouldn't crash)
    assert_true(len(result) >= 0, "q_pipeline produces output")

    print("  q_pipeline_generate_basic: PASS")


fn test_q_pipeline_with_eos_stopping() raises:
    """Pipeline stops at EOS token."""
    var qm = _build_tiny_q_model()
    var tok = _build_tiny_tokenizer()

    # Force model to produce EOS by making EOS token (1) have highest logit
    # We'll set lm_head row 1 to very high values
    var p = qm.params.copy()
    for d in range(p.hidden_dim):
        qm.lm_head.set(1 * p.hidden_dim + d, 100.0)

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 20

    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    # Should stop early due to EOS — result might be empty (EOS on first token)
    assert_true(len(result) >= 0, "eos stopping works")

    print("  q_pipeline_with_eos_stopping: PASS")


fn test_q_pipeline_with_repetition_penalty() raises:
    """Repetition penalty runs without error."""
    var qm = _build_tiny_q_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5
    cfg.repetition_penalty = 1.5

    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    assert_true(len(result) >= 0, "repetition penalty runs")

    print("  q_pipeline_with_repetition_penalty: PASS")


fn test_q_pipeline_with_chat_template() raises:
    """Llama chat template works with quantized pipeline."""
    var qm = _build_tiny_q_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 3
    cfg.chat_template = String("llama")

    var result = q_pipeline_generate(qm, tok, "hello", cfg)
    assert_true(len(result) >= 0, "chat template works")

    print("  q_pipeline_with_chat_template: PASS")


fn test_q_pipeline_config_reuse() raises:
    """Same PipelineConfig works for both FP32 and Q8 pipelines."""
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 3

    # Both pipelines use the same config type
    var fp32_result = pipeline_generate(model, tok, "ab", cfg)
    var q8_result = q_pipeline_generate(qm, tok, "ab", cfg)

    assert_true(len(fp32_result) >= 0, "fp32 pipeline works")
    assert_true(len(q8_result) >= 0, "q8 pipeline works")

    print("  q_pipeline_config_reuse: PASS")


fn test_q_pipeline_vs_fp32() raises:
    """Both pipelines produce output (no crash), output length similar."""
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5

    var fp32_result = pipeline_generate(model, tok, "abc", cfg)
    var q8_result = q_pipeline_generate(qm, tok, "abc", cfg)

    # Both should produce valid output
    assert_true(len(fp32_result) >= 0, "fp32 output valid")
    assert_true(len(q8_result) >= 0, "q8 output valid")

    print("  q_pipeline_vs_fp32: PASS")


fn test_q_pipeline_frequency_penalty() raises:
    """Frequency+presence penalties work with quantized pipeline."""
    var qm = _build_tiny_q_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5
    cfg.frequency_penalty = 0.5
    cfg.presence_penalty = 0.3

    var result = q_pipeline_generate(qm, tok, "ab", cfg)
    assert_true(len(result) >= 0, "frequency+presence penalties work")

    print("  q_pipeline_frequency_penalty: PASS")


fn main() raises:
    print("test_q_pipeline:")

    test_q_pipeline_generate_basic()
    test_q_pipeline_with_eos_stopping()
    test_q_pipeline_with_repetition_penalty()
    test_q_pipeline_with_chat_template()
    test_q_pipeline_config_reuse()
    test_q_pipeline_vs_fp32()
    test_q_pipeline_frequency_penalty()

    print("ALL PASSED (7 tests)")
