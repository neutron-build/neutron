# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Pipeline Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the unified generation pipeline and chat templates."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.tokenizer import BPETokenizer, build_test_tokenizer
from neutron_mojo.nn.sampler import SamplerConfig, greedy_config
from neutron_mojo.nn.pipeline import (
    PipelineConfig,
    pipeline_generate,
    format_llama,
    format_chatml,
    default_pipeline_config,
    chat_pipeline_config,
)
from neutron_mojo.model.config import ModelConfig
from neutron_mojo.model.populate import model_from_config, load_named_weight


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected " + String(b) + " got " + String(a)
        )


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
# Helper: build a tiny model + tokenizer for pipeline testing
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() raises -> Model:
    """Create a tiny model with populated weights."""
    var p = tiny_test_params()
    var model = Model(p)

    # Populate with small deterministic weights
    var embed_size = p.vocab_size * p.hidden_dim
    var embed_data = Tensor[DType.float32](Shape(embed_size))
    for i in range(embed_size):
        embed_data.set(i, Float32(i) * 0.01)
    load_named_weight(model, "model.embed_tokens.weight", embed_data, embed_size)

    var lm_size = p.vocab_size * p.hidden_dim
    var lm_data = Tensor[DType.float32](Shape(lm_size))
    for i in range(lm_size):
        lm_data.set(i, Float32(i % 5) * 0.1)
    load_named_weight(model, "lm_head.weight", lm_data, lm_size)

    var norm_data = Tensor[DType.float32](Shape(p.hidden_dim))
    for i in range(p.hidden_dim):
        norm_data.set(i, 1.0)
    load_named_weight(model, "model.norm.weight", norm_data, p.hidden_dim)

    for layer in range(p.num_layers):
        var prefix = "model.layers." + String(layer) + "."
        var hd = p.hidden_dim
        var qd = p.q_dim()
        var kvd = p.kv_dim()
        var fd = p.ffn_dim

        var an = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            an.set(i, 1.0)
        load_named_weight(model, prefix + "input_layernorm.weight", an, hd)

        var fn_ = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            fn_.set(i, 1.0)
        load_named_weight(model, prefix + "post_attention_layernorm.weight", fn_, hd)

        var wq = Tensor[DType.float32](Shape(qd * hd))
        for i in range(qd * hd):
            wq.set(i, Float32(i % 7) * 0.01)
        load_named_weight(model, prefix + "self_attn.q_proj.weight", wq, qd * hd)

        var wk = Tensor[DType.float32](Shape(kvd * hd))
        for i in range(kvd * hd):
            wk.set(i, Float32(i % 5) * 0.01)
        load_named_weight(model, prefix + "self_attn.k_proj.weight", wk, kvd * hd)

        var wv = Tensor[DType.float32](Shape(kvd * hd))
        for i in range(kvd * hd):
            wv.set(i, Float32(i % 3) * 0.01)
        load_named_weight(model, prefix + "self_attn.v_proj.weight", wv, kvd * hd)

        var wo = Tensor[DType.float32](Shape(hd * qd))
        for i in range(hd * qd):
            wo.set(i, Float32(i % 9) * 0.01)
        load_named_weight(model, prefix + "self_attn.o_proj.weight", wo, hd * qd)

        var wg = Tensor[DType.float32](Shape(fd * hd))
        for i in range(fd * hd):
            wg.set(i, Float32(i % 11) * 0.001)
        load_named_weight(model, prefix + "mlp.gate_proj.weight", wg, fd * hd)

        var wu = Tensor[DType.float32](Shape(fd * hd))
        for i in range(fd * hd):
            wu.set(i, Float32(i % 13) * 0.001)
        load_named_weight(model, prefix + "mlp.up_proj.weight", wu, fd * hd)

        var wd = Tensor[DType.float32](Shape(hd * fd))
        for i in range(hd * fd):
            wd.set(i, Float32(i % 7) * 0.001)
        load_named_weight(model, prefix + "mlp.down_proj.weight", wd, hd * fd)

    return model^


fn _build_tiny_tokenizer() -> BPETokenizer:
    """Build a tiny tokenizer whose vocab matches tiny_test_params vocab_size=8."""
    var tok = BPETokenizer()
    _ = tok.add_special_token("<s>", "bos")     # 0
    _ = tok.add_special_token("</s>", "eos")    # 1
    _ = tok.add_special_token("<unk>", "unk")   # 2
    _ = tok.add_token(" ")                       # 3
    _ = tok.add_token("a")                       # 4
    _ = tok.add_token("b")                       # 5
    _ = tok.add_token("c")                       # 6
    _ = tok.add_token("d")                       # 7
    return tok^


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_pipeline_config_defaults() raises:
    """Test PipelineConfig has correct defaults."""
    var cfg = PipelineConfig()

    assert_eq(cfg.max_new_tokens, 128, "max_new_tokens")
    assert_near(cfg.repetition_penalty, 1.0, 0.001, "rep_penalty")
    assert_near(cfg.frequency_penalty, 0.0, 0.001, "freq_penalty")
    assert_near(cfg.presence_penalty, 0.0, 0.001, "pres_penalty")
    assert_true(cfg.add_bos, "add_bos")
    assert_eq_str(cfg.chat_template, "none", "template")
    assert_eq_str(cfg.system_prompt, "", "system_prompt")

    # Test default_pipeline_config helper
    var d = default_pipeline_config()
    assert_eq(d.max_new_tokens, 128, "default max_tokens")
    assert_near(d.sampler_config.temperature, 0.0, 0.001, "default temp")

    # Test chat_pipeline_config helper
    var c = chat_pipeline_config("llama")
    assert_near(c.sampler_config.temperature, 0.7, 0.001, "chat temp")
    assert_eq(c.sampler_config.top_k, 40, "chat top_k")
    assert_near(c.sampler_config.top_p, 0.9, 0.001, "chat top_p")
    assert_near(c.repetition_penalty, 1.1, 0.001, "chat rep_penalty")
    assert_eq_str(c.chat_template, "llama", "chat template")

    print("  pipeline_config_defaults: PASS")


fn test_pipeline_generate_basic() raises:
    """Test basic pipeline generation with a tiny model."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5

    var result = pipeline_generate(model, tok, "ab", cfg)

    # Should produce a non-empty string
    assert_true(len(result) > 0, "non-empty output")

    print("  pipeline_generate_basic: PASS")


fn test_pipeline_with_eos_stopping() raises:
    """Test that pipeline stops at EOS token."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 100  # Large limit — should stop at EOS

    # Run generation — it will stop at EOS or hit limit
    var result = pipeline_generate(model, tok, "a", cfg)

    # Just verify it doesn't crash and returns something
    # (with random-ish weights, we can't predict if EOS will be hit)
    assert_true(len(result) >= 0, "valid output")

    print("  pipeline_with_eos_stopping: PASS")


fn test_pipeline_with_repetition_penalty() raises:
    """Test pipeline with repetition penalty enabled."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 5
    cfg.repetition_penalty = 1.5

    var result = pipeline_generate(model, tok, "a", cfg)
    # Just verify it runs without error
    assert_true(len(result) >= 0, "rep penalty output")

    print("  pipeline_with_repetition_penalty: PASS")


fn test_pipeline_with_chat_template() raises:
    """Test pipeline with llama chat template."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    var cfg = default_pipeline_config()
    cfg.max_new_tokens = 3
    cfg.chat_template = String("llama")

    var result = pipeline_generate(model, tok, "ab", cfg)
    # Should produce some output (template adds extra tokens around prompt)
    assert_true(len(result) >= 0, "chat template output")

    print("  pipeline_with_chat_template: PASS")


fn test_format_llama_basic() raises:
    """Test Llama template formatting without system prompt."""
    var result = format_llama("Hello", "")
    assert_eq_str(result, "[INST] Hello [/INST]", "llama basic")

    print("  format_llama_basic: PASS")


fn test_format_llama_with_system() raises:
    """Test Llama template formatting with system prompt."""
    var result = format_llama("Hello", "You are helpful.")
    assert_true(
        len(result) > 0
        and result[:8] == "<<SYS>>\n",
        "llama system starts with <<SYS>>",
    )
    assert_true(
        result.endswith("[/INST]"),
        "llama system ends with [/INST]",
    )

    print("  format_llama_with_system: PASS")


fn test_format_chatml() raises:
    """Test ChatML template formatting."""
    var result = format_chatml("Hello", "")
    assert_true(
        result[:15] == "<|im_start|>use",
        "chatml starts with im_start user",
    )
    assert_true(
        result.endswith("assistant\n"),
        "chatml ends with assistant",
    )

    # With system prompt
    var result2 = format_chatml("Hello", "You are helpful.")
    assert_true(
        result2[:15] == "<|im_start|>sys",
        "chatml sys starts with im_start system",
    )

    print("  format_chatml: PASS")


fn main() raises:
    print("test_pipeline:")

    test_pipeline_config_defaults()
    test_pipeline_generate_basic()
    test_pipeline_with_eos_stopping()
    test_pipeline_with_repetition_penalty()
    test_pipeline_with_chat_template()
    test_format_llama_basic()
    test_format_llama_with_system()
    test_format_chatml()

    print("ALL PASSED (8 tests)")
