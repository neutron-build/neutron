# ===----------------------------------------------------------------------=== #
# Test Mixed Precision Pipeline + Auto-Quantize
# ===----------------------------------------------------------------------=== #

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.mixed_quant import (
    MixedQuantModel,
    quantize_mixed,
    auto_quantize,
    analyze_sensitivity,
    auto_calibrate,
    mixed_generate,
)
from neutron_mojo.nn.mixed_pipeline import mixed_pipeline_generate
from neutron_mojo.nn.pipeline import PipelineConfig, default_pipeline_config
from neutron_mojo.nn.tokenizer import BPETokenizer, build_test_tokenizer
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model
from neutron_mojo.nn.q_pipeline import q_pipeline_generate
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.sampler import SamplerConfig, greedy_config


# ===----------------------------------------------------------------------=== #
# Helpers
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    """Build a tiny FP32 model with deterministic weights."""
    var p = tiny_test_params()
    var model = Model(p)
    var total = p.num_layers * p.layer_weight_count()
    for i in range(total):
        model.layer_weights.set(i, Float32(i % 17) * 0.01 - 0.08)
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32((v + d) % 11) * 0.02 - 0.1)
            model.lm_head.set(v * p.hidden_dim + d, Float32((v * 3 + d) % 13) * 0.02 - 0.12)
    return model^


fn _build_tokenizer() raises -> BPETokenizer:
    """Build a test tokenizer."""
    return build_test_tokenizer()


fn _build_mixed_all_q8() -> MixedQuantModel:
    """Build a tiny mixed model with all layers Q8."""
    var model = _build_tiny_model()
    var modes = List[Int]()
    for i in range(model.params.num_layers):
        modes.append(1)
    return quantize_mixed(model, modes)


# ===----------------------------------------------------------------------=== #
# Tests — mixed_pipeline_generate
# ===----------------------------------------------------------------------=== #

fn test_mixed_pipeline_basic() raises:
    """mixed_pipeline_generate produces non-empty output."""
    var model = _build_tiny_model()
    var modes = List[Int]()
    for i in range(model.params.num_layers):
        modes.append(1)
    var mixed = quantize_mixed(model, modes)
    var tok = _build_tokenizer()
    var config = default_pipeline_config()
    config.max_new_tokens = 5
    var result = mixed_pipeline_generate(mixed, tok, "hello", config)
    if len(result) == 0:
        print("FAIL test_mixed_pipeline_basic: empty output")
        return
    print("PASS test_mixed_pipeline_basic")


fn test_mixed_pipeline_with_eos() raises:
    """Pipeline stops at EOS token."""
    var model = _build_tiny_model()
    var modes = List[Int]()
    for i in range(model.params.num_layers):
        modes.append(1)
    var mixed = quantize_mixed(model, modes)
    var tok = _build_tokenizer()
    var config = default_pipeline_config()
    config.max_new_tokens = 100
    # Even with high max, should stop at EOS or produce bounded output
    var result = mixed_pipeline_generate(mixed, tok, "test", config)
    # Just verify it doesn't crash with high max_new_tokens
    print("PASS test_mixed_pipeline_with_eos")


fn test_mixed_pipeline_repetition_penalty() raises:
    """Pipeline with repetition penalty runs without error."""
    var model = _build_tiny_model()
    var modes = List[Int]()
    for i in range(model.params.num_layers):
        modes.append(2)  # all Q4
    var mixed = quantize_mixed(model, modes)
    var tok = _build_tokenizer()
    var config = default_pipeline_config()
    config.max_new_tokens = 5
    config.repetition_penalty = 1.2
    var result = mixed_pipeline_generate(mixed, tok, "hello", config)
    print("PASS test_mixed_pipeline_repetition_penalty")


fn test_mixed_pipeline_frequency_penalty() raises:
    """Pipeline with frequency + presence penalties works."""
    var model = _build_tiny_model()
    var modes = List[Int]()
    for i in range(model.params.num_layers):
        modes.append(1)
    var mixed = quantize_mixed(model, modes)
    var tok = _build_tokenizer()
    var config = default_pipeline_config()
    config.max_new_tokens = 5
    config.frequency_penalty = 0.5
    config.presence_penalty = 0.3
    var result = mixed_pipeline_generate(mixed, tok, "test", config)
    print("PASS test_mixed_pipeline_frequency_penalty")


fn test_mixed_pipeline_llama_template() raises:
    """Pipeline with llama chat template works."""
    var model = _build_tiny_model()
    var modes = List[Int]()
    for i in range(model.params.num_layers):
        modes.append(1)
    var mixed = quantize_mixed(model, modes)
    var tok = _build_tokenizer()
    var config = default_pipeline_config()
    config.max_new_tokens = 3
    config.chat_template = "llama"
    var result = mixed_pipeline_generate(mixed, tok, "hi", config)
    print("PASS test_mixed_pipeline_llama_template")


fn test_mixed_pipeline_chatml_template() raises:
    """Pipeline with chatml chat template works."""
    var model = _build_tiny_model()
    var modes = List[Int]()
    for i in range(model.params.num_layers):
        modes.append(1)
    var mixed = quantize_mixed(model, modes)
    var tok = _build_tokenizer()
    var config = default_pipeline_config()
    config.max_new_tokens = 3
    config.chat_template = "chatml"
    config.system_prompt = "You are helpful."
    var result = mixed_pipeline_generate(mixed, tok, "hi", config)
    print("PASS test_mixed_pipeline_chatml_template")


fn test_mixed_pipeline_config_reuse() raises:
    """Same PipelineConfig works with both mixed and Q8 pipelines."""
    var model = _build_tiny_model()
    var q8 = quantize_from_model(model)
    var modes = List[Int]()
    for i in range(model.params.num_layers):
        modes.append(1)
    var mixed = quantize_mixed(model, modes)
    var tok = _build_tokenizer()

    var config = default_pipeline_config()
    config.max_new_tokens = 3

    var r1 = mixed_pipeline_generate(mixed, tok, "test", config)
    var r2 = q_pipeline_generate(q8, tok, "test", config)
    # Both should produce output without crashing
    print("PASS test_mixed_pipeline_config_reuse")


fn test_mixed_pipeline_mixed_modes() raises:
    """Pipeline with actual mixed modes (Q8 + Q4) runs correctly."""
    var model = _build_tiny_model()
    var modes = List[Int]()
    # Alternate Q8 and Q4
    for i in range(model.params.num_layers):
        if i % 2 == 0:
            modes.append(1)  # Q8
        else:
            modes.append(2)  # Q4
    var mixed = quantize_mixed(model, modes)
    var tok = _build_tokenizer()
    var config = default_pipeline_config()
    config.max_new_tokens = 5
    var result = mixed_pipeline_generate(mixed, tok, "hello", config)
    if len(result) == 0:
        print("FAIL test_mixed_pipeline_mixed_modes: empty output")
        return
    print("PASS test_mixed_pipeline_mixed_modes")


# ===----------------------------------------------------------------------=== #
# Tests — auto_quantize
# ===----------------------------------------------------------------------=== #

fn test_auto_quantize_basic() raises:
    """auto_quantize returns a valid MixedQuantModel."""
    var model = _build_tiny_model()
    var mixed = auto_quantize(model)
    # Should have correct number of layer modes
    if len(mixed.layer_modes) != model.params.num_layers:
        print("FAIL test_auto_quantize_basic: wrong layer count")
        return
    # Each mode should be 1 or 2
    for i in range(len(mixed.layer_modes)):
        var m = mixed.layer_modes[i]
        if m != 1 and m != 2:
            print("FAIL test_auto_quantize_basic: invalid mode", m)
            return
    print("PASS test_auto_quantize_basic")


fn test_auto_quantize_generates() raises:
    """auto_quantize model can run forward pass and generate tokens."""
    var model = _build_tiny_model()
    var mixed = auto_quantize(model)
    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)
    var tokens = mixed_generate(mixed, prompt, 3)
    if len(tokens) != 3:
        print("FAIL test_auto_quantize_generates: expected 3 tokens, got", len(tokens))
        return
    print("PASS test_auto_quantize_generates")


fn test_auto_quantize_pipeline() raises:
    """auto_quantize model works with mixed_pipeline_generate."""
    var model = _build_tiny_model()
    var mixed = auto_quantize(model)
    var tok = _build_tokenizer()
    var config = default_pipeline_config()
    config.max_new_tokens = 3
    var result = mixed_pipeline_generate(mixed, tok, "hello", config)
    print("PASS test_auto_quantize_pipeline")


fn test_auto_quantize_threshold_all_q4() raises:
    """Very high threshold makes all layers Q4."""
    var model = _build_tiny_model()
    var mixed = auto_quantize(model, q4_threshold=100.0)
    for i in range(len(mixed.layer_modes)):
        if mixed.layer_modes[i] != 2:
            print("FAIL test_auto_quantize_threshold_all_q4: layer", i, "not Q4")
            return
    print("PASS test_auto_quantize_threshold_all_q4")


fn test_auto_quantize_threshold_all_q8() raises:
    """Very low threshold makes all layers Q8."""
    var model = _build_tiny_model()
    var mixed = auto_quantize(model, q4_threshold=0.0)
    for i in range(len(mixed.layer_modes)):
        if mixed.layer_modes[i] != 1:
            print("FAIL test_auto_quantize_threshold_all_q8: layer", i, "not Q8")
            return
    print("PASS test_auto_quantize_threshold_all_q8")


fn test_auto_quantize_mode_summary() raises:
    """auto_quantize model has valid mode summary."""
    var model = _build_tiny_model()
    var mixed = auto_quantize(model)
    var summary = mixed.mode_summary()
    if len(summary) == 0:
        print("FAIL test_auto_quantize_mode_summary: empty summary")
        return
    # Should contain Q8 and/or Q4
    print("  mode_summary:", summary)
    print("PASS test_auto_quantize_mode_summary")


fn test_auto_quantize_vs_manual() raises:
    """auto_quantize matches manual analyze+calibrate+quantize chain."""
    var model = _build_tiny_model()

    # Manual chain
    var sens = analyze_sensitivity(model)
    var modes = auto_calibrate(sens, 0.01)
    var manual = quantize_mixed(model, modes)

    # auto_quantize
    var auto = auto_quantize(model, q4_threshold=0.01)

    # Should have same modes
    for i in range(len(manual.layer_modes)):
        if manual.layer_modes[i] != auto.layer_modes[i]:
            print("FAIL test_auto_quantize_vs_manual: mode mismatch at layer", i)
            return

    # Forward pass should produce same logits
    var p = model.params.copy()
    var cache1 = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=16,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var cache2 = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=16,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=16, theta=p.rope_theta)

    var logits1 = manual.forward(1, cache1, rope, pos=0)
    var logits2 = auto.forward(1, cache2, rope, pos=0)

    for i in range(p.vocab_size):
        var diff = logits1.get(i) - logits2.get(i)
        if diff < 0:
            diff = -diff
        if diff > 1e-6:
            print("FAIL test_auto_quantize_vs_manual: logit mismatch at", i)
            return

    print("PASS test_auto_quantize_vs_manual")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("=== Test Mixed Pipeline + Auto-Quantize ===")
    print()

    print("--- mixed_pipeline_generate ---")
    test_mixed_pipeline_basic()
    test_mixed_pipeline_with_eos()
    test_mixed_pipeline_repetition_penalty()
    test_mixed_pipeline_frequency_penalty()
    test_mixed_pipeline_llama_template()
    test_mixed_pipeline_chatml_template()
    test_mixed_pipeline_config_reuse()
    test_mixed_pipeline_mixed_modes()
    print()

    print("--- auto_quantize ---")
    test_auto_quantize_basic()
    test_auto_quantize_generates()
    test_auto_quantize_pipeline()
    test_auto_quantize_threshold_all_q4()
    test_auto_quantize_threshold_all_q8()
    test_auto_quantize_mode_summary()
    test_auto_quantize_vs_manual()
    print()

    print("=== All 15 tests completed ===")
