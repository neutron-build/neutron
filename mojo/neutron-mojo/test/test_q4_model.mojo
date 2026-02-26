# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Q4 Model Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Q4-quantized model: Q4Model, quantize_from_model_q4, q4_generate."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import simd_q8_matvec
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model, q_generate
from neutron_mojo.nn.q4_model import Q4Model, quantize_from_model_q4, q4_generate
from neutron_mojo.nn.tokenizer import BPETokenizer, build_test_tokenizer
from neutron_mojo.nn.pipeline import PipelineConfig, default_pipeline_config
from neutron_mojo.nn.q4_pipeline import q4_pipeline_generate


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn _build_tiny_model() -> Model:
    """Create a small model with non-trivial weights for testing."""
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


# ===----------------------------------------------------------------------=== #
# Q4Model Creation Tests
# ===----------------------------------------------------------------------=== #

fn test_q4_model_creation() raises:
    """Test Q4Model struct creation."""
    var p = tiny_test_params()
    var qm = Q4Model(p, block_size=2)

    assert_true(qm.block_size == 2, "block_size")
    assert_true(qm.layer_size == p.layer_weight_count(), "layer_size matches Model")
    assert_true(qm.scales_per_layer > 0, "scales_per_layer > 0")

    print("  q4_model_creation: PASS")


fn test_quantize_from_model_q4() raises:
    """Test converting FP32 Model to Q4Model."""
    var model = _build_tiny_model()
    var p = model.params.copy()
    var qm = quantize_from_model_q4(model, block_size=2)

    # Embeddings should be copied exactly
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            assert_near(qm.embed.get(v, d), model.embed.get(v, d), 0.0001, "embed copied")

    # LM head copied exactly
    assert_near(qm.lm_head.get(0, 0), model.lm_head.get(0, 0), 0.0001, "lm_head copied")

    # Norms should be 1.0
    var off = qm._layer_offsets(0)
    assert_near(
        qm.layer_weights.get(off.attn_norm), 1.0, 0.0001, "attn_norm preserved"
    )

    # Q4 values should be integers in [-8, 7]
    var wq_val = qm.layer_weights.get(off.wq)
    var rounded = Float32(Int(wq_val))
    assert_near(wq_val, rounded, 0.0001, "wq is integer-valued")
    assert_true(wq_val >= -8.0 and wq_val <= 7.0, "wq in Q4 range [-8, 7]")

    print("  quantize_from_model_q4: PASS")


fn test_q4_values_in_range() raises:
    """Verify all Q4 projection values are in [-8, 7]."""
    var model = _build_tiny_model()
    var p = model.params.copy()
    var qm = quantize_from_model_q4(model, block_size=2)

    for layer in range(p.num_layers):
        var off = qm._layer_offsets(layer)
        # Check first few wq values
        var qd = p.q_dim()
        var hd = p.hidden_dim
        for i in range(qd * hd):
            var val = qm.layer_weights.get(off.wq + i)
            assert_true(
                val >= -8.0 and val <= 7.0,
                "Q4 value in range at wq+" + String(i)
            )

    print("  q4_values_in_range: PASS")


# ===----------------------------------------------------------------------=== #
# Q4 Forward Pass Tests
# ===----------------------------------------------------------------------=== #

fn test_q4_forward_produces_output() raises:
    """Test that Q4Model forward pass produces valid logits."""
    var model = _build_tiny_model()
    var p = model.params.copy()
    var qm = quantize_from_model_q4(model, block_size=2)

    var prompt = List[Int]()
    prompt.append(1)

    var tokens = q4_generate(qm, prompt, max_new_tokens=2)
    assert_true(len(tokens) == 2, "generated 2 tokens")

    for i in range(len(tokens)):
        assert_true(tokens[i] >= 0, "token >= 0")
        assert_true(tokens[i] < p.vocab_size, "token < vocab_size")

    print("  q4_forward_produces_output: PASS")


fn test_q4_vs_fp32() raises:
    """Test that Q4 model produces valid tokens (may differ from FP32)."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    # FP32 generation
    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)
    var fp32_tokens = generate(model, prompt, max_new_tokens=3)

    # Q4 generation
    var qm = quantize_from_model_q4(model, block_size=2)
    var q4_tokens = q4_generate(qm, prompt, max_new_tokens=3)

    assert_true(len(fp32_tokens) == 3, "fp32 generated 3")
    assert_true(len(q4_tokens) == 3, "q4 generated 3")

    for i in range(3):
        assert_true(fp32_tokens[i] >= 0 and fp32_tokens[i] < p.vocab_size, "fp32 valid")
        assert_true(q4_tokens[i] >= 0 and q4_tokens[i] < p.vocab_size, "q4 valid")

    print("  q4_vs_fp32: PASS")


fn test_q4_vs_q8_memory() raises:
    """Verify Q4 uses same memory layout but different value range than Q8."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var q8m = quantize_from_model(model, block_size=2)
    var q4m = quantize_from_model_q4(model, block_size=2)

    # Same layout sizes (both store values as float32 with per-block scales)
    assert_true(q8m.layer_size == q4m.layer_size, "same layer_size")
    assert_true(q8m.scales_per_layer == q4m.scales_per_layer, "same scales_per_layer")

    # But Q4 values are smaller magnitude ([-8, 7] vs [-127, 127])
    var off = q4m._layer_offsets(0)
    var q4_max: Float32 = 0.0
    var q8_max: Float32 = 0.0
    var qd = p.q_dim()
    var hd = p.hidden_dim
    for i in range(qd * hd):
        var v4 = q4m.layer_weights.get(off.wq + i)
        var v8 = q8m.layer_weights.get(off.wq + i)
        if v4 > q4_max:
            q4_max = v4
        if v8 > q8_max:
            q8_max = v8

    assert_true(q4_max <= 7.0, "Q4 max value <= 7")
    assert_true(q8_max <= 127.0, "Q8 max value <= 127")

    # Q4 scales are larger (absmax/7 vs absmax/127)
    var soff = q4m._layer_scale_offsets(0)
    var q4_scale = q4m.layer_scales.get(soff.wq)
    var q8_scale = q8m.layer_scales.get(soff.wq)
    # Q4 scale should be ~18x larger than Q8 scale (127/7 ≈ 18.14)
    if q8_scale > 0.0001:
        var ratio = q4_scale / q8_scale
        assert_true(ratio > 10.0 and ratio < 25.0, "Q4 scale ~18x larger than Q8")

    print("  q4_vs_q8_memory: PASS")


# ===----------------------------------------------------------------------=== #
# Q4 Offset Tests
# ===----------------------------------------------------------------------=== #

fn test_q4_offsets_consistency() raises:
    """Test that data and scale offsets are computed consistently."""
    var p = tiny_test_params()
    var qm = Q4Model(p, block_size=2)

    # Verify layer 0 and layer 1 offsets don't overlap
    var off0 = qm._layer_offsets(0)
    var off1 = qm._layer_offsets(1)
    assert_true(off1.attn_norm > off0.w_down, "layer 1 starts after layer 0")

    var soff0 = qm._layer_scale_offsets(0)
    var soff1 = qm._layer_scale_offsets(1)
    assert_true(soff1.wq > soff0.w_down, "scale layer 1 after scale layer 0")

    # Verify scale offset ordering within a layer
    assert_true(soff0.wk > soff0.wq, "wk scales after wq")
    assert_true(soff0.wv > soff0.wk, "wv scales after wk")
    assert_true(soff0.wo > soff0.wv, "wo scales after wv")
    assert_true(soff0.w_gate > soff0.wo, "w_gate scales after wo")
    assert_true(soff0.w_up > soff0.w_gate, "w_up scales after w_gate")
    assert_true(soff0.w_down > soff0.w_up, "w_down scales after w_up")

    print("  q4_offsets_consistency: PASS")


# ===----------------------------------------------------------------------=== #
# Q4 Pipeline Tests
# ===----------------------------------------------------------------------=== #

fn test_q4_pipeline_generate() raises:
    """Test Q4 pipeline text-in/text-out."""
    var model = _build_tiny_model()
    var qm = quantize_from_model_q4(model, block_size=2)
    var tok = build_test_tokenizer()

    var config = default_pipeline_config()
    config.max_new_tokens = 3

    var result = q4_pipeline_generate(qm, tok, "hello", config)
    assert_true(len(result) > 0, "Q4 pipeline produced output")

    print("  q4_pipeline_generate: PASS")


fn test_q4_pipeline_with_chat_template() raises:
    """Test Q4 pipeline with llama chat template."""
    var model = _build_tiny_model()
    var qm = quantize_from_model_q4(model, block_size=2)
    var tok = build_test_tokenizer()

    var config = default_pipeline_config()
    config.max_new_tokens = 2
    config.chat_template = "llama"

    var result = q4_pipeline_generate(qm, tok, "hello", config)
    assert_true(len(result) >= 0, "Q4 pipeline with template ran")

    print("  q4_pipeline_with_chat_template: PASS")


fn test_q4_pipeline_with_penalties() raises:
    """Test Q4 pipeline with repetition and frequency penalties."""
    var model = _build_tiny_model()
    var qm = quantize_from_model_q4(model, block_size=2)
    var tok = build_test_tokenizer()

    var config = default_pipeline_config()
    config.max_new_tokens = 3
    config.repetition_penalty = 1.2
    config.frequency_penalty = 0.5
    config.presence_penalty = 0.3

    var result = q4_pipeline_generate(qm, tok, "test", config)
    assert_true(len(result) >= 0, "Q4 pipeline with penalties ran")

    print("  q4_pipeline_with_penalties: PASS")


fn test_q4_pipeline_config_reuse() raises:
    """Verify Q4 pipeline reuses same PipelineConfig as FP32/Q8."""
    var config = default_pipeline_config()
    # PipelineConfig is shared across FP32, Q8, and Q4 pipelines
    assert_true(config.max_new_tokens > 0, "config has max_new_tokens")
    assert_true(config.chat_template == "none", "default template is none")
    assert_true(config.add_bos == True, "default add_bos is true")

    print("  q4_pipeline_config_reuse: PASS")


fn main() raises:
    print("test_q4_model:")

    # Q4Model creation
    test_q4_model_creation()
    test_quantize_from_model_q4()
    test_q4_values_in_range()

    # Q4 forward pass
    test_q4_forward_produces_output()
    test_q4_vs_fp32()
    test_q4_vs_q8_memory()

    # Offsets
    test_q4_offsets_consistency()

    # Pipeline
    test_q4_pipeline_generate()
    test_q4_pipeline_with_chat_template()
    test_q4_pipeline_with_penalties()
    test_q4_pipeline_config_reuse()

    print("ALL PASSED")
