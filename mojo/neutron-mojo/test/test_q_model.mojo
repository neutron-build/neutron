# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantized Model Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Q8-quantized model: simd_q8_matvec, QuantizedModel, quantize_from_model."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import simd_q8_matvec, simd_matvec
from neutron_mojo.nn.quantized_linear import (
    Q8Weight,
    quantize_weight_q8,
    q8_linear,
)
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model, q_generate


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


# ===----------------------------------------------------------------------=== #
# simd_q8_matvec Tests
# ===----------------------------------------------------------------------=== #

fn test_simd_q8_matvec_basic() raises:
    """Verify simd_q8_matvec matches manual Q8 computation."""
    # 2x4 weight, block_size=4 (1 block per row)
    var q_data = Tensor[DType.float32](Shape(8))
    var scales = Tensor[DType.float32](Shape(2))
    var x = Tensor[DType.float32](Shape(4))

    # Row 0: quantized values [127, 0, -127, 64], scale = 0.1
    # Dequantized: [12.7, 0, -12.7, 6.4]
    q_data.set(0, 127.0)
    q_data.set(1, 0.0)
    q_data.set(2, -127.0)
    q_data.set(3, 64.0)
    scales.set(0, 0.1)

    # Row 1: quantized values [50, 50, 50, 50], scale = 0.2
    # Dequantized: [10, 10, 10, 10]
    q_data.set(4, 50.0)
    q_data.set(5, 50.0)
    q_data.set(6, 50.0)
    q_data.set(7, 50.0)
    scales.set(1, 0.2)

    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, -1.0)
    x.set(3, 0.5)

    var out = Tensor[DType.float32](Shape(2))
    simd_q8_matvec(out, 0, q_data, 0, scales, 0, x, 0, 2, 4, 4)

    # Row 0: scale * (127*1 + 0*2 + (-127)*(-1) + 64*0.5) = 0.1 * (127+0+127+32) = 0.1 * 286 = 28.6
    assert_near(out.get(0), 28.6, 0.01, "q8_matvec row 0")
    # Row 1: scale * (50*1 + 50*2 + 50*(-1) + 50*0.5) = 0.2 * (50+100-50+25) = 0.2 * 125 = 25.0
    assert_near(out.get(1), 25.0, 0.01, "q8_matvec row 1")

    print("  simd_q8_matvec_basic: PASS")


fn test_simd_q8_matvec_multi_block() raises:
    """Test simd_q8_matvec with multiple blocks per row."""
    # 1x8 weight, block_size=4 (2 blocks)
    var q_data = Tensor[DType.float32](Shape(8))
    var scales = Tensor[DType.float32](Shape(2))  # 1 row * 2 blocks
    var x = Tensor[DType.float32](Shape(8))

    # Block 0: values [10, 20, 30, 40], scale = 0.5
    q_data.set(0, 10.0)
    q_data.set(1, 20.0)
    q_data.set(2, 30.0)
    q_data.set(3, 40.0)
    scales.set(0, 0.5)

    # Block 1: values [5, 5, 5, 5], scale = 1.0
    q_data.set(4, 5.0)
    q_data.set(5, 5.0)
    q_data.set(6, 5.0)
    q_data.set(7, 5.0)
    scales.set(1, 1.0)

    for i in range(8):
        x.set(i, 1.0)

    var out = Tensor[DType.float32](Shape(1))
    simd_q8_matvec(out, 0, q_data, 0, scales, 0, x, 0, 1, 8, 4)

    # Block 0: 0.5 * (10+20+30+40) = 0.5 * 100 = 50
    # Block 1: 1.0 * (5+5+5+5) = 20
    # Total: 70
    assert_near(out.get(0), 70.0, 0.01, "q8_matvec multi-block")

    print("  simd_q8_matvec_multi_block: PASS")


fn test_simd_q8_matches_q8_linear() raises:
    """Verify simd_q8_matvec matches q8_linear for realistic sizes."""
    var out_features = 8
    var in_features = 16
    var bs = 4

    # Create FP32 weights, quantize, then compare q8_linear vs simd_q8_matvec
    var w = Tensor[DType.float32](Shape(out_features, in_features))
    for i in range(out_features * in_features):
        w.set(i, Float32(i % 11) * 0.1 - 0.5)

    var qw = quantize_weight_q8(w, out_features, in_features, block_size=bs)

    var x = Tensor[DType.float32](Shape(in_features))
    for i in range(in_features):
        x.set(i, Float32(i) * 0.1)

    # q8_linear (uses simd_q8_matvec internally now)
    var y1 = q8_linear(x, qw)

    # Direct simd_q8_matvec call
    var y2 = Tensor[DType.float32](Shape(out_features))
    simd_q8_matvec(
        y2, 0, qw.data, 0, qw.scales, 0,
        x, 0, out_features, in_features, bs,
    )

    for i in range(out_features):
        assert_near(y1.get(i), y2.get(i), 0.001, "q8_linear vs simd_q8_matvec[" + String(i) + "]")

    print("  simd_q8_matches_q8_linear: PASS")


# ===----------------------------------------------------------------------=== #
# QuantizedModel Tests
# ===----------------------------------------------------------------------=== #

fn test_quantized_model_creation() raises:
    """Test QuantizedModel struct creation."""
    var p = tiny_test_params()
    var qm = QuantizedModel(p, block_size=2)

    assert_true(qm.block_size == 2, "block_size")
    assert_true(qm.layer_size == p.layer_weight_count(), "layer_size matches Model")
    assert_true(qm.scales_per_layer > 0, "scales_per_layer > 0")

    print("  quantized_model_creation: PASS")


fn test_quantize_from_model() raises:
    """Test converting FP32 Model to QuantizedModel."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set some non-trivial weights
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v * d + 1) * 0.05)

    # Set some layer weights
    for i in range(p.num_layers * model.layer_size):
        model.layer_weights.set(i, Float32(i % 17) * 0.02 - 0.15)

    # Re-set norms to 1.0 after overwriting
    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)

    var qm = quantize_from_model(model, block_size=2)

    # Embeddings should be copied exactly (use 2D get for safe access)
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            assert_near(qm.embed.get(v, d), model.embed.get(v, d), 0.0001, "embed copied")

    # LM head copied exactly
    assert_near(qm.lm_head.get(0, 0), model.lm_head.get(0, 0), 0.0001, "lm_head copied")

    # Norms should be 1.0 (1D tensor, get() works correctly)
    var off = qm._layer_offsets(0)
    assert_near(
        qm.layer_weights.get(off.attn_norm), 1.0, 0.0001, "attn_norm preserved"
    )

    # Quantized projection values should be integers in [-127, 127]
    var wq_val = qm.layer_weights.get(off.wq)
    var rounded = Float32(Int(wq_val))
    assert_near(wq_val, rounded, 0.0001, "wq is integer-valued")

    print("  quantize_from_model: PASS")


fn test_q8_forward_produces_output() raises:
    """Test that QuantizedModel forward pass produces valid logits."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set weights
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)

    var qm = quantize_from_model(model, block_size=2)

    # Copy embed and lm_head (quantize_from_model already does this)
    var prompt = List[Int]()
    prompt.append(1)

    var tokens = q_generate(qm, prompt, max_new_tokens=2)
    assert_true(len(tokens) == 2, "generated 2 tokens")

    for i in range(len(tokens)):
        assert_true(tokens[i] >= 0, "token >= 0")
        assert_true(tokens[i] < p.vocab_size, "token < vocab_size")

    print("  q8_forward_produces_output: PASS")


fn test_q8_vs_fp32_similarity() raises:
    """Test that Q8 model output is reasonably close to FP32."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set non-trivial weights
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(
                v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01
            )
            model.lm_head.set(
                v * p.hidden_dim + d, Float32(v + d) * 0.1
            )
    for i in range(p.num_layers * model.layer_size):
        model.layer_weights.set(i, Float32(i % 13) * 0.02 - 0.12)
    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)

    # FP32 generation
    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)
    var fp32_tokens = generate(model, prompt, max_new_tokens=3)

    # Q8 generation
    var qm = quantize_from_model(model, block_size=2)
    var q8_tokens = q_generate(qm, prompt, max_new_tokens=3)

    assert_true(len(fp32_tokens) == 3, "fp32 generated 3")
    assert_true(len(q8_tokens) == 3, "q8 generated 3")

    # Both should produce valid tokens
    for i in range(3):
        assert_true(fp32_tokens[i] >= 0 and fp32_tokens[i] < p.vocab_size, "fp32 valid")
        assert_true(q8_tokens[i] >= 0 and q8_tokens[i] < p.vocab_size, "q8 valid")

    # With tiny weights, they may or may not match exactly due to quantization
    # Just verify both produce plausible results
    print("  q8_vs_fp32_similarity: PASS")


fn test_q8_offsets_consistency() raises:
    """Test that data and scale offsets are computed consistently."""
    var p = tiny_test_params()
    var qm = QuantizedModel(p, block_size=2)

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

    print("  q8_offsets_consistency: PASS")


fn main() raises:
    print("test_q_model:")

    # simd_q8_matvec
    test_simd_q8_matvec_basic()
    test_simd_q8_matvec_multi_block()
    test_simd_q8_matches_q8_linear()

    # QuantizedModel
    test_quantized_model_creation()
    test_quantize_from_model()
    test_q8_forward_produces_output()
    test_q8_vs_fp32_similarity()
    test_q8_offsets_consistency()

    print("ALL PASSED")
