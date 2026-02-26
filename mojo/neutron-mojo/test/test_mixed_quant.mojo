# ===----------------------------------------------------------------------=== #
# Test — Mixed Precision Quantization
# ===----------------------------------------------------------------------=== #

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, LayerWeightOffsets
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.mixed_quant import (
    LayerSensitivity,
    MixedQuantModel,
    measure_layer_sensitivity,
    analyze_sensitivity,
    auto_calibrate,
    quantize_mixed,
    mixed_generate,
    _compute_offsets,
    _quant_roundtrip_error,
)


# ===----------------------------------------------------------------------=== #
# Test Helpers
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    """Build a tiny model with non-trivial weights for testing."""
    var p = tiny_test_params()
    var model = Model(p)

    # Populate embed and lm_head
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32((v + d) % 5) * 0.1)
            model.lm_head.set(v * p.hidden_dim + d, Float32((v * 3 + d) % 7) * 0.1 - 0.3)

    # Populate all layer weights with non-trivial values
    var total = p.num_layers * p.layer_weight_count()
    for i in range(total):
        model.layer_weights.set(i, Float32(i % 13) * 0.04 - 0.24)

    # Re-set norms to 1.0 (needed for stable rmsnorm)
    for layer in range(p.num_layers):
        var off = _compute_offsets(p, layer)
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)
    for i in range(p.hidden_dim):
        model.final_norm.set(i, 1.0)

    return model^


fn _assert(cond: Bool, msg: String) raises:
    if not cond:
        raise Error(msg)


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_layer_sensitivity_creation() raises:
    """Test LayerSensitivity struct construction."""
    var s = LayerSensitivity()
    _assert(s.q8_error == 0.0, "q8_error should default to 0")
    _assert(s.q4_error == 0.0, "q4_error should default to 0")

    # Test copy
    s.q8_error = 0.001
    s.q4_error = 0.05
    var s2 = s.copy()
    _assert(s2.q8_error == 0.001, "copy q8_error mismatch")
    _assert(s2.q4_error == 0.05, "copy q4_error mismatch")
    print("PASS: test_layer_sensitivity_creation")


fn test_quant_roundtrip_error() raises:
    """Test _quant_roundtrip_error helper."""
    # Create small weight tensor: 2 rows x 4 cols
    var weights = Tensor[DType.float32](Shape(8))
    weights.set(0, 0.5)
    weights.set(1, -0.3)
    weights.set(2, 0.1)
    weights.set(3, -0.7)
    weights.set(4, 0.2)
    weights.set(5, -0.4)
    weights.set(6, 0.6)
    weights.set(7, -0.1)

    # Q8 error should be very small
    var q8_err = _quant_roundtrip_error(weights, 0, 2, 4, 32, 127.0, -127.0)
    _assert(q8_err >= 0.0, "Q8 error should be non-negative")
    _assert(q8_err < 0.01, "Q8 error should be very small")

    # Q4 error should be larger than Q8
    var q4_err = _quant_roundtrip_error(weights, 0, 2, 4, 32, 7.0, -8.0)
    _assert(q4_err >= 0.0, "Q4 error should be non-negative")
    _assert(q4_err > q8_err, "Q4 error should be larger than Q8")

    print("PASS: test_quant_roundtrip_error")


fn test_measure_layer_sensitivity() raises:
    """Test per-layer sensitivity measurement."""
    var model = _build_tiny_model()
    var sens = measure_layer_sensitivity(model, layer=0)

    _assert(sens.q8_error >= 0.0, "Q8 error should be non-negative")
    _assert(sens.q4_error >= 0.0, "Q4 error should be non-negative")
    _assert(sens.q4_error >= sens.q8_error, "Q4 error should be >= Q8 error")

    print("PASS: test_measure_layer_sensitivity")


fn test_q8_error_less_than_q4() raises:
    """Verify Q8 always has less error than Q4 for all layers."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    for layer in range(p.num_layers):
        var sens = measure_layer_sensitivity(model, layer)
        _assert(
            sens.q8_error <= sens.q4_error,
            "Q8 error should be <= Q4 error for layer " + String(layer),
        )

    print("PASS: test_q8_error_less_than_q4")


fn test_analyze_all_layers() raises:
    """Test sensitivity analysis across all layers."""
    var model = _build_tiny_model()
    var results = analyze_sensitivity(model)

    _assert(
        len(results) == model.params.num_layers,
        "Should have one result per layer",
    )

    for i in range(len(results)):
        _assert(results[i].q8_error >= 0.0, "Q8 error should be non-negative")
        _assert(results[i].q4_error >= 0.0, "Q4 error should be non-negative")

    print("PASS: test_analyze_all_layers")


fn test_auto_calibrate_all_q4() raises:
    """With very high threshold, all layers should be Q4."""
    var model = _build_tiny_model()
    var sens = analyze_sensitivity(model)

    # Very high threshold -> everything Q4
    var modes = auto_calibrate(sens, q4_threshold=100.0)

    _assert(len(modes) == model.params.num_layers, "Wrong number of modes")
    for i in range(len(modes)):
        _assert(modes[i] == 2, "All should be Q4 with high threshold")

    print("PASS: test_auto_calibrate_all_q4")


fn test_auto_calibrate_all_q8() raises:
    """With very low threshold, all layers should be Q8."""
    var model = _build_tiny_model()
    var sens = analyze_sensitivity(model)

    # Very low threshold -> everything Q8
    var modes = auto_calibrate(sens, q4_threshold=0.0)

    _assert(len(modes) == model.params.num_layers, "Wrong number of modes")
    for i in range(len(modes)):
        _assert(modes[i] == 1, "All should be Q8 with zero threshold")

    print("PASS: test_auto_calibrate_all_q8")


fn test_auto_calibrate_mixed() raises:
    """Test that calibration can produce mixed modes with right threshold."""
    # Create model with different weight magnitudes per layer
    var p = tiny_test_params()
    # Need more layers for mixed calibration to be visible
    p.num_layers = 4
    var model = Model(p)

    # Populate with varying magnitudes per layer
    for layer in range(p.num_layers):
        var off = _compute_offsets(p, layer)
        var layer_count = p.layer_weight_count()
        var base = layer * layer_count
        # Layer 0,1: small weights (low quant error)
        # Layer 2,3: larger weights (higher quant error)
        var scale = Float32(1.0)
        if layer >= 2:
            scale = 5.0
        for i in range(layer_count):
            model.layer_weights.set(base + i, Float32(i % 13) * 0.04 * scale - 0.24 * scale)
        # Restore norms
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)
    for i in range(p.hidden_dim):
        model.final_norm.set(i, 1.0)

    var sens = analyze_sensitivity(model)

    # Find a threshold between the min Q4 error and max Q4 error
    var min_q4: Float32 = sens[0].q4_error
    var max_q4: Float32 = sens[0].q4_error
    for i in range(1, len(sens)):
        if sens[i].q4_error < min_q4:
            min_q4 = sens[i].q4_error
        if sens[i].q4_error > max_q4:
            max_q4 = sens[i].q4_error

    # If there's variation, pick a threshold in the middle
    if max_q4 > min_q4:
        var threshold = (min_q4 + max_q4) / 2.0
        var modes = auto_calibrate(sens, q4_threshold=threshold)
        var has_q4 = False
        var has_q8 = False
        for i in range(len(modes)):
            if modes[i] == 2:
                has_q4 = True
            elif modes[i] == 1:
                has_q8 = True
        _assert(has_q4 and has_q8, "Should have both Q4 and Q8 layers")

    print("PASS: test_auto_calibrate_mixed")


fn test_mixed_model_creation() raises:
    """Test MixedQuantModel construction."""
    var p = tiny_test_params()
    var modes = List[Int]()
    modes.append(0)  # Layer 0: FP32
    modes.append(1)  # Layer 1: Q8

    var mm = MixedQuantModel(p, modes)

    _assert(len(mm.layer_modes) == 2, "Should have 2 layer modes")
    _assert(mm.layer_modes[0] == 0, "Layer 0 should be FP32")
    _assert(mm.layer_modes[1] == 1, "Layer 1 should be Q8")
    _assert(mm.block_size == 32, "Default block size should be 32")

    print("PASS: test_mixed_model_creation")


fn test_quantize_mixed_fp32() raises:
    """Test that FP32-mode layers preserve exact weights."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    # All FP32
    var modes = List[Int]()
    for _ in range(p.num_layers):
        modes.append(0)

    var mm = quantize_mixed(model, modes)

    # Check that projection weights match exactly
    var off = _compute_offsets(p, 0)
    for i in range(p.q_dim() * p.hidden_dim):
        var orig = model.layer_weights.get(off.wq + i)
        var mixed = mm.layer_weights.get(off.wq + i)
        _assert(orig == mixed, "FP32 layer weights should match exactly")

    print("PASS: test_quantize_mixed_fp32")


fn test_quantize_mixed_q8_q4() raises:
    """Test mixed Q8 + Q4 quantization."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var modes = List[Int]()
    modes.append(1)  # Layer 0: Q8
    modes.append(2)  # Layer 1: Q4

    var mm = quantize_mixed(model, modes)

    _assert(mm.layer_modes[0] == 1, "Layer 0 should be Q8")
    _assert(mm.layer_modes[1] == 2, "Layer 1 should be Q4")

    # Verify Q8 layer has quantized values (should be integers)
    var off0 = _compute_offsets(p, 0)
    var val = mm.layer_weights.get(off0.wq)
    var rounded = Float32(Int(val))
    _assert(
        val == rounded or val == 0.0,
        "Q8 weight should be integer-valued",
    )

    # Verify Q4 layer has values in [-8, 7]
    var off1 = _compute_offsets(p, 1)
    for i in range(p.q_dim() * p.hidden_dim):
        var v = mm.layer_weights.get(off1.wq + i)
        _assert(v >= -8.0 and v <= 7.0, "Q4 weight should be in [-8, 7]")

    print("PASS: test_quantize_mixed_q8_q4")


fn test_mixed_model_forward() raises:
    """Test that mixed model forward pass produces valid logits."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var modes = List[Int]()
    modes.append(1)  # Q8
    modes.append(2)  # Q4

    var mm = quantize_mixed(model, modes)

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=16,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(
        head_dim=p.head_dim,
        max_seq_len=16,
        theta=p.rope_theta,
    )

    var logits = mm.forward(0, cache, rope, pos=0)
    _assert(logits.numel() == p.vocab_size, "Should produce vocab_size logits")

    # Check logits are finite
    var has_nonzero = False
    for i in range(p.vocab_size):
        var v = logits.get(i)
        _assert(v == v, "Logits should not be NaN")
        if v != 0.0:
            has_nonzero = True
    _assert(has_nonzero, "Logits should not all be zero")

    print("PASS: test_mixed_model_forward")


fn test_mixed_generate() raises:
    """Test autoregressive generation with mixed model."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var modes = List[Int]()
    modes.append(1)
    modes.append(2)

    var mm = quantize_mixed(model, modes)

    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)

    var tokens = mixed_generate(mm, prompt, max_new_tokens=5)
    _assert(len(tokens) == 5, "Should generate 5 tokens")

    for i in range(len(tokens)):
        _assert(
            tokens[i] >= 0 and tokens[i] < p.vocab_size,
            "Token should be in valid range",
        )

    print("PASS: test_mixed_generate")


fn test_mixed_all_q8_matches_quantized() raises:
    """Mixed model with all-Q8 should match QuantizedModel output."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    # Create QuantizedModel
    var qm = quantize_from_model(model)

    # Create MixedQuantModel with all Q8
    var modes = List[Int]()
    for _ in range(p.num_layers):
        modes.append(1)
    var mm = quantize_mixed(model, modes)

    # Run both on same input
    var cache_q = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=16,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var cache_m = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=16,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=16, theta=p.rope_theta)

    var logits_q = qm.forward(1, cache_q, rope, pos=0)
    var logits_m = mm.forward(1, cache_m, rope, pos=0)

    # Should be identical (same quantization, same kernel)
    var max_diff: Float32 = 0.0
    for i in range(p.vocab_size):
        var diff = logits_q.get(i) - logits_m.get(i)
        if diff < 0.0:
            diff = -diff
        if diff > max_diff:
            max_diff = diff

    _assert(
        max_diff < 1e-5,
        "All-Q8 mixed should match QuantizedModel (max_diff=" + String(max_diff) + ")",
    )

    print("PASS: test_mixed_all_q8_matches_quantized")


fn test_mode_summary() raises:
    """Test mode_summary output."""
    var p = tiny_test_params()
    var modes = List[Int]()
    modes.append(0)
    modes.append(1)

    var mm = MixedQuantModel(p, modes)
    var summary = mm.mode_summary()

    # Should contain FP32 and Q8
    _assert(len(summary) > 0, "Summary should not be empty")

    print("PASS: test_mode_summary")


fn test_fp32_forward_matches_model() raises:
    """Mixed model with all FP32 should match original Model output."""
    var model = _build_tiny_model()
    var p = model.params.copy()

    var modes = List[Int]()
    for _ in range(p.num_layers):
        modes.append(0)
    var mm = quantize_mixed(model, modes)

    # Run both on same input
    var cache_orig = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=16,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var cache_mixed = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=16,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=16, theta=p.rope_theta)

    var logits_orig = model.forward(1, cache_orig, rope, pos=0)
    var logits_mixed = mm.forward(1, cache_mixed, rope, pos=0)

    var max_diff: Float32 = 0.0
    for i in range(p.vocab_size):
        var diff = logits_orig.get(i) - logits_mixed.get(i)
        if diff < 0.0:
            diff = -diff
        if diff > max_diff:
            max_diff = diff

    _assert(
        max_diff < 1e-5,
        "All-FP32 mixed should match Model (max_diff=" + String(max_diff) + ")",
    )

    print("PASS: test_fp32_forward_matches_model")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    test_layer_sensitivity_creation()
    test_quant_roundtrip_error()
    test_measure_layer_sensitivity()
    test_q8_error_less_than_q4()
    test_analyze_all_layers()
    test_auto_calibrate_all_q4()
    test_auto_calibrate_all_q8()
    test_auto_calibrate_mixed()
    test_mixed_model_creation()
    test_quantize_mixed_fp32()
    test_quantize_mixed_q8_q4()
    test_mixed_model_forward()
    test_mixed_generate()
    test_mixed_all_q8_matches_quantized()
    test_mode_summary()
    test_fp32_forward_matches_model()

    print("All 16 mixed_quant tests passed!")
