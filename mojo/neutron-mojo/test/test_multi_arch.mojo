# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Multi-Architecture Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Mistral, Phi, and Gemma architecture support."""

from neutron_mojo.model.architecture import (
    ArchitectureKind, ArchitectureConfig,
    llama_arch, mistral_arch, phi_arch, gemma_arch, qwen_arch,
)
from neutron_mojo.nn.model import ModelParams, Model, tiny_test_params
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model
from neutron_mojo.nn.mixed_quant import MixedQuantModel, quantize_mixed, auto_calibrate
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn make_tiny_model(arch: ArchitectureConfig) raises -> Model:
    """Create a tiny model with a specific architecture."""
    var p = tiny_test_params()
    p.arch = arch.copy()
    return Model(p)


# ===----------------------------------------------------------------------=== #
# Llama (baseline)
# ===----------------------------------------------------------------------=== #

fn test_llama_forward() raises:
    """Llama forward pass produces valid logits."""
    var model = make_tiny_model(llama_arch())
    var p = model.params.copy()
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits = model.forward(0, cache, rope, 0)
    assert_true(logits.numel() == p.vocab_size, "Logits size correct")
    # Check logits are finite
    var has_nan = False
    for i in range(logits.numel()):
        var v = logits.get(i)
        if v != v:  # NaN check
            has_nan = True
    assert_true(has_nan == False, "Llama logits should not be NaN")
    print("  llama_forward: PASS")


fn test_llama_generates_tokens() raises:
    """Llama generates multiple tokens without error."""
    var model = make_tiny_model(llama_arch())
    var p = model.params.copy()
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    for pos in range(5):
        _ = model.forward(0, cache, rope, pos)
    print("  llama_generates_tokens: PASS")


# ===----------------------------------------------------------------------=== #
# Phi (GeLU + partial rotary)
# ===----------------------------------------------------------------------=== #

fn test_phi_forward() raises:
    """Phi forward pass with GeLU activation."""
    var model = make_tiny_model(phi_arch(0.5))
    var p = model.params.copy()
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits = model.forward(0, cache, rope, 0)
    assert_true(logits.numel() == p.vocab_size, "Phi logits size correct")
    print("  phi_forward: PASS")


fn test_phi_partial_rotary() raises:
    """Phi partial rotary config is set correctly and forward pass works at multiple positions."""
    var p = tiny_test_params()
    p.arch = phi_arch(0.5)
    assert_true(p.arch.partial_rotary_factor == 0.5, "Partial rotary factor")
    var rotary_dim = Int(Float32(p.head_dim) * p.arch.partial_rotary_factor)
    assert_true(rotary_dim == 1, "Rotary dim should be half of head_dim=2")

    var model = Model(p)
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    # Run forward at multiple positions to exercise the partial rotary code
    for pos in range(3):
        var logits = model.forward(0, cache, rope, pos)
        assert_true(logits.numel() == p.vocab_size, "Logits size at pos " + String(pos))
    print("  phi_partial_rotary: PASS")


fn test_phi_generates_tokens() raises:
    """Phi generates multiple tokens without error."""
    var model = make_tiny_model(phi_arch(0.5))
    var p = model.params.copy()
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    for pos in range(5):
        _ = model.forward(0, cache, rope, pos)
    print("  phi_generates_tokens: PASS")


# ===----------------------------------------------------------------------=== #
# Gemma
# ===----------------------------------------------------------------------=== #

fn test_gemma_forward() raises:
    """Gemma forward pass."""
    var model = make_tiny_model(gemma_arch())
    var p = model.params.copy()
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits = model.forward(0, cache, rope, 0)
    assert_true(logits.numel() == p.vocab_size, "Gemma logits size correct")
    print("  gemma_forward: PASS")


# ===----------------------------------------------------------------------=== #
# Mistral (sliding window config present, uses standard attention in test)
# ===----------------------------------------------------------------------=== #

fn test_mistral_config() raises:
    """Mistral config has sliding window set."""
    var arch = mistral_arch(4096)
    var p = tiny_test_params()
    p.arch = arch.copy()
    assert_true(p.arch.use_sliding_window == True, "Mistral has sliding window")
    assert_true(p.arch.window_size == 4096, "Window size correct")
    print("  mistral_config: PASS")


fn test_mistral_forward() raises:
    """Mistral forward pass (uses standard attention path for tiny model)."""
    var model = make_tiny_model(mistral_arch(4096))
    var p = model.params.copy()
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits = model.forward(0, cache, rope, 0)
    assert_true(logits.numel() == p.vocab_size, "Mistral logits size correct")
    print("  mistral_forward: PASS")


# ===----------------------------------------------------------------------=== #
# Q8 variants
# ===----------------------------------------------------------------------=== #

fn test_q8_phi_forward() raises:
    """Q8 model with Phi architecture."""
    var p = tiny_test_params()
    p.arch = phi_arch(0.5)
    var fp32_model = Model(p)
    var q8_model = quantize_from_model(fp32_model)
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits = q8_model.forward(0, cache, rope, 0)
    assert_true(logits.numel() == p.vocab_size, "Q8 Phi logits size correct")
    print("  q8_phi_forward: PASS")


fn test_q8_gemma_forward() raises:
    """Q8 model with Gemma architecture."""
    var p = tiny_test_params()
    p.arch = gemma_arch()
    var fp32_model = Model(p)
    var q8_model = quantize_from_model(fp32_model)
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits = q8_model.forward(0, cache, rope, 0)
    assert_true(logits.numel() == p.vocab_size, "Q8 Gemma logits size correct")
    print("  q8_gemma_forward: PASS")


fn test_arch_name_roundtrip() raises:
    """Architecture name -> config -> kind roundtrip."""
    var names = List[String]()
    names.append("llama")
    names.append("mistral")
    names.append("phi")
    names.append("gemma")
    names.append("qwen")

    var expected = List[Int]()
    expected.append(0)
    expected.append(1)
    expected.append(2)
    expected.append(3)
    expected.append(4)

    from neutron_mojo.model.architecture import arch_from_name
    for i in range(len(names)):
        var config = arch_from_name(names[i])
        assert_true(config.kind._value == expected[i],
                    "Name roundtrip for " + names[i])
    print("  arch_name_roundtrip: PASS")


fn main() raises:
    print("test_multi_arch")
    test_llama_forward()
    test_llama_generates_tokens()
    test_phi_forward()
    test_phi_partial_rotary()
    test_phi_generates_tokens()
    test_gemma_forward()
    test_mistral_config()
    test_mistral_forward()
    test_q8_phi_forward()
    test_q8_gemma_forward()
    test_arch_name_roundtrip()
    print("All 11 multi-arch tests passed!")
