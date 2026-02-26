# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Architecture Registry Tests
# ===----------------------------------------------------------------------=== #

"""Tests for model architecture abstraction."""

from neutron_mojo.model.architecture import (
    ArchitectureKind,
    ArchitectureConfig,
    llama_arch,
    mistral_arch,
    phi_arch,
    gemma_arch,
    qwen_arch,
    arch_from_name,
    detect_architecture,
)
from neutron_mojo.nn.model import ModelParams, Model, tiny_test_params
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_architecture_kind() raises:
    """Architecture kind enum values."""
    assert_true(ArchitectureKind.Llama != ArchitectureKind.Mistral, "Llama != Mistral")
    assert_true(ArchitectureKind.Llama == ArchitectureKind.Llama, "Llama == Llama")
    assert_true(ArchitectureKind.Phi != ArchitectureKind.Gemma, "Phi != Gemma")
    assert_true(ArchitectureKind.Llama.name() == "Llama", "Llama name")
    assert_true(ArchitectureKind.Mistral.name() == "Mistral", "Mistral name")
    print("  architecture_kind: PASS")


fn test_llama_config() raises:
    """Default Llama config."""
    var c = llama_arch()
    assert_true(c.kind == ArchitectureKind.Llama, "Should be Llama")
    assert_true(c.use_sliding_window == False, "No sliding window")
    assert_true(c.use_gelu == False, "Uses SiLU not GeLU")
    assert_true(c.partial_rotary_factor == 1.0, "Full rotary")
    print("  llama_config: PASS")


fn test_mistral_config() raises:
    """Mistral config with sliding window."""
    var c = mistral_arch(4096)
    assert_true(c.kind == ArchitectureKind.Mistral, "Should be Mistral")
    assert_true(c.use_sliding_window == True, "Has sliding window")
    assert_true(c.window_size == 4096, "Window size 4096")
    assert_true(c.use_gelu == False, "Uses SiLU")
    print("  mistral_config: PASS")


fn test_phi_config() raises:
    """Phi config with partial rotary + GeLU."""
    var c = phi_arch(0.5)
    assert_true(c.kind == ArchitectureKind.Phi, "Should be Phi")
    assert_true(c.use_gelu == True, "Uses GeLU")
    assert_true(c.partial_rotary_factor == 0.5, "Half rotary")
    assert_true(c.use_pre_norm_bias == True, "Has pre-norm bias")
    print("  phi_config: PASS")


fn test_gemma_config() raises:
    """Gemma config."""
    var c = gemma_arch()
    assert_true(c.kind == ArchitectureKind.Gemma, "Should be Gemma")
    assert_true(c.use_sliding_window == False, "No sliding window")
    assert_true(c.use_gelu == False, "Uses SiLU")
    print("  gemma_config: PASS")


fn test_qwen_config() raises:
    """Qwen config."""
    var c = qwen_arch()
    assert_true(c.kind == ArchitectureKind.Qwen, "Should be Qwen")
    assert_true(c.use_gelu == False, "Uses SiLU")
    print("  qwen_config: PASS")


fn test_from_name_llama() raises:
    """arch_from_name for Llama variants."""
    var c1 = arch_from_name("llama")
    assert_true(c1.kind == ArchitectureKind.Llama, "llama -> Llama")
    var c2 = arch_from_name("LlamaForCausalLM")
    assert_true(c2.kind == ArchitectureKind.Llama, "LlamaForCausalLM -> Llama")
    print("  from_name_llama: PASS")


fn test_from_name_all() raises:
    """arch_from_name for all architectures."""
    assert_true(arch_from_name("mistral").kind == ArchitectureKind.Mistral, "mistral")
    assert_true(arch_from_name("phi").kind == ArchitectureKind.Phi, "phi")
    assert_true(arch_from_name("gemma").kind == ArchitectureKind.Gemma, "gemma")
    assert_true(arch_from_name("qwen").kind == ArchitectureKind.Qwen, "qwen")
    # Unknown defaults to Llama
    assert_true(arch_from_name("unknown_model").kind == ArchitectureKind.Llama, "unknown -> Llama")
    print("  from_name_all: PASS")


fn test_detect_architecture() raises:
    """detect_architecture from metadata."""
    var c1 = detect_architecture("llama", False, 0)
    assert_true(c1.kind == ArchitectureKind.Llama, "Detected Llama")
    assert_true(c1.use_sliding_window == False, "No SW for Llama")

    var c2 = detect_architecture("mistral", True, 4096)
    assert_true(c2.kind == ArchitectureKind.Mistral, "Detected Mistral")
    assert_true(c2.use_sliding_window == True, "SW enabled")
    assert_true(c2.window_size == 4096, "SW size correct")
    print("  detect_architecture: PASS")


fn test_config_copy() raises:
    """ArchitectureConfig copy preserves all fields."""
    var orig = mistral_arch(2048)
    var copy = orig.copy()
    assert_true(copy.kind == ArchitectureKind.Mistral, "Kind preserved")
    assert_true(copy.use_sliding_window == True, "SW preserved")
    assert_true(copy.window_size == 2048, "Window size preserved")
    print("  config_copy: PASS")


fn test_model_params_has_arch() raises:
    """ModelParams includes arch field."""
    var p = ModelParams()
    assert_true(p.arch.kind == ArchitectureKind.Llama, "Default arch is Llama")

    var p2 = tiny_test_params()
    assert_true(p2.arch.kind == ArchitectureKind.Llama, "tiny_test_params default Llama")
    print("  model_params_has_arch: PASS")


fn test_forward_with_default_arch() raises:
    """Forward pass with default Llama arch still works."""
    var p = tiny_test_params()
    var model = Model(p)
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits = model.forward(0, cache, rope, 0)
    assert_true(logits.numel() == p.vocab_size, "Logits should be vocab_size")
    print("  forward_with_default_arch: PASS")


fn test_forward_with_gelu_arch() raises:
    """Forward pass with GeLU activation (Phi-like)."""
    var p = tiny_test_params()
    p.arch = phi_arch(0.5)
    var model = Model(p)
    var cache = MultiLayerKVCache(p.num_layers, p.max_seq_len, p.num_kv_heads, p.head_dim)
    var rope = RoPETable(p.head_dim, p.max_seq_len, p.rope_theta)
    var logits = model.forward(0, cache, rope, 0)
    assert_true(logits.numel() == p.vocab_size, "Logits should be vocab_size")
    print("  forward_with_gelu_arch: PASS")


fn main() raises:
    print("test_architecture")
    test_architecture_kind()
    test_llama_config()
    test_mistral_config()
    test_phi_config()
    test_gemma_config()
    test_qwen_config()
    test_from_name_llama()
    test_from_name_all()
    test_detect_architecture()
    test_config_copy()
    test_model_params_has_arch()
    test_forward_with_default_arch()
    test_forward_with_gelu_arch()
    print("All 13 architecture tests passed!")
