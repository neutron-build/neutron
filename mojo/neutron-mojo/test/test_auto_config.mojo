# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Auto-Config from GGUF Tests
# ===----------------------------------------------------------------------=== #

"""Tests for automatic architecture detection from GGUF metadata."""

from neutron_mojo.io.gguf import (
    GGUFFile, gguf_to_model_config, detect_arch_from_gguf,
)
from neutron_mojo.model.architecture import (
    ArchitectureKind, ArchitectureConfig,
    arch_from_name, detect_architecture,
    llama_arch, mistral_arch, phi_arch,
)
from neutron_mojo.model.populate import model_from_config
from neutron_mojo.model.config import ModelConfig
from neutron_mojo.nn.model import ModelParams, tiny_test_params


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn _make_gguf_with_arch(arch_name: String) -> GGUFFile:
    """Create a mock GGUFFile with the given architecture name."""
    var gguf = GGUFFile()
    gguf.metadata_str["general.architecture"] = arch_name
    return gguf^


fn test_detect_llama() raises:
    """Detect Llama architecture from GGUF metadata."""
    var gguf = _make_gguf_with_arch("llama")
    var arch = detect_arch_from_gguf(gguf)
    assert_true(arch.kind == ArchitectureKind.Llama, "Should detect Llama")
    assert_true(arch.use_sliding_window == False, "Llama has no sliding window")
    assert_true(arch.use_gelu == False, "Llama uses SiLU not GeLU")
    print("  detect_llama: PASS")


fn test_detect_mistral() raises:
    """Detect Mistral architecture from GGUF metadata."""
    var gguf = _make_gguf_with_arch("mistral")
    var arch = detect_arch_from_gguf(gguf)
    assert_true(arch.kind == ArchitectureKind.Mistral, "Should detect Mistral")
    print("  detect_mistral: PASS")


fn test_detect_mistral_with_sliding_window() raises:
    """Detect Mistral with sliding window metadata."""
    var gguf = _make_gguf_with_arch("mistral")
    gguf.metadata_int["mistral.attention.sliding_window"] = 4096
    var arch = detect_arch_from_gguf(gguf)
    assert_true(arch.kind == ArchitectureKind.Mistral, "Should detect Mistral")
    assert_true(arch.use_sliding_window == True, "Should enable sliding window")
    assert_true(arch.window_size == 4096, "Window size should be 4096")
    print("  detect_mistral_with_sliding_window: PASS")


fn test_detect_phi() raises:
    """Detect Phi architecture from GGUF metadata."""
    var gguf = _make_gguf_with_arch("phi")
    var arch = detect_arch_from_gguf(gguf)
    assert_true(arch.kind == ArchitectureKind.Phi, "Should detect Phi")
    assert_true(arch.use_gelu == True, "Phi uses GeLU")
    print("  detect_phi: PASS")


fn test_detect_gemma() raises:
    """Detect Gemma architecture from GGUF metadata."""
    var gguf = _make_gguf_with_arch("gemma")
    var arch = detect_arch_from_gguf(gguf)
    assert_true(arch.kind == ArchitectureKind.Gemma, "Should detect Gemma")
    print("  detect_gemma: PASS")


fn test_detect_qwen() raises:
    """Detect Qwen architecture from GGUF metadata."""
    var gguf = _make_gguf_with_arch("qwen")
    var arch = detect_arch_from_gguf(gguf)
    assert_true(arch.kind == ArchitectureKind.Qwen, "Should detect Qwen")
    print("  detect_qwen: PASS")


fn test_unknown_defaults_to_llama() raises:
    """Unknown architecture defaults to Llama."""
    var gguf = _make_gguf_with_arch("foobar_unknown")
    var arch = detect_arch_from_gguf(gguf)
    assert_true(arch.kind == ArchitectureKind.Llama, "Unknown should default to Llama")
    print("  unknown_defaults_to_llama: PASS")


fn test_missing_arch_defaults_to_llama() raises:
    """Missing general.architecture defaults to Llama."""
    var gguf = GGUFFile()  # No metadata set
    var arch = detect_arch_from_gguf(gguf)
    assert_true(arch.kind == ArchitectureKind.Llama, "Missing arch should default to Llama")
    print("  missing_arch_defaults_to_llama: PASS")


fn test_model_from_config_sets_arch() raises:
    """model_from_config auto-detects arch from model_type."""
    var cfg = ModelConfig()
    cfg.model_type = "mistral"
    cfg.num_hidden_layers = 1
    cfg.vocab_size = 8
    cfg.hidden_size = 4
    cfg.num_attention_heads = 2
    cfg.num_key_value_heads = 1
    cfg.head_dim = 2
    cfg.intermediate_size = 8
    cfg.max_position_embeddings = 32
    var model = model_from_config(cfg)
    assert_true(model.params.arch.kind == ArchitectureKind.Mistral, "model_from_config should detect Mistral")
    print("  model_from_config_sets_arch: PASS")


fn test_gguf_to_model_config_preserves_model_type() raises:
    """gguf_to_model_config sets model_type from GGUF metadata."""
    var gguf = _make_gguf_with_arch("phi")
    var cfg = gguf_to_model_config(gguf)
    assert_true(cfg.model_type == "phi", "model_type should be phi")
    print("  gguf_to_model_config_preserves_model_type: PASS")


fn main() raises:
    print("test_auto_config")
    test_detect_llama()
    test_detect_mistral()
    test_detect_mistral_with_sliding_window()
    test_detect_phi()
    test_detect_gemma()
    test_detect_qwen()
    test_unknown_defaults_to_llama()
    test_missing_arch_defaults_to_llama()
    test_model_from_config_sets_arch()
    test_gguf_to_model_config_preserves_model_type()
    print("All 10 auto-config tests passed!")
