# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Configuration Tests
# ===----------------------------------------------------------------------=== #

"""Tests for model configuration structs."""

from neutron_mojo.model.config import (
    ActivationType,
    ACT_SILU,
    ACT_GELU,
    RoPEConfig,
    ModelConfig,
    llama3_8b_config,
    llama3_70b_config,
    mistral_7b_config,
    layer_weight_name,
    embed_weight_name,
    final_norm_weight_name,
    lm_head_weight_name,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_activation_type() raises:
    """Test activation type enum."""
    var silu = ACT_SILU()
    var gelu = ACT_GELU()

    assert_true(silu == ACT_SILU(), "SILU should match")
    assert_true(silu != gelu, "SILU and GELU should differ")

    print("  activation_type: PASS")


fn test_rope_config_defaults() raises:
    """Test RoPE config defaults."""
    var rope = RoPEConfig()

    assert_true(rope.theta == 10000.0, "Default theta should be 10000.0")
    assert_true(rope.max_position == 8192, "Default max_position should be 8192")
    assert_true(rope.scaling_factor == 1.0, "Default scaling_factor should be 1.0")
    assert_true(rope.scaling_type == "none", "Default scaling_type should be none")

    print("  rope_config_defaults: PASS")


fn test_model_config_defaults() raises:
    """Test ModelConfig default construction."""
    var cfg = ModelConfig()

    assert_true(cfg.model_type == "llama", "Default model_type should be llama")
    assert_true(cfg.vocab_size == 32000, "Default vocab_size should be 32000")
    assert_true(cfg.hidden_size == 4096, "Default hidden_size should be 4096")
    assert_true(cfg.num_hidden_layers == 32, "Default layers should be 32")
    assert_true(cfg.num_attention_heads == 32, "Default heads should be 32")
    assert_true(not cfg.is_quantized, "Default should not be quantized")

    print("  model_config_defaults: PASS")


fn test_llama3_8b_config() raises:
    """Test Llama-3 8B config."""
    var cfg = llama3_8b_config()

    assert_true(cfg.vocab_size == 128256, "Llama-3 vocab should be 128256")
    assert_true(cfg.hidden_size == 4096, "Llama-3 8B hidden should be 4096")
    assert_true(cfg.intermediate_size == 14336, "Llama-3 8B intermediate should be 14336")
    assert_true(cfg.num_hidden_layers == 32, "Llama-3 8B should have 32 layers")
    assert_true(cfg.num_attention_heads == 32, "Llama-3 8B should have 32 heads")
    assert_true(cfg.num_key_value_heads == 8, "Llama-3 8B should have 8 KV heads")
    assert_true(cfg.head_dim == 128, "Llama-3 8B head_dim should be 128")
    assert_true(cfg.rope.theta == 500000.0, "Llama-3 rope theta should be 500000")
    assert_true(cfg.bos_token_id == 128000, "Llama-3 BOS should be 128000")

    print("  llama3_8b_config: PASS")


fn test_llama3_70b_config() raises:
    """Test Llama-3 70B config."""
    var cfg = llama3_70b_config()

    assert_true(cfg.hidden_size == 8192, "Llama-3 70B hidden should be 8192")
    assert_true(cfg.intermediate_size == 28672, "Llama-3 70B intermediate should be 28672")
    assert_true(cfg.num_hidden_layers == 80, "Llama-3 70B should have 80 layers")
    assert_true(cfg.num_attention_heads == 64, "Llama-3 70B should have 64 heads")
    assert_true(cfg.num_key_value_heads == 8, "Llama-3 70B should have 8 KV heads")

    print("  llama3_70b_config: PASS")


fn test_mistral_7b_config() raises:
    """Test Mistral 7B config."""
    var cfg = mistral_7b_config()

    assert_true(cfg.model_type == "mistral", "Should be mistral type")
    assert_true(cfg.vocab_size == 32000, "Mistral vocab should be 32000")
    assert_true(cfg.hidden_size == 4096, "Mistral hidden should be 4096")
    assert_true(cfg.num_key_value_heads == 8, "Mistral should have 8 KV heads")
    assert_true(cfg.max_position_embeddings == 32768, "Mistral max_pos should be 32768")

    print("  mistral_7b_config: PASS")


fn test_gqa_detection() raises:
    """Test GQA detection."""
    var llama3 = llama3_8b_config()
    var default_cfg = ModelConfig()

    assert_true(llama3.is_gqa(), "Llama-3 should use GQA")
    assert_true(not default_cfg.is_gqa(), "Default config should not use GQA")

    assert_true(llama3.kv_group_size() == 4, "Llama-3 8B group size should be 4")

    print("  gqa_detection: PASS")


fn test_param_estimate() raises:
    """Test parameter estimation."""
    var cfg = llama3_8b_config()
    var params = cfg.total_params_estimate()

    # Llama-3 8B has ~8B params
    assert_true(params > 7_000_000_000, "Llama-3 8B should have > 7B params")
    assert_true(params < 10_000_000_000, "Llama-3 8B should have < 10B params")

    print("  param_estimate: PASS")


fn test_layer_weight_names() raises:
    """Test weight name generation."""
    var name0 = layer_weight_name(0, "self_attn.q_proj.weight")
    var name5 = layer_weight_name(5, "mlp.gate_proj.weight")

    assert_true(name0 == "model.layers.0.self_attn.q_proj.weight", "Layer 0 Q proj name")
    assert_true(name5 == "model.layers.5.mlp.gate_proj.weight", "Layer 5 gate proj name")

    assert_true(embed_weight_name() == "model.embed_tokens.weight", "Embed weight name")
    assert_true(final_norm_weight_name() == "model.norm.weight", "Final norm name")
    assert_true(lm_head_weight_name() == "lm_head.weight", "LM head name")

    print("  layer_weight_names: PASS")


fn test_model_config_copy() raises:
    """Test ModelConfig copy semantics."""
    var original = llama3_8b_config()
    var copied = original.copy()

    assert_true(copied.vocab_size == original.vocab_size, "Copy should preserve vocab_size")
    assert_true(copied.hidden_size == original.hidden_size, "Copy should preserve hidden_size")
    assert_true(copied.rope.theta == original.rope.theta, "Copy should preserve rope theta")

    print("  model_config_copy: PASS")


fn main() raises:
    print("test_model_config:")

    test_activation_type()
    test_rope_config_defaults()
    test_model_config_defaults()
    test_llama3_8b_config()
    test_llama3_70b_config()
    test_mistral_7b_config()
    test_gqa_detection()
    test_param_estimate()
    test_layer_weight_names()
    test_model_config_copy()

    print("ALL PASSED")
