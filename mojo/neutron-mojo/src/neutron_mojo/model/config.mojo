# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Configuration
# ===----------------------------------------------------------------------=== #

"""Model configuration structs for transformer architectures.

Supports Llama-3, Mistral, and similar decoder-only transformer models.
Configurations mirror the HuggingFace config.json format.
"""


# ===----------------------------------------------------------------------=== #
# Activation Type
# ===----------------------------------------------------------------------=== #

struct ActivationType(Writable, Copyable, Movable):
    """Activation function type."""
    var _value: Int

    @implicit
    fn __init__(out self, value: Int):
        self._value = value

    fn __copyinit__(out self, existing: Self):
        self._value = existing._value

    fn __eq__(self, other: ActivationType) -> Bool:
        return self._value == other._value

    fn __ne__(self, other: ActivationType) -> Bool:
        return self._value != other._value

    fn write_to[W: Writer](self, mut writer: W):
        if self._value == 0:
            writer.write("silu")
        elif self._value == 1:
            writer.write("gelu")
        elif self._value == 2:
            writer.write("relu")
        elif self._value == 3:
            writer.write("swiglu")
        else:
            writer.write("unknown")


fn ACT_SILU() -> ActivationType:
    return ActivationType(0)

fn ACT_GELU() -> ActivationType:
    return ActivationType(1)

fn ACT_RELU() -> ActivationType:
    return ActivationType(2)

fn ACT_SWIGLU() -> ActivationType:
    return ActivationType(3)


# ===----------------------------------------------------------------------=== #
# RoPE Configuration
# ===----------------------------------------------------------------------=== #

struct RoPEConfig(Copyable):
    """Rotary Position Embedding configuration."""
    var theta: Float64          # Base frequency (default 10000.0)
    var max_position: Int       # Maximum sequence length for RoPE
    var scaling_factor: Float64 # For extended context (YaRN, etc.)
    var scaling_type: String    # "linear", "dynamic", "yarn", "none"

    fn __init__(out self):
        self.theta = 10000.0
        self.max_position = 8192
        self.scaling_factor = 1.0
        self.scaling_type = String("none")

    fn __copyinit__(out self, existing: Self):
        self.theta = existing.theta
        self.max_position = existing.max_position
        self.scaling_factor = existing.scaling_factor
        self.scaling_type = existing.scaling_type


# ===----------------------------------------------------------------------=== #
# Model Configuration
# ===----------------------------------------------------------------------=== #

struct ModelConfig(Copyable):
    """Configuration for a transformer language model.

    Covers Llama-3, Mistral, and similar architectures.
    Fields mirror HuggingFace config.json.
    """
    # Model identity
    var model_type: String          # "llama", "mistral", etc.
    var architecture: String        # "LlamaForCausalLM", etc.

    # Core dimensions
    var vocab_size: Int             # Vocabulary size
    var hidden_size: Int            # Model hidden dimension (d_model)
    var intermediate_size: Int      # FFN intermediate dimension
    var num_hidden_layers: Int      # Number of transformer layers
    var num_attention_heads: Int    # Number of attention heads
    var num_key_value_heads: Int    # Number of KV heads (for GQA)
    var head_dim: Int               # Per-head dimension

    # Normalization
    var rms_norm_eps: Float64       # RMSNorm epsilon

    # Activation
    var hidden_act: ActivationType  # Activation function

    # Context
    var max_position_embeddings: Int  # Max sequence length

    # RoPE
    var rope: RoPEConfig

    # Tokenizer
    var bos_token_id: Int
    var eos_token_id: Int
    var pad_token_id: Int

    # Quantization
    var is_quantized: Bool
    var quant_method: String        # "gptq", "awq", "gguf", "none"

    fn __init__(out self):
        self.model_type = String("llama")
        self.architecture = String("LlamaForCausalLM")
        self.vocab_size = 32000
        self.hidden_size = 4096
        self.intermediate_size = 11008
        self.num_hidden_layers = 32
        self.num_attention_heads = 32
        self.num_key_value_heads = 32
        self.head_dim = 128
        self.rms_norm_eps = 1e-5
        self.hidden_act = ACT_SILU()
        self.max_position_embeddings = 4096
        self.rope = RoPEConfig()
        self.bos_token_id = 1
        self.eos_token_id = 2
        self.pad_token_id = 0
        self.is_quantized = False
        self.quant_method = String("none")

    fn __copyinit__(out self, existing: Self):
        self.model_type = existing.model_type
        self.architecture = existing.architecture
        self.vocab_size = existing.vocab_size
        self.hidden_size = existing.hidden_size
        self.intermediate_size = existing.intermediate_size
        self.num_hidden_layers = existing.num_hidden_layers
        self.num_attention_heads = existing.num_attention_heads
        self.num_key_value_heads = existing.num_key_value_heads
        self.head_dim = existing.head_dim
        self.rms_norm_eps = existing.rms_norm_eps
        self.hidden_act = existing.hidden_act.copy()
        self.max_position_embeddings = existing.max_position_embeddings
        self.rope = existing.rope.copy()
        self.bos_token_id = existing.bos_token_id
        self.eos_token_id = existing.eos_token_id
        self.pad_token_id = existing.pad_token_id
        self.is_quantized = existing.is_quantized
        self.quant_method = existing.quant_method

    fn is_gqa(self) -> Bool:
        """Check if model uses Grouped Query Attention."""
        return self.num_key_value_heads < self.num_attention_heads

    fn kv_group_size(self) -> Int:
        """Get number of query heads per KV head."""
        return self.num_attention_heads // self.num_key_value_heads

    fn total_params_estimate(self) -> Int:
        """Estimate total parameter count (rough).

        Returns:
            Approximate parameter count.
        """
        # Embedding: vocab_size * hidden_size
        var embed = self.vocab_size * self.hidden_size

        # Per-layer: attention (Q, K, V, O) + FFN (gate, up, down) + norms
        var attn_qo = self.hidden_size * self.hidden_size * 2  # Q + O
        var attn_kv = self.hidden_size * (self.num_key_value_heads * self.head_dim) * 2  # K + V
        var ffn = self.hidden_size * self.intermediate_size * 3  # gate + up + down
        var norms = self.hidden_size * 2  # attn_norm + ffn_norm

        var per_layer = attn_qo + attn_kv + ffn + norms

        # LM head: hidden_size * vocab_size (often tied with embed)
        var lm_head = self.hidden_size * self.vocab_size

        return embed + (per_layer * self.num_hidden_layers) + lm_head


# ===----------------------------------------------------------------------=== #
# Predefined Model Configurations
# ===----------------------------------------------------------------------=== #

fn llama3_8b_config() -> ModelConfig:
    """Llama-3 8B configuration."""
    var cfg = ModelConfig()
    cfg.model_type = String("llama")
    cfg.architecture = String("LlamaForCausalLM")
    cfg.vocab_size = 128256
    cfg.hidden_size = 4096
    cfg.intermediate_size = 14336
    cfg.num_hidden_layers = 32
    cfg.num_attention_heads = 32
    cfg.num_key_value_heads = 8  # GQA with 4:1 ratio
    cfg.head_dim = 128
    cfg.rms_norm_eps = 1e-5
    cfg.hidden_act = ACT_SILU()
    cfg.max_position_embeddings = 8192
    cfg.rope.theta = 500000.0
    cfg.rope.max_position = 8192
    cfg.bos_token_id = 128000
    cfg.eos_token_id = 128001
    return cfg^


fn llama3_70b_config() -> ModelConfig:
    """Llama-3 70B configuration."""
    var cfg = ModelConfig()
    cfg.model_type = String("llama")
    cfg.architecture = String("LlamaForCausalLM")
    cfg.vocab_size = 128256
    cfg.hidden_size = 8192
    cfg.intermediate_size = 28672
    cfg.num_hidden_layers = 80
    cfg.num_attention_heads = 64
    cfg.num_key_value_heads = 8  # GQA with 8:1 ratio
    cfg.head_dim = 128
    cfg.rms_norm_eps = 1e-5
    cfg.hidden_act = ACT_SILU()
    cfg.max_position_embeddings = 8192
    cfg.rope.theta = 500000.0
    cfg.rope.max_position = 8192
    cfg.bos_token_id = 128000
    cfg.eos_token_id = 128001
    return cfg^


fn tinyllama_1_1b_config() -> ModelConfig:
    """TinyLlama-1.1B configuration.

    TinyLlama-1.1B-Chat: 22 layers, hidden=2048, GQA 32:4 (8:1 ratio).
    Uses SiLU activation and standard RoPE with theta=10000.
    """
    var cfg = ModelConfig()
    cfg.model_type = String("llama")
    cfg.architecture = String("LlamaForCausalLM")
    cfg.vocab_size = 32000
    cfg.hidden_size = 2048
    cfg.intermediate_size = 5632
    cfg.num_hidden_layers = 22
    cfg.num_attention_heads = 32
    cfg.num_key_value_heads = 4  # GQA with 8:1 ratio
    cfg.head_dim = 64
    cfg.rms_norm_eps = 1e-5
    cfg.hidden_act = ACT_SILU()
    cfg.max_position_embeddings = 2048
    cfg.rope.theta = 10000.0
    cfg.rope.max_position = 2048
    cfg.bos_token_id = 1
    cfg.eos_token_id = 2
    return cfg^


fn mini_tinyllama_config() -> ModelConfig:
    """Scaled-down TinyLlama config for testing.

    Same architecture ratios as TinyLlama-1.1B but with dimensions
    small enough for in-memory testing: hidden=64, 2 layers, vocab=256.
    GQA ratio preserved at 8:2 = 4:1 (matching TinyLlama's 32:4).
    """
    var cfg = ModelConfig()
    cfg.model_type = String("llama")
    cfg.architecture = String("LlamaForCausalLM")
    cfg.vocab_size = 256
    cfg.hidden_size = 64
    cfg.intermediate_size = 128
    cfg.num_hidden_layers = 2
    cfg.num_attention_heads = 8
    cfg.num_key_value_heads = 2  # GQA with 4:1 ratio
    cfg.head_dim = 8
    cfg.rms_norm_eps = 1e-5
    cfg.hidden_act = ACT_SILU()
    cfg.max_position_embeddings = 128
    cfg.rope.theta = 10000.0
    cfg.rope.max_position = 128
    cfg.bos_token_id = 1
    cfg.eos_token_id = 2
    return cfg^


fn mistral_7b_config() -> ModelConfig:
    """Mistral 7B configuration."""
    var cfg = ModelConfig()
    cfg.model_type = String("mistral")
    cfg.architecture = String("MistralForCausalLM")
    cfg.vocab_size = 32000
    cfg.hidden_size = 4096
    cfg.intermediate_size = 14336
    cfg.num_hidden_layers = 32
    cfg.num_attention_heads = 32
    cfg.num_key_value_heads = 8  # GQA
    cfg.head_dim = 128
    cfg.rms_norm_eps = 1e-5
    cfg.hidden_act = ACT_SILU()
    cfg.max_position_embeddings = 32768
    cfg.rope.theta = 10000.0
    cfg.rope.max_position = 32768
    cfg.bos_token_id = 1
    cfg.eos_token_id = 2
    return cfg^


# ===----------------------------------------------------------------------=== #
# Layer Weight Names
# ===----------------------------------------------------------------------=== #

fn layer_weight_name(layer_idx: Int, suffix: String) -> String:
    """Generate standard HuggingFace layer weight name.

    Args:
        layer_idx: Layer index.
        suffix: Weight suffix (e.g., "self_attn.q_proj.weight").

    Returns:
        Full weight name (e.g., "model.layers.12.self_attn.q_proj.weight").
    """
    return "model.layers." + String(layer_idx) + "." + suffix


fn embed_weight_name() -> String:
    """Get embedding weight name."""
    return "model.embed_tokens.weight"


fn final_norm_weight_name() -> String:
    """Get final layer norm weight name."""
    return "model.norm.weight"


fn lm_head_weight_name() -> String:
    """Get language model head weight name."""
    return "lm_head.weight"
