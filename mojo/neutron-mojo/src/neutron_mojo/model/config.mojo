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

struct ActivationType(Writable, Copyable, Movable, ImplicitlyCopyable):
    """Activation function type."""
    var _value: Int

    @implicit
    def __init__(out self, value: Int):
        self._value = value

    def __init__(out self, *, copy: Self):
        self._value = copy._value

    def __eq__(self, other: ActivationType) -> Bool:
        return self._value == other._value

    def __ne__(self, other: ActivationType) -> Bool:
        return self._value != other._value

    def write_to(self, mut writer: Some[Writer]):
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


def ACT_SILU() -> ActivationType:
    return ActivationType(0)

def ACT_GELU() -> ActivationType:
    return ActivationType(1)

def ACT_RELU() -> ActivationType:
    return ActivationType(2)

def ACT_SWIGLU() -> ActivationType:
    return ActivationType(3)


# ===----------------------------------------------------------------------=== #
# RoPE Configuration
# ===----------------------------------------------------------------------=== #

struct RoPEConfig(Copyable, ImplicitlyCopyable):
    """Rotary Position Embedding configuration."""
    var theta: Float64          # Base frequency (default 10000.0)
    var max_position: Int       # Maximum sequence length for RoPE
    var scaling_factor: Float64 # For extended context (YaRN, etc.)
    var scaling_type: String    # "linear", "dynamic", "yarn", "none"

    def __init__(out self):
        self.theta = 10000.0
        self.max_position = 8192
        self.scaling_factor = 1.0
        self.scaling_type = String("none")

    def __init__(out self, *, copy: Self):
        self.theta = copy.theta
        self.max_position = copy.max_position
        self.scaling_factor = copy.scaling_factor
        self.scaling_type = copy.scaling_type


# ===----------------------------------------------------------------------=== #
# Model Configuration
# ===----------------------------------------------------------------------=== #

struct ModelConfig(Copyable, ImplicitlyCopyable):
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

    def __init__(out self):
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

    def __init__(out self, *, copy: Self):
        self.model_type = copy.model_type
        self.architecture = copy.architecture
        self.vocab_size = copy.vocab_size
        self.hidden_size = copy.hidden_size
        self.intermediate_size = copy.intermediate_size
        self.num_hidden_layers = copy.num_hidden_layers
        self.num_attention_heads = copy.num_attention_heads
        self.num_key_value_heads = copy.num_key_value_heads
        self.head_dim = copy.head_dim
        self.rms_norm_eps = copy.rms_norm_eps
        self.hidden_act = copy.hidden_act.copy()
        self.max_position_embeddings = copy.max_position_embeddings
        self.rope = copy.rope.copy()
        self.bos_token_id = copy.bos_token_id
        self.eos_token_id = copy.eos_token_id
        self.pad_token_id = copy.pad_token_id
        self.is_quantized = copy.is_quantized
        self.quant_method = copy.quant_method

    def is_gqa(self) -> Bool:
        """Check if model uses Grouped Query Attention."""
        return self.num_key_value_heads < self.num_attention_heads

    def kv_group_size(self) -> Int:
        """Get number of query heads per KV head."""
        return self.num_attention_heads // self.num_key_value_heads

    def total_params_estimate(self) -> Int:
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

def llama3_8b_config() -> ModelConfig:
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


def llama3_70b_config() -> ModelConfig:
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


def tinyllama_1_1b_config() -> ModelConfig:
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


def mini_tinyllama_config() -> ModelConfig:
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


def mistral_7b_config() -> ModelConfig:
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

def layer_weight_name(layer_idx: Int, suffix: String) -> String:
    """Generate standard HuggingFace layer weight name.

    Args:
        layer_idx: Layer index.
        suffix: Weight suffix (e.g., "self_attn.q_proj.weight").

    Returns:
        Full weight name (e.g., "model.layers.12.self_attn.q_proj.weight").
    """
    return "model.layers." + String(layer_idx) + "." + suffix


def embed_weight_name() -> String:
    """Get embedding weight name."""
    return "model.embed_tokens.weight"


def final_norm_weight_name() -> String:
    """Get final layer norm weight name."""
    return "model.norm.weight"


def lm_head_weight_name() -> String:
    """Get language model head weight name."""
    return "lm_head.weight"
