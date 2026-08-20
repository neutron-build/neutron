# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Architecture Registry
# ===----------------------------------------------------------------------=== #

"""Model architecture abstraction for multi-architecture support.

Defines architecture-specific configuration (sliding window, partial rotary,
activation function) so the forward pass can dispatch correctly without
duplicating code for each model family.
"""


# ===----------------------------------------------------------------------=== #
# ArchitectureKind — Enum of supported architectures
# ===----------------------------------------------------------------------=== #

struct ArchitectureKind(Writable, TrivialRegisterPassable):
    """Enum for supported model architectures."""
    var _value: Int

    comptime Llama = ArchitectureKind(0)
    comptime Mistral = ArchitectureKind(1)
    comptime Phi = ArchitectureKind(2)
    comptime Gemma = ArchitectureKind(3)
    comptime Qwen = ArchitectureKind(4)

    @implicit
    def __init__(out self, value: Int):
        self._value = value

    def __eq__(self, other: ArchitectureKind) -> Bool:
        return self._value == other._value

    def __ne__(self, other: ArchitectureKind) -> Bool:
        return self._value != other._value

    def write_to(self, mut writer: Some[Writer]):
        if self._value == 0:
            writer.write("Llama")
        elif self._value == 1:
            writer.write("Mistral")
        elif self._value == 2:
            writer.write("Phi")
        elif self._value == 3:
            writer.write("Gemma")
        elif self._value == 4:
            writer.write("Qwen")
        else:
            writer.write("Unknown")

    def name(self) -> String:
        if self._value == 0:
            return "Llama"
        elif self._value == 1:
            return "Mistral"
        elif self._value == 2:
            return "Phi"
        elif self._value == 3:
            return "Gemma"
        elif self._value == 4:
            return "Qwen"
        return "Unknown"


# ===----------------------------------------------------------------------=== #
# ArchitectureConfig — Per-architecture settings
# ===----------------------------------------------------------------------=== #

struct ArchitectureConfig(Copyable, Movable, ImplicitlyCopyable):
    """Architecture-specific configuration.

    Controls how the forward pass dispatches for different model families:
    - Mistral: sliding window attention
    - Phi: partial rotary embeddings + GeLU activation
    - Gemma: different norm scaling (pre-multiply by sqrt(hidden_dim))
    - Qwen: similar to Llama with different defaults
    """
    var kind: ArchitectureKind
    var use_sliding_window: Bool
    var window_size: Int
    var partial_rotary_factor: Float32  # 1.0 = full, 0.5 = half
    var use_gelu: Bool
    var use_pre_norm_bias: Bool
    var rope_scaling: Float32  # 1.0 = no scaling
    var norm_eps: Float32

    def __init__(out self):
        """Default: Llama-like config."""
        self.kind = ArchitectureKind.Llama
        self.use_sliding_window = False
        self.window_size = 0
        self.partial_rotary_factor = 1.0
        self.use_gelu = False
        self.use_pre_norm_bias = False
        self.rope_scaling = 1.0
        self.norm_eps = 1e-6

    def __init__(out self, *, copy: Self):
        self.kind = copy.kind
        self.use_sliding_window = copy.use_sliding_window
        self.window_size = copy.window_size
        self.partial_rotary_factor = copy.partial_rotary_factor
        self.use_gelu = copy.use_gelu
        self.use_pre_norm_bias = copy.use_pre_norm_bias
        self.rope_scaling = copy.rope_scaling
        self.norm_eps = copy.norm_eps

    def __init__(out self, *, deinit move: Self):
        self.kind = move.kind^
        self.use_sliding_window = move.use_sliding_window^
        self.window_size = move.window_size^
        self.partial_rotary_factor = move.partial_rotary_factor^
        self.use_gelu = move.use_gelu^
        self.use_pre_norm_bias = move.use_pre_norm_bias^
        self.rope_scaling = move.rope_scaling^
        self.norm_eps = move.norm_eps^

    def copy(self) -> ArchitectureConfig:
        var c = ArchitectureConfig()
        c.kind = self.kind
        c.use_sliding_window = self.use_sliding_window
        c.window_size = self.window_size
        c.partial_rotary_factor = self.partial_rotary_factor
        c.use_gelu = self.use_gelu
        c.use_pre_norm_bias = self.use_pre_norm_bias
        c.rope_scaling = self.rope_scaling
        c.norm_eps = self.norm_eps
        return c^


# ===----------------------------------------------------------------------=== #
# Factory functions
# ===----------------------------------------------------------------------=== #

def llama_arch() -> ArchitectureConfig:
    """Standard Llama architecture config."""
    return ArchitectureConfig()


def mistral_arch(window_size: Int = 4096) -> ArchitectureConfig:
    """Mistral architecture config with sliding window attention."""
    var c = ArchitectureConfig()
    c.kind = ArchitectureKind.Mistral
    c.use_sliding_window = True
    c.window_size = window_size
    return c^


def phi_arch(partial_rotary_factor: Float32 = 0.5) -> ArchitectureConfig:
    """Phi architecture config with partial rotary + GeLU."""
    var c = ArchitectureConfig()
    c.kind = ArchitectureKind.Phi
    c.use_gelu = True
    c.partial_rotary_factor = partial_rotary_factor
    c.use_pre_norm_bias = True
    return c^


def gemma_arch() -> ArchitectureConfig:
    """Gemma architecture config."""
    var c = ArchitectureConfig()
    c.kind = ArchitectureKind.Gemma
    c.norm_eps = 1e-6
    return c^


def qwen_arch() -> ArchitectureConfig:
    """Qwen architecture config (Llama-like)."""
    var c = ArchitectureConfig()
    c.kind = ArchitectureKind.Qwen
    return c^


def arch_from_name(name: String) -> ArchitectureConfig:
    """Create an ArchitectureConfig from a name string.

    Supported names: llama, mistral, phi, gemma, qwen.
    Unknown names default to Llama.
    """
    if name == "llama" or name == "Llama" or name == "LlamaForCausalLM":
        return llama_arch()
    elif name == "mistral" or name == "Mistral" or name == "MistralForCausalLM":
        return mistral_arch()
    elif name == "phi" or name == "Phi" or name == "PhiForCausalLM" or name == "phi3":
        return phi_arch()
    elif name == "gemma" or name == "Gemma" or name == "GemmaForCausalLM":
        return gemma_arch()
    elif name == "qwen" or name == "Qwen" or name == "Qwen2ForCausalLM":
        return qwen_arch()
    # Default to Llama
    return llama_arch()


def detect_architecture(arch_name: String, has_sliding_window: Bool, sw_size: Int) -> ArchitectureConfig:
    """Auto-detect architecture from metadata.

    Args:
        arch_name: Architecture name from model metadata (e.g., GGUF general.architecture).
        has_sliding_window: Whether sliding window metadata is present.
        sw_size: Sliding window size from metadata.

    Returns:
        Detected ArchitectureConfig.
    """
    var config = arch_from_name(arch_name)

    # Override sliding window if metadata says so
    if has_sliding_window and sw_size > 0:
        config.use_sliding_window = True
        config.window_size = sw_size

    return config^
