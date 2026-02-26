# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Package
# ===----------------------------------------------------------------------=== #

"""Model configuration, weight loading, and architecture utilities."""

from .architecture import (
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

from .config import (
    ActivationType,
    ACT_SILU,
    ACT_GELU,
    ACT_RELU,
    ACT_SWIGLU,
    RoPEConfig,
    ModelConfig,
    llama3_8b_config,
    llama3_70b_config,
    mistral_7b_config,
    tinyllama_1_1b_config,
    mini_tinyllama_config,
    layer_weight_name,
    embed_weight_name,
    final_norm_weight_name,
    lm_head_weight_name,
)

from .loader import (
    FileFormat,
    FMT_SAFETENSORS,
    FMT_GGUF,
    FMT_UNKNOWN,
    detect_format,
    WeightDescriptor,
    WeightIndex,
    register_safetensors_weight,
    register_gguf_weight,
    validate_weights_for_model,
)

from .populate import (
    model_from_config,
    load_named_weight,
    normalize_weight_name,
    set_embed,
    set_lm_head,
    set_final_norm,
    set_layer_projection,
)

from .weight_reader import (
    read_tensor_f32,
    read_tensor_f16_as_f32,
    read_tensor_q8_0_as_f32,
    read_tensor_q4_0_as_f32,
    load_gguf_model,
    load_gguf_model_from_buffer,
    load_gguf_quantized,
    QuantizedTensorData,
    read_tensor_q8_0_as_quantized,
    load_gguf_quantized_direct,
    load_gguf_quantized_direct_from_buffer,
    load_gguf_model_mmap,
    load_gguf_quantized_mmap,
    load_gguf_quantized_direct_mmap,
    load_safetensors_model,
    load_safetensors_sharded,
    load_safetensors_from_buffer,
)
