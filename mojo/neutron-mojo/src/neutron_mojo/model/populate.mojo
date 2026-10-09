# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Weight Population
# ===----------------------------------------------------------------------=== #

"""Functions to populate a Model struct from named weight tensors.

Bridges the gap between weight loading (GGUF/SafeTensors parsed metadata)
and the Model struct's flat storage layout. Supports HuggingFace naming
conventions for Llama-style architectures.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, LayerWeightOffsets
from neutron_mojo.model.config import ModelConfig, ACT_GELU, ACT_SILU, ACT_SWIGLU
from neutron_mojo.model.architecture import detect_architecture, arch_from_name


# ===----------------------------------------------------------------------=== #
# Config → Model
# ===----------------------------------------------------------------------=== #

def params_from_config(config: ModelConfig) raises -> ModelParams:
    """Create a Model with dimensions matching a ModelConfig.

    Auto-detects architecture from config.model_type and sets ModelParams.arch
    accordingly (e.g., Llama, Mistral with sliding window, Phi with GeLU).

    Args:
        config: Model configuration (Llama-3, Mistral, etc.).

    Returns:
        Model with correct architecture dimensions, weights initialized to defaults.
    """
    var p = ModelParams()
    p.num_layers = config.num_hidden_layers
    p.vocab_size = config.vocab_size
    p.hidden_dim = config.hidden_size
    p.num_q_heads = config.num_attention_heads
    p.num_kv_heads = config.num_key_value_heads
    p.head_dim = config.head_dim
    p.ffn_dim = config.intermediate_size
    p.max_seq_len = config.max_position_embeddings
    p.rope_theta = config.rope.theta

    # Auto-detect architecture from model_type string (e.g., "llama", "mistral")
    p.arch = arch_from_name(config.model_type)

    p.arch.norm_eps = Float32(config.rms_norm_eps)
    if config.hidden_act == ACT_GELU():
        p.arch.use_gelu = True
    elif config.hidden_act == ACT_SILU() or config.hidden_act == ACT_SWIGLU():
        p.arch.use_gelu = False
    else:
        raise Error("Model execution does not support this configured activation")
    if config.rope.scaling_type != "none" or config.rope.scaling_factor != 1:
        raise Error("Model execution does not support configured RoPE scaling")
    p.validate()
    return p


def model_from_config(config: ModelConfig) raises -> Model:
    return Model(params_from_config(config))


# ===----------------------------------------------------------------------=== #
# Named Weight Loading
# ===----------------------------------------------------------------------=== #

# ===----------------------------------------------------------------------=== #
# GGUF → HuggingFace Name Mapping
# ===----------------------------------------------------------------------=== #

def normalize_weight_name(name: String) -> String:
    """Map GGUF tensor names to HuggingFace convention.

    Handles both global tensors and per-layer blk.N.* tensors.
    If the name is already in HF format, passes through unchanged.

    Args:
        name: Tensor name (GGUF or HF format).

    Returns:
        HuggingFace-convention name.
    """
    # Global mappings
    if name == "token_embd.weight":
        return String("model.embed_tokens.weight")
    if name == "output_norm.weight":
        return String("model.norm.weight")
    if name == "output.weight":
        return String("lm_head.weight")

    # Per-layer: blk.{N}.suffix -> model.layers.{N}.hf_suffix
    if name.byte_length() > 4 and name[byte=:4] == "blk.":
        # Find the layer number between first and second dot
        var dot2 = -1
        for i in range(4, name.byte_length()):
            if ord(name[byte=i]) == 46:  # '.'
                dot2 = i
                break
        if dot2 < 0:
            return name

        var layer_str = String(name[byte=4:dot2])
        var suffix = String(name[byte=dot2 + 1:])

        var prefix = "model.layers." + layer_str + "."

        if suffix == "attn_norm.weight":
            return prefix + "input_layernorm.weight"
        elif suffix == "attn_q.weight":
            return prefix + "self_attn.q_proj.weight"
        elif suffix == "attn_k.weight":
            return prefix + "self_attn.k_proj.weight"
        elif suffix == "attn_v.weight":
            return prefix + "self_attn.v_proj.weight"
        elif suffix == "attn_output.weight":
            return prefix + "self_attn.o_proj.weight"
        elif suffix == "ffn_norm.weight":
            return prefix + "post_attention_layernorm.weight"
        elif suffix == "ffn_gate.weight":
            return prefix + "mlp.gate_proj.weight"
        elif suffix == "ffn_up.weight":
            return prefix + "mlp.up_proj.weight"
        elif suffix == "ffn_down.weight":
            return prefix + "mlp.down_proj.weight"

    # Pass-through (already HF format or unknown)
    return name


# ===----------------------------------------------------------------------=== #
# Named Weight Loading
# ===----------------------------------------------------------------------=== #

def load_named_weight(
    mut model: Model,
    name: String,
    data: Tensor[DType.float32],
    size: Int,
) raises:
    """Load a single named weight tensor into the correct model position.

    Supports HuggingFace/GGUF naming conventions:
        model.embed_tokens.weight → embed
        model.norm.weight → final_norm
        lm_head.weight → lm_head
        model.layers.{L}.input_layernorm.weight → attn_norm
        model.layers.{L}.self_attn.q_proj.weight → wq
        model.layers.{L}.self_attn.k_proj.weight → wk
        model.layers.{L}.self_attn.v_proj.weight → wv
        model.layers.{L}.self_attn.o_proj.weight → wo
        model.layers.{L}.post_attention_layernorm.weight → ffn_norm
        model.layers.{L}.mlp.gate_proj.weight → w_gate
        model.layers.{L}.mlp.up_proj.weight → w_up
        model.layers.{L}.mlp.down_proj.weight → w_down

    Args:
        model: Model to populate.
        name: Weight name (HuggingFace convention).
        data: Weight data as flat float32 tensor.
        size: Number of elements to copy.
    """
    model.validate_storage()
    var expected = expected_weight_shape(model.params, name)
    if data.ndim() != 1 and data.shape() != Shape(expected.copy()):
        raise Error("Named weight source shape mismatch: " + name)
    var count = Shape(expected.copy()).numel()
    if size != count or data.numel() != count:
        raise Error("Named weight size mismatch: " + name)
    var src = data.data_ptr()
    if name == "model.embed_tokens.weight":
        for i in range(count):
            model.embed.set(i, src[i])
    elif name == "model.norm.weight":
        for i in range(count):
            model.final_norm.set(i, src[i])
    elif name == "lm_head.weight":
        for i in range(count):
            model.lm_head.set(i, src[i])
    else:
        var layer = _extract_layer_idx(name)
        var target = _match_layer_suffix(name, model._layer_offsets(layer))
        if target < 0 or target > model.layer_weights.numel() or count > model.layer_weights.numel() - target:
            raise Error("Named weight target span mismatch")
        for i in range(count):
            model.layer_weights.set(target + i, src[i])


def is_supported_weight_name(name: String) -> Bool:
    """Names consumed by the standard weight loader; auxiliary tensors are skipped."""
    if name == "model.embed_tokens.weight" or name == "model.norm.weight" or name == "lm_head.weight":
        return True
    if not name.startswith("model.layers."):
        return False
    return name.endswith(".input_layernorm.weight") or name.endswith(".post_attention_layernorm.weight") or name.endswith(".self_attn.q_proj.weight") or name.endswith(".self_attn.k_proj.weight") or name.endswith(".self_attn.v_proj.weight") or name.endswith(".self_attn.o_proj.weight") or name.endswith(".mlp.gate_proj.weight") or name.endswith(".mlp.up_proj.weight") or name.endswith(".mlp.down_proj.weight")


def expected_weight_shape(params: ModelParams, name: String) raises -> List[Int]:
    """Exact row-major parameter shape, shared by all loader admission paths."""
    params.validate()
    var dims = List[Int]()
    var h = params.hidden_dim
    if name == "model.embed_tokens.weight" or name == "lm_head.weight":
        dims.append(params.vocab_size)
        dims.append(h)
    elif name == "model.norm.weight":
        dims.append(h)
    elif name.startswith("model.layers."):
        var layer = _extract_layer_idx(name)
        if layer < 0 or layer >= params.num_layers:
            raise Error("Weight layer index out of range")
        var prefix = "model.layers." + String(layer) + "."
        var suffix = String(name[byte=prefix.byte_length():])
        if suffix == "input_layernorm.weight" or suffix == "post_attention_layernorm.weight":
            dims.append(h)
        elif suffix == "self_attn.q_proj.weight":
            dims.append(params.num_q_heads * params.head_dim)
            dims.append(h)
        elif suffix == "self_attn.k_proj.weight" or suffix == "self_attn.v_proj.weight":
            dims.append(params.num_kv_heads * params.head_dim)
            dims.append(h)
        elif suffix == "self_attn.o_proj.weight":
            dims.append(h)
            dims.append(params.num_q_heads * params.head_dim)
        elif suffix == "mlp.gate_proj.weight" or suffix == "mlp.up_proj.weight":
            dims.append(params.ffn_dim)
            dims.append(h)
        elif suffix == "mlp.down_proj.weight":
            dims.append(h)
            dims.append(params.ffn_dim)
        else:
            raise Error("Unknown layer weight: " + name)
    else:
        raise Error("Unknown weight: " + name)
    _ = Shape(dims.copy()).numel()
    return dims^


def _extract_layer_idx(name: String) raises -> Int:
    """Extract layer index from 'model.layers.N.xxx' pattern."""
    var start = 13  # len("model.layers.")
    var end = start
    while end < name.byte_length():
        var c = ord(name[byte=end])
        if c < 48 or c > 57:  # '0'=48, '9'=57
            break
        end += 1
    if end == start:
        raise Error("No layer index in: " + name)
    if end - start > 1 and ord(name[byte=start]) == 48:
        raise Error("Noncanonical layer index in weight name")
    var result = 0
    for i in range(start, end):
        var digit = ord(name[byte=i]) - 48
        if result > (0x7FFFFFFFFFFFFFFF - digit) // 10:
            raise Error("Weight layer index overflow")
        result = result * 10 + digit
    if end >= name.byte_length() or ord(name[byte=end]) != 46:
        raise Error("Malformed layer weight name")
    return result


def _match_layer_suffix(name: String, off: LayerWeightOffsets) -> Int:
    """Match the suffix of a layer weight name to its offset. Returns -1 if no match."""
    if name.endswith("input_layernorm.weight"):
        return off.attn_norm
    elif name.endswith("self_attn.q_proj.weight"):
        return off.wq
    elif name.endswith("self_attn.k_proj.weight"):
        return off.wk
    elif name.endswith("self_attn.v_proj.weight"):
        return off.wv
    elif name.endswith("self_attn.o_proj.weight"):
        return off.wo
    elif name.endswith("post_attention_layernorm.weight"):
        return off.ffn_norm
    elif name.endswith("mlp.gate_proj.weight"):
        return off.w_gate
    elif name.endswith("mlp.up_proj.weight"):
        return off.w_up
    elif name.endswith("mlp.down_proj.weight"):
        return off.w_down
    return -1


# ===----------------------------------------------------------------------=== #
# Direct Weight Setters
# ===----------------------------------------------------------------------=== #

def set_embed(mut model: Model, data: Tensor[DType.float32], size: Int) raises:
    load_named_weight(model, "model.embed_tokens.weight", data, size)


def set_lm_head(mut model: Model, data: Tensor[DType.float32], size: Int) raises:
    load_named_weight(model, "lm_head.weight", data, size)


def set_final_norm(mut model: Model, data: Tensor[DType.float32], size: Int) raises:
    load_named_weight(model, "model.norm.weight", data, size)


def set_layer_projection(mut model: Model, layer: Int, proj: String,
                         data: Tensor[DType.float32], size: Int) raises:
    var suffix = String("")
    if proj == "attn_norm":
        suffix = "input_layernorm.weight"
    elif proj == "ffn_norm":
        suffix = "post_attention_layernorm.weight"
    elif proj == "wq":
        suffix = "self_attn.q_proj.weight"
    elif proj == "wk":
        suffix = "self_attn.k_proj.weight"
    elif proj == "wv":
        suffix = "self_attn.v_proj.weight"
    elif proj == "wo":
        suffix = "self_attn.o_proj.weight"
    elif proj == "w_gate":
        suffix = "mlp.gate_proj.weight"
    elif proj == "w_up":
        suffix = "mlp.up_proj.weight"
    elif proj == "w_down":
        suffix = "mlp.down_proj.weight"
    else:
        raise Error("Unknown projection")
    load_named_weight(model, "model.layers." + String(layer) + "." + suffix, data, size)
