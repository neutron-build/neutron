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
from neutron_mojo.model.config import ModelConfig
from neutron_mojo.model.architecture import detect_architecture, arch_from_name


# ===----------------------------------------------------------------------=== #
# Config → Model
# ===----------------------------------------------------------------------=== #

fn model_from_config(config: ModelConfig) -> Model:
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

    return Model(p)


# ===----------------------------------------------------------------------=== #
# Named Weight Loading
# ===----------------------------------------------------------------------=== #

# ===----------------------------------------------------------------------=== #
# GGUF → HuggingFace Name Mapping
# ===----------------------------------------------------------------------=== #

fn normalize_weight_name(name: String) -> String:
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
    if len(name) > 4 and name[:4] == "blk.":
        # Find the layer number between first and second dot
        var dot2 = -1
        for i in range(4, len(name)):
            if ord(name[byte=i]) == 46:  # '.'
                dot2 = i
                break
        if dot2 < 0:
            return name

        var layer_str = String(name[4:dot2])
        var suffix = String(name[dot2 + 1:])

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

fn load_named_weight(
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
    if name == "model.embed_tokens.weight":
        for i in range(size):
            model.embed.set(i, data.get(i))
    elif name == "model.norm.weight":
        for i in range(size):
            model.final_norm.set(i, data.get(i))
    elif name == "lm_head.weight":
        for i in range(size):
            model.lm_head.set(i, data.get(i))
    elif len(name) > 13 and name[:13] == "model.layers.":
        # Layer weight — extract layer index and route to projection
        var layer = _extract_layer_idx(name)
        var off = model._layer_offsets(layer)
        var target = _match_layer_suffix(name, off)
        if target < 0:
            raise Error("Unknown layer weight suffix: " + name)
        for i in range(size):
            model.layer_weights.set(target + i, data.get(i))
    else:
        raise Error("Unknown weight name: " + name)


fn _extract_layer_idx(name: String) raises -> Int:
    """Extract layer index from 'model.layers.N.xxx' pattern."""
    var start = 13  # len("model.layers.")
    var end = start
    while end < len(name):
        var c = ord(name[byte=end])
        if c < 48 or c > 57:  # '0'=48, '9'=57
            break
        end += 1
    if end == start:
        raise Error("No layer index in: " + name)
    var result = 0
    for i in range(start, end):
        result = result * 10 + (ord(name[byte=i]) - 48)
    return result


fn _match_layer_suffix(name: String, off: LayerWeightOffsets) -> Int:
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

fn set_embed(mut model: Model, data: Tensor[DType.float32], size: Int):
    """Copy embedding weights into model (flat copy, handles 2D shape)."""
    var src = data.data_ptr()
    for i in range(size):
        model.embed.set(i, src[i])


fn set_lm_head(mut model: Model, data: Tensor[DType.float32], size: Int):
    """Copy LM head weights into model (flat copy, handles 2D shape)."""
    var src = data.data_ptr()
    for i in range(size):
        model.lm_head.set(i, src[i])


fn set_final_norm(mut model: Model, data: Tensor[DType.float32], size: Int):
    """Copy final norm weights into model."""
    for i in range(size):
        model.final_norm.set(i, data.data_ptr()[i])


fn set_layer_projection(
    mut model: Model,
    layer: Int,
    proj: String,
    data: Tensor[DType.float32],
    size: Int,
):
    """Copy a layer projection weight into model's flat storage.

    Args:
        model: Target model.
        layer: Layer index.
        proj: Projection name ("wq", "wk", "wv", "wo", "attn_norm",
              "ffn_norm", "w_gate", "w_up", "w_down").
        data: Weight data.
        size: Number of elements.
    """
    var off = model._layer_offsets(layer)
    var target: Int = -1

    if proj == "attn_norm":
        target = off.attn_norm
    elif proj == "wq":
        target = off.wq
    elif proj == "wk":
        target = off.wk
    elif proj == "wv":
        target = off.wv
    elif proj == "wo":
        target = off.wo
    elif proj == "ffn_norm":
        target = off.ffn_norm
    elif proj == "w_gate":
        target = off.w_gate
    elif proj == "w_up":
        target = off.w_up
    elif proj == "w_down":
        target = off.w_down

    if target >= 0:
        var src = data.data_ptr()
        for i in range(size):
            model.layer_weights.set(target + i, src[i])
