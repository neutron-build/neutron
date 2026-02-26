# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Weight Transfer (Model <-> Tape)
# ===----------------------------------------------------------------------=== #

"""Bidirectional weight transfer between inference Model and training Tape.

Enables:
- Initializing a Tape from a pretrained Model (for fine-tuning)
- Extracting trained weights from a Tape back into a Model (for inference)

Uses tape.get_data()/set_data() and model.embed.get()/layer_weights.get()
— never data_ptr().
"""

from neutron_mojo.autograd.tape import Tape
from neutron_mojo.nn.model import Model, ModelParams, LayerWeightOffsets
from neutron_mojo.train.trainable import TrainableLM, TrainableTransformerBlock
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


struct WeightMapping(ImplicitlyCopyable, Copyable, Movable):
    """Maps a model weight offset to a tape variable index."""
    var model_offset: Int   # offset into Model flat storage (or -1 for embed/head)
    var tape_var_idx: Int   # tape variable index
    var numel: Int          # number of elements
    var source: Int         # 0=layer_weights, 1=embed, 2=final_norm, 3=lm_head

    fn __init__(out self, model_offset: Int, tape_var_idx: Int, numel: Int, source: Int = 0):
        self.model_offset = model_offset
        self.tape_var_idx = tape_var_idx
        self.numel = numel
        self.source = source

    fn __copyinit__(out self, other: Self):
        self.model_offset = other.model_offset
        self.tape_var_idx = other.tape_var_idx
        self.numel = other.numel
        self.source = other.source

    fn __moveinit__(out self, deinit other: Self):
        self.model_offset = other.model_offset
        self.tape_var_idx = other.tape_var_idx
        self.numel = other.numel
        self.source = other.source


fn _add_layer_mappings(
    mut mappings: List[WeightMapping],
    block: TrainableTransformerBlock,
    off: LayerWeightOffsets,
    params: ModelParams,
):
    """Add weight mappings for one transformer layer."""
    var hd = params.hidden_dim
    var qd = params.q_dim()
    var kvd = params.kv_dim()
    var fd = params.ffn_dim

    # attn_norm
    mappings.append(WeightMapping(off.attn_norm, block.attn_norm.gamma_idx, hd, 0))
    # q_proj
    mappings.append(WeightMapping(off.wq, block.q_proj.weight_idx, qd * hd, 0))
    # k_proj
    mappings.append(WeightMapping(off.wk, block.k_proj.weight_idx, kvd * hd, 0))
    # v_proj
    mappings.append(WeightMapping(off.wv, block.v_proj.weight_idx, kvd * hd, 0))
    # o_proj
    mappings.append(WeightMapping(off.wo, block.o_proj.weight_idx, hd * qd, 0))
    # ffn_norm
    mappings.append(WeightMapping(off.ffn_norm, block.ffn_norm.gamma_idx, hd, 0))
    # gate_proj
    mappings.append(WeightMapping(off.w_gate, block.gate_proj.weight_idx, fd * hd, 0))
    # up_proj
    mappings.append(WeightMapping(off.w_up, block.up_proj.weight_idx, fd * hd, 0))
    # down_proj
    mappings.append(WeightMapping(off.w_down, block.down_proj.weight_idx, hd * fd, 0))


fn build_weight_mapping(model: Model, trainable: TrainableLM) -> List[WeightMapping]:
    """Build mapping between Model weights and TrainableLM tape variables.

    Returns a list of WeightMapping entries covering all weights.
    """
    var mappings = List[WeightMapping]()
    var p = model.params.copy()

    # Embedding: source=1
    var embed_numel = p.vocab_size * p.hidden_dim
    mappings.append(WeightMapping(0, trainable.embedding.embed_idx, embed_numel, 1))

    # Per-layer weights: source=0
    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        _add_layer_mappings(mappings, trainable.blocks[layer], off, p)

    # Final norm: source=2
    mappings.append(WeightMapping(0, trainable.final_norm.gamma_idx, p.hidden_dim, 2))

    # LM head: source=3
    var head_numel = p.vocab_size * p.hidden_dim
    mappings.append(WeightMapping(0, trainable.lm_head.weight_idx, head_numel, 3))

    return mappings^


fn _copy_embed_to_tape(model: Model, mut tape: Tape, tape_idx: Int, numel: Int):
    """Copy model.embed data into tape variable."""
    for i in range(numel):
        var row = i // model.params.hidden_dim
        var col = i % model.params.hidden_dim
        tape.set_data(tape_idx, i, model.embed.get(row, col))


fn _copy_final_norm_to_tape(model: Model, mut tape: Tape, tape_idx: Int, numel: Int):
    """Copy model.final_norm data into tape variable."""
    for i in range(numel):
        tape.set_data(tape_idx, i, model.final_norm.get(i))


fn _copy_lm_head_to_tape(model: Model, mut tape: Tape, tape_idx: Int, numel: Int):
    """Copy model.lm_head data into tape variable."""
    for i in range(numel):
        var row = i // model.params.hidden_dim
        var col = i % model.params.hidden_dim
        tape.set_data(tape_idx, i, model.lm_head.get(row, col))


fn _copy_layer_weights_to_tape(
    model: Model, mut tape: Tape, model_offset: Int, tape_idx: Int, numel: Int,
):
    """Copy a slice of model.layer_weights into tape variable."""
    for i in range(numel):
        tape.set_data(tape_idx, i, model.layer_weights.get(model_offset + i))


fn model_to_tape(model: Model, mut tape: Tape, trainable: TrainableLM):
    """Copy Model weights into the tape for training.

    Transfers all weights from the inference Model's flat storage
    into the corresponding tape variables of the TrainableLM.
    """
    var mappings = build_weight_mapping(model, trainable)
    for i in range(len(mappings)):
        var m = mappings[i]
        if m.source == 1:
            _copy_embed_to_tape(model, tape, m.tape_var_idx, m.numel)
        elif m.source == 2:
            _copy_final_norm_to_tape(model, tape, m.tape_var_idx, m.numel)
        elif m.source == 3:
            _copy_lm_head_to_tape(model, tape, m.tape_var_idx, m.numel)
        else:
            _copy_layer_weights_to_tape(model, tape, m.model_offset, m.tape_var_idx, m.numel)


fn _copy_tape_to_embed(tape: Tape, tape_idx: Int, mut embed: Tensor[DType.float32], numel: Int):
    """Copy tape variable into embed tensor."""
    for i in range(numel):
        embed.set(i, tape.get_data(tape_idx, i))


fn _copy_tape_to_norm(tape: Tape, tape_idx: Int, mut norm: Tensor[DType.float32], numel: Int):
    """Copy tape variable into norm tensor."""
    for i in range(numel):
        norm.set(i, tape.get_data(tape_idx, i))


fn _copy_tape_to_layer(
    tape: Tape, tape_idx: Int, mut lw: Tensor[DType.float32], offset: Int, numel: Int,
):
    """Copy tape variable into layer_weights at offset."""
    for i in range(numel):
        lw.set(offset + i, tape.get_data(tape_idx, i))


fn tape_to_model(tape: Tape, trainable: TrainableLM, params: ModelParams) -> Model:
    """Copy trained weights from tape back into a new Model for inference.

    Creates a fresh Model and populates it from the tape variables
    referenced by the TrainableLM.
    """
    var model = Model(params)
    var p = params.copy()

    # Embedding
    var embed_numel = p.vocab_size * p.hidden_dim
    _copy_tape_to_embed(tape, trainable.embedding.embed_idx, model.embed, embed_numel)

    # Per-layer weights
    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        var block = trainable.blocks[layer]
        var hd = p.hidden_dim
        var qd = p.q_dim()
        var kvd = p.kv_dim()
        var fd = p.ffn_dim

        _copy_tape_to_layer(tape, block.attn_norm.gamma_idx, model.layer_weights, off.attn_norm, hd)
        _copy_tape_to_layer(tape, block.q_proj.weight_idx, model.layer_weights, off.wq, qd * hd)
        _copy_tape_to_layer(tape, block.k_proj.weight_idx, model.layer_weights, off.wk, kvd * hd)
        _copy_tape_to_layer(tape, block.v_proj.weight_idx, model.layer_weights, off.wv, kvd * hd)
        _copy_tape_to_layer(tape, block.o_proj.weight_idx, model.layer_weights, off.wo, hd * qd)
        _copy_tape_to_layer(tape, block.ffn_norm.gamma_idx, model.layer_weights, off.ffn_norm, hd)
        _copy_tape_to_layer(tape, block.gate_proj.weight_idx, model.layer_weights, off.w_gate, fd * hd)
        _copy_tape_to_layer(tape, block.up_proj.weight_idx, model.layer_weights, off.w_up, fd * hd)
        _copy_tape_to_layer(tape, block.down_proj.weight_idx, model.layer_weights, off.w_down, hd * fd)

    # Final norm
    _copy_tape_to_norm(tape, trainable.final_norm.gamma_idx, model.final_norm, p.hidden_dim)

    # LM head
    var head_numel = p.vocab_size * p.hidden_dim
    _copy_tape_to_embed(tape, trainable.lm_head.weight_idx, model.lm_head, head_numel)

    return model^
