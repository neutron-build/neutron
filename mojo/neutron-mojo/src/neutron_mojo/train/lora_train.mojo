# ===----------------------------------------------------------------------=== #
# Neutron Mojo — LoRA Training
# ===----------------------------------------------------------------------=== #

"""Low-Rank Adaptation (LoRA) for parameter-efficient fine-tuning.

Creates trainable low-rank adapters that can be added to frozen base
model projections. Only LoRA parameters receive gradients, reducing
memory and compute vs full fine-tuning.

Key idea: Instead of updating W directly, learn delta W = B @ A
where A is (rank, in_features) and B is (out_features, rank).
"""

from math import sqrt
from random import random_float64

from neutron_mojo.autograd.tape import Tape, TapeEntry
from neutron_mojo.autograd.ops import tracked_add, tracked_matmul
from neutron_mojo.train.trainable import TrainableLM, TrainableTransformerBlock
from neutron_mojo.train.modules import Linear


struct TrainableLoRA(ImplicitlyCopyable, Copyable, Movable):
    """Low-rank adapter with A and B matrices.

    Forward: delta = x @ A^T @ B^T
    A is (rank, in_features), B is (out_features, rank).
    A is random-initialized, B is zero-initialized (so delta starts at 0).
    """
    var a_idx: Int       # tape var: (rank, in_features)
    var b_idx: Int       # tape var: (out_features, rank)
    var in_features: Int
    var out_features: Int
    var rank: Int
    var registered: Bool

    fn __init__(out self, in_features: Int, out_features: Int, rank: Int):
        self.a_idx = -1
        self.b_idx = -1
        self.in_features = in_features
        self.out_features = out_features
        self.rank = rank
        self.registered = False

    fn __copyinit__(out self, other: Self):
        self.a_idx = other.a_idx
        self.b_idx = other.b_idx
        self.in_features = other.in_features
        self.out_features = other.out_features
        self.rank = other.rank
        self.registered = other.registered

    fn __moveinit__(out self, deinit other: Self):
        self.a_idx = other.a_idx
        self.b_idx = other.b_idx
        self.in_features = other.in_features
        self.out_features = other.out_features
        self.rank = other.rank
        self.registered = other.registered

    fn register(mut self, mut tape: Tape):
        """Register A (random init) and B (zero init) on tape."""
        # A: (rank, in_features)
        var a_dims = List[Int]()
        a_dims.append(self.rank)
        a_dims.append(self.in_features)
        self.a_idx = tape.add_variable(a_dims^, requires_grad=True)

        # Kaiming init for A
        var scale = sqrt(2.0 / Float64(self.in_features))
        var a_numel = self.rank * self.in_features
        for i in range(a_numel):
            var val = Float32((random_float64() * 2.0 - 1.0) * scale)
            tape.set_data(self.a_idx, i, val)

        # B: (out_features, rank) — zero init
        var b_dims = List[Int]()
        b_dims.append(self.out_features)
        b_dims.append(self.rank)
        self.b_idx = tape.add_variable(b_dims^, requires_grad=True)
        # B is already zero-initialized by tape

        self.registered = True

    fn forward(self, mut tape: Tape, x_idx: Int) -> Int:
        """LoRA forward: x @ A^T @ B^T (additive delta).

        x: (in_features,) -> intermediate: (rank,) -> delta: (out_features,).
        Uses tracked_matmul for gradient tracking.
        """
        # x @ A^T: (1, in) @ (in, rank) -> (1, rank)
        var mid_idx = tracked_matmul(tape, x_idx, self.a_idx,
            1, self.in_features, self.rank)
        # mid @ B^T: (1, rank) @ (rank, out) -> (1, out)
        var delta_idx = tracked_matmul(tape, mid_idx, self.b_idx,
            1, self.rank, self.out_features)
        return delta_idx

    fn param_indices(self) -> List[Int]:
        """Return LoRA parameter indices."""
        var params = List[Int]()
        if self.a_idx >= 0:
            params.append(self.a_idx)
        if self.b_idx >= 0:
            params.append(self.b_idx)
        return params^


struct LoRATrainableLM(Movable):
    """Frozen base model + LoRA adapters on Q/V projections.

    Only the LoRA A/B matrices are trained. The base model weights
    are frozen (their requires_grad can be set to False).
    """
    var base: TrainableLM
    var lora_q: List[TrainableLoRA]
    var lora_v: List[TrainableLoRA]
    var rank: Int
    var registered: Bool

    fn __init__(out self, vocab_size: Int, hidden_dim: Int,
                num_layers: Int, rank: Int, ffn_dim: Int = 0):
        self.base = TrainableLM(vocab_size, hidden_dim, num_layers, ffn_dim)
        self.lora_q = List[TrainableLoRA]()
        self.lora_v = List[TrainableLoRA]()
        self.rank = rank
        self.registered = False

        for i in range(num_layers):
            self.lora_q.append(TrainableLoRA(hidden_dim, hidden_dim, rank))
            self.lora_v.append(TrainableLoRA(hidden_dim, hidden_dim, rank))

    fn __moveinit__(out self, deinit other: Self):
        self.base = other.base^
        self.lora_q = other.lora_q^
        self.lora_v = other.lora_v^
        self.rank = other.rank
        self.registered = other.registered

    fn register(mut self, mut tape: Tape):
        """Register base model and LoRA adapters."""
        self.base.register(tape)
        for i in range(len(self.lora_q)):
            self.lora_q[i].register(tape)
            self.lora_v[i].register(tape)
        self.registered = True

    fn freeze_base(self, mut tape: Tape):
        """Freeze base model parameters (set requires_grad=False)."""
        var base_params = self.base.all_param_indices()
        for i in range(len(base_params)):
            tape.var_requires_grad[base_params[i]] = False

    fn forward(self, mut tape: Tape, token_id: Int) -> Int:
        """Forward with LoRA: base forward + LoRA deltas on Q/V."""
        var x_idx = self.base.embedding.forward(tape, token_id)

        for i in range(len(self.base.blocks)):
            x_idx = self._forward_block_lora(tape, i, x_idx)

        var normed = self.base.final_norm.forward(tape, x_idx)
        var logits = self.base.lm_head.forward(tape, normed)
        return logits

    fn _forward_block_lora(self, mut tape: Tape, layer: Int, x_idx: Int) -> Int:
        """Forward one block with LoRA on Q and V projections."""
        var block = self.base.blocks[layer]
        var normed = block.attn_norm.forward(tape, x_idx)

        # Q with LoRA
        var q_base = block.q_proj.forward(tape, normed)
        var q_delta = self.lora_q[layer].forward(tape, normed)
        var q_idx = tracked_add(tape, q_base, q_delta)

        # K without LoRA
        var k_idx = block.k_proj.forward(tape, normed)

        # V with LoRA
        var v_base = block.v_proj.forward(tape, normed)
        var v_delta = self.lora_v[layer].forward(tape, normed)
        var v_idx = tracked_add(tape, v_base, v_delta)

        # Single-token attention: attn = O(V)
        var attn_out = block.o_proj.forward(tape, v_idx)
        var post_attn = tracked_add(tape, x_idx, attn_out)

        # FFN (no LoRA)
        return block._ffn_block(tape, post_attn)

    fn lora_param_indices(self) -> List[Int]:
        """Return only LoRA parameter indices (for optimizer)."""
        var params = List[Int]()
        for i in range(len(self.lora_q)):
            var qp = self.lora_q[i].param_indices()
            for j in range(len(qp)):
                params.append(qp[j])
            var vp = self.lora_v[i].param_indices()
            for j in range(len(vp)):
                params.append(vp[j])
        return params^

    fn total_lora_params(self, tape: Tape) -> Int:
        """Count total LoRA trainable parameters."""
        var params = self.lora_param_indices()
        var total = 0
        for i in range(len(params)):
            total += tape.var_numel(params[i])
        return total
