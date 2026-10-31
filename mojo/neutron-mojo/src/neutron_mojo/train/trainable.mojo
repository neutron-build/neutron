# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Trainable Transformer
# ===----------------------------------------------------------------------=== #

"""Trainable transformer model using autograd tape.

Combines autograd + modules + losses for end-to-end differentiable
transformer blocks with simplified attention (no KV cache for training).
"""

from std.math import sqrt, exp
from neutron_mojo.autograd.ops import tracked_slice, tracked_concat_flat, tracked_vector_scale

from neutron_mojo.autograd.tape import Tape, TapeEntry, OP_SOFTMAX
from neutron_mojo.autograd.ops import (
    tracked_add, tracked_mul, tracked_matmul, tracked_relu,
    tracked_softmax, tracked_sum, tracked_scalar_mul, tracked_div,
    tracked_scalar_add,
)
from neutron_mojo.train.modules import Linear, Embedding, RMSNormModule
from neutron_mojo.train.losses import cross_entropy_loss


struct TrainableTransformerBlock(ImplicitlyCopyable, Copyable, Movable):
    """A single transformer block with trainable parameters.

    Components:
    - Pre-attention RMSNorm
    - Q, K, V, O linear projections
    - Simplified single-head attention (no KV cache)
    - Pre-FFN RMSNorm
    - Gate, Up, Down FFN projections (SwiGLU pattern simplified to ReLU gate)
    """
    var attn_norm: RMSNormModule
    var q_proj: Linear
    var k_proj: Linear
    var v_proj: Linear
    var o_proj: Linear
    var ffn_norm: RMSNormModule
    var gate_proj: Linear
    var up_proj: Linear
    var down_proj: Linear
    var hidden_dim: Int
    var head_dim: Int
    var ffn_dim: Int
    var registered: Bool

    def __init__(out self, hidden_dim: Int, ffn_dim: Int = 0):
        var actual_ffn = ffn_dim if ffn_dim > 0 else hidden_dim * 4
        self.hidden_dim = hidden_dim
        self.head_dim = hidden_dim  # single-head for simplicity
        self.ffn_dim = actual_ffn
        self.attn_norm = RMSNormModule(hidden_dim)
        self.q_proj = Linear(hidden_dim, hidden_dim, has_bias=False)
        self.k_proj = Linear(hidden_dim, hidden_dim, has_bias=False)
        self.v_proj = Linear(hidden_dim, hidden_dim, has_bias=False)
        self.o_proj = Linear(hidden_dim, hidden_dim, has_bias=False)
        self.ffn_norm = RMSNormModule(hidden_dim)
        self.gate_proj = Linear(hidden_dim, actual_ffn, has_bias=False)
        self.up_proj = Linear(hidden_dim, actual_ffn, has_bias=False)
        self.down_proj = Linear(actual_ffn, hidden_dim, has_bias=False)
        self.registered = False

    def __init__(out self, *, copy: Self):
        self.attn_norm = copy.attn_norm.copy()
        self.q_proj = copy.q_proj.copy()
        self.k_proj = copy.k_proj.copy()
        self.v_proj = copy.v_proj.copy()
        self.o_proj = copy.o_proj.copy()
        self.ffn_norm = copy.ffn_norm.copy()
        self.gate_proj = copy.gate_proj.copy()
        self.up_proj = copy.up_proj.copy()
        self.down_proj = copy.down_proj.copy()
        self.hidden_dim = copy.hidden_dim
        self.head_dim = copy.head_dim
        self.ffn_dim = copy.ffn_dim
        self.registered = copy.registered

    def __init__(out self, *, deinit move: Self):
        self.attn_norm = move.attn_norm^
        self.q_proj = move.q_proj^
        self.k_proj = move.k_proj^
        self.v_proj = move.v_proj^
        self.o_proj = move.o_proj^
        self.ffn_norm = move.ffn_norm^
        self.gate_proj = move.gate_proj^
        self.up_proj = move.up_proj^
        self.down_proj = move.down_proj^
        self.hidden_dim = move.hidden_dim^
        self.head_dim = move.head_dim^
        self.ffn_dim = move.ffn_dim^
        self.registered = move.registered^

    def register(mut self, mut tape: Tape) raises:
        """Register all parameters on the tape."""
        self.attn_norm.register(tape)
        self.q_proj.register(tape)
        self.k_proj.register(tape)
        self.v_proj.register(tape)
        self.o_proj.register(tape)
        self.ffn_norm.register(tape)
        self.gate_proj.register(tape)
        self.up_proj.register(tape)
        self.down_proj.register(tape)
        self.registered = True

    def forward(self, mut tape: Tape, x_idx: Int) raises -> Int:
        """Forward pass through one transformer block (single token).

        For a single token, Q @ K^T is a scalar and softmax of one value = 1.0,
        so attention output = O(V). This is the optimized single-token path.
        """
        return self.forward_with_seq(tape, x_idx, 1)

    def forward_with_seq(self, mut tape: Tape, x_idx: Int, seq_len: Int) raises -> Int:
        """Forward pass through one transformer block with sequence support.

        For seq_len=1: single-token optimization (attn = O(V)).
        For seq_len>1: x_idx is (seq_len * hidden_dim), performs real
        causal self-attention with Q @ K^T / sqrt(d) masking.
        """
        tape.validate_variable(x_idx)
        if seq_len <= 0 or self.hidden_dim <= 0 or seq_len > 0x7FFFFFFFFFFFFFFF // self.hidden_dim or tape.var_numel(x_idx) != seq_len * self.hidden_dim:
            raise Error("Transformer training sequence size mismatch")
        if seq_len == 1:
            return self._forward_single(tape, x_idx)
        return self._forward_seq(tape, x_idx, seq_len)

    def _forward_single(self, mut tape: Tape, x_idx: Int) raises -> Int:
        """Single-token forward: attention is just O(V)."""
        var normed = self.attn_norm.forward(tape, x_idx)
        var q_idx = self.q_proj.forward(tape, normed)
        var k_idx = self.k_proj.forward(tape, normed)
        var v_idx = self.v_proj.forward(tape, normed)

        # Single token: softmax of scalar = 1.0, so attn = O(V)
        var attn_out = self.o_proj.forward(tape, v_idx)

        var post_attn = tracked_add(tape, x_idx, attn_out)
        return self._ffn_block(tape, post_attn)

    def _forward_seq(self, mut tape: Tape, x_idx: Int, seq_len: Int) raises -> Int:
        """Multi-token forward with real causal self-attention.

        x_idx has shape (seq_len * hidden_dim).
        Processes each position, computing Q_i @ K_j^T for j <= i.
        """
        var hd = self.hidden_dim
        # Process each token through norm and projections
        var q_list = List[Int]()
        var k_list = List[Int]()
        var v_list = List[Int]()
        var normed_list = List[Int]()

        for t in range(seq_len):
            var x_t = self._extract_token(tape, x_idx, t, hd)
            var normed_t = self.attn_norm.forward(tape, x_t)
            normed_list.append(normed_t)
            q_list.append(self.q_proj.forward(tape, normed_t))
            k_list.append(self.k_proj.forward(tape, normed_t))
            v_list.append(self.v_proj.forward(tape, normed_t))

        # Causal attention for each position
        var attn_outputs = List[Int]()
        for t in range(seq_len):
            var attn_t = self._causal_attn_pos(tape, q_list, k_list, v_list, t, hd)
            var proj_t = self.o_proj.forward(tape, attn_t)
            attn_outputs.append(proj_t)

        # Residual + FFN for each position, pack into flat output
        return self._seq_residual_ffn(tape, x_idx, attn_outputs, seq_len, hd)

    def _extract_token(self, mut tape: Tape, x_idx: Int, t: Int, hd: Int) raises -> Int:
        if t < 0 or hd <= 0 or t > 0x7FFFFFFFFFFFFFFF // hd:
            raise Error("Invalid sequence token slice")
        return tracked_slice(tape, x_idx, t * hd, hd)

    def _causal_attn_pos(
        self, mut tape: Tape,
        q_list: List[Int], k_list: List[Int], v_list: List[Int],
        t: Int, hd: Int,
    ) raises -> Int:
        """Compute causal attention for position t: attn(Q_t, K_{0..t}, V_{0..t}).

        Uses tracked_scalar_mul + tracked_add for the weighted sum
        to ensure proper gradient flow to attention weights and values.
        """
        var scale = 1.0 / sqrt(Float64(hd))
        var num_keys = t + 1

        # Build every Q/K score with tracked multiplication and reduction.
        var scores_idx = -1
        for j in range(num_keys):
            var product = tracked_mul(tape, q_list[t], k_list[j])
            var dot = tracked_sum(tape, product)
            var score = tracked_scalar_mul(tape, dot, scale)
            if j == 0:
                scores_idx = score
            else:
                scores_idx = tracked_concat_flat(tape, scores_idx, score)
        var attn_weights_idx = tracked_softmax(tape, scores_idx)
        var w0 = tracked_slice(tape, attn_weights_idx, 0, 1)
        var out_idx = tracked_vector_scale(tape, v_list[0], w0)
        for j in range(1, num_keys):
            var wj = tracked_slice(tape, attn_weights_idx, j, 1)
            var scaled = tracked_vector_scale(tape, v_list[j], wj)
            out_idx = tracked_add(tape, out_idx, scaled)

        return out_idx

    def _seq_residual_ffn(
        self, mut tape: Tape, x_idx: Int,
        attn_outputs: List[Int], seq_len: Int, hd: Int,
    ) raises -> Int:
        """Apply residual + FFN for each position, return flat output."""
        var result_idx = -1
        for t in range(seq_len):
            var x_t = self._extract_token(tape, x_idx, t, hd)
            var post_attn = tracked_add(tape, x_t, attn_outputs[t])
            var ffn_out = self._ffn_block(tape, post_attn)
            if t == 0:
                result_idx = ffn_out
            else:
                result_idx = tracked_concat_flat(tape, result_idx, ffn_out)

        return result_idx

    def _ffn_block(self, mut tape: Tape, x_idx: Int) raises -> Int:
        """FFN sub-block: norm -> gate * up (relu) -> down + residual."""
        var ffn_normed = self.ffn_norm.forward(tape, x_idx)
        var gate = self.gate_proj.forward(tape, ffn_normed)
        var up = self.up_proj.forward(tape, ffn_normed)
        var gate_activated = tracked_relu(tape, gate)
        var ffn_hidden = tracked_mul(tape, gate_activated, up)
        var ffn_out = self.down_proj.forward(tape, ffn_hidden)
        var output = tracked_add(tape, x_idx, ffn_out)
        return output

    def param_indices(self) -> List[Int]:
        """Return all parameter indices."""
        var params = List[Int]()
        var lists = List[List[Int]]()
        lists.append(self.attn_norm.param_indices())
        lists.append(self.q_proj.param_indices())
        lists.append(self.k_proj.param_indices())
        lists.append(self.v_proj.param_indices())
        lists.append(self.o_proj.param_indices())
        lists.append(self.ffn_norm.param_indices())
        lists.append(self.gate_proj.param_indices())
        lists.append(self.up_proj.param_indices())
        lists.append(self.down_proj.param_indices())
        for li in range(len(lists)):
            var p = lists[li].copy()
            for j in range(len(p)):
                params.append(p[j])
        return params^


struct TrainableLM(Movable):
    """Complete trainable language model.

    Architecture: Embedding + N transformer blocks + final norm + LM head.
    """
    var embedding: Embedding
    var blocks: List[TrainableTransformerBlock]
    var final_norm: RMSNormModule
    var lm_head: Linear
    var vocab_size: Int
    var hidden_dim: Int
    var num_layers: Int
    var registered: Bool

    def __init__(out self, vocab_size: Int, hidden_dim: Int, num_layers: Int, ffn_dim: Int = 0):
        self.vocab_size = vocab_size
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.embedding = Embedding(vocab_size, hidden_dim)
        self.blocks = List[TrainableTransformerBlock]()
        for i in range(num_layers):
            self.blocks.append(TrainableTransformerBlock(hidden_dim, ffn_dim))
        self.final_norm = RMSNormModule(hidden_dim)
        self.lm_head = Linear(hidden_dim, vocab_size, has_bias=False)
        self.registered = False

    def __init__(out self, *, deinit move: Self):
        self.embedding = move.embedding^
        self.blocks = move.blocks^
        self.final_norm = move.final_norm^
        self.lm_head = move.lm_head^
        self.vocab_size = move.vocab_size^
        self.hidden_dim = move.hidden_dim^
        self.num_layers = move.num_layers^
        self.registered = move.registered^

    def register(mut self, mut tape: Tape) raises:
        """Register all parameters on the tape."""
        self.embedding.register(tape)
        for i in range(len(self.blocks)):
            self.blocks[i].register(tape)
        self.final_norm.register(tape)
        self.lm_head.register(tape)
        self.registered = True

    def forward(self, mut tape: Tape, token_id: Int) raises -> Int:
        """Forward pass: token_id -> logits.

        Returns the variable index of the logits (shape: vocab_size).
        """
        var x_idx = self.embedding.forward(tape, token_id)

        for i in range(len(self.blocks)):
            x_idx = self.blocks[i].forward(tape, x_idx)

        var normed = self.final_norm.forward(tape, x_idx)
        var logits_idx = self.lm_head.forward(tape, normed)
        return logits_idx

    def all_param_indices(self) -> List[Int]:
        """Return all parameter indices for the model."""
        var params = List[Int]()
        var embed_params = self.embedding.param_indices()
        for i in range(len(embed_params)):
            params.append(embed_params[i])
        for i in range(len(self.blocks)):
            var block_params = self.blocks[i].param_indices()
            for j in range(len(block_params)):
                params.append(block_params[j])
        var norm_params = self.final_norm.param_indices()
        for i in range(len(norm_params)):
            params.append(norm_params[i])
        var head_params = self.lm_head.param_indices()
        for i in range(len(head_params)):
            params.append(head_params[i])
        return params^

    def num_parameters(self, tape: Tape) -> Int:
        """Count total trainable parameters."""
        var params = self.all_param_indices()
        var total = 0
        for i in range(len(params)):
            total += tape.var_numel(params[i])
        return total

    def forward_seq(self, mut tape: Tape, token_ids: List[Int]) raises -> List[Int]:
        """Forward pass for a sequence: token_ids -> per-position logits.

        Embeds all tokens, processes through blocks with causal attention,
        applies final norm and LM head to each position.

        Returns list of logits variable indices, one per position.
        """
        var seq_len = len(token_ids)
        var hd = self.hidden_dim

        if seq_len <= 0 or hd <= 0 or seq_len > 0x7FFFFFFFFFFFFFFF // hd:
            raise Error("Invalid training sequence shape")
        # Validate all tokens before embedding or creating graph state.
        for t in range(seq_len):
            if token_ids[t] < 0 or token_ids[t] >= self.vocab_size:
                raise Error("Training token outside vocabulary")
        var x_idx = -1
        for t in range(seq_len):
            var emb_t = self.embedding.forward(tape, token_ids[t])
            if t == 0:
                x_idx = emb_t
            else:
                x_idx = tracked_concat_flat(tape, x_idx, emb_t)

        # Process through transformer blocks
        for i in range(len(self.blocks)):
            x_idx = self.blocks[i].forward_with_seq(tape, x_idx, seq_len)

        # Extract each position, norm, and project to logits
        var logits_list = List[Int]()
        for t in range(seq_len):
            var x_t = tracked_slice(tape, x_idx, t * hd, hd)
            var normed = self.final_norm.forward(tape, x_t)
            var logits = self.lm_head.forward(tape, normed)
            logits_list.append(logits)
        return logits_list^


def causal_lm_loss(mut tape: Tape, model: TrainableLM, token_id: Int, target_id: Int) raises -> Int:
    """Compute language modeling loss for a single token prediction.

    Forward passes the token through the model and computes
    cross-entropy loss against the target.

    Args:
        tape: The autograd tape.
        model: The trainable language model.
        token_id: Input token ID.
        target_id: Target token ID.

    Returns:
        Variable index of the scalar loss.
    """
    var logits_idx = model.forward(tape, token_id)
    return cross_entropy_loss(tape, logits_idx, target_id, model.vocab_size)
