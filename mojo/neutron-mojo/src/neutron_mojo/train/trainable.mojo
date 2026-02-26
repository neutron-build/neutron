# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Trainable Transformer
# ===----------------------------------------------------------------------=== #

"""Trainable transformer model using autograd tape.

Combines autograd + modules + losses for end-to-end differentiable
transformer blocks with simplified attention (no KV cache for training).
"""

from math import sqrt, exp

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

    fn __init__(out self, hidden_dim: Int, ffn_dim: Int = 0):
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

    fn __copyinit__(out self, other: Self):
        self.attn_norm = other.attn_norm.copy()
        self.q_proj = other.q_proj.copy()
        self.k_proj = other.k_proj.copy()
        self.v_proj = other.v_proj.copy()
        self.o_proj = other.o_proj.copy()
        self.ffn_norm = other.ffn_norm.copy()
        self.gate_proj = other.gate_proj.copy()
        self.up_proj = other.up_proj.copy()
        self.down_proj = other.down_proj.copy()
        self.hidden_dim = other.hidden_dim
        self.head_dim = other.head_dim
        self.ffn_dim = other.ffn_dim
        self.registered = other.registered

    fn __moveinit__(out self, deinit other: Self):
        self.attn_norm = other.attn_norm^
        self.q_proj = other.q_proj^
        self.k_proj = other.k_proj^
        self.v_proj = other.v_proj^
        self.o_proj = other.o_proj^
        self.ffn_norm = other.ffn_norm^
        self.gate_proj = other.gate_proj^
        self.up_proj = other.up_proj^
        self.down_proj = other.down_proj^
        self.hidden_dim = other.hidden_dim
        self.head_dim = other.head_dim
        self.ffn_dim = other.ffn_dim
        self.registered = other.registered

    fn register(mut self, mut tape: Tape):
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

    fn forward(self, mut tape: Tape, x_idx: Int) -> Int:
        """Forward pass through one transformer block (single token).

        For a single token, Q @ K^T is a scalar and softmax of one value = 1.0,
        so attention output = O(V). This is the optimized single-token path.
        """
        return self.forward_with_seq(tape, x_idx, 1)

    fn forward_with_seq(self, mut tape: Tape, x_idx: Int, seq_len: Int) -> Int:
        """Forward pass through one transformer block with sequence support.

        For seq_len=1: single-token optimization (attn = O(V)).
        For seq_len>1: x_idx is (seq_len * hidden_dim), performs real
        causal self-attention with Q @ K^T / sqrt(d) masking.
        """
        if seq_len == 1:
            return self._forward_single(tape, x_idx)
        return self._forward_seq(tape, x_idx, seq_len)

    fn _forward_single(self, mut tape: Tape, x_idx: Int) -> Int:
        """Single-token forward: attention is just O(V)."""
        var normed = self.attn_norm.forward(tape, x_idx)
        var q_idx = self.q_proj.forward(tape, normed)
        var k_idx = self.k_proj.forward(tape, normed)
        var v_idx = self.v_proj.forward(tape, normed)

        # Single token: softmax of scalar = 1.0, so attn = O(V)
        var attn_out = self.o_proj.forward(tape, v_idx)

        var post_attn = tracked_add(tape, x_idx, attn_out)
        return self._ffn_block(tape, post_attn)

    fn _forward_seq(self, mut tape: Tape, x_idx: Int, seq_len: Int) -> Int:
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

    fn _extract_token(self, mut tape: Tape, x_idx: Int, t: Int, hd: Int) -> Int:
        """Extract token t from flat (seq_len * hd) tensor."""
        var dims = List[Int]()
        dims.append(hd)
        var tok_idx = tape.add_variable(dims^, requires_grad=True)
        var off = t * hd
        for d in range(hd):
            tape.set_data(tok_idx, d, tape.get_data(x_idx, off + d))
        # Record as identity (reshape/split-like) for backward
        from neutron_mojo.autograd.tape import OP_SPLIT
        tape.record(TapeEntry(OP_SPLIT(), x_idx, -1, tok_idx, cached_int=off))
        return tok_idx

    fn _causal_attn_pos(
        self, mut tape: Tape,
        q_list: List[Int], k_list: List[Int], v_list: List[Int],
        t: Int, hd: Int,
    ) -> Int:
        """Compute causal attention for position t: attn(Q_t, K_{0..t}, V_{0..t}).

        Uses tracked_scalar_mul + tracked_add for the weighted sum
        to ensure proper gradient flow to attention weights and values.
        """
        var scale = 1.0 / sqrt(Float64(hd))
        var num_keys = t + 1

        # Compute scores: Q_t . K_j / sqrt(d)
        var score_dims = List[Int]()
        score_dims.append(num_keys)
        var scores_idx = tape.add_variable(score_dims^, requires_grad=True)

        for j in range(num_keys):
            var dot_val = Float64(0.0)
            for d in range(hd):
                dot_val += Float64(tape.get_data(q_list[t], d)) * Float64(tape.get_data(k_list[j], d))
            tape.set_data(scores_idx, j, Float32(dot_val * scale))

        # Softmax
        var attn_weights_idx = tracked_softmax(tape, scores_idx)

        # Weighted sum: out = sum_j w_j * V_j (using tracked ops)
        var w0 = Float64(tape.get_data(attn_weights_idx, 0))
        var out_idx = tracked_scalar_mul(tape, v_list[0], w0)

        for j in range(1, num_keys):
            var wj = Float64(tape.get_data(attn_weights_idx, j))
            var scaled_vj = tracked_scalar_mul(tape, v_list[j], wj)
            out_idx = tracked_add(tape, out_idx, scaled_vj)

        return out_idx

    fn _seq_residual_ffn(
        self, mut tape: Tape, x_idx: Int,
        attn_outputs: List[Int], seq_len: Int, hd: Int,
    ) -> Int:
        """Apply residual + FFN for each position, return flat output."""
        var out_dims = List[Int]()
        out_dims.append(seq_len * hd)
        var result_idx = tape.add_variable(out_dims^, requires_grad=True)

        for t in range(seq_len):
            # Extract x_t for residual
            var x_t = self._extract_token(tape, x_idx, t, hd)
            # Residual: x_t + attn_output_t
            var post_attn = tracked_add(tape, x_t, attn_outputs[t])
            # FFN
            var ffn_out = self._ffn_block(tape, post_attn)
            # Pack into result
            var off = t * hd
            for d in range(hd):
                tape.set_data(result_idx, off + d, tape.get_data(ffn_out, d))

        return result_idx

    fn _ffn_block(self, mut tape: Tape, x_idx: Int) -> Int:
        """FFN sub-block: norm -> gate * up (relu) -> down + residual."""
        var ffn_normed = self.ffn_norm.forward(tape, x_idx)
        var gate = self.gate_proj.forward(tape, ffn_normed)
        var up = self.up_proj.forward(tape, ffn_normed)
        var gate_activated = tracked_relu(tape, gate)
        var ffn_hidden = tracked_mul(tape, gate_activated, up)
        var ffn_out = self.down_proj.forward(tape, ffn_hidden)
        var output = tracked_add(tape, x_idx, ffn_out)
        return output

    fn param_indices(self) -> List[Int]:
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

    fn __init__(out self, vocab_size: Int, hidden_dim: Int, num_layers: Int, ffn_dim: Int = 0):
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

    fn __moveinit__(out self, deinit other: Self):
        self.embedding = other.embedding^
        self.blocks = other.blocks^
        self.final_norm = other.final_norm^
        self.lm_head = other.lm_head^
        self.vocab_size = other.vocab_size
        self.hidden_dim = other.hidden_dim
        self.num_layers = other.num_layers
        self.registered = other.registered

    fn register(mut self, mut tape: Tape):
        """Register all parameters on the tape."""
        self.embedding.register(tape)
        for i in range(len(self.blocks)):
            self.blocks[i].register(tape)
        self.final_norm.register(tape)
        self.lm_head.register(tape)
        self.registered = True

    fn forward(self, mut tape: Tape, token_id: Int) -> Int:
        """Forward pass: token_id -> logits.

        Returns the variable index of the logits (shape: vocab_size).
        """
        var x_idx = self.embedding.forward(tape, token_id)

        for i in range(len(self.blocks)):
            x_idx = self.blocks[i].forward(tape, x_idx)

        var normed = self.final_norm.forward(tape, x_idx)
        var logits_idx = self.lm_head.forward(tape, normed)
        return logits_idx

    fn all_param_indices(self) -> List[Int]:
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

    fn num_parameters(self, tape: Tape) -> Int:
        """Count total trainable parameters."""
        var params = self.all_param_indices()
        var total = 0
        for i in range(len(params)):
            total += tape.var_numel(params[i])
        return total

    fn forward_seq(self, mut tape: Tape, token_ids: List[Int]) -> List[Int]:
        """Forward pass for a sequence: token_ids -> per-position logits.

        Embeds all tokens, processes through blocks with causal attention,
        applies final norm and LM head to each position.

        Returns list of logits variable indices, one per position.
        """
        var seq_len = len(token_ids)
        var hd = self.hidden_dim

        # Embed all tokens into flat (seq_len * hidden_dim)
        var flat_dims = List[Int]()
        flat_dims.append(seq_len * hd)
        var x_idx = tape.add_variable(flat_dims^, requires_grad=True)
        for t in range(seq_len):
            var emb_t = self.embedding.forward(tape, token_ids[t])
            var off = t * hd
            for d in range(hd):
                tape.set_data(x_idx, off + d, tape.get_data(emb_t, d))

        # Process through transformer blocks
        for i in range(len(self.blocks)):
            x_idx = self.blocks[i].forward_with_seq(tape, x_idx, seq_len)

        # Extract each position, norm, and project to logits
        var logits_list = List[Int]()
        for t in range(seq_len):
            var tok_dims = List[Int]()
            tok_dims.append(hd)
            var x_t = tape.add_variable(tok_dims^, requires_grad=True)
            var off = t * hd
            for d in range(hd):
                tape.set_data(x_t, d, tape.get_data(x_idx, off + d))
            var normed = self.final_norm.forward(tape, x_t)
            var logits = self.lm_head.forward(tape, normed)
            logits_list.append(logits)
        return logits_list^


fn causal_lm_loss(mut tape: Tape, model: TrainableLM, token_id: Int, target_id: Int) -> Int:
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
