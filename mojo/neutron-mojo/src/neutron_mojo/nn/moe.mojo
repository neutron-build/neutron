# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Mixture of Experts
# ===----------------------------------------------------------------------=== #

"""Mixture of Experts (MoE) layer for sparse transformer models.

Used in Mixtral, DeepSeek, Grok, and other models where each token is
routed to a subset of expert FFN layers. This reduces compute while
maintaining model capacity.

Architecture:
    1. Router: linear projection → top-k softmax → expert selection
    2. Expert FFN: standard SwiGLU (gate + up → SiLU → down)
    3. Combine: weighted sum of selected expert outputs
"""

from math import exp
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import simd_dot, simd_matvec, simd_swiglu


# ===----------------------------------------------------------------------=== #
# MoE Configuration
# ===----------------------------------------------------------------------=== #

struct MoEConfig(Copyable, Movable):
    """Configuration for Mixture of Experts layer."""
    var num_experts: Int        # Total number of experts (e.g., 8)
    var top_k: Int              # Experts per token (e.g., 2)
    var hidden_dim: Int         # Input/output dimension
    var expert_dim: Int         # Expert FFN intermediate dimension

    fn __init__(out self, num_experts: Int, top_k: Int, hidden_dim: Int, expert_dim: Int):
        self.num_experts = num_experts
        self.top_k = top_k
        self.hidden_dim = hidden_dim
        self.expert_dim = expert_dim

    fn __copyinit__(out self, existing: Self):
        self.num_experts = existing.num_experts
        self.top_k = existing.top_k
        self.hidden_dim = existing.hidden_dim
        self.expert_dim = existing.expert_dim

    fn __moveinit__(out self, deinit other: Self):
        self.num_experts = other.num_experts
        self.top_k = other.top_k
        self.hidden_dim = other.hidden_dim
        self.expert_dim = other.expert_dim


# ===----------------------------------------------------------------------=== #
# Router
# ===----------------------------------------------------------------------=== #

struct MoERouter(Movable):
    """Gating network that routes tokens to experts.

    Uses a linear projection from hidden_dim → num_experts,
    then top-k softmax to select experts and compute weights.
    """
    var gate_weight: Tensor[DType.float32]  # [num_experts, hidden_dim]
    var num_experts: Int
    var top_k: Int
    var hidden_dim: Int

    fn __init__(out self, num_experts: Int, top_k: Int, hidden_dim: Int):
        self.num_experts = num_experts
        self.top_k = top_k
        self.hidden_dim = hidden_dim
        self.gate_weight = Tensor[DType.float32](Shape(num_experts * hidden_dim))

    fn __moveinit__(out self, deinit other: Self):
        self.gate_weight = other.gate_weight^
        self.num_experts = other.num_experts
        self.top_k = other.top_k
        self.hidden_dim = other.hidden_dim

    fn route(self, x: Tensor[DType.float32]) -> RoutingResult:
        """Compute routing for a single token.

        Args:
            x: Hidden state [hidden_dim].

        Returns:
            RoutingResult with selected expert indices and weights.
        """
        # Gate logits: W_gate @ x (SIMD-vectorized)
        var logits = Tensor[DType.float32](Shape(self.num_experts))
        simd_matvec(logits, 0, self.gate_weight, 0, x, 0, self.num_experts, self.hidden_dim)

        # Top-k selection
        var selected_experts = Tensor[DType.float32](Shape(self.top_k))
        var selected_logits = Tensor[DType.float32](Shape(self.top_k))
        var used = Tensor[DType.float32](Shape(self.num_experts))
        for i in range(self.num_experts):
            used.set(i, 0.0)

        for k in range(self.top_k):
            var best_idx = -1
            var best_val: Float32 = -1e30
            for e in range(self.num_experts):
                if used.get(e) == 0.0 and logits.get(e) > best_val:
                    best_val = logits.get(e)
                    best_idx = e
            if best_idx >= 0:
                selected_experts.set(k, Float32(best_idx))
                selected_logits.set(k, best_val)
                used.set(best_idx, 1.0)

        # Softmax over selected experts to get weights
        var max_logit = selected_logits.get(0)
        for k in range(1, self.top_k):
            var v = selected_logits.get(k)
            if v > max_logit:
                max_logit = v

        var weights = Tensor[DType.float32](Shape(self.top_k))
        var sum_exp: Float32 = 0.0
        for k in range(self.top_k):
            var e = Float32(exp(Float64(selected_logits.get(k) - max_logit)))
            weights.set(k, e)
            sum_exp += e

        if sum_exp > 0.0:
            for k in range(self.top_k):
                weights.set(k, weights.get(k) / sum_exp)

        return RoutingResult(selected_experts^, weights^, self.top_k)


struct RoutingResult(Movable):
    """Result of expert routing: which experts and their weights."""
    var expert_indices: Tensor[DType.float32]  # [top_k] — expert IDs as Float32
    var expert_weights: Tensor[DType.float32]  # [top_k] — softmax weights
    var top_k: Int

    fn __init__(
        out self,
        var expert_indices: Tensor[DType.float32],
        var expert_weights: Tensor[DType.float32],
        top_k: Int,
    ):
        self.expert_indices = expert_indices^
        self.expert_weights = expert_weights^
        self.top_k = top_k

    fn __moveinit__(out self, deinit other: Self):
        self.expert_indices = other.expert_indices^
        self.expert_weights = other.expert_weights^
        self.top_k = other.top_k

    fn get_expert_id(self, k: Int) -> Int:
        """Get the expert index for the k-th selected expert."""
        return Int(self.expert_indices.get(k))

    fn get_weight(self, k: Int) -> Float32:
        """Get the routing weight for the k-th selected expert."""
        return self.expert_weights.get(k)


# ===----------------------------------------------------------------------=== #
# Expert FFN
# ===----------------------------------------------------------------------=== #

struct ExpertWeights(Movable):
    """Weights for all experts in a MoE layer.

    Each expert has: w_gate [expert_dim, hidden_dim], w_up [expert_dim, hidden_dim],
    w_down [hidden_dim, expert_dim]. Stored flat per expert.
    """
    var data: Tensor[DType.float32]
    var num_experts: Int
    var hidden_dim: Int
    var expert_dim: Int
    var expert_stride: Int  # Elements per expert

    fn __init__(out self, num_experts: Int, hidden_dim: Int, expert_dim: Int):
        self.num_experts = num_experts
        self.hidden_dim = hidden_dim
        self.expert_dim = expert_dim
        # Per expert: gate(expert_dim*hidden_dim) + up(expert_dim*hidden_dim) + down(hidden_dim*expert_dim)
        self.expert_stride = expert_dim * hidden_dim * 2 + hidden_dim * expert_dim
        self.data = Tensor[DType.float32](Shape(num_experts * self.expert_stride))

    fn __moveinit__(out self, deinit other: Self):
        self.data = other.data^
        self.num_experts = other.num_experts
        self.hidden_dim = other.hidden_dim
        self.expert_dim = other.expert_dim
        self.expert_stride = other.expert_stride

    fn gate_offset(self, expert: Int) -> Int:
        """Offset of gate weights for an expert."""
        return expert * self.expert_stride

    fn up_offset(self, expert: Int) -> Int:
        """Offset of up-projection weights for an expert."""
        return expert * self.expert_stride + self.expert_dim * self.hidden_dim

    fn down_offset(self, expert: Int) -> Int:
        """Offset of down-projection weights for an expert."""
        return expert * self.expert_stride + self.expert_dim * self.hidden_dim * 2


fn expert_ffn(
    x: Tensor[DType.float32],
    weights: ExpertWeights,
    expert_id: Int,
) -> Tensor[DType.float32]:
    """Run a single expert's FFN: SwiGLU(gate, up) → down.

    Args:
        x: Input [hidden_dim].
        weights: All expert weights.
        expert_id: Which expert to run.

    Returns:
        Output [hidden_dim].
    """
    var hd = weights.hidden_dim
    var ed = weights.expert_dim

    # Gate projection: SIMD matvec
    var gate = Tensor[DType.float32](Shape(ed))
    simd_matvec(gate, 0, weights.data, weights.gate_offset(expert_id), x, 0, ed, hd)

    # Up projection: SIMD matvec
    var up = Tensor[DType.float32](Shape(ed))
    simd_matvec(up, 0, weights.data, weights.up_offset(expert_id), x, 0, ed, hd)

    # Fused SwiGLU: silu(gate) * up
    var swiglu_out = Tensor[DType.float32](Shape(ed))
    simd_swiglu(swiglu_out, 0, gate, 0, up, 0, ed)

    # Down projection: SIMD matvec
    var output = Tensor[DType.float32](Shape(hd))
    simd_matvec(output, 0, weights.data, weights.down_offset(expert_id), swiglu_out, 0, hd, ed)

    return output^


# ===----------------------------------------------------------------------=== #
# MoE Layer
# ===----------------------------------------------------------------------=== #

fn moe_forward(
    x: Tensor[DType.float32],
    router: MoERouter,
    expert_weights: ExpertWeights,
) -> Tensor[DType.float32]:
    """Full MoE forward pass for a single token.

    Routes to top-k experts, runs each expert FFN, combines with routing weights.

    Args:
        x: Input hidden state [hidden_dim].
        router: Gating network.
        expert_weights: All expert FFN weights.

    Returns:
        Output [hidden_dim].
    """
    var routing = router.route(x)
    var hidden_dim = expert_weights.hidden_dim

    var output = Tensor[DType.float32](Shape(hidden_dim))
    for d in range(hidden_dim):
        output.set(d, 0.0)

    for k in range(routing.top_k):
        var expert_id = routing.get_expert_id(k)
        var weight = routing.get_weight(k)

        var expert_out = expert_ffn(x, expert_weights, expert_id)

        for d in range(hidden_dim):
            output.set(d, output.get(d) + weight * expert_out.get(d))

    return output^


fn compute_load_balance_loss(
    routing_counts: Tensor[DType.float32],
    num_experts: Int,
    num_tokens: Int,
) -> Float32:
    """Compute auxiliary load balancing loss.

    Encourages equal distribution of tokens across experts.
    Loss = num_experts * sum(fraction_i * gate_prob_i).

    Args:
        routing_counts: Number of tokens routed to each expert [num_experts].
        num_experts: Total number of experts.
        num_tokens: Total tokens processed.

    Returns:
        Load balancing loss value.
    """
    if num_tokens == 0:
        return 0.0

    var loss: Float32 = 0.0
    var inv_tokens = Float32(1.0) / Float32(num_tokens)

    for e in range(num_experts):
        var fraction = routing_counts.get(e) * inv_tokens
        loss += fraction * fraction

    return Float32(num_experts) * loss
