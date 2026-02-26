# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Transformer Block
# ===----------------------------------------------------------------------=== #

"""Single transformer block for Llama-style models.

Architecture (pre-norm):
    1. RMSNorm → Q/K/V projection → RoPE → GQA Attention → Residual add
    2. RMSNorm → Gate/Up projection → SwiGLU → Down projection → Residual add
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import rmsnorm, swiglu, matmul
from neutron_mojo.tensor.simd_math import simd_matvec
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head
from neutron_mojo.nn.kv_cache import KVCache
from neutron_mojo.nn.attention import gqa_attention


# ===----------------------------------------------------------------------=== #
# Linear Projection (matrix-vector multiply)
# ===----------------------------------------------------------------------=== #

fn linear(
    x: Tensor[DType.float32],
    weight: Tensor[DType.float32],
) raises -> Tensor[DType.float32]:
    """Linear projection: y = W @ x (no bias).

    Args:
        x: Input vector [in_features].
        weight: Weight matrix [out_features, in_features].

    Returns:
        Output vector [out_features].
    """
    var out_features = weight.shape()[0]
    var in_features = weight.shape()[1]

    var result = Tensor[DType.float32](Shape(out_features))
    simd_matvec(result, 0, weight, 0, x, 0, out_features, in_features)
    return result^


# ===----------------------------------------------------------------------=== #
# Transformer Block Weights
# ===----------------------------------------------------------------------=== #

struct TransformerWeights(Movable):
    """Weights for a single transformer block.

    Stores all weight matrices for attention and FFN.
    """
    # Attention
    var attn_norm: Tensor[DType.float32]    # [hidden_dim] RMSNorm gamma
    var wq: Tensor[DType.float32]           # [num_q_heads * head_dim, hidden_dim]
    var wk: Tensor[DType.float32]           # [num_kv_heads * head_dim, hidden_dim]
    var wv: Tensor[DType.float32]           # [num_kv_heads * head_dim, hidden_dim]
    var wo: Tensor[DType.float32]           # [hidden_dim, num_q_heads * head_dim]

    # FFN (SwiGLU)
    var ffn_norm: Tensor[DType.float32]     # [hidden_dim] RMSNorm gamma
    var w_gate: Tensor[DType.float32]       # [ffn_dim, hidden_dim]
    var w_up: Tensor[DType.float32]         # [ffn_dim, hidden_dim]
    var w_down: Tensor[DType.float32]       # [hidden_dim, ffn_dim]

    fn __init__(
        out self,
        hidden_dim: Int,
        num_q_heads: Int,
        num_kv_heads: Int,
        head_dim: Int,
        ffn_dim: Int,
    ):
        """Create transformer weights (initialized to zeros).

        Args:
            hidden_dim: Model hidden dimension.
            num_q_heads: Number of query heads.
            num_kv_heads: Number of KV heads.
            head_dim: Per-head dimension.
            ffn_dim: FFN intermediate dimension.
        """
        var q_dim = num_q_heads * head_dim
        var kv_dim = num_kv_heads * head_dim

        self.attn_norm = Tensor[DType.float32](Shape(hidden_dim))
        self.wq = Tensor[DType.float32](Shape(q_dim, hidden_dim))
        self.wk = Tensor[DType.float32](Shape(kv_dim, hidden_dim))
        self.wv = Tensor[DType.float32](Shape(kv_dim, hidden_dim))
        self.wo = Tensor[DType.float32](Shape(hidden_dim, q_dim))

        self.ffn_norm = Tensor[DType.float32](Shape(hidden_dim))
        self.w_gate = Tensor[DType.float32](Shape(ffn_dim, hidden_dim))
        self.w_up = Tensor[DType.float32](Shape(ffn_dim, hidden_dim))
        self.w_down = Tensor[DType.float32](Shape(hidden_dim, ffn_dim))

        # Initialize norms to 1.0 (identity)
        for i in range(hidden_dim):
            self.attn_norm.set(i, 1.0)
            self.ffn_norm.set(i, 1.0)

    fn __moveinit__(out self, deinit other: Self):
        self.attn_norm = other.attn_norm^
        self.wq = other.wq^
        self.wk = other.wk^
        self.wv = other.wv^
        self.wo = other.wo^
        self.ffn_norm = other.ffn_norm^
        self.w_gate = other.w_gate^
        self.w_up = other.w_up^
        self.w_down = other.w_down^


# ===----------------------------------------------------------------------=== #
# Transformer Block Forward Pass
# ===----------------------------------------------------------------------=== #

fn transformer_block(
    x: Tensor[DType.float32],
    weights: TransformerWeights,
    mut cache: KVCache,
    rope_table: RoPETable,
    pos: Int,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
) raises -> Tensor[DType.float32]:
    """Forward pass through a single transformer block.

    Args:
        x: Input hidden state [hidden_dim].
        weights: Block weights.
        cache: KV cache for this layer.
        rope_table: Precomputed RoPE frequencies.
        pos: Current position in sequence.
        num_q_heads: Number of query heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.

    Returns:
        Output hidden state [hidden_dim].
    """
    var hidden_dim = x.numel()

    # === Attention sublayer ===

    # 1. RMSNorm
    var normed = rmsnorm[DType.float32](x, weights.attn_norm)

    # 2. Q/K/V projections
    var q = linear(normed, weights.wq)   # [num_q_heads * head_dim]
    var k = linear(normed, weights.wk)   # [num_kv_heads * head_dim]
    var v = linear(normed, weights.wv)   # [num_kv_heads * head_dim]

    # 3. Apply RoPE to Q and K
    for h in range(num_q_heads):
        var q_head = Tensor[DType.float32](Shape(head_dim))
        var base = h * head_dim
        for d in range(head_dim):
            q_head.set(d, q.get(base + d))
        apply_rope_single_head(q_head, rope_table, pos)
        for d in range(head_dim):
            q.set(base + d, q_head.get(d))

    for h in range(num_kv_heads):
        var k_head = Tensor[DType.float32](Shape(head_dim))
        var base = h * head_dim
        for d in range(head_dim):
            k_head.set(d, k.get(base + d))
        apply_rope_single_head(k_head, rope_table, pos)
        for d in range(head_dim):
            k.set(base + d, k_head.get(d))

    # 4. Update KV cache
    cache.append_kv(k, v, num_new_tokens=1)

    # 5. GQA attention
    var attn_out = gqa_attention(q, cache, num_q_heads, num_kv_heads, head_dim)

    # 6. Output projection
    var attn_proj = linear(attn_out, weights.wo)  # [hidden_dim]

    # 7. Residual connection
    var residual1 = Tensor[DType.float32](Shape(hidden_dim))
    for i in range(hidden_dim):
        residual1.set(i, x.get(i) + attn_proj.get(i))

    # === FFN sublayer ===

    # 8. RMSNorm
    var ffn_normed = rmsnorm[DType.float32](residual1, weights.ffn_norm)

    # 9. Gate and Up projections
    var gate = linear(ffn_normed, weights.w_gate)  # [ffn_dim]
    var up = linear(ffn_normed, weights.w_up)      # [ffn_dim]

    # 10. SwiGLU activation: silu(gate) * up
    var ffn_out = swiglu[DType.float32](gate, up)

    # 11. Down projection
    var down = linear(ffn_out, weights.w_down)  # [hidden_dim]

    # 12. Residual connection
    var output = Tensor[DType.float32](Shape(hidden_dim))
    for i in range(hidden_dim):
        output.set(i, residual1.get(i) + down.get(i))

    return output^
