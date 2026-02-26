# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Rotary Position Embeddings (RoPE)
# ===----------------------------------------------------------------------=== #

"""Rotary Position Embeddings for transformer attention.

RoPE encodes position information by rotating Q/K vectors in pairs of
dimensions using position-dependent rotation matrices.

Reference: "RoFormer: Enhanced Transformer with Rotary Position Embedding"
           (Su et al., 2021)
"""

from math import sin, cos
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# RoPE Frequency Table
# ===----------------------------------------------------------------------=== #

struct RoPETable(Movable):
    """Precomputed cos/sin tables for RoPE.

    Stores cos(m*theta_i) and sin(m*theta_i) for all positions m
    and frequency indices i, where theta_i = 1 / (base^(2i/dim)).
    """
    var cos_table: Tensor[DType.float32]  # [max_seq_len, head_dim/2]
    var sin_table: Tensor[DType.float32]  # [max_seq_len, head_dim/2]
    var head_dim: Int
    var max_seq_len: Int
    var theta_base: Float64

    fn __init__(out self, head_dim: Int, max_seq_len: Int, theta: Float64 = 10000.0):
        """Precompute RoPE cos/sin tables.

        Args:
            head_dim: Per-head dimension (must be even).
            max_seq_len: Maximum sequence length to precompute.
            theta: Base frequency (10000 for original, 500000 for Llama-3).
        """
        self.head_dim = head_dim
        self.max_seq_len = max_seq_len
        self.theta_base = theta

        var half_dim = head_dim // 2
        self.cos_table = Tensor[DType.float32](Shape(max_seq_len * half_dim))
        self.sin_table = Tensor[DType.float32](Shape(max_seq_len * half_dim))

        # Precompute frequencies: theta_i = 1 / (base^(2i/dim))
        for pos in range(max_seq_len):
            for i in range(half_dim):
                var freq_exp = Float64(2 * i) / Float64(head_dim)
                var freq = 1.0 / (theta ** freq_exp)
                var angle = Float64(pos) * freq

                self.cos_table.set(pos * half_dim + i, Float32(cos(angle)))
                self.sin_table.set(pos * half_dim + i, Float32(sin(angle)))

    fn __moveinit__(out self, deinit other: Self):
        self.cos_table = other.cos_table^
        self.sin_table = other.sin_table^
        self.head_dim = other.head_dim
        self.max_seq_len = other.max_seq_len
        self.theta_base = other.theta_base


# ===----------------------------------------------------------------------=== #
# Apply RoPE
# ===----------------------------------------------------------------------=== #

fn apply_rope(
    mut x: Tensor[DType.float32],
    table: RoPETable,
    start_pos: Int,
    seq_len: Int,
    num_heads: Int,
) raises:
    """Apply RoPE to a Q or K tensor in-place.

    Rotates pairs of dimensions (2i, 2i+1) using precomputed cos/sin.

    The rotation for each pair is:
        x_rot[2i]   = x[2i] * cos - x[2i+1] * sin
        x_rot[2i+1] = x[2i] * sin + x[2i+1] * cos

    Args:
        x: Tensor of shape [seq_len, num_heads, head_dim] to rotate in-place.
        table: Precomputed RoPE cos/sin table.
        start_pos: Starting position index (for KV cache continuation).
        seq_len: Number of positions to process.
        num_heads: Number of attention heads.
    """
    var head_dim = table.head_dim
    var half_dim = head_dim // 2

    for s in range(seq_len):
        var pos = start_pos + s
        if pos >= table.max_seq_len:
            break

        for h in range(num_heads):
            var head_offset = s * num_heads * head_dim + h * head_dim
            var table_offset = pos * half_dim

            for i in range(half_dim):
                var cos_val = table.cos_table.get(table_offset + i)
                var sin_val = table.sin_table.get(table_offset + i)

                var x0 = x.get(head_offset + 2 * i)
                var x1 = x.get(head_offset + 2 * i + 1)

                x.set(head_offset + 2 * i, x0 * cos_val - x1 * sin_val)
                x.set(head_offset + 2 * i + 1, x0 * sin_val + x1 * cos_val)


fn apply_rope_single_head(
    mut x: Tensor[DType.float32],
    table: RoPETable,
    pos: Int,
) raises:
    """Apply RoPE to a single vector [head_dim].

    Args:
        x: Tensor of shape [head_dim].
        table: Precomputed RoPE cos/sin table.
        pos: Position index.
    """
    var half_dim = table.head_dim // 2
    var table_offset = pos * half_dim

    for i in range(half_dim):
        var cos_val = table.cos_table.get(table_offset + i)
        var sin_val = table.sin_table.get(table_offset + i)

        var x0 = x.get(2 * i)
        var x1 = x.get(2 * i + 1)

        x.set(2 * i, x0 * cos_val - x1 * sin_val)
        x.set(2 * i + 1, x0 * sin_val + x1 * cos_val)


fn apply_rope_batch(
    mut q_batch: Tensor[DType.float32],
    mut k_batch: Tensor[DType.float32],
    table: RoPETable,
    start_pos: Int,
    num_tokens: Int,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
):
    """Apply RoPE to batched Q and K tensors in-place.

    Processes all tokens at once instead of per-token extraction loops.

    Args:
        q_batch: Q vectors [num_tokens * num_q_heads * head_dim].
        k_batch: K vectors [num_tokens * num_kv_heads * head_dim].
        table: Precomputed RoPE cos/sin table.
        start_pos: Starting position index.
        num_tokens: Number of tokens in the batch.
        num_q_heads: Number of Q heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.
    """
    var half_dim = head_dim // 2
    var q_dim = num_q_heads * head_dim
    var kv_dim = num_kv_heads * head_dim

    for t in range(num_tokens):
        var pos = start_pos + t
        if pos >= table.max_seq_len:
            break
        var table_offset = pos * half_dim
        var q_base = t * q_dim
        var k_base = t * kv_dim

        # Apply RoPE to all Q heads for this token
        for h in range(num_q_heads):
            var head_off = q_base + h * head_dim
            for i in range(half_dim):
                var cos_val = table.cos_table.get(table_offset + i)
                var sin_val = table.sin_table.get(table_offset + i)
                var x0 = q_batch.get(head_off + 2 * i)
                var x1 = q_batch.get(head_off + 2 * i + 1)
                q_batch.set(head_off + 2 * i, x0 * cos_val - x1 * sin_val)
                q_batch.set(head_off + 2 * i + 1, x0 * sin_val + x1 * cos_val)

        # Apply RoPE to all K heads for this token
        for h in range(num_kv_heads):
            var head_off = k_base + h * head_dim
            for i in range(half_dim):
                var cos_val = table.cos_table.get(table_offset + i)
                var sin_val = table.sin_table.get(table_offset + i)
                var x0 = k_batch.get(head_off + 2 * i)
                var x1 = k_batch.get(head_off + 2 * i + 1)
                k_batch.set(head_off + 2 * i, x0 * cos_val - x1 * sin_val)
                k_batch.set(head_off + 2 * i + 1, x0 * sin_val + x1 * cos_val)
