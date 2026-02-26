# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Grouped Query Attention (GQA)
# ===----------------------------------------------------------------------=== #

"""Multi-head attention with Grouped Query Attention support.

GQA reduces KV heads relative to Q heads. Each KV head is shared by
a group of Q heads: group_size = num_q_heads / num_kv_heads.

For Llama-3 8B: 32 Q heads, 8 KV heads → group_size = 4.
Standard MHA: num_q_heads == num_kv_heads (group_size = 1).
"""

from math import sqrt, exp
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.nn.paged_kv_cache import PagedKVCache


# ===----------------------------------------------------------------------=== #
# Attention Scores
# ===----------------------------------------------------------------------=== #

fn dot_product(
    a: Tensor[DType.float32],
    b: Tensor[DType.float32],
    size: Int,
    a_offset: Int,
    b_offset: Int,
) -> Float32:
    """Compute dot product of two vectors at given offsets.

    Args:
        a: First tensor.
        b: Second tensor.
        size: Number of elements.
        a_offset: Starting offset in a.
        b_offset: Starting offset in b.

    Returns:
        Dot product sum.
    """
    var result: Float32 = 0.0
    for i in range(size):
        result += a.get(a_offset + i) * b.get(b_offset + i)
    return result


fn softmax_inplace(mut scores: Tensor[DType.float32], length: Int) raises:
    """Apply softmax to first `length` elements in-place.

    Args:
        scores: Tensor to apply softmax to.
        length: Number of elements to process.
    """
    if length == 0:
        return

    # Find max for numerical stability
    var max_val: Float32 = scores.get(0)
    for i in range(1, length):
        var v = scores.get(i)
        if v > max_val:
            max_val = v

    # exp and sum
    var sum_exp: Float32 = 0.0
    for i in range(length):
        var e = exp(Float64(scores.get(i) - max_val))
        var ef = Float32(e)
        scores.set(i, ef)
        sum_exp += ef

    # Normalize
    if sum_exp > 0.0:
        for i in range(length):
            scores.set(i, scores.get(i) / sum_exp)


# ===----------------------------------------------------------------------=== #
# Single-Head Attention (building block)
# ===----------------------------------------------------------------------=== #

fn attention_single_head(
    q: Tensor[DType.float32],
    cache: KVCache,
    q_head: Int,
    kv_head: Int,
    seq_len: Int,
    head_dim: Int,
    scale: Float32,
) raises -> Tensor[DType.float32]:
    """Compute attention for a single Q head against cached K/V.

    Computes: softmax(Q * K^T / sqrt(d)) * V

    Args:
        q: Query vector [head_dim] for this head.
        cache: KV cache with past keys/values.
        q_head: Which Q head this is (for debugging, not used in computation).
        kv_head: Which KV head to attend to.
        seq_len: Current sequence length (how many cached positions).
        head_dim: Per-head dimension.
        scale: Scaling factor (typically 1/sqrt(head_dim)).

    Returns:
        Attention output [head_dim].
    """
    # Compute attention scores: Q dot K for each cached position
    var scores = Tensor[DType.float32](Shape(seq_len))

    for pos in range(seq_len):
        var score: Float32 = 0.0
        for d in range(head_dim):
            score += q.get(d) * cache.get_key_at(pos, kv_head, d)
        scores.set(pos, score * scale)

    # Apply causal softmax (all positions are visible since we only
    # store past positions in the cache)
    softmax_inplace(scores, seq_len)

    # Weighted sum of values
    var output = Tensor[DType.float32](Shape(head_dim))
    for d in range(head_dim):
        output.set(d, 0.0)

    for pos in range(seq_len):
        var w = scores.get(pos)
        for d in range(head_dim):
            output.set(d, output.get(d) + w * cache.get_value_at(pos, kv_head, d))

    return output^


# ===----------------------------------------------------------------------=== #
# GQA Multi-Head Attention
# ===----------------------------------------------------------------------=== #

fn gqa_attention(
    q_all: Tensor[DType.float32],
    cache: KVCache,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
) raises -> Tensor[DType.float32]:
    """Compute GQA attention for all Q heads.

    Args:
        q_all: All query vectors [num_q_heads * head_dim].
        cache: KV cache with past keys/values.
        num_q_heads: Number of query heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.

    Returns:
        Attention output [num_q_heads * head_dim].
    """
    var seq_len = cache.length
    var scale = Float32(1.0 / sqrt(Float64(head_dim)))
    var group_size = num_q_heads // num_kv_heads

    var output = Tensor[DType.float32](Shape(num_q_heads * head_dim))

    for qh in range(num_q_heads):
        # Map Q head to its KV head
        var kv_h = qh // group_size

        # Extract this Q head's vector
        var q = Tensor[DType.float32](Shape(head_dim))
        var q_offset = qh * head_dim
        for d in range(head_dim):
            q.set(d, q_all.get(q_offset + d))

        # Compute attention for this head
        var head_out = attention_single_head(
            q, cache, qh, kv_h, seq_len, head_dim, scale
        )

        # Copy to output
        var out_offset = qh * head_dim
        for d in range(head_dim):
            output.set(out_offset + d, head_out.get(d))

    return output^


# ===----------------------------------------------------------------------=== #
# Standard Multi-Head Attention (MHA = GQA with group_size=1)
# ===----------------------------------------------------------------------=== #

fn mha_attention(
    q_all: Tensor[DType.float32],
    cache: KVCache,
    num_heads: Int,
    head_dim: Int,
) raises -> Tensor[DType.float32]:
    """Standard multi-head attention (num_q_heads == num_kv_heads).

    Args:
        q_all: All query vectors [num_heads * head_dim].
        cache: KV cache.
        num_heads: Number of heads.
        head_dim: Per-head dimension.

    Returns:
        Attention output [num_heads * head_dim].
    """
    return gqa_attention(q_all, cache, num_heads, num_heads, head_dim)


# ===----------------------------------------------------------------------=== #
# Direct Multi-Layer KV Cache Attention (zero-copy)
# ===----------------------------------------------------------------------=== #

fn gqa_attention_direct(
    q_all: Tensor[DType.float32],
    cache: MultiLayerKVCache,
    layer: Int,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
) raises -> Tensor[DType.float32]:
    """GQA attention reading directly from MultiLayerKVCache.

    Eliminates the per-layer copy into a temporary KVCache. Reads K/V
    data directly from the multi-layer cache using layer + position
    indexing.

    Args:
        q_all: All query vectors [num_q_heads * head_dim].
        cache: Multi-layer KV cache.
        layer: Layer index to read from.
        num_q_heads: Number of query heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.

    Returns:
        Attention output [num_q_heads * head_dim].
    """
    var seq_len = cache.lengths[layer]
    var scale = Float32(1.0 / sqrt(Float64(head_dim)))
    var group_size = num_q_heads // num_kv_heads

    var output = Tensor[DType.float32](Shape(num_q_heads * head_dim))

    for qh in range(num_q_heads):
        var kv_h = qh // group_size
        var q_offset = qh * head_dim

        # Compute attention scores: Q dot K for each cached position
        var scores = Tensor[DType.float32](Shape(seq_len))
        for pos in range(seq_len):
            var score: Float32 = 0.0
            for d in range(head_dim):
                score += q_all.get(q_offset + d) * cache.get_key_at(layer, pos, kv_h, d)
            scores.set(pos, score * scale)

        # Softmax
        softmax_inplace(scores, seq_len)

        # Weighted sum of values
        var out_offset = qh * head_dim
        for d in range(head_dim):
            output.set(out_offset + d, 0.0)

        for pos in range(seq_len):
            var w = scores.get(pos)
            for d in range(head_dim):
                output.set(out_offset + d,
                    output.get(out_offset + d) + w * cache.get_value_at(layer, pos, kv_h, d))

    return output^


# ===----------------------------------------------------------------------=== #
# Paged KV Cache Attention (zero-copy, page-indirect)
# ===----------------------------------------------------------------------=== #

fn paged_gqa_attention(
    q_all: Tensor[DType.float32],
    cache: PagedKVCache,
    layer: Int,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
) raises -> Tensor[DType.float32]:
    """GQA attention reading from PagedKVCache via page table indirection.

    Same computation as gqa_attention_direct but reads K/V through the
    paged cache's page table instead of contiguous memory.

    Args:
        q_all: All query vectors [num_q_heads * head_dim].
        cache: Paged KV cache.
        layer: Layer index to read from.
        num_q_heads: Number of query heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.

    Returns:
        Attention output [num_q_heads * head_dim].
    """
    var seq_len = cache.seq_len(layer)
    var scale = Float32(1.0 / sqrt(Float64(head_dim)))
    var group_size = num_q_heads // num_kv_heads

    var output = Tensor[DType.float32](Shape(num_q_heads * head_dim))

    for qh in range(num_q_heads):
        var kv_h = qh // group_size
        var q_offset = qh * head_dim

        # Compute attention scores: Q dot K for each cached position
        var scores = Tensor[DType.float32](Shape(seq_len))
        for pos in range(seq_len):
            var score: Float32 = 0.0
            for d in range(head_dim):
                score += q_all.get(q_offset + d) * cache.get_key_at(layer, pos, kv_h, d)
            scores.set(pos, score * scale)

        # Softmax
        softmax_inplace(scores, seq_len)

        # Weighted sum of values
        var out_offset = qh * head_dim
        for d in range(head_dim):
            output.set(out_offset + d, 0.0)

        for pos in range(seq_len):
            var w = scores.get(pos)
            for d in range(head_dim):
                output.set(out_offset + d,
                    output.get(out_offset + d) + w * cache.get_value_at(layer, pos, kv_h, d))

    return output^


# ===----------------------------------------------------------------------=== #
# Batched Prefill Attention (Parallel Prefill)
# ===----------------------------------------------------------------------=== #

fn gqa_attention_prefill(
    q_batch: Tensor[DType.float32],
    cache: MultiLayerKVCache,
    layer: Int,
    num_tokens: Int,
    start_pos: Int,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
) raises -> Tensor[DType.float32]:
    """Batched GQA attention for prefill with causal masking.

    Computes attention for all prompt tokens at once against the KV cache.
    Each token t (at position start_pos + t) attends to all cache positions
    up to and including start_pos + t (causal constraint).

    Eliminates the per-token KVCache copy that the old sequential path did.
    Reads directly from MultiLayerKVCache.

    Args:
        q_batch: All query vectors [num_tokens * num_q_heads * head_dim].
        cache: Multi-layer KV cache (already populated with prompt's K/V).
        layer: Layer index.
        num_tokens: Number of prompt tokens.
        start_pos: Starting position of the prompt in the cache.
        num_q_heads: Number of query heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.

    Returns:
        Attention output [num_tokens * num_q_heads * head_dim].
    """
    var total_seq_len = cache.lengths[layer]
    var scale = Float32(1.0 / sqrt(Float64(head_dim)))
    var group_size = num_q_heads // num_kv_heads
    var q_dim = num_q_heads * head_dim

    var output = Tensor[DType.float32](Shape(num_tokens * q_dim))

    for t in range(num_tokens):
        var query_pos = start_pos + t
        # This token can attend to positions 0..query_pos (inclusive)
        var attend_len = query_pos + 1
        if attend_len > total_seq_len:
            attend_len = total_seq_len

        var q_base = t * q_dim

        for qh in range(num_q_heads):
            var kv_h = qh // group_size
            var q_offset = q_base + qh * head_dim

            # Compute attention scores: Q dot K for each visible position
            var scores = Tensor[DType.float32](Shape(attend_len))
            for pos in range(attend_len):
                var score: Float32 = 0.0
                for d in range(head_dim):
                    score += q_batch.get(q_offset + d) * cache.get_key_at(layer, pos, kv_h, d)
                scores.set(pos, score * scale)

            # Softmax
            softmax_inplace(scores, attend_len)

            # Weighted sum of values
            var out_offset = q_base + qh * head_dim
            for d in range(head_dim):
                output.set(out_offset + d, 0.0)

            for pos in range(attend_len):
                var w = scores.get(pos)
                for d in range(head_dim):
                    output.set(out_offset + d,
                        output.get(out_offset + d) + w * cache.get_value_at(layer, pos, kv_h, d))

    return output^


# ===----------------------------------------------------------------------=== #
# Causal Mask Utilities
# ===----------------------------------------------------------------------=== #

fn apply_causal_mask(
    mut scores: Tensor[DType.float32],
    query_pos: Int,
    seq_len: Int,
) raises:
    """Apply causal mask to attention scores.

    Masks out future positions by setting them to -inf.

    Args:
        scores: Attention scores [seq_len].
        query_pos: Position of the query token.
        seq_len: Total sequence length.
    """
    var neg_inf: Float32 = -1e9
    for pos in range(seq_len):
        if pos > query_pos:
            scores.set(pos, neg_inf)
