# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Fused Attention Kernel
# ===----------------------------------------------------------------------=== #

"""Fused attention that combines QK^T, scaling, causal masking, softmax, and
V weighting in a single pass per head. Avoids materializing the full
[seq_len, seq_len] attention matrix by computing online softmax.

Uses the "online softmax" trick (Milakov & Gimelshein, 2018):
Keep running max and sum of exponentials, correct at the end.
"""

from math import exp
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.kv_cache import KVCache
from neutron_mojo.nn.q_kv_cache import Q8KVCache


# ===----------------------------------------------------------------------=== #
# Fused Attention (FP32 KV Cache)
# ===----------------------------------------------------------------------=== #

fn fused_attention_head(
    query: Tensor[DType.float32],
    cache: KVCache,
    kv_head: Int,
    head_dim: Int,
    current_pos: Int,
) -> Tensor[DType.float32]:
    """Fused single-head attention with online softmax.

    Computes attention output in a single pass over the KV cache,
    without materializing the full attention weight matrix.

    Uses online softmax: maintains running max and exponential sum,
    rescaling the output accumulator when a new maximum is found.

    Args:
        query: Query vector [head_dim].
        cache: FP32 KV cache.
        kv_head: KV head index.
        head_dim: Per-head dimension.
        current_pos: Current position (for causal masking: attend to [0, current_pos]).

    Returns:
        Attention output [head_dim].
    """
    var output = Tensor[DType.float32](Shape(head_dim))
    for d in range(head_dim):
        output.set(d, 0.0)

    var seq_len = cache.length
    if seq_len == 0:
        return output^

    # Clamp attend range to [0, current_pos] for causal masking
    var attend_len = current_pos + 1
    if attend_len > seq_len:
        attend_len = seq_len

    # Scale factor: 1/sqrt(head_dim) via Newton's method
    var inv_sqrt_d: Float32 = 1.0
    if head_dim > 1:
        var df = Float32(head_dim)
        var x: Float32 = 0.5
        for _ in range(10):
            x = x * (1.5 - 0.5 * df * x * x)
        inv_sqrt_d = x

    # Online softmax pass
    var running_max: Float32 = -1e30
    var running_sum: Float32 = 0.0

    for pos in range(attend_len):
        # Compute Q dot K for this position
        var dot: Float32 = 0.0
        for d in range(head_dim):
            dot += query.get(d) * cache.get_key_at(pos, kv_head, d)
        var score = dot * inv_sqrt_d

        if score > running_max:
            # New max found: rescale existing accumulator
            var correction = Float32(exp(Float64(running_max - score)))
            running_sum *= correction
            for d in range(head_dim):
                output.set(d, output.get(d) * correction)
            running_max = score

        var w = Float32(exp(Float64(score - running_max)))
        running_sum += w

        # Accumulate weighted value
        for d in range(head_dim):
            output.set(d, output.get(d) + w * cache.get_value_at(pos, kv_head, d))

    # Normalize by sum of weights
    if running_sum > 0.0:
        for d in range(head_dim):
            output.set(d, output.get(d) / running_sum)

    return output^


fn fused_gqa_attention(
    query: Tensor[DType.float32],
    cache: KVCache,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
    current_pos: Int,
) -> Tensor[DType.float32]:
    """Fused GQA attention for all heads.

    Args:
        query: Query [num_q_heads * head_dim].
        cache: FP32 KV cache.
        num_q_heads: Number of query heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.
        current_pos: Current position for causal masking.

    Returns:
        Output [num_q_heads * head_dim].
    """
    var output = Tensor[DType.float32](Shape(num_q_heads * head_dim))
    var group_size = num_q_heads // num_kv_heads

    for qh in range(num_q_heads):
        var kv_h = qh // group_size

        var q_head = Tensor[DType.float32](Shape(head_dim))
        var q_base = qh * head_dim
        for d in range(head_dim):
            q_head.set(d, query.get(q_base + d))

        var head_out = fused_attention_head(q_head, cache, kv_h, head_dim, current_pos)

        for d in range(head_dim):
            output.set(q_base + d, head_out.get(d))

    return output^


# ===----------------------------------------------------------------------=== #
# Fused Attention (Quantized KV Cache)
# ===----------------------------------------------------------------------=== #

fn fused_q8_attention_head(
    query: Tensor[DType.float32],
    cache: Q8KVCache,
    kv_head: Int,
    head_dim: Int,
    current_pos: Int,
) -> Tensor[DType.float32]:
    """Fused attention with online softmax using quantized KV cache.

    Same algorithm as fused_attention_head but dequantizes K/V on-the-fly.

    Args:
        query: Query vector [head_dim].
        cache: Q8 KV cache.
        kv_head: KV head index.
        head_dim: Per-head dimension.
        current_pos: Current position for causal masking.

    Returns:
        Attention output [head_dim].
    """
    var output = Tensor[DType.float32](Shape(head_dim))
    for d in range(head_dim):
        output.set(d, 0.0)

    var seq_len = cache.length
    if seq_len == 0:
        return output^

    var attend_len = current_pos + 1
    if attend_len > seq_len:
        attend_len = seq_len

    var inv_sqrt_d: Float32 = 1.0
    if head_dim > 1:
        var df = Float32(head_dim)
        var x: Float32 = 0.5
        for _ in range(10):
            x = x * (1.5 - 0.5 * df * x * x)
        inv_sqrt_d = x

    var running_max: Float32 = -1e30
    var running_sum: Float32 = 0.0

    for pos in range(attend_len):
        # Dequantize and dot-product K in one pass
        var k_scale_idx = pos * cache.num_kv_heads + kv_head
        var k_scale = cache.key_scales.get(k_scale_idx)
        var k_base = pos * cache.num_kv_heads * head_dim + kv_head * head_dim

        var dot: Float32 = 0.0
        for d in range(head_dim):
            dot += query.get(d) * cache.key_data.get(k_base + d) * k_scale
        var score = dot * inv_sqrt_d

        if score > running_max:
            var correction = Float32(exp(Float64(running_max - score)))
            running_sum *= correction
            for d in range(head_dim):
                output.set(d, output.get(d) * correction)
            running_max = score

        var w = Float32(exp(Float64(score - running_max)))
        running_sum += w

        # Dequantize and accumulate V
        var v_scale_idx = pos * cache.num_kv_heads + kv_head
        var v_scale = cache.value_scales.get(v_scale_idx)
        var v_base = pos * cache.num_kv_heads * head_dim + kv_head * head_dim
        for d in range(head_dim):
            var v_val = cache.value_data.get(v_base + d) * v_scale
            output.set(d, output.get(d) + w * v_val)

    if running_sum > 0.0:
        for d in range(head_dim):
            output.set(d, output.get(d) / running_sum)

    return output^


fn fused_q8_gqa_attention(
    query: Tensor[DType.float32],
    cache: Q8KVCache,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
    current_pos: Int,
) -> Tensor[DType.float32]:
    """Fused GQA attention with quantized KV cache.

    Args:
        query: Query [num_q_heads * head_dim].
        cache: Q8 KV cache.
        num_q_heads: Number of query heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.
        current_pos: Current position for causal masking.

    Returns:
        Output [num_q_heads * head_dim].
    """
    var output = Tensor[DType.float32](Shape(num_q_heads * head_dim))
    var group_size = num_q_heads // num_kv_heads

    for qh in range(num_q_heads):
        var kv_h = qh // group_size

        var q_head = Tensor[DType.float32](Shape(head_dim))
        var q_base = qh * head_dim
        for d in range(head_dim):
            q_head.set(d, query.get(q_base + d))

        var head_out = fused_q8_attention_head(q_head, cache, kv_h, head_dim, current_pos)

        for d in range(head_dim):
            output.set(q_base + d, head_out.get(d))

    return output^
