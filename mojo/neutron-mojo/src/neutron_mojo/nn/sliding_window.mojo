# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sliding Window Attention
# ===----------------------------------------------------------------------=== #

"""Sliding window attention for Mistral-style models.

Limits attention to a fixed window of recent positions, enabling bounded
memory usage regardless of sequence length. Uses the same online softmax
approach as fused_attention.

Mistral 7B uses window_size=4096, meaning each token attends to at most
the 4096 most recent positions.
"""

from math import exp
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.kv_cache import KVCache


# ===----------------------------------------------------------------------=== #
# Sliding Window KV Cache
# ===----------------------------------------------------------------------=== #

struct SlidingWindowKVCache(Movable):
    """KV cache with a fixed-size sliding window.

    Uses a ring buffer: positions wrap around when the window is full.
    Only stores the most recent `window_size` positions.
    """
    var key_cache: Tensor[DType.float32]
    var value_cache: Tensor[DType.float32]
    var window_size: Int
    var num_kv_heads: Int
    var head_dim: Int
    var total_length: Int  # Total tokens seen (may exceed window_size)
    var write_pos: Int     # Current write position in ring buffer

    fn __init__(out self, window_size: Int, num_kv_heads: Int, head_dim: Int):
        self.window_size = window_size
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim
        self.total_length = 0
        self.write_pos = 0

        var total = window_size * num_kv_heads * head_dim
        self.key_cache = Tensor[DType.float32](Shape(total))
        self.value_cache = Tensor[DType.float32](Shape(total))

    fn __moveinit__(out self, deinit other: Self):
        self.key_cache = other.key_cache^
        self.value_cache = other.value_cache^
        self.window_size = other.window_size
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim
        self.total_length = other.total_length
        self.write_pos = other.write_pos

    fn _stride(self) -> Int:
        return self.num_kv_heads * self.head_dim

    fn append_kv(
        mut self,
        key: Tensor[DType.float32],
        value: Tensor[DType.float32],
    ):
        """Append a single position's K/V to the ring buffer.

        Args:
            key: Key vector [num_kv_heads * head_dim].
            value: Value vector [num_kv_heads * head_dim].
        """
        var stride = self._stride()
        var base = self.write_pos * stride

        for i in range(stride):
            self.key_cache.set(base + i, key.get(i))
            self.value_cache.set(base + i, value.get(i))

        self.write_pos = (self.write_pos + 1) % self.window_size
        self.total_length += 1

    fn active_length(self) -> Int:
        """Number of valid positions in the cache."""
        if self.total_length < self.window_size:
            return self.total_length
        return self.window_size

    fn get_ring_pos(self, logical_idx: Int) -> Int:
        """Map a logical index [0, active_length) to ring buffer position.

        When total_length <= window_size, logical == physical.
        When total_length > window_size, the oldest entry is at write_pos.
        """
        if self.total_length <= self.window_size:
            return logical_idx
        return (self.write_pos + logical_idx) % self.window_size

    fn get_key_at(self, logical_idx: Int, head: Int, dim: Int) -> Float32:
        """Get key value at logical position."""
        var ring = self.get_ring_pos(logical_idx)
        var offset = ring * self._stride() + head * self.head_dim + dim
        return self.key_cache.get(offset)

    fn get_value_at(self, logical_idx: Int, head: Int, dim: Int) -> Float32:
        """Get value at logical position."""
        var ring = self.get_ring_pos(logical_idx)
        var offset = ring * self._stride() + head * self.head_dim + dim
        return self.value_cache.get(offset)

    fn memory_bytes(self) -> Int:
        """Fixed memory usage (always window_size, regardless of sequence length)."""
        return self.window_size * self.num_kv_heads * self.head_dim * 4 * 2

    fn reset(mut self):
        """Clear the cache."""
        self.total_length = 0
        self.write_pos = 0


# ===----------------------------------------------------------------------=== #
# Sliding Window Fused Attention
# ===----------------------------------------------------------------------=== #

fn sliding_window_attention_head(
    query: Tensor[DType.float32],
    cache: SlidingWindowKVCache,
    kv_head: Int,
    head_dim: Int,
) -> Tensor[DType.float32]:
    """Fused attention with sliding window constraint.

    Attends only to positions within the window. Uses online softmax.

    Args:
        query: Query vector [head_dim].
        cache: Sliding window KV cache.
        kv_head: KV head index.
        head_dim: Per-head dimension.

    Returns:
        Attention output [head_dim].
    """
    var output = Tensor[DType.float32](Shape(head_dim))
    for d in range(head_dim):
        output.set(d, 0.0)

    var active = cache.active_length()
    if active == 0:
        return output^

    # Scale: 1/sqrt(head_dim)
    var inv_sqrt_d: Float32 = 1.0
    if head_dim > 1:
        var df = Float32(head_dim)
        var x: Float32 = 0.5
        for _ in range(10):
            x = x * (1.5 - 0.5 * df * x * x)
        inv_sqrt_d = x

    # Online softmax over all active positions
    var running_max: Float32 = -1e30
    var running_sum: Float32 = 0.0

    for idx in range(active):
        var dot: Float32 = 0.0
        for d in range(head_dim):
            dot += query.get(d) * cache.get_key_at(idx, kv_head, d)
        var score = dot * inv_sqrt_d

        if score > running_max:
            var correction = Float32(exp(Float64(running_max - score)))
            running_sum *= correction
            for d in range(head_dim):
                output.set(d, output.get(d) * correction)
            running_max = score

        var w = Float32(exp(Float64(score - running_max)))
        running_sum += w

        for d in range(head_dim):
            output.set(d, output.get(d) + w * cache.get_value_at(idx, kv_head, d))

    if running_sum > 0.0:
        for d in range(head_dim):
            output.set(d, output.get(d) / running_sum)

    return output^


fn sliding_window_gqa_attention(
    query: Tensor[DType.float32],
    cache: SlidingWindowKVCache,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
) -> Tensor[DType.float32]:
    """GQA attention with sliding window.

    Args:
        query: Query [num_q_heads * head_dim].
        cache: Sliding window KV cache.
        num_q_heads: Number of query heads.
        num_kv_heads: Number of KV heads.
        head_dim: Per-head dimension.

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

        var head_out = sliding_window_attention_head(q_head, cache, kv_h, head_dim)

        for d in range(head_dim):
            output.set(q_base + d, head_out.get(d))

    return output^


# ===----------------------------------------------------------------------=== #
# Windowed Attention on Standard KV Cache
# ===----------------------------------------------------------------------=== #

fn windowed_fused_attention_head(
    query: Tensor[DType.float32],
    cache: KVCache,
    kv_head: Int,
    head_dim: Int,
    current_pos: Int,
    window_size: Int,
) -> Tensor[DType.float32]:
    """Fused attention with window constraint on a standard KV cache.

    Like fused_attention_head but limits attention range to
    [max(0, current_pos - window_size + 1), current_pos].

    Args:
        query: Query vector [head_dim].
        cache: Standard FP32 KV cache.
        kv_head: KV head index.
        head_dim: Per-head dimension.
        current_pos: Current sequence position.
        window_size: Maximum attention window.

    Returns:
        Attention output [head_dim].
    """
    var output = Tensor[DType.float32](Shape(head_dim))
    for d in range(head_dim):
        output.set(d, 0.0)

    var seq_len = cache.length
    if seq_len == 0:
        return output^

    # Clamp attend range: causal + window
    var attend_end = current_pos + 1
    if attend_end > seq_len:
        attend_end = seq_len

    var attend_start = current_pos - window_size + 1
    if attend_start < 0:
        attend_start = 0

    if attend_start >= attend_end:
        return output^

    var inv_sqrt_d: Float32 = 1.0
    if head_dim > 1:
        var df = Float32(head_dim)
        var x: Float32 = 0.5
        for _ in range(10):
            x = x * (1.5 - 0.5 * df * x * x)
        inv_sqrt_d = x

    var running_max: Float32 = -1e30
    var running_sum: Float32 = 0.0

    for pos in range(attend_start, attend_end):
        var dot: Float32 = 0.0
        for d in range(head_dim):
            dot += query.get(d) * cache.get_key_at(pos, kv_head, d)
        var score = dot * inv_sqrt_d

        if score > running_max:
            var correction = Float32(exp(Float64(running_max - score)))
            running_sum *= correction
            for d in range(head_dim):
                output.set(d, output.get(d) * correction)
            running_max = score

        var w = Float32(exp(Float64(score - running_max)))
        running_sum += w

        for d in range(head_dim):
            output.set(d, output.get(d) + w * cache.get_value_at(pos, kv_head, d))

    if running_sum > 0.0:
        for d in range(head_dim):
            output.set(d, output.get(d) / running_sum)

    return output^
