# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantized KV Cache
# ===----------------------------------------------------------------------=== #

"""INT8-quantized KV cache for memory-efficient generation.

Stores keys and values as INT8 with per-position-per-head scale factors,
reducing memory usage by ~4x compared to FP32. Dequantizes on-the-fly
during attention computation.
"""

from math import abs as math_abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Quantization Helpers
# ===----------------------------------------------------------------------=== #

struct QuantResult(Movable):
    """Result of quantizing a vector: scale factor + quantized data."""
    var scale: Float32
    var data: Tensor[DType.float32]

    fn __init__(out self, scale: Float32, var data: Tensor[DType.float32]):
        self.scale = scale
        self.data = data^

    fn __moveinit__(out self, deinit other: Self):
        self.scale = other.scale
        self.data = other.data^


fn quantize_vector_q8(
    src: Tensor[DType.float32],
    offset: Int,
    length: Int,
) -> QuantResult:
    """Quantize a float vector to INT8 range [-127, 127].

    Returns QuantResult with scale factor and quantized values.

    Args:
        src: Source tensor.
        offset: Start offset in src.
        length: Number of elements to quantize.

    Returns:
        QuantResult where data = round(src / scale), scale = absmax / 127.
    """
    # Find absmax
    var absmax: Float32 = 0.0
    for i in range(length):
        var v = src.get(offset + i)
        var av = v
        if av < 0.0:
            av = -av
        if av > absmax:
            absmax = av

    var scale: Float32 = 1.0
    if absmax > 0.0:
        scale = absmax / 127.0

    var quantized = Tensor[DType.float32](Shape(length))
    for i in range(length):
        var v = src.get(offset + i) / scale
        # Clamp to [-127, 127] and round
        if v > 127.0:
            v = 127.0
        elif v < -127.0:
            v = -127.0
        # Round to nearest integer
        if v >= 0.0:
            quantized.set(i, Float32(Int(v + 0.5)))
        else:
            quantized.set(i, Float32(Int(v - 0.5)))

    return QuantResult(scale, quantized^)


fn dequantize_value(quantized_val: Float32, scale: Float32) -> Float32:
    """Dequantize a single INT8 value back to FP32."""
    return quantized_val * scale


# ===----------------------------------------------------------------------=== #
# Quantized KV Cache (Single Layer)
# ===----------------------------------------------------------------------=== #

struct Q8KVCache(Movable):
    """INT8-quantized KV cache for a single layer.

    Layout:
        key_data:   [max_seq_len * num_kv_heads * head_dim] (INT8 as Float32)
        value_data: [max_seq_len * num_kv_heads * head_dim] (INT8 as Float32)
        key_scales:   [max_seq_len * num_kv_heads] (one scale per head per position)
        value_scales: [max_seq_len * num_kv_heads]

    Memory: ~(head_dim + 4) bytes per KV entry vs 4*head_dim for FP32.
    For head_dim=128: ~132 bytes vs 512 bytes = ~3.9x reduction.
    """
    var key_data: Tensor[DType.float32]
    var value_data: Tensor[DType.float32]
    var key_scales: Tensor[DType.float32]
    var value_scales: Tensor[DType.float32]
    var max_seq_len: Int
    var num_kv_heads: Int
    var head_dim: Int
    var length: Int

    fn __init__(out self, max_seq_len: Int, num_kv_heads: Int, head_dim: Int):
        self.max_seq_len = max_seq_len
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim
        self.length = 0

        var data_size = max_seq_len * num_kv_heads * head_dim
        var scale_size = max_seq_len * num_kv_heads
        self.key_data = Tensor[DType.float32](Shape(data_size))
        self.value_data = Tensor[DType.float32](Shape(data_size))
        self.key_scales = Tensor[DType.float32](Shape(scale_size))
        self.value_scales = Tensor[DType.float32](Shape(scale_size))

    fn __moveinit__(out self, deinit other: Self):
        self.key_data = other.key_data^
        self.value_data = other.value_data^
        self.key_scales = other.key_scales^
        self.value_scales = other.value_scales^
        self.max_seq_len = other.max_seq_len
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim
        self.length = other.length

    fn append_kv(
        mut self,
        key: Tensor[DType.float32],
        value: Tensor[DType.float32],
        num_new_tokens: Int,
    ) raises:
        """Append FP32 K/V and quantize in-place.

        Args:
            key: FP32 keys [num_new_tokens * num_kv_heads * head_dim].
            value: FP32 values, same shape.
            num_new_tokens: Positions to append.
        """
        if self.length + num_new_tokens > self.max_seq_len:
            raise Error("Q8KVCache overflow")

        var stride = self.num_kv_heads * self.head_dim

        for t in range(num_new_tokens):
            var pos = self.length + t
            for h in range(self.num_kv_heads):
                var src_offset = t * stride + h * self.head_dim
                var dst_offset = pos * stride + h * self.head_dim
                var scale_idx = pos * self.num_kv_heads + h

                # Quantize key head vector
                var k_result = quantize_vector_q8(key, src_offset, self.head_dim)
                self.key_scales.set(scale_idx, k_result.scale)
                for d in range(self.head_dim):
                    self.key_data.set(dst_offset + d, k_result.data.get(d))

                # Quantize value head vector
                var v_result = quantize_vector_q8(value, src_offset, self.head_dim)
                self.value_scales.set(scale_idx, v_result.scale)
                for d in range(self.head_dim):
                    self.value_data.set(dst_offset + d, v_result.data.get(d))

        self.length += num_new_tokens

    fn get_key_at(self, pos: Int, head: Int, dim: Int) -> Float32:
        """Get dequantized key value."""
        var data_offset = pos * self.num_kv_heads * self.head_dim + head * self.head_dim + dim
        var scale_idx = pos * self.num_kv_heads + head
        return self.key_data.get(data_offset) * self.key_scales.get(scale_idx)

    fn get_value_at(self, pos: Int, head: Int, dim: Int) -> Float32:
        """Get dequantized value."""
        var data_offset = pos * self.num_kv_heads * self.head_dim + head * self.head_dim + dim
        var scale_idx = pos * self.num_kv_heads + head
        return self.value_data.get(data_offset) * self.value_scales.get(scale_idx)

    fn get_key_head_vector(self, pos: Int, head: Int) -> Tensor[DType.float32]:
        """Get dequantized key vector for a position and head."""
        var result = Tensor[DType.float32](Shape(self.head_dim))
        var scale_idx = pos * self.num_kv_heads + head
        var scale = self.key_scales.get(scale_idx)
        var base = pos * self.num_kv_heads * self.head_dim + head * self.head_dim
        for d in range(self.head_dim):
            result.set(d, self.key_data.get(base + d) * scale)
        return result^

    fn get_value_head_vector(self, pos: Int, head: Int) -> Tensor[DType.float32]:
        """Get dequantized value vector for a position and head."""
        var result = Tensor[DType.float32](Shape(self.head_dim))
        var scale_idx = pos * self.num_kv_heads + head
        var scale = self.value_scales.get(scale_idx)
        var base = pos * self.num_kv_heads * self.head_dim + head * self.head_dim
        for d in range(self.head_dim):
            result.set(d, self.value_data.get(base + d) * scale)
        return result^

    fn memory_bytes(self) -> Int:
        """Approximate memory used by filled portion.

        INT8 data (simulated as FP32 here, but represents 1 byte/element) +
        FP32 scales (4 bytes per head per position).
        In a real INT8 implementation: head_dim + 4 bytes per KV head-position.
        """
        var filled = self.length * self.num_kv_heads
        # data: 2 * length * num_kv_heads * head_dim bytes (INT8)
        # scales: 2 * length * num_kv_heads * 4 bytes (FP32)
        return filled * self.head_dim * 2 + filled * 4 * 2

    fn fp32_equivalent_bytes(self) -> Int:
        """What the FP32 cache would use for the same data."""
        return self.length * self.num_kv_heads * self.head_dim * 4 * 2

    fn reset(mut self):
        """Clear the cache."""
        self.length = 0


# ===----------------------------------------------------------------------=== #
# Quantized Attention (using Q8KVCache)
# ===----------------------------------------------------------------------=== #

fn q8_attention_single_head(
    query: Tensor[DType.float32],
    cache: Q8KVCache,
    q_head: Int,
    kv_head: Int,
    head_dim: Int,
) -> Tensor[DType.float32]:
    """Compute attention for a single Q head using quantized KV cache.

    Dequantizes K/V on-the-fly during the dot product computation.

    Args:
        query: Query vector [head_dim].
        cache: Quantized KV cache.
        q_head: Query head index (unused for indexing, just for documentation).
        kv_head: KV head index to attend to.
        head_dim: Per-head dimension.

    Returns:
        Attention output [head_dim].
    """
    var seq_len = cache.length
    if seq_len == 0:
        return Tensor[DType.float32](Shape(head_dim))

    var inv_sqrt_d = Float32(1.0)
    if head_dim > 1:
        # 1/sqrt(head_dim) — compute manually
        var d = Float32(head_dim)
        # Newton's method for 1/sqrt: start with reasonable guess
        var x = Float32(0.5)
        for _ in range(10):
            x = x * (1.5 - 0.5 * d * x * x)
        inv_sqrt_d = x

    # Compute attention scores: Q dot K^T / sqrt(d)
    var scores = Tensor[DType.float32](Shape(seq_len))
    for pos in range(seq_len):
        var dot: Float32 = 0.0
        var k_scale_idx = pos * cache.num_kv_heads + kv_head
        var k_scale = cache.key_scales.get(k_scale_idx)
        var k_base = pos * cache.num_kv_heads * head_dim + kv_head * head_dim
        for d in range(head_dim):
            dot += query.get(d) * cache.key_data.get(k_base + d) * k_scale
        scores.set(pos, dot * inv_sqrt_d)

    # Softmax
    var max_score = scores.get(0)
    for i in range(1, seq_len):
        var v = scores.get(i)
        if v > max_score:
            max_score = v

    from math import exp
    var sum_exp: Float32 = 0.0
    for i in range(seq_len):
        var e = Float32(exp(Float64(scores.get(i) - max_score)))
        scores.set(i, e)
        sum_exp += e

    if sum_exp > 0.0:
        for i in range(seq_len):
            scores.set(i, scores.get(i) / sum_exp)

    # Weighted sum of values
    var output = Tensor[DType.float32](Shape(head_dim))
    for d in range(head_dim):
        output.set(d, 0.0)

    for pos in range(seq_len):
        var weight = scores.get(pos)
        if weight > 1e-8:
            var v_scale_idx = pos * cache.num_kv_heads + kv_head
            var v_scale = cache.value_scales.get(v_scale_idx)
            var v_base = pos * cache.num_kv_heads * head_dim + kv_head * head_dim
            for d in range(head_dim):
                var v_val = cache.value_data.get(v_base + d) * v_scale
                output.set(d, output.get(d) + weight * v_val)

    return output^


# ===----------------------------------------------------------------------=== #
# Multi-Layer Quantized KV Cache
# ===----------------------------------------------------------------------=== #

struct MultiLayerQ8KVCache(Movable):
    """Q8-quantized KV caches for all transformer layers.

    Stores K/V as INT8 (in Float32 containers) with per-head-per-position
    scale factors. ~4x memory reduction vs FP32 MultiLayerKVCache.

    Layout per layer (same as Q8KVCache):
        data: [max_seq_len * num_kv_heads * head_dim] (INT8 as Float32)
        scales: [max_seq_len * num_kv_heads] (FP32)
    """
    var key_data: Tensor[DType.float32]
    var value_data: Tensor[DType.float32]
    var key_scales: Tensor[DType.float32]
    var value_scales: Tensor[DType.float32]
    var lengths: List[Int]
    var num_layers: Int
    var max_seq_len: Int
    var num_kv_heads: Int
    var head_dim: Int

    fn __init__(
        out self,
        num_layers: Int,
        max_seq_len: Int,
        num_kv_heads: Int,
        head_dim: Int,
    ):
        self.num_layers = num_layers
        self.max_seq_len = max_seq_len
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim

        var data_per_layer = max_seq_len * num_kv_heads * head_dim
        var scale_per_layer = max_seq_len * num_kv_heads
        var total_data = num_layers * data_per_layer
        var total_scales = num_layers * scale_per_layer

        self.key_data = Tensor[DType.float32](Shape(total_data))
        self.value_data = Tensor[DType.float32](Shape(total_data))
        self.key_scales = Tensor[DType.float32](Shape(total_scales))
        self.value_scales = Tensor[DType.float32](Shape(total_scales))

        self.lengths = List[Int]()
        for _ in range(num_layers):
            self.lengths.append(0)

    fn __moveinit__(out self, deinit other: Self):
        self.key_data = other.key_data^
        self.value_data = other.value_data^
        self.key_scales = other.key_scales^
        self.value_scales = other.value_scales^
        self.lengths = other.lengths^
        self.num_layers = other.num_layers
        self.max_seq_len = other.max_seq_len
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim

    fn _data_offset(self, layer: Int) -> Int:
        """Base offset for a layer's KV data."""
        return layer * self.max_seq_len * self.num_kv_heads * self.head_dim

    fn _scale_offset(self, layer: Int) -> Int:
        """Base offset for a layer's scale data."""
        return layer * self.max_seq_len * self.num_kv_heads

    fn _stride_per_pos(self) -> Int:
        """Elements per position (num_kv_heads * head_dim)."""
        return self.num_kv_heads * self.head_dim

    fn append_kv(
        mut self,
        layer: Int,
        key: Tensor[DType.float32],
        value: Tensor[DType.float32],
        num_new_tokens: Int,
    ) raises:
        """Append FP32 K/V, quantizing to INT8 in-place.

        Args:
            layer: Layer index.
            key: FP32 keys [num_new_tokens * num_kv_heads * head_dim].
            value: FP32 values, same shape.
            num_new_tokens: Positions to append.
        """
        var cur_len = self.lengths[layer]
        if cur_len + num_new_tokens > self.max_seq_len:
            raise Error("Q8 KV cache overflow at layer " + String(layer))

        var data_base = self._data_offset(layer)
        var scale_base = self._scale_offset(layer)
        var stride = self._stride_per_pos()

        for t in range(num_new_tokens):
            var pos = cur_len + t
            for h in range(self.num_kv_heads):
                var src_offset = t * stride + h * self.head_dim
                var dst_data = data_base + pos * stride + h * self.head_dim
                var dst_scale = scale_base + pos * self.num_kv_heads + h

                # Quantize key head
                var k_result = quantize_vector_q8(key, src_offset, self.head_dim)
                self.key_scales.set(dst_scale, k_result.scale)
                for d in range(self.head_dim):
                    self.key_data.set(dst_data + d, k_result.data.get(d))

                # Quantize value head
                var v_result = quantize_vector_q8(value, src_offset, self.head_dim)
                self.value_scales.set(dst_scale, v_result.scale)
                for d in range(self.head_dim):
                    self.value_data.set(dst_data + d, v_result.data.get(d))

        self.lengths[layer] = cur_len + num_new_tokens

    fn get_layer_cache(self, layer: Int) -> Q8KVCache:
        """Extract a single layer's cache as a Q8KVCache (copy).

        Args:
            layer: Layer index.

        Returns:
            Q8KVCache with this layer's data copied in.
        """
        var cache = Q8KVCache(self.max_seq_len, self.num_kv_heads, self.head_dim)
        var data_base = self._data_offset(layer)
        var scale_base = self._scale_offset(layer)
        var cur_len = self.lengths[layer]
        var stride = self._stride_per_pos()

        for pos in range(cur_len):
            for h in range(self.num_kv_heads):
                var src_data = data_base + pos * stride + h * self.head_dim
                var src_scale = scale_base + pos * self.num_kv_heads + h
                var dst_data = pos * stride + h * self.head_dim
                var dst_scale = pos * self.num_kv_heads + h

                cache.key_scales.set(dst_scale, self.key_scales.get(src_scale))
                cache.value_scales.set(dst_scale, self.value_scales.get(src_scale))
                for d in range(self.head_dim):
                    cache.key_data.set(dst_data + d, self.key_data.get(src_data + d))
                    cache.value_data.set(dst_data + d, self.value_data.get(src_data + d))

        cache.length = cur_len
        return cache^

    fn current_length(self) -> Int:
        """Current sequence length (assumes all layers in sync)."""
        if self.num_layers > 0:
            return self.lengths[0]
        return 0

    fn memory_bytes(self) -> Int:
        """Approximate memory used (INT8 data + FP32 scales)."""
        var total = 0
        for i in range(self.num_layers):
            var filled = self.lengths[i] * self.num_kv_heads
            total += filled * self.head_dim * 2 + filled * 4 * 2
        return total

    fn fp32_equivalent_bytes(self) -> Int:
        """What FP32 cache would use for the same data."""
        var total = 0
        for i in range(self.num_layers):
            total += self.lengths[i] * self.num_kv_heads * self.head_dim * 4 * 2
        return total

    fn reset_all(mut self):
        """Reset all layer caches."""
        for i in range(self.num_layers):
            self.lengths[i] = 0


fn q8_gqa_attention(
    query: Tensor[DType.float32],
    cache: Q8KVCache,
    num_q_heads: Int,
    num_kv_heads: Int,
    head_dim: Int,
) -> Tensor[DType.float32]:
    """GQA attention using quantized KV cache.

    Args:
        query: Query [num_q_heads * head_dim].
        cache: Quantized KV cache.
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

        # Extract this Q head
        var q_head = Tensor[DType.float32](Shape(head_dim))
        var q_base = qh * head_dim
        for d in range(head_dim):
            q_head.set(d, query.get(q_base + d))

        var head_out = q8_attention_single_head(q_head, cache, qh, kv_h, head_dim)

        for d in range(head_dim):
            output.set(q_base + d, head_out.get(d))

    return output^
