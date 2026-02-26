# ===----------------------------------------------------------------------=== #
# Neutron Mojo — KV Cache for Autoregressive Generation
# ===----------------------------------------------------------------------=== #

"""Key-Value cache for transformer attention.

Stores past K and V projections so they don't need to be recomputed during
autoregressive generation. Each layer has its own KV cache.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Single-Layer KV Cache
# ===----------------------------------------------------------------------=== #

struct KVCache(Movable):
    """KV cache for a single transformer layer.

    Stores key and value tensors as flat buffers with shape:
        [max_seq_len, num_kv_heads, head_dim]

    The cache tracks how many positions have been filled via `length`.
    """
    var key_cache: Tensor[DType.float32]    # [max_seq_len * num_kv_heads * head_dim]
    var value_cache: Tensor[DType.float32]  # [max_seq_len * num_kv_heads * head_dim]
    var max_seq_len: Int
    var num_kv_heads: Int
    var head_dim: Int
    var length: Int  # Number of positions currently cached

    fn __init__(out self, max_seq_len: Int, num_kv_heads: Int, head_dim: Int):
        """Create an empty KV cache.

        Args:
            max_seq_len: Maximum sequence length.
            num_kv_heads: Number of KV heads (for GQA, fewer than Q heads).
            head_dim: Per-head dimension.
        """
        self.max_seq_len = max_seq_len
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim
        self.length = 0

        var total = max_seq_len * num_kv_heads * head_dim
        self.key_cache = Tensor[DType.float32](Shape(total))
        self.value_cache = Tensor[DType.float32](Shape(total))

    fn __moveinit__(out self, deinit other: Self):
        self.key_cache = other.key_cache^
        self.value_cache = other.value_cache^
        self.max_seq_len = other.max_seq_len
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim
        self.length = other.length

    fn stride_per_pos(self) -> Int:
        """Elements per position (num_kv_heads * head_dim)."""
        return self.num_kv_heads * self.head_dim

    fn append_kv(
        mut self,
        key: Tensor[DType.float32],
        value: Tensor[DType.float32],
        num_new_tokens: Int,
    ) raises:
        """Append new K/V entries to the cache.

        Args:
            key: New key tensor, flat [num_new_tokens * num_kv_heads * head_dim].
            value: New value tensor, same shape as key.
            num_new_tokens: Number of new positions to append.
        """
        if self.length + num_new_tokens > self.max_seq_len:
            raise Error("KV cache overflow: " + String(self.length) + " + " +
                String(num_new_tokens) + " > " + String(self.max_seq_len))

        var stride = self.stride_per_pos()
        var dst_offset = self.length * stride
        var num_elements = num_new_tokens * stride

        for i in range(num_elements):
            self.key_cache.set(dst_offset + i, key.get(i))
            self.value_cache.set(dst_offset + i, value.get(i))

        self.length += num_new_tokens

    fn get_key_at(self, pos: Int, head: Int, dim: Int) -> Float32:
        """Get a single key value.

        Args:
            pos: Sequence position.
            head: KV head index.
            dim: Head dimension index.

        Returns:
            The key value at (pos, head, dim).
        """
        var offset = pos * self.num_kv_heads * self.head_dim + head * self.head_dim + dim
        return self.key_cache.get(offset)

    fn get_value_at(self, pos: Int, head: Int, dim: Int) -> Float32:
        """Get a single value value.

        Args:
            pos: Sequence position.
            head: KV head index.
            dim: Head dimension index.

        Returns:
            The value at (pos, head, dim).
        """
        var offset = pos * self.num_kv_heads * self.head_dim + head * self.head_dim + dim
        return self.value_cache.get(offset)

    fn get_key_head_vector(self, pos: Int, head: Int) -> Tensor[DType.float32]:
        """Get the key vector for a specific position and head.

        Args:
            pos: Sequence position.
            head: KV head index.

        Returns:
            Tensor of shape [head_dim].
        """
        var result = Tensor[DType.float32](Shape(self.head_dim))
        var base = pos * self.num_kv_heads * self.head_dim + head * self.head_dim
        for d in range(self.head_dim):
            result.set(d, self.key_cache.get(base + d))
        return result^

    fn get_value_head_vector(self, pos: Int, head: Int) -> Tensor[DType.float32]:
        """Get the value vector for a specific position and head.

        Args:
            pos: Sequence position.
            head: KV head index.

        Returns:
            Tensor of shape [head_dim].
        """
        var result = Tensor[DType.float32](Shape(self.head_dim))
        var base = pos * self.num_kv_heads * self.head_dim + head * self.head_dim
        for d in range(self.head_dim):
            result.set(d, self.value_cache.get(base + d))
        return result^

    fn remaining_capacity(self) -> Int:
        """Positions still available."""
        return self.max_seq_len - self.length

    fn is_full(self) -> Bool:
        """Whether the cache is at capacity."""
        return self.length >= self.max_seq_len

    fn reset(mut self):
        """Clear the cache (reset length, zero data)."""
        self.length = 0
        var total = self.max_seq_len * self.num_kv_heads * self.head_dim
        for i in range(total):
            self.key_cache.set(i, 0.0)
            self.value_cache.set(i, 0.0)


# ===----------------------------------------------------------------------=== #
# Multi-Layer KV Cache
# ===----------------------------------------------------------------------=== #

struct MultiLayerKVCache(Movable):
    """KV caches for all transformer layers.

    Uses flat storage for all layers' K and V data, with per-layer
    length tracking via a List[Int].
    """
    var key_data: Tensor[DType.float32]     # [num_layers * max_seq_len * num_kv_heads * head_dim]
    var value_data: Tensor[DType.float32]   # same shape
    var lengths: List[Int]                   # per-layer sequence length
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
        """Create KV caches for all layers.

        Args:
            num_layers: Number of transformer layers.
            max_seq_len: Maximum sequence length.
            num_kv_heads: Number of KV heads per layer.
            head_dim: Per-head dimension.
        """
        self.num_layers = num_layers
        self.max_seq_len = max_seq_len
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim

        var layer_size = max_seq_len * num_kv_heads * head_dim
        var total = num_layers * layer_size
        self.key_data = Tensor[DType.float32](Shape(total))
        self.value_data = Tensor[DType.float32](Shape(total))

        self.lengths = List[Int]()
        for _ in range(num_layers):
            self.lengths.append(0)

    fn __moveinit__(out self, deinit other: Self):
        self.key_data = other.key_data^
        self.value_data = other.value_data^
        self.lengths = other.lengths^
        self.num_layers = other.num_layers
        self.max_seq_len = other.max_seq_len
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim

    fn _layer_offset(self, layer: Int) -> Int:
        """Base offset for a layer's data."""
        return layer * self.max_seq_len * self.num_kv_heads * self.head_dim

    fn _stride_per_pos(self) -> Int:
        """Elements per position."""
        return self.num_kv_heads * self.head_dim

    fn append_kv(
        mut self,
        layer: Int,
        key: Tensor[DType.float32],
        value: Tensor[DType.float32],
        num_new_tokens: Int,
    ) raises:
        """Append K/V to a specific layer's cache.

        Args:
            layer: Layer index.
            key: New keys [num_new_tokens * num_kv_heads * head_dim].
            value: New values, same shape.
            num_new_tokens: Number of new positions.
        """
        var cur_len = self.lengths[layer]
        if cur_len + num_new_tokens > self.max_seq_len:
            raise Error("KV cache overflow at layer " + String(layer))

        var stride = self._stride_per_pos()
        var base = self._layer_offset(layer) + cur_len * stride
        var n = num_new_tokens * stride

        for i in range(n):
            self.key_data.set(base + i, key.get(i))
            self.value_data.set(base + i, value.get(i))

        self.lengths[layer] = cur_len + num_new_tokens

    fn get_key_at(self, layer: Int, pos: Int, head: Int, dim: Int) -> Float32:
        """Get a key value from a specific layer."""
        var offset = self._layer_offset(layer) + pos * self._stride_per_pos() + head * self.head_dim + dim
        return self.key_data.get(offset)

    fn get_value_at(self, layer: Int, pos: Int, head: Int, dim: Int) -> Float32:
        """Get a value from a specific layer."""
        var offset = self._layer_offset(layer) + pos * self._stride_per_pos() + head * self.head_dim + dim
        return self.value_data.get(offset)

    fn total_memory_bytes(self) -> Int:
        """Total memory used by all caches (filled portion only)."""
        var total = 0
        for i in range(self.num_layers):
            total += self.lengths[i] * self.num_kv_heads * self.head_dim * 4 * 2
        return total

    fn reset_all(mut self):
        """Reset all layer caches."""
        for i in range(self.num_layers):
            self.lengths[i] = 0
        var total = self.num_layers * self.max_seq_len * self.num_kv_heads * self.head_dim
        for i in range(total):
            self.key_data.set(i, 0.0)
            self.value_data.set(i, 0.0)

    fn current_length(self) -> Int:
        """Current sequence length (assumes all layers in sync)."""
        if self.num_layers > 0:
            return self.lengths[0]
        return 0
