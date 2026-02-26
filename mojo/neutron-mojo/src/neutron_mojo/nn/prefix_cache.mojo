# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Prefix Caching for KV Cache Reuse
# ===----------------------------------------------------------------------=== #

"""Hash-based prefix caching for KV cache reuse across requests.

When multiple requests share a common prompt prefix (e.g., system prompt +
few-shot examples), prefix caching avoids redundant prefill computation
by storing and reusing KV cache snapshots.

Performance: ~2-5x prefill speedup for multi-turn conversations.

Usage:
    var pc = PrefixCache(max_entries=8, ...)
    var match = pc.find_prefix(input_ids)
    if match.matched_len > 0:
        # Copy cached KV data instead of running prefill
        pc.restore_to_cache(match, cache)
    # Only prefill remaining tokens
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.kv_cache import MultiLayerKVCache


# ===----------------------------------------------------------------------=== #
# Prefix Hashing
# ===----------------------------------------------------------------------=== #

fn hash_token_sequence(tokens: List[Int], length: Int) -> Int:
    """Compute a hash of a token sequence prefix.

    Uses FNV-1a style hashing for fast, reasonable distribution.

    Args:
        tokens: Token ID sequence.
        length: Number of tokens to hash (prefix length).

    Returns:
        Hash value.
    """
    var h = 2166136261  # FNV offset basis
    var n = length
    if n > len(tokens):
        n = len(tokens)
    for i in range(n):
        h = h ^ tokens[i]
        h = (h * 16777619) & 0x7FFFFFFF  # FNV prime, keep positive
    return h


fn tokens_match(a: List[Int], b: List[Int], length: Int) -> Bool:
    """Check if two token sequences match for the first `length` tokens.

    Args:
        a: First token sequence.
        b: Second token sequence.
        length: Number of tokens to compare.

    Returns:
        True if first `length` tokens are identical.
    """
    if len(a) < length or len(b) < length:
        return False
    for i in range(length):
        if a[i] != b[i]:
            return False
    return True


# ===----------------------------------------------------------------------=== #
# Cache Entry
# ===----------------------------------------------------------------------=== #

struct PrefixCacheEntry(Copyable, Movable):
    """A cached KV state for a token prefix.

    Stores the full KV cache data for a specific token prefix so it
    can be restored for future requests sharing that prefix.
    """
    var prefix_tokens: List[Int]     # The token sequence this entry covers
    var prefix_hash: Int             # Hash for fast lookup
    var prefix_len: Int              # Number of tokens cached
    var key_data: Tensor[DType.float32]    # Snapshot of KV key data
    var value_data: Tensor[DType.float32]  # Snapshot of KV value data
    var num_layers: Int
    var num_kv_heads: Int
    var head_dim: Int
    var hit_count: Int               # Number of times this entry was used
    var max_seq_len: Int             # Max sequence length of the cache

    fn __init__(out self):
        self.prefix_tokens = List[Int]()
        self.prefix_hash = 0
        self.prefix_len = 0
        self.key_data = Tensor[DType.float32](Shape(1))
        self.value_data = Tensor[DType.float32](Shape(1))
        self.num_layers = 0
        self.num_kv_heads = 0
        self.head_dim = 0
        self.hit_count = 0
        self.max_seq_len = 0

    fn __copyinit__(out self, existing: Self):
        self.prefix_tokens = List[Int]()
        for i in range(len(existing.prefix_tokens)):
            self.prefix_tokens.append(existing.prefix_tokens[i])
        self.prefix_hash = existing.prefix_hash
        self.prefix_len = existing.prefix_len
        self.num_layers = existing.num_layers
        self.num_kv_heads = existing.num_kv_heads
        self.head_dim = existing.head_dim
        self.hit_count = existing.hit_count
        self.max_seq_len = existing.max_seq_len
        var total = existing.num_layers * existing.max_seq_len * existing.num_kv_heads * existing.head_dim
        if total <= 0:
            total = 1
        self.key_data = Tensor[DType.float32](Shape(total))
        self.value_data = Tensor[DType.float32](Shape(total))
        for i in range(total):
            self.key_data.set(i, existing.key_data.get(i))
            self.value_data.set(i, existing.value_data.get(i))

    fn copy(self) -> PrefixCacheEntry:
        """Return a copy of this entry."""
        var e = PrefixCacheEntry()
        for i in range(len(self.prefix_tokens)):
            e.prefix_tokens.append(self.prefix_tokens[i])
        e.prefix_hash = self.prefix_hash
        e.prefix_len = self.prefix_len
        e.num_layers = self.num_layers
        e.num_kv_heads = self.num_kv_heads
        e.head_dim = self.head_dim
        e.hit_count = self.hit_count
        e.max_seq_len = self.max_seq_len
        var total = self.num_layers * self.max_seq_len * self.num_kv_heads * self.head_dim
        if total <= 0:
            total = 1
        e.key_data = Tensor[DType.float32](Shape(total))
        e.value_data = Tensor[DType.float32](Shape(total))
        for i in range(total):
            e.key_data.set(i, self.key_data.get(i))
            e.value_data.set(i, self.value_data.get(i))
        return e^

    fn __moveinit__(out self, deinit other: Self):
        self.prefix_tokens = other.prefix_tokens^
        self.prefix_hash = other.prefix_hash
        self.prefix_len = other.prefix_len
        self.key_data = other.key_data^
        self.value_data = other.value_data^
        self.num_layers = other.num_layers
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim
        self.hit_count = other.hit_count
        self.max_seq_len = other.max_seq_len


# ===----------------------------------------------------------------------=== #
# Prefix Match Result
# ===----------------------------------------------------------------------=== #

struct PrefixMatch(Copyable, Movable):
    """Result of a prefix cache lookup."""
    var entry_idx: Int        # Index into cache entries (-1 if no match)
    var matched_len: Int      # Number of prefix tokens matched

    fn __init__(out self):
        self.entry_idx = -1
        self.matched_len = 0

    fn __init__(out self, entry_idx: Int, matched_len: Int):
        self.entry_idx = entry_idx
        self.matched_len = matched_len

    fn __copyinit__(out self, existing: Self):
        self.entry_idx = existing.entry_idx
        self.matched_len = existing.matched_len

    fn __moveinit__(out self, deinit other: Self):
        self.entry_idx = other.entry_idx
        self.matched_len = other.matched_len

    fn is_hit(self) -> Bool:
        """Whether a prefix match was found."""
        return self.entry_idx >= 0 and self.matched_len > 0


# ===----------------------------------------------------------------------=== #
# Prefix Cache
# ===----------------------------------------------------------------------=== #

struct PrefixCache(Movable):
    """LRU-style prefix cache for KV cache reuse.

    Stores up to max_entries KV cache snapshots keyed by token prefixes.
    Uses hit count for eviction (least-used entry is evicted when full).
    """
    var entries: List[PrefixCacheEntry]
    var max_entries: Int
    var num_layers: Int
    var num_kv_heads: Int
    var head_dim: Int
    var max_seq_len: Int
    var total_hits: Int
    var total_misses: Int

    fn __init__(out self, max_entries: Int, num_layers: Int,
                num_kv_heads: Int, head_dim: Int, max_seq_len: Int):
        """Create a prefix cache.

        Args:
            max_entries: Maximum number of cached prefixes.
            num_layers: Number of transformer layers.
            num_kv_heads: Number of KV heads per layer.
            head_dim: Head dimension.
            max_seq_len: Maximum sequence length.
        """
        self.entries = List[PrefixCacheEntry]()
        self.max_entries = max_entries
        self.num_layers = num_layers
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim
        self.max_seq_len = max_seq_len
        self.total_hits = 0
        self.total_misses = 0

    fn __moveinit__(out self, deinit other: Self):
        self.entries = other.entries^
        self.max_entries = other.max_entries
        self.num_layers = other.num_layers
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim
        self.max_seq_len = other.max_seq_len
        self.total_hits = other.total_hits
        self.total_misses = other.total_misses

    fn find_prefix(mut self, input_ids: List[Int]) -> PrefixMatch:
        """Find the longest matching prefix in the cache.

        Args:
            input_ids: Token sequence to match against.

        Returns:
            PrefixMatch with the best match (or no match).
        """
        var best_idx = -1
        var best_len = 0

        for i in range(len(self.entries)):
            var entry_len = self.entries[i].prefix_len
            var entry_hash = self.entries[i].prefix_hash

            # Quick hash check first
            var query_hash = hash_token_sequence(input_ids, entry_len)
            if query_hash != entry_hash:
                continue

            # Full prefix comparison
            if entry_len <= len(input_ids) and tokens_match(
                input_ids, self.entries[i].prefix_tokens, entry_len
            ):
                if entry_len > best_len:
                    best_len = entry_len
                    best_idx = i

        if best_idx >= 0:
            self.entries[best_idx].hit_count += 1
            self.total_hits += 1
        else:
            self.total_misses += 1

        return PrefixMatch(best_idx, best_len)

    fn store(mut self, input_ids: List[Int], prefix_len: Int,
             cache: MultiLayerKVCache):
        """Store a KV cache snapshot for a token prefix.

        If the cache is full, evicts the least-used entry.

        Args:
            input_ids: Full token sequence.
            prefix_len: Number of prefix tokens to store.
            cache: KV cache to snapshot.
        """
        # Calculate storage size
        var stride = self.num_kv_heads * self.head_dim
        var data_per_layer = self.max_seq_len * stride
        var total = self.num_layers * data_per_layer

        # Create entry
        var entry = PrefixCacheEntry()
        entry.prefix_len = prefix_len
        entry.prefix_hash = hash_token_sequence(input_ids, prefix_len)
        entry.num_layers = self.num_layers
        entry.num_kv_heads = self.num_kv_heads
        entry.head_dim = self.head_dim
        entry.max_seq_len = self.max_seq_len
        entry.hit_count = 0

        # Copy prefix tokens
        for i in range(prefix_len):
            if i < len(input_ids):
                entry.prefix_tokens.append(input_ids[i])

        # Snapshot KV data (only the filled portion per layer)
        entry.key_data = Tensor[DType.float32](Shape(total))
        entry.value_data = Tensor[DType.float32](Shape(total))

        for layer in range(self.num_layers):
            var layer_base = layer * data_per_layer
            var filled = prefix_len * stride
            for i in range(filled):
                entry.key_data.set(layer_base + i,
                    cache.key_data.get(layer_base + i))
                entry.value_data.set(layer_base + i,
                    cache.value_data.get(layer_base + i))

        # Evict if needed
        if len(self.entries) >= self.max_entries:
            self._evict_least_used()

        self.entries.append(entry^)

    fn restore_to_cache(self, prefix_match: PrefixMatch,
                        mut cache: MultiLayerKVCache):
        """Restore cached KV data into a live KV cache.

        Args:
            match: The prefix match result.
            cache: KV cache to populate (modified in-place).
        """
        if not prefix_match.is_hit() or prefix_match.entry_idx >= len(self.entries):
            return

        var entry_idx = prefix_match.entry_idx
        var prefix_len = prefix_match.matched_len
        var stride = self.num_kv_heads * self.head_dim
        var data_per_layer = self.max_seq_len * stride

        for layer in range(self.num_layers):
            var layer_base = layer * data_per_layer
            var filled = prefix_len * stride
            for i in range(filled):
                cache.key_data.set(layer_base + i,
                    self.entries[entry_idx].key_data.get(layer_base + i))
                cache.value_data.set(layer_base + i,
                    self.entries[entry_idx].value_data.get(layer_base + i))
            cache.lengths[layer] = prefix_len

    fn _evict_least_used(mut self):
        """Evict the entry with the lowest hit count."""
        if len(self.entries) == 0:
            return

        var min_idx = 0
        var min_hits = self.entries[0].hit_count
        for i in range(1, len(self.entries)):
            if self.entries[i].hit_count < min_hits:
                min_hits = self.entries[i].hit_count
                min_idx = i

        # Remove by rebuilding list without the evicted entry
        var new_entries = List[PrefixCacheEntry]()
        for i in range(len(self.entries)):
            if i != min_idx:
                new_entries.append(self.entries[i].copy())
        self.entries = new_entries^

    fn num_entries(self) -> Int:
        """Number of cached entries."""
        return len(self.entries)

    fn hit_rate(self) -> Float64:
        """Cache hit rate as a fraction."""
        var total = self.total_hits + self.total_misses
        if total == 0:
            return 0.0
        return Float64(self.total_hits) / Float64(total)

    fn clear(mut self):
        """Clear all cached entries."""
        self.entries = List[PrefixCacheEntry]()
        self.total_hits = 0
        self.total_misses = 0
