# ===----------------------------------------------------------------------=== #
# Neutron Mojo — KV Cache Eviction Policies
# ===----------------------------------------------------------------------=== #

"""KV cache eviction for long-context inference within bounded memory.

Two eviction strategies:

1. StreamingLLM: Keep initial "sink" tokens (attention sinks) + a sliding
   window of recent tokens. When the cache fills, evict the middle portion.
   Simple, effective, O(1) per eviction decision.

2. H2O (Heavy-Hitter Oracle): Track cumulative attention scores per position.
   When the cache fills, evict positions with lowest total attention.
   Better quality than StreamingLLM but requires score tracking overhead.

Usage:
    var policy = streaming_policy(sink_tokens=4, window_size=64)
    # ... during generation ...
    if should_evict(cache, policy):
        streaming_evict(cache, policy.sink_tokens, policy.window_size)
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.kv_cache import MultiLayerKVCache


# ===----------------------------------------------------------------------=== #
# Eviction Policy Configuration
# ===----------------------------------------------------------------------=== #

struct EvictionPolicy(Copyable, Movable):
    """Configuration for KV cache eviction.

    Modes:
    - 0: No eviction (default)
    - 1: StreamingLLM (sink + window)
    - 2: H2O (attention-score based)
    """
    var mode: Int
    var sink_tokens: Int     # Initial tokens to always keep
    var window_size: Int     # Recent tokens to keep (StreamingLLM)
    var budget: Int          # Max positions to retain (H2O)

    fn __init__(out self):
        self.mode = 0
        self.sink_tokens = 4
        self.window_size = 64
        self.budget = 128

    fn __copyinit__(out self, existing: Self):
        self.mode = existing.mode
        self.sink_tokens = existing.sink_tokens
        self.window_size = existing.window_size
        self.budget = existing.budget

    fn __moveinit__(out self, deinit other: Self):
        self.mode = other.mode
        self.sink_tokens = other.sink_tokens
        self.window_size = other.window_size
        self.budget = other.budget

    fn capacity(self) -> Int:
        """Effective capacity after eviction."""
        if self.mode == 1:
            return self.sink_tokens + self.window_size
        elif self.mode == 2:
            return self.budget
        return 0


fn no_eviction() -> EvictionPolicy:
    """Create a no-eviction policy."""
    return EvictionPolicy()


fn streaming_policy(sink_tokens: Int, window_size: Int) -> EvictionPolicy:
    """Create a StreamingLLM eviction policy.

    Args:
        sink_tokens: Number of initial tokens to always keep (attention sinks).
        window_size: Number of recent tokens to keep.

    Returns:
        EvictionPolicy configured for StreamingLLM.
    """
    var p = EvictionPolicy()
    p.mode = 1
    p.sink_tokens = sink_tokens
    p.window_size = window_size
    return p^


fn h2o_policy(budget: Int, sink_tokens: Int = 4) -> EvictionPolicy:
    """Create an H2O (Heavy-Hitter Oracle) eviction policy.

    Args:
        budget: Maximum number of positions to retain.
        sink_tokens: Initial tokens to always keep (even if low-scoring).

    Returns:
        EvictionPolicy configured for H2O.
    """
    var p = EvictionPolicy()
    p.mode = 2
    p.budget = budget
    p.sink_tokens = sink_tokens
    return p^


# ===----------------------------------------------------------------------=== #
# Attention Score Tracker (for H2O)
# ===----------------------------------------------------------------------=== #

struct AttentionScoreTracker(Movable):
    """Tracks cumulative attention scores per position for H2O eviction.

    Each time attention is computed, the attention weights for each
    key position are accumulated. Positions with consistently high
    attention scores are "heavy hitters" and should be kept.
    """
    var scores: Tensor[DType.float32]   # [max_positions] cumulative attention
    var max_positions: Int
    var active_count: Int               # Number of positions with scores

    fn __init__(out self, max_positions: Int):
        self.max_positions = max_positions
        self.active_count = 0
        self.scores = Tensor[DType.float32](Shape(max_positions))
        for i in range(max_positions):
            self.scores.set(i, 0.0)

    fn __moveinit__(out self, deinit other: Self):
        self.scores = other.scores^
        self.max_positions = other.max_positions
        self.active_count = other.active_count

    fn update(mut self, attention_weights: Tensor[DType.float32], seq_len: Int):
        """Accumulate attention weights for positions.

        Args:
            attention_weights: Attention weights [seq_len], one per key position.
            seq_len: Number of key positions (should match cache length).
        """
        var n = seq_len
        if n > self.max_positions:
            n = self.max_positions
        for i in range(n):
            self.scores.set(i, self.scores.get(i) + attention_weights.get(i))
        if n > self.active_count:
            self.active_count = n

    fn update_multi_head(
        mut self,
        attention_weights: Tensor[DType.float32],
        seq_len: Int,
        num_heads: Int,
    ):
        """Accumulate attention from multiple heads (averaged).

        Args:
            attention_weights: [num_heads * seq_len] attention weights.
            seq_len: Number of key positions.
            num_heads: Number of attention heads.
        """
        var n = seq_len
        if n > self.max_positions:
            n = self.max_positions
        for pos in range(n):
            var total: Float32 = 0.0
            for h in range(num_heads):
                total += attention_weights.get(h * seq_len + pos)
            self.scores.set(pos, self.scores.get(pos) + total / Float32(num_heads))
        if n > self.active_count:
            self.active_count = n

    fn get_eviction_candidates(
        self,
        budget: Int,
        sink_tokens: Int,
    ) -> List[Int]:
        """Find positions to keep (sorted by score, always including sinks).

        Args:
            budget: Maximum positions to retain.
            sink_tokens: Initial positions to always keep.

        Returns:
            Sorted list of position indices to KEEP.
        """
        var keep = List[Int]()

        if self.active_count <= budget:
            # No eviction needed — keep all
            for i in range(self.active_count):
                keep.append(i)
            return keep^

        # Always keep sink tokens
        for i in range(sink_tokens):
            if i < self.active_count:
                keep.append(i)

        # Find top (budget - sink_tokens) positions by score among non-sink positions
        var remaining = budget - len(keep)
        if remaining <= 0:
            return keep^

        # Collect non-sink scores with indices
        var candidates = List[Int]()
        var cand_scores = List[Float32]()
        for i in range(sink_tokens, self.active_count):
            candidates.append(i)
            cand_scores.append(self.scores.get(i))

        # Selection sort to find top-K (simple, works for reasonable cache sizes)
        for k in range(remaining):
            if k >= len(candidates):
                break
            var best_idx = k
            var best_score = cand_scores[k]
            for j in range(k + 1, len(candidates)):
                if cand_scores[j] > best_score:
                    best_score = cand_scores[j]
                    best_idx = j
            # Swap
            if best_idx != k:
                var tmp_idx = candidates[k]
                candidates[k] = candidates[best_idx]
                candidates[best_idx] = tmp_idx
                var tmp_score = cand_scores[k]
                cand_scores[k] = cand_scores[best_idx]
                cand_scores[best_idx] = tmp_score

            keep.append(candidates[k])

        # Sort keep indices for sequential access
        for i in range(len(keep)):
            for j in range(i + 1, len(keep)):
                if keep[j] < keep[i]:
                    var tmp = keep[i]
                    keep[i] = keep[j]
                    keep[j] = tmp

        return keep^

    fn compact_scores(mut self, keep_indices: List[Int]):
        """Reindex scores after eviction.

        Args:
            keep_indices: Sorted list of positions that were kept.
        """
        var new_scores = Tensor[DType.float32](Shape(self.max_positions))
        for i in range(self.max_positions):
            new_scores.set(i, 0.0)
        for i in range(len(keep_indices)):
            new_scores.set(i, self.scores.get(keep_indices[i]))
        self.scores = new_scores^
        self.active_count = len(keep_indices)

    fn reset(mut self):
        """Clear all scores."""
        for i in range(self.max_positions):
            self.scores.set(i, 0.0)
        self.active_count = 0


# ===----------------------------------------------------------------------=== #
# StreamingLLM Eviction
# ===----------------------------------------------------------------------=== #

fn streaming_evict_layer(
    mut cache: MultiLayerKVCache,
    layer: Int,
    sink_tokens: Int,
    window_size: Int,
):
    """Evict middle tokens from a single layer's KV cache.

    Keeps [0..sink_tokens) and [len-window_size..len), shifts the window
    portion to immediately follow the sink tokens.

    Args:
        cache: Multi-layer KV cache (modified in place).
        layer: Layer index.
        sink_tokens: Number of initial tokens to keep.
        window_size: Number of recent tokens to keep.
    """
    var cur_len = cache.lengths[layer]
    var target = sink_tokens + window_size

    # Nothing to evict
    if cur_len <= target:
        return

    var stride = cache._stride_per_pos()
    var layer_base = cache._layer_offset(layer)

    # Copy window tokens [cur_len - window_size .. cur_len) to [sink_tokens ..]
    var src_start = cur_len - window_size
    var dst_start = sink_tokens

    for t in range(window_size):
        var src_off = layer_base + (src_start + t) * stride
        var dst_off = layer_base + (dst_start + t) * stride
        for i in range(stride):
            cache.key_data.set(dst_off + i, cache.key_data.get(src_off + i))
            cache.value_data.set(dst_off + i, cache.value_data.get(src_off + i))

    cache.lengths[layer] = target


fn streaming_evict(
    mut cache: MultiLayerKVCache,
    sink_tokens: Int,
    window_size: Int,
):
    """Evict middle tokens from all layers' KV caches.

    Args:
        cache: Multi-layer KV cache.
        sink_tokens: Number of initial tokens to keep.
        window_size: Number of recent tokens to keep.
    """
    for layer in range(cache.num_layers):
        streaming_evict_layer(cache, layer, sink_tokens, window_size)


# ===----------------------------------------------------------------------=== #
# H2O Eviction
# ===----------------------------------------------------------------------=== #

fn h2o_compact_layer(
    mut cache: MultiLayerKVCache,
    layer: Int,
    keep_indices: List[Int],
):
    """Compact a layer's KV cache to only retain specified positions.

    Args:
        cache: Multi-layer KV cache.
        layer: Layer index.
        keep_indices: Sorted list of positions to keep.
    """
    var stride = cache._stride_per_pos()
    var layer_base = cache._layer_offset(layer)
    var num_keep = len(keep_indices)

    # Compact in-place: copy kept positions to fill gaps
    for new_pos in range(num_keep):
        var old_pos = keep_indices[new_pos]
        if old_pos != new_pos:
            var src = layer_base + old_pos * stride
            var dst = layer_base + new_pos * stride
            for i in range(stride):
                cache.key_data.set(dst + i, cache.key_data.get(src + i))
                cache.value_data.set(dst + i, cache.value_data.get(src + i))

    cache.lengths[layer] = num_keep


fn h2o_evict(
    mut cache: MultiLayerKVCache,
    mut tracker: AttentionScoreTracker,
    budget: Int,
    sink_tokens: Int,
):
    """Evict lowest-attention positions from all layers.

    Uses the tracker's cumulative attention scores to decide which
    positions to keep. Always preserves sink tokens.

    Args:
        cache: Multi-layer KV cache.
        tracker: Attention score tracker.
        budget: Maximum positions to retain.
        sink_tokens: Initial positions to always keep.
    """
    var keep = tracker.get_eviction_candidates(budget, sink_tokens)

    for layer in range(cache.num_layers):
        h2o_compact_layer(cache, layer, keep)

    # Reindex tracker scores
    tracker.compact_scores(keep)


# ===----------------------------------------------------------------------=== #
# Convenience: Check + Evict
# ===----------------------------------------------------------------------=== #

fn should_evict(cache: MultiLayerKVCache, policy: EvictionPolicy) -> Bool:
    """Check if eviction should be triggered.

    For StreamingLLM: evict when cache exceeds sink + window capacity.
    For H2O: evict when cache exceeds budget.

    Args:
        cache: KV cache.
        policy: Eviction policy.

    Returns:
        True if eviction should be applied.
    """
    if policy.mode == 0:
        return False

    var cur_len = cache.current_length()
    if policy.mode == 1:
        return cur_len > policy.sink_tokens + policy.window_size
    elif policy.mode == 2:
        return cur_len > policy.budget
    return False


fn evict_if_needed(
    mut cache: MultiLayerKVCache,
    policy: EvictionPolicy,
    mut tracker: AttentionScoreTracker,
) -> Bool:
    """Apply eviction if cache exceeds policy threshold.

    Args:
        cache: KV cache.
        policy: Eviction policy.
        tracker: Attention score tracker (only used for H2O).

    Returns:
        True if eviction was performed.
    """
    if not should_evict(cache, policy):
        return False

    if policy.mode == 1:
        streaming_evict(cache, policy.sink_tokens, policy.window_size)
        return True
    elif policy.mode == 2:
        h2o_evict(cache, tracker, policy.budget, policy.sink_tokens)
        return True

    return False
