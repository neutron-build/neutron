# ===----------------------------------------------------------------------=== #
# Neutron Mojo — KV Cache Eviction Tests
# ===----------------------------------------------------------------------=== #

"""Tests for StreamingLLM and H2O KV cache eviction policies."""

from neutron_mojo.nn.eviction import (
    EvictionPolicy,
    AttentionScoreTracker,
    no_eviction,
    streaming_policy,
    h2o_policy,
    streaming_evict_layer,
    streaming_evict,
    h2o_compact_layer,
    h2o_evict,
    should_evict,
    evict_if_needed,
)
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error("FAIL: " + msg + " got " + String(a) + " vs " + String(b))


# ===----------------------------------------------------------------------=== #
# Helper: build cache with known data
# ===----------------------------------------------------------------------=== #

fn _build_cache_with_data(
    num_layers: Int, max_seq: Int, num_kv_heads: Int, head_dim: Int, fill_len: Int,
) raises -> MultiLayerKVCache:
    """Build a cache and fill it with identifiable data.

    Position p, head h, dim d gets key = p*100 + h*10 + d, value = -(key).
    """
    var cache = MultiLayerKVCache(
        num_layers=num_layers, max_seq_len=max_seq,
        num_kv_heads=num_kv_heads, head_dim=head_dim,
    )
    var kv_dim = num_kv_heads * head_dim

    for pos in range(fill_len):
        var k = Tensor[DType.float32](Shape(kv_dim))
        var v = Tensor[DType.float32](Shape(kv_dim))
        for h in range(num_kv_heads):
            for d in range(head_dim):
                var val = Float32(pos * 100 + h * 10 + d)
                k.set(h * head_dim + d, val)
                v.set(h * head_dim + d, -val)
        for layer in range(num_layers):
            cache.append_kv(layer, k, v, num_new_tokens=1)

    return cache^


# ===----------------------------------------------------------------------=== #
# Policy Creation Tests
# ===----------------------------------------------------------------------=== #

fn test_no_eviction_policy() raises:
    """Test no-eviction policy creation."""
    var p = no_eviction()
    assert_true(p.mode == 0, "Mode should be 0")
    assert_true(p.capacity() == 0, "No capacity for no-eviction")
    print("  no_eviction_policy: PASS")


fn test_streaming_policy_creation() raises:
    """Test StreamingLLM policy creation."""
    var p = streaming_policy(sink_tokens=4, window_size=32)
    assert_true(p.mode == 1, "Mode should be 1")
    assert_true(p.sink_tokens == 4, "Sink tokens")
    assert_true(p.window_size == 32, "Window size")
    assert_true(p.capacity() == 36, "Capacity = sink + window")
    print("  streaming_policy_creation: PASS")


fn test_h2o_policy_creation() raises:
    """Test H2O policy creation."""
    var p = h2o_policy(budget=64, sink_tokens=8)
    assert_true(p.mode == 2, "Mode should be 2")
    assert_true(p.budget == 64, "Budget")
    assert_true(p.sink_tokens == 8, "Sink tokens")
    assert_true(p.capacity() == 64, "Capacity = budget")
    print("  h2o_policy_creation: PASS")


# ===----------------------------------------------------------------------=== #
# StreamingLLM Eviction Tests
# ===----------------------------------------------------------------------=== #

fn test_streaming_evict_basic() raises:
    """Test basic StreamingLLM eviction."""
    # Cache with 10 positions, keep sink=2 + window=3 = 5
    var cache = _build_cache_with_data(
        num_layers=1, max_seq=20, num_kv_heads=1, head_dim=2, fill_len=10,
    )
    assert_true(cache.lengths[0] == 10, "Start with 10 positions")

    streaming_evict(cache, sink_tokens=2, window_size=3)

    assert_true(cache.lengths[0] == 5, "After eviction: 5 positions")

    # Sink tokens [0, 1] should be preserved
    assert_near(cache.get_key_at(0, 0, 0, 0), 0.0, 1e-6, "Sink[0] key")
    assert_near(cache.get_key_at(0, 1, 0, 0), 100.0, 1e-6, "Sink[1] key")

    # Window tokens [7, 8, 9] should now be at positions [2, 3, 4]
    assert_near(cache.get_key_at(0, 2, 0, 0), 700.0, 1e-6, "Window[0] = old pos 7")
    assert_near(cache.get_key_at(0, 3, 0, 0), 800.0, 1e-6, "Window[1] = old pos 8")
    assert_near(cache.get_key_at(0, 4, 0, 0), 900.0, 1e-6, "Window[2] = old pos 9")

    # Values should also be preserved (negated)
    assert_near(cache.get_value_at(0, 2, 0, 0), -700.0, 1e-6, "Window[0] value")

    print("  streaming_evict_basic: PASS")


fn test_streaming_no_evict_when_small() raises:
    """Test that eviction is skipped when cache is below threshold."""
    var cache = _build_cache_with_data(
        num_layers=1, max_seq=20, num_kv_heads=1, head_dim=2, fill_len=5,
    )

    streaming_evict(cache, sink_tokens=2, window_size=4)

    # 5 <= 2 + 4 = 6, so no eviction
    assert_true(cache.lengths[0] == 5, "Should not evict (below threshold)")

    print("  streaming_no_evict_when_small: PASS")


fn test_streaming_evict_multi_layer() raises:
    """Test StreamingLLM eviction across multiple layers."""
    var cache = _build_cache_with_data(
        num_layers=3, max_seq=20, num_kv_heads=1, head_dim=2, fill_len=10,
    )

    streaming_evict(cache, sink_tokens=2, window_size=3)

    # All layers should be evicted consistently
    for layer in range(3):
        assert_true(cache.lengths[layer] == 5,
            "Layer " + String(layer) + " should have 5 positions")
        # Check sink preservation
        assert_near(cache.get_key_at(layer, 0, 0, 0), 0.0, 1e-6,
            "Layer " + String(layer) + " sink[0]")
        # Check window
        assert_near(cache.get_key_at(layer, 2, 0, 0), 700.0, 1e-6,
            "Layer " + String(layer) + " window[0]")

    print("  streaming_evict_multi_layer: PASS")


fn test_streaming_should_evict() raises:
    """Test should_evict predicate for StreamingLLM."""
    var cache = _build_cache_with_data(
        num_layers=1, max_seq=20, num_kv_heads=1, head_dim=2, fill_len=5,
    )
    var policy = streaming_policy(sink_tokens=2, window_size=4)

    # 5 <= 6 (sink + window) → no eviction
    assert_true(not should_evict(cache, policy), "5 <= 6: no eviction")

    # Fill to 7
    var kv_dim = 2
    for pos in range(5, 7):
        var k = Tensor[DType.float32](Shape(kv_dim))
        var v = Tensor[DType.float32](Shape(kv_dim))
        for i in range(kv_dim):
            k.set(i, Float32(pos * 100 + i))
            v.set(i, Float32(-pos * 100 - i))
        cache.append_kv(0, k, v, num_new_tokens=1)

    # 7 > 6 → should evict
    assert_true(should_evict(cache, policy), "7 > 6: should evict")

    print("  streaming_should_evict: PASS")


fn test_streaming_evict_if_needed() raises:
    """Test evict_if_needed integration for StreamingLLM."""
    var cache = _build_cache_with_data(
        num_layers=1, max_seq=20, num_kv_heads=1, head_dim=2, fill_len=10,
    )
    var policy = streaming_policy(sink_tokens=2, window_size=3)
    var tracker = AttentionScoreTracker(20)  # Unused for StreamingLLM

    var evicted = evict_if_needed(cache, policy, tracker)
    assert_true(evicted, "Should have evicted")
    assert_true(cache.lengths[0] == 5, "After eviction: 5 positions")

    # Second call: no eviction needed
    evicted = evict_if_needed(cache, policy, tracker)
    assert_true(not evicted, "Should not evict again")

    print("  streaming_evict_if_needed: PASS")


# ===----------------------------------------------------------------------=== #
# H2O Eviction Tests
# ===----------------------------------------------------------------------=== #

fn test_h2o_score_tracking() raises:
    """Test AttentionScoreTracker accumulation."""
    var tracker = AttentionScoreTracker(10)

    # Simulate attention over 5 positions
    var weights = Tensor[DType.float32](Shape(5))
    weights.set(0, 0.1)
    weights.set(1, 0.3)
    weights.set(2, 0.05)
    weights.set(3, 0.5)
    weights.set(4, 0.05)
    tracker.update(weights, 5)

    assert_near(tracker.scores.get(0), 0.1, 1e-6, "Score[0]")
    assert_near(tracker.scores.get(3), 0.5, 1e-6, "Score[3]")
    assert_true(tracker.active_count == 5, "5 active positions")

    # Second round of attention
    var weights2 = Tensor[DType.float32](Shape(5))
    weights2.set(0, 0.2)
    weights2.set(1, 0.1)
    weights2.set(2, 0.4)
    weights2.set(3, 0.1)
    weights2.set(4, 0.2)
    tracker.update(weights2, 5)

    assert_near(tracker.scores.get(0), 0.3, 1e-6, "Score[0] after 2nd")
    assert_near(tracker.scores.get(2), 0.45, 1e-6, "Score[2] after 2nd")
    assert_near(tracker.scores.get(3), 0.6, 1e-6, "Score[3] after 2nd")

    print("  h2o_score_tracking: PASS")


fn test_h2o_eviction_candidates() raises:
    """Test that eviction candidates are selected correctly."""
    var tracker = AttentionScoreTracker(8)

    # Set up scores: positions with varying importance
    # Scores: [0.9, 0.1, 0.8, 0.2, 0.7, 0.3, 0.6, 0.4]
    var weights = Tensor[DType.float32](Shape(8))
    weights.set(0, 0.9)
    weights.set(1, 0.1)
    weights.set(2, 0.8)
    weights.set(3, 0.2)
    weights.set(4, 0.7)
    weights.set(5, 0.3)
    weights.set(6, 0.6)
    weights.set(7, 0.4)
    tracker.update(weights, 8)

    # Budget=5, sink=2: keep positions 0,1 (sink) + top-3 non-sink by score
    # Non-sink scores: pos2=0.8, pos3=0.2, pos4=0.7, pos5=0.3, pos6=0.6, pos7=0.4
    # Top-3: pos2(0.8), pos4(0.7), pos6(0.6)
    # Keep: [0, 1, 2, 4, 6] sorted
    var keep = tracker.get_eviction_candidates(budget=5, sink_tokens=2)

    assert_true(len(keep) == 5, "Should keep 5 positions, got " + String(len(keep)))
    assert_true(keep[0] == 0, "Keep[0] = 0 (sink)")
    assert_true(keep[1] == 1, "Keep[1] = 1 (sink)")
    assert_true(keep[2] == 2, "Keep[2] = 2 (high score)")
    assert_true(keep[3] == 4, "Keep[3] = 4 (high score)")
    assert_true(keep[4] == 6, "Keep[4] = 6 (high score)")

    print("  h2o_eviction_candidates: PASS")


fn test_h2o_no_eviction_when_within_budget() raises:
    """Test that H2O returns all positions when within budget."""
    var tracker = AttentionScoreTracker(8)
    var weights = Tensor[DType.float32](Shape(4))
    for i in range(4):
        weights.set(i, Float32(i) * 0.1)
    tracker.update(weights, 4)

    var keep = tracker.get_eviction_candidates(budget=8, sink_tokens=2)
    assert_true(len(keep) == 4, "Should keep all 4 (within budget of 8)")

    print("  h2o_no_eviction_when_within_budget: PASS")


fn test_h2o_evict_basic() raises:
    """Test H2O eviction on a cache."""
    var cache = _build_cache_with_data(
        num_layers=1, max_seq=20, num_kv_heads=1, head_dim=2, fill_len=8,
    )
    var tracker = AttentionScoreTracker(20)

    # Assign scores: high for positions 0,1 (sink), 3, 5, 7
    var weights = Tensor[DType.float32](Shape(8))
    weights.set(0, 0.9)   # sink
    weights.set(1, 0.8)   # sink
    weights.set(2, 0.1)   # evict
    weights.set(3, 0.7)   # keep
    weights.set(4, 0.05)  # evict
    weights.set(5, 0.6)   # keep
    weights.set(6, 0.02)  # evict
    weights.set(7, 0.5)   # keep
    tracker.update(weights, 8)

    # Budget=5, sink=2 → keep [0, 1, 3, 5, 7]
    h2o_evict(cache, tracker, budget=5, sink_tokens=2)

    assert_true(cache.lengths[0] == 5, "Should have 5 positions after eviction")

    # Verify compacted data
    # New pos 0 = old pos 0 (key=0)
    assert_near(cache.get_key_at(0, 0, 0, 0), 0.0, 1e-6, "Pos 0 = old 0")
    # New pos 1 = old pos 1 (key=100)
    assert_near(cache.get_key_at(0, 1, 0, 0), 100.0, 1e-6, "Pos 1 = old 1")
    # New pos 2 = old pos 3 (key=300)
    assert_near(cache.get_key_at(0, 2, 0, 0), 300.0, 1e-6, "Pos 2 = old 3")
    # New pos 3 = old pos 5 (key=500)
    assert_near(cache.get_key_at(0, 3, 0, 0), 500.0, 1e-6, "Pos 3 = old 5")
    # New pos 4 = old pos 7 (key=700)
    assert_near(cache.get_key_at(0, 4, 0, 0), 700.0, 1e-6, "Pos 4 = old 7")

    # Tracker scores should be compacted too
    assert_true(tracker.active_count == 5, "Tracker should have 5 active")

    print("  h2o_evict_basic: PASS")


fn test_h2o_preserves_sink() raises:
    """Test that H2O always preserves sink tokens even if low-scoring."""
    var tracker = AttentionScoreTracker(10)

    # Sink tokens have LOW scores, non-sink have HIGH scores
    var weights = Tensor[DType.float32](Shape(6))
    weights.set(0, 0.01)   # sink - low score but kept
    weights.set(1, 0.01)   # sink - low score but kept
    weights.set(2, 0.9)
    weights.set(3, 0.8)
    weights.set(4, 0.7)
    weights.set(5, 0.6)
    tracker.update(weights, 6)

    var keep = tracker.get_eviction_candidates(budget=4, sink_tokens=2)

    assert_true(len(keep) == 4, "Should keep 4")
    assert_true(keep[0] == 0, "Sink 0 preserved despite low score")
    assert_true(keep[1] == 1, "Sink 1 preserved despite low score")

    print("  h2o_preserves_sink: PASS")


fn test_h2o_evict_if_needed() raises:
    """Test evict_if_needed integration for H2O."""
    var cache = _build_cache_with_data(
        num_layers=1, max_seq=20, num_kv_heads=1, head_dim=2, fill_len=8,
    )
    var policy = h2o_policy(budget=5, sink_tokens=2)
    var tracker = AttentionScoreTracker(20)

    # Set up scores
    var weights = Tensor[DType.float32](Shape(8))
    for i in range(8):
        weights.set(i, Float32(8 - i) * 0.1)  # Descending: 0.8, 0.7, ...
    tracker.update(weights, 8)

    # 8 > budget(5) → should evict
    var evicted = evict_if_needed(cache, policy, tracker)
    assert_true(evicted, "Should have evicted")
    assert_true(cache.lengths[0] == 5, "After eviction: 5 positions")

    # Now at budget → no eviction
    evicted = evict_if_needed(cache, policy, tracker)
    assert_true(not evicted, "Should not evict again (at budget)")

    print("  h2o_evict_if_needed: PASS")


# ===----------------------------------------------------------------------=== #
# End-to-End: Generation with Eviction
# ===----------------------------------------------------------------------=== #

fn test_generation_with_streaming_eviction() raises:
    """Test that generation works with StreamingLLM eviction."""
    var params = tiny_test_params()
    var model = Model(params)

    # Initialize model with non-trivial weights
    for i in range(model.layer_weights.numel()):
        model.layer_weights.set(i, Float32(i % 13) * 0.02 - 0.12)
    for v in range(params.vocab_size):
        for d in range(params.hidden_dim):
            model.embed.set(v * params.hidden_dim + d, Float32(v * params.hidden_dim + d) * 0.01)
            model.lm_head.set(v * params.hidden_dim + d, Float32(v + d) * 0.1)
    for layer in range(params.num_layers):
        var off = model._layer_offsets(layer)
        for i in range(params.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)

    # Create a small cache that will overflow
    var max_seq = 8
    var cache = MultiLayerKVCache(
        num_layers=params.num_layers, max_seq_len=max_seq,
        num_kv_heads=params.num_kv_heads, head_dim=params.head_dim,
    )
    var rope = RoPETable(head_dim=params.head_dim, max_seq_len=max_seq)

    var policy = streaming_policy(sink_tokens=2, window_size=4)
    var tracker = AttentionScoreTracker(max_seq)

    # Generate more tokens than cache can hold
    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)

    # Prefill
    for i in range(len(prompt)):
        _ = model.forward(prompt[i], cache, rope, pos=i)

    # Decode with eviction
    var generated = List[Int]()
    var pos = len(prompt)
    var prev_token = prompt[len(prompt) - 1]

    for _ in range(10):  # Generate 10 tokens (more than cache capacity)
        # Check and evict if needed
        _ = evict_if_needed(cache, policy, tracker)

        var logits = model.forward(prev_token, cache, rope, pos=pos)

        # Argmax
        var best = 0
        var best_val = logits.get(0)
        for i in range(1, params.vocab_size):
            if logits.get(i) > best_val:
                best_val = logits.get(i)
                best = i

        generated.append(best)
        prev_token = best
        pos += 1

    assert_true(len(generated) == 10, "Should generate 10 tokens")
    for i in range(len(generated)):
        assert_true(generated[i] >= 0 and generated[i] < params.vocab_size,
            "Token " + String(i) + " valid: " + String(generated[i]))

    # Cache should be within bounds
    assert_true(cache.lengths[0] <= policy.sink_tokens + policy.window_size + 1,
        "Cache within bounds: " + String(cache.lengths[0]))

    print("  generation_with_streaming_eviction: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_eviction:")

    # Policy tests
    test_no_eviction_policy()
    test_streaming_policy_creation()
    test_h2o_policy_creation()

    # StreamingLLM tests
    test_streaming_evict_basic()
    test_streaming_no_evict_when_small()
    test_streaming_evict_multi_layer()
    test_streaming_should_evict()
    test_streaming_evict_if_needed()

    # H2O tests
    test_h2o_score_tracking()
    test_h2o_eviction_candidates()
    test_h2o_no_eviction_when_within_budget()
    test_h2o_evict_basic()
    test_h2o_preserves_sink()
    test_h2o_evict_if_needed()

    # End-to-end
    test_generation_with_streaming_eviction()

    print("ALL PASSED (16 tests)")
