# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sliding Window Attention Tests
# ===----------------------------------------------------------------------=== #

"""Tests for sliding window attention and ring-buffer KV cache."""

from math import abs
from neutron_mojo.nn.sliding_window import (
    SlidingWindowKVCache,
    sliding_window_attention_head,
    sliding_window_gqa_attention,
    windowed_fused_attention_head,
)
from neutron_mojo.nn.kv_cache import KVCache
from neutron_mojo.nn.fused_attention import fused_attention_head
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn test_sw_cache_creation() raises:
    """Test sliding window cache creation."""
    var cache = SlidingWindowKVCache(window_size=4, num_kv_heads=2, head_dim=3)
    assert_true(cache.window_size == 4, "window_size")
    assert_true(cache.active_length() == 0, "empty cache")
    assert_true(cache.total_length == 0, "total 0")

    print("  sw_cache_creation: PASS")


fn test_sw_cache_append_within_window() raises:
    """Test appending within window size."""
    var cache = SlidingWindowKVCache(window_size=4, num_kv_heads=1, head_dim=2)

    for i in range(3):
        var k = Tensor[DType.float32](Shape(2))
        var v = Tensor[DType.float32](Shape(2))
        k.set(0, Float32(i) * 1.0)
        k.set(1, Float32(i) * 0.5)
        v.set(0, Float32(i) * 0.1)
        v.set(1, Float32(i) * 0.2)
        cache.append_kv(k, v)

    assert_true(cache.active_length() == 3, "3 active")
    assert_true(cache.total_length == 3, "3 total")

    # Check values
    assert_near(cache.get_key_at(0, 0, 0), 0.0, 0.01, "k[0,0,0]")
    assert_near(cache.get_key_at(1, 0, 0), 1.0, 0.01, "k[1,0,0]")
    assert_near(cache.get_key_at(2, 0, 0), 2.0, 0.01, "k[2,0,0]")

    print("  sw_cache_append_within_window: PASS")


fn test_sw_cache_ring_buffer_wraps() raises:
    """Test that ring buffer wraps correctly beyond window size."""
    var cache = SlidingWindowKVCache(window_size=3, num_kv_heads=1, head_dim=1)

    # Add 5 positions to a window of 3
    for i in range(5):
        var k = Tensor[DType.float32](Shape(1))
        var v = Tensor[DType.float32](Shape(1))
        k.set(0, Float32(i) * 10.0)
        v.set(0, Float32(i))
        cache.append_kv(k, v)

    assert_true(cache.active_length() == 3, "window capped at 3")
    assert_true(cache.total_length == 5, "total is 5")

    # Should contain positions 2, 3, 4 (most recent 3)
    # logical idx 0 → oldest in window (pos 2), value = 20.0
    # logical idx 1 → pos 3, value = 30.0
    # logical idx 2 → pos 4 (newest), value = 40.0
    assert_near(cache.get_key_at(0, 0, 0), 20.0, 0.01, "oldest key = 20")
    assert_near(cache.get_key_at(1, 0, 0), 30.0, 0.01, "mid key = 30")
    assert_near(cache.get_key_at(2, 0, 0), 40.0, 0.01, "newest key = 40")

    print("  sw_cache_ring_buffer_wraps: PASS")


fn test_sw_cache_memory_bounded() raises:
    """Test that memory stays constant regardless of sequence length."""
    var cache = SlidingWindowKVCache(window_size=8, num_kv_heads=4, head_dim=16)

    var expected_bytes = 8 * 4 * 16 * 4 * 2  # window * heads * dim * sizeof(f32) * 2 (K+V)
    assert_true(cache.memory_bytes() == expected_bytes, "fixed memory")

    # Add 100 positions — memory shouldn't change
    for _ in range(100):
        var k = Tensor[DType.float32](Shape(64))
        var v = Tensor[DType.float32](Shape(64))
        for j in range(64):
            k.set(j, 0.1)
            v.set(j, 0.1)
        cache.append_kv(k, v)

    assert_true(cache.memory_bytes() == expected_bytes, "memory unchanged after 100 tokens")
    assert_true(cache.active_length() == 8, "still capped at window")

    print("  sw_cache_memory_bounded: PASS")


fn test_sw_attention_single_pos() raises:
    """Test sliding window attention with a single position."""
    var cache = SlidingWindowKVCache(window_size=4, num_kv_heads=1, head_dim=2)

    var k = Tensor[DType.float32](Shape(2))
    var v = Tensor[DType.float32](Shape(2))
    k.set(0, 1.0)
    k.set(1, 0.0)
    v.set(0, 0.5)
    v.set(1, 0.8)
    cache.append_kv(k, v)

    var query = Tensor[DType.float32](Shape(2))
    query.set(0, 1.0)
    query.set(1, 0.0)

    var out = sliding_window_attention_head(query, cache, 0, 2)
    assert_near(out.get(0), 0.5, 0.01, "single pos v[0]")
    assert_near(out.get(1), 0.8, 0.01, "single pos v[1]")

    print("  sw_attention_single_pos: PASS")


fn test_sw_attention_only_sees_window() raises:
    """Test that attention only considers tokens within the window."""
    var cache = SlidingWindowKVCache(window_size=2, num_kv_heads=1, head_dim=2)

    # Position 0: V = [10, 0] — this will be evicted
    var k0 = Tensor[DType.float32](Shape(2))
    var v0 = Tensor[DType.float32](Shape(2))
    k0.set(0, 1.0)
    k0.set(1, 0.0)
    v0.set(0, 10.0)
    v0.set(1, 0.0)
    cache.append_kv(k0, v0)

    # Position 1: V = [0, 1]
    var k1 = Tensor[DType.float32](Shape(2))
    var v1 = Tensor[DType.float32](Shape(2))
    k1.set(0, 0.0)
    k1.set(1, 1.0)
    v1.set(0, 0.0)
    v1.set(1, 1.0)
    cache.append_kv(k1, v1)

    # Position 2: V = [0, 5] — this evicts position 0
    var k2 = Tensor[DType.float32](Shape(2))
    var v2 = Tensor[DType.float32](Shape(2))
    k2.set(0, 0.5)
    k2.set(1, 0.5)
    v2.set(0, 0.0)
    v2.set(1, 5.0)
    cache.append_kv(k2, v2)

    assert_true(cache.active_length() == 2, "window of 2")

    # Attend — should NOT see position 0 (V=[10,0])
    var query = Tensor[DType.float32](Shape(2))
    query.set(0, 1.0)
    query.set(1, 1.0)
    var out = sliding_window_attention_head(query, cache, 0, 2)

    # Output should be a mix of V1=[0,1] and V2=[0,5], NOT V0=[10,0]
    assert_near(out.get(0), 0.0, 0.01, "no contribution from evicted pos 0")
    assert_true(out.get(1) > 0.5, "output has value contributions")

    print("  sw_attention_only_sees_window: PASS")


fn test_sw_gqa() raises:
    """Test GQA with sliding window cache."""
    var cache = SlidingWindowKVCache(window_size=8, num_kv_heads=2, head_dim=2)

    var k = Tensor[DType.float32](Shape(4))
    k.set(0, 1.0)
    k.set(1, 0.5)
    k.set(2, -0.5)
    k.set(3, 1.0)
    var v = Tensor[DType.float32](Shape(4))
    v.set(0, 0.3)
    v.set(1, 0.7)
    v.set(2, 0.5)
    v.set(3, -0.2)
    cache.append_kv(k, v)

    var query = Tensor[DType.float32](Shape(8))
    for i in range(8):
        query.set(i, Float32(i) * 0.2 - 0.5)

    var out = sliding_window_gqa_attention(query, cache, 4, 2, 2)
    assert_true(out.numel() == 8, "gqa output size")

    # Single position → output = V head
    assert_near(out.get(0), 0.3, 0.05, "gqa qh0 = v_head0[0]")
    assert_near(out.get(4), 0.5, 0.05, "gqa qh2 = v_head1[0]")

    print("  sw_gqa: PASS")


fn test_windowed_standard_cache() raises:
    """Test windowed attention on standard KV cache."""
    var cache = KVCache(max_seq_len=16, num_kv_heads=1, head_dim=2)

    # Add 6 positions
    for i in range(6):
        var k = Tensor[DType.float32](Shape(2))
        var v = Tensor[DType.float32](Shape(2))
        k.set(0, Float32(i))
        k.set(1, 0.0)
        v.set(0, Float32(i) * 0.1)
        v.set(1, Float32(i) * 0.5)
        cache.append_kv(k, v, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(2))
    query.set(0, 1.0)
    query.set(1, 0.0)

    # Full attention at pos 5
    var full_out = fused_attention_head(query, cache, 0, 2, 5)

    # Windowed attention at pos 5 with window=2 (only see pos 4, 5)
    var windowed_out = windowed_fused_attention_head(query, cache, 0, 2, 5, 2)

    # Full and windowed should differ (full attends to all 6, windowed to 2)
    var differs = False
    for d in range(2):
        var diff = full_out.get(d) - windowed_out.get(d)
        if diff < 0.0:
            diff = -diff
        if diff > 0.001:
            differs = True
    assert_true(differs, "windowed differs from full attention")

    print("  windowed_standard_cache: PASS")


fn test_sw_cache_reset() raises:
    """Test sliding window cache reset."""
    var cache = SlidingWindowKVCache(window_size=4, num_kv_heads=1, head_dim=2)

    var k = Tensor[DType.float32](Shape(2))
    var v = Tensor[DType.float32](Shape(2))
    k.set(0, 1.0)
    k.set(1, 1.0)
    v.set(0, 1.0)
    v.set(1, 1.0)
    cache.append_kv(k, v)
    assert_true(cache.active_length() == 1, "1 active")

    cache.reset()
    assert_true(cache.active_length() == 0, "reset to 0")
    assert_true(cache.total_length == 0, "total reset")
    assert_true(cache.write_pos == 0, "write_pos reset")

    print("  sw_cache_reset: PASS")


fn main() raises:
    print("test_sliding_window:")

    test_sw_cache_creation()
    test_sw_cache_append_within_window()
    test_sw_cache_ring_buffer_wraps()
    test_sw_cache_memory_bounded()
    test_sw_attention_single_pos()
    test_sw_attention_only_sees_window()
    test_sw_gqa()
    test_windowed_standard_cache()
    test_sw_cache_reset()

    print("ALL PASSED")
