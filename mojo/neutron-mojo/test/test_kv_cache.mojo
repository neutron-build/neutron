# ===----------------------------------------------------------------------=== #
# Neutron Mojo — KV Cache Tests
# ===----------------------------------------------------------------------=== #

"""Tests for KV cache."""

from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn test_kv_cache_creation() raises:
    """Test KV cache initialization."""
    var cache = KVCache(max_seq_len=128, num_kv_heads=8, head_dim=64)

    assert_true(cache.max_seq_len == 128, "max_seq_len")
    assert_true(cache.num_kv_heads == 8, "num_kv_heads")
    assert_true(cache.head_dim == 64, "head_dim")
    assert_true(cache.length == 0, "starts empty")
    assert_true(cache.remaining_capacity() == 128, "full capacity")
    assert_true(not cache.is_full(), "not full")

    print("  kv_cache_creation: PASS")


fn test_kv_cache_append_single() raises:
    """Test appending a single token's KV."""
    var cache = KVCache(max_seq_len=8, num_kv_heads=2, head_dim=4)

    # Create K and V for 1 token: [1 * 2 * 4] = 8 elements
    var k = Tensor[DType.float32](Shape(8))
    var v = Tensor[DType.float32](Shape(8))
    for i in range(8):
        k.set(i, Float32(i + 1))       # 1,2,3,...,8
        v.set(i, Float32(i + 10))      # 10,11,...,17

    cache.append_kv(k, v, num_new_tokens=1)

    assert_true(cache.length == 1, "length after append")
    assert_true(cache.remaining_capacity() == 7, "remaining")

    # Verify stored values: pos=0, head=0, dim=0..3
    assert_near(cache.get_key_at(0, 0, 0), 1.0, 1e-5, "k[0,0,0]")
    assert_near(cache.get_key_at(0, 0, 3), 4.0, 1e-5, "k[0,0,3]")
    assert_near(cache.get_key_at(0, 1, 0), 5.0, 1e-5, "k[0,1,0]")

    assert_near(cache.get_value_at(0, 0, 0), 10.0, 1e-5, "v[0,0,0]")
    assert_near(cache.get_value_at(0, 1, 3), 17.0, 1e-5, "v[0,1,3]")

    print("  kv_cache_append_single: PASS")


fn test_kv_cache_append_multi() raises:
    """Test appending multiple tokens at once."""
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=2)

    # 3 tokens, 1 head, 2 dims = 6 elements
    var k = Tensor[DType.float32](Shape(6))
    var v = Tensor[DType.float32](Shape(6))
    k.set(0, 1.0)
    k.set(1, 2.0)
    k.set(2, 3.0)
    k.set(3, 4.0)
    k.set(4, 5.0)
    k.set(5, 6.0)
    v.set(0, 10.0)
    v.set(1, 20.0)
    v.set(2, 30.0)
    v.set(3, 40.0)
    v.set(4, 50.0)
    v.set(5, 60.0)

    cache.append_kv(k, v, num_new_tokens=3)

    assert_true(cache.length == 3, "length after multi append")
    assert_near(cache.get_key_at(0, 0, 0), 1.0, 1e-5, "pos 0")
    assert_near(cache.get_key_at(1, 0, 0), 3.0, 1e-5, "pos 1")
    assert_near(cache.get_key_at(2, 0, 1), 6.0, 1e-5, "pos 2 dim 1")
    assert_near(cache.get_value_at(2, 0, 0), 50.0, 1e-5, "v pos 2")

    print("  kv_cache_append_multi: PASS")


fn test_kv_cache_sequential_append() raises:
    """Test appending tokens one at a time (autoregressive)."""
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=2)

    # Append token 0
    var k0 = Tensor[DType.float32](Shape(2))
    var v0 = Tensor[DType.float32](Shape(2))
    k0.set(0, 1.0)
    k0.set(1, 2.0)
    v0.set(0, 10.0)
    v0.set(1, 20.0)
    cache.append_kv(k0, v0, num_new_tokens=1)

    # Append token 1
    var k1 = Tensor[DType.float32](Shape(2))
    var v1 = Tensor[DType.float32](Shape(2))
    k1.set(0, 3.0)
    k1.set(1, 4.0)
    v1.set(0, 30.0)
    v1.set(1, 40.0)
    cache.append_kv(k1, v1, num_new_tokens=1)

    assert_true(cache.length == 2, "length after 2 appends")

    # Both tokens should be accessible
    assert_near(cache.get_key_at(0, 0, 0), 1.0, 1e-5, "token 0 key")
    assert_near(cache.get_key_at(1, 0, 0), 3.0, 1e-5, "token 1 key")
    assert_near(cache.get_value_at(0, 0, 1), 20.0, 1e-5, "token 0 value")
    assert_near(cache.get_value_at(1, 0, 1), 40.0, 1e-5, "token 1 value")

    print("  kv_cache_sequential_append: PASS")


fn test_kv_cache_head_vector() raises:
    """Test extracting full head vectors."""
    var cache = KVCache(max_seq_len=4, num_kv_heads=2, head_dim=3)

    # 1 token, 2 heads, 3 dims = 6 elements
    var k = Tensor[DType.float32](Shape(6))
    var v = Tensor[DType.float32](Shape(6))
    k.set(0, 1.0)
    k.set(1, 2.0)
    k.set(2, 3.0)
    k.set(3, 4.0)
    k.set(4, 5.0)
    k.set(5, 6.0)
    v.set(0, 10.0)
    v.set(1, 20.0)
    v.set(2, 30.0)
    v.set(3, 40.0)
    v.set(4, 50.0)
    v.set(5, 60.0)
    cache.append_kv(k, v, num_new_tokens=1)

    var kv0 = cache.get_key_head_vector(0, 0)
    assert_true(kv0.numel() == 3, "head vector size")
    assert_near(kv0.get(0), 1.0, 1e-5, "kv head0 d0")
    assert_near(kv0.get(1), 2.0, 1e-5, "kv head0 d1")
    assert_near(kv0.get(2), 3.0, 1e-5, "kv head0 d2")

    var kv1 = cache.get_key_head_vector(0, 1)
    assert_near(kv1.get(0), 4.0, 1e-5, "kv head1 d0")

    var vv1 = cache.get_value_head_vector(0, 1)
    assert_near(vv1.get(0), 40.0, 1e-5, "val head1 d0")

    print("  kv_cache_head_vector: PASS")


fn test_kv_cache_overflow() raises:
    """Test that cache overflow raises error."""
    var cache = KVCache(max_seq_len=2, num_kv_heads=1, head_dim=2)

    var k = Tensor[DType.float32](Shape(2))
    var v = Tensor[DType.float32](Shape(2))
    k.set(0, 1.0)
    k.set(1, 2.0)
    v.set(0, 1.0)
    v.set(1, 2.0)

    cache.append_kv(k, v, num_new_tokens=1)
    cache.append_kv(k, v, num_new_tokens=1)

    # Should overflow
    var overflowed = False
    try:
        cache.append_kv(k, v, num_new_tokens=1)
    except:
        overflowed = True

    assert_true(overflowed, "should overflow")
    assert_true(cache.is_full(), "should be full")

    print("  kv_cache_overflow: PASS")


fn test_kv_cache_reset() raises:
    """Test cache reset."""
    var cache = KVCache(max_seq_len=4, num_kv_heads=1, head_dim=2)

    var k = Tensor[DType.float32](Shape(2))
    var v = Tensor[DType.float32](Shape(2))
    k.set(0, 5.0)
    k.set(1, 6.0)
    v.set(0, 7.0)
    v.set(1, 8.0)
    cache.append_kv(k, v, num_new_tokens=1)
    assert_true(cache.length == 1, "length before reset")

    cache.reset()
    assert_true(cache.length == 0, "length after reset")
    assert_true(cache.remaining_capacity() == 4, "capacity after reset")

    # Data should be zeroed
    assert_near(cache.get_key_at(0, 0, 0), 0.0, 1e-5, "zeroed after reset")

    print("  kv_cache_reset: PASS")


fn test_kv_cache_stride() raises:
    """Test stride_per_pos calculation."""
    var cache = KVCache(max_seq_len=4, num_kv_heads=8, head_dim=128)

    assert_true(cache.stride_per_pos() == 1024, "8*128=1024")

    print("  kv_cache_stride: PASS")


fn test_multi_layer_creation() raises:
    """Test multi-layer KV cache creation."""
    var ml = MultiLayerKVCache(
        num_layers=32,
        max_seq_len=2048,
        num_kv_heads=8,
        head_dim=128,
    )

    assert_true(ml.num_layers == 32, "num layers")
    assert_true(ml.current_length() == 0, "starts empty")
    assert_true(ml.total_memory_bytes() == 0, "no memory used initially")

    print("  multi_layer_creation: PASS")


fn test_multi_layer_usage() raises:
    """Test using multi-layer cache."""
    var ml = MultiLayerKVCache(
        num_layers=2,
        max_seq_len=4,
        num_kv_heads=1,
        head_dim=2,
    )

    # Add a token to layer 0
    var k = Tensor[DType.float32](Shape(2))
    var v = Tensor[DType.float32](Shape(2))
    k.set(0, 1.0)
    k.set(1, 2.0)
    v.set(0, 3.0)
    v.set(1, 4.0)
    ml.append_kv(0, k, v, num_new_tokens=1)

    # Add a token to layer 1
    var k2 = Tensor[DType.float32](Shape(2))
    var v2 = Tensor[DType.float32](Shape(2))
    k2.set(0, 5.0)
    k2.set(1, 6.0)
    v2.set(0, 7.0)
    v2.set(1, 8.0)
    ml.append_kv(1, k2, v2, num_new_tokens=1)

    assert_true(ml.current_length() == 1, "both layers have 1 token")
    # 2 layers * 1 pos * 1 head * 2 dim * 4 bytes * 2 (K+V) = 32 bytes
    assert_true(ml.total_memory_bytes() == 32, "memory bytes")

    # Verify data isolation between layers
    assert_near(ml.get_key_at(0, 0, 0, 0), 1.0, 1e-5, "layer 0 key")
    assert_near(ml.get_key_at(1, 0, 0, 0), 5.0, 1e-5, "layer 1 key")
    assert_near(ml.get_value_at(0, 0, 0, 0), 3.0, 1e-5, "layer 0 val")
    assert_near(ml.get_value_at(1, 0, 0, 0), 7.0, 1e-5, "layer 1 val")

    print("  multi_layer_usage: PASS")


fn test_multi_layer_reset() raises:
    """Test resetting all layers."""
    var ml = MultiLayerKVCache(
        num_layers=2,
        max_seq_len=4,
        num_kv_heads=1,
        head_dim=2,
    )

    var k = Tensor[DType.float32](Shape(2))
    var v = Tensor[DType.float32](Shape(2))
    k.set(0, 1.0)
    k.set(1, 1.0)
    v.set(0, 1.0)
    v.set(1, 1.0)
    ml.append_kv(0, k, v, num_new_tokens=1)
    ml.append_kv(1, k, v, num_new_tokens=1)

    ml.reset_all()
    assert_true(ml.current_length() == 0, "reset all")
    assert_true(ml.total_memory_bytes() == 0, "no memory after reset")

    print("  multi_layer_reset: PASS")


fn main() raises:
    print("test_kv_cache:")

    test_kv_cache_creation()
    test_kv_cache_append_single()
    test_kv_cache_append_multi()
    test_kv_cache_sequential_append()
    test_kv_cache_head_vector()
    test_kv_cache_overflow()
    test_kv_cache_reset()
    test_kv_cache_stride()
    test_multi_layer_creation()
    test_multi_layer_usage()
    test_multi_layer_reset()

    print("ALL PASSED")
