# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantized KV Cache Tests
# ===----------------------------------------------------------------------=== #

"""Tests for INT8-quantized KV cache."""

from math import abs
from neutron_mojo.nn.q_kv_cache import (
    Q8KVCache,
    QuantResult,
    quantize_vector_q8,
    q8_attention_single_head,
    q8_gqa_attention,
)
from neutron_mojo.nn.kv_cache import KVCache
from neutron_mojo.nn.attention import attention_single_head
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


fn test_quantize_vector() raises:
    """Test vector quantization to INT8 range."""
    var src = Tensor[DType.float32](Shape(4))
    src.set(0, 1.0)
    src.set(1, -0.5)
    src.set(2, 0.25)
    src.set(3, -1.0)

    var result = quantize_vector_q8(src, 0, 4)

    # absmax = 1.0, scale = 1.0/127
    assert_near(result.scale, 1.0 / 127.0, 0.001, "scale")
    # Quantized values should be close to src/scale
    assert_near(result.data.get(0), 127.0, 1.0, "q[0]")
    assert_near(result.data.get(3), -127.0, 1.0, "q[3]")

    # Dequantized should be close to original
    assert_near(result.data.get(0) * result.scale, 1.0, 0.01, "deq[0]")
    assert_near(result.data.get(1) * result.scale, -0.5, 0.02, "deq[1]")

    print("  quantize_vector: PASS")


fn test_q8_cache_creation() raises:
    """Test Q8KVCache creation."""
    var cache = Q8KVCache(max_seq_len=16, num_kv_heads=2, head_dim=4)
    assert_true(cache.length == 0, "empty cache")
    assert_true(cache.max_seq_len == 16, "max_seq_len")
    assert_true(cache.num_kv_heads == 2, "heads")
    assert_true(cache.head_dim == 4, "head_dim")

    print("  q8_cache_creation: PASS")


fn test_q8_cache_append() raises:
    """Test appending to quantized cache."""
    var cache = Q8KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)

    var k = Tensor[DType.float32](Shape(4))
    var v = Tensor[DType.float32](Shape(4))
    k.set(0, 1.0)
    k.set(1, 0.5)
    k.set(2, -0.3)
    k.set(3, 0.8)
    v.set(0, 0.2)
    v.set(1, -0.7)
    v.set(2, 0.4)
    v.set(3, -0.1)

    cache.append_kv(k, v, num_new_tokens=1)
    assert_true(cache.length == 1, "length after append")

    # Dequantized values should be close to originals
    assert_near(cache.get_key_at(0, 0, 0), 1.0, 0.02, "k[0]")
    assert_near(cache.get_key_at(0, 0, 1), 0.5, 0.02, "k[1]")
    assert_near(cache.get_value_at(0, 0, 0), 0.2, 0.02, "v[0]")
    assert_near(cache.get_value_at(0, 0, 1), -0.7, 0.02, "v[1]")

    print("  q8_cache_append: PASS")


fn test_q8_cache_multi_position() raises:
    """Test appending multiple positions."""
    var cache = Q8KVCache(max_seq_len=8, num_kv_heads=1, head_dim=2)

    for pos in range(4):
        var k = Tensor[DType.float32](Shape(2))
        var v = Tensor[DType.float32](Shape(2))
        k.set(0, Float32(pos) * 0.5)
        k.set(1, Float32(pos) * 0.3)
        v.set(0, Float32(pos) * 0.1)
        v.set(1, Float32(pos) * 0.2)
        cache.append_kv(k, v, num_new_tokens=1)

    assert_true(cache.length == 4, "4 positions")

    # Check position 2
    assert_near(cache.get_key_at(2, 0, 0), 1.0, 0.02, "pos2 k[0]")
    assert_near(cache.get_key_at(2, 0, 1), 0.6, 0.02, "pos2 k[1]")

    print("  q8_cache_multi_position: PASS")


fn test_q8_cache_multi_head() raises:
    """Test with multiple KV heads."""
    var cache = Q8KVCache(max_seq_len=8, num_kv_heads=2, head_dim=2)

    # Key: [head0_d0, head0_d1, head1_d0, head1_d1]
    var k = Tensor[DType.float32](Shape(4))
    k.set(0, 1.0)
    k.set(1, 2.0)
    k.set(2, 3.0)
    k.set(3, 4.0)
    var v = Tensor[DType.float32](Shape(4))
    v.set(0, 0.1)
    v.set(1, 0.2)
    v.set(2, 0.3)
    v.set(3, 0.4)

    cache.append_kv(k, v, num_new_tokens=1)

    assert_near(cache.get_key_at(0, 0, 0), 1.0, 0.05, "head0 k[0]")
    assert_near(cache.get_key_at(0, 0, 1), 2.0, 0.05, "head0 k[1]")
    assert_near(cache.get_key_at(0, 1, 0), 3.0, 0.05, "head1 k[0]")
    assert_near(cache.get_key_at(0, 1, 1), 4.0, 0.05, "head1 k[1]")

    print("  q8_cache_multi_head: PASS")


fn test_q8_cache_head_vector() raises:
    """Test getting dequantized head vectors."""
    var cache = Q8KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)

    var k = Tensor[DType.float32](Shape(4))
    k.set(0, 0.5)
    k.set(1, -0.3)
    k.set(2, 0.8)
    k.set(3, -0.1)
    var v = Tensor[DType.float32](Shape(4))
    v.set(0, 1.0)
    v.set(1, 0.0)
    v.set(2, -1.0)
    v.set(3, 0.5)

    cache.append_kv(k, v, num_new_tokens=1)

    var k_vec = cache.get_key_head_vector(0, 0)
    assert_true(k_vec.numel() == 4, "key vector size")
    assert_near(k_vec.get(0), 0.5, 0.02, "k_vec[0]")
    assert_near(k_vec.get(2), 0.8, 0.02, "k_vec[2]")

    var v_vec = cache.get_value_head_vector(0, 0)
    assert_near(v_vec.get(0), 1.0, 0.02, "v_vec[0]")
    assert_near(v_vec.get(2), -1.0, 0.02, "v_vec[2]")

    print("  q8_cache_head_vector: PASS")


fn test_q8_memory_savings() raises:
    """Test memory calculation shows savings."""
    var cache = Q8KVCache(max_seq_len=128, num_kv_heads=8, head_dim=64)

    # Fill some positions
    for _ in range(32):
        var k = Tensor[DType.float32](Shape(8 * 64))
        var v = Tensor[DType.float32](Shape(8 * 64))
        for i in range(512):
            k.set(i, 0.1)
            v.set(i, 0.1)
        cache.append_kv(k, v, num_new_tokens=1)

    var q8_bytes = cache.memory_bytes()
    var fp32_bytes = cache.fp32_equivalent_bytes()

    # Q8 should use significantly less memory
    assert_true(q8_bytes < fp32_bytes, "Q8 uses less memory")
    # Roughly 4x savings (data: 1 byte vs 4 bytes, plus small scale overhead)
    var ratio = Float32(fp32_bytes) / Float32(q8_bytes)
    assert_true(ratio > 2.5, "at least 2.5x savings: " + String(ratio))

    print("  q8_memory_savings: PASS")


fn test_q8_attention_vs_fp32() raises:
    """Test Q8 attention gives similar results to FP32 attention."""
    var head_dim = 4
    var seq_len = 3

    # Create FP32 reference cache
    var fp32_cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)
    var q8_cache = Q8KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)

    # Fill both caches with same data
    for pos in range(seq_len):
        var k = Tensor[DType.float32](Shape(4))
        var v = Tensor[DType.float32](Shape(4))
        for d in range(4):
            k.set(d, Float32(pos + d) * 0.3 - 0.5)
            v.set(d, Float32(pos * d + 1) * 0.2)
        fp32_cache.append_kv(k, v, num_new_tokens=1)
        q8_cache.append_kv(k, v, num_new_tokens=1)

    # Query
    var query = Tensor[DType.float32](Shape(4))
    query.set(0, 0.5)
    query.set(1, -0.3)
    query.set(2, 0.8)
    query.set(3, 0.1)

    # FP32 attention (sig: q, cache, q_head, kv_head, seq_len, head_dim, scale)
    # scale = 1/sqrt(head_dim)
    var d_float = Float32(head_dim)
    var inv_sqrt: Float32 = 0.5
    for _ in range(10):
        inv_sqrt = inv_sqrt * (1.5 - 0.5 * d_float * inv_sqrt * inv_sqrt)
    var fp32_out = attention_single_head(query, fp32_cache, 0, 0, seq_len, head_dim, inv_sqrt)

    # Q8 attention
    var q8_out = q8_attention_single_head(query, q8_cache, 0, 0, head_dim)

    # Results should be close
    var max_err: Float32 = 0.0
    for d in range(head_dim):
        var err = fp32_out.get(d) - q8_out.get(d)
        if err < 0.0:
            err = -err
        if err > max_err:
            max_err = err

    assert_true(max_err < 0.1, "Q8 vs FP32 attention error: " + String(max_err))

    print("  q8_attention_vs_fp32: PASS")


fn test_q8_gqa_attention() raises:
    """Test GQA with quantized KV cache."""
    var head_dim = 2
    var num_q_heads = 4
    var num_kv_heads = 2

    var cache = Q8KVCache(max_seq_len=8, num_kv_heads=2, head_dim=2)

    # Add one position
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
    cache.append_kv(k, v, num_new_tokens=1)

    # Query: [4 heads * 2 dim = 8]
    var query = Tensor[DType.float32](Shape(8))
    for i in range(8):
        query.set(i, Float32(i) * 0.2 - 0.5)

    var output = q8_gqa_attention(query, cache, num_q_heads, num_kv_heads, head_dim)
    assert_true(output.numel() == 8, "output size")

    # Q heads 0,1 should use KV head 0; Q heads 2,3 should use KV head 1
    # With only one position, attention weight = 1.0, so output = V head
    assert_near(output.get(0), 0.3, 0.05, "qh0 d0 = v_head0 d0")
    assert_near(output.get(1), 0.7, 0.05, "qh0 d1 = v_head0 d1")
    assert_near(output.get(4), 0.5, 0.05, "qh2 d0 = v_head1 d0")
    assert_near(output.get(5), -0.2, 0.05, "qh2 d1 = v_head1 d1")

    print("  q8_gqa_attention: PASS")


fn test_q8_cache_reset() raises:
    """Test cache reset."""
    var cache = Q8KVCache(max_seq_len=8, num_kv_heads=1, head_dim=2)

    var k = Tensor[DType.float32](Shape(2))
    var v = Tensor[DType.float32](Shape(2))
    k.set(0, 1.0)
    k.set(1, 1.0)
    v.set(0, 1.0)
    v.set(1, 1.0)
    cache.append_kv(k, v, num_new_tokens=1)
    assert_true(cache.length == 1, "length 1")

    cache.reset()
    assert_true(cache.length == 0, "reset to 0")

    print("  q8_cache_reset: PASS")


fn main() raises:
    print("test_q_kv_cache:")

    test_quantize_vector()
    test_q8_cache_creation()
    test_q8_cache_append()
    test_q8_cache_multi_position()
    test_q8_cache_multi_head()
    test_q8_cache_head_vector()
    test_q8_memory_savings()
    test_q8_attention_vs_fp32()
    test_q8_gqa_attention()
    test_q8_cache_reset()

    print("ALL PASSED")
