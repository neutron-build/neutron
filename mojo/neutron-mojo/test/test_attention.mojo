# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Attention Tests
# ===----------------------------------------------------------------------=== #

"""Tests for GQA multi-head attention."""

from math import abs, sqrt, exp
from neutron_mojo.nn.attention import (
    gqa_attention,
    mha_attention,
    attention_single_head,
    softmax_inplace,
    apply_causal_mask,
    dot_product,
)
from neutron_mojo.nn.kv_cache import KVCache
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


fn test_dot_product() raises:
    """Test dot product helper."""
    var a = Tensor[DType.float32](Shape(4))
    var b = Tensor[DType.float32](Shape(4))
    a.set(0, 1.0)
    a.set(1, 2.0)
    a.set(2, 3.0)
    a.set(3, 4.0)
    b.set(0, 2.0)
    b.set(1, 3.0)
    b.set(2, 4.0)
    b.set(3, 5.0)

    var dp = dot_product(a, b, 4, 0, 0)
    # 1*2 + 2*3 + 3*4 + 4*5 = 2+6+12+20 = 40
    assert_near(dp, 40.0, 1e-5, "dot product")

    # With offset
    var dp2 = dot_product(a, b, 2, 1, 2)
    # a[1]*b[2] + a[2]*b[3] = 2*4 + 3*5 = 8+15 = 23
    assert_near(dp2, 23.0, 1e-5, "dot product with offset")

    print("  dot_product: PASS")


fn test_softmax_inplace() raises:
    """Test softmax computation."""
    var s = Tensor[DType.float32](Shape(3))
    s.set(0, 1.0)
    s.set(1, 2.0)
    s.set(2, 3.0)

    softmax_inplace(s, 3)

    # softmax([1,2,3]) = [e^1, e^2, e^3] / sum
    var e1 = Float32(exp(Float64(1.0)))
    var e2 = Float32(exp(Float64(2.0)))
    var e3 = Float32(exp(Float64(3.0)))
    var total = e1 + e2 + e3

    assert_near(s.get(0), e1 / total, 1e-4, "softmax[0]")
    assert_near(s.get(1), e2 / total, 1e-4, "softmax[1]")
    assert_near(s.get(2), e3 / total, 1e-4, "softmax[2]")

    # Sum should be 1.0
    var sm = s.get(0) + s.get(1) + s.get(2)
    assert_near(sm, 1.0, 1e-4, "softmax sums to 1")

    print("  softmax_inplace: PASS")


fn test_softmax_numerical_stability() raises:
    """Test softmax with large values (numerical stability)."""
    var s = Tensor[DType.float32](Shape(3))
    s.set(0, 1000.0)
    s.set(1, 1001.0)
    s.set(2, 1002.0)

    softmax_inplace(s, 3)

    var sm = s.get(0) + s.get(1) + s.get(2)
    assert_near(sm, 1.0, 1e-3, "stable softmax sums to 1")
    assert_true(s.get(2) > s.get(1), "largest input gets highest prob")
    assert_true(s.get(1) > s.get(0), "ordering preserved")

    print("  softmax_numerical_stability: PASS")


fn test_causal_mask() raises:
    """Test causal masking."""
    var s = Tensor[DType.float32](Shape(5))
    for i in range(5):
        s.set(i, 1.0)

    apply_causal_mask(s, query_pos=2, seq_len=5)

    # Positions 0,1,2 should be unmasked
    assert_near(s.get(0), 1.0, 1e-5, "pos 0 unmasked")
    assert_near(s.get(1), 1.0, 1e-5, "pos 1 unmasked")
    assert_near(s.get(2), 1.0, 1e-5, "pos 2 unmasked")
    # Positions 3,4 should be masked to -inf
    assert_true(s.get(3) < -1e8, "pos 3 masked")
    assert_true(s.get(4) < -1e8, "pos 4 masked")

    print("  causal_mask: PASS")


fn test_single_head_uniform_values() raises:
    """Test attention with uniform cached values returns the value."""
    var head_dim = 4
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=head_dim)

    # Cache 3 positions, all with same value vector [1,2,3,4]
    for _ in range(3):
        var k = Tensor[DType.float32](Shape(head_dim))
        var v = Tensor[DType.float32](Shape(head_dim))
        k.set(0, 0.1)
        k.set(1, 0.1)
        k.set(2, 0.1)
        k.set(3, 0.1)
        v.set(0, 1.0)
        v.set(1, 2.0)
        v.set(2, 3.0)
        v.set(3, 4.0)
        cache.append_kv(k, v, num_new_tokens=1)

    # Any query should produce output close to [1,2,3,4]
    # since all values are the same
    var q = Tensor[DType.float32](Shape(head_dim))
    q.set(0, 1.0)
    q.set(1, 0.0)
    q.set(2, 0.0)
    q.set(3, 0.0)

    var scale = Float32(1.0 / sqrt(Float64(head_dim)))
    var out = attention_single_head(q, cache, 0, 0, 3, head_dim, scale)

    assert_near(out.get(0), 1.0, 1e-3, "uniform v[0]")
    assert_near(out.get(1), 2.0, 1e-3, "uniform v[1]")
    assert_near(out.get(2), 3.0, 1e-3, "uniform v[2]")
    assert_near(out.get(3), 4.0, 1e-3, "uniform v[3]")

    print("  single_head_uniform_values: PASS")


fn test_single_head_peaked_attention() raises:
    """Test attention with one key matching query strongly."""
    var head_dim = 2
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=head_dim)

    # Position 0: key=[0,0], value=[10, 20]
    var k0 = Tensor[DType.float32](Shape(head_dim))
    var v0 = Tensor[DType.float32](Shape(head_dim))
    k0.set(0, 0.0)
    k0.set(1, 0.0)
    v0.set(0, 10.0)
    v0.set(1, 20.0)
    cache.append_kv(k0, v0, num_new_tokens=1)

    # Position 1: key=[10,10], value=[100, 200]
    var k1 = Tensor[DType.float32](Shape(head_dim))
    var v1 = Tensor[DType.float32](Shape(head_dim))
    k1.set(0, 10.0)
    k1.set(1, 10.0)
    v1.set(0, 100.0)
    v1.set(1, 200.0)
    cache.append_kv(k1, v1, num_new_tokens=1)

    # Query strongly aligned with position 1's key
    var q = Tensor[DType.float32](Shape(head_dim))
    q.set(0, 10.0)
    q.set(1, 10.0)

    var scale = Float32(1.0 / sqrt(Float64(head_dim)))
    var out = attention_single_head(q, cache, 0, 0, 2, head_dim, scale)

    # Should attend mostly to position 1
    assert_true(out.get(0) > 90.0, "should be close to 100")
    assert_true(out.get(1) > 180.0, "should be close to 200")

    print("  single_head_peaked_attention: PASS")


fn test_mha_basic() raises:
    """Test standard MHA with 2 heads."""
    var head_dim = 2
    var num_heads = 2
    var cache = KVCache(max_seq_len=4, num_kv_heads=2, head_dim=head_dim)

    # Add one cached position
    # Head 0: k=[1,0], v=[10,20]
    # Head 1: k=[0,1], v=[30,40]
    var k = Tensor[DType.float32](Shape(num_heads * head_dim))
    var v = Tensor[DType.float32](Shape(num_heads * head_dim))
    k.set(0, 1.0)
    k.set(1, 0.0)
    k.set(2, 0.0)
    k.set(3, 1.0)
    v.set(0, 10.0)
    v.set(1, 20.0)
    v.set(2, 30.0)
    v.set(3, 40.0)
    cache.append_kv(k, v, num_new_tokens=1)

    # Query: head 0 = [1,0], head 1 = [0,1]
    var q = Tensor[DType.float32](Shape(num_heads * head_dim))
    q.set(0, 1.0)
    q.set(1, 0.0)
    q.set(2, 0.0)
    q.set(3, 1.0)

    var out = mha_attention(q, cache, num_heads, head_dim)

    # With single cached position, softmax is [1.0], so output = value
    assert_near(out.get(0), 10.0, 1e-3, "mha head 0 d0")
    assert_near(out.get(1), 20.0, 1e-3, "mha head 0 d1")
    assert_near(out.get(2), 30.0, 1e-3, "mha head 1 d0")
    assert_near(out.get(3), 40.0, 1e-3, "mha head 1 d1")

    print("  mha_basic: PASS")


fn test_gqa_head_mapping() raises:
    """Test GQA where 4 Q heads share 2 KV heads."""
    var head_dim = 2
    var num_q_heads = 4
    var num_kv_heads = 2
    var cache = KVCache(max_seq_len=4, num_kv_heads=num_kv_heads, head_dim=head_dim)

    # 1 cached position
    # KV head 0: k=[1,0], v=[10,20]
    # KV head 1: k=[0,1], v=[30,40]
    var k = Tensor[DType.float32](Shape(num_kv_heads * head_dim))
    var v = Tensor[DType.float32](Shape(num_kv_heads * head_dim))
    k.set(0, 1.0)
    k.set(1, 0.0)
    k.set(2, 0.0)
    k.set(3, 1.0)
    v.set(0, 10.0)
    v.set(1, 20.0)
    v.set(2, 30.0)
    v.set(3, 40.0)
    cache.append_kv(k, v, num_new_tokens=1)

    # 4 Q heads, all with same query
    var q = Tensor[DType.float32](Shape(num_q_heads * head_dim))
    for i in range(num_q_heads * head_dim):
        q.set(i, 1.0)

    var out = gqa_attention(q, cache, num_q_heads, num_kv_heads, head_dim)

    # Q heads 0,1 map to KV head 0 (group_size=2)
    # Q heads 2,3 map to KV head 1
    # With single position, output = value regardless of query
    assert_near(out.get(0), 10.0, 1e-3, "Q0→KV0 d0")
    assert_near(out.get(1), 20.0, 1e-3, "Q0→KV0 d1")
    assert_near(out.get(2), 10.0, 1e-3, "Q1→KV0 d0")
    assert_near(out.get(3), 20.0, 1e-3, "Q1→KV0 d1")
    assert_near(out.get(4), 30.0, 1e-3, "Q2→KV1 d0")
    assert_near(out.get(5), 40.0, 1e-3, "Q2→KV1 d1")
    assert_near(out.get(6), 30.0, 1e-3, "Q3→KV1 d0")
    assert_near(out.get(7), 40.0, 1e-3, "Q3→KV1 d1")

    print("  gqa_head_mapping: PASS")


fn test_gqa_output_shape() raises:
    """Test that GQA output has correct shape."""
    var head_dim = 4
    var num_q_heads = 8
    var num_kv_heads = 2
    var cache = KVCache(max_seq_len=4, num_kv_heads=num_kv_heads, head_dim=head_dim)

    # Cache 1 position
    var k = Tensor[DType.float32](Shape(num_kv_heads * head_dim))
    var v = Tensor[DType.float32](Shape(num_kv_heads * head_dim))
    for i in range(num_kv_heads * head_dim):
        k.set(i, 0.1)
        v.set(i, 1.0)
    cache.append_kv(k, v, num_new_tokens=1)

    var q = Tensor[DType.float32](Shape(num_q_heads * head_dim))
    for i in range(num_q_heads * head_dim):
        q.set(i, 1.0)

    var out = gqa_attention(q, cache, num_q_heads, num_kv_heads, head_dim)

    assert_true(out.numel() == num_q_heads * head_dim, "output size = 32")

    print("  gqa_output_shape: PASS")


fn test_attention_weights_sum_to_one() raises:
    """Test that attention weights sum to 1 (verified via output)."""
    var head_dim = 2
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=head_dim)

    # Cache 3 positions with value = [1, 1]
    for _ in range(3):
        var k = Tensor[DType.float32](Shape(head_dim))
        var v = Tensor[DType.float32](Shape(head_dim))
        k.set(0, 0.5)
        k.set(1, 0.5)
        v.set(0, 1.0)
        v.set(1, 1.0)
        cache.append_kv(k, v, num_new_tokens=1)

    var q = Tensor[DType.float32](Shape(head_dim))
    q.set(0, 1.0)
    q.set(1, 1.0)

    var scale = Float32(1.0 / sqrt(Float64(head_dim)))
    var out = attention_single_head(q, cache, 0, 0, 3, head_dim, scale)

    # Since all values are [1,1], output should be [1,1]
    # (weighted average of identical values)
    assert_near(out.get(0), 1.0, 1e-3, "weights sum to 1")
    assert_near(out.get(1), 1.0, 1e-3, "weights sum to 1")

    print("  attention_weights_sum_to_one: PASS")


fn main() raises:
    print("test_attention:")

    test_dot_product()
    test_softmax_inplace()
    test_softmax_numerical_stability()
    test_causal_mask()
    test_single_head_uniform_values()
    test_single_head_peaked_attention()
    test_mha_basic()
    test_gqa_head_mapping()
    test_gqa_output_shape()
    test_attention_weights_sum_to_one()

    print("ALL PASSED")
