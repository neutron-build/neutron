# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Fused Attention Tests
# ===----------------------------------------------------------------------=== #

"""Tests for fused attention kernel with online softmax."""

from math import abs, exp
from neutron_mojo.nn.fused_attention import (
    fused_attention_head,
    fused_gqa_attention,
    fused_q8_attention_head,
    fused_q8_gqa_attention,
)
from neutron_mojo.nn.kv_cache import KVCache
from neutron_mojo.nn.q_kv_cache import Q8KVCache
from neutron_mojo.nn.attention import attention_single_head, gqa_attention
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


fn _inv_sqrt(d: Int) -> Float32:
    """Compute 1/sqrt(d) via Newton's method."""
    var df = Float32(d)
    var x: Float32 = 0.5
    for _ in range(10):
        x = x * (1.5 - 0.5 * df * x * x)
    return x


fn test_fused_single_position() raises:
    """Test fused attention with single cached position."""
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)

    var k = Tensor[DType.float32](Shape(4))
    var v = Tensor[DType.float32](Shape(4))
    k.set(0, 1.0)
    k.set(1, 0.0)
    k.set(2, 0.0)
    k.set(3, 0.0)
    v.set(0, 0.5)
    v.set(1, 0.3)
    v.set(2, 0.7)
    v.set(3, 0.1)
    cache.append_kv(k, v, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(4))
    query.set(0, 1.0)
    query.set(1, 0.0)
    query.set(2, 0.0)
    query.set(3, 0.0)

    # With only one position, softmax weight = 1.0, output = V
    var out = fused_attention_head(query, cache, 0, 4, 0)
    assert_near(out.get(0), 0.5, 0.01, "single pos v[0]")
    assert_near(out.get(1), 0.3, 0.01, "single pos v[1]")
    assert_near(out.get(2), 0.7, 0.01, "single pos v[2]")

    print("  fused_single_position: PASS")


fn test_fused_vs_reference() raises:
    """Test fused attention matches reference implementation."""
    var head_dim = 4
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)

    # Add 3 positions
    for pos in range(3):
        var k = Tensor[DType.float32](Shape(4))
        var v = Tensor[DType.float32](Shape(4))
        for d in range(4):
            k.set(d, Float32(pos + d) * 0.3 - 0.5)
            v.set(d, Float32(pos * d + 1) * 0.2)
        cache.append_kv(k, v, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(4))
    query.set(0, 0.5)
    query.set(1, -0.3)
    query.set(2, 0.8)
    query.set(3, 0.1)

    # Reference (non-fused)
    var scale = _inv_sqrt(head_dim)
    var ref_out = attention_single_head(query, cache, 0, 0, 3, head_dim, scale)

    # Fused
    var fused_out = fused_attention_head(query, cache, 0, head_dim, 2)

    # Should match closely
    var max_err: Float32 = 0.0
    for d in range(head_dim):
        var err = ref_out.get(d) - fused_out.get(d)
        if err < 0.0:
            err = -err
        if err > max_err:
            max_err = err

    assert_true(max_err < 0.01, "fused vs reference error: " + String(max_err))

    print("  fused_vs_reference: PASS")


fn test_fused_causal_masking() raises:
    """Test that fused attention respects causal masking."""
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=2)

    # Position 0: V = [1, 0]
    var k0 = Tensor[DType.float32](Shape(2))
    var v0 = Tensor[DType.float32](Shape(2))
    k0.set(0, 1.0)
    k0.set(1, 0.0)
    v0.set(0, 1.0)
    v0.set(1, 0.0)
    cache.append_kv(k0, v0, num_new_tokens=1)

    # Position 1: V = [0, 1]
    var k1 = Tensor[DType.float32](Shape(2))
    var v1 = Tensor[DType.float32](Shape(2))
    k1.set(0, 0.0)
    k1.set(1, 1.0)
    v1.set(0, 0.0)
    v1.set(1, 1.0)
    cache.append_kv(k1, v1, num_new_tokens=1)

    # Position 2: V = [1, 1]
    var k2 = Tensor[DType.float32](Shape(2))
    var v2 = Tensor[DType.float32](Shape(2))
    k2.set(0, 1.0)
    k2.set(1, 1.0)
    v2.set(0, 1.0)
    v2.set(1, 1.0)
    cache.append_kv(k2, v2, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(2))
    query.set(0, 1.0)
    query.set(1, 1.0)

    # At position 0: should only attend to pos 0
    var out0 = fused_attention_head(query, cache, 0, 2, 0)
    assert_near(out0.get(0), 1.0, 0.01, "causal pos0: only sees v0")
    assert_near(out0.get(1), 0.0, 0.01, "causal pos0: only sees v0")

    # At position 1: should attend to pos 0 and 1
    var out1 = fused_attention_head(query, cache, 0, 2, 1)
    # Both positions visible, output is a mix of [1,0] and [0,1]
    assert_true(out1.get(0) > 0.0 and out1.get(0) < 1.0, "causal pos1: mix")
    assert_true(out1.get(1) > 0.0 and out1.get(1) < 1.0, "causal pos1: mix")

    print("  fused_causal_masking: PASS")


fn test_fused_gqa() raises:
    """Test fused GQA attention."""
    var head_dim = 2
    var num_q_heads = 4
    var num_kv_heads = 2
    var cache = KVCache(max_seq_len=8, num_kv_heads=2, head_dim=2)

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

    var query = Tensor[DType.float32](Shape(8))
    for i in range(8):
        query.set(i, Float32(i) * 0.2 - 0.5)

    var out = fused_gqa_attention(query, cache, num_q_heads, num_kv_heads, head_dim, 0)
    assert_true(out.numel() == 8, "gqa output size")

    # Q heads 0,1 use KV head 0; Q heads 2,3 use KV head 1
    # Single position → output = V head
    assert_near(out.get(0), 0.3, 0.05, "qh0 = v_head0[0]")
    assert_near(out.get(1), 0.7, 0.05, "qh0 = v_head0[1]")
    assert_near(out.get(4), 0.5, 0.05, "qh2 = v_head1[0]")
    assert_near(out.get(5), -0.2, 0.05, "qh2 = v_head1[1]")

    print("  fused_gqa: PASS")


fn test_fused_q8_vs_fp32() raises:
    """Test fused Q8 attention matches fused FP32 attention."""
    var head_dim = 4
    var fp32_cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)
    var q8_cache = Q8KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)

    for pos in range(3):
        var k = Tensor[DType.float32](Shape(4))
        var v = Tensor[DType.float32](Shape(4))
        for d in range(4):
            k.set(d, Float32(pos + d) * 0.3 - 0.5)
            v.set(d, Float32(pos * d + 1) * 0.2)
        fp32_cache.append_kv(k, v, num_new_tokens=1)
        q8_cache.append_kv(k, v, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(4))
    query.set(0, 0.5)
    query.set(1, -0.3)
    query.set(2, 0.8)
    query.set(3, 0.1)

    var fp32_out = fused_attention_head(query, fp32_cache, 0, head_dim, 2)
    var q8_out = fused_q8_attention_head(query, q8_cache, 0, head_dim, 2)

    var max_err: Float32 = 0.0
    for d in range(head_dim):
        var err = fp32_out.get(d) - q8_out.get(d)
        if err < 0.0:
            err = -err
        if err > max_err:
            max_err = err

    assert_true(max_err < 0.1, "fused Q8 vs FP32 error: " + String(max_err))

    print("  fused_q8_vs_fp32: PASS")


fn test_fused_q8_gqa() raises:
    """Test fused Q8 GQA attention."""
    var q8_cache = Q8KVCache(max_seq_len=8, num_kv_heads=2, head_dim=2)

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
    q8_cache.append_kv(k, v, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(8))
    for i in range(8):
        query.set(i, Float32(i) * 0.2 - 0.5)

    var out = fused_q8_gqa_attention(query, q8_cache, 4, 2, 2, 0)
    assert_true(out.numel() == 8, "q8 gqa output size")

    # Single position → output ≈ V head
    assert_near(out.get(0), 0.3, 0.1, "q8 gqa qh0")
    assert_near(out.get(4), 0.5, 0.1, "q8 gqa qh2")

    print("  fused_q8_gqa: PASS")


fn test_online_softmax_accuracy() raises:
    """Test online softmax matches standard softmax for various score distributions."""
    var cache = KVCache(max_seq_len=16, num_kv_heads=1, head_dim=2)

    # Create positions with very different scores to stress online softmax
    # Position 0: very high score
    var k0 = Tensor[DType.float32](Shape(2))
    var v0 = Tensor[DType.float32](Shape(2))
    k0.set(0, 10.0)
    k0.set(1, 0.0)
    v0.set(0, 1.0)
    v0.set(1, 0.0)
    cache.append_kv(k0, v0, num_new_tokens=1)

    # Position 1: very low score
    var k1 = Tensor[DType.float32](Shape(2))
    var v1 = Tensor[DType.float32](Shape(2))
    k1.set(0, -10.0)
    k1.set(1, 0.0)
    v1.set(0, 0.0)
    v1.set(1, 1.0)
    cache.append_kv(k1, v1, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(2))
    query.set(0, 1.0)
    query.set(1, 0.0)

    # Reference softmax
    var scale = _inv_sqrt(2)
    var ref_out = attention_single_head(query, cache, 0, 0, 2, 2, scale)

    # Fused online softmax
    var fused_out = fused_attention_head(query, cache, 0, 2, 1)

    # With large score difference, position 0 should dominate
    # Output should be close to v0 = [1, 0]
    assert_near(fused_out.get(0), ref_out.get(0), 0.01, "online softmax accuracy [0]")
    assert_near(fused_out.get(1), ref_out.get(1), 0.01, "online softmax accuracy [1]")

    print("  online_softmax_accuracy: PASS")


fn test_fused_empty_cache() raises:
    """Test fused attention with empty cache returns zeros."""
    var cache = KVCache(max_seq_len=8, num_kv_heads=1, head_dim=4)
    var query = Tensor[DType.float32](Shape(4))
    query.set(0, 1.0)
    query.set(1, 1.0)
    query.set(2, 1.0)
    query.set(3, 1.0)

    var out = fused_attention_head(query, cache, 0, 4, 0)
    for d in range(4):
        assert_near(out.get(d), 0.0, 0.01, "empty cache → zero")

    print("  fused_empty_cache: PASS")


fn main() raises:
    print("test_fused_attention:")

    test_fused_single_position()
    test_fused_vs_reference()
    test_fused_causal_masking()
    test_fused_gqa()
    test_fused_q8_vs_fp32()
    test_fused_q8_gqa()
    test_online_softmax_accuracy()
    test_fused_empty_cache()

    print("ALL PASSED")
