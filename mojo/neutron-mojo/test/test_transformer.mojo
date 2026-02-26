# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Transformer Block Tests
# ===----------------------------------------------------------------------=== #

"""Tests for transformer block components."""

from math import abs
from neutron_mojo.nn.transformer import (
    linear,
    TransformerWeights,
    transformer_block,
)
from neutron_mojo.nn.rope import RoPETable
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


fn test_linear_identity() raises:
    """Test linear projection with identity matrix."""
    var x = Tensor[DType.float32](Shape(3))
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 3.0)

    # Identity matrix [3, 3]
    var w = Tensor[DType.float32](Shape(3, 3))
    w.set(0, 1.0)  # w[0,0] (flat index for 2D)
    # For 2D tensor, need to use proper indexing
    # Shape(3,3) → strides [3,1]
    # w[0,0]=idx0, w[0,1]=idx1, w[0,2]=idx2
    # w[1,0]=idx3, w[1,1]=idx4, w[1,2]=idx5
    # w[2,0]=idx6, w[2,1]=idx7, w[2,2]=idx8

    # Zero everything first
    for i in range(9):
        w.set(i, 0.0)
    # Set diagonal
    w.set(0, 1.0)  # w[0,0]
    w.set(4, 1.0)  # w[1,1]
    w.set(8, 1.0)  # w[2,2]

    var y = linear(x, w)
    assert_near(y.get(0), 1.0, 1e-5, "identity x[0]")
    assert_near(y.get(1), 2.0, 1e-5, "identity x[1]")
    assert_near(y.get(2), 3.0, 1e-5, "identity x[2]")

    print("  linear_identity: PASS")


fn test_linear_projection() raises:
    """Test linear projection with known matrix."""
    var x = Tensor[DType.float32](Shape(2))
    x.set(0, 1.0)
    x.set(1, 2.0)

    # W = [[1, 2], [3, 4], [5, 6]] — projects from dim 2 to dim 3
    var w = Tensor[DType.float32](Shape(3, 2))
    w.set(0, 1.0)  # w[0,0]
    w.set(1, 2.0)  # w[0,1]
    w.set(2, 3.0)  # w[1,0]
    w.set(3, 4.0)  # w[1,1]
    w.set(4, 5.0)  # w[2,0]
    w.set(5, 6.0)  # w[2,1]

    var y = linear(x, w)
    # y[0] = 1*1 + 2*2 = 5
    # y[1] = 3*1 + 4*2 = 11
    # y[2] = 5*1 + 6*2 = 17
    assert_true(y.numel() == 3, "output dim")
    assert_near(y.get(0), 5.0, 1e-5, "proj y[0]")
    assert_near(y.get(1), 11.0, 1e-5, "proj y[1]")
    assert_near(y.get(2), 17.0, 1e-5, "proj y[2]")

    print("  linear_projection: PASS")


fn test_transformer_weights_creation() raises:
    """Test TransformerWeights initialization."""
    var w = TransformerWeights(
        hidden_dim=8,
        num_q_heads=4,
        num_kv_heads=2,
        head_dim=2,
        ffn_dim=16,
    )

    # Q projection: [4*2, 8] = [8, 8]
    assert_true(w.wq.numel() == 64, "wq size")
    # K projection: [2*2, 8] = [4, 8]
    assert_true(w.wk.numel() == 32, "wk size")
    # V projection: [2*2, 8] = [4, 8]
    assert_true(w.wv.numel() == 32, "wv size")
    # Output: [8, 8]
    assert_true(w.wo.numel() == 64, "wo size")

    # FFN
    assert_true(w.w_gate.numel() == 128, "gate size 16*8")
    assert_true(w.w_up.numel() == 128, "up size 16*8")
    assert_true(w.w_down.numel() == 128, "down size 8*16")

    # Norms should be initialized to 1.0
    assert_near(w.attn_norm.get(0), 1.0, 1e-5, "attn norm init")
    assert_near(w.ffn_norm.get(0), 1.0, 1e-5, "ffn norm init")

    print("  transformer_weights_creation: PASS")


fn test_transformer_block_smoke() raises:
    """Smoke test: transformer block runs without error."""
    # Tiny model: hidden=4, 2 Q heads, 1 KV head, head_dim=2, ffn=8
    var hidden_dim = 4
    var num_q_heads = 2
    var num_kv_heads = 1
    var head_dim = 2
    var ffn_dim = 8

    var weights = TransformerWeights(
        hidden_dim, num_q_heads, num_kv_heads, head_dim, ffn_dim
    )

    # Set identity-like weights for Q/K/V/O (just pass through)
    # wq: [4, 4], wk: [2, 4], wv: [2, 4], wo: [4, 4]
    # Set wq to identity
    for i in range(4):
        weights.wq.set(i * 4 + i, 1.0)
    # Set wo to identity
    for i in range(4):
        weights.wo.set(i * 4 + i, 1.0)
    # Set small wk/wv so they produce something
    for i in range(2):
        weights.wk.set(i * 4 + i, 0.1)
        weights.wv.set(i * 4 + i, 0.1)

    var cache = KVCache(max_seq_len=16, num_kv_heads=num_kv_heads, head_dim=head_dim)
    var rope = RoPETable(head_dim=head_dim, max_seq_len=16)

    var x = Tensor[DType.float32](Shape(hidden_dim))
    x.set(0, 1.0)
    x.set(1, 0.5)
    x.set(2, -0.5)
    x.set(3, -1.0)

    var out = transformer_block(
        x, weights, cache, rope, pos=0,
        num_q_heads=num_q_heads,
        num_kv_heads=num_kv_heads,
        head_dim=head_dim,
    )

    assert_true(out.numel() == hidden_dim, "output dim matches")
    assert_true(cache.length == 1, "cache has 1 entry")

    # Output should be different from input (transformations applied)
    var different = False
    for i in range(hidden_dim):
        if abs(out.get(i) - x.get(i)) > 1e-6:
            different = True
            break
    assert_true(different, "output differs from input")

    print("  transformer_block_smoke: PASS")


fn test_transformer_block_residual() raises:
    """Test that residual connections are working.

    With zero weights (except norms), output should equal input
    since the residual adds zeros.
    """
    var hidden_dim = 4
    var num_q_heads = 2
    var num_kv_heads = 1
    var head_dim = 2
    var ffn_dim = 4

    # All weights default to zero except norms (which are 1.0)
    var weights = TransformerWeights(
        hidden_dim, num_q_heads, num_kv_heads, head_dim, ffn_dim
    )

    var cache = KVCache(max_seq_len=8, num_kv_heads=num_kv_heads, head_dim=head_dim)
    var rope = RoPETable(head_dim=head_dim, max_seq_len=8)

    var x = Tensor[DType.float32](Shape(hidden_dim))
    x.set(0, 2.0)
    x.set(1, 3.0)
    x.set(2, 4.0)
    x.set(3, 5.0)

    var out = transformer_block(
        x, weights, cache, rope, pos=0,
        num_q_heads=num_q_heads,
        num_kv_heads=num_kv_heads,
        head_dim=head_dim,
    )

    # With all projection weights zero, attention output is zero,
    # FFN output is zero, so output = input + 0 + 0 = input
    for i in range(hidden_dim):
        assert_near(out.get(i), x.get(i), 1e-3, "residual preserves input")

    print("  transformer_block_residual: PASS")


fn test_transformer_block_sequential() raises:
    """Test running multiple tokens through a transformer block."""
    var hidden_dim = 4
    var num_q_heads = 2
    var num_kv_heads = 1
    var head_dim = 2
    var ffn_dim = 4

    var weights = TransformerWeights(
        hidden_dim, num_q_heads, num_kv_heads, head_dim, ffn_dim
    )
    # Set small weights so attention does something
    for i in range(2):
        weights.wk.set(i * 4 + i, 0.1)
        weights.wv.set(i * 4 + i, 0.1)
    for i in range(4):
        weights.wq.set(i * 4 + i, 0.1)
        weights.wo.set(i * 4 + i, 0.1)

    var cache = KVCache(max_seq_len=8, num_kv_heads=num_kv_heads, head_dim=head_dim)
    var rope = RoPETable(head_dim=head_dim, max_seq_len=8)

    # Token 0
    var x0 = Tensor[DType.float32](Shape(hidden_dim))
    x0.set(0, 1.0)
    x0.set(1, 1.0)
    x0.set(2, 1.0)
    x0.set(3, 1.0)
    var out0 = transformer_block(
        x0, weights, cache, rope, pos=0,
        num_q_heads=num_q_heads,
        num_kv_heads=num_kv_heads,
        head_dim=head_dim,
    )
    assert_true(cache.length == 1, "cache length after token 0")

    # Token 1
    var x1 = Tensor[DType.float32](Shape(hidden_dim))
    x1.set(0, 2.0)
    x1.set(1, 2.0)
    x1.set(2, 2.0)
    x1.set(3, 2.0)
    var out1 = transformer_block(
        x1, weights, cache, rope, pos=1,
        num_q_heads=num_q_heads,
        num_kv_heads=num_kv_heads,
        head_dim=head_dim,
    )
    assert_true(cache.length == 2, "cache length after token 1")

    # Outputs should exist and differ
    assert_true(out0.numel() == hidden_dim, "out0 shape")
    assert_true(out1.numel() == hidden_dim, "out1 shape")

    print("  transformer_block_sequential: PASS")


fn test_linear_zero_weight() raises:
    """Test linear with zero weight matrix."""
    var x = Tensor[DType.float32](Shape(3))
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 3.0)

    var w = Tensor[DType.float32](Shape(2, 3))
    # All zeros by default
    var y = linear(x, w)

    assert_true(y.numel() == 2, "output dim")
    assert_near(y.get(0), 0.0, 1e-5, "zero weight output")
    assert_near(y.get(1), 0.0, 1e-5, "zero weight output")

    print("  linear_zero_weight: PASS")


fn main() raises:
    print("test_transformer:")

    test_linear_identity()
    test_linear_projection()
    test_transformer_weights_creation()
    test_transformer_block_smoke()
    test_transformer_block_residual()
    test_transformer_block_sequential()
    test_linear_zero_weight()

    print("ALL PASSED")
