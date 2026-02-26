# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Parallel Prefill Tests
# ===----------------------------------------------------------------------=== #

"""Tests for parallel prefill: batch RoPE, batched causal attention, and
the optimized forward_layer_prefill that eliminates per-token cache copies."""

from math import abs, sqrt
from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head, apply_rope_batch
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.attention import (
    gqa_attention_direct,
    gqa_attention_prefill,
    softmax_inplace,
)
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.nn.causal_lm import embed_token


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn _build_model() -> Model:
    """Build a tiny model with non-trivial weights."""
    var p = tiny_test_params()
    var model = Model(p)
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)
    for i in range(p.num_layers * model.layer_size):
        model.layer_weights.set(i, Float32(i % 13) * 0.02 - 0.12)
    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)
    return model^


# ===----------------------------------------------------------------------=== #
# Batch RoPE Tests
# ===----------------------------------------------------------------------=== #

fn test_batch_rope_matches_sequential() raises:
    """Verify batch RoPE produces identical results to per-token RoPE."""
    var head_dim = 8
    var num_q_heads = 4
    var num_kv_heads = 2
    var num_tokens = 5
    var q_dim = num_q_heads * head_dim
    var kv_dim = num_kv_heads * head_dim

    var rope = RoPETable(head_dim=head_dim, max_seq_len=32)

    # Create batched tensors with known values
    var q_batch = Tensor[DType.float32](Shape(num_tokens * q_dim))
    var k_batch = Tensor[DType.float32](Shape(num_tokens * kv_dim))
    for i in range(num_tokens * q_dim):
        q_batch.set(i, Float32(i) * 0.1)
    for i in range(num_tokens * kv_dim):
        k_batch.set(i, Float32(i) * 0.05)

    # Copy for sequential comparison
    var q_seq = Tensor[DType.float32](Shape(num_tokens * q_dim))
    var k_seq = Tensor[DType.float32](Shape(num_tokens * kv_dim))
    for i in range(num_tokens * q_dim):
        q_seq.set(i, q_batch.get(i))
    for i in range(num_tokens * kv_dim):
        k_seq.set(i, k_batch.get(i))

    # Apply batch RoPE
    apply_rope_batch(q_batch, k_batch, rope, 0, num_tokens, num_q_heads, num_kv_heads, head_dim)

    # Apply sequential RoPE (mimicking old per-token loop)
    for t in range(num_tokens):
        for h in range(num_q_heads):
            var q_head = Tensor[DType.float32](Shape(head_dim))
            var base = t * q_dim + h * head_dim
            for d in range(head_dim):
                q_head.set(d, q_seq.get(base + d))
            apply_rope_single_head(q_head, rope, t)
            for d in range(head_dim):
                q_seq.set(base + d, q_head.get(d))

        for h in range(num_kv_heads):
            var k_head = Tensor[DType.float32](Shape(head_dim))
            var base = t * kv_dim + h * head_dim
            for d in range(head_dim):
                k_head.set(d, k_seq.get(base + d))
            apply_rope_single_head(k_head, rope, t)
            for d in range(head_dim):
                k_seq.set(base + d, k_head.get(d))

    # Compare
    for i in range(num_tokens * q_dim):
        assert_near(q_batch.get(i), q_seq.get(i), 1e-5, "q_batch[" + String(i) + "]")
    for i in range(num_tokens * kv_dim):
        assert_near(k_batch.get(i), k_seq.get(i), 1e-5, "k_batch[" + String(i) + "]")

    print("  batch_rope_matches_sequential: PASS")


fn test_batch_rope_with_offset() raises:
    """Test batch RoPE with non-zero start_pos."""
    var head_dim = 4
    var num_q_heads = 2
    var num_kv_heads = 1
    var num_tokens = 3
    var start_pos = 5
    var q_dim = num_q_heads * head_dim
    var kv_dim = num_kv_heads * head_dim

    var rope = RoPETable(head_dim=head_dim, max_seq_len=32)

    var q_batch = Tensor[DType.float32](Shape(num_tokens * q_dim))
    var k_batch = Tensor[DType.float32](Shape(num_tokens * kv_dim))
    for i in range(num_tokens * q_dim):
        q_batch.set(i, Float32(i + 1) * 0.2)
    for i in range(num_tokens * kv_dim):
        k_batch.set(i, Float32(i + 1) * 0.3)

    # Copy for sequential
    var q_seq = Tensor[DType.float32](Shape(num_tokens * q_dim))
    var k_seq = Tensor[DType.float32](Shape(num_tokens * kv_dim))
    for i in range(num_tokens * q_dim):
        q_seq.set(i, q_batch.get(i))
    for i in range(num_tokens * kv_dim):
        k_seq.set(i, k_batch.get(i))

    # Batch
    apply_rope_batch(q_batch, k_batch, rope, start_pos, num_tokens, num_q_heads, num_kv_heads, head_dim)

    # Sequential with offset
    for t in range(num_tokens):
        for h in range(num_q_heads):
            var q_head = Tensor[DType.float32](Shape(head_dim))
            var base = t * q_dim + h * head_dim
            for d in range(head_dim):
                q_head.set(d, q_seq.get(base + d))
            apply_rope_single_head(q_head, rope, start_pos + t)
            for d in range(head_dim):
                q_seq.set(base + d, q_head.get(d))

        for h in range(num_kv_heads):
            var k_head = Tensor[DType.float32](Shape(head_dim))
            var base = t * kv_dim + h * head_dim
            for d in range(head_dim):
                k_head.set(d, k_seq.get(base + d))
            apply_rope_single_head(k_head, rope, start_pos + t)
            for d in range(head_dim):
                k_seq.set(base + d, k_head.get(d))

    for i in range(num_tokens * q_dim):
        assert_near(q_batch.get(i), q_seq.get(i), 1e-5, "q_offset[" + String(i) + "]")

    print("  batch_rope_with_offset: PASS")


# ===----------------------------------------------------------------------=== #
# Batched Prefill Attention Tests
# ===----------------------------------------------------------------------=== #

fn test_prefill_attention_single_token() raises:
    """Prefill attention with 1 token matches direct attention."""
    var num_q_heads = 2
    var num_kv_heads = 1
    var head_dim = 4
    var q_dim = num_q_heads * head_dim
    var kv_dim = num_kv_heads * head_dim

    var cache = MultiLayerKVCache(
        num_layers=1, max_seq_len=16,
        num_kv_heads=num_kv_heads, head_dim=head_dim,
    )

    # Insert one K/V into cache
    var k = Tensor[DType.float32](Shape(kv_dim))
    var v = Tensor[DType.float32](Shape(kv_dim))
    for d in range(kv_dim):
        k.set(d, Float32(d) * 0.1 + 0.5)
        v.set(d, Float32(d) * 0.2 + 0.3)
    cache.append_kv(0, k, v, num_new_tokens=1)

    # Query for single token
    var q = Tensor[DType.float32](Shape(q_dim))
    for d in range(q_dim):
        q.set(d, Float32(d) * 0.15)

    # Prefill attention
    var prefill_out = gqa_attention_prefill(
        q, cache, 0, 1, 0, num_q_heads, num_kv_heads, head_dim,
    )

    # Direct attention
    var direct_out = gqa_attention_direct(
        q, cache, 0, num_q_heads, num_kv_heads, head_dim,
    )

    for d in range(q_dim):
        assert_near(prefill_out.get(d), direct_out.get(d), 1e-5,
            "prefill vs direct[" + String(d) + "]")

    print("  prefill_attention_single_token: PASS")


fn test_prefill_attention_causal_mask() raises:
    """Verify causal masking: token t can only see positions 0..t."""
    var num_q_heads = 1
    var num_kv_heads = 1
    var head_dim = 4
    var kv_dim = num_kv_heads * head_dim
    var num_tokens = 3

    var cache = MultiLayerKVCache(
        num_layers=1, max_seq_len=16,
        num_kv_heads=num_kv_heads, head_dim=head_dim,
    )

    # Insert 3 K/V with distinct values
    var k_batch = Tensor[DType.float32](Shape(num_tokens * kv_dim))
    var v_batch = Tensor[DType.float32](Shape(num_tokens * kv_dim))
    for t in range(num_tokens):
        for d in range(kv_dim):
            k_batch.set(t * kv_dim + d, Float32(t + 1) * Float32(d + 1) * 0.1)
            v_batch.set(t * kv_dim + d, Float32(t + 1) * 0.5)
    cache.append_kv(0, k_batch, v_batch, num_new_tokens=num_tokens)

    # Query batch
    var q_batch = Tensor[DType.float32](Shape(num_tokens * head_dim))
    for i in range(num_tokens * head_dim):
        q_batch.set(i, 1.0)

    var out = gqa_attention_prefill(
        q_batch, cache, 0, num_tokens, 0, num_q_heads, num_kv_heads, head_dim,
    )

    # Token 0 should only attend to position 0 (softmax of single element = 1.0)
    # So output = v[0] = [0.5, 0.5, 0.5, 0.5]
    for d in range(head_dim):
        assert_near(out.get(d), 0.5, 0.01, "token0 attends only to pos0")

    # Token 2 should attend to positions 0, 1, 2 (weighted average)
    # Verify it's not equal to token 0's output (it sees more context)
    var t0_sum: Float32 = 0.0
    var t2_sum: Float32 = 0.0
    for d in range(head_dim):
        t0_sum += out.get(d)
        t2_sum += out.get(2 * head_dim + d)
    # t2 attends to v[0]=0.5, v[1]=1.0, v[2]=1.5, so output > t0's 0.5
    assert_true(t2_sum > t0_sum, "token2 sees more context than token0")

    print("  prefill_attention_causal_mask: PASS")


fn test_prefill_attention_matches_sequential() raises:
    """Verify prefill attention matches token-by-token attention."""
    var num_q_heads = 2
    var num_kv_heads = 1
    var head_dim = 4
    var q_dim = num_q_heads * head_dim
    var kv_dim = num_kv_heads * head_dim
    var num_tokens = 4

    # --- Batched path ---
    var cache_batch = MultiLayerKVCache(
        num_layers=1, max_seq_len=16,
        num_kv_heads=num_kv_heads, head_dim=head_dim,
    )
    var k_batch = Tensor[DType.float32](Shape(num_tokens * kv_dim))
    var v_batch = Tensor[DType.float32](Shape(num_tokens * kv_dim))
    var q_batch = Tensor[DType.float32](Shape(num_tokens * q_dim))
    for i in range(num_tokens * kv_dim):
        k_batch.set(i, Float32(i % 7) * 0.1 + 0.1)
        v_batch.set(i, Float32(i % 5) * 0.2 + 0.2)
    for i in range(num_tokens * q_dim):
        q_batch.set(i, Float32(i % 9) * 0.15 + 0.05)

    cache_batch.append_kv(0, k_batch, v_batch, num_new_tokens=num_tokens)
    var batch_out = gqa_attention_prefill(
        q_batch, cache_batch, 0, num_tokens, 0, num_q_heads, num_kv_heads, head_dim,
    )

    # --- Sequential path ---
    var cache_seq = MultiLayerKVCache(
        num_layers=1, max_seq_len=16,
        num_kv_heads=num_kv_heads, head_dim=head_dim,
    )
    var seq_out = Tensor[DType.float32](Shape(num_tokens * q_dim))

    for t in range(num_tokens):
        # Append K/V one at a time
        var k_tok = Tensor[DType.float32](Shape(kv_dim))
        var v_tok = Tensor[DType.float32](Shape(kv_dim))
        for d in range(kv_dim):
            k_tok.set(d, k_batch.get(t * kv_dim + d))
            v_tok.set(d, v_batch.get(t * kv_dim + d))
        cache_seq.append_kv(0, k_tok, v_tok, num_new_tokens=1)

        # Compute attention for this token
        var q_tok = Tensor[DType.float32](Shape(q_dim))
        for d in range(q_dim):
            q_tok.set(d, q_batch.get(t * q_dim + d))

        var direct_out = gqa_attention_direct(
            q_tok, cache_seq, 0, num_q_heads, num_kv_heads, head_dim,
        )

        for d in range(q_dim):
            seq_out.set(t * q_dim + d, direct_out.get(d))

    # Compare
    for i in range(num_tokens * q_dim):
        assert_near(batch_out.get(i), seq_out.get(i), 1e-4,
            "batch vs seq[" + String(i) + "]")

    print("  prefill_attention_matches_sequential: PASS")


# ===----------------------------------------------------------------------=== #
# Full Prefill Integration Tests
# ===----------------------------------------------------------------------=== #

fn test_forward_prefill_produces_valid_logits() raises:
    """Test that forward_prefill produces valid logits."""
    var model = _build_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(3)
    prompt.append(5)

    var logits = model.forward_prefill(prompt, cache, rope)

    # Verify output size
    assert_true(logits.numel() == p.vocab_size, "logits size = vocab_size")

    # Cache should have 3 entries per layer
    for layer in range(p.num_layers):
        assert_true(cache.lengths[layer] == 3, "cache has 3 entries at layer " + String(layer))

    print("  forward_prefill_produces_valid_logits: PASS")


fn test_prefill_matches_sequential_forward() raises:
    """Verify forward_prefill + decode matches sequential forward calls."""
    var model = _build_model()
    var p = model.params.copy()
    var prompt = List[Int]()
    prompt.append(2)
    prompt.append(4)
    prompt.append(6)

    # --- Sequential path ---
    var cache_seq = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope_seq = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var logits_seq = Tensor[DType.float32](Shape(p.vocab_size))
    for i in range(len(prompt)):
        logits_seq = model.forward(prompt[i], cache_seq, rope_seq, pos=i)

    # --- Prefill path ---
    var cache_pre = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope_pre = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var logits_pre = model.forward_prefill(prompt, cache_pre, rope_pre)

    # Both should produce logits of the same shape
    assert_true(logits_seq.numel() == p.vocab_size, "seq logits size")
    assert_true(logits_pre.numel() == p.vocab_size, "pre logits size")

    # Both caches should have same length
    for layer in range(p.num_layers):
        assert_true(cache_seq.lengths[layer] == cache_pre.lengths[layer],
            "cache lengths match at layer " + String(layer))

    # Argmax should match (same model, same input)
    var max_seq = 0
    var max_pre = 0
    var max_val_seq: Float32 = -1e30
    var max_val_pre: Float32 = -1e30
    for i in range(p.vocab_size):
        if logits_seq.get(i) > max_val_seq:
            max_val_seq = logits_seq.get(i)
            max_seq = i
        if logits_pre.get(i) > max_val_pre:
            max_val_pre = logits_pre.get(i)
            max_pre = i

    assert_true(max_seq == max_pre,
        "argmax matches: seq=" + String(max_seq) + " pre=" + String(max_pre))

    print("  prefill_matches_sequential_forward: PASS")


fn test_prefill_then_decode() raises:
    """Test that prefill followed by decode produces valid tokens."""
    var model = _build_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=32,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=32)

    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)

    # Prefill
    var logits = model.forward_prefill(prompt, cache, rope)

    # Decode 3 more tokens
    var generated = List[Int]()
    for step in range(3):
        # Simple argmax
        var best = 0
        var best_val: Float32 = -1e30
        for i in range(p.vocab_size):
            if logits.get(i) > best_val:
                best_val = logits.get(i)
                best = i
        generated.append(best)
        assert_true(best >= 0 and best < p.vocab_size, "valid token")
        var pos = len(prompt) + step
        logits = model.forward(best, cache, rope, pos=pos)

    assert_true(len(generated) == 3, "generated 3 tokens")

    print("  prefill_then_decode: PASS")


fn test_prefill_single_token() raises:
    """Edge case: prefill with just 1 token."""
    var model = _build_model()
    var p = model.params.copy()

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=16,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(head_dim=p.head_dim, max_seq_len=16)

    var prompt = List[Int]()
    prompt.append(3)

    var logits = model.forward_prefill(prompt, cache, rope)
    assert_true(logits.numel() == p.vocab_size, "logits size")
    for layer in range(p.num_layers):
        assert_true(cache.lengths[layer] == 1, "1 entry cached")

    print("  prefill_single_token: PASS")


# ===----------------------------------------------------------------------=== #
# Benchmark
# ===----------------------------------------------------------------------=== #

fn test_prefill_benchmark() raises:
    """Benchmark parallel prefill vs sequential forward for prompt processing."""
    var model = _build_model()
    var p = model.params.copy()
    var num_tokens = 16

    var prompt = List[Int]()
    for i in range(num_tokens):
        prompt.append(i % p.vocab_size)

    # Sequential
    var cache_seq = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=64,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope_seq = RoPETable(head_dim=p.head_dim, max_seq_len=64)

    var t0 = perf_counter_ns()
    var _logits_seq = Tensor[DType.float32](Shape(p.vocab_size))
    for i in range(num_tokens):
        _logits_seq = model.forward(prompt[i], cache_seq, rope_seq, pos=i)
    var t1 = perf_counter_ns()
    var seq_us = Float64(t1 - t0) / 1000.0

    # Prefill
    var cache_pre = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=64,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope_pre = RoPETable(head_dim=p.head_dim, max_seq_len=64)

    var t2 = perf_counter_ns()
    var logits_pre = model.forward_prefill(prompt, cache_pre, rope_pre)
    var t3 = perf_counter_ns()
    var pre_us = Float64(t3 - t2) / 1000.0

    print("  Benchmark (" + String(num_tokens) + " tokens):")
    print("    Sequential: " + String(Int(seq_us)) + " us")
    print("    Prefill:    " + String(Int(pre_us)) + " us")
    if pre_us > 0:
        var speedup = seq_us / pre_us
        print("    Speedup:    " + String(speedup) + "x")

    print("  prefill_benchmark: PASS")


fn main() raises:
    print("test_parallel_prefill:")

    # Batch RoPE
    test_batch_rope_matches_sequential()
    test_batch_rope_with_offset()

    # Batched prefill attention
    test_prefill_attention_single_token()
    test_prefill_attention_causal_mask()
    test_prefill_attention_matches_sequential()

    # Full prefill integration
    test_forward_prefill_produces_valid_logits()
    test_prefill_matches_sequential_forward()
    test_prefill_then_decode()
    test_prefill_single_token()

    # Benchmark
    test_prefill_benchmark()

    print("ALL PASSED")
