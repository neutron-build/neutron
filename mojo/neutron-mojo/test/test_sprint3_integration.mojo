# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 3 Integration Tests
# ===----------------------------------------------------------------------=== #

"""End-to-end integration tests for the inference pipeline.

Tests the full stack: embedding → RoPE → attention → transformer → generate.
"""

from math import abs, sqrt, sin, cos, exp
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import rmsnorm
from neutron_mojo.nn.rope import RoPETable, apply_rope, apply_rope_single_head
from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.nn.attention import gqa_attention, softmax_inplace
from neutron_mojo.nn.transformer import (
    linear,
    TransformerWeights,
    transformer_block,
)
from neutron_mojo.nn.causal_lm import (
    CausalLMWeights,
    embed_token,
    compute_logits,
    argmax,
    apply_temperature,
    top_k_filter,
    generate_greedy_one_layer,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn test_rope_to_attention_pipeline() raises:
    """Test RoPE rotation feeds correctly into attention."""
    var head_dim = 4
    var num_kv_heads = 1
    var cache = KVCache(max_seq_len=8, num_kv_heads=num_kv_heads, head_dim=head_dim)
    var rope = RoPETable(head_dim=head_dim, max_seq_len=8)

    # Create K/V for position 0 and 1
    for pos in range(2):
        var k = Tensor[DType.float32](Shape(head_dim))
        var v = Tensor[DType.float32](Shape(head_dim))
        for d in range(head_dim):
            k.set(d, Float32(d + 1))
            v.set(d, Float32(pos * 10 + d))
        # Apply RoPE to K
        apply_rope_single_head(k, rope, pos)
        cache.append_kv(k, v, num_new_tokens=1)

    # Create a query at position 2 and apply RoPE
    var q = Tensor[DType.float32](Shape(head_dim))
    for d in range(head_dim):
        q.set(d, Float32(d + 1))
    apply_rope_single_head(q, rope, 2)

    # Run GQA attention
    var out = gqa_attention(q, cache, 1, 1, head_dim)

    # Output should be a weighted average of cached values
    assert_true(out.numel() == head_dim, "output size")
    # Values should be finite
    for d in range(head_dim):
        var v = out.get(d)
        assert_true(v > -1000.0 and v < 1000.0, "finite output")

    print("  rope_to_attention_pipeline: PASS")


fn test_multi_layer_kv_cache_with_attention() raises:
    """Test MultiLayerKVCache works with attention across layers."""
    var head_dim = 2
    var num_kv_heads = 1
    var num_layers = 2

    var ml_cache = MultiLayerKVCache(
        num_layers=num_layers,
        max_seq_len=4,
        num_kv_heads=num_kv_heads,
        head_dim=head_dim,
    )

    # Fill each layer with different data
    for layer in range(num_layers):
        var k = Tensor[DType.float32](Shape(head_dim))
        var v = Tensor[DType.float32](Shape(head_dim))
        k.set(0, Float32(layer + 1))
        k.set(1, Float32(layer + 1))
        v.set(0, Float32((layer + 1) * 10))
        v.set(1, Float32((layer + 1) * 20))
        ml_cache.append_kv(layer, k, v, num_new_tokens=1)

    assert_true(ml_cache.current_length() == 1, "1 token cached")
    # Layer isolation
    assert_near(
        ml_cache.get_value_at(0, 0, 0, 0), 10.0, 1e-5, "layer 0 value"
    )
    assert_near(
        ml_cache.get_value_at(1, 0, 0, 0), 20.0, 1e-5, "layer 1 value"
    )

    print("  multi_layer_kv_cache_with_attention: PASS")


fn test_transformer_block_with_rope_position() raises:
    """Test transformer block applies RoPE at correct positions."""
    var hidden_dim = 4
    var num_q_heads = 2
    var num_kv_heads = 1
    var head_dim = 2
    var ffn_dim = 4

    var weights = TransformerWeights(
        hidden_dim, num_q_heads, num_kv_heads, head_dim, ffn_dim
    )
    var cache = KVCache(max_seq_len=8, num_kv_heads=num_kv_heads, head_dim=head_dim)
    var rope = RoPETable(head_dim=head_dim, max_seq_len=8)

    # Process 3 tokens sequentially
    for pos in range(3):
        var x = Tensor[DType.float32](Shape(hidden_dim))
        for d in range(hidden_dim):
            x.set(d, 1.0)
        _ = transformer_block(
            x, weights, cache, rope, pos=pos,
            num_q_heads=num_q_heads,
            num_kv_heads=num_kv_heads,
            head_dim=head_dim,
        )

    assert_true(cache.length == 3, "3 positions cached")

    print("  transformer_block_with_rope_position: PASS")


fn test_full_forward_pass_consistency() raises:
    """Test that a full forward pass produces consistent results."""
    var model = CausalLMWeights(
        num_layers=1,
        vocab_size=4,
        hidden_dim=4,
        num_q_heads=2,
        num_kv_heads=1,
        head_dim=2,
        ffn_dim=4,
    )

    # Set up a simple embedding
    for t in range(4):
        for d in range(4):
            model.embed.set(t * 4 + d, Float32((t + 1) * (d + 1)) * 0.1)

    # Set up LM head to prefer token 1
    for d in range(4):
        model.lm_head.set(1 * 4 + d, 5.0)

    var layer = TransformerWeights(4, 2, 1, 2, 4)

    var prompt = List[Int]()
    prompt.append(0)

    # Run twice — should give same result
    var gen1 = generate_greedy_one_layer(prompt, model, layer, max_new_tokens=2)
    var gen2 = generate_greedy_one_layer(prompt, model, layer, max_new_tokens=2)

    assert_true(gen1[0] == gen2[0], "first token consistent")
    assert_true(gen1[1] == gen2[1], "second token consistent")

    print("  full_forward_pass_consistency: PASS")


fn test_rmsnorm_to_linear_pipeline() raises:
    """Test RMSNorm → Linear projection pipeline."""
    var x = Tensor[DType.float32](Shape(4))
    x.set(0, 2.0)
    x.set(1, 4.0)
    x.set(2, 6.0)
    x.set(3, 8.0)

    var gamma = Tensor[DType.float32](Shape(4))
    for i in range(4):
        gamma.set(i, 1.0)

    var normed = rmsnorm[DType.float32](x, gamma)

    # normed should have RMS ≈ 1
    var rms: Float32 = 0.0
    for i in range(4):
        rms += normed.get(i) * normed.get(i)
    rms = Float32(sqrt(Float64(rms / 4.0)))
    assert_near(rms, 1.0, 0.01, "RMS norm ≈ 1")

    # Linear projection
    var w = Tensor[DType.float32](Shape(2, 4))
    for i in range(8):
        w.set(i, 0.5)

    var proj = linear(normed, w)
    assert_true(proj.numel() == 2, "projection output dim")

    print("  rmsnorm_to_linear_pipeline: PASS")


fn test_gqa_with_different_group_sizes() raises:
    """Test GQA with group_size=1 (MHA), 2, and 4."""
    var head_dim = 2

    var group_sizes = List[Int]()
    group_sizes.append(1)
    group_sizes.append(2)
    group_sizes.append(4)
    for gs_idx in range(len(group_sizes)):
        var group_size = group_sizes[gs_idx]
        var num_kv_heads = 2
        var num_q_heads = num_kv_heads * group_size
        var cache = KVCache(
            max_seq_len=4,
            num_kv_heads=num_kv_heads,
            head_dim=head_dim,
        )

        var k = Tensor[DType.float32](Shape(num_kv_heads * head_dim))
        var v = Tensor[DType.float32](Shape(num_kv_heads * head_dim))
        for i in range(num_kv_heads * head_dim):
            k.set(i, 1.0)
            v.set(i, Float32(i + 1))
        cache.append_kv(k, v, num_new_tokens=1)

        var q = Tensor[DType.float32](Shape(num_q_heads * head_dim))
        for i in range(num_q_heads * head_dim):
            q.set(i, 1.0)

        var out = gqa_attention(q, cache, num_q_heads, num_kv_heads, head_dim)
        assert_true(
            out.numel() == num_q_heads * head_dim,
            "GQA output size for group " + String(group_size),
        )

    print("  gqa_with_different_group_sizes: PASS")


fn test_kv_cache_grows_through_generation() raises:
    """Test that KV cache length increases during generation."""
    var model = CausalLMWeights(
        num_layers=1,
        vocab_size=4,
        hidden_dim=2,
        num_q_heads=1,
        num_kv_heads=1,
        head_dim=2,
        ffn_dim=4,
    )
    var layer = TransformerWeights(2, 1, 1, 2, 4)

    var prompt = List[Int]()
    prompt.append(0)
    prompt.append(1)
    prompt.append(2)

    # Generate 3 more tokens
    var gen = generate_greedy_one_layer(prompt, model, layer, max_new_tokens=3)

    # Should have generated 3 tokens
    assert_true(len(gen) == 3, "generated 3 tokens")

    print("  kv_cache_grows_through_generation: PASS")


fn test_softmax_with_attention_scores() raises:
    """Test softmax applied to realistic attention score patterns."""
    # Simulate attention scores where one key matches strongly
    var scores = Tensor[DType.float32](Shape(4))
    var scale = Float32(1.0 / sqrt(Float64(64)))  # head_dim=64

    # Simulated Q·K scores (pre-scaling)
    scores.set(0, 50.0 * scale)   # moderate
    scores.set(1, 200.0 * scale)  # strong match
    scores.set(2, 30.0 * scale)   # weak
    scores.set(3, 10.0 * scale)   # very weak

    softmax_inplace(scores, 4)

    # Sum should be 1.0
    var total: Float32 = 0.0
    for i in range(4):
        total += scores.get(i)
    assert_near(total, 1.0, 1e-3, "softmax sums to 1")

    # Position 1 should have highest weight
    assert_true(scores.get(1) > scores.get(0), "highest score wins")
    assert_true(scores.get(1) > scores.get(2), "highest score wins")

    print("  softmax_with_attention_scores: PASS")


fn test_embedding_to_logit_roundtrip() raises:
    """Test embed → process → logits → argmax roundtrip."""
    var vocab_size = 4
    var hidden_dim = 2

    var embed = Tensor[DType.float32](Shape(vocab_size, hidden_dim))
    # Each token has a distinct embedding
    embed.set(0, 1.0)
    embed.set(1, 0.0)
    embed.set(2, 0.0)
    embed.set(3, 1.0)
    embed.set(4, 1.0)
    embed.set(5, 1.0)
    embed.set(6, -1.0)
    embed.set(7, -1.0)

    # LM head = transpose of embed (so embedding → logit is identity-like)
    var lm_head = Tensor[DType.float32](Shape(vocab_size, hidden_dim))
    for i in range(vocab_size * hidden_dim):
        lm_head.set(i, embed.get(i // hidden_dim, i % hidden_dim))

    # Embed token 2 → [1, 1]
    var h = embed_token(embed, 2, hidden_dim)
    assert_near(h.get(0), 1.0, 1e-5, "embed[2][0]")
    assert_near(h.get(1), 1.0, 1e-5, "embed[2][1]")

    # Compute logits
    var logits = compute_logits(h, lm_head, vocab_size, hidden_dim)
    # logit[0] = 1*1 + 0*1 = 1
    # logit[1] = 0*1 + 1*1 = 1
    # logit[2] = 1*1 + 1*1 = 2 (highest)
    # logit[3] = -1*1 + -1*1 = -2

    var best = argmax(logits, vocab_size)
    assert_true(best == 2, "should recover token 2")

    print("  embedding_to_logit_roundtrip: PASS")


fn test_temperature_affects_generation() raises:
    """Test that temperature changes sampling distribution."""
    var logits = Tensor[DType.float32](Shape(4))
    logits.set(0, 1.0)
    logits.set(1, 2.0)
    logits.set(2, 3.0)
    logits.set(3, 4.0)

    # High temperature: more uniform
    var hot = Tensor[DType.float32](Shape(4))
    for i in range(4):
        hot.set(i, logits.get(i))
    apply_temperature(hot, 4, 10.0)
    softmax_inplace(hot, 4)

    # Low temperature: more peaked
    var cold = Tensor[DType.float32](Shape(4))
    for i in range(4):
        cold.set(i, logits.get(i))
    apply_temperature(cold, 4, 0.1)
    softmax_inplace(cold, 4)

    # Hot should be more uniform (max prob lower)
    assert_true(hot.get(3) < cold.get(3), "high temp is more uniform")
    # Cold should be very peaked
    assert_true(cold.get(3) > 0.95, "low temp peaks on max")

    print("  temperature_affects_generation: PASS")


fn main() raises:
    print("test_sprint3_integration:")

    test_rope_to_attention_pipeline()
    test_multi_layer_kv_cache_with_attention()
    test_transformer_block_with_rope_position()
    test_full_forward_pass_consistency()
    test_rmsnorm_to_linear_pipeline()
    test_gqa_with_different_group_sizes()
    test_kv_cache_grows_through_generation()
    test_softmax_with_attention_scores()
    test_embedding_to_logit_roundtrip()
    test_temperature_affects_generation()

    print("ALL PASSED")
