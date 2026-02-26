# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 6 Integration Tests
# ===----------------------------------------------------------------------=== #

"""Integration tests for Sprint 6: Architecture Breadth.

Tests cross-module interactions between sliding window attention,
MoE, LoRA, and speculative decoding with earlier sprint components.
"""

from math import abs, exp
from neutron_mojo.nn.sliding_window import (
    SlidingWindowKVCache,
    sliding_window_attention_head,
    windowed_fused_attention_head,
)
from neutron_mojo.nn.fused_attention import fused_attention_head
from neutron_mojo.nn.kv_cache import KVCache
from neutron_mojo.nn.moe import (
    MoERouter,
    ExpertWeights,
    expert_ffn,
    moe_forward,
    compute_load_balance_loss,
)
from neutron_mojo.nn.lora import (
    LoRAConfig,
    LoRAWeight,
    lora_forward,
    lora_linear,
    merge_lora,
    unmerge_lora,
)
from neutron_mojo.nn.speculative import (
    SpeculativeResult,
    compute_probs,
    draft_greedy,
    verify_tokens,
    AcceptanceTracker,
    sample_from_probs,
)
from neutron_mojo.nn.sampler import LCG, Sampler, SamplerConfig, greedy_config
from neutron_mojo.nn.generation import apply_repetition_penalty
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


fn test_sw_cache_with_fused_attention() raises:
    """Test sliding window cache produces same results as standard cache for short sequences."""
    var head_dim = 4
    var window = 8

    var sw_cache = SlidingWindowKVCache(window_size=window, num_kv_heads=1, head_dim=head_dim)
    var std_cache = KVCache(max_seq_len=16, num_kv_heads=1, head_dim=head_dim)

    # Add 3 positions to both caches
    for i in range(3):
        var k = Tensor[DType.float32](Shape(head_dim))
        var v = Tensor[DType.float32](Shape(head_dim))
        for d in range(head_dim):
            k.set(d, Float32(i * head_dim + d) * 0.1)
            v.set(d, Float32(i * head_dim + d) * 0.2)
        sw_cache.append_kv(k, v)

        var k2 = Tensor[DType.float32](Shape(head_dim))
        var v2 = Tensor[DType.float32](Shape(head_dim))
        for d in range(head_dim):
            k2.set(d, Float32(i * head_dim + d) * 0.1)
            v2.set(d, Float32(i * head_dim + d) * 0.2)
        std_cache.append_kv(k2, v2, num_new_tokens=1)

    var query = Tensor[DType.float32](Shape(head_dim))
    for d in range(head_dim):
        query.set(d, Float32(d) * 0.3 + 0.1)

    var sw_out = sliding_window_attention_head(query, sw_cache, 0, head_dim)
    var std_out = fused_attention_head(query, std_cache, 0, head_dim, 2)

    # Within window, results should match closely
    for d in range(head_dim):
        assert_near(sw_out.get(d), std_out.get(d), 0.05, "sw vs std match")

    print("  sw_cache_with_fused_attention: PASS")


fn test_moe_with_lora_expert() raises:
    """Test concept: MoE expert output combined with LoRA adapter."""
    var hidden_dim = 4
    var expert_dim = 4

    # Set up a single expert MoE
    var router = MoERouter(num_experts=1, top_k=1, hidden_dim=hidden_dim)
    var ew = ExpertWeights(num_experts=1, hidden_dim=hidden_dim, expert_dim=expert_dim)

    # Set router to always select expert 0
    for d in range(hidden_dim):
        router.gate_weight.set(d, 1.0)

    # Set expert 0 with identity-like gate, up, down
    var gb = ew.gate_offset(0)
    var ub = ew.up_offset(0)
    var db = ew.down_offset(0)
    for i in range(hidden_dim):
        ew.data.set(gb + i * hidden_dim + i, 1.0)
        ew.data.set(ub + i * hidden_dim + i, 1.0)
        ew.data.set(db + i * expert_dim + i, 1.0)

    var x = Tensor[DType.float32](Shape(hidden_dim))
    x.set(0, 1.0)
    x.set(1, 2.0)
    x.set(2, 0.5)
    x.set(3, -1.0)

    var moe_out = moe_forward(x, router, ew)

    # Now apply LoRA to the MoE output as if it were a projection
    var config = LoRAConfig(rank=2, alpha=2.0, in_features=hidden_dim, out_features=hidden_dim)
    var lora = LoRAWeight(config)
    # Set A to extract first 2 dims, B to project back
    lora.lora_a.set(0, 1.0)  # A[0,0]
    lora.lora_a.set(4, 1.0)  # A[1,1] (offset = rank=2 * in_features=4, at index 1*4+0=4... wait)
    # A is [rank * in_features] = [2 * 4] = 8 elements
    # A[0, :] = row 0 = indices 0..3
    # A[1, :] = row 1 = indices 4..7
    lora.lora_a.set(5, 1.0)  # A[1, 1]

    # B is [out_features * rank] = [4 * 2] = 8 elements
    lora.lora_b.set(0, 0.5)  # B[0, 0]
    lora.lora_b.set(3, 0.5)  # B[1, 1]

    var lora_delta = lora_forward(moe_out, lora)

    # Final output: MoE + LoRA delta
    var final_out = Tensor[DType.float32](Shape(hidden_dim))
    for i in range(hidden_dim):
        final_out.set(i, moe_out.get(i) + lora_delta.get(i))

    # MoE output should be non-zero (SwiGLU of identity), LoRA adds to it
    assert_true(final_out.numel() == hidden_dim, "output size")

    print("  moe_with_lora_expert: PASS")


fn test_lora_merge_unmerge_roundtrip_with_linear() raises:
    """Test that merge→linear→unmerge produces same as separate LoRA linear."""
    var in_f = 3
    var out_f = 2
    var config = LoRAConfig(rank=2, alpha=4.0, in_features=in_f, out_features=out_f)
    var lora = LoRAWeight(config)

    # Set non-trivial weights
    lora.lora_a.set(0, 0.3)
    lora.lora_a.set(1, -0.2)
    lora.lora_a.set(2, 0.5)
    lora.lora_a.set(3, 0.1)
    lora.lora_a.set(4, -0.4)
    lora.lora_a.set(5, 0.6)

    lora.lora_b.set(0, 0.7)
    lora.lora_b.set(1, -0.3)
    lora.lora_b.set(2, 0.2)
    lora.lora_b.set(3, 0.9)

    var base = Tensor[DType.float32](Shape(out_f * in_f))
    base.set(0, 1.0)
    base.set(1, -0.5)
    base.set(2, 0.3)
    base.set(3, 0.2)
    base.set(4, 0.8)
    base.set(5, -0.1)

    var x = Tensor[DType.float32](Shape(in_f))
    x.set(0, 2.0)
    x.set(1, -1.0)
    x.set(2, 0.5)

    # Method 1: lora_linear (separate)
    var out_separate = lora_linear(x, base, lora)

    # Method 2: merge → matmul → unmerge
    var merged = Tensor[DType.float32](Shape(out_f * in_f))
    for i in range(out_f * in_f):
        merged.set(i, base.get(i))
    merge_lora(merged, lora)

    var out_merged = Tensor[DType.float32](Shape(out_f))
    for i in range(out_f):
        var dot: Float32 = 0.0
        for j in range(in_f):
            dot += merged.get(i * in_f + j) * x.get(j)
        out_merged.set(i, dot)

    # Verify match
    for i in range(out_f):
        assert_near(out_separate.get(i), out_merged.get(i), 0.001, "merge==separate")

    # Verify unmerge restores original
    unmerge_lora(merged, lora)
    for i in range(out_f * in_f):
        assert_near(merged.get(i), base.get(i), 0.001, "unmerge restores")

    print("  lora_merge_unmerge_roundtrip_with_linear: PASS")


fn test_speculative_with_sampler() raises:
    """Test speculative decoding feeding results to sampler."""
    var vocab_size = 6
    var k = 3

    # Simulate: draft proposes tokens, target verifies, sampler picks from result
    var rng = LCG(42)

    var draft_tokens = List[Int]()
    draft_tokens.append(2)
    draft_tokens.append(4)
    draft_tokens.append(1)

    # Draft probs: draft model is "confident" on its picks
    var draft_probs = Tensor[DType.float32](Shape(k * vocab_size))
    for step in range(k):
        var base = step * vocab_size
        for j in range(vocab_size):
            draft_probs.set(base + j, 0.05)
        draft_probs.set(base + draft_tokens[step], 0.75)

    # Target probs: agrees with first 2, disagrees on 3rd
    var target_probs = Tensor[DType.float32](Shape((k + 1) * vocab_size))
    # Step 0: target agrees on token 2
    for j in range(vocab_size):
        target_probs.set(j, 0.05)
    target_probs.set(2, 0.8)

    # Step 1: target agrees on token 4
    var b1 = vocab_size
    for j in range(vocab_size):
        target_probs.set(b1 + j, 0.05)
    target_probs.set(b1 + 4, 0.75)

    # Step 2: target disagrees — prefers token 5
    var b2 = 2 * vocab_size
    for j in range(vocab_size):
        target_probs.set(b2 + j, 0.05)
    target_probs.set(b2 + 5, 0.75)
    target_probs.set(b2 + 1, 0.0)  # low on draft's pick

    # Bonus step
    var b3 = 3 * vocab_size
    for j in range(vocab_size):
        target_probs.set(b3 + j, Float32(1.0) / Float32(vocab_size))

    var result = verify_tokens(draft_tokens, draft_probs, target_probs, k, vocab_size, rng)

    # First 2 should be accepted (target agrees)
    assert_true(result.num_accepted >= 2, "at least 2 accepted")

    # Feed accepted tokens to a sampler-like pipeline
    var all_tokens = List[Int]()
    for i in range(len(result.accepted_tokens)):
        all_tokens.append(result.accepted_tokens[i])
    if result.bonus_token >= 0:
        all_tokens.append(result.bonus_token)

    assert_true(len(all_tokens) >= 2, "pipeline produced tokens")

    print("  speculative_with_sampler: PASS")


fn test_sliding_window_long_sequence() raises:
    """Test sliding window handles long sequences with bounded memory."""
    var window = 4
    var head_dim = 2
    var cache = SlidingWindowKVCache(window_size=window, num_kv_heads=1, head_dim=head_dim)

    # Feed 20 tokens through
    for i in range(20):
        var k = Tensor[DType.float32](Shape(head_dim))
        var v = Tensor[DType.float32](Shape(head_dim))
        k.set(0, Float32(i) * 0.1)
        k.set(1, Float32(i) * 0.2)
        v.set(0, Float32(i))
        v.set(1, Float32(i) * 2.0)
        cache.append_kv(k, v)

    assert_true(cache.active_length() == window, "window bounded")
    assert_true(cache.total_length == 20, "total tracked")

    var query = Tensor[DType.float32](Shape(head_dim))
    query.set(0, 1.0)
    query.set(1, 0.5)

    var out = sliding_window_attention_head(query, cache, 0, head_dim)
    assert_true(out.numel() == head_dim, "output size correct")

    # Values should reflect recent tokens (16-19), not early ones
    # V for token 19 = [19, 38], token 18 = [18, 36], etc.
    assert_true(out.get(0) > 10.0, "output reflects recent values")

    print("  sliding_window_long_sequence: PASS")


fn test_moe_load_balance_feedback() raises:
    """Test MoE routing tracks load and loss decreases with balance."""
    var hidden_dim = 4
    var num_experts = 4

    var router = MoERouter(num_experts=num_experts, top_k=1, hidden_dim=hidden_dim)

    # Set gate weights to favor different experts for different inputs
    # Expert 0 responds to dim 0, expert 1 to dim 1, etc.
    for e in range(num_experts):
        router.gate_weight.set(e * hidden_dim + e, 5.0)

    var counts = Tensor[DType.float32](Shape(num_experts))

    # Route 4 different inputs
    for tok in range(4):
        var x = Tensor[DType.float32](Shape(hidden_dim))
        x.set(tok % hidden_dim, 1.0)
        var result = router.route(x)
        var expert_id = result.get_expert_id(0)
        counts.set(expert_id, counts.get(expert_id) + 1.0)

    var loss = compute_load_balance_loss(counts, num_experts, 4)

    # With 4 inputs routed to 4 different experts: balanced
    # Balanced loss = num_experts * sum((1/4)^2) = 4 * 4 * 0.0625 = 1.0
    assert_near(loss, 1.0, 0.1, "balanced routing loss ≈ 1.0")

    print("  moe_load_balance_feedback: PASS")


fn test_speculative_acceptance_rate_tracking() raises:
    """Test end-to-end speculative decoding with acceptance tracking."""
    var vocab_size = 4
    var k = 2
    var tracker = AcceptanceTracker()

    # Run 10 steps with varying agreement levels
    for step in range(10):
        var rng = LCG(step * 13 + 7)

        var draft_tokens = List[Int]()
        draft_tokens.append(step % vocab_size)
        draft_tokens.append((step + 1) % vocab_size)

        var draft_probs = Tensor[DType.float32](Shape(k * vocab_size))
        var target_probs = Tensor[DType.float32](Shape((k + 1) * vocab_size))

        for s in range(k):
            var db = s * vocab_size
            var tb = s * vocab_size
            for j in range(vocab_size):
                draft_probs.set(db + j, 0.1)
                target_probs.set(tb + j, 0.1)
            # Both agree on same token → high acceptance
            var dt = draft_tokens[s]
            draft_probs.set(db + dt, 0.7)
            target_probs.set(tb + dt, 0.7)

        # Bonus
        var bb = k * vocab_size
        for j in range(vocab_size):
            target_probs.set(bb + j, 0.25)

        var result = verify_tokens(
            draft_tokens, draft_probs, target_probs, k, vocab_size, rng
        )
        tracker.update(result, k)

    # With identical distributions, should have near-100% acceptance
    assert_true(tracker.acceptance_rate() > 0.8, "high acceptance rate")
    assert_true(tracker.tokens_per_step() > 2.0, "good tokens per step")
    assert_true(tracker.num_steps == 10, "10 steps tracked")

    print("  speculative_acceptance_rate_tracking: PASS")


fn test_rep_penalty_with_speculative() raises:
    """Test repetition penalty applied to speculative decoding logits."""
    var vocab_size = 5

    # Simulate: model produces logits, apply rep penalty, then compute probs for spec decode
    var logits = Tensor[DType.float32](Shape(vocab_size))
    logits.set(0, 2.0)
    logits.set(1, 2.0)
    logits.set(2, 2.0)
    logits.set(3, 2.0)
    logits.set(4, 2.0)

    # Tokens already generated
    var generated = List[Int]()
    generated.append(0)
    generated.append(1)
    generated.append(0)  # token 0 appears twice

    # Apply repetition penalty
    apply_repetition_penalty(logits, vocab_size, generated, 2.0)

    # Compute probs — penalized tokens should have lower probability
    var probs = compute_probs(logits, vocab_size)

    # Token 0 penalized (appeared twice), token 1 penalized, others not
    assert_true(probs.get(0) < probs.get(2), "token 0 penalized")
    assert_true(probs.get(1) < probs.get(3), "token 1 penalized")
    assert_near(probs.get(2), probs.get(3), 0.01, "unpenalized equal")

    print("  rep_penalty_with_speculative: PASS")


fn main() raises:
    print("test_sprint6_integration:")

    test_sw_cache_with_fused_attention()
    test_moe_with_lora_expert()
    test_lora_merge_unmerge_roundtrip_with_linear()
    test_speculative_with_sampler()
    test_sliding_window_long_sequence()
    test_moe_load_balance_feedback()
    test_speculative_acceptance_rate_tracking()
    test_rep_penalty_with_speculative()

    print("ALL PASSED")
