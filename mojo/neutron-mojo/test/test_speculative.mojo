# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Speculative Decoding Tests
# ===----------------------------------------------------------------------=== #

"""Tests for speculative decoding verification, sampling, and tracking."""

from math import abs
from neutron_mojo.nn.speculative import (
    SpeculativeResult,
    compute_probs,
    draft_greedy,
    verify_tokens,
    sample_from_probs,
    sample_from_flat,
    AcceptanceTracker,
)
from neutron_mojo.nn.sampler import LCG
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


fn set_flat_probs(
    mut flat: Tensor[DType.float32],
    step: Int,
    vocab_size: Int,
    p0: Float32, p1: Float32, p2: Float32, p3: Float32,
):
    """Helper: set 4-element probability slice in flat tensor."""
    var base = step * vocab_size
    flat.set(base + 0, p0)
    flat.set(base + 1, p1)
    flat.set(base + 2, p2)
    flat.set(base + 3, p3)


fn test_speculative_result() raises:
    """Test SpeculativeResult struct."""
    var result = SpeculativeResult()
    assert_true(result.num_accepted == 0, "initial num_accepted")
    assert_true(result.bonus_token == -1, "initial bonus")
    assert_true(result.total_tokens() == 0, "initial total")

    result.accepted_tokens.append(5)
    result.num_accepted = 1
    assert_true(result.total_tokens() == 1, "1 accepted, no bonus")

    result.bonus_token = 7
    assert_true(result.total_tokens() == 2, "1 accepted + bonus")

    print("  speculative_result: PASS")


fn test_draft_greedy() raises:
    """Test greedy argmax selection."""
    var logits = Tensor[DType.float32](Shape(5))
    logits.set(0, 1.0)
    logits.set(1, 5.0)
    logits.set(2, 3.0)
    logits.set(3, 2.0)
    logits.set(4, 4.0)

    var best = draft_greedy(logits, 5)
    assert_true(best == 1, "argmax = 1")

    print("  draft_greedy: PASS")


fn test_compute_probs() raises:
    """Test softmax probability computation."""
    var logits = Tensor[DType.float32](Shape(3))
    logits.set(0, 1.0)
    logits.set(1, 2.0)
    logits.set(2, 3.0)

    var probs = compute_probs(logits, 3)

    var total = probs.get(0) + probs.get(1) + probs.get(2)
    assert_near(total, 1.0, 0.01, "probs sum to 1")
    assert_true(probs.get(2) > probs.get(1), "p(2) > p(1)")
    assert_true(probs.get(1) > probs.get(0), "p(1) > p(0)")
    assert_near(probs.get(0), 0.09, 0.01, "softmax[0]")

    print("  compute_probs: PASS")


fn test_verify_all_accepted() raises:
    """Test verification when target always agrees with draft."""
    var vocab_size = 4
    var k = 2
    var rng = LCG(42)

    var draft_tokens = List[Int]()
    draft_tokens.append(1)
    draft_tokens.append(2)

    # Draft probs: [k * vocab_size]
    var draft_probs = Tensor[DType.float32](Shape(k * vocab_size))
    set_flat_probs(draft_probs, 0, vocab_size, 0.1, 0.4, 0.3, 0.2)
    set_flat_probs(draft_probs, 1, vocab_size, 0.1, 0.2, 0.5, 0.2)

    # Target probs: [(k+1) * vocab_size] — higher on draft tokens
    var target_probs = Tensor[DType.float32](Shape((k + 1) * vocab_size))
    set_flat_probs(target_probs, 0, vocab_size, 0.05, 0.7, 0.15, 0.1)
    set_flat_probs(target_probs, 1, vocab_size, 0.05, 0.1, 0.75, 0.1)
    set_flat_probs(target_probs, 2, vocab_size, 0.1, 0.1, 0.1, 0.7)  # bonus

    var result = verify_tokens(draft_tokens, draft_probs, target_probs, k, vocab_size, rng)

    assert_true(result.num_accepted == 2, "all 2 accepted")
    assert_true(result.accepted_tokens[0] == 1, "token 0 = 1")
    assert_true(result.accepted_tokens[1] == 2, "token 1 = 2")
    assert_true(result.bonus_token >= 0, "bonus token generated")
    assert_true(result.total_tokens() == 3, "total = 2 + 1 bonus")

    print("  verify_all_accepted: PASS")


fn test_verify_rejection() raises:
    """Test verification when target rejects a draft token."""
    var vocab_size = 3
    var k = 1

    var rejects = 0
    for trial in range(50):
        var rng = LCG(trial)
        var draft_tokens = List[Int]()
        draft_tokens.append(0)

        var draft_probs = Tensor[DType.float32](Shape(k * vocab_size))
        draft_probs.set(0, 0.9)
        draft_probs.set(1, 0.05)
        draft_probs.set(2, 0.05)

        var target_probs = Tensor[DType.float32](Shape((k + 1) * vocab_size))
        # Step 0: low prob on token 0
        target_probs.set(0, 0.01)
        target_probs.set(1, 0.49)
        target_probs.set(2, 0.50)
        # Step 1: bonus (not reached if rejected)
        target_probs.set(3, 0.33)
        target_probs.set(4, 0.33)
        target_probs.set(5, 0.34)

        var result = verify_tokens(draft_tokens, draft_probs, target_probs, k, vocab_size, rng)
        if result.num_accepted == 0:
            rejects += 1

    # ratio = 0.01/0.9 ≈ 0.011 → ~99% rejection
    assert_true(rejects > 30, "high rejection rate when target disagrees")

    print("  verify_rejection: PASS")


fn test_sample_from_probs() raises:
    """Test sampling from probability distribution."""
    var probs = Tensor[DType.float32](Shape(3))
    probs.set(0, 0.0)
    probs.set(1, 0.0)
    probs.set(2, 1.0)

    var rng = LCG(42)
    var token = sample_from_probs(probs, 3, rng)
    assert_true(token == 2, "deterministic sample from [0, 0, 1]")

    probs.set(0, 1.0)
    probs.set(1, 0.0)
    probs.set(2, 0.0)
    var rng2 = LCG(99)
    var token2 = sample_from_probs(probs, 3, rng2)
    assert_true(token2 == 0, "deterministic sample from [1, 0, 0]")

    print("  sample_from_probs: PASS")


fn test_sample_from_flat() raises:
    """Test sampling from flat prob tensor at a specific step."""
    var flat = Tensor[DType.float32](Shape(6))
    # Step 0: [0, 0, 1]
    flat.set(0, 0.0)
    flat.set(1, 0.0)
    flat.set(2, 1.0)
    # Step 1: [1, 0, 0]
    flat.set(3, 1.0)
    flat.set(4, 0.0)
    flat.set(5, 0.0)

    var rng = LCG(42)
    var t0 = sample_from_flat(flat, 0, 3, rng)
    assert_true(t0 == 2, "step 0 → token 2")

    var rng2 = LCG(42)
    var t1 = sample_from_flat(flat, 1, 3, rng2)
    assert_true(t1 == 0, "step 1 → token 0")

    print("  sample_from_flat: PASS")


fn test_acceptance_tracker() raises:
    """Test acceptance rate tracking."""
    var tracker = AcceptanceTracker()
    assert_near(tracker.acceptance_rate(), 0.0, 0.01, "initial rate = 0")
    assert_near(tracker.tokens_per_step(), 0.0, 0.01, "initial tps = 0")

    # Step 1: 3 draft, 2 accepted, bonus
    var r1 = SpeculativeResult()
    r1.num_accepted = 2
    r1.bonus_token = 5
    tracker.update(r1, 3)

    assert_near(tracker.acceptance_rate(), 0.667, 0.01, "rate = 2/3")
    assert_near(tracker.tokens_per_step(), 3.0, 0.01, "tps = 3/1")

    # Step 2: 3 draft, 3 accepted, bonus
    var r2 = SpeculativeResult()
    r2.num_accepted = 3
    r2.bonus_token = 7
    tracker.update(r2, 3)

    assert_near(tracker.acceptance_rate(), 0.833, 0.01, "rate = 5/6")
    assert_near(tracker.tokens_per_step(), 3.5, 0.01, "tps = 7/2")

    # Step 3: 3 draft, 0 accepted, bonus (all rejected)
    var r3 = SpeculativeResult()
    r3.num_accepted = 0
    r3.bonus_token = 2
    tracker.update(r3, 3)

    assert_near(tracker.acceptance_rate(), 0.556, 0.02, "rate = 5/9")
    assert_near(tracker.tokens_per_step(), 2.667, 0.02, "tps = 8/3")

    print("  acceptance_tracker: PASS")


fn test_verify_preserves_distribution() raises:
    """Test that identical distributions yield 100% acceptance."""
    var vocab_size = 4
    var k = 3

    var total_accepted = 0
    var total_draft = 0

    for trial in range(100):
        var rng = LCG(trial * 7)

        # Draft tokens: sample from uniform
        var probs_1d = Tensor[DType.float32](Shape(vocab_size))
        for j in range(vocab_size):
            probs_1d.set(j, 0.25)

        var draft_tokens = List[Int]()
        for _ in range(k):
            draft_tokens.append(sample_from_probs(probs_1d, vocab_size, rng))

        # Both models have identical uniform probs
        var draft_probs = Tensor[DType.float32](Shape(k * vocab_size))
        var target_probs = Tensor[DType.float32](Shape((k + 1) * vocab_size))
        for step in range(k):
            for j in range(vocab_size):
                draft_probs.set(step * vocab_size + j, 0.25)
                target_probs.set(step * vocab_size + j, 0.25)
        # Bonus step
        for j in range(vocab_size):
            target_probs.set(k * vocab_size + j, 0.25)

        var result = verify_tokens(
            draft_tokens, draft_probs, target_probs, k, vocab_size, rng
        )
        total_accepted += result.num_accepted
        total_draft += k

    # Identical distributions → ratio = 1.0 → always accept
    var rate = Float32(total_accepted) / Float32(total_draft)
    assert_near(rate, 1.0, 0.01, "identical dists → 100% acceptance")

    print("  verify_preserves_distribution: PASS")


fn test_compute_probs_numerical_stability() raises:
    """Test softmax stability with large logits."""
    var logits = Tensor[DType.float32](Shape(3))
    logits.set(0, 1000.0)
    logits.set(1, 1001.0)
    logits.set(2, 1002.0)

    var probs = compute_probs(logits, 3)

    var total = probs.get(0) + probs.get(1) + probs.get(2)
    assert_near(total, 1.0, 0.01, "stable softmax sums to 1")
    assert_true(probs.get(2) > probs.get(1), "ordering preserved")
    assert_true(probs.get(0) > 0.0, "no zero from overflow")

    print("  compute_probs_numerical_stability: PASS")


fn test_residual_sampling_direction() raises:
    """Test that rejection samples from residual favoring target's preference."""
    var vocab_size = 4
    var k = 1

    # Draft strongly prefers token 0, target prefers token 3
    var counts = Tensor[DType.float32](Shape(vocab_size))
    for _ in range(vocab_size):
        counts.set(0, 0.0)

    for trial in range(200):
        var rng = LCG(trial + 1000)
        var draft_tokens = List[Int]()
        draft_tokens.append(0)

        var draft_probs = Tensor[DType.float32](Shape(k * vocab_size))
        draft_probs.set(0, 0.8)
        draft_probs.set(1, 0.1)
        draft_probs.set(2, 0.05)
        draft_probs.set(3, 0.05)

        var target_probs = Tensor[DType.float32](Shape((k + 1) * vocab_size))
        # Step 0: target prefers token 3
        target_probs.set(0, 0.05)
        target_probs.set(1, 0.05)
        target_probs.set(2, 0.1)
        target_probs.set(3, 0.8)
        # Bonus step
        target_probs.set(4, 0.25)
        target_probs.set(5, 0.25)
        target_probs.set(6, 0.25)
        target_probs.set(7, 0.25)

        var result = verify_tokens(
            draft_tokens, draft_probs, target_probs, k, vocab_size, rng
        )
        # When rejected, bonus should come from residual favoring token 3
        if result.num_accepted == 0 and result.bonus_token >= 0:
            var bt = result.bonus_token
            counts.set(bt, counts.get(bt) + 1.0)

    # Residual = max(0, target - draft)
    # token 0: max(0, 0.05-0.8) = 0
    # token 1: max(0, 0.05-0.1) = 0
    # token 2: max(0, 0.1-0.05) = 0.05
    # token 3: max(0, 0.8-0.05) = 0.75
    # So residual strongly favors token 3
    assert_true(counts.get(3) > counts.get(0), "residual favors token 3 over 0")
    assert_true(counts.get(3) > counts.get(1), "residual favors token 3 over 1")
    assert_true(counts.get(3) > counts.get(2), "residual favors token 3 over 2")

    print("  residual_sampling_direction: PASS")


fn main() raises:
    print("test_speculative:")

    test_speculative_result()
    test_draft_greedy()
    test_compute_probs()
    test_verify_all_accepted()
    test_verify_rejection()
    test_sample_from_probs()
    test_sample_from_flat()
    test_acceptance_tracker()
    test_verify_preserves_distribution()
    test_compute_probs_numerical_stability()
    test_residual_sampling_direction()

    print("ALL PASSED")
