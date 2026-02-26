# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Speculative Decoding
# ===----------------------------------------------------------------------=== #

"""Speculative decoding for faster autoregressive generation.

Uses a small draft model to propose K candidate tokens, then verifies
them against the target model in a single forward pass. Accepted tokens
skip individual target model evaluations, yielding speedup proportional
to the acceptance rate.

Reference: Leviathan et al., "Fast Inference from Transformers via
Speculative Decoding" (2023).

Probability tensors are stored flat: [num_steps * vocab_size] where
step i's probabilities are at offset i * vocab_size.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.sampler import LCG


# ===----------------------------------------------------------------------=== #
# Speculative Decoding Result
# ===----------------------------------------------------------------------=== #

struct SpeculativeResult(Movable):
    """Result of one speculative decoding step.

    Contains accepted tokens and the number accepted out of K draft tokens.
    """
    var accepted_tokens: List[Int]
    var num_accepted: Int
    var bonus_token: Int  # Token from target model after accepted prefix (-1 if none)

    fn __init__(out self):
        self.accepted_tokens = List[Int]()
        self.num_accepted = 0
        self.bonus_token = -1

    fn __moveinit__(out self, deinit other: Self):
        self.accepted_tokens = other.accepted_tokens^
        self.num_accepted = other.num_accepted
        self.bonus_token = other.bonus_token

    fn total_tokens(self) -> Int:
        """Total tokens produced: accepted + bonus (if any)."""
        if self.bonus_token >= 0:
            return self.num_accepted + 1
        return self.num_accepted


# ===----------------------------------------------------------------------=== #
# Draft Token Generation
# ===----------------------------------------------------------------------=== #

fn draft_greedy(logits: Tensor[DType.float32], vocab_size: Int) -> Int:
    """Greedy argmax selection from logits."""
    var best_idx = 0
    var best_val = logits.get(0)
    for i in range(1, vocab_size):
        var v = logits.get(i)
        if v > best_val:
            best_val = v
            best_idx = i
    return best_idx


fn compute_probs(
    logits: Tensor[DType.float32],
    vocab_size: Int,
) -> Tensor[DType.float32]:
    """Convert logits to probabilities via softmax.

    Args:
        logits: Raw logits [vocab_size].
        vocab_size: Vocabulary size.

    Returns:
        Probabilities [vocab_size] summing to 1.
    """
    from math import exp

    var max_val = logits.get(0)
    for i in range(1, vocab_size):
        var v = logits.get(i)
        if v > max_val:
            max_val = v

    var probs = Tensor[DType.float32](Shape(vocab_size))
    var total: Float32 = 0.0
    for i in range(vocab_size):
        var e = Float32(exp(Float64(logits.get(i) - max_val)))
        probs.set(i, e)
        total += e

    if total > 0.0:
        for i in range(vocab_size):
            probs.set(i, probs.get(i) / total)

    return probs^


# ===----------------------------------------------------------------------=== #
# Flat Probability Tensor Helpers
# ===----------------------------------------------------------------------=== #

fn get_prob(
    probs_flat: Tensor[DType.float32],
    step: Int,
    token: Int,
    vocab_size: Int,
) -> Float32:
    """Get probability at probs_flat[step * vocab_size + token]."""
    return probs_flat.get(step * vocab_size + token)


fn get_prob_slice_offset(step: Int, vocab_size: Int) -> Int:
    """Get the flat offset for a step's probability slice."""
    return step * vocab_size


# ===----------------------------------------------------------------------=== #
# Verification
# ===----------------------------------------------------------------------=== #

fn verify_tokens(
    draft_tokens: List[Int],
    draft_probs: Tensor[DType.float32],
    target_probs: Tensor[DType.float32],
    k: Int,
    vocab_size: Int,
    mut rng: LCG,
) -> SpeculativeResult:
    """Verify draft tokens against target model probabilities.

    For each draft token x_i:
      - If p_target(x_i) >= p_draft(x_i): accept deterministically
      - Else: accept with probability p_target(x_i) / p_draft(x_i)

    When a token is rejected, sample from the residual distribution:
      p_residual(x) = max(0, p_target(x) - p_draft(x)) / Z

    Args:
        draft_tokens: K draft token IDs.
        draft_probs: Flat [K * vocab_size] — draft probabilities per step.
        target_probs: Flat [(K+1) * vocab_size] — target probabilities per step
                      (one extra for the bonus token after all accepted).
        k: Number of draft tokens.
        vocab_size: Vocabulary size.
        rng: Random number generator.

    Returns:
        SpeculativeResult with accepted tokens and optional bonus.
    """
    var result = SpeculativeResult()

    for i in range(k):
        var token = draft_tokens[i]
        var p_draft = get_prob(draft_probs, i, token, vocab_size)
        var p_target = get_prob(target_probs, i, token, vocab_size)

        if p_draft <= 0.0:
            # Draft assigned zero probability — sample from target
            var bonus = sample_from_flat(target_probs, i, vocab_size, rng)
            result.bonus_token = bonus
            return result^

        var ratio = p_target / p_draft
        if ratio >= 1.0:
            result.accepted_tokens.append(token)
            result.num_accepted += 1
        else:
            var r = rng.next_float()
            if r < ratio:
                result.accepted_tokens.append(token)
                result.num_accepted += 1
            else:
                # Rejected — sample from residual
                var bonus = sample_residual_flat(
                    target_probs, draft_probs, i, vocab_size, rng
                )
                result.bonus_token = bonus
                return result^

    # All K tokens accepted — bonus from target_probs step k
    var bonus = sample_from_flat(target_probs, k, vocab_size, rng)
    result.bonus_token = bonus

    return result^


fn sample_from_flat(
    probs_flat: Tensor[DType.float32],
    step: Int,
    vocab_size: Int,
    mut rng: LCG,
) -> Int:
    """Sample a token from step's probability slice in flat tensor."""
    var r = rng.next_float()
    var cumulative: Float32 = 0.0
    var base = step * vocab_size
    for i in range(vocab_size):
        cumulative += probs_flat.get(base + i)
        if r < cumulative:
            return i
    return vocab_size - 1


fn sample_from_probs(
    probs: Tensor[DType.float32],
    vocab_size: Int,
    mut rng: LCG,
) -> Int:
    """Sample a token from a probability distribution (1D tensor)."""
    var r = rng.next_float()
    var cumulative: Float32 = 0.0
    for i in range(vocab_size):
        cumulative += probs.get(i)
        if r < cumulative:
            return i
    return vocab_size - 1


fn sample_residual_flat(
    target_probs: Tensor[DType.float32],
    draft_probs: Tensor[DType.float32],
    step: Int,
    vocab_size: Int,
    mut rng: LCG,
) -> Int:
    """Sample from residual distribution at a given step.

    residual(x) = max(0, p_target(x) - p_draft(x)) / Z
    """
    var residual = Tensor[DType.float32](Shape(vocab_size))
    var total: Float32 = 0.0
    var t_base = step * vocab_size
    var d_base = step * vocab_size

    for i in range(vocab_size):
        var diff = target_probs.get(t_base + i) - draft_probs.get(d_base + i)
        if diff > 0.0:
            residual.set(i, diff)
            total += diff
        else:
            residual.set(i, 0.0)

    if total <= 0.0:
        return sample_from_flat(target_probs, step, vocab_size, rng)

    for i in range(vocab_size):
        residual.set(i, residual.get(i) / total)

    return sample_from_probs(residual, vocab_size, rng)


# ===----------------------------------------------------------------------=== #
# Acceptance Rate Tracking
# ===----------------------------------------------------------------------=== #

struct AcceptanceTracker(Copyable, Movable):
    """Track speculative decoding acceptance rate over time."""
    var total_draft: Int
    var total_accepted: Int
    var total_bonus: Int
    var num_steps: Int

    fn __init__(out self):
        self.total_draft = 0
        self.total_accepted = 0
        self.total_bonus = 0
        self.num_steps = 0

    fn __copyinit__(out self, existing: Self):
        self.total_draft = existing.total_draft
        self.total_accepted = existing.total_accepted
        self.total_bonus = existing.total_bonus
        self.num_steps = existing.num_steps

    fn __moveinit__(out self, deinit other: Self):
        self.total_draft = other.total_draft
        self.total_accepted = other.total_accepted
        self.total_bonus = other.total_bonus
        self.num_steps = other.num_steps

    fn update(mut self, result: SpeculativeResult, k: Int):
        """Record results of one speculative step."""
        self.total_draft += k
        self.total_accepted += result.num_accepted
        if result.bonus_token >= 0:
            self.total_bonus += 1
        self.num_steps += 1

    fn acceptance_rate(self) -> Float32:
        """Fraction of draft tokens accepted."""
        if self.total_draft == 0:
            return 0.0
        return Float32(self.total_accepted) / Float32(self.total_draft)

    fn tokens_per_step(self) -> Float32:
        """Average tokens produced per speculative step."""
        if self.num_steps == 0:
            return 0.0
        var total = self.total_accepted + self.total_bonus
        return Float32(total) / Float32(self.num_steps)

    fn speedup_estimate(self) -> Float32:
        """Estimated speedup vs standard autoregressive decoding."""
        var tps = self.tokens_per_step()
        if tps <= 0.0:
            return 1.0
        return tps
