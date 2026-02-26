"""Token sampling strategies — correctness oracle.

Implements all sampling methods used in LLM inference:
  - Greedy (argmax)
  - Top-k
  - Top-p (nucleus)
  - Temperature scaling
  - Repetition penalty
  - Combined top-k + top-p + temperature

These are applied to logits (raw model output) to select the next token.

Tolerance: Exact for greedy/top-k/top-p masks. Statistical for sampling.
"""

import numpy as np


def greedy(logits: np.ndarray) -> int:
    """Greedy decoding: return token with highest logit."""
    return int(np.argmax(logits))


def temperature_scale(logits: np.ndarray, temperature: float) -> np.ndarray:
    """Scale logits by temperature. Higher temp = more random."""
    assert temperature > 0, "Temperature must be positive"
    return logits / temperature


def top_k_mask(logits: np.ndarray, k: int) -> np.ndarray:
    """Zero out all logits except the top-k highest.

    Returns masked logits (non-top-k set to -inf).
    """
    assert k > 0
    if k >= len(logits):
        return logits.copy()

    # Find k-th largest value
    top_k_vals = np.partition(logits, -k)[-k:]
    threshold = np.min(top_k_vals)

    masked = logits.copy()
    masked[logits < threshold] = -np.inf
    return masked


def top_p_mask(logits: np.ndarray, p: float) -> np.ndarray:
    """Nucleus sampling: keep smallest set of tokens whose cumulative
    probability exceeds p.

    Returns masked logits (excluded tokens set to -inf).
    """
    assert 0.0 < p <= 1.0

    # Sort descending
    sorted_indices = np.argsort(logits)[::-1]
    sorted_logits = logits[sorted_indices]

    # Convert to probabilities
    probs = np.exp(sorted_logits - np.max(sorted_logits))
    probs = probs / np.sum(probs)

    # Cumulative sum
    cumsum = np.cumsum(probs)

    # Find cutoff: keep tokens until cumsum > p
    # Include the first token that pushes us over p
    cutoff_idx = np.searchsorted(cumsum, p)
    if cutoff_idx < len(logits) - 1:
        cutoff_idx += 1  # include the boundary token

    # Mask
    masked = np.full_like(logits, -np.inf)
    masked[sorted_indices[:cutoff_idx]] = logits[sorted_indices[:cutoff_idx]]
    return masked


def repetition_penalty(
    logits: np.ndarray,
    token_ids: list[int],
    penalty: float = 1.2,
) -> np.ndarray:
    """Apply repetition penalty to previously generated tokens.

    For each token in token_ids:
      - If logit > 0: logit /= penalty
      - If logit < 0: logit *= penalty
    This discourages repetition.

    Reference: Keskar et al., "CTRL" (2019)
    """
    result = logits.copy()
    for token_id in set(token_ids):
        if 0 <= token_id < len(result):
            if result[token_id] > 0:
                result[token_id] /= penalty
            else:
                result[token_id] *= penalty
    return result


def sample(
    logits: np.ndarray,
    temperature: float = 1.0,
    top_k: int = 0,
    top_p: float = 1.0,
    repetition_penalty_val: float = 1.0,
    previous_tokens: list[int] | None = None,
    rng: np.random.Generator | None = None,
) -> int:
    """Full sampling pipeline: penalty → temperature → top-k → top-p → sample.

    Args:
        logits: Raw model output (vocab_size,)
        temperature: Sampling temperature (1.0 = normal, <1 = more greedy, >1 = more random)
        top_k: Keep only top-k tokens (0 = disabled)
        top_p: Nucleus sampling threshold (1.0 = disabled)
        repetition_penalty_val: Repetition penalty (1.0 = disabled)
        previous_tokens: Previously generated token ids for repetition penalty
        rng: Random number generator

    Returns:
        Selected token id
    """
    if rng is None:
        rng = np.random.default_rng()

    logits = logits.astype(np.float64).copy()

    # 1. Repetition penalty
    if repetition_penalty_val != 1.0 and previous_tokens:
        logits = repetition_penalty(logits, previous_tokens, repetition_penalty_val)

    # 2. Temperature
    if temperature != 1.0:
        logits = temperature_scale(logits, temperature)

    # 3. Top-k
    if top_k > 0:
        logits = top_k_mask(logits, top_k)

    # 4. Top-p
    if top_p < 1.0:
        logits = top_p_mask(logits, top_p)

    # 5. Convert to probabilities
    logits_max = np.max(logits[logits > -np.inf]) if np.any(logits > -np.inf) else 0.0
    exp_logits = np.where(logits > -np.inf, np.exp(logits - logits_max), 0.0)
    probs = exp_logits / np.sum(exp_logits)

    # 6. Sample
    return int(rng.choice(len(probs), p=probs))


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_greedy():
    logits = np.array([1.0, 5.0, 3.0, 2.0], dtype=np.float32)
    assert greedy(logits) == 1
    print("  greedy: PASS")


def _test_temperature():
    logits = np.array([1.0, 2.0, 3.0], dtype=np.float32)

    # Low temperature → sharper distribution
    cold = temperature_scale(logits, 0.1)
    hot = temperature_scale(logits, 10.0)

    cold_probs = np.exp(cold) / np.sum(np.exp(cold))
    hot_probs = np.exp(hot) / np.sum(np.exp(hot))

    # Cold should concentrate more on max
    assert cold_probs[2] > hot_probs[2], "Cold should be sharper"
    # Hot should be more uniform
    assert np.std(hot_probs) < np.std(cold_probs), "Hot should be more uniform"
    print("  temperature: PASS")


def _test_top_k():
    logits = np.array([1.0, 5.0, 3.0, 2.0, 4.0], dtype=np.float32)
    masked = top_k_mask(logits, k=3)

    # Top 3 are indices 1(5.0), 4(4.0), 2(3.0)
    assert masked[1] == 5.0
    assert masked[4] == 4.0
    assert masked[2] == 3.0
    assert masked[0] == -np.inf
    assert masked[3] == -np.inf
    print("  top_k: PASS")


def _test_top_p():
    # Logits that give clear probability ranking
    logits = np.array([10.0, 5.0, 1.0, 0.0, -5.0], dtype=np.float32)
    masked = top_p_mask(logits, p=0.95)

    # Token 0 (logit=10) should have >95% probability by itself
    probs = np.exp(logits - np.max(logits))
    probs = probs / np.sum(probs)

    # The top token should definitely be kept
    assert masked[0] > -np.inf, "Top token should be kept"
    print("  top_p: PASS")


def _test_repetition_penalty():
    logits = np.array([3.0, 1.0, -1.0, 2.0], dtype=np.float32)
    penalized = repetition_penalty(logits, [0, 2], penalty=1.5)

    # Token 0 (positive logit) should decrease
    assert penalized[0] < logits[0], "Positive logit should decrease"
    # Token 2 (negative logit) should become more negative
    assert penalized[2] < logits[2], "Negative logit should become more negative"
    # Token 1, 3 (not in list) should be unchanged
    assert penalized[1] == logits[1]
    assert penalized[3] == logits[3]
    print("  repetition_penalty: PASS")


def _test_full_pipeline():
    """Full sampling pipeline runs without error."""
    rng = np.random.default_rng(42)
    logits = rng.standard_normal(32000).astype(np.float32)  # Llama vocab size

    token = sample(
        logits,
        temperature=0.8,
        top_k=50,
        top_p=0.9,
        repetition_penalty_val=1.2,
        previous_tokens=[100, 200, 300],
        rng=rng,
    )

    assert 0 <= token < 32000, f"Token out of range: {token}"
    print("  full pipeline: PASS")


def _test_greedy_via_sample():
    """Temperature=0.01 should behave like greedy."""
    logits = np.array([1.0, 10.0, 3.0, 2.0], dtype=np.float32)
    rng = np.random.default_rng(0)

    # Very low temperature should always pick the max
    tokens = [sample(logits, temperature=0.01, rng=np.random.default_rng(i)) for i in range(100)]
    assert all(t == 1 for t in tokens), "Low temperature should act like greedy"
    print("  greedy via low temperature: PASS")


def _test_statistical_properties():
    """Top-k=1 should always return the same token."""
    logits = np.array([1.0, 5.0, 3.0], dtype=np.float32)
    rng = np.random.default_rng(42)

    tokens = [sample(logits, top_k=1, rng=np.random.default_rng(i)) for i in range(50)]
    assert all(t == 1 for t in tokens), "top_k=1 should always pick token 1"
    print("  top_k=1 deterministic: PASS")


if __name__ == "__main__":
    print("sampling reference tests:")
    _test_greedy()
    _test_temperature()
    _test_top_k()
    _test_top_p()
    _test_repetition_penalty()
    _test_full_pipeline()
    _test_greedy_via_sample()
    _test_statistical_properties()
    print("ALL PASSED")
