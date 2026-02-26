"""Softmax — correctness oracle for Mojo kernels.

Three implementations:
  1. Naive (numerically unstable — for comparison only)
  2. Stable (max subtraction — standard)
  3. Online (single-pass, used in FlashAttention)

Tolerances: FP32 1e-6, FP16 1e-3
"""

import numpy as np


def softmax_naive(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Naive softmax. Numerically unstable for large values."""
    e = np.exp(x)
    return e / np.sum(e, axis=axis, keepdims=True)


def softmax_stable(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Numerically stable softmax: subtract max before exp."""
    x_max = np.max(x, axis=axis, keepdims=True)
    e = np.exp(x - x_max)
    return e / np.sum(e, axis=axis, keepdims=True)


def log_softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Log-softmax (numerically stable)."""
    x_max = np.max(x, axis=axis, keepdims=True)
    shifted = x - x_max
    log_sum_exp = np.log(np.sum(np.exp(shifted), axis=axis, keepdims=True))
    return shifted - log_sum_exp


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_basic():
    x = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    s = softmax_stable(x)
    assert np.allclose(np.sum(s), 1.0, atol=1e-6), "Softmax doesn't sum to 1"
    assert s[2] > s[1] > s[0], "Softmax order wrong"
    print("  basic: PASS")


def _test_stability():
    """Large values that would overflow naive softmax."""
    x = np.array([1000.0, 1001.0, 1002.0], dtype=np.float32)

    s_stable = softmax_stable(x)
    assert np.all(np.isfinite(s_stable)), "Stable softmax produced inf/nan"
    assert np.allclose(np.sum(s_stable), 1.0, atol=1e-6)

    # Naive will overflow
    s_naive = softmax_naive(x)
    if np.any(~np.isfinite(s_naive)):
        print("  stability (naive overflows, stable doesn't): PASS")
    else:
        # Naive might work on some platforms with extended precision
        assert np.allclose(s_naive, s_stable, rtol=1e-5)
        print("  stability: PASS (naive survived)")


def _test_2d():
    """Softmax along last axis of 2D array."""
    rng = np.random.default_rng(42)
    x = rng.standard_normal((4, 8)).astype(np.float32)
    s = softmax_stable(x, axis=-1)

    assert s.shape == (4, 8)
    for row in range(4):
        assert np.allclose(np.sum(s[row]), 1.0, atol=1e-6), f"Row {row} doesn't sum to 1"

    print("  2d (4x8): PASS")


def _test_batched():
    """Softmax on batched attention scores: (batch, heads, seq, seq)."""
    rng = np.random.default_rng(7)
    x = rng.standard_normal((2, 8, 64, 64)).astype(np.float32)
    s = softmax_stable(x, axis=-1)

    assert s.shape == x.shape
    # Each row along last axis should sum to 1
    sums = np.sum(s, axis=-1)
    assert np.allclose(sums, 1.0, atol=1e-5), f"Max sum error: {np.max(np.abs(sums - 1.0))}"
    print("  batched (2,8,64,64): PASS")


def _test_log_softmax():
    """log_softmax matches log(softmax)."""
    rng = np.random.default_rng(0)
    x = rng.standard_normal((32,)).astype(np.float32)

    ls = log_softmax(x)
    ls_ref = np.log(softmax_stable(x))

    assert np.allclose(ls, ls_ref, rtol=1e-5, atol=1e-6), "log_softmax mismatch"
    print("  log_softmax: PASS")


def _test_uniform():
    """Uniform input → uniform output."""
    x = np.ones(10, dtype=np.float32) * 5.0
    s = softmax_stable(x)
    assert np.allclose(s, 0.1, atol=1e-6), "Uniform input should give uniform output"
    print("  uniform: PASS")


if __name__ == "__main__":
    print("softmax reference tests:")
    _test_basic()
    _test_stability()
    _test_2d()
    _test_batched()
    _test_log_softmax()
    _test_uniform()
    print("ALL PASSED")
