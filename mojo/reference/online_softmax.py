"""Online softmax — single-pass numerically stable softmax.

This is the algorithm used inside FlashAttention: compute softmax
incrementally as new blocks of the key sequence arrive, without
needing to see all values first.

The key insight: maintain a running (max, sum_exp, output) tuple and
rescale when a new max is found.

Reference: Milakov & Gimelshein, "Online normalizer calculation for softmax" (2018)
"""

import numpy as np


def online_softmax_1d(x: np.ndarray) -> np.ndarray:
    """Single-pass online softmax for a 1D vector.

    Processes elements one at a time, maintaining running statistics.
    This is the scalar version of what FlashAttention does with tiles.
    """
    assert x.ndim == 1
    n = x.shape[0]

    m = -np.inf  # running max
    d = 0.0      # running sum of exp(x_i - m)

    # Pass 1: compute max and denominator in a single pass
    for i in range(n):
        m_new = max(m, float(x[i]))
        d = d * np.exp(m - m_new) + np.exp(float(x[i]) - m_new)
        m = m_new

    # Pass 2: compute output (could also be folded into pass 1 for attention)
    out = np.exp(x.astype(np.float64) - m) / d
    return out.astype(x.dtype)


def online_softmax_blocked(x: np.ndarray, block_size: int = 16) -> np.ndarray:
    """Block-wise online softmax — matches FlashAttention's tiling pattern.

    Processes blocks of elements at a time, rescaling when blocks are merged.
    This is exactly how FlashAttention computes attention weights.
    """
    assert x.ndim == 1
    n = x.shape[0]

    m = -np.inf
    d = 0.0
    out = np.zeros(n, dtype=np.float64)

    for start in range(0, n, block_size):
        end = min(start + block_size, n)
        block = x[start:end].astype(np.float64)

        m_block = np.max(block)
        m_new = max(m, m_block)

        # Rescale existing accumulator
        scale_old = np.exp(m - m_new)
        d = d * scale_old
        out[:start] *= scale_old

        # Add new block contribution
        exp_block = np.exp(block - m_new)
        d += np.sum(exp_block)
        out[start:end] = exp_block

        m = m_new

    return (out / d).astype(x.dtype)


def online_softmax_2d(x: np.ndarray, block_size: int = 16) -> np.ndarray:
    """Row-wise online softmax for 2D array. Each row processed independently."""
    assert x.ndim == 2
    result = np.zeros_like(x)
    for i in range(x.shape[0]):
        result[i] = online_softmax_blocked(x[i], block_size)
    return result


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_matches_stable():
    """Online softmax matches stable softmax."""
    rng = np.random.default_rng(42)
    x = rng.standard_normal(128).astype(np.float32)

    from softmax import softmax_stable
    ref = softmax_stable(x)

    online = online_softmax_1d(x)
    assert np.allclose(online, ref, rtol=1e-5, atol=1e-6), \
        f"Max diff: {np.max(np.abs(online - ref))}"
    print("  1d matches stable: PASS")


def _test_blocked_matches():
    """Blocked online softmax matches standard."""
    rng = np.random.default_rng(7)
    x = rng.standard_normal(200).astype(np.float32)

    from softmax import softmax_stable
    ref = softmax_stable(x)

    for bs in [4, 8, 16, 32, 64]:
        blocked = online_softmax_blocked(x, block_size=bs)
        assert np.allclose(blocked, ref, rtol=1e-5, atol=1e-6), \
            f"Block size {bs} failed, max diff: {np.max(np.abs(blocked - ref))}"

    print("  blocked [4,8,16,32,64]: PASS")


def _test_large_values():
    """Large values that stress rescaling."""
    x = np.array([1000.0, 1001.0, 999.0, 1000.5], dtype=np.float32)
    online = online_softmax_1d(x)
    assert np.all(np.isfinite(online)), "Online softmax produced inf/nan"
    assert np.allclose(np.sum(online), 1.0, atol=1e-6)
    print("  large values: PASS")


def _test_2d():
    """2D row-wise online softmax."""
    rng = np.random.default_rng(0)
    x = rng.standard_normal((8, 64)).astype(np.float32)

    from softmax import softmax_stable
    ref = softmax_stable(x, axis=-1)

    online = online_softmax_2d(x, block_size=16)
    assert np.allclose(online, ref, rtol=1e-5, atol=1e-6), \
        f"2D max diff: {np.max(np.abs(online - ref))}"
    print("  2d (8x64): PASS")


def _test_single_element():
    """Single element → softmax is 1.0."""
    x = np.array([3.14], dtype=np.float32)
    assert np.allclose(online_softmax_1d(x), [1.0], atol=1e-7)
    print("  single element: PASS")


if __name__ == "__main__":
    print("online_softmax reference tests:")
    _test_matches_stable()
    _test_blocked_matches()
    _test_large_values()
    _test_2d()
    _test_single_element()
    print("ALL PASSED")
