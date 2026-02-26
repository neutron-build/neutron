"""RMS Normalization — correctness oracle for Mojo kernel.

RMSNorm(x) = x / sqrt(mean(x^2) + eps) * gamma

Used in Llama-3 and most modern LLMs (replaces LayerNorm).
Simpler than LayerNorm: no mean subtraction, no beta bias.

Reference: Zhang & Sennrich, "Root Mean Square Layer Normalization" (2019)

Tolerances: FP32 1e-6, FP16 1e-3
"""

import numpy as np


def rmsnorm(x: np.ndarray, gamma: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    """RMSNorm along the last axis.

    Args:
        x: Input tensor, any shape. Normalized along last axis.
        gamma: Scale parameter, shape matches last dim of x.
        eps: Epsilon for numerical stability.
    """
    # Compute RMS along last axis
    rms = np.sqrt(np.mean(x.astype(np.float64) ** 2, axis=-1, keepdims=True) + eps)
    return ((x.astype(np.float64) / rms) * gamma.astype(np.float64)).astype(x.dtype)


def rmsnorm_backward(
    x: np.ndarray, gamma: np.ndarray, grad_out: np.ndarray, eps: float = 1e-6
) -> tuple:
    """RMSNorm backward pass. Returns (grad_x, grad_gamma)."""
    x64 = x.astype(np.float64)
    g64 = gamma.astype(np.float64)
    go64 = grad_out.astype(np.float64)

    d = x64.shape[-1]
    ms = np.mean(x64 ** 2, axis=-1, keepdims=True)
    rms = np.sqrt(ms + eps)
    x_norm = x64 / rms

    # grad_gamma: sum over all dims except last
    grad_gamma = np.sum(go64 * x_norm, axis=tuple(range(x64.ndim - 1)))

    # grad_x
    dx_norm = go64 * g64
    grad_x = (dx_norm - x_norm * np.mean(dx_norm * x_norm, axis=-1, keepdims=True)) / rms

    return grad_x.astype(x.dtype), grad_gamma.astype(gamma.dtype)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_basic():
    x = np.array([[1.0, 2.0, 3.0, 4.0]], dtype=np.float32)
    gamma = np.ones(4, dtype=np.float32)

    out = rmsnorm(x, gamma)
    rms = np.sqrt(np.mean(x ** 2) + 1e-6)
    expected = x / rms

    assert np.allclose(out, expected, rtol=1e-5), f"Basic failed: {out} vs {expected}"
    print("  basic: PASS")


def _test_scale():
    """Gamma scaling works."""
    x = np.ones((2, 4), dtype=np.float32)
    gamma = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)

    out = rmsnorm(x, gamma)
    # RMS of all-ones is 1.0, so output should be gamma
    assert np.allclose(out, gamma, rtol=1e-5), f"Scale failed: {out}"
    print("  scale: PASS")


def _test_3d():
    """3D input (batch, seq, hidden)."""
    rng = np.random.default_rng(42)
    x = rng.standard_normal((2, 8, 64)).astype(np.float32)
    gamma = rng.standard_normal(64).astype(np.float32)

    out = rmsnorm(x, gamma)
    assert out.shape == x.shape, f"Shape mismatch: {out.shape}"

    # Verify each position independently
    for b in range(2):
        for s in range(8):
            rms = np.sqrt(np.mean(x[b, s].astype(np.float64) ** 2) + 1e-6)
            expected = (x[b, s].astype(np.float64) / rms * gamma.astype(np.float64))
            assert np.allclose(out[b, s], expected.astype(np.float32), rtol=1e-4), \
                f"Position [{b},{s}] failed"

    print("  3d (2,8,64): PASS")


def _test_gradient():
    """Gradient check via finite differences."""
    rng = np.random.default_rng(7)
    x = rng.standard_normal((4, 8)).astype(np.float64)
    gamma = rng.standard_normal(8).astype(np.float64)
    grad_out = rng.standard_normal((4, 8)).astype(np.float64)

    grad_x, grad_gamma = rmsnorm_backward(x, gamma, grad_out)

    # Finite difference check for grad_x
    eps_fd = 1e-5
    for i in range(4):
        for j in range(8):
            x_plus = x.copy()
            x_plus[i, j] += eps_fd
            x_minus = x.copy()
            x_minus[i, j] -= eps_fd
            fd = np.sum(grad_out * (rmsnorm(x_plus, gamma) - rmsnorm(x_minus, gamma))) / (2 * eps_fd)
            assert abs(grad_x[i, j] - fd) < 1e-3, \
                f"grad_x[{i},{j}]: analytic={grad_x[i,j]:.6f}, fd={fd:.6f}"

    print("  gradient check: PASS")


def _test_fp16():
    """FP16 with relaxed tolerance."""
    rng = np.random.default_rng(0)
    x = rng.standard_normal((2, 32)).astype(np.float16)
    gamma = np.ones(32, dtype=np.float16)

    out = rmsnorm(x, gamma)
    ref = rmsnorm(x.astype(np.float32), gamma.astype(np.float32))

    assert np.allclose(out.astype(np.float32), ref, rtol=1e-2, atol=1e-2), "FP16 mismatch"
    print("  fp16: PASS")


if __name__ == "__main__":
    print("rmsnorm reference tests:")
    _test_basic()
    _test_scale()
    _test_3d()
    _test_gradient()
    _test_fp16()
    print("ALL PASSED")
