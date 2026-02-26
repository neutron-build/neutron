"""Layer Normalization — correctness oracle for Mojo kernel.

LayerNorm(x) = (x - mean(x)) / sqrt(var(x) + eps) * gamma + beta

Used in GPT-2, BERT, and older transformer architectures.
Llama-3 uses RMSNorm instead, but LayerNorm is still needed for compatibility.

Reference: Ba, Kiros & Hinton, "Layer Normalization" (2016)

Tolerances: FP32 1e-6, FP16 1e-3
"""

import numpy as np


def layernorm(
    x: np.ndarray,
    gamma: np.ndarray,
    beta: np.ndarray,
    eps: float = 1e-5,
) -> np.ndarray:
    """Layer normalization along the last axis.

    Args:
        x: Input tensor, any shape. Normalized along last axis.
        gamma: Scale parameter, shape matches last dim of x.
        beta: Shift parameter, shape matches last dim of x.
        eps: Epsilon for numerical stability.
    """
    x64 = x.astype(np.float64)
    mean = np.mean(x64, axis=-1, keepdims=True)
    var = np.var(x64, axis=-1, keepdims=True)
    x_norm = (x64 - mean) / np.sqrt(var + eps)
    return (x_norm * gamma.astype(np.float64) + beta.astype(np.float64)).astype(x.dtype)


def layernorm_backward(
    x: np.ndarray,
    gamma: np.ndarray,
    beta: np.ndarray,
    grad_out: np.ndarray,
    eps: float = 1e-5,
) -> tuple:
    """LayerNorm backward. Returns (grad_x, grad_gamma, grad_beta)."""
    x64 = x.astype(np.float64)
    g64 = gamma.astype(np.float64)
    go64 = grad_out.astype(np.float64)
    d = x64.shape[-1]

    mean = np.mean(x64, axis=-1, keepdims=True)
    var = np.var(x64, axis=-1, keepdims=True)
    std_inv = 1.0 / np.sqrt(var + eps)
    x_norm = (x64 - mean) * std_inv

    grad_beta = np.sum(go64, axis=tuple(range(x64.ndim - 1)))
    grad_gamma = np.sum(go64 * x_norm, axis=tuple(range(x64.ndim - 1)))

    dx_norm = go64 * g64
    grad_x = (
        dx_norm
        - np.mean(dx_norm, axis=-1, keepdims=True)
        - x_norm * np.mean(dx_norm * x_norm, axis=-1, keepdims=True)
    ) * std_inv

    return (
        grad_x.astype(x.dtype),
        grad_gamma.astype(gamma.dtype),
        grad_beta.astype(beta.dtype),
    )


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_basic():
    x = np.array([[1.0, 2.0, 3.0, 4.0]], dtype=np.float32)
    gamma = np.ones(4, dtype=np.float32)
    beta = np.zeros(4, dtype=np.float32)

    out = layernorm(x, gamma, beta)

    # Manual: mean=2.5, var=1.25, std=sqrt(1.25+1e-5)
    mean = 2.5
    var = 1.25
    expected = (x - mean) / np.sqrt(var + 1e-5)
    assert np.allclose(out, expected, rtol=1e-5), f"Basic failed"

    # Output should have mean≈0 and var≈1
    assert abs(np.mean(out)) < 1e-5, f"Mean not zero: {np.mean(out)}"
    assert abs(np.var(out) - 1.0) < 0.01, f"Var not one: {np.var(out)}"
    print("  basic: PASS")


def _test_affine():
    """Gamma and beta shift the output."""
    x = np.array([[0.0, 1.0, 2.0, 3.0]], dtype=np.float32)
    gamma = np.array([2.0, 2.0, 2.0, 2.0], dtype=np.float32)
    beta = np.array([1.0, 1.0, 1.0, 1.0], dtype=np.float32)

    out = layernorm(x, gamma, beta)
    # After norm: zero-mean unit-var, then scale by 2 and shift by 1
    assert abs(np.mean(out) - 1.0) < 0.01, f"Affine mean wrong: {np.mean(out)}"
    print("  affine: PASS")


def _test_3d():
    """3D input (batch, seq, hidden)."""
    rng = np.random.default_rng(42)
    x = rng.standard_normal((2, 8, 64)).astype(np.float32)
    gamma = np.ones(64, dtype=np.float32)
    beta = np.zeros(64, dtype=np.float32)

    out = layernorm(x, gamma, beta)
    assert out.shape == x.shape

    # Each (batch, seq) position should have mean≈0, var≈1
    for b in range(2):
        for s in range(8):
            m = np.mean(out[b, s].astype(np.float64))
            v = np.var(out[b, s].astype(np.float64))
            assert abs(m) < 1e-4, f"[{b},{s}] mean={m}"
            assert abs(v - 1.0) < 0.02, f"[{b},{s}] var={v}"

    print("  3d (2,8,64): PASS")


def _test_gradient():
    """Gradient check via finite differences."""
    rng = np.random.default_rng(7)
    x = rng.standard_normal((4, 8)).astype(np.float64)
    gamma = rng.standard_normal(8).astype(np.float64)
    beta = rng.standard_normal(8).astype(np.float64)
    grad_out = rng.standard_normal((4, 8)).astype(np.float64)

    grad_x, grad_gamma, grad_beta = layernorm_backward(x, gamma, beta, grad_out)

    eps_fd = 1e-5
    for i in range(4):
        for j in range(8):
            x_p = x.copy(); x_p[i, j] += eps_fd
            x_m = x.copy(); x_m[i, j] -= eps_fd
            fd = np.sum(grad_out * (layernorm(x_p, gamma, beta) - layernorm(x_m, gamma, beta))) / (2 * eps_fd)
            assert abs(grad_x[i, j] - fd) < 1e-3, \
                f"grad_x[{i},{j}]: analytic={grad_x[i,j]:.6f}, fd={fd:.6f}"

    print("  gradient check: PASS")


def _test_matches_pytorch_formula():
    """Verify against the explicit PyTorch LayerNorm formula."""
    rng = np.random.default_rng(0)
    x = rng.standard_normal((3, 5)).astype(np.float32)
    gamma = rng.uniform(0.5, 2.0, size=5).astype(np.float32)
    beta = rng.standard_normal(5).astype(np.float32)

    out = layernorm(x, gamma, beta, eps=1e-5)

    # Manual per-row computation
    for i in range(3):
        row = x[i].astype(np.float64)
        m = np.mean(row)
        v = np.var(row)
        normed = (row - m) / np.sqrt(v + 1e-5)
        expected = normed * gamma.astype(np.float64) + beta.astype(np.float64)
        assert np.allclose(out[i], expected.astype(np.float32), rtol=1e-4)

    print("  matches formula: PASS")


if __name__ == "__main__":
    print("layernorm reference tests:")
    _test_basic()
    _test_affine()
    _test_3d()
    _test_gradient()
    _test_matches_pytorch_formula()
    print("ALL PASSED")
