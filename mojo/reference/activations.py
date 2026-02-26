"""Activation functions — correctness oracles for Mojo kernels.

Implements all activation functions used in modern transformers:
  - GeLU (GPT-2, BERT) — Gaussian Error Linear Unit
  - SiLU / Swish (Llama-3) — Sigmoid Linear Unit
  - ReLU (baseline, rarely used in LLMs)
  - SwiGLU (Llama-3 FFN) — SiLU-gated linear unit

Each includes forward and (where applicable) backward.

Reference:
  - GeLU: Hendrycks & Gimpel, "Gaussian Error Linear Units" (2016)
  - SiLU: Elfwing et al., "Sigmoid-Weighted Linear Units" (2017)
  - SwiGLU: Shazeer, "GLU Variants Improve Transformer" (2020)

Tolerance: FP32 1e-6
"""

import numpy as np
from scipy import special  # for exact erf


def relu(x: np.ndarray) -> np.ndarray:
    return np.maximum(x, 0)


def relu_backward(x: np.ndarray, grad_out: np.ndarray) -> np.ndarray:
    return grad_out * (x > 0).astype(grad_out.dtype)


def gelu_exact(x: np.ndarray) -> np.ndarray:
    """Exact GeLU: x * 0.5 * (1 + erf(x / sqrt(2)))."""
    return x * 0.5 * (1.0 + special.erf(x.astype(np.float64) / np.sqrt(2.0)))


def gelu_approx(x: np.ndarray) -> np.ndarray:
    """Approximate GeLU (tanh approximation, used in PyTorch).
    x * 0.5 * (1 + tanh(sqrt(2/pi) * (x + 0.044715 * x^3)))
    """
    x64 = x.astype(np.float64)
    return (0.5 * x64 * (1.0 + np.tanh(np.sqrt(2.0 / np.pi) * (x64 + 0.044715 * x64 ** 3)))).astype(x.dtype)


def silu(x: np.ndarray) -> np.ndarray:
    """SiLU (Swish): x * sigmoid(x)."""
    x64 = x.astype(np.float64)
    return (x64 / (1.0 + np.exp(-x64))).astype(x.dtype)


def silu_backward(x: np.ndarray, grad_out: np.ndarray) -> np.ndarray:
    """SiLU backward: sigmoid(x) * (1 + x * (1 - sigmoid(x)))."""
    x64 = x.astype(np.float64)
    sig = 1.0 / (1.0 + np.exp(-x64))
    return (grad_out.astype(np.float64) * sig * (1.0 + x64 * (1.0 - sig))).astype(x.dtype)


def swiglu(x: np.ndarray, gate: np.ndarray) -> np.ndarray:
    """SwiGLU: silu(gate) * x. Used in Llama-3 FFN.

    In practice, a linear layer produces [x, gate] = Linear(input),
    then the output is silu(gate) * x.
    """
    return silu(gate) * x.astype(np.float64)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_relu():
    x = np.array([-2, -1, 0, 1, 2], dtype=np.float32)
    assert np.array_equal(relu(x), [0, 0, 0, 1, 2])
    grad = relu_backward(x, np.ones_like(x))
    assert np.array_equal(grad, [0, 0, 0, 1, 1])
    print("  relu: PASS")


def _test_gelu():
    """GeLU properties: gelu(0)=0, gelu(x)→x for large x, gelu(x)→0 for x<<0."""
    assert abs(gelu_exact(np.array([0.0]))[0]) < 1e-10
    assert abs(gelu_exact(np.array([10.0]))[0] - 10.0) < 1e-5
    assert abs(gelu_exact(np.array([-10.0]))[0]) < 1e-5

    # Exact vs approx should be close
    rng = np.random.default_rng(42)
    x = rng.standard_normal(1000).astype(np.float32)
    exact = gelu_exact(x)
    approx = gelu_approx(x)
    assert np.allclose(exact, approx, rtol=1e-3, atol=1e-3), \
        f"GeLU exact vs approx max diff: {np.max(np.abs(exact - approx))}"
    print("  gelu (exact vs approx): PASS")


def _test_silu():
    """SiLU properties: silu(0)=0, monotonically increasing for x>~-0.28."""
    assert abs(silu(np.array([0.0], dtype=np.float32))[0]) < 1e-7

    # Large positive: silu(x) ≈ x
    assert abs(silu(np.array([10.0], dtype=np.float32))[0] - 10.0) < 0.001

    # Large negative: silu(x) ≈ 0
    assert abs(silu(np.array([-10.0], dtype=np.float32))[0]) < 0.001

    print("  silu: PASS")


def _test_silu_gradient():
    """Finite difference check for SiLU backward."""
    rng = np.random.default_rng(7)
    x = rng.standard_normal(100).astype(np.float64)
    grad_out = np.ones_like(x)

    grad_analytic = silu_backward(x, grad_out)

    eps = 1e-5
    grad_fd = (silu(x + eps) - silu(x - eps)) / (2 * eps)

    assert np.allclose(grad_analytic, grad_fd, rtol=1e-4, atol=1e-6), \
        f"SiLU gradient max diff: {np.max(np.abs(grad_analytic - grad_fd))}"
    print("  silu gradient: PASS")


def _test_swiglu():
    """SwiGLU basic properties."""
    x = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    gate = np.array([0.0, 1.0, 2.0], dtype=np.float32)

    out = swiglu(x, gate)
    # silu(0) = 0, so first element should be 0
    assert abs(out[0]) < 1e-6, f"SwiGLU(x, gate=0) should be 0, got {out[0]}"

    # silu(1) ≈ 0.7311, so second element ≈ 2 * 0.7311
    assert abs(out[1] - 2.0 * silu(np.array([1.0], dtype=np.float32))[0]) < 1e-5
    print("  swiglu: PASS")


def _test_dtypes():
    """All activations work with FP16."""
    rng = np.random.default_rng(0)
    x = rng.standard_normal(64).astype(np.float16)

    for name, fn in [("relu", relu), ("gelu", gelu_approx), ("silu", silu)]:
        out = fn(x)
        assert np.all(np.isfinite(out)), f"{name} FP16 produced inf/nan"

    print("  fp16 all activations: PASS")


if __name__ == "__main__":
    print("activations reference tests:")
    _test_relu()
    _test_gelu()
    _test_silu()
    _test_silu_gradient()
    _test_swiglu()
    _test_dtypes()
    print("ALL PASSED")
