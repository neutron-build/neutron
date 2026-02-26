"""Naive matrix multiplication — correctness oracle for Mojo kernels.

Three implementations:
  1. Triple-loop (pure Python, reference of references)
  2. NumPy (fast, used for large-scale validation)
  3. Batched matmul (NumPy, for transformer workloads)

Tolerances:
  FP32: 1e-6 relative
  FP16: 1e-3 relative
"""

import numpy as np


def matmul_naive(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Triple-loop matmul. O(MNK). Only for small sizes."""
    assert a.ndim == 2 and b.ndim == 2
    assert a.shape[1] == b.shape[0], f"Inner dims mismatch: {a.shape} @ {b.shape}"
    m, k = a.shape
    _, n = b.shape
    c = np.zeros((m, n), dtype=np.float64)
    for i in range(m):
        for j in range(n):
            acc = 0.0
            for p in range(k):
                acc += float(a[i, p]) * float(b[p, j])
            c[i, j] = acc
    return c


def matmul_numpy(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """NumPy matmul. Supports 2D and batched."""
    return np.matmul(a, b)


def matmul_batched(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Batched matmul for >=3D tensors (transformer-style)."""
    assert a.ndim >= 2 and b.ndim >= 2
    return np.matmul(a, b)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_basic():
    """2x3 @ 3x4 = 2x4."""
    a = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
    b = np.array([[7, 8, 9, 10], [11, 12, 13, 14], [15, 16, 17, 18]], dtype=np.float32)
    expected = np.array([[74, 80, 86, 92], [173, 188, 203, 218]], dtype=np.float32)

    # Naive
    c_naive = matmul_naive(a, b)
    assert np.allclose(c_naive, expected, atol=1e-6), f"Naive failed: {c_naive}"

    # NumPy
    c_np = matmul_numpy(a, b)
    assert np.allclose(c_np, expected, atol=1e-6), f"NumPy failed: {c_np}"

    print("  basic: PASS")


def _test_square():
    """Random 64x64 square matmul, naive vs NumPy."""
    rng = np.random.default_rng(42)
    a = rng.standard_normal((64, 64)).astype(np.float32)
    b = rng.standard_normal((64, 64)).astype(np.float32)

    c_naive = matmul_naive(a, b)
    c_np = matmul_numpy(a, b)

    assert np.allclose(c_naive, c_np, rtol=1e-5, atol=1e-5), "Square matmul mismatch"
    print("  square 64x64: PASS")


def _test_rectangular():
    """Non-square: 128x64 @ 64x256."""
    rng = np.random.default_rng(123)
    a = rng.standard_normal((128, 64)).astype(np.float32)
    b = rng.standard_normal((64, 256)).astype(np.float32)

    c_naive = matmul_naive(a, b)
    c_np = matmul_numpy(a, b)

    assert np.allclose(c_naive, c_np, rtol=1e-5, atol=1e-5), "Rectangular matmul mismatch"
    print("  rectangular 128x64 @ 64x256: PASS")


def _test_fp16():
    """FP16 matmul with relaxed tolerance."""
    rng = np.random.default_rng(7)
    a = rng.standard_normal((32, 32)).astype(np.float16)
    b = rng.standard_normal((32, 32)).astype(np.float16)

    c_np = matmul_numpy(a, b)
    c_ref = matmul_numpy(a.astype(np.float32), b.astype(np.float32))

    assert np.allclose(c_np.astype(np.float32), c_ref, rtol=1e-2, atol=1e-2), "FP16 matmul mismatch"
    print("  fp16 32x32: PASS")


def _test_batched():
    """Batched matmul: (2, 3, 4, 5) @ (2, 3, 5, 6) = (2, 3, 4, 6)."""
    rng = np.random.default_rng(99)
    a = rng.standard_normal((2, 3, 4, 5)).astype(np.float32)
    b = rng.standard_normal((2, 3, 5, 6)).astype(np.float32)

    c = matmul_batched(a, b)
    assert c.shape == (2, 3, 4, 6), f"Bad shape: {c.shape}"

    # Verify against manual loop over batch dims
    for i in range(2):
        for j in range(3):
            expected = matmul_numpy(a[i, j], b[i, j])
            assert np.allclose(c[i, j], expected, rtol=1e-5), f"Batch [{i},{j}] mismatch"

    print("  batched (2,3,4,5)@(2,3,5,6): PASS")


def _test_identity():
    """A @ I = A."""
    rng = np.random.default_rng(0)
    a = rng.standard_normal((16, 16)).astype(np.float32)
    eye = np.eye(16, dtype=np.float32)

    c = matmul_numpy(a, eye)
    assert np.allclose(c, a, atol=1e-7), "Identity matmul failed"
    print("  identity: PASS")


if __name__ == "__main__":
    print("matmul reference tests:")
    _test_basic()
    _test_square()
    _test_rectangular()
    _test_fp16()
    _test_batched()
    _test_identity()
    print("ALL PASSED")
