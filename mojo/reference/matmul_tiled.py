"""Tiled matrix multiplication — reference for Mojo tiled kernel.

Demonstrates the tiling pattern that maps to GPU blocks:
  - Outer loops iterate over tiles (blocks)
  - Inner loops iterate within a tile
  - Accumulator is tile-sized

This is the CPU reference for what @kernel + tiles[] does on GPU.

Tolerances: FP32 1e-6, FP16 1e-3
"""

import numpy as np


def matmul_tiled(a: np.ndarray, b: np.ndarray, block_size: int = 64) -> np.ndarray:
    """Tiled matmul with explicit blocking. Matches GPU tile pattern."""
    assert a.ndim == 2 and b.ndim == 2
    m, k = a.shape
    _, n = b.shape
    assert a.shape[1] == b.shape[0]

    c = np.zeros((m, n), dtype=a.dtype)

    for i0 in range(0, m, block_size):
        for j0 in range(0, n, block_size):
            # Tile accumulator
            i1 = min(i0 + block_size, m)
            j1 = min(j0 + block_size, n)

            for k0 in range(0, k, block_size):
                k1 = min(k0 + block_size, k)
                # Inner tile matmul
                a_tile = a[i0:i1, k0:k1]
                b_tile = b[k0:k1, j0:j1]
                c[i0:i1, j0:j1] += a_tile @ b_tile

    return c


def matmul_tiled_manual(a: np.ndarray, b: np.ndarray, block_size: int = 32) -> np.ndarray:
    """Fully manual tiled matmul — no NumPy matmul inside tiles.
    This is the exact pattern a GPU kernel would execute per thread block."""
    assert a.ndim == 2 and b.ndim == 2
    m, k = a.shape
    _, n = b.shape
    assert a.shape[1] == b.shape[0]

    c = np.zeros((m, n), dtype=np.float64)

    for i0 in range(0, m, block_size):
        for j0 in range(0, n, block_size):
            i1 = min(i0 + block_size, m)
            j1 = min(j0 + block_size, n)
            acc = np.zeros((i1 - i0, j1 - j0), dtype=np.float64)

            for k0 in range(0, k, block_size):
                k1 = min(k0 + block_size, k)
                for ii in range(i1 - i0):
                    for jj in range(j1 - j0):
                        for kk in range(k1 - k0):
                            acc[ii, jj] += float(a[i0 + ii, k0 + kk]) * float(b[k0 + kk, j0 + jj])

            c[i0:i1, j0:j1] = acc

    return c


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_basic():
    rng = np.random.default_rng(42)
    a = rng.standard_normal((64, 64)).astype(np.float32)
    b = rng.standard_normal((64, 64)).astype(np.float32)

    ref = np.matmul(a, b)
    c_tiled = matmul_tiled(a, b, block_size=16)
    assert np.allclose(c_tiled, ref, rtol=1e-5, atol=1e-5), "Tiled matmul mismatch"
    print("  basic 64x64 block=16: PASS")


def _test_non_divisible():
    """Matrix size not divisible by block size."""
    rng = np.random.default_rng(7)
    a = rng.standard_normal((100, 70)).astype(np.float32)
    b = rng.standard_normal((70, 90)).astype(np.float32)

    ref = np.matmul(a, b)
    c_tiled = matmul_tiled(a, b, block_size=32)
    assert np.allclose(c_tiled, ref, rtol=1e-5, atol=1e-5), "Non-divisible tiled mismatch"
    print("  non-divisible 100x70 @ 70x90 block=32: PASS")


def _test_manual_vs_numpy():
    """Manual inner loop matches NumPy."""
    rng = np.random.default_rng(99)
    a = rng.standard_normal((32, 32)).astype(np.float32)
    b = rng.standard_normal((32, 32)).astype(np.float32)

    ref = np.matmul(a.astype(np.float64), b.astype(np.float64))
    c_manual = matmul_tiled_manual(a, b, block_size=8)
    assert np.allclose(c_manual, ref, rtol=1e-6, atol=1e-6), "Manual tiled mismatch"
    print("  manual 32x32 block=8: PASS")


def _test_block_sizes():
    """Various block sizes all produce correct results."""
    rng = np.random.default_rng(0)
    a = rng.standard_normal((128, 128)).astype(np.float32)
    b = rng.standard_normal((128, 128)).astype(np.float32)
    ref = np.matmul(a, b)

    for bs in [8, 16, 32, 64, 128]:
        c = matmul_tiled(a, b, block_size=bs)
        assert np.allclose(c, ref, rtol=1e-5, atol=1e-5), f"Block size {bs} failed"

    print("  block sizes [8,16,32,64,128]: PASS")


if __name__ == "__main__":
    print("matmul_tiled reference tests:")
    _test_basic()
    _test_non_divisible()
    _test_manual_vs_numpy()
    _test_block_sizes()
    print("ALL PASSED")
