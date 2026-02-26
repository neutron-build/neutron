"""Tensor operations — correctness oracle matching Mojo ops.mojo API.

This module provides NumPy reference implementations for every operation
in neutron_mojo/tensor/ops.mojo:
  - Elementwise: add, sub, mul, div (with broadcasting)
  - Matmul: tiled 2D matmul
  - Activations: relu, softmax (1D + 2D)
  - Reductions: reduce_sum, reduce_max (1D + 2D, axis support)

Also generates test vectors as JSON for cross-validation with Mojo tests.

Tolerances: FP32 1e-6
"""

import json
import numpy as np


# ===----------------------------------------------------------------------=== #
# Elementwise ops (with broadcasting)
# ===----------------------------------------------------------------------=== #


def tensor_add(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Elementwise addition with NumPy broadcasting."""
    return a + b


def tensor_sub(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Elementwise subtraction with NumPy broadcasting."""
    return a - b


def tensor_mul(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Elementwise multiplication with NumPy broadcasting."""
    return a * b


def tensor_div(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Elementwise division with NumPy broadcasting."""
    return a / b


# ===----------------------------------------------------------------------=== #
# Matrix multiplication
# ===----------------------------------------------------------------------=== #


def tensor_matmul(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """2D matrix multiplication: C = A @ B."""
    assert a.ndim == 2 and b.ndim == 2
    assert a.shape[1] == b.shape[0]
    return np.matmul(a, b)


# ===----------------------------------------------------------------------=== #
# Activations
# ===----------------------------------------------------------------------=== #


def tensor_relu(x: np.ndarray) -> np.ndarray:
    """ReLU: max(0, x)."""
    return np.maximum(x, 0)


def tensor_softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Numerically stable softmax along axis."""
    x_max = np.max(x, axis=axis, keepdims=True)
    e = np.exp(x - x_max)
    return e / np.sum(e, axis=axis, keepdims=True)


# ===----------------------------------------------------------------------=== #
# Reductions
# ===----------------------------------------------------------------------=== #


def tensor_reduce_sum(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Sum reduction along axis."""
    return np.sum(x, axis=axis, keepdims=True)


def tensor_reduce_max(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Max reduction along axis."""
    return np.max(x, axis=axis, keepdims=True)


# ===----------------------------------------------------------------------=== #
# Test vector generation
# ===----------------------------------------------------------------------=== #


def generate_test_vectors() -> dict:
    """Generate deterministic test vectors for cross-validation with Mojo.

    Returns a dict with named test cases, each containing inputs and
    expected outputs as flat lists.
    """
    rng = np.random.default_rng(42)
    vectors = {}

    # --- Elementwise same-shape ---
    a = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)
    b = np.array([5.0, 6.0, 7.0, 8.0], dtype=np.float32)
    vectors["add_1d"] = {
        "a": a.tolist(), "b": b.tolist(),
        "expected": tensor_add(a, b).tolist(),
    }
    vectors["sub_1d"] = {
        "a": a.tolist(), "b": b.tolist(),
        "expected": tensor_sub(a, b).tolist(),
    }
    vectors["mul_1d"] = {
        "a": a.tolist(), "b": b.tolist(),
        "expected": tensor_mul(a, b).tolist(),
    }
    vectors["div_1d"] = {
        "a": a.tolist(), "b": b.tolist(),
        "expected": tensor_div(a, b).tolist(),
    }

    # --- Elementwise broadcast ---
    a_2d = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
    b_row = np.array([[10, 20, 30]], dtype=np.float32)
    vectors["add_broadcast_2d"] = {
        "a_shape": [2, 3], "b_shape": [1, 3],
        "a": a_2d.flatten().tolist(), "b": b_row.flatten().tolist(),
        "expected": tensor_add(a_2d, b_row).flatten().tolist(),
    }

    # --- Matmul (matches test_ops.mojo test_matmul_basic) ---
    mat_a = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
    mat_b = np.array([[7, 8, 9, 10], [11, 12, 13, 14], [15, 16, 17, 18]], dtype=np.float32)
    vectors["matmul_2x3_3x4"] = {
        "a_shape": [2, 3], "b_shape": [3, 4],
        "a": mat_a.flatten().tolist(),
        "b": mat_b.flatten().tolist(),
        "expected": tensor_matmul(mat_a, mat_b).flatten().tolist(),
    }

    # --- Matmul 64x64 (for benchmarking correctness) ---
    a64 = rng.standard_normal((64, 64)).astype(np.float32)
    b64 = rng.standard_normal((64, 64)).astype(np.float32)
    c64 = tensor_matmul(a64, b64)
    vectors["matmul_64x64"] = {
        "a_shape": [64, 64], "b_shape": [64, 64],
        "a": a64.flatten().tolist(),
        "b": b64.flatten().tolist(),
        "expected": c64.flatten().tolist(),
        "tolerance": 1e-4,
    }

    # --- ReLU ---
    relu_in = np.array([-2, -1, 0, 1, 2], dtype=np.float32)
    vectors["relu"] = {
        "input": relu_in.tolist(),
        "expected": tensor_relu(relu_in).tolist(),
    }

    # --- Softmax 1D ---
    sm_in = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    vectors["softmax_1d"] = {
        "input": sm_in.tolist(),
        "expected": tensor_softmax(sm_in).tolist(),
    }

    # --- Softmax stability ---
    sm_large = np.array([1000.0, 1001.0, 1002.0], dtype=np.float32)
    vectors["softmax_stability"] = {
        "input": sm_large.tolist(),
        "expected": tensor_softmax(sm_large).tolist(),
    }

    # --- Softmax 2D ---
    sm_2d = np.arange(8, dtype=np.float32).reshape(2, 4)
    vectors["softmax_2d"] = {
        "shape": [2, 4],
        "input": sm_2d.flatten().tolist(),
        "expected": tensor_softmax(sm_2d, axis=-1).flatten().tolist(),
    }

    # --- Reduce sum ---
    red_in = np.array([1, 2, 3, 4, 5], dtype=np.float32)
    vectors["reduce_sum_1d"] = {
        "input": red_in.tolist(),
        "expected": [float(np.sum(red_in))],
    }

    red_2d = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
    vectors["reduce_sum_2d_axis1"] = {
        "shape": [2, 3],
        "input": red_2d.flatten().tolist(),
        "expected": np.sum(red_2d, axis=1).tolist(),
    }

    # --- Reduce max ---
    vectors["reduce_max_1d"] = {
        "input": [3, 1, 4, 1, 5],
        "expected": [5.0],
    }

    # --- Broadcasting shape tests ---
    vectors["broadcast_shapes"] = [
        {"a": [3, 1, 5], "b": [1, 4, 5], "expected": [3, 4, 5]},
        {"a": [5], "b": [3, 5], "expected": [3, 5]},
        {"a": [1], "b": [4, 5], "expected": [4, 5]},
        {"a": [2, 1, 3], "b": [4, 3], "expected": [2, 4, 3]},
        {"a": [8, 1, 6, 1], "b": [7, 1, 5], "expected": [8, 7, 6, 5]},
    ]

    return vectors


# ===----------------------------------------------------------------------=== #
# Self-tests
# ===----------------------------------------------------------------------=== #


def _test_elementwise_same_shape():
    a = np.array([1, 2, 3, 4], dtype=np.float32)
    b = np.array([5, 6, 7, 8], dtype=np.float32)
    assert np.array_equal(tensor_add(a, b), [6, 8, 10, 12])
    assert np.array_equal(tensor_sub(a, b), [-4, -4, -4, -4])
    assert np.array_equal(tensor_mul(a, b), [5, 12, 21, 32])
    assert np.allclose(tensor_div(a, b), [0.2, 1/3, 3/7, 0.5])
    print("  elementwise same shape: PASS")


def _test_elementwise_broadcast():
    a = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
    b = np.array([[10, 20, 30]], dtype=np.float32)
    expected = np.array([[11, 22, 33], [14, 25, 36]], dtype=np.float32)
    assert np.array_equal(tensor_add(a, b), expected)
    print("  elementwise broadcast: PASS")


def _test_matmul_known():
    a = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
    b = np.array([[7, 8, 9, 10], [11, 12, 13, 14], [15, 16, 17, 18]], dtype=np.float32)
    expected = np.array([[74, 80, 86, 92], [173, 188, 203, 218]], dtype=np.float32)
    assert np.allclose(tensor_matmul(a, b), expected)
    print("  matmul known values: PASS")


def _test_relu():
    x = np.array([-2, -1, 0, 1, 2], dtype=np.float32)
    assert np.array_equal(tensor_relu(x), [0, 0, 0, 1, 2])
    print("  relu: PASS")


def _test_softmax_basic():
    x = np.array([1, 2, 3], dtype=np.float32)
    s = tensor_softmax(x)
    assert np.allclose(np.sum(s), 1.0, atol=1e-6)
    assert s[2] > s[1] > s[0]
    print("  softmax basic: PASS")


def _test_softmax_stability():
    x = np.array([1000, 1001, 1002], dtype=np.float32)
    s = tensor_softmax(x)
    assert np.all(np.isfinite(s))
    assert np.allclose(np.sum(s), 1.0, atol=1e-6)
    print("  softmax stability: PASS")


def _test_softmax_2d():
    x = np.arange(8, dtype=np.float32).reshape(2, 4)
    s = tensor_softmax(x, axis=-1)
    for row in range(2):
        assert np.allclose(np.sum(s[row]), 1.0, atol=1e-6)
    print("  softmax 2d: PASS")


def _test_reductions():
    x = np.array([1, 2, 3, 4, 5], dtype=np.float32)
    assert np.allclose(tensor_reduce_sum(x), [15.0])
    assert np.allclose(tensor_reduce_max(x), [5.0])

    x2d = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
    assert np.allclose(tensor_reduce_sum(x2d, axis=1), [[6], [15]])
    assert np.allclose(tensor_reduce_max(x2d, axis=1), [[3], [6]])
    print("  reductions: PASS")


def _test_broadcast_shapes():
    """Validate broadcast shape computation matches NumPy."""
    cases = [
        ((3, 1, 5), (1, 4, 5), (3, 4, 5)),
        ((5,), (3, 5), (3, 5)),
        ((1,), (4, 5), (4, 5)),
        ((2, 1, 3), (4, 3), (2, 4, 3)),
        ((8, 1, 6, 1), (7, 1, 5), (8, 7, 6, 5)),
    ]
    for a_shape, b_shape, expected in cases:
        a = np.zeros(a_shape)
        b = np.zeros(b_shape)
        result = np.broadcast_shapes(a.shape, b.shape)
        assert result == expected, f"{a_shape} x {b_shape}: got {result}, expected {expected}"
    print("  broadcast shapes: PASS")


def _test_generate_vectors():
    """Verify test vector generation doesn't crash and has expected keys."""
    vectors = generate_test_vectors()
    assert "matmul_2x3_3x4" in vectors
    assert "softmax_stability" in vectors
    assert "broadcast_shapes" in vectors
    assert len(vectors["matmul_2x3_3x4"]["expected"]) == 8  # 2x4
    print("  generate vectors: PASS")


if __name__ == "__main__":
    print("tensor_ops reference tests:")
    _test_elementwise_same_shape()
    _test_elementwise_broadcast()
    _test_matmul_known()
    _test_relu()
    _test_softmax_basic()
    _test_softmax_stability()
    _test_softmax_2d()
    _test_reductions()
    _test_broadcast_shapes()
    _test_generate_vectors()
    print("ALL PASSED")

    # Optionally dump test vectors to JSON
    if "--dump" in __import__("sys").argv:
        vectors = generate_test_vectors()
        with open("test_vectors.json", "w") as f:
            json.dump(vectors, f, indent=2)
        print(f"\nWrote test_vectors.json ({len(vectors)} test cases)")
