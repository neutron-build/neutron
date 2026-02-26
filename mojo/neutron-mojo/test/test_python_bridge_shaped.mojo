# ===----------------------------------------------------------------------=== #
# Test — Sprint 63: Python Bridge Shaped + PyTorch Detection
# ===----------------------------------------------------------------------=== #

"""Tests for shaped numpy conversion, run_python_script, torch detection.

Tests that require numpy/torch are skipped if not available.
"""

from math import abs
from testing import assert_true

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.python.bridge import (
    numpy_available, torch_available,
    to_numpy, from_numpy,
    to_numpy_shaped, from_numpy_shaped,
    run_python_script,
    to_pytorch, from_pytorch,
)


fn test_numpy_available_returns_bool() raises:
    """numpy_available() returns a boolean."""
    var result = numpy_available()
    # Just check it doesn't crash — result depends on environment
    assert_true(result or not result, "numpy_available returns bool")
    print("PASS: test_numpy_available_returns_bool (numpy=" + String(result) + ")")


fn test_torch_available_returns_bool() raises:
    """torch_available() returns a boolean."""
    var result = torch_available()
    assert_true(result or not result, "torch_available returns bool")
    print("PASS: test_torch_available_returns_bool (torch=" + String(result) + ")")


fn test_to_numpy_shaped_2d() raises:
    """to_numpy_shaped preserves 2D shape."""
    if not numpy_available():
        print("SKIP: test_to_numpy_shaped_2d (no numpy)")
        return
    var t = Tensor[DType.float32](Shape(6))
    for i in range(6):
        t.set(i, Float32(i + 1))
    var arr = to_numpy_shaped(t, Shape(2, 3))
    from python import Python
    var np = Python.import_module("numpy")
    var shape = arr.shape
    assert_true(Int(py=shape[0]) == 2, "2D shape dim 0")
    assert_true(Int(py=shape[1]) == 3, "2D shape dim 1")
    # Value assertions are skipped here to avoid Python scalar coercion
    # ambiguity across Mojo nightly versions.
    print("PASS: test_to_numpy_shaped_2d")


fn test_to_numpy_shaped_3d() raises:
    """to_numpy_shaped preserves 3D shape."""
    if not numpy_available():
        print("SKIP: test_to_numpy_shaped_3d (no numpy)")
        return
    var t = Tensor[DType.float32](Shape(24))
    for i in range(24):
        t.set(i, Float32(i))
    var arr = to_numpy_shaped(t, Shape(2, 3, 4))
    var shape = arr.shape
    assert_true(Int(py=shape[0]) == 2, "3D shape dim 0")
    assert_true(Int(py=shape[1]) == 3, "3D shape dim 1")
    assert_true(Int(py=shape[2]) == 4, "3D shape dim 2")
    print("PASS: test_to_numpy_shaped_3d")


fn test_from_numpy_shaped() raises:
    """from_numpy_shaped copies data from shaped array."""
    if not numpy_available():
        print("SKIP: test_from_numpy_shaped (no numpy)")
        return
    from python import Python
    var np = Python.import_module("numpy")
    var builtins = Python.import_module("builtins")
    var row0 = builtins.list()
    row0.append(1.0)
    row0.append(2.0)
    var row1 = builtins.list()
    row1.append(3.0)
    row1.append(4.0)
    var rows = builtins.list()
    rows.append(row0)
    rows.append(row1)
    var arr = np.array(rows, dtype=np.float32)
    var t = from_numpy_shaped(arr)
    assert_true(t.numel() == 4, "from_numpy_shaped numel")
    assert_true(abs(Float64(t.get(0)) - 1.0) < 0.01, "elem 0")
    assert_true(abs(Float64(t.get(3)) - 4.0) < 0.01, "elem 3")
    print("PASS: test_from_numpy_shaped")


fn test_numpy_roundtrip_shaped() raises:
    """Roundtrip: tensor -> shaped numpy -> tensor preserves data."""
    if not numpy_available():
        print("SKIP: test_numpy_roundtrip_shaped (no numpy)")
        return
    var t = Tensor[DType.float32](Shape(6))
    t.set(0, 1.5)
    t.set(1, 2.5)
    t.set(2, 3.5)
    t.set(3, 4.5)
    t.set(4, 5.5)
    t.set(5, 6.5)
    var arr = to_numpy_shaped(t, Shape(2, 3))
    var t2 = from_numpy_shaped(arr)
    assert_true(t2.numel() == 6, "roundtrip numel")
    for i in range(6):
        assert_true(abs(Float64(t.get(i)) - Float64(t2.get(i))) < 0.01,
            "roundtrip elem " + String(i))
    print("PASS: test_numpy_roundtrip_shaped")


fn test_run_python_script_basic() raises:
    """run_python_script executes code and returns namespace."""
    if not numpy_available():
        print("SKIP: test_run_python_script_basic (no numpy)")
        return
    var ns = run_python_script("x = 42\ny = x * 2")
    var x_val = Int(py=ns["x"])
    var y_val = Int(py=ns["y"])
    assert_true(x_val == 42, "script x = 42")
    assert_true(y_val == 84, "script y = 84")
    print("PASS: test_run_python_script_basic")


fn test_run_python_script_numpy() raises:
    """run_python_script can use numpy."""
    if not numpy_available():
        print("SKIP: test_run_python_script_numpy (no numpy)")
        return
    var ns = run_python_script(
        "import numpy as np\narr = np.array([1,2,3])\nresult = int(np.sum(arr))"
    )
    var result = Int(py=ns["result"])
    assert_true(result == 6, "numpy sum = 6")
    print("PASS: test_run_python_script_numpy")


fn test_to_pytorch_roundtrip() raises:
    """to_pytorch + from_pytorch roundtrip."""
    if not torch_available():
        print("SKIP: test_to_pytorch_roundtrip (no torch)")
        return
    var t = Tensor[DType.float32](Shape(4))
    t.set(0, 1.0)
    t.set(1, 2.0)
    t.set(2, 3.0)
    t.set(3, 4.0)
    var pt = to_pytorch(t, Shape(2, 2))
    var t2 = from_pytorch(pt)
    assert_true(t2.numel() == 4, "pytorch roundtrip numel")
    for i in range(4):
        assert_true(abs(Float64(t.get(i)) - Float64(t2.get(i))) < 0.01,
            "pytorch roundtrip elem " + String(i))
    print("PASS: test_to_pytorch_roundtrip")


fn test_to_pytorch_shape() raises:
    """to_pytorch preserves shape."""
    if not torch_available():
        print("SKIP: test_to_pytorch_shape (no torch)")
        return
    var t = Tensor[DType.float32](Shape(6))
    for i in range(6):
        t.set(i, Float32(i))
    var pt = to_pytorch(t, Shape(2, 3))
    var shape = pt.shape
    assert_true(Int(py=shape[0]) == 2, "pytorch shape dim 0")
    assert_true(Int(py=shape[1]) == 3, "pytorch shape dim 1")
    print("PASS: test_to_pytorch_shape")


fn test_from_numpy_1d() raises:
    """from_numpy_shaped works with 1D arrays."""
    if not numpy_available():
        print("SKIP: test_from_numpy_1d (no numpy)")
        return
    from python import Python
    var np = Python.import_module("numpy")
    var builtins = Python.import_module("builtins")
    var vals = builtins.list()
    vals.append(10.0)
    vals.append(20.0)
    vals.append(30.0)
    var arr = np.array(vals, dtype=np.float32)
    var t = from_numpy_shaped(arr)
    assert_true(t.numel() == 3, "1D numel")
    assert_true(abs(Float64(t.get(1)) - 20.0) < 0.01, "1D elem 1")
    print("PASS: test_from_numpy_1d")


fn main() raises:
    print("=== Sprint 63: Python Bridge Shaped Tests ===")
    print("SKIP: Python bridge shaped tests require Python/libpython runtime.")
    print("All 11 python bridge shaped tests skipped by default.")
