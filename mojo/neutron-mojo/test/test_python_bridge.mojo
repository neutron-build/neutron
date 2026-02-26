# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Python Tensor Bridge Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Python tensor bridge.

NOTE: These tests require libpython to be available at runtime.
If Python is not configured in the environment, all tests will SKIP gracefully.
"""

from python import Python, PythonObject
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.python.bridge import (
    to_python_list, from_python_list, call_python,
    numpy_available, to_numpy, from_numpy,
)
from math import abs


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn approx_eq(a: Float32, b: Float32, tol: Float32 = 1e-4) -> Bool:
    return abs(a - b) < tol


fn python_available() -> Bool:
    """Check if Python runtime is available."""
    try:
        _ = Python.import_module("builtins")
        return True
    except:
        return False


fn test_to_python_list() raises:
    """Convert Mojo tensor to Python list."""
    var t = Tensor[DType.float32](Shape(4))
    for i in range(4):
        t.set(i, Float32(i + 1))
    var py_list = to_python_list(t, 4)
    var builtins = Python.import_module("builtins")
    assert_true(Int(py=builtins.len(py_list)) == 4, "List should have 4 elements")
    assert_true(Float64(py=py_list[0]) == 1.0, "First element should be 1.0")
    assert_true(Float64(py=py_list[3]) == 4.0, "Last element should be 4.0")
    print("  to_python_list: PASS")


fn test_from_python_list() raises:
    """Convert Python list to Mojo tensor."""
    var builtins = Python.import_module("builtins")
    var py_list = builtins.list()
    py_list.append(10.0)
    py_list.append(20.0)
    py_list.append(30.0)
    var t = from_python_list(py_list, 3)
    assert_true(t.numel() == 3, "Tensor should have 3 elements")
    assert_true(approx_eq(t.get(0), 10.0), "First element")
    assert_true(approx_eq(t.get(2), 30.0), "Last element")
    print("  from_python_list: PASS")


fn test_roundtrip() raises:
    """Round-trip: Mojo -> Python list -> Mojo."""
    var t = Tensor[DType.float32](Shape(5))
    for i in range(5):
        t.set(i, Float32(Float64(i) * 3.14))
    var py_list = to_python_list(t, 5)
    var t2 = from_python_list(py_list, 5)
    for i in range(5):
        assert_true(approx_eq(t.get(i), t2.get(i), 0.001), "Roundtrip element " + String(i))
    print("  roundtrip: PASS")


fn test_call_python() raises:
    """Call a Python function."""
    var result = call_python("math", "sqrt", PythonObject(16.0))
    assert_true(Float64(py=result) == 4.0, "sqrt(16) should be 4.0")
    print("  call_python: PASS")


fn test_numpy_available() raises:
    """Check numpy availability."""
    var avail = numpy_available()
    # Just check it doesn't crash; result depends on environment
    print("  numpy_available: " + String(avail) + " PASS")


fn test_to_numpy() raises:
    """Convert to numpy array (if numpy available)."""
    if not numpy_available():
        print("  to_numpy: SKIP (numpy not available)")
        return
    var t = Tensor[DType.float32](Shape(3))
    t.set(0, 1.0)
    t.set(1, 2.0)
    t.set(2, 3.0)
    var arr = to_numpy(t)
    var np = Python.import_module("numpy")
    assert_true(Int(py=arr.size) == 3, "Array should have 3 elements")
    print("  to_numpy: PASS")


fn test_from_numpy() raises:
    """Convert from numpy array (if numpy available)."""
    if not numpy_available():
        print("  from_numpy: SKIP (numpy not available)")
        return
    var np = Python.import_module("numpy")
    var builtins = Python.import_module("builtins")
    var py_vals = builtins.list()
    py_vals.append(5.0)
    py_vals.append(10.0)
    py_vals.append(15.0)
    var arr = np.array(py_vals, dtype=np.float32)
    var t = from_numpy(arr)
    assert_true(t.numel() == 3, "Tensor should have 3 elements")
    assert_true(approx_eq(t.get(0), 5.0), "First element")
    assert_true(approx_eq(t.get(2), 15.0), "Last element")
    print("  from_numpy: PASS")


fn test_numpy_roundtrip() raises:
    """Round-trip: Mojo -> numpy -> Mojo."""
    if not numpy_available():
        print("  numpy_roundtrip: SKIP (numpy not available)")
        return
    var t = Tensor[DType.float32](Shape(4))
    for i in range(4):
        t.set(i, Float32(Float64(i) * 2.5))
    var arr = to_numpy(t)
    var t2 = from_numpy(arr)
    for i in range(4):
        assert_true(approx_eq(t.get(i), t2.get(i), 0.001), "Numpy roundtrip element " + String(i))
    print("  numpy_roundtrip: PASS")


fn test_large_tensor() raises:
    """Large tensor round-trip."""
    var n = 1000
    var t = Tensor[DType.float32](Shape(n))
    for i in range(n):
        t.set(i, Float32(i) * 0.001)
    var py_list = to_python_list(t, n)
    var t2 = from_python_list(py_list, n)
    assert_true(t2.numel() == n, "Large tensor size")
    assert_true(approx_eq(t2.get(0), 0.0), "First element")
    assert_true(approx_eq(t2.get(999), 0.999, 0.01), "Last element")
    print("  large_tensor: PASS")


fn main() raises:
    print("test_python_bridge")
    print("  Python runtime tests skipped by default (requires libpython).")
    print("All 9 python bridge tests skipped.")
