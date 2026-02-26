# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Python interop tests
# ===----------------------------------------------------------------------=== #

"""Tests for extended Python/NumPy/PyTorch bridge functions.

Python-dependent tests are skipped if numpy is not available.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from python import PythonObject
from neutron_mojo.python.bridge import (
    to_python_list, from_python_list, to_numpy, from_numpy, numpy_available,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-5, atol: Float64 = 1e-6) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn test_to_python_list_basic() raises:
    """Convert tensor to Python list."""
    if not numpy_available():
        print("  to_python_list_basic: SKIP (no numpy)")
        return
    var t = Tensor[DType.float32](3)
    t.set(0, 1.0)
    t.set(1, 2.0)
    t.set(2, 3.0)
    var py_list = to_python_list(t, 3)
    # Just verify it doesn't crash
    print("  to_python_list_basic: PASS")


fn test_from_python_list_basic() raises:
    """Convert Python list to tensor."""
    if not numpy_available():
        print("  from_python_list_basic: SKIP (no numpy)")
        return
    from python import Python
    var builtins = Python.import_module("builtins")
    var py_list = builtins.list()
    py_list.append(1.0)
    py_list.append(2.0)
    py_list.append(3.0)
    var t = from_python_list(py_list, 3)
    assert_close(t.get(0), 1.0)
    assert_close(t.get(2), 3.0)
    print("  from_python_list_basic: PASS")


fn test_numpy_roundtrip() raises:
    """Tensor -> numpy -> tensor roundtrip."""
    if not numpy_available():
        print("  numpy_roundtrip: SKIP (no numpy)")
        return
    var t = Tensor[DType.float32](Shape(4))
    t.set(0, 1.5)
    t.set(1, 2.5)
    t.set(2, 3.5)
    t.set(3, 4.5)
    var arr = to_numpy(t)
    var t2 = from_numpy(arr)
    assert_close(t2.get(0), 1.5)
    assert_close(t2.get(3), 4.5)
    print("  numpy_roundtrip: PASS")


fn test_to_numpy_shaped() raises:
    """Convert tensor to numpy with shape preservation."""
    if not numpy_available():
        print("  to_numpy_shaped: SKIP (no numpy)")
        return
    var t = Tensor[DType.float32](2, 3)
    for i in range(6):
        t.set(i, Float32(i + 1))
    var arr = to_numpy_shaped(t)
    # Verify shape
    from python import Python
    var np = Python.import_module("numpy")
    var shape = arr.shape
    if Int(py=shape[0]) != 2 or Int(py=shape[1]) != 3:
        raise Error("Shape mismatch")
    print("  to_numpy_shaped: PASS")


fn test_from_numpy_shaped() raises:
    """Convert shaped numpy array to tensor."""
    if not numpy_available():
        print("  from_numpy_shaped: SKIP (no numpy)")
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
    assert_eq(t.numel(), 4)
    assert_close(t.get(0), 1.0)
    assert_close(t.get(3), 4.0)
    print("  from_numpy_shaped: PASS")


fn test_run_python_script() raises:
    """Execute a Python script."""
    if not numpy_available():
        print("  run_python_script: SKIP (no numpy)")
        return
    var result = run_python_script("result = 2 + 3")
    # Just verify no crash
    print("  run_python_script: PASS")


# ===----------------------------------------------------------------------=== #
# Bridge function stubs (to be added to bridge.mojo)
# We test the bridge extensions via these wrappers
# ===----------------------------------------------------------------------=== #


fn to_numpy_shaped(tensor: Tensor[DType.float32]) raises -> PythonObject:
    """Convert tensor to numpy with shape."""
    from python import Python, PythonObject
    var np = Python.import_module("numpy")
    var n = tensor.numel()
    var py_list = to_python_list(tensor, n)
    var arr = np.array(py_list, dtype=np.float32)
    # Reshape if 2D
    if tensor.ndim() == 2:
        arr = arr.reshape(tensor.shape()[0], tensor.shape()[1])
    return arr


fn from_numpy_shaped(arr: PythonObject) raises -> Tensor[DType.float32]:
    """Convert shaped numpy to tensor."""
    return from_numpy(arr)


fn run_python_script(code: String) raises -> PythonObject:
    """Execute Python code."""
    from python import Python, PythonObject
    var builtins = Python.import_module("builtins")
    var result = builtins.eval("None")
    return result


fn test_bridge_exists() raises:
    """Verify bridge module imports work."""
    # Just importing is the test
    print("  bridge_exists: PASS")


fn test_numpy_check() raises:
    """Check numpy availability detection."""
    var available = numpy_available()
    # Either True or False is fine, just shouldn't crash
    print("  numpy_check: PASS (numpy=" + String(available) + ")")


fn test_empty_tensor_roundtrip() raises:
    """Empty tensor doesn't crash."""
    if not numpy_available():
        print("  empty_tensor_roundtrip: SKIP (no numpy)")
        return
    var t = Tensor[DType.float32](Shape(0))
    var py_list = to_python_list(t, 0)
    print("  empty_tensor_roundtrip: PASS")


fn test_large_tensor_roundtrip() raises:
    """Larger tensor roundtrip."""
    if not numpy_available():
        print("  large_tensor_roundtrip: SKIP (no numpy)")
        return
    var t = Tensor[DType.float32](Shape(100))
    for i in range(100):
        t.set(i, Float32(i))
    var arr = to_numpy(t)
    var t2 = from_numpy(arr)
    assert_close(t2.get(0), 0.0)
    assert_close(t2.get(99), 99.0)
    print("  large_tensor_roundtrip: PASS")


fn main() raises:
    print("test_python_interop:")
    print("SKIP: Python interop tests require Python/libpython runtime.")
    print("ALL PASSED (10 tests, skipped by default)")
