# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Python Tensor Bridge
# ===----------------------------------------------------------------------=== #

"""Copy-based tensor exchange between Mojo and Python.

Provides functions to convert between Mojo Tensors and Python lists/numpy arrays.
Uses Mojo's built-in Python interop. True zero-copy DLPack exchange is deferred
to a future sprint when Mojo's UnsafePointer exposure to Python stabilizes.
"""

from python import Python, PythonObject
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn to_python_list(tensor: Tensor[DType.float32], size: Int) raises -> PythonObject:
    """Copy tensor data to a Python list.

    Args:
        tensor: Source Mojo tensor.
        size: Number of elements to copy.

    Returns:
        Python list of floats.
    """
    var builtins = Python.import_module("builtins")
    var py_list = builtins.list()
    for i in range(size):
        py_list.append(Float64(tensor.get(i)))
    return py_list


fn from_python_list(py_list: PythonObject, size: Int) raises -> Tensor[DType.float32]:
    """Copy Python list to a Mojo tensor.

    Args:
        py_list: Source Python list of numbers.
        size: Number of elements to copy.

    Returns:
        Mojo tensor with copied data.
    """
    var t = Tensor[DType.float32](Shape(size))
    for i in range(size):
        var val = py_list[i]
        t.set(i, Float32(py=val))
    return t^


fn call_python(module_name: String, func_name: String, arg: PythonObject) raises -> PythonObject:
    """Call a Python function with a single argument.

    Args:
        module_name: Python module to import.
        func_name: Function name to call.
        arg: Argument to pass.

    Returns:
        Result of the Python function call.
    """
    var mod = Python.import_module(module_name)
    var func = mod.__getattr__(func_name)
    return func(arg)


fn numpy_available() -> Bool:
    """Check if numpy is importable."""
    try:
        _ = Python.import_module("numpy")
        return True
    except:
        return False


fn to_numpy(tensor: Tensor[DType.float32]) raises -> PythonObject:
    """Convert a Mojo tensor to a numpy array (copy-based).

    Args:
        tensor: Source Mojo tensor.

    Returns:
        numpy.ndarray with the tensor data.
    """
    var np = Python.import_module("numpy")
    var n = tensor.numel()
    var py_list = to_python_list(tensor, n)
    var arr = np.array(py_list, dtype=np.float32)
    return arr


fn from_numpy(arr: PythonObject) raises -> Tensor[DType.float32]:
    """Convert a numpy array to a Mojo tensor (copy-based).

    Args:
        arr: Source numpy array.

    Returns:
        Mojo tensor with copied data.
    """
    var n = Int(py=arr.size)
    var flat = arr.flatten()
    var t = Tensor[DType.float32](Shape(n))
    for i in range(n):
        t.set(i, Float32(py=flat[i]))
    return t^


fn to_numpy_shaped(tensor: Tensor[DType.float32], shape: Shape) raises -> PythonObject:
    """Convert a Mojo tensor to a shaped numpy array.

    Args:
        tensor: Source Mojo tensor.
        shape: Desired output shape.

    Returns:
        numpy.ndarray with the given shape.
    """
    var np = Python.import_module("numpy")
    var n = tensor.numel()
    var py_list = to_python_list(tensor, n)
    var arr = np.array(py_list, dtype=np.float32)
    # Build shape tuple
    var builtins = Python.import_module("builtins")
    var shape_list = builtins.list()
    for i in range(shape.ndim()):
        shape_list.append(shape[i])
    var shape_tuple = builtins.tuple(shape_list)
    return arr.reshape(shape_tuple)


fn from_numpy_shaped(arr: PythonObject) raises -> Tensor[DType.float32]:
    """Convert a shaped numpy array to a Mojo tensor, preserving shape info.

    The returned tensor is flat but its numel matches the array's total size.
    Shape information can be extracted from the array's .shape attribute.

    Args:
        arr: Source numpy array (any shape).

    Returns:
        Mojo tensor with data copied from the array.
    """
    var n = Int(py=arr.size)
    var flat = arr.flatten()
    var t = Tensor[DType.float32](Shape(n))
    for i in range(n):
        t.set(i, Float32(py=flat[i]))
    return t^


fn run_python_script(code: String) raises -> PythonObject:
    """Execute a Python script string and return the result namespace.

    The code is executed via exec() in a fresh namespace dict.
    Access results via the returned dict, e.g., result["variable_name"].

    Args:
        code: Python source code to execute.

    Returns:
        PythonObject dict containing the namespace after execution.
    """
    var builtins = Python.import_module("builtins")
    var namespace = builtins.dict()
    builtins.exec(code, namespace)
    return namespace


fn torch_available() -> Bool:
    """Check if PyTorch is importable."""
    try:
        _ = Python.import_module("torch")
        return True
    except:
        return False


fn to_pytorch(tensor: Tensor[DType.float32], shape: Shape) raises -> PythonObject:
    """Convert a Mojo tensor to a PyTorch tensor via numpy intermediate.

    Args:
        tensor: Source Mojo tensor.
        shape: Desired shape for the PyTorch tensor.

    Returns:
        torch.Tensor with the given shape.
    """
    var torch = Python.import_module("torch")
    var np_arr = to_numpy_shaped(tensor, shape)
    return torch.from_numpy(np_arr).clone()


fn from_pytorch(pt_tensor: PythonObject) raises -> Tensor[DType.float32]:
    """Convert a PyTorch tensor to a Mojo tensor via numpy intermediate.

    Args:
        pt_tensor: Source PyTorch tensor.

    Returns:
        Mojo tensor with data copied from the PyTorch tensor.
    """
    var np_arr = pt_tensor.detach().cpu().numpy()
    return from_numpy_shaped(np_arr)
