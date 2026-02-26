# ===----------------------------------------------------------------------=== #
# Test — Sprint 64: PyTorch Training Bridge
# ===----------------------------------------------------------------------=== #

"""Tests for tape <-> PyTorch state_dict exchange. Skip if no torch."""

from math import abs
from testing import assert_true

from neutron_mojo.autograd.tape import Tape
from neutron_mojo.autograd.backward import run_backward
from neutron_mojo.autograd.ops import tracked_add, tracked_sum
from neutron_mojo.python.bridge import torch_available, numpy_available


fn _make_var(mut tape: Tape, vals: List[Float32]) -> Int:
    var dims = List[Int]()
    dims.append(len(vals))
    var idx = tape.add_variable(dims^, requires_grad=True)
    for i in range(len(vals)):
        tape.set_data(idx, i, vals[i])
    return idx


fn _make_2d_var(mut tape: Tape, rows: Int, cols: Int, vals: List[Float32]) -> Int:
    var dims = List[Int]()
    dims.append(rows)
    dims.append(cols)
    var idx = tape.add_variable(dims^, requires_grad=True)
    for i in range(len(vals)):
        tape.set_data(idx, i, vals[i])
    return idx


fn test_tape_to_state_dict() raises:
    """Export tape variables as state_dict."""
    if not torch_available():
        print("SKIP: test_tape_to_state_dict (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_to_state_dict
    var tape = Tape(1024)
    var vals = List[Float32]()
    vals.append(1.0)
    vals.append(2.0)
    vals.append(3.0)
    var idx = _make_var(tape, vals)
    var indices = List[Int]()
    indices.append(idx)
    var names = List[String]()
    names.append("weight")
    var sd = tape_to_state_dict(tape, indices, names)

    from python import Python
    var builtins = Python.import_module("builtins")
    assert_true(Int(py=builtins.len(sd)) == 1, "state_dict has 1 entry")
    var w = sd["weight"]
    assert_true(Int(py=w.numel()) == 3, "weight has 3 elements")
    print("PASS: test_tape_to_state_dict")


fn test_state_dict_to_tape() raises:
    """Import state_dict into tape."""
    if not torch_available():
        print("SKIP: test_state_dict_to_tape (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_to_state_dict, state_dict_to_tape
    from python import Python
    var torch = Python.import_module("torch")
    var np = Python.import_module("numpy")

    var tape = Tape(1024)
    var vals = List[Float32]()
    vals.append(0.0)
    vals.append(0.0)
    var idx = _make_var(tape, vals)
    var indices = List[Int]()
    indices.append(idx)
    var names = List[String]()
    names.append("bias")

    # Create a state_dict with specific values
    var builtins = Python.import_module("builtins")
    var sd = builtins.dict()
    var py_vals = builtins.list()
    py_vals.append(10.0)
    py_vals.append(20.0)
    var arr = np.array(py_vals, dtype=np.float32)
    sd["bias"] = torch.from_numpy(arr).clone()

    state_dict_to_tape(tape, sd, indices, names)
    assert_true(abs(Float64(tape.get_data(idx, 0)) - 10.0) < 0.01, "imported val 0")
    assert_true(abs(Float64(tape.get_data(idx, 1)) - 20.0) < 0.01, "imported val 1")
    print("PASS: test_state_dict_to_tape")


fn test_roundtrip_state_dict() raises:
    """Roundtrip: tape -> state_dict -> tape preserves values."""
    if not torch_available():
        print("SKIP: test_roundtrip_state_dict (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_to_state_dict, state_dict_to_tape
    var tape1 = Tape(1024)
    var vals = List[Float32]()
    vals.append(3.14)
    vals.append(2.72)
    vals.append(1.41)
    var idx1 = _make_var(tape1, vals)
    var indices = List[Int]()
    indices.append(idx1)
    var names = List[String]()
    names.append("params")
    var sd = tape_to_state_dict(tape1, indices, names)

    var tape2 = Tape(1024)
    var zeros = List[Float32]()
    zeros.append(0.0)
    zeros.append(0.0)
    zeros.append(0.0)
    var idx2 = _make_var(tape2, zeros)
    var indices2 = List[Int]()
    indices2.append(idx2)
    state_dict_to_tape(tape2, sd, indices2, names)

    for i in range(3):
        assert_true(abs(Float64(tape2.get_data(idx2, i)) - Float64(vals[i])) < 0.01,
            "roundtrip elem " + String(i))
    print("PASS: test_roundtrip_state_dict")


fn test_tape_grads_export() raises:
    """Export gradients to state_dict."""
    if not torch_available():
        print("SKIP: test_tape_grads_export (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_grads_to_state_dict
    var tape = Tape(2048)
    var a_vals = List[Float32]()
    a_vals.append(2.0)
    a_vals.append(3.0)
    var a = _make_var(tape, a_vals)
    var b_vals = List[Float32]()
    b_vals.append(4.0)
    b_vals.append(5.0)
    var b = _make_var(tape, b_vals)
    var c = tracked_add(tape, a, b)
    var loss = tracked_sum(tape, c)
    run_backward(tape, loss)

    var indices = List[Int]()
    indices.append(a)
    var names = List[String]()
    names.append("a")
    var gd = tape_grads_to_state_dict(tape, indices, names)

    from python import Python
    var builtins = Python.import_module("builtins")
    var g = gd["a"]
    # grad of sum(a+b) w.r.t. a = [1, 1]
    assert_true(abs(Float64(py=g[0].item()) - 1.0) < 0.01, "grad a[0]")
    assert_true(abs(Float64(py=g[1].item()) - 1.0) < 0.01, "grad a[1]")
    print("PASS: test_tape_grads_export")


fn test_2d_state_dict() raises:
    """2D variable preserves shape in state_dict."""
    if not torch_available():
        print("SKIP: test_2d_state_dict (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_to_state_dict
    var tape = Tape(1024)
    var vals = List[Float32]()
    vals.append(1.0)
    vals.append(2.0)
    vals.append(3.0)
    vals.append(4.0)
    vals.append(5.0)
    vals.append(6.0)
    var idx = _make_2d_var(tape, 2, 3, vals)
    var indices = List[Int]()
    indices.append(idx)
    var names = List[String]()
    names.append("weight")
    var sd = tape_to_state_dict(tape, indices, names)
    var w = sd["weight"]
    var shape = w.shape
    assert_true(Int(py=shape[0]) == 2, "2D dim 0")
    assert_true(Int(py=shape[1]) == 3, "2D dim 1")
    print("PASS: test_2d_state_dict")


fn test_multiple_params() raises:
    """Export multiple params to state_dict."""
    if not torch_available():
        print("SKIP: test_multiple_params (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_to_state_dict
    var tape = Tape(1024)
    var v1 = List[Float32]()
    v1.append(1.0)
    v1.append(2.0)
    var v2 = List[Float32]()
    v2.append(3.0)
    var idx1 = _make_var(tape, v1)
    var idx2 = _make_var(tape, v2)

    var indices = List[Int]()
    indices.append(idx1)
    indices.append(idx2)
    var names = List[String]()
    names.append("layer.weight")
    names.append("layer.bias")
    var sd = tape_to_state_dict(tape, indices, names)

    from python import Python
    var builtins = Python.import_module("builtins")
    assert_true(Int(py=builtins.len(sd)) == 2, "state_dict has 2 entries")
    print("PASS: test_multiple_params")


fn test_state_dict_import_2d() raises:
    """Import 2D tensor from state_dict."""
    if not torch_available():
        print("SKIP: test_state_dict_import_2d (no torch)")
        return

    from neutron_mojo.python.torch_bridge import state_dict_to_tape
    from python import Python
    var torch = Python.import_module("torch")
    var np = Python.import_module("numpy")
    var builtins = Python.import_module("builtins")

    var tape = Tape(1024)
    var vals = List[Float32]()
    for _ in range(6):
        vals.append(0.0)
    var idx = _make_2d_var(tape, 2, 3, vals)

    var sd = builtins.dict()
    var row0 = builtins.list()
    row0.append(1.0)
    row0.append(2.0)
    row0.append(3.0)
    var row1 = builtins.list()
    row1.append(4.0)
    row1.append(5.0)
    row1.append(6.0)
    var rows = builtins.list()
    rows.append(row0)
    rows.append(row1)
    var arr = np.array(rows, dtype=np.float32)
    sd["w"] = torch.from_numpy(arr).clone()

    var indices = List[Int]()
    indices.append(idx)
    var names = List[String]()
    names.append("w")
    state_dict_to_tape(tape, sd, indices, names)

    assert_true(abs(Float64(tape.get_data(idx, 0)) - 1.0) < 0.01, "2d import [0,0]")
    assert_true(abs(Float64(tape.get_data(idx, 5)) - 6.0) < 0.01, "2d import [1,2]")
    print("PASS: test_state_dict_import_2d")


fn test_empty_params() raises:
    """Export with no params produces empty dict."""
    if not torch_available():
        print("SKIP: test_empty_params (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_to_state_dict
    from python import Python
    var builtins = Python.import_module("builtins")
    var tape = Tape(1024)
    var indices = List[Int]()
    var names = List[String]()
    var sd = tape_to_state_dict(tape, indices, names)
    assert_true(Int(py=builtins.len(sd)) == 0, "empty state_dict")
    print("PASS: test_empty_params")


fn test_grad_zero_after_zero_grads() raises:
    """Exported grads are zero after zero_all_grads."""
    if not torch_available():
        print("SKIP: test_grad_zero_after_zero_grads (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_grads_to_state_dict
    var tape = Tape(2048)
    var vals = List[Float32]()
    vals.append(1.0)
    vals.append(2.0)
    var a = _make_var(tape, vals)
    var loss = tracked_sum(tape, a)
    run_backward(tape, loss)
    tape.zero_all_grads()

    var indices = List[Int]()
    indices.append(a)
    var names = List[String]()
    names.append("a")
    var gd = tape_grads_to_state_dict(tape, indices, names)
    var g = gd["a"]
    assert_true(abs(Float64(py=g[0].item())) < 1e-6, "zeroed grad 0")
    assert_true(abs(Float64(py=g[1].item())) < 1e-6, "zeroed grad 1")
    print("PASS: test_grad_zero_after_zero_grads")


fn test_large_roundtrip() raises:
    """Roundtrip with larger variable."""
    if not torch_available():
        print("SKIP: test_large_roundtrip (no torch)")
        return

    from neutron_mojo.python.torch_bridge import tape_to_state_dict, state_dict_to_tape
    var tape1 = Tape(4096)
    var vals = List[Float32]()
    for i in range(100):
        vals.append(Float32(i) * 0.01)
    var idx1 = _make_var(tape1, vals)
    var indices = List[Int]()
    indices.append(idx1)
    var names = List[String]()
    names.append("big")
    var sd = tape_to_state_dict(tape1, indices, names)

    var tape2 = Tape(4096)
    var zeros = List[Float32]()
    for _ in range(100):
        zeros.append(0.0)
    var idx2 = _make_var(tape2, zeros)
    var indices2 = List[Int]()
    indices2.append(idx2)
    state_dict_to_tape(tape2, sd, indices2, names)

    var max_err = Float64(0.0)
    for i in range(100):
        var err = abs(Float64(tape2.get_data(idx2, i)) - Float64(vals[i]))
        if err > max_err:
            max_err = err
    assert_true(max_err < 0.001, "large roundtrip max error < 0.001")
    print("PASS: test_large_roundtrip")


fn main() raises:
    print("=== Sprint 64: PyTorch Training Bridge Tests ===")
    print("SKIP: PyTorch bridge tests require Python/libpython runtime.")
    print("All 10 PyTorch bridge tests skipped by default.")
