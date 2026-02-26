# ===----------------------------------------------------------------------=== #
# Neutron Mojo — PyTorch Training Bridge
# ===----------------------------------------------------------------------=== #

"""Weight and gradient exchange between autograd Tape and PyTorch state_dict.

Enables:
- Exporting tape weights as a PyTorch state_dict
- Importing PyTorch state_dict back into tape
- Exporting gradients for PyTorch optimizers
- Save/load PyTorch checkpoints via torch.save/load
"""

from python import Python, PythonObject
from neutron_mojo.autograd.tape import Tape
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.python.bridge import to_numpy_shaped, from_numpy_shaped


fn tape_to_state_dict(
    tape: Tape,
    param_indices: List[Int],
    param_names: List[String],
) raises -> PythonObject:
    """Export tape variable data as a PyTorch-compatible state_dict.

    Args:
        tape: Autograd tape containing the variables.
        param_indices: Tape variable indices to export.
        param_names: Corresponding names for each parameter.

    Returns:
        Python dict mapping name -> torch.Tensor.
    """
    var torch = Python.import_module("torch")
    var np = Python.import_module("numpy")
    var builtins = Python.import_module("builtins")
    var sd = builtins.dict()

    var n = len(param_indices)
    for i in range(n):
        var idx = param_indices[i]
        var name = param_names[i]
        var numel = tape.var_numel(idx)

        # Build numpy array from tape data
        var py_list = builtins.list()
        for j in range(numel):
            py_list.append(Float64(tape.get_data(idx, j)))
        var arr = np.array(py_list, dtype=np.float32)

        # Reshape to variable shape
        var shape = tape.var_shapes[idx].copy()
        if len(shape) > 1:
            var shape_list = builtins.list()
            for d in range(len(shape)):
                shape_list.append(shape[d])
            arr = arr.reshape(builtins.tuple(shape_list))

        sd[name] = torch.from_numpy(arr).clone()

    return sd


fn state_dict_to_tape(
    mut tape: Tape,
    state_dict: PythonObject,
    param_indices: List[Int],
    param_names: List[String],
) raises:
    """Import a PyTorch state_dict into tape variables.

    Args:
        tape: Target autograd tape.
        state_dict: Python dict mapping name -> torch.Tensor.
        param_indices: Tape variable indices to populate.
        param_names: Corresponding names for each parameter.
    """
    var n = len(param_indices)
    for i in range(n):
        var idx = param_indices[i]
        var name = param_names[i]
        var pt_tensor = state_dict[name]
        var np_arr = pt_tensor.detach().cpu().numpy().flatten()
        var numel = tape.var_numel(idx)
        for j in range(numel):
            tape.set_data(idx, j, Float32(py=np_arr[j]))


fn tape_grads_to_state_dict(
    tape: Tape,
    param_indices: List[Int],
    param_names: List[String],
) raises -> PythonObject:
    """Export tape gradients as a PyTorch-compatible dict.

    Args:
        tape: Autograd tape with computed gradients.
        param_indices: Tape variable indices.
        param_names: Corresponding names.

    Returns:
        Python dict mapping name -> torch.Tensor of gradients.
    """
    var torch = Python.import_module("torch")
    var np = Python.import_module("numpy")
    var builtins = Python.import_module("builtins")
    var gd = builtins.dict()

    var n = len(param_indices)
    for i in range(n):
        var idx = param_indices[i]
        var name = param_names[i]
        var numel = tape.var_numel(idx)

        var py_list = builtins.list()
        for j in range(numel):
            py_list.append(Float64(tape.get_grad(idx, j)))
        var arr = np.array(py_list, dtype=np.float32)

        var shape = tape.var_shapes[idx].copy()
        if len(shape) > 1:
            var shape_list = builtins.list()
            for d in range(len(shape)):
                shape_list.append(shape[d])
            arr = arr.reshape(builtins.tuple(shape_list))

        gd[name] = torch.from_numpy(arr).clone()

    return gd


fn save_pytorch_checkpoint(
    tape: Tape,
    param_indices: List[Int],
    param_names: List[String],
    path: String,
) raises:
    """Save tape weights as a PyTorch checkpoint file.

    Args:
        tape: Autograd tape containing trained weights.
        param_indices: Tape variable indices to save.
        param_names: Corresponding parameter names.
        path: File path to save to (e.g., "model.pt").
    """
    var torch = Python.import_module("torch")
    var sd = tape_to_state_dict(tape, param_indices, param_names)
    torch.save(sd, path)


fn load_pytorch_checkpoint(
    mut tape: Tape,
    param_indices: List[Int],
    param_names: List[String],
    path: String,
) raises:
    """Load a PyTorch checkpoint into tape variables.

    Args:
        tape: Target autograd tape.
        param_indices: Tape variable indices to populate.
        param_names: Corresponding parameter names.
        path: File path to load from.
    """
    var torch = Python.import_module("torch")
    var sd = torch.load(path, weights_only=True)
    state_dict_to_tape(tape, sd, param_indices, param_names)
