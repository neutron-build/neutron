# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Gradient Clipping
# ===----------------------------------------------------------------------=== #

"""Gradient clipping utilities."""

from math import sqrt

from neutron_mojo.autograd.tape import Tape


fn clip_grad_norm(mut tape: Tape, param_indices: List[Int], max_norm: Float64) -> Float64:
    """Clip gradients by global norm.

    Scales all parameter gradients so that the global L2 norm
    does not exceed max_norm.

    Returns the original (pre-clip) global norm.
    """
    # Compute global norm
    var total_sq = Float64(0.0)
    for p in range(len(param_indices)):
        var idx = param_indices[p]
        var n = tape.var_numel(idx)
        var off = tape.var_offset(idx)
        var grad = tape.grad_flat.data_ptr()
        for i in range(n):
            var g = Float64(grad.load(off + i))
            total_sq += g * g

    var global_norm = sqrt(total_sq)

    # Clip if needed
    if global_norm > max_norm:
        var scale = max_norm / (global_norm + 1e-6)
        for p in range(len(param_indices)):
            var idx = param_indices[p]
            var n = tape.var_numel(idx)
            var off = tape.var_offset(idx)
            var grad = tape.grad_flat.data_ptr()
            for i in range(n):
                grad.store(off + i, Float32(Float64(grad.load(off + i)) * scale))

    return global_norm
