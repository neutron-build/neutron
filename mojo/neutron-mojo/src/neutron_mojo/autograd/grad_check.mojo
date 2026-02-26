# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Gradient Checking Utility
# ===----------------------------------------------------------------------=== #

"""Numerical gradient checking for verifying autograd correctness.

Compares analytical gradients from backward() with numerical finite-difference
gradients. Essential for testing new backward implementations.
"""

from neutron_mojo.tensor.tensor import Tensor


struct GradCheckResult(Copyable, Movable):
    """Result of comparing analytical vs numerical gradients."""
    var max_abs_diff: Float64
    var max_rel_diff: Float64
    var passed: Bool

    fn __init__(out self, max_abs_diff: Float64, max_rel_diff: Float64, passed: Bool):
        self.max_abs_diff = max_abs_diff
        self.max_rel_diff = max_rel_diff
        self.passed = passed

    fn __copyinit__(out self, other: Self):
        self.max_abs_diff = other.max_abs_diff
        self.max_rel_diff = other.max_rel_diff
        self.passed = other.passed

    fn __moveinit__(out self, deinit other: Self):
        self.max_abs_diff = other.max_abs_diff
        self.max_rel_diff = other.max_rel_diff
        self.passed = other.passed


fn compare_gradients(
    analytical: Tensor[DType.float32],
    numerical: Tensor[DType.float32],
    rtol: Float64 = 1e-3,
    atol: Float64 = 1e-5,
) -> GradCheckResult:
    """Compare analytical and numerical gradients.

    Args:
        analytical: Gradients computed by backward().
        numerical: Gradients computed by finite differences.
        rtol: Relative tolerance.
        atol: Absolute tolerance.

    Returns:
        GradCheckResult with max differences and pass/fail.
    """
    var n = analytical.numel()
    var max_abs = Float64(0.0)
    var max_rel = Float64(0.0)
    var a_ptr = analytical.data_ptr()
    var n_ptr = numerical.data_ptr()

    for i in range(n):
        var a_val = Float64(a_ptr.load(i))
        var n_val = Float64(n_ptr.load(i))
        var abs_diff = abs(a_val - n_val)
        if abs_diff > max_abs:
            max_abs = abs_diff

        var denom = max(abs(a_val), abs(n_val))
        if denom > 1e-10:
            var rel = abs_diff / denom
            if rel > max_rel:
                max_rel = rel

    var passed = max_abs <= atol + rtol * max_rel
    # Also check absolute: all diffs within tolerance
    if max_abs > atol * 10.0 and max_rel > rtol * 10.0:
        passed = False

    return GradCheckResult(max_abs, max_rel, passed)
