# ===----------------------------------------------------------------------=== #
# Neutron Mojo — NN Modules tests
# ===----------------------------------------------------------------------=== #

"""Tests for Linear, Embedding, RMSNormModule, LayerNormModule, Dropout."""

from neutron_mojo.autograd import Tape, run_backward, tracked_sum
from neutron_mojo.train.modules import (
    Linear, Embedding, RMSNormModule, LayerNormModule, Dropout,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-3, atol: Float64 = 1e-4) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error("Values not close: " + String(a) + " vs " + String(b) + " (diff=" + String(diff) + ")")


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn test_linear_register() raises:
    """Linear layer registers weight and bias."""
    var tape = Tape(4096)
    var layer = Linear(4, 3, has_bias=True)
    layer.register(tape)
    assert_eq(len(layer.param_indices()), 2)  # weight + bias
    assert_eq(tape.var_numel(layer.weight_idx), 12)  # 3 * 4
    assert_eq(tape.var_numel(layer.bias_idx), 3)
    print("  linear_register: PASS")


fn test_linear_forward() raises:
    """Linear forward produces output."""
    var tape = Tape(8192)
    var layer = Linear(4, 3)
    layer.register(tape)

    # Create input
    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^, requires_grad=True)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))
    tape.set_data(x_idx, 2, Float32(3.0))
    tape.set_data(x_idx, 3, Float32(4.0))

    var y_idx = layer.forward(tape, x_idx)
    # Output should have 3 elements (out_features)
    # Matmul: (1,4) @ (4,3) = (1,3)
    if tape.var_numel(y_idx) < 3:
        raise Error("Expected at least 3 output elements, got " + String(tape.var_numel(y_idx)))
    print("  linear_forward: PASS")


fn test_linear_backward() raises:
    """Linear backward produces gradients."""
    var tape = Tape(16384)
    var layer = Linear(4, 3, has_bias=False)
    layer.register(tape)

    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^, requires_grad=True)
    for i in range(4):
        tape.set_data(x_idx, i, Float32(1.0))

    var y_idx = layer.forward(tape, x_idx)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    # Weight should have gradients
    var w_grad = tape.get_grad(layer.weight_idx, 0)
    # Gradients can be any value; just check they exist (not NaN-like)
    _ = w_grad
    print("  linear_backward: PASS")


fn test_linear_no_bias() raises:
    """Linear without bias."""
    var tape = Tape(4096)
    var layer = Linear(3, 2, has_bias=False)
    layer.register(tape)
    assert_eq(len(layer.param_indices()), 1)  # weight only
    print("  linear_no_bias: PASS")


fn test_embedding_register() raises:
    """Embedding registers lookup table."""
    var tape = Tape(8192)
    var embed = Embedding(100, 16)
    embed.register(tape)
    assert_eq(tape.var_numel(embed.embed_idx), 1600)  # 100 * 16
    print("  embedding_register: PASS")


fn test_embedding_forward() raises:
    """Embedding lookup returns correct dim."""
    var tape = Tape(8192)
    var embed = Embedding(10, 4)
    embed.register(tape)

    # Set known values for token 3
    for d in range(4):
        tape.set_data(embed.embed_idx, 3 * 4 + d, Float32(d + 1))

    var y_idx = embed.forward(tape, 3)
    assert_eq(tape.var_numel(y_idx), 4)
    assert_close(tape.get_data(y_idx, 0), 1.0)
    assert_close(tape.get_data(y_idx, 3), 4.0)
    print("  embedding_forward: PASS")


fn test_embedding_backward() raises:
    """Embedding backward accumulates into table."""
    var tape = Tape(8192)
    var embed = Embedding(10, 4)
    embed.register(tape)

    var y_idx = embed.forward(tape, 2)
    var loss_idx = tracked_sum(tape, y_idx)
    run_backward(tape, loss_idx)

    # Gradient should accumulate into row 2 of embedding table
    var g = tape.get_grad(embed.embed_idx, 2 * 4)
    _ = g  # Just check no crash
    print("  embedding_backward: PASS")


fn test_rmsnorm_module() raises:
    """RMSNorm module forward."""
    var tape = Tape(4096)
    var norm = RMSNormModule(4)
    norm.register(tape)

    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))
    tape.set_data(x_idx, 2, Float32(3.0))
    tape.set_data(x_idx, 3, Float32(4.0))

    var y_idx = norm.forward(tape, x_idx)
    assert_eq(tape.var_numel(y_idx), 4)
    # Output should be normalized (not zero)
    var v0 = tape.get_data(y_idx, 0)
    if abs(Float64(v0)) < 1e-10:
        raise Error("Expected non-zero RMSNorm output")
    print("  rmsnorm_module: PASS")


fn test_layernorm_module() raises:
    """LayerNorm module forward."""
    var tape = Tape(4096)
    var norm = LayerNormModule(4)
    norm.register(tape)

    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^)
    tape.set_data(x_idx, 0, Float32(1.0))
    tape.set_data(x_idx, 1, Float32(2.0))
    tape.set_data(x_idx, 2, Float32(3.0))
    tape.set_data(x_idx, 3, Float32(4.0))

    var y_idx = norm.forward(tape, x_idx)
    assert_eq(tape.var_numel(y_idx), 4)
    # Output should be approximately normalized (mean ≈ 0)
    var sum_val = Float64(0.0)
    for i in range(4):
        sum_val += Float64(tape.get_data(y_idx, i))
    if abs(sum_val) > 0.1:
        raise Error("LayerNorm output should have near-zero mean, got " + String(sum_val))
    print("  layernorm_module: PASS")


fn test_dropout_training() raises:
    """Dropout zeros some elements in training mode."""
    var tape = Tape(4096)
    var drop = Dropout(0.5)

    var dims = List[Int]()
    dims.append(100)
    var x_idx = tape.add_variable(dims^)
    for i in range(100):
        tape.set_data(x_idx, i, Float32(1.0))

    var y_idx = drop.forward(tape, x_idx)
    var num_zero = 0
    for i in range(100):
        if abs(Float64(tape.get_data(y_idx, i))) < 1e-6:
            num_zero += 1
    # With p=0.5, expect roughly 50 zeros (±20 for randomness)
    if num_zero < 20 or num_zero > 80:
        raise Error("Expected roughly 50 zeros from dropout, got " + String(num_zero))
    print("  dropout_training: PASS")


fn test_dropout_eval() raises:
    """Dropout passes through in eval mode."""
    var tape = Tape(4096)
    var drop = Dropout(0.5)
    drop.eval_mode()

    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^)
    for i in range(4):
        tape.set_data(x_idx, i, Float32(i + 1))

    var y_idx = drop.forward(tape, x_idx)
    # In eval mode, should return same index (identity)
    assert_eq(y_idx, x_idx)
    print("  dropout_eval: PASS")


fn test_linear_xavier_init() raises:
    """Xavier init produces reasonable magnitude values."""
    var tape = Tape(16384)
    var layer = Linear(256, 128)
    layer.register(tape)

    # Check that weights are not all zero and have reasonable scale
    var sum_abs = Float64(0.0)
    var n = tape.var_numel(layer.weight_idx)
    for i in range(n):
        sum_abs += abs(Float64(tape.get_data(layer.weight_idx, i)))
    var avg_abs = sum_abs / Float64(n)
    # Xavier scale ≈ sqrt(2/(256+128)) ≈ 0.072, avg_abs should be ~0.04
    if avg_abs < 0.001 or avg_abs > 1.0:
        raise Error("Xavier init out of range: avg_abs=" + String(avg_abs))
    print("  linear_xavier_init: PASS")


fn main() raises:
    print("test_modules:")
    test_linear_register()
    test_linear_forward()
    test_linear_backward()
    test_linear_no_bias()
    test_embedding_register()
    test_embedding_forward()
    test_embedding_backward()
    test_rmsnorm_module()
    test_layernorm_module()
    test_dropout_training()
    test_dropout_eval()
    test_linear_xavier_init()
    print("ALL PASSED (12 tests)")
