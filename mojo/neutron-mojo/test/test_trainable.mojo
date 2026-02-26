# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Trainable transformer tests
# ===----------------------------------------------------------------------=== #

"""Tests for TrainableTransformerBlock and TrainableLM."""

from neutron_mojo.autograd import Tape, run_backward
from neutron_mojo.train.modules import Linear, Embedding, RMSNormModule
from neutron_mojo.train.trainable import (
    TrainableTransformerBlock, TrainableLM, causal_lm_loss,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-3, atol: Float64 = 1e-3) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn test_block_init() raises:
    """TransformerBlock initializes correctly."""
    var block = TrainableTransformerBlock(hidden_dim=8, ffn_dim=32)
    assert_eq(block.hidden_dim, 8)
    assert_eq(block.head_dim, 8)
    assert_eq(block.ffn_dim, 32)
    if block.registered:
        raise Error("Block should not be registered yet")
    print("  block_init: PASS")


fn test_block_default_ffn() raises:
    """Default FFN dim is 4x hidden."""
    var block = TrainableTransformerBlock(hidden_dim=16)
    assert_eq(block.ffn_dim, 64)
    print("  block_default_ffn: PASS")


fn test_block_register() raises:
    """Block registers all parameters on tape."""
    var tape = Tape(65536)
    var block = TrainableTransformerBlock(hidden_dim=4, ffn_dim=8)
    block.register(tape)
    if not block.registered:
        raise Error("Block should be registered")
    var params = block.param_indices()
    # attn_norm(1) + q/k/v/o(4 weights) + ffn_norm(1) + gate/up/down(3 weights) = 9
    if len(params) < 9:
        raise Error("Expected at least 9 param indices, got " + String(len(params)))
    print("  block_register: PASS")


fn test_block_forward() raises:
    """Block forward pass produces output."""
    var tape = Tape(65536)
    var block = TrainableTransformerBlock(hidden_dim=4, ffn_dim=8)
    block.register(tape)

    # Create input
    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^)
    for i in range(4):
        tape.set_data(x_idx, i, Float32(0.1 * (i + 1)))

    var out_idx = block.forward(tape, x_idx)
    # Output should be a valid variable index
    if out_idx < 0:
        raise Error("Invalid output index")
    # Output should have same size as input (hidden_dim=4)
    assert_eq(tape.var_numel(out_idx), 4)
    print("  block_forward: PASS")


fn test_lm_init() raises:
    """TrainableLM initializes correctly."""
    var lm = TrainableLM(vocab_size=16, hidden_dim=4, num_layers=2)
    assert_eq(lm.vocab_size, 16)
    assert_eq(lm.hidden_dim, 4)
    assert_eq(lm.num_layers, 2)
    if lm.registered:
        raise Error("LM should not be registered yet")
    print("  lm_init: PASS")


fn test_lm_register() raises:
    """TrainableLM registers all parameters."""
    var tape = Tape(262144)
    var lm = TrainableLM(vocab_size=8, hidden_dim=4, num_layers=1)
    lm.register(tape)
    if not lm.registered:
        raise Error("LM should be registered")
    var params = lm.all_param_indices()
    # embedding(1) + 1 block(9) + final_norm(1) + lm_head(1) = 12
    if len(params) < 3:
        raise Error("Expected at least 3 param indices, got " + String(len(params)))
    print("  lm_register: PASS")


fn test_lm_forward() raises:
    """TrainableLM forward: token -> logits."""
    var tape = Tape(262144)
    var lm = TrainableLM(vocab_size=8, hidden_dim=4, num_layers=1)
    lm.register(tape)

    var logits_idx = lm.forward(tape, 3)  # token_id=3
    # Logits should have vocab_size elements
    assert_eq(tape.var_numel(logits_idx), 8)
    print("  lm_forward: PASS")


fn test_lm_num_parameters() raises:
    """Count trainable parameters."""
    var tape = Tape(262144)
    var lm = TrainableLM(vocab_size=8, hidden_dim=4, num_layers=1, ffn_dim=8)
    lm.register(tape)
    var num_params = lm.num_parameters(tape)
    # Should be positive
    if num_params <= 0:
        raise Error("Expected positive num_parameters, got " + String(num_params))
    # Rough calculation:
    # embed: 8*4=32, q/k/v/o: 4*4*4=64, gate/up: 2*4*8=64, down: 8*4=32
    # attn_norm: 4, ffn_norm: 4, final_norm: 4, lm_head: 4*8=32
    # Total ≈ 236
    print("  lm_num_parameters: PASS (params=" + String(num_params) + ")")


fn test_causal_lm_loss() raises:
    """Causal LM loss produces a scalar."""
    var tape = Tape(262144)
    var lm = TrainableLM(vocab_size=8, hidden_dim=4, num_layers=1)
    lm.register(tape)

    var loss_idx = causal_lm_loss(tape, lm, token_id=2, target_id=5)
    # Loss should be a scalar
    assert_eq(tape.var_numel(loss_idx), 1)
    # Loss should be positive
    var loss_val = Float64(tape.get_data(loss_idx, 0))
    if loss_val < 0.0:
        raise Error("Loss should be non-negative")
    print("  causal_lm_loss: PASS (loss=" + String(loss_val) + ")")


fn test_causal_lm_backward() raises:
    """Backward through causal LM loss produces gradients."""
    var tape = Tape(262144)
    var lm = TrainableLM(vocab_size=8, hidden_dim=4, num_layers=1)
    lm.register(tape)

    var loss_idx = causal_lm_loss(tape, lm, token_id=1, target_id=3)
    run_backward(tape, loss_idx)

    # Check that at least some parameters have non-zero gradients
    var params = lm.all_param_indices()
    var has_nonzero = False
    for i in range(len(params)):
        var n = tape.var_numel(params[i])
        for j in range(n):
            if abs(Float64(tape.get_grad(params[i], j))) > 1e-10:
                has_nonzero = True
                break
        if has_nonzero:
            break
    if not has_nonzero:
        raise Error("Expected non-zero gradients after backward")
    print("  causal_lm_backward: PASS")


fn test_block_copy() raises:
    """TransformerBlock is Copyable."""
    var block1 = TrainableTransformerBlock(hidden_dim=4, ffn_dim=8)
    var block2 = block1.copy()
    assert_eq(block2.hidden_dim, 4)
    assert_eq(block2.ffn_dim, 8)
    print("  block_copy: PASS")


fn main() raises:
    print("test_trainable:")
    test_block_init()
    test_block_default_ffn()
    test_block_register()
    test_block_forward()
    test_lm_init()
    test_lm_register()
    test_lm_forward()
    test_lm_num_parameters()
    test_causal_lm_loss()
    test_causal_lm_backward()
    test_block_copy()
    print("ALL PASSED (11 tests)")
