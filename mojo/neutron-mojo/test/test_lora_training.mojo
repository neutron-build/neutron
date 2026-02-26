# ===----------------------------------------------------------------------=== #
# Neutron Mojo — LoRA Training Tests
# ===----------------------------------------------------------------------=== #

"""Tests for LoRA adapters, LoRATrainableLM, and sequence cross-entropy loss."""

from neutron_mojo.autograd import Tape, run_backward
from neutron_mojo.autograd.ops import tracked_add
from neutron_mojo.train.modules import Linear
from neutron_mojo.train.trainable import TrainableLM
from neutron_mojo.train.lora_train import TrainableLoRA, LoRATrainableLM
from neutron_mojo.train.losses import cross_entropy_loss, sequence_cross_entropy_loss


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-3, atol: Float64 = 1e-3) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error("Values not close: " + String(a) + " vs " + String(b))


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn test_lora_init() raises:
    """TrainableLoRA initializes with correct fields."""
    var lora = TrainableLoRA(in_features=8, out_features=8, rank=2)
    assert_eq(lora.in_features, 8)
    assert_eq(lora.out_features, 8)
    assert_eq(lora.rank, 2)
    assert_eq(lora.a_idx, -1)
    assert_eq(lora.b_idx, -1)
    if lora.registered:
        raise Error("Should not be registered yet")
    print("  lora_init: PASS")


fn test_lora_register() raises:
    """TrainableLoRA registers A and B on tape."""
    var tape = Tape(65536)
    var lora = TrainableLoRA(in_features=4, out_features=4, rank=2)
    lora.register(tape)

    if not lora.registered:
        raise Error("Should be registered")
    if lora.a_idx < 0:
        raise Error("A should be registered")
    if lora.b_idx < 0:
        raise Error("B should be registered")

    # A: (rank, in_features) = (2, 4) = 8 elements
    assert_eq(tape.var_numel(lora.a_idx), 8)
    # B: (out_features, rank) = (4, 2) = 8 elements
    assert_eq(tape.var_numel(lora.b_idx), 8)
    print("  lora_register: PASS")


fn test_lora_b_zero_init() raises:
    """LoRA B matrix starts at zero (so delta = 0 initially)."""
    var tape = Tape(65536)
    var lora = TrainableLoRA(in_features=4, out_features=4, rank=2)
    lora.register(tape)

    var b_numel = tape.var_numel(lora.b_idx)
    for i in range(b_numel):
        assert_close(tape.get_data(lora.b_idx, i), Float32(0.0), atol=1e-10)
    print("  lora_b_zero_init: PASS")


fn test_lora_forward_zero_delta() raises:
    """LoRA forward with zero B produces zero output."""
    var tape = Tape(65536)
    var lora = TrainableLoRA(in_features=4, out_features=4, rank=2)
    lora.register(tape)

    # Create input
    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^)
    for i in range(4):
        tape.set_data(x_idx, i, Float32(1.0))

    var delta_idx = lora.forward(tape, x_idx)
    # B is zero, so delta should be all zeros
    var n = tape.var_numel(delta_idx)
    for i in range(n):
        assert_close(tape.get_data(delta_idx, i), Float32(0.0), atol=1e-5)
    print("  lora_forward_zero_delta: PASS")


fn test_lora_forward_shape() raises:
    """LoRA forward produces correct output shape."""
    var tape = Tape(65536)
    var lora = TrainableLoRA(in_features=4, out_features=6, rank=2)
    lora.register(tape)

    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^)
    for i in range(4):
        tape.set_data(x_idx, i, Float32(0.5))

    var delta_idx = lora.forward(tape, x_idx)
    # Output should have out_features elements (1 * out_features from matmul)
    assert_eq(tape.var_numel(delta_idx), 6)
    print("  lora_forward_shape: PASS")


fn test_lora_copy() raises:
    """TrainableLoRA is Copyable."""
    var lora1 = TrainableLoRA(in_features=4, out_features=4, rank=2)
    var lora2 = lora1
    assert_eq(lora2.in_features, 4)
    assert_eq(lora2.rank, 2)
    print("  lora_copy: PASS")


fn test_lora_backward() raises:
    """LoRA backward produces gradients for A and B."""
    var tape = Tape(65536)
    var lora = TrainableLoRA(in_features=4, out_features=4, rank=2)
    lora.register(tape)

    # Set B to non-zero for gradient flow
    var b_numel = tape.var_numel(lora.b_idx)
    for i in range(b_numel):
        tape.set_data(lora.b_idx, i, Float32(0.1))

    var dims = List[Int]()
    dims.append(4)
    var x_idx = tape.add_variable(dims^, requires_grad=False)
    for i in range(4):
        tape.set_data(x_idx, i, Float32(1.0))

    var delta_idx = lora.forward(tape, x_idx)
    # Sum to scalar for backward
    from neutron_mojo.autograd.ops import tracked_sum
    var loss_idx = tracked_sum(tape, delta_idx)
    run_backward(tape, loss_idx)

    # Both A and B should have gradients
    var has_a_grad = False
    var a_numel = tape.var_numel(lora.a_idx)
    for i in range(a_numel):
        if abs(Float64(tape.get_grad(lora.a_idx, i))) > 1e-10:
            has_a_grad = True
            break
    if not has_a_grad:
        raise Error("Expected non-zero A gradients")

    var has_b_grad = False
    for i in range(b_numel):
        if abs(Float64(tape.get_grad(lora.b_idx, i))) > 1e-10:
            has_b_grad = True
            break
    if not has_b_grad:
        raise Error("Expected non-zero B gradients")
    print("  lora_backward: PASS")


fn test_lora_lm_init() raises:
    """LoRATrainableLM initializes with correct structure."""
    var lora_lm = LoRATrainableLM(vocab_size=8, hidden_dim=4, num_layers=2, rank=2, ffn_dim=8)
    assert_eq(len(lora_lm.lora_q), 2)
    assert_eq(len(lora_lm.lora_v), 2)
    assert_eq(lora_lm.rank, 2)
    print("  lora_lm_init: PASS")


fn test_lora_lm_register() raises:
    """LoRATrainableLM registers all parameters."""
    var tape = Tape(524288)
    var lora_lm = LoRATrainableLM(vocab_size=8, hidden_dim=4, num_layers=1, rank=2, ffn_dim=8)
    lora_lm.register(tape)

    if not lora_lm.registered:
        raise Error("Should be registered")
    # LoRA params: 1 layer * 2 adapters * (A + B) = 4 tensors
    var lora_params = lora_lm.lora_param_indices()
    assert_eq(len(lora_params), 4)
    print("  lora_lm_register: PASS")


fn test_lora_lm_forward() raises:
    """LoRATrainableLM forward produces logits."""
    var tape = Tape(524288)
    var lora_lm = LoRATrainableLM(vocab_size=8, hidden_dim=4, num_layers=1, rank=2, ffn_dim=8)
    lora_lm.register(tape)

    var logits_idx = lora_lm.forward(tape, 3)
    assert_eq(tape.var_numel(logits_idx), 8)
    print("  lora_lm_forward: PASS")


fn test_lora_lm_freeze_base() raises:
    """freeze_base disables gradients for base model params."""
    var tape = Tape(524288)
    var lora_lm = LoRATrainableLM(vocab_size=8, hidden_dim=4, num_layers=1, rank=2, ffn_dim=8)
    lora_lm.register(tape)
    lora_lm.freeze_base(tape)

    # Base params should have requires_grad=False
    var base_params = lora_lm.base.all_param_indices()
    for i in range(len(base_params)):
        if tape.var_requires_grad[base_params[i]]:
            raise Error("Base param " + String(base_params[i]) + " should be frozen")

    # LoRA params should still have requires_grad=True
    var lora_params = lora_lm.lora_param_indices()
    for i in range(len(lora_params)):
        if not tape.var_requires_grad[lora_params[i]]:
            raise Error("LoRA param should be trainable")
    print("  lora_lm_freeze_base: PASS")


fn test_lora_param_count() raises:
    """Total LoRA params is much smaller than base."""
    var tape = Tape(524288)
    var lora_lm = LoRATrainableLM(vocab_size=8, hidden_dim=4, num_layers=2, rank=2, ffn_dim=8)
    lora_lm.register(tape)

    var lora_total = lora_lm.total_lora_params(tape)
    var base_total = lora_lm.base.num_parameters(tape)

    # LoRA params should be much less than base
    # Per layer: A_q(2*4=8) + B_q(4*2=8) + A_v(2*4=8) + B_v(4*2=8) = 32
    # 2 layers = 64
    if lora_total >= base_total:
        raise Error("LoRA params should be fewer: " + String(lora_total) + " vs " + String(base_total))
    print("  lora_param_count: PASS (lora=" + String(lora_total) + " base=" + String(base_total) + ")")


fn test_seq_cross_entropy_basic() raises:
    """sequence_cross_entropy_loss computes average loss."""
    var tape = Tape(65536)

    # Create 2 logit vectors (vocab=4)
    var logits_list = List[Int]()
    for t in range(2):
        var dims = List[Int]()
        dims.append(4)
        var l_idx = tape.add_variable(dims^)
        for i in range(4):
            tape.set_data(l_idx, i, Float32(0.25 * (i + 1) + t * 0.1))
        logits_list.append(l_idx)

    var targets = List[Int]()
    targets.append(1)
    targets.append(3)

    var loss_idx = sequence_cross_entropy_loss(tape, logits_list, targets, 4)
    var loss_val = Float64(tape.get_data(loss_idx, 0))

    # Loss should be positive
    if loss_val <= 0.0:
        raise Error("Loss should be positive, got " + String(loss_val))
    print("  seq_cross_entropy_basic: PASS (loss=" + String(loss_val) + ")")


fn test_seq_cross_entropy_backward() raises:
    """sequence_cross_entropy_loss backward produces gradients."""
    var tape = Tape(65536)

    var logits_list = List[Int]()
    for t in range(2):
        var dims = List[Int]()
        dims.append(4)
        var l_idx = tape.add_variable(dims^)
        for i in range(4):
            tape.set_data(l_idx, i, Float32(0.5 * (i + 1)))
        logits_list.append(l_idx)

    var targets = List[Int]()
    targets.append(0)
    targets.append(2)

    var loss_idx = sequence_cross_entropy_loss(tape, logits_list, targets, 4)
    run_backward(tape, loss_idx)

    # Both logit vectors should have gradients
    for t in range(2):
        var has_grad = False
        for i in range(4):
            if abs(Float64(tape.get_grad(logits_list[t], i))) > 1e-10:
                has_grad = True
                break
        if not has_grad:
            raise Error("Expected gradients for logits position " + String(t))
    print("  seq_cross_entropy_backward: PASS")


fn test_lora_lm_backward_frozen() raises:
    """LoRA backward with frozen base: only LoRA params get gradients."""
    var tape = Tape(524288)
    var lora_lm = LoRATrainableLM(vocab_size=8, hidden_dim=4, num_layers=1, rank=2, ffn_dim=8)
    lora_lm.register(tape)
    lora_lm.freeze_base(tape)

    # Set B to non-zero for gradient flow
    var lora_params = lora_lm.lora_param_indices()
    for i in range(len(lora_params)):
        var n = tape.var_numel(lora_params[i])
        for j in range(n):
            tape.set_data(lora_params[i], j, Float32(0.1))

    var logits_idx = lora_lm.forward(tape, 2)
    var loss_idx = cross_entropy_loss(tape, logits_idx, 5, 8)
    run_backward(tape, loss_idx)

    # Base params should have zero gradients (frozen)
    var base_params = lora_lm.base.all_param_indices()
    for i in range(len(base_params)):
        var n = tape.var_numel(base_params[i])
        for j in range(n):
            if abs(Float64(tape.get_grad(base_params[i], j))) > 1e-10:
                raise Error("Frozen base param " + String(base_params[i]) + " has gradient")

    print("  lora_lm_backward_frozen: PASS")


fn main() raises:
    print("test_lora_training:")
    test_lora_init()
    test_lora_register()
    test_lora_b_zero_init()
    test_lora_forward_zero_delta()
    test_lora_forward_shape()
    test_lora_copy()
    test_lora_backward()
    test_lora_lm_init()
    test_lora_lm_register()
    test_lora_lm_forward()
    test_lora_lm_freeze_base()
    test_lora_param_count()
    test_seq_cross_entropy_basic()
    test_seq_cross_entropy_backward()
    test_lora_lm_backward_frozen()
    print("ALL PASSED (15 tests)")
