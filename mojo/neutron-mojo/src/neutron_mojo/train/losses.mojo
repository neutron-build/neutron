# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Loss Functions
# ===----------------------------------------------------------------------=== #

"""Standard loss functions with autograd tape integration.

NOTE: Uses tape.get_data()/set_data() instead of data_ptr() to avoid
the Mojo 0.26.2 aliasing bug.
"""

from math import exp, log

from neutron_mojo.autograd.tape import (
    Tape, TapeEntry, OP_LOG_SOFTMAX, OP_CROSS_ENTROPY, OP_MSE,
    OP_L1, OP_BCE, OP_KL_DIV,
)
from neutron_mojo.autograd.ops import tracked_sum, tracked_scalar_mul, tracked_add


fn log_softmax(mut tape: Tape, x_idx: Int) -> Int:
    """Numerically stable log-softmax."""
    var n = tape.var_numel(x_idx)
    var dims = List[Int]()
    var shape = tape.var_shapes[x_idx].copy()
    for i in range(len(shape)):
        dims.append(shape[i])
    var y_idx = tape.add_variable(dims^, requires_grad=True)

    # Find max
    var max_val = Float64(tape.get_data(x_idx, 0))
    for i in range(1, n):
        var v = Float64(tape.get_data(x_idx, i))
        if v > max_val:
            max_val = v

    # log(sum(exp(x - max)))
    var log_sum_exp = Float64(0.0)
    for i in range(n):
        log_sum_exp += exp(Float64(tape.get_data(x_idx, i)) - max_val)
    log_sum_exp = log(log_sum_exp) + max_val

    # log_softmax = x - log_sum_exp
    for i in range(n):
        tape.set_data(y_idx, i, Float32(Float64(tape.get_data(x_idx, i)) - log_sum_exp))

    tape.record(TapeEntry(OP_LOG_SOFTMAX(), x_idx, -1, y_idx))
    return y_idx


fn cross_entropy_loss(mut tape: Tape, logits_idx: Int, target: Int, vocab_size: Int) -> Int:
    """Cross-entropy loss: -log(softmax(logits)[target]).

    Fused backward: softmax(logits) - one_hot(target).
    """
    var dims = List[Int]()
    dims.append(1)
    var loss_idx = tape.add_variable(dims^, requires_grad=True)

    # Numerically stable cross-entropy
    var max_val = Float64(tape.get_data(logits_idx, 0))
    for i in range(1, vocab_size):
        var v = Float64(tape.get_data(logits_idx, i))
        if v > max_val:
            max_val = v

    var sum_exp = Float64(0.0)
    for i in range(vocab_size):
        sum_exp += exp(Float64(tape.get_data(logits_idx, i)) - max_val)

    var log_sum_exp = log(sum_exp) + max_val
    var loss = -(Float64(tape.get_data(logits_idx, target)) - log_sum_exp)
    tape.set_data(loss_idx, 0, Float32(loss))

    tape.record(TapeEntry(OP_CROSS_ENTROPY(), logits_idx, -1, loss_idx,
        cached_int=target, cached_int2=vocab_size))
    return loss_idx


fn mse_loss(mut tape: Tape, pred_idx: Int, target_idx: Int) -> Int:
    """Mean squared error loss: mean((pred - target)^2)."""
    var n = tape.var_numel(pred_idx)
    var dims = List[Int]()
    dims.append(1)
    var loss_idx = tape.add_variable(dims^, requires_grad=True)

    var mse = Float64(0.0)
    for i in range(n):
        var diff = Float64(tape.get_data(pred_idx, i)) - Float64(tape.get_data(target_idx, i))
        mse += diff * diff
    mse /= Float64(n)
    tape.set_data(loss_idx, 0, Float32(mse))

    tape.record(TapeEntry(OP_MSE(), pred_idx, target_idx, loss_idx))
    return loss_idx


fn l1_loss(mut tape: Tape, pred_idx: Int, target_idx: Int) -> Int:
    """L1 (mean absolute error) loss: mean(|pred - target|)."""
    var n = tape.var_numel(pred_idx)
    var dims = List[Int]()
    dims.append(1)
    var loss_idx = tape.add_variable(dims^, requires_grad=True)

    var mae = Float64(0.0)
    for i in range(n):
        var diff = Float64(tape.get_data(pred_idx, i)) - Float64(tape.get_data(target_idx, i))
        mae += abs(diff)
    mae /= Float64(n)
    tape.set_data(loss_idx, 0, Float32(mae))

    tape.record(TapeEntry(OP_L1(), pred_idx, target_idx, loss_idx))
    return loss_idx


fn binary_cross_entropy(mut tape: Tape, pred_idx: Int, target_idx: Int) -> Int:
    """Binary cross-entropy: -mean(target*log(pred) + (1-target)*log(1-pred))."""
    var n = tape.var_numel(pred_idx)
    var dims = List[Int]()
    dims.append(1)
    var loss_idx = tape.add_variable(dims^, requires_grad=True)

    var bce = Float64(0.0)
    for i in range(n):
        var p = Float64(tape.get_data(pred_idx, i))
        var t = Float64(tape.get_data(target_idx, i))
        # Clamp p for numerical stability
        p = max(1e-7, min(1.0 - 1e-7, p))
        bce += -(t * log(p) + (1.0 - t) * log(1.0 - p))
    bce /= Float64(n)
    tape.set_data(loss_idx, 0, Float32(bce))

    tape.record(TapeEntry(OP_BCE(), pred_idx, target_idx, loss_idx))
    return loss_idx


fn kl_divergence(mut tape: Tape, p_idx: Int, q_idx: Int) -> Int:
    """KL divergence: sum(p * log(p/q))."""
    var n = tape.var_numel(p_idx)
    var dims = List[Int]()
    dims.append(1)
    var loss_idx = tape.add_variable(dims^, requires_grad=True)

    var kl = Float64(0.0)
    for i in range(n):
        var p_val = Float64(tape.get_data(p_idx, i))
        var q_val = Float64(tape.get_data(q_idx, i))
        if p_val > 1e-10 and q_val > 1e-10:
            kl += p_val * log(p_val / q_val)
    tape.set_data(loss_idx, 0, Float32(kl))

    tape.record(TapeEntry(OP_KL_DIV(), p_idx, q_idx, loss_idx))
    return loss_idx


fn sequence_cross_entropy_loss(
    mut tape: Tape,
    logits_indices: List[Int],
    targets: List[Int],
    vocab_size: Int,
) -> Int:
    """Average cross-entropy loss across all positions in a sequence.

    Computes per-position cross-entropy, sums them with tracked_add,
    then scales by 1/seq_len with tracked_scalar_mul for proper
    gradient flow through the tape.

    Args:
        tape: The autograd tape.
        logits_indices: Per-position logits variable indices.
        targets: Per-position target token IDs.
        vocab_size: Vocabulary size.

    Returns:
        Variable index of the scalar mean loss.
    """
    var seq_len = len(logits_indices)

    # Compute first position loss
    var acc_idx = cross_entropy_loss(tape, logits_indices[0], targets[0], vocab_size)

    # Accumulate remaining position losses using tracked_add
    for t in range(1, seq_len):
        var loss_t = cross_entropy_loss(tape, logits_indices[t], targets[t], vocab_size)
        acc_idx = tracked_add(tape, acc_idx, loss_t)

    # Scale by 1/seq_len to get mean
    var scale = 1.0 / Float64(seq_len)
    var avg_idx = tracked_scalar_mul(tape, acc_idx, scale)
    return avg_idx
