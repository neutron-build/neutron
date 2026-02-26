# ===----------------------------------------------------------------------=== #
# Neutron Mojo -- Gradient Checkpointing Tests
# ===----------------------------------------------------------------------=== #

"""Tests for gradient checkpointing: verifies that checkpointed backward
produces identical gradients to regular backward."""

from neutron_mojo.autograd import (
    Tape, run_backward,
    tracked_add, tracked_sub, tracked_mul, tracked_matmul,
    tracked_relu, tracked_sigmoid, tracked_scalar_mul,
    tracked_sum, tracked_mean, tracked_neg,
)
from neutron_mojo.autograd.checkpoint import (
    CheckpointSegment, mark_checkpoint,
    auto_checkpoint_segments, run_backward_checkpointed,
    gradients_match,
)


fn assert_close(a: Float32, b: Float32, rtol: Float64 = 1e-4, atol: Float64 = 1e-5) raises:
    var diff = abs(Float64(a) - Float64(b))
    var threshold = atol + rtol * abs(Float64(b))
    if diff > threshold:
        raise Error(
            "Values not close: " + String(a) + " vs " + String(b)
            + " (diff=" + String(diff) + ")"
        )


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn assert_true(val: Bool, msg: String = "Expected true") raises:
    if not val:
        raise Error(msg)


# ===----------------------------------------------------------------------=== #
# Helper: build a tape with chain a*b+c -> relu -> sum
# ===----------------------------------------------------------------------=== #

fn _build_chain_tape() -> Tape:
    """Build a tape with: loss = sum(relu(a*b + c))."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(4)
    var a_idx = tape.add_variable(d.copy())
    var b_idx = tape.add_variable(d.copy())
    var c_idx = tape.add_variable(d.copy())

    tape.set_data(a_idx, 0, Float32(1.0))
    tape.set_data(a_idx, 1, Float32(2.0))
    tape.set_data(a_idx, 2, Float32(-1.0))
    tape.set_data(a_idx, 3, Float32(0.5))
    tape.set_data(b_idx, 0, Float32(2.0))
    tape.set_data(b_idx, 1, Float32(3.0))
    tape.set_data(b_idx, 2, Float32(4.0))
    tape.set_data(b_idx, 3, Float32(-2.0))
    tape.set_data(c_idx, 0, Float32(0.5))
    tape.set_data(c_idx, 1, Float32(-10.0))
    tape.set_data(c_idx, 2, Float32(1.0))
    tape.set_data(c_idx, 3, Float32(3.0))

    var ab = tracked_mul(tape, a_idx, b_idx)
    var s = tracked_add(tape, ab, c_idx)
    var r = tracked_relu(tape, s)
    var loss = tracked_sum(tape, r)
    return tape^


fn _build_chain_tape_copy() -> Tape:
    """Build an identical tape for comparison."""
    return _build_chain_tape()


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #


fn test_checkpoint_segment_basic() raises:
    """CheckpointSegment stores start/end and can be copied."""
    var seg = CheckpointSegment(0, 5)
    assert_eq(seg.start_entry, 0)
    assert_eq(seg.end_entry, 5)
    seg.saved_var_indices.append(1)
    seg.saved_var_indices.append(3)
    var seg2 = seg.copy()
    assert_eq(seg2.start_entry, 0)
    assert_eq(len(seg2.saved_var_indices), 2)
    print("  checkpoint_segment_basic: PASS")


fn test_mark_checkpoint() raises:
    """mark_checkpoint returns current tape entry count."""
    var tape = Tape(4096)
    var d = List[Int]()
    d.append(3)
    var a = tape.add_variable(d.copy())
    var b = tape.add_variable(d.copy())
    tape.set_data(a, 0, Float32(1.0))
    tape.set_data(a, 1, Float32(2.0))
    tape.set_data(a, 2, Float32(3.0))
    tape.set_data(b, 0, Float32(4.0))
    tape.set_data(b, 1, Float32(5.0))
    tape.set_data(b, 2, Float32(6.0))

    var m0 = mark_checkpoint(tape)
    assert_eq(m0, 0)
    var c = tracked_add(tape, a, b)
    var m1 = mark_checkpoint(tape)
    assert_eq(m1, 1)
    print("  mark_checkpoint: PASS")


fn test_auto_segments_even_split() raises:
    """auto_checkpoint_segments divides tape evenly."""
    var tape = _build_chain_tape()
    # Tape has 4 entries: mul, add, relu, sum
    assert_eq(tape.num_entries(), 4)
    var segs = auto_checkpoint_segments(tape, 2)
    assert_eq(len(segs), 2)
    assert_eq(segs[0].start_entry, 0)
    assert_eq(segs[0].end_entry, 2)
    assert_eq(segs[1].start_entry, 2)
    assert_eq(segs[1].end_entry, 4)
    print("  auto_segments_even_split: PASS")


fn test_auto_segments_more_than_entries() raises:
    """More segments than entries: clamped to entries."""
    var tape = _build_chain_tape()
    var segs = auto_checkpoint_segments(tape, 10)
    assert_eq(len(segs), 4)
    # Each segment has 1 entry
    for i in range(4):
        assert_eq(segs[i].end_entry - segs[i].start_entry, 1)
    print("  auto_segments_more_than_entries: PASS")


fn test_auto_segments_single() raises:
    """Single segment covers entire tape."""
    var tape = _build_chain_tape()
    var segs = auto_checkpoint_segments(tape, 1)
    assert_eq(len(segs), 1)
    assert_eq(segs[0].start_entry, 0)
    assert_eq(segs[0].end_entry, 4)
    print("  auto_segments_single: PASS")


fn test_checkpointed_matches_regular_simple() raises:
    """Checkpointed backward matches regular for simple add."""
    # Regular
    var tape1 = Tape(4096)
    var d = List[Int]()
    d.append(3)
    var a1 = tape1.add_variable(d.copy())
    var b1 = tape1.add_variable(d.copy())
    tape1.set_data(a1, 0, Float32(1.0))
    tape1.set_data(a1, 1, Float32(2.0))
    tape1.set_data(a1, 2, Float32(3.0))
    tape1.set_data(b1, 0, Float32(4.0))
    tape1.set_data(b1, 1, Float32(5.0))
    tape1.set_data(b1, 2, Float32(6.0))
    var c1 = tracked_add(tape1, a1, b1)
    var l1 = tracked_sum(tape1, c1)
    run_backward(tape1, l1)

    # Checkpointed
    var tape2 = Tape(4096)
    var a2 = tape2.add_variable(d.copy())
    var b2 = tape2.add_variable(d.copy())
    tape2.set_data(a2, 0, Float32(1.0))
    tape2.set_data(a2, 1, Float32(2.0))
    tape2.set_data(a2, 2, Float32(3.0))
    tape2.set_data(b2, 0, Float32(4.0))
    tape2.set_data(b2, 1, Float32(5.0))
    tape2.set_data(b2, 2, Float32(6.0))
    var c2 = tracked_add(tape2, a2, b2)
    var l2 = tracked_sum(tape2, c2)
    var segs = auto_checkpoint_segments(tape2, 2)
    run_backward_checkpointed(tape2, l2, segs)

    # Compare
    assert_true(gradients_match(tape1, tape2, 0), "a grads mismatch")
    assert_true(gradients_match(tape1, tape2, 1), "b grads mismatch")
    print("  checkpointed_matches_regular_simple: PASS")


fn test_checkpointed_matches_chain() raises:
    """Checkpointed backward matches regular for mul->add->relu->sum chain."""
    var tape1 = _build_chain_tape()
    var loss1 = tape1.num_variables() - 1
    run_backward(tape1, loss1)

    var tape2 = _build_chain_tape_copy()
    var loss2 = tape2.num_variables() - 1
    var segs = auto_checkpoint_segments(tape2, 2)
    run_backward_checkpointed(tape2, loss2, segs)

    # Compare all input variable grads (indices 0, 1, 2)
    assert_true(gradients_match(tape1, tape2, 0), "a grads mismatch in chain")
    assert_true(gradients_match(tape1, tape2, 1), "b grads mismatch in chain")
    assert_true(gradients_match(tape1, tape2, 2), "c grads mismatch in chain")
    print("  checkpointed_matches_chain: PASS")


fn test_checkpointed_matches_4_segments() raises:
    """Checkpointed with 4 segments (1 per entry) matches regular."""
    var tape1 = _build_chain_tape()
    var loss1 = tape1.num_variables() - 1
    run_backward(tape1, loss1)

    var tape2 = _build_chain_tape_copy()
    var loss2 = tape2.num_variables() - 1
    var segs = auto_checkpoint_segments(tape2, 4)
    run_backward_checkpointed(tape2, loss2, segs)

    assert_true(gradients_match(tape1, tape2, 0), "a grads mismatch 4-seg")
    assert_true(gradients_match(tape1, tape2, 1), "b grads mismatch 4-seg")
    assert_true(gradients_match(tape1, tape2, 2), "c grads mismatch 4-seg")
    print("  checkpointed_matches_4_segments: PASS")


fn test_checkpointed_matmul() raises:
    """Checkpointed backward matches for matmul chain."""
    # Build matmul + sum tape
    var d_a = List[Int]()
    d_a.append(2)
    d_a.append(3)
    var d_b = List[Int]()
    d_b.append(3)
    d_b.append(2)

    # Regular
    var tape1 = Tape(4096)
    var a1 = tape1.add_variable(d_a.copy())
    var b1 = tape1.add_variable(d_b.copy())
    tape1.set_data(a1, 0, Float32(1.0))
    tape1.set_data(a1, 1, Float32(2.0))
    tape1.set_data(a1, 2, Float32(3.0))
    tape1.set_data(a1, 3, Float32(4.0))
    tape1.set_data(a1, 4, Float32(5.0))
    tape1.set_data(a1, 5, Float32(6.0))
    tape1.set_data(b1, 0, Float32(1.0))
    tape1.set_data(b1, 1, Float32(0.0))
    tape1.set_data(b1, 2, Float32(0.0))
    tape1.set_data(b1, 3, Float32(1.0))
    tape1.set_data(b1, 4, Float32(1.0))
    tape1.set_data(b1, 5, Float32(1.0))
    var c1 = tracked_matmul(tape1, a1, b1, 2, 3, 2)
    var l1 = tracked_sum(tape1, c1)
    run_backward(tape1, l1)

    # Checkpointed
    var tape2 = Tape(4096)
    var a2 = tape2.add_variable(d_a.copy())
    var b2 = tape2.add_variable(d_b.copy())
    tape2.set_data(a2, 0, Float32(1.0))
    tape2.set_data(a2, 1, Float32(2.0))
    tape2.set_data(a2, 2, Float32(3.0))
    tape2.set_data(a2, 3, Float32(4.0))
    tape2.set_data(a2, 4, Float32(5.0))
    tape2.set_data(a2, 5, Float32(6.0))
    tape2.set_data(b2, 0, Float32(1.0))
    tape2.set_data(b2, 1, Float32(0.0))
    tape2.set_data(b2, 2, Float32(0.0))
    tape2.set_data(b2, 3, Float32(1.0))
    tape2.set_data(b2, 4, Float32(1.0))
    tape2.set_data(b2, 5, Float32(1.0))
    var c2 = tracked_matmul(tape2, a2, b2, 2, 3, 2)
    var l2 = tracked_sum(tape2, c2)
    var segs = auto_checkpoint_segments(tape2, 2)
    run_backward_checkpointed(tape2, l2, segs)

    assert_true(gradients_match(tape1, tape2, 0), "A grads mismatch matmul")
    assert_true(gradients_match(tape1, tape2, 1), "B grads mismatch matmul")
    print("  checkpointed_matmul: PASS")


fn test_empty_segments() raises:
    """Empty segment list falls back to regular backward."""
    var tape1 = Tape(4096)
    var d = List[Int]()
    d.append(3)
    var a1 = tape1.add_variable(d.copy())
    var b1 = tape1.add_variable(d.copy())
    tape1.set_data(a1, 0, Float32(1.0))
    tape1.set_data(a1, 1, Float32(2.0))
    tape1.set_data(a1, 2, Float32(3.0))
    tape1.set_data(b1, 0, Float32(4.0))
    tape1.set_data(b1, 1, Float32(5.0))
    tape1.set_data(b1, 2, Float32(6.0))
    var c1 = tracked_mul(tape1, a1, b1)
    var l1 = tracked_sum(tape1, c1)
    run_backward(tape1, l1)

    var tape2 = Tape(4096)
    var a2 = tape2.add_variable(d.copy())
    var b2 = tape2.add_variable(d.copy())
    tape2.set_data(a2, 0, Float32(1.0))
    tape2.set_data(a2, 1, Float32(2.0))
    tape2.set_data(a2, 2, Float32(3.0))
    tape2.set_data(b2, 0, Float32(4.0))
    tape2.set_data(b2, 1, Float32(5.0))
    tape2.set_data(b2, 2, Float32(6.0))
    var c2 = tracked_mul(tape2, a2, b2)
    var l2 = tracked_sum(tape2, c2)
    var segs = List[CheckpointSegment]()
    run_backward_checkpointed(tape2, l2, segs)

    assert_true(gradients_match(tape1, tape2, 0), "empty seg a mismatch")
    assert_true(gradients_match(tape1, tape2, 1), "empty seg b mismatch")
    print("  empty_segments: PASS")


fn test_saved_var_indices() raises:
    """auto_checkpoint_segments populates saved_var_indices correctly."""
    var tape = _build_chain_tape()
    var segs = auto_checkpoint_segments(tape, 2)
    # First segment (entries 0,1 = mul, add) inputs are var 0,1,2 (a,b,c)
    # But mul output (var 3) is in segment, add uses var 3 + var 2
    # Segment 0 inputs from outside: vars 0, 1, 2
    assert_true(len(segs[0].saved_var_indices) >= 2, "segment 0 should have external inputs")

    # Second segment (entries 2,3 = relu, sum) uses outputs from seg 0
    assert_true(len(segs[1].saved_var_indices) >= 1, "segment 1 should have external inputs")
    print("  saved_var_indices: PASS")


fn test_checkpointed_deep_chain() raises:
    """Checkpointed backward matches for deeper chain (5 ops)."""
    var d = List[Int]()
    d.append(4)

    # Regular
    var tape1 = Tape(8192)
    var x1 = tape1.add_variable(d.copy())
    var w1 = tape1.add_variable(d.copy())
    for i in range(4):
        tape1.set_data(x1, i, Float32(i + 1))
        tape1.set_data(w1, i, Float32(0.5 * (i + 1)))
    var m1 = tracked_mul(tape1, x1, w1)
    var a1 = tracked_relu(tape1, m1)
    var s1 = tracked_scalar_mul(tape1, a1, 2.0)
    var n1 = tracked_neg(tape1, s1)
    var l1 = tracked_mean(tape1, n1)
    run_backward(tape1, l1)

    # Checkpointed with 3 segments
    var tape2 = Tape(8192)
    var x2 = tape2.add_variable(d.copy())
    var w2 = tape2.add_variable(d.copy())
    for i in range(4):
        tape2.set_data(x2, i, Float32(i + 1))
        tape2.set_data(w2, i, Float32(0.5 * (i + 1)))
    var m2 = tracked_mul(tape2, x2, w2)
    var a2 = tracked_relu(tape2, m2)
    var s2 = tracked_scalar_mul(tape2, a2, 2.0)
    var n2 = tracked_neg(tape2, s2)
    var l2 = tracked_mean(tape2, n2)
    var segs = auto_checkpoint_segments(tape2, 3)
    run_backward_checkpointed(tape2, l2, segs)

    assert_true(gradients_match(tape1, tape2, 0), "x grads mismatch deep")
    assert_true(gradients_match(tape1, tape2, 1), "w grads mismatch deep")
    print("  checkpointed_deep_chain: PASS")


fn main() raises:
    print("test_checkpoint:")
    test_checkpoint_segment_basic()
    test_mark_checkpoint()
    test_auto_segments_even_split()
    test_auto_segments_more_than_entries()
    test_auto_segments_single()
    test_checkpointed_matches_regular_simple()
    test_checkpointed_matches_chain()
    test_checkpointed_matches_4_segments()
    test_checkpointed_matmul()
    test_empty_segments()
    test_saved_var_indices()
    test_checkpointed_deep_chain()
    print("ALL PASSED (12 tests)")
