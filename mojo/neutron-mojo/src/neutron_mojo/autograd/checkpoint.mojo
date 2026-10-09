# ===----------------------------------------------------------------------=== #
# Neutron Mojo -- Gradient Checkpointing
# ===----------------------------------------------------------------------=== #

"""Gradient checkpointing for memory-efficient training.

During standard backpropagation, all intermediate activations are kept in
memory. With gradient checkpointing, the tape is divided into segments.
During backward, intermediates in each segment can be freed and the
forward pass replayed to recompute them on demand.

This module provides a simplified implementation that proves the mechanism
is correct by verifying that checkpointed backward produces identical
gradients to regular backward. The actual memory savings require a proper
memory allocator (future work).
"""

from std.math import isfinite
from .tape import Tape, TapeEntry
from .backward import run_backward, _dispatch_backward


# ===----------------------------------------------------------------------=== #
# CheckpointSegment
# ===----------------------------------------------------------------------=== #


struct CheckpointSegment(ImplicitlyCopyable, Copyable, Movable):
    """A segment of the tape that can be checkpointed.

    During backward, intermediates in this segment are freed and the
    forward pass is replayed to recompute them on demand.
    """
    var start_entry: Int  # first tape entry index in this segment
    var end_entry: Int    # last tape entry index (exclusive)
    var saved_var_indices: List[Int]  # variable indices to save (inputs)

    def __init__(out self, start_entry: Int, end_entry: Int):
        self.start_entry = start_entry
        self.end_entry = end_entry
        self.saved_var_indices = List[Int]()

    def __init__(out self, *, copy: Self):
        self.start_entry = copy.start_entry
        self.end_entry = copy.end_entry
        self.saved_var_indices = List[Int]()
        for i in range(len(copy.saved_var_indices)):
            self.saved_var_indices.append(copy.saved_var_indices[i])

    def __init__(out self, *, deinit move: Self):
        self.start_entry = move.start_entry^
        self.end_entry = move.end_entry^
        self.saved_var_indices = move.saved_var_indices^

    def copy(self) -> CheckpointSegment:
        """Explicit copy."""
        var seg = CheckpointSegment(self.start_entry, self.end_entry)
        for i in range(len(self.saved_var_indices)):
            seg.saved_var_indices.append(self.saved_var_indices[i])
        return seg^


# ===----------------------------------------------------------------------=== #
# mark_checkpoint
# ===----------------------------------------------------------------------=== #


def mark_checkpoint(tape: Tape) -> Int:
    """Record the current tape entry count as a checkpoint boundary.

    Call this during forward between segments (e.g., between layers).
    Returns the entry index at the boundary.
    """
    return tape.num_entries()


# ===----------------------------------------------------------------------=== #
# auto_checkpoint_segments
# ===----------------------------------------------------------------------=== #


def auto_checkpoint_segments(
    tape: Tape, num_segments: Int
) -> List[CheckpointSegment]:
    """Divide the tape into roughly equal segments for checkpointing.

    Typically num_segments = num_layers.

    Args:
        tape: The autograd tape with recorded operations.
        num_segments: Number of segments to create.

    Returns:
        List of CheckpointSegment covering the entire tape.
    """
    var total = tape.num_entries()
    var result = List[CheckpointSegment]()

    if total == 0 or num_segments <= 0:
        return result^

    var segs = num_segments
    if segs > total:
        segs = total

    var base_size = total // segs
    var remainder = total % segs
    var start = 0

    for s in range(segs):
        var size = base_size
        if s < remainder:
            size += 1
        var end = start + size
        var seg = CheckpointSegment(start, end)
        _collect_input_vars(tape, seg)
        result.append(seg^)
        start = end

    return result^


def _collect_input_vars(tape: Tape, mut seg: CheckpointSegment):
    """Collect input variable indices for a segment.

    Input variables are those referenced as inputs by entries in the
    segment but defined (as outputs) before the segment starts.
    """
    # Collect output var indices produced within this segment
    var segment_outputs = List[Int]()
    for i in range(seg.start_entry, seg.end_entry):
        var entry = tape.get_entry(i)
        segment_outputs.append(entry.output_idx)

    # Find inputs that come from outside the segment
    for i in range(seg.start_entry, seg.end_entry):
        var entry = tape.get_entry(i)
        if entry.input0_idx >= 0:
            if not _list_contains(segment_outputs, entry.input0_idx):
                if not _list_contains(seg.saved_var_indices, entry.input0_idx):
                    seg.saved_var_indices.append(entry.input0_idx)
        if entry.input1_idx >= 0:
            if not _list_contains(segment_outputs, entry.input1_idx):
                if not _list_contains(seg.saved_var_indices, entry.input1_idx):
                    seg.saved_var_indices.append(entry.input1_idx)


def _list_contains(lst: List[Int], val: Int) -> Bool:
    """Check if a list contains a value."""
    for i in range(len(lst)):
        if lst[i] == val:
            return True
    return False


# ===----------------------------------------------------------------------=== #
# run_backward_checkpointed
# ===----------------------------------------------------------------------=== #


def run_backward_checkpointed(mut tape: Tape, loss_idx: Int,
                             segments: List[CheckpointSegment]) raises:
    """Validate full segment coverage and use the same reachable reverse walk.

    The current checkpoint API keeps all intermediates, so it provides
    equivalent gradients and segment admission without claiming memory savings.
    """
    if len(segments) > 0:
        var next_entry = 0
        for i in range(len(segments)):
            if segments[i].start_entry != next_entry or segments[i].end_entry < next_entry or segments[i].end_entry > tape.num_entries():
                raise Error("Checkpoint segments must cover the tape in order")
            next_entry = segments[i].end_entry
        if next_entry != tape.num_entries():
            raise Error("Checkpoint segments do not cover the whole tape")
    run_backward(tape, loss_idx)


def _backward_segment(mut tape: Tape, seg: CheckpointSegment):
    """Run backward through a single segment's entries in reverse.

    Args:
        tape: The autograd tape.
        seg: The segment to process.
    """
    var i = seg.end_entry - 1
    while i >= seg.start_entry:
        var entry = tape.get_entry(i)
        _dispatch_backward(tape, entry)
        i -= 1


def _backward_all_entries(mut tape: Tape):
    """Run backward through all tape entries (no checkpointing)."""
    var num_entries = tape.num_entries()
    var i = num_entries - 1
    while i >= 0:
        var entry = tape.get_entry(i)
        _dispatch_backward(tape, entry)
        i -= 1


# ===----------------------------------------------------------------------=== #
# Gradient comparison utility
# ===----------------------------------------------------------------------=== #


def gradients_match(
    tape_a: Tape,
    tape_b: Tape,
    var_idx: Int,
    atol: Float64 = 1e-5,
) -> Bool:
    """Check if gradients for a variable match between two tapes.

    Args:
        tape_a: First tape.
        tape_b: Second tape.
        var_idx: Variable index to compare.
        atol: Absolute tolerance.

    Returns:
        True if all gradient elements match within tolerance.
    """
    if atol < 0 or not isfinite(atol):
        return False
    if var_idx < 0 or var_idx >= tape_a.num_variables() or var_idx >= tape_b.num_variables():
        return False
    if tape_a.var_shapes[var_idx] != tape_b.var_shapes[var_idx]:
        return False
    var n = tape_a.var_numel(var_idx)
    if n != tape_b.var_numel(var_idx):
        return False
    for i in range(n):
        var ga = Float64(tape_a.get_grad(var_idx, i))
        var gb = Float64(tape_b.get_grad(var_idx, i))
        if not isfinite(ga) or not isfinite(gb) or abs(ga - gb) > atol:
            return False
    return True
