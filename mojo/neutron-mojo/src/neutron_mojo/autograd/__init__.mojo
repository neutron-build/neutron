# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Autograd Package
# ===----------------------------------------------------------------------=== #

"""Reverse-mode automatic differentiation for training.

Core components:
- Variable: Lightweight handle into tape storage
- Tape: Flat storage for all variable data and gradients
- Tracked ops: Forward functions that record on the tape
- Backward: Reverse-walk the tape to compute gradients
- GradCheck: Numerical gradient verification
"""

from .variable import Variable

from .tape import (
    Tape,
    TapeEntry,
    OP_ADD,
    OP_MUL,
    OP_MATMUL,
    OP_RELU,
    OP_SIGMOID,
    OP_TANH,
    OP_EXP,
    OP_LOG,
    OP_SOFTMAX,
    OP_SUM,
    OP_MEAN,
    OP_SUB,
    OP_DIV,
    OP_POW,
    OP_SQRT,
    OP_NEG,
    OP_CLAMP,
    OP_SCALAR_MUL,
    OP_RMSNORM,
    OP_LAYERNORM,
    OP_GELU,
    OP_SILU,
    OP_SWIGLU,
    OP_RESHAPE,
    OP_TRANSPOSE,
    OP_CONCAT,
    OP_SPLIT,
    OP_LOG_SOFTMAX,
    OP_CROSS_ENTROPY,
    OP_MSE,
    OP_EMBEDDING,
    OP_SCALAR_ADD,
    OP_L1,
    OP_BCE,
    OP_KL_DIV,
)

from .ops import (
    tracked_add,
    tracked_sub,
    tracked_mul,
    tracked_div,
    tracked_matmul,
    tracked_relu,
    tracked_sigmoid,
    tracked_tanh,
    tracked_exp,
    tracked_log,
    tracked_neg,
    tracked_scalar_mul,
    tracked_scalar_add,
    tracked_softmax,
    tracked_sum,
    tracked_mean,
)

from .backward import run_backward

from .grad_check import GradCheckResult, compare_gradients

from .checkpoint import (
    CheckpointSegment,
    mark_checkpoint,
    auto_checkpoint_segments,
    run_backward_checkpointed,
    gradients_match,
)
