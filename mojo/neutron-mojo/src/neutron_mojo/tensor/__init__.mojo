# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Tensor primitives
# ===----------------------------------------------------------------------=== #

"""Tensor primitives: dtype, dim, shape, storage, view, tensor, ops."""

# Layer 0 — no internal deps
from .dtype import (
    QuantConfig,
    NF4_CONFIG,
    Q4_K_CONFIG,
    Q8_0_CONFIG,
    bitwidth_of,
    is_floating_point,
    is_integer,
    is_signed,
    can_cast,
    optimal_simd_width,
    dtype_to_dlpack_code,
    dlpack_code_to_dtype,
)
from .dim import Dim, Batch, Seq, Hidden, Vocab, Heads, HeadDim, Dynamic
from .shape import Shape

# Layer 1 — depends on Layer 0
from .storage import DeviceKind, Storage
from .view import TensorView

# Layer 2 — depends on Layer 0+1
from .tensor import Tensor
from .ops import (
    add,
    sub,
    mul,
    div,
    matmul,
    relu,
    softmax,
    reduce_sum,
    reduce_max,
    reduce_mean,
    sum_all,
    max_all,
    rmsnorm,
    layernorm,
    gelu,
    silu,
    swiglu,
    neg,
    abs_val,
    exp_op,
    log_op,
    sqrt_op,
    sigmoid,
    tanh_op,
    pow_scalar,
    clamp,
    scalar_mul,
    scalar_add,
    ArgResult,
    argmax_tensor,
    argmin_tensor,
    argmax_axis,
    topk,
    where_op,
    gather,
    index_select,
    eq,
    ne,
    gt,
    lt,
    ge,
    le,
)

# SIMD primitives for hot-path acceleration
from .simd_math import (
    simd_dot,
    simd_matvec,
    simd_rmsnorm,
    simd_softmax,
    simd_silu,
    simd_swiglu,
    simd_axpy,
    par_simd_matvec,
    simd_q8_matvec,
    tiled_simd_matvec,
    par_tiled_simd_matvec,
    simd_attention_scores,
    simd_attention_weighted_sum,
    simd_online_softmax_attention,
    simd_batch_matvec,
    simd_batch_rmsnorm,
    simd_batch_swiglu,
    simd_batch_add,
)

# Shape manipulation & creation ops
from .shape_ops import (
    concat2,
    concat3,
    concat4,
    SplitResult2,
    SplitResult3,
    split2,
    split3,
    squeeze,
    unsqueeze,
    flatten,
    expand,
    arange,
    linspace,
    eye,
    tril,
    triu,
    SortResult,
    sort,
    argsort,
)
