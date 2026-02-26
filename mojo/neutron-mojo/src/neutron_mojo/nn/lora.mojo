# ===----------------------------------------------------------------------=== #
# Neutron Mojo — LoRA (Low-Rank Adaptation)
# ===----------------------------------------------------------------------=== #

"""Low-Rank Adaptation for efficient fine-tuning.

LoRA (Hu et al., 2021) adds a low-rank decomposition to frozen weight
matrices: W' = W + (alpha/rank) * B @ A, where A is [rank, in_dim]
and B is [out_dim, rank].

During inference, the LoRA update can be merged into the base weights
(merge_lora) or applied separately (lora_linear). Separate application
is useful when switching between multiple LoRA adapters.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import simd_dot, simd_matvec


# ===----------------------------------------------------------------------=== #
# LoRA Configuration
# ===----------------------------------------------------------------------=== #

struct LoRAConfig(Copyable, Movable):
    """Configuration for a LoRA adapter."""
    var rank: Int           # Low-rank dimension (typically 4, 8, 16, 32, 64)
    var alpha: Float32      # Scaling factor (typically same as rank or 2*rank)
    var in_features: Int    # Input dimension of base weight
    var out_features: Int   # Output dimension of base weight

    fn __init__(out self, rank: Int, alpha: Float32, in_features: Int, out_features: Int):
        self.rank = rank
        self.alpha = alpha
        self.in_features = in_features
        self.out_features = out_features

    fn __copyinit__(out self, existing: Self):
        self.rank = existing.rank
        self.alpha = existing.alpha
        self.in_features = existing.in_features
        self.out_features = existing.out_features

    fn __moveinit__(out self, deinit other: Self):
        self.rank = other.rank
        self.alpha = other.alpha
        self.in_features = other.in_features
        self.out_features = other.out_features

    fn scaling(self) -> Float32:
        """Compute the LoRA scaling factor: alpha / rank."""
        if self.rank > 0:
            return self.alpha / Float32(self.rank)
        return 0.0


# ===----------------------------------------------------------------------=== #
# LoRA Weight
# ===----------------------------------------------------------------------=== #

struct LoRAWeight(Movable):
    """Low-rank weight matrices A and B.

    A: [rank, in_features]  — initialized with small random or Kaiming
    B: [out_features, rank]  — initialized to zero (so LoRA starts as identity)

    The LoRA update is: delta_W = (alpha/rank) * B @ A
    Applied as: y = W @ x + (alpha/rank) * B @ (A @ x)
    """
    var lora_a: Tensor[DType.float32]   # [rank * in_features]
    var lora_b: Tensor[DType.float32]   # [out_features * rank]
    var config: LoRAConfig

    fn __init__(out self, config: LoRAConfig):
        self.config = config.copy()
        self.lora_a = Tensor[DType.float32](Shape(config.rank * config.in_features))
        self.lora_b = Tensor[DType.float32](Shape(config.out_features * config.rank))
        # B initialized to zero → LoRA initially contributes nothing

    fn __moveinit__(out self, deinit other: Self):
        self.lora_a = other.lora_a^
        self.lora_b = other.lora_b^
        self.config = other.config.copy()


# ===----------------------------------------------------------------------=== #
# LoRA Operations
# ===----------------------------------------------------------------------=== #

fn lora_forward(
    x: Tensor[DType.float32],
    lora: LoRAWeight,
) -> Tensor[DType.float32]:
    """Compute LoRA delta output: (alpha/rank) * B @ (A @ x).

    This is the additive correction, NOT the full output.
    Full output = base_linear(x) + lora_forward(x).

    Args:
        x: Input vector [in_features].
        lora: LoRA weight matrices.

    Returns:
        Delta output [out_features].
    """
    var rank = lora.config.rank
    var in_f = lora.config.in_features
    var out_f = lora.config.out_features
    var scale = lora.config.scaling()

    # Step 1: A @ x → [rank] (SIMD matvec)
    var hidden = Tensor[DType.float32](Shape(rank))
    simd_matvec(hidden, 0, lora.lora_a, 0, x, 0, rank, in_f)

    # Step 2: B @ hidden → [out_features] (SIMD matvec)
    var output = Tensor[DType.float32](Shape(out_f))
    simd_matvec(output, 0, lora.lora_b, 0, hidden, 0, out_f, rank)

    # Apply scaling
    if scale != 1.0:
        var o_ptr = output.data_ptr()
        for i in range(out_f):
            o_ptr[i] = o_ptr[i] * scale

    return output^


fn lora_linear(
    x: Tensor[DType.float32],
    base_weight: Tensor[DType.float32],
    lora: LoRAWeight,
) -> Tensor[DType.float32]:
    """Linear projection with LoRA: y = W @ x + (alpha/rank) * B @ A @ x.

    Args:
        x: Input vector [in_features].
        base_weight: Base weight matrix [out_features * in_features].
        lora: LoRA adapter weights.

    Returns:
        Output [out_features].
    """
    var in_f = lora.config.in_features
    var out_f = lora.config.out_features

    # Base: W @ x (SIMD matvec)
    var output = Tensor[DType.float32](Shape(out_f))
    simd_matvec(output, 0, base_weight, 0, x, 0, out_f, in_f)

    # LoRA delta (already SIMD-accelerated)
    var delta = lora_forward(x, lora)
    for i in range(out_f):
        output.set(i, output.get(i) + delta.get(i))

    return output^


fn merge_lora(
    mut base_weight: Tensor[DType.float32],
    lora: LoRAWeight,
):
    """Merge LoRA weights into base weight matrix permanently.

    After merging: W_merged = W + (alpha/rank) * B @ A
    This avoids runtime overhead of separate LoRA computation.

    Args:
        base_weight: Base weight [out_features * in_features], modified in-place.
        lora: LoRA adapter to merge.
    """
    var rank = lora.config.rank
    var in_f = lora.config.in_features
    var out_f = lora.config.out_features
    var scale = lora.config.scaling()

    # Compute B @ A and add scaled result to base_weight
    for i in range(out_f):
        for j in range(in_f):
            var delta: Float32 = 0.0
            for r in range(rank):
                delta += lora.lora_b.get(i * rank + r) * lora.lora_a.get(r * in_f + j)
            var idx = i * in_f + j
            base_weight.set(idx, base_weight.get(idx) + scale * delta)


fn unmerge_lora(
    mut base_weight: Tensor[DType.float32],
    lora: LoRAWeight,
):
    """Remove merged LoRA weights from base weight matrix.

    Reverses merge_lora: W_original = W_merged - (alpha/rank) * B @ A

    Args:
        base_weight: Merged weight [out_features * in_features], modified in-place.
        lora: LoRA adapter to remove.
    """
    var rank = lora.config.rank
    var in_f = lora.config.in_features
    var out_f = lora.config.out_features
    var scale = lora.config.scaling()

    for i in range(out_f):
        for j in range(in_f):
            var delta: Float32 = 0.0
            for r in range(rank):
                delta += lora.lora_b.get(i * rank + r) * lora.lora_a.get(r * in_f + j)
            var idx = i * in_f + j
            base_weight.set(idx, base_weight.get(idx) - scale * delta)
