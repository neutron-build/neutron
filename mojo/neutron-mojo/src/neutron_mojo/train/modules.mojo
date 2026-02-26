# ===----------------------------------------------------------------------=== #
# Neutron Mojo — NN Modules for Training
# ===----------------------------------------------------------------------=== #

"""PyTorch-like trainable layers.

All modules are Copyable (store tape indices as Int, not Tensors).
This enables List[TrainableTransformerBlock] pattern.
"""

from math import sqrt
from random import random_float64

from neutron_mojo.autograd.tape import Tape, TapeEntry, OP_EMBEDDING
from neutron_mojo.autograd.ops import (
    tracked_add, tracked_matmul, tracked_relu, tracked_sigmoid,
    tracked_scalar_mul,
)


struct Linear(ImplicitlyCopyable, Copyable, Movable):
    """Fully connected linear layer: y = x @ W^T + b.

    Stores weight and bias as tape variable indices.
    Uses Xavier initialization.
    """
    var weight_idx: Int
    var bias_idx: Int
    var in_features: Int
    var out_features: Int
    var has_bias: Bool
    var registered: Bool

    fn __init__(out self, in_features: Int, out_features: Int, has_bias: Bool = True):
        self.in_features = in_features
        self.out_features = out_features
        self.has_bias = has_bias
        self.weight_idx = -1
        self.bias_idx = -1
        self.registered = False

    fn __copyinit__(out self, other: Self):
        self.weight_idx = other.weight_idx
        self.bias_idx = other.bias_idx
        self.in_features = other.in_features
        self.out_features = other.out_features
        self.has_bias = other.has_bias
        self.registered = other.registered

    fn __moveinit__(out self, deinit other: Self):
        self.weight_idx = other.weight_idx
        self.bias_idx = other.bias_idx
        self.in_features = other.in_features
        self.out_features = other.out_features
        self.has_bias = other.has_bias
        self.registered = other.registered

    fn register(mut self, mut tape: Tape):
        """Register parameters on the tape with Xavier init."""
        var w_dims = List[Int]()
        w_dims.append(self.out_features)
        w_dims.append(self.in_features)
        self.weight_idx = tape.add_variable(w_dims^, requires_grad=True)

        # Xavier initialization: scale = sqrt(2 / (in + out))
        var scale = sqrt(2.0 / Float64(self.in_features + self.out_features))
        var n = self.in_features * self.out_features
        for i in range(n):
            var val = Float32((random_float64() * 2.0 - 1.0) * scale)
            tape.set_data(self.weight_idx, i, val)

        if self.has_bias:
            var b_dims = List[Int]()
            b_dims.append(self.out_features)
            self.bias_idx = tape.add_variable(b_dims^, requires_grad=True)
            # Bias initialized to zeros (already default)

        self.registered = True

    fn forward(self, mut tape: Tape, x_idx: Int) -> Int:
        """Forward pass: y = x @ W^T (+ b).

        x_idx points to a variable of shape (in_features,) or (batch, in_features).
        W is (out_features, in_features).
        """
        var x_numel = tape.var_numel(x_idx)
        var x_shape = tape.var_shapes[x_idx].copy()

        if len(x_shape) == 1:
            # Single vector: x (in,) @ W^T (in, out) -> (out,)
            # Treat as matmul: (1, in) @ (in, out) -> (1, out)
            var y_idx = tracked_matmul(tape, x_idx, self.weight_idx,
                1, self.in_features, self.out_features)
            # Note: matmul stores (1, out_features) but we want to use it as-is
            if self.has_bias and self.bias_idx >= 0:
                y_idx = tracked_add(tape, y_idx, self.bias_idx)
            return y_idx
        else:
            # Batch: (batch, in) @ (in, out) -> (batch, out)
            var batch = x_shape[0]
            var y_idx = tracked_matmul(tape, x_idx, self.weight_idx,
                batch, self.in_features, self.out_features)
            if self.has_bias and self.bias_idx >= 0:
                # Broadcasting add: need to add bias to each row
                # For now, add element-wise (bias is broadcast)
                y_idx = tracked_add(tape, y_idx, self.bias_idx)
            return y_idx

    fn param_indices(self) -> List[Int]:
        """Return list of parameter variable indices."""
        var params = List[Int]()
        if self.weight_idx >= 0:
            params.append(self.weight_idx)
        if self.has_bias and self.bias_idx >= 0:
            params.append(self.bias_idx)
        return params^


struct Embedding(ImplicitlyCopyable, Copyable, Movable):
    """Embedding lookup table with gradient support."""
    var embed_idx: Int
    var num_embeddings: Int
    var embedding_dim: Int
    var registered: Bool

    fn __init__(out self, num_embeddings: Int, embedding_dim: Int):
        self.num_embeddings = num_embeddings
        self.embedding_dim = embedding_dim
        self.embed_idx = -1
        self.registered = False

    fn __copyinit__(out self, other: Self):
        self.embed_idx = other.embed_idx
        self.num_embeddings = other.num_embeddings
        self.embedding_dim = other.embedding_dim
        self.registered = other.registered

    fn __moveinit__(out self, deinit other: Self):
        self.embed_idx = other.embed_idx
        self.num_embeddings = other.num_embeddings
        self.embedding_dim = other.embedding_dim
        self.registered = other.registered

    fn register(mut self, mut tape: Tape):
        """Register the embedding table."""
        var dims = List[Int]()
        dims.append(self.num_embeddings)
        dims.append(self.embedding_dim)
        self.embed_idx = tape.add_variable(dims^, requires_grad=True)

        # Initialize with small random values
        var n = self.num_embeddings * self.embedding_dim
        var scale = 1.0 / sqrt(Float64(self.embedding_dim))
        for i in range(n):
            tape.set_data(self.embed_idx, i, Float32((random_float64() * 2.0 - 1.0) * scale))
        self.registered = True

    fn forward(self, mut tape: Tape, token_id: Int) -> Int:
        """Look up embedding for a single token."""
        var dims = List[Int]()
        dims.append(self.embedding_dim)
        var y_idx = tape.add_variable(dims^, requires_grad=True)

        var row_off = token_id * self.embedding_dim
        for d in range(self.embedding_dim):
            tape.set_data(y_idx, d, tape.get_data(self.embed_idx, row_off + d))

        tape.record(TapeEntry(OP_EMBEDDING(), self.embed_idx, -1, y_idx,
            cached_int=self.embedding_dim, cached_int2=token_id))
        return y_idx

    fn param_indices(self) -> List[Int]:
        var params = List[Int]()
        if self.embed_idx >= 0:
            params.append(self.embed_idx)
        return params^


struct RMSNormModule(ImplicitlyCopyable, Copyable, Movable):
    """Learnable RMSNorm: (x / rms) * gamma."""
    var gamma_idx: Int
    var dim: Int
    var eps: Float64
    var registered: Bool

    fn __init__(out self, dim: Int, eps: Float64 = 1e-6):
        self.dim = dim
        self.eps = eps
        self.gamma_idx = -1
        self.registered = False

    fn __copyinit__(out self, other: Self):
        self.gamma_idx = other.gamma_idx
        self.dim = other.dim
        self.eps = other.eps
        self.registered = other.registered

    fn __moveinit__(out self, deinit other: Self):
        self.gamma_idx = other.gamma_idx
        self.dim = other.dim
        self.eps = other.eps
        self.registered = other.registered

    fn register(mut self, mut tape: Tape):
        """Register gamma parameter (initialized to ones)."""
        var dims = List[Int]()
        dims.append(self.dim)
        self.gamma_idx = tape.add_variable(dims^, requires_grad=True)
        for i in range(self.dim):
            tape.set_data(self.gamma_idx, i, Float32(1.0))
        self.registered = True

    fn forward(self, mut tape: Tape, x_idx: Int) -> Int:
        """RMSNorm forward."""
        var n = tape.var_numel(x_idx)
        var dims = List[Int]()
        dims.append(n)
        var y_idx = tape.add_variable(dims^, requires_grad=True)

        # Compute RMS
        var sum_sq = Float64(0.0)
        for i in range(n):
            var v = Float64(tape.get_data(x_idx, i))
            sum_sq += v * v
        var rms = sqrt(sum_sq / Float64(n) + self.eps)

        for i in range(n):
            var x_val = Float64(tape.get_data(x_idx, i))
            var g_val = Float64(tape.get_data(self.gamma_idx, i))
            tape.set_data(y_idx, i, Float32((x_val / rms) * g_val))

        from neutron_mojo.autograd.tape import OP_RMSNORM
        tape.record(TapeEntry(OP_RMSNORM(), x_idx, self.gamma_idx, y_idx,
            cached_scalar=self.eps, cached_int=n))
        return y_idx

    fn param_indices(self) -> List[Int]:
        var params = List[Int]()
        if self.gamma_idx >= 0:
            params.append(self.gamma_idx)
        return params^


struct LayerNormModule(ImplicitlyCopyable, Copyable, Movable):
    """Learnable LayerNorm: ((x - mean) / std) * gamma + beta."""
    var gamma_idx: Int
    var beta_idx: Int
    var dim: Int
    var eps: Float64
    var registered: Bool

    fn __init__(out self, dim: Int, eps: Float64 = 1e-5):
        self.dim = dim
        self.eps = eps
        self.gamma_idx = -1
        self.beta_idx = -1
        self.registered = False

    fn __copyinit__(out self, other: Self):
        self.gamma_idx = other.gamma_idx
        self.beta_idx = other.beta_idx
        self.dim = other.dim
        self.eps = other.eps
        self.registered = other.registered

    fn __moveinit__(out self, deinit other: Self):
        self.gamma_idx = other.gamma_idx
        self.beta_idx = other.beta_idx
        self.dim = other.dim
        self.eps = other.eps
        self.registered = other.registered

    fn register(mut self, mut tape: Tape):
        """Register gamma (ones) and beta (zeros) parameters."""
        var dims = List[Int]()
        dims.append(self.dim)
        self.gamma_idx = tape.add_variable(dims.copy(), requires_grad=True)
        for i in range(self.dim):
            tape.set_data(self.gamma_idx, i, Float32(1.0))
        self.beta_idx = tape.add_variable(dims^, requires_grad=True)
        # beta already zeros
        self.registered = True

    fn forward(self, mut tape: Tape, x_idx: Int) -> Int:
        """LayerNorm forward."""
        var n = tape.var_numel(x_idx)
        var dims = List[Int]()
        dims.append(n)
        var y_idx = tape.add_variable(dims^, requires_grad=True)

        var sum_val = Float64(0.0)
        for i in range(n):
            sum_val += Float64(tape.get_data(x_idx, i))
        var mean = sum_val / Float64(n)

        var sum_sq = Float64(0.0)
        for i in range(n):
            var diff = Float64(tape.get_data(x_idx, i)) - mean
            sum_sq += diff * diff
        var variance = sum_sq / Float64(n)
        var std_inv = 1.0 / sqrt(variance + self.eps)

        for i in range(n):
            var normed = (Float64(tape.get_data(x_idx, i)) - mean) * std_inv
            var scaled = normed * Float64(tape.get_data(self.gamma_idx, i)) + Float64(tape.get_data(self.beta_idx, i))
            tape.set_data(y_idx, i, Float32(scaled))

        from neutron_mojo.autograd.tape import OP_LAYERNORM
        tape.record(TapeEntry(OP_LAYERNORM(), x_idx, self.gamma_idx, y_idx,
            cached_scalar=self.eps, cached_int=n, cached_int3=self.beta_idx))
        return y_idx

    fn param_indices(self) -> List[Int]:
        var params = List[Int]()
        if self.gamma_idx >= 0:
            params.append(self.gamma_idx)
        if self.beta_idx >= 0:
            params.append(self.beta_idx)
        return params^


struct Dropout(Copyable, Movable):
    """Dropout layer (training mode only)."""
    var p: Float64
    var training: Bool

    fn __init__(out self, p: Float64 = 0.1):
        self.p = p
        self.training = True

    fn __copyinit__(out self, other: Self):
        self.p = other.p
        self.training = other.training

    fn __moveinit__(out self, deinit other: Self):
        self.p = other.p
        self.training = other.training

    fn forward(self, mut tape: Tape, x_idx: Int) -> Int:
        """Apply dropout: randomly zero elements with probability p."""
        if not self.training or self.p == 0.0:
            return x_idx

        var n = tape.var_numel(x_idx)
        var dims = List[Int]()
        var shape = tape.var_shapes[x_idx].copy()
        for i in range(len(shape)):
            dims.append(shape[i])
        var y_idx = tape.add_variable(dims^, requires_grad=True)

        var scale = Float32(1.0 / (1.0 - self.p))
        for i in range(n):
            if random_float64() < self.p:
                tape.set_data(y_idx, i, Float32(0.0))
            else:
                tape.set_data(y_idx, i, tape.get_data(x_idx, i) * scale)

        # Record as scalar_mul for simplified backward
        from neutron_mojo.autograd.tape import OP_SCALAR_MUL
        tape.record(TapeEntry(OP_SCALAR_MUL(), x_idx, -1, y_idx, cached_scalar=1.0 / (1.0 - self.p)))
        return y_idx

    fn eval_mode(mut self):
        self.training = False

    fn train_mode(mut self):
        self.training = True
