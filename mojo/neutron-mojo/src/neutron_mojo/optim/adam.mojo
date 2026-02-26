# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Adam/AdamW Optimizer
# ===----------------------------------------------------------------------=== #

"""Adam optimizer with optional decoupled weight decay (AdamW)."""

from math import sqrt

from neutron_mojo.autograd.tape import Tape


struct Adam(Movable):
    """Adam optimizer with bias correction and optional weight decay (AdamW)."""
    var lr: Float64
    var beta1: Float64
    var beta2: Float64
    var eps: Float64
    var weight_decay: Float64
    var m: List[Float64]       # First moment
    var v: List[Float64]       # Second moment
    var step_count: Int
    var initialized: Bool

    fn __init__(out self, lr: Float64 = 1e-3, beta1: Float64 = 0.9,
                beta2: Float64 = 0.999, eps: Float64 = 1e-8,
                weight_decay: Float64 = 0.0):
        self.lr = lr
        self.beta1 = beta1
        self.beta2 = beta2
        self.eps = eps
        self.weight_decay = weight_decay
        self.m = List[Float64]()
        self.v = List[Float64]()
        self.step_count = 0
        self.initialized = False

    fn __moveinit__(out self, deinit other: Self):
        self.lr = other.lr
        self.beta1 = other.beta1
        self.beta2 = other.beta2
        self.eps = other.eps
        self.weight_decay = other.weight_decay
        self.m = other.m^
        self.v = other.v^
        self.step_count = other.step_count
        self.initialized = other.initialized

    fn step(mut self, mut tape: Tape, param_indices: List[Int]):
        """Perform one Adam update step."""
        self.step_count += 1

        # Initialize m/v on first call
        if not self.initialized:
            var total_params = 0
            for p in range(len(param_indices)):
                total_params += tape.var_numel(param_indices[p])
            self.m = List[Float64]()
            self.v = List[Float64]()
            for i in range(total_params):
                self.m.append(0.0)
                self.v.append(0.0)
            self.initialized = True

        # Bias correction
        from math import pow
        var bc1 = 1.0 - pow(self.beta1, Float64(self.step_count))
        var bc2 = 1.0 - pow(self.beta2, Float64(self.step_count))

        var buf_offset = 0
        for p in range(len(param_indices)):
            var idx = param_indices[p]
            var n = tape.var_numel(idx)
            var data_off = tape.var_offset(idx)

            var data = tape.data_flat.data_ptr()
            var grad = tape.grad_flat.data_ptr()

            for i in range(n):
                var g = Float64(grad.load(data_off + i))
                var param_val = Float64(data.load(data_off + i))

                # Decoupled weight decay (AdamW)
                if self.weight_decay != 0.0:
                    param_val -= self.lr * self.weight_decay * param_val

                # Update biased first moment
                var mi = self.beta1 * self.m[buf_offset + i] + (1.0 - self.beta1) * g
                self.m[buf_offset + i] = mi

                # Update biased second moment
                var vi = self.beta2 * self.v[buf_offset + i] + (1.0 - self.beta2) * g * g
                self.v[buf_offset + i] = vi

                # Bias-corrected estimates
                var m_hat = mi / bc1
                var v_hat = vi / bc2

                # Parameter update
                param_val -= self.lr * m_hat / (sqrt(v_hat) + self.eps)
                data.store(data_off + i, Float32(param_val))

            buf_offset += n
