# ===----------------------------------------------------------------------=== #
# Neutron Mojo — SGD Optimizer
# ===----------------------------------------------------------------------=== #

"""Stochastic Gradient Descent with momentum and weight decay."""

from neutron_mojo.autograd.tape import Tape


struct SGD(Movable):
    """SGD optimizer with optional momentum and weight decay."""
    var lr: Float64
    var momentum: Float64
    var weight_decay: Float64
    var dampening: Float64
    var velocity: List[Float64]
    var initialized: Bool

    fn __init__(out self, lr: Float64 = 0.01, momentum: Float64 = 0.0,
                weight_decay: Float64 = 0.0, dampening: Float64 = 0.0):
        self.lr = lr
        self.momentum = momentum
        self.weight_decay = weight_decay
        self.dampening = dampening
        self.velocity = List[Float64]()
        self.initialized = False

    fn __moveinit__(out self, deinit other: Self):
        self.lr = other.lr
        self.momentum = other.momentum
        self.weight_decay = other.weight_decay
        self.dampening = other.dampening
        self.velocity = other.velocity^
        self.initialized = other.initialized

    fn step(mut self, mut tape: Tape, param_indices: List[Int]):
        """Perform one SGD update step."""
        # Initialize velocity on first call
        if not self.initialized:
            var total_params = 0
            for p in range(len(param_indices)):
                total_params += tape.var_numel(param_indices[p])
            self.velocity = List[Float64]()
            for i in range(total_params):
                self.velocity.append(0.0)
            self.initialized = True

        var vel_offset = 0
        for p in range(len(param_indices)):
            var idx = param_indices[p]
            var n = tape.var_numel(idx)
            var data_off = tape.var_offset(idx)
            var grad_off = data_off  # grad uses same offset in grad_flat

            var data = tape.data_flat.data_ptr()
            var grad = tape.grad_flat.data_ptr()

            for i in range(n):
                var g = Float64(grad.load(data_off + i))

                # Weight decay
                if self.weight_decay != 0.0:
                    g += self.weight_decay * Float64(data.load(data_off + i))

                # Momentum
                if self.momentum != 0.0:
                    var v = self.velocity[vel_offset + i]
                    v = self.momentum * v + (1.0 - self.dampening) * g
                    self.velocity[vel_offset + i] = v
                    g = v

                # Update
                data.store(data_off + i, Float32(Float64(data.load(data_off + i)) - self.lr * g))

            vel_offset += n
