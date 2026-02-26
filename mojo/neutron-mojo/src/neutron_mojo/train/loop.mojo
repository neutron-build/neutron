# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Training Loop Utilities
# ===----------------------------------------------------------------------=== #

"""High-level training orchestration: config, state, metrics."""

from math import exp, log, sqrt, cos


struct TrainingConfig(Copyable, Movable):
    """Configuration for training."""
    var epochs: Int
    var batch_size: Int
    var lr: Float64
    var grad_accumulation_steps: Int
    var log_interval: Int
    var eval_interval: Int
    var save_interval: Int
    var max_grad_norm: Float64
    var use_adam: Bool
    var warmup_steps: Int
    var weight_decay: Float64

    fn __init__(out self):
        self.epochs = 10
        self.batch_size = 4
        self.lr = 1e-3
        self.grad_accumulation_steps = 1
        self.log_interval = 10
        self.eval_interval = 100
        self.save_interval = 500
        self.max_grad_norm = 1.0
        self.use_adam = True
        self.warmup_steps = 100
        self.weight_decay = 0.01

    fn __copyinit__(out self, other: Self):
        self.epochs = other.epochs
        self.batch_size = other.batch_size
        self.lr = other.lr
        self.grad_accumulation_steps = other.grad_accumulation_steps
        self.log_interval = other.log_interval
        self.eval_interval = other.eval_interval
        self.save_interval = other.save_interval
        self.max_grad_norm = other.max_grad_norm
        self.use_adam = other.use_adam
        self.warmup_steps = other.warmup_steps
        self.weight_decay = other.weight_decay

    fn __moveinit__(out self, deinit other: Self):
        self.epochs = other.epochs
        self.batch_size = other.batch_size
        self.lr = other.lr
        self.grad_accumulation_steps = other.grad_accumulation_steps
        self.log_interval = other.log_interval
        self.eval_interval = other.eval_interval
        self.save_interval = other.save_interval
        self.max_grad_norm = other.max_grad_norm
        self.use_adam = other.use_adam
        self.warmup_steps = other.warmup_steps
        self.weight_decay = other.weight_decay

    fn copy(self) -> TrainingConfig:
        var c = TrainingConfig()
        c.epochs = self.epochs
        c.batch_size = self.batch_size
        c.lr = self.lr
        c.grad_accumulation_steps = self.grad_accumulation_steps
        c.log_interval = self.log_interval
        c.eval_interval = self.eval_interval
        c.save_interval = self.save_interval
        c.max_grad_norm = self.max_grad_norm
        c.use_adam = self.use_adam
        c.warmup_steps = self.warmup_steps
        c.weight_decay = self.weight_decay
        return c^


struct TrainingState(Copyable, Movable):
    """Mutable training state."""
    var current_epoch: Int
    var global_step: Int
    var total_loss: Float64
    var loss_count: Int
    var best_eval_loss: Float64

    fn __init__(out self):
        self.current_epoch = 0
        self.global_step = 0
        self.total_loss = 0.0
        self.loss_count = 0
        self.best_eval_loss = 1e10

    fn __copyinit__(out self, other: Self):
        self.current_epoch = other.current_epoch
        self.global_step = other.global_step
        self.total_loss = other.total_loss
        self.loss_count = other.loss_count
        self.best_eval_loss = other.best_eval_loss

    fn __moveinit__(out self, deinit other: Self):
        self.current_epoch = other.current_epoch
        self.global_step = other.global_step
        self.total_loss = other.total_loss
        self.loss_count = other.loss_count
        self.best_eval_loss = other.best_eval_loss

    fn avg_loss(self) -> Float64:
        if self.loss_count == 0:
            return 0.0
        return self.total_loss / Float64(self.loss_count)

    fn record_loss(mut self, loss: Float64):
        self.total_loss += loss
        self.loss_count += 1
        self.global_step += 1

    fn reset_running_loss(mut self):
        self.total_loss = 0.0
        self.loss_count = 0


struct TrainingMetrics(Copyable, Movable):
    """Training metrics history."""
    var loss_history: List[Float64]
    var lr_history: List[Float64]

    fn __init__(out self):
        self.loss_history = List[Float64]()
        self.lr_history = List[Float64]()

    fn __copyinit__(out self, other: Self):
        self.loss_history = List[Float64]()
        for i in range(len(other.loss_history)):
            self.loss_history.append(other.loss_history[i])
        self.lr_history = List[Float64]()
        for i in range(len(other.lr_history)):
            self.lr_history.append(other.lr_history[i])

    fn __moveinit__(out self, deinit other: Self):
        self.loss_history = other.loss_history^
        self.lr_history = other.lr_history^

    fn record(mut self, loss: Float64, lr: Float64):
        self.loss_history.append(loss)
        self.lr_history.append(lr)

    fn last_loss(self) -> Float64:
        if len(self.loss_history) == 0:
            return 0.0
        return self.loss_history[len(self.loss_history) - 1]

    fn perplexity(self) -> Float64:
        """Compute perplexity from last loss (exp(loss))."""
        var l = self.last_loss()
        if l > 20.0:
            return 1e10
        return exp(l)

    fn num_records(self) -> Int:
        return len(self.loss_history)


fn estimate_training_memory(model_params: Int, config: TrainingConfig) -> Int:
    """Estimate total training memory in bytes.

    Accounts for: params, gradients, optimizer state (Adam: m + v).
    """
    var bytes_per_param = 4  # float32
    var param_bytes = model_params * bytes_per_param
    var grad_bytes = param_bytes  # same size
    var optimizer_bytes = param_bytes * 2 if config.use_adam else 0  # m + v
    var activation_estimate = model_params * bytes_per_param  # rough estimate
    return param_bytes + grad_bytes + optimizer_bytes + activation_estimate
