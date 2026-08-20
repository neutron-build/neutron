# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Learning Rate Scheduler
# ===----------------------------------------------------------------------=== #

"""Learning rate scheduling: constant, cosine, linear warmup, step decay."""

from std.math import cos


struct LRScheduler(Copyable, Movable, ImplicitlyCopyable):
    """Learning rate scheduler with multiple strategies."""
    var base_lr: Float64
    var warmup_steps: Int
    var total_steps: Int
    var schedule_type: Int  # 0=constant, 1=cosine, 2=linear, 3=step_decay
    var step_decay_factor: Float64
    var step_decay_interval: Int

    def __init__(out self, base_lr: Float64, warmup_steps: Int = 0,
                total_steps: Int = 1000, schedule_type: Int = 0):
        self.base_lr = base_lr
        self.warmup_steps = warmup_steps
        self.total_steps = total_steps
        self.schedule_type = schedule_type
        self.step_decay_factor = 0.1
        self.step_decay_interval = 100

    def __init__(out self, *, copy: Self):
        self.base_lr = copy.base_lr
        self.warmup_steps = copy.warmup_steps
        self.total_steps = copy.total_steps
        self.schedule_type = copy.schedule_type
        self.step_decay_factor = copy.step_decay_factor
        self.step_decay_interval = copy.step_decay_interval

    def __init__(out self, *, deinit move: Self):
        self.base_lr = move.base_lr^
        self.warmup_steps = move.warmup_steps^
        self.total_steps = move.total_steps^
        self.schedule_type = move.schedule_type^
        self.step_decay_factor = move.step_decay_factor^
        self.step_decay_interval = move.step_decay_interval^

    def get_lr(self, step: Int) -> Float64:
        """Get the learning rate for the given step."""
        # Warmup phase
        if step < self.warmup_steps and self.warmup_steps > 0:
            return self.base_lr * Float64(step + 1) / Float64(self.warmup_steps)

        # Post-warmup scheduling
        if self.schedule_type == 0:
            # Constant
            return self.base_lr
        elif self.schedule_type == 1:
            # Cosine annealing
            var pi = 3.14159265358979323846
            var progress = Float64(step - self.warmup_steps) / Float64(max(1, self.total_steps - self.warmup_steps))
            return self.base_lr * 0.5 * (1.0 + cos(pi * progress))
        elif self.schedule_type == 2:
            # Linear decay
            var progress = Float64(step - self.warmup_steps) / Float64(max(1, self.total_steps - self.warmup_steps))
            return self.base_lr * max(0.0, 1.0 - progress)
        elif self.schedule_type == 3:
            # Step decay
            var num_decays = (step - self.warmup_steps) // self.step_decay_interval
            var factor = 1.0
            for i in range(num_decays):
                factor *= self.step_decay_factor
            return self.base_lr * factor
        else:
            return self.base_lr
