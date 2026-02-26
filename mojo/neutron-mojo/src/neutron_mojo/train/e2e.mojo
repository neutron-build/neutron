# ===----------------------------------------------------------------------=== #
# Neutron Mojo — End-to-End Training
# ===----------------------------------------------------------------------=== #

"""Complete training pipeline: data to trained model.

Combines autograd, modules, losses, optimizers, data loading,
and trainable transformer for end-to-end language model training.
"""

from math import sqrt

from neutron_mojo.autograd import Tape, run_backward
from neutron_mojo.train.trainable import TrainableLM, causal_lm_loss
from neutron_mojo.train.loop import TrainingConfig, TrainingState, TrainingMetrics
from neutron_mojo.optim import Adam, SGD, LRScheduler, clip_grad_norm
from neutron_mojo.data import Dataset, DataSample


struct TrainResult(Movable):
    """Result of training: the tape with trained weights + metrics."""
    var tape: Tape
    var metrics: TrainingMetrics
    var final_loss: Float64
    var total_steps: Int

    fn __init__(out self, var tape: Tape, var metrics: TrainingMetrics,
                final_loss: Float64, total_steps: Int):
        self.tape = tape^
        self.metrics = metrics^
        self.final_loss = final_loss
        self.total_steps = total_steps

    fn __moveinit__(out self, deinit other: Self):
        self.tape = other.tape^
        self.metrics = other.metrics^
        self.final_loss = other.final_loss
        self.total_steps = other.total_steps


fn train_tiny_lm(
    dataset: Dataset,
    config: TrainingConfig,
    vocab_size: Int,
    hidden_dim: Int,
    num_layers: Int,
    ffn_dim: Int = 0,
    verbose: Bool = True,
) raises -> TrainResult:
    """Train a small language model end-to-end.

    Args:
        dataset: Training dataset of (input_ids, target_id) samples.
        config: Training configuration.
        vocab_size: Vocabulary size.
        hidden_dim: Hidden dimension.
        num_layers: Number of transformer layers.
        ffn_dim: FFN dimension (0 = 4x hidden).
        verbose: Print progress.

    Returns:
        TrainResult with trained tape and metrics.
    """
    # Estimate tape capacity
    var estimated_params = vocab_size * hidden_dim + num_layers * (
        hidden_dim * hidden_dim * 4 +  # Q, K, V, O
        hidden_dim * (ffn_dim if ffn_dim > 0 else hidden_dim * 4) * 3 +  # gate, up, down
        hidden_dim * 2  # norms
    ) + hidden_dim * vocab_size  # lm_head
    var tape_capacity = max(estimated_params * 20, 262144)

    var tape = Tape(tape_capacity)
    var model = TrainableLM(vocab_size, hidden_dim, num_layers, ffn_dim)
    model.register(tape)

    var param_indices = model.all_param_indices()

    if verbose:
        print("Training LM: vocab=" + String(vocab_size) + " hidden=" + String(hidden_dim)
              + " layers=" + String(num_layers))
        print("Parameters: " + String(model.num_parameters(tape)))
        print("Dataset size: " + String(dataset.size()))
        print("Config: epochs=" + String(config.epochs) + " lr=" + String(config.lr))
        print("")

    # Set up optimizer
    var state = TrainingState()
    var metrics = TrainingMetrics()

    var scheduler = LRScheduler(
        base_lr=config.lr,
        warmup_steps=config.warmup_steps,
        total_steps=config.epochs * dataset.size(),
        schedule_type=1,  # cosine
    )

    if config.use_adam:
        var adam = Adam(lr=config.lr)

        for epoch in range(config.epochs):
            state.current_epoch = epoch
            state.reset_running_loss()

            for sample_idx in range(dataset.size()):
                var sample = dataset.get(sample_idx)

                # Forward + loss for last token
                var last_token = sample.input_ids[sample.seq_len() - 1]
                var loss_idx = causal_lm_loss(tape, model, last_token, sample.target_id)
                var loss_val = Float64(tape.get_data(loss_idx, 0))

                # Backward
                run_backward(tape, loss_idx)

                # Gradient clipping
                if config.max_grad_norm > 0.0:
                    _ = clip_grad_norm(tape, param_indices, config.max_grad_norm)

                # Update LR
                var lr = scheduler.get_lr(state.global_step)
                adam.lr = lr

                # Step
                adam.step(tape, param_indices)
                tape.zero_all_grads()

                state.record_loss(loss_val)
                metrics.record(loss_val, lr)

            if verbose:
                print("Epoch " + String(epoch + 1) + "/" + String(config.epochs)
                      + " loss=" + String(state.avg_loss()))

    else:
        var sgd = SGD(lr=config.lr)

        for epoch in range(config.epochs):
            state.current_epoch = epoch
            state.reset_running_loss()

            for sample_idx in range(dataset.size()):
                var sample = dataset.get(sample_idx)

                var last_token = sample.input_ids[sample.seq_len() - 1]
                var loss_idx = causal_lm_loss(tape, model, last_token, sample.target_id)
                var loss_val = Float64(tape.get_data(loss_idx, 0))

                run_backward(tape, loss_idx)

                if config.max_grad_norm > 0.0:
                    _ = clip_grad_norm(tape, param_indices, config.max_grad_norm)

                var lr = scheduler.get_lr(state.global_step)
                sgd.lr = lr

                sgd.step(tape, param_indices)
                tape.zero_all_grads()

                state.record_loss(loss_val)
                metrics.record(loss_val, lr)

            if verbose:
                print("Epoch " + String(epoch + 1) + "/" + String(config.epochs)
                      + " loss=" + String(state.avg_loss()))

    var final_loss = metrics.last_loss()
    var total_steps = state.global_step

    if verbose:
        print("")
        print("Training complete. Final loss: " + String(final_loss))
        print("Total steps: " + String(total_steps))

    return TrainResult(tape^, metrics^, final_loss, total_steps)


fn create_simple_dataset(token_sequence: List[Int], seq_len: Int) -> Dataset:
    """Create a simple next-token prediction dataset from a token sequence.

    Uses a sliding window: input = [t0..t_{seq_len-1}], target = t_{seq_len}.

    Args:
        token_sequence: List of token IDs.
        seq_len: Number of input tokens per sample.

    Returns:
        Dataset with sliding window samples.
    """
    var ds = Dataset()
    var n = len(token_sequence)
    if n <= seq_len:
        return ds^

    var i = 0
    while i + seq_len < n:
        var input_ids = List[Int]()
        for j in range(seq_len):
            input_ids.append(token_sequence[i + j])
        ds.add(DataSample(input_ids^, token_sequence[i + seq_len]))
        i += 1

    return ds^
