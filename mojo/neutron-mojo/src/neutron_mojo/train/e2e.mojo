# ===----------------------------------------------------------------------=== #
# Neutron Mojo — End-to-End Training
# ===----------------------------------------------------------------------=== #

"""Complete training pipeline: data to trained model.

Combines autograd, modules, losses, optimizers, data loading,
and trainable transformer for end-to-end language model training.
"""

from std.math import sqrt, isfinite

from neutron_mojo.autograd import Tape, run_backward
from neutron_mojo.train.trainable import TrainableLM, causal_lm_loss
from neutron_mojo.train.loop import TrainingConfig, TrainingState, TrainingMetrics
from neutron_mojo.optim import Adam, SGD, LRScheduler, clip_grad_norm
from neutron_mojo.data import Dataset, DataSample
from neutron_mojo.tensor.shape import Shape


struct TrainResult(Movable):
    """Result of training: the tape with trained weights + metrics."""
    var tape: Tape
    var metrics: TrainingMetrics
    var final_loss: Float64
    var total_steps: Int

    def __init__(out self, var tape: Tape, var metrics: TrainingMetrics,
                final_loss: Float64, total_steps: Int):
        self.tape = tape^
        self.metrics = metrics^
        self.final_loss = final_loss
        self.total_steps = total_steps

    def __init__(out self, *, deinit move: Self):
        self.tape = move.tape^
        self.metrics = move.metrics^
        self.final_loss = move.final_loss^
        self.total_steps = move.total_steps^


def train_tiny_lm(
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
    # Admit the entire run before allocating parameters or touching optimizer state.
    if config.batch_size <= 0 or config.grad_accumulation_steps <= 0 or config.epochs < 0:
        raise Error("Invalid training batch/accumulation/epoch configuration")
    if config.batch_size > 0x7FFFFFFFFFFFFFFF // config.grad_accumulation_steps:
        raise Error("Training accumulation size overflow")
    if not isfinite(config.weight_decay) or config.weight_decay < 0 or not isfinite(config.lr) or config.lr < 0:
        raise Error("Invalid training learning rate or decay")
    if config.eval_interval != 0 or config.save_interval != 0:
        raise Error("train_tiny_lm does not provide evaluation or checkpoint callbacks; set intervals to zero")
    if config.warmup_steps < 0 or config.log_interval < 0 or not isfinite(config.max_grad_norm) or config.max_grad_norm < 0:
        raise Error("Invalid training warmup/logging/gradient clipping configuration")
    if vocab_size <= 0 or hidden_dim <= 0 or num_layers < 0 or ffn_dim < 0:
        raise Error("Invalid training model dimensions")
    if ffn_dim == 0 and hidden_dim > 0x7FFFFFFFFFFFFFFF // 4:
        raise Error("Training FFN width overflow")
    var effective_ffn = ffn_dim if ffn_dim > 0 else hidden_dim * 4
    for sample_idx in range(dataset.size()):
        var sample = dataset.get(sample_idx)
        if sample.seq_len() <= 0 or sample.target_id < 0 or sample.target_id >= vocab_size:
            raise Error("Invalid training sample or target")
        for token_idx in range(sample.seq_len()):
            if sample.input_ids[token_idx] < 0 or sample.input_ids[token_idx] >= vocab_size:
                raise Error("Training input token outside vocabulary")
    var window = config.batch_size * config.grad_accumulation_steps
    var steps_per_epoch = dataset.size() // window + Int(dataset.size() % window != 0)
    if steps_per_epoch > 0 and config.epochs > 0x7FFFFFFFFFFFFFFF // steps_per_epoch:
        raise Error("Training step count overflow")
    var embedding_count = Shape(vocab_size, hidden_dim).numel()
    var attention_count = Shape(hidden_dim, hidden_dim, 4).numel()
    var ffn_count = Shape(hidden_dim, effective_ffn, 3).numel()
    var norm_count = Shape(hidden_dim, 2).numel()
    if attention_count > 0x7FFFFFFFFFFFFFFF - ffn_count or attention_count + ffn_count > 0x7FFFFFFFFFFFFFFF - norm_count:
        raise Error("Training parameter count overflow")
    var layer_count = Shape(num_layers, attention_count + ffn_count + norm_count).numel()
    if embedding_count > (0x7FFFFFFFFFFFFFFF - layer_count) // 2:
        raise Error("Training parameter count overflow")
    var estimated_params = embedding_count * 2 + layer_count
    if estimated_params > 0x7FFFFFFFFFFFFFFF // 20:
        raise Error("Training tape capacity overflow")
    var tape_capacity = max(estimated_params * 20, 262144)
    var tape = Tape(tape_capacity)
    var model = TrainableLM(vocab_size, hidden_dim, num_layers, ffn_dim)
    model.register(tape)
    var param_indices = model.all_param_indices()

    var state = TrainingState()
    var metrics = TrainingMetrics()
    var scheduler = LRScheduler(base_lr=config.lr, warmup_steps=config.warmup_steps,
        total_steps=config.epochs * steps_per_epoch, schedule_type=1)
    var adam = Adam(lr=config.lr, weight_decay=config.weight_decay)
    var sgd = SGD(lr=config.lr, weight_decay=config.weight_decay)
    var parameter_count = tape.num_variables()
    tape.zero_all_grads()
    for epoch in range(config.epochs):
        state.current_epoch = epoch
        state.reset_running_loss()
        var sample_idx = 0
        while sample_idx < dataset.size():
            var count = min(window, dataset.size() - sample_idx)
            var summed_loss = Float64(0)
            for micro in range(count):
                var sample = dataset.get(sample_idx + micro)
                if sample.seq_len() <= 0:
                    raise Error("Training sample must be nonempty")
                var token = sample.input_ids[sample.seq_len() - 1]
                var loss_idx = causal_lm_loss(tape, model, token, sample.target_id)
                summed_loss += Float64(tape.get_data(loss_idx, 0))
                run_backward(tape, loss_idx)
                tape.rewind_activations(parameter_count)
            for pidx in range(len(param_indices)):
                var idx = param_indices[pidx]
                for i in range(tape.var_numel(idx)):
                    tape.set_grad(idx, i, tape.get_grad(idx, i) / Float32(count))
            if config.max_grad_norm > 0:
                _ = clip_grad_norm(tape, param_indices, config.max_grad_norm)
            var lr = scheduler.get_lr(state.global_step)
            if config.use_adam:
                adam.lr = lr
                adam.step(tape, param_indices)
            else:
                sgd.lr = lr
                sgd.step(tape, param_indices)
            tape.zero_all_grads()
            var mean_loss = summed_loss / Float64(count)
            state.record_loss(mean_loss)
            metrics.record(mean_loss, lr)
            if verbose and config.log_interval > 0 and state.global_step % config.log_interval == 0:
                print("Step " + String(state.global_step) + " loss=" + String(mean_loss))
            sample_idx += count
        if verbose:
            print("Epoch " + String(epoch + 1) + "/" + String(config.epochs) + " loss=" + String(state.avg_loss()))

    var final_loss = metrics.last_loss()
    var total_steps = state.global_step

    if verbose:
        print("")
        print("Training complete. Final loss: " + String(final_loss))
        print("Total steps: " + String(total_steps))

    return TrainResult(tape^, metrics^, final_loss, total_steps)


def create_simple_dataset(token_sequence: List[Int], seq_len: Int) raises -> Dataset:
    """Create a simple next-token prediction dataset from a token sequence.

    Uses a sliding window: input = [t0..t_{seq_len-1}], target = t_{seq_len}.

    Args:
        token_sequence: List of token IDs.
        seq_len: Number of input tokens per sample.

    Returns:
        Dataset with sliding window samples.
    """
    if seq_len <= 0:
        raise Error("Sequence length must be positive")
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
