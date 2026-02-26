# ===----------------------------------------------------------------------=== #
# Neutron Mojo — DataLoader
# ===----------------------------------------------------------------------=== #

"""Batched data loading with optional shuffling."""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from .dataset import Dataset, DataSample


struct BatchResult(Movable):
    """A batch of training data: flat input tensor + target list."""
    var inputs: Tensor[DType.float32]
    var targets: List[Int]
    var batch_size: Int
    var seq_len: Int

    fn __init__(out self, var inputs: Tensor[DType.float32], var targets: List[Int],
                batch_size: Int, seq_len: Int):
        self.inputs = inputs^
        self.targets = targets^
        self.batch_size = batch_size
        self.seq_len = seq_len

    fn __moveinit__(out self, deinit other: Self):
        self.inputs = other.inputs^
        self.targets = other.targets^
        self.batch_size = other.batch_size
        self.seq_len = other.seq_len


struct DataLoader(Movable):
    """Batched data loader with optional shuffling."""
    var dataset: Dataset
    var batch_size: Int
    var shuffle: Bool
    var current_idx: Int
    var order: List[Int]
    var _seed: Int

    fn __init__(out self, var dataset: Dataset, batch_size: Int = 4, shuffle: Bool = True):
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.current_idx = 0
        self._seed = 42
        self.order = List[Int]()
        var n = dataset.size()
        for i in range(n):
            self.order.append(i)
        self.dataset = dataset^
        if shuffle:
            self._shuffle_order()

    fn __moveinit__(out self, deinit other: Self):
        self.dataset = other.dataset^
        self.batch_size = other.batch_size
        self.shuffle = other.shuffle
        self.current_idx = other.current_idx
        self.order = other.order^
        self._seed = other._seed

    fn _lcg_next(mut self) -> Int:
        """Simple LCG random number generator."""
        self._seed = (self._seed * 1103515245 + 12345) & 0x7FFFFFFF
        return self._seed

    fn _shuffle_order(mut self):
        """Fisher-Yates shuffle."""
        var n = len(self.order)
        var i = n - 1
        while i > 0:
            var j = self._lcg_next() % (i + 1)
            var tmp = self.order[i]
            self.order[i] = self.order[j]
            self.order[j] = tmp
            i -= 1

    fn reset(mut self):
        """Reset to beginning of dataset."""
        self.current_idx = 0
        if self.shuffle:
            self._shuffle_order()

    fn has_next(self) -> Bool:
        return self.current_idx < self.dataset.size()

    fn next_batch(mut self) -> BatchResult:
        """Get the next batch of data."""
        var n = self.dataset.size()
        var actual_batch = min(self.batch_size, n - self.current_idx)

        # Get the first sample to determine seq_len
        var first_sample = self.dataset.get(self.order[self.current_idx])
        var seq_len = first_sample.seq_len()

        # Create flat input tensor: (batch_size * seq_len)
        var inputs = Tensor[DType.float32](actual_batch * seq_len)
        var targets = List[Int]()

        for b in range(actual_batch):
            var sample = self.dataset.get(self.order[self.current_idx + b])
            var sample_seq_len = sample.seq_len()
            var actual_len = min(sample_seq_len, seq_len)
            for s in range(actual_len):
                inputs.set(b * seq_len + s, Float32(sample.input_ids[s]))
            targets.append(sample.target_id)

        self.current_idx += actual_batch
        return BatchResult(inputs^, targets^, actual_batch, seq_len)

    fn num_batches(self) -> Int:
        """Return the number of batches per epoch."""
        var n = self.dataset.size()
        return (n + self.batch_size - 1) // self.batch_size
