# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Dataset
# ===----------------------------------------------------------------------=== #

"""Simple dataset: list of (input_ids, target_id) pairs."""


struct DataSample(Copyable, Movable, ImplicitlyCopyable):
    """A single training sample: input token IDs and a target token ID."""
    var input_ids: List[Int]
    var target_id: Int

    def __init__(out self, var input_ids: List[Int], target_id: Int):
        self.input_ids = input_ids^
        self.target_id = target_id

    def __init__(out self, *, copy: Self):
        self.input_ids = List[Int]()
        for i in range(len(copy.input_ids)):
            self.input_ids.append(copy.input_ids[i])
        self.target_id = copy.target_id

    def __init__(out self, *, deinit move: Self):
        self.input_ids = move.input_ids^
        self.target_id = move.target_id^

    def copy(self) -> DataSample:
        var ids = List[Int]()
        for i in range(len(self.input_ids)):
            ids.append(self.input_ids[i])
        return DataSample(ids^, self.target_id)

    def seq_len(self) -> Int:
        return len(self.input_ids)


struct Dataset(Movable):
    """Collection of DataSamples."""
    var samples: List[DataSample]

    def __init__(out self):
        self.samples = List[DataSample]()

    def __init__(out self, *, deinit move: Self):
        self.samples = move.samples^

    def add(mut self, sample: DataSample):
        self.samples.append(sample.copy())

    def get(self, idx: Int) -> DataSample:
        return self.samples[idx].copy()

    def size(self) -> Int:
        return len(self.samples)
