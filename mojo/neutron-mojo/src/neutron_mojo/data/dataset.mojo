# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Dataset
# ===----------------------------------------------------------------------=== #

"""Simple dataset: list of (input_ids, target_id) pairs."""


struct DataSample(Copyable, Movable):
    """A single training sample: input token IDs and a target token ID."""
    var input_ids: List[Int]
    var target_id: Int

    fn __init__(out self, var input_ids: List[Int], target_id: Int):
        self.input_ids = input_ids^
        self.target_id = target_id

    fn __copyinit__(out self, other: Self):
        self.input_ids = List[Int]()
        for i in range(len(other.input_ids)):
            self.input_ids.append(other.input_ids[i])
        self.target_id = other.target_id

    fn __moveinit__(out self, deinit other: Self):
        self.input_ids = other.input_ids^
        self.target_id = other.target_id

    fn copy(self) -> DataSample:
        var ids = List[Int]()
        for i in range(len(self.input_ids)):
            ids.append(self.input_ids[i])
        return DataSample(ids^, self.target_id)

    fn seq_len(self) -> Int:
        return len(self.input_ids)


struct Dataset(Movable):
    """Collection of DataSamples."""
    var samples: List[DataSample]

    fn __init__(out self):
        self.samples = List[DataSample]()

    fn __moveinit__(out self, deinit other: Self):
        self.samples = other.samples^

    fn add(mut self, sample: DataSample):
        self.samples.append(sample.copy())

    fn get(self, idx: Int) -> DataSample:
        return self.samples[idx].copy()

    fn size(self) -> Int:
        return len(self.samples)
