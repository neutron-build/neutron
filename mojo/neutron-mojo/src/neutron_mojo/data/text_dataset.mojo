# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Text Dataset
# ===----------------------------------------------------------------------=== #

"""Create training datasets from token sequences using sliding window."""

from .dataset import DataSample, Dataset


fn create_text_dataset(token_ids: List[Int], seq_len: Int) -> Dataset:
    """Create a Dataset from a sequence of token IDs using a sliding window.

    Each sample has `seq_len` input tokens and the next token as target.

    Args:
        token_ids: List of token IDs.
        seq_len: Number of input tokens per sample.

    Returns:
        Dataset with sliding window samples.
    """
    var ds = Dataset()
    var n = len(token_ids)
    if n <= seq_len:
        return ds^

    var i = 0
    while i + seq_len < n:
        var input_ids = List[Int]()
        for j in range(seq_len):
            input_ids.append(token_ids[i + j])
        var target = token_ids[i + seq_len]
        ds.add(DataSample(input_ids^, target))
        i += 1

    return ds^
