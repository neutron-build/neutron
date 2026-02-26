# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Data utilities tests
# ===----------------------------------------------------------------------=== #

"""Tests for Dataset, DataLoader, text_dataset, csv_reader."""

from neutron_mojo.data import (
    DataSample, Dataset, DataLoader, BatchResult,
    create_text_dataset, parse_csv_line, CSVRow,
)


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn test_data_sample() raises:
    """DataSample stores input_ids and target."""
    var ids = List[Int]()
    ids.append(1)
    ids.append(2)
    ids.append(3)
    var s = DataSample(ids^, 4)
    assert_eq(s.seq_len(), 3)
    assert_eq(s.target_id, 4)
    assert_eq(s.input_ids[0], 1)
    print("  data_sample: PASS")


fn test_data_sample_copy() raises:
    """DataSample is Copyable."""
    var ids = List[Int]()
    ids.append(10)
    ids.append(20)
    var s = DataSample(ids^, 30)
    var s2 = s.copy()
    assert_eq(s2.target_id, 30)
    assert_eq(s2.input_ids[0], 10)
    print("  data_sample_copy: PASS")


fn test_dataset_add_get() raises:
    """Dataset add and get."""
    var ds = Dataset()
    var ids1 = List[Int]()
    ids1.append(1)
    ids1.append(2)
    ds.add(DataSample(ids1^, 3))
    var ids2 = List[Int]()
    ids2.append(4)
    ids2.append(5)
    ds.add(DataSample(ids2^, 6))
    assert_eq(ds.size(), 2)
    var s = ds.get(1)
    assert_eq(s.target_id, 6)
    print("  dataset_add_get: PASS")


fn test_text_dataset() raises:
    """Sliding window text dataset."""
    var tokens = List[Int]()
    for i in range(10):
        tokens.append(i)
    var ds = create_text_dataset(tokens, seq_len=3)
    # Samples: [0,1,2]->3, [1,2,3]->4, ..., [6,7,8]->9 = 7 samples
    assert_eq(ds.size(), 7)
    var s0 = ds.get(0)
    assert_eq(s0.input_ids[0], 0)
    assert_eq(s0.target_id, 3)
    var s6 = ds.get(6)
    assert_eq(s6.target_id, 9)
    print("  text_dataset: PASS")


fn test_text_dataset_short() raises:
    """Text dataset with sequence too short."""
    var tokens = List[Int]()
    tokens.append(1)
    tokens.append(2)
    var ds = create_text_dataset(tokens, seq_len=3)
    assert_eq(ds.size(), 0)
    print("  text_dataset_short: PASS")


fn test_dataloader_basic() raises:
    """DataLoader produces batches."""
    var ds = Dataset()
    for i in range(10):
        var ids = List[Int]()
        ids.append(i)
        ids.append(i + 1)
        ds.add(DataSample(ids^, i + 2))

    var dl = DataLoader(ds^, batch_size=3, shuffle=False)
    assert_eq(dl.num_batches(), 4)  # ceil(10/3)

    var batch = dl.next_batch()
    assert_eq(batch.batch_size, 3)
    assert_eq(batch.seq_len, 2)
    print("  dataloader_basic: PASS")


fn test_dataloader_iteration() raises:
    """DataLoader iterates through all data."""
    var ds = Dataset()
    for i in range(7):
        var ids = List[Int]()
        ids.append(i)
        ds.add(DataSample(ids^, i + 1))

    var dl = DataLoader(ds^, batch_size=3, shuffle=False)
    var total = 0
    while dl.has_next():
        var batch = dl.next_batch()
        total += batch.batch_size
    assert_eq(total, 7)
    print("  dataloader_iteration: PASS")


fn test_dataloader_reset() raises:
    """DataLoader reset allows re-iteration."""
    var ds = Dataset()
    for i in range(5):
        var ids = List[Int]()
        ids.append(i)
        ds.add(DataSample(ids^, i + 1))

    var dl = DataLoader(ds^, batch_size=2, shuffle=False)
    # Consume all
    while dl.has_next():
        _ = dl.next_batch()
    # Reset and check
    dl.reset()
    if not dl.has_next():
        raise Error("Expected has_next after reset")
    print("  dataloader_reset: PASS")


fn test_csv_parse_basic() raises:
    """Parse a simple CSV line."""
    var row = parse_csv_line("hello,world,123")
    assert_eq(row.num_fields(), 3)
    print("  csv_parse_basic: PASS")


fn test_csv_parse_single() raises:
    """Parse CSV with single field."""
    var row = parse_csv_line("only_one")
    assert_eq(row.num_fields(), 1)
    print("  csv_parse_single: PASS")


fn test_csv_empty_fields() raises:
    """CSV with empty fields."""
    var row = parse_csv_line("a,,c")
    assert_eq(row.num_fields(), 3)
    print("  csv_empty_fields: PASS")


fn test_dataloader_shuffle() raises:
    """Shuffled DataLoader produces different order on reset."""
    var ds = Dataset()
    for i in range(20):
        var ids = List[Int]()
        ids.append(i)
        ds.add(DataSample(ids^, i))

    var dl = DataLoader(ds^, batch_size=20, shuffle=True)
    var batch1 = dl.next_batch()
    var targets1 = List[Int]()
    for i in range(len(batch1.targets)):
        targets1.append(batch1.targets[i])

    dl.reset()
    var batch2 = dl.next_batch()

    # With shuffle, at least some targets should differ
    var diff_count = 0
    for i in range(min(len(targets1), len(batch2.targets))):
        if targets1[i] != batch2.targets[i]:
            diff_count += 1
    if diff_count == 0:
        raise Error("Shuffle should produce different order")
    print("  dataloader_shuffle: PASS")


fn main() raises:
    print("test_data:")
    test_data_sample()
    test_data_sample_copy()
    test_dataset_add_get()
    test_text_dataset()
    test_text_dataset_short()
    test_dataloader_basic()
    test_dataloader_iteration()
    test_dataloader_reset()
    test_csv_parse_basic()
    test_csv_parse_single()
    test_csv_empty_fields()
    test_dataloader_shuffle()
    print("ALL PASSED (12 tests)")
