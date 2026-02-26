# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Memory-Mapped Reader Tests (Sprint 13)
# ===----------------------------------------------------------------------=== #

"""Tests for mmap-backed BinaryReader and mmap model loading."""

from time import perf_counter_ns
from neutron_mojo.io.binary_reader import BinaryReader, mmap_reader
from neutron_mojo.io.gguf import (
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
    GGUF_MAGIC,
)
from neutron_mojo.model.weight_reader import (
    load_gguf_model_from_buffer,
    load_gguf_model_mmap,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    var diff = a - b
    if diff < 0:
        diff = -diff
    if diff > tol:
        raise Error(
            "Assertion failed: " + msg
            + " got " + String(a) + " vs " + String(b)
        )


# ===----------------------------------------------------------------------=== #
# Test Helpers
# ===----------------------------------------------------------------------=== #

fn _write_test_file(path: String) raises:
    """Write a small binary test file with known values."""
    var buf = List[UInt8]()
    # Write 4 bytes: 0x01 0x02 0x03 0x04
    buf.append(UInt8(0x01))
    buf.append(UInt8(0x02))
    buf.append(UInt8(0x03))
    buf.append(UInt8(0x04))
    # Write 4 more bytes for F32: 1.0 = 0x3F800000
    buf.append(UInt8(0x00))
    buf.append(UInt8(0x00))
    buf.append(UInt8(0x80))
    buf.append(UInt8(0x3F))
    # Write 4 more: 2.5 = 0x40200000
    buf.append(UInt8(0x00))
    buf.append(UInt8(0x00))
    buf.append(UInt8(0x20))
    buf.append(UInt8(0x40))

    var f = open(path, "w")
    var bytes_span = Span[Byte](buf)
    f.write_bytes(bytes_span)


fn _build_tiny_gguf_file(path: String) raises:
    """Build a tiny GGUF model file on disk for mmap testing."""
    var buf = _build_tiny_gguf_buffer()
    var f = open(path, "w")
    var bytes_span = Span[Byte](buf)
    f.write_bytes(bytes_span)


fn _build_tiny_gguf_buffer() -> List[UInt8]:
    """Build a tiny GGUF buffer with known structure."""
    var buf = List[UInt8]()
    # Magic
    _write_u32_le(buf, GGUF_MAGIC)
    # Version 3
    _write_u32_le(buf, 3)
    # Tensor count: 1 (embed only)
    _write_u64_le(buf, 1)
    # Metadata count: 6
    _write_u64_le(buf, 6)

    # Metadata: architecture
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)  # STRING
    _write_string_gguf(buf, "llama")

    # Metadata: hidden_size
    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)  # UINT32
    _write_u32_le(buf, 4)

    # Metadata: num_layers
    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    # Metadata: num_heads
    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    # Metadata: num_kv_heads
    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    # Metadata: ffn_dim
    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    # Tensor info: token_embd.weight [8, 4] F32
    _write_string_gguf(buf, "token_embd.weight")
    _write_u32_le(buf, 2)  # n_dims
    _write_u64_le(buf, 4)  # dim 0 (hidden)
    _write_u64_le(buf, 8)  # dim 1 (vocab)
    _write_u32_le(buf, 0)  # F32 = type 0
    _write_u64_le(buf, 0)  # offset (relative to data section)

    # Pad to alignment (32 bytes)
    while len(buf) % 32 != 0:
        buf.append(UInt8(0))

    # Tensor data: 8*4 = 32 float32 values
    for i in range(32):
        _write_f32_le(buf, Float32(i) * 0.01)

    return buf^


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_mmap_reader_basic_reads() raises:
    """Mmap reader reads same bytes as slurp reader."""
    _write_test_file("/tmp/test_mmap_basic.bin")

    var slurp = BinaryReader("/tmp/test_mmap_basic.bin")
    var mmap = mmap_reader("/tmp/test_mmap_basic.bin")

    assert_true(mmap.is_mmap(), "mmap reader is in mmap mode")
    assert_true(not slurp.is_mmap(), "slurp reader is not in mmap mode")
    assert_true(slurp.size == mmap.size, "same file size")

    # Read first 4 bytes
    var s_u32 = slurp.read_u32_le()
    var m_u32 = mmap.read_u32_le()
    assert_true(s_u32 == m_u32, "u32_le match")

    # Read F32 values
    var s_f1 = slurp.read_f32_le()
    var m_f1 = mmap.read_f32_le()
    assert_near(s_f1, m_f1, 0.0001, "f32 1.0 match")

    var s_f2 = slurp.read_f32_le()
    var m_f2 = mmap.read_f32_le()
    assert_near(s_f2, m_f2, 0.0001, "f32 2.5 match")

    print("  mmap_reader_basic_reads: PASS")


fn test_mmap_reader_seek_tell() raises:
    """Seek and tell work correctly with mmap."""
    _write_test_file("/tmp/test_mmap_seek.bin")

    var mmap = mmap_reader("/tmp/test_mmap_seek.bin")

    assert_true(mmap.tell() == 0, "initial position is 0")

    mmap.seek(4)
    assert_true(mmap.tell() == 4, "seek to 4")

    # Read F32 at offset 4 (should be 1.0)
    var val = mmap.read_f32_le()
    assert_near(val, 1.0, 0.0001, "read f32 at offset 4")
    assert_true(mmap.tell() == 8, "cursor advanced to 8")

    mmap.seek(0)
    var first = mmap.read_u8()
    assert_true(Int(first) == 1, "first byte is 0x01")

    print("  mmap_reader_seek_tell: PASS")


fn test_mmap_reader_skip_remaining() raises:
    """Skip and remaining work with mmap."""
    _write_test_file("/tmp/test_mmap_skip.bin")

    var mmap = mmap_reader("/tmp/test_mmap_skip.bin")

    assert_true(mmap.remaining() == 12, "remaining is file size at start")

    mmap.skip(4)
    assert_true(mmap.remaining() == 8, "remaining after skip 4")
    assert_true(mmap.tell() == 4, "tell after skip 4")

    print("  mmap_reader_skip_remaining: PASS")


fn test_mmap_reader_f32_array() raises:
    """F32 array reading via mmap matches slurp."""
    _write_test_file("/tmp/test_mmap_f32arr.bin")

    var slurp = BinaryReader("/tmp/test_mmap_f32arr.bin")
    var mmap = mmap_reader("/tmp/test_mmap_f32arr.bin")

    # Skip first 4 bytes
    slurp.skip(4)
    mmap.skip(4)

    # Read 2 floats as array
    var s_arr = slurp.read_f32_array(2)
    var m_arr = mmap.read_f32_array(2)

    assert_near(s_arr.get(0), m_arr.get(0), 0.0001, "f32 array [0] match")
    assert_near(s_arr.get(1), m_arr.get(1), 0.0001, "f32 array [1] match")
    assert_near(m_arr.get(0), 1.0, 0.0001, "f32 array [0] is 1.0")
    assert_near(m_arr.get(1), 2.5, 0.0001, "f32 array [1] is 2.5")

    print("  mmap_reader_f32_array: PASS")


fn test_mmap_gguf_model_load() raises:
    """Load a GGUF model via mmap and verify weights match buffer loading."""
    _build_tiny_gguf_file("/tmp/test_mmap_gguf.gguf")

    # Load via mmap
    var mmap_model = load_gguf_model_mmap("/tmp/test_mmap_gguf.gguf")

    # Load via buffer for comparison
    var buf = _build_tiny_gguf_buffer()
    var buf_model = load_gguf_model_from_buffer(buf^)

    # Compare embed weights
    var p = mmap_model.params.copy()
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            assert_near(
                mmap_model.embed.get(v, d),
                buf_model.embed.get(v, d),
                0.0001,
                "embed match at (" + String(v) + "," + String(d) + ")",
            )

    print("  mmap_gguf_model_load: PASS")


fn test_mmap_vs_slurp_benchmark() raises:
    """Benchmark mmap vs slurp loading (informational)."""
    _build_tiny_gguf_file("/tmp/test_mmap_bench.gguf")

    # Warm up
    var _ = load_gguf_model_mmap("/tmp/test_mmap_bench.gguf")

    # Benchmark mmap
    var t0 = perf_counter_ns()
    for _ in range(10):
        var m = load_gguf_model_mmap("/tmp/test_mmap_bench.gguf")
    var t1 = perf_counter_ns()
    var mmap_us = (t1 - t0) // 10000  # avg in microseconds

    # Benchmark slurp
    var t2 = perf_counter_ns()
    var buf = _build_tiny_gguf_buffer()
    for _ in range(10):
        var b = buf.copy()
        var m = load_gguf_model_from_buffer(b^)
    var t3 = perf_counter_ns()
    var slurp_us = (t3 - t2) // 10000

    print(
        "  mmap_vs_slurp_benchmark: mmap=" + String(mmap_us) + "us slurp="
        + String(slurp_us) + "us: PASS"
    )


fn test_mmap_cleanup() raises:
    """Mmap reader cleans up without errors."""
    _write_test_file("/tmp/test_mmap_cleanup.bin")

    # Create and destroy mmap reader
    var mmap = mmap_reader("/tmp/test_mmap_cleanup.bin")
    var val = mmap.read_u32_le()
    # Reader goes out of scope here — __del__ calls munmap

    # Create another one to verify no resource leaks
    var mmap2 = mmap_reader("/tmp/test_mmap_cleanup.bin")
    var val2 = mmap2.read_u32_le()
    assert_true(val == val2, "consistent reads after cleanup")

    print("  mmap_cleanup: PASS")


fn main() raises:
    print("test_mmap_reader:")

    test_mmap_reader_basic_reads()
    test_mmap_reader_seek_tell()
    test_mmap_reader_skip_remaining()
    test_mmap_reader_f32_array()
    test_mmap_gguf_model_load()
    test_mmap_vs_slurp_benchmark()
    test_mmap_cleanup()

    print("ALL PASSED (7 tests)")
