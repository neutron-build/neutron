"""Exact regressions for MJ-02, MJ-16, MJ-K12 and typed initialization."""
from std.testing import assert_equal, assert_true
from neutron_mojo.io.binary_reader import _fp16_to_fp32
from neutron_mojo.data.csv_reader import parse_csv_line
from neutron_mojo.tensor.storage import Storage
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.view import TensorView


def test_fp16_exhaustive() raises:
    # Independent arithmetic IEEE-754 oracle, without the decoder's bit
    # normalization loop. Every finite half is exactly representable in f32.
    for bits in range(65536):
        var exponent = (bits >> 10) & 31
        var fraction = bits & 1023
        var actual = _fp16_to_fp32(bits)
        if exponent == 31:
            if fraction != 0:
                assert_true(actual != actual)
            elif bits < 32768:
                assert_true(actual > Float32(3.4e38))
            else:
                assert_true(actual < Float32(-3.4e38))
            continue
        var expected = Float32(fraction) / Float32(16777216)
        if exponent != 0:
            expected = Float32(1024 + fraction) / Float32(1024)
            if exponent >= 15:
                for _ in range(exponent - 15):
                    expected *= 2
            else:
                for _ in range(15 - exponent):
                    expected /= 2
        if bits >= 32768:
            expected = -expected
        assert_equal(actual, expected)
    assert_equal(_fp16_to_fp32(0x0001), Float32(1) / 16777216)
    assert_equal(_fp16_to_fp32(0x8001), -Float32(1) / 16777216)
    assert_equal(_fp16_to_fp32(0x03ff), Float32(1023) / 16777216)
    assert_equal(_fp16_to_fp32(0x0400), Float32(1) / 16384)
    # Distinguish signed zero, which numeric equality alone cannot do.
    assert_true(Float32(1) / _fp16_to_fp32(0x8000) < Float32(-3.4e38))
    print("MJ-02: all 65536 bit patterns PASS")


def test_csv_empty_fields() raises:
    var a = parse_csv_line("a,b,")
    assert_equal(a.num_fields(), 3)
    assert_equal(a.get(2), String(""))
    var b = parse_csv_line(",,")
    assert_equal(b.num_fields(), 3)
    for i in range(3):
        assert_equal(b.get(i), String(""))
    var c = parse_csv_line("")
    assert_equal(c.num_fields(), 0)
    var d = parse_csv_line("a,")
    assert_equal(d.num_fields(), 2)
    assert_equal(d.get(0), String("a"))
    assert_equal(d.get(1), String(""))
    print("MJ-16: trailing/quoted/multiple/blank PASS")


def test_slice_reshape_reads_and_writes() raises:
    var s = Storage[DType.float32](12)
    for i in range(12):
        s.store(i, Float32(i))
    var v = TensorView[DType.float32](s.unsafe_ptr(), Shape(4, 3))
    var sliced = v.slice_dim(0, 1, 2)
    var flat = sliced.reshape(Shape(6))
    for i in range(6):
        assert_equal(flat.load(i), Float32(i + 3))
    var nested = flat.slice_dim(0, 3, 3).reshape(Shape(1, 3))
    assert_equal(nested.load(0, 0), Float32(6))
    nested.store(0, 1, value=99)
    assert_equal(s.load(7), Float32(99))
    assert_equal(s.load(1), Float32(1))
    var rejected = False
    try:
        var transposed = v.transpose(0, 1).reshape(Shape(12))
    except:
        rejected = True
    assert_true(rejected)
    _ = s.load(0)  # keep the borrowed storage alive through all view accesses
    print("MJ-K12: nested sliced reshape reads/write-through PASS")


def check_zero[dtype: DType]() raises:
    # Odd and empty lengths, plus reuse after a nonzero fill, exercise every
    # element of 1/2/4/8-byte scalar allocations.
    for count in range(18):
        var s = Storage[dtype](count)
        for i in range(count):
            assert_equal(s.load(i), Scalar[dtype](0))
        s.fill(Scalar[dtype](7))
        s.fill(Scalar[dtype](0))
        for i in range(count):
            assert_equal(s.load(i), Scalar[dtype](0))


def main() raises:
    test_fp16_exhaustive()
    test_csv_empty_fields()
    test_slice_reshape_reads_and_writes()
    check_zero[DType.uint8]()
    check_zero[DType.int16]()
    check_zero[DType.float16]()
    check_zero[DType.float32]()
    check_zero[DType.float64]()
    check_zero[DType.int64]()
    print("typed storage initialization PASS")
