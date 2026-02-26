# ===----------------------------------------------------------------------=== #
# Neutron Mojo — JSON Parser Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the minimal JSON parser (SafeTensors header format)."""

from neutron_mojo.io.json import (
    json_skip_whitespace,
    json_parse_string,
    json_parse_int,
    json_parse_int_array,
    parse_safetensors_header,
)
from neutron_mojo.io.safetensors import TensorInfo


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected " + String(b) + " got " + String(a)
        )


fn test_skip_whitespace() raises:
    """Test skipping whitespace."""
    assert_eq(json_skip_whitespace("  hello", 0), 2, "skip 2 spaces")
    assert_eq(json_skip_whitespace("hello", 0), 0, "no whitespace")
    assert_eq(json_skip_whitespace("  ", 0), 2, "all whitespace")
    print("  skip_whitespace: PASS")


fn test_parse_string() raises:
    """Test parsing JSON strings."""
    var r1 = json_parse_string('"hello"', 0)
    assert_true(r1.value == "hello", "simple string")
    assert_eq(r1.pos, 7, "pos after simple string")

    var r2 = json_parse_string('"ab\\"cd"', 0)
    assert_true(r2.value == 'ab"cd', "escaped quote")

    var r3 = json_parse_string('"line\\nbreak"', 0)
    assert_true(len(r3.value) > 0, "string with newline escape")

    var r4 = json_parse_string('""', 0)
    assert_true(r4.value == "", "empty string")

    print("  parse_string: PASS")


fn test_parse_int() raises:
    """Test parsing JSON integers."""
    var r1 = json_parse_int("42", 0)
    assert_eq(r1.value, 42, "positive int")
    assert_eq(r1.pos, 2, "pos after 42")

    var r2 = json_parse_int("-7", 0)
    assert_eq(r2.value, -7, "negative int")

    var r3 = json_parse_int("0,", 0)
    assert_eq(r3.value, 0, "zero")
    assert_eq(r3.pos, 1, "stops at comma")

    var r4 = json_parse_int("123456", 0)
    assert_eq(r4.value, 123456, "large int")

    print("  parse_int: PASS")


fn test_parse_int_array() raises:
    """Test parsing JSON integer arrays."""
    var r1 = json_parse_int_array("[1, 2, 3]", 0)
    assert_eq(len(r1.values), 3, "3 elements")
    assert_eq(r1.values[0], 1, "first element")
    assert_eq(r1.values[1], 2, "second element")
    assert_eq(r1.values[2], 3, "third element")

    var r2 = json_parse_int_array("[]", 0)
    assert_eq(len(r2.values), 0, "empty array")

    var r3 = json_parse_int_array("[4096]", 0)
    assert_eq(len(r3.values), 1, "single element")
    assert_eq(r3.values[0], 4096, "single element value")

    print("  parse_int_array: PASS")


fn test_parse_safetensors_header_simple() raises:
    """Test parsing a simple SafeTensors header."""
    var json = '{"weight": {"dtype": "F32", "shape": [4, 8], "data_offsets": [0, 128]}}'
    var result = parse_safetensors_header(json)

    assert_true("weight" in result, "should have 'weight'")
    var info = result["weight"].copy()
    assert_true(info.dtype == "F32", "dtype F32")
    assert_eq(len(info.shape), 2, "2 dims")
    assert_eq(info.shape[0], 4, "shape[0]")
    assert_eq(info.shape[1], 8, "shape[1]")
    assert_eq(info.data_offset_start, 0, "data_offset_start")
    assert_eq(info.data_offset_end, 128, "data_offset_end")

    print("  parse_safetensors_header_simple: PASS")


fn test_parse_safetensors_header_metadata() raises:
    """Test that __metadata__ is skipped."""
    var json = '{"__metadata__": {"format": "pt"}, "bias": {"dtype": "F32", "shape": [8], "data_offsets": [0, 32]}}'
    var result = parse_safetensors_header(json)

    assert_true("__metadata__" not in result, "metadata should be skipped")
    assert_true("bias" in result, "should have bias")
    var info = result["bias"].copy()
    assert_true(info.dtype == "F32", "dtype F32")
    assert_eq(len(info.shape), 1, "1 dim")
    assert_eq(info.shape[0], 8, "shape[0]")

    print("  parse_safetensors_header_metadata: PASS")


fn test_multiple_dtypes() raises:
    """Test parsing headers with various dtype strings."""
    var json = '{"a": {"dtype": "F16", "shape": [2], "data_offsets": [0, 4]}, "b": {"dtype": "BF16", "shape": [3], "data_offsets": [4, 10]}}'
    var result = parse_safetensors_header(json)

    assert_true("a" in result, "has a")
    assert_true("b" in result, "has b")
    var a = result["a"].copy()
    var b = result["b"].copy()
    assert_true(a.dtype == "F16", "a dtype F16")
    assert_true(b.dtype == "BF16", "b dtype BF16")

    print("  multiple_dtypes: PASS")


fn test_empty_shape() raises:
    """Test parsing a tensor with empty shape (scalar)."""
    var json = '{"scalar": {"dtype": "F32", "shape": [], "data_offsets": [0, 4]}}'
    var result = parse_safetensors_header(json)
    assert_true("scalar" in result, "has scalar")
    var info = result["scalar"].copy()
    assert_eq(len(info.shape), 0, "no dims")

    print("  empty_shape: PASS")


fn main() raises:
    print("test_json_parser:")

    test_skip_whitespace()
    test_parse_string()
    test_parse_int()
    test_parse_int_array()
    test_parse_safetensors_header_simple()
    test_parse_safetensors_header_metadata()
    test_multiple_dtypes()
    test_empty_shape()

    print("ALL PASSED (8 tests)")
