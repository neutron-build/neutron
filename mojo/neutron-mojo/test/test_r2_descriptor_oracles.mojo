"""Independent descriptor and parser expectations; runtime pending capacity release."""
from std.testing import assert_true, assert_equal
from std.math import cos, sin, abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head
from neutron_mojo.model.loader import WeightDescriptor, WeightIndex
from neutron_mojo.io.json import parse_config_json, parse_weight_map
from neutron_mojo.autograd.tape import Tape, TapeEntry, OP_MATMUL, OP_SPLIT, OP_BIAS_ADD


def test_rotary_frequency() raises:
    # Physical width 8, selected width 4: second pair frequency is 100^-1/2.
    # This distinguishes selected-width frequencies from the old physical width.
    var table = RoPETable(8, 8, theta=100.0, rotary_dim=4)
    var x = Tensor[DType.float32](Shape(8))
    for i in range(8):
        x.set(i, Float32(i + 1))
    apply_rope_single_head(x, table, 3)
    for pair in range(2):
        var angle = 3.0 if pair == 0 else 0.3
        var a = Float64(2 * pair + 1)
        var b = Float64(2 * pair + 2)
        assert_true(abs(Float64(x.get(2 * pair)) - (a * cos(angle) - b * sin(angle))) < 0.00001)
        assert_true(abs(Float64(x.get(2 * pair + 1)) - (a * sin(angle) + b * cos(angle))) < 0.00001)
    for i in range(4, 8):
        assert_equal(x.get(i), Float32(i + 1))
    var zero = RoPETable(8, 8, rotary_dim=0)
    apply_rope_single_head(x, zero, 2)
    # Public metadata mutation must fail before touching x.
    table.max_seq_len = 7
    var old = x.clone()
    var refused = False
    try:
        apply_rope_single_head(x, table, 1)
    except:
        refused = True
    assert_true(refused)
    for i in range(8):
        assert_equal(x.get(i), old.get(i))


def test_weight_index_admission() raises:
    var index = WeightIndex()
    var desc = WeightDescriptor()
    desc.name = "x"
    desc.shape = [2, 3]
    desc.size_bytes = 24
    assert_equal(desc.numel(), 6)
    index.add_weight(desc)
    var refused = False
    try:
        index.add_weight(desc)
    except:
        refused = True
    assert_true(refused)
    assert_equal(index.num_weights(), 1)
    assert_equal(index.total_size_bytes, 24)
    desc.name = "huge"
    desc.shape = [0x7FFFFFFFFFFFFFFF, 2]
    refused = False
    try:
        index.add_weight(desc)
    except:
        refused = True
    assert_true(refused)
    assert_equal(index.num_weights(), 1)
    desc.shape = [1]
    desc.file_offset = 0x7FFFFFFFFFFFFFFF
    refused = False
    try:
        index.add_weight(desc)
    except:
        refused = True
    assert_true(refused)
    assert_equal(index.num_weights(), 1)


def test_numeric_and_weight_map() raises:
    var config = parse_config_json('{"epsilon":1e-6,"hidden_size":7,"theta":1.25E+4,"num_layers":3}')
    assert_equal(config["hidden_size"], 7)
    assert_equal(config["num_layers"], 3)
    assert_true("epsilon" not in config)
    assert_true("theta" not in config)
    var bare = parse_weight_map('{"x":"a.safetensors","y":"b.safetensors"}')
    var wrapped = parse_weight_map('{"metadata":{"total_size":24},"weight_map":{"x":"a.safetensors","y":"b.safetensors"}}')
    assert_equal(bare["x"], wrapped["x"])
    assert_equal(bare["y"], wrapped["y"])
    var invalid = ['{"x":01}', '{"x":1e}', '{"x":1.}', '{"x":2,"x":3}', '{"hidden_size":7}junk']
    for i in range(len(invalid)):
        var refused = False
        try:
            var ignored = parse_config_json(invalid[i])
        except:
            refused = True
        assert_true(refused)


def test_recorded_spans() raises:
    var tape = Tape(32)
    var a = tape.add_variable_from_shape(Shape(2, 3))
    var b = tape.add_variable_from_shape(Shape(3, 2))
    var output = tape.add_variable_from_shape(Shape(2, 2))
    var bad = List[TapeEntry]()
    bad.append(TapeEntry(OP_MATMUL(), a, b, output, cached_int=2, cached_int2=4, cached_int3=2))
    bad.append(TapeEntry(OP_SPLIT(), a, -1, output, cached_int=4))
    bad.append(TapeEntry(OP_BIAS_ADD(), a, b, output, cached_int=0))
    for i in range(len(bad)):
        var refused = False
        try:
            tape.record(bad[i])
        except:
            refused = True
        assert_true(refused)
        assert_equal(tape.num_entries(), 0)
    tape.record(TapeEntry(OP_MATMUL(), a, b, output, cached_int=2, cached_int2=3, cached_int3=2))
    assert_equal(tape.num_entries(), 1)


def main() raises:
    test_rotary_frequency()
    test_weight_index_admission()
    test_numeric_and_weight_map()
    test_recorded_spans()
    print("R2 descriptor/parser/recorded-span independent oracles PASS")
