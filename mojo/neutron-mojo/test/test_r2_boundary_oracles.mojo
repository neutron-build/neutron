"""Boundary oracles authored under capacity hold; native execution pending."""
from std.testing import assert_true, assert_false, assert_equal
from std.ffi import c_int
from neutron_mojo.io.binary_reader import _map_readonly_descriptor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.dlpack.exchange import tensor_to_dlpack, dlpack_to_tensor, dlpack_free
from neutron_mojo.nn.tokenizer import BPETokenizer, build_byte_level_vocab, _byte_symbol, _symbol_byte
from neutron_mojo.nn.streaming import _utf8_complete_prefix
from neutron_mojo.nn.grammar import JsonFSM, apply_grammar_mask, advance_fsm
from neutron_mojo.fusion.egraph import EGraph, CanonicalNode
from neutron_mojo.fusion.graph import OpKind
from neutron_mojo.fusion.pattern import Pattern, Bindings, match_pattern_egraph
from neutron_mojo.fusion.rules import RuleSet, rule_mul_zero
from neutron_mojo.fusion.rewrite import RewriteEngine


def test_mapping_failure() raises:
    var refused = False
    try:
        # A positive length and invalid borrowed fd force MAP_FAILED/EBADF.
        var ptr = _map_readonly_descriptor(c_int(-1), 4096)
    except error:
        refused = String(error).find("errno=9") >= 0
    assert_true(refused)


def test_owned_exchange() raises:
    var source = Tensor[DType.float32](Shape(2, 3))
    for i in range(6):
        source.set(i, Float32(i * i - 3))
    var owner = tensor_to_dlpack(source)
    var borrowed = owner.borrow()
    var borrowed_view = borrowed
    var roundtrip = dlpack_to_tensor(borrowed_view)
    assert_true(roundtrip.shape() == Shape(2, 3))
    for i in range(6):
        assert_equal(roundtrip.data_ptr()[i], Float32(i * i - 3))
    var moved = owner^
    # Explicit release twice, followed by destructor: only one owner can free.
    dlpack_free(moved)
    dlpack_free(moved)
    var refused = False
    try:
        var invalid_borrow = moved.borrow()
    except:
        refused = True
    assert_true(refused)
    # The independently copied tensor survives release. Never use stale alias.
    assert_equal(roundtrip.data_ptr()[5], Float32(22))


def test_unicode_bytes() raises:
    var text = String("ASCII café 中文 😀\n")
    var latin = BPETokenizer()
    build_byte_level_vocab(latin)
    assert_equal(latin.decode(latin.encode(text)), text)
    var gpt = BPETokenizer()
    gpt.byte_encoding = 1
    build_byte_level_vocab(gpt)
    assert_equal(gpt.decode(gpt.encode(text)), text)
    # Independent published GPT-2 mapping anchors, not encode/decode alone.
    assert_equal(_byte_symbol(32, 1), String("Ġ"))
    assert_equal(_byte_symbol(10, 1), String("Ċ"))
    assert_equal(_symbol_byte(288, 1), UInt8(32))
    assert_equal(_symbol_byte(266, 1), UInt8(10))
    var bytes = List[UInt8]()
    for v in [0xF0, 0x9F, 0x98, 0x80]:
        bytes.append(UInt8(v))
    for count in range(1, 4):
        var partial = List[UInt8]()
        for i in range(count):
            partial.append(bytes[i])
        assert_equal(_utf8_complete_prefix(partial), 0)
    assert_equal(_utf8_complete_prefix(bytes), 4)
    var malformed = List[UInt8]()
    for v in [0xED, 0xA0, 0x80]:
        malformed.append(UInt8(v))
    var refused = False
    try:
        var size = _utf8_complete_prefix(malformed)
    except:
        refused = True
    assert_true(refused)


def test_json_grammar() raises:
    var valid = ["0", "-0", "12", "1.25", "1e-2", "true", "false", "null", "[]", "{}", "{\"x\":[1,\"\\u00e9\"]}"]
    for i in range(len(valid)):
        var fsm = JsonFSM()
        advance_fsm(fsm, valid[i])
        assert_false(fsm.is_error())
        assert_true(fsm.can_end())
    var invalid = ["01", "-", "1.", "1e", "1e+", "1e++2", "[1,]", "{\"x\":1,}", "\"\\u12X4\"", "\"\\u123\"", "true false", "\"\n\""]
    for i in range(len(invalid)):
        var fsm = JsonFSM()
        advance_fsm(fsm, invalid[i])
        assert_true(fsm.is_error() or not fsm.can_end())
    # Both tokens start legally; only the complete valid one may survive.
    var fsm = JsonFSM()
    var vocab = List[String]()
    vocab.append("[1,]")
    vocab.append("[1]")
    vocab.append("<eos>")
    var logits = Tensor[DType.float32](Shape(3))
    logits.set(0, 100)
    logits.set(1, 0)
    logits.set(2, 200)
    apply_grammar_mask(logits, 3, fsm, vocab, 2)
    assert_true(logits.data_ptr()[0] < Float32(-1e30))
    assert_equal(logits.data_ptr()[1], Float32(0))
    assert_true(logits.data_ptr()[2] < Float32(-1e30))
    var dead_vocab = List[String]()
    dead_vocab.append("]")
    dead_vocab.append("}")
    dead_vocab.append("<eos>")
    var refused = False
    try:
        apply_grammar_mask(logits, 3, fsm, dead_vocab, 2)
    except:
        refused = True
    assert_true(refused)


def test_egraph_identity_and_rebuild() raises:
    var eg = EGraph()
    var x = eg.add(CanonicalNode(OpKind.Input))
    var y = eg.add(CanonicalNode(OpKind.Input))
    assert_true(x != y)
    var zero = eg.add_scalar_f32(0)
    var one = eg.add_scalar_f32(1)
    var zero_again = eg.add_scalar_f32(0)
    assert_true(zero != one)
    assert_true(eg.find(zero) == eg.find(zero_again))
    var fx = eg.add(CanonicalNode(OpKind.ReLU, x))
    var fy = eg.add(CanonicalNode(OpKind.ReLU, y))
    var ffx = eg.add(CanonicalNode(OpKind.ReLU, fx))
    var ffy = eg.add(CanonicalNode(OpKind.ReLU, fy))
    var merged = eg.merge(x, y)
    assert_true(eg.find(fx) == eg.find(fy))
    assert_true(eg.find(ffx) == eg.find(ffy))
    # Repeated variables compare canonical classes rather than throwing.
    var pattern = Pattern.operation(OpKind.Add)
    pattern.add_child(Pattern.variable(0))
    pattern.add_child(Pattern.variable(0))
    var sum = eg.add(CanonicalNode(OpKind.Add, x, y))
    var bindings = Bindings(1)
    assert_true(match_pattern_egraph(pattern, sum, bindings, eg))
    # Supplying an unsafe rule directly still requires explicit fast math.
    var product = eg.add(CanonicalNode(OpKind.Mul, x, zero))
    var rules = RuleSet()
    rules.add_rule(rule_mul_zero())
    var engine = RewriteEngine()
    var stats = engine.run_phase1(eg, rules)
    assert_equal(stats.rules_applied, 0)
    assert_true(eg.find(product) != eg.find(zero))


def main() raises:
    test_mapping_failure()
    test_owned_exchange()
    test_unicode_bytes()
    test_json_grammar()
    test_egraph_identity_and_rebuild()
    print("PASS: r2 independent boundary oracles")
