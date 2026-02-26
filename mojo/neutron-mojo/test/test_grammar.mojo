# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 19: Grammar-Constrained Sampling Tests
# ===----------------------------------------------------------------------=== #

"""Tests for JSON grammar FSM and constrained sampling.

Tests:
1. Character classification helpers
2. FSM initial state
3. Simple JSON string parsing
4. JSON number parsing
5. JSON boolean/null parsing
6. JSON object parsing
7. JSON array parsing
8. Nested JSON parsing
9. Invalid JSON detection
10. Valid chars from START state
11. Valid chars from AFTER_VALUE state
12. Grammar mask — first byte
13. Grammar mask — full validation
14. FSM advance with token string
15. FSM copy independence
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.grammar import (
    GrammarState,
    JsonFSM,
    apply_grammar_mask,
    apply_grammar_mask_full,
    advance_fsm,
    is_digit,
    is_whitespace,
    is_hex,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error("FAIL: " + msg + " expected=" + String(b) + " got=" + String(a))


fn _feed_string(mut fsm: JsonFSM, s: String):
    """Feed all bytes of a string into the FSM."""
    for i in range(len(s)):
        fsm.feed_char(ord(s[byte=i]))


fn _char_in_list(c: Int, valid: List[Int]) -> Bool:
    """Check if char is in valid list."""
    for i in range(len(valid)):
        if valid[i] == c:
            return True
    return False


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_char_classification() raises:
    """Character classification helpers work correctly."""
    assert_true(is_digit(48), "0 is digit")
    assert_true(is_digit(57), "9 is digit")
    assert_true(not is_digit(47), "/ is not digit")
    assert_true(not is_digit(58), ": is not digit")

    assert_true(is_whitespace(32), "space is ws")
    assert_true(is_whitespace(10), "newline is ws")
    assert_true(is_whitespace(9), "tab is ws")
    assert_true(is_whitespace(13), "CR is ws")
    assert_true(not is_whitespace(65), "A is not ws")

    assert_true(is_hex(48), "0 is hex")
    assert_true(is_hex(65), "A is hex")
    assert_true(is_hex(97), "a is hex")
    assert_true(is_hex(70), "F is hex")
    assert_true(not is_hex(71), "G is not hex")

    print("  char_classification: PASS")


fn test_fsm_initial_state() raises:
    """FSM starts in START state."""
    var fsm = JsonFSM()

    assert_true(fsm.state == GrammarState(GrammarState.START), "initial state is START")
    assert_true(not fsm.is_done(), "not done initially")
    assert_true(not fsm.is_error(), "not error initially")
    assert_eq(fsm.depth, 0, "depth starts at 0")

    print("  fsm_initial_state: PASS")


fn test_json_string() raises:
    """FSM correctly parses a JSON string."""
    var fsm = JsonFSM()
    _feed_string(fsm, '"hello"')

    assert_true(fsm.is_done(), "string -> DONE")
    assert_true(not fsm.is_error(), "string no error")

    print("  json_string: PASS")


fn test_json_number() raises:
    """FSM correctly parses JSON numbers."""
    # Integer
    var fsm1 = JsonFSM()
    _feed_string(fsm1, "42")
    # Number ends when no more digits — need a delimiter
    # For standalone numbers, feed a trailing space to finalize
    fsm1.feed_char(32)  # space triggers number end
    assert_true(fsm1.is_done(), "integer 42 -> DONE")

    # Negative float
    var fsm2 = JsonFSM()
    _feed_string(fsm2, "-3.14")
    fsm2.feed_char(32)
    assert_true(fsm2.is_done(), "float -3.14 -> DONE")

    # Scientific notation
    var fsm3 = JsonFSM()
    _feed_string(fsm3, "1e10")
    fsm3.feed_char(32)
    assert_true(fsm3.is_done(), "sci 1e10 -> DONE")

    print("  json_number: PASS")


fn test_json_literals() raises:
    """FSM correctly parses true, false, null."""
    var fsm1 = JsonFSM()
    _feed_string(fsm1, "true")
    assert_true(fsm1.is_done(), "true -> DONE")

    var fsm2 = JsonFSM()
    _feed_string(fsm2, "false")
    assert_true(fsm2.is_done(), "false -> DONE")

    var fsm3 = JsonFSM()
    _feed_string(fsm3, "null")
    assert_true(fsm3.is_done(), "null -> DONE")

    print("  json_literals: PASS")


fn test_json_object() raises:
    """FSM correctly parses a JSON object."""
    var fsm = JsonFSM()
    _feed_string(fsm, '{"key":"val"}')

    assert_true(fsm.is_done(), "object -> DONE")
    assert_true(not fsm.is_error(), "object no error")
    assert_eq(fsm.depth, 0, "depth back to 0")

    # Empty object
    var fsm2 = JsonFSM()
    _feed_string(fsm2, "{}")
    assert_true(fsm2.is_done(), "empty object -> DONE")

    print("  json_object: PASS")


fn test_json_array() raises:
    """FSM correctly parses a JSON array."""
    var fsm = JsonFSM()
    _feed_string(fsm, "[1,2,3]")
    # Numbers in array need delimiters; commas and ] serve as delimiters
    assert_true(fsm.is_done(), "array -> DONE")

    # Empty array
    var fsm2 = JsonFSM()
    _feed_string(fsm2, "[]")
    assert_true(fsm2.is_done(), "empty array -> DONE")

    print("  json_array: PASS")


fn test_nested_json() raises:
    """FSM handles nested objects and arrays."""
    var fsm = JsonFSM()
    _feed_string(fsm, '{"a":[1,{"b":true}]}')

    assert_true(fsm.is_done(), "nested -> DONE")
    assert_eq(fsm.depth, 0, "nested depth back to 0")

    print("  nested_json: PASS")


fn test_invalid_json() raises:
    """FSM detects invalid JSON."""
    # Leading comma
    var fsm1 = JsonFSM()
    _feed_string(fsm1, ",")
    assert_true(fsm1.is_error(), "leading comma -> ERROR")

    # Unclosed string (check if NOT done)
    var fsm2 = JsonFSM()
    _feed_string(fsm2, '"hello')
    assert_true(not fsm2.is_done(), "unclosed string not done")

    # Bad literal
    var fsm3 = JsonFSM()
    _feed_string(fsm3, "tru")
    fsm3.feed_char(120)  # 'x' instead of 'e'
    assert_true(fsm3.is_error(), "bad literal -> ERROR")

    print("  invalid_json: PASS")


fn test_valid_chars_start() raises:
    """Valid chars from START state include value starters."""
    var fsm = JsonFSM()
    var valid = fsm.get_valid_chars()

    assert_true(_char_in_list(123, valid), "{ valid at start")  # {
    assert_true(_char_in_list(91, valid), "[ valid at start")   # [
    assert_true(_char_in_list(34, valid), "quote valid at start")  # "
    assert_true(_char_in_list(116, valid), "t valid at start")  # t (true)
    assert_true(_char_in_list(102, valid), "f valid at start")  # f (false)
    assert_true(_char_in_list(110, valid), "n valid at start")  # n (null)
    assert_true(_char_in_list(48, valid), "0 valid at start")   # 0
    assert_true(_char_in_list(45, valid), "- valid at start")   # -
    assert_true(not _char_in_list(125, valid), "} not valid at start")
    assert_true(not _char_in_list(44, valid), ", not valid at start")

    print("  valid_chars_start: PASS")


fn test_valid_chars_after_value() raises:
    """Valid chars after value in object include , and }."""
    var fsm = JsonFSM()
    _feed_string(fsm, '{"k":"v"')
    # After "v" closes, we're AFTER_VALUE inside an object
    var valid = fsm.get_valid_chars()

    assert_true(_char_in_list(44, valid), ", valid after value in obj")
    assert_true(_char_in_list(125, valid), "} valid after value in obj")
    assert_true(_char_in_list(32, valid), "space valid after value")
    assert_true(not _char_in_list(91, valid), "[ not valid after value in obj")

    print("  valid_chars_after_value: PASS")


fn test_grammar_mask_first_byte() raises:
    """apply_grammar_mask masks invalid first-byte tokens."""
    var vocab = List[String]()
    vocab.append("{")    # 0 — valid at start
    vocab.append("}")    # 1 — invalid at start
    vocab.append('"')    # 2 — valid at start
    vocab.append(",")    # 3 — invalid at start
    vocab.append("true") # 4 — valid at start (first byte 't')
    vocab.append("xyz")  # 5 — invalid

    var logits = Tensor[DType.float32](Shape(6))
    for i in range(6):
        logits.set(i, 1.0)

    var fsm = JsonFSM()
    apply_grammar_mask(logits, 6, fsm, vocab, -1)

    assert_true(logits.get(0) > -1e20, "{ not masked")
    assert_true(logits.get(1) < -1e20, "} masked at start")
    assert_true(logits.get(2) > -1e20, "quote not masked")
    assert_true(logits.get(3) < -1e20, ", masked at start")
    assert_true(logits.get(4) > -1e20, "true not masked")
    assert_true(logits.get(5) < -1e20, "xyz masked at start")

    print("  grammar_mask_first_byte: PASS")


fn test_grammar_mask_full() raises:
    """apply_grammar_mask_full validates all bytes of tokens."""
    var vocab = List[String]()
    vocab.append('{"')    # 0 — valid: { then start key
    vocab.append("{}")    # 1 — valid: empty object
    vocab.append("{,")    # 2 — invalid: , after { is bad
    vocab.append('"hi"')  # 3 — valid: complete string

    var logits = Tensor[DType.float32](Shape(4))
    for i in range(4):
        logits.set(i, 1.0)

    var fsm = JsonFSM()
    apply_grammar_mask_full(logits, 4, fsm, vocab, -1)

    assert_true(logits.get(0) > -1e20, '{" not masked')
    assert_true(logits.get(1) > -1e20, "{} not masked")
    assert_true(logits.get(2) < -1e20, "{, masked (invalid)")
    assert_true(logits.get(3) > -1e20, '"hi" not masked')

    print("  grammar_mask_full: PASS")


fn test_advance_fsm() raises:
    """advance_fsm correctly updates FSM state."""
    var fsm = JsonFSM()
    advance_fsm(fsm, '{"key"')
    # After {"key" we should be in AFTER_KEY state
    assert_true(fsm.state == GrammarState(GrammarState.AFTER_KEY), "after key string")
    assert_eq(fsm.depth, 1, "depth is 1 in object")

    advance_fsm(fsm, ':')
    assert_true(fsm.state == GrammarState(GrammarState.AFTER_COLON), "after colon")

    advance_fsm(fsm, '"val"')
    assert_true(fsm.state == GrammarState(GrammarState.AFTER_VALUE), "after value")

    advance_fsm(fsm, "}")
    assert_true(fsm.is_done(), "after } -> DONE")

    print("  advance_fsm: PASS")


fn test_fsm_copy_independence() raises:
    """Copied FSM is independent of original."""
    var fsm = JsonFSM()
    _feed_string(fsm, '{"a"')

    var fsm2 = fsm.copy()
    # Advance original to error
    fsm.feed_char(91)  # '[' — invalid after key, should be ':'
    assert_true(fsm.is_error(), "original errored")

    # Copy should still be in AFTER_KEY
    assert_true(fsm2.state == GrammarState(GrammarState.AFTER_KEY), "copy preserved")
    assert_true(not fsm2.is_error(), "copy not errored")

    print("  fsm_copy_independence: PASS")


fn main() raises:
    print("test_grammar:")

    test_char_classification()
    test_fsm_initial_state()
    test_json_string()
    test_json_number()
    test_json_literals()
    test_json_object()
    test_json_array()
    test_nested_json()
    test_invalid_json()
    test_valid_chars_start()
    test_valid_chars_after_value()
    test_grammar_mask_first_byte()
    test_grammar_mask_full()
    test_advance_fsm()
    test_fsm_copy_independence()

    print("ALL PASSED (15 tests)")
