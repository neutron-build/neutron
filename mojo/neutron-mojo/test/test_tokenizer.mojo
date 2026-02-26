# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Tokenizer Tests
# ===----------------------------------------------------------------------=== #

"""Tests for BPE tokenizer."""

from neutron_mojo.nn.tokenizer import (
    BPETokenizer,
    MergeRule,
    build_test_tokenizer,
    build_byte_level_vocab,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_empty_tokenizer() raises:
    """Test empty tokenizer creation."""
    var tok = BPETokenizer()
    assert_true(tok.vocab_size == 0, "starts empty")
    assert_true(tok.bos_id == -1, "no bos")
    assert_true(tok.eos_id == -1, "no eos")

    print("  empty_tokenizer: PASS")


fn test_add_tokens() raises:
    """Test adding tokens to vocabulary."""
    var tok = BPETokenizer()
    var id0 = tok.add_token("hello")
    var id1 = tok.add_token("world")

    assert_true(id0 == 0, "first token id=0")
    assert_true(id1 == 1, "second token id=1")
    assert_true(tok.vocab_size == 2, "vocab size")
    assert_true(tok.id_to_token[0] == "hello", "id→token")
    assert_true(tok.token_to_id["hello"] == 0, "token→id")

    print("  add_tokens: PASS")


fn test_special_tokens() raises:
    """Test special token assignment."""
    var tok = BPETokenizer()
    var bos = tok.add_special_token("<s>", "bos")
    var eos = tok.add_special_token("</s>", "eos")
    var unk = tok.add_special_token("<unk>", "unk")

    assert_true(tok.bos_id == bos, "bos assigned")
    assert_true(tok.eos_id == eos, "eos assigned")
    assert_true(tok.unk_id == unk, "unk assigned")

    print("  special_tokens: PASS")


fn test_add_merges() raises:
    """Test merge rule addition and lookup."""
    var tok = BPETokenizer()
    tok.add_merge("a", "b")
    tok.add_merge("ab", "c")

    assert_true(tok.get_merge_priority("a", "b") == 0, "first merge priority")
    assert_true(tok.get_merge_priority("ab", "c") == 1, "second merge priority")
    assert_true(tok.get_merge_priority("x", "y") == -1, "no merge exists")

    print("  add_merges: PASS")


fn test_encode_single_chars() raises:
    """Test encoding text that stays as single characters."""
    var tok = build_test_tokenizer()

    # "abc" → should be [a, b, c] = [4, 5, 6]
    # No merge rule for a+b or b+c
    var ids = tok.encode("abc")
    assert_true(len(ids) == 3, "3 tokens")
    assert_true(ids[0] == 4, "a=4")
    assert_true(ids[1] == 5, "b=5")
    assert_true(ids[2] == 6, "c=6")

    print("  encode_single_chars: PASS")


fn test_encode_with_merge() raises:
    """Test encoding text that triggers BPE merges."""
    var tok = build_test_tokenizer()

    # "the" should merge: t+h → th (priority 0), then th+e → the (priority 3)
    # Or: h+e → he (priority 1), then t+he → the (priority 2)
    # Either way, result should be ["the"] = [23]
    var ids = tok.encode("the")
    assert_true(len(ids) == 1, "merged to 1 token")
    assert_true(ids[0] == 23, "the=23")

    print("  encode_with_merge: PASS")


fn test_encode_partial_merge() raises:
    """Test encoding where only some pairs merge."""
    var tok = build_test_tokenizer()

    # "thin" → t+h merge → "th", then i+n merge → "in"
    # Result: ["th", "in"] = [21, 24]
    var ids = tok.encode("thin")
    assert_true(len(ids) == 2, "2 tokens")
    assert_true(ids[0] == 21, "th=21")
    assert_true(ids[1] == 24, "in=24")

    print("  encode_partial_merge: PASS")


fn test_encode_with_spaces() raises:
    """Test encoding text with spaces."""
    var tok = build_test_tokenizer()

    # "a b" → ["a", " ", "b"] = [4, 3, 5]
    var ids = tok.encode("a b")
    assert_true(len(ids) == 3, "3 tokens")
    assert_true(ids[0] == 4, "a")
    assert_true(ids[1] == 3, "space")
    assert_true(ids[2] == 5, "b")

    print("  encode_with_spaces: PASS")


fn test_encode_unknown() raises:
    """Test encoding with unknown characters falls back to UNK."""
    var tok = build_test_tokenizer()

    # "x" is not in the vocab → should get UNK (id=2)
    var ids = tok.encode("x")
    assert_true(len(ids) == 1, "1 token")
    assert_true(ids[0] == 2, "unknown → unk_id=2")

    print("  encode_unknown: PASS")


fn test_encode_empty() raises:
    """Test encoding empty string."""
    var tok = build_test_tokenizer()
    var ids = tok.encode("")
    assert_true(len(ids) == 0, "empty input → empty output")

    print("  encode_empty: PASS")


fn test_encode_with_bos() raises:
    """Test encoding with BOS token prepended."""
    var tok = build_test_tokenizer()

    var ids = tok.encode_with_special("ab", add_bos=True)
    assert_true(len(ids) == 3, "bos + a + b")
    assert_true(ids[0] == 0, "bos_id=0")
    assert_true(ids[1] == 4, "a=4")
    assert_true(ids[2] == 5, "b=5")

    print("  encode_with_bos: PASS")


fn test_decode_basic() raises:
    """Test basic decoding."""
    var tok = build_test_tokenizer()

    var ids = List[Int]()
    ids.append(4)   # a
    ids.append(5)   # b
    ids.append(6)   # c

    var text = tok.decode(ids)
    assert_true(text == "abc", "decode abc")

    print("  decode_basic: PASS")


fn test_decode_merged_tokens() raises:
    """Test decoding merged tokens."""
    var tok = build_test_tokenizer()

    var ids = List[Int]()
    ids.append(23)  # "the"
    ids.append(3)   # " "
    ids.append(24)  # "in"

    var text = tok.decode(ids)
    assert_true(text == "the in", "decode 'the in'")

    print("  decode_merged_tokens: PASS")


fn test_decode_skips_special() raises:
    """Test that decode skips BOS/EOS tokens."""
    var tok = build_test_tokenizer()

    var ids = List[Int]()
    ids.append(0)   # <bos>
    ids.append(4)   # a
    ids.append(5)   # b
    ids.append(1)   # <eos>

    var text = tok.decode(ids)
    assert_true(text == "ab", "skips special tokens")

    print("  decode_skips_special: PASS")


fn test_decode_single() raises:
    """Test single-token decode."""
    var tok = build_test_tokenizer()

    assert_true(tok.decode_single(23) == "the", "decode 'the'")
    assert_true(tok.decode_single(999) == "<unk>", "oob → <unk>")

    print("  decode_single: PASS")


fn test_roundtrip() raises:
    """Test encode → decode roundtrip."""
    var tok = build_test_tokenizer()

    var texts = List[String]()
    texts.append("the")
    texts.append("thin")
    texts.append("a b c")
    texts.append("is")

    for i in range(len(texts)):
        var original = texts[i]
        var ids = tok.encode(original)
        var decoded = tok.decode(ids)
        assert_true(decoded == original, "roundtrip: " + original)

    print("  roundtrip: PASS")


fn test_merge_rule_struct() raises:
    """Test MergeRule struct."""
    var rule = MergeRule("ab", "cd", 5)
    assert_true(rule.left == "ab", "left")
    assert_true(rule.right == "cd", "right")
    assert_true(rule.merged == "abcd", "merged")
    assert_true(rule.priority == 5, "priority")

    print("  merge_rule_struct: PASS")


fn test_byte_level_vocab() raises:
    """Test byte-level vocab builder."""
    var tok = BPETokenizer()
    build_byte_level_vocab(tok)

    assert_true(tok.vocab_size == 256, "256 byte tokens")
    # Printable ASCII 'A' (65) should be "A"
    assert_true(tok.id_to_token[65] == "A", "byte 65 = A")
    # Space (32) should be " "
    assert_true(tok.id_to_token[32] == " ", "byte 32 = space")

    print("  byte_level_vocab: PASS")


fn main() raises:
    print("test_tokenizer:")

    test_empty_tokenizer()
    test_add_tokens()
    test_special_tokens()
    test_add_merges()
    test_encode_single_chars()
    test_encode_with_merge()
    test_encode_partial_merge()
    test_encode_with_spaces()
    test_encode_unknown()
    test_encode_empty()
    test_encode_with_bos()
    test_decode_basic()
    test_decode_merged_tokens()
    test_decode_skips_special()
    test_decode_single()
    test_roundtrip()
    test_merge_rule_struct()
    test_byte_level_vocab()

    print("ALL PASSED")
