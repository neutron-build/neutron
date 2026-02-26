# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Tokenizer Loading Tests
# ===----------------------------------------------------------------------=== #

"""Tests for GGUF tokenizer extraction and vocab file loading."""

from neutron_mojo.nn.tokenizer import (
    BPETokenizer,
    MergePair,
    _parse_merge_rule,
    load_gguf_tokenizer,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected " + String(b) + " got " + String(a)
        )


fn test_parse_merge_rule() raises:
    """Test parsing merge rule strings."""
    var p1 = _parse_merge_rule("hello world")
    assert_true(p1.left == "hello", "left should be 'hello'")
    assert_true(p1.right == "world", "right should be 'world'")

    var p2 = _parse_merge_rule("a b")
    assert_true(p2.left == "a", "left 'a'")
    assert_true(p2.right == "b", "right 'b'")

    var p3 = _parse_merge_rule("th e")
    assert_true(p3.left == "th", "left 'th'")
    assert_true(p3.right == "e", "right 'e'")

    # No space: entire string is left
    var p4 = _parse_merge_rule("nospace")
    assert_true(p4.left == "nospace", "no space: left = whole string")
    assert_true(p4.right == "", "no space: right = empty")

    print("  parse_merge_rule: PASS")


fn test_load_gguf_tokenizer_basic() raises:
    """Test creating a tokenizer from GGUF-style vocab data."""
    var vocab = List[String]()
    vocab.append("<bos>")   # 0
    vocab.append("<eos>")   # 1
    vocab.append("h")       # 2
    vocab.append("e")       # 3
    vocab.append("l")       # 4
    vocab.append("o")       # 5
    vocab.append("he")      # 6
    vocab.append("ll")      # 7
    vocab.append("hel")     # 8

    var scores = List[Float64]()
    for _ in range(len(vocab)):
        scores.append(0.0)

    var merges = List[String]()
    merges.append("h e")    # 0: h+e -> he
    merges.append("l l")    # 1: l+l -> ll
    merges.append("he l")   # 2: he+l -> hel

    var tok = load_gguf_tokenizer(vocab, scores, merges, bos_id=0, eos_id=1)

    assert_eq(tok.vocab_size, 9, "vocab_size 9")
    assert_eq(tok.bos_id, 0, "bos_id 0")
    assert_eq(tok.eos_id, 1, "eos_id 1")

    # Test encode/decode roundtrip with characters in vocab
    var ids = tok.encode("helo")
    var decoded = tok.decode(ids)
    assert_true(decoded == "helo", "roundtrip: 'helo'")

    print("  load_gguf_tokenizer_basic: PASS")


fn test_special_token_ids() raises:
    """Test BOS/EOS token ID extraction."""
    var vocab = List[String]()
    vocab.append("a")       # 0
    vocab.append("<s>")     # 1
    vocab.append("</s>")    # 2
    vocab.append("b")       # 3

    var scores = List[Float64]()
    for _ in range(4):
        scores.append(0.0)

    var merges = List[String]()

    var tok = load_gguf_tokenizer(vocab, scores, merges, bos_id=1, eos_id=2)
    assert_eq(tok.bos_id, 1, "bos_id 1")
    assert_eq(tok.eos_id, 2, "eos_id 2")

    print("  special_token_ids: PASS")


fn test_empty_merges() raises:
    """Test tokenizer with vocab only, no merges."""
    var vocab = List[String]()
    vocab.append("a")
    vocab.append("b")
    vocab.append("c")

    var scores = List[Float64]()
    for _ in range(3):
        scores.append(0.0)

    var merges = List[String]()

    var tok = load_gguf_tokenizer(vocab, scores, merges, bos_id=-1, eos_id=-1)
    assert_eq(tok.vocab_size, 3, "3 tokens")
    assert_eq(len(tok.merges), 0, "no merges")

    # Encoding should work with individual chars
    var ids = tok.encode("abc")
    assert_eq(len(ids), 3, "3 token IDs")
    assert_eq(ids[0], 0, "a=0")
    assert_eq(ids[1], 1, "b=1")
    assert_eq(ids[2], 2, "c=2")

    print("  empty_merges: PASS")


fn test_encode_decode_roundtrip() raises:
    """Test full encode-decode roundtrip with merges."""
    var vocab = List[String]()
    vocab.append("t")       # 0
    vocab.append("h")       # 1
    vocab.append("e")       # 2
    vocab.append("th")      # 3
    vocab.append("the")     # 4

    var scores = List[Float64]()
    for _ in range(5):
        scores.append(0.0)

    var merges = List[String]()
    merges.append("t h")     # t+h -> th
    merges.append("th e")    # th+e -> the

    var tok = load_gguf_tokenizer(vocab, scores, merges, bos_id=-1, eos_id=-1)

    var ids = tok.encode("the")
    assert_eq(len(ids), 1, "should merge to single token")
    assert_eq(ids[0], 4, "should be 'the' token")

    var decoded = tok.decode(ids)
    assert_true(decoded == "the", "decode should give 'the'")

    print("  encode_decode_roundtrip: PASS")


fn test_byte_level_token_handling() raises:
    """Test that byte-level tokens (non-ASCII) are handled."""
    var vocab = List[String]()
    vocab.append("a")
    vocab.append("b")
    # Add a byte-fallback token representation
    vocab.append("<0xFF>")

    var scores = List[Float64]()
    for _ in range(3):
        scores.append(0.0)

    var merges = List[String]()

    var tok = load_gguf_tokenizer(vocab, scores, merges, bos_id=-1, eos_id=-1)
    assert_eq(tok.vocab_size, 3, "3 tokens including byte-level")

    # Decode the byte-level token should give the token string back
    var decoded = tok.decode_single(2)
    assert_true(decoded == "<0xFF>", "byte-level token decodes to its string")

    print("  byte_level_token_handling: PASS")


fn main() raises:
    print("test_tokenizer_loading:")

    test_parse_merge_rule()
    test_load_gguf_tokenizer_basic()
    test_special_token_ids()
    test_empty_merges()
    test_encode_decode_roundtrip()
    test_byte_level_token_handling()

    print("ALL PASSED (6 tests)")
