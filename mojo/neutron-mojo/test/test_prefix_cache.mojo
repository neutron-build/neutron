# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 21: Prefix Cache Tests
# ===----------------------------------------------------------------------=== #

"""Tests for prefix caching (KV cache reuse).

Tests:
1. Token sequence hashing
2. Token sequence matching
3. PrefixMatch defaults
4. PrefixCache creation
5. Store and find prefix
6. Longest prefix match
7. Cache miss
8. Restore to KV cache
9. LRU eviction
10. Hit rate tracking
11. Cache clear
12. Prefix reuse correctness
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.prefix_cache import (
    PrefixCache,
    PrefixMatch,
    hash_token_sequence,
    tokens_match,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)

fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error("FAIL: " + msg + " expected=" + String(b) + " got=" + String(a))


fn _make_tokens(values: List[Int]) -> List[Int]:
    """Helper to create token list."""
    var result = List[Int]()
    for i in range(len(values)):
        result.append(values[i])
    return result^


fn _make_cache(num_layers: Int, max_seq: Int, heads: Int, dim: Int) -> MultiLayerKVCache:
    """Create a KV cache with known values."""
    var cache = MultiLayerKVCache(
        num_layers=num_layers, max_seq_len=max_seq,
        num_kv_heads=heads, head_dim=dim,
    )
    return cache^


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_hash_sequence() raises:
    """Token sequence hashing produces consistent results."""
    var tok1 = List[Int]()
    tok1.append(1)
    tok1.append(2)
    tok1.append(3)

    var tok2 = List[Int]()
    tok2.append(1)
    tok2.append(2)
    tok2.append(3)

    var h1 = hash_token_sequence(tok1, 3)
    var h2 = hash_token_sequence(tok2, 3)
    assert_eq(h1, h2, "same tokens -> same hash")

    var tok3 = List[Int]()
    tok3.append(1)
    tok3.append(2)
    tok3.append(4)
    var h3 = hash_token_sequence(tok3, 3)
    assert_true(h1 != h3, "different tokens -> different hash")

    # Prefix hash
    var h4 = hash_token_sequence(tok1, 2)
    var h5 = hash_token_sequence(tok2, 2)
    assert_eq(h4, h5, "same prefix -> same hash")

    print("  hash_sequence: PASS")


fn test_tokens_match() raises:
    """Token matching works correctly."""
    var a = List[Int]()
    a.append(10)
    a.append(20)
    a.append(30)

    var b = List[Int]()
    b.append(10)
    b.append(20)
    b.append(30)
    b.append(40)

    assert_true(tokens_match(a, b, 3), "prefix match 3")
    assert_true(tokens_match(a, b, 2), "prefix match 2")
    assert_true(tokens_match(a, b, 1), "prefix match 1")
    assert_true(not tokens_match(a, b, 4), "a too short for 4")

    var c = List[Int]()
    c.append(10)
    c.append(99)
    c.append(30)
    assert_true(not tokens_match(a, c, 3), "mismatch at pos 1")

    print("  tokens_match: PASS")


fn test_prefix_match_defaults() raises:
    """PrefixMatch defaults indicate no match."""
    var m = PrefixMatch()
    assert_eq(m.entry_idx, -1, "default entry_idx")
    assert_eq(m.matched_len, 0, "default matched_len")
    assert_true(not m.is_hit(), "default is not a hit")
    print("  prefix_match_defaults: PASS")


fn test_cache_creation() raises:
    """PrefixCache creates with correct params."""
    var pc = PrefixCache(max_entries=4, num_layers=2,
                         num_kv_heads=2, head_dim=4, max_seq_len=16)
    assert_eq(pc.num_entries(), 0, "empty initially")
    assert_eq(pc.total_hits, 0, "no hits initially")
    assert_eq(pc.total_misses, 0, "no misses initially")
    print("  cache_creation: PASS")


fn test_store_and_find() raises:
    """Store a prefix and find it."""
    var pc = PrefixCache(max_entries=4, num_layers=2,
                         num_kv_heads=2, head_dim=4, max_seq_len=16)

    # Create KV cache and populate it
    var cache = MultiLayerKVCache(num_layers=2, max_seq_len=16,
                                  num_kv_heads=2, head_dim=4)
    # Simulate prefill of 3 tokens by setting lengths
    for layer in range(2):
        cache.lengths[layer] = 3
        var layer_base = layer * 16 * 2 * 4
        for i in range(3 * 2 * 4):
            cache.key_data.set(layer_base + i, Float32(i + layer * 100))
            cache.value_data.set(layer_base + i, Float32(i + layer * 200))

    var tokens = List[Int]()
    tokens.append(1)
    tokens.append(2)
    tokens.append(3)

    pc.store(tokens, 3, cache)
    assert_eq(pc.num_entries(), 1, "one entry stored")

    # Find it
    var pm = pc.find_prefix(tokens)
    assert_true(pm.is_hit(), "found stored prefix")
    assert_eq(pm.matched_len, 3, "matched all 3 tokens")
    assert_eq(pc.total_hits, 1, "one hit recorded")

    print("  store_and_find: PASS")


fn test_longest_match() raises:
    """Finds the longest matching prefix."""
    var pc = PrefixCache(max_entries=4, num_layers=1,
                         num_kv_heads=1, head_dim=4, max_seq_len=16)

    var cache = MultiLayerKVCache(num_layers=1, max_seq_len=16,
                                  num_kv_heads=1, head_dim=4)

    # Store prefix of length 2
    var tok2 = List[Int]()
    tok2.append(10)
    tok2.append(20)
    cache.lengths[0] = 2
    pc.store(tok2, 2, cache)

    # Store prefix of length 4
    var tok4 = List[Int]()
    tok4.append(10)
    tok4.append(20)
    tok4.append(30)
    tok4.append(40)
    cache.lengths[0] = 4
    pc.store(tok4, 4, cache)

    # Query with length 5 — should match the 4-token prefix
    var query = List[Int]()
    query.append(10)
    query.append(20)
    query.append(30)
    query.append(40)
    query.append(50)

    var pm = pc.find_prefix(query)
    assert_true(pm.is_hit(), "found match")
    assert_eq(pm.matched_len, 4, "matched longest prefix (4)")

    print("  longest_match: PASS")


fn test_cache_miss() raises:
    """Cache miss returns no match."""
    var pc = PrefixCache(max_entries=4, num_layers=1,
                         num_kv_heads=1, head_dim=4, max_seq_len=16)

    var cache = MultiLayerKVCache(num_layers=1, max_seq_len=16,
                                  num_kv_heads=1, head_dim=4)
    var tok = List[Int]()
    tok.append(1)
    tok.append(2)
    cache.lengths[0] = 2
    pc.store(tok, 2, cache)

    # Query with different tokens
    var query = List[Int]()
    query.append(99)
    query.append(88)
    var pm = pc.find_prefix(query)
    assert_true(not pm.is_hit(), "no match for different tokens")
    assert_eq(pc.total_misses, 1, "miss recorded")

    print("  cache_miss: PASS")


fn test_restore_to_cache() raises:
    """Restoring cached KV data produces correct values."""
    var pc = PrefixCache(max_entries=4, num_layers=1,
                         num_kv_heads=1, head_dim=4, max_seq_len=16)

    # Create and populate source cache
    var src = MultiLayerKVCache(num_layers=1, max_seq_len=16,
                                num_kv_heads=1, head_dim=4)
    src.lengths[0] = 3
    for i in range(3 * 4):  # 3 positions * 1 head * 4 dim
        src.key_data.set(i, Float32(i) * 0.5)
        src.value_data.set(i, Float32(i) * 0.25)

    var tok = List[Int]()
    tok.append(5)
    tok.append(10)
    tok.append(15)
    pc.store(tok, 3, src)

    # Create fresh dest cache and restore
    var dst = MultiLayerKVCache(num_layers=1, max_seq_len=16,
                                num_kv_heads=1, head_dim=4)
    var pm = pc.find_prefix(tok)
    assert_true(pm.is_hit(), "prefix found")
    pc.restore_to_cache(pm, dst)

    # Verify restored values
    assert_eq(dst.lengths[0], 3, "restored length")
    for i in range(3 * 4):
        var expected_k = Float32(i) * 0.5
        var expected_v = Float32(i) * 0.25
        var got_k = dst.key_data.get(i)
        var got_v = dst.value_data.get(i)
        assert_true(got_k == expected_k,
            "key mismatch at " + String(i))
        assert_true(got_v == expected_v,
            "value mismatch at " + String(i))

    print("  restore_to_cache: PASS")


fn test_eviction() raises:
    """LRU eviction removes least-used entry."""
    var pc = PrefixCache(max_entries=2, num_layers=1,
                         num_kv_heads=1, head_dim=2, max_seq_len=8)

    var cache = MultiLayerKVCache(num_layers=1, max_seq_len=8,
                                  num_kv_heads=1, head_dim=2)

    # Store entry A
    var tokA = List[Int]()
    tokA.append(1)
    cache.lengths[0] = 1
    pc.store(tokA, 1, cache)

    # Store entry B
    var tokB = List[Int]()
    tokB.append(2)
    cache.lengths[0] = 1
    pc.store(tokB, 1, cache)

    assert_eq(pc.num_entries(), 2, "2 entries stored")

    # Hit entry A twice to increase its hit count
    _ = pc.find_prefix(tokA)
    _ = pc.find_prefix(tokA)

    # Store entry C — should evict B (least used, 0 hits)
    var tokC = List[Int]()
    tokC.append(3)
    cache.lengths[0] = 1
    pc.store(tokC, 1, cache)

    assert_eq(pc.num_entries(), 2, "still 2 entries after eviction")

    # A should still be there
    var pmA = pc.find_prefix(tokA)
    assert_true(pmA.is_hit(), "A survived eviction")

    # B should be gone
    var pmB = pc.find_prefix(tokB)
    assert_true(not pmB.is_hit(), "B was evicted")

    print("  eviction: PASS")


fn test_hit_rate() raises:
    """Hit rate tracking works."""
    var pc = PrefixCache(max_entries=4, num_layers=1,
                         num_kv_heads=1, head_dim=2, max_seq_len=8)

    var cache = MultiLayerKVCache(num_layers=1, max_seq_len=8,
                                  num_kv_heads=1, head_dim=2)
    var tok = List[Int]()
    tok.append(1)
    cache.lengths[0] = 1
    pc.store(tok, 1, cache)

    _ = pc.find_prefix(tok)  # hit
    _ = pc.find_prefix(tok)  # hit

    var miss_tok = List[Int]()
    miss_tok.append(99)
    _ = pc.find_prefix(miss_tok)  # miss

    # 2 hits, 1 miss => 2/3 ~= 0.667
    var rate = pc.hit_rate()
    assert_true(rate > 0.6 and rate < 0.7, "hit rate ~0.667")
    assert_eq(pc.total_hits, 2, "2 total hits")
    assert_eq(pc.total_misses, 1, "1 total miss")

    print("  hit_rate: PASS")


fn test_cache_clear() raises:
    """Clear removes all entries and resets stats."""
    var pc = PrefixCache(max_entries=4, num_layers=1,
                         num_kv_heads=1, head_dim=2, max_seq_len=8)

    var cache = MultiLayerKVCache(num_layers=1, max_seq_len=8,
                                  num_kv_heads=1, head_dim=2)
    var tok = List[Int]()
    tok.append(1)
    cache.lengths[0] = 1
    pc.store(tok, 1, cache)
    _ = pc.find_prefix(tok)

    pc.clear()
    assert_eq(pc.num_entries(), 0, "cleared entries")
    assert_eq(pc.total_hits, 0, "cleared hits")
    assert_eq(pc.total_misses, 0, "cleared misses")

    print("  cache_clear: PASS")


fn test_prefix_reuse_correctness() raises:
    """Prefix reuse produces same KV values as full prefill."""
    # This tests that storing and restoring preserves exact values
    var pc = PrefixCache(max_entries=4, num_layers=2,
                         num_kv_heads=2, head_dim=4, max_seq_len=16)

    # Create "golden" cache with specific values
    var golden = MultiLayerKVCache(num_layers=2, max_seq_len=16,
                                   num_kv_heads=2, head_dim=4)
    for layer in range(2):
        golden.lengths[layer] = 5
        var base = layer * 16 * 2 * 4
        for i in range(5 * 2 * 4):
            golden.key_data.set(base + i, Float32(layer * 1000 + i) * 0.01)
            golden.value_data.set(base + i, Float32(layer * 2000 + i) * 0.02)

    var tok = List[Int]()
    tok.append(100)
    tok.append(200)
    tok.append(300)
    tok.append(400)
    tok.append(500)
    pc.store(tok, 5, golden)

    # Restore to fresh cache
    var restored = MultiLayerKVCache(num_layers=2, max_seq_len=16,
                                     num_kv_heads=2, head_dim=4)
    var pm = pc.find_prefix(tok)
    pc.restore_to_cache(pm, restored)

    # Verify all values match
    for layer in range(2):
        assert_eq(restored.lengths[layer], 5, "restored length layer " + String(layer))
        var base = layer * 16 * 2 * 4
        for i in range(5 * 2 * 4):
            var g = golden.key_data.get(base + i)
            var r = restored.key_data.get(base + i)
            assert_true(g == r, "key match layer=" + String(layer) + " i=" + String(i))
            var gv = golden.value_data.get(base + i)
            var rv = restored.value_data.get(base + i)
            assert_true(gv == rv, "val match layer=" + String(layer) + " i=" + String(i))

    print("  prefix_reuse_correctness: PASS")


fn main() raises:
    print("test_prefix_cache:")

    test_hash_sequence()
    test_tokens_match()
    test_prefix_match_defaults()
    test_cache_creation()
    test_store_and_find()
    test_longest_match()
    test_cache_miss()
    test_restore_to_cache()
    test_eviction()
    test_hit_rate()
    test_cache_clear()
    test_prefix_reuse_correctness()

    print("ALL PASSED (12 tests)")
