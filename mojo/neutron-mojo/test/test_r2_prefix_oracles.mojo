"""Independent multi-layer prefix stride, capacity and stale-match fixtures."""
from std.testing import assert_true, assert_equal
from neutron_mojo.nn.prefix_cache import PrefixCache
from neutron_mojo.nn.kv_cache import MultiLayerKVCache


def fill_source(mut cache: MultiLayerKVCache):
    for layer in range(cache.num_layers):
        for pos in range(cache.max_seq_len):
            for d in range(4):
                var offset = (layer * cache.max_seq_len + pos) * 4 + d
                cache.key_data.set(offset,Float32(layer * 1000 + pos * 10 + d))
                cache.value_data.set(offset,Float32(-layer * 1000 - pos * 10 - d - 1))
        cache.lengths[layer] = cache.max_seq_len


def assert_snapshot(cache: MultiLayerKVCache, prefix_len: Int) raises:
    for layer in range(2):
        assert_equal(cache.lengths[layer],prefix_len)
        for pos in range(cache.max_seq_len):
            for d in range(4):
                var offset = (layer * cache.max_seq_len + pos) * 4 + d
                if pos < prefix_len:
                    assert_equal(cache.key_data.get(offset),Float32(layer * 1000 + pos * 10 + d))
                    assert_equal(cache.value_data.get(offset),Float32(-layer * 1000 - pos * 10 - d - 1))
                else:
                    assert_equal(cache.key_data.get(offset),Float32(77))
                    assert_equal(cache.value_data.get(offset),Float32(88))


def test_prefix_matrix() raises:
    var source = MultiLayerKVCache(2,5,2,2)
    fill_source(source)
    var prefixes = PrefixCache(1,2,2,2,7)
    var tokens = List[Int]()
    tokens.append(1)
    tokens.append(2)
    tokens.append(3)
    prefixes.store(tokens^,3,source)
    var query = List[Int]()
    query.append(1)
    query.append(2)
    query.append(3)
    query.append(4)
    var prefix_hit = prefixes.find_prefix(query^)
    assert_true(prefix_hit.is_hit())
    for capacity in range(3, 10, 3):
        var destination = MultiLayerKVCache(2,capacity,2,2)
        for i in range(destination.key_data.numel()):
            destination.key_data.set(i,77)
            destination.value_data.set(i,88)
        prefixes.restore_to_cache(prefix_hit,destination)
        assert_snapshot(destination,3)
    var small = MultiLayerKVCache(2,2,2,2)
    for i in range(small.key_data.numel()):
        small.key_data.set(i,77)
    var refused = False
    try:
        prefixes.restore_to_cache(prefix_hit,small)
    except:
        refused = True
    assert_true(refused)
    for i in range(small.key_data.numel()):
        assert_equal(small.key_data.get(i),Float32(77))
    for layer in range(2):
        assert_equal(small.lengths[layer],0)
    # One-entry eviction makes the saved match stale even if its index is reused.
    var more = List[Int]()
    more.append(4)
    more.append(5)
    more.append(6)
    prefixes.store(more^,3,source)
    var destination = MultiLayerKVCache(2,6,2,2)
    refused = False
    try:
        prefixes.restore_to_cache(prefix_hit,destination)
    except:
        refused = True
    assert_true(refused)
    assert_equal(destination.lengths[0],0)
    assert_equal(destination.lengths[1],0)
    var current = prefixes.find_prefix([4,5,6])
    prefixes.clear()
    refused = False
    try:
        prefixes.restore_to_cache(current,destination)
    except:
        refused = True
    assert_true(refused)


def main() raises:
    test_prefix_matrix()
    print("R2 independent multi-layer prefix stride/capacity/staleness oracles PASS")
