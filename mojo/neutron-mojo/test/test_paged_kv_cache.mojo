# ===----------------------------------------------------------------------=== #
# Neutron Mojo -- Sprint 30: Paged KV Cache Tests
# ===----------------------------------------------------------------------=== #

"""Tests for paged KV cache and paged attention.

Tests:
1. PageAllocator allocates and deallocates pages
2. PageAllocator exhaustion raises error
3. PageAllocator reuse after deallocation
4. PageTable resolves positions to pages correctly
5. PagedKVCache append allocates pages on demand
6. PagedKVCache read matches written data
7. PagedKVCache multi-page sequence
8. PagedKVCache multi-layer independence
9. Paged attention matches direct attention (correctness)
10. PagedKVCache free reclaims pages
11. PagedKVCache can_fit capacity check
12. Memory savings vs contiguous cache
13. Benchmark: paged vs contiguous decode
"""

from math import abs, sqrt
from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.paged_kv_cache import PageAllocator, PageTable, PagedKVCache
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.attention import (
    gqa_attention_direct,
    paged_gqa_attention,
    softmax_inplace,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


fn assert_close(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error("FAIL: " + msg + " a=" + String(a) + " b=" + String(b))


# ===----------------------------------------------------------------------=== #
# PageAllocator Tests
# ===----------------------------------------------------------------------=== #

fn test_page_allocator_basic() raises:
    """PageAllocator allocates and tracks pages."""
    var alloc = PageAllocator(max_pages=8, page_size=4, kv_dim=6)
    assert_true(alloc.num_free() == 8, "8 free pages initially")
    assert_true(alloc.num_allocated == 0, "0 allocated initially")

    var p0 = alloc.allocate()
    assert_true(alloc.num_free() == 7, "7 free after 1 alloc")
    assert_true(alloc.num_allocated == 1, "1 allocated")

    var p1 = alloc.allocate()
    assert_true(alloc.num_free() == 6, "6 free after 2 allocs")
    assert_true(p0 != p1, "Different page IDs")

    alloc.deallocate(p0)
    assert_true(alloc.num_free() == 7, "7 free after dealloc")
    assert_true(alloc.num_allocated == 1, "1 allocated after dealloc")

    print("  page_allocator_basic: PASS")


fn test_page_allocator_exhaustion() raises:
    """PageAllocator raises on exhaustion."""
    var alloc = PageAllocator(max_pages=2, page_size=4, kv_dim=4)
    _ = alloc.allocate()
    _ = alloc.allocate()
    assert_true(alloc.num_free() == 0, "0 free after exhaustion")

    var caught = False
    try:
        _ = alloc.allocate()
    except:
        caught = True
    assert_true(caught, "Should raise on exhaustion")
    print("  page_allocator_exhaustion: PASS")


fn test_page_allocator_reuse() raises:
    """Deallocated pages can be reused."""
    var alloc = PageAllocator(max_pages=2, page_size=4, kv_dim=4)
    var p0 = alloc.allocate()
    var p1 = alloc.allocate()
    assert_true(alloc.num_free() == 0, "exhausted")

    alloc.deallocate(p0)
    assert_true(alloc.num_free() == 1, "1 free after dealloc")

    var p2 = alloc.allocate()
    assert_true(p2 == p0, "Reused deallocated page")
    assert_true(alloc.num_free() == 0, "exhausted again")
    print("  page_allocator_reuse: PASS")


# ===----------------------------------------------------------------------=== #
# PageTable Tests
# ===----------------------------------------------------------------------=== #

fn test_page_table_resolution() raises:
    """PageTable maps logical positions to page indices and slots."""
    var pt = PageTable(page_size=4)

    # Page 0 holds positions 0-3, page 1 holds 4-7, etc.
    assert_true(pt.resolve(0) == 0, "pos 0 -> page 0")
    assert_true(pt.resolve(3) == 0, "pos 3 -> page 0")
    assert_true(pt.resolve(4) == 1, "pos 4 -> page 1")
    assert_true(pt.resolve(7) == 1, "pos 7 -> page 1")
    assert_true(pt.resolve(8) == 2, "pos 8 -> page 2")

    assert_true(pt.slot_in_page(0) == 0, "pos 0 -> slot 0")
    assert_true(pt.slot_in_page(3) == 3, "pos 3 -> slot 3")
    assert_true(pt.slot_in_page(4) == 0, "pos 4 -> slot 0")
    assert_true(pt.slot_in_page(5) == 1, "pos 5 -> slot 1")

    print("  page_table_resolution: PASS")


# ===----------------------------------------------------------------------=== #
# PagedKVCache Tests
# ===----------------------------------------------------------------------=== #

fn test_paged_cache_append() raises:
    """PagedKVCache allocates pages on demand during append."""
    var num_kv = 1
    var hd = 2
    var page_size = 4

    var cache = PagedKVCache(
        max_pages=16, page_size=page_size,
        num_layers=2, num_kv_heads=num_kv, head_dim=hd,
    )

    # Append 1 token to layer 0 -> should allocate 1 page
    var k = Tensor[DType.float32](Shape(num_kv * hd))
    var v = Tensor[DType.float32](Shape(num_kv * hd))
    k.set(0, 1.0)
    k.set(1, 2.0)
    v.set(0, 3.0)
    v.set(1, 4.0)

    cache.append_kv(0, k, v, num_new_tokens=1)
    assert_true(cache.seq_len(0) == 1, "layer 0 has 1 token")
    assert_true(cache.seq_len(1) == 0, "layer 1 still empty")
    assert_true(cache.total_pages_used() == 1, "1 page allocated")

    # Append 3 more tokens (fills page 0)
    for _ in range(3):
        cache.append_kv(0, k, v, num_new_tokens=1)
    assert_true(cache.seq_len(0) == 4, "layer 0 has 4 tokens")
    assert_true(cache.total_pages_used() == 1, "still 1 page (full)")

    # Append 1 more -> new page
    cache.append_kv(0, k, v, num_new_tokens=1)
    assert_true(cache.seq_len(0) == 5, "layer 0 has 5 tokens")
    assert_true(cache.total_pages_used() == 2, "2 pages now")

    print("  paged_cache_append: PASS")


fn test_paged_cache_read_write() raises:
    """PagedKVCache reads match written data."""
    var num_kv = 2
    var hd = 3
    var page_size = 4

    var cache = PagedKVCache(
        max_pages=16, page_size=page_size,
        num_layers=1, num_kv_heads=num_kv, head_dim=hd,
    )

    # Append 3 tokens with known values
    for tok in range(3):
        var k = Tensor[DType.float32](Shape(num_kv * hd))
        var v = Tensor[DType.float32](Shape(num_kv * hd))
        for i in range(num_kv * hd):
            k.set(i, Float32(tok * 100 + i))
            v.set(i, Float32(tok * 100 + i + 50))
        cache.append_kv(0, k, v, num_new_tokens=1)

    # Verify reads
    for tok in range(3):
        for h in range(num_kv):
            for d in range(hd):
                var expected_k = Float32(tok * 100 + h * hd + d)
                var expected_v = Float32(tok * 100 + h * hd + d + 50)
                assert_close(cache.get_key_at(0, tok, h, d), expected_k, 1e-6,
                    "key mismatch at tok=" + String(tok))
                assert_close(cache.get_value_at(0, tok, h, d), expected_v, 1e-6,
                    "value mismatch at tok=" + String(tok))

    print("  paged_cache_read_write: PASS")


fn test_paged_cache_multi_page() raises:
    """PagedKVCache handles sequences spanning multiple pages."""
    var num_kv = 1
    var hd = 2
    var page_size = 4

    var cache = PagedKVCache(
        max_pages=16, page_size=page_size,
        num_layers=1, num_kv_heads=num_kv, head_dim=hd,
    )

    # Append 10 tokens -> 3 pages (4+4+2)
    for tok in range(10):
        var k = Tensor[DType.float32](Shape(num_kv * hd))
        var v = Tensor[DType.float32](Shape(num_kv * hd))
        k.set(0, Float32(tok))
        k.set(1, Float32(tok + 100))
        v.set(0, Float32(tok + 200))
        v.set(1, Float32(tok + 300))
        cache.append_kv(0, k, v, num_new_tokens=1)

    assert_true(cache.seq_len(0) == 10, "10 tokens stored")
    assert_true(cache.total_pages_used() == 3, "3 pages (4+4+2)")

    # Verify reads across page boundaries
    for tok in range(10):
        assert_close(cache.get_key_at(0, tok, 0, 0), Float32(tok), 1e-6,
            "key head0 dim0 at pos " + String(tok))
        assert_close(cache.get_value_at(0, tok, 0, 1), Float32(tok + 300), 1e-6,
            "value head0 dim1 at pos " + String(tok))

    print("  paged_cache_multi_page: PASS")


fn test_paged_cache_multi_layer() raises:
    """Each layer allocates pages independently."""
    var num_kv = 1
    var hd = 2
    var page_size = 4

    var cache = PagedKVCache(
        max_pages=16, page_size=page_size,
        num_layers=2, num_kv_heads=num_kv, head_dim=hd,
    )

    var k = Tensor[DType.float32](Shape(num_kv * hd))
    var v = Tensor[DType.float32](Shape(num_kv * hd))

    # Layer 0: 3 tokens (1 page)
    for tok in range(3):
        k.set(0, Float32(tok))
        k.set(1, Float32(tok + 10))
        v.set(0, Float32(tok + 20))
        v.set(1, Float32(tok + 30))
        cache.append_kv(0, k, v, num_new_tokens=1)

    # Layer 1: 7 tokens (2 pages)
    for tok in range(7):
        k.set(0, Float32(tok + 100))
        k.set(1, Float32(tok + 110))
        v.set(0, Float32(tok + 120))
        v.set(1, Float32(tok + 130))
        cache.append_kv(1, k, v, num_new_tokens=1)

    assert_true(cache.seq_len(0) == 3, "layer 0 has 3")
    assert_true(cache.seq_len(1) == 7, "layer 1 has 7")
    assert_true(cache.total_pages_used() == 3, "3 pages total (1+2)")

    # Verify layer isolation
    assert_close(cache.get_key_at(0, 0, 0, 0), 0.0, 1e-6, "layer 0 tok 0")
    assert_close(cache.get_key_at(1, 0, 0, 0), 100.0, 1e-6, "layer 1 tok 0")

    print("  paged_cache_multi_layer: PASS")


# ===----------------------------------------------------------------------=== #
# Paged Attention Tests
# ===----------------------------------------------------------------------=== #

fn test_paged_attention_matches_direct() raises:
    """Paged attention produces same results as direct contiguous attention."""
    var num_q = 2
    var num_kv = 1
    var hd = 4
    var page_size = 4
    var num_tokens = 6

    # Create query
    var q = Tensor[DType.float32](Shape(num_q * hd))
    for i in range(num_q * hd):
        q.set(i, Float32(i) * 0.1 - 0.5)

    # Fill both caches with identical data
    var paged_cache = PagedKVCache(
        max_pages=16, page_size=page_size,
        num_layers=1, num_kv_heads=num_kv, head_dim=hd,
    )
    var contiguous_cache = MultiLayerKVCache(
        num_layers=1, max_seq_len=32,
        num_kv_heads=num_kv, head_dim=hd,
    )

    for tok in range(num_tokens):
        var k = Tensor[DType.float32](Shape(num_kv * hd))
        var v = Tensor[DType.float32](Shape(num_kv * hd))
        for i in range(num_kv * hd):
            k.set(i, Float32(tok * 10 + i) * 0.05)
            v.set(i, Float32(tok * 10 + i + 5) * 0.03)
        paged_cache.append_kv(0, k, v, num_new_tokens=1)

        # Same data for contiguous
        var k2 = Tensor[DType.float32](Shape(num_kv * hd))
        var v2 = Tensor[DType.float32](Shape(num_kv * hd))
        for i in range(num_kv * hd):
            k2.set(i, Float32(tok * 10 + i) * 0.05)
            v2.set(i, Float32(tok * 10 + i + 5) * 0.03)
        contiguous_cache.append_kv(0, k2, v2, num_new_tokens=1)

    # Compare outputs
    var out_paged = paged_gqa_attention(q, paged_cache, 0, num_q, num_kv, hd)
    var out_direct = gqa_attention_direct(q, contiguous_cache, 0, num_q, num_kv, hd)

    for i in range(num_q * hd):
        assert_close(out_paged.get(i), out_direct.get(i), 1e-5,
            "Paged vs direct mismatch at " + String(i))

    print("  paged_attention_matches_direct: PASS")


fn test_paged_cache_free() raises:
    """Freeing pages reclaims memory."""
    var cache = PagedKVCache(
        max_pages=8, page_size=4,
        num_layers=2, num_kv_heads=1, head_dim=2,
    )

    var k = Tensor[DType.float32](Shape(2))
    var v = Tensor[DType.float32](Shape(2))

    # Fill both layers
    for tok in range(5):
        cache.append_kv(0, k, v, num_new_tokens=1)
        cache.append_kv(1, k, v, num_new_tokens=1)
    # 2 pages per layer = 4 total
    assert_true(cache.total_pages_used() == 4, "4 pages used")

    # Free layer 0
    cache.free_layer(0)
    assert_true(cache.seq_len(0) == 0, "layer 0 freed")
    assert_true(cache.total_pages_used() == 2, "2 pages after free")
    assert_true(cache.allocator.num_free() == 6, "6 free pages")

    # Free all
    cache.free_all()
    assert_true(cache.total_pages_used() == 0, "0 pages after free_all")
    assert_true(cache.allocator.num_free() == 8, "all pages free")

    print("  paged_cache_free: PASS")


fn test_paged_cache_can_fit() raises:
    """Can_fit checks available capacity."""
    var cache = PagedKVCache(
        max_pages=4, page_size=4,
        num_layers=2, num_kv_heads=1, head_dim=2,
    )

    # Need 1 page/layer for 4 tokens -> 2 pages total
    assert_true(cache.can_fit(4), "can fit 4 tokens")
    # Need 2 pages/layer for 5 tokens -> 4 pages total
    assert_true(cache.can_fit(5), "can fit 5 tokens (exactly 4 pages)")
    # Need 3 pages/layer for 9 tokens -> 6 pages total > 4 available
    assert_true(not cache.can_fit(9), "cannot fit 9 tokens")

    print("  paged_cache_can_fit: PASS")


fn test_memory_savings() raises:
    """Paged cache uses less memory than contiguous for short sequences."""
    var num_layers = 2
    var num_kv = 4
    var hd = 8
    var max_seq = 1024
    var page_size = 16
    var actual_seq = 50  # Short sequence

    # Contiguous: allocates for max_seq
    var contiguous_bytes = num_layers * max_seq * num_kv * hd * 4 * 2

    # Paged: allocates only needed pages
    var pages_per_layer = (actual_seq + page_size - 1) // page_size  # 4 pages
    var paged_bytes = pages_per_layer * num_layers * page_size * num_kv * hd * 4 * 2

    var savings_pct = Float64(contiguous_bytes - paged_bytes) * 100.0 / Float64(contiguous_bytes)

    assert_true(paged_bytes < contiguous_bytes, "paged uses less memory")
    assert_true(savings_pct > 80.0, "should save >80% for short seq")

    print("  memory_savings: " + String(Int(savings_pct)) + "% savings (" +
          String(contiguous_bytes // 1024) + " KB contiguous vs " +
          String(paged_bytes // 1024) + " KB paged): PASS")


fn test_benchmark_paged_decode() raises:
    """Benchmark paged vs contiguous attention performance."""
    var num_q = 4
    var num_kv = 2
    var hd = 8
    var page_size = 16
    var seq_len = 64
    var iters = 50

    var q = Tensor[DType.float32](Shape(num_q * hd))
    for i in range(num_q * hd):
        q.set(i, Float32(i) * 0.1)

    # Fill paged cache
    var paged = PagedKVCache(
        max_pages=64, page_size=page_size,
        num_layers=1, num_kv_heads=num_kv, head_dim=hd,
    )
    var contig = MultiLayerKVCache(
        num_layers=1, max_seq_len=128,
        num_kv_heads=num_kv, head_dim=hd,
    )

    for tok in range(seq_len):
        var k = Tensor[DType.float32](Shape(num_kv * hd))
        var v = Tensor[DType.float32](Shape(num_kv * hd))
        for i in range(num_kv * hd):
            k.set(i, Float32(tok + i) * 0.02)
            v.set(i, Float32(tok + i) * 0.03)
        paged.append_kv(0, k, v, num_new_tokens=1)

        var k2 = Tensor[DType.float32](Shape(num_kv * hd))
        var v2 = Tensor[DType.float32](Shape(num_kv * hd))
        for i in range(num_kv * hd):
            k2.set(i, Float32(tok + i) * 0.02)
            v2.set(i, Float32(tok + i) * 0.03)
        contig.append_kv(0, k2, v2, num_new_tokens=1)

    # Benchmark contiguous
    var t0 = Int(perf_counter_ns())
    for _ in range(iters):
        _ = gqa_attention_direct(q, contig, 0, num_q, num_kv, hd)
    var contig_ns = Int(perf_counter_ns()) - t0

    # Benchmark paged
    var t1 = Int(perf_counter_ns())
    for _ in range(iters):
        _ = paged_gqa_attention(q, paged, 0, num_q, num_kv, hd)
    var paged_ns = Int(perf_counter_ns()) - t1

    var contig_us = Int(Float64(contig_ns) / 1000.0)
    var paged_us = Int(Float64(paged_ns) / 1000.0)
    var overhead_pct: Int = 0
    if contig_ns > 0:
        overhead_pct = Int(Float64(paged_ns - contig_ns) * 100.0 / Float64(contig_ns))

    print("  benchmark_paged_decode: contiguous=" + String(contig_us) +
          "us paged=" + String(paged_us) +
          "us overhead=" + String(overhead_pct) + "%: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_paged_kv_cache:")

    # PageAllocator
    test_page_allocator_basic()
    test_page_allocator_exhaustion()
    test_page_allocator_reuse()

    # PageTable
    test_page_table_resolution()

    # PagedKVCache
    test_paged_cache_append()
    test_paged_cache_read_write()
    test_paged_cache_multi_page()
    test_paged_cache_multi_layer()

    # Paged Attention
    test_paged_attention_matches_direct()

    # Memory management
    test_paged_cache_free()
    test_paged_cache_can_fit()
    test_memory_savings()

    # Benchmark
    test_benchmark_paged_decode()

    print("ALL PASSED (13 tests)")
