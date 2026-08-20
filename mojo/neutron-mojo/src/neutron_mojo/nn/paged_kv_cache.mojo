# ===----------------------------------------------------------------------=== #
# Neutron Mojo -- Paged KV Cache
# ===----------------------------------------------------------------------=== #

"""Paged KV cache for memory-efficient inference serving.

Instead of pre-allocating max_seq_len contiguous memory per request,
this cache allocates fixed-size pages (e.g., 16 tokens) on demand.
Short sequences use fewer pages, enabling 2-4x more concurrent
requests at the same peak memory.

Architecture:
- PageAllocator: Manages a pool of fixed-size pages
- PageTable: Maps logical positions to physical page indices
- PagedKVCache: Multi-layer paged cache with append/read via indirection

Usage:
    var allocator = PageAllocator(max_pages=256, page_size=16,
                                   kv_dim=num_kv_heads * head_dim)
    var cache = PagedKVCache(allocator, num_layers, num_kv_heads, head_dim)
    cache.append_kv(layer, k, v, num_new_tokens=1)
    var val = cache.get_key_at(layer, pos, head, dim)
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Page Allocator
# ===----------------------------------------------------------------------=== #

struct PageAllocator(Copyable, Movable, ImplicitlyCopyable):
    """Pool allocator for KV cache pages.

    Manages a flat buffer of pages. Each page holds `page_size` token
    positions worth of K and V data. Free pages are tracked via a
    free list for O(1) allocation/deallocation.
    """
    var key_pool: Tensor[DType.float32]    # [max_pages * page_size * kv_dim]
    var value_pool: Tensor[DType.float32]  # same shape
    var free_list: List[Int]               # Stack of free page indices
    var max_pages: Int
    var page_size: Int    # Tokens per page
    var kv_dim: Int       # num_kv_heads * head_dim
    var num_allocated: Int

    def __init__(
        out self,
        max_pages: Int,
        page_size: Int,
        kv_dim: Int,
    ):
        """Create a page allocator.

        Args:
            max_pages: Total number of pages in the pool.
            page_size: Number of token positions per page.
            kv_dim: Elements per position (num_kv_heads * head_dim).
        """
        self.max_pages = max_pages
        self.page_size = page_size
        self.kv_dim = kv_dim
        self.num_allocated = 0

        var page_elements = page_size * kv_dim
        var total = max_pages * page_elements
        self.key_pool = Tensor[DType.float32](Shape(total))
        self.value_pool = Tensor[DType.float32](Shape(total))

        # Initialize free list (all pages free, LIFO order)
        self.free_list = List[Int]()
        for i in range(max_pages):
            self.free_list.append(max_pages - 1 - i)

    def __init__(out self, *, copy: Self):
        self.max_pages = copy.max_pages
        self.page_size = copy.page_size
        self.kv_dim = copy.kv_dim
        self.num_allocated = copy.num_allocated

        var total = copy.key_pool.numel()
        self.key_pool = Tensor[DType.float32](Shape(total))
        self.value_pool = Tensor[DType.float32](Shape(total))
        for i in range(total):
            self.key_pool.set(i, copy.key_pool.get(i))
            self.value_pool.set(i, copy.value_pool.get(i))

        self.free_list = List[Int]()
        for i in range(len(copy.free_list)):
            self.free_list.append(copy.free_list[i])

    def __init__(out self, *, deinit move: Self):
        self.key_pool = move.key_pool^
        self.value_pool = move.value_pool^
        self.free_list = move.free_list^
        self.max_pages = move.max_pages^
        self.page_size = move.page_size^
        self.kv_dim = move.kv_dim^
        self.num_allocated = move.num_allocated^

    def allocate(mut self) raises -> Int:
        """Allocate one page from the pool.

        Returns:
            Physical page index.

        Raises:
            Error if no free pages remain.
        """
        if len(self.free_list) == 0:
            raise Error("PageAllocator: out of pages (" +
                        String(self.max_pages) + " pages exhausted)")
        var page_id = self.free_list.pop()
        self.num_allocated += 1
        return page_id

    def deallocate(mut self, page_id: Int):
        """Return a page to the pool.

        Args:
            page_id: Physical page index to free.
        """
        self.free_list.append(page_id)
        self.num_allocated -= 1

    def num_free(self) -> Int:
        """Number of free pages remaining."""
        return len(self.free_list)

    def page_offset(self, page_id: Int) -> Int:
        """Compute flat offset for a page's data in the pool.

        Args:
            page_id: Physical page index.

        Returns:
            Starting element offset in key_pool/value_pool.
        """
        return page_id * self.page_size * self.kv_dim

    def get_key(self, page_id: Int, slot: Int, head: Int, head_dim: Int, dim: Int) -> Float32:
        """Read a key value from a page.

        Args:
            page_id: Physical page index.
            slot: Position within the page (0 to page_size-1).
            head: KV head index.
            head_dim: Per-head dimension.
            dim: Dimension index within the head.

        Returns:
            Key value.
        """
        var offset = self.page_offset(page_id) + slot * self.kv_dim + head * head_dim + dim
        return self.key_pool.get(offset)

    def get_value(self, page_id: Int, slot: Int, head: Int, head_dim: Int, dim: Int) -> Float32:
        """Read a value from a page."""
        var offset = self.page_offset(page_id) + slot * self.kv_dim + head * head_dim + dim
        return self.value_pool.get(offset)

    def write_key(mut self, page_id: Int, slot: Int, flat_offset: Int, value: Float32):
        """Write a key value to a page at a flat offset within the slot."""
        var offset = self.page_offset(page_id) + slot * self.kv_dim + flat_offset
        self.key_pool.set(offset, value)

    def write_value(mut self, page_id: Int, slot: Int, flat_offset: Int, value: Float32):
        """Write a value to a page at a flat offset within the slot."""
        var offset = self.page_offset(page_id) + slot * self.kv_dim + flat_offset
        self.value_pool.set(offset, value)

    def total_memory_bytes(self) -> Int:
        """Total pool memory in bytes (key + value)."""
        return self.max_pages * self.page_size * self.kv_dim * 4 * 2

    def used_memory_bytes(self) -> Int:
        """Memory currently allocated in bytes."""
        return self.num_allocated * self.page_size * self.kv_dim * 4 * 2


# ===----------------------------------------------------------------------=== #
# Page Table (per-layer, per-sequence)
# ===----------------------------------------------------------------------=== #

struct PageTable(Copyable, Movable, ImplicitlyCopyable):
    """Maps logical token positions to physical pages.

    Each layer of a sequence has its own PageTable. As tokens are appended,
    new pages are allocated on demand when the current page fills up.
    """
    var pages: List[Int]     # List of physical page IDs (in order)
    var num_tokens: Int      # Total tokens stored
    var page_size: Int       # Tokens per page

    def __init__(out self, page_size: Int):
        self.pages = List[Int]()
        self.num_tokens = 0
        self.page_size = page_size

    def __init__(out self, *, copy: Self):
        self.pages = copy.pages.copy()
        self.num_tokens = copy.num_tokens
        self.page_size = copy.page_size

    def __init__(out self, *, deinit move: Self):
        self.pages = move.pages^
        self.num_tokens = move.num_tokens^
        self.page_size = move.page_size^

    def num_pages(self) -> Int:
        """Number of pages currently allocated."""
        return len(self.pages)

    def current_page_slots_used(self) -> Int:
        """Slots used in the last page."""
        if self.num_tokens == 0:
            return 0
        return ((self.num_tokens - 1) % self.page_size) + 1

    def current_page_has_space(self) -> Bool:
        """Whether the current (last) page has room for more tokens."""
        if len(self.pages) == 0:
            return False
        return self.current_page_slots_used() < self.page_size

    def resolve(self, logical_pos: Int) -> Int:
        """Map a logical position to (page_index, slot_within_page).

        Returns the page index in the pages list. Use pages[page_index]
        to get the physical page ID.

        Args:
            logical_pos: Logical token position (0-based).

        Returns:
            Index into self.pages list.
        """
        return logical_pos // self.page_size

    def slot_in_page(self, logical_pos: Int) -> Int:
        """Get the slot index within a page for a logical position.

        Args:
            logical_pos: Logical token position.

        Returns:
            Slot index within the page (0 to page_size-1).
        """
        return logical_pos % self.page_size


# ===----------------------------------------------------------------------=== #
# Paged KV Cache
# ===----------------------------------------------------------------------=== #

struct PagedKVCache(Copyable, Movable, ImplicitlyCopyable):
    """Multi-layer paged KV cache.

    Each layer has its own PageTable mapping logical positions to
    physical pages in the shared PageAllocator. Pages are allocated
    on demand as tokens are appended.

    Provides the same get_key_at/get_value_at interface as
    MultiLayerKVCache for compatibility with attention functions.
    """
    var allocator: PageAllocator
    var page_tables: List[PageTable]  # One per layer
    var num_layers: Int
    var num_kv_heads: Int
    var head_dim: Int
    var page_size: Int

    def __init__(
        out self,
        max_pages: Int,
        page_size: Int,
        num_layers: Int,
        num_kv_heads: Int,
        head_dim: Int,
    ):
        """Create a paged KV cache.

        Args:
            max_pages: Total pages in the shared pool.
            page_size: Tokens per page (e.g., 16).
            num_layers: Number of transformer layers.
            num_kv_heads: Number of KV heads.
            head_dim: Per-head dimension.
        """
        var kv_dim = num_kv_heads * head_dim
        self.allocator = PageAllocator(max_pages, page_size, kv_dim)
        self.num_layers = num_layers
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim
        self.page_size = page_size

        self.page_tables = List[PageTable]()
        for _ in range(num_layers):
            self.page_tables.append(PageTable(page_size))

    def __init__(out self, *, copy: Self):
        self.allocator = copy.allocator.copy()
        self.page_tables = copy.page_tables.copy()
        self.num_layers = copy.num_layers
        self.num_kv_heads = copy.num_kv_heads
        self.head_dim = copy.head_dim
        self.page_size = copy.page_size

    def __init__(out self, *, deinit move: Self):
        self.allocator = move.allocator^
        self.page_tables = move.page_tables^
        self.num_layers = move.num_layers^
        self.num_kv_heads = move.num_kv_heads^
        self.head_dim = move.head_dim^
        self.page_size = move.page_size^

    def seq_len(self, layer: Int) -> Int:
        """Current sequence length for a layer."""
        return self.page_tables[layer].num_tokens

    def append_kv(
        mut self,
        layer: Int,
        key: Tensor[DType.float32],
        value: Tensor[DType.float32],
        num_new_tokens: Int,
    ) raises:
        """Append K/V data for a layer, allocating pages as needed.

        Args:
            layer: Layer index.
            key: New keys [num_new_tokens * num_kv_heads * head_dim].
            value: New values, same shape.
            num_new_tokens: Number of new positions.
        """
        var kv_dim = self.num_kv_heads * self.head_dim

        for t in range(num_new_tokens):
            # Check if we need a new page
            if not self.page_tables[layer].current_page_has_space():
                var new_page = self.allocator.allocate()
                self.page_tables[layer].pages.append(new_page)

            # Find where to write
            var logical_pos = self.page_tables[layer].num_tokens
            var page_idx = self.page_tables[layer].resolve(logical_pos)
            var slot = self.page_tables[layer].slot_in_page(logical_pos)
            var phys_page = self.page_tables[layer].pages[page_idx]

            # Write K/V data for this token
            var src_offset = t * kv_dim
            for i in range(kv_dim):
                self.allocator.write_key(phys_page, slot, i, key.get(src_offset + i))
                self.allocator.write_value(phys_page, slot, i, value.get(src_offset + i))

            self.page_tables[layer].num_tokens += 1

    def get_key_at(self, layer: Int, pos: Int, head: Int, dim: Int) -> Float32:
        """Get a key value from the paged cache.

        Same interface as MultiLayerKVCache.get_key_at.

        Args:
            layer: Layer index.
            pos: Logical sequence position.
            head: KV head index.
            dim: Dimension within the head.

        Returns:
            Key value.
        """
        var page_idx = self.page_tables[layer].resolve(pos)
        var slot = self.page_tables[layer].slot_in_page(pos)
        var phys_page = self.page_tables[layer].pages[page_idx]
        return self.allocator.get_key(phys_page, slot, head, self.head_dim, dim)

    def get_value_at(self, layer: Int, pos: Int, head: Int, dim: Int) -> Float32:
        """Get a value from the paged cache.

        Same interface as MultiLayerKVCache.get_value_at.
        """
        var page_idx = self.page_tables[layer].resolve(pos)
        var slot = self.page_tables[layer].slot_in_page(pos)
        var phys_page = self.page_tables[layer].pages[page_idx]
        return self.allocator.get_value(phys_page, slot, head, self.head_dim, dim)

    def free_layer(mut self, layer: Int):
        """Free all pages for a specific layer.

        Args:
            layer: Layer index to free.
        """
        for i in range(len(self.page_tables[layer].pages)):
            self.allocator.deallocate(self.page_tables[layer].pages[i])
        self.page_tables[layer].pages = List[Int]()
        self.page_tables[layer].num_tokens = 0

    def free_all(mut self):
        """Free all pages across all layers."""
        for layer in range(self.num_layers):
            self.free_layer(layer)

    def total_pages_used(self) -> Int:
        """Total pages currently allocated across all layers."""
        return self.allocator.num_allocated

    def total_memory_bytes(self) -> Int:
        """Total pool memory in bytes."""
        return self.allocator.total_memory_bytes()

    def used_memory_bytes(self) -> Int:
        """Memory currently allocated in bytes."""
        return self.allocator.used_memory_bytes()

    def pages_needed(self, seq_len: Int) -> Int:
        """Pages needed for a given sequence length (per layer).

        Args:
            seq_len: Number of tokens.

        Returns:
            Number of pages required.
        """
        return (seq_len + self.page_size - 1) // self.page_size

    def can_fit(self, seq_len: Int) -> Bool:
        """Check if enough free pages exist for a new sequence.

        Args:
            seq_len: Expected sequence length.

        Returns:
            True if pool has enough pages for all layers.
        """
        var needed = self.pages_needed(seq_len) * self.num_layers
        return self.allocator.num_free() >= needed
