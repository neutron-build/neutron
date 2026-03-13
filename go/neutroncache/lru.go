package neutroncache

import (
	"container/list"
	"sync"
	"time"
)

// lruEntry stores a cache item with optional expiration.
type lruEntry struct {
	key       string
	value     []byte
	expiresAt time.Time
}

// LRUCache is a thread-safe in-memory LRU cache.
type LRUCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List
}

// NewLRU creates a new LRU cache with the given capacity.
func NewLRU(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

// Get retrieves a value from the cache. Returns nil if not found or expired.
func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}

	entry := elem.Value.(*lruEntry)
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		c.removeElement(elem)
		return nil, false
	}

	c.order.MoveToFront(elem)
	return entry.value, true
}

// Set stores a value in the cache with an optional TTL (0 = no expiry).
func (c *LRUCache) Set(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		entry := elem.Value.(*lruEntry)
		entry.value = value
		entry.expiresAt = expiresAt
		return
	}

	entry := &lruEntry{key: key, value: value, expiresAt: expiresAt}
	elem := c.order.PushFront(entry)
	c.items[key] = elem

	if c.order.Len() > c.capacity {
		c.removeOldest()
	}
}

// Delete removes a key from the cache.
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *LRUCache) removeOldest() {
	elem := c.order.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

func (c *LRUCache) removeElement(elem *list.Element) {
	c.order.Remove(elem)
	entry := elem.Value.(*lruEntry)
	delete(c.items, entry.key)
}
