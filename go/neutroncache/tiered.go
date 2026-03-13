package neutroncache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/neutron-dev/neutron-go/nucleus"
)

// TieredCache provides L1 (in-memory LRU) + L2 (Nucleus KV) caching.
type TieredCache struct {
	l1 *LRUCache
	l2 *nucleus.KVModel // nil if no L2
}

// NewTiered creates a tiered cache. If kv is nil, only L1 is used.
func NewTiered(l1Size int, kv *nucleus.KVModel) *TieredCache {
	return &TieredCache{
		l1: NewLRU(l1Size),
		l2: kv,
	}
}

// Get retrieves a typed value, checking L1 first, then L2.
func Get[T any](ctx context.Context, c *TieredCache, key string) (T, error) {
	var zero T

	// L1
	if data, ok := c.l1.Get(key); ok {
		var result T
		if err := json.Unmarshal(data, &result); err != nil {
			return zero, fmt.Errorf("neutroncache: unmarshal L1: %w", err)
		}
		return result, nil
	}

	// L2
	if c.l2 != nil {
		data, err := c.l2.Get(ctx, key)
		if err != nil {
			return zero, fmt.Errorf("neutroncache: L2 get: %w", err)
		}
		if data != nil {
			// Promote to L1
			c.l1.Set(key, data, 0)
			var result T
			if err := json.Unmarshal(data, &result); err != nil {
				return zero, fmt.Errorf("neutroncache: unmarshal L2: %w", err)
			}
			return result, nil
		}
	}

	return zero, fmt.Errorf("neutroncache: key %q not found", key)
}

// Set stores a typed value in both L1 and L2.
func Set[T any](ctx context.Context, c *TieredCache, key string, value T, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("neutroncache: marshal: %w", err)
	}

	c.l1.Set(key, data, ttl)

	if c.l2 != nil {
		if err := c.l2.Set(ctx, key, data, nucleus.WithTTL(ttl)); err != nil {
			return fmt.Errorf("neutroncache: L2 set: %w", err)
		}
	}

	return nil
}

// Invalidate removes a key from both tiers.
func Invalidate(ctx context.Context, c *TieredCache, key string) error {
	c.l1.Delete(key)
	if c.l2 != nil {
		if _, err := c.l2.Delete(ctx, key); err != nil {
			return fmt.Errorf("neutroncache: L2 delete: %w", err)
		}
	}
	return nil
}
