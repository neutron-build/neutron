package nucleus

import (
	"testing"
	"time"
)

func TestKVOptionWithTTL(t *testing.T) {
	o := applyKVOpts([]KVOption{WithTTL(5 * time.Minute)})
	if o.ttl == nil {
		t.Fatal("ttl should be set")
	}
	if *o.ttl != 5*time.Minute {
		t.Errorf("ttl = %v, want 5m", *o.ttl)
	}
}

func TestKVOptionWithNamespace(t *testing.T) {
	o := applyKVOpts([]KVOption{WithNamespace("session")})
	key := o.resolveKey("abc123")
	if key != "session:abc123" {
		t.Errorf("key = %q, want session:abc123", key)
	}
}

func TestKVOptionNoNamespace(t *testing.T) {
	o := applyKVOpts(nil)
	key := o.resolveKey("abc123")
	if key != "abc123" {
		t.Errorf("key = %q, want abc123", key)
	}
}

func TestKVSetTypedMarshal(t *testing.T) {
	// Verify KVSetTyped/KVGetTyped generic functions exist and compile
	// Actual database calls require integration tests
	_ = KVSetTyped[map[string]string]
	_ = KVGetTyped[map[string]string]
}
