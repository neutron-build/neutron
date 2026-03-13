package neutroncache

import (
	"testing"
	"time"
)

func TestLRUSetGet(t *testing.T) {
	c := NewLRU(10)
	c.Set("key1", []byte("value1"), 0)

	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(val) != "value1" {
		t.Errorf("value = %q", string(val))
	}
}

func TestLRUMiss(t *testing.T) {
	c := NewLRU(10)
	_, ok := c.Get("missing")
	if ok {
		t.Fatal("expected miss")
	}
}

func TestLRUEviction(t *testing.T) {
	c := NewLRU(2)
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Set("c", []byte("3"), 0) // should evict "a"

	if _, ok := c.Get("a"); ok {
		t.Fatal("'a' should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("'b' should still exist")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("'c' should exist")
	}
}

func TestLRUAccessOrder(t *testing.T) {
	c := NewLRU(2)
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Get("a") // access "a" so it's recently used
	c.Set("c", []byte("3"), 0) // should evict "b", not "a"

	if _, ok := c.Get("a"); !ok {
		t.Fatal("'a' should still exist after access")
	}
	if _, ok := c.Get("b"); ok {
		t.Fatal("'b' should have been evicted")
	}
}

func TestLRUUpdate(t *testing.T) {
	c := NewLRU(10)
	c.Set("key", []byte("old"), 0)
	c.Set("key", []byte("new"), 0)

	val, ok := c.Get("key")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(val) != "new" {
		t.Errorf("value = %q, want 'new'", string(val))
	}
	if c.Len() != 1 {
		t.Errorf("len = %d, want 1", c.Len())
	}
}

func TestLRUDelete(t *testing.T) {
	c := NewLRU(10)
	c.Set("key", []byte("val"), 0)
	c.Delete("key")

	if _, ok := c.Get("key"); ok {
		t.Fatal("expected miss after delete")
	}
	if c.Len() != 0 {
		t.Errorf("len = %d, want 0", c.Len())
	}
}

func TestLRUTTL(t *testing.T) {
	c := NewLRU(10)
	c.Set("key", []byte("val"), 50*time.Millisecond)

	if _, ok := c.Get("key"); !ok {
		t.Fatal("should be available immediately")
	}

	time.Sleep(60 * time.Millisecond)

	if _, ok := c.Get("key"); ok {
		t.Fatal("should have expired")
	}
}

func TestLRULen(t *testing.T) {
	c := NewLRU(10)
	if c.Len() != 0 {
		t.Errorf("len = %d, want 0", c.Len())
	}
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	if c.Len() != 2 {
		t.Errorf("len = %d, want 2", c.Len())
	}
}
