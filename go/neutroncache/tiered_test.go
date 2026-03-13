package neutroncache

import (
	"context"
	"testing"
	"time"
)

func TestTieredL1Only(t *testing.T) {
	c := NewTiered(100, nil) // no L2

	err := Set(context.Background(), c, "key", map[string]string{"hello": "world"}, time.Minute)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := Get[map[string]string](context.Background(), c, "key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got["hello"] != "world" {
		t.Errorf("value = %v", got)
	}
}

func TestTieredMiss(t *testing.T) {
	c := NewTiered(100, nil)

	_, err := Get[string](context.Background(), c, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestTieredInvalidate(t *testing.T) {
	c := NewTiered(100, nil)

	Set(context.Background(), c, "key", "value", time.Minute)
	Invalidate(context.Background(), c, "key")

	_, err := Get[string](context.Background(), c, "key")
	if err == nil {
		t.Fatal("expected miss after invalidate")
	}
}
