package nucleus

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

func TestKVOptionCombined(t *testing.T) {
	ttl := 10 * time.Second
	o := applyKVOpts([]KVOption{WithTTL(ttl), WithNamespace("cache")})
	if o.ttl == nil || *o.ttl != ttl {
		t.Errorf("ttl = %v, want %v", o.ttl, ttl)
	}
	key := o.resolveKey("user:1")
	if key != "cache:user:1" {
		t.Errorf("key = %q, want cache:user:1", key)
	}
}

func TestKVEntryStruct(t *testing.T) {
	e := KVEntry{Key: "hello", Value: []byte("world")}
	if e.Key != "hello" {
		t.Errorf("Key = %q", e.Key)
	}
	if string(e.Value) != "world" {
		t.Errorf("Value = %q", string(e.Value))
	}
}

func TestKVRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	kv := &KVModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Get", func() error { _, err := kv.Get(context.Background(), "k"); return err }},
		{"Set", func() error { return kv.Set(context.Background(), "k", []byte("v")) }},
		{"SetNX", func() error { _, err := kv.SetNX(context.Background(), "k", []byte("v")); return err }},
		{"Delete", func() error { _, err := kv.Delete(context.Background(), "k"); return err }},
		{"Exists", func() error { _, err := kv.Exists(context.Background(), "k"); return err }},
		{"Incr", func() error { _, err := kv.Incr(context.Background(), "k"); return err }},
		{"TTL", func() error { _, err := kv.TTL(context.Background(), "k"); return err }},
		{"Expire", func() error { _, err := kv.Expire(context.Background(), "k", time.Minute); return err }},
		{"DBSize", func() error { _, err := kv.DBSize(context.Background()); return err }},
		{"FlushDB", func() error { return kv.FlushDB(context.Background()) }},
		{"LPush", func() error { _, err := kv.LPush(context.Background(), "k", "v"); return err }},
		{"RPush", func() error { _, err := kv.RPush(context.Background(), "k", "v"); return err }},
		{"LPop", func() error { _, err := kv.LPop(context.Background(), "k"); return err }},
		{"RPop", func() error { _, err := kv.RPop(context.Background(), "k"); return err }},
		{"LRange", func() error { _, err := kv.LRange(context.Background(), "k", 0, 10); return err }},
		{"LLen", func() error { _, err := kv.LLen(context.Background(), "k"); return err }},
		{"LIndex", func() error { _, err := kv.LIndex(context.Background(), "k", 0); return err }},
		{"HSet", func() error { _, err := kv.HSet(context.Background(), "k", "f", "v"); return err }},
		{"HGet", func() error { _, err := kv.HGet(context.Background(), "k", "f"); return err }},
		{"HDel", func() error { _, err := kv.HDel(context.Background(), "k", "f"); return err }},
		{"HExists", func() error { _, err := kv.HExists(context.Background(), "k", "f"); return err }},
		{"HGetAll", func() error { _, err := kv.HGetAll(context.Background(), "k"); return err }},
		{"HLen", func() error { _, err := kv.HLen(context.Background(), "k"); return err }},
		{"SAdd", func() error { _, err := kv.SAdd(context.Background(), "k", "m"); return err }},
		{"SRem", func() error { _, err := kv.SRem(context.Background(), "k", "m"); return err }},
		{"SMembers", func() error { _, err := kv.SMembers(context.Background(), "k"); return err }},
		{"SIsMember", func() error { _, err := kv.SIsMember(context.Background(), "k", "m"); return err }},
		{"SCard", func() error { _, err := kv.SCard(context.Background(), "k"); return err }},
		{"ZAdd", func() error { _, err := kv.ZAdd(context.Background(), "k", 1.0, "m"); return err }},
		{"ZRange", func() error { _, err := kv.ZRange(context.Background(), "k", 0, 10); return err }},
		{"ZRangeByScore", func() error { _, err := kv.ZRangeByScore(context.Background(), "k", 0, 100); return err }},
		{"ZRem", func() error { _, err := kv.ZRem(context.Background(), "k", "m"); return err }},
		{"ZCard", func() error { _, err := kv.ZCard(context.Background(), "k"); return err }},
		{"PFAdd", func() error { _, err := kv.PFAdd(context.Background(), "k", "e"); return err }},
		{"PFCount", func() error { _, err := kv.PFCount(context.Background(), "k"); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected error for non-Nucleus database")
			}
		})
	}
}

func TestKVGet(t *testing.T) {
	val := "hello-world"
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(**string)) = &val
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	data, err := kv.Get(context.Background(), "mykey")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "hello-world" {
		t.Errorf("data = %q, want hello-world", string(data))
	}
}

func TestKVGetNil(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(**string)) = nil
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	data, err := kv.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data != nil {
		t.Errorf("data = %v, want nil", data)
	}
}

func TestKVGetDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("connection refused")
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	_, err := kv.Get(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestKVSet(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			capturedSQL = sql
			capturedArgs = args
			return pgconn.CommandTag{}, nil
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	err := kv.Set(context.Background(), "mykey", []byte("myval"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(capturedSQL, "KV_SET") {
		t.Errorf("SQL = %q, expected KV_SET", capturedSQL)
	}
	if capturedArgs[0] != "mykey" {
		t.Errorf("key = %v", capturedArgs[0])
	}
}

func TestKVSetWithTTL(t *testing.T) {
	var capturedArgs []any

	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			capturedArgs = args
			return pgconn.CommandTag{}, nil
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	err := kv.Set(context.Background(), "mykey", []byte("myval"), WithTTL(5*time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// With TTL, should have 3 args: key, value, ttl_seconds
	if len(capturedArgs) != 3 {
		t.Fatalf("args len = %d, want 3", len(capturedArgs))
	}
	ttlSecs, ok := capturedArgs[2].(int64)
	if !ok {
		t.Fatalf("ttl arg type = %T, want int64", capturedArgs[2])
	}
	if ttlSecs != 300 {
		t.Errorf("ttl = %d, want 300", ttlSecs)
	}
}

func TestKVSetWithNamespace(t *testing.T) {
	var capturedArgs []any

	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			capturedArgs = args
			return pgconn.CommandTag{}, nil
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	err := kv.Set(context.Background(), "mykey", []byte("myval"), WithNamespace("cache"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedArgs[0] != "cache:mykey" {
		t.Errorf("key = %v, want cache:mykey", capturedArgs[0])
	}
}

func TestKVDelete(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	ok, err := kv.Delete(context.Background(), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
}

func TestKVExists(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	ok, err := kv.Exists(context.Background(), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
}

func TestKVIncr(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				// Incr scans natively into *int64 — the #23 pgwire fix
				// describes KV_INCR as BIGINT, so the CAST-to-TEXT
				// workaround is gone.
				*(dest[0].(*int64)) = 5
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	val, err := kv.Incr(context.Background(), "counter")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 5 {
		t.Errorf("val = %d, want 5", val)
	}
}

func TestKVIncrWithAmount(t *testing.T) {
	var capturedArgs []any
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 10
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	val, err := kv.Incr(context.Background(), "counter", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 10 {
		t.Errorf("val = %d, want 10", val)
	}
	if len(capturedArgs) != 2 {
		t.Fatalf("args len = %d, want 2", len(capturedArgs))
	}
}

func TestKVDBSize(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 42
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	n, err := kv.DBSize(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 42 {
		t.Errorf("n = %d, want 42", n)
	}
}

func TestKVFlushDB(t *testing.T) {
	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, nil
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	err := kv.FlushDB(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestKVSetNX(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	ok, err := kv.SetNX(context.Background(), "k", []byte("v"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
}

func TestKVTTL(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 300
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	ttl, err := kv.TTL(context.Background(), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ttl != 300 {
		t.Errorf("ttl = %d, want 300", ttl)
	}
}

// rawStringQuerier returns a querier whose QueryRow scans the given string into
// the first *string destination, simulating the single-TEXT-column result the
// KV collection SQL functions emit.
func rawStringQuerier(raw string) *mockCDCQuerier {
	return &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = raw
				return nil
			}}
		},
	}
}

func TestKVLRangeJSON(t *testing.T) {
	// Includes an element with an embedded comma to prove JSON, not comma-split.
	kv := &KVModel{pool: rawStringQuerier(`["a","b,c"]`), client: nucleusClient()}
	got, err := kv.LRange(context.Background(), "k", 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a", "b,c"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestKVLRangeEmpty(t *testing.T) {
	kv := &KVModel{pool: rawStringQuerier(`[]`), client: nucleusClient()}
	got, err := kv.LRange(context.Background(), "k", 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

func TestKVSMembersJSON(t *testing.T) {
	kv := &KVModel{pool: rawStringQuerier(`["x","y"]`), client: nucleusClient()}
	got, err := kv.SMembers(context.Background(), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"x", "y"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestKVSMembersEmpty(t *testing.T) {
	kv := &KVModel{pool: rawStringQuerier(`[]`), client: nucleusClient()}
	got, err := kv.SMembers(context.Background(), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

func TestKVHGetAllJSON(t *testing.T) {
	// Value "v=2" carries an embedded '=' to prove JSON pairs, not '=' split.
	kv := &KVModel{pool: rawStringQuerier(`[["f1","v1"],["f2","v=2"]]`), client: nucleusClient()}
	got, err := kv.HGetAll(context.Background(), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["f1"] != "v1" {
		t.Errorf("f1 = %q, want v1", got["f1"])
	}
	if got["f2"] != "v=2" {
		t.Errorf("f2 = %q, want v=2", got["f2"])
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
}

func TestKVHGetAllEmpty(t *testing.T) {
	kv := &KVModel{pool: rawStringQuerier(`[]`), client: nucleusClient()}
	got, err := kv.HGetAll(context.Background(), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty non-nil map", got)
	}
}

func TestKVZRangeJSON(t *testing.T) {
	// "bob,x" carries an embedded comma; scores are JSON numbers.
	kv := &KVModel{pool: rawStringQuerier(`[["alice",1.0],["bob,x",2.5]]`), client: nucleusClient()}
	got, err := kv.ZRange(context.Background(), "k", 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Members only. This asserted []string{"alice:1", "bob,x:2.5"} — the joined
	// form — so the test and the implementation shared one wrong assumption and
	// the test passed while pinning the bug. The engine returns clean JSON
	// pairs; joining them here is what made a member containing ':' ambiguous.
	want := []string{"alice", "bob,x"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestKVZRangeByScoreJSON(t *testing.T) {
	kv := &KVModel{pool: rawStringQuerier(`[["a",10],["b",20.25]]`), client: nucleusClient()}
	got, err := kv.ZRangeByScore(context.Background(), "k", 0, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a", "b"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestKVZRangeEmpty(t *testing.T) {
	kv := &KVModel{pool: rawStringQuerier(`[]`), client: nucleusClient()}
	got, err := kv.ZRange(context.Background(), "k", 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

func TestKVExpire(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}

	kv := &KVModel{pool: q, client: nucleusClient()}
	ok, err := kv.Expire(context.Background(), "k", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
}
