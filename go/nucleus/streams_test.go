package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestStreamEntryStruct(t *testing.T) {
	e := StreamEntry{
		ID:     "1234-0",
		Fields: map[string]any{"temp": 72.5},
	}
	if e.ID != "1234-0" {
		t.Errorf("ID = %q", e.ID)
	}
	if e.Fields["temp"] != 72.5 {
		t.Errorf("Fields = %v", e.Fields)
	}
}

func TestStreamEntryJSON(t *testing.T) {
	e := StreamEntry{
		ID:     "100-0",
		Fields: map[string]any{"action": "login", "user": "alice"},
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	var decoded StreamEntry
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if decoded.ID != "100-0" {
		t.Errorf("ID = %q", decoded.ID)
	}
	if decoded.Fields["action"] != "login" {
		t.Errorf("Fields = %v", decoded.Fields)
	}
}

func TestStreamXAdd(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "1700000000-0"
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	id, err := s.XAdd(context.Background(), "events", map[string]any{"action": "login"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "1700000000-0" {
		t.Errorf("id = %q, want 1700000000-0", id)
	}
	if !strings.Contains(capturedSQL, "STREAM_XADD") {
		t.Errorf("SQL = %q, expected STREAM_XADD", capturedSQL)
	}
	// First arg should be stream name
	if capturedArgs[0] != "events" {
		t.Errorf("args[0] = %v, want events", capturedArgs[0])
	}
}

func TestStreamXAddMultipleFields(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "200-0"
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	// Use a single field to get deterministic ordering
	_, err := s.XAdd(context.Background(), "metrics", map[string]any{"cpu": 72.5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have stream + key + value = 3 args
	if len(capturedArgs) != 3 {
		t.Errorf("args len = %d, want 3", len(capturedArgs))
	}
	// Verify SQL has placeholders
	if !strings.Contains(capturedSQL, "$1") {
		t.Errorf("SQL missing $1: %q", capturedSQL)
	}
}

func TestStreamXLen(t *testing.T) {
	var capturedSQL string

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 42
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	n, err := s.XLen(context.Background(), "events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 42 {
		t.Errorf("len = %d, want 42", n)
	}
	if capturedSQL != "SELECT STREAM_XLEN($1)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
}

func TestStreamXRange(t *testing.T) {
	var capturedSQL string

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			return &mockCDCRow{scanFn: func(dest ...any) error {
				entries := []StreamEntry{
					{ID: "100-0", Fields: map[string]any{"a": 1}},
					{ID: "200-0", Fields: map[string]any{"b": 2}},
				}
				data, _ := json.Marshal(entries)
				*(dest[0].(*string)) = string(data)
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	entries, err := s.XRange(context.Background(), "events", 0, 1000, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("entries len = %d, want 2", len(entries))
	}
	if entries[0].ID != "100-0" {
		t.Errorf("first entry ID = %q", entries[0].ID)
	}
	if capturedSQL != "SELECT STREAM_XRANGE($1, $2, $3, $4)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
}

func TestStreamXRangeUnmarshalError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "not-valid-json{{"
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	_, err := s.XRange(context.Background(), "events", 0, 1000, 10)
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
	if !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("error = %q, expected unmarshal mention", err.Error())
	}
}

func TestStreamXRead(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				entries := []StreamEntry{{ID: "300-0", Fields: map[string]any{"x": 1}}}
				data, _ := json.Marshal(entries)
				*(dest[0].(*string)) = string(data)
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	entries, err := s.XRead(context.Background(), "events", 200, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("entries len = %d, want 1", len(entries))
	}
}

func TestStreamXGroupCreate(t *testing.T) {
	var capturedSQL string

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	ok, err := s.XGroupCreate(context.Background(), "events", "workers", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
	if capturedSQL != "SELECT STREAM_XGROUP_CREATE($1, $2, $3)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
}

func TestStreamXReadGroup(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				entries := []StreamEntry{{ID: "400-0", Fields: map[string]any{"task": "process"}}}
				data, _ := json.Marshal(entries)
				*(dest[0].(*string)) = string(data)
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	entries, err := s.XReadGroup(context.Background(), "events", "workers", "w1", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("entries len = %d, want 1", len(entries))
	}
	if entries[0].Fields["task"] != "process" {
		t.Errorf("Fields = %v", entries[0].Fields)
	}
}

func TestStreamXAck(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	ok, err := s.XAck(context.Background(), "events", "workers", 400, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
	if capturedSQL != "SELECT STREAM_XACK($1, $2, $3, $4)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
	if len(capturedArgs) != 4 {
		t.Fatalf("args len = %d, want 4", len(capturedArgs))
	}
}

func TestStreamRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	s := &StreamModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"XAdd", func() error { _, err := s.XAdd(context.Background(), "s", map[string]any{}); return err }},
		{"XLen", func() error { _, err := s.XLen(context.Background(), "s"); return err }},
		{"XRange", func() error { _, err := s.XRange(context.Background(), "s", 0, 100, 10); return err }},
		{"XRead", func() error { _, err := s.XRead(context.Background(), "s", 0, 10); return err }},
		{"XGroupCreate", func() error { _, err := s.XGroupCreate(context.Background(), "s", "g", 0); return err }},
		{"XReadGroup", func() error { _, err := s.XReadGroup(context.Background(), "s", "g", "c", 1); return err }},
		{"XAck", func() error { _, err := s.XAck(context.Background(), "s", "g", 0, 0); return err }},
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

func TestStreamXAddDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("connection refused")
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	_, err := s.XAdd(context.Background(), "events", map[string]any{"k": "v"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestStreamXRangeDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("timeout")
			}}
		},
	}

	s := &StreamModel{pool: q, client: nucleusClient()}
	_, err := s.XRange(context.Background(), "events", 0, 100, 10)
	if err == nil {
		t.Fatal("expected error")
	}
}
