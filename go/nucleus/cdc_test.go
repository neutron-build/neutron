package nucleus

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// mockCDCRow implements pgx.Row for CDC tests.
type mockCDCRow struct {
	scanFn func(dest ...any) error
}

func (m *mockCDCRow) Scan(dest ...any) error { return m.scanFn(dest...) }

// mockCDCQuerier implements the querier interface for CDC tests.
type mockCDCQuerier struct {
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (m *mockCDCQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return m.queryRowFn(ctx, sql, args...)
}
func (m *mockCDCQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return m.queryFn(ctx, sql, args...)
}
func (m *mockCDCQuerier) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return m.execFn(ctx, sql, args...)
}

func nucleusClient() *Client {
	return &Client{features: Features{IsNucleus: true}}
}

func plainPGClient() *Client {
	return &Client{features: Features{IsNucleus: false, Version: "PostgreSQL 16.0"}}
}

// --- CDCModel Tests ---

func TestCDCModelExists(t *testing.T) {
	// Verify the CDCModel struct and methods exist and compile
	var _ *CDCModel
}

func TestCDCRead(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = `[{"seq":1,"table":"users","change":"INSERT","ts":1700000000000}]`
				return nil
			}}
		},
	}

	cdc := &CDCModel{pool: q, client: nucleusClient()}
	result, err := cdc.Read(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Assert the DECODED event, not that a string is non-empty. The old
	// assertion was `result == ""`, which a raw JSON payload satisfies
	// whatever it contains — the "a non-empty string is truthy" false green
	// this model's conformance case was filed against.
	if len(result) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(result), result)
	}
	if result[0].Seq != 1 || result[0].Table != "users" ||
		result[0].Change != "INSERT" || result[0].TS != 1700000000000 {
		t.Errorf("event = %+v, want {1 users INSERT 1700000000000}", result[0])
	}
	if capturedSQL != "SELECT CDC_READ($1, $2)" {
		t.Errorf("SQL = %q, want SELECT CDC_READ($1, $2)", capturedSQL)
	}
	if len(capturedArgs) != 2 || capturedArgs[0] != int64(0) || capturedArgs[1] != int64(10) {
		t.Errorf("args = %v, want [0 10]", capturedArgs)
	}
}

func TestCDCReadDefaultLimit(t *testing.T) {
	var capturedArgs []any
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = `[]`
				return nil
			}}
		},
	}

	cdc := &CDCModel{pool: q, client: nucleusClient()}
	_, err := cdc.Read(context.Background(), 42, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Non-positive limit falls back to the default
	if len(capturedArgs) != 2 || capturedArgs[1] != int64(defaultCDCLimit) {
		t.Errorf("args = %v, want limit %d", capturedArgs, defaultCDCLimit)
	}
}

func TestCDCCount(t *testing.T) {
	var capturedSQL string

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 100
				return nil
			}}
		},
	}

	cdc := &CDCModel{pool: q, client: nucleusClient()}
	count, err := cdc.Count(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
	if capturedSQL != "SELECT CDC_COUNT()" {
		t.Errorf("SQL = %q, want SELECT CDC_COUNT()", capturedSQL)
	}
}

func TestCDCTableRead(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = `[{"seq":6,"table":"users","change":"UPDATE","ts":1700000000000}]`
				return nil
			}}
		},
	}

	cdc := &CDCModel{pool: q, client: nucleusClient()}
	result, err := cdc.TableRead(context.Background(), "users", 5, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(result), result)
	}
	if result[0].Seq != 6 || result[0].Table != "users" || result[0].Change != "UPDATE" {
		t.Errorf("event = %+v, want {6 users UPDATE ...}", result[0])
	}
	if capturedSQL != "SELECT CDC_TABLE_READ($1, $2, $3)" {
		t.Errorf("SQL = %q, want SELECT CDC_TABLE_READ($1, $2, $3)", capturedSQL)
	}
	if len(capturedArgs) != 3 {
		t.Fatalf("args len = %d, want 3", len(capturedArgs))
	}
	if capturedArgs[0] != "users" {
		t.Errorf("arg[0] = %v, want 'users'", capturedArgs[0])
	}
	if capturedArgs[1] != int64(5) {
		t.Errorf("arg[1] = %v, want 5", capturedArgs[1])
	}
	if capturedArgs[2] != int64(20) {
		t.Errorf("arg[2] = %v, want 20", capturedArgs[2])
	}
}

func TestCDCRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	cdc := &CDCModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Read", func() error { _, err := cdc.Read(context.Background(), 0, 10); return err }},
		{"Count", func() error { _, err := cdc.Count(context.Background()); return err }},
		{"TableRead", func() error { _, err := cdc.TableRead(context.Background(), "t", 0, 10); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected error for non-Nucleus database")
			}
			if got := err.Error(); got == "" {
				t.Error("expected non-empty error message")
			}
		})
	}
}

func TestCDCReadDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("connection refused")
			}}
		},
	}

	cdc := &CDCModel{pool: q, client: nucleusClient()}
	_, err := cdc.Read(context.Background(), 0, 10)
	if err == nil {
		t.Fatal("expected error from DB failure")
	}
}

func TestCDCCountDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("timeout")
			}}
		},
	}

	cdc := &CDCModel{pool: q, client: nucleusClient()}
	_, err := cdc.Count(context.Background())
	if err == nil {
		t.Fatal("expected error from DB failure")
	}
}
