package nucleus

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{"users", true},
		{"my_table", true},
		{"_private", true},
		{"Table123", true},
		{"", false},
		{"123abc", false},
		{"my-table", false},
		{"drop table; --", false},
		{"my table", false},
		{"table.name", false},
	}
	for _, tt := range tests {
		if got := isValidIdentifier(tt.name); got != tt.valid {
			t.Errorf("isValidIdentifier(%q) = %v, want %v", tt.name, got, tt.valid)
		}
	}
}

func TestColumnarRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	c := &ColumnarModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Insert", func() error { _, err := c.Insert(context.Background(), "tbl", nil); return err }},
		{"Count", func() error { _, err := c.Count(context.Background(), "tbl"); return err }},
		{"Sum", func() error { _, err := c.Sum(context.Background(), "tbl", "col"); return err }},
		{"Avg", func() error { _, err := c.Avg(context.Background(), "tbl", "col"); return err }},
		{"Min", func() error { _, err := c.Min(context.Background(), "tbl", "col"); return err }},
		{"Max", func() error { _, err := c.Max(context.Background(), "tbl", "col"); return err }},
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

func TestColumnarInvalidIdentifier(t *testing.T) {
	q := &mockCDCQuerier{}
	c := &ColumnarModel{pool: q, client: nucleusClient()}

	_, err := c.Insert(context.Background(), "bad-name", map[string]any{"a": 1})
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}
	if !strings.Contains(err.Error(), "invalid table name") {
		t.Errorf("error = %q", err.Error())
	}

	_, err = c.Count(context.Background(), "123bad")
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}

	_, err = c.Sum(context.Background(), "has space", "col")
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}

	_, err = c.Avg(context.Background(), "drop;", "col")
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}

	_, err = c.Min(context.Background(), "a.b", "col")
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}

	_, err = c.Max(context.Background(), "a b", "col")
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}
}

func TestColumnarInsert(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				// COLUMNAR_INSERT returns the text 'OK'
				*(dest[0].(*string)) = "OK"
				return nil
			}}
		},
	}

	c := &ColumnarModel{pool: q, client: nucleusClient()}
	ok, err := c.Insert(context.Background(), "metrics", map[string]any{"value": 42, "host": "srv1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
	// Variadic form: (table, col1, val1, col2, val2, ...)
	if capturedSQL != "SELECT COLUMNAR_INSERT($1, $2, $3, $4, $5)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
	if len(capturedArgs) != 5 {
		t.Fatalf("args len = %d, want 5", len(capturedArgs))
	}
	if capturedArgs[0] != "metrics" {
		t.Errorf("args[0] = %v, want metrics", capturedArgs[0])
	}
	// Columns are passed in sorted order as name/value pairs
	if capturedArgs[1] != "host" || capturedArgs[2] != "srv1" {
		t.Errorf("args[1:3] = %v %v, want host srv1", capturedArgs[1], capturedArgs[2])
	}
	if capturedArgs[3] != "value" || capturedArgs[4] != 42 {
		t.Errorf("args[3:5] = %v %v, want value 42", capturedArgs[3], capturedArgs[4])
	}
}

func TestColumnarInsertEmptyValues(t *testing.T) {
	q := &mockCDCQuerier{}
	c := &ColumnarModel{pool: q, client: nucleusClient()}
	_, err := c.Insert(context.Background(), "metrics", nil)
	if err == nil {
		t.Fatal("expected error for empty values")
	}
}

func TestColumnarCount(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 1000
				return nil
			}}
		},
	}

	c := &ColumnarModel{pool: q, client: nucleusClient()}
	n, err := c.Count(context.Background(), "metrics")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1000 {
		t.Errorf("count = %d, want 1000", n)
	}
}

func TestColumnarSum(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*float64)) = 42.5
				return nil
			}}
		},
	}

	c := &ColumnarModel{pool: q, client: nucleusClient()}
	val, err := c.Sum(context.Background(), "metrics", "value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42.5 {
		t.Errorf("sum = %f, want 42.5", val)
	}
}

func TestColumnarSumDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("connection timeout")
			}}
		},
	}

	c := &ColumnarModel{pool: q, client: nucleusClient()}
	_, err := c.Sum(context.Background(), "metrics", "value")
	if err == nil {
		t.Fatal("expected error")
	}
}
