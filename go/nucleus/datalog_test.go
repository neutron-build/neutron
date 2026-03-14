package nucleus

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestDatalogModelExists(t *testing.T) {
	var _ *DatalogModel
}

func TestDatalogAssert(t *testing.T) {
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

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	ok, err := dl.Assert(context.Background(), "parent(alice, bob)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
	if capturedSQL != "SELECT DATALOG_ASSERT($1)" {
		t.Errorf("SQL = %q, want SELECT DATALOG_ASSERT($1)", capturedSQL)
	}
	if len(capturedArgs) != 1 || capturedArgs[0] != "parent(alice, bob)" {
		t.Errorf("args = %v, want [parent(alice, bob)]", capturedArgs)
	}
}

func TestDatalogRetract(t *testing.T) {
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

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	ok, err := dl.Retract(context.Background(), "parent(alice, bob)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
	if capturedSQL != "SELECT DATALOG_RETRACT($1)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
}

func TestDatalogRule(t *testing.T) {
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

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	ok, err := dl.Rule(context.Background(), "ancestor(X, Z)", "parent(X, Y), ancestor(Y, Z)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
	if capturedSQL != "SELECT DATALOG_RULE($1, $2)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
	if len(capturedArgs) != 2 {
		t.Fatalf("args len = %d, want 2", len(capturedArgs))
	}
	if capturedArgs[0] != "ancestor(X, Z)" {
		t.Errorf("head = %v", capturedArgs[0])
	}
	if capturedArgs[1] != "parent(X, Y), ancestor(Y, Z)" {
		t.Errorf("body = %v", capturedArgs[1])
	}
}

func TestDatalogQuery(t *testing.T) {
	var capturedSQL string

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "alice,bob\ncarol,dave"
				return nil
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	result, err := dl.Query(context.Background(), "ancestor(alice, ?X)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result")
	}
	if capturedSQL != "SELECT DATALOG_QUERY($1)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
}

func TestDatalogClear(t *testing.T) {
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

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	ok, err := dl.Clear(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
	if capturedSQL != "SELECT DATALOG_CLEAR()" {
		t.Errorf("SQL = %q", capturedSQL)
	}
}

func TestDatalogImportGraph(t *testing.T) {
	var capturedSQL string

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 25
				return nil
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	n, err := dl.ImportGraph(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 25 {
		t.Errorf("count = %d, want 25", n)
	}
	if capturedSQL != "SELECT DATALOG_IMPORT_GRAPH()" {
		t.Errorf("SQL = %q", capturedSQL)
	}
}

func TestDatalogRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	dl := &DatalogModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Assert", func() error { _, err := dl.Assert(context.Background(), "f"); return err }},
		{"Retract", func() error { _, err := dl.Retract(context.Background(), "f"); return err }},
		{"Rule", func() error { _, err := dl.Rule(context.Background(), "h", "b"); return err }},
		{"Query", func() error { _, err := dl.Query(context.Background(), "q"); return err }},
		{"Clear", func() error { _, err := dl.Clear(context.Background()); return err }},
		{"ImportGraph", func() error { _, err := dl.ImportGraph(context.Background()); return err }},
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

func TestDatalogAssertDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("db error")
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	_, err := dl.Assert(context.Background(), "fact")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDatalogQueryDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("query timeout")
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	_, err := dl.Query(context.Background(), "q")
	if err == nil {
		t.Fatal("expected error")
	}
}
