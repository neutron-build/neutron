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
				*(dest[0].(*string)) = "ASSERT parent/2"
				return nil
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	msg, err := dl.Assert(context.Background(), "parent(alice, bob)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg != "ASSERT parent/2" {
		t.Errorf("msg = %q", msg)
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
				*(dest[0].(*string)) = "RETRACT parent/2"
				return nil
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	msg, err := dl.Retract(context.Background(), "parent(alice, bob)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg != "RETRACT parent/2" {
		t.Errorf("msg = %q", msg)
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
				*(dest[0].(*string)) = "RULE ancestor/2"
				return nil
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	msg, err := dl.Rule(context.Background(), "ancestor(X, Z)", "parent(X, Y), ancestor(Y, Z)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg != "RULE ancestor/2" {
		t.Error("expected status message")
	}
	if capturedSQL != "SELECT DATALOG_RULE($1)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
	if len(capturedArgs) != 1 {
		t.Fatalf("args len = %d, want 1", len(capturedArgs))
	}
	if capturedArgs[0] != "ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)" {
		t.Errorf("rule = %v", capturedArgs[0])
	}
}

func TestDatalogQuery(t *testing.T) {
	var capturedSQL string

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = `[["alice","bob"],["carol","dave"]]`
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
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "CLEARED parent"
				return nil
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	msg, err := dl.Clear(context.Background(), "parent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg != "CLEARED parent" {
		t.Error("expected status message")
	}
	if capturedSQL != "SELECT DATALOG_CLEAR($1)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
	if len(capturedArgs) != 1 || capturedArgs[0] != "parent" {
		t.Errorf("args = %v, want [parent]", capturedArgs)
	}
}

func TestDatalogImportGraph(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "IMPORTED 25 edges into edge"
				return nil
			}}
		},
	}

	dl := &DatalogModel{pool: q, client: nucleusClient()}
	msg, err := dl.ImportGraph(context.Background(), "edge")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg != "IMPORTED 25 edges into edge" {
		t.Errorf("msg = %q", msg)
	}
	if capturedSQL != "SELECT DATALOG_IMPORT_GRAPH($1)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
	if len(capturedArgs) != 1 || capturedArgs[0] != "edge" {
		t.Errorf("args = %v, want [edge]", capturedArgs)
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
		{"Clear", func() error { _, err := dl.Clear(context.Background(), "p"); return err }},
		{"ImportGraph", func() error { _, err := dl.ImportGraph(context.Background(), "p"); return err }},
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
