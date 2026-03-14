package nucleus

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestSQLModelExec(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			capturedSQL = sql
			capturedArgs = args
			return pgconn.NewCommandTag("INSERT 0 3"), nil
		},
	}

	sql := &SQLModel{pool: q}
	n, err := sql.Exec(context.Background(), "INSERT INTO users (name) VALUES ($1)", "Alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Errorf("rows affected = %d, want 3", n)
	}
	if capturedSQL != "INSERT INTO users (name) VALUES ($1)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
	if len(capturedArgs) != 1 || capturedArgs[0] != "Alice" {
		t.Errorf("args = %v", capturedArgs)
	}
}

func TestSQLModelExecUpdate(t *testing.T) {
	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 5"), nil
		},
	}

	sql := &SQLModel{pool: q}
	n, err := sql.Exec(context.Background(), "UPDATE users SET active = true")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("rows affected = %d, want 5", n)
	}
}

func TestSQLModelExecDelete(t *testing.T) {
	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("DELETE 2"), nil
		},
	}

	sql := &SQLModel{pool: q}
	n, err := sql.Exec(context.Background(), "DELETE FROM sessions WHERE expired = true")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Errorf("rows affected = %d, want 2", n)
	}
}

func TestSQLModelExecError(t *testing.T) {
	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, fmt.Errorf("relation does not exist")
		},
	}

	sql := &SQLModel{pool: q}
	_, err := sql.Exec(context.Background(), "INSERT INTO missing_table (x) VALUES (1)")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSQLModelExecNoArgs(t *testing.T) {
	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			if len(args) != 0 {
				t.Errorf("expected no args, got %d", len(args))
			}
			return pgconn.NewCommandTag("CREATE TABLE"), nil
		},
	}

	sql := &SQLModel{pool: q}
	_, err := sql.Exec(context.Background(), "CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQLModelExecMultipleParams(t *testing.T) {
	var capturedArgs []any

	q := &mockCDCQuerier{
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			capturedArgs = args
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	sql := &SQLModel{pool: q}
	_, err := sql.Exec(context.Background(),
		"INSERT INTO users (name, age, active) VALUES ($1, $2, $3)",
		"Bob", 30, true,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(capturedArgs) != 3 {
		t.Fatalf("args len = %d, want 3", len(capturedArgs))
	}
	if capturedArgs[0] != "Bob" {
		t.Errorf("arg[0] = %v", capturedArgs[0])
	}
	if capturedArgs[1] != 30 {
		t.Errorf("arg[1] = %v", capturedArgs[1])
	}
	if capturedArgs[2] != true {
		t.Errorf("arg[2] = %v", capturedArgs[2])
	}
}

// Test the querier interface is satisfied
func TestQuerierInterface(t *testing.T) {
	// Verify the querier interface compiles with our mock
	var q querier = &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error { return nil }}
		},
		queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			return nil, nil
		},
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, nil
		},
	}
	_ = q
}

func TestWrapErr(t *testing.T) {
	// Test wrapErr with nil error
	err := wrapErr("test", nil)
	if err != nil {
		t.Errorf("wrapErr(nil) = %v, want nil", err)
	}

	// Test wrapErr with actual error
	err = wrapErr("kv get", fmt.Errorf("connection refused"))
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	expected := "nucleus: kv get: connection refused"
	if err.Error() != expected {
		t.Errorf("error = %q, want %q", err.Error(), expected)
	}
}
