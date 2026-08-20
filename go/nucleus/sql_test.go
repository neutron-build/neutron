package nucleus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// mockScanRows simulates Nucleus pgwire results for scanRow tests.
// All column values are text strings, matching real Nucleus behavior.
type mockScanRows struct {
	cols   []string
	vals   []*string
	closed bool
}

func (m *mockScanRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(m.cols))
	for i, c := range m.cols {
		fds[i] = pgconn.FieldDescription{Name: c}
	}
	return fds
}

func (m *mockScanRows) Scan(dest ...any) error {
	for i, d := range dest {
		pp, ok := d.(**string)
		if !ok {
			return fmt.Errorf("expected **string at position %d, got %T", i, d)
		}
		*pp = m.vals[i]
	}
	return nil
}

func (m *mockScanRows) Next() bool                         { return false }
func (m *mockScanRows) Close()                              { m.closed = true }
func (m *mockScanRows) Err() error                          { return nil }
func (m *mockScanRows) CommandTag() pgconn.CommandTag        { return pgconn.CommandTag{} }
func (m *mockScanRows) RawValues() [][]byte                  { return nil }
func (m *mockScanRows) Conn() *pgx.Conn                     { return nil }
func (m *mockScanRows) Values() ([]any, error)              { return nil, nil }

func strPtr(s string) *string { return &s }

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
	// First arg is pgx.QueryExecModeSimpleProtocol, user args follow
	if len(capturedArgs) < 2 || capturedArgs[1] != "Alice" {
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
			// Expect 1 arg: SimpleProtocol mode (no user args)
			if len(args) != 1 {
				t.Errorf("expected 1 arg (SimpleProtocol), got %d", len(args))
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
	// First arg is SimpleProtocol, then user args
	if len(capturedArgs) != 4 {
		t.Fatalf("args len = %d, want 4 (1 SimpleProtocol + 3 user)", len(capturedArgs))
	}
	if capturedArgs[1] != "Bob" {
		t.Errorf("arg[1] = %v", capturedArgs[1])
	}
	if capturedArgs[2] != 30 {
		t.Errorf("arg[2] = %v", capturedArgs[2])
	}
	if capturedArgs[3] != true {
		t.Errorf("arg[3] = %v", capturedArgs[3])
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

// ─── scanRow Tests ───

func TestScanRow_StringFields(t *testing.T) {
	type row struct {
		Name  string `db:"name"`
		Value string `db:"value"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"name", "value"},
		vals: []*string{strPtr("alice"), strPtr("hello")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Name != "alice" || r.Value != "hello" {
		t.Errorf("got %+v", r)
	}
}

func TestScanRow_IntFields(t *testing.T) {
	type row struct {
		Count int64 `db:"count"`
		Small int32 `db:"small"`
		Tiny  int   `db:"tiny"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"count", "small", "tiny"},
		vals: []*string{strPtr("42"), strPtr("7"), strPtr("3")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Count != 42 || r.Small != 7 || r.Tiny != 3 {
		t.Errorf("got %+v", r)
	}
}

func TestScanRow_FloatFields(t *testing.T) {
	type row struct {
		Rate  float64 `db:"rate"`
		Score float32 `db:"score"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"rate", "score"},
		vals: []*string{strPtr("3.14"), strPtr("99.5")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Rate != 3.14 {
		t.Errorf("rate: got %v want 3.14", r.Rate)
	}
}

func TestScanRow_BoolFields(t *testing.T) {
	type row struct {
		A bool `db:"a"`
		B bool `db:"b"`
		C bool `db:"c"`
		D bool `db:"d"`
		E bool `db:"e"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"a", "b", "c", "d", "e"},
		vals: []*string{strPtr("true"), strPtr("t"), strPtr("1"), strPtr("TRUE"), strPtr("false")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.A || !r.B || !r.C || !r.D || r.E {
		t.Errorf("got %+v", r)
	}
}

func TestScanRow_TimeFields_EpochMs(t *testing.T) {
	type row struct {
		CreatedAt time.Time `db:"created_at"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"created_at"},
		vals: []*string{strPtr("1712000000000")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.UnixMilli(1712000000000).UTC()
	if !r.CreatedAt.Equal(want) {
		t.Errorf("got %v want %v", r.CreatedAt, want)
	}
}

func TestScanRow_TimeFields_RFC3339(t *testing.T) {
	type row struct {
		TS time.Time `db:"ts"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"ts"},
		vals: []*string{strPtr("2024-04-01T12:00:00Z")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.TS.IsZero() {
		t.Error("expected non-zero time")
	}
	if r.TS.Year() != 2024 || r.TS.Month() != 4 || r.TS.Day() != 1 {
		t.Errorf("got %v", r.TS)
	}
}

func TestScanRow_TimeFields_Empty(t *testing.T) {
	type row struct {
		TS time.Time `db:"ts"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"ts"},
		vals: []*string{strPtr("")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.TS.IsZero() {
		t.Errorf("expected zero time, got %v", r.TS)
	}
}

func TestScanRow_NullValues(t *testing.T) {
	type row struct {
		Name  string `db:"name"`
		Count int64  `db:"count"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"name", "count"},
		vals: []*string{nil, nil},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Name != "" || r.Count != 0 {
		t.Errorf("expected zero values, got %+v", r)
	}
}

func TestScanRow_UintFields(t *testing.T) {
	type row struct {
		Size uint64 `db:"size"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"size"},
		vals: []*string{strPtr("18446744073709551615")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Size != 18446744073709551615 {
		t.Errorf("got %d", r.Size)
	}
}

func TestScanRow_EmptyNumericDefaults(t *testing.T) {
	type row struct {
		A int64   `db:"a"`
		B float64 `db:"b"`
		C uint64  `db:"c"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"a", "b", "c"},
		vals: []*string{strPtr(""), strPtr(""), strPtr("")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.A != 0 || r.B != 0 || r.C != 0 {
		t.Errorf("expected zero values, got %+v", r)
	}
}

func TestScanRow_JsonTagFallback(t *testing.T) {
	type row struct {
		SiteName string `json:"site_name"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"site_name"},
		vals: []*string{strPtr("My Site")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.SiteName != "My Site" {
		t.Errorf("got %q", r.SiteName)
	}
}

func TestScanRow_MixedTypes(t *testing.T) {
	type row struct {
		ID        string    `db:"id"`
		Count     int64     `db:"count"`
		Rate      float64   `db:"rate"`
		Active    bool      `db:"active"`
		CreatedAt time.Time `db:"created_at"`
	}
	var r row
	rows := &mockScanRows{
		cols: []string{"id", "count", "rate", "active", "created_at"},
		vals: []*string{strPtr("abc"), strPtr("100"), strPtr("0.95"), strPtr("true"), strPtr("1712000000000")},
	}
	if err := scanRow(rows, &r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.ID != "abc" || r.Count != 100 || r.Rate != 0.95 || !r.Active || r.CreatedAt.IsZero() {
		t.Errorf("got %+v", r)
	}
}

func TestParseTimeValue(t *testing.T) {
	cases := []struct {
		input string
		zero  bool
	}{
		{"", true},
		{"0", true},
		{"1712000000000", false},
		{"2024-04-01T12:00:00Z", false},
		{"2024-04-01T12:00:00.123456Z", false},
	}
	for _, tc := range cases {
		got, err := parseTimeValue(tc.input)
		if err != nil {
			t.Errorf("parseTimeValue(%q): %v", tc.input, err)
			continue
		}
		if tc.zero && !got.IsZero() {
			t.Errorf("parseTimeValue(%q): expected zero, got %v", tc.input, got)
		}
		if !tc.zero && got.IsZero() {
			t.Errorf("parseTimeValue(%q): expected non-zero", tc.input)
		}
	}
}

func TestParseTimeValue_Invalid(t *testing.T) {
	_, err := parseTimeValue("not-a-time")
	if err == nil {
		t.Error("expected error for invalid time string")
	}
}
