package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// NA-09 regressions: destination selection is a batch decision driven by the
// TABLE count, not the per-table column count (one three-column table used to
// create a directory named after a file argument; several one-column tables
// all selected the same file and overwrote each other).

func TestPlanOutputDestinations(t *testing.T) {
	t.Run("single table with custom file path is a file", func(t *testing.T) {
		root := t.TempDir()
		out := filepath.Join(root, "models.go")
		dests, err := planOutputDestinations([]string{"users"}, "go", out)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if dests["users"] != out {
			t.Errorf("dest = %s, want %s", dests["users"], out)
		}
	})

	t.Run("single table with three columns still one file", func(t *testing.T) {
		// The old bug keyed off column count; the planner never sees columns,
		// and one table is one table regardless of its column count.
		root := t.TempDir()
		out := filepath.Join(root, "models.go")
		dests, err := planOutputDestinations([]string{"wide"}, "go", out)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if dests["wide"] != out {
			t.Errorf("dest = %s, want the file itself", dests["wide"])
		}
		if fi, err := os.Stat(out); err == nil && fi.IsDir() {
			t.Error("a directory named models.go was created for a single-table batch")
		}
	})

	t.Run("two single-column tables get distinct files", func(t *testing.T) {
		root := t.TempDir()
		dests, err := planOutputDestinations([]string{"a", "b"}, "go", root)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if dests["a"] != filepath.Join(root, "a.go") || dests["b"] != filepath.Join(root, "b.go") {
			t.Errorf("dests = %v, want a.go and b.go in %s", dests, root)
		}
	})

	t.Run("two tables with a file-like out is rejected", func(t *testing.T) {
		root := t.TempDir()
		out := filepath.Join(root, "models.go")
		_, err := planOutputDestinations([]string{"a", "b"}, "go", out)
		if err == nil || !strings.Contains(err.Error(), "requires a directory") {
			t.Fatalf("err = %v, want a directory-required error", err)
		}
		if fi, statErr := os.Stat(out); statErr == nil && fi.IsDir() {
			t.Error("a directory named models.go was created by a rejected plan")
		}
	})

	t.Run("two tables over an existing regular file is rejected", func(t *testing.T) {
		root := t.TempDir()
		out := filepath.Join(root, "models.go")
		if err := os.WriteFile(out, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := planOutputDestinations([]string{"a", "b"}, "go", out)
		if err == nil || !strings.Contains(err.Error(), "requires a directory") {
			t.Fatalf("err = %v, want an existing-file error", err)
		}
	})

	t.Run("existing directory named models.go stays valid", func(t *testing.T) {
		root := t.TempDir()
		out := filepath.Join(root, "models.go")
		if err := os.Mkdir(out, 0o755); err != nil {
			t.Fatal(err)
		}
		dests, err := planOutputDestinations([]string{"a", "b"}, "go", out)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if dests["a"] != filepath.Join(out, "a.go") {
			t.Errorf("dest = %s", dests["a"])
		}
	})

	t.Run("trailing separator means directory even if missing", func(t *testing.T) {
		root := t.TempDir()
		out := filepath.Join(root, "gen") + string(filepath.Separator)
		dests, err := planOutputDestinations([]string{"a"}, "go", out)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if dests["a"] != filepath.Join(root, "gen", "a.go") {
			t.Errorf("dest = %s", dests["a"])
		}
	})

	t.Run("empty out defaults to the working directory", func(t *testing.T) {
		dests, err := planOutputDestinations([]string{"a"}, "go", "")
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if dests["a"] != filepath.Join(".", "a.go") {
			t.Errorf("dest = %s", dests["a"])
		}
	})

	t.Run("duplicate destinations are rejected", func(t *testing.T) {
		// Two tables whose file names collide case-insensitively can't happen
		// on one backend, but the plan must still refuse rather than race.
		root := t.TempDir()
		if _, err := planOutputDestinations([]string{"a", "a"}, "go", root); err == nil {
			t.Fatal("duplicate destinations accepted")
		}
	})
}

func TestPublishBatchWritesBytes(t *testing.T) {
	t.Run("two tables produce two files with their own bytes", func(t *testing.T) {
		root := t.TempDir()
		codes := map[string]string{"a": "// code for a", "b": "// code for b"}
		if err := publishBatch([]string{"a", "b"}, codes, "go", root); err != nil {
			t.Fatalf("publish: %v", err)
		}
		for _, table := range []string{"a", "b"} {
			b, err := os.ReadFile(filepath.Join(root, table+".go"))
			if err != nil {
				t.Fatalf("read %s.go: %v", table, err)
			}
			if string(b) != codes[table] {
				t.Errorf("%s.go = %q, want %q", table, b, codes[table])
			}
		}
		// No staging temporaries left behind.
		entries, _ := os.ReadDir(root)
		if len(entries) != 2 {
			t.Errorf("output directory has %d entries, want 2: %v", len(entries), entries)
		}
	})

	t.Run("single table overwrites an existing file atomically", func(t *testing.T) {
		root := t.TempDir()
		out := filepath.Join(root, "models.go")
		if err := os.WriteFile(out, []byte("stale"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := publishBatch([]string{"users"}, map[string]string{"users": "fresh"}, "go", out); err != nil {
			t.Fatalf("publish: %v", err)
		}
		b, _ := os.ReadFile(out)
		if string(b) != "fresh" {
			t.Errorf("file = %q, want fresh", b)
		}
	})
}

// NA-14 regressions: enumeration fails closed. A cursor that fails partway
// must never become a subset batch (or an empty success).

type fakeRows struct {
	names    []string
	scanAt   int // fail the scan on this row index (-1 never)
	scanErr  error
	iterErr  error
	next     int
	closed   bool
	failScan bool
}

func (f *fakeRows) Next() bool {
	if f.next >= len(f.names) {
		return false
	}
	f.next++
	if f.failScan && f.next-1 == f.scanAt {
		return true // row present, but its scan fails
	}
	return true
}

func (f *fakeRows) Scan(dest ...any) error {
	if f.failScan && f.next-1 == f.scanAt {
		return f.scanErr
	}
	*(dest[0].(*string)) = f.names[f.next-1]
	return nil
}

func (f *fakeRows) Err() error { return f.iterErr }
func (f *fakeRows) Close()     { f.closed = true }

func TestEnumerateTablesFailsClosed(t *testing.T) {
	query := func(rows *fakeRows) func(context.Context, string, ...any) (rowIterator, error) {
		return func(context.Context, string, ...any) (rowIterator, error) {
			return rows, nil
		}
	}

	t.Run("scan error on a later row", func(t *testing.T) {
		rows := &fakeRows{names: []string{"a", "b", "c"}, failScan: true, scanAt: 2, scanErr: errors.New("wire reset")}
		tables, err := enumerateTables(context.Background(), query(rows), "public")
		if err == nil || !strings.Contains(err.Error(), "scan tables") {
			t.Fatalf("err = %v, want scan tables failure", err)
		}
		if tables != nil {
			t.Errorf("tables = %v on failure, want nil", tables)
		}
		if !rows.closed {
			t.Error("rows not closed on failure")
		}
	})

	t.Run("iteration error after valid rows", func(t *testing.T) {
		rows := &fakeRows{names: []string{"a"}, iterErr: errors.New("connection dropped")}
		tables, err := enumerateTables(context.Background(), query(rows), "public")
		if err == nil || !strings.Contains(err.Error(), "query table rows") {
			t.Fatalf("err = %v, want iteration failure", err)
		}
		if tables != nil {
			t.Errorf("tables = %v on failure, want nil", tables)
		}
	})

	t.Run("complete read", func(t *testing.T) {
		rows := &fakeRows{names: []string{"a", "b"}}
		tables, err := enumerateTables(context.Background(), query(rows), "public")
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(tables) != 2 || tables[0] != "a" || tables[1] != "b" {
			t.Errorf("tables = %v", tables)
		}
	})
}
