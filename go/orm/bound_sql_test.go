package orm

import (
	"errors"
	"testing"
)

func TestBoundSQLParametersDetachedBeforeExecution(t *testing.T) {
	table, _, _, _, _, _, _ := setupMetadata(t)
	value := int64(9007199254740993)
	plan, err := NewBoundSQL(table, "SELECT bound trusted shape", &value)
	if err != nil {
		t.Fatal(err)
	}
	value = 0
	compiled, err := CompileBound(plan)
	if err != nil || compiled.SQL != "SELECT bound trusted shape" || compiled.Args[0] != int64(9007199254740993) {
		t.Fatal("bound pointer alias", compiled, err)
	}
	compiled.Args[0] = int64(7)
	fresh, err := CompileBound(plan)
	if err != nil || fresh.Args[0] != int64(9007199254740993) {
		t.Fatal("bound compile args alias", fresh, err)
	}
	if _, err := NewBoundSQL(table, "SELECT $1", []byte{1, 2}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("mutable uncertified slice bound", err)
	}
	if _, err := NewBoundSQL(table, "SELECT $1", Bytea{}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid codec bound", err)
	}
	if _, err := NewBoundSQL(table, " "); err == nil {
		t.Fatal("empty SQL allowed")
	}
}
