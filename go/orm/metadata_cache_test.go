package orm

import (
	"sync"
	"testing"
)

func TestMetadataCacheKeepsIndependentTableBindings(t *testing.T) {
	type record struct {
		ID int64 `db:"id"`
	}
	first, err := NewTable[record]("a", "records")
	if err != nil {
		t.Fatal(err)
	}
	column, err := NewColumn[record, int64](first, "ID")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			second, err := NewTable[record]("b", "records")
			if err != nil {
				t.Error(err)
				return
			}
			if first.info == second.info || second.info.sqlName() != `"b"."records"` {
				t.Error("cached shape reused table identity")
			}
			if _, err := CompileSelect(second, Query[record]{}.Where(column.Eq(1))); err == nil {
				t.Error("cached shape accepted foreign binding")
			}
		}()
	}
	workers.Wait()
}
