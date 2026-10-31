package orm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestBulkAdmissionAndCopySourceBudgets(t *testing.T) {
	table, id, _, _, _, name, note := setupMetadata(t)
	if _, err := InsertBatch(context.Background(), nil, table, [][]Assignment[testModel]{{Set(name, Some(""))}, {Set(id, Some(int64(1))), Set(id, Some(int64(2)))}}, 2); err == nil || errors.Is(err, ErrScopeClosed) {
		t.Fatal("late row not prevalidated", err)
	}
	if _, err := UpsertOne(context.Background(), nil, table, []BoundColumn[testModel]{BindColumn(id)}, []Assignment[testModel]{Set(id, Some(int64(1))), Set(name, Some(""))}, []BoundColumn[testModel]{BindColumn(id)}); err == nil || errors.Is(err, ErrScopeClosed) {
		t.Fatal("upsert changed conflict identity", err)
	}
	bounded := &boundedCopySource{source: pgx.CopyFromRows([][]any{{int64(1), nil}, {int64(2), nil}}), columns: []fieldInfo{id.field, note.field}, maxRows: 1}
	if !bounded.Next() {
		t.Fatal("first row refused")
	}
	values, err := bounded.Values()
	if err != nil || len(values) != 2 || values[1] != nil {
		t.Fatal(values, err)
	}
	if bounded.Next() || !errors.Is(bounded.Err(), ErrCopyBudget) {
		t.Fatal("COPY overflow not retained", bounded.Err())
	}
	for _, values := range [][]any{{"wrong", nil}, {int64(1)}, {nil, nil}} {
		source := &boundedCopySource{source: pgx.CopyFromRows([][]any{values}), columns: []fieldInfo{id.field, note.field}, maxRows: 1}
		if !source.Next() {
			t.Fatal("test source admission")
		}
		if _, err := source.Values(); err == nil || source.Err() == nil {
			t.Fatal("invalid dynamic COPY value escaped metadata check", values, err)
		}
	}
}
