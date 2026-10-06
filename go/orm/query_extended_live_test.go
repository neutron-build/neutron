package orm

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresMembershipStreamingAndOptional(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	type nullableRecord struct {
		ID    int64   `db:"id"`
		Value *string `db:"value,nullable"`
	}
	table, _ := NewTable[nullableRecord](schema, "nullable_records")
	id, _ := NewColumn[nullableRecord, int64](table, "ID")
	value, _ := NewColumn[nullableRecord, *string](table, "Value")
	if _, err := admin.Exec(ctx, "CREATE TABLE "+table.info.sqlName()+" (id bigint PRIMARY KEY,value text); INSERT INTO "+table.info.sqlName()+" VALUES (1,NULL),(2,'x'),(3,'y')"); err != nil {
		t.Fatal(err)
	}
	x := "x"
	for _, item := range []struct {
		predicate Predicate[nullableRecord]
		oracle    string
	}{
		{value.In(nil, &x), "value IN (NULL,'x')"},
		{value.NotIn(nil, &x), "value NOT IN (NULL,'x')"},
		{value.CompareAll(NotEqual, nil, &x), "value <> ALL(ARRAY[NULL,'x']::text[])"},
		{id.CompareAny(Greater, 1, 2), "id > ANY(ARRAY[1,2])"},
		{id.In(), "FALSE"},
		{id.NotIn(), "TRUE"},
	} {
		actual, err := SelectColumn(ctx, pool, id, Query[nullableRecord]{}.Where(item.predicate).OrderBy(id.Asc()))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := admin.Query(ctx, "SELECT id FROM "+table.info.sqlName()+" WHERE "+item.oracle+" ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		expected := []int64{}
		for rows.Next() {
			var v int64
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			expected = append(expected, v)
		}
		rows.Close()
		if rows.Err() != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatal(actual, expected, rows.Err())
		}
	}
	ordered, err := SelectColumn(ctx, pool, id, Query[nullableRecord]{}.OrderBy(value.Desc().NullsFirst(), id.Asc()))
	if err != nil || !reflect.DeepEqual(ordered, []int64{1, 3, 2}) {
		t.Fatal(ordered, err)
	}
	optional, err := SelectOptional(ctx, pool, table, Query[nullableRecord]{}.Where(id.Eq(99)))
	if err != nil || optional.Valid {
		t.Fatal(optional, err)
	}
	if _, err := SelectOptional(ctx, pool, table, Query[nullableRecord]{}); !errors.Is(err, ErrCardinality) {
		t.Fatal(err)
	}
	for _, stop := range []bool{true, false} {
		err := WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
			visits := 0
			err := Stream(ctx, scope, table, Query[nullableRecord]{}.OrderBy(id.Asc()), 2, func(nullableRecord) (bool, error) { visits++; return !stop, nil })
			if stop {
				if err != nil || visits != 1 {
					t.Fatal(err, visits)
				}
			} else {
				if !errors.Is(err, ErrStreamBudget) || visits != 2 {
					t.Fatal(err, visits)
				}
			}
			_, err = scope.Exec(ctx, "SELECT 1")
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		requirePoolReuse(t, ctx, pool)
	}
}
