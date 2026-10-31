package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestMultipleCTEDependenciesBindingsAndParameterOrder(t *testing.T) {
	table, id, _, _, _, _, _ := setupMetadata(t)
	base, err := NewModelQuery(table, Query[testModel]{}.Where(id.Gt(0)))
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewCTE(`first"rows`, base)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := DerivedColumn[testModel, int64](first, "ID")
	if err != nil {
		t.Fatal(err)
	}
	next, err := FromCTE(first, Query[testModel]{}.Where(firstID.Lt(10)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCTE("second", next)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := DerivedColumn[testModel, int64](second, "ID")
	if err != nil {
		t.Fatal(err)
	}
	sql, args, err := second.compile(quote(secondID.field.name), Query[testModel]{}.Where(secondID.Gt(2)))
	if err != nil || !strings.Contains(sql, `WITH "first""rows" AS (SELECT`) || !strings.Contains(sql, `), "second" AS (SELECT`) || !strings.Contains(sql, `FROM "first""rows" WHERE "id" < $2`) || !reflect.DeepEqual(args, []any{int64(0), int64(10), int64(2)}) {
		t.Fatal(sql, args, err)
	}
	other, err := NewCTE("second", base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.WithCTEs(other); err == nil {
		t.Fatal("different binding with same CTE alias accepted")
	}
	if _, err := second.WithCTEs(second); err == nil {
		t.Fatal("dependency cycle accepted")
	}
	if _, err := second.WithCTEs(Derived[testModel]{}); err == nil {
		t.Fatal("zero dependency accepted")
	}
	if _, err := FromCTE(first, Query[testModel]{}.Where(id.Gt(0))); err == nil {
		t.Fatal("original column entered dependent CTE scope")
	}
}
