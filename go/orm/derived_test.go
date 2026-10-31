package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestTypedSetAndDerivedBindingCompilation(t *testing.T) {
	table, id, _, _, _, _, _ := setupMetadata(t)
	left, _ := NewModelQuery(table, Query[testModel]{}.Where(id.Eq(1)))
	right, _ := NewModelQuery(table, Query[testModel]{}.Where(id.Eq(2)))
	union, err := UnionAll(left, right)
	if err != nil {
		t.Fatal(err)
	}
	derived, err := NewCTE(`rows"quoted`, union)
	if err != nil {
		t.Fatal(err)
	}
	field, err := DerivedColumn[testModel, int64](derived, "ID")
	if err != nil {
		t.Fatal(err)
	}
	sql, args, err := derived.compile(quote(field.field.name), Query[testModel]{}.Where(field.Gt(0)).OrderBy(field.Asc()).Limit(5))
	if err != nil || !strings.Contains(sql, `WITH "rows""quoted" AS ((SELECT`) || !strings.Contains(sql, `"id" = $1) UNION ALL (SELECT`) || !strings.Contains(sql, `"id" = $2)) SELECT "id" FROM "rows""quoted" WHERE "id" > $3`) || !reflect.DeepEqual(args, []any{int64(1), int64(2), int64(0), 5}) {
		t.Fatal(sql, args, err)
	}
	if _, _, err := derived.compile(derived.table.info.columns(), Query[testModel]{}.Where(id.Gt(0))); err == nil {
		t.Fatal("original column entered derived scope")
	}
	if _, err := DerivedColumn[testModel, string](derived, "ID"); err == nil {
		t.Fatal("derived scalar type mismatch accepted")
	}
	relation, _, _ := selfJoinMetadata(t, "owned")
	anchor, _ := NewModelQuery(relation.parent, Query[selfModel]{})
	if _, err := NewRecursiveCTE("recursive", anchor, relation, 129, 10); err == nil {
		t.Fatal("recursive depth budget omitted")
	}
}
