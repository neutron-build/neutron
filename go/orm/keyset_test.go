package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestKeysetNullLexicographicAndUniqueAdmission(t *testing.T) {
	table, id, tenant, _, _, _, note := setupMetadata(t)
	q := Query[testModel]{}.Where(tenant.Eq("a")).OrderBy(note.Asc().NullsLast(), id.Asc()).Limit(2)
	v := "same"
	after, err := SeekAfter(table, q, []BoundColumn[testModel]{BindColumn(id)}, []Assignment[testModel]{Set(note, Some(&v)), Set(id, Some(int64(3)))})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := CompileSelect(table, after)
	if err != nil || !strings.Contains(compiled.SQL, `("note" > $2 OR "note" IS NULL)`) || !strings.Contains(compiled.SQL, `("note" = $3 AND "id" > $4)`) || !reflect.DeepEqual(compiled.Args, []any{"a", "same", "same", int64(3), 2}) {
		t.Fatal(compiled, err)
	}
	if _, err := SeekAfter(table, q, []BoundColumn[testModel]{BindColumn(note)}, []Assignment[testModel]{Set(note, Some(&v)), Set(id, Some(int64(3)))}); err == nil {
		t.Fatal("nullable unique declaration accepted")
	}
	if _, err := SeekAfter(table, q, []BoundColumn[testModel]{BindColumn(id)}, []Assignment[testModel]{Set(id, Some(int64(3))), Set(note, Some(&v))}); err == nil {
		t.Fatal("cursor order mismatch accepted")
	}
	if _, err := SeekAfter(table, q.Offset(1), []BoundColumn[testModel]{BindColumn(id)}, []Assignment[testModel]{Set(note, Some(&v)), Set(id, Some(int64(3)))}); err == nil {
		t.Fatal("offset keyset accepted")
	}
}
