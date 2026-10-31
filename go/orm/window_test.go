package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestWindowFrameBindingsAndOwnership(t *testing.T) {
	table, id, tenant, _, score, _, _ := setupMetadata(t)
	window := Over(SumInt64(score), []BoundColumn[testModel]{BindColumn(tenant)}, id.Asc()).RowsBetween(Preceding(2), CurrentRow())
	args := []any{}
	expr, err := window.sql(&args)
	if err != nil {
		t.Fatal(err)
	}
	sql, args, err := selectSQLArgs(table, quote(id.field.name)+", "+expr, Query[testModel]{}.Where(tenant.Eq("a")).OrderBy(id.Asc()).Limit(3), args)
	if err != nil || !strings.Contains(sql, `ROWS BETWEEN $1 PRECEDING AND CURRENT ROW`) || !strings.Contains(sql, `WHERE "tenant" = $2`) || !reflect.DeepEqual(args, []any{2, "a", 3}) {
		t.Fatal(sql, args, err)
	}
	bad := window.RowsBetween(Preceding(-1), CurrentRow())
	args = nil
	if _, err := bad.sql(&args); err == nil {
		t.Fatal("negative frame offset accepted")
	}
	args = nil
	if _, err := Over(CountDistinct(id), nil).sql(&args); err == nil {
		t.Fatal("unsupported DISTINCT window accepted")
	}
}
