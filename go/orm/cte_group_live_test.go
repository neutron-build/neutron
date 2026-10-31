package orm

import (
	"reflect"
	"strings"
	"testing"
)

type cteGroupModel struct {
	ID    int64  `db:"id"`
	Value string `db:"value"`
}

func TestPostgresMultipleCTETypedDependencies(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, err := NewTable[cteGroupModel](schema, "records")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewColumn[cteGroupModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO "+records+" (id,value) VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four')"); err != nil {
		t.Fatal(err)
	}
	base, err := NewModelQuery(table, Query[cteGroupModel]{}.Where(id.Gt(1)))
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewCTE("first_rows", base)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := DerivedColumn[cteGroupModel, int64](first, "ID")
	if err != nil {
		t.Fatal(err)
	}
	dependent, err := FromCTE(first, Query[cteGroupModel]{}.Where(firstID.Lt(4)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCTE("second_rows", dependent)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := DerivedColumn[cteGroupModel, int64](second, "ID")
	if err != nil {
		t.Fatal(err)
	}
	models, err := SelectDerived(ctx, admin, second, Query[cteGroupModel]{}.OrderBy(secondID.Desc()))
	if err != nil {
		t.Fatal(err)
	}
	native, err := admin.Query(ctx, "WITH first_rows AS (SELECT id,value FROM "+records+" WHERE id>$1),second_rows AS (SELECT id,value FROM first_rows WHERE id<$2) SELECT id,value FROM second_rows ORDER BY id DESC", int64(1), int64(4))
	if err != nil {
		t.Fatal(err)
	}
	expected := []cteGroupModel{}
	for native.Next() {
		var m cteGroupModel
		if err := native.Scan(&m.ID, &m.Value); err != nil {
			native.Close()
			t.Fatal(err)
		}
		expected = append(expected, m)
	}
	native.Close()
	if err := native.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(models, expected) || len(models) != 2 {
		t.Fatal("native multi CTE oracle mismatch", models, expected)
	}
	plan, err := FromCTE(second, Query[cteGroupModel]{}.Where(secondID.Eq(2)))
	if err != nil {
		t.Fatal(err)
	}
	models, err = SelectModels(ctx, admin, plan)
	if err != nil || len(models) != 1 || models[0].ID != 2 {
		t.Fatal("model plan dropped CTE dependencies", err)
	}
}
