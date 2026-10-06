package orm

import (
	"reflect"
	"strings"
	"testing"
)

type selfModel struct {
	ID      int64  `db:"id"`
	Manager int64  `db:"manager"`
	Name    string `db:"name"`
}

func selfJoinMetadata(t *testing.T, schema string) (Relation[selfModel, selfModel], Column[selfModel, int64], Column[selfModel, string]) {
	t.Helper()
	table, err := NewTable[selfModel](schema, "employees")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewColumn[selfModel, int64](table, "ID")
	manager, _ := NewColumn[selfModel, int64](table, "Manager")
	name, _ := NewColumn[selfModel, string](table, "Name")
	relation, err := NewRelation(table, table, Join(id, manager))
	if err != nil {
		t.Fatal(err)
	}
	return relation, id, name
}
func TestAliasedSelfJoinRoleBindings(t *testing.T) {
	relation, id, name := selfJoinMetadata(t, "owned")
	scope, err := NewAliasedLeftJoin(relation, `p"; --`, "c")
	if err != nil {
		t.Fatal(err)
	}
	first, _ := JoinParentField(scope, id)
	second, _ := LeftChildField(scope, name)
	query := scope.Query().WhereParent(id.Eq(1)).WhereChild(name.In("child")).OrderParent(id.Asc()).OrderChild(name.Desc().NullsLast())
	sql, args, err := joinedSQL(query, []joinedProjection{{first.info, first.field, first.outer, first.child}, {second.info, second.field, second.outer, second.child}})
	if err != nil || !strings.Contains(sql, `SELECT "p""; --"."id", "c"."name"`) || !strings.Contains(sql, `ON ("p""; --"."id" = "c"."manager")`) || !strings.Contains(sql, `"c"."name" IN ($2)`) || !reflect.DeepEqual(args, []any{int64(1), "child"}) {
		t.Fatal(sql, args, err)
	}
	if _, err := NewAliasedInnerJoin(relation, "same", "same"); err == nil {
		t.Fatal("ambiguous aliases accepted")
	}
	other, _ := NewAliasedLeftJoin(relation, "p", "c")
	if err := validateJoinedField(other.Query(), first); err == nil {
		t.Fatal("self field from different binding escaped")
	}
}
