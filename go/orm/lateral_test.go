package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestLateralPerParentCompilationAndBinding(t *testing.T) {
	relation, id := associationMetadata(t, "owned")
	childName, _ := NewColumn[assocChild, string](relation.child, "Name")
	parentID, _ := NewColumn[assocParent, int64](relation.parent, "ID")
	scope, err := NewLateralLeftJoin(relation, "p", "c", Query[assocChild]{}.Where(childName.Eq("match")).OrderBy(id.Desc()).Limit(2).Offset(1))
	if err != nil {
		t.Fatal(err)
	}
	first, _ := JoinParentField(scope, parentID)
	second, _ := LeftChildField(scope, id)
	sql, args, err := joinedSQL(scope.Query().WhereParent(parentID.Gt(0)).Limit(4), []joinedProjection{{first.info, first.field, first.outer, first.child}, {second.info, second.field, second.outer, second.child}})
	if err != nil || !strings.Contains(sql, `LEFT JOIN LATERAL (SELECT`) || !strings.Contains(sql, `"__neutron_lateral_source"."tenant" = "p"."tenant" AND "__neutron_lateral_source"."parent_id" = "p"."id"`) || !strings.Contains(sql, `LIMIT $2 OFFSET $3) AS "c" ON TRUE`) || !reflect.DeepEqual(args, []any{"match", 2, 1, int64(0), 4}) {
		t.Fatal(sql, args, err)
	}
	if _, err := NewLateralInnerJoin(relation, "p", "c", Query[assocChild]{}); err == nil {
		t.Fatal("unbounded lateral child accepted")
	}
	foreign, _ := associationMetadata(t, "owned")
	foreignID, _ := NewColumn[assocChild, int64](foreign.child, "ID")
	if _, err := NewLateralInnerJoin(relation, "p", "c", Query[assocChild]{}.OrderBy(foreignID.Asc()).Limit(1)); err == nil {
		t.Fatal("foreign child query accepted")
	}
}
