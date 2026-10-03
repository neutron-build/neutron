package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestTypedSubqueryBindingsAndCompositeCorrelation(t *testing.T) {
	relation, childID := associationMetadata(t, "owned")
	parentID, err := NewColumn[assocParent, int64](relation.parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	scalar, err := NewScalarQuery(childID, Query[assocChild]{}.Where(childID.Gt(10)))
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := CompileSelect(relation.parent, Query[assocParent]{}.Where(And(parentID.Gt(0), InSubquery(parentID, scalar))))
	if err != nil || !strings.Contains(compiled.SQL, `"owned"."parents"."id" IN (SELECT "id" FROM "owned"."children" WHERE "id" > $2)`) || !reflect.DeepEqual(compiled.Args, []any{int64(0), int64(10)}) {
		t.Fatal(compiled, err)
	}
	compiled, err = CompileSelect(relation.parent, Query[assocParent]{}.Where(ExistsRelated(relation, Query[assocChild]{}.Where(childID.Gt(10)))))
	if err != nil || !strings.Contains(compiled.SQL, `"owned"."parents"."tenant" = "__neutron_subquery_child"."tenant"`) || !strings.Contains(compiled.SQL, `"owned"."parents"."id" = "__neutron_subquery_child"."parent_id"`) || !reflect.DeepEqual(compiled.Args, []any{int64(10)}) {
		t.Fatal(compiled, err)
	}
	if _, err := CompileSelect(relation.parent, Query[assocParent]{}.Where(CompareSubquery(parentID, Comparison("=; DROP"), scalar))); err == nil {
		t.Fatal("raw comparator accepted")
	}
	if _, err := CompileSelect(relation.parent, Query[assocParent]{}.Where(InSubquery(parentID, ScalarQuery[assocChild, int64]{}))); err == nil {
		t.Fatal("zero scalar plan accepted")
	}
	other, _ := associationMetadata(t, "owned")
	if _, err := CompileSelect(other.parent, Query[assocParent]{}.Where(ExistsRelated(relation, Query[assocChild]{}))); err == nil {
		t.Fatal("correlation escaped exact parent binding")
	}
	self, id, name := selfJoinMetadata(t, "owned")
	nested := ExistsRelated(self, Query[selfModel]{}.Where(ExistsRelated(self, Query[selfModel]{}.Where(name.Eq("grand")))))
	compiled, err = CompileSelect(self.parent, Query[selfModel]{}.Where(nested).OrderBy(id.Asc()))
	if err != nil || !strings.Contains(compiled.SQL, `AS "__neutron_subquery_childx"`) || !strings.Contains(compiled.SQL, `"__neutron_subquery_child"."id" = "__neutron_subquery_childx"."manager"`) {
		t.Fatal("nested self correlation shadowed outer alias", compiled, err)
	}
}
