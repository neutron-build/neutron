package orm

import (
	"errors"
	"testing"
)

func TestConnectCreateNaturalKeyCannotChangeOwnerOrIdentity(t *testing.T) {
	relation, id, name, _ := nullableMetadata(t, "owned")
	tenant, _ := NewColumn[nullableChild, string](relation.relation.child, "Tenant")
	key := UniqueConstraint[nullableChild]{relation.relation.child, "declared", []BoundColumn[nullableChild]{BindColumn(tenant), BindColumn(id)}}
	parent := assocParent{"a", 1, "parent"}
	values := []Assignment[nullableChild]{Set(tenant, Some("a")), Set(id, Some(int64(10)))}
	_, plan, err := connectCreatePlans(relation, key, values, []Assignment[nullableChild]{Set(name, Some("child"))}, parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 4 {
		t.Fatal("unique/foreign assignments incomplete", len(plan))
	}
	if _, _, err := connectCreatePlans(relation, key, values, []Assignment[nullableChild]{Set(id, Some(int64(11))), Set(name, Some("child"))}, parent); err == nil {
		t.Fatal("create changed unique identity")
	}
	values[0] = Set(tenant, Some("b"))
	if _, _, err := connectCreatePlans(relation, key, values, nil, parent); !errors.Is(err, ErrAssociationOwner) {
		t.Fatal("unique shared tenant override accepted", err)
	}
}
