package orm

import (
	"errors"
	"reflect"
	"testing"
)

type nullableChild struct {
	Tenant   string `db:"tenant"`
	ParentID *int64 `db:"parent_id,nullable"`
	ID       int64  `db:"id"`
	Name     string `db:"name"`
}

func nullableMetadata(t *testing.T, schema string) (NullableRelation[assocParent, nullableChild], Column[nullableChild, int64], Column[nullableChild, string], Column[nullableChild, *int64]) {
	t.Helper()
	parent, err := NewTable[assocParent](schema, "parents")
	if err != nil {
		t.Fatal(err)
	}
	child, err := NewTable[nullableChild](schema, "nullable_children")
	if err != nil {
		t.Fatal(err)
	}
	pt, _ := NewColumn[assocParent, string](parent, "Tenant")
	pi, _ := NewColumn[assocParent, int64](parent, "ID")
	ct, _ := NewColumn[nullableChild, string](child, "Tenant")
	cp, _ := NewColumn[nullableChild, *int64](child, "ParentID")
	ci, _ := NewColumn[nullableChild, int64](child, "ID")
	cn, _ := NewColumn[nullableChild, string](child, "Name")
	relation, err := NewNullableRelation(parent, child, JoinRequired(pt, ct), JoinNullable(pi, cp))
	if err != nil {
		t.Fatal(err)
	}
	return relation, ci, cn, cp
}
func TestNullableRelationCompositeIdentityAndOwnership(t *testing.T) {
	relation, _, _, _ := nullableMetadata(t, "owned")
	id := int64(1)
	child := nullableChild{"a", &id, 10, "child"}
	parent := assocParent{"a", 1, "parent"}
	pfields, cfields := []fieldInfo{}, []fieldInfo{}
	for _, part := range relation.relation.parts {
		pfields = append(pfields, part.parentField)
		cfields = append(cfields, part.childField)
	}
	if relationKey(reflect.ValueOf(parent), pfields) != relationKey(reflect.ValueOf(child), cfields) {
		t.Fatal("nullable FK identity not exact parent identity")
	}
	if err := nullableOwnership(relation, parent, child, false); err != nil {
		t.Fatal(err)
	}
	other := int64(2)
	child.ParentID = &other
	if err := nullableOwnership(relation, parent, child, false); !errors.Is(err, ErrAssociationOwner) {
		t.Fatal("connect implicitly reparented", err)
	}
	if err := nullableOwnership(relation, parent, child, true); err != nil {
		t.Fatal("explicit same-tenant reparent refused", err)
	}
	child.Tenant = "b"
	if err := nullableOwnership(relation, parent, child, true); !errors.Is(err, ErrAssociationOwner) {
		t.Fatal("reparent changed shared tenant", err)
	}
	child.Tenant = "a"
	child.ParentID = nil
	if err := nullableOwnership(relation, parent, child, false); err != nil {
		t.Fatal("unowned child connect refused", err)
	}
}
