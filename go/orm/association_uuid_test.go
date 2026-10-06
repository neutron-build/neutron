package orm

import (
	"reflect"
	"testing"
)

type uuidAssocParent struct {
	Tenant string `db:"tenant"`
	ID     UUID   `db:"id"`
	Name   string `db:"name"`
}
type uuidAssocChild struct {
	Tenant   string `db:"tenant"`
	ParentID UUID   `db:"parent_id"`
	ID       int64  `db:"id"`
	Name     string `db:"name"`
}

func uuidAssociationMetadata(t *testing.T, schema string) (Relation[uuidAssocParent, uuidAssocChild], Column[uuidAssocChild, int64]) {
	t.Helper()
	parent, err := NewTable[uuidAssocParent](schema, "uuid_parents")
	if err != nil {
		t.Fatal(err)
	}
	child, err := NewTable[uuidAssocChild](schema, "uuid_children")
	if err != nil {
		t.Fatal(err)
	}
	pt, err := NewColumn[uuidAssocParent, string](parent, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	pi, err := NewColumn[uuidAssocParent, UUID](parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	ct, err := NewColumn[uuidAssocChild, string](child, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := NewColumn[uuidAssocChild, UUID](child, "ParentID")
	if err != nil {
		t.Fatal(err)
	}
	ci, err := NewColumn[uuidAssocChild, int64](child, "ID")
	if err != nil {
		t.Fatal(err)
	}
	relation, err := NewRelation(parent, child, Join(pt, ct), Join(pi, cp))
	if err != nil {
		t.Fatal(err)
	}
	return relation, ci
}

func requireUUID(t *testing.T, text string) UUID {
	t.Helper()
	value, err := ParseUUID(text)
	if err != nil {
		t.Fatal("test UUID invalid")
	}
	return value
}

func TestAssociationUUIDExactIdentityAndNullPolicy(t *testing.T) {
	relation, _ := uuidAssociationMetadata(t, "owned")
	id := requireUUID(t, "abcdef12-3456-7890-abcd-ef1234567890")
	upper := requireUUID(t, "ABCDEF12-3456-7890-ABCD-EF1234567890")
	distinct := requireUUID(t, "abcdef12-3456-7890-abcd-ef1234567891")
	fields := []fieldInfo{relation.parts[0].parentField, relation.parts[1].parentField}
	key := func(tenant string, value UUID) string {
		return relationKey(reflect.ValueOf(uuidAssocParent{Tenant: tenant, ID: value}), fields)
	}
	if key("a", id) != key("a", upper) {
		t.Fatal("UUID textual case changed binary identity")
	}
	if key("a", id) == key("a", distinct) || key("a", id) == key("b", id) || key("a", UUID{}) == key("a", distinct) {
		t.Fatal("distinct composite UUID identity collapsed")
	}
	// Inspect exact bytes after the tenant/type prefix: no formatted strings are
	// used as an identity oracle, including the legitimate zero UUID.
	uuidOnly := []fieldInfo{relation.parts[1].parentField}
	encoded := relationKey(reflect.ValueOf(uuidAssocParent{ID: id}), uuidOnly)
	if len(encoded) != 17 || encoded[1:] != string(id.bytes[:]) {
		t.Fatal("UUID identity is not the exact128bit value")
	}
	other, _ := uuidAssociationMetadata(t, "owned")
	if _, err := NewRelation(relation.parent, relation.child, other.parts...); err == nil {
		t.Fatal("UUID key escaped qualified binding")
	}
	inverse := Inverse(relation)
	if inverse.parts[1].parentField.typ != reflect.TypeOf(UUID{}) || inverse.parts[1].parent != relation.child.info {
		t.Fatal("inverse UUID ownership")
	}
	type nullable struct {
		ID *UUID `db:"id,nullable"`
	}
	nullableTable, err := NewTable[nullable]("owned", "nullable_uuid")
	if err != nil {
		t.Fatal(err)
	}
	column, err := NewColumn[nullable, *UUID](nullableTable, "ID")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRelation(nullableTable, nullableTable, Join(column, column)); err == nil {
		t.Fatal("nullable UUID key accepted (SQL NULL must not equal nil UUID)")
	}
	if associationKeyType(reflect.TypeOf((*UUID)(nil))) {
		t.Fatal("nullable UUID classified as exact key")
	}
	type named UUID
	if associationKeyType(reflect.TypeOf(named{})) {
		t.Fatal("uncertified named UUID admitted")
	}
}
