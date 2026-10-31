package orm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestJoinedQualifiedBoundComposition(t *testing.T) {
	relation, childID := associationMetadata(t, "owned")
	parentID, err := NewColumn[assocParent, int64](relation.parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	parentTenant, err := NewColumn[assocParent, string](relation.parent, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	scope, err := NewInnerJoin(relation)
	if err != nil {
		t.Fatal(err)
	}
	first, err := JoinParentField(scope, parentID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := InnerChildField(scope, childID)
	if err != nil {
		t.Fatal(err)
	}
	base := scope.Query().WhereParent(parentTenant.Eq("a' OR true--")).OrderParent(parentID.Asc())
	query := base.WhereChild(childID.Gt(0)).OrderChild(childID.Desc()).Limit(0).Offset(0)
	sql, args, err := joinedSQL(query, []joinedProjection{{first.info, first.field, first.outer, first.child}, {second.info, second.field, second.outer, second.child}})
	if err != nil {
		t.Fatal(err)
	}
	expected := `SELECT "owned"."parents"."id", "owned"."children"."id" FROM "owned"."parents" INNER JOIN "owned"."children" ON ("owned"."parents"."tenant" = "owned"."children"."tenant" AND "owned"."parents"."id" = "owned"."children"."parent_id") WHERE ("owned"."parents"."tenant" = $1) AND ("owned"."children"."id" > $2) ORDER BY "owned"."parents"."id" ASC, "owned"."children"."id" DESC LIMIT $3 OFFSET $4`
	if sql != expected || !reflect.DeepEqual(args, []any{"a' OR true--", int64(0), 0, 0}) {
		t.Fatal("qualified bound join differs", sql, args)
	}
	if len(base.filters) != 1 || len(base.order) != 1 || base.limited || base.offsetSet {
		t.Fatal("join builder mutated prior value")
	}
	if strings.Contains(sql, "OR true") {
		t.Fatal("bound value interpolated")
	}
}

func TestJoinedLeftNullableProjectionPolicy(t *testing.T) {
	relation, id := associationMetadata(t, "owned")
	scope, err := NewLeftJoin(relation)
	if err != nil {
		t.Fatal(err)
	}
	field, err := LeftChildField(scope, id)
	if err != nil {
		t.Fatal(err)
	}
	var result Nullable[int64]
	if field.destination(&result, true) != nil || result.Valid || result.Value != 0 {
		t.Fatal("SQL NULL substituted zero")
	}
	destination := field.destination(&result, false)
	if !result.Valid {
		t.Fatal("non-NULL not valid")
	}
	reflect.ValueOf(destination).Elem().SetInt(0)
	if !result.Valid || result.Value != 0 {
		t.Fatal("actual zero lost")
	}
	// Non-NULL JSON null keeps its explicit document even inside an outer field.
	type jsonChild struct {
		ID       int64 `db:"id"`
		Document *JSON `db:"document,nullable"`
	}
	child, err := NewTable[jsonChild]("owned", "json_child")
	if err != nil {
		t.Fatal(err)
	}
	parentID, err := NewColumn[assocParent, int64](relation.parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	childID, err := NewColumn[jsonChild, int64](child, "ID")
	if err != nil {
		t.Fatal(err)
	}
	document, err := NewColumn[jsonChild, *JSON](child, "Document")
	if err != nil {
		t.Fatal(err)
	}
	jsonRelation, err := NewRelation(relation.parent, child, Join(parentID, childID))
	if err != nil {
		t.Fatal(err)
	}
	jsonScope, err := NewLeftJoin(jsonRelation)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := LeftChildField(jsonScope, document)
	if err != nil {
		t.Fatal(err)
	}
	var doc Nullable[*JSON]
	scanner := outer.destination(&doc, false).(pgtype.BytesScanner)
	if err := scanner.ScanBytes([]byte("null")); err != nil || !doc.Valid || doc.Value == nil || !doc.Value.IsNull() {
		t.Fatal("outer JSON null collapsed", err)
	}
	if outer.destination(&doc, true) != nil || doc.Valid || doc.Value != nil {
		t.Fatal("outer SQLNULL not reset")
	}
	// Compiler revalidates private AST invariants even for internal construction.
	if _, _, err := joinedSQL(scope.Query(), []joinedProjection{{field.info, field.field, false, field.child}}); err == nil {
		t.Fatal("nonnullable left child projection accepted")
	}
}

func TestJoinedOwnershipAndCardinalityPreflight(t *testing.T) {
	relation, id := associationMetadata(t, "owned")
	scope, err := NewInnerJoin(relation)
	if err != nil {
		t.Fatal(err)
	}
	field, err := InnerChildField(scope, id)
	if err != nil {
		t.Fatal(err)
	}
	another, err := NewInnerJoin(relation)
	if err != nil {
		t.Fatal(err)
	}
	db := &associationRefusingExecutor{}
	if _, err := SelectJoinedColumn(context.Background(), db, field, another.Query()); err == nil {
		t.Fatal("projection escaped unique join binding")
	}
	for _, query := range []JoinQuery[assocParent, assocChild]{scope.Query().Limit(0), scope.Query().Offset(0)} {
		if _, err := SelectJoinedOne(context.Background(), db, field, query); err == nil {
			t.Fatal("explicit zero pagination hid cardinality")
		}
		if _, err := SelectJoinedPairOne(context.Background(), db, field, field, query); err == nil {
			t.Fatal("pair zero pagination hid cardinality")
		}
	}
	other, otherID := associationMetadata(t, "owned")
	if _, err := InnerChildField(scope, otherID); err == nil {
		t.Fatal("same physical name foreign metadata accepted")
	}
	if _, _, err := joinedSQL(scope.Query().WhereChild(otherID.Eq(1)), []joinedProjection{{field.info, field.field, false, field.child}}); err == nil {
		t.Fatal("foreign predicate accepted")
	}
	if _, _, err := joinedSQL(scope.Query().OrderChild(otherID.Asc()), []joinedProjection{{field.info, field.field, false, field.child}}); err == nil {
		t.Fatal("foreign order accepted")
	}
	_ = other
	if _, _, err := joinedSQL(scope.Query().Limit(-1), []joinedProjection{{field.info, field.field, false, field.child}}); err == nil {
		t.Fatal("negative limit accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := SelectJoinedColumn(ctx, db, field, scope.Query()); !errors.Is(err, context.Canceled) {
		t.Fatal("context ignored", err)
	}
	var nilScope *InnerJoin[assocParent, assocChild]
	parentID, err := NewColumn[assocParent, int64](relation.parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := JoinParentField(nilScope, parentID); err == nil {
		t.Fatal("nil join scope accepted")
	}
	if db.called {
		t.Fatal("invalid request reached executor")
	}
}

func TestJoinedRejectsSelfPhysicalTableAndWrongOrderingSlot(t *testing.T) {
	parent, err := NewTable[assocParent]("owned", "same")
	if err != nil {
		t.Fatal(err)
	}
	copyTable, err := NewTable[assocParent]("owned", "same")
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewColumn[assocParent, int64](parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewColumn[assocParent, int64](copyTable, "ID")
	if err != nil {
		t.Fatal(err)
	}
	relation, err := NewRelation(parent, copyTable, Join(p, c))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewInnerJoin(relation); err == nil {
		t.Fatal("self physical table accepted without aliases")
	}
	child, err := NewTable[assocParent]("owned", "different")
	if err != nil {
		t.Fatal(err)
	}
	childID, err := NewColumn[assocParent, int64](child, "ID")
	if err != nil {
		t.Fatal(err)
	}
	relation, err = NewRelation(parent, child, Join(p, childID))
	if err != nil {
		t.Fatal(err)
	}
	scope, err := NewInnerJoin(relation)
	if err != nil {
		t.Fatal(err)
	}
	field, err := JoinParentField(scope, p)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := joinedSQL(scope.Query().OrderChild(p.Asc()), []joinedProjection{{field.info, field.field, false, field.child}}); err == nil {
		t.Fatal("same-model order escaped child slot")
	}
}
