package orm

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"reflect"
	"testing"
)

type assocParent struct {
	Tenant string `db:"tenant"`
	ID     int64  `db:"id"`
	Name   string `db:"name"`
}
type assocChild struct {
	Tenant   string `db:"tenant"`
	ParentID int64  `db:"parent_id"`
	ID       int64  `db:"id"`
	Name     string `db:"name"`
}

func associationMetadata(t *testing.T, schema string) (Relation[assocParent, assocChild], Column[assocChild, int64]) {
	t.Helper()
	parent, err := NewTable[assocParent](schema, "parents")
	if err != nil {
		t.Fatal(err)
	}
	child, err := NewTable[assocChild](schema, "children")
	if err != nil {
		t.Fatal(err)
	}
	pt, err := NewColumn[assocParent, string](parent, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	pi, err := NewColumn[assocParent, int64](parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	ct, err := NewColumn[assocChild, string](child, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := NewColumn[assocChild, int64](child, "ParentID")
	if err != nil {
		t.Fatal(err)
	}
	ci, err := NewColumn[assocChild, int64](child, "ID")
	if err != nil {
		t.Fatal(err)
	}
	relation, err := NewRelation(parent, child, Join(pt, ct), Join(pi, cp))
	if err != nil {
		t.Fatal(err)
	}
	return relation, ci
}

func TestAssociationMetadataIdentity(t *testing.T) {
	relation, _ := associationMetadata(t, "owned")
	other, _ := associationMetadata(t, "owned")
	if _, err := NewRelation(relation.parent, relation.child, other.parts...); err == nil {
		t.Fatal("equally named but unbound relation accepted")
	}
	if _, err := NewRelation(relation.parent, relation.child, relation.parts[0], relation.parts[0]); err == nil {
		t.Fatal("duplicate key accepted")
	}
	inv := Inverse(relation)
	if inv.parent.info != relation.child.info || inv.child.info != relation.parent.info || inv.parts[1].parentField.name != "parent_id" || inv.parts[1].childField.name != "id" {
		t.Fatal("inverse changed qualified ownership")
	}
	type nullable struct {
		ID *int64 `db:"id,nullable"`
	}
	table, err := NewTable[nullable]("owned", "nullable")
	if err != nil {
		t.Fatal(err)
	}
	column, err := NewColumn[nullable, *int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRelation(table, table, Join(column, column)); err == nil {
		t.Fatal("nullable key accepted")
	}
	type floating struct {
		ID float64 `db:"id"`
	}
	ft, err := NewTable[floating]("owned", "float")
	if err != nil {
		t.Fatal(err)
	}
	fc, err := NewColumn[floating, float64](ft, "ID")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRelation(ft, ft, Join(fc, fc)); err == nil {
		t.Fatal("floating-point key accepted")
	}
}

func TestAssociationTupleIdentity(t *testing.T) {
	type tuple struct {
		A string
		B string
		C int64
		D bool
	}
	typ := reflect.TypeOf(tuple{})
	fields := []fieldInfo{}
	for i := 0; i < typ.NumField(); i++ {
		fields = append(fields, fieldInfo{index: i, typ: typ.Field(i).Type})
	}
	inputs := []tuple{{"a", "bc", 0, false}, {"ab", "c", 0, false}, {"a\x00", "bc", 0, false}, {"a", "bc", -1, false}, {"a", "bc", 0, true}}
	seen := map[string]bool{}
	for _, input := range inputs {
		key := relationKey(reflect.ValueOf(input), fields)
		if seen[key] {
			t.Fatal("distinct tuple collapsed")
		}
		seen[key] = true
	}
	if relationKey(reflect.ValueOf(inputs[0]), fields) != relationKey(reflect.ValueOf(inputs[0]), fields) {
		t.Fatal("unstable identity")
	}
}

func TestAssociationRefusesInvalidLoadBeforeSQL(t *testing.T) {
	relation, id := associationMetadata(t, "owned")
	db := &associationRefusingExecutor{}
	parents := []assocParent{{Tenant: "t", ID: 1}}
	budget := LoadBudget{MaxParents: 1, MaxRows: 1, BatchSize: 1}
	if _, err := LoadMany(context.Background(), db, relation, parents, Query[assocChild]{}.Limit(1), budget); err == nil {
		t.Fatal("global limit accepted")
	}
	if _, err := LoadMany(context.Background(), db, relation, parents, Query[assocChild]{}.Offset(1), budget); err == nil {
		t.Fatal("global offset accepted")
	}
	if _, err := LoadMany(context.Background(), db, relation, append(parents, parents...), Query[assocChild]{}, budget); !errors.Is(err, ErrLoadBudget) {
		t.Fatal("input budget", err)
	}
	if _, err := LoadMany(context.Background(), db, relation, nil, Query[assocChild]{}.OrderBy(id.Asc()), LoadBudget{}); err == nil {
		t.Fatal("missing budget accepted")
	}
	other, _ := associationMetadata(t, "owned")
	if _, err := LoadMany(context.Background(), db, relation, nil, Query[assocChild]{}.Where(Predicate[assocChild]{&expression{kind: "=", info: other.child.info, field: other.parts[0].childField, value: "t"}}), budget); err == nil {
		t.Fatal("invalid query hidden by empty input")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := LoadMany(ctx, db, relation, nil, Query[assocChild]{}, budget); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	if db.called {
		t.Fatal("invalid request reached PostgreSQL")
	}
}

type associationRefusingExecutor struct{ called bool }

func (db *associationRefusingExecutor) Query(context.Context, string, ...any) (pgx.Rows, error) {
	db.called = true
	return nil, errors.New("unexpected query")
}
func (db *associationRefusingExecutor) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	db.called = true
	return pgconn.CommandTag{}, errors.New("unexpected exec")
}

type associationFixtureRows struct {
	fixtureRows
	rows []assocChild
	next int
}

func (r *associationFixtureRows) Next() bool {
	if r.closed || r.next >= len(r.rows) {
		return false
	}
	r.next++
	return true
}
func (r *associationFixtureRows) Scan(dest ...any) error {
	value := reflect.ValueOf(r.rows[r.next-1])
	for i, target := range dest {
		reflect.ValueOf(target).Elem().Set(value.Field(i))
	}
	return nil
}

type associationFixtureExecutor struct {
	associationRefusingExecutor
	rows []assocChild
}

func (db *associationFixtureExecutor) Query(context.Context, string, ...any) (pgx.Rows, error) {
	db.called = true
	return &associationFixtureRows{rows: db.rows}, nil
}

func TestAssociationExpandedAttachmentBudget(t *testing.T) {
	relation, _ := associationMetadata(t, "owned")
	parents := make([]assocParent, 1000)
	for i := range parents {
		parents[i] = assocParent{Tenant: "a", ID: 1}
	}
	children := []assocChild{{Tenant: "a", ParentID: 1, ID: 10}, {Tenant: "a", ParentID: 1, ID: 11}}
	db := &associationFixtureExecutor{rows: children}
	if result, err := LoadMany(context.Background(), db, relation, parents, Query[assocChild]{}, LoadBudget{1000, 1999, 1}); !errors.Is(err, ErrLoadBudget) || result != nil {
		t.Fatal("expanded fanout budget exceeded without refusal", err)
	}
	result, err := LoadMany(context.Background(), db, relation, parents, Query[assocChild]{}, LoadBudget{1000, 2000, 1})
	if err != nil || len(result) != 1000 {
		t.Fatal("exact expanded budget refused", err)
	}
	total := 0
	for _, item := range result {
		total += len(item.Children)
	}
	if total != 2000 {
		t.Fatal("expanded slot count", total)
	}
}
