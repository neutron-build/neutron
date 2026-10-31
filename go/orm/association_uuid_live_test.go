package orm

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestPostgresUUIDCompositeAssociations(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	relation, childID := uuidAssociationMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schemaSQL+`.uuid_parents (tenant text NOT NULL,id uuid NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id));CREATE TABLE `+schemaSQL+`.uuid_children (tenant text NOT NULL,parent_id uuid NOT NULL,id bigint PRIMARY KEY,name text NOT NULL);INSERT INTO `+schemaSQL+`.uuid_parents VALUES ('a','abcdef12-3456-7890-abcd-ef1234567890','A1'),('b','abcdef12-3456-7890-abcd-ef1234567890','B1'),('a','abcdef12-3456-7890-abcd-ef1234567891','A2'),('a','00000000-0000-0000-0000-000000000000','zero');INSERT INTO `+schemaSQL+`.uuid_children VALUES ('a','abcdef12-3456-7890-abcd-ef1234567890',1,'first'),('a','abcdef12-3456-7890-abcd-ef1234567890',2,'second'),('b','abcdef12-3456-7890-abcd-ef1234567890',3,'other tenant'),('a','abcdef12-3456-7890-abcd-ef1234567891',4,'other UUID'),('a','00000000-0000-0000-0000-000000000000',5,'zero UUID'),('a','ffffffff-ffff-ffff-ffff-ffffffffffff',6,'orphan')`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE TEMP TABLE uuid_children (tenant text,parent_id uuid,id bigint,name text);INSERT INTO uuid_children VALUES ('a','abcdef12-3456-7890-abcd-ef1234567890',999,'decoy');SET search_path TO pg_temp`); err != nil {
		t.Fatal(err)
	}
	a := requireUUID(t, "abcdef12-3456-7890-abcd-ef1234567890")
	b := requireUUID(t, "abcdef12-3456-7890-abcd-ef1234567891")
	missing := requireUUID(t, "ffffffff-ffff-ffff-ffff-ffffffffffff")
	parents := []uuidAssocParent{{"a", a, "input A"}, {"a", a, "duplicate input"}, {"b", a, "input B"}, {"a", b, "other UUID"}, {"a", UUID{}, "zero UUID"}, {"b", missing, "missing"}}
	counted := &associationCountingExecutor{db: admin}
	loaded, err := LoadMany(ctx, counted, relation, parents, Query[uuidAssocChild]{}.OrderBy(childID.Desc()), LoadBudget{6, 7, 1})
	if err != nil {
		t.Fatal(err)
	}
	if counted.queries != 5 || len(loaded) != 6 {
		t.Fatal("UUID dedup/composite batches", counted.queries)
	}
	type nativeKey struct {
		tenant string
		bytes  [16]byte
	}
	oracle := map[nativeKey][]int64{}
	native, err := admin.Query(ctx, "SELECT tenant,parent_id,id FROM "+schemaSQL+".uuid_children ORDER BY id DESC")
	if err != nil {
		t.Fatal(err)
	}
	for native.Next() {
		var tenant string
		var uuid pgtype.UUID
		var id int64
		if err := native.Scan(&tenant, &uuid, &id); err != nil {
			native.Close()
			t.Fatal(err)
		}
		if !uuid.Valid {
			native.Close()
			t.Fatal("native fixture key NULL")
		}
		key := nativeKey{tenant, uuid.Bytes}
		oracle[key] = append(oracle[key], id)
	}
	native.Close()
	if err := native.Err(); err != nil {
		t.Fatal(err)
	}
	for i, item := range loaded {
		if item.Parent != parents[i] {
			t.Fatal("UUID parent position/value changed")
		}
		actual := []int64{}
		for _, child := range item.Children {
			actual = append(actual, child.ID)
			if child.Tenant != parents[i].Tenant || child.ParentID != parents[i].ID {
				t.Fatal("UUID cross-tenant attachment")
			}
		}
		expected := oracle[nativeKey{parents[i].Tenant, parents[i].ID.bytes}]
		if len(actual) != len(expected) || len(actual) > 0 && !reflect.DeepEqual(actual, expected) {
			t.Fatal("UUID relation disagrees with independent native rows", i, actual, expected)
		}
	}
	if len(loaded[4].Children) != 1 || loaded[4].Children[0].ID != 5 || len(loaded[5].Children) != 0 {
		t.Fatal("zero UUID confused with NULL/missing")
	}
	if result, err := LoadMany(ctx, admin, relation, parents, Query[uuidAssocChild]{}, LoadBudget{6, 6, 2}); !errors.Is(err, ErrLoadBudget) || result != nil {
		t.Fatal("UUID duplicate attachment budget not enforced", err)
	}
	if result, err := LoadOne(ctx, admin, relation, parents, Query[uuidAssocChild]{}, LoadBudget{6, 7, 2}); !errors.Is(err, ErrCardinality) || result != nil {
		t.Fatal("UUID cardinality concealed", err)
	}
	inputs := []uuidAssocChild{{Tenant: "a", ParentID: a, ID: 1}, {Tenant: "a", ParentID: a, ID: 2}, {Tenant: "b", ParentID: a, ID: 3}, {Tenant: "a", ParentID: UUID{}, ID: 5}, {Tenant: "a", ParentID: missing, ID: 6}}
	inverse, err := LoadOne(ctx, admin, Inverse(relation), inputs, Query[uuidAssocParent]{}, LoadBudget{5, 4, 1})
	if err != nil {
		t.Fatal(err)
	}
	expectedOwners := []string{"A1", "A1", "B1", "zero", ""}
	for i, item := range inverse {
		if expectedOwners[i] == "" {
			if len(item.Children) != 0 {
				t.Fatal("UUID orphan attached")
			}
		} else if len(item.Children) != 1 || item.Children[0].Name != expectedOwners[i] {
			t.Fatal("inverse UUID ownership", i)
		}
	}
	// Nullable mapped foreign keys are refused at relation construction; they
	// cannot silently be matched to the legitimate all-zero UUID.
	type nullableChild struct {
		ParentID *UUID `db:"parent_id,nullable"`
	}
	optional, err := NewTable[nullableChild](schema, "uuid_children")
	if err != nil {
		t.Fatal(err)
	}
	optionalKey, err := NewColumn[nullableChild, *UUID](optional, "ParentID")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRelation(optional, optional, Join(optionalKey, optionalKey)); err == nil {
		t.Fatal("nullable UUID relation admitted")
	}
}
