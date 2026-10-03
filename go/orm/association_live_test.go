package orm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type associationCountingExecutor struct {
	db      Executor
	queries int
}

func (db *associationCountingExecutor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	db.queries++
	return db.db.Query(ctx, sql, args...)
}
func (db *associationCountingExecutor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return db.db.Exec(ctx, sql, args...)
}

func TestPostgresCompositeAssociationLoading(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	relation, id := associationMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schemaSQL+`.parents (tenant text NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE `+schemaSQL+`.children (tenant text NOT NULL,parent_id bigint NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); INSERT INTO `+schemaSQL+`.parents VALUES ('a',1,'A1'),('b',1,'B1'),('a',2,'A2'); INSERT INTO `+schemaSQL+`.children VALUES ('a',1,10,'first'),('a',1,11,'second'),('b',1,20,'other tenant'),('a',2,30,'other parent'),('a',99,99,'orphan')`); err != nil {
		t.Fatal(err)
	}
	// Search-path decoys must never replace the relation's qualified metadata.
	if _, err := admin.Exec(ctx, `CREATE TEMP TABLE parents (tenant text,id bigint,name text); CREATE TEMP TABLE children (tenant text,parent_id bigint,id bigint,name text); INSERT INTO children VALUES ('a',1,999,'decoy'); SET search_path TO pg_temp`); err != nil {
		t.Fatal(err)
	}
	parents := []assocParent{{"a", 1, "input first"}, {"a", 1, "duplicate slot"}, {"b", 1, "input b"}, {"a", 2, "input a2"}, {"a", 9, "missing"}}
	budget := LoadBudget{MaxParents: 5, MaxRows: 4, BatchSize: 1}
	counted := &associationCountingExecutor{db: admin}
	loaded, err := LoadMany(ctx, counted, relation, parents, Query[assocChild]{}.OrderBy(id.Desc()), budget)
	if err != nil {
		t.Fatal(err)
	}
	if counted.queries != 4 {
		t.Fatal("distinct composite keys not batched/deduplicated", counted.queries)
	}
	// Native rows are the oracle, independently regrouped by explicit tenant/id.
	native, err := admin.Query(ctx, "SELECT tenant,parent_id,id FROM "+schemaSQL+".children ORDER BY id DESC")
	if err != nil {
		t.Fatal(err)
	}
	oracle := map[struct {
		tenant string
		id     int64
	}][]int64{}
	for native.Next() {
		var tenant string
		var parentID, childID int64
		if err := native.Scan(&tenant, &parentID, &childID); err != nil {
			native.Close()
			t.Fatal(err)
		}
		key := struct {
			tenant string
			id     int64
		}{tenant, parentID}
		oracle[key] = append(oracle[key], childID)
	}
	native.Close()
	if err := native.Err(); err != nil {
		t.Fatal(err)
	}
	if len(loaded) != len(parents) {
		t.Fatal("input slots lost")
	}
	for i, item := range loaded {
		if item.Parent != parents[i] {
			t.Fatal("input parent mutated", i)
		}
		ids := []int64{}
		for _, child := range item.Children {
			ids = append(ids, child.ID)
			if child.Tenant != parents[i].Tenant || child.ParentID != parents[i].ID {
				t.Fatal("cross-tenant attachment", i)
			}
		}
		expected := oracle[struct {
			tenant string
			id     int64
		}{parents[i].Tenant, parents[i].ID}]
		if len(ids) != len(expected) || len(ids) > 0 && !reflect.DeepEqual(ids, expected) {
			t.Fatal("native association mismatch", i, ids, expected)
		}
	}
	if loaded[4].Children == nil {
		t.Fatal("missing relation must have empty slice")
	}
	loaded[0].Children[0].Name = "local edit"
	if loaded[1].Children[0].Name == "local edit" {
		t.Fatal("duplicate inputs share mutable result slice")
	}
	filtered, err := LoadMany(ctx, admin, relation, parents, Query[assocChild]{}.Where(id.Gt(10)).OrderBy(id.Asc()), LoadBudget{5, 3, 2})
	if err != nil || len(filtered[0].Children) != 1 || filtered[0].Children[0].ID != 11 {
		t.Fatal("bound child filter", err)
	}
	if result, err := LoadMany(ctx, admin, relation, parents, Query[assocChild]{}, LoadBudget{5, 3, 2}); !errors.Is(err, ErrLoadBudget) || result != nil {
		t.Fatal("excess rows silently truncated", err)
	}
	if result, err := LoadOne(ctx, admin, relation, parents, Query[assocChild]{}, budget); !errors.Is(err, ErrCardinality) || result != nil {
		t.Fatal("to-one cardinality hidden", err)
	}
	inverse := Inverse(relation)
	childInputs := []assocChild{{Tenant: "a", ParentID: 1, ID: 10}, {Tenant: "a", ParentID: 1, ID: 11}, {Tenant: "b", ParentID: 1, ID: 20}, {Tenant: "a", ParentID: 99, ID: 99}}
	owners, err := LoadOne(ctx, admin, inverse, childInputs, Query[assocParent]{}, LoadBudget{4, 2, 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(owners[0].Children) != 1 || owners[0].Children[0].Name != "A1" || len(owners[1].Children) != 1 || owners[1].Children[0].Name != "A1" || len(owners[2].Children) != 1 || owners[2].Children[0].Name != "B1" || len(owners[3].Children) != 0 {
		t.Fatal("inverse ownership/orphan mismatch")
	}
	empty, err := LoadMany(ctx, admin, relation, nil, Query[assocChild]{}, budget)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatal("empty input", err)
	}
}
