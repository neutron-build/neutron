package orm

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresCompositeManyToManyThroughBudgets(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	through, linkID := throughMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schemaSQL+`.parents (tenant text NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE `+schemaSQL+`.children (tenant text NOT NULL,parent_id bigint NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE `+schemaSQL+`.links (tenant text NOT NULL,parent_id bigint NOT NULL,child_id bigint NOT NULL,id bigint NOT NULL,PRIMARY KEY(tenant,id)); INSERT INTO `+schemaSQL+`.parents VALUES ('a',1,'A1'),('a',2,'A2'),('b',1,'B1'); INSERT INTO `+schemaSQL+`.children VALUES ('a',0,10,'A10'),('a',0,11,'A11'),('b',0,10,'B10'); INSERT INTO `+schemaSQL+`.links VALUES ('a',1,10,1),('a',1,11,2),('a',2,99,3),('b',1,10,1)`); err != nil {
		t.Fatal(err)
	}
	parents := []assocParent{{"a", 1, "first"}, {"a", 1, "duplicate"}, {"b", 1, "foreign"}, {"a", 2, "missing target"}, {"a", 9, "missing links"}}
	counted := &associationCountingExecutor{db: admin}
	budget := ThroughBudget{MaxParents: 5, MaxLinks: 6, MaxRows: 5, BatchSize: 10}
	loaded, err := LoadThrough(ctx, counted, through, parents, Query[throughLink]{}.OrderBy(linkID.Asc()), Query[assocChild]{}, budget)
	if err != nil {
		t.Fatal(err)
	}
	if counted.queries != 2 {
		t.Fatal("through eager query budget", counted.queries)
	}
	rows, err := admin.Query(ctx, "SELECT l.tenant,l.parent_id,c.tenant,c.parent_id,c.id,c.name FROM "+schemaSQL+".links l JOIN "+schemaSQL+".children c ON c.tenant=l.tenant AND c.id=l.child_id ORDER BY l.id")
	if err != nil {
		t.Fatal(err)
	}
	oracle := map[struct {
		tenant string
		id     int64
	}][]assocChild{}
	for rows.Next() {
		var tenant string
		var parentID int64
		var child assocChild
		if err := rows.Scan(&tenant, &parentID, &child.Tenant, &child.ParentID, &child.ID, &child.Name); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		key := struct {
			tenant string
			id     int64
		}{tenant, parentID}
		oracle[key] = append(oracle[key], child)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for i, parent := range parents {
		expected := oracle[struct {
			tenant string
			id     int64
		}{parent.Tenant, parent.ID}]
		if loaded[i].Parent != parent || len(loaded[i].Children) != len(expected) || (len(expected) > 0 && !reflect.DeepEqual(loaded[i].Children, expected)) {
			t.Fatal("native through composite oracle", i, loaded[i], expected)
		}
	}
	budget.MaxRows = 4
	if partial, err := LoadThrough(ctx, admin, through, parents, Query[throughLink]{}, Query[assocChild]{}, budget); partial != nil || !errors.Is(err, ErrLoadBudget) {
		t.Fatal("expanded duplicate row budget not enforced", err)
	}
}
