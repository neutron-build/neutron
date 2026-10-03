package orm

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresTypedCTESetAndBoundedRecursiveQueries(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	relation, id, _ := selfJoinMetadata(t, schema)
	manager, _ := NewColumn[selfModel, int64](relation.parent, "Manager")
	if _, err := admin.Exec(ctx, "CREATE TABLE "+relation.parent.info.sqlName()+" (id bigint PRIMARY KEY,manager bigint NOT NULL,name text NOT NULL); INSERT INTO "+relation.parent.info.sqlName()+" VALUES (1,0,'root'),(2,1,'child'),(3,2,'grandchild'),(4,4,'cycle')"); err != nil {
		t.Fatal(err)
	}
	left, _ := NewModelQuery(relation.parent, Query[selfModel]{}.Where(id.In(1, 2)))
	right, _ := NewModelQuery(relation.parent, Query[selfModel]{}.Where(id.In(2, 3)))
	for _, scenario := range []struct {
		build func(ModelQuery[selfModel], ModelQuery[selfModel]) (ModelQuery[selfModel], error)
		sql   string
	}{
		{Union[selfModel], "UNION"}, {UnionAll[selfModel], "UNION ALL"}, {Intersect[selfModel], "INTERSECT"}, {Except[selfModel], "EXCEPT"},
	} {
		plan, err := scenario.build(left, right)
		if err != nil {
			t.Fatal(err)
		}
		derived, err := NewCTE("selected_rows", plan)
		if err != nil {
			t.Fatal(err)
		}
		column, _ := DerivedColumn[selfModel, int64](derived, "ID")
		actual, err := SelectDerivedColumn(ctx, pool, derived, column, Query[selfModel]{}.Where(column.Gt(0)).OrderBy(column.Asc()))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := admin.Query(ctx, "SELECT id FROM ((SELECT id,manager,name FROM "+relation.parent.info.sqlName()+" WHERE id IN (1,2)) "+scenario.sql+" (SELECT id,manager,name FROM "+relation.parent.info.sqlName()+" WHERE id IN (2,3))) AS native_result WHERE id>0 ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		expected := []int64{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			expected = append(expected, id)
		}
		rows.Close()
		if rows.Err() != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatal(actual, expected, rows.Err())
		}
	}
	anchor, _ := NewModelQuery(relation.parent, Query[selfModel]{}.Where(manager.Eq(0)))
	recursive, err := NewRecursiveCTE("walk", anchor, relation, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	field, _ := DerivedColumn[selfModel, int64](recursive, "ID")
	actual, err := SelectDerivedColumn(ctx, pool, recursive, field, Query[selfModel]{}.OrderBy(field.Asc()))
	if err != nil || !reflect.DeepEqual(actual, []int64{1, 2, 3}) {
		t.Fatal(actual, err)
	}
	rows, err := admin.Query(ctx, "WITH RECURSIVE native_walk(id,manager,name,depth) AS (SELECT id,manager,name,0 FROM "+relation.parent.info.sqlName()+" WHERE manager=0 UNION ALL SELECT c.id,c.manager,c.name,p.depth+1 FROM "+relation.parent.info.sqlName()+" AS c JOIN native_walk AS p ON p.id=c.manager WHERE p.depth<2) SELECT id FROM native_walk ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	expected := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, id)
	}
	rows.Close()
	if rows.Err() != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatal(actual, expected, rows.Err())
	}
	cycleAnchor, _ := NewModelQuery(relation.parent, Query[selfModel]{}.Where(id.Eq(4)))
	cycle, err := NewRecursiveCTE("cycle", cycleAnchor, relation, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, _ := DerivedColumn[selfModel, int64](cycle, "ID")
	cycleRows, err := SelectDerivedColumn(ctx, pool, cycle, cycleID, Query[selfModel]{})
	if err != nil || !reflect.DeepEqual(cycleRows, []int64{4, 4, 4}) {
		t.Fatal(cycleRows, err)
	}
	budgeted, _ := NewRecursiveCTE("bounded_cycle", cycleAnchor, relation, 2, 2)
	if rows, err := SelectDerived(ctx, pool, budgeted, Query[selfModel]{}); rows != nil || !errors.Is(err, ErrRecursiveBudget) {
		t.Fatal("recursive excess rows exposed", rows, err)
	}
	// DISTINCT is model-projected, retaining original struct shape and types.
	duplicates, _ := UnionAll(left, left)
	distinctCTE, _ := NewCTE("distinct_rows", duplicates)
	distinctID, _ := DerivedColumn[selfModel, int64](distinctCTE, "ID")
	distinct, err := SelectDerived(ctx, pool, distinctCTE, Query[selfModel]{}.Distinct().OrderBy(distinctID.Asc()))
	if err != nil || len(distinct) != 2 {
		t.Fatal(distinct, err)
	}
}
