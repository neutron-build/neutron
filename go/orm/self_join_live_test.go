package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestPostgresAliasedSelfJoinRoleProjections(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	relation, id, name := selfJoinMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+relation.parent.info.sqlName()+" (id bigint PRIMARY KEY,manager bigint NOT NULL,name text NOT NULL); INSERT INTO "+relation.parent.info.sqlName()+" VALUES (1,0,'manager'),(2,1,'report'),(3,0,'solo')"); err != nil {
		t.Fatal(err)
	}
	scope, err := NewAliasedLeftJoin(relation, "p", "c")
	if err != nil {
		t.Fatal(err)
	}
	first, _ := JoinParentField(scope, id)
	second, _ := LeftChildField(scope, name)
	actual, err := SelectJoinedPair(ctx, pool, first, second, scope.Query().OrderParent(id.Asc()).OrderChild(name.Asc().NullsLast()))
	if err != nil {
		t.Fatal(err)
	}
	oracle := []Pair[int64, Nullable[string]]{}
	rows, err := admin.Query(ctx, "SELECT p.id,c.name FROM "+relation.parent.info.sqlName()+" AS p LEFT JOIN "+relation.child.info.sqlName()+" AS c ON p.id=c.manager ORDER BY p.id,c.name NULLS LAST")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var p int64
		var child *string
		if err := rows.Scan(&p, &child); err != nil {
			t.Fatal(err)
		}
		v := Nullable[string]{}
		if child != nil {
			v = Nullable[string]{Valid: true, Value: *child}
		}
		oracle = append(oracle, Pair[int64, Nullable[string]]{First: p, Second: v})
	}
	rows.Close()
	if rows.Err() != nil || !reflect.DeepEqual(actual, oracle) || len(actual) != 3 || actual[0].Second.Value != "report" || actual[1].Second.Valid {
		t.Fatal(actual, oracle, rows.Err())
	}
	inner, _ := NewAliasedInnerJoin(relation, "manager", "report")
	parentName, _ := JoinParentField(inner, name)
	childName, _ := InnerChildField(inner, name)
	pair, err := SelectJoinedPairOne(ctx, pool, parentName, childName, inner.Query().WhereParent(id.Eq(1)).WhereChild(name.Eq("report")))
	if err != nil || pair.First != "manager" || pair.Second != "report" {
		t.Fatal(pair, err)
	}
}
