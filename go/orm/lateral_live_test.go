package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestPostgresCompositeLateralPerParentPaging(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	relation, id := associationMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schemaSQL+`.parents (tenant text NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE `+schemaSQL+`.children (tenant text NOT NULL,parent_id bigint NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); INSERT INTO `+schemaSQL+`.parents VALUES ('a',1,'A1'),('b',1,'B1'),('a',2,'A2'),('a',3,'missing'); INSERT INTO `+schemaSQL+`.children VALUES ('a',1,10,'match'),('a',1,11,'match'),('a',1,12,'excluded'),('b',1,20,'match'),('a',2,30,'match')`); err != nil {
		t.Fatal(err)
	}
	parentName, _ := NewColumn[assocParent, string](relation.parent, "Name")
	childName, _ := NewColumn[assocChild, string](relation.child, "Name")
	for _, offset := range []int{0, 1} {
		scope, err := NewLateralLeftJoin(relation, "p", "c", Query[assocChild]{}.Where(childName.Eq("match")).OrderBy(id.Desc()).Limit(1).Offset(offset))
		if err != nil {
			t.Fatal(err)
		}
		first, _ := JoinParentField(scope, parentName)
		second, _ := LeftChildField(scope, id)
		actual, err := SelectJoinedPair(ctx, pool, first, second, scope.Query().OrderParent(parentName.Asc()))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := admin.Query(ctx, "SELECT p.name,c.id FROM "+schemaSQL+`.parents AS p LEFT JOIN LATERAL (SELECT id FROM `+schemaSQL+`.children AS child WHERE child.tenant=p.tenant AND child.parent_id=p.id AND child.name='match' ORDER BY child.id DESC LIMIT 1 OFFSET $1) AS c ON TRUE ORDER BY p.name`, offset)
		if err != nil {
			t.Fatal(err)
		}
		expected := []Pair[string, Nullable[int64]]{}
		for rows.Next() {
			var parent string
			var child *int64
			if err := rows.Scan(&parent, &child); err != nil {
				t.Fatal(err)
			}
			value := Nullable[int64]{}
			if child != nil {
				value = Nullable[int64]{Valid: true, Value: *child}
			}
			expected = append(expected, Pair[string, Nullable[int64]]{First: parent, Second: value})
		}
		rows.Close()
		if rows.Err() != nil || !reflect.DeepEqual(actual, expected) || len(actual) != 4 {
			t.Fatal(actual, expected, rows.Err())
		}
	}
}
