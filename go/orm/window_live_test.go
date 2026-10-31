package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestPostgresTypedWindowsAndFrames(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, tenant, id, _, version, _ := policyMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+table.info.sqlName()+" (tenant text NOT NULL,id bigint NOT NULL,value text NOT NULL,version bigint NOT NULL,deleted timestamptz); INSERT INTO "+table.info.sqlName()+" VALUES ('a',1,'x',10,NULL),('a',2,'y',20,NULL),('a',3,'z',30,NULL),('b',4,'other',100,NULL)"); err != nil {
		t.Fatal(err)
	}
	query := Query[policyModel]{}.OrderBy(id.Asc())
	ranks, err := SelectWindowPair(ctx, pool, id, RowNumber(table, []BoundColumn[policyModel]{BindColumn(tenant)}, id.Asc()), query)
	if err != nil || !reflect.DeepEqual(ranks, []Pair[int64, int64]{{1, 1}, {2, 2}, {3, 3}, {4, 1}}) {
		t.Fatal(ranks, err)
	}
	window := Over(SumInt64(version), []BoundColumn[policyModel]{BindColumn(tenant)}, id.Asc()).RowsBetween(Preceding(1), CurrentRow())
	actual, err := SelectWindowPair(ctx, pool, id, window, query)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := admin.Query(ctx, "SELECT id,(sum(version) OVER (PARTITION BY tenant ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW))::text FROM "+table.info.sqlName()+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for rows.Next() {
		var id int64
		var sum string
		if err := rows.Scan(&id, &sum); err != nil {
			t.Fatal(err)
		}
		if i >= len(actual) || actual[i].First != id || !actual[i].Second.Valid || actual[i].Second.Value.String() != sum {
			t.Fatal(actual, id, sum)
		}
		i++
	}
	rows.Close()
	if rows.Err() != nil || i != len(actual) {
		t.Fatal(i, len(actual), rows.Err())
	}
	// A frame containing only following rows has an empty last aggregate.
	next := Over(SumInt64(version), nil, id.Asc()).RowsBetween(Following(1), Following(1))
	values, err := SelectWindowPair(ctx, pool, id, next, query)
	if err != nil || len(values) != 4 || values[3].Second.Valid {
		t.Fatal(values, err)
	}
}
