package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestPostgresKeysetTiesAndNullOrdering(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	type nullableRecord struct {
		ID    int64   `db:"id"`
		Value *string `db:"value,nullable"`
	}
	table, _ := NewTable[nullableRecord](schema, "keyset_records")
	id, _ := NewColumn[nullableRecord, int64](table, "ID")
	value, _ := NewColumn[nullableRecord, *string](table, "Value")
	if _, err := admin.Exec(ctx, "CREATE TABLE "+table.info.sqlName()+" (id bigint PRIMARY KEY,value text); INSERT INTO "+table.info.sqlName()+" VALUES (1,NULL),(2,'same'),(3,'same'),(4,'a'),(5,NULL),(6,'z')"); err != nil {
		t.Fatal(err)
	}
	for _, order := range []Order[nullableRecord]{value.Asc(), value.Desc(), value.Asc().NullsFirst(), value.Desc().NullsLast()} {
		base := Query[nullableRecord]{}.OrderBy(order, id.Asc())
		compiled, err := CompileSelect(table, base)
		if err != nil {
			t.Fatal(err)
		}
		// Independent PostgreSQL native ordering is the oracle; pagination must
		// reproduce it exactly without skipping/duplicating equal values or NULLs.
		rows, err := admin.Query(ctx, compiled.SQL, compiled.Args...)
		if err != nil {
			t.Fatal(err)
		}
		expected := []int64{}
		for rows.Next() {
			var n int64
			var v *string
			if err := rows.Scan(&n, &v); err != nil {
				t.Fatal(err)
			}
			expected = append(expected, n)
		}
		rows.Close()
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		pageQuery := base.Limit(2)
		actual := []int64{}
		for page := 0; page < 5; page++ {
			models, err := Select(ctx, pool, table, pageQuery)
			if err != nil {
				t.Fatal(err)
			}
			if len(models) == 0 {
				break
			}
			for _, model := range models {
				actual = append(actual, model.ID)
			}
			last := models[len(models)-1]
			pageQuery, err = SeekAfter(table, base.Limit(2), []BoundColumn[nullableRecord]{BindColumn(id)}, []Assignment[nullableRecord]{Set(value, Some(last.Value)), Set(id, Some(last.ID))})
			if err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatal("keyset omitted/duplicated tied or NULL rows", actual, expected)
		}
	}
}
