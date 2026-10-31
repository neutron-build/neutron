package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestPostgresTypedTextPatternsBoundAndNullable(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	type model struct {
		ID       int64   `db:"id"`
		Text     string  `db:"text"`
		Optional *string `db:"optional,nullable"`
	}
	name := schemaSQL + ".text_patterns"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+name+"(id bigint PRIMARY KEY,text text NOT NULL,optional text); INSERT INTO "+name+" VALUES(1,'a%_\\b','Alpha'),(2,'alpha','alpha'),(3,'ALPHA',NULL),(4,'/0/1/2/','other')"); err != nil {
		t.Fatal(err)
	}
	table, err := NewPostgresTable[model](ctx, admin, schema, "text_patterns")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewColumn[model, int64](table, "ID")
	text, _ := NewColumn[model, string](table, "Text")
	optional, _ := NewColumn[model, *string](table, "Optional")
	cases := []struct {
		predicate Predicate[model]
		sql       string
		pattern   string
	}{
		{ContainsText(text, `%_\`), "text LIKE $1", `%\%\_\\%`},
		{Like(text, "%/1/%"), "text LIKE $1", "%/1/%"},
		{ILike(text, "alpha"), "text ILIKE $1", "alpha"},
		{LikeNullable(optional, "alpha"), "optional LIKE $1", "alpha"},
		{ILikeNullable(optional, "alpha"), "optional ILIKE $1", "alpha"},
		{Like(text, "'; DROP TABLE records;--"), "text LIKE $1", "'; DROP TABLE records;--"},
	}
	for _, test := range cases {
		actual, err := SelectColumn(ctx, admin, id, Query[model]{}.Where(test.predicate).OrderBy(id.Asc()))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := admin.Query(ctx, "SELECT id FROM "+name+" WHERE "+test.sql+" ORDER BY id", test.pattern)
		if err != nil {
			t.Fatal(err)
		}
		expected := []int64{}
		for rows.Next() {
			var value int64
			if err := rows.Scan(&value); err != nil {
				t.Fatal(err)
			}
			expected = append(expected, value)
		}
		rows.Close()
		if err := rows.Err(); err != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatal("independent native pattern oracle", actual, expected, err)
		}
	}
}
