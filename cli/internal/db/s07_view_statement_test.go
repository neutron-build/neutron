package db

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestS07ViewDefinitionSingleStatement: a view definition is one statement.
// A separator may only end it (pg_get_viewdef writes a trailing one);
// separators inside string literals, quoted identifiers, dollar quotes and
// comments are text. A second statement is refused at validation, as a
// statement separator in a check expression is (M08 review-3 INFO 7).
func TestS07ViewDefinitionSingleStatement(t *testing.T) {
	doc := func(def string) string {
		enc, err := json.Marshal(def)
		if err != nil {
			t.Fatal(err)
		}
		return `{"version": 2, "dialect": "postgresql", "capabilities": [], "schemas": [{"name": "public"}], "tables": [], "enums": [],
			"views": [{"identity": {"schema": "public", "name": "v"}, "managed": true, "definition": ` + string(enc) + `}], "opaque": []}`
	}
	for _, def := range []string{
		"select 1 as x",
		" SELECT 1 AS x;",
		"select 1 as x;\n  ",
		"select 1 as x; -- trailing comment",
		"select 1 as x; /* trailing ; comment */",
		"select 'a;b' as x",
		"select E'a\\';b' as x",
		`select 1 as "a;b"`,
		"select $q$;drop table t;$q$ as x",
		"select 1 as x -- ; not a separator",
	} {
		if _, err := ParseV2Document([]byte(doc(def))); err != nil {
			t.Errorf("single statement %q refused: %v", def, err)
		}
	}
	for _, def := range []string{
		`select 1 as x; drop table if exists "public"."other_app"`,
		"select 1 as x;;",
		"select 1 as x; select 2",
		"select 'a' as x;/* c */select 2",
	} {
		_, err := ParseV2Document([]byte(doc(def)))
		if err == nil || !strings.Contains(err.Error(), "[invalid-value]") || !strings.Contains(err.Error(), "view definition must be a single statement") {
			t.Errorf("second statement %q: got %v, want invalid-value", def, err)
		}
	}
}
