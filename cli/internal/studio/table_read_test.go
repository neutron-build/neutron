package studio

import (
	"strings"
	"testing"
)

// Offline unit coverage for the multi-filter/multi-sort read parsing (the
// live conversation is pinned by TestStudioDataEditorS03E2E).

func readTestMeta() *tableMeta {
	return &tableMeta{
		Columns: map[string]tableColumnMeta{
			"id":   {Name: "id"},
			"body": {Name: "body"},
			"flag": {Name: "flag"},
			"note": {Name: "note"},
		},
	}
}

func TestParseTableFilters(t *testing.T) {
	meta := readTestMeta()

	t.Run("multiple conditions AND with ordered bound parameters", func(t *testing.T) {
		conds, args, err := parseTableFilters(
			`[{"column":"flag","op":"eq","value":"true"},{"column":"body","op":"like","value":"%a%"},{"column":"note","op":"is-null","value":""}]`,
			meta, true, 0)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(conds) != 3 {
			t.Fatalf("conds = %v", conds)
		}
		if conds[0] != `"flag" = $1` || conds[1] != `"body" LIKE $2` || conds[2] != `"note" IS NULL` {
			t.Fatalf("conds = %v", conds)
		}
		if len(args) != 2 || args[0] != "true" || args[1] != "%a%" {
			t.Fatalf("args = %v", args)
		}
	})

	t.Run("placeholders continue at the given offset (match first)", func(t *testing.T) {
		conds, args, err := parseTableFilters(`[{"column":"body","op":"ne","value":"x"}]`, meta, true, 2)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if conds[0] != `"body" <> $3` {
			t.Fatalf("conds = %v", conds)
		}
		if len(args) != 1 {
			t.Fatalf("args = %v", args)
		}
	})

	t.Run("unknown column, op, repeats and smuggled fields are refused", func(t *testing.T) {
		for name, raw := range map[string]string{
			"unknown column":  `[{"column":"ghost","op":"eq","value":"1"}]`,
			"unknown op":      `[{"column":"body","op":"regex","value":"1"}]`,
			"repeated column": `[{"column":"body","op":"eq","value":"1"},{"column":"body","op":"eq","value":"2"}]`,
			"empty column":    `[{"column":"","op":"eq","value":"1"}]`,
			"unknown field":   `[{"column":"body","op":"eq","value":"1","or":true}]`,
			"not an array":    `{"column":"body","op":"eq","value":"1"}`,
			"too many":        "[" + strings.Repeat(`{"column":"note","op":"is-null","value":""},`, maxTableFilters) + `{"column":"body","op":"eq","value":"1"}]`,
		} {
			if _, _, err := parseTableFilters(raw, meta, true, 0); err == nil {
				t.Fatalf("%s: expected refusal for %s", name, raw)
			}
		}
	})

	t.Run("column existence is only enforced when the catalog is known", func(t *testing.T) {
		if _, _, err := parseTableFilters(`[{"column":"ghost","op":"eq","value":"1"}]`, meta, false, 0); err != nil {
			t.Fatalf("catalog unknown must not validate columns: %v", err)
		}
	})
}

func TestParseTableSorts(t *testing.T) {
	meta := readTestMeta()

	t.Run("keys apply in array order with qualified columns", func(t *testing.T) {
		parts, err := parseTableSorts(`[{"column":"body","dir":"asc"},{"column":"id","dir":"desc"}]`, "rows", meta, true)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(parts) != 2 || parts[0] != `"rows"."body" ASC` || parts[1] != `"rows"."id" DESC` {
			t.Fatalf("parts = %v", parts)
		}
	})

	t.Run("dir is case-insensitive asc/desc, anything else refused", func(t *testing.T) {
		parts, err := parseTableSorts(`[{"column":"body","dir":"DESC"}]`, "rows", meta, true)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if parts[0] != `"rows"."body" DESC` {
			t.Fatalf("parts = %v", parts)
		}
		if _, err := parseTableSorts(`[{"column":"body","dir":"random"}]`, "rows", meta, true); err == nil {
			t.Fatal("invalid dir must be refused")
		}
	})

	t.Run("unknown column, smuggled fields and too many keys are refused", func(t *testing.T) {
		for name, raw := range map[string]string{
			"unknown column": `[{"column":"ghost","dir":"asc"}]`,
			"unknown field":  `[{"column":"body","dir":"asc","nulls":"first"}]`,
			"empty column":   `[{"column":"","dir":"asc"}]`,
			"too many":       `[{"column":"id","dir":"asc"},{"column":"body","dir":"asc"},{"column":"flag","dir":"asc"},{"column":"note","dir":"asc"},{"column":"id","dir":"desc"}]`,
		} {
			if _, err := parseTableSorts(raw, "rows", meta, true); err == nil {
				t.Fatalf("%s: expected refusal for %s", name, raw)
			}
		}
	})
}
