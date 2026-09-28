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
		"select 1 as a$x$",
		"select 1 as a$x$, 2 as \u00e9$x$",
		"select $$;$$ as x",
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
		// S07 review-1 F2: '$' continues an identifier, so "a$x$" does not
		// open a dollar quote that hides the separators.
		"select 1 as a$x$; drop table if exists public.victim; select 1 as b$x$",
		"select 1 as \u00e9$x$; drop table if exists public.victim; select 1 as b$x$",
	} {
		_, err := ParseV2Document([]byte(doc(def)))
		if err == nil || !strings.Contains(err.Error(), "[invalid-value]") || !strings.Contains(err.Error(), "view definition must be a single statement") {
			t.Errorf("second statement %q: got %v, want invalid-value", def, err)
		}
	}
}

// TestS07ExpressionSingleExpression: expression fields hold exactly one
// expression. A top-level comma or ';', or text the scanner cannot close,
// is refused; function-call commas, array subscripts, nested parens, row
// constructors and quoted/comment/dollar-quoted separators pass (S07
// review-2 N3).
func TestS07ExpressionSingleExpression(t *testing.T) {
	ok := []string{
		"1", "(1)", "abs(x)", "coalesce(a, b, c)", "greatest(a, b) + least(c, d)",
		"x + 1", "array[1, 2, 3]", "a[1]", "(a).b", "row(1, 2)",
		"x || ',' || y", "x || ';'", "y > 0 /* a, b; c */", "n$x$ + m$y$",
		"substring(s from '^[a-z,;]+')", "$q$a, b; c$q$ = t",
	}
	for _, e := range ok {
		if r := v2NotSingleExpression(e); r != "" {
			t.Errorf("legitimate expression %q refused: %s", e, r)
		}
	}
	bad := map[string]string{
		"1, drop column id":         "top-level comma",
		"1; drop table t":           "statement separator",
		"a, b":                      "top-level comma",
		"x > 0 -- c\r, drop column": "top-level comma",
		"(1":                        "unbalanced opening",
		"1)":                        "unbalanced closing",
		"'unterminated":             "unterminated",
		"$q$ open":                  "unterminated",
		"a /* open":                 "unterminated",
	}
	for e, want := range bad {
		r := v2NotSingleExpression(e)
		if r == "" || !strings.Contains(r, want) {
			t.Errorf("expression %q: got %q, want a reason mentioning %q", e, r, want)
		}
	}
}

// TestS07CRTerminatedLineComment: a bare CR ends a -- comment, as in
// PostgreSQL, so it cannot hide a following statement (S07 review-2 N2).
func TestS07CRTerminatedLineComment(t *testing.T) {
	got := SplitSQLStatements("select 1 -- c\r; select 2")
	var exec []string
	for _, s := range got {
		if hasExecutableSQL(s) {
			exec = append(exec, s)
		}
	}
	if len(exec) != 2 {
		t.Fatalf("CR must end the line comment: %d statements, want 2 (%q)", len(exec), exec)
	}
	// A view definition with a CR-terminated comment hiding a separator is
	// refused by the validator.
	if !v2HasSecondStatement("select 1 as x -- c\r; drop table t") {
		t.Error("v2HasSecondStatement must see the statement after a CR-terminated comment")
	}
	// \f and \v are whitespace, not punctuation.
	if toks := significantTokens("select\f1\v+\t2"); len(toks) != 4 {
		t.Errorf("form feed / vertical tab must be whitespace: %+v", toks)
	}
}
