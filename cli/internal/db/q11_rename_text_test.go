package db

// Q11 review-1: a table whose live text predates its renames is refused
// only for elements whose text may name a renamed column. The text is read
// as PostgreSQL's lexer reads identifiers; what it cannot read counts as
// naming one.

import (
	"reflect"
	"testing"
)

func TestQ11SQLIdentifiers(t *testing.T) {
	cases := []struct {
		text   string
		idents []string
		ok     bool
	}{
		{`(net > (0)::numeric)`, []string{"net", "numeric"}, true},
		{`abs("Net") + NET + Other`, []string{"abs", "Net", "net", "other"}, true},
		{`"a""b" = 'net'`, []string{`a"b`}, true},
		{`label <> 'it''s net' AND x = E'\'net\'' AND b = B'101' AND h = X'1F'`, []string{"label", "and", "x", "and", "b", "and", "h"}, true},
		{`$$net$$ || $tag$ net $ $tag$ || other`, []string{"other"}, true},
		{`/* net /* nested net */ */ other -- net` + "\n" + `+ id`, []string{"other", "id"}, true},
		{`(t.* IS NOT NULL)`, []string{"t", "is", "not", "null"}, true},
		{`1.5e-3 + .5 + 0x1F + 1_000 + x$1`, []string{"x$1"}, true},
		{`'unterminated`, nil, false},
		{`"unterminated`, nil, false},
		{`/* unterminated`, nil, false},
		{`U&"\0061" > 0`, nil, false},
		{`u&'x'`, nil, false},
		{`$1 > 0`, nil, false},
		{`$tag$ unterminated`, nil, false},
		{`1net`, nil, false},
	}
	for _, c := range cases {
		idents, ok := sqlIdentifiers(c.text)
		if ok != c.ok || (ok && !reflect.DeepEqual(idents, c.idents)) {
			t.Errorf("sqlIdentifiers(%q) = %q, %v; want %q, %v", c.text, idents, ok, c.idents, c.ok)
		}
	}
}

func TestQ11TextMayNameRename(t *testing.T) {
	table := V2Identity{Schema: "app", Name: "t"}
	p := &v2Planner{opts: DiffV2Options{Renames: map[string]string{
		"app.t.amount":  "net",
		"app.t.label":   "Note",
		"app.u.renamed": "other",
	}}}
	for text, want := range map[string]bool{
		`(net > (0)::numeric)`: true,
		`abs("Net")`:           false, // a different column
		`"Note" <> ''`:         true,
		`note <> ''`:           false, // folds to note, not Note
		`abs(other)`:           false, // renamed on another table
		`(other * 2)`:          false,
		`label = 'net'`:        false, // a literal is no reference
		`(row_to_json(t.*) ->> 'net') IS NOT NULL`: true, // whole row
		`U&"\006Eet" > 0`:                          true, // unreadable counts as naming
	} {
		if got := p.textMayNameRename(table, text); got != want {
			t.Errorf("textMayNameRename(%q) = %v, want %v", text, got, want)
		}
	}
}
