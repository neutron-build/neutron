package db

// Q12 review-1: helpers of the cross-table constraint order and of the
// refusals that recommend --allow-destructive.

import (
	"context"
	"strings"
	"testing"
)

func TestQ12SameColumnSetIn(t *testing.T) {
	keys := [][]string{{"a", "b"}, {"c"}}
	for _, c := range []struct {
		cols []string
		want bool
	}{
		{[]string{"b", "a"}, true}, // a foreign key may list the key's columns in any order
		{[]string{"c"}, true},
		{[]string{"a"}, false},
		{[]string{"a", "b", "c"}, false},
		{nil, false},
	} {
		if got := sameColumnSetIn(c.cols, keys); got != c.want {
			t.Errorf("sameColumnSetIn(%v) = %v, want %v", c.cols, got, c.want)
		}
	}
}

func TestQ12UniqueIndexColumns(t *testing.T) {
	col := func(s string) *string { return &s }
	pred := "a > 0"
	if got := uniqueIndexColumns(V2Index{Unique: true, Key: []V2IndexKeyPart{{Column: col("a")}, {Column: col("b")}}}); strings.Join(got, ",") != "a,b" {
		t.Errorf("plain unique index: %v", got)
	}
	for name, idx := range map[string]V2Index{
		"not unique": {Key: []V2IndexKeyPart{{Column: col("a")}}},
		"partial":    {Unique: true, Where: &pred, Key: []V2IndexKeyPart{{Column: col("a")}}},
		"expression": {Unique: true, Key: []V2IndexKeyPart{{Expression: col("lower(a)")}}},
	} {
		if got := uniqueIndexColumns(idx); got != nil {
			t.Errorf("%s: a foreign key cannot reference it, got %v", name, got)
		}
	}
}

func TestQ12FlagDropsNote(t *testing.T) {
	one := []RetainedObject{{Kind: "view", Identity: V2Identity{Schema: "app", Name: "v"}}}
	if got := flagDropsNote(one); got != "" {
		t.Errorf("a single retained object is the one the refusal names: %q", got)
	}
	two := append(one, RetainedObject{Kind: "column", Identity: V2Identity{Schema: "app", Name: "t"}, Column: "legacy"})
	if got := flagDropsNote(two); !strings.Contains(got, "view app.v, column app.t.legacy") {
		t.Errorf("the note must list every retained object: %q", got)
	}
}

// Q12 review-2 R2-1: a view reads another only when its definition names
// it in a relation position; aliases and column names never count.
func TestQ12RelationRefs(t *testing.T) {
	for text, want := range map[string]string{
		// pg_get_viewdef spellings.
		" SELECT customer_totals.customer,\n    sum(customer_totals.total) AS summary\n   FROM app.orders customer_totals\n  GROUP BY customer_totals.customer;": "app.orders",
		" SELECT count(*) AS n\n   FROM app.customer_totals;":                      "app.customer_totals",
		" SELECT a.id\n   FROM (app.a a\n     JOIN app.b b ON ((a.id = b.id)));":   "app.a app.b",
		" SELECT x.id\n   FROM app.x, app.y summary\n  WHERE (x.id = summary.id);": "app.x app.y",
		" SELECT f.id\n   FROM app.f() f(id, keep);":                               "",
		" SELECT s.id\n   FROM ( SELECT t.id\n           FROM app.t) s;":           "app.t",
		` SELECT "Q".id FROM "My Schema"."My View" "Q"`:                            "My Schema.My View",
		" SELECT id FROM plain_view":                                               "plain_view",
		" SELECT summary FROM app.t WHERE summary > 0":                             "app.t",
		" SELECT 'FROM app.fake' AS s FROM app.t -- JOIN app.fake2":                "app.t",
	} {
		refs, ok := relationRefs(text)
		if !ok {
			t.Errorf("relationRefs(%q) could not read it", text)
			continue
		}
		var got []string
		for _, r := range refs {
			got = append(got, strings.Join(r, "."))
		}
		if strings.Join(got, " ") != want {
			t.Errorf("relationRefs(%q) = %q, want %q", text, strings.Join(got, " "), want)
		}
	}
}

func TestQ12ViewOrder(t *testing.T) {
	id := func(s, n string) V2Identity { return V2Identity{Schema: s, Name: n} }
	totals, summary := id("app", "customer_totals"), id("app", "summary")
	// Review-2 n01b: the base aliases a column as the dependent's name.
	defs := map[V2Identity]string{
		totals:  " SELECT orders.customer, sum(orders.total) AS summary FROM app.orders GROUP BY orders.customer",
		summary: " SELECT count(*) AS n FROM app.customer_totals",
	}
	order := orderViews([]V2Identity{totals, summary}, viewReadsByText([]V2Identity{totals, summary}, defs))
	if order[0] != totals {
		t.Errorf("the base must be created first: %v", order)
	}
	order = orderViews([]V2Identity{summary, totals}, viewReadsByText([]V2Identity{summary, totals}, defs))
	if order[0] != totals {
		t.Errorf("the base must be created first whatever the given order: %v", order)
	}
	// A table alias, and a column, named like the dependent view.
	for _, base := range []string{
		" SELECT summary.id FROM app.t summary",
		" SELECT t.id, t.summary FROM app.t",
	} {
		a := id("app", "a")
		d := map[V2Identity]string{a: base, summary: " SELECT id FROM app.a"}
		if got := orderViews([]V2Identity{a, summary}, viewReadsByText([]V2Identity{a, summary}, d)); got[0] != a {
			t.Errorf("%q: a spurious edge inverted the order: %v", base, got)
		}
	}
	// Schema-qualified: another schema's view of the same name is not it.
	x, rx := id("app", "x"), id("rep", "x")
	d := map[V2Identity]string{x: " SELECT id FROM rep.x", rx: " SELECT id FROM app.t"}
	if got := orderViews([]V2Identity{x, rx}, viewReadsByText([]V2Identity{x, rx}, d)); got[0] != rx {
		t.Errorf("app.x reads rep.x, so rep.x comes first: %v", got)
	}
	// Unqualified names match only public views.
	pa, pb := id("public", "a"), id("public", "b")
	d = map[V2Identity]string{pa: " SELECT id FROM b", pb: " SELECT id FROM t"}
	if got := orderViews([]V2Identity{pa, pb}, viewReadsByText([]V2Identity{pa, pb}, d)); got[0] != pb {
		t.Errorf("public.a reads public.b: %v", got)
	}
	// A cycle falls back to the given order.
	if got := orderViews([]V2Identity{x, rx}, map[V2Identity][]V2Identity{x: {rx}, rx: {x}}); got[0] != x || got[1] != rx {
		t.Errorf("a cycle keeps the given order: %v", got)
	}
}

// Q12 review-2 R2-2: the live default operator class follows PostgreSQL's
// GetDefaultOpClass: an exact type match, then the preferred type of the
// category among binary-coercible ones (varchar resolves to text_ops, not
// bpchar_ops), domains through their base type, polymorphic classes for
// arrays and enums.
func TestQ12DefaultOpclassLive(t *testing.T) {
	h := newQ07Harness(t, "q12opc")
	h.exec(`CREATE TYPE public.q12_mood AS ENUM ('a', 'b')`)
	h.exec(`CREATE DOMAIN public.q12_dom AS varchar(5)`)
	norm, err := h.client.NewTwinNormalizer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer norm.Close()
	for _, c := range []struct{ method, typ, want string }{
		{"btree", "text", "text_ops"},
		{"btree", "varchar(20)", "text_ops"},
		{"btree", "character varying", "text_ops"},
		{"btree", "character(3)", "bpchar_ops"},
		{"btree", "integer", "int4_ops"},
		{"btree", "integer[]", "array_ops"},
		{"btree", "public.q12_mood", "enum_ops"},
		{"btree", "public.q12_dom", "text_ops"},
		{"hash", "text", "text_ops"},
	} {
		got, err := norm.DefaultOpclass(context.Background(), c.method, c.typ)
		if err != nil || got != c.want {
			t.Errorf("DefaultOpclass(%s, %s) = %q, %v; want %q", c.method, c.typ, got, err, c.want)
		}
	}
}
