package db

// Q12 review-1: helpers of the cross-table constraint order and of the
// refusals that recommend --allow-destructive.

import (
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
