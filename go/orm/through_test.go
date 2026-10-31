package orm

import "testing"

type throughLink struct {
	Tenant   string `db:"tenant"`
	ParentID int64  `db:"parent_id"`
	ChildID  int64  `db:"child_id"`
	ID       int64  `db:"id"`
}

func throughMetadata(t *testing.T, schema string) (ThroughRelation[assocParent, throughLink, assocChild], Column[throughLink, int64]) {
	t.Helper()
	base, _ := associationMetadata(t, schema)
	links, err := NewTable[throughLink](schema, "links")
	if err != nil {
		t.Fatal(err)
	}
	pt, _ := NewColumn[assocParent, string](base.parent, "Tenant")
	pi, _ := NewColumn[assocParent, int64](base.parent, "ID")
	lt, _ := NewColumn[throughLink, string](links, "Tenant")
	lp, _ := NewColumn[throughLink, int64](links, "ParentID")
	lc, _ := NewColumn[throughLink, int64](links, "ChildID")
	li, _ := NewColumn[throughLink, int64](links, "ID")
	ct, _ := NewColumn[assocChild, string](base.child, "Tenant")
	ci, _ := NewColumn[assocChild, int64](base.child, "ID")
	left, err := NewRelation(base.parent, links, Join(pt, lt), Join(pi, lp))
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewRelation(links, base.child, Join(lt, ct), Join(lc, ci))
	if err != nil {
		t.Fatal(err)
	}
	through, err := NewThroughRelation(left, right)
	if err != nil {
		t.Fatal(err)
	}
	return through, li
}
func TestThroughRelationKeepsExactLinkBinding(t *testing.T) {
	first, _ := throughMetadata(t, "owned")
	second, _ := throughMetadata(t, "owned")
	if _, err := NewThroughRelation(first.links, second.targets); err == nil {
		t.Fatal("same-named different link binding admitted")
	}
}
