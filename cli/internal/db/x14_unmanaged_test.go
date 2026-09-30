package db

// X14: a table (view, enum) the schema declares with managed: false is not
// neutron's. The planner never creates, alters or drops it, whatever the
// flags, does not compare it, and a managed table may reference it.

import (
	"context"
	"strings"
	"testing"
)

func x14Live(t *testing.T) *V2Document {
	t.Helper()
	m := m08Base()
	m.Views = []V2View{}
	m.Enums = []V2EnumDecl{}
	m.Tables[1].Columns = m.Tables[1].Columns[:2]
	m.Tables[1].Constraints = m.Tables[1].Constraints[:1]
	return m08Doc(t, m)
}

// x14Desired declares t, and other_app with the given managed flag (its
// content deliberately differs from the live table).
func x14Desired(t *testing.T, managed bool) *V2Document {
	t.Helper()
	m := m08Desired()
	m.Tables = append(m.Tables, V2Table{
		Identity: m08ID("public", "other_app"), Managed: managed,
		Columns:     []V2Column{m08Col("id", "int4", true), m08Col("elsewhere", "text", false)},
		Constraints: []V2Constraint{{Name: "other_app_pkey", Type: "primary-key", Columns: []string{"id"}}},
		Indexes:     []V2Index{},
	})
	return m08Doc(t, m)
}

func x14Diff(t *testing.T, desired, actual *V2Document, destructive, snapshot bool) DiffResult {
	t.Helper()
	res, err := DiffV2Document(context.Background(), desired, actual, DiffV2Options{AllowDestructive: destructive, SnapshotBase: snapshot})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return res
}

func TestX14UnmanagedTableIsNeverTouched(t *testing.T) {
	live := x14Live(t)
	for _, snapshot := range []bool{false, true} {
		res := x14Diff(t, x14Desired(t, false), live, true, snapshot)
		for _, stmt := range res.Up {
			if strings.Contains(stmt, "other_app") {
				t.Fatalf("snapshot=%v: a managed: false table is not planned: %q", snapshot, res.Up)
			}
		}
		var about []string
		for _, w := range res.Warnings {
			if strings.Contains(w, "other_app") {
				about = append(about, w)
			}
		}
		if len(about) != 1 || HasDrift(about) {
			t.Fatalf("snapshot=%v: an unmanaged table is noted, not drift: %q", snapshot, res.Warnings)
		}
		if !strings.Contains(strings.Join(res.Warnings, "\n"), "table public.other_app "+UnmanagedNote) {
			t.Fatalf("snapshot=%v: the plan notes the unmanaged table: %q", snapshot, res.Warnings)
		}
	}
	// Managed, the same declaration alters the table: the marker is the difference.
	res := x14Diff(t, x14Desired(t, true), live, true, false)
	if !strings.Contains(strings.Join(res.Up, "\n"), "other_app") {
		t.Fatalf("control: the managed declaration plans changes: %q", res.Up)
	}
}

func TestX14UnmanagedTableIsNotCreated(t *testing.T) {
	actual := m08Doc(t, m08Desired())
	res := x14Diff(t, x14Desired(t, false), actual, true, true)
	if len(res.Up) != 0 {
		t.Fatalf("a managed: false table that is missing is not created: %q", res.Up)
	}
}

func TestX14ManagedForeignKeyOntoUnmanagedTable(t *testing.T) {
	live := x14Live(t)
	desired := func(present bool) *V2Document {
		m := m08Desired()
		m.Tables[0].Constraints = append(m.Tables[0].Constraints, V2Constraint{Name: "t_o_fkey", Type: "foreign-key", Columns: []string{"id"}, References: &V2FKReference{Table: m08ID("public", "other_app"), Columns: []string{"id"}}})
		m.Tables = append(m.Tables, V2Table{
			Identity: m08ID("public", "other_app"), Managed: false,
			Columns:     []V2Column{m08Col("id", "int4", true)},
			Constraints: []V2Constraint{{Name: "other_app_pkey", Type: "primary-key", Columns: []string{"id"}}},
			Indexes:     []V2Index{},
		})
		return m08Doc(t, m)
	}
	res := x14Diff(t, desired(true), live, true, false)
	joined := strings.Join(res.Up, "\n")
	if !strings.Contains(joined, "t_o_fkey") || strings.Contains(joined, "drop table") || strings.Contains(joined, `create table "public"."other_app"`) {
		t.Fatalf("the key is added and the target is left alone: %q", res.Up)
	}
	// A live plan against a database without the table refuses; a snapshot plan cannot know.
	missing := m08Doc(t, m08Desired())
	if _, err := DiffV2Document(context.Background(), desired(false), missing, DiffV2Options{}); err == nil || !strings.Contains(err.Error(), "managed: false") || !strings.Contains(err.Error(), "t_o_fkey") {
		t.Fatalf("want a refusal naming the key and the unmanaged target, got %v", err)
	}
	if _, err := DiffV2Document(context.Background(), desired(false), missing, DiffV2Options{SnapshotBase: true}); err != nil {
		t.Fatalf("snapshot plans do not check the catalog: %v", err)
	}
}

func TestX14UnmanagedTableKeyDropIsRefused(t *testing.T) {
	// The unmanaged table has a foreign key onto a key the plan drops: the
	// plan cannot re-add it, and the refusal no longer offers the flag.
	m := m08Base()
	m.Views = []V2View{}
	live := m08Doc(t, m)
	d := m08Desired()
	d.Tables[0].Constraints = d.Tables[0].Constraints[:1] // t_keep_key dropped
	d.Tables = append(d.Tables, V2Table{
		Identity: m08ID("public", "other_app"), Managed: false,
		Columns:     []V2Column{m08Col("id", "int4", true)},
		Constraints: []V2Constraint{{Name: "other_app_pkey", Type: "primary-key", Columns: []string{"id"}}},
		Indexes:     []V2Index{},
	})
	_, err := DiffV2Document(context.Background(), m08Doc(t, d), live, DiffV2Options{AllowDestructive: true, SnapshotBase: true})
	if err == nil || !strings.Contains(err.Error(), "a table the schema declares managed: false") || strings.Contains(err.Error(), "re-run with --allow-destructive") {
		t.Fatalf("want a refusal that does not offer the flag, got %v", err)
	}
}

func TestX14UnmanagedEnumAndViewAreNotDropped(t *testing.T) {
	live := m08Doc(t, m08Base())
	d := m08Desired()
	d.Enums = []V2EnumDecl{{Identity: m08ID("public", "mood"), Managed: false, Values: []string{"ok"}}}
	d.Views = []V2View{{Identity: m08ID("public", "v_other"), Managed: false, Definition: "select 1"}}
	d.Tables = append(d.Tables, V2Table{
		Identity: m08ID("public", "other_app"), Managed: false,
		Columns:     []V2Column{m08Col("id", "int4", true)},
		Constraints: []V2Constraint{{Name: "other_app_pkey", Type: "primary-key", Columns: []string{"id"}}},
		Indexes:     []V2Index{},
	})
	res := x14Diff(t, m08Doc(t, d), live, true, true)
	for _, stmt := range res.Up {
		if strings.Contains(stmt, "mood") || strings.Contains(stmt, "v_other") || strings.Contains(stmt, "other_app") {
			t.Fatalf("unmanaged objects are not planned: %q", res.Up)
		}
	}
}

func TestX14PreserveUnmanaged(t *testing.T) {
	pulled := m08Doc(t, m08Base())
	m := m08Base()
	for i := range m.Tables {
		if m.Tables[i].Identity == m08ID("public", "other_app") {
			m.Tables[i].Managed = false
		}
	}
	m.Views = []V2View{}
	prev := m08Doc(t, m)
	out, kept, err := PreserveUnmanaged(pulled, prev)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 1 || kept[0] != "table public.other_app" {
		t.Fatalf("kept = %q", kept)
	}
	om := m08Model(t, out)
	if o := om.Table(m08ID("public", "other_app")); o == nil || o.Managed || len(o.Columns) != 4 {
		t.Fatalf("the marker is kept and the content is the pulled one: %+v", o)
	}
	if tt := om.Table(m08ID("public", "t")); tt == nil || !tt.Managed {
		t.Fatalf("other tables stay managed: %+v", tt)
	}
	same, kept, err := PreserveUnmanaged(pulled, pulled)
	if err != nil || same != pulled || kept != nil {
		t.Fatalf("no markers: unchanged, got %v %v", kept, err)
	}
}

func TestX14SnapshotKeepsKnownUnmanagedDependencies(t *testing.T) {
	baseModel := m08Base()
	base := m08Doc(t, baseModel)
	desiredModel := m08Base()
	desiredModel.Tables[0].Columns = append(desiredModel.Tables[0].Columns, m08Col("note", "text", false))
	desiredModel.Tables[1].Managed = false
	// The declaration is not authoritative for the external table's shape.
	// Its live FK still exists even when it is absent from this document.
	desiredModel.Tables[1].Constraints = desiredModel.Tables[1].Constraints[:1]
	desired := m08Doc(t, desiredModel)
	plan := x14Diff(t, desired, base, true, true)
	target, _, err := SnapshotTarget(base, desired, plan.Up)
	if err != nil {
		t.Fatal(err)
	}
	tm := m08Model(t, target)
	external := tm.Table(m08ID("public", "other_app"))
	if external.Managed || len(external.Constraints) != 2 {
		t.Fatalf("known external FK must survive with managed: false: %+v", external)
	}
	desiredModel.Tables = desiredModel.Tables[1:]
	_, err = DiffV2Document(context.Background(), m08Doc(t, desiredModel), target, DiffV2Options{AllowDestructive: true, SnapshotBase: true})
	if err == nil || !strings.Contains(err.Error(), "other_app_t_keep_fkey") {
		t.Fatalf("dropping the referenced table must be refused before apply: %v", err)
	}
}

func TestX14OwnershipUsesSchemaAndNameSeparately(t *testing.T) {
	previousModel := m08Desired()
	previousModel.Schemas = []V2SchemaDecl{{Name: "a"}, {Name: "a.b"}}
	previousModel.Tables[0].Identity = m08ID("a.b", "c")
	previousModel.Tables[0].Managed = false
	pulledModel := m08Desired()
	pulledModel.Schemas = previousModel.Schemas
	pulledModel.Tables[0].Identity = m08ID("a", "b.c")
	pulled := m08Doc(t, pulledModel)
	result, kept, err := PreserveUnmanaged(pulled, m08Doc(t, previousModel))
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 0 || result.SHA256Hex != pulled.SHA256Hex {
		t.Fatalf("quoted identities a.b.c are different tuples: kept=%q", kept)
	}
}
