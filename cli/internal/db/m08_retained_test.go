package db

// M08: a snapshot plan's target snapshot records what the plan leaves in
// place, and chains written by earlier CLIs are read with it.

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func m08Col(name, typ string, notNull bool) V2Column {
	codec := map[string]string{"int4": "number", "text": "string"}[typ]
	return V2Column{Name: name, Type: V2ColumnType{Name: typ, Codec: codec}, NotNull: notNull}
}

func m08Str(s string) *string { return &s }

func m08ID(schema, name string) V2Identity { return V2Identity{Schema: schema, Name: name} }

// m08Base is what a baseline records: t with a column, an index and a
// unique key the application no longer declares, a second application's
// table with a foreign key into t, a view over it and an enum it uses.
func m08Base() V2DocumentModel {
	mood := m08ID("public", "mood")
	return V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Enums:   []V2EnumDecl{{Identity: mood, Managed: true, Values: []string{"ok", "bad"}}},
		Tables: []V2Table{
			{
				Identity: m08ID("public", "t"), Managed: true,
				Columns: []V2Column{m08Col("id", "int4", true), m08Col("keep", "text", false), m08Col("old", "int4", false)},
				Constraints: []V2Constraint{
					{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}},
					{Name: "t_keep_key", Type: "unique", Columns: []string{"keep"}},
				},
				Indexes: []V2Index{{Identity: m08ID("public", "t_old_idx"), Method: "btree", Key: []V2IndexKeyPart{{Column: m08Str("old")}}}},
			},
			{
				Identity: m08ID("public", "other_app"), Managed: true,
				Columns: []V2Column{
					m08Col("id", "int4", true), m08Col("v", "text", false),
					{Name: "m", Type: V2ColumnType{Name: "enum", Codec: "enum", Enum: &mood}},
					m08Col("t_keep", "text", false),
				},
				Constraints: []V2Constraint{
					{Name: "other_app_pkey", Type: "primary-key", Columns: []string{"id"}},
					{Name: "other_app_t_keep_fkey", Type: "foreign-key", Columns: []string{"t_keep"}, References: &V2FKReference{Table: m08ID("public", "t"), Columns: []string{"keep"}}},
				},
				Indexes: []V2Index{},
			},
		},
		Views:  []V2View{{Identity: m08ID("public", "v_other"), Managed: true, Definition: " SELECT other_app.id, other_app.v FROM public.other_app;"}},
		Opaque: []V2Opaque{},
	}
}

// m08Desired is the application's schema: t without old and its index,
// plus the new columns; nothing else.
func m08Desired(extra ...string) V2DocumentModel {
	cols := []V2Column{m08Col("id", "int4", true), m08Col("keep", "text", false)}
	for _, c := range extra {
		cols = append(cols, m08Col(c, "int4", false))
	}
	return V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Enums:   []V2EnumDecl{},
		Tables: []V2Table{{
			Identity: m08ID("public", "t"), Managed: true, Columns: cols,
			Constraints: []V2Constraint{
				{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}},
				{Name: "t_keep_key", Type: "unique", Columns: []string{"keep"}},
			},
			Indexes: []V2Index{},
		}},
		Views: []V2View{}, Opaque: []V2Opaque{},
	}
}

func m08Doc(t *testing.T, m V2DocumentModel) *V2Document {
	t.Helper()
	root, err := RootFromModel(m)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := ParseV2Document(raw)
	if err != nil {
		t.Fatalf("fixture document invalid: %v", err)
	}
	return doc
}

func m08Hash(t *testing.T, raw []byte) string {
	t.Helper()
	doc, err := ParseV2Document(raw)
	if err != nil {
		t.Fatal(err)
	}
	return doc.SHA256Hex
}

func m08Model(t *testing.T, doc *V2Document) V2DocumentModel {
	t.Helper()
	m, err := ModelFromRoot(doc.Root)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func m08Plan(t *testing.T, desired, base *V2Document, destructive bool, renames map[string]string) DiffResult {
	t.Helper()
	res, err := DiffV2Document(context.Background(), desired, base, DiffV2Options{AllowDestructive: destructive, SnapshotBase: true, Renames: renames})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return res
}

func m08Target(t *testing.T, base, desired *V2Document, up []string) (*V2Document, []string) {
	t.Helper()
	target, retained, err := SnapshotTarget(base, desired, up)
	if err != nil {
		t.Fatalf("SnapshotTarget: %v", err)
	}
	var names []string
	for _, r := range retained {
		names = append(names, r.String())
	}
	return target, names
}

func m08Count(warnings []string, sub string) int {
	n := 0
	for _, w := range warnings {
		if strings.Contains(w, sub) {
			n++
		}
	}
	return n
}

var m08Retained = []string{
	"table public.other_app", "column public.t.old", "index public.t_old_idx on table public.t",
	"view public.v_other", "enum public.mood",
}

func TestSnapshotTargetRecordsWhatThePlanLeavesInPlace(t *testing.T) {
	base := m08Doc(t, m08Base())
	desired := m08Doc(t, m08Desired("note"))

	res := m08Plan(t, desired, base, false, nil)
	if len(res.Up) != 1 || !strings.Contains(res.Up[0], `add column "note"`) {
		t.Fatalf("non-destructive plan must only add note: %q", res.Up)
	}
	if got := m08Count(res.Warnings, "left untouched"); got != 5 {
		t.Fatalf("want 5 left-untouched notes, got %d: %q", got, res.Warnings)
	}
	target, retained := m08Target(t, base, desired, res.Up)
	if !reflect.DeepEqual(retained, m08Retained) {
		t.Fatalf("retained = %q, want %q", retained, m08Retained)
	}
	tm, bm := m08Model(t, target), m08Model(t, base)
	tt := tm.Table(m08ID("public", "t"))
	if got := strings.Join(columnNames(*tt), ","); got != "id,keep,old,note" {
		t.Fatalf("t columns %s: old keeps its place, note is appended (the database order)", got)
	}
	if !reflect.DeepEqual(*tt.Column("old"), *bm.Table(m08ID("public", "t")).Column("old")) {
		t.Fatalf("old must be carried verbatim")
	}
	if tt.Index("t_old_idx") == nil {
		t.Fatalf("t_old_idx must be recorded")
	}
	if !reflect.DeepEqual(*tm.Table(m08ID("public", "other_app")), *bm.Table(m08ID("public", "other_app"))) {
		t.Fatalf("other_app must be carried verbatim")
	}
	if tm.View(m08ID("public", "v_other")) == nil || tm.Enum(m08ID("public", "mood")) == nil {
		t.Fatalf("view and enum must be recorded")
	}
	again, _ := m08Target(t, base, desired, res.Up)
	if !bytes.Equal(again.Canonical, target.Canonical) {
		t.Fatalf("equal inputs must give byte-identical targets")
	}

	// The next plan still reports them as left in place, not as new, and
	// records them again.
	desired2 := m08Doc(t, m08Desired("note", "note2"))
	res2 := m08Plan(t, desired2, target, false, nil)
	if len(res2.Up) != 1 || !strings.Contains(res2.Up[0], `add column "note2"`) {
		t.Fatalf("next plan must only add note2: %q", res2.Up)
	}
	if got := m08Count(res2.Warnings, "left untouched"); got != 5 {
		t.Fatalf("next plan: want 5 left-untouched notes, got %d: %q", got, res2.Warnings)
	}
	target2, retained2 := m08Target(t, target, desired2, res2.Up)
	if !reflect.DeepEqual(retained2, m08Retained) {
		t.Fatalf("next plan retained = %q", retained2)
	}
	tm2 := m08Model(t, target2)
	if got := strings.Join(columnNames(*tm2.Table(m08ID("public", "t"))), ","); got != "id,keep,old,note,note2" {
		t.Fatalf("t columns after the next plan: %s", got)
	}

	// An explicit --allow-destructive plan drops them, and its target is
	// the desired document itself.
	res3 := m08Plan(t, desired2, target2, true, nil)
	for _, want := range []string{
		`drop view if exists "public"."v_other"`,
		`alter table "public"."t" drop column if exists "old"`,
		`drop index if exists "public"."t_old_idx"`,
		`drop table if exists "public"."other_app"`,
		`drop type if exists "public"."mood"`,
	} {
		found := false
		for _, up := range res3.Up {
			found = found || up == want
		}
		if !found {
			t.Fatalf("destructive plan must contain %s: %q", want, res3.Up)
		}
	}
	target3, retained3 := m08Target(t, target2, desired2, res3.Up)
	if len(retained3) != 0 || target3.SHA256Hex != desired2.SHA256Hex {
		t.Fatalf("after the drops the target is the desired document (retained %q)", retained3)
	}
}

// With --allow-destructive, an enum whose last column the same plan drops
// is not dropped (still used while the plan runs): it stays in place and
// the next destructive plan drops it.
func TestSnapshotTargetKeepsEnumStillUsedByADroppedColumn(t *testing.T) {
	mood := m08ID("public", "mood")
	base := m08Doc(t, V2DocumentModel{
		Version: 2, Dialect: "postgresql", Capabilities: []string{},
		Schemas: []V2SchemaDecl{{Name: "public"}},
		Enums:   []V2EnumDecl{{Identity: mood, Managed: true, Values: []string{"ok"}}},
		Tables: []V2Table{{
			Identity: m08ID("public", "t"), Managed: true,
			Columns:     []V2Column{m08Col("id", "int4", true), {Name: "status", Type: V2ColumnType{Name: "enum", Codec: "enum", Enum: &mood}}},
			Constraints: []V2Constraint{{Name: "t_pkey", Type: "primary-key", Columns: []string{"id"}}},
			Indexes:     []V2Index{},
		}},
		Views: []V2View{}, Opaque: []V2Opaque{},
	})
	dm := m08Desired()
	dm.Tables[0].Columns = dm.Tables[0].Columns[:1]
	dm.Tables[0].Constraints = dm.Tables[0].Constraints[:1]
	desired := m08Doc(t, dm)

	res := m08Plan(t, desired, base, true, nil)
	if m08Count(res.Warnings, "still used") != 1 {
		t.Fatalf("fixture must hit the still-used branch: %q", res.Warnings)
	}
	target, retained := m08Target(t, base, desired, res.Up)
	if !reflect.DeepEqual(retained, []string{"enum public.mood"}) {
		t.Fatalf("retained = %q", retained)
	}
	res2 := m08Plan(t, desired, target, true, nil)
	if !reflect.DeepEqual(res2.Up, []string{`drop type if exists "public"."mood"`}) {
		t.Fatalf("next destructive plan must drop the enum: %q", res2.Up)
	}
}

// A rename in the same plan: structured references of what stays in place
// follow it; expressions that may name the old column refuse.
func TestSnapshotTargetFollowsRenames(t *testing.T) {
	bm := m08Base()
	bm.Tables[0].Indexes = append(bm.Tables[0].Indexes, V2Index{
		Identity: m08ID("public", "t_keep_idx"), Method: "btree",
		Key: []V2IndexKeyPart{{Column: m08Str("keep")}}, Include: []string{"old"},
	})
	base := m08Doc(t, bm)
	dm := m08Desired("note")
	dm.Tables[0].Columns[1].Name = "kept"
	dm.Tables[0].Constraints[1].Columns = []string{"kept"}
	desired := m08Doc(t, dm)
	renames := map[string]string{"public.t.kept": "keep"}

	res := m08Plan(t, desired, base, false, renames)
	target, _ := m08Target(t, base, desired, res.Up)
	tm := m08Model(t, target)
	tt := tm.Table(m08ID("public", "t"))
	if got := strings.Join(columnNames(*tt), ","); got != "id,kept,old,note" {
		t.Fatalf("t columns %s", got)
	}
	idx := tt.Index("t_keep_idx")
	if idx == nil || *idx.Key[0].Column != "kept" || !reflect.DeepEqual(idx.Include, []string{"old"}) {
		t.Fatalf("carried index must follow the rename: %+v", idx)
	}
	fk := tm.Table(m08ID("public", "other_app")).Constraint("other_app_t_keep_fkey")
	if !reflect.DeepEqual(fk.References.Columns, []string{"kept"}) {
		t.Fatalf("carried foreign key must follow the rename: %v", fk.References.Columns)
	}

	for name, mutate := range map[string]func(*V2DocumentModel){
		"index expression": func(m *V2DocumentModel) {
			m.Tables[0].Indexes = append(m.Tables[0].Indexes, V2Index{Identity: m08ID("public", "t_lower_idx"), Method: "btree", Key: []V2IndexKeyPart{{Expression: m08Str("lower(keep)")}}})
		},
		"index predicate": func(m *V2DocumentModel) {
			m.Tables[0].Indexes[0].Where = m08Str("(keep IS NOT NULL)")
		},
		"view": func(m *V2DocumentModel) {
			m.Views[0].Definition = " SELECT t.keep FROM public.t;"
		},
		"generated column": func(m *V2DocumentModel) {
			m.Tables[0].Columns = append(m.Tables[0].Columns, V2Column{Name: "g", Type: V2ColumnType{Name: "text", Codec: "string"}, Generated: &V2Generated{Expression: `("keep" || 'x'::text)`}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			bm := m08Base()
			mutate(&bm)
			base := m08Doc(t, bm)
			res := m08Plan(t, desired, base, false, renames)
			_, _, err := SnapshotTarget(base, desired, res.Up)
			if err == nil || !strings.Contains(err.Error(), `may reference column "keep"`) || !strings.Contains(err.Error(), "--allow-destructive") {
				t.Fatalf("want a refusal naming the renamed column, got %v", err)
			}
		})
	}
}

func TestSnapshotTargetScope(t *testing.T) {
	app := m08ID("app", "x")
	withApp := func() V2DocumentModel {
		m := m08Base()
		m.Schemas = append(m.Schemas, V2SchemaDecl{Name: "app"})
		m.Tables = append(m.Tables, V2Table{
			Identity: app, Managed: true, Columns: []V2Column{m08Col("id", "int4", true)},
			Constraints: []V2Constraint{{Name: "x_pkey", Type: "primary-key", Columns: []string{"id"}}}, Indexes: []V2Index{},
		})
		return m
	}

	t.Run("OtherSchemasAreNotRecorded", func(t *testing.T) {
		base := m08Doc(t, withApp())
		desired := m08Doc(t, m08Desired("note"))
		res := m08Plan(t, desired, base, false, nil)
		target, retained := m08Target(t, base, desired, res.Up)
		tm := m08Model(t, target)
		if !reflect.DeepEqual(retained, m08Retained) || tm.Table(app) != nil {
			t.Fatalf("app.x is outside the managed scope: retained %q", retained)
		}
	})

	t.Run("ReferenceOutOfScopeRefuses", func(t *testing.T) {
		bm := withApp()
		bm.Tables[1].Constraints = append(bm.Tables[1].Constraints, V2Constraint{Name: "other_app_x_fkey", Type: "foreign-key", Columns: []string{"id"}, References: &V2FKReference{Table: app, Columns: []string{"id"}}})
		base := m08Doc(t, bm)
		desired := m08Doc(t, m08Desired("note"))
		res := m08Plan(t, desired, base, false, nil)
		_, _, err := SnapshotTarget(base, desired, res.Up)
		if err == nil || !strings.Contains(err.Error(), "table public.other_app is left in place") ||
			!strings.Contains(err.Error(), "references table app.x") ||
			!strings.Contains(err.Error(), "declare schema app and table app.x in the schema document") ||
			strings.Count(err.Error(), "--allow-destructive") != 1 {
			t.Fatalf("want a refusal naming the undeclared target once, got %v", err)
		}
	})

	t.Run("UnmanagedDeclarationIsRecordedAsDeclared", func(t *testing.T) {
		base := m08Doc(t, m08Base())
		dm := m08Desired("note")
		dm.Views = []V2View{{Identity: m08ID("public", "v_other"), Managed: false, Definition: "select 1"}}
		desired := m08Doc(t, dm)
		res := m08Plan(t, desired, base, false, nil)
		target, retained := m08Target(t, base, desired, res.Up)
		tm := m08Model(t, target)
		v := tm.View(m08ID("public", "v_other"))
		for _, r := range retained {
			if strings.HasPrefix(r, "view ") {
				t.Fatalf("a managed: false view is declared, not left in place: %q", retained)
			}
		}
		if v == nil || v.Managed || v.Definition != "select 1" {
			t.Fatalf("the target records the unmanaged declaration as written: %+v", v)
		}
	})

	t.Run("UnmanagedBaseEntryIsCarriedNotReported", func(t *testing.T) {
		bm := m08Base()
		for i := range bm.Tables {
			if bm.Tables[i].Identity == m08ID("public", "other_app") {
				bm.Tables[i].Managed = false
			}
		}
		base := m08Doc(t, bm)
		desired := m08Doc(t, m08Desired("note"))
		res := m08Plan(t, desired, base, true, nil)
		for _, stmt := range res.Up {
			if strings.Contains(stmt, "other_app") {
				t.Fatalf("a table the chain records managed: false is never dropped: %q", res.Up)
			}
		}
		target, retained := m08Target(t, base, desired, res.Up)
		for _, r := range retained {
			if r == "table public.other_app" {
				t.Fatalf("carried, not reported: %q", retained)
			}
		}
		tm := m08Model(t, target)
		if tt := tm.Table(m08ID("public", "other_app")); tt == nil || tt.Managed {
			t.Fatalf("the unmanaged entry is carried forward as recorded: %+v", tt)
		}
	})
}

func TestStatementKeyMatchesLayoutNotCase(t *testing.T) {
	a := statementKey(`drop table if exists "public"."other_app"`)
	for _, same := range []string{
		`DROP TABLE IF EXISTS "public"."other_app";`,
		"drop  table if exists\n  \"public\" . \"other_app\" -- note\n",
	} {
		if statementKey(same) != a {
			t.Fatalf("%q must match", same)
		}
	}
	if statementKey(`drop table if exists "public"."Other_app"`) == a {
		t.Fatalf("quoted identifiers are case-sensitive")
	}
	got := plannedRenames([]string{`alter table "public"."t" rename column "a""b" to "c";`, `alter table "public"."t" add column "x" integer`})
	if !reflect.DeepEqual(got, map[V2Identity]map[string]string{m08ID("public", "t"): {`a"b`: "c"}}) {
		t.Fatalf("plannedRenames = %v", got)
	}
}

// m08WriteMigration writes a generated migration the way the CLI does, with
// the given target snapshot document: the desired document itself is what
// CLIs before M08 recorded.
func m08WriteMigration(t *testing.T, dir, version, name, baseRef string, base, target *V2Document, res DiffResult) {
	t.Helper()
	plan, err := BuildPlanArtifact(version, name, baseRef, base.SHA256Hex, target, nil, res)
	if err != nil {
		t.Fatal(err)
	}
	down := make([]string, len(res.Down))
	for i := range res.Down {
		down[i] = res.Down[len(res.Down)-1-i]
	}
	files, err := MigrationArtifactSet(dir, version, name, plan, target, strings.Join(res.Up, ";\n")+";", strings.Join(down, ";\n")+";")
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteArtifactSet(files); err != nil {
		t.Fatal(err)
	}
}

func m08WriteBaseline(t *testing.T, dir string, base *V2Document) {
	t.Helper()
	content, err := MarshalSnapshotJSON(BaselineSnapshotFor(base, nil, BaselineHistory{Shape: "absent", AppliedVersions: []string{}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, SnapshotDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, SnapshotDir, "000_baseline.snapshot.json"), content, 0o644); err != nil {
		t.Fatal(err)
	}
}

// M08 review-2: the chain is read exactly as recorded. A pre-M08 snapshot
// that omits what its migration left in place stays as written (the drift
// it causes recovers by re-baselining), and its up file does not change
// what the chain reads.
func TestSnapshotChainReadsSnapshotsAsRecorded(t *testing.T) {
	base := m08Doc(t, m08Base())
	desired := m08Doc(t, m08Desired("note"))
	desired2 := m08Doc(t, m08Desired("note", "note2"))

	dir := t.TempDir()
	m08WriteBaseline(t, dir, base)
	res := m08Plan(t, desired, base, false, nil)
	m08WriteMigration(t, dir, "001", "add_note", "000_baseline", base, desired, res) // pre-M08: target = desired
	if err := os.WriteFile(filepath.Join(dir, "001_add_note.up.sql"), []byte("select 1;\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	chain, err := LoadSnapshotChain(dir)
	if err != nil {
		t.Fatal(err)
	}
	head, err := chain.HeadDocument()
	if err != nil {
		t.Fatal(err)
	}
	if head.SHA256Hex != desired.SHA256Hex || chain.HeadSHA256 != desired.SHA256Hex {
		t.Fatalf("the head must read as recorded")
	}

	res2 := m08Plan(t, desired2, head, false, nil)
	target, retained := m08Target(t, head, desired2, res2.Up)
	if len(retained) != 0 || target.SHA256Hex != desired2.SHA256Hex {
		t.Fatalf("the recorded head holds nothing the schema omits, so nothing is carried: %q", retained)
	}
	m08WriteMigration(t, dir, "002", "add_note2", "001_add_note", desired, target, res2)
	if chain, err = LoadSnapshotChain(dir); err != nil || chain.HeadSHA256 != desired2.SHA256Hex {
		t.Fatalf("the chain extends as recorded: %v", err)
	}
}

// m08OrderDoc is public.t with the given integer columns (id first, the
// primary key).
func m08OrderDoc(t *testing.T, cols ...string) *V2Document {
	t.Helper()
	m := m08Desired()
	m.Tables[0].Columns = []V2Column{m08Col("id", "int4", true)}
	m.Tables[0].Constraints = m.Tables[0].Constraints[:1]
	for _, c := range cols {
		m.Tables[0].Columns = append(m.Tables[0].Columns, m08Col(c, "int4", false))
	}
	return m08Doc(t, m)
}

func m08Cols(t *testing.T, doc *V2Document) string {
	t.Helper()
	m := m08Model(t, doc)
	return strings.Join(columnNames(*m.Table(m08ID("public", "t"))), ",")
}

// P1: a column declared between existing ones is appended by PostgreSQL.
// The target snapshot records the database order and the next plan
// compares columns by name.
func TestSnapshotTargetRecordsDatabaseColumnOrder(t *testing.T) {
	base := m08OrderDoc(t, "a", "b")
	declared := m08OrderDoc(t, "a", "mid", "b")

	aligned, notes, err := AlignColumnOrder(declared, base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := m08Cols(t, aligned); got != "id,a,b,mid" || len(notes) != 1 || strings.Join(notes[0].Declared, ",") != "id,a,mid,b" {
		t.Fatalf("aligned %s, notes %+v", got, notes)
	}
	res := m08Plan(t, aligned, base, false, nil)
	if !reflect.DeepEqual(res.Up, []string{`alter table "public"."t" add column "mid" integer`}) {
		t.Fatalf("plan %q", res.Up)
	}
	target, _ := m08Target(t, base, aligned, res.Up)
	if got := m08Cols(t, target); got != "id,a,b,mid" {
		t.Fatalf("target records %s, the database order is id,a,b,mid", got)
	}

	// Next plan: mid is still declared between a and b, another new
	// column comes first: only mid2 is planned.
	next := m08OrderDoc(t, "mid2", "a", "mid", "b")
	aligned2, _, err := AlignColumnOrder(next, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	res2 := m08Plan(t, aligned2, target, false, nil)
	if !reflect.DeepEqual(res2.Up, []string{`alter table "public"."t" add column "mid2" integer`}) {
		t.Fatalf("next plan %q", res2.Up)
	}
	target2, _ := m08Target(t, target, aligned2, res2.Up)
	if got := m08Cols(t, target2); got != "id,a,b,mid,mid2" {
		t.Fatalf("next target %s", got)
	}
	aligned3, _, _ := AlignColumnOrder(next, target2, nil)
	if res3 := m08Plan(t, aligned3, target2, false, nil); len(res3.Up) != 0 {
		t.Fatalf("an order difference alone plans %q", res3.Up)
	}

	// M08 review-2: a swap of existing columns is informational too; the
	// snapshot keeps the database order and new columns are appended.
	for _, c := range []struct {
		cols []string
		want string
	}{
		{[]string{"b", "a"}, "id,a,b"},
		{[]string{"b", "mid", "a"}, "id,a,b,mid"},
		{[]string{"mid", "b", "a"}, "id,a,b,mid"},
	} {
		got, notes, err := AlignColumnOrder(m08OrderDoc(t, c.cols...), base, nil)
		if err != nil || len(notes) != 1 || m08Cols(t, got) != c.want {
			t.Fatalf("%v: err %v, notes %+v, aligned %s, want %s", c.cols, err, notes, m08Cols(t, got), c.want)
		}
	}

	// With renames and a column left in place.
	withOld := m08OrderDoc(t, "a", "old", "b")
	renamed := m08OrderDoc(t, "mid", "a2", "b")
	aligned4, _, err := AlignColumnOrder(renamed, withOld, RenamesByTable(map[string]string{"public.t.a2": "a"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := m08Cols(t, aligned4); got != "id,a2,b,mid" {
		t.Fatalf("aligned with rename %s", got)
	}
	res4 := m08Plan(t, aligned4, withOld, false, map[string]string{"public.t.a2": "a"})
	target4, retained := m08Target(t, withOld, aligned4, res4.Up)
	if got := m08Cols(t, target4); got != "id,a2,old,b,mid" || !reflect.DeepEqual(retained, []string{"column public.t.old"}) {
		t.Fatalf("target %s, retained %q", got, retained)
	}
}

// A pre-M08 snapshot recorded the declared order. The chain reads it as
// recorded; compared against the database order it is not drift.
func TestSnapshotChainDeclaredOrderIsOnlyNoted(t *testing.T) {
	dir := t.TempDir()
	base := m08OrderDoc(t, "a", "b")
	m08WriteBaseline(t, dir, base)
	declared := m08OrderDoc(t, "a", "mid", "b")
	res := m08Plan(t, declared, base, false, nil)
	m08WriteMigration(t, dir, "001", "add_mid", "000_baseline", base, declared, res)
	chain, err := LoadSnapshotChain(dir)
	if err != nil {
		t.Fatal(err)
	}
	head, err := chain.HeadDocument()
	if err != nil {
		t.Fatal(err)
	}
	if got := m08Cols(t, head); got != "id,a,mid,b" {
		t.Fatalf("head reads %s, as recorded", got)
	}
	// The drift gate's comparison: recorded snapshot against the database.
	drift := m08Plan(t, head, m08OrderDoc(t, "a", "b", "mid"), true, nil)
	if len(drift.Up) != 0 || HasDrift(drift.Warnings) {
		t.Fatalf("an order difference is not drift: up %q warnings %q", drift.Up, drift.Warnings)
	}
}

// Review-1 F6: "stays" is decided by matching the planner's rendering. Any
// other spelling of a drop is read as leaving the object in place, so the
// expected state keeps it and the drift gate refuses once it is gone:
// a mismatch always fails closed.
func TestSnapshotTargetUnrecognisedDropFailsClosed(t *testing.T) {
	base := m08Doc(t, m08Base())
	dm := m08Desired("note")
	dm.Enums = m08Base().Enums // other_app's column type stays declared
	desired := m08Doc(t, dm)
	canonical := m08Plan(t, desired, base, true, nil).Up
	if _, retained := m08Target(t, base, desired, canonical); len(retained) != 0 {
		t.Fatalf("the planner's own drops leave nothing in place: %q", retained)
	}
	for _, spelling := range []string{
		`drop table public.other_app`,
		`DROP TABLE "public"."other_app" CASCADE`,
		`drop table other_app`,
		`drop table "public"."other_app", "public"."x"`,
		`drop table "Public"."other_app"`,
		`-- drop table if exists "public"."other_app"`,
		`select 'drop table if exists "public"."other_app"'`,
		`DO $$ BEGIN drop table if exists "public"."other_app"; END $$`,
	} {
		var up []string
		for _, u := range canonical {
			if u == `drop table if exists "public"."other_app"` {
				u = spelling
			}
			up = append(up, u)
		}
		_, retained := m08Target(t, base, desired, up)
		found := false
		for _, r := range retained {
			found = found || r == "table public.other_app"
		}
		if !found {
			t.Fatalf("%q: other_app must be read as left in place (fail closed), retained %q", spelling, retained)
		}
	}
}
