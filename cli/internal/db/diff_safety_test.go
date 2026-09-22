package db

import (
	"strings"
	"testing"
)

// V05 containment behavior: metadata protection, destructive gating, rename
// validation, and rejection of unsupported catalog structures/changes.

func TestDiffProtectedTablesNeverDropped(t *testing.T) {
	actual := Schema{Version: 1, Tables: []TableDef{
		{Name: "_neutron_migrations", Columns: []ColumnDef{{Name: "version", Type: "text", PrimaryKey: true}}},
		{Name: "_neutron_migration_lock", Columns: []ColumnDef{{Name: "id", Type: "integer"}}},
		{Name: "_neutron_schema_owner", Columns: []ColumnDef{{Name: "owner", Type: "text"}}},
		{Name: "audit_sentinel", Columns: []ColumnDef{{Name: "id", Type: "integer", PrimaryKey: true}}},
	}}
	desired := Schema{Version: 1}

	for _, allowDestructive := range []bool{false, true} {
		result, err := DiffSchema(desired, actual, DiffOptions{AllowDestructive: allowDestructive})
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range result.Up {
			if strings.Contains(stmt, "_neutron_") {
				t.Fatalf("destructive=%v: internal table leaked into plan: %s", allowDestructive, stmt)
			}
		}
		protectedWarned := false
		for _, w := range result.Warnings {
			if strings.Contains(w, "neutron-internal metadata") {
				protectedWarned = true
			}
		}
		if !protectedWarned {
			t.Fatalf("destructive=%v: expected internal-metadata warning, got %v", allowDestructive, result.Warnings)
		}
	}

	// With destructive acknowledgement the unmanaged sentinel drops; the
	// internal tables still never do.
	result, err := DiffSchema(desired, actual, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(result.Up, "\n")
	if !strings.Contains(joined, `drop table if exists "audit_sentinel"`) {
		t.Fatalf("sentinel drop missing with acknowledgement: %v", result.Up)
	}
}

func TestDiffExtensionOwnedTablesNeverDropped(t *testing.T) {
	actual := Schema{Version: 1, Tables: []TableDef{
		{Name: "spatial_ref_sys", ExtensionOwned: true, ExtensionOwner: "postgis", Columns: []ColumnDef{{Name: "srid", Type: "integer", PrimaryKey: true}}},
	}}
	desired := Schema{Version: 1}
	result, err := DiffSchema(desired, actual, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Up) != 0 {
		t.Fatalf("extension-owned table must never be dropped, got %v", result.Up)
	}
	warned := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "owned by a database extension") {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("expected extension-owned warning, got %v", result.Warnings)
	}
}

func TestDiffDesiredDeclaringExtensionOwnedTableRejected(t *testing.T) {
	// M1: a desired-schema table that matches an extension-owned actual
	// table must be REJECTED (named table + owning extension), never
	// modified — regardless of destructive acknowledgement.
	actual := Schema{Version: 1, Tables: []TableDef{
		{Name: "ext_t", ExtensionOwned: true, ExtensionOwner: "plpgsql", Columns: []ColumnDef{
			{Name: "id", Type: "integer", PrimaryKey: true},
		}},
	}}
	desired := Schema{Version: 1, Tables: []TableDef{
		{Name: "ext_t", Columns: []ColumnDef{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "extra", Type: "text"},
		}},
	}}
	_, err := DiffSchema(desired, actual, DiffOptions{AllowDestructive: true})
	if err == nil {
		t.Fatal("declaring an extension-owned table must be rejected even with acknowledgement")
	}
	if !strings.Contains(err.Error(), `"ext_t"`) || !strings.Contains(err.Error(), "plpgsql") {
		t.Fatalf("rejection must name the table and its owning extension, got: %v", err)
	}

	// Without a known owner name the rejection still fires.
	anon := actual
	anon.Tables[0].ExtensionOwner = ""
	if _, err := DiffSchema(desired, anon, DiffOptions{}); err == nil || !strings.Contains(err.Error(), `"ext_t"`) {
		t.Fatalf("expected rejection with unknown owner too, got %v", err)
	}
}

func TestDiffDesiredDeclaringInternalTableRejected(t *testing.T) {
	desired := Schema{Version: 1, Tables: []TableDef{
		{Name: "_neutron_migrations", Columns: []ColumnDef{{Name: "version", Type: "text"}}},
	}}
	_, err := DiffSchema(desired, Schema{Version: 1}, DiffOptions{AllowDestructive: true})
	if err == nil || !strings.Contains(err.Error(), "neutron-internal") {
		t.Fatalf("expected internal-table declaration rejection even with acknowledgement, got %v", err)
	}
}

func TestDiffColumnDropRequiresDestructiveIntent(t *testing.T) {
	desired := Schema{Version: 1, Tables: []TableDef{{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true}},
	}}}
	actual := Schema{Version: 1, Tables: []TableDef{{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "legacy", Type: "text"},
		},
	}}}

	result, err := DiffSchema(desired, actual, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(result.Up, "\n"), "drop column") {
		t.Fatalf("column drop must not be planned without acknowledgement: %v", result.Up)
	}

	result, err = DiffSchema(desired, actual, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(result.Up, "\n"), `drop column if exists "legacy"`) {
		t.Fatalf("column drop missing with acknowledgement: %v", result.Up)
	}
}

func TestDiffIndexDropRequiresDestructiveIntent(t *testing.T) {
	base := TableDef{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true}, {Name: "email", Type: "text"}},
	}
	desired := Schema{Version: 1, Tables: []TableDef{base}}
	actualTable := base
	actualTable.Indexes = []IndexDef{{Name: "users_email_idx", Columns: []string{"email"}}}
	actual := Schema{Version: 1, Tables: []TableDef{actualTable}}

	result, err := DiffSchema(desired, actual, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(result.Up, "\n"), "drop index") {
		t.Fatalf("index drop must not be planned without acknowledgement: %v", result.Up)
	}

	result, err = DiffSchema(desired, actual, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(result.Up, "\n"), `drop index if exists "users_email_idx"`) {
		t.Fatalf("index drop missing with acknowledgement: %v", result.Up)
	}
}

func TestDiffRenameSourceAndTargetValidated(t *testing.T) {
	desired := Schema{Version: 1, Tables: []TableDef{{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "full_name", Type: "text"},
		},
	}}}
	actual := Schema{Version: 1, Tables: []TableDef{{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "name", Type: "text"},
		},
	}}}

	// Forged source: "ghost" does not exist in the database.
	_, err := DiffSchema(desired, actual, DiffOptions{Renames: map[string]string{"users.full_name": "ghost"}})
	if err == nil || !strings.Contains(err.Error(), `source column "ghost" does not exist`) {
		t.Fatalf("expected forged-source rejection, got %v", err)
	}

	// Forged target table.
	_, err = DiffSchema(desired, actual, DiffOptions{Renames: map[string]string{"orders.full_name": "name"}})
	if err == nil || !strings.Contains(err.Error(), `table "orders" is not in the desired schema`) {
		t.Fatalf("expected unknown-table rejection, got %v", err)
	}

	// Target column missing from desired.
	_, err = DiffSchema(desired, actual, DiffOptions{Renames: map[string]string{"users.nope": "name"}})
	if err == nil || !strings.Contains(err.Error(), `target column "users.nope" does not exist`) {
		t.Fatalf("expected missing-target rejection, got %v", err)
	}

	// One source renamed to two targets (both targets exist in desired).
	twoTargets := Schema{Version: 1, Tables: []TableDef{{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "full_name", Type: "text"},
			{Name: "other", Type: "text"},
		},
	}}}
	_, err = DiffSchema(twoTargets, actual, DiffOptions{Renames: map[string]string{
		"users.full_name": "name",
		"users.other":     "name",
	}})
	if err == nil || !strings.Contains(err.Error(), "renamed to both") {
		t.Fatalf("expected ambiguous rename rejection, got %v", err)
	}
}

func TestDiffSharedTableUnsupportedCatalogRejected(t *testing.T) {
	base := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "a", Type: "integer"},
			{Name: "b", Type: "integer"},
		},
	}
	for _, reason := range []string{
		`multi-column unique constraint "uq_a_b" (spanning 2 columns)`,
		`composite foreign key "fk_ab" (spanning 2 columns)`,
		`partial index "users_partial" (with a WHERE predicate)`,
	} {
		actualTable := base
		actualTable.UnsupportedCatalog = []string{reason}
		_, err := DiffSchema(Schema{Version: 1, Tables: []TableDef{base}}, Schema{Version: 1, Tables: []TableDef{actualTable}}, DiffOptions{})
		if err == nil || !strings.Contains(err.Error(), reason) {
			t.Fatalf("expected rejection naming %q, got %v", reason, err)
		}
	}
}

func TestDiffSharedTableUnsupportedActualTypeRejected(t *testing.T) {
	desired := Schema{Version: 1, Tables: []TableDef{{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "tags", Type: "text"},
		},
	}}}
	actual := Schema{Version: 1, Tables: []TableDef{{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "tags", Type: "ARRAY"},
		},
	}}}
	_, err := DiffSchema(desired, actual, DiffOptions{})
	if err == nil || !strings.Contains(err.Error(), `unsupported type "ARRAY"`) {
		t.Fatalf("expected unsupported actual type rejection, got %v", err)
	}
}

func TestDiffSharedTableFKDriftRejected(t *testing.T) {
	usersTable := func(fk *ForeignKeyDef) TableDef {
		return TableDef{
			Name: "users",
			Columns: []ColumnDef{
				{Name: "id", Type: "serial", PrimaryKey: true},
				{Name: "org_id", Type: "integer", ForeignKey: fk},
			},
		}
	}
	orgs := TableDef{Name: "orgs", Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true}, {Name: "uuid", Type: "uuid"}}}
	desired := func(t TableDef) Schema { return Schema{Version: 1, Tables: []TableDef{t, orgs}} }
	withFK := usersTable(&ForeignKeyDef{Table: "orgs", Column: "id"})
	withoutFK := usersTable(nil)

	// Actual has the FK, desired does not declare it.
	_, err := DiffSchema(desired(withoutFK), Schema{Version: 1, Tables: []TableDef{withFK}}, DiffOptions{})
	if err == nil || !strings.Contains(err.Error(), "foreign key in the database that the schema does not declare") {
		t.Fatalf("expected undeclared-FK rejection, got %v", err)
	}

	// Desired declares it, database lacks it.
	_, err = DiffSchema(desired(withFK), Schema{Version: 1, Tables: []TableDef{withoutFK}}, DiffOptions{})
	if err == nil || !strings.Contains(err.Error(), "foreign key that does not exist in the database") {
		t.Fatalf("expected missing-FK rejection, got %v", err)
	}

	// Different target.
	drifted := usersTable(&ForeignKeyDef{Table: "orgs", Column: "uuid"})
	_, err = DiffSchema(desired(drifted), Schema{Version: 1, Tables: []TableDef{withFK}}, DiffOptions{})
	if err == nil || !strings.Contains(err.Error(), "foreign key differs") {
		t.Fatalf("expected FK-drift rejection, got %v", err)
	}

	// Matching FK (with no action spelled differently) is fine.
	spelled := usersTable(&ForeignKeyDef{Table: "orgs", Column: "id", OnDelete: "no action"})
	if _, err := DiffSchema(desired(spelled), Schema{Version: 1, Tables: []TableDef{withFK}}, DiffOptions{}); err != nil {
		t.Fatalf("matching FK must not error: %v", err)
	}
}

func TestDiffSharedTablePKDriftRejected(t *testing.T) {
	desired := Schema{Version: 1, Tables: []TableDef{{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true}, {Name: "email", Type: "text"}},
	}}}
	actual := Schema{Version: 1, Tables: []TableDef{{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "serial"}, {Name: "email", Type: "text"}},
	}}}
	_, err := DiffSchema(desired, actual, DiffOptions{})
	if err == nil || !strings.Contains(err.Error(), "primary key changes") {
		t.Fatalf("expected PK-drift rejection, got %v", err)
	}
}

func TestDiffSameNameIndexDefinitionChangeRejected(t *testing.T) {
	base := TableDef{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true}, {Name: "email", Type: "text"}, {Name: "name", Type: "text"}},
	}
	desiredTable := base
	desiredTable.Indexes = []IndexDef{{Name: "users_email_idx", Columns: []string{"name"}}}
	actualTable := base
	actualTable.Indexes = []IndexDef{{Name: "users_email_idx", Columns: []string{"email"}}}
	_, err := DiffSchema(
		Schema{Version: 1, Tables: []TableDef{desiredTable}},
		Schema{Version: 1, Tables: []TableDef{actualTable}},
		DiffOptions{},
	)
	if err == nil || !strings.Contains(err.Error(), `index "users_email_idx"`) || !strings.Contains(err.Error(), "different definition") {
		t.Fatalf("expected same-name index definition rejection, got %v", err)
	}
}

func TestDiffInvalidDesiredSchemaRejectedInsideDiff(t *testing.T) {
	// DiffSchema is the library boundary: it validates even if a caller forgot.
	_, err := DiffSchema(Schema{}, Schema{Version: 1}, DiffOptions{})
	if err == nil || !strings.Contains(err.Error(), "invalid desired schema") {
		t.Fatalf("expected validation inside DiffSchema, got %v", err)
	}
}

func TestDiffDestructiveDropsOrderedByFK(t *testing.T) {
	// L2: aaa (referenced) sorts before zzz (referencing) alphabetically;
	// drops must be FK-ordered instead, or --allow-destructive cannot
	// complete.
	aaa := TableDef{Name: "aaa", Columns: []ColumnDef{{Name: "id", Type: "integer", PrimaryKey: true}}}
	zzz := TableDef{Name: "zzz", Columns: []ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true},
		{Name: "aaa_id", Type: "integer", ForeignKey: &ForeignKeyDef{Table: "aaa", Column: "id"}},
	}}
	actual := Schema{Version: 1, Tables: []TableDef{aaa, zzz}}

	result, err := DiffSchema(Schema{Version: 1}, actual, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	dropAaa, dropZzz := -1, -1
	for i, stmt := range result.Up {
		if strings.Contains(stmt, `drop table if exists "aaa"`) {
			dropAaa = i
		}
		if strings.Contains(stmt, `drop table if exists "zzz"`) {
			dropZzz = i
		}
	}
	if dropAaa < 0 || dropZzz < 0 {
		t.Fatalf("both drops must be planned, got %v", result.Up)
	}
	if dropZzz > dropAaa {
		t.Fatalf("referencing table zzz must drop before referenced aaa, got %v", result.Up)
	}
}

func TestDiffDestructiveDropCycleRejected(t *testing.T) {
	// Two dropped tables referencing each other: a clear error, never an
	// infinite loop or a partially ordered plan.
	p := TableDef{Name: "p", Columns: []ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true},
		{Name: "q_id", Type: "integer", ForeignKey: &ForeignKeyDef{Table: "q", Column: "id"}},
	}}
	q := TableDef{Name: "q", Columns: []ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true},
		{Name: "p_id", Type: "integer", ForeignKey: &ForeignKeyDef{Table: "p", Column: "id"}},
	}}
	actual := Schema{Version: 1, Tables: []TableDef{p, q}}
	_, err := DiffSchema(Schema{Version: 1}, actual, DiffOptions{AllowDestructive: true})
	if err == nil || !strings.Contains(err.Error(), "foreign-key cycle") {
		t.Fatalf("expected cycle rejection, got %v", err)
	}

	// Self-reference cycle too.
	self := TableDef{Name: "self", Columns: []ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true},
		{Name: "parent_id", Type: "integer", ForeignKey: &ForeignKeyDef{Table: "self", Column: "id"}},
	}}
	_, err = DiffSchema(Schema{Version: 1}, Schema{Version: 1, Tables: []TableDef{self}}, DiffOptions{AllowDestructive: true})
	if err == nil || !strings.Contains(err.Error(), "foreign-key cycle") {
		t.Fatalf("expected self-reference cycle rejection, got %v", err)
	}
}

func TestDiffPrimaryKeyWithoutNotNullPlansNoDropNotNull(t *testing.T) {
	// L3: PG PKs are implicitly NOT NULL; hand-written JSON with primaryKey
	// but no notNull must not plan a doomed `drop not null` (the TS exporter
	// normalizes at export.ts — validation now does the same).
	desired := Schema{Version: 1, Tables: []TableDef{{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true}}, // notNull absent
	}}}
	actual := Schema{Version: 1, Tables: []TableDef{{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true, NotNull: true}},
	}}}
	result, err := DiffSchema(desired, actual, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(result.Up, "\n")
	if strings.Contains(joined, "drop not null") {
		t.Fatalf("PK implies NOT NULL — no drop-not-null may be planned: %v", result.Up)
	}
	if len(result.Up) != 0 {
		t.Fatalf("schemas must be considered in sync, got %v", result.Up)
	}
}

func TestDiffUnsupportedTypeColumnDropHasHonestDown(t *testing.T) {
	// I3: dropping a column whose actual type is unsupported (with intent)
	// must not emit down SQL that cannot re-apply; mark it irreversible.
	desired := Schema{Version: 1, Tables: []TableDef{{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "serial", PrimaryKey: true}},
	}}}
	actual := Schema{Version: 1, Tables: []TableDef{{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "tags", Type: "ARRAY"},
		},
	}}}
	result, err := DiffSchema(desired, actual, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(result.Up, "\n"), `drop column if exists "tags"`) {
		t.Fatalf("drop must still be planned with acknowledgement, got %v", result.Up)
	}
	down := strings.Join(result.Down, "\n")
	if strings.Contains(down, "add column") {
		t.Fatalf("down must not try to re-create the unsupported type, got: %s", down)
	}
	if !strings.Contains(down, "IRREVERSIBLE") || !strings.Contains(down, "ARRAY") {
		t.Fatalf("down must carry an explicit irreversible marker naming the type, got: %s", down)
	}
	warned := false
	for _, w := range result.Warnings {
		if strings.Contains(w, `unsupported type "ARRAY"`) {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("expected irreversibility warning, got %v", result.Warnings)
	}
}

func TestDiffUnsupportedTypeTableDropHasHonestDown(t *testing.T) {
	actual := Schema{Version: 1, Tables: []TableDef{{
		Name: "legacy",
		Columns: []ColumnDef{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "vals", Type: "ARRAY"},
		},
	}}}
	result, err := DiffSchema(Schema{Version: 1}, actual, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(result.Up, "\n"), `drop table if exists "legacy"`) {
		t.Fatalf("table drop must still be planned, got %v", result.Up)
	}
	down := strings.Join(result.Down, "\n")
	if strings.Contains(down, "create table") {
		t.Fatalf("down must not re-create a table with unsupported column types, got: %s", down)
	}
	if !strings.Contains(down, "IRREVERSIBLE") {
		t.Fatalf("down must carry an explicit irreversible marker, got: %s", down)
	}
}
