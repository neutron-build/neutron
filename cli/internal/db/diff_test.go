package db

import (
	"strings"
	"testing"
)

func strPtr(s string) *string { return &s }

func desiredUsers() TableDef {
	return TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "email", Type: "varchar", VarcharLength: 255, NotNull: true, Unique: true},
			{Name: "name", Type: "text"},
		},
	}
}

func TestDiffCreateTable(t *testing.T) {
	desired := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}
	actual := Schema{Version: 1}
	result, err := DiffSchema(desired, actual, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Up) == 0 {
		t.Fatal("expected up statements")
	}
	if !strings.Contains(result.Up[0], `create table "users"`) {
		t.Fatalf("unexpected up[0]: %s", result.Up[0])
	}
	if !strings.Contains(result.Up[0], `"email" varchar(255) not null unique`) {
		t.Fatalf("column DDL missing: %s", result.Up[0])
	}
	if result.Down[0] != `drop table if exists "users"` {
		t.Fatalf("unexpected down[0]: %s", result.Down[0])
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", result.Warnings)
	}
}

func TestDiffDropTableWarns(t *testing.T) {
	desired := Schema{Version: 1}
	actual := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}

	// Default: dropping an unmanaged table requires explicit destructive
	// acknowledgement; omission alone must not produce a DROP.
	result, err := DiffSchema(desired, actual, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Up) != 0 {
		t.Fatalf("implicit drop must not be planned without destructive acknowledgement: %v", result.Up)
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], `"users" exists in the database`) || !strings.Contains(result.Warnings[0], "--allow-destructive") {
		t.Fatalf("expected left-untouched warning naming the flag, got %v", result.Warnings)
	}

	// With the acknowledgement the drop is planned and warned as data loss.
	result, err = DiffSchema(desired, actual, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], `"users" exists in the database`) || !strings.Contains(result.Warnings[0], "all rows lost") {
		t.Fatalf("expected data-loss warning, got %v", result.Warnings)
	}
	if result.Up[0] != `drop table if exists "users"` {
		t.Fatalf("unexpected up[0]: %s", result.Up[0])
	}
	if !strings.Contains(result.Down[0], `create table "users"`) {
		t.Fatalf("down should recreate: %s", result.Down[0])
	}
}

func TestDiffAddColumn(t *testing.T) {
	desired := desiredUsers()
	desired.Columns = append(desired.Columns, ColumnDef{Name: "age", Type: "integer"})
	d := Schema{Version: 1, Tables: []TableDef{desired}}
	a := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}
	result, err := DiffSchema(d, a, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, stmt := range result.Up {
		if strings.Contains(stmt, `alter table "users" add column "age" integer`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("add column missing: %v", result.Up)
	}
}

func TestDiffRenameViaFlag(t *testing.T) {
	desired := desiredUsers()
	// "name" renamed to "full_name"
	for i := range desired.Columns {
		if desired.Columns[i].Name == "name" {
			desired.Columns[i].Name = "full_name"
		}
	}
	d := Schema{Version: 1, Tables: []TableDef{desired}}
	a := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}
	renames := map[string]string{"users.full_name": "name"}
	result, err := DiffSchema(d, a, DiffOptions{Renames: renames})
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Up) != 1 || result.Up[0] != `alter table "users" rename column "name" to "full_name"` {
		t.Fatalf("rename up wrong: %v", result.Up)
	}
	if len(result.Down) != 1 || result.Down[0] != `alter table "users" rename column "full_name" to "name"` {
		t.Fatalf("rename down wrong: %v", result.Down)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("explicit rename must not warn: %v", result.Warnings)
	}
}

func TestDiffRenameSuggestion(t *testing.T) {
	desired := desiredUsers()
	for i := range desired.Columns {
		if desired.Columns[i].Name == "name" {
			desired.Columns[i].Name = "full_name"
		}
	}
	d := Schema{Version: 1, Tables: []TableDef{desired}}
	a := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}

	// Without a rename flag the add lands; the destructive half (drop of the
	// old column) is skipped unless destructive intent is acknowledged.
	result, err := DiffSchema(d, a, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}

	warned := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "--rename") && strings.Contains(w, `"name"`) {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("expected rename suggestion, got %v", result.Warnings)
	}
	joined := strings.Join(result.Up, "\n")
	if !strings.Contains(joined, "add column") {
		t.Fatalf("expected add: %v", result.Up)
	}
	if strings.Contains(joined, "drop column") {
		t.Fatalf("drop column must not be planned without destructive acknowledgement: %v", result.Up)
	}

	// With destructive acknowledgement it degrades to add + drop.
	result, err = DiffSchema(d, a, DiffOptions{AllowDestructive: true})
	if err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(result.Up, "\n")
	if !strings.Contains(joined, "add column") || !strings.Contains(joined, "drop column") {
		t.Fatalf("expected add+drop fallback: %v", result.Up)
	}
}

func TestDiffTypeChangeUsesCast(t *testing.T) {
	desired := desiredUsers()
	desired.Columns[2].Type = "integer"
	d := Schema{Version: 1, Tables: []TableDef{desired}}
	a := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}
	result, err := DiffSchema(d, a, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}

	joined := strings.Join(result.Up, "\n")
	if !strings.Contains(joined, `alter column "name" type integer using "name"::integer`) {
		t.Fatalf("cast missing: %v", result.Up)
	}
	if len(result.Warnings) == 0 {
		t.Fatal("type change must warn")
	}
}

func TestDiffDefaultChange(t *testing.T) {
	desired := desiredUsers()
	desired.Columns[2].HasDefault = true
	v := "anon"
	desired.Columns[2].Default = &v
	d := Schema{Version: 1, Tables: []TableDef{desired}}
	a := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}
	result, err := DiffSchema(d, a, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}

	joined := strings.Join(result.Up, "\n")
	if !strings.Contains(joined, `alter column "name" set default 'anon'`) {
		t.Fatalf("set default missing: %v", result.Up)
	}
	down := strings.Join(result.Down, "\n")
	if !strings.Contains(down, `alter column "name" drop default`) {
		t.Fatalf("down drop default missing: %v", result.Down)
	}
}

func TestDiffIndexes(t *testing.T) {
	withIdx := desiredUsers()
	withIdx.Indexes = []IndexDef{{Name: "users_email_idx", Columns: []string{"email"}}}
	d := Schema{Version: 1, Tables: []TableDef{withIdx}}
	a := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}
	result, err := DiffSchema(d, a, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}

	joined := strings.Join(result.Up, "\n")
	if !strings.Contains(joined, `create index "users_email_idx" on "users" ("email")`) {
		t.Fatalf("index create missing: %v", result.Up)
	}
	if !strings.Contains(strings.Join(result.Down, "\n"), `drop index if exists "users_email_idx"`) {
		t.Fatalf("index drop missing: %v", result.Down)
	}
}

func TestDiffNoChanges(t *testing.T) {
	d := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}
	a := Schema{Version: 1, Tables: []TableDef{desiredUsers()}}
	result, err := DiffSchema(d, a, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Up) != 0 || len(result.Down) != 0 {
		t.Fatalf("expected empty diff, got up=%v down=%v", result.Up, result.Down)
	}
}

func TestColumnDDLBooleanDefault(t *testing.T) {
	c := ColumnDef{Name: "active", Type: "boolean", NotNull: true, HasDefault: true, Default: strPtr("true")}
	got := columnDDL(c, false)
	want := `"active" boolean not null default true`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestColumnDDLFK(t *testing.T) {
	c := ColumnDef{
		Name: "user_id", Type: "integer", NotNull: true,
		ForeignKey: &ForeignKeyDef{Table: "users", Column: "id", OnDelete: "cascade"},
	}
	got := columnDDL(c, true)
	if !strings.Contains(got, `references "users" ("id") on delete cascade`) {
		t.Fatalf("fk missing: %s", got)
	}
}

// X01: the v1 skip behavior is withdrawn — a vector column in a v1
// document fails validation loudly (migrations fail rather than skipping a
// column queries later depend on). This test pins the fail-closed contract.
func TestDiffVectorColumnFailsClosedInV1(t *testing.T) {
	desired := TableDef{
		Name: "docs",
		Columns: []ColumnDef{
			{Name: "id", Type: "serial", PrimaryKey: true},
			{Name: "embedding", Type: "vector", VectorDims: 1536, NucleusOnly: true},
		},
	}
	d := Schema{Version: 1, Tables: []TableDef{desired}}
	a := Schema{Version: 1}
	_, err := DiffSchema(d, a, DiffOptions{})
	if err == nil {
		t.Fatal("v1 diff must reject vector columns (fail closed) — the skip behavior was removed in X01")
	}
	if !strings.Contains(err.Error(), "vector columns require schema document v2") {
		t.Fatalf("expected the schema-document-v2 pointer, got: %v", err)
	}
	if !strings.Contains(err.Error(), "skipped column") {
		t.Fatalf("expected the why (a skipped column is later queried but nonexistent), got: %v", err)
	}

	// Same verdict without the obsolete nucleusOnly flag.
	desired.Columns[1].NucleusOnly = false
	_, err = DiffSchema(Schema{Version: 1, Tables: []TableDef{desired}}, a, DiffOptions{})
	if err == nil || !strings.Contains(err.Error(), "vector columns require schema document v2") {
		t.Fatalf("vector without nucleusOnly must fail the same way, got: %v", err)
	}
}
