package db

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// usersWithHistoryDocJSON is what live introspection returns for a database
// with one user table and neutron's history and lock tables, plus a user
// table in another schema whose name merely resembles the internal prefix.
const usersWithHistoryDocJSON = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}, {"name": "app"}],
	"tables": [
		{
			"identity": {"schema": "public", "name": "users"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
				{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}
			],
			"constraints": [{"type": "primary-key", "name": "users_pkey", "columns": ["id"]}],
			"indexes": []
		},
		{
			"identity": {"schema": "public", "name": "_neutron_migrations"},
			"managed": true,
			"columns": [
				{"name": "version", "type": {"name": "text", "codec": "string"}, "notNull": true},
				{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}
			],
			"constraints": [{"type": "primary-key", "name": "_neutron_migrations_pkey", "columns": ["version"]}],
			"indexes": []
		},
		{
			"identity": {"schema": "app", "name": "_neutron_migration_lock"},
			"managed": true,
			"columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
			"constraints": [],
			"indexes": []
		},
		{
			"identity": {"schema": "app", "name": "neutron_notes"},
			"managed": true,
			"columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
			"constraints": [],
			"indexes": []
		}
	],
	"enums": [], "views": [], "opaque": []
}`

func tableNames(t *testing.T, doc *V2Document) []string {
	t.Helper()
	m, err := ModelFromRoot(doc.Root)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, tb := range m.Tables {
		out = append(out, tb.Identity.String())
	}
	return out
}

func tableJSON(t *testing.T, doc *V2Document, id V2Identity) string {
	t.Helper()
	m, err := ModelFromRoot(doc.Root)
	if err != nil {
		t.Fatal(err)
	}
	tb := m.Table(id)
	if tb == nil {
		t.Fatalf("table %s missing", id)
	}
	raw, err := json.Marshal(tb)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestWithoutInternalMetadataRemovesOnlyProtectedTables(t *testing.T) {
	full := testV2Doc(t, usersWithHistoryDocJSON)
	out, removed, err := WithoutInternalMetadata(full)
	if err != nil {
		t.Fatal(err)
	}
	if got := IdentityList(removed); got != "app._neutron_migration_lock, public._neutron_migrations" {
		t.Fatalf("removed = %q", got)
	}
	if got := strings.Join(tableNames(t, out), ","); got != "app.neutron_notes,public.users" {
		t.Fatalf("kept tables = %q", got)
	}
	for _, id := range []V2Identity{{Schema: "public", Name: "users"}, {Schema: "app", Name: "neutron_notes"}} {
		if tableJSON(t, out, id) != tableJSON(t, full, id) {
			t.Fatalf("table %s changed", id)
		}
	}
	// The result is a document the diff accepts as a desired state.
	if _, err := DiffV2Document(context.Background(), out, full, DiffV2Options{SnapshotBase: true}); err != nil {
		t.Fatalf("filtered document refused as desired: %v", err)
	}
	if _, err := DiffV2Document(context.Background(), full, full, DiffV2Options{SnapshotBase: true}); err == nil || !strings.Contains(err.Error(), "neutron-internal metadata") {
		t.Fatalf("the unfiltered document must be refused as desired (control): %v", err)
	}

	clean := testV2Doc(t, usersDocJSON)
	same, removed, err := WithoutInternalMetadata(clean)
	if err != nil || len(removed) != 0 || same != clean {
		t.Fatalf("a document without internal tables must come back as is: %v %v", removed, err)
	}
}

// writePreM07Baseline writes a baseline the way `schema baseline` did before
// M07: the whole introspected document, internal tables included.
func writePreM07Baseline(t *testing.T, dir string, doc *V2Document) string {
	t.Helper()
	content, err := MarshalSnapshotJSON(BaselineSnapshotFor(doc, nil, BaselineHistory{Shape: "v2-text", AppliedVersions: []string{}}))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, SnapshotDir, BaselineVersion+"_"+BaselineName+".snapshot.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSnapshotChainIgnoresInternalTablesInPreM07Baseline(t *testing.T) {
	dir := t.TempDir()
	full := testV2Doc(t, usersWithHistoryDocJSON)
	path := writePreM07Baseline(t, dir, full)
	onDisk := dirFileBytes(t, path)

	chain, err := LoadSnapshotChain(dir)
	if err != nil {
		t.Fatalf("pre-M07 baseline must load: %v", err)
	}
	if got := IdentityList(chain.BaselineInternal); got != "app._neutron_migration_lock, public._neutron_migrations" {
		t.Fatalf("BaselineInternal = %q", got)
	}
	// The recorded hash still anchors the chain.
	if chain.RootSHA256 != full.SHA256Hex || chain.HeadSHA256 != full.SHA256Hex {
		t.Fatalf("chain must keep the recorded baseline hash")
	}
	head, err := chain.HeadDocument()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(tableNames(t, head), ","); got != "app.neutron_notes,public.users" {
		t.Fatalf("loaded baseline tables = %q", got)
	}
	// As the expected state of a live check: in sync with a catalog that
	// still has the internal tables, and user-table drift is still drift.
	res, err := DiffV2Document(context.Background(), head, full, DiffV2Options{AllowDestructive: true})
	if err != nil || len(res.Up) != 0 {
		t.Fatalf("filtered baseline vs full catalog: up=%v err=%v", res.Up, err)
	}
	drifted := testV2Doc(t, strings.Replace(usersWithHistoryDocJSON,
		`{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}`,
		`{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true},
				{"name": "rogue", "type": {"name": "int4", "codec": "number"}, "notNull": false}`, 1))
	res, err = DiffV2Document(context.Background(), head, drifted, DiffV2Options{AllowDestructive: true})
	if err != nil || len(res.Up) != 1 || !strings.Contains(res.Up[0], "rogue") {
		t.Fatalf("user-table drift against the baseline must be reported: up=%v err=%v", res.Up, err)
	}

	// A migration planned from it chains from the recorded hash and reloads.
	plan, err := generateIntoDir(t, dir, "add_posts", testV2Doc(t, usersAndPostsDocJSON), nil, false)
	if err != nil || plan == nil {
		t.Fatalf("generate from pre-M07 baseline: %v", err)
	}
	if plan.BaseSHA256 != full.SHA256Hex {
		t.Fatalf("plan base = %s, want the recorded baseline hash", plan.BaseSHA256)
	}
	if _, err := LoadSnapshotChain(dir); err != nil {
		t.Fatalf("chain after generate: %v", err)
	}
	if string(dirFileBytes(t, path)) != string(onDisk) {
		t.Fatalf("loading must never rewrite the baseline file")
	}

	// Integrity is still checked on the file as written.
	tampered := strings.Replace(string(onDisk), `"users_pkey"`, `"users_pkey2"`, 1)
	if tampered == string(onDisk) {
		t.Fatal("tamper edit did not apply")
	}
	if err := os.WriteFile(path, []byte(tampered), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSnapshotChain(dir); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("edited baseline must still be refused as corrupt: %v", err)
	}
}

// Only the baseline is read leniently: a migration snapshot that lists an
// internal table is not something the CLI ever wrote, and stays refused.
func TestSnapshotChainKeepsInternalTablesInMigrationSnapshots(t *testing.T) {
	dir := t.TempDir()
	target := testV2Doc(t, usersWithHistoryDocJSON)
	empty, err := EmptyDocumentSHA256()
	if err != nil {
		t.Fatal(err)
	}
	res := DiffResult{Up: []string{"create table users (id int4)"}, Down: []string{"drop table users"}}
	plan, err := BuildPlanArtifact("001", "crafted", "empty", empty, target, nil, res)
	if err != nil {
		t.Fatal(err)
	}
	files, err := MigrationArtifactSet(dir, "001", "crafted", plan, target, res.Up[0]+";", res.Down[0]+";")
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteArtifactSet(files); err != nil {
		t.Fatal(err)
	}
	chain, err := LoadSnapshotChain(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain.BaselineInternal) != 0 {
		t.Fatalf("no baseline, nothing ignored: %v", chain.BaselineInternal)
	}
	head, err := chain.HeadDocument()
	if err != nil {
		t.Fatal(err)
	}
	if head.SHA256Hex != target.SHA256Hex {
		t.Fatalf("a migration snapshot's document must load unchanged")
	}
	if _, err := DiffV2Document(context.Background(), head, target, DiffV2Options{AllowDestructive: true}); err == nil || !strings.Contains(err.Error(), "neutron-internal metadata") {
		t.Fatalf("a migration snapshot listing an internal table must stay refused: %v", err)
	}
}
