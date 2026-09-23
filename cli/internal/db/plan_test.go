package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testV2Doc parses a raw document and fails the test on invalid input.
func testV2Doc(t *testing.T, raw string) *V2Document {
	t.Helper()
	doc, err := ParseV2Document([]byte(raw))
	if err != nil {
		t.Fatalf("parse v2 document: %v", err)
	}
	return doc
}

const usersDocJSON = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}],
	"tables": [{
		"identity": {"schema": "public", "name": "users"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}
		],
		"constraints": [{"type": "primary-key", "name": "users_pkey", "columns": ["id"]}],
		"indexes": []
	}],
	"enums": [], "views": [], "opaque": []
}`

// usersAndPostsDoc adds a posts table to the users document.
const usersAndPostsDocJSON = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}],
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
			"identity": {"schema": "public", "name": "posts"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
				{"name": "title", "type": {"name": "text", "codec": "string"}, "notNull": true},
				{"name": "author_id", "type": {"name": "int4", "codec": "number"}, "notNull": true}
			],
			"constraints": [
				{"type": "primary-key", "name": "posts_pkey", "columns": ["id"]},
				{"type": "foreign-key", "name": "posts_author_fkey", "columns": ["author_id"], "references": {"table": {"schema": "public", "name": "users"}, "columns": ["id"]}}
			],
			"indexes": []
		}
	],
	"enums": [], "views": [], "opaque": []
}`

func TestEmptyV2DocumentIsStableAndValid(t *testing.T) {
	doc, err := EmptyV2Document()
	if err != nil {
		t.Fatalf("empty document: %v", err)
	}
	hash, err := EmptyDocumentSHA256()
	if err != nil {
		t.Fatalf("empty hash: %v", err)
	}
	if hash != doc.SHA256Hex {
		t.Fatalf("empty document hash drifted between calls")
	}
	again, _ := EmptyV2Document()
	if string(again.Canonical) != string(doc.Canonical) {
		t.Fatalf("empty document bytes not deterministic")
	}
}

// generateIntoDir runs the offline snapshot generation used by the CLI
// command against a temp migrations directory, so chain behavior can be
// exercised without spawning cobra.
func generateIntoDir(t *testing.T, dir, name string, desired *V2Document, renames map[string]string, allowDestructive bool) (*PlanArtifact, error) {
	t.Helper()
	chain, err := LoadSnapshotChain(dir)
	if err != nil {
		return nil, err
	}
	base, err := chain.HeadDocument()
	if err != nil {
		return nil, err
	}
	result, err := DiffV2Document(context.Background(), desired, base, DiffV2Options{
		Renames:          renames,
		AllowDestructive: allowDestructive,
		SnapshotBase:     true, // mirrors the CLI's snapshot-mode invocation
	})
	if err != nil {
		return nil, err
	}
	if len(result.Up) == 0 {
		return nil, nil
	}
	version, err := NextMigrationVersion(dir)
	if err != nil {
		return nil, err
	}
	plan, err := BuildPlanArtifact(version, name, chain.HeadRef, chain.HeadSHA256, desired, renames, result)
	if err != nil {
		return nil, err
	}
	upSQL := strings.Join(result.Up, ";\n") + ";"
	downSQL := strings.Join(reverseSQL(result.Down), ";\n") + ";"
	files, err := MigrationArtifactSet(dir, version, name, plan, desired, upSQL, downSQL)
	if err != nil {
		return nil, err
	}
	if err := WriteArtifactSet(files); err != nil {
		return nil, err
	}
	return plan, nil
}

func reverseSQL(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[len(in)-1-i] = s
	}
	return out
}

func dirFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func TestSnapshotChainTwoPendingMigrations(t *testing.T) {
	dir := t.TempDir()

	first, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false)
	if err != nil {
		t.Fatalf("first generate: %v", err)
	}
	if first == nil {
		t.Fatalf("first generate produced no plan")
	}
	if first.MigrationVersion != "001" {
		t.Fatalf("first version = %q, want 001", first.MigrationVersion)
	}
	if first.BaseSource != "empty" {
		t.Fatalf("first base source = %q, want empty", first.BaseSource)
	}

	// The V11 core scenario: generate a SECOND migration without applying
	// the first — it must plan against the first's snapshot, so the up SQL
	// contains ONLY the delta (posts), never users DDL again.
	second, err := generateIntoDir(t, dir, "add_posts", testV2Doc(t, usersAndPostsDocJSON), nil, false)
	if err != nil {
		t.Fatalf("second generate: %v", err)
	}
	if second == nil {
		t.Fatalf("second generate produced no plan")
	}
	if second.MigrationVersion != "002" {
		t.Fatalf("second version = %q, want 002", second.MigrationVersion)
	}
	if second.BaseSource != "001_add_users" {
		t.Fatalf("second base source = %q, want 001_add_users", second.BaseSource)
	}
	up := string(dirFileBytes(t, filepath.Join(dir, "002_add_posts.up.sql")))
	if strings.Contains(up, `"users"`) && strings.Contains(strings.ToLower(up), "create table") {
		// users appears in the FK reference clause; only a second CREATE TABLE would be wrong.
		if strings.Count(strings.ToLower(up), "create table") != 1 {
			t.Fatalf("second migration re-plans the first: %s", up)
		}
	}
	if !strings.Contains(up, `"posts"`) {
		t.Fatalf("second migration lacks posts DDL: %s", up)
	}

	// The chain must load cleanly with both pending.
	chain, err := LoadSnapshotChain(dir)
	if err != nil {
		t.Fatalf("chain after two pending: %v", err)
	}
	if len(chain.Snapshots) != 2 || chain.HeadRef != "002_add_posts" {
		t.Fatalf("chain head = %q, snapshots = %d", chain.HeadRef, len(chain.Snapshots))
	}
}

func TestSnapshotGenerationIsDeterministic(t *testing.T) {
	// Two developers, equal inputs: byte-identical artifacts.
	var sets [][]string
	for i := 0; i < 2; i++ {
		dir := t.TempDir()
		if _, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
		var files []string
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			files = append(files, fmt.Sprintf("%s=%s", e.Name(), data))
		}
		snap, _ := os.ReadFile(filepath.Join(dir, SnapshotDir, "001_add_users.snapshot.json"))
		files = append(files, "snapshot="+string(snap))
		sets = append(sets, files)
	}
	if strings.Join(sets[0], "\x00") != strings.Join(sets[1], "\x00") {
		t.Fatalf("equal inputs produced different artifacts")
	}
}

func TestSnapshotChainDivergentBranchesConflict(t *testing.T) {
	base := t.TempDir()
	if _, err := generateIntoDir(t, base, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base generate: %v", err)
	}

	// Two branches from the same state, different second migrations.
	branchA := t.TempDir()
	branchB := t.TempDir()
	copyDir(t, base, branchA)
	copyDir(t, base, branchB)
	if _, err := generateIntoDir(t, branchA, "add_posts", testV2Doc(t, usersAndPostsDocJSON), nil, false); err != nil {
		t.Fatalf("branch A: %v", err)
	}
	bDoc := testV2Doc(t, usersDocJSON)
	// branch B adds a column instead
	bDoc2Raw := strings.Replace(usersDocJSON,
		`{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}`,
		`{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}, {"name": "email", "type": {"name": "text", "codec": "string"}, "notNull": false}`,
		1)
	bDoc = testV2Doc(t, bDoc2Raw)
	if _, err := generateIntoDir(t, branchB, "add_email", bDoc, nil, false); err != nil {
		t.Fatalf("branch B: %v", err)
	}

	// Merge both branches' files into one directory: generation and even
	// chain LOADING must refuse, never pick a winner.
	merged := t.TempDir()
	copyDir(t, branchA, merged)
	copyDir(t, branchB, merged)

	_, err := LoadSnapshotChain(merged)
	if err == nil {
		t.Fatalf("divergent branches loaded without error")
	}
	if !strings.Contains(err.Error(), "divergent") || !strings.Contains(err.Error(), "002_add_posts") || !strings.Contains(err.Error(), "002_add_email") {
		t.Fatalf("divergence error does not name both branches: %v", err)
	}
	if _, err := generateIntoDir(t, merged, "next", testV2Doc(t, usersAndPostsDocJSON), nil, false); err == nil {
		t.Fatalf("generate on divergent chain succeeded")
	}
}

func TestSnapshotChainSameIDCollision(t *testing.T) {
	base := t.TempDir()
	if _, err := generateIntoDir(t, base, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base: %v", err)
	}
	// Two snapshots claiming version 002 with different names — the
	// numeric-collision rule (distinct text, same value) must fire.
	if err := os.Rename(
		filepath.Join(base, SnapshotDir, "001_add_users.snapshot.json"),
		filepath.Join(base, SnapshotDir, "002_other.snapshot.json"),
	); err != nil {
		t.Fatal(err)
	}
	_, err := LoadSnapshotChain(base)
	if err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("numeric collision not detected: %v", err)
	}
}

func TestSnapshotChainDuplicateVersion(t *testing.T) {
	base := t.TempDir()
	if _, err := generateIntoDir(t, base, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base: %v", err)
	}
	// A second, internally-consistent snapshot with the SAME version text
	// under another name: two snapshots claim "001". (The copy's recorded
	// name matches its filename; only the version clashes.)
	src := dirFileBytes(t, filepath.Join(base, SnapshotDir, "001_add_users.snapshot.json"))
	dup := filepath.Join(base, SnapshotDir, "001_add_users_copy.snapshot.json")
	if err := os.WriteFile(dup, bytes.ReplaceAll(src, []byte(`"add_users"`), []byte(`"add_users_copy"`)), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadSnapshotChain(base)
	if err == nil || !strings.Contains(err.Error(), "claimed by two snapshots") {
		t.Fatalf("duplicate version not detected: %v", err)
	}
}

func TestSnapshotChainHandwrittenMigrationWithoutSnapshot(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "001_manual.up.sql"), []byte("-- hand written\nCREATE TABLE x (id int);\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadSnapshotChain(dir)
	if err == nil || !strings.Contains(err.Error(), "no snapshot") {
		t.Fatalf("hand-written migration not refused: %v", err)
	}

	// A baseline covering the file legitimizes it.
	baselineDoc := testV2Doc(t, usersDocJSON)
	snap := BaselineSnapshotFor(baselineDoc, []string{"001"}, BaselineHistory{Shape: "absent"})
	content, err := MarshalSnapshotJSON(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, SnapshotDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, SnapshotDir, "000_baseline.snapshot.json"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	chain, err := LoadSnapshotChain(dir)
	if err != nil {
		t.Fatalf("covered migration still refused: %v", err)
	}
	if !chain.Covers["001"] {
		t.Fatalf("covers missing 001")
	}
}

// TestSnapshotChainMergeResidueSameVersionRefused reproduces review-1
// MAJOR-1: two branches each generate their own 002 from a shared 001, and
// a hand merge keeps ONE branch's snapshot but BOTH branches' sql files.
// The un-snapshotted 002 is a pending migration the runner would apply on
// top of the planned state — the chain must refuse the directory loudly,
// never silently plan 003 around the orphan.
func TestSnapshotChainMergeResidueSameVersionRefused(t *testing.T) {
	base := t.TempDir()
	if _, err := generateIntoDir(t, base, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base generate: %v", err)
	}

	branchA := t.TempDir()
	branchB := t.TempDir()
	copyDir(t, base, branchA)
	copyDir(t, base, branchB)
	if _, err := generateIntoDir(t, branchA, "add_posts", testV2Doc(t, usersAndPostsDocJSON), nil, false); err != nil {
		t.Fatalf("branch A: %v", err)
	}
	emailDoc := strings.Replace(usersDocJSON,
		`{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}`,
		`{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}, {"name": "email", "type": {"name": "text", "codec": "string"}, "notNull": false}`,
		1)
	if _, err := generateIntoDir(t, branchB, "add_users_email", testV2Doc(t, emailDoc), nil, false); err != nil {
		t.Fatalf("branch B: %v", err)
	}

	// Merge residue: 001 full set, branch B's full 002 set INCLUDING its
	// snapshot, branch A's 002 sql/plan files WITHOUT its snapshot.
	merged := t.TempDir()
	copyDir(t, base, merged)
	copyDir(t, branchB, merged)
	for _, f := range []string{"002_add_posts.up.sql", "002_add_posts.down.sql", "002_add_posts.plan.json"} {
		if err := os.WriteFile(filepath.Join(merged, f), dirFileBytes(t, filepath.Join(branchA, f)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	_, err := LoadSnapshotChain(merged)
	if err == nil {
		t.Fatalf("merge-residue directory loaded cleanly — the un-snapshotted 002 would be invisible to planning")
	}
	if !strings.Contains(err.Error(), "002_add_posts") || !strings.Contains(err.Error(), "002_add_users_email") {
		t.Fatalf("error does not name both same-version migrations (orphan included): %v", err)
	}
	if _, err := generateIntoDir(t, merged, "next", testV2Doc(t, usersAndPostsDocJSON), nil, false); err == nil {
		t.Fatalf("generate planned around merge residue")
	}
	if _, statErr := os.Stat(filepath.Join(merged, "003_next.up.sql")); !os.IsNotExist(statErr) {
		t.Fatalf("failed generate wrote artifacts")
	}
}

// TestSnapshotChainCoverageIsStemKeyed: coverage between .up.sql files and
// snapshots must key on the full stem, not the version text — a same-
// version different-name snapshot never covers a file (review-1 MAJOR-1,
// single-file variant), and a snapshot without its own file cannot anchor
// the chain even when a same-version stand-in exists.
func TestSnapshotChainCoverageIsStemKeyed(t *testing.T) {
	dir := t.TempDir()
	if _, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base: %v", err)
	}
	if _, err := generateIntoDir(t, dir, "add_posts", testV2Doc(t, usersAndPostsDocJSON), nil, false); err != nil {
		t.Fatalf("second: %v", err)
	}

	// Rename ONLY the 002 up file: same version, different stem. The
	// snapshot 002_add_posts must not cover 002_remerged, and 002_remerged
	// must not anchor the snapshot.
	if err := os.Rename(filepath.Join(dir, "002_add_posts.up.sql"), filepath.Join(dir, "002_remerged.up.sql")); err != nil {
		t.Fatal(err)
	}
	_, err := LoadSnapshotChain(dir)
	if err == nil || !strings.Contains(err.Error(), "002_remerged") || !strings.Contains(err.Error(), "002_add_posts") {
		t.Fatalf("stem mismatch not reported naming both identities: %v", err)
	}

	// Restore, then delete the up file outright: the snapshot is unanchored.
	if err := os.Rename(filepath.Join(dir, "002_remerged.up.sql"), filepath.Join(dir, "002_add_posts.up.sql")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "002_add_posts.up.sql")); err != nil {
		t.Fatal(err)
	}
	_, err = LoadSnapshotChain(dir)
	if err == nil || !strings.Contains(err.Error(), "no matching .up.sql") {
		t.Fatalf("unanchored snapshot accepted: %v", err)
	}
}

// TestSnapshotModeWordNamesPlanningBase: offline snapshot-mode messages
// must name the planning base, not "the database"/"the live table"
// (review-1 LOW-1) — while the live-mode default wording stays
// byte-stable.
func TestSnapshotModeWordNamesPlanningBase(t *testing.T) {
	dir := t.TempDir()
	plan, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false)
	if err != nil || plan == nil {
		t.Fatalf("generate: %v", err)
	}
	found := false
	for _, c := range plan.Caveats {
		if strings.Contains(c, "database") || strings.Contains(c, "live table") || strings.Contains(c, "live type") {
			t.Fatalf("snapshot-mode caveat carries live wording: %q", c)
		}
		if strings.Contains(c, "planning base (snapshot)") {
			found = true
		}
	}
	if !found {
		t.Fatalf("planning-base wording missing from caveats: %v", plan.Caveats)
	}
	planBytes := dirFileBytes(t, filepath.Join(dir, "001_add_users.plan.json"))
	if strings.Contains(string(planBytes), "does not exist in the database") {
		t.Fatalf("plan.json caveat carries live wording")
	}

	// The column-order rejection in snapshot mode names the planning base.
	dir2 := t.TempDir()
	if _, err := generateIntoDir(t, dir2, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base: %v", err)
	}
	reordered := strings.Replace(usersDocJSON,
		`{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}`,
		`{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true},
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}`,
		1)
	_, err = generateIntoDir(t, dir2, "reorder", testV2Doc(t, reordered), nil, false)
	if err == nil || !strings.Contains(err.Error(), "planning-base table") {
		t.Fatalf("snapshot-mode reorder rejection does not name the planning base: %v", err)
	}

	// Live-mode default wording is byte-stable: the same fresh-chain diff
	// with default options keeps the historical database wording.
	empty, err := EmptyV2Document()
	if err != nil {
		t.Fatal(err)
	}
	res, err := DiffV2Document(context.Background(), testV2Doc(t, usersDocJSON), empty, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	liveWording := false
	for _, w := range res.Warnings {
		if strings.Contains(w, `does not exist in the database: it will be created`) {
			liveWording = true
		}
	}
	if !liveWording {
		t.Fatalf("live-mode default wording changed: %v", res.Warnings)
	}
}

func TestSnapshotChainTamperedTargetHash(t *testing.T) {
	dir := t.TempDir()
	if _, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("generate: %v", err)
	}
	path := filepath.Join(dir, SnapshotDir, "001_add_users.snapshot.json")
	raw := dirFileBytes(t, path)
	var snap struct {
		TargetSHA256 string `json:"targetSha256"`
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(raw), snap.TargetSHA256, strings.Repeat("0", 64), 1)
	if err := os.WriteFile(path, []byte(tampered), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadSnapshotChain(dir)
	if err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("tampered target hash not detected: %v", err)
	}
}

func TestColumnReorderRejectedAtPlanTime(t *testing.T) {
	dir := t.TempDir()
	if _, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base: %v", err)
	}
	// Desired document with the SAME columns in swapped order: PostgreSQL
	// cannot reorder without a rewrite; the planner must reject, never
	// encode (M01 consumer contract).
	reordered := strings.Replace(usersDocJSON,
		`{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true}`,
		`{"name": "name", "type": {"name": "text", "codec": "string"}, "notNull": true},
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}`,
		1)
	_, err := generateIntoDir(t, dir, "reorder", testV2Doc(t, reordered), nil, false)
	if err == nil {
		t.Fatalf("column reorder was planned instead of rejected")
	}
	if !strings.Contains(err.Error(), "reorder") && !strings.Contains(err.Error(), "order") {
		t.Fatalf("reorder error does not explain the cause: %v", err)
	}
	// Nothing was written by the failed run.
	if _, statErr := os.Stat(filepath.Join(dir, "002_reorder.up.sql")); !os.IsNotExist(statErr) {
		t.Fatalf("failed reorder run left artifacts behind")
	}
}

func TestEquivalenceCaveatsRecordedInPlan(t *testing.T) {
	dir := t.TempDir()
	withCheck := strings.Replace(usersDocJSON,
		`"constraints": [{"type": "primary-key", "name": "users_pkey", "columns": ["id"]}],`,
		`"constraints": [{"type": "primary-key", "name": "users_pkey", "columns": ["id"]}, {"type": "check", "name": "users_name_check", "expression": "char_length(name) > 0"}],`,
		1)
	if _, err := generateIntoDir(t, dir, "init", testV2Doc(t, withCheck), nil, false); err != nil {
		t.Fatalf("base: %v", err)
	}
	// Respell the check expression: offline comparison has no catalog
	// oracle, so the change is planned WITH an explicit caveat.
	respled := strings.Replace(withCheck, "char_length(name) > 0", "char_length((name)) > 0", 1)
	plan, err := generateIntoDir(t, dir, "respell_check", testV2Doc(t, respled), nil, false)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	found := false
	for _, c := range plan.Caveats {
		if strings.Contains(c, "equivalence not verified") {
			found = true
		}
	}
	if !found {
		t.Fatalf("equivalence caveat missing from plan artifact: %v", plan.Caveats)
	}
	// The caveat must also be in the written artifact bytes.
	planBytes := dirFileBytes(t, filepath.Join(dir, "002_respell_check.plan.json"))
	if !strings.Contains(string(planBytes), "equivalence not verified") {
		t.Fatalf("caveat missing from written plan.json")
	}
}

func TestRenameValidatedAgainstSnapshot(t *testing.T) {
	dir := t.TempDir()
	if _, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base: %v", err)
	}
	renamed := strings.Replace(usersDocJSON, `"name": "name"`, `"name": "full_name"`, 1)
	renamed = strings.Replace(renamed, `{"schema": "public", "name": "users"}`, `{"schema": "public", "name": "users"}`, 1)

	// Invalid rename intent: source column does not exist in the base.
	_, err := generateIntoDir(t, dir, "rename_bad", testV2Doc(t, renamed), map[string]string{"public.users.full_name": "nonexistent"}, false)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("forged rename accepted: %v", err)
	}

	// Valid rename: planned and recorded in the artifact.
	plan, err := generateIntoDir(t, dir, "rename", testV2Doc(t, renamed), map[string]string{"public.users.full_name": "name"}, false)
	if err != nil {
		t.Fatalf("valid rename rejected: %v", err)
	}
	if len(plan.Renames) != 1 || plan.Renames[0] != "public.users.name>public.users.full_name" {
		t.Fatalf("renames not recorded: %v", plan.Renames)
	}
	up := string(dirFileBytes(t, filepath.Join(dir, "002_rename.up.sql")))
	if !strings.Contains(up, "rename column") {
		t.Fatalf("rename SQL missing: %s", up)
	}
}

func TestRiskClassificationDestructiveAndDataLoss(t *testing.T) {
	dir := t.TempDir()
	if _, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatalf("base: %v", err)
	}
	// Desired document that drops the users table (empty managed scope for
	// public) — with --allow-destructive the plan must classify the drop.
	empty := testV2Doc(t, `{"version":2,"dialect":"postgresql","capabilities":[],"schemas":[{"name":"public"}],"tables":[],"enums":[],"views":[],"opaque":[]}`)
	plan, err := generateIntoDir(t, dir, "drop_users", empty, nil, true)
	if err != nil {
		t.Fatalf("destructive generate: %v", err)
	}
	if !plan.Risk.HasDestructive || !plan.Risk.HasDataLoss {
		t.Fatalf("drop not classified: %+v", plan.Risk)
	}
	if plan.Risk.OverallReversibility != ReversibilityReversible {
		// The drop carries a structural down (re-create) — reversible in
		// structure. Down SQL is not data restoration; the dataLoss flag
		// says the rows are gone. Pin that vocabulary.
		t.Fatalf("drop with structural down should be structurally reversible: %+v", plan.Risk.OverallReversibility)
	}
	var dropOp *PlanOperation
	for i := range plan.Operations {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(plan.Operations[i].SQL)), "drop table") {
			dropOp = &plan.Operations[i]
		}
	}
	if dropOp == nil {
		t.Fatalf("no drop operation in plan: %+v", plan.Operations)
	}
	if !dropOp.Destructive || !dropOp.DataLoss {
		t.Fatalf("drop op flags wrong: %+v", dropOp)
	}
}

func TestRiskClassificationCommentOnlyDownIsIrreversible(t *testing.T) {
	ops := []PlanOperation{
		{SQL: "drop table if exists x", Down: "-- IRREVERSIBLE: unrepresentable", Destructive: true, DataLoss: true},
	}
	risk := summarizeRisk(ops)
	if risk.OverallReversibility != ReversibilityIrreversible || risk.IrreversibleCount != 1 {
		t.Fatalf("comment-only down should be irreversible: %+v", risk)
	}
}

func TestWriteArtifactSetNeverOverwritesAndRollsBack(t *testing.T) {
	dir := t.TempDir()
	files := []ArtifactFile{
		{Path: filepath.Join(dir, "a.txt"), Content: []byte("a")},
		{Path: filepath.Join(dir, "sub", "b.txt"), Content: []byte("b")},
	}
	if err := WriteArtifactSet(files); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// A pre-existing target refuses the whole set (branch conflict), and
	// nothing partial remains from the failed attempt.
	files2 := []ArtifactFile{
		{Path: filepath.Join(dir, "c.txt"), Content: []byte("c")},
		{Path: filepath.Join(dir, "a.txt"), Content: []byte("a2")},
	}
	err := WriteArtifactSet(files2)
	if err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("overwrite accepted: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "c.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("failed set left partial file c.txt behind")
	}
	if string(dirFileBytes(t, filepath.Join(dir, "a.txt"))) != "a" {
		t.Fatalf("existing artifact was modified by a failed set")
	}
	assertNoTempFiles(t, dir)
}

func TestWriteArtifactSetStagingFailureLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	// A directory blocking a temp-file target forces staging failure.
	if err := os.MkdirAll(filepath.Join(dir, "blocker"), 0o755); err != nil {
		t.Fatal(err)
	}
	files := []ArtifactFile{
		{Path: filepath.Join(dir, "first.txt"), Content: []byte("1")},
		{Path: filepath.Join(dir, "blocker"), Content: []byte("not a dir anymore")},
	}
	if err := WriteArtifactSet(files); err == nil {
		t.Fatalf("staging failure not surfaced")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "first.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("staging failure left first.txt behind")
	}
	assertNoTempFiles(t, dir)
}

func assertNoTempFiles(t *testing.T, root string) {
	t.Helper()
	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			name := d.Name()
			if strings.HasPrefix(name, ".neutron-") {
				t.Fatalf("temp file left behind: %s", path)
			}
		}
		return nil
	})
}

func TestNextMigrationVersionAllocatesPastSnapshots(t *testing.T) {
	dir := t.TempDir()
	if _, err := generateIntoDir(t, dir, "add_users", testV2Doc(t, usersDocJSON), nil, false); err != nil {
		t.Fatal(err)
	}
	// Snapshots with no up file (deleted file) still reserve the version.
	v, err := NextMigrationVersion(dir)
	if err != nil || v != "002" {
		t.Fatalf("next version = %q err %v, want 002", v, err)
	}
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}
