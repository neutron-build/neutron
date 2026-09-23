package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/spf13/cobra"
)

// snapshotModeTestCommand builds a private migrate-generate command with
// the production flag set, so flag dispatch can be exercised without
// mutating the package-level command state.
func snapshotModeTestCommand() *cobra.Command {
	c := &cobra.Command{Use: "generate"}
	c.Flags().String("dir", "migrations", "")
	c.Flags().String("schema", "neutron.schema.json", "")
	c.Flags().String("name", "", "")
	c.Flags().StringArray("rename", nil, "")
	c.Flags().Duration("timeout", 30*time.Second, "")
	c.Flags().Bool("allow-destructive", false, "")
	c.Flags().String("mode", "", "")
	return c
}

// review-1 LOW-2: an explicitly passed --timeout must be rejected in
// snapshot mode (offline planning waits on nothing), while the default
// value stays accepted and dispatch proceeds normally.
func TestSnapshotModeRejectsExplicitTimeout(t *testing.T) {
	c := snapshotModeTestCommand()
	if err := c.ParseFlags([]string{"--mode", "snapshot", "--timeout", "5s"}); err != nil {
		t.Fatal(err)
	}
	err := runMigrateGenerate(c, nil)
	if err == nil || !strings.Contains(err.Error(), "--timeout") || !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("--timeout in snapshot mode not rejected clearly: %v", err)
	}

	// The default (unset) --timeout must not be rejected: dispatch moves
	// past the flag check and fails on the missing schema document instead.
	c2 := snapshotModeTestCommand()
	if err := c2.ParseFlags([]string{"--mode", "snapshot"}); err != nil {
		t.Fatal(err)
	}
	err = runMigrateGenerate(c2, nil)
	if err == nil || strings.Contains(err.Error(), "--timeout") {
		t.Fatalf("default --timeout wrongly rejected (or no schema error surfaced): %v", err)
	}
}

// chainFixtureDoc is a minimal valid v2 document for building chains.
const chainFixtureDoc = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}],
	"tables": [{
		"identity": {"schema": "public", "name": "users"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}
		],
		"constraints": [{"type": "primary-key", "name": "users_pkey", "columns": ["id"]}],
		"indexes": []
	}],
	"enums": [], "views": [], "opaque": []
}`

// writeChainFixture builds a loadable chain: a baseline covering 001 plus
// one migration snapshot 002 with its up file.
func writeChainFixture(t *testing.T, dir string) *db.SnapshotChain {
	t.Helper()
	doc, err := db.ParseV2Document([]byte(chainFixtureDoc))
	if err != nil {
		t.Fatalf("parse fixture doc: %v", err)
	}
	baseline := db.BaselineSnapshotFor(doc, []string{"001"}, db.BaselineHistory{Shape: "absent"})
	content, err := db.MarshalSnapshotJSON(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "snapshots"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "snapshots", "000_baseline.snapshot.json"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "001_pre.up.sql"), []byte("-- covered by baseline\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	plan, err := db.BuildPlanArtifact("002", "add_thing", "000_baseline", baseline.TargetSHA256, doc, nil,
		db.DiffResult{Up: []string{"alter table x add column y int"}, Down: []string{"alter table x drop column y"}})
	if err != nil {
		t.Fatal(err)
	}
	files, err := db.MigrationArtifactSet(dir, "002", "add_thing", plan, doc, "up;", "down;")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteArtifactSet(files); err != nil {
		t.Fatal(err)
	}
	chain, err := db.LoadSnapshotChain(dir)
	if err != nil {
		t.Fatalf("fixture chain does not load: %v", err)
	}
	return chain
}

// review-1 MINOR-1: applied history versions no chain snapshot or baseline
// covers entry accounts for must be surfaced, never silently dropped from
// the applied-state attribution.
func TestUnchainableAppliedVersions(t *testing.T) {
	chain := writeChainFixture(t, t.TempDir())

	if got := unchainableAppliedVersions(chain, nil); len(got) != 0 {
		t.Fatalf("empty history reported unchainable: %v", got)
	}
	if got := unchainableAppliedVersions(chain, []string{"001", "002"}); len(got) != 0 {
		t.Fatalf("chain-attributable versions reported unchainable: %v", got)
	}
	got := unchainableAppliedVersions(chain, []string{"001", "002", "099_foreign_branch", "004_other"})
	if len(got) != 2 || got[0] != "004_other" || got[1] != "099_foreign_branch" {
		t.Fatalf("unchainable versions = %v, want [004_other 099_foreign_branch]", got)
	}
}

// Baseline covers are version-keyed, so a directory with two files claiming
// one version cannot be represented — the baseline must refuse it up front
// instead of writing a chain that would immediately fail to load.
func TestBaselineCoversRefuseDuplicateVersions(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "002_a.up.sql"), []byte("-- a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "002_b.up.sql"), []byte("-- b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readMigrationVersionsSorted(dir); err == nil || !strings.Contains(err.Error(), "002_a") || !strings.Contains(err.Error(), "002_b") {
		t.Fatalf("duplicate migration versions accepted into baseline covers: %v", err)
	}

	// Distinct versions still pass through.
	ok := t.TempDir()
	if err := os.WriteFile(filepath.Join(ok, "001_a.up.sql"), []byte("-- a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ok, "002_b.up.sql"), []byte("-- b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	covers, err := readMigrationVersionsSorted(ok)
	if err != nil || strings.Join(covers, ",") != "001,002" {
		t.Fatalf("distinct covers = %v err %v", covers, err)
	}
}
