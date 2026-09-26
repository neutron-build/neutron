package cmd

// Offline Q09 tests (no database): snapshot generation writes enum
// additions as their own earlier migration with a continuous chain, and
// the migrate version precondition refuses gated migrations.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

func runSnapshotGenerate(t *testing.T, dir, schema, name string) error {
	t.Helper()
	c := snapshotModeTestCommand()
	if err := c.ParseFlags([]string{"--mode", "snapshot", "--dir", dir, "--schema", schema, "--name", name}); err != nil {
		t.Fatal(err)
	}
	return runMigrateGenerate(c, nil)
}

func TestQ09SnapshotGenerateWritesEnumAdditionsFirst(t *testing.T) {
	work, docA, docB, _ := q09Fixtures(t)
	mig := filepath.Join(work, "mig")
	if err := runSnapshotGenerate(t, mig, docA, "init"); err != nil {
		t.Fatal(err)
	}
	if err := runSnapshotGenerate(t, mig, docB, "mood"); err != nil {
		t.Fatal(err)
	}

	enumUp := readFile(t, filepath.Join(mig, "002_mood_enum_values.up.sql"))
	restUp := readFile(t, filepath.Join(mig, "003_mood.up.sql"))
	if !strings.Contains(enumUp, `alter type "app"."mood" add value 'elated'`) {
		t.Fatalf("002 must add the value:\n%s", enumUp)
	}
	for _, stmt := range db.SplitSQLStatements(enumUp) {
		if db.HasExecutableSQL(stmt) && !strings.Contains(stmt, " add value ") {
			t.Fatalf("002 must hold only enum additions, found: %s", stmt)
		}
	}
	if strings.Contains(restUp, "add value") || !strings.Contains(restUp, "create view") || !strings.Contains(restUp, "'elated'") {
		t.Fatalf("003 must hold the uses and not the addition:\n%s", restUp)
	}

	chain, err := db.LoadSnapshotChain(mig)
	if err != nil {
		t.Fatalf("the chain must stay continuous: %v", err)
	}
	if chain.HeadRef != "003_mood" {
		t.Fatalf("chain head = %s, want 003_mood", chain.HeadRef)
	}
	plan2, err := db.LoadPlanArtifact(filepath.Join(mig, "002_mood_enum_values.plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	plan3, err := db.LoadPlanArtifact(filepath.Join(mig, "003_mood.plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	if plan2.BaseSource != "001_init" || plan3.BaseSource != "002_mood_enum_values" || plan3.BaseSHA256 != plan2.TargetSHA256 {
		t.Fatalf("plans must chain 001 -> 002 -> 003: %s/%s, %s/%s", plan2.BaseSource, plan2.TargetSHA256, plan3.BaseSource, plan3.BaseSHA256)
	}
	if !strings.Contains(strings.Join(plan2.Caveats, "\n"), "55P04") {
		t.Fatalf("002's plan must say why it is separate: %v", plan2.Caveats)
	}
	for _, w := range plan3.Caveats {
		if strings.Contains(w, "will be added") {
			t.Fatalf("003's plan must not carry 002's enum caveat: %s", w)
		}
	}

	// The head equals the desired document: nothing more to generate.
	if err := runSnapshotGenerate(t, mig, docB, "again"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(mig, "004_again.up.sql")); err == nil {
		t.Fatal("a converged chain must not generate another migration")
	}
}

func TestQ09CheckServerVersionRefusesGatedMigrations(t *testing.T) {
	gated := pendingMigration{
		File:       db.MigrationFile{Version: "002", Name: "expr"},
		Statements: []string{`alter table "app"."tenants" alter column "gross" set expression as ((net * 3))`},
	}
	plain := pendingMigration{
		File:       db.MigrationFile{Version: "001", Name: "init"},
		Statements: []string{`create table t (id int)`},
	}
	err := checkServerVersion(16, []pendingMigration{plain, gated})
	if err == nil {
		t.Fatal("PostgreSQL 16 must refuse SET EXPRESSION before running anything")
	}
	for _, want := range []string{"PostgreSQL 16", "002_expr", "SET EXPRESSION needs PostgreSQL 17+", "before any statement runs"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal must mention %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "001_init") {
		t.Fatalf("only the gated migration is named: %v", err)
	}
	if err := checkServerVersion(17, []pendingMigration{plain, gated}); err != nil {
		t.Fatalf("PostgreSQL 17 runs SET EXPRESSION: %v", err)
	}

	// The plan's recorded floor is enforced on its own (a floor the
	// statement scan does not recognize).
	recorded := pendingMigration{
		File:       db.MigrationFile{Version: "003", Name: "future"},
		Statements: []string{`create table u (id int)`},
		Plan:       &db.PlanArtifact{MinServerMajor: 18},
	}
	err = checkServerVersion(17, []pendingMigration{recorded})
	if err == nil || !strings.Contains(err.Error(), "003_future") || !strings.Contains(err.Error(), "minServerMajor 18") {
		t.Fatalf("a recorded floor above the server must refuse: %v", err)
	}
	if err := checkServerVersion(18, []pendingMigration{recorded}); err != nil {
		t.Fatalf("a server at the recorded floor passes: %v", err)
	}
}
