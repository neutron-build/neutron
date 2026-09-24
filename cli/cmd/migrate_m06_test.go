package cmd

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Offline M06 coverage: journal parsing wired into migration analysis,
// journaled files exempt from (and unmarked files still subject to) the
// nontransactional-DML refusal, and down reversibility limits extended to
// journal markers. Live behavior is covered by migrate_m06_e2e_test.go.

func TestAnalyzeMigrationsParsesJournal(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "001_j.up.sql"),
		"-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\" expect=\"1\"\nUPDATE t SET a = 1;\n")
	writeFile(t, filepath.Join(dir, "002_p.up.sql"), "CREATE TABLE t2 (id int);\n")

	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	pendings, err := analyzeMigrations(dir, files)
	if err != nil {
		t.Fatalf("analyzeMigrations: %v", err)
	}
	if pendings[0].Journal == nil || len(pendings[0].Journal.Steps) != 1 {
		t.Fatalf("journaled migration not parsed: %+v", pendings[0])
	}
	if pendings[1].Journal != nil {
		t.Fatalf("plain migration parsed as journaled: %+v", pendings[1])
	}
}

func TestAnalyzeMigrationsRefusesMalformedJournal(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "001_bad.up.sql"), "-- neutron:journaled\nUPDATE t SET a = 1;\n")
	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = analyzeMigrations(dir, files)
	if err == nil || !strings.Contains(err.Error(), "no verifiable effect") {
		t.Fatalf("malformed journal must fail analysis, got %v", err)
	}
}

func TestRefuseNontransactionalDataChangesJournalException(t *testing.T) {
	mk := func(journaled bool) pendingMigration {
		p := pendingMigration{
			Nontransactional: true,
			Statements:       []string{"UPDATE t SET a = 1;"},
		}
		if journaled {
			p.Journal = &db.JournaledFile{}
		}
		return p
	}
	if err := refuseNontransactionalDataChanges([]pendingMigration{mk(false)}); err == nil {
		t.Fatal("unmarked concurrent DML must stay refused")
	}
	if err := refuseNontransactionalDataChanges([]pendingMigration{mk(true)}); err != nil {
		t.Fatalf("journaled migration refused by the unmarked rule: %v", err)
	}
}

func TestValidateDownReversibilityRefusesJournalMarker(t *testing.T) {
	p := pendingMigration{
		DownFile: &db.MigrationFile{SQL: "-- neutron:journaled\nDROP INDEX IF EXISTS i;"},
	}
	err := validateDownReversibility(p)
	if err == nil || !strings.Contains(err.Error(), "journal") {
		t.Fatalf("journaled down must be refused, got %v", err)
	}
}

func TestValidateStatementAllowlistAcceptsJournaledStatements(t *testing.T) {
	// The statements themselves (annotations are comments) must pass the
	// allowlist exactly like their unmarked equivalents — SET LOCAL, DML
	// and concurrent index kinds.
	stmts := []string{
		"SET LOCAL lock_timeout = '1s';",
		"UPDATE t SET a = 1 WHERE a IS NULL;",
		"CREATE INDEX CONCURRENTLY i ON t (a);",
	}
	for _, s := range stmts {
		if err := db.CheckStatementAllowlist(s); err != nil {
			t.Errorf("allowlist refused journaled-class statement %q: %v", s, err)
		}
	}
}

func TestExamplesShipWellFormedArtifacts(t *testing.T) {
	// The tracked example set parses: journaled files validate, downs
	// carry no markers, and every migration file is readable.
	migrations := filepath.Join("..", "examples-src", "migrations")
	files, err := db.ReadMigrationFiles(migrations)
	if err != nil {
		t.Fatalf("example migrations unreadable: %v", err)
	}
	if len(files) != 4 {
		t.Fatalf("example migrations = %d files, want 4", len(files))
	}
	journaled := 0
	for _, f := range files {
		jf, err := db.ParseJournaledFile(f.SQL)
		if err != nil {
			t.Errorf("example %s: %v", f.Path, err)
			continue
		}
		if jf != nil {
			journaled++
			for i := range jf.Steps {
				if err := db.CheckStatementAllowlist(jf.Steps[i].Statement); err != nil {
					t.Errorf("example %s step %d: %v", f.Path, i+1, err)
				}
			}
		}
	}
	if journaled != 3 {
		t.Fatalf("journaled examples = %d, want 3 (backfill, contract, index lifecycle)", journaled)
	}
	downs, err := db.ReadDownMigrationFiles(migrations)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range downs {
		if err := db.RefuseJournaledDown(d.SQL); err != nil {
			t.Errorf("example down %s: %v", d.Path, err)
		}
	}
}
