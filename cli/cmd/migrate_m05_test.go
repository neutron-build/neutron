package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

func writeMigrationPair(t *testing.T, dir, name, up, down string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".up.sql"), []byte(up), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".down.sql"), []byte(down), 0o644); err != nil {
		t.Fatal(err)
	}
}

func fileOf(t *testing.T, dir, version string) db.MigrationFile {
	t.Helper()
	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.Version == version {
			return f
		}
	}
	t.Fatalf("version %s not found in %s", version, dir)
	return db.MigrationFile{}
}

func TestAnalyzeMigrationsClassification(t *testing.T) {
	dir := t.TempDir()
	writeMigrationPair(t, dir, "001_plain", "CREATE TABLE a (id int);", "DROP TABLE a;")
	writeMigrationPair(t, dir, "002_destructive", "DROP TABLE a;", "CREATE TABLE a (id int);")
	writeMigrationPair(t, dir, "003_cic", "CREATE INDEX CONCURRENTLY i ON a (id);", "DROP INDEX i;")

	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	analyses, err := analyzeMigrations(dir, files)
	if err != nil {
		t.Fatal(err)
	}
	byVersion := map[string]pendingMigration{}
	for _, p := range analyses {
		byVersion[p.File.Version] = p
	}

	if p := byVersion["001"]; p.Nontransactional || len(p.RiskyStatements) != 0 || p.Plan != nil {
		t.Errorf("001 misclassified: %+v", p)
	}
	p := byVersion["002"]
	if len(p.RiskyStatements) != 1 || !strings.Contains(p.RiskyStatements[0], "DROP TABLE a") || !p.HasDataLoss {
		t.Errorf("002 destructive classification wrong: %+v", p)
	}
	p = byVersion["003"]
	if !p.Nontransactional {
		t.Errorf("003 must classify nontransactional: %+v", p)
	}
	if p.DownFile == nil || !strings.Contains(p.DownFile.SQL, "DROP INDEX") {
		t.Errorf("003 down file not attached: %+v", p.DownFile)
	}
}

func TestAnalyzeMigrationsStalePlanRefused(t *testing.T) {
	dir := t.TempDir()
	up := []string{`create table "public"."users" ("id" integer not null)`}
	planUp := strings.Join(up, ";\n") + ";"
	writeMigrationPair(t, dir, "001_add_users", planUp, "DROP TABLE users;")

	// Write a plan.json exactly as the generator would.
	ops := db.DiffResult{Up: up, Down: []string{`drop table "public"."users"`}}
	doc, err := db.EmptyV2Document()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := db.BuildPlanArtifact("001", "add_users", "empty", doc.SHA256Hex, doc, nil, ops)
	if err != nil {
		t.Fatal(err)
	}
	planJSON, err := db.MarshalPlanJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "001_add_users.plan.json"), planJSON, 0o644); err != nil {
		t.Fatal(err)
	}
	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := analyzeMigrations(dir, files); err != nil {
		t.Fatalf("matching plan must analyze cleanly: %v", err)
	}

	// Hand-edit the up.sql after generation: the plan is stale and must be
	// refused, not trusted for risk/reversibility gating.
	writeMigrationPair(t, dir, "001_add_users", planUp+"\nCREATE TABLE extra (id int);", "DROP TABLE users;")
	files, err = db.ReadMigrationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = analyzeMigrations(dir, files)
	if err == nil || !strings.Contains(err.Error(), "stale plan") {
		t.Fatalf("hand-edited up.sql with stale plan.json accepted: %v", err)
	}
}

func TestRequireDestructiveAcknowledgement(t *testing.T) {
	plain := pendingMigration{File: db.MigrationFile{Version: "001", Name: "plain"}}
	risky := pendingMigration{File: db.MigrationFile{Version: "002", Name: "risky"}, RiskyStatements: []string{"DROP TABLE users"}}

	if err := requireDestructiveAcknowledgement([]pendingMigration{plain}, false); err != nil {
		t.Errorf("plain migration refused without cause: %v", err)
	}
	err := requireDestructiveAcknowledgement([]pendingMigration{plain, risky}, false)
	if err == nil {
		t.Fatal("destructive migration applied without acknowledgement")
	}
	for _, want := range []string{"destructive", "--allow-destructive", "DROP TABLE users", "002_risky"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q: %v", want, err)
		}
	}
	if err := requireDestructiveAcknowledgement([]pendingMigration{risky}, true); err != nil {
		t.Errorf("acknowledged destructive migration refused: %v", err)
	}
}

func TestRefuseNontransactionalDataChanges(t *testing.T) {
	pure := pendingMigration{File: db.MigrationFile{Version: "001", Name: "cic"}}
	pure.Nontransactional = true
	pure.Statements = db.SplitSQLStatements("CREATE INDEX CONCURRENTLY i ON t (c);")
	if err := refuseNontransactionalDataChanges([]pendingMigration{pure}); err != nil {
		t.Errorf("pure DDL nontransactional refused: %v", err)
	}

	mixed := pendingMigration{File: db.MigrationFile{Version: "002", Name: "mixed"}}
	mixed.Nontransactional = true
	mixed.Statements = db.SplitSQLStatements("CREATE INDEX CONCURRENTLY i ON t (c);\nINSERT INTO t VALUES (1);")
	err := refuseNontransactionalDataChanges([]pendingMigration{mixed})
	if err == nil || !strings.Contains(err.Error(), "data-changing") {
		t.Fatalf("nontransactional data change accepted: %v", err)
	}

	// Transactional DML stays fine: atomic rollback makes it recoverable.
	txDml := pendingMigration{File: db.MigrationFile{Version: "003", Name: "seed"}}
	txDml.Statements = db.SplitSQLStatements("INSERT INTO t VALUES (1);")
	if err := refuseNontransactionalDataChanges([]pendingMigration{txDml}); err != nil {
		t.Errorf("transactional DML refused: %v", err)
	}
}

func TestEffectsReportVerdict(t *testing.T) {
	clean := &effectsReport{CreationsTotal: 2, CreationsPresent: 0}
	if clean.Verdict() != "clean" {
		t.Errorf("clean verdict = %s", clean.Verdict())
	}
	partial := &effectsReport{CreationsTotal: 2, CreationsPresent: 1}
	if partial.Verdict() != "partial" {
		t.Errorf("partial verdict = %s", partial.Verdict())
	}
	complete := &effectsReport{CreationsTotal: 2, CreationsPresent: 2}
	if complete.Verdict() != "complete" {
		t.Errorf("complete verdict = %s", complete.Verdict())
	}
	if !complete.MarkAppliedAllowed() {
		t.Error("fully-satisfied report must allow mark-applied")
	}
	invalid := &effectsReport{CreationsTotal: 1, CreationsPresent: 1, HasInvalid: true}
	if invalid.Verdict() != "invalid" {
		t.Errorf("invalid verdict = %s", invalid.Verdict())
	}
	if invalid.MarkAppliedAllowed() {
		t.Error("invalid index debris must not allow mark-applied")
	}
	unverifiable := &effectsReport{CreationsTotal: 1, CreationsPresent: 1, Unverifiable: 1}
	if unverifiable.MarkAppliedAllowed() {
		t.Error("unverifiable statements must not allow mark-applied")
	}
	unsatisfied := &effectsReport{
		Effects: []statementEffect{{State: "unsatisfied"}, {State: "satisfied"}},
	}
	if unsatisfied.MarkAppliedAllowed() {
		t.Error("unsatisfied effect must not allow mark-applied")
	}
}

func TestEffectsReportProvenClean(t *testing.T) {
	// Provably clean: every postcondition verifiable and absent.
	clean := &effectsReport{CreationsTotal: 2, CreationsPresent: 0}
	if !clean.ProvenClean() {
		t.Error("verifiable clean report must be ProvenClean")
	}

	// MAJOR-2 (M05 review): a "clean" verdict with unverifiable statements
	// is NOT proof that nothing ran — an ALTER executed in the kill window
	// leaves no checkable trace.
	unverifiable := &effectsReport{CreationsTotal: 1, CreationsPresent: 0, Unverifiable: 1}
	if unverifiable.Verdict() != "clean" {
		t.Fatalf("premise wrong: verdict = %s, want clean", unverifiable.Verdict())
	}
	if unverifiable.ProvenClean() {
		t.Error("clean verdict with unverifiable statements must not be ProvenClean")
	}

	partial := &effectsReport{CreationsTotal: 2, CreationsPresent: 1}
	if partial.ProvenClean() {
		t.Error("partial report must not be ProvenClean")
	}
	complete := &effectsReport{CreationsTotal: 2, CreationsPresent: 2}
	if complete.ProvenClean() {
		t.Error("complete report must not be ProvenClean")
	}
}

func TestValidateDownReversibility(t *testing.T) {
	down := func(sql string) *db.MigrationFile {
		f := db.MigrationFile{Version: "001", Name: "x", SQL: sql, IsDown: true}
		return &f
	}
	base := func(d *db.MigrationFile) pendingMigration {
		return pendingMigration{File: db.MigrationFile{Version: "001", Name: "x"}, DownFile: d}
	}

	if err := validateDownReversibility(base(nil)); err == nil || !strings.Contains(err.Error(), "no down migration file") {
		t.Errorf("missing down: %v", err)
	}
	if err := validateDownReversibility(base(down("-- IRREVERSIBLE: no restoration possible\n"))); err == nil || !strings.Contains(err.Error(), "irreversible") {
		t.Errorf("comment-only down must be refused as irreversible, got: %v", err)
	}
	if err := validateDownReversibility(base(down("DROP INDEX CONCURRENTLY i;"))); err == nil || !strings.Contains(err.Error(), "concurrent") {
		t.Errorf("concurrent down must be refused, got: %v", err)
	}
	if err := validateDownReversibility(base(down("DROP TABLE a;"))); err != nil {
		t.Errorf("ordinary down refused: %v", err)
	}

	// A plan.json marking the migration irreversible refuses even a real
	// down file.
	p := base(down("DROP TABLE a;"))
	p.Plan = &db.PlanArtifact{}
	p.Plan.Risk.OverallReversibility = db.ReversibilityIrreversible
	p.Plan.Risk.IrreversibleCount = 1
	if err := validateDownReversibility(p); err == nil || !strings.Contains(err.Error(), "irreversible") {
		t.Errorf("plan-irreversible down accepted: %v", err)
	}
}

func TestChainIfPresent(t *testing.T) {
	// No snapshots directory: nil chain, no error (legacy file workflow).
	dir := t.TempDir()
	writeMigrationPair(t, dir, "001_a", "CREATE TABLE a (id int);", "DROP TABLE a;")
	chain, err := chainIfPresent(dir)
	if err != nil {
		t.Fatal(err)
	}
	if chain != nil {
		t.Fatal("legacy directory must not produce a chain")
	}

	// Empty snapshots directory: still no chain.
	if err := os.MkdirAll(filepath.Join(dir, db.SnapshotDir), 0o755); err != nil {
		t.Fatal(err)
	}
	chain, err = chainIfPresent(dir)
	if err != nil || chain != nil {
		t.Fatalf("empty snapshots dir: chain=%v err=%v", chain, err)
	}

	// A snapshots directory with files but a broken chain is a hard error.
	if err := os.WriteFile(filepath.Join(dir, db.SnapshotDir, "009_orphan.snapshot.json"), []byte(`{"formatVersion":1,"kind":"migration","version":"009","name":"orphan","baseSha256":"deadbeef","targetSha256":"beefdead","document":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := chainIfPresent(dir); err == nil {
		t.Fatal("broken chain accepted for apply")
	}
}

func TestValidateStatementAllowlist(t *testing.T) {
	pend := func(version, up string) pendingMigration {
		return pendingMigration{
			File:       db.MigrationFile{Version: version, Name: "x"},
			Statements: db.SplitSQLStatements(up),
		}
	}

	// Every allowlisted kind in one batch passes.
	legal := pend("001", "CREATE TABLE a (id int);\nALTER TABLE a ADD COLUMN b int;\nINSERT INTO a VALUES (1);\nSET LOCAL statement_timeout = '5s';")
	if err := validateStatementAllowlist([]pendingMigration{legal}); err != nil {
		t.Fatalf("allowlisted statements refused: %v", err)
	}

	// A refused kind names the migration, the statement number and the kind.
	bad := pend("002", "CREATE TABLE a (id int);\nEXPLAIN ANALYZE DELETE FROM _neutron_migrations;")
	err := validateStatementAllowlist([]pendingMigration{legal, bad})
	if err == nil {
		t.Fatal("out-of-allowlist statement accepted")
	}
	for _, want := range []string{"002_x", "statement 2", "statement kind EXPLAIN is refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q: %v", want, err)
		}
	}

	// Comment-only fragments never trip the allowlist.
	comments := pend("003", "-- EXPLAIN ANALYZE DELETE FROM _neutron_migrations\n/* COPY t FROM STDIN */")
	if err := validateStatementAllowlist([]pendingMigration{comments}); err != nil {
		t.Errorf("comment-only migration refused: %v", err)
	}
}

func TestValidateDownStatementAllowlist(t *testing.T) {
	base := func(down string) pendingMigration {
		return pendingMigration{
			File:     db.MigrationFile{Version: "001", Name: "x"},
			DownFile: &db.MigrationFile{Version: "001", Name: "x", SQL: down, IsDown: true},
		}
	}
	if err := validateDownStatementAllowlist(base("DROP TABLE a;")); err != nil {
		t.Fatalf("ordinary down refused: %v", err)
	}
	noDown := pendingMigration{File: db.MigrationFile{Version: "001", Name: "x"}}
	if err := validateDownStatementAllowlist(noDown); err != nil {
		t.Fatalf("no down file must not be an allowlist error: %v", err)
	}
	err := validateDownStatementAllowlist(base("PREPARE p AS DELETE FROM _neutron_migrations;\nEXECUTE p;"))
	if err == nil || !strings.Contains(err.Error(), "down file for 001_x") || !strings.Contains(err.Error(), "statement kind PREPARE is refused") {
		t.Fatalf("down-file escape accepted or unnamed: %v", err)
	}
}
