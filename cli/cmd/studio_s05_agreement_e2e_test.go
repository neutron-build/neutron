package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/studio"
)

// TestStudioDesignerAgreesWithCLI is the S05 designer/CLI agreement leg of
// V10: a visual change set planned by Studio (studio.PlanSchemaChanges, the
// function behind POST /api/schema/plan) is reproduced by the REAL CLI
// binary from the plan's own target document —
//
//   - `neutron schema pull` yields the plan's base identity (same canonical
//     document hash: designer and CLI agree on object identity);
//   - `neutron db push --dry-run` with the plan's cliEquivalent flags prints
//     exactly the plan's statements;
//   - `neutron migrate generate --mode snapshot` from a `schema baseline`
//     records the same up/down operations in its plan.json.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server (NEUTRON_LIVE_REQUIRED=1 turns the skip into a failure).
func TestStudioDesignerAgreesWithCLI(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio/CLI agreement check skipped")
	}
	ctx := context.Background()
	bin := buildCLIBinary(t)

	dbName := fmt.Sprintf("s05cli_%d_%d", os.Getpid(), time.Now().UnixNano())
	dbURL := deriveDatabaseURL(t, base, dbName)
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		if !strings.HasPrefix(dbName, "s05cli_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := admin.Exec(cctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	client, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()
	for _, stmt := range []string{
		`CREATE SCHEMA crm`,
		`CREATE TABLE crm.accounts (id bigint PRIMARY KEY, name text NOT NULL, region text)`,
		`CREATE TABLE crm.contacts (
			id bigint PRIMARY KEY,
			account_id bigint NOT NULL REFERENCES crm.accounts(id),
			email varchar(200) NOT NULL,
			phone text,
			legacy text
		)`,
		`CREATE INDEX contacts_phone_idx ON crm.contacts (phone)`,
		`INSERT INTO crm.accounts VALUES (1, 'a', 'eu')`,
	} {
		if err := client.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}

	changes := []studio.SchemaChange{
		{Op: "rename-column", Schema: "crm", Table: "contacts", From: "phone", To: "mobile"},
		{Op: "alter-column-type", Schema: "crm", Table: "contacts", Column: "email", Type: "varchar(320)"},
		{Op: "add-column", Schema: "crm", Table: "accounts", Column: "tier", Type: "integer", NotNull: true, Default: strPtrS05("1")},
		{Op: "drop-column", Schema: "crm", Table: "contacts", Column: "legacy"},
		{Op: "add-index", Schema: "crm", Table: "accounts", Index: "accounts_region_idx", Column: "region"},
	}
	plan, err := studio.PlanSchemaChanges(ctx, client, changes)
	if err != nil {
		t.Fatalf("studio plan: %v", err)
	}
	if len(plan.Up) == 0 || len(plan.Up) != len(plan.Down) || len(plan.Operations) != len(plan.Up) {
		t.Fatalf("plan shape: up %d down %d ops %d", len(plan.Up), len(plan.Down), len(plan.Operations))
	}

	t.Logf("designer plan (%d statements):\n%s", len(plan.Up), strings.Join(plan.Up, ";\n"))

	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.schema.json")
	if err := os.WriteFile(targetPath, plan.Target, 0o644); err != nil {
		t.Fatal(err)
	}

	// Object identity: the CLI's pulled document is the plan's base.
	livePath := filepath.Join(dir, "live.json")
	if code, out := runCLIProcess(t, bin, dbURL, "schema", "pull", "--out", livePath); code != 0 {
		t.Fatalf("schema pull (%d): %s", code, out)
	}
	pulledRaw, err := os.ReadFile(livePath)
	if err != nil {
		t.Fatal(err)
	}
	pulled, err := db.ParseV2Document(pulledRaw)
	if err != nil {
		t.Fatalf("pulled document: %v", err)
	}
	if pulled.SHA256Hex != plan.BaseSHA256 {
		t.Fatalf("designer base identity %s != CLI pulled document %s", plan.BaseSHA256, pulled.SHA256Hex)
	}

	// The documented CLI equivalent, flag for flag.
	wantFlags := []string{"--dry-run", "--schema", "target.schema.json", "--rename", "'crm.contacts.phone>crm.contacts.mobile'", "--allow-destructive"}
	if got := strings.Fields(plan.CLIEquivalent)[3:]; !reflect.DeepEqual(got, wantFlags) {
		t.Fatalf("cliEquivalent flags = %v, want %v", got, wantFlags)
	}
	pushArgs := []string{"db", "push", "--dry-run", "--schema", targetPath, "--allow-destructive"}
	for _, f := range plan.RenameFlags {
		pushArgs = append(pushArgs, "--rename", f)
	}
	code, out := runCLIProcess(t, bin, dbURL, pushArgs...)
	if code != 0 {
		t.Fatalf("db push --dry-run (%d): %s", code, out)
	}
	wantSQL := strings.Join(plan.Up, ";\n") + ";"
	if !strings.HasSuffix(strings.TrimSpace(out), wantSQL) {
		t.Fatalf("CLI dry-run plan differs from the designer plan.\nCLI output:\n%s\ndesigner:\n%s", out, wantSQL)
	}

	// Offline snapshot generation from a baseline of the same database.
	migDir := filepath.Join(dir, "migrations")
	if code, out := runCLIProcess(t, bin, dbURL, "schema", "baseline", "--dir", migDir); code != 0 {
		t.Fatalf("schema baseline (%d): %s", code, out)
	}
	genArgs := []string{"migrate", "generate", "--mode", "snapshot", "--dir", migDir, "--schema", targetPath, "--name", "designer_change", "--allow-destructive"}
	for _, f := range plan.RenameFlags {
		genArgs = append(genArgs, "--rename", f)
	}
	if code, out := runCLIProcess(t, bin, dbURL, genArgs...); code != 0 {
		t.Fatalf("migrate generate --mode snapshot (%d): %s", code, out)
	}
	matches, _ := filepath.Glob(filepath.Join(migDir, "*_designer_change.plan.json"))
	if len(matches) != 1 {
		t.Fatalf("plan.json artifacts: %v", matches)
	}
	generated, err := db.LoadPlanArtifact(matches[0])
	if err != nil {
		t.Fatalf("load generated plan: %v", err)
	}
	if generated.TargetSHA256 != plan.TargetSHA256 || generated.BaseSHA256 != plan.BaseSHA256 {
		t.Fatalf("generated plan identities base %s target %s != designer base %s target %s",
			generated.BaseSHA256, generated.TargetSHA256, plan.BaseSHA256, plan.TargetSHA256)
	}
	var genUp, genDown []string
	for _, op := range generated.Operations {
		genUp = append(genUp, op.SQL)
		genDown = append(genDown, op.Down)
	}
	if !reflect.DeepEqual(genUp, plan.Up) || !reflect.DeepEqual(genDown, plan.Down) {
		t.Fatalf("snapshot generation differs from the designer plan.\ngenerated up: %q\ndesigner up:  %q\ngenerated down: %q\ndesigner down:  %q", genUp, plan.Up, genDown, plan.Down)
	}
	if generated.Risk.HasDestructive != plan.Risk.HasDestructive || generated.Risk.HasDataLoss != plan.Risk.HasDataLoss {
		t.Fatalf("risk differs: generated %+v designer %+v", generated.Risk, plan.Risk)
	}
}

func strPtrS05(s string) *string { return &s }
