package cmd

// M07 live coverage through the REAL CLI binary: the documented upgrade
// path (history -> baseline -> snapshot generate -> migrate -> schema check
// --live) on a database that already has neutron's history table, and a
// baseline written before M07 (internal table included) that must keep
// working. Skipped unless NEUTRON_E2E_DATABASE_URL is set
// (NEUTRON_LIVE_REQUIRED=1 fails instead).

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

const m07Desired = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}],
	"tables": [{
		"identity": {"schema": "public", "name": "users"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "email", "type": {"name": "text", "codec": "string"}, "notNull": true},
			{"name": "nick", "type": {"name": "text", "codec": "string"}, "notNull": false}
		],
		"constraints": [{"name": "users_pkey", "type": "primary-key", "columns": ["id"]}],
		"indexes": []
	}],
	"enums": [], "views": [], "opaque": []
}`

const m07InitSQL = `CREATE TABLE users (id integer PRIMARY KEY, email text NOT NULL)`

const m07NickCount = `SELECT count(*)::text FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'nick'`

// m07DocTables lists the table identities of a schema document file, or of
// the document embedded in a snapshot file.
func m07DocTables(t *testing.T, path string) []string {
	t.Helper()
	var raw struct {
		Document json.RawMessage `json:"document"`
		Tables   []struct {
			Identity struct{ Schema, Name string } `json:"identity"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(mustReadFile(t, path), &raw); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(raw.Document) > 0 {
		if err := json.Unmarshal(raw.Document, &raw); err != nil {
			t.Fatalf("parse %s document: %v", path, err)
		}
	}
	var out []string
	for _, tb := range raw.Tables {
		out = append(out, tb.Identity.Schema+"."+tb.Identity.Name)
	}
	return out
}

// m07LegacyFixture creates a database the pre-M04 CLI migrated: users plus
// a legacy-text history row for 001, and the matching migration file.
func m07LegacyFixture(t *testing.T, fx *db.Client, mig string) {
	t.Helper()
	ctx := context.Background()
	for _, sql := range []string{
		m07InitSQL,
		`CREATE TABLE _neutron_migrations (version text PRIMARY KEY, name text NOT NULL, applied_at timestamptz DEFAULT now())`,
		`INSERT INTO _neutron_migrations (version, name) VALUES ('001', 'init')`,
	} {
		if err := fx.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if err := os.MkdirAll(mig, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mig, "001_init.up.sql"), m07InitSQL+";\n")
	writeFile(t, filepath.Join(mig, "001_init.down.sql"), "DROP TABLE users;\n")
}

func TestM07BaselineUpgradePath(t *testing.T) {
	if os.Getenv("NEUTRON_E2E_DATABASE_URL") == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; M07 upgrade-path E2E skipped (set it to a disposable Postgres URL to run)")
	}
	built := buildCLIBinary(t)
	unreachable := "postgres://m07-no-such-user@127.0.0.1:1/m07offline"

	must := func(t *testing.T, bin, url string, args ...string) string {
		t.Helper()
		code, out := runCLIProcess(t, bin, url, args...)
		if code != 0 {
			t.Fatalf("neutron %s exited %d:\n%s", strings.Join(args, " "), code, out)
		}
		return out
	}

	// generateApplyCheck is README steps 4 onward: snapshot generate
	// (offline), live check, migrate, live check. The generated migration
	// is version next; it must plan only the nick column.
	generateApplyCheck := func(t *testing.T, bin, dbURL string, fx *db.Client, mig, desired, next, wantHistory string) {
		t.Helper()
		must(t, bin, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", desired, "--name", "add_nick")
		stem := next + "_add_nick"
		up := string(mustReadFile(t, filepath.Join(mig, stem+".up.sql")))
		if strings.Contains(up, "_neutron_") || strings.Contains(up, "bio") || !strings.Contains(up, "nick") {
			t.Fatalf("plan must add nick only:\n%s", up)
		}
		must(t, bin, dbURL, "schema", "check", "--live", "--dir", mig)
		must(t, bin, dbURL, "migrate", "--dir", mig)
		if got := q09Query(t, fx, m07NickCount); got != "1" {
			t.Fatalf("nick column count %s after migrate", got)
		}
		if got := q09Query(t, fx, `SELECT string_agg(version, ',' ORDER BY version) FROM _neutron_migrations`); got != wantHistory {
			t.Fatalf("history = %s, want %s", got, wantHistory)
		}
		out := must(t, bin, dbURL, "schema", "check", "--live", "--dir", mig)
		if !strings.Contains(out, stem) {
			t.Fatalf("live check must compare against the applied snapshot:\n%s", out)
		}
	}

	assertNoInternal := func(t *testing.T, path string) {
		t.Helper()
		tables := m07DocTables(t, path)
		if strings.Join(tables, ",") != "public.users" {
			t.Fatalf("%s lists %v, want public.users only", filepath.Base(path), tables)
		}
	}

	// README "Existing databases" steps 2-4 on a pre-M04 CLI history.
	t.Run("AdoptedLegacyHistory", func(t *testing.T) {
		dbURL, fx := newM02CommandDB(t, "m07adopt")
		bin := built
		work := t.TempDir()
		mig := filepath.Join(work, "migrations")
		desired := filepath.Join(work, "desired.json")
		writeFile(t, desired, m07Desired)
		m07LegacyFixture(t, fx, mig)

		if code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig); code == 0 || !strings.Contains(out, "adopt") {
			t.Fatalf("legacy history must be refused until adopted (%d):\n%s", code, out)
		}
		must(t, bin, dbURL, "migrate", "adopt", "--dir", mig)
		must(t, bin, dbURL, "schema", "baseline", "--dir", mig)
		assertNoInternal(t, filepath.Join(mig, "snapshots", "000_baseline.snapshot.json"))

		pulled := filepath.Join(work, "pulled.json")
		must(t, bin, dbURL, "schema", "pull", "--out", pulled)
		assertNoInternal(t, pulled)

		generateApplyCheck(t, bin, dbURL, fx, mig, desired, "002", "001,002")
	})

	// History written by the current runner (v2 shape), then a baseline.
	// The workflow runs first so that a baseline listing the history table
	// fails on the refusal users hit, not on the file assertion.
	t.Run("RunnerHistory", func(t *testing.T) {
		dbURL, fx := newM02CommandDB(t, "m07runner")
		bin := built
		work := t.TempDir()
		mig := filepath.Join(work, "migrations")
		desired := filepath.Join(work, "desired.json")
		writeFile(t, desired, m07Desired)
		if err := os.MkdirAll(mig, 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(mig, "001_init.up.sql"), m07InitSQL+";\n")
		must(t, bin, dbURL, "migrate", "--dir", mig)
		must(t, bin, dbURL, "schema", "baseline", "--dir", mig)
		generateApplyCheck(t, bin, dbURL, fx, mig, desired, "002", "001,002")
		assertNoInternal(t, filepath.Join(mig, "snapshots", "000_baseline.snapshot.json"))
	})

	// A baseline written before M07 lists _neutron_migrations. It keeps
	// working as written (no re-baseline, no edit), and drift on a user
	// table is still refused against it.
	t.Run("PreM07BaselineRecovery", func(t *testing.T) {
		dbURL, fx := newM02CommandDB(t, "m07old")
		bin := built
		ctx := context.Background()
		work := t.TempDir()
		mig := filepath.Join(work, "migrations")
		desired := filepath.Join(work, "desired.json")
		writeFile(t, desired, m07Desired)
		m07LegacyFixture(t, fx, mig)
		must(t, bin, dbURL, "migrate", "adopt", "--dir", mig)

		// What `schema baseline` wrote before M07: the whole introspected
		// document, the internal table included.
		full, err := fx.IntrospectV2(ctx)
		if err != nil {
			t.Fatal(err)
		}
		applied, shape, err := fx.AppliedVersionsReadOnly(ctx)
		if err != nil {
			t.Fatal(err)
		}
		content, err := db.MarshalSnapshotJSON(db.BaselineSnapshotFor(full, []string{"001"},
			db.BaselineHistory{Shape: shape.String(), AppliedVersions: applied, Note: "history observed read-only; not modified"}))
		if err != nil {
			t.Fatal(err)
		}
		baselinePath := filepath.Join(mig, "snapshots", "000_baseline.snapshot.json")
		if err := os.MkdirAll(filepath.Dir(baselinePath), 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, baselinePath, string(content))
		if got := strings.Join(m07DocTables(t, baselinePath), ","); got != "public._neutron_migrations,public.users" {
			t.Fatalf("fixture must be the pre-M07 baseline shape, lists %s", got)
		}

		must(t, bin, unreachable, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", desired, "--name", "add_nick")
		out := must(t, bin, dbURL, "schema", "check", "--live", "--dir", mig)
		if !strings.Contains(out, "000_baseline") {
			t.Fatalf("with nothing new applied the live check compares against the baseline:\n%s", out)
		}

		// Drift on a user table: refused by the live check and by migrate.
		if err := fx.Exec(ctx, `ALTER TABLE users ADD COLUMN rogue integer`); err != nil {
			t.Fatal(err)
		}
		if code, out := runCLIProcess(t, bin, dbURL, "schema", "check", "--live", "--dir", mig); code == 0 || !strings.Contains(out, "rogue") {
			t.Fatalf("user-table drift must fail the live check (%d):\n%s", code, out)
		}
		if code, out := runCLIProcess(t, bin, dbURL, "migrate", "--dir", mig); code == 0 || !strings.Contains(out, "drift") || !strings.Contains(out, "rogue") {
			t.Fatalf("user-table drift must refuse migrate (%d):\n%s", code, out)
		}
		if got := q09Query(t, fx, m07NickCount); got != "0" {
			t.Fatalf("refused migrate applied something (nick count %s)", got)
		}
		if err := fx.Exec(ctx, `ALTER TABLE users DROP COLUMN rogue`); err != nil {
			t.Fatal(err)
		}

		must(t, bin, dbURL, "migrate", "--dir", mig)
		if got := q09Query(t, fx, m07NickCount); got != "1" {
			t.Fatalf("nick column count %s after migrate", got)
		}
		must(t, bin, dbURL, "schema", "check", "--live", "--dir", mig)
		if string(mustReadFile(t, baselinePath)) != string(content) {
			t.Fatalf("the baseline file must never be rewritten")
		}
	})
}
