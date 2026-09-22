package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestMigrationSafetyE2E exercises the V05 containment contract through the
// REAL CLI binary against a REAL disposable Postgres database: metadata
// protection, destructive acknowledgement, input validation, rename
// validation, and transactional push rollback (verified by inspecting the
// database, not command output).
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server (e.g. postgres://user@localhost:5432/postgres), or when
// NEUTRON_LIVE_REQUIRED=1 that missing URL is a failure instead of a skip.
// The test creates one uniquely-named database (neutron_orm_b03_*) and drops
// it afterwards; it never touches other databases.
func TestMigrationSafetyE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; end-to-end migration-safety check skipped (set it to a disposable Postgres URL to run)")
	}

	bin := buildCLIBinary(t)
	dbName := fmt.Sprintf("neutron_orm_b03_%d_%d", os.Getpid(), time.Now().Unix())
	dbURL := deriveDatabaseURL(t, base, dbName)

	admin, err := db.Connect(context.Background(), base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	// Cleanups run LIFO: registered first => closed last, after the drop.
	t.Cleanup(admin.Close)
	if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, dbName,
		)); err != nil {
			t.Errorf("terminate backends: %v", err)
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	seed := []string{
		`CREATE TABLE users (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text NOT NULL UNIQUE,
			active boolean NOT NULL DEFAULT true,
			created_at timestamptz NOT NULL DEFAULT now()
		)`,
		`INSERT INTO users (name, email) VALUES ('Alice', 'a@x.com')`,
		`CREATE TABLE _neutron_migrations (
			version TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TIMESTAMPTZ DEFAULT now()
		)`,
		`INSERT INTO _neutron_migrations (version, name) VALUES ('001_init', 'init')`,
		`CREATE TABLE audit_sentinel (id int PRIMARY KEY, note text NOT NULL)`,
		`INSERT INTO audit_sentinel VALUES (1, 'do-not-drop')`,
	}
	fixture, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer fixture.Close()
	for _, stmt := range seed {
		if err := fixture.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	work := t.TempDir()
	desiredPath := filepath.Join(work, "desired.json")
	writeFile(t, desiredPath, `{
		"version": 1,
		"tables": [
			{
				"name": "users",
				"columns": [
					{ "name": "id", "type": "serial", "notNull": true, "primaryKey": true },
					{ "name": "name", "type": "text", "notNull": true },
					{ "name": "email", "type": "text", "notNull": true, "unique": true },
					{ "name": "active", "type": "boolean", "notNull": true, "hasDefault": true, "default": "true" },
					{ "name": "created_at", "type": "timestamptz", "notNull": true, "hasDefault": true, "defaultNow": true }
				],
				"indexes": []
			}
		]
	}`)

	// runCLI executes the real binary with --url pinned to the disposable DB.
	runCLI := func(dir string, args ...string) (int, string) {
		t.Helper()
		full := append([]string{"--url", dbURL}, args...)
		cmd := exec.Command(bin, full...)
		if dir != "" {
			cmd.Dir = dir
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				return exitErr.ExitCode(), string(out)
			}
			t.Fatalf("run CLI %v: %v", args, err)
		}
		return 0, string(out)
	}
	queryInt := func(sql string) int {
		t.Helper()
		var n int
		if err := fixture.QueryRow(context.Background(), sql).Scan(&n); err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		return n
	}
	tableExists := func(name string) bool {
		t.Helper()
		return queryInt(fmt.Sprintf(`SELECT (to_regclass('public.%s') IS NOT NULL)::int`, name)) == 1
	}

	t.Run("generate matching schema emits no drops", func(t *testing.T) {
		genDir := filepath.Join(work, "mig1")
		if err := os.MkdirAll(genDir, 0755); err != nil {
			t.Fatal(err)
		}
		code, out := runCLI(genDir, "migrate", "generate", "--schema", desiredPath, "--name", "repro")
		if code != 0 {
			t.Fatalf("exit %d, output:\n%s", code, out)
		}
		files, _ := filepath.Glob(filepath.Join(genDir, "migrations", "*repro*.up.sql"))
		for _, f := range files {
			content := readFile(t, f)
			if strings.Contains(strings.ToLower(content), "drop") {
				t.Fatalf("generated up.sql must not contain DROP statements, got:\n%s", content)
			}
		}
		if !tableExists("audit_sentinel") || queryInt(`SELECT count(*) FROM audit_sentinel`) != 1 {
			t.Fatal("sentinel table/data must survive generate")
		}
		if !tableExists("_neutron_migrations") || queryInt(`SELECT count(*) FROM _neutron_migrations`) != 1 {
			t.Fatal("migration history must survive generate")
		}
	})

	t.Run("push with history override only preserves unmanaged data", func(t *testing.T) {
		// --force overrides the history guard but must NOT authorize drops
		// (B00 fail-before: this exact invocation erased history + sentinel).
		code, out := runCLI("", "db", "push", "--schema", desiredPath, "--force")
		if code != 0 {
			t.Fatalf("exit %d, output:\n%s", code, out)
		}
		if !tableExists("audit_sentinel") || queryInt(`SELECT count(*) FROM audit_sentinel`) != 1 {
			t.Fatalf("sentinel must survive push --force; output:\n%s", out)
		}
		if !tableExists("_neutron_migrations") || queryInt(`SELECT count(*) FROM _neutron_migrations`) != 1 {
			t.Fatalf("history must survive push --force; output:\n%s", out)
		}
		if queryInt(`SELECT count(*) FROM users`) != 1 {
			t.Fatal("user data must survive")
		}
	})

	t.Run("destructive acknowledgement drops sentinel but never metadata", func(t *testing.T) {
		code, out := runCLI("", "db", "push", "--schema", desiredPath, "--force", "--allow-destructive")
		if code != 0 {
			t.Fatalf("exit %d, output:\n%s", code, out)
		}
		if tableExists("audit_sentinel") {
			t.Fatalf("sentinel must drop with explicit acknowledgement; output:\n%s", out)
		}
		if !tableExists("_neutron_migrations") || queryInt(`SELECT count(*) FROM _neutron_migrations`) != 1 {
			t.Fatalf("history must NEVER drop, even with acknowledgement; output:\n%s", out)
		}
		if queryInt(`SELECT count(*) FROM users`) != 1 {
			t.Fatal("user data must survive")
		}
	})

	t.Run("empty object schema rejected", func(t *testing.T) {
		p := filepath.Join(work, "empty.json")
		writeFile(t, p, "{}")
		code, out := runCLI("", "db", "push", "--schema", p, "--force", "--allow-destructive")
		if code == 0 || !strings.Contains(out, "version") {
			t.Fatalf("push must reject {} with a version error, exit=%d output:\n%s", code, out)
		}
		genDir := filepath.Join(work, "mig2")
		if err := os.MkdirAll(genDir, 0755); err != nil {
			t.Fatal(err)
		}
		code, out = runCLI(genDir, "migrate", "generate", "--schema", p, "--name", "empty")
		if code == 0 || !strings.Contains(out, "version") {
			t.Fatalf("generate must reject {} with a version error, exit=%d output:\n%s", code, out)
		}
	})

	t.Run("unknown version rejected", func(t *testing.T) {
		p := filepath.Join(work, "v2.json")
		writeFile(t, p, `{"version": 2, "tables": []}`)
		code, out := runCLI("", "db", "push", "--schema", p, "--force")
		if code == 0 || !strings.Contains(out, "version 2") {
			t.Fatalf("push must reject version 2, exit=%d output:\n%s", code, out)
		}
	})

	t.Run("duplicate table names rejected", func(t *testing.T) {
		p := filepath.Join(work, "dup.json")
		writeFile(t, p, `{
			"version": 1,
			"tables": [
				{ "name": "users", "columns": [{ "name": "id", "type": "serial", "primaryKey": true }] },
				{ "name": "users", "columns": [{ "name": "id", "type": "serial", "primaryKey": true }] }
			]
		}`)
		code, out := runCLI("", "db", "push", "--schema", p, "--force")
		if code == 0 || !strings.Contains(out, "duplicate table name") {
			t.Fatalf("push must reject duplicate tables, exit=%d output:\n%s", code, out)
		}
	})

	t.Run("statement failure at step 2 rolls back step 1", func(t *testing.T) {
		// flag (nullable) is added first; req (NOT NULL, no default) then
		// fails on the populated users table. The whole transaction —
		// including step 1 — must roll back.
		p := filepath.Join(work, "partial.json")
		writeFile(t, p, `{
			"version": 1,
			"tables": [
				{
					"name": "users",
					"columns": [
						{ "name": "id", "type": "serial", "notNull": true, "primaryKey": true },
						{ "name": "name", "type": "text", "notNull": true },
						{ "name": "email", "type": "text", "notNull": true, "unique": true },
						{ "name": "active", "type": "boolean", "notNull": true, "hasDefault": true, "default": "true" },
						{ "name": "created_at", "type": "timestamptz", "notNull": true, "hasDefault": true, "defaultNow": true },
						{ "name": "flag", "type": "boolean" },
						{ "name": "req", "type": "text", "notNull": true }
					],
					"indexes": []
				}
			]
		}`)
		code, out := runCLI("", "db", "push", "--schema", p, "--force")
		if code == 0 {
			t.Fatalf("push must fail on the not-null column, output:\n%s", out)
		}
		if got := queryInt(`SELECT count(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'flag'`); got != 0 {
			t.Fatalf("step 1 (add column flag) must NOT survive a step-2 failure — atomicity broken; output:\n%s", out)
		}
		if got := queryInt(`SELECT count(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'req'`); got != 0 {
			t.Fatalf("failed column req must not exist; output:\n%s", out)
		}
		if queryInt(`SELECT count(*) FROM users`) != 1 {
			t.Fatal("user data must survive the failed push")
		}
	})

	t.Run("declaring internal table rejected even with all overrides", func(t *testing.T) {
		p := filepath.Join(work, "internal.json")
		writeFile(t, p, `{
			"version": 1,
			"tables": [
				{ "name": "_neutron_migrations", "columns": [{ "name": "version", "type": "text", "primaryKey": true }] }
			]
		}`)
		code, out := runCLI("", "db", "push", "--schema", p, "--force", "--allow-destructive")
		if code == 0 || !strings.Contains(out, "neutron-internal") {
			t.Fatalf("push must refuse to manage internal tables, exit=%d output:\n%s", code, out)
		}
		if !tableExists("_neutron_migrations") || queryInt(`SELECT count(*) FROM _neutron_migrations`) != 1 {
			t.Fatal("history must be intact after the refused push")
		}
	})

	t.Run("forged rename flags rejected", func(t *testing.T) {
		renamedPath := filepath.Join(work, "renamed.json")
		writeFile(t, renamedPath, `{
			"version": 1,
			"tables": [
				{
					"name": "users",
					"columns": [
						{ "name": "id", "type": "serial", "notNull": true, "primaryKey": true },
						{ "name": "full_name", "type": "text", "notNull": true },
						{ "name": "email", "type": "text", "notNull": true, "unique": true },
						{ "name": "active", "type": "boolean", "notNull": true, "hasDefault": true, "default": "true" },
						{ "name": "created_at", "type": "timestamptz", "notNull": true, "hasDefault": true, "defaultNow": true }
					],
					"indexes": []
				}
			]
		}`)
		genDir := filepath.Join(work, "mig3")
		if err := os.MkdirAll(genDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Legit rename succeeds and the generated SQL renames the column.
		code, out := runCLI(genDir, "migrate", "generate", "--schema", renamedPath, "--name", "ren1",
			"--rename", "users.name>users.full_name")
		if code != 0 {
			t.Fatalf("legit rename must work, exit=%d output:\n%s", code, out)
		}
		upFiles, _ := filepath.Glob(filepath.Join(genDir, "migrations", "*ren1*.up.sql"))
		if len(upFiles) != 1 || !strings.Contains(readFile(t, upFiles[0]), `rename column "name" to "full_name"`) {
			t.Fatalf("expected rename statement in generated up.sql, files=%v output:\n%s", upFiles, out)
		}

		// Forged source column (not in the database).
		code, out = runCLI(genDir, "migrate", "generate", "--schema", renamedPath, "--name", "ren2",
			"--rename", "users.ghost>users.full_name")
		if code == 0 || !strings.Contains(out, "source column") {
			t.Fatalf("forged rename source must be rejected, exit=%d output:\n%s", code, out)
		}

		// Forged target column (not in the desired schema).
		code, out = runCLI(genDir, "migrate", "generate", "--schema", renamedPath, "--name", "ren3",
			"--rename", "users.name>users.nope")
		if code == 0 || !strings.Contains(out, "target column") {
			t.Fatalf("forged rename target must be rejected, exit=%d output:\n%s", code, out)
		}
	})

	t.Run("declaring extension-owned table rejected, database unchanged", func(t *testing.T) {
		// M1 live fixture: a REAL pg_depend deptype='e' ownership row via
		// ALTER EXTENSION ADD (no table-creating extension needed locally).
		if err := fixture.Exec(context.Background(), `CREATE EXTENSION IF NOT EXISTS plpgsql`); err != nil {
			t.Fatalf("ensure plpgsql: %v", err)
		}
		if err := fixture.Exec(context.Background(), `CREATE TABLE ext_t (id integer PRIMARY KEY, note text)`); err != nil {
			t.Fatalf("create ext_t: %v", err)
		}
		if err := fixture.Exec(context.Background(), `ALTER EXTENSION plpgsql ADD TABLE ext_t`); err != nil {
			t.Fatalf("mark ext_t extension-owned: %v", err)
		}

		// Desired schema DECLARES the extension-owned table with one extra
		// column: must be rejected naming table and owning extension —
		// never modified, whatever flags are passed.
		p := filepath.Join(work, "extdecl.json")
		writeFile(t, p, `{
			"version": 1,
			"tables": [
				{
					"name": "ext_t",
					"columns": [
						{ "name": "id", "type": "integer", "primaryKey": true },
						{ "name": "note", "type": "text" },
						{ "name": "extra", "type": "text" }
					],
					"indexes": []
				}
			]
		}`)
		code, out := runCLI("", "db", "push", "--schema", p, "--force", "--allow-destructive")
		if code == 0 || !strings.Contains(out, "ext_t") || !strings.Contains(out, "plpgsql") {
			t.Fatalf("push must reject a declared extension-owned table naming table+extension, exit=%d output:\n%s", code, out)
		}
		if got := queryInt(`SELECT count(*) FROM information_schema.columns WHERE table_name = 'ext_t' AND column_name = 'extra'`); got != 0 {
			t.Fatalf("extension-owned table must stay unchanged; extra column was applied. output:\n%s", out)
		}
		if !tableExists("ext_t") {
			t.Fatalf("extension-owned table must still exist; output:\n%s", out)
		}

		// Same hole through migrate generate.
		genDir := filepath.Join(work, "mig4")
		if err := os.MkdirAll(genDir, 0755); err != nil {
			t.Fatal(err)
		}
		code, out = runCLI(genDir, "migrate", "generate", "--schema", p, "--name", "extdecl", "--allow-destructive")
		if code == 0 || !strings.Contains(out, "extension-owned") {
			t.Fatalf("generate must reject the declared extension-owned table too, exit=%d output:\n%s", code, out)
		}
	})

	t.Run("fk-linked destructive drops order by dependency", func(t *testing.T) {
		// L2: aaa (referenced) sorts before zzz (referencing); without FK
		// ordering the whole --allow-destructive push fails atomically.
		if err := fixture.Exec(context.Background(), `CREATE TABLE b03rw_aaa (id integer PRIMARY KEY)`); err != nil {
			t.Fatalf("create aaa: %v", err)
		}
		if err := fixture.Exec(context.Background(), `CREATE TABLE b03rw_zzz (id integer PRIMARY KEY, aaa_id integer REFERENCES b03rw_aaa(id))`); err != nil {
			t.Fatalf("create zzz: %v", err)
		}
		if err := fixture.Exec(context.Background(), `INSERT INTO b03rw_aaa VALUES (1)`); err != nil {
			t.Fatal(err)
		}
		if err := fixture.Exec(context.Background(), `INSERT INTO b03rw_zzz VALUES (1, 1)`); err != nil {
			t.Fatal(err)
		}

		code, out := runCLI("", "db", "push", "--schema", desiredPath, "--force", "--allow-destructive")
		if code != 0 {
			t.Fatalf("FK-linked drops must succeed in dependency order with intent, exit=%d output:\n%s", code, out)
		}
		if tableExists("b03rw_aaa") || tableExists("b03rw_zzz") {
			t.Fatalf("both FK-linked tables must be dropped; output:\n%s", out)
		}
		if queryInt(`SELECT count(*) FROM users`) != 1 {
			t.Fatal("user data must survive the destructive push")
		}
	})
}

func buildCLIBinary(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source path")
	}
	moduleRoot := filepath.Join(filepath.Dir(thisFile), "..")
	bin := filepath.Join(t.TempDir(), "neutron-cli")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = moduleRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	return bin
}

func deriveDatabaseURL(t *testing.T, base, dbName string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse NEUTRON_E2E_DATABASE_URL: %v", err)
	}
	u.Path = "/" + dbName
	return u.String()
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
