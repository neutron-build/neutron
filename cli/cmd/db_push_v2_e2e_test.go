package cmd

// End-to-end V10 coverage for schema document v2 through the REAL CLI
// binary against disposable m02_* Postgres databases: db push (create,
// converge, same-name index change, rename with data preservation,
// blocked plans) and migrate generate. Skipped unless
// NEUTRON_E2E_DATABASE_URL is set (or NEUTRON_LIVE_REQUIRED=1 fails).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

func newM02CommandDB(t *testing.T, label string) (string, *db.Client) {
	t.Helper()
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; v2 push E2E skipped (set it to a disposable Postgres URL to run)")
	}
	dbName := fmt.Sprintf("m02_cmd_%s_%d_%d", label, os.Getpid(), time.Now().UnixNano()%1_000_000)
	dbURL := deriveDatabaseURL(t, base, dbName)
	admin, err := db.Connect(context.Background(), base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
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
	client, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect case db: %v", err)
	}
	t.Cleanup(client.Close)
	return dbURL, client
}

const m02CmdDocV2A = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}, {"name": "app"}],
	"tables": [
		{
			"identity": {"schema": "public", "name": "users"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "always"}},
				{"name": "email", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'x'"}},
				{"name": "role", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "public", "name": "role"}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'user'"}}
			],
			"constraints": [
				{"name": "users_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "users_id_email_key", "type": "unique", "columns": ["id", "email"]}
			],
			"indexes": []
		},
		{
			"identity": {"schema": "app", "name": "posts"},
			"managed": true,
			"columns": [
				{"name": "author", "type": {"name": "int8", "codec": "bigint"}, "notNull": true},
				{"name": "author_email", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true},
				{"name": "slug", "type": {"name": "varchar", "codec": "string", "params": {"length": 80}}, "notNull": true},
				{"name": "tags", "type": {"name": "text", "codec": "array", "array": true}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'{}'"}}
			],
			"constraints": [
				{"name": "posts_pk", "type": "primary-key", "columns": ["author", "slug"]},
				{"name": "posts_slug_check", "type": "check", "expression": "slug <> ''"},
				{"name": "posts_author_fkey", "type": "foreign-key", "columns": ["author", "author_email"],
				 "references": {"table": {"schema": "public", "name": "users"}, "columns": ["id", "email"], "onDelete": "cascade"}}
			],
			"indexes": [
				{"identity": {"schema": "app", "name": "posts_slug_idx"}, "unique": false, "method": "btree",
				 "key": [{"expression": "lower(slug)"}], "where": "slug <> ''"}
			]
		}
	],
	"enums": [
		{"identity": {"schema": "public", "name": "role"}, "managed": true, "values": ["user", "admin"]}
	],
	"views": [
		{"identity": {"schema": "public", "name": "v_users"}, "managed": true,
		 "definition": "select id, email from users"}
	],
	"opaque": []
}`

// m02CmdDocV2B changes the same-name index predicate, the FK action, the
// email default and appends an enum value; renames nothing.
const m02CmdDocV2B = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}, {"name": "app"}],
	"tables": [
		{
			"identity": {"schema": "public", "name": "users"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int8", "codec": "bigint"}, "notNull": true,
				 "default": {"kind": "identity", "generated": "always"}},
				{"name": "email", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'y'"}},
				{"name": "role", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "public", "name": "role"}}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'user'"}}
			],
			"constraints": [
				{"name": "users_pkey", "type": "primary-key", "columns": ["id"]},
				{"name": "users_id_email_key", "type": "unique", "columns": ["id", "email"]}
			],
			"indexes": []
		},
		{
			"identity": {"schema": "app", "name": "posts"},
			"managed": true,
			"columns": [
				{"name": "author", "type": {"name": "int8", "codec": "bigint"}, "notNull": true},
				{"name": "author_email", "type": {"name": "varchar", "codec": "string", "params": {"length": 120}}, "notNull": true},
				{"name": "slug", "type": {"name": "varchar", "codec": "string", "params": {"length": 80}}, "notNull": true},
				{"name": "tags", "type": {"name": "text", "codec": "array", "array": true}, "notNull": true,
				 "default": {"kind": "literal", "sql": "'{}'"}}
			],
			"constraints": [
				{"name": "posts_pk", "type": "primary-key", "columns": ["author", "slug"]},
				{"name": "posts_slug_check", "type": "check", "expression": "slug <> ''"},
				{"name": "posts_author_fkey", "type": "foreign-key", "columns": ["author", "author_email"],
				 "references": {"table": {"schema": "public", "name": "users"}, "columns": ["id", "email"], "onDelete": "restrict"}}
			],
			"indexes": [
				{"identity": {"schema": "app", "name": "posts_slug_idx"}, "unique": false, "method": "btree",
				 "key": [{"expression": "lower(slug)"}], "where": "slug like 'a%'"}
			]
		}
	],
	"enums": [
		{"identity": {"schema": "public", "name": "role"}, "managed": true, "values": ["user", "admin", "bot"]}
	],
	"views": [
		{"identity": {"schema": "public", "name": "v_users"}, "managed": true,
		 "definition": "select id, email from users"}
	],
	"opaque": []
}`

func TestDBPushV2E2E(t *testing.T) {
	dbURL, fixture := newM02CommandDB(t, "push")
	bin := buildCLIBinary(t)
	work := t.TempDir()
	docA := filepath.Join(work, "a.json")
	docB := filepath.Join(work, "b.json")
	writeFile(t, docA, m02CmdDocV2A)
	writeFile(t, docB, m02CmdDocV2B)

	run := func(args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}
	queryStr := func(sql string) string {
		t.Helper()
		var v string
		if err := fixture.QueryRow(context.Background(), sql).Scan(&v); err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		return v
	}

	t.Run("CreateThenConverge", func(t *testing.T) {
		code, out := run("db", "push", "--schema", docA)
		if code != 0 {
			t.Fatalf("initial v2 push failed (%d):\n%s", code, out)
		}
		code, out = run("db", "push", "--schema", docA)
		if code != 0 {
			t.Fatalf("second v2 push failed (%d):\n%s", code, out)
		}
		if !strings.Contains(out, "in sync") {
			t.Fatalf("second push must report sync, got:\n%s", out)
		}
		if got := queryStr(`SELECT count(*)::text FROM pg_tables WHERE schemaname='app' AND tablename='posts'`); got != "1" {
			t.Fatalf("app.posts must exist")
		}
	})

	t.Run("MigrateGenerateV2", func(t *testing.T) {
		gen := filepath.Join(work, "mig")
		if err := os.MkdirAll(gen, 0o755); err != nil {
			t.Fatal(err)
		}
		// Database currently at doc A state: generating to doc B plans the
		// forward changes without applying anything.
		code, out := run("migrate", "generate", "--schema", docB, "--dir", gen, "--name", "to_b")
		if code != 0 {
			t.Fatalf("migrate generate v2 failed (%d):\n%s", code, out)
		}
		up := readFile(t, filepath.Join(gen, "001_to_b.up.sql"))
		for _, want := range []string{
			`alter type "public"."role" add value 'bot'`,
			`drop index if exists "app"."posts_slug_idx"`,
			`references "public"."users" ("id", "email") on delete restrict`,
		} {
			if !strings.Contains(up, want) {
				t.Fatalf("generated migration missing %q:\n%s", want, up)
			}
		}
	})

	t.Run("ModifySameNameIndexFKDefaultEnum", func(t *testing.T) {
		code, out := run("db", "push", "--schema", docB)
		if code != 0 {
			t.Fatalf("modified v2 push failed (%d):\n%s", code, out)
		}
		code, out = run("db", "push", "--schema", docB)
		if code != 0 || !strings.Contains(out, "in sync") {
			t.Fatalf("modified push must converge (%d):\n%s", code, out)
		}
		if got := queryStr(`SELECT indexdef FROM pg_indexes WHERE schemaname='app' AND indexname='posts_slug_idx'`); !strings.Contains(got, "'a%'::text") {
			t.Fatalf("same-name index predicate must change in place, got: %s", got)
		}
		if got := queryStr(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='app.posts'::regclass AND conname='posts_author_fkey'`); !strings.Contains(got, "ON DELETE RESTRICT") {
			t.Fatalf("FK action must change, got: %s", got)
		}
		if got := queryStr(`SELECT count(*)::text FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='role'`); got != "3" {
			t.Fatalf("enum value must append, got %s values", got)
		}
	})

	t.Run("RenamePreservesData", func(t *testing.T) {
		if err := fixture.Exec(context.Background(),
			`INSERT INTO public.users (email) VALUES ('seed@x.com')`); err != nil {
			t.Fatal(err)
		}
		docC := strings.ReplaceAll(m02CmdDocV2B, `"email"`, `"contact"`)
		docC = strings.Replace(docC, `"definition": "select id, email from users"`, `"definition": "select id, contact from users"`, 1)
		docCPath := filepath.Join(work, "c.json")
		writeFile(t, docCPath, docC)

		// Without the flag the push is destructive-refusing, not a rename.
		code, _ := run("db", "push", "--schema", docCPath)
		if code == 0 {
			t.Fatal("push without rename flag must not silently succeed")
		}

		code, out := run("db", "push", "--schema", docCPath, "--rename", "public.users.email>public.users.contact")
		if code != 0 {
			t.Fatalf("rename push failed (%d):\n%s", code, out)
		}
		if got := queryStr(`SELECT count(*)::text FROM public.users WHERE contact = 'seed@x.com'`); got != "1" {
			t.Fatalf("renamed column must preserve the seeded row")
		}
		code, out = run("db", "push", "--schema", docCPath)
		if code != 0 || !strings.Contains(out, "in sync") {
			t.Fatalf("post-rename push must converge (%d):\n%s", code, out)
		}
	})

	t.Run("BlockedPlanExitsNonzero", func(t *testing.T) {
		if err := fixture.Exec(context.Background(), `CREATE TABLE unman (p int, g int GENERATED ALWAYS AS (p + 1) STORED)`); err != nil {
			t.Fatal(err)
		}
		blocked := `{
			"version": 2, "dialect": "postgresql", "capabilities": [],
			"schemas": [{"name": "public"}],
			"tables": [{
				"identity": {"schema": "public", "name": "unman"}, "managed": true,
				"columns": [{"name": "p", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
				"constraints": [{"name": "unman_pkey", "type": "primary-key", "columns": ["p"]}],
				"indexes": []
			}],
			"enums": [], "views": [], "opaque": []
		}`
		blockedPath := filepath.Join(work, "blocked.json")
		writeFile(t, blockedPath, blocked)
		code, out := run("db", "push", "--schema", blockedPath)
		if code == 0 || !strings.Contains(out, "generated column") {
			t.Fatalf("declaring an unrepresentable table must fail loudly, code=%d out:\n%s", code, out)
		}
	})

	t.Run("VersionOnePathUnchanged", func(t *testing.T) {
		v1 := filepath.Join(work, "v1.json")
		writeFile(t, v1, `{"version":1,"tables":[{"name":"legacy_users","columns":[{"name":"id","type":"serial","notNull":true,"primaryKey":true},{"name":"n","type":"text"}],"indexes":[]}]}`)
		code, out := run("db", "push", "--schema", v1, "--force")
		if code != 0 {
			t.Fatalf("v1 push regression failed (%d):\n%s", code, out)
		}
		if got := queryStr(`SELECT count(*)::text FROM pg_tables WHERE tablename='legacy_users'`); got != "1" {
			t.Fatal("v1 push must keep working")
		}
	})
}
