package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestMigrateApplySafetyE2E exercises the M05 apply/recovery contract
// through the REAL CLI binary against REAL disposable Postgres databases
// (prefix m05_): destructive acknowledgement, protected-object guarding at
// apply time (the B03-L1 closure — widened in rework to every DROP form,
// DROP EXTENSION, DROP OWNED, CASCADE transitives, ALTERs and row-writing
// DML), managed-drift refusal under the lock, stale-plan refusal, down
// reversibility limits, nontransactional concurrent-index runs with
// postcondition reconciliation (resolve retry/mark-applied/abort,
// including the ALTER+CIC interrupted-after-ALTER abort recovery),
// read-only status, and push serialization under the migration advisory
// lock.
//
// Every refusal is first reproduced as a FAILURE on the pre-M05 binary
// (built from the pinned pre-M05 revision 35858e6e via read-only
// `git archive`; the working tree is never stashed or checked out). State is asserted by inspecting the database
// with raw SQL, not command output.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server, or when NEUTRON_LIVE_REQUIRED=1 the missing URL is a failure.
func TestMigrateApplySafetyE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; M05 apply-safety E2E skipped (set it to a disposable Postgres URL to run)")
	}

	bin := buildCLIBinary(t)
	preM05Bin := buildPreM05CLIBinary(t)

	newDB := func(t *testing.T) string {
		t.Helper()
		dbName := fmt.Sprintf("m05_%d_%d", os.Getpid(), time.Now().UnixNano())
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
			// Review-8 LOW-1 mitigation: under load (pinned-reference
			// binary builds, -race instrumentation, busy dev machines) the
			// original 15s window fired as a flake on DROP DATABASE —
			// harness robustness only, never product behavior. Bounded at
			// 45s with one retry.
			for attempt := 0; attempt < 2; attempt++ {
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				if err := admin.Exec(ctx, fmt.Sprintf(
					`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, dbName,
				)); err != nil {
					t.Errorf("terminate backends: %v", err)
				}
				err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName))
				cancel()
				if err == nil {
					return
				}
				if attempt == 1 {
					t.Errorf("drop database %s: %v", dbName, err)
				}
			}
		})
		return dbURL
	}

	query := func(t *testing.T, dbURL, sql string) string {
		t.Helper()
		c, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer c.Close()
		var v string
		if err := c.QueryRow(context.Background(), sql).Scan(&v); err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		return v
	}

	execRaw := func(t *testing.T, dbURL, sql string) {
		t.Helper()
		c, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer c.Close()
		if err := c.Exec(context.Background(), sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	runCLI := func(t *testing.T, dbURL string, args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	writeMigrations := func(t *testing.T, files map[string]string) string {
		t.Helper()
		dir := t.TempDir()
		for name, sql := range files {
			writeFile(t, filepath.Join(dir, name), sql)
		}
		return dir
	}

	// pullDoc introspects a reference DB into a v2 document (read-only).
	pullDoc := func(t *testing.T, dbURL, path string) {
		t.Helper()
		if code, out := runCLI(t, dbURL, "schema", "pull", "--out", path); code != 0 {
			t.Fatalf("schema pull failed (%d): %s", code, out)
		}
	}

	addPostsTable := func(m map[string]any) {
		posts := map[string]any{
			"identity": map[string]any{"schema": "public", "name": "posts"},
			"managed":  true,
			"columns": []any{
				map[string]any{"name": "id", "type": map[string]any{"name": "int4", "codec": "number"}, "notNull": true},
				map[string]any{"name": "title", "type": map[string]any{"name": "text", "codec": "string"}, "notNull": true},
			},
			"constraints": []any{},
			"indexes":     []any{},
		}
		m["tables"] = append(m["tables"].([]any), posts)
	}

	editDocJSON := func(t *testing.T, src, dst string, edit func(m map[string]any)) {
		t.Helper()
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		edit(m)
		out, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, out, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	waitForLockHeld := func(t *testing.T, dbURL string) bool {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			var n string
			c, err := db.Connect(context.Background(), dbURL)
			if err == nil {
				err = c.QueryRow(context.Background(),
					"SELECT count(*) FROM pg_locks WHERE "+advisoryLockPredicate).Scan(&n)
				c.Close()
			}
			if err == nil && n != "0" {
				return true
			}
			time.Sleep(50 * time.Millisecond)
		}
		return false
	}

	waitForLockReleased := func(t *testing.T, dbURL string) {
		t.Helper()
		deadline := time.Now().Add(45 * time.Second)
		for {
			n := query(t, dbURL, "SELECT count(*) FROM pg_locks WHERE "+advisoryLockPredicate)
			if n == "0" || !time.Now().Before(deadline) {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}

	// ------------------------------------------------------------------
	// A. Destructive acknowledgement at apply time.
	// ------------------------------------------------------------------
	t.Run("DestructiveAcknowledgementRequired", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE keepme (id int PRIMARY KEY)`)
		execRaw(t, dbURL, `INSERT INTO keepme VALUES (7)`)
		dir := writeMigrations(t, map[string]string{
			"001_drop_old.up.sql":   "DROP TABLE keepme;",
			"001_drop_old.down.sql": "CREATE TABLE keepme (id int PRIMARY KEY);",
		})

		// FAIL-BEFORE (pre-M05 binary): applies the drop with no ack.
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("pre-M05 binary must apply destructive migration ungated (fail-before setup): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='keepme'`); got != "0" {
			t.Fatalf("fail-before state wrong: keepme still exists")
		}
		// Restore for the real assertion.
		execRaw(t, dbURL, `CREATE TABLE keepme (id int PRIMARY KEY)`)
		execRaw(t, dbURL, `INSERT INTO keepme VALUES (7)`)
		execRaw(t, dbURL, `DROP TABLE _neutron_migrations`)

		code, out := runCLI(t, dbURL, "migrate", "--dir", dir)
		if code == 0 {
			t.Fatal("destructive migration applied without acknowledgement")
		}
		for _, want := range []string{"destructive", "--allow-destructive", "DROP TABLE keepme"} {
			if !strings.Contains(out, want) {
				t.Errorf("refusal missing %q: %s", want, out)
			}
		}
		if got := query(t, dbURL, `SELECT id FROM keepme WHERE id=7`); got != "7" {
			t.Fatalf("refusal must preserve data, keepme=%s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("refusal must precede history writes, rows=%s", got)
		}

		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--allow-destructive"); code != 0 {
			t.Fatalf("acknowledged destructive migration refused: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='keepme'`); got != "0" {
			t.Fatalf("acknowledged drop did not apply")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows = %s, want 1", got)
		}
	})

	// ------------------------------------------------------------------
	// B. Protected objects: metadata + extension-owned, never bypassable.
	// ------------------------------------------------------------------
	t.Run("ProtectedObjectsRefusedAtApply", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE EXTENSION IF NOT EXISTS plpgsql`)
		execRaw(t, dbURL, `CREATE TABLE ext_guard_t (id int PRIMARY KEY)`)
		execRaw(t, dbURL, `ALTER EXTENSION plpgsql ADD TABLE ext_guard_t`)

		base := writeMigrations(t, map[string]string{
			"001_init.up.sql": "CREATE TABLE app_t (id int);",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", base); code != 0 {
			t.Fatalf("init migrate failed: %s", out)
		}

		dir := writeMigrations(t, map[string]string{
			"001_init.up.sql":       "CREATE TABLE app_t (id int);",
			"002_trunc_meta.up.sql": "TRUNCATE TABLE _neutron_migrations;",
		})

		// FAIL-BEFORE (pre-M05): TRUNCATE of the metadata table applies
		// silently (the history write afterwards still succeeds) — B03's
		// documented L1 limitation. (The extension-table DROP needs no
		// fail-before: PostgreSQL itself refuses deptype='e' drops; the
		// guarded refusal below is our own earlier, clearer boundary.)
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("pre-M05 binary must apply the protected statement (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "0" {
			t.Fatalf("fail-before: TRUNCATE destroyed applied history (001's row vanished)")
		}

		// Rebuild the guarded state: 001 applied, and a pending attack on
		// both protected scopes. (The extension fixture from setup is
		// untouched: the fail-before dir never reached its drop.)
		execRaw(t, dbURL, `DROP TABLE app_t`)
		execRaw(t, dbURL, `TRUNCATE _neutron_migrations`)
		execRaw(t, dbURL, `INSERT INTO _neutron_migrations (version, name, checksum, owner, format)
			VALUES ('001','init',NULL,'neutron-cli','v2')`)
		writeFile(t, filepath.Join(dir, "003_drop_ext.up.sql"), "DROP TABLE ext_guard_t;")

		code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--allow-destructive")
		if code == 0 {
			t.Fatal("protected statements accepted even with --allow-destructive")
		}
		for _, want := range []string{"protected", "_neutron_migrations", "ext_guard_t", "extension plpgsql"} {
			if !strings.Contains(out, want) {
				t.Errorf("refusal missing %q: %s", want, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("refusal must not touch history, rows=%s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='ext_guard_t'`); got != "1" {
			t.Fatalf("refusal must leave the extension-owned table intact")
		}
	})

	// ------------------------------------------------------------------
	// C. Managed drift under the lock aborts apply before any DDL.
	// ------------------------------------------------------------------
	t.Run("DriftBlocksApplyUnderLock", func(t *testing.T) {
		dbURL := newDB(t)
		ref := newDB(t)
		execRaw(t, ref, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		proj := t.TempDir()
		docA := filepath.Join(proj, "docA.json")
		pullDoc(t, ref, docA)
		docB := filepath.Join(proj, "docB.json")
		editDocJSON(t, docA, docB, addPostsTable)

		mig := filepath.Join(proj, "migrations")
		if code, out := runCLI(t, dbURL, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "add_users"); code != 0 {
			t.Fatalf("generate 001 failed (%d): %s", code, out)
		}
		if code, out := runCLI(t, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("apply 001 failed (%d): %s", code, out)
		}
		execRaw(t, dbURL, `CREATE SCHEMA side; CREATE TABLE side.keep (k text); INSERT INTO side.keep VALUES ('mine')`)

		if code, out := runCLI(t, dbURL, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docB, "--name", "add_posts"); code != 0 {
			t.Fatalf("generate 002 failed (%d): %s", code, out)
		}

		// Drift: a column added outside migration files (raw SQL oracle).
		execRaw(t, dbURL, `ALTER TABLE users ADD COLUMN sneaky text`)

		// FAIL-BEFORE (pre-M05): pending 002 applies straight over drift.
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("pre-M05 binary must apply over drift (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='posts'`); got != "1" {
			t.Fatalf("fail-before state wrong: posts missing")
		}
		execRaw(t, dbURL, `DROP TABLE posts`)
		execRaw(t, dbURL, `DELETE FROM _neutron_migrations WHERE version='002'`)

		code, out := runCLI(t, dbURL, "migrate", "--dir", mig)
		if code == 0 || !strings.Contains(out, "drift") {
			t.Fatalf("drifted database accepted for apply (code %d): %s", code, out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='posts'`); got != "0" {
			t.Fatalf("pending migration ran despite drift")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.columns WHERE table_name='users' AND column_name='sneaky'`); got != "1" {
			t.Fatalf("refusal must leave the (hand-made) drift untouched")
		}
		if got := query(t, dbURL, `SELECT k FROM side.keep`); got != "mine" {
			t.Fatalf("unmanaged sentinel disturbed: %s", got)
		}

		// Remove the drift: apply proceeds.
		execRaw(t, dbURL, `ALTER TABLE users DROP COLUMN sneaky`)
		if code, out := runCLI(t, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("post-drift-cleanup migrate failed (%d): %s", code, out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "2" {
			t.Fatalf("history rows = %s, want 2", got)
		}
	})

	// ------------------------------------------------------------------
	// D. Stale plan.json (hand-edited up.sql after generation) refuses.
	// ------------------------------------------------------------------
	t.Run("StalePlanRefusedBeforeApply", func(t *testing.T) {
		dbURL := newDB(t)
		ref := newDB(t)
		execRaw(t, ref, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		proj := t.TempDir()
		docA := filepath.Join(proj, "docA.json")
		pullDoc(t, ref, docA)
		docB := filepath.Join(proj, "docB.json")
		editDocJSON(t, docA, docB, addPostsTable)

		mig := filepath.Join(proj, "migrations")
		if code, out := runCLI(t, dbURL, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "add_users"); code != 0 {
			t.Fatalf("generate 001 failed (%d): %s", code, out)
		}
		if code, out := runCLI(t, dbURL, "migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docB, "--name", "add_posts"); code != 0 {
			t.Fatalf("generate 002 failed (%d): %s", code, out)
		}

		// Hand-edit the PENDING 002 after generation.
		up2 := filepath.Join(mig, "002_add_posts.up.sql")
		writeFile(t, up2, string(mustReadFile(t, up2))+"\nCREATE TABLE hacked (id int);\n")

		// FAIL-BEFORE (pre-M05): the stale plan is never consulted.
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("pre-M05 binary must apply the stale-planned migration (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='hacked'`); got != "1" {
			t.Fatalf("fail-before state wrong: hacked missing")
		}
		execRaw(t, dbURL, `DROP TABLE users, posts, hacked`)
		execRaw(t, dbURL, `DROP TABLE _neutron_migrations`)

		code, out := runCLI(t, dbURL, "migrate", "--dir", mig)
		if code == 0 || !strings.Contains(out, "stale plan") {
			t.Fatalf("stale plan accepted (code %d): %s", code, out)
		}
		// No user-visible schema object may exist (the empty history
		// table's under-lock creation is the documented metadata birth,
		// not a user mutation).
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('users','posts','hacked')`); got != "0" {
			t.Fatalf("refusal must precede all mutations, tables present: %s", got)
		}
	})

	// ------------------------------------------------------------------
	// E. Down reversibility limits.
	// ------------------------------------------------------------------
	t.Run("DownIrreversibleRefused", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_a.up.sql":   "CREATE TABLE irp (id int);",
			"001_a.down.sql": "DROP TABLE IF EXISTS irp;",
			"002_b.up.sql":   "DROP TABLE irp;",
			"002_b.down.sql": "-- IRREVERSIBLE: dropped table data cannot be restored\n",
			"003_c.up.sql":   "CREATE TABLE irp2 (id int);",
			"003_c.down.sql": "DROP TABLE irp2;",
			"003_c.plan.json": `{
  "formatVersion": 1,
  "workflow": "snapshot-v1",
  "mode": "snapshot",
  "migrationVersion": "003",
  "migrationName": "c",
  "baseSource": "002_b",
  "baseSha256": "0000000000000000000000000000000000000000000000000000000000000000",
  "targetSha256": "0000000000000000000000000000000000000000000000000000000000000000",
  "transactionMode": "single",
  "operations": [
    {"index": 1, "sql": "CREATE TABLE irp2 (id int);", "down": "DROP TABLE irp2;", "destructive": false, "dataLoss": false, "reversibility": "irreversible"}
  ],
  "risk": {"hasDestructive": false, "hasDataLoss": false, "statementCount": 1, "irreversibleCount": 1, "overallReversibility": "irreversible"},
  "caveats": []
}`,
		})

		// Guarded gate visible first: the run refuses while a pending
		// statement is destructive without acknowledgement.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code == 0 || !strings.Contains(out, "--allow-destructive") {
			t.Fatalf("destructive pending migration applied without acknowledgement (code %d): %s", code, out)
		}

		// FAIL-BEFORE (pre-M05): the pre-M05 binary applies everything
		// ungated, and its `down 3` "reverts" 002 by executing ZERO SQL —
		// the comment-only stub runs as nothing and the history row
		// silently disappears (the fake rollback).
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("pre-M05 binary must apply ungated (fail-before): %s", out)
		}
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "down", "3", "--dir", dir); code != 0 {
			t.Fatalf("pre-M05 binary must fake the rollback (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("fail-before: fake rollback left history rows = %s, want 0 (002's row vanished without SQL)", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name IN ('irp','irp2')`); got != "0" {
			t.Fatalf("fail-before: nothing was restored (no SQL ran)")
		}

		// Guarded: apply everything with the acknowledgement, then down
		// must REFUSE — a comment-only stub is not a restoration, and a
		// plan.json marking 003 irreversible refuses even a real down.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--allow-destructive"); code != 0 {
			t.Fatalf("acknowledged apply failed: %s", out)
		}
		code, out := runCLI(t, dbURL, "migrate", "down", "1", "--dir", dir)
		if code == 0 || !strings.Contains(out, "irreversible") || !strings.Contains(out, "plan") {
			t.Fatalf("plan-irreversible down accepted (code %d): %s", code, out)
		}
		code, out = runCLI(t, dbURL, "migrate", "down", "2", "--dir", dir)
		if code == 0 {
			t.Fatal("down over a comment-only IRREVERSIBLE stub accepted")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "3" {
			t.Fatalf("refused down must not delete history rows, got %s", got)
		}

		// Repairing the artifacts (real down SQL, plan removed) makes
		// `down 2` work again: structure restored; data is still gone —
		// the documented limit of down SQL.
		writeFile(t, filepath.Join(dir, "002_b.down.sql"), "CREATE TABLE irp (id int);\n")
		os.Remove(filepath.Join(dir, "003_c.plan.json"))
		if code, out := runCLI(t, dbURL, "migrate", "down", "2", "--dir", dir); code != 0 {
			t.Fatalf("down over repaired reversible downs failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='irp'`); got != "1" {
			t.Fatalf("repaired down must restore structure")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after down = %s, want 1", got)
		}
	})

	// ------------------------------------------------------------------
	// F. Nontransactional concurrent indexes: failure leaves inspectable
	//    partial state; resolve --retry reconciles postconditions.
	// ------------------------------------------------------------------
	t.Run("ConcurrentIndexFailureAndResolveRetry", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE big (a int, b int, c int)`)
		execRaw(t, dbURL, `INSERT INTO big SELECT g, g, g FROM generate_series(1, 500) g`)

		dir := writeMigrations(t, map[string]string{
			"001_idx.up.sql":   "CREATE INDEX CONCURRENTLY cic_a ON big (a);\nCREATE INDEX CONCURRENTLY cic_bad ON big (nosuchcol);",
			"001_idx.down.sql": "DROP INDEX IF EXISTS cic_a;\nDROP INDEX IF EXISTS cic_bad;",
		})

		code, out := runCLI(t, dbURL, "migrate", "--dir", dir)
		if code == 0 {
			t.Fatal("failing concurrent migration reported success")
		}
		for _, want := range []string{"outside any transaction", "resolve"} {
			if !strings.Contains(out, want) {
				t.Errorf("failure output missing %q: %s", want, out)
			}
		}
		// Independent oracle: cic_a IS present (no pretend rollback),
		// cic_bad absent, no history row.
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='cic_a'`); got != "1" {
			t.Fatalf("cic_a must remain after the failed run (no pretend rollback), got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='cic_bad'`); got != "0" {
			t.Fatalf("cic_bad must not exist")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("history rows = %s, want 0", got)
		}

		// Status reports the interrupted state readably.
		code, out = runCLI(t, dbURL, "migrate", "status", "--dir", dir)
		if code != 0 {
			t.Fatalf("status failed: %s", out)
		}
		if !strings.Contains(out, "INTERRUPTED") || !strings.Contains(out, "resolve") {
			t.Fatalf("status must report INTERRUPTED with the resolve directive: %s", out)
		}

		// Bare resolve prints the postcondition report.
		code, out = runCLI(t, dbURL, "migrate", "resolve", "001", "--dir", dir)
		if code != 0 {
			t.Fatalf("resolve report failed: %s", out)
		}
		for _, want := range []string{"satisfied", "unsatisfied", "cic_a", "cic_bad"} {
			if !strings.Contains(out, want) {
				t.Errorf("resolve report missing %q: %s", want, out)
			}
		}

		// Fix the file (never applied: edits are legal) and retry with
		// verified skipping — exactly one effect per statement.
		writeFile(t, filepath.Join(dir, "001_idx.up.sql"),
			"CREATE INDEX CONCURRENTLY cic_a ON big (a);\nCREATE INDEX CONCURRENTLY cic_b ON big (b);")
		writeFile(t, filepath.Join(dir, "001_idx.down.sql"),
			"DROP INDEX IF EXISTS cic_a;\nDROP INDEX IF EXISTS cic_b;")
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--retry", "--dir", dir); code != 0 {
			t.Fatalf("resolve --retry failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='cic_a'`); got != "1" {
			t.Fatalf("cic_a re-created (retry must skip satisfied postconditions)")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='cic_b'`); got != "1" {
			t.Fatalf("cic_b missing after retry")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after retry = %s, want 1", got)
		}

		// Repeat application is a no-op; indexes stay exactly-once.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("repeat migrate failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE tablename='big' AND indexname LIKE 'cic%'`); got != "2" {
			t.Fatalf("cic indexes = %s, want exactly 2", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after repeat = %s, want 1", got)
		}

		// Resolve on an applied version is an exit-0 report.
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--dir", dir); code != 0 || !strings.Contains(out, "applied") {
			t.Fatalf("resolve on applied version must report cleanly (code %d): %s", code, out)
		}
	})

	// ------------------------------------------------------------------
	// G. Kill mid-nontransactional run: durable partial state, explicit
	//    --abort recovery, then a clean retry.
	// ------------------------------------------------------------------
	t.Run("KillDuringNontransactionalThenAbort", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE big2 (a int, b int)`)
		execRaw(t, dbURL, `INSERT INTO big2 SELECT g, g FROM generate_series(1, 100) g`)
		dir := writeMigrations(t, map[string]string{
			"001_k.up.sql":   "CREATE INDEX CONCURRENTLY k_ia ON big2 (a);\nSELECT pg_sleep(8);\nCREATE INDEX CONCURRENTLY k_ib ON big2 (b);",
			"001_k.down.sql": "DROP INDEX IF EXISTS k_ia;\nDROP INDEX IF EXISTS k_ib;",
		})

		proc := exec.Command(bin, "--url", dbURL, "migrate", "--dir", dir, "--timeout", "120s")
		if err := proc.Start(); err != nil {
			t.Fatal(err)
		}
		// Wait until index k_ia exists AND the runner is inside pg_sleep:
		// statement 1 done, statements 2-3 pending — the kill window.
		deadline := time.Now().Add(30 * time.Second)
		inWindow := false
		for time.Now().Before(deadline) {
			ia := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='k_ia'`)
			sleeping := query(t, dbURL, `SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%pg_sleep%' AND pid <> pg_backend_pid()`)
			if ia == "1" && sleeping != "0" {
				inWindow = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !inWindow {
			proc.Process.Kill()
			t.Fatal("kill window (k_ia present + pg_sleep active) never observed")
		}
		if err := proc.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = proc.Wait()

		waitForLockReleased(t, dbURL)

		// Durable state before any retry (V12): partial effects, no history.
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='k_ia'`); got != "1" {
			t.Fatalf("k_ia must survive the kill, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='k_ib'`); got != "0" {
			t.Fatalf("k_ib must not exist after kill")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("history rows after kill = %s, want 0", got)
		}

		// Status shows the interrupted state.
		if code, out := runCLI(t, dbURL, "migrate", "status", "--dir", dir); code != 0 || !strings.Contains(out, "INTERRUPTED") {
			t.Fatalf("status must report INTERRUPTED after kill (code %d): %s", code, out)
		}

		// A plain retry must NOT silently replay: the SELECT makes the
		// file unverifiable, so resolve --retry refuses with the reason.
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--retry", "--dir", dir); code == 0 {
			t.Fatal("retry accepted with unverifiable statements after partial effects")
		} else if !strings.Contains(out, "unverifiable") {
			t.Errorf("retry refusal must name the unverifiable statements: %s", out)
		}

		// Explicit abort: down SQL removes the partial effects; history
		// stays empty; a plain migrate then applies everything.
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--abort", "--dir", dir); code != 0 {
			t.Fatalf("resolve --abort failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname IN ('k_ia','k_ib')`); got != "0" {
			t.Fatalf("abort must remove partial effects, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("abort must not write history, rows=%s", got)
		}
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "60s"); code != 0 {
			t.Fatalf("post-abort migrate failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname IN ('k_ia','k_ib')`); got != "2" {
			t.Fatalf("post-abort migrate must create both indexes, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after recovery = %s, want 1", got)
		}
	})

	// ------------------------------------------------------------------
	// H. mark-applied verifies postconditions; unverifiable refuses.
	// ------------------------------------------------------------------
	t.Run("MarkAppliedVerifiesPostconditions", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_mk.up.sql":  "CREATE TABLE mk (id int);",
			"002_dml.up.sql": "CREATE TABLE mk2 (id int);\nINSERT INTO mk2 VALUES (1);",
		})

		// Apply 001's SQL by hand (independent oracle), leaving no history.
		execRaw(t, dbURL, `CREATE TABLE mk (id int)`)

		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--mark-applied", "--dir", dir); code != 0 {
			t.Fatalf("mark-applied over fully-present DDL failed: %s", out)
		}
		wantChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte("CREATE TABLE mk (id int);")))
		if got := query(t, dbURL, `SELECT checksum FROM _neutron_migrations WHERE version='001'`); got != wantChecksum {
			t.Fatalf("mark-applied checksum = %s, want %s", got, wantChecksum)
		}

		// mark-applied on a migration with DML: INSERT has no checkable
		// postcondition — never asserted, never replayed. (Checked BEFORE
		// any run applies 002.)
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "002", "--mark-applied", "--dir", dir); code == 0 {
			t.Fatal("mark-applied accepted an unverifiable DML migration")
		} else if !strings.Contains(out, "not everything is durably knowable") {
			t.Errorf("refusal must state what is unknowable: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version='002'`); got != "0" {
			t.Fatalf("refused mark-applied must write nothing")
		}

		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("migrate after mark-applied failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='mk'`); got != "1" {
			t.Fatalf("mark-applied must not re-run 001 (exactly one table)")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "1" {
			t.Fatalf("mark-applied row must survive re-migrate, rows=%s", got)
		}
	})

	// ------------------------------------------------------------------
	// I. Status is read-only (M04 review L1 closure).
	// ------------------------------------------------------------------
	t.Run("StatusDoesNotCreateHistory", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_s.up.sql": "CREATE TABLE st (id int);",
		})

		// FAIL-BEFORE (pre-M05): status created the empty history table.
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "status", "--dir", dir); code != 0 {
			t.Fatalf("pre-M05 status failed: %s", out)
		}
		preM05Created := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='_neutron_migrations'`)
		if preM05Created != "1" {
			t.Fatalf("fail-before premise wrong: pre-M05 status did not create the history table (got %s)", preM05Created)
		}
		execRaw(t, dbURL, `DROP TABLE _neutron_migrations`)

		if code, out := runCLI(t, dbURL, "migrate", "status", "--dir", dir); code != 0 {
			t.Fatalf("status failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='_neutron_migrations'`); got != "0" {
			t.Fatalf("status must not create the history table, got %s tables", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='st'`); got != "0" {
			t.Fatalf("status must not apply migrations")
		}
	})

	// ------------------------------------------------------------------
	// J. Nontransactional migrations with data changes are refused.
	// ------------------------------------------------------------------
	t.Run("NontransactionalDataChangesRefused", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_mix.up.sql": "CREATE TABLE dml_t (id int);\nINSERT INTO dml_t VALUES (1);\nCREATE INDEX CONCURRENTLY dml_i ON dml_t (id);",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir)
		if code == 0 || !strings.Contains(out, "data-changing") {
			t.Fatalf("nontransactional data change accepted (code %d): %s", code, out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='dml_t'`); got != "0" {
			t.Fatalf("refusal must precede all effects")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='dml_i'`); got != "0" {
			t.Fatalf("refusal must precede index creation")
		}
	})

	// ------------------------------------------------------------------
	// K. Push serializes under the migration advisory lock.
	// ------------------------------------------------------------------
	t.Run("PushTakesMigrationLock", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_slow.up.sql": "SELECT pg_sleep(6);\nCREATE TABLE lock_t (id int);",
		})

		schemaDoc := filepath.Join(t.TempDir(), "push.json")
		writeFile(t, schemaDoc, `{
			"version": 1,
			"tables": [
				{"name": "push_t", "columns": [{"name": "id", "type": "integer", "primaryKey": true}], "indexes": []}
			]
		}`)

		holder := exec.Command(bin, "--url", dbURL, "migrate", "--dir", dir, "--timeout", "60s")
		if err := holder.Start(); err != nil {
			t.Fatal(err)
		}
		if !waitForLockHeld(t, dbURL) {
			holder.Process.Kill()
			t.Fatal("holder never took the advisory lock")
		}

		// FAIL-BEFORE (pre-M05): push does not wait — it finishes while
		// the holder is still mid-migration.
		preM05Done := make(chan time.Time, 1)
		go func() {
			start := time.Now()
			code, _ := runCLIProcess(t, preM05Bin, dbURL, "db", "push", "--schema", schemaDoc, "--force")
			if code != 0 {
				t.Errorf("pre-M05 push failed")
			}
			preM05Done <- time.Now()
			_ = start
		}()
		holderDone := make(chan error, 1)
		go func() { holderDone <- holder.Wait() }()
		preM05End := <-preM05Done
		if err := <-holderDone; err != nil {
			t.Fatalf("holder failed: %v", err)
		}
		if preM05End.Before(time.Now().Add(-5 * time.Second)) {
			t.Log("fail-before confirmed: pre-M05 push completed while the migration still ran")
		} else {
			t.Log("pre-M05 push duration inconclusive (timing); guarded behavior is what is asserted")
		}
		// The pre-M05 push applied during the run; reset so the guarded
		// assertion starts from a known state (fresh history + no tables).
		execRaw(t, dbURL, `DROP TABLE IF EXISTS push_t, lock_t`)
		execRaw(t, dbURL, `DROP TABLE IF EXISTS _neutron_migrations`)

		// Guarded: push waits for the lock, then applies cleanly.
		holder2 := exec.Command(bin, "--url", dbURL, "migrate", "--dir", dir, "--timeout", "60s")
		if err := holder2.Start(); err != nil {
			t.Fatal(err)
		}
		if !waitForLockHeld(t, dbURL) {
			holder2.Process.Kill()
			holder2.Wait()
			t.Fatal("second holder never took the advisory lock")
		}

		pushDone := make(chan time.Time, 1)
		go func() {
			code, out := runCLIProcess(t, bin, dbURL, "db", "push", "--schema", schemaDoc, "--force")
			if code != 0 {
				t.Errorf("guarded push failed: %s", out)
			}
			pushDone <- time.Now()
		}()
		holder2Done := make(chan error, 1)
		go func() { holder2Done <- holder2.Wait() }()
		pushEnd := <-pushDone
		if err := <-holder2Done; err != nil {
			t.Fatalf("second holder failed: %v", err)
		}
		holder2End := time.Now()
		if pushEnd.Before(holder2End.Add(-2 * time.Second)) {
			t.Fatalf("push finished well before the lock holder (%v < %v): it did not serialize under the advisory lock", pushEnd, holder2End)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name IN ('lock_t','push_t')`); got != "2" {
			t.Fatalf("both holder and push effects must exist, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows = %s, want 1", got)
		}
	})

	// ------------------------------------------------------------------
	// L. MAJOR-1 (M05 review): widened drop-kind vocabulary — DROP
	//    EXTENSION ... CASCADE, DROP MATERIALIZED VIEW / DROP SEQUENCE of
	//    _neutron_* objects, and the non-relation kinds (type, domain,
	//    function) are refused; innocent drops of the same kinds still
	//    apply.
	// ------------------------------------------------------------------
	t.Run("DropVocabularyGuarded", func(t *testing.T) {
		dbURL := newDB(t)
		// Real pg_depend fixture with a non-plpgsql extension (cube), as
		// the reviewer used: an extension-owned table hangs off it.
		execRaw(t, dbURL, `CREATE EXTENSION cube`)
		execRaw(t, dbURL, `CREATE TABLE ext_t (id int)`)
		execRaw(t, dbURL, `ALTER EXTENSION cube ADD TABLE ext_t`)
		execRaw(t, dbURL, `CREATE MATERIALIZED VIEW _neutron_probe_mv AS SELECT 1 AS x`)
		execRaw(t, dbURL, `CREATE SEQUENCE _neutron_probe_seq`)
		execRaw(t, dbURL, `CREATE TYPE _neutron_probe_type AS ENUM ('a')`)
		execRaw(t, dbURL, `CREATE DOMAIN _neutron_probe_dom AS int`)
		execRaw(t, dbURL, `CREATE FUNCTION _neutron_probe_fn() RETURNS int AS 'SELECT 1' LANGUAGE sql`)
		execRaw(t, dbURL, `CREATE SEQUENCE app_seq`)

		dirExtCascade := writeMigrations(t, map[string]string{
			"001_drop_ext_cascade.up.sql": "DROP EXTENSION cube CASCADE;",
		})
		// FAIL-BEFORE (pre-M05): the sibling spelling applies with NO
		// flags — extension and member destroyed.
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dirExtCascade); code != 0 {
			t.Fatalf("pre-M05 binary must apply DROP EXTENSION CASCADE ungated (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_extension WHERE extname='cube'`); got != "0" {
			t.Fatalf("fail-before: extension must be gone, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='ext_t'`); got != "0" {
			t.Fatalf("fail-before: extension-owned ext_t must be destroyed, got %s", got)
		}
		// Restore the fixture for the guarded assertions.
		execRaw(t, dbURL, `CREATE EXTENSION cube`)
		execRaw(t, dbURL, `CREATE TABLE ext_t (id int)`)
		execRaw(t, dbURL, `ALTER EXTENSION cube ADD TABLE ext_t`)
		execRaw(t, dbURL, `DROP TABLE _neutron_migrations`)

		code, out := runCLI(t, dbURL, "migrate", "--dir", dirExtCascade)
		if code == 0 {
			t.Fatal("DROP EXTENSION ... CASCADE accepted with no flags")
		}
		for _, want := range []string{"protected", "cube", "extension-owned member"} {
			if !strings.Contains(out, want) {
				t.Errorf("refusal missing %q: %s", want, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_extension WHERE extname='cube'`); got != "1" {
			t.Fatalf("refusal must leave the extension intact, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='ext_t'`); got != "1" {
			t.Fatalf("refusal must leave the extension-owned table intact")
		}

		// Plain DROP EXTENSION (no CASCADE) destroys its members too —
		// same refusal.
		dirExt := writeMigrations(t, map[string]string{
			"001_drop_ext.up.sql": "DROP EXTENSION cube;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirExt, "--allow-destructive"); code == 0 {
			t.Fatal("DROP EXTENSION without CASCADE accepted even with --allow-destructive")
		} else if !strings.Contains(out, "extension-owned member") {
			t.Errorf("refusal must name the extension members: %s", out)
		}

		// DROP MATERIALIZED VIEW of a _neutron_* object — even with the
		// ack, the prefix rule fires.
		dirMv := writeMigrations(t, map[string]string{
			"001_drop_mv.up.sql": "DROP MATERIALIZED VIEW _neutron_probe_mv;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirMv, "--allow-destructive"); code == 0 {
			t.Fatal("DROP MATERIALIZED VIEW of _neutron_* accepted with --allow-destructive")
		} else if !strings.Contains(out, "_neutron_probe_mv") {
			t.Errorf("refusal must name the matview: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_class WHERE relname='_neutron_probe_mv' AND relkind='m'`); got != "1" {
			t.Fatalf("matview must survive the refused drop, got %s", got)
		}

		// DROP SEQUENCE of a _neutron_* object with NO ack.
		dirSeq := writeMigrations(t, map[string]string{
			"001_drop_seq.up.sql": "DROP SEQUENCE _neutron_probe_seq;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirSeq); code == 0 {
			t.Fatal("DROP SEQUENCE of _neutron_* accepted with no flags")
		} else if !strings.Contains(out, "_neutron_probe_seq") {
			t.Errorf("refusal must name the sequence: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.sequences WHERE sequence_name='_neutron_probe_seq'`); got != "1" {
			t.Fatalf("sequence must survive the refused drop, got %s", got)
		}

		// Non-relation kinds: type, domain, function.
		dirObj := writeMigrations(t, map[string]string{
			"001_drop_objs.up.sql": "DROP TYPE _neutron_probe_type;\nDROP DOMAIN _neutron_probe_dom;\nDROP FUNCTION _neutron_probe_fn();",
		})
		code, out = runCLI(t, dbURL, "migrate", "--dir", dirObj, "--allow-destructive")
		if code == 0 {
			t.Fatal("DROP TYPE/DOMAIN/FUNCTION of _neutron_* accepted with --allow-destructive")
		}
		for _, want := range []string{"_neutron_probe_type", "_neutron_probe_dom", "_neutron_probe_fn"} {
			if !strings.Contains(out, want) {
				t.Errorf("refusal missing %q: %s", want, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_type WHERE typname IN ('_neutron_probe_type','_neutron_probe_dom')`); got != "2" {
			t.Fatalf("type/domain must survive, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_proc WHERE proname='_neutron_probe_fn'`); got != "1" {
			t.Fatalf("function must survive, got %s", got)
		}

		// Innocent control: the widened vocabulary does not over-refuse
		// legitimately managed drops of the same kinds.
		dirInnocent := writeMigrations(t, map[string]string{
			"001_innocent.up.sql": "DROP SEQUENCE app_seq;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirInnocent); code != 0 {
			t.Fatalf("innocent DROP SEQUENCE refused (over-refusal): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.sequences WHERE sequence_name='app_seq'`); got != "0" {
			t.Fatalf("innocent sequence drop did not apply, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// M. MAJOR-1 (M05 review): the real CASCADE case — an innocent
	//    table's CASCADE drop would transitively destroy an
	//    extension-owned dependent (and through it the extension), and
	//    the guard refuses it before any DDL.
	// ------------------------------------------------------------------
	t.Run("CascadeThroughInnocentTableRefused", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE EXTENSION cube`)
		execRaw(t, dbURL, `CREATE TABLE innoc (id int PRIMARY KEY)`)
		execRaw(t, dbURL, `CREATE VIEW ext_dep AS SELECT * FROM innoc`)
		execRaw(t, dbURL, `ALTER EXTENSION cube ADD VIEW ext_dep`)

		dir := writeMigrations(t, map[string]string{
			"001_cascade.up.sql": "DROP TABLE innoc CASCADE;",
		})
		// FAIL-BEFORE (pre-M05): the server happily executes the cascade
		// — the extension-owned view AND the extension are destroyed.
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("pre-M05 binary must apply the cascade drop ungated (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='innoc'`); got != "0" {
			t.Fatalf("fail-before: innoc must be gone, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.views WHERE table_name='ext_dep'`); got != "0" {
			t.Fatalf("fail-before: extension-owned ext_dep must be destroyed by the cascade, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_extension WHERE extname='cube'`); got != "0" {
			t.Fatalf("fail-before: the cascade took the extension too, got %s", got)
		}
		// Restore the fixture.
		execRaw(t, dbURL, `CREATE EXTENSION cube`)
		execRaw(t, dbURL, `CREATE TABLE innoc (id int PRIMARY KEY)`)
		execRaw(t, dbURL, `CREATE VIEW ext_dep AS SELECT * FROM innoc`)
		execRaw(t, dbURL, `ALTER EXTENSION cube ADD VIEW ext_dep`)
		execRaw(t, dbURL, `DROP TABLE _neutron_migrations`)

		code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--allow-destructive")
		if code == 0 {
			t.Fatal("CASCADE drop over an extension-owned dependent accepted even with --allow-destructive")
		}
		for _, want := range []string{"CASCADE", "ext_dep"} {
			if !strings.Contains(out, want) {
				t.Errorf("refusal missing %q: %s", want, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='innoc'`); got != "1" {
			t.Fatalf("refusal must leave the innocent table intact, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.views WHERE table_name='ext_dep'`); got != "1" {
			t.Fatalf("refusal must leave the extension-owned view intact, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_extension WHERE extname='cube'`); got != "1" {
			t.Fatalf("refusal must leave the extension intact, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// N. MINOR-2 + LOW-1 (M05 review): hand-edited DML cannot forge
	//    history rows, and non-drop ALTERs of _neutron_*/extension-owned
	//    objects are refused; SELECTs stay legal.
	// ------------------------------------------------------------------
	t.Run("ProtectedWritesAndAltersRefused", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE EXTENSION cube`)
		execRaw(t, dbURL, `CREATE TABLE ext_t2 (id int)`)
		execRaw(t, dbURL, `ALTER EXTENSION cube ADD TABLE ext_t2`)
		execRaw(t, dbURL, `CREATE TABLE _neutron_probe_tbl (id int)`)

		// FAIL-BEFORE (pre-M05): the forged history row applies.
		dirForge := writeMigrations(t, map[string]string{
			"001_forge.up.sql": "INSERT INTO _neutron_migrations (version, name, applied_at, owner, format) VALUES ('999','fake',now(),'attacker','v2');",
		})
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dirForge); code != 0 {
			t.Fatalf("pre-M05 binary must apply the forged history row (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version='999' AND owner='attacker'`); got != "1" {
			t.Fatalf("fail-before: the forged row must be present, got %s", got)
		}
		execRaw(t, dbURL, `DELETE FROM _neutron_migrations`)

		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirForge, "--allow-destructive"); code == 0 {
			t.Fatal("history-forging INSERT accepted even with --allow-destructive")
		} else if !strings.Contains(out, "_neutron_migrations") {
			t.Errorf("refusal must name the target: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("refused run must forge nothing, rows=%s", got)
		}

		dirUpdate := writeMigrations(t, map[string]string{
			"001_update.up.sql": "UPDATE _neutron_migrations SET name = 'evil';",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirUpdate); code == 0 || !strings.Contains(out, "_neutron_migrations") {
			t.Fatalf("UPDATE of _neutron_migrations accepted or unnamed: %s", out)
		}
		dirDelete := writeMigrations(t, map[string]string{
			"001_delete.up.sql": "DELETE FROM _neutron_migrations;",
		})
		if code, _ := runCLI(t, dbURL, "migrate", "--dir", dirDelete, "--allow-destructive"); code == 0 {
			t.Fatal("DELETE of _neutron_migrations accepted even with --allow-destructive")
		}

		// LOW-1: non-drop ALTERs of protected names / extension members.
		dirAlter := writeMigrations(t, map[string]string{
			"001_alter.up.sql": "ALTER TABLE _neutron_probe_tbl ADD COLUMN evil text;\nALTER TABLE ext_t2 ADD COLUMN evil text;",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dirAlter)
		if code == 0 {
			t.Fatal("ALTER of _neutron_*/extension-owned objects accepted")
		}
		for _, want := range []string{"_neutron_probe_tbl", "ext_t2", "owned by extension cube"} {
			if !strings.Contains(out, want) {
				t.Errorf("refusal missing %q: %s", want, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_probe_tbl' AND column_name='evil'`); got != "0" {
			t.Fatalf("ALTER must not have applied, evil column exists")
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.columns WHERE table_name='ext_t2' AND column_name='evil'`); got != "0" {
			t.Fatalf("ALTER must not have applied to the extension-owned table")
		}

		// SELECTs into protected metadata stay legal (read-only).
		dirSelect := writeMigrations(t, map[string]string{
			"001_select.up.sql": "CREATE TABLE sel_t (id int);\nINSERT INTO sel_t VALUES (1);\nSELECT count(*) FROM _neutron_migrations;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirSelect); code != 0 {
			t.Fatalf("legal SELECT refused (over-refusal): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='sel_t'`); got != "1" {
			t.Fatalf("SELECT-bearing migration did not apply, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// O. MAJOR-2 + MINOR-1 (M05 review): the ALTER+CIC migration
	//    interrupted after the ALTER — every explicit recovery path
	//    refuses on the clean-looking unverifiable state — now aborts,
	//    and a plain migrate converges. Plus: abort still refuses when
	//    nothing ran IS provable, and retry's clean-state refusal tells
	//    the truth.
	// ------------------------------------------------------------------
	t.Run("ResolveAbortUnverifiableState", func(t *testing.T) {
		// Sanity first: the same migration applies cleanly end-to-end
		// when uninterrupted (the reviewer's premise).
		cleanURL := newDB(t)
		execRaw(t, cleanURL, `CREATE TABLE msg_t (a int)`)
		cleanDir := writeMigrations(t, map[string]string{
			"001_msg.up.sql":   "ALTER TABLE msg_t ADD COLUMN c text;\nCREATE INDEX CONCURRENTLY msg_i ON msg_t (a);",
			"001_msg.down.sql": "ALTER TABLE msg_t DROP COLUMN IF EXISTS c;\nDROP INDEX IF EXISTS msg_i;",
		})
		if code, out := runCLI(t, cleanURL, "migrate", "--dir", cleanDir); code != 0 {
			t.Fatalf("uninterrupted ALTER+CIC migration must apply cleanly: %s", out)
		}
		if got := query(t, cleanURL, `SELECT count(*) FROM information_schema.columns WHERE table_name='msg_t' AND column_name='c'`); got != "1" {
			t.Fatalf("uninterrupted apply must add the column, got %s", got)
		}
		if got := query(t, cleanURL, `SELECT i.indisvalid::int::text AS v FROM pg_index i JOIN pg_class cl ON cl.oid=i.indexrelid WHERE cl.relname='msg_i'`); got != "1" {
			t.Fatalf("uninterrupted apply must build a valid index, got %s", got)
		}

		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE msg_t (a int)`)
		dir := writeMigrations(t, map[string]string{
			"001_msg.up.sql":   "ALTER TABLE msg_t ADD COLUMN c text;\nCREATE INDEX CONCURRENTLY msg_i ON msg_t (a);",
			"001_msg.down.sql": "ALTER TABLE msg_t DROP COLUMN IF EXISTS c;\nDROP INDEX IF EXISTS msg_i;",
		})

		// The kill window after statement 1, landed deterministically:
		// hand-apply the ALTER as the oracle (a real kill in the gap
		// leaves exactly this durable state — proven by the M05 kill
		// test G).
		execRaw(t, dbURL, `ALTER TABLE msg_t ADD COLUMN c text`)

		// Bare resolve must NOT claim nothing ran (the old false
		// verdict), and must point at --abort.
		code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--dir", dir)
		if code != 0 {
			t.Fatalf("bare resolve failed: %s", out)
		}
		if strings.Contains(out, "No effects present — nothing ran") {
			t.Fatalf("bare resolve still claims nothing ran on the unverifiable state: %s", out)
		}
		if !strings.Contains(out, "not provable") || !strings.Contains(out, "--abort") {
			t.Fatalf("bare resolve must state the unverifiable trichotomy and the abort path: %s", out)
		}

		// --abort now works on the unverifiable partial state: down SQL
		// removes the column, history stays empty.
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--abort", "--dir", dir); code != 0 {
			t.Fatalf("resolve --abort must recover the ALTER-ran state (MAJOR-2): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.columns WHERE table_name='msg_t' AND column_name='c'`); got != "0" {
			t.Fatalf("abort must remove the interrupted ALTER's column, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='msg_i'`); got != "0" {
			t.Fatalf("abort must leave no index, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("abort must write no history, rows=%s", got)
		}

		// Recovery proven: a plain migrate converges.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "90s"); code != 0 {
			t.Fatalf("post-abort migrate failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.columns WHERE table_name='msg_t' AND column_name='c'`); got != "1" {
			t.Fatalf("post-abort migrate must re-add the column, got %s", got)
		}
		if got := query(t, dbURL, `SELECT i.indisvalid::int::text AS v FROM pg_index i JOIN pg_class cl ON cl.oid=i.indexrelid WHERE cl.relname='msg_i'`); got != "1" {
			t.Fatalf("post-abort migrate must build a valid index, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after recovery = %s, want 1", got)
		}

		// MINOR-1: on a provably-clean state (fresh pending 002, nothing
		// ran), --retry's refusal no longer claims present effects.
		writeFile(t, filepath.Join(dir, "002_msg2.up.sql"),
			"ALTER TABLE msg_t ADD COLUMN d text;\nCREATE INDEX CONCURRENTLY msg_i2 ON msg_t (a);")
		writeFile(t, filepath.Join(dir, "002_msg2.down.sql"),
			"ALTER TABLE msg_t DROP COLUMN IF EXISTS d;\nDROP INDEX IF EXISTS msg_i2;")
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "002", "--retry", "--dir", dir); code == 0 {
			t.Fatal("retry must still refuse unverifiable statements after an interruption window exists")
		} else {
			if strings.Contains(out, "shows present effects") {
				t.Errorf("retry refusal still lies about present effects: %s", out)
			}
			if !strings.Contains(out, "cannot prove whether they ran") {
				t.Errorf("retry refusal must state the honest reason: %s", out)
			}
			if !strings.Contains(out, "plain `neutron migrate`") {
				t.Errorf("retry refusal must point clean states at plain migrate: %s", out)
			}
		}

		// Abort still refuses when nothing ran IS provable (pure-create
		// 003, no unverifiable statements).
		writeFile(t, filepath.Join(dir, "003_clean.up.sql"), "CREATE TABLE clean_t (id int);")
		writeFile(t, filepath.Join(dir, "003_clean.down.sql"), "DROP TABLE clean_t;")
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "003", "--abort", "--dir", dir); code == 0 {
			t.Fatal("abort accepted on a provably-clean state")
		} else if !strings.Contains(out, "provably present") {
			t.Errorf("clean-state abort refusal must name provability: %s", out)
		}
	})

	// ------------------------------------------------------------------
	// P. Pass-2 BLOCKER-2: comments (leading line, leading/nested/mid-
	//    statement blocks) must not hide destructive statements from
	//    the guard AND the acknowledgement. Every escape applied on the
	//    pre-M05 binary (fail-before) and on the attempt-2 tree.
	// ------------------------------------------------------------------
	t.Run("CommentObfuscatedDestructiveRefused", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE _neutron_probe_tbl (i int)`)

		dirComments := writeMigrations(t, map[string]string{
			"001_comments.up.sql": "/* note */ DROP TABLE _neutron_probe_tbl;\n" +
				"/* outer /* nested */ still note */ ALTER TABLE _neutron_probe_tbl ADD COLUMN evil text;\n" +
				"DROP /* between */ TABLE /* parts */ _neutron_probe_tbl;\n" +
				"-- header note\nDELETE FROM _neutron_probe_tbl;",
			"001_comments.down.sql": "SELECT 1;",
		})
		// FAIL-BEFORE: the block-comment escape applies with no flags
		// (attempt-2 behavior — the guard never saw the statement). One
		// statement only: the pre-M05 binary executes verbatim, so a second
		// drop of the same table would fail on its own.
		dirEscape := writeMigrations(t, map[string]string{
			"001_escape.up.sql":   "/* note */ DROP TABLE _neutron_probe_tbl;",
			"001_escape.down.sql": "SELECT 1;",
		})
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dirEscape); code != 0 {
			t.Fatalf("pre-M05 binary must apply the comment-obfuscated drop (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_class WHERE relname='_neutron_probe_tbl'`); got != "0" {
			t.Fatalf("fail-before: comment-obfuscated drop must destroy the table, got %s", got)
		}
		// Restore the fixture for the guarded assertions.
		execRaw(t, dbURL, `DROP TABLE IF EXISTS _neutron_migrations`)
		execRaw(t, dbURL, `CREATE TABLE _neutron_probe_tbl (i int)`)

		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbURL, append([]string{"migrate", "--dir", dirComments}, flags...)...); code == 0 {
				t.Fatalf("comment-obfuscated destructive statements accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "targets protected object") {
				t.Fatalf("refusal must be the protected-object guard (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_class WHERE relname='_neutron_probe_tbl'`); got != "1" {
			t.Fatalf("table must survive comment-obfuscated refusals, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_probe_tbl' AND column_name='evil'`); got != "0" {
			t.Fatalf("comment-obfuscated ALTER must not add columns, got %s", got)
		}

		// The history-truncation escape: a comment-prefixed TRUNCATE of
		// the migrations history after a recorded migration.
		execRaw(t, dbURL, `DROP TABLE IF EXISTS _neutron_migrations`)
		dirBase := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirBase); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		dirTrunc := writeMigrations(t, map[string]string{
			"002_trunc.up.sql":   "/* rollback note */ TRUNCATE _neutron_migrations;",
			"002_trunc.down.sql": "SELECT 1;",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbURL, append([]string{"migrate", "--dir", dirTrunc}, flags...)...); code == 0 {
				t.Fatalf("comment-prefixed TRUNCATE of history accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "targets protected object") {
				t.Fatalf("refusal must be the guard (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "1" {
			t.Fatalf("history row 001 must survive the refused truncate, got %s", got)
		}

		// Controls: legal statements under comments still apply, and
		// comment-only fragments stay no-ops.
		dirLegal := writeMigrations(t, map[string]string{
			"003_legal.up.sql":   "CREATE /* split */ TABLE legal_t (id int);\n-- DROP TABLE _neutron_migrations\n",
			"003_legal.down.sql": "DROP TABLE legal_t;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dirLegal); code != 0 {
			t.Fatalf("legal comment-bearing migration refused (over-refusal): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_class WHERE relname='legal_t'`); got != "1" {
			t.Fatalf("CREATE with interleaved comment must apply, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "2" {
			t.Fatalf("history after legal apply = %s, want 2 (line comment must stay a comment)", got)
		}
	})

	// ------------------------------------------------------------------
	// Q. Pass-2 control: the guard must not over-block — dollar-quoted
	//    and escaped string BODIES carrying scary text are opaque to the
	//    tokenizer and apply as legitimate migrations.
	// ------------------------------------------------------------------
	t.Run("DollarQuotedStringNotOverBlocked", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE notes_t (body text)`)
		dir := writeMigrations(t, map[string]string{
			"001_notes.up.sql": "INSERT INTO notes_t (body) VALUES ($rev$DROP TABLE _neutron_migrations$rev$);\n" +
				"INSERT INTO notes_t (body) VALUES (E'foo\\' DROP TABLE _neutron_migrations');\n" +
				"INSERT INTO notes_t (body) VALUES ('it''s; TRUNCATE _neutron_migrations');\n" +
				"SELECT $$DROP TABLE _neutron_migrations$$ AS note;",
			"001_notes.down.sql": "DELETE FROM notes_t;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("string-bodies migration refused (over-refusal): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM notes_t WHERE body LIKE '%_neutron_migrations%'`); got != "3" {
			t.Fatalf("all three string literals must land as rows, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// R. Pass-2 MAJOR-3: data-modifying CTEs — WITH-wrapped writes into
	//    _neutron_* are refused (history survives); WITH-wrapped writes
	//    into managed tables stay legal.
	// ------------------------------------------------------------------
	t.Run("WithCTEWritesTargetGuarded", func(t *testing.T) {
		// Destruction proof on dbA: the pre-M05 binary applies the
		// WITH-wrapped delete and destroys the 001 history row.
		dbA := newDB(t)
		dirA := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbA, "migrate", "--dir", dirA); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirA, "002_cte.up.sql"),
			"WITH d AS (DELETE FROM _neutron_migrations WHERE version <> '002' RETURNING 1) SELECT count(*) FROM d;")
		writeFile(t, filepath.Join(dirA, "002_cte.down.sql"), "SELECT 1;")
		if code, out := runCLIProcess(t, preM05Bin, dbA, "migrate", "--dir", dirA); code != 0 {
			t.Fatalf("pre-M05 binary must apply the WITH-wrapped delete (fail-before): %s", out)
		}
		if got := query(t, dbA, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "0" {
			t.Fatalf("fail-before: WITH-wrapped delete must destroy the 001 row, got %s", got)
		}

		// Guarded proof on dbB: same shape, refused — history survives.
		dbB := newDB(t)
		execRaw(t, dbB, `CREATE TABLE legal_t (i int)`)
		dirB := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbB, "migrate", "--dir", dirB); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirB, "002_cte.up.sql"),
			"WITH d AS (DELETE FROM _neutron_migrations WHERE version <> '002' RETURNING 1) SELECT count(*) FROM d;")
		writeFile(t, filepath.Join(dirB, "002_cte.down.sql"), "SELECT 1;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", dirB}, flags...)...); code == 0 {
				t.Fatalf("WITH-wrapped delete into _neutron_* accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "targets protected object") || !strings.Contains(out, "_neutron_migrations") {
				t.Fatalf("refusal must name the guarded target (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "1" {
			t.Fatalf("history row 001 must survive the refused CTE, got %s", got)
		}

		// The same shape writing a legitimately managed table stays
		// legal, with and without the acknowledgement (the bare DELETE
		// form has never been ack-gated either).
		dirLegal := writeMigrations(t, map[string]string{
			"003_cte.up.sql":   "WITH d AS (DELETE FROM legal_t WHERE false RETURNING 1) SELECT count(*) FROM d;",
			"003_cte.down.sql": "SELECT 1;",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", dirLegal}, flags...)...); code != 0 {
				t.Fatalf("WITH-wrapped write to a managed table refused (over-refusal, flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations WHERE version='003'`); got != "1" {
			t.Fatalf("legal CTE write must record history, got %s", got)
		}

		// A read-only CTE over the metadata stays legal too.
		dirRead := writeMigrations(t, map[string]string{
			"004_read.up.sql":   "WITH x AS (SELECT count(*) AS n FROM _neutron_migrations) SELECT n FROM x;",
			"004_read.down.sql": "SELECT 1;",
		})
		if code, out := runCLI(t, dbB, "migrate", "--dir", dirRead); code != 0 {
			t.Fatalf("read-only CTE over _neutron_* refused (over-refusal): %s", out)
		}
	})

	// ------------------------------------------------------------------
	// S. Pass-2 escalation decision: DO blocks are refused outright in
	//    migration bodies — plpgsql bodies are opaque and cannot be
	//    proven safe (a harmless body is refused too: conservative and
	//    documented, never guessed at).
	// ------------------------------------------------------------------
	t.Run("DoBlocksRefused", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_do.up.sql":   "DO $$ BEGIN CREATE TABLE do_t (i int); END $$;",
			"001_do.down.sql": "DROP TABLE do_t;",
		})
		// FAIL-BEFORE: the pre-M05 binary applies the DO block.
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("pre-M05 binary must apply the DO block (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_class WHERE relname='do_t'`); got != "1" {
			t.Fatalf("fail-before: DO block must create do_t, got %s", got)
		}
		execRaw(t, dbURL, `DROP TABLE IF EXISTS do_t`)
		execRaw(t, dbURL, `DROP TABLE IF EXISTS _neutron_migrations`)

		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbURL, append([]string{"migrate", "--dir", dir}, flags...)...); code == 0 {
				t.Fatalf("DO block accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "DO") || !strings.Contains(out, "opaque") {
				t.Fatalf("refusal must name DO's opaque-body reason (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_class WHERE relname='do_t'`); got != "0" {
			t.Fatalf("do_t must not exist after the refused DO, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("no history may record a refused DO, got %s rows", got)
		}
	})

	// ------------------------------------------------------------------
	// T. Pass-2 BLOCKER-1: the live-catalog _neutron_ arms of the guard.
	//    DROP SCHEMA ... CASCADE over a namespace holding _neutron_*
	//    relations, and relation CASCADE drops with _neutron_-prefixed
	//    dependents, are refused — with and without the acknowledgement.
	//    (Both arms were dead code in attempt-2: LIKE '\_neutron\%'
	//    never matches.)
	// ------------------------------------------------------------------
	t.Run("DropSchemaCascadeGuarded", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE SCHEMA guard_s`)
		execRaw(t, dbURL, `CREATE TABLE guard_s._neutron_insch (i int)`)
		execRaw(t, dbURL, `CREATE TABLE guard_s.app_insch (i int)`)
		execRaw(t, dbURL, `CREATE TABLE app_t (i int)`)
		execRaw(t, dbURL, `CREATE INDEX _neutron_probe_idx ON app_t(i)`)

		dirSchema := writeMigrations(t, map[string]string{
			"001_schema.up.sql":   "DROP SCHEMA guard_s CASCADE;",
			"001_schema.down.sql": "SELECT 1;",
		})
		// FAIL-BEFORE: with the acknowledgement, the attempt-2 guard's
		// dead LIKE arm let the schema and its _neutron_* relation be
		// destroyed. The pre-M05 binary is even earlier (no guard,
		// no --allow-destructive flag — it applies ungated).
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dirSchema); code != 0 {
			t.Fatalf("pre-M05 binary must apply DROP SCHEMA CASCADE ungated (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.schemata WHERE schema_name='guard_s'`); got != "0" {
			t.Fatalf("fail-before: schema must be destroyed, got %s", got)
		}
		// Restore the fixture.
		execRaw(t, dbURL, `DROP TABLE IF EXISTS _neutron_migrations`)
		execRaw(t, dbURL, `CREATE SCHEMA guard_s`)
		execRaw(t, dbURL, `CREATE TABLE guard_s._neutron_insch (i int)`)
		execRaw(t, dbURL, `CREATE TABLE guard_s.app_insch (i int)`)

		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbURL, append([]string{"migrate", "--dir", dirSchema}, flags...)...); code == 0 {
				t.Fatalf("DROP SCHEMA over _neutron_* relations accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "guard_s") || !strings.Contains(out, "protected") {
				t.Fatalf("refusal must name the schema and protection (flags=%v): %s", flags, out)
			}
		}
		for _, probe := range []string{
			`SELECT count(*) FROM information_schema.schemata WHERE schema_name='guard_s'`,
			`SELECT count(*) FROM pg_class WHERE relname='_neutron_insch'`,
			`SELECT count(*) FROM pg_class WHERE relname='app_insch'`,
		} {
			if got := query(t, dbURL, probe); got != "1" {
				t.Fatalf("schema and members must survive the refused cascade (%s), got %s", probe, got)
			}
		}

		// Relation CASCADE drop with a _neutron_-prefixed dependent.
		dirCascade := writeMigrations(t, map[string]string{
			"002_cascade.up.sql":   "DROP TABLE app_t CASCADE;",
			"002_cascade.down.sql": "SELECT 1;",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbURL, append([]string{"migrate", "--dir", dirCascade}, flags...)...); code == 0 {
				t.Fatalf("CASCADE drop onto a _neutron_ index accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "_neutron_probe_idx") {
				t.Fatalf("refusal must name the protected dependent (flags=%v): %s", flags, out)
			}
		}
		for _, probe := range []string{
			`SELECT count(*) FROM pg_class WHERE relname='app_t'`,
			`SELECT count(*) FROM pg_class WHERE relname='_neutron_probe_idx'`,
		} {
			if got := query(t, dbURL, probe); got != "1" {
				t.Fatalf("table and protected index must survive the refused cascade (%s), got %s", probe, got)
			}
		}
	})

	// ------------------------------------------------------------------
	// U. Pass-3 BLOCKER-1: statements whose leading word is outside the
	//    guard's dispatch — EXPLAIN ANALYZE-wrapped DML and PREPARE/
	//    EXECUTE — destroyed protected history ungated. The statement-
	//    kind allowlist (pass-3 escalation) refuses them before any
	//    statement runs, atomically, under both flag variants.
	// ------------------------------------------------------------------
	t.Run("ExplainAndPrepareExecuteRefused", func(t *testing.T) {
		// EXPLAIN ANALYZE — fail-before: the pre-M05 binary applies it and
		// the 001 history row is destroyed.
		dbA := newDB(t)
		dirA := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbA, "migrate", "--dir", dirA); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirA, "002_attack.up.sql"), "EXPLAIN ANALYZE DELETE FROM _neutron_migrations;")
		writeFile(t, filepath.Join(dirA, "002_attack.down.sql"), "SELECT 1;")
		if code, out := runCLIProcess(t, preM05Bin, dbA, "migrate", "--dir", dirA); code != 0 {
			t.Fatalf("pre-M05 binary must apply the EXPLAIN-wrapped delete (fail-before): %s", out)
		}
		if got := query(t, dbA, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "0" {
			t.Fatalf("fail-before: EXPLAIN ANALYZE destroyed the 001 row, got %s", got)
		}

		dbB := newDB(t)
		dirB := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbB, "migrate", "--dir", dirB); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirB, "002_attack.up.sql"), "EXPLAIN ANALYZE DELETE FROM _neutron_migrations;")
		writeFile(t, filepath.Join(dirB, "002_attack.down.sql"), "SELECT 1;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", dirB}, flags...)...); code == 0 {
				t.Fatalf("EXPLAIN-wrapped DML accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind EXPLAIN is refused") {
				t.Fatalf("refusal must name the EXPLAIN statement kind (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "1" {
			t.Fatalf("history row 001 must survive the refused EXPLAIN, got %s", got)
		}

		// PREPARE + EXECUTE — compound file and split files.
		dbC := newDB(t)
		dirC := writeMigrations(t, map[string]string{
			"001_base.up.sql":     "CREATE TABLE base_t (id int);",
			"001_base.down.sql":   "DROP TABLE base_t;",
			"002_attack.up.sql":   "PREPARE p AS DELETE FROM _neutron_migrations;\nEXECUTE p;",
			"002_attack.down.sql": "SELECT 1;",
		})
		if code, out := runCLIProcess(t, preM05Bin, dbC, "migrate", "--dir", dirC); code != 0 {
			t.Fatalf("pre-M05 binary must apply the PREPARE/EXECUTE pair (fail-before): %s", out)
		}
		if got := query(t, dbC, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "0" {
			t.Fatalf("fail-before: EXECUTE destroyed the 001 row, got %s", got)
		}

		dbD := newDB(t)
		dirD := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbD, "migrate", "--dir", dirD); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirD, "002_prep.up.sql"), "PREPARE p AS DELETE FROM _neutron_migrations;")
		writeFile(t, filepath.Join(dirD, "002_prep.down.sql"), "SELECT 1;")
		writeFile(t, filepath.Join(dirD, "003_exec.up.sql"), "EXECUTE p;")
		writeFile(t, filepath.Join(dirD, "003_exec.down.sql"), "SELECT 1;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbD, append([]string{"migrate", "--dir", dirD}, flags...)...); code == 0 {
				t.Fatalf("PREPARE/EXECUTE split files accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind PREPARE is refused") {
				t.Fatalf("refusal must name the PREPARE statement kind (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbD, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history must hold only 001 after the refused batch, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// V. Pass-3 BLOCKER-2: the opaque server-code routes — CREATE
	//    FUNCTION + SELECT, CREATE PROCEDURE + CALL, CREATE RULE +
	//    managed DML — escaped the DO-only refusal and destroyed
	//    history rows ungated. The allowlist refuses the defining
	//    statements themselves; nothing in the file runs.
	// ------------------------------------------------------------------
	t.Run("OpaqueServerCodeRoutesRefused", func(t *testing.T) {
		routes := []struct {
			name   string
			attack string
			label  string
		}{
			{"FunctionSelect", "CREATE FUNCTION boom2() RETURNS void LANGUAGE plpgsql AS $$ BEGIN DELETE FROM _neutron_migrations; END $$;\nSELECT boom2();", "CREATE FUNCTION"},
			{"ProcedureCall", "CREATE PROCEDURE kaboom() LANGUAGE plpgsql AS $$ BEGIN TRUNCATE _neutron_migrations; END $$;\nCALL kaboom();", "CREATE PROCEDURE"},
			{"RuleInstead", "CREATE RULE r AS ON DELETE TO base_t DO INSTEAD DELETE FROM _neutron_migrations;\nDELETE FROM base_t;", "CREATE RULE"},
		}
		for _, route := range routes {
			t.Run(route.name, func(t *testing.T) {
				// FAIL-BEFORE: the pre-M05 binary applies the route and
				// the 001 history row is destroyed.
				dbA := newDB(t)
				dirA := writeMigrations(t, map[string]string{
					"001_base.up.sql":     "CREATE TABLE base_t (id int);",
					"001_base.down.sql":   "DROP TABLE base_t;",
					"002_attack.up.sql":   route.attack,
					"002_attack.down.sql": "SELECT 1;",
				})
				if code, out := runCLIProcess(t, preM05Bin, dbA, "migrate", "--dir", dirA); code != 0 {
					t.Fatalf("pre-M05 binary must apply the %s route (fail-before): %s", route.label, out)
				}
				if got := query(t, dbA, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "0" {
					t.Fatalf("fail-before: the %s route destroyed the 001 row, got %s", route.label, got)
				}

				dbB := newDB(t)
				dirB := writeMigrations(t, map[string]string{
					"001_base.up.sql":   "CREATE TABLE base_t (id int);",
					"001_base.down.sql": "DROP TABLE base_t;",
				})
				if code, out := runCLI(t, dbB, "migrate", "--dir", dirB); code != 0 {
					t.Fatalf("base migration must apply: %s", out)
				}
				writeFile(t, filepath.Join(dirB, "002_attack.up.sql"), route.attack)
				writeFile(t, filepath.Join(dirB, "002_attack.down.sql"), "SELECT 1;")
				for _, flags := range [][]string{{}, {"--allow-destructive"}} {
					if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", dirB}, flags...)...); code == 0 {
						t.Fatalf("%s route accepted (flags=%v)", route.label, flags)
					} else if !strings.Contains(out, "statement kind "+route.label+" is refused") {
						t.Fatalf("refusal must name the %s statement kind (flags=%v): %s", route.label, flags, out)
					}
				}
				if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "1" {
					t.Fatalf("history row 001 must survive the refused %s route, got %s", route.label, got)
				}
			})
		}
	})

	// ------------------------------------------------------------------
	// W. Pass-3 MINOR-1: COPY FROM STDIN stalled the batch until the
	//    timeout with an error that never named COPY; COPY FROM PROGRAM
	//    executed its program. The allowlist refuses COPY at validation
	//    time — fast, named, atomic.
	// ------------------------------------------------------------------
	t.Run("CopyStatementsRefusedFast", func(t *testing.T) {
		// FROM STDIN — fail-before: the pre-M05 binary stalls for the full
		// --timeout and its error does not name COPY.
		dbA := newDB(t)
		dirA := writeMigrations(t, map[string]string{
			"001_copy.up.sql": "COPY _neutron_migrations FROM STDIN;",
		})
		start := time.Now()
		if code, out := runCLIProcess(t, preM05Bin, dbA, "migrate", "--dir", dirA, "--timeout", "3s"); code == 0 {
			t.Fatalf("pre-M05 binary must fail the stalled COPY (fail-before): %s", out)
		}
		if elapsed := time.Since(start); elapsed < 2500*time.Millisecond {
			t.Fatalf("fail-before premise wrong: pre-M05 COPY failed in %v, expected a stall until the timeout", elapsed)
		}
		if code, out := runCLIProcess(t, preM05Bin, dbA, "migrate", "--dir", dirA, "--timeout", "3s"); code == 0 || strings.Contains(out, "COPY") {
			t.Fatalf("pre-M05 failure must be the unnamed timeout, not a COPY-named refusal: %s", out)
		}

		// Guarded: refused fast, with COPY named, under both variants.
		dbB := newDB(t)
		dirB := writeMigrations(t, map[string]string{
			"001_copy.up.sql":   "COPY _neutron_migrations FROM STDIN;",
			"001_copy.down.sql": "SELECT 1;",
		})
		start = time.Now()
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", dirB, "--timeout", "60s"}, flags...)...); code == 0 {
				t.Fatalf("COPY FROM STDIN accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind COPY is refused") {
				t.Fatalf("refusal must name the COPY statement kind (flags=%v): %s", flags, out)
			}
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("COPY refusal must fail fast (validation time), took %v", elapsed)
		}
		// The empty v2 history table's under-lock creation is the
		// documented metadata birth (subtest D); the assertion is that
		// it holds no rows and no user object was created.
		if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("refused COPY batch must record nothing, got %s rows", got)
		}

		// FROM PROGRAM — fail-before: the program executes on the pre-M05
		// binary (its output lands as rows).
		dbC := newDB(t)
		dirC := writeMigrations(t, map[string]string{
			"001_prog.up.sql": "CREATE TABLE prog_t (data text);\nCOPY prog_t FROM PROGRAM 'echo pwned-by-program';",
		})
		if code, out := runCLIProcess(t, preM05Bin, dbC, "migrate", "--dir", dirC); code != 0 {
			t.Fatalf("pre-M05 binary must execute the FROM PROGRAM copy (fail-before): %s", out)
		}
		if got := query(t, dbC, `SELECT count(*) FROM prog_t WHERE data LIKE 'pwned-by-program%'`); got != "1" {
			t.Fatalf("fail-before: the program must have executed, got %s rows", got)
		}

		// Guarded: refused outright — nothing in the file runs, not even
		// the legal CREATE TABLE before the COPY.
		dbD := newDB(t)
		dirD := writeMigrations(t, map[string]string{
			"001_prog.up.sql": "CREATE TABLE prog_t (data text);\nCOPY prog_t FROM PROGRAM 'echo pwned-by-program';",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbD, append([]string{"migrate", "--dir", dirD}, flags...)...); code == 0 {
				t.Fatalf("COPY FROM PROGRAM accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind COPY is refused") || !strings.Contains(out, "arbitrary program execution") {
				t.Fatalf("refusal must name COPY and the program-execution reason (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbD, `SELECT count(*) FROM information_schema.tables WHERE table_name='prog_t'`); got != "0" {
			t.Fatalf("atomic refusal must leave prog_t uncreated, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// X. Pass-3 escalation core property: a mixed file — legal
	//    statement first, refused kind after — applies NOTHING. The
	//    validation failure is atomic across the whole batch.
	// ------------------------------------------------------------------
	t.Run("MixedFileAtomicRefusal", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dir, "002_mixed.up.sql"),
			"CREATE TABLE legit_t (id int);\nEXPLAIN ANALYZE DELETE FROM _neutron_migrations;")
		writeFile(t, filepath.Join(dir, "002_mixed.down.sql"), "DROP TABLE legit_t;")

		// FAIL-BEFORE: the pre-M05 binary applies the whole file — legit_t
		// exists AND the 001 history row is destroyed.
		if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("pre-M05 binary must apply the mixed file (fail-before): %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='legit_t'`); got != "1" {
			t.Fatalf("fail-before: legit_t must exist, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "0" {
			t.Fatalf("fail-before: the 001 row must be destroyed, got %s", got)
		}

		// Guarded: NOTHING applies — not the legal statement before the
		// refused one, not the earlier migration's re-run.
		dbB := newDB(t)
		dirB := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbB, "migrate", "--dir", dirB); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirB, "002_mixed.up.sql"),
			"CREATE TABLE legit_t (id int);\nEXPLAIN ANALYZE DELETE FROM _neutron_migrations;")
		writeFile(t, filepath.Join(dirB, "002_mixed.down.sql"), "DROP TABLE legit_t;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", dirB}, flags...)...); code == 0 {
				t.Fatalf("mixed file accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement 2") || !strings.Contains(out, "statement kind EXPLAIN is refused") {
				t.Fatalf("refusal must name statement 2 and its kind (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbB, `SELECT count(*) FROM information_schema.tables WHERE table_name='legit_t'`); got != "0" {
			t.Fatalf("atomic refusal: legit_t must NOT exist, got %s", got)
		}
		if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history must hold exactly 001 after the refused batch, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// Y. Review-4 BLOCKER-1: database-wide and role-wide kinds were
	//    allowlisted via the full guard vocabulary and executed without
	//    acknowledgement — a CIC statement made the batch
	//    nontransactional, where the server permits DROP DATABASE of
	//    ANOTHER database in the cluster (rc 0, victim gone); ALTER
	//    ROLE persisted pg_roles.rolconfig; DROP ROLE executed on
	//    unprotected names. The allowlist now intersects ALTER/DROP
	//    acceptance with the schema-object subset; the guard
	//    vocabulary itself is untouched. The CIC+DROP DATABASE
	//    fail-before is reproduced on the attempt-4 binary in
	//    attempt-5.md (the pinned revision predates nontransactional execution and is
	//    server-blocked inside a transaction, so preM05Bin cannot carry
	//    that premise); the guarded assertions below lock the fix — a
	//    revert re-executes the attack. ALTER ROLE and DROP ROLE carry
	//    their own preM05Bin fail-befores (transactional shapes).
	// ------------------------------------------------------------------
	t.Run("DatabaseAndRoleWideKindsRefused", func(t *testing.T) {
		admin, err := db.Connect(context.Background(), base)
		if err != nil {
			t.Fatalf("connect admin: %v", err)
		}
		t.Cleanup(admin.Close)
		unique := fmt.Sprintf("m05_%d_%d", os.Getpid(), time.Now().UnixNano())
		victim := unique + "_victim"
		role := unique + "_role"
		role2 := unique + "_role2"
		ts := unique + "_ts"
		if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %q`, victim)); err != nil {
			t.Fatalf("create victim database: %v", err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, victim)); err != nil {
				t.Errorf("drop victim database: %v", err)
			}
		})
		if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE ROLE %q NOLOGIN`, role)); err != nil {
			t.Fatalf("create role: %v", err)
		}
		t.Cleanup(func() {
			if err := admin.Exec(context.Background(), fmt.Sprintf(`DROP ROLE IF EXISTS %q`, role)); err != nil {
				t.Errorf("drop role: %v", err)
			}
		})
		if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE ROLE %q NOLOGIN`, role2)); err != nil {
			t.Fatalf("create role2: %v", err)
		}
		t.Cleanup(func() {
			if err := admin.Exec(context.Background(), fmt.Sprintf(`DROP ROLE IF EXISTS %q`, role2)); err != nil {
				t.Errorf("drop role2: %v", err)
			}
		})
		// The LOCATION must exist on the SERVER's filesystem. A local server
		// sees the test's temp dir; a containerized one (CI service
		// containers) does not, so fall back to a directory the server
		// creates itself (superuser COPY ... TO PROGRAM, owned by the server
		// user as CREATE TABLESPACE requires).
		tsdir := t.TempDir()
		if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE TABLESPACE %q LOCATION '%s'`, ts, tsdir)); err != nil {
			if !strings.Contains(err.Error(), "58P01") {
				t.Fatalf("create tablespace: %v", err)
			}
			tsdir = fmt.Sprintf("/tmp/neutron_ts_%d_%d", os.Getpid(), time.Now().UnixNano())
			if err := admin.Exec(context.Background(), fmt.Sprintf(`COPY (SELECT 1) TO PROGRAM 'mkdir -p %s'`, tsdir)); err != nil {
				t.Skipf("server cannot see the client's filesystem and cannot create a directory itself (%v); tablespace refusal needs a server-visible directory", err)
			}
			t.Cleanup(func() {
				_ = admin.Exec(context.Background(), fmt.Sprintf(`COPY (SELECT 1) TO PROGRAM 'rm -rf %s'`, tsdir))
			})
			if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE TABLESPACE %q LOCATION '%s'`, ts, tsdir)); err != nil {
				t.Fatalf("create tablespace in server-created dir: %v", err)
			}
		}
		t.Cleanup(func() {
			if err := admin.Exec(context.Background(), fmt.Sprintf(`DROP TABLESPACE IF EXISTS %q`, ts)); err != nil {
				t.Errorf("drop tablespace: %v", err)
			}
		})

		// DROP DATABASE mixed with CIC — the exact nontransactional
		// window: refused atomically under both flag variants, the
		// victim database intact, nothing in the file applied.
		dbA := newDB(t)
		dirA := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbA, "migrate", "--dir", dirA); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirA, "010_mix.up.sql"),
			"CREATE TABLE cic_t (id int);\nCREATE INDEX CONCURRENTLY ci ON cic_t (id);\nDROP DATABASE "+victim+";")
		writeFile(t, filepath.Join(dirA, "010_mix.down.sql"), "SELECT 1;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbA, append([]string{"migrate", "--dir", dirA, "--timeout", "120s"}, flags...)...); code == 0 {
				t.Fatalf("CIC+DROP DATABASE file accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind DROP DATABASE is refused") || !strings.Contains(out, "schema objects only") {
				t.Fatalf("refusal must name DROP DATABASE and the schema-object reason (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbA, `SELECT count(*) FROM pg_database WHERE datname='`+victim+`'`); got != "1" {
			t.Fatalf("the victim database must survive the refused batch, got count %s", got)
		}
		if got := query(t, dbA, `SELECT count(*) FROM information_schema.tables WHERE table_name='cic_t'`); got != "0" {
			t.Fatalf("atomic refusal: cic_t must NOT exist, got %s", got)
		}
		if got := query(t, dbA, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history must hold exactly 001, got %s", got)
		}

		// ALTER ROLE — fail-before: the pre-M05 binary applies it and the
		// role's configuration is persistently mutated.
		dbB := newDB(t)
		dirB := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		writeFile(t, filepath.Join(dirB, "020_ar.up.sql"), "ALTER ROLE "+role+" SET statement_timeout = '10s';")
		writeFile(t, filepath.Join(dirB, "020_ar.down.sql"), "SELECT 1;")
		if code, out := runCLIProcess(t, preM05Bin, dbB, "migrate", "--dir", dirB); code != 0 {
			t.Fatalf("pre-M05 binary must apply the ALTER ROLE (fail-before): %s", out)
		}
		if got := query(t, dbB, `SELECT count(*) FROM pg_roles WHERE rolname='`+role+`' AND rolconfig IS NOT NULL`); got != "1" {
			t.Fatalf("fail-before: ALTER ROLE must have persisted rolconfig, got %s", got)
		}
		execRaw(t, base, "ALTER ROLE "+role+" RESET statement_timeout")
		dbB2 := newDB(t)
		dirB2 := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbB2, "migrate", "--dir", dirB2); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirB2, "020_ar.up.sql"), "ALTER ROLE "+role+" SET statement_timeout = '10s';")
		writeFile(t, filepath.Join(dirB2, "020_ar.down.sql"), "SELECT 1;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB2, append([]string{"migrate", "--dir", dirB2}, flags...)...); code == 0 {
				t.Fatalf("ALTER ROLE accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind ALTER ROLE is refused") || !strings.Contains(out, "schema objects only") {
				t.Fatalf("refusal must name ALTER ROLE and the schema-object reason (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbB2, `SELECT count(*) FROM pg_roles WHERE rolname='`+role+`' AND rolconfig IS NOT NULL`); got != "0" {
			t.Fatalf("the role's configuration must be untouched by the refused batch, got %s", got)
		}

		// DROP ROLE — fail-before: the pre-M05 binary drops the role.
		dbC := newDB(t)
		dirC := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		writeFile(t, filepath.Join(dirC, "030_dr.up.sql"), "DROP ROLE "+role2+";")
		writeFile(t, filepath.Join(dirC, "030_dr.down.sql"), "SELECT 1;")
		if code, out := runCLIProcess(t, preM05Bin, dbC, "migrate", "--dir", dirC); code != 0 {
			t.Fatalf("pre-M05 binary must apply the DROP ROLE (fail-before): %s", out)
		}
		if got := query(t, dbC, `SELECT count(*) FROM pg_roles WHERE rolname='`+role2+`'`); got != "0" {
			t.Fatalf("fail-before: DROP ROLE must have destroyed the role, got %s", got)
		}
		if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE ROLE %q NOLOGIN`, role2)); err != nil {
			t.Fatalf("re-create role2: %v", err)
		}
		dbC2 := newDB(t)
		dirC2 := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbC2, "migrate", "--dir", dirC2); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirC2, "030_dr.up.sql"), "DROP ROLE "+role2+";")
		writeFile(t, filepath.Join(dirC2, "030_dr.down.sql"), "SELECT 1;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbC2, append([]string{"migrate", "--dir", dirC2}, flags...)...); code == 0 {
				t.Fatalf("DROP ROLE accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind DROP ROLE is refused") || !strings.Contains(out, "schema objects only") {
				t.Fatalf("refusal must name DROP ROLE and the schema-object reason (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbC2, `SELECT count(*) FROM pg_roles WHERE rolname='`+role2+`'`); got != "1" {
			t.Fatalf("the role must survive the refused batch, got %s", got)
		}

		// DROP TABLESPACE mixed with CIC (same nontransactional window):
		// refused under both flag variants, tablespace intact.
		dbD := newDB(t)
		dirD := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbD, "migrate", "--dir", dirD); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirD, "040_ts.up.sql"),
			"CREATE TABLE cic2_t (id int);\nCREATE INDEX CONCURRENTLY ci2 ON cic2_t (id);\nDROP TABLESPACE "+ts+";")
		writeFile(t, filepath.Join(dirD, "040_ts.down.sql"), "SELECT 1;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbD, append([]string{"migrate", "--dir", dirD, "--timeout", "120s"}, flags...)...); code == 0 {
				t.Fatalf("CIC+DROP TABLESPACE file accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind DROP TABLESPACE is refused") || !strings.Contains(out, "schema objects only") {
				t.Fatalf("refusal must name DROP TABLESPACE and the schema-object reason (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbD, `SELECT count(*) FROM pg_tablespace WHERE spcname='`+ts+`'`); got != "1" {
			t.Fatalf("the tablespace must survive the refused batch, got %s", got)
		}

		// ALTER DATABASE / CREATE DATABASE — already refused before this
		// rework (alterKinds never held database; createAllowKinds never
		// held it): regression-locked so they stay refused.
		dbE := newDB(t)
		dirE := writeMigrations(t, map[string]string{
			"001_base.up.sql":   "CREATE TABLE base_t (id int);",
			"001_base.down.sql": "DROP TABLE base_t;",
		})
		if code, out := runCLI(t, dbE, "migrate", "--dir", dirE); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(dirE, "050_ctl.up.sql"),
			"ALTER DATABASE "+victim+" SET search_path = public;\nCREATE DATABASE "+unique+"_newdb;")
		writeFile(t, filepath.Join(dirE, "050_ctl.down.sql"), "SELECT 1;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbE, append([]string{"migrate", "--dir", dirE}, flags...)...); code == 0 {
				t.Fatalf("ALTER DATABASE + CREATE DATABASE accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "statement kind ALTER DATABASE is refused") {
				t.Fatalf("refusal must name ALTER DATABASE (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbE, `SELECT count(*) FROM pg_database WHERE datname='`+unique+`_newdb'`); got != "0" {
			t.Fatalf("no database may be created by the refused batch, got %s", got)
		}
		t.Cleanup(func() {
			if err := admin.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, unique+"_newdb")); err != nil {
				t.Errorf("drop newdb database: %v", err)
			}
		})
	})

	// ------------------------------------------------------------------
	// Z. Review-4 MINOR-1: CREATE OR REPLACE spellings refused with the
	//    degraded label "CREATE OR" and the generic reason. The label
	//    now skips the modifier words, so each form names its real kind
	//    and fires the precise per-kind reason. (Fail-before: the pre-M05
	//    binary applies these statements outright — pre-allowlist.)
	// ------------------------------------------------------------------
	t.Run("CreateOrReplaceKindsNamedPrecisely", func(t *testing.T) {
		forms := []struct {
			name   string
			sql    string
			label  string
			reason string
		}{
			{"Function", "CREATE OR REPLACE FUNCTION orfunc() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$;", "CREATE FUNCTION", "opaque"},
			{"Procedure", "CREATE OR REPLACE PROCEDURE orproc() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;", "CREATE PROCEDURE", "opaque"},
			{"Rule", "CREATE OR REPLACE RULE orr AS ON DELETE TO base_t DO INSTEAD SELECT 1;", "CREATE RULE", "execution time"},
			{"Trigger", "CREATE OR REPLACE TRIGGER ort AFTER INSERT ON base_t FOR EACH ROW EXECUTE FUNCTION orsrc();", "CREATE TRIGGER", "opaque execution route"},
		}
		for _, form := range forms {
			t.Run(form.name, func(t *testing.T) {
				// FAIL-BEFORE: the pre-M05 binary applies the statement
				// (the trigger form needs its function to exist —
				// created through the harness, not migration SQL).
				dbA := newDB(t)
				dirA := writeMigrations(t, map[string]string{
					"001_base.up.sql":   "CREATE TABLE base_t (id int);",
					"001_base.down.sql": "DROP TABLE base_t;",
					"002_or.up.sql":     form.sql,
					"002_or.down.sql":   "SELECT 1;",
				})
				execRaw(t, dbA, "CREATE FUNCTION orsrc() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$")
				if code, out := runCLIProcess(t, preM05Bin, dbA, "migrate", "--dir", dirA); code != 0 {
					t.Fatalf("pre-M05 binary must apply the %s form (fail-before): %s", form.label, out)
				}

				// Guarded: refused naming the REAL kind (a degraded
				// "CREATE OR" label fails this) with its precise reason.
				dbB := newDB(t)
				dirB := writeMigrations(t, map[string]string{
					"001_base.up.sql":   "CREATE TABLE base_t (id int);",
					"001_base.down.sql": "DROP TABLE base_t;",
				})
				if code, out := runCLI(t, dbB, "migrate", "--dir", dirB); code != 0 {
					t.Fatalf("base migration must apply: %s", out)
				}
				writeFile(t, filepath.Join(dirB, "002_or.up.sql"), form.sql)
				writeFile(t, filepath.Join(dirB, "002_or.down.sql"), "SELECT 1;")
				for _, flags := range [][]string{{}, {"--allow-destructive"}} {
					if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", dirB}, flags...)...); code == 0 {
						t.Fatalf("%s form accepted (flags=%v)", form.label, flags)
					} else if !strings.Contains(out, "statement kind "+form.label+" is refused") {
						t.Fatalf("refusal must name the real kind %q, not a degraded label (flags=%v): %s", form.label, flags, out)
					} else if !strings.Contains(out, form.reason) {
						t.Fatalf("refusal must carry the precise reason %q (flags=%v): %s", form.reason, flags, out)
					}
				}
				if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
					t.Fatalf("history must hold exactly 001 after the refused batch, got %s", got)
				}
			})
		}

		// The allowed OR REPLACE form (VIEW) still applies — the label
		// fix must not narrow classification.
		dbV := newDB(t)
		dirV := writeMigrations(t, map[string]string{
			"001_orv.up.sql":   "CREATE OR REPLACE VIEW orv AS SELECT 1 AS c;",
			"001_orv.down.sql": "DROP VIEW orv;",
		})
		if code, out := runCLI(t, dbV, "migrate", "--dir", dirV); code != 0 {
			t.Fatalf("CREATE OR REPLACE VIEW must still apply: %s", out)
		}
	})

	// ------------------------------------------------------------------
	// AA. Review-5 BLOCKER-1 (a): the recursive pg_depend cascade check
	//     ran only for relation drops — DROP DOMAIN/TYPE CASCADE destroyed
	//     a column of a protected-prefix table with NO acknowledgement,
	//     and DROP SCHEMA ... CASCADE --allow-destructive destroyed a
	//     protected-prefix view in ANOTHER schema. Non-relation cascade
	//     kinds now run the same transitive closure, cross-schema. (The
	//     attempt-5-binary fail-befores are reproduced on
	//     /tmp/m05a5/neutron-fix in attempt-6.md; the preM05Bin premises
	//     below carry the same attacks pre-gate.)
	// ------------------------------------------------------------------
	t.Run("NonRelationCascadeTransitivityRefused", func(t *testing.T) {
		plant := func(t *testing.T, dbURL string) {
			t.Helper()
			execRaw(t, dbURL, `CREATE SCHEMA vict`)
			execRaw(t, dbURL, `CREATE DOMAIN vict.d AS int`)
			execRaw(t, dbURL, `CREATE TABLE _neutron_z (c vict.d)`)
			execRaw(t, dbURL, `CREATE TYPE vict.st AS ENUM ('a')`)
			execRaw(t, dbURL, `CREATE TABLE _neutron_zt (c vict.st)`)
			execRaw(t, dbURL, `CREATE TABLE vict.t (i int)`)
			execRaw(t, dbURL, `CREATE VIEW _neutron_dep AS SELECT i FROM vict.t`)
		}

		// Vector 1 — DROP DOMAIN ... CASCADE, no acknowledgement.
		dbA := newDB(t)
		plant(t, dbA)
		dirA := writeMigrations(t, map[string]string{
			"001_dom.up.sql": "DROP DOMAIN vict.d CASCADE;",
		})
		// FAIL-BEFORE (pre-M05 binary): applies ungated; the domain
		// cascade empties the protected table's column.
		if code, out := runCLIProcess(t, preM05Bin, dbA, "migrate", "--dir", dirA); code != 0 {
			t.Fatalf("pre-M05 binary must apply the domain cascade (fail-before): %s", out)
		}
		if got := query(t, dbA, `SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_z'`); got != "0" {
			t.Fatalf("fail-before: the domain cascade must have dropped _neutron_z's column, got %s columns", got)
		}
		dbB := newDB(t)
		plant(t, dbB)
		baseB := writeMigrations(t, map[string]string{
			"001_init.up.sql":   "CREATE TABLE app_t (id int);",
			"001_init.down.sql": "DROP TABLE app_t;",
		})
		if code, out := runCLI(t, dbB, "migrate", "--dir", baseB); code != 0 {
			t.Fatalf("base migration must apply: %s", out)
		}
		writeFile(t, filepath.Join(baseB, "002_dom.up.sql"), "DROP DOMAIN vict.d CASCADE;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", baseB}, flags...)...); code == 0 {
				t.Fatalf("DROP DOMAIN CASCADE onto a protected table accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "_neutron_z") || !strings.Contains(out, "transitively") {
				t.Fatalf("refusal must name the protected transitive dependent (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbB, `SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_z' AND column_name='c'`); got != "1" {
			t.Fatalf("the protected column must survive the refused domain cascade, got %s", got)
		}
		if got := query(t, dbB, `SELECT count(*) FROM pg_type WHERE typname='d' AND typnamespace='vict'::regnamespace`); got != "1" {
			t.Fatalf("the domain itself must survive the refused cascade, got %s", got)
		}
		if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history must hold exactly 001, got %s", got)
		}

		// DROP TYPE ... CASCADE — same transitive path, ack-gated kind.
		writeFile(t, filepath.Join(baseB, "003_type.up.sql"), "DROP TYPE vict.st CASCADE;")
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", baseB}, flags...)...); code == 0 {
				t.Fatalf("DROP TYPE CASCADE onto a protected table accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "_neutron_zt") || !strings.Contains(out, "transitively") {
				t.Fatalf("refusal must name the protected transitive dependent (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbB, `SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_zt' AND column_name='c'`); got != "1" {
			t.Fatalf("the protected column must survive the refused type cascade, got %s", got)
		}

		// Vector 2 — DROP SCHEMA ... CASCADE destroys a protected view
		// in ANOTHER schema (the cross-schema transitive dependent).
		writeFile(t, filepath.Join(baseB, "006_schema.up.sql"), "DROP SCHEMA vict CASCADE;")
		// FAIL-BEFORE (pre-M05 binary): applies ungated; the schema and
		// the cross-schema dependent view are destroyed.
		if code, out := runCLIProcess(t, preM05Bin, dbB, "migrate", "--dir", baseB); code != 0 {
			t.Fatalf("pre-M05 binary must apply the schema cascade (fail-before): %s", out)
		}
		if got := query(t, dbB, `SELECT count(*) FROM information_schema.schemata WHERE schema_name='vict'`); got != "0" {
			t.Fatalf("fail-before: the schema must be destroyed, got %s", got)
		}
		if got := query(t, dbB, `SELECT count(*) FROM pg_class WHERE relname='_neutron_dep'`); got != "0" {
			t.Fatalf("fail-before: the cross-schema protected view must be destroyed, got %s", got)
		}
		dbC := newDB(t)
		plant(t, dbC)
		baseC := writeMigrations(t, map[string]string{
			"001_init.up.sql":   "CREATE TABLE app_t (id int);",
			"001_init.down.sql": "DROP TABLE app_t;",
			"006_schema.up.sql": "DROP SCHEMA vict CASCADE;",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbC, append([]string{"migrate", "--dir", baseC}, flags...)...); code == 0 {
				t.Fatalf("DROP SCHEMA CASCADE with a cross-schema protected dependent accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "vict") || !strings.Contains(out, "_neutron_dep") {
				t.Fatalf("refusal must name the schema and the cross-schema victim (flags=%v): %s", flags, out)
			}
		}
		for _, probe := range []string{
			`SELECT count(*) FROM information_schema.schemata WHERE schema_name='vict'`,
			`SELECT count(*) FROM pg_class WHERE relname='_neutron_dep'`,
			`SELECT count(*) FROM pg_class WHERE relname='_neutron_z'`,
		} {
			if got := query(t, dbC, probe); got != "1" {
				t.Fatalf("schema and protected dependents must survive the refused cascade (%s), got %s", probe, got)
			}
		}

		// No-regression controls: legitimate non-relation cascades of
		// purely user objects keep their documented semantics.
		dbD := newDB(t)
		execRaw(t, dbD, `CREATE SCHEMA leg_s`)
		execRaw(t, dbD, `CREATE DOMAIN leg_s.d AS int`)
		execRaw(t, dbD, `CREATE TYPE leg_s.st AS ENUM ('a')`)
		execRaw(t, dbD, `CREATE TABLE leg_t (c leg_s.d)`)
		execRaw(t, dbD, `CREATE TABLE leg_t2 (c leg_s.st)`)
		execRaw(t, dbD, `CREATE SCHEMA leg2_s`)
		execRaw(t, dbD, `CREATE TABLE leg2_s.tbl (i int)`)
		dirD := writeMigrations(t, map[string]string{
			"001_legdom.up.sql": "DROP DOMAIN leg_s.d CASCADE;",
		})
		// DROP DOMAIN carries no acknowledgement (documented ack
		// vocabulary) — it applies with no flags.
		if code, out := runCLI(t, dbD, "migrate", "--dir", dirD); code != 0 {
			t.Fatalf("legit DROP DOMAIN CASCADE over user dependents refused (over-refusal): %s", out)
		}
		if got := query(t, dbD, `SELECT count(*) FROM information_schema.columns WHERE table_name='leg_t' AND column_name='c'`); got != "0" {
			t.Fatalf("legit domain cascade did not drop the user column, got %s", got)
		}
		writeFile(t, filepath.Join(dirD, "002_legtype.up.sql"), "DROP TYPE leg_s.st CASCADE;")
		writeFile(t, filepath.Join(dirD, "002_legtype.down.sql"), "SELECT 1;")
		// DROP TYPE is ack-gated: no flag demands the acknowledgement.
		if code, out := runCLI(t, dbD, "migrate", "--dir", dirD); code == 0 || !strings.Contains(out, "--allow-destructive") {
			t.Fatalf("legit DROP TYPE CASCADE must demand acknowledgement without the flag (code %d): %s", code, out)
		}
		if code, out := runCLI(t, dbD, "migrate", "--dir", dirD, "--allow-destructive"); code != 0 {
			t.Fatalf("acknowledged legit DROP TYPE CASCADE refused: %s", out)
		}
		if got := query(t, dbD, `SELECT count(*) FROM information_schema.columns WHERE table_name='leg_t2' AND column_name='c'`); got != "0" {
			t.Fatalf("acknowledged type cascade did not drop the user column, got %s", got)
		}
		writeFile(t, filepath.Join(dirD, "003_legschema.up.sql"), "DROP SCHEMA leg2_s CASCADE;")
		writeFile(t, filepath.Join(dirD, "003_legschema.down.sql"), "SELECT 1;")
		if code, out := runCLI(t, dbD, "migrate", "--dir", dirD); code == 0 || !strings.Contains(out, "--allow-destructive") {
			t.Fatalf("legit DROP SCHEMA CASCADE must demand acknowledgement without the flag (code %d): %s", code, out)
		}
		if code, out := runCLI(t, dbD, "migrate", "--dir", dirD, "--allow-destructive"); code != 0 {
			t.Fatalf("acknowledged legit DROP SCHEMA CASCADE refused: %s", out)
		}
		if got := query(t, dbD, `SELECT count(*) FROM information_schema.schemata WHERE schema_name='leg2_s'`); got != "0" {
			t.Fatalf("acknowledged schema cascade did not drop the user schema, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// BB. Review-5 BLOCKER-1 (b)+(c): the planting enabler and the
	//     intra-file ordering hole. CREATE of _neutron_-prefixed names
	//     is refused (the namespace is reserved), so a create-then-drop
	//     schema file cannot plant its victim; and DROP EXTENSION of an
	//     extension the SAME batch creates is refused on the ordering
	//     alone (its members are invisible to the pre-execution member
	//     count). The pre-existing 60-member cube control must keep
	//     refusing via the member rule.
	// ------------------------------------------------------------------
	t.Run("PlantedLookalikesAndIntraFileOrderingRefused", func(t *testing.T) {
		// The planting enabler closed: migration SQL cannot create
		// _neutron_-prefixed objects at all.
		dbA := newDB(t)
		dirA := writeMigrations(t, map[string]string{
			"001_plant.up.sql":   "CREATE TABLE ok_t (i int);\nCREATE TABLE _neutron_plant (i int);",
			"001_plant.down.sql": "DROP TABLE ok_t;",
		})
		// FAIL-BEFORE (pre-M05 binary): the planted lookalike applies.
		if code, out := runCLIProcess(t, preM05Bin, dbA, "migrate", "--dir", dirA); code != 0 {
			t.Fatalf("pre-M05 binary must apply the planted lookalike (fail-before): %s", out)
		}
		if got := query(t, dbA, `SELECT count(*) FROM pg_class WHERE relname='_neutron_plant'`); got != "1" {
			t.Fatalf("fail-before: the planted table must exist, got %s", got)
		}
		dbB := newDB(t)
		dirB := writeMigrations(t, map[string]string{
			"001_plant.up.sql":   "CREATE TABLE ok_t (i int);\nCREATE TABLE _neutron_plant (i int);",
			"001_plant.down.sql": "DROP TABLE ok_t;",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbB, append([]string{"migrate", "--dir", dirB}, flags...)...); code == 0 {
				t.Fatalf("CREATE of a _neutron_-prefixed table accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "_neutron_plant") || !strings.Contains(out, "reserved") {
				t.Fatalf("refusal must name the target and the reserved-namespace reason (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbB, `SELECT count(*) FROM pg_class WHERE relname IN ('_neutron_plant','ok_t')`); got != "0" {
			t.Fatalf("atomic refusal: neither table may exist, got %s", got)
		}
		if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("no history may record the refused batch, got %s rows", got)
		}

		// Vector 3 — create-then-drop-schema-with-_neutron_-inside in
		// ONE file (with ack on the attempt-5 binary): the create-side
		// refusal fires before anything runs.
		dirC := writeMigrations(t, map[string]string{
			"001_insch.up.sql":   "CREATE SCHEMA guard_s;\nCREATE TABLE guard_s._neutron_insch (i int);\nDROP SCHEMA guard_s CASCADE;",
			"001_insch.down.sql": "SELECT 1;",
		})
		// FAIL-BEFORE (pre-M05 binary): applies ungated — the file
		// creates the schema with its planted table and destroys both.
		if code, out := runCLIProcess(t, preM05Bin, dbB, "migrate", "--dir", dirC); code != 0 {
			t.Fatalf("pre-M05 binary must apply the create-then-drop file (fail-before): %s", out)
		}
		if got := query(t, dbB, `SELECT count(*) FROM information_schema.schemata WHERE schema_name='guard_s'`); got != "0" {
			t.Fatalf("fail-before: the self-dropping schema must be gone, got %s", got)
		}
		if got := query(t, dbB, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "1" {
			t.Fatalf("fail-before: pre-M05 must have recorded the applied 001, got %s", got)
		}
		dbC := newDB(t)
		dirC2 := writeMigrations(t, map[string]string{
			"001_insch.up.sql":   "CREATE SCHEMA guard_s;\nCREATE TABLE guard_s._neutron_insch (i int);\nDROP SCHEMA guard_s CASCADE;",
			"001_insch.down.sql": "SELECT 1;",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbC, append([]string{"migrate", "--dir", dirC2}, flags...)...); code == 0 {
				t.Fatalf("create-then-drop file with a planted _neutron_ table accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "_neutron_insch") || !strings.Contains(out, "reserved") {
				t.Fatalf("refusal must name the planted table and the reserved-namespace reason (flags=%v): %s", flags, out)
			}
		}
		for _, probe := range []string{
			`SELECT count(*) FROM information_schema.schemata WHERE schema_name='guard_s'`,
			`SELECT count(*) FROM pg_class WHERE relname='_neutron_insch'`,
		} {
			if got := query(t, dbC, probe); got != "0" {
				t.Fatalf("atomic refusal: nothing in the file may exist (%s), got %s", probe, got)
			}
		}

		// Vector 4 — CREATE EXTENSION + DROP EXTENSION in one file,
		// NO flag: the member guard read 0 members at validation time
		// (the extension did not exist yet); the ordering refusal
		// closes it.
		dbD := newDB(t)
		dirD := writeMigrations(t, map[string]string{
			"001_ext.up.sql":   "CREATE EXTENSION IF NOT EXISTS cube;\nDROP EXTENSION cube;",
			"001_ext.down.sql": "SELECT 1;",
		})
		// FAIL-BEFORE (pre-M05 binary): applies — the extension is
		// created and destroyed in one migration.
		if code, out := runCLIProcess(t, preM05Bin, dbD, "migrate", "--dir", dirD); code != 0 {
			t.Fatalf("pre-M05 binary must apply the extension round-trip (fail-before): %s", out)
		}
		if got := query(t, dbD, `SELECT count(*) FROM pg_extension WHERE extname='cube'`); got != "0" {
			t.Fatalf("fail-before: the extension must be gone after the round-trip, got %s", got)
		}
		dbE := newDB(t)
		dirE := writeMigrations(t, map[string]string{
			"001_ext.up.sql":   "CREATE EXTENSION IF NOT EXISTS cube;\nDROP EXTENSION cube;",
			"001_ext.down.sql": "SELECT 1;",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbE, append([]string{"migrate", "--dir", dirE}, flags...)...); code == 0 {
				t.Fatalf("same-batch CREATE+DROP EXTENSION accepted (flags=%v)", flags)
			} else if !strings.Contains(out, "cube") || !strings.Contains(out, "batch also CREATEs") {
				t.Fatalf("refusal must name the extension and the ordering reason (flags=%v): %s", flags, out)
			}
		}
		if got := query(t, dbE, `SELECT count(*) FROM pg_extension WHERE extname='cube'`); got != "0" {
			t.Fatalf("atomic refusal: the extension must never have been created, got %s", got)
		}
		if got := query(t, dbE, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("no history may record the refused batch, got %s rows", got)
		}

		// Control: a PRE-EXISTING extension with members (planted
		// outside migrations, before the run) is still refused by the
		// member rule — the ordering check must not have weakened it.
		dbF := newDB(t)
		execRaw(t, dbF, `CREATE EXTENSION cube`)
		members := query(t, dbF, `SELECT count(*) FROM pg_depend d JOIN pg_extension e ON e.oid = d.refobjid WHERE d.deptype = 'e' AND e.extname = 'cube'`)
		if members == "0" {
			t.Fatalf("control premise wrong: cube must have members, got %s", members)
		}
		dirF := writeMigrations(t, map[string]string{
			"001_drop.up.sql":   "DROP EXTENSION cube;",
			"001_drop.down.sql": "SELECT 1;",
		})
		for _, flags := range [][]string{{}, {"--allow-destructive"}} {
			if code, out := runCLI(t, dbF, append([]string{"migrate", "--dir", dirF}, flags...)...); code == 0 {
				t.Fatalf("pre-existing member-bearing extension drop accepted (flags=%v)", flags)
			} else if !strings.Contains(out, members+" extension-owned member") {
				t.Fatalf("refusal must name the live member count %s (flags=%v): %s", members, flags, out)
			}
		}
		if got := query(t, dbF, `SELECT count(*) FROM pg_extension WHERE extname='cube'`); got != "1" {
			t.Fatalf("the pre-existing extension must survive, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// Y. Review-6 BLOCKER-1: the six demonstrated cascade escapes —
	//    DROP FUNCTION/OPERATOR/COLLATION CASCADE, DROP SCHEMA CASCADE
	//    over a collation-only schema, DROP TABLE CASCADE stripping an
	//    FK of a protected table, DROP SEQUENCE CASCADE stripping a
	//    column default — reproduced fail-before on the pre-M05 binary
	//    binary, then refused under both flag variants with victims
	//    intact and nothing applied.
	// ------------------------------------------------------------------
	t.Run("Review6CascadeVectorsRefused", func(t *testing.T) {
		vectors := []struct {
			name   string
			plant  []string
			drop   string
			named  string
			probes [][2]string // intact-state probe -> expected
		}{
			{
				name: "FunctionCascadeDestroysProtectedView",
				plant: []string{
					`CREATE SCHEMA vict; CREATE TABLE vict.t (i int);`,
					`CREATE FUNCTION vict.f() RETURNS int LANGUAGE sql RETURN 7;`,
					`CREATE VIEW _neutron_fnv AS SELECT vict.f() AS r;`,
				},
				drop:   "DROP FUNCTION vict.f() CASCADE;",
				named:  "_neutron_fnv",
				probes: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_fnv'`, "1"}},
			},
			{
				name: "OperatorCascadeDestroysProtectedView",
				plant: []string{
					`CREATE SCHEMA vict; CREATE TABLE vict.t (i int);`,
					`CREATE OPERATOR vict.=== (LEFTARG = int, RIGHTARG = int, PROCEDURE = int4eq);`,
					`CREATE VIEW _neutron_opv AS SELECT i OPERATOR(vict.===) 1 AS r FROM vict.t;`,
				},
				drop:   "DROP OPERATOR vict.===(int, int) CASCADE;",
				named:  "_neutron_opv",
				probes: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_opv'`, "1"}},
			},
			{
				name: "CollationCascadeDestroysProtectedColumn",
				plant: []string{
					`CREATE SCHEMA vs; CREATE COLLATION vs.c (provider = icu, locale = 'und');`,
					`CREATE TABLE _neutron_colt (t text COLLATE vs.c);`,
				},
				drop:   "DROP COLLATION vs.c CASCADE;",
				named:  "_neutron_colt",
				probes: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_colt'`, "1"}},
			},
			{
				name: "CollationOnlySchemaCascadeDestroysProtectedColumn",
				plant: []string{
					`CREATE SCHEMA vs3; CREATE COLLATION vs3.c (provider = icu, locale = 'und');`,
					`CREATE TABLE _neutron_sct (t text COLLATE vs3.c);`,
				},
				drop:  "DROP SCHEMA vs3 CASCADE;", // ack demand only on the pre-M05 binary
				named: "_neutron_sct",
				probes: [][2]string{
					{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_sct'`, "1"},
					{`SELECT count(*) FROM information_schema.schemata WHERE schema_name='vs3'`, "1"},
				},
			},
			{
				name: "TableCascadeStripsProtectedFK",
				plant: []string{
					`CREATE SCHEMA vict; CREATE TABLE vict.par (id int PRIMARY KEY);`,
					`CREATE TABLE _neutron_fk (id int REFERENCES vict.par(id));`,
				},
				drop:  "DROP TABLE vict.par CASCADE;",
				named: "_neutron_fk",
				probes: [][2]string{
					{`SELECT count(*) FROM pg_constraint WHERE conrelid='_neutron_fk'::regclass AND contype='f'`, "1"},
					{`SELECT count(*) FROM pg_class WHERE relname='vict.par' OR relname='par'`, "1"},
				},
			},
			{
				name: "SequenceCascadeStripsProtectedDefault",
				plant: []string{
					`CREATE SCHEMA vs2; CREATE SEQUENCE vs2.s;`,
					`CREATE TABLE _neutron_def (i int DEFAULT nextval('vs2.s'));`,
				},
				drop:   "DROP SEQUENCE vs2.s CASCADE;",
				named:  "_neutron_def",
				probes: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_def' AND column_default IS NOT NULL`, "1"}},
			},
		}
		for _, vec := range vectors {
			t.Run(vec.name, func(t *testing.T) {
				dbURL := newDB(t)
				for _, stmt := range vec.plant {
					execRaw(t, dbURL, stmt)
				}
				dir := t.TempDir()
				writeFile(t, filepath.Join(dir, "001_base.up.sql"), "CREATE TABLE base_t (i int);")
				writeFile(t, filepath.Join(dir, "001_base.down.sql"), "DROP TABLE base_t;")
				if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
					t.Fatalf("base migration must apply: %s", out)
				}
				writeFile(t, filepath.Join(dir, "002_vec.up.sql"), vec.drop)
				writeFile(t, filepath.Join(dir, "002_vec.down.sql"), "SELECT 1;")
				// FAIL-BEFORE (attempt-6 binary class): the pre-M05
				// pre-M05 binary carries no guard and no ack gate — it
				// applies every vector ungated and destroys the victim.
				if code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir); code != 0 {
					t.Fatalf("pre-M05 binary must apply the vector (fail-before): %s", out)
				}
				for _, probe := range vec.probes {
					if got := query(t, dbURL, probe[0]); got == probe[1] {
						t.Fatalf("fail-before premise wrong: victim intact before the pre-M05-binary run (%s)", probe[0])
					}
				}
				// Restore the fixture on a fresh database for the
				// guarded assertions.
				dbGuarded := newDB(t)
				for _, stmt := range vec.plant {
					execRaw(t, dbGuarded, stmt)
				}
				dirG := t.TempDir()
				writeFile(t, filepath.Join(dirG, "001_base.up.sql"), "CREATE TABLE base_t (i int);")
				writeFile(t, filepath.Join(dirG, "001_base.down.sql"), "DROP TABLE base_t;")
				if code, out := runCLI(t, dbGuarded, "migrate", "--dir", dirG); code != 0 {
					t.Fatalf("base migration must apply on the guarded DB: %s", out)
				}
				writeFile(t, filepath.Join(dirG, "002_vec.up.sql"), vec.drop)
				writeFile(t, filepath.Join(dirG, "002_vec.down.sql"), "SELECT 1;")
				for _, flags := range [][]string{{}, {"--allow-destructive"}} {
					if code, out := runCLI(t, dbGuarded, append([]string{"migrate", "--dir", dirG}, flags...)...); code == 0 {
						t.Fatalf("vector accepted (flags=%v): %s", flags, out)
					} else if !strings.Contains(out, "protected") || !strings.Contains(out, vec.named) {
						t.Fatalf("refusal must name protection and the victim %q (flags=%v): %s", vec.named, flags, out)
					}
				}
				for _, probe := range vec.probes {
					if got := query(t, dbGuarded, probe[0]); got != probe[1] {
						t.Fatalf("victim must survive the refused cascade (%s), got %s", probe[0], got)
					}
				}
				if got := query(t, dbGuarded, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
					t.Fatalf("only the base migration may be recorded, got %s rows", got)
				}
			})
		}
	})

	// ------------------------------------------------------------------
	// Z. Review-6 BLOCKER-1: the CASCADE property table — the reviewer's
	//    permanent regression surface. EVERY allowlisted drop kind is
	//    enumerated from db.AllowlistedDropKinds(); each carries a
	//    planted dependent chain terminating in a _neutron_-protected
	//    fixture (table/view/constraint/default/column), a fail-before
	//    run on the pre-M05 binary, refusal under BOTH flag
	//    variants with the victim intact and nothing applied, and the
	//    LEGAL direction — the same shape with ordinary user victims —
	//    applying with ack semantics. A kind added to the allowlist
	//    without a fixture fails here and in
	//    TestCascadeDispatchCoversEveryAllowlistedDropKind. Kinds for
	//    which PostgreSQL 17 cannot construct a dependent edge (nothing
	//    records a pg_depend reference INTO the object) assert the
	//    vacuous direction: the closure runs and does not over-refuse.
	// ------------------------------------------------------------------
	t.Run("CascadeKindPropertyTable", func(t *testing.T) {
		type legalSpec struct {
			plant []string
			drop  string
			ack   bool // no-flag run must demand --allow-destructive
			check [][2]string
		}
		type kindFixture struct {
			plant           []string
			drop            string
			named           string
			probes          [][2]string
			vacuous         string // non-empty: no constructible chain; assert no false refusal
			serverGuard     string // non-empty: only the server refuses it; assert layered refusal
			preM05Applies   bool   // the pre-M05 binary applies the drop (rc 0)
			preM05NeedsFlag bool   // ...but only with --allow-destructive
			legal           *legalSpec
		}
		fixtures := map[string]kindFixture{
			"table": {
				plant: []string{
					`CREATE SCHEMA pt_table; CREATE TABLE pt_table.src (i int);`,
					`CREATE VIEW _neutron_pt_table_v AS SELECT i FROM pt_table.src;`,
				},
				drop: "DROP TABLE pt_table.src CASCADE;", named: "_neutron_pt_table_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_table_v'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TABLE pt_table.legalt (i int); CREATE VIEW pt_table.legalv AS SELECT i FROM pt_table.legalt;`},
					drop:  "DROP TABLE pt_table.legalt CASCADE;", ack: true,
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='legalv'`, "0"}},
				},
			},
			"view": {
				plant: []string{
					`CREATE SCHEMA pt_view; CREATE VIEW pt_view.src AS SELECT 1 AS x;`,
					`CREATE VIEW _neutron_pt_view_v AS SELECT x FROM pt_view.src;`,
				},
				drop: "DROP VIEW pt_view.src CASCADE;", named: "_neutron_pt_view_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_view_v'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE VIEW pt_view.legal1 AS SELECT 1 AS x; CREATE VIEW pt_view.legal2 AS SELECT x FROM pt_view.legal1;`},
					drop:  "DROP VIEW pt_view.legal1 CASCADE;", ack: true,
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='legal2'`, "0"}},
				},
			},
			"materialized-view": {
				plant: []string{
					`CREATE SCHEMA pt_mview; CREATE MATERIALIZED VIEW pt_mview.src AS SELECT 1 AS x;`,
					`CREATE VIEW _neutron_pt_mview_v AS SELECT x FROM pt_mview.src;`,
				},
				drop: "DROP MATERIALIZED VIEW pt_mview.src CASCADE;", named: "_neutron_pt_mview_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_mview_v'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE MATERIALIZED VIEW pt_mview.legal1 AS SELECT 1 AS x; CREATE VIEW pt_mview.legal2 AS SELECT x FROM pt_mview.legal1;`},
					drop:  "DROP MATERIALIZED VIEW pt_mview.legal1 CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='legal2'`, "0"}},
				},
			},
			"index": {
				// The only possible dependent of an index is the
				// constraint that adopted it, and that edge is an
				// INTERNAL dependency PostgreSQL itself refuses to
				// break even with CASCADE — no guard-visible chain
				// exists. The refusal is layered: the ack gate without
				// the flag, the server's own error with it.
				plant: []string{
					`CREATE TABLE _neutron_pt_index (i int);`,
					`CREATE UNIQUE INDEX pt_index_ix ON _neutron_pt_index(i);`,
					`ALTER TABLE _neutron_pt_index ADD CONSTRAINT pt_index_c UNIQUE USING INDEX pt_index_ix;`,
				},
				drop: "DROP INDEX pt_index_c CASCADE;", named: "_neutron_pt_index",
				probes:        [][2]string{{`SELECT count(*) FROM pg_constraint WHERE conrelid='_neutron_pt_index'::regclass`, "1"}},
				serverGuard:   "the only dependent edge of an index is its owning constraint's internal dependency, which PostgreSQL refuses to break even with CASCADE",
				preM05Applies: false,
				legal: &legalSpec{
					plant: []string{`CREATE TABLE pt_index_t (i int); CREATE INDEX pt_index_li ON pt_index_t(i);`},
					drop:  "DROP INDEX pt_index_li CASCADE;", ack: true,
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='pt_index_li'`, "0"}},
				},
			},
			"sequence": {
				plant: []string{
					`CREATE SCHEMA pt_seq; CREATE SEQUENCE pt_seq.s;`,
					`CREATE TABLE _neutron_pt_seq (i int DEFAULT nextval('pt_seq.s'));`,
				},
				drop: "DROP SEQUENCE pt_seq.s CASCADE;", named: "_neutron_pt_seq",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_seq' AND column_default IS NOT NULL`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE SEQUENCE pt_seq.ls; CREATE TABLE pt_seq.lt (i int DEFAULT nextval('pt_seq.ls'));`},
					drop:  "DROP SEQUENCE pt_seq.ls CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='lt' AND column_default IS NOT NULL`, "0"}},
				},
			},
			"type": {
				plant: []string{
					`CREATE SCHEMA pt_type; CREATE TYPE pt_type.e AS ENUM ('a');`,
					`CREATE TABLE _neutron_pt_type (c pt_type.e);`,
				},
				drop: "DROP TYPE pt_type.e CASCADE;", named: "_neutron_pt_type",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_type'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TYPE pt_type.le AS ENUM ('a'); CREATE TABLE pt_type.lt (c pt_type.le);`},
					drop:  "DROP TYPE pt_type.le CASCADE;", ack: true,
					check: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='lt'`, "0"}},
				},
			},
			"domain": {
				plant: []string{
					`CREATE SCHEMA pt_domain; CREATE DOMAIN pt_domain.d AS int;`,
					`CREATE TABLE _neutron_pt_domain (c pt_domain.d);`,
				},
				drop: "DROP DOMAIN pt_domain.d CASCADE;", named: "_neutron_pt_domain",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_domain'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE DOMAIN pt_domain.ld AS int; CREATE TABLE pt_domain.lt (c pt_domain.ld);`},
					drop:  "DROP DOMAIN pt_domain.ld CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='lt'`, "0"}},
				},
			},
			"schema": {
				plant: []string{
					`CREATE SCHEMA pt_schema; CREATE COLLATION pt_schema.c (provider = icu, locale = 'und');`,
					`CREATE TABLE _neutron_pt_schema (t text COLLATE pt_schema.c);`,
				},
				drop: "DROP SCHEMA pt_schema CASCADE;", named: "_neutron_pt_schema",
				probes: [][2]string{
					{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_schema'`, "1"},
					{`SELECT count(*) FROM information_schema.schemata WHERE schema_name='pt_schema'`, "1"},
				},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE SCHEMA pt_schema_l; CREATE TABLE pt_schema_l.t (i int);`},
					drop:  "DROP SCHEMA pt_schema_l CASCADE;", ack: true,
					check: [][2]string{{`SELECT count(*) FROM information_schema.schemata WHERE schema_name='pt_schema_l'`, "0"}},
				},
			},
			"extension": {
				// The extension's members ARE the protected fixture;
				// no stock extension installs member-free, so the legal
				// direction is unverifiable against stock PostgreSQL
				// (review-6 recorded the same limit).
				plant: []string{`CREATE EXTENSION cube`},
				drop:  "DROP EXTENSION cube CASCADE;", named: "cube",
				probes:        [][2]string{{`SELECT count(*) FROM pg_extension WHERE extname='cube'`, "1"}},
				preM05Applies: true,
				legal:         nil,
			},
			"function": {
				plant: []string{
					`CREATE SCHEMA pt_fn; CREATE TABLE pt_fn.t (i int);`,
					`CREATE FUNCTION pt_fn.f() RETURNS int LANGUAGE sql RETURN 7;`,
					`CREATE VIEW _neutron_pt_fn_v AS SELECT pt_fn.f() AS r;`,
				},
				drop: "DROP FUNCTION pt_fn.f() CASCADE;", named: "_neutron_pt_fn_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_fn_v'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE FUNCTION pt_fn.lf() RETURNS int LANGUAGE sql RETURN 7; CREATE VIEW pt_fn.lv AS SELECT pt_fn.lf() AS r;`},
					drop:  "DROP FUNCTION pt_fn.lf() CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='lv'`, "0"}},
				},
			},
			"procedure": {
				// pg_proc is seeded by proname, prokind-agnostic (the
				// routine-family matching rule): the guard refuses even
				// though the server would itself reject a DROP
				// PROCEDURE spelled over a function.
				plant: []string{
					`CREATE SCHEMA pt_proc;`,
					`CREATE FUNCTION pt_proc.f() RETURNS int LANGUAGE sql RETURN 7;`,
					`CREATE VIEW _neutron_pt_proc_v AS SELECT pt_proc.f() AS r;`,
				},
				drop: "DROP PROCEDURE pt_proc.f() CASCADE;", named: "_neutron_pt_proc_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_proc_v'`, "1"}},
				preM05Applies: false,
				legal: &legalSpec{
					plant: []string{`CREATE PROCEDURE pt_proc.lp() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;`},
					drop:  "DROP PROCEDURE pt_proc.lp() CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_proc WHERE proname='lp'`, "0"}},
				},
			},
			"routine": {
				plant: []string{
					`CREATE SCHEMA pt_routine;`,
					`CREATE FUNCTION pt_routine.f() RETURNS int LANGUAGE sql RETURN 7;`,
					`CREATE VIEW _neutron_pt_routine_v AS SELECT pt_routine.f() AS r;`,
				},
				drop: "DROP ROUTINE pt_routine.f() CASCADE;", named: "_neutron_pt_routine_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_routine_v'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE FUNCTION pt_routine.lf() RETURNS int LANGUAGE sql RETURN 7; CREATE VIEW pt_routine.lv AS SELECT pt_routine.lf() AS r;`},
					drop:  "DROP ROUTINE pt_routine.lf() CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='lv'`, "0"}},
				},
			},
			"aggregate": {
				plant: []string{
					`CREATE SCHEMA pt_agg; CREATE TABLE pt_agg.t (i int);`,
					`CREATE AGGREGATE pt_agg.a (int) (SFUNC = int4_sum, STYPE = int8, INITCOND = '0');`,
					`CREATE VIEW _neutron_pt_agg_v AS SELECT pt_agg.a(i) AS s FROM pt_agg.t;`,
				},
				drop: "DROP AGGREGATE pt_agg.a (int) CASCADE;", named: "_neutron_pt_agg_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_agg_v'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE AGGREGATE pt_agg.la (int) (SFUNC = int4_sum, STYPE = int8, INITCOND = '0'); CREATE VIEW pt_agg.lv AS SELECT pt_agg.la(i) AS s FROM pt_agg.t;`},
					drop:  "DROP AGGREGATE pt_agg.la (int) CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='lv'`, "0"}},
				},
			},
			"collation": {
				plant: []string{
					`CREATE SCHEMA pt_col; CREATE COLLATION pt_col.c (provider = icu, locale = 'und');`,
					`CREATE TABLE _neutron_pt_col (t text COLLATE pt_col.c);`,
				},
				drop: "DROP COLLATION pt_col.c CASCADE;", named: "_neutron_pt_col",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_col'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE COLLATION pt_col.lc (provider = icu, locale = 'und'); CREATE TABLE pt_col.lt (t text COLLATE pt_col.lc);`},
					drop:  "DROP COLLATION pt_col.lc CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='lt'`, "0"}},
				},
			},
			"conversion": {
				vacuous:       "nothing in PostgreSQL 17 records a pg_depend reference INTO a pg_conversion, so no dependent chain terminating in a protected fixture is constructible; the closure runs and must not over-refuse",
				plant:         []string{`CREATE SCHEMA pt_conv; CREATE CONVERSION pt_conv.c FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8; CREATE TABLE _neutron_pt_conv (i int);`},
				drop:          "DROP CONVERSION pt_conv.c CASCADE;",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_conv'`, "1"}},
				preM05Applies: true,
				legal:         nil,
			},
			"policy": {
				plant: []string{
					`CREATE TABLE _neutron_pt_policy (i int);`,
					`CREATE POLICY pt_policy_p ON _neutron_pt_policy USING (true);`,
				},
				drop: "DROP POLICY pt_policy_p ON _neutron_pt_policy CASCADE;", named: "_neutron_pt_policy",
				probes:        [][2]string{{`SELECT count(*) FROM pg_policy WHERE polname='pt_policy_p'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TABLE pt_policy_t (i int); CREATE POLICY pt_policy_lp ON pt_policy_t USING (true);`},
					drop:  "DROP POLICY pt_policy_lp ON pt_policy_t CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_policy WHERE polname='pt_policy_lp'`, "0"}},
				},
			},
			"trigger": {
				plant: []string{
					`CREATE SCHEMA pt_trg; CREATE FUNCTION pt_trg.f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;`,
					`CREATE TABLE _neutron_pt_trigger (i int);`,
					`CREATE TRIGGER pt_trigger_t BEFORE UPDATE ON _neutron_pt_trigger FOR EACH ROW EXECUTE FUNCTION pt_trg.f();`,
				},
				drop: "DROP TRIGGER pt_trigger_t ON _neutron_pt_trigger CASCADE;", named: "_neutron_pt_trigger",
				probes:        [][2]string{{`SELECT count(*) FROM pg_trigger WHERE tgname='pt_trigger_t' AND NOT tgisinternal`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TABLE pt_trigger_lt (i int); CREATE TRIGGER pt_trigger_lt2 BEFORE UPDATE ON pt_trigger_lt FOR EACH ROW EXECUTE FUNCTION pt_trg.f();`},
					drop:  "DROP TRIGGER pt_trigger_lt2 ON pt_trigger_lt CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_trigger WHERE tgname='pt_trigger_lt2'`, "0"}},
				},
			},
			"rule": {
				plant: []string{
					`CREATE TABLE _neutron_pt_rule (i int);`,
					`CREATE RULE pt_rule_r AS ON INSERT TO _neutron_pt_rule DO INSTEAD NOTHING;`,
				},
				drop: "DROP RULE pt_rule_r ON _neutron_pt_rule CASCADE;", named: "_neutron_pt_rule",
				probes:        [][2]string{{`SELECT count(*) FROM pg_rewrite WHERE rulename='pt_rule_r'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TABLE pt_rule_lt (i int); CREATE RULE pt_rule_lr AS ON INSERT TO pt_rule_lt DO INSTEAD NOTHING;`},
					drop:  "DROP RULE pt_rule_lr ON pt_rule_lt CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_rewrite WHERE rulename='pt_rule_lr'`, "0"}},
				},
			},
			"statistics": {
				vacuous:       "nothing in PostgreSQL 17 records a pg_depend reference INTO a pg_statistic_ext, so no dependent chain terminating in a protected fixture is constructible; the closure runs and must not over-refuse",
				plant:         []string{`CREATE SCHEMA pt_stats; CREATE TABLE pt_stats.t (a int, b int); CREATE STATISTICS pt_stats.st ON a, b FROM pt_stats.t; CREATE TABLE _neutron_pt_stats (i int);`},
				drop:          "DROP STATISTICS pt_stats.st CASCADE;",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_stats'`, "1"}},
				preM05Applies: true,
				legal:         nil,
			},
			"operator": {
				plant: []string{
					`CREATE SCHEMA pt_op; CREATE TABLE pt_op.t (i int);`,
					`CREATE OPERATOR pt_op.=== (LEFTARG = int, RIGHTARG = int, PROCEDURE = int4eq);`,
					`CREATE VIEW _neutron_pt_op_v AS SELECT i OPERATOR(pt_op.===) 1 AS r FROM pt_op.t;`,
				},
				drop: "DROP OPERATOR pt_op.===(int, int) CASCADE;", named: "_neutron_pt_op_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_op_v'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE OPERATOR pt_op.!== (LEFTARG = int, RIGHTARG = int, PROCEDURE = int4ne); CREATE VIEW pt_op.lv AS SELECT i OPERATOR(pt_op.!==) 1 AS r FROM pt_op.t;`},
					drop:  "DROP OPERATOR pt_op.!== (int, int) CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='lv'`, "0"}},
				},
			},
			"foreign-table": {
				plant: []string{
					`CREATE FOREIGN DATA WRAPPER pt_fdw`,
					`CREATE SERVER pt_srv FOREIGN DATA WRAPPER pt_fdw`,
					`CREATE SCHEMA pt_ft; CREATE FOREIGN TABLE pt_ft.src (i int) SERVER pt_srv;`,
					`CREATE VIEW _neutron_pt_ft_v AS SELECT i FROM pt_ft.src;`,
				},
				drop: "DROP FOREIGN TABLE pt_ft.src CASCADE;", named: "_neutron_pt_ft_v",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='_neutron_pt_ft_v'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE FOREIGN TABLE pt_ft.lt (i int) SERVER pt_srv; CREATE VIEW pt_ft.lv AS SELECT i FROM pt_ft.lt;`},
					drop:  "DROP FOREIGN TABLE pt_ft.lt CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='lv'`, "0"}},
				},
			},
			"operator-class": {
				plant: []string{
					`CREATE SCHEMA pt_oc;`,
					`CREATE OPERATOR FAMILY pt_oc.fam USING btree;` + `CREATE OPERATOR CLASS pt_oc.oc FOR TYPE int USING btree FAMILY pt_oc.fam AS OPERATOR 1 <, OPERATOR 3 =, FUNCTION 1 btint4cmp(int4,int4);`,
					`CREATE TABLE _neutron_pt_oc (i int);`,
					`CREATE INDEX pt_oc_ix ON _neutron_pt_oc USING btree (i pt_oc.oc);`,
				},
				drop: "DROP OPERATOR CLASS pt_oc.oc USING btree CASCADE;", named: "pt_oc_ix",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='pt_oc_ix'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TABLE pt_oc.lt (i int); CREATE OPERATOR FAMILY pt_oc.lfam USING btree; CREATE OPERATOR CLASS pt_oc.loc FOR TYPE int USING btree FAMILY pt_oc.lfam AS OPERATOR 1 <, OPERATOR 3 =, FUNCTION 1 btint4cmp(int4,int4); CREATE INDEX pt_oc_li ON pt_oc.lt USING btree (i pt_oc.loc);`},
					drop:  "DROP OPERATOR CLASS pt_oc.loc USING btree CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='pt_oc_li'`, "0"}},
				},
			},
			"operator-family": {
				plant: []string{
					`CREATE SCHEMA pt_of;`,
					`CREATE OPERATOR FAMILY pt_of.fam USING btree;`,
					`CREATE OPERATOR CLASS pt_of.oc FOR TYPE int USING btree FAMILY pt_of.fam AS OPERATOR 1 <, OPERATOR 3 =, FUNCTION 1 btint4cmp(int4,int4);`,
					`CREATE TABLE _neutron_pt_of (i int);`,
					`CREATE INDEX pt_of_ix ON _neutron_pt_of USING btree (i pt_of.oc);`,
				},
				drop: "DROP OPERATOR FAMILY pt_of.fam USING btree CASCADE;", named: "pt_of_ix",
				probes:        [][2]string{{`SELECT count(*) FROM pg_class WHERE relname='pt_of_ix'`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE OPERATOR FAMILY pt_of.lfam USING btree;`},
					drop:  "DROP OPERATOR FAMILY pt_of.lfam USING btree CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM pg_opfamily WHERE opfname='lfam'`, "0"}},
				},
			},
			"text-search-config": {
				plant: []string{
					`CREATE SCHEMA pt_tsc; CREATE TEXT SEARCH CONFIGURATION pt_tsc.c (COPY = english);`,
					`CREATE TABLE _neutron_pt_tsc (v tsvector DEFAULT to_tsvector('pt_tsc.c'::regconfig, 'x'));`,
				},
				drop: "DROP TEXT SEARCH CONFIGURATION pt_tsc.c CASCADE;", named: "_neutron_pt_tsc",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_tsc' AND column_default IS NOT NULL`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TEXT SEARCH CONFIGURATION pt_tsc.lc (COPY = english); CREATE TABLE pt_tsc.lt (v tsvector DEFAULT to_tsvector('pt_tsc.lc'::regconfig, 'x'));`},
					drop:  "DROP TEXT SEARCH CONFIGURATION pt_tsc.lc CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='lt' AND column_default IS NOT NULL`, "0"}},
				},
			},
			"text-search-dictionary": {
				plant: []string{
					`CREATE SCHEMA pt_tsd;`,
					`CREATE TEXT SEARCH DICTIONARY pt_tsd.d (TEMPLATE = snowball, Language = 'english');`,
					`CREATE TEXT SEARCH CONFIGURATION pt_tsd.c (COPY = english);`,
					`ALTER TEXT SEARCH CONFIGURATION pt_tsd.c ALTER MAPPING FOR word WITH pt_tsd.d;`,
					`CREATE TABLE _neutron_pt_tsd (v tsvector DEFAULT to_tsvector('pt_tsd.c'::regconfig, 'x'));`,
				},
				drop: "DROP TEXT SEARCH DICTIONARY pt_tsd.d CASCADE;", named: "_neutron_pt_tsd",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_tsd' AND column_default IS NOT NULL`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TEXT SEARCH DICTIONARY pt_tsd.ld (TEMPLATE = snowball, Language = 'english'); CREATE TEXT SEARCH CONFIGURATION pt_tsd.lc (COPY = english); ALTER TEXT SEARCH CONFIGURATION pt_tsd.lc ALTER MAPPING FOR word WITH pt_tsd.ld; CREATE TABLE pt_tsd.lt (v tsvector DEFAULT to_tsvector('pt_tsd.lc'::regconfig, 'x'));`},
					drop:  "DROP TEXT SEARCH DICTIONARY pt_tsd.ld CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='lt' AND column_default IS NOT NULL`, "0"}},
				},
			},
			"text-search-parser": {
				plant: []string{
					`CREATE SCHEMA pt_tsp;`,
					`CREATE TEXT SEARCH PARSER pt_tsp.p (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);`,
					`CREATE TEXT SEARCH CONFIGURATION pt_tsp.c (PARSER = pt_tsp.p);`,
					`ALTER TEXT SEARCH CONFIGURATION pt_tsp.c ADD MAPPING FOR word WITH english_stem;`,
					`CREATE TABLE _neutron_pt_tsp (v tsvector DEFAULT to_tsvector('pt_tsp.c'::regconfig, 'x'));`,
				},
				drop: "DROP TEXT SEARCH PARSER pt_tsp.p CASCADE;", named: "_neutron_pt_tsp",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_tsp' AND column_default IS NOT NULL`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TEXT SEARCH PARSER pt_tsp.lp (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype); CREATE TEXT SEARCH CONFIGURATION pt_tsp.lc (PARSER = pt_tsp.lp); ALTER TEXT SEARCH CONFIGURATION pt_tsp.lc ADD MAPPING FOR word WITH english_stem; CREATE TABLE pt_tsp.lt (v tsvector DEFAULT to_tsvector('pt_tsp.lc'::regconfig, 'x'));`},
					drop:  "DROP TEXT SEARCH PARSER pt_tsp.lp CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='lt' AND column_default IS NOT NULL`, "0"}},
				},
			},
			"text-search-template": {
				plant: []string{
					`CREATE SCHEMA pt_tt;`,
					`CREATE TEXT SEARCH TEMPLATE pt_tt.t (INIT = dsimple_init, LEXIZE = dsimple_lexize);`,
					`CREATE TEXT SEARCH DICTIONARY pt_tt.d (TEMPLATE = pt_tt.t);`,
					`CREATE TEXT SEARCH CONFIGURATION pt_tt.c (COPY = english);`,
					`ALTER TEXT SEARCH CONFIGURATION pt_tt.c ALTER MAPPING FOR word WITH pt_tt.d;`,
					`CREATE TABLE _neutron_pt_tt (v tsvector DEFAULT to_tsvector('pt_tt.c'::regconfig, 'x'));`,
				},
				drop: "DROP TEXT SEARCH TEMPLATE pt_tt.t CASCADE;", named: "_neutron_pt_tt",
				probes:        [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_pt_tt' AND column_default IS NOT NULL`, "1"}},
				preM05Applies: true,
				legal: &legalSpec{
					plant: []string{`CREATE TEXT SEARCH TEMPLATE pt_tt.lt (INIT = dsimple_init, LEXIZE = dsimple_lexize); CREATE TEXT SEARCH DICTIONARY pt_tt.ld (TEMPLATE = pt_tt.lt); CREATE TEXT SEARCH CONFIGURATION pt_tt.lc (COPY = english); ALTER TEXT SEARCH CONFIGURATION pt_tt.lc ALTER MAPPING FOR word WITH pt_tt.ld; CREATE TABLE pt_tt.lt2 (v tsvector DEFAULT to_tsvector('pt_tt.lc'::regconfig, 'x'));`},
					drop:  "DROP TEXT SEARCH TEMPLATE pt_tt.lt CASCADE;",
					check: [][2]string{{`SELECT count(*) FROM information_schema.columns WHERE table_name='lt2' AND column_default IS NOT NULL`, "0"}},
				},
			},
		}
		for _, kind := range db.AllowlistedDropKinds() {
			kind := kind
			fix, ok := fixtures[kind]
			if !ok {
				t.Errorf("no property-table fixture for allowlisted drop kind %q — every allowlisted DROP kind needs a protected-direction fixture (or a documented vacuous entry)", kind)
				continue
			}
			t.Run(kind, func(t *testing.T) {
				dbURL := newDB(t)
				if kind == "foreign-table" {
					if got := query(t, dbURL, `SELECT rolsuper::text FROM pg_roles WHERE rolname = current_user`); got != "true" {
						t.Skipf("foreign-table fixture needs CREATE FOREIGN DATA WRAPPER (superuser); refusing to assert blindly")
					}
				}
				for _, stmt := range fix.plant {
					execRaw(t, dbURL, stmt)
				}
				dir := writeMigrations(t, map[string]string{
					"001_pt.up.sql":   fix.drop,
					"001_pt.down.sql": "SELECT 1;",
				})
				// Fail-before: the pre-M05 binary carries no
				// guard; for constructible chains it applies the drop
				// and destroys the victim (premise carried forever).
				code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir)
				if fix.preM05Applies && code != 0 {
					t.Fatalf("pre-M05 binary must apply the %s drop (fail-before): %s", kind, out)
				}
				if !fix.preM05Applies && code == 0 {
					t.Fatalf("pre-M05 binary unexpectedly applied the %s drop (fixture premise wrong — the server should refuse it too)", kind)
				}
				execRaw(t, dbURL, `DROP TABLE IF EXISTS _neutron_migrations`)
				// Whatever the pre-M05 run destroyed is replanted on a
				// fresh database for the guarded assertions.
				dbGuarded := newDB(t)
				for _, stmt := range fix.plant {
					execRaw(t, dbGuarded, stmt)
				}
				dirG := writeMigrations(t, map[string]string{
					"001_pt.up.sql":   fix.drop,
					"001_pt.down.sql": "SELECT 1;",
				})
				if fix.vacuous != "" {
					// No constructible dependent edge exists: the
					// closure must not over-refuse the legal drop.
					if code, out := runCLI(t, dbGuarded, "migrate", "--dir", dirG); code != 0 {
						t.Fatalf("vacuous %s kind over-refused (%s): %s", kind, fix.vacuous, out)
					}
					for _, probe := range fix.probes {
						if got := query(t, dbGuarded, probe[0]); got != probe[1] {
							t.Fatalf("bystander must be intact after the applied %s drop (%s), got %s", kind, probe[0], got)
						}
					}
					return
				}
				for _, flags := range [][]string{{}, {"--allow-destructive"}} {
					if code, out := runCLI(t, dbGuarded, append([]string{"migrate", "--dir", dirG}, flags...)...); code == 0 {
						t.Fatalf("%s CASCADE onto a protected victim accepted (flags=%v)", kind, flags)
					} else if fix.serverGuard == "" && (!strings.Contains(out, "protected") || !strings.Contains(out, fix.named)) {
						t.Fatalf("refusal must name protection and the victim %q (flags=%v): %s", fix.named, flags, out)
					}
				}
				for _, probe := range fix.probes {
					if got := query(t, dbGuarded, probe[0]); got != probe[1] {
						t.Fatalf("victim must survive the refused %s cascade (%s), got %s", kind, probe[0], got)
					}
				}
				if got := query(t, dbGuarded, `SELECT CASE WHEN to_regclass('public._neutron_migrations') IS NULL THEN 'none' ELSE (SELECT count(*)::text FROM _neutron_migrations) END`); got != "none" && got != "0" {
					t.Fatalf("atomic refusal: no history may exist, got %s", got)
				}
				// Legal direction: same chain shape with ordinary user
				// victims applies, with ack semantics for the
				// destructive-vocabulary kinds.
				if fix.legal != nil {
					for _, stmt := range fix.legal.plant {
						execRaw(t, dbGuarded, stmt)
					}
					dirL := writeMigrations(t, map[string]string{
						"001_legal.up.sql":   fix.legal.drop,
						"001_legal.down.sql": "SELECT 1;",
					})
					if fix.legal.ack {
						if code, out := runCLI(t, dbGuarded, "migrate", "--dir", dirL); code == 0 || !strings.Contains(out, "--allow-destructive") {
							t.Fatalf("legal %s drop must demand acknowledgement without the flag: rc=%d %s", kind, code, out)
						}
					} else if code, out := runCLI(t, dbGuarded, "migrate", "--dir", dirL); code != 0 {
						t.Fatalf("legal %s drop must apply without the flag (outside the ack vocabulary): %s", kind, out)
					}
					if code, out := runCLI(t, dbGuarded, "migrate", "--dir", dirL, "--allow-destructive"); code != 0 {
						t.Fatalf("legal %s drop must apply with the flag: %s", kind, out)
					}
					for _, probe := range fix.legal.check {
						if got := query(t, dbGuarded, probe[0]); got != probe[1] {
							t.Fatalf("legal %s drop effect wrong (%s), got %s", kind, probe[0], got)
						}
					}
				}
			})
		}
	})

	// ------------------------------------------------------------------
	// AA. Review-7 MAJOR-1: the demonstrated name-class ALTER escapes —
	//     ALTER OPERATOR CLASS/FAMILY ... RENAME and ALTER OPERATOR
	//     ... SET SCHEMA over cube's own members applied rc 0 on the
	//     attempt-7 binary — reproduced fail-before on the pre-M05 binary
	//     binary (PostgreSQL accepts member renames; it refuses only
	//     member drops, 2BP01), then refused under both flag variants
	//     with members intact. The member-DROP shapes (plain and
	//     CASCADE) are pinned guard-side, ahead of the server's own
	//     2BP01, so a future PostgreSQL behavior change cannot reopen
	//     them silently; the pre-M05 binary already fails those through
	//     the server. Control: the same SET SCHEMA spelling over a
	//     USER-created operator stays legal.
	// ------------------------------------------------------------------
	t.Run("Review7NameClassAlterVectorsRefused", func(t *testing.T) {
		vectors := []struct {
			name          string
			plant         []string
			stmt          string
			preM05Applies bool // the pre-M05 binary applies it (the server accepts)
			probes        [][2]string
		}{
			{
				name:  "OperatorClassMemberRename",
				plant: []string{`CREATE EXTENSION cube`},
				stmt:  "ALTER OPERATOR CLASS public.cube_ops USING btree RENAME TO cube_ops2;",
				probes: [][2]string{
					{`SELECT count(*) FROM pg_opclass WHERE opcname='cube_ops'`, "1"},
					{`SELECT count(*) FROM pg_opclass WHERE opcname='cube_ops2'`, "0"},
				},
				preM05Applies: true,
			},
			{
				name:  "OperatorFamilyMemberRename",
				plant: []string{`CREATE EXTENSION cube`},
				stmt:  "ALTER OPERATOR FAMILY public.cube_ops USING btree RENAME TO cube_fam2;",
				probes: [][2]string{
					{`SELECT count(*) FROM pg_opfamily WHERE opfname='cube_ops'`, "1"},
					{`SELECT count(*) FROM pg_opfamily WHERE opfname='cube_fam2'`, "0"},
				},
				preM05Applies: true,
			},
			{
				name:  "OperatorMemberSetSchema",
				plant: []string{`CREATE EXTENSION cube`, `CREATE SCHEMA m05opdst`},
				stmt:  "ALTER OPERATOR public.<>(cube,cube) SET SCHEMA m05opdst;",
				probes: [][2]string{
					{`SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid=o.oprnamespace JOIN pg_depend d ON d.classid='pg_operator'::regclass AND d.objid=o.oid WHERE d.deptype='e' AND o.oprname='<>' AND n.nspname='public'`, "1"},
				},
				preM05Applies: true,
			},
			{
				name:  "OperatorMemberDropPlainPinnedServerSide",
				plant: []string{`CREATE EXTENSION cube`},
				stmt:  "DROP OPERATOR public.<>(cube,cube);",
				probes: [][2]string{
					{`SELECT count(*) FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE e.extname='cube' AND d.deptype='e' AND d.classid='pg_operator'::regclass`, "14"},
				},
				preM05Applies: false, // PostgreSQL itself refuses (2BP01)
			},
			{
				name:  "OperatorMemberDropCascadePinnedServerSide",
				plant: []string{`CREATE EXTENSION cube`},
				stmt:  "DROP OPERATOR public.<>(cube,cube) CASCADE;",
				probes: [][2]string{
					{`SELECT count(*) FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE e.extname='cube' AND d.deptype='e' AND d.classid='pg_operator'::regclass`, "14"},
				},
				preM05Applies: false, // PostgreSQL itself refuses (2BP01)
			},
		}
		for _, vec := range vectors {
			t.Run(vec.name, func(t *testing.T) {
				dbURL := newDB(t)
				for _, stmt := range vec.plant {
					execRaw(t, dbURL, stmt)
				}
				dir := writeMigrations(t, map[string]string{
					"001_vec.up.sql":   vec.stmt,
					"001_vec.down.sql": "SELECT 1;",
				})
				// Fail-before (member ALTERs) / premise (member DROPs):
				// the pre-M05 pre-M05 binary carries no guard. PostgreSQL
				// accepts member ALTERs, so they apply and mutate the
				// member; member DROPs it refuses itself (2BP01).
				code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir)
				if vec.preM05Applies && code != 0 {
					t.Fatalf("pre-M05 binary must apply the member ALTER (fail-before — PostgreSQL accepts member renames): %s", out)
				}
				if !vec.preM05Applies && code == 0 {
					t.Fatal("pre-M05 binary unexpectedly applied the member drop (fixture premise wrong — the server should refuse it too)")
				}
				if vec.preM05Applies {
					for _, probe := range vec.probes {
						if got := query(t, dbURL, probe[0]); got == probe[1] {
							t.Fatalf("fail-before premise wrong: member intact after the pre-M05-binary run (%s)", probe[0])
						}
					}
				}
				// Guarded assertions on a freshly planted database.
				dbGuarded := newDB(t)
				for _, stmt := range vec.plant {
					execRaw(t, dbGuarded, stmt)
				}
				dirG := writeMigrations(t, map[string]string{
					"001_vec.up.sql":   vec.stmt,
					"001_vec.down.sql": "SELECT 1;",
				})
				for _, flags := range [][]string{{}, {"--allow-destructive"}} {
					if code, out := runCLI(t, dbGuarded, append([]string{"migrate", "--dir", dirG}, flags...)...); code == 0 {
						t.Fatalf("member statement accepted (flags=%v): %s", flags, out)
					} else if !strings.Contains(out, "protected") || !strings.Contains(out, "owned by extension cube") {
						t.Fatalf("refusal must name protection and the owning extension (flags=%v): %s", flags, out)
					}
				}
				for _, probe := range vec.probes {
					if got := query(t, dbGuarded, probe[0]); got != probe[1] {
						t.Fatalf("member must survive the refused statement (%s), got %s", probe[0], got)
					}
				}
				if got := query(t, dbGuarded, `SELECT CASE WHEN to_regclass('public._neutron_migrations') IS NULL THEN 'none' ELSE (SELECT count(*)::text FROM _neutron_migrations) END`); got != "none" && got != "0" {
					t.Fatalf("atomic refusal: no history may exist, got %s", got)
				}
			})
		}

		// Control: the same SET SCHEMA spelling over a USER-created
		// operator applies ungated — the membership arm may not
		// over-refuse ordinary operators (review-7's no-regression
		// condition; cube stays installed in the same database).
		dbU := newDB(t)
		execRaw(t, dbU, `CREATE EXTENSION cube`)
		execRaw(t, dbU, `CREATE SCHEMA m05uop; CREATE SCHEMA m05uop_dst; CREATE OPERATOR m05uop.=== (LEFTARG = int, RIGHTARG = int, PROCEDURE = int4ne);`)
		dirU := writeMigrations(t, map[string]string{
			"001_user_op.up.sql":   "ALTER OPERATOR m05uop.=== (int, int) SET SCHEMA m05uop_dst;",
			"001_user_op.down.sql": "SELECT 1;",
		})
		if code, out := runCLI(t, dbU, "migrate", "--dir", dirU); code != 0 {
			t.Fatalf("user-operator SET SCHEMA must stay legal with the extension installed: %s", out)
		}
		if got := query(t, dbU, `SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid=o.oprnamespace WHERE o.oprname='===' AND n.nspname='m05uop_dst'`); got != "1" {
			t.Fatalf("user operator must be relocated, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// AB. Review-7 MAJOR-1: the name-class ALTER property table — the
	//     ALTER-side companion of the CASCADE property table. EVERY
	//     allowlisted ALTER kind of the "name" guard target class is
	//     enumerated from db.AllowlistedAlterKinds() +
	//     db.GuardTargetClass (the same runtime source the guard
	//     dispatches on); each carries an extension-member fixture
	//     (cube's stock members, or a member planted through
	//     ALTER EXTENSION cube ADD), a fail-before run on the pre-M05
	//     pre-M05 binary proving PostgreSQL accepts the member ALTER,
	//     refusal under BOTH flag variants with the member intact and
	//     nothing applied, and the LEGAL direction — the same ALTER
	//     over a user object applies. Kinds for which PostgreSQL 17
	//     cannot construct an extension member at all assert the
	//     vacuous direction: the membership arm runs and does not
	//     over-refuse the user ALTER. A name-class kind added to the
	//     alter allowlist without a fixture fails here and in
	//     TestNameClassMembershipCoversEveryAllowlistedAlterKind.
	// ------------------------------------------------------------------
	t.Run("NameClassAlterPropertyTable", func(t *testing.T) {
		type legalAlter struct {
			plant []string
			alter string
			check [][2]string
		}
		type alterFixture struct {
			plant   []string
			alter   string
			probes  [][2]string
			vacuous string // non-empty: no member is constructible; assert no false refusal
			legal   *legalAlter
		}
		fixtures := map[string]alterFixture{
			"schema": {
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE SCHEMA m05pts; ALTER EXTENSION cube ADD SCHEMA m05pts;`,
				},
				alter:  "ALTER SCHEMA m05pts RENAME TO m05pts2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_namespace WHERE nspname='m05pts'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE SCHEMA m05us`},
					alter: "ALTER SCHEMA m05us RENAME TO m05us2;",
					check: [][2]string{{`SELECT count(*) FROM pg_namespace WHERE nspname='m05us2'`, "1"}},
				},
			},
			"operator": {
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE SCHEMA m05pt_op_dst`,
				},
				alter: "ALTER OPERATOR public.<>(cube,cube) SET SCHEMA m05pt_op_dst;",
				probes: [][2]string{
					{`SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid=o.oprnamespace JOIN pg_depend d ON d.classid='pg_operator'::regclass AND d.objid=o.oid WHERE d.deptype='e' AND o.oprname='<>' AND n.nspname='public'`, "1"},
				},
				legal: &legalAlter{
					plant: []string{`CREATE SCHEMA m05pt_op; CREATE SCHEMA m05pt_op_dst2; CREATE OPERATOR m05pt_op.=== (LEFTARG = int, RIGHTARG = int, PROCEDURE = int4ne);`},
					alter: "ALTER OPERATOR m05pt_op.=== (int, int) SET SCHEMA m05pt_op_dst2;",
					check: [][2]string{{`SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid=o.oprnamespace WHERE o.oprname='===' AND n.nspname='m05pt_op_dst2'`, "1"}},
				},
			},
			"operator-class": {
				plant: []string{
					`CREATE EXTENSION cube`,
				},
				alter:  "ALTER OPERATOR CLASS public.cube_ops USING btree RENAME TO m05pt_oc2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_opclass WHERE opcname='cube_ops'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE SCHEMA m05pt_oc; CREATE OPERATOR FAMILY m05pt_oc.f USING btree; CREATE OPERATOR CLASS m05pt_oc.c FOR TYPE int USING btree FAMILY m05pt_oc.f AS OPERATOR 1 <, OPERATOR 3 =, FUNCTION 1 btint4cmp(int4,int4);`},
					alter: "ALTER OPERATOR CLASS m05pt_oc.c USING btree RENAME TO m05pt_oc2;",
					check: [][2]string{{`SELECT count(*) FROM pg_opclass WHERE opcname='m05pt_oc2'`, "1"}},
				},
			},
			"operator-family": {
				plant: []string{
					`CREATE EXTENSION cube`,
				},
				alter:  "ALTER OPERATOR FAMILY public.cube_ops USING btree RENAME TO m05pt_of2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_opfamily WHERE opfname='cube_ops'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE SCHEMA m05pt_of; CREATE OPERATOR FAMILY m05pt_of.f USING btree;`},
					alter: "ALTER OPERATOR FAMILY m05pt_of.f USING btree RENAME TO m05pt_of2;",
					check: [][2]string{{`SELECT count(*) FROM pg_opfamily WHERE opfname='m05pt_of2'`, "1"}},
				},
			},
			"collation": {
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE COLLATION m05pt_col (provider = icu, locale = 'und'); ALTER EXTENSION cube ADD COLLATION m05pt_col;`,
				},
				alter:  "ALTER COLLATION public.m05pt_col RENAME TO m05pt_col2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_collation WHERE collname='m05pt_col'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE COLLATION m05pt_lcol (provider = icu, locale = 'und');`},
					alter: "ALTER COLLATION public.m05pt_lcol RENAME TO m05pt_lcol2;",
					check: [][2]string{{`SELECT count(*) FROM pg_collation WHERE collname='m05pt_lcol2'`, "1"}},
				},
			},
			"statistics": {
				vacuous: "PostgreSQL 17 cannot make a statistics object an extension member: ALTER EXTENSION ... ADD STATISTICS is refused (cannot add an object of this type to an extension) and pg_statistic_ext rows created inside an extension script record no 'e' membership either; the membership arm runs and must not over-refuse",
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE TABLE m05pt_st (a int, b int); CREATE STATISTICS m05pt_lst ON a, b FROM m05pt_st;`,
				},
				alter:  "ALTER STATISTICS public.m05pt_lst RENAME TO m05pt_lst2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_statistic_ext WHERE stxname='m05pt_lst2'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE STATISTICS m05pt_lst3 ON a, b FROM m05pt_st;`},
					alter: "ALTER STATISTICS public.m05pt_lst3 RENAME TO m05pt_lst4;",
					check: [][2]string{{`SELECT count(*) FROM pg_statistic_ext WHERE stxname='m05pt_lst4'`, "1"}},
				},
			},
			"policy": {
				vacuous: "PostgreSQL 17 cannot make a policy an extension member: ALTER EXTENSION ADD has no POLICY production and pg_policy rows created inside an extension script record no 'e' membership; the membership arm runs and must not over-refuse (the ON-table arm still guards protected tables)",
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE TABLE m05pt_ptab (i int); CREATE POLICY m05pt_lpol ON m05pt_ptab USING (true);`,
				},
				alter:  "ALTER POLICY m05pt_lpol ON public.m05pt_ptab RENAME TO m05pt_lpol2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_policy WHERE polname='m05pt_lpol2'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE POLICY m05pt_lpol3 ON m05pt_ptab USING (true);`},
					alter: "ALTER POLICY m05pt_lpol3 ON public.m05pt_ptab RENAME TO m05pt_lpol4;",
					check: [][2]string{{`SELECT count(*) FROM pg_policy WHERE polname='m05pt_lpol4'`, "1"}},
				},
			},
			"trigger": {
				vacuous: "PostgreSQL 17 cannot make a trigger an extension member: ALTER EXTENSION ADD has no TRIGGER production and pg_trigger rows created inside an extension script record no 'e' membership; the membership arm runs and must not over-refuse (the ON-table arm still guards protected tables)",
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE FUNCTION m05pt_tf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;`,
					`CREATE TABLE m05pt_ttab (i int); CREATE TRIGGER m05pt_ltrg BEFORE UPDATE ON m05pt_ttab FOR EACH ROW EXECUTE FUNCTION m05pt_tf();`,
				},
				alter:  "ALTER TRIGGER m05pt_ltrg ON public.m05pt_ttab RENAME TO m05pt_ltrg2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_trigger WHERE tgname='m05pt_ltrg2'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE TRIGGER m05pt_ltrg3 BEFORE UPDATE ON m05pt_ttab FOR EACH ROW EXECUTE FUNCTION m05pt_tf();`},
					alter: "ALTER TRIGGER m05pt_ltrg3 ON public.m05pt_ttab RENAME TO m05pt_ltrg4;",
					check: [][2]string{{`SELECT count(*) FROM pg_trigger WHERE tgname='m05pt_ltrg4'`, "1"}},
				},
			},
			"rule": {
				vacuous: "PostgreSQL 17 cannot make a rule an extension member: ALTER EXTENSION ADD has no RULE production and pg_rewrite rows created inside an extension script record no 'e' membership; the membership arm runs and must not over-refuse (the ON-table arm still guards protected tables)",
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE TABLE m05pt_rtab (i int); CREATE RULE m05pt_lrule AS ON INSERT TO m05pt_rtab DO INSTEAD NOTHING;`,
				},
				alter:  "ALTER RULE m05pt_lrule ON public.m05pt_rtab RENAME TO m05pt_lrule2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_rewrite WHERE rulename='m05pt_lrule2'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE RULE m05pt_lrule3 AS ON INSERT TO m05pt_rtab DO INSTEAD NOTHING;`},
					alter: "ALTER RULE m05pt_lrule3 ON public.m05pt_rtab RENAME TO m05pt_lrule4;",
					check: [][2]string{{`SELECT count(*) FROM pg_rewrite WHERE rulename='m05pt_lrule4'`, "1"}},
				},
			},
			"text-search-config": {
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE TEXT SEARCH CONFIGURATION m05pt_cfg (COPY = english); ALTER EXTENSION cube ADD TEXT SEARCH CONFIGURATION m05pt_cfg;`,
				},
				alter:  "ALTER TEXT SEARCH CONFIGURATION public.m05pt_cfg RENAME TO m05pt_cfg2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_ts_config WHERE cfgname='m05pt_cfg'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE TEXT SEARCH CONFIGURATION m05pt_lcfg (COPY = english);`},
					alter: "ALTER TEXT SEARCH CONFIGURATION public.m05pt_lcfg RENAME TO m05pt_lcfg2;",
					check: [][2]string{{`SELECT count(*) FROM pg_ts_config WHERE cfgname='m05pt_lcfg2'`, "1"}},
				},
			},
			"text-search-dictionary": {
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE TEXT SEARCH DICTIONARY m05pt_dict (TEMPLATE = snowball, Language = 'english'); ALTER EXTENSION cube ADD TEXT SEARCH DICTIONARY m05pt_dict;`,
				},
				alter:  "ALTER TEXT SEARCH DICTIONARY public.m05pt_dict RENAME TO m05pt_dict2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_ts_dict WHERE dictname='m05pt_dict'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE TEXT SEARCH DICTIONARY m05pt_ldict (TEMPLATE = snowball, Language = 'english');`},
					alter: "ALTER TEXT SEARCH DICTIONARY public.m05pt_ldict RENAME TO m05pt_ldict2;",
					check: [][2]string{{`SELECT count(*) FROM pg_ts_dict WHERE dictname='m05pt_ldict2'`, "1"}},
				},
			},
			"text-search-parser": {
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE TEXT SEARCH PARSER m05pt_prs (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype); ALTER EXTENSION cube ADD TEXT SEARCH PARSER m05pt_prs;`,
				},
				alter:  "ALTER TEXT SEARCH PARSER public.m05pt_prs RENAME TO m05pt_prs2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_ts_parser WHERE prsname='m05pt_prs'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE TEXT SEARCH PARSER m05pt_lprs (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);`},
					alter: "ALTER TEXT SEARCH PARSER public.m05pt_lprs RENAME TO m05pt_lprs2;",
					check: [][2]string{{`SELECT count(*) FROM pg_ts_parser WHERE prsname='m05pt_lprs2'`, "1"}},
				},
			},
			"text-search-template": {
				plant: []string{
					`CREATE EXTENSION cube`,
					`CREATE TEXT SEARCH TEMPLATE m05pt_tmpl (INIT = dsimple_init, LEXIZE = dsimple_lexize); ALTER EXTENSION cube ADD TEXT SEARCH TEMPLATE m05pt_tmpl;`,
				},
				alter:  "ALTER TEXT SEARCH TEMPLATE public.m05pt_tmpl RENAME TO m05pt_tmpl2;",
				probes: [][2]string{{`SELECT count(*) FROM pg_ts_template WHERE tmplname='m05pt_tmpl'`, "1"}},
				legal: &legalAlter{
					plant: []string{`CREATE TEXT SEARCH TEMPLATE m05pt_ltmpl (INIT = dsimple_init, LEXIZE = dsimple_lexize);`},
					alter: "ALTER TEXT SEARCH TEMPLATE public.m05pt_ltmpl RENAME TO m05pt_ltmpl2;",
					check: [][2]string{{`SELECT count(*) FROM pg_ts_template WHERE tmplname='m05pt_ltmpl2'`, "1"}},
				},
			},
		}
		var nameKinds []string
		for _, kind := range db.AllowlistedAlterKinds() {
			if kind == "extension" || db.GuardTargetClass(kind) != "name" {
				continue
			}
			nameKinds = append(nameKinds, kind)
		}
		if len(nameKinds) == 0 {
			t.Fatal("no name-class allowlisted alter kinds enumerated — the property table went vacuous")
		}
		for _, kind := range nameKinds {
			kind := kind
			fix, ok := fixtures[kind]
			if !ok {
				t.Errorf("no property-table fixture for name-class allowlisted alter kind %q — every name-class ALTER kind needs an extension-member fixture (or a documented vacuous entry) and a legal user direction", kind)
				continue
			}
			t.Run(kind, func(t *testing.T) {
				dbURL := newDB(t)
				for _, stmt := range fix.plant {
					execRaw(t, dbURL, stmt)
				}
				dir := writeMigrations(t, map[string]string{
					"001_pt.up.sql":   fix.alter,
					"001_pt.down.sql": "SELECT 1;",
				})
				if fix.vacuous == "" {
					// Fail-before: the pre-M05 binary carries no
					// guard, and PostgreSQL accepts member ALTERs —
					// the member is renamed or relocated ungated.
					code, out := runCLIProcess(t, preM05Bin, dbURL, "migrate", "--dir", dir)
					if code != 0 {
						t.Fatalf("pre-M05 binary must apply the %s member ALTER (fail-before — PostgreSQL accepts member renames): %s", kind, out)
					}
					for _, probe := range fix.probes {
						if got := query(t, dbURL, probe[0]); got == probe[1] {
							t.Fatalf("fail-before premise wrong: member intact after the pre-M05-binary run (%s)", probe[0])
						}
					}
				}
				// Guarded assertions on a freshly planted database.
				dbGuarded := newDB(t)
				for _, stmt := range fix.plant {
					execRaw(t, dbGuarded, stmt)
				}
				dirG := writeMigrations(t, map[string]string{
					"001_pt.up.sql":   fix.alter,
					"001_pt.down.sql": "SELECT 1;",
				})
				if fix.vacuous != "" {
					// No member is constructible for this class: the
					// membership arm must not over-refuse the user
					// ALTER (the extension stays installed).
					if code, out := runCLI(t, dbGuarded, "migrate", "--dir", dirG); code != 0 {
						t.Fatalf("vacuous %s member direction over-refused (%s): %s", kind, fix.vacuous, out)
					}
					for _, probe := range fix.probes {
						if got := query(t, dbGuarded, probe[0]); got != probe[1] {
							t.Fatalf("user ALTER effect wrong after the vacuous %s run (%s), got %s", kind, probe[0], got)
						}
					}
				} else {
					for _, flags := range [][]string{{}, {"--allow-destructive"}} {
						if code, out := runCLI(t, dbGuarded, append([]string{"migrate", "--dir", dirG}, flags...)...); code == 0 {
							t.Fatalf("%s ALTER of an extension member accepted (flags=%v)", kind, flags)
						} else if !strings.Contains(out, "protected") || !strings.Contains(out, "owned by extension cube") {
							t.Fatalf("refusal must name protection and the owning extension (flags=%v): %s", flags, out)
						}
					}
					for _, probe := range fix.probes {
						if got := query(t, dbGuarded, probe[0]); got != probe[1] {
							t.Fatalf("member must survive the refused %s ALTER (%s), got %s", kind, probe[0], got)
						}
					}
					if got := query(t, dbGuarded, `SELECT CASE WHEN to_regclass('public._neutron_migrations') IS NULL THEN 'none' ELSE (SELECT count(*)::text FROM _neutron_migrations) END`); got != "none" && got != "0" {
						t.Fatalf("atomic refusal: no history may exist, got %s", got)
					}
				}
				// Legal direction: the same ALTER shape over a USER
				// object applies without the flag (no name-class ALTER
				// kind is in the destructive vocabulary).
				if fix.legal != nil {
					for _, stmt := range fix.legal.plant {
						execRaw(t, dbGuarded, stmt)
					}
					dirL := writeMigrations(t, map[string]string{
						"002_legal.up.sql":   fix.legal.alter,
						"002_legal.down.sql": "SELECT 1;",
					})
					if code, out := runCLI(t, dbGuarded, "migrate", "--dir", dirL); code != 0 {
						t.Fatalf("legal %s ALTER must apply without the flag: %s", kind, out)
					}
					for _, probe := range fix.legal.check {
						if got := query(t, dbGuarded, probe[0]); got != probe[1] {
							t.Fatalf("legal %s ALTER effect wrong (%s), got %s", kind, probe[0], got)
						}
					}
				}
			})
		}
	})

	// ------------------------------------------------------------------
	// AC. Review-7 LOW-1: a flag-parse failure (an unknown flag on a
	//     subcommand) must be reported, not a silent exit 1 — cobra's
	//     error is surfaced by the root FlagErrorFunc, and parsing
	//     precedes every connection-side effect.
	// ------------------------------------------------------------------
	t.Run("UnknownFlagFailureIsNotSilent", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_x.up.sql":   "CREATE TABLE m05uf (i int);",
			"001_x.down.sql": "DROP TABLE m05uf;",
		})
		code, out := runCLI(t, dbURL, "migrate", "down", "1", "--dir", dir, "--allow-destructive")
		if code == 0 {
			t.Fatalf("unknown flag accepted: %s", out)
		}
		if !strings.Contains(out, "unknown flag") {
			t.Fatalf("unknown-flag failure must be reported, not a silent exit 1: %q", out)
		}
		if got := query(t, dbURL, `SELECT CASE WHEN to_regclass('_neutron_migrations') IS NULL THEN 'none' ELSE 'some' END`); got != "none" {
			t.Fatalf("flag-parse failure must precede every connection-side effect, history table: %s", got)
		}
	})
}

// buildPreM05CLIBinary builds the pre-M05 CLI — the parent of the M05
// landing, the last tree WITHOUT the M05 guards — via read-only
// `git archive` (plus the gitignored embedded Studio assets copied from the
// working tree): fail-before reproduction without touching the tree. The
// reference is located by the card marker in the landing's subject, not a
// pinned SHA, so it survives rebases and rebase-merges.
func buildPreM05CLIBinary(t *testing.T) string {
	t.Helper()
	return buildRevisionCLIBinary(t, parentOfLanding(t, "(orm-program M05)"))
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
