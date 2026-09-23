package cmd

import (
	"context"
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

// TestSnapshotPlanningE2E exercises the V11 offline-generation contract
// through the REAL CLI binary against REAL disposable Postgres databases:
// two pending migrations planned without applying the first (the second
// plans against the first's snapshot), byte-identical artifacts for equal
// inputs across independent checkouts, divergent-branch and same-ID
// conflicts failing loudly, baseline with unmanaged sentinels and unknown
// history (no silent adoption/reset), drift failing `schema check --live`
// before any apply, export/source failures leaving no half-written
// artifacts, and the documented shell-QUOTED rename invocation running
// correctly end to end.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server, or when NEUTRON_LIVE_REQUIRED=1 that missing URL is a failure.
// Each case owns a uniquely-named database (m03_*) and drops it afterwards.
func TestSnapshotPlanningE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; end-to-end snapshot-planning check skipped (set it to a disposable Postgres URL to run)")
	}

	bin := buildCLIBinary(t)

	newDB := func(t *testing.T) string {
		t.Helper()
		dbName := fmt.Sprintf("m03_%d_%d", os.Getpid(), time.Now().UnixNano())
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
		return dbURL
	}

	mustExec := func(t *testing.T, dbURL, sql string) {
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

	queryScalar := func(t *testing.T, dbURL, sql string) string {
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

	runCLI := func(t *testing.T, dbURL string, args ...string) (int, string) {
		t.Helper()
		return runCLIProcess(t, bin, dbURL, args...)
	}

	// unreachableURL proves a command is offline: nothing can connect to it.
	unreachableURL := "postgres://m03-no-such-user@127.0.0.1:1/m03offline"

	// pullDoc introspects dbURL into a v2 document at path (read-only).
	pullDoc := func(t *testing.T, dbURL, path string) {
		t.Helper()
		if code, out := runCLI(t, dbURL, "schema", "pull", "--out", path); code != 0 {
			t.Fatalf("schema pull failed (%d): %s", code, out)
		}
	}

	// editDocJSON loads a v2 document as generic JSON, applies edit, and
	// writes the result: the CLI canonicalizes any valid document, so the
	// edit shape stays independent of the canonical form.
	editDocJSON := func(t *testing.T, src, dst string, edit func(m map[string]any)) {
		t.Helper()
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("parse %s: %v", src, err)
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

	addPostsTable := func(m map[string]any) {
		posts := map[string]any{
			"identity": map[string]any{"schema": "public", "name": "posts"},
			"managed":  true,
			"columns": []any{
				map[string]any{"name": "id", "type": map[string]any{"name": "int4", "codec": "number"}, "notNull": true},
				map[string]any{"name": "title", "type": map[string]any{"name": "text", "codec": "string"}, "notNull": true},
				map[string]any{"name": "author_id", "type": map[string]any{"name": "int4", "codec": "number"}, "notNull": true},
			},
			"constraints": []any{
				map[string]any{"type": "primary-key", "name": "posts_pkey", "columns": []any{"id"}},
				map[string]any{"type": "foreign-key", "name": "posts_author_fkey", "columns": []any{"author_id"},
					"references": map[string]any{"table": map[string]any{"schema": "public", "name": "users"}, "columns": []any{"id"}}},
			},
			"indexes": []any{},
		}
		m["tables"] = append(m["tables"].([]any), posts)
	}

	renameUsersColumn := func(m map[string]any, from, to string) {
		for _, tv := range m["tables"].([]any) {
			tm := tv.(map[string]any)
			id := tm["identity"].(map[string]any)
			if id["schema"] == "public" && id["name"] == "users" {
				for _, cv := range tm["columns"].([]any) {
					cm := cv.(map[string]any)
					if cm["name"] == from {
						cm["name"] = to
					}
				}
			}
		}
	}

	dirFiles := func(t *testing.T, root string) []string {
		t.Helper()
		var out []string
		filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				rel, _ := filepath.Rel(root, path)
				data, _ := os.ReadFile(path)
				out = append(out, fmt.Sprintf("%s=%s", rel, data))
			}
			return nil
		})
		return out
	}

	assertNoTempArtifacts := func(t *testing.T, root string) {
		t.Helper()
		for _, f := range dirFiles(t, root) {
			name := f[:strings.IndexByte(f, '=')]
			if strings.HasPrefix(filepath.Base(name), ".neutron-") {
				t.Fatalf("temp artifact left behind: %s", name)
			}
		}
	}

	// ------------------------------------------------------------------
	// A. V11 core: two pending migrations generated offline, second plans
	//    against the first's snapshot; then both apply; drift detection.
	// ------------------------------------------------------------------
	t.Run("TwoPendingOfflineThenApplyAndDrift", func(t *testing.T) {
		proj := t.TempDir()
		mig := filepath.Join(proj, "migrations")

		// Desired documents, produced by pull from reference databases
		// (independent oracle: introspection, not hand-written hashes).
		refA := newDB(t)
		mustExec(t, refA, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		docA := filepath.Join(proj, "docA.json")
		pullDoc(t, refA, docA)

		// First migration, generated with an UNREACHABLE database URL:
		// snapshot planning is offline or this fails.
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "add_users"); code != 0 {
			t.Fatalf("offline generate 001 failed (%d): %s", code, out)
		}
		up1 := string(mustRead(t, filepath.Join(mig, "001_add_users.up.sql")))
		if !strings.Contains(up1, `create table "public"."users"`) {
			t.Fatalf("001 up.sql lacks users DDL:\n%s", up1)
		}

		// Second migration WITHOUT applying the first.
		docB := filepath.Join(proj, "docB.json")
		editDocJSON(t, docA, docB, addPostsTable)
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docB, "--name", "add_posts"); code != 0 {
			t.Fatalf("offline generate 002 failed (%d): %s", code, out)
		}
		up2 := string(mustRead(t, filepath.Join(mig, "002_add_posts.up.sql")))
		if strings.Count(strings.ToLower(up2), "create table") != 1 || !strings.Contains(up2, `"posts"`) {
			t.Fatalf("002 up.sql must plan ONLY the posts table:\n%s", up2)
		}
		if strings.Contains(up2, `create table "public"."users"`) {
			t.Fatalf("002 re-plans the first migration:\n%s", up2)
		}
		// The plan artifact must record the pending-aware base: 001's
		// snapshot, not empty, not the live DB.
		plan2 := string(mustRead(t, filepath.Join(mig, "002_add_posts.plan.json")))
		if !strings.Contains(plan2, `"baseSource": "001_add_users"`) {
			t.Fatalf("002 plan.json base is not 001_add_users:\n%s", plan2)
		}

		// Offline check: desired document is in sync with the chain head.
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"schema", "check", "--dir", mig, "--schema", docB); code != 0 {
			t.Fatalf("offline schema check after generate failed (%d): %s", code, out)
		}

		// Apply both pending migrations with the serialized runner.
		dbURL := newDB(t)
		if code, out := runCLI(t, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("apply failed (%d): %s", code, out)
		}
		if got := queryScalar(t, dbURL,
			`SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('users','posts')`); got != "2" {
			t.Fatalf("users+posts tables = %s, want 2", got)
		}
		if got := queryScalar(t, dbURL,
			`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='posts_author_fkey'`); !strings.Contains(got, "FOREIGN KEY (author_id) REFERENCES users(id)") {
			t.Fatalf("FK constraint wrong: %s", got)
		}
		if got := queryScalar(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "2" {
			t.Fatalf("history rows = %s, want 2", got)
		}
		if code, out := runCLI(t, dbURL, "schema", "check", "--live", "--dir", mig); code != 0 {
			t.Fatalf("live check after apply failed (%d): %s", code, out)
		}

		// Unmanaged out-of-scope objects are not drift.
		mustExec(t, dbURL, `CREATE SCHEMA side; CREATE TABLE side.keep (k text); INSERT INTO side.keep VALUES ('mine')`)
		if code, out := runCLI(t, dbURL, "schema", "check", "--live", "--dir", mig); code != 0 {
			t.Fatalf("live check with out-of-scope sentinel failed (%d): %s", code, out)
		}

		// DRIFT: change the database outside migration files. The check
		// must fail BEFORE any further apply is considered.
		mustExec(t, dbURL, `ALTER TABLE users ADD COLUMN sneaky text`)
		code, out := runCLI(t, dbURL, "schema", "check", "--live", "--dir", mig)
		if code == 0 || !strings.Contains(out, "Drift") {
			t.Fatalf("drift not detected (code %d): %s", code, out)
		}
		mustExec(t, dbURL, `ALTER TABLE users DROP COLUMN sneaky`)
		if code, out := runCLI(t, dbURL, "schema", "check", "--live", "--dir", mig); code != 0 {
			t.Fatalf("live check after revert failed (%d): %s", code, out)
		}
	})

	// ------------------------------------------------------------------
	// B. Equal inputs, independent checkouts: byte-identical artifacts.
	// ------------------------------------------------------------------
	t.Run("EqualInputsEqualPlans", func(t *testing.T) {
		refA := newDB(t)
		mustExec(t, refA, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		docA := filepath.Join(t.TempDir(), "docA.json")
		pullDoc(t, refA, docA)
		want := string(mustRead(t, docA))

		var runs [][]string
		for i := 0; i < 2; i++ {
			proj := t.TempDir()
			mig := filepath.Join(proj, "migrations")
			if code, out := runCLIProcess(t, bin, unreachableURL,
				"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "add_users"); code != 0 {
				t.Fatalf("dev %d generate failed (%d): %s", i+1, code, out)
			}
			runs = append(runs, dirFiles(t, proj))
		}
		if strings.Join(runs[0], "\x00") != strings.Join(runs[1], "\x00") {
			t.Fatalf("equal inputs produced different artifacts:\n%s\n---\n%s", strings.Join(runs[0], "\n"), strings.Join(runs[1], "\n"))
		}
		if len(runs[0]) != 4 {
			t.Fatalf("artifact set = %d files, want 4 (up, down, plan, snapshot)", len(runs[0]))
		}
		if string(mustRead(t, docA)) != want {
			t.Fatalf("generate mutated the desired document")
		}
	})

	// ------------------------------------------------------------------
	// C. Divergent branches / same migration ID: fail clearly, never
	//    overwrite IDs or snapshots, never write partial artifacts.
	// ------------------------------------------------------------------
	t.Run("DivergentBranchesSameIDConflict", func(t *testing.T) {
		refA := newDB(t)
		mustExec(t, refA, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		docA := filepath.Join(t.TempDir(), "docA.json")
		pullDoc(t, refA, docA)
		docB := filepath.Join(t.TempDir(), "docB.json")
		editDocJSON(t, docA, docB, addPostsTable)

		shared := t.TempDir()
		mig := filepath.Join(shared, "migrations")
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "add_users"); code != 0 {
			t.Fatalf("base generate failed (%d): %s", code, out)
		}

		gen := func(proj string, schema, name string) {
			if code, out := runCLIProcess(t, bin, unreachableURL,
				"migrate", "generate", "--mode", "snapshot", "--dir", filepath.Join(proj, "migrations"), "--schema", schema, "--name", name); code != 0 {
				t.Fatalf("branch generate %s failed (%d): %s", name, code, out)
			}
		}
		branchA := t.TempDir()
		copyDirTest(t, shared, branchA)
		gen(branchA, docB, "add_posts")

		branchB := t.TempDir()
		copyDirTest(t, shared, branchB)
		docE := filepath.Join(t.TempDir(), "docE.json")
		editDocJSON(t, docA, docE, func(m map[string]any) { renameUsersColumn(m, "name", "email") })
		gen(branchB, docE, "add_email")

		// Merge both branches into one directory: both claim version 002
		// from the same base state. Everything must refuse.
		merged := t.TempDir()
		copyDirTest(t, branchA, merged)
		copyDirTest(t, branchB, merged)
		migM := filepath.Join(merged, "migrations")
		before := dirFiles(t, merged)

		code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", migM, "--schema", docB, "--name", "next")
		if code == 0 || !strings.Contains(out, "divergent") ||
			!strings.Contains(out, "002_add_posts") || !strings.Contains(out, "002_add_email") {
			t.Fatalf("divergent chain not refused clearly (code %d): %s", code, out)
		}
		if code, out := runCLIProcess(t, bin, unreachableURL, "schema", "check", "--dir", migM, "--schema", docB); code == 0 {
			t.Fatalf("schema check accepted a divergent chain: %s", out)
		}
		if got := dirFiles(t, merged); strings.Join(got, "\x00") != strings.Join(before, "\x00") {
			t.Fatalf("failed generate on divergent chain wrote files")
		}
	})

	// ------------------------------------------------------------------
	// D. Baseline: unmanaged sentinels + UNKNOWN history — read-only
	//    inspection, no silent adoption, no reset.
	// ------------------------------------------------------------------
	t.Run("BaselineSentinelsUnknownHistory", func(t *testing.T) {
		dbURL := newDB(t)
		mustExec(t, dbURL, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		mustExec(t, dbURL, `INSERT INTO users VALUES (1, 'Ada')`)
		// Unknown history: pre-M04 SDK integer-version rows.
		mustExec(t, dbURL, `CREATE TABLE _neutron_migrations (version integer PRIMARY KEY, name text, applied_at timestamptz DEFAULT now())`)
		mustExec(t, dbURL, `INSERT INTO _neutron_migrations (version, name) VALUES (1, 'init'), (2, 'more')`)
		// Unmanaged sentinels outside public.
		mustExec(t, dbURL, `CREATE SCHEMA sentinel; CREATE TABLE sentinel.archive (id integer PRIMARY KEY, note text NOT NULL)`)
		mustExec(t, dbURL, `INSERT INTO sentinel.archive VALUES (7, 'keep me')`)

		historyBefore := queryScalar(t, dbURL, `SELECT string_agg(version::text || ':' || coalesce(name,''), ',' ORDER BY version) FROM _neutron_migrations`)
		archiveBefore := queryScalar(t, dbURL, `SELECT note FROM sentinel.archive WHERE id = 7`)
		objectsBefore := queryScalar(t, dbURL, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','S','v','m')`)

		proj := t.TempDir()
		mig := filepath.Join(proj, "migrations")
		code, out := runCLI(t, dbURL, "schema", "baseline", "--dir", mig)
		if code != 0 {
			t.Fatalf("baseline failed (%d): %s", code, out)
		}
		if !strings.Contains(out, "UNKNOWN") {
			t.Fatalf("baseline did not report UNKNOWN history: %s", out)
		}
		if _, err := os.Stat(filepath.Join(mig, "snapshots", "000_baseline.snapshot.json")); err != nil {
			t.Fatalf("baseline snapshot missing: %v", err)
		}

		// Read-only proof: nothing in the database changed.
		if got := queryScalar(t, dbURL, `SELECT string_agg(version::text || ':' || coalesce(name,''), ',' ORDER BY version) FROM _neutron_migrations`); got != historyBefore {
			t.Fatalf("baseline modified history: %s -> %s", historyBefore, got)
		}
		if got := queryScalar(t, dbURL, `SELECT note FROM sentinel.archive WHERE id = 7`); got != archiveBefore {
			t.Fatalf("baseline touched sentinel data")
		}
		if got := queryScalar(t, dbURL, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','S','v','m')`); got != objectsBefore {
			t.Fatalf("baseline created/dropped objects: %s -> %s", objectsBefore, got)
		}

		// Live check refuses the legacy history: applied state stays
		// unknown until M04 adoption — no silent adoption here.
		if code, out := runCLI(t, dbURL, "schema", "check", "--live", "--dir", mig); code == 0 || !strings.Contains(out, "legacy") {
			t.Fatalf("live check did not refuse legacy history (code %d): %s", code, out)
		}

		// Generating from the baseline: desired document pulled from a
		// clean reference (public.users only). Sentinels are out of scope,
		// users converges — no changes, nothing written.
		refA := newDB(t)
		mustExec(t, refA, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		docA := filepath.Join(proj, "docA.json")
		pullDoc(t, refA, docA)
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "noop"); code != 0 {
			t.Fatalf("generate from baseline failed (%d): %s", code, out)
		} else if !strings.Contains(out, "No schema changes detected") && !strings.Contains(out, "No applicable changes") {
			t.Fatalf("baseline-vs-desired should converge: %s", out)
		}
		if _, err := os.Stat(filepath.Join(mig, "001_noop.up.sql")); !os.IsNotExist(err) {
			t.Fatalf("converged generate wrote artifacts")
		}

		// Planning a real change off the baseline keeps sentinels out of
		// the plan (out-of-scope) and the runner still refuses the
		// unadopted legacy history at apply time.
		docB := filepath.Join(proj, "docB.json")
		editDocJSON(t, docA, docB, addPostsTable)
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docB, "--name", "add_posts"); code != 0 {
			t.Fatalf("generate add_posts from baseline failed (%d): %s", code, out)
		}
		up := string(mustRead(t, filepath.Join(mig, "001_add_posts.up.sql")))
		if strings.Contains(up, "sentinel") || strings.Contains(up, "_neutron_migrations") || strings.Contains(up, "drop") {
			t.Fatalf("plan touches unmanaged objects:\n%s", up)
		}
		if code, out := runCLI(t, dbURL, "migrate", "--dir", mig); code == 0 {
			t.Fatalf("runner applied against unadopted legacy history: %s", out)
		}
		if got := queryScalar(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='posts'`); got != "0" {
			t.Fatalf("refused apply still created posts")
		}
		if got := queryScalar(t, dbURL, `SELECT note FROM sentinel.archive WHERE id = 7`); got != archiveBefore {
			t.Fatalf("sentinel data disturbed by refused apply")
		}
	})

	// ------------------------------------------------------------------
	// E. Source/export failures leave no half-written artifacts.
	// ------------------------------------------------------------------
	t.Run("ExportAndSourceFailuresAtomic", func(t *testing.T) {
		proj := t.TempDir()
		mig := filepath.Join(proj, "migrations")

		refA := newDB(t)
		mustExec(t, refA, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		goodDoc := filepath.Join(proj, "good.json")
		pullDoc(t, refA, goodDoc)
		goodBytes := mustRead(t, goodDoc)

		outPath := filepath.Join(proj, "neutron.schema.json")
		writeFile(t, outPath, string(goodBytes))

		// A working export module round-trips the document.
		okModule := filepath.Join(proj, "export-ok.mjs")
		writeFile(t, okModule, fmt.Sprintf("process.stdout.write(%s);\n", strconvQuote(goodBytes)))
		if code, out := runCLIProcess(t, bin, unreachableURL, "schema", "export", "--module", okModule, "--out", outPath); code != 0 {
			t.Fatalf("export failed (%d): %s", code, out)
		}

		// A crashing export module must not replace the previous document.
		crashModule := filepath.Join(proj, "export-crash.mjs")
		writeFile(t, crashModule, "process.stderr.write('boom'); process.exit(3);\n")
		if code, out := runCLIProcess(t, bin, unreachableURL, "schema", "export", "--module", crashModule, "--out", outPath); code == 0 {
			t.Fatalf("crashing export reported success: %s", out)
		}
		if string(mustRead(t, outPath)) != string(goodBytes) {
			t.Fatalf("crashing export replaced the previous document")
		}

		// A module printing garbage must not replace it either.
		junkModule := filepath.Join(proj, "export-junk.mjs")
		writeFile(t, junkModule, "process.stdout.write('this is not json');\n")
		if code, out := runCLIProcess(t, bin, unreachableURL, "schema", "export", "--module", junkModule, "--out", outPath); code == 0 {
			t.Fatalf("garbage export reported success: %s", out)
		}
		if string(mustRead(t, outPath)) != string(goodBytes) {
			t.Fatalf("garbage export replaced the previous document")
		}
		assertNoTempArtifacts(t, proj)

		// A corrupt desired document fails generation before any write.
		badDoc := filepath.Join(proj, "bad.json")
		writeFile(t, badDoc, `{"version": 2, "not": "a valid document"}`)
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", badDoc, "--name", "broken"); code == 0 {
			t.Fatalf("generate accepted a corrupt document: %s", out)
		}
		if _, err := os.Stat(filepath.Join(mig, "001_broken.up.sql")); !os.IsNotExist(err) {
			t.Fatalf("failed generate left artifacts")
		}
		assertNoTempArtifacts(t, proj)
	})

	// ------------------------------------------------------------------
	// F. The documented shell-QUOTED rename invocation, end to end.
	// ------------------------------------------------------------------
	t.Run("QuotedRenameShellInvocation", func(t *testing.T) {
		dbURL := newDB(t)
		proj := t.TempDir()
		mig := filepath.Join(proj, "migrations")

		refA := newDB(t)
		mustExec(t, refA, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		docA := filepath.Join(proj, "docA.json")
		pullDoc(t, refA, docA)

		if code, out := runCLI(t, dbURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "add_users"); code != 0 {
			t.Fatalf("generate 001 failed (%d): %s", code, out)
		}
		if code, out := runCLI(t, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("apply 001 failed (%d): %s", code, out)
		}
		mustExec(t, dbURL, `INSERT INTO users VALUES (1, 'Ada')`)

		docR := filepath.Join(proj, "docR.json")
		editDocJSON(t, docA, docR, func(m map[string]any) { renameUsersColumn(m, "name", "full_name") })

		// The documented form: a real shell with the SINGLE-QUOTED rename
		// value (unquoted `>` is a shell redirect, not an argument).
		workdir := t.TempDir()
		script := fmt.Sprintf(
			`cd %q && %q --url %q migrate generate --mode snapshot --dir %q --schema %q --name rename_full_name --rename 'public.users.name>public.users.full_name'`,
			proj, bin, dbURL, mig, docR)
		cmd := exec.Command("sh", "-c", script)
		cmd.Dir = workdir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("documented quoted-rename invocation failed: %s\n%s", err, out)
		}
		up := string(mustRead(t, filepath.Join(mig, "002_rename_full_name.up.sql")))
		if !strings.Contains(up, `rename column "name" to "full_name"`) {
			t.Fatalf("rename SQL missing:\n%s", up)
		}
		// The validated rename intent is recorded in the plan artifact.
		plan := string(mustRead(t, filepath.Join(mig, "002_rename_full_name.plan.json")))
		if !strings.Contains(plan, `"renames": [`) || !strings.Contains(plan, "public.users.name>public.users.full_name") {
			t.Fatalf("rename not recorded in plan.json:\n%s", plan)
		}

		if code, out := runCLI(t, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("apply rename failed (%d): %s", code, out)
		}
		if got := queryScalar(t, dbURL, `SELECT full_name FROM users WHERE id = 1`); got != "Ada" {
			t.Fatalf("rename lost data: full_name = %q", got)
		}
		if got := queryScalar(t, dbURL, `SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='name'`); got != "0" {
			t.Fatalf("old column still present after rename")
		}

		// The UNQUOTED variant is the documented hazard: the shell eats
		// the `>` and the CLI must reject the leftover value.
		bad := t.TempDir()
		badScript := fmt.Sprintf(
			`cd %q && %q --url %q migrate generate --mode snapshot --dir %q --schema %q --name bad_rename --rename public.users.name>public.users.full_name`,
			proj, bin, dbURL, mig, docR)
		bcmd := exec.Command("sh", "-c", badScript)
		bcmd.Dir = bad
		out, err := bcmd.CombinedOutput()
		if err == nil {
			t.Fatalf("unquoted rename variant unexpectedly succeeded: %s", out)
		}
		if _, statErr := os.Stat(filepath.Join(mig, "003_bad_rename.up.sql")); !os.IsNotExist(statErr) {
			t.Fatalf("unquoted rename attempt left artifacts")
		}
		assertNoTempArtifacts(t, proj)
	})
	// ------------------------------------------------------------------
	// G. Review-1 MAJOR-1 repro through the real binary: merge residue —
	//    one branch's snapshot kept, BOTH branches' sql files present.
	//    Generation must refuse naming the un-snapshotted migration.
	// ------------------------------------------------------------------
	t.Run("MergeResidueSameVersionRefused", func(t *testing.T) {
		refA := newDB(t)
		mustExec(t, refA, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		docA := filepath.Join(t.TempDir(), "docA.json")
		pullDoc(t, refA, docA)
		docB := filepath.Join(t.TempDir(), "docB.json")
		editDocJSON(t, docA, docB, addPostsTable)
		docE := filepath.Join(t.TempDir(), "docE.json")
		editDocJSON(t, docA, docE, func(m map[string]any) { renameUsersColumn(m, "name", "email") })

		shared := t.TempDir()
		mig := filepath.Join(shared, "migrations")
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "add_users"); code != 0 {
			t.Fatalf("base generate failed (%d): %s", code, out)
		}
		gen := func(proj string, schema, name string) {
			if code, out := runCLIProcess(t, bin, unreachableURL,
				"migrate", "generate", "--mode", "snapshot", "--dir", filepath.Join(proj, "migrations"), "--schema", schema, "--name", name); code != 0 {
				t.Fatalf("branch generate %s failed (%d): %s", name, code, out)
			}
		}
		branchA := t.TempDir()
		copyDirTest(t, shared, branchA)
		gen(branchA, docB, "add_posts")
		branchB := t.TempDir()
		copyDirTest(t, shared, branchB)
		gen(branchB, docE, "add_users_email")

		// Merge: 001 full set + branch B's full 002 set (snapshot
		// included) + branch A's 002 sql/plan files WITHOUT its snapshot.
		merged := t.TempDir()
		copyDirTest(t, shared, merged)
		copyDirTest(t, branchB, merged)
		for _, f := range []string{"002_add_posts.up.sql", "002_add_posts.down.sql", "002_add_posts.plan.json"} {
			writeFile(t, filepath.Join(merged, "migrations", f), string(mustRead(t, filepath.Join(branchA, "migrations", f))))
		}
		migM := filepath.Join(merged, "migrations")
		before := dirFiles(t, merged)

		code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", migM, "--schema", docB, "--name", "next")
		if code == 0 || !strings.Contains(out, "002_add_posts") || !strings.Contains(out, "002_add_users_email") {
			t.Fatalf("merge residue not refused naming both same-version migrations (code %d): %s", code, out)
		}
		if got := dirFiles(t, merged); strings.Join(got, "\x00") != strings.Join(before, "\x00") {
			t.Fatalf("refused generate on merge residue wrote files")
		}
		if code, out := runCLIProcess(t, bin, unreachableURL, "schema", "check", "--dir", migM, "--schema", docB); code == 0 {
			t.Fatalf("schema check accepted merge residue: %s", out)
		}
	})

	// ------------------------------------------------------------------
	// H. Review-1 MINOR-1: applied history versions unknown to the chain
	//    (foreign-branch rows) must surface as a check failure, never be
	//    silently dropped from the applied-state attribution.
	// ------------------------------------------------------------------
	t.Run("LiveCheckReportsUnchainableAppliedHistory", func(t *testing.T) {
		refA := newDB(t)
		mustExec(t, refA, `CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)`)
		proj := t.TempDir()
		mig := filepath.Join(proj, "migrations")
		docA := filepath.Join(proj, "docA.json")
		pullDoc(t, refA, docA)
		if code, out := runCLIProcess(t, bin, unreachableURL,
			"migrate", "generate", "--mode", "snapshot", "--dir", mig, "--schema", docA, "--name", "add_users"); code != 0 {
			t.Fatalf("generate 001 failed (%d): %s", code, out)
		}

		dbURL := newDB(t)
		if code, out := runCLI(t, dbURL, "migrate", "--dir", mig); code != 0 {
			t.Fatalf("apply 001 failed (%d): %s", code, out)
		}

		// Foreign applied version: a raw history row no chain snapshot or
		// baseline covers (independent SQL oracle, not the CLI's writer).
		mustExec(t, dbURL, `INSERT INTO _neutron_migrations (version, name) VALUES ('099_foreign_branch', 'residue')`)
		code, out := runCLI(t, dbURL, "schema", "check", "--live", "--dir", mig)
		if code == 0 || !strings.Contains(out, "099_foreign_branch") {
			t.Fatalf("applied version unknown to the chain silently ignored (code %d): %s", code, out)
		}

		// With the foreign row gone the check attributes state cleanly again.
		mustExec(t, dbURL, `DELETE FROM _neutron_migrations WHERE version = '099_foreign_branch'`)
		if code, out := runCLI(t, dbURL, "schema", "check", "--live", "--dir", mig); code != 0 {
			t.Fatalf("live check after cleanup failed (%d): %s", code, out)
		}
	})
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func copyDirTest(t *testing.T, src, dst string) {
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

// strconvQuote renders bytes as a double-quoted JavaScript string literal.
func strconvQuote(b []byte) string {
	q, _ := json.Marshal(string(b))
	return string(q)
}
