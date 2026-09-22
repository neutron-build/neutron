package cmd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestMigrationRunnerE2E exercises the V12 history/concurrency/recovery
// contract through the REAL CLI binary against REAL disposable Postgres
// databases: advisory-lock serialization (two processes, kill windows, lock
// timeout, pinned session), checksum enforcement before mutation, legacy
// adoption fixtures (CLI/TS/Go-SDK shapes, partially applied histories), and
// mixed-runner refusal. State is asserted by inspecting the database, not
// command output.
//
// Skipped unless NEUTRON_E2E_DATABASE_URL points at a disposable Postgres
// server, or when NEUTRON_LIVE_REQUIRED=1 that missing URL is a failure.
// Each case owns a uniquely-named database (neutron_orm_m04_*) and drops it
// afterwards; kill-test processes are waited on before teardown.
func TestMigrationRunnerE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; end-to-end migration-runner check skipped (set it to a disposable Postgres URL to run)")
	}

	bin := buildCLIBinary(t)

	// newDB provisions one disposable database per case and registers LIFO
	// cleanup (terminate stragglers, then drop). Cases with their own
	// namespace (review rework) pass their own prefix.
	newDBWithPrefix := func(t *testing.T, prefix string) string {
		t.Helper()
		dbName := fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
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
	newDB := func(t *testing.T) string { return newDBWithPrefix(t, "neutron_orm_m04") }

	// adminQuery runs a scalar query against a case's database.
	adminQuery := func(t *testing.T, dbURL, sql string) string {
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

	// runCLI executes the real binary with --url pinned to the disposable DB.
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

	// ------------------------------------------------------------------
	// A. Two processes, same migration: exactly one effect + one entry.
	// ------------------------------------------------------------------
	t.Run("TwoProcessesExactlyOneEffect", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_init.up.sql":   "SELECT pg_sleep(3);\nCREATE TABLE solo (id INT PRIMARY KEY);\nINSERT INTO solo VALUES (1);",
			"001_init.down.sql": "DROP TABLE solo;",
		})

		var wg sync.WaitGroup
		codes := make([]int, 2)
		for i := range codes {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				code, _ := runCLI(t, dbURL, "migrate", "--dir", dir)
				codes[i] = code
			}(i)
		}
		wg.Wait()

		for i, code := range codes {
			if code != 0 {
				t.Fatalf("process %d exited %d; both must succeed (second reports up-to-date)", i+1, code)
			}
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM solo"); got != "1" {
			t.Errorf("solo rows = %s, want exactly 1 effect", got)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations"); got != "1" {
			t.Errorf("history rows = %s, want exactly 1", got)
		}
		row := adminQuery(t, dbURL,
			"SELECT owner||'|'||format||'|'||length(checksum) FROM _neutron_migrations WHERE version = '001'")
		if row != "neutron-cli|v2|64" {
			t.Errorf("history row metadata = %q, want owner=neutron-cli format=v2 64-hex checksum", row)
		}
	})

	// ------------------------------------------------------------------
	// B. Changed SQL fails before any new mutation.
	// ------------------------------------------------------------------
	t.Run("ChangedSQLFailsBeforeNewMutation", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_a.up.sql": "CREATE TABLE t1 (id INT);",
			"002_b.up.sql": "CREATE TABLE t2 (id INT);",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("initial migrate failed: %s", out)
		}

		// Tamper with the APPLIED 001 and add a pending 003.
		writeFile(t, filepath.Join(dir, "001_a.up.sql"), "CREATE TABLE t1 (id BIGINT);")
		writeFile(t, filepath.Join(dir, "003_c.up.sql"), "CREATE TABLE t3 (id INT);")

		code, out := runCLI(t, dbURL, "migrate", "--dir", dir)
		if code == 0 {
			t.Fatal("modified applied migration accepted")
		}
		if !strings.Contains(out, "modified") {
			t.Errorf("error output must name the drift: %s", out)
		}
		// The pending migration must NOT have been applied.
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM information_schema.tables WHERE table_name IN ('t3')"); got != "0" {
			t.Errorf("pending 003 was applied despite checksum failure: t3 exists")
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations"); got != "2" {
			t.Errorf("history rows = %s, want 2 (no new entries)", got)
		}
	})

	// ------------------------------------------------------------------
	// C. Legacy numeric/text collisions are explicit reconciliation errors.
	// ------------------------------------------------------------------
	t.Run("LegacyNumericTextCollision", func(t *testing.T) {
		dbURL := newDB(t)

		// Files alone collide: "1" and "001" in one checkout. Refusal must
		// precede ALL database side effects — not even the history table is
		// created.
		dir := writeMigrations(t, map[string]string{
			"1_init.up.sql":     "CREATE TABLE c1 (id INT);",
			"001_padded.up.sql": "CREATE TABLE c2 (id INT);",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir)
		if code == 0 || !strings.Contains(out, "collision") {
			t.Fatalf("migrate must name the 1-vs-001 collision, exit=%d output=%s", code, out)
		}
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM information_schema.tables WHERE table_name IN ('_neutron_migrations','c1','c2')"); got != "0" {
			t.Errorf("collision refusal must precede all DDL; found tables")
		}

		// History-vs-file collision through adoption: history written by an
		// old CLI with unpadded id "1", supplied files also carry "001".
		fixture, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer fixture.Close()
		for _, stmt := range []string{
			`CREATE TABLE _neutron_migrations (version TEXT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ DEFAULT now())`,
			`INSERT INTO _neutron_migrations (version, name) VALUES ('1', 'init')`,
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed legacy: %v", err)
			}
		}
		code, out = runCLI(t, dbURL, "migrate", "adopt", "--dir", dir)
		if code == 0 || !strings.Contains(out, "collision") {
			t.Fatalf("adopt must name the history-vs-file collision, exit=%d output=%s", code, out)
		}
		// The aborted adoption left the legacy history untouched.
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations WHERE version='1'"); got != "1" {
			t.Errorf("legacy row lost after refused adoption")
		}
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM information_schema.columns WHERE table_name='_neutron_migrations' AND column_name='format'"); got != "0" {
			t.Errorf("refused adoption must not upgrade the table shape")
		}

		// Exact same spelling passes the identity rule: adopting with the
		// matching file works and reports the row as unverified.
		dir2 := writeMigrations(t, map[string]string{
			"1_init.up.sql": "CREATE TABLE c1 (id INT);",
		})
		if code, out := runCLI(t, dbURL, "migrate", "adopt", "--dir", dir2); code != 0 {
			t.Fatalf("adopt exact-id failed: %s", out)
		} else if !strings.Contains(out, "UNVERIFIED") {
			t.Errorf("CLI legacy row must adopt as unverified: %s", out)
		}
	})

	// ------------------------------------------------------------------
	// D. CLI legacy history adoption (partially applied + missing file).
	// ------------------------------------------------------------------
	t.Run("AdoptCLILegacyHistory", func(t *testing.T) {
		dbURL := newDB(t)
		fixture, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer fixture.Close()
		for _, stmt := range []string{
			`CREATE TABLE _neutron_migrations (version TEXT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ DEFAULT now())`,
			`INSERT INTO _neutron_migrations (version, name) VALUES ('001', 'init'), ('002', 'gone')`,
			`CREATE TABLE adopted_sentinel (id INT PRIMARY KEY)`,
			`INSERT INTO adopted_sentinel VALUES (7)`,
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed legacy: %v", err)
			}
		}

		dir := writeMigrations(t, map[string]string{
			"001_init.up.sql":   "CREATE TABLE users1 (id INT);",
			"003_new.up.sql":    "CREATE TABLE users3 (id INT);",
			"001_init.down.sql": "DROP TABLE users1;",
			"003_new.down.sql":  "DROP TABLE users3;",
		})

		// Pre-M04 history is refused for running until adopted.
		if code, _ := runCLI(t, dbURL, "migrate", "--dir", dir); code == 0 {
			t.Fatal("legacy history accepted for migration without adoption")
		}

		code, out := runCLI(t, dbURL, "migrate", "adopt", "--dir", dir)
		if code != 0 {
			t.Fatalf("adopt failed: %s", out)
		}
		// 001 has a file but CLI legacy rows carry no checksum: unverified.
		// 002 has no file at all: unverified and still visible.
		for _, want := range []string{"001", "002", "UNVERIFIED"} {
			if !strings.Contains(out, want) {
				t.Errorf("adoption report missing %q: %s", want, out)
			}
		}

		// Adoption preserved history and data; running afterwards applies
		// ONLY the pending 003 (partially applied history converges).
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("post-adopt migrate failed: %s", out)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations"); got != "3" {
			t.Errorf("history rows = %s, want 3", got)
		}
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM information_schema.tables WHERE table_name IN ('users1','users3')"); got != "1" {
			t.Errorf("users1/users3 tables = %s, want only users3 (001 was already applied; its DDL must not rerun)", got)
		}
		if got := adminQuery(t, dbURL, "SELECT id FROM adopted_sentinel"); got != "7" {
			t.Errorf("sentinel data lost: %s", got)
		}
		// Unverified rows are never fabricated into checksums.
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM _neutron_migrations WHERE checksum IS NULL AND version IN ('001','002')"); got != "2" {
			t.Errorf("legacy rows must keep NULL checksums, got %s non-null", got)
		}

		// Down runs under the same lock + verification: reverting the newest
		// applied migration removes both its effect and its history row.
		if code, out := runCLI(t, dbURL, "migrate", "down", "--dir", dir); code != 0 {
			t.Fatalf("migrate down failed: %s", out)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM information_schema.tables WHERE table_name='users3'"); got != "0" {
			t.Errorf("users3 still exists after migrate down")
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations"); got != "2" {
			t.Errorf("history rows after down = %s, want 2", got)
		}
		// Re-applying converges back to 3.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("re-migrate after down failed: %s", out)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations"); got != "3" {
			t.Errorf("history rows after re-migrate = %s, want 3", got)
		}
	})

	// ------------------------------------------------------------------
	// E. TS SDK legacy adoption (INTEGER versions, no checksums).
	// ------------------------------------------------------------------
	t.Run("AdoptTSLegacyIntegerHistory", func(t *testing.T) {
		dbURL := newDB(t)
		fixture, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer fixture.Close()
		for _, stmt := range []string{
			// Exactly the table the TS SDK creates (migrate.ts).
			`CREATE TABLE _neutron_migrations (
				version INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`,
			`INSERT INTO _neutron_migrations (version, name) VALUES (1, 'first'), (2, 'second')`,
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed TS legacy: %v", err)
			}
		}

		// Mixed-runner rule: the CLI refuses the SDK-shaped history before
		// mutations.
		dir := writeMigrations(t, map[string]string{
			"1_first.up.sql":  "CREATE TABLE ts_a (id INT);",
			"2_second.up.sql": "CREATE TABLE ts_b (id INT);",
			"3_third.up.sql":  "CREATE TABLE ts_c (id INT);",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code == 0 {
			t.Fatal("SDK-integer history accepted by CLI migrate without adoption")
		} else if !strings.Contains(out, "adopt") {
			t.Errorf("refusal must point at adoption: %s", out)
		}

		if code, out := runCLI(t, dbURL, "migrate", "adopt", "--dir", dir); code != 0 {
			t.Fatalf("adopt failed: %s", out)
		}
		// Explicit INTEGER->TEXT conversion happened and ids kept their
		// canonical decimal spelling.
		if got := adminQuery(t, dbURL,
			"SELECT data_type FROM information_schema.columns WHERE table_name='_neutron_migrations' AND column_name='version'"); got != "text" {
			t.Errorf("version column type = %s, want text after adoption", got)
		}
		if got := adminQuery(t, dbURL,
			"SELECT string_agg(version, ',' ORDER BY version) FROM _neutron_migrations"); got != "1,2" {
			t.Errorf("adopted ids = %s, want 1,2 (no padding, no loss)", got)
		}
		// Migrate then applies only 3.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("post-adopt migrate failed: %s", out)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM information_schema.tables WHERE table_name='ts_c'"); got != "1" {
			t.Errorf("ts_c missing after migrate")
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('ts_a','ts_b')"); got != "0" {
			t.Errorf("already-applied TS migrations were re-run: %s tables exist", got)
		}
	})

	// ------------------------------------------------------------------
	// F. Go SDK legacy adoption (INTEGER + legacy digests + lock table).
	// ------------------------------------------------------------------
	t.Run("AdoptGoSDKLegacyHistory", func(t *testing.T) {
		legacyDigest := func(version int, name, up string) string {
			sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s", version, name, up)))
			return fmt.Sprintf("%x", sum)
		}
		up1 := "CREATE TABLE go_a (id INT);"
		up2 := "CREATE TABLE go_b (id INT);"

		dbURL := newDB(t)
		fixture, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer fixture.Close()
		for _, stmt := range []string{
			// Exactly the table the pre-M04 Go SDK creates (GO-30 era), plus
			// its lock claim table — both present on a real legacy DB.
			`CREATE TABLE _neutron_migrations (
				version INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				checksum TEXT
			)`,
			`CREATE TABLE _neutron_migration_lock (
				id INTEGER PRIMARY KEY,
				token BIGINT NOT NULL,
				locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`,
			fmt.Sprintf(`INSERT INTO _neutron_migrations (version, name, checksum) VALUES (1, 'first', '%s'), (2, 'second', '%s')`,
				legacyDigest(1, "first", up1), legacyDigest(2, "second", up2)),
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed Go legacy: %v", err)
			}
		}

		dir := writeMigrations(t, map[string]string{
			"1_first.up.sql":  up1,
			"2_second.up.sql": up2,
			"3_third.up.sql":  "CREATE TABLE go_c (id INT);",
		})
		if code, _ := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Log("CLI refuses SDK-shaped history before adoption (expected)")
		}

		code, out := runCLI(t, dbURL, "migrate", "adopt", "--dir", dir)
		if code != 0 {
			t.Fatalf("adopt failed: %s", out)
		}
		// Digests reproduced from the supplied files -> verified, and now
		// recorded under the v2 algorithm.
		want1 := db.MigrationChecksum(up1)
		if got := adminQuery(t, dbURL, "SELECT checksum FROM _neutron_migrations WHERE version='1'"); got != want1 {
			t.Errorf("row 1 checksum = %s, want v2 digest %s (verified adoption rewrites it)", got, want1)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations WHERE format='v2'"); got != "2" {
			t.Errorf("v2-stamped rows = %s, want 2", got)
		}

		// Post-adopt run applies only 3, and the adopted rows are enforced.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("post-adopt migrate failed: %s", out)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM information_schema.tables WHERE table_name='go_c'"); got != "1" {
			t.Errorf("go_c missing after migrate")
		}
		writeFile(t, filepath.Join(dir, "1_first.up.sql"), up1+" -- ALTERED")
		if code, _ := runCLI(t, dbURL, "migrate", "--dir", dir); code == 0 {
			t.Error("tampered adopted-verified migration accepted")
		}
	})

	// ------------------------------------------------------------------
	// G. Kill windows: durable state inspected before retry; convergence.
	// ------------------------------------------------------------------
	killCase := func(t *testing.T, name, sql string, expectEffectAfterKill bool, killWhen func(t *testing.T, dbURL string) bool) {
		t.Run(name, func(t *testing.T) {
			dbURL := newDB(t)
			dir := writeMigrations(t, map[string]string{
				"001_kill.up.sql":   sql,
				"001_kill.down.sql": "DROP TABLE IF EXISTS kx;",
			})

			// Spawn the real runner with the migration in flight.
			proc := exec.Command(bin, "--url", dbURL, "migrate", "--dir", dir)
			if err := proc.Start(); err != nil {
				t.Fatal(err)
			}
			if !killWhen(t, dbURL) {
				proc.Process.Kill()
				t.Fatal("kill condition never observed")
			}
			if err := proc.Process.Kill(); err != nil { // SIGKILL
				t.Fatalf("kill: %v", err)
			}
			_ = proc.Wait() // reap; no stray processes survive the case

			// Inspect durable state BEFORE retry — the recovery rule.
			rowsAfterKill := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations")
			if !expectEffectAfterKill && rowsAfterKill != "0" {
				t.Fatalf("history rows after mid-flight kill = %s, want 0 (tx must not have committed)", rowsAfterKill)
			}

			// The killed process's backend keeps the advisory lock until it
			// finishes its server-side pg_sleep and notices the dead socket
			// — the lock's crash story. Wait for it to drain before retry.
			drain := time.Now().Add(45 * time.Second)
			for {
				n := adminQuery(t, dbURL, "SELECT count(*) FROM pg_locks WHERE "+advisoryLockPredicate)
				if n == "0" || !time.Now().Before(drain) {
					break
				}
				time.Sleep(250 * time.Millisecond)
			}

			// Retry converges: exactly one effect, exactly one entry.
			if code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "120s"); code != 0 {
				t.Fatalf("retry failed: %s", out)
			}
			if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations"); got != "1" {
				t.Errorf("history rows after retry = %s, want exactly 1", got)
			}
		})
	}

	// waitForActiveSleep polls until the runner's session is executing the
	// fixture's pg_sleep — the kill point inside the migration transaction.
	waitForActiveSleep := func(t *testing.T, dbURL string) bool {
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			var n string
			fixture, err := db.Connect(context.Background(), dbURL)
			if err == nil {
				err = fixture.QueryRow(context.Background(), `
					SELECT count(*) FROM pg_stat_activity
					WHERE datname = current_database()
					  AND query LIKE '%pg_sleep%'
					  AND pid <> pg_backend_pid()`).Scan(&n)
				fixture.Close()
			}
			if err == nil && n != "0" {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	killCase(t, "KillBeforeDDL",
		"SELECT pg_sleep(30);\nCREATE TABLE k1 (id INT);",
		false, waitForActiveSleep)
	killCase(t, "KillDuringDDL",
		"CREATE TABLE k2 (id INT);\nSELECT pg_sleep(30);\nCREATE TABLE k2b (id INT);",
		false, waitForActiveSleep)
	killCase(t, "KillAfterDDLBeforeCommit",
		"CREATE TABLE k3 (id INT);\nSELECT pg_sleep(30);",
		false, waitForActiveSleep)
	killCase(t, "KillAtUncertainCommit",
		"CREATE TABLE k4 (id INT);\nSELECT pg_sleep(2);",
		true, // commit may land while we watch; retry must still converge
		func(t *testing.T, dbURL string) bool {
			// The uncertain-commit window: wait until the history row is
			// durably visible, then kill before the process can finish.
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				var n string
				fixture, err := db.Connect(context.Background(), dbURL)
				if err == nil {
					err = fixture.QueryRow(context.Background(),
						"SELECT count(*) FROM _neutron_migrations").Scan(&n)
					fixture.Close()
				}
				if err == nil && n == "1" {
					return true
				}
				time.Sleep(20 * time.Millisecond)
			}
			return false
		})

	// ------------------------------------------------------------------
	// H. Pinned-lock session: the advisory lock and the running DDL are the
	// SAME session; no pool-level lock separation.
	// ------------------------------------------------------------------
	t.Run("PinnedLockSessionLifecycle", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_pin.up.sql": "CREATE TABLE pin_t (id INT);\nSELECT pg_sleep(8);",
		})
		proc := exec.Command(bin, "--url", dbURL, "migrate", "--dir", dir, "--timeout", "30s")
		if err := proc.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = proc.Wait() }()

		// Wait until the advisory lock exists, then assert the holder pid is
		// the session RUNNING the migration (not an idle parked session).
		deadline := time.Now().Add(20 * time.Second)
		same := false
		var lockHolders, activeRunners string
		for time.Now().Before(deadline) {
			fixture, err := db.Connect(context.Background(), dbURL)
			if err != nil {
				t.Fatal(err)
			}
			var runner string
			err = fixture.QueryRow(context.Background(), `
				SELECT
				  (SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = 1639946364 AND objid = 284230823),
				  (SELECT count(DISTINCT l.pid) FROM pg_locks l
				   JOIN pg_stat_activity a ON a.pid = l.pid
				   WHERE l.locktype = 'advisory' AND l.classid = 1639946364 AND l.objid = 284230823
				     AND a.query LIKE '%pg_sleep%'
				     AND a.pid <> pg_backend_pid())`).Scan(&lockHolders, &runner)
			fixture.Close()
			if err != nil {
				t.Fatalf("inspect lock/session: %v", err)
			}
			activeRunners = runner
			if lockHolders == "1" && runner == "1" {
				same = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !same {
			t.Fatalf("advisory lock holders = %s, of which running the migration = %s; want 1/1 (lock and work on one session)",
				lockHolders, activeRunners)
		}
	})

	// ------------------------------------------------------------------
	// I. Lock timeout: a waiter with a short budget fails cleanly; the
	// holder is unaffected; exactly one effect.
	// ------------------------------------------------------------------
	t.Run("LockTimeoutCancellation", func(t *testing.T) {
		dbURL := newDB(t)
		dir := writeMigrations(t, map[string]string{
			"001_slow.up.sql": "SELECT pg_sleep(6);\nCREATE TABLE lt (id INT);",
		})

		holder := exec.Command(bin, "--url", dbURL, "migrate", "--dir", dir, "--timeout", "30s")
		if err := holder.Start(); err != nil {
			t.Fatal(err)
		}

		// Wait for the holder to take the lock.
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			var n string
			fixture, err := db.Connect(context.Background(), dbURL)
			if err == nil {
				err = fixture.QueryRow(context.Background(),
					"SELECT count(*) FROM pg_locks WHERE "+advisoryLockPredicate).Scan(&n)
				fixture.Close()
			}
			if err == nil && n == "1" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "2s")
		if code == 0 {
			t.Fatal("waiter with a 2s budget succeeded while a 6s migration held the lock")
		}
		if !strings.Contains(out, "lock") {
			t.Errorf("timeout error must name the lock: %s", out)
		}
		if err := holder.Wait(); err != nil {
			t.Fatalf("holder failed after waiter timeout: %v", err)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations"); got != "1" {
			t.Errorf("history rows = %s, want exactly 1 (holder unaffected)", got)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM information_schema.tables WHERE table_name='lt'"); got != "1" {
			t.Errorf("holder effect missing")
		}
	})

	// ------------------------------------------------------------------
	// J. Mixed-runner refusal on v2-integer history (SDK-owned shape).
	// ------------------------------------------------------------------
	t.Run("RefusesV2IntegerHistory", func(t *testing.T) {
		dbURL := newDB(t)
		fixture, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer fixture.Close()
		for _, stmt := range []string{
			`CREATE TABLE _neutron_migrations (
				version INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				checksum TEXT, owner TEXT, format TEXT
			)`,
			`INSERT INTO _neutron_migrations (version, name, owner, format) VALUES (1, 'first', 'nucleus-go-sdk', 'v2')`,
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed v2-integer: %v", err)
			}
		}
		dir := writeMigrations(t, map[string]string{
			"1_first.up.sql": "CREATE TABLE mx (id INT);",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir)
		if code == 0 {
			t.Fatal("CLI accepted an SDK-shaped v2 history")
		}
		if !strings.Contains(out, "integer") {
			t.Errorf("refusal must name the shape: %s", out)
		}
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM information_schema.tables WHERE table_name='mx'"); got != "0" {
			t.Errorf("refusal must precede mutations")
		}
	})

	// ------------------------------------------------------------------
	// K. v2-text history with a NULL-format row: refuse-until-adopted
	// (review-1 R1). The state is hand-crafted — no runner produces it —
	// and must be refused pre-mutation like the SDKs' verifyHistory rule,
	// never silently treated as unverified (NULL-checksum) history.
	// ------------------------------------------------------------------
	t.Run("RefusesV2TextRowWithNullFormat", func(t *testing.T) {
		dbURL := newDBWithPrefix(t, "neutron_orm_m04rw")
		up1 := "CREATE TABLE rw1 (id INT);"
		fixture, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer fixture.Close()
		for _, stmt := range []string{
			`CREATE TABLE _neutron_migrations (
				version TEXT PRIMARY KEY,
				name TEXT NOT NULL,
				applied_at TIMESTAMPTZ DEFAULT now(),
				checksum TEXT, owner TEXT, format TEXT
			)`,
			fmt.Sprintf(`INSERT INTO _neutron_migrations (version, name, checksum, owner, format)
				VALUES ('001', 'init', '%s', 'neutron-cli', 'v2')`, db.MigrationChecksum(up1)),
			`INSERT INTO _neutron_migrations (version, name) VALUES ('002', 'gone')`,
		} {
			if err := fixture.Exec(context.Background(), stmt); err != nil {
				t.Fatalf("seed v2-text with NULL-format row: %v", err)
			}
		}

		dir := writeMigrations(t, map[string]string{
			"001_init.up.sql": up1,
			"009_new.up.sql":  "CREATE TABLE rw9 (id INT);",
		})

		// The reviewer's exact scenario: stamped 001 + NULL-format 002 (no
		// file) + pending 009. Refusal names the row and adoption.
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir)
		if code == 0 {
			t.Fatal("pending 009 applied despite a NULL-format history row")
		}
		for _, want := range []string{"002", "adopt"} {
			if !strings.Contains(out, want) {
				t.Errorf("refusal must name %q: %s", want, out)
			}
		}
		// Database unchanged by the refusal.
		if got := adminQuery(t, dbURL, "SELECT count(*) FROM _neutron_migrations"); got != "2" {
			t.Errorf("history rows = %s, want 2 (refusal must precede mutations)", got)
		}
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM _neutron_migrations WHERE version='002' AND format IS NULL"); got != "1" {
			t.Errorf("NULL-format row must be untouched by the refusal")
		}
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM information_schema.tables WHERE table_name='rw9'"); got != "0" {
			t.Errorf("pending 009 must not run")
		}

		// The directive is actionable: adoption stamps the row unverified,
		// after which the pending migration applies.
		code, out = runCLI(t, dbURL, "migrate", "adopt", "--dir", dir)
		if code != 0 {
			t.Fatalf("adopt failed: %s", out)
		}
		if !strings.Contains(out, "UNVERIFIED") {
			t.Errorf("NULL-format row must adopt as unverified: %s", out)
		}
		if code, out = runCLI(t, dbURL, "migrate", "--dir", dir); code != 0 {
			t.Fatalf("post-adopt migrate failed: %s", out)
		}
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM information_schema.tables WHERE table_name='rw9'"); got != "1" {
			t.Errorf("rw9 missing after post-adopt migrate")
		}
		if got := adminQuery(t, dbURL,
			"SELECT count(*) FROM _neutron_migrations WHERE version='002' AND format='v2' AND checksum IS NULL"); got != "1" {
			t.Errorf("adopted 002 must be stamped v2 with a NULL checksum")
		}
	})
}

// advisoryLockPredicate matches our advisory lock in pg_locks: a single
// bigint key is stored as its two 32-bit halves (classid, objid).
const advisoryLockPredicate = "locktype = 'advisory' AND classid = 1639946364 AND objid = 284230823"

// runCLIProcess runs the built binary and returns (exit code, output).
func runCLIProcess(t *testing.T, bin, dbURL string, args ...string) (int, string) {
	t.Helper()
	full := append([]string{"--url", dbURL}, args...)
	cmd := exec.Command(bin, full...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), string(out)
		}
		t.Fatalf("run CLI %v: %v", args, err)
	}
	return 0, string(out)
}
