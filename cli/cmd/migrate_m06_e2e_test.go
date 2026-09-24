package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestMigrateJournaledE2E exercises the M06 journaled operational-migration
// contract through the REAL CLI binary against REAL disposable Postgres
// databases (prefix m06_): the shipped expand→backfill→contract example
// set end to end (seeded through the existing seed command), bounded/
// resumable/idempotent backfills interrupted at each step's kill window
// (during a statement, after a checkpoint commit, all-effects-unrecorded)
// with safe recovery via resolve/status/plain migrate, lock_timeout and
// statement_timeout failing honestly and reconciling, the concurrent-index
// INVALID-debris lifecycle (drop + rebuild, judged against REINDEX), and
// down honesty for irreversible data operations.
//
// Fail-before vectors run against the pre-M06 binary built from HEAD via
// read-only git archive. State is asserted with raw SQL, not command
// output. Skipped unless NEUTRON_E2E_DATABASE_URL is set (a failure when
// NEUTRON_LIVE_REQUIRED=1).
func TestMigrateJournaledE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; M06 journaled E2E skipped (set it to a disposable Postgres URL to run)")
	}

	bin := buildCLIBinary(t)
	// The fail-before reference is the M05 landing revision (the last
	// pre-M06 tree), pinned: after M06 lands, HEAD would contain the
	// journal and the premises could never hold. Same pattern as the M05
	// battery's pinned pre-M05 reference.
	preM06Bin := buildRevisionCLIBinary(t, parentOfLanding(t, "(orm-program M06)"))

	newDB := func(t *testing.T) string {
		t.Helper()
		dbName := fmt.Sprintf("m06_%d_%d", os.Getpid(), time.Now().UnixNano())
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
			// Same bounded-retry cleanup as the M05 harness (its review-8
			// LOW-1 mitigation): terminate + drop under a 45s window with
			// one retry — harness robustness under load, never product
			// behavior.
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

	// copyExamplesSrc copies the shipped example set (runbook flow runs the
	// exact tracked files).
	copyExamplesSrc := func(t *testing.T) (migrations, seeds string) {
		t.Helper()
		_, thisFile, _, ok := runtime.Caller(0)
		if !ok {
			t.Fatal("cannot locate test source path")
		}
		src := filepath.Join(filepath.Dir(thisFile), "..", "examples-src")
		dst := t.TempDir()
		if out, err := exec.Command("cp", "-R", src+string(os.PathSeparator), dst).CombinedOutput(); err != nil {
			t.Fatalf("copy examples-src: %v\n%s", err, out)
		}
		return filepath.Join(dst, "migrations"), filepath.Join(dst, "seeds")
	}

	// holdRowLock opens a transaction holding row locks on the id range so
	// a journaled UPDATE targeting those rows blocks (lock window).
	holdRowLock := func(t *testing.T, dbURL, table string, lo, hi int) (release func()) {
		t.Helper()
		pool, err := pgxpool.New(context.Background(), dbURL)
		if err != nil {
			t.Fatalf("lock helper connect: %v", err)
		}
		conn, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("lock helper acquire: %v", err)
		}
		tx, err := conn.Begin(context.Background())
		if err != nil {
			t.Fatalf("lock helper begin: %v", err)
		}
		if _, err := tx.Exec(context.Background(),
			fmt.Sprintf("SELECT * FROM %s WHERE id >= %d AND id <= %d FOR UPDATE", table, lo, hi)); err != nil {
			tx.Rollback(context.Background())
			t.Fatalf("lock helper select-for-update: %v", err)
		}
		return func() {
			tx.Rollback(context.Background())
			conn.Release()
			pool.Close()
		}
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

	// waitUntil polls a scalar SQL condition until it holds.
	waitUntil := func(t *testing.T, dbURL, sql, want string) bool {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if query(t, dbURL, sql) == want {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	// ------------------------------------------------------------------
	// A. The shipped example set, end to end, seeded through the EXISTING
	//    seed command: expand -> bounded backfill -> contract -> concurrent
	//    index swap, with idempotent re-runs and bounds asserted.
	// ------------------------------------------------------------------
	t.Run("ExamplesRunbookEndToEnd", func(t *testing.T) {
		dbURL := newDB(t)
		migrations, seeds := copyExamplesSrc(t)

		// The runbook's first step IS the seed command (reused, not
		// duplicated): the demo table and its 1000 rows come from it.
		if code, out := runCLI(t, dbURL, "seed", "-f", filepath.Join(seeds, "orders.sql")); code != 0 {
			t.Fatalf("neutron seed (the existing command) failed (%d): %s", code, out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM orders`); got != "1000" {
			t.Fatalf("seeded rows = %s, want 1000", got)
		}
		// Seed idempotency holds against the schema it owns (pre-migrate);
		// after the contract migration the schema has legitimately moved
		// past the seed, so re-seeding then is not part of the flow.
		if code, out := runCLI(t, dbURL, "seed", "-f", filepath.Join(seeds, "orders.sql")); code != 0 {
			t.Fatalf("re-seed failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM orders`); got != "1000" {
			t.Fatalf("re-seed duplicated rows: %s", got)
		}

		// --allow-destructive: 004 drops the superseded legacy index, and
		// index drops carry the destructive acknowledgement like any other.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", migrations, "--timeout", "120s", "--allow-destructive"); code != 0 {
			t.Fatalf("example migrate failed (%d): %s", code, out)
		}
		// Independent oracle: every row carries the derived total, the
		// constraint is contracted, the concurrent index is valid.
		if got := query(t, dbURL, `SELECT count(*) FROM orders WHERE total_cents IS DISTINCT FROM unit_price_cents * qty`); got != "0" {
			t.Fatalf("backfilled values wrong: %s row(s) deviate", got)
		}
		if got := query(t, dbURL, `SELECT attnotnull::text FROM pg_attribute WHERE attrelid = 'orders'::regclass AND attname = 'total_cents'`); got != "true" {
			t.Fatalf("contract not applied: total_cents attnotnull = %s", got)
		}
		if got := query(t, dbURL, `SELECT i.indisvalid::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'orders_customer_idx'`); got != "true" {
			t.Fatalf("concurrent index not valid: %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "4" {
			t.Fatalf("history rows = %s, want 4", got)
		}

		// Bounds respected: the backfill's four steps each report at most
		// 250 rows, summing to exactly 1000.
		code, out := runCLI(t, dbURL, "migrate", "--dir", migrations, "--timeout", "120s", "--allow-destructive")
		if code != 0 || !strings.Contains(out, "up to date") {
			t.Fatalf("repeat migrate must be a no-op (code %d): %s", code, out)
		}
	})

	// ------------------------------------------------------------------
	// B. Backfill interruption at the kill windows: during a statement
	//    (rolled back) and after a checkpoint commit (durable), with
	//    per-step recovery through resolve.
	// ------------------------------------------------------------------
	t.Run("BackfillKillWindowsAndResume", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE bk (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL, `INSERT INTO bk SELECT g, NULL FROM generate_series(1, 300) g`)

		backfill := func() string {
			return writeMigrations(t, map[string]string{
				"001_bf.up.sql": "-- neutron:journaled\n" +
					"-- neutron:step verify=\"SELECT count(*) FROM bk WHERE id <= 100 AND v IS NULL\" expect=\"0\" lock_timeout=60s\n" +
					"UPDATE bk SET v = id WHERE id <= 100 AND v IS NULL;\n" +
					"-- neutron:step verify=\"SELECT count(*) FROM bk WHERE id > 100 AND id <= 200 AND v IS NULL\" expect=\"0\" lock_timeout=60s\n" +
					"UPDATE bk SET v = id WHERE id > 100 AND id <= 200 AND v IS NULL;\n" +
					"-- neutron:step verify=\"SELECT count(*) FROM bk WHERE id > 200 AND v IS NULL\" expect=\"0\" lock_timeout=60s\n" +
					"UPDATE bk SET v = id WHERE id > 200 AND v IS NULL;\n",
				"001_bf.down.sql": "-- IRREVERSIBLE: rewrote row values\n",
			})
		}

		// Window 0 — BEFORE any step takes effect: every row locked, so
		// step 1 blocks on its very first statement; the kill lands
		// before any checkpoint exists. Nothing ran, nothing is
		// recorded, status reports an ordinary pending migration (no
		// false interruption), and a plain migrate completes from
		// scratch.
		dbURL0 := newDB(t)
		execRaw(t, dbURL0, `CREATE TABLE bk (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL0, `INSERT INTO bk SELECT g, NULL FROM generate_series(1, 300) g`)
		dir0 := backfill()
		release0 := holdRowLock(t, dbURL0, "bk", 1, 300)
		proc0 := exec.Command(bin, "--url", dbURL0, "migrate", "--dir", dir0, "--timeout", "180s")
		if err := proc0.Start(); err != nil {
			t.Fatal(err)
		}
		blocked0 := waitUntil(t, dbURL0,
			`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'UPDATE bk%' AND wait_event_type = 'Lock'`, "1")
		if !blocked0 {
			proc0.Process.Kill()
			t.Fatal("kill window 0 (step 1 blocked before any checkpoint) never observed")
		}
		if err := proc0.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = proc0.Wait()
		release0()
		waitForLockReleased(t, dbURL0)
		if got := query(t, dbURL0, `SELECT count(*) FROM bk WHERE v IS NOT NULL`); got != "0" {
			t.Fatalf("after kill-before-any-step: migrated rows = %s, want 0", got)
		}
		// The session bootstrap creates the (empty) history table; ROWS
		// are the evidence of recording, and there must be none.
		if got := query(t, dbURL0, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("kill-before-any-step must record nothing, got %s row(s)", got)
		}
		if code, out := runCLI(t, dbURL0, "migrate", "status", "--dir", dir0); code != 0 || strings.Contains(out, "INTERRUPTED") {
			t.Fatalf("status after kill-before-any-step must report ordinary pending (code %d): %s", code, out)
		}
		if code, out := runCLI(t, dbURL0, "migrate", "--dir", dir0, "--timeout", "120s"); code != 0 {
			t.Fatalf("plain migrate after kill-before-any-step failed: %s", out)
		}
		if got := query(t, dbURL0, `SELECT count(*) FROM bk WHERE v IS DISTINCT FROM id`); got != "0" {
			t.Fatalf("values wrong after from-scratch recovery: %s", got)
		}
		if got := query(t, dbURL0, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after from-scratch recovery = %s, want 1", got)
		}

		// Window 1 — DURING a statement: batch 2 blocks on a held row
		// lock; the kill lands mid-statement, so batch 2 rolls back while
		// batch 1 (committed = the checkpoint write) stays durable.
		release := holdRowLock(t, dbURL, "bk", 101, 200)
		dir := backfill()

		proc := exec.Command(bin, "--url", dbURL, "migrate", "--dir", dir, "--timeout", "180s")
		if err := proc.Start(); err != nil {
			t.Fatal(err)
		}
		blocked := waitUntil(t, dbURL,
			`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'UPDATE bk%' AND wait_event_type = 'Lock'`, "1")
		migrated1 := waitUntil(t, dbURL, `SELECT count(*) FROM bk WHERE id <= 100 AND v IS NOT NULL`, "100")
		if !blocked || !migrated1 {
			proc.Process.Kill()
			t.Fatal("kill window (batch 1 committed + batch 2 blocked on lock) never observed")
		}
		if err := proc.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = proc.Wait()
		// Release the row lock BEFORE waiting on the advisory lock: the
		// killed backend is blocked inside the UPDATE and only notices the
		// dead socket once the lock conflict resolves — holding the row
		// lock would pin the advisory lock until its wait deadline.
		release()
		waitForLockReleased(t, dbURL)

		// Durable state before any retry: batch 1's checkpoint is present,
		// the killed batch is fully rolled back, nothing recorded.
		if got := query(t, dbURL, `SELECT count(*) FROM bk WHERE v IS NOT NULL`); got != "100" {
			t.Fatalf("after kill: migrated rows = %s, want 100 (batch 1 only)", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("history rows after kill = %s, want 0", got)
		}

		// Status names the interrupted state; resolve reports per-step
		// verification (1 satisfied, 2 unsatisfied).
		if code, out := runCLI(t, dbURL, "migrate", "status", "--dir", dir); code != 0 || !strings.Contains(out, "INTERRUPTED") {
			t.Fatalf("status must report INTERRUPTED (code %d): %s", code, out)
		}
		code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--dir", dir)
		if code != 0 {
			t.Fatalf("resolve report failed: %s", out)
		}
		for _, want := range []string{"satisfied", "unsatisfied"} {
			if !strings.Contains(out, want) {
				t.Fatalf("resolve report missing %q: %s", want, out)
			}
		}

		// Recovery: retry — verified batch 1 is skipped, the remainder
		// runs, exactly-once.
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--retry", "--dir", dir, "--timeout", "120s"); code != 0 {
			t.Fatalf("resolve --retry failed: %s", out)
		} else if !strings.Contains(out, "skip") {
			t.Fatalf("retry must report skipping the verified batch: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM bk WHERE v IS DISTINCT FROM id`); got != "0" {
			t.Fatalf("post-retry values wrong: %s row(s) deviate", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after retry = %s, want 1", got)
		}

		// Batch idempotency at the statement level: re-running every batch
		// statement verbatim touches ZERO rows.
		for _, stmt := range []string{
			`UPDATE bk SET v = id WHERE id <= 100 AND v IS NULL`,
			`UPDATE bk SET v = id WHERE id > 100 AND id <= 200 AND v IS NULL`,
			`UPDATE bk SET v = id WHERE id > 200 AND v IS NULL`,
		} {
			c, err := db.Connect(context.Background(), dbURL)
			if err != nil {
				t.Fatal(err)
			}
			n, err := c.ExecTag(context.Background(), stmt)
			c.Close()
			if err != nil {
				t.Fatalf("idempotency probe %q: %v", stmt, err)
			}
			if n != 0 {
				t.Fatalf("second-pass effect: %q affected %d rows, want 0", stmt, n)
			}
		}

		// Window 1b — the same mid-journal kill as window 1 (batches
		// committed, a later statement blocked), recovered by a PLAIN
		// `neutron migrate` instead of resolve: the apply path itself
		// skips the verified batches and finishes the remainder.
		dbURL1b := newDB(t)
		execRaw(t, dbURL1b, `CREATE TABLE bk (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL1b, `INSERT INTO bk SELECT g, NULL FROM generate_series(1, 300) g`)
		dir1b := backfill()
		release1b := holdRowLock(t, dbURL1b, "bk", 201, 300)
		proc1b := exec.Command(bin, "--url", dbURL1b, "migrate", "--dir", dir1b, "--timeout", "180s")
		if err := proc1b.Start(); err != nil {
			t.Fatal(err)
		}
		blocked1b := waitUntil(t, dbURL1b,
			`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'UPDATE bk%' AND wait_event_type = 'Lock'`, "1")
		migrated1b := waitUntil(t, dbURL1b, `SELECT count(*) FROM bk WHERE id <= 200 AND v IS NOT NULL`, "200")
		if !blocked1b || !migrated1b {
			proc1b.Process.Kill()
			t.Fatal("kill window 1b (batches 1-2 committed + batch 3 blocked) never observed")
		}
		if err := proc1b.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = proc1b.Wait()
		release1b()
		waitForLockReleased(t, dbURL1b)
		if got := query(t, dbURL1b, `SELECT count(*) FROM bk WHERE v IS NOT NULL`); got != "200" {
			t.Fatalf("after kill 1b: migrated rows = %s, want 200 (batches 1-2 only)", got)
		}
		if code, out := runCLI(t, dbURL1b, "migrate", "--dir", dir1b, "--timeout", "120s"); code != 0 {
			t.Fatalf("plain-migrate recovery after mid-journal kill failed: %s", out)
		} else if !strings.Contains(out, "skip") {
			t.Fatalf("plain-migrate recovery must skip the verified batches: %s", out)
		}
		if got := query(t, dbURL1b, `SELECT count(*) FROM bk WHERE v IS DISTINCT FROM id`); got != "0" {
			t.Fatalf("values wrong after plain-migrate recovery: %s", got)
		}
		if got := query(t, dbURL1b, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after plain-migrate recovery = %s, want 1", got)
		}

		// Window 2 — after the LAST step's checkpoint, before the history
		// row (the unrecorded-complete state, reproduced as M05's
		// mark-applied battery reproduces its states: the steps' SQL
		// applied by hand, no history). A plain migrate must close it by
		// skipping every verified step and recording history.
		dbURL2 := newDB(t)
		execRaw(t, dbURL2, `CREATE TABLE bk (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL2, `INSERT INTO bk SELECT g, NULL FROM generate_series(1, 300) g`)
		dir2 := backfill()
		for _, stmt := range []string{
			`UPDATE bk SET v = id WHERE id <= 100 AND v IS NULL`,
			`UPDATE bk SET v = id WHERE id > 100 AND id <= 200 AND v IS NULL`,
			`UPDATE bk SET v = id WHERE id > 200 AND v IS NULL`,
		} {
			execRaw(t, dbURL2, stmt)
		}
		if code, out := runCLI(t, dbURL2, "migrate", "status", "--dir", dir2); code != 0 || !strings.Contains(out, "UNRECORDED") {
			t.Fatalf("status must report effects-present-unrecorded (code %d): %s", code, out)
		}
		// FAIL-BEFORE (pre-M06 binary): with no journal the file is plain
		// transactional DML — mark-applied refuses because DML outcomes
		// are not durably knowable.
		if code, out := runCLIProcess(t, preM06Bin, dbURL2, "migrate", "resolve", "001", "--mark-applied", "--dir", dir2); code == 0 {
			t.Fatal("fail-before premise wrong: pre-M06 mark-applied accepted unverifiable DML")
		} else if !strings.Contains(out, "not everything is durably knowable") {
			t.Fatalf("fail-before refusal must name the unknowable state: %s", out)
		}
		// M06: journaled verification makes the state provable — resolve
		// closes it WITHOUT running any SQL...
		if code, out := runCLI(t, dbURL2, "migrate", "resolve", "001", "--mark-applied", "--dir", dir2); code != 0 {
			t.Fatalf("journaled mark-applied over verified effects failed: %s", out)
		}
		if got := query(t, dbURL2, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "1" {
			t.Fatalf("mark-applied row missing")
		}
		// ...and the executor's own skip-verified path closes the same
		// state on a second copy via plain migrate.
		dbURL3 := newDB(t)
		execRaw(t, dbURL3, `CREATE TABLE bk (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL3, `INSERT INTO bk SELECT g, NULL FROM generate_series(1, 300) g`)
		dir3 := backfill()
		for _, stmt := range []string{
			`UPDATE bk SET v = id WHERE id <= 100 AND v IS NULL`,
			`UPDATE bk SET v = id WHERE id > 100 AND id <= 200 AND v IS NULL`,
			`UPDATE bk SET v = id WHERE id > 200 AND v IS NULL`,
		} {
			execRaw(t, dbURL3, stmt)
		}
		if code, out := runCLI(t, dbURL3, "migrate", "--dir", dir3, "--timeout", "120s"); code != 0 {
			t.Fatalf("plain migrate over verified-unrecorded state failed: %s", out)
		} else if !strings.Contains(out, "skip") {
			t.Fatalf("plain migrate must skip verified steps: %s", out)
		}
		if got := query(t, dbURL3, `SELECT count(*) FROM bk WHERE v IS DISTINCT FROM id`); got != "0" {
			t.Fatalf("values disturbed by the no-op close: %s", got)
		}
		if got := query(t, dbURL3, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after no-op close = %s, want 1", got)
		}
	})

	// ------------------------------------------------------------------
	// C. lock_timeout / statement_timeout fail honestly and reconcile.
	// ------------------------------------------------------------------
	t.Run("TimeoutsFailHonestly", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE to1 (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL, `INSERT INTO to1 SELECT g, NULL FROM generate_series(1, 20) g`)

		dir := writeMigrations(t, map[string]string{
			"001_to.up.sql": "-- neutron:journaled\n" +
				"-- neutron:step verify=\"SELECT count(*) FROM to1 WHERE id <= 10 AND v IS NULL\" expect=\"0\"\n" +
				"UPDATE to1 SET v = id WHERE id <= 10 AND v IS NULL;\n" +
				"-- neutron:step verify=\"SELECT count(*) FROM to1 WHERE id > 10 AND v IS NULL\" expect=\"0\" lock_timeout=300ms\n" +
				"UPDATE to1 SET v = id WHERE id > 10 AND v IS NULL;\n",
			"001_to.down.sql": "-- IRREVERSIBLE\n",
		})

		// lock_timeout: hold rows 11-20; step 2 must fail with the
		// server's lock-timeout error (SQLSTATE 55P03), step 1 durable,
		// nothing recorded.
		release := holdRowLock(t, dbURL, "to1", 11, 20)
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "120s")
		release()
		if code == 0 {
			t.Fatal("lock-blocked journaled step reported success")
		}
		for _, want := range []string{"lock timeout", "resolve"} {
			if !strings.Contains(out, want) {
				t.Errorf("lock-timeout failure missing %q: %s", want, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM to1 WHERE v IS NOT NULL`); got != "10" {
			t.Fatalf("lock-timeout reconciliation wrong: %s rows migrated, want 10 (step 1 only)", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("history rows after lock-timeout failure = %s, want 0", got)
		}
		// Recovery completes exactly-once.
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--retry", "--dir", dir, "--timeout", "120s"); code != 0 {
			t.Fatalf("retry after lock-timeout failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM to1 WHERE v IS DISTINCT FROM id`); got != "0" {
			t.Fatalf("values wrong after retry: %s", got)
		}

		// statement_timeout: a slow predicate makes the UPDATE exceed a
		// 300ms budget; the step fails honestly, unapplied.
		execRaw(t, dbURL, `CREATE TABLE to2 (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL, `INSERT INTO to2 SELECT g, NULL FROM generate_series(1, 5) g`)
		dir2 := writeMigrations(t, map[string]string{
			"002_st.up.sql": "-- neutron:journaled\n" +
				"-- neutron:step verify=\"SELECT count(*) FROM to2 WHERE v IS NULL\" expect=\"0\" statement_timeout=300ms\n" +
				"UPDATE to2 SET v = id WHERE v IS NULL AND pg_sleep(1) IS NOT NULL;\n",
			"002_st.down.sql": "-- IRREVERSIBLE\n",
		})
		code, out = runCLI(t, dbURL, "migrate", "--dir", dir2, "--timeout", "120s")
		if code == 0 {
			t.Fatal("statement-timeout-exceeding step reported success")
		}
		if !strings.Contains(out, "statement timeout") {
			t.Fatalf("statement-timeout failure must name the cause: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM to2 WHERE v IS NOT NULL`); got != "0" {
			t.Fatalf("timed-out step must leave no effect, got %s rows", got)
		}
	})

	// ------------------------------------------------------------------
	// D. Concurrent-index INVALID debris lifecycle: failed unique build
	//    leaves debris; journaled retry drops and rebuilds. Fail-before:
	//    the pre-M06 binary refuses the retry (by-hand recovery).
	// ------------------------------------------------------------------
	t.Run("ConcurrentIndexDebrisLifecycle", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE dup (id int PRIMARY KEY, tag int)`)
		execRaw(t, dbURL, `INSERT INTO dup SELECT g, 1 FROM generate_series(1, 5000) g`) // tag all = 1 (duplicates)

		dir := writeMigrations(t, map[string]string{
			"001_uq.up.sql": "-- neutron:journaled\n" +
				"CREATE UNIQUE INDEX CONCURRENTLY dup_tag_uq ON dup (tag);\n",
			"001_uq.down.sql": "DROP INDEX IF EXISTS dup_tag_uq;",
		})

		// The build fails on the duplicates and leaves INVALID debris.
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "120s"); code == 0 {
			t.Fatalf("unique CIC over duplicates reported success: %s", out)
		}
		if got := query(t, dbURL, `SELECT c.relname || ':' || i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'dup_tag_uq'`); got != "dup_tag_uq:false" {
			t.Fatalf("INVALID debris must remain after the failed build, got %q", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("history rows after failed build = %s, want 0", got)
		}

		// FAIL-BEFORE (pre-M06 binary): the same file without journal
		// support refuses the retry — by-hand debris recovery.
		if code, out := runCLIProcess(t, preM06Bin, dbURL, "migrate", "resolve", "001", "--retry", "--dir", dir); code == 0 {
			t.Fatal("fail-before premise wrong: pre-M06 retry accepted INVALID debris")
		} else if !strings.Contains(out, "INVALID concurrent index remains") {
			t.Fatalf("fail-before refusal must name the debris: %s", out)
		}

		// Status reports the invalid state; resolve names it too.
		if code, out := runCLI(t, dbURL, "migrate", "status", "--dir", dir); code != 0 || !strings.Contains(out, "invalid concurrent index") {
			t.Fatalf("status must report the INVALID debris (code %d): %s", code, out)
		}

		// Remove the duplicates, then retry: the journal detects the
		// debris, DROPS it concurrently and rebuilds.
		execRaw(t, dbURL, `UPDATE dup SET tag = id`)
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--retry", "--dir", dir, "--timeout", "120s"); code != 0 {
			t.Fatalf("journaled retry over debris failed: %s", out)
		} else if !strings.Contains(out, "debris") {
			t.Fatalf("retry must report the debris drop: %s", out)
		}
		if got := query(t, dbURL, `SELECT i.indisvalid::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'dup_tag_uq'`); got != "true" {
			t.Fatalf("rebuilt index must be valid, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows after debris retry = %s, want 1", got)
		}
		// The index works (unique holds).
		if err := queryFail(t, dbURL, `UPDATE dup SET tag = 1 WHERE id = 2`); err == nil {
			t.Fatal("rebuilt unique index does not enforce uniqueness")
		}

		// pg_stat_progress_create_index is queryable on this server (the
		// watcher's source; best-effort by contract).
		if got := query(t, dbURL, `SELECT count(*) FROM pg_stat_progress_create_index`); got != "0" {
			t.Fatalf("progress view should be idle, got %s active builds", got)
		}
	})

	// ------------------------------------------------------------------
	// E. Journaled batch bounds are respected and reported.
	// ------------------------------------------------------------------
	t.Run("BackfillBoundsReported", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE bb (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL, `INSERT INTO bb SELECT g, NULL FROM generate_series(1, 300) g`)
		dir := writeMigrations(t, map[string]string{
			"001_bb.up.sql": "-- neutron:journaled\n" +
				"-- neutron:step verify=\"SELECT count(*) FROM bb WHERE id <= 100 AND v IS NULL\" expect=\"0\"\n" +
				"UPDATE bb SET v = id WHERE id <= 100 AND v IS NULL;\n" +
				"-- neutron:step verify=\"SELECT count(*) FROM bb WHERE id > 100 AND id <= 200 AND v IS NULL\" expect=\"0\"\n" +
				"UPDATE bb SET v = id WHERE id > 100 AND id <= 200 AND v IS NULL;\n" +
				"-- neutron:step verify=\"SELECT count(*) FROM bb WHERE id > 200 AND v IS NULL\" expect=\"0\"\n" +
				"UPDATE bb SET v = id WHERE id > 200 AND v IS NULL;\n",
			"001_bb.down.sql": "-- IRREVERSIBLE\n",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "120s")
		if code != 0 {
			t.Fatalf("bounded backfill failed: %s", out)
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "row(s)") && strings.Contains(line, "[run]") {
				fields := strings.Fields(line)
				runAt := -1
				for i, f := range fields {
					if f == "[run]" {
						runAt = i
						break
					}
				}
				if runAt < 0 || runAt+1 >= len(fields) {
					t.Fatalf("cannot locate row count in %q", line)
				}
				rows, err := strconv.Atoi(fields[runAt+1])
				if err != nil {
					t.Fatalf("cannot parse step rows from %q: %v", line, err)
				}
				if rows > 100 {
					t.Fatalf("batch bound violated: step reports %d rows (line %q)", rows, line)
				}
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM bb WHERE v IS NOT NULL`); got != "300" {
			t.Fatalf("total migrated = %s, want 300", got)
		}
	})

	// ------------------------------------------------------------------
	// F. Validation refusals are atomic: malformed journals never run.
	// ------------------------------------------------------------------
	t.Run("JournaledValidationRefusedLive", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE vr (id int PRIMARY KEY, v int)`)
		cases := []struct {
			name string
			up   string
			want string
		}{
			{"dml without verify", "-- neutron:journaled\nUPDATE vr SET v = 1;", "no verifiable effect"},
			{"set local before concurrent step", "-- neutron:journaled\nSET LOCAL lock_timeout = '1s';\nCREATE INDEX CONCURRENTLY vr_i ON vr (id);", "cannot attach to a concurrent-index step"},
			{"unknown directive", "-- neutron:journld\nUPDATE vr SET v = 1;", "unknown journal directive"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				dir := writeMigrations(t, map[string]string{
					"001_vr.up.sql":   tc.up,
					"001_vr.down.sql": "-- IRREVERSIBLE\n",
				})
				code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "60s")
				if code == 0 || !strings.Contains(out, tc.want) {
					t.Fatalf("refusal missing %q (code %d): %s", tc.want, code, out)
				}
				if got := query(t, dbURL, `SELECT count(*) FROM vr WHERE v IS NOT NULL`); got != "0" {
					t.Fatalf("refused migration must leave no effects, got %s rows", got)
				}
				if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE tablename='vr' AND indexname='vr_i'`); got != "0" {
					t.Fatalf("refused migration must create no indexes")
				}
			})
		}
	})

	// ------------------------------------------------------------------
	// G. Unmarked concurrent files keep the exact M05 semantics (regression
	//    pin): DML still refused, with the journal as the taught escape.
	// ------------------------------------------------------------------
	t.Run("UnmarkedConcurrentDmlStillRefused", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE um (id int PRIMARY KEY, v int)`)
		dir := writeMigrations(t, map[string]string{
			"001_um.up.sql": "CREATE INDEX CONCURRENTLY um_i ON um (id);\nUPDATE um SET v = 1;",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "60s")
		if code == 0 || !strings.Contains(out, "data-changing") {
			t.Fatalf("unmarked concurrent DML must stay refused (code %d): %s", code, out)
		}
		if !strings.Contains(out, "neutron:journaled") {
			t.Fatalf("refusal should teach the journal escape: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='um_i'`); got != "0" {
			t.Fatalf("refusal must precede all effects")
		}
	})

	// ------------------------------------------------------------------
	// G2. The journal exemption NEVER weakens protection: journaled DML
	//     on _neutron_ tables stays refused by the same
	//     protected-object guard (regression pin for the M06 surface —
	//     the exemption is from the DML-in-concurrent-file refusal, not
	//     from the guard).
	// ------------------------------------------------------------------
	t.Run("JournaledGuardStillAbsolute", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE _neutron_jv (i int)`)
		execRaw(t, dbURL, `INSERT INTO _neutron_jv VALUES (1)`)
		dir := writeMigrations(t, map[string]string{
			"001_jv.up.sql": "-- neutron:journaled\n" +
				"-- neutron:step verify=\"SELECT count(*) FROM _neutron_jv WHERE i = 2\" expect=\"1\"\n" +
				"UPDATE _neutron_jv SET i = 2;\n",
			"001_jv.down.sql": "-- IRREVERSIBLE\n",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "60s")
		if code == 0 || !strings.Contains(out, "neutron-internal") {
			t.Fatalf("journaled DML on a _neutron_ table must be refused by the protected-object guard (code %d): %s", code, out)
		}
		if got := query(t, dbURL, `SELECT i FROM _neutron_jv`); got != "1" {
			t.Fatalf("refused journaled write must leave the value untouched, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("refused journaled batch must record nothing, got %s row(s)", got)
		}
	})

	// ------------------------------------------------------------------
	// D2. Identity refusals around the debris path: a same-schema name
	//     held by ANOTHER relation's index refuses the step BEFORE
	//     execution (the runner never drops or runs over another
	//     relation's object), and an extension-member index on the
	//     step's own table is never dropped by the runner-internal
	//     debris recovery.
	// ------------------------------------------------------------------
	t.Run("DebrisIdentityRefused", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE di_own (id int PRIMARY KEY)`)
		execRaw(t, dbURL, `CREATE TABLE di_other (tag int)`)
		execRaw(t, dbURL, `INSERT INTO di_own SELECT g FROM generate_series(1, 10) g`)
		execRaw(t, dbURL, `INSERT INTO di_other SELECT 1 FROM generate_series(1, 200) g`)

		// A doomed unique concurrent build leaves INVALID di_x attached
		// to di_other (the fixture's failure IS the point).
		tolerantExec(t, dbURL, `CREATE UNIQUE INDEX CONCURRENTLY di_x ON di_other (tag)`)
		if got := query(t, dbURL, `SELECT i.indisvalid::text FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname='di_x'`); got != "false" {
			t.Fatalf("fixture wrong: di_x must be INVALID debris, got valid=%s", got)
		}

		dir := writeMigrations(t, map[string]string{
			"001_di.up.sql":   "-- neutron:journaled\nCREATE UNIQUE INDEX CONCURRENTLY di_x ON di_own (id);\n",
			"001_di.down.sql": "DROP INDEX IF EXISTS di_x;",
		})
		// Apply AND retry must both refuse BEFORE EXECUTION — the name is
		// taken in the resolved schema by another relation's index, and
		// the runner never drops or runs over another relation's object.
		for _, args := range [][]string{
			{"migrate", "--dir", dir, "--timeout", "120s"},
			{"migrate", "resolve", "001", "--retry", "--dir", dir, "--timeout", "120s"},
		} {
			code, out := runCLI(t, dbURL, args...)
			if code == 0 || !strings.Contains(out, "di_other") || !strings.Contains(out, "already taken in schema") {
				t.Fatalf("index identity must refuse the same-schema name collision and name the holder (args %v, code %d): %s", args, code, out)
			}
			if strings.Contains(out, "the statement ran") {
				t.Fatalf("pre-execution refusal must not claim the statement ran (args %v): %s", args, out)
			}
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname='di_x' AND NOT i.indisvalid`); got != "1" {
			t.Fatalf("the foreign INVALID debris must remain, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE tablename='di_own' AND indexname='di_x'`); got != "0" {
			t.Fatalf("di_own must not carry the index, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("refused debris batch must record nothing, got %s row(s)", got)
		}

		// Extension-member debris: this state is not constructible
		// through supported SQL (indexes cannot be ALTER EXTENSION
		// ADDed and extension scripts never ship invalid indexes), so
		// the fixture forces the exact catalog rows on this disposable
		// database — requiring the cluster superuser; skipped visibly
		// otherwise (the name-collision refusal above already pins the
		// identity gate).
		if got := query(t, dbURL, `SELECT rolsuper::text FROM pg_roles WHERE rolname = current_user`); got != "true" {
			t.Skipf("extension-member debris fixture needs catalog write access (current_user rolsuper=%s); identity gate covered by the name-collision vector above", got)
		}
		execRaw(t, dbURL, `CREATE EXTENSION IF NOT EXISTS cube`)
		execRaw(t, dbURL, `CREATE TABLE di_ext (i int)`)
		execRaw(t, dbURL, `CREATE INDEX di_m ON di_ext (i)`)
		execRaw(t, dbURL, `INSERT INTO pg_depend (classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype)
			VALUES ('pg_class'::regclass, 'di_m'::regclass, 0, 'pg_extension'::regclass, (SELECT oid FROM pg_extension WHERE extname='cube'), 0, 'e')`)
		execRaw(t, dbURL, `UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'di_m'::regclass`)
		dirM := writeMigrations(t, map[string]string{
			"002_dm.up.sql":   "-- neutron:journaled\nCREATE INDEX CONCURRENTLY di_m ON di_ext (i);\n",
			"002_dm.down.sql": "DROP INDEX IF EXISTS di_m;",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dirM, "--timeout", "120s")
		if code == 0 || !strings.Contains(out, "extension member") {
			t.Fatalf("extension-member debris must be refused with the extension reason (code %d): %s", code, out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_depend d ON d.objid=c.oid AND d.deptype='e' WHERE c.relname='di_m'`); got != "1" {
			t.Fatalf("the extension-member debris must remain, got %s", got)
		}
	})

	// ------------------------------------------------------------------
	// D3. Index identity is schema+table+name everywhere (M06 rework,
	//     review-1 MAJOR-1/MAJOR-2): foreign-schema same-name indexes
	//     never satisfy, block or collide — the step BUILDS; a
	//     legitimately-satisfied qualified index still skips; an
	//     unresolvable unqualified table refuses the step precisely.
	// ------------------------------------------------------------------
	t.Run("IndexIdentityFullNameMatching", func(t *testing.T) {
		// MAJOR-1 vector — the reviewer's exact repro: a VALID same-name
		// index in a foreign schema on a different table. The old
		// name-only probe SKIPPED the step and recorded history with the
		// intended index never built; identity must RUN and build.
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE SCHEMA ii_other`)
		execRaw(t, dbURL, `CREATE TABLE ii_other.different (id int)`)
		execRaw(t, dbURL, `CREATE INDEX ii_twin ON ii_other.different (id)`)
		execRaw(t, dbURL, `CREATE TABLE ii_t (id int)`)
		dir := writeMigrations(t, map[string]string{
			"001_ii.up.sql":   "-- neutron:journaled\nCREATE INDEX CONCURRENTLY ii_twin ON ii_t (id);\n",
			"001_ii.down.sql": "DROP INDEX CONCURRENTLY IF EXISTS ii_twin;\n",
		})
		code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "120s")
		if code != 0 {
			t.Fatalf("foreign-schema same-name twin must BUILD, not fail: %s", out)
		}
		if !strings.Contains(out, "[run]") || strings.Contains(out, "[skip]") {
			t.Fatalf("foreign-schema twin must RUN (never a name-only skip): %s", out)
		}
		if got := query(t, dbURL, `SELECT i.indisvalid::text FROM pg_index i WHERE i.indexrelid = 'public.ii_twin'::regclass AND i.indrelid = 'public.ii_t'::regclass`); got != "true" {
			t.Fatalf("index on public.ii_t must exist and be valid, got %q", got)
		}
		if got := query(t, dbURL, `SELECT i.indisvalid::text FROM pg_index i WHERE i.indexrelid = 'ii_other.ii_twin'::regclass`); got != "true" {
			t.Fatalf("foreign twin must be untouched, got valid=%s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "1" {
			t.Fatalf("history rows = %s, want 1 (a real effect was recorded)", got)
		}

		// MAJOR-2 vector — INVALID debris on a same-name table in a
		// foreign schema. The old path dropped unqualified (a search_path
		// no-op), CREATED successfully, then 21000'd the postcondition
		// probe and left an unwedgeable unrecorded effect with a false
		// "nothing ran" verdict. Identity evaluates (public, ii2_t,
		// ii2_twin): the foreign debris is irrelevant, the build simply
		// completes.
		execRaw(t, dbURL, `CREATE TABLE ii2_t_src (id int)`)
		execRaw(t, dbURL, `CREATE SCHEMA ii2_other`)
		execRaw(t, dbURL, `CREATE TABLE ii2_other.ii2_t (id int)`)
		execRaw(t, dbURL, `INSERT INTO ii2_other.ii2_t SELECT 1 FROM generate_series(1, 50)`)
		tolerantExec(t, dbURL, `CREATE UNIQUE INDEX CONCURRENTLY ii2_twin ON ii2_other.ii2_t (id)`)
		if got := query(t, dbURL, `SELECT i.indisvalid::text FROM pg_index i WHERE i.indexrelid = 'ii2_other.ii2_twin'::regclass`); got != "false" {
			t.Fatalf("fixture wrong: foreign debris must be INVALID, got %s", got)
		}
		execRaw(t, dbURL, `CREATE TABLE ii2_t (id int)`)
		dir2 := writeMigrations(t, map[string]string{
			"002_ii.up.sql":   "-- neutron:journaled\nCREATE INDEX CONCURRENTLY ii2_twin ON ii2_t (id);\n",
			"002_ii.down.sql": "DROP INDEX CONCURRENTLY IF EXISTS ii2_twin;\n",
		})
		code, out = runCLI(t, dbURL, "migrate", "--dir", dir2, "--timeout", "120s")
		if code != 0 {
			t.Fatalf("foreign-schema INVALID debris + journaled CIC must complete provably, not wedge: %s", out)
		}
		if strings.Contains(out, "21000") {
			t.Fatalf("identity-aware probes must never 21000 on name twins: %s", out)
		}
		if got := query(t, dbURL, `SELECT i.indisvalid::text FROM pg_index i WHERE i.indexrelid = 'public.ii2_twin'::regclass AND i.indrelid = 'public.ii2_t'::regclass`); got != "true" {
			t.Fatalf("built index must be valid on public.ii2_t, got %q", got)
		}
		// The old false-verbatim state, reproduced honestly: delete the
		// history row (window-2 equivalent) and resolve must REPORT the
		// present effect (complete) — never "nothing ran" — and
		// --mark-applied must close it.
		execRaw(t, dbURL, `DELETE FROM _neutron_migrations WHERE version = '002'`)
		code, out = runCLI(t, dbURL, "migrate", "resolve", "002", "--dir", dir2)
		if code != 0 {
			t.Fatalf("resolve report failed: %s", out)
		}
		if strings.Contains(out, "No effects present") || !strings.Contains(out, "satisfied") || !strings.Contains(out, "unrecorded") {
			t.Fatalf("resolve must report the present effect as complete-unrecorded, never nothing-ran: %s", out)
		}
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "002", "--mark-applied", "--dir", dir2); code != 0 {
			t.Fatalf("mark-applied over the identity-verified effect must close: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version = '002'`); got != "1" {
			t.Fatalf("mark-applied row missing")
		}

		// A legitimately-satisfied QUALIFIED index still skips: apply,
		// remove the history row, plain migrate closes by skipping (and
		// the skip line carries the phrase exactly once).
		execRaw(t, dbURL, `CREATE TABLE ii3_t (id int)`)
		dir3 := writeMigrations(t, map[string]string{
			"003_ii.up.sql":   "-- neutron:journaled\nCREATE INDEX CONCURRENTLY ii3_twin ON public.ii3_t (id);\n",
			"003_ii.down.sql": "DROP INDEX CONCURRENTLY IF EXISTS ii3_twin;\n",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir3, "--timeout", "120s"); code != 0 {
			t.Fatalf("qualified journaled CIC must apply: %s", out)
		}
		execRaw(t, dbURL, `DELETE FROM _neutron_migrations WHERE version = '003'`)
		code, out = runCLI(t, dbURL, "migrate", "--dir", dir3, "--timeout", "120s")
		if code != 0 {
			t.Fatalf("plain migrate over satisfied-qualified state failed: %s", out)
		}
		if !strings.Contains(out, "[skip] verified effect present") || strings.Contains(out, "verified effect present: verified effect present") {
			t.Fatalf("satisfied qualified index must skip with the phrase exactly once: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_index i WHERE i.indexrelid = 'public.ii3_twin'::regclass`); got != "1" {
			t.Fatalf("skip must not rebuild (exactly one index), got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version = '003'`); got != "1" {
			t.Fatalf("history re-recorded = %s, want 1", got)
		}

		// An unresolvable unqualified table REFUSES the step precisely —
		// even when a same-name VALID index exists in the search-path
		// schema on another table (the compound form of the old
		// name-only skip). Nothing runs; the wrapper names the refusal
		// class and never claims the statement ran.
		execRaw(t, dbURL, `CREATE TABLE ii4_decoy (id int)`)
		execRaw(t, dbURL, `CREATE INDEX ii4_twin ON ii4_decoy (id)`)
		dir4 := writeMigrations(t, map[string]string{
			"004_ii.up.sql":   "-- neutron:journaled\nCREATE INDEX CONCURRENTLY ii4_twin ON ii4_missing (id);\n",
			"004_ii.down.sql": "DROP INDEX CONCURRENTLY IF EXISTS ii4_twin;\n",
		})
		code, out = runCLI(t, dbURL, "migrate", "--dir", dir4, "--timeout", "120s")
		if code == 0 || !strings.Contains(out, "REFUSED BEFORE EXECUTION") || !strings.Contains(out, "qualify the table") {
			t.Fatalf("unresolvable unqualified table must refuse precisely (code %d): %s", code, out)
		}
		if strings.Contains(out, "the statement ran") || strings.Contains(out, "[skip]") {
			t.Fatalf("identity refusal must not claim execution or skip: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_index i WHERE i.indrelid = 'public.ii4_decoy'::regclass`); got != "1" {
			t.Fatalf("decoy index must be untouched, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version = '004'`); got != "0" {
			t.Fatalf("refused step must record nothing, got %s row(s)", got)
		}
	})

	// ------------------------------------------------------------------
	// H. Down honesty: reversible structure reverts; the irreversible
	//    backfill stub refuses; the journal marker never rides a down file.
	// ------------------------------------------------------------------
	t.Run("DownHonestyJournaled", func(t *testing.T) {
		dbURL := newDB(t)
		migrations, seeds := copyExamplesSrc(t)
		if code, out := runCLI(t, dbURL, "seed", "-f", filepath.Join(seeds, "orders.sql")); code != 0 {
			t.Fatalf("seed failed: %s", out)
		}
		if code, out := runCLI(t, dbURL, "migrate", "--dir", migrations, "--timeout", "120s", "--allow-destructive"); code != 0 {
			t.Fatalf("migrate failed: %s", out)
		}

		// 004's down (plain DROP INDEX) reverts.
		if code, out := runCLI(t, dbURL, "migrate", "down", "1", "--dir", migrations, "--timeout", "60s"); code != 0 {
			t.Fatalf("revert 004 failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM pg_indexes WHERE indexname='orders_customer_idx'`); got != "0" {
			t.Fatalf("004 down must drop the index")
		}

		// 003's down (DROP NOT NULL) reverts the contract.
		if code, out := runCLI(t, dbURL, "migrate", "down", "1", "--dir", migrations, "--timeout", "60s"); code != 0 {
			t.Fatalf("revert 003 failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT attnotnull::text FROM pg_attribute WHERE attrelid = 'orders'::regclass AND attname = 'total_cents'`); got != "false" {
			t.Fatalf("003 down must drop NOT NULL")
		}

		// 002's down is an IRREVERSIBLE stub: the downgrade refuses
		// honestly, values survive, and 001 stays applied beneath it.
		code, out := runCLI(t, dbURL, "migrate", "down", "1", "--dir", migrations, "--timeout", "60s")
		if code == 0 || !strings.Contains(out, "forward-fix") {
			t.Fatalf("irreversible backfill down must refuse with forward-fix guidance (code %d): %s", code, out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM orders WHERE total_cents IS NOT NULL`); got != "1000" {
			t.Fatalf("refused downgrade must preserve the backfilled values, got %s", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations WHERE version='001'`); got != "1" {
			t.Fatalf("001 must remain applied beneath the refused 002 downgrade")
		}

		// A down file carrying the journal marker is refused (downs run in
		// one transaction; no half-supported semantics). Version 005: the
		// database already carries 001-004 from the example set.
		dir := writeMigrations(t, map[string]string{
			"005_jm.up.sql":   "-- neutron:journaled\nCREATE TABLE jm (id int);",
			"005_jm.down.sql": "-- neutron:journaled\nDROP TABLE jm;",
		})
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "60s"); code != 0 {
			t.Fatalf("journaled create must apply: %s", out)
		}
		if code, out := runCLI(t, dbURL, "migrate", "down", "1", "--dir", dir, "--timeout", "60s"); code == 0 || !strings.Contains(out, "journal") {
			t.Fatalf("journaled down file must be refused with the marker reason (code %d): %s", code, out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM information_schema.tables WHERE table_name='jm'`); got != "1" {
			t.Fatalf("refused down must leave the table")
		}
	})

	// ------------------------------------------------------------------
	// I. The resolve trichotomy extended to operational shapes: journaled
	//    steps are all provable, so provably-nothing-ran refuses --abort,
	//    and partial effects abort through the guarded down SQL.
	// ------------------------------------------------------------------
	t.Run("JournaledResolveAbortTrichotomy", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE ab (id int PRIMARY KEY, v int)`)
		execRaw(t, dbURL, `INSERT INTO ab SELECT g, NULL FROM generate_series(1, 50) g`)
		dir := writeMigrations(t, map[string]string{
			"001_ab.up.sql": "-- neutron:journaled\n" +
				"-- neutron:step verify=\"SELECT count(*) FROM ab WHERE v IS NULL\" expect=\"0\"\n" +
				"UPDATE ab SET v = id WHERE v IS NULL;\n",
			"001_ab.down.sql": "UPDATE ab SET v = NULL WHERE v IS NOT NULL;",
		})

		// Nothing ran and every step PROVES it: abort refuses (a plain
		// migrate is the safe path). M05's equivalent state with
		// unannotated DML could not prove this and allowed abort.
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--abort", "--dir", dir, "--timeout", "60s"); code == 0 {
			t.Fatalf("abort over provably-nothing-ran must refuse: %s", out)
		} else if !strings.Contains(out, "nothing to abort") {
			t.Fatalf("refusal must state nothing-to-abort: %s", out)
		}

		// Partial effects (batch applied by hand = interrupted state):
		// abort runs the guarded down SQL transactionally, history stays
		// empty, and a plain migrate then applies cleanly.
		execRaw(t, dbURL, `UPDATE ab SET v = id WHERE v IS NULL`)
		if code, out := runCLI(t, dbURL, "migrate", "resolve", "001", "--abort", "--dir", dir, "--timeout", "60s"); code != 0 {
			t.Fatalf("abort over partial journaled effects failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM ab WHERE v IS NOT NULL`); got != "0" {
			t.Fatalf("abort must reverse the journaled effect, got %s rows", got)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM _neutron_migrations`); got != "0" {
			t.Fatalf("abort must not write history, rows=%s", got)
		}
		if code, out := runCLI(t, dbURL, "migrate", "--dir", dir, "--timeout", "60s"); code != 0 {
			t.Fatalf("post-abort migrate failed: %s", out)
		}
		if got := query(t, dbURL, `SELECT count(*) FROM ab WHERE v IS DISTINCT FROM id`); got != "0" {
			t.Fatalf("post-abort migrate values wrong: %s", got)
		}
	})

	// ------------------------------------------------------------------
	// J. The progress watcher: pg_stat_progress_create_index projection
	//    is valid on this server, and the watcher terminates cleanly on
	//    cancellation whether or not a build was observed (best effort by
	//    contract — observation is never load-bearing).
	// ------------------------------------------------------------------
	t.Run("ProgressWatcherCleanLifecycle", func(t *testing.T) {
		dbURL := newDB(t)
		execRaw(t, dbURL, `CREATE TABLE pw (id int PRIMARY KEY, pad text)`)
		execRaw(t, dbURL, `INSERT INTO pw SELECT g, repeat('x', 64) FROM generate_series(1, 200000) g`)

		// The watcher's exact projection is queryable (schema contract).
		if got := query(t, dbURL, `SELECT count(*) FROM pg_stat_progress_create_index p WHERE p.command = 'CREATE INDEX'`); got != "0" {
			t.Fatalf("progress view should be idle before any build, got %s", got)
		}

		client, err := db.Connect(context.Background(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		observed := make(chan string, 16)
		done := make(chan struct{})
		go func() {
			defer close(done)
			client.WatchCreateIndexProgress(ctx, "pw", func(p db.IndexBuildProgress) {
				select {
				case observed <- p.Phase:
				default:
				}
			})
		}()

		// A concurrent build gives the watcher something real to see; its
		// observation is not asserted (builds this size can complete
		// between polls) — the contract under test is the clean lifecycle.
		execRaw(t, dbURL, `CREATE INDEX pw_pad_idx ON pw (pad)`)
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("watcher did not terminate after cancellation")
		}
		close(observed)
		for phase := range observed {
			if phase == "" {
				t.Fatal("watcher reported an empty phase")
			}
		}
	})
}

// parentOfLanding returns the parent of the commit on HEAD's history whose
// subject contains marker (e.g. "(orm-program M06)"): the last tree without
// that card. Once a feature lands, HEAD contains it and "fails before"
// could never hold, so fail-before batteries build this revision instead.
// Located by subject rather than a pinned SHA so rebases cannot break it;
// requires full history (CI checks out with fetch-depth: 0).
func parentOfLanding(t *testing.T, marker string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source path")
	}
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	out, err := exec.Command("git", "-C", repoRoot, "log", "--format=%H %s").Output()
	landing := ""
	for _, line := range strings.Split(string(out), "\n") {
		if sha, subject, ok := strings.Cut(line, " "); ok && strings.Contains(subject, marker) {
			landing = sha
			break
		}
	}
	if err != nil || landing == "" {
		t.Fatalf("no commit with %q in HEAD's history (shallow clone? fetch full history): %v", marker, err)
	}
	return landing + "^"
}

// buildRevisionCLIBinary builds the CLI from an arbitrary pinned revision
// via read-only `git archive` (plus the gitignored embedded Studio assets
// copied from the working tree). Fail-before reproduction without touching
// the working tree.
func buildRevisionCLIBinary(t *testing.T, rev string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source path")
	}
	cmdDir := filepath.Dir(thisFile) // .../cli/cmd
	cliDir := filepath.Dir(cmdDir)   // .../cli
	repoRoot := filepath.Dir(cliDir) // repository root
	tmp := t.TempDir()
	archive := exec.Command("git", "-C", repoRoot, "archive", rev, "cli")
	var tarBytes bytes.Buffer
	archive.Stdout = &tarBytes
	archive.Stderr = os.Stderr
	if err := archive.Run(); err != nil {
		t.Fatalf("git archive %s cli: %v", rev, err)
	}
	extract := exec.Command("tar", "-x", "-C", tmp)
	extract.Stdin = &tarBytes
	if out, err := extract.CombinedOutput(); err != nil {
		t.Fatalf("extract %s archive: %v\n%s", rev, err, out)
	}
	// The embedded Studio dist is a gitignored build artifact: copy it in
	// so the pinned binary links exactly like the working-tree one.
	srcDist := filepath.Join(cliDir, "internal", "studio", "dist")
	dstDist := filepath.Join(tmp, "cli", "internal", "studio", "dist")
	if err := filepath.WalkDir(srcDist, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(srcDist, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dstDist, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	}); err != nil {
		t.Fatalf("copy studio dist: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "neutron-pinned")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Join(tmp, "cli")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s CLI: %v\n%s", rev, err, out)
	}
	return bin
}

// queryFail runs a statement expecting failure and returns the error (nil
// on success) — the inverse assertion helper.
func queryFail(t *testing.T, dbURL, sql string) error {
	t.Helper()
	c, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Exec(ctx, sql); err != nil {
		return err
	}
	return nil
}

// tolerantExec runs one fixture statement whose FAILURE is the point (a
// doomed unique concurrent build leaving INVALID debris) and only logs the
// error instead of failing the test.
func tolerantExec(t *testing.T, dbURL, sql string) {
	t.Helper()
	c, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.Exec(ctx, sql); err != nil {
		t.Logf("fixture statement failed as intended: %v", err)
	}
}
