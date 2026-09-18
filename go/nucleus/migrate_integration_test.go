package nucleus

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// Migration behavior that only a live engine can prove: the checksum
// lifecycle (GO-30), the cross-process ledger claim (Consumer-1), and the
// text-format integer contract the ledger depends on (GO-31 — the engine
// pins the same contract server-side in nucleus's
// wire::tests_row_description integer-format test).
//
// Run with a live Nucleus:
//
//	NEUTRON_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55599/nucleus \
//	    go test ./nucleus/ -run Migration -v

func testClient(t *testing.T) *Client {
	t.Helper()

	url := os.Getenv("NEUTRON_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("NEUTRON_TEST_DATABASE_URL not set; skipping database integration test")
	}
	ctx := context.Background()
	client, err := Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func resetMigrationTables(t *testing.T, c *Client) {
	t.Helper()
	ctx := context.Background()
	// The history/lock tables AND the user tables the migration plans below
	// create — a reset that leaves the latter makes the next run's CREATE
	// TABLE fail with "already exists" (the engine's 42P07) once the
	// history saying they were applied is gone.
	for _, ddl := range []string{
		"DROP TABLE IF EXISTS _neutron_migration_lock",
		"DROP TABLE IF EXISTS _neutron_migrations",
		"DROP TABLE IF EXISTS mig_a",
		"DROP TABLE IF EXISTS mig_b",
		"DROP TABLE IF EXISTS legacy_a",
		"DROP TABLE IF EXISTS legacy_b",
		"DROP TABLE IF EXISTS conc_a",
		"DROP TABLE IF EXISTS conc_b",
	} {
		if _, err := c.pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
}

// GO-31 regression: integer results arrive as TEXT under the engine's
// declared text format — ASCII decimal, negatives included — so pgx's native
// decoders round-trip them. This is the client-side half of the contract the
// engine pins byte-for-byte on the wire; the heuristic byte-inspecting
// decoder that used to live here (guessing text vs binary per value) was
// removed once the engine honored its declaration, and this test is what
// notices if either side regresses.
func TestIntegerResultsAreTextRoundTrip(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	resetMigrationTables(t, c)

	if _, err := c.pool.Exec(ctx, migrationsTable); err != nil {
		t.Fatalf("create migrations table: %v", err)
	}
	for _, v := range []int{-123, 0, 7, 2147483647} {
		if _, err := c.pool.Exec(ctx,
			"INSERT INTO _neutron_migrations (version, name) VALUES ($1, 't')",
			sqlParam(v)); err != nil {
			t.Fatalf("insert version %d: %v", v, err)
		}
	}

	// Raw bytes: every version value must be pure ASCII text, or the
	// declared format and the payload disagree again.
	rows, err := c.pool.Query(ctx, "SELECT version FROM _neutron_migrations ORDER BY version")
	if err != nil {
		t.Fatalf("query versions: %v", err)
	}
	defer rows.Close()
	var gotRaw []string
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan raw version: %v", err)
		}
		for _, b := range raw {
			if (b < '0' || b > '9') && b != '-' {
				t.Fatalf("version bytes %q are not text-formatted (byte 0x%02x)", raw, b)
			}
		}
		gotRaw = append(gotRaw, string(raw))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	want := []string{"-123", "0", "7", "2147483647"}
	if strings.Join(gotRaw, ",") != strings.Join(want, ",") {
		t.Errorf("versions = %v, want %v", gotRaw, want)
	}

	// Native typed scan (what appliedVersions does): negative int64s
	// included, since the old heuristic misread "-123" as ~1.7e9 binary.
	var neg int64
	if err := c.pool.QueryRow(ctx, "SELECT version FROM _neutron_migrations WHERE version < 0").
		Scan(&neg); err != nil {
		t.Fatalf("scan negative version: %v", err)
	}
	if neg != -123 {
		t.Errorf("negative version = %d, want -123", neg)
	}
	var big int64
	if err := c.pool.QueryRow(ctx, "SELECT 9223372036854775807").Scan(&big); err != nil {
		t.Fatalf("scan int64 literal: %v", err)
	}
	if big != 9223372036854775807 {
		t.Errorf("int64 literal = %d", big)
	}
}

// GO-30: applied history records checksums; modified applied migrations are
// refused; legacy rows (no checksum column content) are baselined silently.
func TestMigrateChecksumLifecycle(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	resetMigrationTables(t, c)

	plan := []Migration{
		{Version: 1, Name: "first", Up: "CREATE TABLE mig_a (id INT)", Down: "DROP TABLE mig_a"},
		{Version: 2, Name: "second", Up: "CREATE TABLE mig_b (id INT)", Down: "DROP TABLE mig_b"},
	}
	if err := c.Migrate(ctx, plan); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Both applied, both checksummed with the deterministic digest.
	applied, err := c.appliedVersions(ctx)
	if err != nil {
		t.Fatalf("applied versions: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("applied = %d rows, want 2", len(applied))
	}
	for _, m := range plan {
		rec, ok := applied[m.Version]
		if !ok {
			t.Fatalf("version %d not applied", m.Version)
		}
		if rec.checksum == nil {
			t.Fatalf("version %d recorded without checksum", m.Version)
		}
		if *rec.checksum != migrationChecksum(m) {
			t.Errorf("version %d checksum = %s, want %s", m.Version, *rec.checksum, migrationChecksum(m))
		}
	}

	// Re-running is a no-op that still verifies checksums.
	if err := c.Migrate(ctx, plan); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}

	// A modified applied migration is refused — the checksum is enforced,
	// not decorative.
	tampered := make([]Migration, len(plan))
	copy(tampered, plan)
	tampered[1].Up = "CREATE TABLE mig_b (id BIGINT)"
	err = c.Migrate(ctx, tampered)
	if err == nil {
		t.Fatal("tampered migration accepted")
	}
	if !strings.Contains(err.Error(), "modified since it was applied") {
		t.Errorf("tamper error = %v", err)
	}

	// Roll back and re-apply cleanly.
	if err := c.MigrateDown(ctx, plan, 1); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	applied, err = c.appliedVersions(ctx)
	if err != nil {
		t.Fatalf("applied after down: %v", err)
	}
	if _, still := applied[2]; still {
		t.Error("version 2 still applied after MigrateDown")
	}
	if err := c.Migrate(ctx, plan); err != nil {
		t.Fatalf("re-apply after down: %v", err)
	}
}

// GO-30 baseline policy: a history table written by an older client (no
// checksum column, no checksum content) upgrades in place — the column is
// added, existing rows are baselined from the current plan, and nothing
// fails. Drifted legacy rows are accepted by policy: there is no earlier
// recorded content to compare against.
func TestMigrateBaselinesLegacyHistory(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	resetMigrationTables(t, c)

	// Pre-checksum-era table, exactly as old clients created it, with a
	// legacy row whose content only roughly matches the plan.
	legacy := `
CREATE TABLE _neutron_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`
	if _, err := c.pool.Exec(ctx, legacy); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := c.pool.Exec(ctx,
		"INSERT INTO _neutron_migrations (version, name) VALUES (1, 'first')"); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}

	plan := []Migration{
		// "Modified" relative to whatever actually ran in the legacy row —
		// baselining must not care.
		{Version: 1, Name: "first", Up: "CREATE TABLE legacy_a (id INT)", Down: "DROP TABLE legacy_a"},
		{Version: 2, Name: "second", Up: "CREATE TABLE legacy_b (id INT)", Down: "DROP TABLE legacy_b"},
	}
	if err := c.Migrate(ctx, plan); err != nil {
		t.Fatalf("migrate over legacy history: %v", err)
	}

	applied, err := c.appliedVersions(ctx)
	if err != nil {
		t.Fatalf("applied versions: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("applied = %d rows, want 2", len(applied))
	}
	for _, m := range plan {
		rec, ok := applied[m.Version]
		if !ok {
			t.Fatalf("version %d missing", m.Version)
		}
		if rec.checksum == nil {
			t.Fatalf("version %d not baselined", m.Version)
		}
		if *rec.checksum != migrationChecksum(m) {
			t.Errorf("version %d checksum = %s, want plan digest", m.Version, *rec.checksum)
		}
	}

	// And the baselined row is now ENFORCED: modifying it fails.
	tampered := make([]Migration, len(plan))
	copy(tampered, plan)
	tampered[0].Up = "CREATE TABLE legacy_a (id BIGINT)"
	if err := c.Migrate(ctx, tampered); err == nil {
		t.Error("tampered baselined migration accepted")
	}
}

// Consumer-1: the ledger claim serializes migration runners. Exactly one
// acquirer holds the claim; a second waits; a released claim is re-taken;
// a stale claim (holder crashed without releasing) is stolen.
func TestMigrationLedgerLock(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	resetMigrationTables(t, c)

	token, err := c.acquireMigrationLock(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// A second acquisition waits rather than succeeding or erroring.
	waitCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.acquireMigrationLock(waitCtx); err == nil {
		t.Fatal("second acquire succeeded while the claim was held")
	} else if !errorsIsContextDeadline(err) {
		t.Fatalf("second acquire error = %v, want context deadline", err)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Errorf("second acquire returned early (%v) instead of waiting", elapsed)
	}

	// Release drops exactly this token.
	c.releaseMigrationLock(ctx, token)
	var count int
	if err := c.pool.QueryRow(ctx, "SELECT COUNT(*) FROM _neutron_migration_lock").Scan(&count); err != nil {
		t.Fatalf("count lock rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("lock rows after release = %d, want 0", count)
	}

	// Free claim is taken again immediately.
	token2, err := c.acquireMigrationLock(ctx)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if token2 == token {
		t.Error("re-acquisition reused the released token")
	}

	// Stale claim is stolen: age locked_at past the threshold.
	if _, err := c.pool.Exec(ctx,
		"UPDATE _neutron_migration_lock SET locked_at = NOW() - make_interval(secs => 3600) WHERE id = 1"); err != nil {
		t.Fatalf("age lock row: %v", err)
	}
	stolen, err := c.acquireMigrationLock(ctx)
	if err != nil {
		t.Fatalf("acquire over stale claim: %v", err)
	}
	var holder int64
	if err := c.pool.QueryRow(ctx, "SELECT token FROM _neutron_migration_lock WHERE id = 1").Scan(&holder); err != nil {
		t.Fatalf("read holder: %v", err)
	}
	if holder != stolen {
		t.Errorf("holder token = %d, want the stealing acquirer's %d", holder, stolen)
	}

	// A late release of the OLD claim must not remove the stolen one.
	c.releaseMigrationLock(ctx, token2)
	if err := c.pool.QueryRow(ctx, "SELECT token FROM _neutron_migration_lock WHERE id = 1").Scan(&holder); err != nil {
		t.Fatalf("read holder after late release: %v", err)
	}
	if holder != stolen {
		t.Errorf("late release removed the stolen claim: holder = %d, want %d", holder, stolen)
	}
	c.releaseMigrationLock(ctx, stolen)
}

// Consumer-1, end to end: two clients over separate connection pools
// migrating concurrently both succeed and every version is applied exactly
// once. Within one process the package gate already serializes these; the
// value here is that the ledger claim composes with it (the second runner
// blocks on the ledger, then finds the work done) — the same interleaving
// two PROCESSES will hit, which is the scenario the consumer reported.
func TestConcurrentMigrateBothClientsSucceed(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	resetMigrationTables(t, c)

	url := os.Getenv("NEUTRON_TEST_DATABASE_URL")
	c2, err := Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect second client: %v", err)
	}
	t.Cleanup(c2.Close)

	plan := []Migration{
		{Version: 1, Name: "one", Up: "CREATE TABLE conc_a (id INT)", Down: "DROP TABLE conc_a"},
		{Version: 2, Name: "two", Up: "CREATE TABLE conc_b (id INT)", Down: "DROP TABLE conc_b"},
	}

	type result struct {
		name string
		err  error
	}
	results := make(chan result, 2)
	for _, client := range []*Client{c, c2} {
		go func(cl *Client) {
			err := cl.Migrate(ctx, plan)
			results <- result{cl.pool.Config().ConnConfig.Database, err}
		}(client)
	}
	for i := 0; i < 2; i++ {
		if r := <-results; r.err != nil {
			t.Fatalf("concurrent migrate (%s): %v", r.name, r.err)
		}
	}

	applied, err := c.appliedVersions(ctx)
	if err != nil {
		t.Fatalf("applied versions: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("applied = %d rows, want exactly 2 (each version once)", len(applied))
	}
}

func errorsIsContextDeadline(err error) bool {
	return err != nil && (err == context.DeadlineExceeded || strings.Contains(err.Error(), "context deadline exceeded"))
}
