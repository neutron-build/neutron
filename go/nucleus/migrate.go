package nucleus

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// sqlParam converts a value to a string for use as a pgwire query parameter.
// Nucleus pgwire reports TEXT (OID 25) for all parameter slots, so pgx
// must send values as strings. This helper ensures int/int64 values are
// properly converted.
func sqlParam(v any) string {
	switch val := v.(type) {
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Migration represents a database migration with up and down SQL.
type Migration struct {
	Version int
	Name    string
	Up      string
	Down    string
}

// MigrationRecord represents a completed migration stored in the database.
type MigrationRecord struct {
	Version   int
	Name      string
	AppliedAt time.Time
}

const migrationsTable = `
CREATE TABLE IF NOT EXISTS _neutron_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum    TEXT
)`

// migrationsAddChecksum upgrades history tables created before the checksum
// column existed (GO-30). Nullable on purpose: rows applied by older clients
// have no checksum until Migrate baselines them on the next run.
const migrationsAddChecksum = `
ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS checksum TEXT`

// migrationLockTable is the cross-process claim one migration runner holds
// for a database (Consumer-1). A single fixed row (id = 1); holding it means
// having your token in it.
const migrationLockTable = `
CREATE TABLE IF NOT EXISTS _neutron_migration_lock (
    id        INTEGER PRIMARY KEY,
    token     BIGINT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`

// migrationLockStaleAfter is how long a claim may go unrefreshed before a
// waiting runner may steal it — the crash-safety valve for a holder that
// died without releasing. Generous on purpose: a steal that fires while the
// holder is alive (one very slow migration statement) would let two runners
// interleave, which is exactly what the lock exists to prevent. Migrate
// refreshes locked_at after every applied migration, so a live runner only
// approaches the threshold on a single statement that takes this long.
const migrationLockStaleAfter = 10 * time.Minute

// migrationGate serializes migration runners within this process (GO-29):
// Migrate/MigrateDown used to read applied versions OUTSIDE each migration
// transaction, so two concurrent callers both saw a version absent, both
// ran its Up SQL, then fought over the unique history INSERT
// (consumer-observed SQLSTATE 23505) — with the migration work possibly
// executed twice. Holding this gate across the WHOLE operation (history
// read included) removes the in-process race; the ledger lock below covers
// runners in DIFFERENT processes.
var migrationGate sync.Mutex

// Advisory-lock verdict (Consumer-1, tested before this design was chosen):
// Nucleus has no advisory locks. pg_advisory_lock does not exist in the
// engine — the only advisory function implemented is pg_advisory_unlock_all,
// an honest no-op for asyncpg's pool reset (nucleus
// src/executor/scalar_fns.rs documents that if locks are ever implemented
// it must start releasing them). A lock cannot be built on a function the
// engine does not have, so cross-process serialization uses the INSERT-first
// ledger claim below, on engine features that DO exist and are regression-
// tested: INSERT ... ON CONFLICT DO NOTHING and conditional UPDATE (the
// engine re-checks UPDATE predicates atomically at apply, so an UPDATE whose
// predicate no longer matches affects zero rows instead of overwriting).

// migrationLockStealSQL transfers a STALE claim: one statement whose WHERE
// both decides staleness (locked_at older than the threshold, measured
// against the server's NOW()) and writes the new token, so the decision and
// the write are atomic. Built from migrationLockStaleAfter at init.
var migrationLockStealSQL = fmt.Sprintf(
	"UPDATE _neutron_migration_lock SET token = $1, locked_at = NOW() "+
		"WHERE id = 1 AND locked_at < NOW() - make_interval(secs => %d)",
	int(migrationLockStaleAfter.Seconds()))

// acquireMigrationLock claims the database's migration runner slot, waiting
// until any other holder releases (or proves stale). The claim is one row:
// an INSERT that conflicts does nothing, so exactly one caller's token lands
// in it. Waiting is a poll loop with capped backoff — the engine has no
// LISTEN-based wake for this table, and the common case (no contention) pays
// one INSERT.
//
// Stale claims (holder crashed without releasing) are stolen by the
// server-side predicate in migrationLockStealSQL, so no client clock is
// involved and the check is atomic with the write: two waiters cannot both
// steal (the loser's UPDATE re-evaluates against the winner's refreshed
// locked_at and matches zero rows), and a holder that refreshes between a
// waiter's polls cannot be stolen from. A compare-and-swap on a previously
// READ token — the obvious alternative — has neither property here: the
// token does not change on refresh, so a stale read could steal from a live
// holder, and reading holder state means scanning engine timestamps, which
// the wire layer declares as text (pgx refuses them as *time.Time).
//
// The returned token identifies this claim; releaseMigrationLock deletes
// only the row carrying it, so a late release never removes a successor's
// stolen claim.
func (c *Client) acquireMigrationLock(ctx context.Context) (int64, error) {
	if _, err := c.pool.Exec(ctx, migrationLockTable); err != nil {
		return 0, fmt.Errorf("nucleus: create migration lock table: %w", err)
	}

	var token int64
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("nucleus: migration lock token: %w", err)
	}
	for _, b := range buf {
		token = token<<8 | int64(b)
	}

	delay := 25 * time.Millisecond
	const maxDelay = 2 * time.Second
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		ct, err := c.pool.Exec(ctx,
			"INSERT INTO _neutron_migration_lock (id, token) VALUES (1, $1) ON CONFLICT (id) DO NOTHING",
			sqlParam(token))
		if err != nil {
			return 0, fmt.Errorf("nucleus: claim migration lock: %w", err)
		}
		if ct.RowsAffected() == 1 {
			return token, nil
		}

		// Held by someone: steal it if — and only if — the claim is stale.
		ct, err = c.pool.Exec(ctx, migrationLockStealSQL, sqlParam(token))
		if err != nil {
			return 0, fmt.Errorf("nucleus: steal stale migration lock: %w", err)
		}
		if ct.RowsAffected() == 1 {
			return token, nil
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(delay):
		}
		if delay < maxDelay {
			delay *= 2
		}
	}
}

// releaseMigrationLock drops the claim if — and only if — the row still
// carries this token. Failures are deliberately not returned to the caller:
// by the time Migrate releases, its work is committed, and an unreleased
// claim self-heals via the stale steal after migrationLockStaleAfter.
func (c *Client) releaseMigrationLock(ctx context.Context, token int64) {
	_, _ = c.pool.Exec(ctx,
		"DELETE FROM _neutron_migration_lock WHERE id = 1 AND token = $1",
		sqlParam(token))
}

// migrationChecksum is the digest recorded per applied version (GO-30):
// version, name, and Up SQL, NUL-separated so no field can bleed into the
// next one. The Down SQL is excluded — rolling back is allowed to evolve
// independently of what was applied.
func migrationChecksum(m Migration) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d\x00%s\x00%s", m.Version, m.Name, m.Up)
	return hex.EncodeToString(h.Sum(nil))
}

// prepareMigrations copies, sorts, and validates the migration plan before
// any SQL runs (GO-30): Migrate/MigrateDown used to sort the CALLER's slice
// in place (mutating shared configuration and racing concurrent reuse), and
// duplicate or nonpositive versions were only discovered mid-run — after
// earlier migrations had already executed.
func prepareMigrations(input []Migration, descending bool) ([]Migration, error) {
	result := make([]Migration, len(input))
	copy(result, input)
	sort.Slice(result, func(i, j int) bool {
		if descending {
			return result[i].Version > result[j].Version
		}
		return result[i].Version < result[j].Version
	})
	seen := make(map[int]struct{}, len(result))
	for _, m := range result {
		if m.Version <= 0 {
			return nil, fmt.Errorf("nucleus: invalid migration version %d (must be positive)", m.Version)
		}
		if strings.TrimSpace(m.Name) == "" {
			return nil, fmt.Errorf("nucleus: migration %d has an empty name", m.Version)
		}
		if strings.TrimSpace(m.Up) == "" {
			return nil, fmt.Errorf("nucleus: migration %d (%s) has empty Up SQL", m.Version, m.Name)
		}
		if _, exists := seen[m.Version]; exists {
			return nil, fmt.Errorf("nucleus: duplicate migration version %d", m.Version)
		}
		seen[m.Version] = struct{}{}
	}
	return result, nil
}

// Migrate runs all pending migrations in order.
//
// Concurrency: runners in one process are serialized by a package gate
// (GO-29); runners in DIFFERENT processes are serialized by the ledger
// claim in _neutron_migration_lock (Consumer-1) — a second runner's Migrate
// blocks until the first releases or its claim goes stale.
//
// Checksums: every applied migration records migrationChecksum in
// _neutron_migrations. History rows written before the checksum column
// existed are baselined on the next run — their checksum is backfilled from
// the CURRENT plan without complaint, because legacy rows cannot be
// re-derived. From then on the checksum is enforced: a migration whose
// content no longer matches its recorded checksum fails Migrate instead of
// silently skipping (the applied version would otherwise mask a modified —
// possibly already-deployed-differently — script forever).
func (c *Client) Migrate(ctx context.Context, migrations []Migration) error {
	migrationGate.Lock()
	defer migrationGate.Unlock()

	plan, err := prepareMigrations(migrations, false)
	if err != nil {
		return err
	}

	lockToken, err := c.acquireMigrationLock(ctx)
	if err != nil {
		return err
	}
	defer c.releaseMigrationLock(context.WithoutCancel(ctx), lockToken)

	// Ensure migrations table exists (with the checksum column, upgrading
	// history tables created by older clients in place).
	if _, err := c.pool.Exec(ctx, migrationsTable); err != nil {
		return fmt.Errorf("nucleus: create migrations table: %w", err)
	}
	if _, err := c.pool.Exec(ctx, migrationsAddChecksum); err != nil {
		return fmt.Errorf("nucleus: add migrations checksum column: %w", err)
	}

	// Get applied versions with their checksums.
	applied, err := c.appliedVersions(ctx)
	if err != nil {
		return err
	}

	for _, m := range plan {
		rec, isApplied := applied[m.Version]
		if isApplied {
			if rec.checksum == nil {
				// Legacy row from before checksums existed: baseline it from
				// the current plan. Drift here is accepted by policy — there
				// is no earlier recorded content to compare against.
				if _, err := c.pool.Exec(ctx,
					"UPDATE _neutron_migrations SET checksum = $1 WHERE version = $2 AND checksum IS NULL",
					migrationChecksum(m), sqlParam(m.Version)); err != nil {
					return fmt.Errorf("nucleus: baseline checksum for migration %d: %w", m.Version, err)
				}
				continue
			}
			if *rec.checksum != migrationChecksum(m) {
				return fmt.Errorf(
					"nucleus: migration %d (%s) has been modified since it was applied: "+
						"recorded checksum sha256:%s does not match the current script (%s) — "+
						"restore the applied script or write a new migration",
					m.Version, m.Name, *rec.checksum, migrationChecksum(m))
			}
			continue
		}

		tx, err := c.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("nucleus: begin tx for migration %d: %w", m.Version, err)
		}

		if _, err := tx.Exec(ctx, m.Up); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("nucleus: migration %d (%s) up: %w", m.Version, m.Name, err)
		}

		if _, err := tx.Exec(ctx, "INSERT INTO _neutron_migrations (version, name, checksum) VALUES ($1, $2, $3)", sqlParam(m.Version), m.Name, migrationChecksum(m)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("nucleus: record migration %d: %w", m.Version, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("nucleus: commit migration %d: %w", m.Version, err)
		}

		// Refresh the claim so a long train of migrations never approaches
		// the stale threshold while still alive.
		_, _ = c.pool.Exec(ctx,
			"UPDATE _neutron_migration_lock SET locked_at = NOW() WHERE id = 1 AND token = $1",
			sqlParam(lockToken))
	}

	return nil
}

// MigrateDown rolls back the specified number of migrations. Serialized by
// the same gates as Migrate (package gate in-process, ledger claim across
// processes).
func (c *Client) MigrateDown(ctx context.Context, migrations []Migration, steps int) error {
	migrationGate.Lock()
	defer migrationGate.Unlock()

	plan, err := prepareMigrations(migrations, true)
	if err != nil {
		return err
	}

	lockToken, err := c.acquireMigrationLock(ctx)
	if err != nil {
		return err
	}
	defer c.releaseMigrationLock(context.WithoutCancel(ctx), lockToken)

	applied, err := c.appliedVersions(ctx)
	if err != nil {
		return err
	}

	rolled := 0
	for _, m := range plan {
		if rolled >= steps {
			break
		}
		if _, isApplied := applied[m.Version]; !isApplied {
			continue
		}
		if m.Down == "" {
			return fmt.Errorf("nucleus: migration %d (%s) has no down SQL", m.Version, m.Name)
		}

		tx, err := c.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("nucleus: begin tx for rollback %d: %w", m.Version, err)
		}

		if _, err := tx.Exec(ctx, m.Down); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("nucleus: migration %d (%s) down: %w", m.Version, m.Name, err)
		}

		if _, err := tx.Exec(ctx, "DELETE FROM _neutron_migrations WHERE version = $1", sqlParam(m.Version)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("nucleus: remove migration record %d: %w", m.Version, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("nucleus: commit rollback %d: %w", m.Version, err)
		}
		rolled++
	}

	return nil
}

// MigrationStatus returns all applied migrations.
func (c *Client) MigrationStatus(ctx context.Context) ([]MigrationRecord, error) {
	rows, err := c.pool.Query(ctx, "SELECT version, name, applied_at FROM _neutron_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("nucleus: migration status: %w", err)
	}
	defer rows.Close()

	var records []MigrationRecord
	for rows.Next() {
		var r MigrationRecord
		if err := rows.Scan(&r.Version, &r.Name, &r.AppliedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// appliedVersion is one _neutron_migrations row as Migrate consumes it.
// checksum is nil for legacy rows written before the column existed.
type appliedVersion struct {
	checksum *string
}

// appliedVersions reads the applied-history map. Integer columns arrive as
// text-formatted ASCII under the engine's declared text format (the wire
// contract is pinned by nucleus's tests_row_description integer-format
// test), so pgx decodes them natively — no per-value byte inspection.
func (c *Client) appliedVersions(ctx context.Context) (map[int]appliedVersion, error) {
	rows, err := c.pool.Query(ctx, "SELECT version, checksum FROM _neutron_migrations")
	if err != nil {
		return nil, fmt.Errorf("nucleus: query applied versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]appliedVersion)
	for rows.Next() {
		var version int
		var rec appliedVersion
		if err := rows.Scan(&version, &rec.checksum); err != nil {
			return nil, fmt.Errorf("nucleus: scan applied version: %w", err)
		}
		applied[version] = rec
	}
	return applied, rows.Err()
}

// LoadMigrations reads migration files from an embedded filesystem.
// Expected file format: {version}_{name}.up.sql and {version}_{name}.down.sql
func LoadMigrations(fsys embed.FS) ([]Migration, error) {
	migMap := make(map[int]*Migration)

	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		base := filepath.Base(path)
		if !strings.HasSuffix(base, ".sql") {
			return nil
		}

		data, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}

		// Parse filename: 001_create_users.up.sql
		var version int
		var name string
		var direction string

		if strings.HasSuffix(base, ".up.sql") {
			direction = "up"
			base = strings.TrimSuffix(base, ".up.sql")
		} else if strings.HasSuffix(base, ".down.sql") {
			direction = "down"
			base = strings.TrimSuffix(base, ".down.sql")
		} else {
			return nil
		}

		parts := strings.SplitN(base, "_", 2)
		if len(parts) < 2 {
			return nil
		}
		version, err = strconv.Atoi(parts[0])
		if err != nil {
			return nil
		}
		name = parts[1]

		m, ok := migMap[version]
		if !ok {
			m = &Migration{Version: version, Name: name}
			migMap[version] = m
		}

		switch direction {
		case "up":
			m.Up = string(data)
		case "down":
			m.Down = string(data)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("nucleus: load migrations: %w", err)
	}

	migrations := make([]Migration, 0, len(migMap))
	for _, m := range migMap {
		migrations = append(migrations, *m)
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}
