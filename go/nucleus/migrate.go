package nucleus

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
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

// MigrationHistoryFormat is the protocol marker recorded in the history
// table's format column (contracts/data/MIGRATIONS.md). NULL marks legacy
// rows awaiting adoption.
const MigrationHistoryFormat = "v2"

// migrationOwner is the runner identity stamped on rows this SDK writes and
// on claims it holds, for diagnostics.
func migrationOwner() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	return fmt.Sprintf("nucleus-go-sdk@%s:%d", host, os.Getpid())
}

const migrationsTable = `
CREATE TABLE IF NOT EXISTS _neutron_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum    TEXT,
    owner       TEXT,
    format      TEXT
)`

// migrationsAddColumns upgrades history tables created by earlier clients in
// place: the checksum column (GO-30 era) and the protocol-v2 owner/format
// columns. Nullable on purpose: rows applied by older clients carry no
// values until explicit adoption graduates them.
const migrationsAddColumns = `
ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS checksum TEXT`

const migrationsAddOwner = `
ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS owner TEXT`

const migrationsAddFormat = `
ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS format TEXT`

// migrationLockTable is the cross-process claim one migration runner holds
// for a database. A single fixed row (id = 1); holding it means having your
// token in it. The owner column identifies the holder for diagnostics, and
// locked_at is a HEARTBEAT, not a lease: nothing ever takes over a claim
// based on its age (contracts/data/MIGRATIONS.md §5).
const migrationLockTable = `
CREATE TABLE IF NOT EXISTS _neutron_migration_lock (
    id        INTEGER PRIMARY KEY,
    token     BIGINT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    owner     TEXT
)`

const migrationLockAddOwner = `
ALTER TABLE _neutron_migration_lock ADD COLUMN IF NOT EXISTS owner TEXT`

// migrationGate serializes migration runners within this process (GO-29):
// Migrate/MigrateDown used to read applied versions OUTSIDE each migration
// transaction, so two concurrent callers both saw a version absent, both
// ran its Up SQL, then fought over the unique history INSERT
// (consumer-observed SQLSTATE 23505) — with the migration work possibly
// executed twice. Holding this gate across the WHOLE operation (history
// read included) removes the in-process race; the ledger claim below covers
// runners in DIFFERENT processes.
var migrationGate sync.Mutex

// Advisory-lock verdict (Consumer-1, tested before this design was chosen):
// Nucleus has no advisory locks. pg_advisory_lock does not exist in the
// engine — the only advisory function implemented is pg_advisory_unlock_all,
// an honest no-op for asyncpg's pool reset. A lock cannot be built on a
// function the engine does not have, so cross-process serialization uses the
// INSERT-first ledger claim below, on engine features that DO exist and are
// regression-tested: INSERT ... ON CONFLICT DO NOTHING and conditional
// UPDATE/DELETE.
//
// Stale-takeover policy (M04): there is NONE. The pre-M04 Go SDK stole
// claims whose heartbeat went unrefreshed for 10 minutes; that policy is
// retired because it cannot distinguish a crashed holder from a holder
// running one very slow migration statement. Recovery from a holder that
// died without releasing is EXPLICIT: an operator with evidence the old
// runner cannot execute calls ForceUnlockMigrations. Old binaries that
// steal remain a mixed-deployment hazard no marker can fix — deployments
// must exclude them during upgrade.

// acquireMigrationLock claims the database's migration runner slot, waiting
// until any other holder RELEASES. The claim is one row: an INSERT that
// conflicts does nothing, so exactly one caller's token lands in it.
// Waiting is a poll loop with capped backoff — the engine has no
// LISTEN-based wake for this table, and the common case (no contention)
// pays one INSERT. ctx cancellation aborts the wait.
//
// A claim held by a dead runner blocks forever by design; the escape hatch
// is ForceUnlockMigrations (diagnose first with MigrationLockInfo).
func (c *Client) acquireMigrationLock(ctx context.Context) (int64, error) {
	if _, err := c.pool.Exec(ctx, migrationLockTable); err != nil {
		return 0, fmt.Errorf("nucleus: create migration lock table: %w", err)
	}
	if _, err := c.pool.Exec(ctx, migrationLockAddOwner); err != nil {
		return 0, fmt.Errorf("nucleus: add migration lock owner column: %w", err)
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
			"INSERT INTO _neutron_migration_lock (id, token, owner) VALUES (1, $1, $2) ON CONFLICT (id) DO NOTHING",
			sqlParam(token), migrationOwner())
		if err != nil {
			return 0, fmt.Errorf("nucleus: claim migration lock: %w", err)
		}
		if ct.RowsAffected() == 1 {
			return token, nil
		}

		select {
		case <-ctx.Done():
			info, _ := c.MigrationLockInfo(context.WithoutCancel(ctx))
			if info.Held {
				return 0, fmt.Errorf(
					"nucleus: migration lock is held by %s (heartbeat %s); no automatic takeover — "+
						"verify that holder cannot run, then call ForceUnlockMigrations: %w",
					info.Owner, info.Heartbeat, ctx.Err())
			}
			return 0, ctx.Err()
		case <-time.After(delay):
		}
		if delay < maxDelay {
			delay *= 2
		}
	}
}

// releaseMigrationLock drops the claim if — and only if — the row still
// carries this token, so a late release never removes a successor's claim.
// Failures are deliberately not returned to the caller: by the time Migrate
// releases, its work is committed, and an unreleased claim is recoverable
// via ForceUnlockMigrations (it does NOT self-heal by timeout anymore).
func (c *Client) releaseMigrationLock(ctx context.Context, token int64) {
	_, _ = c.pool.Exec(ctx,
		"DELETE FROM _neutron_migration_lock WHERE id = 1 AND token = $1",
		sqlParam(token))
}

// MigrationLockInfo reports the current claim for diagnostics: who holds
// it and how fresh the heartbeat is. It is INFORMATIONAL — nothing in this
// SDK acts on staleness.
type MigrationLockInfo struct {
	Held      bool
	Owner     string
	Heartbeat string
}

// MigrationLockInfo reads the current claim, if any. The heartbeat is read
// through a ::text cast because the engine ships timestamptz in a binary
// wire format pgx cannot scan into Go time or string destinations.
func (c *Client) MigrationLockInfo(ctx context.Context) (MigrationLockInfo, error) {
	var info MigrationLockInfo
	var owner, heartbeat *string
	err := c.pool.QueryRow(ctx,
		"SELECT owner, locked_at::text FROM _neutron_migration_lock WHERE id = 1").Scan(&owner, &heartbeat)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return info, nil
		}
		return info, fmt.Errorf("nucleus: read migration lock: %w", err)
	}
	info.Held = true
	if owner != nil {
		info.Owner = *owner
	}
	if heartbeat != nil {
		info.Heartbeat = *heartbeat
	}
	return info, nil
}

// ForceUnlockMigrations deletes the migration claim row unconditionally.
// This is the explicit crash-recovery escape hatch for a holder that died
// without releasing: call it ONLY with evidence the previous holder cannot
// execute (a dead process, a retired deployment). The SDK cannot
// manufacture that evidence, which is exactly why no automatic takeover
// exists. If the holder is alive, the two of you will interleave — the
// thing the lock exists to prevent.
func (c *Client) ForceUnlockMigrations(ctx context.Context) error {
	if _, err := c.pool.Exec(ctx, "DELETE FROM _neutron_migration_lock WHERE id = 1"); err != nil {
		return fmt.Errorf("nucleus: force unlock migrations: %w", err)
	}
	return nil
}

// migrationChecksum is the canonical v2 digest (contracts/data/MIGRATIONS.md
// §3): lowercase-hex SHA-256 over the up SQL text exactly as supplied,
// UTF-8 encoded. Identical inputs produce identical digests in the CLI, the
// Go SDK and the TS SDK. The down SQL is excluded — rolling back is allowed
// to evolve independently of what was applied.
func migrationChecksum(m Migration) string {
	sum := sha256.Sum256([]byte(m.Up))
	return hex.EncodeToString(sum[:])
}

// legacyMigrationChecksum reproduces the pre-M04 Go SDK digest (GO-30 era):
// SHA-256 over version/name/up, NUL-separated. Rows written by those
// clients carry this digest in the checksum column; because version and
// name are known from the row itself, it is re-verifiable during adoption.
// Never written by this SDK.
func legacyMigrationChecksum(version int, name, up string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s", version, name, up)))
	return hex.EncodeToString(sum[:])
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

// ensureMigrationsTable creates or upgrades the history table to carry the
// v2 columns (checksum from the GO-30 era, owner/format from M04).
func (c *Client) ensureMigrationsTable(ctx context.Context) error {
	for _, stmt := range []string{migrationsTable, migrationsAddColumns, migrationsAddOwner, migrationsAddFormat} {
		if _, err := c.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("nucleus: prepare migrations table: %w", err)
		}
	}
	return nil
}

// checkHistoryShape refuses a history this runner must not touch, before
// any mutation: a TEXT version column means the canonical CLI protocol owns
// the database (the SDK's public API carries integer versions and cannot
// represent its text IDs).
func (c *Client) checkHistoryShape(ctx context.Context) error {
	var versionType *string
	err := c.pool.QueryRow(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_name = '_neutron_migrations' AND column_name = 'version'`).Scan(&versionType)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil // table absent: fresh database
		}
		return fmt.Errorf("nucleus: inspect migration history shape: %w", err)
	}
	if versionType == nil {
		return nil
	}
	switch t := strings.ToLower(*versionType); t {
	case "integer", "smallint", "bigint":
		return nil
	case "text", "character varying", "character", "varchar":
		return fmt.Errorf(
			"nucleus: _neutron_migrations.version is a text column — this history belongs to the " +
				"canonical CLI protocol (text IDs); the SDK runner refuses rather than mix formats. " +
				"Use `neutron migrate`, or re-adopt the history with the CLI to move it back to integer versions")
	default:
		return fmt.Errorf("nucleus: _neutron_migrations.version has unsupported type %q", t)
	}
}

// appliedVersion is one _neutron_migrations row as Migrate consumes it.
// checksum/format are nil for legacy rows written before the column existed
// or before protocol v2.
type appliedVersion struct {
	checksum *string
	format   *string
}

// appliedVersions reads the applied-history map. Integer columns arrive as
// text-formatted ASCII under the engine's declared text format (the wire
// contract is pinned by nucleus's tests_row_description integer-format
// test), so pgx decodes them natively.
func (c *Client) appliedVersions(ctx context.Context) (map[int]appliedVersion, error) {
	rows, err := c.pool.Query(ctx, "SELECT version, checksum, format FROM _neutron_migrations")
	if err != nil {
		return nil, fmt.Errorf("nucleus: query applied versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]appliedVersion)
	for rows.Next() {
		var version int
		var rec appliedVersion
		if err := rows.Scan(&version, &rec.checksum, &rec.format); err != nil {
			return nil, fmt.Errorf("nucleus: scan applied version: %w", err)
		}
		applied[version] = rec
	}
	return applied, rows.Err()
}

// verifyHistory refuses rows this runner cannot trust, before any mutation
// (the M04 transition): every row must be protocol v2. Rows with NULL
// format are legacy — TS-SDK history (no checksum) or pre-M04 Go history
// (legacy digest) — and graduate only through explicit adoption. Rows that
// carry a v2 marker are checksum-enforced; adopted-unverified rows (NULL
// checksum, v2 format) are exempt: there is nothing to compare against and
// they are never silently baselined.
func verifyHistory(plan []Migration, applied map[int]appliedVersion) error {
	for _, m := range plan {
		rec, isApplied := applied[m.Version]
		if !isApplied {
			continue
		}
		if rec.format == nil || *rec.format != MigrationHistoryFormat {
			return fmt.Errorf(
				"nucleus: migration %d (%s) is recorded in a legacy history format and must be adopted "+
					"once before this runner continues — call AdoptMigrations (explicit, transactional; "+
					"unprovable rows stay unverified)", m.Version, m.Name)
		}
		if rec.checksum == nil {
			continue // adopted-unverified: exempt, reported by adoption
		}
		if *rec.checksum != migrationChecksum(m) {
			return fmt.Errorf(
				"nucleus: migration %d (%s) has been modified since it was applied: "+
					"recorded checksum sha256:%s does not match the current script (%s) — "+
					"restore the applied script or write a new migration",
				m.Version, m.Name, *rec.checksum, migrationChecksum(m))
		}
	}
	return nil
}

// MigrationAdoptionReport is what one explicit adoption decided, per row.
type MigrationAdoptionReport struct {
	// Verified versions: the recorded legacy digest reproduced from the
	// supplied plan, so the content is proven identical to what ran.
	Verified []int
	// Unverified versions: nothing recorded to verify against (TS legacy,
	// or no matching plan entry). Checksum stays NULL; reported honestly.
	Unverified []int
}

// AdoptMigrations graduates a legacy history into protocol v2 in ONE
// transaction (contracts/data/MIGRATIONS.md §6). It never fabricates trust:
//
//   - a row whose recorded legacy Go SDK digest reproduces from the
//     supplied plan is adopted as VERIFIED and re-recorded under the v2
//     checksum;
//   - a row with no checksum (TS-SDK history) or no matching plan entry is
//     adopted as UNVERIFIED — checksum stays NULL;
//   - a recorded checksum that matches neither digest aborts the adoption:
//     recorded content disagrees with every supplied file.
//
// Supersedes the GO-30 silent baselining, which backfilled checksums from
// the current plan without proof.
func (c *Client) AdoptMigrations(ctx context.Context, migrations []Migration) (*MigrationAdoptionReport, error) {
	migrationGate.Lock()
	defer migrationGate.Unlock()

	plan, err := prepareMigrations(migrations, false)
	if err != nil {
		return nil, err
	}

	lockToken, err := c.acquireMigrationLock(ctx)
	if err != nil {
		return nil, err
	}
	defer c.releaseMigrationLock(context.WithoutCancel(ctx), lockToken)

	if err := c.checkHistoryShape(ctx); err != nil {
		return nil, err
	}

	// Nothing to adopt on a fresh database (and no table to read).
	var exists bool
	if err := c.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '_neutron_migrations')").Scan(&exists); err != nil {
		return nil, fmt.Errorf("nucleus: check history existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("nucleus: nothing to adopt: no migration history exists")
	}
	if err := c.ensureMigrationsTable(ctx); err != nil {
		return nil, err
	}

	byVersion := make(map[int]Migration, len(plan))
	for _, m := range plan {
		byVersion[m.Version] = m
	}

	rows, err := c.pool.Query(ctx, "SELECT version, name, checksum FROM _neutron_migrations")
	if err != nil {
		return nil, fmt.Errorf("nucleus: read history for adoption: %w", err)
	}
	type historyRow struct {
		version  int
		name     string
		checksum *string
	}
	var history []historyRow
	for rows.Next() {
		var r historyRow
		if err := rows.Scan(&r.version, &r.name, &r.checksum); err != nil {
			rows.Close()
			return nil, fmt.Errorf("nucleus: scan history for adoption: %w", err)
		}
		history = append(history, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	report := &MigrationAdoptionReport{}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("nucleus: begin adoption tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, r := range history {
		m, hasPlan := byVersion[r.version]
		newDigest := migrationChecksum(m)
		var legacyDigest string
		if hasPlan {
			legacyDigest = legacyMigrationChecksum(r.version, r.name, m.Up)
		}

		switch {
		case hasPlan && r.checksum != nil && *r.checksum == legacyDigest:
			if _, err := tx.Exec(ctx,
				"UPDATE _neutron_migrations SET checksum = $1, owner = $2, format = $3 WHERE version = $4",
				newDigest, migrationOwner(), MigrationHistoryFormat, sqlParam(r.version)); err != nil {
				return nil, fmt.Errorf("nucleus: adopt %d as verified: %w", r.version, err)
			}
			report.Verified = append(report.Verified, r.version)
		case hasPlan && r.checksum != nil && *r.checksum == newDigest:
			if _, err := tx.Exec(ctx,
				"UPDATE _neutron_migrations SET owner = $1, format = $2 WHERE version = $3",
				migrationOwner(), MigrationHistoryFormat, sqlParam(r.version)); err != nil {
				return nil, fmt.Errorf("nucleus: adopt %d: %w", r.version, err)
			}
			report.Verified = append(report.Verified, r.version)
		case hasPlan && r.checksum != nil:
			return nil, fmt.Errorf(
				"nucleus: adoption refused: migration %d (%s) has a recorded checksum that matches neither the "+
					"supplied plan nor the legacy Go SDK digest — restore the applied SQL or reconcile manually",
				r.version, r.name)
		default:
			// No recorded checksum (TS-SDK legacy) or no matching plan
			// entry: honest NULL, reported — never baselined.
			if _, err := tx.Exec(ctx,
				"UPDATE _neutron_migrations SET checksum = NULL, owner = $1, format = $2 WHERE version = $3",
				migrationOwner(), MigrationHistoryFormat, sqlParam(r.version)); err != nil {
				return nil, fmt.Errorf("nucleus: adopt %d as unverified: %w", r.version, err)
			}
			report.Unverified = append(report.Unverified, r.version)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("nucleus: commit adoption: %w", err)
	}
	return report, nil
}

// Migrate runs all pending migrations in order.
//
// Concurrency: runners in one process are serialized by a package gate
// (GO-29); runners in DIFFERENT processes are serialized by the ledger
// claim in _neutron_migration_lock — a second runner's Migrate blocks until
// the first RELEASES. There is no time-based takeover: a claim held by a
// crashed runner blocks until ForceUnlockMigrations is called with evidence
// the holder cannot execute (the heartbeat is diagnostic only).
//
// Checksums: every applied migration records the protocol-v2 checksum
// (SHA-256 over the up SQL) plus owner/format metadata, in the same
// transaction as its DDL. History rows in legacy formats (written before
// protocol v2) are refused before any mutation and graduate through
// AdoptMigrations — the GO-30 silent baselining is superseded.
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

	if err := c.checkHistoryShape(ctx); err != nil {
		return err
	}
	if err := c.ensureMigrationsTable(ctx); err != nil {
		return err
	}

	applied, err := c.appliedVersions(ctx)
	if err != nil {
		return err
	}
	if err := verifyHistory(plan, applied); err != nil {
		return err
	}

	for _, m := range plan {
		if _, isApplied := applied[m.Version]; isApplied {
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

		if _, err := tx.Exec(ctx,
			"INSERT INTO _neutron_migrations (version, name, checksum, owner, format) VALUES ($1, $2, $3, $4, $5)",
			sqlParam(m.Version), m.Name, migrationChecksum(m), migrationOwner(), MigrationHistoryFormat); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("nucleus: record migration %d: %w", m.Version, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("nucleus: commit migration %d: %w", m.Version, err)
		}

		// Heartbeat refresh: DIAGNOSTIC ONLY. Nothing steals based on it;
		// it exists so MigrationLockInfo can report a live holder's age.
		_, _ = c.pool.Exec(ctx,
			"UPDATE _neutron_migration_lock SET locked_at = NOW() WHERE id = 1 AND token = $1",
			sqlParam(lockToken))
	}

	return nil
}

// MigrateDown rolls back the specified number of migrations. Serialized by
// the same gates as Migrate and bound by the same history rules: legacy
// rows must be adopted first; recorded v2 checksums are enforced before any
// rollback runs.
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

	if err := c.checkHistoryShape(ctx); err != nil {
		return err
	}
	if err := c.ensureMigrationsTable(ctx); err != nil {
		return err
	}

	applied, err := c.appliedVersions(ctx)
	if err != nil {
		return err
	}
	if err := verifyHistory(plan, applied); err != nil {
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
