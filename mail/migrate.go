package mail

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Versioned engine migrations (audit OPS-02, pass 7).
//
// The boot path used to re-execute every schema statement on every start.
// That converges an existing database but applies unchanged work forever,
// gives a half-written boot no ledger to resume from, and lets two
// starting processes interleave DDL. The runner replaces it with the
// standard shape: a version ledger applied exactly once per version, a
// checksum that refuses a modified already-applied migration, and a
// session advisory lock serializing concurrent runners.
//
// Convergence contract: version 1 is the old statement list verbatim —
// all IF NOT EXISTS — so a database converged by earlier boots records it
// as applied without doing anything, and every later version is a real
// one-time transition. A database of ANY age converges through the same
// ordered list; nothing is rewritten in place.

// engineLockKey is the advisory-lock key for the engine migration runner.
// Distinct from the product runner's key and from the per-account
// maintenance keys; boot order (engine first, then product) is fixed in
// every entry point, so the two runners can never deadlock even when one
// process holds both.
const engineLockKey = 7812634095511001

// migration is one one-time schema transition.
type migration struct {
	Version    int64
	Name       string
	Statements []string
}

// EngineMigrations is the ordered, append-only engine migration list.
// Editing a shipped migration's statements changes its checksum and makes
// the runner refuse to boot — add a new version instead.
var EngineMigrations = []migration{
	{Version: 1, Name: "mirror baseline", Statements: Schema},
	{Version: 2, Name: "staged reconciliation scans", Statements: ScanSchema},
	{Version: 3, Name: "mirror referential integrity", Statements: ReferentialSchema},
	{Version: 4, Name: "scan generations", Statements: GenerationSchema},
}

const engineLedgerDDL = `CREATE TABLE IF NOT EXISTS mail_migrations (
	version    BIGINT PRIMARY KEY,
	name       TEXT NOT NULL,
	checksum   TEXT NOT NULL,
	applied_at TIMESTAMP NOT NULL
)`

// advisoryUnsupported / advisorySupported are probed once per process:
// backends without pg_advisory_lock (Nucleus over pgwire) still migrate
// correctly when boots are sequential — the ledger's primary key is the
// race backstop — but concurrent runners on such a backend are not
// serialized.
var advisoryUnsupported, advisorySupported atomic.Bool

// advisoryCheck reports whether this backend supports advisory locks,
// probing once per process. The probe key is arbitrary; it is locked and
// immediately unlocked only to ask the question.
func advisoryCheck(ctx context.Context, conn *pgx.Conn) (bool, error) {
	if advisoryUnsupported.Load() {
		return false, nil
	}
	if advisorySupported.Load() {
		return true, nil
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, engineLockKey); err != nil {
		advisoryUnsupported.Store(true)
		return false, nil
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, engineLockKey); err != nil {
		return false, err
	}
	advisorySupported.Store(true)
	return true, nil
}

func migrationChecksum(stmts []string) string {
	h := sha256.New()
	for _, s := range stmts {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Migrate converges the store's schema through the versioned ledger.
//
// The advisory lock is taken BEFORE the ledger bootstrap: two concurrent
// `CREATE TABLE IF NOT EXISTS` on one catalog race on pg_type even with
// the IF NOT EXISTS guard, so everything touching the ledger happens
// inside the lock.
func (s *PgStore) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("mail: migrate conn: %w", err)
	}
	defer conn.Release()

	supported, err := advisoryCheck(ctx, conn.Conn())
	if err != nil {
		return fmt.Errorf("mail: migration lock: %w", err)
	}
	if supported {
		if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, engineLockKey); err != nil {
			return fmt.Errorf("mail: migration lock: %w", err)
		}
		defer releaseMigrationLock(ctx, conn, engineLockKey)
	}

	if _, err := conn.Exec(ctx, engineLedgerDDL); err != nil {
		return fmt.Errorf("mail: migrate ledger: %w", err)
	}

	applied := map[int64]string{}
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM mail_migrations`)
	if err != nil {
		return fmt.Errorf("mail: read ledger: %w", err)
	}
	for rows.Next() {
		var version int64
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			rows.Close()
			return fmt.Errorf("mail: scan ledger: %w", err)
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("mail: read ledger: %w", err)
	}
	rows.Close()

	for _, m := range EngineMigrations {
		sum, ok := applied[m.Version]
		if !ok {
			if err := applyMigration(ctx, conn.Conn(), "mail_migrations", m); err != nil {
				return err
			}
			continue
		}
		if sum != migrationChecksum(m.Statements) {
			return fmt.Errorf("mail: migration %d (%s) was modified after application; refusing to boot", m.Version, m.Name)
		}
	}
	return nil
}

// releaseMigrationLock drops the session lock. If that fails the
// connection is hijacked and closed rather than returned to the pool: a
// pooled connection holding an unreleased session lock would block the
// next migration run forever (the audit's exact warning).
func releaseMigrationLock(ctx context.Context, conn *pgxpool.Conn, key int64) {
	if _, err := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, key); err != nil {
		if hijacked := conn.Hijack(); hijacked != nil {
			_ = hijacked.Close(context.Background())
		}
	}
}

func applyMigration(ctx context.Context, conn *pgx.Conn, ledger string, m migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mail: migration %d begin: %w", m.Version, err)
	}
	defer tx.Rollback(ctx)
	for _, stmt := range m.Statements {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("mail: migration %d (%s): %w\nstatement: %.80s", m.Version, m.Name, err, stmt)
		}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO `+ledger+` (version, name, checksum, applied_at) VALUES ($1, $2, $3, $4)`,
		m.Version, m.Name, migrationChecksum(m.Statements), time.Now().UTC()); err != nil {
		return fmt.Errorf("mail: migration %d ledger: %w", m.Version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("mail: migration %d commit: %w", m.Version, err)
	}
	return nil
}
