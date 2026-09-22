package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// Migration history protocol v2 (contracts/data/MIGRATIONS.md)
// ---------------------------------------------------------------------------

// MigrationHistoryFormat is the protocol marker recorded in the history
// table's format column. NULL (absent) marks legacy rows awaiting adoption.
const MigrationHistoryFormat = "v2"

// MigrationOwnerCLI is the owner recorded on rows the CLI writes or adopts.
const MigrationOwnerCLI = "neutron-cli"

// migrationAdvisoryLockKey serializes migration runners on PostgreSQL:
// session-level advisory lock taken on a dedicated pinned connection and held
// from the history read through the final apply. Value derivation (stable
// across processes and releases): the first 8 bytes, big-endian signed, of
// SHA-256("_neutron_migrations.runner").
const migrationAdvisoryLockKey = 7043516000858342567

// MigrationChecksum is the canonical v2 digest: lowercase-hex SHA-256 over
// the up SQL text exactly as supplied, UTF-8 encoded (the verbatim-text rule
// of schema contract v2 — formatting differences are different content).
func MigrationChecksum(upSQL string) string {
	sum := sha256.Sum256([]byte(upSQL))
	return hex.EncodeToString(sum[:])
}

// LegacyGoSDKChecksum reproduces the pre-M04 Go SDK digest (GO-30 era):
// SHA-256 over "%d\x00%s\x00%s" of (version, name, up). Version and name are
// known from the history row itself, so this digest is RE-VERIFIABLE during
// adoption — a match proves the supplied up SQL is byte-identical to what the
// legacy runner recorded. Used only inside adoption; never written.
func LegacyGoSDKChecksum(version int64, name, upSQL string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s", version, name, upSQL)))
	return hex.EncodeToString(sum[:])
}

// HistoryShape classifies an existing _neutron_migrations table.
type HistoryShape int

const (
	// HistoryAbsent: no table (fresh database).
	HistoryAbsent HistoryShape = iota
	// HistoryV2Text: TEXT version PK + v2 columns — the CLI's own shape.
	HistoryV2Text
	// HistoryV2Integer: INTEGER version PK + v2 columns — the SDK shape.
	// CLI runners refuse it (mixed-runner rule).
	HistoryV2Integer
	// HistoryLegacyText: TEXT version PK without v2 columns (pre-M04 CLI).
	HistoryLegacyText
	// HistoryLegacyInteger: INTEGER version PK without v2 columns
	// (pre-M04 Go/TS SDK history).
	HistoryLegacyInteger
	// HistoryIncompatible: a _neutron_migrations table whose shape matches
	// no known protocol generation.
	HistoryIncompatible
)

// String names the shape for diagnostics and refusal messages.
func (s HistoryShape) String() string {
	switch s {
	case HistoryAbsent:
		return "absent"
	case HistoryV2Text:
		return "v2-text"
	case HistoryV2Integer:
		return "v2-integer"
	case HistoryLegacyText:
		return "legacy-text"
	case HistoryLegacyInteger:
		return "legacy-integer"
	default:
		return "incompatible"
	}
}

// historyColumnTypes reads the version column type and v2 column presence
// from information_schema.
func (c *Client) historyColumnTypes(ctx context.Context) (versionType string, hasV2Columns bool, exists bool, err error) {
	rows, err := c.pool.Query(ctx, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = '_neutron_migrations'`)
	if err != nil {
		return "", false, false, err
	}
	defer rows.Close()

	cols := make(map[string]string)
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return "", false, false, err
		}
		cols[strings.ToLower(name)] = strings.ToLower(dataType)
	}
	if err := rows.Err(); err != nil {
		return "", false, false, err
	}
	if len(cols) == 0 {
		return "", false, false, nil
	}
	vt, ok := cols["version"]
	if !ok {
		return "", false, true, errors.New("_neutron_migrations has no version column")
	}
	hasV2 := cols["checksum"] != "" && cols["owner"] != "" && cols["format"] != ""
	return vt, hasV2, true, nil
}

// InspectMigrationHistory classifies the database's migration history shape.
func (c *Client) InspectMigrationHistory(ctx context.Context) (HistoryShape, error) {
	versionType, hasV2, exists, err := c.historyColumnTypes(ctx)
	if err != nil {
		return HistoryIncompatible, err
	}
	if !exists {
		return HistoryAbsent, nil
	}
	integer := versionType == "integer" || versionType == "smallint" || versionType == "bigint"
	text := versionType == "text" || versionType == "character varying" || versionType == "character"
	switch {
	case integer && hasV2:
		return HistoryV2Integer, nil
	case integer:
		return HistoryLegacyInteger, nil
	case text && hasV2:
		return HistoryV2Text, nil
	case text:
		return HistoryLegacyText, nil
	default:
		return HistoryIncompatible, fmt.Errorf(
			"_neutron_migrations.version has unsupported type %q", versionType)
	}
}

// ---------------------------------------------------------------------------
// Advisory lock session (PostgreSQL)
// ---------------------------------------------------------------------------

// MigrationSession is the migration runner's dedicated pinned connection:
// one pooled connection holding the advisory lock for the entire run. Every
// history read, checksum verification and migration transaction executes on
// THIS session, so the lock and the work can never be separated by a pool
// checkout — the failure mode of a pool-level pg_advisory_lock query.
type MigrationSession struct {
	conn     *pgxpool.Conn
	released bool
}

// LockMigrations acquires the migration advisory lock on a dedicated
// connection. It blocks while another runner holds the lock; ctx
// cancellation/deadline aborts the wait cleanly. The caller MUST call
// Release, typically deferred. Transaction-pooled proxies (PgBouncer
// transaction mode) are unsupported: a session-level lock cannot survive a
// pooler that reassigns sessions between statements.
func (c *Client) LockMigrations(ctx context.Context) (*MigrationSession, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire migration connection: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockKey); err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquire migration lock (another runner may be holding it): %w", err)
	}
	return &MigrationSession{conn: conn}, nil
}

// Release drops the advisory lock and returns the connection to the pool.
// Safe to call more than once. The unlock uses a context that survives
// cancellation of the run: a failed run must still release the lock. If the
// session is already dead the unlock error is irrelevant — PostgreSQL
// releases session advisory locks on disconnect, which is the crash story.
func (s *MigrationSession) Release() {
	if s == nil || s.released {
		return
	}
	s.released = true
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 5*time.Second)
	defer cancel()
	_, _ = s.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockKey)
	s.conn.Release()
}

// Exec runs a statement on the pinned session.
func (s *MigrationSession) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.conn.Exec(ctx, sql, args...)
	return err
}

// QueryRow runs a single-row query on the pinned session.
func (s *MigrationSession) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return s.conn.QueryRow(ctx, sql, args...)
}

// BeginTx starts a transaction on the pinned session, so migration DDL and
// its history row commit atomically while the lock is held.
func (s *MigrationSession) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.conn.Begin(ctx)
}

// EnsureMigrationTableV2 creates the protocol v2 history table. Only valid
// when the table does not exist yet; existing tables graduate via adoption.
func (s *MigrationSession) EnsureMigrationTableV2(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS _neutron_migrations (
		version    TEXT PRIMARY KEY,
		name       TEXT NOT NULL,
		applied_at TIMESTAMPTZ DEFAULT now(),
		checksum   TEXT,
		owner      TEXT,
		format     TEXT
	)`)
	return err
}

// AppliedMigrations reads the full history (v2 columns included) on the
// pinned session. Requires a v2-shaped table.
func (s *MigrationSession) AppliedMigrations(ctx context.Context) ([]MigrationRecord, error) {
	rows, err := s.conn.Query(ctx,
		"SELECT version, name, applied_at, checksum, owner, format FROM _neutron_migrations ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMigrationRecords(rows)
}

// scanMigrationRecords reads the six-column history projection into
// MigrationRecords, mapping SQL NULL checksum to a nil pointer.
func scanMigrationRecords(rows pgx.Rows) ([]MigrationRecord, error) {
	var records []MigrationRecord
	for rows.Next() {
		var r MigrationRecord
		var checksum sql.Null[string]
		var owner, format sql.Null[string]
		if err := rows.Scan(&r.Version, &r.Name, &r.AppliedAt, &checksum, &owner, &format); err != nil {
			return nil, err
		}
		if checksum.Valid {
			r.Checksum = &checksum.V
		}
		if owner.Valid {
			r.Owner = owner.V
		}
		if format.Valid {
			r.Format = format.V
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// ApplyMigration executes one migration and records its history row with v2
// metadata in the SAME transaction: either both commit or neither does. Runs
// on the pinned session under the advisory lock.
func (s *MigrationSession) ApplyMigration(ctx context.Context, mf MigrationFile) error {
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, mf.SQL); err != nil {
		return fmt.Errorf("execute migration %s: %w", mf.Version, err)
	}

	if _, err := tx.Exec(ctx,
		"INSERT INTO _neutron_migrations (version, name, checksum, owner, format) VALUES ($1, $2, $3, $4, $5)",
		mf.Version, mf.Name, MigrationChecksum(mf.SQL), MigrationOwnerCLI, MigrationHistoryFormat,
	); err != nil {
		return fmt.Errorf("record migration %s: %w", mf.Version, err)
	}

	return tx.Commit(ctx)
}

// RevertMigration executes one down migration and deletes its history row in
// one transaction on the pinned session. Checksum verification of the up
// file happens before any revert runs (VerifyAppliedChecksums).
func (s *MigrationSession) RevertMigration(ctx context.Context, mf MigrationFile) error {
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, mf.SQL); err != nil {
		return fmt.Errorf("execute down migration %s: %w", mf.Version, err)
	}

	if _, err := tx.Exec(ctx, "DELETE FROM _neutron_migrations WHERE version = $1", mf.Version); err != nil {
		return fmt.Errorf("delete migration record %s: %w", mf.Version, err)
	}

	return tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// Collisions, verification, adoption
// ---------------------------------------------------------------------------

// numericID parses an ID as an integer when it is a canonical numeric
// spelling (no padding, no signs beyond a leading minus). "001" and "1" both
// parse; the COLLISION rule is what keeps them from being equated silently.
func numericID(id string) (int64, bool) {
	n, err := strconv.ParseInt(id, 10, 64)
	return n, err == nil
}

// DetectMigrationCollisions rejects identity ambiguity in the union of
// history and files, before any mutation:
//
//   - two files claiming the same version (001_a + 001_b): the second apply
//     would otherwise fail mid-run on the history primary key;
//   - two distinct text IDs that are numerically equal (history "1" vs file
//     "001"): the runner cannot know whether the file is the applied
//     migration renumbered or a different migration.
//
// Identity is never padded or normalized to make ambiguity disappear.
func DetectMigrationCollisions(files []MigrationFile, applied []MigrationRecord) error {
	seenText := make(map[string]bool)
	var numericIDs []string
	for _, f := range files {
		if seenText[f.Version] {
			return fmt.Errorf(
				"migration version %q is claimed by more than one file — resolve the duplicate before running", f.Version)
		}
		seenText[f.Version] = true
		numericIDs = append(numericIDs, f.Version)
	}
	for _, r := range applied {
		numericIDs = append(numericIDs, r.Version)
	}
	byValue := make(map[int64]string)
	sort.Strings(numericIDs)
	for _, id := range numericIDs {
		v, ok := numericID(id)
		if !ok {
			continue
		}
		if prev, seen := byValue[v]; seen && prev != id {
			return fmt.Errorf(
				"migration ID collision: %q and %q are numerically equal but textually distinct — "+
					"rename one (or adopt the history with matching IDs) before running; "+
					"Neutron never assumes \"001\" and \"1\" are the same migration",
				prev, id)
		}
		byValue[v] = id
	}
	return nil
}

// VerifyAppliedChecksums enforces recorded content for every applied
// migration that has a supplied file and a non-NULL checksum. A mismatch is
// returned as an error and must abort the run BEFORE any new mutation. Rows
// with NULL checksum (adopted-unverified or legacy) are returned as
// unverified for reporting; they are exempt because there is nothing to
// compare against — they are never silently given a checksum.
func VerifyAppliedChecksums(files []MigrationFile, applied []MigrationRecord) (unverified []string, err error) {
	byVersion := make(map[string]MigrationFile, len(files))
	for _, f := range files {
		byVersion[f.Version] = f
	}
	var mismatches []string
	for _, r := range applied {
		f, ok := byVersion[r.Version]
		if !ok {
			continue
		}
		if r.Checksum == nil {
			unverified = append(unverified, r.Version)
			continue
		}
		if want := MigrationChecksum(f.SQL); *r.Checksum != want {
			mismatches = append(mismatches, fmt.Sprintf(
				"  %s (%s): recorded sha256:%s, current file sha256:%s",
				r.Version, r.Name, *r.Checksum, want))
		}
	}
	if len(mismatches) > 0 {
		return unverified, fmt.Errorf(
			"applied migration(s) have been modified since they were applied — "+
				"restore the applied SQL or write a new migration:\n%s",
			strings.Join(mismatches, "\n"))
	}
	return unverified, nil
}

// VerifyHistoryFormats enforces the refuse-until-adopted transition on the
// CLI's own shape: every row of a v2-text history must carry the v2 marker
// (contracts/data/MIGRATIONS.md §4/§7). A row with NULL format — or a
// foreign value — is pre-protocol state no CLI runner can produce; it is
// refused before any mutation with the adopt directive, mirroring the SDK
// runners' verifyHistory rule. It must never be silently treated as
// unverified (NULL-checksum) history.
func VerifyHistoryFormats(applied []MigrationRecord) error {
	for _, r := range applied {
		if r.Format != MigrationHistoryFormat {
			return fmt.Errorf(
				"migration %s (%s) is recorded without the v2 format marker and must be adopted "+
					"once before this runner continues — run `neutron migrate adopt` (explicit, "+
					"transactional; unprovable rows stay unverified)",
				r.Version, r.Name)
		}
	}
	return nil
}

// AdoptionResult reports what one adoption decided, per row. Verified rows
// carry recorded content proof; unverified rows keep checksum NULL.
type AdoptionResult struct {
	// Verified versions: legacy Go SDK digest reproduced from the supplied
	// file, so the content is proven identical to what was applied.
	Verified []string
	// Unverified versions: no recorded checksum existed (CLI/TS legacy) or
	// no exact-ID file was supplied. Checksum stays NULL; reported honestly.
	Unverified []string
	// ConvertedFromInteger records an explicit INTEGER->TEXT version
	// conversion (SDK-shaped history adopted by the CLI).
	ConvertedFromInteger bool
}

// AdoptMigrationHistory graduates a legacy history into protocol v2 in ONE
// transaction on the pinned session (contracts/data/MIGRATIONS.md §6):
// collision check first, shape upgrade, then per-row verification. It never
// fabricates trust: unverifiable rows stay checksum-NULL and are reported;
// a recorded checksum that matches no supplied file under either digest
// aborts the adoption.
func (s *MigrationSession) AdoptMigrationHistory(ctx context.Context, files []MigrationFile) (*AdoptionResult, error) {
	shape, err := s.inspectShape(ctx)
	if err != nil {
		return nil, err
	}

	var records []MigrationRecord
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin adoption tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Read history AS-IS inside the transaction.
	switch shape {
	case HistoryLegacyText, HistoryLegacyInteger, HistoryV2Text, HistoryV2Integer:
		records, err = readHistoryTx(ctx, tx)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("nothing to adopt: history shape is %s", shape)
	}

	// Collisions first: ambiguity cannot be adopted away.
	if err := DetectMigrationCollisions(files, records); err != nil {
		return nil, err
	}

	res := &AdoptionResult{}

	// Shape upgrade: add v2 columns (idempotent), and for SDK-shaped history
	// convert the version column INTEGER -> TEXT explicitly. This is the
	// ONLY place the conversion happens; ordinary runs never rewrite it.
	if _, err := tx.Exec(ctx,
		"ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS checksum TEXT"); err != nil {
		return nil, fmt.Errorf("add checksum column: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS owner TEXT"); err != nil {
		return nil, fmt.Errorf("add owner column: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS format TEXT"); err != nil {
		return nil, fmt.Errorf("add format column: %w", err)
	}
	if shape == HistoryLegacyInteger || shape == HistoryV2Integer {
		if _, err := tx.Exec(ctx,
			"ALTER TABLE _neutron_migrations ALTER COLUMN version TYPE TEXT USING version::text"); err != nil {
			return nil, fmt.Errorf("convert version column to text (explicit adoption step): %w", err)
		}
		res.ConvertedFromInteger = true
	}

	byVersion := make(map[string]MigrationFile, len(files))
	for _, f := range files {
		byVersion[f.Version] = f
	}

	for _, r := range records {
		f, hasFile := byVersion[r.Version]
		var legacyDigest string
		if v, err := strconv.ParseInt(r.Version, 10, 64); err == nil && hasFile {
			legacyDigest = LegacyGoSDKChecksum(v, r.Name, f.SQL)
		}

		newDigest := MigrationChecksum(f.SQL)
		switch {
		case hasFile && r.Checksum != nil && *r.Checksum == legacyDigest:
			// Legacy Go SDK digest reproduced from the supplied file: the
			// content is proven identical to what was applied.
			if _, err := tx.Exec(ctx,
				"UPDATE _neutron_migrations SET checksum = $1, owner = $2, format = $3 WHERE version = $4",
				newDigest, MigrationOwnerCLI, MigrationHistoryFormat, r.Version); err != nil {
				return nil, fmt.Errorf("adopt %s as verified: %w", r.Version, err)
			}
			res.Verified = append(res.Verified, r.Version)
		case hasFile && r.Checksum != nil && *r.Checksum == newDigest:
			// Already v2 content (e.g. adoption re-run): stamp the metadata.
			if _, err := tx.Exec(ctx,
				"UPDATE _neutron_migrations SET owner = $1, format = $2 WHERE version = $3",
				MigrationOwnerCLI, MigrationHistoryFormat, r.Version); err != nil {
				return nil, fmt.Errorf("adopt %s: %w", r.Version, err)
			}
			res.Verified = append(res.Verified, r.Version)
		case hasFile && r.Checksum != nil:
			// Recorded checksum matches neither the legacy digest nor the
			// v2 digest of the supplied file: the recorded content
			// disagrees with what is on disk. Only a human can reconcile.
			return nil, fmt.Errorf(
				"adoption refused: migration %s (%s) has a recorded checksum that matches neither the "+
					"supplied file nor the legacy Go SDK digest — restore the applied SQL or reconcile manually",
				r.Version, r.Name)
		default:
			// No recorded checksum (CLI/TS legacy) or no exact-ID file:
			// nothing to verify against. Checksum becomes NULL (v2
			// semantics; recorded-but-unverifiable legacy digests are not
			// v2 values), format/owner are stamped, the row is reported.
			// Never baselined.
			if _, err := tx.Exec(ctx,
				"UPDATE _neutron_migrations SET checksum = NULL, owner = $1, format = $2 WHERE version = $3",
				MigrationOwnerCLI, MigrationHistoryFormat, r.Version); err != nil {
				return nil, fmt.Errorf("adopt %s as unverified: %w", r.Version, err)
			}
			res.Unverified = append(res.Unverified, r.Version)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit adoption: %w", err)
	}
	return res, nil
}

// inspectShape classifies history using the session's own connection.
func (s *MigrationSession) inspectShape(ctx context.Context) (HistoryShape, error) {
	var versionType string
	var n int
	err := s.QueryRow(ctx, `
		SELECT count(*),
		       coalesce(max(data_type) FILTER (WHERE column_name = 'version'), '')
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = '_neutron_migrations'`).Scan(&n, &versionType)
	if err != nil {
		return HistoryIncompatible, err
	}
	if n == 0 {
		return HistoryAbsent, nil
	}
	var hasChecksum, hasOwner, hasFormat bool
	err = s.QueryRow(ctx, `
		SELECT
		  EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '_neutron_migrations' AND column_name = 'checksum'),
		  EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '_neutron_migrations' AND column_name = 'owner'),
		  EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '_neutron_migrations' AND column_name = 'format')
	`).Scan(&hasChecksum, &hasOwner, &hasFormat)
	if err != nil {
		return HistoryIncompatible, err
	}
	hasV2 := hasChecksum && hasOwner && hasFormat
	versionType = strings.ToLower(versionType)
	integer := versionType == "integer" || versionType == "smallint" || versionType == "bigint"
	text := versionType == "text" || versionType == "character varying" || versionType == "character"
	switch {
	case integer && hasV2:
		return HistoryV2Integer, nil
	case integer:
		return HistoryLegacyInteger, nil
	case text && hasV2:
		return HistoryV2Text, nil
	case text:
		return HistoryLegacyText, nil
	default:
		return HistoryIncompatible, fmt.Errorf("_neutron_migrations.version has unsupported type %q", versionType)
	}
}

// readHistoryTx reads history rows inside the adoption transaction, using
// the checksum column when the table has one (legacy Go SDK tables do; CLI
// and TS SDK legacy tables do not).
func readHistoryTx(ctx context.Context, tx pgx.Tx) ([]MigrationRecord, error) {
	var hasChecksum bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = '_neutron_migrations' AND column_name = 'checksum'
		)`).Scan(&hasChecksum); err != nil {
		return nil, err
	}

	if !hasChecksum {
		rows, err := tx.Query(ctx, "SELECT version, name, applied_at FROM _neutron_migrations ORDER BY version")
		if err != nil {
			return nil, err
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

	rows, err := tx.Query(ctx, "SELECT version, name, applied_at, checksum FROM _neutron_migrations ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []MigrationRecord
	for rows.Next() {
		var r MigrationRecord
		var checksum sql.Null[string]
		if err := rows.Scan(&r.Version, &r.Name, &r.AppliedAt, &checksum); err != nil {
			return nil, err
		}
		if checksum.Valid {
			r.Checksum = &checksum.V
		}
		records = append(records, r)
	}
	return records, rows.Err()
}
