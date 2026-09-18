package nucleus

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"math"
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
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`

// migrationGate serializes migration runners within this process (GO-29):
// Migrate/MigrateDown used to read applied versions OUTSIDE each migration
// transaction, so two concurrent callers both saw a version absent, both
// ran its Up SQL, then fought over the unique history INSERT
// (consumer-observed SQLSTATE 23505) — with the migration work possibly
// executed twice. Holding this gate across the WHOLE operation (history
// read included) removes the in-process race; cross-process coordination
// still requires running a single migration runner per database.
var migrationGate sync.Mutex

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
func (c *Client) Migrate(ctx context.Context, migrations []Migration) error {
	migrationGate.Lock()
	defer migrationGate.Unlock()

	// Ensure migrations table exists
	_, err := c.pool.Exec(ctx, migrationsTable)
	if err != nil {
		return fmt.Errorf("nucleus: create migrations table: %w", err)
	}

	// Copy + validate the plan before executing anything (GO-30).
	plan, err := prepareMigrations(migrations, false)
	if err != nil {
		return err
	}

	// Get applied versions
	applied, err := c.appliedVersions(ctx)
	if err != nil {
		return err
	}

	for _, m := range plan {
		if applied[m.Version] {
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

		if _, err := tx.Exec(ctx, "INSERT INTO _neutron_migrations (version, name) VALUES ($1, $2)", sqlParam(m.Version), m.Name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("nucleus: record migration %d: %w", m.Version, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("nucleus: commit migration %d: %w", m.Version, err)
		}
	}

	return nil
}

// MigrateDown rolls back the specified number of migrations.
func (c *Client) MigrateDown(ctx context.Context, migrations []Migration, steps int) error {
	migrationGate.Lock()
	defer migrationGate.Unlock()

	plan, err := prepareMigrations(migrations, true)
	if err != nil {
		return err
	}

	applied, err := c.appliedVersions(ctx)
	if err != nil {
		return err
	}

	rolled := 0
	for _, m := range plan {
		if rolled >= steps {
			break
		}
		if !applied[m.Version] {
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
		var rawVer []byte
		if err := rows.Scan(&rawVer, &r.Name, &r.AppliedAt); err != nil {
			return nil, err
		}
		v, err := scanInt(rawVer)
		if err != nil {
			return nil, fmt.Errorf("nucleus: parse migration version: %w", err)
		}
		r.Version = v
		records = append(records, r)
	}
	return records, rows.Err()
}

func (c *Client) appliedVersions(ctx context.Context) (map[int]bool, error) {
	rows, err := c.pool.Query(ctx, "SELECT version FROM _neutron_migrations")
	if err != nil {
		return nil, fmt.Errorf("nucleus: query applied versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var rawVer []byte
		if err := rows.Scan(&rawVer); err != nil {
			return nil, err
		}
		v, err := scanInt(rawVer)
		if err != nil {
			return nil, fmt.Errorf("nucleus: parse migration version: %w", err)
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// scanInt decodes an integer value from raw pgwire bytes. Nucleus declares
// text format (code 0) in RowDescription but sends big-endian binary bytes
// for INTEGER columns — pgx's text decoder then fails. We inspect the bytes:
// anything that is valid textual integer syntax (optional leading '-', then
// ASCII digits, fitting the platform int) decodes as text — including
// NEGATIVE text like "-123", which the old all-digits test misread as a
// 4-byte big-endian value near 1.7 billion (GO-31). Otherwise decode as
// big-endian int32/int64 with a platform-int range check. Fully resolving
// the text/binary ambiguity needs the engine to honor its declared wire
// format; until then the reserved-autocommit note stands: migration
// versions are validated positive before they are ever written.
// Track as Nucleus finding #35.
func scanInt(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty value")
	}
	// Textual integer syntax first (GO-31): handles signed text and keeps
	// digit-only binary bytes on their historical text interpretation.
	if isTextualInt(b) {
		n, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return 0, err
		}
		if n > math.MaxInt || n < math.MinInt {
			return 0, fmt.Errorf("value %d overflows platform int", n)
		}
		return int(n), nil
	}
	switch len(b) {
	case 4:
		v := int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3])
		return int(v), nil
	case 8:
		v := int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 |
			int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
		if v > math.MaxInt || v < math.MinInt {
			return 0, fmt.Errorf("value %d overflows platform int", v)
		}
		return int(v), nil
	default:
		return 0, fmt.Errorf("unexpected %d-byte integer", len(b))
	}
}

// isTextualInt reports whether b is `[+-]?[0-9]+` — the shape a text-format
// integer takes on the wire.
func isTextualInt(b []byte) bool {
	digits := b
	if len(b) > 0 && (b[0] == '-' || b[0] == '+') {
		digits = b[1:]
	}
	if len(digits) == 0 {
		return false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
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
