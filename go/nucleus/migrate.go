package nucleus

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

// Migrate runs all pending migrations in order.
func (c *Client) Migrate(ctx context.Context, migrations []Migration) error {
	// Ensure migrations table exists
	_, err := c.pool.Exec(ctx, migrationsTable)
	if err != nil {
		return fmt.Errorf("nucleus: create migrations table: %w", err)
	}

	// Sort migrations by version
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	// Get applied versions
	applied, err := c.appliedVersions(ctx)
	if err != nil {
		return err
	}

	for _, m := range migrations {
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

		if _, err := tx.Exec(ctx, "INSERT INTO _neutron_migrations (version, name) VALUES ($1, $2)", m.Version, m.Name); err != nil {
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
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version > migrations[j].Version // descending
	})

	applied, err := c.appliedVersions(ctx)
	if err != nil {
		return err
	}

	rolled := 0
	for _, m := range migrations {
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

		if _, err := tx.Exec(ctx, "DELETE FROM _neutron_migrations WHERE version = $1", m.Version); err != nil {
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

func (c *Client) appliedVersions(ctx context.Context) (map[int]bool, error) {
	rows, err := c.pool.Query(ctx, "SELECT version FROM _neutron_migrations")
	if err != nil {
		return nil, fmt.Errorf("nucleus: query applied versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
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
