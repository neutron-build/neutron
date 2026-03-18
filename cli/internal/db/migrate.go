package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// MigrationFile represents a SQL migration file on disk.
type MigrationFile struct {
	Version  string
	Name     string
	Path     string
	SQL      string
	IsDown   bool
}

// MigrationRecord represents an applied migration in the tracking table.
type MigrationRecord struct {
	Version   string
	Name      string
	AppliedAt time.Time
}

// MigrationStatus combines file and database state for a migration.
type MigrationStatus struct {
	Version   string
	Name      string
	Applied   bool
	AppliedAt time.Time
}

const createTrackingTable = `CREATE TABLE IF NOT EXISTS _neutron_migrations (
    version TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ DEFAULT now()
);`

// EnsureMigrationTable creates the tracking table if it doesn't exist.
func (c *Client) EnsureMigrationTable(ctx context.Context) error {
	return c.Exec(ctx, createTrackingTable)
}

// AppliedMigrations returns all applied migrations from the tracking table.
func (c *Client) AppliedMigrations(ctx context.Context) ([]MigrationRecord, error) {
	if err := c.EnsureMigrationTable(ctx); err != nil {
		return nil, err
	}

	rows, err := c.Query(ctx, "SELECT version, name, applied_at FROM _neutron_migrations ORDER BY version")
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

// ApplyMigration applies a single migration within a transaction.
func (c *Client) ApplyMigration(ctx context.Context, mf MigrationFile) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, mf.SQL); err != nil {
		return fmt.Errorf("execute migration %s: %w", mf.Version, err)
	}

	if _, err := tx.Exec(ctx, "INSERT INTO _neutron_migrations (version, name) VALUES ($1, $2)",
		mf.Version, mf.Name); err != nil {
		return fmt.Errorf("record migration %s: %w", mf.Version, err)
	}

	return tx.Commit(ctx)
}

// RevertMigration reverts a single migration within a transaction.
func (c *Client) RevertMigration(ctx context.Context, mf MigrationFile) error {
	tx, err := c.pool.Begin(ctx)
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

// readMigrationFilesWithSuffix is a helper that reads migration files with a given suffix.
func readMigrationFilesWithSuffix(dir, suffix string, reverseSort bool) ([]MigrationFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("migrations directory %q not found", dir)
		}
		return nil, err
	}

	var files []MigrationFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, suffix) {
			continue
		}

		// Parse: {version}_{name}.sql
		suffixWithDot := "." + suffix
		base := strings.TrimSuffix(name, suffixWithDot)
		parts := strings.SplitN(base, "_", 2)
		if len(parts) < 2 {
			continue
		}

		sql, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}

		files = append(files, MigrationFile{
			Version: parts[0],
			Name:    parts[1],
			Path:    filepath.Join(dir, name),
			SQL:     string(sql),
			IsDown:  suffix == "down.sql",
		})
	}

	if reverseSort {
		sort.Slice(files, func(i, j int) bool {
			return files[i].Version > files[j].Version
		})
	} else {
		sort.Slice(files, func(i, j int) bool {
			return files[i].Version < files[j].Version
		})
	}

	return files, nil
}

// ReadMigrationFiles reads .up.sql files from a directory.
func ReadMigrationFiles(dir string) ([]MigrationFile, error) {
	return readMigrationFilesWithSuffix(dir, "up.sql", false)
}

// ReadDownMigrationFiles reads .down.sql files from a directory, sorted newest-first.
func ReadDownMigrationFiles(dir string) ([]MigrationFile, error) {
	return readMigrationFilesWithSuffix(dir, "down.sql", true)
}

// MigrationStatuses returns the status of all migrations (applied + pending).
func (c *Client) MigrationStatuses(ctx context.Context, dir string) ([]MigrationStatus, error) {
	files, err := ReadMigrationFiles(dir)
	if err != nil {
		return nil, err
	}

	applied, err := c.AppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	appliedMap := make(map[string]MigrationRecord)
	for _, r := range applied {
		appliedMap[r.Version] = r
	}

	var statuses []MigrationStatus
	for _, f := range files {
		status := MigrationStatus{
			Version: f.Version,
			Name:    f.Name,
		}
		if r, ok := appliedMap[f.Version]; ok {
			status.Applied = true
			status.AppliedAt = r.AppliedAt
		}
		statuses = append(statuses, status)
	}

	return statuses, nil
}

// CreateMigrationFiles generates a new pair of .up.sql and .down.sql files.
func CreateMigrationFiles(dir, name string) (string, string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", "", err
	}

	// Determine next version number
	files, _ := ReadMigrationFiles(dir)
	nextVersion := fmt.Sprintf("%03d", len(files)+1)

	safeName := strings.ReplaceAll(strings.ToLower(name), " ", "_")
	upPath := filepath.Join(dir, fmt.Sprintf("%s_%s.up.sql", nextVersion, safeName))
	downPath := filepath.Join(dir, fmt.Sprintf("%s_%s.down.sql", nextVersion, safeName))

	upContent := fmt.Sprintf("-- Migration: %s\n", name)
	downContent := fmt.Sprintf("-- Rollback: %s\n", name)

	if err := os.WriteFile(upPath, []byte(upContent), 0644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(downPath, []byte(downContent), 0644); err != nil {
		return "", "", err
	}

	return upPath, downPath, nil
}
