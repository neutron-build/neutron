package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MigrationFile represents a SQL migration file on disk.
type MigrationFile struct {
	Version string
	Name    string
	Path    string
	SQL     string
	IsDown  bool
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
	// Missing marks a version recorded in the database whose source file
	// is absent from the local migrations directory. It can be inspected
	// in status output but not rolled back from this checkout.
	Missing bool
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

	// Sort numerically by version. Lexicographic ordering breaks once a
	// version exceeds the padding width ("1000" < "999" as strings), which
	// would apply migrations out of dependency order. Mixed-width and
	// duplicate numeric spellings ("7" vs "007") order by value with the
	// text as a deterministic tie-break; non-numeric versions sort last.
	if reverseSort {
		sort.Slice(files, func(i, j int) bool {
			return CompareVersions(files[j].Version, files[i].Version) < 0
		})
	} else {
		sort.Slice(files, func(i, j int) bool {
			return CompareVersions(files[i].Version, files[j].Version) < 0
		})
	}

	return files, nil
}

// CompareVersions orders version strings numerically when both parse,
// falling back to a numeric-before-non-numeric then lexicographic rule.
func CompareVersions(a, b string) int {
	an, aErr := strconv.ParseInt(a, 10, 64)
	bn, bErr := strconv.ParseInt(b, 10, 64)
	switch {
	case aErr == nil && bErr == nil:
		if an != bn {
			if an < bn {
				return -1
			}
			return 1
		}
	case aErr == nil:
		return -1
	case bErr == nil:
		return 1
	}
	return strings.Compare(a, b)
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
// The list is the union of local files and database records: an applied
// migration whose file is missing from this checkout appears with Missing
// set rather than silently disappearing from the status output (audit
// neutron-07).
func (c *Client) MigrationStatuses(ctx context.Context, dir string) ([]MigrationStatus, error) {
	files, err := ReadMigrationFiles(dir)
	if err != nil {
		return nil, err
	}

	applied, err := c.AppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	return mergeMigrationStatuses(files, applied), nil
}

// mergeMigrationStatuses is the pure union of local files and applied
// records, ordered by version.
func mergeMigrationStatuses(files []MigrationFile, applied []MigrationRecord) []MigrationStatus {
	appliedMap := make(map[string]MigrationRecord)
	for _, r := range applied {
		appliedMap[r.Version] = r
	}

	seen := make(map[string]bool)
	var statuses []MigrationStatus
	for _, f := range files {
		status := MigrationStatus{
			Version: f.Version,
			Name:    f.Name,
		}
		if r, ok := appliedMap[f.Version]; ok {
			status.Applied = true
			status.AppliedAt = r.AppliedAt
			seen[f.Version] = true
		}
		statuses = append(statuses, status)
	}

	// Applied versions with no local file: visible and flagged, not hidden.
	for _, r := range applied {
		if seen[r.Version] {
			continue
		}
		statuses = append(statuses, MigrationStatus{
			Version:   r.Version,
			Name:      r.Name,
			Applied:   true,
			AppliedAt: r.AppliedAt,
			Missing:   true,
		})
	}

	sort.SliceStable(statuses, func(i, j int) bool {
		return CompareVersions(statuses[i].Version, statuses[j].Version) < 0
	})
	return statuses
}

// migrationNameSlug validates and normalizes a migration name for use in a
// filename. Path separators and dot components must be rejected outright:
// lowercasing and space replacement alone let "part/../../escape" write
// outside the migrations directory (audit neutron-05).
func migrationNameSlug(name string) (string, error) {
	slug := strings.ToLower(strings.ReplaceAll(name, " ", "_"))
	if slug == "" {
		return "", fmt.Errorf("migration name must not be empty")
	}
	for _, r := range slug {
		allowed := (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') ||
			r == '_' || r == '-'
		if !allowed {
			return "", fmt.Errorf("migration name %q contains %q; use letters, digits, spaces, underscores, or hyphens", name, r)
		}
	}
	return slug, nil
}

// CreateMigrationFiles generates a new pair of .up.sql and .down.sql files.
func CreateMigrationFiles(dir, name string) (string, string, error) {
	safeName, err := migrationNameSlug(name)
	if err != nil {
		return "", "", err
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", "", err
	}

	// Determine next version number: one past the highest existing version.
	// Counting files instead (len+1) re-allocates a version after a gap
	// (001, 003) and silently overwrites the existing pair.
	files, _ := ReadMigrationFiles(dir)
	next := 1
	for _, f := range files {
		if n, err := strconv.Atoi(f.Version); err == nil && n >= next {
			next = n + 1
		}
	}
	nextVersion := fmt.Sprintf("%03d", next)

	upPath := filepath.Join(dir, fmt.Sprintf("%s_%s.up.sql", nextVersion, safeName))
	downPath := filepath.Join(dir, fmt.Sprintf("%s_%s.down.sql", nextVersion, safeName))

	upContent := fmt.Sprintf("-- Migration: %s\n", name)
	downContent := fmt.Sprintf("-- Rollback: %s\n", name)

	// O_EXCL: a collision means this version is already taken — fail loudly
	// rather than overwrite whatever owns it.
	up, err := os.OpenFile(upPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return "", "", err
	}
	if _, err := up.WriteString(upContent); err != nil {
		up.Close()
		return "", "", err
	}
	if err := up.Close(); err != nil {
		return "", "", err
	}

	down, err := os.OpenFile(downPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		os.Remove(upPath) // keep the pair atomic
		return "", "", err
	}
	if _, err := down.WriteString(downContent); err != nil {
		down.Close()
		os.Remove(upPath)
		return "", "", err
	}
	if err := down.Close(); err != nil {
		os.Remove(upPath)
		return "", "", err
	}

	return upPath, downPath, nil
}
