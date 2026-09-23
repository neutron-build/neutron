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
	// Checksum is the recorded v2 digest (SHA-256 over the applied up SQL).
	// Nil means unverified history: no trustworthy content record exists
	// (legacy row or adopted-unverified). Never silently backfilled.
	Checksum *string
	// Owner is the runner identity that wrote or adopted the row.
	Owner string
	// Format is the protocol marker ("v2"); empty for legacy rows.
	Format string
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
	// Unverified marks an applied migration whose recorded content could
	// not be proven (NULL checksum) — reported, never baselined.
	Unverified bool
}

// HasMigrationHistory reports whether the tracking table exists and holds at
// least one applied migration (guards `db push` against clobbering managed DBs).
func (c *Client) HasMigrationHistory(ctx context.Context) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = '_neutron_migrations'
	)`).Scan(&exists)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	var count int
	if err := c.pool.QueryRow(ctx, `SELECT count(*) FROM _neutron_migrations`).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// AppliedMigrations returns all applied migrations from the tracking table,
// reading v2 columns when the table has them. Read-only: an absent history
// yields no records and creates nothing — the history table is born only
// under a locked run (MigrationSession.EnsureMigrationTableV2) or adoption.
func (c *Client) AppliedMigrations(ctx context.Context) ([]MigrationRecord, error) {
	shape, err := c.InspectMigrationHistory(ctx)
	if err != nil {
		return nil, err
	}
	if shape == HistoryAbsent {
		return nil, nil
	}
	if shape == HistoryLegacyText || shape == HistoryLegacyInteger || shape == HistoryIncompatible {
		// Read what is there without the v2 columns.
		rows, err := c.pool.Query(ctx, "SELECT version, name, applied_at FROM _neutron_migrations ORDER BY version")
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

	rows, err := c.pool.Query(ctx, "SELECT version, name, applied_at, checksum, owner, format FROM _neutron_migrations ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMigrationRecords(rows)
}

// ApplyMigration/RevertMigration live on MigrationSession
// (migrate_history.go): every mutation runs on the pinned advisory-lock
// connection with v2 checksum/owner/format metadata recorded atomically with
// the DDL. There is deliberately no unlocked pool-level apply path.

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
			status.Unverified = r.Checksum == nil
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
			Version:    r.Version,
			Name:       r.Name,
			Applied:    true,
			AppliedAt:  r.AppliedAt,
			Missing:    true,
			Unverified: r.Checksum == nil,
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
	return CreateMigrationFilesWithContent(dir, name, "", "")
}

// CreateMigrationFilesWithContent generates a migration pair with explicit
// up/down SQL bodies (used by `migrate generate`). Empty content falls back
// to the comment-header stubs of CreateMigrationFiles.
func CreateMigrationFilesWithContent(dir, name, upSQL, downSQL string) (string, string, error) {
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
	if strings.TrimSpace(upSQL) != "" {
		upContent += "\n" + strings.TrimRight(upSQL, "\n") + "\n"
	}
	downContent := fmt.Sprintf("-- Rollback: %s\n", name)
	if strings.TrimSpace(downSQL) != "" {
		downContent += "\n" + strings.TrimRight(downSQL, "\n") + "\n"
	}

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
