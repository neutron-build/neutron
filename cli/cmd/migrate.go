package cmd

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	migrateCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateCmd.Flags().Duration("timeout", 60*time.Second, "total time budget for the migration batch (0 = no deadline)")

	migrateStatusCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateStatusCmd.Flags().Duration("timeout", 10*time.Second, "time budget for the status query (0 = no deadline)")

	migrateCreateCmd.Flags().String("dir", "migrations", "migrations directory")

	migrateAdoptCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateAdoptCmd.Flags().Duration("timeout", 60*time.Second, "time budget for the adoption (0 = no deadline)")

	migrateDownCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateDownCmd.Flags().Duration("timeout", 60*time.Second, "total time budget for the rollback batch (0 = no deadline)")

	migrateCmd.AddCommand(migrateStatusCmd)
	migrateCmd.AddCommand(migrateCreateCmd)
	migrateCmd.AddCommand(migrateAdoptCmd)
	migrateCmd.AddCommand(migrateDownCmd)
	rootCmd.AddCommand(migrateCmd)
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run database migrations",
	Long: `Apply pending SQL migration files to the database.

Execution is serialized across runners with a PostgreSQL session-level
advisory lock held on a dedicated connection from the history read through
the final apply; each migration's DDL and its history row commit atomically.
Applied migrations are checksum-verified (SHA-256 over the file's SQL) before
anything new runs. Transaction-pooled proxies (e.g. PgBouncer transaction
mode) are unsupported for migration connections: use a direct or
session-pooled connection.

Migration files run verbatim in filename order: the protections enforced when
SQL is generated (migrate generate and db push never plan changes to
neutron-internal _neutron_* metadata or extension-owned objects) are
generation-time only — hand-edited files are not re-checked before they run.

Targets PostgreSQL. Nucleus migration runners live in the language SDKs and
remain experimental. Databases with pre-protocol histories (older CLI, or
Go/TS SDK integer-version tables) are refused until adopted once with
` + "`neutron migrate adopt`" + `.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrate(cmd, args)) },
}

var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	RunE:  func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateStatus(cmd, args)) },
}

var migrateCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new migration file",
	Args:  cobra.ExactArgs(1),
	RunE:  func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateCreate(cmd, args)) },
}

var migrateAdoptCmd = &cobra.Command{
	Use:   "adopt",
	Short: "Adopt a legacy migration history into protocol v2",
	Long: `Explicitly graduate a legacy _neutron_migrations history into the v2
protocol, in one transaction under the migration lock.

Adoption matches existing history rows to the supplied files by exact text ID
("001" and "1" are never treated as the same migration; numerically equal but
textually distinct IDs are a reconciliation error). Rows whose recorded
content can be proven — a legacy Go SDK checksum that reproduces from the
supplied file — are adopted as verified. Everything else is adopted as
UNVERIFIED: the checksum stays empty and the row is reported. Unknown
checksums are unverified history, not proof of integrity — nothing is ever
silently baselined.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateAdopt(cmd, args)) },
}

var migrateDownCmd = &cobra.Command{
	Use:   "down [N]",
	Short: "Revert N migrations (default 1)",
	Args:  cobra.MaximumNArgs(1),
	RunE:  func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateDown(cmd, args)) },
}

// commandContext derives the operation context from the command's own
// context, so caller cancellation (signals, parent tooling) propagates into
// the queries, with the --timeout flag as an optional overall budget.
func commandContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	parent := cmd.Context()
	if parent == nil {
		parent = context.Background()
	}
	timeout, err := cmd.Flags().GetDuration("timeout")
	if err != nil || timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

// migrateSessionGuard connects, refuses Nucleus targets (SDK runners own
// Nucleus migrations; the CLI file workflow targets PostgreSQL), reads the
// migration files, then acquires the advisory-lock session. Files are read
// before locking so the lock is never held across local disk I/O; the lock
// wait itself honors the run's deadline/cancellation. The returned release
// function must be deferred by the caller.
func migrateSessionGuard(ctx context.Context, dir string) (*db.Client, []db.MigrationFile, *db.MigrationSession, func(), error) {
	url := config.DatabaseURL()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("connect: %w", err)
	}
	closeAll := func() { client.Close() }

	if isNucleus, _, err := client.IsNucleus(ctx); err == nil && isNucleus {
		return nil, nil, nil, closeAll, fmt.Errorf(
			"this database is a Nucleus server: `neutron migrate` targets PostgreSQL — " +
				"Nucleus migration runners are the language SDKs (go/nucleus, @neutron-build/nucleus) and stay experimental")
	}

	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		return nil, nil, nil, closeAll, err
	}

	sess, err := client.LockMigrations(ctx)
	if err != nil {
		return nil, nil, nil, closeAll, err
	}
	release := func() {
		sess.Release()
		client.Close()
	}
	return client, files, sess, release, nil
}

// prepareHistoryRun validates the history shape and returns the applied
// records for a run. Every refusal happens BEFORE any mutation, and the
// identity-collision check runs before a fresh database's history table is
// even created.
func prepareHistoryRun(ctx context.Context, client *db.Client, sess *db.MigrationSession, files []db.MigrationFile) ([]db.MigrationRecord, error) {
	shape, err := client.InspectMigrationHistory(ctx)
	if err != nil {
		return nil, err
	}
	switch shape {
	case db.HistoryAbsent:
		if err := db.DetectMigrationCollisions(files, nil); err != nil {
			return nil, err
		}
		if err := sess.EnsureMigrationTableV2(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	case db.HistoryV2Text:
		applied, err := sess.AppliedMigrations(ctx)
		if err != nil {
			return nil, err
		}
		// A v2-shaped table with a NULL-format row is pre-protocol state:
		// refuse until adopted (§4/§7), never treat it as unverified history.
		if err := db.VerifyHistoryFormats(applied); err != nil {
			return nil, err
		}
		if err := db.DetectMigrationCollisions(files, applied); err != nil {
			return nil, err
		}
		return applied, nil
	case db.HistoryLegacyText, db.HistoryLegacyInteger:
		return nil, fmt.Errorf(
			"migration history is in the legacy %s shape and must be adopted once before it can be run on — "+
				"run `neutron migrate adopt` (explicit, transactional; unprovable rows stay unverified)",
			shape)
	case db.HistoryV2Integer:
		return nil, fmt.Errorf(
			"migration history uses the SDK integer-version shape (written by the Go/TS Nucleus SDKs); " +
				"the CLI's canonical history uses text IDs — adopt it with `neutron migrate adopt` to convert, " +
				"or keep using the SDK runners that wrote it")
	default:
		return nil, fmt.Errorf("migration history has an incompatible shape (%s); refusing before any mutation", shape)
	}
}

func runMigrate(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, files, sess, release, err := migrateSessionGuard(ctx, dir)
	if err != nil {
		return err
	}
	defer release()

	if len(files) == 0 {
		ui.Warnf("No migration files found in %s", dir)
		return nil
	}

	applied, err := prepareHistoryRun(ctx, client, sess, files)
	if err != nil {
		return err
	}

	// Recorded-content drift is refused before any new mutation.
	unverified, err := db.VerifyAppliedChecksums(files, applied)
	if err != nil {
		return err
	}

	appliedSet := make(map[string]bool)
	for _, r := range applied {
		appliedSet[r.Version] = true
	}

	var count int
	for _, f := range files {
		if appliedSet[f.Version] {
			continue
		}

		spinner := ui.NewSpinner(fmt.Sprintf("Applying %s_%s...", f.Version, f.Name))
		if err := sess.ApplyMigration(ctx, f); err != nil {
			spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed %s_%s: %v", f.Version, f.Name, err))
			// Name the interruption boundary: a partial batch is a
			// different operational state than an untouched one.
			return fmt.Errorf("interrupted after %d of %d pending migration(s) (failed at %s_%s): %w",
				count, len(files)-len(applied), f.Version, f.Name, err)
		}
		spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Applied %s_%s", f.Version, f.Name))
		count++
	}

	if len(unverified) > 0 {
		ui.Warnf("%d applied migration(s) have unverified history (no recorded checksum): %s",
			len(unverified), strings.Join(unverified, ", "))
	}

	if count == 0 {
		ui.Successf("Database is up to date (%d migrations applied)", len(applied))
	} else {
		ui.Successf("Applied %d migration(s)", count)
	}

	return nil
}

func runMigrateAdopt(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, files, sess, release, err := migrateSessionGuard(ctx, dir)
	if err != nil {
		return err
	}
	defer release()

	shape, err := client.InspectMigrationHistory(ctx)
	if err != nil {
		return err
	}
	switch shape {
	case db.HistoryAbsent:
		return fmt.Errorf("nothing to adopt: this database has no migration history (the first `neutron migrate` creates a v2 history)")
	case db.HistoryV2Text:
		ui.Infof("History is already in the v2 shape; adopting is a no-op report")
	}

	res, err := sess.AdoptMigrationHistory(ctx, files)
	if err != nil {
		return err
	}

	if res.ConvertedFromInteger {
		ui.Infof("Converted the version column INTEGER -> TEXT (explicit adoption step)")
	}
	for _, v := range res.Verified {
		ui.Successf("adopted %s: verified (recorded checksum reproduced from the supplied file)", v)
	}
	for _, v := range res.Unverified {
		ui.Warnf("adopted %s: UNVERIFIED — no proof of what was applied (checksum left empty)", v)
	}
	ui.Successf("Adoption complete: %d verified, %d unverified", len(res.Verified), len(res.Unverified))
	return nil
}

func runMigrateStatus(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	url := config.DatabaseURL()

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	shape, err := client.InspectMigrationHistory(ctx)
	if err != nil {
		return err
	}
	switch shape {
	case db.HistoryLegacyText, db.HistoryLegacyInteger:
		ui.Warnf("History is in the legacy %s shape; `neutron migrate` will refuse until `neutron migrate adopt` runs once", shape)
	case db.HistoryV2Integer:
		ui.Warnf("History uses the SDK integer-version shape; CLI migrations refuse — use the SDK runners or `neutron migrate adopt`")
	case db.HistoryIncompatible:
		ui.Warnf("History has an incompatible shape; refusing to interpret it as applied state")
	}

	statuses, err := client.MigrationStatuses(ctx, dir)
	if err != nil {
		return err
	}

	if len(statuses) == 0 {
		ui.Warnf("No migrations found in %s", dir)
		return nil
	}

	tbl := ui.NewTable("Version", "Name", "Status", "Applied At")
	for _, s := range statuses {
		status := "pending"
		appliedAt := ""
		if s.Applied {
			status = "applied"
			if s.Unverified {
				status = "applied (unverified)"
			}
			if s.Missing {
				status += ", file missing"
			}
			appliedAt = s.AppliedAt.Format("2006-01-02 15:04:05")
		}
		tbl.AddRow(s.Version, s.Name, status, appliedAt)
	}
	tbl.Render()
	return nil
}

func runMigrateCreate(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	name := args[0]

	upPath, downPath, err := db.CreateMigrationFiles(dir, name)
	if err != nil {
		return err
	}

	ui.Successf("Created migration files:")
	fmt.Printf("  %s\n", upPath)
	fmt.Printf("  %s\n", downPath)
	return nil
}

// parseRevertCount parses the optional `down [N]` argument with strict
// full-string numeric validation: fmt.Sscanf accepted integer prefixes
// like "1junk" (audit neutron-03).
func parseRevertCount(args []string) (int, error) {
	if len(args) == 0 {
		return 1, nil
	}
	count, err := strconv.Atoi(args[0])
	if err != nil {
		return 0, fmt.Errorf("invalid count: %s", args[0])
	}
	if count < 1 {
		return 0, fmt.Errorf("count must be >= 1")
	}
	return count, nil
}

func runMigrateDown(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")

	count, err := parseRevertCount(args)
	if err != nil {
		return err
	}

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, _, sess, release, err := migrateSessionGuard(ctx, dir)
	if err != nil {
		return err
	}
	defer release()

	downFiles, err := db.ReadDownMigrationFiles(dir)
	if err != nil {
		return err
	}
	// Up files are needed for checksum verification of what is being
	// reverted: rolling back under a modified script record is exactly the
	// drift the checksum exists to catch.
	upFiles, err := db.ReadMigrationFiles(dir)
	if err != nil {
		return err
	}

	applied, err := prepareHistoryRun(ctx, client, sess, upFiles)
	if err != nil {
		return err
	}

	if _, err := db.VerifyAppliedChecksums(upFiles, applied); err != nil {
		return err
	}

	// Preflight before executing any SQL: the newest `count` applied
	// migrations — by version order, not by which down files happen to be
	// present — must each have a non-empty down file. Otherwise a missing
	// down file for a newer migration would be silently skipped and an older
	// one reverted beneath it.
	toRevert, err := selectRevertFrontier(applied, downFiles, count)
	if err != nil {
		return err
	}

	if len(toRevert) == 0 {
		ui.Infof("No migrations to revert")
		return nil
	}

	// Revert migrations
	for _, f := range toRevert {
		spinner := ui.NewSpinner(fmt.Sprintf("Reverting %s_%s...", f.Version, f.Name))
		if err := sess.RevertMigration(ctx, f); err != nil {
			spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed %s_%s: %v", f.Version, f.Name, err))
			return err
		}
		spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Reverted %s_%s", f.Version, f.Name))
	}

	ui.Successf("Reverted %d migration(s)", len(toRevert))
	return nil
}

// selectRevertFrontier returns the down migrations to revert: the newest
// `count` applied versions in version order, newest first. Every one of them
// must have a non-empty down file — otherwise the rollback is aborted before
// any SQL runs, because skipping a newer migration and reverting an older one
// beneath it corrupts the schema.
func selectRevertFrontier(applied []db.MigrationRecord, downFiles []db.MigrationFile, count int) ([]db.MigrationFile, error) {
	downByVersion := make(map[string]db.MigrationFile, len(downFiles))
	for _, f := range downFiles {
		downByVersion[f.Version] = f
	}

	sorted := append([]db.MigrationRecord(nil), applied...)
	sort.Slice(sorted, func(i, j int) bool {
		return db.CompareVersions(sorted[i].Version, sorted[j].Version) > 0
	})

	var toRevert []db.MigrationFile
	for _, rec := range sorted {
		if len(toRevert) == count {
			break
		}
		f, ok := downByVersion[rec.Version]
		if !ok {
			return nil, fmt.Errorf("aborting rollback: applied migration %s has no down migration file", rec.Version)
		}
		if strings.TrimSpace(f.SQL) == "" {
			return nil, fmt.Errorf("aborting rollback: down migration for applied version %s is empty", rec.Version)
		}
		toRevert = append(toRevert, f)
	}
	return toRevert, nil
}
