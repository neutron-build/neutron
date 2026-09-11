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

	migrateDownCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateDownCmd.Flags().Duration("timeout", 60*time.Second, "total time budget for the rollback batch (0 = no deadline)")

	migrateCmd.AddCommand(migrateStatusCmd)
	migrateCmd.AddCommand(migrateCreateCmd)
	migrateCmd.AddCommand(migrateDownCmd)
	rootCmd.AddCommand(migrateCmd)
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run database migrations",
	Long:  "Apply pending SQL migration files to the database.",
	RunE:  runMigrate,
}

var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	RunE:  runMigrateStatus,
}

var migrateCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new migration file",
	Args:  cobra.ExactArgs(1),
	RunE:  runMigrateCreate,
}

var migrateDownCmd = &cobra.Command{
	Use:   "down [N]",
	Short: "Revert N migrations (default 1)",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runMigrateDown,
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

func runMigrate(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	url := config.DatabaseURL()

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	// Ensure tracking table exists
	if err := client.EnsureMigrationTable(ctx); err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	// Read migration files
	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		ui.Warnf("No migration files found in %s", dir)
		return nil
	}

	// Get applied migrations
	applied, err := client.AppliedMigrations(ctx)
	if err != nil {
		return err
	}
	appliedSet := make(map[string]bool)
	for _, r := range applied {
		appliedSet[r.Version] = true
	}

	// Apply pending
	var count int
	for _, f := range files {
		if appliedSet[f.Version] {
			continue
		}

		spinner := ui.NewSpinner(fmt.Sprintf("Applying %s_%s...", f.Version, f.Name))
		if err := client.ApplyMigration(ctx, f); err != nil {
			spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed %s_%s: %v", f.Version, f.Name, err))
			// Name the interruption boundary: a partial batch is a
			// different operational state than an untouched one.
			return fmt.Errorf("interrupted after %d of %d pending migration(s) (failed at %s_%s): %w",
				count, len(files)-len(applied), f.Version, f.Name, err)
		}
		spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Applied %s_%s", f.Version, f.Name))
		count++
	}

	if count == 0 {
		ui.Successf("Database is up to date (%d migrations applied)", len(applied))
	} else {
		ui.Successf("Applied %d migration(s)", count)
	}

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
			if s.Missing {
				status = "applied (file missing)"
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
	url := config.DatabaseURL()

	count, err := parseRevertCount(args)
	if err != nil {
		return err
	}

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	// Ensure tracking table exists
	if err := client.EnsureMigrationTable(ctx); err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	// Read down migration files
	downFiles, err := db.ReadDownMigrationFiles(dir)
	if err != nil {
		return err
	}

	// Get applied migrations
	applied, err := client.AppliedMigrations(ctx)
	if err != nil {
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
		if err := client.RevertMigration(ctx, f); err != nil {
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
