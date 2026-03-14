package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	migrateCmd.Flags().String("dir", "migrations", "migrations directory")

	migrateStatusCmd.Flags().String("dir", "migrations", "migrations directory")

	migrateCreateCmd.Flags().String("dir", "migrations", "migrations directory")

	migrateDownCmd.Flags().String("dir", "migrations", "migrations directory")

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

func runMigrate(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	url := config.DatabaseURL()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
			return err
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func runMigrateDown(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	url := config.DatabaseURL()

	// Parse count argument (default 1)
	count := 1
	if len(args) > 0 {
		if _, err := fmt.Sscanf(args[0], "%d", &count); err != nil {
			return fmt.Errorf("invalid count: %s", args[0])
		}
		if count < 1 {
			return fmt.Errorf("count must be >= 1")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	if len(downFiles) == 0 {
		ui.Warnf("No down migration files found in %s", dir)
		return nil
	}

	// Get applied migrations
	applied, err := client.AppliedMigrations(ctx)
	if err != nil {
		return err
	}

	appliedMap := make(map[string]bool)
	for _, r := range applied {
		appliedMap[r.Version] = true
	}

	// Find which down migrations to revert (newest first)
	var toRevert []db.MigrationFile
	for _, f := range downFiles {
		if appliedMap[f.Version] && len(toRevert) < count {
			toRevert = append(toRevert, f)
		}
		if len(toRevert) >= count {
			break
		}
	}

	if len(toRevert) == 0 {
		ui.Infof("No migrations to revert")
		return nil
	}

	// Safety check: ensure each applied migration has a corresponding down file
	for _, f := range toRevert {
		// Check that the down file exists (we already have it from ReadDownMigrationFiles)
		if f.SQL == "" {
			return fmt.Errorf("down migration for version %s is empty", f.Version)
		}
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
