package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/nucleus"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	dbStartCmd.Flags().Int("port", 0, "port to listen on (default from config)")
	dbStartCmd.Flags().String("data-dir", "", "data directory (default from config)")
	dbStartCmd.Flags().Bool("memory", false, "use in-memory storage (no persistence)")

	dbResetCmd.Flags().Bool("force", false, "skip confirmation prompt")

	dbCmd.AddCommand(dbStartCmd)
	dbCmd.AddCommand(dbStopCmd)
	dbCmd.AddCommand(dbStatusCmd)
	dbCmd.AddCommand(dbResetCmd)
	rootCmd.AddCommand(dbCmd)
}

var dbCmd = &cobra.Command{
	Use:   "db",
	Short: "Manage the Nucleus database",
	Long:  "Start, stop, and manage your local Nucleus database instance.",
}

var dbStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a local Nucleus instance",
	Long:  "Downloads Nucleus if needed, then starts a local database server.",
	RunE:  runDBStart,
}

var dbStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the local Nucleus instance",
	RunE:  runDBStop,
}

var dbStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check database status",
	RunE:  runDBStatus,
}

var dbResetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Drop and recreate the database",
	RunE:  runDBReset,
}

func runDBStart(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	if port == 0 {
		port = config.NucleusPort()
	}
	dataDir, _ := cmd.Flags().GetString("data-dir")
	if dataDir == "" {
		dataDir = config.NucleusDataDir()
	}
	memory, _ := cmd.Flags().GetBool("memory")

	// Check if already running
	if running, pid := nucleus.IsRunning(); running {
		ui.Warnf("Nucleus is already running (PID %d)", pid)
		return nil
	}

	// Resolve version and download if needed
	ver := config.NucleusVersion()
	spinner := ui.NewSpinner("Resolving Nucleus version...")
	resolvedVer, err := nucleus.ResolveVersion(ver)
	if err != nil {
		spinner.StopWithMessage(ui.WarnMark, fmt.Sprintf("Could not resolve version, using %s", resolvedVer))
	} else {
		spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Nucleus v%s", resolvedVer))
	}

	spinner = ui.NewSpinner("Downloading Nucleus binary...")
	binaryPath, err := nucleus.FindOrDownload(resolvedVer)
	if err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Download failed: %v", err))
		return err
	}
	spinner.StopWithMessage(ui.CheckMark, "Binary ready")

	spinner = ui.NewSpinner(fmt.Sprintf("Starting Nucleus on port %d...", port))
	pid, err := nucleus.Start(binaryPath, port, dataDir, memory)
	if err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed to start: %v", err))
		return err
	}
	spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Nucleus running (PID %d, port %d)", pid, port))

	storageMode := "persistent"
	if memory {
		storageMode = "in-memory"
	}
	ui.KeyValue("mode", storageMode)
	ui.KeyValue("url", fmt.Sprintf("postgres://localhost:%d/neutron", port))
	return nil
}

func runDBStop(cmd *cobra.Command, args []string) error {
	running, pid := nucleus.IsRunning()
	if !running {
		ui.Warnf("No running Nucleus instance found")
		return nil
	}

	spinner := ui.NewSpinner(fmt.Sprintf("Stopping Nucleus (PID %d)...", pid))
	if err := nucleus.Stop(); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed: %v", err))
		return err
	}
	spinner.StopWithMessage(ui.CheckMark, "Nucleus stopped")
	return nil
}

func runDBStatus(cmd *cobra.Command, args []string) error {
	// Check local process first
	if running, pid := nucleus.IsRunning(); running {
		ui.Successf("Local Nucleus running (PID %d)", pid)
	}

	url := config.DatabaseURL()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		ui.Errorf("Cannot connect to %s", url)
		return nil
	}
	defer client.Close()

	info, err := client.Status(ctx)
	if err != nil {
		ui.Errorf("Connected but status query failed: %v", err)
		return nil
	}

	ui.Header("Database Status")
	ui.KeyValue("url", info.URL)
	ui.KeyValue("version", info.Version)
	if info.IsNucleus {
		ui.KeyValue("nucleus", info.NucleusVersion)
	}
	ui.KeyValue("server time", info.ServerTime.Format(time.RFC3339))
	return nil
}

func runDBReset(cmd *cobra.Command, args []string) error {
	force, _ := cmd.Flags().GetBool("force")
	if !force {
		if !ui.Confirm("This will DROP and recreate the database. Continue?") {
			fmt.Println("Aborted.")
			return nil
		}
	}

	url := config.DatabaseURL()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	spinner := ui.NewSpinner("Resetting database...")

	// Drop all tables in public schema
	dropSQL := `DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;`

	if err := client.Exec(ctx, dropSQL); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Reset failed: %v", err))
		return err
	}

	spinner.StopWithMessage(ui.CheckMark, "Database reset complete")
	return nil
}
