package cmd

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/studio"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	studioCmd.Flags().Int("port", 0, "Studio port (default 4983)")
	studioCmd.Flags().String("migrations", "migrations", "application migrations directory shown in the inspection journey (applied history is read either way)")
	rootCmd.AddCommand(studioCmd)
}

var studioCmd = &cobra.Command{
	Use:   "studio",
	Short: "Launch Neutron Studio in the browser",
	Long:  "Start the embedded Studio web UI server and open it in your default browser.",
	RunE:  runStudio,
}

func runStudio(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	if port == 0 {
		port = config.StudioPort()
	}
	if port == 0 {
		port = 4983
	}

	srv, err := studio.NewServer(port)
	if err != nil {
		return fmt.Errorf("init studio: %w", err)
	}

	migrationsDir, _ := cmd.Flags().GetString("migrations")
	srv.SetMigrationsDir(migrationsDir)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	url := srv.URL()
	ui.Infof("Starting Studio at %s", url)

	// Open browser after brief startup window
	go func() {
		time.Sleep(200 * time.Millisecond)
		studio.OpenBrowser(url)
	}()

	fmt.Println()
	ui.Infof("Studio is running. Press Ctrl+C to stop.")
	fmt.Println()

	return srv.Start(ctx)
}
