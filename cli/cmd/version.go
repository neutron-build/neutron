package cmd

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show CLI and Nucleus version information",
	RunE:  runVersion,
}

func runVersion(cmd *cobra.Command, args []string) error {
	fmt.Printf("neutron cli  %s  (%s/%s)\n", version, runtime.GOOS, runtime.GOARCH)

	// Try to detect a running Nucleus instance
	url := config.DatabaseURL()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		ui.KeyValue("nucleus", "not connected")
		return nil
	}
	defer conn.Close(ctx)

	var dbVersion string
	if err := conn.QueryRow(ctx, "SELECT VERSION()").Scan(&dbVersion); err != nil {
		ui.KeyValue("nucleus", "connected but could not query version")
		return nil
	}

	ui.KeyValue("database", dbVersion)
	return nil
}
