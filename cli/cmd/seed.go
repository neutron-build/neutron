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
	seedCmd.Flags().StringP("file", "f", "", "seed file path (default: seeds/seed.sql)")
	rootCmd.AddCommand(seedCmd)
}

var seedCmd = &cobra.Command{
	Use:   "seed",
	Short: "Seed the database with data",
	Long:  "Execute a SQL seed file against the database.",
	RunE:  runSeed,
}

func runSeed(cmd *cobra.Command, args []string) error {
	file, _ := cmd.Flags().GetString("file")
	url := config.DatabaseURL()

	seedPath, err := db.FindSeedFile(file)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	spinner := ui.NewSpinner(fmt.Sprintf("Seeding from %s...", seedPath))
	if err := client.RunSeedFile(ctx, seedPath); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Seed failed: %v", err))
		return err
	}

	spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Seeded from %s", seedPath))
	return nil
}
