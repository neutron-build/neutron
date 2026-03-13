package cmd

import (
	"fmt"
	"os"

	"github.com/neutron-build/neutron/cli/internal/delegate"
	"github.com/neutron-build/neutron/cli/internal/detect"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(devCmd)
}

var devCmd = &cobra.Command{
	Use:   "dev",
	Short: "Start the development server",
	Long:  "Detects the project language and delegates to the appropriate dev server.",
	RunE:  runDev,
}

func runDev(cmd *cobra.Command, args []string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	lang := detect.DetectLanguage(cwd)
	if lang == detect.Unknown {
		return fmt.Errorf("could not detect project language — are you in a Neutron project directory?\nHint: run 'neutron init' to set up the project")
	}

	ui.Infof("Detected %s project — starting dev server...", lang.DisplayName())

	return delegate.RunDevServer(lang, cwd)
}
