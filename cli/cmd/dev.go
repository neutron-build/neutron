package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/neutron-build/neutron/cli/internal/delegate"
	"github.com/neutron-build/neutron/cli/internal/detect"
	"github.com/neutron-build/neutron/cli/internal/project"
	"github.com/neutron-build/neutron/cli/internal/supervisor"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	devCmd.Flags().String("service", "", "application service to run with its dependencies")
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

	manifest, err := project.Discover(cwd, cfgFile)
	if err != nil {
		return applicationError(cmd, err)
	}
	selected, _ := cmd.Flags().GetString("service")
	if manifest != nil {
		plan, err := manifest.Build(selected)
		if err != nil {
			return applicationError(cmd, err)
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		err = supervisor.Run(ctx, plan, supervisor.Options{Output: cmd.OutOrStdout()})
		if err != nil && err != context.Canceled {
			return applicationError(cmd, err)
		}
		return err
	}
	if selected != "" {
		return applicationError(cmd, fmt.Errorf("--service requires an [application] manifest"))
	}
	lang := detect.DetectLanguage(cwd)
	if lang == detect.Unknown {
		return fmt.Errorf("could not detect project language — are you in a Neutron project directory?\nHint: run 'neutron init' to set up the project")
	}

	ui.Infof("Detected %s project — starting dev server...", lang.DisplayName())

	return delegate.RunDevServer(lang, cwd)
}
