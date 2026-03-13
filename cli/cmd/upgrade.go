package cmd

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/neutron-build/neutron/cli/internal/selfupdate"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(upgradeCmd)
}

var upgradeCmd = &cobra.Command{
	Use:   "upgrade",
	Short: "Upgrade the Neutron CLI to the latest version",
	RunE:  runUpgrade,
}

func runUpgrade(cmd *cobra.Command, args []string) error {
	// Detect install method to avoid corrupting Homebrew state
	method := selfupdate.DetectInstallMethod()
	if method == selfupdate.InstallHomebrew {
		ui.Infof("Installed via Homebrew — delegating to brew upgrade")
		brewCmd := exec.Command("brew", "upgrade", "neutron")
		brewCmd.Stdout = os.Stdout
		brewCmd.Stderr = os.Stderr
		return brewCmd.Run()
	}

	spinner := ui.NewSpinner("Checking for updates...")

	release, hasUpdate, err := selfupdate.CheckForUpdate(version)
	if err != nil {
		spinner.StopWithMessage(ui.WarnMark, fmt.Sprintf("Could not check: %v", err))
		return nil
	}

	if !hasUpdate {
		spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Already at latest version (%s)", version))
		return nil
	}

	spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("New version available: %s (current: %s)", release.TagName, version))

	if !ui.Confirm("Upgrade now?") {
		fmt.Println("Cancelled.")
		return nil
	}

	spinner = ui.NewSpinner("Downloading...")
	if err := selfupdate.DownloadAndReplace(release); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Upgrade failed: %v", err))
		return err
	}

	spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Upgraded to %s", release.TagName))
	return nil
}
