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

// checkForUpdate is a seam over selfupdate.CheckForUpdate so tests can
// inject a failed update check without the network.
var checkForUpdate = selfupdate.CheckForUpdate

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

	release, hasUpdate, err := checkForUpdate(version)
	if err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Could not check: %v", err))
		// An explicitly requested upgrade must fail loudly: scripts cannot
		// distinguish "already latest" from a network or release-metadata
		// failure if this returns nil. Best-effort warnings belong to
		// optional startup notifications, not the upgrade command.
		return fmt.Errorf("check for updates: %w", err)
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
	if err := selfupdate.DownloadAndReplace(cmd.Context(), release); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Upgrade failed: %v", err))
		return err
	}

	spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Upgraded to %s", release.TagName))
	return nil
}
