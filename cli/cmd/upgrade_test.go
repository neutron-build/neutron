package cmd

import (
	"errors"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/selfupdate"
)

func TestUpgradeCommand(t *testing.T) {
	if upgradeCmd.Use != "upgrade" {
		t.Errorf("upgradeCmd.Use = %q, want %q", upgradeCmd.Use, "upgrade")
	}
	if upgradeCmd.Short == "" {
		t.Error("upgradeCmd.Short is empty")
	}
	if upgradeCmd.RunE == nil {
		t.Error("upgradeCmd.RunE is nil")
	}
}

// An explicit upgrade whose update check fails must return an error, not
// warn-and-succeed: scripts could not tell a failed check from "already
// latest" (audit neutron-02).
func TestRunUpgradeReturnsErrorWhenCheckFails(t *testing.T) {
	orig := checkForUpdate
	defer func() { checkForUpdate = orig }()
	checkForUpdate = func(currentVersion string) (*selfupdate.Release, bool, error) {
		return nil, false, errors.New("network unreachable")
	}

	err := runUpgrade(upgradeCmd, nil)
	if err == nil {
		t.Fatal("runUpgrade returned nil for a failed update check")
	}
}
