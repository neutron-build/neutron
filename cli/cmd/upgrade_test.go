package cmd

import (
	"testing"
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
