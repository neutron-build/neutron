package cmd

import (
	"os"
	"testing"
)

func TestDevCommand(t *testing.T) {
	if devCmd.Use != "dev" {
		t.Errorf("devCmd.Use = %q, want %q", devCmd.Use, "dev")
	}
	if devCmd.Short == "" {
		t.Error("devCmd.Short is empty")
	}
	if devCmd.Long == "" {
		t.Error("devCmd.Long is empty")
	}
	if devCmd.RunE == nil {
		t.Error("devCmd.RunE is nil")
	}
}

func TestDevCommandFailsInEmptyDir(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := runDev(devCmd, nil)
	if err == nil {
		t.Fatal("expected error when no project detected")
	}
}
