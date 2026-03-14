package cmd

import (
	"testing"
)

func TestStudioCommand(t *testing.T) {
	if studioCmd.Use != "studio" {
		t.Errorf("studioCmd.Use = %q, want %q", studioCmd.Use, "studio")
	}
	if studioCmd.Short == "" {
		t.Error("studioCmd.Short is empty")
	}
	if studioCmd.Long == "" {
		t.Error("studioCmd.Long is empty")
	}
	if studioCmd.RunE == nil {
		t.Error("studioCmd.RunE is nil")
	}
}

func TestStudioPortFlag(t *testing.T) {
	flag := studioCmd.Flags().Lookup("port")
	if flag == nil {
		t.Fatal("studioCmd missing --port flag")
	}
	if flag.DefValue != "0" {
		t.Errorf("--port default = %q, want %q", flag.DefValue, "0")
	}
}
