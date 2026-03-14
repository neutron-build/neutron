package cmd

import (
	"testing"
)

func TestSeedCommand(t *testing.T) {
	if seedCmd.Use != "seed" {
		t.Errorf("seedCmd.Use = %q, want %q", seedCmd.Use, "seed")
	}
	if seedCmd.Short == "" {
		t.Error("seedCmd.Short is empty")
	}
	if seedCmd.RunE == nil {
		t.Error("seedCmd.RunE is nil")
	}
}

func TestSeedFileFlag(t *testing.T) {
	flag := seedCmd.Flags().Lookup("file")
	if flag == nil {
		t.Fatal("seedCmd missing --file flag")
	}
	if flag.Shorthand != "f" {
		t.Errorf("--file shorthand = %q, want %q", flag.Shorthand, "f")
	}
	if flag.DefValue != "" {
		t.Errorf("--file default = %q, want empty", flag.DefValue)
	}
}
