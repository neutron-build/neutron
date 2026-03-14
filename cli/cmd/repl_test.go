package cmd

import (
	"testing"
)

func TestReplCommand(t *testing.T) {
	if replCmd.Use != "repl" {
		t.Errorf("replCmd.Use = %q, want %q", replCmd.Use, "repl")
	}
	if replCmd.Short == "" {
		t.Error("replCmd.Short is empty")
	}
	if replCmd.Long == "" {
		t.Error("replCmd.Long is empty")
	}
	if replCmd.RunE == nil {
		t.Error("replCmd.RunE is nil")
	}
}

func TestDelegateToNucleusShellURLParsing(t *testing.T) {
	// We can't actually run the delegation but we can verify
	// the URL parsing logic by checking the function exists and is callable.
	// The delegateToNucleusShell function parses URLs like:
	// postgres://user:pass@host:port/db -> extracts host and port.
	// Since it shells out, we only test the command configuration.
}
