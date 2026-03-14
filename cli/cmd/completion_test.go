package cmd

import (
	"bytes"
	"testing"
)

func TestCompletionCommand(t *testing.T) {
	if completionCmd.Use != "completion [bash|zsh|fish|powershell]" {
		t.Errorf("completionCmd.Use = %q, want %q", completionCmd.Use, "completion [bash|zsh|fish|powershell]")
	}
	if completionCmd.Short == "" {
		t.Error("completionCmd.Short is empty")
	}
	if completionCmd.RunE == nil {
		t.Error("completionCmd.RunE is nil")
	}
}

func TestCompletionValidArgs(t *testing.T) {
	expected := []string{"bash", "zsh", "fish", "powershell"}
	if len(completionCmd.ValidArgs) != len(expected) {
		t.Fatalf("ValidArgs len = %d, want %d", len(completionCmd.ValidArgs), len(expected))
	}
	for i, v := range expected {
		if completionCmd.ValidArgs[i] != v {
			t.Errorf("ValidArgs[%d] = %q, want %q", i, completionCmd.ValidArgs[i], v)
		}
	}
}

func TestCompletionRequiresExactArgs(t *testing.T) {
	err := completionCmd.Args(completionCmd, []string{})
	if err == nil {
		t.Error("expected error for 0 args")
	}
	err = completionCmd.Args(completionCmd, []string{"bash", "zsh"})
	if err == nil {
		t.Error("expected error for 2 args")
	}
}

func TestCompletionRejectsInvalidShell(t *testing.T) {
	err := completionCmd.Args(completionCmd, []string{"csh"})
	if err == nil {
		t.Error("expected error for invalid shell")
	}
}

func TestCompletionBashOutput(t *testing.T) {
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	err := rootCmd.GenBashCompletion(buf)
	if err != nil {
		t.Fatalf("GenBashCompletion error: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("bash completion output is empty")
	}
}

func TestCompletionZshOutput(t *testing.T) {
	buf := new(bytes.Buffer)
	err := rootCmd.GenZshCompletion(buf)
	if err != nil {
		t.Fatalf("GenZshCompletion error: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("zsh completion output is empty")
	}
}

func TestCompletionFishOutput(t *testing.T) {
	buf := new(bytes.Buffer)
	err := rootCmd.GenFishCompletion(buf, true)
	if err != nil {
		t.Fatalf("GenFishCompletion error: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("fish completion output is empty")
	}
}

func TestCompletionPowerShellOutput(t *testing.T) {
	buf := new(bytes.Buffer)
	err := rootCmd.GenPowerShellCompletionWithDesc(buf)
	if err != nil {
		t.Fatalf("GenPowerShellCompletionWithDesc error: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("powershell completion output is empty")
	}
}
