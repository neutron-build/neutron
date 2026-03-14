package cmd

import (
	"testing"
)

func TestNewCommand(t *testing.T) {
	if newCmd.Use != "new <project-name>" {
		t.Errorf("newCmd.Use = %q, want %q", newCmd.Use, "new <project-name>")
	}
	if newCmd.Short == "" {
		t.Error("newCmd.Short is empty")
	}
	if newCmd.RunE == nil {
		t.Error("newCmd.RunE is nil")
	}
}

func TestNewCommandRequiresExactArgs(t *testing.T) {
	// cobra.ExactArgs(1) should be set
	err := newCmd.Args(newCmd, []string{})
	if err == nil {
		t.Error("expected error for 0 args")
	}
	err = newCmd.Args(newCmd, []string{"one", "two"})
	if err == nil {
		t.Error("expected error for 2 args")
	}
	err = newCmd.Args(newCmd, []string{"my-project"})
	if err != nil {
		t.Errorf("expected no error for 1 arg, got: %v", err)
	}
}

func TestNewCommandHasLangFlag(t *testing.T) {
	flag := newCmd.Flags().Lookup("lang")
	if flag == nil {
		t.Fatal("newCmd missing --lang flag")
	}
	if flag.Shorthand != "l" {
		t.Errorf("--lang shorthand = %q, want %q", flag.Shorthand, "l")
	}
}

func TestNewInvalidLanguage(t *testing.T) {
	// Reset the flag to a known invalid value
	newCmd.Flags().Set("lang", "brainfuck")
	err := runNew(newCmd, []string{"test-project"})
	if err == nil {
		t.Fatal("expected error for unsupported language")
	}
}
