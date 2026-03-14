package cmd

import (
	"testing"
)

func TestRootCommand(t *testing.T) {
	if rootCmd.Use != "neutron" {
		t.Errorf("rootCmd.Use = %q, want %q", rootCmd.Use, "neutron")
	}
	if rootCmd.Short == "" {
		t.Error("rootCmd.Short is empty")
	}
	if rootCmd.Long == "" {
		t.Error("rootCmd.Long is empty")
	}
}

func TestRootCommandSilenceConfig(t *testing.T) {
	if !rootCmd.SilenceUsage {
		t.Error("rootCmd.SilenceUsage should be true")
	}
	if !rootCmd.SilenceErrors {
		t.Error("rootCmd.SilenceErrors should be true")
	}
}

func TestRootPersistentFlags(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"config"},
		{"url"},
		{"verbose"},
		{"no-color"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := rootCmd.PersistentFlags().Lookup(tt.name)
			if flag == nil {
				t.Errorf("rootCmd missing --%s persistent flag", tt.name)
			}
		})
	}
}

func TestAllSubcommandsRegistered(t *testing.T) {
	expectedCmds := []string{
		"version", "doctor", "init", "new", "dev",
		"db", "migrate", "seed", "native", "desktop",
		"studio", "mcp", "repl", "upgrade", "completion",
	}

	cmds := rootCmd.Commands()
	nameSet := make(map[string]bool)
	for _, c := range cmds {
		nameSet[c.Name()] = true
	}

	for _, name := range expectedCmds {
		if !nameSet[name] {
			t.Errorf("subcommand %q not registered on rootCmd", name)
		}
	}
}

func TestExecuteReturnsNoErrorForHelp(t *testing.T) {
	rootCmd.SetArgs([]string{"--help"})
	err := rootCmd.Execute()
	if err != nil {
		t.Errorf("Execute() with --help returned error: %v", err)
	}
}
