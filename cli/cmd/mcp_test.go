package cmd

import (
	"testing"
)

func TestMCPCommand(t *testing.T) {
	if mcpCmd.Use != "mcp" {
		t.Errorf("mcpCmd.Use = %q, want %q", mcpCmd.Use, "mcp")
	}
	if mcpCmd.Short == "" {
		t.Error("mcpCmd.Short is empty")
	}
	if mcpCmd.Long == "" {
		t.Error("mcpCmd.Long is empty")
	}
	if mcpCmd.RunE == nil {
		t.Error("mcpCmd.RunE is nil")
	}
}

func TestMCPFlags(t *testing.T) {
	tests := []struct {
		name     string
		defValue string
	}{
		{"db", ""},
		{"log", "false"},
		{"transport", "stdio"},
		{"port", "7700"},
		{"dump-schema", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := mcpCmd.Flags().Lookup(tt.name)
			if flag == nil {
				t.Fatalf("mcpCmd missing --%s flag", tt.name)
			}
			if flag.DefValue != tt.defValue {
				t.Errorf("--%s default = %q, want %q", tt.name, flag.DefValue, tt.defValue)
			}
		})
	}
}

func TestMCPRequiresDBURL(t *testing.T) {
	// Set dump-schema to empty so it tries to connect
	mcpCmd.Flags().Set("dump-schema", "")
	mcpCmd.Flags().Set("db", "")
	t.Setenv("DATABASE_URL", "")

	err := runMCP(mcpCmd, nil)
	if err == nil {
		t.Fatal("expected error when no database URL provided")
	}
}
