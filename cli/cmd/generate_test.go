package cmd

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestGenerateCommand(t *testing.T) {
	tests := []struct {
		name      string
		cmd       *cobra.Command
		shouldErr bool
	}{
		{
			name:      "generate command exists",
			cmd:       generateCmd,
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.cmd.Use != "generate" {
				t.Errorf("expected Use to be 'generate', got %q", tt.cmd.Use)
			}
			if tt.cmd.RunE == nil {
				t.Error("expected RunE to be set")
			}
		})
	}
}

func TestGenerateCommandFlags(t *testing.T) {
	flags := generateCmd.Flags()

	if _, err := flags.GetString("table"); err != nil {
		t.Errorf("missing --table flag: %v", err)
	}
	if _, err := flags.GetString("schema"); err != nil {
		t.Errorf("missing --schema flag: %v", err)
	}
	if _, err := flags.GetString("lang"); err != nil {
		t.Errorf("missing --lang flag: %v", err)
	}
	if _, err := flags.GetString("out"); err != nil {
		t.Errorf("missing --out flag: %v", err)
	}
	if _, err := flags.GetBool("all"); err != nil {
		t.Errorf("missing --all flag: %v", err)
	}
}

func TestExtensionForLang(t *testing.T) {
	tests := []struct {
		lang string
		want string
	}{
		{"go", ".go"},
		{"ts", ".ts"},
		{"rust", ".rs"},
		{"python", ".py"},
		{"elixir", ".ex"},
		{"zig", ".zig"},
		{"unknown", ".txt"},
	}

	for _, tt := range tests {
		t.Run(tt.lang, func(t *testing.T) {
			got := extensionForLang(tt.lang)
			if got != tt.want {
				t.Errorf("extensionForLang(%q) = %q, want %q", tt.lang, got, tt.want)
			}
		})
	}
}
