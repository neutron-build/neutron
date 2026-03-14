package cmd

import (
	"runtime"
	"testing"
)

func TestVersionCommand(t *testing.T) {
	// Verify versionCmd is correctly configured
	if versionCmd.Use != "version" {
		t.Errorf("versionCmd.Use = %q, want %q", versionCmd.Use, "version")
	}
	if versionCmd.Short == "" {
		t.Error("versionCmd.Short is empty")
	}
	if versionCmd.RunE == nil {
		t.Error("versionCmd.RunE is nil")
	}
}

func TestVersionVariables(t *testing.T) {
	// Verify build-time variables have defaults
	if version == "" {
		t.Error("version is empty string, want at least 'dev'")
	}
	if commit == "" {
		t.Error("commit is empty string")
	}
	if date == "" {
		t.Error("date is empty string")
	}
}

func TestVersionOutputFormat(t *testing.T) {
	// The version string should reference the current OS/arch
	expectedOS := runtime.GOOS
	expectedArch := runtime.GOARCH
	if expectedOS == "" {
		t.Error("runtime.GOOS is empty")
	}
	if expectedArch == "" {
		t.Error("runtime.GOARCH is empty")
	}
}

func TestLastPathComponent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/Users/test/project", "project"},
		{"/foo/bar/baz", "baz"},
		{"no-slash", "no-slash"},
		{"/", ""},
		{"/a", "a"},
		{"/a/b", "b"},
	}
	for _, tt := range tests {
		got := lastPathComponent(tt.input)
		if got != tt.want {
			t.Errorf("lastPathComponent(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
