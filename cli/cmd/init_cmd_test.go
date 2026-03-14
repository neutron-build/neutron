package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitCommand(t *testing.T) {
	if initCmd.Use != "init" {
		t.Errorf("initCmd.Use = %q, want %q", initCmd.Use, "init")
	}
	if initCmd.Short == "" {
		t.Error("initCmd.Short is empty")
	}
	if initCmd.RunE == nil {
		t.Error("initCmd.RunE is nil")
	}
}

func TestInitCommandHasLangFlag(t *testing.T) {
	flag := initCmd.Flags().Lookup("lang")
	if flag == nil {
		t.Fatal("initCmd missing --lang flag")
	}
	if flag.Shorthand != "l" {
		t.Errorf("--lang shorthand = %q, want %q", flag.Shorthand, "l")
	}
}

func TestInitCreatesNeutronToml(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	// Create a go.mod so it detects Go
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test"), 0644)

	err := runInit(initCmd, nil)
	if err != nil {
		t.Fatalf("runInit() error: %v", err)
	}

	// Verify neutron.toml was created
	data, err := os.ReadFile(filepath.Join(dir, "neutron.toml"))
	if err != nil {
		t.Fatalf("neutron.toml not created: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, "[database]") {
		t.Error("neutron.toml missing [database] section")
	}
	if !strings.Contains(content, "[studio]") {
		t.Error("neutron.toml missing [studio] section")
	}
	if !strings.Contains(content, "[project]") {
		t.Error("neutron.toml missing [project] section")
	}
	if !strings.Contains(content, `lang = "go"`) {
		t.Errorf("neutron.toml should contain lang = \"go\", got:\n%s", content)
	}
}

func TestInitCreatesMigrationsDir(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	// Create a pyproject.toml so it detects Python
	os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte("[project]\nname=\"test\""), 0644)

	err := runInit(initCmd, nil)
	if err != nil {
		t.Fatalf("runInit() error: %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, "migrations"))
	if err != nil {
		t.Fatalf("migrations dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("migrations is not a directory")
	}
}

func TestInitFailsIfAlreadyExists(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	// Create neutron.toml first
	os.WriteFile(filepath.Join(dir, "neutron.toml"), []byte("existing"), 0644)

	err := runInit(initCmd, nil)
	if err == nil {
		t.Fatal("expected error when neutron.toml already exists")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error should mention 'already exists', got: %v", err)
	}
}

func TestInitDefaultsToGoWhenUnknown(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	// Empty dir, no markers to detect
	err := runInit(initCmd, nil)
	if err != nil {
		t.Fatalf("runInit() error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "neutron.toml"))
	if !strings.Contains(string(data), `lang = "go"`) {
		t.Errorf("expected default lang=go when unknown, got:\n%s", string(data))
	}
}
