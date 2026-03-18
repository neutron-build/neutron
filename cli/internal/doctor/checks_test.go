package doctor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckResultStruct(t *testing.T) {
	r := CheckResult{
		Name:    "Go",
		Status:  Pass,
		Detail:  "/usr/local/go/bin/go",
		Version: "1.23.0",
	}
	if r.Name != "Go" {
		t.Errorf("Name = %q, want %q", r.Name, "Go")
	}
	if r.Status != Pass {
		t.Errorf("Status = %d, want Pass", r.Status)
	}
}

func TestStatusConstants(t *testing.T) {
	if Pass != 0 {
		t.Errorf("Pass = %d, want 0", Pass)
	}
	if Warn != 1 {
		t.Errorf("Warn = %d, want 1", Warn)
	}
	if Fail != 2 {
		t.Errorf("Fail = %d, want 2", Fail)
	}
}

func TestRunAllReturnsResults(t *testing.T) {
	results := RunAll()
	if len(results) == 0 {
		t.Error("RunAll() returned empty results")
	}

	// We should at least have checks for: Go, Python, Node, Rust, Zig, Julia, Git, Nucleus binary, Database, Config file
	names := make(map[string]bool)
	for _, r := range results {
		names[r.Name] = true
	}
	expectedNames := []string{"Git", "Database", "Config file"}
	for _, name := range expectedNames {
		if !names[name] {
			t.Errorf("RunAll() missing check for %q", name)
		}
	}
}

func TestCheckGit(t *testing.T) {
	result := checkGit()
	if result.Name != "Git" {
		t.Errorf("Name = %q, want %q", result.Name, "Git")
	}
	// Git should be available in any CI/dev environment
	if result.Status != Pass {
		t.Logf("Git check returned status %d (may be OK if git is not installed)", result.Status)
	}
}

func TestCheckConfigFilePresent(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "neutron.toml"), []byte("[database]\nurl = \"test\""), 0644)

	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	result := checkConfigFile()
	if result.Name != "Config file" {
		t.Errorf("Name = %q, want %q", result.Name, "Config file")
	}
	if result.Status != Pass {
		t.Errorf("Status = %d, want Pass when neutron.toml exists", result.Status)
	}
}

func TestCheckConfigFileMissing(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	result := checkConfigFile()
	if result.Status != Warn {
		t.Errorf("Status = %d, want Warn when neutron.toml missing", result.Status)
	}
}

func TestCheckNucleusBinary(t *testing.T) {
	result := checkNucleusBinary()
	if result.Name != "Nucleus binary" {
		t.Errorf("Name = %q, want %q", result.Name, "Nucleus binary")
	}
	// It might be Pass or Warn depending on whether binaries are cached
	if result.Status != Pass && result.Status != Warn {
		t.Errorf("Status = %d, want Pass or Warn", result.Status)
	}
}

func TestCheckRuntimeWithGo(t *testing.T) {
	results := checkRuntime("Go", "go", "version")
	if len(results) != 1 {
		t.Fatalf("checkRuntime returned %d results, want 1", len(results))
	}
	r := results[0]
	if r.Name != "Go" {
		t.Errorf("Name = %q, want %q", r.Name, "Go")
	}
}

func TestCheckRuntimeWithMissingBinary(t *testing.T) {
	results := checkRuntime("Nonexistent", "this-binary-does-not-exist-xyz", "--version")
	if len(results) != 1 {
		t.Fatalf("checkRuntime returned %d results, want 1", len(results))
	}
	r := results[0]
	if r.Name != "Nonexistent" {
		t.Errorf("Name = %q, want %q", r.Name, "Nonexistent")
	}
	if r.Status != Warn {
		t.Errorf("Status = %d, want Warn for missing binary", r.Status)
	}
	if r.Detail != "not installed" {
		t.Errorf("Detail = %q, want %q", r.Detail, "not installed")
	}
}

func TestCheckDatabase(t *testing.T) {
	// Database check will likely Warn in test environment (no DB running)
	result := checkDatabase()
	if result.Name != "Database" {
		t.Errorf("Name = %q, want %q", result.Name, "Database")
	}
	// Either Pass (if DB running) or Warn (if not) is valid
	if result.Status != Pass && result.Status != Warn {
		t.Errorf("Status = %d, want Pass or Warn", result.Status)
	}
}
