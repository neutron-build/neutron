package db

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindSeedFileExplicitPath(t *testing.T) {
	dir := t.TempDir()
	seedPath := filepath.Join(dir, "custom_seed.sql")
	os.WriteFile(seedPath, []byte("INSERT INTO users (name) VALUES ('test');"), 0644)

	result, err := FindSeedFile(seedPath)
	if err != nil {
		t.Fatalf("FindSeedFile() error: %v", err)
	}
	if result != seedPath {
		t.Errorf("FindSeedFile() = %q, want %q", result, seedPath)
	}
}

func TestFindSeedFileExplicitPathNotFound(t *testing.T) {
	_, err := FindSeedFile("/nonexistent/seed.sql")
	if err == nil {
		t.Fatal("expected error for nonexistent seed file")
	}
}

func TestFindSeedFileDefault(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	// Create the first default path
	os.MkdirAll(filepath.Join(dir, "seeds"), 0755)
	seedPath := filepath.Join(dir, "seeds", "seed.sql")
	os.WriteFile(seedPath, []byte("SELECT 1;"), 0644)

	result, err := FindSeedFile("")
	if err != nil {
		t.Fatalf("FindSeedFile() error: %v", err)
	}
	if result != "seeds/seed.sql" {
		t.Errorf("FindSeedFile() = %q, want %q", result, "seeds/seed.sql")
	}
}

func TestFindSeedFileSecondDefault(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	// Create only the second default path (seed.sql in root)
	os.WriteFile(filepath.Join(dir, "seed.sql"), []byte("SELECT 1;"), 0644)

	result, err := FindSeedFile("")
	if err != nil {
		t.Fatalf("FindSeedFile() error: %v", err)
	}
	if result != "seed.sql" {
		t.Errorf("FindSeedFile() = %q, want %q", result, "seed.sql")
	}
}

func TestFindSeedFileThirdDefault(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	// Create only the third default path
	os.MkdirAll(filepath.Join(dir, "seeds"), 0755)
	os.WriteFile(filepath.Join(dir, "seeds", "data.sql"), []byte("SELECT 1;"), 0644)

	result, err := FindSeedFile("")
	if err != nil {
		t.Fatalf("FindSeedFile() error: %v", err)
	}
	if result != "seeds/data.sql" {
		t.Errorf("FindSeedFile() = %q, want %q", result, "seeds/data.sql")
	}
}

func TestFindSeedFileNoneFound(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	_, err := FindSeedFile("")
	if err == nil {
		t.Fatal("expected error when no seed file found")
	}
}

func TestDefaultSeedPaths(t *testing.T) {
	if len(DefaultSeedPaths) == 0 {
		t.Error("DefaultSeedPaths is empty")
	}

	expected := []string{
		"seeds/seed.sql",
		"seed.sql",
		"seeds/data.sql",
	}

	if len(DefaultSeedPaths) != len(expected) {
		t.Fatalf("DefaultSeedPaths len = %d, want %d", len(DefaultSeedPaths), len(expected))
	}

	for i, p := range expected {
		if DefaultSeedPaths[i] != p {
			t.Errorf("DefaultSeedPaths[%d] = %q, want %q", i, DefaultSeedPaths[i], p)
		}
	}
}
