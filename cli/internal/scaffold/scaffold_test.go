package scaffold

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/detect"
)

func TestScaffoldPython(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := ScaffoldProject("test-app", detect.Python)
	if err != nil {
		t.Fatalf("ScaffoldProject(Python) error: %v", err)
	}

	// Verify key files exist
	expect := []string{
		"test-app/pyproject.toml",
		"test-app/app/main.py",
		"test-app/.gitignore",
		"test-app/neutron.toml",
		"test-app/migrations/001_init.up.sql",
	}
	for _, f := range expect {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected file %s to exist, got error: %v", f, err)
		}
	}

	// Verify neutron.toml contents
	data, _ := os.ReadFile(filepath.Join(dir, "test-app/neutron.toml"))
	if len(data) == 0 {
		t.Error("neutron.toml is empty")
	}
}

func TestScaffoldGo(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := ScaffoldProject("my-service", detect.Go)
	if err != nil {
		t.Fatalf("ScaffoldProject(Go) error: %v", err)
	}

	expect := []string{
		"my-service/go.mod",
		"my-service/cmd/server/main.go",
		"my-service/internal/handler/health.go",
		"my-service/neutron.toml",
		"my-service/migrations/001_init.up.sql",
		"my-service/migrations/001_init.down.sql",
	}
	for _, f := range expect {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected file %s to exist, got error: %v", f, err)
		}
	}
}

func TestScaffoldTypeScript(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := ScaffoldProject("ts-app", detect.TypeScript)
	if err != nil {
		t.Fatalf("ScaffoldProject(TypeScript) error: %v", err)
	}

	expect := []string{
		"ts-app/package.json",
		"ts-app/tsconfig.json",
		"ts-app/src/index.ts",
		"ts-app/neutron.toml",
	}
	for _, f := range expect {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected file %s to exist, got error: %v", f, err)
		}
	}
}

func TestScaffoldRust(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := ScaffoldProject("rust-svc", detect.Rust)
	if err != nil {
		t.Fatalf("ScaffoldProject(Rust) error: %v", err)
	}

	expect := []string{
		"rust-svc/Cargo.toml",
		"rust-svc/src/main.rs",
		"rust-svc/neutron.toml",
	}
	for _, f := range expect {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected file %s to exist, got error: %v", f, err)
		}
	}
}

func TestScaffoldAlreadyExists(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	os.Mkdir("existing", 0755)
	err := ScaffoldProject("existing", detect.Python)
	if err == nil {
		t.Fatal("expected error for existing directory, got nil")
	}
}

func TestScaffoldInvalidName(t *testing.T) {
	err := ScaffoldProject("", detect.Python)
	if err == nil {
		t.Fatal("expected error for empty name")
	}

	err = ScaffoldProject("bad name", detect.Python)
	if err == nil {
		t.Fatal("expected error for name with spaces")
	}

	err = ScaffoldProject("-starts-with-dash", detect.Python)
	if err == nil {
		t.Fatal("expected error for name starting with dash")
	}
}
