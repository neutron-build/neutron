package nucleus

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestBinDir(t *testing.T) {
	dir, err := BinDir()
	if err != nil {
		t.Fatalf("BinDir() error: %v", err)
	}
	if dir == "" {
		t.Error("BinDir() returned empty string")
	}
	if !strings.Contains(dir, ".neutron") {
		t.Errorf("BinDir() = %q, should contain .neutron", dir)
	}
	if !strings.HasSuffix(dir, "bin") {
		t.Errorf("BinDir() = %q, should end with bin", dir)
	}

	// Verify directory was created
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("BinDir directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("BinDir path is not a directory")
	}
}

func TestBinaryPath(t *testing.T) {
	path, err := BinaryPath("0.1.0")
	if err != nil {
		t.Fatalf("BinaryPath() error: %v", err)
	}
	if path == "" {
		t.Error("BinaryPath() returned empty string")
	}
	if !strings.Contains(path, "nucleus-0.1.0") {
		t.Errorf("BinaryPath() = %q, should contain nucleus-0.1.0", path)
	}
	if !strings.Contains(path, platformOS()) {
		t.Errorf("BinaryPath() = %q, should contain platform OS", path)
	}
	if !strings.Contains(path, platformArch()) {
		t.Errorf("BinaryPath() = %q, should contain platform arch", path)
	}
}

func TestFindOrDownloadCached(t *testing.T) {
	// Create a fake cached binary
	binDir, err := BinDir()
	if err != nil {
		t.Fatalf("BinDir() error: %v", err)
	}

	version := "99.99.99" // unlikely to conflict
	fakeName := "nucleus-" + version + "-" + platformOS() + "-" + platformArch()
	fakePath := filepath.Join(binDir, fakeName)
	os.WriteFile(fakePath, []byte("fake binary"), 0755)
	defer os.Remove(fakePath)

	// Isolate so FindLocal doesn't short-circuit by finding a monorepo binary.
	// Point NEUTRON_NUCLEUS_BIN at the versioned cache entry so FindLocal returns it.
	t.Setenv("NEUTRON_NUCLEUS_BIN", fakePath)

	path, err := FindOrDownload(version)
	if err != nil {
		t.Fatalf("FindOrDownload() error: %v", err)
	}
	if path != fakePath {
		t.Errorf("FindOrDownload() = %q, want cached path %q", path, fakePath)
	}
}

func TestPlatformOS(t *testing.T) {
	os := platformOS()
	validOS := []string{"darwin", "linux", "windows"}
	found := false
	for _, valid := range validOS {
		if os == valid {
			found = true
			break
		}
	}
	if !found && os != runtime.GOOS {
		t.Errorf("platformOS() = %q, not a recognized platform", os)
	}
}

func TestPlatformArch(t *testing.T) {
	arch := platformArch()
	validArch := []string{"amd64", "arm64"}
	found := false
	for _, valid := range validArch {
		if arch == valid {
			found = true
			break
		}
	}
	if !found && arch != runtime.GOARCH {
		t.Errorf("platformArch() = %q, not a recognized arch", arch)
	}
}

func TestFindLocal_EnvVar(t *testing.T) {
	// Create a temp file to act as the nucleus binary
	tmp, err := os.CreateTemp("", "nucleus-test-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())
	os.Chmod(tmp.Name(), 0755)

	t.Setenv("NEUTRON_NUCLEUS_BIN", tmp.Name())

	path, err := FindLocal()
	if err != nil {
		t.Fatalf("FindLocal() error: %v", err)
	}
	if path != tmp.Name() {
		t.Errorf("FindLocal() = %q, want %q", path, tmp.Name())
	}
}

func TestFindLocal_MonorepoDetection(t *testing.T) {
	// Create a temp dir simulating a monorepo with nucleus/target/release/nucleus
	tmpDir, err := os.MkdirTemp("", "neutron-monorepo-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Resolve symlinks (macOS /var -> /private/var) so paths match os.Getwd()
	tmpDir, err = filepath.EvalSymlinks(tmpDir)
	if err != nil {
		t.Fatalf("eval symlinks: %v", err)
	}

	releaseDir := filepath.Join(tmpDir, "nucleus", "target", "release")
	if err := os.MkdirAll(releaseDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	fakeBin := filepath.Join(releaseDir, "nucleus")
	if err := os.WriteFile(fakeBin, []byte("fake"), 0755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	// Clear env var so it doesn't short-circuit
	t.Setenv("NEUTRON_NUCLEUS_BIN", "")

	// Save and restore cwd
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer os.Chdir(origDir)

	// Remove nucleus from PATH to avoid interference
	t.Setenv("PATH", "")

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	path, err := FindLocal()
	if err != nil {
		t.Fatalf("FindLocal() error: %v", err)
	}
	if path != fakeBin {
		t.Errorf("FindLocal() = %q, want %q", path, fakeBin)
	}
}

func TestFindLocal_PATH(t *testing.T) {
	// Create a temp dir with a fake nucleus binary, add to PATH
	tmpDir, err := os.MkdirTemp("", "neutron-path-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fakeBin := filepath.Join(tmpDir, "nucleus")
	if err := os.WriteFile(fakeBin, []byte("fake"), 0755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	// Clear env var so it doesn't short-circuit
	t.Setenv("NEUTRON_NUCLEUS_BIN", "")
	t.Setenv("PATH", tmpDir)

	path, err := FindLocal()
	if err != nil {
		t.Fatalf("FindLocal() error: %v", err)
	}

	// exec.LookPath returns the full path
	expected, _ := exec.LookPath("nucleus")
	if path != expected {
		t.Errorf("FindLocal() = %q, want %q", path, expected)
	}
}

func TestFindLocal_NotFound(t *testing.T) {
	// Clear everything that could match
	t.Setenv("NEUTRON_NUCLEUS_BIN", "")
	t.Setenv("PATH", "")

	// Use a temp dir with no nucleus artifacts
	tmpDir, err := os.MkdirTemp("", "neutron-empty-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer os.Chdir(origDir)
	os.Chdir(tmpDir)

	// Override HOME so the ~/.neutron/bin fallback doesn't find anything
	t.Setenv("HOME", tmpDir)

	_, err = FindLocal()
	if err == nil {
		t.Fatal("FindLocal() should return error when nothing exists")
	}
}

func TestFindLocal_FallbackToUnversioned(t *testing.T) {
	// Create a temp HOME with ~/.neutron/bin/nucleus
	tmpHome, err := os.MkdirTemp("", "neutron-home-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpHome)

	binDir := filepath.Join(tmpHome, ".neutron", "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	fakeBin := filepath.Join(binDir, "nucleus")
	if err := os.WriteFile(fakeBin, []byte("fake"), 0755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	// Clear env var and PATH so earlier checks don't match
	t.Setenv("NEUTRON_NUCLEUS_BIN", "")
	t.Setenv("PATH", "")
	t.Setenv("HOME", tmpHome)

	// Cd to a dir with no monorepo artifacts
	tmpDir, err := os.MkdirTemp("", "neutron-nomonorepo-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer os.Chdir(origDir)
	os.Chdir(tmpDir)

	path, err := FindLocal()
	if err != nil {
		t.Fatalf("FindLocal() error: %v", err)
	}
	if path != fakeBin {
		t.Errorf("FindLocal() = %q, want %q", path, fakeBin)
	}
}
