package nucleus

import (
	"os"
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
