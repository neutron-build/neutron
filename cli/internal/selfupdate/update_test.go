package selfupdate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExtractVersion(t *testing.T) {
	tests := []struct {
		tag  string
		want string
	}{
		{"cli/v1.2.3", "1.2.3"},
		{"cli/1.2.3", "1.2.3"},
		{"v1.2.3", "1.2.3"},
		{"1.2.3", "1.2.3"},
		{"cli/v0.1.0-beta", "0.1.0-beta"},
	}
	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			got := extractVersion(tt.tag)
			if got != tt.want {
				t.Errorf("extractVersion(%q) = %q, want %q", tt.tag, got, tt.want)
			}
		})
	}
}

func TestNormalizeVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"v1.2.3", "1.2.3"},
		{"1.2.3", "1.2.3"},
		{"v0.1.0", "0.1.0"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeVersion(tt.input)
			if got != tt.want {
				t.Errorf("normalizeVersion(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestCompareSemver(t *testing.T) {
	tests := []struct {
		a, b string
		want int // >0 means a>b, <0 means a<b, 0 means equal
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.0.1", "1.0.0", 1},
		{"1.0.0", "1.0.1", -1},
		{"2.0.0", "1.0.0", 1},
		{"1.1.0", "1.0.0", 1},
		{"1.0.0", "2.0.0", -1},
		{"0.2.0", "0.1.0", 1},
		{"0.1.1", "0.1.0", 1},
		{"10.0.0", "9.0.0", 1},
		{"1.10.0", "1.9.0", 1},
	}
	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			got := compareSemver(tt.a, tt.b)
			switch {
			case tt.want > 0 && got <= 0:
				t.Errorf("compareSemver(%q, %q) = %d, want >0", tt.a, tt.b, got)
			case tt.want < 0 && got >= 0:
				t.Errorf("compareSemver(%q, %q) = %d, want <0", tt.a, tt.b, got)
			case tt.want == 0 && got != 0:
				t.Errorf("compareSemver(%q, %q) = %d, want 0", tt.a, tt.b, got)
			}
		})
	}
}

func TestParseSemver(t *testing.T) {
	tests := []struct {
		input string
		want  [3]int
	}{
		{"1.2.3", [3]int{1, 2, 3}},
		{"0.1.0", [3]int{0, 1, 0}},
		{"10.20.30", [3]int{10, 20, 30}},
		{"1.0", [3]int{1, 0, 0}},
		{"1", [3]int{1, 0, 0}},
		{"1.2.3-beta", [3]int{1, 2, 3}},
		{"0.1.0-rc1", [3]int{0, 1, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseSemver(tt.input)
			if got != tt.want {
				t.Errorf("parseSemver(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestCheckForUpdateDevVersion(t *testing.T) {
	// "dev" version should always return no update available
	release, hasUpdate, err := CheckForUpdate("dev")
	if err != nil {
		t.Fatalf("CheckForUpdate(dev) error: %v", err)
	}
	if hasUpdate {
		t.Error("hasUpdate should be false for dev version")
	}
	if release != nil {
		t.Error("release should be nil for dev version")
	}
}

func TestDetectInstallMethod(t *testing.T) {
	method := DetectInstallMethod()
	// Should return either InstallDirect or InstallHomebrew
	if method != InstallDirect && method != InstallHomebrew {
		t.Errorf("DetectInstallMethod() = %d, want InstallDirect or InstallHomebrew", method)
	}
}

func TestInstallMethodConstants(t *testing.T) {
	if InstallDirect == InstallHomebrew {
		t.Error("InstallDirect == InstallHomebrew, should be different")
	}
}

func TestReleaseStruct(t *testing.T) {
	r := Release{
		TagName: "cli/v1.0.0",
		Body:    "Release notes",
		Assets: []Asset{
			{Name: "neutron_1.0.0_darwin_arm64.tar.gz", BrowserDownloadURL: "https://example.com/download"},
		},
	}
	if r.TagName != "cli/v1.0.0" {
		t.Errorf("TagName = %q", r.TagName)
	}
	if len(r.Assets) != 1 {
		t.Errorf("Assets len = %d", len(r.Assets))
	}
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	content := []byte("hello world")
	os.WriteFile(src, content, 0644)

	err := copyFile(src, dst)
	if err != nil {
		t.Fatalf("copyFile() error: %v", err)
	}

	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile(dst) error: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("dst content = %q, want %q", string(data), string(content))
	}
}

func TestCopyFileSrcNotFound(t *testing.T) {
	dir := t.TempDir()
	err := copyFile(filepath.Join(dir, "nonexistent"), filepath.Join(dir, "dst"))
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
}
